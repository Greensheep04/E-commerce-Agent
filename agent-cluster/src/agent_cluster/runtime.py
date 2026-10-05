import asyncio
from contextlib import ExitStack
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from agent_cluster.failures import failure_detail, failure_status


def schedule_trigger(definition):
    timezone = ZoneInfo(definition.timezone)
    if definition.interval_seconds is not None:
        return IntervalTrigger(seconds=definition.interval_seconds, timezone=timezone)
    return CronTrigger.from_crontab(definition.cron, timezone=timezone)


class ClusterRuntime:
    def __init__(self, store, engine, *, before_run=None, after_run=None, goal_filter=None):
        self.store = store
        self.engine = engine
        self.scheduler = AsyncIOScheduler(timezone="UTC")
        self.wakeup = asyncio.Event()
        self.stopping = False
        self.jobs = {}
        self.task = None
        self.lease_stack = ExitStack()
        self.is_leader = False
        self.before_run = before_run
        self.after_run = after_run
        self.goal_filter = goal_filter

    async def startup(self):
        self.stopping = False
        self.task = asyncio.create_task(self._loop(), name="agent-cluster-worker")

    def notify(self):
        self.wakeup.set()

    async def shutdown(self):
        self.stopping = True
        self.wakeup.set()
        if self.task is not None:
            await self.task
        if self.is_leader:
            self.scheduler.shutdown(wait=True)
        self.lease_stack.close()

    async def tick(self):
        for schedule in self.store.schedules():
            if self.goal_filter is not None and not self.goal_filter(schedule.goal_id):
                continue
            result = self.store.schedule_due(schedule.schedule_id, schedule_trigger(schedule),
                                            expected_definition=schedule.model_dump(mode="json"))
            if result is not None:
                self.notify()

    async def _drive(self, run):
        try:
            if self.before_run is not None and await self.before_run(run):
                return
            await self.engine.run(run.run_id, run.actor_id)
            if self.after_run is not None:
                await self.after_run(self.store.get_run(run.run_id, run.actor_id))
        except Exception as error:
            current = self.store.get_run(run.run_id, run.actor_id)
            if current.status not in {"paused", "cancelled", "unknown"}:
                self.store.set_run(run.run_id, run.actor_id, status=failure_status(error),
                                   error=failure_detail(error))
            raise

    async def _loop(self):
        while not self.stopping:
            if not self.is_leader:
                lease = self.store.lease("scheduler")
                if self.lease_stack.enter_context(lease):
                    self.is_leader = True
                    self.scheduler.add_job(self.tick, "interval", seconds=0.5, coalesce=True,
                                           max_instances=1, misfire_grace_time=None, id="cluster-schedules")
                    self.scheduler.start()
                else:
                    self.lease_stack.close()
                    self.lease_stack = ExitStack()
            for key, task in list(self.jobs.items()):
                if task.done():
                    self.jobs.pop(key)
                    if task.exception() is not None:
                        # 错误已持久记录，worker继续处理其他独立目标。
                        task.exception()
            for run in self.store.runnable():
                if self.goal_filter is not None and not self.goal_filter(run.goal_id):
                    continue
                if len(self.jobs) >= 4:
                    break
                if run.run_id not in self.jobs:
                    self.jobs[run.run_id] = asyncio.create_task(self._drive(run), name="goal:" + run.run_id)
            self.wakeup.clear()
            try:
                await asyncio.wait_for(self.wakeup.wait(), timeout=1)
            except TimeoutError:
                pass
        if self.jobs:
            await asyncio.gather(*self.jobs.values(), return_exceptions=True)
