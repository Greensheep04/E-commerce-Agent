import json
from hashlib import sha256
from uuid import uuid4

from agent_cluster.store import digest
from agent_cluster.types import ScheduleDefinition, TriggerEvent
from integration_protocol import ResourceRef
from sqlalchemy import select

from shop.data_center_host import ShopDataAuthorization, ShopSourceReadPort
from shop.data_center_models import DataCenterSource, DataCenterWatch


class DataCenterAutomation:
    def __init__(self, cluster, integration, settings, maker):
        self.cluster = cluster
        self.integration = integration
        self.settings = settings
        self.maker = maker
        self.authorization = ShopDataAuthorization(integration.auth, maker)
        self.sources = ShopSourceReadPort(settings, maker, self.authorization)

    def install_watch(self, goal_id, actor_id, source_ids, *, operation="sync", interval_seconds=900,
                      nightly_cron="0 2 * * *", timezone="Asia/Shanghai"):
        source_ids = sorted(set(source_ids))
        if not source_ids or any(not isinstance(source_id, str) or not source_id.strip()
                                 for source_id in source_ids):
            raise ValueError("来源监听需要有效来源编号")
        if operation not in {"sync", "ingest", "query", "ask"}:
            raise ValueError("来源监听操作没有登记")
        goal = self.cluster.store.get_goal(goal_id, actor_id)
        if self.cluster.store.goal_state(goal_id, actor_id) == "cancelled":
            raise ValueError("已取消目标不能安装来源监听，请使用新的安装编号")
        if operation not in goal.allowed_operations.get("shop_data_center_agent", []):
            raise PermissionError("目标没有登记此数据中台操作")
        sources = self._source_records(source_ids)
        kinds = {source.source_id: source.kind for source in sources}
        allowed_kinds = {
            "sync": {"analytics"},
            "ingest": {"document"},
            "query": {"document"},
            "ask": {"document"},
        }[operation]
        if any(kind not in allowed_kinds for kind in kinds.values()):
            raise ValueError("来源监听操作与来源类型不匹配")
        context = self.integration.auth.issue(actor_id, str(uuid4()))
        for source_id in source_ids:
            authorization_action = {
                "sync": "sync",
                "ingest": "ingest",
                "query": "read",
                "ask": "read",
            }[operation]
            self.authorization.authorize_sync(context, [source_id], authorization_action)
        with self.maker.begin() as session:
            prior = session.scalar(select(DataCenterWatch).where(DataCenterWatch.goal_id == goal_id)
                                   .with_for_update())
            if prior is not None:
                if (prior.actor_id != actor_id or prior.source_ids != source_ids
                        or prior.operation != operation):
                    raise ValueError("目标已经登记了不同的来源监听")
                prior.active = True
                watch = prior
            else:
                watch = DataCenterWatch(watch_id=str(uuid4()), goal_id=goal_id, actor_id=actor_id,
                                        source_ids=source_ids, operation=operation,
                                        committed_signature=None, observed_signatures={}, active=True)
                session.add(watch)
        if operation == "sync":
            self.cluster.store.save_schedule(ScheduleDefinition(
                schedule_id=self._schedule_id(goal_id, "interval"), goal_id=goal_id, actor_id=actor_id,
                interval_seconds=interval_seconds, timezone=timezone,
            ))
        self.cluster.store.save_schedule(ScheduleDefinition(
            schedule_id=self._schedule_id(goal_id, "nightly"), goal_id=goal_id, actor_id=actor_id,
            cron=nightly_cron, timezone=timezone,
        ))
        return watch

    @staticmethod
    def _schedule_id(goal_id, cadence):
        return "data-center-" + cadence + "-" + sha256(goal_id.encode()).hexdigest()[:32]

    def _watch_for_goal(self, goal_id, actor_id):
        with self.maker() as session:
            watch = session.scalar(select(DataCenterWatch).where(
                DataCenterWatch.goal_id == goal_id, DataCenterWatch.actor_id == actor_id,
                DataCenterWatch.active.is_(True)))
            if watch is None:
                return None
            session.expunge(watch)
            return watch

    def accepts_goal(self, goal_id):
        with self.maker() as session:
            return session.scalar(select(DataCenterWatch.watch_id).where(
                DataCenterWatch.goal_id == goal_id, DataCenterWatch.active.is_(True))) is not None

    def _source_records(self, source_ids):
        with self.maker() as session:
            sources = [session.get(DataCenterSource, source_id) for source_id in source_ids]
            if any(source is None or not source.active for source in sources):
                raise PermissionError("来源未登记或已经撤回")
            for source in sources:
                session.expunge(source)
            return sources

    async def source_signature(self, source_ids, context):
        signatures = []
        for source_id in sorted(set(source_ids)):
            source = self._source_records([source_id])[0]
            snapshot = await self.sources.snapshot(source_id, context)
            source_file_sha256 = None
            if source.kind == "document":
                if source.resource is None:
                    raise ValueError("文档来源缺少登记文件引用")
                reference = ResourceRef.model_validate(source.resource)
                content = await self.sources.read_file(reference, context)
                source_file_sha256 = sha256(content).hexdigest()
            elif source.kind != "analytics":
                raise ValueError("来源监听只支持真实经营快照和登记文档")
            signatures.append({"source_id": source_id, "snapshot_sha256": snapshot.content_sha256,
                               "source_sha256": source.sha256, "file_sha256": source_file_sha256})
        return digest(signatures)

    async def trigger_source_event(self, source_id):
        with self.maker() as session:
            watches = session.scalars(select(DataCenterWatch).where(
                DataCenterWatch.active.is_(True), DataCenterWatch.source_ids.contains([source_id])
            ).order_by(DataCenterWatch.watch_id)).all()
            for watch in watches:
                session.expunge(watch)
        if not watches:
            raise LookupError("没有登记此来源的有效监听")
        triggered = []
        for watch in watches:
            if self.cluster.store.goal_state(watch.goal_id, watch.actor_id) != "active":
                continue
            context = self.integration.auth.issue(watch.actor_id, str(uuid4()))
            signature = await self.source_signature(watch.source_ids, context)
            event_id = "data-center-source:" + watch.watch_id + ":" + signature
            run = self.cluster.store.trigger(TriggerEvent(
                event_id=event_id, goal_id=watch.goal_id, actor_id=watch.actor_id, kind="event",
                payload={"watch_id": watch.watch_id, "source_id": source_id,
                         "source_signature": signature, "operation": watch.operation},
            ), only_active=True)
            if run is None:
                continue
            self.cluster.runtime.notify()
            triggered.append(run)
        return triggered[0] if len(triggered) == 1 else triggered

    async def preflight(self, run):
        with self.cluster.store.lease("dc-watch:" + run.run_id) as acquired:
            if not acquired:
                return True
            current_run = self.cluster.store.get_run(run.run_id, run.actor_id)
            if current_run.status not in {"queued", "running"}:
                return True
            watch = self._watch_for_goal(run.goal_id, run.actor_id)
            if watch is None:
                return False
            if self.cluster.store.tasks(run.run_id, run.actor_id):
                return False
            context = self.integration.auth.issue(run.actor_id, str(uuid4()))
            signature = await self.source_signature(watch.source_ids, context)
            current_run = self.cluster.store.get_run(run.run_id, run.actor_id)
            if current_run.status not in {"queued", "running"}:
                return True
            with self.maker.begin() as session:
                current = session.scalar(select(DataCenterWatch).where(
                    DataCenterWatch.watch_id == watch.watch_id).with_for_update())
                if current is None or not current.active:
                    raise PermissionError("来源监听已经撤回")
                observed = dict(current.observed_signatures)
                unchanged = current.committed_signature == signature
                pending = None
                if not unchanged:
                    for prior_run_id, prior_signature in observed.items():
                        if prior_run_id == run.run_id or prior_signature != signature:
                            continue
                        prior_run = self.cluster.store.get_run(prior_run_id, run.actor_id)
                        if prior_run.status == "completed":
                            current.committed_signature = signature
                            unchanged = True
                            break
                        if prior_run.status != "cancelled":
                            pending = prior_run
                            break
                if not unchanged and pending is None:
                    observed[run.run_id] = signature
                    current.observed_signatures = observed
            if not unchanged and pending is None:
                return False
            note_id = "data-center-watch-" + sha256(run.run_id.encode()).hexdigest()[:40]
            observation = {"run_id": run.run_id, "watch_id": watch.watch_id,
                           "source_ids": watch.source_ids, "signature": signature,
                           "result": "unchanged" if unchanged else "pending"}
            if pending is not None:
                observation.update({"prior_run_id": pending.run_id, "prior_status": pending.status})
            content = json.dumps(observation, ensure_ascii=False, sort_keys=True)
            summary = ("来源签名没有变化，运行进入等待状态" if unchanged
                       else "同一来源已有未完成运行，等待原运行处理")
            self.cluster.notes.write(goal_id=run.goal_id, actor_id=run.actor_id,
                                     topic="data-center-watch", content=content,
                                     summary=summary,
                                     source="data_center_automation", note_id=note_id)
            self.cluster.store.set_run(run.run_id, run.actor_id, status="waiting",
                                       summary=("来源内容与上次完成的检查一致" if unchanged else summary))
            return True

    async def after_run(self, run):
        current_run = self.cluster.store.get_run(run.run_id, run.actor_id)
        if current_run.status != "completed":
            return
        with self.maker.begin() as session:
            watch = session.scalar(select(DataCenterWatch).where(
                DataCenterWatch.goal_id == run.goal_id, DataCenterWatch.actor_id == run.actor_id,
                DataCenterWatch.active.is_(True)).with_for_update())
            if watch is None:
                return
            signature = watch.observed_signatures.get(run.run_id)
            if signature is None:
                return
            watch.committed_signature = signature
