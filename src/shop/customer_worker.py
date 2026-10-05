import asyncio
import logging
from contextlib import suppress
from datetime import UTC, datetime
from decimal import Decimal

import httpx
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from sqlalchemy import select, text
from sqlalchemy.engine import make_url

from shop.customer_agent import CustomerServiceAgent
from shop.customer_runtime import ACTIVE_STATUSES
from shop.customer_runtime_models import CustomerConversation, CustomerTurn
from shop.runtime_models import ProviderRequest

logger = logging.getLogger(__name__)


class CustomerWorker:
    def __init__(self, service):
        self.service = service
        self.tasks: dict[str, asyncio.Task] = {}
        self.stopping = False

    async def setup(self):
        url = make_url(self.service.settings.database_url).set(drivername='postgresql')
        self.connection_string = url.render_as_string(hide_password=False)
        async with AsyncPostgresSaver.from_conn_string(self.connection_string) as saver:
            lock_query = "SELECT pg_try_advisory_lock(hashtext(current_schema() || ':checkpoint-setup')) AS acquired"
            while True:
                cursor = await saver.conn.execute(lock_query)
                if (await cursor.fetchone())["acquired"]:
                    break
                await asyncio.sleep(0.05)
            try:
                await saver.setup()
            finally:
                await saver.conn.execute(
                    "SELECT pg_advisory_unlock(hashtext(current_schema() || ':checkpoint-setup'))")
        self.stopping = False
        with self.service.maker() as session:
            ids = list(session.scalars(select(CustomerTurn.id).where(
                CustomerTurn.status.in_({'queued', 'running'}))))
        for turn_id in ids:
            self.schedule(turn_id)

    def schedule(self, turn_id: str):
        if self.stopping or turn_id in self.tasks:
            return
        task = asyncio.create_task(self.run(turn_id), name='customer-' + turn_id)
        self.tasks[turn_id] = task
        task.add_done_callback(lambda finished: self.finished(turn_id, finished))

    def finished(self, turn_id: str, task: asyncio.Task):
        self.tasks.pop(turn_id, None)
        if not task.cancelled() and task.exception():
            logger.error('客服运行器异常，turn_id=%s', turn_id,
                         exc_info=(type(task.exception()), task.exception(), task.exception().__traceback__))

    async def shutdown(self):
        self.stopping = True
        tasks = list(self.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    def accumulate(turn):
        if turn.started_at:
            current = datetime.now(UTC)
            turn.elapsed_seconds += Decimal(str((current - turn.started_at).total_seconds()))
            turn.started_at = current

    async def monitor(self, turn_id: str, execution: asyncio.Task):
        while not execution.done():
            await asyncio.sleep(0.1)
            with self.service.maker.begin() as session:
                turn = session.get(CustomerTurn, turn_id, with_for_update=True)
                if turn.cancel_requested:
                    execution.cancel()
                    return
                self.accumulate(turn)
                if turn.elapsed_seconds >= turn.config['time_limit_seconds']:
                    execution.cancel()
                    return

    async def execute(self, turn_id: str) -> dict:
        agent = CustomerServiceAgent(self.service, turn_id)
        config = {'configurable': {'thread_id': 'customer-' + turn_id}, 'recursion_limit': 12}
        async with AsyncPostgresSaver.from_conn_string(self.connection_string) as saver:
            graph = agent.graph(saver)
            snapshot = await graph.aget_state(config)
            if snapshot.values.get('result') and not snapshot.next:
                return snapshot.values['result']
            state = await graph.ainvoke(None if snapshot.next else agent.initial(), config)
            if not state.get('result'):
                raise ValueError('客服循环结束后没有经过验证的业务答复')
            return state['result']

    async def run(self, turn_id: str):
        connection = self.service.engine.connect()
        execution = None
        monitor = None
        locked = False
        processing = False
        try:
            with self.service.maker() as session:
                turn = session.get(CustomerTurn, turn_id)
                if turn is None:
                    raise ValueError('消息不存在')
                conversation_id = turn.conversation_id
            lock_key = 'customer-conversation:' + conversation_id
            while not locked:
                locked = connection.scalar(text('SELECT pg_try_advisory_lock(hashtextextended(:key, 0))'),
                                           {'key': lock_key})
                connection.commit()
                if not locked:
                    with self.service.maker() as session:
                        waiting = session.get(CustomerTurn, turn_id)
                        if waiting.cancel_requested or waiting.status not in ACTIVE_STATUSES:
                            return
                    await asyncio.sleep(0.05)
            with self.service.maker.begin() as session:
                row = session.get(CustomerConversation, conversation_id, with_for_update=True)
                turn = session.get(CustomerTurn, turn_id, with_for_update=True)
                if turn.status not in ACTIVE_STATUSES or turn.cancel_requested:
                    return
                turn.status = 'running'
                turn.error = None
                turn.started_at = datetime.now(UTC)
                self.service.event(session, row, turn.id, 'running', {'model_calls': turn.model_calls})
            processing = True
            execution = asyncio.create_task(self.execute(turn_id))
            monitor = asyncio.create_task(self.monitor(turn_id, execution))
            result = await execution
            with self.service.maker.begin() as session:
                row = session.get(CustomerConversation, conversation_id, with_for_update=True)
                turn = session.get(CustomerTurn, turn_id, with_for_update=True)
                self.accumulate(turn)
                turn.started_at = None
                if not turn.cancel_requested:
                    turn.result = result
                    turn.status = 'completed'
                    turn.finished_at = datetime.now(UTC)
                    self.service.event(session, row, turn.id, 'answer', result)
        except asyncio.CancelledError:
            if not processing:
                raise
            if execution and not execution.done():
                execution.cancel()
                with suppress(asyncio.CancelledError):
                    await execution
            self.finish_error(turn_id, '本地处理已终止', interrupted=True)
            if self.stopping:
                raise
        except Exception as error:
            if not processing:
                raise
            self.finish_error(turn_id, f'{type(error).__name__}: {error}',
                              interrupted=isinstance(error, (httpx.HTTPError, TimeoutError)))
            logger.exception('客服消息处理失败，turn_id=%s', turn_id)
        finally:
            if monitor:
                monitor.cancel()
                with suppress(asyncio.CancelledError):
                    await monitor
            if locked:
                connection.execute(text('SELECT pg_advisory_unlock(hashtextextended(:key, 0))'),
                                   {'key': lock_key})
                connection.commit()
            connection.close()

    def finish_error(self, turn_id: str, error: str, *, interrupted: bool):
        with self.service.maker.begin() as session:
            turn = session.get(CustomerTurn, turn_id)
            if turn is None:
                raise ValueError('消息不存在')
            row = session.get(CustomerConversation, turn.conversation_id, with_for_update=True)
            session.refresh(turn, with_for_update=True)
            self.accumulate(turn)
            turn.started_at = None
            unknown_request = session.scalar(select(ProviderRequest.id).where(
                ProviderRequest.run_id == turn_id,
                ProviderRequest.status == 'unknown',
                ProviderRequest.price_basis['cluster_run_id'].astext.is_not(None),
            ).limit(1)) is not None
            if turn.cancel_requested:
                turn.status = 'cancelled'
            elif unknown_request:
                turn.status = 'unknown'
                turn.error = error
            elif turn.elapsed_seconds >= turn.config['time_limit_seconds']:
                turn.status = 'failed'
                turn.error = '消息累计处理时间超过上限'
            else:
                turn.status = 'paused' if interrupted else 'failed'
                turn.error = error
            if turn.status in {'cancelled', 'failed', 'unknown'}:
                turn.finished_at = datetime.now(UTC)
            self.service.event(session, row, turn.id, turn.status, {'error': turn.error})
