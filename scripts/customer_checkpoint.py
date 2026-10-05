import argparse
import asyncio
import json
from datetime import UTC, datetime

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from sqlalchemy import select
from sqlalchemy.engine import make_url

from shop.config import Settings
from shop.customer_agent import CustomerServiceAgent
from shop.customer_runtime import CustomerService
from shop.customer_runtime_models import CustomerConversation, CustomerTicket, CustomerTurn
from shop.customer_worker import CustomerWorker


async def execute(turn_id: str, node: str, until_ticket: bool):
    service = CustomerService(Settings())
    with service.maker.begin() as session:
        turn = session.get(CustomerTurn, turn_id, with_for_update=True)
        if (turn.status != 'queued' or turn.model_calls) and not (until_ticket and turn.status == 'paused'):
            raise ValueError('检查入口需要全新未执行的真实消息')
        turn.status = 'running'
        turn.started_at = datetime.now(UTC)
    url = make_url(service.settings.database_url).set(drivername='postgresql')
    async with AsyncPostgresSaver.from_conn_string(url.render_as_string(hide_password=False)) as saver:
        await saver.setup()
        agent = CustomerServiceAgent(service, turn_id)
        graph = agent.graph(saver, interrupt_after=[node])
        config = {'configurable': {'thread_id': 'customer-' + turn_id}}
        prior = await graph.aget_state(config)
        await graph.ainvoke(None if prior.next else agent.initial(), config)
        snapshot = await graph.aget_state(config)
        if until_ticket:
            while snapshot.next:
                with service.maker() as session:
                    ticket = session.scalar(select(CustomerTicket).where(CustomerTicket.turn_id == turn_id))
                if ticket is not None:
                    break
                await graph.ainvoke(None, config)
                snapshot = await graph.aget_state(config)
        if not snapshot.next:
            raise ValueError('本次没有形成未完成的真实checkpoint')
        with service.maker.begin() as session:
            turn = session.get(CustomerTurn, turn_id, with_for_update=True)
            CustomerWorker.accumulate(turn)
            turn.started_at = None
            turn.status = 'paused'
            row = session.get(CustomerConversation, turn.conversation_id, with_for_update=True)
            service.event(session, row, turn.id, 'paused', {'checkpoint_after': node})
            print(json.dumps({'turn_id': turn.id, 'status': turn.status,
                              'model_calls': turn.model_calls, 'tool_calls': turn.tool_calls,
                              'elapsed_seconds': str(turn.elapsed_seconds), 'next': list(snapshot.next)}, ensure_ascii=False))
    service.engine.dispose()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('turn_id')
    parser.add_argument('--after', choices=['model', 'tools'], default='model')
    parser.add_argument('--until-ticket', action='store_true')
    args = parser.parse_args()
    asyncio.run(execute(args.turn_id, args.after, args.until_ticket))


if __name__ == '__main__':
    main()
