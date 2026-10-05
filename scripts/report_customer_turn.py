import argparse
import json

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from shop.config import Settings
from shop.customer_runtime_models import CustomerToolCall, CustomerTurn
from shop.runtime_models import ProviderRequest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('turn_id')
    args = parser.parse_args()
    with Session(create_engine(Settings().database_url)) as session:
        turn = session.get(CustomerTurn, args.turn_id)
        tools = session.scalars(select(CustomerToolCall).where(CustomerToolCall.turn_id == turn.id)).all()
        requests = session.scalars(select(ProviderRequest).where(ProviderRequest.run_id == turn.id)
                                   .order_by(ProviderRequest.created_at)).all()
        print(json.dumps({'id': turn.id, 'message': turn.message, 'status': turn.status,
            'error': turn.error, 'model_calls': turn.model_calls, 'tool_calls': turn.tool_calls,
            'result': turn.result, 'tools': [{'name': item.name, 'arguments': item.arguments,
             'result': item.result, 'error': item.error} for item in tools],
            'requests': [{'id': item.id, 'step': item.step_key, 'status': item.status,
                          'usage': item.usage, 'reservation_cny': str(item.reservation_cny),
                          'charged_cny': str(item.charged_cny),
                          'message': {key: value for key, value in item.response_body['choices'][0]['message'].items()
                                      if key != 'reasoning_content'} if item.response_body and 'choices' in item.response_body else None}
                         for item in requests]}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
