import argparse
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import numpy as np
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from shop.budget import usage_cost
from shop.config import Settings
from shop.customer_runtime import explicit_ticket_request
from shop.customer_runtime_models import (
    CustomerConversation,
    CustomerTicket,
    CustomerToolCall,
    CustomerTurn,
)
from shop.orders import OrderQueryService
from shop.runtime_models import BudgetAccount, ProviderRequest


def summary(values: list) -> dict | None:
    if not values:
        return None
    return {'samples': len(values), 'mean': float(np.mean(values)),
            'p50': float(np.percentile(values, 50)), 'p95': float(np.percentile(values, 95)),
            'maximum': max(values)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--evaluation-report', type=Path)
    parser.add_argument('--directory', type=Path)
    args = parser.parse_args()
    evaluation = json.loads(args.evaluation_report.read_text()) if args.evaluation_report else None
    turn_ids = None
    if evaluation:
        turn_ids = {item['turn_id'] for case in evaluation['cases'] for step in case['steps']
                    for item in (step, step.get('other', {})) if item.get('turn_id')}
        if not turn_ids:
            raise ValueError('评价报告缺少业务消息身份')
    directory = args.directory or Path('.scratch/evidence') / (
        f'cus-usage-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}')
    directory.mkdir(parents=True, exist_ok=True)
    engine = create_engine(Settings().database_url)
    issues = []
    with Session(engine) as session:
        all_requests = session.scalars(select(ProviderRequest).order_by(ProviderRequest.created_at)).all()
        customer_requests = [item for item in all_requests if item.price_basis.get('scope') == 'customer']
        requests = [item for item in customer_requests if turn_ids is None or item.run_id in turn_ids]
        all_turns = session.scalars(select(CustomerTurn)).all()
        turns = [item for item in all_turns if turn_ids is None or item.id in turn_ids]
        if turn_ids is not None and {item.id for item in turns} != turn_ids:
            raise ValueError('评价消息与数据库记录不完整')
        grouped = defaultdict(list)
        for request in requests:
            grouped[request.model].append(request)
            if request.status == 'completed':
                expected = usage_cost(request.usage, request.price_basis,
                                      embedding=request.model == 'qwen3.7-text-embedding')
                if expected != request.charged_cny or not request.provider_request_id:
                    issues.append({'request_id': request.id, 'issue': '提供商身份或用量费用不一致'})
            elif request.status == 'unknown' and (request.charged_cny is not None or not request.reservation_cny):
                issues.append({'request_id': request.id, 'issue': '未知用量没有保留全部费用预留'})
        modules = {}
        for model, items in grouped.items():
            reasoning, outputs, durations = [], [], []
            for item in items:
                if item.usage and 'completion_tokens' in item.usage:
                    value = item.usage.get('completion_tokens_details', {}).get('reasoning_tokens')
                    if value is not None:
                        reasoning.append(value)
                        outputs.append(item.usage['completion_tokens'] - value)
                if item.response_body and 'client_duration_seconds' in item.response_body:
                    durations.append(item.response_body['client_duration_seconds'])
            modules[model] = {'requests': len(items), 'statuses': dict(Counter(item.status for item in items)),
                'request_ids': [item.id for item in items],
                'known_cost_cny': str(sum((item.charged_cny or Decimal(0) for item in items), Decimal(0))),
                'reserved_cny': str(sum((item.reservation_cny for item in items if item.charged_cny is None), Decimal(0))),
                'reasoning_tokens': summary(reasoning), 'formal_output_tokens': summary(outputs),
                'provider_seconds': summary(durations), 'max_tokens': sorted({item.request_body['max_tokens']
                    for item in items if 'max_tokens' in item.request_body}),
                'usage_unknown_request_ids': [item.id for item in items if not item.usage],
                'truncated_request_ids': [item.id for item in items if item.response_body
                    and item.response_body.get('choices', [{}])[0].get('finish_reason') == 'length']}
        conversations = {item.id: item for item in session.scalars(select(CustomerConversation))}
        selected = {item.id for item in turns}
        tools = session.scalars(select(CustomerToolCall).where(CustomerToolCall.turn_id.in_(selected))).all()
        tickets = session.scalars(select(CustomerTicket).where(CustomerTicket.turn_id.in_(selected))).all()
        by_turn = {item.id: item for item in turns}
        ticket_keys = set()
        fact_orders, fact_products = 0, 0
        for turn in turns:
            conversation = conversations[turn.conversation_id]
            if turn.model_calls > turn.config['model_limit'] or turn.tool_calls > turn.config['tool_limit']:
                issues.append({'turn_id': turn.id, 'issue': '累计调用超限'})
            if turn.status == 'cancelled' and turn.result is not None:
                issues.append({'turn_id': turn.id, 'issue': '取消消息存在后续答复'})
            if not turn.result:
                continue
            for order in turn.result['orders']:
                actual = OrderQueryService(session, conversation.owner_id).get_order(order['order_number'])
                if actual is None or any(order[key] != actual[key] for key in (
                    'order_id', 'order_date', 'due_date', 'ship_date', 'subtotal', 'tax', 'freight',
                    'total_due', 'status', 'status_label', 'item_count', 'source')):
                    issues.append({'turn_id': turn.id, 'issue': '订单来源字段或客户范围错误'})
                elif order['items'] != actual['items'][:len(order['items'])]:
                    issues.append({'turn_id': turn.id, 'issue': '订单明细来源错误'})
                fact_orders += 1
            fact_products += len(turn.result['products'])
            if turn.result['ticket']:
                ticket = session.get(CustomerTicket, turn.result['ticket']['id'])
                if ticket is None or ticket.owner_id != conversation.owner_id or ticket.number != turn.result['ticket']['number']:
                    issues.append({'turn_id': turn.id, 'issue': '答复缺少真实本人工单'})
        for ticket in tickets:
            turn = by_turn[ticket.turn_id]
            conversation = conversations[turn.conversation_id]
            key = (ticket.conversation_id, ticket.authorization_id)
            if key in ticket_keys:
                issues.append({'ticket_id': ticket.id, 'issue': '工单授权身份重复写入'})
            ticket_keys.add(key)
            if ticket.owner_id != conversation.owner_id:
                issues.append({'ticket_id': ticket.id, 'issue': '工单客户范围错误'})
            if turn.authorization:
                if (ticket.authorization_id != turn.authorization['id']
                        or ticket.order_number != turn.authorization['order_number']
                        or ticket.reason != turn.authorization['reason']):
                    issues.append({'ticket_id': ticket.id, 'issue': '工单与确认内容不同'})
            elif ticket.authorization_id != turn.id or not explicit_ticket_request(turn.message):
                issues.append({'ticket_id': ticket.id, 'issue': '工单缺少明确授权'})
            if ticket.order_number and OrderQueryService(session, ticket.owner_id).get_order(ticket.order_number) is None:
                issues.append({'ticket_id': ticket.id, 'issue': '工单关联其他客户订单'})
        account = session.get(BudgetAccount, 'project')
        spent = sum((item.charged_cny or Decimal(0) for item in all_requests), Decimal(0))
        reserved = sum((item.reservation_cny for item in all_requests if item.charged_cny is None), Decimal(0))
        if account.spent_cny != spent or account.reserved_cny != reserved:
            issues.append({'issue': '项目账户与逐条请求账目不一致'})
        customer_total = sum((item.charged_cny if item.charged_cny is not None else item.reservation_cny
                              for item in customer_requests), Decimal(0))
        if customer_total > 30 or spent + reserved > 300:
            issues.append({'issue': '费用超过批准额度'})
        report = {'measured_at': datetime.now(UTC).isoformat(), 'scope': 'fixed_evaluation' if evaluation else 'customer_history',
            'evaluation_report': str(args.evaluation_report) if evaluation else None,
            'evaluation_sha256': sha256(args.evaluation_report.read_bytes()).hexdigest() if evaluation else None,
            'turn_ids': sorted(selected), 'modules': modules,
            'turn_statuses': dict(Counter(item.status for item in turns)),
            'tool_names': dict(Counter(item.name for item in tools)), 'ticket_count': len(tickets),
            'checked_order_references': fact_orders, 'product_references_checked_by_frozen_evaluation': fact_products,
            'account': {'spent_cny': str(spent), 'reserved_cny': str(reserved), 'customer_total_cny': str(customer_total)},
            'issues': issues, 'status': 'passed' if not issues else 'failed'}
    engine.dispose()
    output = directory / 'report.json'
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'report': str(output), 'status': report['status'], 'account': report['account']}, ensure_ascii=False))
    if issues:
        raise ValueError('客服来源、写入或费用审计未通过')


if __name__ == '__main__':
    main()
