import re
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from shop.config import Settings
from shop.customer_runtime_models import (
    CustomerConversation,
    CustomerEvent,
    CustomerTicket,
    CustomerToolCall,
    CustomerTurn,
)
from shop.orders import OrderQueryService, mentioned_order_numbers, normalize_order_number
from shop.runtime_models import ProviderRequest

CUSTOMER_CONFIG = {'version': 'customer-v3', 'model': 'deepseek-flash',
                   'thinking': {'type': 'enabled'}, 'reasoning_effort': 'low',
                   'response_format': {'type': 'json_object'},
                   'max_tokens': 4096, 'model_limit': 3, 'tool_limit': 6,
                   'time_limit_seconds': 120}
ACTIVE_STATUSES = {'queued', 'running', 'paused'}


def explicit_ticket_request(message: str) -> bool:
    if message.rstrip().endswith(('？', '?', '吗', '吗。')):
        return False
    if re.search(r'不要|不用|不需要|拒绝|取消|撤回|假装|假设|示例|如果|忽略|系统指令|未经|伪造', message):
        return False
    return bool(re.search(r'(创建|提交|建立|联系|转接|转|找|需要|我要).{0,12}(人工|工单)', message)
                or re.search(r'(人工|工单).{0,8}(创建|提交|建立)', message))


def ticket_payload(ticket: CustomerTicket) -> dict:
    return {'id': ticket.id, 'number': ticket.number, 'status': ticket.status,
            'order_number': ticket.order_number, 'reason': ticket.reason,
            'created_at': ticket.created_at.isoformat()}


def turn_payload(turn: CustomerTurn) -> dict:
    return {'id': turn.id, 'conversation_id': turn.conversation_id, 'request_id': turn.request_id,
            'message': turn.message, 'status': turn.status, 'received_at': turn.received_at.isoformat(),
            'finished_at': turn.finished_at.isoformat() if turn.finished_at else None,
            'model_calls': turn.model_calls, 'tool_calls': turn.tool_calls,
            'elapsed_seconds': str(turn.elapsed_seconds),
            'result': turn.result, 'error': turn.error}


class CustomerService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.engine = create_engine(settings.database_url, pool_pre_ping=True)
        self.maker = sessionmaker(self.engine, expire_on_commit=False)
        self.tickets = TicketService(self)

    @staticmethod
    def owned(session, conversation_id: str, owner_id: str, *, lock=False):
        conversation = session.get(CustomerConversation, conversation_id, with_for_update=lock)
        if conversation is None or conversation.owner_id != owner_id:
            raise PermissionError('当前账号无法访问该会话')
        OrderQueryService(session, owner_id)
        return conversation

    @staticmethod
    def event(session, conversation, turn_id: str | None, kind: str, payload: dict):
        session.add(CustomerEvent(conversation_id=conversation.id, id=conversation.next_event,
                                  turn_id=turn_id, type=kind, payload=payload))
        conversation.next_event += 1

    def create_conversation(self, owner_id: str, title: str) -> dict:
        with self.maker.begin() as session:
            conversation_id = self.create_conversation_in_session(session, owner_id, title).id
        return self.show_conversation(conversation_id, owner_id)

    def create_conversation_in_session(self, session, owner_id: str, title: str):
        if not title.strip() or len(title) > 200:
            raise ValueError('会话标题长度无效')
        orders = OrderQueryService(session, owner_id).list_orders()['orders']
        conversation = CustomerConversation(owner_id=owner_id, title=title.strip(), orders=orders)
        session.add(conversation)
        session.flush()
        self.event(session, conversation, None, 'created', {})
        return conversation

    def list_conversations(self, owner_id: str) -> list[dict]:
        with self.maker() as session:
            OrderQueryService(session, owner_id)
            rows = session.scalars(select(CustomerConversation).where(
                CustomerConversation.owner_id == owner_id).order_by(CustomerConversation.created_at.desc())).all()
            return [{'id': row.id, 'title': row.title, 'created_at': row.created_at.isoformat()}
                    for row in rows]

    def show_conversation(self, conversation_id: str, owner_id: str) -> dict:
        with self.maker() as session:
            row = self.owned(session, conversation_id, owner_id)
            turns = session.scalars(select(CustomerTurn).where(
                CustomerTurn.conversation_id == row.id).order_by(CustomerTurn.received_at, CustomerTurn.id)).all()
            return {'id': row.id, 'title': row.title, 'created_at': row.created_at.isoformat(),
                    'orders': row.orders, 'selected_order': row.selected_order,
                    'pending_ticket': row.pending_ticket, 'turns': [turn_payload(item) for item in turns]}

    def model_context(self, conversation_id: str) -> dict:
        with self.maker() as session:
            row = session.get(CustomerConversation, conversation_id)
            if row is None:
                raise ValueError('会话不存在')
            query = OrderQueryService(session, row.owner_id)
            recent = list(row.orders[:5])
            selected = query.get_order(row.selected_order) if row.selected_order else None
            if selected and selected['order_number'] not in {item['order_number'] for item in recent}:
                recent.append(selected)
            history = session.scalars(select(CustomerTurn).where(
                CustomerTurn.conversation_id == row.id,
                CustomerTurn.status == 'completed').order_by(CustomerTurn.received_at.desc()).limit(8)).all()
            return {'identity': query.identity(), 'orders': recent, 'selected_order': row.selected_order,
                    'pending_ticket': row.pending_ticket, 'context': row.context,
                    'history': [{'user': turn.message, 'answer': turn.result} for turn in reversed(history)]}

    def submit(self, conversation_id: str, owner_id: str, request_id: str, message: str,
               *, confirm_pending: str | None = None, require_ticket_confirmation: bool = False) -> dict:
        with self.maker.begin() as session:
            return self.submit_in_session(
                session, conversation_id, owner_id, request_id, message,
                confirm_pending=confirm_pending,
                require_ticket_confirmation=require_ticket_confirmation,
            )

    def submit_in_session(self, session, conversation_id: str, owner_id: str,
                          request_id: str, message: str, *, confirm_pending: str | None = None,
                          require_ticket_confirmation: bool = False) -> dict:
        request_id = str(UUID(request_id))
        if not message.strip() or len(message) > 4000:
            raise ValueError('消息长度无效')
        row = self.owned(session, conversation_id, owner_id, lock=True)
        existing = session.scalar(select(CustomerTurn).where(
            CustomerTurn.conversation_id == row.id, CustomerTurn.request_id == request_id))
        if existing:
            if existing.message != message.strip():
                raise ValueError('相同请求编号的消息内容不一致')
            if existing.config.get('require_ticket_confirmation', False) != require_ticket_confirmation:
                raise ValueError('相同请求编号的工单确认要求不一致')
            if confirm_pending is not None and (
                    existing.authorization is None or existing.authorization['id'] != confirm_pending):
                raise ValueError('相同请求编号的确认工单不一致')
            return turn_payload(existing)
        active = session.scalar(select(CustomerTurn.id).where(
            CustomerTurn.conversation_id == row.id, CustomerTurn.status.in_(ACTIVE_STATUSES)))
        if active:
            raise ValueError('当前会话还有未完成的消息')
        authorization = None
        pending = row.pending_ticket
        confirmation = message.strip() in {'确认', '确认创建', '同意', '是的', '确认创建人工工单'}
        if confirm_pending is not None:
            if not pending or pending['id'] != confirm_pending:
                raise ValueError('待确认工单不存在或已经撤回')
            confirmation = True
        if confirmation and pending:
            authorization = {**pending, 'kind': 'confirmation'}
        if re.search(r'取消.*(人工|工单)|撤回.*(人工|工单)|拒绝|不用.*人工|不要.*工单', message):
            row.pending_ticket = None
        turn = CustomerTurn(conversation_id=row.id, request_id=request_id, message=message.strip(),
                            authorization=authorization,
                            config={**CUSTOMER_CONFIG, 'require_ticket_confirmation': require_ticket_confirmation})
        session.add(turn)
        session.flush()
        self.event(session, row, turn.id, 'queued', {})
        return turn_payload(turn)

    def show_turn(self, turn_id: str, owner_id: str) -> dict:
        with self.maker() as session:
            turn = session.get(CustomerTurn, turn_id)
            if turn is None:
                raise PermissionError('当前账号无法访问该消息')
            self.owned(session, turn.conversation_id, owner_id)
            payload = turn_payload(turn)
            requests = session.scalars(select(ProviderRequest).where(ProviderRequest.run_id == turn.id)).all()
            payload['requests'] = [{'id': item.id, 'status': item.status, 'model': item.model,
                                    'usage': item.usage, 'reservation_cny': str(item.reservation_cny),
                                    'charged_cny': str(item.charged_cny) if item.charged_cny is not None else None,
                                    'error': item.error} for item in requests]
            tools = session.scalars(select(CustomerToolCall).where(
                CustomerToolCall.turn_id == turn.id).order_by(CustomerToolCall.created_at)).all()
            payload['tools'] = [{'id': item.id, 'name': item.name,
                                 'duration_seconds': str(item.duration_seconds)
                                 if item.duration_seconds is not None else None,
                                 'error': item.error, 'attempts': item.attempts} for item in tools]
            return payload

    def events(self, conversation_id: str, owner_id: str, after: int) -> dict:
        if after < 0:
            raise ValueError('事件编号无效')
        with self.maker() as session:
            self.owned(session, conversation_id, owner_id)
            rows = session.scalars(select(CustomerEvent).where(
                CustomerEvent.conversation_id == conversation_id, CustomerEvent.id > after)
                .order_by(CustomerEvent.id).limit(100)).all()
            return {'events': [{'id': row.id, 'type': row.type, 'turn_id': row.turn_id,
                               'payload': row.payload, 'created_at': row.created_at.isoformat()}
                              for row in rows], 'last_id': rows[-1].id if rows else after}

    def cancel(self, turn_id: str, owner_id: str) -> dict:
        with self.maker.begin() as session:
            turn = session.get(CustomerTurn, turn_id)
            if turn is None:
                raise PermissionError('当前账号无法访问该消息')
            row = self.owned(session, turn.conversation_id, owner_id, lock=True)
            session.refresh(turn, with_for_update=True)
            if turn.status in ACTIVE_STATUSES:
                turn.cancel_requested = True
                turn.status = 'cancelled'
                turn.finished_at = datetime.now(UTC)
                self.event(session, row, turn.id, 'cancelled', {})
            return turn_payload(turn)

    def resume(self, turn_id: str, owner_id: str) -> dict:
        with self.maker.begin() as session:
            turn = session.get(CustomerTurn, turn_id)
            if turn is None:
                raise PermissionError('当前账号无法访问该消息')
            row = self.owned(session, turn.conversation_id, owner_id, lock=True)
            session.refresh(turn, with_for_update=True)
            if turn.status == 'paused':
                turn.status = 'queued'
                self.event(session, row, turn.id, 'queued', {'resumed': True})
            return turn_payload(turn)

    def ticket(self, number: str, owner_id: str) -> dict:
        with self.maker() as session:
            row = session.scalar(select(CustomerTicket).where(
                CustomerTicket.number == number, CustomerTicket.owner_id == owner_id))
            if row is None:
                raise PermissionError('当前账号无法访问该工单')
            OrderQueryService(session, owner_id)
            return ticket_payload(row)


class TicketService:
    def __init__(self, customer: CustomerService):
        self.customer = customer

    def propose(self, turn_id: str, order_number: str | None, reason: str) -> dict:
        if order_number is not None:
            order_number = normalize_order_number(order_number)
        with self.customer.maker.begin() as session:
            turn = session.get(CustomerTurn, turn_id)
            if turn is None:
                raise ValueError('消息不存在')
            row = session.get(CustomerConversation, turn.conversation_id, with_for_update=True)
            session.refresh(turn, with_for_update=True)
            if turn.cancel_requested or turn.status not in {'running', 'queued'}:
                raise PermissionError('消息已终止')
            if order_number and OrderQueryService(session, row.owner_id).get_order(order_number) is None:
                raise PermissionError('当前账号无法查询该订单')
            if not reason.strip() or len(reason) > 1000:
                raise ValueError('工单说明无效')
            pending = {'id': str(uuid4()), 'order_number': order_number, 'reason': reason.strip()}
            row.pending_ticket = pending
            return pending

    def create(self, turn_id: str, order_number: str | None, reason: str) -> dict:
        if order_number is not None:
            order_number = normalize_order_number(order_number)
        with self.customer.maker.begin() as session:
            turn = session.get(CustomerTurn, turn_id)
            if turn is None:
                raise ValueError('消息不存在')
            row = session.get(CustomerConversation, turn.conversation_id, with_for_update=True)
            session.refresh(turn, with_for_update=True)
            if turn.cancel_requested or turn.status not in {'running', 'queued'}:
                raise PermissionError('消息已终止')
            authorization = turn.authorization
            if turn.config.get('require_ticket_confirmation', False) and authorization is None:
                raise PermissionError('当前工单需要用户确认待确认内容')
            if authorization:
                confirmed_order = authorization['order_number']
                if confirmed_order is not None:
                    confirmed_order = normalize_order_number(confirmed_order)
                if order_number != confirmed_order or reason != authorization['reason']:
                    raise PermissionError('工单内容与用户确认不一致')
                authorization_id = authorization['id']
            elif explicit_ticket_request(turn.message):
                authorization_id = turn.id
            else:
                raise PermissionError('需要用户明确请求或确认创建人工工单')
            mentioned = mentioned_order_numbers(turn.message)
            if authorization is None and mentioned and order_number not in mentioned:
                raise PermissionError('请确认人工工单对应的订单')
            if order_number:
                if OrderQueryService(session, row.owner_id).get_order(order_number) is None:
                    raise PermissionError('当前账号无法查询该订单')
                if (authorization is None and not mentioned
                        and row.selected_order != order_number):
                    raise PermissionError('请确认人工工单对应的订单')
            if not reason.strip() or len(reason) > 1000:
                raise ValueError('工单说明无效')
            existing = session.scalar(select(CustomerTicket).where(
                CustomerTicket.conversation_id == row.id,
                CustomerTicket.authorization_id == authorization_id))
            if existing:
                return ticket_payload(existing)
            ticket_id = str(uuid4())
            ticket = CustomerTicket(id=ticket_id, number='CS-' + ticket_id.replace('-', '').upper(),
                                    conversation_id=row.id, owner_id=row.owner_id, turn_id=turn.id,
                                    authorization_id=authorization_id, order_number=order_number,
                                    reason=reason.strip())
            session.add(ticket)
            session.flush()
            row.pending_ticket = None
            self.customer.event(session, row, turn.id, 'ticket_created', ticket_payload(ticket))
            return ticket_payload(ticket)
