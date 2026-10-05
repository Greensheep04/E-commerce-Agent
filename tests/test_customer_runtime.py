import asyncio
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select

from shop.adventureworks import REVISION, import_orders
from shop.auth import hash_password
from shop.config import Settings
from shop.customer_agent import CustomerServiceAgent, ReadOrder, SearchProducts
from shop.customer_runtime import CustomerService
from shop.customer_runtime_models import CustomerConversation, CustomerTicket, CustomerTurn
from shop.models import User
from shop.orders import bind_customer


def test_delegated_ticket_requires_recorded_confirmation(db_session):
    import_orders(db_session, Path('.local/data/orders') / REVISION)
    user = User(username=f'cus-confirmation-{uuid4().hex}',
                password_hash=hash_password('local-check'), role='customer')
    db_session.add(user)
    db_session.flush()
    bind_customer(db_session, user.id, 11091)
    service = CustomerService(Settings(database_url=db_session.info['test_database_url']))
    conversation = service.create_conversation(user.id, '需要确认的工单')
    number = conversation['orders'][0]['order_number']
    request_id = str(uuid4())
    message = f'请为订单{number}创建人工工单，等待本人确认'
    turn = service.submit(conversation['id'], user.id, request_id, message,
                          require_ticket_confirmation=True)
    assert service.submit(conversation['id'], user.id, request_id, message,
                          require_ticket_confirmation=True)['id'] == turn['id']
    with pytest.raises(ValueError, match='确认要求'):
        service.submit(conversation['id'], user.id, request_id, message,
                       require_ticket_confirmation=False)
    with pytest.raises(PermissionError, match='确认'):
        service.tickets.create(turn['id'], number, '商品问题')
    pending = service.tickets.propose(turn['id'], number, '商品问题')
    initial = CustomerServiceAgent(service, turn['id']).initial()
    assert 'require_ticket_confirmation' in initial['messages'][1]['content']
    assert 'propose_ticket' in initial['messages'][0]['content']
    tools = CustomerServiceAgent(service, turn['id']).tool_definitions()
    assert 'propose_ticket' in {tool['function']['name'] for tool in tools}
    assert 'create_ticket' not in {tool['function']['name'] for tool in tools}
    with service.maker() as session:
        assert session.scalar(select(CustomerTicket).where(
            CustomerTicket.conversation_id == conversation['id'])) is None
    service.cancel(turn['id'], user.id)
    confirmed = service.submit(conversation['id'], user.id, str(uuid4()), '确认创建人工工单',
                               confirm_pending=pending['id'], require_ticket_confirmation=True)
    confirmed_agent = CustomerServiceAgent(service, confirmed['id'])
    confirmed_agent.initial()
    assert 'create_ticket' in {tool['function']['name'] for tool in confirmed_agent.tool_definitions()}
    ticket = service.tickets.create(confirmed['id'], number, pending['reason'])
    with service.maker() as session:
        assert session.get(CustomerTicket, ticket['id']).authorization_id == pending['id']
    service.cancel(confirmed['id'], user.id)
    service.engine.dispose()


def test_real_conversation_ownership_idempotency_and_ticket_authorization(db_session):
    import_orders(db_session, Path('.local/data/orders') / REVISION)
    users = [User(username=f'cus-runtime-{index}', password_hash=hash_password('local-check'),
                  role='customer') for index in range(2)]
    db_session.add_all(users)
    db_session.flush()
    for user, customer in zip(users, (11091, 11176), strict=True):
        bind_customer(db_session, user.id, customer)
    service = CustomerService(Settings(database_url=db_session.info['test_database_url']))
    conversation = service.create_conversation(users[0].id, '订单问题')
    assert len(conversation['orders']) == 20
    assert len(service.model_context(conversation['id'])['orders']) == 5
    with pytest.raises(PermissionError):
        service.show_conversation(conversation['id'], users[1].id)
    request_id = str(uuid4())
    first = service.submit(conversation['id'], users[0].id, request_id, '查询我的订单')
    same = service.submit(conversation['id'], users[0].id, request_id, '查询我的订单')
    assert first['id'] == same['id']
    with pytest.raises(ValueError):
        service.submit(conversation['id'], users[0].id, request_id, '改变原始请求')
    with pytest.raises(ValueError):
        service.submit(conversation['id'], users[0].id, str(uuid4()), '另一个并发消息')
    with pytest.raises(PermissionError):
        service.cancel(first['id'], users[1].id)
    service.cancel(first['id'], users[0].id)
    assert service.show_turn(first['id'], users[0].id)['status'] == 'cancelled'
    order = conversation['orders'][0]['order_number']
    ticket_request = service.submit(conversation['id'], users[0].id, str(uuid4()), '查询订单')
    with pytest.raises(PermissionError):
        service.tickets.create(ticket_request['id'], order, '订单咨询')
    service.cancel(ticket_request['id'], users[0].id)
    question = service.submit(conversation['id'], users[0].id, str(uuid4()),
                              f'可以为订单{order}创建人工工单吗？')
    with pytest.raises(PermissionError):
        service.tickets.create(question['id'], order, '订单咨询')
    service.cancel(question['id'], users[0].id)
    requested = service.submit(conversation['id'], users[0].id, str(uuid4()),
                               f'请为订单{order}创建人工工单，查询发货信息')
    with service.maker() as session:
        row = session.get(CustomerConversation, conversation['id'])
        turn = session.get(CustomerTurn, requested['id'])
        agent = CustomerServiceAgent(service, turn.id)
        assert agent.order_selection_allowed(row, turn, order)
        assert not agent.order_selection_allowed(row, turn, conversation['orders'][1]['order_number'])
    ticket = service.tickets.create(requested['id'], order, '发货信息')
    repeated = service.tickets.create(requested['id'], order, '发货信息')
    assert ticket['id'] == repeated['id']
    assert ticket['number'] == service.ticket(ticket['number'], users[0].id)['number']
    with service.maker() as session:
        assert len(session.scalars(select(CustomerTicket)).all()) == 1
    with pytest.raises(PermissionError):
        service.ticket(ticket['number'], users[1].id)
    service.cancel(requested['id'], users[0].id)
    assert service.ticket(ticket['number'], users[0].id)['status'] == 'open'
    service.engine.dispose()


def test_invalid_order_and_unsupported_attributes_have_explicit_tool_refusals(db_session):
    import_orders(db_session, Path('.local/data/orders') / REVISION)
    user = User(username='cus-input-boundaries', password_hash=hash_password('local-check'), role='customer')
    db_session.add(user)
    db_session.flush()
    bind_customer(db_session, user.id, 11091)
    service = CustomerService(Settings(database_url=db_session.info['test_database_url']))
    conversation = service.create_conversation(user.id, '输入边界')
    turn = service.submit(conversation['id'], user.id, str(uuid4()), '查询订单SOABC123')
    agent = CustomerServiceAgent(service, turn['id'])
    invalid = asyncio.run(agent.operation('read_order', ReadOrder(order_number='SOABC123'), 'invalid-order', {}))
    assert invalid['error']
    assert invalid['reply'] == {'kind': 'clarify', 'code': 'invalid_number'}
    unsupported = asyncio.run(agent.operation('search_products', SearchProducts(
        query='leather', attributes={'material': 'leather'}), 'unsupported-attribute', {}))
    assert unsupported['error']
    assert unsupported['reply'] == {'kind': 'insufficient', 'code': 'unknown_attribute'}
    assert service.show_conversation(conversation['id'], user.id)['selected_order'] is None
    assert service.show_turn(turn['id'], user.id)['model_calls'] == 0
    service.cancel(turn['id'], user.id)
    service.engine.dispose()


@pytest.mark.parametrize('mode', ['explicit', 'confirmation', 'wrong_prefix', 'conflicting_selection'])
def test_ticket_order_number_normalization_and_exact_selection(db_session, mode):
    import_orders(db_session, Path('.local/data/orders') / REVISION)
    user = User(username='cus-ticket-number-' + mode, password_hash=hash_password('local-check'), role='customer')
    db_session.add(user)
    db_session.flush()
    bind_customer(db_session, user.id, 11091)
    service = CustomerService(Settings(database_url=db_session.info['test_database_url']))
    conversation = service.create_conversation(user.id, '工单编号检查')
    number = conversation['orders'][0]['order_number']
    reason = '核查发货信息'
    if mode == 'confirmation':
        proposal = service.submit(conversation['id'], user.id, str(uuid4()), f'可以为订单{number[2:]}创建人工工单吗？')
        pending = service.tickets.propose(proposal['id'], number[2:], reason)
        assert pending['order_number'] == number
        service.cancel(proposal['id'], user.id)
        turn = service.submit(conversation['id'], user.id, str(uuid4()), '确认创建人工工单', confirm_pending=pending['id'])
    elif mode == 'conflicting_selection':
        selecting = service.submit(conversation['id'], user.id, str(uuid4()), f'查询订单{number}')
        agent = CustomerServiceAgent(service, selecting['id'])
        asyncio.run(agent.operation('read_order', ReadOrder(order_number=number), 'select-order', {}))
        service.cancel(selecting['id'], user.id)
        other = conversation['orders'][1]['order_number']
        turn = service.submit(conversation['id'], user.id, str(uuid4()), f'请为订单{other}创建人工工单，{reason}')
    else:
        stated = number[2:] if mode == 'explicit' else number + '0'
        turn = service.submit(conversation['id'], user.id, str(uuid4()), f'请为订单{stated}创建人工工单，{reason}')
    if mode in {'wrong_prefix', 'conflicting_selection'}:
        with pytest.raises(PermissionError):
            service.tickets.create(turn['id'], number, reason)
        with pytest.raises(PermissionError):
            service.tickets.create(turn['id'], None, reason)
    else:
        ticket = service.tickets.create(turn['id'], number[2:], reason)
        assert ticket['order_number'] == number
        assert service.tickets.create(turn['id'], number, reason)['id'] == ticket['id']
    service.cancel(turn['id'], user.id)
    service.engine.dispose()


@pytest.mark.parametrize('mode', ['different_pending', 'missing_authorization'])
def test_confirmation_request_id_cannot_target_another_pending_ticket(db_session, mode):
    import_orders(db_session, Path('.local/data/orders') / REVISION)
    user = User(username='cus-confirm-request-' + mode,
                password_hash=hash_password('local-check'), role='customer')
    db_session.add(user)
    db_session.flush()
    bind_customer(db_session, user.id, 11091)
    service = CustomerService(Settings(database_url=db_session.info['test_database_url']))
    conversation = service.create_conversation(user.id, '工单确认请求检查')
    first_order, second_order = [item['order_number'] for item in conversation['orders'][:2]]
    request_id = str(uuid4())
    original_pending = None
    if mode == 'different_pending':
        proposal = service.submit(conversation['id'], user.id, str(uuid4()),
                                  f'可以为订单{first_order}创建人工工单吗？')
        original_pending = service.tickets.propose(proposal['id'], first_order, '核查发货信息')
        service.cancel(proposal['id'], user.id)
        original = service.submit(conversation['id'], user.id, request_id,
                                  '确认创建人工工单', confirm_pending=original_pending['id'])
        service.tickets.create(original['id'], first_order, '核查发货信息')
    else:
        original = service.submit(conversation['id'], user.id, request_id, '确认创建人工工单')
    service.cancel(original['id'], user.id)
    proposal = service.submit(conversation['id'], user.id, str(uuid4()),
                              f'可以为订单{second_order}创建人工工单吗？')
    pending = service.tickets.propose(proposal['id'], second_order, '核查商品数量')
    service.cancel(proposal['id'], user.id)
    with pytest.raises(ValueError, match='相同请求编号'):
        service.submit(conversation['id'], user.id, request_id,
                       '确认创建人工工单', confirm_pending=pending['id'])
    if original_pending is not None:
        retry = service.submit(conversation['id'], user.id, request_id,
                               '确认创建人工工单', confirm_pending=original_pending['id'])
        assert retry['id'] == original['id']
    state = service.show_conversation(conversation['id'], user.id)
    assert state['pending_ticket']['id'] == pending['id']
    assert len(state['turns']) == (3 if mode == 'different_pending' else 2)
    assert all(turn['model_calls'] == 0 for turn in state['turns'])
    with service.maker() as session:
        assert len(session.scalars(select(CustomerTicket)).all()) == (1 if mode == 'different_pending' else 0)
    service.engine.dispose()
