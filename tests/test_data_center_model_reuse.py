from pathlib import Path
from uuid import uuid4

import pytest
from data_center_protocol import ModelInvocation
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from shop.config import Settings
from shop.data_center_gateway import ShopDataModelGateway
from shop.data_center_host import ShopDataAuthorization
from shop.data_center_models import DataCenterSource, DataCenterTaskBinding
from shop.integration import ShopAuthentication
from shop.integration_models import IntegrationBudgetWindow
from shop.models import User
from shop.runtime_models import ProviderRequest


@pytest.fixture
def saved_embedding(db_session):
    database = create_engine("postgresql+psycopg://shop@127.0.0.1:55432/shop")
    with database.connect() as connection, connection.begin():
        connection.execute(text("SET TRANSACTION READ ONLY"))
        row = connection.execute(text(
            "SELECT p.* FROM provider_requests p JOIN data_center_task_bindings b ON b.run_id=p.run_id "
            "JOIN users u ON u.id=b.actor_id WHERE p.status='completed' "
            "AND p.step_key LIKE 'graph-embedding:%' AND u.role='admin' "
            "ORDER BY p.created_at LIMIT 1"
        )).mappings().one()
        binding = connection.execute(text(
            "SELECT * FROM data_center_task_bindings WHERE run_id=:run_id"
        ), {"run_id": row["run_id"]}).mappings().one()
        actor = connection.execute(text("SELECT * FROM users WHERE id=:actor"),
                                   {"actor": binding["actor_id"]}).mappings().one()
        sources = [dict(connection.execute(text(
            "SELECT * FROM data_center_sources WHERE source_id=:source"
        ), {"source": source_id}).mappings().one()) for source_id in binding["source_ids"]]
        original = dict(row)
        bound = dict(binding)
    database.dispose()
    assert original["response_body"]["data"] and original["price_basis"]["verification_scope"] == "data_center"
    db_session.add(User(**dict(actor)))
    db_session.add_all(DataCenterSource(**source) for source in sources)
    db_session.flush()
    db_session.add(DataCenterTaskBinding(**bound))
    db_session.add(ProviderRequest(**original))
    db_session.add(IntegrationBudgetWindow(id="reuse-zero-new-cost", limit_cny=0,
                                         include_open_reservations=True, active=True))
    db_session.commit()
    maker = sessionmaker(db_session.bind, expire_on_commit=False)
    settings = Settings(session_secret="embedding-reuse-test", data_center_source_root=Path("."))
    authentication = ShopAuthentication(settings, maker)
    gateway = ShopDataModelGateway(settings, maker, ShopDataAuthorization(authentication, maker))
    context = authentication.issue(bound["actor_id"], str(uuid4()))
    return gateway, context, original, bound


@pytest.mark.asyncio
async def test_completed_graph_embedding_reuses_original_request_in_new_run(saved_embedding, db_session):
    gateway, context, original, binding = saved_embedding
    task_id, run_id = str(uuid4()), str(uuid4())
    await gateway.bind(task_id, run_id, "build_graph", binding["source_ids"],
                       {"recovery_from": binding["task_id"]}, context)
    invocation = ModelInvocation(task_id=task_id, run_id=run_id, step_key=original["step_key"],
        kind="embedding", model=original["model"], texts=original["request_body"]["input"],
        source_ids=binding["source_ids"])
    reply = await gateway.invoke(invocation, context)
    assert reply.request_id == original["id"] and reply.response == original["response_body"]
    restored = await gateway.lookup(task_id, invocation.step_key, context)
    assert restored == reply
    usage = await gateway.usage(task_id, context)
    assert usage["requests"] == [] and usage["spent_cny"] == "0" and usage["reserved_cny"] == "0"
    assert usage["reused_requests"][0]["request_id"] == original["id"]
    assert usage["reused_requests"][0]["original_run_id"] == original["run_id"]
    assert db_session.scalars(select(ProviderRequest)).one().response_body == original["response_body"]
    await gateway.cancel(task_id, context)
    with pytest.raises(PermissionError, match="取消"):
        await gateway.lookup(task_id, invocation.step_key, context)


@pytest.mark.asyncio
async def test_reused_graph_embedding_rejects_changed_input_and_withdrawn_source(saved_embedding, db_session):
    gateway, context, original, binding = saved_embedding
    task_id, run_id = str(uuid4()), str(uuid4())
    await gateway.bind(task_id, run_id, "build_graph", binding["source_ids"], {}, context)
    invocation = ModelInvocation(task_id=task_id, run_id=run_id, step_key=original["step_key"],
        kind="embedding", model=original["model"], texts=original["request_body"]["input"],
        source_ids=binding["source_ids"])
    await gateway.invoke(invocation, context)
    with pytest.raises(ValueError, match="参数"):
        await gateway.invoke(invocation.model_copy(update={"texts": ["changed-input"]}), context)
    source = db_session.get(DataCenterSource, binding["source_ids"][0])
    source.active = False
    db_session.commit()
    with pytest.raises(PermissionError, match="撤回"):
        await gateway.lookup(task_id, invocation.step_key, context)
    assert len(db_session.scalars(select(ProviderRequest)).all()) == 1


@pytest.mark.asyncio
async def test_explicit_saved_request_reuse_never_creates_paid_request(saved_embedding, db_session):
    gateway, context, original, binding = saved_embedding
    task_id, run_id = str(uuid4()), str(uuid4())
    await gateway.bind(task_id, run_id, "build_graph", binding["source_ids"], {}, context)
    invocation = ModelInvocation(task_id=task_id, run_id=run_id, step_key=original["step_key"],
        kind="embedding", model=original["model"], texts=original["request_body"]["input"],
        source_ids=binding["source_ids"])
    with pytest.raises(ValueError, match="原请求"):
        await gateway.reuse(invocation, str(uuid4()), context)
    with pytest.raises(ValueError, match="原请求"):
        await gateway.reuse(invocation.model_copy(update={"texts": ["changed-input"]}), original["id"], context)
    assert len(db_session.scalars(select(ProviderRequest)).all()) == 1
    reply = await gateway.reuse(invocation, original["id"], context)
    assert reply.request_id == original["id"] and reply.response == original["response_body"]
    with pytest.raises(ValueError, match="原始响应"):
        await gateway.reuse(invocation, str(uuid4()), context)
    assert len(db_session.scalars(select(ProviderRequest)).all()) == 1
