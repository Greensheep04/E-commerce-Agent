import asyncio
import hashlib
import json
import os
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from shop.cluster import ShopCluster
from shop.config import Settings
from shop.customer_runtime_models import CustomerConversation, CustomerTurn
from shop.models import Base
from shop.runtime_models import ProviderRequest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateSchema

from agent_cluster.engine import bind_payload, completion_check
from agent_cluster.mcp_client import connect
from agent_cluster.types import (
    CompletionCriterion,
    GoalDefinition,
    InputBinding,
    TaskPlan,
    TaskSpec,
    TriggerEvent,
)

ROOT = Path(__file__).resolve().parents[2]
CLUSTER_CONFIG = Path(os.environ.get(
    "CLU05_CLUSTER_CONFIG", str(ROOT / ".scratch/cluster-live-config.json"),
)).resolve(strict=True)
EVIDENCE_DIR = ROOT / ".scratch/evidence/clu-paused-results-20261003-r1"
MAIN_DATABASE_URL = "postgresql+psycopg://shop@127.0.0.1:55432/shop"
ACTOR_ID = "912365af-e5b0-4a34-875c-90b047692d9f"
PAUSED_TURN_ID = "9eee9875-f015-425e-b176-eea7d7b10add"


@pytest.fixture
def db_session():
    database_url = os.environ["SHOP_TEST_DATABASE_URL"]
    schema = "paused_" + uuid4().hex
    admin_engine = create_engine(database_url)
    with admin_engine.begin() as connection:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public"))
        connection.execute(CreateSchema(schema))
    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema},public"})
    import shop.assessment_models  # noqa: F401
    import shop.cluster_models  # noqa: F401
    import shop.customer_models  # noqa: F401
    import shop.integration_models  # noqa: F401

    Base.metadata.create_all(engine)
    maker = sessionmaker(engine, expire_on_commit=False)
    try:
        with maker() as session:
            session.info["test_database_url"] = str(
                engine.url.set(query={"options": f"-csearch_path={schema},public"}))
            yield session
    finally:
        engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin_engine.dispose()


def _request_snapshot(session):
    requests = session.scalars(select(ProviderRequest).where(
        ProviderRequest.run_id == PAUSED_TURN_ID).order_by(ProviderRequest.step_key)).all()
    return [{
        "id": item.id,
        "step_key": item.step_key,
        "status": item.status,
        "model": item.model,
        "usage": item.usage,
        "reservation_cny": str(item.reservation_cny),
        "charged_cny": str(item.charged_cny) if item.charged_cny is not None else None,
        "error": item.error,
    } for item in requests]


def _public_requests(requests):
    fields = ("id", "status", "model", "usage", "reservation_cny", "charged_cny", "error")
    return sorted(({key: item[key] for key in fields} for item in requests),
                  key=lambda item: item["id"])


def _expense_snapshot(requests):
    charged = sum((Decimal(item["charged_cny"] or "0") for item in requests), Decimal("0"))
    reserved = sum((Decimal(item["reservation_cny"]) for item in requests
                    if item["status"] == "reserved"), Decimal("0"))
    return {"charged_cny": str(charged), "reserved_cny": str(reserved)}


def test_real_paused_customer_turn_cannot_complete_or_send_dependent_task(db_session):
    main_engine = create_engine(MAIN_DATABASE_URL, execution_options={"postgresql_readonly": True})
    try:
        with Session(main_engine) as session:
            paused_turn = session.get(CustomerTurn, PAUSED_TURN_ID)
            assert paused_turn is not None
            assert paused_turn.status == "paused"
            conversation = session.get(CustomerConversation, paused_turn.conversation_id)
            assert conversation is not None
            assert conversation.owner_id == ACTOR_ID
            requests_before = _request_snapshot(session)
            assert sorted(item["status"] for item in requests_before) == ["completed", "reserved"]
    finally:
        main_engine.dispose()

    notes_dir = ROOT / ".scratch/runtime/paused-results" / uuid4().hex
    settings = Settings(
        database_url=db_session.info["test_database_url"],
        cluster_config_file=CLUSTER_CONFIG,
        cluster_notes_dir=notes_dir,
        provider_env_file=None,
    )
    cluster = ShopCluster(settings, db_session.bind,
                          sessionmaker(db_session.bind, expire_on_commit=False))
    assert ACTOR_ID in cluster.configuration.actors
    cluster.store.initialize()
    source_task_id = "read-paused-turn"
    dependent_task_id = "consume-paused-turn"
    goal = GoalDefinition(
        actor_id=ACTOR_ID,
        objective="检查暂停客服消息的目标完成状态",
        allowed_operations={"shop_customer_agent": ["turn"]},
        criteria=[CompletionCriterion(
            tool_name="shop_customer_agent",
            action="turn",
            path=["status"],
            operator="equals",
            expected="paused",
        )],
        max_decisions=1,
    )
    cluster.store.create_goal(goal)
    run = cluster.store.trigger(TriggerEvent(
        event_id="paused-customer-" + uuid4().hex,
        goal_id=goal.goal_id,
        actor_id=ACTOR_ID,
    ))
    dependent = TaskSpec(
        task_id=dependent_task_id,
        tool_name="shop_customer_agent",
        action="turn",
        dependencies=[source_task_id],
        bindings=[InputBinding(
            source_task_id=source_task_id,
            source_path=["data", "id"],
            target_path=["turn_id"],
        )],
    )
    cluster.store.save_plan(run.run_id, ACTOR_ID, TaskPlan(tasks=[
        TaskSpec(
            task_id=source_task_id,
            tool_name="shop_customer_agent",
            action="turn",
            payload={"turn_id": PAUSED_TURN_ID},
        ),
        dependent,
    ]), expected_version=0)
    cluster.store.set_run(run.run_id, ACTOR_ID, status="running")

    async def execute_real_turn():
        async with connect(cluster.command_for_actor(ACTOR_ID)) as client:
            client.catalog.validate_goal(goal)
            cluster.engine.clients[run.run_id] = client
            try:
                return await cluster.engine._execute_node({
                    "run_id": run.run_id,
                    "actor_id": ACTOR_ID,
                    "proceed": True,
                })
            finally:
                cluster.engine.clients.pop(run.run_id)

    execution = asyncio.run(execute_real_turn())
    tasks = cluster.store.tasks(run.run_id, ACTOR_ID)
    source = next(item for item in tasks if item["task_id"] == source_task_id)
    consumer = next(item for item in tasks if item["task_id"] == dependent_task_id)
    assert source["result"]["status"] == "paused"
    assert source["result"]["data"]["status"] == "paused"
    assert source["status"] == "failed"
    assert completion_check(goal, tasks)[0] is False
    assert execution["proceed"] is True
    with pytest.raises(ValueError, match="前置任务业务结果尚未成功"):
        bind_payload(dependent, {source_task_id: source})
    assert consumer["status"] == "queued"
    assert consumer["arguments"] is None

    async def finish_real_tasks():
        return await cluster.engine._model_tool(
            run,
            "finish",
            {"summary": "检查暂停消息的完成条件"},
            str(uuid4()),
        )

    finish_result = asyncio.run(finish_real_tasks())
    stored_run = cluster.store.get_run(run.run_id, ACTOR_ID)
    assert finish_result["completed"] is False
    assert stored_run.status != "completed"

    assert _public_requests(source["result"]["data"]["requests"]) == _public_requests(requests_before)
    with Session(main_engine) as session:
        requests_after = _request_snapshot(session)
    assert requests_after == requests_before
    expenses_before = _expense_snapshot(requests_before)
    expenses_after = _expense_snapshot(requests_after)
    assert expenses_after == expenses_before
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0

    engine_source = ROOT / "agent-cluster/src/agent_cluster/engine.py"
    config_digest = hashlib.sha256(CLUSTER_CONFIG.read_bytes()).hexdigest()
    source_paths = {
        "engine.py": engine_source,
        "mcp_client.py": ROOT / "agent-cluster/src/agent_cluster/mcp_client.py",
        "integration_adapters.py": ROOT / "src/shop/integration_adapters.py",
        "customer_runtime.py": ROOT / "src/shop/customer_runtime.py",
        "test_paused_results.py": Path(__file__),
    }
    report = {
        "status": "passed",
        "check": "CLU-RV-01/02 paused customer result",
        "recorded_at": datetime.now(UTC).isoformat(),
        "goal_run_id": run.run_id,
        "goal_status": stored_run.status,
        "goal_completed": stored_run.status == "completed",
        "completion_check_passed": completion_check(goal, tasks)[0],
        "finish_tool_completed": finish_result["completed"],
        "source": {
            "actor_id": ACTOR_ID,
            "turn_id": PAUSED_TURN_ID,
            "business_status": source["result"]["status"],
            "task_status": source["status"],
            "requests_before": requests_before,
            "requests_returned": source["result"]["data"]["requests"],
            "requests_after": requests_after,
            "expenses_before": expenses_before,
            "expenses_after": expenses_after,
            "incremental_charged_cny": str(
                Decimal(expenses_after["charged_cny"]) - Decimal(expenses_before["charged_cny"])),
            "incremental_reserved_cny": str(
                Decimal(expenses_after["reserved_cny"]) - Decimal(expenses_before["reserved_cny"])),
        },
        "dependent": {
            "task_id": dependent_task_id,
            "status": consumer["status"],
            "arguments": consumer["arguments"],
            "binding_rejected": True,
        },
        "isolated_test_database": {
            "provider_request_count": 0,
            "budget_account_count": 0,
        },
        "configuration": {
            "path": str(CLUSTER_CONFIG.relative_to(ROOT)),
            "sha256": config_digest,
        },
        "source_sha256": {
            name: hashlib.sha256(path.read_bytes()).hexdigest()
            for name, path in source_paths.items()
        },
    }
    run_evidence_dir = EVIDENCE_DIR / run.run_id
    run_evidence_dir.mkdir(parents=True, exist_ok=False)
    (run_evidence_dir / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
