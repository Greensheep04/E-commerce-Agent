import json
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest
from agent_cluster.store import ClusterStore, GoalRecord, RunRecord
from agent_cluster.types import CompletionCriterion, GoalDefinition, TriggerEvent
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import sessionmaker

from shop.budget import BudgetExceeded, BudgetService
from shop.config import Settings
from shop.integration_models import IntegrationBudgetWindow
from shop.models import Base
from shop.runtime_models import BudgetAccount, ProviderRequest

ROOT = Path(__file__).resolve().parents[1]
FIXED_REQUEST_PATH = ROOT / ".scratch/evidence/clu-unsent-resume-20261003-r2/request.json"
FIXED_REPORT_PATH = ROOT / ".scratch/evidence/clu-unsent-resume-20261003-r2/report-red.json"
EVIDENCE_ROOT = ROOT / ".scratch/evidence/clu-unsent-boundaries-20261003-r1"
EXPECTED_RESERVATION_CNY = Decimal("0.07628400")


def digest_json(value):
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"), default=str).encode("utf-8")
    return sha256(encoded).hexdigest()


def digest_file(path):
    return sha256(Path(path).read_bytes()).hexdigest()


def fixed_request():
    request = json.loads(FIXED_REQUEST_PATH.read_text(encoding="utf-8"))
    report = json.loads(FIXED_REPORT_PATH.read_text(encoding="utf-8"))
    assert report["status"] == "red_reproduced"
    assert request == report["original_request"]
    assert request["service"] == "deepseek"
    assert request["body"]["model"] == "deepseek-flash"
    assert request["output_limit"] == 8192
    return request, report


def source_hashes():
    paths = (
        ROOT / "tests/test_cluster_unsent_budget.py",
        ROOT / "src/shop/budget.py",
        ROOT / "src/shop/runtime_models.py",
        ROOT / "src/shop/integration_models.py",
        ROOT / "agent-cluster/src/agent_cluster/store.py",
        ROOT / "agent-cluster/src/agent_cluster/types.py",
        FIXED_REQUEST_PATH,
        FIXED_REPORT_PATH,
    )
    return {str(path.relative_to(ROOT)): digest_file(path) for path in paths}


def save_evidence(case, report):
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ") + "-" + uuid4().hex[:8]
    directory = EVIDENCE_ROOT / case / run_id
    directory.mkdir(parents=True, exist_ok=False)
    report.update({
        "evidence_run_id": run_id,
        "milestone": ["CLU-05"],
        "recorded_at": datetime.now(UTC).isoformat(),
        "status": "passed",
        "model_calls": 0,
        "source_sha256": source_hashes(),
    })
    path = directory / "report.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n",
                    encoding="utf-8")
    return str(path.relative_to(ROOT))


def database_identity(engine):
    with engine.connect() as connection:
        return dict(connection.execute(text(
            "select current_database() as database, current_schema() as schema")).mappings().one())


def request_snapshot(maker, request_id):
    with maker() as session:
        row = session.get(ProviderRequest, request_id)
        assert row is not None
        return {
            "id": row.id,
            "run_id": row.run_id,
            "step_key": row.step_key,
            "service": row.service,
            "model": row.model,
            "status": row.status,
            "reservation_cny": str(row.reservation_cny),
            "charged_cny": None if row.charged_cny is None else str(row.charged_cny),
            "error": row.error,
            "request_body_sha256": digest_json(row.request_body),
            "price_basis_sha256": digest_json(row.price_basis),
            "unsent_resumptions": row.price_basis.get("unsent_resumptions", []),
            "response_body_sha256": None if row.response_body is None else digest_json(row.response_body),
            "usage": row.usage,
            "provider_request_id": row.provider_request_id,
            "created_at": row.created_at.isoformat(),
            "completed_at": None if row.completed_at is None else row.completed_at.isoformat(),
        }


def account_snapshot(maker):
    with maker() as session:
        account = session.get(BudgetAccount, "project")
        assert account is not None
        return {"spent_cny": str(account.spent_cny), "reserved_cny": str(account.reserved_cny)}


def request_ids(maker, run_id):
    with maker() as session:
        return session.scalars(select(ProviderRequest.id).where(
            ProviderRequest.run_id == run_id).order_by(ProviderRequest.id)).all()


def request_count(maker):
    with maker() as session:
        return session.scalar(select(func.count()).select_from(ProviderRequest))


def create_scenario(db_session, *, goal_budget, window_limit=Decimal(5)):
    request, source_report = fixed_request()
    Base.metadata.create_all(db_session.bind)
    store = ClusterStore(db_session.bind)
    store.initialize()
    maker = sessionmaker(db_session.bind, expire_on_commit=False)
    window_id = "clu-unsent-boundary-" + uuid4().hex
    db_session.add(IntegrationBudgetWindow(id=window_id, limit_cny=window_limit, active=True))
    db_session.commit()
    budget = BudgetService(maker, verification_scope="cluster")
    with maker.begin() as session:
        budget.account(session)
    actor_id = source_report["actor_id"]
    goal_id = "clu-unsent-boundary-" + uuid4().hex
    goal = GoalDefinition(
        goal_id=goal_id,
        actor_id=actor_id,
        objective="复核集群协调请求的未发送费用恢复边界。",
        allowed_operations={"shop_customer_agent": ["orders"]},
        criteria=[CompletionCriterion(
            tool_name="shop_customer_agent", action="orders", path=["data", "orders"],
            operator="exists")],
        budget_cny=goal_budget,
        max_decisions=1,
        max_tasks=1,
    )
    store.create_goal(goal)
    run = store.trigger(TriggerEvent(
        goal_id=goal_id, actor_id=actor_id, event_id="clu-unsent-boundary-" + uuid4().hex,
        kind="event", payload={}))
    store.set_run(run.run_id, actor_id, status="running")
    return {
        "request": request,
        "source_report": source_report,
        "store": store,
        "maker": maker,
        "budget": budget,
        "actor_id": actor_id,
        "goal_id": goal_id,
        "run_id": run.run_id,
        "window_id": window_id,
    }


def reserve_request(scenario, step_key):
    request = scenario["request"]
    return scenario["budget"].reserve(
        task_id=None,
        run_id=scenario["run_id"],
        step_key=step_key,
        service=request["service"],
        model=request["body"]["model"],
        request_body=request["body"],
        input_limit=request["input_limit"],
        output_limit=request["output_limit"],
        verification=True,
    )


def cancel_original_after_stop(scenario, request, action):
    scenario["store"].control_goal(scenario["goal_id"], scenario["actor_id"], action)
    with pytest.raises(BudgetExceeded, match="cancelled"):
        scenario["budget"].mark_sent(request.id)
    stored = request_snapshot(scenario["maker"], request.id)
    assert stored["status"] == "cancelled"
    assert stored["error"] == "cluster_stopped"
    assert Decimal(stored["charged_cny"]) == 0
    assert Decimal(stored["reservation_cny"]) == EXPECTED_RESERVATION_CNY
    return stored


def assert_resume_rejected(scenario, request_id, error_type, message):
    before_request = request_snapshot(scenario["maker"], request_id)
    before_account = account_snapshot(scenario["maker"])
    before_ids = request_ids(scenario["maker"], scenario["run_id"])
    with pytest.raises(error_type, match=message):
        scenario["budget"].resume_unsent(request_id)
    after_request = request_snapshot(scenario["maker"], request_id)
    after_account = account_snapshot(scenario["maker"])
    after_ids = request_ids(scenario["maker"], scenario["run_id"])
    assert after_request == before_request
    assert after_account == before_account
    assert after_ids == before_ids
    return {
        "request_before": before_request,
        "request_after": after_request,
        "account_before": before_account,
        "account_after": after_account,
        "request_ids_before": before_ids,
        "request_ids_after": after_ids,
        "rejected_with": error_type.__name__,
        "expected_message": message,
        "failed_resume_added_reservation": False,
        "failed_resume_added_request": False,
    }


def base_evidence(scenario, case):
    return {
        "case": case,
        "database": database_identity(scenario["maker"].kw["bind"]),
        "fixed_request": {
            "source_path": str(FIXED_REQUEST_PATH.relative_to(ROOT)),
            "source_red_report": str(FIXED_REPORT_PATH.relative_to(ROOT)),
            "source_run_id": scenario["source_report"]["run_id"],
            "step_key": scenario["request"]["step_key"],
            "service": scenario["request"]["service"],
            "model": scenario["request"]["body"]["model"],
            "output_limit": scenario["request"]["output_limit"],
            "body_sha256": digest_json(scenario["request"]["body"]),
            "reservation_cny": str(EXPECTED_RESERVATION_CNY),
        },
        "goal_id": scenario["goal_id"],
        "run_id": scenario["run_id"],
        "window_id": scenario["window_id"],
    }


@pytest.mark.parametrize("action", ["pause", "cancel"])
def test_resume_unsent_rejects_stopped_goal_without_changing_original_request(db_session, action):
    scenario = create_scenario(db_session, goal_budget=Decimal(5))
    original = reserve_request(scenario, "unsent-boundary-original")
    assert original.reservation_cny == EXPECTED_RESERVATION_CNY
    before_stop = request_snapshot(scenario["maker"], original.id)
    cancelled = cancel_original_after_stop(scenario, original, action)
    rejected = assert_resume_rejected(
        scenario, original.id, BudgetExceeded, "目标尚未恢复付费调用")
    with scenario["maker"]() as session:
        goal = session.get(GoalRecord, scenario["goal_id"])
        run = session.get(RunRecord, scenario["run_id"])
        assert goal.state == ("paused" if action == "pause" else "cancelled")
        assert run.status == goal.state
    report = base_evidence(scenario, f"{action}-goal")
    report.update({"goal_state": goal.state, "run_status": run.status,
                   "request_before_stop": before_stop, "request_after_stop": cancelled,
                   "resume_rejection": rejected})
    report_path = save_evidence(f"{action}-goal", report)
    assert Path(ROOT / report_path).is_file()


def test_resume_unsent_rejects_changed_goal_revision(db_session):
    scenario = create_scenario(db_session, goal_budget=Decimal(5))
    original = reserve_request(scenario, "unsent-boundary-original")
    assert original.reservation_cny == EXPECTED_RESERVATION_CNY
    cancelled = cancel_original_after_stop(scenario, original, "pause")
    current = scenario["store"].list_goals(scenario["actor_id"])[0]
    updated = scenario["store"].update_goal(
        current.model_copy(update={"objective": current.objective + " 版本核验"}),
        expected_revision=current.revision,
    )
    with scenario["maker"]() as session:
        run = session.get(RunRecord, scenario["run_id"])
        assert updated.revision == current.revision + 1
        assert run.document["goal_revision"] == current.revision
    rejected = assert_resume_rejected(
        scenario, original.id, ValueError, "恢复请求的目标身份或版本不一致")
    report = base_evidence(scenario, "goal-revision")
    report.update({"old_goal_revision": current.revision,
                   "current_goal_revision": updated.revision,
                   "run_goal_revision": run.document["goal_revision"],
                   "request_after_stop": cancelled, "resume_rejection": rejected})
    report_path = save_evidence("goal-revision", report)
    assert Path(ROOT / report_path).is_file()


def test_resume_unsent_rejects_changed_active_budget_window(db_session):
    scenario = create_scenario(db_session, goal_budget=Decimal(5))
    original = reserve_request(scenario, "unsent-boundary-original")
    assert original.reservation_cny == EXPECTED_RESERVATION_CNY
    cancelled = cancel_original_after_stop(scenario, original, "pause")
    scenario["store"].control_goal(scenario["goal_id"], scenario["actor_id"], "resume")
    next_window_id = "clu-unsent-boundary-" + uuid4().hex
    with scenario["maker"].begin() as session:
        current_window = session.get(IntegrationBudgetWindow, scenario["window_id"])
        current_window.active = False
        session.add(IntegrationBudgetWindow(
            id=next_window_id, limit_cny=Decimal(5), active=True))
    rejected = assert_resume_rejected(
        scenario, original.id, ValueError, "恢复请求的费用窗口与原始记录不一致")
    with scenario["maker"]() as session:
        active_window = session.scalar(select(IntegrationBudgetWindow).where(
            IntegrationBudgetWindow.active))
        assert active_window.id == next_window_id
    report = base_evidence(scenario, "budget-window-identity")
    report.update({"request_after_stop": cancelled,
                   "request_window_id": original.price_basis["integration_run_id"],
                   "active_window_id": active_window.id,
                   "resume_rejection": rejected})
    report_path = save_evidence("budget-window-identity", report)
    assert Path(ROOT / report_path).is_file()


@pytest.mark.parametrize("limit_kind", ["goal", "window"])
def test_resume_unsent_rechecks_goal_and_window_limits_after_two_real_reservations(
    db_session, limit_kind,
):
    exact_limit = EXPECTED_RESERVATION_CNY * 2
    scenario = create_scenario(
        db_session,
        goal_budget=exact_limit if limit_kind == "goal" else Decimal(5),
        window_limit=exact_limit if limit_kind == "window" else Decimal(5),
    )
    original = reserve_request(scenario, "unsent-boundary-original")
    assert original.reservation_cny == EXPECTED_RESERVATION_CNY
    cancelled = cancel_original_after_stop(scenario, original, "pause")
    scenario["store"].control_goal(scenario["goal_id"], scenario["actor_id"], "resume")
    fillers = [reserve_request(scenario, f"unsent-boundary-filler-{index}")
               for index in (1, 2)]
    assert [item.reservation_cny for item in fillers] == [
        EXPECTED_RESERVATION_CNY, EXPECTED_RESERVATION_CNY]
    before_rejection = account_snapshot(scenario["maker"])
    before_ids = request_ids(scenario["maker"], scenario["run_id"])
    expected_error = "目标预算不足" if limit_kind == "goal" else "接口集成验证预算不足"
    rejected = assert_resume_rejected(
        scenario, original.id, BudgetExceeded, expected_error)
    assert account_snapshot(scenario["maker"]) == before_rejection
    assert request_ids(scenario["maker"], scenario["run_id"]) == before_ids
    filler_before_cleanup = [request_snapshot(scenario["maker"], item.id) for item in fillers]
    for item in fillers:
        scenario["budget"].cancel_unsent(item.id, f"{limit_kind}预算拒绝检查清理")
    account_after_cleanup = account_snapshot(scenario["maker"])
    assert Decimal(account_after_cleanup["reserved_cny"]) == 0
    filler_after_cleanup = [request_snapshot(scenario["maker"], item.id) for item in fillers]
    assert all(item["status"] == "cancelled" and Decimal(item["charged_cny"]) == 0
               for item in filler_after_cleanup)
    report = base_evidence(scenario, f"{limit_kind}-budget")
    report.update({
        "request_after_stop": cancelled,
        "filler_reservations": filler_before_cleanup,
        "resume_rejection": rejected,
        "account_before_rejection": before_rejection,
        "account_after_rejection": before_rejection,
        "account_after_cleanup": account_after_cleanup,
        "filler_requests_after_cleanup": filler_after_cleanup,
        "request_ids_before_rejection": before_ids,
        "request_ids_after_rejection": before_ids,
    })
    report_path = save_evidence(f"{limit_kind}-budget", report)
    assert Path(ROOT / report_path).is_file()


def main_cluster_unknown_snapshot():
    engine = create_engine(Settings().database_url, pool_size=1, max_overflow=0)
    try:
        with engine.connect() as connection, connection.begin():
            connection.execute(text("SET TRANSACTION READ ONLY"))
            identity = dict(connection.execute(text(
                "select current_database() as database, current_schema() as schema"
            )).mappings().one())
            rows = connection.execute(select(*ProviderRequest.__table__.columns).where(
                ProviderRequest.status == "unknown").order_by(
                    ProviderRequest.created_at.desc(), ProviderRequest.id)).mappings().all()
            candidates = [dict(row) for row in rows
                          if row["price_basis"].get("verification_scope") == "cluster"
                          and row["price_basis"].get("cluster_run_id") is not None
                          and row["task_id"] is None and row["assessment_id"] is None]
            assert candidates, "主数据库缺少可供只读复制的真实cluster unknown请求"
            values = candidates[0]
        return identity, values
    finally:
        engine.dispose()


def test_resume_unsent_rejects_read_only_copy_of_real_cluster_unknown(db_session):
    identity_before, source = main_cluster_unknown_snapshot()
    assert identity_before["database"] != db_session.bind.url.database
    Base.metadata.create_all(db_session.bind)
    store = ClusterStore(db_session.bind)
    store.initialize()
    maker = sessionmaker(db_session.bind, expire_on_commit=False)
    budget = BudgetService(maker, verification_scope="cluster")
    with maker.begin() as session:
        budget.account(session)
        copied = ProviderRequest(**source)
        session.add(copied)
    copied_before = request_snapshot(maker, source["id"])
    account_before = account_snapshot(maker)
    request_count_before = request_count(maker)
    with pytest.raises(ValueError, match="此请求没有可恢复的未发送预留"):
        budget.resume_unsent(source["id"])
    copied_after = request_snapshot(maker, source["id"])
    account_after = account_snapshot(maker)
    request_count_after = request_count(maker)
    assert copied_after == copied_before
    assert account_after == account_before
    assert request_count_after == request_count_before == 1
    identity_after, preserved_source = main_cluster_unknown_snapshot()
    assert identity_after == identity_before
    assert {key: preserved_source[key] for key in source} == source
    report = {
        "case": "real-cluster-unknown-copy",
        "database": database_identity(maker.kw["bind"]),
        "source_database": identity_before,
        "source_request": {
            "id": source["id"],
            "run_id": source["run_id"],
            "step_key": source["step_key"],
            "status": source["status"],
            "reservation_cny": str(source["reservation_cny"]),
            "charged_cny": None if source["charged_cny"] is None else str(source["charged_cny"]),
            "model": source["model"],
            "request_body_sha256": digest_json(source["request_body"]),
            "price_basis_sha256": digest_json(source["price_basis"]),
        },
        "isolated_copy_before": copied_before,
        "isolated_copy_after": copied_after,
        "account_before": account_before,
        "account_after": account_after,
        "request_count_before": request_count_before,
        "request_count_after": request_count_after,
        "source_preserved_read_only": True,
        "failed_resume_added_reservation": False,
        "failed_resume_added_request": False,
    }
    report_path = save_evidence("real-cluster-unknown-copy", report)
    assert Path(ROOT / report_path).is_file()
