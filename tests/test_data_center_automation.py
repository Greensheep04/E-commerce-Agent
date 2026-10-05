import asyncio
import json
import os
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from agent_cluster.notes import NoteStore
from agent_cluster.runtime import ClusterRuntime
from agent_cluster.store import ClusterStore
from agent_cluster.types import (
    CompletionCriterion,
    GoalDefinition,
    TaskPlan,
    TaskSpec,
    TriggerEvent,
)
from data_center_protocol import ResourceRef
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from shop.assessment_order_adapter import ORDER_FILE_SHA256
from shop.auth import hash_password
from shop.config import Settings
from shop.data_center_automation import DataCenterAutomation
from shop.data_center_models import DataCenterGrant, DataCenterSource, DataCenterWatch
from shop.integration import ShopAuthentication
from shop.models import User
from shop.runtime_models import ProviderRequest


def automation_environment(db_session, tmp_path: Path):
    user = User(username=f"data-watch-{uuid4().hex}", password_hash=hash_password("local-test"),
                role="admin")
    db_session.add(user)
    db_session.flush()
    source_path = tmp_path / "registered-source.txt"
    source_path.write_text("来源版本一", encoding="utf-8")
    checksum = sha256(source_path.read_bytes()).hexdigest()
    reference = ResourceRef(module_id="data_center", resource_type="source",
                            resource_id="registered-source.txt", version="v1", sha256=checksum)
    source_id = "automation/source-" + uuid4().hex
    db_session.add(DataCenterSource(
        source_id=source_id, kind="document", title="自动化来源", revision="v1",
        nature="technical_sample", license_id="project-owned",
        resource=reference.model_dump(mode="json"), relative_path=source_path.name,
        sha256=checksum, properties={"record_id": "registered-source"}, active=True,
    ))
    db_session.flush()
    db_session.add(DataCenterGrant(actor_id=user.id, source_id=source_id, permissions=["read"],
                                  asset_ids=["registered-source"], source_wide=False))
    db_session.commit()

    maker = sessionmaker(db_session.bind, expire_on_commit=False)
    settings = Settings(session_secret="data-watch-test", data_center_source_root=tmp_path,
                        cluster_notes_dir=tmp_path / "notes")
    authentication = ShopAuthentication(settings, maker)
    store = ClusterStore(db_session.bind)
    store.initialize()
    cluster = SimpleNamespace(store=store, runtime=ClusterRuntime(store, engine=None),
                              notes=NoteStore(settings.cluster_notes_dir))
    integration = SimpleNamespace(auth=authentication)
    goal = store.create_goal(GoalDefinition(
        actor_id=user.id, objective="检查登记来源的变化",
        allowed_operations={"shop_data_center_agent": ["ingest"]},
        criteria=[CompletionCriterion(tool_name="shop_data_center_agent", action="ingest")],
    ))

    automation = DataCenterAutomation(cluster, integration, settings, maker)
    return automation, cluster, goal, user, source_id, source_path, db_session


@pytest.mark.asyncio
async def test_document_source_event_is_authorized_and_deduplicated(db_session, tmp_path):
    automation, cluster, goal, user, source_id, source_path, session = automation_environment(
        db_session, tmp_path)
    watch = automation.install_watch(goal.goal_id, user.id, [source_id], operation="ingest")

    assert automation.accepts_goal(goal.goal_id) is True
    first = await automation.trigger_source_event(source_id)
    repeated = await automation.trigger_source_event(source_id)

    assert watch.source_ids == [source_id]
    assert first.run_id == repeated.run_id
    assert len(cluster.store.list_runs(goal.goal_id, user.id)) == 1

    source = session.get(DataCenterSource, source_id)
    source_path.write_text("来源版本二", encoding="utf-8")
    checksum = sha256(source_path.read_bytes()).hexdigest()
    reference = ResourceRef(module_id="data_center", resource_type="source",
                            resource_id="registered-source.txt", version="v2", sha256=checksum)
    source.revision = "v2"
    source.sha256 = checksum
    source.resource = reference.model_dump(mode="json")
    session.commit()
    changed = await automation.trigger_source_event(source_id)
    assert changed.run_id != first.run_id
    assert len(cluster.store.list_runs(goal.goal_id, user.id)) == 2

    source.active = False
    session.commit()
    with pytest.raises(PermissionError, match="来源未登记或已经撤回"):
        await automation.trigger_source_event(source_id)


@pytest.mark.asyncio
async def test_unchanged_source_waits_without_tasks_and_writes_note(db_session, tmp_path):
    automation, cluster, goal, user, source_id, _source_path, _session = automation_environment(
        db_session, tmp_path)
    automation.install_watch(goal.goal_id, user.id, [source_id], operation="ingest")
    context = automation.integration.auth.issue(user.id, str(uuid4()))
    baseline = await automation.source_signature([source_id], context)
    first_run = cluster.store.trigger(TriggerEvent(event_id="initial-source-" + uuid4().hex,
                                                  goal_id=goal.goal_id, actor_id=user.id))
    assert await automation.preflight(first_run) is False
    cluster.store.set_run(first_run.run_id, user.id, status="completed")
    await automation.after_run(first_run)
    run = cluster.store.trigger(TriggerEvent(event_id="unchanged-source-" + uuid4().hex,
                                             goal_id=goal.goal_id, actor_id=user.id))

    assert await asyncio.gather(automation.preflight(run), automation.preflight(run)) == [True, True]

    current = cluster.store.get_run(run.run_id, user.id)
    assert current.status == "waiting"
    assert cluster.store.tasks(run.run_id, user.id) == []
    with db_session.no_autoflush:
        saved_watch = db_session.get(DataCenterWatch, automation._watch_for_goal(
            goal.goal_id, user.id).watch_id)
        assert saved_watch.committed_signature == baseline
    notes = cluster.notes.index(goal_id=goal.goal_id, actor_id=user.id)
    assert any(note.topic == "data-center-watch" for note in notes)
    note_versions = [note for note in notes if note.topic == "data-center-watch"]
    assert len(note_versions) == 1 and note_versions[0].version == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_status", ["queued", "running", "unknown", "failed"])
async def test_same_source_waits_for_prior_unresolved_run(db_session, tmp_path, prior_status):
    automation, cluster, goal, user, source_id, _source_path, session = automation_environment(
        db_session, tmp_path)
    automation.install_watch(goal.goal_id, user.id, [source_id], operation="ingest")
    prior = cluster.store.trigger(TriggerEvent(
        event_id="pending-source-" + uuid4().hex, goal_id=goal.goal_id, actor_id=user.id))
    assert await automation.preflight(prior) is False
    cluster.store.set_run(prior.run_id, user.id, status=prior_status)
    following = cluster.store.trigger(TriggerEvent(
        event_id="following-period-" + uuid4().hex, goal_id=goal.goal_id, actor_id=user.id))
    requests_before = list(session.scalars(select(ProviderRequest.id).order_by(ProviderRequest.id)))

    assert await automation.preflight(following) is True

    assert cluster.store.get_run(following.run_id, user.id).status == "waiting"
    assert cluster.store.tasks(following.run_id, user.id) == []
    assert cluster.store.get_run(prior.run_id, user.id).status == prior_status
    notes = cluster.notes.index(goal_id=goal.goal_id, actor_id=user.id)
    waiting_note = next(note for note in notes if note.topic == "data-center-watch")
    content = json.loads(cluster.notes.read(waiting_note, actor_id=user.id, goal_id=goal.goal_id))
    assert content["result"] == "pending"
    assert content["prior_run_id"] == prior.run_id
    assert list(session.scalars(select(ProviderRequest.id).order_by(ProviderRequest.id))) == requests_before
    if prior_status == "queued":
        assert await automation.preflight(prior) is False


@pytest.mark.asyncio
async def test_parallel_periods_claim_source_once(db_session, tmp_path):
    automation, cluster, goal, user, source_id, _source_path, _session = automation_environment(
        db_session, tmp_path)
    automation.install_watch(goal.goal_id, user.id, [source_id], operation="ingest")
    runs = [cluster.store.trigger(TriggerEvent(
        event_id="parallel-period-" + uuid4().hex, goal_id=goal.goal_id, actor_id=user.id))
        for _ in range(2)]

    assert sorted(await asyncio.gather(*(automation.preflight(run) for run in runs))) == [False, True]

    assert sorted(cluster.store.get_run(run.run_id, user.id).status for run in runs) == ["queued", "waiting"]
    assert all(cluster.store.tasks(run.run_id, user.id) == [] for run in runs)


@pytest.mark.asyncio
async def test_metadata_revision_triggers_new_source_signature(db_session, tmp_path):
    automation, cluster, goal, user, source_id, _source_path, session = automation_environment(
        db_session, tmp_path)
    automation.install_watch(goal.goal_id, user.id, [source_id], operation="ingest")
    original = await automation.trigger_source_event(source_id)
    source = session.get(DataCenterSource, source_id)
    original_sha256 = source.sha256
    source.revision = "v2-metadata"
    source.properties = {**source.properties, "description": "来源说明第二版"}
    session.commit()

    changed = await automation.trigger_source_event(source_id)

    assert changed.run_id != original.run_id
    assert source.sha256 == original_sha256
    assert len(cluster.store.list_runs(goal.goal_id, user.id)) == 2


@pytest.mark.asyncio
async def test_completed_run_recovers_source_commit_before_next_period(db_session, tmp_path):
    automation, cluster, goal, user, source_id, _source_path, _session = automation_environment(
        db_session, tmp_path)
    automation.install_watch(goal.goal_id, user.id, [source_id], operation="ingest")
    prior = cluster.store.trigger(TriggerEvent(
        event_id="completed-source-" + uuid4().hex, goal_id=goal.goal_id, actor_id=user.id))
    assert await automation.preflight(prior) is False
    cluster.store.set_run(prior.run_id, user.id, status="completed")
    following = cluster.store.trigger(TriggerEvent(
        event_id="completed-following-" + uuid4().hex, goal_id=goal.goal_id, actor_id=user.id))

    assert await automation.preflight(following) is True

    assert cluster.store.get_run(following.run_id, user.id).status == "waiting"
    watch = automation._watch_for_goal(goal.goal_id, user.id)
    assert watch.committed_signature == watch.observed_signatures[prior.run_id]
    note = next(note for note in cluster.notes.index(goal_id=goal.goal_id, actor_id=user.id)
                if note.topic == "data-center-watch")
    assert json.loads(cluster.notes.read(note, actor_id=user.id, goal_id=goal.goal_id))["result"] == "unchanged"


@pytest.mark.asyncio
async def test_waiting_period_does_not_prevent_recovery_after_prior_cancel(db_session, tmp_path):
    automation, cluster, goal, user, source_id, _source_path, _session = automation_environment(
        db_session, tmp_path)
    automation.install_watch(goal.goal_id, user.id, [source_id], operation="ingest")
    prior = cluster.store.trigger(TriggerEvent(
        event_id="cancelled-prior-" + uuid4().hex, goal_id=goal.goal_id, actor_id=user.id))
    assert await automation.preflight(prior) is False
    waiting = cluster.store.trigger(TriggerEvent(
        event_id="waiting-period-" + uuid4().hex, goal_id=goal.goal_id, actor_id=user.id))
    assert await automation.preflight(waiting) is True
    cluster.store.set_run(prior.run_id, user.id, status="cancelled")
    following = cluster.store.trigger(TriggerEvent(
        event_id="after-cancel-period-" + uuid4().hex, goal_id=goal.goal_id, actor_id=user.id))

    assert await automation.preflight(following) is False

    assert cluster.store.get_run(waiting.run_id, user.id).status == "waiting"
    assert cluster.store.get_run(prior.run_id, user.id).status == "cancelled"


@pytest.mark.asyncio
async def test_nightly_source_check_rejects_file_digest_drift(db_session, tmp_path):
    automation, _cluster, goal, user, source_id, source_path, _session = automation_environment(
        db_session, tmp_path)
    automation.install_watch(goal.goal_id, user.id, [source_id], operation="ingest")
    source_path.write_text("来源内容已经变化", encoding="utf-8")
    context = automation.integration.auth.issue(user.id, str(uuid4()))

    with pytest.raises(ValueError, match="摘要"):
        await automation.source_signature([source_id], context)


@pytest.mark.asyncio
async def test_existing_task_bypasses_source_preflight(db_session, tmp_path):
    automation, cluster, goal, user, source_id, _source_path, _session = automation_environment(
        db_session, tmp_path)
    automation.install_watch(goal.goal_id, user.id, [source_id], operation="ingest")
    context = automation.integration.auth.issue(user.id, str(uuid4()))
    baseline = await automation.source_signature([source_id], context)
    initial = cluster.store.trigger(TriggerEvent(event_id="baseline-" + uuid4().hex,
                                                 goal_id=goal.goal_id, actor_id=user.id))
    assert await automation.preflight(initial) is False
    cluster.store.set_run(initial.run_id, user.id, status="completed")
    await automation.after_run(initial)

    resumed = cluster.store.trigger(TriggerEvent(event_id="resume-existing-" + uuid4().hex,
                                                 goal_id=goal.goal_id, actor_id=user.id))
    cluster.store.save_plan(resumed.run_id, user.id,
                            TaskPlan(tasks=[TaskSpec(task_id="read-source",
                                                     tool_name="shop_data_center_agent",
                                                     action="ingest")]),
                            expected_version=0)

    assert await automation.preflight(resumed) is False
    assert cluster.store.get_run(resumed.run_id, user.id).status == "queued"
    cluster.store.set_run(resumed.run_id, user.id, status="completed")
    await automation.after_run(resumed)
    with automation.maker() as session:
        saved_watch = session.scalar(select(DataCenterWatch).where(
            DataCenterWatch.goal_id == goal.goal_id))
        assert saved_watch.committed_signature == baseline


def test_sync_watch_registers_default_and_nightly_schedules(db_session, tmp_path):
    automation, cluster, _goal, _admin, _source_id, _source_path, session = automation_environment(
        db_session, tmp_path)
    sync_user = User(username=f"data-watch-sync-{uuid4().hex}",
                     password_hash=hash_password("local-test"), role="data_sync")
    session.add(sync_user)
    session.flush()
    source_id = "microsoft/adventureworks"
    session.add(DataCenterSource(
        source_id=source_id, kind="analytics", title="AdventureWorks季度经营数据",
        revision="adventureworks-test", nature="public_business_sample", license_id="MIT",
        sha256=ORDER_FILE_SHA256, properties={"employee_manifest": {"revision": "adventureworks-test"}},
        active=True,
    ))
    session.flush()
    session.add(DataCenterGrant(actor_id=sync_user.id, source_id=source_id,
                                permissions=["sync"], asset_ids=[], source_wide=True))
    session.commit()
    goal = cluster.store.create_goal(GoalDefinition(
        actor_id=sync_user.id, objective="同步已登记来源",
        allowed_operations={"shop_data_center_agent": ["sync"]},
        criteria=[CompletionCriterion(tool_name="shop_data_center_agent", action="sync")],
    ))

    automation.install_watch(goal.goal_id, sync_user.id, [source_id], operation="sync")

    schedules = cluster.store.schedules(sync_user.id)
    assert sorted(schedule.interval_seconds for schedule in schedules if schedule.interval_seconds) == [900]
    assert sorted(schedule.cron for schedule in schedules if schedule.cron) == ["0 2 * * *"]
    with pytest.raises(ValueError, match="来源监听操作与来源类型不匹配"):
        automation.install_watch(goal.goal_id, sync_user.id, [_source_id], operation="sync")


@pytest.mark.asyncio
async def test_paused_goal_does_not_become_waiting(db_session, tmp_path):
    automation, cluster, goal, user, source_id, _source_path, _session = automation_environment(
        db_session, tmp_path)
    automation.install_watch(goal.goal_id, user.id, [source_id], operation="ingest")
    run = cluster.store.trigger(TriggerEvent(event_id="paused-source-" + uuid4().hex,
                                             goal_id=goal.goal_id, actor_id=user.id))
    cluster.store.control_goal(goal.goal_id, user.id, "pause")

    assert await automation.preflight(run) is True

    assert cluster.store.get_run(run.run_id, user.id).status == "paused"
    with automation.maker() as session:
        watch = session.scalar(select(DataCenterWatch).where(DataCenterWatch.goal_id == goal.goal_id))
        assert watch.observed_signatures == {}
    assert cluster.notes.index(goal_id=goal.goal_id, actor_id=user.id) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["pause", "cancel"])
async def test_inactive_goal_does_not_block_source_event(db_session, tmp_path, action):
    automation, cluster, goal, user, source_id, _source_path, _session = automation_environment(
        db_session, tmp_path)
    automation.install_watch(goal.goal_id, user.id, [source_id], operation="ingest")
    cluster.store.control_goal(goal.goal_id, user.id, action)

    assert await automation.trigger_source_event(source_id) == []
    assert cluster.store.list_runs(goal.goal_id, user.id) == []
    assert cluster.notes.index(goal_id=goal.goal_id, actor_id=user.id) == []


def test_cancelled_goal_cannot_reactivate_an_existing_watch(db_session, tmp_path):
    automation, cluster, goal, user, source_id, _source_path, session = automation_environment(
        db_session, tmp_path)
    watch = automation.install_watch(goal.goal_id, user.id, [source_id], operation="ingest")
    cluster.store.control_goal(goal.goal_id, user.id, "cancel")
    saved_watch = session.get(DataCenterWatch, watch.watch_id)
    saved_watch.active = False
    session.commit()
    before = [schedule.model_dump(mode="json") for schedule in cluster.store.schedules(user.id)]
    model_requests_before = list(session.scalars(select(ProviderRequest.id).order_by(ProviderRequest.id)))

    with pytest.raises(ValueError, match="已取消"):
        automation.install_watch(goal.goal_id, user.id, [source_id], operation="ingest")

    session.expire_all()
    assert session.get(DataCenterWatch, watch.watch_id).active is False
    assert [schedule.model_dump(mode="json") for schedule in cluster.store.schedules(user.id)] == before
    assert cluster.store.goal_state(goal.goal_id, user.id) == "cancelled"
    model_requests_after = list(session.scalars(select(ProviderRequest.id).order_by(ProviderRequest.id)))
    assert model_requests_after == model_requests_before
    if os.environ.get("DC_WATCH_INSTALL_OUTPUT"):
        root = Path(__file__).resolve().parents[1]
        output = Path(os.environ["DC_WATCH_INSTALL_OUTPUT"])
        output.parent.mkdir(parents=True, exist_ok=True)
        source = "src/shop/data_center_automation.py"
        output.write_text(json.dumps({
            "schema": "dc06.watch_installation_integrity.v1", "status": "passed",
            "captured_at": datetime.now(UTC).isoformat(), "database": "shop_test",
            "goal_id": goal.goal_id, "actor_id": user.id, "watch_id": watch.watch_id,
            "goal_state": "cancelled", "watch_active": False,
            "model_requests_before": model_requests_before, "model_requests_after": model_requests_after,
            "checks": {"cancelled_goal_install_denied": True, "prior_watch_unchanged": True,
                       "prior_schedules_unchanged": True, "zero_new_model_calls": True},
            "source_sha256": {source: sha256((root / source).read_bytes()).hexdigest()},
            "schedules_before": before,
            "schedules_after": [schedule.model_dump(mode="json")
                                for schedule in cluster.store.schedules(user.id)],
        }, ensure_ascii=False, indent=2) + "\n")
