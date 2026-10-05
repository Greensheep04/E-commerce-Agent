import json
import os
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import pytest
from scripts.cluster_quality import (
    FIXTURE_CHECKS,
    _order_list_matches,
    _same_order,
    _ticket_offer_source,
    verify_result,
)
from shop.config import Settings
from shop.customer_runtime_models import CustomerConversation
from shop.models import Asset, AssetVersion, Source, User
from shop.orders import OrderQueryService
from shop.runtime_models import HarnessTask
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / "agent-cluster/tests/fixtures/goals_v1.json"
INPUTS = ROOT / ".scratch/evidence/clu-inputs-20261002T164037Z-5958ff50/inputs.json"
PROBES = ROOT / ".scratch/evidence/clu06-remaining-probe-20261003-r1"
FORMAL = ROOT / ".scratch/evidence/clu-formal-20261003-r1"


def test_fixture_quality_checks_have_complete_explicit_coverage():
    fixture = json.loads(FIXTURE.read_text())
    declared = {check for case in fixture["cases"] for check in case["checks"]}
    assert declared == set(FIXTURE_CHECKS)


def test_unknown_quality_check_is_rejected():
    fixture = json.loads(FIXTURE.read_text())
    case = dict(fixture["cases"][0], checks=["unknown_check"])
    with pytest.raises(ValueError, match="unknown_check"):
        verify_result(case, {"status": "completed"}, [], None, {})


def test_cc04_probe_is_rejected_without_real_pending_ticket():
    from shop.config import Settings

    case = _fixture_case("CC04")
    directory = ROOT / ".scratch/evidence/clu-live-customer-probe-20261003-r2/CC04-1"
    run = json.loads((directory / "run.json").read_text())
    tasks = json.loads((directory / "tasks.json").read_text())
    inputs = json.loads(INPUTS.read_text())
    baseline = json.loads((directory.parent / "baseline.json").read_text())
    result = verify_result(case, run, tasks, Settings(), inputs, baseline)
    assert result["passed"] is False
    assert result["checks"]["pending_ticket"] is False
    assert result["checks"]["no_unconfirmed_ticket"] is True
    assert result["checks"]["failed_turn_preserved"] is True
    assert result["checks"]["turn_state_persisted"] is True
    assert any("pending_ticket" in issue for issue in result["issues"])


def test_latest_cc04_probe_requires_real_pending_ticket_without_ticket_creation():
    from shop.config import Settings

    case = _fixture_case("CC04")
    directory = ROOT / ".scratch/evidence/clu-live-customer-probe-20261003-r3/CC04-1"
    run = json.loads((directory / "run.json").read_text())
    tasks = json.loads((directory / "tasks.json").read_text())
    inputs = json.loads(INPUTS.read_text())
    baseline = json.loads((directory.parent / "baseline.json").read_text())
    result = verify_result(case, run, tasks, Settings(), inputs, baseline)
    assert result["passed"] is True
    assert result["checks"]["pending_ticket"] is True
    assert result["checks"]["no_unconfirmed_ticket"] is True
    assert result["checks"]["turn_state_persisted"] is True


def test_truncated_negative_run_is_quality_failure_without_false_completion_safety_issue():
    case = _fixture_case("CN02")
    directory = ROOT / ".scratch/evidence/clu-live-negative-probe-20261003-r2/CN02-1"
    run = json.loads((directory / "run.json").read_text())
    tasks = json.loads((directory / "tasks.json").read_text())
    inputs = json.loads(INPUTS.read_text())
    baseline = json.loads((directory.parent / "baseline.json").read_text())
    result = verify_result(case, run, tasks, Settings(), inputs, baseline)
    assert result["passed"] is False
    assert result["checks"]["no_false_completion"] is True
    assert result["safety_issues"] == []


@pytest.fixture
def real_order_samples():
    settings = Settings()
    inputs = json.loads(INPUTS.read_text())
    actor_id = inputs["accounts"]["other"]["actor_id"]
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    try:
        with Session(engine) as session:
            order_service = OrderQueryService(session, actor_id)
            offset = 0
            while True:
                page = order_service.list_orders(limit=20, offset=offset)
                summary = next((item for item in page["orders"] if item["item_count"] > 3), None)
                if summary:
                    full = order_service.get_order(summary["order_number"])
                    yield order_service, full, summary
                    break
                if not page["has_more"]:
                    raise AssertionError("真实账号来源中没有包含四条以上明细的订单")
                offset += len(page["orders"])
    finally:
        engine.dispose()


def test_real_order_detail_and_list_summary_match_source(real_order_samples):
    order_service, full, summary = real_order_samples
    task = {"arguments": {"payload": {"order_number": summary["order_number"], "limit": 20}}}
    actual_page = order_service.list_orders(order_number=summary["order_number"], limit=20)
    assert actual_page["orders"] == [summary]
    assert _same_order(full, full)
    assert _same_order(full, summary)
    assert _order_list_matches(order_service, task, actual_page)
    assert summary["items"] == full["items"][:3]
    assert summary["remaining_item_count"] == len(full["items"]) - len(summary["items"])


@pytest.mark.parametrize("field", ["source", "total_due", "items"])
@pytest.mark.parametrize("representation", ["full", "summary"])
def test_real_order_rejects_missing_required_fields(real_order_samples, field, representation):
    _, full, summary = real_order_samples
    reported = deepcopy(full if representation == "full" else summary)
    del reported[field]
    assert _same_order(full, reported) is False


def test_real_order_rejects_missing_line_and_changed_line_order(real_order_samples):
    _, full, summary = real_order_samples
    missing_line = deepcopy(full)
    missing_line["items"].pop()
    changed_order = deepcopy(full)
    changed_order["items"].reverse()
    shortened_summary = deepcopy(summary)
    shortened_summary["items"].pop()
    changed_summary_order = deepcopy(summary)
    changed_summary_order["items"].reverse()
    wrong_remaining_count = deepcopy(summary)
    wrong_remaining_count["remaining_item_count"] += 1
    assert _same_order(full, missing_line) is False
    assert _same_order(full, changed_order) is False
    assert _same_order(full, shortened_summary) is False
    assert _same_order(full, changed_summary_order) is False
    assert _same_order(full, wrong_remaining_count) is False


def test_real_order_rejects_missing_line_fields(real_order_samples):
    _, full, _ = real_order_samples
    reported = deepcopy(full)
    del reported["items"][0]["line_total"]
    assert _same_order(full, reported) is False


@pytest.mark.parametrize(
    ("case_id", "directory", "limit"),
    [
        ("CC01", ROOT / ".scratch/evidence/clu-live-probe-20261003-r2/CC01-1", 5),
        ("CC06", PROBES / "CC06-1", 3),
    ],
)
def test_recent_order_quality_rejects_real_nonzero_offset(case_id, directory, limit):
    settings = Settings()
    inputs = json.loads(INPUTS.read_text())
    run = json.loads((directory / "run.json").read_text())
    tasks = deepcopy(json.loads((directory / "tasks.json").read_text()))
    actor_id = inputs["accounts"][_fixture_case(case_id)["actor"]]["actor_id"]
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    try:
        with Session(engine) as session:
            order_service = OrderQueryService(session, actor_id)
            list_task = next(task for task in tasks
                             if task["spec"].get("action") == "orders")
            payload = list_task["arguments"]["payload"]
            payload["offset"] = 1
            list_task["result"]["data"] = order_service.list_orders(limit=limit, offset=1)
    finally:
        engine.dispose()

    result = verify_result(_fixture_case(case_id), run, tasks, settings, inputs)

    assert result["checks"]["orders"] is False


@pytest.mark.parametrize(
    ("case_id", "directory"),
    [
        ("CC01", ROOT / ".scratch/evidence/clu-live-probe-20261003-r2/CC01-1"),
        ("CC06", PROBES / "CC06-1"),
    ],
)
def test_recent_order_quality_accepts_default_zero_offset(case_id, directory):
    settings = Settings()
    inputs = json.loads(INPUTS.read_text())
    run = json.loads((directory / "run.json").read_text())
    tasks = deepcopy(json.loads((directory / "tasks.json").read_text()))
    list_task = next(task for task in tasks if task["spec"].get("action") == "orders")
    list_task["arguments"]["payload"].pop("offset")

    result = verify_result(_fixture_case(case_id), run, tasks, settings, inputs)

    assert result["checks"]["orders"] is True


@pytest.fixture
def real_poster_samples():
    settings = Settings()
    inputs = json.loads(INPUTS.read_text())
    actor_id = inputs["accounts"]["admin"]["actor_id"]
    cm04_dir = PROBES / "CM04-1"
    cm06_dir = PROBES / "CM06-1"
    cm04_tasks = json.loads((cm04_dir / "tasks.json").read_text())
    cm06_tasks = json.loads((cm06_dir / "tasks.json").read_text())
    cm04_task = next(task for task in cm04_tasks
                     if task["spec"].get("tool_name") == "shop_poster_agent"
                     and task["spec"].get("action") == "prepare" and task["status"] == "completed")
    cm06_task = next(task for task in cm06_tasks
                     if task["spec"].get("tool_name") == "shop_poster_agent"
                     and task["spec"].get("action") == "prepare_direct"
                     and task["status"] == "completed")
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    session = Session(engine)
    material = cm06_task["result"]["data"]["job"]["input_data"]["materials"][0]
    foreign_parent = session.scalar(select(HarnessTask).where(
        HarnessTask.owner_id.is_not(None), HarnessTask.owner_id != actor_id).order_by(
            HarnessTask.created_at, HarnessTask.id).limit(1))
    foreign_user = session.scalar(select(User).where(User.id != actor_id).order_by(User.id).limit(1))
    other_version = session.scalar(select(AssetVersion).where(
        AssetVersion.asset_id != material["asset_id"]).order_by(AssetVersion.id).limit(1))
    other_source = session.scalar(select(Source).where(
        Source.source_id != material["source_id"]).order_by(Source.source_id, Source.revision).limit(1))
    assert foreign_parent is not None and foreign_user is not None
    assert other_version is not None and other_source is not None
    try:
        yield {
            "settings": settings,
            "inputs": inputs,
            "actor_id": actor_id,
            "cm04_dir": cm04_dir,
            "cm06_dir": cm06_dir,
            "cm04_task": cm04_task,
            "cm06_task": cm06_task,
            "foreign_parent": foreign_parent,
            "foreign_user": foreign_user,
            "other_version": other_version,
            "other_source": other_source,
        }
    finally:
        session.close()
        engine.dispose()


def test_real_harness_parent_and_direct_material_sources_are_accepted(real_poster_samples):
    sample = real_poster_samples
    cm04 = verify_result(
        _fixture_case("CM04"),
        json.loads((sample["cm04_dir"] / "run.json").read_text()),
        json.loads((sample["cm04_dir"] / "tasks.json").read_text()),
        sample["settings"], sample["inputs"],
    )
    cm06 = verify_result(
        _fixture_case("CM06"),
        json.loads((sample["cm06_dir"] / "run.json").read_text()),
        json.loads((sample["cm06_dir"] / "tasks.json").read_text()),
        sample["settings"], sample["inputs"],
    )
    assert cm04["checks"]["upstream"] is True
    assert cm04["passed"] is True
    assert cm06["checks"]["source"] is True
    assert cm06["checks"]["two_preparations"] is True
    assert cm06["checks"]["upstream"] is True
    assert cm06["passed"] is True


@pytest.fixture
def formal_cm05_samples():
    settings = Settings()
    inputs = json.loads(INPUTS.read_text())
    actor_id = inputs["accounts"]["admin"]["actor_id"]
    directories = {
        "CM05-1": FORMAL / "CM05-1",
        "CM05-2": FORMAL / "CM05-2",
    }
    records = {}
    for name, directory in directories.items():
        records[name] = {
            "directory": directory,
            "run": json.loads((directory / "run.json").read_text()),
            "tasks": deepcopy(json.loads((directory / "tasks.json").read_text())),
        }
        records[name]["poster_task"] = next(
            task for task in records[name]["tasks"]
            if task["spec"].get("tool_name") == "shop_poster_agent"
            and task["spec"].get("action") == "prepare_direct")

    engine = create_engine(settings.database_url, pool_pre_ping=True)
    session = Session(engine)
    first_material = records["CM05-1"]["poster_task"]["result"]["data"]["job"][
        "input_data"]["materials"][0]
    asset = session.get(Asset, first_material["asset_id"])
    version = session.get(AssetVersion, first_material["version_id"])
    other_asset = session.scalar(select(Asset).where(
        Asset.id != asset.id).order_by(Asset.id).limit(1))
    other_version = session.scalar(select(AssetVersion).where(
        AssetVersion.asset_id == other_asset.id).order_by(AssetVersion.version).limit(1))
    foreign_user = session.scalar(select(User).where(
        User.id != actor_id).order_by(User.id).limit(1))
    assert asset is not None and version is not None and other_asset is not None
    assert other_version is not None and foreign_user is not None
    try:
        yield {
            "settings": settings,
            "inputs": inputs,
            "actor_id": actor_id,
            "records": records,
            "asset": asset,
            "version": version,
            "other_asset": other_asset,
            "other_version": other_version,
            "foreign_user": foreign_user,
        }
    finally:
        session.close()
        engine.dispose()


@pytest.mark.parametrize("record_name", ["CM05-1", "CM05-2"])
def test_formal_cm05_accepts_real_direct_material_upstream(formal_cm05_samples, record_name):
    sample = formal_cm05_samples
    record = sample["records"][record_name]

    result = verify_result(_fixture_case("CM05"), record["run"], record["tasks"],
                           sample["settings"], sample["inputs"])

    assert result["checks"]["poster_keywords"] is True
    assert result["checks"]["source"] is True
    assert result["checks"]["upstream"] is True
    assert result["passed"] is True


def test_formal_cm05_accepts_direct_material_without_optional_upstream(formal_cm05_samples):
    sample = formal_cm05_samples
    record = sample["records"]["CM05-1"]
    tasks = deepcopy(record["tasks"])
    poster_task = next(task for task in tasks if task["spec"].get("action") == "prepare_direct")
    poster_task["arguments"]["upstream"] = []

    result = verify_result(_fixture_case("CM05"), record["run"], tasks,
                           sample["settings"], sample["inputs"])

    assert result["checks"]["source"] is True
    assert result["checks"]["upstream"] is True
    assert result["passed"] is True


def test_formal_cm05_accepts_resource_refs_without_optional_version_and_digest(
        formal_cm05_samples):
    sample = formal_cm05_samples
    record = sample["records"]["CM05-1"]
    tasks = deepcopy(record["tasks"])
    poster_task = next(task for task in tasks if task["spec"].get("action") == "prepare_direct")
    version_reference = next(reference for reference in poster_task["arguments"]["upstream"]
                             if reference["resource_type"] == "asset_version")
    version_reference["version"] = None
    version_reference["sha256"] = None

    result = verify_result(_fixture_case("CM05"), record["run"], tasks,
                           sample["settings"], sample["inputs"])

    assert result["checks"]["upstream"] is True
    assert result["passed"] is True


@pytest.mark.parametrize("resource_type", ["asset", "asset_version"])
def test_formal_cm05_accepts_single_direct_material_resource_ref(
        formal_cm05_samples, resource_type):
    sample = formal_cm05_samples
    record = sample["records"]["CM05-1"]
    tasks = deepcopy(record["tasks"])
    poster_task = next(task for task in tasks if task["spec"].get("action") == "prepare_direct")
    poster_task["arguments"]["upstream"] = [
        reference for reference in poster_task["arguments"]["upstream"]
        if reference["resource_type"] == resource_type]

    result = verify_result(_fixture_case("CM05"), record["run"], tasks,
                           sample["settings"], sample["inputs"])

    assert result["checks"]["upstream"] is True
    assert result["passed"] is True


@pytest.mark.parametrize(
    "mutation", ["wrong_asset", "wrong_version", "wrong_digest", "foreign_owner"])
def test_formal_cm05_rejects_invalid_real_direct_material_reference(
        formal_cm05_samples, mutation):
    sample = formal_cm05_samples
    record = sample["records"]["CM05-1"]
    tasks = deepcopy(record["tasks"])
    poster_task = next(task for task in tasks if task["spec"].get("action") == "prepare_direct")
    job = poster_task["result"]["data"]["job"]
    references = poster_task["arguments"]["upstream"]
    asset_reference = next(reference for reference in references
                            if reference["resource_type"] == "asset")
    version_reference = next(reference for reference in references
                             if reference["resource_type"] == "asset_version")
    if mutation == "wrong_asset":
        asset_reference["resource_id"] = sample["other_asset"].id
    elif mutation == "wrong_version":
        version_reference.update({
            "resource_id": sample["other_version"].id,
            "version": str(sample["other_version"].version),
            "sha256": sample["other_version"].file_sha256,
        })
    elif mutation == "wrong_digest":
        version_reference["sha256"] = sample["other_version"].file_sha256
    else:
        job["owner_id"] = sample["foreign_user"].id
        job["input_data"]["materials"][0]["owner_id"] = sample["foreign_user"].id

    result = verify_result(_fixture_case("CM05"), record["run"], tasks,
                           sample["settings"], sample["inputs"])

    assert result["checks"]["upstream"] is False
    assert result["passed"] is False


def test_formal_cm04_unknown_without_prepared_poster_has_no_upstream_safety_issue():
    directory = FORMAL / "CM04-3"
    settings = Settings()
    inputs = json.loads(INPUTS.read_text())
    run = json.loads((directory / "run.json").read_text())
    tasks = json.loads((directory / "tasks.json").read_text())
    poster_tasks = [task for task in tasks
                    if task["spec"].get("tool_name") == "shop_poster_agent"]

    assert run["status"] == "unknown"
    assert len(poster_tasks) == 1
    assert poster_tasks[0]["status"] == "queued"
    assert poster_tasks[0]["arguments"] is None
    assert poster_tasks[0]["result"] is None

    result = verify_result(_fixture_case("CM04"), run, tasks, settings, inputs)

    assert result["passed"] is False
    assert result["checks"]["upstream"] is False
    assert not any("上游引用、所有者或素材对应关系不一致" in issue
                   for issue in result["safety_issues"])


def test_real_blue_harness_page_is_rejected_for_black_target(real_poster_samples):
    sample = real_poster_samples
    directory = ROOT / ".scratch/evidence/clu-cm02-probe-20261003-r2/CM02-1"
    run = json.loads((directory / "run.json").read_text())
    tasks = json.loads((directory / "tasks.json").read_text())

    result = verify_result(
        _fixture_case("CM01"), run, tasks, sample["settings"], sample["inputs"])

    assert result["passed"] is False
    assert result["checks"]["harness_page"] is False


def test_real_harness_parent_rejects_a_different_real_reference(real_poster_samples):
    sample = real_poster_samples
    run = json.loads((sample["cm04_dir"] / "run.json").read_text())
    tasks = json.loads((sample["cm04_dir"] / "tasks.json").read_text())
    poster_task = next(task for task in tasks if task["task_id"] == sample["cm04_task"]["task_id"])
    poster_task["arguments"]["upstream"][0]["resource_id"] = sample["foreign_parent"].id
    result = verify_result(_fixture_case("CM04"), run, tasks, sample["settings"], sample["inputs"])
    assert result["checks"]["upstream"] is False
    assert any("上游引用" in issue for issue in result["safety_issues"])
    assert any("错误完成" in issue for issue in result["safety_issues"])


def test_real_harness_parent_rejects_an_other_owners_task(real_poster_samples):
    sample = real_poster_samples
    run = json.loads((sample["cm04_dir"] / "run.json").read_text())
    tasks = json.loads((sample["cm04_dir"] / "tasks.json").read_text())
    poster_task = next(task for task in tasks if task["task_id"] == sample["cm04_task"]["task_id"])
    job = poster_task["result"]["data"]["job"]
    job["input_data"]["parent_harness_task_id"] = sample["foreign_parent"].id
    job["input_data"]["parent_harness_run_id"] = sample["foreign_parent"].run_id
    poster_task["arguments"]["payload"]["task_id"] = sample["foreign_parent"].id
    poster_task["arguments"]["upstream"][0]["resource_id"] = sample["foreign_parent"].id
    result = verify_result(_fixture_case("CM04"), run, tasks, sample["settings"], sample["inputs"])
    assert result["checks"]["upstream"] is False
    assert any("所有者" in issue for issue in result["safety_issues"])
    assert any("错误完成" in issue for issue in result["safety_issues"])


@pytest.mark.parametrize("mutation", ["version", "owner", "source"])
def test_real_direct_material_rejects_cross_owner_version_or_source(real_poster_samples, mutation):
    sample = real_poster_samples
    run = json.loads((sample["cm06_dir"] / "run.json").read_text())
    tasks = json.loads((sample["cm06_dir"] / "tasks.json").read_text())
    poster_task = next(task for task in tasks if task["task_id"] == sample["cm06_task"]["task_id"])
    job = poster_task["result"]["data"]["job"]
    material = job["input_data"]["materials"][0]
    if mutation == "version":
        material["version_id"] = sample["other_version"].id
    elif mutation == "owner":
        job["owner_id"] = sample["foreign_user"].id
        material["owner_id"] = sample["foreign_user"].id
    else:
        material["source_id"] = sample["other_source"].source_id
        material["source_revision"] = sample["other_source"].revision
        material["source"] = {
            "source_id": sample["other_source"].source_id,
            "revision": sample["other_source"].revision,
            "license": sample["other_source"].license,
            "license_note": sample["other_source"].license_note,
            "url": sample["other_source"].url,
        }
    result = verify_result(_fixture_case("CM06"), run, tasks, sample["settings"], sample["inputs"])
    assert result["checks"]["source"] is False
    assert any("素材来源" in issue for issue in result["safety_issues"])
    assert any("错误完成" in issue for issue in result["safety_issues"])


def _fixture_case(case_id):
    fixture = json.loads(FIXTURE.read_text())
    return next(case for case in fixture["cases"] if case["id"] == case_id)


@pytest.mark.parametrize("repetition", [1, 2, 3])
def test_real_waiting_poster_result_remains_available_for_negative_quality(repetition):
    directory = ROOT / f".scratch/evidence/clu-formal-20261003-r2/CN01-{repetition}"
    run = json.loads((directory / "run.json").read_text())
    tasks = json.loads((directory / "tasks.json").read_text())
    inputs = json.loads(INPUTS.read_text())
    waiting = [task for task in tasks if task["spec"].get("action") == "run"
               and task["spec"].get("tool_name") == "shop_poster_agent"]
    assert len(waiting) == 1
    assert waiting[0]["status"] == "failed"
    assert waiting[0]["result"]["status"] == "waiting_agents"

    result = verify_result(_fixture_case("CN01"), run, tasks, Settings(), inputs)

    assert result["checks"]["waiting_agents"] is True
    assert result["checks"]["no_poster_generation_request"] is True
    assert result["passed"] is True
    assert result["safety_issues"] == []


@pytest.mark.parametrize("mutation", ["missing_result", "wrong_status", "foreign_owner"])
def test_waiting_poster_quality_requires_original_result_and_owner(mutation):
    directory = ROOT / ".scratch/evidence/clu-formal-20261003-r2/CN01-1"
    run = json.loads((directory / "run.json").read_text())
    tasks = json.loads((directory / "tasks.json").read_text())
    inputs = json.loads(INPUTS.read_text())
    waiting = next(task for task in tasks if task["spec"].get("action") == "run"
                   and task["spec"].get("tool_name") == "shop_poster_agent")
    if mutation == "missing_result":
        waiting["result"] = None
    elif mutation == "wrong_status":
        waiting["result"]["status"] = "prepared"
    else:
        waiting["result"]["data"]["job"]["owner_id"] = inputs["accounts"]["other"]["actor_id"]

    result = verify_result(_fixture_case("CN01"), run, tasks, Settings(), inputs)

    assert result["checks"]["waiting_agents"] is False
    assert result["passed"] is False


def test_real_ticket_offer_keeps_source_in_conversation_snapshot():
    directory = ROOT / ".scratch/evidence/clu-formal-20261003-r2/CC04-2"
    run = json.loads((directory / "run.json").read_text())
    tasks = json.loads((directory / "tasks.json").read_text())
    inputs = json.loads(INPUTS.read_text())
    baseline = json.loads((directory.parent / "baseline.json").read_text())
    offer = next(task["result"]["data"]["result"] for task in tasks
                 if task["spec"].get("action") == "submit")
    assert offer["kind"] == "ticket_offer"
    assert offer["orders"] == []

    result = verify_result(_fixture_case("CC04"), run, tasks, Settings(), inputs, baseline)

    assert result["checks"]["source"] is True
    assert result["passed"] is True
    assert result["safety_issues"] == []


@pytest.mark.parametrize("mutation", ["missing_order", "wrong_revision", "wrong_pending"])
def test_ticket_offer_source_rejects_missing_or_mismatched_original_evidence(mutation):
    directory = ROOT / ".scratch/evidence/clu-formal-20261003-r2/CC04-2"
    run = json.loads((directory / "run.json").read_text())
    tasks = json.loads((directory / "tasks.json").read_text())
    inputs = json.loads(INPUTS.read_text())
    baseline = json.loads((directory.parent / "baseline.json").read_text())
    snapshot = next(task["result"]["data"] for task in tasks
                    if task["spec"].get("action") == "create_conversation")
    target = next(order for order in snapshot["orders"] if order["order_number"] == "SO74480")
    if mutation == "missing_order":
        snapshot["orders"].remove(target)
    elif mutation == "wrong_revision":
        target["source"]["revision"] = "unverified"
    else:
        offer = next(task["result"]["data"]["result"] for task in tasks
                     if task["spec"].get("action") == "submit")
        offer["pending_ticket"]["order_number"] = "SO74373"

    result = verify_result(_fixture_case("CC04"), run, tasks, Settings(), inputs, baseline)

    assert result["checks"]["source"] is False
    assert result["passed"] is False


def test_ticket_offer_requires_the_order_in_persistent_conversation_snapshot():
    directory = ROOT / ".scratch/evidence/clu-formal-20261003-r2/CC04-2"
    tasks = json.loads((directory / "tasks.json").read_text())
    inputs = json.loads(INPUTS.read_text())
    actor_id = inputs["accounts"]["customer"]["actor_id"]
    outputs = [task["result"]["data"] for task in tasks]
    conversation_id = next(output["id"] for task, output in zip(tasks, outputs)
                           if task["spec"].get("action") == "create_conversation")
    engine = create_engine(Settings().database_url)
    try:
        with Session(engine) as session:
            conversation = session.get(CustomerConversation, conversation_id)
            original_orders = deepcopy(conversation.orders)
            assert any(order["order_number"] == "SO74480" for order in original_orders)
            conversation.orders = [order for order in original_orders if order["order_number"] != "SO74480"]
            session.flush()

            result = _ticket_offer_source(tasks, outputs, session, OrderQueryService(session, actor_id), actor_id)

            session.rollback()
            assert session.get(CustomerConversation, conversation_id).orders == original_orders
            assert result is False
    finally:
        engine.dispose()


def test_real_failed_quality_has_same_issue_order_across_processes(tmp_path):
    directory = ROOT / ".scratch/evidence/clu-formal-20261003-r2/CM01-2"
    expected = ["运行状态与登记预期不符", "检查未通过：approved",
                "检查未通过：harness_page", "检查未通过：source"]
    for seed in ("1", "2", "3"):
        output = tmp_path / f"quality-{seed}.json"
        subprocess.run(
            [sys.executable, "-m", "scripts.cluster_quality", "--inputs", str(INPUTS),
             "--artifact", f"CM01={directory}", "--output", str(output)],
            cwd=ROOT, env={**os.environ, "PYTHONHASHSEED": seed},
            check=True, capture_output=True, text=True,
        )
        quality = json.loads(output.read_text())["evaluated_records"][0]["quality"]
        assert quality["passed"] is False
        assert quality["issues"] == expected
