import copy
import json
import runpy
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from shop.config import Settings

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/compare_data_center_retrieval.py"


def comparison_functions():
    assert SCRIPT.is_file(), "缺少最终回答成对比较程序"
    return runpy.run_path(str(SCRIPT))


def original_inputs():
    fixture = json.loads((ROOT / "tests/fixtures/data_center_cases.json").read_text())
    registry = json.loads((ROOT / ".local/data-center/sources/r3/sources.json").read_text())
    frozen = json.loads((ROOT / ".scratch/evidence/dc-formal-20261004-r4/inputs.json").read_text())
    return fixture, registry, frozen


def test_pairs_preserve_fixed_first_attempt_and_independent_hybrid_identity():
    functions = comparison_functions()
    fixture, registry, frozen = original_inputs()
    frozen["attempts"].reverse()
    pairs = functions["build_pairs"](fixture, registry, frozen)
    assert [pair["case_id"] for pair in pairs] == [
        "REL-01", "REL-02", "REL-03", "REL-04", "REL-05", "REL-06"]
    assert pairs[1]["graph"]["request_id"] == "a67accad-3e62-4fc8-8786-37510775435c"
    for pair in pairs:
        assert pair["graph"]["attempt"] == 1
        assert pair["hybrid"]["actor"] == pair["graph"]["actor"]
        assert pair["hybrid"]["request_id"] != pair["graph"]["request_id"]
        assert pair["hybrid"]["payload"] == {
            **pair["graph"]["payload"], "retrieval_mode": "hybrid"}


def test_missing_first_attempt_does_not_select_a_later_answer():
    functions = comparison_functions()
    fixture, registry, frozen = original_inputs()
    frozen["attempts"] = [item for item in frozen["attempts"]
                          if (item["case_id"], item["attempt"]) != ("REL-02", 1)]
    with pytest.raises(ValueError, match="第一次"):
        functions["build_pairs"](fixture, registry, frozen)


def test_first_record_identity_rejects_an_actual_second_attempt():
    functions = comparison_functions()
    pair = functions["build_pairs"](*original_inputs())[1]
    first = json.loads((ROOT / ".scratch/evidence/dc-formal-20261004-r4/attempts/REL-02-1.json").read_text())
    second = json.loads((ROOT / ".scratch/evidence/dc-formal-20261004-r4/attempts/REL-02-2.json").read_text())
    functions["validate_graph_record"](pair, first)
    with pytest.raises(ValueError, match="第一次"):
        functions["validate_graph_record"](pair, second)


def test_frozen_answer_configuration_rejects_changed_thinking():
    functions = comparison_functions()
    fixture, registry, frozen = original_inputs()
    changed = copy.deepcopy(frozen)
    changed["config"]["thinking"] = "low"
    with pytest.raises(ValueError, match="回答配置"):
        functions["build_pairs"](fixture, registry, changed)


def test_pairs_accept_registered_16384_answer_limit_and_reject_other_limits():
    functions = comparison_functions()
    fixture, registry, frozen = original_inputs()
    frozen["config"]["output_tokens"] = 16384
    assert len(functions["build_pairs"](fixture, registry, frozen)) == 6
    frozen["config"]["output_tokens"] = 16385
    with pytest.raises(ValueError, match="回答配置"):
        functions["build_pairs"](fixture, registry, frozen)


def test_changed_allowed_sources_are_rejected_against_the_real_fixture():
    functions = comparison_functions()
    fixture, registry, frozen = original_inputs()
    first = next(item for item in frozen["attempts"]
                 if (item["case_id"], item["attempt"]) == ("REL-02", 1))
    first["payload"]["source_ids"].pop()
    with pytest.raises(ValueError, match="固定输入"):
        functions["build_pairs"](fixture, registry, frozen)


def test_real_pg_execution_binds_the_saved_task_and_actual_model_profile():
    functions = comparison_functions()
    record = json.loads((ROOT / ".scratch/evidence/dc-formal-20261004-r4/attempts/REL-02-1.json").read_text())
    shop_database = create_engine(Settings().database_url)
    data_database = create_engine(
        "postgresql+psycopg://data_center_writer@127.0.0.1:55432/shop_data_center")
    try:
        execution = functions["read_execution"](shop_database, data_database, record["task_id"])
        assert execution["request_id"] == "a67accad-3e62-4fc8-8786-37510775435c"
        assert execution["actor_id"] == "291eeb2a-094f-4dba-ae90-efcd9222cc65"
        assert execution["run_id"] == record["result"]["run_id"]
        assert execution["payload"] == {**record["input"]["payload"], "snapshot_id": None}
        assert {name: execution["answer_profile"][name] for name in (
            "model", "thinking", "output_tokens")} == {
                "model": "deepseek-flash", "thinking": "high", "output_tokens": 8192}
        assert execution["token_totals"]["total_tokens"] > 0
        assert execution["plan"]
        conditions = functions["pair_conditions"](record, record, execution, execution)
        assert conditions["same_actor"] and conditions["same_actual_snapshots"]
        assert conditions["same_answer_profile_and_prompt"]
        assert not conditions["independent_request"]
        assert not conditions["independent_task_run"]
        requests = copy.deepcopy(execution["requests"])
        answer = next(item for item in requests if item["step_key"] == "answer")
        answer["request_body"]["reasoning_effort"] = "low"
        with pytest.raises(ValueError, match="实际模型回答配置"):
            functions["answer_profile"](requests)
    finally:
        shop_database.dispose()
        data_database.dispose()
