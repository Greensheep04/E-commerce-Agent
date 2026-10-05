import asyncio
import json
from argparse import Namespace
from pathlib import Path

import pytest
from agent_cluster.engine import completion_check
from agent_cluster.types import GoalDefinition

from scripts.verify_cluster_live import definition, execute, execution_manifest

INPUTS = Path(".scratch/evidence/clu-inputs-20261002T164037Z-5958ff50/inputs.json")
CONFIGURATION = Path(".scratch/cluster-live-config.json")


def test_marketing_completion_requires_requested_material_colour():
    inputs = json.loads(INPUTS.read_text())
    tasks = json.loads(Path(
        ".scratch/evidence/clu-cm02-probe-20261003-r2/CM02-1/tasks.json").read_text())
    cases = {case["id"]: case for case in inputs["fixture"]["cases"]}
    actor_id = inputs["accounts"]["admin"]["actor_id"]
    blue_goal = GoalDefinition(actor_id=actor_id, **definition(cases["CM02"]))
    black_goal = GoalDefinition(actor_id=actor_id, **definition(cases["CM01"]))
    assert completion_check(blue_goal, tasks)[0] is True
    assert completion_check(black_goal, tasks)[0] is False


def test_recent_orders_completion_requires_requested_page_size():
    inputs = json.loads(INPUTS.read_text())
    tasks = json.loads(Path(
        ".scratch/evidence/clu06-remaining-probe-20261003-r1/CC06-1/tasks.json").read_text())
    case = next(case for case in inputs["fixture"]["cases"] if case["id"] == "CC01")
    goal = GoalDefinition(actor_id=inputs["accounts"]["other"]["actor_id"], **definition(case))
    assert completion_check(goal, tasks)[0] is False


def test_resuming_acceptance_rejects_changed_model_configuration(tmp_path):
    inputs = json.loads(INPUTS.read_text())
    original = json.loads(CONFIGURATION.read_text())
    directory = tmp_path / "acceptance"
    directory.mkdir()
    (directory / "definitions.json").write_text(json.dumps({
        "source_sha256": inputs["source_sha256"],
        "fixture_sha256": inputs["fixture_sha256"],
        "execution_source_sha256": execution_manifest(),
        "cases": inputs["fixture"]["cases"],
    }))
    (directory / "configuration.json").write_text(json.dumps(original))
    changed = json.loads(CONFIGURATION.read_text())
    changed["profile"]["output_limit"] = 8191
    current_path = tmp_path / "current.json"
    current_path.write_text(json.dumps(changed))
    args = Namespace(inputs=INPUTS, cluster_config=current_path, directory=directory,
                     formal=False, case=["configuration-check-only"], repetitions=1)
    with pytest.raises(ValueError, match="配置"):
        asyncio.run(execute(args))
