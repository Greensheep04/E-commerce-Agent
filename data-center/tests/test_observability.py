import json
from importlib.metadata import version
from uuid import uuid4

import pytest
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from packaging.requirements import Requirement

from data_center.observability import RunObserver


def test_sqlalchemy_version_is_supported_by_real_instrumentation():
    requirements = [Requirement(value) for value in SQLAlchemyInstrumentor().instrumentation_dependencies()]
    assert all(version(item.name) in item.specifier for item in requirements)


def test_real_lineage_and_trace_files_record_run_and_sources(tmp_path):
    observer = RunObserver(tmp_path, provider=TracerProvider())
    run_id, task_id = str(uuid4()), str(uuid4())
    with observer.running(task_id, run_id, "sync", ["microsoft/adventureworks"]):
        observer.complete(run_id, "sync", ["microsoft/adventureworks"], ["a" * 64])
    observer.close()
    events = [json.loads(line) for line in (tmp_path / "lineage.jsonl").read_text().splitlines()]
    assert [event["eventType"] for event in events] == ["START", "COMPLETE"]
    assert all(event["run"]["runId"] == run_id for event in events)
    assert events[0]["inputs"][0]["name"] == "microsoft/adventureworks"
    assert events[1]["outputs"][0]["name"] == "a" * 64
    spans = [json.loads(line) for line in (tmp_path / "traces.jsonl").read_text().splitlines()]
    assert spans[0]["attributes"]["task.id"] == task_id
    assert spans[0]["attributes"]["run.id"] == run_id


def test_real_failure_defers_lineage_terminal_until_runtime_persists_state(tmp_path):
    observer = RunObserver(tmp_path, provider=TracerProvider())
    run_id = str(uuid4())
    with pytest.raises(ValueError, match="缺少来源"), observer.running(str(uuid4()), run_id, "query", ["source"]):
        raise ValueError("缺少来源")
    observer.close()
    events = [json.loads(line) for line in (tmp_path / "lineage.jsonl").read_text().splitlines()]
    assert [event["eventType"] for event in events] == ["START"]
