import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.verify_cluster_release import verify_formal
from shop.config import Settings

ROOT = Path(__file__).resolve().parents[3]
FORMAL = ROOT / ".scratch/evidence/clu-formal-20261003-r2"
QUALITY = ROOT / ".scratch/evidence/clu-formal-quality-recheck-20261003-r6"
INPUTS = ROOT / ".scratch/evidence/clu-inputs-20261002T164037Z-5958ff50/inputs.json"


def arguments(formal=FORMAL, quality=QUALITY):
    return SimpleNamespace(
        cluster_config=ROOT / ".scratch/cluster-live-config.json",
        formal_directory=formal,
        formal_quality_directory=quality,
        inputs=INPUTS,
    )


def test_real_formal_recheck_preserves_all_sixty_runs_and_three_failures(tmp_path):
    report = verify_formal(arguments(), tmp_path, Settings())

    assert report["executed"] == 60
    assert report["passed"] == 57
    assert report["failed"] == 3
    assert report["original_passed"] == 53
    assert report["original_failed"] == 7
    assert report["goal_count"] == report["run_count"] == 60
    assert report["safety_issues"] == []
    assert list(report["source_changes"]) == ["scripts/cluster_quality.py"]
    records = json.loads((tmp_path / "formal-independent-recheck.json").read_text())
    assert {(item["case_id"], item["repetition"]) for item in records
            if not item["quality"]["passed"]} == {("CM01", 2), ("CM02", 2), ("CN04", 2)}


def test_formal_recheck_rejects_changed_business_source(tmp_path):
    formal = tmp_path / "formal"
    formal.mkdir()
    for name in ("report.json", "definitions.json", "configuration.json"):
        shutil.copyfile(FORMAL / name, formal / name)
    definitions = json.loads((formal / "definitions.json").read_text())
    definitions["execution_source_sha256"]["src/shop/campaign.py"] = "untrusted"
    (formal / "definitions.json").write_text(json.dumps(definitions))

    with pytest.raises(AssertionError, match="正式业务源码与当前版本不一致"):
        verify_formal(arguments(formal=formal), tmp_path, Settings())


def test_formal_recheck_rejects_stale_evaluator_digest(tmp_path):
    quality = tmp_path / "quality"
    quality.mkdir()
    for name in ("report.json", "quality.json"):
        shutil.copyfile(QUALITY / name, quality / name)
    report = json.loads((quality / "report.json").read_text())
    report["evaluator_sha256"] = "untrusted"
    (quality / "report.json").write_text(json.dumps(report))

    with pytest.raises(AssertionError, match="独立评分源码摘要不一致"):
        verify_formal(arguments(quality=quality), tmp_path, Settings())


def test_formal_recheck_rejects_rewritten_original_report(tmp_path):
    quality = tmp_path / "quality"
    quality.mkdir()
    for name in ("report.json", "quality.json"):
        shutil.copyfile(QUALITY / name, quality / name)
    report = json.loads((quality / "report.json").read_text())
    report["original_report_sha256"] = "untrusted"
    (quality / "report.json").write_text(json.dumps(report))

    with pytest.raises(AssertionError, match="正式原报告摘要不一致"):
        verify_formal(arguments(quality=quality), tmp_path, Settings())
