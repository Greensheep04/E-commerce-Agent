import argparse
import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import numpy as np
from sqlalchemy import func, select

from shop.asset_index import AUX_SOURCE, DESCRIPTION_FIELDS, MAIN_SOURCE, IndexService, digest
from shop.campaign import CampaignService
from shop.campaign_types import ImageDescription
from shop.config import Settings
from shop.materials import MaterialRepository
from shop.models import Asset
from shop.runtime_models import AssetIndex, BudgetAccount, ProviderRequest


def main() -> None:
    parser = argparse.ArgumentParser(description="校验真实索引、来源及保存响应的对应关系")
    parser.add_argument("--ready-only", action="store_true")
    args = parser.parse_args()
    service = CampaignService(Settings())
    index = IndexService(service.settings, service.maker)
    run_id = f"har-index-check-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"
    directory = Path(".scratch/evidence") / run_id
    directory.mkdir(parents=True, exist_ok=False)
    records = []
    missing = []
    with service.maker() as session:
        before = session.scalar(select(func.count()).select_from(ProviderRequest))
        account = session.get(BudgetAccount, "project")
        budget_before = (str(account.spent_cny), str(account.reserved_cny))
        repository = MaterialRepository(session, service.settings.data_dir)
        for source_id, expected_count in ((MAIN_SOURCE, 300), (AUX_SOURCE, 50)):
            assets = session.scalars(select(Asset).where(Asset.source_id == source_id).order_by(Asset.source_order)).all()
            assert len(assets) == expected_count
            if not args.ready_only:
                assert index.coverage(source_id) == expected_count
            for asset in assets:
                version = asset.versions[-1]
                record = session.scalar(select(AssetIndex).where(
                    AssetIndex.asset_version_id == version.id,
                    AssetIndex.configuration.contains(index.configuration()), AssetIndex.status == "ready"))
                if record is None:
                    assert args.ready_only
                    missing.append({"asset_id": asset.id, "source_id": source_id,
                                    "version_id": version.id})
                    continue
                assert sha256(repository.read_file(version)).hexdigest() == record.file_sha256 == version.file_sha256
                assert record.fields_sha256 == digest(version.original_fields)
                requests = session.scalars(select(ProviderRequest).where(
                    ProviderRequest.run_id == record.id, ProviderRequest.status == "completed")).all()
                step_key = record.configuration.get("description_step_key", "description")
                described = next(item for item in requests if item.step_key == step_key)
                embedding_step = record.configuration.get("embedding_step_key", "embedding")
                embedded = next(item for item in requests if item.step_key == embedding_step)
                description = ImageDescription.model_validate_json(described.response_body["choices"][0]["message"]["content"])
                assert description.model_dump() == record.description
                assert embedded.request_body["input"] == [record.search_text]
                vector = index.embedding_vector(embedded.response_body)
                assert np.allclose(vector, np.asarray(record.embedding), atol=1e-6)
                assert described.request_body["model"] == record.configuration["description"]["model"]
                if source_id == AUX_SOURCE:
                    fields = {key: value for key, value in version.original_fields.items()
                              if key in DESCRIPTION_FIELDS}
                    expected_text = "来源字段：" + json.dumps(fields, ensure_ascii=False) + "。请依据下方原图填写四个字段，输出一个完整JSON object。"
                    assert described.request_body["messages"][-1]["content"][0]["text"] == expected_text
                    assert "text" not in fields and "text" not in version.original_fields
                reused = index.index_asset(asset.id)
                assert reused["reused"] and reused["index_id"] == record.id
                records.append({"asset_id": asset.id, "version_id": version.id,
                                "source_id": source_id, "revision": asset.source_revision,
                                "record_id": asset.record_id, "file_sha256": record.file_sha256,
                                "index_id": record.id, "description_request_id": described.id,
                                "embedding_request_id": embedded.id,
                                "description_step_key": step_key})
    with service.maker() as session:
        account = session.get(BudgetAccount, "project")
        assert session.scalar(select(func.count()).select_from(ProviderRequest)) == before
        assert (str(account.spent_cny), str(account.reserved_cny)) == budget_before
    status = "incomplete" if missing else "passed"
    report = {"run_id": run_id, "milestone": "HAR-02", "status": status,
              "count": len(records), "records": records, "configuration": index.configuration(),
              "missing": missing, "ready_only": args.ready_only,
              "request_count_unchanged": before,
              "budget_cny_unchanged": {"spent": budget_before[0], "reserved": budget_before[1]},
              "code_sha256": {str(path): sha256(path.read_bytes()).hexdigest() for path in (
                  Path("src/shop/asset_index.py"), Path("src/shop/paid_models.py"),
                  Path("scripts/verify_harness_indexes.py"))}}
    path = directory / "report.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"report": str(path), "count": len(records),
                      "missing": len(missing), "status": status}))


if __name__ == "__main__":
    main()
