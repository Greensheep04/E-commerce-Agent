import argparse
import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from scripts.verify_integration_live import digest, snapshot
from shop.assessment_models import AssessmentAccess, AssessmentSource
from shop.config import Settings
from shop.customer_models import OrderDataset
from shop.models import Asset, AssetVersion, User
from shop.orders import OrderQueryService

ROOT = Path(__file__).resolve().parents[1]


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="固定Agent集群验收目标和真实来源，不调用模型")
    parser.add_argument("--previous-inputs", type=Path, required=True)
    parser.add_argument("--directory", type=Path)
    args = parser.parse_args()
    settings = Settings()
    fixture_path = ROOT / "agent-cluster/tests/fixtures/goals_v1.json"
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    previous = json.loads(args.previous_inputs.read_text(encoding="utf-8"))
    run_id = "clu-inputs-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    directory = args.directory or ROOT / ".scratch/evidence" / run_id
    directory.mkdir(parents=True, mode=0o700, exist_ok=False)
    engine = create_engine(settings.database_url)
    try:
        with Session(engine) as session:
            accounts = previous["accounts"]
            for account in accounts.values():
                user = session.get(User, account["actor_id"])
                assert user is not None and user.username == account["username"]
                password_path = Path(account["password_file"])
                if not password_path.is_absolute():
                    password_path = ROOT / password_path
                assert password_path.is_file(), "真实身份需要已有密码文件"
            orders = {name: OrderQueryService(session, accounts[name]["actor_id"]).list_orders(limit=20)
                      for name in ("customer", "other")}
            assert any(item["order_number"] == "SO74480" for item in orders["customer"]["orders"])
            assert all(item["order_number"] != "SO74480" for item in orders["other"]["orders"])
            materials = [{"asset_id": asset.id, "title": asset.title, "source_id": asset.source_id,
                          "source_revision": asset.source_revision, "version_id": version.id,
                          "version": version.version, "file_sha256": version.file_sha256,
                          "original_fields": version.original_fields}
                         for asset, version in session.execute(select(Asset, AssetVersion).join(
                             AssetVersion, AssetVersion.asset_id == Asset.id).order_by(Asset.id, AssetVersion.version))]
            for colour in ("Black", "Blue"):
                assert any(item["original_fields"].get("baseColour") == colour
                           and item["original_fields"].get("gender") == "Men"
                           and item["original_fields"].get("articleType") == "Tshirts"
                           for item in materials), "营销目标需要真实可用素材"
            assert not any(item["original_fields"].get("baseColour") ==
                           "cluster-unavailable-colour-20261003" for item in materials)
            access = [{"user_id": row.user_id, "level": row.level, "employee_id": row.employee_id,
                       "managed_employee_ids": row.managed_employee_ids}
                      for row in session.scalars(select(AssessmentAccess))]
            assert any(item["user_id"] == accounts["assessment"]["actor_id"]
                       and item["employee_id"] == 275 for item in access)
            order_sources = [{"source_id": row.source_id, "revision": row.revision,
                              "counts": row.counts, "manifest": row.manifest}
                             for row in session.scalars(select(OrderDataset))]
            assessment_sources = [{"source_id": row.source_id, "revision": row.revision,
                                   "fingerprint": row.fingerprint, "manifest": row.manifest}
                                  for row in session.scalars(select(AssessmentSource))]
            source = {"orders": orders, "materials": materials, "assessment_access": access,
                      "order_sources": order_sources, "assessment_sources": assessment_sources}
        save(directory / "sources.json", source)
        save(directory / "baseline.json", snapshot(settings))
        save(directory / "inputs.json", {
            "run_id": run_id, "fixture": fixture, "fixture_sha256": sha256(fixture_path.read_bytes()).hexdigest(),
            "accounts": accounts, "hub_configs": previous["hub_configs"],
            "previous_inputs_sha256": sha256(args.previous_inputs.read_bytes()).hexdigest(),
            "source_sha256": digest(source), "source_file": str(directory / "sources.json"),
            "prepared_at": datetime.now(UTC).isoformat(), "model_requests": 0,
        })
        print(directory / "inputs.json")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
