import argparse
import json
import secrets
import shutil
from pathlib import Path

from data_center_protocol import SourceRegistration
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from shop.auth import hash_password
from shop.config import Settings
from shop.customer_models import OrderDataset
from shop.data_center_host import content_digest
from shop.data_center_models import DataCenterGrant, DataCenterSource
from shop.integration_models import IntegrationBudgetWindow
from shop.models import User


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sources", type=Path)
    parser.add_argument("--open-window", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    settings = Settings()
    engine = create_engine(settings.database_url)
    maker = sessionmaker(engine)
    auth_root = Path(".local/data-center/auth")
    auth_root.mkdir(parents=True, exist_ok=True)
    old_inputs = json.loads(Path(".scratch/evidence/clu-inputs-20261002T164037Z-5958ff50/inputs.json").read_text())
    actors = old_inputs["accounts"]
    copied = {}
    for name, value in actors.items():
        original = Path(value["password_file"])
        if original.is_file():
            target = auth_root / (value["username"] + ".password")
            if target.exists() and target.read_bytes() != original.read_bytes():
                raise ValueError("既有本地凭据与原已验收身份不一致")
            if not target.exists():
                shutil.copyfile(original, target)
            target.chmod(0o600)
            copied[name] = {"username": value["username"], "actor_id": value["actor_id"],
                            "password_file": str(target)}
    with maker.begin() as session:
        sync_user = session.scalar(select(User).where(User.username == "data-center-sync"))
        password_path = auth_root / "data-center-sync.password"
        if sync_user is None:
            password = secrets.token_urlsafe(32)
            password_path.write_text(password + "\n")
            password_path.chmod(0o600)
            sync_user = User(username="data-center-sync", password_hash=hash_password(password), role="data_sync")
            session.add(sync_user)
            session.flush()
        elif sync_user.role != "data_sync" or not password_path.is_file():
            raise PermissionError("既有同步账号角色或本地凭据不一致")
        copied["sync"] = {"username": sync_user.username, "actor_id": sync_user.id,
                          "password_file": str(password_path)}
        source_specs = [] if args.sources is None else [
            SourceRegistration.model_validate_json(json.dumps(item)).model_dump(mode="json")
            for item in json.loads(args.sources.read_text())["sources"]]
        manifest = json.loads(Path(".local/assessment/source/manifest.json").read_text())
        dataset = session.get(OrderDataset, manifest["revision"])
        if dataset is None:
            raise ValueError("业务来源尚未导入")
        file_hashes = {item["name"]: item["sha256"] for item in dataset.manifest["files"]}
        order_hash = file_hashes["SalesOrderHeader.csv"]
        source_specs.append({"source_id": "microsoft/adventureworks", "kind": "analytics",
                             "title": "AdventureWorks季度经营数据", "revision": manifest["revision"],
                             "nature": "public_business_sample", "license_id": "MIT",
                             "sha256": order_hash, "properties": {"employee_manifest": manifest}})
        for source_id, business_type in (("microsoft/adventureworks/orders", "orders"),
                                         ("microsoft/adventureworks/products", "products")):
            source_specs.append({"source_id": source_id, "kind": "business", "title": business_type,
                                 "revision": manifest["revision"], "nature": "public_business_sample",
                                 "license_id": "MIT", "sha256": file_hashes[
                                     "Product.csv" if business_type == "products" else "SalesOrderHeader.csv"],
                                 "properties": {"business_type": business_type}})
        unavailable = {"business_type": "inventory", "availability": "DATA_BLOCKED",
                       "reason": "尚未登记真实库存来源"}
        source_specs.append({"source_id": "shop/inventory", "kind": "unavailable", "title": "库存来源",
                             "revision": "DATA_BLOCKED", "nature": "business_source",
                             "license_id": "pending-source", "sha256": content_digest(unavailable),
                             "properties": unavailable})
        for spec in source_specs:
            source = session.get(DataCenterSource, spec["source_id"])
            if source is None:
                source = DataCenterSource(source_id=spec["source_id"])
                session.add(source)
            for field in ("kind", "title", "revision", "nature", "license_id", "sha256", "properties"):
                setattr(source, field, spec[field])
            source.relative_path = spec.get("relative_path")
            source.resource = spec.get("resource")
            source.active = True
            session.flush()
            if source.kind == "analytics":
                grant = session.get(DataCenterGrant, (sync_user.id, source.source_id))
                if grant is None:
                    session.add(DataCenterGrant(actor_id=sync_user.id, source_id=source.source_id,
                                               permissions=["sync"], source_wide=True))
            elif source.kind == "document":
                for actor in copied.values():
                    if actor["actor_id"] == sync_user.id:
                        continue
                    grant = session.get(DataCenterGrant, (actor["actor_id"], source.source_id))
                    if grant is None:
                        session.add(DataCenterGrant(actor_id=actor["actor_id"], source_id=source.source_id,
                                                   permissions=["read"], source_wide=True))
        if args.open_window:
            active = session.scalar(select(IntegrationBudgetWindow).where(IntegrationBudgetWindow.active))
            if active is not None and active.id != "dc-live-20261004":
                raise ValueError("存在其他活动费用窗口")
            if active is None:
                session.add(IntegrationBudgetWindow(id="dc-live-20261004", limit_cny=50,
                                                    include_open_reservations=True, active=True))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"actors": copied, "source_ids": [spec["source_id"] for spec in source_specs],
                                      "window": "dc-live-20261004" if args.open_window else None},
                                     ensure_ascii=False, indent=2) + "\n")
    args.output.chmod(0o600)
    engine.dispose()
    print(json.dumps({"actors": len(copied), "sources": len(source_specs), "window_opened": args.open_window}))


if __name__ == "__main__":
    main()
