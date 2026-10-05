import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from shop.asset_index import AUX_SOURCE, MAIN_SOURCE, IndexService
from shop.config import Settings
from shop.models import Asset, AssetVersion
from shop.runtime_models import AssetIndex, ProviderRequest


def main() -> None:
    parser = argparse.ArgumentParser(description="生成固定主图库或辅助图库的图片描述与向量")
    parser.add_argument("--source", choices=[MAIN_SOURCE, AUX_SOURCE], default=MAIN_SOURCE)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--asset-id")
    parser.add_argument("--repeat-unknown", action="append", default=[])
    parser.add_argument("--repair-invalid-json", action="store_true")
    parser.add_argument("--repeat-all-original-unknown-once", action="store_true")
    args = parser.parse_args()
    settings = Settings()
    maker = sessionmaker(create_engine(settings.database_url), expire_on_commit=False)
    run_id = f"har-index-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"
    directory = Path(".scratch/evidence") / run_id
    directory.mkdir(parents=True, exist_ok=False)
    report = {"run_id": run_id, "source": args.source}
    (directory / "started.json").write_text(json.dumps(report), encoding="utf-8")
    service = IndexService(settings, maker, set(args.repeat_unknown), args.repair_invalid_json)
    if args.repeat_all_original_unknown_once:
        with maker() as session:
            request_ids = session.scalars(select(ProviderRequest.id).join(
                AssetIndex, ProviderRequest.run_id == AssetIndex.id).join(
                AssetVersion, AssetIndex.asset_version_id == AssetVersion.id).join(
                Asset, AssetVersion.asset_id == Asset.id).where(
                Asset.source_id == args.source, ProviderRequest.status == "unknown",
                ProviderRequest.step_key.in_(["description", "embedding", "description-json-repair1"]),
                AssetIndex.configuration.contains(service.configuration()))).all()
        service.repeat_unknown.update(request_ids)
    report["configuration"] = service.configuration()
    report["authorized_repeat_requests"] = sorted(service.repeat_unknown)
    report["repair_invalid_json"] = args.repair_invalid_json
    report["records"] = [service.index_asset(args.asset_id)] if args.asset_id else service.prepare(
        args.source, 300 if args.source == MAIN_SOURCE else 50, args.workers,
    )
    failed = sum("error" in record for record in report["records"])
    report["status"] = "failed" if failed else "passed"
    (directory / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"run_id": run_id, "count": len(report["records"]),
                      "reused": sum(row["reused"] for row in report["records"]),
                      "failed": failed,
                      "report": str(directory / "report.json")}, ensure_ascii=False))
    if failed:
        raise RuntimeError(f"索引批次有{failed}条未完成，详情已写入报告")


if __name__ == "__main__":
    main()
