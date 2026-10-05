import argparse
import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from time import monotonic
from uuid import uuid4

from sqlalchemy import select

from shop.asset_index import MAIN_SOURCE
from shop.campaign import CampaignService
from shop.config import Settings
from shop.models import Asset, AssetVersion


def main() -> None:
    parser = argparse.ArgumentParser(description="用真实原图审核明确标注的错误输入")
    parser.add_argument("--case", choices=["image_mismatch", "unsupported_facts"], action="append")
    args = parser.parse_args()
    service = CampaignService(Settings())
    run_id = f"har-review-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"
    directory = Path(".scratch/evidence") / run_id
    directory.mkdir(parents=True, exist_ok=False)
    with service.maker() as session:
        asset = session.scalar(select(Asset).join(AssetVersion).where(
            Asset.source_id == MAIN_SOURCE, AssetVersion.original_fields["gender"].astext == "Men",
            AssetVersion.original_fields["articleType"].astext == "Tshirts",
            AssetVersion.original_fields["baseColour"].astext == "Black").order_by(Asset.source_order))
        if asset is None:
            raise ValueError("主来源没有所需的审核边界图片")
        version = asset.versions[-1]
        candidate = {"asset_id": asset.id, "version_id": version.id, "version": version.version,
                     "file_sha256": version.file_sha256, "original_fields": version.original_fields,
                     "title": version.title, "source_id": asset.source_id,
                     "source_revision": asset.source_revision, "record_id": asset.record_id}
    records = []
    for label, caption in (
        ("image_mismatch", "红色女士长裙，搭配花朵图案。"),
        ("unsupported_facts", "限时售价99元，库存100件，100%纯棉并具有防水性能。"),
    ):
        if args.case and label not in args.case:
            continue
        task = service.create("请推荐一件黑色男士T恤。")
        state = {"plan": {"count": 1, "must": ["黑色", "男士", "T恤"]}, "candidates": [candidate],
                 "draft": {"headline": "服装推荐", "summary": "根据素材展示商品。",
                           "recommendations": [{"asset_id": candidate["asset_id"], "caption": caption}]}}
        input_path = directory / f"{label}-input.json"
        input_path.write_text(json.dumps({"task": task, "state": state, "label": "人工编写的错误审核输入"},
                                        ensure_ascii=False, indent=2), encoding="utf-8")
        service._active_started = monotonic()
        try:
            reviewed = service._review(task["task_id"], state, "review")
            (directory / f"{label}-result.json").write_text(
                json.dumps(reviewed, ensure_ascii=False, indent=2), encoding="utf-8")
            assert not reviewed["review"]["approved"] and reviewed["review"]["issues"]
            records.append({"label": label, "task_id": task["task_id"], "review": reviewed["review"],
                            "task": service.show(task["task_id"])})
        finally:
            service._persist_elapsed(task["task_id"])
            service._active_started = None
            service.cancel(task["task_id"])
    report = {"run_id": run_id, "milestone": "HAR-03", "status": "passed", "records": records,
              "model": service._profile(records[0]["task_id"], "review").model_dump(),
              "source_sha256": sha256(Path("src/shop/campaign.py").read_bytes()).hexdigest()}
    path = directory / "report.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"report": str(path), "checks": len(records), "status": "passed"}))


if __name__ == "__main__":
    main()
