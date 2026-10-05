import base64
import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from shop.budget import BudgetService
from shop.config import Settings
from shop.materials import MaterialRepository
from shop.model_profiles import profiles
from shop.models import Asset
from shop.paid_models import PaidModels
from shop.runtime_models import ProviderRequest


def main() -> None:
    settings = Settings()
    maker = sessionmaker(create_engine(settings.database_url), expire_on_commit=False)
    client = PaidModels(settings, BudgetService(maker))
    config = profiles(settings.harness_token_limits_file)
    run_id = str(uuid4())
    directory = Path(".scratch/evidence") / f"har-models-{datetime.now(UTC):%Y%m%dT%H%M%S}-{run_id[:8]}"
    directory.mkdir(parents=True, exist_ok=False)
    with maker() as session:
        asset = session.scalar(select(Asset).where(
            Asset.source_id == "ashraq/fashion-product-images-small"
        ).order_by(Asset.source_order))
        if asset is None:
            raise ValueError("缺少已验收主图库素材")
        version = asset.versions[-1]
        content = MaterialRepository(session, settings.data_dir).read_file(version)
        source = {"asset_id": asset.id, "version": version.version,
                  "sha256": sha256(content).hexdigest(), "fields": version.original_fields}
        image = "data:" + version.mime_type + ";base64," + base64.b64encode(content).decode()
    text = client.text(config["understand"], [
        {"role": "system", "content": "从中文需求提取数量与条件，返回 JSON object，包含 count、attributes。"},
        {"role": "user", "content": "请推荐两件黑色男士T恤，用于日常穿搭宣传。"},
    ], run_id=run_id, step_key="understand", timeout_seconds=120)
    vision = client.text(config["description"], [
        {"role": "system", "content": "只根据原图可见特征和给定来源生成中文 JSON object：description、visible_features。不可猜测材质价格。"},
        {"role": "user", "content": [
            {"type": "text", "text": json.dumps(source["fields"], ensure_ascii=False)},
            {"type": "image_url", "image_url": {"url": image}},
        ]},
    ], run_id=run_id, step_key="description", timeout_seconds=120)
    for response in (text, vision):
        parsed = json.loads(response["choices"][0]["message"]["content"])
        if not isinstance(parsed, dict):
            raise TypeError("模型没有返回 JSON object")
        if response["usage"]["completion_tokens_details"]["reasoning_tokens"] <= 0:
            raise ValueError("模型请求没有实际思考用量")
    embedding = client.embed(["黑色男士T恤，日常穿搭宣传"],
                             run_id=run_id, step_key="embedding", timeout_seconds=60)
    vector = embedding["data"][0]["embedding"]
    if len(vector) != 1024 or not all(isinstance(value, (float, int)) for value in vector):
        raise ValueError("embedding 返回维度或元素无效")
    with maker() as session:
        requests = session.scalars(select(ProviderRequest).where(
            ProviderRequest.run_id == run_id
        ).order_by(ProviderRequest.created_at)).all()
        records = [{"id": item.id, "role": item.step_key, "model": item.model,
                    "usage": item.usage, "charged_cny": str(item.charged_cny),
                    "response": item.response_body, "price_basis": item.price_basis}
                   for item in requests]
    for item in records:
        (directory / f'{item["role"]}.json').write_text(
            json.dumps(item, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    report = {"run_id": run_id, "source": source, "checks": {
        "text_thinking_json": True, "image_thinking_json": True,
        "embedding_1024": True,
    }, "requests": [{key: item[key] for key in ("id", "role", "model", "usage", "charged_cny")}
                     for item in records], "profiles": {key: value.model_dump() for key, value in config.items()},
              "source_hashes": {str(path): sha256(path.read_bytes()).hexdigest()
                                for path in Path("src/shop").glob("*.py")}}
    (directory / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps({"run_id": run_id, "report": str(directory / "report.json"),
                      "checks": report["checks"], "requests": report["requests"]},
                     ensure_ascii=False))


if __name__ == "__main__":
    main()
