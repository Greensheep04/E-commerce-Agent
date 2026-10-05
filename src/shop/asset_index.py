import base64
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from uuid import uuid4

import numpy as np
from pydantic import ValidationError
from sqlalchemy import case, func, select, text
from sqlalchemy.orm import sessionmaker

from shop.budget import BudgetService
from shop.campaign_types import ALLOWED_ATTRIBUTES, ImageDescription, RequirementPlan
from shop.config import Settings
from shop.materials import MaterialRepository
from shop.model_profiles import profiles
from shop.models import Asset, AssetVersion
from shop.paid_models import PaidModels
from shop.runtime_models import AssetIndex, ProviderRequest

MAIN_SOURCE = "ashraq/fashion-product-images-small"
AUX_SOURCE = "Marqo/deepfashion-multimodal"
DESCRIPTION_FIELDS = {"gender", "masterCategory", "subCategory", "articleType", "baseColour",
                      "season", "year", "usage", "productDisplayName",
                      "category1", "category2", "category3", "item_ID"}
PROMPT_VERSION = "description-v2"


def digest(value: dict) -> str:
    return sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def description_messages(fields: dict, image_url: str) -> list[dict]:
    return [
        {"role": "system", "content": (
            "为图片生成中文检索描述，返回符合以下结构的 JSON object："
            + json.dumps(ImageDescription.model_json_schema(), ensure_ascii=False)
            + '。顶层必须为object，填写description和visible_features，结构为'
            '{"description":"图片的完整描述","visible_features":["可见特征"],'
            '"inferred_uses":["建议用途"],"uncertainties":["无法确认的细节"]}。'
            "description及visible_features只描述可见颜色、商品类别、图案、形状与构图。"
            "用途和风格推断放inferred_uses，并标记为建议。看不清的细节放uncertainties。"
            "禁止推断材质、性能、价格、库存、品牌真伪。来源类别作为背景，原图可见内容优先。"
        )},
        {"role": "user", "content": [
            {"type": "text", "text": "来源字段：" + json.dumps(fields, ensure_ascii=False)
             + "。请依据下方原图填写四个字段，输出一个完整JSON object。"},
            {"type": "image_url", "image_url": {"url": image_url}},
        ]},
    ]


def repair_messages(fields: dict, image_url: str) -> list[dict]:
    messages = description_messages(fields, image_url)
    messages[0]["content"] = (
        '依据原图生成中文检索描述，只输出一个JSON object，包含以下四个字段：'
        'description为完整的中文描述字符串；visible_features为可见特征字符串数组；'
        'inferred_uses为建议用途字符串数组；uncertainties为无法确认的细节字符串数组。'
        'description和visible_features必须非空。仅描述可见颜色、商品类别、图案、形状与构图。'
        '用途与风格标记为建议。禁止推断材质、性能、价格、库存、品牌真伪。'
    )
    return messages


class IndexService:
    def __init__(
        self, settings: Settings, maker: sessionmaker,
        repeat_unknown: set[str] | None = None,
        repair_invalid: bool = False,
    ):
        self.settings = settings
        self.maker = maker
        self.client = PaidModels(settings, BudgetService(maker))
        self.profile = profiles(settings.harness_token_limits_file)["description"]
        self.repeat_unknown = repeat_unknown or set()
        self.repair_invalid = repair_invalid

    def configuration(self) -> dict:
        return {"description": self.profile.model_dump(), "prompt_version": PROMPT_VERSION,
                "embedding_model": "qwen3.7-text-embedding", "dimensions": 1024}

    def request_step(self, index_id: str, step_key: str) -> str:
        with self.maker() as session:
            prior = session.scalar(select(ProviderRequest).where(
                ProviderRequest.run_id == index_id, ProviderRequest.step_key == step_key))
            if prior and prior.status == "unknown" and prior.id in self.repeat_unknown:
                return step_key + "-repeat-" + prior.id
        return step_key

    def description_request(
        self, index_id: str, saved_step: str | None, fields: dict, image_url: str,
    ) -> tuple[str, list[dict]]:
        step_key = saved_step or "description"
        messages = (repair_messages(fields, image_url)
                    if step_key.startswith("description-json-repair1")
                    else description_messages(fields, image_url))
        with self.maker() as session:
            prior = session.scalar(select(ProviderRequest).where(
                ProviderRequest.run_id == index_id, ProviderRequest.step_key == step_key,
            ))
            if (prior and prior.status == "completed" and self.repair_invalid
                    and prior.response_body["choices"][0]["finish_reason"] == "stop"):
                try:
                    ImageDescription.model_validate_json(
                        prior.response_body["choices"][0]["message"]["content"])
                except ValidationError:
                    step_key = "description-json-repair1"
                    messages = repair_messages(fields, image_url)
        return self.request_step(index_id, step_key), messages

    def index_asset(self, asset_id: str) -> dict:
        with self.maker() as session:
            repository = MaterialRepository(session, self.settings.data_dir)
            asset = repository.get(asset_id)
            if asset is None or asset.kind != "image":
                raise ValueError("索引素材必须为真实图片")
            version = asset.versions[-1]
            raw = repository.read_file(version)
            fields_hash = digest(version.original_fields)
            allowed = {key: value for key, value in version.original_fields.items()
                       if key in DESCRIPTION_FIELDS}
            configuration = self.configuration()
            identity = {"version_id": version.id, "file_sha256": version.file_sha256,
                        "fields_sha256": fields_hash, "configuration": configuration}
            fingerprint = digest(identity)
            version_id, file_hash, mime = version.id, version.file_sha256, version.mime_type
        with self.maker.kw["bind"].connect() as connection:
            connection.execute(text("SELECT pg_advisory_lock(hashtext(:key))"), {"key": fingerprint})
            connection.commit()
            try:
                with self.maker.begin() as session:
                    record = session.scalar(select(AssetIndex).where(AssetIndex.fingerprint == fingerprint))
                    if record is None:
                        record = AssetIndex(id=str(uuid4()), asset_version_id=version_id,
                                            fingerprint=fingerprint, file_sha256=file_hash,
                                            fields_sha256=fields_hash, configuration=configuration,
                                            status="describing")
                        session.add(record)
                        session.flush()
                    if record.status == "ready":
                        return {"asset_id": asset_id, "index_id": record.id, "reused": True}
                    index_id = record.id
                    saved_description_step = record.configuration.get("description_step_key")
                    saved_embedding_step = record.configuration.get("embedding_step_key", "embedding")
                image_url = "data:" + mime + ";base64," + base64.b64encode(raw).decode()
                step_key, messages = self.description_request(
                    index_id, saved_description_step, allowed, image_url,
                )
                with self.maker.begin() as session:
                    record = session.get(AssetIndex, index_id)
                    record.configuration = {**record.configuration, "description_step_key": step_key}
                response = self.client.text(
                    self.profile, messages,
                    run_id=index_id, step_key=step_key, timeout_seconds=120,
                )
                description = ImageDescription.model_validate_json(response["choices"][0]["message"]["content"])
                search_text = "\n".join([
                    description.description, *description.visible_features,
                    "建议用途：" + "、".join(description.inferred_uses),
                    json.dumps(allowed, ensure_ascii=False, sort_keys=True),
                ])
                with self.maker.begin() as session:
                    record = session.get(AssetIndex, index_id)
                    record.description = description.model_dump()
                    record.search_text = search_text
                    record.status = "embedding"
                    record.configuration = {**configuration, "description_step_key": step_key}
                embedding_step = self.request_step(index_id, saved_embedding_step)
                with self.maker.begin() as session:
                    record = session.get(AssetIndex, index_id)
                    record.configuration = {**record.configuration, "embedding_step_key": embedding_step}
                response = self.client.embed([search_text], run_id=index_id, step_key=embedding_step,
                                             timeout_seconds=60)
                vector = self.embedding_vector(response)
                with self.maker.begin() as session:
                    record = session.get(AssetIndex, index_id)
                    record.embedding = vector.tolist()
                    record.status = "ready"
                    record.error = None
                    record.configuration = {**record.configuration, "embedding_step_key": embedding_step}
                return {"asset_id": asset_id, "index_id": index_id, "reused": False}
            except Exception as error:
                with self.maker.begin() as session:
                    record = session.scalar(select(AssetIndex).where(AssetIndex.fingerprint == fingerprint))
                    if record is not None:
                        record.status = "failed"
                        record.error = f"{type(error).__name__}: {error}"
                raise
            finally:
                connection.execute(text("SELECT pg_advisory_unlock(hashtext(:key))"), {"key": fingerprint})
                connection.commit()

    @staticmethod
    def embedding_vector(response: dict) -> np.ndarray:
        values = sorted(response["data"], key=lambda item: item["index"])
        vectors = np.asarray([item["embedding"] for item in values], dtype=float)
        if vectors.ndim != 2 or vectors.shape[1] != 1024 or not np.isfinite(vectors).all():
            raise ValueError("embedding 维度或数值无效")
        vector = vectors.mean(axis=0)
        if np.linalg.norm(vector) == 0:
            raise ValueError("embedding 向量为零")
        return vector / np.linalg.norm(vector)

    def prepare(self, source_id: str, count: int, workers: int = 4) -> list[dict]:
        if source_id not in {MAIN_SOURCE, AUX_SOURCE} or count not in {50, 300} or not 1 <= workers <= 4:
            raise ValueError("索引范围或并发配置无效")
        with self.maker() as session:
            ids = list(session.scalars(select(Asset.id).where(
                Asset.source_id == source_id, Asset.kind == "image"
            ).order_by(Asset.source_order).limit(count)))
        if len(ids) != count:
            raise ValueError("固定来源的图片数量不足")
        results = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [(asset_id, pool.submit(self.index_asset, asset_id)) for asset_id in ids]
            for asset_id, future in futures:
                try:
                    results.append(future.result())
                except Exception as error:
                    logging.getLogger(__name__).exception("图片索引执行失败，asset_id=%s", asset_id)
                    results.append({"asset_id": asset_id, "reused": False,
                                    "error": f"{type(error).__name__}: {error}"})
        return results

    def attribute_values(self, source_id: str = MAIN_SOURCE) -> dict[str, list[str]]:
        latest = select(AssetVersion.asset_id, func.max(AssetVersion.version).label("version")).group_by(
            AssetVersion.asset_id).subquery()
        query = select(AssetVersion.original_fields).join(
            Asset, AssetVersion.asset_id == Asset.id).join(
            latest, (latest.c.asset_id == Asset.id) & (latest.c.version == AssetVersion.version)
        ).where(Asset.source_id == source_id)
        values = {key: set() for key in sorted(ALLOWED_ATTRIBUTES)}
        with self.maker() as session:
            records = session.scalars(query).all()
            if not records:
                raise ValueError("来源没有可用于理解需求的属性记录")
            for fields in records:
                for key, items in values.items():
                    if fields.get(key) is not None:
                        items.add(str(fields[key]))
        return {key: sorted(items) for key, items in values.items()}

    def search(self, plan: RequirementPlan, vector: list, source_id: str = MAIN_SOURCE) -> list[dict]:
        latest = select(AssetVersion.asset_id, func.max(AssetVersion.version).label("version")).group_by(
            AssetVersion.asset_id
        ).subquery()
        query = select(AssetIndex, Asset, AssetVersion).join(
            AssetVersion, AssetIndex.asset_version_id == AssetVersion.id
        ).join(Asset, AssetVersion.asset_id == Asset.id).join(
            latest, (latest.c.asset_id == Asset.id) & (latest.c.version == AssetVersion.version)
        ).where(Asset.source_id == source_id, AssetIndex.status == "ready",
                AssetIndex.configuration.contains(self.configuration()))
        for key, value in plan.attributes.items():
            query = query.where(AssetVersion.original_fields[key].astext == value)
        for key, values in plan.forbidden_attributes.items():
            query = query.where(~AssetVersion.original_fields[key].astext.in_(values))
        bonus = sum((case((AssetIndex.search_text.contains(word, autoescape=True), 1), else_=0)
                     for word in plan.keywords), start=0)
        query = query.order_by(AssetIndex.embedding.cosine_distance(vector) - bonus * 0.025,
                               Asset.source_order, AssetIndex.id).limit(12)
        with self.maker() as session:
            records = session.execute(query).all()
            result = []
            seen = set()
            for index, asset, version in records:
                if asset.id in seen:
                    continue
                if index.file_sha256 != version.file_sha256 or index.fields_sha256 != digest(version.original_fields):
                    raise ValueError("图片索引与当前来源摘要不一致")
                seen.add(asset.id)
                result.append({"asset_id": asset.id, "title": version.title,
                               "version_id": version.id, "version": version.version,
                               "file_sha256": version.file_sha256,
                               "source_id": asset.source_id, "source_revision": asset.source_revision,
                               "record_id": asset.record_id, "original_fields": version.original_fields,
                               "description": index.description, "index_id": index.id})
            return result

    def coverage(self, source_id: str) -> int:
        latest = select(AssetVersion.asset_id, func.max(AssetVersion.version).label("version")).group_by(
            AssetVersion.asset_id).subquery()
        query = select(func.count(func.distinct(Asset.id))).select_from(AssetIndex).join(
            AssetVersion, AssetIndex.asset_version_id == AssetVersion.id).join(
            Asset, AssetVersion.asset_id == Asset.id).join(
            latest, (latest.c.asset_id == Asset.id) & (latest.c.version == AssetVersion.version)
        ).where(Asset.source_id == source_id, AssetIndex.status == "ready",
                AssetIndex.configuration.contains(self.configuration()),
                AssetIndex.file_sha256 == AssetVersion.file_sha256)
        with self.maker() as session:
            return session.scalar(query)
