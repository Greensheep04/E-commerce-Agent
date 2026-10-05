from hashlib import sha256
from io import BytesIO
from json import dumps
from os import replace
from pathlib import Path
from uuid import uuid4

from PIL import Image, UnidentifiedImageError
from pydantic import BaseModel, Field
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from shop.models import Asset, AssetVersion, Source, User


class MaterialInput(BaseModel):
    source_id: str = Field(min_length=1, max_length=200)
    source_revision: str = Field(min_length=1, max_length=200)
    record_id: str = Field(min_length=1, max_length=200)
    title: str = Field(min_length=1, max_length=500)
    kind: str = Field(pattern="^(text|image|video)$")
    tags: list[str] = Field(default_factory=list)
    owner_id: str
    source_order: int = Field(ge=0)
    original_fields: dict
    filename: str = Field(min_length=1, max_length=500)
    mime_type: str = Field(min_length=1, max_length=200)
    file_bytes: bytes = Field(min_length=1)


class MaterialRepository:
    def __init__(self, session: Session, data_dir: Path):
        self.session = session
        self.data_dir = data_dir.resolve()

    def _locked_asset(
        self, source_id: str, source_revision: str, record_id: str
    ) -> Asset | None:
        identity = "material:" + dumps([source_id, source_revision, record_id])
        self.session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))"),
            {"identity": identity},
        )
        # 取得写入权限后重新读取版本，包含当前会话已缓存的素材。
        return self.session.scalar(
            select(Asset).where(
                Asset.source_id == source_id,
                Asset.source_revision == source_revision,
                Asset.record_id == record_id,
            ).execution_options(populate_existing=True)
        )

    def save(self, item: MaterialInput) -> Asset:
        asset = self.save_in_session(item)
        self.session.commit()
        return asset

    def save_in_session(self, item: MaterialInput) -> Asset:
        if self.session.get(Source, (item.source_id, item.source_revision)) is None:
            raise ValueError("来源及其版本不存在")
        if self.session.get(User, item.owner_id) is None:
            raise ValueError("负责人不存在")
        if item.kind == "image":
            try:
                with Image.open(BytesIO(item.file_bytes)) as image:
                    image.verify()
                    image_format = image.format
            except (UnidentifiedImageError, OSError, ValueError) as error:
                raise ValueError("图片文件损坏") from error
            if image_format is None:
                raise ValueError("图片格式无法识别")
            item.mime_type = Image.MIME[image_format]
        elif item.kind == "text":
            item.file_bytes.decode("utf-8")
            item.mime_type = "text/plain; charset=utf-8"
        else:
            item.mime_type = "application/octet-stream"
        digest = sha256(item.file_bytes).hexdigest()
        path = self.data_dir / "assets" / digest[:2] / digest
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if sha256(path.read_bytes()).hexdigest() != digest:
                raise ValueError("本地文件校验失败")
        else:
            temporary_path = path.parent / f".{uuid4().hex}.part"
            try:
                temporary_path.write_bytes(item.file_bytes)
                replace(temporary_path, path)
            finally:
                temporary_path.unlink(missing_ok=True)
        asset = self._locked_asset(item.source_id, item.source_revision, item.record_id)
        if asset is None:
            asset = Asset(
                source_id=item.source_id,
                source_revision=item.source_revision,
                record_id=item.record_id,
                source_order=item.source_order,
                title=item.title,
                kind=item.kind,
                tags=item.tags,
                owner_id=item.owner_id,
            )
            self.session.add(asset)
            self.session.flush()
        elif asset.kind != item.kind or asset.source_order != item.source_order:
            raise ValueError("已存在素材的类型或来源顺序发生冲突")
        previous = asset.versions[-1] if asset.versions else None
        if previous is not None and all(
            (
                previous.file_sha256 == digest,
                previous.original_fields == item.original_fields,
                previous.title == item.title,
                previous.tags == item.tags,
                previous.owner_id == item.owner_id,
                previous.filename == item.filename,
                previous.mime_type == item.mime_type,
            )
        ):
            return asset
        version = AssetVersion(
            asset_id=asset.id,
            version=1 if previous is None else previous.version + 1,
            file_path=str(path.relative_to(self.data_dir)),
            file_sha256=digest,
            file_size=len(item.file_bytes),
            filename=item.filename,
            mime_type=item.mime_type,
            original_fields=item.original_fields,
            title=item.title,
            tags=item.tags,
            owner_id=item.owner_id,
        )
        asset.title = item.title
        asset.tags = item.tags
        asset.owner_id = item.owner_id
        asset.versions.append(version)
        self.session.flush()
        return asset

    def get(self, asset_id: str) -> Asset | None:
        return self.session.get(Asset, asset_id)

    def search(
        self, *, title: str | None = None, kind: str | None = None,
        tag: str | None = None, owner_id: str | None = None,
        source_id: str | None = None, source_revision: str | None = None,
        attributes: dict[str, str] | None = None, limit: int = 50, offset: int = 0,
    ) -> list[Asset]:
        if not 1 <= limit <= 200 or offset < 0:
            raise ValueError("分页参数无效")
        query = select(Asset)
        if attributes:
            latest = (
                select(AssetVersion.asset_id, func.max(AssetVersion.version).label("version"))
                .group_by(AssetVersion.asset_id)
                .subquery()
            )
            query = query.join(latest, latest.c.asset_id == Asset.id).join(
                AssetVersion,
                (AssetVersion.asset_id == latest.c.asset_id)
                & (AssetVersion.version == latest.c.version),
            )
            for key, value in attributes.items():
                if key not in {"gender", "masterCategory", "subCategory", "articleType", "baseColour", "season", "usage"}:
                    raise ValueError(f"不支持的来源属性：{key}")
                query = query.where(AssetVersion.original_fields[key].astext == value)
        if title:
            query = query.where(Asset.title.ilike(f"%{title}%"))
        if kind:
            query = query.where(Asset.kind == kind)
        if tag:
            query = query.where(Asset.tags.contains([tag]))
        if owner_id:
            query = query.where(Asset.owner_id == owner_id)
        if source_id:
            query = query.where(Asset.source_id == source_id)
        if source_revision:
            query = query.where(Asset.source_revision == source_revision)
        return list(self.session.scalars(query.order_by(Asset.source_order, Asset.id).offset(offset).limit(limit)).all())

    def update_metadata(
        self, asset_id: str, *, title: str, tags: list[str], owner_id: str
    ) -> Asset:
        asset = self.get(asset_id)
        if asset is None:
            raise ValueError("素材不存在")
        asset = self._locked_asset(asset.source_id, asset.source_revision, asset.record_id)
        if asset is None:
            raise ValueError("素材不存在")
        previous = asset.versions[-1]
        return self.save(MaterialInput(
            source_id=asset.source_id, source_revision=asset.source_revision,
            record_id=asset.record_id, source_order=asset.source_order,
            title=title, kind=asset.kind, tags=tags, owner_id=owner_id,
            original_fields=previous.original_fields, filename=previous.filename,
            mime_type=previous.mime_type, file_bytes=self.read_file(previous),
        ))

    def read_file(self, version: AssetVersion) -> bytes:
        path = (self.data_dir / version.file_path).resolve()
        if not path.is_relative_to(self.data_dir):
            raise ValueError("文件路径超出素材目录")
        content = path.read_bytes()
        if sha256(content).hexdigest() != version.file_sha256:
            raise ValueError("素材文件校验失败")
        return content
