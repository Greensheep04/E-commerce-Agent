from hashlib import sha256

from data_center_protocol import GrantRegistration, SourceRegistration
from integration_protocol import ResourceRef
from sqlalchemy import select

from shop.data_center_models import DataCenterGrant, DataCenterSource, DataCenterSourceVersion
from shop.integration_store import IntegrationStore
from shop.models import User


class DataCenterRegistry:
    def __init__(self, settings, maker, authentication):
        self.settings, self.maker, self.authentication = settings, maker, authentication
        self.store = IntegrationStore(maker)

    def _admin(self, context):
        self.authentication.verify(context)
        if self.authentication.actor(context.actor_id).role != "admin":
            raise PermissionError("资料目录管理需要管理员权限")

    @staticmethod
    def view(source):
        return {field: getattr(source, field) for field in (
            "source_id", "kind", "title", "revision", "nature", "license_id", "sha256",
            "properties", "resource", "relative_path", "active")}

    def list(self, context):
        self._admin(context)
        with self.maker() as session:
            return [self.view(row) for row in session.scalars(
                select(DataCenterSource).order_by(DataCenterSource.source_id))]

    def register(self, spec, context):
        self._admin(context)
        spec = SourceRegistration.model_validate_json(spec.model_dump_json())
        if spec.kind == "document":
            root = self.settings.data_center_source_root.resolve()
            path = (root / spec.relative_path).resolve()
            if not path.is_relative_to(root) or not path.is_file():
                raise ValueError("文档原件路径没有处于登记目录")
            if sha256(path.read_bytes()).hexdigest() != spec.sha256:
                raise ValueError("文档原件摘要与登记内容不一致")

        def create(session):
            if spec.resource is not None:
                duplicate = session.scalar(select(DataCenterSource.source_id).where(
                    DataCenterSource.source_id != spec.source_id,
                    DataCenterSource.resource["resource_id"].as_string() == spec.resource.resource_id,
                ).limit(1))
                if duplicate is not None:
                    raise ValueError("原件资源编号已经用于其他来源")
            row = session.get(DataCenterSource, spec.source_id, with_for_update=True)
            document = spec.model_dump(mode="json")
            if row is None:
                row = DataCenterSource(**document)
                session.add(row)
            else:
                previous_document = {field: getattr(row, field) for field in document}
                previous = session.get(DataCenterSourceVersion, (row.source_id, row.revision))
                if previous is None:
                    previous = DataCenterSourceVersion(source_id=row.source_id, revision=row.revision,
                                                       document=previous_document)
                    session.add(previous)
                elif previous.document != previous_document:
                    raise ValueError("当前目录与已保存来源版本不一致")
                if row.revision == spec.revision and previous_document != document:
                    raise ValueError("来源版本内容已经登记，修改来源说明需要新版本")
            version = session.get(DataCenterSourceVersion, (spec.source_id, spec.revision))
            if version is not None and version.document != document:
                raise ValueError("来源版本内容已经登记，修改来源说明需要新版本")
            for field, value in document.items():
                setattr(row, field, value)
            row.active = True
            session.flush()
            if version is None:
                session.add(DataCenterSourceVersion(source_id=spec.source_id, revision=spec.revision,
                                                    document=document))
                session.flush()
            return ResourceRef(module_id="data_center", resource_type="source_metadata", resource_id=spec.source_id)

        self.store.bind("data_center", "register_source", spec.model_dump(mode="json"), context, create)
        with self.maker() as session:
            return self.view(session.get(DataCenterSource, spec.source_id))

    def grant(self, spec, context):
        self._admin(context)
        spec = GrantRegistration.model_validate_json(spec.model_dump_json())

        def create(session):
            source = session.get(DataCenterSource, spec.source_id)
            user = session.get(User, spec.actor_id)
            if source is None or not source.active or user is None:
                raise ValueError("授权来源或账号没有登记")
            if ("sync" in spec.permissions) != (user.role == "data_sync" and bool(spec.permissions)):
                raise PermissionError("后台同步授权需要专用同步账号")
            if user.role == "data_sync" and "read" in spec.permissions:
                raise PermissionError("同步账号不能获得前台资料读取权限")
            if source.kind == "document" and any(value != source.properties["record_id"] for value in spec.asset_ids):
                raise ValueError("授权资料编号没有登记在来源")
            row = session.get(DataCenterGrant, (spec.actor_id, spec.source_id), with_for_update=True)
            if row is None:
                row = DataCenterGrant(actor_id=spec.actor_id, source_id=spec.source_id)
                session.add(row)
            row.permissions, row.asset_ids, row.source_wide = spec.permissions, spec.asset_ids, spec.source_wide
            session.flush()
            return ResourceRef(module_id="data_center", resource_type="source_metadata", resource_id=spec.source_id)

        self.store.bind("data_center", "set_grant", spec.model_dump(mode="json"), context, create)
        return spec.model_dump(mode="json")

    def withdraw(self, source_id, context):
        self._admin(context)

        def create(session):
            row = session.get(DataCenterSource, source_id, with_for_update=True)
            if row is None:
                raise LookupError("资料来源没有登记")
            row.active = False
            return ResourceRef(module_id="data_center", resource_type="source_metadata", resource_id=source_id)

        self.store.bind("data_center", "withdraw_source", {"source_id": source_id}, context, create)
        return {"source_id": source_id, "active": False}
