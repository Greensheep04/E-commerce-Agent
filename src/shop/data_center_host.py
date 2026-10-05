import asyncio
import json
from datetime import UTC
from hashlib import sha256

from data_center_protocol import AuthorizedScope, DataSnapshot, SourcePage
from integration_protocol import ResourceRef
from sqlalchemy import select

from shop.assessment_models import AssessmentEmployee, AssessmentQuota, AssessmentSource
from shop.assessment_order_adapter import ORDER_FILE_SHA256
from shop.assessment_orders import DataBlocked, configured_order_port
from shop.assessment_service import SalesMetricService, principal_for
from shop.assessment_types import PERIOD_END, PERIOD_START, SOURCE_KEY
from shop.customer_models import CustomerBinding, CustomerOrder, OrderDataset, OrderProduct
from shop.data_center_models import DataCenterGrant, DataCenterSource


def content_digest(value):
    return sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":")).encode()).hexdigest()


class ShopDataAuthorization:
    def __init__(self, authentication, maker):
        self.authentication = authentication
        self.maker = maker

    async def authorize(self, context, source_ids, action):
        return await asyncio.to_thread(self.authorize_sync, context, source_ids, action)

    def authorize_sync(self, context, source_ids, action):
        self.authentication.verify(context)
        if not source_ids or len(set(source_ids)) != len(source_ids):
            raise ValueError("授权来源需要非空且不重复")
        if action not in {"read", "sync", "manage", "ingest", "model"}:
            raise ValueError("未登记的资料操作")
        user = self.authentication.actor(context.actor_id)
        sync_actor = user.role == "data_sync"
        if sync_actor and action not in {"sync", "model"}:
            raise PermissionError("同步账号不能执行前台资料查询")
        if action == "sync" and not sync_actor:
            raise PermissionError("来源同步需要专用后台账号")
        source_wide, asset_ids, employees = [], [], []
        customer_id, organization = None, False
        policy = []
        with self.maker() as session:
            for source_id in source_ids:
                source = session.get(DataCenterSource, source_id)
                if source is None or not source.active:
                    raise PermissionError("来源未登记或已经撤回")
                grant = session.get(DataCenterGrant, (context.actor_id, source_id))
                policy.append({"source": source_id, "revision": source.revision,
                               "active": source.active, "sha256": source.sha256,
                               "properties": source.properties,
                               "permissions": [] if grant is None else grant.permissions,
                               "assets": [] if grant is None else grant.asset_ids,
                               "wide": False if grant is None else grant.source_wide})
                if sync_actor:
                    if grant is None or "sync" not in grant.permissions:
                        raise PermissionError("同步账号没有此登记来源的读取权限")
                    source_wide.append(source_id)
                    continue
                if action in {"manage", "ingest"}:
                    if user.role != "admin":
                        raise PermissionError("资料管理需要管理员权限")
                    source_wide.append(source_id)
                    continue
                if source.kind == "analytics":
                    principal = principal_for(session, context.actor_id)
                    employees.extend(str(value) for value in principal.allowed_employee_ids)
                    organization = principal.level == "admin"
                    policy.append({"level": principal.level,
                                   "employees": list(principal.allowed_employee_ids)})
                elif source.kind == "business":
                    binding = session.get(CustomerBinding, context.actor_id)
                    if binding is None:
                        raise PermissionError("当前账号尚未绑定业务客户")
                    customer_id = binding.customer_id
                    policy.append({"customer": customer_id, "revision": binding.source_revision})
                elif user.role == "admin":
                    source_wide.append(source_id)
                elif grant is None or "read" not in grant.permissions:
                    raise PermissionError("当前账号没有此来源的资料权限")
                elif grant.source_wide:
                    source_wide.append(source_id)
                else:
                    asset_ids.extend(f"{source_id}::{value}" for value in grant.asset_ids)
        values = {"actor_id": context.actor_id, "source_ids": sorted(source_ids),
                  "source_wide": sorted(set(source_wide)), "asset_ids": sorted(set(asset_ids)),
                  "employee_ids": sorted(set(employees)), "customer_id": customer_id,
                  "organization": organization, "source_admin": user.role == "admin",
                  "sync_actor": sync_actor}
        version = content_digest({"role": user.role, "policy": policy})
        return AuthorizedScope(**values, policy_version=version,
                               fingerprint=content_digest({**values, "policy_version": version}))


class ShopSourceReadPort:
    def __init__(self, settings, maker, authorization, *, order_port=None):
        self.settings = settings
        self.maker = maker
        self.authorization = authorization
        self.order_port = order_port

    def _source(self, source_id):
        with self.maker() as session:
            source = session.get(DataCenterSource, source_id)
            if source is None or not source.active:
                raise PermissionError("来源未登记或已经撤回")
            session.expunge(source)
            return source

    def _analytics_rows(self, source):
        port = self.order_port or configured_order_port(self.settings)
        if port is None:
            raise DataBlocked("订单来源适配器尚未配置")
        service = SalesMetricService(port)
        with self.maker() as session:
            source_identity = service.snapshot(session)
            employee_source = session.get(AssessmentSource, SOURCE_KEY)
            quotas = session.scalars(select(AssessmentQuota).where(
                AssessmentQuota.quota_date >= PERIOD_START,
                AssessmentQuota.quota_date < PERIOD_END,
            ).order_by(AssessmentQuota.employee_id)).all()
            employee_ids = {row.employee_id for row in quotas}
            employee_ids.update(session.scalars(select(CustomerOrder.sales_person_id).where(
                CustomerOrder.order_date >= PERIOD_START, CustomerOrder.order_date < PERIOD_END,
                CustomerOrder.source_revision == source.revision,
                CustomerOrder.sales_person_id.is_not(None),
            )).all())
            rows = []
            for employee_id in [None, *sorted(employee_ids)]:
                for row in port.evidence(session, employee_id=employee_id,
                                         period_start=PERIOD_START, period_end=PERIOD_END):
                    rows.append({"record_type": "order", "record_id": str(row.order_id),
                                 "order_id": row.order_id, "employee_id": row.employee_id,
                                 "order_date": row.order_date.isoformat(), "status": row.status,
                                 "subtotal": str(row.subtotal), "source_line": row.source_line,
                                 "source_sha256": ORDER_FILE_SHA256})
            quota_files = [entry for entry in employee_source.manifest["files"]
                           if entry["name"] == "SalesPersonQuotaHistory.csv"]
            if len(quota_files) != 1:
                raise DataBlocked("季度目标缺少唯一的来源文件摘要")
            for quota in quotas:
                rows.append({"record_type": "quota", "record_id": str(quota.employee_id),
                             "employee_id": quota.employee_id,
                             "period_start": PERIOD_START.date().isoformat(),
                             "period_end": PERIOD_END.date().isoformat(),
                             "quota_amount": str(quota.amount), "source_line": quota.source_line,
                             "source_sha256": quota_files[0]["sha256"]})
            rows.sort(key=lambda value: (value["record_type"], value["record_id"]))
            if sum(row["record_type"] == "order" for row in rows) != 1022:
                raise DataBlocked("来源导出没有完整包含登记的季度订单")
            return rows, source_identity, employee_source.imported_at

    def _snapshot_and_rows(self, source):
        if source.kind == "unavailable":
            raise DataBlocked("登记来源尚未提供真实业务数据")
        if source.kind == "analytics":
            rows, identity, created = self._analytics_rows(source)
        else:
            rows = []
            identity, created = source.properties, source.updated_at
        digest = content_digest({"source": source.source_id, "revision": source.revision,
                                 "sha256": source.sha256, "rows": rows, "identity": identity})
        snapshot = DataSnapshot(
            snapshot_id=digest, source_ids=[source.source_id],
            source_revisions={source.source_id: source.revision}, created_at=created,
            data_as_of=(PERIOD_END.replace(tzinfo=UTC) if source.kind == "analytics" else None),
            state="published", record_count=len(rows), schema_sha256=content_digest(
                sorted({key for row in rows for key in row})), content_sha256=digest,
            quality={"source_nature": source.nature, "source_sha256": source.sha256,
                     "identity": identity},
        )
        return snapshot, rows

    async def snapshot(self, source_id, context):
        source = await asyncio.to_thread(self._source, source_id)
        await self.authorization.authorize(context, [source_id], "sync" if source.kind == "analytics" else "read")
        snapshot, _ = await asyncio.to_thread(self._snapshot_and_rows, source)
        return snapshot

    async def read_page(self, source_id, snapshot_id, cursor, limit, context):
        if not 1 <= limit <= 1000:
            raise ValueError("来源分页数量必须在1至1000之间")
        source = await asyncio.to_thread(self._source, source_id)
        await self.authorization.authorize(context, [source_id], "sync")
        snapshot, rows = await asyncio.to_thread(self._snapshot_and_rows, source)
        if snapshot.snapshot_id != snapshot_id:
            raise ValueError("来源版本已经变化，分页不能混合快照")
        offset = 0 if cursor is None else int(cursor)
        if offset < 0 or offset > len(rows):
            raise ValueError("来源分页位置无效")
        following = offset + limit
        return SourcePage(snapshot=snapshot, rows=rows[offset:following],
                          next_cursor=str(following) if following < len(rows) else None,
                          mode="snapshot")

    async def read_current(self, source_id, record_id, context):
        scope = await self.authorization.authorize(context, [source_id], "read")
        source = await asyncio.to_thread(self._source, source_id)
        if source.kind == "unavailable":
            raise DataBlocked("当前业务来源尚未接入")
        if source.kind == "analytics":
            return await asyncio.to_thread(self._quota_record, source, record_id, scope)
        if source.kind != "business":
            raise ValueError("来源没有登记当前业务读取能力")
        return await asyncio.to_thread(self._current_record, source, record_id, scope)

    def _quota_record(self, source, record_id, scope):
        kind, separator, identity = record_id.partition(":")
        if kind != "quota" or not separator or not identity.isdecimal():
            raise ValueError("分析原始记录需要quota及员工编号")
        employee_id = int(identity)
        if not scope.organization and str(employee_id) not in scope.employee_ids:
            raise PermissionError("季度目标不在当前员工授权范围")
        with self.maker() as session:
            rows = session.scalars(select(AssessmentQuota).where(
                AssessmentQuota.employee_id == employee_id,
                AssessmentQuota.quota_date >= PERIOD_START,
                AssessmentQuota.quota_date < PERIOD_END)).all()
            if len(rows) != 1:
                raise DataBlocked("固定期间没有唯一的季度目标记录")
            original = session.get(AssessmentSource, SOURCE_KEY)
            if original is None or original.revision != source.revision:
                raise DataBlocked("季度目标来源版本与登记内容不一致")
            files = [item for item in original.manifest["files"]
                     if item["name"] == "SalesPersonQuotaHistory.csv"]
            if len(files) != 1:
                raise DataBlocked("季度目标没有唯一原件摘要")
            employee = session.get(AssessmentEmployee, employee_id)
            if employee is None or employee.source_key != original.id or employee.salesperson_line is None:
                raise DataBlocked("季度目标缺少同一来源版本的员工与销售人员记录")
            identity_records = []
            for file_name, kind, line in (("Employee.csv", "employee", employee.source_line),
                                          ("SalesPerson.csv", "salesperson", employee.salesperson_line)):
                identity_files = [item for item in original.manifest["files"] if item["name"] == file_name]
                if len(identity_files) != 1:
                    raise DataBlocked("员工身份没有唯一原件摘要")
                identity_records.append({
                    "source_id": original.source_id, "revision": original.revision,
                    "record_id": f"{kind}:{employee_id}", "file_name": file_name,
                    "source_line": line, "source_sha256": identity_files[0]["sha256"],
                    "BusinessEntityID": employee_id,
                })
            row = rows[0]
            return {"record_id": record_id, "employee_id": employee_id,
                    "quota_date": row.quota_date.isoformat(), "quota_amount": str(row.amount),
                    "source_line": row.source_line, "source_sha256": files[0]["sha256"],
                    "source_revision": original.revision, "identity_records": identity_records}

    def _current_record(self, source, record_id, scope):
        with self.maker() as session:
            if source.properties.get("business_type") == "products":
                row = session.get(OrderProduct, (source.revision, int(record_id)))
                if row is None:
                    raise LookupError("商品记录不存在")
                return {"record_id": str(row.product_id), "name": row.name,
                        "product_number": row.product_number, "list_price": str(row.list_price),
                        "source_revision": row.source_revision}
            row = session.get(CustomerOrder, (source.revision, int(record_id)))
            if row is None or row.customer_id != scope.customer_id:
                raise PermissionError("订单不在当前客户的授权范围")
            dataset = session.get(OrderDataset, source.revision)
            if dataset is None:
                raise DataBlocked("订单来源批次缺失")
            return {"record_id": str(row.order_id), "customer_id": row.customer_id,
                    "order_date": row.order_date.isoformat(), "status": row.status,
                    "subtotal": str(row.subtotal), "total_due": str(row.total_due),
                    "source_revision": row.source_revision,
                    "source_sha256": source.sha256}

    async def read_file(self, ref, context):
        return await asyncio.to_thread(self._read_file, ref, context)

    def _read_file(self, ref, context):
        with self.maker() as session:
            candidates = session.scalars(select(DataCenterSource).where(
                DataCenterSource.active.is_(True), DataCenterSource.kind == "document",
            )).all()
            matching = [row for row in candidates if row.resource is not None
                        and row.resource.get("resource_id") == ref.resource_id
                        and row.resource.get("module_id") == ref.module_id]
            source = next((row for row in matching if ResourceRef.model_validate(row.resource) == ref), None)
            if source is None:
                if matching:
                    raise ValueError("文件引用与登记版本或摘要不一致")
                raise PermissionError("文件没有登记在当前资料目录")
            scope = self.authorization.authorize_sync(context, [source.source_id], "read")
            asset_id = f"{source.source_id}::{source.properties['record_id']}"
            if source.source_id not in scope.source_wide and asset_id not in scope.asset_ids:
                raise PermissionError("文件不在当前资料授权范围")
            if ref != ResourceRef.model_validate(source.resource):
                raise ValueError("文件引用与登记版本或摘要不一致")
            root = self.settings.data_center_source_root.resolve()
            path = (root / source.relative_path).resolve()
            if not path.is_relative_to(root) or not path.is_file():
                raise ValueError("登记文件路径无效或不存在")
            content = path.read_bytes()
            if sha256(content).hexdigest() != source.sha256 or source.sha256 != ref.sha256:
                raise ValueError("文件内容摘要与来源登记不一致")
            return content
