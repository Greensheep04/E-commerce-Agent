from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import sys
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar

import dlt
import sqlglot
from data_center_protocol import (
    AuthorizationPort,
    AuthorizedScope,
    DataEvidenceRef,
    DataQuery,
    DataResult,
    DataSnapshot,
    MetricDefinition,
    SourcePage,
    SourceReadPort,
)
from integration_protocol import CallContext
from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.postgres import PostgresSqlExpressionRenderer
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer
from metricflow_semantic_interfaces.parsing.dir_to_model import (
    parse_directory_of_yaml_files_to_semantic_manifest,
)
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from sqlalchemy import MetaData, Table, and_, or_, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlglot import exp

from data_center.analytics.models import (
    AnalyticsRecord,
    AnalyticsSnapshot,
    AnalyticsSyncPage,
    AnalyticsSyncRun,
)
from data_center.database import install_read_policies

_RELATIONS = {"sales_orders", "sales_quotas"}
_FIELDS = {
    "snapshot_id", "source_id", "source_revision", "record_id", "order_id", "employee_id", "employee",
    "order_date", "status", "subtotal", "period_start", "period_end", "quota_amount",
    "source_line", "source_sha256", "sales_amount", "order_count", "average_order_value",
    "quota_completion",
    "__sales_amount", "__order_count", "metric_time__day",
}


def validate_metric_sql(
    sql: str, *, schema: str = "analytics", relations: set[str] | None = None,
) -> str:
    statements = sqlglot.parse(sql, read="postgres")
    if len(statements) != 1 or not isinstance(statements[0], exp.Select):
        raise PermissionError("MetricFlow query must be one read-only SELECT")

    statement = statements[0]
    if statement.args.get("into") or statement.args.get("locks"):
        raise ValueError("MetricFlow query must be read-only")
    for node in statement.walk():
        if isinstance(node, (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Create,
                             exp.Drop, exp.Command, exp.Transaction)):
            raise PermissionError("MetricFlow query must be read-only")

    cte_names = {
        cte.alias_or_name.casefold()
        for cte in statement.find_all(exp.CTE)
    }
    allowed_relations = _RELATIONS if relations is None else relations
    for table in statement.find_all(exp.Table):
        relation = table.name.casefold()
        if not table.db and relation in cte_names:
            continue
        if table.db.casefold() != schema.casefold() or relation not in allowed_relations:
            raise ValueError(f"MetricFlow query references an unapproved relation: {table.sql()}")

    for column in statement.find_all(exp.Column):
        name = column.name.casefold()
        if name == "*":
            if not isinstance(column.parent, exp.Count):
                raise ValueError("MetricFlow query contains an unapproved field")
            continue
        if name not in _FIELDS:
            raise ValueError(f"MetricFlow query contains an unapproved field: {name}")
    for star in statement.find_all(exp.Star):
        if not isinstance(star.parent, exp.Count):
            raise TypeError("MetricFlow query contains an unapproved wildcard")

    return sql


class _MetricFlowSqlClient:
    @property
    def sql_engine_type(self):
        return SqlEngine.POSTGRES

    @property
    def sql_plan_renderer(self):
        return _PostgresSqlPlanRenderer()

    def query(self, stmt, sql_bind_parameter_set=None):
        raise RuntimeError("DC-04 uses MetricFlow explain to compile SQL")

    def execute(self, stmt, sql_bind_parameter_set=None):
        raise RuntimeError("MetricFlow compilation cannot execute SQL")

    def dry_run(self, stmt, sql_bind_parameter_set=None):
        raise RuntimeError("MetricFlow compilation cannot execute SQL")

    def close(self):
        return None

    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        return f":{bind_parameter_key}"


class _PostgresSqlPlanRenderer(DefaultSqlPlanRenderer):
    EXPR_RENDERER = PostgresSqlExpressionRenderer()


class AnalyticsEngine:
    _source_id = "microsoft/adventureworks"
    _period_start = date(2023, 7, 1)
    _period_end = date(2023, 10, 1)
    _page_size = 1000
    _metric_ids: ClassVar[set[str]] = {
        "sales_amount", "order_count", "average_order_value", "quota_completion",
    }
    _schema_digest = hashlib.sha256(
        b"order:record_id,order_id,employee_id,order_date,status,subtotal,source_line,source_sha256;"
        b"quota:record_id,employee_id,period_start,period_end,quota_amount,source_line,source_sha256"
    ).hexdigest()

    def __init__(
        self,
        database: Engine,
        artifact_root: Path,
        authorization: AuthorizationPort,
        source: SourceReadPort,
    ):
        self.database = database
        self.artifact_root = artifact_root
        self.authorization = authorization
        self.source = source
        options = database.get_execution_options()
        self.database_schema = options.get("schema_translate_map", {}).get(None, "public")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", self.database_schema):
            raise ValueError("analytics 数据库 schema 名称无效")

    async def sync(
        self, source_id: str, context: CallContext, *, task_id: str, run_id: str,
    ) -> DataSnapshot:
        if source_id != self._source_id:
            raise ValueError("DC-04 未登记该数据来源")
        scope = await self.authorization.authorize(context, [source_id], "sync")
        if scope.actor_id != context.actor_id:
            raise PermissionError("授权端口返回的同步主体与调用主体不一致")
        self._require_sync_scope(scope, source_id)
        source_snapshot = await self.source.snapshot(source_id, context)
        self._validate_source_snapshot(source_snapshot, source_id)
        run_dir = self._run_directory(task_id, run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self._start_sync_run, source_id, task_id, run_id,
                                source_snapshot.snapshot_id)
        previous = await asyncio.to_thread(self._completed_run_snapshot, task_id, run_id)
        if previous is not None:
            return previous
        try:
            pages = await self._read_snapshot_pages(
                source_id, source_snapshot, context, task_id=task_id, run_id=run_id,
            )
            orders, quotas = self._flatten_pages(pages)
            content_sha256 = self._content_sha256(orders, quotas)
            snapshot_id = hashlib.sha256(
                f"{source_snapshot.snapshot_id}:{self._schema_digest}:{content_sha256}".encode()
            ).hexdigest()
            existing = await asyncio.to_thread(
                self._find_published_content, source_snapshot, content_sha256,
            )
            if existing is not None:
                await asyncio.to_thread(self._complete_reused_run, task_id, run_id)
                await asyncio.to_thread(install_read_policies, self.database, self.database_schema)
                result = self._to_snapshot(
                    existing.snapshot_id, source_snapshot, existing.content_sha256,
                    existing.quality,
                )
                self._write_sync_evidence(
                    run_dir, source_snapshot, result, task_id, run_id, "reused",
                    self.database_schema, existing.quality,
                    {"reused": True}, {"reused": True},
                )
                return result
            stage_schema = f"dc_stage_{snapshot_id[:16]}"
            pipeline = dlt.pipeline(
                pipeline_name=f"dc_{snapshot_id[:16]}",
                destination=dlt.destinations.postgres(self._destination_url()),
                dataset_name=stage_schema,
                pipelines_dir=str(run_dir / "dlt"),
            )
            resources = [
                dlt.resource(orders, name="sales_orders_raw", write_disposition="replace"),
                dlt.resource(quotas, name="sales_quotas_raw", write_disposition="replace"),
            ]
            load_info = await asyncio.to_thread(pipeline.run, resources)
            quality = await asyncio.to_thread(
                self._stage_quality, stage_schema, snapshot_id, source_snapshot, orders, quotas,
            )
            await asyncio.to_thread(
                self._stage_published_records, snapshot_id, source_snapshot, task_id, run_id,
                orders, quotas, quality,
            )
            analytics_schema = self.database_schema
            dbt_output = await self._run_dbt(
                run_dir, snapshot_id, analytics_schema, source_snapshot,
            )
            await asyncio.to_thread(install_read_policies, self.database, analytics_schema)
            quality.update(await asyncio.to_thread(
                self._dbt_quality, analytics_schema, snapshot_id, quality,
            ))
            await asyncio.to_thread(self._publish_snapshot, snapshot_id, task_id, run_id, quality)
            result = self._to_snapshot(snapshot_id, source_snapshot, content_sha256, quality)
            self._write_sync_evidence(
                run_dir, source_snapshot, result, task_id, run_id, stage_schema,
                analytics_schema, quality, dbt_output, load_info,
            )
            return result
        except BaseException as error:
            status = "cancelled" if isinstance(error, asyncio.CancelledError) else "failed"
            await asyncio.to_thread(self._fail_sync_run, task_id, run_id, str(error), status)
            raise

    async def query(self, query: DataQuery, context: CallContext) -> DataResult:
        if query.intent != "metrics":
            raise ValueError("AnalyticsEngine 只处理 metrics 查询")
        if set(query.source_ids) != {self._source_id}:
            raise ValueError("指标查询必须指定已登记的 AdventureWorks 来源")
        if not set(query.metric_ids) <= self._metric_ids:
            raise ValueError("指标查询包含未登记指标")
        if set(query.dimensions) - {"employee_id"}:
            raise ValueError("指标查询包含未登记维度或筛选条件")
        requested_employee_ids = self._requested_employee_ids(query.filters)
        if (query.start_date, query.end_date) != (self._period_start, self._period_end):
            raise ValueError("当前来源仅支持 2023 年第三季度")

        scope = await self.authorization.authorize(context, [self._source_id], "read")
        if scope.actor_id != context.actor_id:
            raise PermissionError("授权端口返回的查询主体与调用主体不一致")
        self._require_query_scope(scope)
        if (requested_employee_ids is not None
                and not {str(value) for value in requested_employee_ids} <= set(scope.employee_ids)):
            raise PermissionError("员工筛选范围超出当前账号授权范围")
        snapshot = await asyncio.to_thread(self._get_snapshot, query.snapshot_id)
        analytics_schema = self.database_schema
        metrics = list(query.metric_ids)
        group_by = ["employee"] if "employee_id" in query.dimensions else []
        compiled = self._compile_metricflow_query(
            set(metrics), group_by, analytics_schema, query.start_date.isoformat(),
            query.end_date.isoformat(), 100,
        )
        scoped_sql = self._apply_authorization_scope(
            compiled, analytics_schema, snapshot, scope, set(metrics), requested_employee_ids,
        )
        safe_sql = validate_metric_sql(
            scoped_sql, schema=analytics_schema, relations={"sales_orders", "sales_quotas"},
        )
        rows = await asyncio.to_thread(self._execute_metric_sql, safe_sql, scope, snapshot.snapshot_id)
        rows = self._normalize_rows(rows, query, scope, snapshot, requested_employee_ids)
        result_hash = hashlib.sha256(json.dumps(
            rows, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        ).encode()).hexdigest()
        evidence = [DataEvidenceRef(
            kind="metric", source_id=self._source_id, snapshot_id=snapshot.snapshot_id,
            record_id=f"{metric_id}:{result_hash}",
            source_revision=snapshot.source_revisions[self._source_id],
            source_sha256=snapshot.content_sha256, content_sha256=result_hash,
            locator={
                "metric_id": metric_id, "formula": self._metric_formula(metric_id),
                "question": query.question, "metric_ids": metrics,
                "dimensions": query.dimensions,
                "start_date": query.start_date.isoformat(),
                "end_date": query.end_date.isoformat(), "filters": query.filters,
                "limit": query.limit, "aggregate_sha256": result_hash,
                "authorized_employee_ids": scope.employee_ids,
                "organization": scope.organization,
            },
        ) for metric_id in metrics]
        self._write_query_evidence(query, context, snapshot, safe_sql, rows, scope)
        return DataResult(
            result_id=f"{context.request_id}:{snapshot.snapshot_id}", status="completed",
            rows=rows, evidence=evidence, snapshots=[snapshot],
            metric_versions={metric_id: "sales-q3-v1" for metric_id in metrics},
            metadata={"data_as_of": snapshot.data_as_of.isoformat() if snapshot.data_as_of else None,
                      "currency": None, "currency_status": "未核实"},
        )

    async def read_evidence(self, ref: DataEvidenceRef, context: CallContext) -> dict[str, Any]:
        if ref.kind != "metric" or ref.source_id != self._source_id:
            raise ValueError("指标证据来源或类型无效")
        locator = ref.locator
        metric_id = locator.get("metric_id")
        metric_ids = locator.get("metric_ids")
        required_locator_fields = {
            "metric_id", "formula", "question", "metric_ids", "dimensions", "start_date",
            "end_date", "filters", "limit", "aggregate_sha256", "authorized_employee_ids",
            "organization",
        }
        if required_locator_fields - locator.keys():
            raise ValueError("指标证据缺少查询重建条件")
        if (not isinstance(metric_id, str) or not isinstance(metric_ids, list)
                or metric_id not in self._metric_ids or metric_id not in metric_ids
                or locator.get("formula") != self._metric_formula(metric_id)
                or locator.get("aggregate_sha256") != ref.content_sha256
                or ref.record_id != f"{metric_id}:{ref.content_sha256}"):
            raise ValueError("指标证据公式或聚合摘要无效")
        snapshot = await asyncio.to_thread(self._get_snapshot, ref.snapshot_id)
        if (ref.source_revision != snapshot.source_revisions[self._source_id]
                or ref.source_sha256 != snapshot.content_sha256):
            raise ValueError("指标证据来源快照摘要不一致")

        scope = await self.authorization.authorize(context, [self._source_id], "read")
        if scope.actor_id != context.actor_id:
            raise PermissionError("授权端口返回的查询主体与调用主体不一致")
        self._require_query_scope(scope)
        original_employee_id_values = locator["authorized_employee_ids"]
        if (not isinstance(original_employee_id_values, list)
                or any(not isinstance(value, str) for value in original_employee_id_values)):
            raise TypeError("指标证据员工授权范围格式无效")
        original_employee_ids = set(original_employee_id_values)
        original_organization = locator.get("organization")
        if not isinstance(original_organization, bool):
            raise TypeError("指标证据缺少原始组织授权范围")
        if original_organization and not scope.organization:
            raise PermissionError("当前组织权限无法读取此前的组织范围指标证据")
        if (not original_organization and not scope.organization
                and not original_employee_ids <= set(scope.employee_ids)):
            raise PermissionError("当前员工权限无法读取此前的员工范围指标证据")

        filters = locator["filters"]
        if not original_organization and not filters:
            filters = {"employee_ids": [int(value) for value in sorted(original_employee_ids)]}
        query = DataQuery(
            question=locator["question"], intent="metrics", source_ids=[self._source_id],
            snapshot_id=ref.snapshot_id, metric_ids=metric_ids, dimensions=locator["dimensions"],
            start_date=date.fromisoformat(locator["start_date"]),
            end_date=date.fromisoformat(locator["end_date"]), filters=filters,
            limit=locator["limit"],
        )
        result = await self.query(query, context)
        matching_evidence = [value for value in result.evidence if value.locator["metric_id"] == metric_id]
        if (len(matching_evidence) != 1
                or matching_evidence[0].content_sha256 != ref.content_sha256
                or matching_evidence[0].record_id != ref.record_id):
            raise PermissionError("当前授权范围或指标结果与原始证据不一致")

        scope = await self.authorization.authorize(context, [self._source_id], "read")
        if scope.actor_id != context.actor_id:
            raise PermissionError("授权端口返回的查询主体与调用主体不一致")
        self._require_query_scope(scope)
        if original_organization and not scope.organization:
            raise PermissionError("当前组织权限无法读取此前的组织范围指标证据")
        if (not original_organization and not scope.organization
                and not original_employee_ids <= set(scope.employee_ids)):
            raise PermissionError("当前员工权限无法读取此前的员工范围指标证据")
        records = await asyncio.to_thread(
            self._read_metric_source_rows, query, scope, snapshot.snapshot_id,
        )
        original_quotas = []
        for row in records:
            if row["record_type"] != "quota":
                continue
            employee_id = row["employee_id"]
            current = await self.source.read_current(
                self._source_id, f"quota:{employee_id}", context,
            )
            required_current_fields = {
                "record_id", "employee_id", "quota_date", "quota_amount", "source_line",
                "source_sha256", "source_revision",
            }
            if required_current_fields - current.keys():
                raise ValueError("季度目标原始来源记录缺少核对字段")
            quota_date = datetime.fromisoformat(current["quota_date"]).date()
            if (current["record_id"] != f"quota:{employee_id}"
                    or current["employee_id"] != employee_id
                    or quota_date < query.start_date or quota_date >= query.end_date):
                raise ValueError("季度目标原始来源记录编号或日期与查询范围不一致")
            if Decimal(current["quota_amount"]) != Decimal(row["quota_amount"]):
                raise ValueError("季度目标原始金额与已发布分析快照不一致")
            if current["source_line"] != row["source_line"]:
                raise ValueError("季度目标原始来源行号与已发布分析快照不一致")
            if current["source_sha256"] != row["source_sha256"]:
                raise ValueError("季度目标原始 CSV 摘要与已发布分析快照不一致")
            if current["source_revision"] != snapshot.source_revisions[self._source_id]:
                raise ValueError("季度目标来源版本与已发布分析快照不一致")
            row["quota_date"] = quota_date.isoformat()
            row["quota_amount"] = current["quota_amount"]
            row["source_revision"] = current["source_revision"]
            original_quotas.append({
                "record_id": current["record_id"], "employee_id": current["employee_id"],
                "quota_date": quota_date.isoformat(), "quota_amount": current["quota_amount"],
                "source_line": current["source_line"], "source_sha256": current["source_sha256"],
                "source_revision": current["source_revision"],
            })
            if "identity_records" in current:
                identities = current["identity_records"]
                if not isinstance(identities, list) or not identities:
                    raise ValueError("员工原始身份记录需要非空列表")
                required_identity = {"source_id", "revision", "record_id", "file_name",
                                     "source_line", "source_sha256", "BusinessEntityID"}
                for identity in identities:
                    if (not isinstance(identity, dict) or required_identity - identity.keys()
                            or identity["source_id"] != self._source_id
                            or identity["revision"] != current["source_revision"]
                            or identity["BusinessEntityID"] != employee_id):
                        raise ValueError("员工原始身份与季度目标来源不一致")
                original_quotas[-1]["identity_records"] = identities
        raw_sha256 = hashlib.sha256(json.dumps(
            records, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        ).encode()).hexdigest()
        return {
            "metric_id": metric_id, "formula": self._metric_formula(metric_id),
            "snapshot_id": snapshot.snapshot_id, "source_revision": ref.source_revision,
            "aggregate_sha256": ref.content_sha256, "source_rows_sha256": raw_sha256,
            "rows": records, "original_quotas": original_quotas,
        }

    @staticmethod
    def metric_definitions() -> list[MetricDefinition]:
        definitions = [
            MetricDefinition(
                metric_id="sales_amount", title="销售额", version="sales-q3-v1",
                source_ids=["microsoft/adventureworks"],
                formula="SUM(SubTotal)，仅计算 Status=5 的订单", dimensions=["employee_id"],
                unit="来源币种金额，币种未核实",
                properties={"field": "SubTotal", "filter": {"Status": 5}},
            ),
            MetricDefinition(
                metric_id="order_count", title="订单数", version="sales-q3-v1",
                source_ids=["microsoft/adventureworks"],
                formula="COUNT(DISTINCT SalesOrderID)，仅计算 Status=5 的订单",
                dimensions=["employee_id"], unit="订单",
                properties={"field": "SalesOrderID", "filter": {"Status": 5}},
            ),
            MetricDefinition(
                metric_id="average_order_value", title="客单价", version="sales-q3-v1",
                source_ids=["microsoft/adventureworks"],
                formula="SUM(SubTotal) / COUNT(DISTINCT SalesOrderID)，仅计算 Status=5 的订单",
                dimensions=["employee_id"], unit="来源币种金额/订单，币种未核实",
                properties={"numerator": "sales_amount", "denominator": "order_count"},
            ),
            MetricDefinition(
                metric_id="quota_completion", title="季度目标完成率", version="sales-q3-v1",
                source_ids=["microsoft/adventureworks"],
                formula="销售额 / SalesQuota，返回比例值", dimensions=["employee_id"],
                unit="比例", properties={"numerator": "sales_amount", "denominator": "quota_amount"},
            ),
        ]
        for definition in definitions:
            definition.properties["query_periods"] = [{
                "start_date": AnalyticsEngine._period_start.isoformat(),
                "end_date": AnalyticsEngine._period_end.isoformat(),
                "end_exclusive": True,
            }]
        return definitions

    @staticmethod
    def _requested_employee_ids(filters: dict[str, Any]) -> list[int] | None:
        if not filters:
            return None
        if set(filters) != {"employee_ids"}:
            raise ValueError("指标查询包含未登记筛选条件")
        employee_ids = filters["employee_ids"]
        if (not isinstance(employee_ids, list) or not employee_ids
                or any(type(value) is not int or value < 1 for value in employee_ids)
                or len(employee_ids) != len(set(employee_ids))):
            raise ValueError("员工筛选必须包含不重复的正整数编号")
        return employee_ids

    @staticmethod
    def _compile_metricflow_query(
        metric_ids: set[str], group_by: list[str], schema: str,
        start_date: str, end_date: str, limit: int,
    ) -> str:
        manifest = parse_directory_of_yaml_files_to_semantic_manifest(
            str(Path(__file__).parent / "metricflow"),
            template_mapping={"schema": schema},
        ).semantic_manifest
        engine = MetricFlowEngine(SemanticManifestLookup(manifest), _MetricFlowSqlClient())
        request = MetricFlowQueryRequest.create(
            metric_names=sorted(metric_ids), group_by_names=group_by, limit=limit,
            time_constraint_start=datetime.combine(date.fromisoformat(start_date), time.min),
            time_constraint_end=datetime.combine(date.fromisoformat(end_date), time.min),
        )
        return engine.explain(request).sql_statement.sql

    @staticmethod
    def _metric_formula(metric_id: str) -> str:
        return {
            "sales_amount": "SUM(SubTotal), Status=5",
            "order_count": "COUNT(DISTINCT SalesOrderID), Status=5",
            "average_order_value": "sales_amount / order_count",
            "quota_completion": "sales_amount / SalesQuota",
        }[metric_id]

    @staticmethod
    def _require_sync_scope(scope: AuthorizedScope, source_id: str) -> None:
        if (not scope.sync_actor or source_id not in scope.source_ids
                or source_id not in scope.source_wide):
            raise PermissionError("同步账号没有来源全量读取权限")

    @staticmethod
    def _require_query_scope(scope: AuthorizedScope) -> None:
        if "microsoft/adventureworks" not in scope.source_ids:
            raise PermissionError("当前账号没有 AdventureWorks 考核读取权限")
        if not scope.employee_ids and not scope.organization:
            raise PermissionError("当前账号没有已分配员工的考核读取权限")

    @classmethod
    def _validate_source_snapshot(cls, snapshot: DataSnapshot, source_id: str) -> None:
        if (snapshot.state != "published" or source_id not in snapshot.source_ids
                or snapshot.source_revisions.get(source_id) != "beaab06ef72831089ca80e5355d65e661fd19b26"
                or snapshot.record_count < 1):
            raise ValueError("来源快照状态、来源编号或记录数量无效")

    def _run_directory(self, task_id: str, run_id: str) -> Path:
        for identity in (task_id, run_id):
            if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}", identity):
                raise ValueError("同步任务或运行编号包含无效字符")
        return (self.artifact_root / f"dc04-{task_id}-{run_id}").resolve()

    def _start_sync_run(
        self, source_id: str, task_id: str, run_id: str, source_snapshot_id: str,
    ) -> None:
        with Session(self.database, expire_on_commit=False) as session, session.begin():
            run = session.query(AnalyticsSyncRun).filter_by(task_id=task_id, run_id=run_id).one_or_none()
            if run is not None:
                if run.source_id != source_id or run.source_snapshot_id != source_snapshot_id:
                    raise ValueError("已存在的同步运行与来源快照冲突")
                if run.status == "completed":
                    return
                run.status = "running"
                run.error = None
                run.updated_at = datetime.now(UTC)
                return
            session.add(AnalyticsSyncRun(
                task_id=task_id, run_id=run_id, source_id=source_id,
                source_snapshot_id=source_snapshot_id, status="running", next_cursor=None,
                pages_read=0, records_read=0, updated_at=datetime.now(UTC),
            ))

    async def _read_snapshot_pages(
        self, source_id: str, source_snapshot: DataSnapshot, context: CallContext,
        *, task_id: str, run_id: str,
    ) -> list[AnalyticsSyncPage]:
        pages = await asyncio.to_thread(
            self._get_stored_pages, task_id, run_id, source_snapshot.snapshot_id,
        )
        page_number = len(pages)
        cursor = pages[-1].next_cursor if pages else None
        seen_cursors = {page.cursor for page in pages if page.cursor is not None}
        previous_key = self._last_page_key(pages)
        if pages and cursor is None:
            return pages
        while True:
            page = await self.source.read_page(
                source_id, source_snapshot.snapshot_id, cursor, self._page_size, context,
            )
            self._validate_source_page(page, source_snapshot)
            normalized = self._normalize_source_rows(page.rows, previous_key)
            previous_key = self._row_key(normalized[-1]) if normalized else previous_key
            if page.next_cursor is not None and (
                    page.next_cursor == cursor or page.next_cursor in seen_cursors):
                raise ValueError("来源分页游标重复")
            stored = await asyncio.to_thread(
                self._store_sync_page, task_id, run_id, source_snapshot.snapshot_id,
                page_number, cursor, page.next_cursor, normalized,
            )
            pages.append(stored)
            page_number += 1
            cursor = page.next_cursor
            if cursor is None:
                break
            seen_cursors.add(cursor)
        return pages

    def _get_stored_pages(
        self, task_id: str, run_id: str, source_snapshot_id: str,
    ) -> list[AnalyticsSyncPage]:
        with Session(self.database) as session:
            pages = session.query(AnalyticsSyncPage).filter_by(
                task_id=task_id, run_id=run_id,
            ).order_by(AnalyticsSyncPage.page_number).all()
            for expected, page in enumerate(pages):
                if page.page_number != expected or page.source_snapshot_id != source_snapshot_id:
                    raise ValueError("已保存分页与来源快照不一致")
                digest = self._page_digest(page.rows)
                if digest != page.content_sha256:
                    raise ValueError("已保存分页内容摘要不一致")
                if expected and pages[expected - 1].next_cursor != page.cursor:
                    raise ValueError("已保存分页游标链不连续")
            return pages

    @staticmethod
    def _last_page_key(pages: list[AnalyticsSyncPage]) -> tuple[str, str] | None:
        for page in reversed(pages):
            if page.rows:
                row = page.rows[-1]
                return row["record_type"], row["record_id"]
        return None

    def _store_sync_page(
        self, task_id: str, run_id: str, source_snapshot_id: str, page_number: int,
        cursor: str | None, next_cursor: str | None, rows: list[dict[str, Any]],
    ) -> AnalyticsSyncPage:
        digest = self._page_digest(rows)
        with Session(self.database, expire_on_commit=False) as session, session.begin():
            run = session.query(AnalyticsSyncRun).filter_by(task_id=task_id, run_id=run_id).one()
            prior = session.query(AnalyticsSyncPage).filter_by(
                task_id=task_id, run_id=run_id, page_number=page_number,
            ).one_or_none()
            if prior is not None:
                if (prior.source_snapshot_id != source_snapshot_id or prior.cursor != cursor
                        or prior.next_cursor != next_cursor or prior.content_sha256 != digest):
                    raise ValueError("重复同步分页内容与已保存内容冲突")
                return prior
            page = AnalyticsSyncPage(
                task_id=task_id, run_id=run_id, source_snapshot_id=source_snapshot_id,
                page_number=page_number, cursor=cursor, next_cursor=next_cursor,
                rows=self._serialize_rows(rows), content_sha256=digest,
            )
            session.add(page)
            run.next_cursor = next_cursor
            run.pages_read = page_number + 1
            run.records_read += len(rows)
            run.updated_at = datetime.now(UTC)
            session.flush()
            return page

    @staticmethod
    def _serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        serialized = []
        for row in rows:
            item = {}
            for key, value in row.items():
                if isinstance(value, (date, datetime)):
                    item[key] = value.isoformat()
                elif isinstance(value, Decimal):
                    item[key] = str(value)
                else:
                    item[key] = value
            serialized.append(item)
        return serialized

    @classmethod
    def _page_digest(cls, rows: list[dict[str, Any]]) -> str:
        serialized = json.dumps(
            cls._serialize_rows(rows), sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        )
        return hashlib.sha256(serialized.encode()).hexdigest()

    def _flatten_pages(
        self, pages: list[AnalyticsSyncPage],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        all_rows = []
        previous_key = None
        for page in pages:
            normalized = self._normalize_source_rows(page.rows, previous_key)
            if self._page_digest(self._serialize_rows(normalized)) != page.content_sha256:
                raise ValueError("已保存来源分页内容摘要不一致")
            if page.rows and self._row_key(normalized[-1]) != self._row_key(page.rows[-1]):
                raise ValueError("来源分页恢复结果与保存编号不一致")
            if normalized:
                previous_key = self._row_key(normalized[-1])
            all_rows.extend(normalized)
        record_ids = [(row["record_type"], row["record_id"]) for row in all_rows]
        if len(record_ids) != len(set(record_ids)):
            raise ValueError("来源快照包含重复业务编号")
        orders = [row for row in all_rows if row["record_type"] == "order"]
        quotas = [row for row in all_rows if row["record_type"] == "quota"]
        if len(all_rows) < 1:
            raise ValueError("来源快照没有业务记录")
        return orders, quotas

    @classmethod
    def _content_sha256(
        cls, orders: list[dict[str, Any]], quotas: list[dict[str, Any]],
    ) -> str:
        body = cls._serialize_rows(orders + quotas)
        return cls._page_digest(body)

    def _destination_url(self) -> str:
        return self.database.url.set(
            drivername="postgresql", password=self.database.url.password or "",
        ).render_as_string(hide_password=False)

    def _stage_quality(
        self, stage_schema: str, snapshot_id: str, source_snapshot: DataSnapshot,
        orders: list[dict[str, Any]], quotas: list[dict[str, Any]],
    ) -> dict[str, Any]:
        metadata = MetaData()
        orders_table = Table("sales_orders_raw", metadata, schema=stage_schema, autoload_with=self.database)
        quotas_table = Table("sales_quotas_raw", metadata, schema=stage_schema, autoload_with=self.database)
        with self.database.connect() as connection:
            staged_orders = connection.execute(select(orders_table)).mappings().all()
            staged_quotas = connection.execute(select(quotas_table)).mappings().all()
        if len(staged_orders) != len(orders) or len(staged_quotas) != len(quotas):
            raise ValueError("dlt 暂存数量与来源快照数量不一致")
        expected_rows = self._serialize_rows(orders + quotas)
        staged_records = [
            {key: value for key, value in row.items() if not key.startswith("_dlt_")}
            for row in (*staged_orders, *staged_quotas)
        ]
        staged_keys = {(row["record_type"], row["record_id"]) for row in staged_records}
        expected_keys = {(row["record_type"], row["record_id"]) for row in expected_rows}
        if staged_keys != expected_keys:
            raise ValueError("dlt 暂存记录编号与来源快照不一致")
        if len(orders) + len(quotas) != source_snapshot.record_count:
            raise ValueError("来源快照记录总数与固定分页读取数量不一致")
        quarter_orders = [
            row for row in orders if self._period_start <= row["order_date"].date() < self._period_end
        ]
        assigned = sum(row["employee_id"] is not None for row in quarter_orders)
        unassigned = sum(row["employee_id"] is None for row in quarter_orders)
        shipped = [row for row in quarter_orders if row["status"] == 5]
        if (len(quarter_orders), assigned, unassigned, len(shipped)) != (1022, 317, 705, 1022):
            raise ValueError("固定季度订单数量、状态或员工归属不符合 AdventureWorks 来源")
        quarter_quotas = [
            row for row in quotas
            if self._period_start <= row["period_start"] < self._period_end
        ]
        quota_employees = [row["employee_id"] for row in quarter_quotas]
        if len(quarter_quotas) != 14 or len(quota_employees) != len(set(quota_employees)):
            raise ValueError("固定季度目标记录数量或员工归属不完整")
        return {
            "source_snapshot_id": source_snapshot.snapshot_id,
            "source_revision": source_snapshot.source_revisions[self._source_id],
            "order_count": len(orders), "quota_count": len(quotas),
            "quarter_order_count": len(quarter_orders), "shipped_order_count": len(shipped),
            "assigned_order_count": assigned, "unassigned_order_count": unassigned,
            "quarter_quota_count": len(quarter_quotas), "snapshot_id": snapshot_id,
            "content_sha256": self._content_sha256(orders, quotas),
        }

    def _dbt_quality(
        self, analytics_schema: str, snapshot_id: str, staged_quality: dict[str, Any],
    ) -> dict[str, Any]:
        metadata = MetaData()
        orders = Table("sales_orders", metadata, schema=analytics_schema, autoload_with=self.database)
        quotas = Table("sales_quotas", metadata, schema=analytics_schema, autoload_with=self.database)
        with self.database.connect() as connection:
            order_counts = connection.execute(select(
                text("count(*) filter (where order_date >= date '2023-07-01' and order_date < date '2023-10-01')"),
                text("count(*) filter (where order_date >= date '2023-07-01' and order_date < date '2023-10-01' and employee_id is not null)"),
                text("count(*) filter (where order_date >= date '2023-07-01' and order_date < date '2023-10-01' and employee_id is null)"),
            ).select_from(orders).where(orders.c.snapshot_id == snapshot_id)).one()
            quota_count = connection.scalar(select(text("count(*)")).select_from(quotas).where(
                quotas.c.snapshot_id == snapshot_id,
                quotas.c.period_start >= self._period_start,
                quotas.c.period_start < self._period_end,
            ))
        if tuple(order_counts) != (1022, 317, 705) or quota_count != 14:
            raise ValueError("dbt 转换结果的季度订单或目标质量检查失败")
        return {
            "dbt_quarter_order_count": order_counts[0],
            "dbt_assigned_order_count": order_counts[1],
            "dbt_unassigned_order_count": order_counts[2],
            "dbt_quarter_quota_count": quota_count,
            "dbt_snapshot_id": snapshot_id,
            "dbt_matches_dlt": True,
            "dlt_quarter_order_count": staged_quality["quarter_order_count"],
            "dlt_quarter_quota_count": staged_quality["quarter_quota_count"],
        }

    def _publish_snapshot(self, snapshot_id: str, task_id: str, run_id: str,
                          quality: dict[str, Any]) -> None:
        now = datetime.now(UTC)
        with Session(self.database) as session, session.begin():
            snapshot = session.get(AnalyticsSnapshot, snapshot_id)
            run = session.query(AnalyticsSyncRun).filter_by(task_id=task_id, run_id=run_id).one()
            if snapshot is None or snapshot.content_sha256 != quality["content_sha256"]:
                raise ValueError("待发布快照不存在或内容摘要发生变化")
            if snapshot.state not in {"staged", "published"}:
                raise ValueError("快照状态不能发布")
            snapshot.state = "published"
            snapshot.published_at = snapshot.published_at or now
            snapshot.quality = quality
            run.status = "completed"
            run.error = None
            run.updated_at = now

    def _to_snapshot(
        self, snapshot_id: str, source_snapshot: DataSnapshot,
        content_sha256: str, quality: dict[str, Any],
    ) -> DataSnapshot:
        with Session(self.database) as session:
            record = session.get(AnalyticsSnapshot, snapshot_id)
            if record is None or record.state != "published":
                raise ValueError("同步返回前没有已发布的不可变快照")
            return DataSnapshot(
                snapshot_id=record.snapshot_id, source_ids=[record.source_id],
                source_revisions={record.source_id: record.source_revision},
                created_at=record.created_at, data_as_of=source_snapshot.data_as_of,
                state=record.state, record_count=record.record_count,
                schema_sha256=record.schema_sha256, content_sha256=content_sha256,
                quality=quality,
            )

    def _fail_sync_run(self, task_id: str, run_id: str, error: str, status: str) -> None:
        with Session(self.database) as session, session.begin():
            run = session.query(AnalyticsSyncRun).filter_by(task_id=task_id, run_id=run_id).one_or_none()
            if run is not None and run.status != "completed":
                run.status = status
                password = self.database.url.password
                safe_error = error.replace(password, "[REDACTED]") if password else error
                run.error = safe_error[:4000]
                run.updated_at = datetime.now(UTC)

    def _find_published_content(
        self, source_snapshot: DataSnapshot, content_sha256: str,
    ) -> AnalyticsSnapshot | None:
        with Session(self.database, expire_on_commit=False) as session:
            return session.query(AnalyticsSnapshot).filter_by(
                source_id=self._source_id,
                source_revision=source_snapshot.source_revisions[self._source_id],
                content_sha256=content_sha256,
                state="published",
            ).order_by(AnalyticsSnapshot.published_at.desc()).first()

    def _complete_reused_run(self, task_id: str, run_id: str) -> None:
        with Session(self.database) as session, session.begin():
            run = session.query(AnalyticsSyncRun).filter_by(task_id=task_id, run_id=run_id).one()
            run.status = "completed"
            run.error = None
            run.updated_at = datetime.now(UTC)

    def _get_snapshot(self, snapshot_id: str | None) -> DataSnapshot:
        with Session(self.database) as session:
            if snapshot_id is None:
                record = session.query(AnalyticsSnapshot).filter_by(
                    source_id=self._source_id, state="published",
                ).order_by(AnalyticsSnapshot.published_at.desc()).first()
            else:
                record = session.get(AnalyticsSnapshot, snapshot_id)
                if record is not None and (record.state != "published" or record.source_id != self._source_id):
                    raise ValueError("指定分析快照尚未发布或来源不符")
            if record is None:
                raise ValueError("AdventureWorks 尚无已发布分析快照")
            quality = record.quality
            return DataSnapshot(
                snapshot_id=record.snapshot_id, source_ids=[record.source_id],
                source_revisions={record.source_id: record.source_revision},
                created_at=record.created_at, data_as_of=record.created_at,
                state=record.state, record_count=record.record_count,
                schema_sha256=record.schema_sha256, content_sha256=record.content_sha256,
                quality=quality,
            )

    def _apply_authorization_scope(
        self, sql: str, schema: str, snapshot: DataSnapshot, scope: AuthorizedScope,
        metric_ids: set[str], requested_employee_ids: list[int] | None,
    ) -> str:
        if scope.actor_id == "" or not scope.policy_version:
            raise PermissionError("授权端口没有返回有效的服务端策略身份")
        allowed_employees = sorted(
            str(value) for value in (
                requested_employee_ids if requested_employee_ids is not None else scope.employee_ids
            )
        )
        if not scope.organization and not allowed_employees:
            raise PermissionError("当前账号没有可查询的员工范围")
        tree = sqlglot.parse_one(sql, read="postgres")
        found = set()
        for select_node in tree.find_all(exp.Select):
            from_clause = select_node.args.get("from_")
            direct_sources = []
            if from_clause is not None:
                direct_sources.append(from_clause.this)
            direct_sources.extend(join.this for join in select_node.args.get("joins") or [])
            for source_node in direct_sources:
                if not isinstance(source_node, exp.Table):
                    continue
                table_name = source_node.name.casefold()
                if source_node.db.casefold() != schema.casefold() or table_name not in _RELATIONS:
                    continue
                found.add(table_name)
                alias = source_node.alias_or_name
                predicates = [exp.EQ(
                    this=exp.column("snapshot_id", table=alias),
                    expression=exp.Literal.string(snapshot.snapshot_id),
                )]
                if requested_employee_ids is not None or not scope.organization:
                    predicates.append(exp.In(
                        this=exp.column("employee_id", table=alias),
                        expressions=[exp.Literal.number(employee_id) for employee_id in allowed_employees],
                    ))
                if table_name == "sales_orders":
                    predicates.extend((
                        exp.GTE(this=exp.column("order_date", table=alias),
                                expression=exp.Literal.string(self._period_start.isoformat())),
                        exp.LT(this=exp.column("order_date", table=alias),
                               expression=exp.Literal.string(self._period_end.isoformat())),
                    ))
                else:
                    predicates.extend((
                        exp.GTE(this=exp.column("period_start", table=alias),
                                expression=exp.Literal.string(self._period_start.isoformat())),
                        exp.LT(this=exp.column("period_start", table=alias),
                               expression=exp.Literal.string(self._period_end.isoformat())),
                    ))
                predicate = predicates[0]
                for additional in predicates[1:]:
                    predicate = exp.and_(predicate, additional)
                existing = select_node.args.get("where")
                if existing is not None:
                    predicate = exp.and_(existing.this, predicate)
                select_node.set("where", exp.Where(this=predicate))
        required = {"sales_orders"}
        if "quota_completion" in metric_ids:
            required.add("sales_quotas")
        if not required <= found:
            raise ValueError("MetricFlow 编译查询没有覆盖要求的授权来源表")
        return tree.sql(dialect="postgres")

    def _completed_run_snapshot(self, task_id: str, run_id: str) -> DataSnapshot | None:
        with Session(self.database) as session:
            run = session.query(AnalyticsSyncRun).filter_by(task_id=task_id, run_id=run_id).one()
            if run.status != "completed":
                return None
            pages = self._get_stored_pages(task_id, run_id, run.source_snapshot_id)
            orders, quotas = self._flatten_pages(pages)
            content_sha256 = self._content_sha256(orders, quotas)
            record = session.query(AnalyticsSnapshot).filter_by(
                source_id=run.source_id, source_snapshot_id=run.source_snapshot_id,
                schema_sha256=self._schema_digest, content_sha256=content_sha256,
                state="published",
            ).one()
            source_snapshot = DataSnapshot(
                snapshot_id=record.source_snapshot_id, source_ids=[record.source_id],
                source_revisions={record.source_id: record.source_revision},
                created_at=record.created_at, state="published", record_count=record.record_count,
                schema_sha256=record.schema_sha256, content_sha256=record.content_sha256,
            )
            return self._to_snapshot(
                record.snapshot_id, source_snapshot, record.content_sha256,
                record.quality,
            )

    def _execute_metric_sql(
        self, sql: str, scope: AuthorizedScope, snapshot_id: str,
    ) -> list[dict[str, Any]]:
        from data_center.database import authorized_connection

        with authorized_connection(self.database, scope, snapshot_id) as connection:
            result = connection.execute(text(sql))
            return [dict(row) for row in result.mappings().all()]

    def _read_metric_source_rows(
        self, query: DataQuery, scope: AuthorizedScope, snapshot_id: str,
    ) -> list[dict[str, Any]]:
        from data_center.database import authorized_connection

        order_predicates = [
            AnalyticsRecord.snapshot_id == snapshot_id,
            AnalyticsRecord.source_id == self._source_id,
            AnalyticsRecord.record_type == "order",
            AnalyticsRecord.order_date >= query.start_date,
            AnalyticsRecord.order_date < query.end_date,
            AnalyticsRecord.status == 5,
        ]
        quota_predicates = [
            AnalyticsRecord.snapshot_id == snapshot_id,
            AnalyticsRecord.source_id == self._source_id,
            AnalyticsRecord.record_type == "quota",
            AnalyticsRecord.period_start >= query.start_date,
            AnalyticsRecord.period_start < query.end_date,
        ]
        employee_ids = self._requested_employee_ids(query.filters)
        if employee_ids is not None:
            order_predicates.append(AnalyticsRecord.employee_id.in_(employee_ids))
            quota_predicates.append(AnalyticsRecord.employee_id.in_(employee_ids))
        elif not scope.organization:
            allowed_ids = [int(value) for value in scope.employee_ids]
            order_predicates.append(AnalyticsRecord.employee_id.in_(allowed_ids))
            quota_predicates.append(AnalyticsRecord.employee_id.in_(allowed_ids))

        order_filter = and_(*order_predicates)
        if "quota_completion" in query.metric_ids:
            predicates = or_(order_filter, and_(*quota_predicates))
        else:
            predicates = order_filter
        statement = select(
            AnalyticsRecord.record_type, AnalyticsRecord.record_id,
            AnalyticsRecord.order_id, AnalyticsRecord.employee_id,
            AnalyticsRecord.order_date, AnalyticsRecord.status, AnalyticsRecord.subtotal,
            AnalyticsRecord.period_start, AnalyticsRecord.period_end,
            AnalyticsRecord.quota_amount, AnalyticsRecord.source_line,
            AnalyticsRecord.source_sha256,
        ).where(predicates).order_by(
            AnalyticsRecord.record_type, AnalyticsRecord.source_line, AnalyticsRecord.record_id,
        )
        with authorized_connection(self.database, scope, snapshot_id) as connection:
            records = [dict(row) for row in connection.execute(statement).mappings().all()]
        return self._serialize_rows(records)

    def _normalize_rows(
        self, rows: list[dict[str, Any]], query: DataQuery, scope: AuthorizedScope,
        snapshot: DataSnapshot, requested_employee_ids: list[int] | None,
    ) -> list[dict[str, Any]]:
        normalized = []
        for row in rows:
            item = {
                ("employee_id" if key == "employee" else key):
                value.isoformat() if isinstance(value, (datetime, date))
                else str(value) if isinstance(value, Decimal)
                else value
                for key, value in row.items()
            }
            self._set_metric_status(item, query.metric_ids)
            normalized.append(item)
        if "employee_id" not in query.dimensions:
            return normalized

        present_employee_ids = {
            str(row["employee_id"]) for row in normalized if row.get("employee_id") is not None
        }
        visible_employee_ids = {
            str(value) for value in (
                requested_employee_ids if requested_employee_ids is not None else scope.employee_ids
            )
        }
        for employee_id in sorted(visible_employee_ids - present_employee_ids):
            item = {"employee_id": int(employee_id)}
            for metric_id in query.metric_ids:
                item[metric_id] = 0 if metric_id in {"sales_amount", "order_count"} else None
            self._set_metric_status(item, query.metric_ids)
            normalized.append(item)
        has_unassigned_group = any(row.get("employee_id") is None for row in normalized)
        if (scope.organization and snapshot.quality.get("unassigned_order_count")
                and ("sales_amount" in query.metric_ids or "order_count" in query.metric_ids)
                and requested_employee_ids is None and not has_unassigned_group):
            raise ValueError("组织查询没有返回真实未分配订单分组")
        return sorted(normalized, key=lambda row: (
            row.get("employee_id") is None, row.get("employee_id") or 0,
        ))[:query.limit]

    @staticmethod
    def _set_metric_status(row: dict[str, Any], metric_ids: list[str]) -> None:
        statuses = {}
        for metric_id in metric_ids:
            value = row.get(metric_id)
            if metric_id == "sales_amount" and value is None:
                row[metric_id] = "0"
                statuses[metric_id] = "ok"
            elif metric_id == "order_count" and value is None:
                row[metric_id] = 0
                statuses[metric_id] = "ok"
            elif metric_id == "average_order_value" and value is None:
                statuses[metric_id] = "no_orders"
            elif metric_id == "quota_completion" and value is None:
                statuses[metric_id] = "missing_target"
            else:
                statuses[metric_id] = "ok"
        row["metric_status"] = statuses

    def _write_sync_evidence(
        self, run_dir: Path, source_snapshot: DataSnapshot, result: DataSnapshot,
        task_id: str, run_id: str, stage_schema: str, analytics_schema: str,
        quality: dict[str, Any], dbt_output: dict[str, Any], load_info: Any,
    ) -> None:
        report = {
            "milestone": "DC-04",
            "task_id": task_id,
            "run_id": run_id,
            "source_id": self._source_id,
            "source_snapshot_id": source_snapshot.snapshot_id,
            "source_revision": source_snapshot.source_revisions[self._source_id],
            "published_snapshot_id": result.snapshot_id,
            "source_record_count": source_snapshot.record_count,
            "schema_sha256": result.schema_sha256,
            "content_sha256": result.content_sha256,
            "dlt": {"version": dlt.__version__, "stage_schema": stage_schema,
                    "load_info_type": type(load_info).__name__},
            "dbt": dbt_output,
            "quality": quality,
            "state": result.state,
        }
        (run_dir / "sync-evidence.json").write_text(
            json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8",
        )

    def _write_query_evidence(
        self, query: DataQuery, context: CallContext, snapshot: DataSnapshot,
        sql: str, rows: list[dict[str, Any]], scope: AuthorizedScope,
    ) -> None:
        path = self.artifact_root / f"dc04-query-{context.request_id}"
        path.mkdir(parents=True, exist_ok=True)
        report = {
            "milestone": "DC-04",
            "request_id": context.request_id,
            "source_id": self._source_id,
            "snapshot_id": snapshot.snapshot_id,
            "source_revision": snapshot.source_revisions[self._source_id],
            "policy_version": scope.policy_version,
            "scope_fingerprint": scope.fingerprint,
            "organization": scope.organization,
            "authorized_employee_count": len(scope.employee_ids),
            "authorized_employee_ids": scope.employee_ids,
            "question": query.question,
            "metric_ids": query.metric_ids,
            "dimensions": query.dimensions,
            "start_date": query.start_date.isoformat(),
            "end_date_exclusive": query.end_date.isoformat(),
            "filters": query.filters,
            "limit": query.limit,
            "sql": sql,
            "row_count": len(rows),
            "aggregate_sha256": hashlib.sha256(json.dumps(
                rows, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
            ).encode()).hexdigest(),
            "currency": None,
        }
        (path / "query-evidence.json").write_text(
            json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8",
        )

    def _stage_published_records(
        self, snapshot_id: str, source_snapshot: DataSnapshot, task_id: str, run_id: str,
        orders: list[dict[str, Any]], quotas: list[dict[str, Any]], quality: dict[str, Any],
    ) -> None:
        revision = source_snapshot.source_revisions[self._source_id]
        with Session(self.database) as session, session.begin():
            prior_snapshot = session.get(AnalyticsSnapshot, snapshot_id)
            if prior_snapshot is not None:
                if prior_snapshot.content_sha256 != quality["content_sha256"]:
                    raise ValueError("已存在快照内容与当前来源冲突")
                return
            session.add(AnalyticsSnapshot(
                snapshot_id=snapshot_id, source_id=self._source_id,
                source_snapshot_id=source_snapshot.snapshot_id, source_revision=revision,
                state="staged", record_count=len(orders) + len(quotas),
                schema_sha256=self._schema_digest, content_sha256=quality["content_sha256"],
                created_at=source_snapshot.created_at, published_at=None,
                task_id=task_id, run_id=run_id,
                quality=quality,
            ))
            session.flush()
            for row in orders + quotas:
                session.add(AnalyticsRecord(
                    snapshot_id=snapshot_id, source_id=self._source_id,
                    source_revision=revision, record_type=row["record_type"],
                    record_id=row["record_id"], order_id=row.get("order_id"),
                    employee_id=row["employee_id"], order_date=row.get("order_date"),
                    status=row.get("status"), subtotal=row.get("subtotal"),
                    period_start=row.get("period_start"), period_end=row.get("period_end"),
                    quota_amount=row.get("quota_amount"), source_line=row["source_line"],
                    source_sha256=row["source_sha256"],
                ))

    async def _run_dbt(
        self, run_dir: Path, snapshot_id: str, analytics_schema: str, source_snapshot: DataSnapshot,
    ) -> dict[str, Any]:
        profile_dir = run_dir / "dbt-profile"
        profile_dir.mkdir(parents=True, exist_ok=True)
        profile_path = profile_dir / "profiles.yml"
        profile_path.write_text(
            "data_center_analytics:\n"
            "  target: local\n"
            "  outputs:\n"
            "    local:\n"
            "      type: postgres\n"
            "      host: \"{{ env_var('DC_ANALYTICS_DB_HOST') }}\"\n"
            "      port: \"{{ env_var('DC_ANALYTICS_DB_PORT') | int }}\"\n"
            "      user: \"{{ env_var('DC_ANALYTICS_DB_USER') }}\"\n"
            "      password: \"{{ env_var('DC_ANALYTICS_DB_PASSWORD') }}\"\n"
            "      dbname: \"{{ env_var('DC_ANALYTICS_DB_NAME') }}\"\n"
            "      schema: \"{{ env_var('DC_ANALYTICS_DB_SCHEMA') }}\"\n"
            "      threads: 1\n",
            encoding="utf-8",
        )
        url = self.database.url
        if not all((url.host, url.database, url.username)):
            raise ValueError("dbt PostgreSQL 连接缺少 host、database 或 user")
        subprocess_env = os.environ.copy()
        subprocess_env.update({
            "DC_ANALYTICS_DB_HOST": url.host,
            "DC_ANALYTICS_DB_PORT": str(url.port or 5432),
            "DC_ANALYTICS_DB_USER": url.username,
            "DC_ANALYTICS_DB_PASSWORD": url.password or "",
            "DC_ANALYTICS_DB_NAME": url.database,
            "DC_ANALYTICS_DB_SCHEMA": analytics_schema,
            "DC_ANALYTICS_SOURCE_SCHEMA": self.database_schema,
        })
        project_dir = Path(__file__).parent / "dbt"
        target_dir = run_dir / "dbt-target"
        log_dir = run_dir / "dbt-log"
        command = [
            str(Path(sys.executable).with_name("dbt")), "build",
            "--project-dir", str(project_dir), "--profiles-dir", str(profile_dir),
            "--target-path", str(target_dir), "--log-path", str(log_dir),
            "--vars", json.dumps({"snapshot_id": snapshot_id}),
        ]
        process = await asyncio.create_subprocess_exec(
            *command, cwd=project_dir, env=subprocess_env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        try:
            output_bytes, _ = await process.communicate()
        except asyncio.CancelledError:
            process.terminate()
            await process.wait()
            raise
        output = output_bytes.decode(errors="replace")
        (run_dir / "dbt-build.log").write_text(output, encoding="utf-8")
        if process.returncode != 0:
            raise RuntimeError(f"dbt build 失败，退出码 {process.returncode}，详见 {run_dir / 'dbt-build.log'}")
        return {
            "command": command,
            "returncode": process.returncode,
            "source_snapshot_id": source_snapshot.snapshot_id,
            "analytics_schema": analytics_schema,
            "log_path": str(run_dir / "dbt-build.log"),
        }

    @staticmethod
    def _validate_source_page(page: SourcePage, source_snapshot: DataSnapshot) -> None:
        if (page.snapshot.snapshot_id != source_snapshot.snapshot_id or page.mode != "snapshot"
                or page.deleted_ids):
            raise ValueError("来源分页与固定完整快照不一致")

    def _normalize_source_rows(
        self, rows: list[dict[str, Any]], previous_key: tuple[str, str] | None,
    ) -> list[dict[str, Any]]:
        normalized = []
        last_key = previous_key
        for raw in rows:
            record_type = raw.get("record_type")
            if record_type == "order":
                expected = {
                    "record_type", "record_id", "order_id", "employee_id", "order_date", "status",
                    "subtotal", "source_line", "source_sha256",
                }
                if set(raw) != expected:
                    raise ValueError("AdventureWorks 订单分页字段与登记结构不一致")
                row = {
                    **raw,
                    "order_id": self._positive_int(raw["order_id"], "order_id"),
                    "employee_id": self._nullable_positive_int(raw["employee_id"], "employee_id"),
                    "order_date": self._parse_datetime(raw["order_date"], "order_date"),
                    "status": self._nonnegative_int(raw["status"], "status"),
                    "subtotal": self._positive_decimal(raw["subtotal"], "subtotal", allow_zero=True),
                }
            elif record_type == "quota":
                expected = {
                    "record_type", "record_id", "employee_id", "period_start", "period_end",
                    "quota_amount", "source_line", "source_sha256",
                }
                if set(raw) != expected:
                    raise ValueError("AdventureWorks quota 分页字段与登记结构不一致")
                row = {
                    **raw,
                    "employee_id": self._positive_int(raw["employee_id"], "employee_id"),
                    "period_start": self._parse_date(raw["period_start"], "period_start"),
                    "period_end": self._parse_date(raw["period_end"], "period_end"),
                    "quota_amount": self._positive_decimal(raw["quota_amount"], "quota_amount"),
                }
                if row["period_end"] <= row["period_start"]:
                    raise ValueError("目标期间结束日期必须晚于开始日期")
            else:
                raise ValueError("AdventureWorks 来源包含未知记录类型")
            row["record_id"] = self._nonempty_string(raw["record_id"], "record_id")
            row["source_line"] = self._positive_int(raw["source_line"], "source_line")
            row["source_sha256"] = self._sha256(raw["source_sha256"])
            key = self._row_key(row)
            if last_key is not None and key <= last_key:
                raise ValueError("来源记录没有按类型与编号稳定递增")
            last_key = key
            normalized.append(row)
        return normalized

    @staticmethod
    def _row_key(row: dict[str, Any]) -> tuple[str, str]:
        return row["record_type"], row["record_id"]

    @staticmethod
    def _nonempty_string(value: Any, name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} 必须是非空文本")
        return value

    @staticmethod
    def _positive_int(value: Any, name: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{name} 必须是正整数")
        return value

    @staticmethod
    def _nullable_positive_int(value: Any, name: str) -> int | None:
        if value is None:
            return None
        return AnalyticsEngine._positive_int(value, name)

    @staticmethod
    def _nonnegative_int(value: Any, name: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} 必须是非负整数")
        return value

    @staticmethod
    def _positive_decimal(value: Any, name: str, *, allow_zero: bool = False) -> Decimal:
        if not isinstance(value, (str, Decimal)):
            raise TypeError(f"{name} 必须使用 Decimal 字符串")
        amount = Decimal(value)
        if not amount.is_finite() or amount < 0 or (amount == 0 and not allow_zero):
            raise ValueError(f"{name} 金额无效")
        return amount

    @staticmethod
    def _parse_datetime(value: Any, name: str) -> datetime:
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str):
            parsed = datetime.fromisoformat(value)
        else:
            raise TypeError(f"{name} 必须是 ISO 日期时间")
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(UTC).replace(tzinfo=None)
        return parsed

    @staticmethod
    def _parse_date(value: Any, name: str) -> date:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            return date.fromisoformat(value)
        raise ValueError(f"{name} 必须是 ISO 日期")

    @staticmethod
    def _sha256(value: Any) -> str:
        if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
            raise ValueError("source_sha256 必须是小写 SHA-256")
        return value
