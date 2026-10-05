import asyncio
import json
from copy import deepcopy
from decimal import ROUND_HALF_UP, Decimal, localcontext
from time import monotonic

from data_center_protocol import (
    DataEvidenceRef,
    DataQuery,
    DataQuestion,
    DataResult,
    DocumentIngest,
    ModelInvocation,
)
from integration_protocol import OperationResult, ProtocolModel, ResourceRef
from opentelemetry import trace
from pydantic import Field, create_model
from sqlalchemy import text

from data_center.documents.gateway import ModelStepPending
from data_center.ports import ModelBudgetLimited, SourceUnavailable
from data_center.store import digest


class GroundedAnswer(ProtocolModel):
    answer: str = Field(min_length=1)
    citations: list[int] = Field(min_length=1)
    unknowns: list[str] = Field(default_factory=list)


QuerySelection = create_model("QuerySelection", __base__=ProtocolModel, **{
    name: (field.annotation, deepcopy(field))
    for name, field in DataQuery.model_fields.items()
    if name not in {"question", "snapshot_id", "retrieval_mode"}
})


class QueryPlan(ProtocolModel):
    queries: list[QuerySelection] = Field(max_length=3)
    reason: str = Field(min_length=1)

    def to_queries(self, question: DataQuestion) -> list[DataQuery]:
        if len({selection.intent for selection in self.queries}) != len(self.queries):
            raise ValueError("查询规划重复安排同类查询")
        queries = []
        has_document_query = False
        for selection in self.queries:
            if not set(selection.source_ids).issubset(question.source_ids):
                raise PermissionError("查询规划超出用户选择的来源")
            documents = selection.intent == "documents"
            has_document_query |= documents
            required_sources = list(dict.fromkeys([
                *selection.required_source_ids,
                *(question.required_source_ids if documents else []),
            ]))
            queries.append(DataQuery.model_validate({
                **selection.model_dump(), "question": question.question,
                "required_source_ids": required_sources,
                "retrieval_mode": question.retrieval_mode if documents else "hybrid",
                "snapshot_id": question.snapshot_id if documents else None,
            }))
        if self.queries and question.required_source_ids and not has_document_query:
            raise ValueError("查询规划没有安排必需来源的文档查询")
        return queries


class TaskStopped(RuntimeError):
    pass


class ModelPending(RuntimeError):
    pass


class DataRuntime:
    def __init__(self, store, ports, documents, analytics, *, model="deepseek-flash", observer=None):
        self.store = store
        self.ports = ports
        self.documents = documents
        self.analytics = analytics
        self.model = model
        self.tracer = trace.get_tracer("data-center")
        self.observer = observer

    async def submit(self, operation, payload, context):
        if operation in {"query", "build_graph"}:
            parsed = DataQuery.model_validate_json(json.dumps(payload))
            source_ids = parsed.source_ids
            action = "read"
        elif operation == "ask":
            parsed = DataQuestion.model_validate_json(json.dumps(payload))
            source_ids, action = parsed.source_ids, "read"
        elif operation == "ingest":
            parsed = DocumentIngest.model_validate_json(json.dumps(payload))
            source_ids = sorted({document.source_id for document in parsed.documents})
            action = "ingest"
        elif operation == "sync":
            if set(payload) != {"source_id"} or not isinstance(payload["source_id"], str):
                raise ValueError("来源同步只接收登记来源编号")
            source_ids, action = [payload["source_id"]], "sync"
        else:
            raise ValueError("数据任务操作没有登记")
        scope = await self.ports.authorize(context, source_ids, action)
        task = await asyncio.to_thread(self.store.create, operation, payload, context)
        if "authorization" not in task.checkpoints:
            await asyncio.to_thread(self.store.checkpoint, task.task_id, "authorization",
                                    scope.model_dump(mode="json"))
        await self.ports.bind(task.task_id, task.run_id, operation, source_ids, payload, context)
        return task

    async def _active(self, task, context):
        current = await asyncio.to_thread(self.store.get, task.task_id, context.actor_id)
        if current.cancel_requested or current.status == "cancelled":
            raise TaskStopped("数据任务已经取消")
        await self._validate_saved_scope(current, context)
        return current

    async def _validate_saved_scope(self, task, context):
        original = task.checkpoints.get("authorization")
        if original is None:
            raise PermissionError("持久任务没有保存原始授权范围")
        action = "sync" if task.operation == "sync" else "ingest" if task.operation == "ingest" else "read"
        source_ids = (task.payload["source_ids"] if task.operation in {"query", "ask", "build_graph"}
                      else original["source_ids"])
        current = await self.ports.authorize(context, source_ids, action)
        if task.result is None and original["policy_version"] != current.policy_version:
            raise PermissionError("未完成任务的来源或授权说明已经变化，需要新的请求")
        if (original["customer_id"] != current.customer_id
                or original["organization"] and not current.organization
                or not set(original["employee_ids"]).issubset(current.employee_ids)):
            raise PermissionError("当前权限不能读取任务原始业务范围")
        if task.result is not None and task.operation in {"query", "ask"}:
            for value in task.result["data"].get("evidence", []):
                ref = DataEvidenceRef.model_validate_json(json.dumps(value))
                if ref.kind == "document":
                    await self.documents.read_evidence(ref, context)
                elif ref.kind == "metric":
                    await self.analytics.read_evidence(ref, context)
                else:
                    row = await self.ports.read_current(ref.source_id, ref.record_id, context)
                    if digest(row) != ref.content_sha256:
                        raise ValueError("当前业务记录与引用摘要不一致")

    async def cancel(self, task_id, context):
        await asyncio.to_thread(self.store.get, task_id, context.actor_id)
        await self.ports.cancel(task_id, context)
        return await asyncio.to_thread(
            self.store.update, task_id, cancel_requested=True, status="cancelled")

    async def show(self, task_id, context):
        task = await asyncio.to_thread(self.store.get, task_id, context.actor_id)
        source_ids = ([task.payload["source_id"]] if task.operation == "sync" else
                      sorted({value["source_id"] for value in task.payload["documents"]})
                      if task.operation == "ingest" else task.payload["source_ids"])
        action = "sync" if task.operation == "sync" else "ingest" if task.operation == "ingest" else "read"
        await self.ports.authorize(context, source_ids, action)
        await self._validate_saved_scope(task, context)
        return task

    @staticmethod
    def _stored_outcome(task):
        if task.status == "cancelled":
            return OperationResult(data={"task_id": task.task_id},
                                   run_id=task.run_id, status="cancelled")
        if task.result is not None:
            return OperationResult.model_validate_json(json.dumps(task.result))
        data = {"task_id": task.task_id}
        if task.error is not None:
            data["error"] = task.error
        return OperationResult(data=data, run_id=task.run_id, status=task.status)

    def _observe_terminal(self, task, sources):
        if self.observer is None:
            return
        if task.status == "cancelled":
            self.observer.stop(task.run_id, task.operation, sources, cancelled=True)
        elif task.status in {"completed", "partial"} and task.result is not None:
            saved = self._stored_outcome(task)
            snapshots = saved.data.get("snapshots", [])
            if "snapshot" in saved.data:
                snapshots = [saved.data["snapshot"]]
            self.observer.complete(task.run_id, task.operation, sources,
                                   [value["snapshot_id"] for value in snapshots])
        elif task.status == "failed":
            self.observer.fail(task.run_id, task.operation, sources)
        else:
            self.observer.stop(task.run_id, task.operation, sources)

    async def run(self, task_id, context):
        task = await self.show(task_id, context)
        if task.result is not None:
            return self._stored_outcome(task)
        with self.store.database.connect() as connection:
            key = f"dc-worker:{task.task_id}"
            acquired = connection.scalar(text("SELECT pg_try_advisory_lock(hashtextextended(:key,0))"), {"key": key})
            if not acquired:
                current = await asyncio.to_thread(self.store.get, task.task_id, context.actor_id)
                if current.status in {"queued", "running"}:
                    return OperationResult(
                        data={"task_id": current.task_id}, run_id=current.run_id, status="running")
                return self._stored_outcome(current)
            run_started = False
            sources = []
            try:
                try:
                    task = await self._active(task, context)
                    task = await asyncio.to_thread(self.store.get, task.task_id, context.actor_id)
                    if task.result is not None:
                        return self._stored_outcome(task)
                    task = await asyncio.to_thread(
                        self.store.update, task.task_id, status="running", error=None)
                    if task.status != "running":
                        return self._stored_outcome(task)
                    started = monotonic()
                    sources = task.checkpoints["authorization"]["source_ids"]
                    span = (self.observer.running(task.task_id, task.run_id, task.operation, sources)
                            if self.observer is not None else self.tracer.start_as_current_span("data-center.run", attributes={
                        "task.id": task.task_id, "run.id": task.run_id, "operation": task.operation,
                    }))
                    with span:
                        run_started = True
                        result = await self._execute(task, context)
                        usage = await self.ports.usage(task.task_id, context)
                        elapsed = monotonic() - started
                        if isinstance(result, DataResult):
                            result = result.model_copy(update={
                                "spent_cny": Decimal(usage["spent_cny"]),
                                "reserved_cny": Decimal(usage["reserved_cny"]),
                                "call_ids": list(dict.fromkeys([*result.call_ids,
                                                               *(item["request_id"] for item in usage["requests"])])),
                                "metadata": {**result.metadata, "requests": usage["requests"],
                                             "elapsed_seconds": elapsed}})
                    if isinstance(result, DataResult):
                        result = OperationResult(data=result.model_dump(mode="json"), status=result.status,
                                                 run_id=task.run_id, resources=[ResourceRef(
                                                     module_id="data_center", resource_type="result",
                                                     resource_id=task.task_id, version="1",
                                                     sha256=digest(result.model_dump(mode="json")))],
                                                 costs={"spent_cny": str(result.spent_cny),
                                                        "reserved_cny": str(result.reserved_cny)})
                    else:
                        result = result.model_copy(update={"costs": {"spent_cny": usage["spent_cny"],
                                                                      "reserved_cny": usage["reserved_cny"]},
                                                           "data": {**result.data, "usage": usage,
                                                                    "elapsed_seconds": elapsed}})
                    task = await asyncio.to_thread(
                        self.store.update, task.task_id,
                        result=result.model_dump(mode="json"), status=result.status)
                except TaskStopped as error:
                    task = await asyncio.to_thread(
                        self.store.update, task.task_id, status="cancelled", error=str(error))
                    if run_started:
                        self._observe_terminal(task, sources)
                    return self._stored_outcome(task)
                except SourceUnavailable as error:
                    result = OperationResult(data={"task_id": task.task_id, "unknowns": [str(error)]},
                                             run_id=task.run_id, status="DATA_BLOCKED")
                    task = await asyncio.to_thread(
                        self.store.update, task.task_id, status=result.status,
                        result=result.model_dump(mode="json"))
                    if run_started:
                        self._observe_terminal(task, sources)
                    return self._stored_outcome(task)
                except (ModelPending, ModelStepPending) as error:
                    usage = await self.ports.usage(task.task_id, context)
                    result = OperationResult(data={"task_id": task.task_id, "unknowns": [str(error)],
                                                   "usage": usage},
                                             run_id=task.run_id, status="unknown",
                                             costs={"spent_cny": usage["spent_cny"],
                                                    "reserved_cny": usage["reserved_cny"]})
                    task = await asyncio.to_thread(
                        self.store.update, task.task_id, status="unknown", error=str(error),
                        result=result.model_dump(mode="json"))
                    if run_started:
                        self._observe_terminal(task, sources)
                    return self._stored_outcome(task)
                except ModelBudgetLimited as error:
                    result = OperationResult(data={"task_id": task.task_id, "unknowns": [str(error)]},
                                             run_id=task.run_id, status="budget_exhausted")
                    task = await asyncio.to_thread(
                        self.store.update, task.task_id, status=result.status,
                        result=result.model_dump(mode="json"))
                    if run_started:
                        self._observe_terminal(task, sources)
                    return self._stored_outcome(task)
                except Exception as error:
                    task = await asyncio.to_thread(
                        self.store.update, task.task_id, status="failed", error=str(error))
                    if run_started:
                        self._observe_terminal(task, sources)
                    if task.status != "failed":
                        return self._stored_outcome(task)
                    raise
                if run_started:
                    self._observe_terminal(task, sources)
                return self._stored_outcome(task)
            finally:
                connection.execute(text("SELECT pg_advisory_unlock(hashtextextended(:key,0))"), {"key": key})

    async def _execute(self, task, context):
        if task.operation == "ingest":
            payload = DocumentIngest.model_validate_json(json.dumps(task.payload))
            snapshot = await self.documents.ingest(payload, context, task_id=task.task_id, run_id=task.run_id)
            if snapshot.state != "published":
                raise ValueError("文档发布没有提供已发布快照")
            return OperationResult(status="completed", run_id=task.run_id,
                                   data={"task_id": task.task_id, "snapshot": snapshot.model_dump(mode="json")},
                                   resources=[ResourceRef(module_id="data_center", resource_type="snapshot",
                                                          resource_id=snapshot.snapshot_id, version="1",
                                                          sha256=snapshot.content_sha256)])
        if task.operation == "sync":
            snapshot = await self.analytics.sync(task.payload["source_id"], context,
                                                  task_id=task.task_id, run_id=task.run_id)
            if snapshot.state != "published":
                raise ValueError("来源同步没有提供已发布快照")
            return OperationResult(status="completed", run_id=task.run_id,
                                   data={"task_id": task.task_id, "snapshot": snapshot.model_dump(mode="json")},
                                   resources=[ResourceRef(module_id="data_center", resource_type="snapshot",
                                                          resource_id=snapshot.snapshot_id, version="1",
                                                          sha256=snapshot.content_sha256)])
        if task.operation == "ask":
            queries = await self._plan_query(task, context)
            if not queries:
                return DataResult(result_id=task.task_id, status="DATA_BLOCKED",
                                  unknowns=[task.checkpoints.get("query_plan_reason", "授权来源没有提供所需能力")])
        else:
            queries = [DataQuery.model_validate_json(json.dumps(task.payload))]
        if task.operation == "build_graph":
            details = await self.documents.build_graph(queries[0], context, task_id=task.task_id, run_id=task.run_id)
            return OperationResult(status="completed" if details["state"] == "published" else details["state"],
                                   run_id=task.run_id, data={"task_id": task.task_id, **details})
        checkpoint = task.checkpoints.get("evidence")
        if checkpoint is None:
            parts = []
            for index, query in enumerate(queries):
                await self._active(task, context)
                key = f"evidence-{index}"
                saved = task.checkpoints.get(key)
                if saved is None:
                    part = await self._collect(task, query, context)
                    if part.status not in {"completed", "partial", "DATA_BLOCKED"}:
                        return part
                    await asyncio.to_thread(self.store.checkpoint, task.task_id, key, part.model_dump(mode="json"))
                else:
                    part = DataResult.model_validate_json(json.dumps(saved))
                parts.append(part)
            base = self._merge_results(task.task_id, parts)
            if base.status not in {"completed", "partial"}:
                return base
            await asyncio.to_thread(self.store.checkpoint, task.task_id, "evidence", base.model_dump(mode="json"))
        else:
            base = DataResult.model_validate_json(json.dumps(checkpoint))
        await self._active(task, context)
        # 每次回读引用，权限及来源撤回会在模型调用之前终止。
        for ref in base.evidence:
            if ref.kind == "document":
                await self.documents.read_evidence(ref, context)
            elif ref.kind == "metric":
                await self.analytics.read_evidence(ref, context)
            else:
                row = await self.ports.read_current(ref.source_id, ref.record_id, context)
                if digest(row) != ref.content_sha256:
                    raise ValueError("当前业务记录与引用摘要不一致")
        for query in queries:
            await self.ports.authorize(context, query.source_ids, "read")
        required_sources = list(dict.fromkeys(
            source_id for query in queries for source_id in query.required_source_ids
        ))
        missing_sources = sorted(set(required_sources) - {
            ref.source_id for ref in base.evidence if ref.kind == "document"
        })
        if missing_sources:
            return DataResult(
                result_id=task.task_id, status="DATA_BLOCKED", call_ids=base.call_ids,
                unknowns=["检索结果缺少必需来源引用: " + ", ".join(missing_sources)],
                metadata={**base.metadata, "required_source_ids": required_sources,
                          "missing_source_ids": missing_sources},
            )
        return await self._answer(task, queries[0], base, context,
                                  required_source_ids=required_sources)

    @staticmethod
    def _merge_results(task_id, parts):
        if len(parts) == 1:
            return parts[0].model_copy(update={"result_id": task_id})
        rows, evidence, snapshots, versions = [], [], {}, {}
        unknowns, calls, metadata = [], [], []
        for part in parts:
            offset = len(evidence)
            for row in part.rows:
                row = dict(row)
                if "citation" in row:
                    row["citation"] += offset
                rows.append(row)
            evidence.extend(part.evidence)
            for snapshot in part.snapshots:
                previous = snapshots.get(snapshot.snapshot_id)
                if previous is not None and previous != snapshot:
                    raise ValueError("复合查询的相同快照编号内容冲突")
                snapshots[snapshot.snapshot_id] = snapshot
            for name, version in part.metric_versions.items():
                if name in versions and versions[name] != version:
                    raise ValueError("复合查询的指标版本冲突")
                versions[name] = version
            unknowns.extend(part.unknowns)
            calls.extend(part.call_ids)
            metadata.append(part.metadata)
        unknowns = list(dict.fromkeys(unknowns))
        state = "DATA_BLOCKED" if not evidence else "partial" if unknowns or any(
            part.status != "completed" for part in parts) else "completed"
        return DataResult(result_id=task_id, status=state, rows=rows, evidence=evidence,
                          snapshots=list(snapshots.values()), metric_versions=versions,
                          unknowns=unknowns, call_ids=list(dict.fromkeys(calls)),
                          metadata={"queries": metadata})

    async def _plan_query(self, task, context):
        if "query_plan" in task.checkpoints:
            value = task.checkpoints["query_plan"]
            values = [] if value is None else [value] if isinstance(value, dict) else value
            return [DataQuery.model_validate_json(json.dumps(item)) for item in values]
        question = DataQuestion.model_validate_json(json.dumps(task.payload))
        catalog = await self.ports.catalog(context)
        available = [item for item in catalog["items"] if item["source_id"] in question.source_ids]
        definitions = [item.model_dump(mode="json") for item in self.analytics.metric_definitions()]
        instruction = {"question": question.question, "sources": available, "metrics": definitions,
                       "plan_schema": QueryPlan.model_json_schema(), "retrieval_mode": question.retrieval_mode,
                       "snapshot_id": question.snapshot_id,
                       "authorization": task.checkpoints["authorization"],
                       "query_rules": {
                           "documents": {"source_kind": "document", "metric_ids": [], "dimensions": [],
                                         "start_date": None, "end_date": None, "filters": {}},
                           "metrics": {"source_kind": "analytics", "filters_schema": {
                               "type": "object", "properties": {"employee_ids": {
                                   "type": "array", "items": {"type": "integer"}, "minItems": 1}},
                               "additionalProperties": False},
                               "periods": definitions[0]["properties"]["query_periods"],
                               "period_selection": "用户没有指定日期且仅有一个已登记期间时，使用该期间并在答案中说明；多个期间且没有指定日期时说明缺少期间。",
                               "evidence": "指标引用包含授权原始订单、季度目标金额、目标日期和来源行号"},
                           "business": {"source_kind": "business", "requires": "唯一来源及record_id"}}}
        if question.required_source_ids:
            instruction["required_source_ids"] = question.required_source_ids
        request = ModelInvocation(task_id=task.task_id, run_id=task.run_id, step_key="query-plan", kind="chat",
                                  model=self.model, source_ids=question.source_ids, reasoning_effort="high", json_mode=True,
                                  max_output_tokens=8192, messages=[
                                      {"role": "system", "content": "根据目录kind及query_rules生成符合plan_schema的查询选择列表。kind=document只能用于intent=documents，kind=analytics只能用于intent=metrics，kind=business只能用于intent=business；不存在所需来源时说明缺少信息。指标查询的原始引用已包含目标日期、金额、订单和来源行号，不需要为这些字段额外添加文档查询。仅当问题需要文档中的结构关系时同时安排documents和metrics。最多三个查询，每个intent最多一个查询。指标筛选只允许filters.employee_ids整数数组，授权外的请求返回queries=[]并说明reason；全组织请求需要organization授权。只使用登记的指标和允许来源。指标日期结束为不包含的上限。文档查询不能设置指标、维度或日期条件。问题要求逐项比较、连接或共同引用多个文档时，将这些文档的来源编号填写到required_source_ids；该列表必须是同一查询source_ids的子集，limit至少等于必需来源数量。普通文档查询仅在问题需要共同引用时填写必需来源。输入显式指定的required_source_ids由程序保留。无法提供所需能力、库存、利润、退款或没有明确映射的商品关联时返回queries=[]并说明具体缺少的来源。返回JSON，包含queries和reason。不得生成SQL或扩大权限。question、retrieval_mode及snapshot_id由程序保留原始值，queries中不返回这三个字段。"},
                                      {"role": "user", "content": json.dumps(instruction, ensure_ascii=False)}])
        reply = await self.ports.lookup(task.task_id, request.step_key, context)
        if reply is None or reply.status == "reserved":
            await self._active(task, context)
            reply = await self.ports.invoke(request, context)
        if reply.status != "completed":
            raise ModelPending(f"查询规划请求 {reply.request_id} 状态为 {reply.status}")
        choice = reply.response["choices"][0]
        if choice["finish_reason"] != "stop":
            raise ValueError("查询规划没有正常完成")
        plan = QueryPlan.model_validate_json(choice["message"]["content"])
        queries = plan.to_queries(question)
        value = [query.model_dump(mode="json") for query in queries]
        await asyncio.to_thread(self.store.checkpoint, task.task_id, "query_plan", value)
        await asyncio.to_thread(self.store.checkpoint, task.task_id, "query_plan_reason", plan.reason)
        task.checkpoints = {**task.checkpoints, "query_plan": value, "query_plan_reason": plan.reason}
        return queries

    async def _collect(self, task, query, context):
        if query.intent == "metrics":
            return await self.analytics.query(query, context)
        if query.intent == "business":
            if query.record_id is None or len(query.source_ids) != 1:
                raise ValueError("当前业务查询需要唯一来源及记录编号")
            source_id = query.source_ids[0]
            row = await self.ports.read_current(source_id, query.record_id, context)
            snapshot = await self.ports.snapshot(source_id, context)
            ref = DataEvidenceRef(kind="record", source_id=source_id,
                                  snapshot_id=snapshot.snapshot_id, record_id=query.record_id,
                                  source_revision=snapshot.source_revisions[source_id],
                                  source_sha256=snapshot.quality["source_sha256"],
                                  content_sha256=digest(row), locator={"record": row})
            return DataResult(result_id=task.task_id, status="completed", rows=[row],
                              snapshots=[snapshot], evidence=[ref])
        search = await self.documents.search(query, context, task_id=task.task_id, run_id=task.run_id)
        if search.status not in {"completed", "partial"}:
            return DataResult(result_id=task.task_id, status=search.status,
                              unknowns=[str(search.metadata.get("reason", "检索尚未提供最终证据"))],
                              call_ids=search.call_ids, metadata=search.metadata)
        if not search.hits:
            return DataResult(result_id=task.task_id, status="DATA_BLOCKED",
                              unknowns=["允许资料中没有足够的引用片段"], metadata=search.metadata)
        return DataResult(result_id=task.task_id, status="completed", snapshots=search.snapshots,
                          evidence=[hit.chunk.ref for hit in search.hits],
                          rows=[{"text": hit.chunk.text, "citation": index, "metadata": hit.chunk.metadata}
                                for index, hit in enumerate(search.hits, start=1)],
                          call_ids=search.call_ids, metadata=search.metadata)

    async def _answer(self, task, query, base, context, *, required_source_ids=()):
        catalog = await self.ports.catalog(context)
        sources = [item for item in catalog["items"] if item["source_id"] in task.checkpoints["authorization"]["source_ids"]]
        details = []
        record_groups = {}
        document_statistics = {}
        for reference in base.evidence:
            if reference.kind == "document":
                key = (reference.source_id, reference.snapshot_id, reference.record_id)
                if key not in document_statistics:
                    document_statistics[key] = await self.documents.read_document_statistics(reference, context)
            if reference.kind == "metric":
                detail = await self.analytics.read_evidence(reference, context)
                group = {"rows": detail["rows"], "original_quotas": detail["original_quotas"]}
                key = digest(group)
                record_groups[key] = {**group, "metric_inputs": self._metric_inputs(group)}
                details.append({name: value for name, value in {**detail, "records_group": key}.items()
                                if name not in {"rows", "original_quotas"}})
        display_values = []
        for row in base.rows:
            displayed = {}
            for name in ("sales_amount", "average_order_value", "quota_completion"):
                if name in row and row[name] is not None:
                    value = Decimal(str(row[name]))
                    if name == "quota_completion":
                        value *= 100
                    displayed[name] = str(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)) + (
                        "%" if name == "quota_completion" else "")
            if displayed:
                display_values.append({"employee_id": row.get("employee_id"), **displayed})
        evidence = {"question": query.question, "answer_schema": GroundedAnswer.model_json_schema(),
                    "rows": base.rows,
                    "sources": sources, "metric_evidence": details,
                    "document_statistics": list(document_statistics.values()),
                    "source_records": record_groups, "display_values": display_values,
                    "citations": [{"id": index, "reference": ref.model_dump(mode="json")}
                                  for index, ref in enumerate(base.evidence, start=1)],
                    "metric_versions": base.metric_versions, "unknowns": base.unknowns}
        if required_source_ids:
            evidence["required_source_ids"] = list(required_source_ids)
            evidence["citation_requirement"] = "citations必须包含每个required_source_ids来源的文档引用"
        request = ModelInvocation(task_id=task.task_id, run_id=task.run_id, step_key="answer",
                                  kind="chat", model=self.model, source_ids=task.checkpoints["authorization"]["source_ids"],
                                  reasoning_effort="high", json_mode=True, max_output_tokens=16384,
                                  messages=[{"role": "system", "content": (
                                      "根据提供的授权证据回答中文问题。所有事实和数字必须来自证据，不添加价格、库存、政策或来源没有提供的关系。"
                                      "回答须完整覆盖问题中的每项要求，并明确说明支撑关系的来源身份。"
                                      "比较文档来源时列出文件、upstream_source_id、source_revision、license_id及共同关系；自然语言名称与登记编号均可使用。"
                                      "sources.properties.generation是登记的生成记录，包含原始输入资源、输入摘要、输出摘要、生成程序及原清单摘要；"
                                      "按此记录说明生成关系，不能从文本相同推断生成关系。"
                                      "document_statistics包含授权文档的完整已保存转换文本及原件摘要；全文和片段范围按照document_scope与reference区分。"
                                      "worksheets来自摘要核验后的XLSX原件，提供原生单元格坐标、行号和值；涉及表格范围时列出各工作表表头、"
                                      "首个数据行和问题指定行及各自cell range。Excel行号包含表头，使用原生coordinate确定范围。"
                                      "页数使用有page_count的原件统计。字符计数采用normalization_rule所定义的whitespace_collapsed："
                                      "连续Unicode空白统一为单个U+0020 SPACE，保留标点和数字，使用normalized_character_count；"
                                      "完整说明规则并按normalized_text核对文本，不能自行删除空白或过滤其他字符。"
                                      "source_records按records_group关联指标原始记录，字段与行号均可引用；"
                                      "original_quotas.identity_records包含同版本员工与销售人员的原始来源身份及行号。"
                                      "问题明确要求追踪指标来源时，说明员工、销售人员、订单和目标的连接字段、对应来源行号、原始身份与统计条件。"
                                      "普通指标问题按所需指标提供统计期间、公式、来源文件及引用编号，原始明细通过引用按需读取。"
                                      "metric_inputs是程序使用38位Decimal从授权原始记录计算的数值，分子、分母、金额合计与精确除法结果直接采用该字段；"
                                      "source_records用于核查来源身份与统计条件。按employee_id或对应records_group选择适用数值。"
                                      "回答客单价时同时列出销售额与订单数；回答个人或团队目标完成率时同时列出销售额、目标金额合计、比例与百分数，"
                                      "完整说明所选期间及公式，并给出目标记录的日期与来源行号。"
                                      "金额及客单价同时提供原始数值和display_values的两位小数，目标完成率同时提供比例与百分数。币种以登记来源为准。"
                                      "返回符合answer_schema的JSON，answer必须是非空字符串，citations是证据编号的整数数组，"
                                      "unknowns是未解决事项的字符串数组。技术样例必须明确说明其资料性质。缺少答案时说明缺少的信息。"
                                  )},
                                            {"role": "user", "content": json.dumps(evidence, ensure_ascii=False)}])
        reply = await self.ports.lookup(task.task_id, request.step_key, context)
        if reply is None or reply.status == "reserved":
            await self._active(task, context)
            reply = await self.ports.invoke(request, context)
        if reply.status != "completed":
            raise ModelPending(f"模型请求 {reply.request_id} 状态为 {reply.status}，费用预留保留")
        choice = reply.response["choices"][0]
        if choice["finish_reason"] != "stop":
            raise ValueError("最终回答没有正常完成")
        answer = GroundedAnswer.model_validate_json(choice["message"]["content"])
        self._validate_answer_citations(answer, base, required_source_ids)
        unknowns = list(dict.fromkeys([*base.unknowns, *answer.unknowns]))
        return DataResult(result_id=task.task_id, task_id=task.task_id, run_id=task.run_id,
                          status="partial" if unknowns or base.status == "partial" else "completed",
                          answer=answer.answer, rows=base.rows, evidence=base.evidence,
                          snapshots=base.snapshots, metric_versions=base.metric_versions,
                          unknowns=unknowns, call_ids=[*base.call_ids, reply.request_id],
                          spent_cny=base.spent_cny + reply.spent_cny,
                          reserved_cny=base.reserved_cny + reply.reserved_cny,
                          metadata={**base.metadata, "answer_citations": answer.citations,
                                    "source_metadata": sources, "source_metadata_sha256": digest(sources)})

    @staticmethod
    def _validate_answer_citations(answer, base, required_source_ids):
        if any(index < 1 or index > len(base.evidence) for index in answer.citations):
            raise ValueError("模型回答引用了没有提供的证据编号")
        covered = {
            base.evidence[index - 1].source_id for index in answer.citations
            if base.evidence[index - 1].kind == "document"
        }
        missing = sorted(set(required_source_ids) - covered)
        if missing:
            raise ValueError("回答引用缺少必需来源: " + ", ".join(missing))

    @staticmethod
    def _metric_inputs(group):
        orders = [row for row in group["rows"] if row["record_type"] == "order"]
        quotas = group["original_quotas"]
        if len({row["record_id"] for row in orders}) != len(orders):
            raise ValueError("指标原始订单存在重复编号")
        if len({row["record_id"] for row in quotas}) != len(quotas):
            raise ValueError("指标原始目标存在重复编号")
        with localcontext() as context:
            context.prec = 38
            amount = sum((Decimal(row["subtotal"]) for row in orders), Decimal(0))
            count = len(orders)
            values = {"sales_amount": str(amount), "order_count": count,
                      "average_order_value": str(amount / count) if count else None}
            if quotas:
                quota = sum((Decimal(row["quota_amount"]) for row in quotas), Decimal(0))
                values.update(quota_amount=str(quota),
                              quota_completion=str(amount / quota) if quota else None)
            return values
