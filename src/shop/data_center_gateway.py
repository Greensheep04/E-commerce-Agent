import asyncio
from decimal import Decimal
from uuid import UUID

import httpx
from data_center_protocol import ModelReply
from sqlalchemy import select, text

from shop.budget import BudgetService
from shop.data_center_host import content_digest
from shop.data_center_models import DataCenterModelReuse, DataCenterTaskBinding
from shop.paid_models import PaidModels, UnknownRequest
from shop.runtime_models import ProviderRequest


class ShopDataModelGateway:
    def __init__(self, settings, maker, authorization):
        self.maker = maker
        self.authorization = authorization
        self.paid = PaidModels(settings, BudgetService(maker, verification_scope="data_center"))

    async def bind(self, task_id, run_id, operation, source_ids, payload, context):
        UUID(task_id)
        UUID(run_id)
        if operation not in {"query", "ask", "ingest", "sync", "build_graph"}:
            raise ValueError("数据任务操作没有登记")
        action = "sync" if operation == "sync" else "ingest" if operation == "ingest" else "read"
        await self.authorization.authorize(context, source_ids, action)
        digest = content_digest({"operation": operation, "sources": source_ids, "payload": payload})
        with self.maker.begin() as session:
            session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"),
                            {"key": f"data-center-task:{task_id}"})
            prior = session.get(DataCenterTaskBinding, task_id)
            if prior is not None:
                if (prior.actor_id != context.actor_id or prior.run_id != run_id
                        or prior.payload_sha256 != digest or prior.source_ids != source_ids):
                    raise ValueError("相同任务编号的身份、输入或运行编号冲突")
                if prior.cancel_requested:
                    raise PermissionError("数据任务已经取消")
                return
            session.add(DataCenterTaskBinding(task_id=task_id, run_id=run_id,
                                             actor_id=context.actor_id, request_id=context.request_id, operation=operation,
                                             source_ids=source_ids, payload_sha256=digest))

    async def cancel(self, task_id, context):
        self.authorization.authentication.verify(context)
        with self.maker.begin() as session:
            task = session.get(DataCenterTaskBinding, task_id, with_for_update=True)
            if task is None or task.actor_id != context.actor_id:
                raise PermissionError("数据任务不属于当前账号")
            task.cancel_requested = True

    def _binding(self, task_id, context):
        self.authorization.authentication.verify(context)
        with self.maker() as session:
            task = session.get(DataCenterTaskBinding, task_id)
            if task is None or task.actor_id != context.actor_id:
                raise PermissionError("模型请求没有绑定当前账号的数据任务")
            session.expunge(task)
            return task

    @staticmethod
    def _reply(row):
        return ModelReply(request_id=row.id, status=row.status, response=row.response_body,
                          spent_cny=row.charged_cny or Decimal(0),
                          reserved_cny=row.reservation_cny if row.charged_cny is None else Decimal(0))

    async def lookup(self, task_id, step_key, context):
        binding = await asyncio.to_thread(self._binding, task_id, context)
        if binding.cancel_requested:
            raise PermissionError("数据任务已经取消")
        scope = await self.authorization.authorize(context, binding.source_ids, "model")
        with self.maker() as session:
            reused = session.get(DataCenterModelReuse, (task_id, step_key))
            if reused is not None:
                if reused.scope_fingerprint != scope.fingerprint:
                    raise PermissionError("模型复用结果的授权已经改变")
                row = session.get(ProviderRequest, reused.provider_request_id)
                if row.status != "completed" or row.response_body is None:
                    raise ValueError("模型复用记录缺少已完成的原始响应")
                return self._reply(row)
            key = f"data-model:{binding.run_id}:{step_key}"
            acquired = session.scalar(text("SELECT pg_try_advisory_lock(hashtextextended(:key,0))"), {"key": key})
            row = session.scalar(select(ProviderRequest).where(
                ProviderRequest.run_id == binding.run_id, ProviderRequest.step_key == step_key,
            ))
            try:
                if row is None:
                    return None
                if row.price_basis.get("verification_scope") != "data_center":
                    raise ValueError("请求没有归属数据中台费用阶段")
                if acquired and row.status == "sent":
                    self.paid.budget.mark_unknown(row.id, "process_interrupted")
                    session.refresh(row)
                return self._reply(row)
            finally:
                if acquired:
                    session.execute(text("SELECT pg_advisory_unlock(hashtextextended(:key,0))"), {"key": key})

    async def usage(self, task_id, context):
        binding = await asyncio.to_thread(self._binding, task_id, context)
        with self.maker() as session:
            rows = session.scalars(select(ProviderRequest).where(ProviderRequest.run_id == binding.run_id)
                                   .order_by(ProviderRequest.created_at)).all()
            requests = [{"request_id": row.id, "step_key": row.step_key, "status": row.status,
                         "model": row.model, "usage": row.usage,
                         "spent_cny": str(row.charged_cny or Decimal(0)),
                         "reserved_cny": str(row.reservation_cny if row.charged_cny is None else Decimal(0))}
                        for row in rows]
            reused_requests = [{"step_key": reuse.step_key, "request_id": original.id,
                                "original_run_id": original.run_id, "status": original.status,
                                "model": original.model, "usage": original.usage,
                                "original_spent_cny": str(original.charged_cny or Decimal(0))}
                               for reuse, original in session.execute(
                                   select(DataCenterModelReuse, ProviderRequest).join(
                                       ProviderRequest, ProviderRequest.id == DataCenterModelReuse.provider_request_id
                                   ).where(DataCenterModelReuse.task_id == task_id)
                                   .order_by(DataCenterModelReuse.created_at, DataCenterModelReuse.step_key))]
            return {"task_id": task_id, "run_id": binding.run_id, "requests": requests,
                    "reused_requests": reused_requests,
                    "spent_cny": str(sum((row.charged_cny or Decimal(0) for row in rows), Decimal(0))),
                    "reserved_cny": str(sum((row.reservation_cny for row in rows if row.charged_cny is None), Decimal(0)))}

    def _reuse_embedding(self, binding, request, body, scope, expected_request_id=None):
        if request.kind != "embedding" or not request.step_key.startswith("graph-embedding:"):
            return None
        with self.maker.begin() as session:
            saved = session.get(DataCenterModelReuse, (binding.task_id, request.step_key))
            checksum = content_digest(body)
            if saved is not None:
                if saved.request_sha256 != checksum:
                    raise ValueError("复用请求参数与原始记录不一致")
                if saved.scope_fingerprint != scope.fingerprint:
                    raise PermissionError("模型复用结果的授权已经改变")
                original = session.get(ProviderRequest, saved.provider_request_id)
            else:
                prior = session.scalar(select(ProviderRequest).where(
                    ProviderRequest.run_id == binding.run_id, ProviderRequest.step_key == request.step_key))
                if prior is not None:
                    if expected_request_id is not None:
                        if (prior.id != expected_request_id or prior.status != "completed"
                                or prior.request_body != body or prior.response_body is None):
                            raise ValueError("原请求编号、参数或完成状态不符合复用要求")
                        return self._reply(prior)
                    return None
                candidates = select(ProviderRequest).join(
                    DataCenterTaskBinding, DataCenterTaskBinding.run_id == ProviderRequest.run_id
                ).where(DataCenterTaskBinding.actor_id == binding.actor_id,
                        DataCenterTaskBinding.source_ids.contains(binding.source_ids),
                        DataCenterTaskBinding.source_ids.contained_by(binding.source_ids),
                        ProviderRequest.step_key == request.step_key,
                        ProviderRequest.request_body == body,
                        ProviderRequest.service == "dashscope",
                        ProviderRequest.model == request.model,
                        ProviderRequest.status == "completed",
                        ProviderRequest.price_basis["verification_scope"].astext == "data_center"
                )
                if expected_request_id is not None:
                    candidates = candidates.where(ProviderRequest.id == expected_request_id)
                original = session.scalar(candidates.order_by(ProviderRequest.created_at, ProviderRequest.id).limit(1))
                if original is None:
                    return None
                session.add(DataCenterModelReuse(task_id=binding.task_id, step_key=request.step_key,
                    provider_request_id=original.id, request_sha256=checksum, scope_fingerprint=scope.fingerprint))
            if (original.status != "completed" or original.response_body is None
                    or original.request_body != body
                    or (expected_request_id is not None and original.id != expected_request_id)):
                raise ValueError("模型复用记录与已保存原始响应不一致")
            return self._reply(original)

    async def invoke(self, request, context):
        return await asyncio.to_thread(self._invoke, request, context)

    async def reuse(self, request, original_request_id, context):
        UUID(original_request_id)
        return await asyncio.to_thread(self._invoke, request, context, original_request_id)

    def _invoke(self, request, context, expected_request_id=None):
        binding = self._binding(request.task_id, context)
        if (binding.run_id != request.run_id or not set(request.source_ids).issubset(binding.source_ids)
                or binding.cancel_requested):
            raise PermissionError("模型请求的任务范围或运行状态无效")
        scope = self.authorization.authorize_sync(context, binding.source_ids, "model")
        if scope.sync_actor and binding.operation != "sync":
            raise PermissionError("同步账号的模型请求没有归属来源同步任务")
        if len(request.step_key) > 100:
            raise ValueError("模型步骤名称超过100字符")
        if request.kind == "embedding":
            body = {"model": request.model, "input": request.texts,
                    "dimensions": 1024, "encoding_format": "float"}
            service, output_limit = "dashscope", 0
            input_limit = sum(len(value.encode()) for value in request.texts)
            if input_limit > 12000:
                raise ValueError("embedding 输入超过预算预留范围")
        else:
            body = {"model": request.model, "messages": request.messages,
                    "stream": True, "stream_options": {"include_usage": True}}
            if request.json_mode:
                body["response_format"] = {"type": "json_object"}
            service = "deepseek" if request.model == "deepseek-flash" else "dashscope"
            if service == "deepseek":
                body.update(thinking={"type": "enabled"}, reasoning_effort=request.reasoning_effort,
                            max_tokens=request.max_output_tokens)
            else:
                body.update(enable_thinking=False, max_completion_tokens=request.max_output_tokens)
            input_limit = self.paid.input_bound(request.messages)
            output_limit = request.max_output_tokens
        with self.maker() as lock_session:
            key = f"data-model:{request.run_id}:{request.step_key}"
            lock_session.execute(text("SELECT pg_advisory_lock(hashtextextended(:key,0))"), {"key": key})
            cache_key = None
            try:
                if request.kind == "embedding" and request.step_key.startswith("graph-embedding:"):
                    cache_key = "data-embedding-reuse:" + content_digest({
                        "actor": binding.actor_id, "sources": sorted(binding.source_ids),
                        "step": request.step_key, "body": body})
                    lock_session.execute(text("SELECT pg_advisory_lock(hashtextextended(:key,0))"),
                                         {"key": cache_key})
                binding = self._binding(request.task_id, context)
                if binding.cancel_requested:
                    raise PermissionError("数据任务已经停止新增模型请求")
                scope = self.authorization.authorize_sync(context, binding.source_ids, "model")
                reused = self._reuse_embedding(binding, request, body, scope, expected_request_id)
                if reused is not None:
                    return reused
                if expected_request_id is not None:
                    raise ValueError("没有符合要求的已完成原请求，停止结果复用")
                try:
                    self.paid.request(run_id=request.run_id, step_key=request.step_key,
                                      service=service, body=body, input_limit=input_limit,
                                      output_limit=output_limit, timeout_seconds=request.timeout_seconds)
                except UnknownRequest:
                    # 原请求已经保存为unknown，返回持久状态供执行器停止。
                    pass
                except (TimeoutError, httpx.TimeoutException):
                    with self.maker() as session:
                        saved = session.scalar(select(ProviderRequest).where(
                            ProviderRequest.run_id == request.run_id,
                            ProviderRequest.step_key == request.step_key,
                        ))
                        if saved is None or saved.status != "unknown":
                            raise
                with self.maker() as session:
                    row = session.scalar(select(ProviderRequest).where(
                        ProviderRequest.run_id == request.run_id,
                        ProviderRequest.step_key == request.step_key,
                    ))
                    if row is None:
                        raise RuntimeError("付费调用缺少持久请求记录")
                    return self._reply(row)
            finally:
                if cache_key is not None:
                    lock_session.execute(text("SELECT pg_advisory_unlock(hashtextextended(:key,0))"),
                                         {"key": cache_key})
                lock_session.execute(text("SELECT pg_advisory_unlock(hashtextextended(:key,0))"), {"key": key})
