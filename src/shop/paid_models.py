from time import monotonic

import httpx
from sqlalchemy import select

from shop.assessment_models import AssessmentRun
from shop.assessment_service import run_elapsed, run_stop_reason
from shop.budget import BudgetExceeded, BudgetService
from shop.config import Settings
from shop.model_profiles import ModelProfile
from shop.providers import dashscope_endpoint, deepseek_key, provider_call
from shop.runtime_models import HarnessTask, ProviderRequest


class UnknownRequest(RuntimeError):
    pass


class TruncatedResponse(RuntimeError):
    pass


class RequestLimit(RuntimeError):
    pass


class PaidModels:
    def __init__(self, settings: Settings, budget: BudgetService):
        self.settings = settings
        self.budget = budget

    def request(
        self, *, run_id: str, step_key: str, service: str, body: dict,
        output_limit: int, timeout_seconds: float, task_id: str | None = None,
        input_limit: int = 12000,
        assessment_id: str | None = None,
    ) -> dict:
        if task_id is not None and assessment_id is not None:
            raise ValueError("请求不能同时属于两个任务")
        stage = "assessment" if assessment_id else self.budget.verification_scope or "harness"
        with self.budget.maker() as session:
            if assessment_id is not None:
                run = session.get(AssessmentRun, assessment_id)
                if run is None or run.run_id != run_id:
                    raise ValueError("考核请求所属任务无效")
                reason = run_stop_reason(session, run)
                if reason:
                    raise RequestLimit(reason)
            prior = session.scalar(select(ProviderRequest).where(
                ProviderRequest.run_id == run_id, ProviderRequest.step_key == step_key,
            ))
        if prior is not None:
            prior_stage = (prior.price_basis.get("verification_scope")
                           or prior.price_basis.get("scope")
                           or ("assessment" if prior.assessment_id else "harness"))
            if prior_stage != stage:
                raise ValueError("恢复请求的验证阶段与原始记录不一致")
            if prior.request_body != body:
                raise ValueError("恢复请求参数与原始记录不一致")
            if prior.service != service or prior.model != body["model"]:
                raise ValueError("恢复请求的模型服务与原始记录不一致")
            if prior.assessment_id != assessment_id or prior.task_id != task_id:
                raise ValueError("恢复请求所属任务不一致")
            if prior.status == "completed":
                if prior.response_body is None:
                    raise ValueError("已完成请求缺少保存响应")
                return prior.response_body
            if prior.status not in {"reserved", "cancelled"}:
                cluster_bound = prior.price_basis.get("cluster_run_id") is not None
                if (stage in {"cluster", "data_center"} or cluster_bound) and prior.status == "sent":
                    self.budget.mark_unknown(prior.id, "process_interrupted")
                if assessment_id is not None and not cluster_bound:
                    if prior.status == "sent":
                        self.budget.mark_unknown(prior.id, "process_interrupted")
                    return self.request(
                        run_id=run_id, step_key=f"{step_key}-unknown-retry", service=service,
                        body=body, output_limit=output_limit, timeout_seconds=timeout_seconds,
                        input_limit=input_limit, assessment_id=assessment_id,
                    )
                raise UnknownRequest("外部请求结果未知，保留费用预留并停止")
            if prior.status == "cancelled" and prior.error != "cluster_stopped":
                raise ValueError("请求已经取消")
            self.validate_provider(service, body)
            request = self.budget.resume_unsent(prior.id) if prior.status == "cancelled" else prior
        else:
            with self.budget.maker.begin() as session:
                self.budget.account(session)
                if task_id is not None:
                    task = session.get(HarnessTask, task_id, with_for_update=True)
                    if task is None:
                        raise ValueError("任务不存在")
                    if task.cancel_requested:
                        raise RequestLimit("cancelled")
                    if task.elapsed_seconds >= 120:
                        raise RequestLimit("time_limit")
                    if task.model_calls >= 6:
                        raise RequestLimit("model_limit")
                if assessment_id is not None:
                    run = session.get(AssessmentRun, assessment_id, with_for_update=True)
                    if run is None:
                        raise ValueError("考核任务不存在")
                    reason = run_stop_reason(session, run)
                    if reason:
                        raise RequestLimit(reason)
                    if run.model_calls >= 4:
                        raise RequestLimit("model_limit")
                self.validate_provider(service, body)
                request = self.budget.reserve(
                    task_id=task_id, run_id=run_id, step_key=step_key, service=service,
                    model=body["model"], request_body=body, input_limit=input_limit,
                    output_limit=output_limit, session=session,
                    assessment_id=assessment_id, verification_scope=stage,
                )
                if task_id is not None:
                    task.model_calls += 1
                if assessment_id is not None:
                    run.model_calls += 1
        if task_id is not None:
            with self.budget.maker() as session:
                task = session.get(HarnessTask, task_id)
                reason = "cancelled" if task.cancel_requested else (
                    "time_limit" if task.elapsed_seconds >= 120 else None)
                remaining = 120 - float(task.elapsed_seconds)
            if reason:
                self.budget.cancel_unsent(request.id, reason)
                raise RequestLimit(reason)
            timeout_seconds = min(timeout_seconds, remaining)
        if assessment_id is not None:
            with self.budget.maker() as session:
                run = session.get(AssessmentRun, assessment_id)
                reason = run_stop_reason(session, run)
                remaining = 180 - float(run_elapsed(run))
            if reason:
                self.budget.cancel_unsent(request.id, reason)
                raise RequestLimit(reason)
            timeout_seconds = min(timeout_seconds, remaining)
        try:
            self.budget.mark_sent(request.id)
        except BudgetExceeded as error:
            raise RequestLimit(str(error)) from error
        started = monotonic()
        progress = {}
        try:
            response, provider_id = provider_call(self.settings, service, body, timeout_seconds, progress)
            response["client_duration_seconds"] = monotonic() - started
            self.budget.complete(request.id, response, provider_id)
        except httpx.ConnectError:
            self.budget.connection_failed(request.id)
            raise
        except Exception as error:
            progress["client_duration_seconds"] = monotonic() - started
            self.budget.mark_unknown(request.id, type(error).__name__, progress if "choices" in progress else None)
            raise
        return response

    def validate_provider(self, service: str, body: dict) -> None:
        if service == "deepseek":
            deepseek_key(self.settings)
        elif service == "dashscope":
            dashscope_endpoint(self.settings, "embeddings" if body["model"] == "qwen3.7-text-embedding"
                               else "chat/completions")
        else:
            raise ValueError("未知模型提供商")

    def text(
        self, profile: ModelProfile, messages: list[dict], *, run_id: str,
        step_key: str, timeout_seconds: float, task_id: str | None = None,
        assessment_id: str | None = None,
    ) -> dict:
        started = monotonic()
        for attempt in range(2):
            remaining = timeout_seconds - (monotonic() - started)
            if remaining <= 0:
                raise TimeoutError("模型请求没有剩余执行时间")
            response = self.request(
                run_id=run_id, step_key=step_key if attempt == 0 else f"{step_key}-retry1",
                service=profile.service, body=profile.body(messages),
                output_limit=profile.output_limit, timeout_seconds=remaining, task_id=task_id,
                input_limit=self.input_bound(messages),
                assessment_id=assessment_id,
            )
            choice = response["choices"][0]
            if choice["finish_reason"] == "stop":
                return response
            if choice["finish_reason"] != "length":
                raise ValueError("模型响应没有正常完成")
            # 用户授权截断时提高上限；实际请求参数分别保留在请求记录。
            profile = profile.model_copy(update={"output_limit": min(profile.output_limit * 2, 393216)})
        raise TruncatedResponse(f"{step_key} 模型输出达到提高后的 Token 上限")

    @staticmethod
    def input_bound(messages: list[dict]) -> int:
        total = 0
        for message in messages:
            total += 32
            content = message["content"]
            if isinstance(content, str):
                total += len(content.encode("utf-8"))
            else:
                for part in content:
                    if part["type"] == "text":
                        total += len(part["text"].encode("utf-8"))
                    elif part["type"] == "image_url":
                        total += 16384
                    else:
                        raise ValueError("模型输入包含不支持的类型")
        if total > 131072:
            raise ValueError("模型输入超过本项目预留范围")
        return total

    def embed(
        self, texts: list[str], *, run_id: str, step_key: str,
        timeout_seconds: float, task_id: str | None = None,
    ) -> dict:
        if not 1 <= len(texts) <= 20 or any(not item.strip() for item in texts):
            raise ValueError("embedding 输入必须包含1至20条非空文字")
        # 文字字节数用于保守预留，实际 Token 以提供商 usage 为准。
        input_limit = sum(len(item.encode("utf-8")) for item in texts)
        if input_limit > 12000:
            raise ValueError("本项目 embedding 输入超过保守预留范围")
        response = self.request(
            run_id=run_id, step_key=step_key, service="dashscope",
            body={"model": "qwen3.7-text-embedding", "input": texts,
                  "dimensions": 1024, "encoding_format": "float"},
            output_limit=0, input_limit=input_limit, timeout_seconds=timeout_seconds,
            task_id=task_id,
        )
        if sorted(item["index"] for item in response["data"]) != list(range(len(texts))):
            raise ValueError("embedding 返回的索引数量与输入不一致")
        return response
