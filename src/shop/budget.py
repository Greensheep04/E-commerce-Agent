from datetime import UTC, datetime
from decimal import ROUND_CEILING, Decimal
from uuid import uuid4

from agent_cluster.store import GoalRecord, RunRecord, TaskRecord
from sqlalchemy import select, text
from sqlalchemy.orm import Session, sessionmaker

import shop.cluster_models  # noqa: F401
from shop.assessment_models import AssessmentRun
from shop.assessment_service import run_stop_reason
from shop.customer_runtime_models import CustomerTurn
from shop.data_center_models import DataCenterTaskBinding
from shop.integration_models import IntegrationBudgetWindow, IntegrationCall
from shop.runtime_models import BudgetAccount, HarnessTask, ProviderRequest

PROJECT_LIMIT = Decimal(300)
START_THRESHOLD = Decimal(270)
VERIFICATION_LIMIT = Decimal(10)
STAGE_LIMITS = {"harness": Decimal(10), "customer": Decimal(30),
                "assessment": Decimal(20), "m3": Decimal(1), "cluster": Decimal(30),
                "data_center": Decimal(50)}
INPUT_PRICE = Decimal(2)
CACHED_INPUT_PRICE = Decimal("0.04")
OUTPUT_PRICE = Decimal(8)
PRICE_URL = "https://api-docs.deepseek.com/zh-cn/quick_start/pricing"
PRICE_CHECKED_AT = "2026-10-01"
QWEN_PRICE_URL = "https://help.aliyun.com/zh/model-studio/model-pricing"


def rate_card(service: str, model: str) -> dict:
    if (service, model) == ("deepseek", "deepseek-flash"):
        return {"url": PRICE_URL, "input_cny_per_million": "2",
                "cached_input_cny_per_million": "0.04", "output_cny_per_million": "8"}
    if (service, model) == ("dashscope", "qwen3.8-flash"):
        return {"url": QWEN_PRICE_URL, "input_cny_per_million": "0.8",
                "cached_input_cny_per_million": "0.8", "output_cny_per_million": "2.7"}
    if (service, model) == ("dashscope", "qwen3.7-text-embedding"):
        return {"url": QWEN_PRICE_URL, "input_cny_per_million": "0.5",
                "cached_input_cny_per_million": "0.5", "output_cny_per_million": "0"}
    raise ValueError("服务或模型尚未配置计价依据")


def round_cost(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.00000001"), rounding=ROUND_CEILING)


class BudgetExceeded(ValueError):
    pass


def reserved_cost(input_tokens: int, output_tokens: int, card: dict | None = None) -> Decimal:
    if input_tokens < 0 or output_tokens < 0:
        raise ValueError("token 上限无效")
    card = card or rate_card("deepseek", "deepseek-flash")
    return round_cost((Decimal(input_tokens) * Decimal(card["input_cny_per_million"])
                       + Decimal(output_tokens) * Decimal(card["output_cny_per_million"]))
                      / 1_000_000)


def usage_cost(usage: dict, card: dict | None = None, *, embedding: bool = False) -> Decimal:
    card = card or rate_card("deepseek", "deepseek-flash")
    if embedding:
        total = int(usage["total_tokens"])
        if total < 0:
            raise ValueError("模型用量字段无效")
        return reserved_cost(total, 0, card)
    prompt = int(usage["prompt_tokens"])
    completion = int(usage["completion_tokens"])
    hit = int(usage.get("prompt_cache_hit_tokens", 0))
    miss = int(usage.get("prompt_cache_miss_tokens", prompt - hit))
    if min(prompt, completion, hit, miss) < 0 or hit + miss != prompt:
        raise ValueError("模型用量字段无效")
    return round_cost((
        Decimal(hit) * Decimal(card["cached_input_cny_per_million"])
        + Decimal(miss) * Decimal(card["input_cny_per_million"])
        + Decimal(completion) * Decimal(card["output_cny_per_million"])
    ) / 1_000_000)


class BudgetService:
    def __init__(self, maker: sessionmaker, *, verification_scope: str | None = None):
        if verification_scope is not None and verification_scope not in STAGE_LIMITS:
            raise ValueError("未知验证阶段")
        self.maker = maker
        self.verification_scope = verification_scope

    @staticmethod
    def account(session: Session) -> BudgetAccount:
        session.execute(text("SELECT pg_advisory_xact_lock(hashtext('shop:budget-account'))"))
        account = session.get(BudgetAccount, "project", with_for_update=True)
        if account is None:
            account = BudgetAccount(id="project", spent_cny=0, reserved_cny=0)
            session.add(account)
            session.flush()
        return account

    def reserve(
        self, *, task_id: str | None, run_id: str | None, step_key: str,
        service: str, model: str, request_body: dict, input_limit: int,
        output_limit: int, verification: bool = True, session: Session | None = None,
        scope: str | None = None,
        assessment_id: str | None = None, verification_scope: str | None = None,
    ) -> ProviderRequest:
        stage = verification_scope or scope or self.verification_scope or "harness"
        if self.verification_scope is not None and not verification:
            raise ValueError("指定验证阶段必须使用验证预算")
        if stage not in STAGE_LIMITS or (task_id is not None and assessment_id is not None):
            raise ValueError("请求所属任务或费用阶段无效")
        if scope is not None and scope != "customer":
            raise ValueError("费用阶段无效")
        if self.verification_scope is not None and stage != self.verification_scope:
            raise ValueError("请求费用阶段与服务配置不一致")
        if session is None:
            with self.maker.begin() as owned_session:
                return self._reserve(
                    owned_session, task_id=task_id, run_id=run_id, step_key=step_key,
                    service=service, model=model, request_body=request_body,
                    input_limit=input_limit, output_limit=output_limit,
                    verification=verification, scope=scope,
                    assessment_id=assessment_id, stage=stage,
                )
        return self._reserve(
            session, task_id=task_id, run_id=run_id, step_key=step_key,
            service=service, model=model, request_body=request_body,
            input_limit=input_limit, output_limit=output_limit, verification=verification,
            scope=scope,
            assessment_id=assessment_id, stage=stage,
        )

    def _reserve(
        self, session: Session, *, task_id: str | None, run_id: str | None,
        step_key: str, service: str, model: str, request_body: dict,
        input_limit: int, output_limit: int, verification: bool, scope: str | None,
        assessment_id: str | None, stage: str, resume_request: ProviderRequest | None = None,
    ) -> ProviderRequest:
        self.account(session)
        binding = None
        if stage == "data_center":
            binding = session.scalar(select(DataCenterTaskBinding).where(
                DataCenterTaskBinding.run_id == run_id).with_for_update())
            if binding is None or binding.cancel_requested:
                raise BudgetExceeded("数据任务已经停止新增付费调用")
        if assessment_id is not None:
            run = session.get(AssessmentRun, assessment_id, with_for_update=True)
            if run is None or run.run_id != run_id or stage != "assessment":
                raise ValueError("考核请求所属任务无效")
            reason = run_stop_reason(session, run)
            if reason:
                raise BudgetExceeded(reason)
        card = rate_card(service, model)
        # Qwen 文档允许实际总输出与指定上限最多相差 10 Token。
        billed_output_limit = output_limit + (10 if model == "qwen3.8-flash" else 0)
        amount = reserved_cost(input_limit, billed_output_limit, card)
        account = self.account(session)
        total = account.spent_cny + account.reserved_cny
        if total + amount > START_THRESHOLD or total + amount > PROJECT_LIMIT:
            raise BudgetExceeded("项目云 API 预算不足")
        records = session.scalars(select(ProviderRequest)).all()
        cluster_run_id = run_id if resume_request is None else resume_request.price_basis["cluster_run_id"]
        cluster_run = session.get(RunRecord, cluster_run_id) if cluster_run_id is not None else None
        if cluster_run is None:
            resource_ids = [value for value in (run_id, task_id, assessment_id) if value is not None]
            if binding is not None:
                resource_ids.append(binding.task_id)
            cluster_runs = session.scalars(select(RunRecord).join(TaskRecord, TaskRecord.run_id == RunRecord.run_id)
                .join(IntegrationCall, (IntegrationCall.request_id == TaskRecord.request_id)
                      & (IntegrationCall.actor_id == RunRecord.actor_id))
                .where(IntegrationCall.resource_id.in_(resource_ids),
                       TaskRecord.status.in_(["sent", "running"]))).all()
            candidates = {item.run_id: item for item in cluster_runs}
            if binding is not None and binding.request_id is not None:
                direct = session.scalars(select(RunRecord).join(TaskRecord, TaskRecord.run_id == RunRecord.run_id)
                                         .where(TaskRecord.request_id == binding.request_id,
                                                RunRecord.actor_id == binding.actor_id,
                                                TaskRecord.status.in_(["sent", "running"]))).all()
                candidates.update({item.run_id: item for item in direct})
            if len(candidates) > 1:
                raise BudgetExceeded("同一业务请求不能同时归属多个目标运行")
            cluster_run = next(iter(candidates.values()), None)
        cluster_goal = None
        if cluster_run is not None:
            cluster_goal = session.get(GoalRecord, cluster_run.goal_id, with_for_update=True)
            if cluster_goal.state != "active" or cluster_run.status not in {"queued", "running"}:
                raise BudgetExceeded("目标已停止新增付费调用")
            goal_total = sum((item.charged_cny if item.charged_cny is not None else item.reservation_cny
                              for item in records
                              if item.price_basis.get("cluster_goal_id") == cluster_goal.goal_id), Decimal(0))
            if goal_total + amount > Decimal(cluster_goal.document["budget_cny"]):
                raise BudgetExceeded("目标预算不足")
        stage_total = sum((
            item.charged_cny if item.charged_cny is not None else item.reservation_cny
            for item in records
            if (item.price_basis.get("verification_scope")
                or item.price_basis.get("scope")
                or ("assessment" if item.assessment_id else "harness")) == stage
            and (stage != "harness" or item.price_basis.get("verification", False))
        ), Decimal(0))
        if (verification or stage != "harness") and stage_total + amount > STAGE_LIMITS[stage]:
            messages = {"harness": "M2 验证预算不足", "customer": "customer 客服验证预算不足",
                        "assessment": "assessment 考核验证预算不足", "m3": "m3 自媒体验证预算不足",
                        "cluster": "cluster 集群验证预算不足",
                        "data_center": "data_center 验证预算不足"}
            raise BudgetExceeded(messages[stage])
        window = session.scalar(select(IntegrationBudgetWindow).where(IntegrationBudgetWindow.active))
        if stage == "data_center" and (window is None or not window.include_open_reservations
                                       or window.limit_cny > STAGE_LIMITS[stage]):
            raise BudgetExceeded("数据中台需要包含全部未结预留的50元以内费用窗口")
        if window is not None:
            window_total = sum((
                item.charged_cny if item.charged_cny is not None else item.reservation_cny
                for item in records if item.price_basis.get("integration_run_id") == window.id
            ), Decimal(0))
            if window.include_open_reservations:
                window_total += sum((item.reservation_cny for item in records
                                     if item.charged_cny is None
                                     and item.price_basis.get("integration_run_id") != window.id), Decimal(0))
            if window_total + amount > window.limit_cny:
                raise BudgetExceeded("接口集成验证预算不足")
        if resume_request is None:
            request = ProviderRequest(
                id=str(uuid4()), task_id=task_id, assessment_id=assessment_id,
                run_id=run_id, step_key=step_key,
                service=service, model=model, status="reserved", reservation_cny=amount,
                price_basis={**card, "checked_at": PRICE_CHECKED_AT,
                             "input_limit": input_limit, "output_limit": billed_output_limit,
                             "verification": verification, "scope": scope,
                             "verification_scope": stage,
                             "cluster_goal_id": None if cluster_goal is None else cluster_goal.goal_id,
                             "cluster_run_id": None if cluster_run is None else cluster_run.run_id,
                             "integration_run_id": window.id if window is not None else None},
                request_body=request_body,
            )
        else:
            request = resume_request
            basis = request.price_basis
            if amount != request.reservation_cny or any(basis.get(key) != value for key, value in card.items()):
                raise ValueError("恢复请求的计价依据与原始预留不一致")
            request.price_basis = {**basis, "unsent_resumptions": [*basis.get("unsent_resumptions", []), {
                "previous_status": request.status, "previous_error": request.error,
                "previous_charged_cny": str(request.charged_cny),
                "reservation_cny": str(request.reservation_cny), "resumed_at": datetime.now(UTC).isoformat(),
            }]}
            request.status = "reserved"
            request.charged_cny = None
            request.error = None
        session.add(request)
        account.reserved_cny += amount
        session.flush()
        return request

    def resume_unsent(self, request_id: str) -> ProviderRequest:
        with self.maker.begin() as session:
            self.account(session)
            request = session.get(ProviderRequest, request_id, with_for_update=True)
            if (request is None or request.status != "cancelled" or request.error != "cluster_stopped"
                    or request.charged_cny != 0 or request.response_body is not None
                    or request.usage is not None or request.provider_request_id is not None
                    or request.completed_at is not None):
                raise ValueError("此请求没有可恢复的未发送预留")
            basis = request.price_basis
            run = session.get(RunRecord, basis.get("cluster_run_id"))
            if run is None:
                raise ValueError("恢复请求缺少原始目标运行")
            goal = session.get(GoalRecord, run.goal_id, with_for_update=True)
            session.refresh(run, with_for_update=True)
            if (goal is None or basis.get("cluster_goal_id") != goal.goal_id
                    or run.actor_id != goal.actor_id
                    or run.document["goal_revision"] != goal.document["revision"]):
                raise ValueError("恢复请求的目标身份或版本不一致")
            if goal.state != "active" or run.status not in {"queued", "running"}:
                raise BudgetExceeded("目标尚未恢复付费调用")
            window = session.scalar(select(IntegrationBudgetWindow).where(IntegrationBudgetWindow.active))
            if basis.get("integration_run_id") != (window.id if window is not None else None):
                raise ValueError("恢复请求的费用窗口与原始记录不一致")
            stage = basis.get("verification_scope") or basis.get("scope") or "harness"
            if stage not in STAGE_LIMITS or self.verification_scope is not None and stage != self.verification_scope:
                raise ValueError("恢复请求的费用阶段无效")
            output_limit = basis["output_limit"] - (10 if request.model == "qwen3.8-flash" else 0)
            return self._reserve(
                session, task_id=request.task_id, run_id=request.run_id, step_key=request.step_key,
                service=request.service, model=request.model, request_body=request.request_body,
                input_limit=basis["input_limit"], output_limit=output_limit,
                verification=basis["verification"], scope=basis.get("scope"),
                assessment_id=request.assessment_id, stage=stage, resume_request=request,
            )

    def mark_sent(self, request_id: str) -> None:
        cancelled = False
        stop_reason = "cancelled"
        with self.maker.begin() as session:
            self.account(session)
            request = session.get(ProviderRequest, request_id)
            if request is None or request.status != "reserved":
                raise ValueError("请求预留状态无效")
            cluster_run_id = request.price_basis.get("cluster_run_id")
            cluster_run = None
            cluster_goal = None
            if cluster_run_id is not None:
                cluster_run = session.get(RunRecord, cluster_run_id)
                cluster_goal = session.get(GoalRecord, cluster_run.goal_id, with_for_update=True)
                session.refresh(cluster_run, with_for_update=True)
            session.refresh(request, with_for_update=True)
            if request.price_basis.get("verification_scope") == "data_center":
                binding = session.scalar(select(DataCenterTaskBinding).where(
                    DataCenterTaskBinding.run_id == request.run_id).with_for_update())
                if binding is None or binding.cancel_requested:
                    account = self.account(session)
                    account.reserved_cny -= request.reservation_cny
                    request.status = "cancelled"
                    request.charged_cny = Decimal(0)
                    request.error = "data_center_cancelled"
                    cancelled = True
            if request.task_id is not None:
                task = session.get(HarnessTask, request.task_id, with_for_update=True)
                if task is None or task.cancel_requested:
                    account = self.account(session)
                    account.reserved_cny -= request.reservation_cny
                    request.status = "cancelled"
                    request.charged_cny = Decimal(0)
                    cancelled = True
            elif request.price_basis.get('scope') == 'customer':
                turn = session.get(CustomerTurn, request.run_id, with_for_update=True)
                if turn is None or turn.cancel_requested or turn.status not in {'queued', 'running'}:
                    account = self.account(session)
                    account.reserved_cny -= request.reservation_cny
                    request.status = 'cancelled'
                    request.charged_cny = Decimal(0)
                    cancelled = True
            elif request.assessment_id is not None:
                run = session.get(AssessmentRun, request.assessment_id, with_for_update=True)
                reason = run_stop_reason(session, run) if run is not None else "missing_run"
                if reason:
                    stop_reason = reason
                    account = self.account(session)
                    account.reserved_cny -= request.reservation_cny
                    request.status = "cancelled"
                    request.charged_cny = Decimal(0)
                    request.error = reason
                    cancelled = True
            if (not cancelled and cluster_run_id is not None
                    and (cluster_goal.state != "active" or cluster_run.status not in {"queued", "running"})):
                account = self.account(session)
                account.reserved_cny -= request.reservation_cny
                request.status = "cancelled"
                request.charged_cny = Decimal(0)
                request.error = "cluster_stopped"
                cancelled = True
            if not cancelled:
                request.status = "sent"
        if cancelled:
            raise BudgetExceeded(stop_reason)

    def cancel_unsent(self, request_id: str, reason: str) -> None:
        with self.maker.begin() as session:
            self.account(session)
            request = session.get(ProviderRequest, request_id, with_for_update=True)
            if request is not None and request.status == "cancelled":
                return
            if request is None or request.status != "reserved":
                raise ValueError("请求预留状态无效")
            account = self.account(session)
            account.reserved_cny -= request.reservation_cny
            request.status = "cancelled"
            request.charged_cny = Decimal(0)
            request.error = reason

    def complete(self, request_id: str, response: dict, provider_request_id: str | None) -> None:
        usage = response.get("usage")
        if not isinstance(usage, dict):
            raise TypeError("模型响应缺少 usage；保留费用预留")
        with self.maker.begin() as session:
            account = self.account(session)
            request = session.get(ProviderRequest, request_id, with_for_update=True)
            if request is None or request.status != "sent":
                raise ValueError("请求完成状态无效")
            cost = usage_cost(usage, request.price_basis,
                              embedding=request.model == "qwen3.7-text-embedding")
            account.reserved_cny -= request.reservation_cny
            account.spent_cny += cost
            request.status = "completed"
            request.charged_cny = cost
            request.response_body = response
            request.usage = usage
            request.provider_request_id = provider_request_id
            request.completed_at = datetime.now(UTC)

    def mark_unknown(self, request_id: str, error: str, response_body: dict | None = None) -> None:
        with self.maker.begin() as session:
            request = session.get(ProviderRequest, request_id, with_for_update=True)
            if request is None or request.status != "sent":
                raise ValueError("未知请求状态无效")
            request.status = "unknown"
            request.error = error
            request.response_body = response_body

    def connection_failed(self, request_id: str) -> None:
        with self.maker.begin() as session:
            request = session.get(ProviderRequest, request_id, with_for_update=True)
            if request is None or request.status != "sent":
                raise ValueError("连接失败请求的状态无效")
            # ConnectError发生在HTTP发送前，费用预留保留供明确的再次执行使用。
            request.status = "reserved"
            request.error = "ConnectError"

    def reconcile_connect_error(self, request_id: str) -> None:
        with self.maker.begin() as session:
            request = session.get(ProviderRequest, request_id, with_for_update=True)
            if request is None or request.status != "unknown" or request.error != "ConnectError":
                raise ValueError("此记录没有可确认的连接阶段失败")
            request.status = "reserved"
