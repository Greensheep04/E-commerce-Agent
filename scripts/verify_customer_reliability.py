import argparse
import asyncio
import contextlib
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
import traceback
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

sys.dont_write_bytecode = True

import httpx
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from shop.budget import BudgetExceeded, BudgetService
from shop.config import Settings
from shop.customer_models import CustomerBinding, CustomerOrder, OrderDataset
from shop.customer_runtime import CUSTOMER_CONFIG, CustomerService, explicit_ticket_request
from shop.customer_runtime_models import (
    CustomerEvent,
    CustomerTicket,
    CustomerToolCall,
    CustomerTurn,
)
from shop.models import Base, User
from shop.runtime_models import BudgetAccount, ProviderRequest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASE_URL = "http://127.0.0.1:8002"
MAX_DEEPSEEK_REQUESTS = 15
MAX_RESUMED_ADDITIONAL_DEEPSEEK_REQUESTS = 8
RESUMABLE_CHECKS = {
    "real_login_and_customer_identity",
    "database_budget_and_unsent_cancel",
    "parallel_sessions_and_request_deduplication",
    "cancel_sent_request_and_wait_for_session_lock",
    "cross_process_resume_after_model",
}
COMPLETE_RESUMABLE_CHECKS = RESUMABLE_CHECKS | {
    "cross_process_resume_after_ticket_tool",
    "sudden_process_termination_and_restart",
    "customer_provider_request_identity_and_fee_records",
}
RESUME_ONLY_SOURCE_FILES = {
    "scripts/customer_checkpoint.py",
    "scripts/verify_customer_reliability.py",
}
DOCUMENTED_POST_RUN_SOURCE_CHANGES = {
    "src/shop/customer_agent.py": {
        "previous": "151a4113c9955d821fe5da94223d69d724777f777a02c56f2276e4448c08702d",
        "current": "d889f398503cdda40ec2e28fce4d5c718464c9ba9fba2ff098b80424e962d7d1",
        "scope": "最终答复支持 respond 工具或匹配 JSON 内容；CUS-05 增加订单、商品属性及候选错误的明确答复，并收窄提示词职责。",
    },
    "src/shop/customer_provider.py": {
        "previous": "97b8a8058cf8e4ca31b5cb742210d75e41078883ee0c21d876e620e2897ab541",
        "current": "43df3f5c2fb5302e99885e2df50fcab304dee08b96fea5def040b59ab9b7e372",
        "scope": "仅当持久化消息配置包含 response_format 时，才将该字段加入请求体。",
    },
    "src/shop/customer_runtime.py": {
        "previous": "6a4228108027544e4742a6fc99062f3dc56bef28763d9efa77fd6c409d02c84e",
        "current": "4e7dc9e608ac1aae3de3e5346425ede1814b306742a65156ef1f8f096111736a",
        "scope": "新消息使用 customer-v3 配置；会话读取结果增加模型请求和工具调用详情。",
    },
    "src/shop/orders.py": {
        "previous": "8ca8bb878c08a80bb5530419ade11f72a635a8e939fc7d29ef3971a9efd5ee53",
        "current": "48043a2a177bbfbb2e2f0a6ec7d310a70cb9e3697c7e24ee709ab6c32d75cb85",
        "scope": "增加 is_order_number，并供 normalize_order_number 复用。",
    },
}
SOURCE_FILES = (
    "src/shop/app.py",
    "src/shop/auth.py",
    "src/shop/budget.py",
    "src/shop/config.py",
    "src/shop/customer_agent.py",
    "src/shop/customer_api.py",
    "src/shop/customer_models.py",
    "src/shop/customer_provider.py",
    "src/shop/customer_runtime.py",
    "src/shop/customer_runtime_models.py",
    "src/shop/customer_worker.py",
    "src/shop/orders.py",
    "src/shop/providers.py",
    "src/shop/runtime_models.py",
    "scripts/customer_checkpoint.py",
    "scripts/verify_customer_reliability.py",
)


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def safe_decimal(value) -> str | None:
    return str(value) if value is not None else None


def require(condition: bool, message: str):
    if not condition:
        raise AssertionError(message)


class ReliabilityRun:
    def __init__(self, run_id: str, evidence_dir: Path, resume_report_path: Path | None = None):
        self.run_id = run_id
        self.evidence_dir = evidence_dir
        self.evidence_dir.mkdir(parents=True, exist_ok=False)
        self.resume_report_path = resume_report_path
        self.prior_report = None
        self.resume_mode = None
        self.settings = Settings()
        self.engine = create_engine(self.settings.database_url, pool_pre_ping=True)
        self.maker = sessionmaker(self.engine, expire_on_commit=False)
        self.service_process: subprocess.Popen | None = None
        self.service_stdout = None
        self.service_stderr = None
        self.service_attempt = 0
        self.process_records = []
        self.clients: dict[int, httpx.AsyncClient] = {}
        self.user_ids: dict[int, str] = {}
        self.passwords: list[str] = []
        self.turn_ids: set[str] = set()
        self.conversation_ids: set[str] = set()
        self.current_phase = "startup"
        source_hashes_before = self.source_hashes()
        self.report = {
            "milestone": "CUS-04",
            "run_id": run_id,
            "assigned_model": "gpt-6-luna",
            "reasoning_effort": "max",
            "started_at": utc_now(),
            "status": "running",
            "service": {"base_url": BASE_URL, "port": 8002},
            "configuration": {
                "customer_model": dict(CUSTOMER_CONFIG),
                "additional_deepseek_request_limit": MAX_DEEPSEEK_REQUESTS,
                "single_request_reservation_limit_cny": "5",
                "customer_budget_limit_cny": "30",
                "project_new_request_threshold_cny": "270",
                "project_budget_limit_cny": "300",
                "database_url": make_url(self.settings.database_url).render_as_string(hide_password=True),
            },
            "source_revision": self.git_revision(),
            "source_sha256": {"before": source_hashes_before},
            "data_version": self.data_version(),
            "database_budget_before": self.project_budget_snapshot(),
            "deepseek_request_count_before": self.deepseek_request_count(),
            "checks": {},
            "turn_ids": [],
            "conversation_ids": [],
            "service_processes": self.process_records,
            "raw_outputs": [],
        }
        if resume_report_path:
            self.load_resume_report(resume_report_path, source_hashes_before)
        self.write_report()

    def load_resume_report(self, path: Path, current_hashes: dict[str, str]):
        resolved_path = path.resolve()
        require(resolved_path.is_relative_to(PROJECT_ROOT.resolve()),
                "恢复报告必须位于项目目录内")
        previous = json.loads(resolved_path.read_text(encoding="utf-8"))
        require(previous.get("milestone") == "CUS-04", "恢复报告里程碑不是CUS-04")
        require(previous.get("status") == "failed_or_incomplete", "恢复报告没有未完成状态")
        require(previous.get("source_hashes_unchanged") is True,
                "恢复报告没有证明原运行期间源码摘要保持一致")
        failure = previous.get("failure", {})
        previous_checks = previous.get("checks", {})
        ticket_resume = (
            failure.get("phase") == "cross_process_resume_after_ticket_tool"
            and failure.get("message") == "工具检查点前没有恰好写入一条本人订单工单"
        )
        completed_resume = (
            failure.get("type") == "RuntimeError"
            and failure.get("message") == "Event loop is closed"
            and COMPLETE_RESUMABLE_CHECKS.issubset(previous_checks)
        )
        require(ticket_resume or completed_resume,
                "恢复报告没有处于可继续或可完成核验的状态")
        required_checks = RESUMABLE_CHECKS if ticket_resume else COMPLETE_RESUMABLE_CHECKS
        require(required_checks.issubset(previous_checks), "恢复报告缺少必要的已通过检查")
        require(all(check.get("status") == "passed" for check in previous_checks.values()),
                "恢复报告包含未通过的前序检查")
        turn_ids = previous.get("turn_ids", [])
        conversation_ids = previous.get("conversation_ids", [])
        require(turn_ids and conversation_ids, "恢复报告缺少已检查消息或会话编号")
        self.turn_ids.update(turn_ids)
        external_activity = self.outside_request_groups(
            previous["started_at"], previous["completed_at"],
        )
        external_request_count = sum(group["request_count"] for group in external_activity)
        previous_request_count = previous.get("new_deepseek_request_count")
        global_request_change = (
            previous.get("deepseek_request_count_after", 0)
            - previous.get("deepseek_request_count_before", 0)
        )
        if ticket_resume:
            require(previous_request_count == 7, "恢复报告中的已有DeepSeek请求数与批准范围不符")
            require(global_request_change == previous_request_count + external_request_count,
                    "恢复报告中的全局请求变化无法由本次和其他run记录解释")
            self.resume_mode = "continue_ticket_and_restart"
        else:
            require(previous_request_count == 12,
                    "已完成检查报告中的DeepSeek请求总数与记录不符")
            require(previous.get("continuation_additional_deepseek_request_count") == 5,
                    "已完成检查报告中的续跑请求数与记录不符")
            require(previous.get("reused_prior_report", {}).get("request_count_before_resume") == 7,
                    "已完成检查报告缺少原运行请求基数")
            require(global_request_change
                    == previous["continuation_additional_deepseek_request_count"] + external_request_count,
                    "已完成检查报告中的全局请求变化无法由CUS-04与其他run记录解释")
            self.resume_mode = "finalize_completed_checks"
        previous_hashes = previous.get("source_sha256", {}).get("after", {})
        require(previous_hashes, "恢复报告缺少结束时源码摘要")
        checked_hashes = {}
        accepted_source_changes = []
        unapproved_source_changes = []
        for name in SOURCE_FILES:
            if name in RESUME_ONLY_SOURCE_FILES:
                continue
            previous_hash = previous_hashes.get(name)
            current_hash = current_hashes[name]
            matches = previous_hash == current_hash
            documented = DOCUMENTED_POST_RUN_SOURCE_CHANGES.get(name)
            accepted_change = bool(
                not matches and documented
                and previous_hash == documented["previous"]
                and current_hash == documented["current"]
            )
            evidence = {
                "previous": previous_hash,
                "current": current_hash,
                "matches": matches,
                "accepted_documented_change": accepted_change,
                "change_scope": documented["scope"] if accepted_change else None,
            }
            checked_hashes[name] = evidence
            if accepted_change:
                accepted_source_changes.append({"file": name, **evidence})
            elif not matches:
                unapproved_source_changes.append(name)
        require(not unapproved_source_changes,
                f"复用前序检查时发现未登记的生产源码摘要变化：{', '.join(unapproved_source_changes)}")
        require(previous.get("data_version") == self.report["data_version"],
                "恢复报告与当前订单数据版本不一致")
        self.prior_report = previous
        self.conversation_ids.update(conversation_ids)
        self.report["checks"] = previous_checks
        self.report["turn_ids"] = sorted(self.turn_ids)
        self.report["conversation_ids"] = sorted(self.conversation_ids)
        self.report["raw_outputs"] = previous.get("raw_outputs", [])
        self.report["reused_prior_report"] = {
            "path": str(resolved_path.relative_to(PROJECT_ROOT)),
            "run_id": previous["run_id"],
            "status": previous["status"],
            "failed_phase": previous["failure"]["phase"],
            "resume_mode": self.resume_mode,
            "reused_checks": sorted(previous_checks),
            "request_count_before_resume": previous_request_count,
            "additional_request_limit": MAX_RESUMED_ADDITIONAL_DEEPSEEK_REQUESTS,
            "external_customer_request_groups_during_prior_run": external_activity,
            "source_hash_comparison": checked_hashes,
            "accepted_documented_post_run_source_changes": accepted_source_changes,
        }

    def git_revision(self) -> str:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT,
            capture_output=True, text=True, check=True,
        )
        return result.stdout.strip()

    def source_hashes(self) -> dict[str, str]:
        return {name: sha256_file(PROJECT_ROOT / name) for name in SOURCE_FILES}

    def data_version(self) -> dict:
        with self.maker() as session:
            chosen = {}
            for username in ("customer-11091", "customer-11176"):
                user = session.scalar(select(User).where(User.username == username))
                require(user is not None, f"缺少账号 {username}")
                binding = session.get(CustomerBinding, user.id)
                require(binding is not None, f"账号 {username} 没有客户绑定")
                chosen[username] = binding
            revisions = {row.source_revision for row in chosen.values()}
            require(len(revisions) == 1, "测试账号使用了不同订单来源版本")
            revision = next(iter(revisions))
            dataset = session.get(OrderDataset, revision)
            require(dataset is not None, "订单来源版本记录缺失")
            order_counts = {}
            for username, binding in chosen.items():
                order_counts[str(binding.customer_id)] = session.scalar(select(func.count()).select_from(
                    CustomerOrder).where(
                        CustomerOrder.source_revision == revision,
                        CustomerOrder.customer_id == binding.customer_id,
                    ))
            return {
                "source_id": dataset.source_id,
                "revision": dataset.revision,
                "counts": dataset.counts,
                "test_customer_ids": sorted(binding.customer_id for binding in chosen.values()),
                "per_customer_order_counts": order_counts,
            }

    def project_budget_snapshot(self) -> dict:
        with self.maker() as session:
            account = session.get(BudgetAccount, "project")
            if account is None:
                return {"spent_cny": "0", "reserved_cny": "0"}
            return {"spent_cny": str(account.spent_cny), "reserved_cny": str(account.reserved_cny)}

    def deepseek_request_count(self) -> int:
        with self.maker() as session:
            return session.scalar(select(func.count()).select_from(ProviderRequest).where(
                ProviderRequest.model == "deepseek-flash",
                ProviderRequest.price_basis["scope"].astext == "customer",
            )) or 0

    def outside_request_groups(self, started_at: str, completed_at: str) -> list[dict]:
        start = datetime.fromisoformat(started_at)
        end = datetime.fromisoformat(completed_at)
        with self.maker() as session:
            rows = session.scalars(select(ProviderRequest).where(
                ProviderRequest.service == "deepseek",
                ProviderRequest.model == "deepseek-flash",
                ProviderRequest.price_basis["scope"].astext == "customer",
                ProviderRequest.created_at >= start,
                ProviderRequest.created_at <= end,
            ).order_by(ProviderRequest.created_at, ProviderRequest.id)).all()
        groups: dict[str, list[ProviderRequest]] = {}
        for row in rows:
            if row.run_id not in self.turn_ids:
                groups.setdefault(row.run_id, []).append(row)
        return [{
            "run_id": run_id,
            "request_count": len(group),
            "created_at_first": group[0].created_at.isoformat(),
            "created_at_last": group[-1].created_at.isoformat(),
            "statuses": [row.status for row in group],
        } for run_id, group in sorted(groups.items())]

    def new_deepseek_request_count(self) -> int:
        if not self.turn_ids:
            return 0
        with self.maker() as session:
            return session.scalar(select(func.count()).select_from(ProviderRequest).where(
                ProviderRequest.run_id.in_(self.turn_ids),
                ProviderRequest.model == "deepseek-flash",
                ProviderRequest.price_basis["scope"].astext == "customer",
            )) or 0

    def write_report(self):
        self.report["updated_at"] = utc_now()
        (self.evidence_dir / "report.json").write_text(
            json.dumps(self.report, ensure_ascii=False, indent=2, default=str) + "\n",
            encoding="utf-8",
        )

    def pass_check(self, name: str, evidence: dict):
        self.current_phase = name
        self.report["checks"][name] = {"status": "passed", "evidence": evidence}
        self.write_report()

    def add_turn(self, turn_id: str):
        self.turn_ids.add(turn_id)
        self.report["turn_ids"] = sorted(self.turn_ids)
        self.write_report()

    def add_conversation(self, conversation_id: str):
        self.conversation_ids.add(conversation_id)
        self.report["conversation_ids"] = sorted(self.conversation_ids)
        self.write_report()

    def ensure_request_limit(self):
        count = self.new_deepseek_request_count()
        require(count <= MAX_DEEPSEEK_REQUESTS,
                f"新增DeepSeek请求超过上限：{count}/{MAX_DEEPSEEK_REQUESTS}")

    def require_request_capacity(self, maximum_additional_calls: int):
        count = self.new_deepseek_request_count()
        require(count + maximum_additional_calls <= MAX_DEEPSEEK_REQUESTS,
                f"当前剩余请求额度不足以开始此项检查：{count}+{maximum_additional_calls}")

    def db_turn(self, turn_id: str) -> dict:
        with self.maker() as session:
            turn = session.get(CustomerTurn, turn_id)
            require(turn is not None, f"消息记录缺失：{turn_id}")
            return {
                "id": turn.id,
                "conversation_id": turn.conversation_id,
                "request_id": turn.request_id,
                "message": turn.message,
                "status": turn.status,
                "cancel_requested": turn.cancel_requested,
                "model_calls": turn.model_calls,
                "tool_calls": turn.tool_calls,
                "elapsed_seconds": str(turn.elapsed_seconds),
                "started_at": turn.started_at.isoformat() if turn.started_at else None,
                "finished_at": turn.finished_at.isoformat() if turn.finished_at else None,
                "has_result": turn.result is not None,
                "result_kind": turn.result.get("kind") if turn.result else None,
                "error": turn.error,
                "config": {
                    key: turn.config[key]
                    for key in (
                        "version", "model", "thinking", "reasoning_effort", "max_tokens",
                        "model_limit", "tool_limit", "time_limit_seconds",
                    )
                    if key in turn.config
                },
                "initial_messages_saved": "initial_messages" in turn.config,
            }

    def db_requests(self, run_id: str) -> list[dict]:
        with self.maker() as session:
            rows = session.scalars(select(ProviderRequest).where(
                ProviderRequest.run_id == run_id,
            ).order_by(ProviderRequest.created_at, ProviderRequest.id)).all()
            return [self.request_evidence(row) for row in rows]

    @staticmethod
    def request_evidence(row: ProviderRequest) -> dict:
        body = row.request_body if isinstance(row.request_body, dict) else {}
        return {
            "id": row.id,
            "run_id": row.run_id,
            "step_key": row.step_key,
            "task_id": row.task_id,
            "service": row.service,
            "model": row.model,
            "status": row.status,
            "reservation_cny": str(row.reservation_cny),
            "charged_cny": safe_decimal(row.charged_cny),
            "usage": row.usage,
            "provider_request_id": row.provider_request_id,
            "error": row.error,
            "scope": row.price_basis.get("scope"),
            "verification": row.price_basis.get("verification"),
            "request_body_sha256": hashlib.sha256(
                json.dumps(body, ensure_ascii=False, sort_keys=True).encode("utf-8")
            ).hexdigest(),
        }

    def request_rows(self, turn_id: str) -> list[ProviderRequest]:
        with self.maker() as session:
            rows = session.scalars(select(ProviderRequest).where(
                ProviderRequest.run_id == turn_id,
            ).order_by(ProviderRequest.created_at, ProviderRequest.id)).all()
            return [self.request_evidence(row) for row in rows]

    def verify_legacy_ticket_turn_compatibility(self) -> dict:
        ticket_evidence = self.prior_report["checks"][
            "cross_process_resume_after_ticket_tool"
        ]["evidence"]
        turn_id = ticket_evidence["turn_id"]
        expected_turn = ticket_evidence["turn_after_resume"]
        expected_request_rows = {
            row["id"]: row
            for row in self.prior_report["request_and_fee_evidence"]["requests"]
            if row["run_id"] == turn_id
        }
        with self.maker() as session:
            turn = session.get(CustomerTurn, turn_id)
            require(turn is not None, "已完成的工单消息记录缺失")
            profile = dict(turn.config)
            initial_messages = profile.get("initial_messages")
            request_rows = session.scalars(select(ProviderRequest).where(
                ProviderRequest.run_id == turn_id,
            ).order_by(ProviderRequest.created_at, ProviderRequest.id)).all()
            tool_rows = session.scalars(select(CustomerToolCall).where(
                CustomerToolCall.turn_id == turn_id,
            ).order_by(CustomerToolCall.created_at, CustomerToolCall.id)).all()
            ticket_rows = session.scalars(select(CustomerTicket).where(
                CustomerTicket.turn_id == turn_id,
            ).order_by(CustomerTicket.created_at, CustomerTicket.id)).all()
            live_turn = {
                "id": turn.id,
                "status": turn.status,
                "cancel_requested": turn.cancel_requested,
                "model_calls": turn.model_calls,
                "tool_calls": turn.tool_calls,
                "elapsed_seconds": str(turn.elapsed_seconds),
                "authorization_present": turn.authorization is not None,
            }
        profile_keys = (
            "version", "model", "thinking", "reasoning_effort", "max_tokens",
            "model_limit", "tool_limit", "time_limit_seconds",
        )
        profile_snapshot = {key: profile[key] for key in profile_keys if key in profile}
        require(profile_snapshot == expected_turn["config"],
                "旧工单消息配置与CUS-04最终记录不同")
        require(profile.get("version") == "customer-v1" and "response_format" not in profile,
                "旧工单消息配置不符合 customer-v1 请求边界")
        require(isinstance(initial_messages, list) and initial_messages,
                "旧工单消息没有保存初始模型消息")
        require(expected_turn["initial_messages_saved"] is True,
                "CUS-04报告没有记录旧工单消息的初始内容")
        expected_live_turn = {
            "id": expected_turn["id"],
            "status": expected_turn["status"],
            "cancel_requested": expected_turn["cancel_requested"],
            "model_calls": expected_turn["model_calls"],
            "tool_calls": expected_turn["tool_calls"],
            "elapsed_seconds": expected_turn["elapsed_seconds"],
        }
        comparable_live_turn = {
            key: value for key, value in live_turn.items() if key != "authorization_present"
        }
        require(comparable_live_turn == expected_live_turn,
                "旧工单消息的状态、取消标记、累计计数或时间与CUS-04最终记录不同")
        actual_request_rows = {
            row.id: (row, self.request_evidence(row))
            for row in request_rows
        }
        require(expected_request_rows and actual_request_rows.keys() == expected_request_rows.keys(),
                "旧工单消息的持久化模型请求编号与CUS-04最终记录不同")
        require(all(actual_request_rows[request_id][1] == expected
                    for request_id, expected in expected_request_rows.items()),
                "旧工单消息的持久化请求或费用记录与CUS-04最终报告不同")
        chat_rows = [row for row in request_rows if row.step_key.startswith("chat-")]
        require([row.step_key for row in chat_rows] == ["chat-0", "chat-1", "chat-2"],
                "旧工单消息的模型请求步骤与CUS-04最终记录不同")
        required_body_keys = {
            "model", "thinking", "reasoning_effort", "max_tokens", "messages", "tools", "stream",
        }
        for row in chat_rows:
            body = row.request_body
            require(isinstance(body, dict) and set(body) == required_body_keys,
                    "旧工单消息的持久化请求体字段与 customer-v1 配置不符")
            require(all(body[key] == profile[key] for key in (
                "model", "thinking", "reasoning_effort", "max_tokens",
            )), "旧工单消息的请求参数与持久化配置不同")
            require(body["stream"] is False and isinstance(body["messages"], list)
                    and isinstance(body["tools"], list) and body["tools"],
                    "旧工单消息的请求体缺少消息、工具或非流式设置")
        require(chat_rows[0].request_body["messages"] == initial_messages,
                "旧工单消息的首个请求内容与已保存初始消息不同")
        expected_ticket_ids = ticket_evidence["ticket_after_resume_ids"]
        require(len(ticket_rows) == 1 and [row.id for row in ticket_rows] == expected_ticket_ids
                and ticket_rows[0].authorization_id == turn_id
                and ticket_rows[0].order_number == ticket_evidence["requested_order_number"],
                "旧工单消息对应的授权工单记录发生变化")
        expected_ticket = ticket_evidence["ticket_at_pause"]
        actual_ticket = ticket_rows[0]
        require(all(getattr(actual_ticket, key) == expected_ticket[key] for key in (
            "id", "number", "conversation_id", "turn_id", "authorization_id",
            "order_number", "reason", "status",
        )) and actual_ticket.created_at.isoformat() == expected_ticket["created_at"],
                "旧工单的编号、内容、状态或创建时间与CUS-04记录不同")
        tool_ids = [row.id for row in tool_rows]
        tool_names = [row.name for row in tool_rows]
        require(tool_ids == ticket_evidence["tool_ids_after_resume"]
                and tool_names == ticket_evidence["tool_names_after_resume"],
                "旧工单消息的持久化工具调用记录与CUS-04最终报告不同")
        initial_messages_sha256 = hashlib.sha256(json.dumps(
            initial_messages, ensure_ascii=False, sort_keys=True,
        ).encode("utf-8")).hexdigest()
        return {
            "turn_id": turn_id,
            "profile_version": profile["version"],
            "response_format_in_profile": "response_format" in profile,
            "initial_messages_count": len(initial_messages),
            "initial_messages_sha256": initial_messages_sha256,
            "first_request_matches_saved_initial_messages": True,
            "model_request_count": len(chat_rows),
            "request_body_keys": sorted(required_body_keys),
            "response_format_in_request_bodies": False,
            "request_bodies_match_persisted_run_evidence": True,
            "model_request_ids": [row.id for row in chat_rows],
            "turn_counters_and_elapsed_seconds_match": True,
            "turn_authorization_field_present": live_turn["authorization_present"],
            "original_message_explicitly_authorized": ticket_evidence[
                "original_message_explicitly_authorized"
            ],
            "tool_ids_and_names_match": True,
            "ticket_id": ticket_rows[0].id,
            "ticket_number": ticket_rows[0].number,
            "ticket_status": ticket_rows[0].status,
            "ticket_authorization_id": ticket_rows[0].authorization_id,
        }

    def provider_rows_live(self, turn_id: str) -> list[dict]:
        with self.maker() as session:
            rows = session.scalars(select(ProviderRequest).where(
                ProviderRequest.run_id == turn_id,
            ).order_by(ProviderRequest.created_at, ProviderRequest.id)).all()
            return [self.request_evidence(row) for row in rows]

    def lock_key_owners(self, conversation_id: str) -> list[int]:
        key = f"customer-conversation:{conversation_id}"
        with self.engine.connect() as connection:
            value = connection.scalar(text("SELECT hashtextextended(:key, 0)"), {"key": key})
            unsigned = int(value) & ((1 << 64) - 1)
            high = unsigned >> 32
            low = unsigned & 0xFFFFFFFF
            rows = connection.execute(text(
                "SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted "
                "AND objsubid=1 AND classid=:high AND objid=:low "
                "AND database=(SELECT oid FROM pg_database WHERE datname=current_database())"
            ), {"high": high, "low": low}).scalars().all()
            return sorted(int(pid) for pid in rows)

    def check_port_free(self):
        with socket.socket() as sock:
            sock.settimeout(0.25)
            result = sock.connect_ex(("127.0.0.1", 8002))
        require(result != 0, "端口8002已有其他进程监听；本次检查没有终止该进程")

    def other_active_turns(self) -> list[tuple[str, str]]:
        with self.maker() as session:
            rows = session.execute(select(CustomerTurn.id, CustomerTurn.status).where(
                CustomerTurn.status.in_({"queued", "running"}),
            )).all()
            return [(turn_id, status) for turn_id, status in rows if turn_id not in self.turn_ids]

    async def wait_for_other_turns(self, boundary: str, timeout: float = 120):
        started = time.monotonic()
        deadline = started + timeout
        initial = self.other_active_turns()
        while initial and time.monotonic() < deadline:
            await asyncio.sleep(0.2)
            initial = self.other_active_turns()
        observation = {
            "boundary": boundary,
            "wait_seconds": round(time.monotonic() - started, 3),
            "other_active_turn_count": len(initial),
            "ready_to_start": not initial,
        }
        self.report.setdefault("shared_database_worker_preflight", []).append(observation)
        self.write_report()
        require(not initial,
                f"共享shop数据库仍有其他queued/running turn，未在{timeout:g}秒内结束")

    def start_service(self):
        self.check_port_free()
        self.service_attempt += 1
        stdout_path = self.evidence_dir / f"service-{self.service_attempt}.stdout.txt"
        stderr_path = self.evidence_dir / f"service-{self.service_attempt}.stderr.txt"
        self.service_stdout = stdout_path.open("w", encoding="utf-8")
        self.service_stderr = stderr_path.open("w", encoding="utf-8")
        command = [
            ".venv/bin/uvicorn", "shop.app:app", "--host", "127.0.0.1", "--port", "8002",
        ]
        child_env = os.environ.copy()
        child_env["PYTHONDONTWRITEBYTECODE"] = "1"
        process = subprocess.Popen(
            command, cwd=PROJECT_ROOT, stdout=self.service_stdout,
            stderr=self.service_stderr, env=child_env,
        )
        self.service_process = process
        entry = {
            "attempt": self.service_attempt,
            "pid": process.pid,
            "command": command,
            "started_at": utc_now(),
            "stdout": str(stdout_path.relative_to(PROJECT_ROOT)),
            "stderr": str(stderr_path.relative_to(PROJECT_ROOT)),
            "stop_reason": None,
            "exit_code": None,
        }
        self.process_records.append(entry)
        self.write_report()
        with httpx.Client(timeout=1.5, trust_env=False) as client:
            deadline = time.monotonic() + 45
            while True:
                if process.poll() is not None:
                    self.close_service_logs()
                    raise RuntimeError(f"8002客服服务启动失败，exit_code={process.returncode}")
                try:
                    response = client.get(f"{BASE_URL}/health")
                    if response.status_code == 200 and response.json().get("status") == "ok":
                        entry["ready_at"] = utc_now()
                        self.write_report()
                        break
                except httpx.HTTPError:
                    pass
                if time.monotonic() >= deadline:
                    raise TimeoutError("8002客服服务未在时限内通过health检查")
                time.sleep(0.15)
        listener_pid = self.find_listener_pid()
        require(listener_pid == process.pid, "8002监听进程与本次启动进程不一致")
        entry["listener_pid"] = listener_pid
        self.write_report()
        self.add_raw_output(stdout_path)
        self.add_raw_output(stderr_path)

    def close_service_logs(self):
        if self.service_stdout:
            self.service_stdout.close()
            self.service_stdout = None
        if self.service_stderr:
            self.service_stderr.close()
            self.service_stderr = None

    def stop_service(self, reason: str, *, force: bool = False):
        process = self.service_process
        if process is None:
            return
        entry = self.process_records[-1]
        entry["stop_reason"] = reason
        if process.poll() is None:
            if force:
                process.kill()
            else:
                process.terminate()
            try:
                entry["exit_code"] = process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                entry["exit_code"] = process.wait(timeout=5)
                entry["forced_after_timeout"] = True
        else:
            entry["exit_code"] = process.returncode
        entry["stopped_at"] = utc_now()
        entry["termination"] = "SIGKILL" if force else "SIGTERM"
        self.close_service_logs()
        self.service_process = None
        self.write_report()

    def add_raw_output(self, path: Path):
        item = str(path.relative_to(PROJECT_ROOT))
        if item not in self.report["raw_outputs"]:
            self.report["raw_outputs"].append(item)
            self.write_report()

    async def login(self):
        for customer_id in (11091, 11176):
            username = f"customer-{customer_id}"
            password_path = PROJECT_ROOT / ".local" / "auth" / f"customer-{customer_id}.password"
            password = password_path.read_text(encoding="utf-8").strip()
            require(bool(password), f"账号密码文件为空：{username}")
            self.passwords.append(password)
            client = httpx.AsyncClient(base_url=BASE_URL, timeout=20, trust_env=False)
            response = await client.post("/api/login", json={"username": username, "password": password})
            require(response.status_code == 200, f"账号登录失败：{username}，HTTP {response.status_code}")
            user = response.json()
            require(user.get("username") == username, f"账号登录返回的用户名不匹配：{username}")
            self.user_ids[customer_id] = user["id"]
            self.clients[customer_id] = client
        return {
            customer_id: await self.get_identity(client)
            for customer_id, client in self.clients.items()
        }

    @staticmethod
    async def get_identity(client: httpx.AsyncClient) -> dict:
        response = await client.get("/api/customer/identity")
        require(response.status_code == 200, f"读取客户身份失败，HTTP {response.status_code}")
        return response.json()

    async def api_create_conversation(self, customer_id: int, title: str) -> dict:
        response = await self.clients[customer_id].post(
            "/api/customer/conversations", json={"title": title},
        )
        require(response.status_code == 200, f"创建会话失败，HTTP {response.status_code}")
        conversation = response.json()
        self.add_conversation(conversation["id"])
        return conversation

    async def submit_message(self, customer_id: int, conversation_id: str,
                             request_id: str, message: str) -> httpx.Response:
        return await self.clients[customer_id].post(
            f"/api/customer/conversations/{conversation_id}/messages",
            json={"request_id": request_id, "message": message},
        )

    async def wait_turn(self, customer_id: int, turn_id: str, timeout: float = 150) -> dict:
        self.add_turn(turn_id)
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            self.ensure_request_limit()
            response = await self.clients[customer_id].get(f"/api/customer/turns/{turn_id}")
            require(response.status_code == 200, f"读取消息状态失败，HTTP {response.status_code}")
            state = response.json()
            if state["status"] in {"completed", "failed", "cancelled", "paused"}:
                self.report.setdefault("turn_observations", {})[turn_id] = self.db_turn(turn_id)
                self.report.setdefault("request_observations", {})[turn_id] = self.db_requests(turn_id)
                self.write_report()
                return state
            await asyncio.sleep(0.08)
        raise TimeoutError(f"等待消息结束超时：{turn_id}")

    async def wait_sent_with_socket(self, customer_id: int, turn_id: str,
                                    timeout: float = 90) -> tuple[dict, str]:
        self.add_turn(turn_id)
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            self.ensure_request_limit()
            turn = self.db_turn(turn_id)
            requests = self.provider_rows_live(turn_id)
            sent = [row for row in requests if row["status"] == "sent" and row["model"] == "deepseek-flash"]
            if turn["status"] == "running" and turn["started_at"] and sent:
                process = self.service_process
                require(process is not None and process.poll() is None, "8002服务进程已停止")
                lsof = await asyncio.create_subprocess_exec(
                    "lsof", "-nP", "-a", "-p", str(process.pid),
                    "-iTCP", "-sTCP:ESTABLISHED",
                    cwd=PROJECT_ROOT,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout_bytes, stderr_bytes = await lsof.communicate()
                lsof_stdout = stdout_bytes.decode("utf-8", errors="replace")
                lsof_stderr = stderr_bytes.decode("utf-8", errors="replace")
                socket_path = self.evidence_dir / f"sockets-{turn_id}.txt"
                socket_path.write_text(lsof_stdout + lsof_stderr, encoding="utf-8")
                self.add_raw_output(socket_path)
                established = [line for line in lsof_stdout.splitlines()
                               if "->" in line and "(ESTABLISHED)" in line
                               and ":55432" not in line and ":8002" not in line]
                if lsof.returncode == 0 and established:
                    return {
                        "turn": turn,
                        "requests": requests,
                        "provider_request": sent[0],
                        "service_pid": process.pid,
                        "established_external_socket_count": len(established),
                        "socket_evidence": str(socket_path.relative_to(PROJECT_ROOT)),
                    }, lsof_stdout + lsof_stderr
            await asyncio.sleep(0.04)
        raise TimeoutError(f"未观测到发送中的DeepSeek请求及本地外连：{turn_id}")

    async def run_parallel_identity_and_dedupe(self):
        self.current_phase = "parallel_sessions_and_request_deduplication"
        self.require_request_capacity(6)
        first = await self.api_create_conversation(11091, "CUS-04并发与去重")
        second = await self.api_create_conversation(11176, "CUS-04客户并发")
        request_one = str(uuid4())
        request_two = str(uuid4())
        request_other_customer = str(uuid4())
        message = "请用简短中文向我问好。"
        responses = await asyncio.gather(
            self.submit_message(11091, first["id"], request_one, message),
            self.submit_message(11091, first["id"], request_two, message),
            self.submit_message(11176, second["id"], request_other_customer, message),
        )
        first_results = [responses[0], responses[1]]
        accepted = [response for response in first_results if response.status_code == 200]
        rejected = [response for response in first_results if response.status_code == 409]
        require(len(accepted) == 1 and len(rejected) == 1,
                "同会话并发消息没有产生一个成功响应和一个409响应")
        require(responses[2].status_code == 200, "第二客户并发提交失败")
        winning_turn = accepted[0].json()
        other_turn = responses[2].json()
        require(winning_turn["request_id"] in {request_one, request_two}, "成功消息的请求编号不匹配")
        duplicate = await self.submit_message(
            11091, first["id"], winning_turn["request_id"], message,
        )
        require(duplicate.status_code == 200, "相同request_id重试没有复用消息")
        require(duplicate.json()["id"] == winning_turn["id"], "相同request_id重试生成了新消息")
        require(responses[2].json()["request_id"] == request_other_customer,
                "第二客户消息的请求编号不匹配")
        self.add_turn(winning_turn["id"])
        self.add_turn(other_turn["id"])
        completed = await asyncio.gather(
            self.wait_turn(11091, winning_turn["id"]),
            self.wait_turn(11176, other_turn["id"]),
        )
        require(all(item["status"] == "completed" for item in completed),
                "两个客户的并发消息没有都完成")
        with self.maker() as session:
            winner_turn_count = session.scalar(select(func.count()).select_from(CustomerTurn).where(
                CustomerTurn.conversation_id == first["id"],
                CustomerTurn.request_id == winning_turn["request_id"],
            ))
        require(winner_turn_count == 1, "相同request_id在数据库中对应多条消息")

        identity_evidence = {}
        final_by_id = {state["id"]: state for state in completed}
        for customer_id, turn in ((11091, winning_turn), (11176, other_turn)):
            rows = self.request_rows(turn["id"])
            require(rows, f"客户{customer_id}的消息没有保存提供商请求")
            first_row = next(row for row in rows if row["model"] == "deepseek-flash")
            with self.maker() as session:
                provider = session.get(ProviderRequest, first_row["id"])
                body = provider.request_body
            context = self.extract_customer_context(body)
            own_numbers = {item["order_number"] for item in context["orders"]}
            other_id = 11176 if customer_id == 11091 else 11091
            other_orders = await self.clients[other_id].get("/api/customer/orders", params={"limit": 20})
            require(other_orders.status_code == 200, "读取另一个客户的本人订单失败")
            other_numbers = {item["order_number"] for item in other_orders.json()["orders"]}
            require(context["identity"]["customer_id"] == customer_id,
                    f"客户{customer_id}的模型上下文绑定身份错误")
            require(not (own_numbers & other_numbers), "两个客户的订单编号集合发生交叉")
            encoded_context = json.dumps(context, ensure_ascii=False)
            require(not any(number in encoded_context for number in other_numbers),
                    f"客户{customer_id}的模型上下文包含另一客户订单")
            final_result = final_by_id[turn["id"]].get("result") or {}
            encoded_result = json.dumps(final_result, ensure_ascii=False)
            require(not any(number in encoded_result for number in other_numbers),
                    f"客户{customer_id}的最终答复包含另一客户订单")
            identity_evidence[str(customer_id)] = {
                "turn_id": turn["id"],
                "request_id": turn["request_id"],
                "context_customer_id": context["identity"]["customer_id"],
                "context_order_count": len(own_numbers),
                "other_customer_order_count": len(other_numbers),
                "other_customer_order_numbers_present": False,
                "other_customer_order_numbers_in_result": False,
                "provider_request_ids": [row["id"] for row in rows],
            }

        order_access = {}
        for customer_id in (11091, 11176):
            own = await self.clients[customer_id].get("/api/customer/orders", params={"limit": 1})
            require(own.status_code == 200 and own.json()["orders"],
                    f"客户{customer_id}没有读取到本人订单")
            own_number = own.json()["orders"][0]["order_number"]
            other_id = 11176 if customer_id == 11091 else 11091
            other_orders = await self.clients[other_id].get("/api/customer/orders", params={"limit": 1})
            other_number = other_orders.json()["orders"][0]["order_number"]
            own_detail = await self.clients[customer_id].get(f"/api/customer/orders/{own_number}")
            other_detail = await self.clients[customer_id].get(f"/api/customer/orders/{other_number}")
            require(own_detail.status_code == 200, f"客户{customer_id}读取本人订单失败")
            require(other_detail.status_code == 404, f"客户{customer_id}读取他人订单没有返回404")
            order_access[str(customer_id)] = {
                "own_order_number": own_number,
                "own_http_status": own_detail.status_code,
                "other_order_number": other_number,
                "other_http_status": other_detail.status_code,
            }

        self.pass_check("parallel_sessions_and_request_deduplication", {
            "same_conversation_different_request_ids": {
                "successful_responses": len(accepted),
                "http_409_responses": len(rejected),
            },
            "same_request_id_reuses_turn_id": duplicate.json()["id"] == winning_turn["id"],
            "parallel_customer_turns_completed": [item["id"] for item in (winning_turn, other_turn)],
            "identity_contexts": identity_evidence,
            "order_access": order_access,
        })

    @staticmethod
    def extract_customer_context(body: dict) -> dict:
        for message in body.get("messages", []):
            content = message.get("content")
            if message.get("role") != "user" or not isinstance(content, str):
                continue
            prefix = "服务端上下文："
            marker = "\n当前客户消息："
            if prefix in content and marker in content:
                raw = content.split(prefix, 1)[1].split(marker, 1)[0]
                return json.loads(raw)
        raise AssertionError("提供商请求没有包含可读取的服务端客户上下文")

    async def run_cancel_and_lock_wait(self):
        self.current_phase = "cancel_sent_request_and_wait_for_session_lock"
        self.require_request_capacity(4)
        conversation = await self.api_create_conversation(11091, "CUS-04取消与锁竞争")
        request_id = str(uuid4())
        message = "请用简短中文向我问好。"
        response = await self.submit_message(11091, conversation["id"], request_id, message)
        require(response.status_code == 200, f"创建取消检查消息失败，HTTP {response.status_code}")
        old_turn = response.json()
        self.add_turn(old_turn["id"])
        sent_observation, _socket_output = await self.wait_sent_with_socket(11091, old_turn["id"])
        require(sent_observation["established_external_socket_count"] > 0,
                "没有发现服务进程正在处理的外部HTTP连接")
        old_lock_owners = self.lock_key_owners(conversation["id"])
        require(old_lock_owners, "ProviderRequest.sent时没有发现会话advisory锁")
        old_process_pid = old_lock_owners[0]
        cancel_response = await self.clients[11091].post(f"/api/customer/turns/{old_turn['id']}/cancel")
        require(cancel_response.status_code == 200, f"取消消息失败，HTTP {cancel_response.status_code}")
        require(cancel_response.json()["status"] == "cancelled", "取消接口没有保存cancelled状态")

        next_request_id = str(uuid4())
        next_response = await self.submit_message(
            11091, conversation["id"], next_request_id, "请用简短中文向我问好。",
        )
        require(next_response.status_code == 200, f"取消后提交新消息失败，HTTP {next_response.status_code}")
        next_turn = next_response.json()
        self.add_turn(next_turn["id"])
        next_lock_owners = self.lock_key_owners(conversation["id"])
        next_state = self.db_turn(next_turn["id"])
        require(next_state["status"] == "queued" and old_process_pid in next_lock_owners,
                "取消后新消息提交时没有观察到旧turn仍占用会话锁")

        deadline = asyncio.get_running_loop().time() + 30
        while asyncio.get_running_loop().time() < deadline:
            old_rows = self.provider_rows_live(old_turn["id"])
            old_request = next((row for row in old_rows if row["id"] == sent_observation["provider_request"]["id"]), None)
            if old_request and old_request["status"] == "unknown":
                break
            await asyncio.sleep(0.04)
        require(old_request is not None and old_request["status"] == "unknown",
                "取消期间的已发送请求没有保留为unknown")
        require(old_request["error"] == "本地请求已终止，提供商用量未知",
                "取消期间未知用量说明不匹配")
        canceled_state = self.db_turn(old_turn["id"])
        require(canceled_state["status"] == "cancelled" and not canceled_state["has_result"],
                "取消后消息生成了结果或状态错误")
        require(canceled_state["model_calls"] == 1, "取消后的旧消息出现新增模型调用")
        unknown_rows = self.db_requests(old_turn["id"])
        require(len(unknown_rows) == 1, "取消后的旧消息新增了提供商请求")
        require(unknown_rows[0]["reservation_cny"] == old_request["reservation_cny"],
                "未知用量预留金额发生变化")
        require(unknown_rows[0]["charged_cny"] is None and unknown_rows[0]["usage"] is None,
                "取消时写入了未经提供商确认的用量或费用")
        with self.maker() as session:
            answer_events = session.scalars(select(CustomerEvent).where(
                CustomerEvent.conversation_id == conversation["id"],
                CustomerEvent.turn_id == old_turn["id"],
                CustomerEvent.type == "answer",
            )).all()
        require(not answer_events, "取消后会话事件中出现最终答复")
        new_state = await self.wait_turn(11091, next_turn["id"])
        require(new_state["status"] == "completed", "等待会话锁的新消息没有完成")
        new_requests = self.db_requests(next_turn["id"])
        require(new_requests and all(row["run_id"] == next_turn["id"] for row in new_requests),
                "取消后新消息没有按独立run_id记录请求")
        require(all(row["id"] != old_request["id"] for row in new_requests),
                "新消息复用了旧的提供商请求编号")
        self.pass_check("cancel_sent_request_and_wait_for_session_lock", {
            "sent_state_observation": sent_observation,
            "cancel_http_status": cancel_response.status_code,
            "old_turn_after_cancel": canceled_state,
            "old_request_after_cancel": unknown_rows[0],
            "unknown_reservation_retained": True,
            "usage_is_unknown": True,
            "no_final_answer": True,
            "answer_event_count": len(answer_events),
            "immediate_new_turn_id": next_turn["id"],
            "old_lock_owner_pid": old_process_pid,
            "lock_owners_after_new_submit": next_lock_owners,
            "new_turn_state_at_contention": next_state,
            "new_turn_final_state": self.db_turn(next_turn["id"]),
            "new_turn_request_ids": [row["id"] for row in new_requests],
        })

    def create_offline_turn(self, owner_id: str, title: str, message: str) -> tuple[dict, dict]:
        service = CustomerService(self.settings)
        try:
            conversation = service.create_conversation(owner_id, title)
            self.add_conversation(conversation["id"])
            turn = service.submit(conversation["id"], owner_id, str(uuid4()), message)
            self.add_turn(turn["id"])
            require(turn["status"] == "queued" and turn["model_calls"] == 0,
                    "离线消息没有保持queued未运行状态")
            return conversation, turn
        finally:
            service.engine.dispose()

    def run_checkpoint_child(self, turn_id: str, node: str) -> dict:
        command = [".venv/bin/python", "scripts/customer_checkpoint.py", turn_id]
        if node == "ticket":
            command.extend(["--after", "tools", "--until-ticket"])
        else:
            command.extend(["--after", node])
        result = subprocess.run(
            command, cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=180,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}, check=False,
        )
        stdout_path = self.evidence_dir / f"checkpoint-{node}-{turn_id}.stdout.txt"
        stderr_path = self.evidence_dir / f"checkpoint-{node}-{turn_id}.stderr.txt"
        stdout_path.write_text(result.stdout, encoding="utf-8")
        stderr_path.write_text(result.stderr, encoding="utf-8")
        self.add_raw_output(stdout_path)
        self.add_raw_output(stderr_path)
        require(result.returncode == 0,
                f"真实checkpoint子进程失败：{node}，exit_code={result.returncode}，详见原始输出")
        data = json.loads(result.stdout)
        require(data["turn_id"] == turn_id and data["status"] == "paused",
                "checkpoint子进程没有暂停指定消息")
        require(data["next"], "checkpoint的snapshot.next为空")
        return data

    async def resume_turn(self, customer_id: int, turn_id: str) -> dict:
        response = await self.clients[customer_id].post(f"/api/customer/turns/{turn_id}/resume")
        require(response.status_code == 200, f"恢复消息失败，HTTP {response.status_code}")
        require(response.json()["status"] in {"queued", "running"}, "恢复接口没有排入消息")
        final = await self.wait_turn(customer_id, turn_id)
        require(final["status"] == "completed", f"恢复消息没有完成：{final['status']}")
        return final

    async def run_model_checkpoint(self):
        self.current_phase = "cross_process_resume_after_model"
        self.require_request_capacity(3)
        owner_id = self.user_ids[11091]
        _conversation, turn = self.create_offline_turn(
            owner_id, "CUS-04模型检查点恢复", "请查询我最近一笔订单的完整明细，包括订单金额。",
        )
        checkpoint = self.run_checkpoint_child(turn["id"], "model")
        require(checkpoint["next"], "模型检查点没有待执行节点")
        paused = self.db_turn(turn["id"])
        first_requests = self.db_requests(turn["id"])
        completed_first = [row for row in first_requests
                           if row["status"] == "completed" and row["model"] == "deepseek-flash"]
        require(paused["status"] == "paused" and completed_first,
                "模型检查点没有保留第一条已完成的提供商请求")
        first_request = completed_first[0]
        require(checkpoint["model_calls"] >= 1 and paused["model_calls"] >= checkpoint["model_calls"],
                "模型检查点计数没有保存")
        self.report.setdefault("checkpoint_evidence", {})[turn["id"]] = {
            "phase": "after_model",
            "checkpoint_stdout": str((self.evidence_dir / f"checkpoint-model-{turn['id']}.stdout.txt").relative_to(PROJECT_ROOT)),
            "snapshot_next": checkpoint["next"],
            "turn_at_pause": paused,
            "requests_at_pause": first_requests,
        }
        self.write_report()
        final = await self.resume_turn(11091, turn["id"])
        after_requests = self.db_requests(turn["id"])
        preserved = [row for row in after_requests if row["id"] == first_request["id"]]
        final_db = self.db_turn(turn["id"])
        require(len(preserved) == 1 and preserved[0]["status"] == "completed",
                "恢复后第一条提供商请求编号或状态发生变化")
        preserved_fields = ("id", "status", "reservation_cny", "charged_cny", "usage", "provider_request_id")
        require(all(preserved[0][key] == first_request[key] for key in preserved_fields),
                "恢复后第一条提供商请求的费用或用量记录发生变化")
        fee_at_pause = sum((Decimal(row["charged_cny"]) for row in first_requests
                            if row["charged_cny"] is not None), Decimal(0))
        fee_after_resume = sum((Decimal(row["charged_cny"]) for row in after_requests
                                if row["charged_cny"] is not None), Decimal(0))
        require(fee_after_resume >= fee_at_pause, "模型恢复后累计费用减少")
        require(final_db["model_calls"] >= paused["model_calls"], "恢复后模型累计次数重置")
        require(final_db["tool_calls"] >= paused["tool_calls"], "恢复后工具累计次数重置")
        require(Decimal(final_db["elapsed_seconds"]) >= Decimal(paused["elapsed_seconds"]),
                "恢复后累计耗时重置")
        require(sum(1 for row in after_requests if row["id"] == first_request["id"]) == 1,
                "恢复重复创建了检查点前的模型请求")
        self.pass_check("cross_process_resume_after_model", {
            "turn_id": turn["id"],
            "snapshot_next": checkpoint["next"],
            "turn_at_pause": paused,
            "turn_after_resume": final_db,
            "first_completed_request_id": first_request["id"],
            "first_request_after_resume": preserved[0],
            "charged_cny_at_pause": str(fee_at_pause),
            "charged_cny_after_resume": str(fee_after_resume),
            "request_ids_after_resume": [row["id"] for row in after_requests],
            "model_calls_preserved": final_db["model_calls"] >= paused["model_calls"],
            "tool_calls_preserved": final_db["tool_calls"] >= paused["tool_calls"],
            "elapsed_seconds_preserved": Decimal(final_db["elapsed_seconds"]) >= Decimal(paused["elapsed_seconds"]),
            "final_http_status": final["status"],
        })

    def find_resume_ticket_turn(self) -> str:
        matches = []
        with self.maker() as session:
            for turn_id in sorted(self.turn_ids):
                turn = session.get(CustomerTurn, turn_id)
                if turn is None or turn.status != "paused" or turn.model_calls != 1 or turn.tool_calls != 1:
                    continue
                if not explicit_ticket_request(turn.message) or "请实际提交工单并返回真实编号" not in turn.message:
                    continue
                tools = session.scalars(select(CustomerToolCall).where(
                    CustomerToolCall.turn_id == turn_id,
                ).order_by(CustomerToolCall.created_at, CustomerToolCall.id)).all()
                tickets = session.scalars(select(CustomerTicket).where(
                    CustomerTicket.turn_id == turn_id,
                )).all()
                if [row.name for row in tools] == ["read_order"] and not tickets:
                    matches.append(turn_id)
        require(len(matches) == 1, "原检查报告中没有唯一可恢复的工单消息")
        return matches[0]

    async def run_ticket_checkpoint(self, resume_turn_id: str | None = None):
        self.current_phase = "cross_process_resume_after_ticket_tool"
        self.require_request_capacity(3)
        owner_id = self.user_ids[11091]
        client = self.clients[11091]
        orders_response = await client.get("/api/customer/orders", params={"limit": 1})
        require(orders_response.status_code == 200 and orders_response.json()["orders"],
                "工单检查无法读取本人订单")
        order_number = orders_response.json()["orders"][0]["order_number"]
        if resume_turn_id:
            turn = self.db_turn(resume_turn_id)
            require(turn["status"] == "paused", "复用的工单消息当前没有处于paused状态")
            require(explicit_ticket_request(turn["message"]), "复用的工单消息没有通过明确请求识别")
            require(order_number in turn["message"], "复用的工单消息没有请求本人当前订单")
            conversation_response = await client.get(
                f"/api/customer/conversations/{turn['conversation_id']}",
            )
            require(conversation_response.status_code == 200,
                    "工单检查账号无法读取原会话")
            with self.maker() as session:
                existing_tools = session.scalars(select(CustomerToolCall).where(
                    CustomerToolCall.turn_id == resume_turn_id,
                ).order_by(CustomerToolCall.created_at, CustomerToolCall.id)).all()
                existing_tickets = session.scalars(select(CustomerTicket).where(
                    CustomerTicket.turn_id == resume_turn_id,
                )).all()
            require([row.name for row in existing_tools] == ["read_order"] and not existing_tickets,
                    "原工单消息的paused checkpoint状态与失败报告不符")
            self.add_turn(resume_turn_id)
            turn_id = resume_turn_id
        else:
            message = (
                f"请为订单{order_number}创建人工工单，说明：商品存在质量问题，请人工联系我。"
                "请实际提交工单并返回真实编号。"
            )
            require(explicit_ticket_request(message), "工单检查消息没有通过明确请求识别")
            _conversation, created_turn = self.create_offline_turn(
                owner_id, "CUS-04工单检查点恢复", message,
            )
            turn_id = created_turn["id"]
        checkpoint = self.run_checkpoint_child(turn_id, "ticket")
        paused = self.db_turn(turn_id)
        with self.maker() as session:
            tickets = session.scalars(select(CustomerTicket).where(
                CustomerTicket.turn_id == turn_id,
            ).order_by(CustomerTicket.created_at, CustomerTicket.id)).all()
            tools = session.scalars(select(CustomerToolCall).where(
                CustomerToolCall.turn_id == turn_id,
            ).order_by(CustomerToolCall.created_at, CustomerToolCall.id)).all()
            ticket_evidence = [{
                "id": row.id,
                "number": row.number,
                "conversation_id": row.conversation_id,
                "turn_id": row.turn_id,
                "authorization_id": row.authorization_id,
                "order_number": row.order_number,
                "reason": row.reason,
                "status": row.status,
                "created_at": row.created_at.isoformat(),
            } for row in tickets]
            tool_evidence = [{
                "id": row.id,
                "call_id": row.call_id,
                "name": row.name,
                "arguments": row.arguments,
                "result": row.result,
                "error": row.error,
            } for row in tools]
        require(paused["status"] == "paused" and checkpoint["next"],
                "工单工具检查点没有保存未完成图状态")
        require(len(ticket_evidence) == 1 and ticket_evidence[0]["order_number"] == order_number,
                "工具检查点前没有恰好写入一条本人订单工单")
        create_tools = [row for row in tool_evidence if row["name"] == "create_ticket"]
        require(len(create_tools) == 1 and create_tools[0]["result"],
                "工单工具检查点没有保存真实create_ticket结果")
        before_ticket = ticket_evidence[0]
        before_tool_ids = [row["id"] for row in tool_evidence]
        before_requests = self.db_requests(turn_id)
        fee_at_pause = sum((Decimal(row["charged_cny"]) for row in before_requests
                            if row["charged_cny"] is not None), Decimal(0))
        require(before_requests and checkpoint["model_calls"] <= 3,
                "工单检查点的模型请求计数无效")
        self.report.setdefault("checkpoint_evidence", {})[turn_id] = {
            "phase": "after_create_ticket",
            "snapshot_next": checkpoint["next"],
            "checkpoint_subprocess": checkpoint,
            "turn_at_pause": paused,
            "tickets_at_pause": ticket_evidence,
            "tools_at_pause": tool_evidence,
            "requests_at_pause": before_requests,
        }
        self.write_report()
        final = await self.resume_turn(11091, turn_id)
        final_db = self.db_turn(turn_id)
        after_requests = self.db_requests(turn_id)
        fee_after_resume = sum((Decimal(row["charged_cny"]) for row in after_requests
                                if row["charged_cny"] is not None), Decimal(0))
        with self.maker() as session:
            after_tickets = session.scalars(select(CustomerTicket).where(
                CustomerTicket.turn_id == turn_id,
            ).order_by(CustomerTicket.created_at, CustomerTicket.id)).all()
            after_tools = session.scalars(select(CustomerToolCall).where(
                CustomerToolCall.turn_id == turn_id,
            ).order_by(CustomerToolCall.created_at, CustomerToolCall.id)).all()
            after_ticket_ids = [row.id for row in after_tickets]
            after_ticket_numbers = [row.number for row in after_tickets]
            after_tool_ids = [row.id for row in after_tools]
            after_tool_names = [row.name for row in after_tools]
            same_ticket = next((row for row in after_tickets if row.id == before_ticket["id"]), None)
            require(same_ticket is not None, "恢复后原工单记录丢失")
            api_ticket = session.get(CustomerTicket, before_ticket["id"])
            require(api_ticket is not None, "恢复后无法读取原工单")
            ticket_number = api_ticket.number
        require(len(after_ticket_ids) == 1 and after_ticket_ids[0] == before_ticket["id"],
                "恢复后工单数量或工单编号发生变化")
        require(set(before_tool_ids).issubset(after_tool_ids),
                "恢复后检查点前已完成工具的记录缺失")
        require(after_tool_names.count("create_ticket") == 1,
                "恢复后create_ticket执行次数不等于一次")
        require(final_db["tool_calls"] >= paused["tool_calls"], "工单恢复后工具累计次数重置")
        require(final_db["model_calls"] >= paused["model_calls"], "工单恢复后模型累计次数重置")
        require(Decimal(final_db["elapsed_seconds"]) >= Decimal(paused["elapsed_seconds"]),
                "工单恢复后累计耗时重置")
        require(fee_after_resume >= fee_at_pause, "工单恢复后累计费用减少")
        ticket_response = await client.get(f"/api/customer/tickets/{ticket_number}")
        require(ticket_response.status_code == 200, "恢复后API没有读取到同一工单")
        require(ticket_response.json()["id"] == before_ticket["id"], "恢复后API返回了不同工单")
        self.pass_check("cross_process_resume_after_ticket_tool", {
            "turn_id": turn_id,
            "original_message_explicitly_authorized": True,
            "requested_order_number": order_number,
            "snapshot_next": checkpoint["next"],
            "turn_at_pause": paused,
            "turn_after_resume": final_db,
            "ticket_at_pause": before_ticket,
            "ticket_after_resume_ids": after_ticket_ids,
            "ticket_after_resume_numbers": after_ticket_numbers,
            "ticket_count_after_resume": len(after_ticket_ids),
            "tool_ids_at_pause": before_tool_ids,
            "tool_ids_after_resume": after_tool_ids,
            "tool_names_after_resume": after_tool_names,
            "original_tool_ids_preserved": set(before_tool_ids).issubset(after_tool_ids),
            "create_ticket_call_count_after_resume": after_tool_names.count("create_ticket"),
            "request_ids_after_resume": [row["id"] for row in after_requests],
            "charged_cny_at_pause": str(fee_at_pause),
            "charged_cny_after_resume": str(fee_after_resume),
            "same_ticket_returned_by_api": ticket_response.json()["id"] == before_ticket["id"],
            "cumulative_counters_and_time_preserved": True,
            "final_status": final["status"],
        })

    def budget_test_engine(self, suffix: str):
        schema = f"cus04_{suffix}_{self.run_id.rsplit('-', 1)[-1]}"
        require(len(schema) < 63 and schema.replace("_", "").isalnum(), "shop_test schema名称无效")
        settings_url = make_url(self.settings.database_url)
        test_url = settings_url.set(database="shop_test")
        admin_engine = create_engine(test_url, pool_pre_ping=True)
        with admin_engine.begin() as connection:
            connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public"))
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        engine = create_engine(
            test_url,
            pool_pre_ping=True,
            connect_args={"options": f"-csearch_path={schema},public"},
        )
        Base.metadata.create_all(engine)
        return schema, engine, sessionmaker(engine, expire_on_commit=False), admin_engine

    def run_budget_checks(self):
        schema_scope, engine_scope, maker_scope, admin_scope = self.budget_test_engine("budget")
        try:
            service = BudgetService(maker_scope)
            with maker_scope.begin() as session:
                session.add(BudgetAccount(id="project", spent_cny=Decimal(0), reserved_cny=Decimal(0)))
            seed = service.reserve(
                task_id=None, run_id=str(uuid4()), step_key="cus04-scope-boundary-seed",
                service="deepseek", model="deepseek-flash",
                request_body={"evidence": "reserved locally; provider not called"},
                input_limit=14_999_000, output_limit=100,
                verification=False, scope="customer",
            )
            require(seed.reservation_cny < Decimal(30), "客服预算边界准备值超过30元")
            with maker_scope() as session:
                before_budget = session.get(BudgetAccount, "project")
                before_reserved = Decimal(before_budget.reserved_cny)
                before_spent = Decimal(before_budget.spent_cny)
                before_rows = session.scalar(select(func.count()).select_from(ProviderRequest))
            rejected_scope = False
            try:
                service.reserve(
                    task_id=None, run_id=str(uuid4()), step_key="cus04-customer-budget-reject",
                    service="deepseek", model="deepseek-flash",
                    request_body={"evidence": "budget check only; provider not called"},
                    input_limit=1, output_limit=1000,
                    verification=False, scope="customer",
                )
            except BudgetExceeded:
                rejected_scope = True
            require(rejected_scope, "客服30元预算边界没有拒绝超额预留")
            with maker_scope() as session:
                after_budget = session.get(BudgetAccount, "project")
                after_rows = session.scalar(select(func.count()).select_from(ProviderRequest))
                saved_seed = session.get(ProviderRequest, seed.id)
            require(Decimal(after_budget.reserved_cny) == before_reserved
                    and Decimal(after_budget.spent_cny) == before_spent,
                    "预算拒绝后账户状态发生变化")
            require(after_rows == before_rows and saved_seed.status == "reserved",
                    "预算拒绝创建了额外提供商请求或修改了原预留")

            unsent = service.reserve(
                task_id=None, run_id=str(uuid4()), step_key="cus04-unsent-cancel",
                service="deepseek", model="deepseek-flash",
                request_body={"evidence": "real BudgetService reservation; provider not called"},
                input_limit=100, output_limit=100,
                verification=False, scope="customer",
            )
            with maker_scope() as session:
                reserved_before_cancel = Decimal(session.get(BudgetAccount, "project").reserved_cny)
                spent_before_cancel = Decimal(session.get(BudgetAccount, "project").spent_cny)
            service.cancel_unsent(unsent.id, "CUS-04未发送取消检查")
            with maker_scope() as session:
                canceled = session.get(ProviderRequest, unsent.id)
                after_cancel = session.get(BudgetAccount, "project")
                reserved_after_cancel = Decimal(after_cancel.reserved_cny)
                spent_after_cancel = Decimal(after_cancel.spent_cny)
            require(canceled.status == "cancelled" and canceled.charged_cny == Decimal(0),
                    "未发送取消没有保存cancelled和零费用")
            require(canceled.usage is None and canceled.provider_request_id is None,
                    "未发送取消写入了提供商用量或请求编号")
            require(reserved_after_cancel == reserved_before_cancel - unsent.reservation_cny,
                    "未发送取消没有释放本地预留")
            require(spent_after_cancel == spent_before_cancel, "未发送取消修改了已发生费用")
            scope_evidence = {
                "database": "shop_test",
                "schema": schema_scope,
                "seed_request_id": seed.id,
                "seed_status": "reserved",
                "seed_provider_call_made": False,
                "seed_usage": None,
                "seed_reservation_cny": str(seed.reservation_cny),
                "customer_limit_rejected": rejected_scope,
                "request_count_before_rejected_reserve": before_rows,
                "request_count_after_rejected_reserve": after_rows,
                "budget_before_rejected_reserve": {
                    "spent_cny": str(before_spent), "reserved_cny": str(before_reserved),
                },
                "budget_after_rejected_reserve": {
                    "spent_cny": str(after_budget.spent_cny),
                    "reserved_cny": str(after_budget.reserved_cny),
                },
                "unsent_request_id": unsent.id,
                "unsent_cancel_status": canceled.status,
                "unsent_cancel_usage": canceled.usage,
                "unsent_cancel_charged_cny": str(canceled.charged_cny),
                "unsent_cancel_reserved_before_cny": str(reserved_before_cancel),
                "unsent_cancel_reserved_after_cny": str(reserved_after_cancel),
                "unsent_cancel_spent_before_cny": str(spent_before_cancel),
                "unsent_cancel_spent_after_cny": str(spent_after_cancel),
            }
        finally:
            engine_scope.dispose()
            admin_scope.dispose()

        schema_project, engine_project, maker_project, admin_project = self.budget_test_engine("project")
        try:
            with maker_project.begin() as session:
                session.add(BudgetAccount(
                    id="project", spent_cny=Decimal("269.99999999"), reserved_cny=Decimal(0),
                ))
            project_service = BudgetService(maker_project)
            before_count = self.count_test_provider_requests(maker_project)
            rejected_project = False
            try:
                project_service.reserve(
                    task_id=None, run_id=str(uuid4()), step_key="cus04-project-budget-reject",
                    service="deepseek", model="deepseek-flash",
                    request_body={"evidence": "project budget check only; provider not called"},
                    input_limit=1, output_limit=1,
                    verification=False, scope="customer",
                )
            except BudgetExceeded:
                rejected_project = True
            require(rejected_project, "项目270元新请求阈值没有拒绝超额预留")
            after_count = self.count_test_provider_requests(maker_project)
            with maker_project() as session:
                account = session.get(BudgetAccount, "project")
                account_after = {"spent_cny": str(account.spent_cny), "reserved_cny": str(account.reserved_cny)}
            require(before_count == after_count == 0, "项目预算拒绝后新增了请求记录")
            require(Decimal(account_after["spent_cny"]) == Decimal("269.99999999")
                    and Decimal(account_after["reserved_cny"]) == Decimal(0),
                    "项目预算拒绝后账户状态发生变化")
            self.pass_check("database_budget_and_unsent_cancel", {
                "customer_scope": scope_evidence,
                "project_budget": {
                    "database": "shop_test",
                    "schema": schema_project,
                    "budget_before_reserve": {"spent_cny": "269.99999999", "reserved_cny": "0"},
                    "budget_after_rejection": account_after,
                    "provider_request_count_before": before_count,
                    "provider_request_count_after": after_count,
                    "rejected_by_actual_budget_service": rejected_project,
                    "provider_call_made": False,
                    "usage": None,
                },
            })
        finally:
            engine_project.dispose()
            admin_project.dispose()

    @staticmethod
    def count_test_provider_requests(maker) -> int:
        with maker() as session:
            return session.scalar(select(func.count()).select_from(ProviderRequest)) or 0

    def find_listener_pid(self) -> int:
        result = subprocess.run(
            ["lsof", "-t", "-nP", "-iTCP:8002", "-sTCP:LISTEN"],
            cwd=PROJECT_ROOT, capture_output=True, text=True, check=True,
        )
        pids = {int(line) for line in result.stdout.splitlines() if line.strip()}
        require(pids == {self.service_process.pid}, "8002监听进程与本次启动进程不一致")
        return next(iter(pids))

    async def run_process_restart(self):
        self.current_phase = "sudden_process_termination_and_restart"
        self.require_request_capacity(4)
        conversation = await self.api_create_conversation(11091, "CUS-04进程中断恢复")
        response = await self.submit_message(
            11091, conversation["id"], str(uuid4()), "请问当前商品的实时库存是多少？",
        )
        require(response.status_code == 200, f"创建进程中断检查消息失败，HTTP {response.status_code}")
        turn = response.json()
        self.add_turn(turn["id"])
        sent_observation, _ = await self.wait_sent_with_socket(11091, turn["id"])
        observed_pid = sent_observation["service_pid"]
        require(observed_pid == self.service_process.pid, "发送中的请求不属于本次8002服务")
        before = sent_observation["turn"]
        original_request = sent_observation["provider_request"]
        require(before["model_calls"] == 1, "进程中断前的模型累计次数不是1")
        self.stop_service("实际DeepSeek HTTP请求sent期间突然终止", force=True)
        after_kill = self.provider_rows_live(turn["id"])
        old_after_kill = next(row for row in after_kill if row["id"] == original_request["id"])
        require(old_after_kill["status"] == "sent", "进程终止后原请求没有保留发送中记录")
        self.report.setdefault("process_interruption", {})[turn["id"]] = {
            "service_pid": observed_pid,
            "turn_before_kill": before,
            "request_before_kill": original_request,
            "request_immediately_after_kill": old_after_kill,
            "termination": "SIGKILL",
        }
        self.write_report()

        await self.wait_for_other_turns("sudden_restart")
        self.start_service()
        recovery_deadline = asyncio.get_running_loop().time() + 150
        recovered = None
        while asyncio.get_running_loop().time() < recovery_deadline:
            self.ensure_request_limit()
            current = self.db_turn(turn["id"])
            rows = self.db_requests(turn["id"])
            recovered_original = next((row for row in rows if row["id"] == original_request["id"]), None)
            if recovered_original and recovered_original["status"] == "unknown" \
                    and current["status"] in {"completed", "failed", "paused", "cancelled"}:
                recovered = {"turn": current, "requests": rows, "original": recovered_original}
                break
            await asyncio.sleep(0.08)
        require(recovered is not None, "独立服务重启后没有将中断请求保留为unknown")
        require(recovered["original"]["error"] == "进程中断后无法确认提供商用量",
                "进程恢复时未知用量说明不匹配")
        require(recovered["original"]["charged_cny"] is None and recovered["original"]["usage"] is None,
                "进程中断的原请求记录了未经确认的费用或用量")
        require(recovered["original"]["reservation_cny"] == original_request["reservation_cny"],
                "进程中断的原请求预留金额发生变化")
        require(recovered["turn"]["model_calls"] >= before["model_calls"],
                "进程恢复后模型累计次数重置")
        require(recovered["turn"]["tool_calls"] >= before["tool_calls"],
                "进程恢复后工具累计次数重置")
        require(Decimal(recovered["turn"]["elapsed_seconds"]) >= Decimal(before["elapsed_seconds"]),
                "进程恢复后累计耗时重置")
        require(recovered["turn"]["model_calls"] <= 3, "进程恢复后的模型请求次数超过每轮上限")

        independent_conversation = await self.api_create_conversation(11091, "CUS-04独立新请求")
        independent_response = await self.submit_message(
            11091, independent_conversation["id"], str(uuid4()), "请问目前是否有实时销售价格？",
        )
        require(independent_response.status_code == 200, "重启后独立新消息提交失败")
        independent_turn = independent_response.json()
        self.add_turn(independent_turn["id"])
        independent_final = await self.wait_turn(11091, independent_turn["id"])
        require(independent_final["status"] == "completed", "重启后独立新消息没有完成")
        independent_requests = self.db_requests(independent_turn["id"])
        require(independent_requests, "重启后独立新消息没有保存新请求")
        require(all(row["run_id"] == independent_turn["id"] for row in independent_requests),
                "重启后独立新请求的run_id错误")
        require(all(row["id"] != original_request["id"] for row in independent_requests),
                "重启后独立新消息复用了中断请求编号")
        self.pass_check("sudden_process_termination_and_restart", {
            "process_interruption": self.report["process_interruption"][turn["id"]],
            "turn_after_recovery": recovered["turn"],
            "original_request_after_recovery": recovered["original"],
            "all_recovery_requests": recovered["requests"],
            "independent_new_turn_id": independent_turn["id"],
            "independent_new_turn_state": self.db_turn(independent_turn["id"]),
            "independent_new_request_rows": independent_requests,
            "independent_new_turn_completed": independent_final["status"] == "completed",
        })

    def run_budget_and_finish(self):
        self.ensure_request_limit()
        requests = []
        for turn_id in sorted(self.turn_ids):
            requests.extend(self.db_requests(turn_id))
        self.report["deepseek_request_count_after"] = self.deepseek_request_count()
        self.report["new_deepseek_request_count"] = self.new_deepseek_request_count()
        require(self.report["new_deepseek_request_count"] <= MAX_DEEPSEEK_REQUESTS,
                "新增DeepSeek请求超过15条授权上限")
        require(all(row["service"] == "deepseek" and row["model"] == "deepseek-flash"
                    for row in requests), "本次检查出现DeepSeek以外的付费请求")
        deepseek_rows = requests
        require(len(deepseek_rows) <= MAX_DEEPSEEK_REQUESTS,
                "本次检查的DeepSeek请求超过15条")
        require(all(row["run_id"] in self.turn_ids and row["task_id"] is None for row in deepseek_rows),
                "客服模型记录没有绑定客服turn run_id")
        require(all(row["scope"] == "customer" and row["verification"] is False
                    for row in deepseek_rows), "客服模型记录费用身份错误")
        reservation_total = sum((Decimal(row["reservation_cny"]) for row in deepseek_rows), Decimal(0))
        require(all(Decimal(row["reservation_cny"]) <= Decimal(5) for row in deepseek_rows),
                "存在单次新增预留超过5元的请求")
        require(reservation_total <= Decimal(5), "本次新增模型预留总额超过5元检查上限")
        self.report["request_and_fee_evidence"] = {
            "requests": deepseek_rows,
            "reservation_total_cny": str(reservation_total),
            "charged_total_cny": str(sum(
                (Decimal(row["charged_cny"]) for row in deepseek_rows if row["charged_cny"] is not None),
                Decimal(0),
            )),
            "unknown_request_ids": [row["id"] for row in deepseek_rows if row["status"] == "unknown"],
            "unknown_reservation_total_cny": str(sum(
                (Decimal(row["reservation_cny"]) for row in deepseek_rows if row["status"] == "unknown"),
                Decimal(0),
            )),
        }
        if self.prior_report:
            additional_requests = (
                self.report["new_deepseek_request_count"]
                - self.prior_report["new_deepseek_request_count"]
            )
            require(0 <= additional_requests <= MAX_RESUMED_ADDITIONAL_DEEPSEEK_REQUESTS,
                    "恢复检查新增DeepSeek请求超过剩余额度")
            self.report["continuation_additional_deepseek_request_count"] = additional_requests
        unknown_rows = [row for row in deepseek_rows if row["status"] == "unknown"]
        require(len(unknown_rows) == 2, "真实取消和进程中断没有各留下一条unknown记录")
        require(all(row["charged_cny"] is None and row["usage"] is None
                    for row in unknown_rows), "unknown请求含有已知费用或用量")
        if self.resume_mode == "finalize_completed_checks":
            previous_fee_evidence = self.prior_report["request_and_fee_evidence"]
            previous_unknowns = {
                row["id"]: row
                for row in previous_fee_evidence["requests"]
                if row["status"] == "unknown"
            }
            current_unknowns = {row["id"]: row for row in unknown_rows}
            require(previous_unknowns and previous_unknowns.keys() == current_unknowns.keys(),
                    "CUS-04未知用量请求编号与已完成报告不同")
            require(all(
                previous_unknowns[request_id]["reservation_cny"]
                == current_unknowns[request_id]["reservation_cny"]
                and current_unknowns[request_id]["charged_cny"] is None
                and current_unknowns[request_id]["usage"] is None
                for request_id in previous_unknowns
            ), "CUS-04未知用量请求的预留、费用或用量状态发生变化")
            unknown_reservation_total = sum(
                (Decimal(row["reservation_cny"]) for row in unknown_rows), Decimal(0),
            )
            require(unknown_reservation_total
                    == Decimal(previous_fee_evidence["unknown_reservation_total_cny"]),
                    "CUS-04未知用量预留总额与已完成报告不同")
            self.report["unknown_fee_preservation"] = {
                "request_ids": sorted(current_unknowns),
                "reservation_by_request": {
                    request_id: current_unknowns[request_id]["reservation_cny"]
                    for request_id in sorted(current_unknowns)
                },
                "previous_total_cny": previous_fee_evidence["unknown_reservation_total_cny"],
                "current_total_cny": str(unknown_reservation_total),
                "reservation_and_unknown_usage_state_match": True,
            }
        self.report["database_budget_after"] = self.project_budget_snapshot()
        self.pass_check("customer_provider_request_identity_and_fee_records", {
            "global_deepseek_request_count_before": self.report["deepseek_request_count_before"],
            "global_deepseek_request_count_after": self.report["deepseek_request_count_after"],
            "new_deepseek_request_count": self.report["new_deepseek_request_count"],
            "limit": MAX_DEEPSEEK_REQUESTS,
            "requests_all_use_turn_run_id": True,
            "requests_all_use_customer_scope": True,
            "task_id_is_null": True,
            "reservation_total_cny": str(reservation_total),
            "unknown_requests_keep_reservation": all(
                row["status"] == "unknown" and row["charged_cny"] is None and row["usage"] is None
                for row in deepseek_rows if row["status"] == "unknown"
            ),
        })

    async def execute(self):
        if self.resume_mode == "finalize_completed_checks":
            await self.wait_for_other_turns("completed_run_finalization")
            self.check_port_free()
            previous_processes = self.prior_report.get("service_processes", [])
            original_report_reference = self.prior_report.get("reused_prior_report", {}).get("path")
            require(original_report_reference, "CUS-04续跑报告缺少初始运行报告路径")
            original_report_path = (PROJECT_ROOT / original_report_reference).resolve()
            require(original_report_path.is_relative_to(PROJECT_ROOT.resolve()),
                    "初始运行报告必须位于项目目录内")
            original_report = json.loads(original_report_path.read_text(encoding="utf-8"))
            require(original_report.get("milestone") == "CUS-04"
                    and original_report.get("run_id")
                    == self.prior_report.get("reused_prior_report", {}).get("run_id"),
                    "初始运行报告身份与CUS-04续跑报告不符")
            original_processes = original_report.get("service_processes", [])
            process_runs = [
                {"run_id": original_report.get("run_id"), "processes": original_processes},
                {"run_id": self.prior_report["run_id"], "processes": previous_processes},
            ]
            require(all(run["processes"] and all(
                process.get("stopped_at") and process.get("exit_code") is not None
                for process in run["processes"]
            ) for run in process_runs),
                "CUS-04初始运行或续跑报告没有证明全部服务进程已停止")
            previous_requests = self.prior_report.get("request_and_fee_evidence", {}).get("requests", [])
            current_requests = [
                request
                for turn_id in sorted(self.turn_ids)
                for request in self.db_requests(turn_id)
            ]
            previous_requests_by_id = {row["id"]: row for row in previous_requests}
            current_requests_by_id = {row["id"]: row for row in current_requests}
            require(previous_requests_by_id and previous_requests_by_id.keys() == current_requests_by_id.keys(),
                    "续跑结束后CUS-04请求记录数量或编号发生变化")
            changed_request_ids = sorted(
                request_id for request_id in previous_requests_by_id
                if previous_requests_by_id[request_id] != current_requests_by_id[request_id]
            )
            require(not changed_request_ids,
                    "续跑结束后CUS-04请求状态、费用或用量记录发生变化")
            self.current_phase = "legacy_ticket_turn_request_compatibility"
            legacy_turn_compatibility = self.verify_legacy_ticket_turn_compatibility()
            self.pass_check("legacy_ticket_turn_request_compatibility", legacy_turn_compatibility)
            current_budget = self.project_budget_snapshot()
            current_global_request_count = self.deepseek_request_count()
            external_activity = self.outside_request_groups(
                self.prior_report["started_at"], self.prior_report["completed_at"],
            )
            self.report["prior_completed_run_state"] = {
                "global_customer_request_count_after_run": self.prior_report["deepseek_request_count_after"],
                "global_customer_request_count_at_finalization": current_global_request_count,
                "global_customer_request_count_change": (
                    current_global_request_count - self.prior_report["deepseek_request_count_after"]
                ),
                "global_project_budget_after_run": self.prior_report["database_budget_after"],
                "global_project_budget_at_finalization": current_budget,
                "external_customer_request_groups_during_run": external_activity,
                "cus04_request_count_preserved": len(current_requests),
                "cus04_request_rows_unchanged": True,
                "legacy_ticket_turn_compatibility": legacy_turn_compatibility,
                "service_cleanup": {
                    "port_8002_free": True,
                    "runs": process_runs,
                    "all_processes_stopped": True,
                },
                "documented_post_run_source_changes": self.report[
                    "reused_prior_report"
                ]["accepted_documented_post_run_source_changes"],
            }
            self.pass_check("completed_report_and_service_cleanup", {
                "reused_report_path": self.report["reused_prior_report"]["path"],
                "reused_run_id": self.prior_report["run_id"],
                "all_required_checks_previously_passed": sorted(COMPLETE_RESUMABLE_CHECKS),
                "all_service_processes_stopped": True,
                "port_8002_free": True,
                "cus04_request_rows_unchanged": True,
                "prior_completed_run_state": self.report["prior_completed_run_state"],
            })
            self.run_budget_and_finish()
            self.report["prior_completed_run_state"]["unknown_fee_preservation"] = (
                self.report["unknown_fee_preservation"]
            )
            return

        await self.wait_for_other_turns("initial_start")
        self.start_service()
        identities = await self.login()
        self.report["accounts"] = {
            str(customer_id): {
                "bound_customer_id": identity["customer_id"],
                "source_id": identity["source_id"],
                "source_revision": identity["revision"],
            }
            for customer_id, identity in identities.items()
        }
        require({identity["customer_id"] for identity in identities.values()} == {11091, 11176},
                "客服身份接口返回的账号绑定不匹配")
        if self.resume_mode == "continue_ticket_and_restart":
            turn_id = self.find_resume_ticket_turn()
            self.report["reused_prior_report"]["ticket_turn_id"] = turn_id
            await self.run_ticket_checkpoint(turn_id)
        else:
            self.pass_check("real_login_and_customer_identity", self.report["accounts"])
            self.current_phase = "database_budget_and_unsent_cancel"
            self.run_budget_checks()
            await self.run_parallel_identity_and_dedupe()
            await self.run_cancel_and_lock_wait()
            await self.run_model_checkpoint()
            await self.run_ticket_checkpoint()
        await self.run_process_restart()
        self.run_budget_and_finish()

    async def close_clients(self):
        for client in self.clients.values():
            await client.aclose()
        self.clients.clear()

    def finish(self, error: BaseException | None):
        self.stop_service("CUS-04检查结束，清理本次8002服务")
        self.close_service_logs()
        try:
            self.report["source_sha256"]["after"] = self.source_hashes()
            self.report["source_hashes_unchanged"] = (
                self.report["source_sha256"]["before"] == self.report["source_sha256"]["after"]
            )
            if error is None and not self.report["source_hashes_unchanged"]:
                error = RuntimeError("检查期间客服源码摘要发生变化")
            self.report["deepseek_request_count_after"] = self.deepseek_request_count()
            self.report["new_deepseek_request_count"] = self.new_deepseek_request_count()
            if self.prior_report:
                self.report["continuation_additional_deepseek_request_count"] = (
                    self.report["new_deepseek_request_count"]
                    - self.prior_report["new_deepseek_request_count"]
                )
            self.report["database_budget_after"] = self.project_budget_snapshot()
        except (OSError, SQLAlchemyError):
            traceback.print_exc()
        if error is None:
            self.report["status"] = "passed"
        else:
            self.report["status"] = "failed_or_incomplete"
            message = str(error)
            for secret in self.passwords:
                if secret:
                    message = message.replace(secret, "[已隐藏]")
            self.report["failure"] = {
                "type": type(error).__name__,
                "message": message,
                "phase": self.current_phase,
            }
            traceback.print_exception(type(error), error, error.__traceback__)
        self.report["completed_at"] = utc_now()
        self.write_report()
        self.engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume-report", type=Path)
    args = parser.parse_args()
    now = datetime.now(UTC)
    run_id = f"cus04-{now:%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    evidence_dir = PROJECT_ROOT / ".scratch" / "evidence" / run_id
    runner = ReliabilityRun(run_id, evidence_dir, args.resume_report)
    stdout_path = evidence_dir / "runner.stdout.txt"
    stderr_path = evidence_dir / "runner.stderr.txt"
    error = None
    with stdout_path.open("w", encoding="utf-8") as stdout_file, \
            stderr_path.open("w", encoding="utf-8") as stderr_file:
        runner.add_raw_output(stdout_path)
        runner.add_raw_output(stderr_path)
        with contextlib.redirect_stdout(stdout_file), contextlib.redirect_stderr(stderr_file):
            print(json.dumps({"run_id": run_id, "started_at": utc_now()}, ensure_ascii=False))

            async def execute_and_close() -> BaseException | None:
                execution_error = None
                try:
                    await runner.execute()
                except Exception as caught:  # noqa: BLE001  # 保留失败状态与本次证据
                    execution_error = caught
                finally:
                    try:
                        await runner.close_clients()
                    except Exception as close_error:  # noqa: BLE001  # 记录客户端关闭错误
                        if execution_error is None:
                            execution_error = close_error
                return execution_error

            try:
                error = asyncio.run(execute_and_close())
            except Exception as caught:  # noqa: BLE001  # 保留失败状态与本次证据
                error = caught
            finally:
                runner.finish(error)
    print(json.dumps({"run_id": run_id, "status": runner.report["status"],
                      "report": str((evidence_dir / "report.json").relative_to(PROJECT_ROOT))},
                     ensure_ascii=False))
    return 0 if runner.report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
