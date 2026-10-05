import json
import re
from dataclasses import asdict, dataclass
from decimal import ROUND_HALF_UP, Decimal, localcontext
from hashlib import sha256
from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from shop.assessment_dataset import load_assessment_source
from shop.assessment_models import (
    AssessmentAccess,
    AssessmentEmployee,
    AssessmentQuota,
    AssessmentRun,
    AssessmentSource,
)
from shop.assessment_orders import DataBlocked, SalesOrderReadPort
from shop.assessment_types import (
    METRIC_IDS,
    PERIOD_END,
    PERIOD_START,
    REVISION,
    RULE_VERSION,
    SAMPLE_NOTICE,
    SOURCE_ID,
    SOURCE_KEY,
)
from shop.models import User, now

LEVELS = {"employee": 0, "manager": 1, "admin": 2}
LABELS = {
    "sales_amount": "销售额", "order_count": "订单数",
    "average_order_value": "客单价", "quota_completion": "季度目标完成率",
}


class AccessDenied(PermissionError):
    pass


@dataclass(frozen=True)
class AssessmentPrincipal:
    user_id: str
    level: str
    employee_id: int | None
    allowed_employee_ids: tuple[int, ...]
    managed_employee_ids: tuple[int, ...]


def import_employee_source(session: Session, directory: Path) -> dict:
    dataset = load_assessment_source(directory)
    fingerprint = sha256(json.dumps(dataset.manifest, sort_keys=True).encode()).hexdigest()
    session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                    {"key": SOURCE_KEY})
    prior = session.get(AssessmentSource, SOURCE_KEY)
    salespeople = {row.employee_id: row for row in dataset.salespeople}
    employee_values = {
        row.employee_id: (
            row.job_title, row.current_flag, row.employee_id in salespeople,
            salespeople[row.employee_id].territory_id if row.employee_id in salespeople else None,
            row.source_line,
            salespeople[row.employee_id].source_line if row.employee_id in salespeople else None,
        ) for row in dataset.employees
    }
    quota_values = {(row.employee_id, row.quota_date): (row.amount, row.source_line)
                    for row in dataset.quotas}
    if prior is not None:
        actual_employees = {
            row.employee_id: (row.job_title, row.current_flag, row.is_salesperson,
                              row.territory_id, row.source_line, row.salesperson_line)
            for row in session.scalars(select(AssessmentEmployee)).all()
        }
        actual_quotas = {(row.employee_id, row.quota_date): (row.amount, row.source_line)
                         for row in session.scalars(select(AssessmentQuota)).all()}
        if (prior.source_id != SOURCE_ID or prior.revision != REVISION
                or prior.fingerprint != fingerprint or prior.manifest != dataset.manifest
                or actual_employees != employee_values or actual_quotas != quota_values):
            raise ValueError("既有员工与目标快照和固定来源不一致")
        return {"status": "reused", "fingerprint": fingerprint,
                "employees": len(actual_employees), "quotas": len(actual_quotas)}
    session.add(AssessmentSource(
        id=SOURCE_KEY, source_id=SOURCE_ID, revision=REVISION,
        fingerprint=fingerprint, manifest=dataset.manifest,
    ))
    session.flush()
    for employee_id, values in employee_values.items():
        session.add(AssessmentEmployee(
            employee_id=employee_id, source_key=SOURCE_KEY, job_title=values[0],
            current_flag=values[1], is_salesperson=values[2], territory_id=values[3],
            source_line=values[4], salesperson_line=values[5],
        ))
    session.flush()
    for (employee_id, quota_date), (amount, source_line) in quota_values.items():
        session.add(AssessmentQuota(
            employee_id=employee_id, quota_date=quota_date, amount=amount,
            source_line=source_line,
        ))
    session.flush()
    return {"status": "imported", "fingerprint": fingerprint,
            "employees": len(employee_values), "quotas": len(quota_values)}


def principal_for(session: Session, user_id: str) -> AssessmentPrincipal:
    access = session.get(AssessmentAccess, user_id)
    if access is None:
        raise AccessDenied("当前账号尚未绑定考核权限")
    if access.level not in LEVELS:
        raise AccessDenied("考核权限等级无效")
    all_sales = set(session.scalars(select(AssessmentEmployee.employee_id).where(
        AssessmentEmployee.is_salesperson.is_(True),
    )).all())
    if access.level == "admin":
        allowed, managed = all_sales, set()
    else:
        if access.employee_id not in all_sales:
            raise AccessDenied("账号未绑定有效销售员工")
        allowed, managed = {access.employee_id}, set()
        if access.level == "manager":
            upper_ids = set(session.scalars(select(AssessmentAccess.employee_id).where(
                AssessmentAccess.level.in_(("manager", "admin")),
                AssessmentAccess.employee_id.is_not(None),
                AssessmentAccess.user_id != user_id,
            )).all())
            managed = (set(access.managed_employee_ids) & all_sales) - upper_ids
            managed.discard(access.employee_id)
            allowed |= managed
    return AssessmentPrincipal(user_id, access.level, access.employee_id,
                               tuple(sorted(allowed)), tuple(sorted(managed)))


def configure_access(
    session: Session, *, user_id: str, level: str, employee_id: int | None,
    managed_employee_ids: list[int],
) -> AssessmentAccess:
    if session.get(User, user_id) is None or level not in LEVELS:
        raise ValueError("账号或考核权限等级无效")
    salespeople = set(session.scalars(select(AssessmentEmployee.employee_id).where(
        AssessmentEmployee.is_salesperson.is_(True),
    )).all())
    if employee_id is not None and employee_id not in salespeople:
        raise ValueError("绑定员工必须来自已接入的销售员工来源")
    if level != "admin" and employee_id is None:
        raise ValueError("员工与主管权限需要绑定员工编号")
    if level != "manager" and managed_employee_ids:
        raise ValueError("只有主管可以配置负责团队")
    if (len(managed_employee_ids) != len(set(managed_employee_ids))
            or not set(managed_employee_ids) <= salespeople
            or employee_id in managed_employee_ids):
        raise ValueError("管理范围包含重复、本人或无效员工")
    upper_ids = set(session.scalars(select(AssessmentAccess.employee_id).where(
        AssessmentAccess.level.in_(("manager", "admin")),
        AssessmentAccess.user_id != user_id,
    )).all())
    if set(managed_employee_ids) & upper_ids:
        raise ValueError("主管不能管理其他主管或管理员的员工身份")
    existing_binding = session.scalar(select(AssessmentAccess).where(
        AssessmentAccess.employee_id == employee_id, AssessmentAccess.user_id != user_id,
    )) if employee_id is not None else None
    if existing_binding is not None:
        raise ValueError("员工编号已经绑定其他账号")
    access = session.get(AssessmentAccess, user_id)
    if access is None:
        access = AssessmentAccess(user_id=user_id)
        session.add(access)
    access.level, access.employee_id = level, employee_id
    access.managed_employee_ids = sorted(managed_employee_ids)
    access.updated_at = now()
    session.flush()
    return access


def assert_run_read(principal: AssessmentPrincipal, run: AssessmentRun) -> None:
    if principal.level == "admin":
        return
    if not set(run.scope_employee_ids) <= set(principal.allowed_employee_ids):
        raise AccessDenied("任务包含当前权限范围以外的员工资料")
    if run.owner_id != principal.user_id and LEVELS[principal.level] <= LEVELS[run.owner_level]:
        raise AccessDenied("当前账号不能读取此考核任务")


def run_elapsed(run: AssessmentRun) -> Decimal:
    elapsed = run.elapsed_seconds
    if run.active_since is not None:
        elapsed += Decimal(str(max(0, (now() - run.active_since).total_seconds())))
    return elapsed


def run_stop_reason(session: Session, run: AssessmentRun) -> str | None:
    if run.cancel_requested or run.status == "cancelled":
        return "cancelled"
    if run.status != "running":
        return "not_running"
    if run_elapsed(run) >= 180:
        return "time_limit"
    try:
        assert_run_read(principal_for(session, run.owner_id), run)
    except AccessDenied:
        return "permission_changed"
    return None


class SalesMetricService:
    def __init__(self, order_port: SalesOrderReadPort | None = None):
        self.order_port = order_port

    def snapshot(self, session: Session) -> dict:
        source = session.get(AssessmentSource, SOURCE_KEY)
        if source is None:
            raise DataBlocked("员工与季度目标来源尚未接入")
        if source.source_id != SOURCE_ID or source.revision != REVISION:
            raise DataBlocked("员工来源版本与固定来源不一致")
        if self.order_port is None:
            raise DataBlocked("语音客服任务尚未提供 SalesOrderReadPort")
        orders = self.order_port.snapshot(session)
        if (not orders.complete or orders.source_id != SOURCE_ID or orders.revision != REVISION
                or orders.period_start != PERIOD_START or orders.period_end != PERIOD_END
                or (orders.row_count, orders.assigned_count, orders.unassigned_count)
                != (1022, 317, 705)
                or re.fullmatch(r"[a-f0-9]{64}", orders.file_sha256) is None or not orders.batch_id):
            raise DataBlocked("订单快照来源、期间或完整性检查未通过")
        order_data = asdict(orders)
        order_data["period_start"] = orders.period_start.isoformat()
        order_data["period_end"] = orders.period_end.isoformat()
        return {"source_id": source.source_id, "revision": source.revision,
                "dataset_fingerprint": source.fingerprint, "orders": order_data}

    @staticmethod
    def select_employees(
        principal: AssessmentPrincipal, scope: str, employee_ids: list[int],
    ) -> tuple[int, ...]:
        if scope == "self":
            if principal.employee_id is None:
                raise ValueError("当前管理员未绑定个人员工编号，请选择员工或组织范围")
            selected = (principal.employee_id,)
        elif scope == "team":
            if principal.level != "manager":
                raise AccessDenied("当前账号没有主管团队查询权限")
            selected = principal.managed_employee_ids
        elif scope == "organization":
            if principal.level != "admin":
                raise AccessDenied("组织汇总需要管理员权限")
            selected = principal.allowed_employee_ids
        elif scope == "employees":
            if not employee_ids or len(set(employee_ids)) != len(employee_ids):
                raise ValueError("员工查询需要不重复的员工编号")
            selected = tuple(sorted(employee_ids))
        else:
            raise ValueError("查询范围无效")
        if scope != "employees" and employee_ids:
            raise ValueError("该查询范围不能同时指定员工编号")
        if not selected:
            raise ValueError("当前查询范围没有销售员工")
        if not set(selected) <= set(principal.allowed_employee_ids):
            raise AccessDenied("查询员工不在当前授权范围")
        return selected

    @staticmethod
    def select_comparison_employees(selected: tuple[int, ...], employee_ids: list[int]) -> tuple[int, ...]:
        if not employee_ids:
            return ()
        if len(employee_ids) < 2 or len(set(employee_ids)) != len(employee_ids):
            raise ValueError("比较需要至少两位不重复的员工")
        if not set(employee_ids) <= set(selected):
            raise AccessDenied("比较员工不在当前查询的授权范围")
        return tuple(sorted(employee_ids))

    @classmethod
    def comparisons(cls, metrics: dict, employee_ids: list[int], metric_ids: list[str]) -> list[dict]:
        selected = cls.select_comparison_employees(
            tuple(row["employee_id"] for row in metrics["employees"]), employee_ids,
        )
        if not selected:
            return []
        if not set(metric_ids) <= set(METRIC_IDS):
            raise ValueError("比较指标没有支持的计算规则")
        rows = []
        for metric_id in metric_ids:
            entries = []
            for employee in metrics["employees"]:
                if employee["employee_id"] in selected:
                    metric = next(item for item in employee["metrics"] if item["metric_id"] == metric_id)
                    entries.append({"employee_id": employee["employee_id"], **{
                        key: metric[key] for key in ("value", "display", "status", "evidence_id")
                    }})
            entries.sort(key=lambda item: (
                item["value"] is None,
                Decimal(item["value"]).copy_negate() if item["value"] is not None else Decimal(0),
                item["employee_id"],
            ))
            previous_value, previous_rank = None, None
            statements = []
            for position, entry in enumerate(entries, start=1):
                value = Decimal(entry["value"]) if entry["value"] is not None else None
                rank = (previous_rank if value == previous_value else position) if value is not None else None
                entry["rank"] = rank
                previous_value, previous_rank = value, rank
                label = f"第{rank}位：" if rank is not None else ""
                statements.append(f"{label}员工{entry['employee_id']} {entry['display']}")
            rows.append({"metric_id": metric_id, "label": LABELS[metric_id],
                         "employee_ids": list(selected), "entries": entries,
                         "text": LABELS[metric_id] + "比较（数值从高到低，相同数值并列）："
                         + "；".join(statements) + "。"})
        return rows

    def query(
        self, session: Session, principal: AssessmentPrincipal, *, scope: str,
        employee_ids: list[int], period_start: str = "2023-07-01",
        period_end: str = "2023-10-01",
    ) -> dict:
        selected = self.select_employees(principal, scope, employee_ids)
        if (period_start, period_end) != ("2023-07-01", "2023-10-01"):
            raise DataBlocked("期间不支持：当前来源仅支持2023年第三季度")
        source = self._source_for(self.snapshot(session), principal)
        include_unassigned = scope == "organization"
        aggregates = self.order_port.aggregate(
            session, employee_ids=selected, period_start=PERIOD_START,
            period_end=PERIOD_END, include_unassigned=include_unassigned,
        )
        by_employee = {}
        for row in aggregates:
            if (row.employee_id in by_employee
                    or row.employee_id is None and not include_unassigned
                    or row.employee_id is not None and row.employee_id not in selected
                    or not isinstance(row.sales_amount, Decimal)
                    or not row.sales_amount.is_finite() or row.sales_amount < 0
                    or not isinstance(row.order_count, int) or row.order_count < 0):
                raise ValueError("订单聚合返回重复、范围外或无效指标")
            by_employee[row.employee_id] = row
        employees = session.scalars(select(AssessmentEmployee).where(
            AssessmentEmployee.employee_id.in_(selected),
        ).order_by(AssessmentEmployee.employee_id)).all()
        if len(employees) != len(selected):
            raise DataBlocked("已绑定员工记录不完整")
        quotas = session.scalars(select(AssessmentQuota).where(
            AssessmentQuota.employee_id.in_(selected),
            AssessmentQuota.quota_date >= PERIOD_START, AssessmentQuota.quota_date < PERIOD_END,
        )).all()
        quota_map = {}
        for quota in quotas:
            if quota.employee_id in quota_map or not quota.amount.is_finite() or quota.amount <= 0:
                raise ValueError("季度目标存在冲突或无效金额")
            quota_map[quota.employee_id] = quota
        results = []
        for employee in employees:
            row = by_employee.get(employee.employee_id)
            amount, count = (row.sales_amount, row.order_count) if row else (Decimal(0), 0)
            target = quota_map.get(employee.employee_id)
            results.append({
                "employee_id": employee.employee_id, "job_title": employee.job_title,
                "metrics": self._metrics(employee.employee_id, amount, count,
                                         target.amount if target else None),
                "quota": {"amount": str(target.amount), "quota_date": target.quota_date.isoformat(),
                          "source_line": target.source_line} if target else None,
            })
        unassigned = None
        if include_unassigned:
            row = by_employee.get(None)
            if row is None or row.order_count != 705:
                raise DataBlocked("无员工归属订单的聚合记录不完整")
            if sum(item.order_count for key, item in by_employee.items() if key is not None) != 317:
                raise DataBlocked("员工归属订单的聚合记录不完整")
            unassigned = {"metrics": self._metrics(None, row.sales_amount, row.order_count, None)}
        with localcontext() as context:
            context.prec = 38
            total_amount = sum((by_employee[key].sales_amount for key in selected if key in by_employee), Decimal(0))
            total_count = sum(by_employee[key].order_count for key in selected if key in by_employee)
            total_quota = (sum((quota_map[key].amount for key in selected), Decimal(0))
                           if all(key in quota_map for key in selected) else None)
        summary = {"employee_ids": list(selected), "label": "所选销售员工汇总",
                   "metrics": self._metrics(None, total_amount, total_count, total_quota)}
        for item in summary["metrics"]:
            item["evidence_id"] = f"{RULE_VERSION}:summary:{item['metric_id']}"
        return {"period_start": period_start, "period_end": period_end,
                "rule_version": RULE_VERSION, "notice": SAMPLE_NOTICE, "source": source,
                "employees": results, "summary": summary, "unassigned": unassigned}

    @staticmethod
    def _source_for(source: dict, principal: AssessmentPrincipal) -> dict:
        if principal.level == "admin":
            return source
        orders = {key: value for key, value in source["orders"].items()
                  if key not in {"row_count", "assigned_count", "unassigned_count"}}
        return {**source, "orders": orders}

    @staticmethod
    def _metrics(employee_id: int | None, amount: Decimal, count: int, quota: Decimal | None) -> list:
        with localcontext() as context:
            context.prec = 38
            values = (amount, Decimal(count), amount / Decimal(count) if count else None,
                      amount / quota if quota is not None else None)
            results = []
            for metric_id, value in zip(METRIC_IDS, values, strict=True):
                status = "ok" if value is not None else (
                    "no_orders" if metric_id == "average_order_value" else "missing_target")
                if value is None:
                    display = "无可计算订单" if status == "no_orders" else "缺少季度目标"
                elif metric_id == "order_count":
                    display = str(count)
                else:
                    shown = value * 100 if metric_id == "quota_completion" else value
                    display = str(shown.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
                    if metric_id == "quota_completion":
                        display += "%"
                results.append({"metric_id": metric_id, "label": LABELS[metric_id],
                                "value": str(value) if value is not None else None,
                                "display": display, "status": status,
                                "evidence_id": f"{RULE_VERSION}:{employee_id}:{metric_id}",
                                "formula": {"sales_amount": "SUM(SubTotal), Status=Shipped",
                                            "order_count": "COUNT(DISTINCT SalesOrderID)",
                                            "average_order_value": "sales_amount / order_count",
                                            "quota_completion": "sales_amount / SalesQuota"}[metric_id]})
            return results

    def evidence(self, session: Session, principal: AssessmentPrincipal, employee_id: int) -> dict:
        self.select_employees(principal, "employees", [employee_id])
        source = self._source_for(self.snapshot(session), principal)
        orders = self.order_port.evidence(
            session, employee_id=employee_id, period_start=PERIOD_START, period_end=PERIOD_END,
        )
        seen = set()
        data = []
        for row in orders:
            if (row.employee_id != employee_id or row.order_id in seen or row.status != 5
                    or not PERIOD_START <= row.order_date < PERIOD_END
                    or not row.subtotal.is_finite() or row.subtotal < 0 or row.source_line < 1):
                raise ValueError("订单依据包含范围外、重复或无效记录")
            seen.add(row.order_id)
            data.append({**asdict(row), "order_date": row.order_date.isoformat(),
                         "subtotal": str(row.subtotal)})
        quotas = session.scalars(select(AssessmentQuota).where(
            AssessmentQuota.employee_id == employee_id,
            AssessmentQuota.quota_date >= PERIOD_START, AssessmentQuota.quota_date < PERIOD_END,
        )).all()
        if len(quotas) > 1:
            raise ValueError("员工季度目标存在冲突")
        quota = quotas[0] if quotas else None
        return {"employee_id": employee_id, "orders": data, "source": source,
                "quota": {"amount": str(quota.amount), "quota_date": quota.quota_date.isoformat(),
                          "source_line": quota.source_line} if quota else None}
