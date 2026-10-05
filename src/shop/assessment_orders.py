from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from importlib import import_module
from typing import Protocol, runtime_checkable

from sqlalchemy.orm import Session

from shop.config import Settings


class DataBlocked(RuntimeError):
    pass


@dataclass(frozen=True)
class OrderSnapshot:
    source_id: str
    revision: str
    batch_id: str
    file_sha256: str
    period_start: datetime
    period_end: datetime
    row_count: int
    assigned_count: int
    unassigned_count: int
    complete: bool
    upstream_source_id: str | None = None


@dataclass(frozen=True)
class SalesOrderAggregate:
    employee_id: int | None
    sales_amount: Decimal
    order_count: int


@dataclass(frozen=True)
class OrderEvidence:
    order_id: int
    employee_id: int | None
    order_date: datetime
    status: int
    subtotal: Decimal
    source_line: int


@runtime_checkable
class SalesOrderReadPort(Protocol):
    def snapshot(self, session: Session) -> OrderSnapshot: ...

    def aggregate(
        self, session: Session, *, employee_ids: tuple[int, ...],
        period_start: datetime, period_end: datetime, include_unassigned: bool,
    ) -> list[SalesOrderAggregate]: ...

    def evidence(
        self, session: Session, *, employee_id: int | None,
        period_start: datetime, period_end: datetime,
    ) -> list[OrderEvidence]: ...


def configured_order_port(settings: Settings) -> SalesOrderReadPort | None:
    reference = settings.assessment_order_adapter
    if reference is None:
        return None
    module_name, factory_name = reference.split(":")
    factory = getattr(import_module(module_name), factory_name)
    port = factory(settings)
    if not isinstance(port, SalesOrderReadPort):
        raise TypeError("订单适配器没有实现 SalesOrderReadPort")
    return port
