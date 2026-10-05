import re
from datetime import date, datetime, time, timedelta

from sqlalchemy import exists, func, select
from sqlalchemy.orm import Session

from shop.adventureworks import REVISION, SOURCE_ID
from shop.customer_models import (
    CustomerBinding,
    CustomerOrder,
    CustomerProfile,
    OrderLine,
    OrderProduct,
)
from shop.models import User


def bind_customer(session: Session, user_id: str, customer_id: int, revision: str = REVISION) -> None:
    if session.get(User, user_id) is None:
        raise ValueError("绑定账号不存在")
    if session.get(CustomerProfile, (revision, customer_id)) is None:
        raise ValueError("绑定客户不存在")
    existing = session.get(CustomerBinding, user_id)
    if existing is not None:
        if (existing.customer_id, existing.source_revision) != (customer_id, revision):
            raise ValueError("账号已绑定其他客户")
        return
    session.add(CustomerBinding(user_id=user_id, source_revision=revision, customer_id=customer_id))
    session.commit()


def is_order_number(value: str) -> bool:
    return re.fullmatch(r"(?:SO)?[1-9][0-9]*", value.strip().upper()) is not None


def mentioned_order_numbers(message: str) -> set[str]:
    return {normalize_order_number(value) for value in re.findall(
        r'(?<![A-Z0-9])(?:SO[1-9][0-9]*|[1-9][0-9]{4,})(?![A-Z0-9])', message.upper())}


def normalize_order_number(value: str) -> str:
    normalized = value.strip().upper()
    if not is_order_number(value):
        raise ValueError("请提供完整订单编号")
    return normalized if normalized.startswith("SO") else f"SO{normalized}"


class OrderQueryService:
    def __init__(self, session: Session, user_id: str):
        self.session = session
        self.binding = session.get(CustomerBinding, user_id)
        if self.binding is None:
            raise PermissionError("当前账号尚未绑定客户")
        self.revision = self.binding.source_revision
        self.customer_id = self.binding.customer_id

    def identity(self) -> dict:
        profile = self.session.get(CustomerProfile, (self.revision, self.customer_id))
        if profile is None:
            raise ValueError("绑定客户资料缺失")
        return {"name": profile.name, "person_name": profile.person_name,
                "store_name": profile.store_name, "customer_id": self.customer_id,
                "source_id": SOURCE_ID, "revision": self.revision, "is_sample": True}

    def _conditions(self) -> list:
        return [CustomerOrder.source_revision == self.revision,
                CustomerOrder.customer_id == self.customer_id]

    def list_orders(self, order_number: str | None = None, start_date: date | None = None,
                    end_date: date | None = None, product: str | None = None,
                    limit: int = 20, offset: int = 0) -> dict:
        if isinstance(limit, bool) or isinstance(offset, bool) or not 1 <= limit <= 20 or offset < 0:
            raise ValueError("订单分页范围错误")
        if start_date and end_date and start_date > end_date:
            raise ValueError("日期起点超过终点")
        conditions = self._conditions()
        if order_number is not None:
            conditions.append(CustomerOrder.order_number == normalize_order_number(order_number))
        if start_date is not None:
            conditions.append(CustomerOrder.order_date >= datetime.combine(start_date, time.min))
        if end_date is not None:
            conditions.append(CustomerOrder.order_date < datetime.combine(end_date + timedelta(days=1), time.min))
        if product is not None:
            if not product.strip():
                raise ValueError("商品条件不能为空")
            conditions.append(exists(select(OrderLine.line_id).join(OrderProduct, (
                OrderProduct.source_revision == OrderLine.source_revision) & (
                OrderProduct.product_id == OrderLine.product_id)).where(
                    OrderLine.source_revision == self.revision,
                    OrderLine.order_id == CustomerOrder.order_id,
                    OrderProduct.name.icontains(product.strip(), autoescape=True))))
        total = self.session.scalar(select(func.count()).select_from(CustomerOrder).where(*conditions))
        records = self.session.scalars(select(CustomerOrder).where(*conditions).order_by(
            CustomerOrder.order_date.desc(), CustomerOrder.order_id.desc()).limit(limit).offset(offset)).all()
        return {"orders": self._payloads(records, summary=True), "total": total,
                "limit": limit, "offset": offset, "has_more": offset + len(records) < total}

    def get_order(self, order_number: str) -> dict | None:
        record = self.session.scalar(select(CustomerOrder).where(
            *self._conditions(), CustomerOrder.order_number == normalize_order_number(order_number)))
        return self._payloads([record], summary=False)[0] if record is not None else None

    def _payloads(self, records: list[CustomerOrder], *, summary: bool) -> list[dict]:
        if not records:
            return []
        items = {record.order_id: [] for record in records}
        rows = self.session.execute(select(OrderLine, OrderProduct).join(OrderProduct, (
            OrderProduct.source_revision == OrderLine.source_revision) & (
            OrderProduct.product_id == OrderLine.product_id)).where(
                OrderLine.source_revision == self.revision,
                OrderLine.order_id.in_(items)).order_by(OrderLine.line_id)).all()
        for line, product in rows:
            items[line.order_id].append({"line_id": line.line_id, "product_id": line.product_id,
                "name": product.name, "quantity": line.quantity, "unit_price": str(line.unit_price),
                "discount": str(line.discount), "line_total": str(line.line_total)})
        result = []
        for record in records:
            full_items = items[record.order_id]
            if not full_items:
                raise ValueError("订单明细缺失")
            shown = full_items[:3] if summary else full_items
            result.append({"order_id": record.order_id, "order_number": record.order_number,
                "order_date": record.order_date.isoformat(), "due_date": record.due_date.isoformat(),
                "ship_date": record.ship_date.isoformat() if record.ship_date else None,
                "status": record.status, "status_label": f"来源状态代码{record.status}",
                "subtotal": str(record.subtotal), "tax": str(record.tax), "freight": str(record.freight),
                "total_due": str(record.total_due), "items": shown, "item_count": len(full_items),
                "remaining_item_count": len(full_items) - len(shown),
                "source": {"source_id": SOURCE_ID, "revision": self.revision, "order_id": record.order_id},
                "is_sample": True})
        return result
