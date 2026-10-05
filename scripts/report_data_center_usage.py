import argparse
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from shop.config import Settings
from shop.integration_models import IntegrationBudgetWindow
from shop.runtime_models import ProviderRequest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    database = create_engine(Settings().database_url)
    with Session(database) as session:
        rows = session.execute(select(
            ProviderRequest.id, ProviderRequest.run_id, ProviderRequest.step_key,
            ProviderRequest.model, ProviderRequest.status, ProviderRequest.usage,
            ProviderRequest.charged_cny, ProviderRequest.reservation_cny, ProviderRequest.price_basis,
        ).order_by(ProviderRequest.created_at)).all()
        window = session.get(IntegrationBudgetWindow, "dc-live-20261004")
        if window is None:
            raise LookupError("数据中台验证窗口没有登记")
        current = [row for row in rows if row.price_basis.get("integration_run_id") == window.id]
        paid = sum((row.charged_cny or Decimal(0) for row in current), Decimal(0))
        reservations = sum((row.reservation_cny for row in rows if row.charged_cny is None), Decimal(0))
        report = {"captured_at": datetime.now(UTC).isoformat(), "window": window.id,
            "active": window.active, "limit_cny": str(window.limit_cny),
            "window_spent_cny": str(paid), "all_open_reservations_cny": str(reservations),
            "window_spent_and_all_open_reservations_cny": str(paid + reservations),
            "within_budget": paid + reservations <= window.limit_cny,
            "project_spent_cny": str(sum((row.charged_cny or Decimal(0) for row in rows), Decimal(0))),
            "unknown_count": sum(row.status == "unknown" for row in rows),
            "requests": [{"request_id": row.id, "run_id": row.run_id, "step_key": row.step_key,
                          "model": row.model, "status": row.status, "usage": row.usage,
                          "spent_cny": str(row.charged_cny or Decimal(0)),
                          "reserved_cny": str(row.reservation_cny if row.charged_cny is None else Decimal(0))}
                         for row in current]}
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    database.dispose()
    print(json.dumps({key: value for key, value in report.items() if key != "requests"}))


if __name__ == "__main__":
    main()
