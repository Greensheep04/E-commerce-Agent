import argparse
import json
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from scripts.verify_integration_live import snapshot
from shop.config import Settings
from shop.integration_models import IntegrationBudgetWindow, IntegrationCall


def main():
    parser = argparse.ArgumentParser(description="记录MCP验证的原业务账目和预算窗口")
    parser.add_argument("mode", choices=["snapshot", "activate", "close"])
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    args = parser.parse_args()
    settings = Settings()
    inputs = json.loads(args.inputs.read_text(encoding="utf-8"))
    assert inputs["run_id"] == args.run_id
    engine = create_engine(settings.database_url)
    try:
        with Session(engine) as session, session.begin():
            window = session.get(IntegrationBudgetWindow, args.run_id)
            if args.mode == "activate":
                active = session.scalar(select(IntegrationBudgetWindow).where(IntegrationBudgetWindow.active))
                assert active is None or active.id == args.run_id, "其他验证预算窗口仍在使用"
                if window is None:
                    window = IntegrationBudgetWindow(id=args.run_id, limit_cny=Decimal(5), active=True)
                    session.add(window)
                else:
                    assert window.limit_cny == Decimal(5)
                    window.active = True
            elif args.mode == "close":
                assert window is not None, "预算窗口必须存在"
                window.active = False
            calls = [{"actor_id": row.actor_id, "module_id": row.module_id, "operation": row.operation,
                      "request_id": row.request_id, "resource_id": row.resource_id,
                      "payload_sha256": row.payload_sha256, "upstream": row.upstream}
                     for row in session.scalars(select(IntegrationCall).where(
                         IntegrationCall.request_id.in_(list(inputs["request_ids"].values()))))]
            window_record = None if window is None else {
                "id": window.id, "limit_cny": str(window.limit_cny), "active": window.active,
            }
        result = {**snapshot(settings), "integration_calls": calls, "window": window_record}
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"report": str(args.report), "window": window_record}, ensure_ascii=False))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
