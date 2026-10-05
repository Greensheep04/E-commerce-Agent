import argparse
import json

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from shop.budget import BudgetService
from shop.config import Settings
from shop.runtime_models import AssetIndex, BudgetAccount, ProviderRequest


def main():
    parser = argparse.ArgumentParser(description="检查索引用量及连接阶段错误")
    parser.add_argument("--reconcile-connect-error")
    args = parser.parse_args()
    maker = sessionmaker(create_engine(Settings().database_url))
    if args.reconcile_connect_error:
        BudgetService(maker).reconcile_connect_error(args.reconcile_connect_error)
    with maker() as session:
        account = session.get(BudgetAccount, "project")
        rows = session.scalars(select(AssetIndex)).all()
        counts = {status: sum(row.status == status for row in rows)
                  for status in {row.status for row in rows}}
        requests = session.scalars(select(ProviderRequest).where(
            ProviderRequest.step_key == "description"
        ).order_by(ProviderRequest.created_at.desc())).all()
        abnormal = []
        for item in requests:
            if item.response_body:
                choice = item.response_body["choices"][0]
                content = choice["message"]["content"]
                if not content.strip().startswith("{"):
                    abnormal.append({"id": item.id, "run_id": item.run_id,
                                     "status": item.status, "usage": item.usage,
                                     "finish_reason": choice["finish_reason"], "content": content[:1500],
                                     "reasoning": choice["message"].get("reasoning_content", "")[:6000],
                                     "system": item.request_body["messages"][0]["content"],
                                     "input_fields": item.request_body["messages"][-1]["content"][0]["text"]})
        print(json.dumps({"budget": {"spent": str(account.spent_cny), "reserved": str(account.reserved_cny)},
                          "indexes": counts, "abnormal": abnormal}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
