import argparse
import json
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from shop.config import Settings
from shop.integration_models import IntegrationBudgetWindow

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description="准备真实集群身份配置及30元费用窗口")
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--hub-python", type=Path, required=True)
    parser.add_argument("--autorun", action="store_true")
    parser.add_argument("--open-budget", action="store_true")
    parser.add_argument("--window-id", default="clu-live-20261003")
    args = parser.parse_args()
    inputs = json.loads(args.inputs.read_text())
    actors = {}
    for name, account in inputs["accounts"].items():
        config = Path(inputs["hub_configs"][name]).resolve(strict=True)
        if not args.hub_python.is_file():
            raise FileNotFoundError("MCP Python环境不存在")
        actors[account["actor_id"]] = {"command": str(args.hub_python.absolute()),
                                     "args": ["-m", "agent_hub", "--config", str(config)],
                                     "cwd": str(ROOT)}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"actors": actors, "autorun": args.autorun,
                                       "profile": {"model": "deepseek-flash", "reasoning_effort": "high",
                                                   "output_limit": 8192, "context_tokens": 1000000}},
                                      indent=2, ensure_ascii=False))
    args.output.chmod(0o600)
    if args.open_budget:
        engine = create_engine(Settings().database_url)
        with Session(engine) as session, session.begin():
            active = session.scalar(select(IntegrationBudgetWindow).where(IntegrationBudgetWindow.active))
            if active is not None and active.id != args.window_id:
                raise ValueError("其他费用窗口仍处于活动状态")
            window = session.get(IntegrationBudgetWindow, args.window_id)
            if window is None:
                session.add(IntegrationBudgetWindow(id=args.window_id, limit_cny=30, active=True))
            elif window.limit_cny != 30:
                raise ValueError("集群费用窗口额度不一致")
            else:
                window.active = True
        engine.dispose()
    print(args.output)


if __name__ == "__main__":
    main()
