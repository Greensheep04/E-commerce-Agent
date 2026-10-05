import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

from sqlalchemy import func, select

from shop.config import Settings
from shop.integration import ShopIntegration
from shop.runtime_models import ProviderRequest

ACTIVE_GATEWAY = None


def wait_for_processes(directory, slot):
    barrier = Path(directory)
    (barrier / f"{slot}.ready").write_text("ready", encoding="utf-8")
    deadline = time.monotonic() + 30
    while not all((barrier / f"{item}.ready").is_file() for item in ("0", "1")):
        if time.monotonic() >= deadline:
            raise TimeoutError("两个真实进程没有在30秒内到达并发入口")
        time.sleep(0.01)


async def invoke(args):
    global ACTIVE_GATEWAY
    settings = Settings(
        database_url=os.environ["IF05_DATABASE_URL"],
        data_dir=Path(os.environ["IF05_DATA_DIR"]),
        poster_dir=Path(os.environ["IF05_POSTER_DIR"]),
        session_secret=os.environ["IF05_SESSION_SECRET"],
    )
    ACTIVE_GATEWAY = ShopIntegration(settings)
    await ACTIVE_GATEWAY.startup()
    context = ACTIVE_GATEWAY.login(
        os.environ["IF05_USERNAME"],
        os.environ["IF05_PASSWORD"],
        os.environ["IF05_REQUEST_ID"],
    )
    with ACTIVE_GATEWAY.maker() as session:
        before = session.scalar(select(func.count()).select_from(ProviderRequest))
    if args.barrier_directory:
        wait_for_processes(args.barrier_directory, args.slot)
    result = await ACTIVE_GATEWAY.invoke(
        "harness", "create", {"prompt": os.environ["IF05_PROMPT"]}, context
    )
    with ACTIVE_GATEWAY.maker() as session:
        after = session.scalar(select(func.count()).select_from(ProviderRequest))
    response = {
        "pid": os.getpid(),
        "actor_id": context.actor_id,
        "module_id": "harness",
        "operation": "create",
        "request_id": context.request_id,
        "task_id": result.data["task_id"],
        "provider_request_count_before": before,
        "provider_request_count_after": after,
    }
    if not args.abrupt_exit:
        await ACTIVE_GATEWAY.shutdown()
    return response


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--barrier-directory")
    parser.add_argument("--slot", choices=("0", "1"))
    parser.add_argument("--abrupt-exit", action="store_true")
    args = parser.parse_args()
    if args.barrier_directory and args.slot is None:
        raise ValueError("并发进程需要指定进程编号")
    response = asyncio.run(invoke(args))
    sys.stdout.write(json.dumps(response, ensure_ascii=False, sort_keys=True) + "\n")
    sys.stdout.flush()
    if args.abrupt_exit:
        os._exit(0)


if __name__ == "__main__":
    main()
