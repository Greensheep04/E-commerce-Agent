import argparse
import asyncio
import json
import os
import signal
import subprocess
import sys
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from time import monotonic, sleep

from agent_cluster.context import ContextManager
from agent_cluster.context.manager import encode
from agent_cluster.engine import SYSTEM_PROMPT, completion_check
from agent_cluster.types import (
    CompletionCriterion,
    CoordinatorProfile,
    GoalDefinition,
    NoteRef,
    TriggerEvent,
)
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from scripts.verify_cluster_live import execution_manifest, usage_records
from scripts.verify_integration_live import save, snapshot
from shop.cluster import ShopCluster
from shop.config import Settings

ROOT = Path(__file__).resolve().parents[1]
PROCEDURE = """本目标按四个阶段顺序执行。一次只登记当前阶段的业务任务；每个阶段完成后使用write_note保存阶段结果及来源，然后才能登记下一阶段。准备关键词的阶段先通过read_note读取对应宣传页阶段的Note。保持此前task_id定义和request_id，以实际业务结果判断进度。最后读取第一个阶段的Note检查原始来源，然后请求finish。"""


class ObservedContext:
    def __init__(self, context, cluster, directory, interrupt):
        self.context, self.cluster, self.directory, self.interrupt = context, cluster, directory, interrupt

    async def prepare(self, goal, run, catalog, history):
        window = await self.context.prepare(goal, run, catalog, history)
        if window.compacted:
            state = self.context._state(goal, run, history)
            path = self.directory / f"compaction-{state['prefix_count']}.json"
            if not path.exists():
                save(path, {"run_id": run.run_id, "process_id": os.getpid(),
                            "prefix_count": state["prefix_count"], "archive_refs": state["archive_refs"],
                            "window": window.model_dump(mode="json"),
                            "history_sha256": sha256(encode(history).encode()).hexdigest(),
                            "recorded_at": datetime.now(UTC).isoformat()})
            if self.interrupt:
                refs = self.cluster.notes.index(goal_id=goal.goal_id, actor_id=goal.actor_id)
                early = next(ref for ref in refs if ref.topic == "business-result")
                content = self.cluster.notes.read(early, goal_id=goal.goal_id, actor_id=goal.actor_id)
                save(self.directory / "early-note.json", early.model_dump(mode="json"))
                save(self.directory / "interruption-ready.json", {
                    "process_id": os.getpid(), "run_id": run.run_id,
                    "early_note_sha256": sha256(content.encode()).hexdigest(),
                    "tasks": self.cluster.store.tasks(run.run_id, goal.actor_id),
                    "usage": usage_records(self.cluster.maker, run.run_id)})
                await asyncio.Event().wait()
        return window


def services(args):
    settings = Settings(cluster_config_file=args.cluster_config)
    engine = create_engine(settings.database_url, pool_size=10, max_overflow=10)
    maker = sessionmaker(engine, expire_on_commit=False)
    return engine, ShopCluster(settings, engine, maker)


async def prepare(args):
    args.directory.mkdir(parents=True, exist_ok=False)
    inputs = json.loads(args.inputs.read_text())
    actor_id = inputs["accounts"]["admin"]["actor_id"]
    goal = GoalDefinition(actor_id=actor_id, budget_cny="8", max_decisions=40, max_tasks=80,
        objective="依次完成四个营销阶段：第一阶段生成一件黑色男士T恤的中文宣传页并审核；"
                  "第二阶段读取第一阶段的Note，依据该宣传页准备海报关键词；"
                  "第三阶段生成一件蓝色男士T恤的中文宣传页并审核；"
                  "第四阶段读取第三阶段的Note，依据该宣传页准备另一组海报关键词。"
                  "每个阶段完成后保存Note；最终读取第一阶段的Note检查来源，保留两页与两组关键词。",
        allowed_operations={"shop_harness_agent": ["create", "run", "show"],
                            "shop_poster_agent": ["prepare", "show"],
                            "shop_materials_search": ["search"], "shop_materials_show": ["show"]},
        criteria=[CompletionCriterion(tool_name="shop_harness_agent", action="run",
                                       path=["data", "result", "status"], operator="equals",
                                       expected="materials_found", min_matches=2),
                  CompletionCriterion(tool_name="shop_harness_agent", action="run",
                                       path=["data", "result", "review", "approved"], operator="equals",
                                       expected=True, min_matches=2),
                  CompletionCriterion(tool_name="shop_poster_agent", action="prepare",
                                       path=["data", "job", "keyword_plan", "keywords"], min_matches=2)])
    engine, cluster = services(args)
    try:
        await cluster.validate_goal(goal)
        cluster.store.create_goal(goal)
        run = cluster.store.trigger(TriggerEvent(goal_id=goal.goal_id, actor_id=actor_id, kind="manual",
                                                event_id=args.directory.name + ":longflow"))
        profile = CoordinatorProfile(context_tokens=262144, compression_ratio=0.16)
        save(args.directory / "state.json", {"milestone": "CLU-03/CLU-05", "goal": goal.model_dump(mode="json"),
            "run_id": run.run_id, "profile": profile.model_dump(mode="json"),
            "production_profile": cluster.configuration.profile.model_dump(mode="json"),
            "system_prompt": SYSTEM_PROMPT + "\n" + PROCEDURE,
            "compression_configuration": "此检查使用262144上下文容量、0.16压缩阈值；生产配置保持1M与0.85",
            "source_sha256": execution_manifest(), "created_at": datetime.now(UTC).isoformat()})
        save(args.directory / "baseline.json", snapshot(Settings()))
    finally:
        engine.dispose()


async def worker(args):
    state = json.loads((args.directory / "state.json").read_text())
    goal = GoalDefinition.model_validate(state["goal"])
    engine, cluster = services(args)
    try:
        profile = CoordinatorProfile.model_validate(state["profile"])
        context = ContextManager(cluster.notes, profile, state["system_prompt"], cluster.model)
        cluster.engine.profile = profile
        cluster.engine.provider.profile = profile
        cluster.engine.context = ObservedContext(context, cluster, args.directory, args.stage == "initial")
        if args.stage == "recovered":
            interruption = json.loads((args.directory / "interruption-ready.json").read_text())
            reference = NoteRef.model_validate_json((args.directory / "early-note.json").read_text())
            content = cluster.notes.read(reference, goal_id=goal.goal_id, actor_id=goal.actor_id)
            assert sha256(content.encode()).hexdigest() == interruption["early_note_sha256"]
            tasks = cluster.store.tasks(state["run_id"], goal.actor_id)
            assert tasks == interruption["tasks"]
            assert usage_records(cluster.maker, state["run_id"]) == interruption["usage"]
            save(args.directory / "recovery-start.json", {"process_id": os.getpid(), "run_id": state["run_id"],
                "early_note_sha256": sha256(content.encode()).hexdigest(), "tasks": tasks})
        run = await cluster.engine.run(state["run_id"], goal.actor_id)
        tasks = cluster.store.tasks(run.run_id, goal.actor_id)
        history = cluster.store.history(run.run_id, goal.actor_id)
        save(args.directory / f"{args.stage}-run.json", run.model_dump(mode="json"))
        save(args.directory / f"{args.stage}-tasks.json", tasks)
        save(args.directory / f"{args.stage}-history.json", history)
        save(args.directory / f"{args.stage}-usage.json", usage_records(cluster.maker, run.run_id))
        assert run.status == "completed"
        assert completion_check(goal, tasks)[0]
    finally:
        engine.dispose()


def launch(args, stage):
    command = [sys.executable, "-m", "scripts.verify_cluster_longflow", "--mode", "worker", "--stage", stage,
               "--inputs", str(args.inputs), "--cluster-config", str(args.cluster_config),
               "--directory", str(args.directory)]
    with (args.directory / f"{stage}.stdout.txt").open("w") as stdout, (
        args.directory / f"{stage}.stderr.txt").open("w") as stderr:
        return subprocess.Popen(command, cwd=ROOT, stdout=stdout, stderr=stderr, env=os.environ.copy())


def execute(args):
    asyncio.run(prepare(args))
    initial = launch(args, "initial")
    deadline = monotonic() + 600
    while not (args.directory / "interruption-ready.json").is_file():
        if initial.poll() is not None:
            raise RuntimeError(f"完整长流程首次进程提前结束，退出码{initial.returncode}")
        if monotonic() > deadline:
            initial.terminate()
            initial.wait(timeout=30)
            raise TimeoutError("完整长流程等待压缩超过检查时间")
        sleep(0.2)
    initial.send_signal(signal.SIGTERM)
    assert initial.wait(timeout=30) == -signal.SIGTERM
    save(args.directory / "process-interruption.json", {"process_id": initial.pid, "signal": "SIGTERM",
        "exit_code": initial.returncode, "recorded_at": datetime.now(UTC).isoformat()})
    recovered = launch(args, "recovered")
    assert recovered.wait(timeout=600) == 0
    state = json.loads((args.directory / "state.json").read_text())
    assert state["source_sha256"] == execution_manifest()
    before = json.loads((args.directory / "interruption-ready.json").read_text())
    final_tasks = json.loads((args.directory / "recovered-tasks.json").read_text())
    by_id = {task["task_id"]: task for task in final_tasks}
    for task in before["tasks"]:
        assert by_id[task["task_id"]]["request_id"] == task["request_id"]
        if task["status"] == "completed":
            assert by_id[task["task_id"]] == task
    compactions = [json.loads(path.read_text()) for path in sorted(args.directory.glob("compaction-*.json"))]
    assert len(compactions) >= 2
    usage = json.loads((args.directory / "recovered-usage.json").read_text())
    old_requests = {row["id"]: row for row in before["usage"]}
    assert all(old_requests[row["id"]] == row for row in usage if row["id"] in old_requests)
    assert initial.pid != recovered.pid
    save(args.directory / "report.json", {"passed": True, "run_id": state["run_id"],
        "goal_id": state["goal"]["goal_id"], "profile": state["profile"], "compactions": compactions,
        "initial_process": initial.pid, "recovered_process": recovered.pid,
        "reused_completed_tasks": sum(task["status"] == "completed" for task in before["tasks"]),
        "preserved_request_ids": list(old_requests), "usage": usage,
        "source_sha256": state["source_sha256"], "finished_at": datetime.now(UTC).isoformat()})
    print(f"长流程通过，运行{state['run_id']}，压缩{len(compactions)}次", flush=True)


def main():
    parser = argparse.ArgumentParser(description="真实集群执行器长流程、两次压缩及进程恢复")
    parser.add_argument("--mode", choices=["run", "worker"], default="run")
    parser.add_argument("--stage", choices=["initial", "recovered"], default="initial")
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--cluster-config", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    if args.mode == "worker":
        asyncio.run(worker(args))
    else:
        execute(args)


if __name__ == "__main__":
    main()
