import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from time import monotonic, sleep

from sqlalchemy import text

from shop.config import Settings
from shop.harness import HarnessService


def main() -> None:
    evidence_dir = Path(sys.argv[1])
    mode = sys.argv[2]
    assert mode in {"checkpoint", "reserved"}
    flag = "--hold-at-checkpoint" if mode == "checkpoint" else "--hold-after-reserve"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    service = HarnessService(Settings())
    task = service.create("请推荐两件黑色男士 T 恤，只根据素材资料说明原因，并给出来源")
    task_id = task["task_id"]
    with (evidence_dir / f"sigkill-{mode}-child.stdout.txt").open("x") as stdout, \
         (evidence_dir / f"sigkill-{mode}-child.stderr.txt").open("x") as stderr:
            process = subprocess.Popen(
                [sys.executable, "scripts/harness.py", "run", task_id, flag],
                cwd=Path.cwd(), stdout=stdout, stderr=stderr,
            )
            try:
                deadline = monotonic() + 20
                before = None
                checkpoint_count = 0
                while monotonic() < deadline:
                    if process.poll() is not None:
                        raise AssertionError("子进程在 checkpoint 前终止")
                    before = service.show(task_id)
                    with service.maker() as session:
                        checkpoint_count = session.scalar(
                            text("SELECT count(*) FROM checkpoints WHERE thread_id=:task_id"),
                            {"task_id": task_id},
                        )
                    expected_status = "completed" if mode == "checkpoint" else "reserved"
                    minimum_checkpoints = 3 if mode == "checkpoint" else 1
                    if (checkpoint_count >= minimum_checkpoints and before["model_calls"] == 1
                            and before["tool_calls"] == 0
                            and before["requests"][0]["status"] == expected_status):
                        break
                    sleep(0.05)
                else:
                    raise AssertionError("等待 checkpoint 超时")
                process.kill()
                exit_code = process.wait(timeout=5)
                assert exit_code < 0
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
    with (evidence_dir / f"sigkill-{mode}-resume.stdout.json").open("x") as stdout, \
         (evidence_dir / f"sigkill-{mode}-resume.stderr.txt").open("x") as stderr:
            subprocess.run(
                [sys.executable, "scripts/harness.py", "resume", task_id],
                cwd=Path.cwd(), stdout=stdout, stderr=stderr, check=True, timeout=30,
            )
    after = service.show(task_id)
    assert after["status"] == "completed"
    assert len(after["result"]["recommendations"]) == 2
    assert after["requests"][0]["id"] == before["requests"][0]["id"]
    assert len(after["requests"]) == after["model_calls"]
    assert sum(call["status"] == "completed" for call in after["tools"]) == after["tool_calls"]
    with service.maker() as session:
        final_checkpoints = session.scalar(
            text("SELECT count(*) FROM checkpoints WHERE thread_id=:task_id"),
            {"task_id": task_id},
        )
    assert final_checkpoints > checkpoint_count
    report = {
        "task_id": task_id, "run_id": after["run_id"], "terminated_pid": process.pid,
        "termination_exit_code": exit_code,
        "checkpoints_before_termination": checkpoint_count,
        "checkpoints_after_resume": final_checkpoints,
        "request_status_before": before["requests"][0]["status"],
        "model_calls_before": before["model_calls"], "model_calls_after": after["model_calls"],
        "tool_calls_before": before["tool_calls"], "tool_calls_after": after["tool_calls"],
        "first_request_id": before["requests"][0]["id"],
        "final_status": after["status"],
        "result": after["result"],
        "estimated_cny": str(sum(
            (Decimal(request["charged_cny"]) for request in after["requests"]), Decimal(0)
        )),
    }
    with (evidence_dir / f"sigkill-{mode}-recovery-report.json").open("x") as output:
        json.dump(report, output, ensure_ascii=False, indent=2)
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
