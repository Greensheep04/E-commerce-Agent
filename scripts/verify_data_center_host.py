import argparse
import asyncio
import json
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import httpx
from data_center.ports import ShopPorts
from integration_protocol import CallContext
from pydantic import SecretStr



async def verify(args):
    identities = json.loads(args.identities.read_text())["actors"]
    ports = ShopPorts(args.shop_url)
    contexts = {}
    for name in ("sync", "assessment", "assessment-manager", "customer", "other"):
        identity = identities[name]
        context = await ports.login(identity["username"], Path(identity["password_file"]).read_text().strip(),
                                    str(uuid4()))
        if context.actor_id != identity["actor_id"]:
            raise AssertionError("真实登录身份与固定身份不一致")
        contexts[name] = context
    source_id = "microsoft/adventureworks"
    scope = await ports.authorize(contexts["assessment"], [source_id], "read")
    assert scope.employee_ids == ["275"] and not scope.organization
    snapshot = await ports.snapshot(source_id, contexts["sync"])
    page = await ports.read_page(source_id, snapshot.snapshot_id, None, 1000, contexts["sync"])
    assert page.next_cursor is not None and len(page.rows) == 1000
    following = await ports.read_page(source_id, snapshot.snapshot_id, page.next_cursor, 1000,
                                      contexts["sync"])
    assert following.next_cursor is None
    rows = [*page.rows, *following.rows]
    assert sum(row["record_type"] == "order" for row in rows) == 1022
    assert sum(row["record_type"] == "quota" for row in rows) == 14
    assert len({(row["record_type"], row["record_id"]) for row in rows}) == len(rows)
    assert page.snapshot.snapshot_id == following.snapshot.snapshot_id == snapshot.snapshot_id
    for name in ("assessment", "customer"):
        try:
            await ports.read_page(source_id, snapshot.snapshot_id, None, 1000, contexts[name])
        except PermissionError:
            pass
        else:
            raise AssertionError("前台账号执行了后台完整来源读取")
    try:
        await ports.authorize(contexts["sync"], [source_id], "read")
    except PermissionError:
        pass
    else:
        raise AssertionError("同步账号获得了前台查询权限")
    forged = CallContext(actor_id=contexts["other"].actor_id, request_id=str(uuid4()),
                         credential=SecretStr(contexts["customer"].credential.get_secret_value()))
    try:
        await ports.authorize(forged, [source_id], "read")
    except PermissionError:
        pass
    else:
        raise AssertionError("伪造actor通过宿主凭据检查")
    async with httpx.AsyncClient(trust_env=False) as client:
        response = await client.post(args.shop_url + "/api/data-center/host/authorize",
                                     json={"source_ids": [source_id], "action": "read"})
        assert response.status_code == 401
    report = {"status": "passed", "run_id": str(uuid4()), "snapshot": snapshot.model_dump(mode="json"),
              "rows": len(rows), "orders": 1022, "quotas": 14,
              "checks": ["real_login", "employee_scope", "source_page_snapshot", "source_page_identity",
                         "frontend_sync_denied", "sync_frontend_denied", "forged_actor_denied", "anonymous_denied"],
              "source_rows_sha256": sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest(),
              "model_calls": 0}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    await ports.close()
    print(json.dumps({"status": "passed", "rows": len(rows), "checks": len(report["checks"])}))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shop-url", default="http://127.0.0.1:8002")
    parser.add_argument("--identities", type=Path, default=Path(".scratch/evidence/dc02-host-20261004-r1/identities.json"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(verify(args))


if __name__ == "__main__":
    main()
