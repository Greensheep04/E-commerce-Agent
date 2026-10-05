import asyncio
import hashlib
import importlib.metadata
import importlib.util
import json
import sys
from pathlib import Path

import integration_host
import integration_protocol
from integration_protocol import CallContext, OperationResult


async def verify_host():
    host = integration_host.ModuleHost()
    assert host.describe() == []
    await host.startup()
    try:
        await host.invoke(
            "unregistered", "read", {}, CallContext(actor_id="actor-1", request_id="request-1")
        )
    except integration_host.UnknownModule:
        pass
    else:
        raise AssertionError("未登记模块必须拒绝调用")
    await host.shutdown()


def main():
    evidence_dir = Path(sys.argv[1]).resolve()
    assert importlib.util.find_spec("shop") is None
    assert importlib.util.find_spec("sqlalchemy") is None
    result = OperationResult(data={"source_id": "source-1"}, status="completed")
    assert result.model_dump(mode="json")["data"] == {"source_id": "source-1"}
    asyncio.run(verify_host())
    versions = {
        package: importlib.metadata.version(package)
        for package in ("integration-protocol", "integration-host", "pydantic", "fastapi")
    }
    wheels = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted((evidence_dir / "wheels").glob("*.whl"))
    }
    packages_dir = Path(__file__).resolve().parents[2]
    sources = {}
    for package_name, module in (
        ("integration-protocol", integration_protocol), ("integration-host", integration_host)
    ):
        source_dir = packages_dir / package_name / "src" / module.__name__
        installed_dir = Path(module.__file__).parent
        for path in sorted(source_dir.rglob("*.py")):
            relative = path.relative_to(source_dir)
            assert path.read_bytes() == (installed_dir / relative).read_bytes()
            key = str(path.relative_to(packages_dir))
            sources[key] = hashlib.sha256(path.read_bytes()).hexdigest()
    report = {
        "milestone": "IF-01",
        "python": sys.version,
        "versions": versions,
        "wheels_sha256": wheels,
        "sources_sha256": sources,
        "protocol_import": integration_protocol.__file__,
        "host_import": integration_host.__file__,
        "shop_installed": False,
        "sqlalchemy_installed": False,
        "host_startup_shutdown": "passed",
        "unknown_module_rejected": "passed",
    }
    (evidence_dir / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
