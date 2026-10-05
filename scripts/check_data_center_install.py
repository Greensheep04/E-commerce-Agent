import argparse
import hashlib
import json
import os
import shutil
import subprocess
import time
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE_PATHS = {
    "integration-protocol": ROOT / "packages/integration-protocol",
    "data-center-protocol": ROOT / "packages/data-center-protocol",
    "data-center": ROOT / "data-center",
}
SOURCE_ROOTS = {
    "integration-protocol": ROOT / "packages/integration-protocol/src",
    "data-center-protocol": ROOT / "packages/data-center-protocol/src",
    "data-center": ROOT / "data-center/src",
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_manifest():
    manifest = {}
    for package, source_root in SOURCE_ROOTS.items():
        paths = sorted(path for path in source_root.rglob("*")
                       if path.is_file() and path.suffix in {".py", ".sql", ".yml", ".yaml"})
        paths.append(PACKAGE_PATHS[package] / "pyproject.toml")
        if package == "data-center":
            paths.append(PACKAGE_PATHS[package] / "uv.lock")
        package_files = {str(path.relative_to(ROOT)): digest(path) for path in paths}
        package_hash = hashlib.sha256()
        for relative_path, file_hash in package_files.items():
            package_hash.update(relative_path.encode())
            package_hash.update(b"\0")
            package_hash.update(file_hash.encode())
            package_hash.update(b"\n")
        manifest[package] = {"sha256": package_hash.hexdigest(), "files": package_files}
    return manifest


def run_command(name, arguments, directory, command_log, environment):
    started = time.monotonic()
    result = subprocess.run(
        [str(value) for value in arguments],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    elapsed = time.monotonic() - started
    stdout_path = directory / f"{name}.stdout.txt"
    stderr_path = directory / f"{name}.stderr.txt"
    stdout_path.write_text(result.stdout, encoding="utf-8")
    stderr_path.write_text(result.stderr, encoding="utf-8")
    entry = {
        "name": name,
        "command": [str(value) for value in arguments],
        "returncode": result.returncode,
        "elapsed_seconds": round(elapsed, 6),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "stdout_sha256": digest(stdout_path),
        "stderr_sha256": digest(stderr_path),
    }
    with command_log.open("a", encoding="utf-8") as file:
        file.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
        file.flush()
        os.fsync(file.fileno())
    if result.returncode:
        raise subprocess.CalledProcessError(result.returncode, arguments, result.stdout, result.stderr)
    return result.stdout.strip()


def package_version(lock_data, package_name):
    canonical_name = package_name.lower().replace("_", "-")
    matches = [item["version"] for item in lock_data["package"]
               if item["name"].lower().replace("_", "-") == canonical_name]
    if len(matches) != 1:
        raise ValueError(f"锁文件中包版本记录数量异常: {package_name}")
    return matches[0]


def verify_installed_environment(python, environment_root, modules, distributions, no_shop=False):
    code = "\n".join((
        "import importlib",
        "import importlib.metadata",
        "import importlib.util",
        "import json",
        "from pathlib import Path",
        "environment_root = Path(" + repr(str(environment_root)) + ")",
        "module_names = " + repr(modules),
        "distribution_names = " + repr(distributions),
        "modules = {name: importlib.import_module(name) for name in module_names}",
        "module_paths = {name: str(Path(module.__file__).resolve()) for name, module in modules.items()}",
        "versions = {name: importlib.metadata.version(name) for name in distribution_names}",
        "if not all(Path(path).is_relative_to(environment_root) for path in module_paths.values()): raise RuntimeError('模块没有从独立环境导入')",
        "if importlib.util.find_spec('shop') is not None: raise RuntimeError('独立公共环境发现 shop')" if no_shop else "",
        "if importlib.util.find_spec('sqlalchemy') is not None: raise RuntimeError('独立公共环境发现 SQLAlchemy')" if no_shop else "",
        "print(json.dumps({'module_paths': module_paths, 'versions': versions}, sort_keys=True))",
    ))
    return [str(python), "-c", code]


def main():
    parser = argparse.ArgumentParser(description="构建并核查数据中台独立Python wheel安装")
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    directory = args.directory.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    wheels = directory / "wheels"
    wheels.mkdir()
    command_log = directory / "commands.jsonl"
    command_log.touch()
    uv = shutil.which("uv")
    if uv is None:
        raise FileNotFoundError("找不到 uv 可执行文件")
    uv_cache = directory / "uv-cache"
    cached_uv_data = ROOT / ".scratch/uv-cache"
    shutil.copytree(cached_uv_data, uv_cache)
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    environment.pop("PYTHONHOME", None)
    environment["PYTHONNOUSERSITE"] = "1"
    environment["UV_CACHE_DIR"] = str(uv_cache)
    environment["UV_LINK_MODE"] = "copy"

    source_before = source_manifest()
    project_lock = tomllib.loads((ROOT / "uv.lock").read_text(encoding="utf-8"))
    service_lock_path = PACKAGE_PATHS["data-center"] / "uv.lock"
    service_lock = tomllib.loads(service_lock_path.read_text(encoding="utf-8"))
    command_environment = environment.copy()

    for package in ("integration-protocol", "data-center-protocol", "data-center"):
        run_command(f"build-{package}",
                    [uv, "build", "--offline", "--wheel", PACKAGE_PATHS[package], "--out-dir", wheels],
                    directory, command_log, command_environment)

    wheel_paths = {path.name: path for path in sorted(wheels.glob("*.whl"))}
    expected_wheels = {
        "integration-protocol": "integration_protocol-0.1.0-py3-none-any.whl",
        "data-center-protocol": "data_center_protocol-0.1.0-py3-none-any.whl",
        "data-center": "data_center-0.1.0-py3-none-any.whl",
    }
    for package, wheel_name in expected_wheels.items():
        if wheel_name not in wheel_paths:
            raise FileNotFoundError(f"构建结果缺少wheel: {wheel_name}")

    env311 = directory / "python311"
    run_command("create-python311-environment", [uv, "venv", "--python", "3.11", env311],
                directory, command_log, command_environment)
    python311 = env311 / "bin/python"
    protocol_environment = command_environment.copy()
    run_command(
        "install-python311-protocol-wheels",
        [uv, "pip", "install", "--offline", "--python", python311, "--find-links", wheels,
         f"integration-protocol=={package_version(project_lock, 'integration-protocol')}",
         f"data-center-protocol=={package_version(project_lock, 'data-center-protocol')}",
         f"pydantic=={package_version(project_lock, 'pydantic') }"],
        directory, command_log, protocol_environment,
    )
    protocol_imports = verify_installed_environment(
        python311, env311, ["data_center_protocol"],
        ["data-center-protocol", "integration-protocol", "pydantic"], no_shop=True,
    )
    run_command("verify-python311-protocol", protocol_imports, directory, command_log,
                protocol_environment)

    env314 = directory / "python314"
    run_command("create-python314-environment", [uv, "venv", "--python", "3.14", env314],
                directory, command_log, command_environment)
    python314 = env314 / "bin/python"
    service_environment = command_environment.copy()
    service_environment["UV_PROJECT_ENVIRONMENT"] = str(env314)
    run_command(
        "install-python314-locked-dependencies",
        [uv, "sync", "--project", PACKAGE_PATHS["data-center"], "--python", python314,
         "--locked", "--no-install-project", "--no-install-package", "data-center-protocol",
         "--no-install-package", "integration-protocol"],
        directory, command_log, service_environment,
    )
    run_command(
        "install-python314-wheels",
        [uv, "pip", "install", "--python", python314, "--no-deps", "--reinstall",
         wheel_paths[expected_wheels["integration-protocol"]],
         wheel_paths[expected_wheels["data-center-protocol"]],
         wheel_paths[expected_wheels["data-center"]]],
        directory, command_log, service_environment,
    )
    service_distributions = [
        "data-center", "data-center-protocol", "integration-protocol", "docling",
        "lightrag-hku", "dlt", "dbt-core", "metricflow", "SQLAlchemy",
    ]
    service_modules = ["data_center", "data_center_protocol", "integration_protocol",
                       "docling", "lightrag", "dlt", "dbt", "metricflow"]
    service_import = verify_installed_environment(
        python314, env314, service_modules, service_distributions,
    )
    service_import_result = run_command("verify-python314-imports", service_import,
                                        directory, command_log, service_environment)
    installed_versions = json.loads(service_import_result)["versions"]
    expected_versions = {
        name: package_version(service_lock, name)
        for name in service_distributions
    }
    if installed_versions != expected_versions:
        raise RuntimeError("独立服务环境的已安装版本与 data-center/uv.lock 不一致")

    for name, executable in (("data-center-help", "data-center"), ("dbt-help", "dbt"),
                             ("dlt-help", "dlt")):
        run_command(name, [env314 / "bin" / executable, "--help"], directory,
                    command_log, service_environment)

    source_after = source_manifest()
    if source_before != source_after:
        raise RuntimeError("独立安装核查期间源码或锁文件发生变化")
    wheel_summary = {
        package: {"path": str(wheel_paths[wheel_name]), "sha256": digest(wheel_paths[wheel_name]),
                  "size_bytes": wheel_paths[wheel_name].stat().st_size}
        for package, wheel_name in expected_wheels.items()
    }
    report = {
        "status": "passed",
        "python311_protocol": {
            "python": str(python311),
            "imports": ["data_center_protocol"],
            "versions": json.loads(
                run_command("python311-final-version-check",
                            verify_installed_environment(
                                python311, env311, ["data_center_protocol"],
                                ["data-center-protocol", "integration-protocol", "pydantic"],
                                no_shop=True,
                            ), directory, command_log, protocol_environment)
            )["versions"],
            "shop_absent": True,
            "sqlalchemy_absent": True,
        },
        "python314_service": {
            "python": str(python314),
            "modules": service_modules,
            "versions": installed_versions,
            "locked_versions": expected_versions,
            "cli_help": ["data-center", "dbt", "dlt"],
        },
        "wheels": wheel_summary,
        "source_manifest": source_before,
        "lock_sha256": {
            "python311": digest(ROOT / "uv.lock"),
            "python314": digest(service_lock_path),
        },
        "commands_path": str(command_log),
        "command_count": sum(1 for _ in command_log.open(encoding="utf-8")),
    }
    report_path = directory / "report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
                           encoding="utf-8")
    print(json.dumps({"status": report["status"], "command_count": report["command_count"],
                      "report": str(report_path)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
