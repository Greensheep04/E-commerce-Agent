import argparse
import json
import subprocess
import tarfile
from hashlib import sha256
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ROOT_FILES = {".env.example", ".gitignore", "README.md", "pyproject.toml", "uv.lock", "alembic.ini"}
SOURCE_DIRS = {
    "src", "frontend", "alembic", "packages", "agent-hub", "agent-cluster",
    "data-center", "vendor", "tests", "scripts",
}
PRIVATE_NAMES = {
    ".git", ".local", ".scratch", ".venv", "node_modules", "__pycache__",
    ".env", "AGENTS.md", "CHANGELOG.md", "REUSE.md",
}
INTERNAL_FILES = {"vendor/poster-framework/docs/contributor-guide.md"}
REQUIRED_FILES = {
    "README.md", ".env.example", "pyproject.toml", "uv.lock", "frontend/package-lock.json",
    "agent-hub/uv.lock", "agent-cluster/uv.lock", "data-center/uv.lock",
    "packages/integration-protocol/pyproject.toml", "packages/integration-host/pyproject.toml",
    "packages/data-center-protocol/pyproject.toml", "vendor/poster-framework/pyproject.toml",
    "docs/使用说明/环境说明.md", "docs/使用说明/命令说明.md", "docs/使用说明/接口说明.md",
    "data-center/src/data_center/graph/engine.py",
}


def public_path(relative: str) -> bool:
    path = Path(relative)
    if (relative in INTERNAL_FILES or path.is_absolute() or ".." in path.parts
            or any(part in PRIVATE_NAMES for part in path.parts)):
        return False
    if path.name.startswith(".env") and relative != ".env.example":
        return False
    if path.suffix in {".password", ".key", ".pem", ".p12"}:
        return False
    return (
        relative in ROOT_FILES
        or (len(path.parts) == 1 and path.name.startswith(("LICENSE", "NOTICE")))
        or path.parts[0] in SOURCE_DIRS
        or path.parts[:2] == ("docs", "使用说明")
    )


def selected_files() -> list[str]:
    listing = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=ROOT, check=True, capture_output=True, encoding="utf-8",
    )
    selected = sorted({name for name in listing.stdout.split("\0") if name and public_path(name)})
    missing = REQUIRED_FILES.difference(selected)
    if missing:
        raise ValueError(f"发布包缺少必要文件：{sorted(missing)}")
    for name in selected:
        source = ROOT / name
        if source.is_symlink() or not source.is_file() or not source.resolve().is_relative_to(ROOT):
            raise ValueError(f"发布文件必须为项目内的普通文件：{name}")
    return selected


def archive_metadata(member: tarfile.TarInfo) -> tarfile.TarInfo:
    member.uid = member.gid = 0
    member.uname = member.gname = ""
    return member


def export_source(output: Path) -> dict:
    output = output.resolve()
    if not output.is_relative_to(ROOT / ".scratch" / "releases"):
        raise ValueError("发布包需要保存在项目.scratch/releases目录")
    manifest = output.with_name(output.name + ".manifest.json")
    if output.exists() or manifest.exists():
        raise FileExistsError("发布包或文件清单已经存在，请使用新的输出名称")
    selected = selected_files()
    original = {name: sha256((ROOT / name).read_bytes()).hexdigest() for name in selected}
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("xb") as stream, tarfile.open(fileobj=stream, mode="w:gz") as archive:
        for name in selected:
            archive.add(ROOT / name, arcname="project-shop/" + name,
                        recursive=False, filter=archive_metadata)
    with tarfile.open(output, "r:gz") as archive:
        members = archive.getmembers()
        expected = {"project-shop/" + name for name in selected}
        if len(members) != len(expected) or {item.name for item in members} != expected:
            raise ValueError("发布包的文件清单不一致")
        for member in members:
            if not member.isfile():
                raise ValueError("发布包包含非普通文件")
            name = member.name.removeprefix("project-shop/")
            with archive.extractfile(member) as stream:
                if sha256(stream.read()).hexdigest() != original[name]:
                    raise ValueError(f"发布包文件摘要不一致：{name}")
            if sha256((ROOT / name).read_bytes()).hexdigest() != original[name]:
                raise ValueError(f"导出期间源文件改变：{name}")
    report = {
        "status": "verified", "archive": str(output.relative_to(ROOT)),
        "archive_sha256": sha256(output.read_bytes()).hexdigest(),
        "file_count": len(selected), "files": original,
    }
    with manifest.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    return report


def main():
    parser = argparse.ArgumentParser(description="导出代码与使用文档组成的公开源码包")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = export_source(args.output)
    print(json.dumps({key: value for key, value in report.items() if key != "files"},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
