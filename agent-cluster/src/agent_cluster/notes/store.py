import fcntl
import hashlib
import json
import os
import re
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from deepagents.backends import FilesystemBackend
from integration_protocol import ResourceRef

from agent_cluster.types import NoteRef


def validate_identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value) or ".." in value:
        raise ValueError("编号必须使用安全的英文、数字或连接符")
    return value


class NoteStore:
    def __init__(self, root: Path):
        root = Path(root)
        if root.is_symlink():
            raise ValueError("Note根目录不能使用符号链接")
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root = root.resolve()

    def _scope(self, *, goal_id: str, actor_id: str) -> Path:
        goal_id, actor_id = validate_identifier(goal_id), validate_identifier(actor_id)
        scope = self.root / actor_id / goal_id
        self._path(scope)
        scope.mkdir(parents=True, exist_ok=True, mode=0o700)
        return scope

    def _path(self, path: Path) -> Path:
        relative = path.relative_to(self.root)
        current = self.root
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                raise ValueError("Note文件范围不能使用符号链接")
        if not path.resolve().is_relative_to(self.root):
            raise ValueError("Note文件超出允许范围")
        return path

    def _backend_for(self, *, goal_id: str, actor_id: str) -> FilesystemBackend:
        return FilesystemBackend(root_dir=self._scope(goal_id=goal_id, actor_id=actor_id), virtual_mode=True)

    @contextmanager
    def _locked(self, scope: Path):
        path = self._path(scope / ".lock")
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "a+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _download(self, backend: FilesystemBackend, path: str) -> bytes:
        self._path(backend.cwd / path.lstrip("/"))
        response = backend.download_files([path])[0]
        if response.error is not None:
            raise FileNotFoundError(f"Note文件读取失败：{response.error}")
        if response.content is None:
            raise ValueError("Note文件缺少正文")
        return response.content

    def _write(self, backend: FilesystemBackend, path: str, content: str) -> None:
        local = self._path(backend.cwd / path.lstrip("/"))
        if local.exists():
            raise FileExistsError("Note历史版本已经存在")
        response = backend.write(path, content)
        if response.error is not None:
            raise OSError(f"Note文件写入失败：{response.error}")
        os.chmod(local, 0o600)
        with local.open("rb") as handle:
            os.fsync(handle.fileno())
        with self._directory_handle(local.parent) as descriptor:
            os.fsync(descriptor)

    @staticmethod
    @contextmanager
    def _directory_handle(path: Path):
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            yield descriptor
        finally:
            os.close(descriptor)

    def _references(self, backend: FilesystemBackend, *, goal_id: str, actor_id: str) -> list[NoteRef]:
        root = self._path(backend.cwd / "index")
        if not root.exists():
            return []
        references = []
        for directory in sorted(root.iterdir()):
            self._path(directory)
            note_id = validate_identifier(directory.name)
            if not directory.is_dir():
                raise ValueError("Note索引目录无效")
            for path in sorted(directory.glob("*.json")):
                path = self._path(path)
                reference = NoteRef.model_validate_json(self._download(backend, "/" + path.relative_to(backend.cwd).as_posix()))
                if reference.goal_id != goal_id or reference.actor_id != actor_id or reference.note_id != note_id:
                    raise ValueError("Note索引访问范围不匹配")
                if path.name != f"{reference.version:08d}.json":
                    raise ValueError("Note索引版本不匹配")
                references.append(reference)
        return references

    def write(self, *, goal_id: str, actor_id: str, topic: str, content: str, source: str,
              summary: str, resources: list[ResourceRef] | None = None, note_id: str | None = None) -> NoteRef:
        note_id = validate_identifier(note_id if note_id is not None else str(uuid4()))
        if not isinstance(content, str):
            raise TypeError("Note正文必须为字符串")
        scope = self._scope(goal_id=goal_id, actor_id=actor_id)
        backend = FilesystemBackend(root_dir=scope, virtual_mode=True)
        with self._locked(scope):
            references = self._references(backend, goal_id=goal_id, actor_id=actor_id)
            versions = [reference.version for reference in references if reference.note_id == note_id]
            body_directory = self._path(scope / "notes" / note_id)
            if body_directory.exists():
                versions.extend(int(path.stem) for path in body_directory.glob("*.txt"))
            version = max(versions, default=0) + 1
            reference = NoteRef(note_id=note_id, goal_id=goal_id, actor_id=actor_id, version=version,
                                sha256=hashlib.sha256(content.encode("utf-8")).hexdigest(), topic=topic,
                                summary=summary, source=source, resources=resources or [])
            self._write(backend, f"/notes/{note_id}/{version:08d}.txt", content)
            pending_path = f"/pending/{uuid4()}.pending"
            self._write(backend, pending_path, json.dumps(reference.model_dump(mode="json"), ensure_ascii=False,
                                                        sort_keys=True, allow_nan=False))
            final = self._path(scope / "index" / note_id / f"{version:08d}.json")
            final.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.link(self._path(scope / pending_path.lstrip("/")), final, follow_symlinks=False)
            with self._directory_handle(final.parent) as descriptor:
                os.fsync(descriptor)
            return reference

    def read(self, reference: NoteRef, *, goal_id: str, actor_id: str) -> str:
        reference = NoteRef.model_validate(reference)
        if reference.goal_id != goal_id or reference.actor_id != actor_id:
            raise PermissionError("Note引用访问范围不匹配")
        validate_identifier(reference.note_id)
        backend = self._backend_for(goal_id=goal_id, actor_id=actor_id)
        manifest = f"/index/{reference.note_id}/{reference.version:08d}.json"
        stored = NoteRef.model_validate_json(self._download(backend, manifest))
        if stored != reference:
            raise ValueError("Note引用版本、摘要或来源不匹配")
        raw = self._download(backend, f"/notes/{reference.note_id}/{reference.version:08d}.txt")
        if hashlib.sha256(raw).hexdigest() != reference.sha256:
            raise ValueError("Note正文摘要不匹配")
        return raw.decode("utf-8")

    def index(self, *, goal_id: str, actor_id: str) -> list[NoteRef]:
        backend = self._backend_for(goal_id=goal_id, actor_id=actor_id)
        with self._locked(backend.cwd):
            latest = {}
            for reference in self._references(backend, goal_id=goal_id, actor_id=actor_id):
                if reference.note_id not in latest or reference.version > latest[reference.note_id].version:
                    latest[reference.note_id] = reference
            return [latest[note_id] for note_id in sorted(latest)]
