import hashlib
import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from integration_protocol import ResourceRef


def write_note(store, **changes):
    values = {
        "goal_id": "goal-one", "actor_id": "actor-one", "topic": "协议检查资料",
        "content": "人工编写的文件检查资料\r\n保留完整正文\n", "source": "unit-file-source",
        "summary": "文件检查摘要", "note_id": "note-one",
    }
    return store.write(**{**values, **changes})


def test_note_preserves_text_resources_and_independent_versions(tmp_path):
    from agent_cluster.notes import NoteStore

    store = NoteStore(tmp_path / "notes")
    resource = ResourceRef(module_id="materials", resource_type="unit-file",
                           resource_id="source-one", version="1", sha256="a" * 64)
    first = write_note(store, resources=[resource])
    second = write_note(store, content="第二份人工文件资料", summary="第二份摘要")
    assert first.version == 1
    assert second.version == 2
    assert first.resources == [resource]
    assert first.sha256 == hashlib.sha256("人工编写的文件检查资料\r\n保留完整正文\n".encode()).hexdigest()
    restarted = NoteStore(tmp_path / "notes")
    assert restarted.read(first, goal_id="goal-one", actor_id="actor-one") == "人工编写的文件检查资料\r\n保留完整正文\n"
    assert restarted.read(second, goal_id="goal-one", actor_id="actor-one") == "第二份人工文件资料"
    assert restarted.index(goal_id="goal-one", actor_id="actor-one") == [second]


@pytest.mark.parametrize("changes", [
    {"goal_id": "other-goal"}, {"actor_id": "other-actor"},
    {"version": 99}, {"sha256": "b" * 64}, {"summary": "被修改的摘要"},
    {"source": "different-source"}, {"topic": "其他主题"},
])
def test_note_rejects_changed_reference_fields(tmp_path, changes):
    from agent_cluster.notes import NoteStore

    store = NoteStore(tmp_path / "notes")
    reference = write_note(store)
    altered = reference.model_copy(update=changes)
    with pytest.raises((ValueError, FileNotFoundError, PermissionError)):
        store.read(altered, goal_id="goal-one", actor_id="actor-one")


@pytest.mark.parametrize("field,value", [
    ("goal_id", "../other"), ("actor_id", "/other"), ("note_id", "a/b"),
    ("note_id", ".."), ("goal_id", "~actor"), ("actor_id", "a\\b"),
])
def test_note_rejects_path_traversal_before_writing(tmp_path, field, value):
    from agent_cluster.notes import NoteStore

    store = NoteStore(tmp_path / "notes")
    with pytest.raises(ValueError, match="编号"):
        write_note(store, **{field: value})
    assert list((tmp_path / "notes").rglob("*.txt")) == []


def test_note_detects_body_tampering(tmp_path):
    from agent_cluster.notes import NoteStore

    store = NoteStore(tmp_path / "notes")
    reference = write_note(store)
    body = next((tmp_path / "notes").rglob("*.txt"))
    body.write_text("被修改的文件正文", encoding="utf-8")
    with pytest.raises(ValueError, match="摘要"):
        store.read(reference, goal_id="goal-one", actor_id="actor-one")


def test_note_scope_rejects_symlink_to_another_identity(tmp_path):
    from agent_cluster.notes import NoteStore

    store = NoteStore(tmp_path / "notes")
    write_note(store)
    (tmp_path / "notes" / "other-actor").symlink_to(tmp_path / "notes" / "actor-one", target_is_directory=True)
    with pytest.raises(ValueError, match="符号链接"):
        store.index(goal_id="goal-one", actor_id="other-actor")


def test_note_concurrent_writes_keep_every_version(tmp_path):
    from agent_cluster.notes import NoteStore

    store = NoteStore(tmp_path / "notes")
    with ThreadPoolExecutor(max_workers=4) as executor:
        references = list(executor.map(lambda value: write_note(store, content=f"人工文件资料 {value}"), range(12)))
    assert sorted(reference.version for reference in references) == list(range(1, 13))
    assert len(list((tmp_path / "notes").rglob("*.txt"))) == 12
    for reference in references:
        assert store.read(reference, goal_id="goal-one", actor_id="actor-one").startswith("人工文件资料 ")


def test_note_index_validates_stored_scope(tmp_path):
    from agent_cluster.notes import NoteStore

    store = NoteStore(tmp_path / "notes")
    write_note(store)
    manifest = next((tmp_path / "notes").rglob("*.json"))
    data = json.loads(manifest.read_text(encoding="utf-8"))
    data["actor_id"] = "other-actor"
    manifest.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match="范围"):
        store.index(goal_id="goal-one", actor_id="actor-one")
