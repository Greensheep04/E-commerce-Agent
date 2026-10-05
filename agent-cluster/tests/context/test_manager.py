import copy
import json

import pytest

from agent_cluster.types import CoordinatorProfile

from .fixtures import goal_and_run, protocol_round


@pytest.mark.asyncio
async def test_prepare_rebuilds_authority_preserves_provider_fields_and_archives(tmp_path):
    from agent_cluster.context import ContextManager
    from agent_cluster.notes import NoteStore

    goal, run = goal_and_run()
    history = [{"role": "user", "content": "人工编写的协议消息"}, *protocol_round(1)]
    original = copy.deepcopy(history)
    notes = NoteStore(tmp_path / "notes")
    manager = ContextManager(notes, CoordinatorProfile(), "完整系统指令", model=None)
    window = await manager.prepare(goal, run, {"tools": [{"name": "unit_protocol"}]}, history)
    assert history == original
    assert window.messages[1:] == original
    assert window.messages[0]["role"] == "system"
    system = window.messages[0]["content"]
    assert system.startswith("完整系统指令")
    authority = json.loads(system.split("\n\n", 1)[1])
    assert authority["goal"]["objective"] == "人工消息协议检查目标"
    assert authority["authorizations"] == {"unit_protocol": ["read"]}
    assert authority["budget"]["remaining_cny"] == "2.25"
    assert authority["criteria"][0]["path"] == ["data"]
    assert authority["required_confirmations"] == ["用户确认后执行"]
    assert authority["run"]["reserved_cny"] == "0.5"
    assert authority["catalog"] == {"tools": [{"name": "unit_protocol"}]}
    assert window.compacted is False
    raw_reference = next(reference for reference in window.archive_refs if reference.topic == "context-history")
    assert json.loads(notes.read(raw_reference, goal_id=goal.goal_id, actor_id=goal.actor_id)) == original
    goal.objective = "当前目标已经更新"
    goal.revision = 2
    run.goal_revision = 2
    next_window = await manager.prepare(goal, run, {}, history)
    assert "当前目标已经更新" in next_window.messages[0]["content"]
    next_authority = json.loads(next_window.messages[0]["content"].split("\n\n", 1)[1])
    assert next_authority["catalog"] == {}
    assert next_authority["authorizations"] == {"unit_protocol": ["read"]}
    assert len(notes.index(goal_id=goal.goal_id, actor_id=goal.actor_id)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [{"actor_id": "actor-other"}, {"goal_id": "goal-other"}, {"goal_revision": 2}])
async def test_prepare_rejects_wrong_run_authority(tmp_path, changes):
    from agent_cluster.context import ContextManager
    from agent_cluster.notes import NoteStore

    goal, run = goal_and_run()
    manager = ContextManager(NoteStore(tmp_path / "notes"), CoordinatorProfile(), "系统指令", model=None)
    with pytest.raises(ValueError, match="运行"):
        await manager.prepare(goal, run.model_copy(update=changes), {}, [])


@pytest.mark.asyncio
async def test_prepare_uses_goal_cumulative_budget_and_keeps_current_run_usage(tmp_path):
    from agent_cluster.context import ContextManager
    from agent_cluster.notes import NoteStore

    goal, run = goal_and_run()
    catalog = {"goal_budget": {"limit_cny": "3", "spent_cny": "2", "reserved_cny": "0.25",
                               "remaining_cny": "0.75"}}
    manager = ContextManager(NoteStore(tmp_path / "notes"), CoordinatorProfile(), "系统指令", model=None)
    window = await manager.prepare(goal, run, catalog, [])
    authority = json.loads(window.messages[0]["content"].split("\n\n", 1)[1])
    assert authority["budget"] == {"limit_cny": "3", "spent_cny": "2", "reserved_cny": "0.25",
                                   "remaining_cny": "0.75"}
    assert authority["run"]["spent_cny"] == "0.25"
    assert authority["run"]["reserved_cny"] == "0.5"


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [{"limit_cny": "4"}, {"remaining_cny": "2"},
                                     {"spent_cny": "-1"}, {"reserved_cny": "NaN"}])
async def test_prepare_rejects_inconsistent_goal_cumulative_budget(tmp_path, changes):
    from agent_cluster.context import ContextManager
    from agent_cluster.notes import NoteStore

    goal, run = goal_and_run()
    budget = {"limit_cny": "3", "spent_cny": "2", "reserved_cny": "0.25", "remaining_cny": "0.75"}
    notes = NoteStore(tmp_path / "notes")
    manager = ContextManager(notes, CoordinatorProfile(), "系统指令", model=None)
    with pytest.raises(ValueError):
        await manager.prepare(goal, run, {"goal_budget": {**budget, **changes}}, [])
    assert notes.index(goal_id=goal.goal_id, actor_id=goal.actor_id) == []


def test_compaction_keeps_recent_complete_tool_pairs():
    from agent_cluster.context.manager import compactable_prefix

    history = [message for number in range(5) for message in protocol_round(number)]
    assert compactable_prefix(history) == 4
    history.append({"role": "user", "content": "新消息"})
    assert compactable_prefix(history) == 4


def test_compaction_waits_for_every_tool_result():
    from agent_cluster.context.manager import compactable_prefix

    history = [message for number in range(5) for message in protocol_round(number)]
    history.extend(protocol_round(6)[:1])
    assert compactable_prefix(history) == 0
    history.append(protocol_round(6)[1])
    assert compactable_prefix(history) == 6


def test_compaction_waits_for_every_result_in_multi_call_round():
    from agent_cluster.context.manager import compactable_prefix

    history = [message for number in range(5) for message in protocol_round(number)]
    assistant = protocol_round(6)[0]
    assistant["tool_calls"].append(protocol_round(7)[0]["tool_calls"][0])
    history.extend([assistant, protocol_round(7)[1]])
    assert compactable_prefix(history) == 0
    history.append(protocol_round(6)[1])
    assert compactable_prefix(history) == 6


def test_compaction_does_not_split_multi_call_round_at_retention_boundary():
    from agent_cluster.context.manager import compactable_prefix

    history = protocol_round(0)
    assistant = protocol_round(1)[0]
    assistant["tool_calls"].append(protocol_round(2)[0]["tool_calls"][0])
    history.extend([assistant, protocol_round(1)[1], protocol_round(2)[1]])
    history.extend([{"role": "user", "content": "人工消息"}] * 5)
    assert compactable_prefix(history) == 2


def test_compaction_preserves_plain_confirmation_information():
    from agent_cluster.context.manager import compactable_prefix

    history = [{"role": "assistant", "content": "人工协议提示：等待用户确认"}]
    history.extend(message for number in range(6) for message in protocol_round(number))
    assert compactable_prefix(history) == 0


def test_compaction_protects_pending_operation_until_status_update():
    from agent_cluster.context.manager import compactable_prefix

    history = [*protocol_round(0, status="waiting_confirmation")]
    history.extend(message for number in range(1, 7) for message in protocol_round(number))
    assert compactable_prefix(history) == 0
    history.append({"role": "user", "content": json.dumps({
        "task_id": "task-0", "request_id": "request-0", "status": "completed", "result": {"status": "completed"},
    })})
    assert compactable_prefix(history) == 8


def test_compaction_protects_nested_business_waiting_status():
    from agent_cluster.context.manager import compactable_prefix

    history = protocol_round(0)
    payload = json.loads(history[1]["content"])
    payload["result"]["status"] = "waiting_agents"
    history[1]["content"] = json.dumps(payload)
    history.extend(message for number in range(1, 7) for message in protocol_round(number))
    assert compactable_prefix(history) == 0


def test_compaction_rejects_orphan_or_duplicate_tool_results():
    from agent_cluster.context.manager import compactable_prefix

    with pytest.raises(ValueError, match="工具"):
        compactable_prefix([protocol_round(0)[1]])
    with pytest.raises(ValueError, match="工具"):
        compactable_prefix([*protocol_round(0), protocol_round(0)[1]])


def test_token_estimate_includes_reasoning_and_catalog():
    from agent_cluster.context.manager import estimate_input_tokens

    small = [{"role": "assistant", "content": "普通消息", "reasoning_content": ""}]
    reasoning = [{**small[0], "reasoning_content": "思考内容" * 1000}]
    assert estimate_input_tokens(reasoning, {}) > estimate_input_tokens(small, {}) + 1000
    assert estimate_input_tokens(small, {"schema": "参数字段" * 1000}) > estimate_input_tokens(small, {}) + 1000


@pytest.mark.asyncio
async def test_prepare_preserves_open_round_and_rejects_irreducible_overflow(tmp_path):
    from agent_cluster.context import ContextManager
    from agent_cluster.notes import NoteStore

    goal, run = goal_and_run()
    profile = CoordinatorProfile(context_tokens=4096, output_limit=128)
    manager = ContextManager(NoteStore(tmp_path / "notes"), profile, "系统指令", model=None)
    history = protocol_round(0)[:1]
    window = await manager.prepare(goal, run, {}, history)
    assert window.messages[-1] == history[-1]
    oversized = [{"role": "user", "content": "人工长消息" * 1000}]
    with pytest.raises(ValueError, match="上下文"):
        await manager.prepare(goal, run, {}, oversized)
