import json
from datetime import timedelta
from decimal import Decimal

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from agent_cluster.context.model import PortChatModel, provider_messages
from agent_cluster.notes import NoteStore
from agent_cluster.types import CoordinatorProfile

from .fixtures import goal_and_run, protocol_round


def test_provider_conversion_keeps_original_reasoning_and_tool_arguments():
    calls = [{"id": "call-one", "type": "function", "function": {
        "name": "unit_protocol", "arguments": '{ "action" : "read" }',
    }}]
    message = AIMessage(content="人工协议消息", additional_kwargs={
        "reasoning_content": "完整原始reasoning内容\n第二行", "tool_calls": calls,
    })
    converted = provider_messages([SystemMessage(content="系统"), HumanMessage(content="用户"), message,
                                   ToolMessage(content="人工协议结果文字", tool_call_id="call-one")])
    assert converted == [
        {"role": "system", "content": "系统"}, {"role": "user", "content": "用户"},
        {"role": "assistant", "content": "人工协议消息", "reasoning_content": "完整原始reasoning内容\n第二行",
         "tool_calls": calls},
        {"role": "tool", "content": "人工协议结果文字", "tool_call_id": "call-one"},
    ]
    converted[2]["tool_calls"][0]["function"]["arguments"] = "changed"
    assert message.additional_kwargs["tool_calls"][0]["function"]["arguments"] == '{ "action" : "read" }'


@pytest.mark.asyncio
async def test_summary_component_requires_real_model_port_and_preserves_archive(tmp_path):
    from agent_cluster.context import ContextManager

    goal, run = goal_and_run()
    notes = NoteStore(tmp_path / "notes")
    profile = CoordinatorProfile(context_tokens=16384, output_limit=128, compression_ratio=0.2)
    manager = ContextManager(notes, profile, "完整系统指令", model=None)
    history = [message for number in range(8) for message in protocol_round(number)]
    with pytest.raises(ValueError, match="ModelPort"):
        await manager.prepare(goal, run, {}, history)
    references = notes.index(goal_id=goal.goal_id, actor_id=goal.actor_id)
    archive = next(reference for reference in references if reference.topic == "context-archive")
    archived = json.loads(notes.read(archive, goal_id=goal.goal_id, actor_id=goal.actor_id))
    assert archived == history[:10]
    assert archived[0]["reasoning_content"] == "人工协议消息 0"
    assert all(reference.topic != "context-compaction" for reference in references)


@pytest.mark.asyncio
async def test_summary_adapter_rejects_unsupported_generation_settings():
    model = PortChatModel(port=None, run_id="run-one", step_key="summary-one",
                          coordinator_profile=CoordinatorProfile(),
                          system_message={"role": "system", "content": "人工协议系统指令"})
    with pytest.raises(ValueError, match="未声明"):
        await model.ainvoke("人工摘要输入", stop=["STOP"])
    with pytest.raises(RuntimeError, match="异步"):
        model.invoke("人工摘要输入")


@pytest.mark.asyncio
async def test_summary_adapter_checks_actual_serialized_input_before_request():
    model = PortChatModel(port=None, run_id="run-one", step_key="summary-one",
                          coordinator_profile=CoordinatorProfile(context_tokens=4096, output_limit=128),
                          system_message={"role": "system", "content": "人工协议系统指令"})
    with pytest.raises(ValueError, match="上下文"):
        await model.ainvoke("人工摘要输入" * 1000)


def test_summary_request_uses_current_input_upper_bound_and_provider_fields():
    from agent_cluster.context.model import build_summary_request

    messages = [HumanMessage(content="人工摘要请求")]
    system = {"role": "system", "content": "人工协议系统指令：目标、授权、预算及完成标准"}
    deepseek_body, input_limit = build_summary_request(messages, CoordinatorProfile(), system)
    assert deepseek_body == {
        "model": "deepseek-flash", "messages": [system, {"role": "user", "content": "人工摘要请求"}],
        "stream": False, "thinking": {"type": "enabled"}, "reasoning_effort": "high", "max_tokens": 8192,
    }
    assert 0 < input_limit < 500
    qwen_body, qwen_input_limit = build_summary_request(
        messages, CoordinatorProfile(service="dashscope", model="qwen3.8-flash"), system)
    assert qwen_body == {
        "model": "qwen3.8-flash", "messages": [system, {"role": "user", "content": "人工摘要请求"}],
        "stream": True, "stream_options": {"include_usage": True}, "enable_thinking": True,
        "thinking_budget": 6144, "max_completion_tokens": 8192,
    }
    assert qwen_input_limit == input_limit


def test_summary_request_rejects_missing_system_authority():
    from agent_cluster.context.model import build_summary_request

    with pytest.raises(ValueError, match="system"):
        build_summary_request([HumanMessage(content="人工摘要输入")], CoordinatorProfile())


def test_context_input_estimate_uses_shared_counter():
    from agent_cluster.context.model import estimate_input_tokens

    messages = [{"role": "user", "content": "hello"}]
    assert estimate_input_tokens(messages, {"coordinator_tools": []}) == 59
    assert estimate_input_tokens(messages, {"coordinator_tools": []}, profile=CoordinatorProfile()) == 59


@pytest.mark.asyncio
async def test_summary_request_is_persisted_before_model_port_call(tmp_path):
    from agent_cluster.context import ContextManager

    goal, run = goal_and_run()
    notes = NoteStore(tmp_path / "notes")
    profile = CoordinatorProfile(context_tokens=16384, output_limit=128, compression_ratio=0.2)
    manager = ContextManager(notes, profile, "完整系统指令", model=None)
    history = [message for number in range(8) for message in protocol_round(number)]
    with pytest.raises(ValueError, match="ModelPort"):
        await manager.prepare(goal, run, {}, history)
    references = [value for value in notes.index(goal_id=goal.goal_id, actor_id=goal.actor_id)
                  if value.topic == "context-summary-request"]
    assert len(references) == 1
    snapshot = json.loads(notes.read(references[0], goal_id=goal.goal_id, actor_id=goal.actor_id))
    assert snapshot["identity"]["run_id"] == run.run_id
    assert snapshot["identity"]["prefix_count"] == 10
    assert snapshot["identity"]["profile"] == profile.model_dump(mode="json")
    request = snapshot["request"]
    assert request["run_id"] == run.run_id and request["step_key"] == snapshot["identity"]["step_key"]
    assert request["body"]["messages"][0]["role"] == "system"
    authority = json.loads(request["body"]["messages"][0]["content"].split("\n\n", 1)[1])
    assert authority["goal"] == goal.model_dump(mode="json")
    assert request["input_limit"] > 0 and request["output_limit"] == 128


@pytest.mark.asyncio
async def test_summary_recovery_reuses_snapshot_and_rebuilds_current_authority(tmp_path):
    from agent_cluster.context import ContextManager

    goal, run = goal_and_run()
    notes = NoteStore(tmp_path / "notes")
    profile = CoordinatorProfile(context_tokens=16384, output_limit=128, compression_ratio=0.2)
    history = [message for number in range(8) for message in protocol_round(number)]
    with pytest.raises(ValueError, match="ModelPort"):
        await ContextManager(notes, profile, "完整系统指令", None).prepare(goal, run, {}, history)
    reference = next(value for value in notes.index(goal_id=goal.goal_id, actor_id=goal.actor_id)
                     if value.topic == "context-summary-request")
    original = notes.read(reference, goal_id=goal.goal_id, actor_id=goal.actor_id)
    notes.write(goal_id=goal.goal_id, actor_id=goal.actor_id, note_id="new-result", topic="protocol",
                content="新增人工协议资料", summary="新增人工协议资料", source="protocol")
    current_run = run.model_copy(update={"updated_at": run.updated_at + timedelta(seconds=1),
                                        "spent_cny": Decimal("1")})
    catalog = {"goal_budget": {"limit_cny": "3", "spent_cny": "1", "reserved_cny": "0.5", "remaining_cny": "1.5"}}
    recovered_manager = ContextManager(NoteStore(tmp_path / "notes"), profile, "完整系统指令", None)
    with pytest.raises(ValueError, match="ModelPort"):
        await recovered_manager.prepare(goal, current_run, catalog, history)
    latest = next(value for value in notes.index(goal_id=goal.goal_id, actor_id=goal.actor_id)
                  if value.topic == "context-summary-request")
    assert latest == reference
    assert notes.read(latest, goal_id=goal.goal_id, actor_id=goal.actor_id) == original
    window = await recovered_manager.prepare(goal, current_run, catalog, history[-6:])
    authority = json.loads(window.messages[0]["content"].split("\n\n", 1)[1])
    assert authority["budget"] == catalog["goal_budget"]
    assert authority["run"]["updated_at"] == current_run.model_dump(mode="json")["updated_at"]


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["run_id", "step_key", "prefix_count", "prefix_sha256", "profile",
                                    "prompt_sha256", "system_prompt_sha256"])
async def test_summary_recovery_rejects_changed_snapshot_identity(tmp_path, field):
    from agent_cluster.context import ContextManager

    goal, run = goal_and_run()
    notes = NoteStore(tmp_path / "notes")
    profile = CoordinatorProfile(context_tokens=16384, output_limit=128, compression_ratio=0.2)
    history = [message for number in range(8) for message in protocol_round(number)]
    manager = ContextManager(notes, profile, "完整系统指令", None)
    with pytest.raises(ValueError, match="ModelPort"):
        await manager.prepare(goal, run, {}, history)
    reference = next(value for value in notes.index(goal_id=goal.goal_id, actor_id=goal.actor_id)
                     if value.topic == "context-summary-request")
    snapshot = json.loads(notes.read(reference, goal_id=goal.goal_id, actor_id=goal.actor_id))
    snapshot["identity"][field] = "changed-protocol-identity"
    notes.write(goal_id=goal.goal_id, actor_id=goal.actor_id, note_id=reference.note_id,
                topic=reference.topic, content=json.dumps(snapshot), summary=reference.summary, source=reference.source)
    with pytest.raises(ValueError, match="恢复身份"):
        await manager.prepare(goal, run, {}, history)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["messages", "input_limit"])
async def test_summary_recovery_rejects_changed_saved_request(tmp_path, field):
    from agent_cluster.context import ContextManager

    goal, run = goal_and_run()
    notes = NoteStore(tmp_path / "notes")
    profile = CoordinatorProfile(context_tokens=16384, output_limit=128, compression_ratio=0.2)
    history = [message for number in range(8) for message in protocol_round(number)]
    manager = ContextManager(notes, profile, "完整系统指令", None)
    with pytest.raises(ValueError, match="ModelPort"):
        await manager.prepare(goal, run, {}, history)
    reference = next(value for value in notes.index(goal_id=goal.goal_id, actor_id=goal.actor_id)
                     if value.topic == "context-summary-request")
    snapshot = json.loads(notes.read(reference, goal_id=goal.goal_id, actor_id=goal.actor_id))
    if field == "messages":
        snapshot["request"]["body"]["messages"][1]["content"] = "不同的人工摘要输入"
    else:
        snapshot["request"]["input_limit"] = 1
    notes.write(goal_id=goal.goal_id, actor_id=goal.actor_id, note_id=reference.note_id,
                topic=reference.topic, content=json.dumps(snapshot), summary=reference.summary, source=reference.source)
    with pytest.raises(ValueError, match="恢复内容"):
        await manager.prepare(goal, run, {}, history)
