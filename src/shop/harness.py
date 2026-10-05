import json
from contextlib import contextmanager
from decimal import Decimal
from hashlib import sha256
from threading import Event, Thread
from time import monotonic
from typing import Literal, TypedDict
from uuid import uuid4

from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import RetryPolicy
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from shop.budget import BudgetExceeded, BudgetService
from shop.config import Settings
from shop.materials import MaterialRepository
from shop.models import now
from shop.providers import create_completion, deepseek_key
from shop.runtime_models import HarnessTask, ProviderRequest, ToolCall

MODEL_LIMIT = 6
TOOL_LIMIT = 10
TIME_LIMIT_SECONDS = 120
INPUT_TOKEN_LIMIT = 12000
OUTPUT_TOKEN_LIMIT = 1000


class TaskStopped(RuntimeError):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class SearchArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    attributes: dict[str, str] = Field(default_factory=dict)
    limit: int = Field(default=20, ge=1, le=50)


class ReadArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    asset_id: str = Field(min_length=36, max_length=36)


class ModelResult(BaseModel):
    model_config = ConfigDict(extra="ignore")
    status: Literal["materials_found", "insufficient"]
    asset_ids: list[str] = Field(max_length=2)
    missing_info: str = ""


class GraphState(TypedDict):
    messages: list[dict]
    round: int
    awaiting_tools: bool


TOOLS = [
    {"type": "function", "function": {
        "name": "search_materials",
        "description": "按英文来源属性检索素材，最多返回两条候选。可用属性：gender、masterCategory、subCategory、articleType、baseColour、season、usage。",
        "parameters": SearchArgs.model_json_schema(),
    }},
    {"type": "function", "function": {
        "name": "read_material",
        "description": "读取具体素材的最新来源版本与文件并校验文件摘要；推荐前必须读取每件素材。",
        "parameters": ReadArgs.model_json_schema(),
    }},
]

SYSTEM_PROMPT = (
    "你是中文素材推荐助手。先用 search_materials 把中文需求映射到来源的英文属性和值。"
    "只有实际搜索结果可作为候选；查询最多需要两条候选。先选两件，再各调用一次 read_material。"
    "只根据工具结果选材。最后只输出 JSON："
    '{"status":"materials_found|insufficient","asset_ids":["素材 UUID"],"missing_info":""}。'
    "最多选两件；没有匹配资料时 status=insufficient 且 asset_ids=[]，说明缺失信息。"
    "不要在 JSON 外添加文字。"
)


class HarnessService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.engine = create_engine(settings.database_url, pool_pre_ping=True)
        self.maker = sessionmaker(self.engine, expire_on_commit=False)
        self.budget = BudgetService(self.maker)
        self._active_started: float | None = None
        self._base_elapsed = Decimal(0)

    @contextmanager
    def _operation_lock(self, task_id: str):
        with self.engine.connect() as connection:
            key = f"{task_id}:operation"
            connection.execute(text("SELECT pg_advisory_lock(hashtext(:key))"), {"key": key})
            connection.commit()
            try:
                yield
            finally:
                connection.execute(text("SELECT pg_advisory_unlock(hashtext(:key))"), {"key": key})
                connection.commit()

    def _current_elapsed(self) -> Decimal:
        if self._active_started is None:
            return self._base_elapsed
        return self._base_elapsed + Decimal(str(monotonic() - self._active_started))

    def _persist_elapsed(self, task_id: str) -> None:
        with self.maker.begin() as session:
            task = session.get(HarnessTask, task_id, with_for_update=True)
            if task is not None:
                task.elapsed_seconds = max(task.elapsed_seconds, self._current_elapsed())

    def _heartbeat(self, task_id: str, stop: Event) -> None:
        while not stop.wait(0.5):
            self._persist_elapsed(task_id)

    def create(
        self, prompt: str, owner_id: str | None = None, *,
        workflow_version: str = "legacy", model_profile: dict | None = None,
    ) -> dict:
        with self.maker.begin() as session:
            task = self.create_in_session(
                session, prompt, owner_id, workflow_version=workflow_version,
                model_profile=model_profile,
            )
            task_id = task.id
        return self.show(task_id)

    def create_in_session(
        self, session, prompt: str, owner_id: str | None = None, *,
        workflow_version: str = "legacy", model_profile: dict | None = None,
    ) -> HarnessTask:
        if not prompt.strip() or len(prompt) > 4000:
            raise ValueError("任务需求长度无效")
        task = HarnessTask(
            id=str(uuid4()), owner_id=owner_id, run_id=str(uuid4()), prompt=prompt.strip(),
            status="created", model_calls=0, tool_calls=0, elapsed_seconds=0,
            cancel_requested=False, created_at=now(), updated_at=now(),
            workflow_version=workflow_version, model_profile=model_profile,
        )
        session.add(task)
        session.flush()
        return task

    def show(self, task_id: str) -> dict:
        with self.maker() as session:
            task = session.get(HarnessTask, task_id)
            if task is None:
                raise ValueError("任务不存在")
            requests = session.scalars(
                select(ProviderRequest).where(ProviderRequest.task_id == task_id)
                .order_by(ProviderRequest.created_at)
            ).all()
            calls = session.scalars(
                select(ToolCall).where(ToolCall.task_id == task_id).order_by(ToolCall.created_at)
            ).all()
            return {
                "task_id": task.id, "run_id": task.run_id, "owner_id": task.owner_id,
                "prompt": task.prompt, "status": task.status,
                "model_calls": task.model_calls, "tool_calls": task.tool_calls,
                "elapsed_seconds": str(task.elapsed_seconds),
                "cancel_requested": task.cancel_requested, "result": task.result,
                "error": task.error, "stop_reason": task.stop_reason,
                "requests": [{"id": r.id, "step_key": r.step_key, "status": r.status,
                              "usage": r.usage, "reserved_cny": str(r.reservation_cny),
                              "charged_cny": str(r.charged_cny) if r.charged_cny is not None else None,
                              "provider_request_id": r.provider_request_id} for r in requests],
                "tools": [{"call_id": c.call_id, "name": c.name, "arguments": c.arguments,
                           "result": c.result, "status": c.status, "error": c.error} for c in calls],
            }

    def cancel(self, task_id: str) -> dict:
        with self.maker.begin() as session:
            task = session.get(HarnessTask, task_id, with_for_update=True)
            if task is None:
                raise ValueError("任务不存在")
            if task.status not in {"completed", "stopped", "cancelled"}:
                task.cancel_requested = True
                task.status = "cancelled"
                task.stop_reason = "cancelled"
                task.updated_at = now()
        with self._operation_lock(task_id):
            with self.maker() as session:
                reserved = session.scalars(select(ProviderRequest).where(
                    ProviderRequest.task_id == task_id,
                    ProviderRequest.status == "reserved",
                )).all()
                reserved_ids = [request.id for request in reserved]
            for request_id in reserved_ids:
                self.budget.cancel_unsent(request_id, "cancelled")
        return self.show(task_id)

    def _guard(self, task_id: str, *, model: bool = False, tool: bool = False) -> HarnessTask:
        reason = None
        with self.maker.begin() as session:
            task = session.get(HarnessTask, task_id, with_for_update=True)
            if task is None:
                raise ValueError("任务不存在")
            elapsed = self._current_elapsed() if self._active_started is not None else task.elapsed_seconds
            task.elapsed_seconds = max(task.elapsed_seconds, elapsed)
            if task.cancel_requested:
                reason = "cancelled"
            elif elapsed >= TIME_LIMIT_SECONDS:
                reason = "time_limit"
            elif model and task.model_calls >= MODEL_LIMIT:
                reason = "model_limit"
            elif tool and task.tool_calls >= TOOL_LIMIT:
                reason = "tool_limit"
        if reason is not None:
            raise TaskStopped(reason)
        return task

    def _model_node(
        self, task_id: str, state: GraphState, hold_after_reserve: bool = False
    ) -> GraphState:
        step_key = f"model-{state['round']}"
        with self.maker() as session:
            existing = session.scalar(select(ProviderRequest).where(
                ProviderRequest.task_id == task_id, ProviderRequest.step_key == step_key
            ))
            if existing is not None:
                if existing.status not in {"reserved", "completed"}:
                    raise TaskStopped("external_request_unknown")
                if existing.status == "completed" and existing.response_body is None:
                    raise TaskStopped("invalid_model_response")
                response = existing.response_body
            else:
                response = None
        if response is None:
            if existing is None:
                self._guard(task_id, model=True)
                deepseek_key(self.settings)
                body = {"model": "deepseek-flash", "messages": state["messages"],
                        "tools": TOOLS, "max_tokens": OUTPUT_TOKEN_LIMIT,
                        "response_format": {"type": "json_object"},
                        "thinking": {"type": "disabled"}}
                if len(json.dumps(body, ensure_ascii=False).encode("utf-8")) > INPUT_TOKEN_LIMIT:
                    raise TaskStopped("input_limit")
                with self.maker.begin() as session:
                    task = session.get(HarnessTask, task_id, with_for_update=True)
                    if task.cancel_requested:
                        raise TaskStopped("cancelled")
                    try:
                        request = self.budget.reserve(
                            task_id=task_id, run_id=task.run_id, step_key=step_key,
                            service="deepseek", model="deepseek-flash", request_body=body,
                            input_limit=INPUT_TOKEN_LIMIT, output_limit=OUTPUT_TOKEN_LIMIT,
                            session=session,
                        )
                    except BudgetExceeded as error:
                        raise TaskStopped("budget_limit") from error
                    task.model_calls += 1
                    task.updated_at = now()
            else:
                request = existing
                body = existing.request_body
            if hold_after_reserve and existing is None:
                Event().wait()
            with self._operation_lock(task_id):
                try:
                    remaining = TIME_LIMIT_SECONDS - float(self._guard(task_id).elapsed_seconds)
                    if remaining <= 0:
                        raise TaskStopped("time_limit")
                    self.budget.mark_sent(request.id)
                except TaskStopped as error:
                    self.budget.cancel_unsent(request.id, error.reason)
                    raise
                except BudgetExceeded as error:
                    raise TaskStopped("cancelled") from error
                try:
                    response, provider_id = create_completion(
                        self.settings, body, min(30.0, remaining)
                    )
                except Exception as error:
                    self.budget.mark_unknown(request.id, type(error).__name__)
                    raise TaskStopped("external_request_unknown") from error
                self.budget.complete(request.id, response, provider_id)
        choices = response.get("choices")
        if not isinstance(choices, list) or not choices:
            raise TaskStopped("invalid_model_response")
        message = choices[0]["message"]
        state["messages"] = [*state["messages"], message]
        state["round"] += 1
        state["awaiting_tools"] = bool(message.get("tool_calls"))
        if not state["awaiting_tools"]:
            self._finalize(task_id, message.get("content"))
        return state

    def _tool_node(self, task_id: str, state: GraphState) -> GraphState:
        message = state["messages"][-1]
        for call in message["tool_calls"]:
            call_id = call["id"]
            name = call["function"]["name"]
            try:
                raw_args = json.loads(call["function"]["arguments"])
                if name == "search_materials":
                    args = SearchArgs.model_validate(raw_args).model_dump()
                elif name == "read_material":
                    args = ReadArgs.model_validate(raw_args).model_dump()
                else:
                    raise ValueError("未知工具名称")
            except (ValueError, ValidationError, TypeError) as error:
                with self.maker.begin() as session:
                    session.add(ToolCall(
                        id=str(uuid4()), task_id=task_id, call_id=call_id, name=name,
                        arguments={"raw": call["function"].get("arguments")},
                        status="rejected", error=type(error).__name__,
                    ))
                raise TaskStopped("invalid_tool_arguments") from error
            with self.maker() as session:
                prior = session.scalar(select(ToolCall).where(
                    ToolCall.task_id == task_id, ToolCall.call_id == call_id
                ))
                result = prior.result if prior is not None and prior.status == "completed" else None
                if prior is not None and prior.status != "completed":
                    raise TaskStopped("tool_result_unknown")
            if result is None:
                if name == "read_material":
                    with self.maker() as session:
                        read_count = len(session.scalars(select(ToolCall).where(
                            ToolCall.task_id == task_id, ToolCall.name == "read_material",
                            ToolCall.status == "completed"
                        )).all())
                    if read_count >= 2:
                        result = {"error": "已读取两件素材，请从已读取的素材中选择"}
                        with self.maker.begin() as session:
                            session.add(ToolCall(
                                id=str(uuid4()), task_id=task_id, call_id=call_id,
                                name=name, arguments=args, result=result, status="rejected",
                                error="read_limit",
                            ))
                        state["messages"].append({
                            "role": "tool", "tool_call_id": call_id,
                            "content": json.dumps(result, ensure_ascii=False),
                        })
                        continue
                self._guard(task_id, tool=True)
                with self.maker.begin() as session:
                    task = session.get(HarnessTask, task_id, with_for_update=True)
                    if task.cancel_requested:
                        raise TaskStopped("cancelled")
                    task.tool_calls += 1
                    session.add(ToolCall(id=str(uuid4()), task_id=task_id, call_id=call_id,
                                         name=name, arguments=args, status="started"))
                try:
                    result = self._execute_tool(name, args)
                except Exception as error:
                    with self.maker.begin() as session:
                        record = session.scalar(select(ToolCall).where(
                            ToolCall.task_id == task_id, ToolCall.call_id == call_id
                        ))
                        record.status = "failed"
                        record.error = type(error).__name__
                    raise
                with self.maker.begin() as session:
                    record = session.scalar(select(ToolCall).where(
                        ToolCall.task_id == task_id, ToolCall.call_id == call_id
                    ))
                    record.result = result
                    record.status = "completed"
            state["messages"].append({"role": "tool", "tool_call_id": call_id,
                                      "content": json.dumps(result, ensure_ascii=False)})
        state["awaiting_tools"] = False
        return state

    def _locked_tool_node(self, task_id: str, state: GraphState) -> GraphState:
        with self._operation_lock(task_id):
            self._guard(task_id)
            return self._tool_node(task_id, state)

    def _execute_tool(self, name: str, args: dict) -> dict:
        with self.maker() as session:
            repository = MaterialRepository(session, self.settings.data_dir)
            if name == "search_materials":
                assets = repository.search(
                    attributes=args["attributes"], limit=min(args["limit"], 2)
                )
                return {"assets": [
                    {"asset_id": a.id, "title": a.title, "source_id": a.source_id,
                     "source_revision": a.source_revision, "record_id": a.record_id,
                     "version": a.versions[-1].version,
                     "original_fields": a.versions[-1].original_fields}
                    for a in assets
                ]}
            asset = repository.get(args["asset_id"])
            if asset is None:
                raise ValueError("素材不存在")
            version = asset.versions[-1]
            content = repository.read_file(version)
            return {"asset_id": asset.id, "title": version.title,
                    "source_id": asset.source_id, "source_revision": asset.source_revision,
                    "record_id": asset.record_id, "version": version.version,
                    "original_fields": version.original_fields,
                    "file_sha256": sha256(content).hexdigest()}

    def _finalize(self, task_id: str, content: str | None) -> None:
        if content is None:
            raise TaskStopped("invalid_model_result")
        try:
            answer = ModelResult.model_validate_json(content)
        except ValidationError as error:
            raise TaskStopped("invalid_model_result") from error
        with self.maker() as session:
            reads = session.scalars(select(ToolCall).where(
                ToolCall.task_id == task_id, ToolCall.name == "read_material",
                ToolCall.status == "completed"
            )).all()
            read_by_id = {call.result["asset_id"]: call.result for call in reads}
            searches = session.scalars(select(ToolCall).where(
                ToolCall.task_id == task_id, ToolCall.name == "search_materials",
                ToolCall.status == "completed"
            )).all()
            searched_ids = {
                item["asset_id"] for call in searches for item in call.result["assets"]
            }
            if answer.status == "insufficient" and answer.asset_ids:
                raise TaskStopped("invalid_model_result")
            if answer.status == "insufficient" and not any(
                not call.result["assets"] for call in searches
            ):
                raise TaskStopped("unverified_insufficiency")
            if answer.status == "materials_found" and not answer.asset_ids:
                raise TaskStopped("invalid_model_result")
            if len(set(answer.asset_ids)) != len(answer.asset_ids):
                raise TaskStopped("invalid_model_result")
            recommendations = []
            repository = MaterialRepository(session, self.settings.data_dir)
            for asset_id in answer.asset_ids:
                cited = read_by_id.get(asset_id)
                if cited is None or asset_id not in searched_ids:
                    raise TaskStopped("unverified_citation")
                asset = repository.get(asset_id)
                if asset is None or asset.versions[-1].version != cited["version"]:
                    raise TaskStopped("unverified_citation")
                verified = self._execute_tool("read_material", {"asset_id": asset_id})
                if verified != cited:
                    raise TaskStopped("unverified_citation")
                fields = verified["original_fields"]
                reason = "；".join(
                    f"{key}={fields[key]}" for key in ("gender", "articleType", "baseColour")
                    if key in fields
                )
                recommendations.append({
                    "asset_id": asset_id, "title": verified["title"],
                    "reason": f"来源属性：{reason}" if reason else "已校验来源文件",
                    "source_id": verified["source_id"],
                    "source_revision": verified["source_revision"],
                    "record_id": verified["record_id"], "version": verified["version"],
                    "file_sha256": verified["file_sha256"],
                })
        reason = None
        with self.maker.begin() as session:
            task = session.get(HarnessTask, task_id, with_for_update=True)
            elapsed = self._current_elapsed() if self._active_started is not None else task.elapsed_seconds
            task.elapsed_seconds = max(task.elapsed_seconds, elapsed)
            if task.cancel_requested:
                reason = "cancelled"
            elif elapsed >= TIME_LIMIT_SECONDS:
                reason = "time_limit"
            elif task.result is None:
                task.result = {
                    "status": answer.status, "recommendations": recommendations,
                    "missing_info": "当前素材来源中未找到符合需求的记录"
                    if answer.status == "insufficient" else "",
                }
                task.status = "completed"
                task.updated_at = now()
        if reason is not None:
            raise TaskStopped(reason)

    def _graph(
        self, checkpointer: PostgresSaver, task_id: str,
        hold_at_checkpoint: bool = False, hold_after_reserve: bool = False,
    ):
        builder = StateGraph(GraphState)
        no_retry = RetryPolicy(max_attempts=1)
        builder.add_node(
            "model", lambda state: self._model_node(task_id, state, hold_after_reserve),
            retry_policy=no_retry,
        )
        builder.add_node("tools", lambda state: self._locked_tool_node(task_id, state), retry_policy=no_retry)
        builder.add_edge(START, "model")
        builder.add_conditional_edges(
            "model", lambda state: "tools" if state["awaiting_tools"] else END,
        )
        builder.add_edge("tools", "model")
        return builder.compile(
            checkpointer=checkpointer,
            interrupt_before=["tools"] if hold_at_checkpoint else None,
        )

    def run(
        self, task_id: str, hold_at_checkpoint: bool = False,
        hold_after_reserve: bool = False,
    ) -> dict:
        with self.maker() as session:
            task = session.get(HarnessTask, task_id)
            if task is None:
                raise ValueError("任务不存在")
            cancelled = task.cancel_requested
        if cancelled:
            return self.cancel(task_id)
        with self.engine.connect() as lock_connection:
            acquired = lock_connection.scalar(text("SELECT pg_try_advisory_lock(hashtext(:id))"), {"id": task_id})
            lock_connection.commit()
            if not acquired:
                raise ValueError("任务已经在运行")
            heartbeat_stop = None
            heartbeat_thread = None
            try:
                cancelled_before_run = False
                with self.maker.begin() as session:
                    task = session.get(HarnessTask, task_id, with_for_update=True)
                    if task is None:
                        raise ValueError("任务不存在")
                    if task.status in {"completed", "stopped", "cancelled"}:
                        return self.show(task_id)
                    if task.cancel_requested:
                        task.status = "cancelled"
                        task.stop_reason = "cancelled"
                        cancelled_before_run = True
                    else:
                        if task.started_at is None:
                            task.started_at = now()
                        task.status = "running"
                        task.updated_at = now()
                if cancelled_before_run:
                    return self.show(task_id)
                self._base_elapsed = task.elapsed_seconds
                self._active_started = monotonic()
                heartbeat_stop = Event()
                heartbeat_thread = Thread(
                    target=self._heartbeat, args=(task_id, heartbeat_stop), daemon=True
                )
                heartbeat_thread.start()
                conn_string = self.settings.database_url.replace("postgresql+psycopg://", "postgresql://", 1)
                with PostgresSaver.from_conn_string(conn_string) as checkpointer:
                    checkpointer.setup()
                    graph = self._graph(
                        checkpointer, task_id, hold_at_checkpoint, hold_after_reserve
                    )
                    config = {"configurable": {"thread_id": task_id}}
                    initial = None if graph.get_state(config).values else {
                        "messages": [{"role": "system", "content": SYSTEM_PROMPT},
                                     {"role": "user", "content": self.show(task_id)["prompt"]}],
                        "round": 0, "awaiting_tools": False,
                    }
                    try:
                        graph.invoke(initial, config, durability="sync")
                        if hold_at_checkpoint and graph.get_state(config).next:
                            Event().wait()
                    except TaskStopped as error:
                        with self.maker.begin() as session:
                            task = session.get(HarnessTask, task_id, with_for_update=True)
                            task.status = "cancelled" if error.reason == "cancelled" else "stopped"
                            task.stop_reason = error.reason
                            task.updated_at = now()
                    except Exception as error:
                        with self.maker.begin() as session:
                            task = session.get(HarnessTask, task_id, with_for_update=True)
                            task.status = "stopped"
                            task.stop_reason = "error"
                            task.error = f"{type(error).__name__}: {error}"
                            task.updated_at = now()
                        raise
                heartbeat_stop.set()
                heartbeat_thread.join()
                self._persist_elapsed(task_id)
                self._active_started = None
                heartbeat_stop = None
                return self.show(task_id)
            finally:
                if heartbeat_stop is not None:
                    heartbeat_stop.set()
                    heartbeat_thread.join()
                    self._persist_elapsed(task_id)
                    self._active_started = None
                lock_connection.execute(text("SELECT pg_advisory_unlock(hashtext(:id))"), {"id": task_id})
                lock_connection.commit()

    def resume(self, task_id: str) -> dict:
        return self.run(task_id)
