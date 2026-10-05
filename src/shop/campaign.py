import base64
import json
from decimal import Decimal
from pathlib import Path
from typing import TypedDict
from uuid import uuid4

from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape
from langgraph.graph import END, START, StateGraph
from langgraph.types import RetryPolicy
from sqlalchemy import select

from shop.asset_index import MAIN_SOURCE, IndexService
from shop.budget import BudgetExceeded
from shop.campaign_types import Draft, RequirementPlan, Review
from shop.harness import TIME_LIMIT_SECONDS, HarnessService, TaskStopped
from shop.materials import MaterialRepository
from shop.model_profiles import ModelProfile, profiles
from shop.models import now
from shop.paid_models import PaidModels, RequestLimit, TruncatedResponse, UnknownRequest
from shop.runtime_models import HarnessTask, ToolCall


class CampaignState(TypedDict, total=False):
    messages: list[dict]
    round: int
    awaiting_tools: bool
    plan: dict
    vector: list
    candidates: list[dict]
    draft: dict
    review: dict
    insufficient: str


class CampaignService(HarnessService):
    def create(self, prompt: str, owner_id: str | None = None) -> dict:
        with self.maker.begin() as session:
            task_id = self.create_in_session(session, prompt, owner_id).id
        return self.show(task_id)

    def create_in_session(self, session, prompt: str, owner_id: str | None = None):
        config = {name: value.model_dump() for name, value in profiles(
            self.settings.harness_token_limits_file
        ).items()}
        config["prompt_version"] = "campaign-v2"
        return super().create_in_session(
            session, prompt, owner_id, workflow_version="campaign-v1", model_profile=config,
        )

    def show(self, task_id: str) -> dict:
        result = super().show(task_id)
        with self.maker() as session:
            task = session.get(HarnessTask, task_id)
            result["workflow_version"] = task.workflow_version
            result["model_profile"] = task.model_profile
            result["elapsed_seconds"] = float(task.elapsed_seconds)
        return result

    def listing(self, owner_id: str | None) -> list[dict]:
        with self.maker() as session:
            query = select(HarnessTask.id).where(
                HarnessTask.workflow_version == "campaign-v1"
            ).order_by(HarnessTask.created_at.desc()).limit(100)
            if owner_id is not None:
                query = query.where(HarnessTask.owner_id == owner_id)
            ids = list(session.scalars(query))
        return [self.show(task_id) for task_id in ids]

    def render(self, task_id: str) -> str:
        task = self.show(task_id)
        result = task["result"]
        if task["status"] != "completed" or not result or result["status"] != "materials_found":
            raise ValueError("任务没有通过审核的宣传页面")
        if not result["review"]["approved"]:
            raise ValueError("宣传页面尚未通过审核")
        records = result["recommendations"]
        evidence = self._evidence({"draft": {
            "headline": result["headline"], "summary": result["summary"],
            "recommendations": [{"asset_id": item["asset_id"], "caption": item["caption"]}
                                for item in records],
        }, "candidates": records})
        images = {item["asset_id"]: item["url"] for item in evidence["images"]}
        environment = Environment(
            loader=FileSystemLoader(Path(__file__).parent / "templates"),
            autoescape=select_autoescape(["html"]),
            undefined=StrictUndefined,
        )
        return environment.get_template("campaign.html").render(
            headline=result["headline"], summary=result["summary"],
            records=evidence["records"], images=images,
        )

    def run(self, task_id: str, hold_at_checkpoint: bool = False, hold_after_reserve: bool = False):
        task = self.show(task_id)
        if task["workflow_version"] != "campaign-v1":
            if task["status"] in {"completed", "stopped", "cancelled"}:
                return task
            raise ValueError("任务执行版本不兼容，请创建新的宣传任务")
        if hold_after_reserve:
            raise ValueError("此入口使用实际checkpoint中断进行恢复检查")
        return super().run(task_id, hold_at_checkpoint=hold_at_checkpoint)

    def _profile(self, task_id: str, role: str) -> ModelProfile:
        return ModelProfile.model_validate(self.show(task_id)["model_profile"][role])

    def _model(self, task_id: str, role: str, messages: list[dict], schema):
        with self._operation_lock(task_id):
            task = self._guard(task_id)
            remaining = TIME_LIMIT_SECONDS - float(task.elapsed_seconds)
            try:
                response = PaidModels(self.settings, self.budget).text(
                    self._profile(task_id, role), messages, run_id=task.run_id,
                    step_key=role, timeout_seconds=remaining, task_id=task_id,
                )
            except BudgetExceeded as error:
                raise TaskStopped("budget_limit") from error
            except UnknownRequest as error:
                raise TaskStopped("external_request_unknown") from error
            except TruncatedResponse as error:
                raise TaskStopped("output_limit") from error
            except RequestLimit as error:
                raise TaskStopped(str(error)) from error
            except TimeoutError as error:
                raise TaskStopped("time_limit") from error
            self._guard(task_id)
            return schema.model_validate_json(response["choices"][0]["message"]["content"])

    def _tool(self, task_id: str, name: str, arguments: dict, operation) -> dict:
        with self._operation_lock(task_id):
            self._guard(task_id)
            with self.maker() as session:
                prior = session.scalar(select(ToolCall).where(
                    ToolCall.task_id == task_id, ToolCall.call_id == name,
                ))
            if prior is not None:
                if prior.status != "completed":
                    raise TaskStopped("tool_result_unknown")
                return prior.result
            self._guard(task_id, tool=True)
            with self.maker.begin() as session:
                task = session.get(HarnessTask, task_id, with_for_update=True)
                if task.cancel_requested:
                    raise TaskStopped("cancelled")
                task.tool_calls += 1
                session.add(ToolCall(id=str(uuid4()), task_id=task_id, call_id=name,
                                     name=name, arguments=arguments, status="started"))
            try:
                result = operation()
            except Exception as error:
                with self.maker.begin() as session:
                    record = session.scalar(select(ToolCall).where(
                        ToolCall.task_id == task_id, ToolCall.call_id == name))
                    record.status = "failed"
                    record.error = type(error).__name__
                raise
            with self.maker.begin() as session:
                record = session.scalar(select(ToolCall).where(
                    ToolCall.task_id == task_id, ToolCall.call_id == name))
                record.result, record.status = result, "completed"
            return result

    def _understand(self, task_id: str, state: CampaignState) -> CampaignState:
        task = self.show(task_id)
        instruction = (
            "理解中文宣传需求，返回以下结构JSON："
            + json.dumps(RequirementPlan.model_json_schema(), ensure_ascii=False)
            + "。数量为1或2；attributes使用来源英文值，常见gender=Men/Women/Unisex，"
            "articleType=Tshirts/Shirts/Jeans，baseColour=Black/White/Blue等。"
            "准确保留用户明确要求的每个必须条件和禁止条件。queries是针对可见特征和用途的中文语义查询。"
            "required_facts仅列用户必须依据来源确认的商品事实字段，如price、stock、material、performance、brand；"
            "普通穿搭风格不要求商品性能证据。不得猜测商品事实，不能擅自放宽条件。"
        )
        if task["model_profile"].get("prompt_version") == "campaign-v2":
            source_values = self._tool(task_id, "source_schema", {"source_id": MAIN_SOURCE},
                                       lambda: IndexService(self.settings, self.maker).attribute_values())
            instruction += (
                "当前来源实际属性及英文值：" + json.dumps(source_values, ensure_ascii=False)
                + "。attributes与forbidden_attributes的键只能使用上方列出的字段。"
                "已有类别使用来源中的完整英文值，例如库尔塔对应Kurtas；女士上衣对应Tops。"
                "明确请求的类别在来源中不存在时保留该类别条件，让检索返回不足，不能替换成其他商品。"
                "仅保留用户原文提出的必须条件；不得添加袖长、领口、图案、季节或用途等未提出的要求。"
                "required_facts仅填写用户明确要求必须核实的价格、库存、材质、性能或品牌事实。"
                "普通颜色、性别和品类推荐的required_facts为空数组。价格、库存、材质及性能不能放入attributes。"
                "用户要求不得编造某事实时，把限制放forbid，不因此要求来源提供该事实。"
                "page_requirements仅保留原文对页面和文案的要求，不增加价格、库存及其他展示要求。"
                "例如请求'推荐红色女士衬衫'不要求价格和库存，required_facts=[]；"
                "请求'推荐必须确认纯棉的服装'需要required_facts=['material']。"
            )
        plan = self._model(task_id, "understand", [
            {"role": "system", "content": instruction},
            {"role": "user", "content": task["prompt"]},
        ], RequirementPlan)
        state["plan"] = plan.model_dump()
        state["insufficient"] = ""
        return state

    def _retrieve(self, task_id: str, state: CampaignState) -> CampaignState:
        plan = RequirementPlan.model_validate(state["plan"])
        index = IndexService(self.settings, self.maker)
        index.profile = self._profile(task_id, "description")
        if index.coverage(MAIN_SOURCE) != 300:
            raise TaskStopped("index_incomplete")
        with self._operation_lock(task_id):
            task = self._guard(task_id)
            try:
                response = PaidModels(self.settings, self.budget).embed(
                    plan.queries, run_id=task.run_id, step_key="query_embedding",
                    timeout_seconds=TIME_LIMIT_SECONDS - float(task.elapsed_seconds), task_id=task_id,
                )
            except BudgetExceeded as error:
                raise TaskStopped("budget_limit") from error
            except UnknownRequest as error:
                raise TaskStopped("external_request_unknown") from error
            except RequestLimit as error:
                raise TaskStopped(str(error)) from error
            except TimeoutError as error:
                raise TaskStopped("time_limit") from error
        vector = IndexService.embedding_vector(response).tolist()
        found = self._tool(task_id, "retrieve", state["plan"], lambda: {
            "candidates": index.search(plan, vector, MAIN_SOURCE),
        })
        state["candidates"] = found["candidates"]
        if len(state["candidates"]) < plan.count:
            state["insufficient"] = "当前素材来源中没有足够的符合条件的图片"
        else:
            available = set.intersection(*[
                {key for key, value in item["original_fields"].items() if value is not None}
                for item in state["candidates"]
            ])
            missing = [fact for fact in plan.required_facts if fact not in available]
            if missing:
                state["insufficient"] = "来源缺少需求必须确认的信息：" + "、".join(missing)
        return state

    def _compose(self, task_id: str, state: CampaignState) -> CampaignState:
        instruction = (
            "根据用户需求、结构化条件及候选素材生成宣传文案，返回以下结构JSON："
            + json.dumps(Draft.model_json_schema(), ensure_ascii=False)
            + "。推荐数量严格等于count。只选择候选asset_id。文案使用中文，商品事实仅引用original_fields；"
            "图片描述中的用途和风格只能作为穿搭建议。不得编造价格、促销、材质、库存和性能。"
        )
        if self.show(task_id)["model_profile"].get("prompt_version") == "campaign-v2":
            instruction += (
                "headline、summary和caption面向阅读宣传页的用户，直接介绍商品及穿搭建议。"
                "使用自然中文，不写字段名称、英文属性值、检索过程、来源核验及审核流程。"
                "商品名称可以引用来源中的正式名称。"
            )
        draft = self._model(task_id, "compose", [
            {"role": "system", "content": instruction},
            {"role": "user", "content": json.dumps({
                "request": self.show(task_id)["prompt"], "plan": state["plan"],
                "candidates": state["candidates"],
            }, ensure_ascii=False)},
        ], Draft)
        self._validate_draft(state, draft)
        state["draft"] = draft.model_dump()
        return state

    @staticmethod
    def _validate_draft(state: CampaignState, draft: Draft) -> None:
        allowed = {item["asset_id"]: item for item in state["candidates"]}
        if len(draft.recommendations) != state["plan"]["count"]:
            raise ValueError("推荐数量与需求不一致")
        for recommendation in draft.recommendations:
            if recommendation.asset_id not in allowed:
                raise ValueError("推荐引用超出实际候选范围")
            fields = allowed[recommendation.asset_id]["original_fields"]
            for key, value in state["plan"]["attributes"].items():
                if fields.get(key) != value:
                    raise ValueError("推荐不符合来源必须条件")
            for key, values in state["plan"]["forbidden_attributes"].items():
                if fields.get(key) in values:
                    raise ValueError("推荐违反来源禁止条件")

    def _evidence(self, state: CampaignState) -> dict:
        draft = Draft.model_validate(state["draft"])
        allowed = {item["asset_id"]: item for item in state["candidates"]}
        records, images = [], []
        with self.maker() as session:
            repository = MaterialRepository(session, self.settings.data_dir)
            for recommendation in draft.recommendations:
                cited = allowed[recommendation.asset_id]
                asset = repository.get(recommendation.asset_id)
                if asset is None or asset.source_id != MAIN_SOURCE:
                    raise ValueError("宣传素材来源不支持业务导出")
                version = asset.versions[-1]
                if (version.id != cited["version_id"] or version.file_sha256 != cited["file_sha256"]
                        or version.original_fields != cited["original_fields"]):
                    raise ValueError("宣传素材版本或来源发生变化")
                raw = repository.read_file(version)
                records.append({**cited, "caption": recommendation.caption})
                images.append({"asset_id": asset.id, "url": "data:" + version.mime_type
                               + ";base64," + base64.b64encode(raw).decode()})
        return {"records": records, "images": images}

    def _review(self, task_id: str, state: CampaignState, role: str) -> CampaignState:
        evidence = self._tool(task_id, f"evidence_{role}", state["draft"], lambda: self._evidence(state))
        instruction = (
            "逐项审核宣传方案与原始用户需求、原图和来源字段的一致性，输出一个JSON object，"
            "包含approved（boolean）、issues（具体问题的字符串数组）与coverage（已检查条件的字符串数组）。"
            "approved=true时issues必须为空；approved=false时issues必须列出问题。"
            "检查数量、每个必须及禁止条件、图片与文案匹配、未获来源支持的商品事实。"
            "明显违规或事实缺失必须approved=false，并用issues列出具体可修改问题及asset_id。"
            "穿搭建议可以作为建议表达。coverage逐项说明检查的原始需求。审核全部标题、summary和caption。"
            "coverage必须提供完整的检查依据，issues每项必须写出完整问题、对应文案片段及可核查的来源内容。"
            "允许productDisplayName的忠实中文翻译或结合gender、articleType、baseColour等字段的中文商品介绍。"
            "按照字段含义与来源描述检查商品事实，中文表达与英文正式名称的文字差异不构成事实冲突。"
            "审核依据为数量、必须及禁止条件、图文一致性和商品事实来源。"
            "措辞吸引力、标题概括程度和个人审美不影响审核结论。"
        )
        content = [{"type": "text", "text": json.dumps({
            "request": self.show(task_id)["prompt"], "plan": state["plan"],
            "draft": state["draft"], "sources": evidence["records"],
        }, ensure_ascii=False)}]
        for image in evidence["images"]:
            content.extend([{"type": "text", "text": "原图asset_id=" + image["asset_id"]},
                            {"type": "image_url", "image_url": {"url": image["url"]}}])
        review = self._model(task_id, role, [
            {"role": "system", "content": instruction}, {"role": "user", "content": content},
        ], Review)
        state["review"] = review.model_dump()
        return state

    def _revise(self, task_id: str, state: CampaignState) -> CampaignState:
        draft = self._model(task_id, "revise", [
            {"role": "system", "content": "根据审核问题修正宣传方案，返回以下结构JSON："
             + json.dumps(Draft.model_json_schema(), ensure_ascii=False)
             + "。保留全部原始需求，不增加来源不支持的商品事实，只选择候选素材。"},
            {"role": "user", "content": json.dumps({
                "request": self.show(task_id)["prompt"], "plan": state["plan"],
                "draft": state["draft"], "review": state["review"], "candidates": state["candidates"],
            }, ensure_ascii=False)},
        ], Draft)
        self._validate_draft(state, draft)
        state["draft"] = draft.model_dump()
        return state

    def _finish(self, task_id: str, state: CampaignState) -> CampaignState:
        self._guard(task_id)
        if state.get("insufficient"):
            result = {"status": "insufficient", "headline": "信息不足", "summary": state["insufficient"],
                      "recommendations": [], "missing_info": [state["insufficient"]], "plan": state["plan"]}
        else:
            if not state["review"]["approved"]:
                raise TaskStopped("review_rejected")
            self._validate_draft(state, Draft.model_validate(state["draft"]))
            evidence = self._tool(task_id, "final_source_check", state["draft"], lambda: self._evidence(state))
            result = {"status": "materials_found", "headline": state["draft"]["headline"],
                      "summary": state["draft"]["summary"], "recommendations": evidence["records"],
                      "missing_info": [], "review": state["review"], "plan": state["plan"]}
        with self.maker.begin() as session:
            task = session.get(HarnessTask, task_id, with_for_update=True)
            if task.cancel_requested:
                raise TaskStopped("cancelled")
            task.elapsed_seconds = max(task.elapsed_seconds, self._current_elapsed())
            if task.elapsed_seconds >= Decimal(TIME_LIMIT_SECONDS):
                raise TaskStopped("time_limit")
            task.result = result
            task.status = "completed"
            task.updated_at = now()
        return state

    def _graph(self, checkpointer, task_id: str, hold_at_checkpoint=False, hold_after_reserve=False):
        graph = StateGraph(CampaignState)
        no_retry = RetryPolicy(max_attempts=1)
        nodes = {"understand": self._understand, "retrieve": self._retrieve,
                 "compose": self._compose, "revise": self._revise, "finish": self._finish}
        for name, method in nodes.items():
            graph.add_node(name, lambda state, action=method: action(task_id, state), retry_policy=no_retry)
        graph.add_node("review", lambda state: self._review(task_id, state, "review"), retry_policy=no_retry)
        graph.add_node("rereview", lambda state: self._review(task_id, state, "rereview"), retry_policy=no_retry)
        graph.add_edge(START, "understand")
        graph.add_edge("understand", "retrieve")
        graph.add_conditional_edges("retrieve", lambda state: "finish" if state["insufficient"] else "compose")
        graph.add_edge("compose", "review")
        graph.add_conditional_edges("review", lambda state: "finish" if state["review"]["approved"] else "revise")
        graph.add_edge("revise", "rereview")
        graph.add_edge("rereview", "finish")
        graph.add_edge("finish", END)
        return graph.compile(checkpointer=checkpointer,
                             interrupt_before=["retrieve"] if hold_at_checkpoint else None)
