import asyncio
import json
import os
import re
from contextlib import asynccontextmanager, contextmanager
from dataclasses import replace
from decimal import Decimal
from uuid import UUID, uuid4

from poster_framework import (
    INTERFACE_VERSION,
    WORKFLOW_REQUEST_FORMAT,
    AgentInput,
    AgentRegistration,
    AgentRole,
    AgentSet,
    ArtifactPurpose,
    ContentFormat,
    ContentProvenance,
    ExecutionIdentity,
    ExecutionMode,
    FileReference,
    OriginalRequest,
    PosterWorkflow,
    RunBudget,
    RunSession,
    ToolRegistry,
    VersionDependency,
    run_single_agent,
    run_single_agent_from_upstream,
)
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from shop.budget import BudgetService
from shop.config import Settings
from shop.materials import MaterialRepository
from shop.models import AssetVersion, Source, User
from shop.paid_models import PaidModels
from shop.poster_types import (
    KEYWORD_PROMPT_VERSION,
    POSTER_PROFILE,
    KeywordPlan,
    MaterialSnapshot,
    PosterInput,
    PosterJob,
    digest,
    poster_job_id,
)
from shop.runtime_models import HarnessTask, ProviderRequest

POSTER_BUDGET = RunBudget(
    max_revision_rounds=4, max_image_operations=5, max_critic_vision_calls=5,
    max_model_calls=20, max_tool_calls=40,
)
KEYWORD_SYSTEM = (
    "根据已审核的商品宣传资料生成2至5个中文海报主题关键词。"
    "关键词共同覆盖品类、颜色与适用人群，允许合并这些条件。"
    "只使用提供资料支持的事实，不添加价格、库存、材质或性能。"
    '只返回JSON对象：{"keywords":["关键词"]}。完整需求会继续传给海报角色。'
)


class PosterModelClient:
    def __init__(self, service):
        self.service = service
        self.paid = PaidModels(service.settings, service.budget)

    async def call(self, context, messages, *, step_key: str, profile=POSTER_PROFILE):
        if profile.service != "deepseek" or profile.model != "deepseek-flash":
            raise ValueError("海报模型桥接当前仅配置deepseek-flash文字接口")
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", step_key):
            raise ValueError("模型步骤编号必须为1至40个英文字符、数字或下划线")
        ledger_key = f"{context.identity.invocation_id}:{step_key}"
        if len(ledger_key) > 90:
            raise ValueError("模型调用身份超过费用记录范围")
        body = profile.body(messages)
        async with self.service.model_lock(context.identity.run_id, ledger_key):
            with self.service.maker() as session:
                prior = session.scalar(select(ProviderRequest).where(
                    ProviderRequest.run_id == context.identity.run_id,
                    ProviderRequest.step_key == ledger_key,
                ))
            if prior is not None and prior.status == "completed":
                # PaidModels继续核对配置并读取原响应，不增加框架调用计数。
                return await asyncio.to_thread(
                    self.paid.text, profile, messages, run_id=context.identity.run_id,
                    step_key=ledger_key, timeout_seconds=120,
                )

            async def invoke(provider, model, request_data, parameters):
                return await asyncio.to_thread(
                    self.paid.text, profile, request_data["messages"],
                    run_id=context.identity.run_id, step_key=ledger_key, timeout_seconds=120,
                )

            return await context.call_model(
                provider=profile.service, model=profile.model,
                request_data={"messages": body["messages"]},
                parameters={"ledger_step_key": ledger_key, "profile": profile.model_dump()},
                invoke=invoke,
            )


class MaterialAgent:
    def __init__(self, instance, service, job, references):
        self.instance = instance
        self.service = service
        self.job = job
        self.references = references

    async def run(self, inputs, context):
        run_id = context.identity.run_id
        if run_id not in self.references:
            self.references[run_id] = self.service.attach_materials(self.job, context)
        references = self.references[run_id]
        metadata = inputs.metadata.model_copy(update={
            "files": tuple({item.file_id: item for item in (
                *inputs.metadata.files, *(self.service.file_reference(ref) for ref in references),
            )}.values()),
            "version_dependencies": tuple({(item.artifact_id, item.revision): item for item in (
                *inputs.metadata.version_dependencies,
                *(VersionDependency(artifact_id=ref.artifact_id, revision=ref.revision)
                  for ref in references),
            )}.values()),
        })
        return await self.instance.run(
            inputs.model_copy(update={"metadata": metadata}),
            context,
        )


class PosterService:
    def __init__(self, settings: Settings, registrations=(), tool_registry=None):
        self.settings = settings
        self.root = settings.poster_dir.resolve()
        self.engine = create_engine(settings.database_url, pool_pre_ping=True)
        self.maker = sessionmaker(self.engine, expire_on_commit=False)
        self.budget = BudgetService(self.maker, verification_scope="m3")
        self.model_client = PosterModelClient(self)
        self.tools = tool_registry or ToolRegistry()
        self.configure(registrations)

    def configure(self, registrations):
        self.registrations = tuple(registrations)
        roles = set()
        for registration in self.registrations:
            if not isinstance(registration, AgentRegistration):
                raise TypeError("组员接口必须返回AgentRegistration")
            if registration.role in roles:
                raise ValueError("角色重复注册")
            if INTERFACE_VERSION not in registration.interface_versions:
                raise ValueError("组员接口版本不兼容")
            roles.add(registration.role)

    @contextmanager
    def operation_lock(self, key):
        with self.engine.connect() as connection:
            connection.execute(text("SELECT pg_advisory_lock(hashtextextended(:key, 0))"),
                               {"key": f"poster:{key}"})
            connection.commit()
            try:
                yield
            finally:
                connection.execute(text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"),
                                   {"key": f"poster:{key}"})
                connection.commit()

    @asynccontextmanager
    async def model_lock(self, run_id, step_key):
        with self.engine.connect() as connection:
            parameters = {"key": f"poster-model:{run_id}:{step_key}"}
            await asyncio.to_thread(connection.execute,
                                    text("SELECT pg_advisory_lock(hashtextextended(:key, 0))"),
                                    parameters)
            connection.commit()
            try:
                yield
            finally:
                connection.execute(text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"),
                                   parameters)
                connection.commit()

    @staticmethod
    def actor(session, actor_id):
        user = session.get(User, str(UUID(actor_id)))
        if user is None:
            raise PermissionError("操作账号不存在")
        return user

    @staticmethod
    def authorize(user, owner_id):
        if user.role != "admin" and user.id != owner_id:
            raise PermissionError("没有访问此任务或素材版本的权限")

    def snapshot(self, session, version_id, user):
        version = session.get(AssetVersion, str(UUID(version_id)))
        if version is None:
            raise ValueError("指定素材版本不存在")
        self.authorize(user, version.owner_id)
        asset = version.asset
        if asset.kind != "image":
            raise ValueError("海报素材必须为图片")
        source = session.get(Source, (asset.source_id, asset.source_revision))
        if source is None:
            raise ValueError("素材来源版本不存在")
        MaterialRepository(session, self.settings.data_dir).read_file(version)
        return MaterialSnapshot(
            asset_id=asset.id, version_id=version.id, source_id=asset.source_id,
            source_revision=asset.source_revision, record_id=asset.record_id,
            file_sha256=version.file_sha256, mime_type=version.mime_type,
            title=version.title, original_fields=version.original_fields,
            owner_id=version.owner_id,
            source={"source_id": source.source_id, "revision": source.revision,
                    "license": source.license, "license_note": source.license_note,
                    "url": source.url},
        )

    def read_harness(self, task_id: str, actor_id: str) -> PosterInput:
        with self.maker() as session:
            user = self.actor(session, actor_id)
            task = session.get(HarnessTask, str(UUID(task_id)))
            if task is None:
                raise ValueError("Harness任务不存在")
            self.authorize(user, task.owner_id)
            result = task.result
            if (task.status != "completed" or task.cancel_requested
                    or task.workflow_version != "campaign-v1" or not isinstance(result, dict)
                    or result.get("status") != "materials_found"
                    or result.get("review", {}).get("approved") is not True
                    or result.get("missing_info")):
                raise ValueError("Harness任务没有完整且通过审核的结果")
            if not task.prompt.strip() or any(
                not isinstance(result.get(name), str) or not result[name].strip()
                for name in ("headline", "summary")
            ):
                raise ValueError("Harness需求或文案不完整")
            recommendations = result.get("recommendations")
            if not isinstance(recommendations, list) or not 1 <= len(recommendations) <= 2:
                raise ValueError("Harness素材数量无效")
            materials = []
            for item in recommendations:
                snapshot = self.snapshot(session, item["version_id"], user)
                for name in ("asset_id", "version_id", "source_id", "source_revision",
                             "record_id", "file_sha256", "original_fields", "title"):
                    if item[name] != getattr(snapshot, name):
                        raise ValueError(f"Harness素材来源字段不一致：{name}")
                if not isinstance(item.get("caption"), str) or not item["caption"].strip():
                    raise ValueError("Harness素材文案不完整")
                materials.append(snapshot)
            return PosterInput(
                parent_harness_task_id=task.id, parent_harness_run_id=task.run_id,
                original_prompt=task.prompt, harness_result=result,
                materials=tuple(materials), constraints=result.get("plan", {}),
            )

    def job_path(self, job_id):
        path = self.root / "jobs" / str(UUID(job_id)) / "job.json"
        if not path.resolve().is_relative_to(self.root):
            raise ValueError("海报任务文件路径无效")
        return path

    def save_job(self, job):
        PosterJob.model_validate(job.model_dump())
        path = self.job_path(job.job_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{uuid4().hex}.part")
        try:
            temporary.write_text(job.model_dump_json(indent=2), encoding="utf-8")
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    def load_job(self, job_id, actor_id):
        with self.maker() as session:
            user = self.actor(session, actor_id)
            job = PosterJob.model_validate_json(self.job_path(job_id).read_text(encoding="utf-8"))
            if job.job_id != str(UUID(job_id)):
                raise ValueError("任务文件身份不一致")
            self.authorize(user, job.owner_id)
            self.validate_input(job.input_data, user.id)
            return job

    def validate_input(self, inputs, actor_id):
        with self.maker() as session:
            user = self.actor(session, actor_id)
            for expected in inputs.materials:
                if self.snapshot(session, expected.version_id, user) != expected:
                    raise ValueError("保存的素材来源与指定版本不一致")

    def create_job(self, inputs, actor_id, direct_keywords=None):
        actor_id = str(UUID(actor_id))
        job_id = poster_job_id(inputs, actor_id, direct_keywords, KEYWORD_PROMPT_VERSION, POSTER_PROFILE)
        return PosterJob(
            job_id=job_id, owner_id=actor_id, preparation_run_id=job_id,
            input_data=inputs, input_sha256=digest(inputs.model_dump(mode="json")),
            direct_keywords=KeywordPlan(keywords=direct_keywords) if direct_keywords else None,
        )

    def prepare(self, task_id, actor_id):
        inputs = self.read_harness(task_id, actor_id)
        new_job = self.create_job(inputs, actor_id)
        with self.operation_lock(new_job.job_id):
            path = self.job_path(new_job.job_id)
            job = self.load_job(new_job.job_id, actor_id) if path.exists() else new_job
            if job.keyword_plan is not None:
                return job
            job = job.model_copy(update={"status": "preparing", "error": None})
            self.save_job(job)
            try:
                response = self.model_client.paid.text(
                    job.model_profile,
                    [{"role": "system", "content": KEYWORD_SYSTEM},
                     {"role": "user", "content": json.dumps(
                         inputs.model_dump(mode="json"), ensure_ascii=False,
                     )}],
                    run_id=job.preparation_run_id, step_key="keywords", timeout_seconds=120,
                )
                plan = KeywordPlan.model_validate_json(response["choices"][0]["message"]["content"])
            except Exception as error:
                self.save_job(job.model_copy(update={"status": "failed", "error": type(error).__name__}))
                raise
            job = job.model_copy(update={"keyword_plan": plan, "status": "prepared"})
            self.save_job(job)
            return job

    def prepare_direct(self, keywords, version_ids, actor_id, constraints=None):
        plan = KeywordPlan(keywords=keywords)
        with self.maker() as session:
            user = self.actor(session, actor_id)
            inputs = PosterInput(
                original_prompt=" / ".join(plan.keywords),
                materials=tuple(self.snapshot(session, item, user) for item in version_ids),
                constraints={} if constraints is None else constraints,
            )
        new_job = self.create_job(inputs, actor_id, list(plan.keywords))
        with self.operation_lock(new_job.job_id):
            if self.job_path(new_job.job_id).exists():
                return self.load_job(new_job.job_id, actor_id)
            job = new_job.model_copy(update={"keyword_plan": plan, "status": "prepared"})
            self.save_job(job)
            return job

    def frame_root(self, job):
        return self.job_path(job.job_id).parent / "framework"

    def record_path(self, job, run_id):
        if not re.fullmatch(r"run-[0-9a-f]{32}", run_id):
            raise ValueError("框架运行编号格式无效")
        path = self.frame_root(job) / "runs" / run_id / "run-record.json"
        if not path.resolve().is_relative_to(self.frame_root(job).resolve()):
            raise ValueError("框架记录路径超出任务目录")
        return path

    @staticmethod
    def constraints(job):
        return {"job_id": job.job_id, "shop_input": job.input_data.model_dump(mode="json"),
                "input_sha256": job.input_sha256,
                "parent_harness_task_id": job.input_data.parent_harness_task_id}

    @staticmethod
    def file_reference(reference):
        return FileReference(
            file_id=reference.artifact_id, run_id=reference.run_id, uri=reference.uri,
            revision=reference.revision, sha256=reference.sha256, media_type=reference.media_type,
        )

    def attach_materials(self, job, context):
        references = []
        with self.maker() as session:
            user = self.actor(session, job.owner_id)
            for expected in job.input_data.materials:
                if self.snapshot(session, expected.version_id, user) != expected:
                    raise ValueError("素材版本内容发生变化")
                version = session.get(AssetVersion, expected.version_id)
                data = MaterialRepository(session, self.settings.data_dir).read_file(version)
                reference = context.files.write_bytes(
                    f"sources/{expected.version_id}.image", data,
                    purpose=ArtifactPurpose.asset, media_type=expected.mime_type,
                )
                context.memory.publish_artifact(
                    f"source_image:{expected.version_id}", reference,
                    expected.model_dump(mode="json"),
                    ContentFormat(name="shop.material_source", version="1"),
                )
                references.append(reference)
        return tuple(references)

    def new_session(self, job, run_id=None):
        with self.operation_lock(job.job_id):
            return self._new_session(job, run_id)

    def _new_session(self, job, run_id=None):
        job = self.load_job(job.job_id, job.owner_id)
        if job.keyword_plan is None:
            raise ValueError("任务尚未完成关键词准备")
        run_id = run_id or f"run-{uuid4().hex}"
        path = self.record_path(job, run_id)
        if path.exists():
            raise ValueError("框架运行记录已存在")
        session = RunSession(
            OriginalRequest.from_keywords(run_id, job.keyword_plan.keywords, self.constraints(job)),
            root=self.frame_root(job), budget=POSTER_BUDGET,
            configuration={"job_id": job.job_id, "parent_harness_task_id": job.input_data.parent_harness_task_id,
                           "model_profile": job.model_profile.model_dump(),
                           "prompt_version": job.prompt_version,
                           "source_materials": [item.model_dump(mode="json")
                                                for item in job.input_data.materials]},
        )
        context = session.create_context(ExecutionIdentity(
            run_id=run_id, role=AgentRole.creative_director,
            invocation_id=f"source-{uuid4().hex}", execution_mode=ExecutionMode.real,
        ))
        self.attach_materials(job, context)
        session.save(path)
        self.save_job(job.model_copy(update={"frame_records": (*job.frame_records, run_id)}))
        return session

    def restore(self, job_id, run_id, actor_id):
        job = self.load_job(job_id, actor_id)
        if run_id not in job.frame_records:
            raise PermissionError("框架记录不属于当前任务")
        return self.load_session(job, self.record_path(job, run_id))

    def load_session(self, job, path):
        session = RunSession.load(path, root=self.frame_root(job), tool_registry=self.tools)
        constraints = {item.name: item.value for item in session.original_request.constraints}
        if constraints != self.constraints(job) or session.configuration.get("job_id") != job.job_id:
            raise ValueError("框架记录来源与海报任务不一致")
        if tuple(item.text for item in session.original_request.keywords) != job.keyword_plan.keywords:
            raise ValueError("框架关键词与准备结果不一致")
        if session.configuration.get("model_profile") != job.model_profile.model_dump():
            raise ValueError("框架模型配置与准备配置不一致")
        if ("prompt_version" in session.configuration
                and session.configuration["prompt_version"] != job.prompt_version):
            raise ValueError("框架提示版本与准备配置不一致")
        for reference in session.file_store.artifacts:
            session.file_store.read(reference)
        for item in job.input_data.materials:
            matches = [ref for ref in session.file_store.artifacts
                       if ref.uri.endswith(f"sources/{item.version_id}.image")]
            if len(matches) != 1 or matches[0].sha256 != item.file_sha256:
                raise ValueError("框架来源图片引用缺失或摘要不一致")
        return session

    def status(self):
        registered = {item.role for item in self.registrations}
        return {"roles": {role.value: "已接入" if role in registered else "待接入" for role in AgentRole},
                "interface_version": INTERFACE_VERSION,
                "budget": POSTER_BUDGET.model_dump()}

    def show(self, job_id, actor_id):
        job = self.load_job(job_id, actor_id)
        runs = [self.restore(job_id, run_id, actor_id) for run_id in job.frame_records]
        agent_runs = [self.load_session(job, self.frame_root(job) / item) for item in job.agent_records]
        with self.maker() as session:
            requests = session.scalars(select(ProviderRequest).where(
                ProviderRequest.run_id.in_([job.preparation_run_id, *job.frame_records]),
            ).order_by(ProviderRequest.created_at)).all()
        spent = sum((item.charged_cny or Decimal(0) for item in requests), Decimal(0))
        reserved = sum((item.reservation_cny for item in requests
                        if item.charged_cny is None), Decimal(0))
        return {"job": job.model_dump(mode="json"), "registration": self.status(),
                "runs": [{"run_id": item.run_id, "configuration": item.configuration,
                          "usage": item.usage.model_dump(), "final_result": item.final_result}
                         for item in runs],
                "agent_runs": [{"record": path, "run_id": item.run_id,
                                "usage": item.usage.model_dump(), "final_result": item.final_result}
                               for path, item in zip(job.agent_records, agent_runs, strict=True)],
                "cost": {"spent_cny": str(spent), "reserved_cny": str(reserved),
                         "requests": [{"id": item.id, "run_id": item.run_id,
                                       "step_key": item.step_key, "status": item.status,
                                       "usage": item.usage, "charged_cny": str(item.charged_cny)
                                       if item.charged_cny is not None else None,
                                       "reservation_cny": str(item.reservation_cny)} for item in requests]}}

    def run(self, job_id, actor_id):
        with self.operation_lock(job_id):
            job = self.load_job(job_id, actor_id)
            if job.keyword_plan is None:
                raise ValueError("任务尚未完成准备")
            missing = [role for role, status in self.status()["roles"].items() if status == "待接入"]
            if missing:
                self.save_job(job.model_copy(update={"status": "waiting_agents"}))
                raise ValueError("组员角色待接入：" + ", ".join(missing))
            if job.status in {"completed", "needs_review"}:
                return self.show(job_id, actor_id)
            references = {}
            registrations = []
            for registration in self.registrations:
                factory = registration.instance_factory

                def create(config, factory=factory):
                    return MaterialAgent(factory(config), self, job, references)

                registrations.append(replace(registration, instance_factory=create))
            workflow = PosterWorkflow(
                AgentSet(registrations), root=self.frame_root(job), tool_registry=self.tools,
                budget=POSTER_BUDGET, configuration={"job_id": job.job_id,
                    "parent_harness_task_id": job.input_data.parent_harness_task_id,
                    "model_profile": job.model_profile.model_dump(), "prompt_version": job.prompt_version},
            )
            self.save_job(job.model_copy(update={"status": "running", "error": None}))
            try:
                result = asyncio.run(workflow.run(job.keyword_plan.keywords, constraints=self.constraints(job)))
            except Exception as error:
                self.capture_records(job, "failed", type(error).__name__)
                raise
            self.capture_records(job, "completed" if result.status.value == "passed" else "needs_review")
            return self.show(job_id, actor_id)

    def capture_records(self, job, status, error=None):
        records = tuple(sorted(path.parent.name for path in self.frame_root(job).glob("runs/*/run-record.json")))
        self.save_job(job.model_copy(update={"frame_records": records, "status": status, "error": error}))

    def agent_run(self, job_id, role, actor_id, upstream_record=None):
        with self.operation_lock(job_id):
            job = self.load_job(job_id, actor_id)
            registration = next((item for item in self.registrations if item.role == AgentRole(role)), None)
            if registration is None:
                raise ValueError(f"组员角色待接入：{role}")
            target = self.frame_root(job) / "agent-records" / f"{uuid4().hex}.json"
            if upstream_record is not None:
                if upstream_record not in job.agent_records:
                    raise PermissionError("上游单角色记录不属于当前任务")
                source = self.frame_root(job) / upstream_record
                self.load_session(job, source)
                try:
                    result = asyncio.run(run_single_agent_from_upstream(
                        registration, source, root=self.frame_root(job), record_path=target,
                    ))
                finally:
                    self.capture_agent_record(job_id, actor_id, target)
            else:
                if registration.role != AgentRole.creative_director:
                    raise ValueError("此角色需要指定已导出的上游单角色记录")
                session = self._new_session(job)
                identity = ExecutionIdentity(
                    run_id=session.run_id, role=registration.role,
                    invocation_id=f"invoke-{uuid4().hex}", execution_mode=ExecutionMode.real,
                )
                references = session.file_store.artifacts
                metadata = session.create_context(identity).metadata.model_copy(update={
                    "files": tuple(self.file_reference(item) for item in references),
                    "version_dependencies": tuple(VersionDependency(
                        artifact_id=item.artifact_id, revision=item.revision,
                    ) for item in references),
                })
                inputs = AgentInput(
                    content=session.original_request.model_dump(mode="json"),
                    content_format=WORKFLOW_REQUEST_FORMAT, metadata=metadata,
                    provenance=ContentProvenance(run_id=session.run_id,
                                                 source_id=session.original_request.request_id),
                )
                try:
                    result = asyncio.run(run_single_agent(registration, inputs, session=session, record_path=target))
                finally:
                    self.capture_agent_record(job_id, actor_id, target)
            return {"record": str(target.relative_to(self.frame_root(job))),
                    "output": result.output.model_dump(mode="json")}

    def capture_agent_record(self, job_id, actor_id, path):
        if path.exists():
            job = self.load_job(job_id, actor_id)
            self.save_job(job.model_copy(update={"agent_records": (
                *job.agent_records, str(path.relative_to(self.frame_root(job))),
            )}))
