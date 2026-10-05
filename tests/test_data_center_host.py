from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest
from data_center_protocol import ModelInvocation
from integration_protocol import ResourceRef
from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker

from shop.auth import hash_password
from shop.budget import BudgetExceeded, BudgetService
from shop.config import Settings
from shop.data_center_gateway import ShopDataModelGateway
from shop.data_center_host import ShopDataAuthorization, ShopSourceReadPort
from shop.data_center_models import DataCenterGrant, DataCenterSource
from shop.data_center_registry import DataCenterRegistry
from shop.integration import ShopAuthentication
from shop.integration_models import IntegrationBudgetWindow
from shop.models import User
from shop.runtime_models import ProviderRequest


def document_source(session, actor_id):
    path = Path("docs/03-技术设计/总体接口.md")
    digest = sha256(path.read_bytes()).hexdigest()
    ref = ResourceRef(module_id="data_center", resource_type="source", resource_id="interfaces.md",
                      version="eceffd4", sha256=digest)
    session.add(DataCenterSource(
        source_id="project-shop/interfaces", kind="document", title="总体接口",
        revision="eceffd4", nature="technical_sample", license_id="project-owned",
        resource=ref.model_dump(mode="json"), relative_path=str(path), sha256=digest,
        properties={"record_id": "interfaces"}, active=True,
    ))
    session.flush()
    session.add(DataCenterGrant(actor_id=actor_id, source_id="project-shop/interfaces",
                               permissions=["read"], asset_ids=["interfaces"], source_wide=False))
    session.commit()
    return ref


@pytest.mark.asyncio
async def test_document_read_requires_current_grant_and_exact_reference(db_session):
    user = User(username="document-reader", password_hash=hash_password("local-test"), role="viewer")
    db_session.add(user)
    db_session.commit()
    maker = sessionmaker(db_session.bind, expire_on_commit=False)
    settings = Settings(session_secret="local-document-test", data_center_source_root=Path("."))
    auth = ShopAuthentication(settings, maker)
    authorization = ShopDataAuthorization(auth, maker)
    source = ShopSourceReadPort(settings, maker, authorization)
    ref = document_source(db_session, user.id)
    context = auth.issue(user.id, str(uuid4()))
    assert await source.read_file(ref, context) == Path("docs/03-技术设计/总体接口.md").read_bytes()
    with pytest.raises(ValueError):
        await source.read_file(ref.model_copy(update={"sha256": "a" * 64}), context)
    scope = await authorization.authorize(context, ["project-shop/interfaces"], "read")
    assert scope.asset_ids == ["project-shop/interfaces::interfaces"]
    grant = db_session.get(DataCenterGrant, (user.id, "project-shop/interfaces"))
    grant.permissions = []
    db_session.commit()
    with pytest.raises(PermissionError):
        await source.read_file(ref, context)


@pytest.mark.asyncio
async def test_quota_read_keeps_authorized_employee_and_salesperson_source_identity(
    db_session, assessment_accounts,
):
    from shop.assessment_models import AssessmentSource
    from shop.assessment_types import SOURCE_KEY

    original = db_session.get(AssessmentSource, SOURCE_KEY)
    files = {item["name"]: item for item in original.manifest["files"]}
    db_session.add(DataCenterSource(
        source_id="microsoft/adventureworks", kind="analytics", title="季度指标",
        revision=original.revision, nature="public_business_sample", license_id="MIT",
        sha256=files["Employee.csv"]["sha256"], properties={}, active=True,
    ))
    db_session.commit()
    maker = sessionmaker(db_session.bind, expire_on_commit=False)
    settings = Settings(session_secret="identity-source-test")
    authentication = ShopAuthentication(settings, maker)
    authorization = ShopDataAuthorization(authentication, maker)
    port = ShopSourceReadPort(settings, maker, authorization)
    context = authentication.issue(assessment_accounts["employee"].id, str(uuid4()))
    record = await port.read_current("microsoft/adventureworks", "quota:275", context)
    identities = record.get("identity_records")
    assert identities is not None
    assert identities == [
        {"source_id": original.source_id, "revision": original.revision,
         "record_id": "employee:275", "file_name": "Employee.csv", "source_line": 275,
         "source_sha256": files["Employee.csv"]["sha256"], "BusinessEntityID": 275},
        {"source_id": original.source_id, "revision": original.revision,
         "record_id": "salesperson:275", "file_name": "SalesPerson.csv", "source_line": 2,
         "source_sha256": files["SalesPerson.csv"]["sha256"], "BusinessEntityID": 275},
    ]
    with pytest.raises(PermissionError):
        await port.read_current("microsoft/adventureworks", "quota:276", context)


@pytest.mark.asyncio
async def test_sync_account_cannot_read_unregistered_or_frontend_source(db_session):
    user = User(username="data-sync", password_hash=hash_password("local-test"), role="data_sync")
    db_session.add(user)
    db_session.commit()
    maker = sessionmaker(db_session.bind, expire_on_commit=False)
    settings = Settings(session_secret="local-sync-test")
    auth = ShopAuthentication(settings, maker)
    authorization = ShopDataAuthorization(auth, maker)
    context = auth.issue(user.id, str(uuid4()))
    document_source(db_session, user.id)
    with pytest.raises(PermissionError):
        await authorization.authorize(context, ["project-shop/interfaces"], "read")
    with pytest.raises(PermissionError):
        await authorization.authorize(context, ["unregistered-source"], "sync")
    grant = db_session.get(DataCenterGrant, (user.id, "project-shop/interfaces"))
    grant.permissions = ["sync"]
    db_session.commit()
    scope = await authorization.authorize(context, ["project-shop/interfaces"], "sync")
    assert scope.sync_actor and not scope.source_admin


@pytest.mark.asyncio
async def test_cancelled_task_and_conflicting_input_stop_before_model_request(db_session):
    user = User(username="data-query", password_hash=hash_password("local-test"), role="viewer")
    db_session.add(user)
    db_session.commit()
    maker = sessionmaker(db_session.bind, expire_on_commit=False)
    settings = Settings(session_secret="local-task-test")
    auth = ShopAuthentication(settings, maker)
    authorization = ShopDataAuthorization(auth, maker)
    gateway = ShopDataModelGateway(settings, maker, authorization)
    document_source(db_session, user.id)
    context = auth.issue(user.id, str(uuid4()))
    task_id, run_id = str(uuid4()), str(uuid4())
    sources = ["project-shop/interfaces"]
    question = {"question": "公共接口有哪些模块？"}
    await gateway.bind(task_id, run_id, "query", sources, question, context)
    await gateway.bind(task_id, run_id, "query", sources, question, context)
    with pytest.raises(ValueError):
        await gateway.bind(task_id, run_id, "query", sources, {"question": "修改后的输入"}, context)
    await gateway.cancel(task_id, context)
    request = ModelInvocation(task_id=task_id, run_id=run_id, step_key="answer", kind="chat",
                              model="deepseek-flash", source_ids=sources,
                              messages=[{"role": "user", "content": question["question"]}])
    with pytest.raises(PermissionError):
        await gateway.invoke(request, context)
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0


@pytest.mark.asyncio
async def test_validation_window_includes_existing_unsent_reservation(db_session):
    user = User(username="data-budget", password_hash=hash_password("local-test"), role="viewer")
    db_session.add(user)
    db_session.commit()
    maker = sessionmaker(db_session.bind, expire_on_commit=False)
    settings = Settings(session_secret="local-budget-test")
    auth = ShopAuthentication(settings, maker)
    authorization = ShopDataAuthorization(auth, maker)
    gateway = ShopDataModelGateway(settings, maker, authorization)
    document_source(db_session, user.id)
    context = auth.issue(user.id, str(uuid4()))
    task_id, run_id = str(uuid4()), str(uuid4())
    await gateway.bind(task_id, run_id, "query", ["project-shop/interfaces"],
                       {"question": "公共接口是什么？"}, context)
    body = {"model": "deepseek-flash", "messages": [{"role": "user", "content": "公共接口是什么？"}]}
    prior = BudgetService(maker).reserve(task_id=None, run_id=str(uuid4()), step_key="budget-check",
                                         service="deepseek", model="deepseek-flash", request_body=body,
                                         input_limit=2000, output_limit=100)
    assert prior.reservation_cny == Decimal("0.00480000")
    db_session.add(IntegrationBudgetWindow(id="data-budget-check", limit_cny=Decimal("0.006"),
                                           include_open_reservations=True, active=True))
    db_session.commit()
    with pytest.raises(BudgetExceeded):
        BudgetService(maker, verification_scope="data_center").reserve(
            task_id=None, run_id=run_id, step_key="answer", service="deepseek", model="deepseek-flash",
            request_body=body, input_limit=1000, output_limit=100)
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 1
    saved = db_session.get(ProviderRequest, prior.id)
    assert saved.status == "reserved" and saved.reservation_cny == Decimal("0.00480000")


def test_registry_requires_admin_and_checks_real_source_content(db_session):
    from data_center_protocol import SourceRegistration

    viewer = User(username="registry-viewer", password_hash=hash_password("local-test"), role="viewer")
    admin = User(username="registry-admin", password_hash=hash_password("local-test"), role="admin")
    db_session.add_all([viewer, admin])
    db_session.commit()
    maker = sessionmaker(db_session.bind, expire_on_commit=False)
    settings = Settings(session_secret="registry-test", data_center_source_root=Path("."))
    auth = ShopAuthentication(settings, maker)
    registry = DataCenterRegistry(settings, maker, auth)
    path = Path("docs/03-技术设计/总体接口.md")
    checksum = sha256(path.read_bytes()).hexdigest()
    spec = SourceRegistration(source_id="registry/interfaces", kind="document", title="总体接口",
                              revision="eceffd4", nature="technical_sample", license_id="project-owned",
                              sha256=checksum, relative_path=str(path), properties={"record_id": "interfaces"},
                              resource=ResourceRef(module_id="data_center", resource_type="source",
                                                   resource_id="registry/interfaces.md", version="eceffd4", sha256=checksum))
    with pytest.raises(PermissionError):
        registry.register(spec, auth.issue(viewer.id, str(uuid4())))
    with pytest.raises(ValueError, match="摘要"):
        changed = spec.model_copy(update={"sha256": "b" * 64})
        registry.register(changed, auth.issue(admin.id, str(uuid4())))
    registry.register(spec, auth.issue(admin.id, str(uuid4())))
    assert registry.list(auth.issue(admin.id, str(uuid4())))[0]["source_id"] == spec.source_id
    registry.withdraw(spec.source_id, auth.issue(admin.id, str(uuid4())))
    assert not registry.list(auth.issue(admin.id, str(uuid4())))[0]["active"]


def test_document_registration_with_resource_free_business_source(db_session):
    from data_center_protocol import SourceRegistration

    from shop.assessment_order_adapter import ORDER_FILE_SHA256

    admin = User(username="registry-mixed-sources", password_hash=hash_password("local-test"),
                 role="admin")
    db_session.add(admin)
    db_session.add(DataCenterSource(
        source_id="microsoft/adventureworks", kind="business", title="AdventureWorks订单",
        revision="beaab06ef72831089ca80e5355d65e661fd19b26", nature="public_business_sample",
        license_id="MIT", resource=None, sha256=ORDER_FILE_SHA256, properties={}, active=True,
    ))
    db_session.commit()
    maker = sessionmaker(db_session.bind, expire_on_commit=False)
    settings = Settings(session_secret="registry-mixed-test", data_center_source_root=Path("."))
    authentication = ShopAuthentication(settings, maker)
    registry = DataCenterRegistry(settings, maker, authentication)
    path = Path("docs/03-技术设计/总体接口.md")
    checksum = sha256(path.read_bytes()).hexdigest()
    specification = SourceRegistration(
        source_id="registry/mixed-interface", kind="document", title="总体接口",
        revision="eceffd4", nature="technical_sample", license_id="project-owned",
        sha256=checksum, relative_path=str(path), properties={"record_id": "interfaces"},
        resource=ResourceRef(module_id="data_center", resource_type="source",
                             resource_id="registry/mixed-interface.md", version="eceffd4",
                             sha256=checksum),
    )
    saved = registry.register(specification, authentication.issue(admin.id, str(uuid4())))
    assert saved["source_id"] == specification.source_id
    assert saved["resource"]["sha256"] == checksum
    duplicate = specification.model_copy(update={"source_id": "registry/duplicate-interface"})
    with pytest.raises(ValueError, match="资源编号"):
        registry.register(duplicate, authentication.issue(admin.id, str(uuid4())))


def test_registry_keeps_source_metadata_immutable_within_revision(db_session):
    from data_center_protocol import SourceRegistration

    admin = User(username="registry-versioned-source", password_hash=hash_password("local-test"),
                 role="admin")
    db_session.add(admin)
    db_session.commit()
    maker = sessionmaker(db_session.bind, expire_on_commit=False)
    settings = Settings(session_secret="registry-version-test", data_center_source_root=Path("."))
    authentication = ShopAuthentication(settings, maker)
    registry = DataCenterRegistry(settings, maker, authentication)
    path = Path("docs/03-技术设计/总体接口.md")
    checksum = sha256(path.read_bytes()).hexdigest()
    specification = SourceRegistration(
        source_id="registry/versioned-interface", kind="document", title="总体接口",
        revision="eceffd4", nature="technical_sample", license_id="project-owned",
        sha256=checksum, relative_path=str(path), properties={"record_id": "interfaces"},
        resource=ResourceRef(module_id="data_center", resource_type="source",
                             resource_id="registry/versioned-interface.md", version="eceffd4",
                             sha256=checksum),
    )
    registry.register(specification, authentication.issue(admin.id, str(uuid4())))
    registry.register(specification, authentication.issue(admin.id, str(uuid4())))
    changed = specification.model_copy(update={
        "properties": {"record_id": "interfaces", "upstream_source_id": "project-shop"},
    })
    with pytest.raises(ValueError, match="来源版本"):
        registry.register(changed, authentication.issue(admin.id, str(uuid4())))
    with maker() as session:
        assert registry.view(session.get(DataCenterSource, specification.source_id))["properties"] == {
            "record_id": "interfaces",
        }
    revised = changed.model_copy(update={
        "revision": "metadata-v2",
        "resource": changed.resource.model_copy(update={"version": "metadata-v2"}),
    })
    saved = registry.register(revised, authentication.issue(admin.id, str(uuid4())))
    assert saved["revision"] == "metadata-v2"
    assert saved["properties"] == revised.properties
    with pytest.raises(ValueError, match="来源版本"):
        registry.register(changed, authentication.issue(admin.id, str(uuid4())))
    restored = registry.register(specification, authentication.issue(admin.id, str(uuid4())))
    assert restored["revision"] == specification.revision
    assert restored["properties"] == specification.properties


@pytest.mark.asyncio
async def test_data_task_budget_obeys_parent_goal_before_integration_mapping(db_session):
    from agent_cluster.store import ClusterStore
    from agent_cluster.types import (
        CompletionCriterion,
        GoalDefinition,
        TaskPlan,
        TaskSpec,
        TriggerEvent,
    )

    user = User(username="data-goal", password_hash=hash_password("local-test"), role="viewer")
    db_session.add(user)
    db_session.commit()
    maker = sessionmaker(db_session.bind, expire_on_commit=False)
    auth = ShopAuthentication(Settings(session_secret="data-goal-test"), maker)
    document_source(db_session, user.id)
    store = ClusterStore(db_session.bind)
    store.initialize()
    goal = GoalDefinition(actor_id=user.id, objective="核查数据中台目标费用",
                          allowed_operations={"shop_data_center_agent": ["query"]},
                          criteria=[CompletionCriterion(tool_name="shop_data_center_agent", action="query")],
                          budget_cny=Decimal("0.001"))
    store.create_goal(goal)
    run = store.trigger(TriggerEvent(goal_id=goal.goal_id, actor_id=user.id, event_id=str(uuid4())))
    store.set_run(run.run_id, user.id, status="running")
    plan = TaskPlan(tasks=[TaskSpec(task_id="query", tool_name="shop_data_center_agent", action="query")])
    store.save_plan(run.run_id, user.id, plan, expected_version=0)
    task = store.tasks(run.run_id, user.id)[0]
    store.set_task(run.run_id, user.id, "query", status="sent")
    gateway = ShopDataModelGateway(Settings(session_secret="data-goal-test"), maker, ShopDataAuthorization(auth, maker))
    child_run = str(uuid4())
    await gateway.bind(str(uuid4()), child_run, "query", ["project-shop/interfaces"],
                       {"question": "接口是什么？"}, auth.issue(user.id, task["request_id"]))
    db_session.add(IntegrationBudgetWindow(id="data-goal-check", limit_cny=Decimal(50),
                                           include_open_reservations=True, active=True))
    db_session.commit()
    with pytest.raises(BudgetExceeded, match="目标预算"):
        BudgetService(maker, verification_scope="data_center").reserve(
            task_id=None, run_id=child_run, step_key="answer", service="deepseek", model="deepseek-flash",
            request_body={"model": "deepseek-flash", "messages": [{"role": "user", "content": "接口是什么？"}]},
            input_limit=2000, output_limit=100)
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0
