import argparse
import hashlib
import json
import os
import re
import secrets
import sys
import time
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker

from shop import (
    integration_models,
)
from shop.adventureworks import REVISION as CUSTOMER_REVISION
from shop.adventureworks import import_orders, source_manifest
from shop.assessment_dataset import REVISION as ASSESSMENT_REVISION
from shop.assessment_models import AssessmentAccess, AssessmentEmployee
from shop.assessment_service import import_employee_source
from shop.asset_index import MAIN_SOURCE, IndexService
from shop.auth import hash_password
from shop.campaign import CampaignService
from shop.campaign_types import RequirementPlan
from shop.config import Settings
from shop.customer_models import CustomerBinding
from shop.models import Asset, AssetVersion, Base, Source, User
from shop.paid_models import PaidModels
from shop.runtime_models import BudgetAccount, HarnessTask, ProviderRequest


def _read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    path.chmod(0o600)


def _config_paths(inputs):
    return {name: Path(path).resolve(strict=True) for name, path in inputs["hub_configs"].items()}


def _database_url(args):
    value = args.database_url or os.environ.get("MCP04_DATABASE_URL")
    if not value:
        raise ValueError("MCP04_DATABASE_URL必须通过受保护环境提供")
    return value


def _material_snapshot(source_session):
    latest = select(
        AssetVersion.asset_id, func.max(AssetVersion.version).label("version")
    ).group_by(AssetVersion.asset_id).subquery()
    row = source_session.execute(
        select(Asset, AssetVersion).join(
            AssetVersion, AssetVersion.asset_id == Asset.id
        ).join(
            latest,
            (latest.c.asset_id == AssetVersion.asset_id)
            & (latest.c.version == AssetVersion.version),
        ).where(
            Asset.source_id == MAIN_SOURCE,
            Asset.kind == "image",
        ).order_by(Asset.source_order).limit(1)
    ).one()
    asset, version = row
    source = source_session.get(Source, (asset.source_id, asset.source_revision))
    if source is None:
        raise ValueError("真实素材来源记录不存在")
    return source, asset, version


def _copy_source_record(model, row):
    return model(**{
        column.name: getattr(row, column.name)
        for column in model.__table__.columns
    })


def _write_hub_configs(inputs, config_dir, base_url, unauthorized_user):
    config_dir = Path(config_dir)
    config_dir.mkdir(parents=True, exist_ok=False)
    config_dir.chmod(0o700)
    paths = _config_paths(inputs)
    outputs = {}
    for name, source_path in paths.items():
        config = _read_json(source_path)
        for connector in config["connectors"]:
            connector["base_url"] = base_url
            password_path = Path(connector["password_file"])
            if not password_path.is_absolute():
                password_path = source_path.parent / password_path
            password_path = password_path.resolve(strict=True)
            if password_path.stat().st_mode & 0o077:
                raise PermissionError("账号密码文件权限必须限制为当前用户")
            connector["password_file"] = str(password_path)
        output_path = config_dir / f"{name}.json"
        _write_json(output_path, config)
        outputs[name] = {
            "path": str(output_path),
            "sha256": hashlib.sha256(output_path.read_bytes()).hexdigest(),
        }

    source_path = paths["assessment-empty"]
    config = _read_json(source_path)
    password_path = config_dir / "assessment-unassigned.password"
    password_path.write_text(unauthorized_user["password"], encoding="utf-8")
    password_path.chmod(0o600)
    for connector in config["connectors"]:
        connector.update({
            "base_url": base_url,
            "username": unauthorized_user["username"],
            "expected_actor_id": unauthorized_user["actor_id"],
            "password_file": str(password_path),
        })
    output_path = config_dir / "assessment-unassigned.json"
    _write_json(output_path, config)
    outputs["assessment-unassigned"] = {
        "path": str(output_path),
        "sha256": hashlib.sha256(output_path.read_bytes()).hexdigest(),
    }
    return outputs


def prepare(args):
    inputs = _read_json(args.inputs)
    source_settings = Settings()
    test_database_url = make_url(source_settings.database_url).set(database="shop_test").render_as_string(
        hide_password=False,
    )
    if make_url(test_database_url).database != "shop_test":
        raise ValueError("SHOP_TEST_DATABASE_URL必须指向shop_test数据库")
    admin_engine = create_engine(test_database_url, pool_pre_ping=True)
    run_token = re.sub(r"[^a-z0-9_]", "_", args.run_id.lower())
    schema = f"mcp04_{run_token[-32:]}"
    if not re.fullmatch(r"mcp04_[a-z0-9_]{1,48}", schema):
        raise ValueError("独立测试schema名称无效")
    with admin_engine.begin() as connection:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public"))
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    database_url = str(make_url(test_database_url).update_query_dict({
        "options": f"-csearch_path={schema},public",
    }).render_as_string(hide_password=False))
    service_database_url = str(make_url(database_url).update_query_dict({
        "application_name": f"mcp04-api-{run_token[-20:]}",
    }).render_as_string(hide_password=False))
    engine = create_engine(database_url, pool_pre_ping=True)
    Base.metadata.create_all(engine)
    maker = sessionmaker(engine, expire_on_commit=False)

    original_root = Path(args.original_root).resolve(strict=True)
    worktree_root = Path(args.worktree_root).resolve(strict=True)
    order_directory = original_root / ".local" / "data" / "orders" / CUSTOMER_REVISION
    assessment_directory = original_root / ".local" / "assessment" / "source"
    manifest = source_manifest(order_directory)
    source_engine = create_engine(
        source_settings.database_url, pool_pre_ping=True,
    ).execution_options(postgresql_readonly=True)
    source_maker = sessionmaker(source_engine, expire_on_commit=False)
    account_ids = {
        entry["actor_id"]
        for entry in inputs["accounts"].values()
    }
    imported_orders = None
    imported_assessment = None
    access_records = {}
    binding_records = {}
    material_info = {}
    salesperson_ids = []
    with maker() as session:
        imported_orders = import_orders(session, order_directory)
    with source_maker() as source_session, maker.begin() as session:
        imported_assessment = import_employee_source(session, assessment_directory)
        salesperson_ids = list(session.scalars(select(AssessmentEmployee.employee_id).where(
            AssessmentEmployee.is_salesperson.is_(True),
        ).order_by(AssessmentEmployee.employee_id)).all())
        source_users = {
            user.id: user for user in source_session.scalars(
                select(User).where(User.id.in_(account_ids))
            ).all()
        }
        missing_users = sorted(account_ids - set(source_users))
        if missing_users:
            raise LookupError(f"主库没有固定身份记录：{len(missing_users)}")
        source_record, asset, version = _material_snapshot(source_session)
        needed_user_ids = account_ids | {asset.owner_id, version.owner_id}
        all_source_users = {
            user.id: user for user in source_session.scalars(
                select(User).where(User.id.in_(needed_user_ids))
            ).all()
        }
        if set(all_source_users) != needed_user_ids:
            raise LookupError("素材来源账号记录不完整")
        for user in all_source_users.values():
            session.add(_copy_source_record(User, user))
        session.flush()
        session.add(_copy_source_record(Source, source_record))
        session.flush()
        session.add(_copy_source_record(Asset, asset))
        session.add(_copy_source_record(AssetVersion, version))

        source_bindings = source_session.scalars(
            select(CustomerBinding).where(CustomerBinding.user_id.in_(account_ids))
        ).all()
        for row in source_bindings:
            session.add(_copy_source_record(CustomerBinding, row))
            binding_records[row.user_id] = {
                "source_revision": row.source_revision,
                "customer_id": row.customer_id,
            }

        source_access = source_session.scalars(
            select(AssessmentAccess).where(AssessmentAccess.user_id.in_(account_ids))
        ).all()
        for row in source_access:
            session.add(_copy_source_record(AssessmentAccess, row))
            access_records[row.user_id] = {
                "level": row.level,
                "employee_id": row.employee_id,
                "managed_employee_ids": list(row.managed_employee_ids),
            }
        material_info = {
            "source_id": asset.source_id,
            "source_revision": asset.source_revision,
            "record_id": asset.record_id,
            "asset_id": asset.id,
            "version_id": version.id,
            "file_sha256": version.file_sha256,
            "attributes": version.original_fields,
        }

        unauthorized_password = secrets.token_urlsafe(32)
        unauthorized_user = {
            "actor_id": str(uuid4()),
            "username": f"mcp04-unassigned-{run_token[-12:]}",
            "password": unauthorized_password,
        }
        session.add(User(
            id=unauthorized_user["actor_id"],
            username=unauthorized_user["username"],
            password_hash=hash_password(unauthorized_password),
            role="viewer",
        ))

    expected_customers = {
        inputs["accounts"][name]["actor_id"]: customer_id
        for name, customer_id in (("customer", 11091), ("other", 11176))
    }
    for user_id, customer_id in expected_customers.items():
        if binding_records.get(user_id, {}).get("customer_id") != customer_id:
            raise ValueError("复制后的客户绑定与固定账号不一致")
    for name in ("assessment", "assessment-manager", "assessment-manager2", "assessment-admin",
                 "assessment-empty"):
        user_id = inputs["accounts"][name]["actor_id"]
        if user_id not in access_records:
            raise LookupError(f"主库缺少{ name }考核权限记录")

    config_dir = Path(args.config_dir)
    config_paths_out = _write_hub_configs(
        inputs, config_dir, args.base_url, unauthorized_user,
    )
    with engine.connect() as connection:
        database, current_schema, server_version = connection.execute(text(
            "SELECT current_database(), current_schema(), current_setting('server_version')"
        )).one()
    application_name = f"mcp04-api-{run_token[-20:]}"
    with maker() as session:
        provider_request_count = session.scalar(select(func.count(ProviderRequest.id)))
    report = {
        "milestone": "MCP-04",
        "check": "isolated_schema_setup",
        "run_id": args.run_id,
        "status": "prepared",
        "database": {"database": database, "schema": current_schema,
                     "postgresql_version": server_version},
        "service_application_name": application_name,
        "sources": {
            "customer_orders": {
                "source_id": manifest["source_id"],
                "revision": manifest["revision"],
                "manifest_sha256": hashlib.sha256(json.dumps(
                    manifest, sort_keys=True, separators=(",", ":")
                ).encode()).hexdigest(),
                "import_result": imported_orders,
            },
            "assessment": {
                "revision": ASSESSMENT_REVISION,
                "fingerprint": imported_assessment["fingerprint"],
                "employees": imported_assessment["employees"],
                "quotas": imported_assessment["quotas"],
                "salesperson_ids": salesperson_ids,
            },
            "material": material_info,
        },
        "accounts": {
            name: {
                "actor_id": entry["actor_id"],
                "username": entry["username"],
                "customer_binding": binding_records.get(entry["actor_id"]),
                "assessment_access": access_records.get(entry["actor_id"]),
            }
            for name, entry in inputs["accounts"].items()
        },
        "unassigned_account": {
            "actor_id": unauthorized_user["actor_id"],
            "username": unauthorized_user["username"],
            "assessment_access": None,
        },
        "hub_configs": config_paths_out,
        "conditions": {
            "python_version": sys.version.split()[0],
            "business_source_root": str(worktree_root / "src"),
            "new_schema_only": True,
            "source_account_password_hashes_copied": len(all_source_users),
            "provider_requests_created_during_setup": provider_request_count,
        },
    }
    _write_json(Path(args.evidence_dir) / "schema-setup.json", report)
    provider_env_file = source_settings.provider_env_file
    output = {
        "database_url": database_url,
        "service_database_url": service_database_url,
        "provider_env_file": str(provider_env_file.resolve()) if provider_env_file else None,
        "session_secret": secrets.token_urlsafe(48),
        "report": report,
    }
    runtime_path = Path(args.evidence_dir) / "runtime.json"
    _write_json(runtime_path, output)
    print(json.dumps({"runtime_file": str(runtime_path)}, ensure_ascii=False))
    source_engine.dispose()
    engine.dispose()
    admin_engine.dispose()


def build_request(args):
    database_url = _database_url(args)
    original_root = Path(args.original_root).resolve(strict=True)
    data_dir = original_root / ".local" / "data"
    settings = Settings(
        database_url=database_url,
        data_dir=data_dir,
        poster_dir=original_root / ".local" / "poster",
        harness_token_limits_file=original_root / ".local" / "harness" / "token-limits.json",
    )
    service = CampaignService(settings)
    try:
        task = service.show(args.task_id)
        source_values = IndexService(settings, service.maker).attribute_values()
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
            source_values = IndexService(settings, service.maker).attribute_values()
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
        messages = [
            {"role": "system", "content": instruction},
            {"role": "user", "content": task["prompt"]},
        ]
        profile = service._profile(args.task_id, "understand")
        body = profile.body(messages)
        with service.maker.begin() as session:
            task_row = session.get(HarnessTask, args.task_id, with_for_update=True)
            if task_row is None:
                raise LookupError("Harness任务不存在")
            if session.scalar(select(ProviderRequest).where(
                ProviderRequest.run_id == task_row.run_id,
                ProviderRequest.step_key == "understand",
            )) is not None:
                raise ValueError("Harness任务已经存在understand请求")
            request = service.budget.reserve(
                task_id=task_row.id,
                run_id=task_row.run_id,
                step_key="understand",
                service=profile.service,
                model=profile.model,
                request_body=body,
                input_limit=PaidModels.input_bound(messages),
                output_limit=profile.output_limit,
                session=session,
            )
            task_row.model_calls = 1
        print(json.dumps({
            "task_id": args.task_id,
            "run_id": task["run_id"],
            "request_id": request.id,
            "status": request.status,
            "reservation_cny": str(request.reservation_cny),
            "charged_cny": None,
            "response_body_present": request.response_body is not None,
            "prompt_version": task["model_profile"].get("prompt_version"),
            "model_profile_sha256": hashlib.sha256(json.dumps(
                profile.model_dump(mode="json"), ensure_ascii=False, sort_keys=True,
                separators=(",", ":"),
            ).encode()).hexdigest(),
            "request_body_sha256": hashlib.sha256(json.dumps(
                body, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            ).encode()).hexdigest(),
        }, ensure_ascii=False))
    finally:
        service.engine.dispose()


def _snapshot(database_url, task_id, service_application_name=None, blocker_pid=None):
    engine = create_engine(database_url, pool_pre_ping=True)
    maker = sessionmaker(engine, expire_on_commit=False)
    with maker() as session:
        task = session.get(HarnessTask, task_id)
        if task is None:
            raise LookupError("Harness任务不存在")
        requests = session.scalars(select(ProviderRequest).where(
            ProviderRequest.task_id == task_id,
        ).order_by(ProviderRequest.created_at)).all()
        account = session.get(BudgetAccount, "project")
        integrations = session.scalars(select(
            integration_models.IntegrationCall
        ).where(integration_models.IntegrationCall.resource_id == task_id)).all()
        activities = []
        if service_application_name is not None:
            rows = session.execute(text(
                "SELECT pid, application_name, state, wait_event_type, wait_event, query "
                "FROM pg_stat_activity WHERE application_name = :application_name "
                "AND (:blocker_pid IS NULL OR :blocker_pid = ANY(pg_blocking_pids(pid))) "
                "ORDER BY pid"
            ), {
                "application_name": service_application_name,
                "blocker_pid": blocker_pid,
            }).mappings().all()
            activities = [dict(row) for row in rows]
        return {
            "task": {
                "id": task.id,
                "status": task.status,
                "cancel_requested": task.cancel_requested,
                "model_calls": task.model_calls,
                "tool_calls": task.tool_calls,
            },
            "provider_requests": [{
                "id": request.id,
                "step_key": request.step_key,
                "status": request.status,
                "charged_cny": str(request.charged_cny) if request.charged_cny is not None else None,
                "reservation_cny": str(request.reservation_cny),
                "response_body_present": request.response_body is not None,
            } for request in requests],
            "project_account": None if account is None else {
                "spent_cny": str(account.spent_cny),
                "reserved_cny": str(account.reserved_cny),
            },
            "integration_call_count": len(integrations),
            "backend_activity": activities,
        }
    engine.dispose()


def wait_running(args):
    database_url = _database_url(args)
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        state = _snapshot(
            database_url, args.task_id, args.application_name, args.blocker_pid,
        )
        active = [row for row in state["backend_activity"] if (
            row["wait_event_type"] == "Lock"
            and "provider_requests" in row["query"].lower()
        )]
        if (state["task"]["status"] == "running"
                and len(state["provider_requests"]) == 1
                and state["provider_requests"][0]["status"] == "reserved"
                and active):
            print(json.dumps({**state, "observed_provider_request_lock_wait": active}, ensure_ascii=False))
            return
        time.sleep(0.05)
    raise TimeoutError("没有观察到running任务在真实ProviderRequest行锁上等待")


def wait_cancel_requested(args):
    database_url = _database_url(args)
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        state = _snapshot(database_url, args.task_id)
        if state["task"]["cancel_requested"] and state["task"]["status"] == "cancelled":
            print(json.dumps(state, ensure_ascii=False))
            return
        time.sleep(0.05)
    raise TimeoutError("没有观察到业务取消在数据库提交")


def lock_request(args):
    engine = create_engine(_database_url(args), pool_pre_ping=True)
    connection = engine.connect()
    transaction = connection.begin()
    blocker_pid = connection.scalar(text("SELECT pg_backend_pid()"))
    connection.execute(select(ProviderRequest).where(
        ProviderRequest.id == args.request_id,
    ).with_for_update()).one()
    print(json.dumps({
        "blocker_pid": blocker_pid,
        "request_id": args.request_id,
        "lock_observed": True,
    }), flush=True)
    command = sys.stdin.readline().strip()
    if command != "release":
        raise ValueError("行锁进程收到未知命令")
    transaction.commit()
    connection.close()
    engine.dispose()
    print(json.dumps({"request_id": args.request_id, "released": True}), flush=True)


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)

    setup_parser = commands.add_parser("prepare")
    setup_parser.add_argument("--inputs", required=True)
    setup_parser.add_argument("--run-id", required=True)
    setup_parser.add_argument("--base-url", required=True)
    setup_parser.add_argument("--original-root", required=True)
    setup_parser.add_argument("--worktree-root", required=True)
    setup_parser.add_argument("--evidence-dir", required=True)
    setup_parser.add_argument("--config-dir", required=True)
    setup_parser.set_defaults(handler=prepare)

    request_parser = commands.add_parser("prepare-request")
    request_parser.add_argument("--database-url")
    request_parser.add_argument("--original-root", required=True)
    request_parser.add_argument("--task-id", required=True)
    request_parser.set_defaults(handler=build_request)

    state_parser = commands.add_parser("state")
    state_parser.add_argument("--database-url")
    state_parser.add_argument("--task-id", required=True)
    state_parser.add_argument("--application-name")
    state_parser.add_argument("--blocker-pid", type=int)
    state_parser.set_defaults(handler=lambda args: print(json.dumps(
        _snapshot(_database_url(args), args.task_id, args.application_name, args.blocker_pid),
        ensure_ascii=False,
    )))

    running_parser = commands.add_parser("wait-running")
    running_parser.add_argument("--database-url")
    running_parser.add_argument("--task-id", required=True)
    running_parser.add_argument("--application-name", required=True)
    running_parser.add_argument("--blocker-pid", required=True, type=int)
    running_parser.add_argument("--timeout", type=float, default=30)
    running_parser.set_defaults(handler=wait_running)

    cancelled_parser = commands.add_parser("wait-cancelled")
    cancelled_parser.add_argument("--database-url")
    cancelled_parser.add_argument("--task-id", required=True)
    cancelled_parser.add_argument("--timeout", type=float, default=30)
    cancelled_parser.set_defaults(handler=wait_cancel_requested)

    lock_parser = commands.add_parser("lock-request")
    lock_parser.add_argument("--database-url")
    lock_parser.add_argument("--request-id", required=True)
    lock_parser.set_defaults(handler=lock_request)

    args = parser.parse_args()
    args.handler(args)


if __name__ == "__main__":
    main()
