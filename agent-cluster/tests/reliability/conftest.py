import hashlib
import json
import os
import re
from pathlib import Path
from urllib.request import ProxyHandler, build_opener
from uuid import uuid4

import pytest
from sqlalchemy import (
    MetaData,
    Table,
    Text,
    cast,
    create_engine,
    func,
    or_,
    select,
    text,
)
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateSchema

ROOT = Path(__file__).resolve().parents[3]
INPUTS_PATH = ROOT / ".scratch/evidence/clu-inputs-20261002T164037Z-5958ff50/inputs.json"
SOURCES_PATH = ROOT / ".scratch/evidence/clu-inputs-20261002T164037Z-5958ff50/sources.json"
DATABASE_URL = "postgresql+psycopg://shop@127.0.0.1:55432/shop_test"
MAIN_DATABASE_URL = "postgresql+psycopg://shop@127.0.0.1:55432/shop"


def save_observation(name, value):
    directory = Path(os.environ["CLU05_EVIDENCE_DIR"])
    (directory / name).write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def integration_call_snapshot(request_ids):
    if not request_ids:
        return []
    from shop.integration_models import IntegrationCall

    engine = create_engine(MAIN_DATABASE_URL, execution_options={"postgresql_readonly": True})
    try:
        with Session(engine) as session:
            rows = session.execute(select(
                IntegrationCall.request_id,
                IntegrationCall.module_id,
                IntegrationCall.operation,
                IntegrationCall.resource_id,
            ).where(IntegrationCall.request_id.in_(request_ids))).all()
        return [dict(row._mapping) for row in rows]
    finally:
        engine.dispose()


def provider_request_snapshot(run_ids, goal_ids):
    from shop.runtime_models import ProviderRequest

    filters = []
    if run_ids:
        filters.append(ProviderRequest.price_basis["cluster_run_id"].astext.in_(run_ids))
    if goal_ids:
        filters.append(ProviderRequest.price_basis["cluster_goal_id"].astext.in_(goal_ids))
    if not filters:
        return []
    engine = create_engine(MAIN_DATABASE_URL, execution_options={"postgresql_readonly": True})
    try:
        with Session(engine) as session:
            rows = session.execute(select(
                ProviderRequest.id,
                ProviderRequest.status,
                ProviderRequest.service,
                ProviderRequest.model,
            ).where(or_(*filters))).all()
        return [dict(row._mapping) for row in rows]
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def fixed_inputs():
    return json.loads(INPUTS_PATH.read_text())


@pytest.fixture(scope="session")
def accounts(fixed_inputs):
    return fixed_inputs["accounts"]


@pytest.fixture(scope="session")
def fixed_sources():
    return json.loads(SOURCES_PATH.read_text())


@pytest.fixture(scope="session")
def database():
    from agent_cluster.store import ClusterStore

    schema = os.environ.get("CLU05_TEST_SCHEMA", "clu05_" + uuid4().hex)
    if re.fullmatch(r"clu05_[a-f0-9]{32}", schema) is None:
        raise ValueError("CLU05_TEST_SCHEMA格式无效")
    admin_engine = create_engine(DATABASE_URL)
    with admin_engine.begin() as connection:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public"))
        connection.execute(CreateSchema(schema))
    admin_engine.dispose()
    engine = create_engine(DATABASE_URL, connect_args={"options": f"-csearch_path={schema},public"})
    import shop.cluster_models  # noqa: F401
    from shop import (  # noqa: F401
        assessment_models,
        customer_models,
        customer_runtime_models,
        integration_models,
        runtime_models,
    )
    from shop.models import Base as ShopBase

    ShopBase.metadata.create_all(engine)
    store = ClusterStore(engine)
    store.initialize()
    yield {"database_url": DATABASE_URL, "engine": engine, "schema": schema, "store": store}
    engine.dispose()


@pytest.fixture(scope="session")
def cluster_config():
    path = Path(os.environ["CLU05_CLUSTER_CONFIG"]).resolve(strict=True)
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("autorun") is not False:
        raise ValueError("真实stdio检查要求autorun=false")
    if not isinstance(document.get("actors"), dict):
        raise TypeError("集群配置缺少actors对象")
    hub_base_url = os.environ["CLU05_HUB_BASE_URL"].rstrip("/")
    hub_configs = []
    for command in document["actors"].values():
        arguments = command["args"]
        config_positions = [index for index, value in enumerate(arguments) if value == "--config"]
        if len(config_positions) != 1 or config_positions[0] + 1 >= len(arguments):
            raise ValueError("真实stdio命令必须提供唯一Hub配置文件")
        config_path = Path(arguments[config_positions[0] + 1]).resolve(strict=True)
        hub_document = json.loads(config_path.read_text(encoding="utf-8"))
        connectors = hub_document["connectors"]
        connector_urls = {connector["base_url"].rstrip("/") for connector in connectors}
        if not connectors or connector_urls != {hub_base_url}:
            raise ValueError("Hub connector地址与真实检查地址不一致")
        hub_configs.append({
            "name": config_path.name,
            "sha256": hashlib.sha256(config_path.read_bytes()).hexdigest(),
            "connector_count": len(connectors),
            "connector_ids": sorted(connector["connector_id"] for connector in connectors),
            "base_urls": sorted(connector_urls),
        })
    opener = build_opener(ProxyHandler({}))
    with opener.open(hub_base_url + "/health", timeout=10) as response:
        health_status = response.status
        health_body = json.loads(response.read())
    if health_status != 200 or health_body != {"status": "ok"}:
        raise RuntimeError("真实业务服务健康检查未通过")
    summary = {
        "cluster_config": {
            "name": path.name,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "autorun": document["autorun"],
            "actor_count": len(document["actors"]),
        },
        "hub_base_url": hub_base_url,
        "hub_configs": hub_configs,
        "service_health": {
            "url": hub_base_url + "/health",
            "status_code": health_status,
            "body": health_body,
        },
    }
    save_observation("configuration-and-health.json", summary)
    return {
        "path": str(path),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "actors": sorted(document["actors"]),
        "hub_base_url": hub_base_url,
        "hub_configs": hub_configs,
        "service_health": summary["service_health"],
    }


@pytest.fixture(scope="session")
def real_deepseek_response():
    from agent_cluster.store import digest

    engine = create_engine(MAIN_DATABASE_URL, execution_options={"postgresql_readonly": True})
    try:
        with engine.connect() as connection, connection.begin():
            provider_requests = Table("provider_requests", MetaData(), autoload_with=connection)
            candidates = connection.execute(select(
                provider_requests.c.id,
                provider_requests.c.service,
                provider_requests.c.model,
                provider_requests.c.status,
                provider_requests.c.created_at,
                provider_requests.c.response_body,
            ).where(
                provider_requests.c.service == "deepseek",
                provider_requests.c.model == "deepseek-flash",
                provider_requests.c.status == "completed",
                provider_requests.c.response_body.is_not(None),
            ).order_by(
                func.length(cast(provider_requests.c.response_body, Text)),
                provider_requests.c.created_at,
                provider_requests.c.id,
            ).limit(200)).mappings().all()
            candidate = next((row for row in candidates
                              if isinstance(row["response_body"], dict)
                              and isinstance(row["response_body"].get("choices"), list)
                              and row["response_body"]["choices"]
                              and isinstance(row["response_body"]["choices"][0].get("message"), dict)), None)
            if candidate is None:
                raise LookupError("shop主库没有可复用的已完成DeepSeek响应")
            request = connection.scalar(select(provider_requests.c.request_body).where(
                provider_requests.c.id == candidate["id"]))
    finally:
        engine.dispose()
    response = candidate["response_body"]
    source = {
        "provider_request_id": candidate["id"],
        "service": candidate["service"],
        "model": candidate["model"],
        "status": candidate["status"],
        "created_at": candidate["created_at"].isoformat(),
        "request_sha256": digest(request),
        "response_sha256": digest(response),
        "response_body_saved_in_evidence": False,
    }
    evidence_dir = Path(os.environ["CLU05_EVIDENCE_DIR"])
    (evidence_dir / "provider_response_source.json").write_text(
        json.dumps(source, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return {"request": request, "response": response, "source": source}
