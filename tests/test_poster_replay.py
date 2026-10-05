import asyncio
import json
from copy import deepcopy
from pathlib import Path

from poster_framework import AgentRole, ExecutionIdentity, ExecutionMode, RunSession
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from shop.config import Settings
from shop.poster import PosterService
from shop.poster_types import KeywordPlan, PosterJob
from shop.runtime_models import ProviderRequest

PAID_REPORT = Path(".scratch/evidence/sm03-poster-paid-20261001T1/report.json")
MAIN_DATA_DIR = Path("/Users/greensheep/Project/project-shop/.local/data")
POSTER_DIR = Path(".local/poster")


def snapshot(service):
    with service.maker() as session:
        return [(item.id, item.status, item.charged_cny, item.reservation_cny)
                for item in session.scalars(select(ProviderRequest).order_by(ProviderRequest.id))]


def test_completed_keyword_response_restores_without_credentials(data_dir):
    report = json.loads(PAID_REPORT.read_text(encoding="utf-8"))
    job_id = report["records"][0]["job_id"]
    original = PosterJob.model_validate_json((POSTER_DIR / "jobs" / job_id / "job.json").read_text(encoding="utf-8"))
    service = PosterService(Settings(
        data_dir=MAIN_DATA_DIR, poster_dir=data_dir / "poster",
        provider_env_file=data_dir / "credentials-unavailable.env",
    ))
    assert not service.settings.provider_env_file.exists()
    interrupted = original.model_copy(update={
        "status": "preparing", "keyword_plan": None, "frame_records": (), "agent_records": (),
    })
    service.save_job(interrupted)
    before = snapshot(service)
    try:
        restored = service.prepare(original.input_data.parent_harness_task_id, original.owner_id)
        assert restored.keyword_plan == original.keyword_plan
        assert restored.job_id == original.job_id
        assert snapshot(service) == before
    finally:
        service.engine.dispose()


def test_completed_framework_response_reuses_without_credentials(db_session, data_dir):
    report = json.loads(PAID_REPORT.read_text(encoding="utf-8"))
    bridge = report["bridge"]
    path = POSTER_DIR / "jobs" / bridge["job_id"] / "framework" / "runs" / bridge["run_id"] / "run-record.json"
    frame = RunSession.load(path, root=data_dir / "framework")
    source_engine = create_engine(Settings().database_url)
    try:
        with Session(source_engine) as source:
            original = source.get(ProviderRequest, bridge["request_id"])
            assert original.status == "completed"
            copied = ProviderRequest(**{
                column.name: deepcopy(getattr(original, column.name))
                for column in ProviderRequest.__table__.columns
            })
            messages = original.request_body["messages"]
            response = original.response_body
        db_session.add(copied)
        db_session.commit()
        service = PosterService(Settings(
            database_url=db_session.info["test_database_url"],
            data_dir=data_dir, poster_dir=data_dir / "poster",
            provider_env_file=data_dir / "credentials-unavailable.env",
        ))
        context = frame.create_context(ExecutionIdentity(
            run_id=bridge["run_id"], role=AgentRole.creative_director,
            invocation_id=bridge["invocation_id"], execution_mode=ExecutionMode.real,
        ))
        before = snapshot(service)
        counts = frame.usage
        try:
            replayed = asyncio.run(service.model_client.call(context, messages, step_key="bridge-check"))
            assert replayed == response
            assert KeywordPlan.model_validate_json(replayed["choices"][0]["message"]["content"]).keywords
            assert snapshot(service) == before and frame.usage == counts
        finally:
            service.engine.dispose()
    finally:
        source_engine.dispose()
