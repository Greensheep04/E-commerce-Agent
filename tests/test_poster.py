import json
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest
from poster_framework import (
    AgentRole,
    ArtifactPurpose,
    ArtifactReference,
    ExecutionIdentity,
    ExecutionMode,
    FileSecurityError,
)
from sqlalchemy import create_engine, func, select
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Session

from shop.config import Settings
from shop.materials import MaterialRepository
from shop.models import Asset, AssetVersion, Source, User
from shop.poster import PosterService
from shop.poster_types import PosterJob
from shop.runtime_models import HarnessTask, ProviderRequest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FROZEN_PATH = PROJECT_ROOT / ".scratch/evidence/sm01-poster-evaluation-20260930T2/frozen.json"
MAIN_DATA_DIR = Path("/Users/greensheep/Project/project-shop/.local/data")
ADMIN_ID = "00000000-0000-4000-8000-000000000001"
DIRECT_KEYWORDS = ["真实素材", "直接选取"]


@pytest.fixture(scope="session")
def frozen_evaluation():
    return json.loads(FROZEN_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def main_records(frozen_evaluation):
    settings = Settings(data_dir=MAIN_DATA_DIR)
    engine = create_engine(settings.database_url)
    task_records = {}
    material_records = {}
    with Session(engine) as session:
        for case in (
            *frozen_evaluation["positive_cases"],
            *frozen_evaluation["negative_cases"]["insufficient"],
            *frozen_evaluation["negative_cases"]["incomplete"],
        ):
            task_id = case["task_id"]
            task = session.get(HarnessTask, task_id)
            assert task is not None
            snapshot = case["task_snapshot"]
            assert task.prompt == snapshot["prompt"]
            assert task.status == snapshot["status"]
            assert task.result == snapshot["result"]
            task_records[task_id] = {
                item.key: deepcopy(getattr(task, item.key))
                for item in inspect(HarnessTask).column_attrs
            }
        source_repository = MaterialRepository(session, settings.data_dir)
        for case in frozen_evaluation["positive_cases"]:
            for material in case["materials"]:
                version_id = material["version_id"]
                if version_id in material_records:
                    continue
                version = session.get(AssetVersion, version_id)
                assert version is not None
                asset = session.get(Asset, version.asset_id)
                assert asset is not None
                source = session.get(Source, (asset.source_id, asset.source_revision))
                assert source is not None
                content = source_repository.read_file(version)
                assert sha256(content).hexdigest() == material["file_sha256"]
                material_records[version_id] = {
                    "asset": {
                        item.key: deepcopy(getattr(asset, item.key))
                        for item in inspect(Asset).column_attrs
                    },
                    "version": {
                        item.key: deepcopy(getattr(version, item.key))
                        for item in inspect(AssetVersion).column_attrs
                    },
                    "source": {
                        item.key: deepcopy(getattr(source, item.key))
                        for item in inspect(Source).column_attrs
                    },
                    "content": content,
                }
    engine.dispose()
    return {"tasks": task_records, "materials": material_records}


@pytest.fixture
def poster_environment(db_session, data_dir, frozen_evaluation, main_records):
    reader_id = str(uuid4())
    owner_ids = {
        material["asset"]["owner_id"]
        for material in main_records["materials"].values()
    }
    db_session.add_all(
        [
            User(
                id=ADMIN_ID,
                username=f"poster-admin-{uuid4().hex}",
                password_hash="test-only-not-a-credential",
                role="admin",
            ),
            User(
                id=reader_id,
                username=f"poster-reader-{uuid4().hex}",
                password_hash="test-only-not-a-credential",
                role="user",
            ),
            *[
                User(
                    id=owner_id,
                    username=f"poster-material-owner-{owner_id}",
                    password_hash="test-only-not-a-credential",
                    role="user",
                )
                for owner_id in owner_ids
                if owner_id != ADMIN_ID
            ],
        ]
    )
    db_session.flush()

    seen_sources = set()
    for material in main_records["materials"].values():
        source_data = material["source"]
        source_key = (source_data["source_id"], source_data["revision"])
        if source_key not in seen_sources:
            db_session.add(Source(**source_data))
            db_session.flush()
            seen_sources.add(source_key)
        db_session.add(Asset(**material["asset"]))
        db_session.flush()
        db_session.add(AssetVersion(**material["version"]))
        target = (data_dir / material["version"]["file_path"]).resolve()
        assert target.is_relative_to(data_dir.resolve())
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(material["content"])

    for task_data in main_records["tasks"].values():
        db_session.add(HarnessTask(**task_data))
    db_session.commit()

    settings = Settings(
        database_url=db_session.info["test_database_url"],
        data_dir=data_dir,
        poster_dir=data_dir / "poster",
        session_secret="poster-test-session-secret",
    )
    service = PosterService(settings, registrations=())
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0
    yield {
        "service": service,
        "session": db_session,
        "settings": settings,
        "admin_id": ADMIN_ID,
        "reader_id": reader_id,
        "frozen": frozen_evaluation,
        "records": main_records,
    }
    assert db_session.scalar(select(func.count()).select_from(ProviderRequest)) == 0
    service.engine.dispose()


def positive_case(environment, case_id="HC-01"):
    return next(
        case for case in environment["frozen"]["positive_cases"]
        if case["case_id"] == case_id
    )


def change_harness_result(environment, task_id, change):
    task = environment["session"].get(HarnessTask, task_id)
    task.result = deepcopy(task.result)
    change(task.result)
    environment["session"].commit()


def create_direct_job(environment, version_id=None):
    material_id = version_id or next(iter(environment["records"]["materials"]))
    return environment["service"].prepare_direct(
        keywords=DIRECT_KEYWORDS,
        version_ids=[material_id],
        actor_id=environment["admin_id"],
    )


def test_read_harness_preserves_real_input_result_and_pinned_material_versions(
    poster_environment,
):
    environment = poster_environment
    case = positive_case(environment)
    first_material = case["materials"][0]
    original_version = environment["session"].get(AssetVersion, first_material["version_id"])
    later_version = AssetVersion(
        id=str(uuid4()),
        asset_id=original_version.asset_id,
        version=original_version.version + 1,
        file_path=original_version.file_path,
        file_sha256=original_version.file_sha256,
        file_size=original_version.file_size,
        filename=original_version.filename,
        mime_type=original_version.mime_type,
        original_fields={**original_version.original_fields, "test_revision": "newer"},
        title=f"{original_version.title} newer",
        tags=list(original_version.tags),
        owner_id=original_version.owner_id,
    )
    environment["session"].add(later_version)
    environment["session"].commit()

    result = environment["service"].read_harness(case["task_id"], environment["admin_id"])

    task = case["task_snapshot"]
    assert result.original_prompt == task["prompt"]
    assert result.harness_result == task["result"]
    assert {material.version_id for material in result.materials} == {
        item["version_id"] for item in case["materials"]
    }
    assert all(material.version_id != later_version.id for material in result.materials)
    for material in result.materials:
        stored = environment["records"]["materials"][material.version_id]
        assert material.asset_id == stored["asset"]["id"]
        assert material.source_revision == stored["asset"]["source_revision"]
        assert material.file_sha256 == stored["version"]["file_sha256"]


@pytest.mark.parametrize(
    "case_id",
    ("HC-13", "HC-17", "HC-18", "HC-19", "HC-20", "HC-05"),
)
def test_read_harness_rejects_insufficient_and_incomplete_cases(poster_environment, case_id):
    environment = poster_environment
    cases = (
        environment["frozen"]["negative_cases"]["insufficient"]
        + environment["frozen"]["negative_cases"]["incomplete"]
    )
    case = next(item for item in cases if item["case_id"] == case_id)
    with pytest.raises(ValueError):
        environment["service"].read_harness(case["task_id"], environment["admin_id"])


def test_read_harness_rejects_unapproved_review(poster_environment):
    environment = poster_environment
    case = positive_case(environment)
    change_harness_result(
        environment,
        case["task_id"],
        lambda result: result["review"].update(approved=False),
    )
    with pytest.raises(ValueError):
        environment["service"].read_harness(case["task_id"], environment["admin_id"])


@pytest.mark.parametrize("field", ("asset_id", "source_revision", "original_fields", "file_sha256"))
def test_read_harness_rejects_material_source_conflicts(poster_environment, field):
    environment = poster_environment
    case = positive_case(environment)

    def conflict(result):
        material = result["recommendations"][0]
        if field == "asset_id":
            material[field] = str(uuid4())
        elif field == "source_revision":
            material[field] = "source-revision-conflict"
        elif field == "original_fields":
            material[field]["gender"] = "Women"
        else:
            material[field] = "0" * 64

    change_harness_result(environment, case["task_id"], conflict)
    with pytest.raises(ValueError):
        environment["service"].read_harness(case["task_id"], environment["admin_id"])


def test_read_harness_rejects_an_actor_without_access(poster_environment):
    environment = poster_environment
    case = positive_case(environment)
    with pytest.raises(PermissionError):
        environment["service"].read_harness(case["task_id"], environment["reader_id"])


def test_read_harness_rejects_unknown_actor(poster_environment):
    environment = poster_environment
    case = positive_case(environment)
    with pytest.raises(PermissionError):
        environment["service"].read_harness(case["task_id"], str(uuid4()))


@pytest.mark.parametrize("damage", ("missing", "corrupt"))
def test_read_harness_rejects_missing_or_damaged_source_files(poster_environment, damage):
    environment = poster_environment
    case = positive_case(environment)
    material = environment["records"]["materials"][case["materials"][0]["version_id"]]
    file_path = environment["settings"].data_dir / material["version"]["file_path"]
    if damage == "missing":
        file_path.unlink()
    else:
        file_path.write_bytes(b"corrupt test copy")
    with pytest.raises((OSError, ValueError)):
        environment["service"].read_harness(case["task_id"], environment["admin_id"])


def test_prepare_direct_uses_one_real_material_without_model_requests(poster_environment):
    environment = poster_environment
    version_id = next(iter(environment["records"]["materials"]))
    job = create_direct_job(environment, version_id)
    assert isinstance(job, PosterJob)
    assert job.direct_keywords.keywords == tuple(DIRECT_KEYWORDS)
    assert [material.version_id for material in job.input_data.materials] == [version_id]
    assert environment["session"].scalar(select(func.count()).select_from(ProviderRequest)) == 0


def test_prepare_direct_rejects_a_missing_material_version(poster_environment):
    environment = poster_environment
    with pytest.raises(ValueError, match="版本不存在"):
        environment["service"].prepare_direct(
            keywords=DIRECT_KEYWORDS,
            version_ids=[str(uuid4())],
            actor_id=environment["admin_id"],
        )


def test_prepare_direct_rejects_a_material_without_access(poster_environment):
    environment = poster_environment
    version_id = next(iter(environment["records"]["materials"]))
    with pytest.raises(PermissionError):
        environment["service"].prepare_direct(
            keywords=DIRECT_KEYWORDS,
            version_ids=[version_id],
            actor_id=environment["reader_id"],
        )


def test_load_job_rejects_a_damaged_input_digest(poster_environment):
    environment = poster_environment
    job = create_direct_job(environment)
    path = environment["service"].job_path(job.job_id)
    saved = json.loads(path.read_text(encoding="utf-8"))
    saved["input_sha256"] = "0" * 64
    path.write_text(json.dumps(saved, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ValueError, match="摘要不一致"):
        environment["service"].load_job(job.job_id, environment["admin_id"])


def test_missing_roles_persist_waiting_state_without_model_requests(poster_environment):
    environment = poster_environment
    roles = environment["service"].status()["roles"]
    assert len(roles) == 4
    assert set(roles.values()) == {"待接入"}
    job = create_direct_job(environment)
    with pytest.raises(ValueError, match="待接入"):
        environment["service"].run(job.job_id, environment["admin_id"])
    saved = environment["service"].load_job(job.job_id, environment["admin_id"])
    assert saved.status == "waiting_agents"
    assert environment["session"].scalar(select(func.count()).select_from(ProviderRequest)) == 0


def test_run_session_saves_and_restores_real_source_files(poster_environment):
    environment = poster_environment
    job = create_direct_job(environment)
    session = environment["service"].new_session(job)
    request = session.original_request
    shop_input = next(item.value for item in request.constraints if item.name == "shop_input")
    assert shop_input == job.input_data.model_dump(mode="json")
    references = session.file_store.artifacts
    assert len(references) == 1
    reference = references[0]
    assert reference.purpose == ArtifactPurpose.asset
    material = job.input_data.materials[0]
    source = environment["records"]["materials"][material.version_id]
    assert reference.uri == f"runs/{session.run_id}/sources/{material.version_id}.image"
    source_materials = session.configuration["source_materials"]
    assert any(item["version_id"] == material.version_id for item in source_materials)
    assert reference.sha256 == source["version"]["file_sha256"]
    assert sha256(session.file_store.read(reference)).hexdigest() == material.file_sha256

    saved_job = environment["service"].load_job(job.job_id, environment["admin_id"])
    assert session.run_id in saved_job.frame_records
    restored = environment["service"].restore(
        job.job_id,
        session.run_id,
        environment["admin_id"],
    )
    assert restored.original_request == session.original_request
    restored_reference = restored.file_store.artifacts[0]
    assert restored_reference.sha256 == reference.sha256
    assert sha256(restored.file_store.read(restored_reference)).hexdigest() == material.file_sha256


def test_restore_rejects_a_damaged_saved_source_file(poster_environment):
    environment = poster_environment
    job = create_direct_job(environment)
    session = environment["service"].new_session(job)
    reference = session.file_store.artifacts[0]
    assert reference.uri.endswith(f"sources/{job.input_data.materials[0].version_id}.image")
    file_path = (
        environment["service"].frame_root(job)
        / session.run_id
        / "sources"
        / f"{job.input_data.materials[0].version_id}.image"
    )
    assert file_path.is_file()
    file_path.write_bytes(b"damaged saved frame file")
    with pytest.raises(ValueError):
        environment["service"].restore(
            job.job_id,
            session.run_id,
            environment["admin_id"],
        )


def test_restore_rejects_a_run_not_owned_by_the_job(poster_environment):
    environment = poster_environment
    job = create_direct_job(environment)
    session = environment["service"].new_session(job)
    assert session.run_id in environment["service"].load_job(job.job_id, environment["admin_id"]).frame_records
    with pytest.raises(PermissionError):
        environment["service"].restore(job.job_id, str(uuid4()), environment["admin_id"])


def test_file_store_rejects_a_reference_from_another_real_run(poster_environment):
    environment = poster_environment
    job = create_direct_job(environment)
    first_session = environment["service"].new_session(job)
    second_session = environment["service"].new_session(job)
    foreign_reference = second_session.file_store.artifacts[0]
    with pytest.raises(FileSecurityError):
        first_session.file_store.read(foreign_reference)


def test_source_file_reference_can_be_read_by_every_role(poster_environment):
    environment = poster_environment
    job = create_direct_job(environment)
    session = environment["service"].new_session(job)
    for role in AgentRole:
        context = session.create_context(ExecutionIdentity(
            run_id=session.run_id, role=role, invocation_id=f"read-{uuid4().hex}",
            execution_mode=ExecutionMode.real,
        ))
        artifacts = context.memory.context()["artifacts"]
        assert len(artifacts) == 1
        reference = ArtifactReference.model_validate(artifacts[0]["reference"])
        assert sha256(context.files.read(reference)).hexdigest() == job.input_data.materials[0].file_sha256


def test_actor_uuid_preserves_owner_access_after_preparation(poster_environment):
    environment = poster_environment
    version_id, material = next(iter(environment["records"]["materials"].items()))
    owner_id = material["version"]["owner_id"]
    job = environment["service"].prepare_direct(
        DIRECT_KEYWORDS, [version_id], owner_id.upper(),
    )
    assert environment["service"].load_job(job.job_id, owner_id).owner_id == owner_id


@pytest.mark.parametrize("constraints", ([], "", 0, False))
def test_direct_constraints_require_a_json_object(poster_environment, constraints):
    environment = poster_environment
    version_id = next(iter(environment["records"]["materials"]))
    with pytest.raises(ValueError):
        environment["service"].prepare_direct(
            DIRECT_KEYWORDS, [version_id], environment["admin_id"], constraints,
        )


@pytest.mark.parametrize("field", ("model_profile", "prompt_version"))
def test_saved_job_configuration_must_match_job_identity(poster_environment, field):
    environment = poster_environment
    job = create_direct_job(environment)
    path = environment["service"].job_path(job.job_id)
    record = json.loads(path.read_text(encoding="utf-8"))
    if field == "model_profile":
        record[field]["output_limit"] += 1
    else:
        record[field] = "poster-keywords-other"
    path.write_text(json.dumps(record), encoding="utf-8")
    with pytest.raises(ValueError):
        environment["service"].load_job(job.job_id, environment["admin_id"])


@pytest.mark.parametrize("field", ("model_profile", "prompt_version"))
def test_saved_run_configuration_must_match_job(poster_environment, field):
    environment = poster_environment
    job = create_direct_job(environment)
    session = environment["service"].new_session(job)
    path = environment["service"].record_path(job, session.run_id)
    record = json.loads(path.read_text(encoding="utf-8"))
    if field == "model_profile":
        record["configuration"][field]["output_limit"] += 1
    else:
        record["configuration"][field] = "poster-keywords-other"
    path.write_text(json.dumps(record), encoding="utf-8")
    with pytest.raises(ValueError):
        environment["service"].restore(job.job_id, session.run_id, environment["admin_id"])
