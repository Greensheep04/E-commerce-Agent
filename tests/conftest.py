import os
from pathlib import Path
from uuid import uuid4

import pytest
from datasets import Image, load_dataset
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from shop import (  # noqa: F401
    assessment_models,
    customer_models,
    customer_runtime_models,
    integration_models,
    runtime_models,
)
from shop.fashion import REVISION, SOURCE_ID
from shop.models import Base

pytest_plugins = ["tests.assessment_fixtures"]

os.environ.setdefault("SHOP_SESSION_SECRET", "test-session-secret")


@pytest.fixture(scope="session")
def source_row():
    dataset = load_dataset(SOURCE_ID, revision=REVISION, split="train", streaming=True)
    return next(iter(dataset.cast_column("image", Image(decode=False))))


@pytest.fixture
def db_session():
    database_url = os.environ["SHOP_TEST_DATABASE_URL"]
    assert database_url.rsplit("/", 1)[-1] != "shop"
    schema = f"test_{uuid4().hex}"
    admin_engine = create_engine(database_url)
    with admin_engine.begin() as connection:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public"))
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema},public"})
    Base.metadata.create_all(engine)
    try:
        with sessionmaker(engine, expire_on_commit=False)() as session:
            session.info["test_database_url"] = str(engine.url.set(query={"options": f"-csearch_path={schema},public"}))
            yield session
    finally:
        engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin_engine.dispose()


@pytest.fixture
def data_dir(tmp_path: Path):
    return tmp_path / "data"
