from sqlalchemy import engine_from_config, pool

import shop.assessment_models
import shop.cluster_models
import shop.customer_models
import shop.customer_runtime_models
import shop.data_center_models
import shop.integration_models  # noqa: F401
from alembic import context
from shop.config import Settings
from shop.runtime_models import Base

config = context.config
config.set_main_option("sqlalchemy.url", Settings().database_url.replace("%", "%%"))
target_metadata = Base.metadata


def include_name(name, type_, parent_names):
    external_tables = {
        "checkpoints", "checkpoint_blobs", "checkpoint_writes", "checkpoint_migrations"
    }
    return type_ != "table" or name not in external_tables


def run_migrations_offline():
    context.configure(url=Settings().database_url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    engine = engine_from_config(config.get_section(config.config_ini_section), prefix="sqlalchemy.", poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata, include_name=include_name
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
