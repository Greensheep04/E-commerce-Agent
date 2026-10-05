import asyncio
import hashlib
from contextlib import asynccontextmanager

from sqlalchemy import Engine, text


@asynccontextmanager
async def database_lock(database: Engine, name: str):
    key = int.from_bytes(hashlib.sha256(name.encode()).digest()[:8], "big", signed=True)
    with database.connect() as connection:
        acquired = False
        try:
            while not acquired:
                acquired = connection.execute(text("SELECT pg_try_advisory_lock(:key)"),
                                               {"key": key}).scalar_one()
                connection.commit()
                if not acquired:
                    await asyncio.sleep(0.05)
            yield
        finally:
            if acquired:
                connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
                connection.commit()
