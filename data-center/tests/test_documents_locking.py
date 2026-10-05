import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from data_center.documents.locking import database_lock


@pytest.mark.asyncio
async def test_real_postgresql_lock_serializes_requests_and_releases_after_cancellation():
    database = create_engine("postgresql+psycopg://data_center_writer@127.0.0.1:55432/shop_data_center")
    key = "dc03-lock-test:" + uuid4().hex
    entered = asyncio.Event()

    async def contender():
        async with database_lock(database, key):
            entered.set()
            with database.connect() as connection:
                assert connection.scalar(text("SELECT current_database()")) == "shop_data_center"

    async with database_lock(database, key):
        waiting = asyncio.create_task(contender())
        await asyncio.sleep(0.15)
        assert not entered.is_set()
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
    await asyncio.wait_for(contender(), timeout=2)
    assert entered.is_set()
    database.dispose()
