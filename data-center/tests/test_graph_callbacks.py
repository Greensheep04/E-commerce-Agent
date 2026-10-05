import asyncio
import json
from pathlib import Path

import pytest
from data_center_protocol import ModelReply
from lightrag.utils import create_prefixed_exception
from sqlalchemy import create_engine

from data_center.documents.engine import DocumentEngine
from data_center.documents.gateway import ModelStepPending
from data_center.graph.engine import GraphEngine
from data_center.ports import ShopPorts


@pytest.mark.asyncio
async def test_real_historical_unknown_survives_lightrag_exception_wrapper():
    evidence = json.loads(Path(".scratch/evidence/dc02-host-20261004-r1/historical-unknown.json").read_text())
    assert evidence["origin"] == "historical_provider_request"
    reply = ModelReply.model_validate_json(json.dumps(evidence["reply"]))
    assert reply.status == "unknown"
    pending = ModelStepPending(reply)
    wrapped = create_prefixed_exception(pending, "C[1/2]")
    assert isinstance(wrapped, RuntimeError)
    assert not isinstance(wrapped, ModelStepPending)
    ports = ShopPorts("http://127.0.0.1:8002")
    database = create_engine("postgresql+psycopg://data_center_writer@127.0.0.1:55432/shop_data_center")
    graph = GraphEngine(DocumentEngine(database, Path(".local/data-center"), ports, ports, ports), None)
    graph.callback_error = pending
    operation = asyncio.get_running_loop().create_future()
    operation.set_exception(wrapped)
    try:
        with pytest.raises(ModelStepPending) as observed:
            await graph._sdk_result(operation)
        assert observed.value is pending
        assert observed.value.reply.request_id == reply.request_id
        assert observed.value.reply.reserved_cny == reply.reserved_cny
    finally:
        await ports.close()
        database.dispose()


@pytest.mark.asyncio
async def test_callback_latches_first_failure_and_stops_remaining_dispatch():
    evidence = json.loads(Path(".scratch/evidence/dc02-host-20261004-r1/historical-unknown.json").read_text())
    pending = ModelStepPending(ModelReply.model_validate_json(json.dumps(evidence["reply"])))
    ports = ShopPorts("http://127.0.0.1:8002")
    database = create_engine("postgresql+psycopg://data_center_writer@127.0.0.1:55432/shop_data_center")
    graph = GraphEngine(DocumentEngine(database, Path(".local/data-center"), ports, ports, ports), None)
    try:
        graph._capture_callback_error(pending)
        graph._capture_callback_error(asyncio.CancelledError())
        with pytest.raises(ModelStepPending) as observed:
            graph._callback_active()
        assert observed.value is pending
    finally:
        await ports.close()
        database.dispose()
