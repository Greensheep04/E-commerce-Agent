import hashlib
import json
from copy import deepcopy

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from shop.config import Settings
from shop.customer_agent import CustomerServiceAgent
from shop.customer_runtime import CustomerService
from shop.customer_runtime_models import CustomerConversation, CustomerTurn
from shop.models import User
from shop.runtime_models import ProviderRequest

REAL_TURN_ID = "e5a6c962-a0af-4c8c-b242-3fb2505e49d7"
REAL_PROVIDER_REQUEST_ID = "2e4064c3-b5e4-4d59-8d90-f77db3ddec18"


def _column_values(record):
    return {column.name: deepcopy(getattr(record, column.name))
            for column in record.__table__.columns}


def test_real_history_tools_restore_and_survive_service_rebuild(db_session):
    source_settings = Settings()
    source_engine = create_engine(source_settings.database_url, pool_pre_ping=True)
    try:
        with Session(source_engine) as source_session:
            source_turn = source_session.get(CustomerTurn, REAL_TURN_ID)
            assert source_turn is not None
            assert "tool_definitions" not in source_turn.config
            source_conversation = source_session.get(
                CustomerConversation, source_turn.conversation_id)
            source_user = source_session.get(User, source_conversation.owner_id)
            earliest_request = source_session.scalar(
                select(ProviderRequest).where(
                    ProviderRequest.run_id == source_turn.id,
                    ProviderRequest.step_key.like("chat-%"),
                ).order_by(ProviderRequest.created_at, ProviderRequest.id).limit(1))
            assert earliest_request is not None
            assert earliest_request.id == REAL_PROVIDER_REQUEST_ID
            assert earliest_request.step_key == "chat-0"
            assert earliest_request.status == "completed"
            assert earliest_request.task_id is None
            assert earliest_request.assessment_id is None
            original_tools = deepcopy(earliest_request.request_body["tools"])
            assert original_tools
            original_turn_config = deepcopy(source_turn.config)
            original_turn_request_id = source_turn.request_id
            original_request_body = deepcopy(earliest_request.request_body)
            original_request_state = {
                "id": earliest_request.id,
                "run_id": earliest_request.run_id,
                "step_key": earliest_request.step_key,
                "status": earliest_request.status,
                "reservation_cny": earliest_request.reservation_cny,
                "charged_cny": earliest_request.charged_cny,
                "price_basis": deepcopy(earliest_request.price_basis),
                "usage": deepcopy(earliest_request.usage),
            }
            copied_rows = [
                _column_values(source_user),
                _column_values(source_conversation),
                _column_values(source_turn),
                _column_values(earliest_request),
            ]
            source_identity = {
                "user_id": source_user.id,
                "conversation_id": source_conversation.id,
                "turn_id": source_turn.id,
                "turn_request_id": source_turn.request_id,
                "provider_request_id": earliest_request.id,
                "provider_run_id": earliest_request.run_id,
                "provider_step_key": earliest_request.step_key,
                "request_body_sha256": hashlib.sha256(json.dumps(
                    original_request_body, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":")).encode()).hexdigest(),
                "tool_names": [tool["function"]["name"] for tool in original_tools],
                "reservation_cny": str(earliest_request.reservation_cny),
                "charged_cny": str(earliest_request.charged_cny),
            }
    finally:
        source_engine.dispose()

    db_session.add(User(**copied_rows[0]))
    db_session.flush()
    db_session.add(CustomerConversation(**copied_rows[1]))
    db_session.flush()
    db_session.add(CustomerTurn(**copied_rows[2]))
    db_session.add(ProviderRequest(**copied_rows[3]))
    db_session.commit()

    isolated_settings = Settings(database_url=db_session.info["test_database_url"])
    first_service = CustomerService(isolated_settings)
    second_service = CustomerService(isolated_settings)
    try:
        returned_tools = CustomerServiceAgent(first_service, REAL_TURN_ID).tool_definitions()
        assert returned_tools == original_tools

        returned_tools[0]["function"]["name"] = "changed-by-caller"
        returned_tools.append({"changed-by-caller": True})

        db_session.expire_all()
        stored_turn = db_session.get(CustomerTurn, REAL_TURN_ID)
        stored_request = db_session.get(ProviderRequest, REAL_PROVIDER_REQUEST_ID)
        assert stored_turn.config == {
            **original_turn_config,
            "tool_definitions": original_tools,
        }
        assert stored_turn.request_id == original_turn_request_id
        assert stored_request.request_body == original_request_body
        assert {
            "id": stored_request.id,
            "run_id": stored_request.run_id,
            "step_key": stored_request.step_key,
            "status": stored_request.status,
            "reservation_cny": stored_request.reservation_cny,
            "charged_cny": stored_request.charged_cny,
            "price_basis": stored_request.price_basis,
            "usage": stored_request.usage,
        } == original_request_state

        rebuilt_tools = CustomerServiceAgent(second_service, REAL_TURN_ID).tool_definitions()
        assert rebuilt_tools == original_tools
        db_session.expire_all()
        assert db_session.get(CustomerTurn, REAL_TURN_ID).config["tool_definitions"] == original_tools
        assert db_session.get(ProviderRequest, REAL_PROVIDER_REQUEST_ID).request_body == original_request_body
    finally:
        first_service.engine.dispose()
        second_service.engine.dispose()

    assert source_identity["turn_id"] == REAL_TURN_ID
