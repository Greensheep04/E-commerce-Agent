import json
from hashlib import sha256

from integration_protocol import CallContext, ResourceRef
from sqlalchemy import select, text

from shop.integration_models import IntegrationCall


class IntegrationStore:
    def __init__(self, maker):
        self.maker = maker

    def check_available(self):
        with self.maker() as session:
            session.execute(select(IntegrationCall).limit(0))

    def bind(self, module_id, operation, payload, context: CallContext, create):
        identity = (context.actor_id, module_id, operation, context.request_id)
        upstream = [item.model_dump(mode="json") for item in context.upstream]
        content = json.dumps({"payload": payload, "upstream": upstream},
                             ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        digest = sha256(content.encode()).hexdigest()
        with self.maker.begin() as session:
            session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                            {"key": json.dumps(identity)})
            existing = session.get(IntegrationCall, identity)
            if existing is not None:
                if existing.payload_sha256 != digest:
                    raise ValueError("相同请求编号的内容不一致")
                return ResourceRef(module_id=module_id, resource_type=existing.resource_type,
                                   resource_id=existing.resource_id)
            reference = create(session)
            if reference.module_id != module_id:
                raise ValueError("请求记录与业务资源所属模块不一致")
            session.add(IntegrationCall(
                actor_id=context.actor_id, module_id=module_id, operation=operation,
                request_id=context.request_id, payload_sha256=digest,
                resource_type=reference.resource_type, resource_id=reference.resource_id,
                upstream=upstream,
            ))
            session.flush()
            return reference
