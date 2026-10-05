from hashlib import sha256
from pathlib import Path

import httpx
from data_center_protocol import AuthorizedScope, DataSnapshot, ModelReply, SourcePage
from integration_protocol import CallContext
from pydantic import SecretStr

from data_center.config import Settings


class SourceUnavailable(RuntimeError):
    pass


class ModelBudgetLimited(RuntimeError):
    pass


class ShopPorts:
    def __init__(self, shop_url: str, *, service_token_file: Path | None = None):
        settings = Settings() if service_token_file is None else Settings(service_token_file=service_token_file)
        self.client = httpx.AsyncClient(base_url=shop_url.rstrip("/") + "/api/data-center/host/",
            headers={"X-Data-Center-Service-Token": settings.get_service_token()},
            timeout=100, trust_env=False)

    @staticmethod
    def _headers(context):
        if context.credential is None:
            raise PermissionError("数据服务调用缺少宿主身份凭据")
        return {"Authorization": "Bearer " + context.credential.get_secret_value(),
                "X-Shop-Actor": context.actor_id, "X-Request-ID": context.request_id}

    async def _post(self, path, body, context):
        response = await self.client.post(path, json=body, headers=self._headers(context))
        if response.status_code in {401, 403}:
            raise PermissionError("宿主拒绝当前身份的资料操作")
        if response.status_code == 424:
            raise SourceUnavailable(response.json()["detail"]["message"])
        if response.status_code == 429:
            raise ModelBudgetLimited(response.json()["detail"]["message"])
        if response.status_code == 422:
            raise ValueError(response.json()["detail"])
        response.raise_for_status()
        return response

    async def login(self, username, password, request_id):
        response = await self.client.post("login", json={"username": username,
                                         "password": password, "request_id": request_id})
        response.raise_for_status()
        identity = response.json()
        return CallContext(actor_id=identity["actor_id"], request_id=request_id,
                           credential=SecretStr(identity["credential"]))

    async def authorize(self, context, source_ids, action):
        response = await self._post("authorize", {"source_ids": source_ids, "action": action}, context)
        return AuthorizedScope.model_validate_json(response.content)

    async def identity(self, context):
        response = await self._post("identity", {}, context)
        if response.json()["actor_id"] != context.actor_id:
            raise PermissionError("宿主认证主体与当前调用不一致")

    async def catalog(self, context):
        response = await self._post("catalog", {}, context)
        return response.json()

    async def snapshot(self, source_id, context):
        response = await self._post("snapshot", {"source_id": source_id}, context)
        return DataSnapshot.model_validate_json(response.content)

    async def read_page(self, source_id, snapshot_id, cursor, limit, context):
        response = await self._post("page", {"source_id": source_id, "snapshot_id": snapshot_id,
                                            "cursor": cursor, "limit": limit}, context)
        return SourcePage.model_validate_json(response.content)

    async def read_current(self, source_id, record_id, context):
        response = await self._post("current", {"source_id": source_id, "record_id": record_id}, context)
        return response.json()

    async def read_file(self, ref, context):
        response = await self._post("file", ref.model_dump(mode="json"), context)
        digest = sha256(response.content).hexdigest()
        if digest != ref.sha256 or response.headers.get("x-content-sha256") != digest:
            raise ValueError("宿主返回文件没有通过原始来源摘要检查")
        return response.content

    async def invoke(self, request, context):
        response = await self._post("models/invoke", request.model_dump(mode="json"), context)
        return ModelReply.model_validate_json(response.content)

    async def reuse(self, request, original_request_id, context):
        response = await self._post("models/reuse", {
            "invocation": request.model_dump(mode="json"), "original_request_id": original_request_id}, context)
        return ModelReply.model_validate_json(response.content)

    async def lookup(self, task_id, step_key, context):
        response = await self._post("models/lookup", {"task_id": task_id, "step_key": step_key}, context)
        return None if response.json() is None else ModelReply.model_validate_json(response.content)

    async def bind(self, task_id, run_id, operation, source_ids, payload, context):
        await self._post("tasks/bind", {"task_id": task_id, "run_id": run_id,
                                       "operation": operation, "source_ids": source_ids,
                                       "payload": payload}, context)

    async def cancel(self, task_id, context):
        await self._post("tasks/cancel", {"task_id": task_id}, context)

    async def usage(self, task_id, context):
        response = await self._post("tasks/usage", {"task_id": task_id}, context)
        return response.json()

    async def manage(self, operation, payload, context):
        paths = {"manage_sources": "list", "register_source": "register",
                 "set_grant": "grant", "withdraw_source": "withdraw"}
        response = await self._post("registry/" + paths[operation], payload, context)
        return response.json()

    async def close(self):
        await self.client.aclose()
