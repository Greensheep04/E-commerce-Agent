import asyncio
import re
from hashlib import sha256
from urllib.parse import quote

import httpx
from integration_protocol import ModuleDescriptor, OperationResult, ResourceRef
from pydantic import TypeAdapter, ValidationError

from agent_hub.config import ConnectorConfig
from agent_hub.core import HubError, ResourceContent


class HttpConnector:
    def __init__(self, config: ConnectorConfig):
        self.config = config
        self._client = httpx.AsyncClient(
            base_url=config.base_url, trust_env=False, follow_redirects=False,
            timeout=httpx.Timeout(config.read_timeout, connect=config.connect_timeout),
        )
        self._login_lock = asyncio.Lock()
        self._authenticated = False

    async def _request(self, method, path, **kwargs):
        try:
            response = await self._client.request(method, path, **kwargs)
        except httpx.TimeoutException as error:
            raise HubError("backend_timeout", "后端等待超时，请通过原任务编号核查业务状态") from error
        except httpx.RequestError as error:
            raise HubError("backend_unavailable", "无法连接后端，请核查后端地址及运行状态") from error
        if not response.is_success:
            if path == "/api/login":
                raise HubError("authentication_failed", "后端身份认证失败")
            code = f"backend_http_{response.status_code}"
            message = "后端拒绝调用"
            if "application/json" in response.headers.get("content-type", ""):
                detail = response.json().get("detail")
                if isinstance(detail, dict):
                    code = detail.get("code", code)
                    message = detail.get("message", message)
                elif isinstance(detail, str):
                    message = detail
            raise HubError(code, message)
        return response

    async def _login(self):
        if self._authenticated:
            return
        async with self._login_lock:
            if self._authenticated:
                return
            path = self.config.password_file
            if path.stat().st_mode & 0o077:
                raise HubError("unsafe_credentials", "密码文件只能允许当前用户读取")
            password = path.read_text(encoding="utf-8").strip()
            if not password:
                raise HubError("invalid_credentials", "密码文件内容不能为空")
            response = await self._request("POST", "/api/login", json={
                "username": self.config.username, "password": password,
            })
            if response.json().get("id") != self.config.expected_actor_id:
                raise HubError("identity_mismatch", "后端身份与启动配置绑定的身份不一致")
            self._authenticated = True

    async def describe(self) -> list[ModuleDescriptor]:
        await self._login()
        response = await self._request("GET", "/api/modules")
        try:
            return TypeAdapter(list[ModuleDescriptor]).validate_json(response.content)
        except ValidationError as error:
            raise HubError("invalid_backend_catalog", "后端模块目录没有通过公共协议检查") from error

    async def invoke(self, module_id, operation, payload, *, version, request_id, upstream) -> OperationResult:
        await self._login()
        response = await self._request(
            "POST", f"/api/modules/{quote(module_id, safe='')}/operations/{quote(operation, safe='')}",
            json={"version": version, "request_id": request_id, "payload": payload,
                  "upstream": [entry.model_dump(mode="json") for entry in upstream]},
        )
        try:
            return OperationResult.model_validate_json(response.content)
        except ValidationError as error:
            raise HubError("invalid_backend_result", "后端结果没有通过公共协议检查") from error

    async def read_file(self, reference: ResourceRef, relative_path: str) -> ResourceContent:
        if (not relative_path or relative_path.startswith("/") or "\\" in relative_path
                or any(segment in {"", ".", ".."} for segment in relative_path.split("/"))):
            raise HubError("invalid_resource_path", "文件路径必须为规范的相对路径")
        await self._login()
        path = (f"/api/modules/{quote(reference.module_id, safe='')}/resources/"
                f"{quote(reference.resource_id, safe='')}/files/{quote(relative_path, safe='/')}")
        params = {"resource_type": reference.resource_type}
        for key in ("version", "sha256"):
            value = getattr(reference, key)
            if value is not None:
                params[key] = value
        response = await self._request("GET", path, params=params)
        digest = sha256(response.content).hexdigest()
        declared = response.headers.get("X-Content-SHA256", "")
        if not re.fullmatch(r"[a-f0-9]{64}", declared) or digest != declared:
            raise HubError("resource_digest_mismatch", "后端文件内容与登记摘要不一致")
        if reference.sha256 is not None and reference.sha256 != digest:
            raise HubError("resource_digest_mismatch", "文件内容与引用摘要不一致")
        mime_type = response.headers.get("content-type", "").split(";", 1)[0].strip()
        if not mime_type:
            raise HubError("invalid_resource_mime", "后端文件没有提供MIME类型")
        return ResourceContent(content=response.content, mime_type=mime_type, sha256=digest)

    async def close(self):
        await self._client.aclose()
