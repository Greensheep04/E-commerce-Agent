import asyncio
from uuid import uuid4

from integration_host import ModuleHost
from integration_host.errors import UnknownModule, UnsupportedOperation
from integration_protocol import CallContext, HostCapabilities
from itsdangerous import BadData, URLSafeTimedSerializer
from pydantic import SecretStr
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from shop.assessment_orders import configured_order_port
from shop.assessment_service import SalesMetricService
from shop.auth import authenticate
from shop.config import Settings
from shop.customer_runtime import CustomerService
from shop.customer_worker import CustomerWorker
from shop.integration_adapters import build_adapters
from shop.integration_models import IntegrationBudgetWindow
from shop.integration_store import IntegrationStore
from shop.models import User
from shop.runtime_models import ProviderRequest


class ShopAuthentication:
    def __init__(self, settings, maker):
        self.maker = maker
        self.signer = URLSafeTimedSerializer(settings.get_session_secret(), salt="shop-module-context")

    def check_available(self):
        with self.maker() as session:
            session.execute(select(User.id).limit(0))

    def actor(self, actor_id):
        with self.maker() as session:
            user = session.get(User, actor_id)
            if user is None:
                raise PermissionError("账号不存在")
            session.expunge(user)
            return user

    def issue(self, actor_id, request_id, upstream=None):
        self.actor(actor_id)
        token = self.signer.dumps({"actor_id": actor_id})
        return CallContext(actor_id=actor_id, request_id=request_id,
                           upstream=[] if upstream is None else upstream, credential=SecretStr(token))

    def verify(self, context):
        if context.credential is None:
            raise PermissionError("调用缺少已认证身份凭据")
        try:
            identity = self.signer.loads(context.credential.get_secret_value(), max_age=3600)
        except BadData as error:
            raise PermissionError("调用身份凭据无效或已经过期") from error
        if identity.get("actor_id") != context.actor_id:
            raise PermissionError("调用身份与凭据不一致")
        self.actor(context.actor_id)


class ShopBudgetCapability:
    def __init__(self, maker):
        self.maker = maker

    def check_available(self):
        with self.maker() as session:
            session.execute(select(ProviderRequest.id).limit(0))
            session.execute(select(IntegrationBudgetWindow.id).limit(0))


class ShopIntegration:
    def __init__(self, settings: Settings, *, customer=None, worker=None, metrics=None):
        self.settings = settings
        self.engine = create_engine(settings.database_url, pool_pre_ping=True)
        self.maker = sessionmaker(self.engine, expire_on_commit=False)
        self.auth = ShopAuthentication(settings, self.maker)
        self.store = IntegrationStore(self.maker)
        self.owns_customer = customer is None
        self.customer = CustomerService(settings) if customer is None else customer
        self.owns_worker = worker is None
        self.worker = CustomerWorker(self.customer) if worker is None else worker
        self.metrics = SalesMetricService(configured_order_port(settings)) if metrics is None else metrics
        self.host = ModuleHost(HostCapabilities(
            authentication=self.auth, persistence=self.store, budget=ShopBudgetCapability(self.maker),
        ))
        self.adapters = build_adapters(self)
        for adapter in self.adapters.values():
            self.host.register(adapter)

    def login(self, username, password, request_id=None, upstream=None):
        with self.maker() as session:
            user = authenticate(session, username, password)
            if user is None:
                raise PermissionError("账号或密码错误")
            return self.auth.issue(user.id, request_id or str(uuid4()), upstream)

    async def startup(self):
        await self.host.startup()

    async def shutdown(self):
        await self.host.shutdown()
        if self.owns_customer:
            self.customer.engine.dispose()
        self.engine.dispose()

    async def invoke(self, module_id, operation, payload, context, version="1"):
        return await self.host.invoke(module_id, operation, payload, context, version)

    def validate_upstream(self, context):
        for reference in context.upstream:
            adapter = self.adapters.get(reference.module_id)
            if adapter is None:
                raise ValueError("上游引用所属模块没有注册")
            actual = adapter.resolve(reference.resource_type, reference.resource_id, context.actor_id)
            if reference.version is not None and actual.version != reference.version:
                raise ValueError("上游资源版本不一致")
            if reference.sha256 is not None and actual.sha256 != reference.sha256:
                raise ValueError("上游资源文件摘要不一致")

    async def file(self, module_id, resource_id, relative_path, context):
        self.auth.verify(context)
        adapter = self.adapters.get(module_id)
        if adapter is None:
            raise UnknownModule("资源所属模块没有注册")
        if not callable(getattr(adapter, "read_file", None)):
            raise UnsupportedOperation("模块没有提供文件读取能力")
        try:
            await asyncio.to_thread(self.validate_upstream, context)
            return await asyncio.to_thread(
                adapter.read_file, resource_id, relative_path, context.actor_id,
            )
        except FileNotFoundError as error:
            raise LookupError("登记的资源文件不存在") from error
