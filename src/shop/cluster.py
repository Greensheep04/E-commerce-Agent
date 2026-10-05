import asyncio
from decimal import Decimal

from agent_cluster.api import create_cluster_router
from agent_cluster.context import ContextManager
from agent_cluster.engine import SYSTEM_PROMPT, ClusterEngine
from agent_cluster.mcp_client import MCPCommand, connect
from agent_cluster.notes import NoteStore
from agent_cluster.runtime import ClusterRuntime
from agent_cluster.store import ClusterStore
from agent_cluster.types import ClusterModel, CoordinatorProfile
from fastapi import Depends
from sqlalchemy import select

from shop.auth import current_user
from shop.budget import BudgetService
from shop.data_center_automation import DataCenterAutomation
from shop.models import User
from shop.paid_models import PaidModels
from shop.runtime_models import ProviderRequest


class ClusterConfiguration(ClusterModel):
    profile: CoordinatorProfile = CoordinatorProfile()
    actors: dict[str, MCPCommand]
    autorun: bool = True
    data_center_autorun: bool = False


class ShopModelPort:
    def __init__(self, settings, maker):
        self.paid = PaidModels(settings, BudgetService(maker, verification_scope="cluster"))

    async def request(self, **kwargs):
        return await asyncio.to_thread(self.paid.request, **kwargs)


class ShopCluster:
    def __init__(self, settings, engine, maker, *, integration=None):
        config_path = settings.cluster_config_file.resolve(strict=True)
        self.configuration = ClusterConfiguration.model_validate_json(config_path.read_text())
        self.store = ClusterStore(engine)
        self.maker = maker
        self.notes = NoteStore(settings.cluster_notes_dir)
        self.model = ShopModelPort(settings, maker)
        self.context = ContextManager(self.notes, self.configuration.profile, SYSTEM_PROMPT, self.model)
        self.engine = ClusterEngine(self.store, self.notes, self.configuration.profile, self.model,
                                    self.context, self.command_for_actor, self.usage)
        self.runtime = ClusterRuntime(self.store, self.engine)
        self.automation = None
        if integration is not None:
            self.automation = DataCenterAutomation(self, integration, settings, maker)
            self.runtime.before_run = self.automation.preflight
            self.runtime.after_run = self.automation.after_run
            if not self.configuration.autorun:
                self.runtime.goal_filter = self.automation.accepts_goal
        if self.configuration.data_center_autorun and self.automation is None:
            raise ValueError("数据中台自动运行需要宿主权限及来源组件")

    def command_for_actor(self, actor_id):
        if actor_id not in self.configuration.actors:
            raise PermissionError("当前账号尚未登记MCP接入配置")
        return self.configuration.actors[actor_id]

    async def validate_goal(self, goal):
        async with connect(self.command_for_actor(goal.actor_id)) as client:
            catalog = client.catalog
        catalog.validate_goal(goal)

    async def usage(self, run_id, *, goal_id=None):
        with self.maker() as session:
            field = "cluster_run_id" if goal_id is None else "cluster_goal_id"
            identity = run_id if goal_id is None else goal_id
            rows = session.scalars(select(ProviderRequest).where(
                ProviderRequest.price_basis[field].astext == identity)).all()
            spent = sum((row.charged_cny for row in rows if row.charged_cny is not None), Decimal(0))
            reserved = sum((row.reservation_cny for row in rows if row.charged_cny is None), Decimal(0))
        return spent, reserved

    def router(self):
        def actor_dependency(user: User = Depends(current_user)):
            return user.id

        return create_cluster_router(self.store, self.runtime, actor_dependency, self.validate_goal)

    async def startup(self):
        if self.configuration.autorun or self.configuration.data_center_autorun:
            await self.runtime.startup()

    async def shutdown(self):
        if self.configuration.autorun or self.configuration.data_center_autorun:
            await self.runtime.shutdown()
