# Agent Hub

Agent Hub 是独立的 MCP stdio 接入项目，通过已认证 HTTP 调用已登记的业务 Agent。业务 Agent 保留内部工具、任务、来源和费用记录；需要跨 Agent 使用的操作由配置明确登记为共享工具。外部 MCP 客户端选择工具与操作。

项目使用 Python 3.11、FastMCP、官方 MCP SDK、HTTPX、JSON Schema 和 `integration_protocol`。公共登记与 MCP 层通过 `BackendConnector` 接入业务，无需导入 `shop`、数据库 ORM 或模型代码。

## 安装

在仓库根目录安装独立环境：

```bash
mkdir -p .scratch/uv-cache .scratch/runtime
UV_CACHE_DIR=.scratch/uv-cache TMPDIR=.scratch/runtime uv sync --project agent-hub --frozen
```

分发到其他项目时构建 `integration-protocol` 和 `agent-hub` 两个 wheel，在目标 Python 3.11 环境安装这些 wheel。目标环境的依赖由包管理器安装。

```bash
UV_CACHE_DIR=.scratch/uv-cache TMPDIR=.scratch/runtime uv build --wheel packages/integration-protocol --out-dir .scratch/wheels
UV_CACHE_DIR=.scratch/uv-cache TMPDIR=.scratch/runtime uv build --wheel agent-hub --out-dir .scratch/wheels
```

## 身份与登记配置

[config.example.json](config.example.json)提供宣传页面、文字客服、销售考核、自媒体海报及数据中台五个Agent和两个素材共享工具的登记示例。根据已启用的后端模块填写登记配置、账号、密码文件路径和已认证账号的`id`。数据中台需要在Shop中配置独立服务地址。`expected_actor_id`与后端登录返回的`id`必须一致。密码文件权限为`0600`；相对路径从配置所在目录解析。

`agents` 指定整体 Agent 的 `module_id`、工具名称、允许操作与接口版本。`shared_tools` 指定单个可复用操作。启动时验证名称唯一、认证、模块版本、允许操作及原始输入输出 Schema。配置中的允许能力继续受后端业务授权约束。为不同身份启动不同的 stdio 进程。

MCP 客户端配置使用以下命令及参数，路径填写为绝对路径：

```json
{
  "mcpServers": {
    "agent-hub": {
      "command": "/path/to/agent-hub/.venv/bin/agent-hub",
      "args": ["--config", "/path/to/private-config.json"]
    }
  }
}
```

也可直接执行 `agent-hub/.venv/bin/python -m agent_hub --config /path/to/private-config.json`。标准输出使用 MCP 协议，诊断输出使用标准错误。

## 调用方式

`list_agents` 返回整体 Agent、共享工具、允许能力、接口版本和每项操作的完整输入输出 Schema。Agent 工具使用 `action`、`payload`、`request_id` 与 `upstream`；共享工具使用 `payload`、`request_id` 与 `upstream`。

```json
{
  "action": "orders",
  "payload": {"limit": 5, "offset": 0},
  "request_id": "616ae3f3-c067-4f4d-8b96-166ae04c5b1a",
  "upstream": []
}
```

返回内容保留 `OperationResult` 的 `data`、`resources`、`status`、`run_id` 与 `costs`。客户端根据业务状态查询后续结果。同一逻辑请求继续使用原 `request_id`、内容及上游引用；相同编号与不同内容会被拒绝。

文件资源使用 `agenthub://{connector_id}/{module_id}/{resource_type}/{resource_id}/{relative_path}`，可添加 `version`、`sha256` 查询参数。文件通过后端受控接口读取，核查身份、实际资源、路径、MIME 和内容摘要。Shop 素材文件路径为 `content`，已审核 Harness 页面为 `page.html`，Poster 文件使用已登记的 `framework/` 相对路径。

HTTP 连接默认等待 10 秒，读取默认等待 300 秒。超时、MCP cancellation 和 stdio 关闭会结束 Hub 的等待；业务执行状态通过原任务编号查询。需要取消业务时明确调用该 Agent 支持的 `cancel`。框架不会自动重新发送失败调用。

Shop Poster 完整生成依赖已登记的业务角色，角色不足时返回 `waiting_agents`。允许能力与当前支持状态由目录说明；费用及未确定用量由原后端预算服务保存和检查。

## 接入其他项目

提供异步 `BackendConnector` 的 `describe()`、`invoke()`、`read_file()` 和 `close()`，再通过 `Registry` 登记 connector、整体 Agent 和共享工具。`describe()` 返回公共 `ModuleDescriptor`，包含独立且完整的操作 Schema；原始 `$defs` 与 `$ref` 保持原结构。Registry 与 MCP server 通过统一生命周期启动并关闭 connector。

HTTP connector 可用于实现相同认证和公共 HTTP 协议的服务。其他传输方式通过自己的 connector 接入，无需修改公共登记与 MCP 层。
