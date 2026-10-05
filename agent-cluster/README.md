# Agent Cluster

通过MCP stdio调用独立业务Agent，保存目标、计划、依赖、日程、事件、Note和原始模型记录。LangGraph负责执行，PostgreSQL负责持久状态与进程协调，APScheduler 3.x负责日程，Deep Agents组件用于文件和上下文管理。

公共包通过`ModelPort`获得模型调用与费用管理，通过宿主认证API获得当前用户身份。业务Agent保留自己的授权、Schema和请求去重逻辑。可复用工具由MCP目录显式登记。

## 安装与宿主接入

需要Python 3.11及PostgreSQL。在本仓库中安装依赖：

```bash
UV_CACHE_DIR=.scratch/uv-cache uv sync --frozen --extra dev
.venv/bin/alembic upgrade head
UV_CACHE_DIR=.scratch/uv-cache uv sync --project agent-hub --frozen
```

独立安装时一并安装`integration-protocol`。宿主使用`ClusterStore`连接数据库，使用`NoteStore`提供授权文件目录，实现异步`ModelPort.request`及用量查询，再创建`ClusterEngine`和`ClusterRuntime`。`create_cluster_router`接受宿主认证依赖和目标能力检查函数。公共代码不导入业务应用。

本项目的宿主接入由`shop.cluster.ShopCluster`提供。设置`SHOP_CLUSTER_CONFIG_FILE`，然后按照根项目说明启动API。`SHOP_CLUSTER_NOTES_DIR`指定Note目录。配置文件包括协调模型和每个用户对应的MCP进程：

```json
{
  "profile": {
    "service": "deepseek",
    "model": "deepseek-flash",
    "reasoning_effort": "high",
    "output_limit": 8192,
    "context_tokens": 1000000,
    "compression_ratio": 0.85
  },
  "actors": {
    "your-actor-id": {
      "command": "/path/to/project/agent-hub/.venv/bin/python",
      "args": ["-m", "agent_hub", "--config", "/path/to/project/.local/agent-hub.json"],
      "cwd": "/path/to/project"
    }
  },
  "autorun": true
}
```

Hub配置需要自己的业务HTTP地址、登录账号、受保护密码文件和`expected_actor_id`。账号编号必须与`actors`键相同。模型凭据由宿主读取，不放入目标、Note、日志或命令参数。

Qwen显式配置使用`service="dashscope"`和`model="qwen3.8-flash"`；提供商请求使用`enable_thinking`及小于总输出上限的`thinking_budget`。可配置官方Tokenizer文件与对应`tokenizer_sha256`，两项必须同时填写。容量检查始终包含输出预留。

## 目标与CLI

目标指定`objective`、`allowed_operations`、`criteria`及预算。完成条件使用结果路径、检查方式和必要产物数量，由程序读取实际返回结果进行检查。协调模型可以追加任务与依赖；原任务编号对应的内容持续保留。改变业务输入需要登记新任务。

CLI配置如下，密码文件使用0600权限：

```json
{
  "base_url": "http://127.0.0.1:8000",
  "username": "your-username",
  "password_file": "account.password",
  "expected_actor_id": "your-actor-id"
}
```

```bash
agent-cluster --config .local/cluster/client.json goals create --file .local/cluster/goal.json
agent-cluster --config .local/cluster/client.json goals start GOAL_ID --event-id manual-001
agent-cluster --config .local/cluster/client.json goals show GOAL_ID
agent-cluster --config .local/cluster/client.json goals pause GOAL_ID
agent-cluster --config .local/cluster/client.json goals resume GOAL_ID
agent-cluster --config .local/cluster/client.json goals cancel GOAL_ID
agent-cluster --config .local/cluster/client.json runs show RUN_ID
agent-cluster --config .local/cluster/client.json schedules create --goal-id GOAL_ID --interval-seconds 3600
```

目标修改使用`goals update GOAL_ID --file FILE --expected-revision REVISION`。事件使用`events emit --goal-id GOAL_ID --event-id EVENT_ID`；相同编号和内容复用，内容冲突明确拒绝。日程支持interval及五字段cron。周期各自保存独立运行，错过周期合并为最近一次并保存合并记录。

## 连续执行与控制

每个目标的协调决策顺序更新，同一数据库的业务任务最多四个并行。无状态变化的目标等待事件或明确恢复操作。目标暂停、取消后禁止新增调度；已发任务保留真实业务状态和费用。恢复使用原任务和请求编号。

每次模型请求保留系统指令、目标、授权、预算和完成标准。较早的完整工具轮次保存为原文及摘要Note，近期消息与待处理信息持续保留。摘要调用前保存完整请求，进程恢复可以复用相同输入。未知响应保留`unknown`费用预留；宿主负责停止重复付费请求。

业务结果显示未知响应时，执行器保存完整结果并停止该次运行的新增调用。任务与运行的`unknown`状态原子保存；进程恢复继续保留原请求、结果和费用预留。

本项目连接宣传页生成、文字客服、销售考核、自媒体海报、数据中台Agent和共享素材工具。数据中台通过独立服务提供来源查询、文档检索及分析，配置见[环境说明](../docs/使用说明/环境说明.md)，运行方式见[命令说明](../docs/使用说明/命令说明.md)。完整海报需要已注册的业务角色，缺少时返回`waiting_agents`。业务数据范围及授权由原Agent决定，公共调用格式见[接口说明](../docs/使用说明/接口说明.md)。
