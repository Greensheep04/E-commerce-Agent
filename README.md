# 电商 AI 工作台

通过网页、CLI或MCP使用多个电商Agent，完成素材管理、宣传页面生成、文字客服、销售考核、自媒体关键词准备和数据查询。

## 特点

- **统一接入**：Agent保留独立业务逻辑和工具，通过标准MCP stdio接口协作，也可接入外部MCP客户端。
- **目标驱动**：协调模型安排任务与依赖，支持并行执行、日程和事件触发。
- **持续执行**：目标、任务和Note持久保存，支持上下文压缩、暂停、取消及进程恢复。
- **有据可查**：结果关联来源、版本和产物引用，权限检查贯穿查询与执行。
- **模块扩展**：独立数据中台支持文档Hybrid检索和经营分析；Graph源码保留，构建默认关闭。

## 使用

1. 按[环境说明](docs/使用说明/环境说明.md)准备运行环境、数据库和模型凭据。
2. 按[命令说明](docs/使用说明/命令说明.md)初始化账号、导入资料并启动服务。
3. 登录网页选择业务模块，或通过CLI提交目标；外部程序可通过[Agent Hub](agent-hub/README.md)调用Agent。

默认网页入口为`http://127.0.0.1:8000/harness/`，管理入口为`http://127.0.0.1:8000/admin`。启用独立数据中台时，服务地址按环境说明配置。

## 使用的技术

| 部分 | 技术 |
|---|---|
| 服务与页面 | Python、FastAPI、SQLAlchemy、React、TypeScript、Vite |
| Agent接入与编排 | MCP SDK、LangGraph、Deep Agents、APScheduler |
| 状态与检索 | PostgreSQL、pgvector、RRF |
| 文档与经营分析 | Docling、dlt、dbt、MetricFlow；Graph扩展使用LightRAG |

Agent集群的配置与操作见[Agent Cluster](agent-cluster/README.md)，公共调用格式见[接口说明](docs/使用说明/接口说明.md)。
