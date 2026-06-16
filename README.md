# VibeFlow

基于 etcd 的 Go 语言分布式工作流引擎。Master-Worker 架构，支持 DAG 编排、定时调度、失败重试、Saga 补偿回滚、条件分支、人工审批、实时推送、Web 管理界面、OpenTelemetry 链路追踪、熔断器、CAS 乐观锁、Fan-Out 分发、增量快照和持久化 Worker Pool。

## 文档目录

| 文档                                          | 说明                                                           |
| --------------------------------------------- | -------------------------------------------------------------- |
| [架构](docs/architecture.md)                  | 整体架构、核心组件设计、技术栈                                 |
| [核心概念](docs/core-concepts.md)             | 任务（Task）、工作流（Workflow）、编排能力                     |
| [高可用与性能保障](docs/high-availability.md) | 熔断器、CAS 乐观锁、Fan-Out 分发、增量快照、持久化 Worker Pool |
| [部署与快速开始](docs/deployment.md)          | Docker 构建、运行、快速体验                                    |
| [使用案例](docs/usage-examples.md)            | 串行、并行、重试、定时、延迟、条件分支、人工审批等场景         |
| [自定义任务开发](docs/custom-tasks.md)        | Task 接口实现、参数类型、Saga 回滚、任务与工作流注册           |
| [API 参考](docs/api-reference.md)             | REST API、WebSocket 实时推送                                   |
| [配置说明](docs/configuration.md)             | config.yaml 完整配置项说明                                     |

## 快速开始

```bash
# 1. 启动基础服务
docker compose -f deploy/docker-compose.yaml up -d

# 2. 启动 Master
go run ./cmd/master --config config.yaml

# 3. 启动 Worker
go run ./cmd/worker --config config.yaml

# 4. 访问 Web 界面
open http://localhost:8080
```

详细步骤请参考 [部署与快速开始](docs/deployment.md)。

## License

Apache License 2.0
