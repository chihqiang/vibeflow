# 架构

VibeFlow 采用 Master-Worker 架构，基于 etcd 实现分布式协调。

## 整体架构

```text
                         ┌──────────────────┐
                         │       etcd        │
                         │     :2379         │
                         │  ┌────────────┐   │
                         │  │ /vibeflow/  │   │
                         │  │  ├─tasks/   │   │  ← 任务下发 & 状态上报
                         │  │  ├─heartbeats│   │  ← Worker 心跳
                         │  │  └─workflows│   │  ← 工作流持久化
                         │  └────────────┘   │
                         └───┬──────────┬────┘
                       Watch │          │ Watch
                    ┌────────┘          └────────┐
                    ▼                            ▼
            ┌──────────────┐            ┌──────────────┐
            │    Master     │            │    Worker     │
            │    :8080      │            │   (N 个实例)   │
            │               │            │               │
            │  HTTP API     │            │  执行业务逻辑   │
            │  Web UI       │            │  心跳上报      │
            │  调度 & 编排   │            │  分布式锁      │
            └───┬───────┬───┘            └───────┬───────┘
                │       │                        │
                │       │  OTLP gRPC             │ OTLP gRPC
                │       └──────────┐    ┌────────┘
                │ 异步写入（日志）   │    │
                ▼                  ▼    ▼
        ┌──────────────┐   ┌──────────────────┐
        │    MySQL      │   │   Jaeger (可选)   │
        │    :3306      │   │   :4317 :16686   │
        │               │   │                  │
        │  执行历史      │   │  分布式链路追踪    │
        │  工作流定义    │   │  W3C TraceContext │
        └──────────────┘   └──────────────────┘
```

## 核心设计

| 组件       | 角色       | 数据流                                 |
| ---------- | ---------- | -------------------------------------- |
| **etcd**   | 运行时核心 | Master↔Worker 通信：任务下发、流转、锁 |
| **Master** | 调度 & API | Watch etcd 感知任务变化→调度→写回      |
| **Worker** | 执行节点   | Watch PENDING→加锁→执行→写回结果       |
| **MySQL**  | 持久化日志 | 异步写入，不参与决策；etcd 过期后保留  |

**关键原则**：etcd 是权威数据源，MySQL 是尽力写入的日志。MySQL 不可用时系统仍可正常运行。

**链路追踪**：Master 和 Worker 通过 OTLP gRPC 将 trace 发至 Jaeger，支持 W3C
TraceContext 跨进程传播（Master → etcd → Worker）。每个 HTTP 响应头 `X-Trace-Id`
返回当前 trace ID，可在 Jaeger UI 中搜索定位。

## 技术栈

| 组件       | 技术                                                        |
| ---------- | ----------------------------------------------------------- |
| 语言       | Go 1.26+                                                    |
| HTTP 框架  | Gin                                                         |
| 分布式协调 | etcd v3.5（服务发现、分布式锁、Watch）                      |
| 数据库     | MySQL 8.0（持久化日志）                                     |
| 链路追踪   | OpenTelemetry + Jaeger（可选，OTLP gRPC，W3C TraceContext） |
| WebSocket  | gorilla/websocket                                           |
| Web 前端   | 原生 JavaScript + Vue.js（CDN）                             |
