# API 参考

所有 API 返回统一格式：

```json
{ "code": 0, "msg": "", "data": ... }
```

`code=0` 成功，非 0 失败（`msg` 含错误信息）。

启用链路追踪后，响应头会包含 `X-Trace-Id`，可在 Jaeger UI 中搜索定位完整调用链路。

---

## 仪表盘

| 方法 | 路径                | 说明                                        |
| ---- | ------------------- | ------------------------------------------- |
| GET  | `/api/v1/health`    | 深度检查（etcd 延迟、MySQL 池、任务队列）   |
| GET  | `/api/v1/metrics`   | 调度器指标（积压/活跃任务、Worker 统计）    |
| GET  | `/api/v1/dashboard` | 聚合统计（运行中/已完成/失败、存活 Worker） |

### 健康检查响应

`/api/v1/health` 深度检查 etcd 连接延迟、MySQL 连接池状态和任务队列深度。
任一关键组件不健康时返回 HTTP 503，适用于 K8s liveness/readiness probe。

```json
{
  "status": "ok",
  "details": {
    "etcd": { "ok": true, "latency": "3.2ms" },
    "mysql": {
      "ok": true,
      "latency": "1.1ms",
      "open_conns": 5,
      "in_use": 1,
      "idle": 4,
      "max_open": 100
    },
    "queue": {
      "ok": true,
      "pending_tasks": 0,
      "active_tasks": 3,
      "queue_depth_per_worker": 0
    }
  }
}
```

### Metrics 响应

`/api/v1/metrics` 返回 `BacklogMetrics` 快照，包含任务积压、并发数和 Worker 统计，可用于外部监控和自动扩缩容：

```json
{
  "code": 0,
  "data": {
    "pending_tasks": 5,
    "active_tasks": 12,
    "total_workers": 4,
    "alive_workers": 3,
    "dead_workers": 1,
    "queue_depth_per_worker": 1
  }
}
```

---

## 任务类型

| 方法 | 路径            | 说明                                 |
| ---- | --------------- | ------------------------------------ |
| GET  | `/api/v1/tasks` | 获取所有已注册任务类型（含参数定义） |

---

## Worker

| 方法 | 路径              | 说明                               |
| ---- | ----------------- | ---------------------------------- |
| GET  | `/api/v1/workers` | 获取所有 Worker 状态（含心跳时间） |

---

## 工作流

| 方法 | 路径 | 说明 |
| ---- | ---- | ---- |
| POST | `/api/v1/workflows` | 创建并提交工作流 |
| GET | `/api/v1/workflows` | 列出运行中+待处理的工作流 |
| GET | `/api/v1/workflows/:uuid` | 获取指定工作流状态 |
| DELETE | `/api/v1/workflows/:uuid` | 取消运行中的工作流 |
| POST | `/api/v1/workflows/:uuid/retry` | 重试已结束的工作流 |
| POST | `/api/v1/workflows/:uuid/run` | 执行 PENDING 状态的工作流 |
| POST | `/api/v1/workflows/:uuid/approve` | 审批通过暂停中的工作流 |
| POST | `/api/v1/workflows/:uuid/reject` | 驳回暂停中的工作流 |

### 驳回请求体

```json
{ "reason": "内容不合规" }
```

| 字段     | 类型   | 必填 | 说明     |
| -------- | ------ | ---- | -------- |
| `reason` | string | 否   | 驳回原因 |

### 创建工作流请求体

```json
{
  "name": "my-workflow",
  "task_groups": [
    [{ "name": "task_a", "params": { "key": "value" } }, { "name": "task_b" }],
    [{ "name": "task_c", "delay_sec": 5 }]
  ],
  "trigger": "manual",
  "cron_expr": "",
  "timeout_sec": 3600,
  "task_timeout_sec": 300,
  "max_retries": 3,
  "base_backoff": 1
}
```

| 字段 | 类型 | 必填 | 说明 |
| ---- | ---- | ---- | ---- |
| `name` | string | 是 | 工作流名称（API 用 UUID 标识） |
| `task_groups` | `[][]TaskNode` | 是 | 任务组，组内并行、组间串行 |
| `task_groups[].name` | string | 是 | 任务类型名，对应 `Task.Name()` |
| `task_groups[].params` | object | 否 | 任务参数（key 对应 `Param.Key`） |
| `task_groups[].delay_sec` | int | 否 | 延迟执行秒数 |
| `task_groups[].branches` | object | 否 | 条件分支定义 |
| `trigger` | string | 是 | `manual`/`cron`/`event` |
| `cron_expr` | string | 条件 | `trigger=cron`时必填 |
| `event_trigger` | object | 条件 | `trigger=event`时必填 |
| `timeout_sec` | int | 否 | 工作流整体超时（秒） |
| `task_timeout_sec` | int | 否 | 单任务超时（秒） |
| `max_retries` | int | 否 | 任务失败最大重试次数 |
| `base_backoff` | int | 否 | 重试退避基数（秒） |
| `priority` | int | 否 | 优先级（默认0，越高越优先） |
| `error_policy` | object | 否 | 错误策略定义 |

### 错误策略定义（`error_policy`）

当任务失败或超时时，可通过 `error_policy` 控制工作流的整体行为：

| 字段 | 类型 | 必填 | 说明 |
| ---- | ---- | ---- | ---- |
| `on_task_failure` | string | 否 | fail(默认)/continue/skip_and_continue |
| `on_timeout` | string | 否 | fail(默认)/mark_skipped |
| `skippable_tasks` | `[]string` | 否 | 可跳过任务列表 |
| `task_policies` | object | 否 | 按任务名细粒度策略 |

创建成功响应：

```json
{ "code": 0, "data": { "uuid": "<UUID>", "name": "my-workflow" } }
```

UUID 在创建时由 Master 自动生成（`google/uuid`），与 `name` 无映射关系。
后续所有操作（查询/取消/重试/审批等）均需使用该 `uuid` 作为路径参数。

### 事件触发定义（`event_trigger`）

当 `trigger=event` 时，工作流不会立即执行，而是等待外部事件触发。支持三种事件类型：

| 字段 | 类型 | 必填 | 说明 |
| ---- | ---- | ---- | ---- |
| `event_type` | string | 是 | `webhook` / `workflow_completed` / `task_failed` |
| `filter` | string | 否 | 因 event_type 而异，见下方说明 |
| `webhook_secret` | string | 否 | Webhook 签名密钥（可选） |

三种事件类型说明：

| 类型 | 行为 |
| ---- | ---- |
| `webhook` | `POST /api/v1/webhooks/:uuid` 触发，payload 注入首个 `params` |
| `workflow_completed` | 指定源工作流完成时自动触发，`filter` 填源 UUID |
| `task_failed` | 指定工作流中任务失败时自动触发，`filter` 填 `源UUID:任务名` |

---

## Webhook

| 方法 | 路径                     | 说明                                  |
| ---- | ------------------------ | ------------------------------------- |
| POST | `/api/v1/webhooks/:uuid` | 通过 Webhook 触发一个事件驱动的工作流 |
| GET  | `/api/v1/webhooks`       | 列出所有注册了 Webhook 触发的工作流   |

### 触发 Webhook 请求体

```json
{
  "payload": { "url": "https://example.com", "key": "value" }
}
```

| 字段      | 类型   | 必填 | 说明                                        |
| --------- | ------ | ---- | ------------------------------------------- |
| `payload` | object | 否   | 传入参数，将作为目标工作流首任务的 `params` |

### 触发流程

1. 先创建一个 `trigger=event` + `event_trigger.event_type=webhook` 的工作流（状态为 PENDING）
2. 外部系统通过 `POST /api/v1/webhooks/:uuid` 触发，传入任意 payload
3. Master 深拷贝工作流定义，将 payload 注入第一个任务的 params，然后提交执行
4. 触发成功后返回 HTTP 201，工作流进入 RUNNING 状态

---

## 历史记录

| 方法 | 路径                                        | 说明             |
| ---- | ------------------------------------------- | ---------------- |
| GET  | `/api/v1/history`                           | 列出所有历史记录 |
| GET  | `/api/v1/history/paged?page=1&page_size=10` | 分页查询历史     |
| GET  | `/api/v1/history/:uuid`                     | 获取指定历史详情 |

---

## WebSocket 实时推送

| 方法 | 路径  | 说明         |
| ---- | ----- | ------------ |
| GET  | `/ws` | 实时事件推送 |

支持可选查询参数 `?workflow=<UUID>` 订阅特定工作流的事件，减少广播消息量。

### 事件类型

| 事件                    | 说明                   |
| ----------------------- | ---------------------- |
| `workflow_submitted`    | 工作流已提交           |
| `workflow_completed`    | 工作流执行成功         |
| `workflow_failed`       | 工作流执行失败         |
| `workflow_cancelled`    | 工作流已取消           |
| `workflow_paused`       | 工作流暂停（等待审批） |
| `workflow_approved`     | 审批通过，继续执行     |
| `workflow_rejected`     | 审批驳回               |
| `workflow_rolling_back` | 正在执行 Saga 回滚     |
| `workflow_rolled_back`  | 回滚完成               |
| `task_completed`        | 任务执行成功           |
| `task_failed`           | 任务执行失败           |
| `task_rolled_back`      | 任务回滚完成           |
| `worker_up`             | Worker 上线            |
| `worker_down`           | Worker 下线            |
