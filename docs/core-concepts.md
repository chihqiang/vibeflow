# 核心概念

## 任务（Task）

任务是工作流的最小执行单元。所有任务实现 `Task` 接口：

```go
type Task interface {
    Name() string                                    // 全局唯一标识
    Params() []Param                                 // 自定义参数定义（前端据此渲染表单）
    Execute(ctx context.Context, paramCtx, taskCtx *Context) error  // 业务逻辑
}
```

### 上下文（Context）

任务执行时有两个上下文：

| 上下文     | 来源           | 生命周期         | 用途                        |
| ---------- | -------------- | ---------------- | --------------------------- |
| `paramCtx` | 用户提交的参数 | 单次任务执行期间 | 读取用户配置的静态参数      |
| `taskCtx`  | 上游任务输出   | 跨任务传递       | 上下游数据共享：`Set`/`Get` |

### 不可重试错误

任务可以通过返回 `model.WrapNoRetry(err)` 标记错误为不可重试（如参数校验失败），调度器不会重试该任务，直接标记工作流失败。

---

## 工作流（Workflow）

工作流由多个**任务组**（TaskGroup）串联组成，组内任务并行执行，组间串行执行：

```text
[task_a, task_b]  →  [task_c]  →  [task_d]
 └─ 并行 ─┘           └─ 串行 ─┘     └─ 串行 ─┘
```

### 工作流状态机

```text
PENDING ──► RUNNING ──┬──► COMPLETED
                      ├──► PAUSED ──► RUNNING（审批通过）
                      ├──► ROLLING_BACK ──► ROLLED_BACK
                      └──► FAILED
```

| 状态           | 含义               |
| -------------- | ------------------ |
| `PENDING`      | 已注册，等待执行   |
| `RUNNING`      | 正在执行           |
| `PAUSED`       | 暂停，等待人工审批 |
| `ROLLING_BACK` | Saga 补偿回滚中    |
| `ROLLED_BACK`  | 回滚完成           |
| `COMPLETED`    | 全部任务成功       |
| `FAILED`       | 任务失败或超时     |

---

## 工作流编排能力

| 能力                   | 说明                                        |
| ---------------------- | ------------------------------------------- |
| **串行**               | 不同 TaskGroup 之间顺序执行                 |
| **并行**               | 同一 TaskGroup 内所有任务并发执行           |
| **并行分支**           | 条件分支的多路径并行执行                    |
| **双层并发控制**       | 全局+per-workflow 最大并发，优先队列        |
| **条件分支**           | 任务输出分支名，走不同下游路径              |
| **条件跳过**           | 任务输出跳过组数，跳中间 TaskGroup          |
| **延迟执行**           | 设置 delay_sec，上游完成后等待              |
| **失败重试**           | 指数退避重试，可配 max_retries/base_backoff |
| **错误策略**           | 定制任务失败/超时行为（继续/跳过）          |
| **Saga 回滚**          | 失败后逆序回滚已完成任务                    |
| **人工审批**           | 输出 `__approval__` 后暂停等审批            |
| **定时触发**           | 支持 Cron 表达式定时执行                    |
| **熔断器**             | 失败达阈值后熔断，半开探测                  |
| **CAS 乐观锁**         | etcd CAS 替代互斥锁，高并发                 |
| **Fan-Out 分发**       | Watch→Worker Pool 并行处理                  |
| **增量快照**           | 仅记录变更任务，快照 O(1)                   |
| **持久化 Worker Pool** | 固定 32 goroutine 池，防 MySQL 爆炸         |
| **断连恢复**           | 断连后自动重连并扫描丢失任务                |
| **工作流优先级**       | 设置 priority 控制调度顺序                  |
| **子工作流**           | 调用另一个工作流作为子流程                  |
| **循环**               | 同一节点反复执行直到条件满足                |
| **Fan-Out 扇出**       | 对列表数据并行展开，每项独立处理            |
| **输入映射**           | 将上游输出字段映射到指定参数名              |

---

## 高级任务节点类型

### 子工作流（SubWorkflow）

任务节点可以通过 `sub_workflow` 字段调用另一个工作流作为子流程：

```json
{
  "name": "process_data",
  "sub_workflow": "data-pipeline",
  "sub_workflow_params": { "source": "{{.input.file}}", "format": "csv" }
}
```

子工作流共享父工作流的上下文，子工作流完成后父工作流继续执行。

### 循环（Loop）

任务节点可以配置循环条件，反复执行直到条件满足：

```json
{
  "name": "poll_status",
  "loop": {
    "max_iterations": 10,
    "condition_type": "output_match",
    "condition_key": "status"
  }
}
```

支持以下循环条件：

- `output_match`：任务输出匹配指定 key
- `timeout`：超时结束循环

### Fan-Out 扇出

对列表数据并行展开执行，每项独立处理：

```json
{
  "name": "process_batch",
  "fan_out": {
    "iterator_key": "items",
    "max_parallel": 5,
    "output_key": "results",
    "task": { "name": "process_item" }
  }
}
```

从 `taskCtx` 中读取 `iterator_key` 指定的数组，对每项并行执行 `task`，结果收集到 `output_key`。

### 并行分支（ParallelBranch）

条件分支的多个路径可同时执行：

```json
{
  "name": "dispatch",
  "branches": {
    "email": [[{ "name": "send_email" }]],
    "sms": [[{ "name": "send_sms" }]],
    "push": [[{ "name": "send_push" }]]
  },
  "parallel_branch": true
}
```

设置 `parallel_branch: true` 后，匹配的多个分支将并行执行而非串行。

### 输入映射（InputMapping）

将上游任务输出映射到下游任务的指定参数：

```json
{
  "name": "report",
  "input_mapping": {
    "content": "result.text",
    "title": "meta.title"
  }
}
```

`input_mapping` 的 key 为下游任务 `params` 中的参数名，value 为从上游 `taskCtx` 读取的路径。

---

## 错误策略（ErrorPolicy）

通过 `error_policy` 控制任务失败或超时时工作流的整体行为：

```json
{
  "error_policy": {
    "on_task_failure": "continue",
    "skippable_tasks": ["notification", "log_cleanup"],
    "task_policies": {
      "data_sync": "fail_workflow"
    }
  }
}
```

### 策略取值

| 取值                    | 说明                               |
| ----------------------- | ---------------------------------- |
| `fail_workflow`（默认） | 任务失败时终止工作流并标记 FAILED  |
| `continue`              | 跳过失败任务，工作流继续执行       |
| `skip_and_continue`     | 标记失败任务为跳过，工作流继续执行 |

### 配置优先级

全局 `on_task_failure` → `task_policies` 按任务名细粒度覆盖 → `skippable_tasks` 白名单。

---

## 工作流优先级

`Workflow.Priority` 控制调度队列中工作流的执行顺序，默认值为 0：

```go
wf := model.NewWorkflow("urgent-task")
wf.Priority = 10
```

优先级越高（数值越大），任务在调度队列中被优先下发执行。当 `max_concurrent_tasks` 全局并发限制生效时，高优先级工作流的任务插队到队列前端。|
