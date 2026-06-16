# 高可用与性能保障

## 熔断器（Circuit Breaker）

为 MySQL 和 etcd 等外部依赖提供熔断保护（`infra/circuit`），防止级联故障拖垮整个系统。

### 三态模型

```text
              连续失败达到阈值
    Closed ──────────────────► Open
      ▲                          │
      │                    冷却期到期
      │                          ▼
      └─────────────────── HalfOpen
                          │
                    探测失败 → 回到 Open
                    探测成功 → 回到 Closed
```

| 状态                 | 行为                                         |
| -------------------- | -------------------------------------------- |
| **Closed（闭合）**   | 正常放行，累计连续失败次数                   |
| **Open（打开）**     | 直接拒绝请求（快速失败），等待冷却期         |
| **HalfOpen（半开）** | 允许一次探测请求；成功 → Closed，失败 → Open |

### 配置参数

| 参数          | Worker etcd | Master MySQL | 说明                 |
| ------------- | ----------- | ------------ | -------------------- |
| `maxFailures` | 5 次        | 5 次         | 连续失败多少次后熔断 |
| `cooldown`    | 15s         | 15s          | 熔断后冷却时间       |

### 应用场景

- **MySQL 持久化**：MySQL 不可用时跳过快照和状态写入，避免 goroutine 堆积
- **Worker 状态上报和心跳**：etcd 不可用时跳过上报，Master 超时机制兜底

---

## CAS 乐观锁

Worker 抢占任务使用 etcd CAS 替代传统互斥锁
（`infra/engine/runtime/cas_lock.go`），大幅减少 etcd 资源开销。

### 工作原理

```text
1. 读取任务 key → 获取当前值和 ModRevision（etcd 全局递增版本号）
2. 修改状态 PENDING → RUNNING
3. 使用 etcd Txn 原子写入：if ModRevision == expectedRevision then Put else fail
4. 成功 → 本 Worker 获得执行权
5. 失败 → 其他 Worker 已抢先，放弃或重试（最多 3 次，随机退避 10-50ms）
```

### 两种锁模式对比

| 特性      | 互斥锁（Mutex）             | CAS 乐观锁             |
| --------- | --------------------------- | ---------------------- |
| etcd 资源 | Session + Lease（常驻连接） | 仅 Txn（无状态）       |
| 开销      | 每次锁操作创建 session      | 一次读 + 一次 Txn      |
| 适用场景  | 低并发（语义清晰）          | 高并发（每秒数千任务） |
| 死锁风险  | Lease TTL 自动释放          | 无死锁（无长期占用）   |

### 状态上报的 CAS

任务执行完毕后，Worker 使用存储的 `EtcdModRevision` 做 CAS 写入 COMPLETED/FAILED。版本冲突时读取最新值判断：

- 已是终态 → 其他 Worker 已完成，放弃上报
- 状态异常 → 由 Master watchdog 超时机制兜底（不做无条件覆盖）

### etcd Key 清理

任务完成后（COMPLETED/FAILED），Master 自动删除 etcd 中的任务 key
（`/vibeflow/tasks/<workflowID>/<taskName>`），减少 etcd 存储压力。删除失败仅打
warning 日志，不影响工作流进度（etcd 自带 24h TTL 兜底清理）。

---

## Fan-Out 分发

Master 的全局任务 Watch 采用 Fan-Out 分发模型
（`infra/engine/scheduler/watch.go`），消除单 goroutine 串行处理瓶颈。

### 架构

```text
                         ┌──────────────────────────────────┐
                         │      主 Watch goroutine           │
                        │  仅做轻量 workflowUUID 提取       │
                        │  （字符串扫描，不反序列化）        │
                        └──────────┬───────────────────────┘
                                   │
                        fan-out channel（缓冲 8192）
                                   │
              ┌────────┬─────────┼─────────┬────────┐
              ▼        ▼         ▼         ▼        ▼
          Worker 0  Worker 1  Worker 2  ...  Worker 7
          反序列化 + dispatch    （共 8 个 worker）
              │        │         │                  │
              ▼        ▼         ▼                  ▼
          ┌───────────────────────────────────────────┐
          │    workflowWatchers（per-workflow channel）│
          │  每个 workflow 独立 channel（缓冲 4096）   │
          └───────────────────────────────────────────┘
```

### 关键参数

| 参数                   | 值    | 说明                              |
| ---------------------- | ----- | --------------------------------- |
| `fanOutWorkers`        | 8     | Fan-Out worker pool 大小          |
| `fanOutChanBuf`        | 8192  | Fan-Out channel 缓冲区            |
| `workflowEventChanBuf` | 4096  | per-workflow channel 缓冲区       |
| `maxBacklogSize`       | 2048  | 积压队列最大长度                  |
| `dispatchBlockTimeout` | 200ms | per-workflow channel 阻塞发送超时 |

### 三级缓冲策略（防止事件丢失）

1. **直接发送**：优先尝试发送到 per-workflow channel（200ms 阻塞窗口）
2. **积压队列**：channel 满时暂存到 backlog（定时重试，最大 2048 条）
3. **断连恢复**：Watch 重连后执行全量 PENDING 任务扫描，补偿遗漏事件

---

## 增量快照（Incremental Snapshot）

运行中工作流的快照采用增量模式（`infra/engine/scheduler/repository.go`），仅记录自上次快照以来变更的任务。

### 传统快照 vs 增量快照

| 对比项           | 全量快照          | 增量快照            |
| ---------------- | ----------------- | ------------------- |
| 每次写入量       | O(总任务数)       | O(本轮变更数)       |
| 100 个任务完成时 | 序列化 100 个任务 | 仅序列化 1 个 delta |
| MySQL 写入频率   | 每次任务完成/失败 | 节流 5s 一次        |
| 恢复精度         | 精确              | 最多丢失 5s 增量    |

### 增量数据结构

```go
type TaskDelta struct {
    TaskName string         `json:"task_name"`
    Action   string         `json:"action"`    // "completed" / "failed" / "rolled_back"
    Output   map[string]any `json:"output,omitempty"`
    Result   string         `json:"result,omitempty"`
}

type WorkflowSnapshot struct {
    WorkflowUUID string      `json:"workflow_uuid"`
    ExecutionID  uint        `json:"execution_id"`
    Status       string      `json:"status"`
    ChangedTasks []TaskDelta `json:"changed_tasks,omitempty"`  // 仅增量
    Error        string      `json:"error,omitempty"`
    UpdatedAt    time.Time   `json:"updated_at"`
}
```

### 去重机制

通过 `state.Snapshotted` map 追踪已快照的任务 + `SnapshotSeq` 版本号，避免重复序列化。任务重试时自动清除旧快照标记。

### 节流间隔

5 秒（`snapshotThrottleInterval`），高并发下任务密集完成时有效减少 MySQL 写入压力。

### 恢复流程

1. 从 MySQL 加载 WorkflowState（包含基础信息）
2. 批量加载所有增量快照（一次 SQL 查询，`BatchLoadTaskDeltas`）
3. 将 deltas 应用到 state（`ApplyDeltas`）
4. 基于 state.CompletedTasks 判断恢复进度，从断点继续执行
5. 并行恢复最多 10 个工作流（`maxParallelRecovery`）

---

## 持久化 Worker Pool

MySQL 持久化使用固定大小的 goroutine 池
（`infra/engine/scheduler/repository.go`），替代 `go func()` 逐次创建方案。

### 异步持久化架构

```text
  persistToMySQL()  ──►  submit()  ──►  jobs channel（缓冲 128）
                                            │
                              ┌─────────────┼─────────────┐
                              ▼             ▼             ▼
                           worker 0      worker 1  ...  worker 31
                           (共 32 个 goroutine，生命周期与 Scheduler 相同)
```

### 持久化关键参数

| 参数                    | 值                    | 说明             |
| ----------------------- | --------------------- | ---------------- |
| `persistWorkerPoolSize` | 32                    | 固定 worker 数量 |
| channel 缓冲            | 128（4×pool 大小）    | 平滑瞬时突发     |
| `persistEnqueueTimeout` | 5s                    | 入队超时则丢弃   |
| 重试策略                | 3 次+退避（500ms 起） | 每个任务独立重试 |

### 降级策略

池满或超时时丢弃本次持久化（非关键路径可降级，etcd 中有完整运行时状态）。配合熔断器，MySQL 连续不可用时直接跳过。
