# 使用案例

## 内置任务

Worker 内置两个示例任务：

| 任务         | 说明                       | 参数                      |
| ------------ | -------------------------- | ------------------------- |
| `fetch_url`  | 获取网页内容，存 `taskCtx` | `url`（必填，text）       |
| `write_file` | 从 `taskCtx` 读取写入文件  | `file_path`（必填，text） |

---

## 场景一：串行执行（数据管道）

**场景**：抓取网页 → 保存到本地。最经典的 ETL 迷你管道。

```text
fetch_url ──► write_file
```

**API 提交：**

```bash
curl -X POST http://localhost:8080/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "网页抓取",
    "task_groups": [
      [{"name": "fetch_url", "params": {"url": "https://www.example.com"}}],
      [{"name": "write_file", "params": {"file_path": "/tmp/output.html"}}]
    ],
    "trigger": "manual"
  }'
```

**代码注册：**

```go
model.NewWorkflow("fetch-and-save").
    AddTaskGroup("fetch_url").
    AddTaskGroup("write_file")
```

---

## 场景二：并行执行（多源聚合）

**场景**：同时抓取 3 个不同网站的内容，全部完成后保存到一个汇总文件。

```text
fetch_url (site A) ──┐
fetch_url (site B) ──┼──► write_file（汇总保存）
fetch_url (site C) ──┘
```

**API 提交：**

```bash
curl -X POST http://localhost:8080/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "多源聚合",
    "task_groups": [
      [
        {"name": "fetch_url", "params": {"url": "https://site-a.example.com"}},
        {"name": "fetch_url", "params": {"url": "https://site-b.example.com"}},
        {"name": "fetch_url", "params": {"url": "https://site-c.example.com"}}
      ],
      [{"name": "write_file", "params": {"file_path": "/tmp/aggregated.txt"}}]
    ],
    "trigger": "manual"
  }'
```

**代码注册：**

```go
model.NewWorkflow("multi-fetch").
    AddTaskGroup("fetch_url", "fetch_url", "fetch_url").
    AddTaskGroup("write_file")
```

---

## 场景三：失败重试与超时

**场景**：抓取一个不稳定的外部 API，配置 3 次指数退避重试，单任务最长 30 秒，整个工作流最长 5 分钟。

```text
fetch_url ──► write_file
   │ (失败自动重试，最多 3 次)
   └── 1s → 2s → 4s 退避
```

**API 提交：**

```bash
curl -X POST http://localhost:8080/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "带重试的抓取",
    "task_groups": [
      [{"name": "fetch_url", "params": {"url": "https://unstable-api.example.com"}}],
      [{"name": "write_file", "params": {"file_path": "/tmp/result.html"}}]
    ],
    "trigger": "manual",
    "max_retries": 3,
    "base_backoff": 1,
    "task_timeout_sec": 30,
    "timeout_sec": 300
  }'
```

**代码注册：**

```go
wf := model.NewWorkflow("retryable-fetch").
    AddTaskGroup("fetch_url").
    AddTaskGroup("write_file")
wf.MaxRetries = 3
wf.BaseBackoff = 1
wf.TaskTimeoutSec = 30
wf.TimeoutSec = 300
```

---

## 场景四：定时执行

**场景**：每天凌晨 2 点自动抓取日报数据。

```text
fetch_url ──► write_file
   ▲
   │ Cron: 0 2 * * *
```

**API 提交：**

```bash
curl -X POST http://localhost:8080/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "每日数据抓取",
    "task_groups": [
      [{"name": "fetch_url", "params": {"url": "https://daily-report.example.com"}}],
      [{"name": "write_file", "params": {"file_path": "/tmp/daily_report.html"}}]
    ],
    "trigger": "cron",
    "cron_expr": "0 2 * * *"
  }'
```

**代码注册：**

```go
wf := model.NewWorkflow("daily-fetch").
    AddTaskGroup("fetch_url").
    AddTaskGroup("write_file")
wf.Trigger = model.TriggerCron
wf.CronExpr = "0 2 * * *"
```

---

## 场景五：延迟执行

**场景**：抓取网页后等待 10 秒（模拟人工浏览），再保存结果。

```text
fetch_url ──► 等待 10s ──► write_file
```

**API 提交：**

```bash
curl -X POST http://localhost:8080/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "延迟保存",
    "task_groups": [
      [{"name": "fetch_url", "params": {"url": "https://www.example.com"}}],
      [{"name": "write_file", "params": {"file_path": "/tmp/output.html"},
        "delay_sec": 10}]
    ],
    "trigger": "manual"
  }'
```

---

## 场景六：条件分支

**场景**：抓取网页后，由 `check_content` 任务检查内容质量。根据检查结果，工作流自动走不同路径 — 内容有效则保存到正式目录，无效则保存到临时目录。

```text
                        ┌──► write_file（保存到正式目录）
fetch_url ──► check ──┤
                        └──► write_file（保存到临时目录）
```

### 工作原理

条件分支的核心机制分为两步：

**第一步：定义分支结构** — 在任务节点上声明 `branches` 字段，类型为
`map[string][][]TaskNode`，key 是分支名，value 是该分支的任务组序列。

```json
{
  "name": "check_content",
  "branches": {
    "valid":   [[{"name": "write_file", ...}]],   // valid 分支：1 个任务组
    "invalid": [[{"name": "write_file", ...}]]     // invalid 分支：1 个任务组
  }
}
```

每个分支可以包含**多个任务组**（串行执行），组内同样支持并行：

```json
{
  "branches": {
    "branch_a": [
      [{ "name": "task1" }, { "name": "task2" }], // 第 1 组：并行
      [{ "name": "task3" }] // 第 2 组：串行
    ]
  }
}
```

**第二步：运行时决策** — `check_content` 执行后，通过
`taskCtx.SetBranch("valid")` 告知框架走哪个分支。框架跳过 `branches` 节点之后
的所有原始 TaskGroup，直接执行选中分支的任务组。

```go
// check_content 任务的 Execute 方法
func (t *CheckTask) Execute(
    ctx context.Context, paramCtx, taskCtx *model.Context,
) error {
    content, _ := taskCtx.GetString("content")
    if isValid(content) {
        taskCtx.SetBranch("valid")     // 走 valid 分支
    } else {
        taskCtx.SetBranch("invalid")   // 走 invalid 分支
    }
    return nil
}
```

> **注意**：`SetBranch`、`SkipGroups`、`SetApproval` 互斥。多标记共存时优先级：
> **审批暂停 > 条件分支 > 条件跳过**。

### API 提交

```bash
curl -X POST http://localhost:8080/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "条件保存",
    "task_groups": [
      [{"name": "fetch_url", "params": {"url": "https://www.example.com"}}],
      [{
        "name": "check_content",
        "branches": {
          "valid":   [[{"name": "write_file", "params": {"file_path": "/data/正式/output.html"}}]],
          "invalid": [[{"name": "write_file", "params": {"file_path": "/tmp/临时/output.html"}}]]
        }
      }]
    ],
    "trigger": "manual"
  }'
```

### 代码注册

```go
wf := model.NewWorkflow("conditional-save").
    AddTaskGroup("fetch_url").
    AddTaskGroup(&model.TaskNode{
        Name: "check_content",
        Branches: model.BranchDef{
            "valid": {
                {model.TaskNode{
                    Name:   "write_file",
                    Params: map[string]any{"file_path": "/data/output.html"},
                }},
            },
            "invalid": {
                {model.TaskNode{
                    Name:   "write_file",
                    Params: map[string]any{"file_path": "/tmp/output.html"},
                }},
            },
        },
    })
```

### 执行流程图解

```text
工作流定义:  [fetch_url] → [check_content (含 branches)]  ← 原始 TaskGroups 在此结束
                                    │
                    taskCtx.SetBranch("valid")
                                    │
                                    ▼
分支 valid 的任务组:  [write_file → /data/output.html]  →  工作流完成

                    taskCtx.SetBranch("invalid")
                                    │
                                    ▼
分支 invalid 的任务组:  [write_file → /tmp/output.html]  →  工作流完成
```

> **关键理解**：声明 `branches` 的节点所在的 TaskGroup 是原始 TaskGroups 的
> **最后一组**。执行后框架不再按原始顺序推进，而是根据 `SetBranch` 的返回值，
> 跳入对应分支的任务组序列独立执行。

---

## 场景七：人工审批

**场景**：抓取网页后，由人工审核内容再决定是否保存。审核不通过则工作流直接失败。

```text
fetch_url ──► review（等待人工审批）──► write_file
                  │
                  └── 审批通过 → 继续
                  └── 审批驳回 → 工作流标记 FAILED
```

> 注：审批通过 `taskCtx.Set("__vibeflow_approval", ...)` 实现。`review` 输出审批
> 标记后工作流自动暂停，等待调用 `/api/v1/workflows/:uuid/approve` 或 `/reject`。

**API 提交：**

```bash
# 1. 提交工作流（review 任务执行后会进入 PAUSED 状态）
curl -X POST http://localhost:8080/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "审批流",
    "task_groups": [
      [{"name": "fetch_url", "params": {"url": "https://www.example.com"}}],
      [{"name": "review"}],
      [{"name": "write_file", "params": {"file_path": "/tmp/approved.html"}}]
    ],
    "trigger": "manual"
  }'

# 2. 审批通过（使用返回的 UUID）
curl -X POST http://localhost:8080/api/v1/workflows/<UUID>/approve

# 或驳回
curl -X POST http://localhost:8080/api/v1/workflows/<UUID>/reject
```

---

## 场景八：Webhook 事件触发

**场景**：外部系统通过 HTTP 请求触发工作流，传入动态参数。第一次调用注册工作流
定义（PENDING），后续每次 POST 即触发一次执行。

```text
外部系统 ──POST /api/v1/webhooks/:uuid──► Master ──► 新建执行实例 ──► fetch_url ──► write_file
```

**1. 注册 Webhook 工作流（只需一次）：**

```bash
curl -X POST http://localhost:8080/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Webhook 触发抓取",
    "task_groups": [
      [{"name": "fetch_url"}],
      [{"name": "write_file", "params": {"file_path": "/tmp/webhook_output.html"}}]
    ],
    "trigger": "event",
    "event_trigger": {
      "event_type": "webhook"
    }
  }'
```

返回的工作流 UUID 即为后续触发的地址。工作流状态为 PENDING，不会自动执行。

**2. 外部系统触发执行（可反复调用）：**

```bash
# 每次调用都会创建一次新的执行实例
curl -X POST http://localhost:8080/api/v1/webhooks/<UUID> \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {
      "url": "https://github.com/example/repo/archive/main.zip"
    }
  }'
```

Webhook 传入的 `payload` 会注入到工作流第一个任务（`fetch_url`）的 `params` 中，即该任务的 `url` 参数由外部动态指定。

**3. 查看触发历史：**

```bash
curl http://localhost:8080/api/v1/history/<UUID>
```

每次 Webhook 触发都会产生一条独立的执行记录，互不干扰。

**链式触发（工作流完成/任务失败时自动触发）：**

除了 Webhook，还支持工作流完成后自动触发下游工作流：

```bash
# 注册下游工作流：当上游工作流完成时自动触发
curl -X POST http://localhost:8080/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "下游处理",
    "task_groups": [
      [{"name": "write_file", "params": {"file_path": "/tmp/downstream.txt"}}]
    ],
    "trigger": "event",
    "event_trigger": {
      "event_type": "workflow_completed",
      "filter": "<上游工作流UUID>"
    }
  }'
```

---

## 场景九：组合拳（并行 + 重试 + 延迟 + 定时）

**场景**：每天早上 8 点并行抓取 2 个数据源，失败最多重试 2 次，抓取完成后等待 5 秒再保存。

```text
                     ┌──► fetch_url (源A)
Cron: 0 8 * * * ──► │                     ├──► 等待 5s ──► write_file
                     └──► fetch_url (源B)
```

**API 提交：**

```bash
curl -X POST http://localhost:8080/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "每日双源聚合",
    "task_groups": [
      [
        {"name": "fetch_url",
         "params": {"url": "https://source-a.example.com"}},
        {"name": "fetch_url",
         "params": {"url": "https://source-b.example.com"}}
      ],
      [{"name": "write_file",
        "params": {"file_path": "/tmp/daily.txt"},
        "delay_sec": 5}]
    ],
    "trigger": "cron",
    "cron_expr": "0 8 * * *",
    "max_retries": 2,
    "base_backoff": 1
  }'
```

**代码注册：**

```go
wf := model.NewWorkflow("daily-dual-fetch").
    AddTaskGroup("fetch_url", "fetch_url").
    AddTaskGroup("write_file")
wf.Trigger = model.TriggerCron
wf.CronExpr = "0 8 * * *"
wf.MaxRetries = 2
wf.BaseBackoff = 1
// 给 write_file 节点加延迟
wf.TaskGroups[1][0].DelaySec = 5
```

---

## 通过 API 提交后的操作

```bash
# 创建工作流，响应返回 UUID
# → {"code":0,"data":{"uuid":"<UUID>","name":"my-workflow"}}

# 查看运行状态
curl http://localhost:8080/api/v1/workflows/<UUID>

# 查看历史记录
curl http://localhost:8080/api/v1/history/<UUID>

# 取消运行中的工作流
curl -X DELETE http://localhost:8080/api/v1/workflows/<UUID>

# 重试已失败的工作流
curl -X POST http://localhost:8080/api/v1/workflows/<UUID>/retry
```
