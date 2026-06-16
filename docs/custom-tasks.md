# 自定义任务开发

## 1. 实现 Task 接口

在 `plugin/builtin/` 下创建文件：

```go
package builtin

import (
    "context"
    "chihqiang/vibeflow/domain/model"
)

type MyTask struct{}

func NewMyTask() model.Task { return &MyTask{} }

func (t *MyTask) Name() string { return "my_task" }

func (t *MyTask) Params() []model.Param {
    return []model.Param{
        {Key: "input", Label: "输入内容", Type: model.ParamTypeText, Required: true},
    }
}

func (t *MyTask) Execute(ctx context.Context, paramCtx,
    taskCtx *model.Context) error {
    input, _ := paramCtx.GetString("input")
    // 处理逻辑...
    taskCtx.Set("result", "processed: "+input)
    return nil
}
```

### 参数类型

| 类型       | HTML 控件                 | 说明     |
| ---------- | ------------------------- | -------- |
| `text`     | `<input type="text">`     | 单行文本 |
| `number`   | `<input type="number">`   | 数字输入 |
| `password` | `<input type="password">` | 密码输入 |
| `email`    | `<input type="email">`    | 邮箱输入 |
| `textarea` | `<textarea>`              | 多行文本 |
| `select`   | `<select>`                | 下拉选择 |
| `checkbox` | `<input type="checkbox">` | 复选框   |
| `radio`    | `<input type="radio">`    | 单选框   |

### 支持回滚（Saga）

```go
func (t *MyTask) OnRollback(ctx context.Context, paramCtx,
    taskCtx *model.Context) error {
    // 补偿逻辑：撤销 Execute 的副作用
    return nil
}
```

---

## 2. 注册任务与工作流

在 `plugin/builtin/` 目录下，Master 侧和 Worker 侧分别注册：

### Master 侧（`plugin/builtin/master.go`）

注册任务类型和内置工作流：

```go
func (p *MasterPlugin) RegisterToMaster(sch *scheduler.Scheduler) {
    // 注册任务类型（供前端表单渲染）
    tasks := []model.Task{
        NewFetchURL(),
        NewWriteFile(),
        NewMyTask(),  // 添加新任务
    }
    for _, t := range tasks {
        sch.RegisterTaskType(&model.TaskType{
            Name:   t.Name(),
            Params: t.Params(),
        })
    }

    // 方式一：使用 model.NewWorkflow 编程式定义内置工作流
    workflows := []*model.Workflow{
        // 串行：A → B → C
        model.NewWorkflow("my-serial-wf").
            AddTaskGroup("task_a").
            AddTaskGroup("task_b").
            AddTaskGroup("task_c"),

        // 并行：A 和 B 同时执行 → C
        model.NewWorkflow("my-parallel-wf").
            AddTaskGroup("task_a", "task_b").
            AddTaskGroup("task_c"),
    }
    for _, wf := range workflows {
        sch.RegisterWorkflow(wf)
    }

    // 方式二：使用 plugin.WorkflowFromJSON 从 JSON 字面量注册
    plugin.WorkflowFromJSON(sch, []byte(`{
        "name": "my-json-wf",
        "trigger": "manual",
        "task_groups": [[{"name": "fetch_url"}]]
    }`))

    // 方式三：从 JSON 文件加载
    // plugin.WorkflowFromJSONFile(sch, "workflows/my-workflow.json")
}
```

### Worker 侧（`plugin/builtin/worker.go`）

注册任务处理器：

```go
func (p *WorkerPlugin) RegisterToWorker(w *runtime.Worker) {
    tasks := []model.Task{
        NewFetchURL(),
        NewWriteFile(),
        NewMyTask(),  // 添加新任务
    }
    for _, t := range tasks {
        w.RegisterTask(t)
    }
}
```

> **注意**：内置工作流使用 `model.NewWorkflow(name)` 创建，此时 name 同时作为 UUID
> （内置工作流固定不变，用 name 作唯一标识即可）。API 动态创建的工作流，UUID 自动生成，
> 与 name 无映射关系。`plugin.WorkflowFromJSON` 和 `plugin.WorkflowFromJSONFile` 是
> `infra/plugin/registry.go` 提供的便捷工具，支持以 JSON 格式定义工作流。
