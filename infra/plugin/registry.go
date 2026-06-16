// Package plugin 提供插件注册表的基础设施：接口定义与全局注册/应用逻辑
package plugin

import (
	"encoding/json"
	"fmt"
	"os"

	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/engine/runtime"
	"chihqiang/vibeflow/infra/engine/scheduler"
)

// MasterPlugin Master 侧插件接口：负责注册 TaskType 和 Workflow 定义
type MasterPlugin interface {
	RegisterToMaster(sch *scheduler.Scheduler)
}

// WorkerPlugin Worker 侧插件接口：负责注册 Task 处理器
type WorkerPlugin interface {
	RegisterToWorker(w *runtime.Worker)
}

// Plugin 同时实现 Master 和 Worker 两侧注册的插件
type Plugin interface {
	MasterPlugin
	WorkerPlugin
}

// --- 全局注册表 ---

var (
	masterPlugins []MasterPlugin
	workerPlugins []WorkerPlugin
)

// Register 注册一个或多个插件到全局注册表
// 每个参数可以同时实现 MasterPlugin、WorkerPlugin、或 Plugin 接口
func Register(plugins ...any) {
	for _, p := range plugins {
		if mp, ok := p.(MasterPlugin); ok {
			masterPlugins = append(masterPlugins, mp)
		}
		if wp, ok := p.(WorkerPlugin); ok {
			workerPlugins = append(workerPlugins, wp)
		}
	}
}

// ApplyToMaster 将所有已注册的 Master 插件应用到 Scheduler
func ApplyToMaster(sch *scheduler.Scheduler) {
	for _, p := range masterPlugins {
		p.RegisterToMaster(sch)
	}
}

// ApplyToWorker 将所有已注册的 Worker 插件应用到 Worker
func ApplyToWorker(w *runtime.Worker) {
	for _, p := range workerPlugins {
		p.RegisterToWorker(w)
	}
}

// WorkflowFromJSON 将 JSON 字节切片解析为 model.Workflow 并直接注册到 Scheduler
// 支持的 JSON 格式与 POST /api/v1/workflows 请求体一致：
//
//	{
//	  "name": "my-workflow",
//	  "task_groups": [[{"name":"task1","params":{"k":"v"}}, {"name":"task2"}]],
//	  "trigger": "manual",
//	  "cron_expr": "",
//	  "timeout_sec": 0,
//	  "task_timeout_sec": 0,
//	  "max_retries": 0,
//	  "base_backoff": 0
//	}
//
// 常用于插件中通过 JSON 字面量快速定义并注册工作流，例如：
//
//	plugin.RegisterToMaster = func(sch *scheduler.Scheduler) {
//	    plugin.WorkflowFromJSON(sch, []byte(`{"name":"demo","trigger":"manual","task_groups":[[{"name":"fetch_url"}]]}`))
//	}
func WorkflowFromJSON(sch *scheduler.Scheduler, data []byte) error {
	var wf model.Workflow
	if err := json.Unmarshal(data, &wf); err != nil {
		return fmt.Errorf("plugin: 解析工作流 JSON 失败: %w", err)
	}
	if wf.Name == "" {
		return fmt.Errorf("plugin: 工作流 JSON 缺少必填字段 name")
	}
	if len(wf.TaskGroups) == 0 {
		return fmt.Errorf("plugin: 工作流 JSON 缺少必填字段 task_groups")
	}
	sch.RegisterWorkflow(&wf)
	return nil
}

// WorkflowFromJSONFile 从 JSON 文件路径读取工作流定义并注册到 Scheduler
// 先读取文件内容，再调用 WorkflowFromJSON 解析并注册。
// 常用于从外部 JSON 配置文件加载工作流定义，例如：
//
//	if err := plugin.WorkflowFromJSONFile(sch, "workflows/demo.json"); err != nil {
//	    log.Fatal(err)
//	}
func WorkflowFromJSONFile(sch *scheduler.Scheduler, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("plugin: 读取工作流文件 %s 失败: %w", path, err)
	}
	return WorkflowFromJSON(sch, data)
}
