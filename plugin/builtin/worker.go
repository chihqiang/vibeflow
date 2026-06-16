package builtin

import (
	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/engine/runtime"
)

// NewWorkerPlugin 返回 Worker 侧内置插件实例
func NewWorkerPlugin() *WorkerPlugin { return &WorkerPlugin{} }

// WorkerPlugin 实现 infra/plugin.WorkerPlugin 接口
type WorkerPlugin struct{}

func (p *WorkerPlugin) RegisterToWorker(w *runtime.Worker) {
	tasks := []model.Task{
		NewFetchURL(),
		NewWriteFile(),
	}
	for _, t := range tasks {
		w.RegisterTask(t)
	}
}
