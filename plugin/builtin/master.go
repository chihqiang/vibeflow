package builtin

import (
	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/engine/scheduler"
)

// NewMasterPlugin 返回 Master 侧内置插件实例
func NewMasterPlugin() *MasterPlugin { return &MasterPlugin{} }

// MasterPlugin 实现 infra/plugin.MasterPlugin 接口
type MasterPlugin struct{}

func (p *MasterPlugin) RegisterToMaster(sch *scheduler.Scheduler) {
	tasks := []model.Task{
		NewFetchURL(),
		NewWriteFile(),
	}
	for _, t := range tasks {
		sch.RegisterTaskType(&model.TaskType{
			Name:        t.Name(),
			Description: "",
			Params:      t.Params(),
		})
	}

	workflows := []*model.Workflow{
		model.NewWorkflow("builtin-fetch-and-save").AddTaskGroup("fetch_url").AddTaskGroup("write_file"),
	}
	for _, wf := range workflows {
		sch.RegisterWorkflow(wf)
	}
}
