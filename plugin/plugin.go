// Package plugin 插件汇总入口
// 新增插件只需：1) 新建一个包实现 infra/plugin 的接口 2) 在下方 import 和 Register 中各加一行
// 入口文件零代码改动。
package plugin

import (
	"chihqiang/vibeflow/infra/plugin"
	"chihqiang/vibeflow/plugin/builtin"
)

func init() {
	// 内置插件 — 新增插件在下方加一行即可
	plugin.Register(
		builtin.NewMasterPlugin(),
		builtin.NewWorkerPlugin(),
		// 通过 JSON 文件注册工作流（在插件的 RegisterToMaster 方法中使用）：
		// plugin.WorkflowFromJSONFile(sch, "workflows/my_workflow.json"),
	)
}
