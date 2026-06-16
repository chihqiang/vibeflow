package model

import (
	"context"
	"errors"
	"fmt"
)

// ErrNoRetry 表示一个不可重试的错误（参数错误、配置错误等），
// 任务处理器应使用此错误包装或直接返回此错误，调度器将不会重试该任务。
var ErrNoRetry = errors.New("no retry")

// WrapNoRetry 包装一个错误为不可重试错误，保留原始错误信息
func WrapNoRetry(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrNoRetry, err)
}

// ParamType 控件类型，对应 HTML input 类型
type ParamType string

const (
	ParamTypeText     ParamType = "text"     // 单行文本输入框 <input type="text">
	ParamTypeNumber   ParamType = "number"   // 数字输入框 <input type="number">
	ParamTypePassword ParamType = "password" // 密码输入框 <input type="password">
	ParamTypeEmail    ParamType = "email"    // 邮箱输入框 <input type="email">
	ParamTypeTextarea ParamType = "textarea" // 多行文本输入框 <textarea>
	ParamTypeSelect   ParamType = "select"   // 下拉选择框 <select>
	ParamTypeCheckbox ParamType = "checkbox" // 复选框 <input type="checkbox">
	ParamTypeRadio    ParamType = "radio"    // 单选框 <input type="radio">
)

// ParamOption 选项定义（用于 select / radio）
type ParamOption struct {
	Label string `json:"label"` // 显示文本
	Value string `json:"value"` // 实际值
}

// Param 任务自定义参数定义，描述一个表单控件的完整配置
type Param struct {
	Key         string        `json:"key"`                   // 参数键名，提交时作为 JSON key
	Label       string        `json:"label"`                 // 前端显示的标签文本
	Type        ParamType     `json:"type"`                  // 控件类型
	Required    bool          `json:"required"`              // 是否必填
	Default     any           `json:"default,omitempty"`     // 默认值
	Options     []ParamOption `json:"options,omitempty"`     // 选项列表（select / radio 使用）
	Placeholder string        `json:"placeholder,omitempty"` // 输入框占位提示文本
	Description string        `json:"description,omitempty"` // 帮助说明，展示在控件下方
}

// TaskType 注册到 Master 的任务类型描述
type TaskType struct {
	Name        string  `json:"name"`                  // 任务名称，与 Task.Name() 对应
	Description string  `json:"description,omitempty"` // 任务用途说明，展示在 UI 中
	Params      []Param `json:"params,omitempty"`      // 任务自定义参数定义，前端据此渲染表单
}

// Task 是 vibeflow 的核心任务接口
// 所有用户自定义的业务逻辑都必须实现此接口，通过 RegisterTask 注册到 Worker
type Task interface {
	// Execute 执行业务逻辑
	// ctx: Go 原生 context，用于超时控制和取消信号传递
	//     Worker 运行时会根据 TaskPayload.TimeoutSec 自动设置 context deadline
	// paramCtx: 用户自定义参数（来自 Workflow.TaskParams），前端根据 Params() 定义的控件渲染表单后收集
	//     这是静态配置数据，在工作流执行期间保持不变
	// taskCtx: 框架提供的上下文，用于读写任务之间的共享变量
	//     上游任务的输出通过 taskCtx 传递，任务执行完成后通过 taskCtx.Set 写入的输出会传递给后续任务
	// 返回 error: 执行失败时返回 error，Master 会根据重试策略决定是否重试
	Execute(ctx context.Context, paramCtx *Context, taskCtx *Context) error

	// Name 返回任务的唯一标识名称
	// 此名称在工作流定义中引用，必须与 Workflow.TaskGroups 中的条目对应
	// 全局注册时不可重复
	Name() string

	// Params 返回任务的自定义参数定义列表
	// 前端根据这些定义渲染表单控件（输入框、下拉框等）
	// 返回 nil 表示该任务无自定义参数
	Params() []Param
}

// Rollbackable 可选接口，任务实现了此接口则支持 Saga 补偿回滚
// 当工作流中后续任务失败时，框架会逆序调用已完成任务的 OnRollback
// taskCtx 包含当前任务执行时的原始输出，可用于撤销副作用
type Rollbackable interface {
	Task

	// OnRollback 执行补偿回滚逻辑
	// ctx: 用于超时控制
	// paramCtx: 任务执行时的原始参数（与 Execute 收到的相同）
	// taskCtx: 任务执行时的输出上下文（与 Execute 写入的相同）
	// 返回 error: 补偿失败时返回 error，框架会记录日志但继续回滚后续任务
	OnRollback(ctx context.Context, paramCtx *Context, taskCtx *Context) error
}

// ValidateTaskParams 校验任务的必选参数是否都有默认值
// 如果某个参数标记为 Required=true 但没有提供 Default 值，
// 运行时该参数可能缺失，Worker 启动时应拒绝注册该任务。
func ValidateTaskParams(task Task) error {
	for _, p := range task.Params() {
		if p.Required && p.Default == nil {
			return fmt.Errorf("任务 %q 的必选参数 %q（%s）缺少默认值", task.Name(), p.Key, p.Label)
		}
	}
	return nil
}
