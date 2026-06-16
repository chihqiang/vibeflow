package model

import (
	"encoding/json"
	"fmt"
	"time"
)

// TriggerType 工作流触发方式
type TriggerType string

const (
	TriggerManual TriggerType = "manual" // 手动触发
	TriggerCron   TriggerType = "cron"   // 定时触发（Cron）
	TriggerEvent  TriggerType = "event"  // 事件触发（Webhook / 工作流完成 / 任务失败）
)

// EventTriggerType 事件触发器类型
// 支持三种事件来源：外部 Webhook、工作流完成、任务失败
type EventTriggerType string

const (
	EventTriggerWebhook           EventTriggerType = "webhook"             // 外部系统通过 HTTP POST 触发
	EventTriggerWorkflowCompleted EventTriggerType = "workflow_completed" // 指定工作流完成后自动触发
	EventTriggerTaskFailed        EventTriggerType = "task_failed"        // 指定工作流中任务失败时触发
)

// EventTrigger 事件触发定义
// 当 Workflow.Trigger == TriggerEvent 时使用
type EventTrigger struct {
	EventType     EventTriggerType `json:"event_type"`              // 事件类型：webhook / workflow_completed / task_failed
	Filter        string           `json:"filter,omitempty"`        // 过滤条件（源工作流 UUID 或 workflow_uuid:task_name）
	WebhookSecret string           `json:"webhook_secret,omitempty"` // Webhook 签名密钥（可选）
}

// WorkflowStatus 工作流运行状态枚举
type WorkflowStatus string

const (
	WorkflowStatusPending      WorkflowStatus = "PENDING"       // 已注册/已保存，等待执行
	WorkflowStatusRunning      WorkflowStatus = "RUNNING"       // 正在执行
	WorkflowStatusPaused       WorkflowStatus = "PAUSED"        // 暂停中：等待人工审批或外部信号
	WorkflowStatusRollingBack  WorkflowStatus = "ROLLING_BACK"  // 补偿回滚中（Saga）
	WorkflowStatusRolledBack   WorkflowStatus = "ROLLED_BACK"   // 补偿回滚完成
	WorkflowStatusCompleted    WorkflowStatus = "COMPLETED"     // 全部任务执行成功
	WorkflowStatusFailed       WorkflowStatus = "FAILED"        // 任务执行失败或超时
)

// BranchDef 条件分支定义
// 分支名到任务组序列的映射，每个分支是一组 TaskGroups
// 例如：{"file_exists": [[{task2}]], "file_missing": [[{task3}]]}
type BranchDef map[string][][]TaskNode

// BranchLocation 记录分支任务在 Workflow.TaskGroups 中的精确位置
// 用于 O(1) 查找分支任务所属的父节点和分支名，避免 AllTaskNames 等场景的多层嵌套遍历
type BranchLocation struct {
	ParentGroupIdx int    // 父 TaskNode 所在的 group 索引
	ParentNodeName string // 父 TaskNode 的名称（定义了 Branches 的节点）
	BranchName     string // 分支名
}

// LoopConditionType 循环条件判断方式
type LoopConditionType string

const (
	// LoopConditionAlways 无条件循环直到达到 MaxIterations
	LoopConditionAlways LoopConditionType = "always"
	// LoopConditionKey 根据 taskCtx 中指定 key 的 bool 值决定是否继续
	LoopConditionKey LoopConditionType = "key"
)

// LoopDef 循环执行定义
// 附加在 TaskNode 上，表示该任务需要循环执行
type LoopDef struct {
	MaxIterations int               `json:"max_iterations"`         // 最大迭代次数（防止死循环，必填，>0）
	ConditionType LoopConditionType `json:"condition_type"`         // 条件判断方式：always / key
	ConditionKey  string            `json:"condition_key,omitempty"` // ConditionType=key 时，从 taskCtx 读取 bool 值决定是否继续
}

// FanOutDef Fan-Out 动态并行定义
// 上游输出一个列表，Fan-Out 为每个元素并行执行一个任务模板，全部完成后汇聚结果
type FanOutDef struct {
	IteratorKey string     `json:"iterator_key"`                // 从上游输出中取列表的 key
	Task        TaskNode   `json:"task"`                         // 每个元素执行的任务模板
	MaxParallel int        `json:"max_parallel"`                  // 最大并行度，0 表示不限制
	OutputKey   string     `json:"output_key,omitempty"`          // 汇聚结果的 key，默认 "fan_out_results"
}

// TaskNodeType 任务节点类型枚举
type TaskNodeType string

const (
	// TaskNodeTypeTask 普通任务节点
	TaskNodeTypeTask TaskNodeType = "task"
	// TaskNodeTypeSubWorkflow 子工作流节点
	TaskNodeTypeSubWorkflow TaskNodeType = "sub_workflow"
	// TaskNodeTypeFanOut Fan-Out 动态并行节点
	TaskNodeTypeFanOut TaskNodeType = "fan_out"
)

// GetTaskNodeType 返回 TaskNode 的实际类型，空值默认为 task（向后兼容）
func GetTaskNodeType(node TaskNode) TaskNodeType {
	if node.Type != "" {
		return node.Type
	}
	return TaskNodeTypeTask
}

// DefaultFanOutOutputKey Fan-Out 汇聚结果的默认 key
const DefaultFanOutOutputKey = "fan_out_results"

// LoopContinueKey 循环继续条件的框架保留键名
// 任务通过 taskCtx.Set(LoopContinueKey, true/false) 控制是否继续下一次循环
const LoopContinueKey = "__vibeflow_loop_continue"

// LoopIterationKey 循环迭代计数器的框架保留键名
// 框架自动设置当前迭代索引（从 0 开始），任务可通过 taskCtx.Get 读取
const LoopIterationKey = "__vibeflow_loop_iteration"

// TaskNode 工作流中的一个任务节点
// 支持三种类型：普通任务（默认）、子工作流引用、Fan-Out 动态并行
// 同一任务类型在同一个工作流中可出现多次，每个实例有独立的参数，互不覆盖
type TaskNode struct {
	// Type 节点类型：task（默认）、sub_workflow、fan_out
	// 为空时默认为 "task"，保持向后兼容
	Type             TaskNodeType    `json:"type,omitempty"`
	Name             string          `json:"name"`                         // 任务类型名称（type=task 时与 Task.Name() 对应）
	Params           map[string]any  `json:"params,omitempty"`               // 该任务实例的初始参数
	DelaySec         int64           `json:"delay_sec,omitempty"`            // 延迟执行秒数：上游任务完成后等待指定秒数再执行本任务，0 表示立即执行
	Branches         BranchDef       `json:"branches,omitempty"`             // 条件分支定义：key=分支名，value=该分支的 TaskGroups
	DefaultBranch    string          `json:"default_branch,omitempty"`       // 默认分支（当无匹配时执行），增强条件分支网关
	ParallelBranch  bool            `json:"parallel_branch,omitempty"`      // 是否并行执行所有匹配的分支（增强条件分支网关）
	SubWorkflow      string          `json:"sub_workflow,omitempty"`         // 子工作流 UUID 引用（type=sub_workflow 时使用）
	SubWorkflowParams map[string]any `json:"sub_workflow_params,omitempty"`  // 子工作流传入参数（type=sub_workflow 时使用）
	FanOut           *FanOutDef      `json:"fan_out,omitempty"`              // Fan-Out 动态并行定义（type=fan_out 时使用）
	Loop             *LoopDef        `json:"loop,omitempty"`                 // 循环定义
	InputMapping     map[string]string `json:"input_mapping,omitempty"`     // 输入映射：key=本任务参数名, value=上游输出变量名
}

// ErrorPolicyType 错误处理策略类型
type ErrorPolicyType string

const (
	// ErrorPolicyRetry 任务失败时重试（默认行为，使用工作流的 MaxRetries/BaseBackoff）
	ErrorPolicyRetry ErrorPolicyType = "retry"
	// ErrorPolicyRollback 任务失败时触发 Saga 回滚
	ErrorPolicyRollback ErrorPolicyType = "rollback"
	// ErrorPolicySkip 任务失败时跳过该任务继续执行后续任务组
	ErrorPolicySkip ErrorPolicyType = "skip"
	// ErrorPolicyFailFast 任务失败时立即终止整个工作流（不回滚）
	ErrorPolicyFailFast ErrorPolicyType = "fail_fast"
)

// ErrorPolicy 全局错误处理策略
// 在 Workflow 级别定义任务失败时的处理方式，替代固定的 retry+rollback 策略
// 当 ErrorPolicy 为 nil 时，回退到原有行为（retry 耗尽后 rollback）
type ErrorPolicy struct {
	// OnTaskFailure 任务失败时的默认策略：retry | rollback | skip | fail_fast
	OnTaskFailure ErrorPolicyType `json:"on_task_failure,omitempty"`
	// OnTimeout 工作流超时时的策略：rollback | fail（默认 rollback）
	OnTimeout string `json:"on_timeout,omitempty"`
	// SkippableTasks 失败后可跳过的任务名列表（仅 OnTaskFailure=skip 时生效）
	// 空列表表示所有任务都可以跳过；非空列表表示只有指定任务可以跳过
	SkippableTasks []string `json:"skippable_tasks,omitempty"`
	// TaskPolicies 特定任务的自定义策略（key=任务名, value=策略类型）
	// 覆盖 OnTaskFailure 的全局策略，允许为关键任务指定不同策略
	TaskPolicies map[string]ErrorPolicyType `json:"task_policies,omitempty"`
}

// GetTaskErrorPolicy 获取指定任务的实际错误处理策略
// 优先使用 TaskPolicies 中的自定义策略，否则使用 OnTaskFailure 全局策略
func (ep *ErrorPolicy) GetTaskErrorPolicy(taskName string) ErrorPolicyType {
	if ep == nil {
		return ErrorPolicyRetry // 默认行为
	}
	if policy, ok := ep.TaskPolicies[taskName]; ok {
		return policy
	}
	if ep.OnTaskFailure != "" {
		return ep.OnTaskFailure
	}
	return ErrorPolicyRetry // 默认行为
}

// IsSkippable 判断任务失败后是否可以跳过
// 仅当 OnTaskFailure=skip 且任务在 SkippableTasks 列表中（或列表为空表示所有任务可跳过）时返回 true
func (ep *ErrorPolicy) IsSkippable(taskName string) bool {
	if ep == nil {
		return false
	}
	if ep.OnTaskFailure != ErrorPolicySkip {
		return false
	}
	if len(ep.SkippableTasks) == 0 {
		return true // 空列表表示所有任务可跳过
	}
	for _, name := range ep.SkippableTasks {
		if name == taskName {
			return true
		}
	}
	return false
}

// GetTimeoutPolicy 获取超时策略
func (ep *ErrorPolicy) GetTimeoutPolicy() string {
	if ep == nil || ep.OnTimeout == "" {
		return "rollback" // 默认超时回滚
	}
	return ep.OnTimeout
}

// Validate 校验错误处理策略的合法性
func (ep *ErrorPolicy) Validate() error {
	if ep == nil {
		return nil
	}
	switch ep.OnTaskFailure {
	case "", ErrorPolicyRetry, ErrorPolicyRollback, ErrorPolicySkip, ErrorPolicyFailFast:
		// 合法
	default:
		return fmt.Errorf("无效的 on_task_failure 策略: %q", ep.OnTaskFailure)
	}
	switch ep.OnTimeout {
	case "", "rollback", "fail":
		// 合法
	default:
		return fmt.Errorf("无效的 on_timeout 策略: %q", ep.OnTimeout)
	}
	for taskName, policy := range ep.TaskPolicies {
		switch policy {
		case ErrorPolicyRetry, ErrorPolicyRollback, ErrorPolicySkip, ErrorPolicyFailFast:
			// 合法
		default:
			return fmt.Errorf("任务 %q 的自定义策略无效: %q", taskName, policy)
		}
	}
	return nil
}

// Workflow 工作流定义
// TaskGroups 支持串并行混合编排：
//   - [[{name:"task1"}], [{name:"task2"}, {name:"task3"}]] 表示 task1 执行完后，task2 和 task3 并行执行
//   - [[{name:"task1"}], [{name:"task2"}], [{name:"task3"}]] 表示纯串行模式
type Workflow struct {
	UUID           string        `json:"uuid"`                  // UUID 唯一标识符，创建时自动生成
	Name           string        `json:"name"`                  // 工作流名称
	TaskGroups     [][]TaskNode  `json:"task_groups,omitempty"` // 任务分组：组内并行，组间串行
	Trigger        TriggerType   `json:"trigger"`               // 触发方式：manual / cron / event
	CronExpr       string        `json:"cron_expr,omitempty"`   // Cron 表达式，trigger=cron 时必填
	EventTrigger   *EventTrigger `json:"event_trigger,omitempty"` // 事件触发定义，trigger=event 时必填
	TimeoutSec     int64         `json:"timeout_sec"`           // 工作流整体超时秒数，超时后自动标记失败（0 表示不限制）
	TaskTimeoutSec int64         `json:"task_timeout_sec"`      // 任务默认超时秒数，可被任务级别配置覆盖（0 表示不限制）
	MaxRetries     int           `json:"max_retries"`           // 任务默认重试次数，可被任务级别配置覆盖
	BaseBackoff    int64         `json:"base_backoff"`          // 任务默认基础退避秒数，指数退避算法：base * 2^retryCount
	Priority       int           `json:"priority,omitempty"`    // 工作流优先级，数字越大越优先，默认 0；影响任务排队顺序
	ErrorPolicy     *ErrorPolicy   `json:"error_policy,omitempty"`   // 全局错误处理策略
}

// AllTaskNames 返回工作流中所有任务的扁平列表（仅名称），按 TaskGroups 定义顺序排列
// 包含分支中定义的任务
// 注意：子工作流和 Fan-Out 节点本身不返回名称（它们的内部任务由子工作流独立管理），
// 但子工作流的 Name（用作父工作流中的标识符）会包含在返回列表中
func (wf *Workflow) AllTaskNames() []string {
	// 预估容量，减少扩容开销
	estimated := 0
	for _, g := range wf.TaskGroups {
		estimated += len(g) * 3 // 每个节点平均贡献约 3 个名称（含分支）
	}
	names := make([]string, 0, estimated)
	for _, g := range wf.TaskGroups {
		for _, n := range g {
			// 子工作流节点：Name 是父工作流中的标识符
			if GetTaskNodeType(n) == TaskNodeTypeSubWorkflow {
				if n.Name != "" {
					names = append(names, n.Name)
				}
			} else if GetTaskNodeType(n) == TaskNodeTypeFanOut {
				// Fan-Out 节点：Name 是父工作流中的标识符
				if n.Name != "" {
					names = append(names, n.Name)
				}
				// Fan-Out 模板中的任务名称也需收集（运行时动态创建的任务实例使用此名称）
				if n.FanOut != nil && n.FanOut.Task.Name != "" {
					names = append(names, n.FanOut.Task.Name)
				}
			} else {
				names = append(names, n.Name)
			}

			for _, branchGroups := range n.Branches {
				for _, bg := range branchGroups {
					for _, bn := range bg {
						names = append(names, bn.Name)
					}
				}
			}
		}
	}
	return names
}

// AllTaskNodes 返回工作流中所有任务节点的扁平列表
func (wf *Workflow) AllTaskNodes() []TaskNode {
	var nodes []TaskNode
	for _, g := range wf.TaskGroups {
		nodes = append(nodes, g...)
	}
	return nodes
}

// BuildTaskGroupIndex 构建 taskName → groupIdx 的 O(1) 查找表
// 用于 startSagaRollback、pauseForApproval 等需要从任务名反查 group 索引的场景
func (wf *Workflow) BuildTaskGroupIndex() map[string]int {
	idx := make(map[string]int, len(wf.TaskGroups)*2) // 预估容量
	for gi, g := range wf.TaskGroups {
		for _, node := range g {
			idx[node.Name] = gi
		}
	}
	return idx
}

// BuildBranchTaskIndex 构建分支任务名 → BranchLocation 的 O(1) 查找表
// 遍历 TaskGroups → Branches → branchGroups，将结果缓存为 map，
// 后续 AllTaskNames / 分支任务查找无需重复遍历
func (wf *Workflow) BuildBranchTaskIndex() map[string]BranchLocation {
	idx := make(map[string]BranchLocation)
	for gi, g := range wf.TaskGroups {
		for _, node := range g {
			if node.Branches == nil {
				continue
			}
			for branchName, branchGroups := range node.Branches {
				for _, bg := range branchGroups {
					for _, bn := range bg {
						idx[bn.Name] = BranchLocation{
							ParentGroupIdx: gi,
							ParentNodeName: node.Name,
							BranchName:     branchName,
						}
					}
				}
			}
		}
	}
	return idx
}

// AllTaskNamesCached 基于缓存的索引快速收集所有任务名（主任务 + 分支任务）
// 避免每次都做深层嵌套遍历
func (wf *Workflow) AllTaskNamesCached(groupIndex map[string]int, branchIndex map[string]BranchLocation) []string {
	names := make([]string, 0, len(groupIndex)+len(branchIndex))
	for name := range groupIndex {
		names = append(names, name)
	}
	for name := range branchIndex {
		names = append(names, name)
	}
	return names
}

// WorkflowState 工作流运行时状态，包含工作流定义和当前的执行进度
type WorkflowState struct {
	Workflow         *Workflow                 `json:"workflow"`                    // 工作流定义
	Status           WorkflowStatus            `json:"status"`                      // 运行状态
	StartedAt        time.Time                 `json:"started_at"`                  // 提交时间
	CompletedTasks   map[string]map[string]any `json:"completed_tasks"`             // 已完成的任务及其输出，key = taskName
	FailedTasks      map[string]string         `json:"failed_tasks"`                // 已失败的任务及其错误信息，key = taskName
	RolledBack       map[string]bool           `json:"rolled_back,omitempty"`       // 已回滚的任务，key = taskName
	Error            string                    `json:"error,omitempty"`             // 工作流级错误信息（如超时、用户取消）
	PausedTaskName   string                    `json:"paused_task_name,omitempty"`  // 暂停等待审批的任务名（PAUSED 状态时有值）
	PausedTaskOutput map[string]any            `json:"paused_task_output,omitempty"` // 暂停任务的输出（含审批消息、控制标记等）
	PausedGroupIdx   int                       `json:"paused_group_idx,omitempty"`  // 暂停任务所在 group 索引（用于恢复时查找下一组）
	TaskGroupIndex   map[string]int            `json:"task_group_index,omitempty"`   // O(1) 查找：taskName → groupIdx（主 TaskGroups 的索引）
	BranchTaskIndex  map[string]BranchLocation `json:"branch_task_index,omitempty"`  // O(1) 查找：分支任务名 → 所属的父节点和分支名
	WorkflowUUID     string                    `json:"workflow_uuid"`                // 工作流 UUID，与 Workflow.UUID 一致
	WorkflowID       uint                      `json:"workflow_id"`                  // MySQL vibeflow_workflows 主键
	ExecutionID      uint                      `json:"execution_id"`                // MySQL vibeflow_executions 主键

	// 增量快照追踪（运行时字段，不持久化到 JSON）
	SnapshotSeq  uint64            `json:"-"` // 快照版本号，每次 collectTaskDeltasLocked 后递增
	Snapshotted  map[string]uint64 `json:"-"` // 任务名 → 已快照时的 SnapshotSeq，用于判断增量变更
}

// ============================================================================
// 增量快照类型 — 避免每次任务完成都序列化整个 WorkflowState
// ============================================================================

// TaskDelta 单个任务的变更记录，仅记录变化的任务而非整个 WorkflowState
// 快照大小从 O(总任务数) 降为 O(1)
type TaskDelta struct {
	TaskName string         `json:"task_name"`
	Action   string         `json:"action"` // "completed" | "failed" | "rolled_back"
	Output   map[string]any `json:"output,omitempty"`
	Result   string         `json:"result,omitempty"`
}

// WorkflowSnapshot 工作流增量快照，仅包含状态和变更部分
// 工作流结束时 PersistWorkflowLocked 仍写入完整快照，运行时仅写增量
type WorkflowSnapshot struct {
	WorkflowUUID string       `json:"workflow_uuid"`
	ExecutionID  uint         `json:"execution_id"`
	Status       string       `json:"status"`
	ChangedTasks []TaskDelta  `json:"changed_tasks,omitempty"` // 仅变更部分
	Error        string       `json:"error,omitempty"`
	UpdatedAt    time.Time    `json:"updated_at"`
}

// ApplyDeltas 将一批增量变更应用到 WorkflowState 上
// 用于从 MySQL 恢复运行中工作流时，将增量快照聚合还原为完整状态
func (s *WorkflowState) ApplyDeltas(deltas []TaskDelta) {
	for _, d := range deltas {
		switch d.Action {
		case "completed":
			if s.CompletedTasks == nil {
				s.CompletedTasks = make(map[string]map[string]any)
			}
			s.CompletedTasks[d.TaskName] = d.Output
		case "failed":
			if s.FailedTasks == nil {
				s.FailedTasks = make(map[string]string)
			}
			s.FailedTasks[d.TaskName] = d.Result
		case "rolled_back":
			if s.RolledBack == nil {
				s.RolledBack = make(map[string]bool)
			}
			s.RolledBack[d.TaskName] = true
		}
	}
}

// NewWorkflow 创建一个新的空工作流（默认为手动触发）
// UUID 使用 name 作为唯一标识，调用方如需自定义 UUID 请直接设置 Workflow.UUID 字段
func NewWorkflow(name string) *Workflow {
	return &Workflow{
		UUID:       name,
		Name:       name,
		TaskGroups: make([][]TaskNode, 0),
		Trigger:    TriggerManual,
	}
}

// AddTaskGroup 向工作流添加一个串行任务组（组内任务并行执行）
// 返回自身以支持链式调用
// tasks 为可变参数，支持两种形式：
//   - 仅名称：AddTaskGroup("task1", "task2") — 不带参数
//   - 带参数：AddTaskGroup(TaskNode{Name: "task1", Params: ...}, ...)
func (wf *Workflow) AddTaskGroup(tasks ...string) *Workflow {
	nodes := make([]TaskNode, len(tasks))
	for i, name := range tasks {
		nodes[i] = TaskNode{Name: name}
	}
	wf.TaskGroups = append(wf.TaskGroups, nodes)
	return wf
}

// AddTaskNodeGroup 向工作流添加一个任务组（支持 TaskNode 参数）
func (wf *Workflow) AddTaskNodeGroup(nodes ...TaskNode) *Workflow {
	wf.TaskGroups = append(wf.TaskGroups, nodes)
	return wf
}

// AddSubWorkflowGroup 添加一个包含子工作流的任务组
// name: 在父工作流中的标识符名称
// subWorkflowUUID: 引用的子工作流 UUID
func (wf *Workflow) AddSubWorkflowGroup(name, subWorkflowUUID string) *Workflow {
	node := TaskNode{
		Type:        TaskNodeTypeSubWorkflow,
		Name:        name,
		SubWorkflow: subWorkflowUUID,
	}
	wf.TaskGroups = append(wf.TaskGroups, []TaskNode{node})
	return wf
}

// AddSubWorkflowGroupWithParams 添加一个包含子工作流的任务组（带传入参数）
func (wf *Workflow) AddSubWorkflowGroupWithParams(name, subWorkflowUUID string, params map[string]any) *Workflow {
	node := TaskNode{
		Type:             TaskNodeTypeSubWorkflow,
		Name:             name,
		SubWorkflow:      subWorkflowUUID,
		SubWorkflowParams: params,
	}
	wf.TaskGroups = append(wf.TaskGroups, []TaskNode{node})
	return wf
}

// AddFanOutGroup 添加一个 Fan-Out 动态并行任务组
// name: 任务组标识名称
// iteratorKey: 从上游输出中取列表的 key
// taskName: 每个元素执行的任务名称
func (wf *Workflow) AddFanOutGroup(name, iteratorKey, taskName string) *Workflow {
	node := TaskNode{
		Type: TaskNodeTypeFanOut,
		Name: name,
		FanOut: &FanOutDef{
			IteratorKey: iteratorKey,
			Task:        TaskNode{Name: taskName},
		},
	}
	wf.TaskGroups = append(wf.TaskGroups, []TaskNode{node})
	return wf
}

// AddFanOutGroupWithOptions 添加一个 Fan-Out 动态并行任务组（完整配置）
func (wf *Workflow) AddFanOutGroupWithOptions(name, iteratorKey string, task TaskNode, maxParallel int, outputKey string) *Workflow {
	node := TaskNode{
		Type: TaskNodeTypeFanOut,
		Name: name,
		FanOut: &FanOutDef{
			IteratorKey: iteratorKey,
			Task:        task,
			MaxParallel: maxParallel,
			OutputKey:   outputKey,
	},
	}
	wf.TaskGroups = append(wf.TaskGroups, []TaskNode{node})
	return wf
}

// AddLoopGroup 添加一个包含循环的任务组
// taskName: 循环执行的任务名称
// maxIterations: 最大迭代次数
// conditionType: 条件类型（always / key）
// conditionKey: 条件 key（conditionType=key 时使用）
func (wf *Workflow) AddLoopGroup(taskName string, maxIterations int, conditionType LoopConditionType, conditionKey string) *Workflow {
	node := TaskNode{
		Name: taskName,
		Loop: &LoopDef{
			MaxIterations: maxIterations,
			ConditionType: conditionType,
			ConditionKey:  conditionKey,
		},
	}
	wf.TaskGroups = append(wf.TaskGroups, []TaskNode{node})
	return wf
}

// AddLoopGroupWithParams 添加一个包含循环的任务组（带参数和循环节点）
func (wf *Workflow) AddLoopGroupWithParams(taskName string, params map[string]any, maxIterations int, conditionType LoopConditionType, conditionKey string) *Workflow {
	node := TaskNode{
		Name:   taskName,
		Params: params,
		Loop: &LoopDef{
			MaxIterations: maxIterations,
			ConditionType: conditionType,
			ConditionKey:  conditionKey,
		},
	}
	wf.TaskGroups = append(wf.TaskGroups, []TaskNode{node})
	return wf
}

// AddTaskGroupWithInputMapping 添加带输入映射的任务组
// mapping: key=本任务参数名, value=上游输出变量名
// 仅名称：AddTaskGroupWithInputMapping("task1", map[string]string{"repo": "upstream.git_url"})
func (wf *Workflow) AddTaskGroupWithInputMapping(tasks []string, mappings []map[string]string) *Workflow {
	nodes := make([]TaskNode, len(tasks))
	for i, name := range tasks {
		nodes[i] = TaskNode{Name: name}
		if i < len(mappings) && mappings[i] != nil {
			nodes[i].InputMapping = mappings[i]
		}
	}
	wf.TaskGroups = append(wf.TaskGroups, nodes)
	return wf
}

// AddBranchNodeGroup 添加一个带增强条件分支的任务组
// taskName: 定义分支的父任务名
// branches: 分支定义
// defaultBranch: 默认分支（当无匹配时执行，可选）
// parallelBranch: 是否并行执行所有匹配的分支
func (wf *Workflow) AddBranchNodeGroup(taskName string, branches BranchDef, defaultBranch string, parallelBranch bool) *Workflow {
	node := TaskNode{
		Name:            taskName,
		Branches:        branches,
		DefaultBranch:   defaultBranch,
		ParallelBranch:  parallelBranch,
	}
	wf.TaskGroups = append(wf.TaskGroups, []TaskNode{node})
	return wf
}

// SetEventTrigger 设置事件触发
// eventType: 事件类型（webhook / workflow_completed / task_failed）
// filter: 过滤条件
func (wf *Workflow) SetEventTrigger(eventType EventTriggerType, filter string) *Workflow {
	wf.Trigger = TriggerEvent
	wf.EventTrigger = &EventTrigger{
		EventType: eventType,
		Filter:    filter,
	}
	return wf
}

// SetErrorPolicy 设置全局错误处理策略
// onTaskFailure: 任务失败策略（retry / rollback / skip / fail_fast）
// onTimeout: 超时策略（rollback / fail）
func (wf *Workflow) SetErrorPolicy(onTaskFailure ErrorPolicyType, onTimeout string) *Workflow {
	wf.ErrorPolicy = &ErrorPolicy{
		OnTaskFailure: onTaskFailure,
		OnTimeout:     onTimeout,
	}
	return wf
}

// SetErrorPolicyWithDetails 设置带详细配置的全局错误处理策略
func (wf *Workflow) SetErrorPolicyWithDetails(policy *ErrorPolicy) *Workflow {
	wf.ErrorPolicy = policy
	return wf
}

// ApplyInputMapping 应用输入映射，根据 TaskNode 的 InputMapping 配置
// 从上游输出中提取指定变量，替代全量合并
// 返回映射后的输入 map，如果无 InputMapping 配置则返回 nil（调用方应回退到全量合并）
func ApplyInputMapping(node TaskNode, upstreamOutputs []map[string]any) map[string]any {
	if node.InputMapping == nil || len(node.InputMapping) == 0 {
		return nil
	}

	// 构建扁平化的上游输出合并（所有上游输出合并到一个 map，后写入覆盖先写入）
	merged := make(map[string]any)
	for _, output := range upstreamOutputs {
		if output == nil {
			continue
		}
		for k, v := range output {
			merged[k] = v
		}
	}

	// 按映射规则提取
	result := make(map[string]any, len(node.InputMapping))
	for localKey, sourceKey := range node.InputMapping {
		// 支持点号路径："upstream.task_name.key" → 从 merged 中查找
		// 简化实现：直接在 merged 中查找 sourceKey
		if val, ok := merged[sourceKey]; ok {
			result[localKey] = val
		}
	}
	return result
}


// DeepCopy 深拷贝 Workflow，避免并发修改原始对象
// 用于 RetryWorkflow、ScheduleCronWorkflow 等需要复制工作流定义的场景
// 通过 JSON 序列化/反序列化实现真正的深拷贝，确保嵌套 map/slice 也被完整复制
func (wf *Workflow) DeepCopy() (*Workflow, error) {
	data, err := json.Marshal(wf)
	if err != nil {
		return nil, fmt.Errorf("深拷贝序列化失败: %w", err)
	}
	var copy Workflow
	if err := json.Unmarshal(data, &copy); err != nil {
		return nil, fmt.Errorf("深拷贝反序列化失败: %w", err)
	}
	return &copy, nil
}
