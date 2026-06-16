package store

import (
	"context"
	"time"
)

// ExecutionRecord 执行记录摘要
type ExecutionRecord struct {
	ExecutionID uint   // 执行记录 ID
	UUID        string // 工作流 UUID
	Name        string // 工作流名称
	Data        []byte // WorkflowState JSON
}

// 工作流状态枚举
const (
	WorkflowStatusPending   = "PENDING"
	WorkflowStatusRunning   = "RUNNING"
	WorkflowStatusCompleted = "COMPLETED"
	WorkflowStatusFailed    = "FAILED"
)

// TaskDef 工作流中的任务定义，包含名称和参数值
type TaskDef struct {
	Name   string                 `json:"name"`
	Params map[string]interface{} `json:"params,omitempty"`
}

// WorkflowDefRecord 工作流定义记录
type WorkflowDefRecord struct {
	ID             uint     // 定义 ID
	Name           string   // 工作流名称
	Tasks          []TaskDef // 任务列表
	TimeoutSec     int64
	TaskTimeoutSec int64
	MaxRetries     int
	BaseBackoff    int64
	Trigger        string
	CronExpr       string
}

// HistoryStore 工作流历史记录存储接口
// 基于 MySQL/GORM 实现，所有记录只增不删，保留完整执行过程
type HistoryStore interface {
	// SaveWorkflowDef 插入一条工作流定义，每次执行产生一条新记录
	// 返回新记录的 ID
	SaveWorkflowDef(ctx context.Context, uuid, name string, tasks []TaskDef,
		timeoutSec, taskTimeoutSec int64, maxRetries int, baseBackoff int64, trigger, cronExpr string) (uint, error)

	// UpsertWorkflowDef 按 UUID upsert 工作流定义（存在则更新，不存在则插入）
	// 用于 Master 启动时注册内置工作流，确保定义与内存一致
	UpsertWorkflowDef(ctx context.Context, uuid, name string, tasks []TaskDef,
		timeoutSec, taskTimeoutSec int64, maxRetries int, baseBackoff int64, trigger, cronExpr string) (uint, error)

	// LoadWorkflowDefs 获取所有状态为 PENDING 的工作流定义（即内置/已保存但未执行的定义）
	LoadWorkflowDefs(ctx context.Context) ([]WorkflowDefRecord, error)

	// CreateExecution 插入一条工作流执行记录，返回新记录的 ID
	CreateExecution(ctx context.Context, workflowID uint, data []byte, status string, startedAt time.Time, errMsg string) (uint, error)

	// UpdateWorkflowStatus 更新工作流定义的状态
	UpdateWorkflowStatus(ctx context.Context, workflowID uint, status string) error

	// UpdateExecution 更新已有的执行记录状态（完成/失败时调用）
	UpdateExecution(ctx context.Context, executionID uint, data []byte, status string, errMsg string) error

	// SaveTask 追加一条任务执行详情记录（vibeflow_execution_tasks）
	SaveTask(ctx context.Context, executionID, workflowID uint, taskName string, status string,
		params map[string]any, output map[string]any, errMsg string, retryCount int, maxRetries int) error

	// SaveTaskDeltas 批量保存任务增量变更记录（vibeflow_task_deltas）
	// 运行时增量快照：每次任务完成/失败/回滚只记录变更的任务，而非整个 WorkflowState
	SaveTaskDeltas(ctx context.Context, executionID uint, deltas []byte) error

	// LoadTaskDeltas 加载指定执行记录的所有增量变更
	LoadTaskDeltas(ctx context.Context, executionID uint) ([]byte, error)

	// BatchLoadTaskDeltas 批量加载多个执行记录的增量变更
	// 用于并行恢复工作流时减少 MySQL 查询次数
	// 返回 map[executionID]deltas，executionID 无增量时不在 map 中
	BatchLoadTaskDeltas(ctx context.Context, executionIDs []uint) (map[uint][]byte, error)

	// LoadExecutions 分页获取执行记录
	LoadExecutions(ctx context.Context, offset, limit int) ([]ExecutionRecord, int64, error)

	// GetExecutionByUUID 按工作流 UUID 获取最新一条执行记录（用于精确查询单条历史）
	GetExecutionByUUID(ctx context.Context, uuid string) (*ExecutionRecord, error)

	// LoadRunningExecutions 获取所有状态为 RUNNING 的执行记录，用于 Master 重启后恢复工作流
	LoadRunningExecutions(ctx context.Context) ([]ExecutionRecord, error)

	// Close 关闭数据库连接
	Close() error

	// Ping 检查 MySQL 连接是否正常，返回延迟（用于健康检查端点）
	Ping(ctx context.Context) (latency time.Duration, err error)

	// Stats 返回 MySQL 连接池统计信息（用于健康检查和监控）
	Stats() DBStats
}

// DBStats 数据库连接池统计
type DBStats struct {
	MaxOpenConnections int // 最大连接数配置
	OpenConnections    int // 当前打开的连接数
	InUse              int // 正在使用的连接数
	Idle               int // 空闲连接数
}
