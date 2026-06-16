package store

import (
	"encoding/json"
	"strings"
	"time"

	"chihqiang/vibeflow/domain/model"
)

// TaskStatus 任务状态枚举
type TaskStatus string

const (
	StatusPending   TaskStatus = "PENDING"   // 等待 Worker 调度执行
	StatusRunning   TaskStatus = "RUNNING"   // Worker 正在执行中
	StatusCompleted TaskStatus = "COMPLETED" // 执行成功，已写入 Output
	StatusFailed    TaskStatus = "FAILED"    // 执行失败，已写入 Result 错误信息
)

// TaskPayload 存储在 etcd 中的任务负载数据
// Master 将任务以 TaskPayload JSON 形式写入 etcd，Worker 读取后执行
type TaskPayload struct {
	WorkflowID   string                 `json:"workflow_id"`   // 所属工作流 UUID
	TaskName     string                 `json:"task_name"`     // 任务名称，与 core.Task 的 Name() 对应
	TraceID      string                 `json:"trace_id"`      // 分布式链路追踪 ID，贯穿整个执行链路（Master → etcd → Worker → etcd → Master）
	TraceContext map[string]string      `json:"trace_context,omitempty"` // W3C TraceContext 传播载体（traceparent, tracestate），用于跨进程链路串联
	Status       TaskStatus             `json:"status"`        // 当前执行状态
	Priority     int                    `json:"priority,omitempty"` // 工作流优先级，数字越大越优先；Worker 端据此排序执行顺序
	Params       map[string]interface{} `json:"params"`        // 用户自定义参数（来自 Workflow.TaskParams，静态配置）
	Input        map[string]interface{} `json:"input"`         // 上游任务的输出数据（运行时动态传递）
	Output       map[string]interface{} `json:"output"`        // 执行完成后由 Worker 写入的输出
	Result       string                 `json:"result"`        // 成功时为空，失败时记录错误信息
	RetryCount   int                    `json:"retry_count"`   // 当前已重试次数，首次执行时为 0
	MaxRetries   int                    `json:"max_retries"`   // 最大允许重试次数，0 表示不重试
	TimeoutSec   int64                  `json:"timeout_sec"`   // 单次执行超时秒数，0 表示不限制
	BaseBackoff  int64                  `json:"base_backoff"`  // 重试基础退避秒数，实际等待 baseBackoff * 2^retryCount
	Rollback     bool                   `json:"rollback,omitempty"`  // 是否为补偿回滚任务（Saga）
	NoRetry      bool                   `json:"no_retry,omitempty"`  // 不可重试的错误（如参数错误），直接失败不重试
	// etcdModRevision 乐观锁版本号，CAS 写入时使用（不序列化到 JSON）
	EtcdModRevision int64 `json:"-"`
	// EtcdKey etcd 中的完整 key 路径，用于任务完成/失败后主动删除
	// 不序列化到 JSON，仅在内存中传递，供 recordTaskCompleted/recordTaskFailed 清理使用
	EtcdKey string `json:"-"`
}

// HeartbeatPayload Worker 心跳负载
// Worker 定期向 etcd 写入心跳，Master 通过 Watch 检测 Worker 存活状态
//
// 心跳优化：常规心跳仅携带 Tasks 列表（不含 TaskTypes）和 TaskTypesHash。
// 首次心跳或任务类型发生变化时，才携带完整 TaskTypes 定义。
// Master 检测到 hash 变化时重新调用 RegisterTaskType 更新注册表。
type HeartbeatPayload struct {
	WorkerID      string           `json:"worker_id"`                 // 唯一标识，如 "worker-01" 或自动生成
	Tasks         []string         `json:"tasks"`                     // 该 Worker 注册的任务类型名称列表（兼容）
	TaskTypes     []model.TaskType `json:"task_types,omitempty"`      // 该 Worker 注册的任务类型详细定义（含参数），仅在首次心跳或类型变更时携带
	TaskTypesHash string           `json:"task_types_hash,omitempty"` // TaskTypes 的 hash 值，Master 通过比较 hash 判断是否需要更新注册表
	StartedAt     time.Time        `json:"started_at"`                // Worker 启动时间
	AliveAt       time.Time        `json:"alive_at"`                  // 最近一次心跳时间
}



// Serialize 将 TaskPayload 序列化为 JSON 字符串
func Serialize(payload *TaskPayload) (string, error) {
	bytes, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// Deserialize 从 JSON 字符串反序列化 TaskPayload
func Deserialize(data string) (*TaskPayload, error) {
	var payload TaskPayload
	err := json.Unmarshal([]byte(data), &payload)
	if err != nil {
		return nil, err
	}
	return &payload, nil
}

// ExtractTaskName 从原始 JSON 中快速提取 task_name 字段。
// 不做完整 JSON 解析，仅做一次字符串扫描。比 QuickStatusCheck 更轻量，
// 适用于已通过 key 前缀确认 status 的场景（如 Worker Watch PENDING 前缀）。
func ExtractTaskName(data string) string {
	return extractJSONField(data, "task_name")
}

// ExtractWorkflowUUID 从原始 JSON 中快速提取 workflow_id 字段。
// 不做完整 JSON 解析，仅做一次字符串扫描。用于 Fan-Out 分发模型中
// 主 Watch goroutine 的轻量路由，无需完整反序列化。
func ExtractWorkflowUUID(data string) string {
	return extractJSONField(data, "workflow_id")
}

// extractJSONField 从 JSON 字符串中提取指定 key 的字符串值
// 这是一个轻量级的字符串扫描，不做完整的 JSON 解析
// 支持转义引号（\"），适用于 workflow_id、task_name 等常见场景
func extractJSONField(data, key string) string {
	// 搜索 "key":
	search := `"` + key + `":`
	idx := strings.Index(data, search)
	if idx < 0 {
		return ""
	}
	idx += len(search)
	// 跳过空白
	for idx < len(data) && (data[idx] == ' ' || data[idx] == '\t') {
		idx++
	}
	if idx >= len(data) || data[idx] != '"' {
		return ""
	}
	idx++ // skip opening quote
	var result strings.Builder
	for idx < len(data) {
		switch data[idx] {
		case '\\':
			// 转义字符：将下一个字符直接写入
			if idx+1 < len(data) {
				result.WriteByte(data[idx+1])
				idx += 2
			} else {
				result.WriteByte(data[idx])
				idx++
			}
		case '"':
			return result.String()
		default:
			result.WriteByte(data[idx])
			idx++
		}
	}
	return ""
}

// SerializeHeartbeat 将 HeartbeatPayload 序列化为 JSON 字符串
func SerializeHeartbeat(p *HeartbeatPayload) (string, error) {
	bytes, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// DeserializeHeartbeat 从 JSON 字符串反序列化 HeartbeatPayload
func DeserializeHeartbeat(data string) (*HeartbeatPayload, error) {
	var p HeartbeatPayload
	err := json.Unmarshal([]byte(data), &p)
	if err != nil {
		return nil, err
	}
	return &p, nil
}
