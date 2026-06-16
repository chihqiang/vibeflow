package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/domain/model"
)

// TaskRegistry 任务注册表，管理所有已注册的任务处理器
// 线程安全，支持并发注册和查询
type TaskRegistry struct {
	mu    sync.RWMutex
	tasks map[string]model.Task
}

// NewTaskRegistry 创建一个空的注册表
func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		tasks: make(map[string]model.Task),
	}
}

// Register 注册一个任务处理器
// 任务名不能为空，不允许重复注册，必选参数必须有默认值
func (r *TaskRegistry) Register(task model.Task) error {
	name := task.Name()
	if name == "" {
		return fmt.Errorf("任务名称不能为空")
	}

	// 校验必选参数的默认值：Required=true 但没有 Default 值的任务不允许注册
	if err := model.ValidateTaskParams(task); err != nil {
		return fmt.Errorf("任务参数校验失败: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tasks[name]; exists {
		return fmt.Errorf("任务 %q 已注册", name)
	}

	r.tasks[name] = task
	logger.Info("任务已注册", "task", name)
	return nil
}

// Get 按名称查找已注册的任务处理器
func (r *TaskRegistry) Get(name string) (model.Task, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tasks[name]
	return t, ok
}

// Registered 返回所有已注册的任务名称列表
func (r *TaskRegistry) Registered() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.namesLocked()
}

// namesLocked 内部使用，调用方需持有 r.mu 读锁
func (r *TaskRegistry) namesLocked() []string {
	names := make([]string, 0, len(r.tasks))
	for name := range r.tasks {
		names = append(names, name)
	}
	return names
}

// RegisteredTypes 返回所有已注册任务类型的详细定义（含参数描述）
// 用于 Worker 心跳上报，供 Master 注册任务类型以供 API 查询
func (r *TaskRegistry) RegisteredTypes() []model.TaskType {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.typesLocked()
}

// typesLocked 内部使用，调用方需持有 r.mu 读锁
func (r *TaskRegistry) typesLocked() []model.TaskType {
	types := make([]model.TaskType, 0, len(r.tasks))
	for _, task := range r.tasks {
		types = append(types, model.TaskType{
			Name:        task.Name(),
			Description: "",
			Params:      task.Params(),
		})
	}
	return types
}

// NamesAndTypes 同时返回任务名称列表和类型定义（一次锁操作完成两个查询）
// 用于首次心跳上报
func (r *TaskRegistry) NamesAndTypes() ([]string, []model.TaskType) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.namesLocked(), r.typesLocked()
}

// TypesHash 计算当前 TaskTypes 的 SHA-256 hash 值
// 用于心跳优化：Master 通过比较 hash 判断是否需要更新任务类型注册表
func (r *TaskRegistry) TypesHash() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	types := r.typesLocked()
	if len(types) == 0 {
		return ""
	}
	data, err := json.Marshal(types)
	if err != nil {
		return ""
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
