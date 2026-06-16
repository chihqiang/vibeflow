package model

import (
	"encoding/json"
	"sync"
)

// 框架保留的 taskCtx 输出键名，任务通过 taskCtx.Set 写入以控制工作流行为
const (
	// SkipGroupsKey 控制条件跳过：值为 int，表示跳过后续 N 个任务组
	// 示例：taskCtx.Set(model.SkipGroupsKey, 1) — 跳过下一组，直接执行下下组
	SkipGroupsKey = "__vibeflow_skip_groups"

	// BranchKey 控制条件分支：值为 string，选择下一个要执行的分支名称
	// 与 Workflow.TaskGroups 中的 BranchDef 配合使用
	// 示例：taskCtx.SetBranch("file_exists") — 选择名为 "file_exists" 的分支
	// 注：SetBranch 和 SkipGroups 互斥，框架优先处理分支
	BranchKey = "__vibeflow_branch"

	// ApprovalKey 控制人工审批：值为 string，表示当前任务完成后暂停工作流等待人工审批
	// 示例：taskCtx.SetApproval("文件内容审核通过了吗？") — 暂停工作流，等待外部调用 ApproveWorkflow/RejectWorkflow
	// 注：SetApproval 与 SetBranch/SkipGroups 互斥，框架优先处理审批暂停
	ApprovalKey = "__vibeflow_approval"
)

// Context 任务执行上下文，用于在工作流的任务之间传递变量
// 线程安全，支持并发读写
// 任务通过 Set 写入输出，后续任务通过 Get 读取输入
type Context struct {
	mu   sync.RWMutex
	vars map[string]interface{}
}

// NewContext 创建一个空的执行上下文
func NewContext() *Context {
	return &Context{
		vars: make(map[string]interface{}),
	}
}

// NewContextWith 从给定的参数 map 创建上下文（复制一份，避免外部修改）
func NewContextWith(params map[string]interface{}) *Context {
	vars := make(map[string]interface{}, len(params))
	for k, v := range params {
		vars[k] = v
	}
	return &Context{vars: vars}
}

// NewContextFromMap 从 map 直接创建上下文（不复制，调用方需保证 map 不会在读取期间被修改）
// 用于框架内部从 output 反查控制标记，零分配开销
func NewContextFromMap(vars map[string]interface{}) *Context {
	return &Context{vars: vars}
}

// Set 写入一个变量
// 通常在前置任务执行完毕后调用，将输出写入上下文供后续任务使用
func (c *Context) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.vars[key] = value
}

// Get 读取一个变量
// 返回变量值和是否存在，key 不存在时 ok 为 false
func (c *Context) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.vars[key]
	return val, ok
}

// GetString 读取字符串类型的变量
func (c *Context) GetString(key string) (string, bool) {
	v, ok := c.Get(key)
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// GetInt 读取 int 类型的变量（兼容 json 反序列化的 float64）
func (c *Context) GetInt(key string) (int, bool) {
	v, ok := c.Get(key)
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, false
		}
		return int(i), true
	default:
		return 0, false
	}
}

// GetInt64 读取 int64 类型的变量
func (c *Context) GetInt64(key string) (int64, bool) {
	v, ok := c.Get(key)
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, false
		}
		return i, true
	default:
		return 0, false
	}
}

// GetFloat64 读取 float64 类型的变量
func (c *Context) GetFloat64(key string) (float64, bool) {
	v, ok := c.Get(key)
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

// GetBool 读取 bool 类型的变量
func (c *Context) GetBool(key string) (bool, bool) {
	v, ok := c.Get(key)
	if !ok {
		return false, false
	}
	b, ok := v.(bool)
	return b, ok
}

// GetStringSlice 读取 []string 类型的变量
func (c *Context) GetStringSlice(key string) ([]string, bool) {
	v, ok := c.Get(key)
	if !ok {
		return nil, false
	}
	// 处理 []interface{} 的情况（JSON 反序列化产生）
	if arr, ok := v.([]interface{}); ok {
		ss := make([]string, 0, len(arr))
		for _, item := range arr {
			s, ok := item.(string)
			if !ok {
				return nil, false
			}
			ss = append(ss, s)
		}
		return ss, true
	}
	ss, ok := v.([]string)
	return ss, ok
}

// GetMap 读取 map[string]interface{} 类型的变量
func (c *Context) GetMap(key string) (map[string]interface{}, bool) {
	v, ok := c.Get(key)
	if !ok {
		return nil, false
	}
	m, ok := v.(map[string]interface{})
	return m, ok
}

// GetAll 返回当前上下文中所有变量的快照拷贝
// 用于序列化或日志记录
// ⚠️ 注意：此为浅拷贝，对返回 map 顶层 key 的 set/delete 是安全的，
// 但如果 value 是 map、slice 等引用类型，修改嵌套数据仍会影响原始上下文。
// 如需完全隔离的深拷贝，请使用 json.Marshal/Unmarshal。
func (c *Context) GetAll() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cp := make(map[string]interface{}, len(c.vars))
	for k, v := range c.vars {
		cp[k] = v
	}
	return cp
}

// SkipGroups 标记跳过后续 N 个任务组，当前任务完成后直接跳到第 N+1 组
// 例如 SkipGroups(1) 表示跳过下一组；SkipGroups(2) 表示跳过下两组
// 任务中通过 taskCtx.SkipGroups(n) 调用，内部写入框架保留键
func (c *Context) SkipGroups(n int) {
	c.Set(SkipGroupsKey, n)
}

// GetSkipGroups 读取当前上下文中标记的跳过组数，未设置返回 0
func (c *Context) GetSkipGroups() int {
	v, ok := c.Get(SkipGroupsKey)
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	}
	return 0
}

// SetBranch 设置条件分支：指定下一个要执行的分支名称
// 任务中通过 taskCtx.SetBranch("branch_name") 调用，内部写入框架保留键
// 与 SkipGroups 互斥：如果同时设置了 Branch 和 SkipGroups，框架优先处理分支
func (c *Context) SetBranch(name string) {
	c.Set(BranchKey, name)
}

// GetBranch 读取当前上下文中标记的分支名称，未设置返回空字符串
func (c *Context) GetBranch() string {
	v, ok := c.Get(BranchKey)
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

// SetApproval 标记当前任务需要人工审批，暂停工作流等待外部信号
// msg 为审批界面展示的提示信息，如 "请确认是否放行该操作"
// 任务中通过 taskCtx.SetApproval("message") 调用，内部写入框架保留键
func (c *Context) SetApproval(msg string) {
	c.Set(ApprovalKey, msg)
}

// GetApproval 读取当前上下文中标记的审批信息，未设置返回空字符串
func (c *Context) GetApproval() string {
	s, _ := c.GetString(ApprovalKey)
	return s
}
