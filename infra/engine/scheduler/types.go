package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/google/uuid"

	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/store"
)

// randHex 返回 n 字节随机数的 hex 编码（无锁，crypto/rand）
func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand 在 Linux 上极少失败，fallback 用时间戳
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// pendingTask 等待执行的任务（并发数达到上限时入队等待）
type pendingTask struct {
	workflowName    string           // 所属工作流名称
	node            model.TaskNode   // 任务节点（含名称和参数）
	upstreamOutputs []map[string]any // 上游任务的输出参数
	priority        int              // 优先级，数字越大越优先
	enqueued        time.Time        // 入队时间，用于超时淘汰
}

// pendingQueue 基于 container/heap 的优先队列实现
// 按 priority 降序（越大越优先），同优先级按 enqueued 升序（FIFO）
type pendingQueue []*pendingTask

func (pq pendingQueue) Len() int { return len(pq) }

func (pq pendingQueue) Less(i, j int) bool {
	// 优先级高的在前
	if pq[i].priority != pq[j].priority {
		return pq[i].priority > pq[j].priority
	}
	// 同优先级按入队时间，先进先出
	return pq[i].enqueued.Before(pq[j].enqueued)
}

func (pq pendingQueue) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }

func (pq *pendingQueue) Push(x any) {
	*pq = append(*pq, x.(*pendingTask))
}

func (pq *pendingQueue) Pop() any {
	old := *pq
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // 避免内存泄漏
	*pq = old[:n-1]
	return x
}

// NewTraceID 生成一个新的分布式链路追踪 ID
// 使用 google/uuid 生成，格式为标准 UUID v4（36 字符，含连字符）
func NewTraceID() string {
	return uuid.New().String()
}

// taskTTLSeconds etcd 中任务 key 的默认 TTL（秒），防止历史任务数据累积
const taskTTLSeconds int64 = 86400 // 24 小时

// validateWorkflowName 校验工作流名称的合法性
// 名称不能为空，长度不超过 128 字符，仅允许字母、数字、下划线、连字符
func validateWorkflowName(name string) error {
	if name == "" {
		return fmt.Errorf("工作流名称不能为空")
	}
	if len(name) > 128 {
		return fmt.Errorf("工作流名称长度不能超过 128 字符")
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
			return fmt.Errorf("工作流名称仅允许字母、数字、下划线和连字符，不支持: %q", c)
		}
	}
	return nil
}

// validateWorkflowTasks 校验工作流任务定义的合法性
// 1. 每个任务名称不能为空
// 2. 同一个并行组内不允许有同名任务（会导致 listener key 冲突）
// 3. 同一个工作流内不同组之间不允许有同名任务（任务输出覆盖/状态混乱）
// 4. 分支任务名也不能与主任务名冲突（同样会导致 listener key 冲突）
// 5. 子工作流和 Fan-Out 节点的验证
// 6. 事件触发工作流的验证
// 7. 输入映射和增强条件分支校验
// 8. 错误处理策略校验
func validateWorkflowTasks(wf *model.Workflow) error {
	// 校验 ErrorPolicy 定义
	if wf.ErrorPolicy != nil {
		if err := wf.ErrorPolicy.Validate(); err != nil {
			return err
		}
	}

	// 校验事件触发定义
	if wf.Trigger == model.TriggerEvent {
		if wf.EventTrigger == nil {
			return fmt.Errorf("工作流 %s trigger=event 但缺少 event_trigger 定义", wf.Name)
		}
		switch wf.EventTrigger.EventType {
		case model.EventTriggerWebhook:
			// Webhook 无额外校验
		case model.EventTriggerWorkflowCompleted:
			if wf.EventTrigger.Filter == "" {
				return fmt.Errorf("工作流 %s 事件触发 workflow_completed 缺少 filter（源工作流 UUID）", wf.Name)
			}
		case model.EventTriggerTaskFailed:
			if wf.EventTrigger.Filter == "" {
				return fmt.Errorf("工作流 %s 事件触发 task_failed 缺少 filter（workflow_uuid:task_name）", wf.Name)
			}
		default:
			return fmt.Errorf("工作流 %s 未知的事件触发类型: %s", wf.Name, wf.EventTrigger.EventType)
		}
	}

	// 校验输入映射定义
	for gi, group := range wf.TaskGroups {
		for _, node := range group {
			if len(node.InputMapping) > 0 {
				for localKey, sourceKey := range node.InputMapping {
					if localKey == "" {
						return fmt.Errorf("组 %d 任务 %q 的输入映射中存在空的目标键名", gi+1, node.Name)
					}
					if sourceKey == "" {
						return fmt.Errorf("组 %d 任务 %q 的输入映射中存在空的源键名", gi+1, node.Name)
					}
				}
			}
		}
	}

	// 校验增强条件分支定义
	for gi, group := range wf.TaskGroups {
		for _, node := range group {
			if node.DefaultBranch != "" && node.Branches != nil {
				if _, exists := node.Branches[node.DefaultBranch]; !exists {
					return fmt.Errorf("组 %d 任务 %q 的默认分支 %q 在分支定义中不存在", gi+1, node.Name, node.DefaultBranch)
				}
			}
		}
	}

	// 全局任务名称去重检查
	allNames := make(map[string]int) // taskName -> groupIndex (1-based) 用于错误提示

	// 先收集所有主任务名称
	for gi, group := range wf.TaskGroups {
		groupNames := make(map[string]struct{}, len(group))
		for _, node := range group {
			resolvedName, err := validateTaskNode(node, gi+1, "")
			if err != nil {
				return err
			}

			// 普通任务和高级节点都需要检查名称唯一性
			name := resolvedName
			// 如果验证器自动生成了名称，回写到原始节点（避免后续运行时出现空名称）
			if name != node.Name {
				node.Name = name
			}

			// 检查组内重复
			if _, exists := groupNames[name]; exists {
				return fmt.Errorf("组 %d 中存在同名任务 %q，并行组内任务名称不能重复", gi+1, name)
			}
			groupNames[name] = struct{}{}

			// 检查全局重复
			if prevGroup, exists := allNames[name]; exists {
				return fmt.Errorf("任务 %q 在组 %d 和组 %d 中重复定义，同一工作流内任务名称不能重复", name, prevGroup, gi+1)
			}
			allNames[name] = gi + 1
		}
	}

	// 检查分支任务名是否与主任务名冲突
	for gi, group := range wf.TaskGroups {
		for _, node := range group {
			if node.Branches == nil {
				continue
			}
			for branchName, branchGroups := range node.Branches {
				for bi, bg := range branchGroups {
					branchGroupNames := make(map[string]struct{}, len(bg))
					for _, bn := range bg {
						if bn.Name == "" {
							return fmt.Errorf("组 %d 任务 %q 的分支 %q 子组 %d 中存在空名称的任务", gi+1, node.Name, branchName, bi+1)
						}
						// 检查分支组内重复
						if _, exists := branchGroupNames[bn.Name]; exists {
							return fmt.Errorf("组 %d 任务 %q 的分支 %q 子组 %d 中存在同名任务 %q", gi+1, node.Name, branchName, bi+1, bn.Name)
						}
						branchGroupNames[bn.Name] = struct{}{}

						// 检查是否与主任务名冲突
						if prevGroup, exists := allNames[bn.Name]; exists {
							return fmt.Errorf("分支任务 %q（组 %d 任务 %q 的分支 %q）与组 %d 中的任务名冲突，同一工作流内任务名称不能重复",
								bn.Name, gi+1, node.Name, branchName, prevGroup)
						}
						allNames[bn.Name] = gi + 1
					}
				}
			}
		}
	}

	return nil
}

// validateTaskNode 校验单个任务节点的合法性
// 返回修正后的任务名称（子工作流节点可能自动生成名称），调用方应使用返回值更新原始节点
func validateTaskNode(node model.TaskNode, groupIdx int, parentInfo string) (string, error) {
	nodeType := GetTaskNodeType(node)

	switch nodeType {
	case model.TaskNodeTypeSubWorkflow:
		if node.SubWorkflow == "" {
			return "", fmt.Errorf("组 %d %s中子工作流节点缺少 sub_workflow UUID", groupIdx, parentInfo)
		}
		// 子工作流的 Name 用作父工作流中的标识符（用于状态追踪）
		name := node.Name
		if name == "" {
			// 自动生成名称
			name = "__sub_wf_" + node.SubWorkflow
		}
		// 验证子工作流的循环节点
		if node.Loop != nil {
			return name, validateLoopDef(node.Loop, groupIdx, name)
		}
		return name, nil

	case model.TaskNodeTypeFanOut:
		if node.FanOut == nil {
			return "", fmt.Errorf("组 %d %s中 Fan-Out 节点缺少 fan_out 定义", groupIdx, parentInfo)
		}
		if node.FanOut.IteratorKey == "" {
			return "", fmt.Errorf("组 %d %s中 Fan-Out 节点缺少 iterator_key", groupIdx, parentInfo)
		}
		if node.FanOut.MaxParallel < 0 {
			return "", fmt.Errorf("组 %d %s中 Fan-Out 节点的 max_parallel 不能为负数", groupIdx, parentInfo)
		}
		// 递归验证 Fan-Out 模板中的任务节点
		if node.FanOut.Task.Name == "" {
			return "", fmt.Errorf("组 %d %s中 Fan-Out 模板任务缺少名称", groupIdx, parentInfo)
		}
		return node.Name, nil

	case model.TaskNodeTypeTask:
		// 普通任务：名称不能为空
		if node.Name == "" {
			return "", fmt.Errorf("组 %d %s中存在空名称的任务", groupIdx, parentInfo)
		}
		// 验证循环节点
		if node.Loop != nil {
			return node.Name, validateLoopDef(node.Loop, groupIdx, node.Name)
		}
		return node.Name, nil
	}

	return node.Name, nil
}

// validateLoopDef 校验循环定义的合法性
func validateLoopDef(loop *model.LoopDef, groupIdx int, taskName string) error {
	if loop.MaxIterations <= 0 {
		return fmt.Errorf("组 %d 任务 %q 的循环定义 max_iterations 必须 > 0", groupIdx, taskName)
	}
	if loop.ConditionType != model.LoopConditionAlways && loop.ConditionType != model.LoopConditionKey {
		return fmt.Errorf("组 %d 任务 %q 的循环定义 condition_type 无效，只支持 always 或 key", groupIdx, taskName)
	}
	if loop.ConditionType == model.LoopConditionKey && loop.ConditionKey == "" {
		return fmt.Errorf("组 %d 任务 %q 的循环定义 condition_type=key 时 condition_key 不能为空", groupIdx, taskName)
	}
	return nil
}

// GetTaskNodeType 返回 TaskNode 的实际类型（对外便捷函数，委托给 model.GetTaskNodeType）
func GetTaskNodeType(node model.TaskNode) model.TaskNodeType {
	return model.GetTaskNodeType(node)
}

// taskKey 构造 etcd 中任务 key：{prefix}PENDING/{workflowName}/{taskName}/{nano}-{randHex}
// 将 status 编码到 key 前缀中，使 Worker 可通过 Watch {prefix}PENDING/ 仅接收新任务。
// 按工作流名组织子前缀，使 Scheduler 的 Watch 可精确监听单个工作流，避免 N 个 Watch goroutine 重复消费全局事件。
// 使用 crypto/rand 生成唯一后缀，无锁竞争。
func taskKey(prefix, workflowName, taskName string) string {
	return fmt.Sprintf("%s%s/%s/%s/%d-%s", prefix, string(store.StatusPending), workflowName, taskName, time.Now().UnixNano(), randHex(4))
}

// retryTaskKey 构造 etcd 中重试任务 key：{prefix}PENDING/{workflowName}/{taskName}/retry-{count}-{nano}-{randHex}
// 重试任务也以 PENDING 状态写入，Worker 端无需区分首次和重试。
func retryTaskKey(prefix, workflowName, taskName string, retryCount int) string {
	return fmt.Sprintf("%s%s/%s/%s/retry-%d-%d-%s", prefix, string(store.StatusPending), workflowName, taskName, retryCount, time.Now().UnixNano(), randHex(4))
}

// putWithTTL 写入带 TTL 的键值对，过期后自动删除
func putWithTTL(s store.Store, ctx context.Context, key, value string, ttl int64) error {
	return s.PutWithTTL(ctx, key, value, ttl)
}
