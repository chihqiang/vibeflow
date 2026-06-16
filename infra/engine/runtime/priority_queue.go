package runtime

import (
	"container/heap"
	"sync"

	"chihqiang/vibeflow/infra/store"
)

// prioritizedTask 带优先级的任务事件包装
// Worker 端通过优先队列对 PENDING 任务排序，高优先级任务优先获取执行槽位
type prioritizedTask struct {
	event    store.Event
	priority int
	index    int // heap.Interface 需要的索引字段
}

// priorityTaskQueue 实现 container/heap.Interface 的优先队列
// 排序规则：priority 降序（数值越大越优先），同优先级时 FIFO（index 小的先出）
type priorityTaskQueue struct {
	items     []*prioritizedTask
	mu        sync.Mutex
	notEmpty  chan struct{} // 用于通知有新任务到达，避免忙等
	closed    bool
	nextIndex int64 // per-instance 单调递增计数器，由 pq.mu 保护
	                 // 替代全局 atomic.Int64，消除多 Worker 实例的 CPU 缓存行 bouncing
}

// newPriorityTaskQueue 创建任务优先队列
func newPriorityTaskQueue() *priorityTaskQueue {
	return &priorityTaskQueue{
		items:    make([]*prioritizedTask, 0),
		notEmpty: make(chan struct{}, 1),
	}
}

// Len 返回队列长度（heap.Interface）
func (pq *priorityTaskQueue) Len() int { return len(pq.items) }

// Less 排序比较：priority 降序，同优先级按 index 升序（FIFO）
func (pq *priorityTaskQueue) Less(i, j int) bool {
	if pq.items[i].priority != pq.items[j].priority {
		return pq.items[i].priority > pq.items[j].priority // 降序：大值优先
	}
	return pq.items[i].index < pq.items[j].index // 同优先级 FIFO
}

// Swap 交换元素（heap.Interface）
func (pq *priorityTaskQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

// Push 入队（heap.Interface）
func (pq *priorityTaskQueue) Push(x interface{}) {
	item := x.(*prioritizedTask)
	pq.items = append(pq.items, item)
}

// Pop 出队（heap.Interface）
func (pq *priorityTaskQueue) Pop() interface{} {
	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // 避免内存泄漏
	pq.items = old[0 : n-1]
	return item
}

// Enqueue 线程安全地将任务事件加入优先队列
func (pq *priorityTaskQueue) Enqueue(event store.Event, priority int) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if pq.closed {
		return
	}
	// per-instance 单调递增索引，由 pq.mu 保护，无需全局原子操作
	pq.nextIndex++
	idx := int(pq.nextIndex)
	heap.Push(pq, &prioritizedTask{
		event:    event,
		priority: priority,
		index:    idx,
	})
	// 非阻塞通知：如果 notEmpty 已有信号则不重复发送
	select {
	case pq.notEmpty <- struct{}{}:
	default:
	}
}

// Dequeue 线程安全地从优先队列取出最高优先级任务
// 如果队列为空且 closeCh 为 nil，则阻塞等待
// 返回 (event, true) 表示成功取出，(store.Event{}, false) 表示队列已关闭且为空
func (pq *priorityTaskQueue) Dequeue(closeCh <-chan struct{}) (store.Event, bool) {
	for {
		pq.mu.Lock()
		if pq.closed && len(pq.items) == 0 {
			pq.mu.Unlock()
			return store.Event{}, false
		}
		if len(pq.items) > 0 {
			item := heap.Pop(pq).(*prioritizedTask)
			pq.mu.Unlock()
			return item.event, true
		}
		pq.mu.Unlock()

		// 队列为空，等待通知或关闭信号
		select {
		case <-pq.notEmpty:
			// 收到通知，重新检查队列
		case <-closeCh:
			// 收到关闭信号，做最后一次检查后退出
			pq.mu.Lock()
			if len(pq.items) == 0 {
				pq.closed = true
				pq.mu.Unlock()
				return store.Event{}, false
			}
			item := heap.Pop(pq).(*prioritizedTask)
			pq.mu.Unlock()
			return item.event, true
		}
	}
}

// Close 关闭优先队列，不再接受新任务
func (pq *priorityTaskQueue) Close() {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.closed = true
}

// Closed 返回队列是否已关闭（线程安全）
func (pq *priorityTaskQueue) Closed() bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.closed
}

// LenSafe 线程安全地返回队列长度
func (pq *priorityTaskQueue) LenSafe() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.items)
}


