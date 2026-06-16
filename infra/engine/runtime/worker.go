package runtime

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"chihqiang/vibeflow/infra/circuit"
	"chihqiang/vibeflow/infra/config"
	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/store"

	"crypto/rand"

	"golang.org/x/sync/semaphore"
)

// shutdownWaitTimeout Worker 优雅关闭时等待所有正在执行的任务完成的超时时间
// 超过此时间仍未完成的任务会被强制中断
const shutdownWaitTimeout = 30 * time.Second

// taskAcquireTimeout Worker 获取并发槽位的超时时间
// 当并发满时，任务会排队等待（阻塞式 Acquire），而非被直接丢弃。
// 超时后任务重新入队到优先级队列尾部，避免依赖 etcd Watch 断连恢复。
const taskAcquireTimeout = 15 * time.Second

// cryptoRandIntn 使用 crypto/rand 生成 [0, n) 范围内的随机 int
// 替代 math/rand.Intn，确保 ID 生成的密码学安全性
func cryptoRandIntn(n int) int {
	if n <= 0 {
		return 0
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return int(time.Now().UnixNano()) % n
	}
	return int(binary.BigEndian.Uint64(b[:])) % n
}

// worker 侧常量
const (
	// defaultMaxTaskConcurrency Worker 并发任务的默认上限，当配置值为 0 时使用。
	// 纯 IO 型任务可进一步调高此值。
	defaultMaxTaskConcurrency = 100

	// workerEtcdMaxFailures etcd 操作连续失败多少次后熔断
	workerEtcdMaxFailures = 5
	// workerEtcdCooldown etcd 熔断后的冷却时间
	workerEtcdCooldown = 15 * time.Second
)

// Worker 是工作流引擎的执行节点
// 通过 Watch etcd 上的任务前缀获取 Master 下发的任务并执行，结果写回 etcd
type Worker struct {
	store store.Store        // etcd 存储后端
	Conf  *config.WorkerConfig // Worker 配置（ID、心跳间隔等）

	registry *TaskRegistry    // 已注册的任务处理器
	reporter *StatusReporter  // 任务状态报告器（写入 etcd）
	executor *TaskExecutor    // 任务执行引擎
	hb       *HeartbeatSender // 心跳发送器

	etcdBreaker *circuit.Breaker // etcd 操作熔断器（状态上报 + 心跳共用）

	// taskSem 有界 goroutine 池信号量，限制同时执行的任务数
	// 防止任务涌入时 goroutine 暴涨导致 OOM
	taskSem *semaphore.Weighted

	// taskPQ 优先级任务队列，高优先级任务优先获取执行槽位
	// 替代原来 FIFO 的事件处理方式，使 Worker 端具备优先级感知能力
	taskPQ *priorityTaskQueue

	// reenqueueAttempts 记录每个任务的重新入队次数，用于指数退避
	// key = taskKey（etcd key），value = 已重新入队的次数
	reenqueueAttempts   map[string]int
	reenqueueAttemptsMu sync.Mutex

	wg      sync.WaitGroup // 等待正在执行的任务完成
	running atomic.Bool    // 运行标志，防止重复启动
}

// NewWorker 创建 Worker 实例
// 如果 workerCfg.ID 为空则自动生成 "worker-HHMMSS" 格式的 ID
func NewWorker(s store.Store, workerCfg *config.WorkerConfig) *Worker {
	if workerCfg.ID == "" {
		// 使用纳秒 + 随机后缀避免同一秒内启动多个 Worker 时 ID 重复
		workerCfg.ID = fmt.Sprintf("worker-%s-%d", time.Now().Format("150405"), cryptoRandIntn(10000))
	}

	maxConcurrency := workerCfg.MaxTaskConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = defaultMaxTaskConcurrency
	}

	reg := NewTaskRegistry()
	breaker := circuit.NewBreaker("worker-etcd", workerEtcdMaxFailures, workerEtcdCooldown)
	rep := NewStatusReporter(s, breaker)
	hb := NewHeartbeatSender(s, workerCfg.ID, reg.Registered, reg.NamesAndTypes, reg.TypesHash,
		workerCfg.HeartbeatInterval.ToDuration(), s.Prefixes().Heartbeats, breaker)

	return &Worker{
		store:       s,
		Conf:        workerCfg,
		registry:    reg,
		reporter:    rep,
		etcdBreaker: breaker,
		executor:    NewTaskExecutor(s, s.Prefixes().Tasks, reg, rep),
		hb:          hb,
		taskSem:     semaphore.NewWeighted(int64(maxConcurrency)),
		taskPQ:      newPriorityTaskQueue(),
	}
}

// ID 返回 Worker 的唯一标识
func (w *Worker) ID() string { return w.Conf.ID }

// RegisterTask 注册一个任务处理器到 Worker 的任务注册表
// 注册后 Worker 才能执行该类型的任务
// 同名任务重复注册会 log.Fatal 终止进程，应在启动阶段就暴露问题
func (w *Worker) RegisterTask(task model.Task) *Worker {
	if err := w.registry.Register(task); err != nil {
		logger.Fatal("注册任务失败", "error", err)
	}
	return w
}

// RegisteredTasks 返回所有已注册的任务名称列表
func (w *Worker) RegisteredTasks() []string {
	return w.registry.Registered()
}

// Start 启动 Worker 主循环
// 1. 启动心跳发送器，定期向 etcd 上报存活状态
// 2. Watch 任务前缀，收到新任务后入队到优先级队列
// 3. 优先级消费 goroutine 按优先级顺序取出任务执行
// 4. ctx 取消时等待正在执行的任务完成并退出
func (w *Worker) Start(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return fmt.Errorf("Worker %s 已在运行", w.Conf.ID)
	}
	defer w.running.Store(false)

	logger.Info("Worker 正在启动",
		"worker_id", w.Conf.ID,
		"tasks", w.registry.Registered(),
	)

	// 启动后台心跳 goroutine
	go w.hb.Start(ctx)

	// 启动优先级消费者 goroutine：按优先级顺序取出任务并执行
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.consumeFromPriorityQueue(ctx)
	}()

	// 使用带过滤的 Watch，仅接收本 Worker 注册的任务类型事件。
	// Watch 前缀为 {prefix}PENDING/，通过 key 路径前缀过滤 status，
	// filter 中仅需检查 task_name 是否在本 Worker 注册表中。
	pendingPrefix := w.store.Prefixes().Tasks + string(store.StatusPending) + "/"
	eventChan, err := w.store.WatchWithFilter(ctx, pendingPrefix, func(event *store.Event) bool {
		// 第1步：从原始 JSON 快速提取 task_name（仅一次字符串扫描）
		taskName := store.ExtractTaskName(event.Value)
		if taskName == "" {
			return false
		}
		// 第2步：检查是否为本 Worker 注册的任务类型（O(1) map 查找）
		if _, exists := w.registry.Get(taskName); !exists {
			return false
		}
		// 第3步：通过轻量检查后才做完整反序列化，此时已确认事件与本 Worker 相关
		payload, err := store.Deserialize(event.Value)
		if err != nil {
			return false
		}
		event.Payload = payload
		return true
	})
	if err != nil {
		return fmt.Errorf("启动任务监听失败: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			logger.Info("Worker 正在关闭", "worker_id", w.Conf.ID)
			w.taskPQ.Close()
			// 带超时的等待，防止个别任务长时间运行导致 Worker 无法退出
			done := make(chan struct{})
			go func() {
				w.wg.Wait()
				close(done)
			}()
			select {
			case <-done:
				logger.Info("所有任务已正常结束", "worker_id", w.Conf.ID)
			case <-time.After(shutdownWaitTimeout):
				logger.Warn("等待任务结束超时，强制退出", "worker_id", w.Conf.ID)
			}
			logger.Info("Worker 已停止", "worker_id", w.Conf.ID)
			return nil

		case event, ok := <-eventChan:
			if !ok {
				w.taskPQ.Close()
				return nil
			}
			if event.Type != store.EventPut {
				continue
			}

			// 将任务事件放入优先级队列，由消费者 goroutine 按优先级处理
			priority := 0
			if event.Payload != nil {
				priority = event.Payload.Priority
			}
			w.taskPQ.Enqueue(event, priority)
		}
	}
}

// consumeFromPriorityQueue 从优先级队列中按优先级顺序取出任务并执行
// 高优先级任务优先获取执行槽位（信号量），同优先级任务 FIFO
// 当 ctx 取消或队列关闭且为空时退出
// 超时未获取到槽位的任务会被重新入队到队列尾部，而非直接丢弃，
// 避免依赖 etcd Watch 断连恢复机制带来的高延迟。
// 重新入队使用指数退避策略，避免低优先级任务在持续涌入的高优先级任务面前频繁
// 超时→入队→再超时，浪费消费循环的处理周期。
func (w *Worker) consumeFromPriorityQueue(ctx context.Context) {
	for {
		event, ok := w.taskPQ.Dequeue(ctx.Done())
		if !ok {
			logger.Info("优先级消费循环退出", "worker_id", w.Conf.ID)
			return
		}

		// 有界 goroutine 池：通过信号量限制并发任务数
		// 使用阻塞式 Acquire（带超时），替代原来的 TryAcquire。
		// 当并发满时，任务在 Worker 端排队等待而非被直接丢弃。
		acquireCtx, acquireCancel := context.WithTimeout(ctx, taskAcquireTimeout)
		err := w.taskSem.Acquire(acquireCtx, 1)
		acquireCancel()
		if err != nil {
			payload := event.Payload
			taskName := "unknown"
			taskKey := event.Key
			if payload != nil {
				taskName = payload.TaskName
			}

			// 指数退避：记录重新入队次数，按 2^attempt 递增等待时间
			attempt := w.recordReenqueueAttempt(taskKey)
			backoff := taskAcquireTimeout * (1 << attempt) // 15s, 30s, 60s, 120s...
			if backoff > 5*time.Minute {
				backoff = 5 * time.Minute // 最大退避 5 分钟
			}

			logger.Warn("Worker 达到并发上限，指数退避后重新入队",
				"worker_id", w.Conf.ID,
				"task", taskName,
				"key", taskKey,
				"attempt", attempt,
				"backoff", backoff,
			)

			// 指数退避等待后再重新入队，避免频繁占用消费循环处理周期
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}

			// 重新入队到优先级队列尾部，保留原始 priority 不重置入队时间
			priority := 0
			if payload != nil {
				priority = payload.Priority
			}
			// 检查队列是否已关闭，避免向已关闭的队列入队
			if !w.taskPQ.Closed() {
				w.taskPQ.Enqueue(event, priority)
			}
			continue
		}

		// 成功获取槽位，清除该任务的重新入队计数
		w.clearReenqueueAttempt(event.Key)

		// 异步执行任务，避免阻塞优先级消费循环
		// 派生自主 ctx，当 Worker 关闭时任务能感知取消信号
		w.wg.Add(1)
		go func(ev store.Event) {
			defer w.wg.Done()
			defer w.taskSem.Release(1)
			w.executor.Execute(ctx, ev)
		}(event)
	}
}

// recordReenqueueAttempt 记录并返回任务的重新入队次数（线程安全）
func (w *Worker) recordReenqueueAttempt(taskKey string) int {
	w.reenqueueAttemptsMu.Lock()
	defer w.reenqueueAttemptsMu.Unlock()
	if w.reenqueueAttempts == nil {
		w.reenqueueAttempts = make(map[string]int)
	}
	w.reenqueueAttempts[taskKey]++
	return w.reenqueueAttempts[taskKey]
}

// clearReenqueueAttempt 清除任务的重新入队计数（成功获取槽位后调用）
func (w *Worker) clearReenqueueAttempt(taskKey string) {
	w.reenqueueAttemptsMu.Lock()
	defer w.reenqueueAttemptsMu.Unlock()
	delete(w.reenqueueAttempts, taskKey)
}


