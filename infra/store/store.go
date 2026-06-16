package store

import (
	"context"
	"time"
)

// Prefixes 存储路径前缀配置
// 所有写入 etcd 的 key 都通过这里定义的前缀来组织，避免不同业务类型的 key 互相冲突
type Prefixes struct {
	Tasks      string // 任务下发前缀，Worker 通过 Watch 此前缀来获取新任务
	Heartbeats string // Worker 心跳前缀，Master 通过 Watch 此前缀来检测 Worker 存活
	Workflows  string // 工作流定义前缀，存储工作流定义
}

// Store 共享存储抽象接口
// 所有的分布式协调（如任务下发、状态流转、分布式锁）都通过此接口完成
// 当前基于 etcd 实现，预留接口以便未来切换其他后端
type Store interface {
	// Put 写入一个键值对
	// Master 下发任务或更新任务状态时使用
	Put(ctx context.Context, key, value string) error

	// Get 获取一个键的值
	// 当 key 不存在时返回空字符串，不返回错误
	Get(ctx context.Context, key string) (string, error)

	// Delete 删除一个键
	// 用于 Worker 下线时清理心跳记录，或删除历史/调度配置
	Delete(ctx context.Context, key string) error

	// Watch 监听指定前缀的键变化
	// Master 通过此接口监听 Worker 心跳和任务状态变更；
	// Worker 通过此接口监听新任务下发。
	// 返回一个只读的 Event 通道，当调用方取消 ctx 时监听自动停止。
	Watch(ctx context.Context, prefix string) (<-chan Event, error)

	// WatchWithFilter 监听指定前缀下的 key 变更，仅传递匹配 filter 条件的事件
	// filter 函数返回 true 表示该事件应传递给调用方，false 则丢弃
	// filter 中可修改 event.Payload 以传递预解析数据，避免事件循环中重复反序列化
	// 用于 Scheduler 按工作流名过滤任务事件，避免全局 Watch 的性能开销
	WatchWithFilter(ctx context.Context, prefix string, filter func(*Event) bool) (<-chan Event, error)

	// PutWithTTL 写入一个键值对并设置 TTL（秒），过期后自动删除
	// 用于任务 key 的自动清理，防止 etcd 中积压已完成的任务数据
	PutWithTTL(ctx context.Context, key, value string, ttlSec int64) error

	// BatchPutWithTTL 批量写入键值对，共享同一个 lease，减少 etcd 的 lease 管理开销
	// 所有 key 使用相同的 TTL，通过 etcd 事务（Txn）实现原子批量写入。
	// 高并发场景下（大量工作流同时启动），相比逐个 PutWithTTL 能显著减少 etcd 压力。
	BatchPutWithTTL(ctx context.Context, kvs []KeyValue, ttlSec int64) error

	// List 列出指定前缀下的所有键值对
	// 用于启动时恢复调度规则、历史记录等全量数据
	List(ctx context.Context, prefix string) ([]*KeyValue, error)

	// Lock 获取一个分布式锁
	// 用于防止多个 Worker 并发抢占同一个任务的排他控制
	// 返回的 UnlockFunc 必须在任务处理完成后调用以释放锁
	Lock(ctx context.Context, key string) (UnlockFunc, error)

	// CASPut 基于乐观锁（CAS）的原子写入
	// 仅当 key 的当前 ModRevision == expectedRevision 时才写入成功
	// 返回 (true, newRevision, nil) 表示写入成功
	// 返回 (false, 0, nil) 表示版本冲突（key 已被其他进程修改）
	// 用于高并发场景下替代互斥锁，减少 etcd session/lease 开销
	CASPut(ctx context.Context, key, value string, expectedRevision int64) (bool, int64, error)

	// GetWithRevision 获取 key 的值及其 ModRevision（用于 CAS 写入）
	GetWithRevision(ctx context.Context, key string) (value string, revision int64, err error)

	// Ping 检查存储连接是否正常，返回延迟（用于健康检查端点）
	Ping(ctx context.Context) (latency time.Duration, err error)

	// Prefixes 返回当前存储使用的路径前缀配置
	Prefixes() Prefixes

	// Close 关闭存储连接，释放资源
	Close() error
}

// KeyValue 键值对，用于 List 返回
type KeyValue struct {
	Key   string
	Value string
}

// Event 代表存储中发生的数据变更事件
type Event struct {
	Type    EventType     // 事件类型：EventPut（写入）或 EventDelete（删除）
	Key     string        // 发生变化的键的完整路径
	Value   string        // 变更后的最新值
	Payload *TaskPayload  // 可选：filter 阶段解析后的负载，避免事件循环中重复反序列化
}

// EventType 事件类型
type EventType int

const (
	EventPut    EventType = iota // 键被创建或更新
	EventDelete                  // 键被删除
)

// UnlockFunc 释放分布式锁的函数
// 由 Lock 方法返回，调用方在任务处理完成后调用此函数来释放锁
type UnlockFunc func() error
