package etcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"chihqiang/vibeflow/infra/config"
	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/infra/store"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// EtcdStore store.Store 接口的 etcd 实现
// 所有数据操作（读写、监听、分布式锁）均通过 etcd client v3 API 完成
type EtcdStore struct {
	client   *clientv3.Client // etcd 客户端连接
	lockTTL  int              // 分布式锁的 lease TTL（秒），锁超时后自动释放
	prefixes store.Prefixes   // key 路径前缀，所有读写操作基于此前缀组织
}

// NewEtcdStore 创建 EtcdStore 实例
// cfg 提供 etcd 连接参数（endpoints、dial_timeout、lock_ttl、tls）
// prefixes 定义各业务类型的 key 路径前缀
// 创建时会验证 etcd 连通性，连接失败则返回 error
func NewEtcdStore(cfg *config.EtcdConfig, prefixes store.Prefixes) (*EtcdStore, error) {
	clientCfg := clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout.ToDuration(),
	}

	// TLS 配置：生产环境建议启用，保护 etcd 通信数据安全
	if cfg.TLS.Enabled {
		tlsCfg, err := buildTLSConfig(cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("构建 etcd TLS 配置失败: %w", err)
		}
		clientCfg.TLS = tlsCfg
	}

	cli, err := clientv3.New(clientCfg)
	if err != nil {
		return nil, err
	}

	// 健康检查：确保 etcd 真正可达
	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout.ToDuration())
	defer cancel()
	if _, err := cli.Get(ctx, "health-check"); err != nil {
		cli.Close()
		return nil, fmt.Errorf("etcd 连接失败: %w", err)
	}

	return &EtcdStore{client: cli, lockTTL: cfg.LockTTL, prefixes: prefixes}, nil
}

// Close 关闭 etcd 客户端连接
// 程序退出前必须调用，否则可能导致连接泄漏
func (e *EtcdStore) Close() error {
	return e.client.Close()
}

// Ping 检查 etcd 连接是否正常，测量并返回延迟
// 通过读写一个临时 key 来验证完整的 etcd 读写链路
func (e *EtcdStore) Ping(ctx context.Context) (time.Duration, error) {
	healthKey := "/vibeflow/health-check"
	start := time.Now()
	if _, err := e.client.Put(ctx, healthKey, "1"); err != nil {
		return time.Since(start), err
	}
	latency := time.Since(start)
	// 清理临时 key，忽略错误（不影响检查结果）
	_, _ = e.client.Delete(context.Background(), healthKey)
	return latency, nil
}

// Prefixes 返回存储路径前缀配置
func (e *EtcdStore) Prefixes() store.Prefixes {
	return e.prefixes
}

// Put 写入一个键值对到 etcd
// 如果 key 已存在则覆盖
func (e *EtcdStore) Put(ctx context.Context, key, value string) error {
	_, err := e.client.Put(ctx, key, value)
	return err
}

// Delete 从 etcd 删除一个键
// 如果 key 不存在则静默成功，不返回错误
func (e *EtcdStore) Delete(ctx context.Context, key string) error {
	_, err := e.client.Delete(ctx, key)
	return err
}

// Get 获取指定 key 的值
// 返回空字符串表示 key 不存在（与 etcd 的 "" 值无法区分，调用方需自行约定）
func (e *EtcdStore) Get(ctx context.Context, key string) (string, error) {
	resp, err := e.client.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

// PutWithTTL 写入一个键值对并设置 TTL（秒），过期后自动删除
// 用于任务 key 的自动清理，防止 etcd 中积压已完成的任务数据
const taskTTL = 86400 // 默认任务 TTL 24 小时

func (e *EtcdStore) PutWithTTL(ctx context.Context, key, value string, ttlSec int64) error {
	if ttlSec <= 0 {
		ttlSec = int64(taskTTL)
	}
	lease, err := e.client.Grant(ctx, ttlSec)
	if err != nil {
		return err
	}
	_, err = e.client.Put(ctx, key, value, clientv3.WithLease(lease.ID))
	if err != nil {
		// 写入失败时撤销 lease，避免泄漏
		if _, revokeErr := e.client.Revoke(context.Background(), lease.ID); revokeErr != nil {
			logger.Warn("撤销 lease 失败", "lease_id", lease.ID, "error", revokeErr)
		}
	}
	return err
}

// BatchPutWithTTL 批量写入键值对，共享同一个 lease
// 通过 etcd Txn 事务实现：先 Grant 一个 lease，然后为每个 key 构建一个 Put 操作，
// 所有操作在同一个事务中执行，原子写入，共享同一个 lease ID。
// 相比逐个 PutWithTTL（每个任务创建一个 lease），批量写入显著减少 etcd 的 lease 管理开销。
func (e *EtcdStore) BatchPutWithTTL(ctx context.Context, kvs []store.KeyValue, ttlSec int64) error {
	if len(kvs) == 0 {
		return nil
	}
	if ttlSec <= 0 {
		ttlSec = int64(taskTTL)
	}

	// 共享一个 lease，减少 etcd 的 lease 管理开销
	lease, err := e.client.Grant(ctx, ttlSec)
	if err != nil {
		return fmt.Errorf("grant lease for batch put: %w", err)
	}

	// 构建 Txn 操作列表
	ops := make([]clientv3.Op, 0, len(kvs))
	for _, kv := range kvs {
		ops = append(ops, clientv3.OpPut(kv.Key, kv.Value, clientv3.WithLease(lease.ID)))
	}

	// 事务执行：全部成功或全部失败
	_, err = e.client.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		// 写入失败时撤销 lease，避免泄漏
		if _, revokeErr := e.client.Revoke(context.Background(), lease.ID); revokeErr != nil {
			logger.Warn("撤销 lease 失败", "lease_id", lease.ID, "error", revokeErr)
		}
		return fmt.Errorf("batch put txn: %w", err)
	}
	return nil
}

// List 列出指定前缀下的所有键值对
// 返回结果按 key 的字典序排列（etcd 默认行为）
func (e *EtcdStore) List(ctx context.Context, prefix string) ([]*store.KeyValue, error) {
	resp, err := e.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	kvs := make([]*store.KeyValue, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		kvs = append(kvs, &store.KeyValue{
			Key:   string(kv.Key),
			Value: string(kv.Value),
		})
	}
	return kvs, nil
}

// Watch 监听指定前缀下的所有 key 变更
// 返回的 channel 会在以下情况下关闭：
//   - 传入的 ctx 被取消或超时
//   - etcd 连接断开
//
// 当消费端处理速度较慢时，事件会被丢弃（非阻塞发送），防止阻塞 etcd watch 流
func (e *EtcdStore) Watch(ctx context.Context, prefix string) (<-chan store.Event, error) {
	return e.watchInternal(ctx, prefix, nil)
}

// WatchWithFilter 监听指定前缀下的 key 变更，仅传递匹配 filter 条件的事件
// filter 函数返回 true 表示该事件应传递给调用方，false 则丢弃
// filter 中可修改 event.Payload 以传递预解析数据，避免事件循环中重复反序列化
// 用于 Scheduler 按工作流名过滤任务事件，避免全局 Watch 的性能开销
func (e *EtcdStore) WatchWithFilter(ctx context.Context, prefix string, filter func(event *store.Event) bool) (<-chan store.Event, error) {
	return e.watchInternal(ctx, prefix, filter)
}

// watchEventChanBuf Watch 事件通道的缓冲区大小
// 高并发场景下（数百个工作流同时执行），etcd 事件产生速率可能远超消费速率，
// 较大的缓冲区可减少事件丢弃，降低对 rescanAndRedispatchTasks 断连恢复的依赖。
// 增大到 2048 以应对高负载场景下 Worker 端消费不及时的情况。
const watchEventChanBuf = 2048

// watchInternal Watch 的内部实现，支持可选的过滤函数
func (e *EtcdStore) watchInternal(ctx context.Context, prefix string, filter func(*store.Event) bool) (<-chan store.Event, error) {
	watchChan := e.client.Watch(ctx, prefix, clientv3.WithPrefix())
	eventChan := make(chan store.Event, watchEventChanBuf)
	var dropped atomic.Int64
	go func() {
		defer close(eventChan)
		// 定期输出丢弃计数，避免高并发下事件丢失无感知
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		go func() {
			for range ticker.C {
				if n := dropped.Swap(0); n > 0 {
					logger.Warn("etcd watch event dropped due to slow consumer", "prefix", prefix, "dropped", n)
				}
			}
		}()
		for watchResp := range watchChan {
			for _, ev := range watchResp.Events {
				event := store.Event{
					Key:   string(ev.Kv.Key),
					Value: string(ev.Kv.Value),
				}
				if ev.Type == clientv3.EventTypePut {
					event.Type = store.EventPut
				} else if ev.Type == clientv3.EventTypeDelete {
					event.Type = store.EventDelete
				}
				if filter != nil && !filter(&event) {
					continue
				}
				select {
				case eventChan <- event:
				default:
					dropped.Add(1)
					// 每次丢弃时记录 WARN 日志，包含 key 信息便于排查
					logger.Warn("etcd watch event dropped (channel full)", "prefix", prefix, "key", event.Key)
				}
			}
		}
	}()
	return eventChan, nil
}

// Lock 获取一个指定 key 的分布式锁
// 基于 etcd concurrency 包实现，利用 lease 自动过期防止死锁
// 调用方必须在任务完成后调用返回的 UnlockFunc 释放锁
// 如果锁已被其他进程持有，将阻塞直到锁可用或 ctx 取消
func (e *EtcdStore) Lock(ctx context.Context, key string) (store.UnlockFunc, error) {
	session, err := concurrency.NewSession(e.client, concurrency.WithTTL(e.lockTTL))
	if err != nil {
		return nil, err
	}
	mutex := concurrency.NewMutex(session, key)
	if err := mutex.Lock(ctx); err != nil {
		session.Close()
		return nil, err
	}
	unlockFunc := func() error {
		defer session.Close()
		// 释放锁时使用带超时的 context，防止 goroutine 泄漏
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return mutex.Unlock(unlockCtx)
	}
	return unlockFunc, nil
}

// GetWithRevision 获取 key 的值及其 ModRevision
// ModRevision 是 etcd 的全局递增版本号，用于 CAS 乐观锁写入
func (e *EtcdStore) GetWithRevision(ctx context.Context, key string) (string, int64, error) {
	resp, err := e.client.Get(ctx, key)
	if err != nil {
		return "", 0, err
	}
	if len(resp.Kvs) == 0 {
		return "", 0, nil
	}
	return string(resp.Kvs[0].Value), resp.Kvs[0].ModRevision, nil
}

// CASPut 基于乐观锁（CAS）的原子写入
// 使用 etcd Txn 实现：if ModRevision == expectedRevision then Put else fail
// 仅当 key 的当前 ModRevision 与预期值匹配时才写入，否则返回冲突
// 相比互斥锁（每次需要创建 session + lease），CAS 方式无额外开销，适合高并发场景
func (e *EtcdStore) CASPut(ctx context.Context, key, value string, expectedRevision int64) (bool, int64, error) {
	// 使用 etcd Txn 实现 CAS：
	// If: ModRevision == expectedRevision
	// Then: Put(key, value)
	// Else: Get(key) 获取当前版本号供调用方重试
	txnResp, err := e.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", expectedRevision)).
		Then(clientv3.OpPut(key, value)).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return false, 0, err
	}

	if txnResp.Succeeded {
		// Then 分支执行成功：Put 操作完成，从响应中获取新版本号
		newRevision := txnResp.Header.Revision
		return true, newRevision, nil
	}

	// Else 分支：版本冲突，从 Get 响应中获取当前版本号
	if len(txnResp.Responses) > 0 {
		getResp := txnResp.Responses[0].GetResponseRange()
		if getResp != nil && len(getResp.Kvs) > 0 {
			return false, getResp.Kvs[0].ModRevision, nil
		}
	}
	return false, 0, nil
}

// buildTLSConfig 根据配置构建 TLS 配置
// 支持双向认证（客户端证书 + CA 证书）和单向认证（仅 CA 证书）
func buildTLSConfig(cfg config.EtcdTLSConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// CA 证书：验证服务端身份
	if cfg.CAFile != "" {
		caData, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("读取 CA 证书失败 %s: %w", cfg.CAFile, err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caData) {
			return nil, fmt.Errorf("解析 CA 证书失败 %s", cfg.CAFile)
		}
		tlsCfg.RootCAs = caPool
	}

	// 客户端证书：双向 TLS 认证
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("加载客户端证书失败 cert=%s key=%s: %w", cfg.CertFile, cfg.KeyFile, err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}
