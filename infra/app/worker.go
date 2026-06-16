package app

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"chihqiang/vibeflow/infra/engine/runtime"
	"chihqiang/vibeflow/infra/config"
	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/infra/store"
	"chihqiang/vibeflow/infra/plugin"
	"chihqiang/vibeflow/infra/store/etcd"
	"chihqiang/vibeflow/infra/tracing"
)

// Worker 工作节点应用
type Worker struct {
	Cfg  *config.Config
	Etcd store.Store
	Wk   *runtime.Worker
}

// NewWorker 创建并初始化 Worker 应用
func NewWorker(cfg *config.Config) (*Worker, error) {
	if err := logger.Init(cfg.Logger); err != nil {
		return nil, err
	}

	// 初始化链路追踪（可选，未配置时退化为空操作）
	if err := tracing.Init(cfg.Tracing); err != nil {
		logger.Warn("链路追踪初始化失败，将以无追踪模式运行", "error", err)
	}

	cli, err := etcd.NewEtcdStore(&cfg.Etcd, store.Prefixes{
		Tasks:      cfg.Prefixes.Tasks,
		Heartbeats: cfg.Prefixes.Heartbeats,
		Workflows:  cfg.Prefixes.Workflows,
	})
	if err != nil {
		return nil, err
	}

	wk := runtime.NewWorker(cli, &cfg.Worker)

	w := &Worker{
		Cfg:  cfg,
		Etcd: cli,
		Wk:   wk,
	}
	plugin.ApplyToWorker(wk)

	return w, nil
}

// Run 启动 Worker 并等待信号优雅退出
func (w *Worker) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logger.Info("正在关闭 Worker...")
		cancel()
	}()

	if err := w.Wk.Start(ctx); err != nil {
		logger.Error("Worker 已停止", "error", err)
	}
	time.Sleep(w.Cfg.Worker.ShutdownDelay.ToDuration())
	return nil
}

// Close 清理资源
func (w *Worker) Close() {
	w.Etcd.Close()
	tracing.Shutdown()
	logger.Sync()
}
