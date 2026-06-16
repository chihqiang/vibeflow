package app

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"chihqiang/vibeflow/infra/engine/scheduler"
	"chihqiang/vibeflow/infra/config"
	"chihqiang/vibeflow/infra/logger"
	"chihqiang/vibeflow/infra/store"
	"chihqiang/vibeflow/infra/store/etcd"
	"chihqiang/vibeflow/infra/plugin"
	"chihqiang/vibeflow/infra/store/mysql"
	"chihqiang/vibeflow/infra/tracing"
	"chihqiang/vibeflow/infra/ws"
	"chihqiang/vibeflow/transport/http/server"
)

// Master 主节点应用
type Master struct {
	Cfg     *config.Config
	Etcd    store.Store
	History store.HistoryStore
	Sch     *scheduler.Scheduler
	Server  *server.Server
}

// NewMaster 创建并初始化 Master 应用
func NewMaster(cfg *config.Config) (*Master, error) {
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

	var hs store.HistoryStore
	if cfg.MySQL.Host != "" {
		db, err := mysql.OpenDB(&cfg.MySQL)
		if err != nil {
			return nil, err
		}

		hs, err = mysql.NewHistoryStore(db)
		if err != nil {
			return nil, err
		}
		logger.Info("MySQL 历史记录存储已初始化")
	}

	wsEvent := ws.NewWSEvent(&cfg.Master.WS)
	go wsEvent.Run()

	sch := scheduler.NewScheduler(cli, hs, &cfg.Master, wsEvent)

	m := &Master{
		Cfg:     cfg,
		Etcd:    cli,
		History: hs,
		Sch:     sch,
	}
	plugin.ApplyToMaster(sch)

	srv := server.NewServer(sch, wsEvent, cli, hs, &cfg.Master)
	m.Server = srv

	return m, nil
}

// Run 启动 Master 并等待信号优雅退出
func (m *Master) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := m.Server.Start(ctx); err != nil {
			logger.Error("HTTP 服务异常退出", "error", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("正在关闭 Master...")
	cancel()
	m.Sch.Shutdown()
	time.Sleep(m.Cfg.Master.ShutdownDelay.ToDuration())
	return nil
}

// Close 清理资源
func (m *Master) Close() {
	m.Etcd.Close()
	tracing.Shutdown()
	logger.Sync()
}
