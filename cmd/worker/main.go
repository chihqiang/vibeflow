package main

import (
	"flag"
	"os"

	"chihqiang/vibeflow/infra/app"
	"chihqiang/vibeflow/infra/config"
	"chihqiang/vibeflow/infra/logger"
	_ "chihqiang/vibeflow/plugin"
)

func main() {
	configPath := flag.String("config", "config.yaml", "配置文件路径")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("加载配置文件失败", "error", err)
		os.Exit(1)
	}

	worker, err := app.NewWorker(cfg)
	if err != nil {
		logger.Error("初始化 Worker 失败", "error", err)
		os.Exit(1)
	}
	defer worker.Close()

	if err := worker.Run(); err != nil {
		logger.Error("Worker 运行异常", "error", err)
	}
}
