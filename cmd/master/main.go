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

	master, err := app.NewMaster(cfg)
	if err != nil {
		logger.Error("初始化 Master 失败", "error", err)
		os.Exit(1)
	}
	defer master.Close()

	if err := master.Run(); err != nil {
		logger.Error("Master 运行异常", "error", err)
	}
}
