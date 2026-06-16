package model

import "time"

// WorkerStatus Worker 存活状态
type WorkerStatus string

const (
	WorkerStatusAlive WorkerStatus = "alive" // Worker 在线，心跳正常
	WorkerStatusDead  WorkerStatus = "dead"  // Worker 离线或失联
)

// WorkerState Worker 的运行时快照，由心跳事件驱动更新
type WorkerState struct {
	WorkerID  string       `json:"worker_id"`  // Worker 唯一标识
	Tasks     []string     `json:"tasks"`      // Worker 注册的任务类型列表
	Status    WorkerStatus `json:"status"`     // alive / dead
	StartedAt time.Time    `json:"started_at"` // Worker 启动时间
	AliveAt   time.Time    `json:"alive_at"`   // 最近一次心跳时间
}
