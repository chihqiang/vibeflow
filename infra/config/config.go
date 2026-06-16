package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration 时间长度类型，基于 time.Duration
// 支持从 YAML 字符串解析（如 "5s"、"30m"、"24h"），兼容 time.ParseDuration 格式
type Duration time.Duration

// ToDuration 转换为标准 time.Duration 类型
func (d Duration) ToDuration() time.Duration {
	return time.Duration(d)
}

// UnmarshalYAML 实现 yaml.Unmarshaler 接口，支持从 YAML 字符串解析 duration
// 格式："300ms"、"5s"、"1.5h"、"2h45m" 等，与 time.ParseDuration 格式一致
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(dur)
	return nil
}

// MySQLConfig MySQL 数据源配置
type MySQLConfig struct {
	Host     string `yaml:"host"`      // 主机地址，默认 "localhost"
	Port     int    `yaml:"port"`      // 端口，默认 3306
	User     string `yaml:"user"`      // 用户名，默认 "root"
	Password string `yaml:"password"`  // 密码，默认 "root"
	Database string `yaml:"database"`  // 数据库名，默认 "vibeflow"
	Charset  string `yaml:"charset"`   // 字符集，默认 "utf8mb4"
	MaxIdle          int    `yaml:"max_idle"`          // 最大空闲连接数，默认 10
	MaxOpen          int    `yaml:"max_open"`          // 最大打开连接数，默认 100
	MaxConnLifetime string `yaml:"max_conn_lifetime"` // 连接最大存活时间，如 "1h"，默认 "1h"
}

// LoggerConfig 日志配置
type LoggerConfig struct {
	Dir   string `yaml:"dir"`   // 日志文件目录，默认 "runtime/log"
	Level string `yaml:"level"` // 日志级别：debug/info/warn/error，默认 "info"
}

// TracingConfig 链路追踪配置
// 可选配置：如果未设置或 enabled=false，则不启动链路追踪，不影响程序正常运行
type TracingConfig struct {
	Enabled     bool    `yaml:"enabled"`      // 是否启用链路追踪，默认 false
	Endpoint    string  `yaml:"endpoint"`     // OTLP gRPC 接收端地址，如 "localhost:4317"
	ServiceName string  `yaml:"service_name"` // 服务名称，显示在 Jaeger UI 中，默认 "vibeflow"
	SampleRate  float64 `yaml:"sample_rate"`  // 采样率 0.0-1.0，1.0=全量，默认 1.0
}

// Config 顶层配置结构，对应 config.yaml 文件内容
// 由 Load() 读取 YAML 文件后填充，可调用 DefaultConfig() 获取默认值
type Config struct {
	Etcd     EtcdConfig     `yaml:"etcd"`     // etcd 连接配置
	MySQL    MySQLConfig    `yaml:"mysql"`    // MySQL 数据源配置，用于持久化工作流历史记录
	Prefixes PrefixesConfig `yaml:"prefixes"` // etcd key 路径前缀
	Master   MasterConfig   `yaml:"master"`   // Master 服务配置
	Worker   WorkerConfig   `yaml:"worker"`   // Worker 服务配置
	Logger   LoggerConfig   `yaml:"logger"`   // 日志配置
	Tracing  TracingConfig  `yaml:"tracing"`  // 链路追踪配置（可选）
}

// EtcdTLSConfig etcd TLS 加密连接配置
// 生产环境建议启用，保护 etcd 通信数据安全
type EtcdTLSConfig struct {
	Enabled  bool   `yaml:"enabled"`   // 是否启用 TLS，默认 false
	CertFile string `yaml:"cert_file"` // 客户端证书文件路径（PEM 格式）
	KeyFile  string `yaml:"key_file"`  // 客户端私钥文件路径（PEM 格式）
	CAFile   string `yaml:"ca_file"`   // CA 证书文件路径（PEM 格式），为空则使用系统根证书池
}

// EtcdConfig etcd 客户端连接参数
type EtcdConfig struct {
	Endpoints   []string     `yaml:"endpoints"`    // etcd 集群地址列表，如 ["localhost:2379"]
	DialTimeout Duration     `yaml:"dial_timeout"` // 连接超时时间，默认 "5s"
	LockTTL     int          `yaml:"lock_ttl"`     // 分布式锁 lease TTL（秒），锁超时后自动释放
	TLS         EtcdTLSConfig `yaml:"tls"`         // TLS 加密连接配置，生产环境建议启用
}

// PrefixesConfig etcd key 路径前缀配置
// 所有读写 etcd 的操作都基于这些前缀组织 key，不同业务类型使用不同前缀避免冲突
type PrefixesConfig struct {
	Tasks      string `yaml:"tasks"`      // 任务下发前缀，Worker 通过 Watch 获取新任务
	Heartbeats string `yaml:"heartbeats"` // Worker 心跳前缀，Master 监控 Worker 存活性
	Workflows  string `yaml:"workflows"`  // 工作流定义持久化前缀
}

// MasterConfig Master 服务配置
type MasterConfig struct {
	HTTPAddr                 string   `yaml:"http_addr"`                  // HTTP 监听地址，如 ":8080"
	MaxConcurrentTasks       int      `yaml:"max_concurrent_tasks"`       // 最大并发执行任务数（全局上限），0 表示不限制
	PerWorkflowMaxConcurrency int     `yaml:"per_workflow_max_concurrency"` // 每个工作流最大并发任务数，0 表示不限制（仅受全局上限控制），默认 0
	PendingTaskTimeout       Duration `yaml:"pending_task_timeout"`       // 排队任务超时时间，超时后自动标记失败，默认 "5m"
	ShutdownTimeout          Duration `yaml:"shutdown_timeout"`           // 优雅关闭超时时间，超过此时间强制退出，默认 "5s"
	ShutdownDelay            Duration `yaml:"shutdown_delay"`             // 收到退出信号后等待新任务派发完成的延迟，默认 "200ms"
	DefaultTaskTimeout       Duration `yaml:"default_task_timeout"`       // 任务默认超时时间，可被工作流级别或任务级别配置覆盖，默认 "30s"
	DefaultBaseBackoff       Duration `yaml:"default_base_backoff"`       // 任务重试默认基础退避时间，指数退避：base * 2^retryCount，默认 "1s"
	StaleCheckInterval       Duration `yaml:"stale_check_interval"`       // 检查 Worker 心跳超时的间隔，默认 "15s"
	HeartbeatStale           Duration `yaml:"heartbeat_stale"`            // Worker 心跳过期阈值，超过此时间未收到心跳标记为 dead，默认 "15s"
	StoreTimeout             Duration `yaml:"store_timeout"`              // etcd 读写操作的 context 超时时间，默认 "5s"
	MaxHistoryCache          int      `yaml:"max_history_cache"`          // 内存中最多缓存的历史工作流数量，默认 1000，超出则按 LRU 淘汰到 MySQL
	CORSMaxAge               Duration `yaml:"cors_max_age"`               // CORS preflight 缓存时间，默认 "12h"
	WS                       WSConfig `yaml:"ws"`                         // WebSocket 连接配置
}

// WSConfig WebSocket 服务端配置
type WSConfig struct {
	ReadTimeout     Duration `yaml:"read_timeout"`      // WebSocket 读取消息超时，默认 "60s"
	PingInterval    Duration `yaml:"ping_interval"`     // 服务端发送 ping 的间隔，默认 "30s"
	WriteTimeout    Duration `yaml:"write_timeout"`     // WebSocket 写入消息超时，默认 "10s"
	MaxMessageSize  int      `yaml:"max_message_size"`  // 单条消息最大字节数，默认 4096
	BroadcastBuffer int      `yaml:"broadcast_buffer"`  // 广播通道缓冲区大小，默认 256
	ClientBuffer    int      `yaml:"client_buffer"`     // 每个客户端消息通道缓冲区大小，默认 64
}

// WorkerConfig Worker 服务配置
type WorkerConfig struct {
	ID                string   `yaml:"id"`                 // Worker 唯一标识，为空时自动生成 "worker-{HHMMSS}"
	HeartbeatInterval Duration `yaml:"heartbeat_interval"` // 心跳上报间隔，默认 "5s"
	ShutdownDelay     Duration `yaml:"shutdown_delay"`     // 收到退出信号后等待当前任务完成的延迟，默认 "200ms"
	MaxTaskConcurrency int    `yaml:"max_task_concurrency"` // 最大并发执行任务数（goroutine 池上限），默认 50，0 表示不限制
}

// DefaultConfig 返回一份默认配置
// 所有字段均有合理默认值，可直接用于本地开发环境
func DefaultConfig() *Config {
	return &Config{
		MySQL: MySQLConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Password: "root",
			Database: "vibeflow",
			Charset:  "utf8mb4",
			MaxIdle:         10,
			MaxOpen:         100,
			MaxConnLifetime: "1h",
		},
		Etcd: EtcdConfig{
			Endpoints:   []string{"localhost:2379"},
			DialTimeout: Duration(5 * time.Second),
			LockTTL:     10,
		},
		Prefixes: PrefixesConfig{
			Tasks:      "/vibeflow/tasks/",
			Heartbeats: "/vibeflow/heartbeats/",
			Workflows:  "/vibeflow/workflows/",
		},
		Master: MasterConfig{
			HTTPAddr:                 ":8080",
			MaxConcurrentTasks:       250,
			PerWorkflowMaxConcurrency: 100,
			PendingTaskTimeout:       Duration(5 * time.Minute),
			ShutdownTimeout:          Duration(5 * time.Second),
			ShutdownDelay:            Duration(200 * time.Millisecond),
			DefaultTaskTimeout:       Duration(30 * time.Second),
			DefaultBaseBackoff:       Duration(time.Second),
			StaleCheckInterval:       Duration(15 * time.Second),
			HeartbeatStale:           Duration(15 * time.Second),
			StoreTimeout:             Duration(5 * time.Second),
			MaxHistoryCache:          1000,
			CORSMaxAge:               Duration(12 * time.Hour),
			WS: WSConfig{
				ReadTimeout:     Duration(60 * time.Second),
				PingInterval:    Duration(30 * time.Second),
				WriteTimeout:    Duration(10 * time.Second),
				MaxMessageSize:  4096,
				BroadcastBuffer: 256,
				ClientBuffer:    64,
			},
		},
		Worker: WorkerConfig{
			ID:                 "",
			HeartbeatInterval:  Duration(5 * time.Second),
			ShutdownDelay:      Duration(200 * time.Millisecond),
			MaxTaskConcurrency: 100,
		},
		Logger: LoggerConfig{
			Dir:   "runtime/log",
			Level: "info",
		},
	}
}

// Load 读取并解析 YAML 格式的配置文件
// path: 配置文件路径，如 "config.yaml"
// 返回合并后的配置：先加载默认值，再用文件中的值覆盖
// 文件不存在或格式错误时返回 error
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

// Validate 校验配置的合法性，检查必填字段和值的合理性
func (c *Config) Validate() error {
	if len(c.Etcd.Endpoints) == 0 {
		return fmt.Errorf("etcd.endpoints 不能为空")
	}
	if c.Prefixes.Tasks == "" {
		return fmt.Errorf("prefixes.tasks 不能为空")
	}
	if c.Prefixes.Heartbeats == "" {
		return fmt.Errorf("prefixes.heartbeats 不能为空")
	}
	if c.Master.HTTPAddr == "" {
		return fmt.Errorf("master.http_addr 不能为空")
	}
	return nil
}
