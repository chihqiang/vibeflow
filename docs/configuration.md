# 配置说明

```yaml
# MySQL 连接
mysql:
  host: localhost
  port: 3306
  user: root
  password: root
  database: vibeflow
  charset: utf8mb4
  max_idle: 10
  max_open: 100
  max_conn_lifetime: "1h" # 连接最大存活时间

# etcd 连接
etcd:
  endpoints: ["localhost:2379"]
  dial_timeout: "5s"
  lock_ttl: 10 # 分布式锁 TTL（秒）
  # tls:                          # 可选 TLS
  #   enabled: true
  #   cert_file: /path/to/client.pem
  #   key_file: /path/to/client-key.pem
  #   ca_file: /path/to/ca.pem

# etcd Key 前缀
prefixes:
  tasks: "/vibeflow/tasks/" # 任务 key 前缀
  heartbeats: "/vibeflow/heartbeats/" # 心跳 key 前缀
  workflows: "/vibeflow/workflows/" # 工作流 key 前缀

# Master 配置
master:
  http_addr: ":8080" # HTTP 监听地址
  max_concurrent_tasks: 250 # 全局最大并发任务数（0=不限制）
  per_workflow_max_concurrency: 100 # 单工作流最大并发任务数（0=不限制）
  pending_task_timeout: "5m" # 排队任务超时淘汰，超时未下发则标记失败
  shutdown_timeout: "5s" # 优雅关闭超时
  shutdown_delay: "200ms" # 关闭前等待
  default_task_timeout: "30s" # 默认任务超时
  default_base_backoff: "1s" # 默认重试退避基数
  stale_check_interval: "15s" # Worker 过期检查间隔
  heartbeat_stale: "15s" # 心跳过期阈值
  store_timeout: "5s" # etcd 操作超时
  max_history_cache: 1000 # 内存缓存历史工作流数量（LRU 淘汰到 MySQL）
  cors_max_age: "12h" # CORS 缓存时间
  ws:
    read_timeout: "60s" # WebSocket 读超时
    ping_interval: "30s" # WebSocket 心跳间隔
    write_timeout: "10s" # WebSocket 写超时
    max_message_size: 4096 # 最大消息大小（字节）
    broadcast_buffer: 256 # 广播缓冲区大小
    client_buffer: 64 # 客户端缓冲区大小

# 日志配置
logger:
  dir: runtime/log # 日志目录
  level: info # 日志级别：debug / info / warn / error

# Worker 配置
worker:
  id: "" # Worker ID（空则自动生成）
  heartbeat_interval: "5s" # 心跳间隔
  shutdown_delay: "200ms" # 关闭前等待
  max_task_concurrency: 100 # 最大并发任务数


# 链路追踪配置（可选，不配置不影响程序运行）
# tracing:
#   enabled: true                 # 是否启用链路追踪
#   endpoint: "localhost:4317"    # OTLP gRPC 接收端（Jaeger all-in-one）
#   service_name: "vibeflow"      # 服务名称，在 Jaeger UI 中显示
#   sample_rate: 1.0              # 采样率 0.0-1.0，1.0=全量采集
```

## 配置项说明

### MySQL

| 参数                | 默认值      | 说明             |
| ------------------- | ----------- | ---------------- |
| `host`              | `localhost` | MySQL 主机地址   |
| `port`              | `3306`      | MySQL 端口       |
| `user`              | `root`      | 用户名           |
| `password`          | `root`      | 密码             |
| `database`          | `vibeflow`  | 数据库名         |
| `charset`           | `utf8mb4`   | 字符集           |
| `max_idle`          | `10`        | 最大空闲连接数   |
| `max_open`          | `100`       | 最大打开连接数   |
| `max_conn_lifetime` | `1h`        | 连接最大存活时间 |

### etcd

| 参数           | 默认值               | 说明               |
| -------------- | -------------------- | ------------------ |
| `endpoints`    | `["localhost:2379"]` | etcd 节点地址列表  |
| `dial_timeout` | `5s`                 | 连接超时           |
| `lock_ttl`     | `10`                 | 分布式锁 TTL（秒） |

### Master

| 参数                           | 默认值  | 说明                   |
| ------------------------------ | ------- | ---------------------- |
| `http_addr`                    | `:8080` | HTTP 监听地址          |
| `max_concurrent_tasks`         | `250`   | 全局最大并发任务数     |
| `per_workflow_max_concurrency` | `100`   | 单工作流最大并发任务数 |
| `pending_task_timeout`         | `5m`    | 排队任务超时淘汰时间   |
| `default_task_timeout`         | `30s`   | 默认任务超时           |
| `default_base_backoff`         | `1s`    | 默认重试退避基数       |
| `max_history_cache`            | `1000`  | 内存缓存历史工作流数量 |

### Worker

| 参数                   | 默认值           | 说明           |
| ---------------------- | ---------------- | -------------- |
| `id`                   | `""`（自动生成） | Worker ID      |
| `heartbeat_interval`   | `5s`             | 心跳间隔       |
| `max_task_concurrency` | `100`            | 最大并发任务数 |

### 链路追踪（可选）

| 参数           | 默认值           | 说明              |
| -------------- | ---------------- | ----------------- |
| `enabled`      | `false`          | 是否启用链路追踪  |
| `endpoint`     | `localhost:4317` | OTLP gRPC 接收端  |
| `service_name` | `vibeflow`       | 服务名称          |
| `sample_rate`  | `1.0`            | 采样率（0.0-1.0） |
