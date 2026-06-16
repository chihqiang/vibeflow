# 部署与快速开始

## Docker 构建

```bash
# 构建 Master 镜像
docker build -f cmd/master/Dockerfile -t vibeflow-master .

# 构建 Worker 镜像
docker build -f cmd/worker/Dockerfile -t vibeflow-worker .
```

## 运行

```bash
# 启动基础服务（MySQL + etcd）
docker compose -f deploy/docker-compose.yaml up -d

# 运行 Master（前台输出日志，Ctrl+C 停止）
docker run --rm -p 8080:8080 -v $(pwd)/config.yaml:/app/config.yaml vibeflow-master

# 运行 Worker（前台输出日志，Ctrl+C 停止）
docker run --rm -v $(pwd)/config.yaml:/app/config.yaml vibeflow-worker
```

---

## 快速开始

### 1. 启动基础服务

```bash
docker compose -f deploy/docker-compose.yaml up -d
```

启动 MySQL 8.0 和 etcd v3.5。

### 2. 启动 Master

```bash
go run ./cmd/master --config config.yaml
```

Master 监听 `:8080`，提供 Web 界面和 REST API。

### 3. 启动 Worker

```bash
go run ./cmd/worker --config config.yaml
```

Worker 向 etcd 注册，等待 Master 分配任务。

### 4. 访问 Web 界面

打开 `http://localhost:8080`，可以创建工作流、提交执行、查看状态和历史。

### 5. 快速体验

```bash
# 提交一个串行工作流：抓取网页 → 保存到本地
curl -X POST http://localhost:8080/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "快速体验",
    "task_groups": [
      [{"name": "fetch_url", "params": {"url": "https://www.example.com"}}],
      [{"name": "write_file", "params": {"file_path": "/tmp/vibeflow-demo.html"}}]
    ],
    "trigger": "manual"
  }'

# 查看结果（替换 <UUID> 为上面返回的 uuid）
curl http://localhost:8080/api/v1/workflows/<UUID> | python3 -m json.tool
```

更多场景请参考 [使用案例](./usage-examples.md)。
