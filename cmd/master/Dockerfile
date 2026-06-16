# ---- 构建阶段 ----
FROM golang:1.26-alpine AS builder

WORKDIR /build

# 先复制依赖描述文件，利用 Docker 缓存层加速
COPY go.mod go.sum ./
RUN go mod download

# 复制全部源码
COPY . .

# 编译 Master 二进制（静态链接）
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /build/vibeflow-master ./cmd/master

# ---- 运行阶段 ----
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /build/vibeflow-master .

EXPOSE 8080

ENTRYPOINT ["./vibeflow-master", "--config", "config.yaml"]
