// Package ws 提供 WebSocket 事件总线实现
// 负责管理客户端连接和消息广播，支持按工作流路由广播以减少风暴
package ws

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"chihqiang/vibeflow/domain/model"
	"chihqiang/vibeflow/infra/config"
	"chihqiang/vibeflow/infra/logger"

	"github.com/gorilla/websocket"
)

// DefaultBroadcastTimeout 广播消息的默认超时时间
// 用于 BroadcastTimeout 的便捷调用，避免调用方散布魔法数字
const DefaultBroadcastTimeout = 2 * time.Second

// defaultBroadcastBuffer 广播通道默认缓冲区大小，当配置值 <= 0 时兜底
const defaultBroadcastBuffer = 256

// defaultClientBuffer 客户端消息通道默认缓冲区大小，当配置值 <= 0 时兜底
const defaultClientBuffer = 64

// WSClient 单个 WebSocket 客户端连接
type WSClient struct {
	event       *WSEvent
	conn        *websocket.Conn
	send        chan []byte // 待发送消息的缓冲通道
	workflow    string      // 客户端订阅的工作流名称（空串表示订阅全局事件）
	closeConnOnce sync.Once  // 确保 conn.Close 只调用一次
	closeSendOnce sync.Once  // 确保 send channel 只关闭一次
}

// routedMessage 带路由信息的广播消息
type routedMessage struct {
	data      []byte
	workflow  string // 目标工作流，空串表示全局广播
}

// WSEvent WebSocket 事件总线，负责管理客户端连接和消息广播
// 支持按工作流路由：客户端可订阅特定工作流的事件，减少广播风暴
//
// 高并发优化：全局广播和工作流广播使用两个独立 channel + 独立消费 goroutine，
// 消除单一 broadcast channel 的竞争瓶颈。多工作流场景下，全局事件（如 worker
// 状态变更）不会阻塞工作流事件，反之亦然。
type WSEvent struct {
	mu sync.Mutex
	// clients 所有客户端（用于注销和全局广播）
	clients map[*WSClient]bool
	// workflowClients 按工作流名称索引的客户端集合（用于按工作流广播）
	workflowClients map[string]map[*WSClient]bool
	// globalBroadcast 全局广播通道（所有客户端）
	globalBroadcast chan routedMessage
	// workflowBroadcast 工作流广播通道（按工作流路由）
	workflowBroadcast chan routedMessage
	register          chan *WSClient // 客户端注册通道
	unregister        chan *WSClient // 客户端注销通道
	conf              *config.WSConfig // WebSocket 配置
}

// NewWSEvent 创建 WSEvent 实例
func NewWSEvent(wsCfg *config.WSConfig) *WSEvent {
	broadcastBuf := wsCfg.BroadcastBuffer
	if broadcastBuf <= 0 {
		broadcastBuf = defaultBroadcastBuffer
	}
	return &WSEvent{
		clients:           make(map[*WSClient]bool),
		workflowClients:   make(map[string]map[*WSClient]bool),
		globalBroadcast:   make(chan routedMessage, broadcastBuf),
		workflowBroadcast: make(chan routedMessage, broadcastBuf),
		register:          make(chan *WSClient),
		unregister:        make(chan *WSClient),
		conf:              wsCfg,
	}
}

// Run 启动事件总线的消息循环，处理客户端的注册/注销和消息广播。
// 使用两个独立 goroutine 分别消费全局广播和工作流广播通道，
// 消除单一 channel 的竞争瓶颈。高并发多工作流场景下互不阻塞。
func (e *WSEvent) Run() {
	// 全局广播消费者：处理无 workflow 路由的消息（如 worker 状态、系统事件）
	go e.consumeGlobalBroadcast()
	// 工作流广播消费者：处理按 workflow 路由的消息
	go e.consumeWorkflowBroadcast()

	// 主循环：仅处理注册/注销
	for {
		select {
		case client := <-e.register:
			e.mu.Lock()
			e.clients[client] = true
			if client.workflow != "" {
				if e.workflowClients[client.workflow] == nil {
					e.workflowClients[client.workflow] = make(map[*WSClient]bool)
				}
				e.workflowClients[client.workflow][client] = true
			}
			e.mu.Unlock()
			logger.Info("WebSocket 客户端已连接",
				"total", len(e.clients),
				"workflow", client.workflow,
			)

		case client := <-e.unregister:
			e.mu.Lock()
			if _, ok := e.clients[client]; ok {
				delete(e.clients, client)
				client.closeSend()
				if client.workflow != "" {
					delete(e.workflowClients[client.workflow], client)
					if len(e.workflowClients[client.workflow]) == 0 {
						delete(e.workflowClients, client.workflow)
					}
				}
			}
			e.mu.Unlock()
		}
	}
}

// consumeGlobalBroadcast 消费全局广播通道，向所有客户端发送消息。
func (e *WSEvent) consumeGlobalBroadcast() {
	for msg := range e.globalBroadcast {
		e.mu.Lock()
		e.sendToAllLocked(msg.data)
		e.mu.Unlock()
	}
}

// consumeWorkflowBroadcast 消费工作流广播通道，按工作流路由消息。
func (e *WSEvent) consumeWorkflowBroadcast() {
	for msg := range e.workflowBroadcast {
		e.mu.Lock()
		e.sendToWorkflowLocked(msg.workflow, msg.data)
		e.mu.Unlock()
	}
}

// sendToWorkflowLocked 向订阅了指定工作流的所有客户端发送消息
// 调用方必须持有 e.mu
// 先在锁内复制客户端列表，避免在持有锁期间做 channel I/O 和 close 操作
func (e *WSEvent) sendToWorkflowLocked(workflow string, data []byte) {
	wfClients := e.workflowClients[workflow]
	clients := make([]*WSClient, 0, len(wfClients))
	for client := range wfClients {
		clients = append(clients, client)
	}
	e.mu.Unlock()

	for _, client := range clients {
		e.sendToClient(client, data)
	}
	e.mu.Lock()
}

// sendToAllLocked 向所有客户端发送消息
// 调用方必须持有 e.mu
// 先在锁内复制客户端列表，避免在持有锁期间做 channel I/O 和 close 操作
func (e *WSEvent) sendToAllLocked(data []byte) {
	clients := make([]*WSClient, 0, len(e.clients))
	for client := range e.clients {
		clients = append(clients, client)
	}
	e.mu.Unlock()

	for _, client := range clients {
		e.sendToClient(client, data)
	}
	e.mu.Lock()
}

// sendToClient 向单个客户端发送消息
// 通道满时清理客户端并关闭其 send 通道（使用 sync.Once 防止重复关闭）
// 调用方不应持有 e.mu
func (e *WSEvent) sendToClient(client *WSClient, data []byte) {
	select {
	case client.send <- data:
		return
	default:
	}
	// 客户端 send 通道已满，清理客户端
	e.mu.Lock()
	if _, ok := e.clients[client]; ok {
		delete(e.clients, client)
		if client.workflow != "" {
			delete(e.workflowClients[client.workflow], client)
			if len(e.workflowClients[client.workflow]) == 0 {
				delete(e.workflowClients, client.workflow)
			}
		}
		client.closeSend()
	}
	e.mu.Unlock()
}

// broadcastToChannel 将消息发送到指定的广播通道
func (e *WSEvent) broadcastToChannel(ch chan routedMessage, msg model.WSMessage, workflow string) {
	data, err := json.Marshal(msg)
	if err != nil {
		logger.Warn("WebSocket 广播序列化失败", "error", err)
		return
	}
	select {
	case ch <- routedMessage{data: data, workflow: workflow}:
	default:
		logger.Warn("WebSocket 广播通道已满，丢弃消息", "workflow", workflow)
	}
}

// broadcastToChannelCtx 将消息发送到指定的广播通道，带超时
func (e *WSEvent) broadcastToChannelCtx(ctx context.Context, ch chan routedMessage, msg model.WSMessage, workflow string) {
	data, err := json.Marshal(msg)
	if err != nil {
		logger.Warn("WebSocket 广播序列化失败", "error", err)
		return
	}
	select {
	case ch <- routedMessage{data: data, workflow: workflow}:
	case <-ctx.Done():
		logger.Warn("WebSocket 广播超时，丢弃消息", "type", msg.Type, "workflow", workflow)
	}
}

// Broadcast 向所有连接的客户端广播一个事件消息
// 非阻塞发送：如果广播通道已满，丢弃消息并记录警告
func (e *WSEvent) Broadcast(msg model.WSMessage) {
	e.broadcastToChannel(e.globalBroadcast, msg, "")
}

// BroadcastToWorkflow 向订阅了指定工作流的所有客户端广播一个事件消息
// 非阻塞发送：如果广播通道已满，丢弃消息并记录警告
func (e *WSEvent) BroadcastToWorkflow(workflow string, msg model.WSMessage) {
	e.broadcastToChannel(e.workflowBroadcast, msg, workflow)
}

// BroadcastTimeout 向所有连接的客户端广播一个事件消息，带超时
// 便捷方法，内部创建 context.WithTimeout 后调用 BroadcastCtx
func (e *WSEvent) BroadcastTimeout(msg model.WSMessage, timeout time.Duration) {
	e.BroadcastTimeoutToWorkflow("", msg, timeout)
}

// BroadcastTimeoutToWorkflow 向订阅了指定工作流的客户端广播一个事件消息，带超时
func (e *WSEvent) BroadcastTimeoutToWorkflow(workflow string, msg model.WSMessage, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	e.BroadcastCtxToWorkflow(ctx, workflow, msg)
}

// BroadcastCtx 向所有连接的客户端广播一个事件消息
// 带超时的阻塞发送：用于关键事件（如 workflow_failed），确保不丢失
func (e *WSEvent) BroadcastCtx(ctx context.Context, msg model.WSMessage) {
	e.BroadcastCtxToWorkflow(ctx, "", msg)
}

// BroadcastCtxToWorkflow 向订阅了指定工作流的客户端广播一个事件消息，带超时
func (e *WSEvent) BroadcastCtxToWorkflow(ctx context.Context, workflow string, msg model.WSMessage) {
	ch := e.globalBroadcast
	if workflow != "" {
		ch = e.workflowBroadcast
	}
	e.broadcastToChannelCtx(ctx, ch, msg, workflow)
}

// broadcastNonBlock 非阻塞广播，内部使用
func (e *WSEvent) broadcastNonBlock(msg model.WSMessage, workflow string) {
	ch := e.globalBroadcast
	if workflow != "" {
		ch = e.workflowBroadcast
	}
	e.broadcastToChannel(ch, msg, workflow)
}

// ServeWS 处理一个新的 WebSocket 连接，注册客户端并启动读写协程
// workflow 为空时客户端订阅全局事件，非空时仅接收该工作流的事件
func (e *WSEvent) ServeWS(conn *websocket.Conn, workflow string) {
	clientBuf := e.conf.ClientBuffer
	if clientBuf <= 0 {
		clientBuf = defaultClientBuffer
	}
	client := &WSClient{
		event:    e,
		conn:     conn,
		send:     make(chan []byte, clientBuf),
		workflow: workflow,
	}
	e.register <- client

	go client.writePump()
	client.readPump()
}

// closeConn 线程安全地关闭 WebSocket 连接（只关闭一次）
func (c *WSClient) closeConn() {
	c.closeConnOnce.Do(func() {
		c.conn.Close()
	})
}

// closeSend 线程安全地关闭 send channel（只关闭一次）
// 用于防止 unregister 和 sendToClient 重复关闭导致 panic
func (c *WSClient) closeSend() {
	c.closeSendOnce.Do(func() {
		close(c.send)
	})
}

// readPump 持续读取 WebSocket 消息（目前仅用于维持连接，忽略收到的消息）
func (c *WSClient) readPump() {
	defer func() {
		c.event.unregister <- c
		c.closeConn()
	}()
	conf := c.event.conf
	c.conn.SetReadLimit(int64(conf.MaxMessageSize))
	c.conn.SetReadDeadline(time.Now().Add(conf.ReadTimeout.ToDuration()))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(conf.ReadTimeout.ToDuration()))
		return nil
	})
	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

// writePump 持续向客户端发送消息，定时发送 ping 保持连接
func (c *WSClient) writePump() {
	conf := c.event.conf
	ticker := time.NewTicker(conf.PingInterval.ToDuration())
	defer func() {
		ticker.Stop()
		c.closeConn()
	}()
	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			c.conn.SetWriteDeadline(time.Now().Add(conf.WriteTimeout.ToDuration()))
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(conf.WriteTimeout.ToDuration()))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
