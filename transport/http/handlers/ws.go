package handlers

import (
	"net/http"

	"chihqiang/vibeflow/infra/ws"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// wsUpgrader WebSocket 升级器，允许所有来源的连接
var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// HandleWebSocket 处理 WebSocket 连接升级，将连接注册到 WSEvent
// 支持 ?workflow=xxx 查询参数，订阅特定工作流的事件以减少广播风暴
func HandleWebSocket(event *ws.WSEvent) gin.HandlerFunc {
	return func(c *gin.Context) {
		conn, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			return
		}
		workflow := c.Query("workflow")
		event.ServeWS(conn, workflow)
	}
}
