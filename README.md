# turntf-go

`turntf-go` 是 turntf 的 Go SDK，封装了文档里的两类客户端能力：

- WebSocket + Protobuf 长连接客户端
- HTTP JSON 管理与查询客户端

它的目标是让业务代码直接使用 Go API，而不是自己处理 WebSocket 生命周期、protobuf envelope、请求 ID 匹配、`body` 的 base64 编解码和 Bearer token 注入。`Client` 现在除 HTTP 登录外统一走 WebSocket RPC；`HTTPClient` 继续保留给脚本、后台和调试场景独立使用。

## 安装

```bash
go get github.com/tursom/turntf-go
```

## 功能

- WebSocket 首帧登录
- 自动重连与重登录
- `seen_messages` 重放去重
- `MessagePushed` 自动执行 `保存消息 -> 保存游标 -> ack`
- `SendMessage`
- `SendPacket`
- `Ping`
- HTTP 登录
- WS 创建用户、订阅管理、查询消息、发消息、发瞬时包

## 包内容

- `turntf`：高级 SDK API
- `internal/proto`：由 `proto/client.proto` 生成的 protobuf 类型

默认推荐直接用 `turntf` 包，不直接依赖 `ClientEnvelope` / `ServerEnvelope`。

## 快速开始

### 一个 `Client`：HTTP 登录 + WebSocket RPC

```go
package main

import (
	"context"
	"log"
	"time"

	turntf "github.com/tursom/turntf-go"
)

type store struct {
	*turntf.MemoryCursorStore
}

type handler struct{}

func (handler) OnLogin(_ context.Context, info turntf.LoginInfo) {
	log.Printf("login ok: user=%d protocol=%s", info.User.UserID, info.ProtocolVersion)
}

func (handler) OnMessage(_ context.Context, msg turntf.Message) {
	log.Printf("message: seq=%d sender=%s body=%x", msg.Seq, msg.Sender, msg.Body)
}

func (handler) OnPacket(_ context.Context, packet turntf.Packet) {
	log.Printf("packet: id=%d sender=%s", packet.PacketID, packet.Sender)
}

func (handler) OnError(_ context.Context, err error) {
	log.Printf("sdk error: %v", err)
}

func (handler) OnDisconnect(_ context.Context, err error) {
	log.Printf("disconnect: %v", err)
}

func main() {
	client, err := turntf.NewClient(turntf.Config{
		BaseURL: "http://127.0.0.1:8080",
		Credentials: turntf.Credentials{
			NodeID:   4096,
			UserID:   1025,
			Password: "alice-password",
		},
		CursorStore:           store{turntf.NewMemoryCursorStore()},
		Handler:               handler{},
		InitialReconnectDelay: time.Second,
		MaxReconnectDelay:     30 * time.Second,
		PingInterval:          30 * time.Second,
		RequestTimeout:        10 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	token, err := client.Login(ctx, 4096, 1, "root")
	if err != nil {
		log.Fatal(err)
	}

	if err := client.Connect(ctx); err != nil {
		log.Fatal(err)
	}

	_, err = client.CreateUser(ctx, token, turntf.CreateUserRequest{
		Username: "alice",
		Password: "alice-password",
		Role:     "user",
	})
	if err != nil {
		log.Fatal(err)
	}

	_, err = client.SendMessage(ctx, turntf.SendMessageInput{
		Target: turntf.UserRef{NodeID: 4096, UserID: 1025},
		Sender: "mobile",
		Body:   []byte("hello"),
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

如果你只想单独使用 HTTP，也仍然可以保留原来的轻量入口：

```go
package main

import (
	"context"
	"log"

	turntf "github.com/tursom/turntf-go"
)

func main() {
	ctx := context.Background()
	httpClient := turntf.NewHTTPClient("http://127.0.0.1:8080")

	token, err := httpClient.Login(ctx, 4096, 1, "root")
	if err != nil {
		log.Fatal(err)
	}

	user, err := httpClient.CreateUser(ctx, token, turntf.CreateUserRequest{
		Username: "alice",
		Password: "alice-password",
		Role:     "user",
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("created user: node=%d user=%d", user.NodeID, user.UserID)

	err = httpClient.CreateSubscription(
		ctx,
		token,
		turntf.UserRef{NodeID: 4096, UserID: 1025},
		turntf.UserRef{NodeID: 4096, UserID: 1026},
	)
	if err != nil {
		log.Fatal(err)
	}
}
```

## WebSocket API

### `NewClient`

```go
client, err := turntf.NewClient(turntf.Config{...})
```

关键配置项：

- `BaseURL`
  传 `http://host:port` 或 `https://host:port` 即可，SDK 会自动拼成 `/ws/client`
- `Credentials`
  WebSocket 首帧登录使用的 `(node_id, user_id, password)`
- `CursorStore`
  业务侧的消息持久化和游标持久化实现
- `Handler`
  事件回调
- `InitialReconnectDelay`
  自动重连起始退避
- `MaxReconnectDelay`
  自动重连最大退避
- `PingInterval`
  应用层 ping 周期
- `RequestTimeout`
  单次 `SendMessage` / `SendPacket` / `Ping` 超时

### `CursorStore`

`CursorStore` 是 SDK 与业务持久层的接缝：

```go
type CursorStore interface {
	LoadSeenMessages(context.Context) ([]MessageCursor, error)
	SaveMessage(context.Context, Message) error
	SaveCursor(context.Context, MessageCursor) error
}
```

`MessagePushed` 到达后，SDK 会按固定顺序调用：

1. `SaveMessage`
2. `SaveCursor`
3. 发送 `AckMessage`

这和接入文档里的可靠性要求一致。不要把 ack 放到持久化之前。

如果只是本地测试，可以直接用：

```go
store := turntf.NewMemoryCursorStore()
```

### `Handler`

```go
type Handler interface {
	OnLogin(context.Context, LoginInfo)
	OnMessage(context.Context, Message)
	OnPacket(context.Context, Packet)
	OnError(context.Context, error)
	OnDisconnect(context.Context, error)
}
```

也可以先用空实现：

```go
Handler: turntf.NopHandler{},
```

### 集成管理与查询 RPC

`Client` 上的管理、查询、发消息能力现在都通过已登录的 WebSocket 连接完成；`Login` 仍然使用 HTTP：

```go
token, err := client.Login(ctx, 4096, 1, "root")
if err := client.Connect(ctx); err != nil { ... }
user, err := client.CreateUser(ctx, token, turntf.CreateUserRequest{...})
err = client.CreateSubscription(ctx, token, userRef, channelRef)
messages, err := client.ListMessages(ctx, token, target, 20)
message, err := client.PostMessage(ctx, token, target, "ops", payload)
err = client.PostPacket(ctx, token, target.NodeID, target, "relay", payload, turntf.DeliveryModeRouteRetry)
```

如果你想拿到底层 HTTP 客户端本身，也可以：

```go
httpClient := client.HTTP()
```

### 发送持久化消息

```go
msg, err := client.SendMessage(ctx, turntf.SendMessageInput{
	Target: turntf.UserRef{NodeID: 4096, UserID: 1025},
	Sender: "orders",
	Body:   []byte{0xff, 0x00},
})
```

返回值是服务端 `send_message_response.message` 映射成的 `turntf.Message`。

### 发送目标用户瞬时包

```go
accepted, err := client.SendPacket(ctx, turntf.SendPacketInput{
	Target:       turntf.UserRef{NodeID: 8192, UserID: 1025},
	Sender:       "relay",
	Body:         []byte{0xff, 0x00},
	DeliveryMode: turntf.DeliveryModeRouteRetry,
})
```

支持的发送模式：

- `turntf.DeliveryModeBestEffort`
- `turntf.DeliveryModeRouteRetry`

### Ping

```go
err := client.Ping(ctx)
```

## HTTP API

### 登录

```go
token, err := httpClient.Login(ctx, 4096, 1, "root")
```

### 创建用户 / channel

```go
user, err := httpClient.CreateUser(ctx, token, turntf.CreateUserRequest{
	Username: "alice",
	Password: "alice-password",
	Role:     "user",
})

channel, err := httpClient.CreateChannel(ctx, token, turntf.CreateUserRequest{
	Username: "orders",
})
```

### 建立订阅

```go
err := httpClient.CreateSubscription(
	ctx,
	token,
	turntf.UserRef{NodeID: 4096, UserID: 1025},
	turntf.UserRef{NodeID: 4096, UserID: 1026},
)
```

### 查询消息

```go
messages, err := httpClient.ListMessages(
	ctx,
	token,
	turntf.UserRef{NodeID: 4096, UserID: 1025},
	20,
)
```

### HTTP 发消息

```go
message, err := httpClient.PostMessage(
	ctx,
	token,
	turntf.UserRef{NodeID: 4096, UserID: 1025},
	"ops",
	[]byte{0xff, 0x00},
)
```

### HTTP 发瞬时包

```go
err := httpClient.PostPacket(
	ctx,
	token,
	8192,
	turntf.UserRef{NodeID: 8192, UserID: 1025},
	"relay",
	[]byte{0xff, 0x00},
	turntf.DeliveryModeRouteRetry,
)
```

HTTP `body` 在 SDK 外部统一使用 `[]byte`，SDK 内部会自动做 JSON base64 编解码。

## 错误处理

SDK 目前会返回几类主要错误：

- `*turntf.ServerError`
  服务端返回的协议错误，包含 `Code`、`Message`、`RequestID`
- `*turntf.ProtocolError`
  本地发现协议不符合预期，比如响应形状错误
- `*turntf.ConnectionError`
  建连、读写过程中的网络错误

示例：

```go
var serverErr *turntf.ServerError
if errors.As(err, &serverErr) {
	log.Printf("server error: code=%s message=%s request=%d", serverErr.Code, serverErr.Message, serverErr.RequestID)
}
```

`unauthorized` 登录错误会停止自动重连。

## 重新生成 protobuf

仓库里已经带上了生成后的文件。如果你修改了 [proto/client.proto](/root/dev/sys/turntf-go/proto/client.proto)，可以重新生成：

```bash
go generate ./...
```

## 测试

```bash
go test ./...
```

当前测试覆盖了：

- WebSocket 登录成功
- `MessagePushed` 的持久化与 ack 顺序
- `SendMessage` / `Ping` 请求匹配
- `unauthorized` 停止自动重连
- 断线后使用 `seen_messages` 重连
- HTTP base64 编解码与 Bearer token 注入
