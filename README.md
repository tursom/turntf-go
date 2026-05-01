# turntf-go

`turntf-go` 是 turntf 的 Go SDK，面向两条客户端能力线：

- `Client`：基于 WebSocket + Protobuf 的长连接客户端，负责登录、请求响应匹配、自动重连、消息持久化回调、`session_ref` 和会话定向瞬时包。
- `HTTPClient`：基于 HTTP JSON 的轻量客户端，适合脚本、后台、初始化工具和调试。

SDK 的目标不是简单映射 REST 或 protobuf 字段，而是把业务接入时最容易出错的部分收进统一实现里，例如：

- WebSocket 首帧登录和 `seen_messages` 上报
- `MessagePushed` 的 `SaveMessage -> SaveCursor -> AckMessage` 顺序
- 请求 ID 管理和 RPC 响应匹配
- `body` 的 `[]byte` / JSON base64 转换
- `session_ref`、`resolve_user_sessions` 和定向瞬时包

## 文档导航

- [Go SDK 使用总览](docs/sdk-guide.md)：推荐先读。覆盖定位、能力选型、配置项、生命周期、自动重连、错误处理、proto 生成约束。
- [客户端全流程接入文档](docs/client-flow.md)：从创建用户、初始化本地游标到稳定收发消息的端到端流程。
- [客户端 WebSocket 接口](docs/client-websocket.md)：`ClientEnvelope` / `ServerEnvelope` 级别的协议语义、`/ws/client` 与 `/ws/realtime` 边界。
- [API 参考文档](docs/api-reference.md)：类型、方法、错误的完整参考。
- [HTTP 客户端使用指南](docs/http-client-guide.md)：`HTTPClient` 的详细用法和注意事项。
- [运维与上线手册](docs/operations.md)：上线、恢复、监控相关说明。
- [Demo YAML 示例](docs/examples/)：多节点、多 session 的收发验证脚本。

## 安装

```bash
go get github.com/tursom/turntf-go
```

## 选型建议

优先使用 `Client` 的场景：

- 需要实时收消息或瞬时包
- 需要自动重连、登录生命周期回调和本地游标管理
- 需要通过同一条已登录连接执行管理 / 查询 RPC

优先使用 `HTTPClient` 的场景：

- 只做登录、建用户、简单发消息或后台脚本
- 不需要本地 `CursorStore`
- 不需要长连接、实时消息或定向 packet

补充说明：

- `Client.Connect()` 只依赖 `Config.Credentials` 做 WebSocket 首帧登录，不需要 HTTP token。
- `Client.Login()` 只是复用内置 `HTTPClient` 调用 `/auth/login`，便于你在同一个对象上顺手拿管理员 Bearer token。
- WebSocket 和 HTTP 登录都同时支持两种选择器：旧的 `node_id + user_id + password`，以及新的 `login_name + password`。
- `username` 仍然只是用户资料字段，不参与认证；认证前的解析只看 `node_id/user_id` 或 `login_name`。
- `Client` 上保留了部分带 `token string` 参数的方法名以兼容旧调用方式，但这些方法当前实际走已登录 WebSocket RPC，`token` 参数不会参与鉴权。

## 快速开始

```go
package main

import (
	"context"
	"log"
	"time"

	turntf "github.com/tursom/turntf-go"
)

type handler struct{}

func (handler) OnLogin(_ context.Context, info turntf.LoginInfo) {
	log.Printf(
		"login ok: user=%d:%d session=%d/%s protocol=%s",
		info.User.NodeID,
		info.User.UserID,
		info.SessionRef.ServingNodeID,
		info.SessionRef.SessionID,
		info.ProtocolVersion,
	)
}

func (handler) OnMessage(_ context.Context, msg turntf.Message) {
	log.Printf("message: cursor=%d/%d from=%d:%d", msg.NodeID, msg.Seq, msg.Sender.NodeID, msg.Sender.UserID)
}

func (handler) OnPacket(_ context.Context, packet turntf.Packet) {
	log.Printf("packet: id=%d target_session=%d/%s", packet.PacketID, packet.TargetSession.ServingNodeID, packet.TargetSession.SessionID)
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
			Password: turntf.MustPlainPassword("alice-password"),
			// 或者使用 LoginName: "alice.login",
		},
		CursorStore:           turntf.NewMemoryCursorStore(),
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

	if err := client.Connect(ctx); err != nil {
		log.Fatal(err)
	}

	msg, err := client.SendMessage(ctx, turntf.SendMessageInput{
		Target: turntf.UserRef{NodeID: 4096, UserID: 1025},
		Body:   []byte("hello"),
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("sent persistent message: cursor=%d/%d", msg.NodeID, msg.Seq)

	sessions, err := client.ResolveUserSessions(ctx, turntf.UserRef{NodeID: 4096, UserID: 1025})
	if err != nil {
		log.Fatal(err)
	}
	if len(sessions.Sessions) > 0 {
		accepted, err := client.SendPacketToSession(
			ctx,
			sessions.User,
			sessions.Sessions[0].Session,
			[]byte("ephemeral"),
			turntf.DeliveryModeRouteRetry,
		)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("transient accepted: packet=%d target=%d/%s", accepted.PacketID, accepted.TargetSession.ServingNodeID, accepted.TargetSession.SessionID)
	}
}
```

只想单独使用 HTTP 时：

```go
httpClient := turntf.NewHTTPClient("http://127.0.0.1:8080")
token, err := httpClient.Login(ctx, 4096, 1, "root")
// 也可以使用 httpClient.LoginByLoginName(ctx, "alice.login", "alice-password")
user, err := httpClient.CreateUser(ctx, token, turntf.CreateUserRequest{
	Username:  "alice",
	LoginName: "alice.login",
	Password:  turntf.MustPlainPassword("alice-password"),
	Role:      "user",
})
message, err := httpClient.PostMessage(ctx, token, turntf.UserRef{
	NodeID: user.NodeID,
	UserID: user.UserID,
}, []byte("hello"))
```

## 核心语义

- `session_ref`：来自 `LoginResponse`，标识当前这次登录对应的在线连接。`CurrentLogin()`、`OnLogin()` 和 `ResolveUserSessions()` 都会暴露它。
- `seen_messages`：每次建连前，SDK 都会从 `CursorStore.LoadSeenMessages()` 读取已持久化游标，并在首帧登录时一并上报。
- 持久化顺序：SDK 固定按 `SaveMessage -> SaveCursor -> AckMessage` 处理 `MessagePushed`，不要把 ack 提前到本地落库之前。
- `AckMessage`：只用于当前连接内的去重提示。真正的重连恢复依赖 `seen_messages`，而不是依赖服务端记住你上一次的 ack。
- `SendMessageResponse.message`：高层 SDK 也会执行 `SaveMessage -> SaveCursor`，这样发送成功的持久化消息能和推送消息共用一套本地幂等逻辑。
- 瞬时包：`SendPacket` / `SendPacketToSession` 只返回“路由层已受理”，不代表目标用户一定已经收到。

## 常用 API

长连接 `Client`：

- 生命周期：`Connect`、`Close`、`CurrentLogin`、`Ping`
- 持久化消息：`SendMessage`、`WSListMessages`
- 瞬时包：`SendPacket`、`SendPacketToSession`
- 用户管理：`CreateUser`、`CreateChannel`、`GetUser`、`UpdateUser`、`DeleteUser`
- 关系管理：`SubscribeChannel`、`UnsubscribeChannel`、`ListSubscriptions`
- 黑名单：`BlockUser`、`UnblockUser`、`ListBlockedUsers`
- 运维与集群：`ListClusterNodes`、`ListNodeLoggedInUsers`、`ResolveUserSessions`、`ListEvents`、`OperationsStatus`、`Metrics`

HTTP `HTTPClient`：

- 登录：`Login`、`LoginWithPassword`、`LoginByLoginName`、`LoginByLoginNameWithPassword`
- 用户：`CreateUser`、`CreateChannel`
- 消息：`ListMessages`、`PostMessage`、`PostPacket`
- 集群：`ListClusterNodes`、`ListNodeLoggedInUsers`
- 关系：`CreateSubscription`、`BlockUser`、`UnblockUser`、`ListBlockedUsers`
- 通用 attachment：`UpsertAttachment`、`DeleteAttachment`、`ListAttachments`

## Demo Runner

仓库内置了一个只走 WebSocket 的 YAML demo 运行器：

```bash
go run ./cmd/turntf-demo -f docs/examples/demo-cross-node.yaml
```

示例文件：

- [docs/examples/demo-cross-node.yaml](docs/examples/demo-cross-node.yaml)
- [docs/examples/demo-admin-blacklist.yaml](docs/examples/demo-admin-blacklist.yaml)

## Proto 生成

- 源文件：`proto/client.proto`
- 生成文件：`internal/proto/client.pb.go`
- 不要手改生成代码

重新生成：

```bash
go generate ./...
```

或：

```bash
./scripts/gen-proto.sh
```

## 测试

```bash
go test ./...
```
