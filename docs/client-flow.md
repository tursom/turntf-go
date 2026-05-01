# 客户端全流程接入文档

本文档面向 `turntf-go` 使用者，描述从准备账号到稳定收发消息的完整流程。它以 Go SDK 接入为主线，重点解释业务侧真正要落地的状态和顺序；更底层的 envelope 字段见 [客户端 WebSocket 接口](client-websocket.md)，SDK 结构说明见 [Go SDK 使用总览](sdk-guide.md)。

## 1. 明确你的接入形态

接入前先判断你是哪一种客户端：

- 普通实时客户端：长期在线，既收持久化消息，也收瞬时包
- 只关心瞬时流量的在线客户端：只需要 `PacketPushed`
- 管理端或脚本：只用 HTTP 登录、建用户、发测试消息
- 管理端长连接：需要通过已登录 WebSocket 做运维查询、解析在线 session 或定向 packet

对应建议：

| 场景 | 推荐入口 |
| --- | --- |
| 实时收消息 | `Client` |
| 只做后台脚本 | `HTTPClient` |
| 既要管理能力又要实时会话状态 | `Client`，必要时配合 `Client.Login()` |

## 2. 服务端准备

### 2.1 获取管理员 token

如果你要创建用户、channel 或准备测试数据，先拿管理员 token：

```go
httpClient := turntf.NewHTTPClient("http://127.0.0.1:8080")
token, err := httpClient.Login(ctx, 4096, 1, "root")
// 也可以使用 httpClient.LoginByLoginName(ctx, "alice.login", "alice-password")
```

### 2.2 创建普通用户

```go
alice, err := httpClient.CreateUser(ctx, token, turntf.CreateUserRequest{
	Username:  "alice",
	LoginName: "alice.login",
	Password:  turntf.MustPlainPassword("alice-password"),
	Role:      "user",
})
```

响应中的 `(node_id, user_id)` 和可选的 `login_name` 都可以作为后续 WebSocket 登录前的身份选择器。

### 2.3 创建 channel

```go
orders, err := httpClient.CreateChannel(ctx, token, turntf.CreateUserRequest{
	Username: "orders",
})
```

`role=channel` 用户本身不能登录，只用于接收订阅消息。

### 2.4 建立订阅

```go
err = httpClient.CreateSubscription(
	ctx,
	token,
	turntf.UserRef{NodeID: alice.NodeID, UserID: alice.UserID},
	turntf.UserRef{NodeID: orders.NodeID, UserID: orders.UserID},
)
```

补充说明：

- `HTTPClient.CreateSubscription()` 走的是 SDK 内部 attachment 路径封装
- `Client.SubscribeChannel()` 走的是已登录 WebSocket attachment RPC
- 两者语义一致，都是“把用户订阅到 channel”

## 3. 初始化本地状态

业务侧至少要持久化两类数据：

- 消息本身
- 已安全持久化的游标 `(node_id, seq)`

`CursorStore` 接口：

```go
type CursorStore interface {
	LoadSeenMessages(context.Context) ([]MessageCursor, error)
	SaveMessage(context.Context, Message) error
	SaveCursor(context.Context, MessageCursor) error
}
```

推荐本地唯一键：

```text
messages primary key: (node_id, seq)
```

如果你只做本地演示，可先用内存实现：

```go
store := turntf.NewMemoryCursorStore()
```

## 4. 创建 `Client`

最常用配置示例：

```go
client, err := turntf.NewClient(turntf.Config{
	BaseURL: "http://127.0.0.1:8080",
	Credentials: turntf.Credentials{
		NodeID:   alice.NodeID,
		UserID:   alice.UserID,
		Password: turntf.MustPlainPassword("alice-password"),
		// 或者改为 LoginName: "alice.login",
	},
	CursorStore:           store,
	Handler:               myHandler{},
	InitialReconnectDelay: time.Second,
	MaxReconnectDelay:     30 * time.Second,
	PingInterval:          30 * time.Second,
	RequestTimeout:        10 * time.Second,
})
```

几个最关键的配置：

- `BaseURL`：传 `http://` 或 `https://` 即可，SDK 会自动切成 `ws://` 或 `wss://`
- `Credentials`：用于 WebSocket 首帧登录，二选一提供 `(node_id, user_id)` 或 `login_name`
- `CursorStore`：决定 `seen_messages` 和本地恢复能力
- `Handler`：接收登录、消息、packet、错误、断线回调

## 5. 首次连接与登录

### 5.1 业务侧调用

```go
if err := client.Connect(ctx); err != nil {
	log.Fatal(err)
}
```

### 5.2 SDK 内部做了什么

`turntf-go` 会自动完成下面这条流程：

1. 调 `LoadSeenMessages()`
2. 拨号 `/ws/client` 或 `/ws/realtime`
3. 发送首帧 `ClientEnvelope.login`
4. 等待 `login_response`
5. 保存当前 `session_ref`
6. 触发 `OnLogin()`

对应的首帧原始 proto 形状是：

```protobuf
ClientEnvelope {
  login: LoginRequest {
    user: { node_id: 4096, user_id: 1025 }
    password: "$2a$10$..."
    seen_messages: []
  }
}
```

注意这里的登录身份在 `user` 字段里，而不是旧文档中的顶层 `node_id` / `user_id`。

### 5.3 登录成功后拿到什么

```go
info, ok := client.CurrentLogin()
```

你会得到：

- `info.User`
- `info.ProtocolVersion`
- `info.SessionRef`

`session_ref` 标识当前这条在线连接，后续做定向瞬时包时会用到。

## 6. 接收持久化消息

登录成功后，如果不是 transient-only，会先收到历史补发，然后收到实时推送。

高层 SDK 在收到 `MessagePushed` 后固定执行：

1. `SaveMessage`
2. `SaveCursor`
3. `AckMessage`
4. `OnMessage`

这条顺序不要改成：

- 先 ack 再落库
- 先回调业务再写游标

因为断线恢复真正依赖的是“本地已经保存了哪些游标”，而不是服务端是否见过你的 ack。

业务侧建议：

- 让 `SaveMessage` 和 `SaveCursor` 都具备幂等性
- 监控 `OnError()`，因为本地持久化失败会从这里暴露
- 把 `(node_id, seq)` 作为客户端消息唯一标识

## 7. 发送持久化消息

```go
msg, err := client.SendMessage(ctx, turntf.SendMessageInput{
	Target: turntf.UserRef{NodeID: 4096, UserID: 1025},
	Body:   []byte("hello"),
})
```

成功后：

- 服务端返回 `send_message_response.message`
- SDK 会再次执行 `SaveMessage -> SaveCursor`
- 返回值 `msg` 就是这条服务端确认写入后的消息

这意味着“自己发出去的持久化消息”和“别人推给自己的持久化消息”可以共用同一套本地幂等逻辑。

## 8. `session_ref` 与定向瞬时包

### 8.1 为什么需要 `session_ref`

同一个用户可以同时在线多条连接。若你只想把瞬时包发给其中某一条，需要：

1. 先知道目标用户当前有哪些在线 session
2. 再指定某一个 `session_ref`

### 8.2 查询在线 session

```go
resolved, err := client.ResolveUserSessions(ctx, target)
```

返回值里有两组信息：

- `Presence`：按节点聚合的在线态
- `Sessions`：每条可投递 session 的明细，可直接拿去定向发送

### 8.3 定向发送

```go
accepted, err := client.SendPacketToSession(
	ctx,
	target,
	resolved.Sessions[0].Session,
	[]byte("ephemeral"),
	turntf.DeliveryModeRouteRetry,
)
```

要点：

- `accepted` 代表路由层已受理
- 不代表目标连接一定已经收到
- 瞬时包不会落库、不会补发、不会参与 `seen_messages`

## 9. `TransientOnly` 与 `/ws/realtime`

### 9.1 只收瞬时流量

如果你只关心瞬时包，不想接持久化消息，可以在 `Config` 中开启：

```go
TransientOnly: true,
```

效果：

- 登录成功后不会做持久化历史补发
- 不会继续接收 `MessagePushed`
- 仍可接收 `PacketPushed`

### 9.2 专用实时流入口

如果你还希望服务端严格限制这条连接只做实时在线态 / transient 流量，可再开启：

```go
RealtimeStream: true,
```

这会把连接路径切到 `/ws/realtime`，并额外限制：

- 只允许 transient `send_message`
- 不允许 `list_messages`
- 不允许 attachment RPC
- 不允许管理员运维 RPC

## 10. 断线重连

断线后，SDK 会按指数退避尝试重连，并在每次重连前重新调用：

```go
LoadSeenMessages()
```

业务侧要保证：

1. `SaveCursor()` 只有在消息已经安全落库后才写入
2. `LoadSeenMessages()` 能读出全部已持久化游标
3. 游标可以覆盖多个生产节点

重连首帧示例：

```protobuf
ClientEnvelope {
  login: LoginRequest {
    user: { node_id: 4096, user_id: 1025 }
    password: "$2a$10$..."
    seen_messages: [
      { node_id: 4096, seq: 1 },
      { node_id: 4096, seq: 2 },
      { node_id: 4097, seq: 8 }
    ]
  }
}
```

`seen_messages` 可以包含多个节点的游标；服务端会据此跳过这些已经持久化的消息。

## 11. HTTP 与 WebSocket 的职责分工

推荐把两条能力线按下面方式组合：

| 任务 | 推荐入口 |
| --- | --- |
| 获取管理员 token | `Client.Login()` 或 `HTTPClient.Login()` |
| 创建用户 / channel | `HTTPClient.CreateUser()` 或 `Client.CreateUser()` |
| 建立普通订阅 | `HTTPClient.CreateSubscription()` 或 `Client.SubscribeChannel()` |
| 拉取历史消息 | `HTTPClient.ListMessages()` 或 `Client.WSListMessages()` |
| 实时收消息 | `Client` |
| 查询在线 session | `Client.ResolveUserSessions()` |
| 发定向瞬时包 | `Client.SendPacketToSession()` |

补充说明：

- `Client` 上部分带 `token` 的方法目前为了兼容旧签名仍然保留参数，但实际走的是已登录 WebSocket RPC。
- `HTTPClient` 当前不负责在线 session 解析，也不负责任何实时推送。

## 12. 错误处理建议

### 12.1 请求调用

对同步调用，优先按错误类型判断：

```go
var serverErr *turntf.ServerError
switch {
case errors.As(err, &serverErr):
	log.Printf("server error: code=%s request=%d", serverErr.Code, serverErr.RequestID)
case errors.Is(err, turntf.ErrDisconnected):
	log.Printf("websocket disconnected")
case errors.Is(err, turntf.ErrClosed):
	log.Printf("client already closed")
default:
	log.Printf("request failed: %v", err)
}
```

### 12.2 被动回调

`Handler` 里的两个回调要重点监控：

- `OnError()`：协议错误、持久化错误、重连中的读写错误都会从这里暴露
- `OnDisconnect()`：每次连接断开都会触发

登录阶段若收到 `unauthorized`，当前实现会停止自动重连。

## 13. 常见场景清单

### 13.1 普通用户实时收消息

1. 管理员建用户
2. 初始化 `CursorStore`
3. `Connect()`
4. 在 `OnMessage()` 里消费业务消息

### 13.2 管理员查询在线连接并发瞬时包

1. 管理员用户 `Connect()`
2. `ResolveUserSessions()`
3. 选择一个 `SessionRef`
4. `SendPacketToSession()`

### 13.3 只做初始化脚本

1. `HTTPClient.Login()`
2. `CreateUser()` / `CreateChannel()`
3. `CreateSubscription()`
4. `PostMessage()`

## 14. 验收清单

- 能通过 `Connect()` 完成 WebSocket 首帧登录
- `OnLogin()` 能拿到 `session_ref`
- 能接收历史补发消息
- 能接收实时 `MessagePushed`
- 能接收实时 `PacketPushed`
- 本地确实按 `SaveMessage -> SaveCursor -> AckMessage` 处理持久化消息
- 重连后能带上 `seen_messages`，且不会重复展示已落库消息
- 能通过 `ResolveUserSessions()` + `SendPacketToSession()` 做会话定向 packet
- 能区分 `ServerError`、`ConnectionError`、`ErrDisconnected`
- 修改 proto 后知道去 `go generate ./...` 重新生成并同步检查文档
