# Go SDK 使用总览

本文档面向 `turntf-go` 使用者，重点说明高层 SDK 的职责边界，而不是逐字段复述 proto。协议层细节见 [客户端 WebSocket 接口](client-websocket.md)，端到端流程见 [客户端全流程接入文档](client-flow.md)。

## 1. SDK 定位

`turntf-go` 同时提供两条能力线：

- `Client`：面向实时业务。底层使用 WebSocket + Protobuf，负责连接、登录、自动重连、请求 ID 匹配、消息游标恢复和事件回调。
- `HTTPClient`：面向脚本和后台。底层使用 HTTP JSON，请求简单直接，但不负责任何实时状态或断线恢复。

建议按下面的方式理解它们的关系：

| 需求 | 推荐入口 | 说明 |
| --- | --- | --- |
| 登录后持续收消息 | `Client` | `Client` 才会处理 `MessagePushed` / `PacketPushed` |
| 需要 `seen_messages` 和本地游标恢复 | `Client` | `HTTPClient` 不参与重放恢复 |
| 只做创建用户、简单发消息 | `HTTPClient` | 不需要维护连接 |
| 需要查询在线 session 并发定向瞬时包 | `Client` | 依赖 `ResolveUserSessions`、`session_ref` |

## 2. 两条能力线怎么配合

典型接入方式有两种：

### 2.1 纯 WebSocket 业务客户端

适合普通用户端或常驻进程：

1. 用 `Config.Credentials` 指定登录身份。
2. 调用 `client.Connect(ctx)` 建立长连接。
3. 通过 `OnLogin`、`OnMessage`、`OnPacket` 接收回调。
4. 通过 `SendMessage`、`SendPacket`、`ResolveUserSessions` 等方法发起 RPC。

这种模式下，`Client.Connect()` 本身不需要 HTTP token。

### 2.2 HTTP 初始化 + WebSocket 长连

适合管理员工具、测试脚本或需要先建用户再建连的场景：

1. 调 `client.Login()` 或 `client.HTTP().Login()` 获取 Bearer token。
2. 用 `CreateUser`、`CreateChannel` 等管理方法准备测试数据。
3. 再调用 `Connect()` 建立实时连接。

注意：

- `Client.Login()` 本质上只是 `HTTP /auth/login` 的便捷封装。
- `Client` 上部分方法保留了 `token string` 参数，但当前实现实际走已登录 WebSocket RPC，`token` 参数不会参与鉴权。

## 3. `Config` 配置项

`NewClient` 的核心配置如下：

| 字段 | 作用 | 默认行为 |
| --- | --- | --- |
| `BaseURL` | 传 `http://host:port` 或 `https://host:port`，SDK 自动拼接 WebSocket 路径 | 必填 |
| `Credentials` | WebSocket 首帧登录身份，包含 `NodeID`、`UserID`、`Password` | 必填 |
| `CursorStore` | 本地消息和游标持久层接缝 | 默认为 `MemoryCursorStore` |
| `Handler` | 生命周期和推送回调 | 默认为 `NopHandler` |
| `HTTPClient` | 自定义底层 `*http.Client`，同时用于 HTTP 与 WebSocket Dial | 默认 `http.DefaultClient` |
| `Logger` | 记录自动重连日志 | 可选 |
| `InitialReconnectDelay` | 首次重连退避 | 默认 `1s` |
| `MaxReconnectDelay` | 最大重连退避 | 默认 `30s` |
| `PingInterval` | 应用层 Ping 周期 | 默认 `30s` |
| `RequestTimeout` | `Ping` 与各类 RPC 的超时 | 默认 `10s` |
| `TransientOnly` | 登录时携带 `transient_only=true`，不接持久化补发和实时持久化推送 | 默认 `false` |
| `RealtimeStream` | 连接 `/ws/realtime` 而不是 `/ws/client` | 默认 `false` |

布尔配置的当前实现约束：

- `Reconnect` 默认开启，而且当前版本不能通过传 `false` 显式关闭。
- `AckMessages` 默认开启，而且当前版本也不能通过传 `false` 显式关闭。

如果你需要“关闭自动重连”或“关闭自动 ack”的能力，当前版本需要在 SDK 外层自行封装或继续修改实现。

## 4. 密码输入约束

`Credentials.Password` 和各类创建 / 更新用户接口都使用 `PasswordInput`：

- `turntf.MustPlainPassword("alice-password")`
  适合直接传明文，SDK 会先做 bcrypt 哈希，再把哈希结果写到线上请求。
- `turntf.HashedPassword("$2a$...")`
  适合你已经持有 bcrypt 哈希串的场景。

注意：

- `Login` 和 `CreateUser` 都不会把原始明文直接发到服务端。
- `PasswordInput.UnmarshalJSON()` 会把 JSON 字符串视为“已经是哈希后的密码”。

## 5. 连接 / 登录生命周期

### 5.1 建连前

`NewClient()` 只做本地参数校验，不会发起网络连接。

业务侧至少要准备：

- 一个可复用的 `context.Context`
- 登录身份 `Credentials`
- 一个能持久化消息游标的 `CursorStore`

### 5.2 `Connect()`

`Connect()` 的流程是：

1. 从 `CursorStore.LoadSeenMessages()` 读取本地已持久化游标。
2. 连接 `/ws/client` 或 `/ws/realtime`。
3. 发送 `ClientEnvelope.login`。
4. 等待第一条服务端消息，必须是 `login_response` 或 `error`。
5. 登录成功后设置内部连接状态，触发 `Handler.OnLogin()`。
6. 启动读循环和应用层 Ping 循环。

`Connect(ctx)` 只等待“第一次连接结果”：

- 首次登录成功后立即返回 `nil`
- 首次登录失败则返回错误
- 后续断线和重连通过 `Handler.OnDisconnect()` / `Handler.OnError()` 感知

### 5.3 `OnLogin`

每次认证成功都会回调：

```go
type Handler interface {
	OnLogin(context.Context, LoginInfo)
	OnMessage(context.Context, Message)
	OnPacket(context.Context, Packet)
	OnError(context.Context, error)
	OnDisconnect(context.Context, error)
}
```

`LoginInfo` 中最重要的字段：

- `User`：当前登录用户
- `ProtocolVersion`：当前客户端协议版本
- `SessionRef`：当前这一次连接在服务端注册出来的在线 session

### 5.4 `CurrentLogin`

`CurrentLogin()` 是无阻塞只读查询：

- 已认证时返回 `(LoginInfo, true)`
- 尚未登录成功或已经断开时返回 `(_, false)`

### 5.5 断线与关闭

读循环退出后，SDK 会：

1. 清空当前连接状态
2. 让所有待响应 RPC 返回 `ErrDisconnected`
3. 回调 `Handler.OnDisconnect()`
4. 根据错误类型决定是否进入自动重连

`Close()` 会主动取消内部 context 并关闭连接，之后不会再继续重连。

## 6. `CursorStore` 与持久化顺序

`CursorStore` 是最关键的接缝：

```go
type CursorStore interface {
	LoadSeenMessages(context.Context) ([]MessageCursor, error)
	SaveMessage(context.Context, Message) error
	SaveCursor(context.Context, MessageCursor) error
}
```

### 6.1 `MessagePushed` 的固定顺序

收到 `ServerEnvelope.message_pushed` 后，SDK 固定执行：

1. `SaveMessage`
2. `SaveCursor`
3. 发送 `AckMessage`
4. 回调 `Handler.OnMessage`

这条顺序非常重要：

- 不要在你的业务文档或上层封装里把 ack 提前到落库之前。
- 服务端的 `AckMessage` 只影响当前连接内的去重集合，不是可靠持久化确认。
- 真正影响重连恢复的是下次登录时 `LoadSeenMessages()` 返回了哪些游标。

### 6.2 发送成功后的持久化

收到 `SendMessageResponse.message` 时，SDK 也会执行：

1. `SaveMessage`
2. `SaveCursor`

这里不会再额外发 ack，因为服务端当前已经把该连接视作“已见”。

### 6.3 本地存储建议

推荐至少维护两张表：

- `messages`：保存完整消息体
- `message_cursors`：保存 `(node_id, seq)`

推荐唯一键：

```text
messages primary key: (node_id, seq)
```

这样无论消息来自历史补发、实时推送，还是 `send_message_response.message`，都能用同一套幂等逻辑处理。

## 7. 自动重连、`seen_messages` 和 `session_ref`

### 7.1 `seen_messages`

`seen_messages` 是客户端向服务端声明“这些持久化消息我已经安全落库”的集合。

它有几个关键特点：

- 由 `CursorStore.LoadSeenMessages()` 提供
- 以 `(node_id, seq)` 为单位
- 可以同时包含多个消息生产节点的游标
- 重连时会重新读取，而不是使用内存快照

### 7.2 自动重连

当前实现使用指数退避重连：

- 起点由 `InitialReconnectDelay` 控制
- 上限由 `MaxReconnectDelay` 控制
- 登录阶段收到 `unauthorized` 时会停止自动重连

重连时 SDK 会再次执行完整登录流程，并重新上报 `seen_messages`。

### 7.3 `session_ref`

`session_ref` 是服务端分配给“当前这一次在线连接”的标识：

- 在 `LoginResponse.session_ref` 返回
- 断线重连后通常会变化
- 可用于告诉其他客户端“把瞬时包只打到我这个连接”

它的典型用途：

1. 目标用户登录后，通过 `OnLogin` 或 `CurrentLogin` 记录自己的 `session_ref`
2. 发送方调用 `ResolveUserSessions()` 拿到目标用户在线 session 列表
3. 发送方调用 `SendPacketToSession()`，只投递给目标连接

## 8. `TransientOnly` 与 `RealtimeStream`

这两个开关很容易混淆，但语义不同。

### 8.1 `TransientOnly`

作用：

- 登录时把 `LoginRequest.transient_only` 设为 `true`
- 跳过历史持久化消息补发
- 不再接收实时持久化消息推送
- 仍然可以接收 `PacketPushed`
- 仍然可以继续使用普通 `/ws/client` 上的大多数 RPC

适合：

- 只关心瞬时流量，不需要消息历史和持久化消息回放

### 8.2 `RealtimeStream`

作用：

- 把连接路径切到 `/ws/realtime`
- 服务端会按“realtime-only”规则处理这条连接
- 这条连接天然也是 transient-only

`/ws/realtime` 允许的能力主要是：

- `send_message` 的 transient 形式
- `list_cluster_nodes`
- `list_node_logged_in_users`
- `resolve_user_sessions`
- `ping`
- 接收 `PacketPushed`

`/ws/realtime` 会拒绝的能力包括：

- 持久化 `send_message`
- `create_user` / `get_user` / `update_user` / `delete_user`
- `list_messages`
- `upsert_user_attachment` / `delete_user_attachment` / `list_user_attachments`
- `list_events`
- `operations_status`
- `metrics`

## 9. 常用 API 说明

### 9.1 持久化消息

```go
msg, err := client.SendMessage(ctx, turntf.SendMessageInput{
	Target: turntf.UserRef{NodeID: 4096, UserID: 1025},
	Body:   []byte("hello"),
})
```

说明：

- 返回的是服务端确认写入后的 `Message`
- 该消息同样会进入 `CursorStore`
- 当前高层 `SendMessageInput` 不暴露 proto 里的 `sync_mode`，因此会让服务端使用默认同步策略

### 9.2 定向瞬时包

```go
resolved, err := client.ResolveUserSessions(ctx, target)
accepted, err := client.SendPacketToSession(
	ctx,
	target,
	resolved.Sessions[0].Session,
	[]byte("ephemeral"),
	turntf.DeliveryModeRouteRetry,
)
```

说明：

- `ResolveUserSessions()` 同时返回按节点聚合的 `Presence` 和可直接投递的 `Sessions`
- `SendPacketToSession()` 只是 `SendPacket()` 的便捷封装
- `accepted.TargetSession` 表示服务端接受了“只投给某个 session”的路由请求
- 这不等于目标连接一定已经收到 `PacketPushed`

### 9.3 订阅与黑名单

高层 `Client` 提供了语义化方法：

- `SubscribeChannel`
- `UnsubscribeChannel`
- `ListSubscriptions`
- `BlockUser`
- `UnblockUser`
- `ListBlockedUsers`

但底层 WebSocket proto 实际走的是 attachment RPC：

- `upsert_user_attachment`
- `delete_user_attachment`
- `list_user_attachments`

也就是说：

- 文档里如果要写 proto，请按 attachment 名称写
- 文档里如果写 Go API，可继续使用 `SubscribeChannel` / `BlockUser` 这些高层名字

### 9.4 运维查询

`Client` 比 `HTTPClient` 更完整，支持：

- `ListEvents`
- `OperationsStatus`
- `Metrics`
- `ListClusterNodes`
- `ListNodeLoggedInUsers`

`HTTPClient` 当前只覆盖：

- `ListClusterNodes`
- `ListNodeLoggedInUsers`

## 10. 错误处理

常见错误分成三层。

### 10.1 请求级错误

服务端返回 `ServerEnvelope.error` 且带 `request_id` 时，调用中的 RPC 会直接返回：

- `*turntf.ServerError`

常见场景：

- `forbidden`
- `not_found`
- `invalid_request`

### 10.2 连接 / 传输错误

建连、读写失败时，常见返回：

- `*turntf.ConnectionError`
- `turntf.ErrNotConnected`
- `turntf.ErrDisconnected`
- `turntf.ErrClosed`

这些错误既可能直接从方法返回，也可能通过 `Handler.OnError()` / `Handler.OnDisconnect()` 被动收到。

### 10.3 协议或本地持久化错误

包括：

- 服务端回了不符合预期的 envelope，返回 `*turntf.ProtocolError`
- `CursorStore.SaveMessage()` / `SaveCursor()` 返回错误

实现细节要点：

- `persistMessage()` 失败时，SDK 会回调 `Handler.OnError()`，不会继续发送 ack
- 当前连接不会因为一次本地落库失败而立刻主动关闭
- 因此业务侧应把 `OnError()` 纳入监控，并对本地存储故障做告警

### 10.4 示例

```go
var serverErr *turntf.ServerError
switch {
case errors.As(err, &serverErr):
	log.Printf("server error: code=%s message=%s request=%d", serverErr.Code, serverErr.Message, serverErr.RequestID)
case errors.Is(err, turntf.ErrDisconnected):
	log.Printf("request failed because websocket disconnected")
default:
	log.Printf("request failed: %v", err)
}
```

## 11. `HTTPClient` 的额外说明

`HTTPClient` 的特点：

- `body []byte` 会自动转成 JSON base64
- `PasswordInput` 会自动传输哈希值
- 不负责任何本地游标或重连恢复

当前已覆盖的方法：

- `Login`
- `CreateUser` / `CreateChannel`
- `CreateSubscription`
- `ListMessages`
- `PostMessage`
- `PostPacket`
- `ListClusterNodes`
- `ListNodeLoggedInUsers`
- `BlockUser` / `UnblockUser` / `ListBlockedUsers`
- `UpsertAttachment` / `DeleteAttachment` / `ListAttachments`

## 12. Proto 与生成约束

Go SDK 的 proto 约束如下：

- 源文件是 `turntf-go/proto/client.proto`
- 生成结果是 `turntf-go/internal/proto/client.pb.go`
- 生成入口在 `turntf-go/generate.go` 和 `turntf-go/scripts/gen-proto.sh`
- 不要手改 `internal/proto/client.pb.go`

重新生成：

```bash
go generate ./...
```

或：

```bash
./scripts/gen-proto.sh
```

生成依赖：

- `protoc`
- `protoc-gen-go`

如果你修改了 `client.proto`，建议同时检查这些文档是否仍然准确：

- `README.md`
- `docs/sdk-guide.md`
- `docs/client-flow.md`
- `docs/client-websocket.md`

## 13. 建议阅读顺序

第一次接入建议按下面顺序阅读：

1. 本文档
2. [客户端全流程接入文档](client-flow.md)
3. [客户端 WebSocket 接口](client-websocket.md)
4. `client.go` / `client_test.go` 中和你场景最接近的实现与测试
