# API 参考文档

本文档是 `turntf-go` 的 API 参考，覆盖核心类型、方法签名和语义说明。SDK 使用总览见 [sdk-guide.md](sdk-guide.md)，端到端流程见 [client-flow.md](client-flow.md)。

## 包路径

```
import turntf "github.com/tursom/turntf-go"
```

所有公开类型和方法都在根包 `turntf` 中。

---

## 类型索引

- [配置类型](#配置类型)
  - [`Config`](#config)
  - [`Credentials`](#credentials)
- [核心引用类型](#核心引用类型)
  - [`UserRef`](#userref)
  - [`SessionRef`](#sessionref)
  - [`MessageCursor`](#messagecursor)
- [消息与瞬时包](#消息与瞬时包)
  - [`Message`](#message)
  - [`Packet`](#packet)
  - [`RelayAccepted`](#relayaccepted)
  - [`SendMessageInput`](#sendmessageinput)
  - [`SendPacketInput`](#sendpacketinput)
- [用户与关系类型](#用户与关系类型)
  - [`User`](#user)
  - [`Subscription`](#subscription)
  - [`BlacklistEntry`](#blacklistentry)
  - [`Attachment`](#attachment)
  - [`AttachmentType`](#attachmenttype)
  - [`CreateUserRequest`](#createuserrequest)
  - [`UpdateUserRequest`](#updateuserrequest)
- [用户元数据](#用户元数据)
  - [`UserMetadata`](#usermetadata)
  - [`UpsertUserMetadataRequest`](#upsertusermetadatarequest)
  - [`ScanUserMetadataRequest`](#scanusermetadatarequest)
  - [`UserMetadataPage`](#usermetadatapage)
- [运维类型](#运维类型)
  - [`ResolvedUserSessions`](#resolvedusersessions)
  - [`ResolvedSession`](#resolvedsession)
  - [`ClusterNode`](#clusternode)
  - [`LoggedInUser`](#loggedinuser)
  - [`Event`](#event)
  - [`OperationsStatus`](#operationsstatus)
- [回调接口](#回调接口)
  - [`Handler`](#handler)
  - [`NopHandler`](#nophandler)
  - [`Logger`](#logger)
- [存储接口](#存储接口)
  - [`CursorStore`](#cursorstore)
  - [`MemoryCursorStore`](#memorycursorstore)
- [密码类型](#密码类型)
  - [`PasswordInput`](#passwordinput)
- [枚举常量](#枚举常量)
  - [`DeliveryMode`](#deliverymode)
  - [`LoginInfo`](#logininfo)
- [错误类型](#错误类型)
  - [`ErrClosed` / `ErrNotConnected` / `ErrDisconnected`](#errclosed--errnotconnected--errdisconnected)
  - [`*ServerError`](#servererror)
  - [`*ProtocolError`](#protocolerror)
  - [`*ConnectionError`](#connectionerror)

---

## 配置类型

### `Config`

`NewClient()` 的唯一参数，配置 WebSocket 长连接客户端的所有行为。

```go
type Config struct {
    BaseURL               string
    Credentials           Credentials
    CursorStore           CursorStore
    Handler               Handler
    HTTPClient            *http.Client
    Logger                Logger
    Reconnect             bool
    InitialReconnectDelay time.Duration
    MaxReconnectDelay     time.Duration
    PingInterval          time.Duration
    RequestTimeout        time.Duration
    AckMessages           bool
    TransientOnly         bool
    RealtimeStream        bool
}
```

**必填字段：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `BaseURL` | `string` | 服务端 HTTP 地址，如 `"http://127.0.0.1:8080"`，SDK 自动转换为 WebSocket URL |
| `Credentials` | `Credentials` | WebSocket 首帧登录身份 |

**可选字段与默认值：**

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `CursorStore` | `NewMemoryCursorStore()` | 本地消息和游标持久层 |
| `Handler` | `NopHandler{}` | 生命周期和推送回调 |
| `HTTPClient` | `http.DefaultClient` | 自定义 HTTP 客户端，同时用于 HTTP 与 WebSocket Dial |
| `Logger` | `nil`（不输出） | 记录自动重连日志 |
| `Reconnect` | `true` | 是否自动重连。当前版本不能通过 `false` 关闭 |
| `InitialReconnectDelay` | `1s` | 首次重连退避 |
| `MaxReconnectDelay` | `30s` | 最大退避时间 |
| `PingInterval` | `30s` | 应用层 Ping 发送间隔 |
| `RequestTimeout` | `10s` | RPC 请求超时 |
| `AckMessages` | `true` | 收到 `MessagePushed` 后是否自动返回 `AckMessage` |
| `TransientOnly` | `false` | 是否只收瞬时流量，跳过持久化消息补发 |
| `RealtimeStream` | `false` | 是否连接 `/ws/realtime` 而非 `/ws/client` |

### `Credentials`

```go
type Credentials struct {
    NodeID   int64
    UserID   int64
    Password PasswordInput
}
```

WebSocket 首帧登录的身份凭证。`NodeID` 和 `UserID` 均不能为 0，`Password` 通过 `MustPlainPassword()` 或 `HashedPassword()` 构造。

---

## 核心引用类型

### `UserRef`

```go
type UserRef struct {
    NodeID int64 `json:"node_id"`
    UserID int64 `json:"user_id"`
}
```

用户引用，用于指定消息目标、用户查询、创建 attachment 等场景。两个字段都不能为 0。

主要方法：

- `validate() error`：校验 `NodeID` 和 `UserID` 是否非零。

### `SessionRef`

```go
type SessionRef struct {
    ServingNodeID int64  `json:"serving_node_id"`
    SessionID     string `json:"session_id"`
}
```

Online session 引用，标识某次 WebSocket 登录连接。常用于定向瞬时包。

主要方法：

- `IsZero() bool`：是否为空值
- `Valid() bool`：是否合法（两个字段均非零）

### `MessageCursor`

```go
type MessageCursor struct {
    NodeID int64 `json:"node_id"`
    Seq    int64 `json:"seq"`
}
```

消息游标，用于标识一条持久化消息在某个节点上的位置。也是 `seen_messages` 和断线恢复的基础单位。

---

## 消息与瞬时包

### `Message`

```go
type Message struct {
    Recipient    UserRef `json:"recipient"`
    NodeID       int64   `json:"node_id"`
    Seq          int64   `json:"seq"`
    Sender       UserRef `json:"sender"`
    Body         []byte  `json:"body"`
    CreatedAtHLC string  `json:"created_at_hlc"`
}
```

持久化消息。`(NodeID, Seq)` 构成全局唯一标识。`Body` 是原始字节，SDK 不做 JSON base64 或其它编码转换。

方法：

- `Cursor() MessageCursor`：返回该消息对应的游标 `{NodeID, Seq}`。

### `Packet`

```go
type Packet struct {
    PacketID      uint64       `json:"packet_id"`
    SourceNodeID  int64        `json:"source_node_id"`
    TargetNodeID  int64        `json:"target_node_id"`
    Recipient     UserRef      `json:"recipient"`
    Sender        UserRef      `json:"sender"`
    Body          []byte       `json:"body"`
    DeliveryMode  DeliveryMode `json:"delivery_mode"`
    TargetSession SessionRef   `json:"target_session"`
}
```

瞬时包。不会落库、没有游标、不会在重连后补发。`TargetSession` 有值时代表定向 packet。

### `RelayAccepted`

```go
type RelayAccepted struct {
    PacketID      uint64       `json:"packet_id"`
    SourceNodeID  int64        `json:"source_node_id"`
    TargetNodeID  int64        `json:"target_node_id"`
    Recipient     UserRef      `json:"recipient"`
    DeliveryMode  DeliveryMode `json:"delivery_mode"`
    TargetSession SessionRef   `json:"target_session"`
}
```

`SendPacket()` / `SendPacketToSession()` 的返回值。代表路由层已受理瞬时包，不代表目标一定收到。

### `SendMessageInput`

```go
type SendMessageInput struct {
    Target UserRef
    Body   []byte
}
```

`Client.SendMessage()` 的输入参数。

### `SendPacketInput`

```go
type SendPacketInput struct {
    Target        UserRef
    Body          []byte
    DeliveryMode  DeliveryMode
    TargetSession SessionRef
}
```

`Client.SendPacket()` 的输入参数。`DeliveryMode` 必须为 `DeliveryModeBestEffort` 或 `DeliveryModeRouteRetry`。`TargetSession` 可选，指定后只投递给某个具体在线连接。

---

## 用户与关系类型

### `User`

```go
type User struct {
    NodeID         int64  `json:"node_id"`
    UserID         int64  `json:"user_id"`
    Username       string `json:"username"`
    Role           string `json:"role"`
    ProfileJSON    []byte `json:"profile_json,omitempty"`
    SystemReserved bool   `json:"system_reserved"`
    CreatedAt      string `json:"created_at,omitempty"`
    UpdatedAt      string `json:"updated_at,omitempty"`
    OriginNodeID   int64  `json:"origin_node_id"`
}
```

用户实体。`Role` 可为 `"user"`、`"channel"`、`"admin"` 等。

### `Subscription`

```go
type Subscription struct {
    Subscriber   UserRef `json:"subscriber"`
    Channel      UserRef `json:"channel"`
    SubscribedAt string  `json:"subscribed_at,omitempty"`
    DeletedAt    string  `json:"deleted_at,omitempty"`
    OriginNodeID int64   `json:"origin_node_id"`
}
```

用户与 channel 的订阅关系。

### `BlacklistEntry`

```go
type BlacklistEntry struct {
    Owner        UserRef `json:"owner"`
    Blocked      UserRef `json:"blocked"`
    BlockedAt    string  `json:"blocked_at,omitempty"`
    DeletedAt    string  `json:"deleted_at,omitempty"`
    OriginNodeID int64   `json:"origin_node_id"`
}
```

黑名单条目。

### `Attachment`

```go
type Attachment struct {
    Owner          UserRef        `json:"owner"`
    Subject        UserRef        `json:"subject"`
    AttachmentType AttachmentType `json:"attachment_type"`
    ConfigJSON     []byte         `json:"config_json,omitempty"`
    AttachedAt     string         `json:"attached_at,omitempty"`
    DeletedAt      string         `json:"deleted_at,omitempty"`
    OriginNodeID   int64          `json:"origin_node_id"`
}
```

用户附件。底层协议用 attachment 统一实现订阅、黑名单等关系。通过 `AttachmentType` 区分不同语义。

### `AttachmentType`

```go
type AttachmentType string

const (
    AttachmentTypeChannelManager      AttachmentType = "channel_manager"
    AttachmentTypeChannelWriter       AttachmentType = "channel_writer"
    AttachmentTypeChannelSubscription AttachmentType = "channel_subscription"
    AttachmentTypeUserBlacklist       AttachmentType = "user_blacklist"
)
```

附件类型枚举。`Client` 和 `HTTPClient` 上的语义化方法（如 `SubscribeChannel`、`BlockUser`）内部实际使用这些 attachment 类型。

### `CreateUserRequest`

```go
type CreateUserRequest struct {
    Username    string        `json:"username"`
    Password    PasswordInput `json:"password,omitempty"`
    ProfileJSON []byte        `json:"profile_json,omitempty"`
    Role        string        `json:"role"`
}
```

### `UpdateUserRequest`

```go
type UpdateUserRequest struct {
    Username    *string        `json:"username,omitempty"`
    Password    *PasswordInput `json:"password,omitempty"`
    ProfileJSON *[]byte        `json:"profile_json,omitempty"`
    Role        *string        `json:"role,omitempty"`
}
```

使用指针字段表示可选更新，传 `nil` 表示不修改该字段。

---

## 用户元数据

### `UserMetadata`

```go
type UserMetadata struct {
    Owner        UserRef `json:"owner"`
    Key          string  `json:"key"`
    Value        []byte  `json:"value"`
    UpdatedAt    string  `json:"updated_at,omitempty"`
    DeletedAt    string  `json:"deleted_at,omitempty"`
    ExpiresAt    string  `json:"expires_at,omitempty"`
    OriginNodeID int64   `json:"origin_node_id"`
}
```

用户键值元数据。`Key` 支持字符集 `[A-Za-z0-9._:-]`，最长 128 字符。`Value` 为原始字节。

### `UpsertUserMetadataRequest`

```go
type UpsertUserMetadataRequest struct {
    Value     []byte  `json:"value"`
    ExpiresAt *string `json:"expires_at,omitempty"`
}
```

### `ScanUserMetadataRequest`

```go
type ScanUserMetadataRequest struct {
    Prefix string `json:"prefix,omitempty"`
    After  string `json:"after,omitempty"`
    Limit  int    `json:"limit,omitempty"`
}
```

分页扫描参数。`Prefix` 为键前缀过滤，`After` 为分页游标，`Limit` 最大 1000。

### `UserMetadataPage`

```go
type UserMetadataPage struct {
    Items     []UserMetadata `json:"items"`
    Count     int            `json:"count"`
    NextAfter string         `json:"next_after,omitempty"`
}
```

分页结果。`HasMore() bool` 方法判断是否还有更多数据。

---

## 运维类型

### `ResolvedUserSessions`

```go
type ResolvedUserSessions struct {
    User     UserRef              `json:"user"`
    Presence []OnlineNodePresence `json:"presence,omitempty"`
    Sessions []ResolvedSession    `json:"sessions,omitempty"`
}
```

`Client.ResolveUserSessions()` 的返回类型。包含按节点聚合的在线态 `Presence` 和可直接投递的 `Sessions`。

### `ResolvedSession`

```go
type ResolvedSession struct {
    Session          SessionRef `json:"session"`
    Transport        string     `json:"transport,omitempty"`
    TransientCapable bool       `json:"transient_capable"`
}
```

### `ClusterNode`

```go
type ClusterNode struct {
    NodeID        int64  `json:"node_id"`
    IsLocal       bool   `json:"is_local"`
    ConfiguredURL string `json:"configured_url,omitempty"`
    Source        string `json:"source,omitempty"`
}
```

### `LoggedInUser`

```go
type LoggedInUser struct {
    NodeID   int64  `json:"node_id"`
    UserID   int64  `json:"user_id"`
    Username string `json:"username"`
}
```

### `Event`

```go
type Event struct {
    Sequence        int64  `json:"sequence"`
    EventID         int64  `json:"event_id"`
    EventType       string `json:"event_type"`
    Aggregate       string `json:"aggregate"`
    AggregateNodeID int64  `json:"aggregate_node_id"`
    AggregateID     int64  `json:"aggregate_id"`
    HLC             string `json:"hlc,omitempty"`
    OriginNodeID    int64  `json:"origin_node_id"`
    EventJSON       []byte `json:"event_json,omitempty"`
}
```

### `OperationsStatus`

```go
type OperationsStatus struct {
    NodeID            int64             `json:"node_id"`
    MessageWindowSize int32             `json:"message_window_size"`
    LastEventSequence int64             `json:"last_event_sequence"`
    WriteGateReady    bool              `json:"write_gate_ready"`
    ConflictTotal     int64             `json:"conflict_total"`
    MessageTrim       MessageTrimStatus `json:"message_trim"`
    Projection        ProjectionStatus  `json:"projection"`
    Peers             []PeerStatus      `json:"peers,omitempty"`
}
```

---

## 回调接口

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

`Client` 的生命周期和推送回调。所有方法都在 SDK 内部 goroutine 中同步调用，业务侧应尽快返回，不要在回调中执行阻塞操作。

| 方法 | 触发时机 | 注意事项 |
|------|---------|---------|
| `OnLogin` | 每次 WebSocket 认证成功 | `LoginInfo.SessionRef` 标识当前连接 |
| `OnMessage` | 收到 `MessagePushed` | SDK 已先于回调执行 `SaveMessage -> SaveCursor` |
| `OnPacket` | 收到 `PacketPushed` | SDK 不做任何持久化处理 |
| `OnError` | 协议错误、持久化错误、重连读写错误 | 建议纳入监控和告警 |
| `OnDisconnect` | 连接断开 | 含断线原因 error |

### `NopHandler`

```go
type NopHandler struct{}
```

`Handler` 的空实现，所有方法都是无操作。如果不关心任何回调，可传 `NopHandler{}`。

### `Logger`

```go
type Logger interface {
    Printf(format string, args ...any)
}
```

日志接口。SDK 仅在自动重连时输出一行日志。兼容 `*log.Logger`。

---

## 存储接口

### `CursorStore`

```go
type CursorStore interface {
    LoadSeenMessages(context.Context) ([]MessageCursor, error)
    SaveMessage(context.Context, Message) error
    SaveCursor(context.Context, MessageCursor) error
}
```

本地游标持久化接缝。这是 SDK 断线恢复的基础。

| 方法 | 调用时机 |
|------|---------|
| `LoadSeenMessages` | 每次建连前（含重连） |
| `SaveMessage` | 收到 `MessagePushed` 或 `SendMessageResponse.message` |
| `SaveCursor` | 收到 `MessagePushed` 或 `SendMessageResponse.message`，在 `SaveMessage` 之后立即调用 |

### `MemoryCursorStore`

```go
func NewMemoryCursorStore() *MemoryCursorStore
```

`CursorStore` 的内存实现，仅用于演示和测试。生产环境应实现自己的持久化版本。

额外方法：

- `HasCursor(cursor MessageCursor) bool`
- `Message(cursor MessageCursor) (Message, bool)`

---

## 密码类型

### `PasswordInput`

```go
type PasswordInput struct {
    Source  PasswordSource // "plain" 或 "hashed"
    Encoded string         // bcrypt 哈希值
}
```

密码输入封装。无论是明文还是已有的哈希串，`WireValue()` 始终返回 bcrypt 哈希值，**不会**把明文传到线上。

构造方法：

| 函数 | 说明 |
|------|------|
| `MustPlainPassword(plain string) PasswordInput` | 传入明文，SDK 内部做 bcrypt。**推荐使用** |
| `PlainPassword(plain string) (PasswordInput, error)` | 同上，但返回 error |
| `HashedPassword(hash string) PasswordInput` | 传入已有 bcrypt 哈希串 |

辅助方法：

- `Validate() error`：校验密码有效性
- `WireValue() string`：获取线上传输用的哈希值
- `IsHashed() bool`：是否已设置值
- `IsZero() bool`：是否为空
- `MarshalJSON()` / `UnmarshalJSON()`：JSON 序列化时将密码视为哈希串

---

## 枚举常量

### `DeliveryMode`

```go
type DeliveryMode string

const (
    DeliveryModeUnspecified DeliveryMode = ""
    DeliveryModeBestEffort  DeliveryMode = "best_effort"
    DeliveryModeRouteRetry  DeliveryMode = "route_retry"
)
```

瞬时包投递模式：

- `DeliveryModeBestEffort`：尽力投递，不做重试
- `DeliveryModeRouteRetry`：路由层重试，仅在节点内存 TTL 窗口内有效

### `LoginInfo`

```go
type LoginInfo struct {
    User            User
    ProtocolVersion string
    SessionRef      SessionRef
}
```

`Handler.OnLogin()` 和 `Client.CurrentLogin()` 返回的登录结果信息。`SessionRef` 是当前在线连接的唯一标识。

---

## 错误类型

### `ErrClosed` / `ErrNotConnected` / `ErrDisconnected`

```go
var (
    ErrClosed       = errors.New("turntf client is closed")
    ErrNotConnected = errors.New("turntf client is not connected")
    ErrDisconnected = errors.New("turntf websocket disconnected")
)
```

包级别错误哨兵。使用 `errors.Is()` 判断：

```go
if errors.Is(err, turntf.ErrDisconnected) {
    // 处理断线场景
}
```

### `*ServerError`

```go
type ServerError struct {
    Code      string
    Message   string
    RequestID uint64
}
```

服务端返回的业务错误（对应 `ServerEnvelope.error`）。`RequestID` 为 0 表示是登录阶段错误（此时不会重连）。

方法：

- `Unauthorized() bool`：是否未授权错误

### `*ProtocolError`

```go
type ProtocolError struct {
    Message string
}
```

协议解析错误。收到预期外的 envelope 格式时返回。

### `*ConnectionError`

```go
type ConnectionError struct {
    Op  string
    Err error
}
```

网络连接错误。`Op` 表示出错的操作（如 `"dial"`、`"read"`、`"POST /users"`），可通过 `errors.Unwrap()` 获取底层错误。
