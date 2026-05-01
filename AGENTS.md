# AGENTS - turntf-go Go SDK

## 项目概览

`turntf-go` 是 [turntf](https://github.com/tursom/turntf) 分布式通知服务的 Go 语言 SDK。它封装了服务端 HTTP JSON 和 WebSocket + Protobuf 两类接口，提供两条客户端能力线：

- **`Client`**：基于 WebSocket + Protobuf 的长连接客户端，负责登录、请求/响应匹配、自动重连、消息持久化回调、`session_ref` 和会话定向瞬时包。
- **`HTTPClient`**：基于 HTTP JSON 的轻量客户端，适合脚本、后台、初始化工具和调试，不负责任何实时状态或断线恢复。

SDK 的目标不是简单映射 REST 或 protobuf 字段，而是把业务接入时最容易出错的部分收进统一实现里，例如 WebSocket 首帧登录和 `seen_messages` 上报、`MessagePushed` 的持久化顺序、请求 ID 管理和 RPC 响应匹配、`session_ref` 和定向瞬时包等。

完整文档导航见 [README.md](README.md)。

## 目录结构

```
turntf-go/
├── AGENTS.md                          # 本文件 - AI/开发者指引
├── README.md                          # 项目总览与快速开始
├── client.go                          # Client 核心实现（WebSocket 长连接客户端）
├── client_test.go                     # Client 单元测试
├── http_client.go                     # HTTPClient 实现（HTTP JSON 轻量客户端）
├── http_client_test.go                # HTTPClient 单元测试
├── types.go                           # 核心类型定义与 proto 转换函数
├── errors.go                          # 错误类型定义
├── password.go                        # 密码处理（bcrypt 哈希）
├── password_test.go                   # 密码处理测试
├── store.go                           # CursorStore 接口与内存实现
├── generate.go                        # go generate 入口
├── go.mod / go.sum                    # Go 模块依赖
├── proto/
│   └── client.proto                   # Protobuf 协议定义源文件
├── internal/
│   └── proto/
│       └── client.pb.go              # 由 client.proto 生成的 Go 代码（不要手改）
├── scripts/
│   └── gen-proto.sh                  # Proto 生成脚本
├── demo/
│   ├── schema.go                     # YAML demo 场景定义与校验
│   ├── runtime.go                    # YAML demo 运行时执行器
│   └── runtime_test.go               # Demo 运行时测试
├── cmd/
│   └── turntf-demo/
│       └── main.go                   # Demo 运行器主入口
├── config/
│   └── cluster-message-test.yaml     # Demo 配置文件示例
└── docs/
    ├── sdk-guide.md                  # Go SDK 使用总览（推荐先读）
    ├── client-flow.md                # 客户端全流程接入文档
    ├── client-websocket.md           # 客户端 WebSocket 接口协议
    ├── operations.md                 # 运维与上线手册
    ├── api-reference.md              # API 参考文档
    ├── http-client-guide.md          # HTTP 客户端使用指南
    └── examples/
        ├── demo-cross-node.yaml      # 跨节点收发 demo
        └── demo-admin-blacklist.yaml # 管理端黑名单 demo
```

## 构建与测试

```bash
# 构建所有包
go build ./...

# 运行所有测试
go test ./...

# 运行测试并显示详细输出
go test -v ./...

# 运行特定测试
go test -run TestConnect -v ./...

# 检查代码格式化
gofmt -d .

# 格式化代码
gofmt -w .

# 静态分析（需安装 staticcheck）
staticcheck ./...
```

## Proto 生成

### 源文件与输出

- **源文件**：`proto/client.proto`
- **生成文件**：`internal/proto/client.pb.go`
- **生成入口**：`generate.go`（通过 `//go:generate` 触发）

**不要手改生成文件 `internal/proto/client.pb.go`**，所有 proto 修改后必须通过自动生成同步。

### 重新生成命令

```bash
# 方式一：通过 go generate（推荐）
go generate ./...

# 方式二：直接运行脚本
./scripts/gen-proto.sh
```

### 前置依赖

- 本机需安装 `protoc`（Protocol Buffers 编译器）
- `protoc-gen-go` 需在 `PATH` 中

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

### 生成流程说明

`scripts/gen-proto.sh` 的实际执行逻辑：

1. 切换到项目根目录
2. 检查 `protoc` 和 `protoc-gen-go` 是否在 PATH 中
3. 执行：
   ```bash
   protoc \
     --go_out=. \
     --go_opt=module=github.com/tursom/turntf-go \
     proto/client.proto
   ```

`go_package` 选项指向 `github.com/tursom/turntf-go/internal/proto;proto`，因此生成的代码落入 `internal/proto/` 目录。

### 修改 Proto 后的检查清单

修改 `proto/client.proto` 后：

1. 重新生成：`go generate ./...`
2. 确认 `internal/proto/client.pb.go` 已更新
3. 检查 `types.go` 中的 `xxxFromProto` / `xxxToProto` 转换函数是否需要更新
4. 检查 `client.go` 中 `handleServerEnvelope()` 的处理分支是否需要补充新消息类型
5. 同步检查以下文档是否仍然准确：
   - `README.md`
   - `docs/sdk-guide.md`
   - `docs/client-flow.md`
   - `docs/client-websocket.md`
   - `docs/api-reference.md`
   - `docs/http-client-guide.md`

### Proto 修改提交规范

- 提交 proto 变更时**必须**一并提交 `internal/proto/client.pb.go`
- 如果 proto 字段名或语义有变化，**必须**同步更新 `types.go` 和 `client.go` 中对应的转换逻辑
- 如果新增了 RPC 消息类型，**必须**在 `client.go` 的 `handleServerEnvelope()` 中增加对应 `resolvePending` 处理分支

## 代码规范

### 导入顺序

```go
import (
    // 1. 标准库
    "context"
    "fmt"

    // 2. 第三方依赖
    "github.com/coder/websocket"
    "google.golang.org/protobuf/proto"

    // 3. 内部包（按模块路径）
    pb "github.com/tursom/turntf-go/internal/proto"
)
```

### 命名约定

- **公开类型**：使用驼峰命名，首字母大写（`Client`, `HTTPClient`, `Config`, `UserRef`）
- **私有类型和函数**：首字母小写（`requestResult`, `websocketURL`, `userFromProto`）
- **常量**：使用 PascalCase 加描述性前缀（`DeliveryModeBestEffort`, `AttachmentTypeChannelSubscription`）
- **接口**：单方法接口使用 `-er` 后缀（如 `Logger`）；多方法接口使用领域名（如 `Handler`, `CursorStore`）
- **测试辅助类型**：使用描述性前缀（`recordingStore`, `recordingHandler`）

### 错误处理

- SDK 内部错误使用包级别错误变量：`ErrClosed`, `ErrNotConnected`, `ErrDisconnected`
- 服务端返回的错误使用 `*ServerError` 类型，包含 `Code`, `Message`, `RequestID`
- 协议解析错误使用 `*ProtocolError` 类型
- 网络/连接错误使用 `*ConnectionError` 类型，包含 `Op`（出错操作）和 `Err`（底层错误）
- 所有错误都暴露给业务侧，通过 `Handler.OnError()` 回调或同步返回值传递

### 安全约束

- **密码安全**：`PasswordInput` 封装 bcrypt 哈希逻辑。`MustPlainPassword()` 在 SDK 内部完成哈希，**不要**把明文密码序列化到线上请求。
- **WebSocket 安全**：`Credentials` 中的密码同样做 bcrypt 哈希，以哈希值出现在 `LoginRequest.password` 字段。
- **Token 传递**：`Client` 上保留了部分带 `token string` 参数的方法名以兼容旧调用方式，但这些方法实际走已登录 WebSocket RPC，`token` 参数不会参与鉴权。

### 并发安全

- `Client` 内使用 `sync.Mutex` / `sync.RWMutex` 保护连接状态、pending RPC map 和认证状态
- `MemoryCursorStore` 使用 `sync.Mutex` 保护消息和游标存储
- 写 WebSocket 操作通过 `writeMu` 互斥锁保护，禁止并行写入

### 持久化顺序约束

这是 SDK 最核心的约束，固定顺序为：

```
SaveMessage -> SaveCursor -> AckMessage -> Handler.OnMessage()
```

任何时候不要在业务封装中把 ack 提前到落库之前。`AckMessage` 只影响当前连接内的去重集合；真正的重连恢复依赖 `LoadSeenMessages()`。

## 模块结构描述

| 包/文件 | 职责 | 关键导出 |
|---------|------|---------|
| `turntf` (根包) | SDK 核心包 | `Client`, `HTTPClient`, `Config`, `Handler`, `CursorStore` |
| `client.go` | WebSocket 长连接客户端 | `NewClient()`, `Connect()`, `Close()`, `SendMessage()`, `SendPacket()` |
| `http_client.go` | HTTP JSON 客户端 | `NewHTTPClient()`, `Login()`, `CreateUser()`, `PostMessage()` |
| `types.go` | 共享类型与 proto 转换 | `UserRef`, `SessionRef`, `Message`, `Packet`, 所有 `xxxFromProto`/`xxxToProto` |
| `errors.go` | 错误类型定义 | `ErrClosed`, `ErrNotConnected`, `ServerError`, `ConnectionError` |
| `password.go` | 密码处理 | `PasswordInput`, `PlainPassword()`, `MustPlainPassword()`, `HashedPassword()` |
| `store.go` | 游标存储接缝 | `CursorStore` 接口, `MemoryCursorStore` |
| `demo/` | YAML demo 运行器 | `LoadFile()`, `RunScenario()`, `Scenario` 解析与校验 |
| `cmd/turntf-demo/` | Demo 入口 | CLI 主函数，解析 `-f` 和 `-timeout` 参数 |

## 核心 API

### `Client`（WebSocket 长连接客户端）

```go
type Config struct {
    BaseURL               string        // 服务端 HTTP 地址（必填）
    Credentials           Credentials   // WebSocket 首帧登录身份（必填）
    CursorStore           CursorStore   // 本地游标持久层（默认 MemoryCursorStore）
    Handler               Handler       // 生命周期和推送回调（默认 NopHandler）
    HTTPClient            *http.Client  // 自定义 HTTP 客户端
    Logger                Logger        // 重连日志
    Reconnect             bool          // 是否自动重连（默认 true）
    InitialReconnectDelay time.Duration // 首次重连退避（默认 1s）
    MaxReconnectDelay     time.Duration // 最大重连退避（默认 30s）
    PingInterval          time.Duration // 应用层 Ping 间隔（默认 30s）
    RequestTimeout        time.Duration // RPC 超时（默认 10s）
    AckMessages           bool          // 是否自动 Ack（默认 true）
    TransientOnly         bool          // 是否只收瞬时流量
    RealtimeStream        bool          // 是否走 /ws/realtime
}

type Client struct{ /* ... */ }

// 生命周期
func NewClient(cfg Config) (*Client, error)
func (c *Client) Connect(ctx context.Context) error
func (c *Client) Close() error
func (c *Client) CurrentLogin() (LoginInfo, bool)
func (c *Client) Ping(ctx context.Context) error
func (c *Client) HTTP() *HTTPClient

// 持久化消息
func (c *Client) SendMessage(ctx context.Context, input SendMessageInput) (Message, error)
func (c *Client) WSListMessages(ctx context.Context, target UserRef, limit int) ([]Message, error)

// 瞬时包
func (c *Client) SendPacket(ctx context.Context, input SendPacketInput) (RelayAccepted, error)
func (c *Client) SendPacketToSession(ctx context.Context, target UserRef, targetSession SessionRef, body []byte, mode DeliveryMode) (RelayAccepted, error)

// 用户管理
func (c *Client) CreateUser(ctx context.Context, token string, req CreateUserRequest) (User, error)
func (c *Client) CreateChannel(ctx context.Context, token string, req CreateUserRequest) (User, error)
func (c *Client) GetUser(ctx context.Context, target UserRef) (User, error)
func (c *Client) UpdateUser(ctx context.Context, target UserRef, req UpdateUserRequest) (User, error)
func (c *Client) DeleteUser(ctx context.Context, target UserRef) (DeleteUserResult, error)

// 关系管理
func (c *Client) SubscribeChannel(ctx context.Context, token string, subscriber, channel UserRef) (Subscription, error)
func (c *Client) UnsubscribeChannel(ctx context.Context, subscriber, channel UserRef) (Subscription, error)
func (c *Client) ListSubscriptions(ctx context.Context, subscriber UserRef) ([]Subscription, error)

// 黑名单
func (c *Client) BlockUser(ctx context.Context, token string, owner, blocked UserRef) (BlacklistEntry, error)
func (c *Client) UnblockUser(ctx context.Context, token string, owner, blocked UserRef) (BlacklistEntry, error)
func (c *Client) ListBlockedUsers(ctx context.Context, token string, owner UserRef) ([]BlacklistEntry, error)

// 用户元数据
func (c *Client) WSGetUserMetadata(ctx context.Context, owner UserRef, key string) (UserMetadata, error)
func (c *Client) WSUpsertUserMetadata(ctx context.Context, owner UserRef, key string, req UpsertUserMetadataRequest) (UserMetadata, error)
func (c *Client) WSDeleteUserMetadata(ctx context.Context, owner UserRef, key string) (UserMetadata, error)
func (c *Client) WSScanUserMetadata(ctx context.Context, owner UserRef, req ScanUserMetadataRequest) (UserMetadataPage, error)

// 通用 Attachment
func (c *Client) UpsertAttachment(ctx context.Context, owner, subject UserRef, attachmentType AttachmentType, configJSON []byte) (Attachment, error)
func (c *Client) DeleteAttachment(ctx context.Context, owner, subject UserRef, attachmentType AttachmentType) (Attachment, error)
func (c *Client) ListAttachments(ctx context.Context, owner UserRef, attachmentType AttachmentType) ([]Attachment, error)

// 运维查询
func (c *Client) ListEvents(ctx context.Context, after int64, limit int) ([]Event, error)
func (c *Client) ListClusterNodes(ctx context.Context) ([]ClusterNode, error)
func (c *Client) ListNodeLoggedInUsers(ctx context.Context, nodeID int64) ([]LoggedInUser, error)
func (c *Client) ResolveUserSessions(ctx context.Context, user UserRef) (ResolvedUserSessions, error)
func (c *Client) OperationsStatus(ctx context.Context) (OperationsStatus, error)
func (c *Client) Metrics(ctx context.Context) (string, error)
```

### `HTTPClient`（HTTP JSON 轻量客户端）

```go
type HTTPClient struct {
    BaseURL    string
    HTTPClient *http.Client
}

func NewHTTPClient(baseURL string) *HTTPClient

// 登录
func (c *HTTPClient) Login(ctx context.Context, nodeID, userID int64, password string) (string, error)
func (c *HTTPClient) LoginWithPassword(ctx context.Context, nodeID, userID int64, password PasswordInput) (string, error)

// 用户管理
func (c *HTTPClient) CreateUser(ctx context.Context, token string, req CreateUserRequest) (User, error)
func (c *HTTPClient) CreateChannel(ctx context.Context, token string, req CreateUserRequest) (User, error)

// 消息
func (c *HTTPClient) ListMessages(ctx context.Context, token string, target UserRef, limit int) ([]Message, error)
func (c *HTTPClient) PostMessage(ctx context.Context, token string, target UserRef, body []byte) (Message, error)
func (c *HTTPClient) PostPacket(ctx context.Context, token string, targetNodeID int64, relayTarget UserRef, body []byte, mode DeliveryMode) error

// 订阅
func (c *HTTPClient) CreateSubscription(ctx context.Context, token string, userRef, channelRef UserRef) error

// 附件
func (c *HTTPClient) UpsertAttachment(ctx context.Context, token string, owner, subject UserRef, attachmentType AttachmentType, configJSON []byte) (Attachment, error)
func (c *HTTPClient) DeleteAttachment(ctx context.Context, token string, owner, subject UserRef, attachmentType AttachmentType) (Attachment, error)
func (c *HTTPClient) ListAttachments(ctx context.Context, token string, owner UserRef, attachmentType AttachmentType) ([]Attachment, error)

// 黑名单
func (c *HTTPClient) BlockUser(ctx context.Context, token string, owner, blocked UserRef) (BlacklistEntry, error)
func (c *HTTPClient) UnblockUser(ctx context.Context, token string, owner, blocked UserRef) (BlacklistEntry, error)
func (c *HTTPClient) ListBlockedUsers(ctx context.Context, token string, owner UserRef) ([]BlacklistEntry, error)

// 用户元数据
func (c *HTTPClient) GetUserMetadata(ctx context.Context, token string, owner UserRef, key string) (UserMetadata, error)
func (c *HTTPClient) UpsertUserMetadata(ctx context.Context, token string, owner UserRef, key string, req UpsertUserMetadataRequest) (UserMetadata, error)
func (c *HTTPClient) DeleteUserMetadata(ctx context.Context, token string, owner UserRef, key string) (UserMetadata, error)
func (c *HTTPClient) ScanUserMetadata(ctx context.Context, token string, owner UserRef, req ScanUserMetadataRequest) (UserMetadataPage, error)

// 集群
func (c *HTTPClient) ListClusterNodes(ctx context.Context, token string) ([]ClusterNode, error)
func (c *HTTPClient) ListNodeLoggedInUsers(ctx context.Context, token string, nodeID int64) ([]LoggedInUser, error)
```

### 核心类型

| 类型 | 说明 | 关键字段 |
|------|------|---------|
| `UserRef` | 用户引用 | `NodeID`, `UserID` |
| `SessionRef` | Session 引用 | `ServingNodeID`, `SessionID` |
| `Credentials` | 登录凭证 | `NodeID`, `UserID`, `Password` |
| `LoginInfo` | 登录结果 | `User`, `ProtocolVersion`, `SessionRef` |
| `Config` | 客户端配置 | 见上方 `Config` 结构体 |
| `Handler` | 回调接口 | `OnLogin`, `OnMessage`, `OnPacket`, `OnError`, `OnDisconnect` |
| `Message` | 持久化消息 | `Recipient`, `NodeID`, `Seq`, `Sender`, `Body`, `CreatedAtHLC` |
| `Packet` | 瞬时包 | `PacketID`, `Recipient`, `Sender`, `Body`, `DeliveryMode`, `TargetSession` |
| `RelayAccepted` | 瞬时包受理结果 | `PacketID`, `TargetNodeID`, `Recipient`, `DeliveryMode`, `TargetSession` |
| `MessageCursor` | 消息游标 | `NodeID`, `Seq` |
| `CursorStore` | 游标存储接口 | `LoadSeenMessages`, `SaveMessage`, `SaveCursor` |
| `DeliveryMode` | 瞬时投递模式 | `DeliveryModeBestEffort`, `DeliveryModeRouteRetry` |
| `AttachmentType` | 附件类型 | `ChannelManager`, `ChannelWriter`, `ChannelSubscription`, `UserBlacklist` |
| `User` | 用户信息 | `NodeID`, `UserID`, `Username`, `Role`, `ProfileJSON` |

### 错误类型

| 类型 | 说明 | 关键字段/值 |
|------|------|-------------|
| `ErrClosed` | 客户端已关闭 | `errors.Is(err, ErrClosed)` |
| `ErrNotConnected` | 客户端未连接 | `errors.Is(err, ErrNotConnected)` |
| `ErrDisconnected` | 连接已断开 | `errors.Is(err, ErrDisconnected)` |
| `*ServerError` | 服务端返回错误 | `Code`, `Message`, `RequestID` |
| `*ProtocolError` | 协议解析错误 | `Message` |
| `*ConnectionError` | 网络/连接错误 | `Op`, `Err`，可通过 `errors.Unwrap` 获取底层错误 |

## Demo 系统

### 概述

仓库内置了一个 YAML 驱动的 demo 运行器，用于端到端验证 SDK 与 turntf 服务端的交互。它**只走 WebSocket 接口**（不允许调用 HTTP 接口），通过 YAML 描述多节点、多 session 的收发场景。

### 运行方法

```bash
# 运行指定场景文件
go run ./cmd/turntf-demo -f docs/examples/demo-cross-node.yaml

# 带超时运行（防止死锁）
go run ./cmd/turntf-demo -f docs/examples/demo-cross-node.yaml -timeout 30s

# 使用 config 目录下的场景
go run ./cmd/turntf-demo -f config/cluster-message-test.yaml
```

### 命令行参数

- `-f`（必填）：YAML 场景文件的路径
- `-timeout`（可选）：整体场景超时时间，防止脚本死锁

### YAML 场景格式

场景文件版本为 `v1alpha1`，结构如下：

```yaml
version: v1alpha1
name: <场景名称>
description: <场景描述>
defaults:          # 全局默认值
  timeout: 5s
  idle_timeout: 500ms
  auto_ack_messages: true
vars:              # 模板变量，在 script 中用 ${var_name} 引用
  message_text: hello from yaml
nodes:             # 服务端节点定义
  node_a:
    base_url: http://127.0.0.1:8080
sessions:          # 客户端 session 定义
  alice:
    node: node_a
    user:
      node_id: 4096
      user_id: 1025
      password:
        source: plain
        value: alice-password
script:            # 脚本步骤序列
  - step: connect
    session: alice
    expect:
      login:
        user:
          node_id: 4096
          user_id: 1025
```

### 支持的步骤类型

| 步骤 | 说明 |
|------|------|
| `connect` | 建立 WebSocket 连接并登录，需要 `expect.login` |
| `request` | 发送 RPC 请求，需要 `action`、`request` 和 `expect` |
| `expect_event` | 等待异步事件（`message_pushed`, `packet_pushed`, `error`, `disconnect`） |
| `sleep` | 等待指定时长，需要 `duration` |
| `close` | 关闭指定 session 的连接 |
| `parallel` | 并行执行多个分支，需要 `branches` 数组，支持 `barrier` 同步 |
| `barrier` | 用于 parallel 分支间的同步屏障 |

### 支持的 Action

`request` 步骤支持的 `action` 值：`send_message`, `send_packet`, `ping`, `create_user`, `get_user`, `update_user`, `delete_user`, `list_messages`, `subscribe_channel`, `unsubscribe_channel`, `list_subscriptions`, `block_user`, `unblock_user`, `list_blocked_users`, `list_events`, `operations_status`, `metrics`, `list_cluster_nodes`, `list_node_logged_in_users`, `resolve_user_sessions`。

### 变量插值

脚本中使用 `${var_name}` 语法引用 `vars` 中定义的变量，SDK 会在运行时展开。

### Demo 示例文件

- `docs/examples/demo-cross-node.yaml`：跨节点收发持久化消息（alice 在 node_a 发消息给 node_b 上的 bob）
- `docs/examples/demo-admin-blacklist.yaml`：管理端黑名单操作流程

### Demo 实现说明

Demo 运行器的实现在 `demo/` 包中：
- `schema.go`：YAML 场景定义、校验、默认值填充
- `runtime.go`：场景执行引擎，管理 session 生命周期、并行分支、事件匹配和变量插值
- `runtime_test.go`：运行器测试

## 提交约定

- Git 提交信息使用 [Conventional Commits](https://www.conventionalcommits.org/) 风格
- 提交作者必须为 `tursom <tursom@foxmail.com>`
- 常见提交类型：
  - `feat:` 新增功能
  - `fix:` 修复 bug
  - `docs:` 文档变更
  - `refactor:` 重构
  - `test:` 测试相关
  - `chore:` 构建、依赖、脚本等杂项
- 修改 `proto/client.proto` 后**必须**重新生成代码
- 提交 proto 变更时**必须**一并提交 `internal/proto/client.pb.go`
- Proto 新增消息类型时，需要同步更新 `client.go` 中的 `handleServerEnvelope()` 和 `types.go` 中的转换函数
- 文档变更（`docs/`、`README.md`、`AGENTS.md`）与代码变更应放在同一提交中
