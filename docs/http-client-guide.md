# HTTP 客户端使用指南

本文档面向 `turntf-go` 的 `HTTPClient` 使用者，说明如何使用 HTTP JSON 接口完成登录、用户管理、消息发送、订阅和集群查询等操作。

`HTTPClient` 是轻量客户端，适合脚本、后台任务、初始化工具和调试场景。它**不**维护长连接、**不**负责消息持久化、**不**支持实时推送 -- 如果需要这些能力，请使用 `Client`。

## 概述

`HTTPClient` 封装了 turntf 服务端的 HTTP JSON REST 接口。与 `Client` 相比：

| 特性 | `HTTPClient` | `Client` |
|------|-------------|---------|
| 传输协议 | HTTP JSON | WebSocket + Protobuf |
| 实时推送 | 不支持 | 支持 `MessagePushed` / `PacketPushed` |
| 断线重连 | 不涉及 | 指数退避自动重连 |
| 本地游标 | 不参与 | 通过 `CursorStore` 管理 |
| Token 认证 | 每次请求携带 Bearer token | 首次登录通过首帧密码认证 |
| 适用场景 | 脚本、管理工具、初始化 | 实时业务客户端 |

## 创建客户端

```go
httpClient := turntf.NewHTTPClient("http://127.0.0.1:8080")
```

如果需要自定义 HTTP 客户端（如超时、TLS 配置）：

```go
httpClient := &turntf.HTTPClient{
    BaseURL:    "http://127.0.0.1:8080",
    HTTPClient: &http.Client{Timeout: 30 * time.Second},
}
```

## 认证

### 登录获取 Token

```go
token, err := httpClient.Login(ctx, 4096, 1, "root")
```

参数说明：
- `nodeID`：节点 ID（服务端自动生成，同一集群内唯一）
- `userID`：用户 ID
- `password`：明文密码，SDK 内部自动做 bcrypt 哈希

也可以直接按登录名登录：

```go
token, err := httpClient.LoginByLoginName(ctx, "alice.login", "alice-password")
```

### 使用 PasswordInput

```go
// 方式一：明文（SDK 自动 bcrypt）
token, err := httpClient.LoginWithPassword(ctx, 4096, 1, turntf.MustPlainPassword("root"))

// 方式二：已有 bcrypt 哈希串
token, err := httpClient.LoginWithPassword(ctx, 4096, 1, turntf.HashedPassword("$2a$10$..."))

// 登录名 + PasswordInput
token, err := httpClient.LoginByLoginNameWithPassword(ctx, "alice.login", turntf.MustPlainPassword("alice-password"))
```

返回的 `token` 是 Bearer token，后续所有管理接口都需携带。

## 用户管理

### 创建普通用户

```go
alice, err := httpClient.CreateUser(ctx, token, turntf.CreateUserRequest{
    Username:  "alice",
    LoginName: "alice.login",
    Password:  turntf.MustPlainPassword("alice-password"),
    Role:      "user",
})
```

`CreateUserRequest` 字段：

| 字段 | 必填 | 说明 |
|------|------|------|
| `Username` | 是 | 用户名 |
| `LoginName` | 否 | 登录名前解析字段；为空表示不绑定 |
| `Password` | 否 | 密码（channel 类型用户可省略） |
| `ProfileJSON` | 否 | 用户资料 JSON 字节 |
| `Role` | 是 | 角色：`"user"`、`"channel"`、`"admin"` |

返回的 `User` 中包含 `NodeID`、`UserID` 和 `LoginName`。其中 `(NodeID, UserID)` 与 `LoginName` 都可以作为后续登录前的身份选择器。

### 创建频道（Channel）

```go
orders, err := httpClient.CreateChannel(ctx, token, turntf.CreateUserRequest{
    Username: "orders",
})
```

`CreateChannel` 自动设置 `Role = "channel"`。Channel 用户不能登录，只用于接收订阅消息和做消息路由。

### 订阅频道

```go
err = httpClient.CreateSubscription(ctx, token,
    turntf.UserRef{NodeID: alice.NodeID, UserID: alice.UserID},
    turntf.UserRef{NodeID: orders.NodeID, UserID: orders.UserID},
)
```

将用户订阅到频道。订阅后，发送到频道的消息也会被推送给订阅者。

### 创建订阅（attachment 方式）

`CreateSubscription` 内部实际调用的是 `UpsertAttachment`，`AttachmentType` 为 `AttachmentTypeChannelSubscription`。

## 消息操作

### 发送持久化消息

```go
msg, err := httpClient.PostMessage(ctx, token,
    turntf.UserRef{NodeID: alice.NodeID, UserID: alice.UserID},
    []byte("hello"),
)
```

`PostMessage` 返回服务端确认写入后的 `Message`，包含 `NodeID`、`Seq`、`Sender` 等字段。`Body` 在 JSON 序列化时会自动转为 base64 编码。

### 发送瞬时包

```go
err := httpClient.PostPacket(ctx, token,
    8192, // targetNodeID - 目标节点 ID
    turntf.UserRef{NodeID: 8192, UserID: 2025}, // relayTarget - 目标用户
    []byte("ephemeral payload"),
    turntf.DeliveryModeRouteRetry,
)
```

`PostPacket` 与 `PostMessage` 共享同一个 HTTP 路径，但通过 `delivery_kind: "transient"` 区分。

参数说明：
- `targetNodeID`：目标节点 ID（必须与 `relayTarget.NodeID` 一致）
- `relayTarget`：目标用户引用
- `body`：瞬时包负载
- `mode`：投递模式，必须为 `DeliveryModeBestEffort` 或 `DeliveryModeRouteRetry`

返回 `nil` 表示路由层已受理，不等于目标已收到。

### 拉取历史消息

```go
messages, err := httpClient.ListMessages(ctx, token,
    turntf.UserRef{NodeID: 4096, UserID: 1025},
    20, // limit - 最多返回 20 条
)
```

按时间倒序返回指定用户的消息列表。不传 `limit` 或传 `0` 时返回全部（服务端有默认上限）。

## 黑名单管理

### 拉黑用户

```go
entry, err := httpClient.BlockUser(ctx, token,
    turntf.UserRef{NodeID: 4096, UserID: 1025}, // owner - 执行拉黑的用户
    turntf.UserRef{NodeID: 8192, UserID: 2025}, // blocked - 被拉黑的用户
)
```

### 取消拉黑

```go
entry, err := httpClient.UnblockUser(ctx, token,
    turntf.UserRef{NodeID: 4096, UserID: 1025},
    turntf.UserRef{NodeID: 8192, UserID: 2025},
)
```

### 列出已拉黑的用户

```go
entries, err := httpClient.ListBlockedUsers(ctx, token,
    turntf.UserRef{NodeID: 4096, UserID: 1025},
)
```

## 用户元数据

### 获取元数据

```go
metadata, err := httpClient.GetUserMetadata(ctx, token,
    turntf.UserRef{NodeID: 4096, UserID: 1025},
    "profile:display_name",
)
```

### 写入/更新元数据

```go
metadata, err := httpClient.UpsertUserMetadata(ctx, token,
    turntf.UserRef{NodeID: 4096, UserID: 1025},
    "profile:display_name",
    turntf.UpsertUserMetadataRequest{
        Value: []byte("Alice"),
    },
)
```

支持可选的过期时间：

```go
expiresAt := time.Now().Add(24 * time.Hour).Format(time.RFC3339)
metadata, err := httpClient.UpsertUserMetadata(ctx, token, owner, "temp:key",
    turntf.UpsertUserMetadataRequest{
        Value:     []byte("temporary"),
        ExpiresAt: &expiresAt,
    },
)
```

### 删除元数据

```go
metadata, err := httpClient.DeleteUserMetadata(ctx, token,
    turntf.UserRef{NodeID: 4096, UserID: 1025},
    "profile:display_name",
)
```

### 扫描元数据

```go
page, err := httpClient.ScanUserMetadata(ctx, token,
    turntf.UserRef{NodeID: 4096, UserID: 1025},
    turntf.ScanUserMetadataRequest{
        Prefix: "profile:",
        Limit:  50,
    },
)
```

遍历所有结果：

```go
page, err := httpClient.ScanUserMetadata(ctx, token, user, turntf.ScanUserMetadataRequest{Prefix: "profile:", Limit: 100})
for page.HasMore() {
    for _, item := range page.Items {
        fmt.Printf("key=%s value=%s\n", item.Key, string(item.Value))
    }
    page, err = httpClient.ScanUserMetadata(ctx, token, user, turntf.ScanUserMetadataRequest{
        Prefix: "profile:",
        After:  page.NextAfter,
        Limit:  100,
    })
}
```

元数据键约定：
- 支持字符：`[A-Za-z0-9._:-]`
- 最大长度：128 字符
- 建议使用 `:` 分隔命名空间（如 `profile:display_name`）

## 附件操作

附件是底层通用机制，订阅和黑名单等关系实际上都通过附件实现。

### 创建/更新附件

```go
attachment, err := httpClient.UpsertAttachment(ctx, token,
    turntf.UserRef{NodeID: 4096, UserID: 1025},           // owner
    turntf.UserRef{NodeID: 4096, UserID: 1026},           // subject
    turntf.AttachmentTypeChannelSubscription,              // attachmentType
    []byte("{}"),                                          // configJSON
)
```

### 删除附件

```go
attachment, err := httpClient.DeleteAttachment(ctx, token, owner, subject, turntf.AttachmentTypeChannelSubscription)
```

### 列出附件

```go
attachments, err := httpClient.ListAttachments(ctx, token, owner, turntf.AttachmentTypeChannelSubscription)
```

`AttachmentType` 的可选值：

| 常量 | 内部用途 |
|------|---------|
| `AttachmentTypeChannelSubscription` | 频道订阅 |
| `AttachmentTypeUserBlacklist` | 用户黑名单 |
| `AttachmentTypeChannelManager` | 频道管理员 |
| `AttachmentTypeChannelWriter` | 频道写者 |

## 集群查询

### 列出集群节点

```go
nodes, err := httpClient.ListClusterNodes(ctx, token)
```

返回当前节点视角下已连接的节点列表，包含 `NodeID`、`IsLocal`、`ConfiguredURL`。

### 查询节点在线用户

```go
users, err := httpClient.ListNodeLoggedInUsers(ctx, token, 4096)
```

返回指定节点上当前 WebSocket 已登录的用户列表。

## 错误处理

`HTTPClient` 的错误类型：

```go
// 服务端返回的业务错误
var serverErr *turntf.ServerError
if errors.As(err, &serverErr) {
    log.Printf("server error: code=%s message=%s", serverErr.Code, serverErr.Message)
}

// 网络/连接错误
var connErr *turntf.ConnectionError
if errors.As(err, &connErr) {
    log.Printf("connection error during %s: %v", connErr.Op, connErr.Err)
}

// HTTP 状态码异常（服务端返回非预期状态码）
var protoErr *turntf.ProtocolError
if errors.As(err, &protoErr) {
    log.Printf("protocol error: %s", protoErr.Message)
}
```

常见 HTTP 状态码说明：

| 状态码 | 含义 | 常见原因 |
|--------|------|---------|
| `200 OK` | 成功 | 查询类请求成功 |
| `201 Created` | 创建成功 | 用户创建、消息发送 |
| `202 Accepted` | 已受理 | 瞬时包投递 |
| `400 Bad Request` | 请求参数错误 | 字段缺失、非法组合 |
| `401 Unauthorized` | 未认证 | Token 无效或过期 |
| `403 Forbidden` | 权限不足 | 非管理员调用管理接口 |
| `404 Not Found` | 资源不存在 | 用户、节点未找到 |
| `409 Conflict` | 资源冲突 | 用户名已存在 |
| `503 Service Unavailable` | 服务暂不可用 | 节点尚未完成校时 |

## 完整示例

以下是一个使用 `HTTPClient` 完成用户创建、消息发送和订阅的完整示例：

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

    // 1. 管理员登录
    token, err := httpClient.Login(ctx, 4096, 1, "root")
    if err != nil {
        log.Fatal(err)
    }

    // 2. 创建频道
    orders, err := httpClient.CreateChannel(ctx, token, turntf.CreateUserRequest{
        Username: "orders",
    })
    if err != nil {
        log.Fatal(err)
    }

    // 3. 创建普通用户
    alice, err := httpClient.CreateUser(ctx, token, turntf.CreateUserRequest{
        Username: "alice",
        Password: turntf.MustPlainPassword("alice-password"),
        Role:     "user",
    })
    if err != nil {
        log.Fatal(err)
    }

    // 4. 将 alice 订阅到 orders 频道
    err = httpClient.CreateSubscription(ctx, token,
        turntf.UserRef{NodeID: alice.NodeID, UserID: alice.UserID},
        turntf.UserRef{NodeID: orders.NodeID, UserID: orders.UserID},
    )
    if err != nil {
        log.Fatal(err)
    }

    // 5. 向 orders 频道发送消息
    msg, err := httpClient.PostMessage(ctx, token,
        turntf.UserRef{NodeID: orders.NodeID, UserID: orders.UserID},
        []byte("new order created"),
    )
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("message sent: cursor=%d/%d", msg.NodeID, msg.Seq)

    // 6. 查询集群节点
    nodes, err := httpClient.ListClusterNodes(ctx, token)
    if err != nil {
        log.Fatal(err)
    }
    for _, node := range nodes {
        log.Printf("node: id=%d local=%v url=%s", node.NodeID, node.IsLocal, node.ConfiguredURL)
    }
}
```

## `HTTPClient` 与 `Client` 配合使用

在需要同时使用 HTTP 和 WebSocket 的场景中，`Client` 内含一个 `HTTPClient` 实例：

```go
client, _ := turntf.NewClient(config)
httpClient := client.HTTP() // 返回内部的 *HTTPClient

// 通过 HTTP 创建用户
token, _ := httpClient.Login(ctx, 4096, 1, "root")
alice, _ := httpClient.CreateUser(ctx, token, turntf.CreateUserRequest{
    Username: "alice",
    Password: turntf.MustPlainPassword("alice-password"),
    Role:     "user",
})

// 切换到 WebSocket 长连接
client.Connect(ctx)
msg, _ := client.SendMessage(ctx, turntf.SendMessageInput{
    Target: turntf.UserRef{NodeID: alice.NodeID, UserID: alice.UserID},
    Body:   []byte("hello via ws"),
})
```

## 注意事项

1. **Token 有效期**：HTTP token 有服务端配置的有效期，过期后需要重新登录获取。
2. **密码安全**：`HTTPClient.Login()` 传入的明文密码会被 SDK 在客户端侧做 bcrypt 哈希后再发送，不要自行预先哈希。
3. **Body 编码**：`PostMessage` 和 `PostPacket` 的 `body []byte` 在 HTTP JSON 中自动转为 base64，接收方会原样解码。
4. **无幂等保证**：HTTP 请求没有内置幂等机制，网络异常重试时可能出现重复消息。
5. **无实时推送**：`HTTPClient` 不接收 `MessagePushed` 或 `PacketPushed`，需要实时推送请使用 `Client`。
