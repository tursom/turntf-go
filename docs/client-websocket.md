# 客户端 WebSocket 接口

本文档描述 `turntf-go` 对应的 WebSocket + Protobuf 客户端协议。它关注的是“线上到底收发什么 envelope”，而不是 Go API 本身的调用方式；SDK 用法见 [Go SDK 使用总览](sdk-guide.md)，端到端流程见 [客户端全流程接入文档](client-flow.md)。

## 连接入口

客户端有两个 WebSocket 入口：

- `GET /ws/client`
- `GET /ws/realtime`

二者共同点：

- 都只接受 binary frame
- 每个 binary frame 都必须是一条完整 protobuf 消息
- 客户端发送 `notifier.client.v1.ClientEnvelope`
- 服务端发送 `notifier.client.v1.ServerEnvelope`
- 首帧都必须是 `ClientEnvelope.login`

二者差异：

| 路径 | 典型用途 | 持久化消息补发 / 推送 | 支持的 RPC 范围 |
| --- | --- | --- | --- |
| `/ws/client` | 普通客户端、管理员客户端 | 默认支持，可用 `transient_only` 关闭 | 最完整 |
| `/ws/realtime` | 瞬时流量、在线态、定向 packet | 不支持，天然是 transient-only | 只保留在线态与 transient 相关 RPC |

`turntf-go` 对应的配置映射：

- `Config.RealtimeStream = false` -> `/ws/client`
- `Config.RealtimeStream = true` -> `/ws/realtime`

## Envelope 结构

`client.proto` 里的顶层 oneof 如下：

```protobuf
message ClientEnvelope {
  oneof body {
    LoginRequest login = 1;
    SendMessageRequest send_message = 2;
    AckMessage ack_message = 3;
    Ping ping = 4;
    CreateUserRequest create_user = 5;
    GetUserRequest get_user = 6;
    UpdateUserRequest update_user = 7;
    DeleteUserRequest delete_user = 8;
    ListMessagesRequest list_messages = 9;
    UpsertUserAttachmentRequest upsert_user_attachment = 10;
    DeleteUserAttachmentRequest delete_user_attachment = 11;
    ListUserAttachmentsRequest list_user_attachments = 12;
    ListEventsRequest list_events = 13;
    OperationsStatusRequest operations_status = 14;
    MetricsRequest metrics = 15;
    ListClusterNodesRequest list_cluster_nodes = 16;
    ListNodeLoggedInUsersRequest list_node_logged_in_users = 17;
    ResolveUserSessionsRequest resolve_user_sessions = 18;
  }
}
```

```protobuf
message ServerEnvelope {
  oneof body {
    LoginResponse login_response = 1;
    MessagePushed message_pushed = 2;
    SendMessageResponse send_message_response = 3;
    Error error = 4;
    Pong pong = 5;
    PacketPushed packet_pushed = 6;
    CreateUserResponse create_user_response = 7;
    GetUserResponse get_user_response = 8;
    UpdateUserResponse update_user_response = 9;
    DeleteUserResponse delete_user_response = 10;
    ListMessagesResponse list_messages_response = 11;
    UpsertUserAttachmentResponse upsert_user_attachment_response = 12;
    DeleteUserAttachmentResponse delete_user_attachment_response = 13;
    ListUserAttachmentsResponse list_user_attachments_response = 14;
    ListEventsResponse list_events_response = 15;
    OperationsStatusResponse operations_status_response = 16;
    MetricsResponse metrics_response = 17;
    ListClusterNodesResponse list_cluster_nodes_response = 18;
    ListNodeLoggedInUsersResponse list_node_logged_in_users_response = 19;
    ResolveUserSessionsResponse resolve_user_sessions_response = 20;
  }
}
```

## 登录

### 首帧格式

当前 proto 中，登录身份有两种二选一写法：

- 旧式 `(node_id, user_id)`：放在嵌套的 `user` 字段里
- 新式 `login_name`：直接写 `LoginRequest.login_name`

示例一，按 `(node_id, user_id)` 登录：

```protobuf
ClientEnvelope {
  login: LoginRequest {
    user: { node_id: 4096, user_id: 1025 }
    password: "$2a$10$..."
    seen_messages: [
      { node_id: 4096, seq: 1 },
      { node_id: 4097, seq: 8 }
    ]
    transient_only: false
  }
}
```

示例二，按 `login_name` 登录：

```protobuf
ClientEnvelope {
  login: LoginRequest {
    login_name: "alice.login"
    password: "$2a$10$..."
    seen_messages: []
    transient_only: false
  }
}
```

字段说明：

- `user`：旧式登录身份选择器，和 `login_name` 二选一
- `login_name`：新增登录名选择器，和 `user` 二选一
- `password`：密码哈希。`turntf-go` 的 `MustPlainPassword` / `PlainPassword` 会先做 bcrypt，再把哈希串写到线上请求
- `seen_messages`：客户端已经安全持久化的消息游标集合
- `transient_only`：为 `true` 时跳过持久化消息补发和后续持久化消息推送，但仍可接收瞬时包

### 登录响应

```protobuf
ServerEnvelope {
  login_response: LoginResponse {
    user: {
      node_id: 4096
      user_id: 1025
      username: "alice"
      login_name: "alice.login"
      role: "user"
    }
    protocol_version: "client-v1alpha1"
    session_ref: {
      serving_node_id: 4096
      session_id: "session-a"
    }
  }
}
```

`session_ref` 是这一次在线连接的唯一标识，用于：

- 业务侧记录当前会话身份
- 其他客户端通过 `resolve_user_sessions` 查询在线 session
- 定向瞬时包时写入 `target_session`

### 登录失败

登录失败时，服务端返回：

```protobuf
ServerEnvelope {
  error: Error {
    code: "unauthorized"
    message: "invalid credentials"
  }
}
```

随后关闭连接。`turntf-go` 当前会在登录阶段收到 `unauthorized` 时停止自动重连。

## `seen_messages`、游标和 `AckMessage`

客户端可靠恢复的关键不是“服务端记住了哪些 ack”，而是：

1. 本地先把消息安全落库
2. 本地记录 `(node_id, seq)` 游标
3. 重连登录时重新带上 `seen_messages`

游标定义：

```protobuf
message MessageCursor {
  int64 node_id = 1;
  int64 seq = 2;
}
```

`AckMessage` 的作用只是“告诉当前这条连接，我已经处理过这个游标了”：

```protobuf
ClientEnvelope {
  ack_message: AckMessage {
    cursor: { node_id: 4096, seq: 3 }
  }
}
```

注意：

- `AckMessage` 只影响当前连接内的内存去重集合
- `AckMessage` 不会替代 `seen_messages`
- 瞬时包没有 `(node_id, seq)`，因此完全不参与 ack / 重放

## 接收持久化消息

登录成功后，如果连接不是 transient-only，服务端会：

1. 先补发当前用户可见、且不在 `seen_messages` 中的历史消息
2. 再继续推送实时持久化消息

消息格式：

```protobuf
ServerEnvelope {
  message_pushed: MessagePushed {
    message: {
      recipient: { node_id: 4096, user_id: 1025 }
      node_id: 4096
      seq: 3
      sender: { node_id: 4096, user_id: 1 }
      body: "\xff\x00payload"
      created_at_hlc: "..."
    }
  }
}
```

处理顺序建议：

1. 按 `(node_id, seq)` 做本地幂等检查
2. 保存完整消息
3. 保存游标
4. 连接仍在时发送 `AckMessage`

`turntf-go` 高层 SDK 已经内建了这条顺序。

## 接收瞬时包

瞬时包使用 `PacketPushed`：

```protobuf
ServerEnvelope {
  packet_pushed: PacketPushed {
    packet: {
      packet_id: 77
      source_node_id: 4096
      target_node_id: 8192
      recipient: { node_id: 8192, user_id: 1025 }
      sender: { node_id: 4096, user_id: 1 }
      body: "\xff\x00payload"
      delivery_mode: CLIENT_DELIVERY_MODE_ROUTE_RETRY
      target_session: {
        serving_node_id: 8192
        session_id: "session-b"
      }
    }
  }
}
```

特点：

- 不落消息库
- 没有 `(node_id, seq)` 游标
- 不会在重连后补发
- 只有目标用户当前在线时才可能收到
- `target_session` 有值时，代表这是面向某个具体在线连接的定向 packet

## 发送消息

### 持久化消息

```protobuf
ClientEnvelope {
  send_message: SendMessageRequest {
    request_id: 42
    target: { node_id: 4096, user_id: 1025 }
    body: "\xff\x00payload"
  }
}
```

成功响应：

```protobuf
ServerEnvelope {
  send_message_response: SendMessageResponse {
    request_id: 42
    message: {
      recipient: { node_id: 4096, user_id: 1025 }
      node_id: 4096
      seq: 4
      sender: { node_id: 4096, user_id: 1025 }
      body: "\xff\x00payload"
      created_at_hlc: "..."
    }
  }
}
```

说明：

- 持久化消息成功后，服务端返回完整 `Message`
- 高层 `turntf-go.Client.SendMessage()` 会把这条返回消息也交给本地 `CursorStore`
- proto 中存在 `sync_mode` 字段，但当前高层 Go SDK 没有暴露对应配置，默认让服务端使用自身的同步策略

### 瞬时包

```protobuf
ClientEnvelope {
  send_message: SendMessageRequest {
    request_id: 43
    target: { node_id: 8192, user_id: 1025 }
    body: "\xff\x00payload"
    delivery_kind: CLIENT_DELIVERY_KIND_TRANSIENT
    delivery_mode: CLIENT_DELIVERY_MODE_ROUTE_RETRY
    target_session: {
      serving_node_id: 8192
      session_id: "session-b"
    }
  }
}
```

成功响应：

```protobuf
ServerEnvelope {
  send_message_response: SendMessageResponse {
    request_id: 43
    transient_accepted: {
      packet_id: 77
      source_node_id: 4096
      target_node_id: 8192
      recipient: { node_id: 8192, user_id: 1025 }
      delivery_mode: CLIENT_DELIVERY_MODE_ROUTE_RETRY
      target_session: {
        serving_node_id: 8192
        session_id: "session-b"
      }
    }
  }
}
```

`transient_accepted` 的语义是“当前节点已经接受了这次瞬时路由请求”，不是最终送达确认。

## 查询与管理 RPC

### 实际存在的 proto 消息

当前 `client.proto` 中，除了 `send_message` / `ping` / `ack_message` 外，还支持：

- `create_user`
- `get_user`
- `update_user`
- `delete_user`
- `list_messages`
- `upsert_user_attachment`
- `delete_user_attachment`
- `list_user_attachments`
- `list_events`
- `operations_status`
- `metrics`
- `list_cluster_nodes`
- `list_node_logged_in_users`
- `resolve_user_sessions`

### 概念 API 与 attachment RPC 的映射

Go SDK 为了更易用，额外提供了语义化方法：

- `SubscribeChannel` / `UnsubscribeChannel` / `ListSubscriptions`
- `BlockUser` / `UnblockUser` / `ListBlockedUsers`

但在线上协议里，它们统一映射到 attachment RPC：

- channel 订阅 -> `ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION`
- 用户黑名单 -> `ATTACHMENT_TYPE_USER_BLACKLIST`

也就是说，协议文档里不要再写旧的：

- `list_subscriptions`
- `block_user`
- `unblock_user`

这些名称在当前 `turntf-go/proto/client.proto` 里并不存在。

### 例子：创建订阅

```protobuf
ClientEnvelope {
  upsert_user_attachment: UpsertUserAttachmentRequest {
    request_id: 1001
    owner: { node_id: 4096, user_id: 1025 }
    subject: { node_id: 4096, user_id: 1026 }
    attachment_type: ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION
    config_json: "{}"
  }
}
```

### 例子：拉黑用户

```protobuf
ClientEnvelope {
  upsert_user_attachment: UpsertUserAttachmentRequest {
    request_id: 1002
    owner: { node_id: 4096, user_id: 1025 }
    subject: { node_id: 8192, user_id: 2025 }
    attachment_type: ATTACHMENT_TYPE_USER_BLACKLIST
    config_json: "{}"
  }
}
```

### 例子：解析在线 session

```protobuf
ClientEnvelope {
  resolve_user_sessions: ResolveUserSessionsRequest {
    request_id: 1003
    user: { node_id: 8192, user_id: 1025 }
  }
}
```

```protobuf
ServerEnvelope {
  resolve_user_sessions_response: ResolveUserSessionsResponse {
    request_id: 1003
    user: { node_id: 8192, user_id: 1025 }
    presence: [
      { serving_node_id: 8192, session_count: 2, transport_hint: "realtime" }
    ]
    items: [
      {
        session: { serving_node_id: 8192, session_id: "session-b" }
        transport: "realtime"
        transient_capable: true
      }
    ]
    count: 1
  }
}
```

## `/ws/realtime` 的能力边界

`/ws/realtime` 的设计目标是“只保留在线态与瞬时流量相关能力”，因此它和 `/ws/client` 不同。

允许：

- `send_message` 的 transient 形式
- `list_cluster_nodes`
- `list_node_logged_in_users`
- `resolve_user_sessions`
- `ping`
- `ack_message`

拒绝：

- 持久化 `send_message`
- `create_user`
- `get_user`
- `update_user`
- `delete_user`
- `list_messages`
- `upsert_user_attachment`
- `delete_user_attachment`
- `list_user_attachments`
- `list_events`
- `operations_status`
- `metrics`

如果你只是把 `LoginRequest.transient_only=true` 发到 `/ws/client`，并不会触发这些 RPC 限制；这和 `/ws/realtime` 是两套不同边界。

## Ping / Pong

```protobuf
ClientEnvelope {
  ping: Ping { request_id: 7 }
}
```

```protobuf
ServerEnvelope {
  pong: Pong { request_id: 7 }
}
```

`turntf-go` 会按 `Config.PingInterval` 自动发送应用层 ping。

## 常见错误码

统一格式：

```protobuf
ServerEnvelope {
  error: Error {
    code: "invalid_request"
    message: "target is required"
    request_id: 42
  }
}
```

常见 `code`：

- `unauthorized`：登录失败、首帧不是登录、登录解码失败
- `invalid_frame`：发送了非 binary frame
- `invalid_protobuf`：binary frame 不是合法 `ClientEnvelope`
- `invalid_message`：发送了不支持的客户端消息
- `already_authenticated`：登录成功后又发了一次 `login`
- `invalid_request`：字段缺失、组合非法，或 `/ws/realtime` 上调用了不允许的 RPC
- `forbidden`：权限不足
- `not_found`：资源不存在
- `conflict`：资源状态冲突
- `service_unavailable`：当前节点暂不可写
- `internal_error`：服务端内部错误

请求级错误通常会带 `request_id`，高层 SDK 会把它转换成 `*turntf.ServerError` 直接返回给调用者。

## Go SDK 对应关系

协议层和高层 SDK 的常见映射如下：

| 协议 | Go SDK | 说明 |
| --- | --- | --- |
| `login` | `Client.Connect()` | SDK 负责发送首帧 |
| `message_pushed` | `Handler.OnMessage()` | SDK 先做持久化和 ack |
| `packet_pushed` | `Handler.OnPacket()` | SDK 不做游标处理 |
| `send_message` | `SendMessage()` / `SendPacket()` | 持久化与瞬时包共用一个 proto |
| `resolve_user_sessions` | `ResolveUserSessions()` | 定向 packet 前通常先调用 |
| `upsert_user_attachment` | `SubscribeChannel()` / `BlockUser()` / `UpsertAttachment()` | 高层封装了语义名 |

## Proto 生成提醒

本文所有字段和消息名以 `turntf-go/proto/client.proto` 为准。如果你改了 proto：

1. 重新生成 `internal/proto/client.pb.go`
2. 同步检查本文和 [Go SDK 使用总览](sdk-guide.md)
3. 再检查 README 的快速示例是否仍然成立
