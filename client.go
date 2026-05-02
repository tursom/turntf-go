package turntf

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	pb "github.com/tursom/turntf-go/internal/proto"
)

// Logger 是日志记录器接口，用于输出客户端内部日志（如重连、错误信息）。
type Logger interface {
	Printf(format string, args ...any)
}

// Handler 是客户端事件处理器接口，用于接收登录成功、消息推送、数据包推送、错误和断开连接等事件。
type Handler interface {
	OnLogin(context.Context, LoginInfo)
	OnMessage(context.Context, Message)
	OnPacket(context.Context, Packet)
	OnError(context.Context, error)
	OnDisconnect(context.Context, error)
}

// NopHandler 是 Handler 的空实现，所有方法均为空操作。
// 当 Config.Handler 未设置时，客户端默认使用 NopHandler。
type NopHandler struct{}

// OnLogin 是登录成功事件的空处理器。
func (NopHandler) OnLogin(context.Context, LoginInfo) {}
// OnMessage 是消息推送事件的空处理器。
func (NopHandler) OnMessage(context.Context, Message) {}
// OnPacket 是数据包推送事件的空处理器。
func (NopHandler) OnPacket(context.Context, Packet)    {}
// OnError 是错误事件的空处理器。
func (NopHandler) OnError(context.Context, error)      {}
// OnDisconnect 是断开连接事件的空处理器。
func (NopHandler) OnDisconnect(context.Context, error) {}

// Config 是客户端配置，包含连接、认证、重连、事件处理器等所有可选设置。
// 创建客户端后可通过 NewClient 初始化，未设置的字段会使用合理的默认值。
type Config struct {
	// BaseURL 是服务端基础地址，格式如 "http://localhost:8080"，必填。
	BaseURL               string
	// Credentials 是用户登录凭据，必填。
	Credentials           Credentials
	// CursorStore 是消息游标持久化存储，用于消息去重。默认为 NewMemoryCursorStore()。
	CursorStore           CursorStore
	// Handler 是事件处理器，接收登录、消息、错误等事件。默认为 NopHandler。
	Handler               Handler
	// HTTPClient 是 HTTP 客户端实例，用于底层 HTTP 请求。默认为 http.DefaultClient。
	HTTPClient            *http.Client
	// Logger 是日志记录器。为空则不输出日志。
	Logger                Logger
	// Reconnect 是否启用自动重连，默认为 true。
	Reconnect             bool
	// InitialReconnectDelay 首次重连等待时间，默认为 1 秒。
	InitialReconnectDelay time.Duration
	// MaxReconnectDelay 最大重连等待时间（指数退避上限），默认为 30 秒。
	MaxReconnectDelay     time.Duration
	// PingInterval WebSocket ping 间隔，默认为 30 秒。
	PingInterval          time.Duration
	// RequestTimeout RPC 请求超时时间，默认为 10 秒。
	RequestTimeout        time.Duration
	// AckMessages 是否自动确认已收到的消息，默认为 true。
	AckMessages           bool
	// TransientOnly 是否仅接收瞬时消息（不接收持久化消息推送），默认为 false。
	TransientOnly         bool
	// RealtimeStream 是否使用实时流通道（/ws/realtime），默认为 false（使用 /ws/client）。
	RealtimeStream        bool
}

// Client 是 WebSocket 客户端，管理与服务端的长连接、消息收发、自动重连和 RPC 请求。
// 使用 NewClient 创建实例后，通过 Connect 建立连接，通过 Handler 接口接收事件推送。
type Client struct {
	cfg Config

	http *HTTPClient

	ctx    context.Context
	cancel context.CancelFunc

	startOnce sync.Once
	started   atomic.Bool
	done      chan struct{}

	firstConnect chan error
	firstSignal  sync.Once

	writeMu sync.Mutex

	stateMu       sync.RWMutex
	conn          *websocket.Conn
	authenticated bool
	closed        bool
	stopReconnect bool
	loginInfo     LoginInfo

	pendingMu sync.Mutex
	pending   map[uint64]chan requestResult

	requestID atomic.Uint64

	relay     *Relay
	relayOnce sync.Once
}

type requestResult struct {
	value any
	err   error
}

// NewClient 创建并返回一个新的 WebSocket Client 实例。
// cfg 为必填配置，其中 BaseURL 和 Credentials 为必填项。
// 未设置的可选字段（如 CursorStore、Handler、超时等）将使用合理的默认值。
func NewClient(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, fmt.Errorf("base URL is required")
	}
	if err := cfg.Credentials.validate(); err != nil {
		return nil, fmt.Errorf("invalid credentials: %w", err)
	}
	if cfg.CursorStore == nil {
		cfg.CursorStore = NewMemoryCursorStore()
	}
	if cfg.Handler == nil {
		cfg.Handler = NopHandler{}
	}
	if cfg.InitialReconnectDelay <= 0 {
		cfg.InitialReconnectDelay = time.Second
	}
	if cfg.MaxReconnectDelay <= 0 {
		cfg.MaxReconnectDelay = 30 * time.Second
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 10 * time.Second
	}
	if cfg.PingInterval <= 0 {
		cfg.PingInterval = 30 * time.Second
	}
	if !cfg.Reconnect {
		cfg.Reconnect = true
	}
	if !cfg.AckMessages {
		cfg.AckMessages = true
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		cfg:          cfg,
		http:         &HTTPClient{BaseURL: strings.TrimRight(cfg.BaseURL, "/"), HTTPClient: cfg.HTTPClient},
		ctx:          ctx,
		cancel:       cancel,
		done:         make(chan struct{}),
		firstConnect: make(chan error, 1),
		pending:      make(map[uint64]chan requestResult),
	}, nil
}

// HTTP 返回关联的 HTTPClient 实例，用于通过 REST API 执行操作。
func (c *Client) HTTP() *HTTPClient {
	return c.http
}

// CurrentLogin 返回当前登录的用户信息。如果尚未登录或已断开连接，第二个返回值为 false。
func (c *Client) CurrentLogin() (LoginInfo, bool) {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	if !c.authenticated {
		return LoginInfo{}, false
	}
	return c.loginInfo, true
}

// Login 使用明文密码通过 HTTP REST API 登录，通过节点 ID 和用户 ID 标识用户。
// 返回认证 token 用于后续 HTTP 请求的 Bearer 认证。
func (c *Client) Login(ctx context.Context, nodeID, userID int64, password string) (string, error) {
	input, err := PlainPassword(password)
	if err != nil {
		return "", err
	}
	return c.http.LoginWithPassword(ctx, nodeID, userID, input)
}

// LoginWithPassword 通过 HTTP REST API 使用 PasswordInput 密码登录，通过节点 ID 和用户 ID 标识用户。
// password 支持明文和已哈希两种模式。
func (c *Client) LoginWithPassword(ctx context.Context, nodeID, userID int64, password PasswordInput) (string, error) {
	return c.http.LoginWithPassword(ctx, nodeID, userID, password)
}

// LoginByLoginName 通过 HTTP REST API 使用明文密码和登录名登录。
// loginName 为用户的登录名，而非 node_id + user_id 组合。
func (c *Client) LoginByLoginName(ctx context.Context, loginName, password string) (string, error) {
	return c.http.LoginByLoginName(ctx, loginName, password)
}

// LoginByLoginNameWithPassword 通过 HTTP REST API 使用 PasswordInput 密码和登录名登录。
// password 支持明文和已哈希两种模式。
func (c *Client) LoginByLoginNameWithPassword(ctx context.Context, loginName string, password PasswordInput) (string, error) {
	return c.http.LoginByLoginNameWithPassword(ctx, loginName, password)
}

// CreateUser 通过 WebSocket RPC 创建用户或频道。
// token 参数当前未被使用（保留以保持 API 一致），请求通过 WebSocket 连接发送。
func (c *Client) CreateUser(ctx context.Context, token string, req CreateUserRequest) (User, error) {
	_ = token

	var zero User
	if req.Username == "" {
		return zero, fmt.Errorf("username is required")
	}
	if req.Role == "" {
		return zero, fmt.Errorf("role is required")
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_CreateUser{
				CreateUser: &pb.CreateUserRequest{
					RequestId:   requestID,
					Username:    req.Username,
					LoginName:   req.LoginName,
					Password:    req.Password.WireValue(),
					ProfileJson: append([]byte(nil), req.ProfileJSON...),
					Role:        req.Role,
				},
			},
		}
	})
	if err != nil {
		return zero, err
	}

	user, ok := res.value.(User)
	if !ok {
		return zero, &ProtocolError{Message: "missing user in create_user_response"}
	}
	return user, nil
}

// CreateChannel 通过 WebSocket RPC 创建频道。与 CreateUser 类似，但 Role 默认设为 "channel"。
func (c *Client) CreateChannel(ctx context.Context, token string, req CreateUserRequest) (User, error) {
	if req.Role == "" {
		req.Role = "channel"
	}
	return c.CreateUser(ctx, token, req)
}

// CreateSubscription 通过 WebSocket RPC 创建频道订阅关系。订阅者将收到频道的消息推送。
func (c *Client) CreateSubscription(ctx context.Context, token string, userRef, channelRef UserRef) error {
	_, err := c.UpsertAttachment(ctx, userRef, channelRef, AttachmentTypeChannelSubscription, []byte("{}"))
	return err
}

// ListMessages 通过 WebSocket RPC 查询指定用户的消息列表。
// target 指定消息所属用户，limit 控制返回数量上限。
func (c *Client) ListMessages(ctx context.Context, token string, target UserRef, limit int) ([]Message, error) {
	_ = token
	return c.WSListMessages(ctx, target, limit)
}

// PostMessage 通过 WebSocket RPC 向目标用户发送持久化消息。
// body 为消息内容，不能为空。返回已保存的消息详情。
func (c *Client) PostMessage(ctx context.Context, token string, target UserRef, body []byte) (Message, error) {
	_ = token
	return c.SendMessage(ctx, SendMessageInput{
		Target: target,
		Body:   body,
	})
}

// PostPacket 通过 WebSocket RPC 发送瞬时消息（非持久化）。
// relayTarget 为目标用户，mode 为投递模式。targetNodeID 必须与 relayTarget.NodeID 一致。
func (c *Client) PostPacket(ctx context.Context, token string, targetNodeID int64, relayTarget UserRef, body []byte, mode DeliveryMode) error {
	_ = token
	if targetNodeID != 0 && targetNodeID != relayTarget.NodeID {
		return fmt.Errorf("target node ID %d does not match target user node_id %d", targetNodeID, relayTarget.NodeID)
	}
	_, err := c.SendPacket(ctx, SendPacketInput{
		Target:       relayTarget,
		Body:         body,
		DeliveryMode: mode,
	})
	return err
}

// GetUserMetadata 通过 WebSocket RPC 获取指定用户的指定元数据键值。
func (c *Client) GetUserMetadata(ctx context.Context, token string, owner UserRef, key string) (UserMetadata, error) {
	_ = token
	return c.WSGetUserMetadata(ctx, owner, key)
}

// UpsertUserMetadata 通过 WebSocket RPC 创建或更新用户元数据。
func (c *Client) UpsertUserMetadata(ctx context.Context, token string, owner UserRef, key string, req UpsertUserMetadataRequest) (UserMetadata, error) {
	_ = token
	return c.WSUpsertUserMetadata(ctx, owner, key, req)
}

// DeleteUserMetadata 通过 WebSocket RPC 删除用户元数据（软删除）。
func (c *Client) DeleteUserMetadata(ctx context.Context, token string, owner UserRef, key string) (UserMetadata, error) {
	_ = token
	return c.WSDeleteUserMetadata(ctx, owner, key)
}

// ScanUserMetadata 通过 WebSocket RPC 按前缀分页扫描用户元数据。
func (c *Client) ScanUserMetadata(ctx context.Context, token string, owner UserRef, req ScanUserMetadataRequest) (UserMetadataPage, error) {
	_ = token
	return c.WSScanUserMetadata(ctx, owner, req)
}

// Connect 启动 WebSocket 连接并阻塞等待首次连接成功或失败。
// 首次连接成功后，客户端会自动处理重连（如果配置启用）。ctx 可用于超时控制。
func (c *Client) Connect(ctx context.Context) error {
	c.startOnce.Do(func() {
		c.started.Store(true)
		go c.run()
	})

	select {
	case err := <-c.firstConnect:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close 关闭客户端，断开 WebSocket 连接并停止重连。
// 方法会等待所有内部 goroutine 退出后返回。已关闭的客户端可安全重复调用。
func (c *Client) Close() error {
	started := c.started.Load()
	c.stateMu.Lock()
	if c.closed {
		c.stateMu.Unlock()
		if started {
			<-c.done
		}
		return nil
	}
	c.closed = true
	conn := c.conn
	c.conn = nil
	c.authenticated = false
	c.loginInfo = LoginInfo{}
	c.stateMu.Unlock()

	c.cancel()
	if conn != nil {
		conn.Close(websocket.StatusNormalClosure, "client closed")
	}
	if started {
		<-c.done
	}
	return nil
}

// Ping 发送 WebSocket ping 请求给服务端，用于检测连接活性。
// 返回服务端的响应错误（如果有）。
func (c *Client) Ping(ctx context.Context) error {
	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_Ping{
				Ping: &pb.Ping{RequestId: requestID},
			},
		}
	})
	if err != nil {
		return err
	}
	return res.err
}

// SendMessage 通过 WebSocket RPC 发送持久化消息。消息会被存储并按可靠投递机制投递。
// input 包含目标用户和消息体。返回已保存的消息详情。
func (c *Client) SendMessage(ctx context.Context, input SendMessageInput) (Message, error) {
	var zero Message
	if err := input.Target.validate(); err != nil {
		return zero, fmt.Errorf("invalid target: %w", err)
	}
	if len(input.Body) == 0 {
		return zero, fmt.Errorf("body is required")
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_SendMessage{
				SendMessage: &pb.SendMessageRequest{
					RequestId: requestID,
					Target:    userRefToProto(input.Target),
					Body:      append([]byte(nil), input.Body...),
				},
			},
		}
	})
	if err != nil {
		return zero, err
	}
	if res.err != nil {
		return zero, res.err
	}
	msg, ok := res.value.(Message)
	if !ok {
		return zero, &ProtocolError{Message: "missing message in send response"}
	}
	return msg, nil
}

// SendPacket 通过 WebSocket RPC 发送瞬时消息（非持久化）。
// 与 SendMessage 不同，Packet 不会被存储，适合心跳、通知等场景。
// input 包含目标用户、消息体、投递模式和可选的会话定位信息。
func (c *Client) SendPacket(ctx context.Context, input SendPacketInput) (RelayAccepted, error) {
	var zero RelayAccepted
	if err := input.Target.validate(); err != nil {
		return zero, fmt.Errorf("invalid target: %w", err)
	}
	if len(input.Body) == 0 {
		return zero, fmt.Errorf("body is required")
	}
	if err := input.DeliveryMode.validatePacketMode(); err != nil {
		return zero, err
	}
	if !input.TargetSession.IsZero() {
		if err := input.TargetSession.validate(); err != nil {
			return zero, fmt.Errorf("invalid target_session: %w", err)
		}
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_SendMessage{
				SendMessage: &pb.SendMessageRequest{
					RequestId:     requestID,
					Target:        userRefToProto(input.Target),
					Body:          append([]byte(nil), input.Body...),
					DeliveryKind:  pb.ClientDeliveryKind_CLIENT_DELIVERY_KIND_TRANSIENT,
					DeliveryMode:  deliveryModeToProto(input.DeliveryMode),
					TargetSession: sessionRefToProto(input.TargetSession),
				},
			},
		}
	})
	if err != nil {
		return zero, err
	}
	if res.err != nil {
		return zero, res.err
	}
	relay, ok := res.value.(RelayAccepted)
	if !ok {
		return zero, &ProtocolError{Message: "missing transient_accepted in send response"}
	}
	return relay, nil
}

// SendPacketToSession 通过 WebSocket RPC 发送瞬时消息到用户的指定会话。
// 与 SendPacket 类似，但明确指定了目标会话。
func (c *Client) SendPacketToSession(ctx context.Context, target UserRef, targetSession SessionRef, body []byte, mode DeliveryMode) (RelayAccepted, error) {
	return c.SendPacket(ctx, SendPacketInput{
		Target:        target,
		Body:          body,
		DeliveryMode:  mode,
		TargetSession: targetSession,
	})
}

// GetUser 通过 WebSocket RPC 查询指定用户的详细信息。
func (c *Client) GetUser(ctx context.Context, target UserRef) (User, error) {
	var zero User
	if err := target.validate(); err != nil {
		return zero, fmt.Errorf("invalid user: %w", err)
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_GetUser{
				GetUser: &pb.GetUserRequest{
					RequestId: requestID,
					User:      userRefToProto(target),
				},
			},
		}
	})
	if err != nil {
		return zero, err
	}

	user, ok := res.value.(User)
	if !ok {
		return zero, &ProtocolError{Message: "missing user in get_user_response"}
	}
	return user, nil
}

// UpdateUser 通过 WebSocket RPC 更新用户信息。仅传递 req 中非 nil 的字段进行更新。
// 部分字段的修改需要额外权限验证。
func (c *Client) UpdateUser(ctx context.Context, target UserRef, req UpdateUserRequest) (User, error) {
	var zero User
	if err := target.validate(); err != nil {
		return zero, fmt.Errorf("invalid user: %w", err)
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_UpdateUser{
				UpdateUser: &pb.UpdateUserRequest{
					RequestId:   requestID,
					User:        userRefToProto(target),
					Username:    optionalStringField(req.Username),
					LoginName:   optionalStringField(req.LoginName),
					Password:    optionalPasswordField(req.Password),
					ProfileJson: optionalBytesField(req.ProfileJSON),
					Role:        optionalStringField(req.Role),
				},
			},
		}
	})
	if err != nil {
		return zero, err
	}

	user, ok := res.value.(User)
	if !ok {
		return zero, &ProtocolError{Message: "missing user in update_user_response"}
	}
	return user, nil
}

// DeleteUser 通过 WebSocket RPC 删除指定用户。
func (c *Client) DeleteUser(ctx context.Context, target UserRef) (DeleteUserResult, error) {
	var zero DeleteUserResult
	if err := target.validate(); err != nil {
		return zero, fmt.Errorf("invalid user: %w", err)
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_DeleteUser{
				DeleteUser: &pb.DeleteUserRequest{
					RequestId: requestID,
					User:      userRefToProto(target),
				},
			},
		}
	})
	if err != nil {
		return zero, err
	}

	result, ok := res.value.(DeleteUserResult)
	if !ok {
		return zero, &ProtocolError{Message: "missing status in delete_user_response"}
	}
	return result, nil
}

// WSGetUserMetadata 通过 WebSocket RPC 获取指定用户的指定元数据键值。
// 与 GetUserMetadata 功能相同，但直接调用 WebSocket 底层方法。
func (c *Client) WSGetUserMetadata(ctx context.Context, owner UserRef, key string) (UserMetadata, error) {
	var zero UserMetadata
	if err := owner.validate(); err != nil {
		return zero, fmt.Errorf("invalid owner: %w", err)
	}
	if err := validateMetadataKey(key); err != nil {
		return zero, err
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_GetUserMetadata{
				GetUserMetadata: &pb.GetUserMetadataRequest{
					RequestId: requestID,
					Owner:     userRefToProto(owner),
					Key:       key,
				},
			},
		}
	})
	if err != nil {
		return zero, err
	}

	metadata, ok := res.value.(UserMetadata)
	if !ok {
		return zero, &ProtocolError{Message: "missing metadata in get_user_metadata_response"}
	}
	return metadata, nil
}

// WSUpsertUserMetadata 通过 WebSocket RPC 创建或更新用户元数据。
// 与 UpsertUserMetadata 功能相同，但直接调用 WebSocket 底层方法。
func (c *Client) WSUpsertUserMetadata(ctx context.Context, owner UserRef, key string, req UpsertUserMetadataRequest) (UserMetadata, error) {
	var zero UserMetadata
	if err := owner.validate(); err != nil {
		return zero, fmt.Errorf("invalid owner: %w", err)
	}
	if err := validateMetadataKey(key); err != nil {
		return zero, err
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_UpsertUserMetadata{
				UpsertUserMetadata: &pb.UpsertUserMetadataRequest{
					RequestId: requestID,
					Owner:     userRefToProto(owner),
					Key:       key,
					Value:     append([]byte{}, req.Value...),
					ExpiresAt: optionalStringField(req.ExpiresAt),
				},
			},
		}
	})
	if err != nil {
		return zero, err
	}

	metadata, ok := res.value.(UserMetadata)
	if !ok {
		return zero, &ProtocolError{Message: "missing metadata in upsert_user_metadata_response"}
	}
	return metadata, nil
}

// WSDeleteUserMetadata 通过 WebSocket RPC 删除用户元数据（软删除）。
// 与 DeleteUserMetadata 功能相同，但直接调用 WebSocket 底层方法。
func (c *Client) WSDeleteUserMetadata(ctx context.Context, owner UserRef, key string) (UserMetadata, error) {
	var zero UserMetadata
	if err := owner.validate(); err != nil {
		return zero, fmt.Errorf("invalid owner: %w", err)
	}
	if err := validateMetadataKey(key); err != nil {
		return zero, err
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_DeleteUserMetadata{
				DeleteUserMetadata: &pb.DeleteUserMetadataRequest{
					RequestId: requestID,
					Owner:     userRefToProto(owner),
					Key:       key,
				},
			},
		}
	})
	if err != nil {
		return zero, err
	}

	metadata, ok := res.value.(UserMetadata)
	if !ok {
		return zero, &ProtocolError{Message: "missing metadata in delete_user_metadata_response"}
	}
	return metadata, nil
}

// WSScanUserMetadata 通过 WebSocket RPC 按前缀分页扫描用户元数据。
// 与 ScanUserMetadata 功能相同，但直接调用 WebSocket 底层方法。
func (c *Client) WSScanUserMetadata(ctx context.Context, owner UserRef, req ScanUserMetadataRequest) (UserMetadataPage, error) {
	var zero UserMetadataPage
	if err := owner.validate(); err != nil {
		return zero, fmt.Errorf("invalid owner: %w", err)
	}
	if err := req.validate(); err != nil {
		return zero, err
	}

	limit := int32(0)
	if req.Limit > 0 {
		limit = int32(req.Limit)
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_ScanUserMetadata{
				ScanUserMetadata: &pb.ScanUserMetadataRequest{
					RequestId: requestID,
					Owner:     userRefToProto(owner),
					Prefix:    req.Prefix,
					After:     req.After,
					Limit:     limit,
				},
			},
		}
	})
	if err != nil {
		return zero, err
	}

	page, ok := res.value.(UserMetadataPage)
	if !ok {
		return zero, &ProtocolError{Message: "missing page in scan_user_metadata_response"}
	}
	return page, nil
}

// UpsertAttachment 通过 WebSocket RPC 创建或更新用户之间的关联关系（如频道订阅、黑名单等）。
func (c *Client) UpsertAttachment(ctx context.Context, owner, subject UserRef, attachmentType AttachmentType, configJSON []byte) (Attachment, error) {
	var zero Attachment
	if err := owner.validate(); err != nil {
		return zero, fmt.Errorf("invalid owner: %w", err)
	}
	if err := subject.validate(); err != nil {
		return zero, fmt.Errorf("invalid subject: %w", err)
	}
	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_UpsertUserAttachment{
				UpsertUserAttachment: &pb.UpsertUserAttachmentRequest{
					RequestId:      requestID,
					Owner:          userRefToProto(owner),
					Subject:        userRefToProto(subject),
					AttachmentType: attachmentTypeToProto(attachmentType),
					ConfigJson:     append([]byte(nil), configJSON...),
				},
			},
		}
	})
	if err != nil {
		return zero, err
	}
	attachment, ok := res.value.(Attachment)
	if !ok {
		return zero, &ProtocolError{Message: "missing attachment in upsert_user_attachment_response"}
	}
	return attachment, nil
}

// DeleteAttachment 通过 WebSocket RPC 删除用户之间的关联关系。
func (c *Client) DeleteAttachment(ctx context.Context, owner, subject UserRef, attachmentType AttachmentType) (Attachment, error) {
	var zero Attachment
	if err := owner.validate(); err != nil {
		return zero, fmt.Errorf("invalid owner: %w", err)
	}
	if err := subject.validate(); err != nil {
		return zero, fmt.Errorf("invalid subject: %w", err)
	}
	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_DeleteUserAttachment{
				DeleteUserAttachment: &pb.DeleteUserAttachmentRequest{
					RequestId:      requestID,
					Owner:          userRefToProto(owner),
					Subject:        userRefToProto(subject),
					AttachmentType: attachmentTypeToProto(attachmentType),
				},
			},
		}
	})
	if err != nil {
		return zero, err
	}
	attachment, ok := res.value.(Attachment)
	if !ok {
		return zero, &ProtocolError{Message: "missing attachment in delete_user_attachment_response"}
	}
	return attachment, nil
}

// ListAttachments 通过 WebSocket RPC 查询用户指定类型的所有关联关系列表。
func (c *Client) ListAttachments(ctx context.Context, owner UserRef, attachmentType AttachmentType) ([]Attachment, error) {
	if err := owner.validate(); err != nil {
		return nil, fmt.Errorf("invalid owner: %w", err)
	}
	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_ListUserAttachments{
				ListUserAttachments: &pb.ListUserAttachmentsRequest{
					RequestId:      requestID,
					Owner:          userRefToProto(owner),
					AttachmentType: attachmentTypeToProto(attachmentType),
				},
			},
		}
	})
	if err != nil {
		return nil, err
	}
	items, ok := res.value.([]Attachment)
	if !ok {
		return nil, &ProtocolError{Message: "missing items in list_user_attachments_response"}
	}
	return items, nil
}

// SubscribeChannel 通过 WebSocket RPC 订阅频道。订阅者将收到频道的消息推送。
func (c *Client) SubscribeChannel(ctx context.Context, token string, subscriber, channel UserRef) (Subscription, error) {
	_ = token
	attachment, err := c.UpsertAttachment(ctx, subscriber, channel, AttachmentTypeChannelSubscription, []byte("{}"))
	if err != nil {
		return Subscription{}, err
	}
	return subscriptionFromProto(&pb.Attachment{
		Owner:        userRefToProto(attachment.Owner),
		Subject:      userRefToProto(attachment.Subject),
		AttachedAt:   attachment.AttachedAt,
		DeletedAt:    attachment.DeletedAt,
		OriginNodeId: attachment.OriginNodeID,
	}), nil
}

// UnsubscribeChannel 通过 WebSocket RPC 取消频道订阅。
func (c *Client) UnsubscribeChannel(ctx context.Context, subscriber, channel UserRef) (Subscription, error) {
	attachment, err := c.DeleteAttachment(ctx, subscriber, channel, AttachmentTypeChannelSubscription)
	if err != nil {
		return Subscription{}, err
	}
	return subscriptionFromProto(&pb.Attachment{
		Owner:        userRefToProto(attachment.Owner),
		Subject:      userRefToProto(attachment.Subject),
		AttachedAt:   attachment.AttachedAt,
		DeletedAt:    attachment.DeletedAt,
		OriginNodeId: attachment.OriginNodeID,
	}), nil
}

// ListSubscriptions 通过 WebSocket RPC 查询指定用户的所有频道订阅。
func (c *Client) ListSubscriptions(ctx context.Context, subscriber UserRef) ([]Subscription, error) {
	attachments, err := c.ListAttachments(ctx, subscriber, AttachmentTypeChannelSubscription)
	if err != nil {
		return nil, err
	}
	items := make([]Subscription, 0, len(attachments))
	for _, attachment := range attachments {
		items = append(items, Subscription{
			Subscriber:   attachment.Owner,
			Channel:      attachment.Subject,
			SubscribedAt: attachment.AttachedAt,
			DeletedAt:    attachment.DeletedAt,
			OriginNodeID: attachment.OriginNodeID,
		})
	}
	return items, nil
}

// BlockUser 通过 WebSocket RPC 将指定用户加入黑名单。被拉黑的用户无法向 owner 发送消息。
func (c *Client) BlockUser(ctx context.Context, token string, owner, blocked UserRef) (BlacklistEntry, error) {
	_ = token
	attachment, err := c.UpsertAttachment(ctx, owner, blocked, AttachmentTypeUserBlacklist, []byte("{}"))
	if err != nil {
		return BlacklistEntry{}, err
	}
	return blacklistEntryFromProto(&pb.Attachment{
		Owner:        userRefToProto(attachment.Owner),
		Subject:      userRefToProto(attachment.Subject),
		AttachedAt:   attachment.AttachedAt,
		DeletedAt:    attachment.DeletedAt,
		OriginNodeId: attachment.OriginNodeID,
	}), nil
}

// UnblockUser 通过 WebSocket RPC 将指定用户移出黑名单。
func (c *Client) UnblockUser(ctx context.Context, token string, owner, blocked UserRef) (BlacklistEntry, error) {
	_ = token
	attachment, err := c.DeleteAttachment(ctx, owner, blocked, AttachmentTypeUserBlacklist)
	if err != nil {
		return BlacklistEntry{}, err
	}
	return blacklistEntryFromProto(&pb.Attachment{
		Owner:        userRefToProto(attachment.Owner),
		Subject:      userRefToProto(attachment.Subject),
		AttachedAt:   attachment.AttachedAt,
		DeletedAt:    attachment.DeletedAt,
		OriginNodeId: attachment.OriginNodeID,
	}), nil
}

// ListBlockedUsers 通过 WebSocket RPC 查询指定用户的黑名单列表。
func (c *Client) ListBlockedUsers(ctx context.Context, token string, owner UserRef) ([]BlacklistEntry, error) {
	_ = token
	attachments, err := c.ListAttachments(ctx, owner, AttachmentTypeUserBlacklist)
	if err != nil {
		return nil, err
	}
	items := make([]BlacklistEntry, 0, len(attachments))
	for _, attachment := range attachments {
		items = append(items, BlacklistEntry{
			Owner:        attachment.Owner,
			Blocked:      attachment.Subject,
			BlockedAt:    attachment.AttachedAt,
			DeletedAt:    attachment.DeletedAt,
			OriginNodeID: attachment.OriginNodeID,
		})
	}
	return items, nil
}

// WSListMessages 通过 WebSocket RPC 查询指定用户的消息列表。
// 与 ListMessages 功能相同，但直接调用 WebSocket 底层方法。
func (c *Client) WSListMessages(ctx context.Context, target UserRef, limit int) ([]Message, error) {
	if err := target.validate(); err != nil {
		return nil, fmt.Errorf("invalid target: %w", err)
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_ListMessages{
				ListMessages: &pb.ListMessagesRequest{
					RequestId: requestID,
					User:      userRefToProto(target),
					Limit:     int32(limit),
				},
			},
		}
	})
	if err != nil {
		return nil, err
	}

	items, ok := res.value.([]Message)
	if !ok {
		return nil, &ProtocolError{Message: "missing items in list_messages_response"}
	}
	return items, nil
}

// ListEvents 通过 WebSocket RPC 查询事件日志，从指定序列号之后开始拉取。
// after 为起始事件序列号（不包含），limit 控制返回数量上限。
func (c *Client) ListEvents(ctx context.Context, after int64, limit int) ([]Event, error) {
	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_ListEvents{
				ListEvents: &pb.ListEventsRequest{
					RequestId: requestID,
					After:     after,
					Limit:     int32(limit),
				},
			},
		}
	})
	if err != nil {
		return nil, err
	}

	items, ok := res.value.([]Event)
	if !ok {
		return nil, &ProtocolError{Message: "missing items in list_events_response"}
	}
	return items, nil
}

// ListClusterNodes 通过 WebSocket RPC 查询集群中的所有节点列表。
func (c *Client) ListClusterNodes(ctx context.Context) ([]ClusterNode, error) {
	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_ListClusterNodes{
				ListClusterNodes: &pb.ListClusterNodesRequest{RequestId: requestID},
			},
		}
	})
	if err != nil {
		return nil, err
	}

	items, ok := res.value.([]ClusterNode)
	if !ok {
		return nil, &ProtocolError{Message: "missing items in list_cluster_nodes_response"}
	}
	return items, nil
}

// ListNodeLoggedInUsers 通过 WebSocket RPC 查询指定节点上当前已登录的用户列表。
func (c *Client) ListNodeLoggedInUsers(ctx context.Context, nodeID int64) ([]LoggedInUser, error) {
	if nodeID == 0 {
		return nil, fmt.Errorf("node_id is required")
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_ListNodeLoggedInUsers{
				ListNodeLoggedInUsers: &pb.ListNodeLoggedInUsersRequest{
					RequestId: requestID,
					NodeId:    nodeID,
				},
			},
		}
	})
	if err != nil {
		return nil, err
	}

	items, ok := res.value.([]LoggedInUser)
	if !ok {
		return nil, &ProtocolError{Message: "missing items in list_node_logged_in_users_response"}
	}
	return items, nil
}

// ResolveUserSessions 通过 WebSocket RPC 查询用户的所有在线节点存在性和会话列表。
func (c *Client) ResolveUserSessions(ctx context.Context, user UserRef) (ResolvedUserSessions, error) {
	var zero ResolvedUserSessions
	if err := user.validate(); err != nil {
		return zero, fmt.Errorf("invalid user: %w", err)
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_ResolveUserSessions{
				ResolveUserSessions: &pb.ResolveUserSessionsRequest{
					RequestId: requestID,
					User:      userRefToProto(user),
				},
			},
		}
	})
	if err != nil {
		return zero, err
	}

	sessions, ok := res.value.(ResolvedUserSessions)
	if !ok {
		return zero, &ProtocolError{Message: "missing sessions in resolve_user_sessions_response"}
	}
	return sessions, nil
}

// OperationsStatus 通过 WebSocket RPC 查询服务节点的运维状态，包括消息窗口、事件序列、写入门控、冲突统计等。
func (c *Client) OperationsStatus(ctx context.Context) (OperationsStatus, error) {
	var zero OperationsStatus
	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_OperationsStatus{
				OperationsStatus: &pb.OperationsStatusRequest{RequestId: requestID},
			},
		}
	})
	if err != nil {
		return zero, err
	}

	status, ok := res.value.(OperationsStatus)
	if !ok {
		return zero, &ProtocolError{Message: "missing status in operations_status_response"}
	}
	return status, nil
}

// Metrics 通过 WebSocket RPC 查询服务端 Prometheus 格式的监控指标。
func (c *Client) Metrics(ctx context.Context) (string, error) {
	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_Metrics{
				Metrics: &pb.MetricsRequest{RequestId: requestID},
			},
		}
	})
	if err != nil {
		return "", err
	}

	text, ok := res.value.(string)
	if !ok {
		return "", &ProtocolError{Message: "missing text in metrics_response"}
	}
	return text, nil
}

func (c *Client) run() {
	defer close(c.done)

	delay := c.cfg.InitialReconnectDelay

	for {
		err := c.connectAndServe()
		if err == nil {
			delay = c.cfg.InitialReconnectDelay
			if c.isClosed() {
				return
			}
			continue
		}
		if c.isClosed() || !c.shouldRetry(err) {
			c.signalFirstConnect(err)
			c.failAllPending(err)
			return
		}

		c.cfg.Handler.OnError(c.ctx, err)
		if c.cfg.Logger != nil {
			c.cfg.Logger.Printf("turntf reconnecting after error: %v", err)
		}

		timer := time.NewTimer(delay)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			c.failAllPending(ErrClosed)
			return
		case <-timer.C:
		}
		delay *= 2
		if delay > c.cfg.MaxReconnectDelay {
			delay = c.cfg.MaxReconnectDelay
		}
	}
}

func (c *Client) connectAndServe() error {
	if c.isClosed() {
		return ErrClosed
	}
	seen, err := c.cfg.CursorStore.LoadSeenMessages(c.ctx)
	if err != nil {
		return err
	}

	conn, err := c.dial(c.ctx)
	if err != nil {
		return err
	}

	loginUser, loginName := c.cfg.Credentials.loginSelector()
	loginEnv := &pb.ClientEnvelope{
		Body: &pb.ClientEnvelope_Login{
			Login: &pb.LoginRequest{
				User:          loginUser,
				LoginName:     loginName,
				Password:      c.cfg.Credentials.Password.WireValue(),
				SeenMessages:  make([]*pb.MessageCursor, 0, len(seen)),
				TransientOnly: c.cfg.TransientOnly,
			},
		},
	}
	for _, cursor := range seen {
		loginEnv.GetLogin().SeenMessages = append(loginEnv.GetLogin().SeenMessages, cursorToProto(cursor))
	}
	if err := c.writeProto(c.ctx, conn, loginEnv); err != nil {
		conn.Close(websocket.StatusInternalError, "login write failed")
		return err
	}

	serverEnv, err := c.readProto(c.ctx, conn)
	if err != nil {
		conn.Close(websocket.StatusInternalError, "login read failed")
		return err
	}

	loginResp, err := c.expectLogin(serverEnv)
	if err != nil {
		conn.Close(websocket.StatusPolicyViolation, "login failed")
		return err
	}

	c.stateMu.Lock()
	c.conn = conn
	c.authenticated = true
	c.loginInfo = loginResp
	c.stateMu.Unlock()

	c.cfg.Handler.OnLogin(c.ctx, loginResp)
	c.signalFirstConnect(nil)

	pingCtx, pingCancel := context.WithCancel(c.ctx)
	defer pingCancel()
	go c.pingLoop(pingCtx)

	readErr := c.readLoop(conn)

	c.stateMu.Lock()
	if c.conn == conn {
		c.conn = nil
	}
	c.authenticated = false
	c.loginInfo = LoginInfo{}
	c.stateMu.Unlock()

	c.failAllPending(ErrDisconnected)
	c.cfg.Handler.OnDisconnect(c.ctx, readErr)
	_ = conn.Close(websocket.StatusNormalClosure, "disconnect")
	return readErr
}

func (c *Client) dial(ctx context.Context) (*websocket.Conn, error) {
	wsURL, err := websocketURL(c.cfg.BaseURL, c.cfg.RealtimeStream)
	if err != nil {
		return nil, err
	}
	conn, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		HTTPClient: c.cfg.HTTPClient,
	})
	if err != nil {
		if resp != nil {
			return nil, &ConnectionError{Op: "dial", Err: fmt.Errorf("status=%d: %w", resp.StatusCode, err)}
		}
		return nil, &ConnectionError{Op: "dial", Err: err}
	}
	return conn, nil
}

func (c *Client) expectLogin(env *pb.ServerEnvelope) (LoginInfo, error) {
	switch body := env.Body.(type) {
	case *pb.ServerEnvelope_LoginResponse:
		return LoginInfo{
			User:            userFromProto(body.LoginResponse.User),
			ProtocolVersion: body.LoginResponse.ProtocolVersion,
			SessionRef:      sessionRefFromProto(body.LoginResponse.SessionRef),
		}, nil
	case *pb.ServerEnvelope_Error:
		c.stateMu.Lock()
		c.stopReconnect = body.Error.Code == "unauthorized"
		c.stateMu.Unlock()
		return LoginInfo{}, &ServerError{
			Code:      body.Error.Code,
			Message:   body.Error.Message,
			RequestID: body.Error.RequestId,
		}
	default:
		return LoginInfo{}, &ProtocolError{Message: "expected login_response or error"}
	}
}

func (c *Client) readLoop(conn *websocket.Conn) error {
	for {
		env, err := c.readProto(c.ctx, conn)
		if err != nil {
			if errors.Is(err, context.Canceled) || websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				return ErrClosed
			}
			return err
		}
		if err := c.handleServerEnvelope(env); err != nil {
			c.cfg.Handler.OnError(c.ctx, err)
		}
	}
}

func (c *Client) handleServerEnvelope(env *pb.ServerEnvelope) error {
	switch body := env.Body.(type) {
	case *pb.ServerEnvelope_MessagePushed:
		msg := messageFromProto(body.MessagePushed.Message)
		if err := c.persistMessage(c.ctx, msg); err != nil {
			return err
		}
		if c.cfg.AckMessages {
			cursor := msg.Cursor()
			if err := c.sendEnvelope(c.ctx, &pb.ClientEnvelope{
				Body: &pb.ClientEnvelope_AckMessage{
					AckMessage: &pb.AckMessage{Cursor: cursorToProto(cursor)},
				},
			}); err != nil && !errors.Is(err, ErrNotConnected) && !errors.Is(err, ErrClosed) {
				c.cfg.Handler.OnError(c.ctx, err)
			}
		}
		c.cfg.Handler.OnMessage(c.ctx, msg)
	case *pb.ServerEnvelope_PacketPushed:
		pkt := packetFromProto(body.PacketPushed.Packet)
			if c.relay != nil && c.relay.handlePacket(pkt) {
				return nil
			}
			c.cfg.Handler.OnPacket(c.ctx, pkt)
	case *pb.ServerEnvelope_SendMessageResponse:
		res := requestResult{}
		switch inner := body.SendMessageResponse.Body.(type) {
		case *pb.SendMessageResponse_Message:
			msg := messageFromProto(inner.Message)
			if err := c.persistMessage(c.ctx, msg); err != nil {
				res.err = err
				break
			}
			res.value = msg
		case *pb.SendMessageResponse_TransientAccepted:
			res.value = relayAcceptedFromProto(inner.TransientAccepted)
		default:
			res.err = &ProtocolError{Message: "empty send_message_response"}
		}
		c.resolvePending(body.SendMessageResponse.RequestId, res)
	case *pb.ServerEnvelope_Pong:
		c.resolvePending(body.Pong.RequestId, requestResult{value: struct{}{}})
	case *pb.ServerEnvelope_CreateUserResponse:
		c.resolvePending(body.CreateUserResponse.RequestId, requestResult{value: userFromProto(body.CreateUserResponse.User)})
	case *pb.ServerEnvelope_GetUserResponse:
		c.resolvePending(body.GetUserResponse.RequestId, requestResult{value: userFromProto(body.GetUserResponse.User)})
	case *pb.ServerEnvelope_GetUserMetadataResponse:
		c.resolvePending(body.GetUserMetadataResponse.RequestId, requestResult{value: userMetadataFromProto(body.GetUserMetadataResponse.Metadata)})
	case *pb.ServerEnvelope_UpsertUserMetadataResponse:
		c.resolvePending(body.UpsertUserMetadataResponse.RequestId, requestResult{value: userMetadataFromProto(body.UpsertUserMetadataResponse.Metadata)})
	case *pb.ServerEnvelope_DeleteUserMetadataResponse:
		c.resolvePending(body.DeleteUserMetadataResponse.RequestId, requestResult{value: userMetadataFromProto(body.DeleteUserMetadataResponse.Metadata)})
	case *pb.ServerEnvelope_ScanUserMetadataResponse:
		c.resolvePending(body.ScanUserMetadataResponse.RequestId, requestResult{value: userMetadataPageFromProto(body.ScanUserMetadataResponse)})
	case *pb.ServerEnvelope_UpdateUserResponse:
		c.resolvePending(body.UpdateUserResponse.RequestId, requestResult{value: userFromProto(body.UpdateUserResponse.User)})
	case *pb.ServerEnvelope_DeleteUserResponse:
		c.resolvePending(body.DeleteUserResponse.RequestId, requestResult{value: DeleteUserResult{
			Status: body.DeleteUserResponse.Status,
			User:   userRefFromProto(body.DeleteUserResponse.User),
		}})
	case *pb.ServerEnvelope_ListMessagesResponse:
		c.resolvePending(body.ListMessagesResponse.RequestId, requestResult{value: messagesFromProto(body.ListMessagesResponse.Items)})
	case *pb.ServerEnvelope_UpsertUserAttachmentResponse:
		c.resolvePending(body.UpsertUserAttachmentResponse.RequestId, requestResult{value: attachmentFromProto(body.UpsertUserAttachmentResponse.Attachment)})
	case *pb.ServerEnvelope_DeleteUserAttachmentResponse:
		c.resolvePending(body.DeleteUserAttachmentResponse.RequestId, requestResult{value: attachmentFromProto(body.DeleteUserAttachmentResponse.Attachment)})
	case *pb.ServerEnvelope_ListUserAttachmentsResponse:
		c.resolvePending(body.ListUserAttachmentsResponse.RequestId, requestResult{value: attachmentsFromProto(body.ListUserAttachmentsResponse.Items)})
	case *pb.ServerEnvelope_ListEventsResponse:
		c.resolvePending(body.ListEventsResponse.RequestId, requestResult{value: eventsFromProto(body.ListEventsResponse.Items)})
	case *pb.ServerEnvelope_ListClusterNodesResponse:
		c.resolvePending(body.ListClusterNodesResponse.RequestId, requestResult{value: clusterNodesFromProto(body.ListClusterNodesResponse.Items)})
	case *pb.ServerEnvelope_ListNodeLoggedInUsersResponse:
		c.resolvePending(body.ListNodeLoggedInUsersResponse.RequestId, requestResult{value: loggedInUsersFromProto(body.ListNodeLoggedInUsersResponse.Items)})
	case *pb.ServerEnvelope_ResolveUserSessionsResponse:
		c.resolvePending(body.ResolveUserSessionsResponse.RequestId, requestResult{value: resolvedUserSessionsFromProto(body.ResolveUserSessionsResponse)})
	case *pb.ServerEnvelope_OperationsStatusResponse:
		c.resolvePending(body.OperationsStatusResponse.RequestId, requestResult{value: operationsStatusFromProto(body.OperationsStatusResponse.Status)})
	case *pb.ServerEnvelope_MetricsResponse:
		c.resolvePending(body.MetricsResponse.RequestId, requestResult{value: body.MetricsResponse.Text})
	case *pb.ServerEnvelope_Error:
		serverErr := &ServerError{
			Code:      body.Error.Code,
			Message:   body.Error.Message,
			RequestID: body.Error.RequestId,
		}
		if body.Error.RequestId != 0 {
			c.resolvePending(body.Error.RequestId, requestResult{err: serverErr})
		} else {
			return serverErr
		}
	case *pb.ServerEnvelope_LoginResponse:
		return &ProtocolError{Message: "unexpected login_response after authentication"}
	default:
		return &ProtocolError{Message: "unsupported server envelope"}
	}
	return nil
}

func (c *Client) sendEnvelope(ctx context.Context, env *pb.ClientEnvelope) error {
	c.stateMu.RLock()
	conn := c.conn
	closed := c.closed
	c.stateMu.RUnlock()

	if closed {
		return ErrClosed
	}
	if conn == nil {
		return ErrNotConnected
	}
	return c.writeProto(ctx, conn, env)
}

func (c *Client) writeProto(ctx context.Context, conn *websocket.Conn, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return conn.Write(ctx, websocket.MessageBinary, payload)
}

func (c *Client) readProto(ctx context.Context, conn *websocket.Conn) (*pb.ServerEnvelope, error) {
	_, payload, err := conn.Read(ctx)
	if err != nil {
		return nil, &ConnectionError{Op: "read", Err: err}
	}
	var env pb.ServerEnvelope
	if err := proto.Unmarshal(payload, &env); err != nil {
		return nil, &ProtocolError{Message: "invalid protobuf frame"}
	}
	return &env, nil
}

func (c *Client) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
			err := c.Ping(pingCtx)
			cancel()
			if err != nil && !errors.Is(err, ErrNotConnected) && !errors.Is(err, ErrClosed) && !errors.Is(err, ErrDisconnected) {
				c.cfg.Handler.OnError(c.ctx, err)
			}
		}
	}
}

func (c *Client) nextRequestID() uint64 {
	return c.requestID.Add(1)
}

func (c *Client) persistMessage(ctx context.Context, msg Message) error {
	if err := c.cfg.CursorStore.SaveMessage(ctx, msg); err != nil {
		return err
	}
	return c.cfg.CursorStore.SaveCursor(ctx, msg.Cursor())
}

func (c *Client) registerPending(requestID uint64) (chan requestResult, error) {
	if c.isClosed() {
		return nil, ErrClosed
	}
	resultCh := make(chan requestResult, 1)
	c.pendingMu.Lock()
	c.pending[requestID] = resultCh
	c.pendingMu.Unlock()
	return resultCh, nil
}

func (c *Client) rpc(ctx context.Context, build func(uint64) *pb.ClientEnvelope) (requestResult, error) {
	requestID := c.nextRequestID()
	resultCh, err := c.registerPending(requestID)
	if err != nil {
		return requestResult{}, err
	}
	defer c.unregisterPending(requestID)

	if err := c.sendEnvelope(ctx, build(requestID)); err != nil {
		return requestResult{}, err
	}
	return waitRequest(ctx, resultCh)
}

func (c *Client) unregisterPending(requestID uint64) {
	c.pendingMu.Lock()
	delete(c.pending, requestID)
	c.pendingMu.Unlock()
}

func (c *Client) resolvePending(requestID uint64, result requestResult) {
	c.pendingMu.Lock()
	ch, ok := c.pending[requestID]
	c.pendingMu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- result:
	default:
	}
}

func (c *Client) failAllPending(err error) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for _, ch := range c.pending {
		select {
		case ch <- requestResult{err: err}:
		default:
		}
	}
}

func (c *Client) shouldRetry(err error) bool {
	c.stateMu.RLock()
	stopReconnect := c.stopReconnect
	closed := c.closed
	c.stateMu.RUnlock()

	if closed {
		return false
	}
	if stopReconnect {
		return false
	}
	if !c.cfg.Reconnect {
		return false
	}
	var serverErr *ServerError
	if errors.As(err, &serverErr) && serverErr.Unauthorized() {
		return false
	}
	return !errors.Is(err, ErrClosed)
}

func (c *Client) isClosed() bool {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.closed
}

func (c *Client) signalFirstConnect(err error) {
	c.firstSignal.Do(func() {
		c.firstConnect <- err
	})
}

func websocketURL(base string, realtime bool) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("unsupported base URL scheme %q", u.Scheme)
	}
	path := "/ws/client"
	if realtime {
		path = "/ws/realtime"
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = path
	} else {
		u.Path = strings.TrimRight(u.Path, "/") + path
	}
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

func waitRequest(ctx context.Context, ch <-chan requestResult) (requestResult, error) {
	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return requestResult{}, ctx.Err()
	}
}
