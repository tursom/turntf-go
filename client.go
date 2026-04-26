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

type Logger interface {
	Printf(format string, args ...any)
}

type Handler interface {
	OnLogin(context.Context, LoginInfo)
	OnMessage(context.Context, Message)
	OnPacket(context.Context, Packet)
	OnError(context.Context, error)
	OnDisconnect(context.Context, error)
}

type NopHandler struct{}

func (NopHandler) OnLogin(context.Context, LoginInfo)  {}
func (NopHandler) OnMessage(context.Context, Message)  {}
func (NopHandler) OnPacket(context.Context, Packet)    {}
func (NopHandler) OnError(context.Context, error)      {}
func (NopHandler) OnDisconnect(context.Context, error) {}

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

	pendingMu sync.Mutex
	pending   map[uint64]chan requestResult

	requestID atomic.Uint64
}

type requestResult struct {
	value any
	err   error
}

func NewClient(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, fmt.Errorf("base URL is required")
	}
	if cfg.Credentials.NodeID == 0 || cfg.Credentials.UserID == 0 {
		return nil, fmt.Errorf("credentials are required")
	}
	if err := cfg.Credentials.Password.Validate(); err != nil {
		return nil, fmt.Errorf("invalid credentials password: %w", err)
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

func (c *Client) HTTP() *HTTPClient {
	return c.http
}

func (c *Client) Login(ctx context.Context, nodeID, userID int64, password string) (string, error) {
	input, err := PlainPassword(password)
	if err != nil {
		return "", err
	}
	return c.http.LoginWithPassword(ctx, nodeID, userID, input)
}

func (c *Client) LoginWithPassword(ctx context.Context, nodeID, userID int64, password PasswordInput) (string, error) {
	return c.http.LoginWithPassword(ctx, nodeID, userID, password)
}

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

func (c *Client) CreateChannel(ctx context.Context, token string, req CreateUserRequest) (User, error) {
	if req.Role == "" {
		req.Role = "channel"
	}
	return c.CreateUser(ctx, token, req)
}

func (c *Client) CreateSubscription(ctx context.Context, token string, userRef, channelRef UserRef) error {
	_, err := c.SubscribeChannel(ctx, token, userRef, channelRef)
	return err
}

func (c *Client) ListMessages(ctx context.Context, token string, target UserRef, limit int) ([]Message, error) {
	_ = token
	return c.WSListMessages(ctx, target, limit)
}

func (c *Client) PostMessage(ctx context.Context, token string, target UserRef, body []byte) (Message, error) {
	_ = token
	return c.SendMessage(ctx, SendMessageInput{
		Target: target,
		Body:   body,
	})
}

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

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_SendMessage{
				SendMessage: &pb.SendMessageRequest{
					RequestId:    requestID,
					Target:       userRefToProto(input.Target),
					Body:         append([]byte(nil), input.Body...),
					DeliveryKind: pb.ClientDeliveryKind_CLIENT_DELIVERY_KIND_TRANSIENT,
					DeliveryMode: deliveryModeToProto(input.DeliveryMode),
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

func (c *Client) SubscribeChannel(ctx context.Context, token string, subscriber, channel UserRef) (Subscription, error) {
	_ = token
	var zero Subscription
	if err := subscriber.validate(); err != nil {
		return zero, fmt.Errorf("invalid subscriber: %w", err)
	}
	if err := channel.validate(); err != nil {
		return zero, fmt.Errorf("invalid channel: %w", err)
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_SubscribeChannel{
				SubscribeChannel: &pb.SubscribeChannelRequest{
					RequestId:  requestID,
					Subscriber: userRefToProto(subscriber),
					Channel:    userRefToProto(channel),
				},
			},
		}
	})
	if err != nil {
		return zero, err
	}

	sub, ok := res.value.(Subscription)
	if !ok {
		return zero, &ProtocolError{Message: "missing subscription in subscribe_channel_response"}
	}
	return sub, nil
}

func (c *Client) UnsubscribeChannel(ctx context.Context, subscriber, channel UserRef) (Subscription, error) {
	var zero Subscription
	if err := subscriber.validate(); err != nil {
		return zero, fmt.Errorf("invalid subscriber: %w", err)
	}
	if err := channel.validate(); err != nil {
		return zero, fmt.Errorf("invalid channel: %w", err)
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_UnsubscribeChannel{
				UnsubscribeChannel: &pb.UnsubscribeChannelRequest{
					RequestId:  requestID,
					Subscriber: userRefToProto(subscriber),
					Channel:    userRefToProto(channel),
				},
			},
		}
	})
	if err != nil {
		return zero, err
	}

	sub, ok := res.value.(Subscription)
	if !ok {
		return zero, &ProtocolError{Message: "missing subscription in unsubscribe_channel_response"}
	}
	return sub, nil
}

func (c *Client) ListSubscriptions(ctx context.Context, subscriber UserRef) ([]Subscription, error) {
	if err := subscriber.validate(); err != nil {
		return nil, fmt.Errorf("invalid subscriber: %w", err)
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_ListSubscriptions{
				ListSubscriptions: &pb.ListSubscriptionsRequest{
					RequestId:  requestID,
					Subscriber: userRefToProto(subscriber),
				},
			},
		}
	})
	if err != nil {
		return nil, err
	}

	items, ok := res.value.([]Subscription)
	if !ok {
		return nil, &ProtocolError{Message: "missing items in list_subscriptions_response"}
	}
	return items, nil
}

func (c *Client) BlockUser(ctx context.Context, token string, owner, blocked UserRef) (BlacklistEntry, error) {
	_ = token
	var zero BlacklistEntry
	if err := owner.validate(); err != nil {
		return zero, fmt.Errorf("invalid owner: %w", err)
	}
	if err := blocked.validate(); err != nil {
		return zero, fmt.Errorf("invalid blocked user: %w", err)
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_BlockUser{
				BlockUser: &pb.BlockUserRequest{
					RequestId: requestID,
					Owner:     userRefToProto(owner),
					Blocked:   userRefToProto(blocked),
				},
			},
		}
	})
	if err != nil {
		return zero, err
	}

	entry, ok := res.value.(BlacklistEntry)
	if !ok {
		return zero, &ProtocolError{Message: "missing entry in block_user_response"}
	}
	return entry, nil
}

func (c *Client) UnblockUser(ctx context.Context, token string, owner, blocked UserRef) (BlacklistEntry, error) {
	_ = token
	var zero BlacklistEntry
	if err := owner.validate(); err != nil {
		return zero, fmt.Errorf("invalid owner: %w", err)
	}
	if err := blocked.validate(); err != nil {
		return zero, fmt.Errorf("invalid blocked user: %w", err)
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_UnblockUser{
				UnblockUser: &pb.UnblockUserRequest{
					RequestId: requestID,
					Owner:     userRefToProto(owner),
					Blocked:   userRefToProto(blocked),
				},
			},
		}
	})
	if err != nil {
		return zero, err
	}

	entry, ok := res.value.(BlacklistEntry)
	if !ok {
		return zero, &ProtocolError{Message: "missing entry in unblock_user_response"}
	}
	return entry, nil
}

func (c *Client) ListBlockedUsers(ctx context.Context, token string, owner UserRef) ([]BlacklistEntry, error) {
	_ = token
	if err := owner.validate(); err != nil {
		return nil, fmt.Errorf("invalid owner: %w", err)
	}

	res, err := c.rpc(ctx, func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{
			Body: &pb.ClientEnvelope_ListBlockedUsers{
				ListBlockedUsers: &pb.ListBlockedUsersRequest{
					RequestId: requestID,
					Owner:     userRefToProto(owner),
				},
			},
		}
	})
	if err != nil {
		return nil, err
	}

	items, ok := res.value.([]BlacklistEntry)
	if !ok {
		return nil, &ProtocolError{Message: "missing items in list_blocked_users_response"}
	}
	return items, nil
}

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

	loginEnv := &pb.ClientEnvelope{
		Body: &pb.ClientEnvelope_Login{
			Login: &pb.LoginRequest{
				User:          userRefToProto(UserRef{NodeID: c.cfg.Credentials.NodeID, UserID: c.cfg.Credentials.UserID}),
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
		c.cfg.Handler.OnPacket(c.ctx, packetFromProto(body.PacketPushed.Packet))
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
	case *pb.ServerEnvelope_UpdateUserResponse:
		c.resolvePending(body.UpdateUserResponse.RequestId, requestResult{value: userFromProto(body.UpdateUserResponse.User)})
	case *pb.ServerEnvelope_DeleteUserResponse:
		c.resolvePending(body.DeleteUserResponse.RequestId, requestResult{value: DeleteUserResult{
			Status: body.DeleteUserResponse.Status,
			User:   userRefFromProto(body.DeleteUserResponse.User),
		}})
	case *pb.ServerEnvelope_ListMessagesResponse:
		c.resolvePending(body.ListMessagesResponse.RequestId, requestResult{value: messagesFromProto(body.ListMessagesResponse.Items)})
	case *pb.ServerEnvelope_SubscribeChannelResponse:
		c.resolvePending(body.SubscribeChannelResponse.RequestId, requestResult{value: subscriptionFromProto(body.SubscribeChannelResponse.Subscription)})
	case *pb.ServerEnvelope_UnsubscribeChannelResponse:
		c.resolvePending(body.UnsubscribeChannelResponse.RequestId, requestResult{value: subscriptionFromProto(body.UnsubscribeChannelResponse.Subscription)})
	case *pb.ServerEnvelope_ListSubscriptionsResponse:
		c.resolvePending(body.ListSubscriptionsResponse.RequestId, requestResult{value: subscriptionsFromProto(body.ListSubscriptionsResponse.Items)})
	case *pb.ServerEnvelope_BlockUserResponse:
		c.resolvePending(body.BlockUserResponse.RequestId, requestResult{value: blacklistEntryFromProto(body.BlockUserResponse.Entry)})
	case *pb.ServerEnvelope_UnblockUserResponse:
		c.resolvePending(body.UnblockUserResponse.RequestId, requestResult{value: blacklistEntryFromProto(body.UnblockUserResponse.Entry)})
	case *pb.ServerEnvelope_ListBlockedUsersResponse:
		c.resolvePending(body.ListBlockedUsersResponse.RequestId, requestResult{value: blacklistEntriesFromProto(body.ListBlockedUsersResponse.Items)})
	case *pb.ServerEnvelope_ListEventsResponse:
		c.resolvePending(body.ListEventsResponse.RequestId, requestResult{value: eventsFromProto(body.ListEventsResponse.Items)})
	case *pb.ServerEnvelope_ListClusterNodesResponse:
		c.resolvePending(body.ListClusterNodesResponse.RequestId, requestResult{value: clusterNodesFromProto(body.ListClusterNodesResponse.Items)})
	case *pb.ServerEnvelope_ListNodeLoggedInUsersResponse:
		c.resolvePending(body.ListNodeLoggedInUsersResponse.RequestId, requestResult{value: loggedInUsersFromProto(body.ListNodeLoggedInUsersResponse.Items)})
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
