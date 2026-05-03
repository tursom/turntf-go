package turntf

import (
	"fmt"
	"regexp"
	"strings"

	pb "github.com/tursom/turntf-go/internal/proto"
)

// Credentials 封装客户端登录凭据，支持通过 (NodeID, UserID) 或 LoginName 两种方式标识用户。
// 两种方式互斥，必须且只能选择一种。Password 必须通过 PlainPassword 或 HashedPassword 构建。
type Credentials struct {
	NodeID    int64
	UserID    int64
	LoginName string
	Password  PasswordInput
}

// UserRef 标识一个用户，由节点 ID 和用户 ID 组成。
// 在调用 API 时，UserRef 的两个字段都必须非零。
type UserRef struct {
	NodeID int64 `json:"node_id"`
	UserID int64 `json:"user_id"`
}

// SessionRef 标识一个用户会话，由服务节点 ID 和会话 ID 组成。
// 用于将消息定向投递到特定会话（而非用户的所有会话）。
type SessionRef struct {
	ServingNodeID int64  `json:"serving_node_id"`
	SessionID     string `json:"session_id"`
}

// User 表示一个用户或频道，包含用户在系统中的完整信息。
// 用户通过 node_id + user_id 唯一确定，也可通过 login_name 登录。
type User struct {
	NodeID         int64  `json:"node_id"`
	UserID         int64  `json:"user_id"`
	Username       string `json:"username"`
	LoginName      string `json:"login_name"`
	Role           string `json:"role"`
	ProfileJSON    []byte `json:"profile_json,omitempty"`
	SystemReserved bool   `json:"system_reserved"`
	CreatedAt      string `json:"created_at,omitempty"`
	UpdatedAt      string `json:"updated_at,omitempty"`
	OriginNodeID   int64  `json:"origin_node_id"`
}

// MessageCursor 标识一个消息的位置，由消息所在节点 ID 和序列号组成。
// 用于消息去重、ACK 确认和断线重连时的消息恢复。
type MessageCursor struct {
	NodeID int64 `json:"node_id"`
	Seq    int64 `json:"seq"`
}

// DeliveryMode 表示瞬时消息（Packet）的投递模式。
type DeliveryMode string

const (
	// DeliveryModeUnspecified 表示未指定投递模式，使用服务端默认策略。
	DeliveryModeUnspecified DeliveryMode = ""
	// DeliveryModeBestEffort 表示尽最大努力投递模式。消息尽力投递，但不保证可靠性。
	DeliveryModeBestEffort DeliveryMode = "best_effort"
	// DeliveryModeRouteRetry 表示路由重试模式。如果目标节点不可达，会持续重试投递。
	DeliveryModeRouteRetry DeliveryMode = "route_retry"
)

const maxUserMetadataLimit = 1000

var metadataKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)

// Message 表示一条已持久化的消息，包含发送者、接收者、消息体和 HLC 时间戳。
// 消息通过 (NodeID, Seq) 唯一标识。
type Message struct {
	Recipient    UserRef `json:"recipient"`
	NodeID       int64   `json:"node_id"`
	Seq          int64   `json:"seq"`
	Sender       UserRef `json:"sender"`
	Body         []byte  `json:"body"`
	CreatedAtHLC string  `json:"created_at_hlc"`
}

// Packet 表示一条瞬时消息（非持久化），包含投递模式和可选的目标会话信息。
// 区别于持久化的 Message，Packet 不会被存储，适合心跳、通知等场景。
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

// RelayAccepted 表示瞬时消息已被服务端接受并准备转发，包含转发目标信息。
type RelayAccepted struct {
	PacketID      uint64       `json:"packet_id"`
	SourceNodeID  int64        `json:"source_node_id"`
	TargetNodeID  int64        `json:"target_node_id"`
	Recipient     UserRef      `json:"recipient"`
	DeliveryMode  DeliveryMode `json:"delivery_mode"`
	TargetSession SessionRef   `json:"target_session"`
}

// AttachmentType 表示附件（关联关系）的类型。
type AttachmentType string

const (
	// AttachmentTypeChannelManager 表示频道管理员，拥有频道管理权限。
	AttachmentTypeChannelManager AttachmentType = "channel_manager"
	// AttachmentTypeChannelWriter 表示频道写入者，拥有向频道发消息的权限。
	AttachmentTypeChannelWriter AttachmentType = "channel_writer"
	// AttachmentTypeChannelSubscription 表示频道订阅关系，订阅者可以收到频道的消息推送。
	AttachmentTypeChannelSubscription AttachmentType = "channel_subscription"
	// AttachmentTypeUserBlacklist 表示用户黑名单，被拉黑的用户无法发送消息。
	AttachmentTypeUserBlacklist AttachmentType = "user_blacklist"
)

// Attachment 表示两个用户之间的关联关系，如频道订阅、黑名单、频道管理员等。
// 通过 AttachmentType 区分不同的关系类型，通过 Owner 和 Subject 标识关系的双方。
type Attachment struct {
	Owner          UserRef        `json:"owner"`
	Subject        UserRef        `json:"subject"`
	AttachmentType AttachmentType `json:"attachment_type"`
	ConfigJSON     []byte         `json:"config_json,omitempty"`
	AttachedAt     string         `json:"attached_at,omitempty"`
	DeletedAt      string         `json:"deleted_at,omitempty"`
	OriginNodeID   int64          `json:"origin_node_id"`
}

// Subscription 表示用户（Subscriber）对频道（Channel）的订阅关系。
// 订阅者可以收到频道的消息推送。注意：Subscription 是 Attachment 的语义封装。
type Subscription struct {
	Subscriber   UserRef `json:"subscriber"`
	Channel      UserRef `json:"channel"`
	SubscribedAt string  `json:"subscribed_at,omitempty"`
	DeletedAt    string  `json:"deleted_at,omitempty"`
	OriginNodeID int64   `json:"origin_node_id"`
}

// BlacklistEntry 表示用户黑名单条目。Owner 将 Blocked 用户拉黑后，
// Blocked 用户将无法向 Owner 发送消息。
type BlacklistEntry struct {
	Owner        UserRef `json:"owner"`
	Blocked      UserRef `json:"blocked"`
	BlockedAt    string  `json:"blocked_at,omitempty"`
	DeletedAt    string  `json:"deleted_at,omitempty"`
	OriginNodeID int64   `json:"origin_node_id"`
}

// UserMetadata 表示用户的自定义元数据键值对，支持过期时间设置。
// 元数据用于存储用户维度的配置信息、状态等轻量数据。
type UserMetadata struct {
	Owner        UserRef `json:"owner"`
	Key          string  `json:"key"`
	Value        []byte  `json:"value"`
	UpdatedAt    string  `json:"updated_at,omitempty"`
	DeletedAt    string  `json:"deleted_at,omitempty"`
	ExpiresAt    string  `json:"expires_at,omitempty"`
	OriginNodeID int64   `json:"origin_node_id"`
}

// UpsertUserMetadataRequest 是创建或更新用户元数据的请求参数。
// Value 为新的元数据值（字节数组），ExpiresAt 为可选过期时间（RFC3339 格式字符串）。
type UpsertUserMetadataRequest struct {
	Value     []byte  `json:"value"`
	ExpiresAt *string `json:"expires_at,omitempty"`
}

// ScanUserMetadataRequest 是按前缀分页扫描用户元数据的请求参数。
// Prefix 为键的前缀过滤条件，After 为分页游标，Limit 为每页返回数量（最大 1000）。
type ScanUserMetadataRequest struct {
	Prefix string `json:"prefix,omitempty"`
	After  string `json:"after,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

// UserMetadataPage 是用户元数据扫描结果的分页数据，包含元数据列表和下一页游标。
type UserMetadataPage struct {
	Items     []UserMetadata `json:"items"`
	Count     int            `json:"count"`
	NextAfter string         `json:"next_after,omitempty"`
}

// Event 表示领域事件，用于事件溯源和跨节点数据同步。
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

// ClusterNode 表示集群中的一个节点信息。
type ClusterNode struct {
	NodeID        int64  `json:"node_id"`
	IsLocal       bool   `json:"is_local"`
	ConfiguredURL string `json:"configured_url,omitempty"`
	Source        string `json:"source,omitempty"`
}

// LoggedInUser 表示节点上当前已登录的用户信息，用于查看节点在线用户。
type LoggedInUser struct {
	NodeID    int64  `json:"node_id"`
	UserID    int64  `json:"user_id"`
	Username  string `json:"username"`
	LoginName string `json:"login_name"`
}

// OnlineNodePresence 表示用户在某个服务节点上的在线存在性信息，包含会话数量和传输方式。
type OnlineNodePresence struct {
	ServingNodeID int64  `json:"serving_node_id"`
	SessionCount  int32  `json:"session_count"`
	TransportHint string `json:"transport_hint,omitempty"`
}

// ResolvedSession 表示用户的一个在线会话详情，包含会话引用、传输协议和是否支持瞬时消息。
type ResolvedSession struct {
	Session          SessionRef `json:"session"`
	Transport        string     `json:"transport,omitempty"`
	TransientCapable bool       `json:"transient_capable"`
}

// ResolvedUserSessions 表示用户的完整在线状态，包含节点存在性信息和所有活跃会话列表。
type ResolvedUserSessions struct {
	User     UserRef              `json:"user"`
	Presence []OnlineNodePresence `json:"presence,omitempty"`
	Sessions []ResolvedSession    `json:"sessions,omitempty"`
}

// MessageTrimStatus 表示消息修剪操作的统计状态。
type MessageTrimStatus struct {
	TrimmedTotal  int64  `json:"trimmed_total"`
	LastTrimmedAt string `json:"last_trimmed_at,omitempty"`
}

// ProjectionStatus 表示事件投影（Projection）的处理状态。
type ProjectionStatus struct {
	PendingTotal int64  `json:"pending_total"`
	LastFailedAt string `json:"last_failed_at,omitempty"`
}

// PeerOriginStatus 表示对等节点上某个数据源（Origin）的同步状态。
type PeerOriginStatus struct {
	OriginNodeID      int64  `json:"origin_node_id"`
	AckedEventID      int64  `json:"acked_event_id"`
	AppliedEventID    int64  `json:"applied_event_id"`
	UnconfirmedEvents int64  `json:"unconfirmed_events"`
	CursorUpdatedAt   string `json:"cursor_updated_at,omitempty"`
	RemoteLastEventID uint64 `json:"remote_last_event_id"`
	PendingCatchup    bool   `json:"pending_catchup"`
}

// PeerStatus 表示集群中对等节点的连接和同步状态。
type PeerStatus struct {
	NodeID                    int64              `json:"node_id"`
	ConfiguredURL             string             `json:"configured_url,omitempty"`
	Source                    string             `json:"source,omitempty"`
	DiscoveredURL             string             `json:"discovered_url,omitempty"`
	DiscoveryState            string             `json:"discovery_state,omitempty"`
	LastDiscoveredAt          string             `json:"last_discovered_at,omitempty"`
	LastConnectedAt           string             `json:"last_connected_at,omitempty"`
	LastDiscoveryError        string             `json:"last_discovery_error,omitempty"`
	Connected                 bool               `json:"connected"`
	SessionDirection          string             `json:"session_direction,omitempty"`
	Origins                   []PeerOriginStatus `json:"origins,omitempty"`
	PendingSnapshotPartitions int32              `json:"pending_snapshot_partitions"`
	RemoteSnapshotVersion     string             `json:"remote_snapshot_version,omitempty"`
	RemoteMessageWindowSize   int32              `json:"remote_message_window_size"`
	ClockOffsetMS             int64              `json:"clock_offset_ms"`
	LastClockSync             string             `json:"last_clock_sync,omitempty"`
	SnapshotDigestsSentTotal  uint64             `json:"snapshot_digests_sent_total"`
	SnapshotDigestsRecvTotal  uint64             `json:"snapshot_digests_received_total"`
	SnapshotChunksSentTotal   uint64             `json:"snapshot_chunks_sent_total"`
	SnapshotChunksRecvTotal   uint64             `json:"snapshot_chunks_received_total"`
	LastSnapshotDigestAt      string             `json:"last_snapshot_digest_at,omitempty"`
	LastSnapshotChunkAt       string             `json:"last_snapshot_chunk_at,omitempty"`
}

// OperationsStatus 表示服务节点的运维状态，包括消息窗口、事件序列、写入门控、冲突统计、
// 消息修剪、投影进度以及集群对等节点状态等综合信息。
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

// DeleteUserResult 表示删除用户操作的结果，包含操作状态和被删除用户的引用。
type DeleteUserResult struct {
	Status string  `json:"status"`
	User   UserRef `json:"user"`
}

// LoginInfo 表示登录成功后的信息，包括当前用户信息、协议版本和当前会话引用。
type LoginInfo struct {
	User            User
	ProtocolVersion string
	SessionRef      SessionRef
}

// SendMessageInput 是发送持久化消息的请求参数。
// Target 指定消息接收者，Body 为消息内容的字节数组。
type SendMessageInput struct {
	Target UserRef
	Body   []byte
}

// SendPacketInput 是发送瞬时消息（Packet）的请求参数。
// Target 指定消息接收者，Body 为消息内容，DeliveryMode 指定投递模式，
// TargetSession 可选，指定目标会话（空值表示投递到所有会话）。
type SendPacketInput struct {
	Target        UserRef
	Body          []byte
	DeliveryMode  DeliveryMode
	TargetSession SessionRef
}

// Reliability 表示 RelayConnection 的可靠性等级。
type Reliability int32

const (
	// ReliabilityBestEffort 无 ACK，无重传，无去重，无排序。延迟最低，适合实时音视频帧。
	ReliabilityBestEffort Reliability = 0
	// ReliabilityAtLeastOnce ACK + 重传，不保证去重和排序。适合幂等指令。
	ReliabilityAtLeastOnce Reliability = 1
	// ReliabilityReliableOrdered ACK + 重传 + 去重 + 严格有序。适合文件传输和聊天消息。
	ReliabilityReliableOrdered Reliability = 2
)

// RelayState 表示 RelayConnection 的当前状态。
type RelayState int32

const (
	// RelayStateClosed 初始状态或已关闭。
	RelayStateClosed RelayState = 0
	// RelayStateOpening 已发送 OPEN，等待 OPEN_ACK。
	RelayStateOpening RelayState = 1
	// RelayStateOpen 连接已建立，可收发数据。
	RelayStateOpen RelayState = 2
	// RelayStateClosing 已发送 CLOSE，等待确认。
	RelayStateClosing RelayState = 3
)

// RelayKind 是 relay 协议帧的类型枚举，对应 proto RelayKind。
type RelayKind int32

const (
	RelayKindUnspecified RelayKind = 0
	RelayKindOpen        RelayKind = 1
	RelayKindOpenAck     RelayKind = 2
	RelayKindData        RelayKind = 3
	RelayKindAck         RelayKind = 4
	RelayKindClose       RelayKind = 5
	RelayKindPing        RelayKind = 6
	RelayKindError       RelayKind = 7
)

// RelayConfig 是 RelayConnection 的配置。
type RelayConfig struct {
	// Reliability 可靠性等级，默认 ReliabilityReliableOrdered。
	Reliability Reliability
	// WindowSize 发送窗口大小（在途未确认帧数上限），范围 1-256，默认 16。
	// BestEffort 模式下忽略此配置。
	WindowSize int
	// OpenTimeoutMs OPEN 等待 OPEN_ACK 超时毫秒数，默认 10000。
	OpenTimeoutMs int64
	// CloseTimeoutMs CLOSE 等待确认超时毫秒数，默认 5000。
	CloseTimeoutMs int64
	// AckTimeoutMs DATA 等待 ACK 超时毫秒数，默认 3000。
	// BestEffort 模式下忽略此配置。
	AckTimeoutMs int64
	// MaxRetransmits 最大重传次数，默认 5。
	// BestEffort 模式下忽略此配置。
	MaxRetransmits int
	// IdleTimeoutMs 无数据超时断开毫秒数，0 表示不超时。
	IdleTimeoutMs int64
	// SendTimeoutMs Send 操作超时毫秒数（窗口或缓冲区满时等待上限），0 表示不超时。
	SendTimeoutMs int64
	// ReceiveTimeoutMs Receive 操作超时毫秒数（无数据等待上限），0 表示不超时。
	ReceiveTimeoutMs int64
	// SendBufferSize 发送缓冲区字节数，默认 65536。
	SendBufferSize int
	// DeliveryMode Packet 投递模式，默认 DeliveryModeRouteRetry。
	DeliveryMode DeliveryMode
}

// DefaultRelayConfig 返回带默认值的 RelayConfig。
func DefaultRelayConfig() RelayConfig {
	return RelayConfig{
		Reliability:      ReliabilityReliableOrdered,
		WindowSize:       16,
		OpenTimeoutMs:    10000,
		CloseTimeoutMs:   5000,
		AckTimeoutMs:     3000,
		MaxRetransmits:   5,
		SendTimeoutMs:    0,
		ReceiveTimeoutMs: 0,
		SendBufferSize:   65536,
		DeliveryMode:     DeliveryModeRouteRetry,
	}
}

// RelayEnvelope 是 relay 协议的帧类型，与 proto RelayEnvelope 对应。
type RelayEnvelope struct {
	RelayID       string
	Kind          RelayKind
	SenderSession SessionRef
	TargetSession SessionRef
	Seq           uint64
	AckSeq        uint64
	Payload       []byte
	SentAtMs      int64
}

// RelayError 表示 relay 层的错误。
type RelayError struct {
	Code    string
	Message string
}

func (e *RelayError) Error() string {
	return "relay: " + e.Code + ": " + e.Message
}

// relay 错误码
const (
	RelayErrorOpenTimeout    = "open_timeout"
	RelayErrorAckTimeout     = "ack_timeout"
	RelayErrorMaxRetransmit  = "max_retransmit"
	RelayErrorIdleTimeout    = "idle_timeout"
	RelayErrorRemoteClose    = "remote_close"
	RelayErrorClientClosed   = "client_closed"
	RelayErrorProtocol       = "protocol_error"
	RelayErrorDuplicateOpen  = "duplicate_open"
	RelayErrorNotConnected   = "not_connected"
	RelayErrorSendTimeout    = "send_timeout"
	RelayErrorReceiveTimeout = "receive_timeout"
)

// CreateUserRequest 是创建用户或频道的请求参数。
// 创建用户时，Username 和 Role 为必填；Password 可选（频道用户不需要密码）。
type CreateUserRequest struct {
	Username    string        `json:"username"`
	LoginName   string        `json:"login_name,omitempty"`
	Password    PasswordInput `json:"password,omitempty"`
	ProfileJSON []byte        `json:"profile_json,omitempty"`
	Role        string        `json:"role"`
}

// UpdateUserRequest 是更新用户的请求参数，所有字段均为可选（指针类型）。
// nil 表示不更新该字段，非 nil 表示更新为指定值。密码更新使用 *PasswordInput 类型。
type UpdateUserRequest struct {
	Username    *string        `json:"username,omitempty"`
	LoginName   *string        `json:"login_name,omitempty"`
	Password    *PasswordInput `json:"password,omitempty"`
	ProfileJSON *[]byte        `json:"profile_json,omitempty"`
	Role        *string        `json:"role,omitempty"`
}

// ListUsersRequest 是列出当前可通讯用户列表的过滤参数。
// Name 为大小写不敏感子串匹配；UID 为可选的精确用户过滤条件。
type ListUsersRequest struct {
	Name string  `json:"name,omitempty"`
	UID  UserRef `json:"uid,omitempty"`
}

// Cursor 返回该消息的游标，由消息所在节点 ID 和序列号组成，用于消息去重和 ACK 确认。
func (m Message) Cursor() MessageCursor {
	return MessageCursor{NodeID: m.NodeID, Seq: m.Seq}
}

func normalizeLoginName(value string) string {
	return strings.TrimSpace(value)
}

func validateLoginSelector(nodeID, userID int64, loginName string) (string, error) {
	normalized := normalizeLoginName(loginName)
	hasIDSelector := nodeID != 0 || userID != 0
	hasLoginNameSelector := normalized != ""
	if hasIDSelector == hasLoginNameSelector {
		return "", fmt.Errorf("exactly one of (node_id,user_id) or login_name is required")
	}
	if hasIDSelector && (nodeID == 0 || userID == 0) {
		return "", fmt.Errorf("both node_id and user_id are required")
	}
	return normalized, nil
}

func (c Credentials) validate() error {
	if _, err := validateLoginSelector(c.NodeID, c.UserID, c.LoginName); err != nil {
		return err
	}
	if err := c.Password.Validate(); err != nil {
		return fmt.Errorf("invalid password: %w", err)
	}
	return nil
}

func (c Credentials) loginSelector() (*pb.UserRef, string) {
	loginName := normalizeLoginName(c.LoginName)
	if loginName != "" {
		return nil, loginName
	}
	return userRefToProto(UserRef{NodeID: c.NodeID, UserID: c.UserID}), ""
}

// HasMore 判断分页结果中是否还有更多数据（NextAfter 不为空）。
func (p UserMetadataPage) HasMore() bool {
	return p.NextAfter != ""
}

func (r UserRef) validate() error {
	if r.NodeID == 0 {
		return fmt.Errorf("node_id is required")
	}
	if r.UserID == 0 {
		return fmt.Errorf("user_id is required")
	}
	return nil
}

// IsZero 判断 UserRef 是否为空（未设置任何值）。
func (r UserRef) IsZero() bool {
	return r.NodeID == 0 && r.UserID == 0
}

func validateMetadataKey(key string) error {
	return validateMetadataKeyFragment(key, "key", false)
}

func (r ListUsersRequest) validate() error {
	if r.UID.IsZero() {
		return nil
	}
	if r.UID.NodeID <= 0 {
		return fmt.Errorf("uid.node_id must be a positive integer")
	}
	if r.UID.UserID <= 0 {
		return fmt.Errorf("uid.user_id must be a positive integer")
	}
	return nil
}

func (r ListUsersRequest) normalized() ListUsersRequest {
	r.Name = strings.TrimSpace(r.Name)
	return r
}

func validateMetadataKeyFragment(value, field string, allowEmpty bool) error {
	if value == "" {
		if allowEmpty {
			return nil
		}
		return fmt.Errorf("%s is required", field)
	}
	if len(value) > 128 {
		return fmt.Errorf("%s cannot exceed 128 characters", field)
	}
	if !metadataKeyPattern.MatchString(value) {
		return fmt.Errorf("%s contains unsupported characters", field)
	}
	return nil
}

func (r ScanUserMetadataRequest) validate() error {
	if err := validateMetadataKeyFragment(r.Prefix, "prefix", true); err != nil {
		return err
	}
	if err := validateMetadataKeyFragment(r.After, "after", true); err != nil {
		return err
	}
	if r.Limit < 0 {
		return fmt.Errorf("limit must be a non-negative integer")
	}
	if r.Limit > maxUserMetadataLimit {
		return fmt.Errorf("limit cannot exceed %d", maxUserMetadataLimit)
	}
	if r.Prefix != "" && r.After != "" && !strings.HasPrefix(r.After, r.Prefix) {
		return fmt.Errorf("after must use the same prefix as prefix")
	}
	return nil
}

// IsZero 判断 SessionRef 是否为空（未设置任何值）。
func (r SessionRef) IsZero() bool {
	return r.ServingNodeID == 0 && r.SessionID == ""
}

// Valid 判断 SessionRef 是否有效（ServingNodeID 和 SessionID 均非空）。
func (r SessionRef) Valid() bool {
	return r.ServingNodeID != 0 && r.SessionID != ""
}

func (r SessionRef) validate() error {
	if r.ServingNodeID == 0 {
		return fmt.Errorf("serving_node_id is required")
	}
	if r.SessionID == "" {
		return fmt.Errorf("session_id is required")
	}
	return nil
}

func (m DeliveryMode) validatePacketMode() error {
	switch m {
	case DeliveryModeBestEffort, DeliveryModeRouteRetry:
		return nil
	default:
		return fmt.Errorf("invalid delivery_mode %q", m)
	}
}

func deliveryModeToProto(mode DeliveryMode) pb.ClientDeliveryMode {
	switch mode {
	case DeliveryModeBestEffort:
		return pb.ClientDeliveryMode_CLIENT_DELIVERY_MODE_BEST_EFFORT
	case DeliveryModeRouteRetry:
		return pb.ClientDeliveryMode_CLIENT_DELIVERY_MODE_ROUTE_RETRY
	default:
		return pb.ClientDeliveryMode_CLIENT_DELIVERY_MODE_UNSPECIFIED
	}
}

func deliveryModeFromProto(mode pb.ClientDeliveryMode) DeliveryMode {
	switch mode {
	case pb.ClientDeliveryMode_CLIENT_DELIVERY_MODE_BEST_EFFORT:
		return DeliveryModeBestEffort
	case pb.ClientDeliveryMode_CLIENT_DELIVERY_MODE_ROUTE_RETRY:
		return DeliveryModeRouteRetry
	default:
		return DeliveryModeUnspecified
	}
}

func userRefToProto(in UserRef) *pb.UserRef {
	if in.IsZero() {
		return nil
	}
	return &pb.UserRef{
		NodeId: in.NodeID,
		UserId: in.UserID,
	}
}

func userRefFromProto(in *pb.UserRef) UserRef {
	if in == nil {
		return UserRef{}
	}
	return UserRef{
		NodeID: in.NodeId,
		UserID: in.UserId,
	}
}

func sessionRefToProto(in SessionRef) *pb.SessionRef {
	if in.IsZero() {
		return nil
	}
	return &pb.SessionRef{
		ServingNodeId: in.ServingNodeID,
		SessionId:     in.SessionID,
	}
}

func sessionRefFromProto(in *pb.SessionRef) SessionRef {
	if in == nil {
		return SessionRef{}
	}
	return SessionRef{
		ServingNodeID: in.ServingNodeId,
		SessionID:     in.SessionId,
	}
}

func userFromProto(in *pb.User) User {
	if in == nil {
		return User{}
	}
	return User{
		NodeID:         in.NodeId,
		UserID:         in.UserId,
		Username:       in.Username,
		LoginName:      in.LoginName,
		Role:           in.Role,
		ProfileJSON:    append([]byte(nil), in.ProfileJson...),
		SystemReserved: in.SystemReserved,
		CreatedAt:      in.CreatedAt,
		UpdatedAt:      in.UpdatedAt,
		OriginNodeID:   in.OriginNodeId,
	}
}

func usersFromProto(items []*pb.User) []User {
	out := make([]User, 0, len(items))
	for _, item := range items {
		out = append(out, userFromProto(item))
	}
	return out
}

func cursorToProto(in MessageCursor) *pb.MessageCursor {
	return &pb.MessageCursor{
		NodeId: in.NodeID,
		Seq:    in.Seq,
	}
}

func cursorFromProto(in *pb.MessageCursor) MessageCursor {
	if in == nil {
		return MessageCursor{}
	}
	return MessageCursor{
		NodeID: in.NodeId,
		Seq:    in.Seq,
	}
}

func messageFromProto(in *pb.Message) Message {
	if in == nil {
		return Message{}
	}
	return Message{
		Recipient:    userRefFromProto(in.Recipient),
		NodeID:       in.NodeId,
		Seq:          in.Seq,
		Sender:       userRefFromProto(in.Sender),
		Body:         append([]byte(nil), in.Body...),
		CreatedAtHLC: in.CreatedAtHlc,
	}
}

func packetFromProto(in *pb.Packet) Packet {
	if in == nil {
		return Packet{}
	}
	return Packet{
		PacketID:      in.PacketId,
		SourceNodeID:  in.SourceNodeId,
		TargetNodeID:  in.TargetNodeId,
		Recipient:     userRefFromProto(in.Recipient),
		Sender:        userRefFromProto(in.Sender),
		Body:          append([]byte(nil), in.Body...),
		DeliveryMode:  deliveryModeFromProto(in.DeliveryMode),
		TargetSession: sessionRefFromProto(in.TargetSession),
	}
}

func relayAcceptedFromProto(in *pb.TransientAccepted) RelayAccepted {
	if in == nil {
		return RelayAccepted{}
	}
	return RelayAccepted{
		PacketID:      in.PacketId,
		SourceNodeID:  in.SourceNodeId,
		TargetNodeID:  in.TargetNodeId,
		Recipient:     userRefFromProto(in.Recipient),
		DeliveryMode:  deliveryModeFromProto(in.DeliveryMode),
		TargetSession: sessionRefFromProto(in.TargetSession),
	}
}

func attachmentTypeToProto(in AttachmentType) pb.AttachmentType {
	switch in {
	case AttachmentTypeChannelManager:
		return pb.AttachmentType_ATTACHMENT_TYPE_CHANNEL_MANAGER
	case AttachmentTypeChannelWriter:
		return pb.AttachmentType_ATTACHMENT_TYPE_CHANNEL_WRITER
	case AttachmentTypeChannelSubscription:
		return pb.AttachmentType_ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION
	case AttachmentTypeUserBlacklist:
		return pb.AttachmentType_ATTACHMENT_TYPE_USER_BLACKLIST
	default:
		return pb.AttachmentType_ATTACHMENT_TYPE_UNSPECIFIED
	}
}

func attachmentTypeFromProto(in pb.AttachmentType) AttachmentType {
	switch in {
	case pb.AttachmentType_ATTACHMENT_TYPE_CHANNEL_MANAGER:
		return AttachmentTypeChannelManager
	case pb.AttachmentType_ATTACHMENT_TYPE_CHANNEL_WRITER:
		return AttachmentTypeChannelWriter
	case pb.AttachmentType_ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION:
		return AttachmentTypeChannelSubscription
	case pb.AttachmentType_ATTACHMENT_TYPE_USER_BLACKLIST:
		return AttachmentTypeUserBlacklist
	default:
		return ""
	}
}

func attachmentFromProto(in *pb.Attachment) Attachment {
	if in == nil {
		return Attachment{}
	}
	return Attachment{
		Owner:          userRefFromProto(in.Owner),
		Subject:        userRefFromProto(in.Subject),
		AttachmentType: attachmentTypeFromProto(in.AttachmentType),
		ConfigJSON:     append([]byte(nil), in.ConfigJson...),
		AttachedAt:     in.AttachedAt,
		DeletedAt:      in.DeletedAt,
		OriginNodeID:   in.OriginNodeId,
	}
}

func subscriptionFromProto(in *pb.Attachment) Subscription {
	attachment := attachmentFromProto(in)
	return Subscription{
		Subscriber:   attachment.Owner,
		Channel:      attachment.Subject,
		SubscribedAt: attachment.AttachedAt,
		DeletedAt:    attachment.DeletedAt,
		OriginNodeID: attachment.OriginNodeID,
	}
}

func blacklistEntryFromProto(in *pb.Attachment) BlacklistEntry {
	attachment := attachmentFromProto(in)
	return BlacklistEntry{
		Owner:        attachment.Owner,
		Blocked:      attachment.Subject,
		BlockedAt:    attachment.AttachedAt,
		DeletedAt:    attachment.DeletedAt,
		OriginNodeID: attachment.OriginNodeID,
	}
}

func userMetadataFromProto(in *pb.UserMetadata) UserMetadata {
	if in == nil {
		return UserMetadata{}
	}
	return UserMetadata{
		Owner:        userRefFromProto(in.Owner),
		Key:          in.Key,
		Value:        append([]byte(nil), in.Value...),
		UpdatedAt:    in.UpdatedAt,
		DeletedAt:    in.DeletedAt,
		ExpiresAt:    in.ExpiresAt,
		OriginNodeID: in.OriginNodeId,
	}
}

func userMetadataPageFromProto(in *pb.ScanUserMetadataResponse) UserMetadataPage {
	if in == nil {
		return UserMetadataPage{}
	}
	return UserMetadataPage{
		Items:     userMetadataItemsFromProto(in.Items),
		Count:     int(in.Count),
		NextAfter: in.NextAfter,
	}
}

func eventFromProto(in *pb.Event) Event {
	if in == nil {
		return Event{}
	}
	return Event{
		Sequence:        in.Sequence,
		EventID:         in.EventId,
		EventType:       in.EventType,
		Aggregate:       in.Aggregate,
		AggregateNodeID: in.AggregateNodeId,
		AggregateID:     in.AggregateId,
		HLC:             in.Hlc,
		OriginNodeID:    in.OriginNodeId,
		EventJSON:       append([]byte(nil), in.EventJson...),
	}
}

func clusterNodeFromProto(in *pb.ClusterNode) ClusterNode {
	if in == nil {
		return ClusterNode{}
	}
	return ClusterNode{
		NodeID:        in.NodeId,
		IsLocal:       in.IsLocal,
		ConfiguredURL: in.ConfiguredUrl,
		Source:        in.Source,
	}
}

func loggedInUserFromProto(in *pb.LoggedInUser) LoggedInUser {
	if in == nil {
		return LoggedInUser{}
	}
	return LoggedInUser{
		NodeID:    in.NodeId,
		UserID:    in.UserId,
		Username:  in.Username,
		LoginName: in.LoginName,
	}
}

func onlineNodePresenceFromProto(in *pb.OnlineNodePresence) OnlineNodePresence {
	if in == nil {
		return OnlineNodePresence{}
	}
	return OnlineNodePresence{
		ServingNodeID: in.ServingNodeId,
		SessionCount:  in.SessionCount,
		TransportHint: in.TransportHint,
	}
}

func resolvedSessionFromProto(in *pb.ResolvedSession) ResolvedSession {
	if in == nil {
		return ResolvedSession{}
	}
	return ResolvedSession{
		Session:          sessionRefFromProto(in.Session),
		Transport:        in.Transport,
		TransientCapable: in.TransientCapable,
	}
}

func resolvedUserSessionsFromProto(in *pb.ResolveUserSessionsResponse) ResolvedUserSessions {
	if in == nil {
		return ResolvedUserSessions{}
	}

	presence := make([]OnlineNodePresence, 0, len(in.Presence))
	for _, item := range in.Presence {
		presence = append(presence, onlineNodePresenceFromProto(item))
	}

	sessions := make([]ResolvedSession, 0, len(in.Items))
	for _, item := range in.Items {
		sessions = append(sessions, resolvedSessionFromProto(item))
	}

	return ResolvedUserSessions{
		User:     userRefFromProto(in.User),
		Presence: presence,
		Sessions: sessions,
	}
}

func operationsStatusFromProto(in *pb.OperationsStatus) OperationsStatus {
	if in == nil {
		return OperationsStatus{}
	}

	peers := make([]PeerStatus, 0, len(in.Peers))
	for _, peer := range in.Peers {
		peers = append(peers, peerStatusFromProto(peer))
	}

	return OperationsStatus{
		NodeID:            in.NodeId,
		MessageWindowSize: in.MessageWindowSize,
		LastEventSequence: in.LastEventSequence,
		WriteGateReady:    in.WriteGateReady,
		ConflictTotal:     in.ConflictTotal,
		MessageTrim:       messageTrimStatusFromProto(in.MessageTrim),
		Projection:        projectionStatusFromProto(in.Projection),
		Peers:             peers,
	}
}

func messageTrimStatusFromProto(in *pb.MessageTrimStatus) MessageTrimStatus {
	if in == nil {
		return MessageTrimStatus{}
	}
	return MessageTrimStatus{
		TrimmedTotal:  in.TrimmedTotal,
		LastTrimmedAt: in.LastTrimmedAt,
	}
}

func projectionStatusFromProto(in *pb.ProjectionStatus) ProjectionStatus {
	if in == nil {
		return ProjectionStatus{}
	}
	return ProjectionStatus{
		PendingTotal: in.PendingTotal,
		LastFailedAt: in.LastFailedAt,
	}
}

func peerOriginStatusFromProto(in *pb.PeerOriginStatus) PeerOriginStatus {
	if in == nil {
		return PeerOriginStatus{}
	}
	return PeerOriginStatus{
		OriginNodeID:      in.OriginNodeId,
		AckedEventID:      in.AckedEventId,
		AppliedEventID:    in.AppliedEventId,
		UnconfirmedEvents: in.UnconfirmedEvents,
		CursorUpdatedAt:   in.CursorUpdatedAt,
		RemoteLastEventID: in.RemoteLastEventId,
		PendingCatchup:    in.PendingCatchup,
	}
}

func peerStatusFromProto(in *pb.PeerStatus) PeerStatus {
	if in == nil {
		return PeerStatus{}
	}

	origins := make([]PeerOriginStatus, 0, len(in.Origins))
	for _, origin := range in.Origins {
		origins = append(origins, peerOriginStatusFromProto(origin))
	}

	return PeerStatus{
		NodeID:                    in.NodeId,
		ConfiguredURL:             in.ConfiguredUrl,
		Source:                    in.Source,
		DiscoveredURL:             in.DiscoveredUrl,
		DiscoveryState:            in.DiscoveryState,
		LastDiscoveredAt:          in.LastDiscoveredAt,
		LastConnectedAt:           in.LastConnectedAt,
		LastDiscoveryError:        in.LastDiscoveryError,
		Connected:                 in.Connected,
		SessionDirection:          in.SessionDirection,
		Origins:                   origins,
		PendingSnapshotPartitions: in.PendingSnapshotPartitions,
		RemoteSnapshotVersion:     in.RemoteSnapshotVersion,
		RemoteMessageWindowSize:   in.RemoteMessageWindowSize,
		ClockOffsetMS:             in.ClockOffsetMs,
		LastClockSync:             in.LastClockSync,
		SnapshotDigestsSentTotal:  in.SnapshotDigestsSentTotal,
		SnapshotDigestsRecvTotal:  in.SnapshotDigestsReceivedTotal,
		SnapshotChunksSentTotal:   in.SnapshotChunksSentTotal,
		SnapshotChunksRecvTotal:   in.SnapshotChunksReceivedTotal,
		LastSnapshotDigestAt:      in.LastSnapshotDigestAt,
		LastSnapshotChunkAt:       in.LastSnapshotChunkAt,
	}
}

func messagesFromProto(items []*pb.Message) []Message {
	out := make([]Message, 0, len(items))
	for _, item := range items {
		out = append(out, messageFromProto(item))
	}
	return out
}

func attachmentsFromProto(items []*pb.Attachment) []Attachment {
	out := make([]Attachment, 0, len(items))
	for _, item := range items {
		out = append(out, attachmentFromProto(item))
	}
	return out
}

func subscriptionsFromProto(items []*pb.Attachment) []Subscription {
	out := make([]Subscription, 0, len(items))
	for _, item := range items {
		out = append(out, subscriptionFromProto(item))
	}
	return out
}

func blacklistEntriesFromProto(items []*pb.Attachment) []BlacklistEntry {
	out := make([]BlacklistEntry, 0, len(items))
	for _, item := range items {
		out = append(out, blacklistEntryFromProto(item))
	}
	return out
}

func userMetadataItemsFromProto(items []*pb.UserMetadata) []UserMetadata {
	out := make([]UserMetadata, 0, len(items))
	for _, item := range items {
		out = append(out, userMetadataFromProto(item))
	}
	return out
}

func eventsFromProto(items []*pb.Event) []Event {
	out := make([]Event, 0, len(items))
	for _, item := range items {
		out = append(out, eventFromProto(item))
	}
	return out
}

func clusterNodesFromProto(items []*pb.ClusterNode) []ClusterNode {
	out := make([]ClusterNode, 0, len(items))
	for _, item := range items {
		out = append(out, clusterNodeFromProto(item))
	}
	return out
}

func loggedInUsersFromProto(items []*pb.LoggedInUser) []LoggedInUser {
	out := make([]LoggedInUser, 0, len(items))
	for _, item := range items {
		out = append(out, loggedInUserFromProto(item))
	}
	return out
}

func optionalStringField(value *string) *pb.StringField {
	if value == nil {
		return nil
	}
	return &pb.StringField{Value: *value}
}

func optionalPasswordField(value *PasswordInput) *pb.StringField {
	if value == nil {
		return nil
	}
	return &pb.StringField{Value: value.WireValue()}
}

func optionalBytesField(value *[]byte) *pb.BytesField {
	if value == nil {
		return nil
	}
	return &pb.BytesField{Value: append([]byte(nil), (*value)...)}
}
