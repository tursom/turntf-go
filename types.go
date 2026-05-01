package turntf

import (
	"fmt"
	"regexp"
	"strings"

	pb "github.com/tursom/turntf-go/internal/proto"
)

type Credentials struct {
	NodeID   int64
	UserID   int64
	Password PasswordInput
}

type UserRef struct {
	NodeID int64 `json:"node_id"`
	UserID int64 `json:"user_id"`
}

type SessionRef struct {
	ServingNodeID int64  `json:"serving_node_id"`
	SessionID     string `json:"session_id"`
}

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

type MessageCursor struct {
	NodeID int64 `json:"node_id"`
	Seq    int64 `json:"seq"`
}

type DeliveryMode string

const (
	DeliveryModeUnspecified DeliveryMode = ""
	DeliveryModeBestEffort  DeliveryMode = "best_effort"
	DeliveryModeRouteRetry  DeliveryMode = "route_retry"
)

const maxUserMetadataLimit = 1000

var metadataKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)

type Message struct {
	Recipient    UserRef `json:"recipient"`
	NodeID       int64   `json:"node_id"`
	Seq          int64   `json:"seq"`
	Sender       UserRef `json:"sender"`
	Body         []byte  `json:"body"`
	CreatedAtHLC string  `json:"created_at_hlc"`
}

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

type RelayAccepted struct {
	PacketID      uint64       `json:"packet_id"`
	SourceNodeID  int64        `json:"source_node_id"`
	TargetNodeID  int64        `json:"target_node_id"`
	Recipient     UserRef      `json:"recipient"`
	DeliveryMode  DeliveryMode `json:"delivery_mode"`
	TargetSession SessionRef   `json:"target_session"`
}

type AttachmentType string

const (
	AttachmentTypeChannelManager      AttachmentType = "channel_manager"
	AttachmentTypeChannelWriter       AttachmentType = "channel_writer"
	AttachmentTypeChannelSubscription AttachmentType = "channel_subscription"
	AttachmentTypeUserBlacklist       AttachmentType = "user_blacklist"
)

type Attachment struct {
	Owner          UserRef        `json:"owner"`
	Subject        UserRef        `json:"subject"`
	AttachmentType AttachmentType `json:"attachment_type"`
	ConfigJSON     []byte         `json:"config_json,omitempty"`
	AttachedAt     string         `json:"attached_at,omitempty"`
	DeletedAt      string         `json:"deleted_at,omitempty"`
	OriginNodeID   int64          `json:"origin_node_id"`
}

type Subscription struct {
	Subscriber   UserRef `json:"subscriber"`
	Channel      UserRef `json:"channel"`
	SubscribedAt string  `json:"subscribed_at,omitempty"`
	DeletedAt    string  `json:"deleted_at,omitempty"`
	OriginNodeID int64   `json:"origin_node_id"`
}

type BlacklistEntry struct {
	Owner        UserRef `json:"owner"`
	Blocked      UserRef `json:"blocked"`
	BlockedAt    string  `json:"blocked_at,omitempty"`
	DeletedAt    string  `json:"deleted_at,omitempty"`
	OriginNodeID int64   `json:"origin_node_id"`
}

type UserMetadata struct {
	Owner        UserRef `json:"owner"`
	Key          string  `json:"key"`
	Value        []byte  `json:"value"`
	UpdatedAt    string  `json:"updated_at,omitempty"`
	DeletedAt    string  `json:"deleted_at,omitempty"`
	ExpiresAt    string  `json:"expires_at,omitempty"`
	OriginNodeID int64   `json:"origin_node_id"`
}

type UpsertUserMetadataRequest struct {
	Value     []byte  `json:"value"`
	ExpiresAt *string `json:"expires_at,omitempty"`
}

type ScanUserMetadataRequest struct {
	Prefix string `json:"prefix,omitempty"`
	After  string `json:"after,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

type UserMetadataPage struct {
	Items     []UserMetadata `json:"items"`
	Count     int            `json:"count"`
	NextAfter string         `json:"next_after,omitempty"`
}

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

type ClusterNode struct {
	NodeID        int64  `json:"node_id"`
	IsLocal       bool   `json:"is_local"`
	ConfiguredURL string `json:"configured_url,omitempty"`
	Source        string `json:"source,omitempty"`
}

type LoggedInUser struct {
	NodeID   int64  `json:"node_id"`
	UserID   int64  `json:"user_id"`
	Username string `json:"username"`
}

type OnlineNodePresence struct {
	ServingNodeID int64  `json:"serving_node_id"`
	SessionCount  int32  `json:"session_count"`
	TransportHint string `json:"transport_hint,omitempty"`
}

type ResolvedSession struct {
	Session          SessionRef `json:"session"`
	Transport        string     `json:"transport,omitempty"`
	TransientCapable bool       `json:"transient_capable"`
}

type ResolvedUserSessions struct {
	User     UserRef              `json:"user"`
	Presence []OnlineNodePresence `json:"presence,omitempty"`
	Sessions []ResolvedSession    `json:"sessions,omitempty"`
}

type MessageTrimStatus struct {
	TrimmedTotal  int64  `json:"trimmed_total"`
	LastTrimmedAt string `json:"last_trimmed_at,omitempty"`
}

type ProjectionStatus struct {
	PendingTotal int64  `json:"pending_total"`
	LastFailedAt string `json:"last_failed_at,omitempty"`
}

type PeerOriginStatus struct {
	OriginNodeID      int64  `json:"origin_node_id"`
	AckedEventID      int64  `json:"acked_event_id"`
	AppliedEventID    int64  `json:"applied_event_id"`
	UnconfirmedEvents int64  `json:"unconfirmed_events"`
	CursorUpdatedAt   string `json:"cursor_updated_at,omitempty"`
	RemoteLastEventID uint64 `json:"remote_last_event_id"`
	PendingCatchup    bool   `json:"pending_catchup"`
}

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

type DeleteUserResult struct {
	Status string  `json:"status"`
	User   UserRef `json:"user"`
}

type LoginInfo struct {
	User            User
	ProtocolVersion string
	SessionRef      SessionRef
}

type SendMessageInput struct {
	Target UserRef
	Body   []byte
}

type SendPacketInput struct {
	Target        UserRef
	Body          []byte
	DeliveryMode  DeliveryMode
	TargetSession SessionRef
}

type CreateUserRequest struct {
	Username    string        `json:"username"`
	Password    PasswordInput `json:"password,omitempty"`
	ProfileJSON []byte        `json:"profile_json,omitempty"`
	Role        string        `json:"role"`
}

type UpdateUserRequest struct {
	Username    *string        `json:"username,omitempty"`
	Password    *PasswordInput `json:"password,omitempty"`
	ProfileJSON *[]byte        `json:"profile_json,omitempty"`
	Role        *string        `json:"role,omitempty"`
}

func (m Message) Cursor() MessageCursor {
	return MessageCursor{NodeID: m.NodeID, Seq: m.Seq}
}

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

func validateMetadataKey(key string) error {
	return validateMetadataKeyFragment(key, "key", false)
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

func (r SessionRef) IsZero() bool {
	return r.ServingNodeID == 0 && r.SessionID == ""
}

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
		Role:           in.Role,
		ProfileJSON:    append([]byte(nil), in.ProfileJson...),
		SystemReserved: in.SystemReserved,
		CreatedAt:      in.CreatedAt,
		UpdatedAt:      in.UpdatedAt,
		OriginNodeID:   in.OriginNodeId,
	}
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
		NodeID:   in.NodeId,
		UserID:   in.UserId,
		Username: in.Username,
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
