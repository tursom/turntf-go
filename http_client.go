package turntf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// HTTPClient 是基于 HTTP REST API 的客户端，提供与 WebSocket 客户端相同功能子集的 HTTP 接口。
// 所有 HTTP 方法都需要传入认证 token（登录接口除外）。
type HTTPClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

// NewHTTPClient 创建并返回一个新的 HTTPClient 实例。
// baseURL 为服务端的基础地址（如 "http://localhost:8080"），末尾的斜杠会被自动去除。
func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{BaseURL: strings.TrimRight(baseURL, "/")}
}

type httpLoginRequest struct {
	NodeID    int64  `json:"node_id,omitempty"`
	UserID    int64  `json:"user_id,omitempty"`
	LoginName string `json:"login_name,omitempty"`
	Password  string `json:"password"`
}

type httpLoginResponse struct {
	Token string `json:"token"`
}

type httpAttachmentRequest struct {
	ConfigJSON json.RawMessage `json:"config_json"`
}

type httpMessageRequest struct {
	Body         []byte `json:"body"`
	DeliveryKind string `json:"delivery_kind,omitempty"`
	DeliveryMode string `json:"delivery_mode,omitempty"`
}

type httpCreateUserRequest struct {
	Username  string          `json:"username"`
	LoginName string          `json:"login_name,omitempty"`
	Password  string          `json:"password,omitempty"`
	Profile   json.RawMessage `json:"profile,omitempty"`
	Role      string          `json:"role"`
}

type httpUserResponse struct {
	NodeID         int64           `json:"node_id"`
	UserID         int64           `json:"user_id"`
	Username       string          `json:"username"`
	LoginName      string          `json:"login_name"`
	Role           string          `json:"role"`
	Profile        json.RawMessage `json:"profile,omitempty"`
	ProfileJSON    json.RawMessage `json:"profile_json,omitempty"`
	SystemReserved bool            `json:"system_reserved"`
	CreatedAt      string          `json:"created_at,omitempty"`
	UpdatedAt      string          `json:"updated_at,omitempty"`
	OriginNodeID   int64           `json:"origin_node_id"`
}

type httpMessageResponse struct {
	Recipient    UserRef `json:"recipient"`
	NodeID       int64   `json:"node_id"`
	Seq          int64   `json:"seq"`
	Sender       UserRef `json:"sender"`
	Body         []byte  `json:"body"`
	CreatedAt    string  `json:"created_at,omitempty"`
	CreatedAtHLC string  `json:"created_at_hlc,omitempty"`
}

type httpMessagesResponse struct {
	Items []httpMessageResponse `json:"items"`
	Count int                   `json:"count"`
}

func (r *httpMessagesResponse) UnmarshalJSON(data []byte) error {
	if isJSONArray(data) {
		return json.Unmarshal(data, &r.Items)
	}
	type alias httpMessagesResponse
	return json.Unmarshal(data, (*alias)(r))
}

type httpClusterNodesResponse struct {
	Nodes []ClusterNode `json:"nodes"`
}

func (r *httpClusterNodesResponse) UnmarshalJSON(data []byte) error {
	if isJSONArray(data) {
		return json.Unmarshal(data, &r.Nodes)
	}
	type alias httpClusterNodesResponse
	return json.Unmarshal(data, (*alias)(r))
}

type httpNodeLoggedInUsersResponse struct {
	TargetNodeID int64          `json:"target_node_id"`
	Items        []LoggedInUser `json:"items"`
	Count        int            `json:"count"`
}

func (r *httpNodeLoggedInUsersResponse) UnmarshalJSON(data []byte) error {
	if isJSONArray(data) {
		return json.Unmarshal(data, &r.Items)
	}
	type alias httpNodeLoggedInUsersResponse
	return json.Unmarshal(data, (*alias)(r))
}

type httpUsersResponse struct {
	Items []httpUserResponse `json:"items"`
	Count int                `json:"count"`
}

func (r *httpUsersResponse) UnmarshalJSON(data []byte) error {
	if isJSONArray(data) {
		return json.Unmarshal(data, &r.Items)
	}
	type alias httpUsersResponse
	return json.Unmarshal(data, (*alias)(r))
}

type httpAttachmentsResponse struct {
	Items []Attachment `json:"items"`
	Count int          `json:"count"`
}

func (r *httpAttachmentsResponse) UnmarshalJSON(data []byte) error {
	if isJSONArray(data) {
		return json.Unmarshal(data, &r.Items)
	}
	type alias httpAttachmentsResponse
	return json.Unmarshal(data, (*alias)(r))
}

type httpDeleteUserResponse struct {
	Status string `json:"status"`
	NodeID int64  `json:"node_id"`
	UserID int64  `json:"user_id"`
}

type httpEventsResponse struct {
	Items []Event `json:"items"`
	Count int     `json:"count"`
}

type httpUpdateUserRequest struct {
	Username  *string          `json:"username,omitempty"`
	LoginName *string          `json:"login_name,omitempty"`
	Password  *string          `json:"password,omitempty"`
	Profile   *json.RawMessage `json:"profile,omitempty"`
	Role      *string          `json:"role,omitempty"`
}

func (r *httpEventsResponse) UnmarshalJSON(data []byte) error {
	if isJSONArray(data) {
		return json.Unmarshal(data, &r.Items)
	}
	type alias httpEventsResponse
	return json.Unmarshal(data, (*alias)(r))
}

func (c *HTTPClient) client() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

// Login 通过 HTTP 接口使用明文密码登录，通过节点 ID 和用户 ID 标识用户。
// 返回认证 token，后续请求需在 Header 中携带 Bearer token。
func (c *HTTPClient) Login(ctx context.Context, nodeID, userID int64, password string) (string, error) {
	input, err := PlainPassword(password)
	if err != nil {
		return "", err
	}
	return c.LoginWithPassword(ctx, nodeID, userID, input)
}

// LoginWithPassword 通过 HTTP 接口使用 PasswordInput 密码登录，通过节点 ID 和用户 ID 标识用户。
// password 支持明文和已哈希两种模式。
func (c *HTTPClient) LoginWithPassword(ctx context.Context, nodeID, userID int64, password PasswordInput) (string, error) {
	return c.loginWithRequest(ctx, httpLoginRequest{NodeID: nodeID, UserID: userID}, password)
}

// LoginByLoginName 通过 HTTP 接口使用明文密码和登录名登录。
// loginName 为用户的登录名，而非 node_id + user_id 组合。
func (c *HTTPClient) LoginByLoginName(ctx context.Context, loginName, password string) (string, error) {
	input, err := PlainPassword(password)
	if err != nil {
		return "", err
	}
	return c.LoginByLoginNameWithPassword(ctx, loginName, input)
}

// LoginByLoginNameWithPassword 通过 HTTP 接口使用 PasswordInput 密码和登录名登录。
// password 支持明文和已哈希两种模式。
func (c *HTTPClient) LoginByLoginNameWithPassword(ctx context.Context, loginName string, password PasswordInput) (string, error) {
	return c.loginWithRequest(ctx, httpLoginRequest{LoginName: loginName}, password)
}

func (c *HTTPClient) loginWithRequest(ctx context.Context, req httpLoginRequest, password PasswordInput) (string, error) {
	var resp httpLoginResponse
	normalizedLoginName, err := validateLoginSelector(req.NodeID, req.UserID, req.LoginName)
	if err != nil {
		return "", err
	}
	if err := password.Validate(); err != nil {
		return "", fmt.Errorf("invalid password: %w", err)
	}
	req.LoginName = normalizedLoginName
	req.Password = password.WireValue()
	err = c.doJSON(ctx, http.MethodPost, "/auth/login", "", req, &resp, http.StatusOK)
	if err != nil {
		return "", err
	}
	if resp.Token == "" {
		return "", &ProtocolError{Message: "empty token in login response"}
	}
	return resp.Token, nil
}

// CreateUser 通过 HTTP 接口创建用户或频道。
// token 为认证令牌，req 包含用户信息（用户名、角色为必填）。
func (c *HTTPClient) CreateUser(ctx context.Context, token string, req CreateUserRequest) (User, error) {
	var resp httpUserResponse
	if req.Username == "" {
		return User{}, fmt.Errorf("username is required")
	}
	if req.Role == "" {
		return User{}, fmt.Errorf("role is required")
	}
	err := c.doJSON(ctx, http.MethodPost, "/users", token, httpCreateUserRequest{
		Username:  req.Username,
		LoginName: req.LoginName,
		Password:  req.Password.WireValue(),
		Profile:   json.RawMessage(req.ProfileJSON),
		Role:      req.Role,
	}, &resp, http.StatusCreated, http.StatusOK)
	if err != nil {
		return User{}, err
	}
	return userFromHTTP(resp), nil
}

// CreateChannel 通过 HTTP 接口创建频道。与 CreateUser 类似，但 Role 默认设为 "channel"。
// token 为认证令牌，req 中 Role 为空时会自动设置为 "channel"。
func (c *HTTPClient) CreateChannel(ctx context.Context, token string, req CreateUserRequest) (User, error) {
	if req.Role == "" {
		req.Role = "channel"
	}
	return c.CreateUser(ctx, token, req)
}

// ListUsers 通过 HTTP 接口查询当前用户可通讯的活跃用户列表。
// req 中可选的 Name 会在可见用户集合内做大小写不敏感子串匹配；UID 会按 node_id:user_id 精确过滤。
func (c *HTTPClient) ListUsers(ctx context.Context, token string, req ListUsersRequest) ([]User, error) {
	req = req.normalized()
	if err := req.validate(); err != nil {
		return nil, err
	}

	values := url.Values{}
	if req.Name != "" {
		values.Set("name", req.Name)
	}
	if !req.UID.IsZero() {
		values.Set("uid", fmt.Sprintf("%d:%d", req.UID.NodeID, req.UID.UserID))
	}

	path := "/users"
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}

	var resp httpUsersResponse
	err := c.doJSON(ctx, http.MethodGet, path, token, nil, &resp, http.StatusOK)
	return usersFromHTTP(resp.Items), err
}

// CreateSubscription 通过 HTTP 接口创建频道订阅关系。订阅者将收到频道的消息推送。
// userRef 为订阅者，channelRef 为要订阅的频道。
func (c *HTTPClient) CreateSubscription(ctx context.Context, token string, userRef, channelRef UserRef) error {
	_, err := c.UpsertAttachment(ctx, token, userRef, channelRef, AttachmentTypeChannelSubscription, []byte("{}"))
	return err
}

// ListMessages 通过 HTTP 接口查询指定用户的消息列表。limit 控制返回的消息数量上限。
// token 为认证令牌，target 指定消息所属用户。
// peerNodeID 和 peerUserID 为可选的会话过滤参数，同时指定时将仅返回与指定 Peer 相关的消息。
// target 的 node_id/user_id 允许为 0（服务端将其解析为"当前用户"）。
func (c *HTTPClient) ListMessages(ctx context.Context, token string, target UserRef, limit int, peerNodeID, peerUserID int64) ([]Message, error) {
	values := url.Values{}
	if limit > 0 {
		values.Set("limit", strconv.Itoa(limit))
	}
	if peerNodeID != 0 || peerUserID != 0 {
		values.Set("peer_node_id", strconv.FormatInt(peerNodeID, 10))
		values.Set("peer_user_id", strconv.FormatInt(peerUserID, 10))
	}
	path := fmt.Sprintf("/nodes/%d/users/%d/messages", target.NodeID, target.UserID)
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}

	var resp httpMessagesResponse
	err := c.doJSON(ctx, http.MethodGet, path, token, nil, &resp, http.StatusOK)
	return messagesFromHTTP(resp.Items), err
}

// PostMessage 通过 HTTP 接口向目标用户发送一条持久化消息。
// body 为消息内容的字节数组，不能为空。返回已保存的消息详情。
func (c *HTTPClient) PostMessage(ctx context.Context, token string, target UserRef, body []byte) (Message, error) {
	var resp httpMessageResponse
	if err := target.validate(); err != nil {
		return Message{}, fmt.Errorf("invalid target: %w", err)
	}
	if len(body) == 0 {
		return Message{}, fmt.Errorf("body is required")
	}
	path := fmt.Sprintf("/nodes/%d/users/%d/messages", target.NodeID, target.UserID)
	err := c.doJSON(ctx, http.MethodPost, path, token, httpMessageRequest{
		Body: body,
	}, &resp, http.StatusCreated, http.StatusOK)
	if err != nil {
		return Message{}, err
	}
	return messageFromHTTP(resp), nil
}

// PostPacket 通过 HTTP 接口发送瞬时消息（非持久化）。消息不会被存储，适合通知类场景。
// targetNodeID 为目标节点 ID，relayTarget 为目标用户，mode 为投递模式。
func (c *HTTPClient) PostPacket(ctx context.Context, token string, targetNodeID int64, relayTarget UserRef, body []byte, mode DeliveryMode) error {
	if targetNodeID == 0 {
		return fmt.Errorf("target node_id is required")
	}
	if err := relayTarget.validate(); err != nil {
		return fmt.Errorf("invalid relay_target: %w", err)
	}
	if len(body) == 0 {
		return fmt.Errorf("body is required")
	}
	if err := mode.validatePacketMode(); err != nil {
		return err
	}

	if targetNodeID != relayTarget.NodeID {
		return fmt.Errorf("target node ID %d does not match target user node_id %d", targetNodeID, relayTarget.NodeID)
	}

	path := fmt.Sprintf("/nodes/%d/users/%d/messages", relayTarget.NodeID, relayTarget.UserID)
	return c.doJSON(ctx, http.MethodPost, path, token, httpMessageRequest{
		Body:         body,
		DeliveryKind: "transient",
		DeliveryMode: string(mode),
	}, nil, http.StatusAccepted)
}

// ListClusterNodes 通过 HTTP 接口查询集群中的所有节点列表。
func (c *HTTPClient) ListClusterNodes(ctx context.Context, token string) ([]ClusterNode, error) {
	var resp httpClusterNodesResponse
	err := c.doJSON(ctx, http.MethodGet, "/cluster/nodes", token, nil, &resp, http.StatusOK)
	return resp.Nodes, err
}

// ListNodeLoggedInUsers 通过 HTTP 接口查询指定节点上当前已登录的用户列表。
// nodeID 为目标节点 ID，不能为 0。
func (c *HTTPClient) ListNodeLoggedInUsers(ctx context.Context, token string, nodeID int64) ([]LoggedInUser, error) {
	if nodeID == 0 {
		return nil, fmt.Errorf("node_id is required")
	}

	var resp httpNodeLoggedInUsersResponse
	path := fmt.Sprintf("/cluster/nodes/%d/logged-in-users", nodeID)
	err := c.doJSON(ctx, http.MethodGet, path, token, nil, &resp, http.StatusOK)
	return resp.Items, err
}

// BlockUser 通过 HTTP 接口将指定用户加入黑名单。被拉黑的用户无法向 owner 发送消息。
func (c *HTTPClient) BlockUser(ctx context.Context, token string, owner, blocked UserRef) (BlacklistEntry, error) {
	attachment, err := c.UpsertAttachment(ctx, token, owner, blocked, AttachmentTypeUserBlacklist, []byte("{}"))
	if err != nil {
		return BlacklistEntry{}, err
	}
	return BlacklistEntry{
		Owner:        attachment.Owner,
		Blocked:      attachment.Subject,
		BlockedAt:    attachment.AttachedAt,
		DeletedAt:    attachment.DeletedAt,
		OriginNodeID: attachment.OriginNodeID,
	}, nil
}

// UnblockUser 通过 HTTP 接口将指定用户移出黑名单。
func (c *HTTPClient) UnblockUser(ctx context.Context, token string, owner, blocked UserRef) (BlacklistEntry, error) {
	attachment, err := c.DeleteAttachment(ctx, token, owner, blocked, AttachmentTypeUserBlacklist)
	if err != nil {
		return BlacklistEntry{}, err
	}
	return BlacklistEntry{
		Owner:        attachment.Owner,
		Blocked:      attachment.Subject,
		BlockedAt:    attachment.AttachedAt,
		DeletedAt:    attachment.DeletedAt,
		OriginNodeID: attachment.OriginNodeID,
	}, nil
}

// ListBlockedUsers 通过 HTTP 接口查询指定用户的黑名单列表。
func (c *HTTPClient) ListBlockedUsers(ctx context.Context, token string, owner UserRef) ([]BlacklistEntry, error) {
	attachments, err := c.ListAttachments(ctx, token, owner, AttachmentTypeUserBlacklist)
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

// GetUser 通过 HTTP 接口获取指定用户的详细信息。
func (c *HTTPClient) GetUser(ctx context.Context, token string, target UserRef) (User, error) {
	if err := target.validate(); err != nil {
		return User{}, fmt.Errorf("invalid target: %w", err)
	}
	var resp httpUserResponse
	path := fmt.Sprintf("/nodes/%d/users/%d", target.NodeID, target.UserID)
	err := c.doJSON(ctx, http.MethodGet, path, token, nil, &resp, http.StatusOK)
	if err != nil {
		return User{}, err
	}
	return userFromHTTP(resp), nil
}

// UpdateUser 通过 HTTP 接口更新用户信息。仅非 nil 字段会被更新。
// login_name 为空字符串时表示解除登录名绑定。频道（role="channel"）不支持设置 login_name。
func (c *HTTPClient) UpdateUser(ctx context.Context, token string, target UserRef, req UpdateUserRequest) (User, error) {
	if err := target.validate(); err != nil {
		return User{}, fmt.Errorf("invalid target: %w", err)
	}
	if req.Role != nil && *req.Role == "channel" && req.LoginName != nil && *req.LoginName != "" {
		return User{}, fmt.Errorf("channel users cannot have a login_name")
	}
	body := httpUpdateUserRequest{
		Username:  req.Username,
		LoginName: req.LoginName,
		Role:      req.Role,
	}
	if req.Password != nil {
		v := req.Password.WireValue()
		body.Password = &v
	}
	if req.ProfileJSON != nil {
		raw := json.RawMessage(append([]byte{}, *req.ProfileJSON...))
		body.Profile = &raw
	}
	var resp httpUserResponse
	path := fmt.Sprintf("/nodes/%d/users/%d", target.NodeID, target.UserID)
	err := c.doJSON(ctx, http.MethodPatch, path, token, body, &resp, http.StatusOK)
	if err != nil {
		return User{}, err
	}
	return userFromHTTP(resp), nil
}

// DeleteUser 通过 HTTP 接口删除指定用户（软删除）。
func (c *HTTPClient) DeleteUser(ctx context.Context, token string, target UserRef) (DeleteUserResult, error) {
	if err := target.validate(); err != nil {
		return DeleteUserResult{}, fmt.Errorf("invalid target: %w", err)
	}
	var resp httpDeleteUserResponse
	path := fmt.Sprintf("/nodes/%d/users/%d", target.NodeID, target.UserID)
	err := c.doJSON(ctx, http.MethodDelete, path, token, nil, &resp, http.StatusOK)
	if err != nil {
		return DeleteUserResult{}, err
	}
	return DeleteUserResult{
		Status: resp.Status,
		User:   UserRef{NodeID: resp.NodeID, UserID: resp.UserID},
	}, nil
}

// ListEvents 通过 HTTP 接口查询事件日志，支持分页游标。
// after 为起始事件序列号（不含），limit 控制返回数量上限。
func (c *HTTPClient) ListEvents(ctx context.Context, token string, after int64, limit int) ([]Event, error) {
	values := url.Values{}
	if after > 0 {
		values.Set("after", strconv.FormatInt(after, 10))
	}
	if limit > 0 {
		values.Set("limit", strconv.Itoa(limit))
	}
	path := "/events"
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var resp httpEventsResponse
	err := c.doJSON(ctx, http.MethodGet, path, token, nil, &resp, http.StatusOK)
	return resp.Items, err
}

// OperationsStatus 通过 HTTP 接口查询节点运行状态，包含消息窗口、写闸门、投影等指标。
func (c *HTTPClient) OperationsStatus(ctx context.Context, token string) (OperationsStatus, error) {
	var status OperationsStatus
	err := c.doJSON(ctx, http.MethodGet, "/ops/status", token, nil, &status, http.StatusOK)
	return status, err
}

// Metrics 通过 HTTP 接口获取 Prometheus 格式的监控指标文本。
func (c *HTTPClient) Metrics(ctx context.Context, token string) (string, error) {
	return c.doText(ctx, "/metrics", token, http.StatusOK)
}

// GetUserMetadata 通过 HTTP 接口获取指定用户的指定元数据键值。
// key 为元数据键名，仅允许字母、数字、点、下划线、冒号和短横线。
func (c *HTTPClient) GetUserMetadata(ctx context.Context, token string, owner UserRef, key string) (UserMetadata, error) {
	var metadata UserMetadata
	if err := owner.validate(); err != nil {
		return metadata, fmt.Errorf("invalid owner: %w", err)
	}
	if err := validateMetadataKey(key); err != nil {
		return metadata, err
	}

	err := c.doJSON(ctx, http.MethodGet, userMetadataPath(owner, key), token, nil, &metadata, http.StatusOK)
	if err != nil {
		return UserMetadata{}, err
	}
	return userMetadataFromHTTP(metadata), nil
}

// UpsertUserMetadata 通过 HTTP 接口创建或更新用户元数据。
// key 为元数据键名，req 包含新的值和可选的过期时间。
func (c *HTTPClient) UpsertUserMetadata(ctx context.Context, token string, owner UserRef, key string, req UpsertUserMetadataRequest) (UserMetadata, error) {
	var metadata UserMetadata
	if err := owner.validate(); err != nil {
		return metadata, fmt.Errorf("invalid owner: %w", err)
	}
	if err := validateMetadataKey(key); err != nil {
		return metadata, err
	}

	err := c.doJSON(ctx, http.MethodPut, userMetadataPath(owner, key), token, UpsertUserMetadataRequest{
		Value:     append([]byte{}, req.Value...),
		ExpiresAt: req.ExpiresAt,
	}, &metadata, http.StatusCreated, http.StatusOK)
	if err != nil {
		return UserMetadata{}, err
	}
	return userMetadataFromHTTP(metadata), nil
}

// DeleteUserMetadata 通过 HTTP 接口删除用户元数据（软删除）。
func (c *HTTPClient) DeleteUserMetadata(ctx context.Context, token string, owner UserRef, key string) (UserMetadata, error) {
	var metadata UserMetadata
	if err := owner.validate(); err != nil {
		return metadata, fmt.Errorf("invalid owner: %w", err)
	}
	if err := validateMetadataKey(key); err != nil {
		return metadata, err
	}

	err := c.doJSON(ctx, http.MethodDelete, userMetadataPath(owner, key), token, nil, &metadata, http.StatusOK)
	if err != nil {
		return UserMetadata{}, err
	}
	return userMetadataFromHTTP(metadata), nil
}

// ScanUserMetadata 通过 HTTP 接口按前缀分页扫描用户元数据。
// req 包含前缀过滤条件、分页游标和每页限制数量。
func (c *HTTPClient) ScanUserMetadata(ctx context.Context, token string, owner UserRef, req ScanUserMetadataRequest) (UserMetadataPage, error) {
	if err := owner.validate(); err != nil {
		return UserMetadataPage{}, fmt.Errorf("invalid owner: %w", err)
	}
	if err := req.validate(); err != nil {
		return UserMetadataPage{}, err
	}

	values := url.Values{}
	if req.Prefix != "" {
		values.Set("prefix", req.Prefix)
	}
	if req.After != "" {
		values.Set("after", req.After)
	}
	if req.Limit > 0 {
		values.Set("limit", strconv.Itoa(req.Limit))
	}

	path := fmt.Sprintf("/nodes/%d/users/%d/metadata", owner.NodeID, owner.UserID)
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}

	var page UserMetadataPage
	err := c.doJSON(ctx, http.MethodGet, path, token, nil, &page, http.StatusOK)
	if err != nil {
		return UserMetadataPage{}, err
	}
	return userMetadataPageFromHTTP(page), nil
}

// UpsertAttachment 通过 HTTP 接口创建或更新用户之间的关联关系（如频道订阅、黑名单等）。
// attachmentType 指定关系类型，configJSON 为可选的配置 JSON。
func (c *HTTPClient) UpsertAttachment(ctx context.Context, token string, owner, subject UserRef, attachmentType AttachmentType, configJSON []byte) (Attachment, error) {
	var attachment Attachment
	if err := owner.validate(); err != nil {
		return attachment, fmt.Errorf("invalid owner: %w", err)
	}
	if err := subject.validate(); err != nil {
		return attachment, fmt.Errorf("invalid subject: %w", err)
	}
	path := fmt.Sprintf("/nodes/%d/users/%d/attachments/%s/%d/%d", owner.NodeID, owner.UserID, attachmentType, subject.NodeID, subject.UserID)
	err := c.doJSON(ctx, http.MethodPut, path, token, httpAttachmentRequest{
		ConfigJSON: append([]byte(nil), configJSON...),
	}, &attachment, http.StatusCreated, http.StatusOK)
	return attachment, err
}

// DeleteAttachment 通过 HTTP 接口删除用户之间的关联关系。
func (c *HTTPClient) DeleteAttachment(ctx context.Context, token string, owner, subject UserRef, attachmentType AttachmentType) (Attachment, error) {
	var attachment Attachment
	if err := owner.validate(); err != nil {
		return attachment, fmt.Errorf("invalid owner: %w", err)
	}
	if err := subject.validate(); err != nil {
		return attachment, fmt.Errorf("invalid subject: %w", err)
	}
	path := fmt.Sprintf("/nodes/%d/users/%d/attachments/%s/%d/%d", owner.NodeID, owner.UserID, attachmentType, subject.NodeID, subject.UserID)
	err := c.doJSON(ctx, http.MethodDelete, path, token, nil, &attachment, http.StatusOK)
	return attachment, err
}

// ListAttachments 通过 HTTP 接口查询用户指定类型的所有关联关系列表。
func (c *HTTPClient) ListAttachments(ctx context.Context, token string, owner UserRef, attachmentType AttachmentType) ([]Attachment, error) {
	if err := owner.validate(); err != nil {
		return nil, fmt.Errorf("invalid owner: %w", err)
	}
	var resp httpAttachmentsResponse
	path := fmt.Sprintf("/nodes/%d/users/%d/attachments", owner.NodeID, owner.UserID)
	if attachmentType != "" {
		path += "?attachment_type=" + url.QueryEscape(string(attachmentType))
	}
	err := c.doJSON(ctx, http.MethodGet, path, token, nil, &resp, http.StatusOK)
	return resp.Items, err
}

func (c *HTTPClient) doJSON(ctx context.Context, method, path, token string, reqBody any, out any, wantStatuses ...int) error {
	fullURL := strings.TrimRight(c.BaseURL, "/") + path
	var body io.Reader
	if reqBody != nil {
		payload, err := json.Marshal(reqBody)
		if err != nil {
			return err
		}
		body = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, body)
	if err != nil {
		return err
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.client().Do(req)
	if err != nil {
		return &ConnectionError{Op: method + " " + path, Err: err}
	}
	defer resp.Body.Close()

	if !statusAllowed(resp.StatusCode, wantStatuses) {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return &ProtocolError{Message: fmt.Sprintf("unexpected HTTP status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))}
	}
	if out == nil {
		io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *HTTPClient) doText(ctx context.Context, path, token string, wantStatuses ...int) (string, error) {
	fullURL := strings.TrimRight(c.BaseURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return "", err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.client().Do(req)
	if err != nil {
		return "", &ConnectionError{Op: "GET " + path, Err: err}
	}
	defer resp.Body.Close()
	if !statusAllowed(resp.StatusCode, wantStatuses) {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return "", &ProtocolError{Message: fmt.Sprintf("unexpected HTTP status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))}
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", &ConnectionError{Op: "GET " + path, Err: err}
	}
	return string(data), nil
}

func statusAllowed(status int, allowed []int) bool {
	if len(allowed) == 0 {
		return status == http.StatusOK
	}
	for _, item := range allowed {
		if status == item {
			return true
		}
	}
	return false
}

func isJSONArray(data []byte) bool {
	trimmed := bytes.TrimSpace(data)
	return len(trimmed) > 0 && trimmed[0] == '['
}

func userFromHTTP(in httpUserResponse) User {
	profile := in.Profile
	if len(profile) == 0 {
		profile = in.ProfileJSON
	}
	return User{
		NodeID:         in.NodeID,
		UserID:         in.UserID,
		Username:       in.Username,
		LoginName:      in.LoginName,
		Role:           in.Role,
		ProfileJSON:    append([]byte(nil), profile...),
		SystemReserved: in.SystemReserved,
		CreatedAt:      in.CreatedAt,
		UpdatedAt:      in.UpdatedAt,
		OriginNodeID:   in.OriginNodeID,
	}
}

func usersFromHTTP(items []httpUserResponse) []User {
	out := make([]User, 0, len(items))
	for _, item := range items {
		out = append(out, userFromHTTP(item))
	}
	return out
}

func messageFromHTTP(in httpMessageResponse) Message {
	createdAt := in.CreatedAtHLC
	if createdAt == "" {
		createdAt = in.CreatedAt
	}
	return Message{
		Recipient:    in.Recipient,
		NodeID:       in.NodeID,
		Seq:          in.Seq,
		Sender:       in.Sender,
		Body:         append([]byte(nil), in.Body...),
		CreatedAtHLC: createdAt,
	}
}

func messagesFromHTTP(items []httpMessageResponse) []Message {
	out := make([]Message, 0, len(items))
	for _, item := range items {
		out = append(out, messageFromHTTP(item))
	}
	return out
}

func userMetadataPath(owner UserRef, key string) string {
	return fmt.Sprintf("/nodes/%d/users/%d/metadata/%s", owner.NodeID, owner.UserID, url.PathEscape(key))
}

func userMetadataFromHTTP(in UserMetadata) UserMetadata {
	in.Value = append([]byte(nil), in.Value...)
	return in
}

func userMetadataPageFromHTTP(in UserMetadataPage) UserMetadataPage {
	out := UserMetadataPage{
		Items:     make([]UserMetadata, 0, len(in.Items)),
		Count:     in.Count,
		NextAfter: in.NextAfter,
	}
	for _, item := range in.Items {
		out.Items = append(out.Items, userMetadataFromHTTP(item))
	}
	return out
}
