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

type HTTPClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{BaseURL: strings.TrimRight(baseURL, "/")}
}

type httpLoginRequest struct {
	NodeID   int64  `json:"node_id"`
	UserID   int64  `json:"user_id"`
	Password string `json:"password"`
}

type httpLoginResponse struct {
	Token string `json:"token"`
}

type httpSubscriptionRequest struct {
	ChannelNodeID int64 `json:"channel_node_id"`
	ChannelUserID int64 `json:"channel_user_id"`
}

type httpBlacklistRequest struct {
	BlockedNodeID int64 `json:"blocked_node_id"`
	BlockedUserID int64 `json:"blocked_user_id"`
}

type httpMessageRequest struct {
	Body         []byte `json:"body"`
	DeliveryKind string `json:"delivery_kind,omitempty"`
	DeliveryMode string `json:"delivery_mode,omitempty"`
}

type httpCreateUserRequest struct {
	Username string          `json:"username"`
	Password string          `json:"password,omitempty"`
	Profile  json.RawMessage `json:"profile,omitempty"`
	Role     string          `json:"role"`
}

type httpUserResponse struct {
	NodeID         int64           `json:"node_id"`
	UserID         int64           `json:"user_id"`
	Username       string          `json:"username"`
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

type httpBlockedUsersResponse struct {
	Items []BlacklistEntry `json:"items"`
	Count int              `json:"count"`
}

func (r *httpBlockedUsersResponse) UnmarshalJSON(data []byte) error {
	if isJSONArray(data) {
		return json.Unmarshal(data, &r.Items)
	}
	type alias httpBlockedUsersResponse
	return json.Unmarshal(data, (*alias)(r))
}

func (c *HTTPClient) client() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

func (c *HTTPClient) Login(ctx context.Context, nodeID, userID int64, password string) (string, error) {
	input, err := PlainPassword(password)
	if err != nil {
		return "", err
	}
	return c.LoginWithPassword(ctx, nodeID, userID, input)
}

func (c *HTTPClient) LoginWithPassword(ctx context.Context, nodeID, userID int64, password PasswordInput) (string, error) {
	var resp httpLoginResponse
	if err := password.Validate(); err != nil {
		return "", fmt.Errorf("invalid password: %w", err)
	}
	err := c.doJSON(ctx, http.MethodPost, "/auth/login", "", httpLoginRequest{
		NodeID:   nodeID,
		UserID:   userID,
		Password: password.WireValue(),
	}, &resp, http.StatusOK)
	if err != nil {
		return "", err
	}
	if resp.Token == "" {
		return "", &ProtocolError{Message: "empty token in login response"}
	}
	return resp.Token, nil
}

func (c *HTTPClient) CreateUser(ctx context.Context, token string, req CreateUserRequest) (User, error) {
	var resp httpUserResponse
	if req.Username == "" {
		return User{}, fmt.Errorf("username is required")
	}
	if req.Role == "" {
		return User{}, fmt.Errorf("role is required")
	}
	err := c.doJSON(ctx, http.MethodPost, "/users", token, httpCreateUserRequest{
		Username: req.Username,
		Password: req.Password.WireValue(),
		Profile:  json.RawMessage(req.ProfileJSON),
		Role:     req.Role,
	}, &resp, http.StatusCreated, http.StatusOK)
	if err != nil {
		return User{}, err
	}
	return userFromHTTP(resp), nil
}

func (c *HTTPClient) CreateChannel(ctx context.Context, token string, req CreateUserRequest) (User, error) {
	if req.Role == "" {
		req.Role = "channel"
	}
	return c.CreateUser(ctx, token, req)
}

func (c *HTTPClient) CreateSubscription(ctx context.Context, token string, userRef, channelRef UserRef) error {
	if err := userRef.validate(); err != nil {
		return fmt.Errorf("invalid user ref: %w", err)
	}
	if err := channelRef.validate(); err != nil {
		return fmt.Errorf("invalid channel ref: %w", err)
	}
	path := fmt.Sprintf("/nodes/%d/users/%d/subscriptions", userRef.NodeID, userRef.UserID)
	req := httpSubscriptionRequest{
		ChannelNodeID: channelRef.NodeID,
		ChannelUserID: channelRef.UserID,
	}
	return c.doJSON(ctx, http.MethodPost, path, token, req, nil, http.StatusCreated, http.StatusOK)
}

func (c *HTTPClient) ListMessages(ctx context.Context, token string, target UserRef, limit int) ([]Message, error) {
	if err := target.validate(); err != nil {
		return nil, fmt.Errorf("invalid target: %w", err)
	}
	values := url.Values{}
	if limit > 0 {
		values.Set("limit", strconv.Itoa(limit))
	}
	path := fmt.Sprintf("/nodes/%d/users/%d/messages", target.NodeID, target.UserID)
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}

	var resp httpMessagesResponse
	err := c.doJSON(ctx, http.MethodGet, path, token, nil, &resp, http.StatusOK)
	return messagesFromHTTP(resp.Items), err
}

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

func (c *HTTPClient) ListClusterNodes(ctx context.Context, token string) ([]ClusterNode, error) {
	var resp httpClusterNodesResponse
	err := c.doJSON(ctx, http.MethodGet, "/cluster/nodes", token, nil, &resp, http.StatusOK)
	return resp.Nodes, err
}

func (c *HTTPClient) ListNodeLoggedInUsers(ctx context.Context, token string, nodeID int64) ([]LoggedInUser, error) {
	if nodeID == 0 {
		return nil, fmt.Errorf("node_id is required")
	}

	var resp httpNodeLoggedInUsersResponse
	path := fmt.Sprintf("/cluster/nodes/%d/logged-in-users", nodeID)
	err := c.doJSON(ctx, http.MethodGet, path, token, nil, &resp, http.StatusOK)
	return resp.Items, err
}

func (c *HTTPClient) BlockUser(ctx context.Context, token string, owner, blocked UserRef) (BlacklistEntry, error) {
	var entry BlacklistEntry
	if err := owner.validate(); err != nil {
		return entry, fmt.Errorf("invalid owner: %w", err)
	}
	if err := blocked.validate(); err != nil {
		return entry, fmt.Errorf("invalid blocked user: %w", err)
	}

	path := fmt.Sprintf("/nodes/%d/users/%d/blacklist", owner.NodeID, owner.UserID)
	err := c.doJSON(ctx, http.MethodPost, path, token, httpBlacklistRequest{
		BlockedNodeID: blocked.NodeID,
		BlockedUserID: blocked.UserID,
	}, &entry, http.StatusCreated, http.StatusOK)
	return entry, err
}

func (c *HTTPClient) UnblockUser(ctx context.Context, token string, owner, blocked UserRef) (BlacklistEntry, error) {
	var entry BlacklistEntry
	if err := owner.validate(); err != nil {
		return entry, fmt.Errorf("invalid owner: %w", err)
	}
	if err := blocked.validate(); err != nil {
		return entry, fmt.Errorf("invalid blocked user: %w", err)
	}

	path := fmt.Sprintf("/nodes/%d/users/%d/blacklist/%d/%d", owner.NodeID, owner.UserID, blocked.NodeID, blocked.UserID)
	err := c.doJSON(ctx, http.MethodDelete, path, token, nil, &entry, http.StatusOK)
	return entry, err
}

func (c *HTTPClient) ListBlockedUsers(ctx context.Context, token string, owner UserRef) ([]BlacklistEntry, error) {
	if err := owner.validate(); err != nil {
		return nil, fmt.Errorf("invalid owner: %w", err)
	}

	var resp httpBlockedUsersResponse
	path := fmt.Sprintf("/nodes/%d/users/%d/blacklist", owner.NodeID, owner.UserID)
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
		Role:           in.Role,
		ProfileJSON:    append([]byte(nil), profile...),
		SystemReserved: in.SystemReserved,
		CreatedAt:      in.CreatedAt,
		UpdatedAt:      in.UpdatedAt,
		OriginNodeID:   in.OriginNodeID,
	}
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
