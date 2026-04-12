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

type httpMessageRequest struct {
	Sender       string   `json:"sender"`
	Body         []byte   `json:"body"`
	RelayTarget  *UserRef `json:"relay_target,omitempty"`
	DeliveryMode string   `json:"delivery_mode,omitempty"`
}

func (c *HTTPClient) client() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

func (c *HTTPClient) Login(ctx context.Context, nodeID, userID int64, password string) (string, error) {
	var resp httpLoginResponse
	err := c.doJSON(ctx, http.MethodPost, "/auth/login", "", httpLoginRequest{
		NodeID:   nodeID,
		UserID:   userID,
		Password: password,
	}, http.StatusOK, &resp)
	if err != nil {
		return "", err
	}
	if resp.Token == "" {
		return "", &ProtocolError{Message: "empty token in login response"}
	}
	return resp.Token, nil
}

func (c *HTTPClient) CreateUser(ctx context.Context, token string, req CreateUserRequest) (User, error) {
	var user User
	if req.Username == "" {
		return user, fmt.Errorf("username is required")
	}
	if req.Role == "" {
		return user, fmt.Errorf("role is required")
	}
	err := c.doJSON(ctx, http.MethodPost, "/users", token, req, http.StatusOK, &user)
	return user, err
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
	return c.doJSON(ctx, http.MethodPost, path, token, req, http.StatusOK, nil)
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

	var messages []Message
	err := c.doJSON(ctx, http.MethodGet, path, token, nil, http.StatusOK, &messages)
	return messages, err
}

func (c *HTTPClient) PostMessage(ctx context.Context, token string, target UserRef, sender string, body []byte) (Message, error) {
	var message Message
	if err := target.validate(); err != nil {
		return message, fmt.Errorf("invalid target: %w", err)
	}
	if sender == "" {
		return message, fmt.Errorf("sender is required")
	}
	if len(body) == 0 {
		return message, fmt.Errorf("body is required")
	}
	path := fmt.Sprintf("/nodes/%d/users/%d/messages", target.NodeID, target.UserID)
	err := c.doJSON(ctx, http.MethodPost, path, token, httpMessageRequest{
		Sender: sender,
		Body:   body,
	}, http.StatusOK, &message)
	return message, err
}

func (c *HTTPClient) PostPacket(ctx context.Context, token string, targetNodeID int64, relayTarget UserRef, sender string, body []byte, mode DeliveryMode) error {
	if targetNodeID == 0 {
		return fmt.Errorf("target node_id is required")
	}
	if err := relayTarget.validate(); err != nil {
		return fmt.Errorf("invalid relay_target: %w", err)
	}
	if sender == "" {
		return fmt.Errorf("sender is required")
	}
	if len(body) == 0 {
		return fmt.Errorf("body is required")
	}
	if err := mode.validatePacketMode(); err != nil {
		return err
	}

	path := fmt.Sprintf("/nodes/%d/users/3/messages", targetNodeID)
	return c.doJSON(ctx, http.MethodPost, path, token, httpMessageRequest{
		Sender:       sender,
		Body:         body,
		RelayTarget:  &relayTarget,
		DeliveryMode: string(mode),
	}, http.StatusAccepted, nil)
}

func (c *HTTPClient) doJSON(ctx context.Context, method, path, token string, reqBody any, wantStatus int, out any) error {
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

	if resp.StatusCode != wantStatus {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return &ProtocolError{Message: fmt.Sprintf("unexpected HTTP status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))}
	}
	if out == nil {
		io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
