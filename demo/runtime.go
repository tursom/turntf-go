package demo

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	turntf "github.com/tursom/turntf-go"
)

type Runner struct {
	scenario *Scenario
	output   io.Writer

	writeMu  sync.Mutex
	globals  map[string]any
	sessions map[string]*sessionRuntime
}

type sessionRuntime struct {
	name     string
	nodeName string
	node     NodeConfig
	spec     SessionSpec
	store    *turntf.MemoryCursorStore
	client   *turntf.Client
	handler  *sessionHandler

	mu        sync.Mutex
	locals    map[string]any
	connected bool
	closed    bool
}

type sessionHandler struct {
	parent *sessionRuntime

	mu     sync.Mutex
	cond   *sync.Cond
	events []sessionEvent
	login  *turntf.LoginInfo
}

type sessionEvent struct {
	kind    string
	payload map[string]any
}

type branchScope struct {
	name     string
	barriers *barrierSet
}

type barrierSet struct {
	mu    sync.Mutex
	cond  *sync.Cond
	total int
	hits  map[string]int
}

func RunScenario(ctx context.Context, scenario *Scenario, output io.Writer) error {
	if scenario == nil {
		return fmt.Errorf("scenario is required")
	}
	if err := scenario.Validate(); err != nil {
		return err
	}
	if output == nil {
		output = io.Discard
	}
	runner := &Runner{
		scenario: scenario,
		output:   output,
		globals:  cloneStringMap(scenario.Vars),
		sessions: make(map[string]*sessionRuntime, len(scenario.Sessions)),
	}
	for name, spec := range scenario.Sessions {
		node := scenario.Nodes[spec.Node]
		store := turntf.NewMemoryCursorStore()
		for _, cursor := range spec.SeenMessages {
			_ = store.SaveCursor(ctx, turntf.MessageCursor{NodeID: cursor.NodeID, Seq: cursor.Seq})
		}
		runner.sessions[name] = &sessionRuntime{
			name:     name,
			nodeName: spec.Node,
			node:     node,
			spec:     spec,
			store:    store,
			locals:   make(map[string]any),
		}
	}

	defer runner.closeAll()
	for i, step := range scenario.Script {
		if err := runner.executeStep(ctx, step, fmt.Sprintf("script[%d]", i), nil); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) executeStep(ctx context.Context, step Step, path string, scope *branchScope) error {
	switch step.Step {
	case "parallel":
		return r.executeParallel(ctx, step, path)
	case "barrier":
		if scope == nil || scope.barriers == nil {
			return fmt.Errorf("%s barrier used outside parallel scope", path)
		}
		r.logf("[%s] barrier %s", path, step.Name)
		return scope.barriers.Wait(ctx, step.Name)
	}

	session := r.sessions[step.Session]
	session.mu.Lock()
	defer session.mu.Unlock()

	label := path
	if scope != nil && scope.name != "" {
		label = scope.name + " " + path
	}
	switch step.Step {
	case "connect":
		return r.executeConnect(ctx, session, step, label)
	case "request":
		return r.executeRequest(ctx, session, step, label)
	case "expect_event":
		return r.executeExpectEvent(ctx, session, step, label)
	case "sleep":
		r.logf("[%s] sleep %s", label, step.Duration.Duration)
		timer := time.NewTimer(step.Duration.Duration)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", label, ctx.Err())
		case <-timer.C:
			return nil
		}
	case "close":
		return r.executeClose(session, label)
	default:
		return fmt.Errorf("%s: unsupported step %q", label, step.Step)
	}
}

func (r *Runner) executeParallel(ctx context.Context, step Step, path string) error {
	barriers := newBarrierSet(len(step.Branches))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, len(step.Branches))
	var wg sync.WaitGroup
	for i, branch := range step.Branches {
		wg.Add(1)
		go func(i int, branch Branch) {
			defer wg.Done()
			scope := &branchScope{name: branch.Name, barriers: barriers}
			for j, branchStep := range branch.Script {
				branchPath := fmt.Sprintf("%s.branches[%d].script[%d]", path, i, j)
				if err := r.executeStep(ctx, branchStep, branchPath, scope); err != nil {
					errCh <- err
					cancel()
					return
				}
			}
		}(i, branch)
	}

	wg.Wait()
	close(errCh)
	if err, ok := <-errCh; ok {
		return err
	}
	return nil
}

func (r *Runner) executeConnect(ctx context.Context, session *sessionRuntime, step Step, label string) error {
	if session.connected {
		return fmt.Errorf("%s: session %q is already connected", label, session.name)
	}
	handler := &sessionHandler{parent: session}
	handler.cond = sync.NewCond(&handler.mu)

	cfg := turntf.Config{
		BaseURL: session.node.BaseURL,
		Credentials: turntf.Credentials{
			NodeID:   session.spec.User.NodeID,
			UserID:   session.spec.User.UserID,
			Password: session.spec.User.Password.PasswordInput,
		},
		CursorStore:           session.store,
		Handler:               handler,
		RequestTimeout:        r.scenario.Defaults.Timeout.Duration,
		PingInterval:          time.Hour,
		Reconnect:             false,
		InitialReconnectDelay: time.Second,
		MaxReconnectDelay:     time.Second,
		AckMessages:           *r.scenario.Defaults.AutoAckMessages,
	}
	client, err := turntf.NewClient(cfg)
	if err != nil {
		return fmt.Errorf("%s: create client: %w", label, err)
	}
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("%s: connect session %q via %s: %w", label, session.name, session.node.BaseURL, err)
	}
	login := handler.currentLogin()
	if login == nil {
		_ = client.Close()
		return fmt.Errorf("%s: missing login info for session %q", label, session.name)
	}
	actual := map[string]any{
		"user":             normalizeValue(login.User),
		"protocol_version": login.ProtocolVersion,
		"session_ref":      normalizeValue(login.SessionRef),
	}
	if err := matchSubset(resolveValue(step.Expect.Login, r.scopeVars(session)), actual); err != nil {
		_ = client.Close()
		return fmt.Errorf("%s: login assertion failed: %w", label, err)
	}
	session.client = client
	session.handler = handler
	session.connected = true
	r.logf("[%s] connected session=%s node=%s base_url=%s", label, session.name, session.nodeName, session.node.BaseURL)
	return r.saveVars(session, label, step.Save, map[string]any{"login": actual})
}

func (r *Runner) executeClose(session *sessionRuntime, label string) error {
	if !session.connected || session.client == nil {
		return fmt.Errorf("%s: session %q is not connected", label, session.name)
	}
	if session.closed {
		return fmt.Errorf("%s: session %q already closed", label, session.name)
	}
	err := session.client.Close()
	session.closed = true
	session.connected = false
	r.logf("[%s] closed session=%s", label, session.name)
	if err != nil {
		return fmt.Errorf("%s: close session %q: %w", label, session.name, err)
	}
	return nil
}

func (r *Runner) executeRequest(ctx context.Context, session *sessionRuntime, step Step, label string) error {
	if !session.connected || session.client == nil {
		return fmt.Errorf("%s: session %q is not connected", label, session.name)
	}
	request := resolveValue(step.Request, r.scopeVars(session))
	payload, root, err := r.callAction(ctx, session.client, step.Action, request)
	if err != nil {
		serverErr := &turntf.ServerError{}
		if step.Expect != nil && step.Expect.Error != nil && errors.As(err, &serverErr) {
			actual := map[string]any{"code": serverErr.Code, "message": serverErr.Message}
			expected := resolveValue(map[string]any{
				"code":    step.Expect.Error.Code,
				"message": step.Expect.Error.Message,
			}, r.scopeVars(session))
			if err := matchSubset(expected, actual); err != nil {
				return fmt.Errorf("%s: error assertion failed: %w", label, err)
			}
			r.logf("[%s] request action=%s error=%s", label, step.Action, serverErr.Code)
			return r.saveVars(session, label, step.Save, map[string]any{"error": actual})
		}
		return fmt.Errorf("%s: request %s failed: %w", label, step.Action, err)
	}
	if step.Expect == nil || len(step.Expect.OK) == 0 {
		return fmt.Errorf("%s: missing success expectation for action %s", label, step.Action)
	}
	expected := resolveValue(step.Expect.OK, r.scopeVars(session))
	if err := matchSubset(expected, payload); err != nil {
		return fmt.Errorf("%s: request assertion failed: %w", label, err)
	}
	r.logf("[%s] request action=%s ok", label, step.Action)
	return r.saveVars(session, label, step.Save, root)
}

func (r *Runner) executeExpectEvent(ctx context.Context, session *sessionRuntime, step Step, label string) error {
	if !session.connected || session.handler == nil {
		return fmt.Errorf("%s: session %q is not connected", label, session.name)
	}
	timeout := step.Timeout.Duration
	if timeout <= 0 {
		timeout = r.scenario.Defaults.Timeout.Duration
	}
	eventCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	expected := resolveValue(step.Match, r.scopeVars(session))
	event, err := session.handler.waitForEvent(eventCtx, step.Event, expected)
	if err != nil {
		return fmt.Errorf("%s: expect %s failed: %w", label, step.Event, err)
	}
	r.logf("[%s] event=%s matched", label, step.Event)
	return r.saveVars(session, label, step.Save, event.payload)
}

func (r *Runner) callAction(ctx context.Context, client *turntf.Client, action string, request any) (map[string]any, map[string]any, error) {
	reqMap, ok := request.(map[string]any)
	if !ok {
		reqMap = map[string]any{}
	}
	switch action {
	case "send_message":
		input, err := parseSendMessageInput(reqMap)
		if err != nil {
			return nil, nil, err
		}
		msg, err := client.SendMessage(ctx, input)
		if err != nil {
			return nil, nil, err
		}
		payload := map[string]any{"message": normalizeValue(msg)}
		return payload, map[string]any{"ok": payload}, nil
	case "send_packet":
		input, err := parseSendPacketInput(reqMap)
		if err != nil {
			return nil, nil, err
		}
		accepted, err := client.SendPacket(ctx, input)
		if err != nil {
			return nil, nil, err
		}
		payload := map[string]any{"transient_accepted": normalizeValue(accepted)}
		return payload, map[string]any{"ok": payload}, nil
	case "ping":
		if err := client.Ping(ctx); err != nil {
			return nil, nil, err
		}
		payload := map[string]any{"pong": map[string]any{}}
		return payload, map[string]any{"ok": payload}, nil
	case "create_user":
		req, err := parseCreateUser(reqMap)
		if err != nil {
			return nil, nil, err
		}
		user, err := client.CreateUser(ctx, "", req)
		if err != nil {
			return nil, nil, err
		}
		payload := map[string]any{"user": normalizeValue(user)}
		return payload, map[string]any{"ok": payload}, nil
	case "get_user":
		target, err := parseUserRefField(reqMap, "user")
		if err != nil {
			return nil, nil, err
		}
		user, err := client.GetUser(ctx, target)
		if err != nil {
			return nil, nil, err
		}
		payload := map[string]any{"user": normalizeValue(user)}
		return payload, map[string]any{"ok": payload}, nil
	case "update_user":
		target, req, err := parseUpdateUser(reqMap)
		if err != nil {
			return nil, nil, err
		}
		user, err := client.UpdateUser(ctx, target, req)
		if err != nil {
			return nil, nil, err
		}
		payload := map[string]any{"user": normalizeValue(user)}
		return payload, map[string]any{"ok": payload}, nil
	case "delete_user":
		target, err := parseUserRefField(reqMap, "user")
		if err != nil {
			return nil, nil, err
		}
		result, err := client.DeleteUser(ctx, target)
		if err != nil {
			return nil, nil, err
		}
		payload := normalizeValue(result).(map[string]any)
		return payload, map[string]any{"ok": payload}, nil
	case "list_messages":
		target, err := parseUserRefField(reqMap, "user")
		if err != nil {
			return nil, nil, err
		}
		limit, err := intField(reqMap, "limit", false)
		if err != nil {
			return nil, nil, err
		}
		items, err := client.WSListMessages(ctx, target, limit)
		if err != nil {
			return nil, nil, err
		}
		payload := listPayload(items)
		return payload, map[string]any{"ok": payload}, nil
	case "subscribe_channel":
		subscriber, err := parseUserRefField(reqMap, "subscriber")
		if err != nil {
			return nil, nil, err
		}
		channel, err := parseUserRefField(reqMap, "channel")
		if err != nil {
			return nil, nil, err
		}
		sub, err := client.SubscribeChannel(ctx, "", subscriber, channel)
		if err != nil {
			return nil, nil, err
		}
		payload := map[string]any{"subscription": normalizeValue(sub)}
		return payload, map[string]any{"ok": payload}, nil
	case "unsubscribe_channel":
		subscriber, err := parseUserRefField(reqMap, "subscriber")
		if err != nil {
			return nil, nil, err
		}
		channel, err := parseUserRefField(reqMap, "channel")
		if err != nil {
			return nil, nil, err
		}
		sub, err := client.UnsubscribeChannel(ctx, subscriber, channel)
		if err != nil {
			return nil, nil, err
		}
		payload := map[string]any{"subscription": normalizeValue(sub)}
		return payload, map[string]any{"ok": payload}, nil
	case "list_subscriptions":
		subscriber, err := parseUserRefField(reqMap, "subscriber")
		if err != nil {
			return nil, nil, err
		}
		items, err := client.ListSubscriptions(ctx, subscriber)
		if err != nil {
			return nil, nil, err
		}
		payload := listPayload(items)
		return payload, map[string]any{"ok": payload}, nil
	case "block_user":
		owner, blocked, err := parseBlacklistRefs(reqMap)
		if err != nil {
			return nil, nil, err
		}
		entry, err := client.BlockUser(ctx, "", owner, blocked)
		if err != nil {
			return nil, nil, err
		}
		payload := map[string]any{"entry": normalizeValue(entry)}
		return payload, map[string]any{"ok": payload}, nil
	case "unblock_user":
		owner, blocked, err := parseBlacklistRefs(reqMap)
		if err != nil {
			return nil, nil, err
		}
		entry, err := client.UnblockUser(ctx, "", owner, blocked)
		if err != nil {
			return nil, nil, err
		}
		payload := map[string]any{"entry": normalizeValue(entry)}
		return payload, map[string]any{"ok": payload}, nil
	case "list_blocked_users":
		owner, err := parseUserRefField(reqMap, "owner")
		if err != nil {
			return nil, nil, err
		}
		items, err := client.ListBlockedUsers(ctx, "", owner)
		if err != nil {
			return nil, nil, err
		}
		payload := listPayload(items)
		return payload, map[string]any{"ok": payload}, nil
	case "list_events":
		after, err := int64Field(reqMap, "after", false)
		if err != nil {
			return nil, nil, err
		}
		limit, err := intField(reqMap, "limit", false)
		if err != nil {
			return nil, nil, err
		}
		items, err := client.ListEvents(ctx, after, limit)
		if err != nil {
			return nil, nil, err
		}
		payload := listPayload(items)
		return payload, map[string]any{"ok": payload}, nil
	case "operations_status":
		status, err := client.OperationsStatus(ctx)
		if err != nil {
			return nil, nil, err
		}
		payload := map[string]any{"status": normalizeValue(status)}
		return payload, map[string]any{"ok": payload}, nil
	case "metrics":
		text, err := client.Metrics(ctx)
		if err != nil {
			return nil, nil, err
		}
		payload := map[string]any{"text": text}
		return payload, map[string]any{"ok": payload}, nil
	case "list_cluster_nodes":
		items, err := client.ListClusterNodes(ctx)
		if err != nil {
			return nil, nil, err
		}
		payload := listPayload(items)
		return payload, map[string]any{"ok": payload}, nil
	case "list_node_logged_in_users":
		nodeID, err := int64Field(reqMap, "node_id", true)
		if err != nil {
			return nil, nil, err
		}
		items, err := client.ListNodeLoggedInUsers(ctx, nodeID)
		if err != nil {
			return nil, nil, err
		}
		payload := listPayload(items)
		return payload, map[string]any{"ok": payload}, nil
	case "resolve_user_sessions":
		user, err := parseUserRefField(reqMap, "user")
		if err != nil {
			return nil, nil, err
		}
		resolved, err := client.ResolveUserSessions(ctx, user)
		if err != nil {
			return nil, nil, err
		}
		payload := map[string]any{"resolved_user_sessions": normalizeValue(resolved)}
		return payload, map[string]any{"ok": payload}, nil
	default:
		return nil, nil, fmt.Errorf("unsupported action %q", action)
	}
}

func listPayload(items any) map[string]any {
	values := normalizeValue(items)
	list, _ := values.([]any)
	return map[string]any{
		"items": list,
		"count": len(list),
	}
}

func (r *Runner) saveVars(session *sessionRuntime, label string, bindings map[string]string, root map[string]any) error {
	for name, path := range bindings {
		value, ok := lookupPath(root, path)
		if !ok {
			return fmt.Errorf("%s: save path %q not found", label, path)
		}
		session.locals[name] = cloneValue(value)
		r.logf("[%s] saved %s", label, name)
	}
	return nil
}

func (r *Runner) scopeVars(session *sessionRuntime) map[string]any {
	scope := cloneStringMap(r.globals)
	for key, value := range session.locals {
		scope[key] = cloneValue(value)
	}
	return scope
}

func (r *Runner) closeAll() {
	names := make([]string, 0, len(r.sessions))
	for name := range r.sessions {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		session := r.sessions[name]
		session.mu.Lock()
		client := session.client
		alreadyClosed := session.closed
		session.closed = true
		session.connected = false
		session.mu.Unlock()
		if client != nil && !alreadyClosed {
			_ = client.Close()
		}
	}
}

func (r *Runner) logf(format string, args ...any) {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	fmt.Fprintf(r.output, format+"\n", args...)
}

func newBarrierSet(total int) *barrierSet {
	b := &barrierSet{
		total: total,
		hits:  make(map[string]int),
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *barrierSet) Wait(ctx context.Context, name string) error {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			b.mu.Lock()
			b.cond.Broadcast()
			b.mu.Unlock()
		case <-done:
		}
	}()
	defer close(done)

	b.mu.Lock()
	defer b.mu.Unlock()
	b.hits[name]++
	b.cond.Broadcast()
	for b.hits[name] < b.total {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		b.cond.Wait()
	}
	return nil
}

func (h *sessionHandler) OnLogin(_ context.Context, info turntf.LoginInfo) {
	h.mu.Lock()
	defer h.mu.Unlock()
	value := info
	h.login = &value
	h.cond.Broadcast()
}

func (h *sessionHandler) OnMessage(_ context.Context, msg turntf.Message) {
	h.push("message_pushed", map[string]any{"message": normalizeValue(msg)})
}

func (h *sessionHandler) OnPacket(_ context.Context, packet turntf.Packet) {
	h.push("packet_pushed", map[string]any{"packet": normalizeValue(packet)})
}

func (h *sessionHandler) OnError(_ context.Context, err error) {
	serverErr := &turntf.ServerError{}
	if errors.As(err, &serverErr) && serverErr.RequestID == 0 {
		h.push("error", map[string]any{"code": serverErr.Code, "message": serverErr.Message})
	}
}

func (h *sessionHandler) OnDisconnect(_ context.Context, err error) {
	payload := map[string]any{"message": err.Error()}
	h.push("disconnect", payload)
}

func (h *sessionHandler) currentLogin() *turntf.LoginInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.login == nil {
		return nil
	}
	value := *h.login
	return &value
}

func (h *sessionHandler) push(kind string, payload map[string]any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, sessionEvent{kind: kind, payload: payload})
	h.cond.Broadcast()
}

func (h *sessionHandler) waitForEvent(ctx context.Context, kind string, expected any) (sessionEvent, error) {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			h.mu.Lock()
			h.cond.Broadcast()
			h.mu.Unlock()
		case <-done:
		}
	}()
	defer close(done)

	h.mu.Lock()
	defer h.mu.Unlock()
	for {
		for i, event := range h.events {
			if event.kind != kind {
				continue
			}
			if err := matchSubset(expected, event.payload); err != nil {
				continue
			}
			h.events = append(h.events[:i], h.events[i+1:]...)
			return event, nil
		}
		if ctx.Err() != nil {
			return sessionEvent{}, ctx.Err()
		}
		h.cond.Wait()
	}
}

func parseSendMessageInput(values map[string]any) (turntf.SendMessageInput, error) {
	target, err := parseUserRefField(values, "target")
	if err != nil {
		return turntf.SendMessageInput{}, err
	}
	body, err := bytesField(values, "body", true)
	if err != nil {
		return turntf.SendMessageInput{}, err
	}
	return turntf.SendMessageInput{Target: target, Body: body}, nil
}

func parseSendPacketInput(values map[string]any) (turntf.SendPacketInput, error) {
	target, err := parseUserRefField(values, "target")
	if err != nil {
		return turntf.SendPacketInput{}, err
	}
	body, err := bytesField(values, "body", true)
	if err != nil {
		return turntf.SendPacketInput{}, err
	}
	modeValue, ok := values["delivery_mode"]
	if !ok {
		return turntf.SendPacketInput{}, fmt.Errorf("delivery_mode is required")
	}
	mode, err := stringValue(modeValue)
	if err != nil {
		return turntf.SendPacketInput{}, fmt.Errorf("delivery_mode: %w", err)
	}
	var targetSession turntf.SessionRef
	if _, ok := values["target_session"]; ok {
		targetSession, err = parseSessionRefField(values, "target_session", true)
		if err != nil {
			return turntf.SendPacketInput{}, err
		}
	}
	return turntf.SendPacketInput{
		Target:        target,
		Body:          body,
		DeliveryMode:  turntf.DeliveryMode(mode),
		TargetSession: targetSession,
	}, nil
}

func parseCreateUser(values map[string]any) (turntf.CreateUserRequest, error) {
	username, err := stringField(values, "username", true)
	if err != nil {
		return turntf.CreateUserRequest{}, err
	}
	role, err := stringField(values, "role", true)
	if err != nil {
		return turntf.CreateUserRequest{}, err
	}
	profile, err := bytesField(values, "profile_json", false)
	if err != nil {
		return turntf.CreateUserRequest{}, err
	}
	var password turntf.PasswordInput
	if raw, ok := values["password"]; ok {
		password, err = parsePasswordValue(raw)
		if err != nil {
			return turntf.CreateUserRequest{}, fmt.Errorf("password: %w", err)
		}
	}
	return turntf.CreateUserRequest{
		Username:    username,
		Password:    password,
		Role:        role,
		ProfileJSON: profile,
	}, nil
}

func parseUpdateUser(values map[string]any) (turntf.UserRef, turntf.UpdateUserRequest, error) {
	target, err := parseUserRefField(values, "user")
	if err != nil {
		return turntf.UserRef{}, turntf.UpdateUserRequest{}, err
	}
	var req turntf.UpdateUserRequest
	if raw, ok := values["username"]; ok {
		value, err := stringValue(raw)
		if err != nil {
			return turntf.UserRef{}, turntf.UpdateUserRequest{}, fmt.Errorf("username: %w", err)
		}
		req.Username = &value
	}
	if raw, ok := values["password"]; ok {
		value, err := parsePasswordValue(raw)
		if err != nil {
			return turntf.UserRef{}, turntf.UpdateUserRequest{}, fmt.Errorf("password: %w", err)
		}
		req.Password = &value
	}
	if raw, ok := values["profile_json"]; ok {
		value, err := decodeBytes(raw)
		if err != nil {
			return turntf.UserRef{}, turntf.UpdateUserRequest{}, fmt.Errorf("profile_json: %w", err)
		}
		req.ProfileJSON = &value
	}
	if raw, ok := values["role"]; ok {
		value, err := stringValue(raw)
		if err != nil {
			return turntf.UserRef{}, turntf.UpdateUserRequest{}, fmt.Errorf("role: %w", err)
		}
		req.Role = &value
	}
	return target, req, nil
}

func parseBlacklistRefs(values map[string]any) (turntf.UserRef, turntf.UserRef, error) {
	owner, err := parseUserRefField(values, "owner")
	if err != nil {
		return turntf.UserRef{}, turntf.UserRef{}, err
	}
	blocked, err := parseUserRefField(values, "blocked")
	if err != nil {
		return turntf.UserRef{}, turntf.UserRef{}, err
	}
	return owner, blocked, nil
}

func parsePasswordValue(raw any) (turntf.PasswordInput, error) {
	value, ok := raw.(map[string]any)
	if !ok {
		return turntf.PasswordInput{}, fmt.Errorf("must be an object with source and value")
	}
	source, err := stringField(value, "source", true)
	if err != nil {
		return turntf.PasswordInput{}, err
	}
	text, err := stringField(value, "value", true)
	if err != nil {
		return turntf.PasswordInput{}, err
	}
	switch source {
	case "plain":
		input, err := turntf.PlainPassword(text)
		if err != nil {
			return turntf.PasswordInput{}, err
		}
		return input, nil
	case "hashed":
		input := turntf.HashedPassword(text)
		if err := input.Validate(); err != nil {
			return turntf.PasswordInput{}, err
		}
		return input, nil
	default:
		return turntf.PasswordInput{}, fmt.Errorf("unsupported password source %q", source)
	}
}

func parseUserRefField(values map[string]any, key string) (turntf.UserRef, error) {
	raw, ok := values[key]
	if !ok {
		return turntf.UserRef{}, fmt.Errorf("%s is required", key)
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return turntf.UserRef{}, fmt.Errorf("%s must be an object", key)
	}
	nodeID, err := int64Field(m, "node_id", true)
	if err != nil {
		return turntf.UserRef{}, fmt.Errorf("%s.%w", key, err)
	}
	userID, err := int64Field(m, "user_id", true)
	if err != nil {
		return turntf.UserRef{}, fmt.Errorf("%s.%w", key, err)
	}
	return turntf.UserRef{NodeID: nodeID, UserID: userID}, nil
}

func parseSessionRefField(values map[string]any, key string, required bool) (turntf.SessionRef, error) {
	raw, ok := values[key]
	if !ok {
		if required {
			return turntf.SessionRef{}, fmt.Errorf("%s is required", key)
		}
		return turntf.SessionRef{}, nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return turntf.SessionRef{}, fmt.Errorf("%s must be an object", key)
	}
	servingNodeID, err := int64Field(m, "serving_node_id", true)
	if err != nil {
		return turntf.SessionRef{}, fmt.Errorf("%s.%w", key, err)
	}
	sessionID, err := stringField(m, "session_id", true)
	if err != nil {
		return turntf.SessionRef{}, fmt.Errorf("%s.%w", key, err)
	}
	return turntf.SessionRef{ServingNodeID: servingNodeID, SessionID: sessionID}, nil
}

func stringField(values map[string]any, key string, required bool) (string, error) {
	raw, ok := values[key]
	if !ok {
		if required {
			return "", fmt.Errorf("%s is required", key)
		}
		return "", nil
	}
	value, err := stringValue(raw)
	if err != nil {
		return "", fmt.Errorf("%s: %w", key, err)
	}
	return value, nil
}

func stringValue(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("expected string, got %T", value)
	}
}

func intField(values map[string]any, key string, required bool) (int, error) {
	value, err := int64Field(values, key, required)
	return int(value), err
}

func int64Field(values map[string]any, key string, required bool) (int64, error) {
	raw, ok := values[key]
	if !ok {
		if required {
			return 0, fmt.Errorf("%s is required", key)
		}
		return 0, nil
	}
	switch v := raw.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case uint64:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		value, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%s must be an integer", key)
		}
		return value, nil
	default:
		return 0, fmt.Errorf("%s must be an integer", key)
	}
}

func bytesField(values map[string]any, key string, required bool) ([]byte, error) {
	raw, ok := values[key]
	if !ok {
		if required {
			return nil, fmt.Errorf("%s is required", key)
		}
		return nil, nil
	}
	value, err := decodeBytes(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	return value, nil
}

func decodeBytes(value any) ([]byte, error) {
	switch v := value.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return append([]byte(nil), v...), nil
	case map[string]any:
		format, err := stringField(v, "format", true)
		if err != nil {
			return nil, err
		}
		rawValue, ok := v["value"]
		if !ok {
			return nil, fmt.Errorf("value is required")
		}
		switch format {
		case "json":
			data, err := json.Marshal(rawValue)
			if err != nil {
				return nil, err
			}
			return data, nil
		case "base64":
			text, err := stringValue(rawValue)
			if err != nil {
				return nil, err
			}
			return base64.StdEncoding.DecodeString(text)
		default:
			return nil, fmt.Errorf("unsupported bytes format %q", format)
		}
	default:
		return nil, fmt.Errorf("expected string or {format,value}, got %T", value)
	}
}

func normalizeValue(value any) any {
	if value == nil {
		return nil
	}
	rv := reflect.ValueOf(value)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.Struct:
		out := make(map[string]any)
		rt := rv.Type()
		for i := 0; i < rv.NumField(); i++ {
			field := rt.Field(i)
			if !field.IsExported() {
				continue
			}
			name := jsonFieldName(field)
			if name == "-" || name == "" {
				continue
			}
			out[name] = normalizeValue(rv.Field(i).Interface())
		}
		return out
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return append([]byte(nil), rv.Bytes()...)
		}
		out := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out[i] = normalizeValue(rv.Index(i).Interface())
		}
		return out
	case reflect.Array:
		out := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out[i] = normalizeValue(rv.Index(i).Interface())
		}
		return out
	case reflect.Map:
		iter := rv.MapRange()
		out := make(map[string]any)
		for iter.Next() {
			out[fmt.Sprint(iter.Key().Interface())] = normalizeValue(iter.Value().Interface())
		}
		return out
	default:
		return rv.Interface()
	}
}

func jsonFieldName(field reflect.StructField) string {
	tag := field.Tag.Get("json")
	if tag == "" {
		return field.Name
	}
	name := strings.Split(tag, ",")[0]
	if name == "" {
		return field.Name
	}
	return name
}

func matchSubset(expected, actual any) error {
	expected = normalizeValue(expected)
	actual = normalizeValue(actual)
	return matchAt("", expected, actual)
}

func matchAt(path string, expected, actual any) error {
	if actualBytes, ok := actual.([]byte); ok {
		expectedBytes, err := decodeBytes(expected)
		if err == nil {
			if string(expectedBytes) == string(actualBytes) {
				return nil
			}
			return fmt.Errorf("%s: expected %q, got %q", pathValue(path), string(expectedBytes), string(actualBytes))
		}
	}
	switch exp := expected.(type) {
	case map[string]any:
		act, ok := actual.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: expected object, got %T", pathValue(path), actual)
		}
		for key, value := range exp {
			actualValue, ok := act[key]
			if !ok {
				return fmt.Errorf("%s: missing key %q", pathValue(path), key)
			}
			next := key
			if path != "" {
				next = path + "." + key
			}
			if err := matchAt(next, value, actualValue); err != nil {
				return err
			}
		}
		return nil
	case []any:
		act, ok := actual.([]any)
		if !ok {
			return fmt.Errorf("%s: expected array, got %T", pathValue(path), actual)
		}
		if len(exp) != len(act) {
			return fmt.Errorf("%s: expected array length %d, got %d", pathValue(path), len(exp), len(act))
		}
		for i := range exp {
			if err := matchAt(fmt.Sprintf("%s[%d]", pathValue(path), i), exp[i], act[i]); err != nil {
				return err
			}
		}
		return nil
	case []byte:
		act, ok := actual.([]byte)
		if !ok {
			return fmt.Errorf("%s: expected bytes, got %T", pathValue(path), actual)
		}
		if string(exp) != string(act) {
			return fmt.Errorf("%s: expected %q, got %q", pathValue(path), string(exp), string(act))
		}
		return nil
	default:
		if sameScalar(exp, actual) {
			return nil
		}
		return fmt.Errorf("%s: expected %v, got %v", pathValue(path), exp, actual)
	}
}

func sameScalar(expected, actual any) bool {
	switch exp := expected.(type) {
	case int:
		return numericEqual(float64(exp), actual)
	case int64:
		return numericEqual(float64(exp), actual)
	case uint64:
		return numericEqual(float64(exp), actual)
	case float64:
		return numericEqual(exp, actual)
	default:
		return reflect.DeepEqual(expected, actual)
	}
}

func numericEqual(expected float64, actual any) bool {
	switch value := actual.(type) {
	case int:
		return expected == float64(value)
	case int64:
		return expected == float64(value)
	case uint64:
		return expected == float64(value)
	case float64:
		return expected == value
	default:
		return false
	}
}

func pathValue(path string) string {
	if path == "" {
		return "value"
	}
	return path
}

var exactVarPattern = regexp.MustCompile(`^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$`)
var embeddedVarPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

func resolveValue(value any, vars map[string]any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = resolveValue(item, vars)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = resolveValue(item, vars)
		}
		return out
	case string:
		if match := exactVarPattern.FindStringSubmatch(v); match != nil {
			if resolved, ok := vars[match[1]]; ok {
				return cloneValue(resolved)
			}
			return v
		}
		return embeddedVarPattern.ReplaceAllStringFunc(v, func(input string) string {
			match := embeddedVarPattern.FindStringSubmatch(input)
			if match == nil {
				return input
			}
			if resolved, ok := vars[match[1]]; ok {
				switch item := resolved.(type) {
				case []byte:
					return string(item)
				default:
					return fmt.Sprint(item)
				}
			}
			return input
		})
	default:
		return cloneValue(v)
	}
}

func lookupPath(root any, path string) (any, bool) {
	current := root
	for _, part := range strings.Split(path, ".") {
		valueMap, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		next, ok := valueMap[part]
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

func cloneStringMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneValue(value)
	}
	return out
}

func cloneValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = cloneValue(item)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = cloneValue(item)
		}
		return out
	case []byte:
		return append([]byte(nil), v...)
	default:
		return v
	}
}
