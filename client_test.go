package turntf

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/proto"

	pb "github.com/tursom/turntf-go/internal/proto"
)

type recordingStore struct {
	mu      sync.Mutex
	cursors []MessageCursor
	saved   []string
}

func (s *recordingStore) LoadSeenMessages(context.Context) ([]MessageCursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]MessageCursor(nil), s.cursors...)
	return out, nil
}

func (s *recordingStore) SaveMessage(_ context.Context, msg Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saved = append(s.saved, "message")
	s.cursors = appendIfMissing(s.cursors, msg.Cursor())
	return nil
}

func (s *recordingStore) SaveCursor(_ context.Context, cursor MessageCursor) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saved = append(s.saved, "cursor")
	s.cursors = appendIfMissing(s.cursors, cursor)
	return nil
}

type recordingHandler struct {
	mu          sync.Mutex
	logins      []LoginInfo
	messages    []Message
	packets     []Packet
	errors      []error
	disconnects []error
}

func (h *recordingHandler) OnLogin(_ context.Context, info LoginInfo) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.logins = append(h.logins, info)
}
func (h *recordingHandler) OnMessage(_ context.Context, msg Message) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messages = append(h.messages, msg)
}
func (h *recordingHandler) OnPacket(_ context.Context, packet Packet) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.packets = append(h.packets, packet)
}
func (h *recordingHandler) OnError(_ context.Context, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.errors = append(h.errors, err)
}
func (h *recordingHandler) OnDisconnect(_ context.Context, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.disconnects = append(h.disconnects, err)
}

func TestClientLoginMessageAckSendAndPing(t *testing.T) {
	store := &recordingStore{}
	handler := &recordingHandler{}

	var firstSeen []*pb.MessageCursor
	acked := make(chan MessageCursor, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		login := mustReadClientEnvelope(t, conn)
		if got := login.GetLogin().GetUser(); got == nil || got.NodeId != 4096 || got.UserId != 1025 {
			t.Fatalf("unexpected login user: %+v", got)
		}
		if login.GetLogin().GetPassword() == "alice-password" {
			t.Fatal("expected login password to be hashed")
		}
		if err := bcrypt.CompareHashAndPassword([]byte(login.GetLogin().GetPassword()), []byte("alice-password")); err != nil {
			t.Fatalf("expected bcrypt password, got %v", err)
		}
		firstSeen = append([]*pb.MessageCursor(nil), login.GetLogin().SeenMessages...)

		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User: &pb.User{
						NodeId:    4096,
						UserId:    1025,
						Username:  "alice",
						LoginName: "alice.login",
						Role:      "user",
					},
					ProtocolVersion: "client-v1alpha1",
					SessionRef:      &pb.SessionRef{ServingNodeId: 4096, SessionId: "session-a"},
				},
			},
		})

		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_MessagePushed{
				MessagePushed: &pb.MessagePushed{
					Message: &pb.Message{
						Recipient:    &pb.UserRef{NodeId: 4096, UserId: 1025},
						NodeId:       4096,
						Seq:          7,
						Sender:       &pb.UserRef{NodeId: 4096, UserId: 1},
						Body:         []byte{0xff, 0x00},
						CreatedAtHlc: "hlc1",
					},
				},
			},
		})

		ack := mustReadClientEnvelope(t, conn)
		acked <- cursorFromProto(ack.GetAckMessage().Cursor)

		sendReq := mustReadClientEnvelope(t, conn)
		if sendReq.GetSendMessage().RequestId == 0 {
			t.Fatalf("expected request id")
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_SendMessageResponse{
				SendMessageResponse: &pb.SendMessageResponse{
					RequestId: sendReq.GetSendMessage().RequestId,
					Body: &pb.SendMessageResponse_Message{
						Message: &pb.Message{
							Recipient:    &pb.UserRef{NodeId: 4096, UserId: 1025},
							NodeId:       4096,
							Seq:          8,
							Sender:       &pb.UserRef{NodeId: 4096, UserId: 1025},
							Body:         sendReq.GetSendMessage().Body,
							CreatedAtHlc: "hlc2",
						},
					},
				},
			},
		})

		ping := mustReadClientEnvelope(t, conn)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_Pong{
				Pong: &pb.Pong{RequestId: ping.GetPing().RequestId},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:        server.URL,
		Credentials:    Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("alice-password")},
		CursorStore:    store,
		Handler:        handler,
		RequestTimeout: 2 * time.Second,
		PingInterval:   time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	loginInfo, ok := client.CurrentLogin()
	if !ok {
		t.Fatal("expected current login info")
	}
	if loginInfo.SessionRef != (SessionRef{ServingNodeID: 4096, SessionID: "session-a"}) {
		t.Fatalf("unexpected current session ref: %+v", loginInfo.SessionRef)
	}
	if loginInfo.User.LoginName != "alice.login" {
		t.Fatalf("unexpected current login user: %+v", loginInfo.User)
	}

	select {
	case cursor := <-acked:
		if cursor != (MessageCursor{NodeID: 4096, Seq: 7}) {
			t.Fatalf("unexpected ack cursor: %+v", cursor)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ack")
	}

	msg, err := client.SendMessage(ctx, SendMessageInput{
		Target: UserRef{NodeID: 4096, UserID: 1025},
		Body:   []byte("payload"),
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if msg.Seq != 8 {
		t.Fatalf("unexpected message seq: %d", msg.Seq)
	}
	if msg.Recipient != (UserRef{NodeID: 4096, UserID: 1025}) {
		t.Fatalf("unexpected message recipient: %+v", msg.Recipient)
	}
	if msg.Sender != (UserRef{NodeID: 4096, UserID: 1025}) {
		t.Fatalf("unexpected message sender: %+v", msg.Sender)
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if len(firstSeen) != 0 {
		t.Fatalf("expected empty seen_messages on first login, got %d", len(firstSeen))
	}
	if len(handler.logins) != 1 {
		t.Fatalf("expected 1 login callback, got %d", len(handler.logins))
	}
	if handler.logins[0].SessionRef != (SessionRef{ServingNodeID: 4096, SessionID: "session-a"}) {
		t.Fatalf("unexpected login callback session ref: %+v", handler.logins[0].SessionRef)
	}
	if handler.logins[0].User.LoginName != "alice.login" {
		t.Fatalf("unexpected login callback user: %+v", handler.logins[0].User)
	}
	if len(handler.messages) != 1 {
		t.Fatalf("expected 1 pushed message, got %d", len(handler.messages))
	}
	if got, want := store.saved, []string{"message", "cursor", "message", "cursor"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Fatalf("unexpected store operation order: %#v", got)
	}
}

func TestClientLoginCanRequestTransientOnlySession(t *testing.T) {
	var transientOnly bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		login := mustReadClientEnvelope(t, conn)
		transientOnly = login.GetLogin().GetTransientOnly()
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: 4096, UserId: 1025, Username: "alice", Role: "user"},
					ProtocolVersion: "client-v1alpha1",
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:        server.URL,
		Credentials:    Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("alice-password")},
		RequestTimeout: 2 * time.Second,
		PingInterval:   time.Hour,
		TransientOnly:  true,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if !transientOnly {
		t.Fatal("expected transient_only login flag")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestClientRealtimeStreamDialsRealtimePath(t *testing.T) {
	var requestPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPath = r.URL.Path
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		_ = mustReadClientEnvelope(t, conn)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: 4096, UserId: 1025, Username: "alice", Role: "user"},
					ProtocolVersion: "client-v1alpha1",
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:        server.URL,
		Credentials:    Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("alice-password")},
		RequestTimeout: 2 * time.Second,
		PingInterval:   time.Hour,
		TransientOnly:  true,
		RealtimeStream: true,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if requestPath != "/ws/realtime" {
		t.Fatalf("unexpected realtime websocket path: %s", requestPath)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestClientResolveUserSessionsAndTargetedPacket(t *testing.T) {
	store := &recordingStore{}
	handler := &recordingHandler{}

	targetUser := UserRef{NodeID: 4096, UserID: 1025}
	targetSession := SessionRef{ServingNodeID: 8192, SessionID: "session-target"}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		_ = mustReadClientEnvelope(t, conn)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: targetUser.NodeID, UserId: targetUser.UserID, Username: "alice", Role: "user"},
					ProtocolVersion: "client-v1alpha1",
					SessionRef:      &pb.SessionRef{ServingNodeId: 4096, SessionId: "session-a"},
				},
			},
		})

		resolveReq := mustReadClientEnvelope(t, conn)
		if got := resolveReq.GetResolveUserSessions().GetUser(); got == nil || got.GetNodeId() != targetUser.NodeID || got.GetUserId() != targetUser.UserID {
			t.Fatalf("unexpected resolve_user_sessions target: %+v", got)
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_ResolveUserSessionsResponse{
				ResolveUserSessionsResponse: &pb.ResolveUserSessionsResponse{
					RequestId: resolveReq.GetResolveUserSessions().GetRequestId(),
					User:      userRefToProto(targetUser),
					Presence: []*pb.OnlineNodePresence{
						{ServingNodeId: 4096, SessionCount: 1, TransportHint: "websocket"},
						{ServingNodeId: 8192, SessionCount: 1, TransportHint: "realtime"},
					},
					Items: []*pb.ResolvedSession{
						{
							Session:          &pb.SessionRef{ServingNodeId: 4096, SessionId: "session-a"},
							Transport:        "websocket",
							TransientCapable: true,
						},
						{
							Session:          sessionRefToProto(targetSession),
							Transport:        "realtime",
							TransientCapable: true,
						},
					},
					Count: 2,
				},
			},
		})

		sendReq := mustReadClientEnvelope(t, conn)
		if sendReq.GetSendMessage().GetDeliveryKind() != pb.ClientDeliveryKind_CLIENT_DELIVERY_KIND_TRANSIENT {
			t.Fatalf("unexpected delivery kind: %v", sendReq.GetSendMessage().GetDeliveryKind())
		}
		if got := sendReq.GetSendMessage().GetTargetSession(); got == nil || got.GetServingNodeId() != targetSession.ServingNodeID || got.GetSessionId() != targetSession.SessionID {
			t.Fatalf("unexpected target_session in send request: %+v", got)
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_SendMessageResponse{
				SendMessageResponse: &pb.SendMessageResponse{
					RequestId: sendReq.GetSendMessage().GetRequestId(),
					Body: &pb.SendMessageResponse_TransientAccepted{
						TransientAccepted: &pb.TransientAccepted{
							PacketId:      77,
							SourceNodeId:  4096,
							TargetNodeId:  8192,
							Recipient:     userRefToProto(targetUser),
							DeliveryMode:  pb.ClientDeliveryMode_CLIENT_DELIVERY_MODE_ROUTE_RETRY,
							TargetSession: sessionRefToProto(targetSession),
						},
					},
				},
			},
		})
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_PacketPushed{
				PacketPushed: &pb.PacketPushed{
					Packet: &pb.Packet{
						PacketId:      77,
						SourceNodeId:  4096,
						TargetNodeId:  8192,
						Recipient:     userRefToProto(targetUser),
						Sender:        userRefToProto(targetUser),
						Body:          []byte("targeted"),
						DeliveryMode:  pb.ClientDeliveryMode_CLIENT_DELIVERY_MODE_ROUTE_RETRY,
						TargetSession: sessionRefToProto(targetSession),
					},
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:        server.URL,
		Credentials:    Credentials{NodeID: targetUser.NodeID, UserID: targetUser.UserID, Password: MustPlainPassword("alice-password")},
		CursorStore:    store,
		Handler:        handler,
		RequestTimeout: 2 * time.Second,
		PingInterval:   time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	resolved, err := client.ResolveUserSessions(ctx, targetUser)
	if err != nil {
		t.Fatalf("ResolveUserSessions: %v", err)
	}
	if resolved.User != targetUser {
		t.Fatalf("unexpected resolved user: %+v", resolved.User)
	}
	if len(resolved.Presence) != 2 || resolved.Presence[1].ServingNodeID != 8192 || resolved.Presence[1].TransportHint != "realtime" {
		t.Fatalf("unexpected presence: %+v", resolved.Presence)
	}
	if len(resolved.Sessions) != 2 || resolved.Sessions[1].Session != targetSession || !resolved.Sessions[1].TransientCapable {
		t.Fatalf("unexpected resolved sessions: %+v", resolved.Sessions)
	}

	accepted, err := client.SendPacketToSession(ctx, targetUser, targetSession, []byte("targeted"), DeliveryModeRouteRetry)
	if err != nil {
		t.Fatalf("SendPacketToSession: %v", err)
	}
	if accepted.TargetSession != targetSession || accepted.TargetNodeID != 8192 {
		t.Fatalf("unexpected transient acceptance: %+v", accepted)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		handler.mu.Lock()
		gotPackets := append([]Packet(nil), handler.packets...)
		handler.mu.Unlock()
		if len(gotPackets) == 1 {
			if gotPackets[0].TargetSession != targetSession || string(gotPackets[0].Body) != "targeted" {
				t.Fatalf("unexpected packet push: %+v", gotPackets[0])
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected targeted packet push, got %+v", handler.packets)
}

func TestClientUnauthorizedStopsReconnect(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusPolicyViolation, "unauthorized")
		_ = mustReadClientEnvelope(t, conn)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_Error{
				Error: &pb.Error{Code: "unauthorized", Message: "bad credentials"},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:               server.URL,
		Credentials:           Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("wrong")},
		Reconnect:             true,
		InitialReconnectDelay: 10 * time.Millisecond,
		MaxReconnectDelay:     20 * time.Millisecond,
		PingInterval:          time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = client.Connect(ctx)
	var serverErr *ServerError
	if !errors.As(err, &serverErr) || !serverErr.Unauthorized() {
		t.Fatalf("expected unauthorized server error, got %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("expected 1 connection attempt, got %d", got)
	}
	_ = client.Close()
}

func TestClientListClusterQueries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		login := mustReadClientEnvelope(t, conn)
		if got := login.GetLogin().GetUser(); got == nil || got.NodeId != 4096 || got.UserId != 1025 {
			t.Fatalf("unexpected login user: %+v", got)
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: 4096, UserId: 1025, Username: "alice", Role: "user"},
					ProtocolVersion: "client-v1alpha1",
				},
			},
		})

		nodesReq := mustReadClientEnvelope(t, conn)
		if nodesReq.GetListClusterNodes().GetRequestId() == 0 {
			t.Fatal("expected request_id for list_cluster_nodes")
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_ListClusterNodesResponse{
				ListClusterNodesResponse: &pb.ListClusterNodesResponse{
					RequestId: nodesReq.GetListClusterNodes().GetRequestId(),
					Items: []*pb.ClusterNode{
						{NodeId: 4096, IsLocal: true},
						{NodeId: 8192, IsLocal: false, ConfiguredUrl: "ws://127.0.0.1:9081/internal/cluster/ws", Source: "discovered"},
					},
					Count: 2,
				},
			},
		})

		usersReq := mustReadClientEnvelope(t, conn)
		if usersReq.GetListNodeLoggedInUsers().GetNodeId() != 4096 {
			t.Fatalf("unexpected node_id: %d", usersReq.GetListNodeLoggedInUsers().GetNodeId())
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_ListNodeLoggedInUsersResponse{
				ListNodeLoggedInUsersResponse: &pb.ListNodeLoggedInUsersResponse{
					RequestId:    usersReq.GetListNodeLoggedInUsers().GetRequestId(),
					TargetNodeId: 4096,
					Items: []*pb.LoggedInUser{
						{NodeId: 4096, UserId: 1025, Username: "alice", LoginName: "alice.login"},
						{NodeId: 4096, UserId: 1026, Username: "bob", LoginName: "bob.login"},
					},
					Count: 2,
				},
			},
		})

		emptyReq := mustReadClientEnvelope(t, conn)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_ListClusterNodesResponse{
				ListClusterNodesResponse: &pb.ListClusterNodesResponse{
					RequestId: emptyReq.GetListClusterNodes().GetRequestId(),
					Items:     []*pb.ClusterNode{},
					Count:     0,
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:        server.URL,
		Credentials:    Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("alice-password")},
		RequestTimeout: 2 * time.Second,
		PingInterval:   time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	nodes, err := client.ListClusterNodes(ctx)
	if err != nil {
		t.Fatalf("ListClusterNodes: %v", err)
	}
	if len(nodes) != 2 || !nodes[0].IsLocal || nodes[1].ConfiguredURL == "" || nodes[1].Source != "discovered" {
		t.Fatalf("unexpected cluster nodes: %+v", nodes)
	}

	users, err := client.ListNodeLoggedInUsers(ctx, 4096)
	if err != nil {
		t.Fatalf("ListNodeLoggedInUsers: %v", err)
	}
	if len(users) != 2 || users[0].Username != "alice" || users[0].LoginName != "alice.login" || users[1].UserID != 1026 || users[1].LoginName != "bob.login" {
		t.Fatalf("unexpected logged-in users: %+v", users)
	}

	emptyNodes, err := client.ListClusterNodes(ctx)
	if err != nil {
		t.Fatalf("ListClusterNodes empty: %v", err)
	}
	if len(emptyNodes) != 0 {
		t.Fatalf("expected empty cluster nodes, got %+v", emptyNodes)
	}
}

func TestClientListUsersRPC(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		_ = mustReadClientEnvelope(t, conn)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: 4096, UserId: 1025, Username: "alice", LoginName: "alice.login", Role: "user"},
					ProtocolVersion: "client-v1alpha2",
				},
			},
		})

		listReq := mustReadClientEnvelope(t, conn)
		if listReq.GetListUsers().GetRequestId() == 0 {
			t.Fatal("expected request_id for list_users")
		}
		if listReq.GetListUsers().GetName() != "" {
			t.Fatalf("unexpected unfiltered list_users name: %q", listReq.GetListUsers().GetName())
		}
		if listReq.GetListUsers().GetUid() != nil {
			t.Fatalf("expected empty uid filter to be omitted, got %+v", listReq.GetListUsers().GetUid())
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_ListUsersResponse{
				ListUsersResponse: &pb.ListUsersResponse{
					RequestId: listReq.GetListUsers().GetRequestId(),
					Items: []*pb.User{
						{NodeId: 4096, UserId: 1025, Username: "alice", LoginName: "alice.login", Role: "user"},
						{NodeId: 4096, UserId: 1026, Username: "bob", LoginName: "", Role: "user", ProfileJson: []byte(`{"display_name":"Bob"}`)},
					},
					Count: 2,
				},
			},
		})

		filterReq := mustReadClientEnvelope(t, conn)
		if filterReq.GetListUsers().GetName() != "bo" {
			t.Fatalf("unexpected filtered list_users name: %q", filterReq.GetListUsers().GetName())
		}
		if got := filterReq.GetListUsers().GetUid(); got == nil || got.GetNodeId() != 4096 || got.GetUserId() != 1026 {
			t.Fatalf("unexpected filtered list_users uid: %+v", got)
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_ListUsersResponse{
				ListUsersResponse: &pb.ListUsersResponse{
					RequestId: filterReq.GetListUsers().GetRequestId(),
					Items: []*pb.User{
						{NodeId: 4096, UserId: 1026, Username: "bob", LoginName: "", Role: "user", ProfileJson: []byte(`{"display_name":"Bob"}`)},
					},
					Count: 1,
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:        server.URL,
		Credentials:    Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("alice-password")},
		RequestTimeout: 2 * time.Second,
		PingInterval:   time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	allUsers, err := client.ListUsers(ctx, "ignored-token", ListUsersRequest{})
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}
	if len(allUsers) != 2 || allUsers[0].LoginName != "alice.login" || allUsers[1].LoginName != "" {
		t.Fatalf("unexpected list users response: %+v", allUsers)
	}

	filteredUsers, err := client.WSListUsers(ctx, ListUsersRequest{
		Name: "bo",
		UID:  UserRef{NodeID: 4096, UserID: 1026},
	})
	if err != nil {
		t.Fatalf("WSListUsers: %v", err)
	}
	if len(filteredUsers) != 1 || filteredUsers[0].UserID != 1026 || filteredUsers[0].LoginName != "" {
		t.Fatalf("unexpected filtered users: %+v", filteredUsers)
	}

	if _, err := client.WSListUsers(ctx, ListUsersRequest{UID: UserRef{NodeID: 4096}}); err == nil {
		t.Fatal("expected validation error for half-empty websocket uid filter")
	}
}

func TestClientBlacklistAndOperationsStatusRPCs(t *testing.T) {
	owner := UserRef{NodeID: 4096, UserID: 1025}
	blocked := UserRef{NodeID: 4096, UserID: 1027}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		_ = mustReadClientEnvelope(t, conn)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: 4096, UserId: 1025, Username: "alice", Role: "user"},
					ProtocolVersion: "client-v1alpha2",
				},
			},
		})

		blockReq := mustReadClientEnvelope(t, conn)
		if blockReq.GetUpsertUserAttachment().GetOwner().GetUserId() != owner.UserID || blockReq.GetUpsertUserAttachment().GetSubject().GetUserId() != blocked.UserID {
			t.Fatalf("unexpected block request: %+v", blockReq.GetUpsertUserAttachment())
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_UpsertUserAttachmentResponse{
				UpsertUserAttachmentResponse: &pb.UpsertUserAttachmentResponse{
					RequestId: blockReq.GetUpsertUserAttachment().GetRequestId(),
					Attachment: &pb.Attachment{
						Owner:          userRefToProto(owner),
						Subject:        userRefToProto(blocked),
						AttachmentType: pb.AttachmentType_ATTACHMENT_TYPE_USER_BLACKLIST,
						AttachedAt:     "hlc-blocked",
						OriginNodeId:   4096,
					},
				},
			},
		})

		listReq := mustReadClientEnvelope(t, conn)
		if listReq.GetListUserAttachments().GetOwner().GetUserId() != owner.UserID {
			t.Fatalf("unexpected list blocked request: %+v", listReq.GetListUserAttachments())
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_ListUserAttachmentsResponse{
				ListUserAttachmentsResponse: &pb.ListUserAttachmentsResponse{
					RequestId: listReq.GetListUserAttachments().GetRequestId(),
					Items: []*pb.Attachment{{
						Owner:          userRefToProto(owner),
						Subject:        userRefToProto(blocked),
						AttachmentType: pb.AttachmentType_ATTACHMENT_TYPE_USER_BLACKLIST,
						AttachedAt:     "hlc-blocked",
						OriginNodeId:   4096,
					}},
					Count: 1,
				},
			},
		})

		unblockReq := mustReadClientEnvelope(t, conn)
		if unblockReq.GetDeleteUserAttachment().GetSubject().GetUserId() != blocked.UserID {
			t.Fatalf("unexpected unblock request: %+v", unblockReq.GetDeleteUserAttachment())
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_DeleteUserAttachmentResponse{
				DeleteUserAttachmentResponse: &pb.DeleteUserAttachmentResponse{
					RequestId: unblockReq.GetDeleteUserAttachment().GetRequestId(),
					Attachment: &pb.Attachment{
						Owner:          userRefToProto(owner),
						Subject:        userRefToProto(blocked),
						AttachmentType: pb.AttachmentType_ATTACHMENT_TYPE_USER_BLACKLIST,
						AttachedAt:     "hlc-blocked",
						DeletedAt:      "hlc-unblocked",
						OriginNodeId:   4096,
					},
				},
			},
		})

		opsReq := mustReadClientEnvelope(t, conn)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_OperationsStatusResponse{
				OperationsStatusResponse: &pb.OperationsStatusResponse{
					RequestId: opsReq.GetOperationsStatus().GetRequestId(),
					Status: &pb.OperationsStatus{
						NodeId: 4096,
						Peers: []*pb.PeerStatus{{
							NodeId:             8192,
							ConfiguredUrl:      "ws://127.0.0.1:9081/internal/cluster/ws",
							Connected:          true,
							Source:             "discovered",
							DiscoveredUrl:      "ws://127.0.0.1:9081/internal/cluster/ws",
							DiscoveryState:     "connected",
							LastDiscoveredAt:   "hlc-discovered",
							LastConnectedAt:    "hlc-connected",
							LastDiscoveryError: "previous error",
						}},
					},
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:        server.URL,
		Credentials:    Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("alice-password")},
		RequestTimeout: 2 * time.Second,
		PingInterval:   time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	entry, err := client.BlockUser(ctx, "", owner, blocked)
	if err != nil {
		t.Fatalf("BlockUser: %v", err)
	}
	if entry.Blocked != blocked || entry.BlockedAt == "" {
		t.Fatalf("unexpected block entry: %+v", entry)
	}

	items, err := client.ListBlockedUsers(ctx, "", owner)
	if err != nil {
		t.Fatalf("ListBlockedUsers: %v", err)
	}
	if len(items) != 1 || items[0].Blocked != blocked {
		t.Fatalf("unexpected blocked users: %+v", items)
	}

	unblocked, err := client.UnblockUser(ctx, "", owner, blocked)
	if err != nil {
		t.Fatalf("UnblockUser: %v", err)
	}
	if unblocked.DeletedAt != "hlc-unblocked" {
		t.Fatalf("unexpected unblock entry: %+v", unblocked)
	}

	status, err := client.OperationsStatus(ctx)
	if err != nil {
		t.Fatalf("OperationsStatus: %v", err)
	}
	if len(status.Peers) != 1 || status.Peers[0].Source != "discovered" || status.Peers[0].DiscoveryState != "connected" {
		t.Fatalf("unexpected operations status: %+v", status)
	}
}

func TestClientUserMetadataRPCs(t *testing.T) {
	owner := UserRef{NodeID: 4096, UserID: 1025}
	key := "prefs.theme"
	expiresAt := "2026-05-01T00:00:00Z"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		_ = mustReadClientEnvelope(t, conn)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: 4096, UserId: 1025, Username: "alice", Role: "user"},
					ProtocolVersion: "client-v1alpha2",
				},
			},
		})

		getReq := mustReadClientEnvelope(t, conn)
		if got := getReq.GetGetUserMetadata(); got == nil || got.GetOwner().GetUserId() != owner.UserID || got.GetKey() != key {
			t.Fatalf("unexpected get metadata request: %+v", got)
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_GetUserMetadataResponse{
				GetUserMetadataResponse: &pb.GetUserMetadataResponse{
					RequestId: getReq.GetGetUserMetadata().GetRequestId(),
					Metadata: &pb.UserMetadata{
						Owner:        userRefToProto(owner),
						Key:          key,
						Value:        []byte{0xff, 0x00},
						UpdatedAt:    "hlc-get",
						ExpiresAt:    expiresAt,
						OriginNodeId: 4096,
					},
				},
			},
		})

		upsertReq := mustReadClientEnvelope(t, conn)
		if got := upsertReq.GetUpsertUserMetadata(); got == nil || got.GetKey() != key || got.GetExpiresAt().GetValue() != expiresAt || len(got.GetValue()) != 0 {
			t.Fatalf("unexpected upsert metadata request: %+v", got)
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_UpsertUserMetadataResponse{
				UpsertUserMetadataResponse: &pb.UpsertUserMetadataResponse{
					RequestId: upsertReq.GetUpsertUserMetadata().GetRequestId(),
					Metadata: &pb.UserMetadata{
						Owner:        userRefToProto(owner),
						Key:          key,
						Value:        []byte{},
						UpdatedAt:    "hlc-upsert",
						ExpiresAt:    expiresAt,
						OriginNodeId: 4096,
					},
				},
			},
		})

		scanReq := mustReadClientEnvelope(t, conn)
		if got := scanReq.GetScanUserMetadata(); got == nil || got.GetPrefix() != "prefs." || got.GetAfter() != key || got.GetLimit() != 2 {
			t.Fatalf("unexpected scan metadata request: %+v", got)
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_ScanUserMetadataResponse{
				ScanUserMetadataResponse: &pb.ScanUserMetadataResponse{
					RequestId: scanReq.GetScanUserMetadata().GetRequestId(),
					Items: []*pb.UserMetadata{
						{
							Owner:        userRefToProto(owner),
							Key:          "prefs.alpha",
							Value:        []byte{0xaa},
							UpdatedAt:    "hlc-scan-1",
							OriginNodeId: 4096,
						},
						{
							Owner:        userRefToProto(owner),
							Key:          "prefs.beta",
							Value:        []byte("next"),
							UpdatedAt:    "hlc-scan-2",
							ExpiresAt:    expiresAt,
							OriginNodeId: 4096,
						},
					},
					Count:     2,
					NextAfter: "prefs.beta",
				},
			},
		})

		deleteReq := mustReadClientEnvelope(t, conn)
		if got := deleteReq.GetDeleteUserMetadata(); got == nil || got.GetKey() != key {
			t.Fatalf("unexpected delete metadata request: %+v", got)
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_DeleteUserMetadataResponse{
				DeleteUserMetadataResponse: &pb.DeleteUserMetadataResponse{
					RequestId: deleteReq.GetDeleteUserMetadata().GetRequestId(),
					Metadata: &pb.UserMetadata{
						Owner:        userRefToProto(owner),
						Key:          key,
						Value:        []byte("gone"),
						UpdatedAt:    "hlc-upsert",
						DeletedAt:    "hlc-deleted",
						ExpiresAt:    expiresAt,
						OriginNodeId: 4096,
					},
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:        server.URL,
		Credentials:    Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("alice-password")},
		RequestTimeout: 2 * time.Second,
		PingInterval:   time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	metadata, err := client.GetUserMetadata(ctx, "", owner, key)
	if err != nil {
		t.Fatalf("GetUserMetadata: %v", err)
	}
	if metadata.Key != key || metadata.UpdatedAt != "hlc-get" || metadata.ExpiresAt != expiresAt || len(metadata.Value) != 2 || metadata.Value[0] != 0xff {
		t.Fatalf("unexpected metadata: %+v", metadata)
	}

	upserted, err := client.UpsertUserMetadata(ctx, "", owner, key, UpsertUserMetadataRequest{
		Value:     []byte{},
		ExpiresAt: &expiresAt,
	})
	if err != nil {
		t.Fatalf("UpsertUserMetadata: %v", err)
	}
	if upserted.Key != key || len(upserted.Value) != 0 || upserted.UpdatedAt != "hlc-upsert" {
		t.Fatalf("unexpected upserted metadata: %+v", upserted)
	}

	page, err := client.ScanUserMetadata(ctx, "", owner, ScanUserMetadataRequest{
		Prefix: "prefs.",
		After:  key,
		Limit:  2,
	})
	if err != nil {
		t.Fatalf("ScanUserMetadata: %v", err)
	}
	if !page.HasMore() || page.Count != 2 || page.NextAfter != "prefs.beta" || len(page.Items) != 2 || page.Items[1].ExpiresAt != expiresAt {
		t.Fatalf("unexpected metadata page: %+v", page)
	}

	deleted, err := client.DeleteUserMetadata(ctx, "", owner, key)
	if err != nil {
		t.Fatalf("DeleteUserMetadata: %v", err)
	}
	if deleted.DeletedAt != "hlc-deleted" || string(deleted.Value) != "gone" {
		t.Fatalf("unexpected deleted metadata: %+v", deleted)
	}
}

func TestClientListNodeLoggedInUsersRequiresNodeID(t *testing.T) {
	client, err := NewClient(Config{
		BaseURL:      "http://127.0.0.1:8080",
		Credentials:  Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("alice-password")},
		PingInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	if _, err := client.ListNodeLoggedInUsers(context.Background(), 0); err == nil {
		t.Fatal("expected validation error for empty node_id")
	}
}

func TestClientSendPacketRejectsInvalidTargetSession(t *testing.T) {
	client, err := NewClient(Config{
		BaseURL:      "http://127.0.0.1:8080",
		Credentials:  Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("alice-password")},
		PingInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	_, err = client.SendPacket(context.Background(), SendPacketInput{
		Target:        UserRef{NodeID: 4096, UserID: 1025},
		Body:          []byte("payload"),
		DeliveryMode:  DeliveryModeBestEffort,
		TargetSession: SessionRef{ServingNodeID: 4096},
	})
	if err == nil {
		t.Fatal("expected validation error for invalid target_session")
	}
}

func TestClientReconnectUsesSeenMessages(t *testing.T) {
	store := &recordingStore{}
	var attempts atomic.Int32
	var secondSeen []*pb.MessageCursor

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		attempt := attempts.Add(1)
		login := mustReadClientEnvelope(t, conn)

		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: 4096, UserId: 1025, Username: "alice", Role: "user"},
					ProtocolVersion: "client-v1alpha1",
				},
			},
		})

		if attempt == 1 {
			writeServerEnvelope(t, conn, &pb.ServerEnvelope{
				Body: &pb.ServerEnvelope_MessagePushed{
					MessagePushed: &pb.MessagePushed{
						Message: &pb.Message{
							Recipient:    &pb.UserRef{NodeId: 4096, UserId: 1025},
							NodeId:       4096,
							Seq:          11,
							Sender:       &pb.UserRef{NodeId: 4096, UserId: 1},
							Body:         []byte("hello"),
							CreatedAtHlc: "hlc1",
						},
					},
				},
			})
			_ = mustReadClientEnvelope(t, conn)
			conn.Close(websocket.StatusGoingAway, "disconnect")
			return
		}

		secondSeen = append([]*pb.MessageCursor(nil), login.GetLogin().SeenMessages...)
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:               server.URL,
		Credentials:           Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("alice-password")},
		CursorStore:           store,
		Reconnect:             true,
		InitialReconnectDelay: 10 * time.Millisecond,
		MaxReconnectDelay:     20 * time.Millisecond,
		PingInterval:          time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if attempts.Load() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if attempts.Load() < 2 {
		t.Fatalf("expected reconnect attempt, got %d", attempts.Load())
	}
	if len(secondSeen) != 1 || secondSeen[0].NodeId != 4096 || secondSeen[0].Seq != 11 {
		t.Fatalf("unexpected seen_messages on reconnect: %#v", secondSeen)
	}
	_ = client.Close()
}

func TestClientUsesProvidedHashedPasswordForWSLogin(t *testing.T) {
	password := MustPlainPassword("secret")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		login := mustReadClientEnvelope(t, conn)
		if got := login.GetLogin().GetPassword(); got != password.WireValue() {
			t.Fatalf("expected hashed password to be sent unchanged, got %q", got)
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: 4096, UserId: 1025, Username: "alice", Role: "user"},
					ProtocolVersion: "client-v1alpha1",
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:      server.URL,
		Credentials:  Credentials{NodeID: 4096, UserID: 1025, Password: HashedPassword(password.WireValue())},
		PingInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
}

func TestClientCanConnectWithLoginNameSelector(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		login := mustReadClientEnvelope(t, conn)
		if login.GetLogin().GetUser() != nil {
			t.Fatalf("did not expect legacy user selector in login request: %+v", login.GetLogin())
		}
		if got := login.GetLogin().GetLoginName(); got != "alice.login" {
			t.Fatalf("unexpected login_name selector: %q", got)
		}
		if err := bcrypt.CompareHashAndPassword([]byte(login.GetLogin().GetPassword()), []byte("alice-password")); err != nil {
			t.Fatalf("expected bcrypt password, got %v", err)
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User: &pb.User{
						NodeId:    4096,
						UserId:    1025,
						Username:  "alice",
						LoginName: "alice.login",
						Role:      "user",
					},
					ProtocolVersion: "client-v1alpha1",
					SessionRef:      &pb.SessionRef{ServingNodeId: 4096, SessionId: "session-login-name"},
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:      server.URL,
		Credentials:  Credentials{LoginName: "alice.login", Password: MustPlainPassword("alice-password")},
		PingInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	loginInfo, ok := client.CurrentLogin()
	if !ok {
		t.Fatal("expected current login info")
	}
	if loginInfo.User.LoginName != "alice.login" || loginInfo.SessionRef.SessionID != "session-login-name" {
		t.Fatalf("unexpected login info: %+v", loginInfo)
	}
}

func TestClientUpdateUserHashesPassword(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		_ = mustReadClientEnvelope(t, conn)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: 4096, UserId: 1025, Username: "alice", Role: "user"},
					ProtocolVersion: "client-v1alpha1",
				},
			},
		})

		updateReq := mustReadClientEnvelope(t, conn)
		password := updateReq.GetUpdateUser().GetPassword().GetValue()
		if password == "new-password" {
			t.Fatal("expected update password to be hashed")
		}
		if err := bcrypt.CompareHashAndPassword([]byte(password), []byte("new-password")); err != nil {
			t.Fatalf("expected bcrypt password, got %v", err)
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_UpdateUserResponse{
				UpdateUserResponse: &pb.UpdateUserResponse{
					RequestId: updateReq.GetUpdateUser().GetRequestId(),
					User: &pb.User{
						NodeId:   4096,
						UserId:   1025,
						Username: "alice",
						Role:     "user",
					},
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:      server.URL,
		Credentials:  Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("alice-password")},
		PingInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	passwordInput := MustPlainPassword("new-password")
	if _, err := client.UpdateUser(ctx, UserRef{NodeID: 4096, UserID: 1025}, UpdateUserRequest{
		Password: &passwordInput,
	}); err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}
}

func TestClientUpdateUserCarriesLoginNameField(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		_ = mustReadClientEnvelope(t, conn)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: 4096, UserId: 1025, Username: "alice", LoginName: "alice.login", Role: "user"},
					ProtocolVersion: "client-v1alpha1",
				},
			},
		})

		updateReq := mustReadClientEnvelope(t, conn)
		loginNameField := updateReq.GetUpdateUser().GetLoginName()
		if loginNameField == nil {
			t.Fatal("expected login_name field in update request")
		}
		if loginNameField.GetValue() != "" {
			t.Fatalf("expected empty login_name to preserve explicit unbind, got %+v", loginNameField)
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_UpdateUserResponse{
				UpdateUserResponse: &pb.UpdateUserResponse{
					RequestId: updateReq.GetUpdateUser().GetRequestId(),
					User: &pb.User{
						NodeId:    4096,
						UserId:    1025,
						Username:  "alice",
						LoginName: "",
						Role:      "user",
					},
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:      server.URL,
		Credentials:  Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("alice-password")},
		PingInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	loginName := ""
	user, err := client.UpdateUser(ctx, UserRef{NodeID: 4096, UserID: 1025}, UpdateUserRequest{
		LoginName: &loginName,
	})
	if err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}
	if user.LoginName != "" {
		t.Fatalf("expected cleared login_name in update response: %+v", user)
	}
}

func appendIfMissing(in []MessageCursor, cursor MessageCursor) []MessageCursor {
	for _, existing := range in {
		if existing == cursor {
			return in
		}
	}
	return append(in, cursor)
}

func mustReadClientEnvelope(t *testing.T, conn *websocket.Conn) *pb.ClientEnvelope {
	t.Helper()
	_, payload, err := conn.Read(context.Background())
	if err != nil {
		t.Fatalf("read client envelope: %v", err)
	}
	var env pb.ClientEnvelope
	if err := proto.Unmarshal(payload, &env); err != nil {
		t.Fatalf("unmarshal client envelope: %v", err)
	}
	return &env
}

func writeServerEnvelope(t *testing.T, conn *websocket.Conn, env *pb.ServerEnvelope) {
	t.Helper()
	payload, err := proto.Marshal(env)
	if err != nil {
		t.Fatalf("marshal server envelope: %v", err)
	}
	if err := conn.Write(context.Background(), websocket.MessageBinary, payload); err != nil {
		t.Fatalf("write server envelope: %v", err)
	}
}
