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
		firstSeen = append([]*pb.MessageCursor(nil), login.GetLogin().SeenMessages...)

		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User: &pb.User{
						NodeId:   4096,
						UserId:   1025,
						Username: "alice",
						Role:     "user",
					},
					ProtocolVersion: "client-v1alpha1",
				},
			},
		})

		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_MessagePushed{
				MessagePushed: &pb.MessagePushed{
					Message: &pb.Message{
						UserNodeId:   4096,
						UserId:       1025,
						NodeId:       4096,
						Seq:          7,
						Sender:       "orders",
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
							UserNodeId:   4096,
							UserId:       1025,
							NodeId:       4096,
							Seq:          8,
							Sender:       sendReq.GetSendMessage().Sender,
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
		Credentials:    Credentials{NodeID: 4096, UserID: 1025, Password: "alice-password"},
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
		Sender: "mobile",
		Body:   []byte("payload"),
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if msg.Seq != 8 {
		t.Fatalf("unexpected message seq: %d", msg.Seq)
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
	if len(handler.messages) != 1 {
		t.Fatalf("expected 1 pushed message, got %d", len(handler.messages))
	}
	if got, want := store.saved, []string{"message", "cursor", "message", "cursor"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Fatalf("unexpected store operation order: %#v", got)
	}
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
		Credentials:           Credentials{NodeID: 4096, UserID: 1025, Password: "wrong"},
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
							UserNodeId:   4096,
							UserId:       1025,
							NodeId:       4096,
							Seq:          11,
							Sender:       "orders",
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
		Credentials:           Credentials{NodeID: 4096, UserID: 1025, Password: "alice-password"},
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
