package turntf

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"

	pb "github.com/tursom/turntf-go/internal/proto"
)

func TestHTTPClientRequestsAndEncoding(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode login request: %v", err)
		}
		json.NewEncoder(w).Encode(map[string]any{"token": "admin-token"})
	})
	mux.HandleFunc("/users", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer admin-token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		var req CreateUserRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode create user: %v", err)
		}
		json.NewEncoder(w).Encode(User{NodeID: 4096, UserID: 1025, Username: req.Username, Role: req.Role})
	})
	mux.HandleFunc("/nodes/4096/users/1025/subscriptions", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer admin-token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/nodes/4096/users/1025/messages", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer admin-token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		switch r.Method {
		case http.MethodGet:
			if got := r.URL.Query().Get("limit"); got != "20" {
				t.Fatalf("unexpected limit: %q", got)
			}
			json.NewEncoder(w).Encode([]Message{{
				Recipient:    UserRef{NodeID: 4096, UserID: 1025},
				NodeID:       4096,
				Seq:          3,
				Sender:       UserRef{NodeID: 4096, UserID: 1},
				Body:         []byte{0xff, 0x00},
				CreatedAtHLC: "hlc1",
			}})
		case http.MethodPost:
			var raw map[string]any
			if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
				t.Fatalf("decode post message: %v", err)
			}
			if raw["body"] != base64.StdEncoding.EncodeToString([]byte{0xff, 0x00}) {
				t.Fatalf("body was not base64 encoded: %#v", raw["body"])
			}
			json.NewEncoder(w).Encode(Message{
				Recipient:    UserRef{NodeID: 4096, UserID: 1025},
				NodeID:       4096,
				Seq:          4,
				Sender:       UserRef{NodeID: 4096, UserID: 1},
				Body:         []byte{0xff, 0x00},
				CreatedAtHLC: "hlc2",
			})
		default:
			t.Fatalf("unexpected method: %s", r.Method)
		}
	})
	mux.HandleFunc("/nodes/8192/users/3/messages", func(w http.ResponseWriter, r *http.Request) {
		var raw map[string]any
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Fatalf("decode post packet: %v", err)
		}
		if raw["delivery_mode"] != "route_retry" {
			t.Fatalf("unexpected delivery mode: %#v", raw["delivery_mode"])
		}
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("/cluster/nodes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method for cluster nodes: %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer admin-token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		json.NewEncoder(w).Encode([]ClusterNode{
			{NodeID: 4096, IsLocal: true},
			{NodeID: 8192, IsLocal: false, ConfiguredURL: "ws://127.0.0.1:9081/internal/cluster/ws"},
		})
	})
	mux.HandleFunc("/cluster/nodes/4096/logged-in-users", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method for logged-in users: %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer admin-token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		json.NewEncoder(w).Encode([]LoggedInUser{
			{NodeID: 4096, UserID: 1025, Username: "alice"},
			{NodeID: 4096, UserID: 1026, Username: "bob"},
		})
	})
	mux.HandleFunc("/cluster/nodes/8192/logged-in-users", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method for empty logged-in users: %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer admin-token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		json.NewEncoder(w).Encode([]LoggedInUser{})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	client := NewHTTPClient(server.URL)
	ctx := context.Background()

	token, err := client.Login(ctx, 4096, 1, "root")
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if token != "admin-token" {
		t.Fatalf("unexpected token: %q", token)
	}

	user, err := client.CreateUser(ctx, token, CreateUserRequest{
		Username: "alice",
		Password: "alice-password",
		Role:     "user",
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if user.UserID != 1025 {
		t.Fatalf("unexpected user: %+v", user)
	}

	if err := client.CreateSubscription(ctx, token, UserRef{NodeID: 4096, UserID: 1025}, UserRef{NodeID: 4096, UserID: 1026}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}

	messages, err := client.ListMessages(ctx, token, UserRef{NodeID: 4096, UserID: 1025}, 20)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(messages) != 1 || len(messages[0].Body) != 2 {
		t.Fatalf("unexpected messages: %+v", messages)
	}

	message, err := client.PostMessage(ctx, token, UserRef{NodeID: 4096, UserID: 1025}, []byte{0xff, 0x00})
	if err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	if message.Seq != 4 {
		t.Fatalf("unexpected post message response: %+v", message)
	}

	if err := client.PostPacket(ctx, token, 8192, UserRef{NodeID: 8192, UserID: 1025}, []byte{0xff, 0x00}, DeliveryModeRouteRetry); err != nil {
		t.Fatalf("PostPacket: %v", err)
	}

	nodes, err := client.ListClusterNodes(ctx, token)
	if err != nil {
		t.Fatalf("ListClusterNodes: %v", err)
	}
	if len(nodes) != 2 || !nodes[0].IsLocal || nodes[1].ConfiguredURL == "" {
		t.Fatalf("unexpected cluster nodes: %+v", nodes)
	}

	users, err := client.ListNodeLoggedInUsers(ctx, token, 4096)
	if err != nil {
		t.Fatalf("ListNodeLoggedInUsers: %v", err)
	}
	if len(users) != 2 || users[0].Username != "alice" || users[1].UserID != 1026 {
		t.Fatalf("unexpected logged-in users: %+v", users)
	}

	emptyUsers, err := client.ListNodeLoggedInUsers(ctx, token, 8192)
	if err != nil {
		t.Fatalf("ListNodeLoggedInUsers empty: %v", err)
	}
	if len(emptyUsers) != 0 {
		t.Fatalf("expected empty logged-in users, got %+v", emptyUsers)
	}
}

func TestHTTPClientListNodeLoggedInUsersRequiresNodeID(t *testing.T) {
	client := NewHTTPClient("http://127.0.0.1:8080")
	if _, err := client.ListNodeLoggedInUsers(context.Background(), "token", 0); err == nil {
		t.Fatal("expected validation error for empty node_id")
	}
}

func TestIntegratedClientUsesHTTPLoginAndWSRPC(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/login", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"token": "admin-token"})
	})
	mux.HandleFunc("/ws/client", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Fatalf("accept websocket: %v", err)
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		login := mustReadClientEnvelope(t, conn)
		if login.GetLogin().GetUser().GetUserId() != 1025 {
			t.Fatalf("unexpected login user: %+v", login.GetLogin())
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: 4096, UserId: 1025, Username: "alice", Role: "user"},
					ProtocolVersion: "client-v1alpha1",
				},
			},
		})

		createReq := mustReadClientEnvelope(t, conn)
		if createReq.GetCreateUser().Username != "alice" {
			t.Fatalf("unexpected create user request: %+v", createReq.GetCreateUser())
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_CreateUserResponse{
				CreateUserResponse: &pb.CreateUserResponse{
					RequestId: createReq.GetCreateUser().RequestId,
					User: &pb.User{
						NodeId:   4096,
						UserId:   1025,
						Username: "alice",
						Role:     "user",
					},
				},
			},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:      server.URL,
		Credentials:  Credentials{NodeID: 4096, UserID: 1025, Password: "alice-password"},
		PingInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	token, err := client.Login(ctx, 4096, 1, "root")
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if token != "admin-token" {
		t.Fatalf("unexpected token: %q", token)
	}

	connectCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := client.Connect(connectCtx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	user, err := client.CreateUser(ctx, token, CreateUserRequest{
		Username: "alice",
		Password: "alice-password",
		Role:     "user",
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if user.Username != "alice" {
		t.Fatalf("unexpected user: %+v", user)
	}
	if client.HTTP() == nil {
		t.Fatal("expected HTTP accessor to return client")
	}
}
