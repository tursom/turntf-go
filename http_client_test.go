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
	"golang.org/x/crypto/bcrypt"

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
		password, _ := req["password"].(string)
		if password == "root" {
			t.Fatal("expected hashed login password")
		}
		if err := bcrypt.CompareHashAndPassword([]byte(password), []byte("root")); err != nil {
			t.Fatalf("expected bcrypt password, got %v", err)
		}
		json.NewEncoder(w).Encode(map[string]any{"token": "admin-token"})
	})
	mux.HandleFunc("/users", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer admin-token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode create user: %v", err)
		}
		password, _ := req["password"].(string)
		if password == "alice-password" {
			t.Fatal("expected create user password to be hashed")
		}
		if err := bcrypt.CompareHashAndPassword([]byte(password), []byte("alice-password")); err != nil {
			t.Fatalf("expected bcrypt password, got %v", err)
		}
		if _, ok := req["profile"]; !ok {
			t.Fatal("expected profile field in create user request")
		}
		if _, ok := req["profile_json"]; ok {
			t.Fatal("did not expect legacy profile_json field in create user request")
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{
			"node_id":    4096,
			"user_id":    1025,
			"username":   req["username"],
			"role":       req["role"],
			"profile":    map[string]any{"tier": "gold"},
			"created_at": "hlc-created",
		})
	})
	mux.HandleFunc("/nodes/4096/users/1025/attachments/channel_subscription/4096/1026", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("unexpected subscription method: %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer admin-token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(Attachment{
			Owner:          UserRef{NodeID: 4096, UserID: 1025},
			Subject:        UserRef{NodeID: 4096, UserID: 1026},
			AttachmentType: AttachmentTypeChannelSubscription,
			AttachedAt:     "hlc-subscribed",
			OriginNodeID:   4096,
		})
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
			json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{{
					"recipient":  UserRef{NodeID: 4096, UserID: 1025},
					"node_id":    4096,
					"seq":        3,
					"sender":     UserRef{NodeID: 4096, UserID: 1},
					"body":       []byte{0xff, 0x00},
					"created_at": "hlc1",
				}},
				"count": 1,
			})
		case http.MethodPost:
			var raw map[string]any
			if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
				t.Fatalf("decode post message: %v", err)
			}
			if raw["body"] != base64.StdEncoding.EncodeToString([]byte{0xff, 0x00}) {
				t.Fatalf("body was not base64 encoded: %#v", raw["body"])
			}
			if raw["delivery_kind"] != nil {
				t.Fatalf("unexpected persistent delivery kind: %#v", raw["delivery_kind"])
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]any{
				"recipient":  UserRef{NodeID: 4096, UserID: 1025},
				"node_id":    4096,
				"seq":        4,
				"sender":     UserRef{NodeID: 4096, UserID: 1},
				"body":       []byte{0xff, 0x00},
				"created_at": "hlc2",
			})
		default:
			t.Fatalf("unexpected method: %s", r.Method)
		}
	})
	mux.HandleFunc("/nodes/8192/users/1025/messages", func(w http.ResponseWriter, r *http.Request) {
		var raw map[string]any
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Fatalf("decode post packet: %v", err)
		}
		if raw["delivery_kind"] != "transient" {
			t.Fatalf("unexpected delivery kind: %#v", raw["delivery_kind"])
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
		json.NewEncoder(w).Encode(map[string]any{"nodes": []ClusterNode{
			{NodeID: 4096, IsLocal: true},
			{NodeID: 8192, IsLocal: false, ConfiguredURL: "ws://127.0.0.1:9081/internal/cluster/ws", Source: "discovered"},
		}})
	})
	mux.HandleFunc("/cluster/nodes/4096/logged-in-users", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method for logged-in users: %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer admin-token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		json.NewEncoder(w).Encode(map[string]any{
			"target_node_id": 4096,
			"items": []LoggedInUser{
				{NodeID: 4096, UserID: 1025, Username: "alice"},
				{NodeID: 4096, UserID: 1026, Username: "bob"},
			},
			"count": 2,
		})
	})
	mux.HandleFunc("/cluster/nodes/8192/logged-in-users", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method for empty logged-in users: %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer admin-token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		json.NewEncoder(w).Encode(map[string]any{"target_node_id": 8192, "items": []LoggedInUser{}, "count": 0})
	})
	mux.HandleFunc("/nodes/4096/users/1025/attachments/user_blacklist/4096/1027", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer admin-token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		switch r.Method {
		case http.MethodPut:
			var raw map[string]any
			if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
				t.Fatalf("decode block user: %v", err)
			}
			configJSON, ok := raw["config_json"].(map[string]any)
			if !ok || len(configJSON) != 0 {
				t.Fatalf("unexpected block request: %#v", raw)
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(Attachment{
				Owner:          UserRef{NodeID: 4096, UserID: 1025},
				Subject:        UserRef{NodeID: 4096, UserID: 1027},
				AttachmentType: AttachmentTypeUserBlacklist,
				AttachedAt:     "hlc-blocked",
				OriginNodeID:   4096,
			})
		case http.MethodDelete:
			json.NewEncoder(w).Encode(Attachment{
				Owner:          UserRef{NodeID: 4096, UserID: 1025},
				Subject:        UserRef{NodeID: 4096, UserID: 1027},
				AttachmentType: AttachmentTypeUserBlacklist,
				AttachedAt:     "hlc-blocked",
				DeletedAt:      "hlc-unblocked",
				OriginNodeID:   4096,
			})
		default:
			t.Fatalf("unexpected blacklist method: %s", r.Method)
		}
	})
	mux.HandleFunc("/nodes/4096/users/1025/attachments", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected attachments method: %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer admin-token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		if got := r.URL.Query().Get("attachment_type"); got != "user_blacklist" {
			t.Fatalf("unexpected attachment_type: %q", got)
		}
		json.NewEncoder(w).Encode(map[string]any{
			"items": []Attachment{{
				Owner:          UserRef{NodeID: 4096, UserID: 1025},
				Subject:        UserRef{NodeID: 4096, UserID: 1027},
				AttachmentType: AttachmentTypeUserBlacklist,
				AttachedAt:     "hlc-blocked",
				OriginNodeID:   4096,
			}},
			"count": 1,
		})
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
		Username:    "alice",
		Password:    MustPlainPassword("alice-password"),
		ProfileJSON: []byte(`{"tier":"gold"}`),
		Role:        "user",
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if user.UserID != 1025 {
		t.Fatalf("unexpected user: %+v", user)
	}
	if string(user.ProfileJSON) != `{"tier":"gold"}` {
		t.Fatalf("unexpected profile json: %s", user.ProfileJSON)
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
	if messages[0].CreatedAtHLC != "hlc1" {
		t.Fatalf("unexpected message created_at: %+v", messages[0])
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
	if len(nodes) != 2 || !nodes[0].IsLocal || nodes[1].ConfiguredURL == "" || nodes[1].Source != "discovered" {
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

	entry, err := client.BlockUser(ctx, token, UserRef{NodeID: 4096, UserID: 1025}, UserRef{NodeID: 4096, UserID: 1027})
	if err != nil {
		t.Fatalf("BlockUser: %v", err)
	}
	if entry.Blocked.UserID != 1027 || entry.BlockedAt == "" {
		t.Fatalf("unexpected block entry: %+v", entry)
	}

	blocked, err := client.ListBlockedUsers(ctx, token, UserRef{NodeID: 4096, UserID: 1025})
	if err != nil {
		t.Fatalf("ListBlockedUsers: %v", err)
	}
	if len(blocked) != 1 || blocked[0].Blocked.UserID != 1027 {
		t.Fatalf("unexpected blocked users: %+v", blocked)
	}

	unblocked, err := client.UnblockUser(ctx, token, UserRef{NodeID: 4096, UserID: 1025}, UserRef{NodeID: 4096, UserID: 1027})
	if err != nil {
		t.Fatalf("UnblockUser: %v", err)
	}
	if unblocked.DeletedAt == "" {
		t.Fatalf("expected deleted_at in unblock response: %+v", unblocked)
	}
}

func TestHTTPClientListNodeLoggedInUsersRequiresNodeID(t *testing.T) {
	client := NewHTTPClient("http://127.0.0.1:8080")
	if _, err := client.ListNodeLoggedInUsers(context.Background(), "token", 0); err == nil {
		t.Fatal("expected validation error for empty node_id")
	}
}

func TestHTTPClientUserMetadataRequests(t *testing.T) {
	owner := UserRef{NodeID: 4096, UserID: 1025}
	key := "prefs.theme"
	expiresAt := "2026-05-01T00:00:00Z"

	mux := http.NewServeMux()
	mux.HandleFunc("/nodes/4096/users/1025/metadata", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer admin-token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected metadata scan method: %s", r.Method)
		}
		if got := r.URL.Query().Get("prefix"); got != "prefs." {
			t.Fatalf("unexpected prefix: %q", got)
		}
		if got := r.URL.Query().Get("after"); got != key {
			t.Fatalf("unexpected after: %q", got)
		}
		if got := r.URL.Query().Get("limit"); got != "2" {
			t.Fatalf("unexpected limit: %q", got)
		}
		json.NewEncoder(w).Encode(UserMetadataPage{
			Items: []UserMetadata{
				{
					Owner:        owner,
					Key:          "prefs.alpha",
					Value:        []byte{0xff, 0x00},
					UpdatedAt:    "hlc-scan-1",
					OriginNodeID: 4096,
				},
				{
					Owner:        owner,
					Key:          "prefs.beta",
					Value:        []byte("next"),
					UpdatedAt:    "hlc-scan-2",
					ExpiresAt:    expiresAt,
					OriginNodeID: 4096,
				},
			},
			Count:     2,
			NextAfter: "prefs.beta",
		})
	})
	mux.HandleFunc("/nodes/4096/users/1025/metadata/", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer admin-token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		if got := r.URL.EscapedPath(); got != "/nodes/4096/users/1025/metadata/prefs.theme" {
			t.Fatalf("unexpected escaped path: %q", got)
		}
		switch r.Method {
		case http.MethodGet:
			json.NewEncoder(w).Encode(UserMetadata{
				Owner:        owner,
				Key:          key,
				Value:        []byte{0xaa, 0xbb},
				UpdatedAt:    "hlc-get",
				ExpiresAt:    expiresAt,
				OriginNodeID: 4096,
			})
		case http.MethodPut:
			var raw map[string]any
			if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
				t.Fatalf("decode upsert metadata: %v", err)
			}
			if raw["value"] != "" {
				t.Fatalf("expected empty metadata value to encode as empty base64 string, got %#v", raw["value"])
			}
			if raw["expires_at"] != expiresAt {
				t.Fatalf("unexpected expires_at: %#v", raw["expires_at"])
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(UserMetadata{
				Owner:        owner,
				Key:          key,
				Value:        []byte{},
				UpdatedAt:    "hlc-upsert",
				ExpiresAt:    expiresAt,
				OriginNodeID: 4096,
			})
		case http.MethodDelete:
			json.NewEncoder(w).Encode(UserMetadata{
				Owner:        owner,
				Key:          key,
				Value:        []byte("gone"),
				UpdatedAt:    "hlc-upsert",
				DeletedAt:    "hlc-deleted",
				ExpiresAt:    expiresAt,
				OriginNodeID: 4096,
			})
		default:
			t.Fatalf("unexpected metadata method: %s", r.Method)
		}
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	client := NewHTTPClient(server.URL)
	ctx := context.Background()

	metadata, err := client.GetUserMetadata(ctx, "admin-token", owner, key)
	if err != nil {
		t.Fatalf("GetUserMetadata: %v", err)
	}
	if metadata.Key != key || base64.StdEncoding.EncodeToString(metadata.Value) != "qrs=" || metadata.ExpiresAt != expiresAt {
		t.Fatalf("unexpected metadata: %+v", metadata)
	}

	upserted, err := client.UpsertUserMetadata(ctx, "admin-token", owner, key, UpsertUserMetadataRequest{
		Value:     []byte{},
		ExpiresAt: &expiresAt,
	})
	if err != nil {
		t.Fatalf("UpsertUserMetadata: %v", err)
	}
	if upserted.Key != key || len(upserted.Value) != 0 || upserted.UpdatedAt != "hlc-upsert" {
		t.Fatalf("unexpected upsert response: %+v", upserted)
	}

	page, err := client.ScanUserMetadata(ctx, "admin-token", owner, ScanUserMetadataRequest{
		Prefix: "prefs.",
		After:  key,
		Limit:  2,
	})
	if err != nil {
		t.Fatalf("ScanUserMetadata: %v", err)
	}
	if !page.HasMore() || page.Count != 2 || page.NextAfter != "prefs.beta" {
		t.Fatalf("unexpected metadata page: %+v", page)
	}
	if len(page.Items) != 2 || page.Items[0].Key != "prefs.alpha" || base64.StdEncoding.EncodeToString(page.Items[0].Value) != "/wA=" {
		t.Fatalf("unexpected metadata items: %+v", page.Items)
	}

	deleted, err := client.DeleteUserMetadata(ctx, "admin-token", owner, key)
	if err != nil {
		t.Fatalf("DeleteUserMetadata: %v", err)
	}
	if deleted.DeletedAt != "hlc-deleted" || string(deleted.Value) != "gone" {
		t.Fatalf("unexpected deleted metadata: %+v", deleted)
	}
}

func TestIntegratedClientUsesHTTPLoginAndWSRPC(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/login", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode login request: %v", err)
		}
		password, _ := req["password"].(string)
		if password == "root" {
			t.Fatal("expected hashed login password")
		}
		if err := bcrypt.CompareHashAndPassword([]byte(password), []byte("root")); err != nil {
			t.Fatalf("expected bcrypt password, got %v", err)
		}
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
		if createReq.GetCreateUser().Password == "alice-password" {
			t.Fatal("expected ws create user password to be hashed")
		}
		if err := bcrypt.CompareHashAndPassword([]byte(createReq.GetCreateUser().Password), []byte("alice-password")); err != nil {
			t.Fatalf("expected bcrypt password, got %v", err)
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
		Credentials:  Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("alice-password")},
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
		Password: MustPlainPassword("alice-password"),
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
