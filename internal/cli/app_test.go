package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	pb "github.com/tursom/turntf-go/internal/proto"
)

func TestRunLoginOutputsToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/auth/login" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"token": "admin-token"})
	}))
	defer server.Close()

	var stdout bytes.Buffer
	err := Run(context.Background(), []string{
		"--base-url", server.URL,
		"--admin-node-id", "4096",
		"--admin-user-id", "1",
		"--admin-password", "root",
		"login",
	}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Run(login): %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "admin-token" {
		t.Fatalf("unexpected token output: %q", got)
	}
}

func TestRunListenPrintsEventsAndFailsOnDisconnect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Fatalf("accept websocket: %v", err)
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		login := mustReadClientEnvelope(t, conn)
		if login.GetLogin().UserId != 1025 {
			t.Fatalf("unexpected login: %+v", login.GetLogin())
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: 4096, UserId: 1025, Username: "alice", Role: "user"},
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
						Sender:       "demo",
						Body:         []byte("hello"),
						CreatedAtHlc: "hlc1",
					},
				},
			},
		})
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_PacketPushed{
				PacketPushed: &pb.PacketPushed{
					Packet: &pb.Packet{
						PacketId:     99,
						SourceNodeId: 4096,
						TargetNodeId: 4096,
						Recipient:    &pb.UserRef{NodeId: 4096, UserId: 1025},
						Sender:       "demo",
						Body:         []byte("pkt"),
						DeliveryMode: pb.ClientDeliveryMode_CLIENT_DELIVERY_MODE_BEST_EFFORT,
					},
				},
			},
		})
	}))
	defer server.Close()

	var stdout bytes.Buffer
	err := Run(context.Background(), []string{
		"--base-url", server.URL,
		"--node-id", "4096",
		"--user-id", "1025",
		"--password", "alice-password",
		"--ping-interval", "1h",
		"listen",
	}, &stdout, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected disconnect error")
	}
	if !strings.Contains(stdout.String(), "login user=4096:1025") {
		t.Fatalf("missing login output: %s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "message target=4096:1025") {
		t.Fatalf("missing message output: %s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "packet packet_id=99") {
		t.Fatalf("missing packet output: %s", stdout.String())
	}
}

func TestRunSendMessageOutputsResponseJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Fatalf("accept websocket: %v", err)
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		_ = mustReadClientEnvelope(t, conn)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: 4096, UserId: 1, Username: "root", Role: "admin"},
					ProtocolVersion: "client-v1alpha1",
				},
			},
		})
		req := mustReadClientEnvelope(t, conn)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_SendMessageResponse{
				SendMessageResponse: &pb.SendMessageResponse{
					RequestId: req.GetSendMessage().RequestId,
					Body: &pb.SendMessageResponse_Message{
						Message: &pb.Message{
							UserNodeId:   4096,
							UserId:       1025,
							NodeId:       4096,
							Seq:          11,
							Sender:       req.GetSendMessage().Sender,
							Body:         req.GetSendMessage().Body,
							CreatedAtHlc: "hlc2",
						},
					},
				},
			},
		})
	}))
	defer server.Close()

	var stdout bytes.Buffer
	err := Run(context.Background(), []string{
		"--base-url", server.URL,
		"--node-id", "4096",
		"--user-id", "1",
		"--password", "root",
		"--json",
		"send-message",
		"--target-node-id", "4096",
		"--target-user-id", "1025",
		"--sender", "ops",
		"--body", "payload",
	}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Run(send-message): %v", err)
	}
	if !strings.Contains(stdout.String(), `"seq": 11`) {
		t.Fatalf("unexpected send-message output: %s", stdout.String())
	}
}

func TestRunAdminCommandsAndMetricsFormats(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/login", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"token": "admin-token"})
	})
	mux.HandleFunc("/nodes/4096/users/1025/subscriptions", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/users", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"node_id":  4096,
			"user_id":  1025,
			"username": req["username"],
			"role":     req["role"],
		})
	})
	mux.HandleFunc("/ws/client", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Fatalf("accept websocket: %v", err)
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		_ = mustReadClientEnvelope(t, conn)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: 4096, UserId: 1, Username: "root", Role: "admin"},
					ProtocolVersion: "client-v1alpha1",
				},
			},
		})
		req := mustReadClientEnvelope(t, conn)
		switch body := req.Body.(type) {
		case *pb.ClientEnvelope_ListMessages:
			writeServerEnvelope(t, conn, &pb.ServerEnvelope{
				Body: &pb.ServerEnvelope_ListMessagesResponse{
					ListMessagesResponse: &pb.ListMessagesResponse{
						RequestId: body.ListMessages.RequestId,
						Items: []*pb.Message{{
							UserNodeId:   4096,
							UserId:       1025,
							NodeId:       4096,
							Seq:          3,
							Sender:       "ops",
							Body:         []byte("hello"),
							CreatedAtHlc: "hlc1",
						}},
						Count: 1,
					},
				},
			})
		case *pb.ClientEnvelope_Metrics:
			writeServerEnvelope(t, conn, &pb.ServerEnvelope{
				Body: &pb.ServerEnvelope_MetricsResponse{
					MetricsResponse: &pb.MetricsResponse{
						RequestId: body.Metrics.RequestId,
						Text:      "metric_total 1\n",
					},
				},
			})
		default:
			t.Fatalf("unexpected request body: %T", req.Body)
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	for _, args := range [][]string{
		{
			"--base-url", server.URL,
			"--admin-node-id", "4096",
			"--admin-user-id", "1",
			"--admin-password", "root",
			"create-user",
			"--username", "alice",
			"--new-password", "alice-password",
		},
		{
			"--base-url", server.URL,
			"--admin-node-id", "4096",
			"--admin-user-id", "1",
			"--admin-password", "root",
			"subscribe",
			"--subscriber-node-id", "4096",
			"--subscriber-user-id", "1025",
			"--channel-node-id", "4096",
			"--channel-user-id", "2026",
		},
		{
			"--base-url", server.URL,
			"--admin-node-id", "4096",
			"--admin-user-id", "1",
			"--admin-password", "root",
			"list-messages",
			"--target-node-id", "4096",
			"--target-user-id", "1025",
		},
	} {
		var stdout bytes.Buffer
		if err := Run(context.Background(), args, &stdout, &bytes.Buffer{}); err != nil {
			t.Fatalf("Run(%v): %v", args, err)
		}
		if stdout.Len() == 0 {
			t.Fatalf("expected output for args=%v", args)
		}
	}

	var human bytes.Buffer
	if err := Run(context.Background(), []string{
		"--base-url", server.URL,
		"--admin-node-id", "4096",
		"--admin-user-id", "1",
		"--admin-password", "root",
		"metrics",
	}, &human, &bytes.Buffer{}); err != nil {
		t.Fatalf("Run(metrics human): %v", err)
	}
	if strings.TrimSpace(human.String()) != "metric_total 1" {
		t.Fatalf("unexpected human metrics output: %q", human.String())
	}

	var structured bytes.Buffer
	if err := Run(context.Background(), []string{
		"--base-url", server.URL,
		"--admin-node-id", "4096",
		"--admin-user-id", "1",
		"--admin-password", "root",
		"--json",
		"metrics",
	}, &structured, &bytes.Buffer{}); err != nil {
		t.Fatalf("Run(metrics json): %v", err)
	}
	if !strings.Contains(structured.String(), `"text": "metric_total 1\n"`) {
		t.Fatalf("unexpected json metrics output: %q", structured.String())
	}
}

func TestRunDemoChainsFlow(t *testing.T) {
	mux := http.NewServeMux()
	var createCount int
	mux.HandleFunc("/auth/login", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"token": "admin-token"})
	})
	mux.HandleFunc("/users", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		createCount++
		role := req["role"].(string)
		userID := 2000 + createCount
		_ = json.NewEncoder(w).Encode(map[string]any{
			"node_id":  4096,
			"user_id":  userID,
			"username": req["username"],
			"role":     role,
		})
	})
	mux.HandleFunc("/nodes/4096/users/2001/subscriptions", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/ws/client", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Fatalf("accept websocket: %v", err)
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		login := mustReadClientEnvelope(t, conn)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: login.GetLogin().NodeId, UserId: login.GetLogin().UserId, Username: "demo", Role: "user"},
					ProtocolVersion: "client-v1alpha1",
				},
			},
		})

		if login.GetLogin().UserId == 1 {
			req := mustReadClientEnvelope(t, conn)
			writeServerEnvelope(t, conn, &pb.ServerEnvelope{
				Body: &pb.ServerEnvelope_SendMessageResponse{
					SendMessageResponse: &pb.SendMessageResponse{
						RequestId: req.GetSendMessage().RequestId,
						Body: &pb.SendMessageResponse_Message{
							Message: &pb.Message{
								UserNodeId:   4096,
								UserId:       2001,
								NodeId:       4096,
								Seq:          30,
								Sender:       req.GetSendMessage().Sender,
								Body:         req.GetSendMessage().Body,
								CreatedAtHlc: "hlc3",
							},
						},
					},
				},
			})
			return
		}

		time.Sleep(20 * time.Millisecond)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_MessagePushed{
				MessagePushed: &pb.MessagePushed{
					Message: &pb.Message{
						UserNodeId:   4096,
						UserId:       2001,
						NodeId:       4096,
						Seq:          30,
						Sender:       "demo-cli",
						Body:         []byte("hello from turntf-client demo"),
						CreatedAtHlc: "hlc3",
					},
				},
			},
		})
		time.Sleep(200 * time.Millisecond)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var stdout bytes.Buffer
	err := Run(ctx, []string{
		"--base-url", server.URL,
		"--admin-node-id", "4096",
		"--admin-user-id", "1",
		"--admin-password", "root",
		"demo",
	}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Run(demo): %v", err)
	}
	if !strings.Contains(stdout.String(), "demo-user node=4096 user=2001") {
		t.Fatalf("missing demo user output: %s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "demo-send-message cursor=4096:30") {
		t.Fatalf("missing demo send output: %s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "message target=4096:2001") {
		t.Fatalf("missing demo listener message output: %s", stdout.String())
	}
}

func TestRunMissingCredentialsIsReadable(t *testing.T) {
	err := Run(context.Background(), []string{
		"--base-url", "http://127.0.0.1:8080",
		"listen",
	}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "client credentials are required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func mustReadClientEnvelope(t *testing.T, conn *websocket.Conn) *pb.ClientEnvelope {
	t.Helper()
	_, data, err := conn.Read(context.Background())
	if err != nil {
		t.Fatalf("read client envelope: %v", err)
	}
	var env pb.ClientEnvelope
	if err := proto.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal client envelope: %v", err)
	}
	return &env
}

func writeServerEnvelope(t *testing.T, conn *websocket.Conn, env *pb.ServerEnvelope) {
	t.Helper()
	data, err := proto.Marshal(env)
	if err != nil {
		t.Fatalf("marshal server envelope: %v", err)
	}
	if err := conn.Write(context.Background(), websocket.MessageBinary, data); err != nil {
		t.Fatalf("write server envelope: %v", err)
	}
}
