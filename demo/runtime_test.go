package demo

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/proto"

	pb "github.com/tursom/turntf-go/internal/proto"
)

func TestParseRejectsUnknownNode(t *testing.T) {
	_, err := Parse([]byte(`
version: v1alpha1
name: invalid
nodes:
  node_a:
    base_url: http://127.0.0.1:8080
sessions:
  alice:
    node: missing
    user:
      node_id: 1
      user_id: 2
      password:
        source: plain
        value: secret
script:
  - step: connect
    session: alice
    expect:
      login:
        user:
          node_id: 1
          user_id: 2
`))
	if err == nil || !strings.Contains(err.Error(), `unknown node "missing"`) {
		t.Fatalf("expected unknown node validation error, got %v", err)
	}
}

func TestParseRejectsInvalidPasswordSpec(t *testing.T) {
	_, err := Parse([]byte(`
version: v1alpha1
name: invalid-password
nodes:
  node_a:
    base_url: http://127.0.0.1:8080
sessions:
  alice:
    node: node_a
    user:
      node_id: 1
      user_id: 2
      password:
        value: secret
script:
  - step: connect
    session: alice
    expect:
      login:
        user:
          node_id: 1
          user_id: 2
`))
	if err == nil || !strings.Contains(err.Error(), "password.source must be plain or hashed") {
		t.Fatalf("expected password validation error, got %v", err)
	}
}

func TestRunScenarioParallelCrossNodeMessage(t *testing.T) {
	var bobMu sync.Mutex
	var bobConn *websocket.Conn
	bobReady := make(chan struct{})
	deliver := make(chan struct{}, 1)

	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		login := mustReadClientEnvelope(t, conn)
		if got := login.GetLogin().GetUser(); got.GetNodeId() != 8192 || got.GetUserId() != 2025 {
			t.Fatalf("unexpected bob login: %+v", got)
		}
		if login.GetLogin().GetPassword() == "bob-password" {
			t.Fatal("expected bob password to be hashed")
		}
		if err := bcrypt.CompareHashAndPassword([]byte(login.GetLogin().GetPassword()), []byte("bob-password")); err != nil {
			t.Fatalf("expected bcrypt password, got %v", err)
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: 8192, UserId: 2025, Username: "bob", Role: "user"},
					ProtocolVersion: Version,
				},
			},
		})

		bobMu.Lock()
		bobConn = conn
		bobMu.Unlock()
		close(bobReady)

		<-deliver
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_MessagePushed{
				MessagePushed: &pb.MessagePushed{
					Message: &pb.Message{
						Recipient:    &pb.UserRef{NodeId: 8192, UserId: 2025},
						NodeId:       4096,
						Seq:          41,
						Sender:       &pb.UserRef{NodeId: 4096, UserId: 1025},
						Body:         []byte("hello from yaml"),
						CreatedAtHlc: "hlc-41",
					},
				},
			},
		})

		ack := mustReadClientEnvelope(t, conn)
		if ack.GetAckMessage().GetCursor().GetSeq() != 41 {
			t.Fatalf("unexpected ack cursor: %+v", ack.GetAckMessage().GetCursor())
		}
	}))
	defer serverB.Close()

	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		login := mustReadClientEnvelope(t, conn)
		if got := login.GetLogin().GetUser(); got.GetNodeId() != 4096 || got.GetUserId() != 1025 {
			t.Fatalf("unexpected alice login: %+v", got)
		}
		if login.GetLogin().GetPassword() == "alice-password" {
			t.Fatal("expected alice password to be hashed")
		}
		if err := bcrypt.CompareHashAndPassword([]byte(login.GetLogin().GetPassword()), []byte("alice-password")); err != nil {
			t.Fatalf("expected bcrypt password, got %v", err)
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: 4096, UserId: 1025, Username: "alice", Role: "user"},
					ProtocolVersion: Version,
				},
			},
		})

		req := mustReadClientEnvelope(t, conn)
		send := req.GetSendMessage()
		if send.GetTarget().GetNodeId() != 8192 || send.GetTarget().GetUserId() != 2025 {
			t.Fatalf("unexpected target: %+v", send.GetTarget())
		}
		if string(send.GetBody()) != "hello from yaml" {
			t.Fatalf("unexpected body: %q", string(send.GetBody()))
		}

		<-bobReady
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_SendMessageResponse{
				SendMessageResponse: &pb.SendMessageResponse{
					RequestId: send.GetRequestId(),
					Body: &pb.SendMessageResponse_Message{
						Message: &pb.Message{
							Recipient:    &pb.UserRef{NodeId: 8192, UserId: 2025},
							NodeId:       4096,
							Seq:          41,
							Sender:       &pb.UserRef{NodeId: 4096, UserId: 1025},
							Body:         send.GetBody(),
							CreatedAtHlc: "hlc-41",
						},
					},
				},
			},
		})

		deliver <- struct{}{}
	}))
	defer serverA.Close()

	scenario, err := Parse([]byte(`
version: v1alpha1
name: cross-node-send-receive
defaults:
  timeout: 2s
  auto_ack_messages: true
vars:
  message_text: hello from yaml
nodes:
  node_a:
    base_url: ` + serverA.URL + `
  node_b:
    base_url: ` + serverB.URL + `
sessions:
  alice:
    node: node_a
    user:
      node_id: 4096
      user_id: 1025
      password:
        source: plain
        value: alice-password
  bob:
    node: node_b
    user:
      node_id: 8192
      user_id: 2025
      password:
        source: plain
        value: bob-password
script:
  - step: connect
    session: alice
    expect:
      login:
        user:
          node_id: 4096
          user_id: 1025
        protocol_version: v1alpha1
  - step: connect
    session: bob
    expect:
      login:
        user:
          node_id: 8192
          user_id: 2025
        protocol_version: v1alpha1
  - step: parallel
    branches:
      - name: sender
        script:
          - step: barrier
            name: ready
          - step: request
            session: alice
            action: send_message
            request:
              target:
                node_id: 8192
                user_id: 2025
              body: ${message_text}
            expect:
              ok:
                message:
                  recipient:
                    node_id: 8192
                    user_id: 2025
                  body: ${message_text}
      - name: receiver
        script:
          - step: barrier
            name: ready
          - step: expect_event
            session: bob
            event: message_pushed
            timeout: 2s
            match:
              message:
                recipient:
                  node_id: 8192
                  user_id: 2025
                body: ${message_text}
  - step: close
    session: alice
  - step: close
    session: bob
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var out strings.Builder
	if err := RunScenario(ctx, scenario, &out); err != nil {
		t.Fatalf("RunScenario: %v\noutput:\n%s", err, out.String())
	}

	bobMu.Lock()
	defer bobMu.Unlock()
	if bobConn == nil {
		t.Fatal("expected bob connection to be established")
	}
	if !strings.Contains(out.String(), "request action=send_message ok") {
		t.Fatalf("expected request success log, got:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "event=message_pushed matched") {
		t.Fatalf("expected event match log, got:\n%s", out.String())
	}
}

func TestRunScenarioBlacklistAndDiscoveryFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		login := mustReadClientEnvelope(t, conn)
		if got := login.GetLogin().GetUser(); got.GetNodeId() != 4096 || got.GetUserId() != 1 {
			t.Fatalf("unexpected admin login: %+v", got)
		}
		if login.GetLogin().GetPassword() == "root" {
			t.Fatal("expected admin password to be hashed")
		}
		if err := bcrypt.CompareHashAndPassword([]byte(login.GetLogin().GetPassword()), []byte("root")); err != nil {
			t.Fatalf("expected bcrypt password, got %v", err)
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_LoginResponse{
				LoginResponse: &pb.LoginResponse{
					User:            &pb.User{NodeId: 4096, UserId: 1, Username: "root", Role: "admin"},
					ProtocolVersion: "client-v1alpha2",
				},
			},
		})

		blockReq := mustReadClientEnvelope(t, conn)
		if blockReq.GetUpsertUserAttachment().GetOwner().GetUserId() != 1025 || blockReq.GetUpsertUserAttachment().GetSubject().GetUserId() != 2025 {
			t.Fatalf("unexpected block request: %+v", blockReq.GetUpsertUserAttachment())
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_UpsertUserAttachmentResponse{
				UpsertUserAttachmentResponse: &pb.UpsertUserAttachmentResponse{
					RequestId: blockReq.GetUpsertUserAttachment().GetRequestId(),
					Attachment: &pb.Attachment{
						Owner:          &pb.UserRef{NodeId: 4096, UserId: 1025},
						Subject:        &pb.UserRef{NodeId: 8192, UserId: 2025},
						AttachmentType: pb.AttachmentType_ATTACHMENT_TYPE_USER_BLACKLIST,
						AttachedAt:     "hlc-blocked",
						OriginNodeId:   4096,
					},
				},
			},
		})

		listReq := mustReadClientEnvelope(t, conn)
		if listReq.GetListUserAttachments().GetOwner().GetUserId() != 1025 {
			t.Fatalf("unexpected list blocked request: %+v", listReq.GetListUserAttachments())
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_ListUserAttachmentsResponse{
				ListUserAttachmentsResponse: &pb.ListUserAttachmentsResponse{
					RequestId: listReq.GetListUserAttachments().GetRequestId(),
					Items: []*pb.Attachment{{
						Owner:          &pb.UserRef{NodeId: 4096, UserId: 1025},
						Subject:        &pb.UserRef{NodeId: 8192, UserId: 2025},
						AttachmentType: pb.AttachmentType_ATTACHMENT_TYPE_USER_BLACKLIST,
						AttachedAt:     "hlc-blocked",
						OriginNodeId:   4096,
					}},
					Count: 1,
				},
			},
		})

		unblockReq := mustReadClientEnvelope(t, conn)
		if unblockReq.GetDeleteUserAttachment().GetSubject().GetUserId() != 2025 {
			t.Fatalf("unexpected unblock request: %+v", unblockReq.GetDeleteUserAttachment())
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_DeleteUserAttachmentResponse{
				DeleteUserAttachmentResponse: &pb.DeleteUserAttachmentResponse{
					RequestId: unblockReq.GetDeleteUserAttachment().GetRequestId(),
					Attachment: &pb.Attachment{
						Owner:          &pb.UserRef{NodeId: 4096, UserId: 1025},
						Subject:        &pb.UserRef{NodeId: 8192, UserId: 2025},
						AttachmentType: pb.AttachmentType_ATTACHMENT_TYPE_USER_BLACKLIST,
						AttachedAt:     "hlc-blocked",
						DeletedAt:      "hlc-unblocked",
						OriginNodeId:   4096,
					},
				},
			},
		})

		nodesReq := mustReadClientEnvelope(t, conn)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{
			Body: &pb.ServerEnvelope_ListClusterNodesResponse{
				ListClusterNodesResponse: &pb.ListClusterNodesResponse{
					RequestId: nodesReq.GetListClusterNodes().GetRequestId(),
					Items: []*pb.ClusterNode{
						{NodeId: 4096, IsLocal: true},
						{NodeId: 8192, IsLocal: false, ConfiguredUrl: "ws://127.0.0.1:8081/internal/cluster/ws", Source: "discovered"},
					},
					Count: 2,
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
							Connected:          true,
							Source:             "discovered",
							DiscoveredUrl:      "ws://127.0.0.1:8081/internal/cluster/ws",
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

	scenario, err := Parse([]byte(`
version: v1alpha1
name: blacklist-and-discovery
defaults:
  timeout: 2s
  auto_ack_messages: true
vars:
  owner_node: 4096
  owner_user: 1025
  blocked_node: 8192
  blocked_user: 2025
nodes:
  node_a:
    base_url: ` + server.URL + `
sessions:
  admin:
    node: node_a
    user:
      node_id: 4096
      user_id: 1
      password:
        source: plain
        value: root
script:
  - step: connect
    session: admin
    expect:
      login:
        user:
          node_id: 4096
          user_id: 1
        protocol_version: client-v1alpha2

  - step: request
    session: admin
    action: block_user
    request:
      owner:
        node_id: ${owner_node}
        user_id: ${owner_user}
      blocked:
        node_id: ${blocked_node}
        user_id: ${blocked_user}
    expect:
      ok:
        entry:
          owner:
            node_id: ${owner_node}
            user_id: ${owner_user}
          blocked:
            node_id: ${blocked_node}
            user_id: ${blocked_user}
          blocked_at: hlc-blocked

  - step: request
    session: admin
    action: list_blocked_users
    request:
      owner:
        node_id: ${owner_node}
        user_id: ${owner_user}
    expect:
      ok:
        count: 1
        items:
          - blocked:
              node_id: ${blocked_node}
              user_id: ${blocked_user}

  - step: request
    session: admin
    action: unblock_user
    request:
      owner:
        node_id: ${owner_node}
        user_id: ${owner_user}
      blocked:
        node_id: ${blocked_node}
        user_id: ${blocked_user}
    expect:
      ok:
        entry:
          deleted_at: hlc-unblocked

  - step: request
    session: admin
    action: list_cluster_nodes
    request: {}
    expect:
      ok:
        count: 2
        items:
          - node_id: 4096
            is_local: true
          - node_id: 8192
            source: discovered

  - step: request
    session: admin
    action: operations_status
    request: {}
    expect:
      ok:
        status:
          node_id: 4096
          peers:
            - node_id: 8192
              source: discovered
              discovery_state: connected
              last_connected_at: hlc-connected

  - step: close
    session: admin
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var out strings.Builder
	if err := RunScenario(ctx, scenario, &out); err != nil {
		t.Fatalf("RunScenario: %v\noutput:\n%s", err, out.String())
	}
	for _, want := range []string{
		"request action=block_user ok",
		"request action=list_blocked_users ok",
		"request action=unblock_user ok",
		"request action=list_cluster_nodes ok",
		"request action=operations_status ok",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("expected log %q, got:\n%s", want, out.String())
		}
	}
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
