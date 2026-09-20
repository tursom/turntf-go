package turntf

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	pb "github.com/tursom/turntf-go/internal/proto"
	"google.golang.org/protobuf/proto"
)

type relayTestPeer struct {
	user    UserRef
	session SessionRef
	ws      *websocket.Conn
	writeMu sync.Mutex
}

func (p *relayTestPeer) send(env *pb.ServerEnvelope) error {
	body, err := proto.Marshal(env)
	if err != nil {
		return err
	}
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return p.ws.Write(ctx, websocket.MessageBinary, body)
}

type relayTestRouter struct {
	t        *testing.T
	server   *httptest.Server
	mu       sync.Mutex
	peers    map[string]*relayTestPeer
	nextID   atomic.Uint64
	packetID atomic.Uint64
}

func newRelayTestRouter(t *testing.T) *relayTestRouter {
	t.Helper()
	r := &relayTestRouter{t: t, peers: make(map[string]*relayTestPeer)}
	r.server = httptest.NewServer(http.HandlerFunc(r.serveWS))
	t.Cleanup(r.server.Close)
	return r
}

func (r *relayTestRouter) serveWS(w http.ResponseWriter, req *http.Request) {
	ws, err := websocket.Accept(w, req, nil)
	if err != nil {
		return
	}
	defer ws.CloseNow()

	loginEnv, err := readRelayTestClientEnvelope(ws)
	if err != nil || loginEnv.GetLogin() == nil {
		return
	}
	login := loginEnv.GetLogin()
	user := userRefFromProto(login.GetUser())
	id := r.nextID.Add(1)
	session := SessionRef{ServingNodeID: user.NodeID, SessionID: fmt.Sprintf("session-%d", id)}
	peer := &relayTestPeer{user: user, session: session, ws: ws}
	r.mu.Lock()
	r.peers[session.SessionID] = peer
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.peers, session.SessionID)
		r.mu.Unlock()
	}()

	if err := peer.send(&pb.ServerEnvelope{Body: &pb.ServerEnvelope_LoginResponse{LoginResponse: &pb.LoginResponse{
		User:            &pb.User{NodeId: user.NodeID, UserId: user.UserID, Username: session.SessionID, Role: "user"},
		ProtocolVersion: clientProtocolVersion,
		SessionRef:      sessionRefToProto(session),
	}}}); err != nil {
		return
	}

	for {
		env, readErr := readRelayTestClientEnvelope(ws)
		if readErr != nil {
			return
		}
		switch {
		case env.GetResolveUserSessions() != nil:
			r.handleResolve(peer, env.GetResolveUserSessions())
		case env.GetSendMessage() != nil:
			r.handleSend(peer, env.GetSendMessage())
		case env.GetPing() != nil:
			_ = peer.send(&pb.ServerEnvelope{Body: &pb.ServerEnvelope_Pong{Pong: &pb.Pong{RequestId: env.GetPing().GetRequestId()}}})
		}
	}
}

func readRelayTestClientEnvelope(ws *websocket.Conn) (*pb.ClientEnvelope, error) {
	_, body, err := ws.Read(context.Background())
	if err != nil {
		return nil, err
	}
	var env pb.ClientEnvelope
	if err := proto.Unmarshal(body, &env); err != nil {
		return nil, err
	}
	return &env, nil
}

func (r *relayTestRouter) handleResolve(source *relayTestPeer, req *pb.ResolveUserSessionsRequest) {
	target := userRefFromProto(req.GetUser())
	r.mu.Lock()
	items := make([]*pb.ResolvedSession, 0, len(r.peers))
	for _, peer := range r.peers {
		if peer.user == target {
			items = append(items, &pb.ResolvedSession{Session: sessionRefToProto(peer.session), TransientCapable: true})
		}
	}
	r.mu.Unlock()
	_ = source.send(&pb.ServerEnvelope{Body: &pb.ServerEnvelope_ResolveUserSessionsResponse{ResolveUserSessionsResponse: &pb.ResolveUserSessionsResponse{
		RequestId: req.GetRequestId(), User: req.GetUser(), Items: items,
	}}})
}

func (r *relayTestRouter) handleSend(source *relayTestPeer, req *pb.SendMessageRequest) {
	targetSession := sessionRefFromProto(req.GetTargetSession())
	r.mu.Lock()
	target := r.peers[targetSession.SessionID]
	r.mu.Unlock()
	if target == nil || target.session != targetSession {
		_ = source.send(&pb.ServerEnvelope{Body: &pb.ServerEnvelope_Error{Error: &pb.Error{
			RequestId: req.GetRequestId(), Code: "session_not_found", Message: "target session is offline",
		}}})
		return
	}
	packetID := r.packetID.Add(1)
	_ = target.send(&pb.ServerEnvelope{Body: &pb.ServerEnvelope_PacketPushed{PacketPushed: &pb.PacketPushed{Packet: &pb.Packet{
		PacketId: packetID, SourceNodeId: source.user.NodeID, TargetNodeId: target.user.NodeID,
		Recipient: userRefToProto(target.user), Sender: userRefToProto(source.user), Body: append([]byte(nil), req.GetBody()...),
		DeliveryMode: req.GetDeliveryMode(), TargetSession: sessionRefToProto(target.session),
	}}}})
	_ = source.send(&pb.ServerEnvelope{Body: &pb.ServerEnvelope_SendMessageResponse{SendMessageResponse: &pb.SendMessageResponse{
		RequestId: req.GetRequestId(), Body: &pb.SendMessageResponse_TransientAccepted{TransientAccepted: &pb.TransientAccepted{
			PacketId: packetID, SourceNodeId: source.user.NodeID, TargetNodeId: target.user.NodeID,
			Recipient: userRefToProto(target.user), DeliveryMode: req.GetDeliveryMode(), TargetSession: sessionRefToProto(target.session),
		}},
	}}})
}

func newRelayTestClient(t *testing.T, baseURL string, user UserRef) *Client {
	t.Helper()
	client, err := NewClient(Config{
		BaseURL: baseURL,
		Credentials: Credentials{
			NodeID: user.NodeID, UserID: user.UserID, Password: MustPlainPassword("relay-test-password"),
		},
		PingInterval:   time.Hour,
		RequestTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	return client
}

func TestRelayRealWebSocketFiveClientsMultiSessionReconnect(t *testing.T) {
	router := newRelayTestRouter(t)
	hubUser := UserRef{NodeID: 1, UserID: 1}
	multiSessionUser := UserRef{NodeID: 2, UserID: 2}
	hub := newRelayTestClient(t, router.server.URL, hubUser)
	defer hub.Close()
	cfg := DefaultRelayConfig()
	cfg.Reliability = ReliabilityAtLeastOnce
	cfg.WindowSize = 8
	cfg.AckTimeoutMs = 100
	cfg.MaxRetransmits = 10
	hub.Relay().SetIncomingConfig(cfg)
	accepted := make(chan *RelayConnection, 16)
	hub.Relay().OnConnection(func(conn *RelayConnection) { accepted <- conn })
	var orphans atomic.Int32
	hub.Relay().OnOrphan(func(RelayOrphan) { orphans.Add(1) })

	clients := make([]*Client, 5)
	for i := range clients {
		clients[i] = newRelayTestClient(t, router.server.URL, multiSessionUser)
		defer clients[i].Close()
		clients[i].Relay().OnOrphan(func(RelayOrphan) { orphans.Add(1) })
	}
	outgoing := make([]*RelayConnection, len(clients))
	var connects sync.WaitGroup
	for i := range clients {
		connects.Add(1)
		go func(i int) {
			defer connects.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			conn, err := clients[i].Relay().Connect(ctx, hubUser, &cfg)
			if err != nil {
				t.Errorf("client %d Connect: %v", i, err)
				return
			}
			outgoing[i] = conn
		}(i)
	}
	connects.Wait()
	incoming := make(map[string]*RelayConnection, len(clients))
	for range clients {
		select {
		case conn := <-accepted:
			incoming[conn.RemoteSession().SessionID] = conn
		case <-time.After(4 * time.Second):
			t.Fatal("hub did not accept all five sessions")
		}
	}
	if len(incoming) != 5 {
		t.Fatalf("hub incoming sessions=%d, want 5", len(incoming))
	}

	for i, conn := range outgoing {
		if conn == nil {
			t.Fatalf("client %d has no outgoing Relay", i)
		}
		payload := []byte(fmt.Sprintf("request-%d", i))
		if err := conn.Send(payload); err != nil {
			t.Fatalf("client %d Send: %v", i, err)
		}
		hubConn := incoming[conn.mySession.SessionID]
		got, err := hubConn.ReceiveTimeout(2 * time.Second)
		if err != nil || string(got) != string(payload) {
			t.Fatalf("hub Receive %d = %q, %v", i, got, err)
		}
		response := []byte(fmt.Sprintf("response-%d", i))
		if err := hubConn.Send(response); err != nil {
			t.Fatalf("hub Send %d: %v", i, err)
		}
		got, err = conn.ReceiveTimeout(2 * time.Second)
		if err != nil || string(got) != string(response) {
			t.Fatalf("client Receive %d = %q, %v", i, got, err)
		}
	}

	reconnected := newRelayTestClient(t, router.server.URL, multiSessionUser)
	defer reconnected.Close()
	reconnected.Relay().OnOrphan(func(RelayOrphan) { orphans.Add(1) })
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	newConn, err := reconnected.Relay().Connect(ctx, hubUser, &cfg)
	cancel()
	if err != nil {
		t.Fatalf("reconnected Relay: %v", err)
	}
	var newIncoming *RelayConnection
	select {
	case newIncoming = <-accepted:
	case <-time.After(4 * time.Second):
		t.Fatal("hub did not accept reconnected session")
	}
	if err := outgoing[0].Close(); err != nil {
		t.Fatalf("old delayed Close: %v", err)
	}
	if err := newConn.Send([]byte("after-reconnect")); err != nil {
		t.Fatalf("reconnected Send: %v", err)
	}
	if got, err := newIncoming.ReceiveTimeout(2 * time.Second); err != nil || string(got) != "after-reconnect" {
		t.Fatalf("reconnected Receive = %q, %v", got, err)
	}
	if got := orphans.Load(); got != 0 {
		t.Fatalf("real multi-session exchange produced %d orphan frames", got)
	}
}
