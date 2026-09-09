package turntf

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	pb "github.com/tursom/turntf-go/internal/proto"
	"google.golang.org/protobuf/proto"
)

// Real WebSocket with independently scheduled replies: delay never blocks ingress.
// This models propagation/acceptance delay, not packet loss or a complete core.
func perfPeer(t *testing.T, onData func(*pb.SendMessageRequest, *RelayEnvelope, func(*pb.ServerEnvelope))) (*Client, *RelayConnection) {
	t.Helper()
	var workers sync.WaitGroup
	var readers sync.WaitGroup
	var readerMu sync.Mutex
	stopping := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		readerMu.Lock()
		if stopping {
			readerMu.Unlock()
			return
		}
		readers.Add(1)
		readerMu.Unlock()
		defer readers.Done()
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		ws.SetReadLimit(1 << 20)
		send := func(env *pb.ServerEnvelope) {
			b, _ := proto.Marshal(env)
			_ = ws.Write(context.Background(), websocket.MessageBinary, b)
		}
		for {
			_, b, err := ws.Read(context.Background())
			if err != nil {
				return
			}
			var env pb.ClientEnvelope
			if proto.Unmarshal(b, &env) != nil {
				return
			}
			switch {
			case env.GetLogin() != nil:
				send(&pb.ServerEnvelope{Body: &pb.ServerEnvelope_LoginResponse{LoginResponse: &pb.LoginResponse{User: &pb.User{NodeId: 1, UserId: 1}, ProtocolVersion: "client-v1alpha5", SessionRef: &pb.SessionRef{ServingNodeId: 1, SessionId: "local"}}}})
			case env.GetPing() != nil:
				send(&pb.ServerEnvelope{Body: &pb.ServerEnvelope_Pong{Pong: &pb.Pong{RequestId: env.GetPing().RequestId}}})
			case env.GetSendMessage() != nil:
				req := env.GetSendMessage()
				relay, _ := decodeRelayEnvelope(req.Body)
				workers.Add(1)
				go func() { defer workers.Done(); onData(req, relay, send) }()
			}
		}
	}))
	client, err := NewClient(Config{BaseURL: server.URL, Credentials: Credentials{NodeID: 1, UserID: 1, Password: MustPlainPassword("test")}, PingInterval: time.Hour, RequestTimeout: 600 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	conn := newTestRelayConnection()
	conn.relay = client.Relay()
	conn.relay.conns[conn.relayID] = conn
	conn.sendBase = 1
	conn.nextSeq = 1
	conn.sendCh = make(chan relaySendItem, 128)
	conn.config.AckTimeoutMs = 500
	conn.remotePeer = UserRef{NodeID: 2, UserID: 2}
	conn.remoteSession = SessionRef{ServingNodeID: 2, SessionID: "remote"}
	conn.mySession = SessionRef{ServingNodeID: 1, SessionID: "local"}
	t.Cleanup(func() {
		conn.Abort(nil)
		client.Close()
		readerMu.Lock()
		stopping = true
		readerMu.Unlock()
		server.Close()
		// Hijacked WS handlers may outlive Server.Close. Stop all producers
		// before waiting on the independently scheduled acceptance callbacks.
		readers.Wait()
		workers.Wait()
		conn.wg.Wait()
	})
	return client, conn
}
func perfAccepted(req *pb.SendMessageRequest) *pb.ServerEnvelope {
	return &pb.ServerEnvelope{Body: &pb.ServerEnvelope_SendMessageResponse{SendMessageResponse: &pb.SendMessageResponse{RequestId: req.RequestId, Body: &pb.SendMessageResponse_TransientAccepted{TransientAccepted: &pb.TransientAccepted{}}}}}
}
func perfAck(env *RelayEnvelope) *pb.ServerEnvelope {
	body, _ := encodeRelayEnvelope(&RelayEnvelope{RelayID: env.RelayID, Kind: RelayKindAck, AckSeq: env.Seq})
	return &pb.ServerEnvelope{Body: &pb.ServerEnvelope_PacketPushed{PacketPushed: &pb.PacketPushed{Packet: &pb.Packet{Body: body}}}}
}

func TestPerfRetransmitDoesNotBlockSharedRPC(t *testing.T) {
	retransmitted := make(chan struct{})
	var mu sync.Mutex
	count := 0
	client, conn := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
		mu.Lock()
		count++
		n := count
		mu.Unlock()
		if n == 1 {
			send(perfAccepted(req))
			return
		}
		send(perfAck(env))
		send(perfAccepted(req))
		if n == 2 {
			close(retransmitted)
		}
	})
	conn.config.AckTimeoutMs = 25
	conn.wg.Add(1)
	go conn.sendLoop()
	_ = conn.Send(make([]byte, 32768))
	select {
	case <-retransmitted:
	case <-time.After(time.Second):
		t.Fatal("no retransmission")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := client.Ping(ctx)
	t.Logf("ping during ACK-before-accept retransmit: %v, elapsed=%s", err, time.Since(start))
	if err != nil {
		t.Errorf("shared readLoop blocked by relay retransmit: %v", err)
	}
}

func TestPerfAcceptanceRTT(t *testing.T) {
	for _, delay := range []time.Duration{0, 20 * time.Millisecond, 80 * time.Millisecond} {
		t.Run(fmt.Sprint(delay), func(t *testing.T) {
			var mu sync.Mutex
			bytes := 0
			done := make(chan struct{})
			_, conn := perfPeer(t, func(req *pb.SendMessageRequest, env *RelayEnvelope, send func(*pb.ServerEnvelope)) {
				time.Sleep(delay)
				send(perfAccepted(req))
				send(perfAck(env))
				mu.Lock()
				bytes += len(env.Payload)
				if bytes == 32*32768 {
					close(done)
				}
				mu.Unlock()
			})
			conn.wg.Add(1)
			go conn.sendLoop()
			start := time.Now()
			for i := 0; i < 32; i++ {
				if err := conn.Send(make([]byte, 32768)); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("1MiB stalled")
			}
			elapsed := time.Since(start)
			t.Logf("accept RTT=%s 1MiB=%s Mbps=%.3f", delay, elapsed, 8.388608/elapsed.Seconds())
		})
	}
}
