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

	pb "github.com/tursom/turntf-go/internal/proto"
)

func newTestRelayConnection() *RelayConnection {
	cfg := DefaultRelayConfig()
	ctx, cancel := context.WithCancel(context.Background())
	conn := &RelayConnection{
		relay:       &Relay{conns: make(map[string]*RelayConnection)},
		relayID:     "test-relay",
		state:       RelayStateOpen,
		config:      cfg,
		sendCh:      make(chan []byte, 1),
		recvCh:      make(chan []byte, 64),
		closeCh:     make(chan struct{}),
		openCh:      make(chan struct{}),
		flushCh:     make(chan struct{}),
		unacked:     make(map[uint64]unackedFrame),
		recvBuf:     make(map[uint64][]byte),
		expectedSeq: 1,
		ctx:         ctx,
		cancel:      cancel,
	}
	conn.relay.conns[conn.relayID] = conn
	return conn
}

func TestRelaySendOwnsQueuedData(t *testing.T) {
	conn := newTestRelayConnection()
	defer conn.Abort(errors.New("test complete"))

	payload := []byte("original")
	if err := conn.Send(payload); err != nil {
		t.Fatalf("Send: %v", err)
	}
	copy(payload, "mutated!")

	select {
	case got := <-conn.sendCh:
		if string(got) != "original" {
			t.Fatalf("queued payload = %q, want original", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued payload")
	}
}

func TestRelayOutgoingReliableOrderedStartsAtSequenceOne(t *testing.T) {
	local := UserRef{NodeID: 4096, UserID: 1025}
	remote := UserRef{NodeID: 8192, UserID: 2048}
	localSession := SessionRef{ServingNodeID: 4096, SessionID: "local-session"}
	remoteSession := SessionRef{ServingNodeID: 8192, SessionID: "remote-session"}
	dataSequence := make(chan uint64, 1)
	remoteAckSent := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		_ = mustReadClientEnvelope(t, conn)
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{Body: &pb.ServerEnvelope_LoginResponse{
			LoginResponse: &pb.LoginResponse{
				User:            &pb.User{NodeId: local.NodeID, UserId: local.UserID, Username: "local", Role: "user"},
				ProtocolVersion: "client-v1alpha5",
				SessionRef:      sessionRefToProto(localSession),
			},
		}})

		resolve := mustReadClientEnvelope(t, conn).GetResolveUserSessions()
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{Body: &pb.ServerEnvelope_ResolveUserSessionsResponse{
			ResolveUserSessionsResponse: &pb.ResolveUserSessionsResponse{
				RequestId: resolve.GetRequestId(),
				User:      userRefToProto(remote),
				Items: []*pb.ResolvedSession{{
					Session:          sessionRefToProto(remoteSession),
					TransientCapable: true,
				}},
			},
		}})

		openRequest := mustReadClientEnvelope(t, conn).GetSendMessage()
		openEnvelope, err := decodeRelayEnvelope(openRequest.GetBody())
		if err != nil {
			t.Fatalf("decode relay OPEN: %v", err)
		}
		writeTransientAccepted(t, conn, openRequest, remote, remoteSession)
		openAck, err := encodeRelayEnvelope(&RelayEnvelope{
			RelayID:       openEnvelope.RelayID,
			Kind:          RelayKindOpenAck,
			SenderSession: remoteSession,
			TargetSession: localSession,
		})
		if err != nil {
			t.Fatalf("encode OPEN_ACK: %v", err)
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{Body: &pb.ServerEnvelope_PacketPushed{
			PacketPushed: &pb.PacketPushed{Packet: &pb.Packet{
				Sender:        userRefToProto(remote),
				Recipient:     userRefToProto(local),
				TargetSession: sessionRefToProto(localSession),
				Body:          openAck,
			}},
		}})

		dataRequest := mustReadClientEnvelope(t, conn).GetSendMessage()
		dataEnvelope, err := decodeRelayEnvelope(dataRequest.GetBody())
		if err != nil {
			t.Fatalf("decode relay DATA: %v", err)
		}
		dataSequence <- dataEnvelope.Seq
		writeTransientAccepted(t, conn, dataRequest, remote, remoteSession)
		go func() {
			time.Sleep(150 * time.Millisecond)
			ack, encodeErr := encodeRelayEnvelope(&RelayEnvelope{
				RelayID:       dataEnvelope.RelayID,
				Kind:          RelayKindAck,
				SenderSession: remoteSession,
				TargetSession: localSession,
				AckSeq:        dataEnvelope.Seq,
			})
			if encodeErr != nil {
				t.Errorf("encode ACK: %v", encodeErr)
				return
			}
			writeServerEnvelope(t, conn, &pb.ServerEnvelope{Body: &pb.ServerEnvelope_PacketPushed{
				PacketPushed: &pb.PacketPushed{Packet: &pb.Packet{
					Sender:        userRefToProto(remote),
					Recipient:     userRefToProto(local),
					TargetSession: sessionRefToProto(localSession),
					Body:          ack,
				}},
			}})
			close(remoteAckSent)
		}()

		closeRequest := mustReadClientEnvelope(t, conn).GetSendMessage()
		closeEnvelope, err := decodeRelayEnvelope(closeRequest.GetBody())
		if err != nil {
			t.Fatalf("decode relay CLOSE: %v", err)
		}
		if closeEnvelope.Kind != RelayKindClose {
			t.Fatalf("frame after DATA = %v, want CLOSE", closeEnvelope.Kind)
		}
		writeTransientAccepted(t, conn, closeRequest, remote, remoteSession)
		<-remoteAckSent
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:        server.URL,
		Credentials:    Credentials{NodeID: local.NodeID, UserID: local.UserID, Password: MustPlainPassword("password")},
		CursorStore:    NewMemoryCursorStore(),
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
	relay, err := client.Relay().Connect(ctx, remote, nil)
	if err != nil {
		t.Fatalf("Relay.Connect: %v", err)
	}
	if err := relay.Send([]byte("first")); err != nil {
		t.Fatalf("Relay.Send: %v", err)
	}
	select {
	case seq := <-dataSequence:
		if seq != 1 {
			t.Fatalf("first DATA sequence = %d, want 1", seq)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for first DATA frame")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- relay.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Relay.Close returned before DATA acknowledgement: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Relay.Close: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for Relay.Close")
	}
}

func writeTransientAccepted(t *testing.T, conn *websocket.Conn, request *pb.SendMessageRequest, target UserRef, session SessionRef) {
	t.Helper()
	writeServerEnvelope(t, conn, &pb.ServerEnvelope{Body: &pb.ServerEnvelope_SendMessageResponse{
		SendMessageResponse: &pb.SendMessageResponse{
			RequestId: request.GetRequestId(),
			Body: &pb.SendMessageResponse_TransientAccepted{TransientAccepted: &pb.TransientAccepted{
				Recipient:     userRefToProto(target),
				TargetSession: sessionRefToProto(session),
			}},
		},
	}})
}

func TestRelayReliableOrderedReceiveDoesNotDropUnderBackpressure(t *testing.T) {
	conn := newTestRelayConnection()
	defer conn.Abort(errors.New("test complete"))

	const frameCount = 96
	delivered := make(chan struct{})
	go func() {
		defer close(delivered)
		for i := 0; i < frameCount; i++ {
			conn.deliverOrdered([]byte{byte(i)})
		}
	}()

	for i := 0; i < frameCount; i++ {
		select {
		case got := <-conn.Receive():
			if len(got) != 1 || got[0] != byte(i) {
				t.Fatalf("frame %d = %v", i, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for frame %d", i)
		}
	}

	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("delivery remained blocked after receiver drained frames")
	}
}

func TestRelayReliableOrderedAcknowledgesOnlyContiguousFrames(t *testing.T) {
	conn := newTestRelayConnection()
	defer conn.Abort(errors.New("test complete"))
	conn.config.Reliability = ReliabilityReliableOrdered
	acks := make(chan uint64, 2)
	conn.sendEnvelope = func(env *RelayEnvelope) error {
		if env.Kind == RelayKindAck {
			acks <- env.AckSeq
		}
		return nil
	}

	conn.handleData(&RelayEnvelope{Kind: RelayKindData, Seq: 2, Payload: []byte("second")})
	select {
	case ack := <-acks:
		t.Fatalf("out-of-order frame produced ACK %d before the gap was filled", ack)
	case <-time.After(50 * time.Millisecond):
	}

	delivered := make(chan struct{})
	go func() {
		conn.handleData(&RelayEnvelope{Kind: RelayKindData, Seq: 1, Payload: []byte("first")})
		close(delivered)
	}()
	for _, want := range []string{"first", "second"} {
		select {
		case got := <-conn.Receive():
			if string(got) != want {
				t.Fatalf("received %q, want %q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %q", want)
		}
	}
	<-delivered
	select {
	case ack := <-acks:
		if ack != 2 {
			t.Fatalf("cumulative ACK = %d, want 2", ack)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cumulative ACK")
	}
}

func TestRelaySendFailureClosesWithoutWaitingForSendLoop(t *testing.T) {
	conn := newTestRelayConnection()
	conn.sendEnvelope = func(*RelayEnvelope) error { return errors.New("send failed") }
	conn.wg.Add(1)
	go conn.sendLoop()

	if err := conn.Send([]byte("payload")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case <-conn.closeCh:
	case <-time.After(time.Second):
		t.Fatal("send failure deadlocked the send loop")
	}
	conn.wg.Wait()
}

func TestRelayCloseAllClosesEveryConnection(t *testing.T) {
	relay := &Relay{conns: make(map[string]*RelayConnection)}
	connections := make([]*RelayConnection, 8)
	for i := range connections {
		connections[i] = newTestRelayConnection()
		connections[i].relay = relay
		connections[i].relayID = string(rune('a' + i))
		relay.conns[connections[i].relayID] = connections[i]
	}
	want := errors.New("disconnected")
	relay.closeAll(want)

	if len(relay.conns) != 0 {
		t.Fatalf("remaining relay connections = %d, want 0", len(relay.conns))
	}
	for _, conn := range connections {
		if conn.State() != RelayStateClosed || !errors.Is(conn.closeErr, want) {
			t.Fatalf("connection state = %v, error = %v", conn.State(), conn.closeErr)
		}
	}
}

func TestRelayConcurrentOnCloseCallbacksRunOnce(t *testing.T) {
	conn := newTestRelayConnection()
	const callbackCount = 128
	counts := make([]atomic.Int32, callbackCount)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range counts {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			conn.OnClose(func(error) { counts[index].Add(1) })
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		conn.handleClose(errors.New("concurrent close"))
	}()
	close(start)
	wg.Wait()

	for i := range counts {
		if got := counts[i].Load(); got != 1 {
			t.Fatalf("callback %d ran %d times, want 1", i, got)
		}
	}
}

func TestRelayConcurrentSendAndCloseKeepsCloseBarrierLast(t *testing.T) {
	conn := newTestRelayConnection()
	conn.sendCh = make(chan []byte, 257)
	const sends = 256
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < sends; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = conn.Send([]byte("payload"))
		}()
	}
	closeDone := make(chan error, 1)
	go func() {
		<-start
		closeDone <- conn.Close()
	}()
	close(start)
	wg.Wait()

	seenBarrier := false
	for len(conn.sendCh) > 0 {
		frame := <-conn.sendCh
		if frame == nil {
			seenBarrier = true
			continue
		}
		if seenBarrier {
			t.Fatal("successful Send was queued after the Close barrier")
		}
	}
	conn.handleClose(errors.New("test complete"))
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close did not exit after connection shutdown")
	}
}

func TestRelayOnCloseAfterClosureStillNotifies(t *testing.T) {
	conn := newTestRelayConnection()
	want := errors.New("connection lost")
	conn.handleClose(want)

	called := make(chan error, 1)
	conn.OnClose(func(err error) {
		called <- err
	})

	select {
	case got := <-called:
		if !errors.Is(got, want) {
			t.Fatalf("close reason = %v, want %v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("late OnClose callback was not invoked")
	}
}
