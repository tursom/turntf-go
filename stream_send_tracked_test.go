package turntf

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	pb "github.com/tursom/turntf-go/internal/proto"
)

type streamSendRecordingHandler struct {
	NopHandler
	results chan StreamSendResult
}

func newStreamSendRecordingHandler(size int) *streamSendRecordingHandler {
	return &streamSendRecordingHandler{results: make(chan StreamSendResult, size)}
}

func (h *streamSendRecordingHandler) OnStreamSendResult(_ context.Context, result StreamSendResult) {
	h.results <- result
}

func newTrackedStreamTestClient(t *testing.T, limit int, handler Handler, serve func(*websocket.Conn)) *Client {
	t.Helper()
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
				User:            &pb.User{NodeId: 4096, UserId: 1025, Username: "sender", Role: "user"},
				ProtocolVersion: clientProtocolVersion,
				SessionRef:      &pb.SessionRef{ServingNodeId: 4096, SessionId: "sender-session"},
			},
		}})
		serve(conn)
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(Config{
		BaseURL:                server.URL,
		Credentials:            Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("password")},
		Handler:                handler,
		RequestTimeout:         2 * time.Second,
		PingInterval:           time.Hour,
		StreamSendPendingLimit: limit,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return client
}

func waitStreamSendResult(t *testing.T, results <-chan StreamSendResult) StreamSendResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for stream send result")
		return StreamSendResult{}
	}
}

func TestSendStreamFrameTrackedCrossPeerSuccess(t *testing.T) {
	handler := newStreamSendRecordingHandler(1)
	target := UserRef{NodeID: 8192, UserID: 2049}
	targetSession := SessionRef{ServingNodeID: 12288, SessionID: "remote-session"}
	frame := StreamFrame{Kind: StreamFrameData, ID: testStreamID(), Epoch: 3, Offset: 7, Payload: []byte("payload")}

	client := newTrackedStreamTestClient(t, 8, handler, func(conn *websocket.Conn) {
		req := mustReadClientEnvelope(t, conn).GetStreamFrame()
		if req.GetRequestId() == 0 {
			t.Error("tracked stream request has zero request ID")
		}
		if got := userRefFromProto(req.GetTarget()); got != target {
			t.Errorf("target = %+v, want %+v", got, target)
		}
		if got := sessionRefFromProto(req.GetTargetSession()); got != targetSession {
			t.Errorf("target session = %+v, want %+v", got, targetSession)
		}
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{Body: &pb.ServerEnvelope_StreamFrameResult{
			StreamFrameResult: &pb.StreamFrameResult{RequestId: req.GetRequestId()},
		}})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	requestID, err := client.SendStreamFrameTracked(ctx, target, targetSession, frame, DeliveryModeBestEffort)
	if err != nil {
		t.Fatalf("SendStreamFrameTracked: %v", err)
	}
	if requestID == 0 {
		t.Fatal("SendStreamFrameTracked returned zero request ID")
	}

	result := waitStreamSendResult(t, handler.results)
	if result.RequestID != requestID || result.Err != nil {
		t.Fatalf("result = %+v, want request ID %d and nil error", result, requestID)
	}
	wantMetadata := StreamSendMetadata{Target: target, TargetSession: targetSession, StreamID: frame.ID, Kind: frame.Kind, Epoch: frame.Epoch}
	if result.Metadata != wantMetadata {
		t.Fatalf("metadata = %+v, want %+v", result.Metadata, wantMetadata)
	}
}

func TestSendStreamFrameTrackedTargetedError(t *testing.T) {
	handler := newStreamSendRecordingHandler(1)
	client := newTrackedStreamTestClient(t, 8, handler, func(conn *websocket.Conn) {
		req := mustReadClientEnvelope(t, conn).GetStreamFrame()
		writeServerEnvelope(t, conn, &pb.ServerEnvelope{Body: &pb.ServerEnvelope_Error{
			Error: &pb.Error{Code: "stream_rejected", Message: "target unavailable", RequestId: req.GetRequestId()},
		}})
	})

	requestID, err := client.SendStreamFrameTracked(context.Background(), UserRef{NodeID: 4096, UserID: 2049}, SessionRef{}, StreamFrame{
		Kind: StreamFrameOpen,
		ID:   testStreamID(),
	}, DeliveryModeBestEffort)
	if err != nil {
		t.Fatalf("SendStreamFrameTracked: %v", err)
	}
	result := waitStreamSendResult(t, handler.results)
	if result.RequestID != requestID {
		t.Fatalf("result request ID = %d, want %d", result.RequestID, requestID)
	}
	var serverErr *ServerError
	if !errors.As(result.Err, &serverErr) || serverErr.Code != "stream_rejected" || serverErr.RequestID != requestID {
		t.Fatalf("result error = %#v, want targeted ServerError", result.Err)
	}
}

func TestSendStreamFrameTrackedConcurrent(t *testing.T) {
	const count = 64
	handler := newStreamSendRecordingHandler(count)
	client := newTrackedStreamTestClient(t, count, handler, func(conn *websocket.Conn) {
		requestIDs := make([]uint64, 0, count)
		for i := 0; i < count; i++ {
			requestIDs = append(requestIDs, mustReadClientEnvelope(t, conn).GetStreamFrame().GetRequestId())
		}
		for i := len(requestIDs) - 1; i >= 0; i-- {
			writeServerEnvelope(t, conn, &pb.ServerEnvelope{Body: &pb.ServerEnvelope_StreamFrameResult{
				StreamFrameResult: &pb.StreamFrameResult{RequestId: requestIDs[i]},
			}})
		}
	})

	var wg sync.WaitGroup
	ids := make(chan uint64, count)
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(offset uint64) {
			defer wg.Done()
			id, err := client.SendStreamFrameTracked(context.Background(), UserRef{NodeID: 8192, UserID: 2049}, SessionRef{}, StreamFrame{
				Kind:   StreamFrameData,
				ID:     testStreamID(),
				Offset: offset,
			}, DeliveryModeBestEffort)
			if err != nil {
				errs <- err
				return
			}
			ids <- id
		}(uint64(i))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent send: %v", err)
	}

	want := make(map[uint64]struct{}, count)
	close(ids)
	for id := range ids {
		if _, exists := want[id]; exists {
			t.Fatalf("duplicate request ID %d", id)
		}
		want[id] = struct{}{}
	}
	for i := 0; i < count; i++ {
		result := waitStreamSendResult(t, handler.results)
		if result.Err != nil {
			t.Errorf("request %d result error: %v", result.RequestID, result.Err)
		}
		if _, ok := want[result.RequestID]; !ok {
			t.Fatalf("unexpected or duplicate result request ID %d", result.RequestID)
		}
		delete(want, result.RequestID)
	}
	if len(want) != 0 {
		t.Fatalf("missing %d stream send results", len(want))
	}
}

func TestSendStreamFrameTrackedDisconnectCleansPending(t *testing.T) {
	const count = 3
	handler := newStreamSendRecordingHandler(count)
	client := newTrackedStreamTestClient(t, count, handler, func(conn *websocket.Conn) {
		for i := 0; i < count; i++ {
			_ = mustReadClientEnvelope(t, conn).GetStreamFrame()
		}
		_ = conn.Close(websocket.StatusInternalError, "connection lost")
	})

	want := make(map[uint64]struct{}, count)
	for i := 0; i < count; i++ {
		id, err := client.SendStreamFrameTracked(context.Background(), UserRef{NodeID: 4096, UserID: 2049}, SessionRef{}, StreamFrame{
			Kind: StreamFrameData,
			ID:   testStreamID(),
		}, DeliveryModeBestEffort)
		if err != nil {
			t.Fatalf("SendStreamFrameTracked %d: %v", i, err)
		}
		want[id] = struct{}{}
	}
	for i := 0; i < count; i++ {
		result := waitStreamSendResult(t, handler.results)
		if !errors.Is(result.Err, ErrDisconnected) {
			t.Fatalf("disconnect result error = %v, want ErrDisconnected", result.Err)
		}
		delete(want, result.RequestID)
	}
	if len(want) != 0 {
		t.Fatalf("disconnect cleanup missed %d pending sends", len(want))
	}
}

func TestSendStreamFrameTrackedCloseCleansPending(t *testing.T) {
	handler := newStreamSendRecordingHandler(1)
	requestRead := make(chan struct{})
	client := newTrackedStreamTestClient(t, 1, handler, func(conn *websocket.Conn) {
		_ = mustReadClientEnvelope(t, conn).GetStreamFrame()
		close(requestRead)
		<-time.After(3 * time.Second)
	})

	requestID, err := client.SendStreamFrameTracked(context.Background(), UserRef{NodeID: 4096, UserID: 2049}, SessionRef{}, StreamFrame{
		Kind: StreamFrameClose,
		ID:   testStreamID(),
	}, DeliveryModeBestEffort)
	if err != nil {
		t.Fatalf("SendStreamFrameTracked: %v", err)
	}
	<-requestRead
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	result := waitStreamSendResult(t, handler.results)
	if result.RequestID != requestID || !errors.Is(result.Err, ErrClosed) {
		t.Fatalf("close result = %+v, want request %d with ErrClosed", result, requestID)
	}
}

func TestSendStreamFrameTrackedBoundedForOldServerAndLegacyUntracked(t *testing.T) {
	handler := newStreamSendRecordingHandler(2)
	legacyRequest := make(chan *pb.StreamFrameRequest, 1)
	client := newTrackedStreamTestClient(t, 2, handler, func(conn *websocket.Conn) {
		_ = mustReadClientEnvelope(t, conn).GetStreamFrame()
		_ = mustReadClientEnvelope(t, conn).GetStreamFrame()
		legacyRequest <- mustReadClientEnvelope(t, conn).GetStreamFrame()
		<-time.After(3 * time.Second)
	})

	frame := StreamFrame{Kind: StreamFrameData, ID: testStreamID(), Payload: []byte("old-server")}
	for i := 0; i < 2; i++ {
		if _, err := client.SendStreamFrameTracked(context.Background(), UserRef{NodeID: 4096, UserID: 2049}, SessionRef{}, frame, DeliveryModeBestEffort); err != nil {
			t.Fatalf("tracked send %d: %v", i, err)
		}
	}
	if _, err := client.SendStreamFrameTracked(context.Background(), UserRef{NodeID: 4096, UserID: 2049}, SessionRef{}, frame, DeliveryModeBestEffort); !errors.Is(err, ErrStreamSendPendingFull) {
		t.Fatalf("send beyond pending limit = %v, want ErrStreamSendPendingFull", err)
	}
	if _, err := client.SendStreamFrame(context.Background(), UserRef{NodeID: 4096, UserID: 2049}, SessionRef{}, frame, DeliveryModeBestEffort); err != nil {
		t.Fatalf("legacy SendStreamFrame: %v", err)
	}
	select {
	case req := <-legacyRequest:
		if req.GetRequestId() != 0 {
			t.Fatalf("legacy request ID = %d, want 0", req.GetRequestId())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for legacy stream request")
	}
}

func TestStreamErrorPrefersRPCPending(t *testing.T) {
	handler := newStreamSendRecordingHandler(1)
	client, err := NewClient(Config{
		BaseURL:     "http://localhost",
		Credentials: Credentials{NodeID: 4096, UserID: 1025, Password: MustPlainPassword("password")},
		Handler:     handler,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	const requestID = 7
	rpcResult, err := client.registerPending(requestID)
	if err != nil {
		t.Fatalf("registerPending: %v", err)
	}
	metadata := StreamSendMetadata{Target: UserRef{NodeID: 4096, UserID: 2049}, StreamID: testStreamID(), Kind: StreamFrameOpen}
	if err := client.registerStreamSend(requestID, metadata); err != nil {
		t.Fatalf("registerStreamSend: %v", err)
	}
	serverErr := &pb.Error{Code: "rpc_error", Message: "rpc wins", RequestId: requestID}
	if err := client.handleServerEnvelope(&pb.ServerEnvelope{Body: &pb.ServerEnvelope_Error{Error: serverErr}}); err != nil {
		t.Fatalf("handleServerEnvelope: %v", err)
	}

	select {
	case result := <-rpcResult:
		var got *ServerError
		if !errors.As(result.err, &got) || got.Code != serverErr.Code {
			t.Fatalf("RPC result error = %#v", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("RPC pending did not receive targeted error")
	}
	select {
	case result := <-handler.results:
		t.Fatalf("stream handler incorrectly received RPC error: %+v", result)
	default:
	}
	if got, ok := client.takeStreamSend(requestID); !ok || got != metadata {
		t.Fatalf("stream pending was consumed: metadata=%+v present=%v", got, ok)
	}
	client.unregisterPending(requestID)
}
