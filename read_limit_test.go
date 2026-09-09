package turntf

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestDialReadLimit(t *testing.T) {
	for _, size := range []int{(32 << 10) + 512, 1 << 20, (1 << 20) + 1} {
		t.Run(fmtSize(size), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				conn.Write(r.Context(), websocket.MessageBinary, make([]byte, size))
				conn.Read(r.Context())
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			client := &Client{cfg: Config{BaseURL: server.URL, HTTPClient: server.Client()}}
			conn, err := client.dial(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.CloseNow()
			_, body, err := conn.Read(ctx)
			if size <= 1<<20 {
				if err != nil || len(body) != size {
					t.Fatalf("valid frame size=%d got=%d err=%v", size, len(body), err)
				}
			} else if !errors.Is(err, websocket.ErrMessageTooBig) {
				t.Fatalf("oversized frame error = %v, want ErrMessageTooBig", err)
			}
		})
	}
}
func fmtSize(n int) string { return fmt.Sprintf("%d", n) }
