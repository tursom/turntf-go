package turntf

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func TestPlainPasswordDoesNotKeepPlaintext(t *testing.T) {
	password, err := PlainPassword("secret")
	if err != nil {
		t.Fatalf("PlainPassword: %v", err)
	}
	if password.WireValue() == "secret" {
		t.Fatal("expected wire value to be hashed")
	}
	if !password.IsHashed() {
		t.Fatal("expected password to report hashed state")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(password.WireValue()), []byte("secret")); err != nil {
		t.Fatalf("expected bcrypt password, got %v", err)
	}
}

func TestHashedPasswordPreservesHash(t *testing.T) {
	password, err := PlainPassword("secret")
	if err != nil {
		t.Fatalf("PlainPassword: %v", err)
	}
	hashed := HashedPassword(password.WireValue())
	if hashed.WireValue() != password.WireValue() {
		t.Fatal("expected hashed password to preserve encoded value")
	}
}

func TestHTTPClientLoginWithPasswordUsesProvidedHash(t *testing.T) {
	password, err := PlainPassword("secret")
	if err != nil {
		t.Fatalf("PlainPassword: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode login request: %v", err)
		}
		if got := req["password"]; got != password.WireValue() {
			t.Fatalf("unexpected password payload: %#v", got)
		}
		json.NewEncoder(w).Encode(map[string]any{"token": "token"})
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL)
	token, err := client.LoginWithPassword(context.Background(), 1, 2, HashedPassword(password.WireValue()))
	if err != nil {
		t.Fatalf("LoginWithPassword: %v", err)
	}
	if token != "token" {
		t.Fatalf("unexpected token: %q", token)
	}
}
