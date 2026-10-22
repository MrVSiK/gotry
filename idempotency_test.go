package gotry

import (
	"net/http"
	"testing"
)

func TestIsIdempotentMethod(t *testing.T) {
	for _, m := range []string{"", "GET", "HEAD", "OPTIONS", "TRACE", "PUT", "DELETE"} {
		if !isIdempotentMethod(m) {
			t.Errorf("%q should be idempotent", m)
		}
	}
	for _, m := range []string{"POST", "PATCH", "CONNECT", "PROPFIND"} {
		if isIdempotentMethod(m) {
			t.Errorf("%q should not be idempotent", m)
		}
	}
}

func TestHasIdempotencyKey(t *testing.T) {
	names := []string{"Idempotency-Key", "X-Idempotency-Key"}
	h := http.Header{}
	if hasIdempotencyKey(h, names) {
		t.Fatal("empty header")
	}
	h["X-Idempotency-Key"] = nil
	if !hasIdempotencyKey(h, names) {
		t.Fatal("present-with-nil should count")
	}
	h2 := http.Header{}
	h2.Set("idempotency-key", "abc")
	if !hasIdempotencyKey(h2, names) {
		t.Fatal("case-insensitive match")
	}
}
