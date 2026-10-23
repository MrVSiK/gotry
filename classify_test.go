package gotry

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"syscall"
	"testing"
)

func TestBuiltinClassify(t *testing.T) {
	statuses := map[int]bool{502: true, 503: true, 504: true}
	resp := func(code int) Attempt { return Attempt{Response: &http.Response{StatusCode: code}} }
	errA := func(err error) Attempt { return Attempt{Err: err} }
	tests := []struct {
		name string
		a    Attempt
		want Decision
	}{
		{"200", resp(200), DontRetry},
		{"404", resp(404), DontRetry},
		{"500", resp(500), DontRetry},
		{"501", resp(501), DontRetry},
		{"502", resp(502), Retry},
		{"503", resp(503), Retry},
		{"504", resp(504), Retry},
		{"429", resp(429), Retry},
		{"attempt timeout", errA(attemptTimeoutError{}), Retry},
		{"conn refused", errA(&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}), Retry},
		{"wrapped reset", errA(fmt.Errorf("x: %w", syscall.ECONNRESET)), Retry},
		{"unexpected EOF", errA(io.ErrUnexpectedEOF), Retry},
		{"EOF", errA(io.EOF), Retry},
		{"dns timeout", errA(&net.DNSError{IsTimeout: true}), Retry},
		{"dns not found", errA(&net.DNSError{IsNotFound: true}), DontRetry},
		{"tls cert verification", errA(&tls.CertificateVerificationError{}), DontRetry},
		{"unknown authority", errA(x509.UnknownAuthorityError{}), DontRetry},
		{"weird", errA(errors.New("weird")), DontRetry},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := builtinClassify(tt.a, statuses); got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDecideUserOverrides(t *testing.T) {
	st := map[int]bool{503: true}
	never := func(Attempt) Decision { return DontRetry }
	always := func(Attempt) Decision { return Retry }
	pass := func(Attempt) Decision { return Default }
	a503 := Attempt{Response: &http.Response{StatusCode: 503}}
	a400 := Attempt{Response: &http.Response{StatusCode: 400}}
	if decide(a503, never, st) {
		t.Fatal("user DontRetry ignored")
	}
	if !decide(a400, always, st) {
		t.Fatal("user Retry ignored")
	}
	if !decide(a503, pass, st) || decide(a400, pass, st) {
		t.Fatal("Default should fall through")
	}
	if !decide(a503, nil, st) {
		t.Fatal("nil classifier should use builtin")
	}
}

func TestDefaultClassifierUsesDefaultStatuses(t *testing.T) {
	if DefaultClassifier(Attempt{Response: &http.Response{StatusCode: 502}}) != Retry {
		t.Fatal("502 should retry")
	}
	if DefaultClassifier(Attempt{Response: &http.Response{StatusCode: 500}}) != DontRetry {
		t.Fatal("500 should not retry")
	}
}

func TestContextErrorsNotRetried(t *testing.T) {
	for _, err := range []error{
		context.Canceled,
		context.DeadlineExceeded,
		&url.Error{Op: "Get", URL: "http://x/", Err: context.DeadlineExceeded},
		fmt.Errorf("wrapped: %w", context.Canceled),
	} {
		if d := DefaultClassifier(Attempt{Err: err}); d != DontRetry {
			t.Errorf("DefaultClassifier(%v) = %v, want DontRetry", err, d)
		}
	}
}

func TestAttemptTimeoutStillRetried(t *testing.T) {
	if errors.Is(errAttemptTimeout, context.DeadlineExceeded) || errors.Is(errAttemptTimeout, context.Canceled) {
		t.Fatal("attempt timeout matches a context error")
	}
	for _, err := range []error{
		errAttemptTimeout,
		&url.Error{Op: "Get", URL: "http://x/", Err: errAttemptTimeout},
	} {
		if d := DefaultClassifier(Attempt{Err: err}); d != Retry {
			t.Errorf("DefaultClassifier(%v) = %v, want Retry", err, d)
		}
	}
}
