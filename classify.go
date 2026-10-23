package gotry

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"syscall"
)

// Decision is a Classifier's verdict on an attempt.
type Decision int

const (
	// Default defers to the built-in rules.
	Default Decision = iota
	// Retry requests another attempt.
	Retry
	// DontRetry accepts the attempt's outcome as final.
	DontRetry
)

// Attempt describes the outcome of one round trip for a Classifier.
type Attempt struct {
	Request  *http.Request
	Response *http.Response
	Err      error
	Number   int
}

// Classifier decides whether an attempt should be retried.
type Classifier func(Attempt) Decision

// DefaultClassifier applies the built-in rules with the default retryable
// status set {502, 503, 504}, plus 429.
func DefaultClassifier(a Attempt) Decision {
	return builtinClassify(a, map[int]bool{502: true, 503: true, 504: true})
}

// builtinClassify applies the built-in rules. It never returns Default.
func builtinClassify(a Attempt, statuses map[int]bool) Decision {
	if a.Err != nil {
		return classifyError(a.Err)
	}
	if a.Response != nil && (a.Response.StatusCode == http.StatusTooManyRequests || statuses[a.Response.StatusCode]) {
		return Retry
	}
	return DontRetry
}

func classifyError(err error) Decision {
	// A per-attempt timeout is retried; it is checked before the context
	// errors, which also report Timeout() and would otherwise look alike.
	if errors.Is(err, errAttemptTimeout) {
		return Retry
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return DontRetry
	}
	var (
		certVerify *tls.CertificateVerificationError
		unknownCA  x509.UnknownAuthorityError
		hostname   x509.HostnameError
		certInval  x509.CertificateInvalidError
	)
	if errors.As(err, &certVerify) || errors.As(err, &unknownCA) ||
		errors.As(err, &hostname) || errors.As(err, &certInval) {
		return DontRetry
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		if dns.IsNotFound {
			return DontRetry
		}
		return Retry
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return Retry
	}
	var op *net.OpError
	if errors.As(err, &op) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return Retry
	}
	return DontRetry
}

// decide runs user first (when non-nil), falling back to the built-in rules
// when it returns Default. It reports whether the attempt should be retried.
func decide(a Attempt, user Classifier, statuses map[int]bool) bool {
	if user != nil {
		switch user(a) {
		case Retry:
			return true
		case DontRetry:
			return false
		}
	}
	return builtinClassify(a, statuses) == Retry
}

// attemptTimeoutError is substituted for the transport error when the
// per-attempt timer fires.
type attemptTimeoutError struct{}

func (attemptTimeoutError) Error() string   { return "gotry: attempt timed out" }
func (attemptTimeoutError) Timeout() bool   { return true }
func (attemptTimeoutError) Temporary() bool { return true }
