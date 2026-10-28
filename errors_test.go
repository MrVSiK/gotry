package gotry

import (
	"errors"
	"strings"
	"syscall"
	"testing"
)

func TestErrorUnwrap(t *testing.T) {
	e := &Error{Attempts: 3, Err: syscall.ECONNRESET}
	if !errors.Is(e, syscall.ECONNRESET) {
		t.Error("errors.Is failed")
	}
	if !strings.Contains(e.Error(), "after 3 attempts") {
		t.Errorf("Error() = %q", e.Error())
	}
}
