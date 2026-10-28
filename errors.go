package gotry

import "fmt"

// Error is returned when gotry gives up after two or more attempts with an
// error. When the final attempt produced a response, that response is
// returned with a nil error instead. Single-attempt errors are returned
// unwrapped.
type Error struct {
	// Attempts is the number of attempts made.
	Attempts int
	// Err is the last attempt's error.
	Err error
}

func (e *Error) Error() string {
	return fmt.Sprintf("gotry: giving up after %d attempts: %v", e.Attempts, e.Err)
}

// Unwrap returns the last attempt's error.
func (e *Error) Unwrap() error { return e.Err }
