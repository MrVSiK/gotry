package gotry

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// step scripts one answer of scriptRT.
type step struct {
	status int
	header http.Header
	body   string
	err    error
	delay  time.Duration // waits (respecting r.Context()) before answering
}

// scriptRT is a fake http.RoundTripper that answers from a script.
type scriptRT struct {
	mu     sync.Mutex
	steps  []step // step i answers call i; calls past the end repeat the last step
	calls  int
	bodies []string        // request bodies received, in call order
	resps  []*closeTracker // every response body handed out
}

func (s *scriptRT) RoundTrip(r *http.Request) (*http.Response, error) {
	var body string
	if r.Body != nil {
		b, _ := io.ReadAll(r.Body)
		r.Body.Close()
		body = string(b)
	}
	s.mu.Lock()
	st := s.steps[min(s.calls, len(s.steps)-1)]
	s.calls++
	s.bodies = append(s.bodies, body)
	s.mu.Unlock()

	if st.delay > 0 {
		tm := time.NewTimer(st.delay)
		defer tm.Stop()
		select {
		case <-tm.C:
		case <-r.Context().Done():
			return nil, context.Cause(r.Context())
		}
	}
	if st.err != nil {
		return nil, st.err
	}
	ct := newCloseTracker(strings.NewReader(st.body))
	s.mu.Lock()
	s.resps = append(s.resps, ct)
	s.mu.Unlock()
	h := st.header.Clone()
	if h == nil {
		h = http.Header{}
	}
	return &http.Response{
		Status:        http.StatusText(st.status),
		StatusCode:    st.status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        h,
		Body:          ct,
		ContentLength: int64(len(st.body)),
		Request:       r,
	}, nil
}

// allClosed reports whether every response body handed out has been closed.
func (s *scriptRT) allClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ct := range s.resps {
		if !ct.Closed() {
			return false
		}
	}
	return true
}

func (s *scriptRT) numCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *scriptRT) gotBodies() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.bodies...)
}

func (s *scriptRT) respBodies() []*closeTracker {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*closeTracker(nil), s.resps...)
}
