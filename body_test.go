package gotry

import (
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

// closeTracker wraps an io.Reader, recording bytes read and whether Close was
// called. It is safe for concurrent use.
type closeTracker struct {
	r      io.Reader
	closed atomic.Bool
	n      atomic.Int64
}

func newCloseTracker(r io.Reader) *closeTracker { return &closeTracker{r: r} }

func (c *closeTracker) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

func (c *closeTracker) Close() error {
	c.closed.Store(true)
	return nil
}

func (c *closeTracker) Closed() bool     { return c.closed.Load() }
func (c *closeTracker) BytesRead() int64 { return c.n.Load() }

type neverEnding struct{}

func (neverEnding) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

type errAfterReader struct {
	data string
	err  error
	off  int
}

func (e *errAfterReader) Read(p []byte) (int, error) {
	if e.off >= len(e.data) {
		return 0, e.err
	}
	n := copy(p, e.data[e.off:])
	e.off += n
	return n, nil
}

func readAll(t *testing.T, rc io.ReadCloser) string {
	t.Helper()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()
	return string(b)
}

func TestBodySourceNil(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://example.com", nil)
	src, err := newBodySource(req, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !src.replayable {
		t.Fatal("nil body should be replayable")
	}
	rc, err := src.get()
	if err != nil {
		t.Fatal(err)
	}
	if rc != http.NoBody {
		t.Fatal("want http.NoBody")
	}
}

func TestBodySourceGetBody(t *testing.T) {
	req, _ := http.NewRequest("POST", "http://example.com", strings.NewReader("abc"))
	src, err := newBodySource(req, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !src.replayable {
		t.Fatal("GetBody should be replayable")
	}
	for i := 0; i < 3; i++ {
		rc, err := src.get()
		if err != nil {
			t.Fatal(err)
		}
		if got := readAll(t, rc); got != "abc" {
			t.Fatalf("call %d: got %q", i, got)
		}
	}
}

func TestBodySourceBuffersSmallBody(t *testing.T) {
	tr := newCloseTracker(strings.NewReader("hello"))
	req, _ := http.NewRequest("POST", "http://example.com", tr)
	req.GetBody = nil
	src, err := newBodySource(req, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !src.replayable {
		t.Fatal("small body should be replayable")
	}
	if !tr.Closed() {
		t.Fatal("original body should be closed after buffering")
	}
	for i := 0; i < 3; i++ {
		rc, err := src.get()
		if err != nil {
			t.Fatal(err)
		}
		if got := readAll(t, rc); got != "hello" {
			t.Fatalf("call %d: got %q", i, got)
		}
	}
}

func TestBodySourceOversizeNotReplayable(t *testing.T) {
	tr := newCloseTracker(strings.NewReader("0123456789"))
	req, _ := http.NewRequest("POST", "http://example.com", tr)
	req.GetBody = nil
	src, err := newBodySource(req, 4)
	if err != nil {
		t.Fatal(err)
	}
	if src.replayable {
		t.Fatal("oversize body should not be replayable")
	}
	rc, err := src.get()
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(rc)
	if err != nil || string(b) != "0123456789" {
		t.Fatalf("got %q, %v", b, err)
	}
	if tr.Closed() {
		t.Fatal("original closed too early")
	}
	rc.Close()
	if !tr.Closed() {
		t.Fatal("closing returned body should close original")
	}
}

func TestBodySourceReadError(t *testing.T) {
	boom := errors.New("boom")
	tr := newCloseTracker(&errAfterReader{data: "ab", err: boom})
	req, _ := http.NewRequest("POST", "http://example.com", tr)
	req.GetBody = nil
	if _, err := newBodySource(req, 1<<20); !errors.Is(err, boom) {
		t.Fatalf("got %v, want boom", err)
	}
	if !tr.Closed() {
		t.Fatal("original should be closed on read error")
	}
}

func TestWrapBodyReleasesOnceOnCloseOrEOF(t *testing.T) {
	var n atomic.Int32
	rc := wrapBody(io.NopCloser(strings.NewReader("abc")), func() { n.Add(1) })
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatal(err)
	}
	rc.Close()
	rc.Close()
	if n.Load() != 1 {
		t.Fatalf("release count %d, want 1", n.Load())
	}

	var m atomic.Int32
	rc2 := wrapBody(io.NopCloser(strings.NewReader("abc")), func() { m.Add(1) })
	rc2.Close()
	if m.Load() != 1 {
		t.Fatalf("early close release count %d, want 1", m.Load())
	}
}

func TestDrainAndCloseLimits(t *testing.T) {
	tr := newCloseTracker(neverEnding{})
	drainAndClose(tr)
	if !tr.Closed() {
		t.Fatal("not closed")
	}
	if n := tr.BytesRead(); n > 64<<10+32<<10 {
		t.Fatalf("read %d bytes, too many", n)
	}
	drainAndClose(nil)
}

func TestBodySourceMaxInt64Buffer(t *testing.T) {
	for _, max := range []int64{math.MaxInt64, math.MaxInt64 - 1} {
		req, _ := http.NewRequest(http.MethodPut, "http://x/", io.NopCloser(strings.NewReader("hello")))
		src, err := newBodySource(req, max)
		if err != nil {
			t.Fatal(err)
		}
		if !src.replayable {
			t.Fatalf("max=%d: not replayable", max)
		}
		for i := range 2 {
			rc, _ := src.get()
			if got := readAll(t, rc); got != "hello" {
				t.Fatalf("max=%d get %d = %q, want hello", max, i, got)
			}
		}
	}
}

func TestBodySourceNegativeBufferMeansZero(t *testing.T) {
	ct := newCloseTracker(strings.NewReader("hello"))
	req, _ := http.NewRequest(http.MethodPut, "http://x/", ct)
	src, err := newBodySource(req, -5)
	if err != nil {
		t.Fatal(err)
	}
	if src.replayable {
		t.Fatal("non-empty body replayable with a negative buffer limit")
	}
	rc, _ := src.get()
	if got := readAll(t, rc); got != "hello" {
		t.Fatalf("body = %q, want hello", got)
	}
}

func TestWrapBodyWriterOnlyWhenInnerWrites(t *testing.T) {
	if _, ok := wrapBody(io.NopCloser(strings.NewReader("")), func() {}).(io.Writer); ok {
		t.Fatal("wrapped read-only body implements io.Writer")
	}
	var released int
	inner := &rwBody{Reader: strings.NewReader("")}
	wb := wrapBody(inner, func() { released++ })
	w, ok := wb.(io.ReadWriteCloser)
	if !ok {
		t.Fatal("wrapped read-write body does not implement io.ReadWriteCloser")
	}
	io.WriteString(w, "hi")
	w.Close()
	w.Close()
	if inner.wrote.String() != "hi" || released != 1 {
		t.Fatalf("wrote %q released %d", inner.wrote.String(), released)
	}
}
