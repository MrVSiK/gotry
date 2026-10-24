package gotry

import (
	"bytes"
	"io"
	"math"
	"net/http"
	"sync"
)

// drainLimit bounds how much of a discarded response body is read before it
// is closed.
const drainLimit = 64 << 10

// bodySource yields a request body for each attempt.
type bodySource struct {
	replayable bool
	get        func() (io.ReadCloser, error)
	getBody    func() (io.ReadCloser, error) // set as each attempt's GetBody; may be nil
}

// newBodySource prepares req's body for replay across attempts. Bodies larger
// than maxBuffer without GetBody yield a single-use, non-replayable source.
func newBodySource(req *http.Request, maxBuffer int64) (bodySource, error) {
	if req.Body == nil || req.Body == http.NoBody {
		get := func() (io.ReadCloser, error) { return http.NoBody, nil }
		return bodySource{replayable: true, get: get, getBody: get}, nil
	}
	if req.GetBody != nil {
		return bodySource{replayable: true, get: req.GetBody, getBody: req.GetBody}, nil
	}
	maxBuffer = max(maxBuffer, 0)
	limit := maxBuffer
	if limit < math.MaxInt64 {
		limit++ // one extra byte tells a body that fits from one that does not
	}
	orig := req.Body
	prefix, err := io.ReadAll(io.LimitReader(orig, limit))
	if err != nil {
		orig.Close()
		return bodySource{}, err
	}
	if int64(len(prefix)) <= maxBuffer {
		orig.Close()
		get := func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(prefix)), nil
		}
		return bodySource{replayable: true, get: get, getBody: get}, nil
	}
	return bodySource{replayable: false, get: func() (io.ReadCloser, error) {
		return struct {
			io.Reader
			io.Closer
		}{io.MultiReader(bytes.NewReader(prefix), orig), orig}, nil
	}}, nil
}

// singleUseBody passes req's body through untouched for a request that gets
// exactly one attempt. GetBody is kept so the base RoundTripper can still use
// it for its own replays.
func singleUseBody(req *http.Request) bodySource {
	body := req.Body
	return bodySource{
		get:     func() (io.ReadCloser, error) { return body, nil },
		getBody: req.GetBody,
	}
}

// wrapBody returns rc with release called exactly once, at the first Close or
// the first read that returns io.EOF. When rc is also an io.Writer, as the
// body of a 101 Switching Protocols response is, the result is too.
func wrapBody(rc io.ReadCloser, release func()) io.ReadCloser {
	b := &releaseBody{rc: rc, release: release}
	if w, ok := rc.(io.Writer); ok {
		return &releaseRWBody{releaseBody: b, w: w}
	}
	return b
}

type releaseRWBody struct {
	*releaseBody
	w io.Writer
}

func (b *releaseRWBody) Write(p []byte) (int, error) { return b.w.Write(p) }

type releaseBody struct {
	rc      io.ReadCloser
	release func()
	once    sync.Once
}

func (b *releaseBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if err == io.EOF {
		b.once.Do(b.release)
	}
	return n, err
}

func (b *releaseBody) Close() error {
	err := b.rc.Close()
	b.once.Do(b.release)
	return err
}

// drainAndClose reads at most drainLimit bytes from rc, then closes it so the
// connection can be reused when the body is small. A nil rc is a no-op.
func drainAndClose(rc io.ReadCloser) {
	if rc == nil {
		return
	}
	io.Copy(io.Discard, io.LimitReader(rc, drainLimit))
	rc.Close()
}
