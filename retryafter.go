package gotry

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// parseRetryAfter parses a Retry-After header value, which is either a number
// of seconds or an HTTP date. A date in the past yields (0, true). Negative
// seconds and unparseable values yield (0, false).
func parseRetryAfter(v string, now time.Time) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0, false
		}
		if secs > int(math.MaxInt64/int64(time.Second)) {
			return time.Duration(math.MaxInt64), true // avoid overflow
		}
		return time.Duration(secs) * time.Second, true
	}
	t, err := http.ParseTime(v)
	if err != nil {
		return 0, false
	}
	return max(t.Sub(now), 0), true
}
