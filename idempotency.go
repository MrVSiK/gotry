package gotry

import "net/http"

// isIdempotentMethod reports whether method is idempotent per RFC 9110.
// The empty string counts as GET, matching net/http.
func isIdempotentMethod(method string) bool {
	switch method {
	case "", http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace,
		http.MethodPut, http.MethodDelete:
		return true
	}
	return false
}

// hasIdempotencyKey reports whether h contains any of names, matched by
// canonical key. A header present with an empty value still counts, as in
// net/http's own replay check.
func hasIdempotencyKey(h http.Header, names []string) bool {
	for _, n := range names {
		if _, ok := h[http.CanonicalHeaderKey(n)]; ok {
			return true
		}
	}
	return false
}
