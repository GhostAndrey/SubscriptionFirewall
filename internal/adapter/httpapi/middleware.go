package httpapi

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"subscriptionfirewall/internal/obs"
)

const (
	maxRequestIDLength = 64
	maxRateLimiters    = 65536
	limiterIdleTTL     = 10 * time.Minute

	routeContextKey contextKey = "route_capture"
)

type rateLimiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

type ipRateLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rateLimiterEntry
	limit    rate.Limit
	burst    int
	resolver clientIPResolver
}

func newIPRateLimiter(limitPerSecond float64, burst int, resolver clientIPResolver) *ipRateLimiter {
	if limitPerSecond <= 0 || burst <= 0 {
		return nil
	}
	return &ipRateLimiter{
		limiters: make(map[string]*rateLimiterEntry),
		limit:    rate.Limit(limitPerSecond),
		burst:    burst,
		resolver: resolver,
	}
}

func (l *ipRateLimiter) middleware(next http.Handler) http.Handler {
	if l == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isPublicPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		if !l.allow(l.resolver.ip(r)) {
			writeJSON(w, http.StatusTooManyRequests, ErrorResponse{Error: "rate limit exceeded"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (l *ipRateLimiter) allow(client string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.evict()
	entry, ok := l.limiters[client]
	if !ok {
		entry = &rateLimiterEntry{limiter: rate.NewLimiter(l.limit, l.burst)}
		l.limiters[client] = entry
	}
	entry.lastSeen = time.Now()
	return entry.limiter.Allow()
}

func (l *ipRateLimiter) evict() {
	if len(l.limiters) < maxRateLimiters {
		return
	}
	cutoff := time.Now().Add(-limiterIdleTTL)
	owners := make([]limiterOwner, 0, len(l.limiters))
	for key, entry := range l.limiters {
		if entry.lastSeen.Before(cutoff) {
			delete(l.limiters, key)
			continue
		}
		owners = append(owners, limiterOwner{key: key, lastSeen: entry.lastSeen})
	}
	if len(l.limiters) < maxRateLimiters {
		return
	}
	slices.SortFunc(owners, func(a, b limiterOwner) int {
		return a.lastSeen.Compare(b.lastSeen)
	})
	surplus := len(l.limiters) - maxRateLimiters + 1
	for _, owner := range owners[:surplus] {
		delete(l.limiters, owner.key)
	}
}

type limiterOwner struct {
	key      string
	lastSeen time.Time
}

type clientIPResolver struct {
	trusted []*net.IPNet
}

func newClientIPResolver(proxies []string) (clientIPResolver, error) {
	resolver := clientIPResolver{}
	for _, entry := range proxies {
		network, err := parseProxyCIDR(entry)
		if err != nil {
			return clientIPResolver{}, err
		}
		resolver.trusted = append(resolver.trusted, network)
	}
	return resolver, nil
}

func parseProxyCIDR(entry string) (*net.IPNet, error) {
	if _, network, err := net.ParseCIDR(strings.TrimSpace(entry)); err == nil {
		return network, nil
	}
	ip := net.ParseIP(strings.TrimSpace(entry))
	if ip == nil {
		return nil, fmt.Errorf("trusted proxy %q is not a CIDR or an IP address", entry)
	}
	bits := 32
	if ip.To4() == nil {
		bits = 128
	}
	return &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)}, nil
}

func (c clientIPResolver) ip(r *http.Request) string {
	remote := hostIP(r.RemoteAddr)
	if !c.isTrusted(remote) {
		return remote
	}
	if client := c.forwardedClient(r.Header.Get("X-Forwarded-For")); client != "" {
		return client
	}
	return remote
}

func (c clientIPResolver) isTrusted(ip string) bool {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	for _, network := range c.trusted {
		if network.Contains(parsed) {
			return true
		}
	}
	return false
}

func (c clientIPResolver) forwardedClient(header string) string {
	entries := strings.Split(header, ",")
	for i := len(entries) - 1; i >= 0; i-- {
		candidate := net.ParseIP(strings.TrimSpace(entries[i]))
		if candidate == nil {
			return ""
		}
		if c.isTrusted(candidate.String()) {
			continue
		}
		return candidate.String()
	}
	return ""
}

func hostIP(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return address
	}
	return host
}

// statusRecorder captures the response status for logging and metrics; Unwrap
// keeps flushing, hijacking and the original ResponseWriter API reachable.
type statusRecorder struct {
	http.ResponseWriter
	status  int
	written bool
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (r *statusRecorder) WriteHeader(status int) {
	if !r.written {
		r.status = status
		r.written = true
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(payload []byte) (int, error) {
	if !r.written {
		r.status = http.StatusOK
		r.written = true
	}
	return r.ResponseWriter.Write(payload)
}

func (r *statusRecorder) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		if !r.written {
			r.status = http.StatusOK
			r.written = true
		}
		flusher.Flush()
	}
}

func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("hijacking is not supported by %T", r.ResponseWriter)
	}
	return hijacker.Hijack()
}

func (r *statusRecorder) Status() int {
	if !r.written {
		return http.StatusOK
	}
	return r.status
}

func observationMiddleware(logger *slog.Logger, metrics *obs.Metrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w}
		route := &routeCapture{}
		r = r.WithContext(context.WithValue(r.Context(), routeContextKey, route))
		requestID := requestIDFromContext(r.Context())

		defer func() {
			routeLabel := route.pattern
			if routeLabel == "" {
				routeLabel = "unmatched"
			}
			duration := time.Since(started)
			metrics.ObserveHTTPRequest(r.Method, routeLabel, recorder.Status(), duration)
			logger.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", recorder.Status(),
				"duration_ms", duration.Milliseconds(),
				"request_id", requestID,
			)
		}()

		next.ServeHTTP(recorder, r)
	})
}

type routeCapture struct {
	pattern string
}

func routeMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		if capture, ok := r.Context().Value(routeContextKey).(*routeCapture); ok {
			capture.pattern = r.Pattern
		}
	})
}

func recoveryMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { //nolint:contextcheck // contextcheck cannot see the captured closure
			recovered := recover()
			if recovered == nil {
				return
			}
			logger.Error("panic recovered",
				"path", r.URL.Path,
				"panic", recovered,
				"stack", string(debug.Stack()),
				"request_id", requestIDFromContext(r.Context()),
			)
			writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "internal error"})
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := incomingRequestID(r.Header.Get("X-Request-Id"))
		if requestID == "" {
			generated, err := generateRequestID()
			if err != nil {
				s.logger.Error("generate request id failed", "error", err)
				writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "internal error"})
				return
			}
			requestID = generated
		}

		w.Header().Set("X-Request-Id", requestID)
		ctx := context.WithValue(r.Context(), requestIDContextKey, requestID)
		ctx = context.WithValue(ctx, loggerContextKey, s.logger.With("request_id", requestID))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func incomingRequestID(header string) string {
	header = strings.TrimSpace(header)
	if header == "" || len(header) > maxRequestIDLength || !isPrintableASCII(header) {
		return ""
	}
	return header
}

func isPrintableASCII(value string) bool {
	for _, char := range value {
		if char < 0x20 || char > 0x7E {
			return false
		}
	}
	return true
}

func generateRequestID() (string, error) {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("read random bytes: %w", err)
	}
	return hex.EncodeToString(buffer), nil
}

func requestIDFromContext(ctx context.Context) string {
	if requestID, ok := ctx.Value(requestIDContextKey).(string); ok {
		return requestID
	}
	return ""
}

func loggerFromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(loggerContextKey).(*slog.Logger); ok {
		return logger
	}
	return slog.Default()
}
