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
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"subscriptionfirewall/internal/obs"
)

const (
	maxRequestIDLength = 64
	maxRateLimiters    = 65536
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
}

func newIPRateLimiter(limitPerSecond float64, burst int) *ipRateLimiter {
	if limitPerSecond <= 0 || burst <= 0 {
		return nil
	}
	return &ipRateLimiter{
		limiters: make(map[string]*rateLimiterEntry),
		limit:    rate.Limit(limitPerSecond),
		burst:    burst,
	}
}

func (l *ipRateLimiter) middleware(next http.Handler) http.Handler {
	if l == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if publicPaths[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}
		if !l.allow(clientIP(r)) {
			writeJSON(w, http.StatusTooManyRequests, ErrorResponse{Error: "rate limit exceeded"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (l *ipRateLimiter) allow(client string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.limiters) > maxRateLimiters {
		cutoff := time.Now().Add(-10 * time.Minute)
		for key, entry := range l.limiters {
			if entry.lastSeen.Before(cutoff) {
				delete(l.limiters, key)
			}
		}
	}

	entry, ok := l.limiters[client]
	if !ok {
		entry = &rateLimiterEntry{limiter: rate.NewLimiter(l.limit, l.burst)}
		l.limiters[client] = entry
	}
	entry.lastSeen = time.Now()
	return entry.limiter.Allow()
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// statusRecorder captures the response status for logging and metrics. It
// forwards to the wrapped writer through Unwrap so handlers keep full access
// to flushing, hijacking and the original ResponseWriter API.
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

// Hijack forwards to the wrapped writer when it supports hijacking, which
// keeps websockets and similar upgrades working through the middleware chain.
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

// observationMiddleware records one entry per request: the status code and the
// latency feed both the access log and the Prometheus metrics, so the chain
// wraps the handler exactly once.
func observationMiddleware(logger *slog.Logger, metrics *obs.Metrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w}

		defer func() {
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			duration := time.Since(started)
			metrics.ObserveHTTPRequest(r.Method, route, recorder.Status(), duration)
			logger.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", recorder.Status(),
				"duration_ms", duration.Milliseconds(),
				"request_id", requestIDFromContext(r.Context()),
			)
		}()

		next.ServeHTTP(recorder, r)
	})
}

func recoveryMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
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

// requestIDMiddleware assigns every request a correlation id: the incoming
// X-Request-Id header when present and well-formed, a random hex id otherwise.
// The id is echoed in the response and a request-scoped logger carrying it is
// stored in the context for downstream handlers.
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
