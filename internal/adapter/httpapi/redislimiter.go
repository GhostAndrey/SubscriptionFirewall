package httpapi

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	redisKeyPrefix = "subscription_firewall:ratelimit:"
	redisWindow    = time.Second
)

// redisRateLimiter is a fixed-window counter shared by all replicas: with the
// same Redis, N instances still allow the configured requests per second in
// total, not per instance. When Redis is unreachable the limiter fails open
// for reads, but money-moving endpoints are refused instead: availability is
// not worth letting an outage turn into an unmetered spending window.
type redisRateLimiter struct {
	client *redis.Client
	limit  int
	burst  int
	window time.Duration
	logger *slog.Logger
}

func newRedisRateLimiter(client *redis.Client, limit, burst int, logger *slog.Logger) *redisRateLimiter {
	if client == nil || limit <= 0 {
		return nil
	}
	if burst < limit {
		burst = limit
	}
	return &redisRateLimiter{
		client: client,
		limit:  limit,
		burst:  burst,
		window: redisWindow,
		logger: logger,
	}
}

func (l *redisRateLimiter) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if publicPaths[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}

		allowed, retryAfter, err := l.allow(r.Context(), clientIP(r))
		if err != nil {
			l.logger.Error("redis rate limit check failed", "path", r.URL.Path, "error", err)
			if requiresHardLimit(r) {
				writeJSON(w, http.StatusServiceUnavailable, ErrorResponse{Error: "rate limiter unavailable"})
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		if !allowed {
			w.Header().Set("Retry-After", strconv.Itoa(max(retryAfter, 1)))
			writeJSON(w, http.StatusTooManyRequests, ErrorResponse{Error: "rate limit exceeded"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requiresHardLimit marks endpoints that move money or provision cards. Their
// volume is low and bounded, so refusing them while the shared counter is
// unavailable is cheaper than letting them through unmetered.
func requiresHardLimit(r *http.Request) bool {
	if r.Method != http.MethodPost {
		return false
	}
	return strings.HasSuffix(r.URL.Path, "/authorize")
}

func (l *redisRateLimiter) allow(ctx context.Context, client string) (allowed bool, retryAfterSeconds int, err error) {
	windowSeconds := max(int(l.window/time.Second), 1)
	key := fmt.Sprintf("%s%s:%d", redisKeyPrefix, client, l.now()/int64(windowSeconds))

	pipe := l.client.TxPipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, 2*time.Duration(windowSeconds)*time.Second)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, 0, fmt.Errorf("rate limit counter: %w", err)
	}

	if incr.Val() > int64(l.burst) {
		retry := windowSeconds - int(l.now()%int64(windowSeconds))
		return false, retry, nil
	}
	return true, 0, nil
}

func (l *redisRateLimiter) now() int64 { return time.Now().Unix() }
