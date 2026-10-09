package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"

	"subscriptionfirewall/pkg/masking"
)

const (
	scopeFull   = "full"
	scopeIngest = "ingest"
	scopeRead   = "read"
	scopeManage = "manage"

	actorContextKey     contextKey = "api_key_actor"
	requestIDContextKey contextKey = "request_id"
	loggerContextKey    contextKey = "request_logger"
)

type contextKey string

type apiKey struct {
	value  string
	scopes map[string]bool
}

func (k apiKey) allows(scope string) bool {
	return k.scopes[scopeFull] || k.scopes[scope]
}

type apiKeyAuthenticator struct {
	keys []apiKey
}

func newAPIKeyAuthenticator(keys []string) *apiKeyAuthenticator {
	parsed := make([]apiKey, 0, len(keys))
	for _, entry := range keys {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		value, scopeList := entry, scopeFull
		if valuePart, scopePart, found := strings.Cut(entry, ":"); found && valuePart != "" {
			value, scopeList = valuePart, scopePart
		}
		scopes := make(map[string]bool)
		for _, scope := range strings.Split(scopeList, ",") {
			if scope = strings.TrimSpace(scope); scope != "" {
				scopes[scope] = true
			}
		}
		if len(scopes) > 0 {
			parsed = append(parsed, apiKey{value: value, scopes: scopes})
		}
	}
	return &apiKeyAuthenticator{keys: parsed}
}

func (a *apiKeyAuthenticator) enabled() bool {
	return len(a.keys) > 0
}

func (a *apiKeyAuthenticator) middleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !a.enabled() || !requiresAuth(r) {
			next.ServeHTTP(w, r)
			return
		}

		presented := apiKeyFromRequest(r)
		key, ok := a.matches(presented)
		if !ok {
			logger.Warn("unauthorized request",
				"method", r.Method,
				"path", r.URL.Path,
				"key", masking.Token(presented),
			)
			writeJSON(w, http.StatusUnauthorized, ErrorResponse{Error: "unauthorized"})
			return
		}

		if scope := requiredScope(r); scope != "" && !key.allows(scope) {
			logger.Warn("forbidden request",
				"method", r.Method,
				"path", r.URL.Path,
				"key", masking.Token(key.value),
				"required_scope", scope,
			)
			writeJSON(w, http.StatusForbidden, ErrorResponse{Error: "forbidden: missing " + scope + " scope"})
			return
		}

		ctx := context.WithValue(r.Context(), actorContextKey, masking.Token(key.value))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// matches walks every configured key without early exit so the number of
// comparisons does not depend on which key matched. Values are hashed first
// because ConstantTimeCompare short-circuits on differing lengths, which would
// otherwise leak the configured key length through response timing.
func (a *apiKeyAuthenticator) matches(presented string) (apiKey, bool) {
	var found apiKey
	matched := false

	for _, key := range a.keys {
		if subtle.ConstantTimeCompare(apiKeyDigest(key.value), apiKeyDigest(presented)) == 1 {
			found = key
			matched = true
		}
	}
	return found, matched
}

func apiKeyDigest(value string) []byte {
	digest := sha256.Sum256([]byte(value))
	return digest[:]
}

var publicPaths = map[string]bool{
	"/healthz": true,
	"/readyz":  true,
	"/version": true,
	"/metrics": true,
}

func requiresAuth(r *http.Request) bool {
	return !publicPaths[r.URL.Path]
}

func requiredScope(r *http.Request) string {
	if !strings.HasPrefix(r.URL.Path, "/v1/") {
		return ""
	}
	switch {
	case r.Method == http.MethodGet:
		return scopeRead
	case r.Method == http.MethodPost && r.URL.Path == "/v1/transactions":
		return scopeIngest
	default:
		return scopeManage
	}
}

func apiKeyFromRequest(r *http.Request) string {
	if header := r.Header.Get("X-API-Key"); header != "" {
		return header
	}
	const prefix = "Bearer "
	if authorization := r.Header.Get("Authorization"); strings.HasPrefix(authorization, prefix) {
		return strings.TrimPrefix(authorization, prefix)
	}
	return ""
}

func actorFromContext(ctx context.Context) string {
	if actor, ok := ctx.Value(actorContextKey).(string); ok {
		return actor
	}
	return "anonymous"
}
