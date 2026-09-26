package httpapi

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"
)

type statusRecorder struct {
	inner  http.ResponseWriter
	status int
}

func (r *statusRecorder) Header() http.Header { return r.inner.Header() }

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.inner.WriteHeader(status)
}

func (r *statusRecorder) Write(payload []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.inner.Write(payload)
}

func loggingMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{inner: w}

		next.ServeHTTP(recorder, r)

		logger.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.status,
			"duration_ms", time.Since(started).Milliseconds(),
		)
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
			)
			writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "internal error"})
		}()
		next.ServeHTTP(w, r)
	})
}
