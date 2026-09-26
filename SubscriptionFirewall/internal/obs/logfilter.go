package obs

import (
	"context"
	"log/slog"

	"subscriptionfirewall/pkg/masking"
)

var sensitiveAttributeKeys = map[string]bool{
	"pan":           true,
	"card_number":   true,
	"token":         true,
	"token_id":      true,
	"authorization": true,
}

type SanitizingHandler struct {
	inner slog.Handler
}

func NewSanitizingHandler(inner slog.Handler) *SanitizingHandler {
	return &SanitizingHandler{inner: inner}
}

func (h *SanitizingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *SanitizingHandler) Handle(ctx context.Context, record slog.Record) error {
	sanitized := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	record.Attrs(func(attr slog.Attr) bool {
		sanitized.AddAttrs(h.sanitize(attr))
		return true
	})
	return h.inner.Handle(ctx, sanitized)
}

func (h *SanitizingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	sanitized := make([]slog.Attr, 0, len(attrs))
	for _, attr := range attrs {
		sanitized = append(sanitized, h.sanitize(attr))
	}
	return &SanitizingHandler{inner: h.inner.WithAttrs(sanitized)}
}

func (h *SanitizingHandler) WithGroup(name string) slog.Handler {
	return &SanitizingHandler{inner: h.inner.WithGroup(name)}
}

func (h *SanitizingHandler) sanitize(attr slog.Attr) slog.Attr {
	if sensitiveAttributeKeys[attr.Key] && attr.Value.Kind() == slog.KindString {
		return slog.String(attr.Key, masking.Token(attr.Value.String()))
	}
	return attr
}
