package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"subscriptionfirewall/internal/domain"
)

type ErrorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if payload != nil {
		_ = json.NewEncoder(w).Encode(payload)
	}
}

func writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: err.Error()})
	case errors.Is(err, domain.ErrAlreadyExists):
		writeJSON(w, http.StatusConflict, ErrorResponse{Error: err.Error()})
	case errors.Is(err, domain.ErrInvalidTransition):
		writeJSON(w, http.StatusConflict, ErrorResponse{Error: err.Error()})
	case errors.Is(err, domain.ErrSpendLimitExceeded):
		writeJSON(w, http.StatusPaymentRequired, ErrorResponse{Error: err.Error()})
	case errors.Is(err, domain.ErrTokenNotActive):
		writeJSON(w, http.StatusForbidden, ErrorResponse{Error: err.Error()})
	case errors.Is(err, domain.ErrInvalidArgument), errors.Is(err, domain.ErrInvalidAmount), errors.Is(err, domain.ErrUnknownCurrency):
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "internal error"})
	}
}

const maxRequestBodyBytes = 1 << 20

func decodeJSON[T any](r *http.Request) (T, error) {
	defer r.Body.Close()

	var target T
	decoder := json.NewDecoder(http.MaxBytesReader(nil, r.Body, maxRequestBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&target); err != nil {
		return target, fmt.Errorf("decode request body: %w", err)
	}
	if decoder.More() {
		return target, errors.New("decode request body: unexpected content after the JSON value")
	}
	return target, nil
}
