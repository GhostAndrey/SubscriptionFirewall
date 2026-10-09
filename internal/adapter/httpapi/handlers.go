package httpapi

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/ports"
	"subscriptionfirewall/pkg/masking"
)

type ingestTransactionRequest struct {
	ID           string `json:"id"`
	UserID       string `json:"user_id"`
	MerchantID   string `json:"merchant_id"`
	MerchantName string `json:"merchant_name"`
	MCC          uint16 `json:"mcc"`
	AmountMinor  int64  `json:"amount_minor"`
	Currency     string `json:"currency"`
	CardPAN      string `json:"card_pan"`
	AuthorizedAt string `json:"authorized_at"`
}

func (r ingestTransactionRequest) toDomain() (domain.Transaction, error) {
	authorizedAt, err := time.Parse(time.RFC3339, r.AuthorizedAt)
	if err != nil {
		return domain.Transaction{}, fmt.Errorf("authorized_at must be RFC3339: %w", err)
	}
	return domain.Transaction{
		ID:           domain.TransactionID(r.ID),
		UserID:       domain.UserID(r.UserID),
		MerchantID:   domain.MerchantID(r.MerchantID),
		MerchantName: r.MerchantName,
		MCC:          domain.MCC(r.MCC),
		AmountMinor:  r.AmountMinor,
		Currency:     r.Currency,
		AuthorizedAt: authorizedAt,
	}, nil
}

type authorizeChargeRequest struct {
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
}

func (r authorizeChargeRequest) validate() error {
	if r.AmountMinor <= 0 {
		return errors.New("amount_minor must be positive")
	}
	if !domain.IsISOCurrency(r.Currency) {
		return errors.New("currency must be a 3-letter ISO code")
	}
	return nil
}

const (
	defaultPageSize = 50
	maxPageSize     = 200
)

func pageParams(r *http.Request) (limit, offset int, err error) {
	limit, offset = defaultPageSize, 0
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > maxPageSize {
			return 0, 0, errors.New("limit must be between 1 and 200")
		}
	}
	if raw := r.URL.Query().Get("offset"); raw != "" {
		offset, err = strconv.Atoi(raw)
		if err != nil || offset < 0 {
			return 0, 0, errors.New("offset must be non-negative")
		}
	}
	return limit, offset, nil
}

func (s *Server) handleIngestTransaction(w http.ResponseWriter, r *http.Request) {
	request, err := decodeJSON[ingestTransactionRequest](r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	transaction, err := request.toDomain()
	if err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	if err := transaction.Validate(s.clock.Now()); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	if _, err := s.transactions.GetByID(r.Context(), transaction.ID); err == nil {
		writeError(w, fmt.Errorf("transaction %s: %w", transaction.ID, domain.ErrAlreadyExists))
		return
	} else if !errors.Is(err, domain.ErrNotFound) {
		writeError(w, err)
		return
	}

	if err := s.outbox.EnqueueWithTransaction(r.Context(), transaction); err != nil {
		writeError(w, err)
		return
	}

	s.metrics.CountTransactionIngested()
	loggerFromContext(r.Context()).LogAttrs(r.Context(), slog.LevelInfo, "transaction ingested",
		slog.String("transaction_id", string(transaction.ID)),
		slog.String("user_id", string(transaction.UserID)),
		slog.String("merchant_id", string(transaction.MerchantID)),
		slog.Int64("amount_minor", transaction.AmountMinor),

		slog.String("pan", masking.PAN(request.CardPAN)),
	)
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

func (s *Server) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := pageParams(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	userID := domain.UserID(r.PathValue("user_id"))
	subscriptions, err := s.subscriptions.ListByUser(r.Context(), userID, ports.Page{Limit: limit, Offset: offset})
	if err != nil {
		writeError(w, err)
		return
	}
	views := make([]subscriptionView, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		views = append(views, toSubscriptionView(subscription))
	}
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) handleGetSubscription(w http.ResponseWriter, r *http.Request) {
	subscription, err := s.subscriptions.Get(r.Context(), domain.SubscriptionID(r.PathValue("id")))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toSubscriptionView(subscription))
}

func (s *Server) handleFreezeSubscription(w http.ResponseWriter, r *http.Request) {
	subscription, err := s.lifecycle.FreezeSubscription(r.Context(), actorFromContext(r.Context()), domain.SubscriptionID(r.PathValue("id")))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toSubscriptionView(subscription))
}

func (s *Server) handleReactivateSubscription(w http.ResponseWriter, r *http.Request) {
	subscription, err := s.lifecycle.ReactivateSubscription(r.Context(), actorFromContext(r.Context()), domain.SubscriptionID(r.PathValue("id")))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toSubscriptionView(subscription))
}

func (s *Server) handleTerminateSubscription(w http.ResponseWriter, r *http.Request) {
	subscription, err := s.lifecycle.TerminateSubscription(r.Context(), actorFromContext(r.Context()), domain.SubscriptionID(r.PathValue("id")))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toSubscriptionView(subscription))
}

func (s *Server) handleListTokens(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := pageParams(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	userID := domain.UserID(r.PathValue("user_id"))
	tokens, err := s.tokens.ListByUser(r.Context(), userID, ports.Page{Limit: limit, Offset: offset})
	if err != nil {
		writeError(w, err)
		return
	}
	views := make([]tokenView, 0, len(tokens))
	for _, token := range tokens {
		views = append(views, toTokenView(token))
	}
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) handleGetToken(w http.ResponseWriter, r *http.Request) {
	token, err := s.tokens.Get(r.Context(), domain.VirtualTokenID(r.PathValue("id")))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTokenView(token))
}

func (s *Server) handleFreezeToken(w http.ResponseWriter, r *http.Request) {
	token, err := s.lifecycle.FreezeToken(r.Context(), actorFromContext(r.Context()), domain.VirtualTokenID(r.PathValue("id")))
	if err != nil {
		writeError(w, err)
		return
	}
	s.metrics.CountTokenFrozen()
	s.logger.Info("virtual token frozen",
		"token", masking.Token(string(token.ID)),
		"merchant_id", string(token.MerchantID),
	)
	writeJSON(w, http.StatusOK, toTokenView(token))
}

func (s *Server) handleReactivateToken(w http.ResponseWriter, r *http.Request) {
	token, err := s.lifecycle.ReactivateToken(r.Context(), actorFromContext(r.Context()), domain.VirtualTokenID(r.PathValue("id")))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTokenView(token))
}

func (s *Server) handleTerminateToken(w http.ResponseWriter, r *http.Request) {
	token, err := s.lifecycle.TerminateToken(r.Context(), actorFromContext(r.Context()), domain.VirtualTokenID(r.PathValue("id")))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTokenView(token))
}

func (s *Server) handleAuthorizeCharge(w http.ResponseWriter, r *http.Request) {
	request, err := decodeJSON[authorizeChargeRequest](r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	if err := request.validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	tokenID := domain.VirtualTokenID(r.PathValue("id"))
	_, err = s.tokens.Authorize(r.Context(), tokenID, request.AmountMinor, request.Currency)
	if err != nil {
		s.metrics.CountTokenAuthorization(authorizationResult(err))
		writeError(w, err)
		return
	}
	s.metrics.CountTokenAuthorization("approved")
	writeJSON(w, http.StatusOK, map[string]string{"status": "approved"})
}

func authorizationResult(err error) string {
	switch {
	case errors.Is(err, domain.ErrSpendLimitExceeded):
		return "declined_spend_limit"
	case errors.Is(err, domain.ErrTokenNotActive):
		return "declined_token_state"
	case errors.Is(err, domain.ErrInvalidAmount):
		return "declined_invalid_amount"
	case errors.Is(err, domain.ErrUnknownCurrency):
		return "declined_currency"
	default:
		return "declined_other"
	}
}
