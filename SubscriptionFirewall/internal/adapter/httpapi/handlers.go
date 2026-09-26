package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	"subscriptionfirewall/internal/domain"
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
		return domain.Transaction{}, err
	}
	return domain.Transaction{
		ID:           domain.TransactionID(r.ID),
		UserID:       domain.UserID(r.UserID),
		MerchantID:   domain.MerchantID(r.MerchantID),
		MerchantName: r.MerchantName,
		MCC:          domain.MCC(r.MCC),
		AmountMinor:  r.AmountMinor,
		Currency:     r.Currency,
		CardPAN:      r.CardPAN,
		AuthorizedAt: authorizedAt,
	}, nil
}

type authorizeChargeRequest struct {
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
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
	if err := s.transactions.Save(r.Context(), transaction); err != nil {
		writeError(w, err)
		return
	}

	s.metrics.CountTransactionIngested()
	s.logger.LogAttrs(r.Context(), slog.LevelInfo, "transaction ingested",
		slog.String("transaction_id", string(transaction.ID)),
		slog.String("user_id", string(transaction.UserID)),
		slog.String("merchant_id", string(transaction.MerchantID)),
		slog.Int64("amount_minor", transaction.AmountMinor),
		slog.String("pan", masking.PAN(transaction.CardPAN)),
	)
	s.pipeline.Enqueue(transaction.UserID)
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

func (s *Server) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	userID := domain.UserID(r.PathValue("user_id"))
	subscriptions, err := s.subscriptions.ListByUser(r.Context(), userID)
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

func (s *Server) handleFreezeSubscription(w http.ResponseWriter, r *http.Request) {
	subscription, err := s.subscriptions.Freeze(r.Context(), domain.SubscriptionID(r.PathValue("id")))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toSubscriptionView(subscription))
}

func (s *Server) handleReactivateSubscription(w http.ResponseWriter, r *http.Request) {
	subscription, err := s.subscriptions.Reactivate(r.Context(), domain.SubscriptionID(r.PathValue("id")))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toSubscriptionView(subscription))
}

func (s *Server) handleTerminateSubscription(w http.ResponseWriter, r *http.Request) {
	subscription, err := s.subscriptions.Terminate(r.Context(), domain.SubscriptionID(r.PathValue("id")))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toSubscriptionView(subscription))
}

func (s *Server) handleListTokens(w http.ResponseWriter, r *http.Request) {
	userID := domain.UserID(r.PathValue("user_id"))
	tokens, err := s.tokens.ListByUser(r.Context(), userID)
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

func (s *Server) handleFreezeToken(w http.ResponseWriter, r *http.Request) {
	token, err := s.tokens.Freeze(r.Context(), domain.VirtualTokenID(r.PathValue("id")))
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
	token, err := s.tokens.Reactivate(r.Context(), domain.VirtualTokenID(r.PathValue("id")))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTokenView(token))
}

func (s *Server) handleTerminateToken(w http.ResponseWriter, r *http.Request) {
	token, err := s.tokens.Terminate(r.Context(), domain.VirtualTokenID(r.PathValue("id")))
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
	case isSpendLimitExceeded(err):
		return "declined_spend_limit"
	case isTokenNotActive(err):
		return "declined_token_state"
	default:
		return "declined_other"
	}
}
