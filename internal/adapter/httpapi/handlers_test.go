package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"subscriptionfirewall/internal/adapter/memory"
	"subscriptionfirewall/internal/domain"
)

type handlerTestFixture struct {
	transactionRepository *memory.TransactionRepository
	subscriptionRepo      *memory.SubscriptionRepository
	tokenRepository       *memory.VirtualTokenRepository
	audit                 *memory.AuditLog
	server                *httptest.Server
}

func newHandlerTestFixture(t *testing.T) *handlerTestFixture {
	t.Helper()

	fixture := newServerFixture()
	apiServer, err := fixture.buildServer(t, Config{APIKeys: []string{"test-key"}})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	testServer := httptest.NewServer(apiServer.http.Handler)
	t.Cleanup(testServer.Close)

	return &handlerTestFixture{
		transactionRepository: fixture.transactions,
		subscriptionRepo:      fixture.subscriptions,
		tokenRepository:       fixture.tokens,
		audit:                 fixture.audit,
		server:                testServer,
	}
}

func (f *handlerTestFixture) do(t *testing.T, method, path string, payload any) *http.Response {
	t.Helper()

	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, f.server.URL+path, body)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	request.Header.Set("X-API-Key", "test-key")
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	t.Cleanup(func() { response.Body.Close() })
	return response
}

func validIngestRequest(id string) map[string]any {
	return map[string]any{
		"id":            id,
		"user_id":       "user-1",
		"merchant_id":   "netflix",
		"merchant_name": "Netflix",
		"mcc":           5968,
		"amount_minor":  1500,
		"currency":      "USD",
		"card_pan":      "4111111111111234",
		"authorized_at": time.Now().Format(time.RFC3339),
	}
}

func TestIngestTransactionIdempotencyAndValidation(t *testing.T) {
	fixture := newHandlerTestFixture(t)

	response := fixture.do(t, http.MethodPost, "/v1/transactions", validIngestRequest("tx-1"))
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 for valid ingest, got %d", response.StatusCode)
	}

	response = fixture.do(t, http.MethodPost, "/v1/transactions", validIngestRequest("tx-1"))
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 for duplicate transaction, got %d", response.StatusCode)
	}
}

func TestIngestTransactionRejectsInvalidPayloads(t *testing.T) {
	fixture := newHandlerTestFixture(t)

	tests := map[string]func(map[string]any){
		"negative amount":    func(request map[string]any) { request["amount_minor"] = -100 },
		"zero amount":        func(request map[string]any) { request["amount_minor"] = 0 },
		"empty user id":      func(request map[string]any) { request["user_id"] = "" },
		"empty id":           func(request map[string]any) { request["id"] = "" },
		"bad currency":       func(request map[string]any) { request["currency"] = "usd" },
		"currency length":    func(request map[string]any) { request["currency"] = "USDT" },
		"bad authorizedAt":   func(request map[string]any) { request["authorized_at"] = "yesterday" },
		"empty authorizedAt": func(request map[string]any) { request["authorized_at"] = "" },
		"oversized id":       func(request map[string]any) { request["id"] = strings.Repeat("i", domain.MaxTransactionIDLength+1) },
		"oversized user id":  func(request map[string]any) { request["user_id"] = strings.Repeat("u", domain.MaxUserIDLength+1) },
		"oversized merchant id": func(request map[string]any) {
			request["merchant_id"] = strings.Repeat("m", domain.MaxMerchantIDLength+1)
		},
		"oversized merchant": func(request map[string]any) {
			request["merchant_name"] = strings.Repeat("n", domain.MaxMerchantNameLength+1)
		},
		"future authorizedAt": func(request map[string]any) {
			request["authorized_at"] = time.Now().Add(48 * time.Hour).Format(time.RFC3339)
		},
		"ancient authorizedAt": func(request map[string]any) {
			request["authorized_at"] = time.Now().Add(-3 * 365 * 24 * time.Hour).Format(time.RFC3339)
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			request := validIngestRequest("tx-" + name)
			mutate(request)
			response := fixture.do(t, http.MethodPost, "/v1/transactions", request)
			if response.StatusCode != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", response.StatusCode)
			}
		})
	}
}

func TestIngestTransactionAcceptsBoundaryValues(t *testing.T) {
	fixture := newHandlerTestFixture(t)

	boundary := validIngestRequest("tx-boundary")
	boundary["id"] = strings.Repeat("i", domain.MaxTransactionIDLength)
	boundary["merchant_name"] = strings.Repeat("n", domain.MaxMerchantNameLength)
	boundary["authorized_at"] = time.Now().Add(-domain.MaxTransactionAge).Add(time.Minute).Format(time.RFC3339)

	if response := fixture.do(t, http.MethodPost, "/v1/transactions", boundary); response.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 at validation boundary, got %d", response.StatusCode)
	}
}

func TestListSubscriptionsPagination(t *testing.T) {
	fixture := newHandlerTestFixture(t)
	now := time.Now()
	for _, id := range []domain.SubscriptionID{"sub-1", "sub-2", "sub-3"} {
		subscription, err := domain.NewSubscription(id, "user-1", "netflix", "Netflix", domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", now, 3)
		if err != nil {
			t.Fatalf("create subscription: %v", err)
		}
		if err := fixture.subscriptionRepo.Save(t.Context(), subscription); err != nil {
			t.Fatalf("save subscription: %v", err)
		}
	}

	var firstPage []subscriptionView
	decode(t, fixture.do(t, http.MethodGet, "/v1/users/user-1/subscriptions?limit=2", nil), &firstPage)
	if len(firstPage) != 2 {
		t.Fatalf("expected 2 items on first page, got %d", len(firstPage))
	}

	var secondPage []subscriptionView
	decode(t, fixture.do(t, http.MethodGet, "/v1/users/user-1/subscriptions?limit=2&offset=2", nil), &secondPage)
	if len(secondPage) != 1 {
		t.Fatalf("expected 1 item on second page, got %d", len(secondPage))
	}

	if response := fixture.do(t, http.MethodGet, "/v1/users/user-1/subscriptions?limit=0", nil); response.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for limit=0, got %d", response.StatusCode)
	}
}

func TestGetSubscriptionByID(t *testing.T) {
	fixture := newHandlerTestFixture(t)
	subscription, err := domain.NewSubscription("sub-1", "user-1", "netflix", "Netflix", domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", time.Now(), 3)
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	if err := fixture.subscriptionRepo.Save(t.Context(), subscription); err != nil {
		t.Fatalf("save subscription: %v", err)
	}

	var view subscriptionView
	decode(t, fixture.do(t, http.MethodGet, "/v1/subscriptions/sub-1", nil), &view)
	if view.ID != "sub-1" {
		t.Errorf("expected sub-1, got %s", view.ID)
	}

	if response := fixture.do(t, http.MethodGet, "/v1/subscriptions/sub-missing", nil); response.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", response.StatusCode)
	}
}

func TestFreezeSubscriptionAlsoFreezesLinkedToken(t *testing.T) {
	fixture := newHandlerTestFixture(t)
	virtualToken, err := domain.NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", 10_000, "USD", time.Now())
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	if err := fixture.tokenRepository.Save(t.Context(), virtualToken); err != nil {
		t.Fatalf("save token: %v", err)
	}
	subscription, err := domain.NewSubscription("sub-1", "user-1", "netflix", "Netflix", domain.SubscriptionActive, domain.WindowMonthly, 1500, "USD", time.Now(), 3)
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	subscription.VirtualTokenID = "vtok-1"
	if err := fixture.subscriptionRepo.Save(t.Context(), subscription); err != nil {
		t.Fatalf("save subscription: %v", err)
	}

	response := fixture.do(t, http.MethodPost, "/v1/subscriptions/sub-1/freeze", nil)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on freeze, got %d", response.StatusCode)
	}

	persisted, err := fixture.tokenRepository.GetByID(t.Context(), "vtok-1")
	if err != nil {
		t.Fatalf("load token: %v", err)
	}
	if persisted.State != domain.VirtualTokenFrozen {
		t.Errorf("expected linked token to be Frozen, got %s", persisted.State)
	}

	entries := fixture.audit.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected audit entries for both entities, got %+v", entries)
	}
	for _, entry := range entries {
		if entry.Action != "freeze" || entry.Actor == "" {
			t.Errorf("unexpected audit entry: %+v", entry)
		}
	}
	if entries[0].EntityType != "subscription" || entries[0].EntityID != "sub-1" {
		t.Errorf("expected the subscription audit first, got %+v", entries[0])
	}
	if entries[1].EntityType != "token" || entries[1].EntityID != "vtok-1" {
		t.Errorf("expected the linked token audit second, got %+v", entries[1])
	}
}

func TestAuthorizeChargeRejectsInvalidPayloads(t *testing.T) {
	fixture := newHandlerTestFixture(t)
	virtualToken, err := domain.NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", 10_000, "USD", time.Now())
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	if err := fixture.tokenRepository.Save(t.Context(), virtualToken); err != nil {
		t.Fatalf("save token: %v", err)
	}

	tests := map[string]map[string]any{
		"negative amount": {"amount_minor": -1, "currency": "USD"},
		"zero amount":     {"amount_minor": 0, "currency": "USD"},
		"bad currency":    {"amount_minor": 100, "currency": "usd"},
		"empty currency":  {"amount_minor": 100, "currency": ""},
	}

	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			response := fixture.do(t, http.MethodPost, "/v1/tokens/vtok-1/authorize", payload)
			if response.StatusCode != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", response.StatusCode)
			}
			persisted, err := fixture.tokenRepository.GetByID(t.Context(), "vtok-1")
			if err != nil {
				t.Fatalf("load token: %v", err)
			}
			if persisted.SpentInPeriod != 0 {
				t.Fatalf("expected spending unchanged, got %d", persisted.SpentInPeriod)
			}
		})
	}
}

func TestAuthorizeChargeSpendsBudget(t *testing.T) {
	fixture := newHandlerTestFixture(t)
	virtualToken, err := domain.NewVirtualToken("vtok-1", "user-1", "netflix", "411111******1234", 10_000, "USD", time.Now())
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	if err := fixture.tokenRepository.Save(t.Context(), virtualToken); err != nil {
		t.Fatalf("save token: %v", err)
	}

	response := fixture.do(t, http.MethodPost, "/v1/tokens/vtok-1/authorize", map[string]any{"amount_minor": 9_000, "currency": "USD"})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", response.StatusCode)
	}

	response = fixture.do(t, http.MethodPost, "/v1/tokens/vtok-1/authorize", map[string]any{"amount_minor": 2_000, "currency": "USD"})
	if response.StatusCode != http.StatusPaymentRequired {
		t.Fatalf("expected 402 over limit, got %d", response.StatusCode)
	}
}

func decode(t *testing.T, response *http.Response, target any) {
	t.Helper()
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}
