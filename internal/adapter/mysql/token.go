package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"subscriptionfirewall/internal/domain"
	"subscriptionfirewall/internal/identifier"
	"subscriptionfirewall/internal/ports"
)

type VirtualTokenRepository struct {
	db *sql.DB
}

func NewVirtualTokenRepository(db *sql.DB) *VirtualTokenRepository {
	return &VirtualTokenRepository{db: db}
}

// Save inserts a new token or replaces an existing row addressed by id. It is
// intended for seeding and tests; concurrent use-case updates go through
// UpdateVersion.
func (r *VirtualTokenRepository) Save(ctx context.Context, token *domain.VirtualToken) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO virtual_tokens (`+tokenColumns+`)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
			state = VALUES(state), spent_in_period = VALUES(spent_in_period),
			monthly_limit = VALUES(monthly_limit), period_started = VALUES(period_started)`,
		tokenArgs(token)...,
	)
	if err != nil {
		return fmt.Errorf("save virtual token %s: %w", token.ID, err)
	}
	return verifyWrite(ctx, "virtual token", string(token.ID), func(ctx context.Context) error {
		_, err := r.GetByID(ctx, token.ID)
		return err
	})
}

// UpdateVersion persists the lifecycle fields of a token only when the row still
// carries the version the caller read. A charge that commits in between bumps
// the version too, so the transition is rejected instead of writing a stale
// spent_in_period back over it.
func (r *VirtualTokenRepository) UpdateVersion(ctx context.Context, token *domain.VirtualToken) error {
	version := token.CurrentVersion()
	current := token.Snapshot()
	result, err := r.db.ExecContext(ctx,
		`UPDATE virtual_tokens
		 SET masked_pan = ?, monthly_limit = ?, currency = ?, state = ?, period_started = ?, version = version + 1
		 WHERE id = ? AND version = ?`,
		current.MaskedPAN, current.MonthlyLimit, current.Currency,
		current.State, current.PeriodStarted.UTC(), token.ID, version,
	)
	if err != nil {
		return fmt.Errorf("update virtual token %s: %w", token.ID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read update result for virtual token %s: %w", token.ID, err)
	}
	if affected == 0 {
		return r.classifyUpdateMiss(ctx, token, version)
	}
	token.AdmitVersion(version + 1)
	return nil
}

func (r *VirtualTokenRepository) GetByID(ctx context.Context, id domain.VirtualTokenID) (*domain.VirtualToken, error) {
	row := r.db.QueryRowContext(ctx, tokenSelect+` WHERE id = ?`, id)
	token, err := scanToken(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("virtual token %s: %w", id, domain.ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get virtual token %s: %w", id, err)
	}
	return token, nil
}

func (r *VirtualTokenRepository) FindByUserAndMerchant(ctx context.Context, userID domain.UserID, merchantID domain.MerchantID) (*domain.VirtualToken, error) {
	row := r.db.QueryRowContext(ctx, tokenSelect+` WHERE user_id = ? AND merchant_id = ?`, userID, merchantID)
	token, err := scanToken(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("virtual token for user %s merchant %s: %w", userID, merchantID, domain.ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("find virtual token for user %s merchant %s: %w", userID, merchantID, err)
	}
	return token, nil
}

func (r *VirtualTokenRepository) ListByUserPage(ctx context.Context, userID domain.UserID, page ports.Page) ([]*domain.VirtualToken, error) {
	query := tokenSelect + ` WHERE user_id = ? ORDER BY id`
	args := []any{userID}
	if !page.IsUnbounded() {
		query += ` LIMIT ? OFFSET ?`
		args = append(args, page.Limit, page.Offset)
	}

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list virtual tokens for user %s: %w", userID, err)
	}
	defer rows.Close()

	tokens := make([]*domain.VirtualToken, 0)
	for rows.Next() {
		token, err := scanToken(rows)
		if err != nil {
			return nil, fmt.Errorf("scan virtual token: %w", err)
		}
		tokens = append(tokens, token)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate virtual tokens: %w", err)
	}
	return tokens, nil
}

// Charge reads the token under an exclusive row lock, applies the domain rule
// and persists the result inside one transaction. Concurrent charges from any
// replica queue on the same row, so the monthly limit cannot be exceeded by
// interleaved read-modify-write pairs.
func (r *VirtualTokenRepository) Charge(
	ctx context.Context,
	id domain.VirtualTokenID,
	amountMinor int64,
	currency string,
	now time.Time,
) (*domain.VirtualToken, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin charge transaction: %w", err)
	}
	defer tx.Rollback()

	token, err := scanToken(tx.QueryRowContext(ctx, tokenSelect+` WHERE id = ? FOR UPDATE`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("virtual token %s: %w", id, domain.ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("lock virtual token %s: %w", id, err)
	}

	if err := token.ApplyCharge(amountMinor, currency, now); err != nil {
		return nil, err
	}
	if err := persistToken(ctx, tx, token); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit charge transaction: %w", err)
	}
	token.AdmitVersion(token.CurrentVersion() + 1)
	return token, nil
}

func (r *VirtualTokenRepository) ReserveTokenIssuance(
	ctx context.Context,
	userID domain.UserID,
	merchantID domain.MerchantID,
	issuanceKey string,
	now time.Time,
	staleBefore time.Time,
) (ports.TokenReservation, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return ports.TokenReservation{}, fmt.Errorf("begin issuance reservation: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM virtual_tokens
		 WHERE user_id = ? AND merchant_id = ? AND state = ? AND period_started < ?`,
		userID, merchantID, domain.VirtualTokenIssuing, staleBefore.UTC(),
	); err != nil {
		return ports.TokenReservation{}, fmt.Errorf("clear stale issuance reservation: %w", err)
	}

	reservedToken := domain.NewIssuingVirtualToken(domain.VirtualTokenID(identifier.New("vtok")), userID, merchantID, now)
	result, err := tx.ExecContext(ctx,
		`INSERT INTO virtual_tokens (`+tokenColumns+`)
		 VALUES (?, ?, ?, '', 0, 0, '', ?, ?, ?, 0)
		 ON DUPLICATE KEY UPDATE id = id`,
		reservedToken.ID, userID, merchantID, domain.VirtualTokenIssuing, now.UTC(), issuanceKey,
	)
	if err != nil {
		return ports.TokenReservation{}, fmt.Errorf("reserve virtual token issuance: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return ports.TokenReservation{}, fmt.Errorf("read issuance reservation result: %w", err)
	}
	if affected == 1 {
		if err := tx.Commit(); err != nil {
			return ports.TokenReservation{}, fmt.Errorf("commit issuance reservation: %w", err)
		}
		return ports.TokenReservation{Reserved: true, TokenID: reservedToken.ID}, nil
	}

	existing, err := scanToken(tx.QueryRowContext(ctx,
		tokenSelect+` WHERE user_id = ? AND merchant_id = ? FOR UPDATE`, userID, merchantID))
	if errors.Is(err, sql.ErrNoRows) {
		return ports.TokenReservation{}, fmt.Errorf("virtual token for user %s merchant %s: %w", userID, merchantID, domain.ErrNotFound)
	}
	if err != nil {
		return ports.TokenReservation{}, fmt.Errorf("read reserved virtual token: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ports.TokenReservation{}, fmt.Errorf("commit issuance reservation: %w", err)
	}
	return ports.TokenReservation{Reserved: false, TokenID: existing.ID, Existing: existing}, nil
}

func (r *VirtualTokenRepository) CompleteTokenIssuance(
	ctx context.Context,
	tokenID domain.VirtualTokenID,
	issuanceKey string,
	card ports.IssuedCard,
	now time.Time,
) (*domain.VirtualToken, error) {
	token, err := r.GetByID(ctx, tokenID)
	if err != nil {
		return nil, err
	}
	if err := token.CompleteIssuance(card.MaskedPAN, card.MonthlyLimit, card.Currency, now); err != nil {
		return nil, err
	}
	result, err := r.db.ExecContext(ctx,
		`UPDATE virtual_tokens
		 SET masked_pan = ?, monthly_limit = ?, currency = ?, state = ?, period_started = ?, issuance_key = NULL
		 WHERE id = ? AND state = ? AND issuance_key = ?`,
		token.Snapshot().MaskedPAN, token.Snapshot().MonthlyLimit, token.Snapshot().Currency,
		token.Snapshot().State, now.UTC(), tokenID, domain.VirtualTokenIssuing, issuanceKey,
	)
	if err != nil {
		return nil, fmt.Errorf("complete virtual token issuance %s: %w", tokenID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("read issuance completion result for %s: %w", tokenID, err)
	}
	if affected == 0 {
		return nil, fmt.Errorf("virtual token %s no longer holds issuance %s: %w",
			tokenID, issuanceKey, ports.ErrIssuanceLost)
	}
	token.AdmitVersion(token.CurrentVersion() + 1)
	return token, nil
}

func (r *VirtualTokenRepository) ReleaseTokenIssuance(ctx context.Context, tokenID domain.VirtualTokenID) error {
	if _, err := r.db.ExecContext(ctx,
		`DELETE FROM virtual_tokens WHERE id = ? AND state = ?`,
		tokenID, domain.VirtualTokenIssuing,
	); err != nil {
		return fmt.Errorf("release virtual token issuance %s: %w", tokenID, err)
	}
	return nil
}

// classifyUpdateMiss separates a missing row from a genuine version conflict.
// Both produce zero affected rows, but only the second one is retryable, so
// collapsing them would make a deleted token surface as a conflict.
func (r *VirtualTokenRepository) classifyUpdateMiss(ctx context.Context, token *domain.VirtualToken, version uint32) error {
	var existing uint32
	err := r.db.QueryRowContext(ctx,
		`SELECT version FROM virtual_tokens WHERE id = ?`, token.ID).Scan(&existing)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("virtual token %s: %w", token.ID, domain.ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("check virtual token %s after missed update: %w", token.ID, err)
	}
	return fmt.Errorf("virtual token %s at version %d, stored version %d: %w",
		token.ID, version, existing, ports.ErrVersionConflict)
}

type tokenExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func persistToken(ctx context.Context, executor tokenExecutor, token *domain.VirtualToken) error {
	current := token.Snapshot()
	if _, err := executor.ExecContext(ctx,
		`UPDATE virtual_tokens
		 SET state = ?, spent_in_period = ?, monthly_limit = ?, period_started = ?, version = version + 1
		 WHERE id = ?`,
		current.State, current.SpentInPeriod, current.MonthlyLimit, current.PeriodStarted.UTC(), token.ID,
	); err != nil {
		return fmt.Errorf("persist virtual token %s: %w", token.ID, err)
	}
	return nil
}

const tokenColumns = `id, user_id, merchant_id, masked_pan, monthly_limit, spent_in_period,
	currency, state, period_started, issuance_key, version`

const tokenSelect = `SELECT id, user_id, merchant_id, masked_pan, monthly_limit, spent_in_period,
	currency, state, period_started, version FROM virtual_tokens`

func tokenArgs(token *domain.VirtualToken) []any {
	snapshot := token.Snapshot()
	issuanceKey := any(nil)
	if snapshot.State == domain.VirtualTokenIssuing {
		issuanceKey = issuanceKeyFor(snapshot.UserID, snapshot.MerchantID)
	}
	return []any{
		snapshot.ID, snapshot.UserID, snapshot.MerchantID, snapshot.MaskedPAN,
		snapshot.MonthlyLimit, snapshot.SpentInPeriod, snapshot.Currency,
		snapshot.State, snapshot.PeriodStarted.UTC(), issuanceKey, snapshot.Version,
	}
}

// issuanceKeyFor is derived from the user and merchant pair alone, so every
// replica and every retry addresses the same logical issuance.
func issuanceKeyFor(userID domain.UserID, merchantID domain.MerchantID) string {
	return "issue:" + string(userID) + ":" + string(merchantID)
}

func scanToken(row rowScanner) (*domain.VirtualToken, error) {
	token := &domain.VirtualToken{}
	if err := row.Scan(
		&token.ID, &token.UserID, &token.MerchantID, &token.MaskedPAN, &token.MonthlyLimit,
		&token.SpentInPeriod, &token.Currency, &token.State, &token.PeriodStarted, &token.Version,
	); err != nil {
		return nil, err
	}
	return token, nil
}

var _ ports.VirtualTokenRepository = (*VirtualTokenRepository)(nil)
