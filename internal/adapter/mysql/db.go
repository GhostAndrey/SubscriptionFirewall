package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"

	"subscriptionfirewall/internal/domain"
)

// rowScanner is the read surface shared by sql.Row and sql.Rows.
type rowScanner interface{ Scan(dest ...any) error }

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

func int64SliceToAny(values []int64) []any {
	result := make([]any, len(values))
	for i, value := range values {
		result[i] = value
	}
	return result
}

// verifyWrite reads a row back after an upsert. Both tables carry a unique key
// over (user_id, merchant_id), so a write that carries a fresh id for an
// existing pair is absorbed by the existing row; reporting that as a conflict
// keeps the caller's id from silently pointing at another record.
func verifyWrite(ctx context.Context, entity, id string, load func(context.Context) error) error {
	err := load(ctx)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrNotFound):
		return fmt.Errorf("%s %s was absorbed by an existing row with a different id: %w",
			entity, id, domain.ErrAlreadyExists)
	default:
		return fmt.Errorf("verify %s %s: %w", entity, id, err)
	}
}

func isDuplicateKey(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}
