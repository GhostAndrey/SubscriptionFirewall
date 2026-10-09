package mysql

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate applies versioned SQL migrations (internal/adapter/mysql/migrations)
// with goose. Application is transactional per migration and recorded in the
// goose_db_version table; repeating it on every start is a no-op.
func Migrate(ctx context.Context, db *sql.DB) error {
	if err := prepareGoose(); err != nil {
		return err
	}
	if err := goose.UpContext(ctx, db, "migrations"); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

// Rollback reverts migrations down to (but not including) target, so a bad
// release can be undone without reaching for the goose binary on the host.
func Rollback(ctx context.Context, db *sql.DB, target int64) error {
	if err := prepareGoose(); err != nil {
		return err
	}
	if err := goose.DownToContext(ctx, db, "migrations", target); err != nil {
		return fmt.Errorf("roll back to migration %d: %w", target, err)
	}
	return nil
}

// MigrationVersion reports the highest applied migration.
func MigrationVersion(ctx context.Context, db *sql.DB) (int64, error) {
	if err := prepareGoose(); err != nil {
		return 0, err
	}
	version, err := goose.GetDBVersionContext(ctx, db)
	if err != nil {
		return 0, fmt.Errorf("read migration version: %w", err)
	}
	return version, nil
}

func prepareGoose() error {
	goose.SetBaseFS(migrationsFS)
	if err := goose.SetDialect("mysql"); err != nil {
		return fmt.Errorf("set goose dialect: %w", err)
	}
	goose.SetLogger(goose.NopLogger())
	return nil
}
