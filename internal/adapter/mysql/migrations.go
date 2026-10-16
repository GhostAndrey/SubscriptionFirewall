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

// Migrate applies versioned SQL migrations with goose; repeating it on every
// start is a no-op.
func Migrate(ctx context.Context, db *sql.DB) error {
	if err := prepareGoose(); err != nil {
		return err
	}
	if err := goose.UpContext(ctx, db, "migrations"); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

func Rollback(ctx context.Context, db *sql.DB, target int64) error {
	if err := prepareGoose(); err != nil {
		return err
	}
	if err := goose.DownToContext(ctx, db, "migrations", target); err != nil {
		return fmt.Errorf("roll back to migration %d: %w", target, err)
	}
	return nil
}

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
