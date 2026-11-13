package mysql

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"time"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

const (
	migrationLockName          = "subscription_firewall_migrations"
	migrationLockTimeout       = 30 * time.Second
	migrationLockReleaseBudget = 5 * time.Second
)

func Migrate(ctx context.Context, db *sql.DB) error {
	if err := prepareGoose(); err != nil {
		return err
	}
	release, err := acquireMigrationLock(ctx, db)
	if err != nil {
		return err
	}
	defer release()
	if err := goose.UpContext(ctx, db, "migrations"); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

func acquireMigrationLock(ctx context.Context, db *sql.DB) (func(), error) {
	connection, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire migration lock connection: %w", err)
	}

	var acquired int
	if err := connection.QueryRowContext(ctx,
		`SELECT GET_LOCK(?, ?)`,
		migrationLockName, int(migrationLockTimeout/time.Second),
	).Scan(&acquired); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("acquire migration lock: %w", err)
	}
	if acquired != 1 {
		_ = connection.Close()
		return nil, fmt.Errorf("migration lock %q was not acquired within %s: another replica is migrating",
			migrationLockName, migrationLockTimeout)
	}

	return func() {
		releaseContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), migrationLockReleaseBudget)
		defer cancel()
		_, _ = connection.ExecContext(releaseContext, `SELECT RELEASE_LOCK(?)`, migrationLockName)
		_ = connection.Close()
	}, nil
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
