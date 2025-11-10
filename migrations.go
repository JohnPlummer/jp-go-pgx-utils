package pgxutils

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// RunMigrations runs database migrations using golang-migrate
func RunMigrations(databaseURL, migrationsPath string) error {
	absPath, err := filepath.Abs(migrationsPath)
	if err != nil {
		return fmt.Errorf("failed to resolve migration path: %w", err)
	}

	if _, err := os.Stat(absPath); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("migration directory does not exist: %s", absPath)
		}
		return fmt.Errorf("failed to access migration directory: %w", err)
	}

	m, err := migrate.New(
		fmt.Sprintf("file://%s", absPath),
		databaseURL,
	)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	defer func() {
		if closeErr := closeMigrate(m); closeErr != nil {
			log.Printf("Warning during migrate cleanup: %v", closeErr)
		}
	}()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return nil
}

// RollbackMigrations rolls back the last migration
func RollbackMigrations(databaseURL, migrationsPath string) error {
	absPath, err := filepath.Abs(migrationsPath)
	if err != nil {
		return fmt.Errorf("failed to resolve migration path: %w", err)
	}

	m, err := migrate.New(
		fmt.Sprintf("file://%s", absPath),
		databaseURL,
	)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	defer func() {
		if closeErr := closeMigrate(m); closeErr != nil {
			log.Printf("Warning during migrate cleanup: %v", closeErr)
		}
	}()

	if err := m.Steps(-1); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to rollback migration: %w", err)
	}

	return nil
}

// closeMigrate safely closes the migrate instance
func closeMigrate(m *migrate.Migrate) error {
	sourceErr, dbErr := m.Close()
	if sourceErr != nil {
		return fmt.Errorf("error closing source: %w", sourceErr)
	}
	if dbErr != nil {
		return fmt.Errorf("error closing database: %w", dbErr)
	}
	return nil
}
