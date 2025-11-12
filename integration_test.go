//go:build integration

package pgxutils

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/JohnPlummer/jp-go-config"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupTestContainer(t *testing.T) (*postgres.PostgresContainer, *config.DatabaseConfig) {
	ctx := context.Background()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	})

	host, err := postgresContainer.Host(ctx)
	require.NoError(t, err)

	port, err := postgresContainer.MappedPort(ctx, "5432")
	require.NoError(t, err)

	cfg := &config.DatabaseConfig{
		Host:     host,
		Port:     port.Int(),
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
		SSLMode:  "disable",
		MaxConns: 10,
		MinConns: 2,
	}

	return postgresContainer, cfg
}

func TestIntegration_Connection_ConnectAndHealth(t *testing.T) {
	_, cfg := setupTestContainer(t)

	conn, err := NewConnection(cfg)
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	err = conn.Connect(ctx)
	require.NoError(t, err)

	// Verify connection is established
	assert.NotNil(t, conn.Pool())

	// Test health check
	err = conn.Health(ctx)
	require.NoError(t, err)

	// Test stats
	stats := conn.Stats()
	assert.NotNil(t, stats)
}

func TestIntegration_Connection_QueryRow(t *testing.T) {
	_, cfg := setupTestContainer(t)

	conn, err := NewConnection(cfg)
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	err = conn.Connect(ctx)
	require.NoError(t, err)

	// Test QueryRow
	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 1, result)
}

func TestIntegration_Connection_Query(t *testing.T) {
	_, cfg := setupTestContainer(t)

	conn, err := NewConnection(cfg)
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	err = conn.Connect(ctx)
	require.NoError(t, err)

	// Create test table
	_, err = conn.Exec(ctx, `
		CREATE TABLE test_users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Exec(ctx, "INSERT INTO test_users (name) VALUES ($1), ($2)", "Alice", "Bob")
	require.NoError(t, err)

	// Test Query
	rows, err := conn.Query(ctx, "SELECT id, name FROM test_users ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	var users []struct {
		ID   int
		Name string
	}

	for rows.Next() {
		var user struct {
			ID   int
			Name string
		}
		err := rows.Scan(&user.ID, &user.Name)
		require.NoError(t, err)
		users = append(users, user)
	}

	require.NoError(t, rows.Err())
	assert.Len(t, users, 2)
	assert.Equal(t, "Alice", users[0].Name)
	assert.Equal(t, "Bob", users[1].Name)
}

func TestIntegration_Connection_Exec(t *testing.T) {
	_, cfg := setupTestContainer(t)

	conn, err := NewConnection(cfg)
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	err = conn.Connect(ctx)
	require.NoError(t, err)

	// Create test table
	tag, err := conn.Exec(ctx, `
		CREATE TABLE test_items (
			id SERIAL PRIMARY KEY,
			value TEXT NOT NULL
		)
	`)
	require.NoError(t, err)
	assert.Equal(t, "CREATE TABLE", tag.String())

	// Insert data
	tag, err = conn.Exec(ctx, "INSERT INTO test_items (value) VALUES ($1)", "test")
	require.NoError(t, err)
	assert.Equal(t, int64(1), tag.RowsAffected())
}

func TestIntegration_Connection_Transaction(t *testing.T) {
	_, cfg := setupTestContainer(t)

	conn, err := NewConnection(cfg)
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	err = conn.Connect(ctx)
	require.NoError(t, err)

	// Create test table
	_, err = conn.Exec(ctx, `
		CREATE TABLE test_accounts (
			id SERIAL PRIMARY KEY,
			balance INTEGER NOT NULL
		)
	`)
	require.NoError(t, err)

	// Insert initial data
	_, err = conn.Exec(ctx, "INSERT INTO test_accounts (balance) VALUES ($1), ($2)", 100, 200)
	require.NoError(t, err)

	// Test successful transaction
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, "UPDATE test_accounts SET balance = balance - 50 WHERE id = 1")
	require.NoError(t, err)

	_, err = tx.Exec(ctx, "UPDATE test_accounts SET balance = balance + 50 WHERE id = 2")
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Verify changes
	var balance int
	err = conn.QueryRow(ctx, "SELECT balance FROM test_accounts WHERE id = 1").Scan(&balance)
	require.NoError(t, err)
	assert.Equal(t, 50, balance)

	err = conn.QueryRow(ctx, "SELECT balance FROM test_accounts WHERE id = 2").Scan(&balance)
	require.NoError(t, err)
	assert.Equal(t, 250, balance)
}

func TestIntegration_Connection_TransactionRollback(t *testing.T) {
	_, cfg := setupTestContainer(t)

	conn, err := NewConnection(cfg)
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	err = conn.Connect(ctx)
	require.NoError(t, err)

	// Create test table
	_, err = conn.Exec(ctx, `
		CREATE TABLE test_logs (
			id SERIAL PRIMARY KEY,
			message TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	// Insert initial data
	_, err = conn.Exec(ctx, "INSERT INTO test_logs (message) VALUES ($1)", "initial")
	require.NoError(t, err)

	// Test transaction rollback
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, "INSERT INTO test_logs (message) VALUES ($1)", "should rollback")
	require.NoError(t, err)

	err = tx.Rollback(ctx)
	require.NoError(t, err)

	// Verify rollback
	var count int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM test_logs").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIntegration_HandleTransactionRollback(t *testing.T) {
	_, cfg := setupTestContainer(t)

	conn, err := NewConnection(cfg)
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	err = conn.Connect(ctx)
	require.NoError(t, err)

	// Create test table
	_, err = conn.Exec(ctx, `
		CREATE TABLE test_data (
			id SERIAL PRIMARY KEY,
			value TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	logger := slog.Default()

	// Test HandleTransactionRollback with actual transaction
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, "INSERT INTO test_data (value) VALUES ($1)", "test")
	require.NoError(t, err)

	// Rollback using HandleTransactionRollback
	HandleTransactionRollback(ctx, tx, logger)

	// Verify rollback
	var count int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM test_data").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestIntegration_WithTransaction_Success(t *testing.T) {
	_, cfg := setupTestContainer(t)

	conn, err := NewConnection(cfg)
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	err = conn.Connect(ctx)
	require.NoError(t, err)

	// Create test table
	_, err = conn.Exec(ctx, `
		CREATE TABLE test_orders (
			id SERIAL PRIMARY KEY,
			product TEXT NOT NULL,
			quantity INTEGER NOT NULL
		)
	`)
	require.NoError(t, err)

	logger := slog.Default()

	// Use WithTransaction
	err = WithTransaction(ctx, conn, logger, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO test_orders (product, quantity) VALUES ($1, $2)", "Widget", 5)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "INSERT INTO test_orders (product, quantity) VALUES ($1, $2)", "Gadget", 3)
		return err
	})
	require.NoError(t, err)

	// Verify data was committed
	var count int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM test_orders").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestIntegration_WithTransaction_Rollback(t *testing.T) {
	_, cfg := setupTestContainer(t)

	conn, err := NewConnection(cfg)
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	err = conn.Connect(ctx)
	require.NoError(t, err)

	// Create test table
	_, err = conn.Exec(ctx, `
		CREATE TABLE test_products (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL UNIQUE
		)
	`)
	require.NoError(t, err)

	logger := slog.Default()

	// Use WithTransaction with error
	err = WithTransaction(ctx, conn, logger, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO test_products (name) VALUES ($1)", "Product1")
		if err != nil {
			return err
		}
		// This will cause a unique constraint violation
		_, err = tx.Exec(ctx, "INSERT INTO test_products (name) VALUES ($1)", "Product1")
		return err
	})
	require.Error(t, err)

	// Verify rollback - no data should be committed
	var count int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM test_products").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestIntegration_Connection_RetryOnFailure(t *testing.T) {
	// Test with invalid host that will retry
	cfg := &config.DatabaseConfig{
		Host:     "invalid-host-that-does-not-exist",
		Port:     5432,
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
		SSLMode:  "disable",
		MaxConns: 10,
		MinConns: 2,
	}

	conn, err := NewConnection(cfg, WithRetryTimeout(5*time.Second))
	require.NoError(t, err)

	ctx := context.Background()
	startTime := time.Now()
	err = conn.Connect(ctx)
	duration := time.Since(startTime)

	require.Error(t, err)
	// Should retry for approximately the retry timeout (at least 3s with exponential backoff)
	assert.GreaterOrEqual(t, duration, 3*time.Second)
	assert.LessOrEqual(t, duration, 7*time.Second) // Allow some overhead
}

func TestIntegration_Connection_CustomHealthTimeout(t *testing.T) {
	_, cfg := setupTestContainer(t)

	conn, err := NewConnection(cfg, WithHealthTimeout(1*time.Second))
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	err = conn.Connect(ctx)
	require.NoError(t, err)

	// Health check should use custom timeout
	err = conn.Health(ctx)
	require.NoError(t, err)
}

func TestIntegration_Connection_BeginTx(t *testing.T) {
	_, cfg := setupTestContainer(t)

	conn, err := NewConnection(cfg)
	require.NoError(t, err)
	defer conn.Close()

	ctx := context.Background()
	err = conn.Connect(ctx)
	require.NoError(t, err)

	// Create test table
	_, err = conn.Exec(ctx, `
		CREATE TABLE test_txopts (
			id SERIAL PRIMARY KEY,
			value TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	// Test BeginTx with custom options
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	})
	require.NoError(t, err)

	_, err = tx.Exec(ctx, "INSERT INTO test_txopts (value) VALUES ($1)", "test")
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Verify data was committed
	var count int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM test_txopts").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}
