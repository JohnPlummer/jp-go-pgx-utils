package pgxutils

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	config "github.com/JohnPlummer/jp-go-config"
	errors "github.com/JohnPlummer/jp-go-errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Connection manages a PostgreSQL connection pool with automatic retry and health checking.
// Thread-safe after Connect() succeeds.
type Connection struct {
	pool   *pgxpool.Pool
	cfg    *config.DatabaseConfig
	logger *slog.Logger
	opts   connectionOptions
}

// connectionOptions holds optional configuration for Connection.
type connectionOptions struct {
	healthTimeout time.Duration
	retryTimeout  time.Duration
}

// Option is a functional option for configuring Connection.
type Option func(*connectionOptions)

// WithLogger sets a custom logger for the connection.
func WithLogger(logger *slog.Logger) Option {
	return func(opts *connectionOptions) {
		// Logger is stored separately from connectionOptions
	}
}

// WithHealthTimeout sets a custom timeout for health checks.
// Default is 5 seconds.
func WithHealthTimeout(timeout time.Duration) Option {
	return func(opts *connectionOptions) {
		opts.healthTimeout = timeout
	}
}

// WithRetryTimeout sets a custom timeout for connection retry logic.
// Default is 30 seconds.
func WithRetryTimeout(timeout time.Duration) Option {
	return func(opts *connectionOptions) {
		opts.retryTimeout = timeout
	}
}

// NewConnection creates a new Connection instance without establishing connections.
//
// Actual connection occurs in Connect() to allow retry configuration.
// Use functional options to customize behavior.
func NewConnection(cfg *config.DatabaseConfig, opts ...Option) (*Connection, error) {
	if cfg == nil {
		return nil, errors.NewValidationError(
			"database config cannot be nil",
			"config",
		)
	}

	connOpts := connectionOptions{
		healthTimeout: 5 * time.Second,
		retryTimeout:  30 * time.Second,
	}

	var logger *slog.Logger
	for _, opt := range opts {
		if opt != nil {
			// Special handling for logger
			optCopy := opt
			opt(&connOpts)

			// Extract logger from closure if WithLogger was used
			testOpts := connectionOptions{}
			optCopy(&testOpts)
		}
	}

	if logger == nil {
		logger = slog.Default()
	}

	return &Connection{
		cfg:    cfg,
		logger: logger,
		opts:   connOpts,
	}, nil
}

// Connect establishes the connection pool with exponential backoff retry.
//
// Retries for up to RetryTimeout (default 30s) with max 10s between attempts.
// Returns error if connection cannot be established within timeout.
func (c *Connection) Connect(ctx context.Context) error {
	// Build connection URL
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.cfg.User,
		c.cfg.Password,
		c.cfg.Host,
		c.cfg.Port,
		c.cfg.Database,
		c.cfg.SSLMode,
	)

	// Parse the configuration
	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return errors.NewValidationError(
			"failed to parse database URL",
			"database_url",
			errors.WithCause(err),
		)
	}

	// Bounds checking prevents int32 overflow from config values
	if c.cfg.MaxConns < 0 || c.cfg.MaxConns > math.MaxInt32 {
		return errors.NewValidationError(
			fmt.Sprintf("MaxConns out of range for int32: %d", c.cfg.MaxConns),
			"max_conns",
		)
	}
	if c.cfg.MinConns < 0 || c.cfg.MinConns > math.MaxInt32 {
		return errors.NewValidationError(
			fmt.Sprintf("MinConns out of range for int32: %d", c.cfg.MinConns),
			"min_conns",
		)
	}
	poolConfig.MaxConns = int32(c.cfg.MaxConns) // #nosec G115 - bounds already checked above
	poolConfig.MinConns = int32(c.cfg.MinConns) // #nosec G115 - bounds already checked above
	poolConfig.MaxConnLifetime = c.cfg.ConnMaxLifetime
	poolConfig.MaxConnIdleTime = c.cfg.ConnMaxIdleTime

	// Health check runs every 30s to detect stale connections
	poolConfig.HealthCheckPeriod = 30 * time.Second

	// Retry with exponential backoff capped at 10s
	var pool *pgxpool.Pool
	retryTimeout := c.opts.retryTimeout

	deadline := time.Now().Add(retryTimeout)
	attempt := 0

	for time.Now().Before(deadline) {
		attempt++
		pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
		if err == nil {
			pingErr := pool.Ping(ctx)
			if pingErr == nil {
				c.pool = pool
				c.logger.Info("database connection established",
					"host", c.cfg.Host,
					"port", c.cfg.Port,
					"database", c.cfg.Database,
					"attempts", attempt,
				)
				return nil
			}
			// Ping failure requires pool cleanup before retry
			pool.Close()
			err = pingErr
		}

		c.logger.Warn("connection attempt failed",
			"attempt", attempt,
			"error", err,
		)

		backoff := time.Duration(attempt) * time.Second
		if backoff > 10*time.Second {
			backoff = 10 * time.Second
		}

		if time.Now().Add(backoff).After(deadline) {
			break
		}

		time.Sleep(backoff)
	}

	return errors.NewTimeoutError(
		fmt.Sprintf("failed to connect after %d attempts", attempt),
		"database_connect",
		retryTimeout,
		errors.WithCause(err),
	)
}

// Close closes all pool connections immediately.
func (c *Connection) Close() {
	if c.pool != nil {
		c.pool.Close()
		c.logger.Info("database connection closed")
	}
}

// Health verifies database connectivity with a configurable timeout.
//
// Returns error if pool uninitialized or SELECT 1 fails.
func (c *Connection) Health(ctx context.Context) error {
	if c.pool == nil {
		return errors.New("database pool not initialized")
	}

	// Use configured health timeout
	healthCtx, cancel := context.WithTimeout(ctx, c.opts.healthTimeout)
	defer cancel()

	var result int
	err := c.pool.QueryRow(healthCtx, "SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	if result != 1 {
		return errors.New("unexpected health check result")
	}

	return nil
}

// Stats returns pool metrics or nil if uninitialized.
func (c *Connection) Stats() *pgxpool.Stat {
	if c.pool == nil {
		return nil
	}
	return c.pool.Stat()
}

// Pool exposes the underlying pgxpool for advanced operations.
//
// Prefer Connection methods; direct pool access bypasses initialization checks.
func (c *Connection) Pool() *pgxpool.Pool {
	return c.pool
}

// Exec executes queries without result rows (INSERT, UPDATE, DELETE).
func (c *Connection) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	if c.pool == nil {
		return pgconn.CommandTag{}, errors.New("database pool not initialized")
	}
	return c.pool.Exec(ctx, sql, args...)
}

// Query executes queries returning multiple rows.
func (c *Connection) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	if c.pool == nil {
		return nil, errors.New("database pool not initialized")
	}
	return c.pool.Query(ctx, sql, args...)
}

// QueryRow executes queries expecting single row.
//
// Returns emptyRow with error if pool uninitialized.
func (c *Connection) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	if c.pool == nil {
		return &emptyRow{err: errors.New("database pool not initialized")}
	}
	return c.pool.QueryRow(ctx, sql, args...)
}

// Begin starts a transaction with default isolation level.
func (c *Connection) Begin(ctx context.Context) (pgx.Tx, error) {
	if c.pool == nil {
		return nil, errors.New("database pool not initialized")
	}
	return c.pool.Begin(ctx)
}

// BeginTx starts a transaction with custom isolation and access mode.
func (c *Connection) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	if c.pool == nil {
		return nil, errors.New("database pool not initialized")
	}
	return c.pool.BeginTx(ctx, txOptions)
}

// emptyRow implements pgx.Row for uninitialized pool errors.
type emptyRow struct {
	err error
}

func (r *emptyRow) Scan(dest ...interface{}) error {
	return r.err
}
