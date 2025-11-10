package pgxutils

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PoolMetrics provides detailed metrics about the connection pool
type PoolMetrics struct {
	TotalConns           int32         `json:"total_connections"`
	AcquiredConns        int32         `json:"acquired_connections"`
	IdleConns            int32         `json:"idle_connections"`
	MaxConns             int32         `json:"max_connections"`
	TotalAcquireCount    int64         `json:"total_acquire_count"`
	TotalAcquireTime     time.Duration `json:"total_acquire_time"`
	EmptyAcquireCount    int64         `json:"empty_acquire_count"`
	CanceledAcquireCount int64         `json:"canceled_acquire_count"`
}

// GetMetrics returns current pool metrics
func (db *Connection) GetMetrics() *PoolMetrics {
	if db.pool == nil {
		return &PoolMetrics{}
	}

	stats := db.pool.Stat()
	return &PoolMetrics{
		TotalConns:           stats.TotalConns(),
		AcquiredConns:        stats.AcquiredConns(),
		IdleConns:            stats.IdleConns(),
		MaxConns:             stats.MaxConns(),
		TotalAcquireCount:    stats.AcquireCount(),
		TotalAcquireTime:     stats.AcquireDuration(),
		EmptyAcquireCount:    stats.EmptyAcquireCount(),
		CanceledAcquireCount: stats.CanceledAcquireCount(),
	}
}

// ConnectionWrapper wraps a connection with additional functionality
type ConnectionWrapper struct {
	conn   *pgxpool.Conn
	db     *Connection
	closed bool
	mu     sync.Mutex
}

// Acquire gets a connection from the pool with context
func (db *Connection) Acquire(ctx context.Context) (*ConnectionWrapper, error) {
	if db.pool == nil {
		return nil, fmt.Errorf("database pool not initialized")
	}

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	return &ConnectionWrapper{
		conn: conn,
		db:   db,
	}, nil
}

// Release returns the connection to the pool
func (cw *ConnectionWrapper) Release() {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	if !cw.closed && cw.conn != nil {
		cw.conn.Release()
		cw.closed = true
	}
}

// Conn returns the underlying connection
func (cw *ConnectionWrapper) Conn() *pgxpool.Conn {
	return cw.conn
}

// WithConnection executes a function with a database connection
func (db *Connection) WithConnection(ctx context.Context, fn func(*pgxpool.Conn) error) error {
	conn, err := db.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	return fn(conn.Conn())
}

// WithTransaction executes a function within a database transaction
func (db *Connection) WithTransaction(ctx context.Context, fn func(pgx.Tx) error) error {
	if db.pool == nil {
		return fmt.Errorf("database pool not initialized")
	}

	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			// Rollback on panic
			_ = tx.Rollback(ctx)
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("failed to rollback transaction: %v (original error: %w)", rbErr, err)
		}
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// CopyFrom performs a bulk insert using PostgreSQL COPY protocol
func (db *Connection) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	if db.pool == nil {
		return 0, fmt.Errorf("database pool not initialized")
	}

	return db.pool.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

// SendBatch sends a batch of queries to be executed
func (db *Connection) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	if db.pool == nil {
		return &errorBatchResults{err: fmt.Errorf("database pool not initialized")}
	}

	return db.pool.SendBatch(ctx, batch)
}

// errorBatchResults implements pgx.BatchResults for error cases
type errorBatchResults struct {
	err error
}

func (e *errorBatchResults) Exec() (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, e.err
}

func (e *errorBatchResults) Query() (pgx.Rows, error) {
	return nil, e.err
}

func (e *errorBatchResults) QueryRow() pgx.Row {
	return &emptyRow{err: e.err}
}

func (e *errorBatchResults) Close() error {
	return e.err
}

// ResetPool closes and recreates the connection pool
// This can be useful for handling certain types of connection errors
func (db *Connection) ResetPool(ctx context.Context) error {
	db.Close()
	return db.Connect(ctx)
}

// AverageAcquireTime returns the average time to acquire a connection
func (db *Connection) AverageAcquireTime() time.Duration {
	if db.pool == nil {
		return 0
	}

	stats := db.pool.Stat()
	if stats.AcquireCount() == 0 {
		return 0
	}

	return time.Duration(int64(stats.AcquireDuration()) / stats.AcquireCount())
}
