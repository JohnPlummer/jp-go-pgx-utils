package pgxutils

import (
	"context"
	"fmt"
	"time"
)

// HealthStatus represents the health status of the database
type HealthStatus struct {
	Healthy     bool          `json:"healthy"`
	Message     string        `json:"message"`
	Latency     time.Duration `json:"latency_ms"`
	Connections int32         `json:"connections"`
	IdleConns   int32         `json:"idle_connections"`
	MaxConns    int32         `json:"max_connections"`
	LastChecked time.Time     `json:"last_checked"`
}

// DetailedHealth performs a comprehensive health check and returns detailed status
func (db *Connection) DetailedHealth(ctx context.Context) *HealthStatus {
	status := &HealthStatus{
		LastChecked: time.Now(),
	}

	if db.pool == nil {
		status.Healthy = false
		status.Message = "database pool not initialized"
		return status
	}

	stats := db.pool.Stat()
	status.Connections = stats.AcquiredConns()
	status.IdleConns = stats.IdleConns()
	status.MaxConns = stats.MaxConns()

	start := time.Now()
	err := db.Health(ctx)
	status.Latency = time.Since(start)

	if err != nil {
		status.Healthy = false
		status.Message = fmt.Sprintf("health check failed: %v", err)
	} else {
		status.Healthy = true
		status.Message = "database is healthy"
	}

	return status
}

// WaitForReady waits for the database to become ready with a timeout
func (db *Connection) WaitForReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for database to be ready")
			}

			if err := db.Health(ctx); err == nil {
				return nil
			}
		}
	}
}

// IsHealthy is a simple boolean check for database health
func (db *Connection) IsHealthy(ctx context.Context) bool {
	return db.Health(ctx) == nil
}

// CheckConnections verifies that the connection pool is within healthy thresholds
func (db *Connection) CheckConnections() error {
	if db.pool == nil {
		return fmt.Errorf("database pool not initialized")
	}

	stats := db.pool.Stat()

	if stats.AcquiredConns() >= stats.MaxConns() {
		return fmt.Errorf("connection pool exhausted: %d/%d connections in use",
			stats.AcquiredConns(), stats.MaxConns())
	}

	idleRatio := float64(stats.IdleConns()) / float64(stats.MaxConns())
	if idleRatio > 0.9 {
		return fmt.Errorf("high idle connection ratio: %.2f%% idle", idleRatio*100)
	}

	return nil
}
