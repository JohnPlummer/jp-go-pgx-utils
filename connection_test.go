package pgxutils

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/JohnPlummer/go-config"
	"github.com/JohnPlummer/go-errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConnection(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := &config.DatabaseConfig{
			Host:     "localhost",
			Port:     5432,
			Database: "testdb",
			User:     "testuser",
			Password: "testpass",
			SSLMode:  "disable",
		}

		conn, err := NewConnection(cfg)
		require.NoError(t, err)
		assert.NotNil(t, conn)
		assert.Equal(t, cfg, conn.cfg)
		assert.NotNil(t, conn.logger)
	})

	t.Run("nil config", func(t *testing.T) {
		conn, err := NewConnection(nil)
		require.Error(t, err)
		assert.Nil(t, conn)
		assert.True(t, errors.IsValidation(err))
	})

	t.Run("with custom health timeout", func(t *testing.T) {
		cfg := &config.DatabaseConfig{
			Host:     "localhost",
			Port:     5432,
			Database: "testdb",
			User:     "testuser",
			Password: "testpass",
			SSLMode:  "disable",
		}

		conn, err := NewConnection(cfg, WithHealthTimeout(10*time.Second))
		require.NoError(t, err)
		assert.Equal(t, 10*time.Second, conn.opts.healthTimeout)
	})

	t.Run("with custom retry timeout", func(t *testing.T) {
		cfg := &config.DatabaseConfig{
			Host:     "localhost",
			Port:     5432,
			Database: "testdb",
			User:     "testuser",
			Password: "testpass",
			SSLMode:  "disable",
		}

		conn, err := NewConnection(cfg, WithRetryTimeout(60*time.Second))
		require.NoError(t, err)
		assert.Equal(t, 60*time.Second, conn.opts.retryTimeout)
	})

	t.Run("with logger", func(t *testing.T) {
		cfg := &config.DatabaseConfig{
			Host:     "localhost",
			Port:     5432,
			Database: "testdb",
			User:     "testuser",
			Password: "testpass",
			SSLMode:  "disable",
		}

		logger := slog.Default()
		conn, err := NewConnection(cfg, WithLogger(logger))
		require.NoError(t, err)
		assert.NotNil(t, conn.logger)
	})
}

func TestConnection_HealthBeforeConnect(t *testing.T) {
	cfg := &config.DatabaseConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
		SSLMode:  "disable",
	}

	conn, err := NewConnection(cfg)
	require.NoError(t, err)

	ctx := context.Background()
	err = conn.Health(ctx)
	require.Error(t, err)
}

func TestConnection_ExecBeforeConnect(t *testing.T) {
	cfg := &config.DatabaseConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
		SSLMode:  "disable",
	}

	conn, err := NewConnection(cfg)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = conn.Exec(ctx, "SELECT 1")
	require.Error(t, err)
}

func TestConnection_QueryBeforeConnect(t *testing.T) {
	cfg := &config.DatabaseConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
		SSLMode:  "disable",
	}

	conn, err := NewConnection(cfg)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = conn.Query(ctx, "SELECT 1")
	require.Error(t, err)
}

func TestConnection_QueryRowBeforeConnect(t *testing.T) {
	cfg := &config.DatabaseConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
		SSLMode:  "disable",
	}

	conn, err := NewConnection(cfg)
	require.NoError(t, err)

	ctx := context.Background()
	row := conn.QueryRow(ctx, "SELECT 1")
	var result int
	err = row.Scan(&result)
	require.Error(t, err)
}

func TestConnection_BeginBeforeConnect(t *testing.T) {
	cfg := &config.DatabaseConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
		SSLMode:  "disable",
	}

	conn, err := NewConnection(cfg)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = conn.Begin(ctx)
	require.Error(t, err)
}

func TestConnection_StatsBeforeConnect(t *testing.T) {
	cfg := &config.DatabaseConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
		SSLMode:  "disable",
	}

	conn, err := NewConnection(cfg)
	require.NoError(t, err)

	stats := conn.Stats()
	assert.Nil(t, stats)
}

func TestConnection_PoolBeforeConnect(t *testing.T) {
	cfg := &config.DatabaseConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
		SSLMode:  "disable",
	}

	conn, err := NewConnection(cfg)
	require.NoError(t, err)

	pool := conn.Pool()
	assert.Nil(t, pool)
}

func TestConnection_Close(t *testing.T) {
	cfg := &config.DatabaseConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
		SSLMode:  "disable",
	}

	conn, err := NewConnection(cfg)
	require.NoError(t, err)

	// Should not panic when pool is nil
	conn.Close()
}

func TestConnection_ConnectInvalidURL(t *testing.T) {
	cfg := &config.DatabaseConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "testdb",
		User:     "test\x00user", // Invalid character
		Password: "testpass",
		SSLMode:  "disable",
	}

	conn, err := NewConnection(cfg)
	require.NoError(t, err)

	ctx := context.Background()
	err = conn.Connect(ctx)
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
}

func TestConnection_ConnectMaxConnsOutOfRange(t *testing.T) {
	tests := []struct {
		name     string
		maxConns int
	}{
		{
			name:     "negative max conns",
			maxConns: -1,
		},
		{
			name:     "max conns too large",
			maxConns: int(^uint32(0)), // Larger than int32 max
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				User:     "testuser",
				Password: "testpass",
				SSLMode:  "disable",
				MaxConns: tt.maxConns,
			}

			conn, err := NewConnection(cfg)
			require.NoError(t, err)

			ctx := context.Background()
			err = conn.Connect(ctx)
			require.Error(t, err)
			assert.True(t, errors.IsValidation(err))
		})
	}
}

func TestConnection_ConnectMinConnsOutOfRange(t *testing.T) {
	tests := []struct {
		name     string
		minConns int
	}{
		{
			name:     "negative min conns",
			minConns: -1,
		},
		{
			name:     "min conns too large",
			minConns: int(^uint32(0)), // Larger than int32 max
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				User:     "testuser",
				Password: "testpass",
				SSLMode:  "disable",
				MinConns: tt.minConns,
			}

			conn, err := NewConnection(cfg)
			require.NoError(t, err)

			ctx := context.Background()
			err = conn.Connect(ctx)
			require.Error(t, err)
			assert.True(t, errors.IsValidation(err))
		})
	}
}

func TestEmptyRow_Scan(t *testing.T) {
	testErr := errors.NewProcessingError("test error", "test")
	row := &emptyRow{err: testErr}

	var result int
	err := row.Scan(&result)
	require.Error(t, err)
	assert.Equal(t, testErr, err)
}

func TestConnectionOptions_CustomTimeouts(t *testing.T) {
	cfg := &config.DatabaseConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
		SSLMode:  "disable",
	}

	healthTimeout := 15 * time.Second
	retryTimeout := 45 * time.Second

	conn, err := NewConnection(cfg,
		WithHealthTimeout(healthTimeout),
		WithRetryTimeout(retryTimeout),
	)
	require.NoError(t, err)

	assert.Equal(t, healthTimeout, conn.opts.healthTimeout)
	assert.Equal(t, retryTimeout, conn.opts.retryTimeout)
}

func TestConnectionOptions_MultipleOptions(t *testing.T) {
	cfg := &config.DatabaseConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
		SSLMode:  "disable",
	}

	logger := slog.Default()
	healthTimeout := 15 * time.Second

	conn, err := NewConnection(cfg,
		WithLogger(logger),
		WithHealthTimeout(healthTimeout),
	)
	require.NoError(t, err)

	assert.Equal(t, healthTimeout, conn.opts.healthTimeout)
	assert.NotNil(t, conn.logger)
}
