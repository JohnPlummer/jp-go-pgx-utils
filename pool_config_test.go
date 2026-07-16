package pgxutils

import (
	"math"
	"runtime"
	"testing"
	"time"

	config "github.com/JohnPlummer/jp-go-config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pgxpool's own defaults, from pgxpool.ParseConfig. Mirrored here so the tests
// state what they expect rather than asserting "not zero".
const (
	pgxpoolDefaultMinConns        = int32(0)
	pgxpoolDefaultMaxConnLifetime = time.Hour
	pgxpoolDefaultMaxConnIdleTime = 30 * time.Minute
)

// pgxpoolDefaultMaxConns is 4, or NumCPU when that is higher.
func pgxpoolDefaultMaxConns() int32 {
	const base = int32(4)
	n := runtime.NumCPU()
	if n <= int(base) || n > math.MaxInt32 {
		return base
	}
	return int32(n) // #nosec G115 - bounds checked above
}

func baseConfig() *config.DatabaseConfig {
	return &config.DatabaseConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
		SSLMode:  "disable",
	}
}

// An unset pool field must leave pgxpool's default in place. Overwriting it with
// the zero value is what broke every caller that builds a DatabaseConfig by hand
// instead of through jp-go-config's viper loading (JP-188).
func TestBuildPoolConfigLeavesDefaultsForUnsetFields(t *testing.T) {
	conn, err := NewConnection(baseConfig())
	require.NoError(t, err)

	poolConfig, err := conn.buildPoolConfig()
	require.NoError(t, err)

	// The regression: a zero MaxConnLifetime expires every connection the moment
	// it is created. From pgx v5.10.0 pgxpool enforces expiry at acquire time, so
	// the pool destroys and retries until it gives up.
	assert.Equal(t, pgxpoolDefaultMaxConnLifetime, poolConfig.MaxConnLifetime,
		"unset ConnMaxLifetime must leave pgxpool's 1h default, not zero")

	// A zero MaxConns is rejected by puddle with "MaxSize must be >= 1".
	assert.Equal(t, pgxpoolDefaultMaxConns(), poolConfig.MaxConns,
		"unset MaxConns must leave pgxpool's default, not zero")

	assert.Equal(t, pgxpoolDefaultMinConns, poolConfig.MinConns)
	assert.Equal(t, pgxpoolDefaultMaxConnIdleTime, poolConfig.MaxConnIdleTime)
}

func TestBuildPoolConfigHonoursCallerSetFields(t *testing.T) {
	cfg := baseConfig()
	cfg.MaxConns = 10
	cfg.MinConns = 2
	cfg.ConnMaxLifetime = 5 * time.Minute
	cfg.ConnMaxIdleTime = 30 * time.Second

	conn, err := NewConnection(cfg)
	require.NoError(t, err)

	poolConfig, err := conn.buildPoolConfig()
	require.NoError(t, err)

	assert.Equal(t, int32(10), poolConfig.MaxConns)
	assert.Equal(t, int32(2), poolConfig.MinConns)
	assert.Equal(t, 5*time.Minute, poolConfig.MaxConnLifetime)
	assert.Equal(t, 30*time.Second, poolConfig.MaxConnIdleTime)
}

// Each field is independent: setting one must not cause the others to be
// overwritten with their zero values.
func TestBuildPoolConfigAppliesFieldsIndependently(t *testing.T) {
	cfg := baseConfig()
	cfg.ConnMaxLifetime = 5 * time.Minute

	conn, err := NewConnection(cfg)
	require.NoError(t, err)

	poolConfig, err := conn.buildPoolConfig()
	require.NoError(t, err)

	assert.Equal(t, 5*time.Minute, poolConfig.MaxConnLifetime)
	assert.Equal(t, pgxpoolDefaultMaxConns(), poolConfig.MaxConns,
		"setting ConnMaxLifetime must not zero MaxConns")
	assert.Equal(t, pgxpoolDefaultMaxConnIdleTime, poolConfig.MaxConnIdleTime,
		"setting ConnMaxLifetime must not zero MaxConnIdleTime")
}

func TestBuildPoolConfigSetsHealthCheckPeriod(t *testing.T) {
	conn, err := NewConnection(baseConfig())
	require.NoError(t, err)

	poolConfig, err := conn.buildPoolConfig()
	require.NoError(t, err)

	assert.Equal(t, 30*time.Second, poolConfig.HealthCheckPeriod)
}

func TestBuildPoolConfigRejectsOutOfRangeConns(t *testing.T) {
	t.Run("negative MaxConns", func(t *testing.T) {
		cfg := baseConfig()
		cfg.MaxConns = -1

		conn, err := NewConnection(cfg)
		require.NoError(t, err)

		_, err = conn.buildPoolConfig()
		require.Error(t, err)
	})

	t.Run("negative MinConns", func(t *testing.T) {
		cfg := baseConfig()
		cfg.MinConns = -1

		conn, err := NewConnection(cfg)
		require.NoError(t, err)

		_, err = conn.buildPoolConfig()
		require.Error(t, err)
	})
}
