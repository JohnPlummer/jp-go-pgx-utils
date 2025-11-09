# go-pgx-utils

Enterprise-grade PostgreSQL connection management for Go applications using pgx/v5.

## Purpose

`go-pgx-utils` provides robust PostgreSQL connection management with automatic retry logic, health checking, and transaction utilities. Built on top of [pgx/v5](https://github.com/jackc/pgx), this package integrates seamlessly with [go-config](https://github.com/JohnPlummer/go-config) for configuration and [go-errors](https://github.com/JohnPlummer/go-errors) for structured error handling.

## Features

- **Automatic Connection Retry**: Exponential backoff retry logic with configurable timeout
- **Health Checking**: Built-in health check with configurable timeout
- **Connection Pooling**: Full support for pgxpool configuration
- **Transaction Management**: Utilities for safe transaction handling and rollback
- **Error Handling**: Integration with go-errors for structured error types
- **Testcontainers Support**: Integration tests using testcontainers-go
- **Thread-Safe**: Safe for concurrent use after initialization

## Installation

```bash
go get github.com/JohnPlummer/go-pgx-utils@v1.0.0
```

## Dependencies

This package requires:

- [github.com/JohnPlummer/go-config](https://github.com/JohnPlummer/go-config) v1.0.0 - Configuration management
- [github.com/JohnPlummer/go-errors](https://github.com/JohnPlummer/go-errors) v1.0.0 - Error handling
- [github.com/jackc/pgx/v5](https://github.com/jackc/pgx) - PostgreSQL driver

## Quick Start

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/JohnPlummer/go-config"
    pgxutils "github.com/JohnPlummer/go-pgx-utils"
)

func main() {
    // Create database configuration
    cfg := &config.DatabaseConfig{
        Host:            "localhost",
        Port:            5432,
        Database:        "mydb",
        User:            "myuser",
        Password:        "mypassword",
        SSLMode:         "disable",
        MaxConns:        25,
        MinConns:        5,
        ConnMaxLifetime: time.Hour,
        ConnMaxIdleTime: 30 * time.Minute,
    }

    // Create connection
    conn, err := pgxutils.NewConnection(cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    // Connect with automatic retry
    ctx := context.Background()
    if err := conn.Connect(ctx); err != nil {
        log.Fatal(err)
    }

    // Verify connection health
    if err := conn.Health(ctx); err != nil {
        log.Fatal(err)
    }

    // Execute queries
    _, err = conn.Exec(ctx, "SELECT 1")
    if err != nil {
        log.Fatal(err)
    }
}
```

## Configuration

The package uses `config.DatabaseConfig` from go-config:

```go
type DatabaseConfig struct {
    Host         string
    Port         int
    Name         string
    User         string
    Password     string
    SSLMode      string
    MaxConns     int
    MinConns     int
    MaxConnLife  time.Duration
    MaxConnIdle  time.Duration
    RetryTimeout time.Duration
}
```

### Environment Variables

Configure via environment variables (using go-config):

```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mydb
DB_USER=myuser
DB_PASSWORD=mypassword
DB_SSLMODE=disable
DB_MAX_CONNS=25
DB_MIN_CONNS=5
DB_MAX_CONN_LIFE=1h
DB_MAX_CONN_IDLE=30m
DB_RETRY_TIMEOUT=30s
```

Or use a `.env` file or other configuration sources supported by go-config.

## Connection Options

Customize connection behavior with functional options:

```go
conn, err := pgxutils.NewConnection(cfg.Database,
    pgxutils.WithLogger(logger),              // Custom logger
    pgxutils.WithHealthTimeout(10*time.Second), // Health check timeout
    pgxutils.WithRetryTimeout(60*time.Second),  // Connection retry timeout
)
```

### Available Options

- **WithLogger**: Set a custom `*slog.Logger` for connection logs
- **WithHealthTimeout**: Override default health check timeout (default: 5s)
- **WithRetryTimeout**: Override default connection retry timeout (default: 30s)

## Retry Logic

Connection retry uses exponential backoff:

- Starts with 1-second delay
- Increases exponentially up to 10-second maximum
- Retries until `RetryTimeout` is reached (default: 30s)
- Configurable via `config.DatabaseConfig.RetryTimeout` or `WithRetryTimeout` option

Example:

```go
cfg.Database.RetryTimeout = 60 * time.Second // Wait up to 60s
conn, err := pgxutils.NewConnection(cfg.Database)
// OR
conn, err := pgxutils.NewConnection(cfg.Database,
    pgxutils.WithRetryTimeout(60*time.Second),
)
```

## Health Checking

Health checks verify database connectivity:

```go
if err := conn.Health(ctx); err != nil {
    // Connection is unhealthy
    log.Printf("Database health check failed: %v", err)
}
```

The health check:

- Executes `SELECT 1` query
- Uses configurable timeout (default: 5s)
- Returns `ErrUnavailable` from go-errors on failure
- Can be customized with `WithHealthTimeout` option

## Transaction Management

### Manual Transaction Handling

```go
tx, err := conn.Begin(ctx)
if err != nil {
    return err
}

var txErr error
defer func() {
    if txErr != nil {
        pgxutils.HandleTransactionRollback(ctx, tx, logger)
    }
}()

_, txErr = tx.Exec(ctx, "INSERT INTO users (name) VALUES ($1)", "Alice")
if txErr != nil {
    return txErr
}

if err := tx.Commit(ctx); err != nil {
    txErr = err
    return err
}
```

### Using WithTransaction Helper

The `WithTransaction` helper automatically handles Begin/Commit/Rollback:

```go
err := pgxutils.WithTransaction(ctx, conn, logger, func(tx pgx.Tx) error {
    _, err := tx.Exec(ctx, "INSERT INTO users (name) VALUES ($1)", "Alice")
    if err != nil {
        return err
    }

    _, err = tx.Exec(ctx, "INSERT INTO audit_log (action) VALUES ($1)", "user_created")
    return err
})

if err != nil {
    // Transaction was automatically rolled back
    log.Printf("Transaction failed: %v", err)
}
```

### Transaction Utilities

**HandleTransactionRollback**: Safe rollback in defer blocks

```go
defer func() {
    if err != nil {
        pgxutils.HandleTransactionRollback(ctx, tx, logger)
    }
}()
```

**RollbackTransaction**: Rollback with error wrapping

```go
defer func() {
    if err != nil {
        err = pgxutils.RollbackTransaction(ctx, tx, logger, err)
    }
}()
```

**WithTransaction**: Complete transaction management

```go
err := pgxutils.WithTransaction(ctx, conn, logger, func(tx pgx.Tx) error {
    // Your transaction logic here
    return nil
})
```

## Error Handling

All errors use go-errors for structured error handling:

```go
import "github.com/JohnPlummer/go-errors"

err := conn.Connect(ctx)
if err != nil {
    switch {
    case errors.Is(err, errors.ErrTimeout):
        // Connection timed out after retry attempts
    case errors.Is(err, errors.ErrInvalidInput):
        // Invalid configuration
    case errors.Is(err, errors.ErrPreconditionFailed):
        // Pool not initialized
    case errors.Is(err, errors.ErrUnavailable):
        // Database unavailable
    case errors.Is(err, errors.ErrInternal):
        // Internal database error
    }
}
```

### Error Types

- `ErrInvalidInput`: Invalid configuration or parameters
- `ErrTimeout`: Connection or operation timeout
- `ErrPreconditionFailed`: Operation called before initialization
- `ErrUnavailable`: Database unavailable or unhealthy
- `ErrInternal`: Database query or transaction error
- `ErrCanceled`: Context canceled during transaction
- `ErrDeadlineExceeded`: Context deadline exceeded during transaction

## Query Methods

The `Connection` type provides the standard pgx query methods:

```go
// Execute a query without returning rows
tag, err := conn.Exec(ctx, "INSERT INTO users (name) VALUES ($1)", "Alice")

// Query multiple rows
rows, err := conn.Query(ctx, "SELECT id, name FROM users")
defer rows.Close()

// Query a single row
var name string
err := conn.QueryRow(ctx, "SELECT name FROM users WHERE id = $1", 1).Scan(&name)

// Begin a transaction
tx, err := conn.Begin(ctx)

// Begin a transaction with options
tx, err := conn.BeginTx(ctx, pgx.TxOptions{
    IsoLevel: pgx.ReadCommitted,
})
```

## Connection Pool Statistics

Monitor pool health with built-in statistics:

```go
stats := conn.Stats()
if stats != nil {
    log.Printf("Pool stats: acquired=%d idle=%d max=%d total=%d",
        stats.AcquiredConns(),
        stats.IdleConns(),
        stats.MaxConns(),
        stats.TotalConns(),
    )
}
```

## Migration Guide

### From Monorepo Pattern

If migrating from the monorepo pattern, update your imports:

**Before:**

```go
import "github.com/YourOrg/your-app/pkg/database"

db, err := database.NewDB(cfg)
```

**After:**

```go
import (
    "github.com/JohnPlummer/go-config"
    pgxutils "github.com/JohnPlummer/go-pgx-utils"
)

conn, err := pgxutils.NewConnection(cfg.Database)
```

### Key Changes

1. **Package name**: `database` → `pgxutils`
2. **Type name**: `DB` → `Connection`
3. **Constructor**: `NewDB(cfg)` → `NewConnection(cfg.Database)`
4. **Config**: Custom config → `config.DatabaseConfig` from go-config
5. **Errors**: Standard errors → go-errors integration

## Testing

### Unit Tests

```bash
go test -v -race -cover ./...
```

### Integration Tests

Integration tests use testcontainers and require Docker:

```bash
go test -v -race -tags=integration ./...
```

### Coverage

```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

This package maintains >80% test coverage.

## Best Practices

1. **Use go-config for configuration**: Load `DatabaseConfig` via go-config
2. **Check errors with go-errors**: Use `errors.Is()` for error type checking
3. **Use WithTransaction for complex transactions**: Automatic rollback on error
4. **Set appropriate connection pool sizes**: Match your workload characteristics
5. **Monitor pool statistics**: Use `Stats()` for observability
6. **Use health checks**: Integrate health checks in readiness probes
7. **Configure retry timeout**: Balance startup time vs. reliability
8. **Log transaction rollbacks**: Use provided utilities for consistent logging

## Thread Safety

The `Connection` type is thread-safe after `Connect()` succeeds. Multiple goroutines can safely:

- Execute queries
- Begin transactions
- Check health
- Access pool statistics

The underlying pgxpool handles concurrency automatically.

## Performance Considerations

- Connection pooling reduces overhead of connection establishment
- Health check period runs every 30s to detect stale connections
- Retry logic adds startup latency in failure scenarios
- Pool size should match expected concurrent query load
- Use transactions for multi-statement operations

## License

MIT License - See [LICENSE](./LICENSE) for details.

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Support

For issues and questions:

- GitHub Issues: <https://github.com/JohnPlummer/go-pgx-utils/issues>
- Documentation: This README and inline code documentation

## Related Packages

- [go-config](https://github.com/JohnPlummer/go-config) - Configuration management
- [go-errors](https://github.com/JohnPlummer/go-errors) - Structured error handling
- [pgx](https://github.com/jackc/pgx) - PostgreSQL driver and toolkit
