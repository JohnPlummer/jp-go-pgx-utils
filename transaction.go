package pgxutils

import (
	"context"
	"log/slog"
	"strings"

	"github.com/JohnPlummer/go-errors"
	"github.com/jackc/pgx/v5"
)

// TransactionRollback defines the minimal interface needed for transaction rollback.
// This allows for easier testing while maintaining compatibility with pgx.Tx.
type TransactionRollback interface {
	Rollback(ctx context.Context) error
}

// HandleTransactionRollback safely rolls back a transaction in deferred cleanup.
//
// Designed for use in defer blocks where err != nil. Logs but doesn't propagate
// rollback errors to prevent masking the original failure. The pgx driver
// automatically rolls back uncommitted transactions on connection close, making
// this a best-effort operation for explicit cleanup.
//
// Expected rollback failures (tx already closed, context canceled, or connection busy)
// are logged at WARN level to reduce noise. Unexpected failures are logged at ERROR level.
//
// Example usage:
//
//	tx, err := conn.Begin(ctx)
//	if err != nil {
//	    return err
//	}
//	defer func() {
//	    if err != nil {
//	        pgxutils.HandleTransactionRollback(ctx, tx, logger)
//	    }
//	}()
func HandleTransactionRollback(ctx context.Context, tx TransactionRollback, logger *slog.Logger) {
	if tx == nil {
		return
	}

	if rbErr := tx.Rollback(ctx); rbErr != nil {
		errMsg := rbErr.Error()
		// Expected conditions: log as warning (transaction already completed, context canceled, or connection busy)
		//
		// Note: String matching is used for pgx-specific errors ("tx is closed", "conn busy")
		// because pgx doesn't expose these as sentinel errors for errors.Is() comparison.
		if strings.Contains(errMsg, "tx is closed") ||
			errors.Is(rbErr, context.Canceled) ||
			strings.Contains(errMsg, "conn busy") {
			logger.Warn("transaction rollback skipped (expected condition)",
				"error", rbErr,
			)
		} else {
			// Unexpected rollback failure: log as error
			logger.Error("failed to rollback transaction",
				"error", rbErr,
			)
		}
	}
}

// RollbackTransaction is a convenience wrapper around HandleTransactionRollback
// that also wraps the original error with transaction context.
//
// This function is useful when you want to both roll back a transaction and
// preserve the original error with additional context.
//
// Example usage:
//
//	tx, err := conn.Begin(ctx)
//	if err != nil {
//	    return err
//	}
//	defer func() {
//	    if err != nil {
//	        err = pgxutils.RollbackTransaction(ctx, tx, logger, err)
//	    }
//	}()
func RollbackTransaction(ctx context.Context, tx TransactionRollback, logger *slog.Logger, originalErr error) error {
	HandleTransactionRollback(ctx, tx, logger)

	if originalErr == nil {
		return nil
	}

	// Check if the error is context-related
	if errors.Is(originalErr, context.Canceled) {
		return errors.NewProcessingError(
			"transaction failed due to context cancellation",
			"transaction_rollback",
			errors.WithCause(originalErr),
		)
	}

	if errors.Is(originalErr, context.DeadlineExceeded) {
		return errors.NewTimeoutError(
			"transaction failed due to context deadline",
			"transaction_rollback",
			0,
			errors.WithCause(originalErr),
		)
	}

	// For other errors, preserve them with transaction context
	return errors.NewProcessingError(
		"transaction failed",
		"transaction_rollback",
		errors.WithCause(originalErr),
	)
}

// WithTransaction executes a function within a database transaction.
// If the function returns an error, the transaction is rolled back.
// Otherwise, the transaction is committed.
//
// This helper simplifies transaction management by handling the
// Begin/Commit/Rollback boilerplate.
//
// Example usage:
//
//	err := pgxutils.WithTransaction(ctx, conn, logger, func(tx pgx.Tx) error {
//	    _, err := tx.Exec(ctx, "INSERT INTO users (name) VALUES ($1)", "Alice")
//	    if err != nil {
//	        return err
//	    }
//	    _, err = tx.Exec(ctx, "INSERT INTO logs (message) VALUES ($1)", "User created")
//	    return err
//	})
func WithTransaction(ctx context.Context, conn *Connection, logger *slog.Logger, fn func(pgx.Tx) error) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return errors.NewProcessingError(
			"failed to begin transaction",
			"with_transaction",
			errors.WithCause(err),
		)
	}

	var fnErr error
	defer func() {
		if fnErr != nil {
			HandleTransactionRollback(ctx, tx, logger)
		}
	}()

	fnErr = fn(tx)
	if fnErr != nil {
		return fnErr
	}

	if err := tx.Commit(ctx); err != nil {
		return errors.NewProcessingError(
			"failed to commit transaction",
			"with_transaction",
			errors.WithCause(err),
		)
	}

	return nil
}
