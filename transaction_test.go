package pgxutils

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockTransaction implements TransactionRollback for testing.
type mockTransaction struct {
	rollbackErr    error
	rollbackCalled bool
}

func (m *mockTransaction) Rollback(ctx context.Context) error {
	m.rollbackCalled = true
	return m.rollbackErr
}

func TestHandleTransactionRollback_Success(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	tx := &mockTransaction{}
	HandleTransactionRollback(context.Background(), tx, logger)

	assert.True(t, tx.rollbackCalled)
	assert.Empty(t, buf.String()) // No error logged
}

func TestHandleTransactionRollback_NilTransaction(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	HandleTransactionRollback(context.Background(), nil, logger)

	assert.Empty(t, buf.String()) // No error logged
}

func TestHandleTransactionRollback_TxClosed(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	tx := &mockTransaction{
		rollbackErr: errors.New("tx is closed"),
	}
	HandleTransactionRollback(context.Background(), tx, logger)

	assert.True(t, tx.rollbackCalled)
	assert.Contains(t, buf.String(), "transaction rollback skipped")
	assert.Contains(t, buf.String(), "WARN")
}

func TestHandleTransactionRollback_ContextCanceled(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	tx := &mockTransaction{
		rollbackErr: context.Canceled,
	}
	HandleTransactionRollback(context.Background(), tx, logger)

	assert.True(t, tx.rollbackCalled)
	assert.Contains(t, buf.String(), "transaction rollback skipped")
	assert.Contains(t, buf.String(), "WARN")
}

func TestHandleTransactionRollback_ConnBusy(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	tx := &mockTransaction{
		rollbackErr: errors.New("conn busy"),
	}
	HandleTransactionRollback(context.Background(), tx, logger)

	assert.True(t, tx.rollbackCalled)
	assert.Contains(t, buf.String(), "transaction rollback skipped")
	assert.Contains(t, buf.String(), "WARN")
}

func TestHandleTransactionRollback_UnexpectedError(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	tx := &mockTransaction{
		rollbackErr: errors.New("unexpected error"),
	}
	HandleTransactionRollback(context.Background(), tx, logger)

	assert.True(t, tx.rollbackCalled)
	assert.Contains(t, buf.String(), "failed to rollback transaction")
	assert.Contains(t, buf.String(), "ERROR")
}

func TestRollbackTransaction_Success(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	tx := &mockTransaction{}
	originalErr := errors.New("original error")

	err := RollbackTransaction(context.Background(), tx, logger, originalErr)

	assert.True(t, tx.rollbackCalled)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transaction failed")
}

func TestRollbackTransaction_NilError(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	tx := &mockTransaction{}

	err := RollbackTransaction(context.Background(), tx, logger, nil)

	assert.True(t, tx.rollbackCalled)
	assert.NoError(t, err)
}

func TestRollbackTransaction_ContextCanceled(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	tx := &mockTransaction{}

	err := RollbackTransaction(context.Background(), tx, logger, context.Canceled)

	assert.True(t, tx.rollbackCalled)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context cancellation")
}

func TestRollbackTransaction_ContextDeadlineExceeded(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	tx := &mockTransaction{}

	err := RollbackTransaction(context.Background(), tx, logger, context.DeadlineExceeded)

	assert.True(t, tx.rollbackCalled)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline")
}
