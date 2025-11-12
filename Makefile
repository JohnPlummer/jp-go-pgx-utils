# go-pgx-utils Makefile
# PostgreSQL utilities library for pgx/v5

# Detect Go binary path and add to PATH for this make session
GOBIN := $(shell go env GOPATH)/bin
export PATH := $(GOBIN):$(PATH)

# PHONY targets - all targets that don't produce files
.PHONY: help check check-ci fmt lint test test-unit test-integration test-coverage test-race deps tools clean security

# Default target - show help
help:
	@echo "go-pgx-utils Makefile Commands:"
	@echo ""
	@echo "Core Commands:"
	@echo "  make check              - Run all verification checks (fmt, lint, test)"
	@echo "  make check-ci           - Run CI checks with coverage"
	@echo "  make fmt                - Format code with gofumpt"
	@echo "  make lint               - Run golangci-lint"
	@echo "  make security           - Run security scan with gosec"
	@echo ""
	@echo "Testing:"
	@echo "  make test               - Run all tests (unit + integration)"
	@echo "  make test-unit          - Run unit tests only"
	@echo "  make test-integration   - Run integration tests only (requires Docker)"
	@echo "  make test-coverage      - Generate coverage report"
	@echo "  make test-race          - Run tests with race detection"
	@echo ""
	@echo "Dependencies:"
	@echo "  make deps               - Download and tidy dependencies"
	@echo "  make tools              - Install required tools"
	@echo ""
	@echo "Other:"
	@echo "  make clean              - Clean build artifacts"

#----------------------------------------------
# Primary check command - runs all verification
#----------------------------------------------
check: deps
	@echo ""
	@echo "========================================"
	@echo "go-pgx-utils Checks - Step 1/4: Formatting"
	@echo "========================================"
	@$(MAKE) fmt
	@echo ""
	@echo "========================================"
	@echo "go-pgx-utils Checks - Step 2/4: Linting"
	@echo "========================================"
	@$(MAKE) lint
	@echo ""
	@echo "========================================"
	@echo "go-pgx-utils Checks - Step 3/4: Tests"
	@echo "========================================"
	@$(MAKE) test
	@echo ""
	@echo "========================================"
	@echo "go-pgx-utils Checks - Step 4/4: Race Detection"
	@echo "========================================"
	@$(MAKE) test-race
	@echo ""
	@echo "✅ All checks passed!"

# CI check command - includes coverage for Codecov
check-ci: deps
	@echo ""
	@echo "========================================"
	@echo "CI Checks - Step 1/3: Linting"
	@echo "========================================"
	@$(MAKE) lint
	@echo ""
	@echo "========================================"
	@echo "CI Checks - Step 2/3: Tests with Coverage"
	@echo "========================================"
	@$(MAKE) test-coverage
	@echo ""
	@echo "========================================"
	@echo "CI Checks - Step 3/3: Race Detection"
	@echo "========================================"
	@$(MAKE) test-race
	@echo ""
	@echo "✅ All CI checks passed with coverage!"

#----------------------------------------------
# Core Commands
#----------------------------------------------

# Format code with gofumpt
fmt:
	@echo "Formatting code with gofumpt..."
	@$(GOBIN)/gofumpt -l -w .

# Run linter (checks formatting with gofumpt and runs security scanning with gosec)
lint:
	@echo "Running linter (checks formatting and security)..."
	@$(GOBIN)/golangci-lint run --timeout=5m ./...

# Run all tests (unit + integration)
test:
	@echo "Running all tests (unit → integration)..."
	@echo ""
	@echo "  ─────────────────────────"
	@echo "  Unit Tests"
	@echo "  ─────────────────────────"
	@$(MAKE) test-unit || exit 1
	@echo ""
	@echo "  ─────────────────────────"
	@echo "  Integration Tests"
	@echo "  ─────────────────────────"
	@$(MAKE) test-integration || exit 1
	@echo ""
	@echo "All tests completed"

# Run unit tests only (excludes integration tests)
test-unit:
	@echo "Running unit tests..."
	@go test -v -timeout=3m $(shell go list ./... | grep -v integration_test)
	@echo "✓ Unit tests passed"

# Run integration tests only
test-integration:
	@echo "Running integration tests..."
	@go test -v -tags=integration -timeout=5m ./...
	@echo "✓ Integration tests passed"

# Run tests with race detection
test-race:
	@echo "Running tests with race detection..."
	@go test -race -timeout=10m $(shell go list ./... | grep -v integration_test)

# Run tests with coverage (for CI/CD and Codecov)
test-coverage:
	@echo "Running tests with coverage..."
	# Run unit tests with coverage
	@go test -v -timeout=3m -coverprofile=coverage.out -covermode=atomic $(shell go list ./... | grep -v integration_test)
	# Run integration tests separately (without coverage to avoid compilation issues)
	@echo "Running integration tests..."
	@go test -v -tags=integration -timeout=5m ./...
	@go tool cover -func=coverage.out | grep total | awk '{print "Total coverage: " $$3}'

# Run security scan
security:
	@echo "Running security scan..."
	@$(GOBIN)/gosec -terse -fmt text ./...

#----------------------------------------------
# Dependencies and Tools
#----------------------------------------------

# Download and tidy dependencies
deps:
	@echo "Downloading dependencies..."
	@go mod download
	@echo "Tidying dependencies..."
	@go mod tidy
	@go mod verify

# Install required tools with pinned versions for reproducibility
tools:
	@echo "Installing required tools..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.8
	@go install mvdan.cc/gofumpt@v0.7.0
	@go install github.com/securego/gosec/v2/cmd/gosec@v2.21.4
	@echo "✅ All tools installed"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -f coverage.out coverage.html
	@go clean -cache
