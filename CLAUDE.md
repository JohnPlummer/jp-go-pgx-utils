# CLAUDE.md

Configuration for Claude Code when working with jp-go-pgx-utils package.

## Standards

Use `/ai-common` skill to load development standards and patterns as needed.

## Package Purpose

jp-go-pgx-utils provides PostgreSQL utilities for Go projects with:

- Connection pooling with pgxpool
- Health check functionality
- Transaction helpers (WithTransaction)
- Query execution utilities
- Integration with jp-go-config DatabaseConfig
- Testcontainer support

## Development Guidelines

This is a **shared package** used across multiple projects. Changes must be:

- Backward compatible
- Well-tested
- Generic (not project-specific)
- Documented in examples
