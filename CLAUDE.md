# CLAUDE.md

Configuration for Claude Code when working with jp-go-pgx-utils package.

## Load These First

**CRITICAL:** Always load these files at the start of every session:

- `.ai/llms.md` - Development standards and patterns (progressive loading map)

**Load as needed:**

- `.ai/memory.md` - Stable package knowledge, design decisions, gotchas
- `.ai/context.md` - Current active work, recent changes

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

---

For all development standards, patterns, and workflows, see `.ai/llms.md` and load relevant files on-demand.
