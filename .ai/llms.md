# jp-go-pgx-utils - AI Documentation

Progressive loading map for AI assistants working with jp-go-pgx-utils package.

**Entry Point**: This file should be referenced from CLAUDE.md.

## Package Overview

**Purpose**: PostgreSQL utilities for Go projects using pgx/pgxpool

**Key Features**:

- Connection type wrapping pgxpool.Pool
- NewConnection for creating connection pools
- Health check with HealthCheck()
- Transaction helper WithTransaction()
- Query execution utilities
- Integration with jp-go-config DatabaseConfig
- Testcontainer support

## Always Load

- `.ai/llms.md` (this file)

## Load for Complex Tasks

- `.ai/memory.md` - Design decisions, gotchas, backward compatibility notes
- `.ai/context.md` - Current changes (if exists and is current)

## Common Standards (Portable Patterns)

**See** `.ai/common/common-llms.md` for the complete list of common standards.

Load these common standards when working on this package:

### Core Go Patterns

- `common/standards/go/constructors.md` - New* constructor functions
- `common/standards/go/type-organization.md` - Interface and type placement
- `common/standards/go/error-wrapping.md` - Error wrapping with %w

### Database

- `common/standards/database/database-transactions.md` - Transaction patterns

### Testing

- `common/standards/testing/bdd-testing.md` - Ginkgo/Gomega patterns
- `common/standards/testing/test-categories.md` - Test organization
- `common/standards/testing/testcontainers.md` - Testcontainer patterns

### Documentation

- `common/standards/documentation/pattern-documentation.md` - Documentation structure
- `common/standards/documentation/code-references.md` - Code examples

## Project Standards (Package-Specific)

This package has minimal package-specific standards since it IS a standard itself.

Any package-specific patterns should go in `.ai/project-standards/`

## Loading Strategy

| Task Type | Load These Standards |
|-----------|---------------------|
| Adding new utilities | constructors.md, error-wrapping.md, type-organization.md |
| Transaction handling | database-transactions.md |
| Writing tests | bdd-testing.md, testcontainers.md, test-categories.md |
| Documenting utilities | pattern-documentation.md, code-references.md |
| Ensuring compatibility | memory.md (for backward compatibility notes) |

## File Organization

```
jp-go-pgx-utils/
├── CLAUDE.md                   # Entry point
├── .gitignore                  # Ignores context.md, memory.md, tasks/
└── .ai/
    ├── llms.md                 # This file (loading map)
    ├── README.md               # Documentation about .ai setup
    ├── context.md              # Current work (gitignored)
    ├── memory.md               # Stable knowledge (gitignored)
    ├── tasks/                  # Scratchpad (gitignored)
    ├── project-standards/      # Package-specific (if needed)
    └── common -> ~/code/ai-common  # Symlink to shared standards
```

## Key Principles

1. **Backward Compatibility**: Never break existing Connection type or methods
2. **Generic Design**: No project-specific utilities in this package
3. **pgx Integration**: Wraps pgxpool.Pool for connection pooling
4. **Transaction Safety**: WithTransaction handles rollback/commit automatically
5. **Health Checks**: Provide health check functionality for service monitoring

## Related Documentation

- Common standard: `common/standards/go/jp-go-pgx-utils.md` - How to USE this package
- This is the implementation, that is the usage guide
