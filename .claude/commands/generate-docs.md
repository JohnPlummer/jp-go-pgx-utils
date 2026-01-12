# Generate AI-Optimized Documentation

Generate token-efficient documentation for the `.ai/` directory structure by combining extracted patterns with existing documentation, presenting each standard for confirmation.

**Project-agnostic command** - works with any codebase structure.

## Your Mission

Create comprehensive, token-efficient documentation in the `.ai/` structure by:

1. **Validating patterns.yaml is fresh** (generated within the last hour, unless --force used)
2. **Discovering documentation files** automatically from the project
3. Reading patterns from `patterns.yaml` (extracted code patterns)
4. Reading architectural patterns from discovered documentation
5. Analyzing each pattern for best practice validation
6. Presenting each standard individually for user confirmation
7. Generating `.ai/` structure files with token budget enforcement

## Step 0: Validate patterns.yaml Freshness

Check if user provided --force flag. If not, validate patterns.yaml:

**If --force flag provided:**

```
⚠ Skipping freshness check (--force flag provided)
Using existing patterns.yaml regardless of age
```

**If no --force flag, check patterns.yaml age:**

```
Checking patterns.yaml...

✓ File exists: patterns.yaml
✓ Last modified: [timestamp] ([duration] ago)
✓ Fresh (within 1 hour)

Proceeding with documentation generation...
```

**If patterns.yaml is stale (>1 hour old) and no --force:**

```
❌ patterns.yaml validation failed

Problem: File is older than 1 hour
Last modified: [actual timestamp]

The pattern extraction must be current to ensure documentation reflects
the actual codebase state.

Action required:
1. Run /extract-patterns to regenerate patterns.yaml
2. Re-run this command after patterns are extracted
   OR
3. Use --force flag to proceed anyway: /generate-docs --force

Stopping documentation generation.
```

**If patterns.yaml doesn't exist:**

```
❌ patterns.yaml not found

Pattern extraction is required before generating documentation.

Action required:
1. Run /extract-patterns to create patterns.yaml
2. Re-run this command after patterns are extracted

Stopping documentation generation.
```

## Step 1: Discover Documentation Sources

Automatically find documentation files in the project:

Search these common locations:

- docs/, documentation/, doc/, wiki/
- .claude/, .ai/
- README.md, CONTRIBUTING.md, ARCHITECTURE.md, DESIGN.md
- *.md files in project root (excluding node_modules, vendor, etc.)

Skip these directories:

- node_modules/, vendor/, .git/
- dist/, build/, bin/, out/
- test fixtures and example directories

**EXAMPLE OUTPUT FORMAT (your actual output will differ):**

```
Discovering documentation sources...

Searching common locations...

Found documentation files:
✓ [path/to/doc1.md] ([N]K tokens)
✓ [path/to/doc2.md] ([N]K tokens)
✓ [path/to/doc3.md] ([N]K tokens)

Total: [N] documentation files ([N]K tokens)

These files will be analyzed for standards and patterns.
```

**If no documentation found:**

```
⚠ No documentation files discovered

Only patterns.yaml will be used for generation.
This is fine for projects without existing documentation.

Proceeding with pattern-only generation...
```

## Input Sources

### Primary Source (Required)

**patterns.yaml** - Syntax patterns extracted by `/extract-patterns`

- Must be generated within the last hour (or use --force)
- Contains frequency, confidence, tier classification, and real code examples
- Project-agnostic format

### Secondary Sources (Auto-discovered)

**Documentation files** - Architectural patterns and standards

- Automatically found by scanning common locations
- May include: standards, testing guides, architecture docs, API references
- Project-specific content
- Used to supplement syntax patterns with architectural guidance

## Target Structure

**IMPORTANT: Following RAG Best Practices (2025)**

- **File size: 400-600 tokens per file** (optimal for retrieval)
  - **Aim for the full range** - 100-150 tokens is TOO SHORT
  - Be comprehensive within the limit
- **One focused topic per file** (better precision)
- **More granular files** (load only what's needed)
- **Be thorough** - check existing docs to ensure no standards are missed

```
.ai/
├── llms.md                       # Universal discovery file (~100 tokens)
├── common-standards/             # Portable patterns
│   ├── error-handling.md         # Error wrapping, custom errors (400-600 tokens)
│   ├── testing-structure.md      # Test organization, BDD patterns (400-600 tokens)
│   ├── constructors.md           # New* pattern, functional options (400-600 tokens)
│   ├── interfaces.md             # Interface design patterns (400-600 tokens)
│   ├── retry-logic.md            # Retry with backoff patterns (400-600 tokens)
│   ├── circuit-breakers.md       # Circuit breaker patterns (400-600 tokens)
│   └── [topic].md                # More focused topics as needed
├── project-standards/            # Project-specific
│   ├── README.md                 # Project overview (200-400 tokens)
│   ├── configuration.md          # Viper config pattern (400-600 tokens)
│   ├── mocking.md                # Mockery v3 patterns (400-600 tokens)
│   ├── database-testing.md       # Testcontainers patterns (400-600 tokens)
│   ├── api-patterns.md           # OpenAPI, Chi router (400-600 tokens)
│   ├── worker-patterns.md        # Worker/pipeline patterns (400-600 tokens)
│   └── [topic].md                # More focused topics as needed
└── examples/                     # Real code examples
    └── [pattern-name].[ext]      # Actual code from codebase
```

**Note:** Total token counts across all files don't matter - only per-file limits matter.
Files are loaded on-demand, not all at once.

**Why smaller files?**

- LLMs load only relevant context (not entire 2K file)
- Better retrieval precision
- Easier maintenance
- Industry standard for RAG systems

## Step 2: Read Pattern Data

Load all input sources:

```
Reading patterns.yaml...
✓ File timestamp: [timestamp]
✓ Fresh - proceeding
✓ Found [N] patterns ([N] Go, [N] TypeScript, etc.)

Reading discovered documentation files...
✓ Analyzing [file1]... ([N] patterns found)
✓ Analyzing [file2]... ([N] patterns found)
...

Total standards to review: [N]
```

**CRITICAL: Be Thorough**

Before proceeding to Step 3:

1. **Check ALL existing documentation** - don't skip any files
2. **Cross-reference patterns** - ensure patterns.yaml patterns are all covered
3. **Review for missing standards** - compare against existing docs to catch gaps
4. **Second pass if needed** - if you find you missed standards, add them

**Common mistake:** Being too conservative and skipping standards.
Better to have 40 comprehensive 500-token files than 20 sparse 150-token files.

## Step 3: Analyze and Present Each Standard

For EACH standard found, perform analysis and present for confirmation.

**CRITICAL: Token Limit Enforcement**

Before presenting each standard:

1. Count tokens in the proposed content
2. **Aim for 400-600 tokens** - be comprehensive, not minimal
   - 100-150 tokens is TOO SHORT
   - Use examples, explanations, and best practices
3. Check if adding it would exceed file limits:
   - **Regular files: 600 tokens MAX**
   - **README files: 400 tokens MAX**
4. If it would exceed, you MUST:
   - Split into multiple focused files (preferred)
   - Trim content to fit (if splitting isn't logical)
   - Skip this standard (last resort)

**Balance:** Files should be comprehensive (400-600 tokens) but NEVER exceed 600.
Too short defeats the purpose, too long defeats RAG optimization.

**EXAMPLE PRESENTATION FORMAT (your actual content will differ):**

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Standard #[X] of [TOTAL]: [Pattern Name]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Source: [patterns.yaml OR documentation file path]
Language: [go/typescript/python/etc]
Tier: [common/project] ([portable pattern/project-specific])
Frequency: [N] occurrences (if from patterns.yaml)

Analysis:
[✓/⚠/❌] Best practice: [assessment]
[✓/⚠/❌] Active usage: [found in N locations OR not found]
[✓/⚠/❌] Consistent: [assessment]
[✓/⚠/❌] Current: [assessment]

Usage examples found in:
- [file:line]
- [file:line]
...

Proposed content ([N] tokens):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[Full markdown content that would be added to the file]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Target file: .ai/[category]/[filename].md
Current file size: [N] tokens
Proposed content: [N] tokens
After adding: [N] tokens

**Token Check:**
- Limit: 600 tokens (400 for README)
- After adding: [N] tokens
- Status: [✓ WITHIN LIMIT | ❌ EXCEEDS LIMIT by [N] tokens]

**If exceeds limit, you MUST:**
1. Split into multiple files (e.g., mocking-basics.md + mocking-advanced.md)
2. Trim content (remove verbose examples, shorten explanations)
3. Skip this standard

Include this standard? [y/n/skip/quit]
```

Wait for user response before continuing to next standard.

## Step 4: Handle Warnings

If a pattern shows warning signs, present with recommendation:

**EXAMPLE WARNING FORMAT:**

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Standard #[X] of [TOTAL]: [Pattern Name]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Source: [documentation file]

Analysis:
⚠ Warning: Documented but low usage
✓ Found: [N] occurrences (expected more for best practice)
⚠ Inconsistent: Not uniformly applied
❌ Examples: Some examples reference deprecated patterns

Recommendation: SKIP or UPDATE

- Pattern is documented but not consistently used
- May be aspirational rather than actual practice
- Consider updating docs to match reality

Include this standard anyway? [y/n/skip/quit]
```

## Step 5: Generate Files

After all confirmations, generate files.

**FINAL TOKEN VALIDATION:**

Before writing each file:

1. Count total tokens
2. Verify against limit (600 for files, 400 for READMEs)
3. If over: STOP and split/trim
4. Only write files that pass validation

**Token violations are test failures** - `npm run test:tokens` must pass.

Generated files:

```
Generating .ai/ structure...

Created: .ai/common-standards/[topic].md ([N] tokens)
  ✓ [N] standards included
  ✓ Within 600 token limit

Created: .ai/project-standards/README.md ([N] tokens)
  ✓ Project overview
  ✓ Within 400 token limit

Created: .ai/examples/[pattern-name].[ext]

Created: .ai/llms.md ([N] tokens)
  ✓ Universal discovery file
  ✓ Progressive loading map

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Summary
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Total standards reviewed: [N]
Included: [N]
Skipped: [N]
Files created: [N]

All per-file token limits respected ✓

All files generated successfully!

Next steps:

1. Review generated .ai/ files
2. Run token counting tests (if available in project)
3. Test with fresh AI coding assistant session
4. Iterate on pattern extraction if needed
```

## Phase 2: Update Existing Files (Diff-based)

When `.ai/` structure already exists, show only changes:

**EXAMPLE UPDATE FORMAT:**

```
Analyzing existing .ai/ structure...

Found existing files:
- [list of existing files with token counts]

Comparing with current patterns and discovered docs...

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Update #[X]: .ai/[category]/[file].md
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Changes detected:

- ADD: [Pattern Name] ([N] tokens)
  Source: [file:lines]
  Reason: [why adding]

  Proposed content:
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  [Full content]
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

- REMOVE: "[Section Name]" ([N] tokens)
  Reason: [why removing]
  Last seen: [when]

~ MODIFY: "[Section Name]"
  Change: [what's changing]
  Tokens: [old] → [new] ([+/-N])

Net token change: [+/-N] tokens
New total: [N] tokens (within [limit] limit ✓ OR ⚠ would exceed)

Apply these changes? [y/n/skip/quit]
```

## Best Practice Validation Criteria

For each standard, check:

### ✓ Include if

- Found in actual codebase (grep confirms usage)
- Used consistently (same pattern across multiple files)
- Has real code examples from existing files
- Documented or extracted from patterns
- Frequency > 5 (for syntax patterns)

### ⚠ Warning if

- Documented but low usage (<3 occurrences)
- Inconsistent application
- Examples don't match current code
- Only found in old/deprecated files

### ❌ Skip if

- Not found in codebase at all
- Theoretical/aspirational only
- Contradicts actual practice
- Deprecated library/pattern
- Duplicate of another standard

## Reference Guidelines

**CRITICAL: Only reference code files, never documentation**

When generating standards, references must point to actual source code:

### ✓ Acceptable References

- **Source code files:**
  - `pkg/repository/user_repository.go`
  - `backend/pkg/api/handlers/activity_handler.go`
  - `pipeline/pkg/workers/enricher/worker.go`
  - `frontend/src/components/ActivityCard.tsx`

- **Configuration files:**
  - `pipeline/pkg/config/config.go`
  - `.mockery.yaml`
  - `shared/api/openapi.yaml`

- **Test files:**
  - `pkg/repository/user_repository_test.go`
  - `tests/integration/database_test.go`

### ❌ NEVER Reference

- **Documentation directories (will be deleted):**
  - ❌ `docs/golang-standards.md`
  - ❌ `docs/testing-strategy.md`
  - ❌ `docs/api-reference.md`
  - ❌ `documentation/*`

- **Meta files:**
  - ❌ Other .ai/ files
  - ❌ README.md sections
  - ❌ CLAUDE.md sections

**Why?** The docs/ directory will be archived/deleted after .ai/ structure is complete.
All documentation must be self-contained without circular references.

**Instead of:** "See docs/golang-standards.md for details"
**Use:** "See pipeline/pkg/config/config.go for implementation"

## Token Budget Enforcement

**Per-file limits are the ONLY limits that matter:**

- **common-standards/*.md: 400-600 tokens each**
- **project-standards/*.md: 400-600 tokens each**
- **project-standards/README.md: 200-400 tokens**
- **llms.md: ~100 tokens**

**Why 400-600 tokens?**

- Based on 2025 RAG research for technical documentation
- Balances context retention with retrieval precision
- LLM loads only what it needs (not all files at once)
- Comprehensive enough to be useful, small enough for precision

**Total token counts don't matter** - files are loaded on-demand, not all together.
You can have 50 files or 100 files - only per-file limits matter.

**When file approaches or exceeds limit:**

```
❌ STOP - Token limit exceeded: .ai/[category]/[file].md

Current: 520 tokens
Proposed addition: 150 tokens
Total would be: 670 tokens
Limit: 600 tokens
EXCEEDS BY: 70 tokens

You MUST take action - cannot proceed with adding this content as-is.

Required action:

1. **Split file** (PREFERRED):
   - Create new focused file for this topic
   - Example: Split mocking.md into:
     - mocking-testify.md (testify/mockery patterns)
     - mocking-manual.md (manual mock patterns)

2. **Trim content**:
   - Remove verbose examples
   - Shorten descriptions
   - Keep only essential patterns

3. **Skip this standard** (last resort)

Choose: [split/trim/skip/quit]
```

**IMPORTANT:** Exceeding limits breaks RAG optimization. Always prefer splitting over cramming.
50 focused 500-token files > 10 bloated 2K files.

## Output Format

### Standard File Structure

Each generated file should follow this format:

```markdown
# [Category] Patterns

[Brief description]

## [Pattern Name]
[Pattern documentation]

## [Pattern Name]
[Pattern documentation]

[... more patterns ...]
```

### llms.md Structure

```markdown
# [Project Name] - AI Documentation

Progressive loading map for AI assistants.

**Note:** This file should be referenced from your project's main config file
(e.g., CLAUDE.md, .cursorrules, or similar). Different assistants have different
entry points - this file provides a universal structure they can all use.

## Always Load
- .ai/llms.md (this file)

## On Request (Common Standards - Portable Patterns)
- .ai/common-standards/[topic].md - [description]
  (e.g., golang.md, typescript.md, testing.md, etc.)

## On Request (Project Standards - Project-Specific)
- .ai/project-standards/README.md - Project overview
- .ai/project-standards/[topic].md - [description]

## Examples
- .ai/examples/*.[ext] - Real code examples from codebase
```

## Confirmation Options

For each standard:

- **y** - Include this standard
- **n** - Skip this standard (don't include)
- **skip** - Skip and don't ask again for similar patterns
- **quit** - Stop processing, save progress so far

## Command Flags

- No flags: Normal mode (checks freshness, generates new structure)
- `--force`: Skip freshness check, use stale patterns.yaml
- `--update`: Update existing .ai/ structure (diff mode)
- `--new`: Generate from scratch (ignore existing)

## Usage

Invoke this command:

```
/generate-docs
```

Or with options:

```
/generate-docs --force     # Skip freshness check
/generate-docs --update    # Update existing .ai/ structure (diff mode)
/generate-docs --new       # Generate from scratch
```

The command will:

1. Validate patterns.yaml is fresh (<1 hour old) unless --force
2. Discover documentation files automatically
3. Guide you through the interactive process
4. Present each standard for confirmation
5. Generate the final `.ai/` structure

**Note:** This command is project-agnostic and will work with any project structure.
