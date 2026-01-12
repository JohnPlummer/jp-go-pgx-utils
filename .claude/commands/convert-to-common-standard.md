# Convert Project Standard to Common Standard

Interactive prompt to analyze a project-specific standard and extract reusable common patterns.

## Purpose

Identify which parts of a project standard are:

- **Common**: Portable patterns usable across all projects
- **Project-specific**: Tied to this project's unique structure, configuration, or decisions

## Process

### Step 1: Analyze the Standard

Read the project standard file and identify:

1. **Universal Patterns**
   - Language/framework patterns (Go, TypeScript, React)
   - Testing strategies (BDD, mocking, integration tests)
   - Architecture patterns (clean architecture, repository pattern)
   - Documentation conventions
   - Code organization principles

2. **Project-Specific Elements**
   - Specific file paths (e.g., `pipeline/pkg/errors/errors.go`)
   - Project-specific types or constants (e.g., `ErrInvalidPackageType`)
   - Configuration tied to this project's structure
   - Service-specific workflow (e.g., CloudId for Jira)
   - Project-specific make targets or scripts

3. **Hybrid Elements**
   - Patterns that are common but with project-specific examples
   - Need to extract the pattern and create generic examples

### Step 2: Discuss with User

Present your analysis:

```
## Analysis: [filename.md]

### Universal Patterns Found
- Pattern 1: [description]
- Pattern 2: [description]

### Project-Specific Elements Found
- Element 1: [description]
- Element 2: [description]

### Hybrid Elements
- Element 1: [pattern name] - Common pattern with project-specific examples

### Recommendation
[Extract fully to common | Keep in project | Split into common + project]
```

Ask user:

1. Do they agree with the classification?
2. For hybrid elements, should we create generic examples?
3. Should any project-specific examples move to `.ai/common/examples/`?

### Step 3: Create Output Files

Based on user agreement, generate:

#### Option A: Extract Fully to Common

- Create `~/code/ai-common/standards/[category]/[filename].md`
- Remove all project-specific references
- Use generic examples or reference common examples
- Delete from project-standards

#### Option B: Keep in Project

- Leave file in `.ai/project-standards/[filename].md`
- No changes needed

#### Option C: Split into Common + Project

- Create `~/code/ai-common/standards/[category]/[filename].md` with universal pattern
- Update `.ai/project-standards/[filename].md` to reference common standard
- Project version extends common with project-specific details

#### Option D: Create Example File

- Extract complex example code to `.ai/common/examples/[filename]-example.md`
- Reference from common standard

### Step 4: Update References

After moving to common:

1. Remove project-specific code paths
2. Replace with generic examples or references to examples
3. Update any cross-references in other standards
4. Add to appropriate category in common-llms.md

## Common Standard Template

```markdown
# [Pattern Name]

*Category: [go/typescript/testing/architecture/database/infrastructure/documentation]*

## When to Use

[1-2 sentences describing when this pattern applies]

## Pattern

[Core pattern description - no project-specific details]

## Implementation

[Generic implementation steps or code examples]

## Examples

[Generic examples or reference to .ai/common/examples/]

## Related Standards

- [link to related common standard]
- [link to related common standard]

## Common Pitfalls

[Issues that apply across all projects]
```

## Project Standard Template (when extending common)

```markdown
# [Pattern Name] - Project Implementation

*See common standard: `.ai/common/standards/[category]/[filename].md`*

## Project-Specific Details

[How this project implements the common pattern]

## Project Examples

[Real code paths and examples from this project]

## Project-Specific Gotchas

[Issues specific to this project's implementation]
```

## Quick Classification Guide

**Move to Common if:**

- ✅ Pattern applies to any Go/TypeScript/React project
- ✅ Testing strategy usable in any project with same framework
- ✅ Architecture pattern not tied to specific services
- ✅ Documentation convention universally applicable
- ✅ No references to specific project files or types

**Keep in Project if:**

- ❌ References specific file paths (e.g., `pipeline/pkg/...`)
- ❌ Uses project-specific types (e.g., `ErrInvalidPackageType`)
- ❌ Tied to project's unique structure or services
- ❌ Contains project CloudId, API keys, or configuration
- ❌ Describes project-specific workflow or make targets

**Split if:**

- 🔀 Contains common pattern with project-specific examples
- 🔀 Pattern is universal but implementation has project details
- 🔀 Could benefit other projects with generic version

## Usage

```
I want to analyze [filename.md] to see if it should be a common standard.

[Assistant reads file, analyzes, discusses with user, generates output]
```

## Notes

- Prefer common over project when uncertain - easier to specialize later
- Generic examples are better than no examples
- Reference common standards from project standards when extending
- Keep token efficiency in mind - don't duplicate content
