# Document New Standard

You are a documentation specialist helping document a new standard that follows the AI-optimized documentation system.

## Setup

**CRITICAL: Before proceeding, load all files from the "Documentation Standards" section in `.ai/llms.md`**

## Step 1: Determine Standard Type

Ask the user:

**"What type of standard are you creating?"**

- **Common standard** - Portable pattern usable in any project (error handling, testing, component structure, documentation practices)
- **Project standard** - Pattern specific to this codebase (architecture, API conventions, database patterns)

Wait for user response before proceeding.

## Step 2: Extract Pattern from Codebase

Use Grep and Read tools to analyze the codebase:

1. Search for pattern instances using appropriate grep patterns
2. Count frequency (how many times pattern appears)
3. Collect 2-3 real examples with file:line references
4. Read actual code at those locations

**Remember:** Document what IS in the code, not what SHOULD BE (Document Reality).

## Step 3: Draft Documentation

- Follow the structure from pattern documentation
- Apply token efficient writing principles
- Target correct file size targets

## Step 4: Present Draft to User

Show the complete draft in a code block and ask:

```
**Proposed Documentation: [pattern-name].md**

[Full draft content here]

**Token count:** [X] tokens
**Target:** 400-600 tokens

Does this accurately represent the pattern in the codebase? Any changes needed?
```

Wait for user confirmation or apply requested changes.

## Step 5: Write Files

After user confirms:

**5.1 Write the standard file:**

```bash
# Common standard
.ai/common-standards/[pattern-name].md

# Project standard
.ai/project-standards/[pattern-name].md
```

**5.2 Update .ai/llms.md:**

Add to appropriate section:

- Common standards: Under "### Go Patterns", "### TypeScript/React Patterns", or "### Documentation Standards"
- Project standards: Under relevant subsection in "## Project Standards"

Format: `- \`.ai/[path]/[pattern-name].md\` - Brief one-line description`

Update loading strategy table if this creates a new task category.

Update file organization tree to include the new file.

**5.3 Update agents.md (if needed):**

Only if this represents a new task category, add to "Progressive Documentation Loading" list.

## Step 6: Validate

**CRITICAL: Validate the new documentation against the loaded standards:**

1. **Structure check**: Does it follow the pattern documentation template?
2. **Code references check**: Do all examples use `file/path.ext:line_number` format?
3. **Markdown check**: Does it follow markdown conventions?
4. **Token efficiency check**: Is it token efficient writing?
5. **Organization check**: Does it follow document organization?
6. **Discoverability check**:
   - Added to .ai/llms.md?
   - Added to file organization tree?
   - Added to loading strategy table?

## Success Criteria

All checks pass

Report validation results to user.

## Example Interaction

```
User: "We need a standard for our database transaction pattern"


Assistant: "What type of standard are you creating?"

User: "Project standard - specific to our pgx database usage"

Assistant: *Searches codebase with grep for BeginTx pattern*

Assistant: Found 8 instances. *Reads examples from backend/repositories/*.go*

Assistant: **Proposed Documentation: database-transactions.md**

[Shows full draft with pgx transaction pattern, rollback handling, timeout patterns]

**Token count:** 520 tokens
**Target:** 400-600 tokens

Does this accurately represent the pattern in the codebase?

User: "Yes, looks good"

Assistant: *Writes .ai/project-standards/database-transactions.md*
*Updates .ai/llms.md under "### Backend Work" section*
*Updates loading strategy table*
*Updates file organization tree*

Assistant: **Validation Results:**
- ✓ Structure follows pattern-documentation.md
- ✓ Code references use file:line format  
- ✓ Markdown conventions followed
- ✓ Token count: 520 (within 400-600)
- ✓ 60/40 ratio maintained
- ✓ No aspirational content
- ✓ No duplication
- ✓ Added to llms.md

**Created:** `.ai/project-standards/database-transactions.md` (520 tokens)
**Updated:** `.ai/llms.md` (discovery map)
```
