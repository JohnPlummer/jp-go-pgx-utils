# Extract Code Patterns from Codebase

Analyze the codebase systematically to identify real code patterns in use, count their frequency, extract examples, and output structured YAML for documentation generation.

## Your Task

Execute pattern extraction across Go and TypeScript files, generating a comprehensive patterns.yaml file.

## Step-by-Step Process

### Step 1: Analyze Go Patterns

For each pattern category below, use Grep to find occurrences, count frequency, and extract 2-3 examples:

#### 1. Error Wrapping

- **Pattern**: `fmt.Errorf` with `%w` verb
- **Search**: `fmt\.Errorf.*%w`
- **Path**: `backend/`, `pipeline/`
- **Classification**: common (Go 1.13+ standard)

#### 2. Ginkgo Test Structure

- **Patterns**: `Describe(`, `Context(`, `It(`, `BeforeEach(`, `AfterEach(`
- **Search**: `(Describe|Context|It|BeforeEach|AfterEach)\(`
- **Path**: `backend/`, `pipeline/`
- **Type**: `go`
- **Classification**: project-specific

#### 3. Interface Patterns

- **Patterns**: Service, Repository, Handler interfaces
- **Search**: `type \w+(Service|Repository|Handler) interface`
- **Path**: `backend/`, `pipeline/`
- **Classification**: project-specific

#### 4. Constructor Patterns

- **Simple constructors**: `func New\w*\(`
- **Functional options**: `type \w+Option func`
- **Path**: `backend/`, `pipeline/`
- **Classification**: common (functional options), project-specific (New*)

#### 5. Package Organization

- **Service layer**: `*_service.go` files or `services/` directories
- **Handler layer**: `*_handler.go` files or `handlers/` directories
- **Repository layer**: `*_repository.go` files or `repository/` directories
- **Classification**: project-specific

### Step 2: Analyze TypeScript/React Patterns

#### 1. Functional Components

- **Pattern**: `const \w+ = \(.*\) =>` or `function \w+\(`
- **Path**: `frontend/src/`
- **Type**: `tsx`
- **Classification**: common (React standard)

#### 2. React Hooks

- **Patterns**: `useState`, `useEffect`, `useContext`, `useQuery`, `useMutation`
- **Search**: `(useState|useEffect|useContext|useQuery|useMutation)\(`
- **Path**: `frontend/src/`
- **Type**: `tsx`
- **Classification**: common (React/React Query standard)

#### 3. API Integration

- **Patterns**: `fetch(`, `axios.`
- **Search**: `fetch\(|axios\.`
- **Path**: `frontend/src/`
- **Classification**: project-specific

#### 4. Testing Patterns

- **Patterns**: `describe(`, `it(`, `test(`, `expect(`, `render(`, `screen.`
- **Search**: `(describe|it|test)\(|render\(|screen\.|fireEvent\.`
- **Path**: `frontend/src/`
- **Type**: `test.tsx`
- **Classification**: common (Vitest/RTL standard)

### Step 3: For Each Pattern

1. **Use Grep to find occurrences**:

   ```
   Grep:
   - pattern: <regex pattern>
   - path: <search path>
   - output_mode: "content"
   - -n: true (show line numbers)
   - type: <go|tsx> if applicable
   ```

2. **Count total occurrences** (this is the frequency)

3. **Select 2-3 representative examples**:
   - Choose from different files if possible
   - Pick clear, typical usage
   - Note file path and line number

4. **Use Read to get full context** (±3-5 lines around the match)

5. **Determine confidence level**:
   - **high**: 10+ occurrences across multiple files
   - **medium**: 5-9 occurrences or 2-3 files
   - **low**: 1-4 occurrences or single file

### Step 4: Generate patterns.yaml

Create a file with this structure:

```yaml
extraction_metadata:
  timestamp: "<ISO 8601 timestamp>"
  total_go_files_analyzed: <count>
  total_ts_files_analyzed: <count>
  total_patterns_found: <count>

patterns:
  - name: "Error Wrapping with fmt.Errorf"
    language: go
    frequency: 47
    confidence: high
    tier: common
    description: "Standard Go 1.13+ error wrapping using %w verb"
    examples:
      - file: backend/services/user_service.go:142
        code: |
          if err != nil {
              return fmt.Errorf("failed to create user: %w", err)
          }
      - file: backend/handlers/activity_handler.go:89
        code: |
          if err := s.activityService.Create(ctx, activity); err != nil {
              return fmt.Errorf("create activity failed: %w", err)
          }

  - name: "Ginkgo BDD Test Structure"
    language: go
    frequency: 156
    confidence: high
    tier: project
    description: "Project-wide BDD testing pattern using Ginkgo"
    examples:
      - file: backend/services/user_service_test.go:23
        code: |
          Describe("UserService", func() {
              Context("when creating a user", func() {
                  It("should return the created user", func() {
                      // test implementation
                  })
              })
          })

  # ... more patterns
```

## Constraints

- Skip these directories: `node_modules/`, `vendor/`, `.git/`, `dist/`, `build/`, `bin/`
- If a pattern has >100 occurrences, sample across different parts of codebase
- Maximum 3 examples per pattern
- Code snippets should be 3-10 lines (enough context, not overwhelming)
- Focus on actual usage patterns in production code

## Output

1. Create `patterns.yaml` in the project root
2. Report summary:
   - Total patterns found
   - Patterns by language (Go vs TypeScript)
   - Patterns by tier (common vs project-specific)
   - Highest frequency patterns (top 5)

## Usage

Invoke this command with:

```
/extract-patterns
```

Or for targeted extraction:

```
/extract-patterns [--go-only | --ts-only]
```
