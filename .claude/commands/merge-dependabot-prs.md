# Merge Dependabot PRs

Process all open PRs across go-packages submodules. For each PR: rebase on main, ensure CI passes, merge.

## Instructions

Use the general-purpose subagent for EACH PR to minimize context bloat. Do not process PRs in the main conversation.

## Step 1: Discover PRs

Run this to get all open PRs across all package repos:

```bash
for dir in /Users/jp/code/go-packages/*/; do
  if [ -d "$dir/.git" ]; then
    repo=$(basename "$dir")
    echo "=== $repo ==="
    cd "$dir" && gh pr list --state open --json number,title,headRefName,author --jq '.[] | "\(.number)\t\(.title)\t\(.headRefName)\t\(.author.login)"' 2>/dev/null || echo "No PRs or not a GH repo"
  fi
done
```

## Step 2: Process Each PR

For EACH PR found, spawn a general-purpose subagent with this prompt:

```
Process PR #[NUMBER] in repo [REPO_NAME] at /Users/jp/code/go-packages/[REPO_NAME]

Steps:
1. cd to the repo directory
2. Fetch latest: git fetch origin
3. Checkout the PR branch: gh pr checkout [NUMBER]
4. Rebase on main: git rebase origin/main
5. If conflicts, resolve them sensibly for dependency updates
6. Force push the rebased branch: git push --force-with-lease
7. Wait for CI to start, then check status: gh pr checks [NUMBER] --watch
8. If CI fails:
   - Investigate the failure
   - Fix if straightforward (usually go mod tidy or minor compatibility)
   - Push fix and wait for CI again
9. Once CI passes, merge: gh pr merge [NUMBER] --squash --delete-branch
10. Report: PR merged successfully OR describe what blocked it

Do NOT ask for confirmation. Process autonomously and report results.
```

## Step 3: Summary

After all subagents complete, summarize:

- Total PRs processed
- Successfully merged
- Failed (with reasons)

## Notes

- Dependabot PRs are usually safe to merge after CI passes
- If a PR requires significant code changes beyond go mod tidy, skip it and report
- Process PRs in dependency order if possible (base packages first)
