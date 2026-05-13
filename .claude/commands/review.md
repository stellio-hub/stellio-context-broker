Review the open PR for the current branch using the `pr_reviewer` agent.

## Steps

1. Detect the current branch:
   ```bash
   git branch --show-current
   ```

2. Find the open PR for that branch via the GitHub MCP (or `gh pr view --json number,title,url`).

3. Invoke the `pr_reviewer` agent with the PR number and URL. Pass the full diff so it can evaluate spec compliance,
4. architecture rules, reactive correctness, test coverage, and Detekt compliance per `.claude/rules/`.

5. Present the review findings grouped by severity:
   - **Blocking** — spec violations, architecture violations, blocking calls, missing tests
   - **Required** — error handling gaps, logging issues, Detekt violations
   - **Suggestions** — style, naming, minor improvements

6. After presenting findings, ask whether to open GitHub review comments via the GitHub MCP.
