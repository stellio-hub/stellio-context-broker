Generate a standup update from recent git activity.

## Steps

1. Pull recent commits by the current author across all modules:
   ```bash
   git log --since="yesterday 00:00" --author="$(git config user.name)" --oneline --all
   ```
   If that returns nothing (e.g. on a Monday), extend to the last 3 days:
   ```bash
   git log --since="3 days ago" --author="$(git config user.name)" --oneline --all
   ```

2. Pull any open PRs:
   ```bash
   gh pr list --author "@me" --state open --json number,title,url,reviewDecision
   ```

3. Check for any failing CI on open PRs via the GitHub MCP or `gh pr checks`.

4. Format the output as:

---
**Yesterday**
- [bullet per commit or PR action — group related commits]

**Today**
- [infer from open PRs and in-progress branches]

**Blockers**
- [list any failing CI, blocked PRs, or explicit blockers — "None" if clean]
---

Keep bullets concise — one line each. Reference PR numbers as links where relevant.
