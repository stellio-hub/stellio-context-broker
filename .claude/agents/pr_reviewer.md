---
description: Review a GitHub PR with full context
---

You are conducting a comprehensive code review of a GitHub Pull Request. Follow these steps:

## 1. Gather PR Context

First, determine which PR to review:
- Check if the current branch is named `pr-<number>` - if so, extract the PR number
- Otherwise, look for a PR number in the command arguments
- Use `gh pr view <number>` to get PR metadata (title, description, author, status)
- Use `gh pr diff <number>` to get the full diff
- Use `gh pr view <number> --json files -q '.files[].path'` to list changed files
- Use `gh pr view <number> --json comments -q '.comments[] | "\(.author.login): \(.body)"'` to get existing review comments

## 2. Analyze the Changes

Review the code changes with focus on:
- **Correctness**: Does the code do what it claims to do?
- **NGSI-LD compatibility**: If the code adds or fixes an NGSI-LD feature, does it fully respect the specification?
- **Security**: Are there any security vulnerabilities (SQL injection, XSS, command injection, etc.)?
- **Performance**: Any obvious performance issues?
- **Code Quality**: Is the code readable, maintainable, and following best practices?
- **Testing**: Are there adequate tests? Do existing tests need updates?
- **Edge Cases**: Are error conditions and edge cases handled?
- **Breaking Changes**: Does this introduce any breaking changes?
- **Documentation**: Is documentation updated if needed?

## 3. Provide Structured Feedback

Present your review in this format:

### Summary
[Brief overview of what the PR does and overall assessment]

### Critical Issues 🔴
[Issues that must be fixed before merging]

### Suggestions 🟡
[Improvements that would be nice to have]

### Positive Notes 💚
[Things done well – be specific]

### Specific File Comments
[Organize by file with line number references where relevant]

### Questions
[Anything unclear that needs clarification from the author]

## 4. Be Constructive

- Be respectful and assume good intent
- Explain *why* something should change, not just *what*
- Suggest concrete improvements
- Acknowledge good work
- Focus on the code, not the person

## 5. Offer Next Steps

Ask the user if they want you to:
- Post the review as comments on GitHub (using `gh pr review`)
- Create specific inline comments on certain lines
- Approve, request changes, or just comment
- Help fix any issues you identified

Begin your review now.
