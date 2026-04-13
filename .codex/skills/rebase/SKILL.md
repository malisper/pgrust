---
name: rebase
description: Rebase the current branch onto `perf-optimization`. Use when the user asks to take the checked out branch and replay it on top of `perf-optimization` without merging.
---

# Rebase Current Branch Onto perf-optimization

This skill rebases the currently checked out branch onto `perf-optimization`.

## Guardrails

- Verify the current branch first with `git branch --show-current`.
- If the current branch is already `perf-optimization`, stop and say there is nothing to rebase onto.
- Do not discard uncommitted changes. If the worktree is dirty, report it and either use `git rebase --autostash perf-optimization` or tell the user why the local changes are risky.
- Do not use merge for this workflow.

## Default command sequence

Run from the repo root:

```bash
git branch --show-current
git status --short
git rebase --autostash perf-optimization
```

## Conflict handling

- Summarize the conflicting files.
- Inspect the conflict markers and resolve them without reverting unrelated user edits.
- Continue with `git rebase --continue`.
- If the branch should not be rebased after inspection, stop and explain why before using `git rebase --abort`.

## Reporting

When finished, report:

- original branch name
- whether autostash was used
- whether conflicts occurred
- final `git status --short --branch`

## Notes

- This keeps `perf-optimization` unchanged and moves the current branch.
- If the branch needs upstream freshness first, mention that a fetch would require an explicit network step.
