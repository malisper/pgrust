---
name: merge
description: Rebase `perf-optimization` onto the current branch. Use when the user asks to take the `perf-optimization` branch and replay it on top of whichever branch is currently checked out.
---

# Rebase perf-optimization Onto Current Branch

This skill moves `perf-optimization` so it sits on top of the currently checked out branch.

## Guardrails

- Capture the starting branch with `git branch --show-current`.
- If the current branch is `perf-optimization`, stop and say there is no separate base branch to rebase onto.
- Check `git status --short` before switching branches.
- Do not discard uncommitted changes. If the worktree is dirty, explain that checking out `perf-optimization` may fail or may require stashing.
- Return to the original branch after the rebase unless the user asks to stay on `perf-optimization`.

## Default command sequence

```bash
git branch --show-current
git status --short
git checkout perf-optimization
git rebase <original-branch>
git checkout <original-branch>
```

Replace `<original-branch>` with the branch captured before checkout.

## Conflict handling

- Resolve conflicts on `perf-optimization` without overwriting unrelated local work.
- Continue with `git rebase --continue`.
- If the rebase is the wrong move, explain the reason before using `git rebase --abort`.

## Reporting

When finished, report:

- original branch used as the new base
- whether branch checkout required extra handling
- whether conflicts occurred
- final branch checked out
- final `git status --short --branch`

## Notes

- Despite the skill name, this is a rebase workflow, not a merge commit workflow.
- If the user truly wants a merge commit instead, do not apply this skill.
