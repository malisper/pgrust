---
name: merge
description: Merge the current branch into `perf-optimization` with a merge commit, then fast-forward the original branch to that merge commit. Use when the user asks to merge `perf-optimization` with the branch that is currently checked out.
---

# Merge Current Branch Into perf-optimization

This skill creates a merge commit on `perf-optimization` that merges the currently checked out branch, then fast-forwards the original branch so both branch names end at that merge commit.

## Guardrails

- Capture the starting branch with `git branch --show-current`.
- If the current branch is `perf-optimization`, stop and say there is no separate branch to merge into `perf-optimization`.
- Check `git status --short` before switching branches.
- Require a clean worktree before switching branches. If there are uncommitted changes, tell the user to commit them first before continuing.
- Do not discard uncommitted changes.
- Finish on the original branch after the fast-forward unless the user asks to stay on `perf-optimization`.

## Default command sequence

```bash
git branch --show-current
git status --short
git checkout perf-optimization
git merge --no-ff <original-branch>
git checkout <original-branch>
git merge --ff-only perf-optimization
```

Replace `<original-branch>` with the branch captured before checkout.

## Conflict handling

- Resolve conflicts while creating the merge commit on `perf-optimization` without overwriting unrelated local work.
- Complete the merge commit on `perf-optimization`, then resume the fast-forward step for the original branch.
- If the merge is the wrong move, explain the reason before using `git merge --abort`.
- If `git merge --ff-only perf-optimization` from the original branch fails, stop and report that the original branch cannot be fast-forwarded as expected.

## Reporting

When finished, report:

- original branch that was merged into `perf-optimization`
- whether branch checkout required extra handling
- whether conflicts occurred
- whether the original branch was fast-forwarded successfully
- final branch checked out
- final `git status --short --branch`

## Notes

- This is intentionally a merge-commit workflow, not a rebase workflow.
- The merge commit is created on `perf-optimization` first; the original branch is updated afterward with a fast-forward.
