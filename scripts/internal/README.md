# scripts/internal

Files in this folder are stripped from the public `malisper/pgrust` repo by `publish-public.sh` — `git filter-repo --path scripts/internal --invert-paths` runs as part of every publish, so nothing here appears in any commit reachable from public `main`.

**Never reference files in this folder from paths outside `scripts/internal/`.** Anything that links here will be a broken link on public (folder doesn't exist there), and anything that imports/sources from here will fail at runtime on public.

## What's here

- `publish-public.sh` — the private → public release script. Runs `git filter-repo` with the rules in `redactions.txt`, then force-pushes to `malisper/pgrust:main`.
- `redactions.txt` — the list of file-path drops and string-replacement rules applied to every publish. Format is `git filter-repo --replace-text`-compatible: `FIND==>REPLACE` per line.

## Adding a new secret pattern

1. Add the rule to `redactions.txt` (`SECRET==>REPLACEMENT`).
2. Commit to `perf-optimization` via the normal merge queue.
3. The next `./scripts/internal/publish-public.sh --republish` will scrub it out of public history — but since the rule is new, old commits get rewritten too, and the push becomes a force-push instead of a fast-forward. To avoid surprise force-pushes, aim to catch new patterns before they land rather than batching.

The pre-commit hook (`.githooks/pre-commit`) reads this same file and warns (will eventually block) if a commit introduces any of these patterns. If the hook hits a false positive, either refine the pattern or bypass with `git commit --no-verify` for that one commit.

## Related docs

- Runbook: `pagerfree-shared/docs/hn-launch/ops/MALIS-publish-runbook.md` §8
- Rationale (old): `pagerfree-shared/docs/hn-launch/ops/publish-public-repo.md`
