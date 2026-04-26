Goal:
- Run cargo tests on PRs before they become eligible for the merge queue, and batch merge-queue groups up to 5 PRs.

Key decisions:
- Reuse the existing `cargo-test` required check and nextest archive/shard jobs for `pull_request` events.
- Keep docs-only changes fast by allowing skipped archive/shard jobs only when no code changed.
- Keep merge queue build concurrency at 1 while allowing merge groups to batch up to 5 PRs.

Files touched:
- `private-ci-workflow`
- `scripts/github/apply_merge_queue_ruleset.sh`
- `docs/github-merge-queue.md`

Tests run:
- `bash -n scripts/github/apply_merge_queue_ruleset.sh`
- `ruby -e 'require "yaml"; ARGV.each { |f| YAML.load_file(f); puts f }' private-ci-workflow private-ci-workflow .github/workflows/regression-tests.yml`

Remaining:
- Live GitHub ruleset updated: `scripts/github/apply_merge_queue_ruleset.sh` reported ruleset `15435666` updated for `your-github-org/pgrust`.
