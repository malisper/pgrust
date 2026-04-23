# GitHub Merge Queue Setup

This repository now includes two GitHub Actions workflows:

- `.github/workflows/merge-queue-tests.yml`
- `.github/workflows/auto-queue-pr.yml`

## What they do

- `merge-queue-tests.yml` runs on `pull_request` and `merge_group` for `perf-optimization`.
- Library tests run on a single 32-vCPU Ubuntu larger runner instead of being split across partitions.
- That test run writes a JUnit XML report.
- The `cargo-test` aggregation job downloads those reports, writes a timing summary into the Actions job summary, and becomes the single required check for branch protection.
- `auto-queue-pr.yml` runs on `pull_request_target` and enables auto-merge immediately for non-draft PRs targeting `perf-optimization`. With a required merge queue enabled, GitHub will add the PR to the queue automatically once its requirements pass.

## Apply the repository settings

GitHub's merge queue limits are repository settings, not workflow YAML. Apply them with:

```bash
scripts/github/apply_merge_queue_ruleset.sh
```

This script:

- enables repository auto-merge
- ensures merge commits are allowed
- creates or updates a repository ruleset on `refs/heads/perf-optimization`
- forces changes through pull requests
- requires the `cargo-test` status check
- enables a merge queue with:
  - build concurrency `5`
  - minimum entries to merge `1`
  - maximum entries to merge `5`
  - wait time `0` minutes

## Runner prerequisite

The workflow targets the GitHub larger-runner label `ubuntu-24.04-32core`.
That runner must exist and be enabled for this repository in GitHub's runner settings, or the test job will stay queued without starting.

## Notes

- If GitHub records the required check under a different context name than `cargo-test`, rerun the script with:

```bash
REQUIRED_CHECK_CONTEXT="actual check name" scripts/github/apply_merge_queue_ruleset.sh
```

- The timing summary uses JUnit XML emitted by `cargo nextest`. If you want longer-term timing history, the next step is to ship those XML files or a derived JSON summary to an external store.
