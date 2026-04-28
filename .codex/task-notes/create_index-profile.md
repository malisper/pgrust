Goal:
Profile why the `create_index` regression test is slow using macOS `sample`.

Key decisions:
Used a one-test schedule file instead of `--test create_index` because
`scripts/run_regression.sh --test` switches to a debug server. Sampled the
release `pgrust_server` process started by the normal regression harness.

Files touched:
- .context/create_index_only.schedule
- .codex/task-notes/create_index-profile.md

Tests run:
- `scripts/cargo_isolated.sh build --release --features tools --bin regression_profile`
- `scripts/cargo_isolated.sh build --release --bin pgrust_server`
- `/usr/bin/time -p scripts/run_regression.sh --skip-build --schedule .context/create_index_only.schedule --jobs 1 --port 55531 --timeout 300 --results-dir /tmp/pgrust_create_index_baseline`
- `sample <pgrust_server pid> 20 1 -file /tmp/pgrust_create_index.sample.txt` during the same one-test schedule on port 55532.

Remaining:
Hot path is GiST build for the geometry index section of `create_index.sql`.
Likely next step is optimizing GiST buffered build/block extension/WAL write
behavior, then rerunning the same sample command.
