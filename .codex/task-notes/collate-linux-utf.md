Goal:
Run the `collate.linux.utf8` PostgreSQL regression against pgrust, copy diffs to `/tmp/diffs`, and identify the first failure.

Key decisions:
Initial targeted runner:
`scripts/run_regression.sh --test collate.linux.utf8 --results-dir /tmp/pgrust_collate_linux_utf8_port55433 --timeout 300 --jobs 1 --port 55433 --skip-build`

Port 5433 was already occupied by another `pgrust_server`, so the successful run used port 55433.

The first mismatch is the skip probe:
`pg_char_to_encoding('UTF8')` is missing, so `\gset` does not set `skip_test`; psql then treats `:skip_test` as an invalid boolean and continues through the Linux locale body. The selected upstream expected file is `collate.linux.utf8_1.out`, which stops after `\quit`.

Files touched:
`.codex/task-notes/collate-linux-utf.md`
`src/include/nodes/primnodes.rs`
`src/include/catalog/pg_proc.rs`
`src/backend/parser/analyze/functions.rs`
`src/backend/parser/analyze/infer.rs`
`src/backend/executor/exec_expr.rs`
`src/backend/executor/tests.rs`

Tests run:
`collate.linux.utf8`: failed, 0/211 queries matched, 800 diff lines.
`scripts/cargo_isolated.sh test --lib --quiet getdatabaseencoding_and_jsonpath_unicode_work`: passed.
`scripts/run_regression.sh --test collate.linux.utf8 --results-dir /tmp/pgrust_collate_linux_utf8_pg_char_rebuilt --timeout 300 --jobs 1 --port 55435`: passed, 211/211 queries matched.
`scripts/run_regression.sh --test collate.linux.utf8 --results-dir /tmp/pgrust_collate_linux_utf8_final --timeout 300 --jobs 1 --port 55436`: passed, 211/211 queries matched after final cleanup.

Diff artifacts:
No current Linux diff artifact remains because the rebuilt regression passes.
`/tmp/diffs/collate.linux.utf8.out`
`/tmp/diffs/collate.linux.utf8.summary.json`

Remaining:
Longer-term collation support remains limited: bootstrap `pg_collation` currently has only `default`, `C`, and `POSIX`. The Linux regression passes locally by taking the upstream skip path.
