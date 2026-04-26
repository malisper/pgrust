Goal:
Rerun PostgreSQL `rowsecurity` regression and diagnose why it fails.

Key decisions:
Used a dedicated target dir because the shared `/private/tmp/pgrust-target`
incremental build raced. Used `--jobs 1`, explicit high port, and
`--timeout 300` because the default parallel base setup hit port 5433 and then
timed out during bootstrap.

Files touched:
No source files changed. Added this task note.

Tests run:
`CARGO_TARGET_DIR=/tmp/pgrust-target-rowsecurity scripts/run_regression.sh --skip-build --jobs 1 --port 55434 --timeout 300 --test rowsecurity`

Remaining:
Actual result is FAIL: 514/774 queries matched, 4424 diff lines.
Artifacts: `/var/folders/tc/1psz8_jd0hnfmgyyr0n2wtzh0000gn/T//pgrust_regress_results.marseille.Urt6uB`.
First mismatch is `\dp` showing shared bootstrap public tables in addition to
the rowsecurity schema. Other large buckets include unsupported TABLESAMPLE,
prepared statements/EXECUTE, MERGE, ON CONFLICT DO UPDATE with RLS, COPY/RLS,
view/security-barrier behavior, partitioned table metadata/display, and some
RLS recursion/dependency behavior.
