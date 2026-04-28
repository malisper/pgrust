Goal:
Count recurring failure reasons in /tmp/diffs/next_failures_20260428/xml.diff.

Key decisions:
The user path /tmp/diffs/xml did not exist; used the available xml regression artifact at /tmp/diffs/next_failures_20260428/xml.diff. Counted distinct changed statements/output rows, not only diff hunks.

Files touched:
.codex/task-notes/xml-diff-count.md
src/backend/executor/expr_xml.rs
src/backend/parser/analyze/expr.rs
src/backend/parser/gram.rs
src/backend/tcop/postgres.rs

Tests run:
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test xml --jobs 1 --timeout 60 --results-dir /tmp/pgrust_xml_diag

Remaining:
XML diagnostics bucket fixed. The XML regression still fails on broader XML semantics,
XMLROOT/XMLSERIALIZE behavior, aggregates, view deparse, XMLTABLE plan/output, and
xmltext support. Latest useful diff copied to /tmp/diffs/xml-diagnostics-after.diff.
