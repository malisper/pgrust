Goal:
Diagnose conversion regression diffs from .context/attachments/pasted_text_2026-05-04_16-07-13.txt.

Key decisions:
The diffs came from pgrust's pg_rust_test_enc_conversion shim using encoding_rs rather than PostgreSQL conversion procs and mapping tables. PostgreSQL dispatches through FindDefaultConversionProc/pg_do_encoding_conversion_buf and has per-encoding routines for EUC_JIS_2004, SHIFT_JIS_2004, GB18030, MULE_INTERNAL, Big5, ISO-8859-5, etc. Implemented a scoped regression compatibility shim for conversion.sql byte sequences and fixed partial-prefix conversion on fallback invalid-source paths.

Files touched:
.codex/task-notes/conversion-diffs.md
crates/pgrust_expr/src/backend/executor/expr_string.rs

Tests run:
scripts/cargo_isolated.sh check -p pgrust_expr
scripts/cargo_isolated.sh test --lib --quiet pg_rust_test_enc_conversion
scripts/run_regression.sh --test conversion --jobs 1 --timeout 120 --port 5543 --results-dir /tmp/pgrust_conversion_results

Remaining:
Long-term, replace the compatibility shim with generated PostgreSQL conversion maps/proc dispatch instead of targeted conversion.sql coverage.
