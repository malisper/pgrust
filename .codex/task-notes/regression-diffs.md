Goal:
Fix the remaining regression failures from the downloaded GitHub diffs for:
database, expressions, float8, hash_index, jsonpath, multirangetypes, mvcc,
name, object_address, oidjoins, rangefuncs, timestamp, timestamptz, tstypes,
and xmlmap.

Key decisions:
- Bind `^` as an operator-like power expression so uncast literals choose the
  PostgreSQL-compatible float8 path while explicit numeric power remains numeric.
- Build expression indexes through the normal index AM path, and avoid no-key
  hash index scans because pgrust hash scans require usable scan keys.
- Add a PL/pgSQL exception-block write-xid override so catalog reads still see
  the parent transaction while inner writes are abortable under the subxid.
- Carry static `FOR record IN SELECT ... LOOP` descriptors for oidjoins record
  field and array-subscript binding; shim pg_get_catalog_foreign_keys ordering.
- Preserve PostgreSQL FROM order for multiple generate_series scans and report
  stored-view positional type mismatch before the dropped-column heuristic.
- Coerce comparison string literals to peer range/multirange types for equality
  and avoid multirange hash index probes until hash support is canonicalized.

Files touched:
Parser/binder/planner/executor/rewrite/PLpgSQL/catalog paths, especially:
src/backend/parser/gram.rs
src/backend/parser/analyze/expr.rs
src/backend/parser/analyze/expr/ops.rs
src/backend/parser/analyze/infer.rs
src/backend/optimizer/path/allpaths.rs
src/backend/optimizer/path/costsize.rs
src/backend/executor/mod.rs
src/backend/commands/tablecmds.rs
src/backend/rewrite/views.rs
src/pgrust/session.rs
src/pgrust/database/commands/index.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/run_regression.sh for the 15-file original set:
  /tmp/pgrust-krakow-v6-rerun-1777583319
  15/15 files passed, 2696/2696 queries matched.
scripts/run_regression.sh for focused files:
  float8, hash_index, mvcc, oidjoins passed before /tmp reported ENOSPC while
  writing rangefuncs status.
scripts/run_regression.sh --test rangefuncs:
  /Volumes/OSCOO PSSD/pgrust/tmp/pgrust-krakow-v6-rerun-1777584161
  rangefuncs passed, 437/437 queries matched.

Remaining:
No known regression mismatches in the requested set. Local /tmp space is tight;
use an explicit CARGO_TARGET_DIR/results dir outside /tmp for large follow-up runs.
