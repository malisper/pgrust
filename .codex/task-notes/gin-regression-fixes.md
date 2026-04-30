Goal:
Fix the remaining upstream gin regression diffs after adding pending-list support.

Key decisions:
- Teach GIN planning to use commuted array containment quals and tune GIN bitmap costs for the regression cases.
- Add the EXPLAIN JSON fields the gin regression checks, including bitmap heap recheck removals.
- Strip temp schema names from non-verbose scan labels.
- Fix VACUUM truncation to flush dirty relation buffers before invalidating them, preserving live rows after large deletes.

Files touched:
- src/backend/access/gin/gin.rs
- src/backend/access/heap/vacuumlazy.rs
- src/backend/commands/explain.rs
- src/backend/executor/nodes.rs
- src/backend/optimizer/path/costsize.rs
- src/backend/parser/analyze/paths.rs
- src/backend/parser/analyze/scope.rs
- src/backend/storage/buffer/bufmgr.rs
- src/include/nodes/execnodes.rs
- src/pgrust/database_tests.rs

Tests run:
- CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-target-washington-v1-ginfix' RUSTC_WRAPPER=/usr/bin/env cargo test --lib --quiet gin
- CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-target-washington-v1-ginfix' RUSTC_WRAPPER=/usr/bin/env cargo test --lib --quiet vacuum_preserves_live_temp_rows_after_large_delete
- scripts/run_regression.sh --skip-build --port 5562 --timeout 300 --schedule /tmp/pgrust_gin_only_schedule --test gin --results-dir '/Volumes/OSCOO PSSD/pgrust/tmp/gin-regression-final-washington-v1'

Remaining:
- Focused gin regression passes 71/71 with no remaining gin.diff.
- A previous default regression harness run was blocked by an unrelated base setup crash in create_index, not by gin output.
