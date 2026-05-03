Goal:
Investigate whether the indexed update/delete timeout could be caused by a deadlock or PostgreSQL mismatch.

Key decisions:
- Focused on the failing contention test `concurrent_indexed_updates_and_deletes_keep_index_results_correct`.
- Heap update/delete paths drop heap page locks before transaction waits, so they are less suspicious.
- The main suspicious mismatch is nbtree incomplete split handling.
- PostgreSQL keeps the left child page write-locked after `_bt_split()` until parent insertion finishes and clears `INCOMPLETE_SPLIT`.
- pgrust writes the split-left page, drops its page lock, then later calls `propagate_split_upwards()` and `clear_incomplete_split()`.
- pgrust `finish_incomplete_split()` reads the left page without first owning the left page write lock, so two writers can observe the same incomplete split and race to complete it.
- Added a forced concurrent split test. The first locking fix reproduced a real self-deadlock: while holding the split-left page write lock, `create_new_root()` called `page_lower_bound()` and tried to read-lock the same page.
- Fixed by carrying the split-left lower bound in `PageSplitResult`, so root creation no longer re-locks a page already locked by the splitter.
- Final shape keeps the split-left page locked until the parent downlink is inserted and `INCOMPLETE_SPLIT` is cleared.

Files touched:
- .codex/task-notes/btree-incomplete-split-race.md
- crates/pgrust_access/src/nbtree/mod.rs
- crates/pgrust_access/src/nbtree/runtime.rs
- src/pgrust/database_tests.rs

Tests run:
- `scripts/cargo_isolated.sh test --lib --quiet concurrent_btree_splits_complete_once`
  - Reproduced timeout with first partial locking fix at 120s.
  - Passed after carrying the left lower bound and avoiding self-read-lock.
- `sample <pid> 10 -file /tmp/pgrust_btree_deadlock_sample.txt`
- `scripts/cargo_isolated.sh check -p pgrust_access --message-format short`
- `scripts/cargo_isolated.sh test -p pgrust_access --quiet`
- `scripts/cargo_isolated.sh test --lib --quiet concurrent_indexed_updates_and_deletes_keep_index_results_correct`
- `scripts/cargo_isolated.sh check --message-format short`
- `cargo fmt --all -- --check`

Remaining:
- Consider replacing the debug-only split pause knob with a narrower scoped test hook if future tests need more deterministic thread coordination.
