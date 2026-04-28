Goal:
Diagnose why PostgreSQL regression btree_index times out in pgrust.

Key decisions:
- Branch renamed immediately to malisper/btree-index-timeout.
- Ran only btree_index regression, using alternate ports because 5433 was occupied.
- First normal run never reached btree_index: dependency setup create_index failed.
- Forced run with --ignore-deps showed btree_index itself times out after 111/133 matched queries.

Files touched:
- .codex/task-notes/btree-index-timeout.md

Tests run:
- scripts/run_regression.sh --test btree_index --jobs 1 --timeout 90 --port 55433 --skip-build --results-dir /tmp/pgrust-btree-index-timeout
  Result: create_index dependency failed before btree_index body.
- scripts/run_regression.sh --test btree_index --ignore-deps --jobs 1 --timeout 90 --port 55434 --skip-build --results-dir /tmp/pgrust-btree-index-ignore-deps
  Result: btree_index TIMEOUT (111/133 queries matched).

Remaining:
- Main btree timeout is at ALTER TABLE delete_test_table ADD PRIMARY KEY (a,b,c,d) after INSERT ... generate_series(1,80000).
- Dependency setup create_index separately hits statement_timeout at CREATE INDEX ggpolygonind ON gpolygon_tbl USING gist (f1).
- Earlier btree_index output reports btree WAL log failed: WAL I/O error: No space left on device during the 1350-iteration delete/insert loop, despite df showing 66GiB free; investigate WAL writer/record size or stale error propagation if fixing.

Profile update:
- Added src/bin/btree_pk_build_profile.rs and Cargo.toml bin target.
- Release run for 80k rows: create 51ms, insert 2424ms, ALTER ADD PRIMARY KEY 620ms.
- Dev run for 80k rows: create 730ms, insert 8522ms, ALTER ADD PRIMARY KEY 5043ms.
- Profiled dev ALTER with macOS sample: /tmp/pgrust_btree_pk_sample_alter.txt.
- Sample breakdown during ALTER: most active samples under build_simple_index_in_transaction -> btbuild -> build_btree_pages -> build_leaf_pages -> group_sorted_tuples_into_pages -> append_page_or_finish. Secondary hotspot is encode_key_payload. Sorting/unique checking barely appear.
- Conclusion: regression timeout is a dev-profile artifact around btree leaf page construction and debug checked slice/copy overhead. Release build does not reproduce.

Dev speedup patch:
- Changed nbtree bulk page construction to append tuples directly into build pages and write the page header once per page.
- Dev btree_pk_build_profile after patch, 80k rows, 3 iterations: ALTER ADD PRIMARY KEY 3248ms, 3462ms, 3428ms. Before patch: about 5043ms in the direct repro and 5964ms in sampled run.
- Full btree_index --ignore-deps still times out, but later: after ALTER ADD PRIMARY KEY, at DELETE FROM delete_test_table WHERE a < 79990. It now reaches 113/133 queries instead of 111/133.
- Focused tests passed: create_index_builds_multilevel_btree_root; create_index_builds_ready_valid_btree_and_explain_uses_it.
