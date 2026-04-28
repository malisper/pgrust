Goal:
Implement PG-style GiST build optimizations for create_index profiling.

Key decisions:
- GiST build writes use build write modes that install page images with a build
  LSN, skip per-page GiST WAL, and log relation-range full-page images after
  build. Normal post-build GiST inserts still use the normal WAL path.
- GiST reloptions now accept and persist `WITH (buffering = auto|on|off)`.
- `GistBuildBuffers` is now PG-shaped: node buffers are keyed by internal block,
  backed by a shared temp file, tracked by level, parent map, emptying queue, and
  split-relocation state.
- `buffering=on` follows PG's stats phase by inserting the first 4096 tuples
  normally, then initializing buffering from the existing tree.
- Build allocation uses `reserve_block` so GiST build avoids zero-page `pwrite`
  extension; an attempted logical-only reserve was rejected because it caused
  intermittent create_index regressions.
- Repeated non-buffered GiST insert now has a PG-like fast path for leaf appends
  that fit: append into the page image and merge the downlink union instead of
  decoding/rebuilding every leaf tuple.
- Point GiST proc 11 is catalog-wired with a PostgreSQL-style z-order
  sortsupport comparator, enabling sorted build for point GiST indexes.

Files touched:
- src/include/access/gist.rs
- src/backend/access/gist/{build.rs,build_buffers.rs,insert.rs,page.rs}
- src/backend/access/gist/support/{mod.rs,point_ops.rs}
- src/backend/storage/smgr/{any.rs,md.rs,mem.rs,smgr.rs}
- src/include/catalog/{pg_amproc.rs,pg_proc.rs}
- catalog/relcache/create-index option plumbing and tests
- src/pgrust/database_tests.rs

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh check
- scripts/cargo_isolated.sh test --lib --quiet gist
- scripts/cargo_isolated.sh test --lib --quiet resolve_gist_options
- scripts/cargo_isolated.sh test --lib --quiet create_gist_box_index_with_forced_buffering_matches_queries
- scripts/cargo_isolated.sh build --release --bin pgrust_server
- scripts/run_regression.sh --skip-build --schedule .context/create_index_only.schedule --jobs 1 --port 55557 --timeout 300 --results-dir /tmp/pgrust_create_index_pgstyle_final

Performance/profile:
- Final create_index run: 557/687 matched, 1771 diff lines, real 54.68s.
- Earlier corrected pre-leaf-fast run on this branch was 103.44s with the same
  557/687 match count, so the leaf fast path made the material difference.
- Previous origin/perf-optimization reference run recorded earlier was 67.55s
  with 561/687 matched.
- Useful final-shape sample: /tmp/pgrust_create_index_pgstyle_leaf_fast_profile.sample.txt
  It no longer shows per-page GiST WAL as dominant; `log_gist_record` appears
  only under final range logging. GiST build allocation uses `reserve_block`
  instead of `ensure_block_exists -> extend -> pwrite`.

Remaining:
- Existing create_index semantic diffs remain; latest stable match count is
  unchanged for this branch at 557/687.
- `create_index` still has non-GiST storage/WAL hotspots, especially hash and
  catalog/storage paths.
