Goal:
Speed up the `inherit` regression planning/catalog hot paths so
`scripts/run_regression.sh --test inherit --timeout 60` no longer times out.

Key decisions:
Added planner-local caches for partition specs, child partition bounds, and
bound index expressions/predicates.
Added persistent relcache storage for lowered partition specs; cached entries are
copied into `BoundRelation`, and relcache invalidation naturally discards them.
Added static bootstrap `pg_proc` indexes by OID/name/function mapping to avoid
repeated full-row clones/scans in planner/type-inference helpers.
Added targeted syscache lookups for hot type/cast/drop/view-dependency paths
that previously rebuilt or scanned broader catalog state.
Kept partition pruning conservative: missing or malformed metadata still falls
back to "may match".

Files touched:
`src/backend/optimizer/partition_cache.rs`
`src/backend/optimizer/{inherit.rs,partition_prune.rs,path/allpaths.rs,root.rs,mod.rs}`
`src/include/nodes/pathnodes.rs`
`src/backend/parser/analyze/{mod.rs,partition.rs,functions.rs,scope.rs,create_table.rs}`
`src/backend/utils/cache/{relcache.rs,lsyscache.rs,syscache.rs,visible_catalog.rs}`
`src/include/catalog/pg_proc.rs`
`src/pgrust/database/commands/{drop.rs,dependency_drop.rs,index.rs,partitioned_indexes.rs,partitioned_keys.rs,publication.rs,foreign_data_wrapper.rs,create.rs}`
`src/pgrust/database/{ddl.rs,temp.rs}`
`src/backend/catalog/store/relcache_init.rs`
`src/bin/query_sql_demo.rs`

Tests run:
`cargo fmt`
`scripts/cargo_isolated.sh check`
`scripts/cargo_isolated.sh test --lib --quiet indexed_builtin_proc_helpers_match_bootstrap_rows`
`CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm CARGO_TARGET_DIR=/tmp/pgrust-target-inherit-llvm scripts/run_regression.sh --test inherit --timeout 60 --jobs 1 --port 64119`
`CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm CARGO_TARGET_DIR=/tmp/pgrust-target-inherit-llvm scripts/run_regression.sh --test inherit --timeout 60 --jobs 1 --port 64336`
`CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm CARGO_TARGET_DIR=/tmp/pgrust-target-inherit-llvm scripts/run_regression.sh --test inherit --timeout 60 --jobs 1 --port 64579`

Sample output:
`/tmp/pgrust-inherit-after-relcache.sample.txt`

Remaining:
The first post-cache 60s run still timed out at `572/884`, but the following
sampled and clean reruns completed under the 60s file timeout as functional
failures: `620/884` matched, `3324` diff lines.
The clean final results directory is
`/var/folders/tc/1psz8_jd0hnfmgyyr0n2wtzh0000gn/T//pgrust_regress_results.yokohama-v2.gVPrT9`.
The latest sample no longer shows the old dominant
`bootstrap_pg_proc_rows`, `relation_partition_spec`, or
`bind_index_exprs_uncached` stacks; remaining time is spread across parser
work, syscache index probes, and storage/index I/O.
