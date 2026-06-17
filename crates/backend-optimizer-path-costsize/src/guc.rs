//! costsize.c-owned scan/join `enable_*` cost GUCs — guc-table slot install.
//!
//! In `costsize.c` these booleans are module globals (`bool enable_seqscan`,
//! ...) registered with the GUC machinery. The fabled GUC engine reads/writes a
//! variable through the [`GucVarAccessors`] its owning unit installs into the
//! named slot; costsize.c is that owner. Each GUC is mirrored here as a
//! process-private `thread_local!` cell (the C `conf->variable` backing store),
//! initialized to its boot-time default, with `get`/`set` accessors the GUC
//! engine drives. Consumers (allpaths.c `create_tidscan_paths`,
//! joinrels.c `join_enable_flags`, ...) read them via `vars::enable_*.read()`.

use backend_utils_misc_guc_tables::vars;
use backend_utils_misc_guc_tables::GucVarAccessors;

macro_rules! enable_guc {
    ($cell:ident, $get:ident, $set:ident, $default:expr) => {
        std::thread_local! {
            static $cell: core::cell::Cell<bool> = const { core::cell::Cell::new($default) };
        }
        fn $get() -> bool {
            $cell.with(core::cell::Cell::get)
        }
        fn $set(value: bool) {
            $cell.with(|c| c.set(value));
        }
    };
}

// Scan-method GUCs (costsize.c, default ON).
enable_guc!(SEQSCAN, get_seqscan, set_seqscan, true);
enable_guc!(INDEXSCAN, get_indexscan, set_indexscan, true);
enable_guc!(INDEXONLYSCAN, get_indexonlyscan, set_indexonlyscan, true);
enable_guc!(BITMAPSCAN, get_bitmapscan, set_bitmapscan, true);
enable_guc!(TIDSCAN, get_tidscan, set_tidscan, true);
// Sort / aggregation GUCs.
enable_guc!(SORT, get_sort, set_sort, true);
enable_guc!(INCREMENTAL_SORT, get_incremental_sort, set_incremental_sort, true);
enable_guc!(HASHAGG, get_hashagg, set_hashagg, true);
// Join-method GUCs.
enable_guc!(NESTLOOP, get_nestloop, set_nestloop, true);
enable_guc!(MATERIAL, get_material, set_material, true);
enable_guc!(MEMOIZE, get_memoize, set_memoize, true);
enable_guc!(MERGEJOIN, get_mergejoin, set_mergejoin, true);
enable_guc!(HASHJOIN, get_hashjoin, set_hashjoin, true);
enable_guc!(GATHERMERGE, get_gathermerge, set_gathermerge, true);
// Parallel GUCs.
enable_guc!(PARALLEL_APPEND, get_parallel_append, set_parallel_append, true);
enable_guc!(PARALLEL_HASH, get_parallel_hash, set_parallel_hash, true);
// Partitionwise GUCs (default OFF, costsize.c).
enable_guc!(PARTITIONWISE_JOIN, get_partitionwise_join, set_partitionwise_join, false);

/// Install every costsize.c-owned `enable_*` GUC slot. Called once from
/// `init_seams()` at single-threaded startup.
pub(crate) fn install_enable_gucs() {
    vars::enable_seqscan.install(GucVarAccessors { get: get_seqscan, set: set_seqscan });
    vars::enable_indexscan.install(GucVarAccessors { get: get_indexscan, set: set_indexscan });
    vars::enable_indexonlyscan
        .install(GucVarAccessors { get: get_indexonlyscan, set: set_indexonlyscan });
    vars::enable_bitmapscan.install(GucVarAccessors { get: get_bitmapscan, set: set_bitmapscan });
    vars::enable_tidscan.install(GucVarAccessors { get: get_tidscan, set: set_tidscan });
    vars::enable_sort.install(GucVarAccessors { get: get_sort, set: set_sort });
    vars::enable_incremental_sort
        .install(GucVarAccessors { get: get_incremental_sort, set: set_incremental_sort });
    vars::enable_hashagg.install(GucVarAccessors { get: get_hashagg, set: set_hashagg });
    vars::enable_nestloop.install(GucVarAccessors { get: get_nestloop, set: set_nestloop });
    vars::enable_material.install(GucVarAccessors { get: get_material, set: set_material });
    vars::enable_memoize.install(GucVarAccessors { get: get_memoize, set: set_memoize });
    vars::enable_mergejoin.install(GucVarAccessors { get: get_mergejoin, set: set_mergejoin });
    vars::enable_hashjoin.install(GucVarAccessors { get: get_hashjoin, set: set_hashjoin });
    vars::enable_gathermerge
        .install(GucVarAccessors { get: get_gathermerge, set: set_gathermerge });
    vars::enable_parallel_append
        .install(GucVarAccessors { get: get_parallel_append, set: set_parallel_append });
    vars::enable_parallel_hash
        .install(GucVarAccessors { get: get_parallel_hash, set: set_parallel_hash });
    vars::enable_partitionwise_join
        .install(GucVarAccessors { get: get_partitionwise_join, set: set_partitionwise_join });
}
