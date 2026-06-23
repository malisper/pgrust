//! Per-relation cumulative-statistics gate (`pgstat_relation.c`).
//!
//! Only [`pgstat_init_relation`] is ported here so far: it is the relation-open
//! gate that decides whether a freshly-built relcache entry should have its
//! statistics counted. The full per-relation pending-stats data path (the count
//! macros, `pgstat_create_relation` / `pgstat_drop_relation` /
//! `pgstat_unlink_relation`) remains seam-and-panic until the rest of
//! `pgstat_relation.c` lands.

use types_tuple::access::{
    RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE,
    RELKIND_TOASTVALUE,
};

/// `RELKIND_HAS_STORAGE(relkind)` (catalog/pg_class.h).
fn relkind_has_storage(relkind: u8) -> bool {
    relkind == RELKIND_RELATION
        || relkind == RELKIND_INDEX
        || relkind == RELKIND_SEQUENCE
        || relkind == RELKIND_TOASTVALUE
        || relkind == RELKIND_MATVIEW
}

/// `pgstat_init_relation(Relation rel)` (pgstat_relation.c).
///
/// Returns whether this relation's cumulative statistics should be counted —
/// the value C stores into `rel->pgstat_enabled`. The seam was re-signed to
/// return the bit (rather than mutate the relation in place), which the
/// relation-open caller stores via `set_pgstat_enabled`.
///
/// ```c
/// char relkind = rel->rd_rel->relkind;
/// if (!RELKIND_HAS_STORAGE(relkind) && relkind != RELKIND_PARTITIONED_TABLE) {
///     rel->pgstat_enabled = false; rel->pgstat_info = NULL; return;
/// }
/// if (!pgstat_track_counts) {
///     if (rel->pgstat_info) pgstat_unlink_relation(rel);
///     rel->pgstat_enabled = false; rel->pgstat_info = NULL; return;
/// }
/// rel->pgstat_enabled = true;
/// ```
///
/// The `pgstat_info` / `pgstat_unlink_relation` side effects collapse into the
/// returned bit here: `pgstat_init_relation` is only ever called on a
/// freshly-built relation (`RelationBuildDesc`), whose `pgstat_info` is NULL, so
/// the `pgstat_unlink_relation` branch is unreachable and the `pgstat_info =
/// NULL` writes are no-ops. `relid` is carried for symmetry with C's
/// OID-keyed pending bookkeeping but is unused on this gate path.
pub fn pgstat_init_relation(_relid: types_core::primitive::Oid, relkind: u8) -> bool {
    // We only count stats for relations with storage and partitioned tables.
    if !relkind_has_storage(relkind) && relkind != RELKIND_PARTITIONED_TABLE {
        return false;
    }

    // ... and only when we're counting at all.
    if !crate::guc::track_counts() {
        return false;
    }

    true
}
