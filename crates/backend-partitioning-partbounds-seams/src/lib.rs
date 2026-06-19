//! Seam declarations for the `backend-partitioning-partbounds` unit
//! (`partitioning/partbounds.c`).
//!
//! Includes the partition-bound search routines `execPartition.c`'s
//! `get_partition_for_tuple` calls, and the `relpartbound`-to-qual builder
//! reached from partcache.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! C's `FmgrInfo *partsupfunc` / `Oid *partcollation` arrays and the
//! `PartitionBoundInfo` come from the partitioned table's relcache entry, so
//! the caller passes the owned `PartitionKeyData` / `PartitionBoundInfoData`
//! views; the support functions dispatch by their stored lookup key.

use mcx::{Mcx, PgVec};
use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::nodes::Node;
use types_nodes::partition::{
    PartitionBoundInfoData, PartitionKeyData, PartitionRangeDatumKind,
};
use types_rel::RelationData;
// Canonical value type (`Datum` unification). The partition-routing seams carry
// partition-key tuple values, compared against the canonical `Datum<'mcx>`
// bounds stored in `PartitionBoundInfoData` — so they use the canonical type,
// not the transitional bare-word newtype. None of these seams sit at one of the
// sanctioned bare-word ABI edges (store_att_byval/fetch_att, PGFunction return,
// or the DSM byte-cursor), so the bare-word `types_datum::Datum` is gone here.
use types_tuple::Datum;

seam_core::seam!(
    /// `compute_partition_hash_value(partnatts, partsupfunc, partcollation,
    /// values, isnull)` (partbounds.c): the combined hash of the partition-key
    /// values for HASH routing. The support functions can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn compute_partition_hash_value<'mcx>(
        key: &PartitionKeyData<'_>,
        values: &[Datum<'mcx>],
        isnull: &[bool],
    ) -> PgResult<u64>
);

seam_core::seam!(
    /// `partition_list_bsearch(partsupfunc, partcollation, boundinfo, value,
    /// &is_equal)` (partbounds.c): binary-search a LIST partition's bounds for
    /// `value`, returning `(bound_offset, is_equal)` (`bound_offset == -1` when
    /// below all bounds). The comparison function can `ereport(ERROR)`.
    pub fn partition_list_bsearch<'mcx>(
        key: &PartitionKeyData<'_>,
        boundinfo: &PartitionBoundInfoData<'_>,
        value: Datum<'mcx>,
    ) -> PgResult<(i32, bool)>
);

seam_core::seam!(
    /// `partition_range_datum_bsearch(partsupfunc, partcollation, boundinfo,
    /// nvalues, values, &is_equal)` (partbounds.c): binary-search a RANGE
    /// partition's bounds for the key tuple, returning `(bound_offset,
    /// is_equal)`. The comparison function can `ereport(ERROR)`.
    pub fn partition_range_datum_bsearch<'mcx>(
        key: &PartitionKeyData<'_>,
        boundinfo: &PartitionBoundInfoData<'_>,
        nvalues: i32,
        values: &[Datum<'mcx>],
    ) -> PgResult<(i32, bool)>
);

seam_core::seam!(
    /// `partition_rbound_datum_cmp(partsupfunc, partcollation, rb_datums,
    /// rb_kind, tuple_datums, n_tuple_datums)` (partbounds.c): compare a range
    /// bound against the key tuple (`<0`, `0`, `>0`). The comparison function
    /// can `ereport(ERROR)`. C reads `partsupfunc[i]` / `partcollation[i]` from
    /// the partitioned table's relcache `PartitionKey`, so the owned model
    /// passes the `PartitionKeyData` view (the per-key comparison support
    /// functions and collations live on it).
    pub fn partition_rbound_datum_cmp<'mcx>(
        key: &PartitionKeyData<'_>,
        rb_datums: &[Datum<'mcx>],
        rb_kind: &[PartitionRangeDatumKind],
        tuple_datums: &[Datum<'mcx>],
        n_tuple_datums: i32,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `FunctionCall2Coll(&key->partsupfunc[0], key->partcollation[0],
    /// last_datum, value)` for a LIST partition's cached-find double-check
    /// (`get_partition_for_tuple` in execPartition.c): compare the last-found
    /// LIST bound datum against the new key datum using the partition's first
    /// support (comparison) function. The comparison function can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn partition_list_datum_cmp<'mcx>(
        key: &PartitionKeyData<'_>,
        last_datum: Datum<'mcx>,
        value: Datum<'mcx>,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// The `relpartbound`-to-qual leg of `generate_partition_qual`
    /// (partcache.c): `SearchSysCache1(RELOID, relid)` (→ `elog(ERROR, "cache
    /// lookup failed for relation %u")` as `Err`), `SysCacheGetAttr(RELOID,
    /// ..., relpartbound, &isnull)`, and when not null `castNode(
    /// PartitionBoundSpec, stringToNode(TextDatumGetCString(boundDatum)))`
    /// then `get_qual_from_partbound(parent, bound)` (partbounds.c). Returns
    /// the implicit-AND qual list `my_qual` (empty when `relpartbound` is
    /// null), allocated in `mcx`. `Err` carries the cache-lookup failure, the
    /// bound-parse errors, and OOM.
    pub fn qual_from_partbound<'mcx, 'p>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        parent: &RelationData<'p>,
    ) -> PgResult<PgVec<'mcx, Node<'mcx>>>
);

/* ==========================================================================
 * Partition-bound comparison/merge routines the partitionwise-join machinery
 * (joinrels.c:compute_partition_bounds) drives over the planner's `RelOptInfo`
 * boundinfo. These read the planner-side `types_pathnodes::PartitionBoundInfoData`
 * (the `RelOptInfo.boundinfo` carrier), distinct from the
 * `types_nodes::partition::PartitionBoundInfoData` used by the routing seams
 * above. (Additive — appended for joinrels.)
 * ======================================================================== */

seam_core::seam!(
    /// `partition_bounds_equal(partnatts, parttyplen, parttypbyval, b1, b2)`
    /// (partbounds.c) — do two partition-bound descriptors describe exactly the
    /// same bounds?
    pub fn partition_bounds_equal(
        partnatts: i32,
        parttyplen: &[i16],
        parttypbyval: &[bool],
        b1: &types_pathnodes::PartitionBoundInfoData,
        b2: &types_pathnodes::PartitionBoundInfoData,
    ) -> bool
);
seam_core::seam!(
    /// `partition_bounds_merge(partnatts, partsupfunc, partcollation, rel1, rel2,
    /// jointype, &parts1, &parts2)` (partbounds.c) — merge the partition bounds
    /// of the two input rels for a join of the given type. Returns
    /// `Some((merged_boundinfo, parts1, parts2))` with the per-segment partition
    /// pairings, or `None` if the bounds are not mergeable. The support functions
    /// / collations are read from the inputs' shared partition scheme by the
    /// owner.
    pub fn partition_bounds_merge(
        root: &mut types_pathnodes::PlannerInfo,
        rel1: types_pathnodes::RelId,
        rel2: types_pathnodes::RelId,
        jointype: types_pathnodes::JoinType,
    ) -> PgResult<
        ::core::option::Option<(
            types_pathnodes::PartitionBoundInfoData,
            ::std::vec::Vec<::core::option::Option<types_pathnodes::RelId>>,
            ::std::vec::Vec<::core::option::Option<types_pathnodes::RelId>>,
        )>,
    >
);
