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

use mcx::{Mcx, PgBox, PgVec};
use ::types_core::primitive::Oid;
use ::types_error::PgResult;
use ::nodes::ddlnodes::PartitionBoundSpec;
use ::nodes::nodes::Node;
use ::nodes::nodes::NodePtr;
use ::nodes::partition::{
    PartitionBoundInfo, PartitionBoundInfoData, PartitionDescData, PartitionKeyData,
    PartitionRangeDatumKind,
};
use ::rel::RelationData;
// Canonical value type (`Datum` unification). The partition-routing seams carry
// partition-key tuple values, compared against the canonical `Datum<'mcx>`
// bounds stored in `PartitionBoundInfoData` — so they use the canonical type,
// not the transitional bare-word newtype. None of these seams sit at one of the
// sanctioned bare-word ABI edges (store_att_byval/fetch_att, PGFunction return,
// or the DSM byte-cursor), so the bare-word `datum::Datum` is gone here.
use ::types_tuple::Datum;

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

seam_core::seam!(
    /// `get_qual_from_partbound(parent, spec)` (partbounds.c:249): build the
    /// implicit-AND partition-constraint qual list for a partition with the given
    /// bound `spec` under `parent`, dispatching by the parent's partition
    /// strategy (HASH/LIST/RANGE). Unlike [`qual_from_partbound`], the bound is
    /// supplied directly (not decoded from `relpartbound`) — this is the entry
    /// point `ATExecAttachPartition` uses on the to-be-attached bound. `key` is
    /// the parent's `RelationGetPartitionKey`. Returns the qual list as `Node`s
    /// allocated in `mcx`; the construction can `ereport(ERROR)`, carried on
    /// `Err`.
    ///
    /// `parent_partdesc` is the parent's `RelationGetPartitionDesc(parent,
    /// false)` (partbounds.c reads it directly for a DEFAULT partition's
    /// negated-sibling constraint). It can be `None` when `spec` is not a
    /// default partition (the only path that consults it); the caller — which
    /// holds the parent relcache entry — supplies it for the default case.
    pub fn get_qual_from_partbound<'mcx>(
        mcx: Mcx<'mcx>,
        parent_relid: Oid,
        key: &PartitionKeyData<'_>,
        spec: &PartitionBoundSpec<'_>,
        parent_partdesc: Option<&PartitionDescData<'_>>,
    ) -> PgResult<PgVec<'mcx, Node<'mcx>>>
);

seam_core::seam!(
    /// `partition_bounds_create(boundspecs, nparts, key, &mapping)`
    /// (partbounds.c): build a [`PartitionBoundInfoData`] from a list of
    /// partition bound-spec parse nodes, dispatching by `key->strategy` to
    /// `create_{hash,list,range}_bounds`. Returns the bound info plus the
    /// `*mapping` array (original spec index → canonical partition index). The
    /// comparison/hash support functions and `datumCopy` can `ereport(ERROR)`,
    /// carried on `Err`; allocations are charged to `mcx`.
    pub fn partition_bounds_create<'mcx>(
        mcx: Mcx<'mcx>,
        boundspecs: &[&PartitionBoundSpec<'_>],
        nparts: usize,
        key: &PartitionKeyData<'_>,
    ) -> PgResult<(PartitionBoundInfo<'mcx>, PgVec<'mcx, i32>)>
);

seam_core::seam!(
    /// `check_new_partition_bound(relname, parent, spec, pstate)` (partbounds.c):
    /// check that the new partition's bound `spec` is valid and does not overlap
    /// any existing partition of the parent. The parent's `PartitionKey` and
    /// `PartitionDesc` (whose `boundinfo`/`oids` the overlap search walks) are
    /// passed in by the caller (which holds the parent open). On a conflict or an
    /// empty/invalid range the body `ereport(ERROR)`s, carried on `Err`. All
    /// three strategies (HASH/LIST/RANGE) are covered.
    pub fn check_new_partition_bound<'mcx>(
        mcx: Mcx<'mcx>,
        relname: &str,
        key: &PartitionKeyData<'_>,
        partdesc: &PartitionDescData<'_>,
        spec: &PartitionBoundSpec<'_>,
        pstate: Option<&::nodes::parsestmt::ParseState<'_>>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `check_default_partition_contents(parent, default_rel, new_spec)`
    /// (partbounds.c): when a DEFAULT partition already exists, verify it holds
    /// no row that would now belong to the partition being added (the default's
    /// constraint tightens). Reached ONLY on the `OidIsValid(defaultPartOid)`
    /// leg of `DefineRelation`'s partition block — i.e. when attaching under a
    /// parent that has a default partition.
    ///
    /// UNINSTALLED keystone: the body needs the partition-qual generators
    /// `get_qual_for_list` / `get_qual_for_range` (partbounds.c, unported),
    /// `PartConstraintImpliedByRelConstraint` (tablecmds.c, unported), and the
    /// executor's `ExecPrepareExpr` / `ExecCheck` over a full table scan (also
    /// unported). Until those land this seam stays a loud declared panic; the
    /// common CREATE TABLE PARTITION OF path (no pre-existing default partition)
    /// never reaches it.
    pub fn check_default_partition_contents<'mcx>(
        mcx: Mcx<'mcx>,
        parent: &RelationData<'_>,
        default_rel: &RelationData<'_>,
        new_spec: &PartitionBoundSpec<'_>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `get_range_partbound_string(bound_datums)` (ruleutils.c): a C-string
    /// rendering of one range partition bound, e.g. `(0)` / `(MINVALUE, 5)`.
    /// Deparses each `PartitionRangeDatum` (MINVALUE/MAXVALUE sentinels or a
    /// `get_const_expr`-deparsed `Const`). Installed by the ruleutils owner.
    pub fn get_range_partbound_string<'mcx>(
        mcx: Mcx<'mcx>,
        bound_datums: &[NodePtr<'_>],
    ) -> PgResult<::std::string::String>
);

seam_core::seam!(
    /// `partition_bounds_copy(src, key)` (partbounds.c): return a deep copy of
    /// `src` with bound data types described by `key`. By-reference bound datums
    /// are `datumCopy`'d into `mcx`; the `kind`/`indexes`/`interleaved_parts`
    /// arrays are duplicated. `datumCopy` can `ereport(ERROR)`, carried on
    /// `Err`; allocations are charged to `mcx`.
    pub fn partition_bounds_copy<'mcx>(
        mcx: Mcx<'mcx>,
        src: &PartitionBoundInfoData<'_>,
        key: &PartitionKeyData<'_>,
    ) -> PgResult<PgBox<'mcx, PartitionBoundInfoData<'mcx>>>
);

/* ==========================================================================
 * Partition-bound comparison/merge routines the partitionwise-join machinery
 * (joinrels.c:compute_partition_bounds) drives over the planner's `RelOptInfo`
 * boundinfo. These read the planner-side `pathnodes::PartitionBoundInfoData`
 * (the `RelOptInfo.boundinfo` carrier), distinct from the
 * `::nodes::partition::PartitionBoundInfoData` used by the routing seams
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
        b1: &pathnodes::PartitionBoundInfoData,
        b2: &pathnodes::PartitionBoundInfoData,
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
        root: &mut pathnodes::PlannerInfo,
        rel1: pathnodes::RelId,
        rel2: pathnodes::RelId,
        jointype: pathnodes::JoinType,
    ) -> PgResult<
        ::core::option::Option<(
            pathnodes::PartitionBoundInfoData,
            ::std::vec::Vec<::core::option::Option<pathnodes::RelId>>,
            ::std::vec::Vec<::core::option::Option<pathnodes::RelId>>,
        )>,
    >
);

seam_core::seam!(
    /// `is_dummy_partition(rel, part_index)` (partbounds.c:1842) — has the given
    /// partition of `rel` been proven empty? In C this is `rel->part_rels[i] ==
    /// NULL || IS_DUMMY_REL(rel->part_rels[i])`. The owning unit (joinrels, which
    /// owns `is_dummy_rel`) installs this so the merge routines can test a
    /// partition rel for dummy-ness without depending on the path machinery.
    pub fn is_dummy_rel(root: &pathnodes::PlannerInfo, rel: pathnodes::RelId) -> bool
);
