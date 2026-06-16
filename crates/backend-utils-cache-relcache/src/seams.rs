//! Seam installation — this unit OWNS `backend-utils-cache-relcache-seams`.
//!
//! [`init_seams`] installs every seam declared there (the inward wiring). The
//! adapter functions below match each seam's declared `Signature` exactly and
//! bridge the cross-unit value-slice (`types_rel::RelationData`/`Relation`,
//! `Oid`) to this crate's owned entry store + family logic. Where a genuine
//! cross-unit owner is not yet ported the adapter calls that owner's real
//! `seam!()` (panics until the owner installs it — `mirror-pg-and-panic`); the
//! substrate (core entry store) is real and its adapters resolve against it.

#![allow(unused_variables)]

use backend_utils_cache_relcache_seams as sx;

use mcx::{Mcx, PgBox, PgVec};
use types_core::primitive::{AttrNumber, Oid, RegProcedure};
use types_core::{MultiXactId, SubTransactionId, TransactionId};
use types_error::{PgError, PgResult};

/// Install every relcache seam. Called once from `seams-init::init_all`.
pub fn init_seams() {
    // --- core-entry-store ---
    sx::relation_id_get_relation::set(relation_id_get_relation);
    sx::relation_id_get_relation_shared::set(relation_id_get_relation_shared);
    sx::relation_close::set(relation_close);
    sx::relation_rd_tableam::set(relation_rd_tableam);
    sx::relation_rd_tableam_by_oid::set(relation_rd_tableam_by_oid);
    sx::relation_needs_wal::set(relation_needs_wal);
    sx::relation_is_accessible_in_logical_decoding::set(relation_is_accessible_in_logical_decoding);
    sx::relation_is_local::set(relation_is_local);
    sx::relation_rd_indam::set(relation_rd_indam);
    sx::relation_increment_reference_count::set(relation_increment_reference_count);
    sx::relation_decrement_reference_count::set(relation_decrement_reference_count);
    sx::rd_support_at::set(rd_support_at);
    sx::index_getprocinfo::set(index_getprocinfo);
    sx::index_opclass_missing_options_error::set(index_opclass_missing_options_error);
    sx::create_fake_relcache_entry::set(create_fake_relcache_entry);
    sx::free_fake_relcache_entry::set(free_fake_relcache_entry);

    // --- invalidate ---
    sx::at_eoxact_relation_cache::set(at_eoxact_relation_cache);
    sx::at_eosubxact_relation_cache::set(at_eosubxact_relation_cache);
    sx::relation_cache_invalidate::set(relation_cache_invalidate);
    sx::relation_cache_invalidate_entry::set(relation_cache_invalidate_entry);

    // --- derived ---
    sx::relation_get_identity_key_bitmap::set(relation_get_identity_key_bitmap);
    sx::relation_get_index_attr_bitmap::set(relation_get_index_attr_bitmap);
    sx::relation_get_index_list::set(relation_get_index_list);

    // --- partition cache read/write (owned per-entry state; build via partcache) ---
    sx::relation_get_partkey::set(relation_get_partkey);
    sx::relation_set_partkey::set(relation_set_partkey);
    sx::relation_get_partcheck::set(relation_get_partcheck);
    sx::relation_set_partcheck::set(relation_set_partcheck);
    sx::relation_get_composite_tupdesc::set(relation_get_composite_tupdesc);

    // --- initfile ---
    sx::relation_id_is_in_init_file::set(relation_id_is_in_init_file);
    sx::relation_cache_init_file_pre_invalidate::set(relation_cache_init_file_pre_invalidate);
    sx::relation_cache_init_file_post_invalidate::set(relation_cache_init_file_post_invalidate);
    sx::relation_cache_initialize::set(relation_cache_initialize);
    sx::relation_cache_initialize_phase2::set(relation_cache_initialize_phase2);
    sx::relation_cache_initialize_phase3::set(relation_cache_initialize_phase3);

    // --- CLUSTER rd_rel / rd_index / rd_indam field reads + transient sets ---
    sx::rd_rel_relam::set(rd_rel_relam);
    sx::rd_rel_reltablespace::set(rd_rel_reltablespace);
    sx::rd_rel_relowner::set(rd_rel_relowner);
    sx::rd_rel_relisshared::set(rd_rel_relisshared);
    sx::rd_rel_relnamespace::set(rd_rel_relnamespace);
    sx::rd_rel_relfrozenxid::set(rd_rel_relfrozenxid);
    sx::rd_rel_relminmxid::set(rd_rel_relminmxid);
    sx::rd_islocaltemp::set(rd_islocaltemp);
    sx::rd_index_indrelid::set(rd_index_indrelid);
    sx::rd_index_indisvalid::set(rd_index_indisvalid);
    sx::rd_index_has_indpred::set(rd_index_has_indpred);
    sx::relation_build_local_relation::set(crate::initfile::RelationBuildLocalRelation);
    sx::rd_index_indkey::set(rd_index_indkey);
    sx::rd_index_indnatts::set(rd_index_indnatts);
    sx::rd_index_indnkeyatts::set(rd_index_indnkeyatts);
    sx::rd_index_indisunique::set(rd_index_indisunique);
    sx::rd_index_indisprimary::set(rd_index_indisprimary);
    sx::rd_index_indisexclusion::set(rd_index_indisexclusion);
    sx::rd_index_indisready::set(rd_index_indisready);
    sx::rd_indam_amclusterable::set(rd_indam_amclusterable);
    sx::relation_is_mapped::set(relation_is_mapped);
    sx::relation_get_number_of_blocks::set(relation_get_number_of_blocks);
    sx::set_rd_toastoid::set(set_rd_toastoid);
    sx::swap_relfilelocator_subids::set(swap_relfilelocator_subids);

    // --- sortsupport index field reads ---
    sx::rd_opfamily::set(rd_opfamily);
    sx::rd_opcintype::set(rd_opcintype);
    sx::rd_indam_amcanorder::set(rd_indam_amcanorder);
    sx::rd_indam_amsearcharray::set(rd_indam_amsearcharray);

    // --- rd_amcache: hash's cached metapage + SP-GiST's cached SpGistCache
    //     (GIN/GiST install their own when those AMs land) ---
    sx::rd_amcache_hashmeta::set(rd_amcache_hashmeta);
    sx::set_rd_amcache_hashmeta::set(set_rd_amcache_hashmeta);
    sx::rd_amcache_spgist::set(rd_amcache_spgist);
    sx::set_rd_amcache_spgist::set(set_rd_amcache_spgist);
}

/* ==========================================================================
 * core-entry-store adapters.
 *
 * `RelationIdGetRelation`/`RelationClose` work on the owned store via the
 * `Oid` handle ([`crate::Relation`]); the seam projects an open relation into
 * the cross-unit `types_rel::RelationData<'mcx>` value-slice (the build family
 * owns the full projection). The pure scalar reads off a passed
 * `types_rel::RelationData`/`Relation` value-slice read its inline fields via
 * the scoped accessors over the owned entry.
 * ======================================================================== */

fn relation_id_get_relation<'mcx>(
    mcx: Mcx<'mcx>,
    relation_id: Oid,
) -> PgResult<Option<types_rel::RelationData<'mcx>>> {
    let rd = crate::core_entry_store::RelationIdGetRelation(relation_id)?;
    if rd == types_core::InvalidOid {
        return Ok(None);
    }
    // Project the owned entry into the cross-unit value-slice in `mcx`. The
    // full projection is build-family logic.
    crate::core_entry_store::with_relation(rd, |r| crate::build::project_relation_data(mcx, r))?
        .map(Some)
}

fn relation_id_get_relation_shared(
    relation_id: Oid,
) -> PgResult<
    Option<std::rc::Rc<std::cell::RefCell<crate::core_entry_store::RelationData>>>,
> {
    // The ADDITIVE shared-ref open: hand back a clone of the cache cell (C's
    // `RelationData *`) instead of a projected copy. Delegates to the
    // crate-local core-entry-store routine; coexists with the copy-projecting
    // `relation_id_get_relation` above.
    crate::core_entry_store::relation_id_get_relation_shared(relation_id)
}

fn relation_close(relation_id: Oid) -> PgResult<()> {
    match crate::core_entry_store::cache_lookup(relation_id) {
        Some(rd) => crate::core_entry_store::RelationClose(rd),
        None => Ok(()),
    }
}

fn relation_rd_tableam(
    rel: &types_rel::RelationData<'_>,
) -> Option<types_tableam::TableAmRoutine> {
    crate::core_entry_store::try_with_relation(rel.rd_id, |rd| rd.rd_tableam).flatten()
}

fn relation_rd_tableam_by_oid(relid: Oid) -> Option<types_tableam::TableAmRoutine> {
    crate::core_entry_store::try_with_relation(relid, |rd| rd.rd_tableam).flatten()
}

fn relation_needs_wal(rel: &types_rel::RelationData<'_>) -> bool {
    // RelationNeedsWAL(relation) (utils/rel.h): permanent && (XLogIsNeeded() ||
    // (rd_createSubid == Invalid && rd_firstRelfilelocatorSubid == Invalid)).
    // rd_createSubid/rd_firstRelfilelocatorSubid are owned-store fields not in
    // the cross-unit value-slice, so resolve the entry; XLogIsNeeded() is
    // `wal_level >= WAL_LEVEL_REPLICA` (the xlog GUC owner seam).
    use types_core::xact::InvalidSubTransactionId;
    use types_wal::xlog_consts::WAL_LEVEL_REPLICA;
    const RELPERSISTENCE_PERMANENT: i8 = b'p' as i8;
    let wal = backend_access_transam_xlog_seams::wal_level::call();
    crate::core_entry_store::try_with_relation(rel.rd_id, |rd| {
        rd.rd_rel.relpersistence == RELPERSISTENCE_PERMANENT
            && (wal >= WAL_LEVEL_REPLICA
                || (rd.rd_createSubid == InvalidSubTransactionId
                    && rd.rd_firstRelfilelocatorSubid == InvalidSubTransactionId))
    })
    .unwrap_or(false)
}

fn relation_is_accessible_in_logical_decoding(
    rel: &types_rel::Relation<'_>,
) -> PgResult<bool> {
    // RelationIsAccessibleInLogicalDecoding(relation) (utils/rel.h):
    //   XLogLogicalInfoActive() && RelationNeedsWAL(relation) &&
    //   (IsCatalogRelation(relation) || RelationIsUsedAsCatalogTable(relation))
    // expanded exactly as the C macros. XLogLogicalInfoActive() is
    // `wal_level >= WAL_LEVEL_LOGICAL`; RelationNeedsWAL is the permanent &&
    // (XLogIsNeeded() || not-newly-created) test; RelationIsUsedAsCatalogTable
    // is `rd_options && (relkind r|m) && user_catalog_table`. rd_createSubid /
    // rd_firstRelfilelocatorSubid / rd_options are owned-store fields, so
    // resolve the live entry (Err propagates a cache miss).
    use types_core::xact::InvalidSubTransactionId;
    use types_wal::xlog_consts::{WAL_LEVEL_LOGICAL, WAL_LEVEL_REPLICA};
    const RELPERSISTENCE_PERMANENT: i8 = b'p' as i8;
    const RELKIND_RELATION: i8 = b'r' as i8;
    const RELKIND_MATVIEW: i8 = b'm' as i8;
    let wal = backend_access_transam_xlog_seams::wal_level::call();
    let xlog_logical_info_active = wal >= WAL_LEVEL_LOGICAL;
    crate::core_entry_store::with_relation(rel.rd_id, |rd| {
        let relation_needs_wal = rd.rd_rel.relpersistence == RELPERSISTENCE_PERMANENT
            && (wal >= WAL_LEVEL_REPLICA
                || (rd.rd_createSubid == InvalidSubTransactionId
                    && rd.rd_firstRelfilelocatorSubid == InvalidSubTransactionId));
        let used_as_catalog_table = rd.rd_options.as_ref().is_some_and(|o| {
            (rd.rd_rel.relkind == RELKIND_RELATION || rd.rd_rel.relkind == RELKIND_MATVIEW)
                && o.user_catalog_table
        });
        xlog_logical_info_active
            && relation_needs_wal
            && (backend_catalog_catalog_seams::is_catalog_relation_oid::call(rd.rd_id)
                || used_as_catalog_table)
    })
}

fn relation_is_local(rel: &types_rel::RelationData<'_>) -> bool {
    // RELATION_IS_LOCAL(relation) (utils/rel.h): rd_islocaltemp ||
    // rd_createSubid != InvalidSubTransactionId. Both are owned-store fields.
    use types_core::xact::InvalidSubTransactionId;
    crate::core_entry_store::try_with_relation(rel.rd_id, |rd| {
        rd.rd_islocaltemp || rd.rd_createSubid != InvalidSubTransactionId
    })
    .unwrap_or(false)
}

fn relation_rd_indam(index_oid: Oid) -> Option<types_tableam::amapi::IndexAmRoutine> {
    // The `IndexAmRoutine` vtable is not copyable out of the cache; re-resolve
    // it from the cached entry's `rd_amhandler` (the C cache holds a `memcpy`
    // of exactly the routine the handler returns). Returns the freshly
    // resolved routine, matching `relation->rd_indam`.
    let amhandler = crate::core_entry_store::try_with_relation(index_oid, |rd| rd.rd_amhandler)?;
    backend_access_index_amapi_seams::get_index_am_routine::call(amhandler).ok()
}

fn relation_increment_reference_count(index_oid: Oid) -> PgResult<()> {
    match crate::core_entry_store::cache_lookup(index_oid) {
        Some(rd) => crate::core_entry_store::RelationIncrementReferenceCount(rd),
        None => Ok(()),
    }
}

fn relation_decrement_reference_count(index_oid: Oid) -> PgResult<()> {
    match crate::core_entry_store::cache_lookup(index_oid) {
        Some(rd) => crate::core_entry_store::RelationDecrementReferenceCount(rd),
        None => Ok(()),
    }
}

fn rd_support_at(index_oid: Oid, procindex: i32) -> PgResult<RegProcedure> {
    // `relation->rd_support[procindex]` off the cached index entry (the
    // support-proc OID array filled by `RelationInitIndexAccessInfo`).
    crate::core_entry_store::with_relation(index_oid, |rd| rd.rd_support[procindex as usize])
}


fn index_getprocinfo(
    index_oid: Oid,
    attnum: AttrNumber,
    procnum: u16,
    optsproc: u16,
    procindex: i32,
) -> PgResult<types_core::fmgr::FmgrInfo> {
    // `index_getprocinfo` (indexam.c) lazy-init half, over the cache-owned
    // `rd_supportinfo[procindex]` `FmgrInfo` array. The caller computed
    // `procindex = nproc*(attnum-1) + (procnum-1)` and passed `optsproc =
    // rd_indam->amoptsprocnum`.
    let pi = procindex as usize;

    // Initialize the lookup info if first time through. The `fmgr_info` seam
    // re-enters the relcache (it may open dependent rels), so resolve the proc
    // id / build the FmgrInfo OUTSIDE a live store borrow (copy the scalars out
    // first), then store it back.
    let (needs_init, proc_id, relname) = crate::core_entry_store::with_relation(index_oid, |rd| {
        // Assert(locinfo != NULL) — the array was sized in
        // RelationInitIndexAccessInfo.
        debug_assert!(pi < rd.rd_supportinfo.len());
        (
            rd.rd_supportinfo[pi].fn_oid == 0,
            rd.rd_support[pi],
            rd.rd_rel.relname.clone(),
        )
    })?;

    if needs_init {
        // Complain if the function was not found during IndexSupportInitialize.
        if proc_id == 0 {
            return Err(backend_utils_error::ereport(types_error::ERROR)
                .errmsg_internal(format!(
                    "missing support function {procnum} for attribute {attnum} of index \"{relname}\""
                ))
                .into_error());
        }
        // fmgr_info_cxt(procId, locinfo, irel->rd_indexcxt): resolve into the
        // cache-owned support-info slot. The owned `FmgrInfo` re-resolves at
        // call time (no handle), so this just records the lookup metadata; the
        // resolution's transient handler state is allocated in a scratch
        // context dropped here (the entry stores the owned `FmgrInfo` by value).
        let scratch = mcx::MemoryContext::new("index_getprocinfo");
        let finfo = backend_utils_fmgr_fmgr_seams::fmgr_info::call(scratch.mcx(), proc_id)?;
        crate::core_entry_store::with_relation_mut(index_oid, |rd| rd.rd_supportinfo[pi] = finfo)?;

        // if (procnum != optsproc) set_fn_opclass_options(locinfo,
        // attoptions[attnum-1]): this only writes `locinfo->fn_expr` with the
        // opclass-options Const. The owned `FmgrInfo` model intentionally drops
        // `fn_expr` (types-core carries no node vocabulary; the executor reads
        // only fn_addr/fn_oid/strict/...), so the assignment is a no-op here —
        // behaviour-preserving. The opclass-option caching side effect already
        // happens in RelationInitIndexAccessInfo's RelationGetIndexAttOptions.
        let _ = (procnum, optsproc);
    }

    crate::core_entry_store::with_relation(index_oid, |rd| rd.rd_supportinfo[pi])
}

fn index_opclass_missing_options_error(
    index_oid: Oid,
    attnum: AttrNumber,
) -> PgResult<PgError> {
    // C: opclass = indclass->values[attnum-1] read off rd_indextuple via
    // SysCacheGetAttrNotNull(INDEXRELID, ..., Anum_pg_index_indclass), then
    // ereport(ERROR, ERRCODE_INVALID_PARAMETER_VALUE, "operator class %s has
    // no options", generate_opclass_name(opclass)). The raw indclass read is
    // the syscache owner's; the name is the ruleutils owner's.
    let scratch = mcx::MemoryContext::new("opclass missing options");
    let mcx = scratch.mcx();
    let (_indnatts, _indnkeyatts, indclass) =
        backend_utils_cache_syscache_seams::pg_index_indclass::call(mcx, index_oid)?
            .ok_or_else(|| {
                backend_utils_error::ereport(types_error::ERROR)
                    .errmsg_internal(format!("cache lookup failed for index {index_oid}"))
                    .into_error()
            })?;
    let opclass = indclass[(attnum - 1) as usize];
    let opclass_name =
        backend_utils_adt_ruleutils_seams::generate_opclass_name::call(mcx, opclass)?;
    Ok(backend_utils_error::ereport(types_error::ERROR)
        .errcode(types_error::error::ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg(format!("operator class {opclass_name} has no options"))
        .into_error())
}

fn create_fake_relcache_entry<'mcx>(
    mcx: Mcx<'mcx>,
    rlocator: types_storage::RelFileLocator,
) -> PgResult<types_rel::RelationData<'mcx>> {
    crate::build::create_fake_relcache_entry(mcx, rlocator)
}

fn free_fake_relcache_entry(fakerel: types_rel::RelationData<'_>) {
    crate::build::free_fake_relcache_entry(fakerel)
}

/* ==========================================================================
 * invalidate adapters.
 * ======================================================================== */

fn at_eoxact_relation_cache(is_commit: bool) -> PgResult<()> {
    crate::invalidate::AtEOXact_RelationCache(is_commit)
}

fn at_eosubxact_relation_cache(
    is_commit: bool,
    my_subid: SubTransactionId,
    parent_subid: SubTransactionId,
) -> PgResult<()> {
    crate::invalidate::AtEOSubXact_RelationCache(is_commit, my_subid, parent_subid)
}

fn relation_cache_invalidate(debug_discard: bool) -> PgResult<()> {
    crate::invalidate::RelationCacheInvalidate(debug_discard)
}

fn relation_cache_invalidate_entry(relation_id: Oid) -> PgResult<()> {
    crate::invalidate::RelationCacheInvalidateEntry(relation_id)
}

/* ==========================================================================
 * derived adapters.
 * ======================================================================== */

fn relation_get_identity_key_bitmap<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::RelationData<'_>,
) -> PgResult<Option<PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>> {
    // Resolve the owned entry and run the derived-family build (own logic over
    // the store); it yields the replica-identity key columns as offset members.
    let rd = match crate::core_entry_store::cache_lookup(rel.rd_id) {
        Some(rd) => rd,
        None => return Ok(None),
    };
    let members = crate::derived::RelationGetIdentityKeyBitmap(rd)?;
    // C NULL (no replica identity / no indexes) — derived returns `None`.
    let Some(members) = members else {
        return Ok(None);
    };
    // Encode the offset members into a node `Bitmapset` via the bitmapset
    // owner's `bms_add_member` (the C `idindexattrs = bms_add_member(...,
    // attrnum - FirstLowInvalidHeapAttributeNumber)` already applied the offset
    // in the derived build). A run that adds no members yields the C NULL set.
    let mut bms: Option<PgBox<'mcx, types_nodes::Bitmapset<'mcx>>> = None;
    for x in members {
        bms = Some(backend_nodes_core_seams::bms_add_member::call(mcx, bms, x)?);
    }
    Ok(bms)
}

fn relation_get_index_attr_bitmap<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::RelationData<'_>,
    attr_kind: sx::IndexAttrBitmapKind,
) -> PgResult<Option<PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>> {
    // The seam-public kind and the owner's `derived` kind are now the same
    // canonical `types_relcache_entry::IndexAttrBitmapKind`, so it passes
    // straight through.
    // Run the derived build (own logic over the store); it yields the offset
    // members (already offset by FirstLowInvalidHeapAttributeNumber).
    let members = crate::derived::RelationGetIndexAttrBitmap(rel.rd_id, attr_kind)?;
    // Encode into a node `Bitmapset` via the bitmapset owner's `bms_add_member`.
    // An empty member list yields the C NULL set (`None`).
    let mut bms: Option<PgBox<'mcx, types_nodes::Bitmapset<'mcx>>> = None;
    for x in members {
        bms = Some(backend_nodes_core_seams::bms_add_member::call(mcx, bms, x)?);
    }
    Ok(bms)
}

fn relation_get_index_list<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'_>,
) -> PgResult<PgVec<'mcx, Oid>> {
    // Resolve the owned entry and run the derived-family build (own logic).
    let rd = crate::core_entry_store::cache_lookup(rel.rd_id);
    let list = match rd {
        Some(rd) => crate::derived::RelationGetIndexList(rd)?,
        None => Vec::new(),
    };
    // Copy the OID list into the caller's `mcx` (C: `list_copy` in the caller's
    // context).
    let mut out = PgVec::new_in(mcx);
    for oid in list {
        out.push(oid);
    }
    Ok(out)
}

/* ==========================================================================
 * partition cache read/write adapters (per-entry state; build via partcache).
 * ======================================================================== */

fn relation_get_partkey<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
) -> PgResult<Option<types_partition::PartitionKeyData<'mcx>>> {
    // `relation->rd_partkey` read: re-project the cache-owned slot into `mcx`.
    crate::core_entry_store::get_partkey(mcx, relid)
}

fn relation_set_partkey<'mcx>(
    relid: Oid,
    key: types_partition::PartitionKeyData<'mcx>,
) -> PgResult<()> {
    // `relation->rd_partkey = key` — copy the partcache-built key into the
    // cache-owned long-lived store (C: `rd_partkeycxt`).
    crate::core_entry_store::set_partkey(relid, &key)
}

fn relation_get_partcheck<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
) -> PgResult<(bool, PgVec<'mcx, types_nodes::nodes::Node<'mcx>>)> {
    // `(rd_partcheckvalid, copyObject(rd_partcheck))` — re-project the
    // cache-owned qual list into `mcx`.
    crate::core_entry_store::get_partcheck(mcx, relid)
}

fn relation_set_partcheck<'mcx>(
    relid: Oid,
    partcheck: PgVec<'mcx, types_nodes::nodes::Node<'mcx>>,
) -> PgResult<()> {
    // `rd_partcheck = copyObject(result); rd_partcheckvalid = true` — copy the
    // partcache-built qual list into the cache-owned long-lived store.
    crate::core_entry_store::set_partcheck(relid, &partcheck)
}

fn relation_get_composite_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    typrelid: Oid,
    type_id: Oid,
) -> PgResult<PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>> {
    crate::build::relation_get_composite_tupdesc(mcx, typrelid, type_id)
}

/* ==========================================================================
 * initfile adapters.
 * ======================================================================== */

fn relation_id_is_in_init_file(relation_id: Oid) -> bool {
    crate::initfile::RelationIdIsInInitFile(relation_id)
}

fn relation_cache_init_file_pre_invalidate() -> PgResult<()> {
    crate::initfile::RelationCacheInitFilePreInvalidate()
}

fn relation_cache_init_file_post_invalidate() -> PgResult<()> {
    crate::initfile::RelationCacheInitFilePostInvalidate()
}

fn relation_cache_initialize() -> PgResult<()> {
    crate::initfile::RelationCacheInitialize()
}

fn relation_cache_initialize_phase2() -> PgResult<()> {
    crate::initfile::RelationCacheInitializePhase2()
}

fn relation_cache_initialize_phase3() -> PgResult<()> {
    crate::initfile::RelationCacheInitializePhase3()
}

/* ==========================================================================
 * CLUSTER rd_rel / rd_index / rd_indam field reads (off the cross-unit
 * `Relation` value-slice's OID; resolve the owned entry) + transient sets.
 * ======================================================================== */

/// Read off the cached entry, applying `f` to the live `RelationData`.
fn with_entry<R>(relid: Oid, f: impl FnOnce(&crate::RelationData) -> R) -> PgResult<R> {
    crate::core_entry_store::with_relation(relid, f)
}

fn rd_rel_relam(rel: &types_rel::Relation<'_>) -> PgResult<Oid> {
    with_entry(rel.rd_id, |rd| rd.rd_rel.relam)
}
fn rd_rel_reltablespace(rel: &types_rel::Relation<'_>) -> PgResult<Oid> {
    with_entry(rel.rd_id, |rd| rd.rd_rel.reltablespace)
}
fn rd_rel_relowner(rel: &types_rel::Relation<'_>) -> PgResult<Oid> {
    with_entry(rel.rd_id, |rd| rd.rd_rel.relowner)
}
fn rd_rel_relisshared(rel: &types_rel::Relation<'_>) -> PgResult<bool> {
    with_entry(rel.rd_id, |rd| rd.rd_rel.relisshared)
}
fn rd_rel_relnamespace(rel: &types_rel::Relation<'_>) -> PgResult<Oid> {
    with_entry(rel.rd_id, |rd| rd.rd_rel.relnamespace)
}
fn rd_rel_relfrozenxid(rel: &types_rel::Relation<'_>) -> PgResult<TransactionId> {
    with_entry(rel.rd_id, |rd| rd.rd_rel.relfrozenxid)
}
fn rd_rel_relminmxid(rel: &types_rel::Relation<'_>) -> PgResult<MultiXactId> {
    with_entry(rel.rd_id, |rd| rd.rd_rel.relminmxid)
}
fn rd_islocaltemp(rel: &types_rel::Relation<'_>) -> PgResult<bool> {
    with_entry(rel.rd_id, |rd| rd.rd_islocaltemp)
}
fn rd_index_indrelid(index: &types_rel::Relation<'_>) -> PgResult<Option<Oid>> {
    with_entry(index.rd_id, |rd| rd.rd_index.as_ref().map(|i| i.indrelid))
}
fn rd_index_indisvalid(index: &types_rel::Relation<'_>) -> PgResult<bool> {
    with_entry(index.rd_id, |rd| {
        rd.rd_index.as_ref().is_some_and(|i| i.indisvalid)
    })
}
fn rd_index_has_indpred(index: &types_rel::Relation<'_>) -> PgResult<bool> {
    // `RelationGetIndexPredicate(index) != NIL`, i.e.
    // `!heap_attisnull(rd_indextuple, Anum_pg_index_indpred)`. The raw
    // `indpred` attribute is read off the index's pg_index tuple — the
    // syscache owner's read (the materialized node tree, which the derived
    // family builds, is not needed for the NIL test). `None` (cache miss) maps
    // to "no predicate".
    Ok(backend_utils_cache_syscache_seams::pg_index_has_predicate::call(index.rd_id)?
        .unwrap_or(false))
}
fn rd_index_indkey(index: &types_rel::Relation<'_>) -> PgResult<Option<std::vec::Vec<types_core::primitive::AttrNumber>>> {
    with_entry(index.rd_id, |rd| {
        rd.rd_index.as_ref().map(|i| i.indkey.clone())
    })
}
fn rd_index_indnatts(index: &types_rel::Relation<'_>) -> PgResult<Option<i16>> {
    with_entry(index.rd_id, |rd| rd.rd_index.as_ref().map(|i| i.indnatts))
}
fn rd_index_indnkeyatts(index: &types_rel::Relation<'_>) -> PgResult<Option<i16>> {
    with_entry(index.rd_id, |rd| {
        rd.rd_index.as_ref().map(|i| i.indnkeyatts)
    })
}
fn rd_index_indisunique(index: &types_rel::Relation<'_>) -> PgResult<bool> {
    with_entry(index.rd_id, |rd| {
        rd.rd_index.as_ref().is_some_and(|i| i.indisunique)
    })
}
fn rd_index_indisprimary(index: &types_rel::Relation<'_>) -> PgResult<bool> {
    with_entry(index.rd_id, |rd| {
        rd.rd_index.as_ref().is_some_and(|i| i.indisprimary)
    })
}
fn rd_index_indisexclusion(index: &types_rel::Relation<'_>) -> PgResult<bool> {
    with_entry(index.rd_id, |rd| {
        rd.rd_index.as_ref().is_some_and(|i| i.indisexclusion)
    })
}
fn rd_index_indisready(index: &types_rel::Relation<'_>) -> PgResult<bool> {
    with_entry(index.rd_id, |rd| {
        rd.rd_index.as_ref().is_some_and(|i| i.indisready)
    })
}
fn rd_indam_amclusterable(index: &types_rel::Relation<'_>) -> PgResult<bool> {
    // `index->rd_indam->amclusterable`: the trimmed in-cache `IndexAmRoutine`
    // vtable does not carry this CLUSTER-only scalar flag, so the amapi owner
    // projects it off the AM's untrimmed routine, keyed by the index's AM OID
    // (`rd_rel->relam`).
    let relam = with_entry(index.rd_id, |rd| rd.rd_rel.relam)?;
    backend_access_index_amapi_seams::index_am_clusterable::call(relam)
}
fn relation_is_mapped(rel: &types_rel::Relation<'_>) -> PgResult<bool> {
    // `RelationIsMapped(relation)` (utils/rel.h): a relation is mapped iff it
    // has storage and `rd_rel->relfilenode == InvalidOid` (the relation map
    // supplies its filenumber).
    with_entry(rel.rd_id, |rd| {
        relation_has_storage(rd.rd_rel.relkind) && rd.rd_rel.relfilenode == 0
    })
}

/// `RELKIND_HAS_STORAGE(relkind)` (pg_class.h) — duplicated from the index
/// family for the `relation_is_mapped` read.
fn relation_has_storage(relkind: i8) -> bool {
    relkind == b'r' as i8
        || relkind == b'i' as i8
        || relkind == b'S' as i8
        || relkind == b't' as i8
        || relkind == b'm' as i8
}
fn relation_get_number_of_blocks(rel: &types_rel::Relation<'_>) -> PgResult<u32> {
    // `RelationGetNumberOfBlocks(rel)` = `RelationGetNumberOfBlocksInFork(rel,
    // MAIN_FORKNUM)` = `smgrnblocks(RelationGetSmgr(rel), MAIN_FORKNUM)`. The
    // smgr owner opens the relation's smgr by its locator + backend; it panics
    // until smgr lands (mirror-pg-and-panic). The locator/backend come off the
    // owned entry.
    let (locator, backend) =
        crate::core_entry_store::with_relation(rel.rd_id, |rd| (rd.rd_locator, rd.rd_backend))?;
    backend_storage_smgr_seams::smgrnblocks::call(
        locator,
        backend,
        types_core::primitive::MAIN_FORKNUM,
    )
}
fn set_rd_toastoid(new_heap: &types_rel::Relation<'_>, value: Oid) -> PgResult<()> {
    // `NewHeap->rd_toastoid = value` — a transient set honored on the owned
    // entry while NewHeap stays open during the cluster copy.
    crate::core_entry_store::with_relation_mut(new_heap.rd_id, |rd| rd.rd_toastoid = value)
}

/// `RelationAssumeNewRelfilelocator(relation)` (relcache.c): record that the
/// relation took a new relfilenumber this (sub)transaction, and flag it for
/// eoxact cleanup. Own logic over the entry; the current subxid is the xact
/// owner seam.
fn assume_new_relfilelocator(relid: Oid) -> PgResult<()> {
    use types_core::xact::InvalidSubTransactionId;
    let subid = backend_access_transam_xact_seams::get_current_sub_transaction_id::call();
    crate::core_entry_store::with_relation_mut(relid, |r| {
        r.rd_newRelfilelocatorSubid = subid;
        if r.rd_firstRelfilelocatorSubid == InvalidSubTransactionId {
            r.rd_firstRelfilelocatorSubid = subid;
        }
    })?;
    // EOXactListAdd(relation): flag for end-of-xact cleanup.
    crate::core_entry_store::with_state(|st| crate::core_entry_store::eoxact_list_add(st, relid));
    Ok(())
}

fn swap_relfilelocator_subids(r1: Oid, r2: Oid) -> PgResult<()> {
    // `swap_relation_files` (cluster.c) tells the relcache both relations took
    // new relfilelocators this transaction.
    assume_new_relfilelocator(r1)?;
    assume_new_relfilelocator(r2)?;
    Ok(())
}

/* ==========================================================================
 * sortsupport index field reads.
 * ======================================================================== */

fn rd_opfamily(index: &types_rel::Relation<'_>, attno: AttrNumber) -> PgResult<Oid> {
    // `index->rd_opfamily[attno]` off the cached index entry.
    with_entry(index.rd_id, |rd| rd.rd_opfamily[attno as usize])
}
fn rd_opcintype(index: &types_rel::Relation<'_>, attno: AttrNumber) -> PgResult<Oid> {
    // `index->rd_opcintype[attno]` off the cached index entry.
    with_entry(index.rd_id, |rd| rd.rd_opcintype[attno as usize])
}
fn rd_indam_amcanorder(index: &types_rel::Relation<'_>) -> PgResult<bool> {
    // `index->rd_indam->amcanorder`: not carried on the trimmed in-cache
    // `IndexAmRoutine` vtable; the amapi owner projects this scalar off the
    // AM's untrimmed routine (cf. `index_am_canbackward`), keyed by the index's
    // AM OID (`rd_rel->relam`).
    let relam = with_entry(index.rd_id, |rd| rd.rd_rel.relam)?;
    backend_access_index_amapi_seams::index_am_canorder::call(relam)
}

fn rd_indam_amsearcharray(index: &types_rel::Relation<'_>) -> PgResult<bool> {
    // `index->rd_indam->amsearcharray`: as for amcanorder, projected off the
    // AM's untrimmed routine by AM OID.
    let relam = with_entry(index.rd_id, |rd| rd.rd_rel.relam)?;
    backend_access_index_amapi_seams::index_am_searcharray::call(relam)
}

/* ==========================================================================
 * rd_amcache adapters.
 *
 * hash's `_hash_getcachedmetap` caches a `HashMetaPageData` in the per-relation
 * `rel->rd_amcache`. The generic slot on the entry holds an erased
 * `Box<dyn AmOpaque<'static>>`; [`HashMetaAmcache`] is the `'static` wrapper
 * that lets a `HashMetaPageData` ride in it with a tag-checked downcast. These
 * two adapters are the read/write halves the hash AM consumes; the generic
 * get/set live in `core_entry_store`.
 * ======================================================================== */

/// The `rd_amcache` payload for a hash index: a cached `HashMetaPageData`. The
/// `'static` bound holds because `HashMetaPageData` is all owned scalars (no
/// `'mcx` borrow), matching the C `rd_indexcxt` (CacheMemoryContext) lifetime.
struct HashMetaAmcache(types_hash::hashpage::HashMetaPageData);

impl types_tableam::amopaque::AmOpaqueType<'static> for HashMetaAmcache {
    const TAG: types_tableam::amopaque::AmOpaqueTag =
        types_tableam::amopaque::tags::HASH_META;
}

fn rd_amcache_hashmeta(
    index_oid: Oid,
) -> PgResult<Option<types_hash::hashpage::HashMetaPageData>> {
    // `(HashMetaPage) rel->rd_amcache` — read the cached metapage, or `None`
    // (the C `rd_amcache == NULL`). A missing/closed entry reads as no cache.
    Ok(crate::core_entry_store::with_rd_amcache::<HashMetaAmcache, _>(index_oid, |m| {
        m.0.clone()
    })
    .unwrap_or(None))
}

fn set_rd_amcache_hashmeta(
    index_oid: Oid,
    metap: types_hash::hashpage::HashMetaPageData,
) -> PgResult<()> {
    // `rel->rd_amcache = MemoryContextAlloc(rel->rd_indexcxt, ...); memcpy(...)`
    // — install/refresh the cached metapage on the entry's rd_amcache slot.
    crate::core_entry_store::set_rd_amcache(index_oid, Box::new(HashMetaAmcache(metap)))
}

/// The `rd_amcache` payload for an SP-GiST index: a cached `SpGistCache`
/// (spgist_private.h). The `'static` bound holds because `SpGistCache` is all
/// owned scalars (`Copy`, no `'mcx` borrow), matching the C `rd_indexcxt`
/// (CacheMemoryContext) lifetime — exactly like [`HashMetaAmcache`].
struct SpGistCacheAmcache(types_spgist::SpGistCache);

impl types_tableam::amopaque::AmOpaqueType<'static> for SpGistCacheAmcache {
    const TAG: types_tableam::amopaque::AmOpaqueTag =
        types_tableam::amopaque::tags::SPGIST_CACHE;
}

fn rd_amcache_spgist(index_oid: Oid) -> PgResult<Option<types_spgist::SpGistCache>> {
    // `(SpGistCache *) index->rd_amcache` — read the cached cache, or `None`
    // (the C `rd_amcache == NULL`). A missing/closed entry reads as no cache.
    Ok(
        crate::core_entry_store::with_rd_amcache::<SpGistCacheAmcache, _>(index_oid, |c| c.0)
            .unwrap_or(None),
    )
}

fn set_rd_amcache_spgist(index_oid: Oid, cache: types_spgist::SpGistCache) -> PgResult<()> {
    // `index->rd_amcache = MemoryContextAlloc(index->rd_indexcxt, ...); memcpy(...)`
    // — install/refresh the cached SpGistCache on the entry's rd_amcache slot.
    crate::core_entry_store::set_rd_amcache(index_oid, Box::new(SpGistCacheAmcache(cache)))
}
