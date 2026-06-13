//! Seam installation — this unit OWNS `backend-utils-cache-relcache-seams`.
//!
//! [`init_seams`] installs every seam declared there (the inward wiring). The
//! adapter functions below match each seam's declared `Signature` exactly and
//! bridge the cross-unit value-slice (`types_rel::RelationData`/`Relation`,
//! `Oid`) to this crate's owned entry store + family logic. Where a family's
//! logic has not yet landed the adapter delegates into that family's `todo!()`
//! body (the documented seam-and-panic boundary); the substrate (core entry
//! store) is real and its adapters resolve against it.

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
    sx::relation_close::set(relation_close);
    sx::relation_rd_tableam::set(relation_rd_tableam);
    sx::relation_needs_wal::set(relation_needs_wal);
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
    sx::rd_indam_amclusterable::set(rd_indam_amclusterable);
    sx::relation_is_mapped::set(relation_is_mapped);
    sx::relation_get_number_of_blocks::set(relation_get_number_of_blocks);
    sx::set_rd_toastoid::set(set_rd_toastoid);
    sx::swap_relfilelocator_subids::set(swap_relfilelocator_subids);

    // --- sortsupport index field reads ---
    sx::rd_opfamily::set(rd_opfamily);
    sx::rd_opcintype::set(rd_opcintype);
    sx::rd_indam_amcanorder::set(rd_indam_amcanorder);
}

/* ==========================================================================
 * core-entry-store adapters.
 *
 * `RelationIdGetRelation`/`RelationClose` work on the owned store via the C
 * `Relation` pointer; the seam projects an open relation into the cross-unit
 * `types_rel::RelationData<'mcx>` value-slice (the build family owns the full
 * projection — `todo!()` until it lands). The pure scalar reads off a passed
 * `types_rel::RelationData`/`Relation` value-slice read its inline fields.
 * ======================================================================== */

fn relation_id_get_relation<'mcx>(
    mcx: Mcx<'mcx>,
    relation_id: Oid,
) -> PgResult<Option<types_rel::RelationData<'mcx>>> {
    let rd = crate::core_entry_store::RelationIdGetRelation(relation_id)?;
    if rd.is_null() {
        return Ok(None);
    }
    // Project the owned entry into the cross-unit value-slice in `mcx`. The
    // full projection is build-family logic.
    crate::build::project_relation_data(mcx, rd).map(Some)
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
    let rd = crate::core_entry_store::cache_lookup(rel.rd_id)?;
    // SAFETY: live cache-owned descriptor.
    #[allow(unsafe_code)]
    unsafe {
        (*rd).rd_tableam
    }
}

fn relation_needs_wal(rel: &types_rel::RelationData<'_>) -> bool {
    // RelationNeedsWAL evaluates the whole macro (rd_createSubid/wal_level).
    // Own logic over the entry — lands with the invalidate/build families.
    todo!("relcache seam: relation_needs_wal (own macro over entry)")
}

fn relation_is_local(rel: &types_rel::RelationData<'_>) -> bool {
    todo!("relcache seam: relation_is_local (own macro over entry)")
}

fn relation_rd_indam(index_oid: Oid) -> Option<types_tableam::amapi::IndexAmRoutine> {
    // Resolving the index-AM vtable for `index_oid` is index-family logic
    // (`RelationInitIndexAccessInfo`/`InitIndexAmRoutine`); the `IndexAmRoutine`
    // vtable is not copyable out of the cache, so this returns the freshly
    // resolved routine when that family lands.
    todo!("relcache seam: relation_rd_indam (index family resolves the vtable)")
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
    todo!("relcache seam: rd_support_at (index family)")
}

fn index_getprocinfo(
    index_oid: Oid,
    attnum: AttrNumber,
    procnum: u16,
    optsproc: u16,
    procindex: i32,
) -> PgResult<types_core::fmgr::FmgrInfo> {
    todo!("relcache seam: index_getprocinfo (index family)")
}

fn index_opclass_missing_options_error(
    index_oid: Oid,
    attnum: AttrNumber,
) -> PgResult<PgError> {
    todo!("relcache seam: index_opclass_missing_options_error (index family)")
}

fn create_fake_relcache_entry<'mcx>(
    mcx: Mcx<'mcx>,
    rlocator: types_storage::RelFileLocator,
) -> PgResult<types_rel::RelationData<'mcx>> {
    todo!("relcache seam: create_fake_relcache_entry (core-entry-store)")
}

fn free_fake_relcache_entry(fakerel: types_rel::RelationData<'_>) {
    todo!("relcache seam: free_fake_relcache_entry (core-entry-store)")
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
    let _members = crate::derived::RelationGetIdentityKeyBitmap(rd)?;
    // Encoding the offset members into a node `Bitmapset` (the `bms_add_member`
    // word layout) is node vocabulary owned by `nodes/bitmapset.c`; that encode
    // lands with the bitmapset owner (seam-and-panic boundary).
    let _ = mcx;
    todo!("relcache seam: relation_get_identity_key_bitmap node-encode (bitmapset owner)")
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
    todo!("relcache seam: relation_get_partkey (index/partition cache)")
}

fn relation_set_partkey<'mcx>(
    relid: Oid,
    key: types_partition::PartitionKeyData<'mcx>,
) -> PgResult<()> {
    todo!("relcache seam: relation_set_partkey (index/partition cache)")
}

fn relation_get_partcheck<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
) -> PgResult<(bool, PgVec<'mcx, types_nodes::nodes::Node<'mcx>>)> {
    todo!("relcache seam: relation_get_partcheck (index/partition cache)")
}

fn relation_set_partcheck<'mcx>(
    relid: Oid,
    partcheck: PgVec<'mcx, types_nodes::nodes::Node<'mcx>>,
) -> PgResult<()> {
    todo!("relcache seam: relation_set_partcheck (index/partition cache)")
}

fn relation_get_composite_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    typrelid: Oid,
    type_id: Oid,
) -> PgResult<PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>> {
    todo!("relcache seam: relation_get_composite_tupdesc (build family)")
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

fn rd_rel_relam(rel: &types_rel::Relation<'_>) -> PgResult<Oid> {
    todo!("relcache seam: rd_rel_relam (index family)")
}
fn rd_rel_reltablespace(rel: &types_rel::Relation<'_>) -> PgResult<Oid> {
    todo!("relcache seam: rd_rel_reltablespace (index family)")
}
fn rd_rel_relowner(rel: &types_rel::Relation<'_>) -> PgResult<Oid> {
    todo!("relcache seam: rd_rel_relowner (index family)")
}
fn rd_rel_relisshared(rel: &types_rel::Relation<'_>) -> PgResult<bool> {
    todo!("relcache seam: rd_rel_relisshared (index family)")
}
fn rd_rel_relnamespace(rel: &types_rel::Relation<'_>) -> PgResult<Oid> {
    todo!("relcache seam: rd_rel_relnamespace (index family)")
}
fn rd_rel_relfrozenxid(rel: &types_rel::Relation<'_>) -> PgResult<TransactionId> {
    todo!("relcache seam: rd_rel_relfrozenxid (index family)")
}
fn rd_rel_relminmxid(rel: &types_rel::Relation<'_>) -> PgResult<MultiXactId> {
    todo!("relcache seam: rd_rel_relminmxid (index family)")
}
fn rd_islocaltemp(rel: &types_rel::Relation<'_>) -> PgResult<bool> {
    todo!("relcache seam: rd_islocaltemp (core-entry-store)")
}
fn rd_index_indrelid(index: &types_rel::Relation<'_>) -> PgResult<Option<Oid>> {
    todo!("relcache seam: rd_index_indrelid (index family)")
}
fn rd_index_indisvalid(index: &types_rel::Relation<'_>) -> PgResult<bool> {
    todo!("relcache seam: rd_index_indisvalid (index family)")
}
fn rd_index_has_indpred(index: &types_rel::Relation<'_>) -> PgResult<bool> {
    todo!("relcache seam: rd_index_has_indpred (index family)")
}
fn rd_indam_amclusterable(index: &types_rel::Relation<'_>) -> PgResult<bool> {
    todo!("relcache seam: rd_indam_amclusterable (index family)")
}
fn relation_is_mapped(rel: &types_rel::Relation<'_>) -> PgResult<bool> {
    todo!("relcache seam: relation_is_mapped (index family)")
}
fn relation_get_number_of_blocks(rel: &types_rel::Relation<'_>) -> PgResult<u32> {
    todo!("relcache seam: relation_get_number_of_blocks (smgr seam, index family)")
}
fn set_rd_toastoid(new_heap: &types_rel::Relation<'_>, value: Oid) -> PgResult<()> {
    todo!("relcache seam: set_rd_toastoid (core-entry-store transient set)")
}
fn swap_relfilelocator_subids(r1: Oid, r2: Oid) -> PgResult<()> {
    todo!("relcache seam: swap_relfilelocator_subids (initfile family)")
}

/* ==========================================================================
 * sortsupport index field reads.
 * ======================================================================== */

fn rd_opfamily(index: &types_rel::Relation<'_>, attno: AttrNumber) -> PgResult<Oid> {
    todo!("relcache seam: rd_opfamily (index family)")
}
fn rd_opcintype(index: &types_rel::Relation<'_>, attno: AttrNumber) -> PgResult<Oid> {
    todo!("relcache seam: rd_opcintype (index family)")
}
fn rd_indam_amcanorder(index: &types_rel::Relation<'_>) -> PgResult<bool> {
    todo!("relcache seam: rd_indam_amcanorder (index family)")
}
