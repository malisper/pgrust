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
    sx::relation_project_existing::set(relation_project_existing);
    sx::relation_id_get_relation_shared::set(relation_id_get_relation_shared);
    sx::relation_id_get_relation_cell::set(relation_id_get_relation_cell);
    sx::relation_close::set(relation_close);
    sx::relation_rd_tableam::set(relation_rd_tableam);
    sx::relation_rd_tableam_by_oid::set(relation_rd_tableam_by_oid);
    sx::relation_needs_wal::set(relation_needs_wal);
    sx::relation_is_accessible_in_logical_decoding::set(relation_is_accessible_in_logical_decoding);
    sx::is_toast_relation::set(is_toast_relation);
    sx::relation_is_logically_logged::set(relation_is_logically_logged);
    sx::rd_rel_relrewrite::set(rd_rel_relrewrite);
    sx::rd_rel_relkind_by_oid::set(rd_rel_relkind_by_oid);
    sx::relation_is_local::set(relation_is_local);
    sx::relation_rd_indam::set(relation_rd_indam);
    sx::relation_increment_reference_count::set(relation_increment_reference_count);
    sx::relation_decrement_reference_count::set(relation_decrement_reference_count);
    // `ResOwnerReleaseRelation` (relcache.c) — the `relref_resowner_desc`
    // `ReleaseResource` callback. Release a leaked relcache pin found during
    // resource-owner release WITHOUT re-forgetting it from the owner.
    sx::release_relation_ref::set(release_relation_ref);
    sx::rd_support_at::set(rd_support_at);
    sx::index_getprocinfo::set(index_getprocinfo);
    sx::index_opclass_missing_options_error::set(index_opclass_missing_options_error);
    sx::create_fake_relcache_entry::set(create_fake_relcache_entry);
    sx::free_fake_relcache_entry::set(free_fake_relcache_entry);

    // --- invalidate ---
    sx::relation_forget_relation::set(crate::invalidate::RelationForgetRelation);
    sx::at_eoxact_relation_cache::set(at_eoxact_relation_cache);
    sx::at_eosubxact_relation_cache::set(at_eosubxact_relation_cache);
    sx::relation_cache_invalidate::set(relation_cache_invalidate);
    sx::relation_cache_invalidate_entry::set(relation_cache_invalidate_entry);

    // --- derived ---
    sx::relation_get_identity_key_bitmap::set(relation_get_identity_key_bitmap);
    sx::relation_get_index_attr_bitmap::set(relation_get_index_attr_bitmap);
    sx::relation_get_index_list::set(relation_get_index_list);
    sx::relation_get_index_expressions::set(relation_get_index_expressions);
    sx::relation_get_index_predicate::set(relation_get_index_predicate);
    sx::relation_get_exclusion_info::set(relation_get_exclusion_info);
    sx::relation_get_dummy_index_expressions::set(relation_get_dummy_index_expressions);

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
    sx::rd_index_indnullsnotdistinct::set(rd_index_indnullsnotdistinct);
    sx::rd_rel_relpersistence::set(rd_rel_relpersistence);
    sx::rd_rel_relkind::set(rd_rel_relkind);
    sx::rd_rel_relnatts::set(rd_rel_relnatts);
    sx::rd_rel_relispartition::set(rd_rel_relispartition);
    sx::relation_get_descr::set(relation_get_descr);
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

    // --- rd_fdwroutine: cached FDW callback-presence table (foreign.c) ---
    sx::relation_fdwroutine::set(relation_fdwroutine);
    sx::set_relation_fdwroutine::set(set_relation_fdwroutine);

    // --- relcache-build global flags + index field reads + relfilenumber ---
    sx::critical_relcaches_built::set(critical_relcaches_built);
    sx::critical_shared_relcaches_built::set(critical_shared_relcaches_built);
    sx::assert_could_get_relation::set(assert_could_get_relation);
    sx::rd_indcollation::set(rd_indcollation);
    sx::index_getprocid::set(index_getprocid);
    sx::relation_set_new_relfilenumber::set(crate::initfile::RelationSetNewRelfilenumber);

    // --- catalog/index.c index_create bootstrap leg: set up an open index
    //     entry's rd_index/rd_indam/opclass/support arrays. The relcache entry
    //     is registry-owned, so the seam addresses it by OID and the install
    //     borrows the cell mutably; RelationInitIndexAccessInfo only re-reads
    //     OTHER catalogs (pg_index/pg_am/pg_opclass) — the one self-re-resolve
    //     (rd_opcoptions priming) is deferred to force_index_att_options after
    //     the entry is cache-resident, so no re-entrant borrow of this cell. ---
    sx::relation_init_index_access_info::set(|index_id| {
        crate::core_entry_store::with_relation_mut(index_id, |rd| {
            crate::index::RelationInitIndexAccessInfo(rd)
        })
        .and_then(|r| r)
    });

    // --- set_attnotnull (tablecmds.c) live-entry compact-attr nullability poke.
    //     C mutates TupleDescCompactAttr(RelationGetDescr(rel), attnum-1)
    //     ->attnullability in place through the cached relation pointer; the
    //     owned descriptor carries attnullability on the per-attr row. ---
    sx::set_relcache_attnullability::set(|relid, attnum, attnullability| {
        crate::core_entry_store::with_relation_mut(relid, |rd| {
            rd.rd_att.attrs[(attnum - 1) as usize].attnullability = attnullability;
        })
    });

    // --- rewriteHandler.c per-query rule reader (rd_rules re-projection) ---
    sx::relation_rules::set(relation_rules);
    sx::relation_row_security::set(relation_row_security);

    // --- matview.c RefreshMatViewByOid rd_rules reads (the RuleLock carrier the
    //     relcache owns): the rewrite-rule shape + the stored dataQuery ---
    backend_commands_matview_deps_seams::matview_rule_info::set(matview_rule_info);
    backend_commands_matview_deps_seams::matview_data_query::set(matview_data_query);
    // is_usable_unique_index (matview.c): the index's Form_pg_index fields +
    // RelationGetIndexPredicate == NIL test, read off the live index relcache
    // entry.
    backend_commands_matview_deps_seams::index_usability_info::set(index_usability_info);
    // index_match_merge_quals (matview.c 740-817): the per-key-column equality
    // quals for one usable unique index, read off the index's relcache opclass
    // projection + the matview tuple descriptor.
    backend_commands_matview_deps_seams::index_match_merge_quals::set(index_match_merge_quals);

    // --- WAL-startup: StartupXLOG (xlog.c:5657) drops stale init files ---
    sx::relation_cache_init_file_remove::set(crate::initfile::RelationCacheInitFileRemove);

    // --- commands/vacuum.c vacuum_rel by-OID rd_rel / rd_options / rd_lockInfo reads ---
    sx::rel_frozenxid_minmxid::set(rel_frozenxid_minmxid);
    sx::rel_pages_tuples::set(rel_pages_tuples);
    sx::rel_relowner::set(rel_relowner);
    sx::rel_reltoastrelid::set(rel_reltoastrelid);
    sx::rel_rd_toastoid::set(rel_rd_toastoid);
    sx::rel_std_rd_options::set(rel_std_rd_options);
    sx::rel_lock_relid::set(rel_lock_relid);

    // --- plancat.c get_relation_info relcache-owned reads (parallel-workers,
    //     index list, per-index catalog detoast, index block count) ---
    crate::plancat_ext::init_seams();

    // --- lazy-vacuum driver relcache-field reads (vacuumlazy.c reads these
    //     inline off the relcache entry; they home in vacuumlazy-seams off
    //     `&Relation<'mcx>`, the relcache is their real owner) ---
    use backend_access_heap_vacuumlazy_seams as vx;
    vx::relation_get_namespace::set(rd_rel_relnamespace);
    vx::relation_is_shared::set(rd_rel_relisshared);
    vx::relation_get_number_of_blocks::set(relation_get_number_of_blocks);
    vx::relation_get_relation_name::set(vac_relation_get_relation_name);
    vx::relation_get_reltuples::set(vac_relation_get_reltuples);
    vx::relation_needs_wal::set(vac_relation_needs_wal);
    vx::relation_uses_local_buffers::set(vac_relation_uses_local_buffers);

    // --- hio.c heap-insertion target-page free-space lookup ---
    use backend_access_heap_hio_seams as hx;
    hx::relation_get_target_page_free_space::set(relation_get_target_page_free_space);
    // `RelationGetTargetBlock(rel)` == `rd_smgr ? rd_smgr->smgr_targblock :
    // InvalidBlockNumber` — the backend-local insertion-target hint. The
    // relation's (locator, backend) come off the owned entry; the cached hint
    // lives in smgr-owned state.
    hx::relation_get_target_block::set(relation_get_target_block);
    hx::relation_set_target_block::set(relation_set_target_block);
    // `GetPageWithFreeSpace(rel, len)` / `RecordAndGetPageWithFreeSpace(rel,
    // oldPage, oldAvail, needed)` (freespace.c) — the hio-seam forms are keyed
    // by OID; project a transient `Relation` read handle off the owned entry and
    // delegate to the freespace owner's `&Relation`-keyed seams.
    hx::get_page_with_free_space::set(get_page_with_free_space);
    hx::record_and_get_page_with_free_space::set(record_and_get_page_with_free_space);
    // `RecordPageWithFreeSpace` / `FreeSpaceMapVacuumRange` (freespace.c) — the
    // remaining OID-keyed FSM hio-seams (the relation-extension FSM updates).
    hx::record_page_with_free_space::set(record_page_with_free_space);
    hx::free_space_map_vacuum_range::set(free_space_map_vacuum_range);
    // `ReadBuffer` / `ReadBufferExtended` / `ExtendBufferedRelBy` /
    // `RelationGetNumberOfBlocks` (bufmgr.c) — the heap-insertion buffer
    // round-trip, projected off the owned entry to the bufmgr `&Relation` seams.
    hx::read_buffer::set(read_buffer);
    hx::read_buffer_extended::set(read_buffer_extended);
    hx::extend_buffered_rel_by::set(extend_buffered_rel_by);
    hx::relation_get_number_of_blocks::set(hio_relation_get_number_of_blocks);
    // `visibilitymap_pin` / `visibilitymap_pin_ok` (visibilitymap.c) — pin the
    // VM page covering the target heap block.
    hx::visibilitymap_pin::set(visibilitymap_pin);
    hx::visibilitymap_pin_ok::set(|heap_blk, vmbuf| {
        Ok(backend_access_heap_visibilitymap_seams::visibilitymap_pin_ok::call(heap_blk, vmbuf))
    });
    // `RELATION_IS_LOCAL` / `RelationGetRelationName` (utils/rel.h) and
    // `RelationExtensionLockWaiterCount` (lmgr.c) off the owned entry.
    hx::relation_is_local::set(hio_relation_is_local);
    hx::relation_get_relation_name::set(hio_relation_get_relation_name);
    hx::relation_extension_lock_waiter_count::set(relation_extension_lock_waiter_count);

    // tablecmds.c CheckTableNotInUse / ExecuteTruncate read these rel.h fields
    // (rd_isnailed / rd_refcnt / rd_createSubid) off `&Relation`; the relcache
    // owns them, resolving against the live owned entry by OID.
    use backend_commands_tablecmds_seams as tcx;
    tcx::relation_is_nailed::set(tc_relation_is_nailed);
    tcx::relation_get_refcount::set(tc_relation_get_refcount);
    tcx::relation_get_create_subid::set(tc_relation_get_create_subid);
    tcx::relation_get_new_relfilelocator_subid::set(tc_relation_get_new_relfilelocator_subid);

    // ruleutils' `set_relation_column_names` relation branch: the live `attname`
    // for each *physical* column (`None` for a dropped column). The relcache owns
    // the relation's `TupleDesc`, so it provides this name-resolution step.
    backend_utils_adt_ruleutils_seams::ruleutils_relation_real_colnames::set(
        ruleutils_relation_real_colnames,
    );
}

/// `set_relation_column_names`' relation branch (ruleutils.c 4390-4412): open the
/// relation, read its current `TupleDesc`, and return one entry per *physical*
/// column — the live `attname` for a live column, `None` for a dropped column.
fn ruleutils_relation_real_colnames<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
) -> PgResult<PgVec<'mcx, Option<mcx::PgString<'mcx>>>> {
    // C: rel = relation_open(rte->relid, AccessShareLock); ... relation_close.
    // The deparser can be invoked on a relation that is not already pinned in
    // the current statement (e.g. standalone `pg_get_indexdef` /
    // `pg_get_constraintdef` over a partial-index predicate or CHECK Var), so
    // open-and-pin here rather than assuming a live entry. The RAII guard
    // unpins on drop, mirroring `relation_close`.
    let rel = crate::core_entry_store::RelationRef::open(relid)?;
    rel.with(|rd| {
        let mut out = PgVec::new_in(mcx);
        out.try_reserve(rd.rd_att.attrs.len())
            .map_err(|_| mcx.oom(0))?;
        for att in rd.rd_att.attrs.iter() {
            if att.attisdropped {
                out.push(None);
            } else {
                out.push(Some(mcx::PgString::from_str_in(&att.attname, mcx)?));
            }
        }
        Ok(out)
    })
}

/// `rel->rd_isnailed` (rel.h) — read off the live owned relcache entry.
fn tc_relation_is_nailed(rel: &types_rel::Relation<'_>) -> PgResult<bool> {
    crate::core_entry_store::with_relation(rel.rd_id, |rd| rd.rd_isnailed)
}

/// `rel->rd_refcnt` (rel.h) — the relcache pin count on the live owned entry.
fn tc_relation_get_refcount(rel: &types_rel::Relation<'_>) -> PgResult<i32> {
    crate::core_entry_store::with_relation(rel.rd_id, |rd| rd.rd_refcnt)
}

/// `rel->rd_createSubid` (rel.h) — the sub-xact that created this relation.
fn tc_relation_get_create_subid(
    rel: &types_rel::Relation<'_>,
) -> PgResult<types_core::SubTransactionId> {
    crate::core_entry_store::with_relation(rel.rd_id, |rd| rd.rd_createSubid)
}

/// `rel->rd_newRelfilelocatorSubid` (rel.h) — the sub-xact that gave this
/// relation a new relfilenumber (`InvalidSubTransactionId` if none).
fn tc_relation_get_new_relfilelocator_subid(
    rel: &types_rel::Relation<'_>,
) -> PgResult<types_core::SubTransactionId> {
    crate::core_entry_store::with_relation(rel.rd_id, |rd| rd.rd_newRelfilelocatorSubid)
}

/// Project the registry-owned relcache entry into a transient `Relation` read
/// handle (no release authority) — the OID→`&Relation` bridge the buffer/FSM/VM
/// owners need. The arena is dropped when the returned handle is dropped.
fn project_open(
    relcx: &mcx::MemoryContext,
    rel: Oid,
) -> PgResult<types_rel::Relation<'_>> {
    let data = crate::core_entry_store::with_relation(rel, |rd| {
        crate::build::project_relation_data(relcx.mcx(), rd)
    })??;
    Ok(types_rel::Relation::open(data, None))
}

/// `RecordPageWithFreeSpace(rel, heapBlk, spaceAvail)` (freespace.c), OID-keyed.
fn record_page_with_free_space(
    rel: Oid,
    heap_blk: types_core::primitive::BlockNumber,
    space_avail: types_core::Size,
) -> PgResult<()> {
    let relcx = mcx::MemoryContext::new("record_page_with_free_space");
    let rel = project_open(&relcx, rel)?;
    backend_storage_freespace_seams::record_page_with_free_space::call(&rel, heap_blk, space_avail)
}

/// `FreeSpaceMapVacuumRange(rel, start, end)` (freespace.c), OID-keyed.
fn free_space_map_vacuum_range(
    rel: Oid,
    start: types_core::primitive::BlockNumber,
    end: types_core::primitive::BlockNumber,
) -> PgResult<()> {
    let relcx = mcx::MemoryContext::new("free_space_map_vacuum_range");
    let rel = project_open(&relcx, rel)?;
    backend_storage_freespace_seams::free_space_map_vacuum_range::call(&rel, start, end)
}

/// `ReadBuffer(rel, blkno)` (bufmgr.c), OID-keyed.
fn read_buffer(
    rel: Oid,
    target_block: types_core::primitive::BlockNumber,
) -> PgResult<types_storage::storage::Buffer> {
    let relcx = mcx::MemoryContext::new("read_buffer");
    let rel = project_open(&relcx, rel)?;
    backend_storage_buffer_bufmgr_seams::read_buffer::call(&rel, target_block)
}

/// `ReadBufferExtended(rel, MAIN_FORKNUM, blkno, mode, strategy)` (bufmgr.c),
/// OID-keyed. The hio.c `mode` is the int `RBM_*` value (0/1/2 == the
/// `ReadBufferMode` discriminants).
fn read_buffer_extended(
    rel: Oid,
    target_block: types_core::primitive::BlockNumber,
    mode: i32,
    has_strategy: bool,
) -> PgResult<types_storage::storage::Buffer> {
    let mode = match mode {
        0 => types_storage::storage::ReadBufferMode::Normal,
        1 => types_storage::storage::ReadBufferMode::ZeroAndLock,
        2 => types_storage::storage::ReadBufferMode::ZeroAndCleanupLock,
        other => {
            return Err(PgError::new(
                types_error::ERROR,
                format!("unexpected read-buffer mode {other} from hio.c"),
            ));
        }
    };
    let relcx = mcx::MemoryContext::new("read_buffer_extended");
    let rel = project_open(&relcx, rel)?;
    backend_storage_buffer_bufmgr_seams::read_buffer_extended_mode::call(
        &rel,
        target_block,
        mode,
        has_strategy,
    )
}

/// `ExtendBufferedRelBy(rel, MAIN_FORKNUM, strategy, EB_LOCK_FIRST, extend_by)`
/// (bufmgr.c), OID-keyed.
fn extend_buffered_rel_by(
    rel: Oid,
    has_strategy: bool,
    extend_by: u32,
) -> PgResult<types_storage::buf::ExtendedRelation> {
    let relcx = mcx::MemoryContext::new("extend_buffered_rel_by");
    let rel = project_open(&relcx, rel)?;
    backend_storage_buffer_bufmgr_seams::extend_buffered_rel_by_main::call(
        &rel,
        has_strategy,
        extend_by,
    )
}

/// `RelationGetNumberOfBlocks(rel)` (bufmgr.h) == `relation_get_number_of_blocks_in_fork(rel, MAIN_FORKNUM)`, OID-keyed.
fn hio_relation_get_number_of_blocks(
    rel: Oid,
) -> PgResult<types_core::primitive::BlockNumber> {
    let relcx = mcx::MemoryContext::new("relation_get_number_of_blocks");
    let rel = project_open(&relcx, rel)?;
    backend_storage_buffer_bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
        &rel,
        types_core::primitive::ForkNumber::MAIN_FORKNUM,
    )
}

/// `visibilitymap_pin(rel, heapBlk, &vmbuf)` (visibilitymap.c), OID-keyed.
fn visibilitymap_pin(
    rel: Oid,
    heap_blk: types_core::primitive::BlockNumber,
    vmbuf: types_storage::storage::Buffer,
) -> PgResult<types_storage::storage::Buffer> {
    let relcx = mcx::MemoryContext::new("visibilitymap_pin");
    let rel = project_open(&relcx, rel)?;
    backend_access_heap_visibilitymap_seams::visibilitymap_pin::call(rel, heap_blk, vmbuf)
}

/// `RELATION_IS_LOCAL(rel)` (utils/rel.h) == `rd_islocaltemp || rd_createSubid
/// != InvalidSubTransactionId`, off the owned entry.
fn hio_relation_is_local(rel: Oid) -> PgResult<bool> {
    with_entry(rel, |rd| {
        rd.rd_islocaltemp
            || rd.rd_createSubid != types_core::xact::InvalidSubTransactionId
    })
}

/// `RelationGetRelationName(rel)` (utils/rel.h), off the owned entry.
fn hio_relation_get_relation_name(rel: Oid) -> PgResult<String> {
    with_entry(rel, |rd| rd.rd_rel.relname.to_string())
}

/// `RelationExtensionLockWaiterCount(rel)` (lmgr.c): build the relation's
/// `LockRelId` (`relId = rd_id`, `dbId = relisshared ? InvalidOid :
/// MyDatabaseId`) off the owned entry and delegate to the lmgr owner.
fn relation_extension_lock_waiter_count(rel: Oid) -> PgResult<u32> {
    let (relid, relisshared) =
        crate::core_entry_store::with_relation(rel, |rd| (rd.rd_id, rd.rd_rel.relisshared))?;
    let db_id = if relisshared {
        types_core::InvalidOid
    } else {
        backend_utils_init_small_seams::my_database_id::call()
    };
    let lock_rel_id = types_storage::lock::LockRelId {
        relId: relid,
        dbId: db_id,
    };
    let n = backend_storage_lmgr_lmgr_seams::relation_extension_lock_waiter_count::call(
        lock_rel_id,
    )?;
    Ok(n as u32)
}

/// `GetPageWithFreeSpace(relation, len)` (freespace.c) as an OID-keyed hio-seam:
/// project the owned entry to a transient `Relation` read handle (no release
/// authority) and call the FSM owner's `&Relation`-keyed seam.
fn get_page_with_free_space(
    rel: Oid,
    len: types_core::Size,
) -> PgResult<types_core::primitive::BlockNumber> {
    let relcx = mcx::MemoryContext::new("get_page_with_free_space");
    let data = crate::core_entry_store::with_relation(rel, |rd| {
        crate::build::project_relation_data(relcx.mcx(), rd)
    })??;
    let rel = types_rel::Relation::open(data, None);
    backend_storage_freespace_seams::get_page_with_free_space::call(&rel, len)
}

/// `RecordAndGetPageWithFreeSpace(relation, oldPage, oldSpaceAvail,
/// spaceNeeded)` (freespace.c) as an OID-keyed hio-seam: project the owned entry
/// to a transient `Relation` read handle and call the FSM owner's seam.
fn record_and_get_page_with_free_space(
    rel: Oid,
    old_page: types_core::primitive::BlockNumber,
    old_avail: types_core::Size,
    needed: types_core::Size,
) -> PgResult<types_core::primitive::BlockNumber> {
    let relcx = mcx::MemoryContext::new("record_and_get_page_with_free_space");
    let data = crate::core_entry_store::with_relation(rel, |rd| {
        crate::build::project_relation_data(relcx.mcx(), rd)
    })??;
    let rel = types_rel::Relation::open(data, None);
    backend_storage_freespace_seams::record_and_get_page_with_free_space::call(
        &rel, old_page, old_avail, needed,
    )
}

/// `RelationGetTargetBlock(relation)` (utils/rel.h) ==
/// `RelationGetSmgr(relation)->smgr_targblock` (or `InvalidBlockNumber` when
/// `rd_smgr` is NULL). A cached insertion hint; reads smgr-owned state keyed by
/// the relation's `(rd_locator, rd_backend)`.
fn relation_get_target_block(rel: Oid) -> PgResult<types_core::primitive::BlockNumber> {
    let (locator, backend) =
        crate::core_entry_store::with_relation(rel, |rd| (rd.rd_locator, rd.rd_backend))?;
    Ok(backend_storage_smgr_seams::smgrgettargblock::call(locator, backend))
}

/// `RelationSetTargetBlock(relation, target_block)` (utils/rel.h) — writes
/// `RelationGetSmgr(relation)->smgr_targblock`. The relation's `(rd_locator,
/// rd_backend)` come off the owned entry; the hint lives in smgr-owned state.
fn relation_set_target_block(
    rel: Oid,
    target_block: types_core::primitive::BlockNumber,
) -> PgResult<()> {
    let (locator, backend) =
        crate::core_entry_store::with_relation(rel, |rd| (rd.rd_locator, rd.rd_backend))?;
    backend_storage_smgr_seams::smgrsettargblock::call(locator, backend, target_block)
}

/// `RelationGetTargetPageFreeSpace(relation, defaultff)` (utils/rel.h) ==
/// `BLCKSZ * (100 - RelationGetFillFactor(relation, defaultff)) / 100`. The
/// fillfactor comes off the relcache entry's `rd_options` (defaulting to
/// `defaultff` when unset).
fn relation_get_target_page_free_space(rel: Oid, defaultff: i32) -> PgResult<usize> {
    let fillfactor = with_entry(rel, |rd| rd.get_fillfactor(defaultff))?;
    Ok(types_core::primitive::BLCKSZ * (100 - fillfactor as usize) / 100)
}

// --- vacuumlazy.c inline relcache reads ---

/// `RelationGetRelationName(rel)` = `rel->rd_rel->relname`.
fn vac_relation_get_relation_name(rel: &types_rel::Relation<'_>) -> PgResult<String> {
    with_entry(rel.rd_id, |rd| rd.rd_rel.relname.to_string())
}

/// `rel->rd_rel->reltuples` (float4) widened to f64.
fn vac_relation_get_reltuples(rel: &types_rel::Relation<'_>) -> PgResult<f64> {
    with_entry(rel.rd_id, |rd| rd.rd_rel.reltuples as f64)
}

/// `RelationNeedsWAL(rel)`.
fn vac_relation_needs_wal(rel: &types_rel::Relation<'_>) -> PgResult<bool> {
    Ok(relation_needs_wal(rel))
}

/// `RelationUsesLocalBuffers(rel)` = `rel->rd_rel->relpersistence ==
/// RELPERSISTENCE_TEMP`.
fn vac_relation_uses_local_buffers(rel: &types_rel::Relation<'_>) -> PgResult<bool> {
    const RELPERSISTENCE_TEMP: i8 = b't' as i8;
    with_entry(rel.rd_id, |rd| {
        rd.rd_rel.relpersistence == RELPERSISTENCE_TEMP
    })
}

/// `relation_rules(mcx, reloid)` — the per-query rewrite-rule reader for
/// `rewriteHandler.c`. Fetch the relcache entry (the C `RelationIdGetRelation`),
/// and if its `rd_rules` is set re-project the whole `RuleLock` into the
/// caller's `mcx` arena by deep-copying each rule's `qual`/`actions`
/// (`Node::clone_in`/`Query::clone_in`, the C `copyObject`). The cached trees
/// live in the process-lifetime CacheMemoryContext (`'static`); copying them
/// into the per-query arena is exactly what the C rewriter does before mutating
/// a rule action list, and decouples the returned image from a possible
/// mid-query cache invalidation/rebuild. `Ok(None)` is the C `rd_rules == NULL`.
fn relation_rules(
    mcx: Mcx<'_>,
    reloid: Oid,
) -> PgResult<Option<sx::RuleLockImage<'_>>> {
    // `with_relation` is the `Oid`-keyed scoped immutable borrow; it errors
    // (loud) if `reloid` names no live relcache entry — the same caller contract
    // as C, where the relation must already be open/pinned before the rewriter
    // reads its rules. The closure builds the re-projected image while the entry
    // is borrowed; `?`-flatten the inner deep-copy `PgResult`.
    crate::core_entry_store::with_relation(reloid, |rd| {
        let rule_lock = match &rd.rd_rules {
            // C `rd_rules == NULL`: relation has no rewrite rules.
            None => return Ok(None),
            Some(rl) => rl,
        };
        let mut rules: PgVec<sx::RewriteRuleImage<'_>> =
            mcx::vec_with_capacity_in(mcx, rule_lock.rules.len())?;
        for r in rule_lock.rules.iter() {
            // `qual = copyObject(rule->qual)` — re-home the qualification Node.
            let qual = match &r.qual {
                Some(q) => Some(mcx::alloc_in(mcx, q.clone_in(mcx)?)?),
                None => None,
            };
            // `actions = copyObject(rule->actions)` — re-home each action Query.
            let mut actions: PgVec<types_nodes::copy_query::Query<'_>> =
                mcx::vec_with_capacity_in(mcx, r.actions.len())?;
            for a in r.actions.iter() {
                actions.push(a.clone_in(mcx)?);
            }
            rules.push(sx::RewriteRuleImage {
                ruleId: r.ruleId,
                event: r.event,
                enabled: r.enabled,
                isInstead: r.isInstead,
                qual,
                actions,
            });
        }
        Ok(Some(sx::RuleLockImage { rules }))
    })?
}

/// `relation_row_security(mcx, reloid)` — the per-query row-security policy
/// reader for `rowsecurity.c`. Fetch the relcache entry, and if its `rd_rsdesc`
/// is set re-project every `RowSecurityPolicy` into the caller's `mcx` arena by
/// deep-copying each policy's `qual`/`with_check_qual` (`Node::clone_in`, the C
/// `copyObject` the rewriter performs before re-pointing a qual's Vars) and
/// copying the scalar fields. `Ok(None)` is the C `rd_rsdesc == NULL`.
fn relation_row_security(
    mcx: Mcx<'_>,
    reloid: Oid,
) -> PgResult<Option<PgVec<sx::RowSecurityPolicyImage<'_>>>> {
    crate::core_entry_store::with_relation(reloid, |rd| {
        let rsdesc = match &rd.rd_rsdesc {
            // C `rd_rsdesc == NULL`: RLS disabled / no policies.
            None => return Ok(None),
            Some(d) => d,
        };
        let mut policies: PgVec<sx::RowSecurityPolicyImage<'_>> =
            mcx::vec_with_capacity_in(mcx, rsdesc.policies.len())?;
        for p in rsdesc.policies.iter() {
            let policy_name = mcx::PgString::from_str_in(p.policy_name.as_str(), mcx)
                .map_err(|_| mcx.oom(p.policy_name.as_str().len()))?;
            let mut roles: PgVec<Oid> = mcx::vec_with_capacity_in(mcx, p.roles.len())?;
            for &r in p.roles.iter() {
                roles.push(r);
            }
            let qual = match &p.qual {
                Some(q) => Some(mcx::alloc_in(mcx, q.clone_in(mcx)?)?),
                None => None,
            };
            let with_check_qual = match &p.with_check_qual {
                Some(q) => Some(mcx::alloc_in(mcx, q.clone_in(mcx)?)?),
                None => None,
            };
            policies.push(sx::RowSecurityPolicyImage {
                policy_name,
                polcmd: p.polcmd,
                roles,
                permissive: p.permissive,
                qual,
                with_check_qual,
                hassublinks: p.hassublinks,
            });
        }
        Ok(Some(policies))
    })?
}

/// `matview_rule_info(rel)` — the `matviewRel->rd_rel->relhasrules` /
/// `matviewRel->rd_rules->...` shape `RefreshMatViewByOid` branches on (matview.c
/// 216-243), read off the live matview relcache entry. `RelationData` owns
/// `rd_rules` (the RuleLock carrier), so the relcache reports this. Mirrors the C
/// reads: `relhasrules`, `numLocks` (`< 0` when `rd_rules == NULL`), and for the
/// first rule `event == CMD_SELECT`, `isInstead`, and `list_length(actions)`.
fn matview_rule_info(rel: Oid) -> PgResult<types_matview::MatViewRuleInfo> {
    use types_nodes::nodes::CmdType;
    crate::core_entry_store::with_relation(rel, |rd| {
        let relhasrules = rd.rd_rel.relhasrules;
        match &rd.rd_rules {
            // C `rd_rules == NULL`: `numLocks` is read as `< 1` so the caller
            // raises "missing rewrite information". Report `num_rules < 0` and
            // leave the first-rule fields at their never-inspected defaults.
            None => Ok(types_matview::MatViewRuleInfo {
                relhasrules,
                num_rules: -1,
                rule_is_select: false,
                rule_is_instead: false,
                rule_actions_length: 0,
            }),
            Some(rl) => {
                let num_rules = rl.rules.len() as i32;
                // The caller checks `num_rules < 1` / `> 1` before inspecting the
                // first rule; mirror C by reading `rules[0]` only when present.
                let (rule_is_select, rule_is_instead, rule_actions_length) =
                    match rl.rules.first() {
                        Some(r) => (
                            r.event == CmdType::CMD_SELECT,
                            r.isInstead,
                            r.actions.len() as i32,
                        ),
                        None => (false, false, 0),
                    };
                Ok(types_matview::MatViewRuleInfo {
                    relhasrules,
                    num_rules,
                    rule_is_select,
                    rule_is_instead,
                    rule_actions_length,
                })
            }
        }
    })?
}

/// `matview_data_query(mcx, rel)` — `dataQuery = linitial_node(Query,
/// rule->actions)` (matview.c 374): the matview's stored data query, read off the
/// first (only) rewrite rule's first action. The cached `Query` lives in the
/// process-lifetime cache arena, so it is deep-copied into the caller's
/// per-command `mcx` (`Query::clone_in`, the C `copyObject` is implicit when the
/// rewriter later copies it). The caller has already validated the rule shape
/// (`matview_rule_info`), so a missing rule/action is an internal error.
fn matview_data_query<'mcx>(
    mcx: Mcx<'mcx>,
    rel: Oid,
) -> PgResult<types_nodes::copy_query::Query<'mcx>> {
    crate::core_entry_store::with_relation(rel, |rd| {
        let internal = |msg: &str| {
            backend_utils_error::ereport(types_error::ERROR)
                .errmsg_internal(msg.to_string())
                .into_error()
        };
        let rl = rd
            .rd_rules
            .as_ref()
            .ok_or_else(|| internal("materialized view is missing rewrite information"))?;
        let rule = rl
            .rules
            .first()
            .ok_or_else(|| internal("materialized view is missing rewrite information"))?;
        let action = rule
            .actions
            .first()
            .ok_or_else(|| internal("the rule for materialized view is not a single action"))?;
        action.clone_in(mcx)
    })?
}

#[cfg(test)]
mod relation_rules_tests {
    //! The `relation_rules` reader keystone: prove that a relcache entry holding
    //! `rd_rules` whose `RewriteRule.actions` are whole `Query<'static>` trees in
    //! the process-lifetime CacheMemoryContext re-projects those trees into a
    //! fresh per-query `'mcx` arena (the C `copyObject`), unblocking
    //! `rewriteHandler.c`'s rule reads off the trimmed per-query handle.

    use crate::core_entry_store;
    use crate::core_entry_store::entry::{RelationData, RewriteRule, RuleLock};
    use mcx::MemoryContext;
    use std::cell::RefCell;
    use std::rc::Rc;
    use types_core::primitive::Oid;
    use types_nodes::copy_query::Query;
    use types_nodes::nodes::CmdType;

    /// Install a relcache entry (OID `reloid`) whose `rd_rules` holds one rule
    /// with one action `Query` of `command`, allocated in the cache arena — the
    /// shape `RelationBuildRuleLock` produces. Then call the `relation_rules`
    /// reader with a SEPARATE per-query context and assert the rule re-projects.
    #[test]
    fn relation_with_rules_reprojects_query_trees_into_fresh_mcx() {
        const RELOID: Oid = 99001;

        // Build the cached rule tree in the process-lifetime cache arena.
        let cache_mcx = crate::derived::cache_memory_context();
        let mut q = Query::new(cache_mcx);
        q.commandType = CmdType::CMD_SELECT;
        let mut actions = mcx::PgVec::new_in(cache_mcx);
        actions.push(q);
        let mut rules = mcx::PgVec::new_in(cache_mcx);
        rules.push(RewriteRule {
            ruleId: 42,
            event: CmdType::CMD_SELECT,
            enabled: b'O',
            isInstead: true,
            qual: None,
            actions,
        });
        let lock: mcx::PgBox<'static, RuleLock> =
            mcx::alloc_in(cache_mcx, RuleLock { rules }).expect("alloc RuleLock");

        // Place the entry in the id_cache, exactly as a relcache build would.
        let mut entry = RelationData::default();
        entry.rd_id = RELOID;
        entry.rd_rules = Some(lock);
        core_entry_store::with_state(|st| {
            st.id_cache.insert(RELOID, Rc::new(RefCell::new(entry)));
        });

        // Read through the seam adapter with a DISTINCT per-query context.
        let query_ctx = MemoryContext::new("relation_rules_test_mcx");
        let image = super::relation_rules(query_ctx.mcx(), RELOID)
            .expect("relation_rules ok")
            .expect("rd_rules present -> Some image");

        assert_eq!(image.rules.len(), 1);
        let r = &image.rules[0];
        assert_eq!(r.ruleId, 42);
        assert_eq!(r.event, CmdType::CMD_SELECT);
        assert_eq!(r.enabled, b'O');
        assert!(r.isInstead);
        assert!(r.qual.is_none());
        assert_eq!(r.actions.len(), 1);
        // The re-projected action carries the original command type — the deep
        // copy preserved the tree, now living in the per-query arena.
        assert_eq!(r.actions[0].commandType, CmdType::CMD_SELECT);

        core_entry_store::with_state(|st| {
            st.id_cache.remove(&RELOID);
        });
    }

    /// A relation with no rewrite rules (`rd_rules == NULL`) yields `Ok(None)`.
    #[test]
    fn relation_without_rules_yields_none() {
        const RELOID: Oid = 99002;
        let mut entry = RelationData::default();
        entry.rd_id = RELOID;
        // rd_rules left None (the C NULL).
        core_entry_store::with_state(|st| {
            st.id_cache.insert(RELOID, Rc::new(RefCell::new(entry)));
        });

        let query_ctx = MemoryContext::new("relation_rules_none_test_mcx");
        let image = super::relation_rules(query_ctx.mcx(), RELOID).expect("relation_rules ok");
        assert!(image.is_none());

        core_entry_store::with_state(|st| {
            st.id_cache.remove(&RELOID);
        });
    }
}

/// `criticalRelcachesBuilt` (relcache.c) — the owned per-backend flag set once
/// the critical relcache entries are built (gates catcache indexscans).
fn critical_relcaches_built() -> bool {
    crate::core_entry_store::with_state(|st| st.critical_relcaches_built)
}

/// `criticalSharedRelcachesBuilt` (relcache.c) — the owned per-backend flag set
/// once the critical *shared* relcache entries are built.
fn critical_shared_relcaches_built() -> bool {
    crate::core_entry_store::with_state(|st| st.critical_shared_relcaches_built)
}

/// `RelationGetIndexExpressions(index)` (relcache.c:5096): the index's
/// expression trees. The C quick-exits to `NIL` when `rd_indextuple == NULL ||
/// heap_attisnull(rd_indextuple, Anum_pg_index_indexprs)`. The owned entry
/// carries the full `indkey` vector, and an index has expression columns iff
/// some `indkey[i] == InvalidAttrNumber` (the on-disk marker; `pg_index.indexprs`
/// is non-NULL exactly when such a column exists). So a non-index relation
/// (`rd_index == None`) or one with no zero `indkey` entry returns `Ok(None)`
/// (== NIL) — the path every system-catalog index (all simple-column) takes.
/// When an expression column IS present, the `indexprs` node-tree decode
/// (`stringToNode`/`eval_const_expressions`/`fix_opfuncids`) is node vocabulary
/// owned cross-unit and unported, so it routes through the node-tree owner seam
/// (mirror-PG-and-panic until `stringToNode` lands).
fn relation_get_index_expressions<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
) -> PgResult<Option<PgVec<'mcx, types_nodes::Expr>>> {
    let has_expression_col = crate::core_entry_store::with_relation(rel.rd_id, |rd| {
        match &rd.rd_index {
            None => false,
            Some(idx) => idx
                .indkey
                .iter()
                .any(|&k| k == types_core::primitive::InvalidAttrNumber),
        }
    })?;
    if !has_expression_col {
        // NIL — no expression columns.
        return Ok(None);
    }
    // An expression column is present: the `indexprs` node-tree decode
    // (`stringToNode`/`eval_const_expressions`/`fix_opfuncids`) is node
    // vocabulary owned cross-unit; route through the node-tree owner seam, which
    // returns the decoded expression list in `mcx`. The owned entry does not
    // carry the C's `rd_indexprs` memoization, so the tree is re-derived per
    // call (faithful behavior, minus the cache).
    backend_utils_cache_relcache_nodexform_seams::index_expressions::call(mcx, rel.rd_id)
}

/// `RelationGetIndexPredicate(index)` (relcache.c:5210): the index's partial
/// predicate tree. The C quick-exits to `NIL` when `rd_indextuple == NULL ||
/// heap_attisnull(rd_indextuple, Anum_pg_index_indpred)` — i.e. the relation is
/// not an index, or the index has no partial predicate. `indpred`-nullity is
/// observable faithfully off the cached `pg_index` tuple via the syscache owner
/// (`pg_index_has_predicate` == `!heap_attisnull(rd_indextuple,
/// Anum_pg_index_indpred)`), exactly the C test (no node tree materialized). A
/// non-index relation (`rd_index == None`) or a non-partial index returns
/// `Ok(None)` (== NIL) — the path every system-catalog index (none are partial)
/// takes. When a real predicate IS present, the `indpred` node-tree decode
/// (`stringToNode`/`eval_const_expressions`/`canonicalize_qual`/
/// `make_ands_implicit`/`fix_opfuncids`) is node vocabulary owned cross-unit and
/// unported, so it routes through the node-tree owner seam (mirror-PG-and-panic
/// until `stringToNode` lands).
fn relation_get_index_predicate<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
) -> PgResult<Option<PgVec<'mcx, types_nodes::Expr>>> {
    // Quick exit when the relation is not an index (C `rd_indextuple == NULL`).
    let is_index = crate::core_entry_store::with_relation(rel.rd_id, |rd| rd.rd_index.is_some())?;
    if !is_index {
        return Ok(None);
    }
    // Quick exit when the index has no partial predicate
    // (`heap_attisnull(rd_indextuple, Anum_pg_index_indpred)`), read faithfully
    // off the cached pg_index tuple via the syscache owner. A cache miss maps to
    // "no predicate" (NIL).
    let has_predicate =
        backend_utils_cache_syscache_seams::pg_index_has_predicate::call(rel.rd_id)?
            .unwrap_or(false);
    if !has_predicate {
        // NIL — not a partial index.
        return Ok(None);
    }
    // A real predicate is present: the `indpred` node-tree decode
    // (`stringToNode`/`eval_const_expressions`/`canonicalize_qual`/
    // `make_ands_implicit`/`fix_opfuncids`) is node vocabulary owned cross-unit;
    // route through the node-tree owner seam, which returns the implicit-AND
    // predicate list in `mcx`. The owned entry does not carry the C's
    // `rd_indpred` memoization, so the tree is re-derived per call.
    backend_utils_cache_relcache_nodexform_seams::index_predicate::call(mcx, rel.rd_id)
}

/// `RelationGetDummyIndexExpressions(relation)` (relcache.c:5156): like
/// `RelationGetIndexExpressions`, but returns null `Const`s of the right
/// type/typmod/collation in place of the real index expressions (used by
/// `BuildDummyIndexInfo` to avoid running user code on TRUNCATE of an
/// expression index). The C quick-exits to `NIL` when `rd_indextuple == NULL ||
/// heap_attisnull(rd_indextuple, Anum_pg_index_indexprs)` — i.e. not an index,
/// or the index has no expression columns. The owned entry observes "has an
/// expression column" exactly as `RelationGetIndexExpressions` does: an index
/// with a stored expression has at least one `indkey[i] == InvalidAttrNumber`
/// (the on-disk marker; `pg_index.indexprs` is non-NULL iff such a column
/// exists). So a non-index relation (`rd_index == None`) or one with no zero
/// `indkey` entry returns `Ok(None)` (== NIL) — the path every system-catalog
/// index (all simple-column) takes. When an expression column IS present, the
/// `indexprs` node-tree decode (`stringToNode`) + `makeConst`/`exprType`/
/// `exprTypmod`/`exprCollation` are node vocabulary owned cross-unit and
/// unported, so it routes through the node-tree owner seam (mirror-PG-and-panic
/// until `stringToNode` lands).
fn relation_get_dummy_index_expressions<'mcx>(
    mcx: Mcx<'mcx>,
    index: &types_rel::Relation<'mcx>,
) -> PgResult<Option<PgVec<'mcx, types_nodes::primnodes::Expr>>> {
    let has_expression_col = crate::core_entry_store::with_relation(index.rd_id, |rd| {
        match &rd.rd_index {
            None => false,
            Some(idx) => idx
                .indkey
                .iter()
                .any(|&k| k == types_core::primitive::InvalidAttrNumber),
        }
    })?;
    if !has_expression_col {
        // NIL — not an index, or no expression columns.
        return Ok(None);
    }
    // An expression column is present: the `indexprs` node-tree decode
    // (`stringToNode`) + the dummy-`Const` build (`makeConst` over
    // `exprType`/`exprTypmod`/`exprCollation`) is node vocabulary owned
    // cross-unit; route through the node-tree owner seam, which returns the
    // dummy null-`Const` list in `mcx`. The owned entry does not carry the C's
    // memoization, so the tree is re-derived per call.
    backend_utils_cache_relcache_nodexform_seams::dummy_index_expressions::call(mcx, index.rd_id)
}

/// `RelationGetExclusionInfo(indexRelation, &operators, &procs, &strategies)`
/// (relcache.c:5653): the exclusion operator/proc/strategy arrays for an
/// exclusion-constraint (or WITHOUT OVERLAPS PK/unique) index. The C body
/// quick-exits from `rd_exclstrats` when cached, else scans `pg_constraint`
/// (on `conrelid`), decodes the `conexclop` 1-D Oid array, then per key column
/// resolves `get_opcode` (proc) and `get_op_opfamily_strategy` (strategy) and
/// caches the three arrays on the entry. The catalog scan + `conexclop` decode
/// + lsyscache lookups are cross-unit primitives, owned by the genam owner and
/// reached via the `derived` family's `exclusion_info_seam`
/// (`genam::relcache_exclusion_info`, a real ported body — NOT a node-tree
/// `stringToNode` decode; `conexclop` is a plain Oid array). The owned
/// `RelationGetExclusionInfo` runs the scan + caches into the entry; this
/// adapter then reads the cached arrays back and copies them into the caller's
/// `mcx` as the three parallel per-key `PgVec`s that `BuildIndexInfo` stores in
/// `ii_ExclusionOps`/`ii_ExclusionProcs`/`ii_ExclusionStrats`.
fn relation_get_exclusion_info<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
) -> PgResult<(PgVec<'mcx, Oid>, PgVec<'mcx, Oid>, PgVec<'mcx, u16>)> {
    // Run the scan + cache the results on the owned entry (own logic + genam
    // seam). This is the C `RelationGetExclusionInfo` body; the quick-exit when
    // `rd_exclstrats` is already populated is handled inside.
    crate::derived::RelationGetExclusionInfo(rel.rd_id)?;

    // Copy the cached arrays into the caller's context (C palloc's the result in
    // the caller's context and memcpy's from the cached copies).
    let (cops, cprocs, cstrats) = crate::core_entry_store::with_relation(rel.rd_id, |rd| {
        (
            rd.rd_exclops.clone(),
            rd.rd_exclprocs.clone(),
            rd.rd_exclstrats.clone(),
        )
    })?;

    let mut ops = PgVec::new_in(mcx);
    for o in cops {
        ops.push(o);
    }
    let mut procs = PgVec::new_in(mcx);
    for p in cprocs {
        procs.push(p);
    }
    let mut strats = PgVec::new_in(mcx);
    for s in cstrats {
        strats.push(s);
    }
    Ok((ops, procs, strats))
}

/// `AssertCouldGetRelation()` (relcache.c) — an assertion-build-only check;
/// a no-op in non-assert builds, mirroring the C macro that compiles away.
fn assert_could_get_relation() {}

/// `rel->rd_indcollation[attno - 1]` — the collation OID of index column
/// `attno` (1-based, as in C), read off the cached index entry's
/// `rd_indcollation` array. `Err` only on a relcache miss.
fn rd_indcollation(index: &types_rel::Relation<'_>, attno: AttrNumber) -> PgResult<Oid> {
    crate::core_entry_store::with_relation(index.rd_id, |rd| {
        rd.rd_indcollation[(attno as usize) - 1]
    })
}

/// `index_getprocid(irel, attnum, procnum)` (indexam.c) — the OID of the
/// support procedure `procnum` for index column `attnum` (1-based), read off
/// `irel->rd_support`. The procindex arithmetic (`nproc*(attnum-1) +
/// (procnum-1)`, where `nproc = rd_indam->amsupport`) and the `procnum` range
/// assert mirror the C. The relcache owner holds both `rd_indam` (for
/// `amsupport`) and the `rd_support` array, so this is a pure owned-store read
/// (the same data the sibling `indexam::index_getprocid` install reaches via
/// the `rd_support_at` seam).
fn index_getprocid(
    index: &types_rel::Relation<'_>,
    attnum: AttrNumber,
    procnum: u16,
) -> PgResult<RegProcedure> {
    let nproc = relation_rd_indam(index.rd_id)
        .map(|am| am.amsupport)
        .unwrap_or(0);
    debug_assert!(procnum > 0 && procnum <= nproc);
    let procindex = (nproc as i32) * ((attnum as i32) - 1) + ((procnum as i32) - 1);
    crate::core_entry_store::with_relation(index.rd_id, |rd| rd.rd_support[procindex as usize])
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

fn relation_project_existing<'mcx>(
    mcx: Mcx<'mcx>,
    relation_id: Oid,
) -> PgResult<Option<types_rel::RelationData<'mcx>>> {
    // Pin-free projection of an already-pinned entry: no RelationIdGetRelation
    // (and so no second `rd_refcnt += 1`); just project the live entry.
    match crate::core_entry_store::cache_lookup(relation_id) {
        Some(rd) => crate::core_entry_store::with_relation(rd, |r| {
            crate::build::project_relation_data(mcx, r)
        })?
        .map(Some),
        None => Ok(None),
    }
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

fn relation_id_get_relation_cell(
    relation_id: Oid,
) -> PgResult<
    Option<std::rc::Rc<std::cell::RefCell<crate::core_entry_store::RelationData>>>,
> {
    // Pin-free dual-carry cell fetch: clone the cell of an already-pinned
    // entry without a second `rd_refcnt` increment (see the seam doc).
    crate::core_entry_store::relation_id_get_relation_cell(relation_id)
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
        let used_as_catalog_table = rd.rd_options.as_ref().and_then(|o| o.std()).is_some_and(|o| {
            (rd.rd_rel.relkind == RELKIND_RELATION || rd.rd_rel.relkind == RELKIND_MATVIEW)
                && o.user_catalog_table
        });
        xlog_logical_info_active
            && relation_needs_wal
            && (backend_catalog_catalog_seams::is_catalog_relation_oid::call(rd.rd_id)
                || used_as_catalog_table)
    })
}

fn is_toast_relation(relation_id: Oid) -> PgResult<bool> {
    // IsToastRelation(relation) (catalog.c): IsToastNamespace(
    //   RelationGetNamespace(relation)). RelationGetNamespace is
    // `rd_rel->relnamespace`; the pg_toast-namespace test is the catalog owner's.
    let relnamespace =
        crate::core_entry_store::with_relation(relation_id, |rd| rd.rd_rel.relnamespace)?;
    Ok(backend_catalog_catalog_seams::is_toast_namespace::call(relnamespace))
}

fn relation_is_logically_logged(relation_id: Oid) -> PgResult<bool> {
    // RelationIsLogicallyLogged(relation) (rel.h):
    //   XLogLogicalInfoActive() && RelationNeedsWAL(relation) &&
    //   relkind != RELKIND_FOREIGN_TABLE && !IsCatalogRelation(relation)
    // expanded exactly as the C macros. XLogLogicalInfoActive() is
    // `wal_level >= WAL_LEVEL_LOGICAL`; RelationNeedsWAL is the permanent &&
    // (XLogIsNeeded() || not-newly-created) test; both read owned-store fields
    // and the wal_level GUC, so resolve the live entry (Err propagates a miss).
    use types_core::xact::InvalidSubTransactionId;
    use types_wal::xlog_consts::{WAL_LEVEL_LOGICAL, WAL_LEVEL_REPLICA};
    const RELPERSISTENCE_PERMANENT: i8 = b'p' as i8;
    const RELKIND_FOREIGN_TABLE: i8 = b'f' as i8;
    let wal = backend_access_transam_xlog_seams::wal_level::call();
    let xlog_logical_info_active = wal >= WAL_LEVEL_LOGICAL;
    crate::core_entry_store::with_relation(relation_id, |rd| {
        let relation_needs_wal = rd.rd_rel.relpersistence == RELPERSISTENCE_PERMANENT
            && (wal >= WAL_LEVEL_REPLICA
                || (rd.rd_createSubid == InvalidSubTransactionId
                    && rd.rd_firstRelfilelocatorSubid == InvalidSubTransactionId));
        xlog_logical_info_active
            && relation_needs_wal
            && rd.rd_rel.relkind != RELKIND_FOREIGN_TABLE
            && !backend_catalog_catalog_seams::is_catalog_relation_oid::call(rd.rd_id)
    })
}

fn rd_rel_relrewrite(relation_id: Oid) -> PgResult<Oid> {
    crate::core_entry_store::with_relation(relation_id, |rd| rd.rd_rel.relrewrite)
}

fn rd_rel_relkind_by_oid(relation_id: Oid) -> PgResult<i8> {
    crate::core_entry_store::with_relation(relation_id, |rd| rd.rd_rel.relkind as i8)
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

/// `ResOwnerReleaseRelation(Datum res)` (relcache.c) — release a leaked
/// relcache pin during resource-owner release. The relation is identified by
/// its `Oid` handle (the entry key).
fn release_relation_ref(relid: Oid) -> PgResult<()> {
    crate::core_entry_store::ResOwnerReleaseRelation(relid)
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

    crate::core_entry_store::with_relation(index_oid, |rd| rd.rd_supportinfo[pi].clone())
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

// --- commands/vacuum.c vacuum_rel by-OID rd_rel / rd_options / rd_lockInfo ---
fn rel_frozenxid_minmxid(rel: Oid) -> PgResult<(TransactionId, MultiXactId)> {
    with_entry(rel, |rd| (rd.rd_rel.relfrozenxid, rd.rd_rel.relminmxid))
}
fn rel_pages_tuples(rel: Oid) -> PgResult<(types_core::primitive::BlockNumber, f64)> {
    // `relpages` is stored as `int32`, `reltuples` as `float4`; widen the
    // latter to `f64` for the caller (`vac_estimate_reltuples`).
    with_entry(rel, |rd| {
        (rd.rd_rel.relpages as u32, rd.rd_rel.reltuples as f64)
    })
}
fn rel_relowner(rel: Oid) -> PgResult<Oid> {
    with_entry(rel, |rd| rd.rd_rel.relowner)
}
fn rel_reltoastrelid(rel: Oid) -> PgResult<Oid> {
    with_entry(rel, |rd| rd.rd_rel.reltoastrelid)
}
fn rel_rd_toastoid(rel: Oid) -> PgResult<Oid> {
    with_entry(rel, |rd| rd.rd_toastoid)
}
fn rel_std_rd_options(rel: Oid) -> PgResult<sx::StdRdOptionsView> {
    with_entry(rel, |rd| match rd.rd_options.as_ref().and_then(|o| o.std()) {
        None => sx::StdRdOptionsView::default(),
        Some(opts) => sx::StdRdOptionsView {
            has_options: true,
            vacuum_index_cleanup: opts.vacuum_index_cleanup as u8,
            max_eager_freeze_failure_rate: opts.vacuum_max_eager_freeze_failure_rate,
            vacuum_truncate: Some((opts.vacuum_truncate_set, opts.vacuum_truncate)),
        },
    })
}
fn rel_lock_relid(rel: Oid) -> PgResult<types_storage::lock::LockRelId> {
    with_entry(rel, |rd| rd.rd_lockInfo.lockRelId)
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
fn rd_index_indnullsnotdistinct(index: &types_rel::Relation<'_>) -> PgResult<bool> {
    with_entry(index.rd_id, |rd| {
        rd.rd_index.as_ref().is_some_and(|i| i.indnullsnotdistinct)
    })
}
/// `index_usability_info(indexRel)` — the `Form_pg_index` fields plus the
/// `RelationGetIndexPredicate(indexRel) == NIL` test that `is_usable_unique_index`
/// (matview.c 914-949) inspects but that are not on the matview crate's trimmed
/// projection. `indisunique`/`indimmediate`/`indisvalid`/`indnatts`/`indkey` are
/// read straight off the live index relcache entry's `rd_index`; `pred_is_nil`
/// is `!RelationGetIndexPredicate(indexRel)`, evaluated via the same raw
/// `pg_index.indpred` attisnull test the other `rd_index_*` readers use (the
/// materialized predicate node tree is not needed for the NIL test). A cache
/// miss (no `rd_index`) yields the all-false / empty shape the caller treats as
/// "not usable".
fn index_usability_info(
    index: &types_rel::Relation<'_>,
) -> PgResult<types_matview::IndexUsabilityInfo> {
    // pred_is_nil = (RelationGetIndexPredicate(indexRel) == NIL).
    let pred_is_nil = !backend_utils_cache_syscache_seams::pg_index_has_predicate::call(index.rd_id)?
        .unwrap_or(false);
    with_entry(index.rd_id, |rd| match rd.rd_index.as_ref() {
        Some(i) => types_matview::IndexUsabilityInfo {
            indisunique: i.indisunique,
            indimmediate: i.indimmediate,
            indisvalid: i.indisvalid,
            pred_is_nil,
            indnatts: i.indnatts,
            indkey: i.indkey.clone(),
        },
        None => types_matview::IndexUsabilityInfo {
            indisunique: false,
            indimmediate: false,
            indisvalid: false,
            pred_is_nil,
            indnatts: 0,
            indkey: std::vec::Vec::new(),
        },
    })
}
/// `refresh_by_match_merge`'s per-usable-unique-index qual resolution
/// (matview.c 740-817). For each key column of the index, identify the equality
/// operator (via the column's opclass → opfamily/opcintype →
/// `get_opfamily_member_for_cmptype(.., COMPARE_EQ)`) and build the
/// `newdata.<col> = mv.<col>` operands (`quote_qualified_identifier`). The
/// `opUsedForQual` de-dup and the `generate_operator_clause` emission stay in
/// the matview driver; quals are returned in index-key order.
fn index_match_merge_quals(
    index: &types_rel::Relation<'_>,
    matview: &types_rel::Relation<'_>,
) -> PgResult<std::vec::Vec<types_matview::MatchMergeQual>> {
    use types_matview::MatchMergeQual;

    // `COMPARE_EQ` (access/cmptype.h) — the equality comparison type.
    const COMPARE_EQ: i32 = 3;

    // Read `indkey` (the key column attnums) and the relcache's per-key-column
    // opclass projection (`rd_opfamily`/`rd_opcintype`, which the relcache
    // populated from the index's `indclass` in `IndexSupportInitialize` — the C
    // here re-reads `indclass` off `rd_indextuple` then looks up
    // `get_opclass_opfamily_and_input_type(opclass)`, yielding exactly these).
    let (indkey, opfamilies, opcintypes) = with_entry(index.rd_id, |rd| {
        let indnkeyatts = rd.rd_index.as_ref().map(|i| i.indnkeyatts).unwrap_or(0) as usize;
        let indkey: std::vec::Vec<AttrNumber> = rd
            .rd_index
            .as_ref()
            .map(|i| i.indkey.iter().copied().take(indnkeyatts).collect())
            .unwrap_or_default();
        let opfamilies: std::vec::Vec<Oid> =
            rd.rd_opfamily.iter().copied().take(indnkeyatts).collect();
        let opcintypes: std::vec::Vec<Oid> =
            rd.rd_opcintype.iter().copied().take(indnkeyatts).collect();
        (indkey, opfamilies, opcintypes)
    })?;

    // Per-key-column matview attribute (attname / atttypid), read off the
    // matview's tuple descriptor (`TupleDescAttr(tupdesc, attnum - 1)`).
    struct Attr {
        attname: std::string::String,
        atttypid: Oid,
    }
    let attrs: std::vec::Vec<Attr> = crate::core_entry_store::with_relation(matview.rd_id, |rd| {
        indkey
            .iter()
            .map(|&attnum| {
                let a = rd.rd_att.attr((attnum - 1) as usize);
                Attr {
                    attname: a.attname.clone(),
                    atttypid: a.atttypid,
                }
            })
            .collect()
    })?;

    // A scratch context for the `quote_qualified_identifier` outputs (copied
    // into owned `String`s before it drops).
    let cxt = mcx::MemoryContext::new("matview match-merge quals");
    let mcx = cxt.mcx();

    let mut quals: std::vec::Vec<MatchMergeQual> = std::vec::Vec::with_capacity(indkey.len());
    for (i, attr) in attrs.iter().enumerate() {
        let attnum = indkey[i];
        let opfamily = opfamilies[i];
        let opcintype = opcintypes[i];
        let attrtype = attr.atttypid;

        // get_opfamily_member_for_cmptype(opfamily, opcintype, opcintype,
        // COMPARE_EQ).
        let op = backend_utils_cache_lsyscache_seams::get_opfamily_member_for_cmptype::call(
            opfamily,
            opcintype,
            opcintype,
            COMPARE_EQ,
        )?;
        if op == Oid::default() {
            return Err(backend_utils_error::ereport(types_error::ERROR)
                .errmsg_internal(format!(
                    "missing equality operator for ({opcintype},{opcintype}) in opfamily {opfamily}"
                ))
                .into_error());
        }

        // leftop = quote_qualified_identifier("newdata", attname);
        // rightop = quote_qualified_identifier("mv", attname);
        let leftop =
            backend_utils_adt_ruleutils_seams::quote_qualified_identifier::call(
                mcx,
                Some("newdata"),
                &attr.attname,
            )?
            .as_str()
            .to_string();
        let rightop =
            backend_utils_adt_ruleutils_seams::quote_qualified_identifier::call(
                mcx,
                Some("mv"),
                &attr.attname,
            )?
            .as_str()
            .to_string();

        quals.push(MatchMergeQual {
            attnum: attnum as i32,
            op,
            attrtype,
            leftop,
            rightop,
        });
    }

    Ok(quals)
}
fn rd_rel_relpersistence(rel: &types_rel::Relation<'_>) -> PgResult<i8> {
    with_entry(rel.rd_id, |rd| rd.rd_rel.relpersistence as i8)
}
fn rd_rel_relkind(rel: &types_rel::Relation<'_>) -> PgResult<i8> {
    with_entry(rel.rd_id, |rd| rd.rd_rel.relkind as i8)
}
fn rd_rel_relnatts(rel: &types_rel::Relation<'_>) -> PgResult<i16> {
    with_entry(rel.rd_id, |rd| rd.rd_rel.relnatts)
}
fn rd_rel_relispartition(rel: &types_rel::Relation<'_>) -> PgResult<bool> {
    with_entry(rel.rd_id, |rd| rd.rd_rel.relispartition)
}
/// `RelationGetDescr(rel)` — the relation's tuple descriptor. The C shares the
/// relcache's reference-counted `rd_att`; the safe port materializes an owned
/// `mcx`-backed copy via [`OwnedTupleDesc::project_in`] (the same projection the
/// relcache build family uses for `rd_att`).
fn relation_get_descr<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
) -> PgResult<PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>> {
    crate::core_entry_store::with_relation(rel.rd_id, |rd| {
        rd.rd_att.project_in(mcx, rd.rd_id)
    })?
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
    // `swap_relation_files` (cluster.c):
    //
    //   rel1 = relation_open(r1, NoLock);
    //   rel2 = relation_open(r2, NoLock);
    //   rel2->rd_createSubid = rel1->rd_createSubid;
    //   rel2->rd_newRelfilelocatorSubid = rel1->rd_newRelfilelocatorSubid;
    //   rel2->rd_firstRelfilelocatorSubid = rel1->rd_firstRelfilelocatorSubid;
    //   RelationAssumeNewRelfilelocator(rel1);
    //   relation_close(rel1, NoLock);
    //   relation_close(rel2, NoLock);
    //
    // Recognize that rel1's relfilenumber (swapped from rel2) is new in this
    // subtransaction. The rel2 storage (swapped from rel1) may or may not be new.
    //
    // The owned-model wrinkle: in C `relation_open` builds/pins the entry in the
    // relcache; only then are the `rd_*Subid` fields reachable. The new heap
    // (`r2` here, the transient CLUSTER work table) is not necessarily resident
    // in the cell map at this point, so we must open (build+pin) both relations
    // first — exactly as C does — before touching their entries. Previously this
    // seam called `assume_new_relfilelocator` directly on bare OIDs, which (a)
    // erroneously assumed-new on BOTH r1 and r2 (C does rel1 only), (b) skipped
    // the rel1->rel2 subid copy, and (c) errored with "no open relation" when r2
    // was not already resident — the VACUUM FULL / CLUSTER crash regression.
    let rel1 = crate::core_entry_store::RelationIdGetRelation(r1)?;
    if rel1 == types_core::InvalidOid {
        return Err(crate::core_entry_store::relcache_open_failed(r1));
    }
    let rel2 = crate::core_entry_store::RelationIdGetRelation(r2)?;
    if rel2 == types_core::InvalidOid {
        // Release rel1's pin before erroring out.
        let _ = crate::core_entry_store::RelationClose(rel1);
        return Err(crate::core_entry_store::relcache_open_failed(r2));
    }

    // rel2->rd_*Subid = rel1->rd_*Subid;
    let (create_subid, new_subid, first_subid) =
        crate::core_entry_store::with_relation(rel1, |r| {
            (
                r.rd_createSubid,
                r.rd_newRelfilelocatorSubid,
                r.rd_firstRelfilelocatorSubid,
            )
        })?;
    crate::core_entry_store::with_relation_mut(rel2, |r| {
        r.rd_createSubid = create_subid;
        r.rd_newRelfilelocatorSubid = new_subid;
        r.rd_firstRelfilelocatorSubid = first_subid;
    })?;

    // RelationAssumeNewRelfilelocator(rel1) — rel1 only.
    let assume_res = assume_new_relfilelocator(r1);

    // relation_close(rel1, NoLock); relation_close(rel2, NoLock):
    // release the pins taken by RelationIdGetRelation above (always, even on the
    // assume-new error path).
    let close1 = crate::core_entry_store::RelationClose(rel1);
    let close2 = crate::core_entry_store::RelationClose(rel2);

    assume_res?;
    close1?;
    close2?;
    Ok(())
}

/* ==========================================================================
 * sortsupport index field reads.
 * ======================================================================== */

fn rd_opfamily(index: &types_rel::Relation<'_>, attno: AttrNumber) -> PgResult<Oid> {
    // `index->rd_opfamily[attno - 1]` off the cached index entry (attno is
    // 1-based, as in C — all callers pass `varattno`/`ssup_attno`/`i + 1`).
    with_entry(index.rd_id, |rd| rd.rd_opfamily[(attno - 1) as usize])
}
fn rd_opcintype(index: &types_rel::Relation<'_>, attno: AttrNumber) -> PgResult<Oid> {
    // `index->rd_opcintype[attno - 1]` off the cached index entry (attno is
    // 1-based, as in C).
    with_entry(index.rd_id, |rd| rd.rd_opcintype[(attno - 1) as usize])
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

/// `relation->rd_fdwroutine` (relcache.c / foreign.c
/// `GetFdwRoutineForRelation`) — read the cached FDW callback-presence table,
/// or `None` (the C `rd_fdwroutine == NULL`) before it has been resolved. The
/// relation is held open by the caller, so a missing entry is a contract
/// violation (errors, mirroring the C dereference of a live `Relation`).
fn relation_fdwroutine(relation_id: Oid) -> PgResult<Option<types_nodes::FdwRoutine>> {
    crate::core_entry_store::with_relation(relation_id, |rd| rd.rd_fdwroutine)
}

/// `cfdwroutine = MemoryContextAlloc(CacheMemoryContext, sizeof(FdwRoutine));
/// memcpy(...); relation->rd_fdwroutine = cfdwroutine`
/// (foreign.c `GetFdwRoutineForRelation`) — memoize the resolved FDW
/// callback-presence table on the relcache entry's `rd_fdwroutine` slot. The
/// cached copy lives for the entry's (CacheMemoryContext) lifetime and is
/// cleared on relcache invalidation / rebuild.
fn set_relation_fdwroutine(
    relation_id: Oid,
    fdwroutine: types_nodes::FdwRoutine,
) -> PgResult<()> {
    crate::core_entry_store::with_relation_mut(relation_id, |rd| {
        rd.rd_fdwroutine = Some(fdwroutine);
    })
}
