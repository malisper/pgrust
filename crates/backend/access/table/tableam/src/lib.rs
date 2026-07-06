//! access/table/tableam.c + the tableam.h dispatch layer.
//!
//! C dispatches through `rel->rd_tableam`, a ~45-slot fn-pointer vtable for
//! pluggable AMs. We ship heap only: dispatch is the closed [`TableAm`] enum
//! (rule 4, the types_slot precedent) — each `table_*` wrapper (C surface kept
//! 1:1) is a `match` compiling to a direct call, no vtable load or indirect
//! branch. A second AM = a new variant; exhaustive matches turn every dispatch
//! point into a compile error — never a fn-pointer table. The shared
//! vocabulary (tableam.h/relscan.h types + block parallel-scan helpers) lives
//! in tableam_vocab, below the heap AM crates, and is re-exported here.
//! `mod heap` binds heapam_handler's read/analyze lanes; DML/sample arms
//! stay loud until their heapam phases land.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use core::ptr::NonNull;
use std::cell::RefCell;
use std::string::String;

use ::heapam::HeapScanDescData;
use ::heapam_handler::IndexFetchHeapData;
use ::mcx::{Mcx, PgVec};
use ::types_snapshot::IsMVCCSnapshot;
use ::types_core::fmgr::NAMEDATALEN;
use ::types_core::primitive::{BlockNumber, Buffer, ForkNumber, TransactionId};
use ::types_core::xact::{CommandId, TransactionIdIsValid};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use ::types_rel::{
    Relation, HEAP_DEFAULT_FILLFACTOR, RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION, RELKIND_TOASTVALUE, RELKIND_VIEW,
};
use ::types_scan::scankey::ScanKeyData;
use ::types_scan::sdir::ScanDirection;
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_storage::RelFileLocator;
use ::types_tuple::{ItemPointerData, ItemPointerGetBlockNumber};

pub use ::tableam_vocab::*;

#[cfg(test)]
mod tests;

pub fn init_seams() {
    guc_tables::vars::synchronize_seqscans.install(guc_tables::GucVarAccessors {
        get: synchronize_seqscans,
        set: set_synchronize_seqscans,
    });
    guc_tables::vars::default_table_access_method.install(guc_tables::GucVarAccessors {
        get: || Some(default_table_access_method()),
        set: |v| {
            set_default_table_access_method(v.as_deref().unwrap_or(DEFAULT_TABLE_ACCESS_METHOD))
        },
    });
    guc_tables::hooks::check_default_table_access_method
        .install(check_default_table_access_method);
    tableam_seams::table_tid_get_latest::set(table_tid_get_latest);
}

// --- The dispatch-facing scan values (closed per-AM extensions, rule 4) ---

// C's TableScanDesc points at the rs_base embedded in the AM's scan state; by
// value the scan IS the AM extension, tagged by the closed set. Single
// variant: the tag is free and every match is a direct call.
pub enum TableScanDesc<'mcx> {
    Heap(HeapScanDescData<'mcx>),
}

// Tagless while heap is the only AM: per-tuple dispatch costs nothing.
const _: () = assert!(
    core::mem::size_of::<TableScanDesc<'static>>()
        == core::mem::size_of::<HeapScanDescData<'static>>()
);

impl<'mcx> TableScanDesc<'mcx> {
    #[inline]
    pub fn base(&self) -> &TableScanDescData<'mcx> {
        match self {
            TableScanDesc::Heap(h) => &h.rs_base,
        }
    }

    #[inline]
    pub fn base_mut(&mut self) -> &mut TableScanDescData<'mcx> {
        match self {
            TableScanDesc::Heap(h) => &mut h.rs_base,
        }
    }
}

// C's IndexFetchTableData base embedded in IndexFetchHeapData, same treatment.
pub enum IndexFetchTableData<'mcx> {
    Heap(IndexFetchHeapData<'mcx>),
}

impl<'mcx> IndexFetchTableData<'mcx> {
    #[inline]
    pub fn rel(&self) -> &Relation<'mcx> {
        match self {
            IndexFetchTableData::Heap(h) => &h.xs_rel,
        }
    }
}

// C dereferences rd_tableam unconditionally: missing AM = C's NULL crash.
#[inline]
fn am(relation: &Relation<'_>) -> TableAm {
    match TableAm::of(relation) {
        Some(am) => am,
        None => no_table_am(relation),
    }
}

#[cold]
#[inline(never)]
fn no_table_am(relation: &Relation<'_>) -> ! {
    panic!(
        "relation \"{}\" has no supported table access method (relam {}); \
         C would dereference NULL rd_tableam",
        relation.name(),
        relation.rd_rel.relam
    )
}

#[cold]
#[inline(never)]
fn unported(unit: &'static str) -> ! {
    panic!("backend-access-tableam reached unported unit: {unit}")
}

// heapam_handler.c's heapam_methods: read lane bound directly onto heapam /
// heapam_handler; the rest panic until their units land.
mod heap {
    use super::*;

    const DML_UNIT: &str = "backend-access-heap-heapam (phase 2 DML)";

    pub(super) fn slot_callbacks(_rel: &Relation<'_>) -> TupleSlotKind {
        TupleSlotKind::BufferHeapTuple
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn scan_begin<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        snapshot: Snapshot<'mcx>,
        nkeys: i32,
        key: PgVec<'mcx, ScanKeyData>,
        parallel: Option<NonNull<ParallelBlockTableScanDescData>>,
        flags: u32,
    ) -> PgResult<TableScanDesc<'mcx>> {
        Ok(TableScanDesc::Heap(::heapam::heap_beginscan(
            mcx, rel, snapshot, nkeys, key, parallel, flags,
        )?))
    }

    pub(super) fn scan_end(scan: HeapScanDescData<'_>) -> PgResult<()> {
        ::heapam::heap_endscan(scan)
    }

    pub(super) fn scan_rescan(
        scan: &mut HeapScanDescData<'_>,
        key: Option<&[ScanKeyData]>,
        set_params: bool,
        allow_strat: bool,
        allow_sync: bool,
        allow_pagemode: bool,
    ) -> PgResult<()> {
        ::heapam::heap_rescan(scan, key, set_params, allow_strat, allow_sync, allow_pagemode)
    }

    #[inline]
    pub(super) fn scan_getnextslot<'mcx>(
        mcx: Mcx<'mcx>,
        scan: &mut HeapScanDescData<'mcx>,
        direction: ScanDirection,
        slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool> {
        ::heapam::heap_getnextslot(mcx, scan, direction, slot)
    }

    pub(super) fn scan_set_tidrange(
        scan: &mut HeapScanDescData<'_>,
        mintid: &ItemPointerData,
        maxtid: &ItemPointerData,
    ) {
        ::heapam::heap_set_tidrange(scan, mintid, maxtid);
    }

    pub(super) fn scan_getnextslot_tidrange<'mcx>(
        mcx: Mcx<'mcx>,
        scan: &mut HeapScanDescData<'mcx>,
        direction: ScanDirection,
        slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool> {
        ::heapam::heap_getnextslot_tidrange(mcx, scan, direction, slot)
    }

    pub(super) fn parallelscan_estimate(rel: &Relation<'_>) -> usize {
        table_block_parallelscan_estimate(rel)
    }

    pub(super) fn parallelscan_initialize(
        rel: &Relation<'_>,
        pscan: &mut ParallelBlockTableScanDescData,
    ) -> PgResult<usize> {
        table_block_parallelscan_initialize(rel, pscan)
    }

    pub(super) fn parallelscan_reinitialize(
        rel: &Relation<'_>,
        pscan: &ParallelBlockTableScanDescData,
    ) {
        table_block_parallelscan_reinitialize(rel, pscan);
    }

    pub(super) fn index_delete_tuples<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        delstate: &mut TM_IndexDeleteOp<'mcx>,
    ) -> PgResult<TransactionId> {
        ::heapam::heap_index_delete_tuples(mcx, rel, delstate)
    }

    // heapam_tuple_insert (heapam_handler.c): fetch the slot's heap tuple in
    // place (virtual/minimal sources take the ExecFetchSlotHeapTuple copy
    // arm), heap_insert it, copy t_self back.
    pub(super) fn tuple_insert<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        slot: &mut SlotData<'mcx>,
        cid: CommandId,
        options: i32,
        bistate: Option<&mut BulkInsertStateData>,
    ) -> PgResult<()> {
        exectuples::exec_materialize_slot(slot, mcx)?;
        slot.base_mut().tts_tableOid = rel.rd_id;
        let t_self = match slot {
            SlotData::Heap(h) => {
                let tuple = h.tuple.as_mut().expect("materialized heap slot holds a tuple");
                tuple.t_tableOid = rel.rd_id;
                ::heapam::heap_insert(rel, tuple, cid, options, bistate)?;
                tuple.t_self
            }
            SlotData::BufferHeap(b) => {
                let tuple = b.base.tuple.as_mut().expect("materialized heap slot holds a tuple");
                tuple.t_tableOid = rel.rd_id;
                ::heapam::heap_insert(rel, tuple, cid, options, bistate)?;
                tuple.t_self
            }
            // ExecFetchSlotHeapTuple copy arm (virtual/minimal source slots,
            // e.g. multi-row VALUES routed into partitions).
            _ => {
                let mut tuple = exectuples::exec_copy_slot_heap_tuple(slot, mcx, mcx)?;
                tuple.t_tableOid = rel.rd_id;
                ::heapam::heap_insert(rel, &mut tuple, cid, options, bistate)?;
                tuple.t_self
            }
        };
        slot.base_mut().tts_tid = t_self;
        Ok(())
    }

    // heapam_tuple_insert_speculative (heapam_handler.c): the token is stamped
    // into t_ctid before insert; hio asserts it when placing the tuple.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn tuple_insert_speculative<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        slot: &mut SlotData<'mcx>,
        cid: CommandId,
        options: i32,
        bistate: Option<&mut BulkInsertStateData>,
        spec_token: u32,
    ) -> PgResult<()> {
        debug_assert!(bistate.is_none(), "GetBulkInsertState lane (COPY) not ported");
        exectuples::exec_materialize_slot(slot, mcx)?;
        slot.base_mut().tts_tableOid = rel.rd_id;
        let t_self = match slot {
            SlotData::Heap(h) => {
                let tuple = h.tuple.as_mut().expect("materialized heap slot holds a tuple");
                tuple.t_tableOid = rel.rd_id;
                tuple.t_data_mut().set_speculative_token(spec_token);
                ::heapam::heap_insert(rel, tuple, cid, options | ::heapam::hio::HEAP_INSERT_SPECULATIVE, bistate)?;
                tuple.t_self
            }
            SlotData::BufferHeap(b) => {
                let tuple = b.base.tuple.as_mut().expect("materialized heap slot holds a tuple");
                tuple.t_tableOid = rel.rd_id;
                tuple.t_data_mut().set_speculative_token(spec_token);
                ::heapam::heap_insert(rel, tuple, cid, options | ::heapam::hio::HEAP_INSERT_SPECULATIVE, bistate)?;
                tuple.t_self
            }
            // ExecFetchSlotHeapTuple copy arm (virtual/minimal source slots,
            // e.g. multi-row VALUES routed into partitions on ON CONFLICT).
            _ => {
                let mut tuple = exectuples::exec_copy_slot_heap_tuple(slot, mcx, mcx)?;
                tuple.t_tableOid = rel.rd_id;
                tuple.t_data_mut().set_speculative_token(spec_token);
                ::heapam::heap_insert(rel, &mut tuple, cid, options | ::heapam::hio::HEAP_INSERT_SPECULATIVE, bistate)?;
                tuple.t_self
            }
        };
        slot.base_mut().tts_tid = t_self;
        Ok(())
    }

    pub(super) fn tuple_complete_speculative<'mcx>(
        _mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        slot: &mut SlotData<'mcx>,
        _spec_token: u32,
        succeeded: bool,
    ) -> PgResult<()> {
        let tid = slot.base().tts_tid;
        if succeeded {
            ::heapam::heap_finish_speculative(rel, &tid)
        } else {
            ::heapam::heap_abort_speculative(rel, &tid)
        }
    }

    // heapam_multi_insert (heapam_handler.c).
    pub(super) fn multi_insert<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        slots: &mut [&mut SlotData<'mcx>],
        cid: CommandId,
        options: i32,
        bistate: Option<&mut BulkInsertStateData>,
    ) -> PgResult<()> {
        ::heapam::heap_multi_insert(mcx, rel, slots, cid, options, bistate)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn tuple_delete<'mcx>(
        _mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        tid: &ItemPointerData,
        cid: CommandId,
        _snapshot: &Snapshot<'mcx>,
        crosscheck: &Snapshot<'mcx>,
        wait: bool,
        tmfd: &mut TM_FailureData,
        changing_part: bool,
    ) -> PgResult<TM_Result> {
        ::heapam::heap_delete(rel, tid, cid, crosscheck.as_deref(), wait, tmfd, changing_part)
    }

    // heapam_tuple_update: the TU_* verdict passes through unfiltered.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn tuple_update<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        otid: &ItemPointerData,
        slot: &mut SlotData<'mcx>,
        cid: CommandId,
        _snapshot: &Snapshot<'mcx>,
        crosscheck: &Snapshot<'mcx>,
        wait: bool,
        tmfd: &mut TM_FailureData,
        lockmode: &mut LockTupleMode,
        update_indexes: &mut TU_UpdateIndexes,
    ) -> PgResult<TM_Result> {
        exectuples::exec_materialize_slot(slot, mcx)?;
        slot.base_mut().tts_tableOid = rel.rd_id;
        let tuple = match slot {
            SlotData::Heap(h) => h.tuple.as_mut(),
            SlotData::BufferHeap(b) => b.base.tuple.as_mut(),
            _ => panic!(
                "heapam_tuple_update (heapam_handler.c): non-heap slot copy arm \
                 not ported"
            ),
        }
        .expect("materialized heap slot holds a tuple");
        tuple.t_tableOid = rel.rd_id;
        let result = ::heapam::heap_update(
            rel,
            otid,
            tuple,
            cid,
            crosscheck.as_deref(),
            wait,
            tmfd,
            lockmode,
            update_indexes,
        )?;
        let t_self = tuple.t_self;
        slot.base_mut().tts_tid = t_self;
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn tuple_lock<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        tid: &ItemPointerData,
        snapshot: &Snapshot<'mcx>,
        slot: &mut SlotData<'mcx>,
        cid: CommandId,
        mode: LockTupleMode,
        wait_policy: LockWaitPolicy,
        flags: u8,
        tmfd: &mut TM_FailureData,
    ) -> PgResult<TM_Result> {
        ::heapam_handler::heapam_tuple_lock(
            mcx, rel, tid, snapshot, slot, cid, mode, wait_policy, flags, tmfd,
        )
    }

    pub(super) fn relation_set_new_filelocator(
        rel: &Relation<'_>,
        newrlocator: &RelFileLocator,
        persistence: i8,
    ) -> PgResult<(TransactionId, TransactionId)> {
        let freeze_xid = procarray::RecentXmin();
        let min_multi = multixact::GetOldestMultiXactId()?;
        let srel =
            catalog_storage::RelationCreateStorage(*newrlocator, persistence as u8, true)?;
        if persistence as u8 == ::types_core::catalog::RELPERSISTENCE_UNLOGGED {
            debug_assert!(
                rel.rd_rel.relkind == RELKIND_RELATION
                    || rel.rd_rel.relkind == RELKIND_TOASTVALUE
            );
            smgr::smgrcreate(srel, ForkNumber::INIT_FORKNUM, false)?;
            catalog_storage::log_smgrcreate(newrlocator, ForkNumber::INIT_FORKNUM)?;
        }
        smgr::smgrclose(srel)?;
        Ok((freeze_xid, min_multi))
    }

    pub(super) fn relation_copy_data(
        rel: &Relation<'_>,
        newrlocator: &RelFileLocator,
    ) -> PgResult<()> {
        if rel.rd_backend != ::types_core::INVALID_PROC_NUMBER {
            unported("heapam_relation_copy_data temp-relation lane");
        }
        let src = ::types_storage::RelFileLocatorBackend {
            locator: rel.rd_locator.get(),
            backend: rel.rd_backend,
        };
        smgr::smgropen(src.locator, src.backend)?;
        ::bufmgr_seams::flush_relations_all_buffers::call(&[src])?;
        let persistence = rel.rd_rel.relpersistence;
        let dstrel = catalog_storage::RelationCreateStorage(*newrlocator, persistence, true)?;
        catalog_storage::RelationCopyStorage(
            src,
            dstrel,
            ForkNumber::MAIN_FORKNUM,
            persistence,
        )?;
        for fork_i in ForkNumber::MAIN_FORKNUM as i32 + 1..=::types_core::MAX_FORKNUM as i32 {
            let fork = ForkNumber::from_i32(fork_i).expect("valid fork number");
            if smgr::smgrexists(src, fork)? {
                smgr::smgrcreate(dstrel, fork, false)?;
                if rel.is_permanent()
                    || (persistence == ::types_core::catalog::RELPERSISTENCE_UNLOGGED
                        && fork == ForkNumber::INIT_FORKNUM)
                {
                    catalog_storage::log_smgrcreate(newrlocator, fork)?;
                }
                catalog_storage::RelationCopyStorage(src, dstrel, fork, persistence)?;
            }
        }
        catalog_storage::RelationDropStorage(rel)?;
        smgr::smgrclose(dstrel)
    }

    pub(super) fn relation_nontransactional_truncate(rel: &Relation<'_>) -> PgResult<()> {
        catalog_storage::RelationTruncate(rel, 0)
    }

    pub(super) fn relation_needs_toast_table(rel: &Relation<'_>) -> bool {
        use ::types_tuple::tupmacs::att_nominal_alignby;
        use ::types_tuple::{BITMAPLEN, MAXALIGN, SizeofHeapTupleHeader, TYPSTORAGE_PLAIN};
        const ATTRIBUTE_GENERATED_VIRTUAL: i8 = b'v' as i8;

        let tupdesc = &rel.rd_att;
        let mut data_length: usize = 0;
        let mut maxlength_unknown = false;
        let mut has_toastable_attrs = false;
        for i in 0..tupdesc.natts as usize {
            let att = tupdesc.attr(i);
            if att.attisdropped || att.attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
                continue;
            }
            data_length =
                att_nominal_alignby(data_length, tupdesc.compact_attr(i).attalignby);
            if att.attlen > 0 {
                data_length += att.attlen as usize;
            } else {
                let maxlen = format_type::type_maximum_size(att.atttypid, att.atttypmod);
                if maxlen < 0 {
                    maxlength_unknown = true;
                } else {
                    data_length += maxlen as usize;
                }
                if att.attstorage != TYPSTORAGE_PLAIN {
                    has_toastable_attrs = true;
                }
            }
        }
        if !has_toastable_attrs {
            return false;
        }
        if maxlength_unknown {
            return true;
        }
        let tuple_length = MAXALIGN(SizeofHeapTupleHeader + BITMAPLEN(tupdesc.natts) as usize)
            + MAXALIGN(data_length);
        tuple_length > ::heapam::dml::TOAST_TUPLE_THRESHOLD
    }

    pub(super) fn relation_toast_am(rel: &Relation<'_>) -> ::types_core::Oid {
        rel.rd_rel.relam
    }

    pub(super) fn relation_size(rel: &Relation<'_>, fork_number: ForkNumber) -> PgResult<u64> {
        table_block_relation_size(rel, fork_number)
    }

    // next_buffer replaces C's read stream: already-pinned buffers, Invalid = done.
    pub(super) fn scan_analyze_next_block<'mcx>(
        _mcx: Mcx<'mcx>,
        scan: &mut HeapScanDescData<'mcx>,
        next_buffer: &mut dyn FnMut() -> PgResult<Buffer>,
    ) -> PgResult<bool> {
        let Some(pin) = bufmgr_seams::BufferPin::adopt(next_buffer()?) else {
            return Ok(false);
        };
        scan.rs_cblock = pin.block_number();
        scan.rs_cbuf = Some(pin);
        scan.rs_cindex = ::types_tuple::FirstOffsetNumber as u32;
        Ok(true)
    }

    // C divergence: C holds the share lock across calls; ContentLockGuard
    // cannot outlive its borrow of the pin, so it is re-taken per call (cold).
    pub(super) fn scan_analyze_next_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        scan: &mut HeapScanDescData<'mcx>,
        oldest_xmin: TransactionId,
        liverows: &mut f64,
        deadrows: &mut f64,
        slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool> {
        use ::types_snapshot::HTSV_Result::*;
        let pin = scan.rs_cbuf.take().expect("analyze scan positioned without a buffer");
        let rd_id = scan.rs_base.rs_rd.rd_id;
        let block = scan.rs_cblock;
        let mut cindex = scan.rs_cindex;
        let mut sampled = false;
        {
            let _lock = pin.lock_share()?;
            let page = pin.page();
            let maxoffset = page.max_offset_number();
            while cindex <= maxoffset as u32 {
                let offnum = cindex as ::types_core::primitive::OffsetNumber;
                let itemid = page.item_id(offnum);
                if !itemid.is_normal() {
                    if itemid.is_dead() {
                        *deadrows += 1.0;
                    }
                    cindex += 1;
                    continue;
                }
                let (ptr, len) = page.item_raw(itemid);
                // SAFETY: normal line pointer on the page pinned by `pin`.
                let mut targtuple = unsafe {
                    ::types_tuple::HeapTupleData::from_raw_parts(
                        ptr,
                        len,
                        ItemPointerData::new(block, offnum),
                        rd_id,
                    )
                };
                let sample_it = match heapam_visibility_seams::heap_tuple_satisfies_vacuum::call(
                    &mut targtuple,
                    oldest_xmin,
                    pin.buffer(),
                )? {
                    HEAPTUPLE_LIVE => {
                        *liverows += 1.0;
                        true
                    }
                    HEAPTUPLE_DEAD | HEAPTUPLE_RECENTLY_DEAD => {
                        *deadrows += 1.0;
                        false
                    }
                    HEAPTUPLE_INSERT_IN_PROGRESS => {
                        if xact_seams::transaction_id_is_current_transaction_id::call(
                            targtuple.t_data().xmin(),
                        ) {
                            *liverows += 1.0;
                            true
                        } else {
                            false
                        }
                    }
                    HEAPTUPLE_DELETE_IN_PROGRESS => {
                        if xact_seams::transaction_id_is_current_transaction_id::call(
                            ::heapam::HeapTupleHeaderGetUpdateXid(targtuple.t_data())?,
                        ) {
                            *deadrows += 1.0;
                            false
                        } else {
                            *liverows += 1.0;
                            true
                        }
                    }
                };
                cindex += 1;
                if sample_it {
                    // SAFETY: same pinned image; the slot store takes its own pin.
                    let tuple = unsafe {
                        ::types_tuple::HeapTupleData::from_raw_parts(
                            targtuple.header_ptr(),
                            targtuple.t_len,
                            targtuple.t_self,
                            rd_id,
                        )
                    };
                    exectuples::exec_store_buffer_heap_tuple(slot, mcx, tuple, pin.buffer());
                    sampled = true;
                    break;
                }
            }
        }
        scan.rs_cindex = cindex;
        if sampled {
            scan.rs_cbuf = Some(pin);
            Ok(true)
        } else {
            pin.release();
            exectuples::exec_clear_tuple(slot, mcx);
            Ok(false)
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn scan_bitmap_next_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        scan: &mut HeapScanDescData<'mcx>,
        tbm: Option<&tidbitmap::TIDBitmap<'_>>,
        iterator: &mut tidbitmap::TbmIterator,
        slot: &mut SlotData<'mcx>,
        recheck: &mut bool,
        lossy_pages: &mut u64,
        exact_pages: &mut u64,
    ) -> PgResult<bool> {
        heapam::bitmap::heap_scan_bitmap_next_tuple(
            mcx, scan, tbm, iterator, slot, recheck, lossy_pages, exact_pages,
        )
    }

    pub(super) fn scan_sample_next_block<'mcx>(
        _mcx: Mcx<'mcx>,
        scan: &mut HeapScanDescData<'mcx>,
        scanstate: &mut dyn SampleScanDriver,
    ) -> PgResult<bool> {
        heapam::sample::heap_scan_sample_next_block(scan, scanstate)
    }

    pub(super) fn scan_sample_next_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        scan: &mut HeapScanDescData<'mcx>,
        scanstate: &mut dyn SampleScanDriver,
        slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool> {
        heapam::sample::heap_scan_sample_next_tuple(mcx, scan, scanstate, slot)
    }
}

// --- GUC variables (tableam.c) + check hook (tableamapi.c) ---

pub const DEFAULT_TABLE_ACCESS_METHOD: &str = "heap";

thread_local! {
    // Cold GUC store: bare String = guc.c's malloc'd string, outlives any mcx.
    static DEFAULT_TABLE_ACCESS_METHOD_GUC: RefCell<String> =
        RefCell::new(String::from(DEFAULT_TABLE_ACCESS_METHOD));
}

pub fn default_table_access_method() -> String {
    DEFAULT_TABLE_ACCESS_METHOD_GUC.with(|v| v.borrow().clone())
}

pub fn set_default_table_access_method(value: &str) {
    DEFAULT_TABLE_ACCESS_METHOD_GUC.with(|v| {
        let mut s = v.borrow_mut();
        s.clear();
        s.push_str(value);
    });
}

fn check_default_table_access_method(
    newval: &mut Option<String>,
    _extra: &mut Option<guc_tables::GucHookExtra>,
    _source: ::types_guc::GucSource,
) -> PgResult<bool> {
    let name = newval.as_deref().unwrap_or("");

    if name.is_empty() {
        return Err(Box::new(PgError::error(
            "\"default_table_access_method\" cannot be empty.",
        )));
    }

    if name.len() >= NAMEDATALEN as usize {
        return Err(Box::new(PgError::error(format!(
            "\"default_table_access_method\" is too long (maximum {} characters).",
            NAMEDATALEN - 1
        ))));
    }

    // C probes get_table_am_oid when IsTransactionState() && MyDatabaseId
    // valid; both unported, so every caller is on C's accept-on-faith path.
    Ok(true)
}

// --- Shared helpers ---

// CheckXidAlive (xact.c) is set only during logical decoding, unported; the
// statically-invalid xid computes the same `unlikely()` false as C.
fn unexpected_during_logical_decoding() -> bool {
    const CHECK_XID_ALIVE: TransactionId = ::types_core::xact::InvalidTransactionId;
    TransactionIdIsValid(CHECK_XID_ALIVE)
}

fn elog_error(message: impl Into<String>) -> Box<PgError> {
    Box::new(PgError::error(message))
}

// shmem.c add_size, private mirror of the unported helper.
fn add_size(s1: usize, s2: usize) -> PgResult<usize> {
    s1.checked_add(s2).ok_or_else(|| {
        Box::new(
            PgError::error("requested shared memory size overflows size_t")
                .with_sqlstate(::types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        )
    })
}

// --- Slot functions (tableam.c) ---

pub fn table_slot_callbacks(relation: &Relation<'_>) -> TupleSlotKind {
    if let Some(am) = TableAm::of(relation) {
        match am {
            TableAm::Heap => heap::slot_callbacks(relation),
        }
    } else if relation.rd_rel.relkind == RELKIND_FOREIGN_TABLE {
        // FDWs historically expect heap tuples in their slots.
        TupleSlotKind::HeapTuple
    } else {
        debug_assert!({
            let relkind = relation.rd_rel.relkind;
            relkind == RELKIND_VIEW || relkind == RELKIND_PARTITIONED_TABLE
        });
        TupleSlotKind::Virtual
    }
}

pub fn table_slot_create<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
) -> PgResult<SlotData<'mcx>> {
    let tts_cb = table_slot_callbacks(relation);
    // MakeSingleTupleTableSlot(RelationGetDescr(relation), tts_cb)
    Ok(exectuples::make_tuple_table_slot(
        mcx,
        tts_cb,
        Some(relation.rd_att.clone()),
    ))
}

// --- Table scan functions (tableam.h wrappers + tableam.c) ---

pub fn table_beginscan<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    snapshot: Snapshot<'mcx>,
    nkeys: i32,
    key: PgVec<'mcx, ScanKeyData>,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags = SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE;
    match am(relation) {
        TableAm::Heap => heap::scan_begin(mcx, relation, snapshot, nkeys, key, None, flags),
    }
}

pub fn table_beginscan_strat<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    snapshot: Snapshot<'mcx>,
    nkeys: i32,
    key: PgVec<'mcx, ScanKeyData>,
    allow_strat: bool,
    allow_sync: bool,
) -> PgResult<TableScanDesc<'mcx>> {
    let mut flags = SO_TYPE_SEQSCAN | SO_ALLOW_PAGEMODE;
    if allow_strat {
        flags |= SO_ALLOW_STRAT;
    }
    if allow_sync {
        flags |= SO_ALLOW_SYNC;
    }
    match am(relation) {
        TableAm::Heap => heap::scan_begin(mcx, relation, snapshot, nkeys, key, None, flags),
    }
}

pub fn table_beginscan_catalog<'mcx>(
    _mcx: Mcx<'mcx>,
    _relation: &Relation<'mcx>,
    _nkeys: i32,
    _key: PgVec<'mcx, ScanKeyData>,
) -> PgResult<TableScanDesc<'mcx>> {
    // RegisterSnapshot(GetCatalogSnapshot(relid)) + scan_begin(SEQSCAN|STRAT|SYNC|PAGEMODE|TEMP_SNAPSHOT).
    unported("backend-utils-time-snapmgr (GetCatalogSnapshot/RegisterSnapshot)")
}

pub fn table_beginscan_bm<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    snapshot: Snapshot<'mcx>,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags = SO_TYPE_BITMAPSCAN | SO_ALLOW_PAGEMODE;
    match am(rel) {
        TableAm::Heap => heap::scan_begin(mcx, rel, snapshot, 0, PgVec::new_in(mcx), None, flags),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn table_beginscan_sampling<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    snapshot: Snapshot<'mcx>,
    nkeys: i32,
    key: PgVec<'mcx, ScanKeyData>,
    allow_strat: bool,
    allow_sync: bool,
    allow_pagemode: bool,
) -> PgResult<TableScanDesc<'mcx>> {
    let mut flags = SO_TYPE_SAMPLESCAN;
    if allow_strat {
        flags |= SO_ALLOW_STRAT;
    }
    if allow_sync {
        flags |= SO_ALLOW_SYNC;
    }
    if allow_pagemode {
        flags |= SO_ALLOW_PAGEMODE;
    }
    match am(relation) {
        TableAm::Heap => heap::scan_begin(mcx, relation, snapshot, nkeys, key, None, flags),
    }
}

pub fn table_beginscan_tid<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    snapshot: Snapshot<'mcx>,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags = SO_TYPE_TIDSCAN;
    match am(rel) {
        TableAm::Heap => heap::scan_begin(mcx, rel, snapshot, 0, PgVec::new_in(mcx), None, flags),
    }
}

pub fn table_beginscan_analyze<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags = SO_TYPE_ANALYZE;
    match am(relation) {
        TableAm::Heap => heap::scan_begin(mcx, relation, None, 0, PgVec::new_in(mcx), None, flags),
    }
}

pub fn table_endscan(scan: TableScanDesc<'_>) -> PgResult<()> {
    match scan {
        TableScanDesc::Heap(h) => heap::scan_end(h),
    }
}

fn table_tid_get_latest<'mcx>(
    mcx: Mcx<'mcx>,
    rel: Relation<'mcx>,
    snapshot: std::rc::Rc<::types_snapshot::SnapshotData<'static>>,
    mut tid: ItemPointerData,
) -> PgResult<ItemPointerData> {
    let mut scan = table_beginscan_tid(mcx, &rel, Some(snapshot))?;
    table_tuple_get_latest_tid(mcx, &mut scan, &mut tid)?;
    table_endscan(scan)?;
    Ok(tid)
}

pub fn table_rescan<'mcx>(
    _mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    key: Option<&[ScanKeyData]>,
) -> PgResult<()> {
    match scan {
        TableScanDesc::Heap(h) => heap::scan_rescan(h, key, false, false, false, false),
    }
}

pub fn table_rescan_set_params<'mcx>(
    _mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    key: Option<&[ScanKeyData]>,
    allow_strat: bool,
    allow_sync: bool,
    allow_pagemode: bool,
) -> PgResult<()> {
    match scan {
        TableScanDesc::Heap(h) => {
            heap::scan_rescan(h, key, true, allow_strat, allow_sync, allow_pagemode)
        }
    }
}

// Per-tuple at M2: the single-variant match monomorphizes to the direct
// heap_getnextslot call — zero glue over heapam's loop.
#[inline]
pub fn table_scan_getnextslot<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    direction: ScanDirection,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    match scan {
        TableScanDesc::Heap(h) => heap::scan_getnextslot(mcx, h, direction, slot),
    }
}

pub fn table_scan_supports_pagebatch(scan: &TableScanDesc<'_>) -> bool {
    match scan {
        TableScanDesc::Heap(h) => {
            (h.rs_base.rs_flags & SO_ALLOW_PAGEMODE) != 0 && h.rs_base.rs_parallel.is_none()
        }
    }
}

/// Relation size at scan start (heap rs_nblocks): the deform-JIT page gate.
pub fn table_scan_nblocks(scan: &TableScanDesc<'_>) -> u32 {
    match scan {
        TableScanDesc::Heap(h) => h.rs_nblocks,
    }
}

/// Page-batch scan feed (upstream batch scan API, CF 6176): 0 = exhausted.
pub fn table_scan_getnextpagebatch<'mcx>(scan: &mut TableScanDesc<'mcx>) -> PgResult<u32> {
    match scan {
        TableScanDesc::Heap(h) => ::heapam::heap_getnextpagebatch(h),
    }
}

/// SoA-deform the staged page batch's column prefix per `plan`.
pub fn table_scan_batch_deform<'mcx>(
    scan: &mut TableScanDesc<'mcx>,
    plan: &::exectuples::SoaDeformPlan<'_>,
    soa: &mut ::exectuples::SoaBatch<'_>,
    qual_col_only: Option<u16>,
) {
    match scan {
        TableScanDesc::Heap(h) => ::heapam::heap_batch_deform_soa(h, plan, soa, qual_col_only),
    }
}

/// Stage the staged page batch's varlena sort-key column per `plan`.
pub fn table_scan_batch_stage_varkey<'mcx>(
    scan: &mut TableScanDesc<'mcx>,
    plan: &::exectuples::SoaVarKeyPlan,
    soa: &mut ::exectuples::SoaBatch<'_>,
) {
    match scan {
        TableScanDesc::Heap(h) => ::heapam::heap_batch_stage_varkey(h, plan, soa),
    }
}

/// Store tuple `i` of the staged page batch into `slot`.
#[inline(always)]
pub fn table_scan_batch_store_slot<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    i: u32,
    slot: &mut SlotData<'mcx>,
) {
    match scan {
        TableScanDesc::Heap(h) => ::heapam::heap_batch_store_slot(mcx, h, i, slot),
    }
}

/// Bitmap page-batch feed for the fused drive: stage the next page with
/// visible tuples (visibility resolved at staging); 0 = bitmap exhausted.
pub fn table_scan_bitmap_next_pagebatch<'mcx>(
    scan: &mut TableScanDesc<'mcx>,
    tbm: Option<&tidbitmap::TIDBitmap<'_>>,
    iterator: &mut tidbitmap::TbmIterator,
    recheck: &mut bool,
    lossy_pages: &mut u64,
    exact_pages: &mut u64,
) -> PgResult<u32> {
    match scan {
        TableScanDesc::Heap(h) => ::heapam::bitmap::heap_scan_bitmap_next_pagebatch(
            h, tbm, iterator, recheck, lossy_pages, exact_pages,
        ),
    }
}

/// Store staged bitmap tuple `i` of the current page into `slot`.
#[inline(always)]
pub fn table_scan_bitmap_batch_store_slot<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    i: u32,
    slot: &mut SlotData<'mcx>,
) {
    match scan {
        TableScanDesc::Heap(h) => ::heapam::bitmap::heap_scan_bitmap_batch_store(mcx, h, i, slot),
    }
}

pub fn table_beginscan_tidrange<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    snapshot: Snapshot<'mcx>,
    mintid: &ItemPointerData,
    maxtid: &ItemPointerData,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags = SO_TYPE_TIDRANGESCAN | SO_ALLOW_PAGEMODE;
    match am(rel) {
        TableAm::Heap => {
            let mut sscan =
                heap::scan_begin(mcx, rel, snapshot, 0, PgVec::new_in(mcx), None, flags)?;
            let TableScanDesc::Heap(h) = &mut sscan;
            heap::scan_set_tidrange(h, mintid, maxtid);
            Ok(sscan)
        }
    }
}

pub fn table_rescan_tidrange<'mcx>(
    _mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    mintid: &ItemPointerData,
    maxtid: &ItemPointerData,
) -> PgResult<()> {
    debug_assert!((scan.base().rs_flags & SO_TYPE_TIDRANGESCAN) != 0);
    match scan {
        TableScanDesc::Heap(h) => {
            heap::scan_rescan(h, None, false, false, false, false)?;
            heap::scan_set_tidrange(h, mintid, maxtid);
            Ok(())
        }
    }
}

pub fn table_scan_getnextslot_tidrange<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    direction: ScanDirection,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    match scan {
        TableScanDesc::Heap(h) => heap::scan_getnextslot_tidrange(mcx, h, direction, slot),
    }
}

// --- Parallel table scan (tableam.c) ---

// C's ParallelTableScanDescData shm block: the typed snapshot field replaces
// the phs_snapshot_off byte image (docs/parallel-query-design.md); stable
// address — worker scans hold NonNull into `pscan`.
#[derive(Default)]
pub struct ParallelTableScanDescShared {
    pub pscan: ParallelBlockTableScanDescData,
    pub snapshot: Option<::snapmgr::SerializedSnapshot>,
}

pub fn table_parallelscan_estimate(
    rel: &Relation<'_>,
    snapshot: &Snapshot<'_>,
) -> PgResult<usize> {
    let mut sz: usize = 0;

    match snapshot {
        // EstimateSnapshotSpace adds nothing: the snapshot crosses typed.
        Some(s) => debug_assert!(IsMVCCSnapshot(s)),
        None => {} // Assert(snapshot == SnapshotAny)
    }

    let am_sz = match am(rel) {
        TableAm::Heap => heap::parallelscan_estimate(rel),
    };
    sz = add_size(sz, am_sz)?;

    Ok(sz)
}

pub fn table_parallelscan_initialize(
    rel: &Relation<'_>,
    target: &mut ParallelTableScanDescShared,
    snapshot: &Snapshot<'static>,
) -> PgResult<()> {
    let snapshot_off = match am(rel) {
        TableAm::Heap => heap::parallelscan_initialize(rel, &mut target.pscan)?,
    };
    target.pscan.phs_snapshot_off = snapshot_off;

    match snapshot {
        Some(s) if IsMVCCSnapshot(s) => {
            target.snapshot = Some(::snapmgr::SerializeSnapshot(s));
            target.pscan.phs_snapshot_any = false;
        }
        _ => {
            debug_assert!(snapshot.is_none());
            target.snapshot = None;
            target.pscan.phs_snapshot_any = true;
        }
    }

    Ok(())
}

pub fn table_parallelscan_reinitialize(
    rel: &Relation<'_>,
    pscan: &ParallelBlockTableScanDescData,
) {
    match am(rel) {
        TableAm::Heap => heap::parallelscan_reinitialize(rel, pscan),
    }
}

pub fn table_beginscan_parallel<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    parallel_scan: &ParallelTableScanDescShared,
) -> PgResult<TableScanDesc<'mcx>> {
    debug_assert!(relation.rd_locator.get() == parallel_scan.pscan.phs_locator);

    let mut flags = SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE;
    let mut registered = None;
    let snapshot: Snapshot<'mcx> = if !parallel_scan.pscan.phs_snapshot_any {
        let serialized = parallel_scan
            .snapshot
            .as_ref()
            .expect("MVCC parallel scan carries its serialized snapshot");
        let snap = ::snapmgr::RestoreSnapshot(serialized);
        let snap = ::snapmgr::RegisterSnapshot(Some(&snap))?.expect("registered a snapshot");
        flags |= SO_TEMP_SNAPSHOT;
        registered = Some(snap.clone());
        Some(snap)
    } else {
        None
    };

    // heapam rs_parallel contract: the shared descriptor outlives the scan.
    let pscan_ptr = NonNull::from(&parallel_scan.pscan);
    match am(relation) {
        TableAm::Heap => {
            let mut scan = heap::scan_begin(
                mcx,
                relation,
                snapshot,
                0,
                PgVec::new_in(mcx),
                Some(pscan_ptr),
                flags,
            )?;
            let TableScanDesc::Heap(h) = &mut scan;
            h.rs_temp_snapshot = registered;
            Ok(scan)
        }
    }
}

// --- Index scan related functions (tableam.h / tableam.c) ---

pub fn table_index_fetch_begin<'mcx>(rel: &Relation<'mcx>) -> IndexFetchTableData<'mcx> {
    match am(rel) {
        TableAm::Heap => {
            IndexFetchTableData::Heap(::heapam_handler::heapam_index_fetch_begin(rel))
        }
    }
}

pub fn table_index_fetch_reset(scan: &mut IndexFetchTableData<'_>) {
    match scan {
        IndexFetchTableData::Heap(h) => ::heapam_handler::heapam_index_fetch_reset(h),
    }
}

pub fn table_index_fetch_end(scan: IndexFetchTableData<'_>) {
    match scan {
        IndexFetchTableData::Heap(h) => ::heapam_handler::heapam_index_fetch_end(h),
    }
}

pub fn table_index_fetch_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexFetchTableData<'mcx>,
    tid: &mut ItemPointerData,
    snapshot: &mut Snapshot<'mcx>,
    slot: &mut SlotData<'mcx>,
    call_again: &mut bool,
    all_dead: Option<&mut bool>,
) -> PgResult<bool> {
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_index_fetch_tuple call during logical decoding",
        ));
    }
    match scan {
        IndexFetchTableData::Heap(h) => ::heapam_handler::heapam_index_fetch_tuple(
            mcx, h, tid, snapshot, slot, call_again, all_dead,
        ),
    }
}

pub use ::heapam_handler::{BatchFetch, INDEX_FETCH_BATCH_MAX};

pub fn table_index_fetch_batch_fill<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexFetchTableData<'mcx>,
    first_tid: &ItemPointerData,
    rest: &[ItemPointerData],
    snapshot: &Snapshot<'mcx>,
) -> PgResult<()> {
    match scan {
        IndexFetchTableData::Heap(h) => {
            ::heapam_handler::heapam_index_fetch_batch_fill(mcx, h, first_tid, rest, snapshot)
        }
    }
}

pub fn table_index_fetch_batch_next<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexFetchTableData<'mcx>,
    tid: &mut ItemPointerData,
    slot: &mut SlotData<'mcx>,
) -> ::heapam_handler::BatchFetch {
    match scan {
        IndexFetchTableData::Heap(h) => {
            ::heapam_handler::heapam_index_fetch_batch_next(mcx, h, tid, slot)
        }
    }
}

pub fn table_index_fetch_tuple_check<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &mut ItemPointerData,
    snapshot: &mut Snapshot<'mcx>,
    all_dead: Option<&mut bool>,
) -> PgResult<bool> {
    let mut call_again = false;

    let mut slot = table_slot_create(mcx, rel)?;
    let mut scan = table_index_fetch_begin(rel);
    let found = table_index_fetch_tuple(
        mcx,
        &mut scan,
        tid,
        snapshot,
        &mut slot,
        &mut call_again,
        all_dead,
    )?;
    table_index_fetch_end(scan);
    // ExecDropSingleTupleTableSlot: the owned slot drops here.
    exectuples::exec_clear_tuple(&mut slot, mcx);

    Ok(found)
}

pub fn table_index_delete_tuples<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    delstate: &mut TM_IndexDeleteOp<'mcx>,
) -> PgResult<TransactionId> {
    match am(rel) {
        TableAm::Heap => heap::index_delete_tuples(mcx, rel, delstate),
    }
}

// --- Non-modifying operations on individual tuples ---

pub fn table_tuple_fetch_row_version<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &ItemPointerData,
    snapshot: &Snapshot<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_tuple_fetch_row_version call during logical decoding",
        ));
    }
    match am(rel) {
        TableAm::Heap => {
            ::heapam_handler::heapam_fetch_row_version(mcx, rel, tid, snapshot, slot)
        }
    }
}

pub fn table_tuple_tid_valid(scan: &mut TableScanDesc<'_>, tid: &ItemPointerData) -> bool {
    match scan {
        TableScanDesc::Heap(h) => ::heapam_handler::heapam_tuple_tid_valid(h, tid),
    }
}

pub fn table_tuple_get_latest_tid<'mcx>(
    _mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    tid: &mut ItemPointerData,
) -> PgResult<()> {
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_tuple_get_latest_tid call during logical decoding",
        ));
    }

    // User-supplied TID: don't trust the input too much.
    if !table_tuple_tid_valid(scan, tid) {
        let blk = ItemPointerGetBlockNumber(tid);
        let off = tid.ip_posid;
        let relname = scan.base().rs_rd.name();
        return Err(Box::new(
            PgError::error(format!(
                "tid ({blk}, {off}) is not valid for relation \"{relname}\""
            ))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }

    match scan {
        TableScanDesc::Heap(h) => ::heapam_handler::heapam_tuple_get_latest_tid(h, tid),
    }
}

pub fn table_tuple_satisfies_snapshot<'mcx>(
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    snapshot: &Snapshot<'mcx>,
) -> PgResult<bool> {
    match am(rel) {
        TableAm::Heap => ::heapam_handler::heapam_tuple_satisfies_snapshot(rel, slot, snapshot),
    }
}

// --- Manipulations of physical tuples (tableam.h wrappers) ---

pub fn table_tuple_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    cid: CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
) -> PgResult<()> {
    match am(rel) {
        TableAm::Heap => heap::tuple_insert(mcx, rel, slot, cid, options, bistate),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn table_tuple_insert_speculative<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    cid: CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
    spec_token: u32,
) -> PgResult<()> {
    match am(rel) {
        TableAm::Heap => {
            heap::tuple_insert_speculative(mcx, rel, slot, cid, options, bistate, spec_token)
        }
    }
}

pub fn table_tuple_complete_speculative<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    spec_token: u32,
    succeeded: bool,
) -> PgResult<()> {
    match am(rel) {
        TableAm::Heap => heap::tuple_complete_speculative(mcx, rel, slot, spec_token, succeeded),
    }
}

pub fn table_multi_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slots: &mut [&mut SlotData<'mcx>],
    cid: CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
) -> PgResult<()> {
    match am(rel) {
        TableAm::Heap => heap::multi_insert(mcx, rel, slots, cid, options, bistate),
    }
}

// heapam_methods leaves the optional finish_bulk_insert slot NULL, so for the
// only shipped AM the C inline is a no-op after the rd_tableam probe.
pub fn table_finish_bulk_insert(rel: &Relation<'_>, _options: i32) -> PgResult<()> {
    let _ = am(rel);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn table_tuple_delete<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &ItemPointerData,
    cid: CommandId,
    snapshot: &Snapshot<'mcx>,
    crosscheck: &Snapshot<'mcx>,
    wait: bool,
    tmfd: &mut TM_FailureData,
    changingPart: bool,
) -> PgResult<TM_Result> {
    match am(rel) {
        TableAm::Heap => heap::tuple_delete(
            mcx,
            rel,
            tid,
            cid,
            snapshot,
            crosscheck,
            wait,
            tmfd,
            changingPart,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn table_tuple_update<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    otid: &ItemPointerData,
    slot: &mut SlotData<'mcx>,
    cid: CommandId,
    snapshot: &Snapshot<'mcx>,
    crosscheck: &Snapshot<'mcx>,
    wait: bool,
    tmfd: &mut TM_FailureData,
    lockmode: &mut LockTupleMode,
    update_indexes: &mut TU_UpdateIndexes,
) -> PgResult<TM_Result> {
    match am(rel) {
        TableAm::Heap => heap::tuple_update(
            mcx,
            rel,
            otid,
            slot,
            cid,
            snapshot,
            crosscheck,
            wait,
            tmfd,
            lockmode,
            update_indexes,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn table_tuple_lock<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &ItemPointerData,
    snapshot: &Snapshot<'mcx>,
    slot: &mut SlotData<'mcx>,
    cid: CommandId,
    mode: LockTupleMode,
    wait_policy: LockWaitPolicy,
    flags: u8,
    tmfd: &mut TM_FailureData,
) -> PgResult<TM_Result> {
    match am(rel) {
        TableAm::Heap => heap::tuple_lock(
            mcx,
            rel,
            tid,
            snapshot,
            slot,
            cid,
            mode,
            wait_policy,
            flags,
            tmfd,
        ),
    }
}

// --- DDL-adjacent dispatch (tableam.h wrappers) ---

pub fn table_relation_set_new_filelocator(
    rel: &Relation<'_>,
    newrlocator: &RelFileLocator,
    relpersistence: i8,
) -> PgResult<(TransactionId, TransactionId)> {
    match am(rel) {
        TableAm::Heap => heap::relation_set_new_filelocator(rel, newrlocator, relpersistence),
    }
}

pub fn table_relation_copy_data(rel: &Relation<'_>, newrlocator: &RelFileLocator) -> PgResult<()> {
    match am(rel) {
        TableAm::Heap => heap::relation_copy_data(rel, newrlocator),
    }
}

pub fn table_relation_nontransactional_truncate(rel: &Relation<'_>) -> PgResult<()> {
    match am(rel) {
        TableAm::Heap => heap::relation_nontransactional_truncate(rel),
    }
}

pub fn table_relation_needs_toast_table(rel: &Relation<'_>) -> bool {
    match am(rel) {
        TableAm::Heap => heap::relation_needs_toast_table(rel),
    }
}

pub fn table_relation_toast_am(rel: &Relation<'_>) -> ::types_core::Oid {
    match am(rel) {
        TableAm::Heap => heap::relation_toast_am(rel),
    }
}

pub fn table_relation_size(rel: &Relation<'_>, forkNumber: ForkNumber) -> PgResult<u64> {
    match am(rel) {
        TableAm::Heap => heap::relation_size(rel, forkNumber),
    }
}

// --- ANALYZE / bitmap / sample scan dispatch (tableam.h wrappers) ---

pub fn table_scan_analyze_next_block<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    next_buffer: &mut dyn FnMut() -> PgResult<Buffer>,
) -> PgResult<bool> {
    match scan {
        TableScanDesc::Heap(h) => heap::scan_analyze_next_block(mcx, h, next_buffer),
    }
}

pub fn table_scan_analyze_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    oldest_xmin: TransactionId,
    liverows: &mut f64,
    deadrows: &mut f64,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    match scan {
        TableScanDesc::Heap(h) => {
            heap::scan_analyze_next_tuple(mcx, h, oldest_xmin, liverows, deadrows, slot)
        }
    }
}

// C divergence: the TBM iterator + bitmap arrive as parameters (C rides them
// in rs_base.st.rs_tbmiterator; ours carries no bitmap back-pointer).
#[allow(clippy::too_many_arguments)]
pub fn table_scan_bitmap_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    tbm: Option<&tidbitmap::TIDBitmap<'_>>,
    iterator: &mut tidbitmap::TbmIterator,
    slot: &mut SlotData<'mcx>,
    recheck: &mut bool,
    lossy_pages: &mut u64,
    exact_pages: &mut u64,
) -> PgResult<bool> {
    match scan {
        TableScanDesc::Heap(h) => {
            heap::scan_bitmap_next_tuple(mcx, h, tbm, iterator, slot, recheck, lossy_pages, exact_pages)
        }
    }
}

pub fn table_scan_sample_next_block<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    scanstate: &mut dyn SampleScanDriver,
) -> PgResult<bool> {
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_scan_sample_next_block call during logical decoding",
        ));
    }
    match scan {
        TableScanDesc::Heap(h) => heap::scan_sample_next_block(mcx, h, scanstate),
    }
}

pub fn table_scan_sample_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    scanstate: &mut dyn SampleScanDriver,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_scan_sample_next_tuple call during logical decoding",
        ));
    }
    match scan {
        TableScanDesc::Heap(h) => heap::scan_sample_next_tuple(mcx, h, scanstate, slot),
    }
}

// --- Functions to make modifications a bit simpler (tableam.c) ---

pub fn simple_table_tuple_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<()> {
    let cid = xact_seams::get_current_command_id::call(true)?;
    table_tuple_insert(mcx, rel, slot, cid, 0, None)
}

pub fn simple_table_tuple_delete<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &ItemPointerData,
    snapshot: &Snapshot<'mcx>,
) -> PgResult<()> {
    let mut tmfd = TM_FailureData::default();

    let cid = xact_seams::get_current_command_id::call(true)?;
    let result = table_tuple_delete(
        mcx, rel, tid, cid, snapshot, &None, /* InvalidSnapshot */
        true,  /* wait for commit */
        &mut tmfd, false, /* changingPart */
    )?;

    match result {
        TM_Result::TM_SelfModified => Err(elog_error("tuple already updated by self")),
        TM_Result::TM_Ok => Ok(()),
        TM_Result::TM_Updated => Err(elog_error("tuple concurrently updated")),
        TM_Result::TM_Deleted => Err(elog_error("tuple concurrently deleted")),
        other => Err(elog_error(format!(
            "unrecognized table_tuple_delete status: {}",
            other as u32
        ))),
    }
}

pub fn simple_table_tuple_update<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    otid: &ItemPointerData,
    slot: &mut SlotData<'mcx>,
    snapshot: &Snapshot<'mcx>,
    update_indexes: &mut TU_UpdateIndexes,
) -> PgResult<()> {
    let mut tmfd = TM_FailureData::default();
    let mut lockmode = LockTupleMode::LockTupleNoKeyExclusive;

    let cid = xact_seams::get_current_command_id::call(true)?;
    let result = table_tuple_update(
        mcx,
        rel,
        otid,
        slot,
        cid,
        snapshot,
        &None, /* InvalidSnapshot */
        true,  /* wait for commit */
        &mut tmfd,
        &mut lockmode,
        update_indexes,
    )?;

    match result {
        TM_Result::TM_SelfModified => Err(elog_error("tuple already updated by self")),
        TM_Result::TM_Ok => Ok(()),
        TM_Result::TM_Updated => Err(elog_error("tuple concurrently updated")),
        TM_Result::TM_Deleted => Err(elog_error("tuple concurrently deleted")),
        other => Err(elog_error(format!(
            "unrecognized table_tuple_update status: {}",
            other as u32
        ))),
    }
}

// --- Parallel-scan sizing for block-oriented AMs (tableam.c) ---

pub fn table_block_parallelscan_estimate(_rel: &Relation<'_>) -> usize {
    core::mem::size_of::<ParallelBlockTableScanDescData>()
}

pub fn table_block_parallelscan_initialize(
    rel: &Relation<'_>,
    pscan: &mut ParallelBlockTableScanDescData,
) -> PgResult<usize> {
    use core::sync::atomic::{AtomicU32, AtomicU64};
    use ::types_core::primitive::InvalidBlockNumber;
    use ::types_storage::Spinlock;

    pscan.phs_locator = relation_locator(rel);
    let phs_nblocks = relation_nblocks(rel)?;
    pscan.phs_nblocks = phs_nblocks;
    // compare phs_syncscan initialization to similar logic in initscan
    pscan.phs_syncscan = synchronize_seqscans()
        && !rel.uses_local_buffers()
        && phs_nblocks > (nbuffers()? / 4) as BlockNumber;
    pscan.phs_mutex = Spinlock::new();
    pscan.phs_startblock = AtomicU32::new(InvalidBlockNumber);
    pscan.phs_nallocated = AtomicU64::new(0);

    Ok(core::mem::size_of::<ParallelBlockTableScanDescData>())
}

pub fn table_block_parallelscan_reinitialize(
    _rel: &Relation<'_>,
    pscan: &ParallelBlockTableScanDescData,
) {
    pscan
        .phs_nallocated
        .store(0, core::sync::atomic::Ordering::SeqCst);
}

// --- Relation-sizing helpers for block-oriented AMs (tableam.c) ---

pub fn table_block_relation_size(rel: &Relation<'_>, forkNumber: ForkNumber) -> PgResult<u64> {
    let h = smgr::RelationGetSmgr(rel)?;
    let mut nblocks: u64 = 0;
    if forkNumber == ForkNumber::InvalidForkNumber {
        // C sums forks i < MAX_FORKNUM, i.e. INIT_FORKNUM excluded (bug-compat).
        for fork in [
            ForkNumber::MAIN_FORKNUM,
            ForkNumber::FSM_FORKNUM,
            ForkNumber::VISIBILITYMAP_FORKNUM,
        ] {
            nblocks += smgr::smgrnblocks_h(h, fork)? as u64;
        }
    } else {
        nblocks = smgr::smgrnblocks_h(h, forkNumber)? as u64;
    }
    Ok(nblocks * ::types_core::BLCKSZ as u64)
}

/// `table_relation_estimate_size` for the heap AM (heapam_estimate_rel_size ->
/// table_block_relation_estimate_size). `get_rel_data_width` lives in the
/// planner's plancat (a direct dep here would cycle), so the never-vacuumed
/// density fallback arrives as `data_width` (called with the attr-width cache).
#[allow(clippy::too_many_arguments)]
pub fn table_relation_estimate_size(
    rel: &Relation<'_>,
    overhead_bytes_per_tuple: usize,
    usable_bytes_per_page: usize,
    data_width: impl FnOnce(Option<&mut [i32]>) -> PgResult<i32>,
    mut attr_widths: Option<&mut [i32]>,
    pages: &mut BlockNumber,
    tuples: &mut f64,
    allvisfrac: &mut f64,
) -> PgResult<()> {
    let curpages =
        bufmgr_seams::relation_get_number_of_blocks_in_fork::call(rel, ForkNumber::MAIN_FORKNUM)?;
    let fillfactor = rel.get_fillfactor(HEAP_DEFAULT_FILLFACTOR);
    block_relation_estimate_size_math(
        curpages,
        rel.rd_rel.relpages as BlockNumber,
        rel.rd_rel.reltuples as f64,
        rel.rd_rel.relallvisible as BlockNumber,
        rel.rd_rel.relhassubclass,
        |aw| {
            let mut tuple_width = data_width(aw)? as usize;
            tuple_width += overhead_bytes_per_tuple;
            // C Size arithmetic: integer division is intentional.
            let raw = usable_bytes_per_page * fillfactor as usize / 100 / tuple_width;
            Ok(clamp_row_est(raw as f64))
        },
        attr_widths.take(),
        pages,
        tuples,
        allvisfrac,
    )
}

// clamp_row_est (costsize.c); grounded here for the density fallback (the
// costsize copy lives with the planner).
fn clamp_row_est(nrows: f64) -> f64 {
    const MAXIMUM_ROWCOUNT: f64 = 1e100;
    if nrows > MAXIMUM_ROWCOUNT || nrows.is_nan() {
        MAXIMUM_ROWCOUNT
    } else if nrows <= 1.0 {
        1.0
    } else {
        nrows.round_ties_even()
    }
}

#[allow(clippy::too_many_arguments)]
pub fn table_block_relation_estimate_size(
    rel: &Relation<'_>,
    attr_widths: Option<&mut [i32]>,
    pages: &mut BlockNumber,
    tuples: &mut f64,
    allvisfrac: &mut f64,
    overhead_bytes_per_tuple: usize,
    usable_bytes_per_page: usize,
) -> PgResult<()> {
    let curpages = relation_nblocks(rel)?;
    let _ = rel.get_fillfactor(HEAP_DEFAULT_FILLFACTOR);
    let _ = (overhead_bytes_per_tuple, usable_bytes_per_page);
    block_relation_estimate_size_math(
        curpages,
        rel.rd_rel.relpages as BlockNumber,
        rel.rd_rel.reltuples as f64,
        rel.rd_rel.relallvisible as BlockNumber,
        rel.rd_rel.relhassubclass,
        // Never-vacuumed density fallback needs planner units, unported.
        |_aw| unported("backend-optimizer-util-plancat (get_rel_data_width)"),
        attr_widths,
        pages,
        tuples,
        allvisfrac,
    )
}

fn block_relation_estimate_size_math(
    mut curpages: BlockNumber,
    relpages: BlockNumber,
    reltuples: f64,
    relallvisible: BlockNumber,
    relhassubclass: bool,
    density_fallback: impl FnOnce(Option<&mut [i32]>) -> PgResult<f64>,
    attr_widths: Option<&mut [i32]>,
    pages: &mut BlockNumber,
    tuples: &mut f64,
    allvisfrac: &mut f64,
) -> PgResult<()> {
    // Never-vacuumed (reltuples < 0) tables get a 10-page floor (cached-plan
    // guard); skipped when there are inheritance children.
    if curpages < 10 && reltuples < 0.0 && !relhassubclass {
        curpages = 10;
    }

    *pages = curpages;
    if curpages == 0 {
        *tuples = 0.0;
        *allvisfrac = 0.0;
        return Ok(());
    }

    let density: f64 = if reltuples >= 0.0 && relpages > 0 {
        reltuples / relpages as f64
    } else {
        density_fallback(attr_widths)?
    };
    // C rint(): round half to even.
    *tuples = (density * curpages as f64).round_ties_even();

    // relallvisible is used as-is (pages added since VACUUM are likely not
    // all-visible), converted to the fraction costsize.c wants.
    if relallvisible == 0 || curpages == 0 {
        *allvisfrac = 0.0;
    } else if relallvisible as f64 >= curpages as f64 {
        *allvisfrac = 1.0;
    } else {
        *allvisfrac = relallvisible as f64 / curpages as f64;
    }

    Ok(())
}

// --- Unported providers, loud per rule 5 ---

fn relation_locator(rel: &Relation<'_>) -> RelFileLocator {
    bufmgr_seams::relation_smgr_locator::call(rel).locator
}

fn relation_nblocks(rel: &Relation<'_>) -> PgResult<BlockNumber> {
    bufmgr_seams::relation_get_number_of_blocks_in_fork::call(rel, ForkNumber::MAIN_FORKNUM)
}

fn nbuffers() -> PgResult<i32> {
    Ok(init_small::globals::NBuffers())
}
