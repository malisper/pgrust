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

pub mod write_buffer;

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
    // Boxed: cold at scan-begin, keeps the enum heap-sized for the hot arm.
    Pgrcolumnar(std::boxed::Box<::pgrcolumnar::CbScanDescData<'mcx>>),
}

impl<'mcx> TableScanDesc<'mcx> {
    #[inline]
    pub fn base(&self) -> &TableScanDescData<'mcx> {
        match self {
            TableScanDesc::Heap(h) => &h.rs_base,
            TableScanDesc::Pgrcolumnar(c) => &c.rs_base,
        }
    }

    #[inline]
    pub fn base_mut(&mut self) -> &mut TableScanDescData<'mcx> {
        match self {
            TableScanDesc::Heap(h) => &mut h.rs_base,
            TableScanDesc::Pgrcolumnar(c) => &mut c.rs_base,
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

// pgrcolumnar arms (scan-only columnar AM; docs/design/pgrcolumnar-impl.md §7.2).
mod cb {
    use super::*;

    pub(super) fn scan_begin<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        snapshot: Snapshot<'mcx>,
        flags: u32,
        parallel: Option<NonNull<ParallelBlockTableScanDescData>>,
    ) -> PgResult<TableScanDesc<'mcx>> {
        let rs_base = TableScanDescData {
            rs_rd: rel.alias(),
            rs_snapshot: snapshot,
            rs_nkeys: 0,
            rs_key: PgVec::new_in(mcx),
            rs_mintid: ItemPointerData::default(),
            rs_maxtid: ItemPointerData::default(),
            rs_flags: flags,
            rs_parallel: parallel,
            rs_am: TableAm::Pgrcolumnar,
        };
        Ok(TableScanDesc::Pgrcolumnar(std::boxed::Box::new(::pgrcolumnar::CbScanDescData::new(
            rs_base,
        )?)))
    }

    // Shared parallel descriptor: phs_nallocated is the row-group claim
    // cursor (docs/design/pgrcolumnar-impl.md S2); block fields stay unused.
    pub(super) fn parallelscan_initialize(
        rel: &Relation<'_>,
        pscan: &mut ParallelBlockTableScanDescData,
    ) -> usize {
        pscan.phs_locator = rel.rd_locator.get();
        pscan.phs_syncscan = false;
        pscan.phs_nblocks = 0;
        pscan.phs_startblock.store(0, std::sync::atomic::Ordering::Relaxed);
        pscan.phs_nallocated.store(0, std::sync::atomic::Ordering::Relaxed);
        0
    }

    pub(super) fn parallelscan_reinitialize(pscan: &ParallelBlockTableScanDescData) {
        pscan.phs_nallocated.store(0, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cold]
#[inline(never)]
fn cb_refused(what: &'static str) -> ! {
    panic!("cbstore does not support {what} (unreachable without indexes/DML)")
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
        let (result, t_self) = match slot {
            SlotData::Heap(_) | SlotData::BufferHeap(_) => {
                let tuple = match slot {
                    SlotData::Heap(h) => h.tuple.as_mut(),
                    SlotData::BufferHeap(b) => b.base.tuple.as_mut(),
                    _ => unreachable!(),
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
                (result, tuple.t_self)
            }
            // ExecFetchSlotHeapTuple copy arm (virtual source slots, e.g. the
            // ON CONFLICT DO UPDATE projection over a partitioned target).
            _ => {
                let mut tuple = exectuples::exec_copy_slot_heap_tuple(slot, mcx, mcx)?;
                tuple.t_tableOid = rel.rd_id;
                let result = ::heapam::heap_update(
                    rel,
                    otid,
                    &mut tuple,
                    cid,
                    crosscheck.as_deref(),
                    wait,
                    tmfd,
                    lockmode,
                    update_indexes,
                )?;
                (result, tuple.t_self)
            }
        };
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
        let src = ::types_storage::RelFileLocatorBackend {
            locator: rel.rd_locator.get(),
            backend: rel.rd_backend,
        };
        smgr::smgropen(src.locator, src.backend)?;
        ::bufmgr_seams::flush_relation_buffers::call(src)?;
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
        donetuples: i64,
    ) -> PgResult<bool> {
        heapam::sample::heap_scan_sample_next_block(scan, scanstate, donetuples)
    }

    pub(super) fn scan_sample_next_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        scan: &mut HeapScanDescData<'mcx>,
        scanstate: &mut dyn SampleScanDriver,
        donetuples: i64,
        slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool> {
        heapam::sample::heap_scan_sample_next_tuple(mcx, scan, scanstate, donetuples, slot)
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

// check_default_table_access_method (tableamapi.c:102).
fn check_default_table_access_method(
    newval: &mut Option<String>,
    _extra: &mut Option<guc_tables::GucHookExtra>,
    source: ::types_guc::GucSource,
) -> PgResult<bool> {
    let errdetail = |d: String| {
        if guc_seams::guc_check_errdetail::is_installed() {
            guc_seams::guc_check_errdetail::call(d);
        }
    };
    let name = newval.as_deref().unwrap_or("");

    if name.is_empty() {
        errdetail("\"default_table_access_method\" cannot be empty.".to_string());
        return Ok(false);
    }

    if name.len() >= NAMEDATALEN as usize {
        errdetail(format!(
            "\"default_table_access_method\" is too long (maximum {} characters).",
            NAMEDATALEN - 1
        ));
        return Ok(false);
    }

    // C accepts on faith outside a transaction or before database selection;
    // uninstalled seams (unit tests) land on the same path.
    if xact_seams::is_transaction_state::is_installed()
        && tableam_seams::get_table_am_oid::is_installed()
        && xact_seams::is_transaction_state::call()
        && init_small::globals::MyDatabaseId() != ::types_core::InvalidOid
    {
        // A wrong-type AM (e.g. an index AM) raises hard from get_table_am_oid.
        if tableam_seams::get_table_am_oid::call(name, true)? == ::types_core::InvalidOid {
            if source == ::types_guc::GucSource::PGC_S_TEST {
                elog_seams::ereport_msg::call(
                    ::types_error::NOTICE,
                    format!("table access method \"{name}\" does not exist"),
                    None,
                )?;
            } else {
                errdetail(format!("Table access method \"{name}\" does not exist."));
                return Ok(false);
            }
        }
    }

    Ok(true)
}

// --- Shared helpers ---

// CheckXidAlive (xact.c) is set while logical decoding replays a prepared
// (or, phase-2, streamed) transaction; any table access then must go through
// the systable_* wrappers (which set bsysscan). Mirrors the tableam.h
// `unlikely(TransactionIdIsValid(CheckXidAlive) && !bsysscan)` checks.
#[inline]
fn unexpected_during_logical_decoding() -> bool {
    TransactionIdIsValid(xact::CheckXidAlive()) && !xact::bsysscan()
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
            TableAm::Pgrcolumnar => TupleSlotKind::Virtual,
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
        TableAm::Pgrcolumnar => {
            let _ = (nkeys, &key);
            cb::scan_begin(mcx, relation, snapshot, flags, None)
        }
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
        TableAm::Pgrcolumnar => {
            let _ = (nkeys, &key);
            cb::scan_begin(mcx, relation, snapshot, flags, None)
        }
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
        TableAm::Pgrcolumnar => Err(::pgrcolumnar::unsupported("bitmap scans")),
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
        TableAm::Pgrcolumnar => Err(::pgrcolumnar::unsupported("TABLESAMPLE")),
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
        TableAm::Pgrcolumnar => Err(::pgrcolumnar::unsupported("TID scans")),
    }
}

pub fn table_beginscan_analyze<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags = SO_TYPE_ANALYZE;
    match am(relation) {
        TableAm::Heap => heap::scan_begin(mcx, relation, None, 0, PgVec::new_in(mcx), None, flags),
        TableAm::Pgrcolumnar => cb::scan_begin(mcx, relation, None, flags, None),
    }
}

// Ingest-time per-column NDV from the pgrcolumnar part footer (0 = unknown).
pub fn pgrcolumnar_footer_ndv(rel: &Relation<'_>) -> PgResult<Option<Vec<u64>>> {
    ::pgrcolumnar::footer_ndv(rel)
}

/// v8 per-column NDV register sketch (whole-part HyperLogLog). Register
/// sketches union losslessly where NDV scalars cannot be combined — the
/// inherited-ANALYZE consumer unions these across pgrcolumnar children for
/// an honest parent-level NDV. None: no committed footer (zero committed
/// rows). Per-column None entries: sketch absent (pre-v8 part,
/// append-invalidated chain, or the writer kill switch).
pub use ::pgrcolumnar::hll::Hll as PgrcolumnarNdvSketch;
pub fn pgrcolumnar_footer_ndv_hll(
    rel: &Relation<'_>,
) -> PgResult<Option<Vec<Option<PgrcolumnarNdvSketch>>>> {
    ::pgrcolumnar::footer_ndv_hll(rel)
}

// v5 whole-part per-column sorted-asc flags (false = unknown); None while
// the table has no committed footer.
pub fn pgrcolumnar_footer_sorted(rel: &Relation<'_>) -> PgResult<Option<Vec<bool>>> {
    ::pgrcolumnar::footer_sorted(rel)
}

// Per-column on-disk chunk bytes from the pgrcolumnar part footer (planner
// column-fraction seqscan disk costing); None while the table has no
// committed footer.
pub fn pgrcolumnar_footer_col_bytes(rel: &Relation<'_>) -> PgResult<Option<Vec<u64>>> {
    ::pgrcolumnar::footer_col_bytes(rel)
}

// Every committed RG carries exact v7 zero/empty counts (q2box lane): the
// plan-time answerability probe for the executor's meta zero-count qual
// arm. None while the table has no committed footer; Some(false) on
// v<=6-lineage parts (incl. preserved-RG mixtures).
pub fn pgrcolumnar_footer_zerocnt_all(rel: &Relation<'_>) -> PgResult<Option<bool>> {
    ::pgrcolumnar::footer_zerocnt_all(rel)
}

// v7 per-column part-global stitch dict sizes (SE-TOPNNI plan-time DictCode
// sort-key answerability; 0 = no stitch). None while the table has no
// committed footer; empty on pre-v7 parts.
pub fn pgrcolumnar_footer_stitch_gndv(rel: &Relation<'_>) -> PgResult<Option<Vec<u64>>> {
    ::pgrcolumnar::footer_stitch_gndv(rel)
}

// pgrcolumnar's AM-specific sample acquisition (C table_relation_analyze lets the
// AM supply the whole acquirefunc): row-group enumeration + random row fetch.
pub fn pgrcolumnar_analyze_visible_rgs(scan: &TableScanDesc<'_>) -> PgResult<Vec<(u32, u32)>> {
    match scan {
        TableScanDesc::Pgrcolumnar(c) => c.analyze_visible_rgs(),
        TableScanDesc::Heap(_) => panic!("cbstore_analyze_visible_rgs: heap scan"),
    }
}

pub fn pgrcolumnar_analyze_fetch_row(
    scan: &mut TableScanDesc<'_>,
    rg: u32,
    row: u32,
    slot: &mut SlotData<'_>,
) -> bool {
    match scan {
        TableScanDesc::Pgrcolumnar(c) => c.gather_row(rg, row, slot),
        TableScanDesc::Heap(_) => panic!("cbstore_analyze_fetch_row: heap scan"),
    }
}

// Parallel ANALYZE sample fetch (loadfinal lane, PGRUST_ANALYZE_SAMPLE_POOL):
// materialize `refs` (ascending (rg, rg-global row) sample positions) into
// `slot` in order via `per_row`, decoding granules on `pool` threads. Rows,
// values, and order are identical to per-ref pgrcolumnar_analyze_fetch_row
// fetches by construction. Returns the number of granule decode tasks.
pub fn pgrcolumnar_analyze_gather_rows(
    scan: &mut TableScanDesc<'_>,
    refs: &[(u32, u32)],
    pool: usize,
    slot: &mut SlotData<'_>,
    per_row: &mut dyn FnMut(&mut SlotData<'_>) -> PgResult<()>,
) -> PgResult<u64> {
    match scan {
        TableScanDesc::Pgrcolumnar(c) => c.analyze_gather_rows(refs, pool, slot, per_row),
        TableScanDesc::Heap(_) => panic!("cbstore_analyze_gather_rows: heap scan"),
    }
}

pub fn table_endscan(scan: TableScanDesc<'_>) -> PgResult<()> {
    match scan {
        TableScanDesc::Heap(h) => heap::scan_end(h),
        TableScanDesc::Pgrcolumnar(mut c) => {
            if std::env::var_os("PGRUST_AGG_BATCH_DEBUG").is_some() {
                let (dfb, dft, dfe, dfa) = ::pgrcolumnar::dict_frame_stats();
                eprintln!(
                    "CBSCAN|windows={}|granules_scanned={}|granules_pruned={}|blocks_pruned={}|granules_bound_skipped={}|adaptive_probe_reverts={}|granules_meta={}|granules_bloom_pruned={}|rgs_readahead={}|rgs_claim_readahead={}|dictlazy_builds={dfb}|dict_frames={dft}|dict_frames_ensured={dfe}|dict_ensure_alls={dfa}",
                    c.windows_staged,
                    c.granules_scanned,
                    c.granules_pruned,
                    c.blocks_pruned,
                    c.granules_bound_skipped,
                    c.adaptive_probe_reverts,
                    c.granules_meta,
                    c.granules_bloom_pruned,
                    c.rgs_readahead,
                    c.rgs_claim_readahead
                );
            }
            if (c.rs_base.rs_flags & SO_TEMP_SNAPSHOT) != 0 {
                let snap = c
                    .rs_temp_snapshot
                    .take()
                    .expect("SO_TEMP_SNAPSHOT scan carries its registered snapshot");
                ::snapmgr::UnregisterSnapshot(Some(&snap));
            }
            drop(c);
            Ok(())
        }
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
        TableScanDesc::Pgrcolumnar(c) => {
            let _ = key;
            c.reset_position();
            Ok(())
        }
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
        TableScanDesc::Pgrcolumnar(c) => {
            let _ = key;
            c.reset_position();
            Ok(())
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
        TableScanDesc::Pgrcolumnar(c) => {
            if direction == ScanDirection::BackwardScanDirection {
                return Err(::pgrcolumnar::unsupported("backward scans"));
            }
            c.getnextslot(slot)
        }
    }
}

pub fn table_scan_supports_pagebatch(scan: &TableScanDesc<'_>) -> bool {
    match scan {
        TableScanDesc::Heap(h) => {
            (h.rs_base.rs_flags & SO_ALLOW_PAGEMODE) != 0 && h.rs_base.rs_parallel.is_none()
        }
        // DELIBERATELY false (was `true` on the old branch): this is the
        // INCUMBENT fused drives' gate (fused agg/sort/hash batched paths in
        // the row executor) plus the lane's admission-economics probe. The
        // lane-v2 pipeline owns pgrcolumnar batching through the PARALLEL
        // variant below instead; keeping the incumbents off pgrcolumnar keeps
        // lane-OFF the pure per-row Volcano drive (`getnextslot`) — the
        // byte-parity oracle for every pgrcolumnar lane path (phase4 design §6).
        TableScanDesc::Pgrcolumnar(_) => false,
    }
}

/// As `table_scan_supports_pagebatch`, but also admits parallel scan
/// descriptors: the batched page feed (`heap_getnextpagebatch` →
/// `pagemode_next_page` → `heap_fetch_next_buffer`) acquires blocks through
/// `parallel_next_block` — the shared DSM block cursor — exactly as the
/// per-tuple pagemode walk does, so page-at-a-time batching partitions blocks
/// across workers without gaps or overlaps. The strict gate above stays for
/// the fused agg/sort/hash drives until each is audited under parallel; only
/// the lane-v2 SeqScan drive routes here.
pub fn table_scan_supports_pagebatch_parallel(scan: &TableScanDesc<'_>) -> bool {
    match scan {
        TableScanDesc::Heap(h) => (h.rs_base.rs_flags & SO_ALLOW_PAGEMODE) != 0,
        // The PgrcolumnarSource tranche: the lane owns pgrcolumnar scan staging
        // (`next_window` windows <= WINDOW_ROWS, RG/granule/block pruning
        // inside produce). Serial and parallel both admit — the window feed
        // claims row groups through the shared `phs_nallocated` cursor
        // exactly as the per-tuple drive does, partitioning RGs across
        // workers without gaps or overlaps.
        TableScanDesc::Pgrcolumnar(_) => true,
    }
}

/// Total committed pgrcolumnar rows (footer metadata; no decode) — the lane's
/// tiny-input admission floor. None = heap (the floor is pgrcolumnar-only today).
pub fn table_scan_cb_total_rows(scan: &TableScanDesc<'_>) -> Option<u64> {
    match scan {
        TableScanDesc::Heap(_) => None,
        TableScanDesc::Pgrcolumnar(c) => Some(c.total_rows()),
    }
}

/// Part-global granule geometry of a pgrcolumnar scan (runtime morsel source,
/// M1 scan pipelines): (total granules, row-group-start prefix sums = the
/// hard morsel boundaries). None = heap or empty part.
pub fn table_scan_cb_granule_geometry(scan: &TableScanDesc<'_>) -> Option<(u64, Vec<u64>)> {
    match scan {
        TableScanDesc::Heap(_) => None,
        TableScanDesc::Pgrcolumnar(c) => c.granule_geometry(),
    }
}

/// Heap block-range geometry for the runtime morsel source (M1 heap
/// source): total blocks at scan start = the granule count (granule = one
/// block; heap has no dictionary epochs, so a heap source has no interior
/// morsel boundaries). `None` = not a heap scan.
pub fn table_scan_heap_block_geometry(scan: &TableScanDesc<'_>) -> Option<u64> {
    match scan {
        TableScanDesc::Heap(h) => Some(h.rs_nblocks as u64),
        TableScanDesc::Pgrcolumnar(_) => None,
    }
}

/// Position a PGRCOLUMNAR scan on the granule claim [g0, g1). Ok(false) =
/// heap, scan untouched — the m2 sink arms' fail-closed worker check
/// (their admission is pgrcolumnar-only; AM-dispatching them onto heap blocks
/// would silently change the claim unit under a divergent worker plan).
/// The AM-dispatched positioning is `table_scan_set_morsel_range` below.
pub fn table_scan_cb_set_granule_range(
    scan: &mut TableScanDesc<'_>,
    g0: u64,
    g1: u64,
) -> PgResult<bool> {
    match scan {
        TableScanDesc::Heap(_) => Ok(false),
        TableScanDesc::Pgrcolumnar(c) => {
            c.set_granule_range(g0, g1)?;
            Ok(true)
        }
    }
}

/// Position the scan on the morsel claim [g0, g1), dispatching on the AM
/// (the runtime morsel drive): pgrcolumnar absolute granules (must lie inside
/// one row group — the runtime clamps claims to the boundaries
/// `table_scan_cb_granule_geometry` reports) or heap blocks (any contiguous
/// in-relation range; heap reports no interior boundaries).
pub fn table_scan_set_morsel_range(
    scan: &mut TableScanDesc<'_>,
    g0: u64,
    g1: u64,
) -> PgResult<()> {
    match scan {
        TableScanDesc::Heap(h) => ::heapam::heap_set_block_range(h, g0, g1),
        TableScanDesc::Pgrcolumnar(c) => c.set_granule_range(g0, g1),
    }
}

/// End-of-claim pin release (single-executor wave 2, WS-O inc-2,
/// append-only seam): heap resets to the drained state, dropping the
/// current page pin if the claim ended early (`heap_end_claim_release` —
/// the R3 zero-pins-at-settle law); pgrcolumnar stages no page pins — no-op.
pub fn table_scan_end_claim_release(scan: &mut TableScanDesc<'_>) {
    match scan {
        TableScanDesc::Heap(h) => ::heapam::heap_end_claim_release(h),
        TableScanDesc::Pgrcolumnar(_) => {}
    }
}

/// Cursor-suspension park point (WS-AI wave-9.5, lane-cursors.md §2,
/// append-only seam): the staged page batch's reposition window
/// `(b0, b1)` — restageable remainder `[b0, b1)` including the staged
/// block — iff the scan holds a settleable mid-claim heap page
/// (`heap_cursor_park_point`). None = nothing staged/pinned, a
/// wrap-capable walk (syncscan-started), a parallel scan, or pgrcolumnar
/// (whose staged windows hold no bufmgr pins — R4 decode scratch; nothing
/// to settle, node-resident by design).
pub fn table_scan_cursor_park_point(scan: &TableScanDesc<'_>) -> Option<(u64, u64)> {
    match scan {
        TableScanDesc::Heap(h) => ::heapam::heap_cursor_park_point(h),
        TableScanDesc::Pgrcolumnar(_) => None,
    }
}

/// Mid-page batch adoption (SE-R41 v2, the page-remainder defect fix): when
/// a batch drive freshly engages over a scan the per-tuple walk already
/// advanced mid-page, adopt the current page's unconsumed remainder
/// `[start, n)` instead of advancing past it (`heap_adopt_midpage_batch`).
/// None = nothing to adopt — a fresh/drained scan stages the next page as
/// before. pgrcolumnar: its staged windows are not page-cursor-interleaved
/// (different staging machinery) — always None below the AM dispatch.
pub fn table_scan_adopt_midpage_batch(scan: &mut TableScanDesc<'_>) -> Option<(u32, u32)> {
    match scan {
        TableScanDesc::Heap(h) => ::heapam::heap_adopt_midpage_batch(h),
        TableScanDesc::Pgrcolumnar(_) => None,
    }
}

/// R3 settle probe (debug-assert face of the zero-pins-at-settle law):
/// does this scan still hold a claim page pin? pgrcolumnar stages no page
/// pins — always false below the AM dispatch.
pub fn table_scan_holds_claim_pin(scan: &TableScanDesc<'_>) -> bool {
    match scan {
        TableScanDesc::Heap(h) => h.rs_cbuf.is_some(),
        TableScanDesc::Pgrcolumnar(_) => false,
    }
}

/// GCUT zone summary of a pgrcolumnar scan's key column (night/sort-merge-
/// redesign inc-2): per-granule best direction-folded order words + the
/// zone-max seed word — see `CbScanDescData::zone_topk_words` for the
/// correctness posture. `None` = heap or no columnar part.
pub fn table_scan_cb_zone_topk_words(
    scan: &TableScanDesc<'_>,
    col: u16,
    desc: bool,
    bound: u64,
) -> PgResult<Option<(Vec<u64>, Option<u64>)>> {
    match scan {
        TableScanDesc::Heap(_) => Ok(None),
        TableScanDesc::Pgrcolumnar(c) => c.zone_topk_words(col, desc, bound),
    }
}

/// Direct bounded top-N granule drive of a pgrcolumnar scan (the runtime
/// sort arm's PGRUST_RUNTIME_TOPN_HEAP feed): hand out the current
/// granule-range claim's next granule whole — (nrows, rowref_base) — with
/// the per-RG visibility gate, no window/SoA staging. `None` = claim
/// exhausted. Heap AMs error (the arm admits pgrcolumnar only).
pub fn table_scan_topn_direct_next_granule(
    scan: &mut TableScanDesc<'_>,
) -> PgResult<Option<(u32, u64)>> {
    match scan {
        TableScanDesc::Heap(_) => Err(elog_error(
            "direct top-N granule drive on a non-columnar scan",
        )),
        TableScanDesc::Pgrcolumnar(c) => c.topn_direct_next_granule(),
    }
}

/// The decoded datum lane of column `col` for the granule
/// `table_scan_topn_direct_next_granule` just handed out. None = dict
/// column / out of range (the caller fails closed to the staged feed).
pub fn table_scan_topn_direct_lane<'a>(
    scan: &'a mut TableScanDesc<'_>,
    col: usize,
) -> Option<&'a [::datum::Datum]> {
    match scan {
        TableScanDesc::Heap(_) => None,
        TableScanDesc::Pgrcolumnar(c) => c.topn_direct_lane(col),
    }
}

/// Drive-scaling observability counters of a pgrcolumnar scan (the runtime
/// WFIN channel): (rg_switches, dict_builds, granules_scanned,
/// windows_staged). None = heap.
pub fn table_scan_cb_drive_counters(scan: &TableScanDesc<'_>) -> Option<(u64, u64, u64, u64)> {
    match scan {
        TableScanDesc::Heap(_) => None,
        TableScanDesc::Pgrcolumnar(c) => Some(c.drive_counters()),
    }
}

/// EA-on-morsels: snapshot the pgrcolumnar scan descriptor's cumulative debug
/// counters (the CBSCAN line's fields) for the EXPLAIN ANALYZE prune fold —
/// [granules_scanned, granules_pruned, granules_bloom_pruned, granules_meta,
/// granules_bound_skipped, blocks_pruned, windows_staged]. None on heap.
/// Read-only; the counters are the pre-existing unconditional scan-desc
/// accumulators (docs/design/ea-morsels.md §1).
pub fn table_scan_cb_ea_counters(scan: &TableScanDesc<'_>) -> Option<[u64; 7]> {
    match scan {
        TableScanDesc::Heap(_) => None,
        TableScanDesc::Pgrcolumnar(c) => Some([
            c.granules_scanned,
            c.granules_pruned,
            c.granules_bloom_pruned,
            c.granules_meta,
            c.granules_bound_skipped,
            c.blocks_pruned,
            c.windows_staged,
        ]),

    }
}

/// Relation size at scan start (heap rs_nblocks): the deform-JIT page gate.
pub fn table_scan_nblocks(scan: &TableScanDesc<'_>) -> u32 {
    match scan {
        TableScanDesc::Heap(h) => h.rs_nblocks,
        TableScanDesc::Pgrcolumnar(c) => c.nblocks(),
    }
}

/// pgrcolumnar column projection: only these attributes' chunks are decoded.
/// Heap scans always materialize whole tuples — no-op.
pub fn table_scan_set_needed_attrs(scan: &mut TableScanDesc<'_>, needed: &[bool]) {
    match scan {
        TableScanDesc::Heap(_) => {}
        TableScanDesc::Pgrcolumnar(c) => c.set_needed_attrs(needed),
    }
}

/// pgrcolumnar post-qual materialization (pgrcolumnar_prewhere): granule decode
/// becomes per-column on demand — deform pulls only the columns it fills,
/// store_slot completes the needed set for surviving rows. Heap no-op.
pub fn table_scan_set_lazy_decode(scan: &mut TableScanDesc<'_>, on: bool) {
    match scan {
        TableScanDesc::Heap(_) => {}
        TableScanDesc::Pgrcolumnar(c) => c.set_lazy_decode(on),
    }
}

/// pgrcolumnar zone-map pruning conjuncts (advisory-only; executor still
/// evaluates the full qual on surviving rows).
/// RG-altitude meta-answerability census over the scan's PUSHED zone quals
/// (pgrcolumnar::zone_meta_rg_census doc — the GL-SERIALTERM-META qual-zone
/// helper). None = heap or no columnar part.
pub fn table_scan_cb_zone_meta_census(
    scan: &TableScanDesc<'_>,
    need_sums: bool,
) -> PgResult<Option<(u64, u64)>> {
    match scan {
        TableScanDesc::Heap(_) => Ok(None),
        TableScanDesc::Pgrcolumnar(c) => c.zone_meta_rg_census(need_sums),
    }
}

pub fn table_scan_push_zone_quals(scan: &mut TableScanDesc<'_>, quals: &[ZoneQual]) {
    match scan {
        TableScanDesc::Heap(_) => {}
        TableScanDesc::Pgrcolumnar(c) => c.push_zone_quals(quals),
    }
}

/// Zone-ordered adaptive granule traversal (pgrcolumnar serial scans over
/// exact-zone int-family columns; docs/design/pgrcolumnar-zone-adaptive.md).
/// false = refused; the physical claim order stays.
pub fn table_scan_arm_adaptive_order(
    scan: &mut TableScanDesc<'_>,
    col: usize,
    desc: bool,
    strict: bool,
) -> PgResult<bool> {
    match scan {
        TableScanDesc::Heap(_) => Ok(false),
        TableScanDesc::Pgrcolumnar(c) => c.arm_adaptive_order(col, desc, strict),
    }
}

/// Drop an armed adaptive traversal (consumer demotion back to the physical
/// claim order); the caller must rescan before pulling again. No-op when
/// unarmed or not pgrcolumnar.
pub fn table_scan_disarm_adaptive_order(scan: &mut TableScanDesc<'_>) {
    match scan {
        TableScanDesc::Heap(_) => {}
        TableScanDesc::Pgrcolumnar(c) => c.disarm_adaptive_order(),
    }
}

/// Consumer bound feedback for an armed adaptive scan (top-k heap floor or
/// running MIN/MAX best); no-op when unarmed.
pub fn table_scan_update_scan_bound(scan: &mut TableScanDesc<'_>, key: ::datum::Datum) {
    match scan {
        TableScanDesc::Heap(_) => {}
        TableScanDesc::Pgrcolumnar(c) => c.set_adaptive_bound(key),
    }
}

/// Footer value min/max covering every row of the staged window (pgrcolumnar
/// granule zone entry; int-family columns only). None = no zone metadata.
pub fn table_scan_window_value_minmax(
    scan: &TableScanDesc<'_>,
    col: usize,
) -> Option<(i64, i64)> {
    match scan {
        TableScanDesc::Heap(_) => None,
        TableScanDesc::Pgrcolumnar(c) => c.staged_window_value_minmax(col),
    }
}

pub use ::pgrcolumnar::CbGranuleMetaStep;

/// v7 granule length-stats metadata peek (pgrcolumnar only; see
/// `CbScanDescData::granule_meta_peek`). Heap scans are never meta.
pub fn table_scan_granule_meta_peek(
    scan: &mut TableScanDesc<'_>,
    key_cols: &[u16],
    len_cols: &[u16],
    key_mm: &mut [(i64, i64)],
    len_stats: &mut [(u64, u32, u32)],
) -> PgResult<CbGranuleMetaStep> {
    match scan {
        TableScanDesc::Heap(_) => Ok(CbGranuleMetaStep::NotMeta),
        TableScanDesc::Pgrcolumnar(c) => c.granule_meta_peek(key_cols, len_cols, key_mm, len_stats),
    }
}

/// Consume the granule the peek just answered (pgrcolumnar only).
pub fn table_scan_granule_meta_consume(scan: &mut TableScanDesc<'_>) {
    match scan {
        TableScanDesc::Heap(_) => unreachable!("granule meta is cbstore-only"),
        TableScanDesc::Pgrcolumnar(c) => c.granule_meta_consume(),
    }
}

pub use ::pgrcolumnar::CbAggMetaStep;

/// Footer-stat aggregate metadata peek (pgrcolumnar only; see
/// `CbScanDescData::agg_meta_peek`). Heap scans are never meta.
#[allow(clippy::too_many_arguments)]
pub fn table_scan_agg_meta_peek(
    scan: &mut TableScanDesc<'_>,
    mm_cols: &[u16],
    sum_cols: &[u16],
    len_cols: &[u16],
    mm: &mut [(i64, i64)],
    sums: &mut [i128],
    lens: &mut [i64],
) -> PgResult<CbAggMetaStep> {
    match scan {
        TableScanDesc::Heap(_) => Ok(CbAggMetaStep::NotMeta),
        TableScanDesc::Pgrcolumnar(c) => {
            c.agg_meta_peek(mm_cols, sum_cols, len_cols, mm, sums, lens)
        }
    }
}

/// Consume the row group the peek just answered (`MetaRg`; pgrcolumnar only).
pub fn table_scan_agg_meta_consume_rg(scan: &mut TableScanDesc<'_>) {
    match scan {
        TableScanDesc::Heap(_) => unreachable!("agg meta is cbstore-only"),
        TableScanDesc::Pgrcolumnar(c) => c.agg_meta_consume_rg(),
    }
}

/// Consume the granule the peek just answered (`MetaGranule`; pgrcolumnar only).
pub fn table_scan_agg_meta_consume_granule(scan: &mut TableScanDesc<'_>) {
    match scan {
        TableScanDesc::Heap(_) => unreachable!("agg meta is cbstore-only"),
        TableScanDesc::Pgrcolumnar(c) => c.agg_meta_consume_granule(),
    }
}

/// Metadata COUNT(*) capability (pgrcolumnar: footer row counts per
/// wholly-visible RG, fail-open per RG otherwise).
pub fn table_scan_supports_meta_count(scan: &TableScanDesc<'_>) -> bool {
    matches!(scan, TableScanDesc::Pgrcolumnar(_))
}

/// Metadata COUNT(*) feed: visible-row count of one row group; 0 = exhausted.
pub fn table_scan_meta_count_next(scan: &mut TableScanDesc<'_>) -> PgResult<u32> {
    match scan {
        TableScanDesc::Pgrcolumnar(c) => c.next_meta_count(),
        TableScanDesc::Heap(_) => unreachable!("meta count is cbstore-only"),
    }
}

pub use ::pgrcolumnar::{MetaAggScan, MetaZeroQual};

/// Condition-cache cumulative counters (hits, misses, insertions,
/// evictions) — the lane stats dump's source.
pub use ::pgrcolumnar::condcache::stats as condcache_stats;

/// Metadata MIN/MAX/COUNT/SUM answer (pgrcolumnar zone maps + footer row counts
/// + footer sums; `zq` = the v7 zero-count qual arm); None = the AM has no
/// exact metadata answer for these columns.
pub fn table_scan_meta_agg(
    scan: &TableScanDesc<'_>,
    cols: &[u16],
    sum_cols: &[u16],
    zq: Option<MetaZeroQual>,
) -> PgResult<Option<MetaAggScan>> {
    match scan {
        TableScanDesc::Pgrcolumnar(c) => c.meta_agg_scan(cols, sum_cols, zq),
        TableScanDesc::Heap(_) => Ok(None),
    }
}

/// Compressed-domain constant-fold of `q` against the currently staged
/// pgrcolumnar granule (int/date/timestamp). Heap has no granule metadata, so
/// its verdict is always Mixed (the staged drive is pgrcolumnar-only anyway).
pub fn table_scan_staged_granule_verdict(
    scan: &TableScanDesc<'_>,
    q: &ZoneQual,
) -> ZoneVerdict {
    match scan {
        TableScanDesc::Heap(_) => ZoneVerdict::Mixed,
        TableScanDesc::Pgrcolumnar(c) => c.staged_granule_verdict(q),
    }
}

/// Arm the pgrcolumnar condition cache (pgrust.condition_cache) on this scan:
/// `fp` = the staged lane prefix's canonical fingerprint, `capacity` = the
/// byte-budget GUC. Heap scans have no granule verdicts — always false.
pub fn table_scan_condcache_arm(scan: &mut TableScanDesc<'_>, fp: u128, capacity: u64) -> bool {
    match scan {
        TableScanDesc::Heap(_) => false,
        TableScanDesc::Pgrcolumnar(c) => c.condcache_arm(fp, capacity),
    }
}

/// Condition-cache lookup for the currently staged pgrcolumnar window: true =
/// hit, `sel` now holds the staged prefix's survivor bits and the qual's
/// decode+eval legs are skipped. false = miss/unarmed (heap: always).
pub fn table_scan_condcache_lookup(scan: &mut TableScanDesc<'_>, sel: &mut [u64]) -> bool {
    match scan {
        TableScanDesc::Heap(_) => false,
        TableScanDesc::Pgrcolumnar(c) => c.condcache_lookup(sel),
    }
}

/// Record the currently staged pgrcolumnar window's freshly evaluated survivor
/// bits into the condition cache. No-op when unarmed or heap.
pub fn table_scan_condcache_store(scan: &mut TableScanDesc<'_>, sel: &[u64]) {
    match scan {
        TableScanDesc::Heap(_) => {}
        TableScanDesc::Pgrcolumnar(c) => c.condcache_store(sel),
    }
}

/// Fold the scan's per-scan condition-cache stat cells into the process
/// counters (before a `condcache_stats` read; scan teardown folds anyway).
pub fn table_scan_condcache_fold_stats(scan: &mut TableScanDesc<'_>) {
    match scan {
        TableScanDesc::Heap(_) => {}
        TableScanDesc::Pgrcolumnar(c) => c.condcache_fold_stats(),
    }
}

/// Page-batch scan feed (upstream batch scan API, CF 6176): 0 = exhausted.
pub fn table_scan_getnextpagebatch<'mcx>(scan: &mut TableScanDesc<'mcx>) -> PgResult<u32> {
    match scan {
        TableScanDesc::Heap(h) => ::heapam::heap_getnextpagebatch(h),
        TableScanDesc::Pgrcolumnar(c) => c.next_window(),
    }
}

/// SoA-deform the staged page batch's column prefix per `plan`.
pub fn table_scan_batch_deform<'mcx>(
    scan: &mut TableScanDesc<'mcx>,
    plan: &::exectuples::SoaDeformPlan<'_>,
    soa: &mut ::exectuples::SoaBatch<'_>,
    qual_col_only: Option<u16>,
) {
    table_scan_batch_deform_sel(scan, plan, soa, qual_col_only, None)
}

/// `table_scan_batch_deform` under an optional PREWHERE selection (the
/// survivor-window COMPLETING deform): `sel` narrows lazy sub-framed dict
/// ensures to selected rows (pgrcolumnar only; cell writes identical, heap
/// ignores it — its deform has no lazy bytes).
pub fn table_scan_batch_deform_sel<'mcx>(
    scan: &mut TableScanDesc<'mcx>,
    plan: &::exectuples::SoaDeformPlan<'_>,
    soa: &mut ::exectuples::SoaBatch<'_>,
    qual_col_only: Option<u16>,
    sel: Option<&[u64]>,
) {
    match scan {
        TableScanDesc::Heap(h) => ::heapam::heap_batch_deform_soa(h, plan, soa, qual_col_only),
        TableScanDesc::Pgrcolumnar(c) => {
            c.batch_deform(plan.ncols() as usize, soa, qual_col_only, sel)
        }
    }
}

/// K1 inc-2 late-materialization STAGING deform (wave-9 WS-AH): the staged
/// page batch's kind-0 column pass narrowed to an explicit column set
/// ({qual clause cols ∪ the grouped feed's key cols}); classification is
/// the full deform's. Heap scans only — the pgrcolumnar staging narrows
/// through PREWHERE's per-clause late materialization instead, so the
/// columnar arm falls open to the full window deform (and debug-asserts:
/// the arming seam refuses non-heap scans).
pub fn table_scan_batch_deform_cols<'mcx>(
    scan: &mut TableScanDesc<'mcx>,
    plan: &::exectuples::SoaDeformPlan<'_>,
    soa: &mut ::exectuples::SoaBatch<'_>,
    cols: &[u16],
) {
    match scan {
        TableScanDesc::Heap(h) => ::heapam::heap_batch_deform_soa_cols(h, plan, soa, cols),
        TableScanDesc::Pgrcolumnar(c) => {
            debug_assert!(false, "late-mat narrowed staging arms on heap scans only");
            c.batch_deform(plan.ncols() as usize, soa, None, None)
        }
    }
}

/// K1 inc-2 COMPLETION deform (the sel-honoring heap arm of the staged
/// batch deform dispatch): fill `cols` for `sel`-selected rows of the
/// ALREADY-staged batch — all-zero 64-row selection words skip whole,
/// full words take the dense fill. Heap = the deform split's completion
/// half (page still pinned, ABI R3); pgrcolumnar staged everything at
/// window fill — no-op (the storage seam default's "sources that staged
/// everything" contract).
pub fn table_scan_batch_complete_deform<'mcx>(
    scan: &mut TableScanDesc<'mcx>,
    plan: &::exectuples::SoaDeformPlan<'_>,
    soa: &mut ::exectuples::SoaBatch<'_>,
    cols: &[u16],
    sel: &[u64],
) {
    match scan {
        TableScanDesc::Heap(h) => {
            ::heapam::heap_batch_complete_deform_soa(h, plan, soa, cols, sel)
        }
        TableScanDesc::Pgrcolumnar(_) => {}
    }
}

/// Prewhere staged deform: fill (or dict-answer) one staged column of the
/// pgrcolumnar window. Callable only on pgrcolumnar scans (the staged qual drive is
/// pgrcolumnar-shaped); the caller owns soa.begin and the needed-set contract.
/// Length-lane re-fill of one staged pgrcolumnar column (post-qual gather for
/// dict-tier qual columns armed as length lanes). Heap scans never arm
/// length lanes (`seq_scan_batch_len_want` is pgrcolumnar-batch-scoped), so the
/// heap arm is unreachable.
pub fn table_scan_batch_fill_len<'mcx>(
    scan: &mut TableScanDesc<'mcx>,
    c: u16,
    chars: bool,
    soa: &mut ::exectuples::SoaBatch<'_>,
) {
    match scan {
        TableScanDesc::Pgrcolumnar(cb) => cb.batch_fill_len_col(c as usize, chars, soa),
        _ => unreachable!("length lanes arm on cbstore batches only"),
    }
}

/// Dict-code side channel of one staged pgrcolumnar window column (str MIN/MAX
/// code folds, lane-v2-dictminmax): `Some` only when the column's current
/// chunk is dict-encoded and decoded for the staged window. Heap scans have
/// no dictionaries — always `None`.
#[inline]
pub fn table_scan_batch_dict_codes<'mcx>(
    scan: &TableScanDesc<'mcx>,
    c: u16,
) -> Option<::exectuples::SoaDictLane> {
    match scan {
        TableScanDesc::Heap(_) => None,
        TableScanDesc::Pgrcolumnar(cb) => cb.staged_codes_lane(c as usize),
    }
}

/// `table_scan_batch_dict_codes` with the v7 part-global stitch published
/// when the scan carries one (the DictCode sort-key class,
/// docs/design/dict-code-flow.md inc-1). Consumers gate order use on
/// `table.has_stitch()` and fail closed otherwise. Heap scans have no
/// dictionaries — always `None`.
#[inline]
pub fn table_scan_batch_dict_codes_global<'mcx>(
    scan: &mut TableScanDesc<'mcx>,
    c: u16,
) -> Option<::exectuples::SoaDictLane> {
    match scan {
        TableScanDesc::Heap(_) => None,
        TableScanDesc::Pgrcolumnar(cb) => cb.staged_codes_lane_global(c as usize),
    }
}

/// Physical rowref base of the CURRENT staged pgrcolumnar window (tie-ordering
/// rule 2): staged row `i`'s rowref is `base + i`. Heap scans carry no
/// rowrefs — always `None` (the rowref-armed consumer demotes).
#[inline]
pub fn table_scan_batch_rowref_base<'mcx>(scan: &TableScanDesc<'mcx>) -> Option<u64> {
    match scan {
        TableScanDesc::Heap(_) => None,
        TableScanDesc::Pgrcolumnar(cb) => cb.staged_rowref_base(),
    }
}

pub fn table_scan_batch_deform_col<'mcx>(
    scan: &mut TableScanDesc<'mcx>,
    c: u16,
    soa: &mut ::exectuples::SoaBatch<'_>,
) {
    match scan {
        TableScanDesc::Heap(_) => unreachable!("staged deform is cbstore-only"),
        TableScanDesc::Pgrcolumnar(cb) => cb.batch_deform_col(c as usize, soa),
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
        TableScanDesc::Pgrcolumnar(c) => c.batch_stage_varkey(plan.key() as usize, soa),
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
        TableScanDesc::Pgrcolumnar(c) => c.store_slot(i, slot),
    }
}

/// Staged pgrcolumnar window base for ref-carrying consumers: (row group,
/// rg-global row index of staged row 0); per-row refs are base + i. None =
/// heap or nothing staged (ref mode never starts).
pub fn table_scan_window_ref(scan: &TableScanDesc<'_>) -> Option<(u32, u32)> {
    match scan {
        TableScanDesc::Heap(_) => None,
        TableScanDesc::Pgrcolumnar(c) => c.window_ref(),
    }
}

/// Materialize rg-global row `row` of row group `rg` into `slot` under the
/// scan's current needed set (the ref-carrying bounded-sort drain). false =
/// unsupported AM; the caller must have materialized eagerly at feed time.
pub fn table_scan_gather_row<'mcx>(
    scan: &mut TableScanDesc<'mcx>,
    rg: u32,
    row: u32,
    slot: &mut SlotData<'mcx>,
) -> bool {
    match scan {
        TableScanDesc::Heap(_) => false,
        TableScanDesc::Pgrcolumnar(c) => c.gather_row(rg, row, slot),
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
        TableScanDesc::Pgrcolumnar(_) => cb_refused("bitmap scans"),
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
        TableScanDesc::Pgrcolumnar(_) => cb_refused("bitmap scans"),
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
            match &mut sscan {
                TableScanDesc::Heap(h) => heap::scan_set_tidrange(h, mintid, maxtid),
                _ => unreachable!(),
            }
            Ok(sscan)
        }
        TableAm::Pgrcolumnar => Err(::pgrcolumnar::unsupported("TID range scans")),
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
        TableScanDesc::Pgrcolumnar(_) => Err(::pgrcolumnar::unsupported("TID range scans")),
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
        TableScanDesc::Pgrcolumnar(_) => Err(::pgrcolumnar::unsupported("TID range scans")),
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
        TableAm::Pgrcolumnar => std::mem::size_of::<ParallelBlockTableScanDescData>(),
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
        TableAm::Pgrcolumnar => cb::parallelscan_initialize(rel, &mut target.pscan),
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
        TableAm::Pgrcolumnar => cb::parallelscan_reinitialize(pscan),
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
            match &mut scan {
                TableScanDesc::Heap(h) => h.rs_temp_snapshot = registered,
                _ => unreachable!(),
            }
            Ok(scan)
        }
        TableAm::Pgrcolumnar => {
            let mut scan = cb::scan_begin(mcx, relation, snapshot, flags, Some(pscan_ptr))?;
            match &mut scan {
                TableScanDesc::Pgrcolumnar(c) => c.rs_temp_snapshot = registered,
                _ => unreachable!(),
            }
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
        TableAm::Pgrcolumnar => cb_refused("index scans"),
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
        TableAm::Pgrcolumnar => cb_refused("index scans"),
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
        TableAm::Pgrcolumnar => Err(::pgrcolumnar::unsupported("TID row fetches")),
    }
}

pub fn table_tuple_tid_valid(scan: &mut TableScanDesc<'_>, tid: &ItemPointerData) -> bool {
    match scan {
        TableScanDesc::Heap(h) => ::heapam_handler::heapam_tuple_tid_valid(h, tid),
        TableScanDesc::Pgrcolumnar(_) => false,
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
        TableScanDesc::Pgrcolumnar(_) => Err(::pgrcolumnar::unsupported("TID row fetches")),
    }
}

pub fn table_tuple_satisfies_snapshot<'mcx>(
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    snapshot: &Snapshot<'mcx>,
) -> PgResult<bool> {
    match am(rel) {
        TableAm::Heap => ::heapam_handler::heapam_tuple_satisfies_snapshot(rel, slot, snapshot),
        TableAm::Pgrcolumnar => Err(::pgrcolumnar::unsupported("tuple visibility rechecks")),
    }
}

// --- Manipulations of physical tuples (tableam.h wrappers) ---

thread_local! {
    static CB_EOXACT_REGISTERED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn cbstore_eoxact_callback(
    event: ::types_core::xact::XactEvent,
    _arg: ::datum::Datum,
) -> PgResult<()> {
    use ::types_core::xact::XactEvent::*;
    match event {
        XACT_EVENT_COMMIT | XACT_EVENT_PARALLEL_COMMIT | XACT_EVENT_ABORT
        | XACT_EVENT_PARALLEL_ABORT | XACT_EVENT_PREPARE => ::pgrcolumnar::at_eoxact(),
        XACT_EVENT_PRE_COMMIT | XACT_EVENT_PARALLEL_PRE_COMMIT | XACT_EVENT_PRE_PREPARE => {}
    }
    Ok(())
}

/// The cbstore ingest writer is buffered in backend TLS across the
/// statement (multi_insert opens it, finish_bulk_insert publishes it). A
/// statement that errors mid-ingest unwinds past its finish, so transaction
/// end must own the drop of whatever is left — C parity with copyfrom.c,
/// whose COPY state dies with the statement's memory contexts during abort
/// processing, never at backend exit. Registered once per backend thread on
/// the first cbstore insert (the xact callback registry is per-thread; the
/// postgres_fdw connection callback is the precedent).
fn ensure_cbstore_eoxact_registered() {
    CB_EOXACT_REGISTERED.with(|c| {
        if !c.get() {
            xact::RegisterXactCallback(cbstore_eoxact_callback, ::datum::Datum::null());
            c.set(true);
        }
    });
}

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
        TableAm::Pgrcolumnar => {
            let _ = (cid, options, bistate);
            ensure_cbstore_eoxact_registered();
            exectuples::exec_materialize_slot(slot, mcx)?;
            ::pgrcolumnar::tuple_insert(rel, slot)
        }
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
        TableAm::Pgrcolumnar => Err(::pgrcolumnar::unsupported("INSERT ... ON CONFLICT")),
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
        TableAm::Pgrcolumnar => Err(::pgrcolumnar::unsupported("INSERT ... ON CONFLICT")),
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
        TableAm::Pgrcolumnar => {
            let _ = (mcx, cid, options, bistate);
            ensure_cbstore_eoxact_registered();
            ::pgrcolumnar::multi_insert(rel, slots)
        }
    }
}

// heapam_methods leaves the optional finish_bulk_insert slot NULL, so for the
// only shipped AM the C inline is a no-op after the rd_tableam probe.
pub fn table_finish_bulk_insert(rel: &Relation<'_>, _options: i32) -> PgResult<()> {
    match am(rel) {
        TableAm::Heap => Ok(()),
        TableAm::Pgrcolumnar => ::pgrcolumnar::finish_bulk_insert(rel),
    }
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
        TableAm::Pgrcolumnar => Err(::pgrcolumnar::unsupported("DELETE")),
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
        TableAm::Pgrcolumnar => Err(::pgrcolumnar::unsupported("UPDATE")),
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
        TableAm::Pgrcolumnar => Err(::pgrcolumnar::unsupported("row locking")),
    }
}

// --- DDL-adjacent dispatch (tableam.h wrappers) ---

pub fn table_relation_set_new_filelocator(
    rel: &Relation<'_>,
    newrlocator: &RelFileLocator,
    relpersistence: i8,
) -> PgResult<(TransactionId, TransactionId)> {
    match am(rel) {
        // Storage create/reset is AM-independent (main fork file); pgrcolumnar
        // reuses the heap arm — an empty file is a valid empty part.
        TableAm::Heap | TableAm::Pgrcolumnar => {
            heap::relation_set_new_filelocator(rel, newrlocator, relpersistence)
        }
    }
}

pub fn table_relation_copy_data(rel: &Relation<'_>, newrlocator: &RelFileLocator) -> PgResult<()> {
    match am(rel) {
        TableAm::Heap => heap::relation_copy_data(rel, newrlocator),
        TableAm::Pgrcolumnar => Err(::pgrcolumnar::unsupported("CLUSTER / rewrites")),
    }
}

pub fn table_relation_nontransactional_truncate(rel: &Relation<'_>) -> PgResult<()> {
    match am(rel) {
        TableAm::Heap | TableAm::Pgrcolumnar => heap::relation_nontransactional_truncate(rel),
    }
}

pub fn table_relation_needs_toast_table(rel: &Relation<'_>) -> bool {
    match am(rel) {
        TableAm::Heap => heap::relation_needs_toast_table(rel),
        TableAm::Pgrcolumnar => false,
    }
}

pub fn table_relation_toast_am(rel: &Relation<'_>) -> ::types_core::Oid {
    match am(rel) {
        TableAm::Heap => heap::relation_toast_am(rel),
        TableAm::Pgrcolumnar => 0,
    }
}

pub fn table_relation_size(rel: &Relation<'_>, forkNumber: ForkNumber) -> PgResult<u64> {
    match am(rel) {
        TableAm::Heap | TableAm::Pgrcolumnar => heap::relation_size(rel, forkNumber),
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
        TableScanDesc::Pgrcolumnar(_) => Err(::pgrcolumnar::unsupported("ANALYZE")),
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
        TableScanDesc::Pgrcolumnar(_) => Err(::pgrcolumnar::unsupported("ANALYZE")),
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
        TableScanDesc::Pgrcolumnar(_) => cb_refused("bitmap scans"),
    }
}

pub fn table_scan_sample_next_block<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    scanstate: &mut dyn SampleScanDriver,
    donetuples: i64,
) -> PgResult<bool> {
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_scan_sample_next_block call during logical decoding",
        ));
    }
    match scan {
        TableScanDesc::Heap(h) => heap::scan_sample_next_block(mcx, h, scanstate, donetuples),
        TableScanDesc::Pgrcolumnar(_) => Err(::pgrcolumnar::unsupported("TABLESAMPLE")),
    }
}

pub fn table_scan_sample_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    scanstate: &mut dyn SampleScanDriver,
    donetuples: i64,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_scan_sample_next_tuple call during logical decoding",
        ));
    }
    match scan {
        TableScanDesc::Heap(h) => {
            heap::scan_sample_next_tuple(mcx, h, scanstate, donetuples, slot)
        }
        TableScanDesc::Pgrcolumnar(_) => Err(::pgrcolumnar::unsupported("TABLESAMPLE")),
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
    // pgrcolumnar: heap density math against the compressed body underestimates
    // rows ~5x; tuples = footer row count (pgrcolumnar-impl.md §7.2). allvisfrac
    // 0.0: no visibility map, and it only feeds index-only-scan costing,
    // which pgrcolumnar refuses. A footerless (never-loaded) table falls through
    // to C's never-analyzed convention below.
    if am(rel) == TableAm::Pgrcolumnar {
        if let Some(nrows) = ::pgrcolumnar::footer_rows(rel)? {
            *pages = relation_nblocks(rel)?;
            *tuples = nrows as f64;
            *allvisfrac = 0.0;
            return Ok(());
        }
    }
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
