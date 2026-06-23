#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `backend-access-heap-rewriteheap` — a faithful port of
//! `src/backend/access/heap/rewriteheap.c` (PostgreSQL 18.3): the heap-rewrite
//! state machine `CLUSTER` / `VACUUM FULL` drive to rebuild a heap while
//! preserving visibility info and update-chain ctid links, plus the logical
//! decoding mapping support (`heap_xlog_logical_rewrite` replay and
//! `CheckPointLogicalRewriteHeap`).
//!
//! ## Owned model (vs the C-ABI)
//! C's opaque `RewriteState` is a `RewriteStateData *` allocated in a private
//! "Table rewrite" memory context; here it is [`RewriteStateData`] boxed into
//! the type-erased `'mcx`-bound carrier
//! [`rewriteheap_seams::RewriteState`] (mirroring
//! bulk_write's `BulkWriteState`). The unported consumers (`cluster.c`, the
//! HEAP2 replay dispatcher, the checkpointer) reach the public entry points
//! through that crate's seams, which `init_seams()` installs to the real fns.
//!
//! Tuples are carried as [`FormedTuple`] (owned header + user-data area); the
//! three C `HTAB`s become `std::collections::HashMap`s (the entries own copied
//! `FormedTuple`s, which a raw-pointer dynahash can't safely hold). The
//! page-build path uses the bulk-write smgr writer and the page support
//! routines directly; the logical-rewrite mapping files use the fd layer
//! directly.

extern crate alloc;

use std::collections::HashMap;

use ::mcx::Mcx;
use ::types_core::primitive::{
    BlockNumber, ForkNumber, MultiXactId, Oid, Size, XLogRecPtr, InvalidBlockNumber,
    InvalidOid,
};
use types_core::{FrozenTransactionId, InvalidTransactionId, TransactionId};
use types_error::{
    PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR,
};
use rel::{Relation, RelationData};
use ::types_storage::RelFileLocator;
use ::types_storage::bufpage::{MaxHeapTupleSize, MovedPartitionsOffsetNumber};
use ::types_tuple::heaptuple::FormedTuple;
use ::types_tuple::heaptuple::{
    HeapTupleHeaderChoice, HeapTupleHeaderData, ItemPointerData,
    HEAP2_XACT_MASK, HEAP_HASNULL, HEAP_HASEXTERNAL, HEAP_UPDATED, HEAP_XACT_MASK,
    HEAP_XMAX_INVALID, ON_PAGE_HEADER_SIZE,
};

use utils_error::{ereport, PgError};

use bulkwrite as bulkwrite;
use page::{
    PageAddItemExtended, PageGetHeapFreeSpace, PageGetItem, PageGetItemId, PageInit, PageMut,
    PageRef,
};

use heaptoast::{heap_toast_insert_or_update, TOAST_TUPLE_THRESHOLD};
use ::heapam::freeze::heap_freeze_tuple;
use ::heapam::insert::{HEAP_INSERT_NO_LOGICAL, HEAP_INSERT_SKIP_FSM};

use transam_xact_seams as xact_seam;
use transam_xlog_seams as xlog_seam;
use xloginsert_seams as xloginsert_seam;
use fd_seams as fd;
use procarray_seams as procarray_seam;
use slot_seams as slot_seam;
use init_small_seams as globals_seam;
use relcache_seams as relcache_seam;
use catalog_seams as catalog_seam;

use ::types_pgstat::wait_event::{
    WAIT_EVENT_LOGICAL_REWRITE_CHECKPOINT_SYNC, WAIT_EVENT_LOGICAL_REWRITE_MAPPING_SYNC,
    WAIT_EVENT_LOGICAL_REWRITE_MAPPING_WRITE, WAIT_EVENT_LOGICAL_REWRITE_SYNC,
    WAIT_EVENT_LOGICAL_REWRITE_TRUNCATE, WAIT_EVENT_LOGICAL_REWRITE_WRITE,
};

pub use rewriteheap_seams as seams;
use seams::{
    LogicalRewriteMappingData, RewriteMappingFile, RewriteState, RewriteStateData, TidHashKey,
    UnresolvedTup,
};

// ---------------------------------------------------------------------------
// rewriteheap-local vocabulary (constants / flag values).
// ---------------------------------------------------------------------------

/// `MAIN_FORKNUM` (`storage/relpath.h`).
const MAIN_FORKNUM: ForkNumber = ForkNumber::MAIN_FORKNUM;

/// `BLCKSZ` (`pg_config.h`).
const BLCKSZ: Size = ::types_core::BLCKSZ;

/// `RELKIND_TOASTVALUE` (`catalog/pg_class.h`).
const RELKIND_TOASTVALUE: u8 = ::types_tuple::access::RELKIND_TOASTVALUE;

/// `HEAP_DEFAULT_FILLFACTOR` (`utils/rel.h`).
const HEAP_DEFAULT_FILLFACTOR: i32 = 100;

/// `InvalidOffsetNumber` (`storage/off.h`).
const InvalidOffsetNumber: u16 = ::types_tuple::heaptuple::INVALID_OFFSET_NUMBER;

/// `PAI_IS_HEAP` (`storage/bufpage.h`) — `PageAddItem(... is_heap=true ...)`.
const PAI_IS_HEAP: i32 = ::types_storage::bufpage::PAI_IS_HEAP;

/// `RM_HEAP2_ID` / `XLOG_HEAP2_REWRITE`.
const RM_HEAP2_ID: ::types_core::RmgrId = wal::wal::RM_HEAP2_ID;
const XLOG_HEAP2_REWRITE: u8 = 0x00;

/// `LOGICAL_REWRITE_FORMAT` (`access/rewriteheap.h`):
/// `"map-%x-%x-%X_%X-%x-%x"`.
/// args: dboid, relid, lsn_hi, lsn_lo, mapped_xid, create_xid.
/// `PG_LOGICAL_MAPPINGS_DIR` (`replication/reorderbuffer.h`):
/// `"pg_logical/mappings"`.
const PG_LOGICAL_MAPPINGS_DIR: &str = "pg_logical/mappings";

/// POSIX `open(2)` flags. `PG_BINARY` is 0 on POSIX.
mod libc_flags {
    pub const O_WRONLY: i32 = 1;
    pub const O_RDWR: i32 = 2;
    pub const O_CREAT: i32 = 0o100;
    pub const O_EXCL: i32 = 0o200;
    pub const PG_BINARY: i32 = 0;
}

/// `ENOSPC`.
const ENOSPC: i32 = 28;

// ---------------------------------------------------------------------------
// LogicalRewriteMappingData (access/rewriteheap.h) — the on-disk mapping
// record; not present elsewhere in the repo, so defined here.
// ---------------------------------------------------------------------------

/// `sizeof(LogicalRewriteMappingData)` on disk: two `RelFileLocator`s (each
/// `spcOid` `dbOid` `relNumber` = 3 * 4 = 12 bytes) plus two `ItemPointerData`
/// (6 bytes each) = 12 + 12 + 6 + 6 = 36 bytes.
const SIZEOF_LOGICAL_REWRITE_MAPPING_DATA: usize = 36;

/// Serialize a [`LogicalRewriteMappingData`] to its 36 on-disk bytes
/// (native-endian, mirroring the C `memcpy` of the struct image).
fn logical_mapping_to_bytes(
    m: &LogicalRewriteMappingData,
) -> [u8; SIZEOF_LOGICAL_REWRITE_MAPPING_DATA] {
    let mut out = [0u8; SIZEOF_LOGICAL_REWRITE_MAPPING_DATA];
    let w = |out: &mut [u8], off: usize, loc: &RelFileLocator| {
        out[off..off + 4].copy_from_slice(&loc.spcOid.to_ne_bytes());
        out[off + 4..off + 8].copy_from_slice(&loc.dbOid.to_ne_bytes());
        out[off + 8..off + 12].copy_from_slice(&loc.relNumber.to_ne_bytes());
    };
    let wt = |out: &mut [u8], off: usize, tid: &ItemPointerData| {
        out[off..off + 2].copy_from_slice(&tid.ip_blkid.bi_hi.to_ne_bytes());
        out[off + 2..off + 4].copy_from_slice(&tid.ip_blkid.bi_lo.to_ne_bytes());
        out[off + 4..off + 6].copy_from_slice(&tid.ip_posid.to_ne_bytes());
    };
    w(&mut out, 0, &m.old_locator);
    w(&mut out, 12, &m.new_locator);
    wt(&mut out, 24, &m.old_tid);
    wt(&mut out, 30, &m.new_tid);
    out
}

/// `xl_heap_rewrite_mapping` (`access/heapam_xlog.h`): the WAL record header
/// for `XLOG_HEAP2_REWRITE`. Not present elsewhere, defined here.
#[derive(Clone, Copy, Debug)]
struct XlHeapRewriteMapping {
    mapped_xid: TransactionId,
    mapped_db: Oid,
    mapped_rel: Oid,
    offset: i64,
    num_mappings: u32,
    start_lsn: XLogRecPtr,
}

/// `sizeof(xl_heap_rewrite_mapping)`. Layout: `mapped_xid`(u32) `mapped_db`(u32)
/// `mapped_rel`(u32) `offset`(off_t = i64, 8-byte aligned → 4 pad bytes after
/// the three u32s) `num_mappings`(u32) `start_lsn`(u64, 8-byte aligned → 4 pad
/// after num_mappings). = 4*3 + 4pad + 8 + 4 + 4pad + 8 = 40 bytes.
const SIZEOF_XL_HEAP_REWRITE_MAPPING: usize = 40;

impl XlHeapRewriteMapping {
    fn to_bytes(&self) -> [u8; SIZEOF_XL_HEAP_REWRITE_MAPPING] {
        let mut out = [0u8; SIZEOF_XL_HEAP_REWRITE_MAPPING];
        out[0..4].copy_from_slice(&self.mapped_xid.to_ne_bytes());
        out[4..8].copy_from_slice(&self.mapped_db.to_ne_bytes());
        out[8..12].copy_from_slice(&self.mapped_rel.to_ne_bytes());
        // 4 bytes pad @12..16
        out[16..24].copy_from_slice(&self.offset.to_ne_bytes());
        out[24..28].copy_from_slice(&self.num_mappings.to_ne_bytes());
        // 4 bytes pad @28..32
        out[32..40].copy_from_slice(&self.start_lsn.to_ne_bytes());
        out
    }
}

// The hash-table key/entry structs (`TidHashKey` / `UnresolvedTup` /
// `RewriteMappingFile`) and the engine state (`RewriteStateData`) live in the
// seam crate (so the inward seam declarations can name them); imported above.

// ===========================================================================
// begin_heap_rewrite — start a table rewrite (rewriteheap.c).
// ===========================================================================

/// `begin_heap_rewrite(old_heap, new_heap, oldest_xmin, freeze_xid,
/// cutoff_multi)` (rewriteheap.c).
pub fn begin_heap_rewrite<'mcx>(
    mcx: Mcx<'mcx>,
    old_heap: &Relation<'mcx>,
    new_heap: &Relation<'mcx>,
    oldest_xmin: TransactionId,
    freeze_xid: TransactionId,
    cutoff_multi: MultiXactId,
) -> PgResult<RewriteState<'mcx>> {
    // C creates a private "Table rewrite" AllocSetContext; here we run in the
    // caller-supplied `mcx`. The state struct and all subsidiary data live in
    // it. (MemoryContextDelete at end_heap_rewrite is replaced by Drop.)

    // state->rs_blockno = RelationGetNumberOfBlocks(new_heap);
    let rs_blockno = RelationGetNumberOfBlocks(&new_heap)?;

    // state->rs_bulkstate = smgr_bulk_start_rel(new_heap, MAIN_FORKNUM);
    let rs_bulkstate = bulkwrite::smgr_bulk_start_rel(mcx, new_heap, MAIN_FORKNUM)?;

    // The engine holds aliased `Relation` handles (C holds `Relation` pointers);
    // the alias pins the relcache entry exactly as the C pointer alias does.
    let mut state = RewriteStateData {
        rs_old_rel: old_heap.alias(),
        rs_new_rel: new_heap.alias(),
        rs_bulkstate: Some(rs_bulkstate),
        rs_buffer: None,
        rs_blockno,
        rs_logical_rewrite: false,
        rs_oldest_xmin: oldest_xmin,
        rs_freeze_xid: freeze_xid,
        rs_logical_xmin: InvalidTransactionId,
        rs_cutoff_multi: cutoff_multi,
        rs_begin_lsn: 0,
        rs_unresolved_tups: HashMap::new(),
        rs_old_new_tid_map: HashMap::new(),
        rs_logical_mappings: HashMap::new(),
        rs_num_rewrite_mappings: 0,
        mcx,
    };

    // logical_begin_heap_rewrite(state);
    logical_begin_heap_rewrite(&mut state)?;

    // palloc the state in the "Table rewrite" context (here: `mcx`).
    ::mcx::alloc_in(mcx, state)
}

// ===========================================================================
// end_heap_rewrite — finish the rewrite (rewriteheap.c).
// ===========================================================================

/// `end_heap_rewrite(state)` (rewriteheap.c).
pub fn end_heap_rewrite<'mcx>(mut handle: RewriteState<'mcx>) -> PgResult<()> {
    let state = downcast_mut(&mut handle)?;

    // Write any remaining tuples in the UnresolvedTups table. If we have any
    // left, they should in fact be dead, but let's err on the safe side.
    //
    // C iterates the hashtable with hash_seq_search; we drain the map.
    let unresolved: Vec<UnresolvedTup<'mcx>> =
        state.rs_unresolved_tups.drain().map(|(_k, v)| v).collect();
    for u in unresolved {
        let mut tuple = u.tuple;
        // ItemPointerSetInvalid(&unresolved->tuple->t_data->t_ctid);
        item_pointer_set_invalid(&mut tuple_header_mut(&mut tuple)?.t_ctid);
        raw_heap_insert(state, tuple)?;
    }

    // Write the last page, if any.
    if let Some(buffer) = state.rs_buffer.take() {
        let bulkstate = state
            .rs_bulkstate
            .as_mut()
            .expect("end_heap_rewrite: bulk state present");
        bulkwrite::smgr_bulk_write(bulkstate, state.rs_blockno, buffer, true)?;
    }

    // smgr_bulk_finish(state->rs_bulkstate);
    let bulkstate = state
        .rs_bulkstate
        .take()
        .expect("end_heap_rewrite: bulk state present");
    bulkwrite::smgr_bulk_finish(bulkstate)?;

    // logical_end_heap_rewrite(state);
    logical_end_heap_rewrite(state)?;

    // MemoryContextDelete(state->rs_cxt) — the handle (and so the boxed
    // RewriteStateData with its maps/tuples) is dropped when `handle` falls out
    // of scope here.
    drop(handle);
    Ok(())
}

// ===========================================================================
// rewrite_heap_tuple — add a tuple to the new heap (rewriteheap.c).
// ===========================================================================

/// `rewrite_heap_tuple(state, old_tuple, new_tuple)` (rewriteheap.c).
///
/// `new_tuple` is consumed (C scribbles on it; it must be temp storage).
pub fn rewrite_heap_tuple<'mcx>(
    handle: &mut RewriteState<'mcx>,
    old_tuple: &FormedTuple<'mcx>,
    mut new_tuple: FormedTuple<'mcx>,
) -> PgResult<()> {
    let state = downcast_mut(handle)?;

    // old_cxt = MemoryContextSwitchTo(state->rs_cxt) — no ambient context; all
    // allocations are explicitly in `state.mcx`.

    let old_hdr_choice;
    let old_infomask;
    {
        let old_h = tuple_header_ref(old_tuple)?;
        old_hdr_choice = old_h.t_choice.clone();
        old_infomask = old_h.t_infomask;
    }

    // Copy the original tuple's visibility information into new_tuple.
    // memcpy(&new->t_data->t_choice.t_heap, &old->t_data->t_choice.t_heap, ...);
    // XXX intentionally clears the HOT status bits.
    {
        let nh = tuple_header_mut(&mut new_tuple)?;
        nh.t_choice = old_hdr_choice;

        // new->t_infomask &= ~HEAP_XACT_MASK;
        nh.t_infomask &= !HEAP_XACT_MASK;
        // new->t_infomask2 &= ~HEAP2_XACT_MASK;
        nh.t_infomask2 &= !HEAP2_XACT_MASK;
        // new->t_infomask |= old->t_infomask & HEAP_XACT_MASK;
        nh.t_infomask |= old_infomask & HEAP_XACT_MASK;
    }

    // While we have our hands on the tuple, freeze any eligible xmin/xmax.
    // heap_freeze_tuple(new->t_data, old_rel->relfrozenxid, old_rel->relminmxid,
    //                   rs_freeze_xid, rs_cutoff_multi);
    let relfrozenxid = state.rs_old_rel.rd_rel.relfrozenxid;
    let relminmxid = state.rs_old_rel.rd_rel.relminmxid;
    let freeze_xid = state.rs_freeze_xid;
    let cutoff_multi = state.rs_cutoff_multi;
    {
        let nh = tuple_header_mut(&mut new_tuple)?;
        heap_freeze_tuple(state.mcx, nh, relfrozenxid, relminmxid, freeze_xid, cutoff_multi)?;
    }

    // Invalid ctid means ctid should point to the tuple itself.
    // ItemPointerSetInvalid(&new->t_data->t_ctid);
    item_pointer_set_invalid(&mut tuple_header_mut(&mut new_tuple)?.t_ctid);

    // If the tuple has been updated, check the old-to-new mapping hash table.
    //
    // if (!((old->t_infomask & HEAP_XMAX_INVALID) ||
    //       HeapTupleHeaderIsOnlyLocked(old->t_data)) &&
    //     !HeapTupleHeaderIndicatesMovedPartitions(old->t_data) &&
    //     !(ItemPointerEquals(&old->t_self, &old->t_data->t_ctid)))
    let old_updated = {
        let old_h = tuple_header_ref(old_tuple)?;
        let xmax_invalid_or_only_locked =
            (old_h.t_infomask & HEAP_XMAX_INVALID) != 0 || HeapTupleHeaderIsOnlyLocked(old_h)?;
        let moved_partitions = HeapTupleHeaderIndicatesMovedPartitions(old_h);
        let ctid_self = ItemPointerEquals(&old_tuple.tuple.t_self, &old_h.t_ctid);
        !xmax_invalid_or_only_locked && !moved_partitions && !ctid_self
    };

    if old_updated {
        // hashkey.xmin = HeapTupleHeaderGetUpdateXid(old->t_data);
        // hashkey.tid = old->t_data->t_ctid;
        let hashkey = {
            let old_h = tuple_header_ref(old_tuple)?;
            TidHashKey {
                xmin: HeapTupleHeaderGetUpdateXid(old_h)?,
                tid: old_h.t_ctid,
            }
        };

        // mapping = hash_search(rs_old_new_tid_map, &hashkey, HASH_FIND, NULL);
        if let Some(new_tid) = state.rs_old_new_tid_map.get(&hashkey).copied() {
            // We've already copied the tuple t_ctid points to: set the ctid of
            // this tuple to the new location and insert it right away.
            // new->t_data->t_ctid = mapping->new_tid;
            tuple_header_mut(&mut new_tuple)?.t_ctid = new_tid;

            // hash_search(..., HASH_REMOVE, &found); Assert(found);
            let removed = state.rs_old_new_tid_map.remove(&hashkey);
            debug_assert!(removed.is_some());
        } else {
            // We haven't seen the tuple t_ctid points to yet. Stash this tuple
            // into unresolved_tups to be written later.
            //
            // unresolved = hash_search(rs_unresolved_tups, &hashkey, HASH_ENTER,
            //                          &found); Assert(!found);
            // unresolved->old_tid = old->t_self;
            // unresolved->tuple = heap_copytuple(new_tuple);
            let old_tid = old_tuple.tuple.t_self;
            let copy = new_tuple.clone_in(state.mcx)?;
            let prev = state.rs_unresolved_tups.insert(
                hashkey,
                UnresolvedTup {
                    old_tid,
                    tuple: copy,
                },
            );
            debug_assert!(prev.is_none());

            // We can't do anything more now; return. (new_tuple is dropped — C
            // leaves the caller's temp storage alone; we own ours.)
            return Ok(());
        }
    }

    // Now write the tuple, then check if it is the B tuple in any pair. The
    // resolution can cascade, so we loop.
    //
    // old_tid = old->t_self; free_new = false;
    let mut old_tid = old_tuple.tuple.t_self;
    // `cur` is the tuple we are currently inserting (C's `new_tuple`, which it
    // re-points to an unresolved entry's tuple as the chain resolves).
    let mut cur = new_tuple;

    loop {
        // raw_heap_insert(state, new_tuple); new_tid = new_tuple->t_self;
        // raw_heap_insert sets cur.tuple.t_self; capture the inserted TID after.
        let new_tid = raw_heap_insert(state, cur)?;

        // We consumed `cur` into the page image; but the chain logic needs the
        // header fields (HEAP_UPDATED / xmin) of the just-inserted tuple and
        // the value to log. raw_heap_insert returns the inserted tuple's
        // metadata we need (its t_self, infomask, xmin) via `Inserted`.
        let inserted = new_tid;

        // logical_rewrite_heap_tuple(state, old_tid, new_tuple);
        logical_rewrite_heap_tuple(state, old_tid, &inserted)?;

        // if ((new->t_infomask & HEAP_UPDATED) &&
        //     !TransactionIdPrecedes(HeapTupleHeaderGetXmin(new->t_data),
        //                            rs_oldest_xmin))
        if (inserted.infomask & HEAP_UPDATED) != 0
            && !TransactionIdPrecedes(inserted.xmin, state.rs_oldest_xmin)
        {
            // This is B in an update pair. See if we've seen A.
            // hashkey.xmin = HeapTupleHeaderGetXmin(new->t_data);
            // hashkey.tid = old_tid;
            let hashkey = TidHashKey {
                xmin: inserted.xmin,
                tid: old_tid,
            };

            // unresolved = hash_search(rs_unresolved_tups, &hashkey, HASH_FIND, NULL);
            if let Some(mut entry) = state.rs_unresolved_tups.remove(&hashkey) {
                // We have seen and memorized the previous tuple. Now that we
                // know where we inserted the tuple its t_ctid points to, fix
                // its t_ctid and insert it.
                //
                // new_tuple = unresolved->tuple; old_tid = unresolved->old_tid;
                // new_tuple->t_data->t_ctid = new_tid;
                tuple_header_mut(&mut entry.tuple)?.t_ctid = inserted.new_tid;
                old_tid = entry.old_tid;
                cur = entry.tuple;
                // Assert(found) — remove already succeeded above.
                // loop back to insert the previous tuple in the chain.
                continue;
            } else {
                // Remember the new tid of this tuple, to set the ctid when we
                // find the previous tuple in the chain.
                //
                // mapping = hash_search(rs_old_new_tid_map, &hashkey, HASH_ENTER,
                //                       &found); Assert(!found);
                // mapping->new_tid = new_tid;
                let prev = state.rs_old_new_tid_map.insert(hashkey, inserted.new_tid);
                debug_assert!(prev.is_none());
            }
        }

        // Done with this (chain of) tuples, for now.
        break;
    }

    Ok(())
}

// ===========================================================================
// rewrite_heap_dead_tuple — register a dead tuple (rewriteheap.c).
// ===========================================================================

/// `rewrite_heap_dead_tuple(state, old_tuple)` (rewriteheap.c). Returns true if
/// a tuple was removed from the unresolved_tups table.
pub fn rewrite_heap_dead_tuple<'mcx>(
    handle: &mut RewriteState<'mcx>,
    old_tuple: &FormedTuple<'mcx>,
) -> PgResult<bool> {
    let state = downcast_mut(handle)?;

    // hashkey.xmin = HeapTupleHeaderGetXmin(old->t_data);
    // hashkey.tid = old->t_self;
    let hashkey = {
        let old_h = tuple_header_ref(old_tuple)?;
        TidHashKey {
            xmin: HeapTupleHeaderGetXmin(old_h),
            tid: old_tuple.tuple.t_self,
        }
    };

    // unresolved = hash_search(rs_unresolved_tups, &hashkey, HASH_FIND, NULL);
    // if (unresolved != NULL) { heap_freetuple(unresolved->tuple);
    //     hash_search(..., HASH_REMOVE, &found); Assert(found); return true; }
    if state.rs_unresolved_tups.remove(&hashkey).is_some() {
        // The contained tuple is freed when the removed entry drops.
        return Ok(true);
    }

    Ok(false)
}

// ===========================================================================
// raw_heap_insert — insert a tuple into the new relation (rewriteheap.c).
// ===========================================================================

/// Metadata of the tuple as actually stored on the page, returned by
/// [`raw_heap_insert`] (C reads these back off the caller's `tup` after the
/// insert: `tup->t_self`, and the header words that `rewrite_heap_tuple`'s chain
/// logic and `logical_rewrite_heap_tuple` consult).
struct Inserted {
    /// `tup->t_self` after the insert (the new TID).
    new_tid: ItemPointerData,
    /// `HeapTupleHeaderGetXmin(tup->t_data)`.
    xmin: TransactionId,
    /// `tup->t_data->t_infomask`.
    infomask: u16,
    /// `HeapTupleHeaderGetUpdateXid(tup->t_data)`.
    update_xid: TransactionId,
    /// `HEAP_XMAX_IS_LOCKED_ONLY(tup->t_data->t_infomask)`.
    xmax_locked_only: bool,
}

/// `raw_heap_insert(state, tup)` (rewriteheap.c). Consumes `tup` (the on-page
/// image is built from it). Returns the stored-tuple metadata
/// `rewrite_heap_tuple` reads back off the caller's `tup` in C.
fn raw_heap_insert<'mcx>(
    state: &mut RewriteStateData<'mcx>,
    mut tup: FormedTuple<'mcx>,
) -> PgResult<Inserted> {
    // If the new tuple is too big for storage or contains already toasted
    // out-of-line attributes from some other relation, invoke the toaster.
    //
    // Note: heaptup is the data we actually store; tup is the caller's
    // original untoasted data.
    let relkind = state.rs_new_rel.rd_rel.relkind as u8;
    let mut toasted: Option<FormedTuple<'mcx>> = None;
    if relkind == RELKIND_TOASTVALUE {
        // toast table entries should never be recursively toasted.
        debug_assert!(!HeapTupleHasExternal(&tup));
        // heaptup = tup;
    } else if HeapTupleHasExternal(&tup) || (tup.tuple.t_len as usize) > TOAST_TUPLE_THRESHOLD {
        // options = HEAP_INSERT_SKIP_FSM | HEAP_INSERT_NO_LOGICAL;
        let options = HEAP_INSERT_SKIP_FSM | HEAP_INSERT_NO_LOGICAL;
        // heaptup = heap_toast_insert_or_update(rs_new_rel, tup, NULL, options);
        toasted = heap_toast_insert_or_update(state.mcx, &state.rs_new_rel, &tup, None, options)?;
    } else {
        // heaptup = tup;
    }

    // `heaptup` is the toasted copy when present, else the caller's tup.
    let heaptup_is_copy = toasted.is_some();
    let heaptup: &mut FormedTuple<'mcx> = match toasted.as_mut() {
        Some(t) => t,
        None => &mut tup,
    };

    // len = MAXALIGN(heaptup->t_len);
    let len = maxalign(heaptup.tuple.t_len as usize);

    // If we're gonna fail for oversize tuple, do it right away.
    if len > MaxHeapTupleSize {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!(
                "row is too big: size {}, maximum size {}",
                len, MaxHeapTupleSize
            ))
            .into_error());
    }

    // saveFreeSpace = RelationGetTargetPageFreeSpace(rs_new_rel,
    //                                                HEAP_DEFAULT_FILLFACTOR);
    let save_free_space = RelationGetTargetPageFreeSpace(&state.rs_new_rel, HEAP_DEFAULT_FILLFACTOR);

    // page = (Page) state->rs_buffer;  if (page) { ... }
    if state.rs_buffer.is_some() {
        let page_free_space = {
            let buf = state.rs_buffer.as_ref().unwrap();
            let page = PageRef::new(buf.as_slice())?;
            PageGetHeapFreeSpace(&page)
        };

        // if (len + saveFreeSpace > pageFreeSpace) { write out the page; ... }
        if len + save_free_space > page_free_space {
            let buffer = state.rs_buffer.take().unwrap();
            let bulkstate = state
                .rs_bulkstate
                .as_mut()
                .expect("raw_heap_insert: bulk state present");
            bulkwrite::smgr_bulk_write(bulkstate, state.rs_blockno, buffer, true)?;
            // state->rs_blockno++;
            state.rs_blockno += 1;
        }
    }

    // if (!page) { rs_buffer = smgr_bulk_get_buf(...); PageInit(page, BLCKSZ, 0); }
    if state.rs_buffer.is_none() {
        let mut buf = {
            let bulkstate = state
                .rs_bulkstate
                .as_mut()
                .expect("raw_heap_insert: bulk state present");
            bulkwrite::smgr_bulk_get_buf(state.mcx, bulkstate)?
        };
        PageInit(buf.as_mut_slice(), BLCKSZ, 0)?;
        state.rs_buffer = Some(buf);
    }

    // Build the full on-page item image of heaptup and add it to the page.
    // newoff = PageAddItem(page, (Item) heaptup->t_data, heaptup->t_len,
    //                      InvalidOffsetNumber, false, true);
    let item = formed_tuple_to_on_page_image(heaptup)?;

    let newoff = {
        let buf = state.rs_buffer.as_mut().unwrap();
        let mut page = PageMut::new(buf.as_mut_slice())?;
        // PageAddItem(... offsetNumber=InvalidOffsetNumber, overwrite=false,
        //             is_heap=true) => flags = PAI_IS_HEAP.
        PageAddItemExtended(&mut page, &item, InvalidOffsetNumber, PAI_IS_HEAP)?
    };

    // if (newoff == InvalidOffsetNumber) elog(ERROR, "failed to add tuple");
    if newoff == InvalidOffsetNumber {
        return Err(PgError::error("failed to add tuple"));
    }

    // Update caller's t_self to the actual position where it was stored.
    // ItemPointerSet(&tup->t_self, state->rs_blockno, newoff);
    heaptup.tuple.t_self = ItemPointerData::new(state.rs_blockno, newoff);

    // Insert the correct position into CTID of the stored tuple, too, if the
    // caller didn't supply a valid CTID. C reads the on-page header back and
    // sets onpage_tup->t_ctid = tup->t_self. We mirror by re-stamping the
    // on-page item bytes' ctid when the in-memory ctid is invalid.
    let stored_self = heaptup.tuple.t_self;
    if !item_pointer_is_valid(&tuple_header_ref(heaptup)?.t_ctid) {
        // newitemid = PageGetItemId(page, newoff);
        // onpage_tup = (HeapTupleHeader) PageGetItem(page, newitemid);
        // onpage_tup->t_ctid = tup->t_self;
        let buf = state.rs_buffer.as_mut().unwrap();
        // Compute the item slice's offset within the page, then write the ctid
        // bytes (offsets 12..18 of the fixed header) in place.
        let (item_off, item_len) = {
            let page = PageRef::new(buf.as_slice())?;
            let iid = PageGetItemId(&page, newoff)?;
            let data = PageGetItem(&page, &iid)?;
            // Position of `data` within the page buffer.
            let base = buf.as_slice().as_ptr() as usize;
            let off = data.as_ptr() as usize - base;
            (off, data.len())
        };
        debug_assert!(item_len >= ON_PAGE_HEADER_SIZE);
        let bytes = buf.as_mut_slice();
        // t_ctid = ip_blkid(bi_hi@12, bi_lo@14) + ip_posid@16.
        bytes[item_off + 12..item_off + 14].copy_from_slice(&stored_self.ip_blkid.bi_hi.to_ne_bytes());
        bytes[item_off + 14..item_off + 16].copy_from_slice(&stored_self.ip_blkid.bi_lo.to_ne_bytes());
        bytes[item_off + 16..item_off + 18].copy_from_slice(&stored_self.ip_posid.to_ne_bytes());
    }

    // Capture the metadata the caller reads back off the inserted tuple. In C,
    // `tup->t_self` is updated even when a private toasted copy was stored
    // (the position is the same), and the chain logic / logical-rewrite reads
    // the header words of `new_tuple` (== the caller's `tup`).
    let inserted = {
        let h = tuple_header_ref(heaptup)?;
        Inserted {
            new_tid: stored_self,
            xmin: HeapTupleHeaderGetXmin(h),
            infomask: h.t_infomask,
            update_xid: HeapTupleHeaderGetUpdateXid(h)?,
            xmax_locked_only: HEAP_XMAX_IS_LOCKED_ONLY(h.t_infomask),
        }
    };

    // If heaptup is a private copy, release it (drop). The caller's `tup`
    // (untoasted) is also dropped here — we own it (C keeps it; the caller is
    // done with it after rewrite_heap_tuple returns).
    let _ = heaptup_is_copy;
    drop(toasted);
    drop(tup);

    Ok(inserted)
}

// ===========================================================================
// Logical rewrite support (rewriteheap.c).
// ===========================================================================

/// `logical_begin_heap_rewrite(state)` (rewriteheap.c).
fn logical_begin_heap_rewrite<'mcx>(state: &mut RewriteStateData<'mcx>) -> PgResult<()> {
    // state->rs_logical_rewrite =
    //     RelationIsAccessibleInLogicalDecoding(state->rs_old_rel);
    state.rs_logical_rewrite = relation_is_accessible_in_logical_decoding(&state.rs_old_rel);

    if !state.rs_logical_rewrite {
        return Ok(());
    }

    // ProcArrayGetReplicationSlotXmin(NULL, &logical_xmin);
    let (_xmin, logical_xmin) = procarray_seam::proc_array_get_replication_slot_xmin::call();

    // If there are no logical slots in progress we don't need to do anything.
    if logical_xmin == InvalidTransactionId {
        state.rs_logical_rewrite = false;
        return Ok(());
    }

    state.rs_logical_xmin = logical_xmin;
    // state->rs_begin_lsn = GetXLogInsertRecPtr();
    state.rs_begin_lsn = xlog_seam::get_xlog_insert_rec_ptr::call();
    state.rs_num_rewrite_mappings = 0;

    // The mapping HTAB starts empty; the HashMap already exists empty.
    state.rs_logical_mappings.clear();
    Ok(())
}

/// `logical_heap_rewrite_flush_mappings(state)` (rewriteheap.c): flush all
/// in-memory mappings to disk (don't fsync yet).
fn logical_heap_rewrite_flush_mappings<'mcx>(state: &mut RewriteStateData<'mcx>) -> PgResult<()> {
    debug_assert!(state.rs_logical_rewrite);

    // no logical rewrite in progress, no need to iterate over mappings.
    if state.rs_num_rewrite_mappings == 0 {
        return Ok(());
    }

    // elog(DEBUG1, "flushing %u logical rewrite mapping entries", ...);
    // DEBUG1 diagnostic; no observable effect.

    // Precompute the shared header bits.
    let dboid = if state.rs_old_rel.rd_rel.relisshared {
        InvalidOid
    } else {
        globals_seam::my_database_id::call()
    };
    let mapped_rel = RelationGetRelid(&state.rs_old_rel);
    let begin_lsn = state.rs_begin_lsn;

    // Iterate over every mapping file (C hash_seq_search). Collect the xids so
    // we can mutate per-file state inside the loop without aliasing the map.
    let xids: Vec<TransactionId> = state.rs_logical_mappings.keys().copied().collect();

    for xid in xids {
        // num_mappings = dclist_count(&src->mappings);
        let num_mappings = state
            .rs_logical_mappings
            .get(&xid)
            .map(|f| f.mappings.len())
            .unwrap_or(0);

        // this file hasn't got any new mappings.
        if num_mappings == 0 {
            continue;
        }

        // len = num_mappings * sizeof(LogicalRewriteMappingData);
        let len = num_mappings * SIZEOF_LOGICAL_REWRITE_MAPPING_DATA;

        // Collect data we need to write out, but don't modify ondisk data yet.
        // waldata holds all mappings consecutively; we drain the list (C
        // dclist_foreach_modify + dclist_delete_from + pfree).
        let mut waldata: Vec<u8> = Vec::with_capacity(len);
        {
            let src = state.rs_logical_mappings.get_mut(&xid).unwrap();
            for pmap in src.mappings.drain(..) {
                waldata.extend_from_slice(&logical_mapping_to_bytes(&pmap));
                // state->rs_num_rewrite_mappings--;
                state.rs_num_rewrite_mappings = state.rs_num_rewrite_mappings.saturating_sub(1);
            }
            debug_assert!(src.mappings.is_empty());
        }
        debug_assert_eq!(waldata.len(), len);

        // Build the xlog record header.
        let xlrec = XlHeapRewriteMapping {
            num_mappings: num_mappings as u32,
            mapped_rel,
            mapped_xid: xid,
            mapped_db: dboid,
            offset: state.rs_logical_mappings.get(&xid).unwrap().off,
            start_lsn: begin_lsn,
        };

        // Note: we deviate from usual WAL coding here, see the module comment.
        // written = FileWrite(src->vfd, waldata_start, len, src->off, WAIT_*);
        let (vfd, off, path) = {
            let src = state.rs_logical_mappings.get(&xid).unwrap();
            (src.vfd, src.off, src.path.clone())
        };
        let written = ::fd::vfd_io::FileWriteV(
            vfd,
            &[std::io::IoSlice::new(&waldata)],
            off,
            WAIT_EVENT_LOGICAL_REWRITE_WRITE,
        )?;
        if written != len as isize {
            return Err(ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not write to file \"{}\", wrote {} of {}",
                    path, written, len
                ))
                .into_error());
        }
        // src->off += len;
        state.rs_logical_mappings.get_mut(&xid).unwrap().off += len as i64;

        // XLogBeginInsert(); XLogRegisterData(&xlrec, sizeof(xlrec));
        // XLogRegisterData(waldata_start, len);
        // XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_REWRITE);
        xloginsert_seam::xlog_begin_insert::call()?;
        xloginsert_seam::xlog_register_data::call(&xlrec.to_bytes())?;
        xloginsert_seam::xlog_register_data::call(&waldata)?;
        xloginsert_seam::xlog_insert_record::call(RM_HEAP2_ID, XLOG_HEAP2_REWRITE)?;
    }

    debug_assert_eq!(state.rs_num_rewrite_mappings, 0);
    Ok(())
}

/// `logical_end_heap_rewrite(state)` (rewriteheap.c).
fn logical_end_heap_rewrite<'mcx>(state: &mut RewriteStateData<'mcx>) -> PgResult<()> {
    // done, no logical rewrite in progress.
    if !state.rs_logical_rewrite {
        return Ok(());
    }

    // writeout remaining in-memory entries.
    if state.rs_num_rewrite_mappings > 0 {
        logical_heap_rewrite_flush_mappings(state)?;
    }

    // Iterate over all mappings we have written and fsync the files.
    let entries: Vec<(::types_storage::file::File, String)> = state
        .rs_logical_mappings
        .values()
        .map(|f| (f.vfd, f.path.clone()))
        .collect();
    for (vfd, path) in entries {
        // if (FileSync(src->vfd, WAIT_*) != 0) ereport(data_sync_elevel(ERROR), ...);
        // FileSync returns Err on failure (carrying data_sync_elevel(ERROR)).
        if let Err(_e) = ::fd::vfd_io::FileSync(vfd, WAIT_EVENT_LOGICAL_REWRITE_SYNC) {
            return Err(ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("could not fsync file \"{}\"", path))
                .into_error());
        }
        // FileClose(src->vfd);
        ::fd::vfd_io::FileClose(vfd)?;
    }
    // memory context cleanup will deal with the rest (Drop).
    Ok(())
}

/// `logical_rewrite_log_mapping(state, xid, map)` (rewriteheap.c).
fn logical_rewrite_log_mapping<'mcx>(
    state: &mut RewriteStateData<'mcx>,
    xid: TransactionId,
    map: &LogicalRewriteMappingData,
) -> PgResult<()> {
    let relid = RelationGetRelid(&state.rs_old_rel);

    // src = hash_search(rs_logical_mappings, &xid, HASH_ENTER, &found);
    // If !found, create per-xid data structures.
    if !state.rs_logical_mappings.contains_key(&xid) {
        let dboid = if state.rs_old_rel.rd_rel.relisshared {
            InvalidOid
        } else {
            globals_seam::my_database_id::call()
        };

        // snprintf(path, ..., "%s/" LOGICAL_REWRITE_FORMAT, PG_LOGICAL_MAPPINGS_DIR,
        //          dboid, relid, LSN_FORMAT_ARGS(rs_begin_lsn), xid,
        //          GetCurrentTransactionId());
        // LOGICAL_REWRITE_FORMAT = "map-%x-%x-%X_%X-%x-%x".
        let create_xid = xact_seam::get_current_transaction_id::call()?;
        let lsn_hi = (state.rs_begin_lsn >> 32) as u32;
        let lsn_lo = state.rs_begin_lsn as u32;
        let path = format!(
            "{}/map-{:x}-{:x}-{:X}_{:X}-{:x}-{:x}",
            PG_LOGICAL_MAPPINGS_DIR, dboid, relid, lsn_hi, lsn_lo, xid, create_xid
        );

        // src->vfd = PathNameOpenFile(path, O_CREAT|O_EXCL|O_WRONLY|PG_BINARY);
        let vfd = ::fd::vfd_io::PathNameOpenFile(
            &path,
            libc_flags::O_CREAT | libc_flags::O_EXCL | libc_flags::O_WRONLY | libc_flags::PG_BINARY,
        )?;
        // if (src->vfd < 0) ereport(ERROR, ...);
        if vfd.0 < 0 {
            return Err(ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("could not create file \"{}\"", path))
                .into_error());
        }

        state.rs_logical_mappings.insert(
            xid,
            RewriteMappingFile {
                vfd,
                off: 0,
                mappings: Vec::new(),
                path,
            },
        );
    }

    // pmap = MemoryContextAlloc(rs_cxt, sizeof(RewriteMappingDataEntry));
    // memcpy(&pmap->map, map, ...); dclist_push_tail(&src->mappings, &pmap->node);
    state
        .rs_logical_mappings
        .get_mut(&xid)
        .unwrap()
        .mappings
        .push(*map);
    // state->rs_num_rewrite_mappings++;
    state.rs_num_rewrite_mappings += 1;

    // Write out buffer every time we've too many in-memory entries.
    if state.rs_num_rewrite_mappings >= 1000 {
        logical_heap_rewrite_flush_mappings(state)?;
    }
    Ok(())
}

/// `logical_rewrite_heap_tuple(state, old_tid, new_tuple)` (rewriteheap.c).
///
/// In the owned port `new_tuple`'s relevant header words travel in the
/// [`Inserted`] metadata that `raw_heap_insert` returned for the just-stored
/// tuple (C reads them off `new_tuple->t_data`).
fn logical_rewrite_heap_tuple<'mcx>(
    state: &mut RewriteStateData<'mcx>,
    old_tid: ItemPointerData,
    inserted: &Inserted,
) -> PgResult<()> {
    let new_tid = inserted.new_tid;
    let cutoff = state.rs_logical_xmin;

    // no logical rewrite in progress, we don't need to log anything.
    if !state.rs_logical_rewrite {
        return Ok(());
    }

    // xmin = HeapTupleHeaderGetXmin(new->t_data);
    let xmin = inserted.xmin;
    // xmax = HeapTupleHeaderGetUpdateXid(new->t_data);
    let xmax = inserted.update_xid;
    let mut do_log_xmin = false;
    let mut do_log_xmax = false;

    // Log the mapping iff the tuple has been created recently.
    if TransactionIdIsNormal(xmin) && !TransactionIdPrecedes(xmin, cutoff) {
        do_log_xmin = true;
    }

    if !TransactionIdIsNormal(xmax) {
        // no xmax set, can't have any permanent ones, so this is sufficient.
    } else if inserted.xmax_locked_only {
        // only locked, we don't care.
    } else if !TransactionIdPrecedes(xmax, cutoff) {
        // tuple has been deleted recently, log.
        do_log_xmax = true;
    }

    // if neither needs to be logged, we're done.
    if !do_log_xmin && !do_log_xmax {
        return Ok(());
    }

    // fill out mapping information.
    let map = LogicalRewriteMappingData {
        old_locator: state.rs_old_rel.rd_locator,
        old_tid,
        new_locator: state.rs_new_rel.rd_locator,
        new_tid,
    };

    // Persist for the individual xids affected; log both xmin and xmax if they
    // differ.
    if do_log_xmin {
        logical_rewrite_log_mapping(state, xmin, &map)?;
    }
    if do_log_xmax && !TransactionIdEquals(xmin, xmax) {
        logical_rewrite_log_mapping(state, xmax, &map)?;
    }
    Ok(())
}

// ===========================================================================
// heap_xlog_logical_rewrite — replay XLOG_HEAP2_REWRITE (rewriteheap.c).
// ===========================================================================

/// `heap_xlog_logical_rewrite(r)` (rewriteheap.c). The decoded
/// `xl_heap_rewrite_mapping` header fields are passed explicitly (the
/// XLogReaderState decode lives with the unported replay dispatcher); `data` is
/// the trailing mapping-array payload.
#[allow(clippy::too_many_arguments)]
pub fn heap_xlog_logical_rewrite(
    mapped_xid: TransactionId,
    mapped_db: Oid,
    mapped_rel: Oid,
    offset: i64,
    num_mappings: u32,
    start_lsn: XLogRecPtr,
    record_xid: TransactionId,
    data: &[u8],
) -> PgResult<()> {
    // snprintf(path, ..., "%s/" LOGICAL_REWRITE_FORMAT, PG_LOGICAL_MAPPINGS_DIR,
    //          xlrec->mapped_db, xlrec->mapped_rel, LSN_FORMAT_ARGS(start_lsn),
    //          xlrec->mapped_xid, XLogRecGetXid(r));
    let lsn_hi = (start_lsn >> 32) as u32;
    let lsn_lo = start_lsn as u32;
    let path = format!(
        "{}/map-{:x}-{:x}-{:X}_{:X}-{:x}-{:x}",
        PG_LOGICAL_MAPPINGS_DIR, mapped_db, mapped_rel, lsn_hi, lsn_lo, mapped_xid, record_xid
    );

    // fd = OpenTransientFile(path, O_CREAT | O_WRONLY | PG_BINARY);
    let fd_no = fd::open_transient_file::call(
        &path,
        libc_flags::O_CREAT | libc_flags::O_WRONLY | libc_flags::PG_BINARY,
    );
    if fd_no < 0 {
        return Err(ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not create file \"{}\"", path))
            .into_error());
    }

    // Truncate all data that's not guaranteed to have been safely fsynced.
    // pgstat_report_wait_start(WAIT_EVENT_LOGICAL_REWRITE_TRUNCATE);
    // if (ftruncate(fd, xlrec->offset) != 0) ereport(ERROR, ...);
    // (the truncate wait-event is accounted inside the transient-fd primitive.)
    let _ = WAIT_EVENT_LOGICAL_REWRITE_TRUNCATE;
    if fd::pg_ftruncate_transient::call(fd_no, offset) != 0 {
        let _ = fd::close_transient_file::call(fd_no);
        return Err(ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!(
                "could not truncate file \"{}\" to {}",
                path, offset as u32
            ))
            .into_error());
    }

    // data = XLogRecGetData(r) + sizeof(*xlrec);  (the caller already sliced it)
    // len = xlrec->num_mappings * sizeof(LogicalRewriteMappingData);
    let len = num_mappings as usize * SIZEOF_LOGICAL_REWRITE_MAPPING_DATA;

    // write out tail end of mapping file (again).
    // pgstat_report_wait_start(WAIT_EVENT_LOGICAL_REWRITE_MAPPING_WRITE);
    // if (pg_pwrite(fd, data, len, xlrec->offset) != len) { ENOSPC; ereport(ERROR); }
    let _ = WAIT_EVENT_LOGICAL_REWRITE_MAPPING_WRITE;
    let to_write = &data[..len.min(data.len())];
    let written = fd::pg_pwrite_transient::call(fd_no, to_write, offset);
    if written != len as isize {
        // if write didn't set errno, assume problem is no disk space.
        let _ = ENOSPC;
        let _ = fd::close_transient_file::call(fd_no);
        return Err(ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not write to file \"{}\"", path))
            .into_error());
    }

    // fsync all previously written data.
    // pgstat_report_wait_start(WAIT_EVENT_LOGICAL_REWRITE_MAPPING_SYNC);
    // if (pg_fsync(fd) != 0) ereport(data_sync_elevel(ERROR), ...);
    let _ = WAIT_EVENT_LOGICAL_REWRITE_MAPPING_SYNC;
    if fd::pg_fsync::call(fd_no) != 0 {
        let _ = fd::close_transient_file::call(fd_no);
        return Err(ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not fsync file \"{}\"", path))
            .into_error());
    }

    // if (CloseTransientFile(fd) != 0) ereport(ERROR, ...);
    if fd::close_transient_file::call(fd_no) != 0 {
        return Err(ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{}\"", path))
            .into_error());
    }
    Ok(())
}

// ===========================================================================
// CheckPointLogicalRewriteHeap — checkpoint cleanup/flush (rewriteheap.c).
// ===========================================================================

/// `PGFILETYPE_ERROR` / `PGFILETYPE_REG` (`common/file_utils.h`), as returned
/// by `get_dirent_type` (0 / 2).
const PGFILETYPE_ERROR: i32 = 0;
const PGFILETYPE_REG: i32 = 2;

/// `CheckPointLogicalRewriteHeap()` (rewriteheap.c).
pub fn CheckPointLogicalRewriteHeap() -> PgResult<()> {
    // We start with a minimum of the last redo pointer. redo = GetRedoRecPtr();
    let redo = xlog_seam::get_redo_rec_ptr::call();

    // cutoff = ReplicationSlotsComputeLogicalRestartLSN();
    let mut cutoff = slot_seam::replication_slots_compute_logical_restart_lsn::call()?;

    // don't start earlier than the restart lsn.
    // if (cutoff != InvalidXLogRecPtr && redo < cutoff) cutoff = redo;
    let invalid_lsn: XLogRecPtr = 0;
    if cutoff != invalid_lsn && redo < cutoff {
        cutoff = redo;
    }

    // mappings_dir = AllocateDir(PG_LOGICAL_MAPPINGS_DIR);
    // while ((mapping_de = ReadDir(mappings_dir, ...)) != NULL) { ... }
    // Use the directory-walk seam (list_dir-style precedent) via the fd-seams
    // read_dir_names helper, which returns every entry name in the directory.
    let names = fd::read_dir_names::call(PG_LOGICAL_MAPPINGS_DIR)?;

    for d_name in names {
        // if (strcmp(".") == 0 || strcmp("..") == 0) continue;
        if d_name == "." || d_name == ".." {
            continue;
        }

        // snprintf(path, ..., "%s/%s", PG_LOGICAL_MAPPINGS_DIR, d_name);
        let path = format!("{}/{}", PG_LOGICAL_MAPPINGS_DIR, d_name);
        // de_type = get_dirent_type(path, mapping_de, false, DEBUG1);
        let de_type = fd::get_dirent_type::call(&path);

        // if (de_type != PGFILETYPE_ERROR && de_type != PGFILETYPE_REG) continue;
        if de_type != PGFILETYPE_ERROR && de_type != PGFILETYPE_REG {
            continue;
        }

        // Skip over files that cannot be ours.
        // if (strncmp(d_name, "map-", 4) != 0) continue;
        if !d_name.starts_with("map-") {
            continue;
        }

        // if (sscanf(d_name, LOGICAL_REWRITE_FORMAT, &dboid, &relid, &hi, &lo,
        //            &rewrite_xid, &create_xid) != 6)
        //     elog(ERROR, "could not parse filename \"%s\"", d_name);
        // LOGICAL_REWRITE_FORMAT = "map-%x-%x-%X_%X-%x-%x".
        let (hi, lo) = match parse_mapping_filename(&d_name) {
            Some((_dboid, _relid, hi, lo, _rewrite_xid, _create_xid)) => (hi, lo),
            None => {
                return Err(PgError::error(format!(
                    "could not parse filename \"{}\"",
                    d_name
                )));
            }
        };

        // lsn = ((uint64) hi) << 32 | lo;
        let lsn: XLogRecPtr = ((hi as u64) << 32) | (lo as u64);

        // if (lsn < cutoff || cutoff == InvalidXLogRecPtr) { unlink; }
        if lsn < cutoff || cutoff == invalid_lsn {
            // elog(DEBUG1, "removing logical rewrite file \"%s\"", path);
            // if (unlink(path) < 0) ereport(ERROR, ...);
            if fd::unlink_file::call(&path) < 0 {
                return Err(ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(format!("could not remove file \"{}\"", path))
                    .into_error());
            }
        } else {
            // on some operating systems fsyncing a file requires O_RDWR.
            // fd = OpenTransientFile(path, O_RDWR | PG_BINARY);
            let fd_no =
                fd::open_transient_file::call(&path, libc_flags::O_RDWR | libc_flags::PG_BINARY);
            // The file cannot vanish (only this fn removes logical mappings,
            // one checkpoint at a time). if (fd < 0) ereport(ERROR, ...);
            if fd_no < 0 {
                return Err(ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(format!("could not open file \"{}\"", path))
                    .into_error());
            }

            // pgstat_report_wait_start(WAIT_EVENT_LOGICAL_REWRITE_CHECKPOINT_SYNC);
            // if (pg_fsync(fd) != 0) ereport(data_sync_elevel(ERROR), ...);
            let _ = WAIT_EVENT_LOGICAL_REWRITE_CHECKPOINT_SYNC;
            if fd::pg_fsync::call(fd_no) != 0 {
                let _ = fd::close_transient_file::call(fd_no);
                return Err(ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(format!("could not fsync file \"{}\"", path))
                    .into_error());
            }

            // if (CloseTransientFile(fd) != 0) ereport(ERROR, ...);
            if fd::close_transient_file::call(fd_no) != 0 {
                return Err(ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(format!("could not close file \"{}\"", path))
                    .into_error());
            }
        }
    }
    // FreeDir(mappings_dir) — read_dir_names drains+frees the dir internally.

    // persist directory entries to disk.
    // fsync_fname(PG_LOGICAL_MAPPINGS_DIR, true);
    fd::fsync_fname::call(PG_LOGICAL_MAPPINGS_DIR, true)?;
    Ok(())
}

/// Parse `"map-%x-%x-%X_%X-%x-%x"` (the C `sscanf` of `LOGICAL_REWRITE_FORMAT`)
/// into `(dboid, relid, hi, lo, rewrite_xid, create_xid)`. Returns `None` if
/// fewer than 6 fields parse (C `!= 6`).
fn parse_mapping_filename(name: &str) -> Option<(u32, u32, u32, u32, u32, u32)> {
    // map-<dboid>-<relid>-<hi>_<lo>-<rewrite_xid>-<create_xid>
    let rest = name.strip_prefix("map-")?;
    // Split: dboid '-' relid '-' hi '_' lo '-' rewrite_xid '-' create_xid.
    let mut dash = rest.splitn(3, '-');
    let dboid = u32::from_str_radix(dash.next()?, 16).ok()?;
    let relid = u32::from_str_radix(dash.next()?, 16).ok()?;
    let tail = dash.next()?; // "<hi>_<lo>-<rewrite_xid>-<create_xid>"

    // tail: "<hi>_<lo>-<rewrite_xid>-<create_xid>".
    let (lsn_part, xid_part) = tail.split_once('-')?;
    let (hi_s, lo_s) = lsn_part.split_once('_')?;
    let hi = u32::from_str_radix(hi_s, 16).ok()?;
    let lo = u32::from_str_radix(lo_s, 16).ok()?;

    let (rewrite_s, create_s) = xid_part.split_once('-')?;
    let rewrite_xid = u32::from_str_radix(rewrite_s, 16).ok()?;
    let create_xid = u32::from_str_radix(create_s, 16).ok()?;

    Some((dboid, relid, hi, lo, rewrite_xid, create_xid))
}

// ===========================================================================
// Small helpers (htup_details.h / utils/rel.h macros + transam.c).
// ===========================================================================

/// `HeapTupleHasExternal(tuple)` — `(t_infomask & HEAP_HASEXTERNAL) != 0`.
fn HeapTupleHasExternal(tup: &FormedTuple<'_>) -> bool {
    tup.tuple
        .t_data
        .as_ref()
        .is_some_and(|hdr| (hdr.t_infomask & HEAP_HASEXTERNAL) != 0)
}

/// `HeapTupleHeaderIndicatesMovedPartitions(tup)` (htup_details.h) —
/// `ItemPointerIndicatesMovedPartitions(&tup->t_ctid)`: the ctid offset is the
/// moved-partitions sentinel and the block is `MovedPartitionsBlockNumber`.
fn HeapTupleHeaderIndicatesMovedPartitions(hdr: &HeapTupleHeaderData<'_>) -> bool {
    use ::types_storage::bufpage::MovedPartitionsBlockNumber;
    hdr.t_ctid.ip_posid == MovedPartitionsOffsetNumber
        && hdr.t_ctid.ip_blkid.block_number() == MovedPartitionsBlockNumber
}

/// `ItemPointerEquals(p1, p2)` (itemptr.c).
fn ItemPointerEquals(p1: &ItemPointerData, p2: &ItemPointerData) -> bool {
    p1.ip_blkid.block_number() == p2.ip_blkid.block_number() && p1.ip_posid == p2.ip_posid
}

/// `ItemPointerSetInvalid(pointer)` (itemptr.h).
fn item_pointer_set_invalid(pointer: &mut ItemPointerData) {
    *pointer = ItemPointerData::new(InvalidBlockNumber, InvalidOffsetNumber);
}

/// `ItemPointerIsValid(pointer)` (itemptr.h): offset is not the invalid sentinel.
fn item_pointer_is_valid(pointer: &ItemPointerData) -> bool {
    pointer.ip_posid != InvalidOffsetNumber
}

/// `RelationGetRelid(relation)` — `relation->rd_id`.
fn RelationGetRelid(relation: &RelationData<'_>) -> Oid {
    relation.rd_id
}

/// `RelationGetNumberOfBlocks(rel)` (`storage/bufmgr.h`):
/// `smgrnblocks({rel->rd_locator, rel->rd_backend}, MAIN_FORKNUM)`.
fn RelationGetNumberOfBlocks(rel: &RelationData<'_>) -> PgResult<BlockNumber> {
    let key = ::types_storage::RelFileLocatorBackend {
        locator: rel.rd_locator,
        backend: rel.rd_backend,
    };
    smgr::smgrnblocks(key, MAIN_FORKNUM)
}

/// `RelationGetTargetPageFreeSpace(rel, defaultff)` (`utils/rel.h`):
/// `BLCKSZ * (100 - fillfactor) / 100`.
fn RelationGetTargetPageFreeSpace(rel: &RelationData<'_>, defaultff: i32) -> Size {
    let ff = rel.get_fillfactor(defaultff);
    (BLCKSZ * (100 - ff as usize)) / 100
}

/// `MAXALIGN(LEN)` — round up to `MAXIMUM_ALIGNOF` (8).
fn maxalign(len: usize) -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (len.wrapping_add(MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `RelationIsAccessibleInLogicalDecoding(relation)` (`utils/rel.h`):
/// `XLogLogicalInfoActive() && RelationNeedsWAL(relation) &&
/// (IsCatalogRelation(relation) || RelationIsUsedAsCatalogTable(relation))`.
/// (Copied from `heapam`'s `insert.rs`.)
fn relation_is_accessible_in_logical_decoding(relation: &RelationData<'_>) -> bool {
    let wal = xlog_seam::wal_level::call();
    let xlog_logical_info_active = wal >= wal::WalLevel::Logical;
    let used_as_catalog_table = relation_is_used_as_catalog_table(relation);
    xlog_logical_info_active
        && relcache_seam::relation_needs_wal::call(relation)
        && (catalog_seam::is_catalog_relation::call(relation) || used_as_catalog_table)
}

/// `RelationIsUsedAsCatalogTable(relation)` (`utils/rel.h`).
fn relation_is_used_as_catalog_table(relation: &RelationData<'_>) -> bool {
    use ::types_tuple::access::{RELKIND_RELATION};
    const RELKIND_MATVIEW: u8 = b'm';
    let relkind = relation.rd_rel.relkind as u8;
    (relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW)
        && relation
            .rd_options
            .as_ref()
            .and_then(|o| o.std())
            .is_some_and(|o| o.user_catalog_table)
}

/// `TransactionIdEquals(id1, id2)` (transam.h).
fn TransactionIdEquals(id1: TransactionId, id2: TransactionId) -> bool {
    id1 == id2
}

/// `TransactionIdIsNormal(xid)` (transam.h): `xid >= FirstNormalTransactionId`.
fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    const FirstNormalTransactionId: TransactionId = 3;
    xid >= FirstNormalTransactionId
}

/// `TransactionIdPrecedes(id1, id2)` (transam.c): modulo-2^32 comparison.
fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }
    (id1.wrapping_sub(id2) as i32) < 0
}

/// `HeapTupleHeaderGetXmin(tup)` (htup_details.h): resolve a frozen xmin to
/// `FrozenTransactionId`, else the raw xmin.
fn HeapTupleHeaderGetXmin(hdr: &HeapTupleHeaderData<'_>) -> TransactionId {
    if HeapTupleHeaderXminFrozen(hdr) {
        FrozenTransactionId
    } else {
        HeapTupleHeaderGetRawXmin(hdr)
    }
}

/// `HeapTupleHeaderXminFrozen(tup)`: `(t_infomask & HEAP_XMIN_FROZEN) ==
/// HEAP_XMIN_FROZEN`.
fn HeapTupleHeaderXminFrozen(hdr: &HeapTupleHeaderData<'_>) -> bool {
    use ::types_tuple::heaptuple::HEAP_XMIN_FROZEN;
    (hdr.t_infomask & HEAP_XMIN_FROZEN) == HEAP_XMIN_FROZEN
}

/// `HeapTupleHeaderGetRawXmin(tup)`: the THeap arm's `t_xmin`.
fn HeapTupleHeaderGetRawXmin(hdr: &HeapTupleHeaderData<'_>) -> TransactionId {
    match &hdr.t_choice {
        HeapTupleHeaderChoice::THeap(f) => f.t_xmin,
        HeapTupleHeaderChoice::TDatum(_) => InvalidTransactionId,
    }
}

// The remaining header predicates (`HeapTupleHeaderIsOnlyLocked`,
// `HeapTupleHeaderGetUpdateXid`, `HEAP_XMAX_IS_LOCKED_ONLY`) come from the
// visibility crate (re-exported below).
use heapam_visibility::{
    HeapTupleHeaderGetUpdateXid, HeapTupleHeaderIsOnlyLocked,
};
use ::heapam_visibility::htup::HEAP_XMAX_IS_LOCKED_ONLY;

// ---------------------------------------------------------------------------
// FormedTuple <-> on-page byte image (own logic, the inverse of
// FormedTuple::read_on_page_full).
// ---------------------------------------------------------------------------

/// Build the full on-page item byte image of a `FormedTuple` — the bytes C
/// stores via `PageAddItem(page, (Item) heaptup->t_data, heaptup->t_len, ...)`.
///
/// Mirrors `HeapTupleHeaderData::read_on_page_full` exactly (its inverse):
/// allocate `t_len` bytes, write the 23-byte fixed header into `[0..23]`, copy
/// the header's `t_bits` null-bitmap bytes into `[23..t_hoff]` when
/// `HEAP_HASNULL`, then copy `formed.data` into `[t_hoff..t_len]`.
fn formed_tuple_to_on_page_image(formed: &FormedTuple<'_>) -> PgResult<Vec<u8>> {
    let hdr = tuple_header_ref(formed)?;
    let t_len = formed.tuple.t_len as usize;
    let t_hoff = hdr.t_hoff as usize;

    if t_hoff < ON_PAGE_HEADER_SIZE || t_hoff > t_len {
        return Err(PgError::error(
            "rewriteheap: tuple t_hoff out of range building on-page image",
        ));
    }

    let mut item = vec![0u8; t_len];

    // Fixed 23-byte header.
    hdr.write_on_page(&mut item[0..ON_PAGE_HEADER_SIZE])?;

    // Null bitmap (the bytes between the fixed header and t_hoff), if HASNULL.
    if (hdr.t_infomask & HEAP_HASNULL) != 0 {
        let bits_len = t_hoff - ON_PAGE_HEADER_SIZE;
        let n = bits_len.min(hdr.t_bits.len());
        item[ON_PAGE_HEADER_SIZE..ON_PAGE_HEADER_SIZE + n].copy_from_slice(&hdr.t_bits[..n]);
        // Any remaining bytes up to t_hoff stay zero (alignment padding).
    }

    // User-data area at [t_hoff..t_len].
    let data_len = t_len - t_hoff;
    let n = data_len.min(formed.data.len());
    item[t_hoff..t_hoff + n].copy_from_slice(&formed.data[..n]);

    Ok(item)
}

// ---------------------------------------------------------------------------
// Carrier downcast + field-access helpers.
// ---------------------------------------------------------------------------

fn downcast_mut<'a, 'mcx>(
    handle: &'a mut RewriteState<'mcx>,
) -> PgResult<&'a mut RewriteStateData<'mcx>> {
    // `RewriteState<'mcx>` is `PgBox<'mcx, RewriteStateData<'mcx>>`; deref to the
    // boxed state directly (no type erasure).
    Ok(&mut **handle)
}

fn tuple_header_ref<'a>(t: &'a FormedTuple<'_>) -> PgResult<&'a HeapTupleHeaderData<'a>> {
    t.tuple
        .t_data
        .as_deref()
        .ok_or_else(|| PgError::error("rewriteheap: tuple has no t_data header"))
}

fn tuple_header_mut<'a, 'mcx>(
    t: &'a mut FormedTuple<'mcx>,
) -> PgResult<&'a mut HeapTupleHeaderData<'mcx>> {
    t.tuple
        .t_data
        .as_deref_mut()
        .ok_or_else(|| PgError::error("rewriteheap: tuple has no t_data header"))
}

// ===========================================================================
// init_seams() — install the inward entry-point seams.
// ===========================================================================

/// Install every seam this unit OWNS (`backend-access-heap-rewriteheap-seams`).
pub fn init_seams() {
    seams::begin_heap_rewrite::set(begin_heap_rewrite);
    seams::rewrite_heap_tuple::set(rewrite_heap_tuple);
    seams::rewrite_heap_dead_tuple::set(rewrite_heap_dead_tuple);
    seams::end_heap_rewrite::set(end_heap_rewrite);
    seams::heap_xlog_logical_rewrite::set(heap_xlog_logical_rewrite);
    seams::check_point_logical_rewrite_heap::set(CheckPointLogicalRewriteHeap);
}
