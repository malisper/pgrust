//! rewriteheap.c: bulk table rewrite preserving visibility + ctid chains.
//! The logical-rewrite lane (wal_level=logical over a decodable catalog) is a
//! loud panic. State memory lives in the caller's statement mcx and dies at
//! statement end where C deletes rs_cxt eagerly (bounded by the rewrite's
//! unresolved-chain footprint, C's own worst case).
#![allow(non_snake_case)]

use heapam::freeze::heap_freeze_tuple;
use heapam::HeapTupleHeaderGetUpdateXid;
use heaptuple::{heap_copytuple, HeapTuple};
use mcx::{Mcx, PgFxHashMap};
use types_core::xact::TransactionIdPrecedes;
use types_core::{BlockNumber, ForkNumber, TransactionId};
use types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR};
use types_rel::{Relation, HEAP_DEFAULT_FILLFACTOR, RELKIND_TOASTVALUE};
use types_storage::bufpage::{MaxHeapTupleSize, PageMut, PAI_IS_HEAP};
use types_tuple::htup::{
    HeapTupleData, HeapTupleHeaderData, HEAP2_XACT_MASK, HEAP_HASEXTERNAL, HEAP_UPDATED,
    HEAP_XACT_MASK, HEAP_XMAX_INVALID,
};
use types_core::OffsetNumber;
use types_tuple::{ItemPointerData, ItemPointerIsValid};

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: rewriteheap.c {what}")
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct TidHashKey {
    xmin: TransactionId,
    tid: ItemPointerData,
}

struct UnresolvedTupData<'mcx> {
    old_tid: ItemPointerData,
    tuple: HeapTuple<'mcx>,
}

pub struct RewriteState<'mcx> {
    mcx: Mcx<'mcx>,
    rs_bulkstate: bulkwrite::BulkWriteState,
    rs_buffer: Option<bulkwrite::BulkWriteBuffer>,
    rs_blockno: BlockNumber,
    rs_oldest_xmin: TransactionId,
    rs_freeze_xid: TransactionId,
    rs_cutoff_multi: TransactionId,
    rs_old_frozenxid: TransactionId,
    rs_old_minmxid: TransactionId,
    rs_new_relkind: u8,
    rs_new_save_free_space: usize,
    rs_unresolved_tups: PgFxHashMap<'mcx, TidHashKey, UnresolvedTupData<'mcx>>,
    rs_old_new_tid_map: PgFxHashMap<'mcx, TidHashKey, ItemPointerData>,
}

pub fn begin_heap_rewrite<'mcx>(
    mcx: Mcx<'mcx>,
    old_heap: &Relation<'mcx>,
    new_heap: &Relation<'mcx>,
    oldest_xmin: TransactionId,
    freeze_xid: TransactionId,
    cutoff_multi: TransactionId,
) -> PgResult<RewriteState<'mcx>> {
    // logical_begin_heap_rewrite gate: RelationIsAccessibleInLogicalDecoding.
    // rd_options.is_some() over-approximates RelationIsUsedAsCatalogTable
    // (user_catalog_table reloption): loud beats a silent mapping-file skip.
    if transam_xlog::XLogLogicalInfoActive()
        && old_heap.is_permanent()
        && (catalog::IsCatalogRelation(old_heap) || old_heap.rd_options.is_some())
    {
        unported("logical_begin_heap_rewrite (logical decoding mapping files)");
    }

    Ok(RewriteState {
        mcx,
        rs_bulkstate: bulkwrite::smgr_bulk_start_rel(new_heap, ForkNumber::MAIN_FORKNUM)?,
        rs_buffer: None,
        rs_blockno: bufmgr::RelationGetNumberOfBlocksInFork(new_heap, ForkNumber::MAIN_FORKNUM)?,
        rs_oldest_xmin: oldest_xmin,
        rs_freeze_xid: freeze_xid,
        rs_cutoff_multi: cutoff_multi,
        rs_old_frozenxid: old_heap.rd_rel.relfrozenxid,
        rs_old_minmxid: old_heap.rd_rel.relminmxid,
        rs_new_relkind: new_heap.rd_rel.relkind,
        rs_new_save_free_space: new_heap.get_target_page_free_space(HEAP_DEFAULT_FILLFACTOR),
        rs_unresolved_tups: PgFxHashMap::with_hasher_in(Default::default(), mcx),
        rs_old_new_tid_map: PgFxHashMap::with_hasher_in(Default::default(), mcx),
    })
}

pub fn end_heap_rewrite<'mcx>(
    mut state: RewriteState<'mcx>,
    new_heap: &Relation<'mcx>,
) -> PgResult<()> {
    let keys: mcx::PgVec<'_, TidHashKey> = {
        let mut v = mcx::PgVec::new_in(state.mcx);
        v.extend(state.rs_unresolved_tups.keys().copied());
        v
    };
    for key in keys.iter() {
        let mut unresolved = state.rs_unresolved_tups.remove(key).unwrap();
        unresolved.tuple.as_tuple_mut().t_data_mut().t_ctid = ItemPointerData::invalid();
        let mut tup = unresolved.tuple;
        raw_heap_insert(&mut state, new_heap, tup.as_tuple_mut())?;
    }

    if let Some(buffer) = state.rs_buffer.take() {
        bulkwrite::smgr_bulk_write(&mut state.rs_bulkstate, state.rs_blockno, buffer, true)?;
    }
    bulkwrite::smgr_bulk_finish(state.rs_bulkstate)
}

pub fn rewrite_heap_tuple<'mcx>(
    state: &mut RewriteState<'mcx>,
    new_heap: &Relation<'mcx>,
    old_tuple: &HeapTupleData<'_>,
    new_tuple: &mut HeapTuple<'mcx>,
) -> PgResult<()> {
    {
        let old_hdr = old_tuple.t_data();
        let t_choice = old_hdr.t_choice;
        let old_infomask = old_hdr.t_infomask;
        let new_hdr = new_tuple.as_tuple_mut().t_data_mut();
        new_hdr.t_choice = t_choice;
        new_hdr.t_infomask &= !HEAP_XACT_MASK;
        new_hdr.t_infomask2 &= !HEAP2_XACT_MASK;
        new_hdr.t_infomask |= old_infomask & HEAP_XACT_MASK;

        heap_freeze_tuple(
            new_hdr,
            state.rs_old_frozenxid,
            state.rs_old_minmxid,
            state.rs_freeze_xid,
            state.rs_cutoff_multi,
        )?;

        new_hdr.t_ctid = ItemPointerData::invalid();
    }

    let old_hdr = old_tuple.t_data();
    let updated = !(old_hdr.t_infomask & HEAP_XMAX_INVALID != 0
        || heapam_visibility::HeapTupleHeaderIsOnlyLocked(old_hdr)?)
        && !old_hdr.indicates_moved_partitions()
        && !(old_tuple.t_self == old_hdr.t_ctid);

    if updated {
        let hashkey =
            TidHashKey { xmin: HeapTupleHeaderGetUpdateXid(old_hdr)?, tid: old_hdr.t_ctid };
        if let Some(new_tid) = state.rs_old_new_tid_map.remove(&hashkey) {
            new_tuple.as_tuple_mut().t_data_mut().t_ctid = new_tid;
        } else {
            let unresolved = UnresolvedTupData {
                old_tid: old_tuple.t_self,
                tuple: heap_copytuple(state.mcx, new_tuple.as_tuple())?,
            };
            let prev = state.rs_unresolved_tups.insert(hashkey, unresolved);
            debug_assert!(prev.is_none());
            return Ok(());
        }
    }

    let mut old_tid = old_tuple.t_self;
    let mut cur: Option<HeapTuple<'mcx>> = None;

    loop {
        {
            let tup = match cur.as_mut() {
                Some(t) => t.as_tuple_mut(),
                None => new_tuple.as_tuple_mut(),
            };
            raw_heap_insert(state, new_heap, tup)?;
        }
        let (new_tid, is_updated, xmin) = {
            let tup = match cur.as_ref() {
                Some(t) => t.as_tuple(),
                None => new_tuple.as_tuple(),
            };
            (tup.t_self, tup.t_data().t_infomask & HEAP_UPDATED != 0, tup.t_data().xmin())
        };

        if is_updated && !TransactionIdPrecedes(xmin, state.rs_oldest_xmin) {
            let hashkey = TidHashKey { xmin, tid: old_tid };
            if let Some(unresolved) = state.rs_unresolved_tups.remove(&hashkey) {
                let mut prev_tuple = unresolved.tuple;
                old_tid = unresolved.old_tid;
                prev_tuple.as_tuple_mut().t_data_mut().t_ctid = new_tid;
                cur = Some(prev_tuple);
                continue;
            }
            let prev = state.rs_old_new_tid_map.insert(hashkey, new_tid);
            debug_assert!(prev.is_none());
        }
        break;
    }
    Ok(())
}

pub fn rewrite_heap_dead_tuple(
    state: &mut RewriteState<'_>,
    old_tuple: &HeapTupleData<'_>,
) -> bool {
    let hashkey =
        TidHashKey { xmin: old_tuple.t_data().xmin(), tid: old_tuple.t_self };
    state.rs_unresolved_tups.remove(&hashkey).is_some()
}

fn raw_heap_insert<'mcx>(
    state: &mut RewriteState<'mcx>,
    new_heap: &Relation<'mcx>,
    tup: &mut HeapTupleData<'_>,
) -> PgResult<()> {
    let has_external = tup.t_data().t_infomask & HEAP_HASEXTERNAL != 0;
    let heaptup: Option<HeapTuple<'mcx>> = if state.rs_new_relkind == RELKIND_TOASTVALUE {
        debug_assert!(!has_external);
        None
    } else if has_external || tup.t_len as usize > heaptoast::TOAST_TUPLE_THRESHOLD {
        // XLOG FPI pages are not logically decoded; the toast writes must not
        // be either.
        let options = heapam::hio::HEAP_INSERT_SKIP_FSM | heapam::hio::HEAP_INSERT_NO_LOGICAL;
        heaptoast::heap_toast_insert_or_update(state.mcx, new_heap, tup, None, options)?
    } else {
        None
    };
    let (img_ptr, img_len) = match heaptup.as_ref() {
        Some(t) => (t.as_tuple().header_ptr(), t.as_tuple().t_len as usize),
        None => (tup.header_ptr(), tup.t_len as usize),
    };

    let len = transam_xlog::MAXALIGN(img_len);
    if len > MaxHeapTupleSize {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("row is too big: size {len}, maximum size {MaxHeapTupleSize}"),
            )
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        ));
    }

    if let Some(buffer) = state.rs_buffer.as_mut() {
        let page_free = page_mut_of(buffer).as_ref().heap_free_space();
        if len + state.rs_new_save_free_space > page_free {
            let buffer = state.rs_buffer.take().unwrap();
            bulkwrite::smgr_bulk_write(&mut state.rs_bulkstate, state.rs_blockno, buffer, true)?;
            state.rs_blockno += 1;
        }
    }

    if state.rs_buffer.is_none() {
        let mut buffer = bulkwrite::smgr_bulk_get_buf(&state.rs_bulkstate);
        page_mut_of(&mut buffer).init(0);
        state.rs_buffer = Some(buffer);
    }

    let buffer = state.rs_buffer.as_mut().unwrap();
    let mut page = page_mut_of(buffer);
    // SAFETY: img_ptr/img_len delimit a live tuple image (HeapTupleData invariant).
    let item = unsafe { core::slice::from_raw_parts(img_ptr, img_len) };
    let newoff: OffsetNumber =
        page.add_item(item, 0, PAI_IS_HEAP).unwrap_or_else(|| panic!("failed to add tuple"));

    tup.t_self = ItemPointerData::new(state.rs_blockno, newoff);

    if !ItemPointerIsValid(&tup.t_data().t_ctid) {
        let r = page.as_ref();
        let id = r.item_id(newoff);
        let (ptr, _) = r.item_raw(id);
        // SAFETY: freshly added heap tuple image on an exclusively owned build
        // page; t_ctid sits inside the fixed 23-byte header.
        unsafe {
            let onpage: *mut HeapTupleHeaderData = ptr.cast_mut().cast();
            (*onpage).t_ctid = tup.t_self;
        }
    }
    Ok(())
}

fn page_mut_of(buf: &mut bulkwrite::BulkWriteBuffer) -> PageMut<'_> {
    // SAFETY: exclusively owned, aligned build page.
    unsafe { PageMut::from_raw(core::ptr::NonNull::new_unchecked(buf.page_mut().as_mut_ptr())) }
}
