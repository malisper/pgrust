//! rewriteheap.c: bulk table rewrite preserving visibility + ctid chains,
//! including the logical-rewrite lane (mapping files under
//! pg_logical/mappings so decoding can follow rewritten catalog tuples).
//! State memory lives in the caller's statement mcx and dies at statement
//! end where C deletes rs_cxt eagerly (bounded by the rewrite's
//! unresolved-chain footprint, C's own worst case). The logical mapping
//! buffers hold plain Rust resources (Files) so an error unwind closes the
//! fds where C leans on vfd/resowner cleanup.
#![allow(non_snake_case)]

use std::io::Write;
use std::path::PathBuf;

use elog::ereport;
use heapam::freeze::heap_freeze_tuple;
use heapam::HeapTupleHeaderGetUpdateXid;
use heaptuple::{heap_copytuple, HeapTuple};
use mcx::{Mcx, PgFxHashMap};
use types_core::xact::{TransactionIdIsNormal, TransactionIdPrecedes};
use types_core::{BlockNumber, ForkNumber, Oid, TransactionId, XLogRecPtr};
use types_error::{
    ErrorLocation, PgError, PgResult, DEBUG1, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR,
};
use types_rel::{Relation, HEAP_DEFAULT_FILLFACTOR, RELKIND_TOASTVALUE};
use types_storage::bufpage::{MaxHeapTupleSize, PageMut, PAI_IS_HEAP};
use types_storage::RelFileLocator;
use types_tuple::htup::{
    HeapTupleData, HeapTupleHeaderData, HEAP2_XACT_MASK, HEAP_HASEXTERNAL, HEAP_UPDATED,
    HEAP_XACT_MASK, HEAP_XMAX_INVALID,
};
use types_core::OffsetNumber;
use types_tuple::{ItemPointerData, ItemPointerIsValid};

// reorderbuffer.h: PG_LOGICAL_DIR "/mappings".
const PG_LOGICAL_MAPPINGS_DIR: &str = "pg_logical/mappings";
// sizeof(LogicalRewriteMappingData): 2x RelFileLocator + 2x ItemPointerData.
const LOGICAL_REWRITE_MAPPING_SIZE: usize = 36;
// sizeof(xl_heap_rewrite_mapping) with C padding: xid(4) db(4) rel(4) pad(4)
// offset(8) num_mappings(4) pad(4) start_lsn(8).
const XL_HEAP_REWRITE_MAPPING_SIZE: usize = 40;
const XLOG_HEAP2_REWRITE: u8 = 0x00;
const RM_HEAP2_ID: u8 = types_core::RmgrIds::RM_HEAP2_ID as u8;

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/access/heap/rewriteheap.c", line, func)
}

fn mappings_dir() -> PathBuf {
    let datadir = init_small::globals::DataDir().expect("rewriteheap: DataDir unset");
    PathBuf::from(datadir).join(PG_LOGICAL_MAPPINGS_DIR)
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

// RewriteMappingFile (rewriteheap.c:186): per-"mapped" xid mapping file with
// its buffered, not-yet-flushed mapping entries.
struct RewriteMappingFile {
    off: u64,
    path: PathBuf,
    file: std::fs::File,
    mappings: Vec<[u8; LOGICAL_REWRITE_MAPPING_SIZE]>,
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
    // C NewHeap->rd_toastoid: valid only for the CLUSTER/VACUUM FULL rewrite
    // when both heaps have toast tables (copy_table_data's choice).
    rs_toastoid: types_core::Oid,
    rs_new_save_free_space: usize,
    rs_unresolved_tups: PgFxHashMap<'mcx, TidHashKey, UnresolvedTupData<'mcx>>,
    rs_old_new_tid_map: PgFxHashMap<'mcx, TidHashKey, ItemPointerData>,
    // --- logical rewrite support (rewriteheap.c "Logical rewrite support") ---
    rs_logical_rewrite: bool,
    rs_logical_xmin: TransactionId,
    rs_begin_lsn: XLogRecPtr,
    rs_num_rewrite_mappings: u32,
    rs_old_locator: RelFileLocator,
    rs_new_locator: RelFileLocator,
    rs_old_relid: Oid,
    // MyDatabaseId, or InvalidOid for shared catalogs (mapping-file naming
    // and the xl_heap_rewrite_mapping.mapped_db field).
    rs_mapped_db: Oid,
    // std HashMap on purpose: the values own Files; Drop closes them on any
    // unwind (C leans on vfd/resowner cleanup for the same guarantee).
    rs_logical_mappings: std::collections::HashMap<TransactionId, RewriteMappingFile>,
}

pub fn begin_heap_rewrite<'mcx>(
    mcx: Mcx<'mcx>,
    old_heap: &Relation<'mcx>,
    new_heap: &Relation<'mcx>,
    oldest_xmin: TransactionId,
    freeze_xid: TransactionId,
    cutoff_multi: TransactionId,
    toastoid: types_core::Oid,
) -> PgResult<RewriteState<'mcx>> {
    let mut state = RewriteState {
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
        rs_toastoid: toastoid,
        rs_new_save_free_space: new_heap.get_target_page_free_space(HEAP_DEFAULT_FILLFACTOR),
        rs_unresolved_tups: PgFxHashMap::with_hasher_in(Default::default(), mcx),
        rs_old_new_tid_map: PgFxHashMap::with_hasher_in(Default::default(), mcx),
        rs_logical_rewrite: false,
        rs_logical_xmin: types_core::InvalidTransactionId,
        rs_begin_lsn: types_core::InvalidXLogRecPtr,
        rs_num_rewrite_mappings: 0,
        rs_old_locator: old_heap.rd_locator.get(),
        rs_new_locator: new_heap.rd_locator.get(),
        rs_old_relid: old_heap.rd_id,
        rs_mapped_db: if old_heap.rd_rel.relisshared {
            types_core::InvalidOid
        } else {
            init_small::globals::MyDatabaseId()
        },
        rs_logical_mappings: std::collections::HashMap::new(),
    };

    logical_begin_heap_rewrite(&mut state, old_heap)?;

    Ok(state)
}

// logical_begin_heap_rewrite (rewriteheap.c:759): prepare mapping-file
// logging if the rewritten table can be accessed during logical decoding
// and any decoding slot holds a catalog xmin.
fn logical_begin_heap_rewrite(state: &mut RewriteState<'_>, old_heap: &Relation<'_>) -> PgResult<()> {
    state.rs_logical_rewrite = heapam::relation_is_accessible_in_logical_decoding(old_heap);
    if !state.rs_logical_rewrite {
        return Ok(());
    }

    let (_slot_xmin, logical_xmin) = procarray::ProcArrayGetReplicationSlotXmin()?;

    // No logical slots in progress: there cannot be any remappings for
    // relevant rows yet. The relation's lock protects us against races.
    if logical_xmin == types_core::InvalidTransactionId {
        state.rs_logical_rewrite = false;
        return Ok(());
    }

    state.rs_logical_xmin = logical_xmin;
    state.rs_begin_lsn = transam_xlog::GetXLogInsertRecPtr();
    state.rs_num_rewrite_mappings = 0;
    Ok(())
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

    // C runs smgr_bulk_finish first, then logical_end_heap_rewrite; the two
    // have no ordering dependency (mapping durability is anchored to the
    // XLOG_HEAP2_REWRITE inserts and the per-file fsyncs, not the heap
    // sync), and bulk-finish consumes the state here.
    logical_end_heap_rewrite(&mut state)?;

    bulkwrite::smgr_bulk_finish(state.rs_bulkstate)
}

// logical_heap_rewrite_flush_mappings (rewriteheap.c:807): write the buffered
// mappings to their files (no fsync yet) and WAL-log each batch. The file
// write happens BEFORE XLogInsert on purpose — the mapping files are not in
// shared_buffers, so the usual buffer-lock/checkpoint interlock does not
// apply; see the C "Logical rewrite support" comment.
fn logical_heap_rewrite_flush_mappings(state: &mut RewriteState<'_>) -> PgResult<()> {
    debug_assert!(state.rs_logical_rewrite);

    if state.rs_num_rewrite_mappings == 0 {
        return Ok(());
    }
    let _ = elog::elog(
        DEBUG1,
        format!("flushing {} logical rewrite mapping entries", state.rs_num_rewrite_mappings),
    );

    let mapped_db = state.rs_mapped_db;
    let mapped_rel = state.rs_old_relid;
    let start_lsn = state.rs_begin_lsn;
    for (&xid, src) in state.rs_logical_mappings.iter_mut() {
        let num_mappings = src.mappings.len() as u32;
        if num_mappings == 0 {
            continue;
        }

        let len = num_mappings as usize * LOGICAL_REWRITE_MAPPING_SIZE;
        let mut waldata: Vec<u8> = Vec::with_capacity(len);
        for m in src.mappings.drain(..) {
            waldata.extend_from_slice(&m);
        }
        state.rs_num_rewrite_mappings -= num_mappings;

        // xl_heap_rewrite_mapping, C struct layout (incl. alignment holes).
        let mut xlrec = [0u8; XL_HEAP_REWRITE_MAPPING_SIZE];
        xlrec[0..4].copy_from_slice(&xid.to_ne_bytes());
        xlrec[4..8].copy_from_slice(&mapped_db.to_ne_bytes());
        xlrec[8..12].copy_from_slice(&mapped_rel.to_ne_bytes());
        xlrec[16..24].copy_from_slice(&(src.off as i64).to_ne_bytes());
        xlrec[24..28].copy_from_slice(&num_mappings.to_ne_bytes());
        xlrec[32..40].copy_from_slice(&start_lsn.to_ne_bytes());

        if let Err(e) = src.file.write_all(&waldata) {
            return ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not write to file \"{}\", wrote {} of {}: {e}",
                    src.path.display(),
                    0,
                    len
                ))
                .finish(loc(886, "logical_heap_rewrite_flush_mappings"));
        }
        src.off += len as u64;

        xloginsert_seams::xlog_insert_record::call(
            RM_HEAP2_ID,
            XLOG_HEAP2_REWRITE,
            0,
            &[&xlrec, &waldata],
            &[],
        )?;
    }
    debug_assert_eq!(state.rs_num_rewrite_mappings, 0);
    Ok(())
}

// logical_end_heap_rewrite (rewriteheap.c:905): flush the remaining
// in-memory entries, then fsync every mapping file we wrote.
fn logical_end_heap_rewrite(state: &mut RewriteState<'_>) -> PgResult<()> {
    if !state.rs_logical_rewrite {
        return Ok(());
    }
    if state.rs_num_rewrite_mappings > 0 {
        logical_heap_rewrite_flush_mappings(state)?;
    }
    for src in state.rs_logical_mappings.values() {
        if let Err(e) = src.file.sync_all() {
            return ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("could not fsync file \"{}\": {e}", src.path.display()))
                .finish(loc(921, "logical_end_heap_rewrite"));
        }
    }
    // Dropping the map closes the files (C: FileClose per entry).
    state.rs_logical_mappings.clear();
    Ok(())
}

// logical_rewrite_log_mapping (rewriteheap.c:935): buffer one (old->new)
// mapping for 'xid', creating the per-xid mapping file on first use.
fn logical_rewrite_log_mapping(
    state: &mut RewriteState<'_>,
    xid: TransactionId,
    map: &[u8; LOGICAL_REWRITE_MAPPING_SIZE],
) -> PgResult<()> {
    if !state.rs_logical_mappings.contains_key(&xid) {
        let name = format!(
            "map-{:x}-{:x}-{:X}_{:X}-{:x}-{:x}",
            state.rs_mapped_db,
            state.rs_old_relid,
            (state.rs_begin_lsn >> 32) as u32,
            state.rs_begin_lsn as u32,
            xid,
            xact_seams::get_current_transaction_id::call()?,
        );
        let path = mappings_dir().join(name);
        let file = match std::fs::OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(f) => f,
            Err(e) => {
                return ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(format!("could not create file \"{}\": {e}", path.display()))
                    .finish(loc(977, "logical_rewrite_log_mapping"));
            }
        };
        state
            .rs_logical_mappings
            .insert(xid, RewriteMappingFile { off: 0, path, file, mappings: Vec::new() });
    }
    let src = state.rs_logical_mappings.get_mut(&xid).unwrap();
    src.mappings.push(*map);
    state.rs_num_rewrite_mappings += 1;

    // Write out the buffers once we have too many in-memory entries across
    // all mapping files (C: 1000, "arbitrary number").
    if state.rs_num_rewrite_mappings >= 1000 {
        logical_heap_rewrite_flush_mappings(state)?;
    }
    Ok(())
}

fn serialize_locator(out: &mut [u8], locator: RelFileLocator) {
    out[0..4].copy_from_slice(&locator.spcOid.to_ne_bytes());
    out[4..8].copy_from_slice(&locator.dbOid.to_ne_bytes());
    out[8..12].copy_from_slice(&locator.relNumber.to_ne_bytes());
}

fn serialize_tid(out: &mut [u8], tid: ItemPointerData) {
    out[0..2].copy_from_slice(&tid.ip_blkid.bi_hi.to_ne_bytes());
    out[2..4].copy_from_slice(&tid.ip_blkid.bi_lo.to_ne_bytes());
    out[4..6].copy_from_slice(&tid.ip_posid.to_ne_bytes());
}

// logical_rewrite_heap_tuple (rewriteheap.c:999): log a mapping from
// old_tid to the tuple's new location if the tuple was created or deleted
// within any decoding slot's xmin horizon.
fn logical_rewrite_heap_tuple(
    state: &mut RewriteState<'_>,
    old_tid: ItemPointerData,
    new_tuple: &HeapTupleData<'_>,
) -> PgResult<()> {
    if !state.rs_logical_rewrite {
        return Ok(());
    }

    let new_tid = new_tuple.t_self;
    let cutoff = state.rs_logical_xmin;
    let hdr = new_tuple.t_data();

    // HeapTupleHeaderGetXmin: FrozenTransactionId (not normal) once frozen.
    let xmin = hdr.xmin();
    // *GetUpdateXid to correctly deal with multixacts.
    let xmax = HeapTupleHeaderGetUpdateXid(hdr)?;

    // Log the mapping iff the tuple has been created recently.
    let do_log_xmin = TransactionIdIsNormal(xmin) && !TransactionIdPrecedes(xmin, cutoff);
    let do_log_xmax = if !TransactionIdIsNormal(xmax) {
        // No xmax set: can't have any permanent ones.
        false
    } else if types_tuple::htup::HEAP_XMAX_IS_LOCKED_ONLY(hdr.t_infomask) {
        // Only locked: we don't care.
        false
    } else {
        // Deleted recently => log.
        !TransactionIdPrecedes(xmax, cutoff)
    };

    if !do_log_xmin && !do_log_xmax {
        return Ok(());
    }

    let mut map = [0u8; LOGICAL_REWRITE_MAPPING_SIZE];
    serialize_locator(&mut map[0..12], state.rs_old_locator);
    serialize_locator(&mut map[12..24], state.rs_new_locator);
    serialize_tid(&mut map[24..30], old_tid);
    serialize_tid(&mut map[30..36], new_tid);

    // Persist per affected xid; both arms unless that would be redundant
    // (subtransaction-imprecise on purpose, matching C).
    if do_log_xmin {
        logical_rewrite_log_mapping(state, xmin, &map)?;
    }
    if do_log_xmax && xmin != xmax {
        logical_rewrite_log_mapping(state, xmax, &map)?;
    }
    Ok(())
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

        {
            let tup = match cur.as_ref() {
                Some(t) => t.as_tuple(),
                None => new_tuple.as_tuple(),
            };
            logical_rewrite_heap_tuple(state, old_tid, tup)?;
        }

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
        heaptoast::heap_toast_insert_or_update(
            state.mcx,
            new_heap,
            tup,
            None,
            state.rs_toastoid,
            options,
        )?
    } else {
        None
    };
    let img_len = match heaptup.as_ref() {
        Some(t) => t.as_tuple().t_len as usize,
        None => tup.t_len as usize,
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
    // Extracted adjacent to the copy: no code between raw view and read.
    let img_ptr = match heaptup.as_ref() {
        Some(t) => t.as_tuple().header_ptr(),
        None => tup.header_ptr(),
    };
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

// CheckPointLogicalRewriteHeap (rewriteheap.c:1155): remove mapping files no
// decoding slot can still need (below the logical restart LSN), fsync the
// rest so post-checkpoint replay only handles bytes written after the redo
// pointer. Runs in checkpoints AND restartpoints (CheckPointGuts).
pub fn CheckPointLogicalRewriteHeap() -> PgResult<()> {
    // Minimum: the last redo pointer — no new decoding slot will start
    // before that.
    let redo = transam_xlog::GetRedoRecPtr();
    let mut cutoff = if slot_seams::replication_slots_compute_logical_restart_lsn::is_installed() {
        slot_seams::replication_slots_compute_logical_restart_lsn::call()?
    } else {
        types_core::InvalidXLogRecPtr
    };
    if cutoff != types_core::InvalidXLogRecPtr && redo < cutoff {
        cutoff = redo;
    }

    let dir = mappings_dir();
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        // Datadirs from initdb always carry pg_logical/mappings; a missing
        // dir here means nothing was ever mapped (minimal test datadirs).
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("could not open directory \"{}\": {e}", dir.display()))
                .finish(loc(1177, "CheckPointLogicalRewriteHeap"));
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(en) => en,
            Err(e) => {
                return ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(format!("could not read directory \"{}\": {e}", dir.display()))
                    .finish(loc(1178, "CheckPointLogicalRewriteHeap"));
            }
        };
        let name = entry.file_name();
        let name = name.to_string_lossy().into_owned();
        // Skip over files that cannot be ours.
        if !name.starts_with("map-") {
            continue;
        }
        let path = dir.join(&name);
        if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }

        // LOGICAL_REWRITE_FORMAT: map-%x-%x-%X_%X-%x-%x.
        let parts: Vec<&str> = name[4..].split('-').collect();
        let lsn_parts: Vec<&str> =
            if parts.len() == 5 { parts[2].split('_').collect() } else { Vec::new() };
        let (Some(Ok(hi)), Some(Ok(lo))) = (
            lsn_parts.first().map(|s| u32::from_str_radix(s, 16)),
            lsn_parts.get(1).map(|s| u32::from_str_radix(s, 16)),
        ) else {
            return Err(Box::new(PgError::new(
                ERROR,
                format!("could not parse filename \"{name}\""),
            )));
        };
        let lsn: XLogRecPtr = ((hi as u64) << 32) | lo as u64;

        if lsn < cutoff || cutoff == types_core::InvalidXLogRecPtr {
            let _ = elog::elog(
                DEBUG1,
                format!("removing logical rewrite file \"{}\"", path.display()),
            );
            if let Err(e) = std::fs::remove_file(&path) {
                return ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(format!("could not remove file \"{}\": {e}", path.display()))
                    .finish(loc(1224, "CheckPointLogicalRewriteHeap"));
            }
        } else {
            // The file cannot vanish concurrently: this function is the only
            // remover and one checkpoint runs at a time.
            let f = match std::fs::OpenOptions::new().read(true).write(true).open(&path) {
                Ok(f) => f,
                Err(e) => {
                    return ereport(ERROR)
                        .errcode_for_file_access()
                        .errmsg(format!("could not open file \"{}\": {e}", path.display()))
                        .finish(loc(1237, "CheckPointLogicalRewriteHeap"));
                }
            };
            if let Err(e) = f.sync_all() {
                return ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(format!("could not fsync file \"{}\": {e}", path.display()))
                    .finish(loc(1249, "CheckPointLogicalRewriteHeap"));
            }
        }
    }
    // Persist directory entries to disk (fsync_fname(..., true)).
    if let Ok(d) = std::fs::File::open(&dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

pub fn init_seams() {
    rewriteheap_seams::check_point_logical_rewrite_heap::set(CheckPointLogicalRewriteHeap);
}
