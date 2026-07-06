//! ginfast.c: pending-list fast insert + cleanup. gin_clean_pending_list
//! (SQL callable) lives in the gin_funcs crate.

use ::bufmgr_seams as bm;
use ::datum::Datum;
use ::gin_vocab::*;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::nbtree::itup::{self, ItupBuf};
use ::types_core::{BlockNumber, Buffer, InvalidBlockNumber, InvalidBuffer, OffsetNumber, BLCKSZ};
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_tuple::itemptr::{FirstOffsetNumber, ItemPointerData, ItemPointerEquals};
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD, REGBUF_WILL_INIT};

use crate::bulk::BuildAccumulator;
use crate::entrypage::gintuple_get_key;
use crate::insert::ginEntryInsert;
use crate::util::{
    gin_pending_list_cleanup_size, ginExtractEntries, set_meta_pd_lower, GinInitBuffer,
    GinNewBuffer,
};
use crate::{
    meta_of, page_bytes, page_bytes_mut, page_mut, page_opaque, page_ref, relation_needs_wal,
    write_meta_to, write_opaque, GinPageIsDeleted, GIN_EXCLUSIVE, GIN_SHARE, GIN_UNLOCK,
    RM_GIN,
};

const GIN_PAGE_FREESIZE: usize =
    BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(core::mem::size_of::<GinPageOpaqueData>());

pub(crate) struct GinTupleCollector<'s> {
    pub tuples: PgVec<'s, ItupBuf<'s>>,
    pub sumsize: usize,
}

impl<'s> GinTupleCollector<'s> {
    pub fn new(mcx: Mcx<'s>) -> Self {
        GinTupleCollector {
            tuples: PgVec::new_in(mcx),
            sumsize: 0,
        }
    }
}

/// ginHeapTupleFastCollect.
pub(crate) fn ginHeapTupleFastCollect<'s>(
    mcx: Mcx<'s>,
    rel: &Relation<'_>,
    state: &GinState,
    collector: &mut GinTupleCollector<'s>,
    attnum: OffsetNumber,
    value: Datum,
    is_null: bool,
    ht_ctid: &ItemPointerData,
) -> PgResult<()> {
    let (entries, categories) = ginExtractEntries(mcx, state, attnum, value, is_null)?;

    for (i, key) in entries.iter().enumerate() {
        let mut itup = crate::entrypage::GinFormTuple(
            mcx,
            rel,
            state,
            attnum,
            *key,
            categories[i],
            &[],
            0,
            0,
            true,
        )?
        .expect("errorTooBig");
        // Pending tuples carry the heap TID in t_tid.
        // SAFETY: owned tuple image.
        unsafe { itup::set_t_tid(itup.as_mut_ptr(), *ht_ctid) };
        // SAFETY: owned image.
        collector.sumsize += unsafe { itup::index_tuple_size(itup.as_ptr()) };
        collector.tuples.push(itup);
    }
    Ok(())
}

/// writeListPage: returns the page's remaining exact free space.
fn write_list_page(
    rel: &Relation<'_>,
    buffer: Buffer,
    tuples: &[ItupBuf<'_>],
    rightlink: BlockNumber,
) -> PgResult<usize> {
    GinInitBuffer(buffer, GIN_LIST);

    let mut size = 0usize;
    {
        // SAFETY: pin + exclusive lock held (GinNewBuffer).
        let mut page = unsafe { page_mut(buffer) };
        let mut off = FirstOffsetNumber;
        for t in tuples {
            // SAFETY: owned image; true length in t_info.
            let this_size = unsafe { itup::index_tuple_size(t.as_ptr()) };
            let bytes = unsafe { core::slice::from_raw_parts(t.as_ptr(), this_size) };
            if page.add_item(bytes, off, 0).is_none() {
                panic!("failed to add item to index page in \"{}\"", rel.name());
            }
            size += this_size;
            off += 1;
        }
        let mut opaque = page_opaque(&page.as_ref());
        opaque.rightlink = rightlink;
        if rightlink == InvalidBlockNumber {
            opaque.flags |= GIN_LIST_FULLROW;
            opaque.maxoff = 1;
        } else {
            opaque.maxoff = 0;
        }
        write_opaque(&mut page, &opaque);
    }
    bm::mark_buffer_dirty::call(buffer)?;

    if relation_needs_wal(rel) {
        let data = crate::wal::ginxlog_insert_listpage(rightlink, tuples.len() as i32);
        let mut frags: Vec<&[u8]> = Vec::with_capacity(tuples.len());
        for t in tuples {
            // SAFETY: owned images live across the WAL call.
            frags.push(unsafe {
                core::slice::from_raw_parts(t.as_ptr(), itup::index_tuple_size(t.as_ptr()))
            });
        }
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            RM_GIN,
            XLOG_GIN_INSERT_LISTPAGE,
            0,
            &[&data],
            &[XLogRegBuf {
                block_id: 0,
                buffer,
                flags: REGBUF_WILL_INIT,
                bufdata: &frags,
            }],
        )?;
        // SAFETY: pin + exclusive lock held.
        unsafe { page_mut(buffer) }.set_lsn(recptr);
    }

    // SAFETY: pin + exclusive lock held.
    let freesize = {
        let page = unsafe { page_ref(buffer) };
        page.exact_free_space()
    };
    bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
    bm::release_buffer::call(buffer)?;
    Ok(freesize)
}

/// makeSublist.
fn make_sublist(
    rel: &Relation<'_>,
    tuples: &[ItupBuf<'_>],
    res: &mut GinMetaPageData,
) -> PgResult<()> {
    debug_assert!(!tuples.is_empty());
    let mut cur_buffer = InvalidBuffer;
    let mut prev_buffer = InvalidBuffer;
    let mut start_tuple = 0usize;
    let mut size = 0usize;

    let mut i = 0usize;
    while i < tuples.len() {
        if cur_buffer == InvalidBuffer {
            cur_buffer = GinNewBuffer(rel)?;
            if prev_buffer != InvalidBuffer {
                res.nPendingPages += 1;
                write_list_page(
                    rel,
                    prev_buffer,
                    &tuples[start_tuple..i],
                    bm::buffer_get_block_number::call(cur_buffer),
                )?;
            } else {
                res.head = bm::buffer_get_block_number::call(cur_buffer);
            }
            prev_buffer = cur_buffer;
            start_tuple = i;
            size = 0;
        }

        // SAFETY: owned image.
        let tupsize = MAXALIGN(unsafe { itup::index_tuple_size(tuples[i].as_ptr()) }) + 4;
        if size + tupsize > GinListPageSize {
            cur_buffer = InvalidBuffer;
            continue;
        }
        size += tupsize;
        i += 1;
    }

    res.tail = bm::buffer_get_block_number::call(cur_buffer);
    res.tailFreeSize = write_list_page(rel, cur_buffer, &tuples[start_tuple..], InvalidBlockNumber)?
        as u32;
    res.nPendingPages += 1;
    res.nPendingHeapTuples = 1;
    Ok(())
}

/// ginHeapTupleFastInsert.
pub(crate) fn ginHeapTupleFastInsert<'s>(
    mcx: Mcx<'s>,
    rel: &Relation<'_>,
    state: &GinState,
    collector: &mut GinTupleCollector<'s>,
) -> PgResult<()> {
    if collector.tuples.is_empty() {
        return Ok(());
    }
    let need_wal = relation_needs_wal(rel);

    let mut wal_ntuples: i32 = 0;
    let mut wal_prev_tail = InvalidBlockNumber;
    let mut wal_new_rightlink = InvalidBlockNumber;

    let metabuffer = bm::read_buffer::call(rel, GIN_METAPAGE_BLKNO)?;

    let payload = collector.sumsize + collector.tuples.len() * 4;
    let mut separate_list = false;
    let mut meta_locked = false;
    if payload > GinListPageSize {
        separate_list = true;
    } else {
        bm::lock_buffer::call(metabuffer, GIN_EXCLUSIVE)?;
        meta_locked = true;
        // SAFETY: pin + exclusive lock held.
        let metadata = meta_of(page_bytes(&unsafe { page_ref(metabuffer) }));
        if metadata.head == InvalidBlockNumber || payload > metadata.tailFreeSize as usize {
            separate_list = true;
            bm::lock_buffer::call(metabuffer, GIN_UNLOCK)?;
            meta_locked = false;
        }
    }

    let mut buffer = InvalidBuffer;
    if separate_list {
        let mut sublist = GinMetaPageData::default();
        sublist.head = InvalidBlockNumber;
        sublist.tail = InvalidBlockNumber;
        make_sublist(rel, collector.tuples.as_slice(), &mut sublist)?;

        bm::lock_buffer::call(metabuffer, GIN_EXCLUSIVE)?;
        meta_locked = true;
        predicate_seams::check_for_serializable_conflict_in::call(
            rel,
            None,
            GIN_METAPAGE_BLKNO,
        )?;

        // SAFETY: pin + exclusive lock held.
        let mut metadata = { meta_of(page_bytes(&unsafe { page_ref(metabuffer) })) };
        if metadata.head == InvalidBlockNumber {
            metadata.head = sublist.head;
            metadata.tail = sublist.tail;
            metadata.tailFreeSize = sublist.tailFreeSize;
            metadata.nPendingPages = sublist.nPendingPages;
            metadata.nPendingHeapTuples = sublist.nPendingHeapTuples;
        } else {
            wal_prev_tail = metadata.tail;
            wal_new_rightlink = sublist.head;

            buffer = bm::read_buffer::call(rel, metadata.tail)?;
            bm::lock_buffer::call(buffer, GIN_EXCLUSIVE)?;
            {
                // SAFETY: pin + exclusive lock held.
                let mut page = unsafe { page_mut(buffer) };
                let mut opaque = page_opaque(&page.as_ref());
                debug_assert!(opaque.rightlink == InvalidBlockNumber);
                opaque.rightlink = sublist.head;
                write_opaque(&mut page, &opaque);
            }
            bm::mark_buffer_dirty::call(buffer)?;

            metadata.tail = sublist.tail;
            metadata.tailFreeSize = sublist.tailFreeSize;
            metadata.nPendingPages += sublist.nPendingPages;
            metadata.nPendingHeapTuples += sublist.nPendingHeapTuples;
        }
        // SAFETY: pin + exclusive lock held.
        let mut page = unsafe { page_mut(metabuffer) };
        // SAFETY: borrow confined here.
        let bytes = unsafe { page_bytes_mut(&mut page) };
        write_meta_to(bytes, &metadata);
    } else {
        // Insert into the tail page; metapage stays locked.
        predicate_seams::check_for_serializable_conflict_in::call(
            rel,
            None,
            GIN_METAPAGE_BLKNO,
        )?;
        // SAFETY: pin + exclusive lock held.
        let mut metadata = { meta_of(page_bytes(&unsafe { page_ref(metabuffer) })) };

        buffer = bm::read_buffer::call(rel, metadata.tail)?;
        bm::lock_buffer::call(buffer, GIN_EXCLUSIVE)?;
        wal_ntuples = collector.tuples.len() as i32;

        {
            // SAFETY: pin + exclusive lock held.
            let mut page = unsafe { page_mut(buffer) };
            let mut off = if page.as_ref().pd_lower() as usize <= SizeOfPageHeaderData {
                FirstOffsetNumber
            } else {
                page.as_ref().max_offset_number() + 1
            };
            let mut opaque = page_opaque(&page.as_ref());
            debug_assert!(opaque.maxoff as i64 <= metadata.nPendingHeapTuples);
            opaque.maxoff += 1;
            metadata.nPendingHeapTuples += 1;
            write_opaque(&mut page, &opaque);

            for t in collector.tuples.iter() {
                // SAFETY: owned image.
                let tupsize = unsafe { itup::index_tuple_size(t.as_ptr()) };
                let bytes = unsafe { core::slice::from_raw_parts(t.as_ptr(), tupsize) };
                if page.add_item(bytes, off, 0).is_none() {
                    panic!("failed to add item to index page in \"{}\"", rel.name());
                }
                off += 1;
            }
        }
        bm::mark_buffer_dirty::call(buffer)?;
        // SAFETY: pin + exclusive lock held.
        metadata.tailFreeSize = {
            let page = unsafe { page_ref(buffer) };
            page.exact_free_space() as u32
        };
        // SAFETY: pin + exclusive lock held.
        let mut page = unsafe { page_mut(metabuffer) };
        // SAFETY: borrow confined here.
        let bytes = unsafe { page_bytes_mut(&mut page) };
        write_meta_to(bytes, &metadata);
    }
    debug_assert!(meta_locked);

    // pd_lower past the metadata (essential for xlog page compression).
    {
        // SAFETY: pin + exclusive lock held.
        let mut page = unsafe { page_mut(metabuffer) };
        // SAFETY: borrow confined here.
        set_meta_pd_lower(unsafe { page_bytes_mut(&mut page) });
    }
    bm::mark_buffer_dirty::call(metabuffer)?;

    // SAFETY: pin + exclusive lock held.
    let metadata = { meta_of(page_bytes(&unsafe { page_ref(metabuffer) })) };

    if need_wal {
        let data = crate::wal::ginxlog_update_meta(
            rel,
            &metadata,
            wal_prev_tail,
            wal_new_rightlink,
            wal_ntuples,
        );
        let mut bufs: Vec<XLogRegBuf<'_>> = Vec::with_capacity(2);
        bufs.push(XLogRegBuf {
            block_id: 0,
            buffer: metabuffer,
            flags: REGBUF_WILL_INIT | REGBUF_STANDARD,
            bufdata: &[],
        });
        let mut frags: Vec<&[u8]> = Vec::new();
        if buffer != InvalidBuffer {
            if wal_ntuples > 0 {
                for t in collector.tuples.iter() {
                    // SAFETY: owned images live across the WAL call.
                    frags.push(unsafe {
                        core::slice::from_raw_parts(t.as_ptr(), itup::index_tuple_size(t.as_ptr()))
                    });
                }
            }
            bufs.push(XLogRegBuf {
                block_id: 1,
                buffer,
                flags: REGBUF_STANDARD,
                bufdata: &frags,
            });
        }
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            RM_GIN,
            XLOG_GIN_UPDATE_META_PAGE,
            0,
            &[&data],
            &bufs,
        )?;
        // SAFETY: pin + exclusive lock held.
        unsafe { page_mut(metabuffer) }.set_lsn(recptr);
        if buffer != InvalidBuffer {
            // SAFETY: pin + exclusive lock held.
            unsafe { page_mut(buffer) }.set_lsn(recptr);
        }
    }

    if buffer != InvalidBuffer {
        bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
        bm::release_buffer::call(buffer)?;
    }

    let cleanup_size = gin_pending_list_cleanup_size(rel);
    let need_cleanup =
        metadata.nPendingPages as usize * GIN_PAGE_FREESIZE > cleanup_size as usize * 1024;

    bm::lock_buffer::call(metabuffer, GIN_UNLOCK)?;
    bm::release_buffer::call(metabuffer)?;

    if need_cleanup {
        ginInsertCleanup(mcx, rel, state, false, true, false, None)?;
    }
    Ok(())
}

/// shiftList: delete pending pages up to (not including) newHead.
fn shift_list(
    rel: &Relation<'_>,
    metabuffer: Buffer,
    new_head: BlockNumber,
    fill_fsm: bool,
    mut stats: Option<&mut ::types_nbtree::IndexBulkDeleteResult>,
) -> PgResult<()> {
    // SAFETY: pin + exclusive lock held throughout.
    let mut blkno_to_delete = { meta_of(page_bytes(&unsafe { page_ref(metabuffer) })).head };

    loop {
        let mut buffers: [Buffer; GIN_NDELETE_AT_ONCE] = [InvalidBuffer; GIN_NDELETE_AT_ONCE];
        let mut freespace: [BlockNumber; GIN_NDELETE_AT_ONCE] =
            [InvalidBlockNumber; GIN_NDELETE_AT_ONCE];
        let mut ndeleted = 0usize;
        let mut n_deleted_heap_tuples: i64 = 0;

        while ndeleted < GIN_NDELETE_AT_ONCE && blkno_to_delete != new_head {
            freespace[ndeleted] = blkno_to_delete;
            let buf = bm::read_buffer::call(rel, blkno_to_delete)?;
            bm::lock_buffer::call(buf, GIN_EXCLUSIVE)?;
            buffers[ndeleted] = buf;
            ndeleted += 1;
            // SAFETY: pin + exclusive lock held.
            let opaque = page_opaque(&unsafe { page_ref(buf) });
            debug_assert!(!GinPageIsDeleted(&opaque));
            n_deleted_heap_tuples += opaque.maxoff as i64;
            blkno_to_delete = opaque.rightlink;
        }

        if let Some(s) = stats.as_deref_mut() {
            s.pages_deleted += ndeleted as u32;
        }

        let metadata = {
            // SAFETY: pin + exclusive lock held.
            let mut page = unsafe { page_mut(metabuffer) };
            // SAFETY: borrow confined here.
            let bytes = unsafe { page_bytes_mut(&mut page) };
            let mut metadata = meta_of(bytes);
            metadata.head = blkno_to_delete;
            debug_assert!(metadata.nPendingPages as usize >= ndeleted);
            metadata.nPendingPages -= ndeleted as u32;
            debug_assert!(metadata.nPendingHeapTuples >= n_deleted_heap_tuples);
            metadata.nPendingHeapTuples -= n_deleted_heap_tuples;
            if blkno_to_delete == InvalidBlockNumber {
                metadata.tail = InvalidBlockNumber;
                metadata.tailFreeSize = 0;
                metadata.nPendingPages = 0;
                metadata.nPendingHeapTuples = 0;
            }
            write_meta_to(bytes, &metadata);
            set_meta_pd_lower(bytes);
            metadata
        };
        bm::mark_buffer_dirty::call(metabuffer)?;

        for buf in &buffers[..ndeleted] {
            // SAFETY: pin + exclusive lock held.
            let mut page = unsafe { page_mut(*buf) };
            let mut opaque = page_opaque(&page.as_ref());
            opaque.flags = GIN_DELETED;
            write_opaque(&mut page, &opaque);
            bm::mark_buffer_dirty::call(*buf)?;
        }

        if relation_needs_wal(rel) {
            let data = crate::wal::ginxlog_delete_listpages(&metadata, ndeleted as i32);
            let mut bufs: Vec<XLogRegBuf<'_>> = Vec::with_capacity(1 + ndeleted);
            bufs.push(XLogRegBuf {
                block_id: 0,
                buffer: metabuffer,
                flags: REGBUF_WILL_INIT | REGBUF_STANDARD,
                bufdata: &[],
            });
            for (i, buf) in buffers[..ndeleted].iter().enumerate() {
                bufs.push(XLogRegBuf {
                    block_id: (i + 1) as u8,
                    buffer: *buf,
                    flags: REGBUF_WILL_INIT,
                    bufdata: &[],
                });
            }
            let recptr = ::xloginsert_seams::xlog_insert_record::call(
                RM_GIN,
                XLOG_GIN_DELETE_LISTPAGE,
                0,
                &[&data],
                &bufs,
            )?;
            // SAFETY: pins + exclusive locks held.
            unsafe {
                page_mut(metabuffer).set_lsn(recptr);
                for buf in &buffers[..ndeleted] {
                    page_mut(*buf).set_lsn(recptr);
                }
            }
        }

        for buf in &buffers[..ndeleted] {
            bm::lock_buffer::call(*buf, GIN_UNLOCK)?;
            bm::release_buffer::call(*buf)?;
        }
        if fill_fsm {
            for blk in &freespace[..ndeleted] {
                freespace::RecordFreeIndexPage(rel, *blk)?;
            }
        }

        if blkno_to_delete == new_head {
            return Ok(());
        }
    }
}

/// processPendingPage.
fn process_pending_page<'s>(
    mcx: Mcx<'s>,
    rel: &Relation<'_>,
    state: &GinState,
    accum: &mut BuildAccumulator<'_>,
    ka: &mut (Vec<Datum>, Vec<GinNullCategory>),
    buffer: Buffer,
    startoff: OffsetNumber,
) -> PgResult<()> {
    ka.0.clear();
    ka.1.clear();
    // SAFETY: pin + share lock held.
    let page = unsafe { page_ref(buffer) };
    let maxoff = page.max_offset_number();
    debug_assert!(maxoff >= FirstOffsetNumber);
    let mut heapptr = ItemPointerData::invalid();
    let mut attrnum: OffsetNumber = 0;

    for i in startoff..=maxoff {
        let id = page.item_id(i);
        let itup = page.item_raw(id).0;
        // SAFETY: live tuple under the lock; t_tid is the heap pointer here.
        let tid = unsafe { itup::t_tid(itup) };
        // SAFETY: as above.
        let curattnum = unsafe { crate::entrypage::gintuple_get_attrnum(state, itup) };

        // C: !ItemPointerIsValid(&heapptr) — validity is posid != 0.
        if heapptr.ip_posid == 0 {
            heapptr = tid;
            attrnum = curattnum;
        } else if !(ItemPointerEquals(&heapptr, &tid) && curattnum == attrnum) {
            accum.insert_entries(&heapptr, attrnum, &ka.0, &ka.1)?;
            ka.0.clear();
            ka.1.clear();
            heapptr = tid;
            attrnum = curattnum;
        }
        let mut category = GIN_CAT_NORM_KEY;
        // SAFETY: as above.
        let key = unsafe { gintuple_get_key(mcx, rel, state, itup, &mut category)? };
        ka.0.push(key);
        ka.1.push(category);
    }

    accum.insert_entries(&heapptr, attrnum, &ka.0, &ka.1)?;
    Ok(())
}

/// ginInsertCleanup. Page-level heavyweight lock arbitration is a no-op in
/// the single-backend model.
pub fn ginInsertCleanup<'s>(
    _outer: Mcx<'s>,
    rel: &Relation<'_>,
    state: &GinState,
    full_clean: bool,
    fill_fsm: bool,
    force_cleanup: bool,
    mut stats: Option<&mut ::types_nbtree::IndexBulkDeleteResult>,
) -> PgResult<()> {
    let work_memory = if force_cleanup {
        init_small::globals::maintenance_work_mem()
    } else {
        init_small::globals::work_mem()
    };

    let metabuffer = bm::read_buffer::call(rel, GIN_METAPAGE_BLKNO)?;
    bm::lock_buffer::call(metabuffer, GIN_SHARE)?;
    // SAFETY: pin + share lock held.
    let metadata0 = { meta_of(page_bytes(&unsafe { page_ref(metabuffer) })) };
    if metadata0.head == InvalidBlockNumber {
        bm::lock_buffer::call(metabuffer, GIN_UNLOCK)?;
        bm::release_buffer::call(metabuffer)?;
        return Ok(());
    }
    let blkno_finish = metadata0.tail;

    let mut blkno = metadata0.head;
    let mut buffer = bm::read_buffer::call(rel, blkno)?;
    bm::lock_buffer::call(buffer, GIN_SHARE)?;
    bm::lock_buffer::call(metabuffer, GIN_UNLOCK)?;

    let mut op_ctx = MemoryContext::new_bump("GIN insert cleanup temporary context");
    // SAFETY: accum is dropped/rebuilt around every op_ctx reset.
    unsafe fn erase<'x>(a: BuildAccumulator<'x>) -> BuildAccumulator<'static> {
        unsafe { core::mem::transmute(a) }
    }
    // SAFETY: per erase contract.
    let mut accum = unsafe { erase(BuildAccumulator::new(op_ctx.mcx(), *state)) };
    let mut ka: (Vec<Datum>, Vec<GinNullCategory>) =
        (Vec::with_capacity(128), Vec::with_capacity(128));
    let mut cleanup_finish = false;
    let mut fsm_vac = false;

    loop {
        // SAFETY: pin + share lock held.
        debug_assert!(!GinPageIsDeleted(&page_opaque(&unsafe { page_ref(buffer) })));

        if blkno == blkno_finish && !full_clean {
            cleanup_finish = true;
        }

        process_pending_page(op_ctx.mcx(), rel, state, &mut accum, &mut ka, buffer, FirstOffsetNumber)?;

        // SAFETY: pin + share lock held.
        let opaque = page_opaque(&unsafe { page_ref(buffer) });
        let full_row = opaque.flags & GIN_LIST_FULLROW != 0;
        if opaque.rightlink == InvalidBlockNumber
            || (full_row && accum.allocated_memory >= work_memory as usize * 1024)
        {
            // SAFETY: pin + share lock held.
            let maxoff = { unsafe { page_ref(buffer) }.max_offset_number() };
            bm::lock_buffer::call(buffer, GIN_UNLOCK)?;

            accum.begin_scan()?;
            loop {
                let Some((attnum, key, category, list)) = accum.next_entry() else {
                    break;
                };
                let dump_ctx = MemoryContext::new_bump("gin cleanup dump scratch");
                ginEntryInsert(dump_ctx.mcx(), rel, state, attnum, key, category, list, None)?;
            }

            bm::lock_buffer::call(metabuffer, GIN_EXCLUSIVE)?;
            bm::lock_buffer::call(buffer, GIN_SHARE)?;
            // SAFETY: pin + share lock held.
            debug_assert!(!GinPageIsDeleted(&page_opaque(&unsafe { page_ref(buffer) })));

            // SAFETY: pin + share lock held.
            let new_maxoff = { unsafe { page_ref(buffer) }.max_offset_number() };
            if new_maxoff != maxoff {
                drop(accum);
                op_ctx.reset();
                accum = unsafe { erase(BuildAccumulator::new(op_ctx.mcx(), *state)) };
                process_pending_page(op_ctx.mcx(), rel, state, &mut accum, &mut ka, buffer, maxoff + 1)?;
                accum.begin_scan()?;
                loop {
                    let Some((attnum, key, category, list)) = accum.next_entry() else {
                        break;
                    };
                    let dump_ctx = MemoryContext::new_bump("gin cleanup dump scratch");
                    ginEntryInsert(dump_ctx.mcx(), rel, state, attnum, key, category, list, None)?;
                }
            }

            // SAFETY: pin + share lock held.
            blkno = { page_opaque(&unsafe { page_ref(buffer) }).rightlink };
            bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
            bm::release_buffer::call(buffer)?;

            shift_list(rel, metabuffer, blkno, fill_fsm, stats.as_deref_mut())?;
            fsm_vac = true;

            bm::lock_buffer::call(metabuffer, GIN_UNLOCK)?;

            if blkno == InvalidBlockNumber || cleanup_finish {
                break;
            }

            drop(accum);
            op_ctx.reset();
            accum = unsafe { erase(BuildAccumulator::new(op_ctx.mcx(), *state)) };
        } else {
            blkno = opaque.rightlink;
            bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
            bm::release_buffer::call(buffer)?;
        }

        buffer = bm::read_buffer::call(rel, blkno)?;
        bm::lock_buffer::call(buffer, GIN_SHARE)?;
    }

    bm::release_buffer::call(metabuffer)?;
    drop(accum);

    if fsm_vac && fill_fsm {
        freespace_seams::free_space_map_vacuum_range::call(rel, 0, InvalidBlockNumber)?;
    }
    Ok(())
}
