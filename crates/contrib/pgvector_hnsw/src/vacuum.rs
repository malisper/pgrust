use crate::layout::*;
use crate::utils::*;
use bufmgr::{
    LockBuffer, LockBufferForCleanup, UnlockReleaseBuffer, BUFFER_LOCK_EXCLUSIVE,
    BUFFER_LOCK_SHARE,
};
use generic_xlog::{GenericXLogAbort, GenericXLogFinish, GenericXLogStart};
use mcx::{Mcx, PgFxHashMap};
use types_core::{BlockNumber, ForkNumber, BLCKSZ};
use types_error::PgResult;
use types_hnsw::*;
use types_nbtree::genam::IndexBulkDeleteResult;
use types_rel::Relation;
use types_storage::bufpage::PageRef;
use types_storage::buf::BufferAccessStrategyType;
use types_storage::lock::{ExclusiveLock, ShareLock};
use types_tuple::itemptr::{
    ItemPointerData, ItemPointerGetBlockNumberNoCheck, ItemPointerGetOffsetNumberNoCheck,
};

use nbtree::IndexVacuumInfo;

fn tid_cmp(a: &ItemPointerData, b: &ItemPointerData) -> core::cmp::Ordering {
    (
        ItemPointerGetBlockNumberNoCheck(a),
        ItemPointerGetOffsetNumberNoCheck(a),
    )
        .cmp(&(
            ItemPointerGetBlockNumberNoCheck(b),
            ItemPointerGetOffsetNumberNoCheck(b),
        ))
}

fn tid_reaped(dead_items: &[ItemPointerData], tid: &ItemPointerData) -> bool {
    dead_items.binary_search_by(|probe| tid_cmp(probe, tid)).is_ok()
}

type Deleting<'t> = PgFxHashMap<'t, (BlockNumber, u16), ()>;

struct VacState<'a, 't, 'mcx> {
    index: &'a Relation<'mcx>,
    stats: IndexBulkDeleteResult,
    dead_items: &'a [ItemPointerData],
    m: i32,
    ef_construction: i32,
    support: HnswSupport,
    deleting: Deleting<'t>,
    op_mcx: Mcx<'t>,
    highest: Option<(BlockNumber, u16, u8)>,
    fallback: Option<(BlockNumber, u16, u8)>,
}

fn vacuum_delay() -> PgResult<()> {
    postgres_seams::check_for_interrupts::call()
}

// RemoveHeapTids (pass 1).
fn remove_heap_tids(vs: &mut VacState<'_, '_, '_>) -> PgResult<()> {
    let mut blkno = HNSW_HEAD_BLKNO;
    let mut highest_level: i32 = -1;
    let mut fallback_level: i32 = -1;

    while blkno != INVALID_BLOCK {
        vacuum_delay()?;
        let buf = bufmgr::ReadBufferExtended(
            vs.index,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            types_storage::storage::ReadBufferMode::Normal,
            bufmgr::GetAccessStrategy(BufferAccessStrategyType::BasBulkread),
        )?;
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE)?;
        let mut state = GenericXLogStart(vs.op_mcx, vs.index)?;
        let page: &mut [u8; BLCKSZ] = state.register_buffer(buf, 0)?;
        let page_ptr = page.as_mut_ptr();
        let mut updated = false;

        // SAFETY: registered image, exclusive access.
        let pr = unsafe {
            PageRef::from_raw(core::ptr::NonNull::new(page_ptr).unwrap())
        };
        let maxoffno = pr.max_offset_number();
        for offno in 1..=maxoffno {
            let (item_off, item_len) = {
                let id = pr.item_id(offno);
                let (p, len) = pr.item_raw(id);
                (p as usize - page_ptr as usize, len as usize)
            };
            // SAFETY: item bounds within the registered image.
            let bytes = unsafe {
                core::slice::from_raw_parts_mut(page_ptr.add(item_off), item_len)
            };
            if bytes[0] != HNSW_ELEMENT_TUPLE_TYPE {
                continue;
            }
            if bytes[2] != 0 {
                continue;
            }
            let mut idx = 0usize;
            let mut item_updated = false;
            if itemptr_is_valid(&bytes[4..10]) {
                for i in 0..HNSW_HEAPTIDS {
                    let off = 4 + i * 6;
                    if !itemptr_is_valid(&bytes[off..off + 6]) {
                        break;
                    }
                    let tid = ipd_from_bytes(&bytes[off..off + 6]);
                    if tid_reaped(vs.dead_items, &tid) {
                        item_updated = true;
                        vs.stats.tuples_removed += 1.0;
                    } else {
                        let dst = 4 + idx * 6;
                        let src: [u8; 6] = bytes[off..off + 6].try_into().unwrap();
                        bytes[dst..dst + 6].copy_from_slice(&src);
                        idx += 1;
                        vs.stats.num_index_tuples += 1.0;
                    }
                }
                if item_updated {
                    for i in idx..HNSW_HEAPTIDS {
                        let off = 4 + i * 6;
                        bytes[off..off + 6]
                            .copy_from_slice(&itemptr_encode(INVALID_BLOCK, INVALID_OFFSET));
                    }
                    updated = true;
                }
            }

            let level = bytes[1];
            if !itemptr_is_valid(&bytes[4..10]) {
                vs.deleting.insert((blkno, offno), ());
            } else if level as i32 > highest_level {
                if let Some(h) = vs.highest {
                    vs.fallback = Some(h);
                    fallback_level = highest_level;
                }
                vs.highest = Some((blkno, offno, level));
                highest_level = level as i32;
            } else if level as i32 > fallback_level {
                vs.fallback = Some((blkno, offno, level));
                fallback_level = level as i32;
            }
        }

        let next = page_opaque_nextblkno(state.page_image_mut(0));
        if updated {
            GenericXLogFinish(state)?;
        } else {
            GenericXLogAbort(state);
        }
        UnlockReleaseBuffer(buf)?;
        blkno = next;
    }
    Ok(())
}

// NeedsUpdated.
fn needs_updated(
    vs: &VacState<'_, '_, '_>,
    neighbor_page: BlockNumber,
    neighbor_offno: u16,
) -> PgResult<bool> {
    let buf = bufmgr::ReadBufferExtended(
        vs.index,
        ForkNumber::MAIN_FORKNUM,
        neighbor_page,
        types_storage::storage::ReadBufferMode::Normal,
        bufmgr::GetAccessStrategy(BufferAccessStrategyType::BasBulkread),
    )?;
    LockBuffer(buf, BUFFER_LOCK_SHARE)?;
    let page = buf_page_bytes(buf);
    // SAFETY: share lock held.
    let pr = unsafe {
        PageRef::from_raw(core::ptr::NonNull::new(page.as_ptr() as *mut u8).unwrap())
    };
    let ntup = NeighborTupleView { bytes: item_bytes(&pr, neighbor_offno) };
    let mut needs = false;
    let count = ntup.count() as usize;
    for i in 0..count {
        let tid = ntup.indextid_bytes(i);
        if !itemptr_is_valid(tid) {
            continue;
        }
        if vs.deleting.contains_key(&itemptr_decode(tid)) {
            needs = true;
            break;
        }
    }
    if !needs && count > 0 && !itemptr_is_valid(ntup.indextid_bytes(count - 1)) {
        needs = true;
    }
    UnlockReleaseBuffer(buf)?;
    Ok(needs)
}

// RepairGraphElement.
fn repair_graph_element(
    vs: &mut VacState<'_, '_, '_>,
    element: (BlockNumber, u16),
    entry_point: Option<(BlockNumber, u16, u8)>,
) -> PgResult<()> {
    if let Some(ep) = entry_point {
        if element.0 == ep.0 && element.1 == ep.1 {
            return Ok(());
        }
    }

    let tmp = mcx::MemoryContext::new_bump("hnsw repair");
    let tmcx = tmp.mcx();
    let mut pool = ElementPool::new(tmcx);
    let e_id = pool.from_block(element.0, element.1);
    let mut support = vs.support.clone();
    // Load level/version/value; then clear heaptids per C (skip self search).
    load_element(&mut pool, e_id, None, None, vs.index, &mut support, true, None)?;
    pool.get_mut(e_id).heaptids_len = 0;

    let entry_id = entry_point.map(|(b, o, l)| {
        let id = pool.from_block(b, o);
        pool.get_mut(id).level = l;
        id
    });

    find_element_neighbors(
        &mut pool,
        e_id,
        entry_id,
        vs.index,
        &mut support,
        vs.m,
        vs.ef_construction,
        true,
    )?;

    let mut ntup: Vec<u8> = Vec::new();
    set_neighbor_tuple(&mut ntup, &pool, e_id, vs.m);

    let (npage_blk, noffno) = {
        let e = pool.get(e_id);
        (e.neighbor_page, e.neighbor_offno)
    };
    let buf = bufmgr::ReadBufferExtended(
        vs.index,
        ForkNumber::MAIN_FORKNUM,
        npage_blk,
        types_storage::storage::ReadBufferMode::Normal,
        bufmgr::GetAccessStrategy(BufferAccessStrategyType::BasBulkread),
    )?;
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE)?;
    let mut state = GenericXLogStart(vs.op_mcx, vs.index)?;
    let page = state.register_buffer(buf, 0)?;
    // SAFETY: registered image, exclusive access.
    let mut pm = unsafe {
        types_storage::bufpage::PageMut::from_raw(
            core::ptr::NonNull::new(page.as_mut_ptr()).unwrap(),
        )
    };
    if !pm.index_tuple_overwrite(noffno, &ntup) {
        return Err(types_error::PgError::error(format!(
            "failed to add index item to \"{}\"",
            vs.index.name()
        ))
        .into());
    }
    GenericXLogFinish(state)?;
    UnlockReleaseBuffer(buf)?;

    update_neighbors_on_disk_repair(vs, &mut pool, e_id)?;
    Ok(())
}

fn update_neighbors_on_disk_repair(
    vs: &mut VacState<'_, '_, '_>,
    pool: &mut ElementPool<'_>,
    e_id: u32,
) -> PgResult<()> {
    let mut support = vs.support.clone();
    crate::insert::update_neighbors_on_disk(
        vs.index, &mut support, pool, e_id, vs.m, true, false, vs.op_mcx,
    )
}

fn element_neighbor_tid(
    vs: &VacState<'_, '_, '_>,
    element: (BlockNumber, u16),
) -> PgResult<(BlockNumber, u16)> {
    let buf = bufmgr::ReadBuffer(vs.index, element.0)?;
    LockBuffer(buf, BUFFER_LOCK_SHARE)?;
    let page = buf_page_bytes(buf);
    // SAFETY: share lock held.
    let pr = unsafe {
        PageRef::from_raw(core::ptr::NonNull::new(page.as_ptr() as *mut u8).unwrap())
    };
    let etup = ElementTupleView { bytes: item_bytes(&pr, element.1) };
    let out = etup.neighbortid();
    UnlockReleaseBuffer(buf)?;
    Ok(out)
}

// RepairGraphEntryPoint.
fn repair_graph_entry_point(vs: &mut VacState<'_, '_, '_>) -> PgResult<()> {
    let mut highest = vs.highest;

    if highest.is_some() {
        lmgr::LockPage(vs.index, HNSW_UPDATE_LOCK, ShareLock)?;
        let entry = meta_entry_point(&read_meta(vs.index)?);
        if let (Some(ep), Some(h)) = (&entry, &highest) {
            if ep.blkno == h.0 && ep.offno == h.1 {
                highest = vs.fallback;
            }
        }
        if let Some(h) = highest {
            let ntid = element_neighbor_tid(vs, (h.0, h.1))?;
            if needs_updated(vs, ntid.0, ntid.1)? {
                let ep = entry.as_ref().map(|e| (e.blkno, e.offno, e.level));
                repair_graph_element(vs, (h.0, h.1), ep)?;
            }
        }
        lmgr::UnlockPage(vs.index, HNSW_UPDATE_LOCK, ShareLock)?;
    }

    lmgr::LockPage(vs.index, HNSW_UPDATE_LOCK, ExclusiveLock)?;
    let entry = meta_entry_point(&read_meta(vs.index)?);
    if let Some(ep) = entry {
        if vs.deleting.contains_key(&(ep.blkno, ep.offno)) {
            update_meta_page(
                vs.index,
                HNSW_UPDATE_ENTRY_ALWAYS,
                highest.map(|(b, o, l)| (b, o, l as i16)),
                INVALID_BLOCK,
                ForkNumber::MAIN_FORKNUM,
                false,
            )?;
        } else {
            let ntid = element_neighbor_tid(vs, (ep.blkno, ep.offno))?;
            if needs_updated(vs, ntid.0, ntid.1)? {
                repair_graph_element(vs, (ep.blkno, ep.offno), highest)?;
            }
        }
    }
    lmgr::UnlockPage(vs.index, HNSW_UPDATE_LOCK, ExclusiveLock)?;
    Ok(())
}

// RepairGraph (pass 2).
fn repair_graph(vs: &mut VacState<'_, '_, '_>) -> PgResult<()> {
    lmgr::LockPage(vs.index, HNSW_UPDATE_LOCK, ExclusiveLock)?;
    lmgr::UnlockPage(vs.index, HNSW_UPDATE_LOCK, ExclusiveLock)?;

    repair_graph_entry_point(vs)?;

    let mut blkno = HNSW_HEAD_BLKNO;
    while blkno != INVALID_BLOCK {
        vacuum_delay()?;

        let mut elements: Vec<(BlockNumber, u16, u8, BlockNumber, u16)> = Vec::new();
        let buf = bufmgr::ReadBufferExtended(
            vs.index,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            types_storage::storage::ReadBufferMode::Normal,
            bufmgr::GetAccessStrategy(BufferAccessStrategyType::BasBulkread),
        )?;
        LockBuffer(buf, BUFFER_LOCK_SHARE)?;
        let page = buf_page_bytes(buf);
        // SAFETY: share lock held.
        let pr = unsafe {
            PageRef::from_raw(core::ptr::NonNull::new(page.as_ptr() as *mut u8).unwrap())
        };
        let maxoffno = pr.max_offset_number();
        for offno in 1..=maxoffno {
            let etup = ElementTupleView { bytes: item_bytes(&pr, offno) };
            if etup.tuple_type() != HNSW_ELEMENT_TUPLE_TYPE {
                continue;
            }
            if etup.deleted() != 0 {
                continue;
            }
            if !itemptr_is_valid(etup.heaptid_bytes(0)) {
                continue;
            }
            let (np, no) = etup.neighbortid();
            elements.push((blkno, offno, etup.level(), np, no));
        }
        let next = page_opaque_nextblkno(page);
        UnlockReleaseBuffer(buf)?;

        for (eb, eo, elevel, np, no) in elements {
            if !needs_updated(vs, np, no)? {
                continue;
            }
            let mut lockmode = ShareLock;
            lmgr::LockPage(vs.index, HNSW_UPDATE_LOCK, lockmode)?;
            let mut entry = meta_entry_point(&read_meta(vs.index)?);

            let promote = match &entry {
                None => true,
                Some(ep) => elevel > ep.level,
            };
            if promote {
                lmgr::UnlockPage(vs.index, HNSW_UPDATE_LOCK, lockmode)?;
                lockmode = ExclusiveLock;
                lmgr::LockPage(vs.index, HNSW_UPDATE_LOCK, lockmode)?;
                entry = meta_entry_point(&read_meta(vs.index)?);
            }

            let ep = entry.as_ref().map(|e| (e.blkno, e.offno, e.level));
            repair_graph_element(vs, (eb, eo), ep)?;

            let promote2 = match &entry {
                None => true,
                Some(e) => elevel > e.level,
            };
            if promote2 {
                update_meta_page(
                    vs.index,
                    HNSW_UPDATE_ENTRY_GREATER,
                    Some((eb, eo, elevel as i16)),
                    INVALID_BLOCK,
                    ForkNumber::MAIN_FORKNUM,
                    false,
                )?;
            }
            lmgr::UnlockPage(vs.index, HNSW_UPDATE_LOCK, lockmode)?;
        }
        blkno = next;
    }
    Ok(())
}

// ConfirmRepaired (pass 3).
fn confirm_repaired(vs: &VacState<'_, '_, '_>) -> PgResult<()> {
    let mut blkno = HNSW_HEAD_BLKNO;
    while blkno != INVALID_BLOCK {
        vacuum_delay()?;
        let buf = bufmgr::ReadBufferExtended(
            vs.index,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            types_storage::storage::ReadBufferMode::Normal,
            bufmgr::GetAccessStrategy(BufferAccessStrategyType::BasBulkread),
        )?;
        LockBuffer(buf, BUFFER_LOCK_SHARE)?;
        let page = buf_page_bytes(buf);
        // SAFETY: share lock held.
        let pr = unsafe {
            PageRef::from_raw(core::ptr::NonNull::new(page.as_ptr() as *mut u8).unwrap())
        };
        let maxoffno = pr.max_offset_number();
        for offno in 1..=maxoffno {
            let etup = ElementTupleView { bytes: item_bytes(&pr, offno) };
            if etup.tuple_type() != HNSW_ELEMENT_TUPLE_TYPE
                || etup.deleted() != 0
                || !itemptr_is_valid(etup.heaptid_bytes(0))
            {
                continue;
            }
            let (np, no) = etup.neighbortid();
            let (nbuf, npr_owned);
            let npr: PageRef<'_>;
            if np == blkno {
                nbuf = buf;
                npr = pr;
            } else {
                nbuf = bufmgr::ReadBufferExtended(
                    vs.index,
                    ForkNumber::MAIN_FORKNUM,
                    np,
                    types_storage::storage::ReadBufferMode::Normal,
                    bufmgr::GetAccessStrategy(BufferAccessStrategyType::BasBulkread),
                )?;
                LockBuffer(nbuf, BUFFER_LOCK_SHARE)?;
                let npage = buf_page_bytes(nbuf);
                // SAFETY: share lock held.
                npr_owned = unsafe {
                    PageRef::from_raw(
                        core::ptr::NonNull::new(npage.as_ptr() as *mut u8).unwrap(),
                    )
                };
                npr = npr_owned;
            }
            let ntup = NeighborTupleView { bytes: item_bytes(&npr, no) };
            for i in 0..ntup.count() as usize {
                let tid = ntup.indextid_bytes(i);
                if !itemptr_is_valid(tid) {
                    continue;
                }
                if vs.deleting.contains_key(&itemptr_decode(tid)) {
                    return Err(
                        types_error::PgError::error("hnsw graph not repaired").into()
                    );
                }
            }
            if nbuf != buf {
                UnlockReleaseBuffer(nbuf)?;
            }
        }
        let next = page_opaque_nextblkno(page);
        UnlockReleaseBuffer(buf)?;
        blkno = next;
    }
    Ok(())
}

// MarkDeleted (pass 4).
fn mark_deleted(vs: &mut VacState<'_, '_, '_>) -> PgResult<()> {
    lmgr::LockPage(vs.index, HNSW_UPDATE_LOCK, ExclusiveLock)?;
    lmgr::UnlockPage(vs.index, HNSW_UPDATE_LOCK, ExclusiveLock)?;

    confirm_repaired(vs)?;

    lmgr::LockPage(vs.index, HNSW_SCAN_LOCK, ExclusiveLock)?;
    lmgr::UnlockPage(vs.index, HNSW_SCAN_LOCK, ExclusiveLock)?;

    let mut insert_page = INVALID_BLOCK;
    let mut blkno = HNSW_HEAD_BLKNO;
    while blkno != INVALID_BLOCK {
        vacuum_delay()?;
        let buf = bufmgr::ReadBufferExtended(
            vs.index,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            types_storage::storage::ReadBufferMode::Normal,
            bufmgr::GetAccessStrategy(BufferAccessStrategyType::BasBulkread),
        )?;
        LockBufferForCleanup(buf)?;

        let mut state = GenericXLogStart(vs.op_mcx, vs.index)?;
        let _ = state.register_buffer(buf, 0)?;

        let maxoffno = {
            // SAFETY: cleanup lock held.
            let pr = unsafe {
                PageRef::from_raw(core::ptr::NonNull::new(state.page_image_mut(0).as_mut_ptr()).unwrap())
            };
            pr.max_offset_number()
        };

        for offno in 1..=maxoffno {
            let page_ptr = state.page_image_mut(0).as_mut_ptr();
            // SAFETY: registered image.
            let pr = unsafe { PageRef::from_raw(core::ptr::NonNull::new(page_ptr).unwrap()) };
            let (eoff, elen) = {
                let id = pr.item_id(offno);
                let (p, len) = pr.item_raw(id);
                (p as usize - page_ptr as usize, len as usize)
            };
            let is_element = {
                // SAFETY: item bounds.
                let b = unsafe { core::slice::from_raw_parts(page_ptr.add(eoff), elen) };
                b[0] == HNSW_ELEMENT_TUPLE_TYPE
            };
            if !is_element {
                continue;
            }
            let (deleted, live, np, no) = {
                // SAFETY: item bounds.
                let b = unsafe { core::slice::from_raw_parts(page_ptr.add(eoff), elen) };
                let etup = ElementTupleView { bytes: b };
                (
                    etup.deleted() != 0,
                    itemptr_is_valid(etup.heaptid_bytes(0)),
                    etup.neighbortid().0,
                    etup.neighbortid().1,
                )
            };
            if deleted {
                if insert_page == INVALID_BLOCK {
                    insert_page = blkno;
                }
                continue;
            }
            if live {
                continue;
            }

            // Register neighbor page too when off-page.
            let (nbuf, same_page) = if np == blkno {
                (buf, true)
            } else {
                let nb = bufmgr::ReadBufferExtended(
                    vs.index,
                    ForkNumber::MAIN_FORKNUM,
                    np,
                    types_storage::storage::ReadBufferMode::Normal,
                    bufmgr::GetAccessStrategy(BufferAccessStrategyType::BasBulkread),
                )?;
                LockBuffer(nb, BUFFER_LOCK_EXCLUSIVE)?;
                let _ = state.register_buffer(nb, 0)?;
                (nb, false)
            };

            let new_version = {
                // Mutate element tuple in the registered image.
                let page_ptr = state.page_image_mut(0).as_mut_ptr();
                // SAFETY: item bounds within registered image.
                let b = unsafe { core::slice::from_raw_parts_mut(page_ptr.add(eoff), elen) };
                b[2] = 1;
                let vlen = {
                    let d = &b[ELEMENT_DATA_OFFSET..];
                    (u32::from_ne_bytes(d[0..4].try_into().unwrap()) >> 2) as usize
                };
                for x in b[ELEMENT_DATA_OFFSET..ELEMENT_DATA_OFFSET + vlen].iter_mut() {
                    *x = 0;
                }
                // Rewrite the varlena header (memset also cleared it in C? No —
                // C memsets &etup->data through VARSIZE_ANY(&etup->data), header
                // included; mirror exactly: full clear).
                b[3] = b[3].wrapping_add(1);
                if b[3] > 15 {
                    b[3] = 1;
                }
                b[3]
            };

            {
                let img_idx = if same_page { 0 } else { 1 };
                let npage_ptr = state.page_image_mut(img_idx).as_mut_ptr();
                // SAFETY: registered image.
                let npr =
                    unsafe { PageRef::from_raw(core::ptr::NonNull::new(npage_ptr).unwrap()) };
                let (noff2, nlen2) = {
                    let id = npr.item_id(no);
                    let (p, len) = npr.item_raw(id);
                    (p as usize - npage_ptr as usize, len as usize)
                };
                // SAFETY: item bounds.
                let nb = unsafe {
                    core::slice::from_raw_parts_mut(npage_ptr.add(noff2), nlen2)
                };
                let count = u16::from_ne_bytes(nb[2..4].try_into().unwrap()) as usize;
                for i in 0..count {
                    let off = NEIGHBOR_TIDS_OFFSET + i * 6;
                    nb[off..off + 6]
                        .copy_from_slice(&itemptr_encode(INVALID_BLOCK, INVALID_OFFSET));
                }
                nb[1] = new_version;
            }

            GenericXLogFinish(state)?;
            if !same_page {
                UnlockReleaseBuffer(nbuf)?;
            }
            if insert_page == INVALID_BLOCK {
                insert_page = blkno;
            }
            state = GenericXLogStart(vs.op_mcx, vs.index)?;
            let _ = state.register_buffer(buf, 0)?;
        }

        let next = page_opaque_nextblkno(state.page_image_mut(0));
        GenericXLogAbort(state);
        UnlockReleaseBuffer(buf)?;
        blkno = next;
    }

    update_meta_page(
        vs.index,
        0,
        None,
        insert_page,
        ForkNumber::MAIN_FORKNUM,
        false,
    )
}

pub fn hnswbulkdelete<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    istat: Option<IndexBulkDeleteResult>,
    dead_items: &[ItemPointerData],
) -> PgResult<IndexBulkDeleteResult> {
    let op = mcx::MemoryContext::new_bump("hnsw vacuum");
    let op_mcx = op.mcx();
    let support = init_support(info.index)?;
    let meta = read_meta(info.index)?;
    let mut vs = VacState {
        index: info.index,
        stats: istat.unwrap_or_default(),
        dead_items,
        m: meta.m as i32,
        ef_construction: hnsw_get_ef_construction(info.index),
        support,
        deleting: PgFxHashMap::with_capacity_and_hasher_in(256, Default::default(), op_mcx),
        op_mcx,
        highest: None,
        fallback: None,
    };

    remove_heap_tids(&mut vs)?;
    repair_graph(&mut vs)?;
    mark_deleted(&mut vs)?;

    Ok(vs.stats)
}

// TID-collect form for validate_index: report every live heap TID.
pub fn hnswbulkdelete_collect<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    callback: &mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + '_),
) -> PgResult<IndexBulkDeleteResult> {
    let mut stats = IndexBulkDeleteResult::default();
    let mut blkno = HNSW_HEAD_BLKNO;
    while blkno != INVALID_BLOCK {
        let buf = bufmgr::ReadBuffer(info.index, blkno)?;
        LockBuffer(buf, BUFFER_LOCK_SHARE)?;
        let page = buf_page_bytes(buf);
        // SAFETY: share lock held.
        let pr = unsafe {
            PageRef::from_raw(core::ptr::NonNull::new(page.as_ptr() as *mut u8).unwrap())
        };
        for offno in 1..=pr.max_offset_number() {
            let etup = ElementTupleView { bytes: item_bytes(&pr, offno) };
            if etup.tuple_type() != HNSW_ELEMENT_TUPLE_TYPE || etup.deleted() != 0 {
                continue;
            }
            for i in 0..HNSW_HEAPTIDS {
                let tid = etup.heaptid_bytes(i);
                if !itemptr_is_valid(tid) {
                    break;
                }
                callback(&ipd_from_bytes(tid))?;
                stats.num_index_tuples += 1.0;
            }
        }
        let next = page_opaque_nextblkno(page);
        UnlockReleaseBuffer(buf)?;
        blkno = next;
    }
    Ok(stats)
}

pub fn hnswvacuumcleanup<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    istat: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    if info.analyze_only {
        return Ok(istat);
    }
    let Some(mut stats) = istat else {
        return Ok(None);
    };
    stats.num_pages =
        bufmgr::RelationGetNumberOfBlocksInFork(info.index, ForkNumber::MAIN_FORKNUM)?;
    Ok(Some(stats))
}
