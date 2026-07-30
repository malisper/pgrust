use crate::layout::*;
use crate::utils::*;
use bufmgr::{
    LockBuffer, MarkBufferDirty, UnlockReleaseBuffer, BUFFER_LOCK_EXCLUSIVE,
};
use datum::Datum;
use generic_xlog::{GenericXLogAbort, GenericXLogFinish, GenericXLogStart, GenericXLogState};
use mcx::{Mcx, PgBox, PgFxHashMap, PgVec};
use types_core::{BlockNumber, Buffer, ForkNumber, BLCKSZ};
use types_error::{PgError, PgResult};
use types_hnsw::*;
use types_rel::Relation;
use types_storage::bufpage::{PageMut, PageRef};
use types_storage::lock::{ExclusiveLock, ShareLock, LOCKMODE};
use types_tuple::itemptr::ItemPointerData;

// SAFETY of all raw page ptrs below: either the buffer's page under an
// exclusive content lock (building) or a registered image inside a pinned
// GenericXLogState PgBox (stable address until Finish/Abort).
unsafe fn page_ref<'a>(p: *mut u8) -> PageRef<'a> {
    unsafe { PageRef::from_raw(core::ptr::NonNull::new(p).unwrap()) }
}

unsafe fn page_mut<'a>(p: *mut u8) -> PageMut<'a> {
    unsafe { PageMut::from_raw(core::ptr::NonNull::new(p).unwrap()) }
}

unsafe fn page_bytes_mut<'a>(p: *mut u8) -> &'a mut [u8] {
    unsafe { core::slice::from_raw_parts_mut(p, BLCKSZ) }
}

fn work_page(
    building: bool,
    state: Option<&mut PgBox<'_, GenericXLogState>>,
    buf: Buffer,
    flags: i32,
) -> PgResult<*mut u8> {
    if building {
        Ok(bufmgr_seams::buffer_get_page::call(buf).as_ptr())
    } else {
        let state = state.expect("xlog state when not building");
        Ok(state.register_buffer(buf, flags)?.as_mut_ptr())
    }
}

fn item_bytes_at<'a>(p: *mut u8, offno: u16) -> &'a [u8] {
    // SAFETY: contract above.
    let pr = unsafe { page_ref(p) };
    let id = pr.item_id(offno);
    let (ip, len) = pr.item_raw(id);
    unsafe { core::slice::from_raw_parts(ip, len as usize) }
}

fn item_bytes_at_mut<'a>(p: *mut u8, offno: u16) -> &'a mut [u8] {
    // SAFETY: contract above; exclusive access.
    let pr = unsafe { page_ref(p) };
    let id = pr.item_id(offno);
    let (ip, len) = pr.item_raw(id);
    unsafe { core::slice::from_raw_parts_mut(ip as *mut u8, len as usize) }
}

// GetInsertPage.
fn get_insert_page(index: &Relation<'_>) -> PgResult<BlockNumber> {
    Ok(read_meta(index)?.insert_page)
}

struct FreeOffsetResult {
    nbuf: Buffer,
    npage: *mut u8,
    free_offno: u16,
    free_neighbor_offno: u16,
    tuple_version: u8,
}

// HnswFreeOffset: hunt for a deleted element with room for both tuples.
#[allow(clippy::too_many_arguments)]
fn hnsw_free_offset(
    index: &Relation<'_>,
    buf: Buffer,
    page: *mut u8,
    etup_size: usize,
    ntup_size: usize,
    new_insert_page: &mut BlockNumber,
) -> PgResult<Option<FreeOffsetResult>> {
    // SAFETY: exclusive lock held on buf.
    let pr = unsafe { page_ref(page) };
    let maxoffno = pr.max_offset_number();
    for offno in 1..=maxoffno {
        let etup = ElementTupleView { bytes: item_bytes_at(page, offno) };
        if etup.tuple_type() != HNSW_ELEMENT_TUPLE_TYPE {
            continue;
        }
        if etup.deleted() == 0 {
            continue;
        }
        let element_page = bufmgr::BufferGetBlockNumber(buf);
        let (neighbor_page, neighbor_offno) = etup.neighbortid();

        if *new_insert_page == INVALID_BLOCK {
            *new_insert_page = element_page;
        }

        let (nbuf, npage) = if neighbor_page == element_page {
            (buf, page)
        } else {
            let nbuf = bufmgr::ReadBuffer(index, neighbor_page)?;
            LockBuffer(nbuf, BUFFER_LOCK_EXCLUSIVE)?;
            (nbuf, bufmgr_seams::buffer_get_page::call(nbuf).as_ptr())
        };

        let eitem_len = {
            let id = pr.item_id(offno);
            let (_, len) = pr.item_raw(id);
            len as usize
        };
        // SAFETY: nbuf locked above (or == buf).
        let npr = unsafe { page_ref(npage) };
        let nitem_len = {
            let id = npr.item_id(neighbor_offno);
            let (_, len) = npr.item_raw(id);
            len as usize
        };

        let page_free = eitem_len + pr.exact_free_space();
        let mut npage_free = nitem_len;
        if neighbor_page != element_page {
            npage_free += npr.exact_free_space();
        } else if page_free >= etup_size {
            npage_free += page_free - etup_size;
        }

        if page_free >= etup_size && npage_free >= ntup_size {
            return Ok(Some(FreeOffsetResult {
                nbuf,
                npage,
                free_offno: offno,
                free_neighbor_offno: neighbor_offno,
                tuple_version: etup.version(),
            }));
        } else if nbuf != buf {
            UnlockReleaseBuffer(nbuf)?;
        }
    }
    Ok(None)
}

// HnswInsertAppendPage.
fn insert_append_page(
    index: &Relation<'_>,
    state: Option<&mut PgBox<'_, GenericXLogState>>,
    page: *mut u8,
    building: bool,
) -> PgResult<(Buffer, *mut u8)> {
    lmgr::LockRelationForExtension(index, ExclusiveLock)?;
    let nbuf = new_buffer(index, ForkNumber::MAIN_FORKNUM)?;
    lmgr::UnlockRelationForExtension(index, ExclusiveLock)?;

    let npage = if building {
        bufmgr_seams::buffer_get_page::call(nbuf).as_ptr()
    } else {
        let state = state.expect("xlog state");
        state
            .register_buffer(nbuf, generic_xlog::GENERIC_XLOG_FULL_IMAGE)?
            .as_mut_ptr()
    };
    // SAFETY: nbuf exclusively locked by new_buffer.
    unsafe {
        let mut pm = page_mut(npage);
        pm.init(HNSW_PAGE_OPAQUE_SIZE);
        page_opaque_init(page_bytes_mut(npage));
        page_opaque_set_nextblkno(page_bytes_mut(page), bufmgr::BufferGetBlockNumber(nbuf));
    }
    Ok((nbuf, npage))
}

// AddElementOnDisk.
#[allow(clippy::too_many_arguments)]
fn add_element_on_disk(
    index: &Relation<'_>,
    pool: &mut ElementPool<'_>,
    e_id: u32,
    m: i32,
    insert_page: BlockNumber,
    updated_insert_page: &mut BlockNumber,
    building: bool,
    op_mcx: Mcx<'_>,
) -> PgResult<()> {
    let value_len = pool.get(e_id).value.as_ref().expect("value").len();
    let level = pool.get(e_id).level;
    let etup_size = element_tuple_size(value_len);
    let ntup_size = neighbor_tuple_size(level, m);
    let combined_size = etup_size + ntup_size + SIZE_OF_ITEM_ID;
    let max_size = HNSW_MAX_SIZE;
    let min_combined_size = etup_size + neighbor_tuple_size(0, m) + SIZE_OF_ITEM_ID;

    let mut etup: Vec<u8> = Vec::new();
    set_element_tuple(&mut etup, pool, e_id);
    let mut ntup: Vec<u8> = Vec::new();
    set_neighbor_tuple(&mut ntup, pool, e_id, m);

    let mut current_page = insert_page;
    let mut new_insert_page = INVALID_BLOCK;
    let mut free_offno: u16 = INVALID_OFFSET;
    let mut free_neighbor_offno: u16 = INVALID_OFFSET;

    let mut buf: Buffer;
    let mut page: *mut u8;
    let nbuf: Buffer;
    let npage: *mut u8;
    let mut state: Option<PgBox<'_, GenericXLogState>> = None;

    loop {
        buf = bufmgr::ReadBuffer(index, current_page)?;
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE)?;

        if !building {
            state = Some(GenericXLogStart(op_mcx, index)?);
        }
        page = work_page(building, state.as_mut(), buf, 0)?;

        // SAFETY: exclusive lock held.
        let free = unsafe { page_ref(page) }.free_space();
        if new_insert_page == INVALID_BLOCK && free >= min_combined_size {
            new_insert_page = current_page;
        }

        if free >= combined_size {
            nbuf = buf;
            npage = page;
            break;
        }

        if let Some(fo) =
            hnsw_free_offset(index, buf, page, etup_size, ntup_size, &mut new_insert_page)?
        {
            nbuf = fo.nbuf;
            if nbuf != buf {
                npage = if building {
                    fo.npage
                } else {
                    state
                        .as_mut()
                        .expect("xlog")
                        .register_buffer(nbuf, 0)?
                        .as_mut_ptr()
                };
            } else {
                npage = page;
            }
            free_offno = fo.free_offno;
            free_neighbor_offno = fo.free_neighbor_offno;
            etup[3] = fo.tuple_version;
            ntup[1] = fo.tuple_version;
            break;
        }

        let next_blkno = page_opaque_nextblkno(
            // SAFETY: lock held.
            unsafe { core::slice::from_raw_parts(page, BLCKSZ) },
        );

        if combined_size > max_size
            && unsafe { page_ref(page) }.free_space() >= etup_size
            && next_blkno == INVALID_BLOCK
        {
            let (b, p) = insert_append_page(index, state.as_mut(), page, building)?;
            nbuf = b;
            npage = p;
            break;
        }

        if next_blkno != INVALID_BLOCK {
            current_page = next_blkno;
            if let Some(st) = state.take() {
                GenericXLogAbort(st);
            }
            UnlockReleaseBuffer(buf)?;
        } else {
            let (newbuf, _newpage) = insert_append_page(index, state.as_mut(), page, building)?;

            if building {
                MarkBufferDirty(buf)?;
            } else {
                GenericXLogFinish(state.take().expect("xlog"))?;
            }
            UnlockReleaseBuffer(buf)?;

            buf = newbuf;
            if !building {
                state = Some(GenericXLogStart(op_mcx, index)?);
            }
            page = work_page(building, state.as_mut(), buf, 0)?;

            // SAFETY: newbuf locked.
            if unsafe { page_ref(page) }.free_space() < combined_size {
                let (b, p) = insert_append_page(index, state.as_mut(), page, building)?;
                nbuf = b;
                npage = p;
            } else {
                nbuf = buf;
                npage = page;
            }
            break;
        }
    }

    {
        let e = pool.get_mut(e_id);
        e.blkno = bufmgr::BufferGetBlockNumber(buf);
        e.neighbor_page = bufmgr::BufferGetBlockNumber(nbuf);
        if new_insert_page == INVALID_BLOCK {
            new_insert_page = e.neighbor_page;
        }

        if free_offno != INVALID_OFFSET {
            e.offno = free_offno;
            e.neighbor_offno = free_neighbor_offno;
        } else {
            // SAFETY: locks held.
            e.offno = unsafe { page_ref(page) }.max_offset_number() + 1;
            if nbuf == buf {
                e.neighbor_offno = e.offno + 1;
            } else {
                e.neighbor_offno = 1;
            }
        }
        let nb = itemptr_encode(e.neighbor_page, e.neighbor_offno);
        etup[4 + HNSW_HEAPTIDS * 6..4 + HNSW_HEAPTIDS * 6 + 6].copy_from_slice(&nb);
    }
    let (e_offno, e_neighbor_offno) = {
        let e = pool.get(e_id);
        (e.offno, e.neighbor_offno)
    };

    // SAFETY: exclusive locks held on both pages.
    unsafe {
        if free_offno != INVALID_OFFSET {
            if !page_mut(page).index_tuple_overwrite(e_offno, &etup) {
                return Err(add_item_failed(index));
            }
            if !page_mut(npage).index_tuple_overwrite(e_neighbor_offno, &ntup) {
                return Err(add_item_failed(index));
            }
        } else {
            if page_mut(page).add_item(&etup, 0, 0) != Some(e_offno) {
                return Err(add_item_failed(index));
            }
            if page_mut(npage).add_item(&ntup, 0, 0) != Some(e_neighbor_offno) {
                return Err(add_item_failed(index));
            }
        }
    }

    if building {
        MarkBufferDirty(buf)?;
        if nbuf != buf {
            MarkBufferDirty(nbuf)?;
        }
    } else {
        GenericXLogFinish(state.take().expect("xlog"))?;
    }
    UnlockReleaseBuffer(buf)?;
    if nbuf != buf {
        UnlockReleaseBuffer(nbuf)?;
    }

    if new_insert_page != INVALID_BLOCK && new_insert_page != insert_page {
        *updated_insert_page = new_insert_page;
    }
    Ok(())
}

#[track_caller]
#[cold]
fn add_item_failed(index: &Relation<'_>) -> Box<PgError> {
    PgError::error(format!(
        "failed to add index item to \"{}\"",
        index.name()
    ))
    .into()
}

// HnswLoadNeighbors (layer lc of an existing element).
fn load_neighbors(
    pool: &mut ElementPool<'_>,
    element: u32,
    index: &Relation<'_>,
    m: i32,
    lm: i32,
    lc: i32,
) -> PgResult<Vec<Candidate>> {
    let mut out: Vec<Candidate> = Vec::with_capacity(lm as usize);
    let mut tids = [[0u8; 6]; 200];
    if !load_neighbor_tids(pool, element, &mut tids[..lm as usize], index, m, lm, lc)? {
        return Ok(out);
    }
    for tid in tids[..lm as usize].iter() {
        if !itemptr_is_valid(tid) {
            break;
        }
        let (blkno, offno) = itemptr_decode(tid);
        let id = pool.from_block(blkno, offno);
        out.push(Candidate { element: id, distance: 0.0, closer: false });
    }
    Ok(out)
}

// GetUpdateIndex.
#[allow(clippy::too_many_arguments)]
fn get_update_index(
    pool: &mut ElementPool<'_>,
    element: u32,
    new_element: u32,
    distance: f32,
    m: i32,
    lm: i32,
    lc: i32,
    index: &Relation<'_>,
    support: &mut HnswSupport,
) -> PgResult<i32> {
    let mut idx: i32 = -1;
    let mut neighbors = load_neighbors(pool, element, index, m, lm, lc)?;

    if (neighbors.len() as i32) < lm {
        idx = -2;
    } else {
        // LoadElementsForInsert: q = the neighbor-owner's value.
        load_element(pool, element, None, None, index, support, true, None)?;
        let q = Some(pool.value_datum(element));
        for (i, hc) in neighbors.iter_mut().enumerate() {
            let mut d = 0.0f64;
            load_element(pool, hc.element, Some(&mut d), q, index, support, true, None)?;
            hc.distance = d as f32;
            // Prune element if being deleted: idx = its slot (C LoadElementsForInsert).
            if pool.get(hc.element).heaptids_len == 0 {
                idx = i as i32;
                break;
            }
        }
        if idx == -1 {
            let mut closer_set = false;
            update_connection(
                pool,
                &mut neighbors,
                &mut closer_set,
                new_element,
                distance,
                lm,
                Some(&mut idx),
                support,
            )?;
        }
    }
    Ok(idx)
}

// ConnectionExists.
fn connection_exists(
    pool: &ElementPool<'_>,
    e: u32,
    ntup_bytes: &[u8],
    start_idx: i32,
    lm: i32,
) -> bool {
    let ntup = NeighborTupleView { bytes: ntup_bytes };
    let e = pool.get(e);
    for i in 0..lm {
        let tid = ntup.indextid_bytes((start_idx + i) as usize);
        if !itemptr_is_valid(tid) {
            break;
        }
        let (blkno, offno) = itemptr_decode(tid);
        if blkno == e.blkno && offno == e.offno {
            return true;
        }
    }
    false
}

// UpdateNeighborOnDisk.
#[allow(clippy::too_many_arguments)]
fn update_neighbor_on_disk(
    pool: &ElementPool<'_>,
    element: u32,
    new_element: u32,
    mut idx: i32,
    m: i32,
    lm: i32,
    lc: i32,
    index: &Relation<'_>,
    check_existing: bool,
    building: bool,
    op_mcx: Mcx<'_>,
) -> PgResult<()> {
    let (neighbor_page, offno, elem_level) = {
        let e = pool.get(element);
        (e.neighbor_page, e.neighbor_offno, e.level as i32)
    };
    let buf = bufmgr::ReadBuffer(index, neighbor_page)?;
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE)?;
    let mut state: Option<PgBox<'_, GenericXLogState>> = None;
    if !building {
        state = Some(GenericXLogStart(op_mcx, index)?);
    }
    let page = work_page(building, state.as_mut(), buf, 0)?;

    let ntup_bytes = item_bytes_at_mut(page, offno);
    let start_idx = (elem_level - lc) * m;

    if check_existing && connection_exists(pool, new_element, ntup_bytes, start_idx, lm) {
        idx = -1;
    } else if idx == -2 {
        idx = -1;
        for j in 0..lm {
            let tid_off = NEIGHBOR_TIDS_OFFSET + ((start_idx + j) as usize) * 6;
            if !itemptr_is_valid(&ntup_bytes[tid_off..tid_off + 6]) {
                idx = start_idx + j;
                break;
            }
        }
    } else if idx >= 0 {
        idx += start_idx;
    }

    let count = NeighborTupleView { bytes: ntup_bytes }.count() as i32;
    if idx >= 0 && idx < count {
        let ne = pool.get(new_element);
        let tid_off = NEIGHBOR_TIDS_OFFSET + (idx as usize) * 6;
        ntup_bytes[tid_off..tid_off + 6]
            .copy_from_slice(&itemptr_encode(ne.blkno, ne.offno));
        if building {
            MarkBufferDirty(buf)?;
        } else {
            GenericXLogFinish(state.take().expect("xlog"))?;
        }
    } else if !building {
        GenericXLogAbort(state.take().expect("xlog"));
    }
    UnlockReleaseBuffer(buf)?;
    Ok(())
}

// HnswUpdateNeighborsOnDisk.
pub fn update_neighbors_on_disk(
    index: &Relation<'_>,
    support: &mut HnswSupport,
    pool: &mut ElementPool<'_>,
    e_id: u32,
    m: i32,
    check_existing: bool,
    building: bool,
    op_mcx: Mcx<'_>,
) -> PgResult<()> {
    let level = pool.get(e_id).level as i32;
    for lc in (0..=level).rev() {
        let lm = hnsw_get_layer_m(m, lc);
        let layer_idx = (level - lc) as usize;
        let items: Vec<Candidate> = pool
            .get(e_id)
            .neighbors
            .as_ref()
            .expect("neighbors")[layer_idx]
            .items
            .iter()
            .copied()
            .collect();
        for hc in items {
            let idx = get_update_index(
                pool, hc.element, e_id, hc.distance, m, lm, lc, index, support,
            )?;
            if idx == -1 {
                continue;
            }
            update_neighbor_on_disk(
                pool, hc.element, e_id, idx, m, lm, lc, index, check_existing, building, op_mcx,
            )?;
        }
    }
    Ok(())
}

// AddDuplicateOnDisk.
fn add_duplicate_on_disk(
    index: &Relation<'_>,
    pool: &ElementPool<'_>,
    element: u32,
    dup: u32,
    building: bool,
    op_mcx: Mcx<'_>,
) -> PgResult<bool> {
    let (dup_blkno, dup_offno) = {
        let d = pool.get(dup);
        (d.blkno, d.offno)
    };
    let buf = bufmgr::ReadBuffer(index, dup_blkno)?;
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE)?;
    let mut state: Option<PgBox<'_, GenericXLogState>> = None;
    if !building {
        state = Some(GenericXLogStart(op_mcx, index)?);
    }
    let page = work_page(building, state.as_mut(), buf, 0)?;

    let etup_bytes = item_bytes_at_mut(page, dup_offno);
    let mut i = 0usize;
    while i < HNSW_HEAPTIDS {
        if !itemptr_is_valid(&etup_bytes[4 + i * 6..4 + i * 6 + 6]) {
            break;
        }
        i += 1;
    }
    if i == 0 || i == HNSW_HEAPTIDS {
        if let Some(st) = state.take() {
            GenericXLogAbort(st);
        }
        UnlockReleaseBuffer(buf)?;
        return Ok(false);
    }
    let tid = ipd_to_bytes(&pool.get(element).heaptids[0]);
    etup_bytes[4 + i * 6..4 + i * 6 + 6].copy_from_slice(&tid);
    if building {
        MarkBufferDirty(buf)?;
    } else {
        GenericXLogFinish(state.take().expect("xlog"))?;
    }
    UnlockReleaseBuffer(buf)?;
    Ok(true)
}

// FindDuplicateOnDisk.
fn find_duplicate_on_disk(
    index: &Relation<'_>,
    pool: &mut ElementPool<'_>,
    element: u32,
    building: bool,
    op_mcx: Mcx<'_>,
) -> PgResult<bool> {
    let level = pool.get(element).level as i32;
    let layer0_idx = level as usize;
    let items: Vec<Candidate> = pool
        .get(element)
        .neighbors
        .as_ref()
        .expect("neighbors")[layer0_idx]
        .items
        .iter()
        .copied()
        .collect();
    let value: Vec<u8> = pool.get(element).value.as_ref().expect("value").to_vec();
    for neighbor in items {
        let nvalue = pool
            .get(neighbor.element)
            .value
            .as_ref()
            .expect("neighbor value loaded (inserting search keeps vecs)")
            .to_vec();
        // datumIsEqual(value, neighborValue, false, -1): whole-image compare.
        if value != nvalue {
            return Ok(false);
        }
        if add_duplicate_on_disk(index, pool, element, neighbor.element, building, op_mcx)? {
            return Ok(true);
        }
    }
    Ok(false)
}

// UpdateGraphOnDisk.
#[allow(clippy::too_many_arguments)]
fn update_graph_on_disk(
    index: &Relation<'_>,
    support: &mut HnswSupport,
    pool: &mut ElementPool<'_>,
    element: u32,
    m: i32,
    entry_point: Option<u32>,
    building: bool,
    op_mcx: Mcx<'_>,
) -> PgResult<()> {
    if find_duplicate_on_disk(index, pool, element, building, op_mcx)? {
        return Ok(());
    }

    let insert_page = get_insert_page(index)?;
    let mut new_insert_page = INVALID_BLOCK;
    add_element_on_disk(
        index,
        pool,
        element,
        m,
        insert_page,
        &mut new_insert_page,
        building,
        op_mcx,
    )?;
    if new_insert_page != INVALID_BLOCK {
        update_meta_page(
            index,
            0,
            None,
            new_insert_page,
            ForkNumber::MAIN_FORKNUM,
            building,
        )?;
    }

    update_neighbors_on_disk(index, support, pool, element, m, false, building, op_mcx)?;

    let promote = match entry_point {
        None => true,
        Some(ep) => pool.get(element).level > pool.get(ep).level,
    };
    if promote {
        let e = pool.get(element);
        update_meta_page(
            index,
            HNSW_UPDATE_ENTRY_GREATER,
            Some((e.blkno, e.offno, e.level as i16)),
            INVALID_BLOCK,
            ForkNumber::MAIN_FORKNUM,
            building,
        )?;
    }
    Ok(())
}

// HnswInitElement's level draw.
pub fn random_level(ml: f64, max_level: i32) -> u8 {
    let r = pg_prng::global_prng(|p| p.next_f64());
    let mut level = (-(r.ln()) * ml) as i32;
    if level > max_level {
        level = max_level;
    }
    level as u8
}

// HnswInsertTupleOnDisk. `value` is a detoasted (possibly normalized) image.
pub fn insert_tuple_on_disk(
    index: &Relation<'_>,
    support: &mut HnswSupport,
    value: &[u8],
    heaptid: &ItemPointerData,
    building: bool,
) -> PgResult<bool> {
    let op = mcx::MemoryContext::new_bump("hnsw insert on disk");
    let op_mcx = op.mcx();
    let mut lockmode: LOCKMODE = ShareLock;

    lmgr::LockPage(index, HNSW_UPDATE_LOCK, lockmode)?;

    let meta = read_meta(index)?;
    let m = meta.m as i32;
    let ef_construction = hnsw_get_ef_construction(index);
    let mut entry = meta_entry_point(&meta);

    let mut pool = ElementPool::new(op_mcx);
    let e_id = pool.from_block(INVALID_BLOCK, INVALID_OFFSET);
    {
        let mut img: PgVec<'_, u8> = mcx::vec_with_capacity_in_infallible(op_mcx, value.len());
        let _ = mcx::vec_append_bytes(&mut img, value);
        let e = pool.get_mut(e_id);
        e.level = random_level(hnsw_get_ml(m), crate::layout::hnsw_get_max_level(m));
        e.version = 1;
        e.heaptids[0] = *heaptid;
        e.heaptids_len = 1;
        e.value = Some(img);
    }

    let promote = match &entry {
        None => true,
        Some(ep) => pool.get(e_id).level > ep.level,
    };
    if promote {
        lmgr::UnlockPage(index, HNSW_UPDATE_LOCK, lockmode)?;
        lockmode = ExclusiveLock;
        lmgr::LockPage(index, HNSW_UPDATE_LOCK, lockmode)?;
        entry = meta_entry_point(&read_meta(index)?);
    }

    let entry_id = entry.as_ref().map(|ep| {
        let id = pool.from_block(ep.blkno, ep.offno);
        pool.get_mut(id).level = ep.level;
        id
    });

    find_element_neighbors(
        &mut pool,
        e_id,
        entry_id,
        index,
        support,
        m,
        ef_construction,
        false,
    )?;
    update_graph_on_disk(
        index, support, &mut pool, e_id, m, entry_id, building, op_mcx,
    )?;

    lmgr::UnlockPage(index, HNSW_UPDATE_LOCK, lockmode)?;
    Ok(true)
}

// HnswFormIndexValue for the vector opclasses: detoast + norm-gate + normalize.
pub fn form_index_value<'m>(
    mcx: Mcx<'m>,
    value: Datum,
    support: &mut HnswSupport,
) -> PgResult<Option<PgVec<'m, u8>>> {
    let p = value.as_usize() as *const u8;
    // SAFETY: non-null vector varlena datum (isnull gated by caller).
    let raw = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let flat = detoast::detoast_attr(mcx, raw)?;
    let mut img: PgVec<'m, u8> = mcx::vec_with_capacity_in_infallible(mcx, flat.len());
    let _ = mcx::vec_append_bytes(&mut img, &flat);

    if support.normprocinfo.is_some() {
        // HnswCheckNorm.
        let d = Datum::from_usize(img.as_ptr() as usize);
        let norm = {
            let np = support.normprocinfo.as_mut().expect("some");
            types_fmgr::function_call1_coll(np, support.collation, d)?.as_f64()
        };
        if !(norm > 0.0) {
            return Ok(None);
        }
        img = crate::scan::norm_value(mcx, &img)?;
    }
    Ok(Some(img))
}

// hnswinsert.
pub fn hnswinsert<'mcx>(
    _mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    values: &[Datum],
    isnull: &[bool],
    heap_tid: &ItemPointerData,
    _heap: &Relation<'mcx>,
) -> PgResult<bool> {
    if isnull.first().copied().unwrap_or(true) {
        return Ok(false);
    }
    check_type_supported(index)?;
    let insert_ctx = mcx::MemoryContext::new_bump("Hnsw insert temporary context");
    let imcx = insert_ctx.mcx();

    let mut support = init_support(index)?;
    let Some(img) = form_index_value(imcx, values[0], &mut support)? else {
        return Ok(false);
    };
    insert_tuple_on_disk(index, &mut support, &img, heap_tid, false)?;
    Ok(false)
}
