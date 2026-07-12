use crate::layout::*;
use bufmgr::{
    LockBuffer, MarkBufferDirty, UnlockReleaseBuffer, BUFFER_LOCK_EXCLUSIVE,
    BUFFER_LOCK_SHARE,
};
use datum::Datum;
use mcx::{Mcx, PgFxHashMap, PgVec};
use types_core::{BlockNumber, Buffer, ForkNumber, Oid};
use types_error::{PgError, PgResult};
use types_hnsw::*;
use types_rel::Relation;
use types_storage::bufpage::{PageMut, PageRef};
use types_tuple::itemptr::{ItemPointerData, ItemPointerIsValid};

pub fn hnsw_get_m(index: &Relation<'_>) -> i32 {
    index
        .rd_options
        .as_ref()
        .and_then(|o| o.hnsw())
        .map(|o| o.m)
        .unwrap_or(HNSW_DEFAULT_M)
}

pub fn hnsw_get_ef_construction(index: &Relation<'_>) -> i32 {
    index
        .rd_options
        .as_ref()
        .and_then(|o| o.hnsw())
        .map(|o| o.ef_construction)
        .unwrap_or(HNSW_DEFAULT_EF_CONSTRUCTION)
}

const HNSW_NPROCS: usize = 3;

fn index_getprocid(index: &Relation<'_>, procnum: u16) -> Oid {
    index
        .rd_support
        .get(procnum as usize - 1)
        .copied()
        .unwrap_or(0)
}

pub fn init_support(index: &Relation<'_>) -> PgResult<HnswSupport> {
    let _ = HNSW_NPROCS;
    let dist = index_getprocid(index, HNSW_DISTANCE_PROC);
    if dist == 0 {
        return Err(PgError::error(format!(
            "missing support function 1 for index \"{}\"",
            index.name()
        ))
        .into());
    }
    let norm = index_getprocid(index, HNSW_NORM_PROC);
    Ok(HnswSupport {
        procinfo: fmgr_seams::fmgr_info::call(dist)?,
        normprocinfo: if norm != 0 {
            Some(fmgr_seams::fmgr_info::call(norm)?)
        } else {
            None
        },
        collation: index.rd_indcollation.first().copied().unwrap_or(0),
    })
}

// HnswGetTypeInfo: proc 3 (halfvec/bit/sparsevec) is unported — vector only.
// Unreachable through the trimmed vector--0.8.5.sql (no such opclasses), but
// error rather than panic if a foreign catalog ever presents one.
pub fn check_type_supported(index: &Relation<'_>) -> PgResult<i32> {
    if index_getprocid(index, HNSW_TYPE_INFO_PROC) != 0 {
        return Err(PgError::error(
            "hnsw type-info opclasses (halfvec/bit/sparsevec) are not supported",
        )
        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
        .into());
    }
    Ok(HNSW_MAX_DIM as i32)
}

#[inline]
pub fn buf_page_mut(buffer: Buffer) -> PageMut<'static> {
    // SAFETY: caller holds the content lock required for its access mode.
    unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) }
}

#[inline]
pub fn buf_page_bytes<'a>(buffer: Buffer) -> &'a [u8] {
    // SAFETY: caller holds at least a share lock.
    unsafe {
        core::slice::from_raw_parts(
            bufmgr_seams::buffer_get_page::call(buffer).as_ptr(),
            types_core::BLCKSZ,
        )
    }
}

#[inline]
pub fn buf_page_bytes_mut<'a>(buffer: Buffer) -> &'a mut [u8] {
    // SAFETY: caller holds the exclusive content lock.
    unsafe {
        core::slice::from_raw_parts_mut(
            bufmgr_seams::buffer_get_page::call(buffer).as_ptr(),
            types_core::BLCKSZ,
        )
    }
}

pub fn item_bytes<'a>(page: &PageRef<'a>, offno: u16) -> &'a [u8] {
    let id = page.item_id(offno);
    let (p, len) = page.item_raw(id);
    // SAFETY: item_raw bounds the item within the page (lock held by caller).
    unsafe { core::slice::from_raw_parts(p, len as usize) }
}

// HnswNewBuffer: ReadBufferExtended(P_NEW) + exclusive lock.
pub fn new_buffer(index: &Relation<'_>, fork_num: ForkNumber) -> PgResult<Buffer> {
    let (buffer, extended_by) = bufmgr_seams::extend_buffered_rel_by::call(
        index,
        fork_num,
        None,
        bufmgr_seams::EB_LOCK_FIRST,
        1,
    )?;
    debug_assert!(extended_by == 1);
    Ok(buffer)
}

// HnswInitPage.
pub fn init_page(buffer: Buffer) {
    let mut pm = buf_page_mut(buffer);
    pm.init(HNSW_PAGE_OPAQUE_SIZE);
    page_opaque_init(buf_page_bytes_mut(buffer));
}

pub fn init_page_bytes(page: &mut [u8; types_core::BLCKSZ]) {
    // SAFETY: exclusive access to the registered image.
    let mut pm =
        unsafe { PageMut::from_raw(core::ptr::NonNull::new(page.as_mut_ptr()).unwrap()) };
    pm.init(HNSW_PAGE_OPAQUE_SIZE);
    page_opaque_init(page);
}

// Metapage contents start at pd_lower base (PageGetContents = MAXALIGNed header).
pub const PAGE_CONTENTS_OFF: usize = SIZE_OF_PAGE_HEADER;

pub fn read_meta(index: &Relation<'_>) -> PgResult<MetaPage> {
    let buf = bufmgr::ReadBuffer(index, HNSW_METAPAGE_BLKNO)?;
    LockBuffer(buf, BUFFER_LOCK_SHARE)?;
    let page = buf_page_bytes(buf);
    let meta = MetaPage::read(&page[PAGE_CONTENTS_OFF..PAGE_CONTENTS_OFF + METAPAGE_SIZE]);
    UnlockReleaseBuffer(buf)?;
    if meta.magic_number != HNSW_MAGIC_NUMBER {
        return Err(PgError::error("hnsw index is not valid").into());
    }
    Ok(meta)
}

pub struct EntryPoint {
    pub blkno: BlockNumber,
    pub offno: u16,
    pub level: u8,
}

pub fn meta_entry_point(meta: &MetaPage) -> Option<EntryPoint> {
    if meta.entry_blkno != INVALID_BLOCK {
        Some(EntryPoint {
            blkno: meta.entry_blkno,
            offno: meta.entry_offno,
            level: meta.entry_level as u8,
        })
    } else {
        None
    }
}

// HnswUpdateMetaPage.
pub fn update_meta_page(
    index: &Relation<'_>,
    update_entry: i32,
    entry_point: Option<(BlockNumber, u16, i16)>,
    insert_page: BlockNumber,
    fork_num: ForkNumber,
    building: bool,
) -> PgResult<()> {
    let buf = bufmgr::ReadBufferExtended(
        index,
        fork_num,
        HNSW_METAPAGE_BLKNO,
        types_storage::storage::ReadBufferMode::Normal,
        None,
    )?;
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE)?;

    let apply = |contents: &mut [u8]| {
        let mut meta = MetaPage::read(&contents[..METAPAGE_SIZE]);
        if update_entry != 0 {
            match entry_point {
                None => {
                    meta.entry_blkno = INVALID_BLOCK;
                    meta.entry_offno = INVALID_OFFSET;
                    meta.entry_level = -1;
                }
                Some((blkno, offno, level)) => {
                    if level > meta.entry_level || update_entry == HNSW_UPDATE_ENTRY_ALWAYS {
                        meta.entry_blkno = blkno;
                        meta.entry_offno = offno;
                        meta.entry_level = level;
                    }
                }
            }
        }
        if insert_page != INVALID_BLOCK {
            meta.insert_page = insert_page;
        }
        meta.write(&mut contents[..METAPAGE_SIZE]);
    };

    if building {
        apply(&mut buf_page_bytes_mut(buf)[PAGE_CONTENTS_OFF..]);
        MarkBufferDirty(buf)?;
    } else {
        let scratch = mcx::MemoryContext::new_bump("hnsw meta xlog");
        let state = generic_xlog::GenericXLogStart(scratch.mcx(), index)?;
        let mut state = state;
        let page = state.register_buffer(buf, 0)?;
        apply(&mut page[PAGE_CONTENTS_OFF..]);
        generic_xlog::GenericXLogFinish(state)?;
    }
    UnlockReleaseBuffer(buf)?;
    Ok(())
}

// ---- element working set for on-disk operations ----

// Droppy payloads (nested PgVec) skip the no-drop arena helpers (skey_vec
// precedent); the bump arena still backs the allocation.
pub fn droppy_vec<'m, T>(mcx: Mcx<'m>, cap: usize) -> PgVec<'m, T> {
    let mut v: PgVec<'m, T> = PgVec::new_in(mcx);
    let _ = v.try_reserve(cap);
    v
}

pub struct Element<'t> {
    pub blkno: BlockNumber,
    pub offno: u16,
    pub level: u8,
    pub version: u8,
    pub deleted: u8,
    pub heaptids: [ItemPointerData; HNSW_HEAPTIDS],
    pub heaptids_len: u8,
    pub neighbor_page: BlockNumber,
    pub neighbor_offno: u16,
    pub value: Option<PgVec<'t, u8>>,
    // Per-layer neighbor lists (index level..0), populated only for the
    // element being inserted/repaired.
    pub neighbors: Option<PgVec<'t, NeighborArray<'t>>>,
}

#[derive(Clone, Copy)]
pub struct Candidate {
    pub element: u32,
    pub distance: f32,
    pub closer: bool,
}

pub struct NeighborArray<'t> {
    pub items: PgVec<'t, Candidate>,
    pub closer_set: bool,
}

pub struct ElementPool<'t> {
    pub mcx: Mcx<'t>,
    pub elems: PgVec<'t, Element<'t>>,
}

impl<'t> ElementPool<'t> {
    pub fn new(mcx: Mcx<'t>) -> Self {
        ElementPool { mcx, elems: droppy_vec(mcx, 16) }
    }

    pub fn from_block(&mut self, blkno: BlockNumber, offno: u16) -> u32 {
        self.elems.push(Element {
            blkno,
            offno,
            level: 0,
            version: 0,
            deleted: 0,
            heaptids: [ItemPointerData::invalid(); HNSW_HEAPTIDS],
            heaptids_len: 0,
            neighbor_page: INVALID_BLOCK,
            neighbor_offno: INVALID_OFFSET,
            value: None,
            neighbors: None,
        });
        (self.elems.len() - 1) as u32
    }

    pub fn get(&self, id: u32) -> &Element<'t> {
        &self.elems[id as usize]
    }

    pub fn get_mut(&mut self, id: u32) -> &mut Element<'t> {
        &mut self.elems[id as usize]
    }

    pub fn value_datum(&self, id: u32) -> Datum {
        Datum::from_usize(
            self.elems[id as usize]
                .value
                .as_ref()
                .expect("element value loaded")
                .as_ptr() as usize,
        )
    }
}

// HnswGetDistance.
pub fn get_distance(support: &mut HnswSupport, a: Datum, b: Datum) -> PgResult<f64> {
    Ok(
        types_fmgr::function_call2_coll(&mut support.procinfo, support.collation, a, b)?
            .as_f64(),
    )
}

// HnswLoadElementFromTuple over a pool element.
pub fn load_element_from_tuple(
    pool: &mut ElementPool<'_>,
    id: u32,
    etup: &ElementTupleView<'_>,
    load_heaptids: bool,
    load_vec: bool,
) {
    let (npage, noff) = etup.neighbortid();
    let value = if load_vec {
        let img = etup.value_image();
        let mut v: PgVec<'_, u8> = mcx::vec_with_capacity_in_infallible(pool.mcx, img.len());
        let _ = mcx::vec_append_bytes(&mut v, img);
        Some(v)
    } else {
        None
    };
    let e = pool.get_mut(id);
    e.level = etup.level();
    e.deleted = etup.deleted();
    e.version = etup.version();
    e.neighbor_page = npage;
    e.neighbor_offno = noff;
    e.heaptids_len = 0;
    if load_heaptids {
        for i in 0..HNSW_HEAPTIDS {
            let tid = etup.heaptid_bytes(i);
            if !itemptr_is_valid(tid) {
                break;
            }
            e.heaptids[e.heaptids_len as usize] = ipd_from_bytes(tid);
            e.heaptids_len += 1;
        }
    }
    if load_vec {
        e.value = value;
    }
}

// HnswLoadElementImpl. Returns Some(id) when the element was (re)loaded.
#[allow(clippy::too_many_arguments)]
pub fn load_element(
    pool: &mut ElementPool<'_>,
    id: u32,
    distance: Option<&mut f64>,
    q: Option<Datum>,
    index: &Relation<'_>,
    support: &mut HnswSupport,
    load_vec: bool,
    max_distance: Option<f64>,
) -> PgResult<bool> {
    let (blkno, offno) = {
        let e = pool.get(id);
        (e.blkno, e.offno)
    };
    let buf = bufmgr::ReadBuffer(index, blkno)?;
    LockBuffer(buf, BUFFER_LOCK_SHARE)?;
    let page_bytes = buf_page_bytes(buf);
    // SAFETY: share lock held; PageRef borrows the locked page image.
    let page = unsafe {
        PageRef::from_raw(core::ptr::NonNull::new(page_bytes.as_ptr() as *mut u8).unwrap())
    };
    let etup = ElementTupleView { bytes: item_bytes(&page, offno) };
    debug_assert!(etup.tuple_type() == HNSW_ELEMENT_TUPLE_TYPE);
    if etup.deleted() != 0 {
        UnlockReleaseBuffer(buf)?;
        return Err(PgError::error("cannot load deleted element").into());
    }

    let mut dist_val = 0.0f64;
    let have_distance = if distance.is_some() {
        match q {
            None => {
                dist_val = 0.0;
                true
            }
            Some(qd) => {
                let vd = Datum::from_usize(etup.value_image().as_ptr() as usize);
                dist_val = get_distance(support, qd, vd)?;
                true
            }
        }
    } else {
        false
    };

    let loaded = if !have_distance || max_distance.is_none() || dist_val < max_distance.unwrap() {
        load_element_from_tuple(pool, id, &etup, true, load_vec);
        true
    } else {
        false
    };

    if let Some(d) = distance {
        *d = dist_val;
    }
    UnlockReleaseBuffer(buf)?;
    Ok(loaded)
}

// HnswLoadNeighborTids: copies layer-lc TIDs; false when version/count mismatch.
pub fn load_neighbor_tids(
    pool: &ElementPool<'_>,
    id: u32,
    indextids: &mut [[u8; 6]],
    index: &Relation<'_>,
    m: i32,
    lm: i32,
    lc: i32,
) -> PgResult<bool> {
    let e = pool.get(id);
    let buf = bufmgr::ReadBuffer(index, e.neighbor_page)?;
    LockBuffer(buf, BUFFER_LOCK_SHARE)?;
    let page_bytes = buf_page_bytes(buf);
    // SAFETY: share lock held.
    let page = unsafe {
        PageRef::from_raw(core::ptr::NonNull::new(page_bytes.as_ptr() as *mut u8).unwrap())
    };
    let ntup = NeighborTupleView { bytes: item_bytes(&page, e.neighbor_offno) };

    if ntup.version() != e.version || ntup.count() as i32 != (e.level as i32 + 2) * m {
        UnlockReleaseBuffer(buf)?;
        return Ok(false);
    }
    let start = (e.level as i32 - lc) * m;
    for i in 0..lm as usize {
        indextids[i]
            .copy_from_slice(ntup.indextid_bytes(start as usize + i));
    }
    UnlockReleaseBuffer(buf)?;
    Ok(true)
}

pub struct SearchCandidate {
    pub element: u32,
    pub distance: f64,
}

pub type Visited<'v> = PgFxHashMap<'v, (BlockNumber, u16), ()>;

pub struct DiscardedHeap<'t> {
    pub items: PgVec<'t, SearchCandidate>,
}

impl<'t> DiscardedHeap<'t> {
    pub fn new(mcx: Mcx<'t>) -> Self {
        DiscardedHeap { items: mcx::vec_with_capacity_in_infallible(mcx, 0) }
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    pub fn push(&mut self, sc: SearchCandidate) {
        self.items.push(sc);
        let mut i = self.items.len() - 1;
        while i > 0 {
            let parent = (i - 1) / 2;
            if self.items[i].distance < self.items[parent].distance {
                self.items.swap(i, parent);
                i = parent;
            } else {
                break;
            }
        }
    }
    pub fn pop(&mut self) -> Option<SearchCandidate> {
        if self.items.is_empty() {
            return None;
        }
        let last = self.items.len() - 1;
        self.items.swap(0, last);
        let out = self.items.pop();
        let n = self.items.len();
        let mut i = 0;
        loop {
            let (l, r) = (2 * i + 1, 2 * i + 2);
            let mut sm = i;
            if l < n && self.items[l].distance < self.items[sm].distance {
                sm = l;
            }
            if r < n && self.items[r].distance < self.items[sm].distance {
                sm = r;
            }
            if sm == i {
                break;
            }
            self.items.swap(i, sm);
            i = sm;
        }
        out
    }
}

// Binary max/min heaps over (distance, candidate index): C's pairingheaps C
// (nearest-first) and W (furthest-first) in HnswSearchLayer.
struct MinHeapF {
    v: Vec<(f64, u32)>,
}
struct MaxHeapF {
    v: Vec<(f64, u32)>,
}

impl MinHeapF {
    fn new() -> Self {
        MinHeapF { v: Vec::new() }
    }
    fn push(&mut self, d: f64, x: u32) {
        self.v.push((d, x));
        let mut i = self.v.len() - 1;
        while i > 0 {
            let p = (i - 1) / 2;
            if self.v[i].0 < self.v[p].0 {
                self.v.swap(i, p);
                i = p;
            } else {
                break;
            }
        }
    }
    fn pop(&mut self) -> Option<(f64, u32)> {
        if self.v.is_empty() {
            return None;
        }
        let last = self.v.len() - 1;
        self.v.swap(0, last);
        let out = self.v.pop();
        let n = self.v.len();
        let mut i = 0;
        loop {
            let (l, r) = (2 * i + 1, 2 * i + 2);
            let mut sm = i;
            if l < n && self.v[l].0 < self.v[sm].0 {
                sm = l;
            }
            if r < n && self.v[r].0 < self.v[sm].0 {
                sm = r;
            }
            if sm == i {
                break;
            }
            self.v.swap(i, sm);
            i = sm;
        }
        out
    }
}

impl MaxHeapF {
    fn new() -> Self {
        MaxHeapF { v: Vec::new() }
    }
    fn push(&mut self, d: f64, x: u32) {
        self.v.push((d, x));
        let mut i = self.v.len() - 1;
        while i > 0 {
            let p = (i - 1) / 2;
            if self.v[i].0 > self.v[p].0 {
                self.v.swap(i, p);
                i = p;
            } else {
                break;
            }
        }
    }
    fn first(&self) -> Option<(f64, u32)> {
        self.v.first().copied()
    }
    fn pop(&mut self) -> Option<(f64, u32)> {
        if self.v.is_empty() {
            return None;
        }
        let last = self.v.len() - 1;
        self.v.swap(0, last);
        let out = self.v.pop();
        let n = self.v.len();
        let mut i = 0;
        loop {
            let (l, r) = (2 * i + 1, 2 * i + 2);
            let mut sm = i;
            if l < n && self.v[l].0 > self.v[sm].0 {
                sm = l;
            }
            if r < n && self.v[r].0 > self.v[sm].0 {
                sm = r;
            }
            if sm == i {
                break;
            }
            self.v.swap(i, sm);
            i = sm;
        }
        out
    }
}

pub struct SearchStats {
    pub tuples: i64,
    pub visited_inserts: i64,
}

// HnswSearchLayer (on-disk form, index != NULL). Returns w emptied from the
// furthest-first heap: furthest first, nearest last (C list order).
#[allow(clippy::too_many_arguments)]
pub fn search_layer_disk<'t>(
    pool: &mut ElementPool<'t>,
    q: Option<Datum>,
    ep: PgVec<'t, SearchCandidate>,
    ef: i32,
    lc: i32,
    index: &Relation<'_>,
    support: &mut HnswSupport,
    m: i32,
    inserting: bool,
    skip_element: Option<(BlockNumber, u16)>,
    visited: &mut Visited<'_>,
    mut discarded: Option<&mut DiscardedHeap<'t>>,
    init_visited: bool,
    mut tuples: Option<&mut i64>,
) -> PgResult<PgVec<'t, SearchCandidate>> {
    let lm = hnsw_get_layer_m(m, lc);
    let mut c_heap = MinHeapF::new();
    let mut w_heap = MaxHeapF::new();
    let mut wlen: i32 = 0;

    // CountElement: skip elements being deleted when vacuuming.
    let count_element = |pool: &ElementPool<'_>, e: u32| -> bool {
        if skip_element.is_none() {
            return true;
        }
        pool.get(e).heaptids_len != 0
    };

    for sc in ep.iter() {
        if init_visited {
            let e = pool.get(sc.element);
            visited.insert((e.blkno, e.offno), ());
            if let Some(t) = tuples.as_deref_mut() {
                *t += 1;
            }
        }
        c_heap.push(sc.distance, sc.element);
        w_heap.push(sc.distance, sc.element);
        if count_element(pool, sc.element) {
            wlen += 1;
        }
    }
    drop(ep);

    let mut unvisited: Vec<(BlockNumber, u16)> = Vec::with_capacity(lm as usize);
    let mut tidbuf = [[0u8; 6]; 200];

    while let Some((c_dist, c_elem)) = c_heap.pop() {
        let (f_dist, _) = w_heap.first().expect("W nonempty while C nonempty");
        if c_dist > f_dist {
            break;
        }

        // HnswLoadUnvisitedFromDisk.
        unvisited.clear();
        if load_neighbor_tids(pool, c_elem, &mut tidbuf[..lm as usize], index, m, lm, lc)? {
            for tid in tidbuf[..lm as usize].iter() {
                if !itemptr_is_valid(tid) {
                    break;
                }
                let key = itemptr_decode(tid);
                if visited.insert(key, ()).is_none() {
                    unvisited.push(key);
                }
            }
        }

        if let Some(t) = tuples.as_deref_mut() {
            *t += unvisited.len() as i64;
        }

        for &(blkno, offno) in unvisited.iter() {
            let always_add = wlen < ef;
            let (f_dist, _) = w_heap.first().expect("W nonempty");

            let id = pool.from_block(blkno, offno);
            let mut e_distance = 0.0f64;
            let max_distance = if always_add || discarded.is_some() {
                None
            } else {
                Some(f_dist)
            };
            let loaded = load_element(
                pool,
                id,
                Some(&mut e_distance),
                q,
                index,
                support,
                inserting,
                max_distance,
            )?;
            if !loaded {
                continue;
            }

            if !(e_distance < f_dist || always_add) {
                if let Some(dh) = discarded.as_deref_mut() {
                    dh.push(SearchCandidate { element: id, distance: e_distance });
                }
                continue;
            }

            // Make robust to issues.
            if (pool.get(id).level as i32) < lc {
                continue;
            }

            c_heap.push(e_distance, id);
            w_heap.push(e_distance, id);

            if count_element(pool, id) {
                wlen += 1;
                if wlen > ef {
                    let (d_dist, d_elem) = w_heap.pop().expect("W nonempty");
                    if let Some(dh) = discarded.as_deref_mut() {
                        dh.push(SearchCandidate { element: d_elem, distance: d_dist });
                    }
                }
            }
        }
    }

    let mut w: PgVec<'t, SearchCandidate> =
        mcx::vec_with_capacity_in_infallible(pool.mcx, w_heap.v.len());
    while let Some((d, x)) = w_heap.pop() {
        w.push(SearchCandidate { element: x, distance: d });
    }
    Ok(w)
}

// CheckElementCloser.
fn check_element_closer(
    pool: &ElementPool<'_>,
    e: &Candidate,
    r: &[Candidate],
    support: &mut HnswSupport,
) -> PgResult<bool> {
    let e_value = pool.value_datum(e.element);
    for ri in r {
        let ri_value = pool.value_datum(ri.element);
        let distance = get_distance(support, e_value, ri_value)? as f32;
        if distance <= e.distance {
            return Ok(false);
        }
    }
    Ok(true)
}

// SelectNeighbors (algorithm 4). `c` is consumed; result ordered as C's r.
#[allow(clippy::too_many_arguments)]
pub fn select_neighbors(
    pool: &ElementPool<'_>,
    c: &[Candidate],
    lm: i32,
    support: &mut HnswSupport,
    closer_set: &mut bool,
    new_candidate: Option<usize>,
    mut pruned: Option<&mut Option<Candidate>>,
    sort_candidates: bool,
    out: &mut Vec<Candidate>,
) -> PgResult<()> {
    out.clear();
    if c.len() as i32 <= lm {
        out.extend_from_slice(c);
        return Ok(());
    }

    let mut w: Vec<Candidate> = c.to_vec();
    let mut w_is_new: Vec<bool> = vec![false; w.len()];
    if let Some(nc) = new_candidate {
        w_is_new[nc] = true;
    }

    // C sorts desc by (distance, element) and pops from the back (nearest first).
    if sort_candidates {
        let mut order: Vec<usize> = (0..w.len()).collect();
        order.sort_by(|&a, &b| {
            w[b].distance
                .partial_cmp(&w[a].distance)
                .unwrap_or(core::cmp::Ordering::Equal)
                .then_with(|| w[b].element.cmp(&w[a].element))
        });
        let neww: Vec<Candidate> = order.iter().map(|&i| w[i]).collect();
        let newn: Vec<bool> = order.iter().map(|&i| w_is_new[i]).collect();
        w = neww;
        w_is_new = newn;
    }

    let must_calculate = !*closer_set;
    let mut wd: Vec<Candidate> = Vec::with_capacity(w.len());
    let mut added: Vec<Candidate> = Vec::new();
    let mut removed_any = false;

    while !w.is_empty() && (out.len() as i32) < lm {
        let mut e = w.pop().expect("nonempty");
        let e_is_new = w_is_new.pop().expect("nonempty");

        if must_calculate {
            e.closer = check_element_closer(pool, &e, out, support)?;
        } else if !added.is_empty() {
            if e.closer {
                e.closer = check_element_closer(pool, &e, &added, support)?;
                if !e.closer {
                    removed_any = true;
                }
            } else if removed_any {
                e.closer = check_element_closer(pool, &e, out, support)?;
                if e.closer {
                    added.push(e);
                }
            }
        } else if e_is_new {
            e.closer = check_element_closer(pool, &e, out, support)?;
            if e.closer {
                added.push(e);
            }
        }

        if e.closer {
            out.push(e);
        } else {
            wd.push(e);
        }
    }

    *closer_set = sort_candidates;

    let mut wdoff = 0usize;
    while wdoff < wd.len() && (out.len() as i32) < lm {
        out.push(wd[wdoff]);
        wdoff += 1;
    }

    if let Some(p) = pruned.as_deref_mut() {
        if wdoff < wd.len() {
            *p = Some(wd[wdoff]);
        } else {
            *p = w.first().copied();
        }
    }
    Ok(())
}

// HnswUpdateConnection over a loaded NeighborArray (items in std scratch).
#[allow(clippy::too_many_arguments)]
pub fn update_connection(
    pool: &ElementPool<'_>,
    neighbors: &mut Vec<Candidate>,
    closer_set: &mut bool,
    new_element: u32,
    distance: f32,
    lm: i32,
    update_idx: Option<&mut i32>,
    support: &mut HnswSupport,
) -> PgResult<()> {
    let new_hc = Candidate { element: new_element, distance, closer: false };
    if (neighbors.len() as i32) < lm {
        neighbors.push(new_hc);
        if let Some(u) = update_idx {
            *u = -2;
        }
        return Ok(());
    }

    let mut c: Vec<Candidate> = neighbors.clone();
    c.push(new_hc);
    let new_idx = c.len() - 1;
    let mut pruned: Option<Candidate> = None;
    let mut selected: Vec<Candidate> = Vec::new();
    select_neighbors(
        pool,
        &c,
        lm,
        support,
        closer_set,
        Some(new_idx),
        Some(&mut pruned),
        true,
        &mut selected,
    )?;

    let Some(pruned) = pruned else { return Ok(()) };
    for i in 0..neighbors.len() {
        if neighbors[i].element == pruned.element {
            neighbors[i] = new_hc;
            if let Some(u) = update_idx {
                *u = i as i32;
            }
            break;
        }
    }
    Ok(())
}

// HnswSetElementTuple: serialize the element tuple bytes.
pub fn set_element_tuple(buf: &mut Vec<u8>, pool: &ElementPool<'_>, id: u32) {
    let e = pool.get(id);
    let value = e.value.as_ref().expect("value loaded");
    let size = element_tuple_size(value.len());
    buf.clear();
    buf.resize(size, 0);
    buf[0] = HNSW_ELEMENT_TUPLE_TYPE;
    buf[1] = e.level;
    buf[2] = 0;
    buf[3] = e.version;
    for i in 0..HNSW_HEAPTIDS {
        let b = if i < e.heaptids_len as usize {
            ipd_to_bytes(&e.heaptids[i])
        } else {
            itemptr_encode(INVALID_BLOCK, INVALID_OFFSET)
        };
        buf[4 + i * 6..4 + i * 6 + 6].copy_from_slice(&b);
    }
    buf[4 + HNSW_HEAPTIDS * 6..4 + HNSW_HEAPTIDS * 6 + 6]
        .copy_from_slice(&itemptr_encode(e.neighbor_page, e.neighbor_offno));
    buf[ELEMENT_DATA_OFFSET..ELEMENT_DATA_OFFSET + value.len()].copy_from_slice(value);
}

// HnswSetNeighborTuple.
pub fn set_neighbor_tuple(buf: &mut Vec<u8>, pool: &ElementPool<'_>, id: u32, m: i32) {
    let e = pool.get(id);
    let neighbors = e.neighbors.as_ref().expect("neighbors set");
    let size = neighbor_tuple_size(e.level, m);
    buf.clear();
    buf.resize(size, 0);
    buf[0] = HNSW_NEIGHBOR_TUPLE_TYPE;
    buf[1] = e.version;
    let mut idx = 0usize;
    for lc in (0..=e.level as i32).rev() {
        let na = &neighbors[(e.level as i32 - lc) as usize];
        let lm = hnsw_get_layer_m(m, lc);
        for i in 0..lm as usize {
            let b = if i < na.items.len() {
                let ne = pool.get(na.items[i].element);
                itemptr_encode(ne.blkno, ne.offno)
            } else {
                itemptr_encode(INVALID_BLOCK, INVALID_OFFSET)
            };
            buf[NEIGHBOR_TIDS_OFFSET + idx * 6..NEIGHBOR_TIDS_OFFSET + idx * 6 + 6]
                .copy_from_slice(&b);
            idx += 1;
        }
    }
    buf[2..4].copy_from_slice(&(idx as u16).to_ne_bytes());
}

// HnswFindElementNeighbors (algorithm 1, on-disk form). Fills e.neighbors.
#[allow(clippy::too_many_arguments)]
pub fn find_element_neighbors<'t>(
    pool: &mut ElementPool<'t>,
    element: u32,
    entry_point: Option<u32>,
    index: &Relation<'_>,
    support: &mut HnswSupport,
    m: i32,
    mut ef_construction: i32,
    existing: bool,
) -> PgResult<()> {
    let level = pool.get(element).level as i32;
    let skip = if existing {
        let e = pool.get(element);
        Some((e.blkno, e.offno))
    } else {
        None
    };
    let q = Some(pool.value_datum(element));

    // Allocate the per-layer neighbor arrays (HnswInitNeighbors).
    {
        let pmcx = pool.mcx;
        let mut arrays: PgVec<'t, NeighborArray<'t>> = droppy_vec(pmcx, (level + 1) as usize);
        for lc in (0..=level).rev() {
            let lm = hnsw_get_layer_m(m, lc);
            arrays.push(NeighborArray {
                items: mcx::vec_with_capacity_in_infallible(pmcx, lm as usize),
                closer_set: false,
            });
        }
        pool.get_mut(element).neighbors = Some(arrays);
    }

    let Some(entry_point) = entry_point else { return Ok(()) };

    // HnswEntryCandidate (loadVec = true).
    let mut ep_dist = 0.0f64;
    load_element(pool, entry_point, Some(&mut ep_dist), q, index, support, true, None)?;
    let entry_level = pool.get(entry_point).level as i32;

    let mut ep: PgVec<'_, SearchCandidate> = mcx::vec_with_capacity_in_infallible(pool.mcx, 1);
    ep.push(SearchCandidate { element: entry_point, distance: ep_dist });

    let mut lc = entry_level;
    while lc >= level + 1 {
        let mut visited: Visited<'_> = PgFxHashMap::with_capacity_and_hasher_in(
            64,
            Default::default(),
            pool.mcx,
        );
        ep = search_layer_disk(
            pool, q, ep, 1, lc, index, support, m, true, skip, &mut visited, None, true, None,
        )?;
        lc -= 1;
    }

    let level = level.min(entry_level);
    if existing {
        ef_construction += 1;
    }

    let mut lc = level;
    loop {
        let lm = hnsw_get_layer_m(m, lc);
        let mut visited: Visited<'_> = PgFxHashMap::with_capacity_and_hasher_in(
            (ef_construction * m * 2) as usize,
            Default::default(),
            pool.mcx,
        );
        let w = search_layer_disk(
            pool,
            q,
            ep,
            ef_construction,
            lc,
            index,
            support,
            m,
            true,
            skip,
            &mut visited,
            None,
            true,
            None,
        )?;

        // Convert search candidates to candidates; RemoveElements (disk form).
        let mut lw: Vec<Candidate> = Vec::with_capacity(w.len());
        for sc in w.iter() {
            let hce = pool.get(sc.element);
            if let Some((sb, so)) = skip {
                if hce.blkno == sb && hce.offno == so {
                    continue;
                }
            }
            if hce.heaptids_len == 0 {
                continue;
            }
            lw.push(Candidate {
                element: sc.element,
                distance: sc.distance as f32,
                closer: false,
            });
        }

        let mut selected: Vec<Candidate> = Vec::new();
        let mut closer_set = {
            let layer_idx = (pool.get(element).level as i32 - lc) as usize;
            pool.get(element).neighbors.as_ref().expect("init")[layer_idx].closer_set
        };
        select_neighbors(
            pool,
            &lw,
            lm,
            support,
            &mut closer_set,
            None,
            None,
            false,
            &mut selected,
        )?;

        {
            let pmcx = pool.mcx;
            let layer_idx = (pool.get(element).level as i32 - lc) as usize;
            let mut items: PgVec<'t, Candidate> =
                mcx::vec_with_capacity_in_infallible(pmcx, selected.len());
            for cand in selected.iter() {
                items.push(*cand);
            }
            let e = pool.get_mut(element);
            let na = &mut e.neighbors.as_mut().expect("init")[layer_idx];
            na.items = items;
            na.closer_set = closer_set;
        }

        ep = w;
        if lc == 0 {
            break;
        }
        lc -= 1;
    }
    Ok(())
}

pub fn relation_needs_wal(rel: &Relation<'_>) -> bool {
    rel.is_permanent()
        && (transam_xlog_seams::xlog_standby_info_active::call()
            || (rel.rd_createSubid.get() == types_core::InvalidSubTransactionId
                && rel.rd_firstRelfilelocatorSubid.get()
                    == types_core::InvalidSubTransactionId))
}

pub fn heaptid_valid(t: &ItemPointerData) -> bool {
    ItemPointerIsValid(t)
}
