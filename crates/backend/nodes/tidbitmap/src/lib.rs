// tidbitmap.c. Pagetable is PgFxHashMap (C: simplehash + murmurhash32). The
// shared iteration lane is thread-native: the frozen arrays + cursor C parks
// in DSA live in an Arc (std containers: cross-thread memory, outside mcx
// accounting like ParallelTableScanDescShared).

extern crate alloc;

use std::sync::{Arc, Mutex};

use mcx::{Mcx, PgFxHashMap, PgVec};
use types_core::{BlockNumber, OffsetNumber, BLCKSZ};
use types_error::{PgError, PgResult};
use types_nodes::{bitmapword, BITS_PER_BITMAPWORD};
use types_storage::bufpage::MaxHeapTuplesPerPage;
use types_tuple::itemptr::{
    ItemPointerData, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
};

pub const TBM_MAX_TUPLES_PER_PAGE: usize = MaxHeapTuplesPerPage;
pub const PAGES_PER_CHUNK: usize = BLCKSZ / 32;

const WORDS_PER_PAGE: usize = (TBM_MAX_TUPLES_PER_PAGE - 1) / BITS_PER_BITMAPWORD + 1;
const WORDS_PER_CHUNK: usize = (PAGES_PER_CHUNK - 1) / BITS_PER_BITMAPWORD + 1;
const TBM_WORDS: usize = if WORDS_PER_PAGE > WORDS_PER_CHUNK {
    WORDS_PER_PAGE
} else {
    WORDS_PER_CHUNK
};

#[inline(always)]
const fn wordnum(x: usize) -> usize {
    x / BITS_PER_BITMAPWORD
}

#[inline(always)]
const fn bitnum(x: usize) -> usize {
    x % BITS_PER_BITMAPWORD
}

#[derive(Clone, Copy)]
pub struct PagetableEntry {
    pub blockno: BlockNumber,
    pub ischunk: bool,
    pub recheck: bool,
    pub words: [bitmapword; TBM_WORDS],
}

const _: () = assert!(core::mem::size_of::<PagetableEntry>() <= 48);

impl PagetableEntry {
    #[inline]
    fn zeroed(blockno: BlockNumber) -> Self {
        PagetableEntry { blockno, ischunk: false, recheck: false, words: [0; TBM_WORDS] }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TbmStatus {
    Empty,
    OnePage,
    Hash,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TbmIterating {
    Not,
    Private,
    Shared,
}

/// C TBMSharedIteratorState + PTEntryArray/PTIterationArray: arrays frozen
/// at prepare, only the cursor mutates, under the mutex (C: istate->lock).
pub struct TbmSharedIterState {
    npages: i32,
    nchunks: i32,
    ptbase: Vec<PagetableEntry>,
    ptpages: Vec<i32>,
    ptchunks: Vec<i32>,
    cursor: Mutex<TbmSharedCursor>,
}

impl TbmSharedIterState {
    /// bitmap-morsels: the frozen entry counts (exact pages, lossy chunks).
    /// The morsel granule space is SEGMENTED over the frozen arrays:
    /// granules [0, npages) are exact-page entries in `ptpages` (block-number
    /// order), granules [npages, npages+nchunks) are chunk entries in
    /// `ptchunks` (block-number order). See [`TbmRangeIterator`].
    pub fn entry_counts(&self) -> (u64, u64) {
        (self.npages.max(0) as u64, self.nchunks.max(0) as u64)
    }
}

struct TbmSharedCursor {
    spageptr: i32,
    schunkptr: i32,
    schunkbit: i32,
}

pub struct TIDBitmap<'mcx> {
    mcx: Mcx<'mcx>,
    status: TbmStatus,
    pagetable: Option<PgFxHashMap<'mcx, BlockNumber, PagetableEntry>>,
    nentries: i32,
    maxentries: i32,
    npages: i32,
    nchunks: i32,
    iterating: TbmIterating,
    lossify_start: usize,
    entry1: PagetableEntry,
    // Sorted readout copies (C keeps pointers into the pagetable; the bitmap
    // is read-only once iterating, so by-value copies are equivalent).
    spages: Option<PgVec<'mcx, PagetableEntry>>,
    schunks: Option<PgVec<'mcx, PagetableEntry>>,
}

pub struct TbmPrivateIterator {
    spageptr: usize,
    schunkptr: usize,
    schunkbit: usize,
}

pub struct TbmIterateResult<'a> {
    pub blockno: BlockNumber,
    pub lossy: bool,
    pub recheck: bool,
    page: Option<&'a PagetableEntry>,
}

impl TbmIterateResult<'_> {
    /// `tbm_extract_page_tuple`: fills as many offsets as fit, returns the
    /// total number of offsets present on the page.
    pub fn extract_page_tuples(&self, offsets: &mut [OffsetNumber]) -> usize {
        let page = self.page.expect("extract_page_tuples on a lossy page");
        let mut ntuples = 0usize;
        for wn in 0..WORDS_PER_PAGE {
            let mut w = page.words[wn];
            if w != 0 {
                let mut off = wn * BITS_PER_BITMAPWORD + 1;
                while w != 0 {
                    if w & 1 != 0 {
                        if ntuples < offsets.len() {
                            offsets[ntuples] = off as OffsetNumber;
                        }
                        ntuples += 1;
                    }
                    off += 1;
                    w >>= 1;
                }
            }
        }
        ntuples
    }
}

pub fn tbm_calculate_entries(maxbytes: usize) -> i32 {
    let nbuckets = maxbytes
        / (core::mem::size_of::<PagetableEntry>() + 2 * core::mem::size_of::<*const u8>());
    nbuckets.clamp(16, (i32::MAX - 1) as usize) as i32
}

#[cold]
fn offset_out_of_range(off: OffsetNumber) -> alloc::boxed::Box<PgError> {
    PgError::error(alloc::format!("tuple offset out of range: {off}")).into()
}

impl<'mcx> TIDBitmap<'mcx> {
    pub fn new(mcx: Mcx<'mcx>, maxbytes: usize) -> Self {
        TIDBitmap {
            mcx,
            status: TbmStatus::Empty,
            pagetable: None,
            nentries: 0,
            maxentries: tbm_calculate_entries(maxbytes),
            npages: 0,
            nchunks: 0,
            iterating: TbmIterating::Not,
            lossify_start: 0,
            entry1: PagetableEntry::zeroed(0),
            spages: None,
            schunks: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.nentries == 0
    }

    fn create_pagetable(&mut self) -> PgResult<()> {
        debug_assert!(self.status != TbmStatus::Hash && self.pagetable.is_none());
        let mut table = PgFxHashMap::with_hasher_in(Default::default(), self.mcx);
        table.try_reserve(128).map_err(|_| self.mcx.oom(128))?;
        if self.status == TbmStatus::OnePage {
            table.insert(self.entry1.blockno, self.entry1);
        }
        self.pagetable = Some(table);
        self.status = TbmStatus::Hash;
        Ok(())
    }

    /// `tbm_add_tuples`; tids need not be sorted, but runs of equal block are
    /// the fast path (one pagetable probe per run, as C's currblk cache).
    pub fn add_tuples(&mut self, tids: &[ItemPointerData], recheck: bool) -> PgResult<()> {
        debug_assert!(self.iterating == TbmIterating::Not);
        let mut i = 0usize;
        while i < tids.len() {
            let blk = ItemPointerGetBlockNumber(&tids[i]);
            let mut run_end = i + 1;
            while run_end < tids.len() && ItemPointerGetBlockNumber(&tids[run_end]) == blk {
                run_end += 1;
            }
            for tid in &tids[i..run_end] {
                let off = ItemPointerGetOffsetNumber(tid);
                if off < 1 || off as usize > TBM_MAX_TUPLES_PER_PAGE {
                    return Err(offset_out_of_range(off));
                }
            }
            if !self.page_is_lossy(blk) {
                let mut new_entry = false;
                let page = self.get_pageentry(blk, &mut new_entry)?;
                if page.ischunk {
                    // Chunk-header page: one bit stands for the page itself.
                    page.words[0] |= 1 as bitmapword;
                } else {
                    for tid in &tids[i..run_end] {
                        let off = ItemPointerGetOffsetNumber(tid) as usize;
                        page.words[wordnum(off - 1)] |= (1 as bitmapword) << bitnum(off - 1);
                    }
                }
                page.recheck |= recheck;
                if self.nentries > self.maxentries {
                    self.lossify()?;
                }
            }
            i = run_end;
        }
        Ok(())
    }

    /// `tbm_add_page` — whole page (always rechecked when reported).
    pub fn add_page(&mut self, pageno: BlockNumber) -> PgResult<()> {
        self.mark_page_lossy(pageno)?;
        if self.nentries > self.maxentries {
            self.lossify()?;
        }
        Ok(())
    }

    /// `tbm_union`: self |= b.
    pub fn union(&mut self, b: &TIDBitmap<'mcx>) -> PgResult<()> {
        debug_assert!(self.iterating == TbmIterating::Not);
        if b.nentries == 0 {
            return Ok(());
        }
        if b.status == TbmStatus::OnePage {
            self.union_page(&b.entry1)?;
        } else {
            debug_assert!(b.status == TbmStatus::Hash);
            for bpage in b.pagetable.as_ref().expect("TBM_HASH without pagetable").values() {
                self.union_page(bpage)?;
            }
        }
        Ok(())
    }

    /// bitmap-morsels mode C: `self |= frozen` — union a FROZEN readout (the
    /// cross-thread partial a build worker produced with
    /// `prepare_shared_iterate`; `TIDBitmap` itself is `!Send`, the frozen
    /// state is the sanctioned handoff) into this live bitmap. Per-entry
    /// semantics are exactly `tbm_union`'s — the same `union_page` body, in
    /// arbitrary entry order (C's tbm_union iterates hash order), including
    /// the exact/lossy fold and the maxentries lossify guard.
    pub fn union_frozen(&mut self, f: &TbmSharedIterState) -> PgResult<()> {
        debug_assert!(self.iterating == TbmIterating::Not);
        for e in f.ptbase.iter() {
            self.union_page(e)?;
        }
        Ok(())
    }

    fn union_page(&mut self, bpage: &PagetableEntry) -> PgResult<()> {
        if bpage.ischunk {
            for wn in 0..WORDS_PER_CHUNK {
                let mut w = bpage.words[wn];
                if w != 0 {
                    let mut pg = bpage.blockno + (wn * BITS_PER_BITMAPWORD) as BlockNumber;
                    while w != 0 {
                        if w & 1 != 0 {
                            self.mark_page_lossy(pg)?;
                        }
                        pg += 1;
                        w >>= 1;
                    }
                }
            }
        } else if self.page_is_lossy(bpage.blockno) {
            return Ok(());
        } else {
            let mut new_entry = false;
            let apage = self.get_pageentry(bpage.blockno, &mut new_entry)?;
            if apage.ischunk {
                apage.words[0] |= 1 as bitmapword;
            } else {
                for wn in 0..WORDS_PER_PAGE {
                    apage.words[wn] |= bpage.words[wn];
                }
                apage.recheck |= bpage.recheck;
            }
        }
        if self.nentries > self.maxentries {
            self.lossify()?;
        }
        Ok(())
    }

    /// `tbm_intersect`: self &= b.
    pub fn intersect(&mut self, b: &TIDBitmap<'mcx>) {
        debug_assert!(self.iterating == TbmIterating::Not);
        if self.nentries == 0 {
            return;
        }
        if self.status == TbmStatus::OnePage {
            if intersect_page(&mut self.entry1, b) {
                debug_assert!(!self.entry1.ischunk);
                self.npages -= 1;
                self.nentries -= 1;
                debug_assert!(self.nentries == 0);
                self.status = TbmStatus::Empty;
            }
        } else {
            debug_assert!(self.status == TbmStatus::Hash);
            let table = self.pagetable.as_mut().expect("TBM_HASH without pagetable");
            let (mut npages, mut nchunks, mut nentries) =
                (self.npages, self.nchunks, self.nentries);
            table.retain(|_, apage| {
                if intersect_page(apage, b) {
                    if apage.ischunk {
                        nchunks -= 1;
                    } else {
                        npages -= 1;
                    }
                    nentries -= 1;
                    false
                } else {
                    true
                }
            });
            self.npages = npages;
            self.nchunks = nchunks;
            self.nentries = nentries;
        }
    }

    fn find_pageentry(&self, pageno: BlockNumber) -> Option<&PagetableEntry> {
        if self.nentries == 0 {
            return None;
        }
        if self.status == TbmStatus::OnePage {
            if self.entry1.blockno != pageno {
                return None;
            }
            debug_assert!(!self.entry1.ischunk);
            return Some(&self.entry1);
        }
        let page = self.pagetable.as_ref().expect("TBM_HASH without pagetable").get(&pageno)?;
        if page.ischunk {
            return None;
        }
        Some(page)
    }

    fn get_pageentry(
        &mut self,
        pageno: BlockNumber,
        new_entry: &mut bool,
    ) -> PgResult<&mut PagetableEntry> {
        if self.status == TbmStatus::Empty {
            self.entry1 = PagetableEntry::zeroed(pageno);
            self.status = TbmStatus::OnePage;
            self.nentries += 1;
            self.npages += 1;
            *new_entry = true;
            return Ok(&mut self.entry1);
        }
        if self.status == TbmStatus::OnePage {
            if self.entry1.blockno == pageno {
                return Ok(&mut self.entry1);
            }
            self.create_pagetable()?;
        }
        let mcx = self.mcx;
        let table = self.pagetable.as_mut().expect("TBM_HASH without pagetable");
        table.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<PagetableEntry>()))?;
        let mut found = true;
        let page = table.entry(pageno).or_insert_with(|| {
            found = false;
            PagetableEntry::zeroed(pageno)
        });
        if !found {
            self.nentries += 1;
            self.npages += 1;
            *new_entry = true;
        }
        Ok(page)
    }

    fn page_is_lossy(&self, pageno: BlockNumber) -> bool {
        if self.nchunks == 0 {
            return false;
        }
        debug_assert!(self.status == TbmStatus::Hash);
        let bitno = pageno as usize % PAGES_PER_CHUNK;
        let chunk_pageno = pageno - bitno as BlockNumber;
        match self.pagetable.as_ref().expect("TBM_HASH without pagetable").get(&chunk_pageno) {
            Some(page) if page.ischunk => {
                page.words[wordnum(bitno)] & ((1 as bitmapword) << bitnum(bitno)) != 0
            }
            _ => false,
        }
    }

    fn mark_page_lossy(&mut self, pageno: BlockNumber) -> PgResult<()> {
        if self.status != TbmStatus::Hash {
            self.create_pagetable()?;
        }
        let bitno = pageno as usize % PAGES_PER_CHUNK;
        let chunk_pageno = pageno - bitno as BlockNumber;
        let mcx = self.mcx;
        let table = self.pagetable.as_mut().expect("TBM_HASH without pagetable");
        if bitno != 0 && table.remove(&pageno).is_some() {
            self.nentries -= 1;
            self.npages -= 1;
        }
        table.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<PagetableEntry>()))?;
        match table.entry(chunk_pageno) {
            hashbrown::hash_map::Entry::Vacant(v) => {
                let mut entry = PagetableEntry::zeroed(chunk_pageno);
                entry.ischunk = true;
                entry.words[wordnum(bitno)] |= (1 as bitmapword) << bitnum(bitno);
                v.insert(entry);
                self.nentries += 1;
                self.nchunks += 1;
            }
            hashbrown::hash_map::Entry::Occupied(mut o) => {
                let page = o.get_mut();
                if !page.ischunk {
                    // Chunk-header page was exact: it had tuple bits, so its
                    // own page-bit is set lossy.
                    *page = PagetableEntry::zeroed(chunk_pageno);
                    page.ischunk = true;
                    page.words[0] = 1 as bitmapword;
                    self.nchunks += 1;
                    self.npages -= 1;
                }
                page.words[wordnum(bitno)] |= (1 as bitmapword) << bitnum(bitno);
            }
        }
        Ok(())
    }

    #[cold]
    #[inline(never)]
    fn lossify(&mut self) -> PgResult<()> {
        debug_assert!(self.iterating == TbmIterating::Not);
        debug_assert!(self.status == TbmStatus::Hash);
        // C's resumable simplehash walk (tbm_lossify + start_iterate_at):
        // scan raw buckets from lossify_start, wrapping once over the bucket
        // count captured at entry. mark_page_lossy inserts/deletes (and may
        // grow the table) mid-walk; skipping a moved entry or visiting a new
        // one is tolerated — C's walk has the identical race and tolerates it
        // the same way. Growth never shrinks the table, so a stale index
        // stays in bounds.
        let nbuckets =
            self.pagetable.as_ref().expect("TBM_HASH without pagetable").raw_table().buckets();
        let mask = nbuckets - 1;
        let start = self.lossify_start & mask;
        for k in 0..nbuckets {
            let idx = (start + k) & mask;
            let table =
                self.pagetable.as_ref().expect("TBM_HASH without pagetable").raw_table();
            // SAFETY: idx < nbuckets <= table.buckets() (grow-only), and the
            // bucket reference is copied out before any table mutation.
            let (blockno, ischunk) = unsafe {
                if !table.is_bucket_full(idx) {
                    continue;
                }
                let (blockno, page) = table.bucket(idx).as_ref();
                (*blockno, page.ischunk)
            };
            if ischunk || blockno as usize % PAGES_PER_CHUNK == 0 {
                continue;
            }
            self.mark_page_lossy(blockno)?;
            if self.nentries <= self.maxentries / 2 {
                self.lossify_start = idx + 1;
                return Ok(());
            }
        }
        if self.nentries > self.maxentries / 2 {
            self.maxentries = self.nentries.min((i32::MAX - 1) / 2) * 2;
        }
        Ok(())
    }

    /// bitmap-morsels: entry counts (exact pages, lossy chunks) before any
    /// freeze — the runtime bitmap arm's admission floor input.
    pub fn entry_counts(&self) -> (u64, u64) {
        match self.status {
            TbmStatus::Empty => (0, 0),
            TbmStatus::OnePage => (1, 0),
            TbmStatus::Hash => (self.npages.max(0) as u64, self.nchunks.max(0) as u64),
        }
    }

    /// `tbm_begin_private_iterate`; the bitmap is read-only afterwards.
    pub fn begin_private_iterate(&mut self) -> PgResult<TbmPrivateIterator> {
        if self.status == TbmStatus::Hash && self.iterating == TbmIterating::Not {
            let table = self.pagetable.as_ref().expect("TBM_HASH without pagetable");
            let mut spages: PgVec<'mcx, PagetableEntry> =
                mcx::vec_with_capacity_in(self.mcx, self.npages as usize)?;
            let mut schunks: PgVec<'mcx, PagetableEntry> =
                mcx::vec_with_capacity_in(self.mcx, self.nchunks as usize)?;
            for page in table.values() {
                if page.ischunk {
                    schunks.push(*page);
                } else {
                    spages.push(*page);
                }
            }
            debug_assert!(spages.len() == self.npages as usize);
            debug_assert!(schunks.len() == self.nchunks as usize);
            spages.sort_unstable_by_key(|p| p.blockno);
            schunks.sort_unstable_by_key(|p| p.blockno);
            self.spages = Some(spages);
            self.schunks = Some(schunks);
        }
        self.iterating = TbmIterating::Private;
        Ok(TbmPrivateIterator { spageptr: 0, schunkptr: 0, schunkbit: 0 })
    }

    /// `tbm_prepare_shared_iterate`: freezes the pagetable into a flat entry
    /// array plus sorted page/chunk index arrays, copied out of the arena
    /// into the workers' Arc (C: dsa). Iteration order matches C exactly.
    pub fn prepare_shared_iterate(&mut self) -> PgResult<Arc<TbmSharedIterState>> {
        debug_assert!(self.iterating != TbmIterating::Private);
        let oom = |n: usize| self.mcx.oom(n * core::mem::size_of::<PagetableEntry>());
        let mut ptbase: Vec<PagetableEntry> = Vec::new();
        ptbase.try_reserve_exact(self.nentries.max(0) as usize).map_err(|_| oom(self.nentries.max(0) as usize))?;
        let mut ptpages: Vec<i32> = Vec::new();
        ptpages.try_reserve_exact(self.npages.max(0) as usize).map_err(|_| oom(1))?;
        let mut ptchunks: Vec<i32> = Vec::new();
        ptchunks.try_reserve_exact(self.nchunks.max(0) as usize).map_err(|_| oom(1))?;
        match self.status {
            TbmStatus::Hash => {
                let table = self.pagetable.as_ref().expect("TBM_HASH without pagetable");
                for page in table.values() {
                    let idx = ptbase.len() as i32;
                    ptbase.push(*page);
                    if page.ischunk {
                        ptchunks.push(idx);
                    } else {
                        ptpages.push(idx);
                    }
                }
            }
            TbmStatus::OnePage => {
                ptbase.push(self.entry1);
                ptpages.push(0);
            }
            TbmStatus::Empty => {}
        }
        debug_assert!(ptpages.len() == self.npages as usize);
        debug_assert!(ptchunks.len() == self.nchunks as usize);
        ptpages.sort_unstable_by_key(|&i| ptbase[i as usize].blockno);
        ptchunks.sort_unstable_by_key(|&i| ptbase[i as usize].blockno);
        self.iterating = TbmIterating::Shared;
        Ok(Arc::new(TbmSharedIterState {
            npages: self.npages,
            nchunks: self.nchunks,
            ptbase,
            ptpages,
            ptchunks,
            cursor: Mutex::new(TbmSharedCursor { spageptr: 0, schunkptr: 0, schunkbit: 0 }),
        }))
    }
}

fn intersect_page(apage: &mut PagetableEntry, b: &TIDBitmap<'_>) -> bool {
    if apage.ischunk {
        let mut candelete = true;
        for wn in 0..WORDS_PER_CHUNK {
            let w = apage.words[wn];
            if w != 0 {
                let mut neww = w;
                let mut pg = apage.blockno + (wn * BITS_PER_BITMAPWORD) as BlockNumber;
                let mut bit = 0usize;
                let mut rest = w;
                while rest != 0 {
                    if rest & 1 != 0 && !b.page_is_lossy(pg) && b.find_pageentry(pg).is_none() {
                        neww &= !((1 as bitmapword) << bit);
                    }
                    pg += 1;
                    bit += 1;
                    rest >>= 1;
                }
                apage.words[wn] = neww;
                if neww != 0 {
                    candelete = false;
                }
            }
        }
        candelete
    } else if b.page_is_lossy(apage.blockno) {
        apage.recheck = true;
        false
    } else {
        let mut candelete = true;
        if let Some(bpage) = b.find_pageentry(apage.blockno) {
            debug_assert!(!bpage.ischunk);
            for wn in 0..WORDS_PER_PAGE {
                apage.words[wn] &= bpage.words[wn];
                if apage.words[wn] != 0 {
                    candelete = false;
                }
            }
            apage.recheck |= bpage.recheck;
        }
        candelete
    }
}

#[inline]
fn advance_schunkbit(chunk: &PagetableEntry, schunkbit: &mut usize) {
    let mut bit = *schunkbit;
    while bit < PAGES_PER_CHUNK {
        if chunk.words[wordnum(bit)] & ((1 as bitmapword) << bitnum(bit)) != 0 {
            break;
        }
        bit += 1;
    }
    *schunkbit = bit;
}

impl TbmPrivateIterator {
    /// `tbm_private_iterate`: pages come out in block-number order;
    /// `None` = exhausted.
    pub fn next<'a>(&mut self, tbm: &'a TIDBitmap<'_>) -> Option<TbmIterateResult<'a>> {
        debug_assert!(tbm.iterating == TbmIterating::Private);
        let nchunks = tbm.nchunks as usize;
        let npages = tbm.npages as usize;
        let schunks: &[PagetableEntry] = tbm.schunks.as_deref().unwrap_or(&[]);
        while self.schunkptr < nchunks {
            let chunk = &schunks[self.schunkptr];
            let mut schunkbit = self.schunkbit;
            advance_schunkbit(chunk, &mut schunkbit);
            if schunkbit < PAGES_PER_CHUNK {
                self.schunkbit = schunkbit;
                break;
            }
            self.schunkptr += 1;
            self.schunkbit = 0;
        }
        // Emit the numerically earlier of the next lossy page and the next
        // exact page.
        if self.schunkptr < nchunks {
            let chunk = &schunks[self.schunkptr];
            let chunk_blockno = chunk.blockno + self.schunkbit as BlockNumber;
            let next_exact_blockno = if tbm.status == TbmStatus::OnePage {
                Some(tbm.entry1.blockno)
            } else {
                tbm.spages.as_deref().and_then(|s| s.get(self.spageptr)).map(|p| p.blockno)
            };
            if self.spageptr >= npages || next_exact_blockno.is_none_or(|b| chunk_blockno < b) {
                self.schunkbit += 1;
                return Some(TbmIterateResult {
                    blockno: chunk_blockno,
                    lossy: true,
                    recheck: true,
                    page: None,
                });
            }
        }
        if self.spageptr < npages {
            let page = if tbm.status == TbmStatus::OnePage {
                &tbm.entry1
            } else {
                &tbm.spages.as_deref().expect("iterating without spages")[self.spageptr]
            };
            self.spageptr += 1;
            return Some(TbmIterateResult {
                blockno: page.blockno,
                lossy: false,
                recheck: page.recheck,
                page: Some(page),
            });
        }
        None
    }
}

/// C TBMSharedIterator: attach handle pinning the Arc (no bitmap needed).
pub struct TbmSharedIterator {
    state: Arc<TbmSharedIterState>,
}

impl TbmSharedIterator {
    /// `tbm_attach_shared_iterate`; C takes (dsa, dsa_pointer).
    pub fn attach(state: Arc<TbmSharedIterState>) -> Self {
        TbmSharedIterator { state }
    }

    /// `tbm_shared_iterate`; cursor advance under the mutex (C: istate->lock).
    pub fn next(&mut self) -> Option<TbmIterateResult<'_>> {
        let st = &*self.state;
        let ptbase: &[PagetableEntry] = &st.ptbase;
        let idxpages: &[i32] = &st.ptpages;
        let idxchunks: &[i32] = &st.ptchunks;
        let mut cur = st.cursor.lock().unwrap_or_else(|e| e.into_inner());
        while cur.schunkptr < st.nchunks {
            let chunk = &ptbase[idxchunks[cur.schunkptr as usize] as usize];
            let mut schunkbit = cur.schunkbit as usize;
            advance_schunkbit(chunk, &mut schunkbit);
            if schunkbit < PAGES_PER_CHUNK {
                cur.schunkbit = schunkbit as i32;
                break;
            }
            cur.schunkptr += 1;
            cur.schunkbit = 0;
        }
        if cur.schunkptr < st.nchunks {
            let chunk = &ptbase[idxchunks[cur.schunkptr as usize] as usize];
            let chunk_blockno = chunk.blockno + cur.schunkbit as BlockNumber;
            if cur.spageptr >= st.npages
                || chunk_blockno < ptbase[idxpages[cur.spageptr as usize] as usize].blockno
            {
                cur.schunkbit += 1;
                return Some(TbmIterateResult {
                    blockno: chunk_blockno,
                    lossy: true,
                    recheck: true,
                    page: None,
                });
            }
        }
        if cur.spageptr < st.npages {
            let idx = idxpages[cur.spageptr as usize] as usize;
            cur.spageptr += 1;
            drop(cur);
            let page = &ptbase[idx];
            return Some(TbmIterateResult {
                blockno: page.blockno,
                lossy: false,
                recheck: page.recheck,
                page: Some(page),
            });
        }
        None
    }
}

/// bitmap-morsels RANGE iterator: iterates one CLAIMED WINDOW of the frozen
/// shared arrays with NO shared cursor — each morsel worker owns a disjoint
/// entry-index window, so no lock and no cross-worker handoff. The granule
/// space is SEGMENTED (not C's block-order page/chunk merge): entries
/// [0, npages) are the exact pages in `ptpages` order (ascending blockno),
/// entries [npages, npages+nchunks) are the lossy chunks in `ptchunks` order.
/// Each chunk entry expands to its set bits exactly like the shared/private
/// iterators (advance_schunkbit), yielding lossy per-page results with
/// recheck=true. Cross-window row ORDER therefore differs from the serial
/// merged order — sound only for order-insensitive consumers (the runtime
/// bitmap arm admits order-insensitive-exact agg partials only); per-page
/// contents, recheck flags, and visibility semantics are identical.
pub struct TbmRangeIterator {
    state: Arc<TbmSharedIterState>,
    /// Next entry index in the segmented space, in [pos, end).
    pos: u64,
    end: u64,
    /// Bit cursor within the current chunk entry (pos >= npages only).
    schunkbit: usize,
}

impl TbmRangeIterator {
    /// Iterate segmented entries [start, end); the caller guarantees
    /// end <= npages + nchunks (the morsel source's granule count).
    pub fn new(state: Arc<TbmSharedIterState>, start: u64, end: u64) -> Self {
        debug_assert!(
            end <= (state.npages.max(0) as u64 + state.nchunks.max(0) as u64)
                && start <= end
        );
        TbmRangeIterator { state, pos: start, end, schunkbit: 0 }
    }

    pub fn next(&mut self) -> Option<TbmIterateResult<'_>> {
        let st = &*self.state;
        let npages = st.npages.max(0) as u64;
        loop {
            if self.pos >= self.end {
                return None;
            }
            if self.pos < npages {
                let idx = st.ptpages[self.pos as usize] as usize;
                self.pos += 1;
                let page = &st.ptbase[idx];
                return Some(TbmIterateResult {
                    blockno: page.blockno,
                    lossy: false,
                    recheck: page.recheck,
                    page: Some(page),
                });
            }
            let ci = (self.pos - npages) as usize;
            let chunk = &st.ptbase[st.ptchunks[ci] as usize];
            let mut bit = self.schunkbit;
            advance_schunkbit(chunk, &mut bit);
            if bit >= PAGES_PER_CHUNK {
                self.pos += 1;
                self.schunkbit = 0;
                continue;
            }
            self.schunkbit = bit + 1;
            return Some(TbmIterateResult {
                blockno: chunk.blockno + bit as BlockNumber,
                lossy: true,
                recheck: true,
                page: None,
            });
        }
    }
}

/// Unified iterator (`TBMIterator`).
pub struct TbmIterator {
    private: Option<TbmPrivateIterator>,
    shared: Option<TbmSharedIterator>,
    range: Option<TbmRangeIterator>,
}

impl TbmIterator {
    pub fn empty() -> Self {
        TbmIterator { private: None, shared: None, range: None }
    }

    pub fn private(iter: TbmPrivateIterator) -> Self {
        TbmIterator { private: Some(iter), shared: None, range: None }
    }

    pub fn shared(iter: TbmSharedIterator) -> Self {
        TbmIterator { private: None, shared: Some(iter), range: None }
    }

    /// bitmap-morsels: a claimed-window iterator (self-contained, lock-free).
    pub fn range(iter: TbmRangeIterator) -> Self {
        TbmIterator { private: None, shared: None, range: Some(iter) }
    }

    pub fn exhausted(&self) -> bool {
        self.private.is_none() && self.shared.is_none() && self.range.is_none()
    }

    pub fn end_iterate(&mut self) {
        self.private = None;
        self.shared = None;
        self.range = None;
    }

    /// `tbm_iterate`; the private lane needs the owning bitmap, the shared
    /// and range lanes are self-contained.
    pub fn next<'a>(&'a mut self, tbm: Option<&'a TIDBitmap<'_>>) -> Option<TbmIterateResult<'a>> {
        if let Some(p) = self.private.as_mut() {
            return p.next(tbm.expect("private tbm_iterate without the owning bitmap"));
        }
        if let Some(r) = self.range.as_mut() {
            return r.next();
        }
        self.shared.as_mut().expect("tbm_iterate on an exhausted TBMIterator").next()
    }
}

mcx::forget_safe_nodrop!(TbmStatus, TbmIterating, PagetableEntry, TbmPrivateIterator);

// Exempt (droppy Arc handles): released by TbmIterator::end_iterate /
// ExecEndBitmapHeapScan before the owning arena resets.
unsafe impl mcx::ForgetSafe for TbmSharedIterator {}
unsafe impl mcx::ForgetSafe for TbmRangeIterator {}
unsafe impl mcx::ForgetSafe for TbmIterator {}

mcx::forget_safe_struct!(
    TIDBitmap<'_> { mcx, status, pagetable, nentries, maxentries, npages,
        nchunks, iterating, lossify_start, entry1, spages, schunks },
);

#[cfg(test)]
mod tests;
