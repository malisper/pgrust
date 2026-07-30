//! ginentrypage.c: entry-tree tuples and page operations. Multicolumn entry
//! tuples carry the attribute number as a leading int2 attribute; the
//! category byte sits at GinCategoryOffset (data offset, +2 for multicol).

use ::bufmgr_seams as bm;
use ::datum::Datum;
use ::gin_vocab::*;
use ::mcx::Mcx;
use ::nbtree::itup::{
    self, index_form_tuple, index_info_find_data_offset, ItupBuf, INDEX_SIZE_MASK as ITUP_SIZE_MASK,
};
use ::types_core::{BlockNumber, Buffer, InvalidBlockNumber, OffsetNumber, BLCKSZ};
use ::types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
use ::types_rel::Relation;
use ::types_storage::bufpage::{PageMut, PageRef, PageTemp};
use ::types_tuple::itemptr::{
    FirstOffsetNumber, InvalidOffsetNumber, ItemPointerData, ItemPointerSet,
};

use crate::btree::{Frame, GinBt, GinPlace};
use crate::postinglist::ginPostingListDecodeAllSegments;
use crate::util::gin_init_page_bytes;
use crate::{page_mut, page_opaque, page_ref};

pub(crate) const GIN_TREE_POSTING: OffsetNumber = 0xffff;
pub(crate) const GIN_ITUP_COMPRESSED: u32 = 1 << 31;

pub type ITup = *const u8;


#[inline]
pub unsafe fn gin_get_nposting(itup: ITup) -> OffsetNumber {
    gin_item_pointer_offset(&itup::t_tid(itup))
}

#[inline]
pub unsafe fn gin_is_posting_tree(itup: ITup) -> bool {
    gin_get_nposting(itup) == GIN_TREE_POSTING
}

#[inline]
pub unsafe fn gin_get_posting_tree(itup: ITup) -> BlockNumber {
    gin_item_pointer_block(&itup::t_tid(itup))
}

#[inline]
pub unsafe fn gin_get_posting_offset(itup: ITup) -> usize {
    (gin_item_pointer_block(&itup::t_tid(itup)) & !GIN_ITUP_COMPRESSED) as usize
}

#[inline]
pub unsafe fn gin_itup_is_compressed(itup: ITup) -> bool {
    gin_item_pointer_block(&itup::t_tid(itup)) & GIN_ITUP_COMPRESSED != 0
}

#[inline]
pub unsafe fn gin_get_downlink(itup: ITup) -> BlockNumber {
    gin_item_pointer_block(&itup::t_tid(itup))
}

unsafe fn set_t_tid_parts(itup: *mut u8, blk: Option<BlockNumber>, off: Option<OffsetNumber>) {
    let mut tid = itup::t_tid(itup);
    if let Some(b) = blk {
        tid.ip_blkid.bi_hi = (b >> 16) as u16;
        tid.ip_blkid.bi_lo = (b & 0xffff) as u16;
    }
    if let Some(o) = off {
        tid.ip_posid = o;
    }
    itup::set_t_tid(itup, tid);
}

#[inline]
pub(crate) unsafe fn gin_set_downlink(itup: *mut u8, blkno: BlockNumber) {
    let mut tid = ItemPointerData::invalid();
    ItemPointerSet(&mut tid, blkno, InvalidOffsetNumber);
    itup::set_t_tid(itup, tid);
}

#[inline]
pub(crate) unsafe fn gin_set_posting_tree(itup: *mut u8, root: BlockNumber) {
    set_t_tid_parts(itup, Some(root), Some(GIN_TREE_POSTING));
}

/// GinCategoryOffset + GinGetNullCategory.
#[inline]
pub(crate) unsafe fn gin_get_null_category(state: &GinState, itup: ITup) -> GinNullCategory {
    let off = index_info_find_data_offset(itup::t_info(itup))
        + if state.one_col { 0 } else { core::mem::size_of::<i16>() };
    *itup.add(off).cast::<GinNullCategory>()
}

/// gintuple_get_attrnum. The multicolumn attnum is the first attribute (int2,
/// never null): raw native-endian read at the data offset.
#[inline]
pub unsafe fn gintuple_get_attrnum(state: &GinState, itup: ITup) -> OffsetNumber {
    if state.one_col {
        return FirstOffsetNumber;
    }
    let off = index_info_find_data_offset(itup::t_info(itup));
    let colnum = itup.add(off).cast::<u16>().read_unaligned() as OffsetNumber;
    debug_assert!(colnum >= 1 && (colnum as u16) <= state.natts);
    colnum
}

/// Transient per-column key tupdesc for the multicolumn entry-tuple layout
/// (C initGinState's state->tupdesc[i]: int2 attnum + key attribute).
pub(crate) fn gin_col_tupdesc<'s>(
    mcx: Mcx<'s>,
    rel: &Relation<'_>,
    attnum: OffsetNumber,
) -> PgResult<::types_tuple::TupleDescData<'s>> {
    use ::types_tuple::{CompactAttribute, FormData_pg_attribute};
    let mut int2 = FormData_pg_attribute {
        atttypid: ::types_core::INT2OID,
        attlen: 2,
        attnum: 1,
        atttypmod: -1,
        attbyval: true,
        attalign: b's' as i8,
        attstorage: b'p' as i8,
        ..Default::default()
    };
    int2.attislocal = true;
    let mut key = *rel.rd_att.attr(attnum as usize - 1);
    key.attnum = 2;
    let mut attrs = mcx::vec_with_capacity_in(mcx, 2)?;
    attrs.push(int2);
    attrs.push(key);
    let mut compact = mcx::vec_with_capacity_in(mcx, 2)?;
    compact.push(CompactAttribute::populate_from(&attrs[0]));
    compact.push(CompactAttribute::populate_from(&attrs[1]));
    Ok(::types_tuple::TupleDescData {
        natts: 2,
        tdtypeid: ::types_core::RECORDOID,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

/// gintuple_get_key: borrowed datum into the page image. `mcx` is scratch for
/// the transient multicolumn tupdesc only.
pub unsafe fn gintuple_get_key(
    mcx: Mcx<'_>,
    rel: &Relation<'_>,
    state: &GinState,
    itup: ITup,
    category: &mut GinNullCategory,
) -> PgResult<Datum> {
    let mut isnull = false;
    let res = if state.one_col {
        itup::index_getattr(itup, 1, &rel.rd_att, &mut isnull)
    } else {
        let colnum = gintuple_get_attrnum(state, itup);
        let desc = gin_col_tupdesc(mcx, rel, colnum)?;
        itup::index_getattr(itup, 2, &desc, &mut isnull)
    };
    *category = if isnull {
        gin_get_null_category(state, itup)
    } else {
        GIN_CAT_NORM_KEY
    };
    Ok(res)
}

#[track_caller]
#[cold]
#[inline(never)]
fn index_row_too_big(newsize: usize, relname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "index row size {newsize} exceeds maximum {GinMaxItemSize} for index \"{relname}\""
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
    )
}

/// GinFormTuple. `data` is the compressed posting bytes (empty to leave the
/// posting area unfilled; extra `data_size` still reserved when nonzero).
pub(crate) fn GinFormTuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'_>,
    state: &GinState,
    attnum: OffsetNumber,
    key: Datum,
    category: GinNullCategory,
    data: &[u8],
    data_size: usize,
    nipd: usize,
    error_too_big: bool,
) -> PgResult<Option<ItupBuf<'mcx>>> {
    let itup = if state.one_col {
        let values = [key];
        let isnull = [category != GIN_CAT_NORM_KEY];
        index_form_tuple(mcx, &rel.rd_att, &values, &isnull)?
    } else {
        let desc = gin_col_tupdesc(mcx, rel, attnum)?;
        let values = [Datum::from_usize(attnum as usize), key];
        let isnull = [false, category != GIN_CAT_NORM_KEY];
        index_form_tuple(mcx, &desc, &values, &isnull)?
    };

    // SAFETY: freshly built owned image.
    let mut newsize = unsafe { itup::index_tuple_size(itup.as_ptr()) };
    // SAFETY: as above.
    if unsafe { itup::index_tuple_has_nulls(itup.as_ptr()) } {
        debug_assert!(category != GIN_CAT_NORM_KEY);
        // GinCategoryOffset + sizeof(GinNullCategory).
        // SAFETY: as above.
        let minsize = index_info_find_data_offset(unsafe { itup::t_info(itup.as_ptr()) })
            + if state.one_col { 0 } else { core::mem::size_of::<i16>() }
            + 1;
        newsize = newsize.max(minsize);
    }
    newsize = SHORTALIGN(newsize);
    let posting_offset = newsize;

    newsize += data_size;
    newsize = MAXALIGN(newsize);

    if newsize > GinMaxItemSize {
        if error_too_big {
            return Err(index_row_too_big(newsize, rel.name()));
        }
        return Ok(None);
    }

    let mut out = ItupBuf::with_size(mcx, newsize)?;
    // SAFETY: out is a zeroed newsize image; itup smaller or equal.
    unsafe {
        core::ptr::copy_nonoverlapping(
            itup.as_ptr(),
            out.as_mut_ptr(),
            itup::index_tuple_size(itup.as_ptr()),
        );
        let info = (itup::t_info(out.as_ptr()) & !ITUP_SIZE_MASK) | newsize as u16;
        itup::set_t_info(out.as_mut_ptr(), info);
        set_t_tid_parts(
            out.as_mut_ptr(),
            Some(posting_offset as u32 | GIN_ITUP_COMPRESSED),
            Some(nipd as OffsetNumber),
        );
        if !data.is_empty() {
            debug_assert!(data.len() == data_size);
            core::ptr::copy_nonoverlapping(
                data.as_ptr(),
                out.as_mut_ptr().add(posting_offset),
                data.len(),
            );
        }
        if category != GIN_CAT_NORM_KEY {
            debug_assert!(itup::index_tuple_has_nulls(out.as_ptr()));
            let off = index_info_find_data_offset(itup::t_info(out.as_ptr()))
                + if state.one_col { 0 } else { core::mem::size_of::<i16>() };
            *out.as_mut_ptr().add(off).cast::<GinNullCategory>() = category;
        }
    }
    Ok(Some(out))
}

/// ginReadTuple: item pointers of a leaf entry tuple.
///
/// # Safety
/// `itup` points at a live entry tuple (pin held for the duration).
pub(crate) unsafe fn ginReadTuple<'mcx>(
    mcx: Mcx<'mcx>,
    itup: ITup,
    out: &mut ::mcx::PgVec<'mcx, ItemPointerData>,
) -> PgResult<()> {
    let _ = mcx;
    let nipd = gin_get_nposting(itup) as usize;
    let ptr = itup.add(gin_get_posting_offset(itup));
    if gin_itup_is_compressed(itup) {
        if nipd > 0 {
            let before = out.len();
            let seglen = crate::postinglist::seg_size(core::slice::from_raw_parts(ptr, 8));
            ginPostingListDecodeAllSegments(core::slice::from_raw_parts(ptr, seglen), out)?;
            if out.len() - before != nipd {
                panic!(
                    "number of items mismatch in GIN entry tuple, {} in tuple header, {} decoded",
                    nipd,
                    out.len() - before
                );
            }
        }
    } else {
        out.try_reserve(nipd).map_err(|_| crate::oom(nipd * 6))?;
        for i in 0..nipd {
            out.push(ptr.add(i * 6).cast::<ItemPointerData>().read_unaligned());
        }
    }
    Ok(())
}

/// GinFormInteriorTuple: copy key data, drop any posting list, set downlink.
fn gin_form_interior_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    itup: ITup,
    page_is_leaf: bool,
    childblk: BlockNumber,
) -> PgResult<ItupBuf<'mcx>> {
    // SAFETY: caller holds the page pin; itup is a live tuple.
    unsafe {
        let mut nitup = if page_is_leaf && !gin_is_posting_tree(itup) {
            let origsize = MAXALIGN(gin_get_posting_offset(itup));
            let mut n = ItupBuf::with_size(mcx, origsize)?;
            core::ptr::copy_nonoverlapping(itup, n.as_mut_ptr(), origsize);
            let info = (itup::t_info(n.as_ptr()) & !ITUP_SIZE_MASK) | origsize as u16;
            itup::set_t_info(n.as_mut_ptr(), info);
            n
        } else {
            itup::copy_index_tuple(mcx, itup)?
        };
        gin_set_downlink(nitup.as_mut_ptr(), childblk);
        Ok(nitup)
    }
}

fn get_rightmost_tuple<'p>(page: &PageRef<'p>) -> ITup {
    let maxoff = page.max_offset_number();
    let id = page.item_id(maxoff);
    page.item_raw(id).0
}

pub(crate) struct EntryPayload<'s> {
    pub entry: ItupBuf<'s>,
    pub is_delete: bool,
}

pub(crate) struct EntryBtree<'a, 'r, 's> {
    pub rel: &'a Relation<'r>,
    pub state: &'a GinState,
    pub attnum: OffsetNumber,
    pub key: Datum,
    pub category: GinNullCategory,
    pub is_build: bool,
    pub full_scan: bool,
    pub scratch: Mcx<'s>,
    pub payload: Option<EntryPayload<'s>>,
}

impl<'a, 'r, 's> EntryBtree<'a, 'r, 's> {
    pub fn new(
        rel: &'a Relation<'r>,
        state: &'a GinState,
        attnum: OffsetNumber,
        key: Datum,
        category: GinNullCategory,
        scratch: Mcx<'s>,
    ) -> Self {
        EntryBtree {
            rel,
            state,
            attnum,
            key,
            category,
            is_build: false,
            full_scan: false,
            scratch,
            payload: None,
        }
    }

    pub(crate) fn compare_to(&self, itup: ITup) -> i32 {
        let mut category = GIN_CAT_NORM_KEY;
        // SAFETY: pin held by the caller of the search callbacks.
        let (tup_attnum, key) = unsafe {
            (
                gintuple_get_attrnum(self.state, itup),
                gintuple_get_key(self.scratch, self.rel, self.state, itup, &mut category)
                    .expect("gin key tupdesc scratch alloc"),
            )
        };
        crate::util::ginCompareAttEntries(
            self.state,
            self.attnum,
            self.key,
            self.category,
            tup_attnum,
            key,
            category,
        )
    }

    /// entryIsEnoughSpace.
    fn is_enough_space(&self, page: &PageRef<'_>, off: OffsetNumber) -> bool {
        let payload = self.payload.as_ref().expect("entry insert payload");
        let mut releasedsz = 0usize;
        if payload.is_delete {
            let id = page.item_id(off);
            releasedsz = MAXALIGN(id.lp_len() as usize) + 4;
        }
        let addedsz = MAXALIGN(payload.entry.size()) + 4;
        page.free_space() + releasedsz >= addedsz
    }

    /// entryPreparePage over a raw page image.
    fn prepare_page(&self, page: &mut PageMut<'_>, off: OffsetNumber, update_blkno: BlockNumber) {
        let payload = self.payload.as_ref().expect("entry insert payload");
        let opaque = page_opaque(&page.as_ref());
        if payload.is_delete {
            debug_assert!(crate::GinPageIsLeaf(&opaque));
            page.index_tuple_delete(off);
        }
        if !crate::GinPageIsLeaf(&opaque) && update_blkno != InvalidBlockNumber {
            let id = page.as_ref().item_id(off);
            let itup = page.as_ref().item_raw(id).0.cast_mut();
            // SAFETY: exclusive page access; itup within the page.
            unsafe { gin_set_downlink(itup, update_blkno) };
        }
    }

    /// entrySplitPage: build new left/right images; original untouched.
    fn split_page(
        &mut self,
        buf: Buffer,
        off: OffsetNumber,
        update_blkno: BlockNumber,
    ) -> PgResult<(PageTemp, PageTemp)> {
        let payload_size = {
            let payload = self.payload.as_ref().expect("entry insert payload");
            MAXALIGN(payload.entry.size())
        };

        let mut lpage = PageTemp::new(BLCKSZ)?;
        let mut rpage = PageTemp::new(BLCKSZ)?;
        // SAFETY: pin + exclusive lock held on buf.
        let orig = unsafe { page_ref(buf) };
        lpage
            .as_mut_bytes()
            .copy_from_slice(crate::page_bytes(&orig));

        {
            // SAFETY: owned temp image.
            let mut lmut =
                unsafe { PageMut::from_raw(core::ptr::NonNull::new(lpage.as_mut_bytes().as_mut_ptr()).unwrap()) };
            self.prepare_page(&mut lmut, off, update_blkno);
        }

        // Append existing tuples and the new tuple in key order to a
        // workspace, then redistribute.
        let mut tupstore: ::mcx::PgVec<'_, u8> = mcx::vec_with_capacity_in(self.scratch, 2 * BLCKSZ)?;
        let mut totalsize = 0usize;
        let flags;
        {
            // SAFETY: owned temp image.
            let lref = unsafe {
                PageRef::from_raw(core::ptr::NonNull::new(lpage.as_mut_bytes().as_mut_ptr()).unwrap())
            };
            flags = page_opaque(&lref).flags;
            let maxoff = lref.max_offset_number();
            let payload = self.payload.as_ref().expect("entry insert payload");
            for i in FirstOffsetNumber..=maxoff {
                if i == off {
                    let size = MAXALIGN(payload.entry.size());
                    // SAFETY: entry image is `size` (MAXALIGNed) bytes.
                    ::mcx::vec_append_bytes(&mut tupstore, unsafe {
                        core::slice::from_raw_parts(payload.entry.as_ptr(), size)
                    })?;
                    totalsize += size + 4;
                }
                let id = lref.item_id(i);
                let (ptr, _) = lref.item_raw(id);
                // SAFETY: live tuple bytes within the temp image.
                let size = MAXALIGN(unsafe { itup::index_tuple_size(ptr) });
                ::mcx::vec_append_bytes(&mut tupstore, unsafe {
                    core::slice::from_raw_parts(ptr, size)
                })?;
                totalsize += size + 4;
            }
            if off == maxoff + 1 {
                let size = MAXALIGN(payload.entry.size());
                // SAFETY: as above.
                ::mcx::vec_append_bytes(&mut tupstore, unsafe {
                    core::slice::from_raw_parts(payload.entry.as_ptr(), size)
                })?;
                totalsize += size + 4;
            }
        }

        gin_init_page_bytes(rpage.as_mut_bytes(), flags);
        gin_init_page_bytes(lpage.as_mut_bytes(), flags);

        let mut ptr = 0usize;
        let mut lsize = 0usize;
        let mut on_left = true;
        while ptr < tupstore.len() {
            let itup = &tupstore[ptr..];
            // SAFETY: workspace holds whole MAXALIGNed tuples.
            let size = unsafe { itup::index_tuple_size(itup.as_ptr()) };
            if on_left && lsize > totalsize / 2 {
                on_left = false;
            }
            if on_left {
                lsize += MAXALIGN(size) + 4;
            }
            let target = if on_left { &mut lpage } else { &mut rpage };
            // SAFETY: owned temp image.
            let mut pm = unsafe {
                PageMut::from_raw(core::ptr::NonNull::new(target.as_mut_bytes().as_mut_ptr()).unwrap())
            };
            if pm.add_item(&itup[..size], InvalidOffsetNumber, 0).is_none() {
                panic!("failed to add item to index page in \"{}\"", self.rel.name());
            }
            ptr += MAXALIGN(size);
        }

        let _ = payload_size;
        Ok((lpage, rpage))
    }
}

impl<'r> GinBt<'r> for EntryBtree<'_, 'r, '_> {
    const IS_DATA: bool = false;

    fn root_blkno(&self) -> BlockNumber {
        GIN_ROOT_BLKNO
    }
    fn is_build(&self) -> bool {
        self.is_build
    }
    fn full_scan(&self) -> bool {
        self.full_scan
    }

    /// entryLocateEntry.
    fn find_child_page(&self, page: &PageRef<'_>, frame: &mut Frame) -> BlockNumber {
        debug_assert!(!crate::GinPageIsLeaf(&page_opaque(page)));
        debug_assert!(!crate::GinPageIsData(&page_opaque(page)));

        if self.full_scan {
            frame.off = FirstOffsetNumber;
            frame.predictNumber *= page.max_offset_number() as u32;
            return self.get_leftmost_child(page);
        }

        let mut low = FirstOffsetNumber;
        let maxoff = page.max_offset_number();
        let mut high = maxoff;
        debug_assert!(high >= low);
        high += 1;

        let rightmost = crate::GinPageRightMost(&page_opaque(page));
        while high > low {
            let mid = low + (high - low) / 2;
            let result = if mid == maxoff && rightmost {
                -1
            } else {
                let id = page.item_id(mid);
                let itup = page.item_raw(id).0;
                self.compare_to(itup)
            };
            if result == 0 {
                frame.off = mid;
                let id = page.item_id(mid);
                // SAFETY: pin + lock held.
                return unsafe { gin_get_downlink(page.item_raw(id).0) };
            } else if result > 0 {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        debug_assert!(high >= FirstOffsetNumber && high <= maxoff);
        frame.off = high;
        let id = page.item_id(high);
        // SAFETY: pin + lock held.
        unsafe { gin_get_downlink(page.item_raw(id).0) }
    }

    /// entryGetLeftMostPage.
    fn get_leftmost_child(&self, page: &PageRef<'_>) -> BlockNumber {
        debug_assert!(page.max_offset_number() >= FirstOffsetNumber);
        let id = page.item_id(FirstOffsetNumber);
        // SAFETY: pin + lock held.
        unsafe { gin_get_downlink(page.item_raw(id).0) }
    }

    /// entryIsMoveRight.
    fn is_move_right(&self, page: &PageRef<'_>) -> bool {
        if crate::GinPageRightMost(&page_opaque(page)) {
            return false;
        }
        let itup = get_rightmost_tuple(page);
        self.compare_to(itup) > 0
    }

    /// entryFindChildPtr.
    fn find_child_ptr(
        &self,
        page: &PageRef<'_>,
        blkno: BlockNumber,
        stored_off: OffsetNumber,
    ) -> OffsetNumber {
        let mut maxoff = page.max_offset_number();
        let downlink_at = |i: OffsetNumber| -> BlockNumber {
            let id = page.item_id(i);
            // SAFETY: pin + lock held.
            unsafe { gin_get_downlink(page.item_raw(id).0) }
        };
        if stored_off >= FirstOffsetNumber && stored_off <= maxoff {
            if downlink_at(stored_off) == blkno {
                return stored_off;
            }
            for i in stored_off + 1..=maxoff {
                if downlink_at(i) == blkno {
                    return i;
                }
            }
            maxoff = stored_off - 1;
        }
        for i in FirstOffsetNumber..=maxoff {
            if downlink_at(i) == blkno {
                return i;
            }
        }
        InvalidOffsetNumber
    }

    /// entryBeginPlaceToPage.
    fn begin_place_to_page(
        &mut self,
        buf: Buffer,
        off: OffsetNumber,
        update_blkno: BlockNumber,
        _is_rightmost_insert_hint: bool,
    ) -> PgResult<GinPlace> {
        // SAFETY: pin + exclusive lock held.
        let fits = { self.is_enough_space(&unsafe { page_ref(buf) }, off) };
        if !fits {
            let (l, r) = self.split_page(buf, off, update_blkno)?;
            return Ok(GinPlace::Split(l, r));
        }
        Ok(GinPlace::Insert)
    }

    /// entryExecPlaceToPage.
    fn exec_place_to_page(
        &mut self,
        buf: Buffer,
        off: OffsetNumber,
        update_blkno: BlockNumber,
    ) -> PgResult<Vec<Vec<u8>>> {
        // SAFETY: pin + exclusive lock held.
        let mut page = unsafe { page_mut(buf) };
        self.prepare_page(&mut page, off, update_blkno);

        let payload = self.payload.as_ref().expect("entry insert payload");
        let size = payload.entry.size();
        // SAFETY: entry image is `size` bytes.
        let entry_bytes = unsafe { core::slice::from_raw_parts(payload.entry.as_ptr(), size) };
        // The image is MAXALIGN-padded; the true tuple length rides t_info.
        let tuplen = unsafe { itup::index_tuple_size(payload.entry.as_ptr()) };
        let placed = page.add_item(&entry_bytes[..tuplen], off, 0);
        if placed != Some(off) {
            panic!("failed to add item to index page in \"{}\"", self.rel.name());
        }
        bm::mark_buffer_dirty::call(buf)?;

        let mut out = Vec::with_capacity(2);
        out.push(
            crate::wal::ginxlog_insert_entry_header(off, payload.is_delete).to_vec(),
        );
        out.push(entry_bytes[..tuplen].to_vec());
        Ok(out)
    }

    /// entryPrepareDownlink.
    fn prepare_downlink(&mut self, lbuf: Buffer) -> PgResult<()> {
        // SAFETY: pin + exclusive lock held on lbuf.
        let (entry, _) = {
            let lpage = unsafe { page_ref(lbuf) };
            let itup = get_rightmost_tuple(&lpage);
            let is_leaf = crate::GinPageIsLeaf(&page_opaque(&lpage));
            (
                gin_form_interior_tuple(
                    self.scratch,
                    itup,
                    is_leaf,
                    bm::buffer_get_block_number::call(lbuf),
                )?,
                (),
            )
        };
        self.payload = Some(EntryPayload {
            entry,
            is_delete: false,
        });
        Ok(())
    }

    /// ginEntryFillRoot.
    fn fill_root(
        &self,
        root: &mut [u8],
        lblkno: BlockNumber,
        lpage: &[u8],
        rblkno: BlockNumber,
        rpage: &[u8],
    ) -> PgResult<()> {
        gin_entry_fill_root(self.scratch, root, lblkno, lpage, rblkno, rpage)
    }
}

/// ginEntryFillRoot over raw images (shared with redo).
pub(crate) fn gin_entry_fill_root(
    mcx: Mcx<'_>,
    root: &mut [u8],
    lblkno: BlockNumber,
    lpage: &[u8],
    rblkno: BlockNumber,
    rpage: &[u8],
) -> PgResult<()> {
    for (blkno, child) in [(lblkno, lpage), (rblkno, rpage)] {
        // SAFETY: child is a full temp page image.
        let cref = unsafe {
            PageRef::from_raw(core::ptr::NonNull::new(child.as_ptr().cast_mut()).unwrap())
        };
        let itup = get_rightmost_tuple(&cref);
        let is_leaf = crate::GinPageIsLeaf(&page_opaque(&cref));
        let interior = gin_form_interior_tuple(mcx, itup, is_leaf, blkno)?;
        // SAFETY: root is an owned temp image.
        let mut pm = unsafe {
            PageMut::from_raw(core::ptr::NonNull::new(root.as_mut_ptr()).unwrap())
        };
        // SAFETY: interior tuple image live for the call.
        let bytes = unsafe {
            core::slice::from_raw_parts(interior.as_ptr(), itup::index_tuple_size(interior.as_ptr()))
        };
        if pm.add_item(bytes, InvalidOffsetNumber, 0).is_none() {
            panic!("failed to add item to index root page");
        }
    }
    Ok(())
}
