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
use ::types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
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

/// Corrupt on-disk GIN entry tuple: the stored multicolumn attribute number
/// is outside the index's key-column range. Raised as ERRCODE_DATA_CORRUPTED.
#[cold]
#[inline(never)]
fn corrupt_gin_attrnum(colnum: u16, natts: u16) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "corrupted GIN multicolumn entry tuple: attribute number {colnum} \
             out of range (index has {natts} key columns)"
        ))
        .with_sqlstate(ERRCODE_DATA_CORRUPTED),
    )
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
    // On-disk format validation. C guards this only with Assert, which a
    // release build compiles out; a crafted page can then carry an attnum of
    // 0 or > natts. Left unchecked the bad value indexes the per-column
    // tupdesc out of bounds (gin_col_tupdesc: rd_att.attr(colnum - 1)) and
    // panics — and in a logical-replication apply/tablesync worker or the
    // startup WAL-redo thread an uncaught panic crash-loops the whole cluster.
    // Raise a catchable ERRCODE_DATA_CORRUPTED instead (recovered by
    // pg_error_from_panic at the apply/redo boundary, the node_funcs/pgrcolumnar
    // precedent for an ereport from an infallible call chain) so the operation
    // aborts the transaction cleanly rather than crashing the process.
    if colnum < FirstOffsetNumber || (colnum as u16) > state.natts {
        std::panic::panic_any(corrupt_gin_attrnum(colnum as u16, state.natts));
    }
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
    let (res, colnum) = if state.one_col {
        (
            itup::index_getattr(itup, 1, &rel.rd_att, &mut isnull),
            FirstOffsetNumber,
        )
    } else {
        let colnum = gintuple_get_attrnum(state, itup);
        let desc = gin_col_tupdesc(mcx, rel, colnum)?;
        (itup::index_getattr(itup, 2, &desc, &mut isnull), colnum)
    };
    if isnull {
        *category = gin_get_null_category(state, itup);
    } else {
        *category = GIN_CAT_NORM_KEY;
        // The key datum is borrowed straight from the (possibly attacker-crafted
        // or corrupted) on-disk tuple image. For a varlena key its length lives
        // in the datum's own header; downstream consumers (pending-list cleanup
        // in bulk.rs, partial-match scans in get.rs, vacuum, entry-tree splits)
        // trust that header to bound raw copies and hashes. A crafted 4-byte
        // header can declare up to ~1GB, so without a bound they read far past
        // the tuple and the 8KB page. Validate the declared size against the
        // containing tuple here — the single choke point every key-extraction
        // path funnels through — so a corrupt key raises a catchable error
        // instead of an out-of-bounds read. (C's gintuple_get_key lacks this
        // check because C's later memcpy would fault identically; in Rust the
        // from_raw_parts read is undefined behavior, so we harden the source.)
        let col = state.col(colnum);
        if !col.key_byval && col.key_len == -1 {
            validate_varlena_key(itup, res, rel.name())?;
        }
    }
    Ok(res)
}

/// Bound a page-borrowed varlena key by the tuple that contains it. `key` must
/// point inside `itup`'s image and its declared varlena size must not run past
/// the tuple's end (IndexTupleSize); otherwise the tuple is corrupt.
///
/// # Safety
/// `itup` is a live index-tuple image and `key` is a datum extracted from it.
unsafe fn validate_varlena_key(itup: ITup, key: Datum, relname: &str) -> PgResult<()> {
    let tup_start = itup as usize;
    // IndexTupleSize is read from t_info (a fixed, small field), so this bound
    // itself never reads out of range even for a hostile tuple.
    let tup_end = tup_start + itup::index_tuple_size(itup);
    let p = key.as_usize();
    // The datum must begin inside the tuple (leaving at least one header byte).
    if p < tup_start || p >= tup_end {
        return Err(gin_corrupt_key(relname));
    }
    let avail = tup_end - p;
    let ptr = p as *const u8;
    let ok = if ::types_tuple::varatt::varatt_is_1b_e(ptr) {
        // External TOAST pointers are never stored inline in GIN index tuples.
        false
    } else if ::types_tuple::varatt::varatt_is_1b(ptr) {
        // 1-byte header: size byte already read above (avail >= 1).
        ::types_tuple::varatt::varsize_1b(ptr) <= avail
    } else {
        // 4-byte header: the whole header must be inside the tuple before we
        // dereference it to read the declared length.
        avail >= ::types_tuple::varatt::VARHDRSZ
            && ::types_tuple::varatt::varsize_4b(ptr) <= avail
    };
    if !ok {
        return Err(gin_corrupt_key(relname));
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn gin_corrupt_key(relname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "corrupted varlena key in GIN index \"{relname}\": declared length exceeds tuple"
        ))
        .with_sqlstate(ERRCODE_DATA_CORRUPTED),
    )
}

#[cfg(test)]
mod validate_varlena_key_tests {
    use super::*;
    use ::types_tuple::varatt;

    // 8-byte MAXALIGN slack after the tuple so an OOB read (were the bound
    // missing) would land on our own memory rather than faulting the test.
    #[repr(C, align(8))]
    struct Buf([u8; 64]);

    /// Build a tuple image: 8-byte IndexTupleData header, then a 4-byte-header
    /// varlena whose declared total size is `declared`, and set IndexTupleSize
    /// so the tuple ends `slack` bytes past the varlena header start.
    fn make_tuple(declared: u32, tuple_size: usize) -> (Buf, usize) {
        let mut b = Buf([0u8; 64]);
        // Varlena 4-byte header at the key offset (INDEX_TUPLE_DATA_SIZE = 8).
        let word = varatt::set_varsize_4b_word(declared).to_ne_bytes();
        b.0[8..12].copy_from_slice(&word);
        // Derive after the slice reborrow to retain pointer provenance.
        let itup = b.0.as_mut_ptr();
        // t_info lives at offset 6; store IndexTupleSize (no null/var flags).
        // SAFETY: itup is a live, 8-aligned image.
        unsafe { itup::set_t_info(itup, tuple_size as u16) };
        (b, 8usize)
    }

    #[test]
    fn accepts_key_within_tuple() {
        // Declares total 8 (4 header + 4 data); tuple is 16 bytes, key at off 8
        // leaves 8 bytes available >= 8. Valid.
        let (mut b, key_off) = make_tuple(8, 16);
        let itup = b.0.as_mut_ptr() as ITup;
        let key = Datum::from_usize(unsafe { itup.add(key_off) } as usize);
        // SAFETY: itup/key are a live image built above.
        assert!(unsafe { validate_varlena_key(itup, key, "t") }.is_ok());
    }

    #[test]
    fn rejects_gigabyte_declared_length() {
        // 4-byte header declares ~1GB while the tuple is only 16 bytes: the
        // pre-fix code would from_raw_parts/read ~1GB past the page.
        let (mut b, key_off) = make_tuple(0x3FFF_FFFF, 16);
        let itup = b.0.as_mut_ptr() as ITup;
        let key = Datum::from_usize(unsafe { itup.add(key_off) } as usize);
        // SAFETY: as above.
        let err = unsafe { validate_varlena_key(itup, key, "t") }.err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
    }

    #[test]
    fn rejects_length_one_byte_past_tuple() {
        // Declares 9 but only 8 bytes remain in the tuple: off-by-one OOB.
        let (mut b, key_off) = make_tuple(9, 16);
        let itup = b.0.as_mut_ptr() as ITup;
        let key = Datum::from_usize(unsafe { itup.add(key_off) } as usize);
        // SAFETY: as above.
        assert!(unsafe { validate_varlena_key(itup, key, "t") }.is_err());
    }

    #[test]
    fn rejects_key_pointer_outside_tuple() {
        let (mut b, _) = make_tuple(8, 16);
        let itup = b.0.as_mut_ptr() as ITup;
        // Key pointer past the declared tuple end.
        let key = Datum::from_usize(unsafe { itup.add(32) } as usize);
        // SAFETY: as above.
        assert!(unsafe { validate_varlena_key(itup, key, "t") }.is_err());
    }
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
    let posting_offset = gin_get_posting_offset(itup);
    // The posting offset and item count are read straight from the on-disk
    // entry tuple's t_tid (block-number field = byte offset, ip_posid = count).
    // C's ginReadTuple trusts them and would fault identically on a bad memcpy,
    // but in Rust the raw from_raw_parts / read_unaligned below are undefined
    // behavior when they run past the tuple. A crafted page can declare a
    // posting offset up to 2^31-1 and a count up to 65535, turning any scan,
    // insert, or vacuum of the index into an attacker-positioned out-of-bounds
    // read. Bound the posting list by the tuple that contains it (the same
    // choke-point hardening gintuple_get_key applies to varlena keys) so a
    // corrupt tuple raises a catchable ERRCODE_DATA_CORRUPTED instead.
    let tup_size = itup::index_tuple_size(itup);
    // `avail` = bytes of posting list available inside the tuple. Guard the
    // subtraction first so an out-of-range offset can't underflow.
    let avail = tup_size
        .checked_sub(posting_offset)
        .ok_or_else(|| gin_corrupt_posting(posting_offset, nipd, tup_size))?;
    let ptr = itup.add(posting_offset);
    if gin_itup_is_compressed(itup) {
        if nipd > 0 {
            // Need the full segment header before reading the declared length,
            // then the whole segment must fit within the tuple.
            if avail < SizeOfGinPostingListHeader {
                return Err(gin_corrupt_posting(posting_offset, nipd, tup_size));
            }
            let before = out.len();
            let seglen = crate::postinglist::seg_size(core::slice::from_raw_parts(ptr, 8));
            if seglen > avail {
                return Err(gin_corrupt_posting(posting_offset, nipd, tup_size));
            }
            ginPostingListDecodeAllSegments(core::slice::from_raw_parts(ptr, seglen), out)?;
            if out.len() - before != nipd {
                // ginentrypage.c:176 elog(ERROR): XX000, catchable.
                return Err(Box::new(PgError::error(format!(
                    "number of items mismatch in GIN entry tuple, {} in tuple header, {} decoded",
                    nipd,
                    out.len() - before
                ))));
            }
        }
    } else {
        // Uncompressed posting list: nipd item pointers of 6 bytes each.
        if nipd
            .checked_mul(core::mem::size_of::<ItemPointerData>())
            .map_or(true, |need| need > avail)
        {
            return Err(gin_corrupt_posting(posting_offset, nipd, tup_size));
        }
        out.try_reserve(nipd).map_err(|_| crate::oom(nipd * 6))?;
        for i in 0..nipd {
            out.push(ptr.add(i * 6).cast::<ItemPointerData>().read_unaligned());
        }
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn gin_corrupt_posting(offset: usize, nipd: usize, tup_size: usize) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "corrupted GIN entry tuple: posting list (offset {offset}, {nipd} items) \
             exceeds tuple size {tup_size}"
        ))
        .with_sqlstate(ERRCODE_DATA_CORRUPTED),
    )
}

#[cfg(test)]
mod gin_read_tuple_tests {
    use super::*;
    use ::mcx::MemoryContext;

    // 8-aligned backing store with slack so an OOB read (were the bound
    // missing) would land in our own buffer rather than faulting the test.
    #[repr(C, align(8))]
    struct Buf([u8; 64]);

    /// Build an entry-tuple image with the given posting offset (in the t_tid
    /// block-number field, `compressed` sets GIN_ITUP_COMPRESSED), item count
    /// (in ip_posid), and IndexTupleSize (t_info at offset 6).
    fn make_tuple(offset: u32, nipd: OffsetNumber, compressed: bool, tuple_size: u16) -> Buf {
        let mut b = Buf([0u8; 64]);
        let itup = b.0.as_mut_ptr();
        let blk = if compressed { offset | GIN_ITUP_COMPRESSED } else { offset };
        // SAFETY: itup is a live, 8-aligned image large enough for the header.
        unsafe {
            set_t_tid_parts(itup, Some(blk), Some(nipd));
            itup::set_t_info(itup, tuple_size);
        }
        b
    }

    #[test]
    fn rejects_gigabyte_posting_offset() {
        // 2^30 offset, well past the 16-byte tuple: must error, not wild-read.
        let mut b = make_tuple(1 << 30, 3, false, 16);
        let itup = b.0.as_mut_ptr() as ITup;
        let ctx = MemoryContext::new_bump("gin_read_tuple_test");
        let mut out = ::mcx::vec_new_in::<ItemPointerData>(ctx.mcx());
        // SAFETY: itup is a live image built above.
        let res = unsafe { ginReadTuple(ctx.mcx(), itup, &mut out) };
        let err = res.err().expect("2^30 posting offset must be rejected");
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
    }

    #[test]
    fn rejects_uncompressed_count_past_tuple() {
        // Offset 8 is in-bounds, but 3 items * 6 bytes = 18 > avail (14-8=6).
        let mut b = make_tuple(8, 3, false, 14);
        let itup = b.0.as_mut_ptr() as ITup;
        let ctx = MemoryContext::new_bump("gin_read_tuple_test");
        let mut out = ::mcx::vec_new_in::<ItemPointerData>(ctx.mcx());
        // SAFETY: itup is a live image built above.
        let res = unsafe { ginReadTuple(ctx.mcx(), itup, &mut out) };
        assert!(res.is_err(), "overlong item count must be rejected");
    }

    #[test]
    fn accepts_in_bounds_uncompressed() {
        // Offset 8, one item: 6 bytes fit within the 14-byte tuple.
        let mut b = make_tuple(8, 1, false, 14);
        let itup = b.0.as_mut_ptr() as ITup;
        let ctx = MemoryContext::new_bump("gin_read_tuple_test");
        let mut out = ::mcx::vec_new_in::<ItemPointerData>(ctx.mcx());
        // SAFETY: itup is a live image built above.
        unsafe { ginReadTuple(ctx.mcx(), itup, &mut out) }.expect("valid tuple");
        assert_eq!(out.len(), 1);
    }
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

    /// The key extraction can raise (ERRCODE_DATA_CORRUPTED from a crafted
    /// varlena key, scratch OOM); the search callbacks propagate it.
    pub(crate) fn compare_to(&self, itup: ITup) -> PgResult<i32> {
        let mut category = GIN_CAT_NORM_KEY;
        // SAFETY: pin held by the caller of the search callbacks.
        let (tup_attnum, key) = unsafe {
            (
                gintuple_get_attrnum(self.state, itup),
                gintuple_get_key(self.scratch, self.rel, self.state, itup, &mut category)?,
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
                // ginentrypage.c:688 elog(ERROR): XX000, catchable.
                return Err(failed_to_add_item(self.rel));
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
    fn find_child_page(&self, page: &PageRef<'_>, frame: &mut Frame) -> PgResult<BlockNumber> {
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
                self.compare_to(itup)?
            };
            if result == 0 {
                frame.off = mid;
                let id = page.item_id(mid);
                // SAFETY: pin + lock held.
                return Ok(unsafe { gin_get_downlink(page.item_raw(id).0) });
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
        Ok(unsafe { gin_get_downlink(page.item_raw(id).0) })
    }

    /// entryGetLeftMostPage.
    fn get_leftmost_child(&self, page: &PageRef<'_>) -> PgResult<BlockNumber> {
        debug_assert!(page.max_offset_number() >= FirstOffsetNumber);
        let id = page.item_id(FirstOffsetNumber);
        // SAFETY: pin + lock held.
        Ok(unsafe { gin_get_downlink(page.item_raw(id).0) })
    }

    /// entryIsMoveRight.
    fn is_move_right(&self, page: &PageRef<'_>) -> PgResult<bool> {
        if crate::GinPageRightMost(&page_opaque(page)) {
            return Ok(false);
        }
        let itup = get_rightmost_tuple(page);
        Ok(self.compare_to(itup)? > 0)
    }

    /// entryFindChildPtr.
    fn find_child_ptr(
        &self,
        page: &PageRef<'_>,
        blkno: BlockNumber,
        stored_off: OffsetNumber,
    ) -> PgResult<OffsetNumber> {
        let mut maxoff = page.max_offset_number();
        let downlink_at = |i: OffsetNumber| -> BlockNumber {
            let id = page.item_id(i);
            // SAFETY: pin + lock held.
            unsafe { gin_get_downlink(page.item_raw(id).0) }
        };
        if stored_off >= FirstOffsetNumber && stored_off <= maxoff {
            if downlink_at(stored_off) == blkno {
                return Ok(stored_off);
            }
            for i in stored_off + 1..=maxoff {
                if downlink_at(i) == blkno {
                    return Ok(i);
                }
            }
            maxoff = stored_off - 1;
        }
        for i in FirstOffsetNumber..=maxoff {
            if downlink_at(i) == blkno {
                return Ok(i);
            }
        }
        Ok(InvalidOffsetNumber)
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
            // ginentrypage.c:570 elog(ERROR): XX000 (PANIC under the
            // caller's critical section, as in C).
            return Err(failed_to_add_item(self.rel));
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
            // ginentrypage.c:731 elog(ERROR): XX000, catchable.
            return Err(Box::new(PgError::error("failed to add item to index root page")));
        }
    }
    Ok(())
}

/// ginentrypage.c:570/688 elog(ERROR, "failed to add item to index page in \"%s\"").
#[cold]
#[inline(never)]
fn failed_to_add_item(rel: &Relation<'_>) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "failed to add item to index page in \"{}\"",
        rel.name()
    )))
}

#[cfg(test)]
mod attrnum_tests {
    use super::*;
    use ::gin_vocab::{GinColState, GinCompareFn, GinState, GIN_MAX_KEY_COLS};
    use ::types_error::pg_error_from_panic;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    // 8-byte aligned backing store so t_info's aligned u16 read at offset 6 is
    // well-defined (real page images are BLCKSZ-aligned).
    #[repr(align(8))]
    struct Tuple([u8; 16]);

    fn dummy_col() -> GinColState {
        GinColState::array_ops(GinCompareFn::Int4, true, 4)
    }

    fn multicol_state(natts: u16) -> GinState {
        GinState {
            natts,
            one_col: false,
            cols: [dummy_col(); GIN_MAX_KEY_COLS],
        }
    }

    /// Minimal on-disk entry-tuple image (no nulls) whose leading multicolumn
    /// int2 attribute carries `attnum`. With no null bitmap the data offset is
    /// 8, so the attnum int2 sits at byte 8.
    fn craft_tuple(attnum: u16) -> Tuple {
        let mut buf = Tuple([0u8; 16]);
        // t_info at offset 6: tuple size, neither the null nor the var mask set.
        buf.0[6..8].copy_from_slice(&12u16.to_le_bytes());
        buf.0[8..10].copy_from_slice(&attnum.to_le_bytes());
        buf
    }

    fn expect_corruption(attnum: u16) {
        let state = multicol_state(2);
        let buf = craft_tuple(attnum);
        let payload = catch_unwind(AssertUnwindSafe(|| unsafe {
            gintuple_get_attrnum(&state, buf.0.as_ptr())
        }))
        .err().expect("out-of-range attnum must raise a catchable error, not return");
        let err = pg_error_from_panic(payload)
            .unwrap_or_else(|_| panic!("payload must be a structured PgError, not a raw panic"));
        assert_eq!(
            err.sqlstate(),
            ERRCODE_DATA_CORRUPTED,
            "corrupt GIN attnum must surface as ERRCODE_DATA_CORRUPTED"
        );
    }

    #[test]
    fn attrnum_above_natts_is_data_corrupted() {
        expect_corruption(99);
    }

    #[test]
    fn attrnum_zero_is_data_corrupted() {
        expect_corruption(0);
    }

    #[test]
    fn valid_attrnum_returns_cleanly() {
        let state = multicol_state(2);
        let buf = craft_tuple(2);
        // SAFETY: well-formed in-range image.
        let got = unsafe { gintuple_get_attrnum(&state, buf.0.as_ptr()) };
        assert_eq!(got, 2);
    }
}
