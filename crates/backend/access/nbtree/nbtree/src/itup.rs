//! itup.h + nbtree.h tuple macros over raw on-page bytes. Unsafe kernel:
//! `itup` must point at a live, MAXALIGNed index tuple (pin held). Moves to
//! the common indextuple unit when that lands.

use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::types_core::{AttrNumber, INDEX_MAX_KEYS};
use ::types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_TOO_MANY_COLUMNS};
use ::types_nbtree::{BT_IS_POSTING, BT_OFFSET_MASK, BT_PIVOT_HEAP_TID_ATTR, INDEX_ALT_TID_MASK};
use ::types_tuple::itemptr::{ItemPointerData, ItemPointerGetBlockNumberNoCheck};
use ::types_tuple::tupdesc::CompactAttribute;
use ::types_tuple::tupmacs::{
    att_addlength_pointer, att_isnull, att_nominal_alignby, att_pointer_alignby, fetchatt,
};
use ::types_tuple::varatt::{
    varatt_is_1b, varatt_is_1b_e, varsize_1b, varsize_4b, vartag_external, VARHDRSZ,
    VARHDRSZ_EXTERNAL, VARHDRSZ_SHORT,
};
use ::types_tuple::TupleDescData;

pub const INDEX_SIZE_MASK: u16 = 0x1FFF;
pub const INDEX_VAR_MASK: u16 = 0x4000;
pub const INDEX_NULL_MASK: u16 = 0x8000;

const INDEX_TUPLE_DATA_SIZE: usize = 8;
const INDEX_TUPLE_DATA_WITH_NULLS_SIZE: usize = 16;

pub type ITup = *const u8;

#[inline]
pub unsafe fn t_info(itup: ITup) -> u16 {
    itup.add(6).cast::<u16>().read()
}

#[inline]
pub unsafe fn t_tid(itup: ITup) -> ItemPointerData {
    itup.cast::<ItemPointerData>().read()
}

#[inline]
pub unsafe fn index_tuple_size(itup: ITup) -> usize {
    (t_info(itup) & INDEX_SIZE_MASK) as usize
}

#[inline]
pub unsafe fn index_tuple_has_nulls(itup: ITup) -> bool {
    (t_info(itup) & INDEX_NULL_MASK) != 0
}

#[inline]
pub const fn index_info_find_data_offset(info: u16) -> usize {
    if info & INDEX_NULL_MASK == 0 {
        INDEX_TUPLE_DATA_SIZE
    } else {
        INDEX_TUPLE_DATA_WITH_NULLS_SIZE
    }
}

#[inline]
pub unsafe fn bt_tuple_is_pivot(itup: ITup) -> bool {
    (t_info(itup) & INDEX_ALT_TID_MASK) != 0 && (t_tid(itup).ip_posid & BT_IS_POSTING) == 0
}

#[inline]
pub unsafe fn bt_tuple_is_posting(itup: ITup) -> bool {
    (t_info(itup) & INDEX_ALT_TID_MASK) != 0 && (t_tid(itup).ip_posid & BT_IS_POSTING) != 0
}

#[inline]
pub unsafe fn bt_tuple_get_nposting(itup: ITup) -> usize {
    debug_assert!(bt_tuple_is_posting(itup));
    (t_tid(itup).ip_posid & BT_OFFSET_MASK) as usize
}

#[inline]
pub unsafe fn bt_tuple_get_posting_offset(itup: ITup) -> usize {
    debug_assert!(bt_tuple_is_posting(itup));
    ItemPointerGetBlockNumberNoCheck(&t_tid(itup)) as usize
}

#[inline]
pub unsafe fn bt_tuple_get_posting_n(itup: ITup, n: usize) -> ItemPointerData {
    itup.add(bt_tuple_get_posting_offset(itup) + n * core::mem::size_of::<ItemPointerData>())
        .cast::<ItemPointerData>()
        .read_unaligned()
}

#[inline]
pub unsafe fn bt_tuple_get_natts(itup: ITup, indnatts: i32) -> i32 {
    if bt_tuple_is_pivot(itup) {
        (t_tid(itup).ip_posid & BT_OFFSET_MASK) as i32
    } else {
        indnatts
    }
}

#[inline]
pub unsafe fn bt_tuple_get_downlink(pivot: ITup) -> ::types_core::BlockNumber {
    ItemPointerGetBlockNumberNoCheck(&t_tid(pivot))
}

pub unsafe fn bt_tuple_get_heap_tid(itup: ITup) -> Option<ItemPointerData> {
    if bt_tuple_is_pivot(itup) {
        if (t_tid(itup).ip_posid & BT_PIVOT_HEAP_TID_ATTR) != 0 {
            let off = index_tuple_size(itup) - core::mem::size_of::<ItemPointerData>();
            return Some(itup.add(off).cast::<ItemPointerData>().read_unaligned());
        }
        None
    } else if bt_tuple_is_posting(itup) {
        Some(bt_tuple_get_posting_n(itup, 0))
    } else {
        Some(t_tid(itup))
    }
}

pub unsafe fn bt_tuple_get_max_heap_tid(itup: ITup) -> ItemPointerData {
    debug_assert!(!bt_tuple_is_pivot(itup));
    if bt_tuple_is_posting(itup) {
        bt_tuple_get_posting_n(itup, bt_tuple_get_nposting(itup) - 1)
    } else {
        t_tid(itup)
    }
}

pub const fn maxalign(l: usize) -> usize {
    (l + 7) & !7
}

/// # Safety
/// `itup` per module contract; the image is writable (owned scratch, never a
/// locked page).
#[inline]
pub unsafe fn set_t_info(itup: *mut u8, info: u16) {
    itup.add(6).cast::<u16>().write(info);
}

/// # Safety
/// As [`set_t_info`].
#[inline]
pub unsafe fn set_t_tid(itup: *mut u8, tid: ItemPointerData) {
    itup.cast::<ItemPointerData>().write_unaligned(tid);
}

/// BTreeTupleSetNAtts.
///
/// # Safety
/// As [`set_t_info`].
pub unsafe fn bt_tuple_set_natts(itup: *mut u8, nkeyatts: u16, heaptid: bool) {
    debug_assert!(nkeyatts <= INDEX_MAX_KEYS as u16);
    debug_assert!(nkeyatts & BT_STATUS_OFFSET_MASK == 0);
    debug_assert!(!heaptid || nkeyatts != 0);
    set_t_info(itup, t_info(itup) | INDEX_ALT_TID_MASK);
    let mut tid = t_tid(itup);
    tid.ip_posid = if heaptid {
        nkeyatts | BT_PIVOT_HEAP_TID_ATTR
    } else {
        nkeyatts
    };
    set_t_tid(itup, tid);
}

const BT_STATUS_OFFSET_MASK: u16 = !BT_OFFSET_MASK;

/// BTreeTupleSetPosting.
///
/// # Safety
/// As [`set_t_info`].
pub unsafe fn bt_tuple_set_posting(itup: *mut u8, nhtids: u16, postingoffset: usize) {
    debug_assert!(nhtids > 1);
    debug_assert!(nhtids & BT_STATUS_OFFSET_MASK == 0);
    debug_assert!(postingoffset == maxalign(postingoffset));
    debug_assert!(postingoffset < INDEX_SIZE_MASK as usize);
    debug_assert!(!bt_tuple_is_pivot(itup));
    set_t_info(itup, t_info(itup) | INDEX_ALT_TID_MASK);
    let mut tid = t_tid(itup);
    tid.ip_posid = nhtids | BT_IS_POSTING;
    tid.ip_blkid.bi_hi = (postingoffset >> 16) as u16;
    tid.ip_blkid.bi_lo = (postingoffset & 0xffff) as u16;
    set_t_tid(itup, tid);
}

/// BTreeTupleSetDownLink.
///
/// # Safety
/// As [`set_t_info`].
pub unsafe fn bt_tuple_set_downlink(itup: *mut u8, blkno: ::types_core::BlockNumber) {
    let mut tid = t_tid(itup);
    tid.ip_blkid.bi_hi = (blkno >> 16) as u16;
    tid.ip_blkid.bi_lo = (blkno & 0xffff) as u16;
    set_t_tid(itup, tid);
}

#[track_caller]
#[cold]
#[inline(never)]
fn index_row_too_large(size: usize) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "index row requires {size} bytes, maximum size is {}",
            INDEX_SIZE_MASK
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
    )
}

pub const INDEX_TUPLE_HEADER_SIZE: usize = 8;

// MAXALIGNed index-tuple image (u64-backed: PgVec<u8> only guarantees align 1,
// the itup module contract requires 8).
pub struct ItupBuf<'mcx>(PgVec<'mcx, u64>);

impl<'mcx> ItupBuf<'mcx> {
    pub fn with_size(mcx: Mcx<'mcx>, size: usize) -> PgResult<Self> {
        debug_assert!(size == maxalign(size));
        Ok(ItupBuf(::mcx::vec_from_elem_in(mcx, 0u64, size / 8)))
    }

    #[inline]
    pub fn as_ptr(&self) -> ITup {
        self.0.as_ptr().cast()
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.0.as_mut_ptr().cast()
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.0.len() * 8
    }
}

// VARATT_IS_COMPRESSED (varatt.h) on a 4B varlena header.
// SAFETY: p live, at least 1 readable byte.
unsafe fn varatt_is_compressed(p: *const u8) -> bool {
    if cfg!(target_endian = "little") {
        (*p & 0x03) == 0x02
    } else {
        (*p & 0xC0) == 0x40
    }
}

// SAFETY: p is a live non-external varlena.
unsafe fn varlena_image<'a>(p: *const u8) -> &'a [u8] {
    use ::types_tuple::varatt::varsize_any;
    core::slice::from_raw_parts(p, varsize_any(p))
}

/// index_form_tuple (indextuple.c), MAXALIGNed image in an mcx-backed buffer.
/// TOAST_INDEX_HACK: detoast external attrs, then in-line-compress anything
/// still over TOAST_INDEX_TARGET (extended/main storage only) so wide values
/// don't inflate the index tuple.
pub fn index_form_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'_>,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<ItupBuf<'mcx>> {
    use ::types_tuple::varatt::{varatt_is_1b, varatt_is_1b_e, varsize_any};
    use ::types_tuple::{TYPSTORAGE_EXTENDED, TYPSTORAGE_MAIN};

    let natts = tupdesc.natts as usize;
    if natts > INDEX_MAX_KEYS as usize {
        return Err(too_many_index_columns(natts));
    }

    let mut untoasted: [Datum; INDEX_MAX_KEYS as usize] =
        [Datum::from_usize(0); INDEX_MAX_KEYS as usize];
    untoasted[..natts].copy_from_slice(&values[..natts]);

    // heaptoast.h:68: TOAST_INDEX_TARGET = MaxHeapTupleSize / 16 (510 bytes);
    // any wider extended/main varlena is compressed in-line. (The btree 1/3
    // page limit, MaximumBytesPerTuple(4) = 2040, is a different number and
    // is enforced by _bt_check_third_page.)
    const TOAST_INDEX_TARGET: usize = ::types_storage::bufpage::MaxHeapTupleSize / 16;

    for i in 0..natts {
        let att = tupdesc.compact_attr(i);
        if isnull[i] || att.attlen != -1 {
            continue;
        }
        // SAFETY: non-null varlena datums carry live pointers (caller contract).
        unsafe {
            let mut p = untoasted[i].as_usize() as *const u8;
            if varatt_is_1b_e(p) {
                let flat = ::detoast::detoast_external_attr(mcx, varlena_image(p))?;
                untoasted[i] = Datum::from_usize(flat.leak().as_ptr() as usize);
                p = untoasted[i].as_usize() as *const u8;
            }
            if !varatt_is_1b(p) && !varatt_is_compressed(p) && varsize_any(p) > TOAST_INDEX_TARGET {
                let storage = tupdesc.attr(i).attstorage;
                if storage == TYPSTORAGE_EXTENDED || storage == TYPSTORAGE_MAIN {
                    let compression = tupdesc.attr(i).attcompression;
                    if let Some(cvalue) = ::heaptoast_seams::toast_compress_datum::call(
                        mcx,
                        varlena_image(p),
                        compression,
                    )? {
                        untoasted[i] = Datum::from_usize(cvalue.leak().as_ptr() as usize);
                    }
                }
            }
        }
    }
    let values = &untoasted[..natts];

    let hasnull = isnull[..natts].contains(&true);
    let mut infomask: u16 = if hasnull { INDEX_NULL_MASK } else { 0 };
    let hoff = index_info_find_data_offset(infomask);
    let data_size = ::heaptuple::heap_compute_data_size(tupdesc, values, isnull);
    let size = maxalign(hoff + data_size);
    if size & INDEX_SIZE_MASK as usize != size {
        return Err(index_row_too_large(size));
    }

    let mut buf = ItupBuf::with_size(mcx, size)?;
    let tp = buf.as_mut_ptr();
    let mut tupmask: u16 = 0;
    // SAFETY: buf holds hoff + data_size zeroed bytes; bitmap area pre-zeroed.
    unsafe {
        ::heaptuple::heap_fill_tuple(
            tupdesc,
            values,
            isnull,
            tp.add(hoff),
            data_size,
            &mut tupmask,
            if hasnull {
                Some(tp.add(INDEX_TUPLE_HEADER_SIZE))
            } else {
                None
            },
        );
        if tupmask & ::types_tuple::HEAP_HASVARWIDTH != 0 {
            infomask |= INDEX_VAR_MASK;
        }
        infomask |= size as u16;
        set_t_info(tp, infomask);
    }
    Ok(buf)
}

/// CopyIndexTuple.
///
/// # Safety
/// `itup` per module contract.
pub unsafe fn copy_index_tuple<'mcx>(mcx: Mcx<'mcx>, itup: ITup) -> PgResult<ItupBuf<'mcx>> {
    let size = maxalign(index_tuple_size(itup));
    let mut buf = ItupBuf::with_size(mcx, size)?;
    core::ptr::copy_nonoverlapping(itup, buf.as_mut_ptr(), index_tuple_size(itup));
    Ok(buf)
}

/// index_truncate_tuple: `source` copied with only `leavenatts` attributes.
///
/// # Safety
/// `source` per module contract; `leavenatts <= tupdesc.natts`.
pub(crate) unsafe fn index_truncate_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'_>,
    source: ITup,
    leavenatts: usize,
) -> PgResult<ItupBuf<'mcx>> {
    debug_assert!(leavenatts <= tupdesc.natts as usize);
    if leavenatts == tupdesc.natts as usize {
        return copy_index_tuple(mcx, source);
    }

    // CreateTupleDescTruncatedCopy (tupdesc.c): both the compact attributes
    // (heap_fill_tuple) and pg_attribute (index_form_tuple reads attstorage /
    // attcompression of a retained varlena still wider than
    // TOAST_INDEX_TARGET) for the kept columns.
    let mut compact = ::mcx::vec_with_capacity_in(mcx, leavenatts)?;
    for i in 0..leavenatts {
        compact.push(tupdesc.compact_attr(i).clone());
    }
    let mut attrs = ::mcx::vec_with_capacity_in(mcx, leavenatts)?;
    for att in tupdesc.attrs.iter().take(leavenatts) {
        attrs.push(*att);
    }
    let truncdesc = TupleDescData {
        natts: leavenatts as i32,
        tdtypeid: tupdesc.tdtypeid,
        tdtypmod: tupdesc.tdtypmod,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    };

    let mut values = [Datum::null(); INDEX_MAX_KEYS as usize];
    let mut isnull = [false; INDEX_MAX_KEYS as usize];
    for i in 0..leavenatts {
        values[i] = index_getattr(source, (i + 1) as AttrNumber, tupdesc, &mut isnull[i]);
    }
    let mut truncated = index_form_tuple(
        mcx,
        &truncdesc,
        &values[..leavenatts],
        &isnull[..leavenatts],
    )?;
    set_t_tid(truncated.as_mut_ptr(), t_tid(source));
    debug_assert!(index_tuple_size(truncated.as_ptr()) <= index_tuple_size(source));
    Ok(truncated)
}

/// index_getattr: borrowed deform — by-ref values point into the page image
/// (family-2 rule); attcacheoff live via CompactAttribute (rule-5).
///
/// # Safety
/// `itup` per module contract; `attnum` in `1..=natts` for this tuple/desc.
#[inline]
pub unsafe fn index_getattr(
    itup: ITup,
    attnum: AttrNumber,
    tupdesc: &TupleDescData<'_>,
    isnull: &mut bool,
) -> Datum {
    debug_assert!(attnum >= 1);
    *isnull = false;
    let a = (attnum - 1) as usize;
    if !index_tuple_has_nulls(itup) {
        let att = tupdesc.compact_attr(a);
        if att.attcacheoff.get() >= 0 {
            return fetchatt(
                att,
                itup.add(INDEX_TUPLE_DATA_SIZE + att.attcacheoff.get() as usize),
            );
        }
        nocache_index_getattr(itup, attnum, tupdesc)
    } else {
        if att_isnull(a, itup.add(INDEX_TUPLE_DATA_SIZE)) {
            *isnull = true;
            return Datum::null();
        }
        nocache_index_getattr(itup, attnum, tupdesc)
    }
}

/// # Safety
/// As [`index_getattr`].
unsafe fn nocache_index_getattr(
    itup: ITup,
    attnum: AttrNumber,
    tupdesc: &TupleDescData<'_>,
) -> Datum {
    let info = t_info(itup);
    let hasnulls = (info & INDEX_NULL_MASK) != 0;
    let mut slow = false;
    let attnum = (attnum - 1) as usize;
    let bp = itup.add(INDEX_TUPLE_DATA_SIZE);

    if hasnulls {
        let byte = attnum >> 3;
        let finalbit = attnum & 0x07;
        if (!*bp.add(byte)) & ((1 << finalbit) - 1) != 0 {
            slow = true;
        } else {
            for i in 0..byte {
                if *bp.add(i) != 0xFF {
                    slow = true;
                    break;
                }
            }
        }
    }

    let tp = itup.add(index_info_find_data_offset(info));
    let atts = &tupdesc.compact_attrs[..];
    debug_assert!(attnum < atts.len());
    let mut off: usize;

    if !slow {
        let att = atts.get_unchecked(attnum);
        if att.attcacheoff.get() >= 0 {
            return fetchatt(att, tp.add(att.attcacheoff.get() as usize));
        }

        if (info & INDEX_VAR_MASK) != 0 {
            for j in 0..=attnum {
                if atts[j].attlen <= 0 {
                    slow = true;
                    break;
                }
            }
        }
    }

    if !slow {
        let natts = atts.len();
        let mut j = 1;

        atts[0].attcacheoff.set(0);
        while j < natts && atts[j].attcacheoff.get() > 0 {
            j += 1;
        }

        off = atts[j - 1].attcacheoff.get() as usize + atts[j - 1].attlen as usize;

        while j < natts {
            let att = &atts[j];
            if att.attlen <= 0 {
                break;
            }
            off = att_nominal_alignby(off, att.attalignby);
            att.attcacheoff.set(off as i32);
            off += att.attlen as usize;
            j += 1;
        }

        debug_assert!(j > attnum);
        off = atts.get_unchecked(attnum).attcacheoff.get() as usize;
    } else {
        let mut usecache = true;
        off = 0;
        let watts = atts.get_unchecked(..=attnum);
        let mut i = 0;
        loop {
            let att = &watts[i];
            let attlen = att.attlen;
            if hasnulls && att_isnull(i, bp) {
                usecache = false;
                i += 1;
                continue;
            }

            if usecache && att.attcacheoff.get() >= 0 {
                off = att.attcacheoff.get() as usize;
            } else if attlen == -1 {
                if usecache && off == att_nominal_alignby(off, att.attalignby) {
                    att.attcacheoff.set(off as i32);
                } else {
                    off = att_pointer_alignby(off, att.attalignby, -1, tp.add(off));
                    usecache = false;
                }
            } else {
                off = att_nominal_alignby(off, att.attalignby);
                if usecache {
                    att.attcacheoff.set(off as i32);
                }
            }

            if i == attnum {
                break;
            }

            off = att_addlength_pointer(off, attlen as i32, tp.add(off));
            if usecache && attlen <= 0 {
                usecache = false;
            }
            i += 1;
        }
    }

    fetchatt(atts.get_unchecked(attnum), tp.add(off))
}

/// Validate that an on-page index tuple's internal structure lies entirely
/// within `lp_len` — the line-pointer extent that bounds the readable image —
/// before any code reads its header, null bitmap, or by-reference (varlena /
/// cstring) attribute data. The deform path ([`index_getattr`]) advances
/// attribute offsets by length words read from the tuple bytes themselves; on
/// an attacker-crafted on-disk tuple those claimed lengths can point past the
/// page, turning a deform into an out-of-bounds read. Upstream C performs no
/// such check, so this is an added engine-level guard: callers deforming tuples
/// whose bytes are not trusted (e.g. GiST index-only-scan reconstruction) must
/// run this first and raise a corruption error when it returns `false`, rather
/// than deform out-of-bounds bytes.
///
/// `atts` describes the attributes actually present in the tuple (leaf vs. non-
/// leaf descriptor). Returns `true` iff every header field and every attribute
/// (including each varlena/cstring payload) is provably contained in
/// `[itup, itup + lp_len)`.
///
/// # Safety
/// `itup` points at `lp_len` readable bytes (the validated line-pointer extent).
pub unsafe fn index_tuple_verify(itup: ITup, lp_len: usize, atts: &[CompactAttribute]) -> bool {
    // The fixed IndexTuple header (t_tid + t_info) must be present before its
    // size/flag words can be read — guards lp_len in 0..8.
    if lp_len < INDEX_TUPLE_DATA_SIZE {
        return false;
    }
    let info = t_info(itup);
    let size = (info & INDEX_SIZE_MASK) as usize;
    // The tuple's own claimed size must cover the header and fit the extent.
    if size < INDEX_TUPLE_DATA_SIZE || size > lp_len {
        return false;
    }
    let hoff = index_info_find_data_offset(info);
    // Null bitmap (when present) must lie within the claimed size.
    if hoff > size {
        return false;
    }

    let hasnulls = (info & INDEX_NULL_MASK) != 0;
    // Null bitmap base; the data area starts after the (optional) bitmap.
    let bp = itup.add(INDEX_TUPLE_DATA_SIZE);
    let tp = itup.add(hoff);
    // Everything an attribute walk may touch must stay within the data area.
    let data_len = size - hoff;

    let mut off: usize = 0;
    for (i, att) in atts.iter().enumerate() {
        if hasnulls && att_isnull(i, bp) {
            continue;
        }
        let attlen = att.attlen as i32;
        if attlen == -1 {
            // Varlena. Peeking the pad byte to decide alignment needs one
            // readable byte; the (possibly aligned) header needs more.
            if off >= data_len {
                return false;
            }
            // att_pointer_alignby reads tp[off] to distinguish a short header
            // (1-byte, unaligned) from a 4-byte header (aligned).
            off = att_pointer_alignby(off, att.attalignby, -1, tp.add(off));
            if off >= data_len {
                return false;
            }
            let p = tp.add(off);
            let this = if varatt_is_1b_e(p) {
                // External/expanded TOAST pointer: 2-byte header + tag body.
                if off + VARHDRSZ_EXTERNAL > data_len {
                    return false;
                }
                let tagsz = match vartag_external(p) {
                    // varatt_indirect / varatt_expanded (8) ; varatt_external (16).
                    1 | 2 | 3 => 8,
                    18 => 16,
                    _ => return false,
                };
                VARHDRSZ_EXTERNAL + tagsz
            } else if varatt_is_1b(p) {
                // Short 1-byte header: total length is self-contained.
                varsize_1b(p)
            } else {
                // 4-byte header: the length word itself must be in bounds.
                if off + VARHDRSZ > data_len {
                    return false;
                }
                varsize_4b(p)
            };
            // Header length must be sane and the whole datum must fit.
            if this < VARHDRSZ_SHORT || this > data_len - off {
                return false;
            }
            off += this;
        } else if attlen == -2 {
            // Cstring: aligned, then NUL-terminated within the data area.
            off = att_nominal_alignby(off, att.attalignby);
            if off >= data_len {
                return false;
            }
            let mut n = 0usize;
            while off + n < data_len && *tp.add(off + n) != 0 {
                n += 1;
            }
            // A missing terminator means the string runs off the extent.
            if off + n >= data_len {
                return false;
            }
            off += n + 1;
        } else if attlen > 0 {
            // Fixed length: align then require the whole field in bounds.
            off = att_nominal_alignby(off, att.attalignby);
            let l = attlen as usize;
            if off > data_len || l > data_len - off {
                return false;
            }
            off += l;
        } else {
            // attlen == 0 (or otherwise invalid) is never a legal on-page shape.
            return false;
        }
    }
    true
}

#[cfg(test)]
mod index_tuple_verify_tests {
    use super::*;
    use ::types_tuple::varatt::set_varsize_4b_word;
    use core::cell::Cell;

    // 8-byte aligned scratch image (index tuples are MAXALIGNed on-page).
    #[repr(align(8))]
    struct Image([u8; 64]);

    fn att(attlen: i16, attbyval: bool, attalignby: u8) -> CompactAttribute {
        CompactAttribute {
            attcacheoff: Cell::new(-1),
            attlen,
            attbyval,
            attispackable: attlen < 0,
            atthasmissing: false,
            attisdropped: false,
            attgenerated: false,
            attnullability: 0,
            attalignby,
        }
    }

    // Write the fixed IndexTuple header (t_info at byte 6) with no t_tid needs.
    fn set_info(image: &mut Image, info: u16) {
        image.0[6..8].copy_from_slice(&info.to_ne_bytes());
    }

    // A single int4 in a 12-byte tuple validates cleanly (no false positive).
    #[test]
    fn well_formed_fixed_attribute_passes() {
        let mut image = Image([0u8; 64]);
        let size = INDEX_TUPLE_DATA_SIZE + 4;
        set_info(&mut image, size as u16);
        let atts = vec![att(4, true, 4)];
        // SAFETY: image is 8-aligned and holds `size` readable bytes.
        assert!(unsafe { index_tuple_verify(image.0.as_ptr(), size, &atts) });
    }

    // A 4-byte varlena header claiming a length past the tuple body is rejected
    // rather than driving an out-of-bounds read.
    #[test]
    fn lying_varlena_header_is_rejected() {
        let mut image = Image([0u8; 64]);
        // 8-byte data area; header at byte 8 claims a 100-byte varlena.
        let size = INDEX_TUPLE_DATA_SIZE + 8;
        set_info(&mut image, (size as u16) | INDEX_VAR_MASK);
        // SAFETY: writing the 4-byte header within the data area.
        unsafe {
            image
                .0
                .as_mut_ptr()
                .add(INDEX_TUPLE_DATA_SIZE)
                .cast::<u32>()
                .write_unaligned(set_varsize_4b_word(100));
        }
        let atts = vec![att(-1, false, 4)];
        // SAFETY: image holds `size` readable bytes.
        assert!(!unsafe { index_tuple_verify(image.0.as_ptr(), size, &atts) });
    }

    // A well-formed varlena (4-byte header of exactly its own length) passes.
    #[test]
    fn well_formed_varlena_passes() {
        let mut image = Image([0u8; 64]);
        let size = INDEX_TUPLE_DATA_SIZE + 8;
        set_info(&mut image, (size as u16) | INDEX_VAR_MASK);
        // A 4-byte varlena occupying its whole header+payload (8 bytes).
        unsafe {
            image
                .0
                .as_mut_ptr()
                .add(INDEX_TUPLE_DATA_SIZE)
                .cast::<u32>()
                .write_unaligned(set_varsize_4b_word(8));
        }
        let atts = vec![att(-1, false, 4)];
        assert!(unsafe { index_tuple_verify(image.0.as_ptr(), size, &atts) });
    }

    // A line pointer too short to hold the fixed header is rejected before any
    // header field is read (guards lp_len in 0..8).
    #[test]
    fn short_line_pointer_is_rejected() {
        let image = Image([0u8; 64]);
        let atts = vec![att(4, true, 4)];
        assert!(!unsafe { index_tuple_verify(image.0.as_ptr(), 4, &atts) });
    }

    // A t_info size larger than the line-pointer extent is rejected.
    #[test]
    fn oversized_claimed_size_is_rejected() {
        let mut image = Image([0u8; 64]);
        set_info(&mut image, 32);
        let atts = vec![att(4, true, 4)];
        assert!(!unsafe { index_tuple_verify(image.0.as_ptr(), 16, &atts) });
    }

    // A fixed attribute wider than the tuple body is rejected rather than read
    // past the extent.
    #[test]
    fn truncated_fixed_attribute_is_rejected() {
        let mut image = Image([0u8; 64]);
        // size claims only 4 data bytes but the attribute is an 8-byte int8.
        let size = INDEX_TUPLE_DATA_SIZE + 4;
        set_info(&mut image, size as u16);
        let atts = vec![att(8, true, 8)];
        assert!(!unsafe { index_tuple_verify(image.0.as_ptr(), size, &atts) });
    }
}

#[cold]
#[inline(never)]
fn too_many_index_columns(natts: usize) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "number of index columns ({natts}) exceeds limit ({INDEX_MAX_KEYS})"
        ))
        .with_sqlstate(ERRCODE_TOO_MANY_COLUMNS),
    )
}

#[cfg(test)]
mod column_limit_tests {
    use super::*;

    #[test]
    fn index_tuple_column_limit_precedes_attribute_access() {
        let context = mcx::MemoryContext::new("index column limit");
        let mcx = context.mcx();
        let natts = INDEX_MAX_KEYS as usize + 1;
        let desc = TupleDescData {
            natts: natts as i32,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: 1,
            constr: None,
            compact_attrs: PgVec::new_in(mcx),
            attrs: PgVec::new_in(mcx),
        };
        let error = match index_form_tuple(mcx, &desc, &[], &[]) {
            Err(error) => error,
            Ok(_) => panic!("oversized index tuple accepted"),
        };
        assert_eq!(error.sqlstate(), ERRCODE_TOO_MANY_COLUMNS);
        assert_eq!(
            error.message(),
            format!("number of index columns ({natts}) exceeds limit ({INDEX_MAX_KEYS})")
        );
    }
}
