pub mod builtins;
pub mod io;
pub mod ops;
#[cfg(test)]
mod tests;

use std::rc::Rc;

use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::typcache::{TypeCacheEntry, TYPECACHE_RANGE_INFO};
use ::types_core::{InvalidOid, Oid};
use ::types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_DATA_EXCEPTION,
    ERRCODE_DATETIME_VALUE_OUT_OF_RANGE, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};
use ::types_fmgr::{function_call2_coll_in, FmgrInfo};

pub use arrayfuncs::foundation::{att_align_nominal, fetch_att};

pub const RANGE_EMPTY: u8 = 0x01;
pub const RANGE_LB_INC: u8 = 0x02;
pub const RANGE_UB_INC: u8 = 0x04;
pub const RANGE_LB_INF: u8 = 0x08;
pub const RANGE_UB_INF: u8 = 0x10;
pub const RANGE_LB_NULL: u8 = 0x20;
pub const RANGE_UB_NULL: u8 = 0x40;
pub const RANGE_CONTAIN_EMPTY: u8 = 0x80;

pub const RANGE_EMPTY_LITERAL: &str = "empty";

#[inline]
pub const fn range_has_lbound(flags: u8) -> bool {
    flags & (RANGE_EMPTY | RANGE_LB_NULL | RANGE_LB_INF) == 0
}

#[inline]
pub const fn range_has_ubound(flags: u8) -> bool {
    flags & (RANGE_EMPTY | RANGE_UB_NULL | RANGE_UB_INF) == 0
}

// sizeof(RangeType): varlena header + rangetypid (MAXALIGN'd on 64-bit).
pub const RANGE_HDRSZ: usize = 8;

const F_INT4RANGE_CANONICAL: Oid = 3914;
const F_DATERANGE_CANONICAL: Oid = 3915;
const F_INT8RANGE_CANONICAL: Oid = 3928;

const TYPSTORAGE_PLAIN: u8 = b'p';

#[derive(Clone, Copy, Debug)]
pub struct ElemInfo {
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,
    pub typstorage: u8,
}

#[derive(Clone, Copy, Debug)]
pub struct RangeBound {
    pub val: Datum,
    pub infinite: bool,
    pub inclusive: bool,
    pub lower: bool,
}

// Resolve-once carrier for one range type (C caches the TypeCacheEntry* in
// fn_extra; the finfo copies keep per-bound compares off the entry RefCells).
pub struct RangeInfo {
    // The typcache pin (C keeps the entry pointer alive); None only in tests.
    pub pin: Option<Rc<TypeCacheEntry>>,
    pub rngtypid: Oid,
    pub collation: Oid,
    pub elem_typid: Oid,
    pub elem: ElemInfo,
    pub cmp: FmgrInfo,
    pub canonical_oid: Oid,
    pub elem_hash: Option<FmgrInfo>,
    pub elem_hash_extended: Option<FmgrInfo>,
    // The range type's own pg_type storage props (constructor2 deconstructs
    // an array of ranges with these, not the element's).
    pub own_typlen: i16,
    pub own_typbyval: bool,
    pub own_typalign: u8,
}

#[track_caller]
#[cold]
fn not_a_range_type(rngtypid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("type {rngtypid} is not a range type")))
}

impl RangeInfo {
    pub fn lookup(rngtypid: Oid) -> PgResult<RangeInfo> {
        let e = ::typcache::lookup_type_cache(rngtypid, TYPECACHE_RANGE_INFO)?;
        Self::from_entry(e)
    }

    /// Snapshot a typcache range entry (RANGE_INFO already loaded).
    pub fn from_entry(e: Rc<TypeCacheEntry>) -> PgResult<RangeInfo> {
        let rngtypid = e.type_id;
        let Some(el) = e.rngelemtype() else {
            return Err(not_a_range_type(rngtypid));
        };
        let elem = ElemInfo {
            typlen: el.typlen(),
            typbyval: el.typbyval(),
            typalign: el.typalign() as u8,
            typstorage: el.typstorage() as u8,
        };
        let cmp = e.rng_cmp_proc_finfo().clone();
        let canonical_oid = e.rng_canonical_finfo().fn_oid;
        Ok(RangeInfo {
            rngtypid,
            collation: e.rng_collation(),
            elem_typid: el.type_id,
            elem,
            cmp,
            canonical_oid,
            elem_hash: None,
            elem_hash_extended: None,
            own_typlen: e.typlen(),
            own_typbyval: e.typbyval(),
            own_typalign: e.typalign() as u8,
            pin: Some(e),
        })
    }
}

// range_get_typcache (rangetypes.c): fn_extra memo keyed by rngtypid.
pub fn cached_range_info<'f>(
    flinfo: &'f mut FmgrInfo,
    rngtypid: Oid,
) -> PgResult<&'f mut RangeInfo> {
    let need = match flinfo.fn_extra_ref::<RangeInfo>() {
        Some(ri) => ri.rngtypid != rngtypid,
        None => true,
    };
    if need {
        flinfo.set_fn_extra(RangeInfo::lookup(rngtypid)?);
    }
    Ok(flinfo.fn_extra_mut::<RangeInfo>().unwrap())
}

#[inline]
pub fn range_type_oid(range: &[u8]) -> Oid {
    Oid::from_ne_bytes(range[4..8].try_into().unwrap())
}

#[inline]
pub fn range_get_flags(range: &[u8]) -> u8 {
    range[range.len() - 1]
}

#[inline]
pub fn range_is_empty(range: &[u8]) -> bool {
    range_get_flags(range) & RANGE_EMPTY != 0
}

#[cold]
pub fn range_types_do_not_match() -> Box<PgError> {
    Box::new(PgError::error("range types do not match"))
}

/// range_deserialize (rangetypes.c). By-ref bound datums point into `range`;
/// they are only valid while the image is.
///
/// The by-value tuple lowers to an sret block the caller reloads with 16B
/// q-register loads spanning the callee's narrower stores — a measured
/// store-to-load-forwarding failure (the two reload sites carried ~55% of
/// rgist_penalty's cycles). The shim inlines, so the out-param core writes
/// the caller's own slots and field reads match store widths (C's shape).
#[inline]
pub fn range_deserialize(elem: &ElemInfo, range: &[u8]) -> (RangeBound, RangeBound, bool) {
    let mut lower =
        RangeBound { val: Datum::from_usize(0), infinite: false, inclusive: false, lower: true };
    let mut upper =
        RangeBound { val: Datum::from_usize(0), infinite: false, inclusive: false, lower: false };
    let empty = range_deserialize_into(elem, range, &mut lower, &mut upper);
    (lower, upper, empty)
}

/// Caller-side slots for [`range_deserialize_into`]. Hot callers must call
/// the out-param core directly on these (NOT the tuple shim): reconstructing
/// the tuple lets LLVM fuse the flag bytes into misaligned word reloads of
/// the callee's byte stores (`ldur w8,[x29,#-71]` class — measured worse
/// than the sret q-reloads it replaced). Stable locals only ever read
/// field-wise keep every load width-matched with the core's stores.
#[inline(always)]
pub fn range_bound_slots() -> (RangeBound, RangeBound) {
    (
        RangeBound { val: Datum::from_usize(0), infinite: false, inclusive: false, lower: true },
        RangeBound { val: Datum::from_usize(0), infinite: false, inclusive: false, lower: false },
    )
}

/// Out-param core of [`range_deserialize`]; returns `empty`. `lower.lower` /
/// `upper.lower` keep the caller-initialized values (true/false as in C's
/// stack-local convention).
pub fn range_deserialize_into(
    elem: &ElemInfo,
    range: &[u8],
    lower: &mut RangeBound,
    upper: &mut RangeBound,
) -> bool {
    let flags = range_get_flags(range);
    let typlen = elem.typlen as i32;
    let base = range.as_ptr();
    let mut off = RANGE_HDRSZ;

    lower.val = if range_has_lbound(flags) {
        // SAFETY: `off` stays within the serialized image range_serialize built.
        let d = fetch_att(unsafe { base.add(off) }, elem.typbyval, typlen);
        off = arrayfuncs::foundation::att_addlength_pointer(off, typlen, unsafe { base.add(off) });
        d
    } else {
        Datum::from_usize(0)
    };
    lower.infinite = flags & RANGE_LB_INF != 0;
    lower.inclusive = flags & RANGE_LB_INC != 0;

    upper.val = if range_has_ubound(flags) {
        off = att_align_ptr(base, off, elem.typalign, elem.typlen);
        // SAFETY: as above.
        fetch_att(unsafe { base.add(off) }, elem.typbyval, typlen)
    } else {
        Datum::from_usize(0)
    };
    upper.infinite = flags & RANGE_UB_INF != 0;
    upper.inclusive = flags & RANGE_UB_INC != 0;

    flags & RANGE_EMPTY != 0
}

// att_align_pointer: no padding before an already-started short varlena.
#[inline]
fn att_align_ptr(base: *const u8, cur: usize, typalign: u8, typlen: i16) -> usize {
    // SAFETY: caller guarantees `cur` is in bounds of the live image.
    if typlen == -1 && unsafe { *base.add(cur) } != 0 {
        cur
    } else {
        att_align_nominal(cur, typalign)
    }
}

#[inline]
fn varatt_is_1b(p: *const u8) -> bool {
    // SAFETY: live varlena header.
    unsafe { *p & 0x01 == 0x01 }
}

#[inline]
fn varatt_is_1b_e(p: *const u8) -> bool {
    // SAFETY: live varlena header.
    unsafe { *p == 0x01 }
}

#[inline]
fn varatt_is_4b_u(p: *const u8) -> bool {
    // SAFETY: live varlena header.
    unsafe { *p & 0x03 == 0x00 }
}

#[inline]
pub fn varsize_4b(p: *const u8) -> usize {
    // SAFETY: live 4-byte varlena header.
    let w = unsafe { u32::from_ne_bytes(core::slice::from_raw_parts(p, 4).try_into().unwrap()) };
    (w >> 2) as usize
}

#[inline]
fn varsize_short(p: *const u8) -> usize {
    // SAFETY: live short varlena header.
    unsafe { (*p as usize >> 1) & 0x7F }
}

#[inline]
const fn type_is_packable(typlen: i16, typstorage: u8) -> bool {
    typlen == -1 && typstorage != TYPSTORAGE_PLAIN
}

#[inline]
fn varatt_can_make_short(p: *const u8) -> bool {
    varatt_is_4b_u(p) && varsize_4b(p) - 4 + 1 <= 0x7F
}

fn datum_compute_size(
    mut data_length: usize,
    val: Datum,
    typbyval: bool,
    typalign: u8,
    typlen: i16,
    typstorage: u8,
) -> usize {
    let p = val.as_usize() as *const u8;
    if type_is_packable(typlen, typstorage) && varatt_can_make_short(p) {
        data_length += varsize_4b(p) - 4 + 1;
    } else if typlen == -1 && !typbyval && varatt_is_1b(p) {
        // att_align_datum: an already-short varlena takes no padding.
        data_length += varsize_short(p);
    } else {
        data_length = att_align_nominal(data_length, typalign);
        data_length += if typbyval {
            typlen as usize
        } else if typlen == -1 {
            if varatt_is_1b(p) {
                varsize_short(p)
            } else {
                varsize_4b(p)
            }
        } else if typlen == -2 {
            // SAFETY: NUL-terminated cstring datum.
            unsafe {
                let mut n = 0usize;
                while *p.add(n) != 0 {
                    n += 1;
                }
                n + 1
            }
        } else {
            typlen as usize
        };
    }
    data_length
}

// datum_write (rangetypes.c); `out` was zero-extended to the computed size,
// so alignment padding stays zeroed as with C's palloc0.
fn datum_write(
    out: &mut [u8],
    mut off: usize,
    datum: Datum,
    typbyval: bool,
    typalign: u8,
    typlen: i16,
    typstorage: u8,
) -> usize {
    if typbyval {
        off = att_align_nominal(off, typalign);
        let n = typlen as usize;
        // C store_att_byval stores `n` low-order bytes of the full Datum
        // word; SIZEOF_DATUM is pinned to 8 on every target. as_usize() is
        // only 4 bytes on wasm32, so `bytes[..n]` panics for the 8-byte byval
        // range subtypes (int8/float8/timestamp) there.
        let bytes = datum.as_u64().to_ne_bytes();
        out[off..off + n].copy_from_slice(&bytes[..n]);
        off += n;
        return off;
    }
    let p = datum.as_usize() as *const u8;
    if typlen == -1 {
        if varatt_is_1b_e(p) {
            panic!("cannot store a toast pointer inside a range");
        } else if varatt_is_1b(p) {
            let n = varsize_short(p);
            // SAFETY: short varlena of n total bytes.
            out[off..off + n].copy_from_slice(unsafe { core::slice::from_raw_parts(p, n) });
            off += n;
        } else if type_is_packable(typlen, typstorage) && varatt_can_make_short(p) {
            let n = varsize_4b(p) - 4 + 1;
            out[off] = ((n as u8) << 1) | 0x01;
            // SAFETY: 4-byte-header varlena; payload follows the header.
            out[off + 1..off + n]
                .copy_from_slice(unsafe { core::slice::from_raw_parts(p.add(4), n - 1) });
            off += n;
        } else {
            off = att_align_nominal(off, typalign);
            let n = varsize_4b(p);
            // SAFETY: live varlena of n total bytes.
            out[off..off + n].copy_from_slice(unsafe { core::slice::from_raw_parts(p, n) });
            off += n;
        }
    } else if typlen == -2 {
        // SAFETY: NUL-terminated cstring datum.
        unsafe {
            let mut n = 0usize;
            while *p.add(n) != 0 {
                n += 1;
            }
            n += 1;
            out[off..off + n].copy_from_slice(core::slice::from_raw_parts(p, n));
            off += n;
        }
    } else {
        off = att_align_nominal(off, typalign);
        let n = typlen as usize;
        // SAFETY: fixed-length by-ref datum of n bytes.
        out[off..off + n].copy_from_slice(unsafe { core::slice::from_raw_parts(p, n) });
        off += n;
    }
    off
}

#[cold]
fn lower_gt_upper() -> PgError {
    PgError::error("range lower bound must be less than or equal to range upper bound")
        .with_sqlstate(ERRCODE_DATA_EXCEPTION)
}

// PG_DETOAST_DATUM_PACKED over a bound value (C rangetypes.c:1855-1874):
// "It is essential that we not insert an out-of-line toast value pointer
// into a range object, for the same reasons that arrays and records can't
// contain them" — the pointer would dangle once the source table's toast
// data is gone. An external bound is fetched inline and a compressed bound
// is decompressed; unlike arrays, a short-header varlena stays as-is (the
// PACKED discipline). The previous gate here let an external pointer (tag
// 0x01, which satisfies the 1B test) fall through to datum_write's panic
// and rejected compressed bounds outright.
fn detoast_bound_packed<'m>(mcx: Mcx<'m>, val: Datum) -> PgResult<Datum> {
    let p = val.as_usize() as *const u8;
    // SAFETY: live varlena header byte. Compressed = 4B tag ..10.
    let is_compressed = unsafe { *p & 0x03 == 0x02 };
    if varatt_is_1b_e(p) || is_compressed {
        // SAFETY: live varlena image; varsize_any reads only header bytes.
        let raw = unsafe {
            core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p))
        };
        let flat = ::detoast_seams::detoast_attr::call(mcx, raw)?;
        Ok(Datum::from_usize(flat.leak().as_ptr() as usize))
    } else {
        Ok(val)
    }
}

/// range_serialize (rangetypes.c): byte-exact image build. `Ok(None)` means a
/// soft error was captured in `esc`.
pub fn range_serialize<'m>(
    mcx: Mcx<'m>,
    ri: &mut RangeInfo,
    lower: &mut RangeBound,
    upper: &mut RangeBound,
    empty: bool,
    mut esc: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'m, u8>>> {
    debug_assert!(lower.lower && !upper.lower);
    let mut flags: u8 = 0;

    if empty {
        flags |= RANGE_EMPTY;
    } else {
        let cmp = range_cmp_bound_values(mcx, ri, lower, upper)?;
        if cmp > 0 {
            return ereturn(esc, None, lower_gt_upper());
        }
        if cmp == 0 && !(lower.inclusive && upper.inclusive) {
            flags |= RANGE_EMPTY;
        } else {
            if lower.infinite {
                flags |= RANGE_LB_INF;
            } else if lower.inclusive {
                flags |= RANGE_LB_INC;
            }
            if upper.infinite {
                flags |= RANGE_UB_INF;
            } else if upper.inclusive {
                flags |= RANGE_UB_INC;
            }
        }
    }

    let ElemInfo { typlen, typbyval, typalign, typstorage } = ri.elem;
    let mut msize = RANGE_HDRSZ;
    if range_has_lbound(flags) {
        if typlen == -1 {
            lower.val = detoast_bound_packed(mcx, lower.val)?;
        }
        msize = datum_compute_size(msize, lower.val, typbyval, typalign, typlen, typstorage);
    }
    if range_has_ubound(flags) {
        if typlen == -1 {
            upper.val = detoast_bound_packed(mcx, upper.val)?;
        }
        msize = datum_compute_size(msize, upper.val, typbyval, typalign, typlen, typstorage);
    }
    msize += 1;

    let mut img: PgVec<'m, u8> = ::mcx::vec_with_capacity_in(mcx, msize)?;
    img.resize(msize, 0);
    img[0..4].copy_from_slice(&::datum::set_varsize_4b(msize));
    img[4..8].copy_from_slice(&ri.rngtypid.to_ne_bytes());
    let mut off = RANGE_HDRSZ;
    if range_has_lbound(flags) {
        off = datum_write(&mut img, off, lower.val, typbyval, typalign, typlen, typstorage);
    }
    if range_has_ubound(flags) {
        off = datum_write(&mut img, off, upper.val, typbyval, typalign, typlen, typstorage);
    }
    img[msize - 1] = flags;
    debug_assert_eq!(off + 1, msize);
    let _ = &mut esc;
    Ok(Some(img))
}

/// make_range (rangetypes.c): serialize, then canonicalize non-empty results.
pub fn make_range<'m>(
    mcx: Mcx<'m>,
    ri: &mut RangeInfo,
    lower: &mut RangeBound,
    upper: &mut RangeBound,
    empty: bool,
    mut esc: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'m, u8>>> {
    let Some(img) = range_serialize(mcx, ri, lower, upper, empty, esc.as_deref_mut())? else {
        return Ok(None);
    };
    if ri.canonical_oid != InvalidOid && !range_is_empty(&img) {
        return canonicalize(mcx, ri, &img, esc);
    }
    Ok(Some(img))
}

// Built-in canonical functions dispatch natively; user-defined range types'
// canonicals go through fmgr (C make_range's FunctionCallInvoke arm).
fn canonicalize<'m>(
    mcx: Mcx<'m>,
    ri: &mut RangeInfo,
    img: &[u8],
    mut esc: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'m, u8>>> {
    let (mut lower, mut upper, empty) = range_deserialize(&ri.elem, img);
    debug_assert!(!empty);
    match ri.canonical_oid {
        F_INT4RANGE_CANONICAL => {
            if !canonical_adjust_i32(&mut lower, &mut upper, esc.as_deref_mut())? {
                return Ok(None);
            }
        }
        F_INT8RANGE_CANONICAL => {
            if !canonical_adjust_i64(&mut lower, &mut upper, esc.as_deref_mut())? {
                return Ok(None);
            }
        }
        F_DATERANGE_CANONICAL => {
            if !canonical_adjust_date(&mut lower, &mut upper, esc.as_deref_mut())? {
                return Ok(None);
            }
        }
        other => {
            let pin = ri
                .pin
                .as_ref()
                .unwrap_or_else(|| panic!("range canonical function {other}: no typcache pin"));
            let mut lfc = ::types_fmgr::LocalFcinfo::<1>::fresh(InvalidOid);
            // SAFETY: mcx outlives this call.
            unsafe { lfc.set_result_mcx(mcx) };
            let mut node = esc
                .as_deref()
                .map(|e| ::types_fmgr::ErrorSaveNode::new(e.details_wanted()));
            if let Some(n) = node.as_mut() {
                lfc.context = n.fm_node_ptr();
            }
            lfc.set_arg(0, Datum::from_usize(img.as_ptr() as usize));
            // Entry-owned finfo (C's rng_canonical_finfo) taken out for the
            // call: the callee's flinfo_ri re-reads this cell (from_entry),
            // so holding the RefMut across invoke double-borrows. The
            // placeholder's InvalidOid fn_oid only reaches the callee's
            // fn_extra memo, whose canonical fc consumers never read it.
            let mut finfo = core::mem::replace(
                &mut *pin.rng_canonical_finfo(),
                FmgrInfo::unresolved(),
            );
            let r = finfo.invoke(&mut lfc);
            *pin.rng_canonical_finfo() = finfo;
            let r = r?;
            if let Some(mut n) = node {
                if n.ctx.error_occurred() {
                    if let Some(e) = esc {
                        match n.ctx.take_error() {
                            Some(err) => e.save(err),
                            None => e.mark_error_occurred(),
                        }
                    }
                    return Ok(None);
                }
            }
            let p = r.as_usize() as *const u8;
            // SAFETY: the canonical fn returned a live flat range varlena.
            let total = unsafe { ::types_tuple::varatt::varsize_any(p) };
            let mut out: PgVec<'m, u8> = ::mcx::vec_with_capacity_in(mcx, total)?;
            // SAFETY: `total` readable bytes at p; capacity reserved above.
            unsafe {
                core::ptr::copy_nonoverlapping(p, out.as_mut_ptr(), total);
                out.set_len(total);
            }
            return Ok(Some(out));
        }
    }
    range_serialize(mcx, ri, &mut lower, &mut upper, false, esc)
}

#[cold]
fn int_out_of_range(what: &str) -> PgError {
    PgError::error(format!("{what} out of range")).with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

#[cold]
fn date_out_of_range() -> PgError {
    PgError::error("date out of range").with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
}

pub(crate) fn canonical_adjust_i32(
    lower: &mut RangeBound,
    upper: &mut RangeBound,
    mut esc: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    if !lower.infinite && !lower.inclusive {
        let bnd = lower.val.as_i32();
        if bnd == i32::MAX {
            return ereturn(esc, false, int_out_of_range("integer"));
        }
        lower.val = Datum::from_i32(bnd + 1);
        lower.inclusive = true;
    }
    if !upper.infinite && upper.inclusive {
        let bnd = upper.val.as_i32();
        if bnd == i32::MAX {
            return ereturn(esc.take(), false, int_out_of_range("integer"));
        }
        upper.val = Datum::from_i32(bnd + 1);
        upper.inclusive = false;
    }
    Ok(true)
}

pub(crate) fn canonical_adjust_i64(
    lower: &mut RangeBound,
    upper: &mut RangeBound,
    mut esc: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    if !lower.infinite && !lower.inclusive {
        let bnd = lower.val.as_i64();
        if bnd == i64::MAX {
            return ereturn(esc, false, int_out_of_range("bigint"));
        }
        lower.val = Datum::from_i64(bnd + 1);
        lower.inclusive = true;
    }
    if !upper.infinite && upper.inclusive {
        let bnd = upper.val.as_i64();
        if bnd == i64::MAX {
            return ereturn(esc.take(), false, int_out_of_range("bigint"));
        }
        upper.val = Datum::from_i64(bnd + 1);
        upper.inclusive = false;
    }
    Ok(true)
}

pub(crate) fn canonical_adjust_date(
    lower: &mut RangeBound,
    upper: &mut RangeBound,
    mut esc: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    if !lower.infinite && !::adt_date::DATE_NOT_FINITE(lower.val.as_i32()) && !lower.inclusive {
        let bnd = lower.val.as_i32() + 1;
        if !::adt_date::IS_VALID_DATE(bnd) {
            return ereturn(esc, false, date_out_of_range());
        }
        lower.val = Datum::from_i32(bnd);
        lower.inclusive = true;
    }
    if !upper.infinite && !::adt_date::DATE_NOT_FINITE(upper.val.as_i32()) && upper.inclusive {
        let bnd = upper.val.as_i32() + 1;
        if !::adt_date::IS_VALID_DATE(bnd) {
            return ereturn(esc.take(), false, date_out_of_range());
        }
        upper.val = Datum::from_i32(bnd);
        upper.inclusive = false;
    }
    Ok(true)
}

pub fn make_empty_range<'m>(mcx: Mcx<'m>, ri: &mut RangeInfo) -> PgResult<PgVec<'m, u8>> {
    let mut lower =
        RangeBound { val: Datum::from_usize(0), infinite: false, inclusive: false, lower: true };
    let mut upper =
        RangeBound { val: Datum::from_usize(0), infinite: false, inclusive: false, lower: false };
    Ok(make_range(mcx, ri, &mut lower, &mut upper, true, None)?
        .expect("empty range never soft-fails"))
}

#[inline]
pub fn cmp_elem_vals(mcx: Mcx<'_>, ri: &mut RangeInfo, a: Datum, b: Datum) -> PgResult<i32> {
    Ok(function_call2_coll_in(&mut ri.cmp, ri.collation, mcx, a, b)?.as_i32())
}

/// range_cmp_bounds (rangetypes.c).
///
/// PgResult<i32>'s 4B payload forbids the two-register ScalarPair return: the
/// outlined kernel returned through an sret slot whose dependent reload+cmp
/// was the hottest instruction cluster in the rgist lanes. The i64-payload
/// core keeps the return in x0/x1; the typed shim folds away on inlining.
#[inline]
pub fn range_cmp_bounds(
    mcx: Mcx<'_>,
    ri: &mut RangeInfo,
    b1: &RangeBound,
    b2: &RangeBound,
) -> PgResult<i32> {
    Ok(range_cmp_bounds_wide(mcx, ri, b1, b2)? as i32)
}

fn range_cmp_bounds_wide(
    mcx: Mcx<'_>,
    ri: &mut RangeInfo,
    b1: &RangeBound,
    b2: &RangeBound,
) -> PgResult<i64> {
    if b1.infinite && b2.infinite {
        return Ok(if b1.lower == b2.lower {
            0
        } else if b1.lower {
            -1
        } else {
            1
        });
    } else if b1.infinite {
        return Ok(if b1.lower { -1 } else { 1 });
    } else if b2.infinite {
        return Ok(if b2.lower { 1 } else { -1 });
    }

    let result = cmp_elem_vals(mcx, ri, b1.val, b2.val)?;
    if result == 0 {
        if !b1.inclusive && !b2.inclusive {
            return Ok(if b1.lower == b2.lower {
                0
            } else if b1.lower {
                1
            } else {
                -1
            });
        } else if !b1.inclusive {
            return Ok(if b1.lower { 1 } else { -1 });
        } else if !b2.inclusive {
            return Ok(if b2.lower { -1 } else { 1 });
        }
    }
    Ok(result as i64)
}

/// range_cmp_bound_values (rangetypes.c). Register-return core as
/// [`range_cmp_bounds`].
#[inline]
pub fn range_cmp_bound_values(
    mcx: Mcx<'_>,
    ri: &mut RangeInfo,
    b1: &RangeBound,
    b2: &RangeBound,
) -> PgResult<i32> {
    Ok(range_cmp_bound_values_wide(mcx, ri, b1, b2)? as i32)
}

fn range_cmp_bound_values_wide(
    mcx: Mcx<'_>,
    ri: &mut RangeInfo,
    b1: &RangeBound,
    b2: &RangeBound,
) -> PgResult<i64> {
    if b1.infinite && b2.infinite {
        Ok(if b1.lower == b2.lower {
            0
        } else if b1.lower {
            -1
        } else {
            1
        })
    } else if b1.infinite {
        Ok(if b1.lower { -1 } else { 1 })
    } else if b2.infinite {
        Ok(if b2.lower { 1 } else { -1 })
    } else {
        Ok(cmp_elem_vals(mcx, ri, b1.val, b2.val)? as i64)
    }
}
