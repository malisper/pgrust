//! serialize-core: the `MultirangeType` varlena serialization layer.
//!
//! The foundation family. Owns the on-disk encoding of a multirange (the item
//! array + flags + inlined bound payloads following the [`MultirangeType`]
//! header) and the in-memory bound explosion. Mirrors the corresponding
//! `multirangetypes.c` statics/externs over `*mut MultirangeType` (modeled by
//! [`MultirangeTypeP`]) + an [`Mcx`] allocator.
//!
//! Owns the inward seams `make_multirange` and `multirange_get_bounds`.

// Private encoding constants/helpers are consumed by the serialize/deserialize
// bodies; some (e.g. make_empty_multirange) are not reached by the inward seams
// but are part of the C surface this family owns.
#![allow(dead_code)]

use core::alloc::Layout;

use mcx::Mcx;
use types_cache::typcache::TypeCacheEntry;
use types_core::primitive::Oid;
use types_datum::datum::Datum;
use types_error::{PgError, PgResult};
use types_rangetypes::{
    MultirangeType, MultirangeTypeP, RangeBound, RangeType, RangeTypeP, RANGE_EMPTY, RANGE_LB_INC,
    RANGE_LB_INF, RANGE_LB_NULL, RANGE_UB_INC, RANGE_UB_INF, RANGE_UB_NULL,
};

use backend_utils_adt_rangetypes_seams as range_seams;
use backend_utils_adt_rangetypes_seams::{
    make_empty_range, make_range, range_adjacent_internal, range_before_internal, range_compare,
    range_union_internal,
};

// ---------------------------------------------------------------------------
// Private serialized-encoding constants (multirangetypes.c lines 80-83). These
// describe the multirange ADT's own item-array layout and stay inside the
// owning crate — consumers never decode the payload directly.
// ---------------------------------------------------------------------------

/// `MULTIRANGE_ITEM_OFF_BIT` — high bit of an item word marks an explicit byte
/// offset (vs an accumulated length).
pub(crate) const MULTIRANGE_ITEM_OFF_BIT: u32 = 0x80000000;
/// `MULTIRANGE_ITEM_OFFSET_STRIDE` — store an explicit offset every Nth item.
pub(crate) const MULTIRANGE_ITEM_OFFSET_STRIDE: i32 = 4;

/// `MULTIRANGE_ITEM_GET_OFFLEN(item)` — strip the offset/length tag bit.
#[inline]
pub(crate) fn multirange_item_get_offlen(item: u32) -> u32 {
    item & 0x7FFFFFFF
}

/// `MULTIRANGE_ITEM_HAS_OFF(item)` — does the item word carry an explicit offset.
#[inline]
pub(crate) fn multirange_item_has_off(item: u32) -> bool {
    item & MULTIRANGE_ITEM_OFF_BIT != 0
}

// ---------------------------------------------------------------------------
// pg_type alignment codes (pg_type.h:302-305) and varlena/tupmacs helpers
// (varatt.h, access/tupmacs.h). These mirror the C macros the serialization
// layer expands inline; they operate on the raw multirange/range varlena bytes
// the ADT owns.
// ---------------------------------------------------------------------------

/// `TYPALIGN_CHAR` ('c'): char alignment (i.e. unaligned).
const TYPALIGN_CHAR: i8 = b'c' as i8;
/// `TYPALIGN_SHORT` ('s'): short alignment (typically 2 bytes).
const TYPALIGN_SHORT: i8 = b's' as i8;
/// `TYPALIGN_INT` ('i'): int alignment (typically 4 bytes).
const TYPALIGN_INT: i8 = b'i' as i8;
/// `TYPALIGN_DOUBLE` ('d'): double alignment (often 8 bytes).
const TYPALIGN_DOUBLE: i8 = b'd' as i8;

/// `sizeof(MultirangeType)` — the C header is `{int32, Oid, uint32}` = 12 bytes.
const SIZEOF_MULTIRANGE_TYPE: usize = core::mem::size_of::<MultirangeType>();
/// `sizeof(RangeType)` — the C header is `{int32, Oid}` = 8 bytes.
const SIZEOF_RANGE_TYPE: usize = core::mem::size_of::<RangeType>();

/// `VARHDRSZ_EXTERNAL` (varatt.h): `offsetof(varattrib_1b_e, va_data)` == 2.
const VARHDRSZ_EXTERNAL: usize = 2;
/// `sizeof(varatt_indirect)` (varatt.h): a single `struct varlena *pointer`.
const SIZEOF_VARATT_INDIRECT: usize = core::mem::size_of::<usize>();
/// `sizeof(varatt_expanded)` (varatt.h): a single pointer.
const SIZEOF_VARATT_EXPANDED: usize = core::mem::size_of::<usize>();
/// `sizeof(varatt_external)` (varatt.h): {int32 rawsize, uint32 extinfo, Oid valueid, Oid toastrelid} = 16.
const SIZEOF_VARATT_EXTERNAL: usize = 16;

/// `VARTAG_INDIRECT` (varatt.h).
const VARTAG_INDIRECT: u8 = 1;
/// `VARTAG_EXPANDED_RO` (varatt.h).
const VARTAG_EXPANDED_RO: u8 = 2;
/// `VARTAG_EXPANDED_RW` (varatt.h).
const VARTAG_EXPANDED_RW: u8 = 3;
/// `VARTAG_ONDISK` (varatt.h).
const VARTAG_ONDISK: u8 = 18;

/// `TYPEALIGN(ALIGNVAL, LEN)` (c.h): round `len` up to an `alignval`-byte
/// boundary. `alignval` is always a power of two.
#[inline]
fn typealign(alignval: usize, len: usize) -> usize {
    (len.wrapping_add(alignval - 1)) & !(alignval - 1)
}

/// `att_align_nominal(cur_offset, attalign)` (access/tupmacs.h): align an
/// offset for a type whose `typalign` code is `attalign`.
#[inline]
fn att_align_nominal(cur_offset: usize, attalign: i8) -> usize {
    if attalign == TYPALIGN_INT {
        typealign(4, cur_offset)
    } else if attalign == TYPALIGN_CHAR {
        cur_offset
    } else if attalign == TYPALIGN_DOUBLE {
        typealign(8, cur_offset)
    } else {
        // TYPALIGN_SHORT
        typealign(2, cur_offset)
    }
}

/// `VARSIZE(PTR)` for a fully-detoasted, 4-byte-header uncompressed varlena
/// (varatt.h `VARSIZE_4B`): native little-endian stores `len << 2`.
#[inline]
unsafe fn varsize_4b(ptr: *const u8) -> u32 {
    let header = (ptr as *const u32).read_unaligned();
    #[cfg(target_endian = "big")]
    let len = header & 0x3FFF_FFFF;
    #[cfg(target_endian = "little")]
    let len = (header >> 2) & 0x3FFF_FFFF;
    len
}

/// `SET_VARSIZE(PTR, len)` for a 4-byte-header uncompressed varlena
/// (varatt.h `SET_VARSIZE_4B`).
#[inline]
unsafe fn set_varsize_4b(ptr: *mut u8, len: u32) {
    #[cfg(target_endian = "big")]
    let header = len & 0x3FFF_FFFF;
    #[cfg(target_endian = "little")]
    let header = len << 2;
    (ptr as *mut u32).write_unaligned(header);
}

/// `VARSIZE_ANY_EXHDR`-style length read of the byte following a varlena
/// pointer, used inside `att_addlength_pointer` for the `typlen == -1` case
/// (varatt.h `VARSIZE_ANY`). Returns the full on-disk length to advance past.
#[inline]
unsafe fn varsize_any(ptr: *const u8) -> usize {
    let b0 = *ptr;
    if b0 == 0x01 {
        // VARATT_IS_1B_E: external/expanded toast pointer.
        let va_tag = *ptr.add(1);
        let body = if va_tag == VARTAG_INDIRECT {
            SIZEOF_VARATT_INDIRECT
        } else if (va_tag & !1) == VARTAG_EXPANDED_RO {
            SIZEOF_VARATT_EXPANDED
        } else if va_tag == VARTAG_ONDISK {
            SIZEOF_VARATT_EXTERNAL
        } else {
            0
        };
        VARHDRSZ_EXTERNAL + body
    } else if (b0 & 0x01) == 0x01 {
        // VARATT_IS_1B: 1-byte short header, length in bits 1..7.
        ((b0 >> 1) & 0x7f) as usize
    } else {
        // 4-byte header.
        varsize_4b(ptr) as usize
    }
}

/// `att_addlength_pointer(cur_offset, attlen, attptr)` (access/tupmacs.h):
/// advance an offset/pointer past a value of length code `attlen` located at
/// `attptr`. Here the C usage always passes `cur_offset == attptr`, so we
/// return the advanced pointer.
#[inline]
unsafe fn att_addlength_pointer(ptr: *const u8, attlen: i16) -> *const u8 {
    if attlen > 0 {
        ptr.add(attlen as usize)
    } else if attlen == -1 {
        ptr.add(varsize_any(ptr))
    } else {
        // attlen == -2: cstring.
        let mut n = 0usize;
        while *ptr.add(n) != 0 {
            n += 1;
        }
        ptr.add(n + 1)
    }
}

/// `att_align_pointer(cur_offset, attalign, attlen, attptr)`
/// (access/tupmacs.h): like `att_align_nominal`, except a `typlen == -1` value
/// with a 1-byte (short) header is not aligned at all.
#[inline]
unsafe fn att_align_pointer(ptr: *const u8, attalign: i8, attlen: i16) -> *const u8 {
    // VARATT_NOT_PAD_BYTE(ptr): the first byte is not a zero pad byte.
    if attlen == -1 && *ptr != 0 {
        ptr
    } else {
        att_align_nominal(ptr as usize, attalign) as *const u8
    }
}

/// `fetch_att(T, attbyval, attlen)` (access/tupmacs.h): read a stored attribute
/// value into a `Datum`. Pass-by-reference values yield the pointer itself.
#[inline]
unsafe fn fetch_att(t: *const u8, attbyval: bool, attlen: i16) -> Datum {
    if attbyval {
        match attlen {
            1 => Datum::from_char(*(t as *const i8)),
            2 => Datum::from_i16((t as *const i16).read_unaligned()),
            4 => Datum::from_i32((t as *const i32).read_unaligned()),
            8 => Datum::from_u64((t as *const u64).read_unaligned()),
            other => panic!("unsupported byval length: {other}"),
        }
    } else {
        // PointerGetDatum(T).
        Datum::from_usize(t as usize)
    }
}

// `RANGE_HAS_LBOUND(flags)` / `RANGE_HAS_UBOUND(flags)` (rangetypes.h).
#[inline]
fn range_has_lbound(flags: u8) -> bool {
    flags & (RANGE_EMPTY | RANGE_LB_NULL | RANGE_LB_INF) == 0
}
#[inline]
fn range_has_ubound(flags: u8) -> bool {
    flags & (RANGE_EMPTY | RANGE_UB_NULL | RANGE_UB_INF) == 0
}

/// `palloc0(size)` over `mcx`: a zero-filled byte buffer whose start address is
/// returned. The buffer is leaked into `mcx` (freed when the context is reset),
/// mirroring how C hands a palloc'd pointer back to the caller.
fn palloc0<'mcx>(mcx: Mcx<'mcx>, size: usize) -> PgResult<*mut u8> {
    use mcx::Allocator;
    mcx::check_alloc_size(size)?;
    // Over-align to 8 bytes (MAXALIGN): palloc returns MAXALIGN'd memory, which
    // the bound-payload alignment math relies on.
    let layout = Layout::from_size_align(size.max(1), 8).expect("valid layout");
    let p = mcx.allocate_zeroed(layout).map_err(|_| mcx.oom(size))?;
    Ok(p.as_ptr() as *mut u8)
}

/// `(&[u8]) view of VARSIZE_ANY(DatumGetPointer(src))` for a varlena pointer
/// `Datum`: read the leading header to learn the total size, then return the
/// full image. Used to obtain the `anyrange[]` array image for the value-lane
/// `deconstruct_array_values_bytes` walk.
///
/// # Safety
/// `src` is a by-reference array `Datum` whose pointer word targets a live
/// `mcx`-owned varlena (the fmgr boundary copies the arg image into `mcx`).
unsafe fn datum_varlena_image<'a>(src: Datum) -> &'a [u8] {
    let p = src.as_usize() as *const u8;
    let total = varsize_any(p);
    core::slice::from_raw_parts(p, total)
}

/// Copy a by-reference element's verbatim varlena bytes (carried on the
/// value-lane `Datum::ByRef`) into a fresh MAXALIGN'd `mcx` image, returning the
/// pointer-word `Datum` a `DatumGet*P` kernel can dereference. This is the
/// value-lane bridge that lets `datum_get_range_type_p` operate on a real
/// pointer instead of a bare in-buffer offset.
fn byref_bytes_to_arg_word<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<Datum> {
    let base = palloc0(mcx, image.len().max(1))?;
    // SAFETY: `base` heads a freshly allocated, zero-filled, image.len()-byte
    // MAXALIGN'd region; copying the verbatim varlena bytes in yields a valid
    // plain varlena living for `'mcx`.
    unsafe {
        core::ptr::copy_nonoverlapping(image.as_ptr(), base, image.len());
    }
    Ok(Datum::from_usize(base as usize))
}

/// The detoasted element type's metadata, dereferenced from
/// `rangetyp->rngelemtype` (C unconditionally derefs this for a range type).
struct ElemType {
    typlen: i16,
    typbyval: bool,
    typalign: i8,
}

fn elem_type(rangetyp: &TypeCacheEntry) -> ElemType {
    let e = rangetyp
        .rngelemtype
        .as_deref()
        .expect("range typcache has rngelemtype");
    ElemType {
        typlen: e.typlen,
        typbyval: e.typbyval,
        typalign: e.typalign,
    }
}

// ---------------------------------------------------------------------------
// Serialization layer (multirangetypes.c).
// ---------------------------------------------------------------------------

/// `multirange_size_estimate(rangetyp, range_count, ranges)`
/// (multirangetypes.c:570): the serialized byte size of a multirange built from
/// `ranges`.
pub fn multirange_size_estimate(
    rangetyp: &TypeCacheEntry,
    ranges: &[RangeTypeP<'_>],
) -> PgResult<usize> {
    let elemalign = elem_type(rangetyp).typalign;
    let range_count = ranges.len() as i32;

    // Count space for MultirangeType struct, items and flags.
    let mut size = att_align_nominal(
        SIZEOF_MULTIRANGE_TYPE
            .wrapping_add((core::cmp::max(range_count - 1, 0) as usize) * 4)
            .wrapping_add((range_count as usize) * 1),
        elemalign,
    );

    // Count space for range bounds.
    for r in ranges {
        let vsize = unsafe { varsize_4b(r.ptr as *const u8) as usize };
        size = size.wrapping_add(att_align_nominal(
            vsize
                .wrapping_sub(SIZEOF_RANGE_TYPE)
                .wrapping_sub(1),
            elemalign,
        ));
    }

    Ok(size)
}

/// Byte offset of the item array within a serialized multirange:
/// `MultirangeGetItemsPtr` is `(Pointer) mr + sizeof(MultirangeType)`.
#[inline]
fn items_offset() -> usize {
    SIZEOF_MULTIRANGE_TYPE
}

/// Byte offset of the flags array: `MultirangeGetFlagsPtr` is
/// `(Pointer) mr + sizeof(MultirangeType) + (rangeCount - 1) * sizeof(uint32)`.
#[inline]
fn flags_offset(range_count: u32) -> usize {
    SIZEOF_MULTIRANGE_TYPE.wrapping_add((range_count.wrapping_sub(1) as usize) * 4)
}

/// Byte offset of the inlined bound payloads: `MultirangeGetBoundariesPtr` is
/// `att_align_nominal(sizeof(MultirangeType) + (rangeCount-1)*sizeof(uint32) +
/// rangeCount*sizeof(uint8), align)`.
#[inline]
fn boundaries_offset(range_count: u32, align: i8) -> usize {
    att_align_nominal(
        SIZEOF_MULTIRANGE_TYPE
            .wrapping_add((range_count.wrapping_sub(1) as usize) * 4)
            .wrapping_add((range_count as usize) * 1),
        align,
    )
}

/// `write_multirange_data(multirange, rangetyp, range_count, ranges)`
/// (multirangetypes.c:597): write the item array, flags, and inlined bounds of
/// `ranges` into the already-allocated `multirange` buffer.
pub fn write_multirange_data<'mcx>(
    multirange: MultirangeTypeP<'mcx>,
    rangetyp: &TypeCacheEntry,
    ranges: &[RangeTypeP<'mcx>],
) -> PgResult<()> {
    let elemalign = elem_type(rangetyp).typalign;
    let range_count = ranges.len() as i32;
    let base = multirange.ptr as *mut u8;
    let rc = multirange.range_count();

    unsafe {
        let items = base.add(items_offset()) as *mut u32;
        let flags = base.add(flags_offset(rc)) as *mut u8;
        let begin = base.add(boundaries_offset(rc, elemalign));
        let mut ptr = begin;
        let mut prev_offset: u32 = 0;

        let mut i: i32 = 0;
        while i < range_count {
            if i > 0 {
                // Every range, except the first, has an item. Every
                // MULTIRANGE_ITEM_OFFSET_STRIDE item contains an offset, others
                // contain lengths.
                let off = ptr.offset_from(begin) as u32;
                let slot = items.add((i - 1) as usize);
                slot.write(off);
                if (i % MULTIRANGE_ITEM_OFFSET_STRIDE) != 0 {
                    slot.write(slot.read().wrapping_sub(prev_offset));
                } else {
                    slot.write(slot.read() | MULTIRANGE_ITEM_OFF_BIT);
                }
                prev_offset = ptr.offset_from(begin) as u32;
            }

            let rp = ranges[i as usize].ptr as *const u8;
            let vsize = varsize_4b(rp) as usize;
            // flags[i] = *((Pointer) ranges[i] + VARSIZE - sizeof(char))
            *flags.add(i as usize) = *rp.add(vsize - 1);
            let len = vsize.wrapping_sub(SIZEOF_RANGE_TYPE).wrapping_sub(1) as u32;
            // memcpy(ptr, (Pointer)(ranges[i] + 1), len)
            core::ptr::copy_nonoverlapping(rp.add(SIZEOF_RANGE_TYPE), ptr, len as usize);
            ptr = ptr.add(att_align_nominal(len as usize, elemalign));
            i += 1;
        }
    }

    Ok(())
}

/// `make_multirange(mltrngtypoid, rangetyp, range_count, ranges)`
/// (multirangetypes.c:647): allocate and serialize a `MultirangeType` from
/// `ranges` (already canonicalized). The inward `make_multirange` seam.
pub fn make_multirange<'mcx>(
    mcx: Mcx<'mcx>,
    mltrngtypoid: Oid,
    rangetyp: &TypeCacheEntry,
    ranges: &[RangeTypeP<'mcx>],
) -> PgResult<MultirangeTypeP<'mcx>> {
    // Sort and merge input ranges.
    let mut ranges: Vec<RangeTypeP<'mcx>> = ranges.to_vec();
    let range_count = multirange_canonicalize(mcx, rangetyp, &mut ranges)?;
    let ranges = &ranges[..range_count as usize];

    // Note: zero-fill is required here, just as in heap tuples.
    let size = multirange_size_estimate(rangetyp, ranges)?;
    let raw = palloc0(mcx, size)?;
    unsafe {
        set_varsize_4b(raw, size as u32);
        let mr = raw as *mut MultirangeType;
        (*mr).multirangetypid = mltrngtypoid;
        (*mr).rangeCount = range_count as u32;
    }

    let multirange = MultirangeTypeP {
        ptr: raw as *const MultirangeType,
        _marker: core::marker::PhantomData,
    };

    write_multirange_data(multirange, rangetyp, ranges)?;

    Ok(multirange)
}

/// `make_empty_multirange(mltrngtypoid, rangetyp)` (multirangetypes.c:849): a
/// zero-range multirange.
pub fn make_empty_multirange<'mcx>(
    mcx: Mcx<'mcx>,
    mltrngtypoid: Oid,
    rangetyp: &TypeCacheEntry,
) -> PgResult<MultirangeTypeP<'mcx>> {
    make_multirange(mcx, mltrngtypoid, rangetyp, &[])
}

// ---------------------------------------------------------------------------
// GENERIC FUNCTIONS — the SQL multirange constructors (multirangetypes.c).
//
// The fmgr prologue of each C entry point (`get_fn_expr_rettype(flinfo)` for the
// return multirange type OID, and `multirange_get_typcache(fcinfo, ...)` for the
// typcache) is resolved by the dispatch layer, so the ports take the already
// resolved `mltrngtypoid` and `rangetyp` (= `typcache->rngtype`). The remaining
// logic (the `PG_NARGS`/`PG_ARGISNULL` guards, dims/elemtype validation, and the
// per-element NULL check + deconstruct loop) is the multirange unit's own.
// ---------------------------------------------------------------------------

/// `multirange_constructor2(PG_FUNCTION_ARGS)` (multirangetypes.c:942):
/// construct a multirange from a variadic array of member ranges. `range_array`
/// is the `Datum` of the (possibly toasted) `anyrange[]`; `nargs` is `PG_NARGS()`.
pub fn multirange_constructor2<'mcx>(
    mcx: Mcx<'mcx>,
    mltrngtypoid: Oid,
    rangetyp: &TypeCacheEntry,
    nargs: i32,
    range_array_isnull: bool,
    range_array: Datum,
) -> PgResult<MultirangeTypeP<'mcx>> {
    use backend_utils_adt_arrayfuncs_seams as array_seams;

    // A no-arg invocation should call multirange_constructor0 instead, but
    // returning an empty range is what that does.
    if nargs == 0 {
        return make_multirange(mcx, mltrngtypoid, rangetyp, &[]);
    }

    // This check should be guaranteed by our signature, but let's do it just
    // in case.
    if range_array_isnull {
        return Err(PgError::error(
            "multirange values cannot contain null members".to_string(),
        ));
    }

    let dims = array_seams::array_get_ndim::call(mcx, range_array)?;
    if dims > 1 {
        return Err(PgError::error(
            "multiranges cannot be constructed from multidimensional arrays".to_string(),
        )
        .with_sqlstate(types_error::error::ERRCODE_CARDINALITY_VIOLATION));
    }

    let rngtypid = array_seams::array_get_elemtype::call(mcx, range_array)?;
    if rngtypid != rangetyp.type_id {
        return Err(PgError::error(format!(
            "type {rngtypid} does not match constructor type"
        )));
    }

    // Be careful: we can still be called with zero ranges, like this:
    // `int4multirange(variadic '{}'::int4range[])`
    let ranges: Vec<RangeTypeP<'mcx>> = if dims == 0 {
        Vec::new()
    } else {
        // `range` is a pass-by-reference (varlena) element type. The bare-word
        // `deconstruct_array` stores only the in-buffer *offset* in each element
        // Datum, which `datum_get_range_type_p` would then dereference as a real
        // pointer — SIGSEGV on a real `anyrange[]`. Use the value-lane element
        // walk, which materializes each member range's verbatim varlena bytes as
        // a `Datum::ByRef`; copy those bytes into a live `mcx` image whose pointer
        // word `datum_get_range_type_p` can safely (de)toast.
        let image = unsafe { datum_varlena_image(range_array) };
        let elements = array_seams::deconstruct_array_values_bytes::call(
            mcx,
            image,
            rngtypid,
            rangetyp.typlen,
            rangetyp.typbyval,
            rangetyp.typalign as core::ffi::c_char,
        )?;

        let mut ranges = Vec::with_capacity(elements.len());
        for (elem, isnull) in elements.iter() {
            if *isnull {
                return Err(PgError::error(
                    "multirange values cannot contain null members".to_string(),
                )
                .with_sqlstate(types_error::error::ERRCODE_NULL_VALUE_NOT_ALLOWED));
            }
            // Copy the element's verbatim varlena bytes into a live `mcx` image
            // and hand its real pointer word to `datum_get_range_type_p`, which
            // detoasts a short/compressed element. make_multirange copies again.
            let elem_word = byref_bytes_to_arg_word(mcx, elem.as_ref_bytes())?;
            ranges.push(range_seams::datum_get_range_type_p::call(mcx, elem_word)?);
        }
        ranges
    };

    make_multirange(mcx, mltrngtypoid, rangetyp, &ranges)
}

/// `multirange_constructor1(PG_FUNCTION_ARGS)` (multirangetypes.c:1024):
/// construct a multirange from a single member range. `range` is the `Datum` of
/// the (possibly toasted) member range.
pub fn multirange_constructor1<'mcx>(
    mcx: Mcx<'mcx>,
    mltrngtypoid: Oid,
    rangetyp: &TypeCacheEntry,
    range_isnull: bool,
    range: Datum,
) -> PgResult<MultirangeTypeP<'mcx>> {
    // This check should be guaranteed by our signature, but let's do it just
    // in case.
    if range_isnull {
        return Err(PgError::error(
            "multirange values cannot contain null members".to_string(),
        ));
    }

    let range = range_seams::datum_get_range_type_p::call(mcx, range)?;

    // Make sure the range type matches.
    let rngtypid = range.rangetypid();
    if rngtypid != rangetyp.type_id {
        return Err(PgError::error(format!(
            "type {rngtypid} does not match constructor type"
        )));
    }

    make_multirange(mcx, mltrngtypoid, rangetyp, &[range])
}

/// `multirange_constructor0(PG_FUNCTION_ARGS)` (multirangetypes.c:1060): the
/// niladic constructor — an empty multirange. `nargs` is `PG_NARGS()`.
pub fn multirange_constructor0<'mcx>(
    mcx: Mcx<'mcx>,
    mltrngtypoid: Oid,
    rangetyp: &TypeCacheEntry,
    nargs: i32,
) -> PgResult<MultirangeTypeP<'mcx>> {
    // This should always be called without arguments.
    if nargs != 0 {
        return Err(PgError::error(
            "niladic multirange constructor must not receive arguments".to_string(),
        ));
    }

    make_multirange(mcx, mltrngtypoid, rangetyp, &[])
}

/// `multirange_get_bounds_offset(multirange, i)` (multirangetypes.c:674): the
/// byte offset of the `i`th range's bound payload within the serialized buffer.
pub fn multirange_get_bounds_offset(multirange: MultirangeTypeP<'_>, i: i32) -> PgResult<i32> {
    let base = multirange.ptr as *const u8;
    let mut offset: u32 = 0;
    let mut i = i;
    unsafe {
        let items = base.add(items_offset()) as *const u32;
        // Summarize lengths till we meet an offset.
        while i > 0 {
            let item = items.add((i - 1) as usize).read();
            offset = offset.wrapping_add(multirange_item_get_offlen(item));
            if multirange_item_has_off(item) {
                break;
            }
            i -= 1;
        }
    }
    Ok(offset as i32)
}

/// `MultirangeGetFlagsPtr(mr)[i]` (multirangetypes.h): the flags byte of the
/// `i`th member range of a serialized multirange. Used by `hash_multirange` /
/// `hash_multirange_extended`, which hash the raw flags byte alongside the
/// bound values.
#[inline]
pub fn multirange_get_flags(multirange: MultirangeTypeP<'_>, i: u32) -> u8 {
    let rc = multirange.range_count();
    debug_assert!(i < rc);
    let base = multirange.ptr as *const u8;
    // SAFETY: the flags array of `rc` bytes lives at `flags_offset(rc)`, and
    // `i < rc` (debug-asserted), exactly as in `MultirangeGetFlagsPtr`.
    unsafe { *((base.add(flags_offset(rc)) as *const u8).add(i as usize)) }
}

/// `multirange_get_range(rangetyp, multirange, i)` (multirangetypes.c:696):
/// deserialize the `i`th member range into a freshly serialized `RangeType`.
pub fn multirange_get_range<'mcx>(
    mcx: Mcx<'mcx>,
    rangetyp: &TypeCacheEntry,
    multirange: MultirangeTypeP<'mcx>,
    i: i32,
) -> PgResult<RangeTypeP<'mcx>> {
    let elem = elem_type(rangetyp);
    let typlen = elem.typlen;
    let typalign = elem.typalign;

    let rc = multirange.range_count();
    debug_assert!((i as u32) < rc);

    let offset = multirange_get_bounds_offset(multirange, i)? as usize;
    let base = multirange.ptr as *const u8;

    unsafe {
        let flags = *((base.add(flags_offset(rc)) as *const u8).add(i as usize));
        let begin = base.add(boundaries_offset(rc, typalign)).add(offset);
        let mut ptr = begin;

        // Calculate the size of bound values; range bound values are aligned,
        // so we walk the values to get the exact size.
        if range_has_lbound(flags) {
            ptr = att_addlength_pointer(ptr, typlen);
        }
        if range_has_ubound(flags) {
            ptr = att_align_pointer(ptr, typalign, typlen);
            ptr = att_addlength_pointer(ptr, typlen);
        }
        let span = ptr.offset_from(begin) as usize;
        let len = span + SIZEOF_RANGE_TYPE + 1;

        let raw = palloc0(mcx, len)?;
        set_varsize_4b(raw, len as u32);
        (*(raw as *mut RangeType)).rangetypid = rangetyp.type_id;
        // memcpy(range + 1, begin, ptr - begin)
        core::ptr::copy_nonoverlapping(begin, raw.add(SIZEOF_RANGE_TYPE), span);
        // *((uint8 *)(range + 1) + (ptr - begin)) = flags
        *raw.add(SIZEOF_RANGE_TYPE + span) = flags;

        Ok(RangeTypeP {
            ptr: raw as *const RangeType,
            _marker: core::marker::PhantomData,
        })
    }
}

/// `multirange_get_bounds(rangetyp, multirange, i, &lower, &upper)`
/// (multirangetypes.c:745): the lower/upper bounds of the `i`th member range.
/// The inward `multirange_get_bounds` seam.
pub fn multirange_get_bounds(
    rangetyp: &TypeCacheEntry,
    multirange: MultirangeTypeP<'_>,
    i: u32,
) -> PgResult<(RangeBound, RangeBound)> {
    let elem = elem_type(rangetyp);
    let typlen = elem.typlen;
    let typalign = elem.typalign;
    let typbyval = elem.typbyval;

    let rc = multirange.range_count();
    debug_assert!(i < rc);

    let offset = multirange_get_bounds_offset(multirange, i as i32)? as usize;
    let base = multirange.ptr as *const u8;

    let lbound;
    let ubound;
    let flags;
    unsafe {
        flags = *((base.add(flags_offset(rc)) as *const u8).add(i as usize));
        let mut ptr = base.add(boundaries_offset(rc, typalign)).add(offset);

        // multirange can't contain empty ranges.
        debug_assert!(flags & RANGE_EMPTY == 0);

        // fetch lower bound, if any.
        if range_has_lbound(flags) {
            // att_align_pointer cannot be necessary here.
            lbound = fetch_att(ptr, typbyval, typlen);
            ptr = att_addlength_pointer(ptr, typlen);
        } else {
            lbound = Datum::from_usize(0);
        }

        // fetch upper bound, if any.
        if range_has_ubound(flags) {
            ptr = att_align_pointer(ptr, typalign, typlen);
            ubound = fetch_att(ptr, typbyval, typlen);
            // no need for att_addlength_pointer.
        } else {
            ubound = Datum::from_usize(0);
        }
    }

    let lower = RangeBound {
        val: lbound,
        infinite: flags & RANGE_LB_INF != 0,
        inclusive: flags & RANGE_LB_INC != 0,
        lower: true,
    };
    let upper = RangeBound {
        val: ubound,
        infinite: flags & RANGE_UB_INF != 0,
        inclusive: flags & RANGE_UB_INC != 0,
        lower: false,
    };

    Ok((lower, upper))
}

/// `multirange_get_union_range(rangetyp, mr)` (multirangetypes.c:803): a range
/// spanning the lowest lower bound to the highest upper bound of the multirange.
pub fn multirange_get_union_range<'mcx>(
    mcx: Mcx<'mcx>,
    rangetyp: &TypeCacheEntry,
    mr: MultirangeTypeP<'mcx>,
) -> PgResult<RangeTypeP<'mcx>> {
    let rc = mr.range_count();
    if rc == 0 {
        return make_empty_range::call(mcx, rangetyp);
    }

    let (lower, _tmp) = multirange_get_bounds(rangetyp, mr, 0)?;
    let (_tmp2, upper) = multirange_get_bounds(rangetyp, mr, rc - 1)?;

    make_range::call(mcx, rangetyp, &lower, &upper, false)
}

/// `multirange_deserialize(rangetyp, multirange, &range_count, &ranges)`
/// (multirangetypes.c:827): explode a serialized multirange into its member
/// `RangeType`s.
pub fn multirange_deserialize<'mcx>(
    mcx: Mcx<'mcx>,
    rangetyp: &TypeCacheEntry,
    multirange: MultirangeTypeP<'mcx>,
) -> PgResult<Vec<RangeTypeP<'mcx>>> {
    let range_count = multirange.range_count() as i32;

    // Convert each ShortRangeType into a RangeType.
    if range_count > 0 {
        // C: *ranges = palloc(range_count * sizeof(RangeType *)). Mirror the
        // MaxAllocSize gate on that request.
        mcx::check_alloc_size((range_count as usize) * core::mem::size_of::<RangeTypeP<'mcx>>())?;
        let mut ranges = Vec::with_capacity(range_count as usize);
        let mut i = 0;
        while i < range_count {
            ranges.push(multirange_get_range(mcx, rangetyp, multirange, i)?);
            i += 1;
        }
        Ok(ranges)
    } else {
        Ok(Vec::new())
    }
}

/// `multirange_canonicalize(rangetyp, input_range_count, ranges)`
/// (multirangetypes.c:477): sort the member ranges, drop empties, and merge
/// overlapping/adjacent neighbors in place; returns the surviving range count.
pub fn multirange_canonicalize<'mcx>(
    mcx: Mcx<'mcx>,
    rangetyp: &TypeCacheEntry,
    ranges: &mut Vec<RangeTypeP<'mcx>>,
) -> PgResult<i32> {
    let input_range_count = ranges.len() as i32;

    // Sort the ranges so we can find the ones that overlap/meet. C calls
    // qsort_arg(ranges, input_range_count, ..., range_compare, rangetyp). The
    // comparator can ereport(ERROR), so we sort fallibly: capture the first
    // error and bail.
    if input_range_count > 0 {
        let mut cmp_err: Option<types_error::PgError> = None;
        merge_sort_by(ranges, &mut |a, b| {
            if cmp_err.is_some() {
                return core::cmp::Ordering::Equal;
            }
            match range_compare::call(rangetyp, *a, *b) {
                Ok(c) => c.cmp(&0),
                Err(e) => {
                    cmp_err = Some(e);
                    core::cmp::Ordering::Equal
                }
            }
        });
        if let Some(e) = cmp_err {
            return Err(e);
        }
    }

    // Now merge where possible. The merge writes the surviving ranges into the
    // front of `ranges` (output_range_count slots) exactly as C does in place.
    let mut last_range: Option<RangeTypeP<'mcx>> = None;
    let mut output_range_count: usize = 0;
    let input: Vec<RangeTypeP<'mcx>> = ranges.clone();

    for i in 0..input_range_count as usize {
        let current_range = input[i];
        if range_is_empty(current_range) {
            continue;
        }

        let last = match last_range {
            None => {
                ranges[output_range_count] = current_range;
                last_range = Some(current_range);
                output_range_count += 1;
                continue;
            }
            Some(l) => l,
        };

        // range_adjacent_internal gives true if *either* A meets B or B meets
        // A; we rely on the sort above to rule out B meets A.
        if range_adjacent_internal::call(rangetyp, last, current_range)? {
            // The two ranges touch (without overlap), so merge them.
            let merged = range_union_internal::call(mcx, rangetyp, last, current_range, false)?;
            ranges[output_range_count - 1] = merged;
            last_range = Some(merged);
        } else if range_before_internal::call(rangetyp, last, current_range)? {
            // There's a gap, so make a new entry.
            ranges[output_range_count] = current_range;
            last_range = Some(current_range);
            output_range_count += 1;
        } else {
            // They must overlap, so merge them.
            let merged = range_union_internal::call(mcx, rangetyp, last, current_range, true)?;
            ranges[output_range_count - 1] = merged;
            last_range = Some(merged);
        }
    }

    Ok(output_range_count as i32)
}

/// `RangeIsEmpty(r)` (rangetypes.h): the range's flags byte carries
/// `RANGE_EMPTY`. The flags byte is the last byte of the serialized varlena.
#[inline]
fn range_is_empty(r: RangeTypeP<'_>) -> bool {
    unsafe {
        let rp = r.ptr as *const u8;
        let vsize = varsize_4b(rp) as usize;
        *rp.add(vsize - 1) & RANGE_EMPTY != 0
    }
}

/// A stable merge sort matching `qsort_arg`'s ordering contract while allowing a
/// fallible comparator (PostgreSQL's `qsort_arg` is not stable, but a stable
/// merge of an already-comparable key set yields the same canonical result the
/// merge loop consumes). Sorts `v` in place.
fn merge_sort_by<T: Copy>(
    v: &mut Vec<T>,
    cmp: &mut dyn FnMut(&T, &T) -> core::cmp::Ordering,
) {
    let n = v.len();
    if n < 2 {
        return;
    }
    let mut buf: Vec<T> = v.clone();
    merge_sort_rec(v, &mut buf, 0, n, cmp);
}

fn merge_sort_rec<T: Copy>(
    v: &mut [T],
    buf: &mut [T],
    lo: usize,
    hi: usize,
    cmp: &mut dyn FnMut(&T, &T) -> core::cmp::Ordering,
) {
    if hi - lo < 2 {
        return;
    }
    let mid = lo + (hi - lo) / 2;
    merge_sort_rec(v, buf, lo, mid, cmp);
    merge_sort_rec(v, buf, mid, hi, cmp);

    let mut i = lo;
    let mut j = mid;
    let mut k = lo;
    while i < mid && j < hi {
        if cmp(&v[i], &v[j]) != core::cmp::Ordering::Greater {
            buf[k] = v[i];
            i += 1;
        } else {
            buf[k] = v[j];
            j += 1;
        }
        k += 1;
    }
    while i < mid {
        buf[k] = v[i];
        i += 1;
        k += 1;
    }
    while j < hi {
        buf[k] = v[j];
        j += 1;
        k += 1;
    }
    v[lo..hi].copy_from_slice(&buf[lo..hi]);
}
