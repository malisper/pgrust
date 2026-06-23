//! Family `range-repr-serialize`: the `RangeType` serialization engine over the
//! REAL `types-rangetypes` structs (`RangeType` / `RangeBound` / `RangeTypeP`)
//! and real `Datum`, NOT a byte blob.
//!
//! Mirrors `rangetypes.c`: `range_serialize` / `range_deserialize` /
//! `range_get_flags` / `range_set_contain_empty`, `make_range` /
//! `make_empty_range`, and the private `datum_compute_size` / `datum_write`
//! payload helpers. This family owns and (via `lib::init_seams`) installs the
//! inward `range_serialize` / `range_deserialize` / `DatumGetRangeTypeP` seams.
//!
//! The serialized `RangeType` is the range ADT's own private on-disk encoding
//! (the fixed `RangeType` header, the bound value(s), then the trailing flags
//! byte). C builds and reads it with raw pointer arithmetic over a palloc'd
//! image; we mirror that exactly: a context-allocated zero-filled byte image
//! whose head is a `RangeType`, written with the same `att_*` / varlena macros
//! C uses, handed back as the opaque `RangeTypeP` raw pointer.

use core::alloc::Layout;
use core::mem::size_of;

use allocator_api2::alloc::Allocator;
use detoast_seams as detoast;
use fmgr_seams as fmgr;
use ::mcx::Mcx;
use ::cache::typcache::TypeCacheEntry;
use ::types_core::primitive::OidIsValid;
use ::datum::datum::Datum;
use ::types_error::{ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_DATA_EXCEPTION};
use ::types_rangetypes::{
    RangeBound, RangeType, RangeTypeP, RANGE_EMPTY, RANGE_LB_INC, RANGE_LB_INF, RANGE_UB_INC,
    RANGE_UB_INF,
};

use crate::range_bounds_compare::range_cmp_bound_values;

// ---------------------------------------------------------------------------
// Type-property constants (catalog/pg_type.h) and alignment constants (c.h),
// verified field-by-field against the C headers for a standard 64-bit build.
// ---------------------------------------------------------------------------

/// `TYPALIGN_CHAR` (`pg_type.h`) -- `'c'`.
const TYPALIGN_CHAR: u8 = b'c';
/// `TYPALIGN_SHORT` -- `'s'`.
const TYPALIGN_SHORT: u8 = b's';
/// `TYPALIGN_INT` -- `'i'`.
const TYPALIGN_INT: u8 = b'i';
/// `TYPALIGN_DOUBLE` -- `'d'`.
const TYPALIGN_DOUBLE: u8 = b'd';

/// `TYPSTORAGE_PLAIN` (`pg_type.h`) -- `'p'`.
const TYPSTORAGE_PLAIN: u8 = b'p';

/// `ALIGNOF_SHORT` (`pg_config.h`).
const ALIGNOF_SHORT: usize = 2;
/// `ALIGNOF_INT`.
const ALIGNOF_INT: usize = 4;
/// `ALIGNOF_DOUBLE`.
const ALIGNOF_DOUBLE: usize = 8;

/// `VARHDRSZ` (`c.h`) -- `sizeof(int32)`.
const VARHDRSZ: usize = 4;
/// `VARHDRSZ_SHORT` (`varatt.h`) -- `offsetof(varattrib_1b, va_data)` == 1.
const VARHDRSZ_SHORT: usize = 1;
/// `VARATT_SHORT_MAX` (`varatt.h`).
const VARATT_SHORT_MAX: usize = 0x7F;

// ---------------------------------------------------------------------------
// Varlena header helpers (varatt.h), little-endian (the build target). These
// operate on the first byte(s) of a `struct varlena *` at `ptr`.
// ---------------------------------------------------------------------------

/// `((varattrib_1b *) ptr)->va_header` -- the physically first header byte.
#[inline]
unsafe fn va_header_1b(ptr: *const u8) -> u8 {
    *ptr
}

/// `VARATT_IS_4B_U(PTR)` (little-endian): `(va_header & 0x03) == 0x00`.
#[inline]
unsafe fn varatt_is_4b_u(ptr: *const u8) -> bool {
    (va_header_1b(ptr) & 0x03) == 0x00
}

/// `VARATT_IS_1B(PTR)` (little-endian): `(va_header & 0x01) == 0x01`.
#[inline]
unsafe fn varatt_is_1b(ptr: *const u8) -> bool {
    (va_header_1b(ptr) & 0x01) == 0x01
}

/// `VARATT_IS_1B_E(PTR)` (little-endian): `va_header == 0x01`.
#[inline]
unsafe fn varatt_is_1b_e(ptr: *const u8) -> bool {
    va_header_1b(ptr) == 0x01
}

/// `VARATT_IS_SHORT(PTR)` == `VARATT_IS_1B(PTR)`.
#[inline]
unsafe fn varatt_is_short(ptr: *const u8) -> bool {
    varatt_is_1b(ptr)
}

/// `VARATT_IS_EXTERNAL(PTR)` == `VARATT_IS_1B_E(PTR)`.
#[inline]
unsafe fn varatt_is_external(ptr: *const u8) -> bool {
    varatt_is_1b_e(ptr)
}

/// `VARATT_NOT_PAD_BYTE(PTR)` -- `*(uint8 *) PTR != 0`.
#[inline]
unsafe fn varatt_not_pad_byte(ptr: *const u8) -> bool {
    *ptr != 0
}

/// `VARSIZE_4B(PTR)` (little-endian): `(va_header >> 2) & 0x3FFFFFFF`. The
/// 4-byte length word is read with an unaligned load, matching C reading
/// `va_4byte.va_header` (which the macro doc warns is alignment-sensitive --
/// here the image head is always aligned so this is exact).
#[inline]
unsafe fn varsize_4b(ptr: *const u8) -> usize {
    let word = (ptr as *const u32).read_unaligned();
    ((word >> 2) & 0x3FFF_FFFF) as usize
}

/// `VARSIZE_1B(PTR)` (little-endian): `(va_header >> 1) & 0x7F`.
#[inline]
unsafe fn varsize_1b(ptr: *const u8) -> usize {
    ((va_header_1b(ptr) >> 1) & 0x7F) as usize
}

/// `VARSIZE(PTR)` == `VARSIZE_4B(PTR)`.
#[inline]
unsafe fn varsize(ptr: *const u8) -> usize {
    varsize_4b(ptr)
}

/// `VARSIZE_SHORT(PTR)` == `VARSIZE_1B(PTR)`.
#[inline]
unsafe fn varsize_short(ptr: *const u8) -> usize {
    varsize_1b(ptr)
}

/// `VARSIZE_ANY(PTR)` (varatt.h).
#[inline]
unsafe fn varsize_any(ptr: *const u8) -> usize {
    if varatt_is_1b_e(ptr) {
        // VARSIZE_EXTERNAL: not reachable for the detoasted bound values this
        // module writes (callers detoast first); mirror C by reading the
        // external header length, which equals VARHDRSZ_EXTERNAL + tag size.
        // The smallest faithful behaviour we need here is the 1B/4B paths; an
        // external pointer must never reach datum_write/compute_size.
        // VARHDRSZ_EXTERNAL (offsetof(varattrib_1b_e, va_data)) == 2 plus tag.
        // We never expect this branch; fall back to the 1B size read.
        varsize_1b(ptr)
    } else if varatt_is_1b(ptr) {
        varsize_1b(ptr)
    } else {
        varsize_4b(ptr)
    }
}

/// The canonical, header-FUL owned image of a by-reference range *element* value
/// at `ptr`, given its subtype `typlen` — the form the fmgr by-reference lane
/// (`RefPayload::Varlena`) and every adt value core expect.
///
/// A range serializes its bound values via [`datum_write`], which (for a
/// packable varlena subtype like `numeric`/`text`) may store the element with a
/// **1-byte short header** (`SET_VARSIZE_SHORT`). The owned fmgr boundary's
/// `RefPayload::Varlena` lane, however, carries an already-detoasted *header-ful*
/// (4-byte) varlena (the numeric/text value cores read `VARSIZE`/
/// `NUMERIC_HEADER_SIZE` off a 4-byte header). So a short-header bound image must
/// be un-packed to the 4-byte form here (mirroring `pg_detoast_datum_packed`):
/// `[SET_VARSIZE(VARHDRSZ + payload_len)] ++ payload`.
///
/// * `typlen == -1` (varlena): un-pack a short header to 4-byte form; a 4-byte
///   (un-compressed) image crosses verbatim. (A genuinely compressed/external
///   image never reaches here — `range_serialize` detoasts bounds first.)
/// * `typlen > 0` (fixed-length by-reference, e.g. `macaddr`/`uuid`): exactly
///   `typlen` raw bytes, copied verbatim.
///
/// # Safety
/// `ptr` must point at a live, fully-detoasted element image of the indicated
/// subtype that stays valid for the read.
pub unsafe fn byref_elem_headerful_image(ptr: *const u8, typlen: i16) -> std::vec::Vec<u8> {
    if typlen != -1 {
        debug_assert!(typlen > 0, "by-reference range element typlen must be -1 or > 0");
        return core::slice::from_raw_parts(ptr, typlen as usize).to_vec();
    }
    if varatt_is_short(ptr) {
        // Short (1-byte header) varlena: rebuild the 4-byte-header form. The
        // payload is `VARSIZE_SHORT - VARHDRSZ_SHORT` bytes just past the 1-byte
        // header; the canonical total length is `VARHDRSZ + payload_len`.
        let payload_len = varsize_short(ptr) - VARHDRSZ_SHORT;
        let total = VARHDRSZ + payload_len;
        let mut out = std::vec::Vec::with_capacity(total);
        out.resize(VARHDRSZ, 0);
        set_varsize(out.as_mut_ptr(), total);
        let payload = core::slice::from_raw_parts(ptr.add(VARHDRSZ_SHORT), payload_len);
        out.extend_from_slice(payload);
        out
    } else {
        // Plain 4-byte-header varlena: crosses verbatim.
        let len = varsize_4b(ptr);
        core::slice::from_raw_parts(ptr, len).to_vec()
    }
}

/// `VARATT_CONVERTED_SHORT_SIZE(PTR)` -- `VARSIZE(PTR) - VARHDRSZ + VARHDRSZ_SHORT`.
#[inline]
unsafe fn varatt_converted_short_size(ptr: *const u8) -> usize {
    varsize(ptr) - VARHDRSZ + VARHDRSZ_SHORT
}

/// `VARATT_CAN_MAKE_SHORT(PTR)` -- `VARATT_IS_4B_U(PTR) && converted <= MAX`.
#[inline]
unsafe fn varatt_can_make_short(ptr: *const u8) -> bool {
    varatt_is_4b_u(ptr) && varatt_converted_short_size(ptr) <= VARATT_SHORT_MAX
}

/// `VARDATA(PTR)` -- payload just past the 4-byte header.
#[inline]
unsafe fn vardata(ptr: *const u8) -> *const u8 {
    ptr.add(VARHDRSZ)
}

/// `SET_VARSIZE(PTR, len)` (little-endian): `va_header = (uint32) len << 2`.
#[inline]
unsafe fn set_varsize(ptr: *mut u8, len: usize) {
    (ptr as *mut u32).write_unaligned((len as u32) << 2);
}

/// `SET_VARSIZE_SHORT(PTR, len)` (little-endian): `va_header = (len << 1) | 0x01`.
#[inline]
unsafe fn set_varsize_short(ptr: *mut u8, len: usize) {
    *ptr = ((len as u8) << 1) | 0x01;
}

/// `TYPE_IS_PACKABLE(typlen, typstorage)` (rangetypes.c:2735) --
/// `typlen == -1 && typstorage != TYPSTORAGE_PLAIN`.
#[inline]
fn type_is_packable(typlen: i16, typstorage: u8) -> bool {
    typlen == -1 && typstorage != TYPSTORAGE_PLAIN
}

// ---------------------------------------------------------------------------
// Alignment helpers (c.h / tupmacs.h).
// ---------------------------------------------------------------------------

/// `TYPEALIGN(ALIGNVAL, LEN)`.
#[inline]
fn type_align(alignval: usize, len: usize) -> usize {
    (len + (alignval - 1)) & !(alignval - 1)
}

/// `att_align_nominal(cur_offset, attalign)` (tupmacs.h).
#[inline]
fn att_align_nominal(cur_offset: usize, attalign: u8) -> usize {
    match attalign {
        TYPALIGN_INT => type_align(ALIGNOF_INT, cur_offset),
        TYPALIGN_CHAR => cur_offset,
        TYPALIGN_DOUBLE => type_align(ALIGNOF_DOUBLE, cur_offset),
        TYPALIGN_SHORT => type_align(ALIGNOF_SHORT, cur_offset),
        _ => {
            // C: AssertMacro((attalign) == TYPALIGN_SHORT) then SHORTALIGN.
            type_align(ALIGNOF_SHORT, cur_offset)
        }
    }
}

/// `att_align_datum(cur_offset, attalign, attlen, attdatum)` (tupmacs.h). `val`
/// carries the bound `Datum`; only inspected (as a varlena pointer) for the
/// `attlen == -1 && VARATT_IS_SHORT` short-circuit.
#[inline]
unsafe fn att_align_datum(cur_offset: usize, attalign: u8, attlen: i16, val: Datum) -> usize {
    if attlen == -1 && varatt_is_short(val.as_usize() as *const u8) {
        cur_offset
    } else {
        att_align_nominal(cur_offset, attalign)
    }
}

/// `att_addlength_pointer(cur_offset, attlen, attptr)` (tupmacs.h).
#[inline]
unsafe fn att_addlength_pointer(cur_offset: usize, attlen: i16, attptr: *const u8) -> usize {
    if attlen > 0 {
        cur_offset + attlen as usize
    } else if attlen == -1 {
        cur_offset + varsize_any(attptr)
    } else {
        // C: AssertMacro((attlen) == -2); cstring length + 1.
        let mut n = 0usize;
        while *attptr.add(n) != 0 {
            n += 1;
        }
        cur_offset + n + 1
    }
}

/// `att_addlength_datum(cur_offset, attlen, attdatum)` (tupmacs.h) --
/// `att_addlength_pointer(.., DatumGetPointer(attdatum))`.
#[inline]
unsafe fn att_addlength_datum(cur_offset: usize, attlen: i16, val: Datum) -> usize {
    att_addlength_pointer(cur_offset, attlen, val.as_usize() as *const u8)
}

/// `att_align_pointer(cur_offset, attalign, attlen, attptr)` (tupmacs.h).
#[inline]
unsafe fn att_align_pointer(
    cur_offset: usize,
    attalign: u8,
    attlen: i16,
    attptr: *const u8,
) -> usize {
    if attlen == -1 && varatt_not_pad_byte(attptr) {
        cur_offset
    } else {
        att_align_nominal(cur_offset, attalign)
    }
}

/// `fetch_att(T, attbyval, attlen)` (tupmacs.h). Returns the bound value as a
/// `Datum`: a sign/zero-extended scalar for by-value, or the pointer itself
/// (`PointerGetDatum(T)`) for by-reference.
#[inline]
unsafe fn fetch_att(t: *const u8, attbyval: bool, attlen: i16) -> Datum {
    if attbyval {
        match attlen {
            1 => Datum::from_char(*(t as *const i8)),
            2 => Datum::from_i16((t as *const i16).read_unaligned()),
            4 => Datum::from_i32((t as *const i32).read_unaligned()),
            8 => Datum::from_i64((t as *const i64).read_unaligned()),
            other => panic!("unsupported byval length: {other}"),
        }
    } else {
        Datum::from_usize(t as usize)
    }
}

/// `store_att_byval(T, newdatum, attlen)` (tupmacs.h).
#[inline]
unsafe fn store_att_byval(t: *mut u8, newdatum: Datum, attlen: i16) {
    match attlen {
        1 => *(t as *mut i8) = newdatum.as_char(),
        2 => (t as *mut i16).write_unaligned(newdatum.as_i16()),
        4 => (t as *mut i32).write_unaligned(newdatum.as_i32()),
        8 => (t as *mut i64).write_unaligned(newdatum.as_i64()),
        other => panic!("unsupported byval length: {other}"),
    }
}

/// `RANGE_HAS_LBOUND(flags)` (rangetypes.h): not empty and lower not infinite.
#[inline]
fn range_has_lbound(flags: u8) -> bool {
    (flags & (RANGE_EMPTY | RANGE_LB_INF)) == 0
}

/// `RANGE_HAS_UBOUND(flags)` (rangetypes.h): not empty and upper not infinite.
#[inline]
fn range_has_ubound(flags: u8) -> bool {
    (flags & (RANGE_EMPTY | RANGE_UB_INF)) == 0
}

/// `RangeIsEmpty(r)` (rangetypes.h): `(range_get_flags(r) & RANGE_EMPTY) != 0`.
#[inline]
fn range_is_empty(range: RangeTypeP<'_>) -> bool {
    (range_get_flags(range) & RANGE_EMPTY) != 0
}

/// Seam `range_is_empty` — `RangeIsEmpty(DatumGetRangeTypeP(attval))`
/// (execIndexing.c's `ExecWithoutOverlapsNotEmpty`): detoast the by-reference
/// range value and report whether it is the empty range.
pub fn range_is_empty_seam<'mcx>(mcx: Mcx<'mcx>, attval: Datum) -> PgResult<bool> {
    let r = datum_get_range_type_p(mcx, attval)?;
    Ok(range_is_empty(r))
}

/// `palloc0(size)` returning MAXALIGN(8)-aligned, zero-filled context memory
/// (C: `palloc0` always returns MAXALIGN'd storage). The block lives for the
/// context's lifetime, like palloc'd memory freed at context reset.
fn palloc0_maxaligned<'mcx>(mcx: Mcx<'mcx>, size: usize) -> PgResult<*mut u8> {
    ::mcx::check_alloc_size(size)?;
    let layout = Layout::from_size_align(size, 8).expect("valid RangeType image layout");
    let block = mcx
        .allocate_zeroed(layout)
        .map_err(|_| mcx.oom(size))?;
    Ok(block.as_ptr() as *mut u8)
}


/// `range_serialize(typcache, lower, upper, empty, escontext)` (rangetypes.c:1791):
/// build a serialized `RangeType` from in-memory bounds, allocated in `mcx`.
///
/// `escontext` is `NULL` here (the hard-error path); a soft error never occurs,
/// so the result is always `Some`. Thin wrapper over [`range_serialize_soft`].
pub fn range_serialize<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    lower: &RangeBound,
    upper: &RangeBound,
    empty: bool,
) -> PgResult<RangeTypeP<'mcx>> {
    Ok(range_serialize_soft(mcx, typcache, lower, upper, empty, None)?
        .expect("range_serialize with NULL escontext never returns NULL"))
}

/// `range_serialize(typcache, lower, upper, empty, escontext)` (rangetypes.c:1791)
/// with the soft-error context threaded: build a serialized `RangeType` from
/// in-memory bounds, allocated in `mcx`.
///
/// Returns `Ok(None)` for the C `ereturn(escontext, NULL, …)` soft-error path
/// (the lower-bound-above-upper-bound check, rangetypes.c:1819) when `escontext`
/// is `Some`; without one that error propagates hard. The bound `Datum`s are
/// taken by value from copies (C mutates the caller's `RangeBound.val` when
/// detoasting; we detoast into `mcx` and rebind the local copies the same way).
pub fn range_serialize_soft<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    lower: &RangeBound,
    upper: &RangeBound,
    empty: bool,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<RangeTypeP<'mcx>>> {
    // Working copies of the bounds: C rewrites lower->val / upper->val in place
    // after detoasting; we mirror that on local copies.
    let mut lower = *lower;
    let mut upper = *upper;

    let mut flags: u8 = 0;

    // Verify range is not invalid on its face, and construct flags value,
    // preventing any non-canonical combinations such as infinite+inclusive.
    debug_assert!(lower.lower);
    debug_assert!(!upper.lower);

    if empty {
        flags |= RANGE_EMPTY;
    } else {
        let cmp = range_cmp_bound_values(typcache, &lower, &upper)?;

        // error check: if lower bound value is above upper, it's wrong
        if cmp > 0 {
            return ereturn(
                escontext,
                None,
                PgError::error(
                    "range lower bound must be less than or equal to range upper bound",
                )
                .with_sqlstate(ERRCODE_DATA_EXCEPTION),
            );
        }

        // if bounds are equal, and not both inclusive, range is empty
        if cmp == 0 && !(lower.inclusive && upper.inclusive) {
            flags |= RANGE_EMPTY;
        } else {
            // infinite boundaries are never inclusive
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

    // Fetch information about range's element type.
    let elem = typcache
        .rngelemtype
        .as_ref()
        .expect("range_serialize: typcache->rngelemtype must be set for a range type");
    let typlen = elem.typlen;
    let typbyval = elem.typbyval;
    let typalign = elem.typalign as u8;
    let typstorage = elem.typstorage as u8;

    // Count space for varlena header and range type's OID.
    let mut msize = size_of::<RangeType>();
    debug_assert_eq!(msize, type_align(8, msize)); // MAXALIGN(msize) == msize

    // Count space for bounds.
    if range_has_lbound(flags) {
        // Make sure item to be inserted is not toasted; allow short headers.
        if typlen == -1 {
            lower.val = detoast_packed(mcx, lower.val)?;
        }
        msize = datum_compute_size(msize, lower.val, typbyval, typalign, typlen, typstorage);
    }

    if range_has_ubound(flags) {
        if typlen == -1 {
            upper.val = detoast_packed(mcx, upper.val)?;
        }
        msize = datum_compute_size(msize, upper.val, typbyval, typalign, typlen, typstorage);
    }

    // Add space for flag byte.
    msize += size_of::<u8>();

    // Note: zero-fill is required here, just as in heap tuples (palloc0).
    // palloc returns MAXALIGN'd memory; the relative-offset accounting in
    // datum_compute_size only matches the absolute-address writes in
    // datum_write because the image base is MAXALIGN(8)-aligned. A `PgVec<u8>`
    // is only byte-aligned, so allocate a zeroed `MaxAligned` chunk instead.
    let base: *mut u8 = palloc0_maxaligned(mcx, msize)?;

    // SET_VARSIZE(range, msize) and fill in the rangetypid.
    let range = base as *mut RangeType;
    // SAFETY: `base` heads a freshly allocated, zero-filled, msize-byte image
    // whose layout begins with a RangeType header.
    unsafe {
        set_varsize(base, msize);
        (*range).rangetypid = typcache.type_id;

        // ptr = (char *) (range + 1)
        let mut ptr = base.add(size_of::<RangeType>());

        if range_has_lbound(flags) {
            debug_assert!(lower.lower);
            let next = datum_write(ptr as *mut RangeType, lower.val, typbyval, typalign, typlen, typstorage);
            ptr = next as *mut u8;
        }

        if range_has_ubound(flags) {
            debug_assert!(!upper.lower);
            let next = datum_write(ptr as *mut RangeType, upper.val, typbyval, typalign, typlen, typstorage);
            ptr = next as *mut u8;
        }

        // *((char *) ptr) = flags
        *ptr = flags;
    }

    Ok(Some(RangeTypeP {
        ptr: range as *const RangeType,
        _marker: core::marker::PhantomData,
    }))
}

/// Inward seam shape for `range_serialize` (thin pass-through to
/// [`range_serialize`]). Matches the `backend-utils-adt-rangetypes-seams`
/// signature; C `escontext` is `NULL` here (hard-error path).
pub fn range_serialize_seam<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    lower: &RangeBound,
    upper: &RangeBound,
    empty: bool,
) -> PgResult<RangeTypeP<'mcx>> {
    range_serialize(mcx, typcache, lower, upper, empty)
}

/// `range_deserialize(typcache, range, &lower, &upper, &empty)`
/// (rangetypes.c:1920): explode a serialized `RangeType` into its bounds.
///
/// NB: the given range object must be fully detoasted; it cannot have a short
/// varlena header. For a pass-by-reference element type the returned bound
/// `Datum`s are pointers into the given range object (exactly as in C).
pub fn range_deserialize(
    typcache: &TypeCacheEntry,
    range: RangeTypeP<'_>,
) -> PgResult<(RangeBound, RangeBound, bool)> {
    let base = range.ptr as *const u8;

    // SAFETY: `range` is a detoasted RangeType image; its trailing flags byte
    // and bound payload are read with the same offsets C uses.
    unsafe {
        debug_assert_eq!(range.rangetypid(), typcache.type_id);

        // fetch the flag byte from datum's last byte
        let flags = *base.add(varsize(base) - 1);

        let elem = typcache
            .rngelemtype
            .as_ref()
            .expect("range_deserialize: typcache->rngelemtype must be set for a range type");
        let typlen = elem.typlen;
        let typbyval = elem.typbyval;
        let typalign = elem.typalign as u8;

        // Initialize data offset just after the range OID. We track the bound
        // positions as offsets *relative to `base`*, not as absolute machine
        // addresses. In C this code aligns the absolute pointer, which is only
        // correct because a serialized RangeType always lives in MAXALIGN'd
        // (palloc'd) memory, so absolute-address alignment and base-relative
        // alignment coincide. Here the image may instead be borrowed from a
        // merely byte-aligned `Datum::ByRef` buffer (e.g. an SP-GiST leaf/prefix
        // image reconstructed into a `Vec<u8>`); aligning the *absolute* address
        // would then add bogus padding and read the upper bound from the wrong
        // offset. Base-relative alignment reproduces the C result exactly while
        // being independent of the image's actual allocation alignment.
        let mut off = size_of::<RangeType>();

        // fetch lower bound, if any
        let lbound = if range_has_lbound(flags) {
            // att_align_pointer cannot be necessary here
            let v = fetch_att(base.add(off), typbyval, typlen);
            off = att_addlength_pointer(off, typlen, base.add(off));
            v
        } else {
            Datum::null()
        };

        // fetch upper bound, if any
        let ubound = if range_has_ubound(flags) {
            off = att_align_pointer(off, typalign, typlen, base.add(off));
            fetch_att(base.add(off), typbyval, typlen)
            // no need for att_addlength_pointer
        } else {
            Datum::null()
        };

        let empty = (flags & RANGE_EMPTY) != 0;

        let lower = RangeBound {
            val: lbound,
            infinite: (flags & RANGE_LB_INF) != 0,
            inclusive: (flags & RANGE_LB_INC) != 0,
            lower: true,
        };
        let upper = RangeBound {
            val: ubound,
            infinite: (flags & RANGE_UB_INF) != 0,
            inclusive: (flags & RANGE_UB_INC) != 0,
            lower: false,
        };

        Ok((lower, upper, empty))
    }
}

/// Inward seam shape for `range_deserialize`.
pub fn range_deserialize_seam(
    typcache: &TypeCacheEntry,
    range: RangeTypeP<'_>,
) -> PgResult<(RangeBound, RangeBound, bool)> {
    range_deserialize(typcache, range)
}

/// `range_get_flags(range)` (rangetypes.c:1987): the trailing flags byte.
pub fn range_get_flags(range: RangeTypeP<'_>) -> u8 {
    let base = range.ptr as *const u8;
    // SAFETY: flag byte is the datum's last byte.
    unsafe { *base.add(varsize(base) - 1) }
}

/// `range_set_contain_empty(range)` (rangetypes.c:2001): set `RANGE_CONTAIN_EMPTY`.
pub fn range_set_contain_empty(range: RangeTypeP<'_>) {
    let base = range.ptr as *mut u8;
    // SAFETY: flag byte is the datum's last byte; the image is writable (the
    // GiST caller owns a mutable copy, as in C).
    unsafe {
        let flagsp = base.add(varsize(base as *const u8) - 1);
        *flagsp |= ::types_rangetypes::RANGE_CONTAIN_EMPTY;
    }
}

/// `make_range(typcache, lower, upper, empty, escontext)` (rangetypes.c:2016):
/// serialize and (if the type has a canonical fn) canonicalize.
///
/// `escontext` is `NULL` here (hard-error path), so a soft-error never occurs;
/// the result is always `Some`. Thin wrapper over [`make_range_soft`].
pub fn make_range<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    lower: &RangeBound,
    upper: &RangeBound,
    empty: bool,
) -> PgResult<RangeTypeP<'mcx>> {
    Ok(make_range_soft(mcx, typcache, lower, upper, empty, None)?
        .expect("make_range with NULL escontext never returns NULL"))
}

/// `make_range(typcache, lower, upper, empty, escontext)` (rangetypes.c:2016)
/// with the soft-error context threaded: serialize and (if the type has a
/// canonical fn) canonicalize.
///
/// Returns `Ok(None)` when `range_serialize` soft-fails (rangetypes.c:2021:
/// `if (range == NULL) return NULL;`).
pub fn make_range_soft<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
    lower: &RangeBound,
    upper: &RangeBound,
    empty: bool,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<RangeTypeP<'mcx>>> {
    // C: range = range_serialize(...); if (SOFT_ERROR_OCCURRED(escontext)) return NULL;
    let mut range = match range_serialize_soft(
        mcx,
        typcache,
        lower,
        upper,
        empty,
        escontext.as_deref_mut(),
    )? {
        Some(r) => r,
        None => return Ok(None),
    };

    // no need to call canonical on empty ranges ...
    if OidIsValid(typcache.rng_canonical_finfo.fn_oid) && !range_is_empty(range) {
        // C invokes typcache->rng_canonical_finfo via FunctionCallInvoke with
        // the range as its single argument, under the range's collation, then
        // re-detoasts the result. The canonical function is a range-in/range-out
        // builtin (`int4range_canonical` etc.): its by-reference RangeType
        // argument and result cross the owned fmgr boundary through the canonical
        // `Datum` lane (`RefPayload::Varlena`), NOT the bare-word `FunctionCall1`
        // path — the latter would drop the by-ref referent and hand back a null
        // word. Marshal the serialized range as a `ByRef` arg and read the
        // canonicalized range back from the `ByRef` result.
        //
        // C: `InitFunctionCallInfoData(*fcinfo, ..., escontext, NULL)` — the
        // canonical proc gets the soft-error sink, so an overflow (e.g.
        // `int4range_canonical` on `[1,INT_MAX]`) `ereturn`s "integer out of
        // range" into `escontext` instead of raising.
        use types_tuple::heaptuple::Datum as CanonDatum;
        let arg = CanonDatum::ByRef(::mcx::slice_in(mcx, &range_to_varlena_bytes(range))?);
        let (result, isnull) = match escontext.as_deref_mut() {
            Some(ctx) => match fmgr::function_call_invoke_datum_soft::call(
                mcx,
                typcache.rng_canonical_finfo.fn_oid,
                typcache.rng_collation,
                &[arg],
                &[],
                None,
                ctx,
            )? {
                Some(pair) => pair,
                // C: if (SOFT_ERROR_OCCURRED(escontext)) return NULL;
                None => return Ok(None),
            },
            None => fmgr::function_call_invoke_datum::call(
                mcx,
                typcache.rng_canonical_finfo.fn_oid,
                typcache.rng_collation,
                &[arg],
                &[],
                None,
            )?,
        };
        if isnull {
            // C: strict-null `function %u returned NULL` (canonical fns are strict
            // and never return NULL for a non-empty range).
            return Err(PgError::error(format!(
                "function {} returned NULL",
                typcache.rng_canonical_finfo.fn_oid
            )));
        }
        let CanonDatum::ByRef(bytes) = result else {
            // A range result is always a by-reference varlena; any other shape is
            // a contract violation.
            return Err(PgError::error(
                "range canonical function returned a non-by-reference result",
            ));
        };
        range = range_p_from_varlena_bytes(mcx, bytes.as_slice())?;
    }

    Ok(Some(range))
}

/// `make_empty_range(typcache)` (rangetypes.c:2229): the canonical empty range.
pub fn make_empty_range<'mcx>(
    mcx: Mcx<'mcx>,
    typcache: &TypeCacheEntry,
) -> PgResult<RangeTypeP<'mcx>> {
    let lower = RangeBound {
        val: Datum::null(),
        infinite: false,
        inclusive: false,
        lower: true,
    };
    let upper = RangeBound {
        val: Datum::null(),
        infinite: false,
        inclusive: false,
        lower: false,
    };

    make_range(mcx, typcache, &lower, &upper, true)
}

/// `DatumGetRangeTypeP(d)` (rangetypes.h): detoast a `Datum` into a `RangeType *`,
/// copying into `mcx` if detoasting is needed. Owns the inward seam.
///
/// `DatumGetRangeTypeP(X) = (RangeType *) PG_DETOAST_DATUM(X)`, i.e.
/// `pg_detoast_datum(DatumGetPointer(X))`: an extended (compressed or external)
/// varlena is fetched/decompressed via the detoast owner seam; an already-plain
/// 4B datum is returned as-is.
pub fn datum_get_range_type_p<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<RangeTypeP<'mcx>> {
    let p = d.as_usize() as *const u8;

    // SAFETY: `d` is a varlena pointer; the header byte distinguishes plain
    // 4B (return as-is) from extended (detoast).
    let ptr = unsafe {
        if varatt_is_4b_u(p) {
            // VARATT_IS_4B_U: not extended -> return as-is (no copy).
            p as *const RangeType
        } else if varatt_is_short(p) && !varatt_is_external(p) {
            // Short (1-byte header) varlena: `pg_detoast_datum` (detoast_attr)
            // converts it to the 4-byte-header form `range_deserialize` /
            // `range_get_flags` require (those read `VARSIZE_4B` + the fixed
            // `sizeof(RangeType)` header offset). Without this un-pack, a heap-
            // stored short-header range (under SHORT_VARLENA_PACKING) would read
            // its length word and bound offsets 3 bytes off. The exact on-disk
            // footprint is `VARSIZE_SHORT` (1-byte header + payload).
            let len = varsize_short(p);
            let bytes = core::slice::from_raw_parts(p, len);
            let copy = detoast::detoast_attr::call(mcx, bytes)?;
            copy.leak().as_ptr() as *const RangeType
        } else {
            // Extended (compressed or external): hand the verbatim datum bytes
            // to detoast_attr, which returns a plain copy in `mcx`.
            let len = if varatt_is_external(p) {
                // detoast_external_attr/detoast_attr inspect the external
                // pointer header; the verbatim datum is VARHDRSZ_EXTERNAL + tag
                // bytes. detoast_attr reads the tag itself, so pass the bytes it
                // needs: a TOAST pointer is at most 1 (header) + 1 (tag) + 18.
                // We cannot know the exact tag size here without varatt internals,
                // so route an external pointer through the detoast seam by its
                // minimal on-disk footprint.
                varsize_external(p)
            } else {
                // Compressed inline (4B_C): full 4-byte header length.
                varsize_4b(p)
            };
            let bytes = core::slice::from_raw_parts(p, len);
            let copy = detoast::detoast_attr::call(mcx, bytes)?;
            copy.leak().as_ptr() as *const RangeType
        }
    };

    Ok(RangeTypeP {
        ptr,
        _marker: core::marker::PhantomData,
    })
}

/// `DatumGetRangeTypeP(d)` for the value-carrying canonical `Datum` arm: the
/// on-disk `RangeType` varlena image rides `Datum::ByRef` (header included), so
/// read it from the element image bytes rather than from a pointer word.
///
/// This is the by-reference array-element counterpart of
/// [`datum_get_range_type_p`]: a `pg_statistic` bounds-histogram entry extracted
/// by `get_attstatsslot_value_datums` carries the range image by value, whose
/// bare-word surrogate would be a non-dereferenceable in-buffer offset. The
/// returned `RangeTypeP` outlives the transient `value`, so the image is copied
/// into `mcx` (`detoast_attr`: a plain inline varlena is the verbatim-copy
/// fall-through; a compressed/external image is fetched/decompressed, mirroring
/// `PG_DETOAST_DATUM`).
pub fn datum_get_range_type_p_value<'mcx>(
    mcx: Mcx<'mcx>,
    value: &types_tuple::heaptuple::Datum<'mcx>,
) -> PgResult<RangeTypeP<'mcx>> {
    // The element image bytes (the verbatim on-disk varlena, header included).
    let bytes = value.as_ref_bytes();
    let copy = detoast::detoast_attr::call(mcx, bytes)?;
    let ptr = copy.leak().as_ptr() as *const RangeType;

    Ok(RangeTypeP {
        ptr,
        _marker: core::marker::PhantomData,
    })
}

/// `VARSIZE_EXTERNAL(PTR)` -- `VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(PTR))`.
/// `VARHDRSZ_EXTERNAL` (offsetof(varattrib_1b_e, va_data)) == 2; the tag byte at
/// offset 1 selects the payload size (VARTAG_SIZE, varatt.h).
#[inline]
unsafe fn varsize_external(ptr: *const u8) -> usize {
    const VARHDRSZ_EXTERNAL: usize = 2;
    let tag = *ptr.add(1);
    // VARTAG_SIZE (varatt.h): INDIRECT=sizeof(varatt_indirect)=16,
    // EXPANDED_RO/RW=sizeof(varatt_expanded)=8/16, ONDISK=sizeof(varatt_external)=18.
    let payload = match tag {
        // VARTAG_INDIRECT
        1 => 16usize,
        // VARTAG_EXPANDED_RO / VARTAG_EXPANDED_RW
        2 | 3 => 16usize,
        // VARTAG_ONDISK
        18 => 18usize,
        // Unknown tag: VARTAG_SIZE asserts; mirror by trusting the tag value as
        // the size (VARTAG_SIZE(tag) is the identity for ONDISK).
        other => other as usize,
    };
    VARHDRSZ_EXTERNAL + payload
}

/// `pg_detoast_datum_packed(DatumGetPointer(d))` (fmgr.h:
/// `PG_DETOAST_DATUM_PACKED`): fetch/decompress only when extended, keeping a
/// short (1B) header as-is. Returns the (possibly new, `mcx`-allocated) datum.
fn detoast_packed<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<Datum> {
    let p = d.as_usize() as *const u8;
    // SAFETY: `d` is a varlena pointer.
    unsafe {
        // VARATT_IS_COMPRESSED(p) || VARATT_IS_EXTERNAL(p) -> detoast; else as-is.
        // VARATT_IS_4B_U is "uncompressed 4B"; VARATT_IS_1B (non-E) is short.
        // pg_detoast_datum_packed only acts on compressed or external.
        if varatt_is_external(p) {
            let len = varsize_external(p);
            let bytes = core::slice::from_raw_parts(p, len);
            let copy = detoast::detoast_attr::call(mcx, bytes)?;
            Ok(Datum::from_usize(copy.leak().as_ptr() as usize))
        } else if !varatt_is_4b_u(p) && !varatt_is_1b(p) {
            // 4B compressed (the only remaining "extended" form).
            let len = varsize_4b(p);
            let bytes = core::slice::from_raw_parts(p, len);
            let copy = detoast::detoast_attr::call(mcx, bytes)?;
            Ok(Datum::from_usize(copy.leak().as_ptr() as usize))
        } else {
            // Plain 4B or short 1B: returned unchanged.
            Ok(d)
        }
    }
}

/// `datum_compute_size(data_length, val, typbyval, typalign, typlen, typstorage)`
/// (rangetypes.c:2747): running serialized size of one bound value.
pub fn datum_compute_size(
    data_length: usize,
    val: Datum,
    _typbyval: bool,
    typalign: u8,
    typlen: i16,
    typstorage: u8,
) -> usize {
    let p = val.as_usize() as *const u8;
    // SAFETY: for the packable short-header path `val` is a varlena pointer;
    // for fixed-length/by-value paths the pointer is not dereferenced.
    unsafe {
        if type_is_packable(typlen, typstorage) && varatt_can_make_short(p) {
            // anticipating conversion to a short varlena header: adjust length
            // and don't count any alignment
            data_length + varatt_converted_short_size(p)
        } else {
            let d = att_align_datum(data_length, typalign, typlen, val);
            att_addlength_datum(d, typlen, val)
        }
    }
}

/// `datum_write(ptr, datum, typbyval, typalign, typlen, typstorage)`
/// (rangetypes.c:2773): write the given datum beginning at `ptr` (after
/// advancing to correct alignment, if needed), returning the pointer
/// incremented by the space used.
///
/// `ptr` is the live write cursor (C's `Pointer ptr`), modeled here as a
/// `*mut RangeType` raw address; the return is the advanced cursor as a raw
/// address (`usize`), exactly mirroring C returning `Pointer`.
pub fn datum_write(
    ptr: *mut RangeType,
    datum: Datum,
    typbyval: bool,
    typalign: u8,
    typlen: i16,
    typstorage: u8,
) -> usize {
    let mut cur = ptr as usize;
    let data_length: usize;

    // SAFETY: `cur` is a cursor into the zero-filled image sized by
    // datum_compute_size; all writes stay inside it.
    unsafe {
        if typbyval {
            // pass-by-value
            cur = att_align_nominal(cur, typalign);
            store_att_byval(cur as *mut u8, datum, typlen);
            data_length = typlen as usize;
        } else if typlen == -1 {
            // varlena
            let val = datum.as_usize() as *const u8;

            if varatt_is_external(val) {
                // Must never put a toast pointer inside a range object; caller
                // should have detoasted it.
                panic!("cannot store a toast pointer inside a range");
            } else if varatt_is_short(val) {
                // no alignment for short varlenas
                data_length = varsize_short(val);
                core::ptr::copy_nonoverlapping(val, cur as *mut u8, data_length);
            } else if type_is_packable(typlen, typstorage) && varatt_can_make_short(val) {
                // convert to short varlena -- no alignment
                data_length = varatt_converted_short_size(val);
                set_varsize_short(cur as *mut u8, data_length);
                core::ptr::copy_nonoverlapping(vardata(val), (cur + 1) as *mut u8, data_length - 1);
            } else {
                // full 4-byte header varlena
                cur = att_align_nominal(cur, typalign);
                data_length = varsize(val);
                core::ptr::copy_nonoverlapping(val, cur as *mut u8, data_length);
            }
        } else if typlen == -2 {
            // cstring ... never needs alignment
            debug_assert_eq!(typalign, TYPALIGN_CHAR);
            let s = datum.as_usize() as *const u8;
            let mut n = 0usize;
            while *s.add(n) != 0 {
                n += 1;
            }
            data_length = n + 1;
            core::ptr::copy_nonoverlapping(s, cur as *mut u8, data_length);
        } else {
            // fixed-length pass-by-reference
            cur = att_align_nominal(cur, typalign);
            debug_assert!(typlen > 0);
            data_length = typlen as usize;
            core::ptr::copy_nonoverlapping(datum.as_usize() as *const u8, cur as *mut u8, data_length);
        }
    }

    cur + data_length
}

/// Read the verbatim varlena image of a (plain, already-detoasted) `RangeType`
/// as owned bytes — `memcpy(palloc(VARSIZE(range)), range, VARSIZE(range))`.
///
/// `RangeTypeGetDatum(range)` is C's `PointerGetDatum(range)`: a bare pointer to
/// the range's flattened 4B varlena. The owned fmgr boundary cannot return a raw
/// pointer word (the referent would dangle past the call's context), so a
/// by-reference range result crosses through the `RefPayload::Varlena` side
/// channel; this distills the bytes that payload carries. The `RangeType` a
/// constructor / `make_range` produces is always a plain 4B varlena, so this is
/// the `varsize_any` 4B path.
pub fn range_to_varlena_bytes(range: RangeTypeP<'_>) -> std::vec::Vec<u8> {
    let p = range.ptr as *const u8;
    // SAFETY: `range.ptr` is a valid, 'mcx-lived, fully-detoasted plain varlena
    // (RangeTypeP's construction invariant); `VARSIZE_ANY` reads its length from
    // the header, and the whole image is contiguous and readable.
    unsafe {
        let len = varsize_any(p);
        core::slice::from_raw_parts(p, len).to_vec()
    }
}

/// `DatumGetRangeTypeP` over a by-reference range argument that crossed the fmgr
/// boundary as `RefPayload::Varlena` bytes (the owned-boundary lane for a by-ref
/// argument; C reads it straight off the pointer `Datum`). Copies the verbatim
/// varlena image into `mcx` (MAXALIGN(8)-aligned, matching `range_serialize`'s
/// invariant so the in-image absolute-address reads stay aligned) and returns a
/// handle pointing at it. The bytes are already a plain 4B varlena (the producer
/// serialized them via `range_to_varlena_bytes`), so no detoast is needed.
pub fn range_p_from_varlena_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    bytes: &[u8],
) -> PgResult<RangeTypeP<'mcx>> {
    // A by-reference range arg crosses the boundary as the verbatim heap-deform
    // image. Under SHORT_VARLENA_PACKING a small stored range carries a 1-byte
    // ("short") header; `range_deserialize` / `range_get_flags` /
    // `range_type_get_oid` read `VARSIZE_4B`, the `rangetypid` at the fixed
    // `RangeType` offset (4), and the bound payload past `sizeof(RangeType)`, so
    // the image MUST be in 4-byte-header form. C's `DatumGetRangeTypeP` is
    // `PG_DETOAST_DATUM`, which un-packs short->4B; mirror that here (this is the
    // hot arg path — `getarg_range_p` routes by-ref args through this, NOT through
    // `datum_get_range_type_p`). Behavior-preserving with the flag OFF (no stored
    // range is short).
    //
    // SAFETY: `bytes` is a fully-bounded varlena image; the header byte
    // distinguishes the short (1B, low bit set, != 0x01) from the 4B form.
    let short = unsafe {
        !bytes.is_empty()
            && varatt_is_short(bytes.as_ptr())
            && !varatt_is_1b_e(bytes.as_ptr())
    };
    if short {
        // Un-pack: SET_VARSIZE(new, data + VARHDRSZ); copy VARDATA_SHORT.
        let data_size = unsafe { varsize_short(bytes.as_ptr()) } - VARHDRSZ_SHORT;
        let new_size = data_size + VARHDRSZ;
        let base = palloc0_maxaligned(mcx, new_size)?;
        // SAFETY: `base` heads a freshly allocated, zero-filled, MAXALIGN'd image
        // of `new_size` bytes; write the 4B header then the short payload.
        unsafe {
            set_varsize(base, new_size);
            core::ptr::copy_nonoverlapping(
                bytes.as_ptr().add(VARHDRSZ_SHORT),
                base.add(VARHDRSZ),
                data_size,
            );
        }
        return Ok(RangeTypeP {
            ptr: base as *const RangeType,
            _marker: core::marker::PhantomData,
        });
    }
    let base = palloc0_maxaligned(mcx, bytes.len())?;
    // SAFETY: `base` heads a freshly allocated, zero-filled, `bytes.len()`-byte
    // MAXALIGN'd image; copying the verbatim varlena bytes into it yields a valid
    // plain `RangeType` varlena that lives for `'mcx`.
    unsafe {
        core::ptr::copy_nonoverlapping(bytes.as_ptr(), base, bytes.len());
    }
    Ok(RangeTypeP {
        ptr: base as *const RangeType,
        _marker: core::marker::PhantomData,
    })
}
