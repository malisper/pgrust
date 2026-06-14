//! Port of PostgreSQL 18.3 `src/backend/utils/adt/datum.c`: the abstract-`Datum`
//! manipulation routines (`datumGetSize`, `datumCopy`, `datumTransfer`,
//! `datumIsEqual`, `datum_image_eq`, `datum_image_hash`, `btequalimage`,
//! `datumEstimateSpace`, `datumSerialize`, `datumRestore`).
//!
//! datum.c is a thin dispatch on a type's `(typByVal, typLen)` over the bytes a
//! `Datum` refers to. This port serves the SAME logic through TWO distinct,
//! already-consumed seam contracts that we MUST satisfy verbatim and never
//! unify:
//!
//! * the **byte-model** lane (`backend-utils-adt-scalar-seams::datum_copy`),
//!   where a by-reference value crosses as the verbatim on-disk bytes held in
//!   [`Datum::ByRef`] (already detoasted, varlena header included) — the
//!   established idiomatic stand-in for C's bare pointer into a tuple. Consumed
//!   by `brin-tuple`.
//!
//! * the **bare-`Datum`** lane (`backend-utils-adt-datum-seams`'s
//!   `datum_copy` / `datum_estimate_space` / `datum_serialize` / `datum_restore`
//!   / `datum_image_hash` / `datum_image_eq`), where a by-reference value
//!   crosses as a bare machine-word [`Datum`] (`types-datum`: just a `usize`,
//!   no embedded length, exactly C's machine word) that points at bytes the
//!   caller keeps alive in `mcx`. This lane is implemented with `unsafe`
//!   raw-pointer reads mirroring C's `DatumGetPointer` + `VARSIZE_ANY` /
//!   `strlen` / `memcpy`: the length is recovered from the pointed-at bytes.
//!   This opacity is INHERITED from C's `Datum` contract, not introduced.
//!   Consumed by `nbtree`, `nodeMemoize`, and `misc2` `rowtypes` (via its
//!   `tuple_value_as_datum` pointer bridge). `backend-nodes-core`
//!   `copyParamList` migrated off this lane to the canonical `datum_copy_v`.
//!
//! Cyclic owners reached by seam: the expanded-object subsystem
//! (`EOH_get_flat_size` / `EOH_flatten_into`, `backend-utils-adt-misc2-seams`).
//! `EOH_get_flat_size` / `EOH_flatten_into` are reached on the by-reference
//! expanded-object copy path; they cross the mcx-ownership boundary `misc2`
//! already flagged as mirror-and-panic. The serial path that this port serves
//! never produces a read-write expanded pointer, so that boundary is reached
//! only by a genuine expanded-object caller.
//!
//! `hash_bytes` (`common/hashfn.h`) is a non-cyclic direct dependency, called
//! directly (as bool.c calls `hash_bytes_uint32`).

extern crate alloc;

use alloc::format;

use mcx::{slice_in, Mcx, PgVec};
use types_core::primitive::Size;
// `ExpandedObjectRef` is the `&[u8]`-over-a-varlena-image handle (NOT the Datum
// shim); the canonical value enum is `types_tuple::…::Datum`. The bare-word
// newtype `types_datum::Datum` is imported under the alias `ScalarWord` solely
// for the audited DSM-cursor ABI edge (`datum_restore` / `datum_serialize`)
// whose seam contract still carries a bare machine word for the not-yet-migrated
// nbtree array (de)serialize and params.c restore callers; every value-model
// operation uses the `Datum` enum and its `from_*`/`as_*` codec.
use types_datum::{Datum as ScalarWord, ExpandedObjectRef};
use types_error::{PgError, PgResult, ERRCODE_DATA_EXCEPTION};
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_adt_misc2_seams::{eoh_flatten_into, eoh_get_flat_size};

// ===========================================================================
// varatt.h helpers over verbatim datum bytes (little-endian model).
//
// These read the SAME bytes brin-tuple's `fetchatt` / `varsize_any` and
// rowtypes' `varlena_payload` already read. They operate on a `&[u8]` view of
// the value's on-disk image (the `Datum::ByRef` payload, or, in the
// bare-Datum lane, a slice synthesised over the pointed-at memory).
// ===========================================================================

/// `VARHDRSZ` (`c.h`): the 4-byte varlena length word.
const VARHDRSZ: usize = 4;
/// `VARHDRSZ_SHORT` (`varatt.h`): a 1-byte ("short") varlena header.
const VARHDRSZ_SHORT: usize = 1;
/// `VARHDRSZ_EXTERNAL` (`varatt.h`): `offsetof(varattrib_1b_e, va_data)`.
const VARHDRSZ_EXTERNAL: usize = 2;

/// `VARATT_IS_1B_E(PTR)` (`varatt.h`): a 1-byte TOAST pointer (`va_header == 0x01`).
#[inline]
fn varatt_is_external(b: &[u8]) -> bool {
    b[0] == 0x01
}

/// `VARATT_IS_1B(PTR)` (`varatt.h`, little-endian): low bit of the first byte set.
#[inline]
fn varatt_is_1b(b: &[u8]) -> bool {
    (b[0] & 0x01) == 0x01
}

/// `VARTAG_SIZE(VARTAG_EXTERNAL(PTR))` (`varatt.h`): payload size of a TOAST
/// pointer for the given `va_tag`.
#[inline]
fn vartag_size(tag: u8) -> usize {
    const VARTAG_INDIRECT: u8 = 1;
    const VARTAG_EXPANDED_RO: u8 = 2;
    const VARTAG_EXPANDED_RW: u8 = 3;
    const VARTAG_ONDISK: u8 = 18;
    match tag {
        VARTAG_INDIRECT => 8,                          // sizeof(varatt_indirect)
        VARTAG_EXPANDED_RO | VARTAG_EXPANDED_RW => 8,  // sizeof(varatt_expanded)
        VARTAG_ONDISK => 16,                           // sizeof(varatt_external)
        _ => 0,
    }
}

/// `VARSIZE_ANY(PTR)` (`varatt.h`): total bytes the varlena occupies, dispatching
/// on the header form (external TOAST pointer / short 1-byte / plain 4-byte).
#[inline]
fn varsize_any(b: &[u8]) -> usize {
    if varatt_is_external(b) {
        VARHDRSZ_EXTERNAL + vartag_size(b[1])
    } else if varatt_is_1b(b) {
        // VARSIZE_1B = (va_header >> 1) & 0x7F
        ((b[0] >> 1) & 0x7F) as usize
    } else {
        // VARSIZE_4B = (va_header >> 2) & 0x3FFFFFFF
        let hdr = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
        ((hdr >> 2) & 0x3FFF_FFFF) as usize
    }
}

/// `VARATT_IS_EXTERNAL_EXPANDED(PTR)` (`varatt.h`): a 1-byte external TOAST
/// pointer whose tag is `VARTAG_EXPANDED_RO`/`_RW`.
#[inline]
fn varatt_is_external_expanded(b: &[u8]) -> bool {
    // VARATT_IS_EXTERNAL(b) && VARTAG_IS_EXPANDED(b[1])
    varatt_is_external(b) && b.len() >= 2 && (b[1] & !1u8) == 2
}

/// `(VARDATA_ANY(PTR), VARSIZE_ANY_EXHDR(PTR))` over an in-line, already-detoasted
/// varlena image: payload slice and its logical length, handling the 1-byte and
/// 4-byte header forms (an external pointer never reaches here in this lane).
#[inline]
fn varlena_payload(b: &[u8]) -> (&[u8], usize) {
    let hdr = if varatt_is_1b(b) { VARHDRSZ_SHORT } else { VARHDRSZ };
    let total = varsize_any(b);
    (&b[hdr..total], total - hdr)
}

// ===========================================================================
// datumGetSize / datumIsEqual / image-eq / image-hash over the BYTE model.
//
// The verbatim on-disk bytes of one by-reference value (the `Datum::ByRef`
// payload). `datumGetSizeBytes` is the `att_addlength_datum`-shaped size read.
// ===========================================================================

/// `datumGetSize(value, typByVal, typLen)` (datum.c) over the byte model. For a
/// by-reference value `bytes` is its verbatim on-disk image (varlena header
/// included). For a by-value value `bytes` is `None` (size is `typLen`).
///
/// Mirrors the C control flow: by-value -> `typLen`; by-ref `typLen > 0` ->
/// `typLen`; `typLen == -1` -> `VARSIZE_ANY`; `typLen == -2` -> `strlen + 1`;
/// any other `typLen` -> `elog(ERROR, "invalid typLen")`.
fn datum_get_size_bytes(bytes: Option<&[u8]>, typ_byval: bool, typ_len: i32) -> PgResult<Size> {
    if typ_byval {
        // Pass-by-value types are always fixed-length (Assert typLen in 1..=8).
        Ok(typ_len as Size)
    } else if typ_len > 0 {
        // Fixed-length pass-by-ref type.
        Ok(typ_len as Size)
    } else if typ_len == -1 {
        // Varlena. C: ereport if the pointer is NULL — here a ByRef value always
        // carries its bytes (a None would be a by-value misdispatch).
        let s = bytes.ok_or_else(invalid_datum_pointer)?;
        Ok(varsize_any(s) as Size)
    } else if typ_len == -2 {
        // cstring: strlen(s) + 1.
        let s = bytes.ok_or_else(invalid_datum_pointer)?;
        Ok(cstr_len(s) + 1)
    } else {
        Err(PgError::error(format!("invalid typLen: {typ_len}")))
    }
}

/// `ereport(ERROR, errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid Datum pointer"))`.
fn invalid_datum_pointer() -> PgError {
    PgError::error("invalid Datum pointer").with_sqlstate(ERRCODE_DATA_EXCEPTION)
}

/// `strlen(s)` over a NUL-terminated byte image (cstring datatype, typLen == -2).
#[inline]
fn cstr_len(s: &[u8]) -> usize {
    s.iter().position(|&c| c == 0).unwrap_or(s.len())
}

// ===========================================================================
// datumCopy — BYTE model (backend-utils-adt-scalar-seams::datum_copy).
//
// Consumed by brin-tuple: a value crosses as `Datum`, the deep copy lands
// in the caller's `mcx`.
// ===========================================================================

/// `datumCopy(value, typByVal, typLen)` (datum.c) — byte-model form. By-value
/// values are returned verbatim; by-reference values are deep-copied into `mcx`
/// (C: `palloc` in the current context). A varlena read-write/read-only
/// **expanded** object is flattened via the misc2 `EOH_*` seams (C: the
/// `VARATT_IS_EXTERNAL_EXPANDED` leg). `typLen == -1` (varlena) copies
/// `VARSIZE_ANY` bytes verbatim; other by-ref types copy `datumGetSize` bytes.
pub fn datum_copy<'mcx>(
    mcx: Mcx<'mcx>,
    value: &Datum<'_>,
    typ_byval: bool,
    typ_len: i16,
) -> PgResult<Datum<'mcx>> {
    if typ_byval {
        // res = value
        return Ok(match value {
            Datum::ByVal(d) => Datum::ByVal(*d),
            // A by-value type must arrive as ByVal (C: the Datum word is the value).
            Datum::ByRef(_) => {
                panic!("datumCopy: by-value type arrived as a by-reference value")
            }
        });
    }

    let bytes = value.as_ref_bytes();

    if typ_len == -1 {
        // Varlena datatype.
        if varatt_is_external_expanded(bytes) {
            // Flatten the expanded object into the caller's memory context.
            let eoh = ExpandedObjectRef::from_expanded_datum_bytes(bytes);
            let resultsize = eoh_get_flat_size::call(eoh)?;
            let mut dest = zeroed_vec(mcx, resultsize)?;
            eoh_flatten_into::call(eoh, &mut dest)?;
            Ok(Datum::ByRef(dest))
        } else {
            // Otherwise copy the varlena datum verbatim (realSize = VARSIZE_ANY).
            let real_size = varsize_any(bytes);
            Ok(Datum::ByRef(slice_in(mcx, &bytes[..real_size])?))
        }
    } else {
        // Pass by reference, but not varlena, so not toasted.
        let real_size = datum_get_size_bytes(Some(bytes), false, typ_len as i32)?;
        Ok(Datum::ByRef(slice_in(mcx, &bytes[..real_size])?))
    }
}

/// Allocate `len` zeroed bytes in `mcx` (a `palloc` target the `EOH_flatten_into`
/// owner fills); flattening writes every byte.
fn zeroed_vec<'mcx>(mcx: Mcx<'mcx>, len: usize) -> PgResult<PgVec<'mcx, u8>> {
    let zeros = alloc::vec![0u8; len];
    slice_in(mcx, &zeros)
}

/// `datumIsEqual(value1, value2, typByVal, typLen)` (datum.c) — byte-model form.
/// By-value compares the `Datum` words; by-reference compares `datumGetSize`
/// bytes with `memcmp` after a length check. (No toast handling, per the C
/// contract: the bytes are compared as-is.)
pub fn datum_is_equal(
    value1: &Datum<'_>,
    value2: &Datum<'_>,
    typ_byval: bool,
    typ_len: i32,
) -> PgResult<bool> {
    if typ_byval {
        let (w1, w2) = match (value1, value2) {
            (Datum::ByVal(a), Datum::ByVal(b)) => (a.as_usize(), b.as_usize()),
            _ => panic!("datumIsEqual: by-value type arrived as a by-reference value"),
        };
        Ok(w1 == w2)
    } else {
        let s1 = value1.as_ref_bytes();
        let s2 = value2.as_ref_bytes();
        let size1 = datum_get_size_bytes(Some(s1), false, typ_len)?;
        let size2 = datum_get_size_bytes(Some(s2), false, typ_len)?;
        if size1 != size2 {
            return Ok(false);
        }
        Ok(s1[..size1] == s2[..size1])
    }
}

/// `datum_image_eq(value1, value2, typByVal, typLen)` (datum.c) — byte-model
/// form. The toast lane (`PG_DETOAST_DATUM_PACKED` / `toast_raw_datum_size`)
/// reads the already-detoasted `ByRef` payload directly via `VARDATA_ANY` /
/// `VARSIZE_ANY_EXHDR`, exactly as rowtypes does.
pub fn datum_image_eq_bytes(
    value1: &Datum<'_>,
    value2: &Datum<'_>,
    typ_byval: bool,
    typ_len: i32,
) -> PgResult<bool> {
    if typ_byval {
        let (w1, w2) = byval_words("datum_image_eq", value1, value2);
        Ok(w1 == w2)
    } else if typ_len > 0 {
        // Fixed-length pass-by-ref: memcmp of typLen bytes.
        let n = typ_len as usize;
        let b1 = value1.as_ref_bytes();
        let b2 = value2.as_ref_bytes();
        Ok(b1[..n] == b2[..n])
    } else if typ_len == -1 {
        // Varlena: compare logical payloads after a logical-length check.
        let (data1, len1) = varlena_payload(value1.as_ref_bytes());
        let (data2, len2) = varlena_payload(value2.as_ref_bytes());
        if len1 != len2 {
            return Ok(false);
        }
        Ok(data1 == data2)
    } else if typ_len == -2 {
        // cstring: compare strlen+1 bytes after a length check.
        let s1 = value1.as_ref_bytes();
        let s2 = value2.as_ref_bytes();
        let len1 = cstr_len(s1) + 1;
        let len2 = cstr_len(s2) + 1;
        if len1 != len2 {
            return Ok(false);
        }
        Ok(s1[..len1] == s2[..len1])
    } else {
        Err(PgError::error(format!("unexpected typLen: {typ_len}")))
    }
}

/// `datum_image_hash(value, typByVal, typLen)` (datum.c) — byte-model form.
/// `hash_bytes` over the relevant byte image: `sizeof(Datum)` for by-value,
/// `typLen` bytes for fixed-length by-ref, the logical varlena payload for
/// `typLen == -1`, and `strlen + 1` for a cstring.
pub fn datum_image_hash_bytes(
    value: &Datum<'_>,
    typ_byval: bool,
    typ_len: i32,
) -> PgResult<u32> {
    if typ_byval {
        let d = match value {
            Datum::ByVal(d) => *d,
            Datum::ByRef(_) => {
                panic!("datum_image_hash: by-value type arrived as a by-reference value")
            }
        };
        // hash_bytes((unsigned char *) &value, sizeof(Datum))
        Ok(common_hashfn::hash_bytes(&d.as_usize().to_ne_bytes()))
    } else if typ_len > 0 {
        let b = value.as_ref_bytes();
        Ok(common_hashfn::hash_bytes(&b[..typ_len as usize]))
    } else if typ_len == -1 {
        let (data, _len) = varlena_payload(value.as_ref_bytes());
        Ok(common_hashfn::hash_bytes(data))
    } else if typ_len == -2 {
        let s = value.as_ref_bytes();
        let len = cstr_len(s) + 1;
        Ok(common_hashfn::hash_bytes(&s[..len]))
    } else {
        Err(PgError::error(format!("unexpected typLen: {typ_len}")))
    }
}

/// `btequalimage(opcintype)` (datum.c) — the generic "equalimage" support
/// function. Returns `true` unconditionally (C: `PG_RETURN_BOOL(true)`), the
/// `opcintype` argument unused.
pub fn btequalimage(_opcintype: u32) -> bool {
    true
}

fn byval_words(
    who: &str,
    value1: &Datum<'_>,
    value2: &Datum<'_>,
) -> (usize, usize) {
    match (value1, value2) {
        (Datum::ByVal(a), Datum::ByVal(b)) => (a.as_usize(), b.as_usize()),
        _ => panic!("{who}: by-value type arrived as a by-reference value"),
    }
}

// ===========================================================================
// The BARE-word residual ABI edge (backend-utils-adt-datum-seams).
//
// These functions are the audited bare-machine-word edge the prompt sanctions
// (fmgr-return / DSM-cursor): the seam contract carries a `ScalarWord`
// (`types_datum::Datum`, C's plain `usize` machine word) for callers that have
// not yet migrated to the `Datum` enum — nbtree array (de)serialize and
// params.c restore (`datum_serialize` / `datum_restore`, a `*mut u8` DSM
// cursor). The value-model lane above and the `*_v` enum seams below are the
// migration target; these stay until their cross-crate consumers move over.
//
// A by-reference `ScalarWord` is C's machine word == a raw pointer into bytes
// the caller keeps alive in `mcx`. We recover the length from the pointed-at
// bytes with `unsafe` reads mirroring C's `DatumGetPointer` +
// `VARSIZE_ANY`/`strlen`. This opacity is INHERITED from C's `Datum` contract.
// ===========================================================================

/// `DatumGetPointer(value)` over a bounded prefix: a `&[u8]` view of the bytes
/// at the pointer word. `len` is how many bytes we are about to read (already
/// computed, or an upper bound). SAFETY: the caller (per datum.c's contract)
/// holds a live by-reference Datum whose target spans at least `len` bytes.
#[inline]
unsafe fn datum_ptr_slice<'a>(value: ScalarWord, len: usize) -> &'a [u8] {
    let p = value.as_usize() as *const u8;
    core::slice::from_raw_parts(p, len)
}

/// `VARSIZE_ANY(DatumGetPointer(value))` for a varlena Datum: read the header
/// first to learn the total size, then return the full image view.
/// SAFETY: `value` points at a valid varlena (caller keeps it alive in mcx).
unsafe fn varlena_image<'a>(value: ScalarWord) -> &'a [u8] {
    // Read enough of the header to compute VARSIZE_ANY (4 bytes covers the
    // 1-byte short / external tag bytes and the 4-byte length word).
    let head = datum_ptr_slice(value, VARHDRSZ);
    let total = varsize_any(head);
    datum_ptr_slice(value, total)
}

/// `datumGetSize(value, typByVal, typLen)` over the bare-Datum lane: dereference
/// the pointer word for the by-ref cases.
/// SAFETY: by-ref `value` points at a live image of the type.
unsafe fn datum_get_size_word(value: ScalarWord, typ_byval: bool, typ_len: i32) -> PgResult<Size> {
    if typ_byval || typ_len > 0 {
        datum_get_size_bytes(None, typ_byval, typ_len)
    } else if typ_len == -1 {
        let img = varlena_image(value);
        Ok(varsize_any(img) as Size)
    } else if typ_len == -2 {
        // strlen + 1 over the C string at the pointer. Read byte-by-byte until NUL.
        Ok(cstring_len_at(value) + 1)
    } else {
        Err(PgError::error(format!("invalid typLen: {typ_len}")))
    }
}

/// `strlen((char *) DatumGetPointer(value))`.
/// SAFETY: `value` points at a NUL-terminated C string the caller keeps alive.
unsafe fn cstring_len_at(value: ScalarWord) -> usize {
    let p = value.as_usize() as *const u8;
    let mut n = 0usize;
    while *p.add(n) != 0 {
        n += 1;
    }
    n
}

// `datumCopy` / `datumTransfer` over the bare-machine-word `Datum` lane —
// removed in the Datum-unification cleanup. Their by-reference path forged a
// pointer word (`leak_bytes_as_datum`, a `Box::leak` + `PointerGetDatum`), the
// kind of pointer-forge the unification retires. The only consumer of the
// bare-word `datum_copy` seam (params.c `copyParamList`) migrated to
// `datum_copy_v`, which returns the canonical `Datum<'mcx>::ByRef` (see
// `datum_copy_v` below, which forwards to the byte-model [`datum_copy`]). The
// expanded-RW `TransferExpandedObject` leg `datumTransfer` carried has no live
// caller and no seam, so it is dropped with the lane.

// `datum_image_eq` over the bare-machine-word `Datum` lane — removed in the
// Datum-unification cleanup. Its by-reference legs read through raw pointer
// words (`datum_ptr_slice` / `varlena_image` / `cstring_len_at`), the unsafe
// byte-window the unification retires. Its only consumer (misc2 rowtypes'
// `tuple_value_as_datum` pointer bridge) migrated to the canonical value-enum
// `datum_image_eq_v` (see below, which forwards to the byte-model
// [`datum_image_eq_bytes`]); the bare-word `datum_image_eq` seam now has zero
// callers, so it is dropped with the lane.

/// `datumSerialize(value, isnull, typByVal, typLen, &cursor)` (datum.c): flatten
/// one datum into `cursor` (a `*mut u8` modelling C's `char **start_address`),
/// returning the advanced cursor.
///
/// Header word: `-2` for NULL, `-1` for pass-by-value, else the payload byte
/// count (expanded objects use their flattened size). Pass-by-value writes
/// `sizeof(Datum)` bytes; by-ref writes the payload bytes (expanded objects are
/// flattened through a maxaligned scratch buffer, as C does).
///
/// SAFETY: `cursor` has at least `datumEstimateSpace` bytes of writable storage;
/// a non-null by-ref `value` points at a live image.
pub fn datum_serialize(
    value: ScalarWord,
    isnull: bool,
    typ_byval: bool,
    typ_len: i32,
    cursor: *mut u8,
) -> *mut u8 {
    unsafe {
        // Compute header word and any flattened-expanded image.
        let mut eoh: Option<ExpandedObjectRef<'_>> = None;
        let header: i32 = if isnull {
            -2
        } else if typ_byval {
            -1
        } else if typ_len == -1 && varatt_is_external_expanded(varlena_image(value)) {
            let e = ExpandedObjectRef::from_expanded_datum_bytes(varlena_image(value));
            let sz = eoh_get_flat_size::call(e).expect("EOH_get_flat_size") as i32;
            eoh = Some(e);
            sz
        } else {
            datum_get_size_word(value, typ_byval, typ_len).expect("datumGetSize") as i32
        };

        // memcpy(*start_address, &header, sizeof(int)); *start_address += sizeof(int);
        let hbytes = header.to_ne_bytes();
        core::ptr::copy_nonoverlapping(hbytes.as_ptr(), cursor, hbytes.len());
        let mut cur = cursor.add(hbytes.len());

        if !isnull {
            if typ_byval {
                // memcpy(*start_address, &value, sizeof(Datum));
                let vbytes = value.as_usize().to_ne_bytes();
                core::ptr::copy_nonoverlapping(vbytes.as_ptr(), cur, vbytes.len());
                cur = cur.add(vbytes.len());
            } else if let Some(e) = eoh {
                // EOH_flatten_into wants a maxaligned target; flatten into scratch
                // then memcpy. (C: palloc(header); EOH_flatten_into; memcpy; pfree.)
                let n = header as usize;
                let mut tmp = alloc::vec![0u8; n];
                eoh_flatten_into::call(e, &mut tmp).expect("EOH_flatten_into");
                core::ptr::copy_nonoverlapping(tmp.as_ptr(), cur, n);
                cur = cur.add(n);
            } else {
                // memcpy(*start_address, DatumGetPointer(value), header);
                let n = header as usize;
                let src = value.as_usize() as *const u8;
                core::ptr::copy_nonoverlapping(src, cur, n);
                cur = cur.add(n);
            }
        }
        cur
    }
}

/// `datumRestore(&cursor, &isnull)` (datum.c): read one datum from `cursor`,
/// returning `(value, isnull, advanced_cursor)`. Header `-2` => NULL; `-1` =>
/// pass-by-value (`sizeof(Datum)` bytes follow); otherwise a by-reference
/// payload of `header` bytes, copied into freshly leaked `mcx`-charged storage.
///
/// Note: C `palloc`s in `CurrentMemoryContext`; this seam has no `Mcx` param
/// (its declared contract is `(*mut u8) -> (Datum, bool, *mut u8)`, consumed by
/// nbtree's array restore). The restored by-ref payload is copied into a leaked
/// heap allocation whose pointer is returned as the `Datum` word — the caller
/// owns it exactly as it would a `palloc`'d chunk. SAFETY: `cursor` points at a
/// valid datumSerialize image with at least the indicated bytes.
pub fn datum_restore(cursor: *mut u8) -> (ScalarWord, bool, *mut u8) {
    unsafe {
        // memcpy(&header, *start_address, sizeof(int)); *start_address += sizeof(int);
        let mut hbytes = [0u8; 4];
        core::ptr::copy_nonoverlapping(cursor as *const u8, hbytes.as_mut_ptr(), 4);
        let header = i32::from_ne_bytes(hbytes);
        let mut cur = cursor.add(4);

        if header == -2 {
            // NULL.
            return (ScalarWord::null(), true, cur);
        }

        if header == -1 {
            // Pass-by-value: sizeof(Datum) bytes follow.
            let mut wbytes = [0u8; core::mem::size_of::<usize>()];
            core::ptr::copy_nonoverlapping(cur as *const u8, wbytes.as_mut_ptr(), wbytes.len());
            cur = cur.add(wbytes.len());
            return (ScalarWord::from_usize(usize::from_ne_bytes(wbytes)), false, cur);
        }

        // Pass-by-reference: copy `header` bytes (Assert header > 0).
        debug_assert!(header > 0);
        let n = header as usize;
        let mut buf = alloc::vec![0u8; n];
        core::ptr::copy_nonoverlapping(cur as *const u8, buf.as_mut_ptr(), n);
        cur = cur.add(n);
        // PointerGetDatum(palloc'd copy): own the bytes via a leaked boxed slice.
        let leaked: &'static mut [u8] = alloc::boxed::Box::leak(buf.into_boxed_slice());
        (ScalarWord::from_usize(leaked.as_ptr() as usize), false, cur)
    }
}

// ===========================================================================
// The `*_v` value-model lane (backend-utils-adt-datum-seams `*_v` variants).
//
// The migration-target contract: every value crosses as the canonical `Datum`
// enum (`ByVal(word)` / `ByRef(bytes)`), never a bare machine word, so there is
// no pointer forge and no `unsafe` raw-pointer read — a by-reference value is
// its verbatim on-disk image in `ByRef`, exactly as the byte-model lane above.
// These reuse the byte-model helpers (`datum_copy`, `datum_image_eq_bytes`,
// `datum_image_hash_bytes`, `datum_get_size_bytes`). Consumed by nodeMemoize
// (`datum_image_hash_v` / `datum_image_eq_v`) and nbtree (`datum_estimate_space_v`).
// ===========================================================================

/// `datumCopy` over the unified value enum (`datum_copy_v` seam). Identical
/// semantics to the byte-model [`datum_copy`]; the seam's `typLen` is `i32`.
pub fn datum_copy_v<'mcx>(
    mcx: Mcx<'mcx>,
    value: &Datum<'_>,
    typ_byval: bool,
    typ_len: i32,
) -> PgResult<Datum<'mcx>> {
    datum_copy(mcx, value, typ_byval, typ_len as i16)
}

/// `datumEstimateSpace` over the unified value enum (`datum_estimate_space_v`
/// seam): `sizeof(int)` header plus the payload. By-value adds `sizeof(Datum)`;
/// by-ref adds `datumGetSize` over the `ByRef` image; an expanded varlena adds
/// its flattened size (`EOH_get_flat_size`). No `unsafe`/pointer read — the
/// bytes are the `ByRef` payload.
pub fn datum_estimate_space_v(
    value: &Datum<'_>,
    isnull: bool,
    typ_byval: bool,
    typ_len: i32,
) -> Size {
    // sz = sizeof(int)
    let mut sz: Size = core::mem::size_of::<i32>();
    if !isnull {
        if typ_byval {
            sz += core::mem::size_of::<usize>();
        } else {
            let bytes = value.as_ref_bytes();
            if typ_len == -1 && varatt_is_external_expanded(bytes) {
                // Expanded objects need to be flattened.
                let eoh = ExpandedObjectRef::from_expanded_datum_bytes(bytes);
                // datumEstimateSpace's C signature returns Size with no error path;
                // EOH_get_flat_size only errors on oversize, mirroring C.
                sz += eoh_get_flat_size::call(eoh).expect("EOH_get_flat_size");
            } else {
                sz += datum_get_size_bytes(Some(bytes), false, typ_len).expect("datumGetSize");
            }
        }
    }
    sz
}

/// `datumSerialize` over the unified value enum (`datum_serialize_v` seam):
/// flatten one value into `cursor` (C's `char **start_address`) and return the
/// advanced cursor. Header word: `-2` NULL, `-1` by-value, else the payload
/// byte count (expanded objects use their flattened size). By-value writes
/// `sizeof(Datum)` bytes of the `ByVal` word; by-ref writes the `ByRef` payload
/// bytes (expanded objects flattened through a maxaligned scratch buffer).
///
/// SAFETY: `cursor` has at least `datum_estimate_space_v` bytes of writable
/// storage.
pub fn datum_serialize_v(
    value: &Datum<'_>,
    isnull: bool,
    typ_byval: bool,
    typ_len: i32,
    cursor: *mut u8,
) -> *mut u8 {
    // Compute header word and any flattened-expanded image.
    let mut eoh: Option<ExpandedObjectRef<'_>> = None;
    let header: i32 = if isnull {
        -2
    } else if typ_byval {
        -1
    } else if typ_len == -1 && varatt_is_external_expanded(value.as_ref_bytes()) {
        let e = ExpandedObjectRef::from_expanded_datum_bytes(value.as_ref_bytes());
        let sz = eoh_get_flat_size::call(e).expect("EOH_get_flat_size") as i32;
        eoh = Some(e);
        sz
    } else {
        datum_get_size_bytes(Some(value.as_ref_bytes()), false, typ_len).expect("datumGetSize")
            as i32
    };

    unsafe {
        // memcpy(*start_address, &header, sizeof(int)); *start_address += sizeof(int);
        let hbytes = header.to_ne_bytes();
        core::ptr::copy_nonoverlapping(hbytes.as_ptr(), cursor, hbytes.len());
        let mut cur = cursor.add(hbytes.len());

        if !isnull {
            if typ_byval {
                // memcpy(*start_address, &value, sizeof(Datum));
                let vbytes = value.as_usize().to_ne_bytes();
                core::ptr::copy_nonoverlapping(vbytes.as_ptr(), cur, vbytes.len());
                cur = cur.add(vbytes.len());
            } else if let Some(e) = eoh {
                // EOH_flatten_into wants a maxaligned target; flatten into scratch
                // then memcpy. (C: palloc(header); EOH_flatten_into; memcpy; pfree.)
                let n = header as usize;
                let mut tmp = alloc::vec![0u8; n];
                eoh_flatten_into::call(e, &mut tmp).expect("EOH_flatten_into");
                core::ptr::copy_nonoverlapping(tmp.as_ptr(), cur, n);
                cur = cur.add(n);
            } else {
                // memcpy(*start_address, DatumGetPointer(value), header);
                let n = header as usize;
                let src = value.as_ref_bytes();
                core::ptr::copy_nonoverlapping(src.as_ptr(), cur, n);
                cur = cur.add(n);
            }
        }
        cur
    }
}

/// `datum_image_hash` over the unified value enum (`datum_image_hash_v` seam).
/// Identical semantics to the byte-model [`datum_image_hash_bytes`].
pub fn datum_image_hash_v(value: &Datum<'_>, typ_byval: bool, typ_len: i16) -> PgResult<u32> {
    datum_image_hash_bytes(value, typ_byval, typ_len as i32)
}

/// `datum_image_eq` over the unified value enum (`datum_image_eq_v` seam).
/// Identical semantics to the byte-model [`datum_image_eq_bytes`].
pub fn datum_image_eq_v(
    value1: &Datum<'_>,
    value2: &Datum<'_>,
    typ_byval: bool,
    typ_len: i16,
) -> PgResult<bool> {
    datum_image_eq_bytes(value1, value2, typ_byval, typ_len as i32)
}

// ===========================================================================
// Seam installation.
//
// The byte-model lane and the migration-target `*_v` value-enum lane are the
// canonical contracts; the residual bare-word seams (`datum_copy`,
// `datum_serialize`, `datum_restore`, `datum_image_eq`) are installed only for
// the cross-crate consumers still on the machine-word edge (params.c, nbtree
// DSM cursor, misc2 rowtypes) and are removed once those migrate. The
// fully-superseded bare-word `datum_estimate_space` / `datum_image_hash` (their
// consumers moved to the `*_v` variants) are no longer installed here.
// ===========================================================================

/// Install datum.c's inward seams. Idempotent at the seam layer.
pub fn init_seams() {
    // Byte-model lane (brin-tuple).
    backend_utils_adt_scalar_seams::datum_copy::set(datum_copy);

    // Migration-target value-enum (`*_v`) lane.
    backend_utils_adt_datum_seams::datum_copy_v::set(datum_copy_v);
    backend_utils_adt_datum_seams::datum_estimate_space_v::set(datum_estimate_space_v);
    backend_utils_adt_datum_seams::datum_serialize_v::set(datum_serialize_v);
    backend_utils_adt_datum_seams::datum_image_hash_v::set(datum_image_hash_v);
    backend_utils_adt_datum_seams::datum_image_eq_v::set(datum_image_eq_v);

    // Residual bare-machine-word ABI edge: the last two transitional word seams,
    // the nbtree/params.c array (de)serialize over a `*mut u8` DSM cursor. Their
    // consumers (nbtree, params.c, execParallel via its own support seams) still
    // carry a `ScalarWord` through the cursor and have no `*_v` form yet; removed
    // when they migrate. The superseded bare-word `datum_copy` / `datum_image_eq`
    // / `datum_estimate_space` / `datum_image_hash` are NOT installed: their
    // consumers moved to the canonical byte-model `datum_copy` and the `*_v`
    // value-enum lane, so the forged `leak_bytes_as_datum` / `datum_copy_word` /
    // `datum_transfer` / `datum_image_eq_word` word-codec has been retired.
    backend_utils_adt_datum_seams::datum_serialize::set(datum_serialize);
    backend_utils_adt_datum_seams::datum_restore::set(datum_restore);
}

#[cfg(test)]
mod tests;
