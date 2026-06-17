//! `backend-access-common-heaptuple` — the in-memory tuple (de)serialization
//! core of `src/backend/access/common/heaptuple.c`.
//!
//! This crate ports the four routines that turn a `(values, isnull)` pair into
//! a heap tuple's on-disk data area and back:
//!
//! * [`heap_compute_data_size`] — size of the data area to be constructed;
//! * [`fill_val`] — per-attribute serializer (the `static inline fill_val`);
//! * [`heap_fill_tuple`] — fill the whole data area + null bitmap + infomask;
//! * [`heap_form_tuple`] — build the full on-disk tuple image;
//! * [`heap_deform_tuple`] — break a tuple's data area back into per-column
//!   `(value, isnull)` pairs.
//!
//! ## The byte model vs. C's pointer model
//!
//! In C a tuple is one contiguous `palloc`'d buffer (`HeapTupleHeaderData`
//! header, then optional null bitmap, then `MAXALIGN`-padded user data), and
//! `fill_val` / `heap_deform_tuple` are raw pointer arithmetic over it. A
//! by-reference attribute Datum is a bare `char *` into some other buffer.
//!
//! This safe port keeps the arithmetic identical but represents the user-data
//! area as a `Vec<u8>` / `&[u8]` (the same approach `backend-utils-adt-array`
//! takes for `ArrayType` buffers — see its `access`/`lowlevel` modules). A
//! per-attribute value that C would pass as a `Datum` is modelled by
//! [`Datum`]:
//!
//! * `ByVal(word)` — a pass-by-value scalar (`att->attbyval`), stored with
//!   `store_att_byval` / read with `fetch_att`;
//! * `ByRef(Vec<u8>)` — the *already-detoasted on-disk bytes* of a
//!   by-reference / varlena / cstring value. This is the faithful idiomatic
//!   stand-in for `DatumGetPointer(datum)`: C dereferences the pointer to read
//!   the varlena header + payload; here those bytes travel with the value.
//!
//! `ByRef` carries the verbatim datum bytes including any 1-byte ("short") or
//! 4-byte varlena header, exactly as C's `memcpy(data, val, VARSIZE*(val))`
//! sees them, so the short-varlena conversion path is reproduced byte-for-byte.
//!
//! ## What is out of scope (loud-panic)
//!
//! Two `fill_val` / `heap_compute_data_size` sub-cases reach into the
//! *expanded-TOAST-object* subsystem (`EOH_get_flat_size` / `EOH_flatten_into`,
//! `utils/adt/expandeddatum.c`). An expanded datum is a raw
//! `ExpandedObjectHeader *` encoded in the TOAST-pointer bytes; the safe byte
//! model cannot carry it (a `ByRef` blob of pointer bytes would be invented
//! opacity), so those branches panic loudly until the expanded-object owner
//! lands and contributes a real owned representation (e.g. a
//! `Datum::Expanded` variant defined with its types).
//! `heap_copy_tuple_as_datum` can reach `toast_flatten_tuple_to_datum`
//! (`access/heap/heaptoast.c`); that goes through the owner's seam crate
//! (`backend-access-heap-heaptoast-seams`). The common catalog/tuple path
//! (fixed-width by-value + plain varlena/cstring) needs none of them.

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod flat;

#[cfg(test)]
mod tests;

use mcx::{alloc_in, slice_in, vec_with_capacity_in, Mcx, PgVec};
use types_tuple::heap::SizeofHeapTupleHeader;
use types_tuple::heaptuple::{
    bits8, BlockIdData, CompactAttribute, DatumTupleFields, HeapTupleData, HeapTupleField3,
    HeapTupleFields, HeapTupleHeaderChoice, HeapTupleHeaderData, HeapTupleHeaderGetNatts,
    HeapTupleHeaderSetNatts, ItemPointerData,
    TupleConstr, TupleDescData, BITMAPLEN, HEAP_HASEXTERNAL, HEAP_HASNULL, HEAP_HASVARWIDTH, HIGHBIT,
    MaxTupleAttributeNumber,
};
use types_core::{Oid, Size};
use types_error::{PgError, PgResult};

// ---------------------------------------------------------------------------
// Per-attribute value model (the faithful idiomatic `Datum` substitute).
// ---------------------------------------------------------------------------

/// A single attribute's value handed to / produced by the tuple
/// (de)serializers, modelling C's per-attribute `Datum` over the safe byte
/// representation (see the module docs).
///
/// Defined in `types_tuple::backend_access_common_heaptuple` so seam signatures can
/// reference it; re-exported here as `crate::Datum`.
pub use types_tuple::backend_access_common_heaptuple::Datum;

// ---------------------------------------------------------------------------
// Alignment + varlena helpers (access/tupmacs.h, varatt.h, c.h), ported 1:1.
// ---------------------------------------------------------------------------

/// `MAXIMUM_ALIGNOF` on a standard 64-bit build.
const MAXIMUM_ALIGNOF: usize = 8;

/// `VARHDRSZ` (== `sizeof(int32)`).
const VARHDRSZ: usize = 4;
/// `VARHDRSZ_SHORT` (== `offsetof(varattrib_1b, va_data)` == 1).
const VARHDRSZ_SHORT: usize = 1;
/// `VARATT_SHORT_MAX` (varatt.h).
const VARATT_SHORT_MAX: usize = 0x7F;

/// `TYPEALIGN(ALIGNVAL, LEN)` (c.h).
#[inline]
fn type_align(alignval: usize, len: usize) -> usize {
    (len + (alignval - 1)) & !(alignval - 1)
}

/// `MAXALIGN(LEN)` (c.h).
#[inline]
fn maxalign(len: usize) -> usize {
    type_align(MAXIMUM_ALIGNOF, len)
}

/// `att_nominal_alignby(cur_offset, attalignby)` (tupmacs.h):
/// `TYPEALIGN(attalignby, cur_offset)`.
#[inline]
fn att_nominal_alignby(cur_offset: usize, attalignby: u8) -> usize {
    type_align(attalignby as usize, cur_offset)
}

/// `VARATT_IS_1B(PTR)` (varatt.h, little-endian): 1-byte ("short") header.
#[inline]
fn varatt_is_1b(b: &[u8]) -> bool {
    (b[0] & 0x01) == 0x01
}

/// `VARATT_IS_1B_E(PTR)` (varatt.h, little-endian): 1-byte TOAST pointer
/// (`va_header == 0x01`).
#[inline]
fn varatt_is_1b_e(b: &[u8]) -> bool {
    b[0] == 0x01
}

/// `VARATT_IS_4B_U(PTR)` (varatt.h, little-endian): uncompressed 4-byte header
/// (low two bits == 00).
#[inline]
fn varatt_is_4b_u(b: &[u8]) -> bool {
    (b[0] & 0x03) == 0x00
}

/// `VARATT_IS_EXTERNAL(PTR)` == `VARATT_IS_1B_E(PTR)`.
#[inline]
fn varatt_is_external(b: &[u8]) -> bool {
    varatt_is_1b_e(b)
}

/// `VARATT_IS_SHORT(PTR)` == `VARATT_IS_1B(PTR)`.
#[inline]
fn varatt_is_short(b: &[u8]) -> bool {
    varatt_is_1b(b)
}

/// `VARSIZE_1B(PTR)` (varatt.h, little-endian): `(va_header >> 1) & 0x7F`.
#[inline]
fn varsize_1b(b: &[u8]) -> usize {
    ((b[0] >> 1) & 0x7F) as usize
}

/// `VARSIZE_4B(PTR)` (varatt.h, little-endian): `(va_header >> 2) & 0x3FFFFFFF`.
#[inline]
fn varsize_4b(b: &[u8]) -> usize {
    let hdr = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
    ((hdr >> 2) & 0x3FFF_FFFF) as usize
}

/// `VARSIZE(PTR)` == `VARSIZE_4B(PTR)`.
#[inline]
fn varsize(b: &[u8]) -> usize {
    varsize_4b(b)
}

/// `VARTAG_SIZE(tag)` (varatt.h): payload size of a TOAST pointer of the given
/// `va_tag` — `sizeof(varatt_indirect)` (a `varlena *`, 8 bytes on 64-bit) for
/// `VARTAG_INDIRECT`, `sizeof(varatt_expanded)` (an `ExpandedObjectHeader *`,
/// 8 bytes) for the expanded tags, and `sizeof(varatt_external)` (4×4 = 16
/// bytes) for `VARTAG_ONDISK`.
#[inline]
fn vartag_size(tag: u8) -> usize {
    const VARTAG_INDIRECT: u8 = 1;
    const VARTAG_EXPANDED_RO: u8 = 2;
    const VARTAG_ONDISK: u8 = 18;
    if tag == VARTAG_INDIRECT {
        // sizeof(varatt_indirect) == sizeof(struct varlena *)
        8
    } else if (tag & !1) == VARTAG_EXPANDED_RO {
        // sizeof(varatt_expanded) == sizeof(ExpandedObjectHeader *)
        8
    } else if tag == VARTAG_ONDISK {
        // sizeof(varatt_external): va_rawsize + va_extinfo + va_valueid +
        // va_toastrelid, 4 bytes each.
        16
    } else {
        debug_assert!(false, "invalid varlena TOAST tag");
        0
    }
}

/// `VARSIZE_EXTERNAL(PTR)` (varatt.h): `VARHDRSZ_EXTERNAL + VARTAG_SIZE(tag)`,
/// `VARHDRSZ_EXTERNAL == 2`.
#[inline]
fn varsize_external(b: &[u8]) -> usize {
    2 + vartag_size(b[1])
}

/// `VARSIZE_ANY(ptr)` (varatt.h) for an in-line varlena starting at `b[0]`.
///
/// Also covers the exported C `varsize_any(void *p)` (heaptuple.c:1610), which
/// is a bare `VARSIZE_ANY` wrapper kept so JIT can inline the definition.
#[inline]
pub fn varsize_any(b: &[u8]) -> usize {
    if varatt_is_1b_e(b) {
        varsize_external(b)
    } else if varatt_is_1b(b) {
        varsize_1b(b)
    } else {
        varsize_4b(b)
    }
}

/// `VARATT_IS_EXTERNAL_EXPANDED(PTR)` (varatt.h): an external datum whose tag is
/// an expanded-object tag (`VARTAG_EXPANDED_RO`/`_RW`).
#[inline]
fn varatt_is_external_expanded(b: &[u8]) -> bool {
    const VARTAG_EXPANDED_RO: u8 = 2;
    varatt_is_external(b) && ((b[1] & !1) == VARTAG_EXPANDED_RO)
}

/// `VARATT_CAN_MAKE_SHORT(PTR)` (varatt.h):
/// `VARATT_IS_4B_U(PTR) && (VARSIZE(PTR) - VARHDRSZ + VARHDRSZ_SHORT) <= VARATT_SHORT_MAX`.
#[inline]
fn varatt_can_make_short(b: &[u8]) -> bool {
    varatt_is_4b_u(b) && (varsize(b) - VARHDRSZ + VARHDRSZ_SHORT) <= VARATT_SHORT_MAX
}

/// `VARATT_CONVERTED_SHORT_SIZE(PTR)` (varatt.h):
/// `VARSIZE(PTR) - VARHDRSZ + VARHDRSZ_SHORT`.
#[inline]
fn varatt_converted_short_size(b: &[u8]) -> usize {
    varsize(b) - VARHDRSZ + VARHDRSZ_SHORT
}

/// `SET_VARSIZE_SHORT(PTR, len)` (varatt.h, little-endian):
/// `va_header = (len) << 1 | 0x01`.
#[inline]
fn set_varsize_short(dest: &mut [u8], len: usize) {
    dest[0] = ((len as u8) << 1) | 0x01;
}

/// `VARDATA(PTR)` byte offset for a 4-byte-header varlena (== `VARHDRSZ`).
const VARDATA_4B_OFF: usize = VARHDRSZ;

// ---------------------------------------------------------------------------
// att_isnull (tupmacs.h)
// ---------------------------------------------------------------------------

/// `att_isnull(ATT, BITS)` (tupmacs.h): a 0 bit in the null bitmap means NULL.
#[inline]
fn att_isnull(att: usize, bits: &[bits8]) -> bool {
    (bits[att >> 3] & (1u8 << (att & 0x07))) == 0
}

// ---------------------------------------------------------------------------
// fetch_att / store_att_byval (tupmacs.h)
// ---------------------------------------------------------------------------

/// `fetch_att(T, attbyval=true, attlen)` for a by-value field at `src[off..]`.
/// (The by-reference case in C returns `PointerGetDatum(T)`; in the byte model
/// the caller takes the slice directly, so it is not represented here.)
///
/// Returns the raw machine word at the storage edge (C's bare `Datum`); the
/// caller wraps it into a [`Datum::ByVal`] via [`Datum::from_usize`].
#[inline]
fn fetch_att_byval(src: &[u8], off: usize, attlen: i16) -> usize {
    match attlen {
        1 => src[off] as i8 as i64 as usize,
        2 => i16::from_ne_bytes([src[off], src[off + 1]]) as i64 as usize,
        4 => i32::from_ne_bytes([src[off], src[off + 1], src[off + 2], src[off + 3]]) as i64
            as usize,
        8 => usize::from_ne_bytes([
            src[off],
            src[off + 1],
            src[off + 2],
            src[off + 3],
            src[off + 4],
            src[off + 5],
            src[off + 6],
            src[off + 7],
        ]),
        _ => {
            // C: elog(ERROR, "unsupported byval length: %d", attlen)
            panic!("unsupported byval length: {attlen}")
        }
    }
}

/// `store_att_byval(T=&dest[off..], newdatum, attlen)` (tupmacs.h). `newdatum`
/// is the raw machine word at the storage edge (C's bare `Datum`).
#[inline]
fn store_att_byval(dest: &mut [u8], off: usize, newdatum: usize, attlen: i16) {
    let word = newdatum as u64;
    match attlen {
        1 => dest[off] = word as u8,
        2 => dest[off..off + 2].copy_from_slice(&(word as u16).to_ne_bytes()),
        4 => dest[off..off + 4].copy_from_slice(&(word as u32).to_ne_bytes()),
        8 => dest[off..off + 8].copy_from_slice(&word.to_ne_bytes()),
        _ => panic!("unsupported byval length: {attlen}"),
    }
}

// ---------------------------------------------------------------------------
// heap_compute_data_size (heaptuple.c:219)
// ---------------------------------------------------------------------------

/// `heap_compute_data_size(tupleDesc, values, isnull)` — determine the size of
/// the data area of a tuple to be constructed.
///
/// `values[i]` is consulted only for non-null attributes (`isnull[i] == false`).
pub fn heap_compute_data_size(
    tuple_desc: &TupleDescData<'_>,
    values: &[Datum<'_>],
    isnull: &[bool],
) -> PgResult<Size> {
    let mut data_length: Size = 0;
    let number_of_attributes = tuple_desc.natts;

    for i in 0..number_of_attributes as usize {
        if isnull[i] {
            continue;
        }

        let val = &values[i];
        let atti = &tuple_desc.compact_attrs[i];

        // COMPACT_ATTR_IS_PACKABLE(atti) && VARATT_CAN_MAKE_SHORT(DatumGetPointer(val))
        if compact_attr_is_packable(atti) && varatt_can_make_short(val.as_ref_bytes()) {
            // we're anticipating converting to a short varlena header, so
            // adjust length and don't count any alignment
            data_length += varatt_converted_short_size(val.as_ref_bytes());
        } else if atti.attlen == -1 && varatt_is_external_expanded(val.as_ref_bytes()) {
            // we want to flatten the expanded value so that the constructed
            // tuple doesn't depend on it
            data_length = att_nominal_alignby(data_length, atti.attalignby);
            data_length += backend_utils_adt_misc2_seams::eoh_get_flat_size::call(
                types_datum::ExpandedObjectRef::from_expanded_datum_bytes(val.as_ref_bytes()),
            )?;
        } else {
            // att_datum_alignby(data_length, attalignby, attlen, val)
            data_length = att_datum_alignby(data_length, atti.attalignby, atti.attlen, val);
            // att_addlength_datum(data_length, attlen, val)
            data_length = att_addlength_datum(data_length, atti.attlen, val);
        }
    }

    Ok(data_length)
}

/// `COMPACT_ATTR_IS_PACKABLE(att)` (heaptuple.c:87):
/// `att->attlen == -1 && att->attispackable`.
#[inline]
fn compact_attr_is_packable(att: &CompactAttribute) -> bool {
    att.attlen == -1 && att.attispackable
}

/// `att_datum_alignby(cur_offset, attalignby, attlen, attdatum)` (tupmacs.h):
/// no alignment for a short varlena, else `TYPEALIGN(attalignby, cur_offset)`.
#[inline]
fn att_datum_alignby(cur_offset: usize, attalignby: u8, attlen: i16, val: &Datum) -> usize {
    if attlen == -1 && varatt_is_short(val.as_ref_bytes()) {
        cur_offset
    } else {
        att_nominal_alignby(cur_offset, attalignby)
    }
}

/// `att_addlength_datum(cur_offset, attlen, attdatum)` (tupmacs.h):
/// `att_addlength_pointer(cur_offset, attlen, DatumGetPointer(attdatum))`.
#[inline]
fn att_addlength_datum(cur_offset: usize, attlen: i16, val: &Datum) -> usize {
    if attlen > 0 {
        cur_offset + attlen as usize
    } else if attlen == -1 {
        cur_offset + varsize_any(val.as_ref_bytes())
    } else {
        debug_assert_eq!(attlen, -2);
        // strlen + 1
        let bytes = val.as_ref_bytes();
        let mut len = 0usize;
        while bytes[len] != 0 {
            len += 1;
        }
        cur_offset + len + 1
    }
}

// ---------------------------------------------------------------------------
// fill_val (heaptuple.c:274) + heap_fill_tuple (heaptuple.c:401)
// ---------------------------------------------------------------------------

/// State for the bit-walk shared by [`heap_fill_tuple`]'s `fill_val` loop,
/// matching C's `bits8 **bit` / `int *bitmask` in/out parameters.
struct BitWalk {
    /// Index of the current null-bitmap byte (C's `*bit`, relative to the start
    /// of the `bits` buffer). Starts at `usize::MAX` to model C's `&bit[-1]`:
    /// the first `*bitmask == HIGHBIT` branch increments it to 0.
    byte: usize,
    bitmask: i32,
}

/// `fill_val(att, &bit, &bitmask, &dataP, &infomask, datum, isnull)`
/// (heaptuple.c:274) — fill in either a data value or a bit in the null mask.
///
/// `data` is the user-data area; `*data_off` is the current write cursor within
/// it (C's `char **dataP`). `bits` is the null bitmap (`None` ⇒ not building
/// one); `walk` carries the bit cursor. The expanded-object flatten branch
/// panics loudly (see the module docs).
#[allow(clippy::too_many_arguments)]
fn fill_val(
    att: &CompactAttribute,
    bits: Option<&mut [bits8]>,
    walk: &mut BitWalk,
    data: &mut [u8],
    data_off: &mut usize,
    infomask: &mut u16,
    datum: &Datum,
    isnull: bool,
) -> PgResult<()> {
    let mut off = *data_off;

    // If we're building a null bitmap, set the appropriate bit here.
    if let Some(bits) = bits {
        if walk.bitmask != HIGHBIT {
            walk.bitmask <<= 1;
        } else {
            // *bit += 1; **bit = 0x0; *bitmask = 1;
            walk.byte = walk.byte.wrapping_add(1);
            bits[walk.byte] = 0x0;
            walk.bitmask = 1;
        }

        if isnull {
            *infomask |= HEAP_HASNULL;
            return Ok(());
        }

        // **bit |= *bitmask;
        bits[walk.byte] |= walk.bitmask as u8;
    }

    let data_length: usize;

    if att.attbyval {
        // pass-by-value
        off = att_nominal_alignby(off, att.attalignby);
        store_att_byval(data, off, byval_datum(datum), att.attlen);
        data_length = att.attlen as usize;
    } else if att.attlen == -1 {
        // varlena
        let val = datum.as_ref_bytes();
        *infomask |= HEAP_HASVARWIDTH;
        if varatt_is_external(val) {
            if varatt_is_external_expanded(val) {
                // flatten the expanded value so the tuple doesn't depend on it
                off = att_nominal_alignby(off, att.attalignby);
                let eoh = types_datum::ExpandedObjectRef::from_expanded_datum_bytes(val);
                data_length = backend_utils_adt_misc2_seams::eoh_get_flat_size::call(eoh)?;
                backend_utils_adt_misc2_seams::eoh_flatten_into::call(
                    eoh,
                    &mut data[off..off + data_length],
                )?;
            } else {
                *infomask |= HEAP_HASEXTERNAL;
                // no alignment, since it's short by definition
                data_length = varsize_external(val);
                data[off..off + data_length].copy_from_slice(&val[..data_length]);
            }
        } else if varatt_is_short(val) {
            // no alignment for short varlenas
            data_length = varsize_short(val);
            data[off..off + data_length].copy_from_slice(&val[..data_length]);
        } else if att.attispackable && varatt_can_make_short(val) {
            // convert to short varlena -- no alignment
            data_length = varatt_converted_short_size(val);
            set_varsize_short(&mut data[off..], data_length);
            // memcpy(data + 1, VARDATA(val), data_length - 1)
            data[off + 1..off + data_length]
                .copy_from_slice(&val[VARDATA_4B_OFF..VARDATA_4B_OFF + (data_length - 1)]);
        } else {
            // full 4-byte header varlena
            off = att_nominal_alignby(off, att.attalignby);
            data_length = varsize(val);
            data[off..off + data_length].copy_from_slice(&val[..data_length]);
        }
    } else if att.attlen == -2 {
        // cstring ... never needs alignment
        *infomask |= HEAP_HASVARWIDTH;
        debug_assert_eq!(att.attalignby, 1);
        let val = datum.as_ref_bytes();
        // strlen(DatumGetCString(datum)) + 1
        let mut slen = 0usize;
        while val[slen] != 0 {
            slen += 1;
        }
        data_length = slen + 1;
        data[off..off + data_length].copy_from_slice(&val[..data_length]);
    } else {
        // fixed-length pass-by-reference
        off = att_nominal_alignby(off, att.attalignby);
        debug_assert!(att.attlen > 0);
        data_length = att.attlen as usize;
        let val = datum.as_ref_bytes();
        data[off..off + data_length].copy_from_slice(&val[..data_length]);
    }

    off += data_length;
    *data_off = off;
    Ok(())
}

/// `VARSIZE_SHORT(PTR)` == `VARSIZE_1B(PTR)`.
#[inline]
fn varsize_short(b: &[u8]) -> usize {
    varsize_1b(b)
}

/// The pass-by-value machine word for a [`Datum`] (C's `datum` in the byval
/// arm). A by-value attribute must carry a `ByVal`; a `ByRef` here is a caller
/// type error (the mirror of [`Datum::as_ref_bytes`]'s panic on `ByVal`) —
/// never silently reassemble a Datum word from bytes. The raw `usize` is the
/// storage-edge ABI value handed to [`store_att_byval`].
#[inline]
fn byval_datum(datum: &Datum) -> usize {
    match datum {
        Datum::ByVal(_) => datum.as_usize(),
        Datum::ByRef(_) => {
            panic!("byval_datum: by-value attribute handed a Datum::ByRef")
        }
        Datum::Cstring(_) | Datum::Composite(_) | Datum::Expanded(_) | Datum::Internal(_) => {
            panic!("byval_datum: by-value attribute handed a non-flat Datum (Cstring/Composite/Expanded/Internal) — not yet produced, wave 2")
        }
    }
}

/// Result of [`heap_fill_tuple`]: the serialized user-data area, the computed
/// `t_infomask`, and the null bitmap bytes (empty when no nulls are present).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilledData<'mcx> {
    /// The `data_size` bytes of the user-data area.
    pub data: PgVec<'mcx, u8>,
    /// The `t_infomask` bits implied by the data (HASNULL/HASVARWIDTH/HASEXTERNAL).
    pub infomask: u16,
    /// The null bitmap (`BITMAPLEN(natts)` bytes), or empty when `bits` is `None`.
    pub bits: PgVec<'mcx, bits8>,
}

/// `heap_fill_tuple(tupleDesc, values, isnull, data, data_size, &infomask, bit)`
/// (heaptuple.c:401) — load the data portion of a tuple from the values/isnull
/// arrays, also filling the null bitmap (when `with_bitmap`) and the infomask.
///
/// Returns the filled [`FilledData`], allocated in `mcx` (in C the caller's
/// `palloc0` block). `data_size` is the [`heap_compute_data_size`] result; the
/// returned `data` is exactly that long. `with_bitmap` corresponds to C's
/// `bit != NULL` (i.e. `hasnull`).
pub fn heap_fill_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple_desc: &TupleDescData<'_>,
    values: &[Datum<'_>],
    isnull: &[bool],
    data_size: Size,
    with_bitmap: bool,
) -> PgResult<FilledData<'mcx>> {
    let number_of_attributes = tuple_desc.natts as usize;
    let mut data = vec_with_capacity_in(mcx, data_size)?;
    data.resize(data_size, 0);
    let mut infomask: u16 = 0;

    // C zeroes the infomask flag bits it owns: *infomask &= ~(HASNULL|HASVARWIDTH|HASEXTERNAL).
    // We start from 0, which is equivalent for these three bits.

    let mut bits: PgVec<'mcx, bits8> = if with_bitmap {
        let len = BITMAPLEN(tuple_desc.natts) as usize;
        let mut b = vec_with_capacity_in(mcx, len)?;
        b.resize(len, 0);
        b
    } else {
        PgVec::new_in(mcx)
    };

    // bitP = &bit[-1]; bitmask = HIGHBIT;  (model &bit[-1] with byte = usize::MAX)
    let mut walk = BitWalk {
        byte: usize::MAX,
        bitmask: HIGHBIT,
    };

    // C's `values ? values[i] : PointerGetDatum(NULL)` is an all-or-nothing
    // NULL-array contract; slices model the non-NULL case, so a short array is
    // a caller bug — index directly and panic rather than fabricate NULLs.
    let mut data_off = 0usize;
    for i in 0..number_of_attributes {
        let attr = &tuple_desc.compact_attrs[i];
        let datum = &values[i];
        let this_isnull = isnull[i];

        if with_bitmap {
            fill_val(
                attr,
                Some(&mut bits),
                &mut walk,
                &mut data,
                &mut data_off,
                &mut infomask,
                datum,
                this_isnull,
            )?;
        } else {
            fill_val(
                attr,
                None,
                &mut walk,
                &mut data,
                &mut data_off,
                &mut infomask,
                datum,
                this_isnull,
            )?;
        }
    }

    debug_assert_eq!(data_off, data_size);

    Ok(FilledData {
        data,
        infomask,
        bits,
    })
}

// ---------------------------------------------------------------------------
// heap_form_tuple (heaptuple.c:1116)
// ---------------------------------------------------------------------------

/// `InvalidOid`.
const INVALID_OID: Oid = 0;

/// A fully-formed heap tuple: the owned [`HeapTupleData`] plus its user-data
/// area bytes (`td + t_hoff .. td + t_len`).
///
/// In C the header, optional null bitmap, and user data are one contiguous
/// `palloc` chunk; here the header (incl. its `t_bits` null bitmap) lives in the
/// owned `HeapTupleHeaderData` and the user-data area travels alongside as
/// [`FormedTuple::data`]. `tuple.t_len` is the full on-disk length
/// (`t_hoff + data.len()`), matching C.
///
/// Defined in `types_tuple::backend_access_common_heaptuple` so seam signatures can
/// reference it; re-exported here as `crate::FormedTuple`.
pub use types_tuple::backend_access_common_heaptuple::FormedTuple;

/// `heap_form_tuple(tupleDescriptor, values, isnull)` (heaptuple.c:1116) —
/// construct a tuple from the given `values`/`isnull` arrays (one entry per
/// `tupleDescriptor.natts`), allocated in `mcx` (C: one `palloc` block in
/// `CurrentMemoryContext`).
///
/// Returns the formed tuple, or [`HeapTupleError::TooManyColumns`] when the
/// descriptor has more than `MaxTupleAttributeNumber` columns (C: `ereport
/// (ERROR, ERRCODE_TOO_MANY_COLUMNS)`).
pub fn heap_form_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple_descriptor: &TupleDescData<'_>,
    values: &[Datum<'_>],
    isnull: &[bool],
) -> Result<FormedTuple<'mcx>, HeapTupleError> {
    let number_of_attributes = tuple_descriptor.natts;

    if number_of_attributes > MaxTupleAttributeNumber {
        return Err(HeapTupleError::TooManyColumns {
            columns: number_of_attributes,
            limit: MaxTupleAttributeNumber,
        });
    }

    // Check for nulls.
    let mut hasnull = false;
    for i in 0..number_of_attributes as usize {
        if isnull[i] {
            hasnull = true;
            break;
        }
    }

    // Determine total space needed.
    // len = offsetof(HeapTupleHeaderData, t_bits)
    let mut len = SizeofHeapTupleHeader;

    if hasnull {
        len += BITMAPLEN(number_of_attributes) as usize;
    }

    // hoff = len = MAXALIGN(len);  /* align user data safely */
    len = maxalign(len);
    let hoff = len;

    let data_len = heap_compute_data_size(tuple_descriptor, values, isnull)?;

    len += data_len;

    // Fill the data area + null bitmap + infomask.
    let filled = heap_fill_tuple(mcx, tuple_descriptor, values, isnull, data_len, hasnull)?;

    // Build the owned header.  ItemPointerSetInvalid sets blockid = (0xffff,
    // 0xffff) and posid = 0 (== InvalidOffsetNumber).
    let invalid_ctid = ItemPointerData {
        ip_blkid: BlockIdData {
            bi_hi: 0xffff,
            bi_lo: 0xffff,
        },
        ip_posid: 0,
    };

    let mut td = HeapTupleHeaderData {
        // HeapTupleHeaderSetDatumLength/TypeId/TypMod fill the t_datum fields so
        // HeapTupleHeaderGetDatum can identify the tuple type if needed.
        t_choice: HeapTupleHeaderChoice::TDatum(DatumTupleFields {
            datum_len_: len as i32,
            datum_typmod: tuple_descriptor.tdtypmod,
            datum_typeid: tuple_descriptor.tdtypeid,
        }),
        // We also make sure t_ctid is invalid unless explicitly set.
        t_ctid: invalid_ctid,
        t_infomask2: 0,
        t_infomask: filled.infomask,
        t_hoff: hoff as u8,
        t_bits: filled.bits,
    };

    // HeapTupleHeaderSetNatts(td, numberOfAttributes)
    HeapTupleHeaderSetNatts(&mut td, number_of_attributes as u16);

    let tuple = alloc_in(
        mcx,
        HeapTupleData {
            t_len: len as u32,
            // ItemPointerSetInvalid(&tuple->t_self)
            t_self: invalid_ctid,
            t_tableOid: INVALID_OID,
            t_data: Some(alloc_in(mcx, td)?),
        },
    )?;

    Ok(FormedTuple {
        tuple,
        data: filled.data,
    })
}

// ---------------------------------------------------------------------------
// heap_deform_tuple (heaptuple.c:1345)
// ---------------------------------------------------------------------------

/// One column produced by [`heap_deform_tuple`]: a `(value, isnull)` pair.
///
/// `value` for a by-value column is the scalar word (`ByVal`); for a
/// by-reference column it is the column's on-disk bytes copied out of the data
/// area (`ByRef`) — the faithful idiomatic stand-in for C's bare pointer into
/// the tuple (the C contract that the pointer "points into the given tuple" is
/// preserved by copying the exact bytes spanned by the field).
///
/// Defined in `types_tuple::backend_access_common_heaptuple` so seam signatures can
/// reference it; re-exported here as `crate::DeformedColumn`.
pub use types_tuple::backend_access_common_heaptuple::DeformedColumn;

/// `heap_deform_tuple(tuple, tupleDesc, values, isnull)` (heaptuple.c:1345) —
/// given a tuple, extract data into `values`/`isnull` arrays.
///
/// `data` is the tuple's user-data area (the bytes at `td + t_hoff`, i.e.
/// [`FormedTuple::data`]). Returns one [`DeformedColumn`] per `tupleDesc`
/// attribute, with the by-reference column byte copies (and the output array
/// itself, C's caller-palloc'd `values`/`isnull`) allocated in `mcx`. Columns
/// beyond the tuple's stored natts are read as missing values (or NULL) via
/// [`getmissingattr`].
///
/// NB: this mirrors C's caching of `attcacheoff`, but because the descriptor is
/// borrowed immutably here the cache writes are omitted (they are a pure
/// performance optimization; the computed offsets are identical).
pub fn heap_deform_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &HeapTupleData<'_>,
    tuple_desc: &TupleDescData<'_>,
    data: &[u8],
) -> PgResult<PgVec<'mcx, DeformedColumn<'mcx>>> {
    let tup = tuple
        .t_data
        .as_ref()
        .expect("heap_deform_tuple: tuple has no t_data");
    let hasnulls = (tup.t_infomask & HEAP_HASNULL) != 0;
    let tdesc_natts = tuple_desc.natts;

    // natts = HeapTupleHeaderGetNatts(tup); clamped to tdesc_natts.
    let mut natts = HeapTupleHeaderGetNatts(tup) as i32;
    if natts > tdesc_natts {
        natts = tdesc_natts;
    }

    let bp = &tup.t_bits; // ptr to null bitmap in tuple
    let mut out: PgVec<'mcx, DeformedColumn<'mcx>> =
        vec_with_capacity_in(mcx, tdesc_natts as usize)?;

    // tp = (char *) tup + tup->t_hoff;  ==> the `data` slice (data area).
    let mut off: usize = 0;
    let mut slow = false; // can we use/set attcacheoff?

    let mut attnum = 0i32;
    while attnum < natts {
        let thisatt = &tuple_desc.compact_attrs[attnum as usize];

        if hasnulls && att_isnull(attnum as usize, bp) {
            out.push((Datum::null(), true));
            slow = true; // can't use attcacheoff anymore
            attnum += 1;
            continue;
        }

        // isnull[attnum] = false;  (recorded via the pushed pair below)

        if !slow && thisatt.attcacheoff >= 0 {
            off = thisatt.attcacheoff as usize;
        } else if thisatt.attlen == -1 {
            // varlena: cache the offset only if already suitably aligned.
            if !slow && off == att_nominal_alignby(off, thisatt.attalignby) {
                // C sets thisatt->attcacheoff = off here (cache write omitted;
                // descriptor borrowed immutably — offsets are unaffected).
            } else {
                off = att_pointer_alignby(off, thisatt.attalignby, -1, data, off);
                slow = true;
            }
        } else {
            // not varlena, so safe to use att_nominal_alignby
            off = att_nominal_alignby(off, thisatt.attalignby);
            // if (!slow) thisatt->attcacheoff = off;  (cache write omitted)
        }

        // values[attnum] = fetchatt(thisatt, tp + off);
        let value = fetchatt(mcx, thisatt, data, off)?;
        out.push((value, false));

        // off = att_addlength_pointer(off, thisatt->attlen, tp + off);
        off = att_addlength_pointer(off, thisatt.attlen, data, off);

        if thisatt.attlen <= 0 {
            slow = true; // can't use attcacheoff anymore
        }

        attnum += 1;
    }

    // Read the rest as nulls or missing values as appropriate.
    while attnum < tdesc_natts {
        let (val, isnull) = getmissingattr(mcx, tuple_desc, attnum + 1)?;
        out.push((val, isnull));
        attnum += 1;
    }

    Ok(out)
}

/// `fetchatt(A, T)` (tupmacs.h): for a by-value att read the scalar from
/// `data[off..]`; for a by-reference att return its on-disk bytes (C returns a
/// pointer into the tuple — here we copy the exact field span into `mcx`).
#[inline]
fn fetchatt<'mcx>(
    mcx: Mcx<'mcx>,
    att: &CompactAttribute,
    data: &[u8],
    off: usize,
) -> PgResult<Datum<'mcx>> {
    if att.attbyval {
        Ok(Datum::from_usize(fetch_att_byval(data, off, att.attlen)))
    } else {
        let end = att_addlength_pointer(off, att.attlen, data, off);
        Ok(Datum::ByRef(slice_in(mcx, &data[off..end])?))
    }
}

/// `att_pointer_alignby(cur_offset, attalignby, attlen, attptr)` (tupmacs.h):
/// no alignment when a varlena field's first byte is not a pad byte (a 1-byte
/// header or a non-zero leading byte of an aligned 4-byte word), else align.
///
/// `VARATT_NOT_PAD_BYTE(ptr)` is `*(ptr) != 0` (varatt.h).
#[inline]
fn att_pointer_alignby(cur_offset: usize, attalignby: u8, attlen: i16, data: &[u8], off: usize) -> usize {
    if attlen == -1 && data[off] != 0 {
        cur_offset
    } else {
        att_nominal_alignby(cur_offset, attalignby)
    }
}

/// `att_addlength_pointer(cur_offset, attlen, attptr=&data[off..])` (tupmacs.h).
#[inline]
fn att_addlength_pointer(cur_offset: usize, attlen: i16, data: &[u8], off: usize) -> usize {
    if attlen > 0 {
        cur_offset + attlen as usize
    } else if attlen == -1 {
        cur_offset + varsize_any(&data[off..])
    } else {
        debug_assert_eq!(attlen, -2);
        let mut len = 0usize;
        while data[off + len] != 0 {
            len += 1;
        }
        cur_offset + len + 1
    }
}

// ---------------------------------------------------------------------------
// getmissingattr (heaptuple.c:150)
// ---------------------------------------------------------------------------

/// `getmissingattr(tupleDesc, attnum, &isnull)` (heaptuple.c:150) — return the
/// missing value of an attribute (`(value, false)`), or `(NULL, true)` when
/// there is none.
///
/// `attnum` is 1-based, as in C.
///
/// A by-value missing value is returned directly, exactly as C's `attbyval`
/// fast path does. For a pass-by-reference missing value C consults its
/// file-static missing-values cache (`missing_hash` / `missing_match` /
/// `init_missing_cache` + `datumCopy` into `TopMemoryContext`,
/// heaptuple.c:96-215) — machinery whose sole purpose is giving the returned
/// pointer Datum a lifetime that survives tupleDesc destruction. In the owned
/// model the value *is* its bytes (`AttrMissing.am_value` is a
/// [`Datum`]), so the cache dissolves (`docs/mctx-design.md`): the bytes
/// are copied into the caller's `mcx` (the `datumCopy`), and lifetime safety
/// is the `'mcx` bound. Fallible: the copy allocates.
pub fn getmissingattr<'mcx>(
    mcx: Mcx<'mcx>,
    tuple_desc: &TupleDescData<'_>,
    attnum: i32,
) -> PgResult<DeformedColumn<'mcx>> {
    debug_assert!(attnum <= tuple_desc.natts);
    debug_assert!(attnum > 0);

    let att = &tuple_desc.compact_attrs[(attnum - 1) as usize];

    if att.atthasmissing {
        // Assert(tupleDesc->constr); Assert(tupleDesc->constr->missing);
        debug_assert!(
            constr_missing(tuple_desc).is_some(),
            "getmissingattr: atthasmissing set but tupleDesc has no missing-values array"
        );
        if let Some(constr) = constr_missing(tuple_desc) {
            let attrmiss = &constr[(attnum - 1) as usize];
            if attrmiss.am_present {
                // Assert(att->attlen > 0 || att->attlen == -1); (the C cache
                // only handles fixed-length and varlena by-ref values)
                debug_assert!(att.attbyval || att.attlen > 0 || att.attlen == -1);
                // *isnull = false; return the missing value (by-ref: the
                // datumCopy into the caller's context).
                return Ok((missing_value(attrmiss, att).clone_in(mcx)?, false));
            }
        }
    }

    // *isnull = true; return PointerGetDatum(NULL);
    Ok((Datum::null(), true))
}

/// Borrow `tupleDesc->constr->missing` (the `AttrMissing[]` array), if present.
#[inline]
fn constr_missing<'a, 'mcx>(
    tuple_desc: &'a TupleDescData<'mcx>,
) -> Option<&'a [types_tuple::heaptuple::AttrMissing<'mcx>]> {
    let constr: &TupleConstr = tuple_desc.constr.as_deref()?;
    if constr.missing.is_empty() {
        None
    } else {
        Some(&constr.missing)
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors raised by the tuple constructors (the `ereport(ERROR, ...)` sites in
/// heaptuple.c that the form/deform core can hit).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeapTupleError {
    /// `ERRCODE_TOO_MANY_COLUMNS`: `number of columns (%d) exceeds limit (%d)`.
    TooManyColumns { columns: i32, limit: i32 },
    /// `elog(ERROR, "invalid column number %d", attnum)` —
    /// `heap_modify_tuple_by_cols` got a target column outside `1..=natts`.
    InvalidColumnNumber { attnum: i32 },
    /// An `ereport(ERROR)` propagated from a callee (e.g. the expanded-object
    /// flatten path reached from `heap_compute_data_size` / `fill_val`).
    Pg(PgError),
}

impl From<PgError> for HeapTupleError {
    fn from(err: PgError) -> Self {
        HeapTupleError::Pg(err)
    }
}

/// The `PgError` C raises at the same site, so every consumer that surfaces a
/// [`HeapTupleError`] as an `ereport(ERROR)` maps it identically.
impl From<HeapTupleError> for PgError {
    fn from(err: HeapTupleError) -> Self {
        match err {
            // ereport(ERROR, errcode(ERRCODE_TOO_MANY_COLUMNS),
            //   errmsg("number of columns (%d) exceeds limit (%d)", ...))
            HeapTupleError::TooManyColumns { columns, limit } => PgError::error(alloc::format!(
                "number of columns ({columns}) exceeds limit ({limit})"
            ))
            .with_sqlstate(types_error::ERRCODE_TOO_MANY_COLUMNS),
            // elog(ERROR, "invalid column number %d", attnum): internal error.
            HeapTupleError::InvalidColumnNumber { attnum } => {
                PgError::error(alloc::format!("invalid column number {attnum}"))
            }
            HeapTupleError::Pg(e) => e,
        }
    }
}

// ===========================================================================
// modify / copy / free / form-minimal (heaptuple.c)
//
// These build on the in-crate form/deform core above (heap_form_tuple /
// heap_deform_tuple / heap_compute_data_size / heap_fill_tuple), so they need
// no new external substrate. The byte-area-travels-alongside model of
// `FormedTuple` carries over to the minimal-tuple result `FormedMinimalTuple`.
// ===========================================================================

/// `SizeofMinimalTupleHeader` == `offsetof(MinimalTupleData, t_bits)`
/// (`access/htup_details.h:704`). On the canonical 64-bit catalog ABI this is
/// `SizeofHeapTupleHeader - MINIMAL_TUPLE_OFFSET` (== `23 - 8 == 15`): a
/// `MinimalTupleData` drops `MINIMAL_TUPLE_OFFSET` bytes off the front of a
/// `HeapTupleHeaderData` and shares the `t_infomask2 .. t_bits` tail.
const SIZEOF_MINIMAL_TUPLE_HEADER: usize =
    types_tuple::heap::SizeofHeapTupleHeader - types_tuple::heaptuple::MINIMAL_TUPLE_OFFSET;
const _: () = assert!(SIZEOF_MINIMAL_TUPLE_HEADER == 15);

// ---------------------------------------------------------------------------
// heap_copytuple (heaptuple.c:777) / heap_copytuple_with_tuple (heaptuple.c:803)
// ---------------------------------------------------------------------------

/// `heap_copytuple(tuple)` (heaptuple.c:777) — return a copy of an entire tuple.
///
/// In C the `HeapTuple` struct, tuple header, and tuple data are one `palloc`
/// block, and the copy is a single `memcpy` of `tuple->t_len` bytes after a
/// `HEAPTUPLESIZE`-byte management struct. In the owned model the management
/// struct (`HeapTupleData`) and its header (`HeapTupleHeaderData`, including its
/// `t_bits` null bitmap) are deep-cloned, and the user-data area (the bytes at
/// `td + t_hoff`) travels alongside as [`FormedTuple::data`] and is cloned too —
/// the faithful equivalent of the contiguous-block `memcpy`.
///
/// C returns `NULL` when `tuple` is invalid or has a NULL `t_data`; here that is
/// `None`. The copy is allocated in `mcx` (C: `palloc` in
/// `CurrentMemoryContext`), so it is fallible (OOM).
pub fn heap_copytuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: Option<&FormedTuple<'_>>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    // if (!HeapTupleIsValid(tuple) || tuple->t_data == NULL) return NULL;
    let Some(tuple) = tuple else { return Ok(None) };
    if tuple.tuple.t_data.is_none() {
        return Ok(None);
    }

    // newTuple = palloc(HEAPTUPLESIZE + tuple->t_len); copy t_len/t_self/
    // t_tableOid; newTuple->t_data = newTuple + HEAPTUPLESIZE;
    // memcpy(newTuple->t_data, tuple->t_data, tuple->t_len).
    Ok(Some(tuple.clone_in(mcx)?))
}

/// `heap_copytuple_with_tuple(src, dest)` (heaptuple.c:803) — copy a tuple into a
/// caller-supplied management struct.
///
/// Unlike [`heap_copytuple`], C does *not* allocate `dest` as a single block:
/// `dest->t_data` is its own `palloc(src->t_len)`. In the owned model `dest` is
/// the returned [`FormedTuple`] whose `t_data` (and user data) are freshly
/// cloned from `src`. When `src` is invalid / has a NULL `t_data`, C sets
/// `dest->t_data = NULL`; here that is a `dest` with `t_data == None` and an
/// empty data area.
pub fn heap_copytuple_with_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    src: Option<&FormedTuple<'_>>,
) -> PgResult<FormedTuple<'mcx>> {
    match src {
        // dest->t_len = src->t_len; dest->t_self = src->t_self;
        // dest->t_tableOid = src->t_tableOid;
        // dest->t_data = palloc(src->t_len); memcpy(dest->t_data, src->t_data, src->t_len);
        Some(src) if src.tuple.t_data.is_some() => src.clone_in(mcx),
        _ => Ok(FormedTuple {
            // dest->t_data = NULL; (the rest of *dest is left as the caller had it).
            tuple: alloc_in(
                mcx,
                types_tuple::heaptuple::HeapTupleData {
                    t_len: 0,
                    t_self: invalid_item_pointer(),
                    t_tableOid: INVALID_OID,
                    t_data: None,
                },
            )?,
            data: PgVec::new_in(mcx),
        }),
    }
}

// ---------------------------------------------------------------------------
// heap_freetuple (heaptuple.c:1434)
// ---------------------------------------------------------------------------

/// `heap_freetuple(htup)` (heaptuple.c:1434) — free a tuple.
///
/// C is a bare `pfree(htup)` releasing the single contiguous block (management
/// struct + header + data). In the owned model freeing is dropping: taking the
/// [`FormedTuple`] by value consumes it, and its `Box<HeapTupleData>` (and the
/// data `Vec`) are released when this function returns — the faithful
/// equivalent of the `pfree`.
#[inline]
pub fn heap_freetuple(htup: FormedTuple<'_>) {
    drop(htup);
}

// ---------------------------------------------------------------------------
// heap_modify_tuple (heaptuple.c:1209)
// ---------------------------------------------------------------------------

/// `heap_modify_tuple(tuple, tupleDesc, replValues, replIsnull, doReplace)`
/// (heaptuple.c:1209) — form a new tuple from an old tuple and a set of
/// replacement values.
///
/// The new tuple takes `replValues[i]`/`replIsnull[i]` wherever `doReplace[i]`
/// is true and the old tuple's value otherwise. The three arrays are each of
/// length `tupleDesc.natts`.
///
/// Mirrors C exactly: `heap_deform_tuple` the old tuple, overlay the replaced
/// columns, `heap_form_tuple` the result, then copy the old tuple's identity
/// (`t_ctid`, `t_self`, `t_tableOid`). C's intermediate `palloc`/`pfree` of the
/// `values`/`isnull` scratch arrays is the owned `Vec`s built and dropped here.
pub fn heap_modify_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &FormedTuple<'_>,
    tuple_desc: &TupleDescData<'_>,
    repl_values: &[Datum<'_>],
    repl_isnull: &[bool],
    do_replace: &[bool],
) -> Result<FormedTuple<'mcx>, HeapTupleError> {
    let number_of_attributes = tuple_desc.natts as usize;

    // values = palloc(natts * sizeof(Datum)); isnull = palloc(natts * sizeof(bool));
    // heap_deform_tuple(tuple, tupleDesc, values, isnull);
    let deformed = heap_deform_tuple(mcx, &tuple.tuple, tuple_desc, &tuple.data)?;
    let mut values: PgVec<'mcx, Datum<'mcx>> =
        vec_with_capacity_in(mcx, number_of_attributes)?;
    let mut isnull: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, number_of_attributes)?;
    for (val, null) in deformed {
        values.push(val);
        isnull.push(null);
    }

    // for (attoff ...) if (doReplace[attoff]) { values[..]=replValues[..]; isnull[..]=replIsnull[..]; }
    for attoff in 0..number_of_attributes {
        if do_replace[attoff] {
            values[attoff] = repl_values[attoff].clone_in(mcx)?;
            isnull[attoff] = repl_isnull[attoff];
        }
    }

    // newTuple = heap_form_tuple(tupleDesc, values, isnull);
    let mut new_tuple = heap_form_tuple(mcx, tuple_desc, &values, &isnull)?;

    // copy the identification info of the old tuple: t_ctid, t_self, t_tableOid.
    copy_tuple_identity(&mut new_tuple, tuple);

    Ok(new_tuple)
}

// ---------------------------------------------------------------------------
// heap_modify_tuple_by_cols (heaptuple.c:1277)
// ---------------------------------------------------------------------------

/// `heap_modify_tuple_by_cols(tuple, tupleDesc, nCols, replCols, replValues,
/// replIsnull)` (heaptuple.c:1277) — like [`heap_modify_tuple`], but the columns
/// to replace are given as an array of 1-based target column numbers
/// (`replCols`) rather than a boolean map. `replCols`, `replValues`, and
/// `replIsnull` are each of length `n_cols`.
///
/// Returns [`HeapTupleError::InvalidColumnNumber`] for a `replCols[i]` outside
/// `1..=natts` (C: `elog(ERROR, "invalid column number %d", attnum)`).
pub fn heap_modify_tuple_by_cols<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &FormedTuple<'_>,
    tuple_desc: &TupleDescData<'_>,
    n_cols: i32,
    repl_cols: &[i32],
    repl_values: &[Datum<'_>],
    repl_isnull: &[bool],
) -> Result<FormedTuple<'mcx>, HeapTupleError> {
    let number_of_attributes = tuple_desc.natts;

    // values = palloc(natts * sizeof(Datum)); isnull = palloc(natts * sizeof(bool));
    // heap_deform_tuple(tuple, tupleDesc, values, isnull);
    let deformed = heap_deform_tuple(mcx, &tuple.tuple, tuple_desc, &tuple.data)?;
    let mut values: PgVec<'mcx, Datum<'mcx>> =
        vec_with_capacity_in(mcx, number_of_attributes as usize)?;
    let mut isnull: PgVec<'mcx, bool> =
        vec_with_capacity_in(mcx, number_of_attributes as usize)?;
    for (val, null) in deformed {
        values.push(val);
        isnull.push(null);
    }

    // for (i = 0; i < nCols; i++) { attnum = replCols[i]; check; values[attnum-1]=...; }
    for i in 0..n_cols as usize {
        let attnum = repl_cols[i];

        if attnum <= 0 || attnum > number_of_attributes {
            return Err(HeapTupleError::InvalidColumnNumber { attnum });
        }
        values[(attnum - 1) as usize] = repl_values[i].clone_in(mcx)?;
        isnull[(attnum - 1) as usize] = repl_isnull[i];
    }

    // newTuple = heap_form_tuple(tupleDesc, values, isnull);
    let mut new_tuple = heap_form_tuple(mcx, tuple_desc, &values, &isnull)?;

    // copy the identification info of the old tuple: t_ctid, t_self, t_tableOid.
    copy_tuple_identity(&mut new_tuple, tuple);

    Ok(new_tuple)
}

/// `newTuple->t_data->t_ctid = tuple->t_data->t_ctid; newTuple->t_self =
/// tuple->t_self; newTuple->t_tableOid = tuple->t_tableOid;` — the identity copy
/// shared by [`heap_modify_tuple`] / [`heap_modify_tuple_by_cols`].
#[inline]
fn copy_tuple_identity(new_tuple: &mut FormedTuple, old: &FormedTuple) {
    let old_ctid = old
        .tuple
        .t_data
        .as_ref()
        .expect("heap_modify_tuple: old tuple has no t_data")
        .t_ctid;
    if let Some(td) = new_tuple.tuple.t_data.as_mut() {
        td.t_ctid = old_ctid;
    }
    new_tuple.tuple.t_self = old.tuple.t_self;
    new_tuple.tuple.t_tableOid = old.tuple.t_tableOid;
}

// ---------------------------------------------------------------------------
// heap_form_minimal_tuple (heaptuple.c:1452)
// ---------------------------------------------------------------------------

/// A fully-formed minimal tuple — re-exported from
/// [`types_tuple::backend_access_common_heaptuple`] (where it lives so the
/// `types-nodes` slot payload model can carry it as the
/// `MinimalTupleTableSlot.mintuple` field), matching the homing of
/// [`FormedTuple`]/[`Datum`].
pub use types_tuple::backend_access_common_heaptuple::FormedMinimalTuple;

/// `heap_form_minimal_tuple(tupleDescriptor, values, isnull, extra)`
/// (heaptuple.c:1452) — construct a `MinimalTuple` from the given
/// `values`/`isnull` arrays (one entry per `tupleDescriptor.natts`).
///
/// Exactly like [`heap_form_tuple`] except the result lacks a `HeapTupleData`
/// management struct and the system-column room: the header starts at
/// `SizeofMinimalTupleHeader` instead of `offsetof(HeapTupleHeaderData, t_bits)`,
/// and `t_hoff` carries the `+ MINIMAL_TUPLE_OFFSET` bias.
///
/// `extra` is the leading-padding allocation knob C uses
/// (`palloc0(len + extra); tuple = mem + extra`) so callers such as
/// `tuplesort`/`tuplestore` can reserve room ahead of the tuple. It must be
/// `MAXALIGN(extra)` (C `Assert`). It does not change any tuple *content*, so in
/// the owned model — where the tuple is not a raw leading-padded block — it
/// affects nothing but is still validated.
pub fn heap_form_minimal_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple_descriptor: &TupleDescData<'_>,
    values: &[Datum<'_>],
    isnull: &[bool],
    extra: Size,
) -> Result<FormedMinimalTuple<'mcx>, HeapTupleError> {
    // Assert(extra == MAXALIGN(extra));
    debug_assert_eq!(extra, maxalign(extra));

    let number_of_attributes = tuple_descriptor.natts;

    if number_of_attributes > types_tuple::heaptuple::MaxTupleAttributeNumber {
        return Err(HeapTupleError::TooManyColumns {
            columns: number_of_attributes,
            limit: types_tuple::heaptuple::MaxTupleAttributeNumber,
        });
    }

    // Check for nulls.
    let mut hasnull = false;
    for i in 0..number_of_attributes as usize {
        if isnull[i] {
            hasnull = true;
            break;
        }
    }

    // Determine total space needed.  len = SizeofMinimalTupleHeader;
    let mut len = SIZEOF_MINIMAL_TUPLE_HEADER;

    if hasnull {
        len += types_tuple::heaptuple::BITMAPLEN(number_of_attributes) as usize;
    }

    // hoff = len = MAXALIGN(len);  /* align user data safely */
    len = maxalign(len);
    let hoff = len;

    let data_len = heap_compute_data_size(tuple_descriptor, values, isnull)?;

    len += data_len;

    // C: mem = palloc0(len + extra); memset(mem, 0, extra); tuple = mem + extra.
    // The `extra` leading-pad bytes carry no tuple content (zeroed, before the
    // tuple), so the owned tuple is built from `len`/`hoff`/`data_len` exactly as
    // C does; `extra` only had to be MAXALIGNed (asserted above).
    let filled = heap_fill_tuple(mcx, tuple_descriptor, values, isnull, data_len, hasnull)?;

    let mut tuple = types_tuple::heaptuple::MinimalTupleData {
        // tuple->t_len = len;
        t_len: len as u32,
        mt_padding: [0; 6],
        t_infomask2: 0,
        t_infomask: filled.infomask,
        // tuple->t_hoff = hoff + MINIMAL_TUPLE_OFFSET;
        t_hoff: (hoff + types_tuple::heaptuple::MINIMAL_TUPLE_OFFSET) as u8,
        t_bits: filled.bits,
    };

    // HeapTupleHeaderSetNatts(tuple, numberOfAttributes) — the MinimalTuple's
    // t_infomask2 shares HeapTupleHeaderData's natts/flags layout, so the same
    // mask write applies (HEAP_NATTS_MASK).
    minimal_tuple_set_natts(&mut tuple, number_of_attributes as u16);

    Ok(FormedMinimalTuple {
        tuple: alloc_in(mcx, tuple)?,
        data: filled.data,
    })
}

/// `HeapTupleHeaderSetNatts(tuple, natts)` applied to a `MinimalTupleData`.
///
/// `HeapTupleHeaderSetNatts` (`htup_details.h`) is defined over
/// `HeapTupleHeaderData`, but a `MinimalTupleData` shares the same
/// `t_infomask2` field (the "Fields below here must match HeapTupleHeaderData!"
/// tail), so `heap_form_minimal_tuple` legitimately calls it on the minimal
/// tuple. We replicate the mask write here because the idiomatic
/// `HeapTupleHeaderSetNatts` is typed to `HeapTupleHeaderData`.
#[inline]
fn minimal_tuple_set_natts(tuple: &mut types_tuple::heaptuple::MinimalTupleData<'_>, natts: u16) {
    tuple.t_infomask2 = (tuple.t_infomask2 & !types_tuple::heaptuple::HEAP_NATTS_MASK)
        | (natts & types_tuple::heaptuple::HEAP_NATTS_MASK);
}

// ===========================================================================
// attisnull / nocachegetattr / getsysattr (heaptuple.c)
//
// These read a single attribute (or the null-presence) out of a fully-formed
// tuple. In C the tuple body (header + bitmap + data) is one contiguous block;
// here the header (incl. `t_bits` null bitmap) lives in the owned
// `HeapTupleHeaderData` and the user-data area travels alongside as the `data`
// argument (the `FormedTuple::data` model used throughout this crate).
// ===========================================================================

/// System attribute numbers used by [`heap_attisnull`] / [`heap_getsysattr`]
/// (access/sysattr.h).
use types_tuple::heaptuple::{
    MaxCommandIdAttributeNumber, MaxTransactionIdAttributeNumber, MinCommandIdAttributeNumber,
    MinTransactionIdAttributeNumber, SelfItemPointerAttributeNumber, TableOidAttributeNumber,
};

/// `HeapTupleNoNulls(tuple)` (htup_details.h): `(t_infomask & HEAP_HASNULL) == 0`.
#[inline]
fn header_no_nulls(header: &HeapTupleHeaderData) -> bool {
    (header.t_infomask & HEAP_HASNULL) == 0
}

/// `HeapTupleHeaderGetRawXmax(td)` (htup_details.h): the raw xmax. (The idiomatic
/// `types_tuple::heaptuple` exposes `HeapTupleHeaderGetRawXmin` but not `Xmax`, so the
/// equivalent field read is mirrored here.)
#[inline]
fn header_raw_xmax(header: &HeapTupleHeaderData) -> types_core::TransactionId {
    // C reads t_heap.t_xmax through the union unconditionally; reading it off
    // a composite-Datum header is a caller bug in both worlds (C would return
    // the datum_typmod bytes). Surface it in debug builds; the release return
    // of InvalidTransactionId keeps the read total.
    debug_assert!(
        matches!(header.t_choice, HeapTupleHeaderChoice::THeap(_)),
        "header_raw_xmax: header is a composite Datum, not a heap tuple"
    );
    match &header.t_choice {
        HeapTupleHeaderChoice::THeap(t_heap) => t_heap.t_xmax,
        HeapTupleHeaderChoice::TDatum(_) => 0,
    }
}

/// `heap_attisnull(tup, attnum, tupleDesc)` (heaptuple.c:455) — returns true iff
/// tuple attribute `attnum` is not present (NULL, or absent beyond the tuple's
/// stored natts with no missing value).
///
/// `tupleDesc` may be `None` for relations not expected to have missing values
/// (catalog relations and indexes), exactly as C allows a NULL `tupledesc`.
/// `attnum` is 1-based for user columns; system columns use the negative
/// `*AttributeNumber` constants. Panics on an invalid `attnum`
/// (C: `elog(ERROR, "invalid attnum: %d", attnum)`).
pub fn heap_attisnull(
    tuple: &HeapTupleData,
    attnum: i32,
    tuple_desc: Option<&TupleDescData>,
) -> bool {
    let tup = tuple
        .t_data
        .as_ref()
        .expect("heap_attisnull: tuple has no t_data");

    // Assert(!tupleDesc || attnum <= tupleDesc->natts);
    debug_assert!(tuple_desc.is_none_or(|d| attnum <= d.natts));

    if attnum > HeapTupleHeaderGetNatts(tup) as i32 {
        return match tuple_desc {
            Some(d) if d.compact_attrs[(attnum - 1) as usize].atthasmissing => false,
            _ => true,
        };
    }

    if attnum > 0 {
        if header_no_nulls(tup) {
            return false;
        }
        return att_isnull((attnum - 1) as usize, &tup.t_bits);
    }

    match attnum {
        x if x == TableOidAttributeNumber as i32
            || x == SelfItemPointerAttributeNumber as i32
            || x == MinTransactionIdAttributeNumber as i32
            || x == MinCommandIdAttributeNumber as i32
            || x == MaxTransactionIdAttributeNumber as i32
            || x == MaxCommandIdAttributeNumber as i32 =>
        {
            // these are never null
            false
        }
        _ => panic!("invalid attnum: {attnum}"),
    }
}

/// `nocachegetattr(tup, attnum, tupleDesc)` (heaptuple.c:520) — fetch a single
/// non-null user attribute (`attnum` is 1-based) when a cached offset is not
/// usable. `data` is the tuple's user-data area ([`FormedTuple::data`]).
///
/// Walks attribute offsets up to `attnum` and fetches only the target column,
/// exactly as C does. C also opportunistically *writes* `attcacheoff` for
/// fixed-width prefixes; the descriptor is borrowed immutably here, so the
/// cache writes are omitted (a pure performance optimization, see
/// [`heap_deform_tuple`]) while existing cached offsets are still honored.
///
/// Callers reach this only for `attnum > 0` non-null attributes (per
/// `fastgetattr`); a NULL or out-of-range `attnum` is a caller bug.
pub fn nocachegetattr<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &HeapTupleData<'_>,
    attnum: i32,
    tuple_desc: &TupleDescData<'_>,
    data: &[u8],
) -> PgResult<Datum<'mcx>> {
    debug_assert!(attnum > 0, "nocachegetattr: attnum must be > 0");
    let td = tuple
        .t_data
        .as_ref()
        .expect("nocachegetattr: tuple has no t_data");
    let bp = &td.t_bits; // ptr to null bitmap in tuple
    let hasnulls = !header_no_nulls(td);
    let attnum = (attnum - 1) as usize; // attnum--;

    let mut slow = false; // do we have to walk attrs?

    if hasnulls {
        // there's a null somewhere in the tuple: check to see if any
        // preceding bits are null...
        let byte = attnum >> 3;
        let finalbit = attnum & 0x07;

        // check for nulls "before" final bit of last byte
        if (!bp[byte]) & ((1u8 << finalbit).wrapping_sub(1)) != 0 {
            slow = true;
        } else {
            // check for nulls in any "earlier" bytes
            for i in 0..byte {
                if bp[i] != 0xFF {
                    slow = true;
                    break;
                }
            }
        }
    }

    // tp = (char *) td + td->t_hoff;  ==> the `data` slice.
    if !slow {
        // No nulls up to and including the target attribute: a cached offset
        // is directly usable.
        let att = &tuple_desc.compact_attrs[attnum];
        if att.attcacheoff >= 0 {
            return fetchatt(mcx, att, data, att.attcacheoff as usize);
        }

        // Otherwise check for non-fixed-length attrs up to and including the
        // target; with none, the offsets are computable without the data.
        if (td.t_infomask & HEAP_HASVARWIDTH) != 0 {
            for j in 0..=attnum {
                if tuple_desc.compact_attrs[j].attlen <= 0 {
                    slow = true;
                    break;
                }
            }
        }
    }

    let mut off: usize;
    if !slow {
        // All fixed-width, no nulls, up to and including the target: compute
        // the offset by pure alignment arithmetic. (C additionally writes the
        // computed offsets into attcacheoff for *all* leading fixed-width
        // columns; cache writes omitted, values identical.)
        off = 0;
        for j in 0..attnum {
            let att = &tuple_desc.compact_attrs[j];
            off = att_nominal_alignby(off, att.attalignby);
            off += att.attlen as usize;
        }
        off = att_nominal_alignby(off, tuple_desc.compact_attrs[attnum].attalignby);
    } else {
        // Walk the tuple CAREFULLY. Nulls have no storage and no alignment
        // padding; cached offsets remain usable until a null or var-width
        // attribute is passed.
        let mut usecache = true;
        off = 0;
        let mut i = 0usize;
        loop {
            let att = &tuple_desc.compact_attrs[i];

            if hasnulls && att_isnull(i, bp) {
                usecache = false;
                i += 1;
                continue; // this cannot be the target att
            }

            // If we know the next offset, we can skip the rest.
            if usecache && att.attcacheoff >= 0 {
                off = att.attcacheoff as usize;
            } else if att.attlen == -1 {
                // Only usable as a cached offset if already suitably aligned
                // (C caches it then; cache write omitted).
                if usecache && off == att_nominal_alignby(off, att.attalignby) {
                    // off is already correct.
                } else {
                    off = att_pointer_alignby(off, att.attalignby, -1, data, off);
                    usecache = false;
                }
            } else {
                // not varlena, so safe to use att_nominal_alignby
                off = att_nominal_alignby(off, att.attalignby);
            }

            if i == attnum {
                break;
            }

            off = att_addlength_pointer(off, att.attlen, data, off);

            if usecache && att.attlen <= 0 {
                usecache = false;
            }
            i += 1;
        }
    }

    fetchatt(mcx, &tuple_desc.compact_attrs[attnum], data, off)
}

/// `heap_getsysattr(tup, attnum, tupleDesc, &isnull)` (heaptuple.c:724) — fetch
/// the value of a system attribute. No system attribute ever reads as NULL, so
/// the returned `bool` (isnull) is always false.
///
/// `SelfItemPointerAttributeNumber` is a pass-by-reference datum (the `t_self`
/// `ItemPointerData`); in the byte model it is returned as the verbatim
/// `ItemPointerData` bytes (`ByRef`), the faithful stand-in for C's
/// `PointerGetDatum(&tup->t_self)`. The xact/cid attributes are by-value.
/// Panics on an invalid `attnum` (C: `elog(ERROR, "invalid attnum: %d")`).
/// Fallible: the ctid by-reference bytes are copied into `mcx`.
pub fn heap_getsysattr<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &HeapTupleData<'_>,
    attnum: i32,
) -> PgResult<DeformedColumn<'mcx>> {
    let header = tuple
        .t_data
        .as_ref()
        .expect("heap_getsysattr: tuple has no t_data");

    // Currently, no sys attribute ever reads as NULL.
    let value = if attnum == SelfItemPointerAttributeNumber as i32 {
        // PointerGetDatum(&(tup->t_self)): the ItemPointerData bytes.
        Datum::ByRef(item_pointer_bytes(mcx, &tuple.t_self)?)
    } else if attnum == MinTransactionIdAttributeNumber as i32 {
        // TransactionIdGetDatum(HeapTupleHeaderGetRawXmin(tup->t_data))
        Datum::from_u32(types_tuple::heaptuple::HeapTupleHeaderGetRawXmin(header))
    } else if attnum == MaxTransactionIdAttributeNumber as i32 {
        // TransactionIdGetDatum(HeapTupleHeaderGetRawXmax(tup->t_data))
        Datum::from_u32(header_raw_xmax(header))
    } else if attnum == MinCommandIdAttributeNumber as i32
        || attnum == MaxCommandIdAttributeNumber as i32
    {
        // CommandIdGetDatum(HeapTupleHeaderGetRawCommandId(tup->t_data))
        Datum::from_u32(types_tuple::heaptuple::HeapTupleHeaderGetRawCommandId(header))
    } else if attnum == TableOidAttributeNumber as i32 {
        // ObjectIdGetDatum(tup->t_tableOid)
        Datum::from_oid(tuple.t_tableOid)
    } else {
        panic!("invalid attnum: {attnum}");
    };

    Ok((value, false))
}

/// The little-endian on-disk bytes of an `ItemPointerData` (4 bytes blockid + 2
/// bytes posid), as `PointerGetDatum(&t_self)` would expose them.
#[inline]
pub fn item_pointer_bytes<'mcx>(mcx: Mcx<'mcx>, ip: &ItemPointerData) -> PgResult<PgVec<'mcx, u8>> {
    let mut out = vec_with_capacity_in(mcx, 6)?;
    out.extend_from_slice(&ip.ip_blkid.bi_hi.to_ne_bytes());
    out.extend_from_slice(&ip.ip_blkid.bi_lo.to_ne_bytes());
    out.extend_from_slice(&ip.ip_posid.to_ne_bytes());
    Ok(out)
}

// ===========================================================================
// expand_tuple / heap_expand_tuple / minimal_expand_tuple (heaptuple.c)
//
// Expand a source tuple that has FEWER attributes than the descriptor requires:
// the source data area is copied VERBATIM and the trailing missing attributes
// (or NULLs) are appended via fill_val, matching heaptuple.c:829.
// ===========================================================================

/// Which kind of tuple [`expand_tuple`] should build.
enum ExpandTarget {
    Heap,
    Minimal,
}

/// The flat layout computed by [`expand_tuple`]: the data area, the null bitmap,
/// the header metadata, and `t_hoff`.
struct ExpandedLayout<'mcx> {
    /// Total tuple length (`t_len`).
    len: usize,
    /// `t_hoff` to store in the header (MINIMAL_TUPLE_OFFSET-adjusted for the
    /// minimal target).
    hoff: usize,
    /// `t_infomask` accumulated while filling the trailing attributes.
    infomask: u16,
    /// The user-data area bytes (verbatim source data + appended missing values).
    data: PgVec<'mcx, u8>,
    /// The null bitmap (`BITMAPLEN(natts)` bytes), or empty when no nulls.
    bits: PgVec<'mcx, bits8>,
}

/// `expand_tuple` (heaptuple.c:829). The source must have fewer attributes than
/// `tuple_desc.natts`. Builds the data area + null bitmap + header metadata; the
/// caller wraps them into a [`FormedTuple`] or [`FormedMinimalTuple`].
///
/// `source` carries the verbatim source data area in [`FormedTuple::data`] (C's
/// `(char *) sourceTuple->t_data + t_hoff`).
fn expand_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    target: &ExpandTarget,
    source: &FormedTuple<'_>,
    tuple_desc: &TupleDescData<'_>,
) -> PgResult<ExpandedLayout<'mcx>> {
    let source_header = source
        .tuple
        .t_data
        .as_ref()
        .expect("expand_tuple: source tuple has no t_data");
    let mut has_nulls = (source_header.t_infomask & HEAP_HASNULL) != 0;
    let source_natts = HeapTupleHeaderGetNatts(source_header) as usize;
    let natts = tuple_desc.natts as usize;

    // Assert(sourceNatts < natts);
    debug_assert!(
        source_natts < natts,
        "expand_tuple: source must have fewer attributes than required"
    );

    let mut source_null_len = if has_nulls {
        BITMAPLEN(source_natts as i32) as usize
    } else {
        0
    };

    // sourceDataLen = sourceTuple->t_len - sourceTHeader->t_hoff; the data area
    // is the source's `data` Vec (laid out at td + t_hoff).
    let source_data = &source.data;
    let source_data_len = source_data.len();
    let mut target_data_len = source_data_len;

    // Determine which trailing attributes have a missing value.
    let attrmiss: Option<&[types_tuple::heaptuple::AttrMissing<'_>]> = constr_missing(tuple_desc);
    let mut first_missing = source_natts;
    if let Some(attrmiss) = attrmiss {
        // Find the first attr for which we don't have a value in the source.
        while first_missing < natts {
            if attrmiss[first_missing].am_present {
                break;
            }
            has_nulls = true;
            first_missing += 1;
        }
        // Walk the missing attributes, making space for present ones.
        for attnum in first_missing..natts {
            if attrmiss[attnum].am_present {
                let att = &tuple_desc.compact_attrs[attnum];
                let value = missing_value(&attrmiss[attnum], att);
                target_data_len = att_datum_alignby(
                    target_data_len,
                    att.attalignby,
                    att.attlen,
                    value,
                );
                target_data_len = att_addlength_datum(target_data_len, att.attlen, value);
            } else {
                // no missing value, so it must be null
                has_nulls = true;
            }
        }
    } else {
        // No missing values at all: NULLs must be allowed, since some attributes
        // are known to be absent.
        has_nulls = true;
    }

    let target_null_len = if has_nulls {
        BITMAPLEN(natts as i32) as usize
    } else {
        0
    };

    // len = targetNullLen; then header base; MAXALIGN -> hoff; + targetDataLen.
    let header_base = match target {
        ExpandTarget::Heap => SizeofHeapTupleHeader,
        ExpandTarget::Minimal => SIZEOF_MINIMAL_TUPLE_HEADER,
    };
    let mut len = target_null_len + header_base;
    let hoff = maxalign(len);
    len = hoff + target_data_len;

    let mut data = vec_with_capacity_in(mcx, target_data_len)?;
    data.resize(target_data_len, 0);
    let mut bits: PgVec<'mcx, bits8> = vec_with_capacity_in(mcx, target_null_len)?;
    bits.resize(target_null_len, 0);
    let mut infomask = source_header.t_infomask;

    // Build the null bitmap from the source (or all-NOT-NULL), exactly as C does.
    let mut walk = BitWalk {
        byte: usize::MAX,
        bitmask: HIGHBIT,
    };
    let has_bitmap = target_null_len > 0;
    if has_bitmap {
        if source_null_len > 0 {
            // Pre-existing bitmap: copy it in (all source attrs already marked).
            bits[..source_null_len].copy_from_slice(&source_header.t_bits[..source_null_len]);
        } else {
            source_null_len = BITMAPLEN(source_natts as i32) as usize;
            // Set NOT NULL for all existing attributes.
            for b in &mut bits[..source_null_len] {
                *b = 0xff;
            }
            if source_natts & 0x07 != 0 {
                // Build the mask (inverted!) and clear the high bits.
                let bit_mask_byte = 0xffu8 << (source_natts & 0x07);
                bits[source_null_len - 1] = !bit_mask_byte;
            }
        }
        // C: nullBits += sourceNullLen - 1; bitMask = 1 << ((sourceNatts-1)&7).
        // fill_val advances the mask first, so seed the cursor at the last source
        // byte with the bit mask for the last source attr.  For sourceNatts == 0
        // (sourceNullLen == 0) C's nullBits points one byte BEFORE the bitmap and
        // bitMask == HIGHBIT, so the first fill_val call advances onto bits[0];
        // wrapping_sub models that &bits[-1] cursor exactly (cf. heap_fill_tuple's
        // usize::MAX seed).
        walk.byte = source_null_len.wrapping_sub(1);
        walk.bitmask = 1 << (source_natts.wrapping_sub(1) & 0x07);
    }

    // Copy the source data area VERBATIM.
    data[..source_data_len].copy_from_slice(source_data);

    // Now fill in the trailing missing values / NULLs.
    let mut cursor = source_data_len;
    for attnum in source_natts..natts {
        let att = &tuple_desc.compact_attrs[attnum];
        let present = attrmiss.is_some_and(|m| m[attnum].am_present);
        if present {
            let value = missing_value(&attrmiss.unwrap()[attnum], att);
            fill_val(
                att,
                if has_bitmap { Some(&mut bits) } else { None },
                &mut walk,
                &mut data,
                &mut cursor,
                &mut infomask,
                value,
                false,
            )?;
        } else {
            fill_val(
                att,
                Some(&mut bits),
                &mut walk,
                &mut data,
                &mut cursor,
                &mut infomask,
                &Datum::null(),
                true,
            )?;
        }
    }

    debug_assert_eq!(cursor, target_data_len);

    Ok(ExpandedLayout {
        len,
        hoff: match target {
            ExpandTarget::Heap => hoff,
            ExpandTarget::Minimal => hoff + types_tuple::heaptuple::MINIMAL_TUPLE_OFFSET,
        },
        infomask,
        data,
        bits,
    })
}

/// The [`Datum`] for a present missing attribute (shared by
/// [`getmissingattr`] and [`expand_tuple`]). C stores `attrmiss->am_value` as
/// a `Datum`: the scalar word for a by-value attribute, a pointer to the
/// value's bytes for a by-reference one. `AttrMissing.am_value` carries
/// exactly that as a [`Datum`]; this only checks (debug) that the stored
/// shape matches the attribute's `attbyval` — in C a mismatch would be the
/// same caller/catalog corruption, read through the wrong Datum
/// interpretation.
#[inline]
fn missing_value<'a, 'mcx>(
    attrmiss: &'a types_tuple::heaptuple::AttrMissing<'mcx>,
    att: &CompactAttribute,
) -> &'a Datum<'mcx> {
    debug_assert_eq!(
        matches!(attrmiss.am_value, Datum::ByVal(_)),
        att.attbyval,
        "missing_value: AttrMissing.am_value shape disagrees with attbyval (attlen={})",
        att.attlen
    );
    &attrmiss.am_value
}

/// `minimal_expand_tuple(sourceTuple, tupleDesc)` (heaptuple.c:1053) — fill in the
/// missing values for a minimal HeapTuple.
pub fn minimal_expand_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    source: &FormedTuple<'_>,
    tuple_desc: &TupleDescData<'_>,
) -> PgResult<FormedMinimalTuple<'mcx>> {
    let layout = expand_tuple(mcx, &ExpandTarget::Minimal, source, tuple_desc)?;

    let mut tuple = types_tuple::heaptuple::MinimalTupleData {
        t_len: layout.len as u32,
        mt_padding: [0; 6],
        t_infomask2: 0,
        t_infomask: layout.infomask,
        t_hoff: layout.hoff as u8,
        t_bits: layout.bits,
    };
    minimal_tuple_set_natts(&mut tuple, tuple_desc.natts as u16);

    Ok(FormedMinimalTuple {
        tuple: alloc_in(mcx, tuple)?,
        data: layout.data,
    })
}

/// `heap_expand_tuple(sourceTuple, tupleDesc)` (heaptuple.c:1065) — fill in the
/// missing values for an ordinary HeapTuple.
pub fn heap_expand_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    source: &FormedTuple<'_>,
    tuple_desc: &TupleDescData<'_>,
) -> PgResult<FormedTuple<'mcx>> {
    let layout = expand_tuple(mcx, &ExpandTarget::Heap, source, tuple_desc)?;

    // C: targetTHeader->t_infomask = sourceTHeader->t_infomask (already folded
    // into layout.infomask); SetNatts/SetDatumLength/SetTypeId/SetTypMod;
    // t_self/t_tableOid from the source; t_ctid invalid.
    let mut td = HeapTupleHeaderData {
        t_choice: HeapTupleHeaderChoice::TDatum(DatumTupleFields {
            datum_len_: layout.len as i32,
            datum_typmod: tuple_desc.tdtypmod,
            datum_typeid: tuple_desc.tdtypeid,
        }),
        t_ctid: invalid_item_pointer(),
        t_infomask2: 0,
        t_infomask: layout.infomask,
        t_hoff: layout.hoff as u8,
        t_bits: layout.bits,
    };
    HeapTupleHeaderSetNatts(&mut td, tuple_desc.natts as u16);

    let tuple = alloc_in(
        mcx,
        HeapTupleData {
            t_len: layout.len as u32,
            // (*targetHeapTuple)->t_self = sourceTuple->t_self;
            t_self: source.tuple.t_self,
            // (*targetHeapTuple)->t_tableOid = sourceTuple->t_tableOid;
            t_tableOid: source.tuple.t_tableOid,
            t_data: Some(alloc_in(mcx, td)?),
        },
    )?;

    Ok(FormedTuple {
        tuple,
        data: layout.data,
    })
}

// ---------------------------------------------------------------------------
// heap_copy_tuple_as_datum (heaptuple.c:1080)
// ---------------------------------------------------------------------------

/// `heap_copy_tuple_as_datum(tuple, tupleDesc)` (heaptuple.c:1080) — copy a tuple
/// as a composite-type Datum.
///
/// In C this returns a `Datum` pointing at a palloc'd `HeapTupleHeader` copy with
/// the composite-Datum header fields (`datum_len_`/`datum_typeid`/`datum_typmod`)
/// set. In the owned model a composite Datum is the tuple's header+data; this
/// returns a [`FormedTuple`] whose header has those Datum fields filled, the
/// faithful idiomatic stand-in for `PointerGetDatum(td)`.
///
/// When the tuple contains external TOAST pointers C inlines them via
/// `toast_flatten_tuple_to_datum`; that detoast machinery
/// (`access/common/toast_helper` + `access/heap/heaptoast`) is unported here, so
/// that branch is routed through the loud-panic
/// `backend_access_heap_heaptoast_seams::toast_flatten_tuple_to_datum` seam.
pub fn heap_copy_tuple_as_datum<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &FormedTuple<'_>,
    tuple_desc: &TupleDescData<'_>,
) -> PgResult<FormedTuple<'mcx>> {
    let header = tuple
        .tuple
        .t_data
        .as_ref()
        .expect("heap_copy_tuple_as_datum: tuple has no t_data");

    // If the tuple contains any external TOAST pointers, inline those fields.
    if (header.t_infomask & HEAP_HASEXTERNAL) != 0 {
        return backend_access_heap_heaptoast_seams::toast_flatten_tuple_to_datum::call(
            mcx, tuple, tuple_desc,
        );
    }

    // Fast path: palloc'd copy with the composite-Datum header fields set.
    let mut new = tuple.clone_in(mcx)?;
    if let Some(td) = new.tuple.t_data.as_mut() {
        // HeapTupleHeaderSetDatumLength(td, tuple->t_len);
        // HeapTupleHeaderSetTypeId / SetTypMod.
        td.t_choice = HeapTupleHeaderChoice::TDatum(DatumTupleFields {
            datum_len_: tuple.tuple.t_len as i32,
            datum_typmod: tuple_desc.tdtypmod,
            datum_typeid: tuple_desc.tdtypeid,
        });
    }
    Ok(new)
}

// ---------------------------------------------------------------------------
// heap_free_minimal_tuple / heap_copy_minimal_tuple (heaptuple.c:1529/1541)
// heap_tuple_from_minimal_tuple / minimal_tuple_from_heap_tuple
//   (heaptuple.c:1564/1586)
// ---------------------------------------------------------------------------

/// `heap_free_minimal_tuple(mtup)` (heaptuple.c:1529) — `pfree(mtup)`. In the
/// owned model freeing is dropping: taking the [`FormedMinimalTuple`] by value
/// consumes it.
#[inline]
pub fn heap_free_minimal_tuple(mtup: FormedMinimalTuple<'_>) {
    drop(mtup);
}

/// `heap_copy_minimal_tuple(mtup, extra)` (heaptuple.c:1541) — copy a
/// MinimalTuple into `mcx`. `extra` must be `MAXALIGN(extra)` (C `Assert`); it
/// only governs C's leading-padding allocation (which carries no tuple
/// content), so the owned copy is a deep clone of the header + data.
pub fn heap_copy_minimal_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    mtup: &FormedMinimalTuple<'_>,
    extra: Size,
) -> PgResult<FormedMinimalTuple<'mcx>> {
    debug_assert_eq!(extra, maxalign(extra));
    mtup.clone_in(mcx)
}

/// `heap_tuple_from_minimal_tuple(mtup)` (heaptuple.c:1564) — create a HeapTuple
/// by copying from a MinimalTuple; system columns are filled with zeroes.
///
/// C lays the minimal tuple body at `(char *) result->t_data + MINIMAL_TUPLE_OFFSET`
/// then zeroes `offsetof(HeapTupleHeaderData, t_infomask2)` bytes of the front
/// (the system-column region: t_choice + t_ctid). In the owned model that is a
/// fresh `HeapTupleHeaderData` whose `t_choice` is zeroed (a default
/// `HeapTupleFields`) and `t_ctid` is zeroed, sharing the minimal tuple's
/// `t_infomask2 .. t_bits` tail and data area. `t_len` is `mtup.t_len +
/// MINIMAL_TUPLE_OFFSET`; `t_hoff` likewise drops the minimal bias back to the
/// full HeapTuple header offset.
pub fn heap_tuple_from_minimal_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    mtup: &FormedMinimalTuple<'_>,
) -> PgResult<FormedTuple<'mcx>> {
    let len = mtup.tuple.t_len as usize + types_tuple::heaptuple::MINIMAL_TUPLE_OFFSET;

    // memset(result->t_data, 0, offsetof(t_infomask2)) zeroes t_choice + t_ctid.
    let td = HeapTupleHeaderData {
        t_choice: HeapTupleHeaderChoice::THeap(HeapTupleFields::default()),
        t_ctid: ItemPointerData::default(),
        // Shared tail copied from the minimal tuple.
        t_infomask2: mtup.tuple.t_infomask2,
        t_infomask: mtup.tuple.t_infomask,
        // result->t_data's t_hoff: the minimal tuple's t_hoff already carries the
        // MINIMAL_TUPLE_OFFSET bias, and the HeapTuple header occupies exactly
        // that region, so the on-disk header offset is unchanged.
        t_hoff: mtup.tuple.t_hoff,
        t_bits: slice_in(mcx, &mtup.tuple.t_bits)?,
    };

    let tuple = alloc_in(
        mcx,
        HeapTupleData {
            t_len: len as u32,
            // ItemPointerSetInvalid(&result->t_self);
            t_self: invalid_item_pointer(),
            t_tableOid: INVALID_OID,
            t_data: Some(alloc_in(mcx, td)?),
        },
    )?;

    Ok(FormedTuple {
        tuple,
        data: slice_in(mcx, &mtup.data)?,
    })
}

/// `minimal_tuple_from_heap_tuple(htup, extra)` (heaptuple.c:1586) — create a
/// MinimalTuple by copying from a HeapTuple. `extra` must be `MAXALIGN(extra)`
/// (C `Assert`). C copies from `(char *) htup->t_data + MINIMAL_TUPLE_OFFSET`,
/// i.e. it drops the leading `MINIMAL_TUPLE_OFFSET` bytes of the HeapTuple header
/// (the t_choice/t_ctid system-column region) and keeps the shared
/// `t_infomask2 .. t_bits` tail + data; `len = htup->t_len - MINIMAL_TUPLE_OFFSET`.
pub fn minimal_tuple_from_heap_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    htup: &FormedTuple<'_>,
    extra: Size,
) -> PgResult<FormedMinimalTuple<'mcx>> {
    debug_assert_eq!(extra, maxalign(extra));
    // Assert(htup->t_len > MINIMAL_TUPLE_OFFSET);
    debug_assert!(htup.tuple.t_len as usize > types_tuple::heaptuple::MINIMAL_TUPLE_OFFSET);

    let header = htup
        .tuple
        .t_data
        .as_ref()
        .expect("minimal_tuple_from_heap_tuple: tuple has no t_data");

    let len = htup.tuple.t_len as usize - types_tuple::heaptuple::MINIMAL_TUPLE_OFFSET;

    let tuple = types_tuple::heaptuple::MinimalTupleData {
        // result->t_len = len; (set after the memcpy in C, overriding the copied
        // bytes that aliased the HeapTuple's t_len region).
        t_len: len as u32,
        mt_padding: [0; 6],
        // Shared tail copied from the heap tuple header.
        t_infomask2: header.t_infomask2,
        t_infomask: header.t_infomask,
        t_hoff: header.t_hoff,
        t_bits: slice_in(mcx, &header.t_bits)?,
    };

    Ok(FormedMinimalTuple {
        tuple: alloc_in(mcx, tuple)?,
        data: slice_in(mcx, &htup.data)?,
    })
}

/// `ItemPointerSetInvalid(&p)` (itemptr.h): `blockid = (0xffff, 0xffff)`,
/// `posid = 0` (== `InvalidOffsetNumber`).
#[inline]
fn invalid_item_pointer() -> types_tuple::heaptuple::ItemPointerData {
    types_tuple::heaptuple::ItemPointerData {
        ip_blkid: types_tuple::heaptuple::BlockIdData {
            bi_hi: 0xffff,
            bi_lo: 0xffff,
        },
        ip_posid: 0,
    }
}

// ===========================================================================
// On-disk tuple image serializer.
//
// The idiomatic owned tuple model splits a tuple's *header*
// (`HeapTupleData::t_data`: the fixed `HeapTupleHeaderData` + its `t_bits` null
// bitmap) from its *user-data area* (the column bytes, `FormedTuple::data`).
// In C those are one contiguous `palloc` chunk laid out as
//
//   [HeapTupleHeaderData fixed 23 bytes][t_bits][pad to t_hoff][user data]
//
// reached through the single `t_data` pointer.  `FormedTuple` is the one
// data-carrying tuple shape; when such a tuple must cross a page-write seam
// (`page_add_item` / `relation_put_heap_tuple`) and be laid down on disk, this
// module is the single source of truth that re-assembles the canonical C
// on-disk byte image from it.
// ===========================================================================

/// Serialize a [`FormedTuple`] (owned header `t_data` + `t_bits` + the column
/// bytes in [`FormedTuple::data`]) into the canonical C on-disk tuple image —
/// the exact bytes a single `palloc` chunk holds at `(char *) tuple->t_data ..
/// + tuple->t_len`:
///
/// ```text
/// [t_choice 12][t_ctid 6][t_infomask2 2][t_infomask 2][t_hoff 1][t_bits?][pad to t_hoff][user data]
/// ```
///
/// The header's already-decided fields (`t_choice`, `t_ctid`, `t_infomask`,
/// `t_infomask2`, `t_hoff`) are written verbatim — this is a *serializer*, not a
/// header-policy function, so it never re-decides xact/visibility bits (the
/// caller — e.g. `heap_prepare_insert` — has already set them on the owned
/// header).  The `t_bits` null bitmap is emitted iff `HEAP_HASNULL` is set in
/// `t_infomask`, exactly as C's `heap_fill_tuple` laid it down.
///
/// Panics if the tuple has no header (`t_data == None`): a tuple destined for a
/// page write must carry a header (a wiring bug, surfaced loud rather than
/// writing a corrupt page).
pub fn heap_tuple_to_disk_image<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &FormedTuple<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let hdr = tuple
        .tuple
        .t_data
        .as_ref()
        .expect("heap_tuple_to_disk_image: tuple has no t_data header");

    let t_hoff = hdr.t_hoff as usize;
    let user: &[u8] = &tuple.data;

    let mut img: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, t_hoff + user.len())?;

    // --- t_choice (12 bytes): the union of HeapTupleFields / DatumTupleFields ---
    match &hdr.t_choice {
        HeapTupleHeaderChoice::THeap(f) => {
            // t_xmin(4) t_xmax(4) t_field3(4)
            img.extend_from_slice(&f.t_xmin.to_ne_bytes());
            img.extend_from_slice(&f.t_xmax.to_ne_bytes());
            let field3: u32 = match f.t_field3 {
                HeapTupleField3::TCid(c) => c,
                HeapTupleField3::TXvac(x) => x,
            };
            img.extend_from_slice(&field3.to_ne_bytes());
        }
        HeapTupleHeaderChoice::TDatum(d) => {
            // datum_len_(4) datum_typmod(4) datum_typeid(4)
            img.extend_from_slice(&d.datum_len_.to_ne_bytes());
            img.extend_from_slice(&d.datum_typmod.to_ne_bytes());
            img.extend_from_slice(&d.datum_typeid.to_ne_bytes());
        }
    }

    // --- t_ctid: BlockIdData(bi_hi u16, bi_lo u16) + ip_posid u16 = 6 bytes ---
    img.extend_from_slice(&hdr.t_ctid.ip_blkid.bi_hi.to_ne_bytes());
    img.extend_from_slice(&hdr.t_ctid.ip_blkid.bi_lo.to_ne_bytes());
    img.extend_from_slice(&hdr.t_ctid.ip_posid.to_ne_bytes());

    // --- t_infomask2(2) t_infomask(2) t_hoff(1) = 5 bytes ---
    img.extend_from_slice(&hdr.t_infomask2.to_ne_bytes());
    img.extend_from_slice(&hdr.t_infomask.to_ne_bytes());
    img.push(hdr.t_hoff);
    debug_assert_eq!(img.len(), SizeofHeapTupleHeader, "SizeofHeapTupleHeader");

    // --- t_bits (null bitmap), present iff HEAP_HASNULL ---
    if (hdr.t_infomask & HEAP_HASNULL) != 0 {
        img.extend_from_slice(&hdr.t_bits);
    }

    // --- pad to t_hoff with zeros (the MAXALIGN slack heap_form_tuple left) ---
    while img.len() < t_hoff {
        img.push(0);
    }

    // --- user data (the post-t_hoff column bytes) ---
    img.extend_from_slice(user);
    Ok(img)
}

/// `heap_copytuple` reading from a contiguous on-disk heap-tuple byte image —
/// the inverse of [`heap_tuple_to_disk_image`] for the `THeap` (on-page) union
/// arm. Decodes the fixed 23-byte header (`t_xmin`/`t_xmax`/`t_field3`/`t_ctid`/
/// `t_infomask2`/`t_infomask`/`t_hoff`), the optional `t_bits` null bitmap, and
/// the post-`t_hoff` user-data column area into an owned [`FormedTuple`]
/// allocated in `mcx`.
///
/// Unlike [`DatumGetHeapTupleHeader`] (which reads a *composite Datum* image —
/// the `TDatum` arm, with no page identity), this preserves the on-page
/// `t_self`/`t_tableOid`/`t_len` the caller carries alongside the image (a
/// catcache entry remembers the source tuple's page identity, the C
/// `ct->tuple.t_self`/`t_tableOid` set from `dtp->t_self`/`t_tableOid`). It is
/// the owned-model realisation of `SearchCatCacheMiss`/`CatalogCacheCreateEntry`
/// rebuilding `&ct->tuple` (`heap_copytuple` of the cached `dtp`).
///
/// `Err` is a structurally corrupt image (length / `t_hoff` bounds / bitmap
/// overrun), surfaced loud rather than fabricated.
pub fn heap_copytuple_from_disk_image<'mcx>(
    mcx: Mcx<'mcx>,
    t_len: u32,
    t_self: ItemPointerData,
    t_tableoid: Oid,
    image: &[u8],
) -> PgResult<FormedTuple<'mcx>> {
    if image.len() < SizeofHeapTupleHeader {
        return Err(PgError::error(
            "heap_copytuple_from_disk_image: image shorter than HeapTupleHeader",
        ));
    }

    let u32_at = |o: usize| u32::from_ne_bytes([image[o], image[o + 1], image[o + 2], image[o + 3]]);
    let u16_at = |o: usize| u16::from_ne_bytes([image[o], image[o + 1]]);

    // --- t_choice (12 bytes): an on-page heap tuple carries the THeap arm ---
    let t_xmin = u32_at(0);
    let t_xmax = u32_at(4);
    let field3_raw = u32_at(8);

    // --- t_ctid (6 bytes) --- carried verbatim from the image.
    let t_ctid = ItemPointerData {
        ip_blkid: BlockIdData {
            bi_hi: u16_at(12),
            bi_lo: u16_at(14),
        },
        ip_posid: u16_at(16),
    };

    // --- t_infomask2(2) t_infomask(2) t_hoff(1) ---
    let t_infomask2 = u16_at(18);
    let t_infomask = u16_at(20);
    let t_hoff = image[22];
    let t_hoff_usize = t_hoff as usize;

    if t_hoff_usize < SizeofHeapTupleHeader || t_hoff_usize > image.len() {
        return Err(PgError::error(
            "heap_copytuple_from_disk_image: t_hoff out of bounds",
        ));
    }

    // `t_field3` is `TXvac` only when HEAP_MOVED is set (the C union accessor).
    let t_field3 = if (t_infomask & types_tuple::heaptuple::HEAP_MOVED) != 0 {
        HeapTupleField3::TXvac(field3_raw)
    } else {
        HeapTupleField3::TCid(field3_raw)
    };

    // --- t_bits (null bitmap), present iff HEAP_HASNULL ---
    let t_bits: PgVec<'mcx, bits8> = if (t_infomask & HEAP_HASNULL) != 0 {
        let natts = t_infomask2 & types_tuple::heaptuple::HEAP_NATTS_MASK;
        let bitmap_len = BITMAPLEN(natts as i32) as usize;
        if SizeofHeapTupleHeader + bitmap_len > t_hoff_usize {
            return Err(PgError::error(
                "heap_copytuple_from_disk_image: null bitmap overruns t_hoff",
            ));
        }
        slice_in(
            mcx,
            &image[SizeofHeapTupleHeader..SizeofHeapTupleHeader + bitmap_len],
        )?
    } else {
        PgVec::new_in(mcx)
    };

    let header = HeapTupleHeaderData {
        t_choice: HeapTupleHeaderChoice::THeap(HeapTupleFields {
            t_xmin,
            t_xmax,
            t_field3,
        }),
        t_ctid,
        t_infomask2,
        t_infomask,
        t_hoff,
        t_bits,
    };

    Ok(FormedTuple {
        tuple: alloc_in(
            mcx,
            HeapTupleData {
                t_len,
                t_self,
                t_tableOid: t_tableoid,
                t_data: Some(alloc_in(mcx, header)?),
            },
        )?,
        data: slice_in(mcx, &image[t_hoff_usize..])?,
    })
}

// ===========================================================================
// The composite/record-Datum carrier bridge (task #161):
// FormedTuple <-> composite Datum (a varlena-wrapped HeapTupleHeader image).
//
// In C a composite/record value crosses as an ordinary pass-by-reference
// (varlena) Datum whose bytes are a `HeapTupleHeader` (`DatumGetHeapTupleHeader`
// == `DatumGetPointer` + detoast; `HeapTupleGetDatum` ==
// `HeapTupleHeaderGetDatum(tuple->t_data)`). PostgreSQL has NO composite *kind*;
// the record case is just `Datum::ByRef(bytes)` (datum-redesign-plan, Option A).
// These two functions are the canonical model's faithful realisation of that
// pair: they compose the existing carrier conversions
// (`heap_copy_tuple_as_datum` to set the composite-Datum header fields +
// `heap_tuple_to_disk_image` to lay down the contiguous byte image) with the
// `Datum::ByRef` byte lane — NO new Datum variant and NO forged pointer.
// ===========================================================================

/// `HeapTupleGetDatum(tuple)` (htup_details.h) — turn a fully-formed tuple into
/// a composite/record `Datum`. C's macro is
/// `HeapTupleHeaderGetDatum((tuple)->t_data)`: it sets the composite-Datum
/// header fields (`datum_len_`/`datum_typeid`/`datum_typmod`, via
/// `heap_form_tuple`/`heap_copy_tuple_as_datum`) and inlines any external TOAST
/// pointers, yielding a self-contained varlena-wrapped `HeapTupleHeader`.
///
/// In the canonical model that self-contained byte image is exactly a
/// by-reference [`Datum::ByRef`] value (a composite value is a pass-by-reference
/// Datum, datum-redesign-plan Option A). This composes
/// [`heap_copy_tuple_as_datum`] (which sets the Datum header fields and routes
/// the `HEAP_HASEXTERNAL` case through the heaptoast flatten seam) with
/// [`heap_tuple_to_disk_image`] (which serialises header + null bitmap + pad +
/// user data into the contiguous `t_len`-byte image). The bytes are allocated in
/// `mcx` (C: palloc in the current context). `Err` carries OOM and any detoast
/// `ereport(ERROR)` from the flatten path.
#[allow(non_snake_case)]
pub fn HeapTupleGetDatum<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &FormedTuple<'_>,
    tuple_desc: &TupleDescData<'_>,
) -> PgResult<Datum<'mcx>> {
    // C: HeapTupleHeaderGetDatum(tuple->t_data) — set the composite-Datum header
    // fields (and flatten external TOAST pointers if any).
    let as_datum = heap_copy_tuple_as_datum(mcx, tuple, tuple_desc)?;
    // The composite Datum *is* the self-contained header+data byte image, the
    // faithful stand-in for C's `PointerGetDatum(td)`.
    let bytes = heap_tuple_to_disk_image(mcx, &as_datum)?;
    Ok(Datum::ByRef(bytes))
}

/// `DatumGetHeapTupleHeader(datum)` (htup_details.h) — the inverse bridge: read
/// a composite/record `Datum` back as a [`FormedTuple`] (owned header + user
/// data). In C this is `DatumGetPointer(datum)` (after detoasting), i.e. the
/// caller reinterprets the varlena bytes as a `HeapTupleHeader`; here the
/// already-detoasted [`Datum::ByRef`] bytes are decoded back into the structured
/// [`FormedTuple`] carrier (the reverse of [`heap_tuple_to_disk_image`]),
/// allocated in `mcx`.
///
/// The bytes are a self-contained composite-Datum image: a fixed
/// `SizeofHeapTupleHeader` (23-byte) header (with the `TDatum` union arm carrying
/// `datum_len_`/`datum_typmod`/`datum_typeid`), an optional `t_bits` null bitmap,
/// MAXALIGN pad to `t_hoff`, then the user-data column area. `t_self`/`t_tableOid`
/// are not part of the on-image bytes (a composite Datum has no page identity);
/// they are set invalid, exactly as a tuple reconstructed from a Datum has no
/// home page. Panics if a `ByVal` scalar is passed (a caller bug — C would read
/// garbage by treating a scalar word as a pointer); `Err` is a structurally
/// corrupt image (length/`t_hoff` bounds), surfaced loud rather than fabricated.
#[allow(non_snake_case)]
pub fn DatumGetHeapTupleHeader<'mcx>(
    mcx: Mcx<'mcx>,
    datum: &Datum<'_>,
) -> PgResult<FormedTuple<'mcx>> {
    let image: &[u8] = match datum {
        Datum::ByRef(b) => b,
        // A composite carried as a live tuple needs no byte decode — re-home it.
        Datum::Composite(t) => return t.clone_in(mcx),
        Datum::ByVal(_) => {
            panic!("DatumGetHeapTupleHeader called on a by-value (non-composite) Datum")
        }
        Datum::Cstring(_) | Datum::Expanded(_) | Datum::Internal(_) => {
            panic!("DatumGetHeapTupleHeader called on a non-composite Datum (Cstring/Expanded/Internal)")
        }
    };

    if image.len() < SizeofHeapTupleHeader {
        return Err(PgError::error(
            "DatumGetHeapTupleHeader: composite Datum shorter than HeapTupleHeader",
        ));
    }

    let u32_at = |o: usize| u32::from_ne_bytes([image[o], image[o + 1], image[o + 2], image[o + 3]]);
    let u16_at = |o: usize| u16::from_ne_bytes([image[o], image[o + 1]]);

    // --- t_choice (12 bytes): a composite Datum carries the TDatum union arm ---
    let datum_len_ = u32_at(0) as i32;
    let datum_typmod = u32_at(4) as i32;
    let datum_typeid: Oid = u32_at(8);

    // --- t_ctid (6 bytes) --- (carried verbatim; a composite Datum's t_ctid
    // holds no page identity, but the bytes round-trip the disk image exactly).
    let t_ctid = ItemPointerData {
        ip_blkid: BlockIdData {
            bi_hi: u16_at(12),
            bi_lo: u16_at(14),
        },
        ip_posid: u16_at(16),
    };

    // --- t_infomask2(2) t_infomask(2) t_hoff(1) ---
    let t_infomask2 = u16_at(18);
    let t_infomask = u16_at(20);
    let t_hoff = image[22];
    let t_hoff_usize = t_hoff as usize;

    if t_hoff_usize < SizeofHeapTupleHeader || t_hoff_usize > image.len() {
        return Err(PgError::error(
            "DatumGetHeapTupleHeader: composite Datum t_hoff out of bounds",
        ));
    }

    // --- t_bits (null bitmap), present iff HEAP_HASNULL ---
    let t_bits: PgVec<'mcx, bits8> = if (t_infomask & HEAP_HASNULL) != 0 {
        let natts = t_infomask2 & types_tuple::heaptuple::HEAP_NATTS_MASK;
        let bitmap_len = BITMAPLEN(natts as i32) as usize;
        if SizeofHeapTupleHeader + bitmap_len > t_hoff_usize {
            return Err(PgError::error(
                "DatumGetHeapTupleHeader: null bitmap overruns t_hoff",
            ));
        }
        slice_in(
            mcx,
            &image[SizeofHeapTupleHeader..SizeofHeapTupleHeader + bitmap_len],
        )?
    } else {
        PgVec::new_in(mcx)
    };

    let header = HeapTupleHeaderData {
        t_choice: HeapTupleHeaderChoice::TDatum(DatumTupleFields {
            datum_len_,
            datum_typmod,
            datum_typeid,
        }),
        t_ctid,
        t_infomask2,
        t_infomask,
        t_hoff,
        t_bits,
    };

    // The byte image's leading `datum_len_` is the varlena length word
    // (`HeapTupleHeaderGetDatumLength`), equal to the full image length — the
    // tuple's `t_len`.
    let t_len = image.len() as u32;

    Ok(FormedTuple {
        tuple: alloc_in(
            mcx,
            HeapTupleData {
                t_len,
                // A composite Datum has no home page; the reconstructed tuple's
                // identity is invalid (C: a Datum-sourced tuple has no t_self).
                t_self: invalid_item_pointer(),
                t_tableOid: types_core::InvalidOid,
                t_data: Some(alloc_in(mcx, header)?),
            },
        )?,
        data: slice_in(mcx, &image[t_hoff_usize..])?,
    })
}

// ---------------------------------------------------------------------------
// Seam installation
// ---------------------------------------------------------------------------

/// Install this crate's seam implementations. This crate currently declares no
/// inward seams (all known callers can depend on it directly without a cycle),
/// so there is nothing to `set()`; the hook exists so `seams-init` wiring stays
/// uniform and a future `backend-access-common-heaptuple-seams` crate is
/// installed from here.
pub fn init_seams() {}
