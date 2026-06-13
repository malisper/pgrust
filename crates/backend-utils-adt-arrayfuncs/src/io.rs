//! I/O family: text I/O (`array_in` + the `ReadArray*` recursive-descent
//! parser, `array_out`) and binary I/O (`array_recv` / `array_send` +
//! `ReadArrayBinary`).
//!
//! The element-type I/O functions are reached through the fmgr owner's seams
//! (`input_function_call_safe` / `array_output_function_call` /
//! `array_receive_function_call` / `array_send_function_call`); the element
//! type's storage metadata + I/O func OID come from `get_type_io_data`
//! (lsyscache owner). Element-type mismatch in `array_recv` is reported through
//! the format-type owner's `format_type_be` seam.
//!
//! C's `array_in` / `array_out` / `array_recv` / `array_send` cache the element
//! I/O metadata in `fcinfo->flinfo->fn_extra` (an `ArrayMetaState`) across
//! calls; that is a pure performance optimization, so the port resolves the
//! metadata with `get_type_io_data` at the top of each call exactly as the C
//! does on a cold cache (`my_extra->element_type != element_type`).
//!
//! The C parser walks a NUL-terminated `char *` by advancing a pointer; this
//! port walks the input bytes with a slice cursor (`&mut &[u8]`), reading a
//! virtual `'\0'` at end of input, so the branch structure is identical.

extern crate alloc;

use alloc::format;
use alloc::vec::Vec;

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_array::{ArrayElementDatum, ArrayElementIoData, ArrayIoFuncSelector};
use types_core::Oid;
use types_datum::datum::Datum;
use types_error::{
    PgError, PgResult, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_INVALID_BINARY_REPRESENTATION, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_UNDEFINED_FUNCTION,
};

use crate::foundation::{
    self, MAX_ALLOC_SIZE, MAX_ARRAY_SIZE, MAX_DIM,
};

use backend_utils_adt_arrayutils_seams as arrayutils;
use backend_utils_adt_format_type_seams as format_type;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_fmgr_fmgr_seams as fmgr;

// ---------------------------------------------------------------------------
// Local constants mirrored from arrayfuncs.c.
// ---------------------------------------------------------------------------

/// `Array_nulls` GUC (arrayfuncs.c): defaults to `true`. The GUC owner is the
/// guc subsystem; the array text parser only reads it, and the installed
/// default is `true`, which is what `array_in` observes.
const ARRAY_NULLS: bool = true;

/// `ASSGN` == `"="` (arrayfuncs.c).
const ASSGN: &[u8] = b"=";

/// `FirstGenbkiObjectId` (`access/transam.h`, verified `#define
/// FirstGenbkiObjectId 10000`): OIDs below this are built-in and stable enough
/// that a binary element-type mismatch is worth complaining about.
const FIRST_GENBKI_OBJECT_ID: Oid = 10000;

// ---------------------------------------------------------------------------
// Small pure helpers (the C macros / scansup.c whitespace class).
// ---------------------------------------------------------------------------

/// `scanner_isspace(ch)` (`parser/scansup.c`): the flex `{space}` character
/// class -- space, tab, newline, carriage return, vertical tab, form feed.
#[inline]
fn scanner_isspace(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

/// `*p`: current byte, or virtual NUL at/after end of input.
#[inline]
fn cur(p: &[u8]) -> u8 {
    p.first().copied().unwrap_or(0)
}

/// `p++`: advance one byte, never past the end.
#[inline]
fn bump(p: &mut &[u8]) {
    if !p.is_empty() {
        *p = &p[1..];
    }
}

fn malformed(orig_str: &str, detail: &str) -> PgError {
    PgError::error(format!("malformed array literal: \"{}\"", orig_str))
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
        .with_detail(detail.to_string())
}

fn size_exceeds_array_error() -> PgError {
    PgError::error(format!(
        "array size exceeds the maximum allowed ({})",
        MAX_ARRAY_SIZE as i32
    ))
    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

fn size_exceeds_alloc_error() -> PgError {
    PgError::error(format!(
        "array size exceeds the maximum allowed ({})",
        MAX_ALLOC_SIZE as i32
    ))
    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

fn insufficient_data() -> PgError {
    PgError::error("insufficient data left in message")
        .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION)
}

// ---------------------------------------------------------------------------
// att_addlength_datum / element-byte materialization (tupmacs.h).
//
// `att_addlength_datum(cur_offset, attlen, attdatum)` advances `cur_offset` by
// the storage span of one element value held as a `Datum`. For pass-by-value
// elements (attlen > 0) this is just `attlen`. For pass-by-reference elements
// (varlena: attlen == -1, cstring: attlen == -2) the C reads the pointed-to
// bytes via VARSIZE_ANY / strlen. The element value crosses the element-I/O
// seam as a bare `Datum`, which for a by-ref type is the address of bytes the
// element input/receive function allocated; this mirrors the C exactly by
// following that pointer to size and (in CopyArrayEls) copy the on-disk bytes.
// ---------------------------------------------------------------------------

/// `att_addlength_datum(cur_offset, attlen, attdatum)` (tupmacs.h).
fn att_addlength_datum(cur_offset: usize, attlen: i32, attdatum: Datum) -> usize {
    if attlen > 0 {
        cur_offset + attlen as usize
    } else {
        // attlen == -1 (varlena): VARSIZE_ANY(DatumGetPointer(attdatum)).
        // attlen == -2 (cstring): strlen(DatumGetPointer(attdatum)) + 1.
        cur_offset + byref_element_len(attdatum.as_usize() as *const u8, attlen)
    }
}

/// On-disk byte length of one by-reference element whose bytes begin at `ptr`
/// (`att_addlength_pointer(0, attlen, attptr)` without alignment padding): a
/// fixed-length type spans `attlen` bytes, a varlena (`attlen == -1`) spans
/// `VARSIZE_ANY`, a cstring (`attlen == -2`) spans `strlen + 1`.
fn byref_element_len(ptr: *const u8, attlen: i32) -> usize {
    if attlen > 0 {
        attlen as usize
    } else if attlen == -1 {
        // VARSIZE_ANY over the varlena header at `ptr`.
        let header = unsafe { *ptr };
        if header == 0x01 {
            // 1-byte external (TOAST pointer): VARHDRSZ_EXTERNAL (2) +
            // VARTAG_SIZE(va_tag). The array build path detoasts varlena
            // elements before storing, so this is only reached for header
            // completeness.
            let tag = unsafe { *ptr.add(1) };
            let vartag = match tag {
                1 => core::mem::size_of::<usize>() * 2, // VARTAG_INDIRECT
                2 | 3 => core::mem::size_of::<usize>() * 2, // VARTAG_EXPANDED_*
                _ => 18,                                // VARTAG_ONDISK
            };
            2 + vartag
        } else if header & 0x01 == 0x01 {
            // 1-byte short header: VARSIZE_1B = (header >> 1) & 0x7F.
            ((header >> 1) & 0x7f) as usize
        } else {
            // 4-byte header (LE): VARSIZE_4B = (va_header >> 2) & 0x3FFFFFFF.
            let b = unsafe { core::slice::from_raw_parts(ptr, 4) };
            let va_header = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
            ((va_header >> 2) & 0x3fff_ffff) as usize
        }
    } else {
        debug_assert_eq!(attlen, -2);
        // cstring: strlen(ptr) + 1.
        let mut len = 0usize;
        while unsafe { *ptr.add(len) } != 0 {
            len += 1;
        }
        len + 1
    }
}

/// `ArrayCastAndSet(src, typlen, typbyval, typalign, dest)` (arrayfuncs.c):
/// store one element value `src` (held as a `Datum`) into the array data area
/// at `dest`, returning the unaligned byte length written so the caller can
/// advance + align. Pass-by-value elements are written with `store_att_byval`;
/// pass-by-reference elements have their on-disk bytes copied out of the
/// pointed-to memory (the seam producer's allocation).
fn array_cast_and_set(
    src: Datum,
    typlen: i32,
    typbyval: bool,
    _typalign: u8,
    dest: &mut [u8],
) -> usize {
    let inc;
    if typlen > 0 {
        if typbyval {
            // store_att_byval(dest, src, typlen)
            foundation::store_att_byval(dest, 0, src, typlen);
        } else {
            // memmove(dest, DatumGetPointer(src), typlen)
            let ptr = src.as_usize() as *const u8;
            let bytes = unsafe { core::slice::from_raw_parts(ptr, typlen as usize) };
            dest[..typlen as usize].copy_from_slice(bytes);
        }
        inc = typlen as usize;
    } else {
        // Pass-by-reference: copy att_addlength_datum(0, typlen, src) bytes.
        let ptr = src.as_usize() as *const u8;
        let len = byref_element_len(ptr, typlen);
        let bytes = unsafe { core::slice::from_raw_parts(ptr, len) };
        dest[..len].copy_from_slice(bytes);
        inc = len;
    }
    inc
}

/// `CopyArrayEls(array, values, nulls, nitems, typlen, typbyval, typalign,
/// freedata)` (arrayfuncs.c:961) — copy the parsed element values into the
/// array's data area and set the null bitmap. The `freedata` flag frees the C
/// by-ref allocations after copying; in this port the element bytes are owned
/// by the seam producer / `mcx`, so there is nothing for this routine to free.
#[allow(clippy::too_many_arguments)]
fn copy_array_els(
    dest: &mut [u8],
    values: &[Datum],
    nulls: Option<&[bool]>,
    nitems: i32,
    typlen: i32,
    typbyval: bool,
    typalign: u8,
    _freedata: bool,
) -> PgResult<()> {
    let data_off = foundation::arr_data_ptr_off(dest);
    let bitmap_off = foundation::arr_nullbitmap_off(dest);

    let mut p = data_off;
    let mut bitval: u8 = 0;
    let mut bitmask: u32 = 1;

    for i in 0..nitems as usize {
        let is_null = matches!(nulls, Some(n) if n[i]);
        if is_null {
            if bitmap_off.is_none() {
                // shouldn't happen
                return Err(PgError::error("null array element where not supported"));
            }
            // bitmap bit stays 0
        } else {
            bitval |= bitmask as u8;
            let inc = array_cast_and_set(values[i], typlen, typbyval, typalign, &mut dest[p..]);
            p += inc;
            // store_att_byval/copy does not pad; the caller spaced each element
            // by att_align_nominal when computing nbytes, so re-align the write
            // cursor here exactly as the C `p += ArrayCastAndSet(...)` does (the
            // C ArrayCastAndSet returns the aligned increment via the
            // att_align_nominal inside it).
            p = foundation::att_align_nominal(p, typalign);
        }
        if let Some(bmoff) = bitmap_off {
            bitmask <<= 1;
            if bitmask == 0x100 {
                dest[bmoff + i / 8] = bitval;
                bitval = 0;
                bitmask = 1;
            }
        }
    }

    if let Some(bmoff) = bitmap_off {
        if bitmask != 1 {
            let last_byte = (nitems as usize).saturating_sub(1) / 8;
            dest[bmoff + last_byte] = bitval;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Text input: array_in + ReadArray parser (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_in(string, element_type, typmod)` (arrayfuncs.c): parse the external
/// text representation of an array into the on-disk `ArrayType` bytes.
pub fn array_in<'mcx>(
    mcx: Mcx<'mcx>,
    string: &str,
    element_type: Oid,
    typmod: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    // Get info about element type, including its input conversion proc. (C
    // caches this in fcinfo->flinfo->fn_extra; we resolve it per call.)
    let meta = lsyscache::get_array_element_io_data::call(element_type, ArrayIoFuncSelector::Input)?;
    let typlen = meta.typlen as i32;
    let typbyval = meta.typbyval;
    let typalign = meta.typalign;

    // Initialize dim[]/lBound[] for ReadArrayStr in case there is no explicit
    // dimension info (ReadArrayDimensions overwrites them if there is).
    let mut p: &[u8] = string.as_bytes();

    let (mut ndim, mut dim, l_bound) = read_array_dimensions(&mut p, string)?;

    if ndim == 0 {
        // No array dimensions, so next character should be a left brace.
        if cur(p) != b'{' {
            return Err(malformed(
                string,
                "Array value must start with \"{\" or dimension information.",
            ));
        }
    } else {
        // If array dimensions are given, expect '=' operator.
        if !p.starts_with(ASSGN) {
            return Err(malformed(
                string,
                &format!(
                    "Missing \"{}\" after array dimensions.",
                    core::str::from_utf8(ASSGN).unwrap()
                ),
            ));
        }
        for _ in 0..ASSGN.len() {
            bump(&mut p);
        }
        // Allow whitespace after it.
        while scanner_isspace(cur(p)) {
            bump(&mut p);
        }
        if cur(p) != b'{' {
            return Err(malformed(string, "Array contents must start with \"{\"."));
        }
    }

    // Parse the value part, in the curly braces: { ... }
    let (values, nulls) =
        match read_array_str(mcx, &mut p, &meta, typmod, string, &mut ndim, &mut dim)? {
            Some(v) => v,
            None => return Ok(PgVec::new_in(mcx)),
        };
    let nitems = values.len() as i32;

    // Only whitespace is allowed after the closing brace.
    while cur(p) != 0 {
        let c = cur(p);
        bump(&mut p);
        if !scanner_isspace(c) {
            return Err(malformed(string, "Junk after closing right brace."));
        }
    }

    // Empty array?
    if nitems == 0 {
        return crate::construct::construct_empty_array(mcx, element_type);
    }

    // Check for nulls, compute total data space needed.
    let mut hasnulls = false;
    let mut nbytes: usize = 0;
    for i in 0..nitems as usize {
        if nulls[i] {
            hasnulls = true;
        } else {
            // let's just make sure data is not toasted (C: typlen == -1 ->
            // PointerGetDatum(PG_DETOAST_DATUM(values[i]))). The element input
            // function returns a fully detoasted varlena Datum already (it
            // allocates a freshly-built value, never an external pointer), so
            // the detoast is a no-op here, exactly as it is in C for an
            // input-function result.
            nbytes = att_addlength_datum(nbytes, typlen, values[i]);
            nbytes = foundation::att_align_nominal(nbytes, typalign);
            // Check for overflow of total request.
            if nbytes > MAX_ALLOC_SIZE {
                return Err(size_exceeds_alloc_error());
            }
        }
    }

    let dataoffset: i32;
    if hasnulls {
        let off = foundation::arr_overhead_withnulls(ndim, nitems);
        dataoffset = off as i32;
        nbytes += off;
    } else {
        dataoffset = 0; // marker for no null bitmap
        nbytes += foundation::arr_overhead_nonulls(ndim);
    }

    // Construct the final array datum (palloc0).
    let mut retval: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    if retval.try_reserve(nbytes).is_err() {
        return Err(mcx.oom(nbytes));
    }
    retval.resize(nbytes, 0);

    foundation::set_header(&mut retval, nbytes, ndim, dataoffset, element_type);
    foundation::write_dims(&mut retval, &dim[..ndim as usize]);
    foundation::write_lbounds(&mut retval, ndim, &l_bound[..ndim as usize]);

    let nulls_arg = if hasnulls { Some(&nulls[..]) } else { None };
    copy_array_els(
        &mut retval,
        &values,
        nulls_arg,
        nitems,
        typlen,
        typbyval,
        typalign,
        true,
    )?;

    Ok(retval)
}

/// `ReadArrayDimensions(&srcptr, &ndim, dim, lBound, origStr, escontext)`
/// (arrayfuncs.c) — parse the optional leading `[l:u]...` dimension box.
/// Returns `(ndim, dim, lBound)`; with `ndim == 0` the C defaults (`-1` / `1`)
/// are returned and `*srcptr` is left at the first non-dimension byte.
#[allow(clippy::type_complexity)]
fn read_array_dimensions(
    cur_ptr: &mut &[u8],
    orig_str: &str,
) -> PgResult<(i32, [i32; MAX_DIM as usize], [i32; MAX_DIM as usize])> {
    let mut p: &[u8] = cur_ptr;
    let mut ndim = 0i32;
    let mut dim = [-1i32; MAX_DIM as usize];
    let mut l_bound = [1i32; MAX_DIM as usize];

    // One iteration per [n] or [m:n] dimension item.
    loop {
        // Whitespace is allowed between, but not within, dimension items.
        while scanner_isspace(cur(p)) {
            bump(&mut p);
        }
        if cur(p) != b'[' {
            break; // no more dimension items
        }
        bump(&mut p);
        if ndim >= MAX_DIM {
            return Err(PgError::error(format!(
                "number of array dimensions exceeds the maximum allowed ({})",
                MAX_DIM
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }

        let qlen = p.len();
        let i = read_dimension_int(&mut p)?;
        if p.len() == qlen {
            // no digits?
            return Err(malformed(
                orig_str,
                "\"[\" must introduce explicitly-specified array dimensions.",
            ));
        }

        let ub;
        if cur(p) == b':' {
            // [m:n] format
            l_bound[ndim as usize] = i;
            bump(&mut p);
            let qlen2 = p.len();
            let v = read_dimension_int(&mut p)?;
            if p.len() == qlen2 {
                // no digits?
                return Err(malformed(orig_str, "Missing array dimension value."));
            }
            ub = v;
        } else {
            // [n] format
            l_bound[ndim as usize] = 1;
            ub = i;
        }
        if cur(p) != b']' {
            return Err(malformed(orig_str, "Missing \"]\" after array dimensions."));
        }
        bump(&mut p);

        // ub < lb is rejected (a zero-length dimension would yield an empty
        // array we keep no dimension data for).
        if ub < l_bound[ndim as usize] {
            return Err(PgError::error("upper bound cannot be less than lower bound")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
        }

        // Upper bound of INT_MAX must be disallowed, cf ArrayCheckBounds().
        if ub == i32::MAX {
            return Err(
                PgError::error(format!("array upper bound is too large: {}", ub))
                    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            );
        }

        // Compute "ub - lBound[ndim] + 1", detecting overflow.
        let span = ub
            .checked_sub(l_bound[ndim as usize])
            .and_then(|x| x.checked_add(1));
        let span = match span {
            Some(s) => s,
            None => return Err(size_exceeds_array_error()),
        };

        dim[ndim as usize] = span;
        ndim += 1;
    }

    *cur_ptr = p;
    Ok((ndim, dim, l_bound))
}

/// `ReadDimensionInt(&srcptr, &result, origStr, escontext)` (arrayfuncs.c) —
/// parse one signed dimension integer. Returns the parsed value (with `*srcptr`
/// advanced past the digits) or, when there are no digits, `0` with `*srcptr`
/// unchanged.
fn read_dimension_int(cur_ptr: &mut &[u8]) -> PgResult<i32> {
    let p: &[u8] = cur_ptr;
    let c = cur(p);
    // Don't accept leading whitespace.
    if !c.is_ascii_digit() && c != b'-' && c != b'+' {
        return Ok(0);
    }

    // strtol(p, srcptr, 10): optional sign + decimal digits.
    let start_len = p.len();
    let mut sp: &[u8] = p;
    if cur(sp) == b'-' || cur(sp) == b'+' {
        bump(&mut sp);
    }
    while cur(sp).is_ascii_digit() {
        bump(&mut sp);
    }
    let consumed = start_len - sp.len();
    let s = core::str::from_utf8(&p[..consumed]).unwrap_or("");

    match s.parse::<i64>() {
        Ok(l) if l >= i32::MIN as i64 && l <= i32::MAX as i64 => {
            *cur_ptr = sp;
            Ok(l as i32)
        }
        _ => Err(PgError::error("array bound is out of integer range")
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)),
    }
}

/// `ReadArrayStr(...)` (arrayfuncs.c) — parse the `{...}` element body,
/// producing element values/nulls and the inferred/validated dimensions.
/// `*ndim_p` / `dim[]` are in/out. Returns `None` on a saved soft error (the C
/// `false` return).
#[allow(clippy::type_complexity)]
fn read_array_str<'mcx>(
    mcx: Mcx<'mcx>,
    cur_ptr: &mut &[u8],
    meta: &ArrayElementIoData,
    typmod: i32,
    orig_str: &str,
    ndim_p: &mut i32,
    dim: &mut [i32; MAX_DIM as usize],
) -> PgResult<Option<(PgVec<'mcx, Datum>, PgVec<'mcx, bool>)>> {
    let typdelim = meta.typdelim;

    let mut values: PgVec<'mcx, Datum> = PgVec::new_in(mcx);
    let mut nulls: PgVec<'mcx, bool> = PgVec::new_in(mcx);

    // Per-element de-escape scratch (the C `StringInfoData elembuf`).
    let mut elembuf: Vec<u8> = Vec::new();

    let mut ndim = *ndim_p;
    let dimensions_specified = ndim != 0;

    debug_assert_eq!(cur(cur_ptr), b'{'); // loop assumes first token is LEVEL_START

    let mut p: &[u8] = cur_ptr;
    let mut nest_level = 0i32;
    let mut ndim_frozen = dimensions_specified;
    let mut expect_delim = false;
    let mut nelems = [0i32; MAX_DIM as usize];

    loop {
        let tok = match read_array_token(&mut p, &mut elembuf, typdelim, orig_str)? {
            Some(t) => t,
            None => return Ok(None), // ATOK_ERROR: soft error already saved
        };

        match tok {
            ArrayTok::LevelStart => {
                // Can't write left brace where a delim is expected.
                if expect_delim {
                    return Err(malformed(orig_str, "Unexpected \"{\" character."));
                }
                // Initialize element counting in the new level.
                if nest_level >= MAX_DIM {
                    return Err(PgError::error(format!(
                        "number of array dimensions exceeds the maximum allowed ({})",
                        MAX_DIM
                    ))
                    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
                }
                nelems[nest_level as usize] = 0;
                nest_level += 1;
                if nest_level > ndim {
                    // Can't increase ndim once it's frozen.
                    if ndim_frozen {
                        return dimension_error(dimensions_specified, orig_str);
                    }
                    ndim = nest_level;
                }
            }
            ArrayTok::LevelEnd => {
                // Can't get here with nest_level == 0.
                debug_assert!(nest_level > 0);
                // Allow a right brace to terminate an empty sub-array; otherwise
                // it must occur where we expect a delimiter.
                if nelems[(nest_level - 1) as usize] > 0 && !expect_delim {
                    return Err(malformed(orig_str, "Unexpected \"}\" character."));
                }
                nest_level -= 1;
                // Nested sub-arrays count as elements of the outer level.
                if nest_level > 0 {
                    nelems[(nest_level - 1) as usize] += 1;
                }
                // Check/record this level's length.
                if dim[nest_level as usize] < 0 {
                    dim[nest_level as usize] = nelems[nest_level as usize];
                } else if nelems[nest_level as usize] != dim[nest_level as usize] {
                    return dimension_error(dimensions_specified, orig_str);
                }
                // Must have a delim or another right brace following.
                expect_delim = true;
            }
            ArrayTok::Delim => {
                if !expect_delim {
                    return Err(malformed(
                        orig_str,
                        &format!("Unexpected \"{}\" character.", typdelim as char),
                    ));
                }
                expect_delim = false;
            }
            ArrayTok::Elem | ArrayTok::ElemNull => {
                // Can't get here with nest_level == 0.
                debug_assert!(nest_level > 0);
                // Disallow consecutive ELEM tokens.
                if expect_delim {
                    return Err(malformed(orig_str, "Unexpected array element."));
                }

                // Enlarge the values/nulls arrays if needed (C grows by doubling
                // up to MaxArraySize).
                if values.len() >= MAX_ARRAY_SIZE {
                    return Err(size_exceeds_array_error());
                }

                let is_null = matches!(tok, ArrayTok::ElemNull);

                // Read the element's value, or check that NULL is allowed.
                let value = if is_null {
                    input_function_call_safe(meta, None, typmod)?
                } else {
                    let s = core::str::from_utf8(&elembuf).map_err(|_| {
                        PgError::error("invalid byte sequence for encoding")
                            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                    })?;
                    input_function_call_safe(meta, Some(s), typmod)?
                };
                let value = match value {
                    Some(v) => v,
                    None => return Ok(None), // soft error reported by input proc
                };

                if values.try_reserve(1).is_err() || nulls.try_reserve(1).is_err() {
                    return Err(size_exceeds_array_error());
                }
                values.push(value);
                nulls.push(is_null);

                // Once an element is found, ndim can no longer increase and all
                // subsequent elements must be at the same nesting depth.
                ndim_frozen = true;
                if nest_level != ndim {
                    return dimension_error(dimensions_specified, orig_str);
                }
                // Count the new element.
                nelems[(nest_level - 1) as usize] += 1;
                // Must have a delim or a right brace following.
                expect_delim = true;
            }
        }

        if nest_level <= 0 {
            break;
        }
    }

    *cur_ptr = p;
    *ndim_p = ndim;
    Ok(Some((values, nulls)))
}

fn dimension_error<T>(dimensions_specified: bool, orig_str: &str) -> PgResult<T> {
    let detail = if dimensions_specified {
        "Specified array dimensions do not match array contents."
    } else {
        "Multidimensional arrays must have sub-arrays with matching dimensions."
    };
    Err(malformed(orig_str, detail))
}

/// `ArrayToken` (arrayfuncs.c): the `ReadArrayToken` return type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArrayTok {
    LevelStart,
    LevelEnd,
    Delim,
    Elem,
    ElemNull,
}

/// `ReadArrayToken(&srcptr, elembuf, typdelim, origStr, escontext)`
/// (arrayfuncs.c) — lex one token. On an `Elem`/`ElemNull` token the de-escaped
/// text is left in `elembuf`. Returns `None` on a saved soft error (the C
/// `ATOK_ERROR`).
fn read_array_token(
    cur_ptr: &mut &[u8],
    elembuf: &mut Vec<u8>,
    typdelim: u8,
    orig_str: &str,
) -> PgResult<Option<ArrayTok>> {
    let mut p: &[u8] = cur_ptr;
    elembuf.clear();

    // Identify token type. Loop advances over leading whitespace.
    loop {
        match cur(p) {
            0 => return ending_error(orig_str),
            b'{' => {
                bump(&mut p);
                *cur_ptr = p;
                return Ok(Some(ArrayTok::LevelStart));
            }
            b'}' => {
                bump(&mut p);
                *cur_ptr = p;
                return Ok(Some(ArrayTok::LevelEnd));
            }
            b'"' => {
                bump(&mut p);
                return read_quoted_element(cur_ptr, &mut p, elembuf, typdelim, orig_str);
            }
            c => {
                if c == typdelim {
                    bump(&mut p);
                    *cur_ptr = p;
                    return Ok(Some(ArrayTok::Delim));
                }
                if scanner_isspace(c) {
                    bump(&mut p);
                    continue;
                }
                return read_unquoted_element(cur_ptr, &mut p, elembuf, typdelim, orig_str);
            }
        }
    }
}

fn read_quoted_element<'a>(
    cur_ptr: &mut &'a [u8],
    p: &mut &'a [u8],
    elembuf: &mut Vec<u8>,
    typdelim: u8,
    orig_str: &str,
) -> PgResult<Option<ArrayTok>> {
    loop {
        match cur(p) {
            0 => return ending_error(orig_str),
            b'\\' => {
                // Skip backslash, copy next character as-is.
                bump(p);
                if cur(p) == 0 {
                    return ending_error(orig_str);
                }
                elembuf.push(cur(p));
                bump(p);
            }
            b'"' => {
                // Next non-whitespace must be typdelim or a brace, else the
                // element is incorrectly quoted. (A quoted element is never the
                // unquoted NULL literal, so this always yields ATOK_ELEM.)
                loop {
                    bump(p);
                    let c = cur(p);
                    if c == 0 {
                        break;
                    }
                    if c == typdelim || c == b'}' || c == b'{' {
                        *cur_ptr = *p;
                        return Ok(Some(ArrayTok::Elem));
                    }
                    if !scanner_isspace(c) {
                        return Err(malformed(orig_str, "Incorrectly quoted array element."));
                    }
                }
                return ending_error(orig_str);
            }
            c => {
                elembuf.push(c);
                bump(p);
            }
        }
    }
}

fn read_unquoted_element<'a>(
    cur_ptr: &mut &'a [u8],
    p: &mut &'a [u8],
    elembuf: &mut Vec<u8>,
    typdelim: u8,
    orig_str: &str,
) -> PgResult<Option<ArrayTok>> {
    // We don't include trailing whitespace in the result. dstlen tracks how much
    // of the output is known to not be trailing whitespace.
    let mut dstlen = 0usize;
    let mut has_escapes = false;
    loop {
        match cur(p) {
            0 => return ending_error(orig_str),
            b'{' => {
                return Err(malformed(orig_str, "Unexpected \"{\" character."));
            }
            b'"' => {
                // Must double-quote all or none of an element.
                return Err(malformed(orig_str, "Incorrectly quoted array element."));
            }
            b'\\' => {
                // Skip backslash, copy next character as-is.
                bump(p);
                if cur(p) == 0 {
                    return ending_error(orig_str);
                }
                elembuf.push(cur(p));
                bump(p);
                dstlen = elembuf.len(); // treat it as non-whitespace
                has_escapes = true;
            }
            c => {
                // End of elem?
                if c == typdelim || c == b'}' {
                    // hack: truncate the output string to dstlen
                    elembuf.truncate(dstlen);
                    *cur_ptr = *p;
                    // Check if it's an unquoted "NULL".
                    if ARRAY_NULLS
                        && !has_escapes
                        && elembuf.eq_ignore_ascii_case(b"NULL")
                    {
                        return Ok(Some(ArrayTok::ElemNull));
                    } else {
                        return Ok(Some(ArrayTok::Elem));
                    }
                }
                elembuf.push(c);
                if !scanner_isspace(c) {
                    dstlen = elembuf.len();
                }
                bump(p);
            }
        }
    }
}

fn ending_error(orig_str: &str) -> PgResult<Option<ArrayTok>> {
    Err(malformed(orig_str, "Unexpected end of input."))
}

/// `InputFunctionCallSafe(&inputproc, str, typioparam, typmod, escontext,
/// &result)` (fmgr.c): convert one element's text (or NULL) to a `Datum` using
/// the element type's input function, routed through the fmgr owner's seam.
/// Returns `None` on a saved soft error (the C `false` return).
fn input_function_call_safe(
    meta: &ArrayElementIoData,
    value: Option<&str>,
    typmod: i32,
) -> PgResult<Option<Datum>> {
    // C calls `InputFunctionCallSafe(proc, NULL, ...)` for a NULL element; the
    // fmgr seam takes `&str`, and a NULL element's returned value is discarded
    // (the null is recorded in the bitmap, never stored), so the empty string
    // stands in for the C NULL pointer here.
    let s = value.unwrap_or("");
    fmgr::input_function_call_safe::call(meta.typiofunc, s, meta.typioparam, typmod)
}

// ---------------------------------------------------------------------------
// Text output: array_out (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `pg_strcasecmp(s, "NULL") == 0`: ASCII case-insensitive compare.
fn eq_null_ci(s: &[u8]) -> bool {
    s.eq_ignore_ascii_case(b"NULL")
}

/// `array_out(v)` (arrayfuncs.c): render an array's external text form.
pub fn array_out<'mcx>(mcx: Mcx<'mcx>, array: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let element_type = foundation::arr_elemtype(array);

    // Get info about element type, including its output conversion proc.
    let meta = lsyscache::get_array_element_io_data::call(element_type, ArrayIoFuncSelector::Output)?;
    let typlen = meta.typlen as i32;
    let typbyval = meta.typbyval;
    let typalign = meta.typalign;
    let typdelim = meta.typdelim;

    let ndim = foundation::arr_ndim(array);
    let nitems = arrayutils::array_get_n_items::call(ndim, &dims_of(mcx, array, ndim)?)?;

    if nitems == 0 {
        let mut out: PgVec<'mcx, u8> = PgVec::new_in(mcx);
        out.try_reserve(2).map_err(|_| mcx.oom(2))?;
        out.extend_from_slice(b"{}");
        return Ok(out);
    }

    let dims = dims_of(mcx, array, ndim)?;
    let lb = lbounds_of(mcx, array, ndim)?;

    // We need explicit dimensions if any dimension has a lower bound != 1.
    let mut needdims = false;
    for i in 0..ndim as usize {
        if lb[i] != 1 {
            needdims = true;
            break;
        }
    }

    // Convert all values to string form, count total space needed (including
    // any overhead such as escaping backslashes), and detect whether each item
    // needs double quotes.
    let mut values: PgVec<'mcx, PgVec<'mcx, u8>> = vec_with_capacity_in(mcx, nitems as usize)?;
    let mut needquotes: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, nitems as usize)?;
    let mut overall_length: usize = 0;

    // array_iter over the flat varlena array: walk the data area element by
    // element, advancing only past non-NULL elements, consulting the null
    // bitmap (if any) per element.
    let nullbitmap = foundation::arr_nullbitmap_off(array);
    let data_off = foundation::arr_data_ptr_off(array);
    let mut data_ptr = data_off;

    for i in 0..nitems {
        let isnull = foundation::array_get_isnull(array, nullbitmap, i);

        let needquote;
        let mut valstr: PgVec<'mcx, u8> = PgVec::new_in(mcx);
        if isnull {
            valstr.try_reserve(4).map_err(|_| mcx.oom(4))?;
            valstr.extend_from_slice(b"NULL");
            overall_length += 4;
            needquote = false;
        } else {
            // array_iter_next: fetch the current element, then advance the data
            // pointer past it (only for non-NULL elements).
            let itemvalue = element_at(array, data_ptr, typbyval, typlen);
            let (new_off, _bm) = foundation::array_seek(
                array, data_ptr, None, 0, typlen, typbyval, typalign, 1,
            );
            data_ptr = new_off;

            let rendered = fmgr::array_output_function_call::call(mcx, meta.typiofunc, itemvalue)?;
            valstr = rendered;

            // count data plus backslashes; detect chars needing quotes.
            let mut nq = if valstr.is_empty() {
                true // force quotes for empty string
            } else {
                eq_null_ci(&valstr) // force quotes for literal NULL
            };

            for &ch in valstr.iter() {
                overall_length += 1;
                if ch == b'"' || ch == b'\\' {
                    nq = true;
                    overall_length += 1;
                } else if ch == b'{' || ch == b'}' || ch == typdelim || scanner_isspace(ch) {
                    nq = true;
                }
            }
            needquote = nq;
        }

        needquotes.push(needquote);

        // Count the pair of double quotes, if needed.
        if needquote {
            overall_length += 2;
        }
        // and the comma (or other typdelim delimiter).
        overall_length += 1;

        values.push(valstr);
    }

    // The very last array element doesn't have a typdelim delimiter after it,
    // but that's OK; that space is needed for the trailing '\0'. Now count
    // total number of curly brace pairs in output string.
    let mut j: i32 = 0;
    let mut k: i32 = 1;
    for i in 0..ndim as usize {
        j += k;
        k *= dims[i];
    }
    overall_length += 2 * j as usize;

    // Format explicit dimensions if required.
    let mut dims_str: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    if needdims {
        for i in 0..ndim as usize {
            let s = format!("[{}:{}]", lb[i], lb[i] + dims[i] - 1);
            dims_str.try_reserve(s.len()).map_err(|_| mcx.oom(s.len()))?;
            dims_str.extend_from_slice(s.as_bytes());
        }
        // *ptr++ = *ASSGN ('='); the trailing '\0' is implicit.
        dims_str.try_reserve(1).map_err(|_| mcx.oom(1))?;
        dims_str.push(b'=');
        overall_length += dims_str.len() + 1; // ptr - dims_str (incl. the '=')
    }

    // Now construct the output string. The C builds a `char *` of
    // `overall_length` bytes (incl. the trailing '\0'); here we build the bytes
    // without the terminating NUL.
    let mut p: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, overall_length)?;

    if needdims {
        p.extend_from_slice(&dims_str);
    }
    p.push(b'{');

    let mut indx = [0i32; MAX_DIM as usize];
    let mut j: i32 = 0;
    let mut k: usize = 0;
    loop {
        // for (i = j; i < ndim - 1; i++) APPENDCHAR('{');
        let mut i = j;
        while i < ndim - 1 {
            p.push(b'{');
            i += 1;
        }

        let elem = &values[k];
        if needquotes[k] {
            p.push(b'"');
            for &ch in elem.iter() {
                if ch == b'"' || ch == b'\\' {
                    p.push(b'\\');
                }
                p.push(ch);
            }
            p.push(b'"');
        } else {
            p.extend_from_slice(elem);
        }
        k += 1;

        // advance the multi-dim index from the rightmost dimension.
        let mut i = ndim - 1;
        while i >= 0 {
            indx[i as usize] += 1;
            if indx[i as usize] < dims[i as usize] {
                p.push(typdelim);
                break;
            } else {
                indx[i as usize] = 0;
                p.push(b'}');
                i -= 1;
            }
        }
        j = i;
        if j == -1 {
            break;
        }
    }

    Ok(p)
}

/// `ARR_DIMS(array)` collected into a `MAXDIM`-bounded slice.
fn dims_of<'mcx>(mcx: Mcx<'mcx>, array: &[u8], ndim: i32) -> PgResult<PgVec<'mcx, i32>> {
    let mut v: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, ndim.max(0) as usize)?;
    for i in 0..ndim.max(0) as usize {
        v.push(foundation::arr_dim(array, i));
    }
    Ok(v)
}

/// `ARR_LBOUND(array)` collected into a slice.
fn lbounds_of<'mcx>(mcx: Mcx<'mcx>, array: &[u8], ndim: i32) -> PgResult<PgVec<'mcx, i32>> {
    let mut v: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, ndim.max(0) as usize)?;
    for i in 0..ndim.max(0) as usize {
        v.push(foundation::arr_lbound(array, i));
    }
    Ok(v)
}

/// Materialize the element at byte offset `off` for the element-I/O seams: a
/// by-value element is `fetch_att`'d into a `ByValue(Datum)`; a by-reference
/// element crosses as `ByRef` of its on-disk bytes within the array buffer
/// (varlena incl. header for `typlen == -1`, fixed `typlen` bytes for
/// `typlen > 0`, NUL-terminated for `typlen == -2`).
fn element_at(array: &[u8], off: usize, typbyval: bool, typlen: i32) -> ArrayElementDatum<'_> {
    if typbyval {
        ArrayElementDatum::ByValue(foundation::fetch_att(array, off, typbyval, typlen))
    } else {
        // att_addlength_pointer(0, typlen, array, off) bytes starting at off.
        let len = foundation::att_addlength_pointer(0, typlen, array, off);
        ArrayElementDatum::ByRef(&array[off..off + len])
    }
}

// ---------------------------------------------------------------------------
// Binary I/O: array_recv / array_send + ReadArrayBinary (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// Network-byte-order reader over the message buffer, mirroring `pq_getmsg*`
/// over a `StringInfo` (`buf->data` / `buf->cursor` / `buf->len`).
struct MsgReader<'a> {
    data: &'a [u8],
    cursor: usize,
}

impl<'a> MsgReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        MsgReader { data, cursor: 0 }
    }

    /// `pq_getmsgint(buf, b)` for `b` of 1/2/4 bytes, network byte order.
    fn get_int(&mut self, b: usize) -> PgResult<u32> {
        if self.cursor + b > self.data.len() {
            return Err(insufficient_data());
        }
        let mut v: u32 = 0;
        for _ in 0..b {
            v = (v << 8) | self.data[self.cursor] as u32;
            self.cursor += 1;
        }
        Ok(v)
    }

    /// `buf->len - buf->cursor`.
    fn remaining(&self) -> usize {
        self.data.len() - self.cursor
    }
}

/// `array_recv(buf, spec_element_type, typmod)` (arrayfuncs.c): decode the
/// binary wire form of an array into on-disk `ArrayType` bytes.
pub fn array_recv<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &[u8],
    spec_element_type: Oid,
    typmod: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut msg = MsgReader::new(buf);

    // Get the array header information.
    let ndim = msg.get_int(4)? as i32;
    if ndim < 0 {
        // We do allow zero-dimension arrays.
        return Err(
            PgError::error(format!("invalid number of dimensions: {ndim}"))
                .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION),
        );
    }
    if ndim > MAX_DIM {
        return Err(PgError::error(format!(
            "number of array dimensions ({ndim}) exceeds the maximum allowed ({MAX_DIM})"
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
    }

    let flags = msg.get_int(4)?;
    if flags != 0 && flags != 1 {
        return Err(PgError::error("invalid array flags")
            .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION));
    }

    // Check element type recorded in the data.
    let mut element_type = msg.get_int(core::mem::size_of::<Oid>())?;

    // Complain about a type mismatch only when BOTH OIDs are built-in (stable);
    // otherwise carry on with the element type we "should" be getting.
    if element_type != spec_element_type {
        if element_type < FIRST_GENBKI_OBJECT_ID && spec_element_type < FIRST_GENBKI_OBJECT_ID {
            // C uses format_type_extended(..., FORMAT_TYPE_ALLOW_INVALID); the
            // format-type owner exposes format_type_be, which is the same
            // printable name (an existing built-in type is always resolvable).
            let got = format_type::format_type_be::call(mcx, element_type)?;
            let want = format_type::format_type_be::call(mcx, spec_element_type)?;
            return Err(PgError::error(format!(
                "binary data has array element type {element_type} ({}) instead of expected {spec_element_type} ({})",
                got.as_str(),
                want.as_str()
            ))
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
        }
        element_type = spec_element_type;
    }

    let mut dim = [0i32; MAX_DIM as usize];
    let mut l_bound = [0i32; MAX_DIM as usize];
    for i in 0..ndim as usize {
        dim[i] = msg.get_int(4)? as i32;
        l_bound[i] = msg.get_int(4)? as i32;
    }

    // This checks for overflow of array dimensions.
    let nitems = arrayutils::array_get_n_items::call(ndim, &dim[..ndim as usize])?;
    arrayutils::array_check_bounds::call(ndim, &dim[..ndim as usize], &l_bound[..ndim as usize])?;

    // Get info about element type, including its receive proc.
    let meta = lsyscache::get_array_element_io_data::call(element_type, ArrayIoFuncSelector::Receive)?;
    if meta.typiofunc == 0 {
        let name = format_type::format_type_be::call(mcx, element_type)?;
        return Err(PgError::error(format!(
            "no binary input function available for type {}",
            name.as_str()
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION));
    }

    if nitems == 0 {
        // Return empty array ... but not till we've validated element_type.
        return crate::construct::construct_empty_array(mcx, element_type);
    }

    let typlen = meta.typlen as i32;
    let typbyval = meta.typbyval;
    let typalign = meta.typalign;

    // dataPtr/nullsPtr = palloc(nitems * sizeof(...)). The reading itself goes
    // through the element type's binary receive function over the unread tail
    // of the message buffer.
    let mut rest: &[u8] = &buf[msg.cursor..];
    let (values, nulls) = read_array_binary(mcx, &mut rest, nitems, &meta, typmod)?;

    // Check for nulls, compute total data space needed (ReadArrayBinary tail).
    let mut hasnull = false;
    let mut nbytes: usize = 0;
    for i in 0..nitems as usize {
        if nulls[i] {
            hasnull = true;
        } else {
            // let's just make sure data is not toasted; the receive function's
            // result is a freshly-built (non-external) Datum, so the C
            // PG_DETOAST_DATUM is a no-op here.
            nbytes = att_addlength_datum(nbytes, typlen, values[i]);
            nbytes = foundation::att_align_nominal(nbytes, typalign);
            if nbytes > MAX_ALLOC_SIZE {
                return Err(size_exceeds_alloc_error());
            }
        }
    }

    let dataoffset: i32;
    if hasnull {
        let off = foundation::arr_overhead_withnulls(ndim, nitems);
        dataoffset = off as i32;
        nbytes += off;
    } else {
        dataoffset = 0; // marker for no null bitmap
        nbytes += foundation::arr_overhead_nonulls(ndim);
    }

    let mut retval: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    if retval.try_reserve(nbytes).is_err() {
        return Err(mcx.oom(nbytes));
    }
    retval.resize(nbytes, 0);
    foundation::set_header(&mut retval, nbytes, ndim, dataoffset, element_type);
    foundation::write_dims(&mut retval, &dim[..ndim as usize]);
    foundation::write_lbounds(&mut retval, ndim, &l_bound[..ndim as usize]);

    let nulls_arg = if hasnull { Some(&nulls[..]) } else { None };
    copy_array_els(
        &mut retval,
        &values,
        nulls_arg,
        nitems,
        typlen,
        typbyval,
        typalign,
        true,
    )?;

    Ok(retval)
}

/// `ReadArrayBinary(buf, nitems, ...)` (arrayfuncs.c): the per-element binary
/// reader invoked by `array_recv`. Decodes each element's `int4` length prefix
/// then dispatches to the element type's receive function over that slice (a
/// `-1` length is a NULL element).
fn read_array_binary<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut &[u8],
    nitems: i32,
    meta: &ArrayElementIoData,
    typmod: i32,
) -> PgResult<(PgVec<'mcx, Datum>, PgVec<'mcx, bool>)> {
    let mut msg = MsgReader::new(buf);

    let mut values: PgVec<'mcx, Datum> = vec_with_capacity_in(mcx, nitems as usize)?;
    let mut nulls: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, nitems as usize)?;

    for i in 0..nitems {
        // Get and check the item length.
        let itemlen = msg.get_int(4)? as i32;
        if itemlen < -1 || itemlen > msg.remaining() as i32 {
            return Err(insufficient_data());
        }

        if itemlen == -1 {
            // -1 length means NULL: ReceiveFunctionCall(receiveproc, NULL, ...).
            // (The receive function is called with an empty buffer; strict
            // element NULLs are recorded in the null bitmap, so the resulting
            // value is unused.)
            let value = fmgr::array_receive_function_call::call(
                meta.typiofunc,
                &[],
                meta.typioparam,
                typmod,
            )?;
            values.push(value);
            nulls.push(true);
            continue;
        }

        // Point a read-only StringInfo at the correct portion of the message
        // buffer rather than copying.
        let start = msg.cursor;
        let elem = &msg.data[start..start + itemlen as usize];
        msg.cursor += itemlen as usize;

        // Now call the element's receiveproc.
        let value = fmgr::array_receive_function_call::call(
            meta.typiofunc,
            elem,
            meta.typioparam,
            typmod,
        )?;
        values.push(value);
        nulls.push(false);

        // The C checks `elem_buf.cursor != itemlen` ("improper binary format in
        // array element %d") to verify the receive proc consumed the whole
        // element; that cursor lives inside the fmgr owner's receive call and is
        // verified there.
        let _ = i;
    }

    *buf = &buf[msg.cursor..];
    Ok((values, nulls))
}

/// `array_send(v)` (arrayfuncs.c): encode an array into its binary wire form.
pub fn array_send<'mcx>(mcx: Mcx<'mcx>, array: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let element_type = foundation::arr_elemtype(array);

    // Get info about element type, including its send proc.
    let meta = lsyscache::get_array_element_io_data::call(element_type, ArrayIoFuncSelector::Send)?;
    if meta.typiofunc == 0 {
        let name = format_type::format_type_be::call(mcx, element_type)?;
        return Err(PgError::error(format!(
            "no binary output function available for type {}",
            name.as_str()
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION));
    }
    let typlen = meta.typlen as i32;
    let typbyval = meta.typbyval;
    let typalign = meta.typalign;

    let ndim = foundation::arr_ndim(array);
    let dims = dims_of(mcx, array, ndim)?;
    let lb = lbounds_of(mcx, array, ndim)?;
    let nitems = arrayutils::array_get_n_items::call(ndim, &dims)?;

    // pq_begintypsend: accumulate the bytea payload (the libpq owner wraps the
    // varlena framing around pq_endtypsend).
    let mut sendbuf: PgVec<'mcx, u8> = PgVec::new_in(mcx);

    // Send the array header information.
    send_int32(mcx, &mut sendbuf, ndim as u32)?;
    send_int32(
        mcx,
        &mut sendbuf,
        if foundation::arr_hasnull(array) { 1 } else { 0 },
    )?;
    send_int32(mcx, &mut sendbuf, element_type)?;
    for i in 0..ndim as usize {
        send_int32(mcx, &mut sendbuf, dims[i] as u32)?;
        send_int32(mcx, &mut sendbuf, lb[i] as u32)?;
    }

    // Send the array elements using the element's own sendproc. The C uses
    // array_iter over the flat (non-expanded) array; walk the data area with the
    // low-level helpers, which is what array_iter_next reduces to for a plain
    // varlena array.
    let nullbitmap = foundation::arr_nullbitmap_off(array);
    let mut data_ptr = foundation::arr_data_ptr_off(array);

    for i in 0..nitems {
        let isnull = foundation::array_get_isnull(array, nullbitmap, i);
        if isnull {
            // -1 length means a NULL.
            send_int32(mcx, &mut sendbuf, (-1i32) as u32)?;
        } else {
            let itemvalue = element_at(array, data_ptr, typbyval, typlen);
            let (new_off, _bm) = foundation::array_seek(
                array, data_ptr, None, 0, typlen, typbyval, typalign, 1,
            );
            data_ptr = new_off;

            // outputbytes = SendFunctionCall(&proc, itemvalue) (fmgr.c); the
            // seam returns the bytea payload with the varlena header stripped.
            let outputbytes = fmgr::array_send_function_call::call(mcx, meta.typiofunc, itemvalue)?;
            send_int32(mcx, &mut sendbuf, outputbytes.len() as u32)?;
            send_bytes(mcx, &mut sendbuf, &outputbytes)?;
        }
    }

    Ok(sendbuf)
}

// ---------------------------------------------------------------------------
// pq_send* wire framing (pure big-endian byte appends; the libpq owner just
// wraps these around a StringInfo).
// ---------------------------------------------------------------------------

/// `pq_sendint32(buf, i)` — append a 4-byte network-byte-order integer.
fn send_int32<'mcx>(mcx: Mcx<'mcx>, buf: &mut PgVec<'mcx, u8>, i: u32) -> PgResult<()> {
    buf.try_reserve(4).map_err(|_| mcx.oom(4))?;
    buf.extend_from_slice(&i.to_be_bytes());
    Ok(())
}

/// `pq_sendbytes(buf, data, datalen)` — append raw bytes.
fn send_bytes<'mcx>(mcx: Mcx<'mcx>, buf: &mut PgVec<'mcx, u8>, data: &[u8]) -> PgResult<()> {
    buf.try_reserve(data.len()).map_err(|_| mcx.oom(data.len()))?;
    buf.extend_from_slice(data);
    Ok(())
}
