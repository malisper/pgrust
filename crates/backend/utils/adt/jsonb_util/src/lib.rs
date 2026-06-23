//! Idiomatic Rust port of PostgreSQL's `jsonb_util.c` -- the on-disk `jsonb`
//! serialization/deserialization engine plus the in-memory `JsonbValue` tree.
//!
//! Mirrors `postgres-18.3/src/backend/utils/adt/jsonb_util.c`.
//!
//! The on-disk ABI types (`Jsonb`, `JsonbContainer`, `JEntry`, the
//! `jbvType`/`JsonbIteratorToken`/`JsonbIterState` enums, and every flag
//! constant) live in [`::types_jsonb::jsonb`] and are re-exported here.  The
//! *in-memory* working types (`JsonbValue`, `JsonbPair`, `JsonbParseState`,
//! `JsonbIterator`) live in [`types_jsonb::jsonb_util`]: they
//! are idiomatic owned-tree
//! Rust types -- never stored on disk, never across a C ABI boundary -- using
//! owned `Vec`s and byte offsets rather than the C unions / raw pointers.  In
//! particular the on-disk container bytes are carried as an owned `Vec<u8>` (the
//! `jbvBinary`/iterator payload) and offsets into those bytes replace the C
//! `char *` cursors.
//!
//! There is ZERO `extern "C"`, raw pointer, or `libc` here.  Genuinely-external
//! operations are routed through the owning crates' per-owner seam crates:
//! `jbvDatetime` rendering via `json.c` (`backend-utils-adt-json-seams`),
//! `numeric` equality/compare via `numeric.c`
//! (`backend-utils-adt-numeric-seams`), collation-aware string compare via
//! `varlena.c` (`backend-utils-adt-varlena-seams`), the recursion guard via
//! `stack_depth.c` (`backend-utils-misc-stack-depth-seams`), and the byte-hash
//! primitives via `common/hashfn.c` (`common-hashfn-seams`).  Hashing of the
//! on-disk `numeric` bytes (the `hash_numeric` digit walk) is ported in-crate
//! against the `common-hashfn` seams and the `types-numeric` byte accessors.
//!
//! Allocation safety: every data-derived growth (`convertToJsonb`'s buffer, the
//! container-byte copies in `fillJsonbValue`, the push API, the iterator
//! placeholder vectors, `JsonbDeepContains`' temp array) reserves fallibly with
//! `try_reserve` against a validated bound (`JENTRY_OFFLENMASK` / `MaxAllocSize`
//! / the count fields already validated against `JSONB_MAX_*`) and surfaces OOM
//! as a recoverable `PgError` rather than aborting.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// `clippy::result_large_err`: every fallible function here returns the shared
// `PgResult` (== `Result<_, PgError>`).  This is the project-wide error contract
// these ports must match -- boxing it locally would diverge from every sibling
// crate's signatures.
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::string::ToString;
use alloc::vec::Vec;

use utils_error::{PgError, PgResult};
use mcx::{Mcx, PgVec};
use types_error::{
    ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE, ERRCODE_INTERNAL_ERROR, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};

// ---------------------------------------------------------------------------
// Re-exports: the on-disk jsonb ABI surface comes from the shared `types-jsonb`
// crate so the whole subsystem speaks one vocabulary.
// ---------------------------------------------------------------------------

pub use types_jsonb::jsonb_util::{
    JsonbDatetime, JsonbIterator, JsonbNumeric, JsonbPair, JsonbParseState, JsonbValue,
    JsonbValueData,
};
pub use ::types_jsonb::jsonb::{
    is_a_jsonb_scalar, jbe_has_off, jbe_isbool_false, jbe_isbool_true, jbe_iscontainer, jbe_isnull,
    jbe_isnumeric, jbe_isstring, jbe_offlenfld, jbvType, json_container_is_array,
    json_container_is_object, json_container_is_scalar, json_container_size, JEntry, Jsonb,
    JsonbContainer, JsonbIterState, JsonbIteratorToken, JB_CMASK, JB_FARRAY, JB_FOBJECT,
    JB_FSCALAR, JB_OFFSET_STRIDE, JENTRY_HAS_OFF, JENTRY_ISBOOL_FALSE, JENTRY_ISBOOL_TRUE,
    JENTRY_ISCONTAINER, JENTRY_ISNULL, JENTRY_ISNUMERIC, JENTRY_ISSTRING, JENTRY_OFFLENMASK,
    JENTRY_TYPEMASK,
};
pub use ::types_jsonb::VARHDRSZ;

use jbvType::*;
use JsonbIterState::*;
use JsonbIteratorToken::*;

/// Install this crate's seams.  `jsonb_util.c` declares no functions that other
/// crates must call back into across a cycle (it only depends one-way on
/// `json.c`/`numeric.c`/`varlena.c`/`stack_depth.c`/`hashfn.c` through their
/// owners' seam crates), so it owns no seam crate and installs nothing here.
/// The slot exists for the uniform `seams-init` call shape.
pub fn init_seams() {}

/// `JSONB_MAX_ELEMS` / `JSONB_MAX_PAIRS` upper bound (jsonb_util.c:36-37).
///
/// C: `Min(MaxAllocSize / sizeof(JsonbValue|JsonbPair), JB_CMASK)`. The `Min`
/// collapses to the `MaxAllocSize` term (NOT `JB_CMASK`): with `MaxAllocSize`
/// = 0x3fffffff = 1073741823, `sizeof(JsonbValue)` = 32 and `sizeof(JsonbPair)`
/// = 72, that is 1073741823/32 = 33554431 elems and 1073741823/72 = 14913080
/// pairs, both below `JB_CMASK` (268435455). These also appear in the
/// "number of jsonb {array elements, object pairs} exceeds the maximum" errmsg.
pub const JSONB_MAX_ELEMS: usize = 33554431;
pub const JSONB_MAX_PAIRS: usize = 14913080;

/// `MAXDATELEN` (datetime.h:200), used to size the `jbvDatetime` render buffer.
pub const MAXDATELEN: usize = 128;

/// `MaxAllocSize` (memutils.h) -- the 1 GB palloc ceiling used to validate any
/// data-derived allocation before reserving.
const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;

/// `DEFAULT_COLLATION_OID` (pg_collation_d.h:80) -- the database default
/// collation used for `jbvString` B-tree comparison.
const DEFAULT_COLLATION_OID: u32 = 100;

/// `INTALIGN(LEN)` (c.h:777) -- round up to a 4-byte (`ALIGNOF_INT`) boundary.
#[inline]
pub const fn intalign(len: usize) -> usize {
    (len + 3) & !3
}

// ---------------------------------------------------------------------------
// Allocation-safety helpers (HARD RULE: data-derived growth reserves fallibly
// against a validated bound).
// ---------------------------------------------------------------------------

/// Out-of-memory error mirroring C's `MemoryContextAlloc` failure.
fn oom() -> PgError {
    PgError::error("out of memory").with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

/// Fallibly copy a `&[u8]` to an owned `Vec<u8>`.  The length is already a
/// sub-slice of existing in-memory bytes (validated by the slice index), so the
/// only failure mode is allocation; we `try_reserve` and surface OOM.
fn slice_to_vec(src: &[u8]) -> PgResult<Vec<u8>> {
    let mut v = Vec::new();
    v.try_reserve_exact(src.len()).map_err(|_| oom())?;
    v.extend_from_slice(src);
    Ok(v)
}

// ---------------------------------------------------------------------------
// On-disk byte helpers (replace the C pointer arithmetic over JsonbContainer).
// ---------------------------------------------------------------------------

/// Read the container header word from container bytes.
#[inline]
fn container_header(jc: &[u8]) -> u32 {
    u32::from_ne_bytes([jc[0], jc[1], jc[2], jc[3]])
}

/// Read the `index`-th `JEntry` child word from container bytes.
#[inline]
fn container_child(jc: &[u8], index: usize) -> JEntry {
    let off = 4 + index * 4;
    u32::from_ne_bytes([jc[off], jc[off + 1], jc[off + 2], jc[off + 3]])
}

/// `VARDATA_ANY` offset for an inline (non-compressed, non-external) `jsonb`
/// varlena image: a short (1-byte, low-bit-set) header skips ONE byte, an
/// ordinary 4-byte header skips `VARHDRSZ`. C's `DatumGetJsonbP`
/// (`PG_DETOAST_DATUM`) un-packs a short header to 4-byte before `&jb->root`,
/// but pgrust's exec / subscripting boundaries reach this with a still-packed
/// image (`pg_detoast_datum_packed` keeps a short header short, and the
/// EEOP_FUNCEXPR boundary never detoasts), so a fixed `VARHDRSZ` strip would
/// land three bytes into the root container once `SHORT_VARLENA_PACKING` is on.
/// No-op while the flag is off (every stored value is 4-byte) and for
/// freshly-built jsonb (`JsonbValueToJsonb` / parse results are always 4-byte).
#[inline]
fn jsonb_vardata_off(jsonb: &[u8]) -> usize {
    match jsonb.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => 1,
        _ => VARHDRSZ,
    }
}

// ---------------------------------------------------------------------------
// JsonbToJsonbValue / JsonbValueToJsonb
// ---------------------------------------------------------------------------

/// C: `JsonbToJsonbValue(Jsonb *jsonb, JsonbValue *val)`.
///
/// `jsonb` is the full on-disk varlena bytes (length header + root container).
pub fn JsonbToJsonbValue(jsonb: &[u8], val: &mut JsonbValue) -> PgResult<()> {
    // val->val.binary.data = &jsonb->root; len = VARSIZE(jsonb) - VARHDRSZ.
    // The varlena header is 1 byte (short) or 4 bytes (long): strip exactly the
    // header form's size so the root container starts at the right byte and its
    // length excludes only the header actually present.
    let off = jsonb_vardata_off(jsonb);
    let len = (jsonb.len() - off) as i32;
    val.typ = jbvBinary;
    val.val = JsonbValueData::Binary {
        len,
        data: slice_to_vec(&jsonb[off..])?,
        // Document root: its container is at offset 0 within itself.
        offset: 0,
    };
    Ok(())
}

/// C: `JsonbValueToJsonb(JsonbValue *val)` -- serialize to an on-disk Jsonb
/// varlena allocated in `mcx` (C: `palloc` in `CurrentMemoryContext`), returned
/// as owned bytes including the varlena length header.
pub fn JsonbValueToJsonb<'mcx>(mcx: Mcx<'mcx>, val: &JsonbValue) -> PgResult<PgVec<'mcx, u8>> {
    if val.is_scalar() {
        // Scalar value: wrap in a one-element raw-scalar array.
        let mut pstate: Option<Box<JsonbParseState>> = None;
        let scalar_array = JsonbValue {
            typ: jbvArray,
            val: JsonbValueData::Array {
                elems: Vec::new(),
                raw_scalar: true,
            },
        };
        pushJsonbValue(&mut pstate, WJB_BEGIN_ARRAY, Some(&scalar_array))?;
        pushJsonbValue(&mut pstate, WJB_ELEM, Some(val))?;
        let res = pushJsonbValue(&mut pstate, WJB_END_ARRAY, None)?
            .ok_or_else(|| PgError::error("WJB_END_ARRAY yields a container value"))?;
        convertToJsonb(mcx, &res)
    } else if matches!(val.typ, jbvObject | jbvArray) {
        convertToJsonb(mcx, val)
    } else {
        debug_assert_eq!(val.typ, jbvBinary);
        let (len, data) = match &val.val {
            JsonbValueData::Binary { len, data, .. } => (*len as usize, data),
            _ => unreachable!("jbvBinary must carry Binary payload"),
        };
        if VARHDRSZ + len > MAX_ALLOC_SIZE {
            return Err(oom());
        }
        let mut out = ::mcx::vec_with_capacity_in(mcx, VARHDRSZ + len)?;
        out.extend_from_slice(&[0u8; VARHDRSZ]);
        out.extend_from_slice(&data[..len]);
        set_varsize(&mut out, VARHDRSZ + len);
        Ok(out)
    }
}

/// `SET_VARSIZE(ptr, len)` for a 4-byte (uncompressed) varlena header in
/// native byte order (matches the FFI varlena read paths).
#[inline]
fn set_varsize(buf: &mut [u8], total_len: usize) {
    let header = (total_len as u32) << 2;
    buf[..VARHDRSZ].copy_from_slice(&header.to_ne_bytes());
}

// ---------------------------------------------------------------------------
// getJsonbOffset / getJsonbLength
// ---------------------------------------------------------------------------

/// C: `getJsonbOffset(const JsonbContainer *jc, int index)`.
pub fn getJsonbOffset(jc: &[u8], index: i32) -> u32 {
    let mut offset: u32 = 0;
    let mut i = index - 1;
    while i >= 0 {
        let child = container_child(jc, i as usize);
        offset = offset.wrapping_add(jbe_offlenfld(child));
        if jbe_has_off(child) {
            break;
        }
        i -= 1;
    }
    offset
}

/// C: `getJsonbLength(const JsonbContainer *jc, int index)`.
pub fn getJsonbLength(jc: &[u8], index: i32) -> u32 {
    let child = container_child(jc, index as usize);
    if jbe_has_off(child) {
        let off = getJsonbOffset(jc, index);
        jbe_offlenfld(child) - off
    } else {
        jbe_offlenfld(child)
    }
}

// ---------------------------------------------------------------------------
// fillJsonbValue
// ---------------------------------------------------------------------------

/// C: `fillJsonbValue(JsonbContainer *container, int index, char *base_addr,
/// uint32 offset, JsonbValue *result)`.
///
/// `container` is the container bytes; `base_addr` is supplied as the byte
/// offset of `dataProper` within `container` so the C `base_addr + offset`
/// pointer arithmetic becomes a slice index.
///
/// `parent_doc_offset` is the byte position of `container` within the root
/// container of its origin document (0 when `container` is the document root,
/// or when the caller does not track document identity).  It is threaded into
/// any `jbvBinary` child produced so the document-relative `offset` field is
/// preserved for `.keyvalue()` ids; it has no effect on scalar children.
fn fillJsonbValue(
    container: &[u8],
    index: usize,
    data_proper: usize,
    offset: u32,
    parent_doc_offset: i32,
    result: &mut JsonbValue,
) -> PgResult<()> {
    let entry = container_child(container, index);
    let base = data_proper + offset as usize;

    if jbe_isnull(entry) {
        result.typ = jbvNull;
        result.val = JsonbValueData::Null;
    } else if jbe_isstring(entry) {
        let len = getJsonbLength(container, index as i32) as usize;
        result.typ = jbvString;
        result.val = JsonbValueData::String(slice_to_vec(&container[base..base + len])?);
    } else if jbe_isnumeric(entry) {
        // result->val.numeric = (Numeric)(base_addr + INTALIGN(offset)).
        let nstart = data_proper + intalign(offset as usize);
        // The numeric is itself a varlena; copy from its header through the
        // node's end (length minus the alignment padding).
        let total_len = getJsonbLength(container, index as i32) as usize;
        let pad = intalign(offset as usize) - offset as usize;
        let numlen = total_len - pad;
        result.typ = jbvNumeric;
        result.val = JsonbValueData::Numeric(slice_to_vec(&container[nstart..nstart + numlen])?);
    } else if jbe_isbool_true(entry) {
        result.typ = jbvBool;
        result.val = JsonbValueData::Bool(true);
    } else if jbe_isbool_false(entry) {
        result.typ = jbvBool;
        result.val = JsonbValueData::Bool(false);
    } else {
        debug_assert!(jbe_iscontainer(entry));
        // Remove alignment padding from data pointer and length.
        let cstart = data_proper + intalign(offset as usize);
        let total_len = getJsonbLength(container, index as i32) as usize;
        let clen = total_len - (intalign(offset as usize) - offset as usize);
        result.typ = jbvBinary;
        result.val = JsonbValueData::Binary {
            len: clen as i32,
            data: slice_to_vec(&container[cstart..cstart + clen])?,
            // Document-relative position = parent's document position plus the
            // child container's byte position within the parent container.
            // Mirrors C's pointer `base_addr + INTALIGN(offset)` being an
            // absolute address inside the same document buffer.
            offset: parent_doc_offset + cstart as i32,
        };
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// pushJsonbValue / pushJsonbValueScalar / pushState / append*
// ---------------------------------------------------------------------------

/// C: `pushJsonbValue(JsonbParseState **pstate, JsonbIteratorToken seq,
/// JsonbValue *jbval)`.
///
/// Returns the container value when a frame is closed (mirrors the C return of
/// `&(*pstate)->contVal`), else `None`.
pub fn pushJsonbValue(
    pstate: &mut Option<Box<JsonbParseState>>,
    seq: JsonbIteratorToken,
    jbval: Option<&JsonbValue>,
) -> PgResult<Option<JsonbValue>> {
    // Unpack jbvObject passed for WJB_ELEM / WJB_VALUE.
    if let Some(jb) = jbval {
        if (seq == WJB_ELEM || seq == WJB_VALUE) && jb.typ == jbvObject {
            pushJsonbValue(pstate, WJB_BEGIN_OBJECT, None)?;
            if let JsonbValueData::Object(pairs) = &jb.val {
                for pair in pairs {
                    pushJsonbValue(pstate, WJB_KEY, Some(&pair.key))?;
                    pushJsonbValue(pstate, WJB_VALUE, Some(&pair.value))?;
                }
            }
            return pushJsonbValue(pstate, WJB_END_OBJECT, None);
        }
        if (seq == WJB_ELEM || seq == WJB_VALUE) && jb.typ == jbvArray {
            pushJsonbValue(pstate, WJB_BEGIN_ARRAY, None)?;
            if let JsonbValueData::Array { elems, .. } = &jb.val {
                for elem in elems {
                    pushJsonbValue(pstate, WJB_ELEM, Some(elem))?;
                }
            }
            return pushJsonbValue(pstate, WJB_END_ARRAY, None);
        }
    }

    // Anything but a jbvBinary value pushed as WJB_ELEM/WJB_VALUE drops through.
    let is_binary =
        matches!(jbval, Some(jb) if (seq == WJB_ELEM || seq == WJB_VALUE) && jb.typ == jbvBinary);
    if !is_binary {
        return pushJsonbValueScalar(pstate, seq, jbval);
    }

    // Unpack the binary and add each piece to the pstate.
    let (blen, bdata) = match &jbval
        .ok_or_else(|| PgError::error("pushJsonbValue: jbval is NULL"))?
        .val
    {
        JsonbValueData::Binary { len, data, .. } => (*len, slice_to_vec(data)?),
        _ => unreachable!(),
    };
    let _ = blen;
    let mut it = JsonbIteratorInit(&bdata);
    let mut res: Option<JsonbValue> = None;
    let mut v = JsonbValue::null();

    if (container_header(&bdata) & JB_FSCALAR) != 0 && pstate.is_some() {
        let tok = JsonbIteratorNext(&mut it, &mut v, true)?;
        debug_assert_eq!(tok, WJB_BEGIN_ARRAY);
        let tok = JsonbIteratorNext(&mut it, &mut v, true)?;
        debug_assert_eq!(tok, WJB_ELEM);
        let pushed = pushJsonbValueScalar(pstate, seq, Some(&v))?;
        let tok = JsonbIteratorNext(&mut it, &mut v, true)?;
        debug_assert_eq!(tok, WJB_END_ARRAY);
        debug_assert!(it.is_none());
        return Ok(pushed);
    }

    loop {
        let tok = JsonbIteratorNext(&mut it, &mut v, false)?;
        if tok == WJB_DONE {
            break;
        }
        let scalar = if (tok as i32) < (WJB_BEGIN_ARRAY as i32)
            || (tok == WJB_BEGIN_ARRAY && array_is_raw_scalar(&v))
        {
            Some(&v)
        } else {
            None
        };
        res = pushJsonbValueScalar(pstate, tok, scalar)?;
    }
    Ok(res)
}

/// Helper mirroring `v.val.array.rawScalar` for a `jbvArray` JsonbValue.
#[inline]
fn array_is_raw_scalar(v: &JsonbValue) -> bool {
    matches!(&v.val, JsonbValueData::Array { raw_scalar, .. } if *raw_scalar)
}

/// C: `pushJsonbValueScalar(JsonbParseState **pstate, JsonbIteratorToken seq,
/// JsonbValue *scalarVal)`.
fn pushJsonbValueScalar(
    pstate: &mut Option<Box<JsonbParseState>>,
    seq: JsonbIteratorToken,
    scalar_val: Option<&JsonbValue>,
) -> PgResult<Option<JsonbValue>> {
    let mut result: Option<JsonbValue> = None;

    match seq {
        WJB_BEGIN_ARRAY => {
            let raw_scalar = scalar_val.map(array_is_raw_scalar).unwrap_or(false);
            let nelems = match scalar_val.map(|s| &s.val) {
                Some(JsonbValueData::Array { elems, .. }) => elems.len(),
                _ => 0,
            };
            pushState(pstate);
            let frame = pstate
                .as_mut()
                .ok_or_else(|| PgError::error("pushJsonbValueScalar: parse state is empty"))?;
            frame.cont_val = JsonbValue {
                typ: jbvArray,
                val: JsonbValueData::Array {
                    elems: Vec::new(),
                    raw_scalar,
                },
            };
            // C sets size from scalarVal->val.array.nElems if > 0, else 4.  We
            // hold elems in a Vec; size only bounds reallocation, so track it.
            frame.size = if nelems > 0 { nelems } else { 4 };
        }
        WJB_BEGIN_OBJECT => {
            debug_assert!(scalar_val.is_none());
            pushState(pstate);
            let frame = pstate
                .as_mut()
                .ok_or_else(|| PgError::error("pushJsonbValueScalar: parse state is empty"))?;
            frame.cont_val = JsonbValue {
                typ: jbvObject,
                val: JsonbValueData::Object(Vec::new()),
            };
            frame.size = 4;
        }
        WJB_KEY => {
            let sv = scalar_val.ok_or_else(|| PgError::error("WJB_KEY requires a value"))?;
            debug_assert_eq!(sv.typ, jbvString);
            appendKey(
                pstate.as_mut().ok_or_else(|| {
                    PgError::error("pushJsonbValueScalar: parse state is empty")
                })?,
                sv,
            )?;
        }
        WJB_VALUE => {
            let sv = scalar_val.ok_or_else(|| PgError::error("WJB_VALUE requires a value"))?;
            appendValue(
                pstate.as_mut().ok_or_else(|| {
                    PgError::error("pushJsonbValueScalar: parse state is empty")
                })?,
                sv,
            );
        }
        WJB_ELEM => {
            let sv = scalar_val.ok_or_else(|| PgError::error("WJB_ELEM requires a value"))?;
            appendElement(
                pstate.as_mut().ok_or_else(|| {
                    PgError::error("pushJsonbValueScalar: parse state is empty")
                })?,
                sv,
            )?;
        }
        WJB_END_OBJECT => {
            {
                let frame = pstate.as_mut().ok_or_else(|| {
                    PgError::error("pushJsonbValueScalar: parse state is empty")
                })?;
                let (uniq, skip) = (frame.unique_keys, frame.skip_nulls);
                uniqueifyJsonbObject(&mut frame.cont_val, uniq, skip)?;
            }
            // fall through to WJB_END_ARRAY handling.
            result = close_frame(pstate)?;
        }
        WJB_END_ARRAY => {
            debug_assert!(scalar_val.is_none());
            result = close_frame(pstate)?;
        }
        WJB_DONE => {
            return Err(unrecognized_token());
        }
    }

    Ok(result)
}

/// Common tail of WJB_END_OBJECT / WJB_END_ARRAY: pop the stack and append the
/// finished container into its parent.  Returns the finished container value.
fn close_frame(pstate: &mut Option<Box<JsonbParseState>>) -> PgResult<Option<JsonbValue>> {
    let mut frame = pstate
        .take()
        .ok_or_else(|| PgError::error("close_frame on empty stack"))?;
    let finished = frame.cont_val.clone();
    *pstate = frame.next.take();

    if let Some(parent) = pstate.as_mut() {
        match parent.cont_val.typ {
            jbvArray => appendElement(parent, &finished)?,
            jbvObject => appendValue(parent, &finished),
            _ => return Err(elog_internal("invalid jsonb container type")),
        }
    }
    Ok(Some(finished))
}

/// C: `pushState(JsonbParseState **pstate)`.
fn pushState(pstate: &mut Option<Box<JsonbParseState>>) {
    let ns = JsonbParseState {
        cont_val: JsonbValue::null(),
        size: 0,
        unique_keys: false,
        skip_nulls: false,
        next: pstate.take(),
    };
    *pstate = Some(Box::new(ns));
}

/// C: `appendKey(JsonbParseState *pstate, JsonbValue *string)`.
fn appendKey(pstate: &mut JsonbParseState, string: &JsonbValue) -> PgResult<()> {
    debug_assert_eq!(pstate.cont_val.typ, jbvObject);
    debug_assert_eq!(string.typ, jbvString);
    let JsonbValueData::Object(pairs) = &mut pstate.cont_val.val else {
        unreachable!();
    };
    if pairs.len() >= JSONB_MAX_PAIRS {
        return Err(PgError::error(alloc::format!(
            "number of jsonb object pairs exceeds the maximum allowed ({})",
            JSONB_MAX_PAIRS
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
    }
    let order = pairs.len() as u32;
    pairs.try_reserve(1).map_err(|_| oom())?;
    pairs.push(JsonbPair {
        key: string.clone(),
        value: JsonbValue::null(),
        order,
    });
    Ok(())
}

/// C: `appendValue(JsonbParseState *pstate, JsonbValue *scalarVal)`.
fn appendValue(pstate: &mut JsonbParseState, scalar_val: &JsonbValue) {
    debug_assert_eq!(pstate.cont_val.typ, jbvObject);
    let JsonbValueData::Object(pairs) = &mut pstate.cont_val.val else {
        unreachable!();
    };
    let last = pairs.last_mut().expect("appendValue with no pending key");
    last.value = scalar_val.clone();
}

/// C: `appendElement(JsonbParseState *pstate, JsonbValue *scalarVal)`.
fn appendElement(pstate: &mut JsonbParseState, scalar_val: &JsonbValue) -> PgResult<()> {
    debug_assert_eq!(pstate.cont_val.typ, jbvArray);
    let JsonbValueData::Array { elems, .. } = &mut pstate.cont_val.val else {
        unreachable!();
    };
    if elems.len() >= JSONB_MAX_ELEMS {
        return Err(PgError::error(alloc::format!(
            "number of jsonb array elements exceeds the maximum allowed ({})",
            JSONB_MAX_ELEMS
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
    }
    elems.try_reserve(1).map_err(|_| oom())?;
    elems.push(scalar_val.clone());
    Ok(())
}

// ---------------------------------------------------------------------------
// Iteration
// ---------------------------------------------------------------------------

/// C: `JsonbIteratorInit(JsonbContainer *container)`.
///
/// `container` is the container bytes (starting at the header word).
///
/// The root container of any validated Jsonb is always an array or object, so
/// `iteratorFromContainer` cannot hit its `unknown type of jsonb container`
/// (XX000) arm here; that arm faithfully raises only on the nested-recursion
/// path inside [`JsonbIteratorNext`].  A structurally-impossible unknown root is
/// reported as `None` (the pre-existing `Option` contract), not silently
/// mis-iterated.
pub fn JsonbIteratorInit(container: &[u8]) -> Option<Box<JsonbIterator>> {
    slice_to_vec(container)
        .and_then(|c| iteratorFromContainer(c, 0, None))
        .ok()
}

/// Like [`JsonbIteratorInit`] but records `doc_offset`: the byte position of
/// `container` within the root container of its origin document.  The offset is
/// propagated to every `jbvBinary` child produced by the iterator so
/// document-relative identities (used by jsonpath `.keyvalue()`) survive when
/// iterating a sub-container.  Plain [`JsonbIteratorInit`] is `doc_offset == 0`.
///
/// This entry point exists only in the safe port: C derives the position from
/// the raw `JsonbContainer *` pointer, which is unavailable here.
pub fn JsonbIteratorInitAt(container: &[u8], doc_offset: i32) -> Option<Box<JsonbIterator>> {
    slice_to_vec(container)
        .and_then(|c| iteratorFromContainer(c, doc_offset, None))
        .ok()
}

/// C: `JsonbIteratorNext(JsonbIterator **it, JsonbValue *val, bool skipNested)`.
pub fn JsonbIteratorNext(
    it: &mut Option<Box<JsonbIterator>>,
    val: &mut JsonbValue,
    skip_nested: bool,
) -> PgResult<JsonbIteratorToken> {
    loop {
        if it.is_none() {
            *val = JsonbValue::null();
            return Ok(WJB_DONE);
        }

        let state = it
            .as_ref()
            .ok_or_else(|| PgError::error("JsonbIteratorNext: iterator is NULL"))?
            .state;
        match state {
            JBI_ARRAY_START => {
                let cur = it
                    .as_mut()
                    .ok_or_else(|| PgError::error("JsonbIteratorNext: iterator is NULL"))?;
                val.typ = jbvArray;
                // C sets `val->val.array.nElems = (*it)->nElems` but deliberately
                // leaves `elems` unset (no full conversion; callers must not
                // touch the element buffer).  We expose the count via `nElems`
                // by carrying that many placeholder elements -- they are never
                // read, only counted.  nElems is bounded by JB_CMASK.
                val.val = JsonbValueData::Array {
                    elems: placeholder_values(cur.n_elems)?,
                    raw_scalar: cur.is_scalar,
                };
                cur.cur_index = 0;
                cur.cur_data_offset = 0;
                cur.cur_value_offset = 0;
                cur.state = JBI_ARRAY_ELEM;
                return Ok(WJB_BEGIN_ARRAY);
            }
            JBI_ARRAY_ELEM => {
                {
                    let cur = it
                        .as_ref()
                        .ok_or_else(|| PgError::error("JsonbIteratorNext: iterator is NULL"))?;
                    if cur.cur_index >= cur.n_elems {
                        *it = freeAndGetParent(it.take().ok_or_else(|| {
                            PgError::error("JsonbIteratorNext: iterator is NULL")
                        })?);
                        *val = JsonbValue::null();
                        return Ok(WJB_END_ARRAY);
                    }
                }
                let recurse_data;
                let recurse_off;
                {
                    let cur = it
                        .as_mut()
                        .ok_or_else(|| PgError::error("JsonbIteratorNext: iterator is NULL"))?;
                    let idx = cur.cur_index as usize;
                    fillJsonbValue(
                        &cur.container,
                        idx,
                        cur.data_proper,
                        cur.cur_data_offset,
                        cur.doc_offset,
                        val,
                    )?;
                    let child = container_child(&cur.container, idx);
                    jbe_advance_offset(&mut cur.cur_data_offset, child);
                    cur.cur_index += 1;

                    if !val.is_scalar() && !skip_nested {
                        recurse_data = Some(binary_data(val)?);
                        recurse_off = binary_offset(val);
                    } else {
                        return Ok(WJB_ELEM);
                    }
                }
                let child_it = iteratorFromContainer(
                    recurse_data.ok_or_else(|| {
                        PgError::error("JsonbIteratorNext: recurse data is NULL")
                    })?,
                    recurse_off,
                    it.take(),
                )?;
                *it = Some(child_it);
                continue;
            }
            JBI_OBJECT_START => {
                let cur = it
                    .as_mut()
                    .ok_or_else(|| PgError::error("JsonbIteratorNext: iterator is NULL"))?;
                val.typ = jbvObject;
                // C sets `val->val.object.nPairs = (*it)->nElems` and leaves
                // `pairs` unset.  Expose the count via that many placeholder
                // pairs (never read, only counted).
                val.val = JsonbValueData::Object(placeholder_pairs(cur.n_elems)?);
                cur.cur_index = 0;
                cur.cur_data_offset = 0;
                cur.cur_value_offset = getJsonbOffset(&cur.container, cur.n_elems as i32);
                cur.state = JBI_OBJECT_KEY;
                return Ok(WJB_BEGIN_OBJECT);
            }
            JBI_OBJECT_KEY => {
                {
                    let cur = it
                        .as_ref()
                        .ok_or_else(|| PgError::error("JsonbIteratorNext: iterator is NULL"))?;
                    if cur.cur_index >= cur.n_elems {
                        *it = freeAndGetParent(it.take().ok_or_else(|| {
                            PgError::error("JsonbIteratorNext: iterator is NULL")
                        })?);
                        *val = JsonbValue::null();
                        return Ok(WJB_END_OBJECT);
                    }
                }
                let cur = it
                    .as_mut()
                    .ok_or_else(|| PgError::error("JsonbIteratorNext: iterator is NULL"))?;
                let idx = cur.cur_index as usize;
                fillJsonbValue(
                    &cur.container,
                    idx,
                    cur.data_proper,
                    cur.cur_data_offset,
                    cur.doc_offset,
                    val,
                )?;
                if val.typ != jbvString {
                    return Err(elog_internal("unexpected jsonb type as object key"));
                }
                cur.state = JBI_OBJECT_VALUE;
                return Ok(WJB_KEY);
            }
            JBI_OBJECT_VALUE => {
                let recurse_data;
                let recurse_off;
                {
                    let cur = it
                        .as_mut()
                        .ok_or_else(|| PgError::error("JsonbIteratorNext: iterator is NULL"))?;
                    cur.state = JBI_OBJECT_KEY;
                    let idx = cur.cur_index as usize;
                    let nelems = cur.n_elems as usize;
                    fillJsonbValue(
                        &cur.container,
                        idx + nelems,
                        cur.data_proper,
                        cur.cur_value_offset,
                        cur.doc_offset,
                        val,
                    )?;
                    let child_k = container_child(&cur.container, idx);
                    jbe_advance_offset(&mut cur.cur_data_offset, child_k);
                    let child_v = container_child(&cur.container, idx + nelems);
                    jbe_advance_offset(&mut cur.cur_value_offset, child_v);
                    cur.cur_index += 1;

                    if !val.is_scalar() && !skip_nested {
                        recurse_data = Some(binary_data(val)?);
                        recurse_off = binary_offset(val);
                    } else {
                        return Ok(WJB_VALUE);
                    }
                }
                let child_it = iteratorFromContainer(
                    recurse_data.ok_or_else(|| {
                        PgError::error("JsonbIteratorNext: recurse data is NULL")
                    })?,
                    recurse_off,
                    it.take(),
                )?;
                *it = Some(child_it);
                continue;
            }
        }
    }
}

/// Build `n` placeholder `jbvNull` values (only counted by callers).  `n` is the
/// container's element count (bounded by `JB_CMASK`); we reserve fallibly.
fn placeholder_values(n: u32) -> PgResult<Vec<JsonbValue>> {
    let n = n as usize;
    let mut v = Vec::new();
    v.try_reserve_exact(n).map_err(|_| oom())?;
    for _ in 0..n {
        v.push(JsonbValue::null());
    }
    Ok(v)
}

/// Build `n` placeholder pairs (only counted by callers).
fn placeholder_pairs(n: u32) -> PgResult<Vec<JsonbPair>> {
    let n = n as usize;
    let mut v = Vec::new();
    v.try_reserve_exact(n).map_err(|_| oom())?;
    for _ in 0..n {
        v.push(JsonbPair {
            key: JsonbValue::null(),
            value: JsonbValue::null(),
            order: 0,
        });
    }
    Ok(v)
}

/// Extract the owned container bytes from a `jbvBinary` JsonbValue produced by
/// `fillJsonbValue` (for recursing).
fn binary_data(val: &JsonbValue) -> PgResult<Vec<u8>> {
    match &val.val {
        JsonbValueData::Binary { data, len, .. } => slice_to_vec(&data[..*len as usize]),
        _ => unreachable!("recurse target must be jbvBinary"),
    }
}

/// Extract the document-relative `offset` of a `jbvBinary` JsonbValue (for
/// propagating into the child iterator).
fn binary_offset(val: &JsonbValue) -> i32 {
    match &val.val {
        JsonbValueData::Binary { offset, .. } => *offset,
        _ => unreachable!("recurse target must be jbvBinary"),
    }
}

/// `JBE_ADVANCE_OFFSET(offset, je)` (jsonb.h:162).
#[inline]
fn jbe_advance_offset(offset: &mut u32, je: JEntry) {
    if jbe_has_off(je) {
        *offset = jbe_offlenfld(je);
    } else {
        *offset = offset.wrapping_add(jbe_offlenfld(je));
    }
}

/// C: `iteratorFromContainer(JsonbContainer *container, JsonbIterator *parent)`.
///
/// `doc_offset` is the byte position of `container` within its origin
/// document's root container (0 for the document root); it is propagated to the
/// `jbvBinary` children produced while iterating.
///
/// Returns `Err` (XX000) for a container header that is neither an array nor an
/// object, exactly as C's `default:` arm `elog(ERROR, "unknown type of jsonb
/// container")` (jsonb_util.c:1042-1043).  This is a can't-happen on validated
/// on-disk data, but is raised rather than silently mis-iterated.
fn iteratorFromContainer(
    container: Vec<u8>,
    doc_offset: i32,
    parent: Option<Box<JsonbIterator>>,
) -> PgResult<Box<JsonbIterator>> {
    let header = container_header(&container);
    let n_elems = json_container_size(header);
    // Array starts just after header (4 bytes); children = container->children.
    let children_off = 4;

    let (data_proper, is_scalar, state) = match header & (JB_FARRAY | JB_FOBJECT) {
        JB_FARRAY => {
            let dp = children_off + n_elems as usize * core::mem::size_of::<JEntry>();
            let is_scalar = json_container_is_scalar(header);
            debug_assert!(!is_scalar || n_elems == 1);
            (dp, is_scalar, JBI_ARRAY_START)
        }
        JB_FOBJECT => {
            let dp = children_off + n_elems as usize * core::mem::size_of::<JEntry>() * 2;
            (dp, false, JBI_OBJECT_START)
        }
        _ => {
            // C: elog(ERROR, "unknown type of jsonb container").
            return Err(elog_internal("unknown type of jsonb container"));
        }
    };

    Ok(Box::new(JsonbIterator {
        container,
        n_elems,
        is_scalar,
        children_off,
        data_proper,
        cur_index: 0,
        cur_data_offset: 0,
        cur_value_offset: 0,
        state,
        parent,
        doc_offset,
    }))
}

/// C: `freeAndGetParent(JsonbIterator *it)`.
///
/// Takes the iterator by value to mirror the C `pfree(it); return it->parent;`
/// ownership transfer (the boxed-local lint does not apply to this consume).
#[allow(clippy::boxed_local)]
fn freeAndGetParent(it: Box<JsonbIterator>) -> Option<Box<JsonbIterator>> {
    it.parent
}

// ---------------------------------------------------------------------------
// convert engine
// ---------------------------------------------------------------------------

/// A growable output buffer mirroring the C `StringInfo` used by the convert
/// routines.  `len` tracks the logical length; `data` carries a trailing NUL
/// like a `StringInfo` (we do not rely on it).
///
/// `data` is a [`PgVec<'mcx, u8>`] allocated in the caller's [`Mcx`] (the
/// `CurrentMemoryContext` the C `StringInfo` lives in).  Growth charges that
/// context and the buffer releases its charge on drop, so no explicit free is
/// needed on either the success or error path.
struct ConvertBuffer<'mcx> {
    mcx: Mcx<'mcx>,
    data: PgVec<'mcx, u8>,
    len: usize,
}

impl<'mcx> ConvertBuffer<'mcx> {
    fn new(mcx: Mcx<'mcx>) -> PgResult<Self> {
        // Mirror the C StringInfo's 1024-byte initial allocation.
        let mut data = ::mcx::vec_with_capacity_in(mcx, 1024)?;
        data.resize(1024, 0);
        Ok(ConvertBuffer { mcx, data, len: 0 })
    }

    /// C: `reserveFromBuffer(StringInfo buffer, int len)`.
    ///
    /// The serialized jsonb is bounded by `JENTRY_OFFLENMASK` (the convert
    /// routines error on any larger `totallen`); we validate against
    /// `MaxAllocSize` and reserve fallibly, charging the growth to `mcx`.
    fn reserve(&mut self, len: usize) -> PgResult<usize> {
        let needed = self.len + len + 1;
        if needed > MAX_ALLOC_SIZE {
            return Err(oom());
        }
        if self.data.len() < needed {
            let extra = needed - self.data.len();
            self.data
                .try_reserve(extra)
                .map_err(|_| self.mcx.oom(extra))?;
            self.data.resize(needed, 0);
        }
        let offset = self.len;
        self.len += len;
        self.data[self.len] = 0;
        Ok(offset)
    }

    /// C: `copyToBuffer(StringInfo buffer, int offset, const void *data, int len)`.
    fn copy_to(&mut self, offset: usize, data: &[u8]) {
        self.data[offset..offset + data.len()].copy_from_slice(data);
    }

    /// C: `appendToBuffer(StringInfo buffer, const void *data, int len)`.
    fn append(&mut self, data: &[u8]) -> PgResult<()> {
        let offset = self.reserve(data.len())?;
        self.copy_to(offset, data);
        Ok(())
    }

    /// C: `padBufferToInt(StringInfo buffer)`.
    fn pad_to_int(&mut self) -> PgResult<usize> {
        let padlen = intalign(self.len) - self.len;
        let offset = self.reserve(padlen)?;
        for p in 0..padlen {
            self.data[offset + p] = 0;
        }
        Ok(padlen)
    }
}

/// C: `convertToJsonb(JsonbValue *val)` -- serialize into a varlena allocated in
/// `mcx` (C: `palloc` in `CurrentMemoryContext`).
fn convertToJsonb<'mcx>(mcx: Mcx<'mcx>, val: &JsonbValue) -> PgResult<PgVec<'mcx, u8>> {
    debug_assert_ne!(val.typ, jbvBinary);

    let mut buffer = ConvertBuffer::new(mcx)?;
    // Make room for the varlena header.
    buffer.reserve(VARHDRSZ)?;

    let mut jentry: JEntry = 0;
    convertJsonbValue(&mut buffer, &mut jentry, Some(val), 0)?;

    // Note: the JEntry of the root is discarded; the root JsonbContainer header
    // tells what kind of value it is.
    let len = buffer.len;
    buffer.data.truncate(len);
    set_varsize(&mut buffer.data, len);
    Ok(buffer.data)
}

/// C: `convertJsonbValue(StringInfo buffer, JEntry *header, JsonbValue *val,
/// int level)`.
fn convertJsonbValue<'mcx>(
    buffer: &mut ConvertBuffer<'mcx>,
    header: &mut JEntry,
    val: Option<&JsonbValue>,
    level: i32,
) -> PgResult<()> {
    // Guard against stack overflow due to overly complex Jsonb (C:
    // check_stack_depth() at the top of convertJsonbValue, jsonb_util.c:1605).
    stack_depth_seams::check_stack_depth::call()?;

    let Some(val) = val else {
        return Ok(());
    };

    if val.is_scalar() {
        convertJsonbScalar(buffer, header, val)?;
    } else if val.typ == jbvArray {
        convertJsonbArray(buffer, header, val, level)?;
    } else if val.typ == jbvObject {
        convertJsonbObject(buffer, header, val, level)?;
    } else {
        return Err(elog_internal("unknown type of jsonb container to convert"));
    }
    Ok(())
}

fn convertJsonbArray<'mcx>(
    buffer: &mut ConvertBuffer<'mcx>,
    header: &mut JEntry,
    val: &JsonbValue,
    level: i32,
) -> PgResult<()> {
    let (elems, raw_scalar) = match &val.val {
        JsonbValueData::Array { elems, raw_scalar } => (elems, *raw_scalar),
        _ => unreachable!(),
    };
    let n_elems = elems.len();

    let base_offset = buffer.len;
    buffer.pad_to_int()?;

    let mut containerhead = n_elems as u32 | JB_FARRAY;
    if raw_scalar {
        debug_assert_eq!(n_elems, 1);
        debug_assert_eq!(level, 0);
        containerhead |= JB_FSCALAR;
    }
    buffer.append(&containerhead.to_ne_bytes())?;

    let mut jentry_offset = buffer.reserve(core::mem::size_of::<JEntry>() * n_elems)?;

    let mut totallen: u32 = 0;
    for (i, elem) in elems.iter().enumerate() {
        let mut meta: JEntry = 0;
        convertJsonbValue(buffer, &mut meta, Some(elem), level + 1)?;

        let len = jbe_offlenfld(meta);
        totallen = totallen.wrapping_add(len);

        if totallen > JENTRY_OFFLENMASK {
            return Err(array_overflow());
        }

        if i % JB_OFFSET_STRIDE as usize == 0 {
            meta = (meta & JENTRY_TYPEMASK) | totallen | JENTRY_HAS_OFF;
        }

        buffer.copy_to(jentry_offset, &meta.to_ne_bytes());
        jentry_offset += core::mem::size_of::<JEntry>();
    }

    let totallen = (buffer.len - base_offset) as u32;
    if totallen > JENTRY_OFFLENMASK {
        return Err(array_overflow());
    }

    *header = JENTRY_ISCONTAINER | totallen;
    Ok(())
}

fn convertJsonbObject<'mcx>(
    buffer: &mut ConvertBuffer<'mcx>,
    header: &mut JEntry,
    val: &JsonbValue,
    level: i32,
) -> PgResult<()> {
    let pairs = match &val.val {
        JsonbValueData::Object(pairs) => pairs,
        _ => unreachable!(),
    };
    let n_pairs = pairs.len();

    let base_offset = buffer.len;
    buffer.pad_to_int()?;

    let containerheader = n_pairs as u32 | JB_FOBJECT;
    buffer.append(&containerheader.to_ne_bytes())?;

    let mut jentry_offset = buffer.reserve(core::mem::size_of::<JEntry>() * n_pairs * 2)?;

    let mut totallen: u32 = 0;
    for (i, pair) in pairs.iter().enumerate() {
        let mut meta: JEntry = 0;
        convertJsonbScalar(buffer, &mut meta, &pair.key)?;

        let len = jbe_offlenfld(meta);
        totallen = totallen.wrapping_add(len);
        if totallen > JENTRY_OFFLENMASK {
            return Err(object_overflow());
        }

        if i % JB_OFFSET_STRIDE as usize == 0 {
            meta = (meta & JENTRY_TYPEMASK) | totallen | JENTRY_HAS_OFF;
        }

        buffer.copy_to(jentry_offset, &meta.to_ne_bytes());
        jentry_offset += core::mem::size_of::<JEntry>();
    }
    for (i, pair) in pairs.iter().enumerate() {
        let mut meta: JEntry = 0;
        convertJsonbValue(buffer, &mut meta, Some(&pair.value), level + 1)?;

        let len = jbe_offlenfld(meta);
        totallen = totallen.wrapping_add(len);
        if totallen > JENTRY_OFFLENMASK {
            return Err(object_overflow());
        }

        if (i + n_pairs) % JB_OFFSET_STRIDE as usize == 0 {
            meta = (meta & JENTRY_TYPEMASK) | totallen | JENTRY_HAS_OFF;
        }

        buffer.copy_to(jentry_offset, &meta.to_ne_bytes());
        jentry_offset += core::mem::size_of::<JEntry>();
    }

    let totallen = (buffer.len - base_offset) as u32;
    if totallen > JENTRY_OFFLENMASK {
        return Err(object_overflow());
    }

    *header = JENTRY_ISCONTAINER | totallen;
    Ok(())
}

fn convertJsonbScalar<'mcx>(
    buffer: &mut ConvertBuffer<'mcx>,
    header: &mut JEntry,
    scalar_val: &JsonbValue,
) -> PgResult<()> {
    match &scalar_val.val {
        JsonbValueData::Null => {
            *header = JENTRY_ISNULL;
        }
        JsonbValueData::String(s) => {
            buffer.append(s)?;
            *header = s.len() as u32;
        }
        JsonbValueData::Numeric(num) => {
            let numlen = num.len();
            let padlen = buffer.pad_to_int()?;
            buffer.append(num)?;
            *header = JENTRY_ISNUMERIC | (padlen + numlen) as u32;
        }
        JsonbValueData::Bool(b) => {
            *header = if *b {
                JENTRY_ISBOOL_TRUE
            } else {
                JENTRY_ISBOOL_FALSE
            };
        }
        JsonbValueData::Datetime(dt) => {
            // C: JsonEncodeDateTime(buf, scalarVal->val.datetime.value,
            // scalarVal->val.datetime.typid, &scalarVal->val.datetime.tz).
            // json.c owns the encoder (the cycle partner); the C call site
            // always passes a non-NULL &tz, so tzp = Some(dt.tz).
            //
            // Datum-unification: `json_encode_datetime` (the json/timestamp
            // owner's seam) takes the canonical `&types_tuple::Datum<'mcx>`.
            // `dt.value` is a by-value datetime word (timestamp/date int), so it
            // is wrapped as the `ByVal` arm via `Datum::from_usize`. This crate
            // stores no `datum::Datum` shim.
            let s = json_seams::json_encode_datetime::call(
                &types_tuple::Datum::from_usize(dt.value),
                dt.typid,
                Some(dt.tz),
            )?;
            buffer.append(s.as_bytes())?;
            *header = s.len() as u32;
        }
        _ => return Err(elog_internal("invalid jsonb scalar type")),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// compareJsonbContainers / findJsonbValueFromContainer /
// getKeyJsonValueFromContainer / getIthJsonbValueFromContainer
// ---------------------------------------------------------------------------

/// `JsonContainerSize(container)` from container bytes.
#[inline]
fn json_container_size_of(jc: &[u8]) -> i32 {
    json_container_size(container_header(jc)) as i32
}

/// Number of array elements (C: `val.array.nElems`); only valid for `jbvArray`.
#[inline]
fn array_n_elems(v: &JsonbValue) -> i32 {
    match &v.val {
        JsonbValueData::Array { elems, .. } => elems.len() as i32,
        _ => 0,
    }
}

/// Number of object pairs (C: `val.object.nPairs`); only valid for `jbvObject`.
#[inline]
fn object_n_pairs(v: &JsonbValue) -> i32 {
    match &v.val {
        JsonbValueData::Object(pairs) => pairs.len() as i32,
        _ => 0,
    }
}

/// The owned container bytes of a `jbvBinary` value (C: `val.binary.data`,
/// truncated to `val.binary.len`).
#[inline]
fn binary_container(v: &JsonbValue) -> PgResult<Vec<u8>> {
    match &v.val {
        JsonbValueData::Binary { len, data, .. } => slice_to_vec(&data[..*len as usize]),
        _ => unreachable!("binary_container on non-binary"),
    }
}

/// C: `compareJsonbContainers(JsonbContainer *a, JsonbContainer *b)`.
///
/// BT comparator worker: returns < 0, 0, or > 0.  `a`/`b` are container bytes.
pub fn compareJsonbContainers(a: &[u8], b: &[u8]) -> PgResult<i32> {
    let mut ita = JsonbIteratorInit(a);
    let mut itb = JsonbIteratorInit(b);
    let mut res: i32 = 0;

    // do { ... } while (res == 0);
    loop {
        let mut va = JsonbValue::null();
        let mut vb = JsonbValue::null();

        let ra = JsonbIteratorNext(&mut ita, &mut va, false)?;
        let rb = JsonbIteratorNext(&mut itb, &mut vb, false)?;

        if ra == rb {
            if ra == WJB_DONE {
                // Decisively equal.
                break;
            }

            if ra == WJB_END_ARRAY || ra == WJB_END_OBJECT {
                // No array/object to compare at this stage; the do/while loops
                // on `res == 0`.
                continue;
            }

            if va.typ == vb.typ {
                match va.typ {
                    jbvString | jbvNull | jbvNumeric | jbvBool => {
                        res = compareJsonbScalarValue(&va, &vb)?;
                    }
                    jbvArray => {
                        // This could be a "raw scalar" pseudo array.
                        if array_is_raw_scalar(&va) != array_is_raw_scalar(&vb) {
                            res = if array_is_raw_scalar(&va) { -1 } else { 1 };
                        }
                        // (No "else" here, faithfully mirroring the C quirk that
                        // an empty top-level array sorts less than null.)
                        if array_n_elems(&va) != array_n_elems(&vb) {
                            res = if array_n_elems(&va) > array_n_elems(&vb) {
                                1
                            } else {
                                -1
                            };
                        }
                    }
                    jbvObject => {
                        if object_n_pairs(&va) != object_n_pairs(&vb) {
                            res = if object_n_pairs(&va) > object_n_pairs(&vb) {
                                1
                            } else {
                                -1
                            };
                        }
                    }
                    jbvBinary => return Err(elog_internal("unexpected jbvBinary value")),
                    jbvDatetime => return Err(elog_internal("unexpected jbvDatetime value")),
                }
            } else {
                // Type-defined order.
                res = if va.typ > vb.typ { 1 } else { -1 };
            }
        } else {
            // Two heterogeneously-typed containers, or a container and a scalar.
            // Type-defined order.
            res = if va.typ > vb.typ { 1 } else { -1 };
        }

        if res != 0 {
            break;
        }
    }

    // C frees both iterator chains here; in Rust they drop automatically.
    Ok(res)
}

/// C: `findJsonbValueFromContainer(JsonbContainer *container, uint32 flags,
/// JsonbValue *key)`.  Returns a copy of the matching value, or `None`.
pub fn findJsonbValueFromContainer(
    container: &[u8],
    flags: u32,
    key: &JsonbValue,
) -> PgResult<Option<JsonbValue>> {
    debug_assert_eq!(flags & !(JB_FARRAY | JB_FOBJECT), 0);

    let count = json_container_size_of(container);

    // Quick out if object/array is empty.
    if count <= 0 {
        return Ok(None);
    }

    if (flags & JB_FARRAY) != 0 && json_container_is_array(container_header(container)) {
        // base_addr = (char *) (children + count)
        let base_addr = 4 + count as usize * core::mem::size_of::<JEntry>();
        let mut offset: u32 = 0;
        let mut result = JsonbValue::null();

        for i in 0..count as usize {
            // `container` is treated as a document root here (offset 0); callers
            // that track document identity re-base the returned `offset` by the
            // searched container's own document position.
            fillJsonbValue(container, i, base_addr, offset, 0, &mut result)?;

            if key.typ == result.typ && equalsJsonbScalarValue(key, &result)? {
                return Ok(Some(result));
            }

            jbe_advance_offset(&mut offset, container_child(container, i));
        }
    } else if (flags & JB_FOBJECT) != 0 && json_container_is_object(container_header(container)) {
        // Object key passed by caller must be a string.
        debug_assert_eq!(key.typ, jbvString);
        let key_bytes = match &key.val {
            JsonbValueData::String(s) => s.as_slice(),
            _ => &[],
        };
        return getKeyJsonValueFromContainer(container, key_bytes);
    }

    // Not found.
    Ok(None)
}

/// C: `getKeyJsonValueFromContainer(JsonbContainer *container, const char
/// *keyVal, int keyLen, JsonbValue *res)`.  The `res` out-parameter (reused or
/// palloc'd in C) is modeled by returning the value.
pub fn getKeyJsonValueFromContainer(
    container: &[u8],
    key_val: &[u8],
) -> PgResult<Option<JsonbValue>> {
    let count = json_container_size_of(container);

    debug_assert!(json_container_is_object(container_header(container)));

    // Quick out if object is empty.
    if count <= 0 {
        return Ok(None);
    }

    // Binary search; account for *Pairs* of JEntrys.
    // baseAddr = (char *) (children + count * 2)
    let base_addr = 4 + count as usize * 2 * core::mem::size_of::<JEntry>();
    let mut stop_low: u32 = 0;
    let mut stop_high: u32 = count as u32;

    while stop_low < stop_high {
        let stop_middle = stop_low + (stop_high - stop_low) / 2;

        let cand_off = getJsonbOffset(container, stop_middle as i32);
        let cand_len = getJsonbLength(container, stop_middle as i32);

        let cand_start = base_addr + cand_off as usize;
        let candidate_val = &container[cand_start..cand_start + cand_len as usize];

        let difference = lengthCompareJsonbString(candidate_val, key_val);

        if difference == 0 {
            // Found our key, return the corresponding value.
            let index = stop_middle as usize + count as usize;
            let mut res = JsonbValue::null();
            fillJsonbValue(
                container,
                index,
                base_addr,
                getJsonbOffset(container, index as i32),
                0,
                &mut res,
            )?;
            return Ok(Some(res));
        } else if difference < 0 {
            stop_low = stop_middle + 1;
        } else {
            stop_high = stop_middle;
        }
    }

    // Not found.
    Ok(None)
}

/// C: `getIthJsonbValueFromContainer(JsonbContainer *container, uint32 i)`.
pub fn getIthJsonbValueFromContainer(container: &[u8], i: u32) -> PgResult<Option<JsonbValue>> {
    if !json_container_is_array(container_header(container)) {
        return Err(elog_internal("not a jsonb array"));
    }

    let nelements = json_container_size_of(container) as u32;
    // base_addr = (char *) &container->children[nelements]
    let base_addr = 4 + nelements as usize * core::mem::size_of::<JEntry>();

    if i >= nelements {
        return Ok(None);
    }

    let mut result = JsonbValue::null();
    fillJsonbValue(
        container,
        i as usize,
        base_addr,
        getJsonbOffset(container, i as i32),
        0,
        &mut result,
    )?;

    Ok(Some(result))
}

// ---------------------------------------------------------------------------
// JsonbDeepContains
// ---------------------------------------------------------------------------

/// C: `JsonbDeepContains(JsonbIterator **val, JsonbIterator **mContained)`.
pub fn JsonbDeepContains(
    val: &mut Option<Box<JsonbIterator>>,
    m_contained: &mut Option<Box<JsonbIterator>>,
) -> PgResult<bool> {
    // Guard against stack overflow due to overly complex Jsonb.
    stack_depth_seams::check_stack_depth::call()?;

    let mut vval = JsonbValue::null();
    let mut vcontained = JsonbValue::null();

    let rval = JsonbIteratorNext(val, &mut vval, false)?;
    let rcont = JsonbIteratorNext(m_contained, &mut vcontained, false)?;

    if rval != rcont {
        debug_assert!(rval == WJB_BEGIN_OBJECT || rval == WJB_BEGIN_ARRAY);
        debug_assert!(rcont == WJB_BEGIN_OBJECT || rcont == WJB_BEGIN_ARRAY);
        Ok(false)
    } else if rcont == WJB_BEGIN_OBJECT {
        debug_assert_eq!(vval.typ, jbvObject);
        debug_assert_eq!(vcontained.typ, jbvObject);

        // If lhs has fewer pairs than rhs, it can't contain rhs.
        if object_n_pairs(&vval) < object_n_pairs(&vcontained) {
            return Ok(false);
        }

        // Work through rhs "is it contained within?" object.
        loop {
            let rcont = JsonbIteratorNext(m_contained, &mut vcontained, false)?;

            if rcont == WJB_END_OBJECT {
                return Ok(true);
            }

            debug_assert_eq!(rcont, WJB_KEY);
            debug_assert_eq!(vcontained.typ, jbvString);

            // First, find value by key...
            let key_bytes = match &vcontained.val {
                JsonbValueData::String(s) => s.clone(),
                _ => Vec::new(),
            };
            let val_container = val
                .as_ref()
                .ok_or_else(|| PgError::error("JsonbDeepContains: val iterator is NULL"))?
                .container
                .clone();
            let lhs_val = match getKeyJsonValueFromContainer(&val_container, &key_bytes)? {
                Some(v) => v,
                None => return Ok(false),
            };

            // ...key matched; now compare values.
            let rcont = JsonbIteratorNext(m_contained, &mut vcontained, true)?;
            debug_assert_eq!(rcont, WJB_VALUE);

            if lhs_val.typ != vcontained.typ {
                return Ok(false);
            } else if lhs_val.is_scalar() {
                if !equalsJsonbScalarValue(&lhs_val, &vcontained)? {
                    return Ok(false);
                }
            } else {
                // Nested container value (object or array).
                debug_assert_eq!(lhs_val.typ, jbvBinary);
                debug_assert_eq!(vcontained.typ, jbvBinary);

                let mut nestval = JsonbIteratorInit(&binary_container(&lhs_val)?);
                let mut nest_contained = JsonbIteratorInit(&binary_container(&vcontained)?);

                if !JsonbDeepContains(&mut nestval, &mut nest_contained)? {
                    return Ok(false);
                }
            }
        }
    } else if rcont == WJB_BEGIN_ARRAY {
        debug_assert_eq!(vval.typ, jbvArray);
        debug_assert_eq!(vcontained.typ, jbvArray);

        let mut lhs_conts: Option<Vec<JsonbValue>> = None;
        let mut n_lhs_elems = array_n_elems(&vval) as u32;

        // A raw scalar may not contain an array.
        if array_is_raw_scalar(&vval) && !array_is_raw_scalar(&vcontained) {
            return Ok(false);
        }

        // Work through rhs "is it contained within?" array.
        loop {
            let rcont = JsonbIteratorNext(m_contained, &mut vcontained, true)?;

            if rcont == WJB_END_ARRAY {
                return Ok(true);
            }

            debug_assert_eq!(rcont, WJB_ELEM);

            if vcontained.is_scalar() {
                let val_container = val
                    .as_ref()
                    .ok_or_else(|| PgError::error("JsonbDeepContains: val iterator is NULL"))?
                    .container
                    .clone();
                if findJsonbValueFromContainer(&val_container, JB_FARRAY, &vcontained)?.is_none() {
                    return Ok(false);
                }
            } else {
                // First container found in the rhs array at this depth:
                // initialize the temp lhs array of containers.
                if lhs_conts.is_none() {
                    // n_lhs_elems is the lhs array's element count (bounded by
                    // JB_CMASK); reserve fallibly for the worst case (all
                    // containers).
                    let mut conts: Vec<JsonbValue> = Vec::new();
                    conts.try_reserve(n_lhs_elems as usize).map_err(|_| oom())?;

                    for _ in 0..n_lhs_elems {
                        let mut tmp = JsonbValue::null();
                        let rc = JsonbIteratorNext(val, &mut tmp, true)?;
                        debug_assert_eq!(rc, WJB_ELEM);

                        if tmp.typ == jbvBinary {
                            conts.push(tmp);
                        }
                    }

                    // No container elements; give up now.
                    if conts.is_empty() {
                        return Ok(false);
                    }

                    n_lhs_elems = conts.len() as u32;
                    lhs_conts = Some(conts);
                }

                let conts = lhs_conts.as_ref().unwrap();

                // XXX: Nested array containment is O(N^2)
                let mut i: u32 = 0;
                while i < n_lhs_elems {
                    let mut nestval = JsonbIteratorInit(&binary_container(&conts[i as usize])?);
                    let mut nest_contained = JsonbIteratorInit(&binary_container(&vcontained)?);

                    let contains = JsonbDeepContains(&mut nestval, &mut nest_contained)?;
                    if contains {
                        break;
                    }
                    i += 1;
                }

                // rhs container value not contained if it matched no lhs cont.
                if i == n_lhs_elems {
                    return Ok(false);
                }
            }
        }
    } else {
        Err(elog_internal("invalid jsonb container type"))
    }
}

// ---------------------------------------------------------------------------
// Scalar equality / comparison
// ---------------------------------------------------------------------------

#[inline]
fn jsonb_bool(v: &JsonbValue) -> bool {
    matches!(&v.val, JsonbValueData::Bool(true))
}

#[inline]
fn jsonb_numeric_bytes(v: &JsonbValue) -> &[u8] {
    match &v.val {
        JsonbValueData::Numeric(n) => n.as_slice(),
        _ => &[],
    }
}

#[inline]
fn jsonb_string_bytes(v: &JsonbValue) -> &[u8] {
    match &v.val {
        JsonbValueData::String(s) => s.as_slice(),
        _ => &[],
    }
}

/// C: `equalsJsonbScalarValue(JsonbValue *a, JsonbValue *b)`.
fn equalsJsonbScalarValue(a: &JsonbValue, b: &JsonbValue) -> PgResult<bool> {
    if a.typ == b.typ {
        return match a.typ {
            jbvNull => Ok(true),
            jbvString => Ok(lengthCompareJsonbStringValue(a, b) == 0),
            jbvNumeric => {
                // numeric_eq over the on-disk numerics, via the numeric seam
                // (the cross-subsystem boundary to backend-utils-adt-numeric).
                Ok(numeric_seams::numeric_eq::call(
                    jsonb_numeric_bytes(a),
                    jsonb_numeric_bytes(b),
                ))
            }
            jbvBool => Ok(jsonb_bool(a) == jsonb_bool(b)),
            _ => Err(elog_internal("invalid jsonb scalar type")),
        };
    }
    Err(elog_internal("jsonb scalar type mismatch"))
}

/// C: `compareJsonbScalarValue(JsonbValue *a, JsonbValue *b)`.
///
/// `jbvString` scalars are compared with `varstr_cmp(.., DEFAULT_COLLATION_OID)`
/// (jsonb_util.c:1454-1459) -- the database default collation, used for B-tree
/// ordering.  That comparison is routed through the `varstr_cmp_collation` seam
/// (the cross-subsystem boundary to backend-utils-adt-varlena);
/// the real provider performs the provider-aware (ICU / libc `strcoll`)
/// comparison for a non-C default collation, reducing to a byte compare only
/// when the resolved locale is `collate_is_c`.  This is off the I/O path (B-tree
/// operator support only) and does not affect `jsonb_in`/`jsonb_out`.
fn compareJsonbScalarValue(a: &JsonbValue, b: &JsonbValue) -> PgResult<i32> {
    if a.typ == b.typ {
        return match a.typ {
            jbvNull => Ok(0),
            jbvString => varlena_seams::varstr_cmp::call(
                jsonb_string_bytes(a),
                jsonb_string_bytes(b),
                DEFAULT_COLLATION_OID,
            ),
            jbvNumeric => Ok(numeric_seams::numeric_cmp::call(
                jsonb_numeric_bytes(a),
                jsonb_numeric_bytes(b),
            )),
            jbvBool => {
                let (ba, bb) = (jsonb_bool(a), jsonb_bool(b));
                Ok(if ba == bb {
                    0
                } else if ba & !bb {
                    1 // true > false
                } else {
                    -1
                })
            }
            _ => Err(elog_internal("invalid jsonb scalar type")),
        };
    }
    Err(elog_internal("jsonb scalar type mismatch"))
}

// ---------------------------------------------------------------------------
// Hashing
// ---------------------------------------------------------------------------

/// `pg_rotate_left32(word, n)` (port/pg_bitutils.h): `(word << n) | (word >>
/// (32 - n))`.  For the `1 <= n < 32` range jsonb uses this is exactly a left
/// rotate.
#[inline]
fn pg_rotate_left32(word: u32, n: u32) -> u32 {
    word.rotate_left(n)
}

/// `ROTATE_HIGH_AND_LOW_32BITS(v)` (common/hashfn.h).
#[inline]
fn rotate_high_and_low_32bits(v: u64) -> u64 {
    ((v << 1) & 0xffff_fffe_ffff_fffe) | ((v >> 31) & 0x0000_0001_0000_0001)
}

/// Decode the i-th `NumericDigit` (i16) from a native-endian digit byte slice.
#[inline]
fn numeric_digit_at_bytes(digit_bytes: &[u8], i: usize) -> i16 {
    ::types_numeric::numeric_digit_at(digit_bytes, i)
}

/// C: `hash_numeric(NumericGetDatum(num))` (numeric.c) -- ported in-crate over
/// the on-disk `numeric` bytes so equal numerics hash to equal codes.
fn hash_numeric(num: &[u8]) -> u32 {
    use types_numeric::{numeric_digits, numeric_is_special, numeric_ndigits, numeric_weight};

    // If it's NaN or infinity, don't hash the rest of the fields.
    if numeric_is_special(num) {
        return 0;
    }

    let mut weight = numeric_weight(num);
    let ndigits = numeric_ndigits(num, num.len());
    let digit_bytes = numeric_digits(num);

    // Omit leading zero digits, decrementing weight for each.
    let mut start_offset = 0usize;
    for i in 0..ndigits {
        if numeric_digit_at_bytes(digit_bytes, i) != 0 {
            break;
        }
        start_offset += 1;
        weight -= 1;
    }

    // No non-zero digits => value is zero, regardless of other fields.
    if ndigits == start_offset {
        return (-1i32) as u32;
    }

    // Omit trailing zero digits.
    let mut end_offset = 0usize;
    for i in (0..ndigits).rev() {
        if numeric_digit_at_bytes(digit_bytes, i) != 0 {
            break;
        }
        end_offset += 1;
    }

    debug_assert!(start_offset + end_offset < ndigits);

    let hash_len = ndigits - start_offset - end_offset;
    // hash_any over (NUMERIC_DIGITS + start_offset), hash_len * sizeof(NumericDigit) bytes.
    let byte_start = start_offset * 2;
    let byte_end = byte_start + hash_len * 2;
    let digit_hash = hashfn_seams::hash_bytes::call(&digit_bytes[byte_start..byte_end]);

    // Mix in the weight, via XOR (int weight -> uint32).
    digit_hash ^ (weight as u32)
}

/// C: `hash_numeric_extended(NumericGetDatum(num), seed)` (numeric.c).
fn hash_numeric_extended(num: &[u8], seed: u64) -> u64 {
    use types_numeric::{numeric_digits, numeric_is_special, numeric_ndigits, numeric_weight};

    if numeric_is_special(num) {
        return seed;
    }

    let mut weight = numeric_weight(num);
    let ndigits = numeric_ndigits(num, num.len());
    let digit_bytes = numeric_digits(num);

    let mut start_offset = 0usize;
    for i in 0..ndigits {
        if numeric_digit_at_bytes(digit_bytes, i) != 0 {
            break;
        }
        start_offset += 1;
        weight -= 1;
    }

    if ndigits == start_offset {
        return seed.wrapping_sub(1);
    }

    let mut end_offset = 0usize;
    for i in (0..ndigits).rev() {
        if numeric_digit_at_bytes(digit_bytes, i) != 0 {
            break;
        }
        end_offset += 1;
    }

    debug_assert!(start_offset + end_offset < ndigits);

    let hash_len = ndigits - start_offset - end_offset;
    let byte_start = start_offset * 2;
    let byte_end = byte_start + hash_len * 2;
    let digit_hash = hashfn_seams::hash_bytes_extended::call(&digit_bytes[byte_start..byte_end], seed);

    // result = digit_hash ^ weight  (int weight sign-extended to int64/uint64).
    digit_hash ^ (weight as i64 as u64)
}

/// C: `JsonbHashScalarValue(const JsonbValue *scalarVal, uint32 *hash)`.
pub fn JsonbHashScalarValue(scalar_val: &JsonbValue, hash: &mut u32) -> PgResult<()> {
    let tmp: u32 = match scalar_val.typ {
        jbvNull => 0x01,
        jbvString => hashfn_seams::hash_bytes::call(jsonb_string_bytes(scalar_val)),
        jbvNumeric => hash_numeric(jsonb_numeric_bytes(scalar_val)),
        jbvBool => {
            if jsonb_bool(scalar_val) {
                0x02
            } else {
                0x04
            }
        }
        _ => return Err(elog_internal("invalid jsonb scalar type")),
    };

    // Rotate the previous value left 1 bit, then XOR in the new hash value.
    *hash = pg_rotate_left32(*hash, 1);
    *hash ^= tmp;
    Ok(())
}

/// C: `JsonbHashScalarValueExtended(const JsonbValue *scalarVal, uint64 *hash,
/// uint64 seed)`.
pub fn JsonbHashScalarValueExtended(
    scalar_val: &JsonbValue,
    hash: &mut u64,
    seed: u64,
) -> PgResult<()> {
    let tmp: u64 = match scalar_val.typ {
        jbvNull => seed + 0x01,
        jbvString => hashfn_seams::hash_bytes_extended::call(jsonb_string_bytes(scalar_val), seed),
        jbvNumeric => hash_numeric_extended(jsonb_numeric_bytes(scalar_val), seed),
        jbvBool => {
            let b = jsonb_bool(scalar_val);
            if seed != 0 {
                // C: hashcharextended(BoolGetDatum(b), seed)
                //  == hash_uint32_extended((uint32) (signed char) b, seed)
                hashfn_seams::hash_bytes_uint32_extended::call(b as u32, seed)
            } else if b {
                0x02
            } else {
                0x04
            }
        }
        _ => return Err(elog_internal("invalid jsonb scalar type")),
    };

    *hash = rotate_high_and_low_32bits(*hash);
    *hash ^= tmp;
    Ok(())
}

// ---------------------------------------------------------------------------
// String / pair comparison + uniqueify
// ---------------------------------------------------------------------------

/// C: `lengthCompareJsonbString(const char *val1, int len1, const char *val2,
/// int len2)`.
fn lengthCompareJsonbString(val1: &[u8], val2: &[u8]) -> i32 {
    if val1.len() == val2.len() {
        match val1.cmp(val2) {
            core::cmp::Ordering::Less => -1,
            core::cmp::Ordering::Equal => 0,
            core::cmp::Ordering::Greater => 1,
        }
    } else if val1.len() > val2.len() {
        1
    } else {
        -1
    }
}

/// C: `lengthCompareJsonbStringValue(const void *a, const void *b)`.
fn lengthCompareJsonbStringValue(a: &JsonbValue, b: &JsonbValue) -> i32 {
    let (sa, sb) = match (&a.val, &b.val) {
        (JsonbValueData::String(sa), JsonbValueData::String(sb)) => (sa, sb),
        _ => unreachable!("lengthCompareJsonbStringValue on non-string"),
    };
    lengthCompareJsonbString(sa, sb)
}

/// C: `lengthCompareJsonbPair(const void *a, const void *b, void *binequal)`.
///
/// `qsort_arg` comparator over `JsonbPair`s.  `binequal` is set to `true` iff
/// the two pairs have equal keys (some callers care whether the values are
/// merely equivalent).  Equal-key pairs are ordered so the original order is
/// respected (the unique algorithm prefers the first element as value).
fn lengthCompareJsonbPair(
    pa: &JsonbPair,
    pb: &JsonbPair,
    binequal: &mut bool,
) -> core::cmp::Ordering {
    let mut res = lengthCompareJsonbStringValue(&pa.key, &pb.key);
    if res == 0 {
        *binequal = true;
    }

    // Guarantee keeping order of equal pair.
    if res == 0 {
        res = if pa.order > pb.order { -1 } else { 1 };
    }

    res.cmp(&0)
}

/// C: `uniqueifyJsonbObject(JsonbValue *object, bool unique_keys, bool
/// skip_nulls)`.
fn uniqueifyJsonbObject(
    object: &mut JsonbValue,
    unique_keys: bool,
    skip_nulls: bool,
) -> PgResult<()> {
    debug_assert_eq!(object.typ, jbvObject);
    let JsonbValueData::Object(pairs) = &mut object.val else {
        unreachable!();
    };

    let mut has_non_uniq = false;
    if pairs.len() > 1 {
        // C: qsort_arg(pairs, nPairs, sizeof, lengthCompareJsonbPair,
        // &hasNonUniq) -- the comparator sets hasNonUniq when two keys are
        // equal.  The comparator's `order` tiebreak makes it a strict total
        // order, so a `sort_by` is well-defined and behaviorally equivalent to
        // the C `qsort_arg` for this comparator (probe-port2-srv-qsort-arg is
        // not ported in this worktree; the total order makes the slice sort
        // identical and stability irrelevant).
        pairs.sort_by(|pa, pb| lengthCompareJsonbPair(pa, pb, &mut has_non_uniq));
    }

    if has_non_uniq && unique_keys {
        return Err(PgError::error("duplicate JSON object key value")
            .with_sqlstate(ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE));
    }

    if has_non_uniq || skip_nulls {
        // Remove leading items with null (if skip_nulls), then collapse
        // duplicate-or-null pairs keeping the first of each equal-key run.
        let mut start = 0usize;
        while skip_nulls && start < pairs.len() && pairs[start].value.typ == jbvNull {
            start += 1;
        }

        if start < pairs.len() {
            let mut res = start;
            let mut ptr = start + 1;
            while ptr < pairs.len() {
                let dup = lengthCompareJsonbStringValue(&pairs[ptr].key, &pairs[res].key) == 0;
                let is_null = skip_nulls && pairs[ptr].value.typ == jbvNull;
                if !dup && !is_null {
                    res += 1;
                    if ptr != res {
                        pairs[res] = pairs[ptr].clone();
                    }
                }
                ptr += 1;
            }
            let new_len = res + 1;
            pairs.drain(0..start);
            pairs.truncate(new_len - start);
        } else {
            pairs.clear();
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Error helpers (mirror C's elog(ERROR, ...) / ereport messages).
// ---------------------------------------------------------------------------

fn elog_internal(msg: &str) -> PgError {
    PgError::error(msg.to_string()).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

fn unrecognized_token() -> PgError {
    elog_internal("unrecognized jsonb sequential processing token")
}

fn array_overflow() -> PgError {
    PgError::error(alloc::format!(
        "total size of jsonb array elements exceeds the maximum of {} bytes",
        JENTRY_OFFLENMASK
    ))
    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

fn object_overflow() -> PgError {
    PgError::error(alloc::format!(
        "total size of jsonb object elements exceeds the maximum of {} bytes",
        JENTRY_OFFLENMASK
    ))
    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

#[cfg(test)]
mod tests;
