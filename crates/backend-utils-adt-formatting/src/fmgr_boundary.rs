//! The fmgr/Datum **boundary** for the formatting.c NUM family —
//! `to_char(numeric/int4/int8/float4/float8, text)` and `to_number(text, text)`.
//!
//! Each `*_boundary` entry mirrors one `Datum fn(PG_FUNCTION_ARGS)` from
//! formatting.c, marshalling the by-reference varlena / by-value scalar args
//! into the ported [`crate::num_entry`] core and re-encoding the result.
//!
//! Like `rangetypes::range_fmgr_boundary`, every NUM allocation in C is charged
//! to `CurrentMemoryContext`; this repo carries no ambient context, so each
//! allocating entry takes the result `Mcx<'mcx>` explicitly and returns
//! `PgResult`. The bare `Datum fn(FunctionCallInfo)` registry wiring is deferred
//! project-wide (the Datum-redesign lifetime-ripple gate): the executor will
//! call these mcx-taking entries when fmgr dispatch lands. C's `ereport(ERROR)`
//! surfaces as `Err(PgError)`; the `to_number` `None` core result is the
//! C `PG_RETURN_NULL()` arm (a genuine SQL NULL, not an error).

use backend_utils_adt_numeric::convert::{make_result, set_var_from_num};
use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_fmgr::{FmgrArg, RefPayload};

use crate::dch_entry::{interval_to_char, timestamp_to_char, timestamptz_to_char};
use crate::num_entry::{
    float4_to_char, float8_to_char, int4_to_char, int8_to_char, numeric_to_char,
    numeric_to_number,
};
use types_datetime::{Interval, Timestamp};

// ===========================================================================
// Built-in function Oids (fmgroids.h; pg_proc.dat 1772-1777).
// ===========================================================================

/// `F_TO_CHAR_NUMERIC_TEXT` (prosrc `numeric_to_char`, pg_proc 1772).
pub const F_TO_CHAR_NUMERIC_TEXT: Oid = 1772;
/// `F_TO_CHAR_INT4_TEXT` (prosrc `int4_to_char`, pg_proc 1773).
pub const F_TO_CHAR_INT4_TEXT: Oid = 1773;
/// `F_TO_CHAR_INT8_TEXT` (prosrc `int8_to_char`, pg_proc 1774).
pub const F_TO_CHAR_INT8_TEXT: Oid = 1774;
/// `F_TO_CHAR_FLOAT4_TEXT` (prosrc `float4_to_char`, pg_proc 1775).
pub const F_TO_CHAR_FLOAT4_TEXT: Oid = 1775;
/// `F_TO_CHAR_FLOAT8_TEXT` (prosrc `float8_to_char`, pg_proc 1776).
pub const F_TO_CHAR_FLOAT8_TEXT: Oid = 1776;
/// `F_TO_NUMBER` (prosrc `numeric_to_number`, pg_proc 1777).
pub const F_TO_NUMBER: Oid = 1777;

/// `VARHDRSZ` — the uncompressed varlena length-word size, in bytes.
const VARHDRSZ: usize = 4;

// ===========================================================================
// Varlena marshalling (byte-identical to builtins-io / varchar).
// ===========================================================================

/// `VARDATA_ANY` of a full `struct varlena *` image: skip ONE header byte for a
/// short (1-byte, low-bit-set) header, else `VARHDRSZ` (4). A small stored value
/// reaches an fmgr arg verbatim (the EEOP_FUNCEXPR boundary does not
/// detoast/unpack); a fixed 4-byte strip would drop three payload bytes once
/// `SHORT_VARLENA_PACKING` is on. No-op while the flag is off (every value is 4B).
fn varlena_payload(image: &[u8]) -> &[u8] {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
    }
}

/// `cstring_to_text`'s `palloc(len + VARHDRSZ)` + `SET_VARSIZE` + `memcpy`:
/// build a full 4-byte-header varlena image from a header-stripped payload,
/// charged to `mcx`.
fn varlena_image_from_payload<'mcx>(
    mcx: Mcx<'mcx>,
    payload: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    let total = payload.len() + VARHDRSZ;
    let mut img = vec_with_capacity_in(mcx, total)?;
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(payload);
    Ok(img)
}

/// Borrow the header-stripped VARDATA payload of a by-reference `text`
/// argument ([`RefPayload::Varlena`] full image, or a [`RefPayload::Cstring`]
/// verbatim). Errors loudly on any other kind.
fn arg_text_payload<'a>(arg: &'a FmgrArg<'a, '_>) -> PgResult<&'a [u8]> {
    match arg {
        FmgrArg::Ref(RefPayload::Varlena(b)) => Ok(varlena_payload(b.as_slice())),
        FmgrArg::Ref(RefPayload::Cstring(s)) => Ok(s.as_bytes()),
        _ => Err(PgError::error(
            "to_char/to_number fmgr arg: expected a by-reference text varlena",
        )),
    }
}

/// Borrow the on-disk numeric image bytes of a by-reference `numeric` argument.
fn arg_numeric_bytes<'a>(arg: &'a FmgrArg<'a, '_>) -> PgResult<&'a [u8]> {
    match arg {
        FmgrArg::Ref(RefPayload::Varlena(b)) => Ok(b.as_slice()),
        _ => Err(PgError::error(
            "to_char fmgr arg: expected a by-reference numeric varlena",
        )),
    }
}

// ===========================================================================
// Typed Option-B entry points (the marshal layer — NO datatype logic).
// ===========================================================================

/// `numeric_to_char(PG_GETARG_NUMERIC(0), PG_GETARG_TEXT_PP(1))`
/// (formatting.c:6383): by-ref numeric + by-ref text format -> text image.
pub fn numeric_to_char_boundary<'mcx>(
    mcx: Mcx<'mcx>,
    value: &FmgrArg<'_, '_>,
    fmt: &FmgrArg<'_, '_>,
    collid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let var = set_var_from_num(mcx, arg_numeric_bytes(value)?)?;
    let bytes = numeric_to_char(mcx, &var, arg_text_payload(fmt)?, collid)?;
    varlena_image_from_payload(mcx, &bytes)
}

/// `numeric_to_number` (formatting.c:6324): by-ref text value + by-ref text
/// format -> by-ref numeric image, or SQL NULL (`None`) for the C
/// `PG_RETURN_NULL()` empty/oversized-format arm.
pub fn numeric_to_number_boundary<'mcx>(
    mcx: Mcx<'mcx>,
    value: &FmgrArg<'_, '_>,
    fmt: &FmgrArg<'_, '_>,
    collid: Oid,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    match numeric_to_number(mcx, arg_text_payload(value)?, arg_text_payload(fmt)?, collid)? {
        Some(var) => Ok(Some(make_result(mcx, &var)?)),
        None => Ok(None),
    }
}

/// `int4_to_char(PG_GETARG_INT32(0), PG_GETARG_TEXT_PP(1))` (formatting.c:6512).
pub fn int4_to_char_boundary<'mcx>(
    mcx: Mcx<'mcx>,
    value: i32,
    fmt: &FmgrArg<'_, '_>,
    collid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let bytes = int4_to_char(value, arg_text_payload(fmt)?, collid)?;
    varlena_image_from_payload(mcx, &bytes)
}

/// `int8_to_char(PG_GETARG_INT64(0), PG_GETARG_TEXT_PP(1))` (formatting.c:6606).
pub fn int8_to_char_boundary<'mcx>(
    mcx: Mcx<'mcx>,
    value: i64,
    fmt: &FmgrArg<'_, '_>,
    collid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let bytes = int8_to_char(mcx, value, arg_text_payload(fmt)?, collid)?;
    varlena_image_from_payload(mcx, &bytes)
}

/// `float4_to_char(PG_GETARG_FLOAT4(0), PG_GETARG_TEXT_PP(1))`
/// (formatting.c:6718).
pub fn float4_to_char_boundary<'mcx>(
    mcx: Mcx<'mcx>,
    value: f32,
    fmt: &FmgrArg<'_, '_>,
    collid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let bytes = float4_to_char(value, arg_text_payload(fmt)?, collid)?;
    varlena_image_from_payload(mcx, &bytes)
}

/// `float8_to_char(PG_GETARG_FLOAT8(0), PG_GETARG_TEXT_PP(1))`
/// (formatting.c:6831).
pub fn float8_to_char_boundary<'mcx>(
    mcx: Mcx<'mcx>,
    value: f64,
    fmt: &FmgrArg<'_, '_>,
    collid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let bytes = float8_to_char(value, arg_text_payload(fmt)?, collid)?;
    varlena_image_from_payload(mcx, &bytes)
}

/// `timestamp_to_char(PG_GETARG_TIMESTAMP(0), PG_GETARG_TEXT_PP(1))`
/// (formatting.c:4011): by-value `timestamp` + by-ref text format -> text image,
/// or SQL NULL (`None`) for the empty-format / non-finite-input arm.
pub fn timestamp_to_char_boundary<'mcx>(
    mcx: Mcx<'mcx>,
    dt: Timestamp,
    fmt: &FmgrArg<'_, '_>,
    collid: Oid,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    match timestamp_to_char(mcx, dt, arg_text_payload(fmt)?, collid)? {
        Some(bytes) => Ok(Some(varlena_image_from_payload(mcx, &bytes)?)),
        None => Ok(None),
    }
}

/// `timestamptz_to_char(PG_GETARG_TIMESTAMPTZ(0), PG_GETARG_TEXT_PP(1))`
/// (formatting.c:4046): by-value `timestamptz` + by-ref text format -> text
/// image, or SQL NULL (`None`).
pub fn timestamptz_to_char_boundary<'mcx>(
    mcx: Mcx<'mcx>,
    dt: Timestamp,
    fmt: &FmgrArg<'_, '_>,
    collid: Oid,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    match timestamptz_to_char(mcx, dt, arg_text_payload(fmt)?, collid)? {
        Some(bytes) => Ok(Some(varlena_image_from_payload(mcx, &bytes)?)),
        None => Ok(None),
    }
}

/// `interval_to_char(PG_GETARG_INTERVAL_P(0), PG_GETARG_TEXT_PP(1))`
/// (formatting.c:4087): by-ref `interval` + by-ref text format -> text image,
/// or SQL NULL (`None`).
pub fn interval_to_char_boundary<'mcx>(
    mcx: Mcx<'mcx>,
    it: &Interval,
    fmt: &FmgrArg<'_, '_>,
    collid: Oid,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    match interval_to_char(mcx, it, arg_text_payload(fmt)?, collid)? {
        Some(bytes) => Ok(Some(varlena_image_from_payload(mcx, &bytes)?)),
        None => Ok(None),
    }
}
