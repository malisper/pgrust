//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for `acl.c`'s
//! SQL-callable functions: the `aclitem` type I/O + hashing + equality +
//! `makeaclitem`, the deprecated `aclinsert`/`aclremove` error stubs, and the
//! `has_*_privilege` / `pg_has_role` read families.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the
//! `types_fmgr` fmgr call frame (`PG_GETARG_*`), calls the matching value core
//! in [`crate::has_privilege`] / [`crate::aclitem_io`] / [`crate::acl_ops`], and
//! writes the result back (`PG_RETURN_*`). A `text` arg arrives as its detoasted
//! `VARDATA_ANY` payload on the by-ref lane; a `name` arg as its fixed
//! `NAMEDATALEN` buffer; an `aclitem` (a fixed 16-byte by-reference type) as its
//! raw `repr(C)` bytes on the by-ref lane. `oid`/`int2`/`int8`/`bool` cross by
//! value. A SQL NULL result (`Ok(None)` from a core) sets `fcinfo->isnull`.
//!
//! [`register_acl_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`). OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat`.
//!
//! `aclcontains` (1037, `aclitem[] @> aclitem`) and `acldefault_sql` (3943,
//! `(char, oid) -> aclitem[]`) ARE registered here: their `aclitem[]`
//! (`ArrayType`) argument / result is marshalled through the owner's value-lane
//! `deconstruct_array_values_bytes` / `construct_array_values` (`arg_acl_array`
//! / `ret_acl_array`), so the fixed-16-byte `'d'`-aligned elements cross the
//! by-ref boundary cleanly.
//!
//! NOT registered here (and still listed in the seams-init builtin gap
//! baseline): `aclexplode` (1689) and `pg_get_acl` (6385) — the former uses the
//! set-returning-function machinery, the latter's catalog-lookup core is not yet
//! ported.

use alloc::string::String;
use alloc::vec::Vec;

use types_acl::AclItem;
use types_core::{AttrNumber, Oid};
use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use crate::has_privilege as hp;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)` → `DatumGetObjectId`.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.arg(i).expect("acl fn: missing oid arg").value.as_oid()
}

/// `PG_GETARG_INT16(i)` → `DatumGetInt16` (an attribute number).
#[inline]
fn arg_int16(fcinfo: &FunctionCallInfoBaseData, i: usize) -> AttrNumber {
    fcinfo.arg(i).expect("acl fn: missing int2 arg").value.as_i16()
}

/// `PG_GETARG_INT64(i)` → `DatumGetInt64`.
#[inline]
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("acl fn: missing int8 arg").value.as_i64()
}

/// `PG_GETARG_BOOL(i)` → `DatumGetBool`.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("acl fn: missing bool arg").value.as_bool()
}

/// A `text` arg's detoasted `VARDATA_ANY` payload bytes on the by-ref lane
/// (C: `text_to_cstring(PG_GETARG_TEXT_PP(i))` reads exactly these bytes).
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("acl fn: text arg missing from by-ref lane");
    // VARDATA_ANY: skip the 4-byte varlena header on the header-ful image.
    &image[types_datum::varlena::VARHDRSZ..]
}

/// A `name` arg's fixed `NAMEDATALEN` buffer on the by-ref lane, NUL-trimmed
/// (C: `NameStr(*PG_GETARG_NAME(i))`).
#[inline]
fn arg_name<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let bytes = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("acl fn: name arg missing from by-ref lane");
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..end]).expect("acl fn: name arg not valid UTF-8")
}

/// A `cstring` arg on the by-ref lane (C: `PG_GETARG_CSTRING(i)`).
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("acl fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_ACLITEM_P(i)` — the 16-byte fixed-length `aclitem` on the by-ref
/// lane, decoded from its raw `repr(C)` image (`ai_grantee`, `ai_grantor`,
/// `ai_privs`, little-endian as the boundary stages it).
#[inline]
fn arg_aclitem(fcinfo: &FunctionCallInfoBaseData, i: usize) -> AclItem {
    let bytes = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("acl fn: aclitem arg missing from by-ref lane");
    aclitem_from_bytes(bytes)
}

/// Decode an `AclItem` from its 16-byte `repr(C)` image.
fn aclitem_from_bytes(bytes: &[u8]) -> AclItem {
    assert!(bytes.len() >= 16, "aclitem image too short");
    let grantee = u32::from_ne_bytes(bytes[0..4].try_into().unwrap());
    let grantor = u32::from_ne_bytes(bytes[4..8].try_into().unwrap());
    let privs = u64::from_ne_bytes(bytes[8..16].try_into().unwrap());
    AclItem { ai_grantee: grantee, ai_grantor: grantor, ai_privs: privs }
}

/// Encode an `AclItem` into its 16-byte `repr(C)` image.
fn aclitem_to_bytes(a: &AclItem) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&a.ai_grantee.to_ne_bytes());
    out.extend_from_slice(&a.ai_grantor.to_ne_bytes());
    out.extend_from_slice(&a.ai_privs.to_ne_bytes());
    out
}

/// `ACLITEMOID` — the element type of an `aclitem[]` (`_aclitem`) array.
const ACLITEMOID: Oid = 1033;
/// `aclitem` is a fixed 16-byte by-reference type with `'d'` (double) alignment
/// (`pg_type.dat`: `typlen => '16'`, `typbyval => 'f'`, `typalign => 'd'`).
const ACLITEM_LEN: i16 = 16;
const ACLITEM_ALIGN: core::ffi::c_char = b'd' as core::ffi::c_char;

/// `PG_GETARG_ACLITEM_ARRAY_P(i)` decode: deconstruct an `aclitem[]` arg's full
/// `ArrayType` varlena image (on the by-ref lane) into a `Vec<AclItem>`. The
/// owner's value-lane `deconstruct_array_values_bytes` detoasts and walks the
/// fixed-16-byte, `'d'`-aligned elements; each by-reference element arrives as a
/// [`types_tuple::Datum::ByRef`] carrying the verbatim 16 stored bytes, which
/// [`aclitem_from_bytes`] decodes. Any SQL-NULL element decodes to the all-zero
/// `AclItem` (the C `aclcontains` path runs over `ACL_NUM`/`ACL_DAT`, so a
/// genuine null-containing acl never reaches here; `check_acl` in the core
/// rejects the malformed shapes).
fn arg_acl_array<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    i: usize,
) -> types_error::PgResult<Vec<AclItem>> {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("acl fn: aclitem[] arg missing from by-ref lane");
    let elems = backend_utils_adt_arrayfuncs::construct::deconstruct_array_values_bytes(
        mcx,
        image,
        ACLITEMOID,
        ACLITEM_LEN,
        false,
        ACLITEM_ALIGN,
    )?;
    let mut out = Vec::with_capacity(elems.len());
    for (d, isnull) in elems.iter() {
        if *isnull {
            out.push(AclItem { ai_grantee: 0, ai_grantor: 0, ai_privs: 0 });
        } else {
            out.push(aclitem_from_bytes(d.as_ref_bytes()));
        }
    }
    Ok(out)
}

/// Write an `aclitem[]` result on the by-ref lane (C: `PG_RETURN_ACLITEM_P` of
/// a `construct_array` over the `Acl`). The owner's value-lane
/// `construct_array_values` builds the 1-D `ArrayType` image from per-element
/// [`types_tuple::Datum::ByRef`] (each the 16-byte `aclitem` repr).
fn ret_acl_array<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
    acl: &[AclItem],
) -> types_error::PgResult<Datum> {
    let mut values: Vec<types_tuple::Datum<'mcx>> = Vec::with_capacity(acl.len());
    for a in acl {
        let bytes = aclitem_to_bytes(a);
        let pv = mcx::slice_in(mcx, &bytes)?;
        values.push(types_tuple::Datum::ByRef(pv));
    }
    let image = backend_utils_adt_arrayfuncs::construct::construct_array_values(
        mcx,
        &values,
        ACLITEMOID,
        ACLITEM_LEN as i32,
        false,
        ACLITEM_ALIGN as u8,
    )?;
    fcinfo.isnull = false;
    fcinfo.set_ref_result(RefPayload::Varlena(image.as_slice().to_vec()));
    Ok(Datum::from_usize(0))
}

/// Write a `bool`-or-NULL result (C: `PG_RETURN_BOOL` / `PG_RETURN_NULL`).
#[inline]
fn ret_bool_opt(fcinfo: &mut FunctionCallInfoBaseData, v: Option<bool>) -> Datum {
    match v {
        Some(b) => {
            fcinfo.isnull = false;
            Datum::from_bool(b)
        }
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}

/// Write an `aclitem` result on the by-ref lane (C: `PG_RETURN_ACLITEM_P`).
#[inline]
fn ret_aclitem(fcinfo: &mut FunctionCallInfoBaseData, a: &AclItem) -> Datum {
    fcinfo.isnull = false;
    fcinfo.set_ref_result(RefPayload::Varlena(aclitem_to_bytes(a)));
    Datum::from_usize(0)
}

/// Write a `cstring` result on the by-ref lane (C: `PG_RETURN_CSTRING`).
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.isnull = false;
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("acl fmgr scratch")
}

// ---------------------------------------------------------------------------
// aclitem type I/O + hashing + equality + makeaclitem.
// ---------------------------------------------------------------------------

fn fc_aclitemin(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: aclitemin(cstring) — forward the soft ErrorSaveContext installed on the
    // fmgr frame by InputFunctionCallSafe so a recoverable parse failure
    // `ereturn`s into the sink (returning `Ok(None)`) instead of throwing past
    // `invoke?`. `s` is an owned copy so it does not conflict with the mutable
    // `escontext_mut` borrow.
    let s = arg_cstring(fcinfo, 0).as_bytes().to_vec();
    let res = crate::aclitem_io::aclitemin(&s, fcinfo.escontext_mut())?;
    // Soft-error path: escontext recorded the failure; return a NULL placeholder
    // the caller discards after `soft_error_occurred()`.
    let parsed = match res {
        Some(p) => p,
        None => return Ok(Datum::null()),
    };
    // ereport(WARNING) for a defaulted grantor (acl.c).
    if let Some(w) = parsed.warning {
        let _ = backend_utils_error_elog_seams::ereport_msg::call(
            types_error::WARNING,
            w.message().into(),
            w.detail().map(Into::into),
        );
    }
    Ok(ret_aclitem(fcinfo, &parsed.item))
}

fn fc_aclitemout(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let aip = arg_aclitem(fcinfo, 0);
    let m = scratch_mcx();
    let out = crate::aclitem_io::aclitemout(m.mcx(), &aip)?;
    Ok(ret_cstring(fcinfo, String::from_utf8_lossy(&out).into_owned()))
}

fn fc_aclitem_eq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a1 = arg_aclitem(fcinfo, 0);
    let a2 = arg_aclitem(fcinfo, 1);
    fcinfo.isnull = false;
    Ok(Datum::from_bool(crate::aclitem_io::aclitem_eq(&a1, &a2)))
}

fn fc_hash_aclitem(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_aclitem(fcinfo, 0);
    fcinfo.isnull = false;
    Ok(Datum::from_i32(crate::aclitem_io::hash_aclitem(&a) as i32))
}

fn fc_hash_aclitem_extended(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_aclitem(fcinfo, 0);
    let seed = arg_int64(fcinfo, 1) as u64;
    fcinfo.isnull = false;
    Ok(Datum::from_i64(crate::aclitem_io::hash_aclitem_extended(&a, seed) as i64))
}

fn fc_makeaclitem(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: makeaclitem(oid grantee, oid grantor, text privileges, bool is_grantable).
    let grantee = arg_oid(fcinfo, 0);
    let grantor = arg_oid(fcinfo, 1);
    let privtext = String::from_utf8_lossy(arg_text(fcinfo, 2)).into_owned();
    let goption = arg_bool(fcinfo, 3);
    let item = crate::acl_ops::makeaclitem_impl(grantee, grantor, &privtext, goption)?;
    Ok(ret_aclitem(fcinfo, &item))
}

/// `aclcontains(_aclitem, aclitem) -> bool` (oid 1037): true iff some entry of
/// the `Acl` array has the same grantee+grantor as the probe and includes all
/// of its rights. C deconstructs `PG_GETARG_ACLITEM_P(1)` against the `Acl`
/// array `PG_GETARG_ACL_P(0)` (`acl.c` `aclcontains`).
fn fc_aclcontains(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let acl = arg_acl_array(m.mcx(), fcinfo, 0)?;
    let aip = arg_aclitem(fcinfo, 1);
    let r = crate::acl_ops::aclcontains_impl(&acl, &aip)?;
    fcinfo.isnull = false;
    Ok(Datum::from_bool(r))
}

/// `acldefault_sql(char, oid) -> _aclitem` (oid 3943): the default ACL for an
/// object of the given type-abbreviation owned by the given role, materialized
/// as an `aclitem[]` (`acl.c` `acldefault_sql`).
fn fc_acldefault_sql(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    // PG_GETARG_CHAR(0): the single "char" type-abbreviation byte.
    let objtypec = fcinfo.arg(0).expect("acl fn: missing char arg").value.as_i8();
    let owner = arg_oid(fcinfo, 1);
    let acl = crate::acldefault::acldefault_sql(m.mcx(), objtypec, owner)?;
    ret_acl_array(m.mcx(), fcinfo, acl)
}

fn fc_aclinsert(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: deprecated; always ereport(ERROR).
    let _ = fcinfo;
    crate::acl_ops::aclinsert()?;
    Ok(Datum::from_usize(0))
}

fn fc_aclremove(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let _ = fcinfo;
    crate::acl_ops::aclremove()?;
    Ok(Datum::from_usize(0))
}

// ---------------------------------------------------------------------------
// has_*_privilege / pg_has_role read families.
//
// Three argument shapes recur: text object (`arg_text`), name role
// (`arg_name`), oid (`arg_oid`); each `fc_` adapter reads its row's shape, calls
// the matching `hp::` core, and writes the bool-or-NULL result.
// ---------------------------------------------------------------------------

/// The mcx every name-resolving core allocates through (a transient scratch
/// context; C: the call's `CurrentMemoryContext`).
macro_rules! with_mcx {
    ($body:expr) => {{
        let m = scratch_mcx();
        let mcx = m.mcx();
        $body(mcx)
    }};
}

// --- table ---
fn fc_has_table_privilege_name_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_table_privilege_name_name(
        mcx, arg_name(f, 0), arg_text(f, 1), arg_text(f, 2)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_table_privilege_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_table_privilege_name(
        mcx, current_user(), arg_text(f, 0), arg_text(f, 1)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_table_privilege_name_id(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::has_table_privilege_name_id(arg_name(f, 0), arg_oid(f, 1), arg_text(f, 2))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_table_privilege_id(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::has_table_privilege_id(current_user(), arg_oid(f, 0), arg_text(f, 1))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_table_privilege_id_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_table_privilege_id_name(
        mcx, arg_oid(f, 0), arg_text(f, 1), arg_text(f, 2)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_table_privilege_id_id(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::has_table_privilege_id_id(arg_oid(f, 0), arg_oid(f, 1), arg_text(f, 2))?;
    Ok(ret_bool_opt(f, v))
}

// --- sequence ---
fn fc_has_sequence_privilege_name_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_sequence_privilege_name_name(
        mcx, arg_name(f, 0), arg_text(f, 1), arg_text(f, 2)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_sequence_privilege_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_sequence_privilege_name(
        mcx, current_user(), arg_text(f, 0), arg_text(f, 1)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_sequence_privilege_name_id(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_sequence_privilege_name_id(
        mcx, arg_name(f, 0), arg_oid(f, 1), arg_text(f, 2)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_sequence_privilege_id(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_sequence_privilege_id(
        mcx, current_user(), arg_oid(f, 0), arg_text(f, 1)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_sequence_privilege_id_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_sequence_privilege_id_name(
        mcx, arg_oid(f, 0), arg_text(f, 1), arg_text(f, 2)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_sequence_privilege_id_id(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_sequence_privilege_id_id(
        mcx, arg_oid(f, 0), arg_oid(f, 1), arg_text(f, 2)
    ))?;
    Ok(ret_bool_opt(f, v))
}

// --- any column ---
fn fc_has_any_column_privilege_name_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_any_column_privilege_name_name(
        mcx, arg_name(f, 0), arg_text(f, 1), arg_text(f, 2)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_any_column_privilege_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_any_column_privilege_name(
        mcx, current_user(), arg_text(f, 0), arg_text(f, 1)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_any_column_privilege_name_id(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::has_any_column_privilege_name_id(arg_name(f, 0), arg_oid(f, 1), arg_text(f, 2))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_any_column_privilege_id(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::has_any_column_privilege_id(current_user(), arg_oid(f, 0), arg_text(f, 1))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_any_column_privilege_id_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_any_column_privilege_id_name(
        mcx, arg_oid(f, 0), arg_text(f, 1), arg_text(f, 2)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_any_column_privilege_id_id(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::has_any_column_privilege_id_id(arg_oid(f, 0), arg_oid(f, 1), arg_text(f, 2))?;
    Ok(ret_bool_opt(f, v))
}

// --- column ---
fn fc_has_column_privilege_name_name_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_column_privilege_name_name_name(
        mcx, arg_name(f, 0), arg_text(f, 1), arg_text(f, 2), arg_text(f, 3)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_column_privilege_name_name_attnum(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_column_privilege_name_name_attnum(
        mcx, arg_name(f, 0), arg_text(f, 1), arg_int16(f, 2), arg_text(f, 3)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_column_privilege_name_id_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_column_privilege_name_id_name(
        mcx, arg_name(f, 0), arg_oid(f, 1), arg_text(f, 2), arg_text(f, 3)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_column_privilege_name_id_attnum(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::has_column_privilege_name_id_attnum(
        arg_name(f, 0), arg_oid(f, 1), arg_int16(f, 2), arg_text(f, 3),
    )?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_column_privilege_id_name_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_column_privilege_id_name_name(
        mcx, arg_oid(f, 0), arg_text(f, 1), arg_text(f, 2), arg_text(f, 3)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_column_privilege_id_name_attnum(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_column_privilege_id_name_attnum(
        mcx, arg_oid(f, 0), arg_text(f, 1), arg_int16(f, 2), arg_text(f, 3)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_column_privilege_id_id_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_column_privilege_id_id_name(
        mcx, arg_oid(f, 0), arg_oid(f, 1), arg_text(f, 2), arg_text(f, 3)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_column_privilege_id_id_attnum(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::has_column_privilege_id_id_attnum(
        arg_oid(f, 0), arg_oid(f, 1), arg_int16(f, 2), arg_text(f, 3),
    )?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_column_privilege_name_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_column_privilege_name_name(
        mcx, current_user(), arg_text(f, 0), arg_text(f, 1), arg_text(f, 2)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_column_privilege_name_attnum(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_column_privilege_name_attnum(
        mcx, current_user(), arg_text(f, 0), arg_int16(f, 1), arg_text(f, 2)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_column_privilege_id_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = with_mcx!(|mcx| hp::has_column_privilege_id_name(
        mcx, current_user(), arg_oid(f, 0), arg_text(f, 1), arg_text(f, 2)
    ))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_column_privilege_id_attnum(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::has_column_privilege_id_attnum(
        current_user(), arg_oid(f, 0), arg_int16(f, 1), arg_text(f, 2),
    )?;
    Ok(ret_bool_opt(f, v))
}

// --- object-class families (database/fdw/function/language/schema/server/
//     tablespace/type): all six variants share the byname/byid argument
//     shapes. Generated by a macro per class. ---
macro_rules! object_class_fcs {
    ($nn:ident => $core_nn:path, $n:ident => $core_n:path, $ni:ident => $core_ni:path,
     $i:ident => $core_i:path, $in_:ident => $core_in:path, $ii:ident => $core_ii:path) => {
        fn $nn(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let v = with_mcx!(|mcx| $core_nn(mcx, arg_name(f, 0), arg_text(f, 1), arg_text(f, 2)))?;
            Ok(ret_bool_opt(f, v))
        }
        fn $n(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let v = with_mcx!(|mcx| $core_n(mcx, current_user(), arg_text(f, 0), arg_text(f, 1)))?;
            Ok(ret_bool_opt(f, v))
        }
        fn $ni(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let v = $core_ni(arg_name(f, 0), arg_oid(f, 1), arg_text(f, 2))?;
            Ok(ret_bool_opt(f, v))
        }
        fn $i(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let v = $core_i(current_user(), arg_oid(f, 0), arg_text(f, 1))?;
            Ok(ret_bool_opt(f, v))
        }
        fn $in_(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let v = with_mcx!(|mcx| $core_in(mcx, arg_oid(f, 0), arg_text(f, 1), arg_text(f, 2)))?;
            Ok(ret_bool_opt(f, v))
        }
        fn $ii(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let v = $core_ii(arg_oid(f, 0), arg_oid(f, 1), arg_text(f, 2))?;
            Ok(ret_bool_opt(f, v))
        }
    };
}

object_class_fcs!(
    fc_has_database_privilege_name_name => hp::has_database_privilege_name_name,
    fc_has_database_privilege_name => hp::has_database_privilege_name,
    fc_has_database_privilege_name_id => hp::has_database_privilege_name_id,
    fc_has_database_privilege_id => hp::has_database_privilege_id,
    fc_has_database_privilege_id_name => hp::has_database_privilege_id_name,
    fc_has_database_privilege_id_id => hp::has_database_privilege_id_id
);
object_class_fcs!(
    fc_has_fdw_privilege_name_name => hp::has_foreign_data_wrapper_privilege_name_name,
    fc_has_fdw_privilege_name => hp::has_foreign_data_wrapper_privilege_name,
    fc_has_fdw_privilege_name_id => hp::has_foreign_data_wrapper_privilege_name_id,
    fc_has_fdw_privilege_id => hp::has_foreign_data_wrapper_privilege_id,
    fc_has_fdw_privilege_id_name => hp::has_foreign_data_wrapper_privilege_id_name,
    fc_has_fdw_privilege_id_id => hp::has_foreign_data_wrapper_privilege_id_id
);
object_class_fcs!(
    fc_has_function_privilege_name_name => hp::has_function_privilege_name_name,
    fc_has_function_privilege_name => hp::has_function_privilege_name,
    fc_has_function_privilege_name_id => hp::has_function_privilege_name_id,
    fc_has_function_privilege_id => hp::has_function_privilege_id,
    fc_has_function_privilege_id_name => hp::has_function_privilege_id_name,
    fc_has_function_privilege_id_id => hp::has_function_privilege_id_id
);
object_class_fcs!(
    fc_has_language_privilege_name_name => hp::has_language_privilege_name_name,
    fc_has_language_privilege_name => hp::has_language_privilege_name,
    fc_has_language_privilege_name_id => hp::has_language_privilege_name_id,
    fc_has_language_privilege_id => hp::has_language_privilege_id,
    fc_has_language_privilege_id_name => hp::has_language_privilege_id_name,
    fc_has_language_privilege_id_id => hp::has_language_privilege_id_id
);
object_class_fcs!(
    fc_has_schema_privilege_name_name => hp::has_schema_privilege_name_name,
    fc_has_schema_privilege_name => hp::has_schema_privilege_name,
    fc_has_schema_privilege_name_id => hp::has_schema_privilege_name_id,
    fc_has_schema_privilege_id => hp::has_schema_privilege_id,
    fc_has_schema_privilege_id_name => hp::has_schema_privilege_id_name,
    fc_has_schema_privilege_id_id => hp::has_schema_privilege_id_id
);
object_class_fcs!(
    fc_has_server_privilege_name_name => hp::has_server_privilege_name_name,
    fc_has_server_privilege_name => hp::has_server_privilege_name,
    fc_has_server_privilege_name_id => hp::has_server_privilege_name_id,
    fc_has_server_privilege_id => hp::has_server_privilege_id,
    fc_has_server_privilege_id_name => hp::has_server_privilege_id_name,
    fc_has_server_privilege_id_id => hp::has_server_privilege_id_id
);
object_class_fcs!(
    fc_has_tablespace_privilege_name_name => hp::has_tablespace_privilege_name_name,
    fc_has_tablespace_privilege_name => hp::has_tablespace_privilege_name,
    fc_has_tablespace_privilege_name_id => hp::has_tablespace_privilege_name_id,
    fc_has_tablespace_privilege_id => hp::has_tablespace_privilege_id,
    fc_has_tablespace_privilege_id_name => hp::has_tablespace_privilege_id_name,
    fc_has_tablespace_privilege_id_id => hp::has_tablespace_privilege_id_id
);
object_class_fcs!(
    fc_has_type_privilege_name_name => hp::has_type_privilege_name_name,
    fc_has_type_privilege_name => hp::has_type_privilege_name,
    fc_has_type_privilege_name_id => hp::has_type_privilege_name_id,
    fc_has_type_privilege_id => hp::has_type_privilege_id,
    fc_has_type_privilege_id_name => hp::has_type_privilege_id_name,
    fc_has_type_privilege_id_id => hp::has_type_privilege_id_id
);

// --- parameter ---
fn fc_has_parameter_privilege_name_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::has_parameter_privilege_name_name(arg_name(f, 0), arg_text(f, 1), arg_text(f, 2))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_parameter_privilege_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::has_parameter_privilege_name(current_user(), arg_text(f, 0), arg_text(f, 1))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_parameter_privilege_id_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::has_parameter_privilege_id_name(arg_oid(f, 0), arg_text(f, 1), arg_text(f, 2))?;
    Ok(ret_bool_opt(f, v))
}

// --- largeobject ---
fn fc_has_largeobject_privilege_name_id(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::has_largeobject_privilege_name_id(arg_name(f, 0), arg_oid(f, 1), arg_text(f, 2))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_largeobject_privilege_id(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::has_largeobject_privilege_id(current_user(), arg_oid(f, 0), arg_text(f, 1))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_has_largeobject_privilege_id_id(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::has_largeobject_privilege_id_id(arg_oid(f, 0), arg_oid(f, 1), arg_text(f, 2))?;
    Ok(ret_bool_opt(f, v))
}

// --- pg_has_role ---
fn fc_pg_has_role_name_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::pg_has_role_name_name(arg_name(f, 0), arg_name(f, 1), arg_text(f, 2))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_pg_has_role_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::pg_has_role_name(current_user(), arg_name(f, 0), arg_text(f, 1))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_pg_has_role_name_id(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::pg_has_role_name_id(arg_name(f, 0), arg_oid(f, 1), arg_text(f, 2))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_pg_has_role_id(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::pg_has_role_id(current_user(), arg_oid(f, 0), arg_text(f, 1))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_pg_has_role_id_name(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::pg_has_role_id_name(arg_oid(f, 0), arg_name(f, 1), arg_text(f, 2))?;
    Ok(ret_bool_opt(f, v))
}
fn fc_pg_has_role_id_id(f: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let v = hp::pg_has_role_id_id(arg_oid(f, 0), arg_oid(f, 1), arg_text(f, 2))?;
    Ok(ret_bool_opt(f, v))
}

/// `GetUserId()` (miscinit) — the current user OID the `_name`/`_id`
/// (no-role-arg) variants check against.
fn current_user() -> Oid {
    backend_utils_init_miscinit_seams::get_user_id::call()
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    retset: bool,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction { foid, name: name.to_string(), nargs, strict: true, retset, func: None },
        native,
    )
}

/// Register every `acl.c` fmgr builtin whose value core is ported and whose
/// arg/result types are expressible at the fmgr boundary (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`. OIDs/nargs
/// from `pg_proc.dat`; every row here is `proisstrict => 't'` and not retset.
pub fn register_acl_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        // ---- aclitem type ----
        builtin(329, "hash_aclitem", 1, false, fc_hash_aclitem),
        builtin(777, "hash_aclitem_extended", 2, false, fc_hash_aclitem_extended),
        builtin(1031, "aclitemin", 1, false, fc_aclitemin),
        builtin(1032, "aclitemout", 1, false, fc_aclitemout),
        builtin(1035, "aclinsert", 2, false, fc_aclinsert),
        builtin(1036, "aclremove", 2, false, fc_aclremove),
        builtin(1062, "aclitem_eq", 2, false, fc_aclitem_eq),
        builtin(1037, "aclcontains", 2, false, fc_aclcontains),
        builtin(1365, "makeaclitem", 4, false, fc_makeaclitem),
        builtin(3943, "acldefault_sql", 2, false, fc_acldefault_sql),
        // ---- has_table_privilege ----
        builtin(1922, "has_table_privilege_name_name", 3, false, fc_has_table_privilege_name_name),
        builtin(1923, "has_table_privilege_name_id", 3, false, fc_has_table_privilege_name_id),
        builtin(1924, "has_table_privilege_id_name", 3, false, fc_has_table_privilege_id_name),
        builtin(1925, "has_table_privilege_id_id", 3, false, fc_has_table_privilege_id_id),
        builtin(1926, "has_table_privilege_name", 2, false, fc_has_table_privilege_name),
        builtin(1927, "has_table_privilege_id", 2, false, fc_has_table_privilege_id),
        // ---- has_sequence_privilege ----
        builtin(2181, "has_sequence_privilege_name_name", 3, false, fc_has_sequence_privilege_name_name),
        builtin(2182, "has_sequence_privilege_name_id", 3, false, fc_has_sequence_privilege_name_id),
        builtin(2183, "has_sequence_privilege_id_name", 3, false, fc_has_sequence_privilege_id_name),
        builtin(2184, "has_sequence_privilege_id_id", 3, false, fc_has_sequence_privilege_id_id),
        builtin(2185, "has_sequence_privilege_name", 2, false, fc_has_sequence_privilege_name),
        builtin(2186, "has_sequence_privilege_id", 2, false, fc_has_sequence_privilege_id),
        // ---- has_database_privilege ----
        builtin(2250, "has_database_privilege_name_name", 3, false, fc_has_database_privilege_name_name),
        builtin(2251, "has_database_privilege_name_id", 3, false, fc_has_database_privilege_name_id),
        builtin(2252, "has_database_privilege_id_name", 3, false, fc_has_database_privilege_id_name),
        builtin(2253, "has_database_privilege_id_id", 3, false, fc_has_database_privilege_id_id),
        builtin(2254, "has_database_privilege_name", 2, false, fc_has_database_privilege_name),
        builtin(2255, "has_database_privilege_id", 2, false, fc_has_database_privilege_id),
        // ---- has_function_privilege ----
        builtin(2256, "has_function_privilege_name_name", 3, false, fc_has_function_privilege_name_name),
        builtin(2257, "has_function_privilege_name_id", 3, false, fc_has_function_privilege_name_id),
        builtin(2258, "has_function_privilege_id_name", 3, false, fc_has_function_privilege_id_name),
        builtin(2259, "has_function_privilege_id_id", 3, false, fc_has_function_privilege_id_id),
        builtin(2260, "has_function_privilege_name", 2, false, fc_has_function_privilege_name),
        builtin(2261, "has_function_privilege_id", 2, false, fc_has_function_privilege_id),
        // ---- has_language_privilege ----
        builtin(2262, "has_language_privilege_name_name", 3, false, fc_has_language_privilege_name_name),
        builtin(2263, "has_language_privilege_name_id", 3, false, fc_has_language_privilege_name_id),
        builtin(2264, "has_language_privilege_id_name", 3, false, fc_has_language_privilege_id_name),
        builtin(2265, "has_language_privilege_id_id", 3, false, fc_has_language_privilege_id_id),
        builtin(2266, "has_language_privilege_name", 2, false, fc_has_language_privilege_name),
        builtin(2267, "has_language_privilege_id", 2, false, fc_has_language_privilege_id),
        // ---- has_schema_privilege ----
        builtin(2268, "has_schema_privilege_name_name", 3, false, fc_has_schema_privilege_name_name),
        builtin(2269, "has_schema_privilege_name_id", 3, false, fc_has_schema_privilege_name_id),
        builtin(2270, "has_schema_privilege_id_name", 3, false, fc_has_schema_privilege_id_name),
        builtin(2271, "has_schema_privilege_id_id", 3, false, fc_has_schema_privilege_id_id),
        builtin(2272, "has_schema_privilege_name", 2, false, fc_has_schema_privilege_name),
        builtin(2273, "has_schema_privilege_id", 2, false, fc_has_schema_privilege_id),
        // ---- has_tablespace_privilege ----
        builtin(2390, "has_tablespace_privilege_name_name", 3, false, fc_has_tablespace_privilege_name_name),
        builtin(2391, "has_tablespace_privilege_name_id", 3, false, fc_has_tablespace_privilege_name_id),
        builtin(2392, "has_tablespace_privilege_id_name", 3, false, fc_has_tablespace_privilege_id_name),
        builtin(2393, "has_tablespace_privilege_id_id", 3, false, fc_has_tablespace_privilege_id_id),
        builtin(2394, "has_tablespace_privilege_name", 2, false, fc_has_tablespace_privilege_name),
        builtin(2395, "has_tablespace_privilege_id", 2, false, fc_has_tablespace_privilege_id),
        // ---- pg_has_role ----
        builtin(2705, "pg_has_role_name_name", 3, false, fc_pg_has_role_name_name),
        builtin(2706, "pg_has_role_name_id", 3, false, fc_pg_has_role_name_id),
        builtin(2707, "pg_has_role_id_name", 3, false, fc_pg_has_role_id_name),
        builtin(2708, "pg_has_role_id_id", 3, false, fc_pg_has_role_id_id),
        builtin(2709, "pg_has_role_name", 2, false, fc_pg_has_role_name),
        builtin(2710, "pg_has_role_id", 2, false, fc_pg_has_role_id),
        // ---- has_foreign_data_wrapper_privilege ----
        builtin(3000, "has_foreign_data_wrapper_privilege_name_name", 3, false, fc_has_fdw_privilege_name_name),
        builtin(3001, "has_foreign_data_wrapper_privilege_name_id", 3, false, fc_has_fdw_privilege_name_id),
        builtin(3002, "has_foreign_data_wrapper_privilege_id_name", 3, false, fc_has_fdw_privilege_id_name),
        builtin(3003, "has_foreign_data_wrapper_privilege_id_id", 3, false, fc_has_fdw_privilege_id_id),
        builtin(3004, "has_foreign_data_wrapper_privilege_name", 2, false, fc_has_fdw_privilege_name),
        builtin(3005, "has_foreign_data_wrapper_privilege_id", 2, false, fc_has_fdw_privilege_id),
        // ---- has_server_privilege ----
        builtin(3006, "has_server_privilege_name_name", 3, false, fc_has_server_privilege_name_name),
        builtin(3007, "has_server_privilege_name_id", 3, false, fc_has_server_privilege_name_id),
        builtin(3008, "has_server_privilege_id_name", 3, false, fc_has_server_privilege_id_name),
        builtin(3009, "has_server_privilege_id_id", 3, false, fc_has_server_privilege_id_id),
        builtin(3010, "has_server_privilege_name", 2, false, fc_has_server_privilege_name),
        builtin(3011, "has_server_privilege_id", 2, false, fc_has_server_privilege_id),
        // ---- has_column_privilege ----
        builtin(3012, "has_column_privilege_name_name_name", 4, false, fc_has_column_privilege_name_name_name),
        builtin(3013, "has_column_privilege_name_name_attnum", 4, false, fc_has_column_privilege_name_name_attnum),
        builtin(3014, "has_column_privilege_name_id_name", 4, false, fc_has_column_privilege_name_id_name),
        builtin(3015, "has_column_privilege_name_id_attnum", 4, false, fc_has_column_privilege_name_id_attnum),
        builtin(3016, "has_column_privilege_id_name_name", 4, false, fc_has_column_privilege_id_name_name),
        builtin(3017, "has_column_privilege_id_name_attnum", 4, false, fc_has_column_privilege_id_name_attnum),
        builtin(3018, "has_column_privilege_id_id_name", 4, false, fc_has_column_privilege_id_id_name),
        builtin(3019, "has_column_privilege_id_id_attnum", 4, false, fc_has_column_privilege_id_id_attnum),
        builtin(3020, "has_column_privilege_name_name", 3, false, fc_has_column_privilege_name_name),
        builtin(3021, "has_column_privilege_name_attnum", 3, false, fc_has_column_privilege_name_attnum),
        builtin(3022, "has_column_privilege_id_name", 3, false, fc_has_column_privilege_id_name),
        builtin(3023, "has_column_privilege_id_attnum", 3, false, fc_has_column_privilege_id_attnum),
        // ---- has_any_column_privilege ----
        builtin(3024, "has_any_column_privilege_name_name", 3, false, fc_has_any_column_privilege_name_name),
        builtin(3025, "has_any_column_privilege_name_id", 3, false, fc_has_any_column_privilege_name_id),
        builtin(3026, "has_any_column_privilege_id_name", 3, false, fc_has_any_column_privilege_id_name),
        builtin(3027, "has_any_column_privilege_id_id", 3, false, fc_has_any_column_privilege_id_id),
        builtin(3028, "has_any_column_privilege_name", 2, false, fc_has_any_column_privilege_name),
        builtin(3029, "has_any_column_privilege_id", 2, false, fc_has_any_column_privilege_id),
        // ---- has_type_privilege ----
        builtin(3138, "has_type_privilege_name_name", 3, false, fc_has_type_privilege_name_name),
        builtin(3139, "has_type_privilege_name_id", 3, false, fc_has_type_privilege_name_id),
        builtin(3140, "has_type_privilege_id_name", 3, false, fc_has_type_privilege_id_name),
        builtin(3141, "has_type_privilege_id_id", 3, false, fc_has_type_privilege_id_id),
        builtin(3142, "has_type_privilege_name", 2, false, fc_has_type_privilege_name),
        builtin(3143, "has_type_privilege_id", 2, false, fc_has_type_privilege_id),
        // ---- has_parameter_privilege ----
        builtin(6205, "has_parameter_privilege_name_name", 3, false, fc_has_parameter_privilege_name_name),
        builtin(6206, "has_parameter_privilege_id_name", 3, false, fc_has_parameter_privilege_id_name),
        builtin(6207, "has_parameter_privilege_name", 2, false, fc_has_parameter_privilege_name),
        // ---- has_largeobject_privilege ----
        builtin(6348, "has_largeobject_privilege_name_id", 3, false, fc_has_largeobject_privilege_name_id),
        builtin(6349, "has_largeobject_privilege_id", 2, false, fc_has_largeobject_privilege_id),
        builtin(6350, "has_largeobject_privilege_id_id", 3, false, fc_has_largeobject_privilege_id_id),
    ]);
}
