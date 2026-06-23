//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! scalar (non-SRF) `jsonpath_exec.c` predicates whose argument/result types are
//! expressible at the current fmgr boundary:
//!
//! * `jsonb_path_exists` / `_tz` (oid 4005 / 1177) and the `@?` operator
//!   `jsonb_path_exists_opr` (oid 4010) — `bool` result (`NULL` on a swallowed
//!   error in non-silent... actually silent — see the cores).
//! * `jsonb_path_match` / `_tz` (oid 4009 / 2030) and the `@@` operator
//!   `jsonb_path_match_opr` (oid 4011) — `bool` result, `NULL` when the path
//!   yields a JSON `null` or no single boolean.
//! * `jsonb_path_query_array` / `_tz` (oid 4007 / 1180) — `jsonb` result.
//! * `jsonb_path_query_first` / `_tz` (oid 4008 / 2023) — `jsonb` result, `NULL`
//!   when the path matches nothing.
//!
//! The set-returning `jsonb_path_query` / `_tz` (oid 4006 / 1179) are NOT
//! registered here — their `retset` dispatch belongs to the SRF executor-frame
//! lane (they live as value cores [`crate::jsonb_path_query`] /
//! [`crate::jsonb_path_query_tz`] until then).
//!
//! # Argument conventions
//!
//! The `jsonb` arguments (`jb`, the `vars` document) and the `jsonpath` argument
//! are pass-by-reference varlenas that cross the fmgr boundary on the
//! by-reference side channel; the `executeJsonPath` cores want the full on-disk
//! varlena of each (they slice past the header themselves — `jsonb_root` /
//! `JsonbInitBinary` for jsonb, `jsonpath_header`/`jsonpath_data` for jsonpath).
//! The lane framing differs by type: the `jsonb` lane carries a single full
//! varlena (forwarded verbatim, see [`arg_jsonb_image`]), while the `jsonpath`
//! lane carries the full `jsonpath` varlena behind ONE extra leading `VARHDRSZ`
//! word (stripped by [`arg_jsonpath_image`], mirroring the `jsonpath` I/O
//! builtins' `arg_jsonpath_payload`).
//!
//! The `silent` `bool` argument arrives as a plain by-value word. All four
//! functions in the named (non-`_opr`) family take `jsonb jsonpath jsonb bool`;
//! the parser fills the `vars` default (`'{}'`) and `silent` default (`false`),
//! so all four args are always present (and non-NULL under the default `strict`
//! flag).

use datum::Datum;
use types_error::PgResult;
use fmgr::boundary::RefPayload;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use crate::{
    jsonb_path_exists, jsonb_path_exists_opr, jsonb_path_exists_tz, jsonb_path_match,
    jsonb_path_match_opr, jsonb_path_match_tz, jsonb_path_query_array, jsonb_path_query_array_tz,
    jsonb_path_query_first, jsonb_path_query_first_tz, PathExistsResult, PathMatchResult,
};

/// `VARHDRSZ` — the uncompressed 4-byte varlena length-word size.
const VARHDRSZ: usize = 4;

/// `PG_GETARG_JSONB_P(i)`: the FULL on-disk `jsonb` varlena image (4-byte length
/// header + root container) on the by-ref lane. The `executeJsonPath` cores
/// (`jsonb_root` / `JsonbInitBinary`) slice past the `VARHDRSZ` header
/// themselves, so the full image is forwarded verbatim. (The jsonb lane carries
/// a single full varlena — the `jsonb_op.c` convention in
/// `backend-utils-adt-jsonb`'s `arg_jsonb_image`.)
#[inline]
fn arg_jsonb_image<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("jsonpath fn: by-ref `jsonb` arg missing from by-ref lane")
}

/// `PG_GETARG_JSONPATH_P(i)`: the FULL on-disk `jsonpath` varlena (4-byte length
/// header + version word + flattened nodes) that the `executeJsonPath` cores
/// (`jsonpath_header` at `[4..8]` / `jsonpath_data` at `[8..]`) consume.
///
/// Like the `jsonpath` I/O builtins (`backend-utils-adt-jsonpath`'s
/// `arg_jsonpath_payload`), the `jsonpath` by-ref lane carries the full
/// `jsonpath` varlena image behind ONE extra leading `VARHDRSZ` word (the
/// canonical-`ByRef`->ABI bridge frames it that way for a pass-by-reference
/// arg); strip that one leading header to recover the real full `jsonpath`
/// varlena the cores slice into.
#[inline]
fn arg_jsonpath_image<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("jsonpath fn: by-ref `jsonpath` arg missing from by-ref lane");
    vardata_any(image)
}

/// `VARDATA_ANY(ptr)` for an inline (non-compressed, non-external) varlena image:
/// skip ONE header byte for a short (1-byte, low-bit-set) header, else `VARHDRSZ`.
/// A small stored value arrives short-headed once `SHORT_VARLENA_PACKING` is on; a
/// fixed 4-byte strip would drop three payload bytes. No-op while off.
#[inline]
fn vardata_any(image: &[u8]) -> &[u8] {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
    }
}

/// `PG_GETARG_BOOL(i)`: the `silent` flag arrives as a plain by-value word.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo
        .arg(i)
        .expect("jsonpath fn: missing by-value bool arg")
        .value
        .as_usize()
        != 0
}

/// A scratch context for the cores that allocate their result through `Mcx`. The
/// bytes are copied onto the by-ref lane before it drops.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("jsonpath_exec fmgr scratch")
}

/// Set a `jsonb` (by-reference) result on the by-ref lane and return the dummy
/// by-value word. `image` is the full jsonb varlena image (with header).
#[inline]
fn ret_jsonb(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// `PG_RETURN_BOOL` / `PG_RETURN_NULL` from a tri-state path result.
#[inline]
fn ret_bool_tri(fcinfo: &mut FunctionCallInfoBaseData, exists: bool, is_null: bool) -> Datum {
    if is_null {
        fcinfo.set_result_null(true);
        return Datum::from_usize(0);
    }
    Datum::from_bool(exists)
}

// ---------------------------------------------------------------------------
// jsonb_path_exists / _tz / _opr  (@?)
// ---------------------------------------------------------------------------

/// `jsonb_path_exists(jsonb, jsonpath, jsonb, bool) -> bool` (oid 4005).
fn fc_jsonb_path_exists(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0);
    let jp = arg_jsonpath_image(fcinfo, 1);
    let vars = arg_jsonb_image(fcinfo, 2);
    let silent = arg_bool(fcinfo, 3);
    let m = scratch_mcx();
    let res = jsonb_path_exists(m.mcx(), jb, jp, Some(vars), silent)?;
    let (exists, is_null) = path_exists_to_bool(res);
    Ok(ret_bool_tri(fcinfo, exists, is_null))
}

/// `jsonb_path_exists_tz(jsonb, jsonpath, jsonb, bool) -> bool` (oid 1177).
fn fc_jsonb_path_exists_tz(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0);
    let jp = arg_jsonpath_image(fcinfo, 1);
    let vars = arg_jsonb_image(fcinfo, 2);
    let silent = arg_bool(fcinfo, 3);
    let m = scratch_mcx();
    let res = jsonb_path_exists_tz(m.mcx(), jb, jp, Some(vars), silent)?;
    let (exists, is_null) = path_exists_to_bool(res);
    Ok(ret_bool_tri(fcinfo, exists, is_null))
}

/// `jsonb_path_exists_opr(jsonb, jsonpath) -> bool` (oid 4010) — the `@?`
/// operator (`silent = true`, no vars).
fn fc_jsonb_path_exists_opr(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0);
    let jp = arg_jsonpath_image(fcinfo, 1);
    let m = scratch_mcx();
    let res = jsonb_path_exists_opr(m.mcx(), jb, jp)?;
    let (exists, is_null) = path_exists_to_bool(res);
    Ok(ret_bool_tri(fcinfo, exists, is_null))
}

#[inline]
fn path_exists_to_bool(res: PathExistsResult) -> (bool, bool) {
    match res {
        PathExistsResult::True => (true, false),
        PathExistsResult::False => (false, false),
        PathExistsResult::Null => (false, true),
    }
}

// ---------------------------------------------------------------------------
// jsonb_path_match / _tz / _opr  (@@)
// ---------------------------------------------------------------------------

/// `jsonb_path_match(jsonb, jsonpath, jsonb, bool) -> bool` (oid 4009).
fn fc_jsonb_path_match(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0);
    let jp = arg_jsonpath_image(fcinfo, 1);
    let vars = arg_jsonb_image(fcinfo, 2);
    let silent = arg_bool(fcinfo, 3);
    let m = scratch_mcx();
    let res = jsonb_path_match(m.mcx(), jb, jp, Some(vars), silent)?;
    let (matched, is_null) = path_match_to_bool(res);
    Ok(ret_bool_tri(fcinfo, matched, is_null))
}

/// `jsonb_path_match_tz(jsonb, jsonpath, jsonb, bool) -> bool` (oid 2030).
fn fc_jsonb_path_match_tz(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0);
    let jp = arg_jsonpath_image(fcinfo, 1);
    let vars = arg_jsonb_image(fcinfo, 2);
    let silent = arg_bool(fcinfo, 3);
    let m = scratch_mcx();
    let res = jsonb_path_match_tz(m.mcx(), jb, jp, Some(vars), silent)?;
    let (matched, is_null) = path_match_to_bool(res);
    Ok(ret_bool_tri(fcinfo, matched, is_null))
}

/// `jsonb_path_match_opr(jsonb, jsonpath) -> bool` (oid 4011) — the `@@`
/// operator (`silent = true`, no vars).
fn fc_jsonb_path_match_opr(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0);
    let jp = arg_jsonpath_image(fcinfo, 1);
    let m = scratch_mcx();
    let res = jsonb_path_match_opr(m.mcx(), jb, jp)?;
    let (matched, is_null) = path_match_to_bool(res);
    Ok(ret_bool_tri(fcinfo, matched, is_null))
}

#[inline]
fn path_match_to_bool(res: PathMatchResult) -> (bool, bool) {
    match res {
        PathMatchResult::True => (true, false),
        PathMatchResult::False => (false, false),
        PathMatchResult::Null => (false, true),
    }
}

// ---------------------------------------------------------------------------
// jsonb_path_query_array / _tz  -> jsonb
// ---------------------------------------------------------------------------

/// `jsonb_path_query_array(jsonb, jsonpath, jsonb, bool) -> jsonb` (oid 4007).
fn fc_jsonb_path_query_array(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0);
    let jp = arg_jsonpath_image(fcinfo, 1);
    let vars = arg_jsonb_image(fcinfo, 2);
    let silent = arg_bool(fcinfo, 3);
    let m = scratch_mcx();
    let image = jsonb_path_query_array(m.mcx(), jb, jp, Some(vars), silent)?;
    Ok(ret_jsonb(fcinfo, image))
}

/// `jsonb_path_query_array_tz(jsonb, jsonpath, jsonb, bool) -> jsonb` (oid 1180).
fn fc_jsonb_path_query_array_tz(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0);
    let jp = arg_jsonpath_image(fcinfo, 1);
    let vars = arg_jsonb_image(fcinfo, 2);
    let silent = arg_bool(fcinfo, 3);
    let m = scratch_mcx();
    let image = jsonb_path_query_array_tz(m.mcx(), jb, jp, Some(vars), silent)?;
    Ok(ret_jsonb(fcinfo, image))
}

// ---------------------------------------------------------------------------
// jsonb_path_query_first / _tz  -> jsonb (NULL on no match)
// ---------------------------------------------------------------------------

/// `jsonb_path_query_first(jsonb, jsonpath, jsonb, bool) -> jsonb` (oid 4008).
fn fc_jsonb_path_query_first(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0);
    let jp = arg_jsonpath_image(fcinfo, 1);
    let vars = arg_jsonb_image(fcinfo, 2);
    let silent = arg_bool(fcinfo, 3);
    let m = scratch_mcx();
    match jsonb_path_query_first(m.mcx(), jb, jp, Some(vars), silent)? {
        Some(image) => Ok(ret_jsonb(fcinfo, image)),
        None => {
            fcinfo.set_result_null(true);
            Ok(Datum::from_usize(0))
        }
    }
}

/// `jsonb_path_query_first_tz(jsonb, jsonpath, jsonb, bool) -> jsonb` (oid 2023).
fn fc_jsonb_path_query_first_tz(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0);
    let jp = arg_jsonpath_image(fcinfo, 1);
    let vars = arg_jsonb_image(fcinfo, 2);
    let silent = arg_bool(fcinfo, 3);
    let m = scratch_mcx();
    match jsonb_path_query_first_tz(m.mcx(), jb, jp, Some(vars), silent)? {
        Some(image) => Ok(ret_jsonb(fcinfo, image)),
        None => {
            fcinfo.set_result_null(true);
            Ok(Datum::from_usize(0))
        }
    }
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

/// `Gen_fmgrtab.pl` builds `fmgr_builtins[]` from `pg_proc.dat`; here each entry
/// is transcribed by hand. OIDs/nargs/strict/retset come straight from
/// `pg_proc.dat`.
fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict,
            retset,
            func: None,
        },
        native,
    )
}

/// Register the expressible scalar (non-SRF) `jsonpath_exec.c` predicates and
/// query helpers. Called from this crate's `init_seams()`. OIDs/nargs/strict/
/// retset transcribed from `pg_proc.dat`.
pub fn register_jsonpath_exec_builtins() {
    fmgr_core::register_builtins_native([
        builtin(4005, "jsonb_path_exists", 4, true, false, fc_jsonb_path_exists),
        builtin(
            1177,
            "jsonb_path_exists_tz",
            4,
            true,
            false,
            fc_jsonb_path_exists_tz,
        ),
        builtin(
            4010,
            "jsonb_path_exists_opr",
            2,
            true,
            false,
            fc_jsonb_path_exists_opr,
        ),
        builtin(4009, "jsonb_path_match", 4, true, false, fc_jsonb_path_match),
        builtin(
            2030,
            "jsonb_path_match_tz",
            4,
            true,
            false,
            fc_jsonb_path_match_tz,
        ),
        builtin(
            4011,
            "jsonb_path_match_opr",
            2,
            true,
            false,
            fc_jsonb_path_match_opr,
        ),
        builtin(
            4007,
            "jsonb_path_query_array",
            4,
            true,
            false,
            fc_jsonb_path_query_array,
        ),
        builtin(
            1180,
            "jsonb_path_query_array_tz",
            4,
            true,
            false,
            fc_jsonb_path_query_array_tz,
        ),
        builtin(
            4008,
            "jsonb_path_query_first",
            4,
            true,
            false,
            fc_jsonb_path_query_first,
        ),
        builtin(
            2023,
            "jsonb_path_query_first_tz",
            4,
            true,
            false,
            fc_jsonb_path_query_first_tz,
        ),
    ]);
}
