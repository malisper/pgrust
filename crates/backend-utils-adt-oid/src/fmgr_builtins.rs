//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for every SQL-callable
//! function in `oid.c` whose argument/result types are expressible at the
//! current fmgr boundary (the scalar `oid` I/O and comparison operators).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_oid_builtins`] registers every row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch (and the
//! `fmgr_isbuiltin` fast path that early catalog scankeys rely on) resolves
//! them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.
//!
//! The `oidvector` comparison family (`oidvectoreq` … `oidvectorgt`) IS
//! registered: each decodes its two `oidvector` array images and delegates to
//! the `btoidvectorcmp` element-wise comparison core. Only the binary
//! `oidvectorrecv`/`oidvectorsend` remain unregistered (they need the
//! `array_recv`/`array_send` fcinfo-sharing path).

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use types_core::Oid;
use types_stringinfo::StringInfo;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)` → `DatumGetObjectId`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.arg(i).expect("oid fn: missing arg").value.as_oid()
}

/// `PG_GETARG_CSTRING(i)`: the input text on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("oid fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_POINTER(i)` for a `StringInfo` (the `oidrecv` wire buffer): the
/// raw message bytes on the by-ref lane.
#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("oid fn: by-ref arg missing from by-ref lane")
}

#[inline]
fn ret_oid(v: Oid) -> Datum {
    Datum::from_oid(v)
}
#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

/// Set a `cstring` (`_out`) result on the by-ref lane and return the dummy word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}
/// Set a `bytea` (`_send`) result on the by-ref lane.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("oid fmgr scratch")
}

// ---------------------------------------------------------------------------
// fc_ adapters (Result-native: `ereport(ERROR)` travels as `Err(PgError)`
// straight back to the fmgr dispatch `invoke_builtin`, no panic/catch_unwind).
// ---------------------------------------------------------------------------

fn fc_oidin(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let s = arg_cstring(fcinfo, 0).to_string();
    // C: uint32in_subr(s, NULL, "oid", fcinfo->context). Forward the soft
    // ErrorSaveContext so a recoverable parse failure `ereturn`s into the sink
    // (returning a placeholder 0 that the caller discards) instead of throwing.
    let escontext = fcinfo.escontext_mut();
    Ok(ret_oid(crate::oidin(&s, escontext)?))
}

fn fc_oidout(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let o = arg_oid(fcinfo, 0);
    Ok(ret_cstring(fcinfo, crate::oidout(o)))
}

fn fc_oidrecv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let src = arg_varlena(fcinfo, 0);
    let mut data = mcx::PgVec::new_in(m.mcx());
    if data.try_reserve(src.len()).is_err() {
        return Err(types_error::PgError::error("out of memory"));
    }
    data.extend_from_slice(src);
    let mut buf = StringInfo::from_vec(data);
    Ok(ret_oid(crate::oidrecv(&mut buf)?))
}

fn fc_oidsend(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let arg1 = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = crate::oidsend(m.mcx(), arg1)?.as_bytes().to_vec();
    Ok(ret_varlena(fcinfo, bytes))
}

fn fc_oidvectorin(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let s = arg_cstring(fcinfo, 0).to_string();
    let m = scratch_mcx();
    // C passes fcinfo->context (the soft ErrorSaveContext) to uint32in_subr.
    // Forward it so a bad OID token `ereturn`s into the soft sink instead of
    // throwing; on the soft path the body returns `Ok(None)` and the caller
    // discards this placeholder after `soft_error_occurred()`.
    let escontext = fcinfo.escontext_mut();
    let image_bytes: Vec<u8> = match crate::oidvectorin(m.mcx(), &s, escontext)? {
        Some(image) => image.as_slice().to_vec(),
        None => return Ok(Datum::null()),
    };
    Ok(ret_varlena(fcinfo, image_bytes))
}

fn fc_oidvectorout(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    use backend_utils_adt_arrayfuncs::foundation;
    let m = scratch_mcx();
    let bytes = arg_varlena(fcinfo, 0).to_vec();
    // check_valid_oidvector reads ndim / dataoffset (== ARR_HASNULL marker) /
    // elemtype off the array header; the values come from the int-aligned data
    // region.
    let ndim = foundation::arr_ndim(&bytes);
    let dataoffset = foundation::arr_dataoffset_field(&bytes);
    let elemtype = foundation::arr_elemtype(&bytes);
    let values: Vec<types_core::Oid> =
        backend_utils_adt_arrayfuncs::construct::oidvector_to_oids_bytes(m.mcx(), &bytes)?
            .iter()
            .copied()
            .collect();
    Ok(ret_cstring(
        fcinfo,
        crate::oidvectorout(ndim, dataoffset, elemtype, &values)?,
    ))
}

/// Decode an `oidvector` argument (a 1-D `ArrayType` varlena image) off the
/// by-ref lane into its header fields and `Oid` values, the form
/// `btoidvectorcmp` consumes.
fn arg_oidvector(
    fcinfo: &FunctionCallInfoBaseData,
    i: usize,
) -> types_error::PgResult<(i32, i32, Oid, Vec<Oid>)> {
    use backend_utils_adt_arrayfuncs::foundation;
    let m = scratch_mcx();
    let bytes = arg_varlena(fcinfo, i).to_vec();
    let ndim = foundation::arr_ndim(&bytes);
    let dataoffset = foundation::arr_dataoffset_field(&bytes);
    let elemtype = foundation::arr_elemtype(&bytes);
    let values: Vec<Oid> =
        backend_utils_adt_arrayfuncs::construct::oidvector_to_oids_bytes(m.mcx(), &bytes)?
            .iter()
            .copied()
            .collect();
    Ok((ndim, dataoffset, elemtype, values))
}

/// `btoidvectorcmp` over the two by-ref `oidvector` arguments.
fn oidvector_cmp(fcinfo: &FunctionCallInfoBaseData) -> types_error::PgResult<i32> {
    let (a_ndim, a_doff, a_et, a) = arg_oidvector(fcinfo, 0)?;
    let (b_ndim, b_doff, b_et, b) = arg_oidvector(fcinfo, 1)?;
    crate::btoidvectorcmp(a_ndim, a_doff, a_et, &a, b_ndim, b_doff, b_et, &b)
}

fn fc_oidvectoreq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::oidvectoreq(oidvector_cmp(fcinfo)?)))
}
fn fc_oidvectorne(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::oidvectorne(oidvector_cmp(fcinfo)?)))
}
fn fc_oidvectorlt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::oidvectorlt(oidvector_cmp(fcinfo)?)))
}
fn fc_oidvectorle(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::oidvectorle(oidvector_cmp(fcinfo)?)))
}
fn fc_oidvectorge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::oidvectorge(oidvector_cmp(fcinfo)?)))
}
fn fc_oidvectorgt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::oidvectorgt(oidvector_cmp(fcinfo)?)))
}

fn fc_oideq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::oideq(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1))))
}
fn fc_oidne(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::oidne(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1))))
}
fn fc_oidlt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::oidlt(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1))))
}
fn fc_oidle(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::oidle(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1))))
}
fn fc_oidgt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::oidgt(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1))))
}
fn fc_oidge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::oidge(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1))))
}
fn fc_oidlarger(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_oid(crate::oidlarger(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1))))
}
fn fc_oidsmaller(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_oid(crate::oidsmaller(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1))))
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

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

/// Register every scalar `oid.c` builtin (C: their `fmgr_builtins[]` rows) as
/// **Result-native** (the panic→Result migration; see
/// `docs/proposals/panic-to-result-migration.md`). Called from this crate's
/// `init_seams()`. OIDs/nargs/strict from `pg_proc.dat` (all are
/// `proisstrict => 't'`, none retset).
pub fn register_oid_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        // ---- I/O ----
        builtin(1798, "oidin", 1, true, false, fc_oidin),
        builtin(1799, "oidout", 1, true, false, fc_oidout),
        builtin(2418, "oidrecv", 1, true, false, fc_oidrecv),
        builtin(2419, "oidsend", 1, true, false, fc_oidsend),
        // ---- oidvector I/O ----
        builtin(54, "oidvectorin", 1, true, false, fc_oidvectorin),
        builtin(55, "oidvectorout", 1, true, false, fc_oidvectorout),
        // ---- oidvector comparison operators (delegate to btoidvectorcmp) ----
        builtin(679, "oidvectoreq", 2, true, false, fc_oidvectoreq),
        builtin(619, "oidvectorne", 2, true, false, fc_oidvectorne),
        builtin(677, "oidvectorlt", 2, true, false, fc_oidvectorlt),
        builtin(678, "oidvectorle", 2, true, false, fc_oidvectorle),
        builtin(680, "oidvectorge", 2, true, false, fc_oidvectorge),
        builtin(681, "oidvectorgt", 2, true, false, fc_oidvectorgt),
        // ---- comparison operators ----
        builtin(184, "oideq", 2, true, false, fc_oideq),
        builtin(185, "oidne", 2, true, false, fc_oidne),
        builtin(716, "oidlt", 2, true, false, fc_oidlt),
        builtin(717, "oidle", 2, true, false, fc_oidle),
        builtin(1638, "oidgt", 2, true, false, fc_oidgt),
        builtin(1639, "oidge", 2, true, false, fc_oidge),
        builtin(1965, "oidlarger", 2, true, false, fc_oidlarger),
        builtin(1966, "oidsmaller", 2, true, false, fc_oidsmaller),
    ]);
}
