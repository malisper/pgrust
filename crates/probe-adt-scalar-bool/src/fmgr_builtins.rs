//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for every SQL-callable
//! function in `bool.c` whose argument/result types are expressible at the
//! current fmgr boundary (the scalar `boolean` I/O, comparison operators, hash
//! functions, the `bool => text` cast, and the `bool_and`/`bool_or` aggregate
//! transition functions).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_probe_adt_scalar_bool_builtins`] registers
//! every row into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID
//! dispatch resolves them. OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat` (all are `proisstrict => 't'`, none retset).
//!
//! The moving-aggregate transition / inverse / final functions (`bool_accum`,
//! `bool_accum_inv`, `bool_alltrue`, `bool_anytrue`) take/return the `internal`
//! `BoolAggState` pointer; these ride the canonical
//! `RefPayload::Internal(Box<dyn Any>)` arm (mirroring the `interval_avg_accum`
//! / `numeric_avg_accum` families), so `bool_and`/`bool_or` work both as plain
//! aggregates and in moving-window (`OVER (... ROWS ...)`) frames.

use mcx::MemoryContext;
use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};
use types_stringinfo::StringInfo;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_BOOL(i)` → `DatumGetBool`: the low bit of arg `i`'s word.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("bool fn: missing arg").value.as_bool()
}

/// `PG_GETARG_INT64(i)` → `DatumGetInt64`: the full word as a signed 64-bit int
/// (the `hashboolextended` seed).
#[inline]
fn arg_i64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("bool fn: missing arg").value.as_i64()
}

/// `PG_GETARG_CSTRING(i)`: the input text on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("bool fn: cstring arg missing from by-ref lane")
}

/// A `bytea` / serialized arg's `VARDATA_ANY` payload (header already stripped
/// by the boundary): the wire bytes a `recv` function reads off the
/// `StringInfo`.
#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("bool fn: by-ref arg missing from by-ref lane");
    // `VARDATA_ANY`: skip the 4-byte header on the header-ful image.
    if image.len() >= 4 {
        &image[4..]
    } else {
        &[]
    }
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

/// Set a `cstring` (`boolout`) result on the by-ref lane and return the dummy
/// word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: alloc::string::String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// Set a varlena (`boolsend`/`text`) result on the by-ref lane. The bytes are
/// the header-less payload (the boundary owns the `VARHDRSZ` framing).
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: alloc::vec::Vec<u8>) -> Datum {
    // `palloc(VARHDRSZ + len)` + `SET_VARSIZE`: build the header-ful image.
    let total = bytes.len() + 4;
    let mut img = alloc::vec::Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(&bytes);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> MemoryContext {
    MemoryContext::new("bool fmgr scratch")
}

// ---------------------------------------------------------------------------
// fc_ adapters (Result-native: `ereport(ERROR)` travels as `Err(PgError)`
// straight back to the fmgr dispatch `invoke_builtin`, no panic/catch_unwind).
// ---------------------------------------------------------------------------

fn fc_boolin(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: boolin(in_str, fcinfo->context). Forward the soft ErrorSaveContext
    // installed on the frame by InputFunctionCallSafe so a bad spelling
    // `ereturn`s into the sink (returning the `false` placeholder) instead of
    // throwing past `invoke?`. Copy the cstring first because `arg_cstring`
    // borrows `fcinfo` immutably while `escontext_mut` needs it mutably.
    let s = arg_cstring(fcinfo, 0).to_owned();
    Ok(ret_bool(crate::boolin(&s, fcinfo.escontext_mut())?))
}

fn fc_boolout(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let b = arg_bool(fcinfo, 0);
    // C: boolout palloc's a 2-byte cstring ("t"/"f"). The owned core returns the
    // static spelling; ret_cstring copies it onto the by-ref lane.
    Ok(ret_cstring(fcinfo, crate::boolout(b).into()))
}

fn fc_boolrecv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: boolrecv reads one byte off the StringInfo and returns `ext != 0`. The
    // wire payload arrives on the by-ref lane (header already stripped); copy it
    // into a scratch StringInfo so pq_getmsgbyte can consume it, mirroring
    // charrecv.
    let m = scratch_mcx();
    let src = arg_varlena(fcinfo, 0);
    let mut data = mcx::PgVec::new_in(m.mcx());
    if data.try_reserve(src.len()).is_err() {
        return Err(types_error::PgError::error("out of memory"));
    }
    data.extend_from_slice(src);
    let mut buf = StringInfo::from_vec(data);
    Ok(ret_bool(crate::boolrecv(&mut buf)?))
}

fn fc_boolsend(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let arg1 = arg_bool(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = crate::boolsend(m.mcx(), arg1)?.as_bytes().to_vec();
    Ok(ret_varlena(fcinfo, bytes))
}

fn fc_booltext(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: booltext returns the SQL-spec spelling "true"/"false" wrapped in a
    // `text` varlena (`cstring_to_text`). The boundary owns the VARHDRSZ framing,
    // so the result payload is exactly those bytes (byte-identical to
    // cstring_to_text's payload, minus the header) — same pattern as char_text.
    let arg1 = arg_bool(fcinfo, 0);
    let s = if arg1 { "true" } else { "false" };
    Ok(ret_varlena(fcinfo, s.as_bytes().to_vec()))
}

fn fc_booleq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::booleq(arg_bool(fcinfo, 0), arg_bool(fcinfo, 1))))
}
fn fc_boolne(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::boolne(arg_bool(fcinfo, 0), arg_bool(fcinfo, 1))))
}
fn fc_boollt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::boollt(arg_bool(fcinfo, 0), arg_bool(fcinfo, 1))))
}
fn fc_boolgt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::boolgt(arg_bool(fcinfo, 0), arg_bool(fcinfo, 1))))
}
fn fc_boolle(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::boolle(arg_bool(fcinfo, 0), arg_bool(fcinfo, 1))))
}
fn fc_boolge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::boolge(arg_bool(fcinfo, 0), arg_bool(fcinfo, 1))))
}

fn fc_booland_statefunc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::booland_statefunc(
        arg_bool(fcinfo, 0),
        arg_bool(fcinfo, 1),
    )))
}
fn fc_boolor_statefunc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::boolor_statefunc(
        arg_bool(fcinfo, 0),
        arg_bool(fcinfo, 1),
    )))
}

fn fc_hashbool(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: hashbool returns int4; the core already returns the result `Datum`
    // (UInt32GetDatum(hash_bytes_uint32(...))).
    Ok(crate::hashbool(arg_bool(fcinfo, 0)))
}
fn fc_hashboolextended(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: hashboolextended returns int8; the core already returns the result
    // `Datum` (UInt64GetDatum(hash_bytes_uint32_extended(..., seed))).
    Ok(crate::hashboolextended(arg_bool(fcinfo, 0), arg_i64(fcinfo, 1)))
}

// ---------------------------------------------------------------------------
// bool_and / bool_or aggregate: the `internal` BoolAggState transition.
//
// The transition value crosses the fmgr boundary on the canonical
// `RefPayload::Internal(Box<dyn Any>)` arm (mirroring `interval_avg_accum` /
// `numeric_avg_accum`). `BoolAggState` is POD/Copy, so the box just carries the
// struct — no per-aggregate MemoryContext is needed once the in-aggregate-context
// check passes.
// ---------------------------------------------------------------------------

use crate::BoolAggState;

/// `PG_ARGISNULL(i)`.
#[inline]
fn arg_isnull(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).map(|d| d.isnull).unwrap_or(true)
}

/// `PG_RETURN_NULL()`.
#[inline]
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

/// Take the `internal` `BoolAggState` out of `args[i]` (C `PG_GETARG_POINTER`).
/// `None` is `PG_ARGISNULL(i)` (the first-call / empty-group case).
fn take_bool_state(fcinfo: &mut FunctionCallInfoBaseData, i: usize) -> Option<Box<BoolAggState>> {
    if arg_isnull(fcinfo, i) {
        return None;
    }
    match fcinfo.take_ref_arg(i) {
        Some(types_fmgr::boundary::RefPayload::Internal(b)) => Some(
            b.downcast::<BoolAggState>().unwrap_or_else(|_| {
                panic!("bool agg fn: args[{i}] internal state is not a BoolAggState")
            }),
        ),
        Some(other) => panic!("bool agg fn: args[{i}] is not an internal state ({other:?})"),
        None => None,
    }
}

/// `PG_RETURN_POINTER(state)`.
#[inline]
fn ret_bool_state(fcinfo: &mut FunctionCallInfoBaseData, state: Box<BoolAggState>) -> Datum {
    fcinfo.set_ref_result(types_fmgr::boundary::RefPayload::Internal(state));
    Datum::from_usize(0)
}

/// Restore the `internal` `BoolAggState` into `args[0]` after a *final* function
/// read it.  C's `PG_GETARG_POINTER(0)` does NOT consume the state: a finalfn
/// only reads it, and the same live state must survive for the next sharing
/// aggregate's finalfn and, in a moving window frame, for the next row's
/// forward/inverse transition.
#[inline]
fn keep_bool_state(fcinfo: &mut FunctionCallInfoBaseData, state: Box<BoolAggState>) {
    fcinfo.set_ref_arg(0, types_fmgr::boundary::RefPayload::Internal(state));
}

/// `bool_accum(internal, bool) -> internal` (oid 3496) — forward transition.
fn fc_bool_accum(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C `makeBoolAggState` calls `AggCheckCallContext`, which succeeds in BOTH
    // nodeAgg and nodeWindowAgg (moving-aggregate) contexts. `bool_accum` is only
    // ever wired as an aggregate transition function, so the call is always in
    // aggregate context; pass a scratch context as the resolved agg context (the
    // POD `BoolAggState` needs no real PG-context allocation — same as
    // `interval_avg_accum`'s `unwrap_or_default`). The `None`-context "aggregate
    // function called in non-aggregate context" branch is unreachable here.
    let prev = take_bool_state(fcinfo, 0).map(|b| *b);
    let value = if arg_isnull(fcinfo, 1) {
        None
    } else {
        Some(arg_bool(fcinfo, 1))
    };
    let m = scratch_mcx();
    let state = crate::bool_accum(Some(m.mcx()), prev, value)?;
    Ok(ret_bool_state(fcinfo, alloc::boxed::Box::new(state)))
}

/// `bool_accum_inv(internal, bool) -> internal` (oid 3497) — inverse transition.
fn fc_bool_accum_inv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let prev = take_bool_state(fcinfo, 0).map(|b| *b);
    let value = if arg_isnull(fcinfo, 1) {
        None
    } else {
        Some(arg_bool(fcinfo, 1))
    };
    let state = crate::bool_accum_inv(prev, value)?;
    Ok(ret_bool_state(fcinfo, alloc::boxed::Box::new(state)))
}

/// `bool_alltrue(internal) -> bool` (oid 3498) — `bool_and` / `every` final.
fn fc_bool_alltrue(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let state = take_bool_state(fcinfo, 0);
    let out = crate::bool_alltrue(state.as_deref().copied());
    // C `PG_GETARG_POINTER(0)` does not consume the state; restore it so the
    // moving-window inverse transition / next row keeps it.
    if let Some(state) = state {
        keep_bool_state(fcinfo, state);
    }
    match out {
        Some(b) => Ok(ret_bool(b)),
        None => Ok(ret_null(fcinfo)),
    }
}

/// `bool_anytrue(internal) -> bool` (oid 3499) — `bool_or` final.
fn fc_bool_anytrue(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let state = take_bool_state(fcinfo, 0);
    let out = crate::bool_anytrue(state.as_deref().copied());
    if let Some(state) = state {
        keep_bool_state(fcinfo, state);
    }
    match out {
        Some(b) => Ok(ret_bool(b)),
        None => Ok(ret_null(fcinfo)),
    }
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    builtin_strict(foid, name, nargs, true, native)
}

fn builtin_strict(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.into(),
            nargs,
            strict,
            retset: false,
            func: None,
        },
        native,
    )
}

/// Register every `bool.c` builtin expressible at the fmgr boundary (C: their
/// `fmgr_builtins[]` rows) as **Result-native** (the panic→Result migration;
/// see `docs/proposals/panic-to-result-migration.md`). Called from this crate's
/// `init_seams()`. OIDs/nargs from `pg_proc.dat`; all are `proisstrict => 't'`
/// and not retset.
pub fn register_probe_adt_scalar_bool_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        // ---- I/O + cast ----
        builtin(1242, "boolin", 1, fc_boolin),
        builtin(1243, "boolout", 1, fc_boolout),
        builtin(2436, "boolrecv", 1, fc_boolrecv),
        builtin(2437, "boolsend", 1, fc_boolsend),
        builtin(2971, "booltext", 1, fc_booltext),
        // ---- comparison operators ----
        builtin(60, "booleq", 2, fc_booleq),
        builtin(84, "boolne", 2, fc_boolne),
        builtin(56, "boollt", 2, fc_boollt),
        builtin(57, "boolgt", 2, fc_boolgt),
        builtin(1691, "boolle", 2, fc_boolle),
        builtin(1692, "boolge", 2, fc_boolge),
        // ---- aggregate transition functions ----
        builtin(2515, "booland_statefunc", 2, fc_booland_statefunc),
        builtin(2516, "boolor_statefunc", 2, fc_boolor_statefunc),
        // ---- bool_and / bool_or moving-aggregate (internal BoolAggState) ----
        // strict flags from builtin_canonical: accum/inv are non-strict (handle
        // NULL state on first call); the finals are strict (NULL state → NULL).
        builtin_strict(3496, "bool_accum", 2, false, fc_bool_accum),
        builtin_strict(3497, "bool_accum_inv", 2, false, fc_bool_accum_inv),
        builtin(3498, "bool_alltrue", 1, fc_bool_alltrue),
        builtin(3499, "bool_anytrue", 1, fc_bool_anytrue),
        // ---- hash functions ----
        builtin(6417, "hashbool", 1, fc_hashbool),
        builtin(6418, "hashboolextended", 2, fc_hashboolextended),
    ]);
}
