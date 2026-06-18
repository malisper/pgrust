//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `LIKE`/`ILIKE`/`NOT LIKE` pattern-matching operators and the `like_escape`
//! pattern normalizers from `like.c` (the matcher template `like_match.c`).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result. A
//! `text`/`bytea` arg arrives as its detoasted `VARDATA_ANY` payload on the
//! by-ref lane (the boundary strips the varlena header); a `name` arg arrives
//! as its fixed `NAMEDATALEN` buffer bytes (the cores NUL-trim / `name_text`
//! it). The collation is read from `fcinfo.fncollation` (C:
//! `PG_GET_COLLATION()`); the `bytea` family takes no collation. The two
//! `like_escape` functions return a `text`/`bytea` varlena on the by-ref lane.
//!
//! [`register_like_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`), so by-OID dispatch (and the `fmgr_isbuiltin`
//! fast path) resolves them. OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat`: every row is `nargs => 2`, `proisstrict` default
//! `'t'`, `proretset` default `'f'`.
//!
//! `like_support.c` (the planner support functions / selectivity entry points)
//! is NOT registered here (see the crate docs): it operates on planner nodes
//! that are not modeled at this boundary and is dispatched only through the bare
//! `PGFunction` registry, which is deferred.

use types_core::Oid;
use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// A `text`/`bytea`/`name` arg's by-ref payload bytes (the boundary strips the
/// varlena header for `text`/`bytea`; for `name` this is the fixed
/// `NAMEDATALEN` buffer, which the cores NUL-trim or `name_text`).
#[inline]
fn arg_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("like fn: by-ref arg missing from by-ref lane")
}

/// `PG_GET_COLLATION()`: the collation the operator was invoked under.
#[inline]
fn collation(fcinfo: &FunctionCallInfoBaseData) -> Oid {
    fcinfo.fncollation
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

/// Set a `text`/`bytea` varlena result on the by-ref lane and return the dummy
/// word.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("like fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

/// Unwrap a `PgResult`, re-raising its error through `raise`.
#[inline]
fn ok<T>(r: types_error::PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters — text/name LIKE family.
// ---------------------------------------------------------------------------

fn fc_textlike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    ret_bool(ok(crate::textlike(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c, m.mcx())))
}
fn fc_textnlike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    ret_bool(ok(crate::textnlike(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c, m.mcx())))
}
fn fc_namelike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    ret_bool(ok(crate::namelike(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c, m.mcx())))
}
fn fc_namenlike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    ret_bool(ok(crate::namenlike(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c, m.mcx())))
}

// ---------------------------------------------------------------------------
// fc_ adapters — text/name ILIKE family.
// ---------------------------------------------------------------------------

fn fc_texticlike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    ret_bool(ok(crate::texticlike(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c, m.mcx())))
}
fn fc_texticnlike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    ret_bool(ok(crate::texticnlike(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c, m.mcx())))
}
fn fc_nameiclike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    ret_bool(ok(crate::nameiclike(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c, m.mcx())))
}
fn fc_nameicnlike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    ret_bool(ok(crate::nameicnlike(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c, m.mcx())))
}

// ---------------------------------------------------------------------------
// fc_ adapters — bytea LIKE family (no collation).
// ---------------------------------------------------------------------------

fn fc_bytealike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ret_bool(ok(crate::bytealike(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), m.mcx())))
}
fn fc_byteanlike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ret_bool(ok(crate::byteanlike(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), m.mcx())))
}

// ---------------------------------------------------------------------------
// fc_ adapters — like_escape (text) / like_escape_bytea (bytea).
// ---------------------------------------------------------------------------

fn fc_like_escape(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let payload = ok(crate::like_escape(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), m.mcx()));
    let bytes = payload.as_slice().to_vec();
    ret_varlena(fcinfo, bytes)
}
fn fc_like_escape_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let payload = ok(crate::like_escape_bytea(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), m.mcx()));
    let bytes = payload.as_slice().to_vec();
    ret_varlena(fcinfo, bytes)
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
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict,
        retset,
        func: Some(func),
    }
}

/// Register every SQL-callable `like.c` builtin (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs / nargs / strict /
/// retset transcribed exactly from `pg_proc.dat` (all `nargs => 2`, all strict
/// by default, none retset).
pub fn register_like_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- text/name LIKE ----
        builtin(850, "textlike", 2, true, false, fc_textlike),
        builtin(851, "textnlike", 2, true, false, fc_textnlike),
        builtin(858, "namelike", 2, true, false, fc_namelike),
        builtin(859, "namenlike", 2, true, false, fc_namenlike),
        builtin(1569, "textlike", 2, true, false, fc_textlike),
        builtin(1570, "textnlike", 2, true, false, fc_textnlike),
        builtin(1571, "namelike", 2, true, false, fc_namelike),
        builtin(1572, "namenlike", 2, true, false, fc_namenlike),
        // ---- text/name ILIKE ----
        builtin(1633, "texticlike", 2, true, false, fc_texticlike),
        builtin(1634, "texticnlike", 2, true, false, fc_texticnlike),
        builtin(1635, "nameiclike", 2, true, false, fc_nameiclike),
        builtin(1636, "nameicnlike", 2, true, false, fc_nameicnlike),
        // ---- bpchar LIKE/ILIKE (prosrc = textlike/textnlike/texticlike/
        //      texticnlike; bpchar is binary-compatible with text as a varlena,
        //      so the same value cores apply to the detoasted by-ref payload) ----
        builtin(1631, "textlike", 2, true, false, fc_textlike),
        builtin(1632, "textnlike", 2, true, false, fc_textnlike),
        builtin(1660, "texticlike", 2, true, false, fc_texticlike),
        builtin(1661, "texticnlike", 2, true, false, fc_texticnlike),
        // ---- like_escape (text) ----
        builtin(1637, "like_escape", 2, true, false, fc_like_escape),
        // ---- bytea LIKE ----
        builtin(2005, "bytealike", 2, true, false, fc_bytealike),
        builtin(2006, "byteanlike", 2, true, false, fc_byteanlike),
        builtin(2007, "bytealike", 2, true, false, fc_bytealike),
        builtin(2008, "byteanlike", 2, true, false, fc_byteanlike),
        // ---- like_escape (bytea) ----
        builtin(2009, "like_escape_bytea", 2, true, false, fc_like_escape_bytea),
    ]);
}
