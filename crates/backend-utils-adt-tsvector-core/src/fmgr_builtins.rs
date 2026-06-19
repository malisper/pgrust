//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `tsvector` functions of `tsvector.c` / `tsvector_op.c` whose argument /
//! result types are expressible at the current fmgr boundary: the I/O family
//! (`tsvectorin`/`out`/`send`/`recv`), the comparison family
//! (`tsvector_cmp`/`eq`/`ne`/`lt`/`le`/`gt`/`ge`), and the scalar manipulation
//! functions (`tsvector_strip`/`setweight`/`concat`/`length`).
//!
//! A `tsvector` value is its flat **header-ful** varlena image: the value cores
//! read `TSVectorData.size` at byte offset 4 (`tsv_size`), so they consume — and
//! produce — the whole `VARHDRSZ`-prefixed image. The by-ref lane already
//! carries that full image, so `tsvector` args/results cross VERBATIM (no
//! header strip, no re-frame). This differs from the `text`-payload convention
//! used by `varchar`/`regexp` (which strip/prepend the 4-byte header); a
//! `tsvector` must NOT go through that path or the size word would be misread.
//!
//! `tsvectorout` returns a `cstring`; `tsvectorsend` returns the header-ful
//! `bytea` wire image (already stamped by `pq_endtypsend`). `tsvector_setweight`
//! takes the `"char"` weight by value.
//!
//! NOT registered here (not expressible at this boundary, skipped per the
//! discipline rather than hollow-stubbed):
//!  * the array-typed manipulators (`tsvector_setweight_by_filter`,
//!    `tsvector_delete_arr`, `tsvector_filter`, `tsvector_to_array`,
//!    `array_to_tsvector`) — their `text[]`/`anyarray` arg or result crosses the
//!    deconstructed-array boundary the value cores take pre-split (`*_datum`);
//!  * the set-returning `tsvector_unnest` / `ts_stat*` and the `@@` match
//!    operators (which dispatch through the `TS_execute` engine) and
//!    `tsvector_update_trigger` (which needs the trigger-manager frame).

use std::string::{String, ToString};

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};
use types_stringinfo::StringInfo;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// A `tsvector` arg's full header-ful varlena image on the by-ref lane (read
/// verbatim — the value cores consume the `VARHDRSZ`-prefixed image, reading the
/// size word at offset 4).
#[inline]
fn arg_tsvector<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("tsvector fn: by-ref tsvector arg missing from by-ref lane")
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("tsvector fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_CHAR(i)`: the `"char"` weight, a single signed byte by value.
#[inline]
fn arg_char(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i8 {
    fcinfo.arg(i).expect("tsvector fn: missing char arg").value.as_i32() as i8
}

/// Set a header-ful `tsvector`/`bytea` varlena result on the by-ref lane: the
/// value core already produced the full `VARHDRSZ`-prefixed image, so it crosses
/// verbatim.
#[inline]
fn ret_varlena_image(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// Set a `cstring` (`tsvectorout`) result on the by-ref lane.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    let s = String::from_utf8(bytes).expect("tsvectorout: result not valid UTF-8");
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}
#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("tsvector fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
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
// fc_ adapters — I/O.
// ---------------------------------------------------------------------------

fn fc_tsvectorin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0).as_bytes().to_vec();
    let m = scratch_mcx();
    let image = ok(crate::io::tsvectorin(m.mcx(), &s));
    ret_varlena_image(fcinfo, image)
}

fn fc_tsvectorout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::io::tsvectorout(m.mcx(), arg_tsvector(fcinfo, 0)));
    ret_cstring(fcinfo, out)
}

fn fc_tsvectorsend(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let image = ok(crate::io::tsvectorsend(m.mcx(), arg_tsvector(fcinfo, 0)));
    ret_varlena_image(fcinfo, image)
}

/// `tsvectorrecv(internal)` — the wire `StringInfo` message arrives on the
/// by-ref lane as its raw bytes; rebuild a `StringInfo` (cursor 0) in a scratch
/// context and hand it to the value core, which returns the header-ful image.
fn fc_tsvectorrecv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let src = fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_varlena())
        .expect("tsvectorrecv: by-ref StringInfo arg missing from by-ref lane");
    let m = scratch_mcx();
    let mut data = mcx::PgVec::new_in(m.mcx());
    if data.try_reserve(src.len()).is_err() {
        raise(types_error::PgError::error("out of memory"));
    }
    data.extend_from_slice(src);
    let mut buf = StringInfo::from_vec(data);
    let image = ok(crate::io::tsvectorrecv(m.mcx(), &mut buf));
    ret_varlena_image(fcinfo, image)
}

// ---------------------------------------------------------------------------
// fc_ adapters — comparison.
// ---------------------------------------------------------------------------

fn fc_tsvector_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::op::tsvector_cmp(arg_tsvector(fcinfo, 0), arg_tsvector(fcinfo, 1)))
}
fn fc_tsvector_eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::op::tsvector_eq(arg_tsvector(fcinfo, 0), arg_tsvector(fcinfo, 1)))
}
fn fc_tsvector_ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::op::tsvector_ne(arg_tsvector(fcinfo, 0), arg_tsvector(fcinfo, 1)))
}
fn fc_tsvector_lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::op::tsvector_lt(arg_tsvector(fcinfo, 0), arg_tsvector(fcinfo, 1)))
}
fn fc_tsvector_le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::op::tsvector_le(arg_tsvector(fcinfo, 0), arg_tsvector(fcinfo, 1)))
}
fn fc_tsvector_gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::op::tsvector_gt(arg_tsvector(fcinfo, 0), arg_tsvector(fcinfo, 1)))
}
fn fc_tsvector_ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::op::tsvector_ge(arg_tsvector(fcinfo, 0), arg_tsvector(fcinfo, 1)))
}

// ---------------------------------------------------------------------------
// fc_ adapters — scalar manipulation.
// ---------------------------------------------------------------------------

fn fc_tsvector_strip(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let image = ok(crate::op::tsvector_strip(arg_tsvector(fcinfo, 0)));
    ret_varlena_image(fcinfo, image)
}

fn fc_tsvector_setweight(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let cw = arg_char(fcinfo, 1);
    let image = ok(crate::op::tsvector_setweight(arg_tsvector(fcinfo, 0), cw));
    ret_varlena_image(fcinfo, image)
}

fn fc_tsvector_concat(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let image = ok(crate::op::tsvector_concat(arg_tsvector(fcinfo, 0), arg_tsvector(fcinfo, 1)));
    ret_varlena_image(fcinfo, image)
}

fn fc_tsvector_length(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::op::tsvector_length(arg_tsvector(fcinfo, 0)))
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict: true,
        retset: false,
        func: Some(func),
    }
}

/// Register every `tsvector` builtin whose value core is ported and whose
/// arg/result types are expressible at the current fmgr boundary (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs/nargs from `pg_proc.dat`; every row here is `proisstrict => 't'` and not
/// retset.
pub fn register_tsvector_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- I/O ----
        builtin(3610, "tsvectorin", 1, fc_tsvectorin),
        builtin(3611, "tsvectorout", 1, fc_tsvectorout),
        builtin(3638, "tsvectorsend", 1, fc_tsvectorsend),
        builtin(3639, "tsvectorrecv", 1, fc_tsvectorrecv),
        // ---- comparison ----
        builtin(3616, "tsvector_lt", 2, fc_tsvector_lt),
        builtin(3617, "tsvector_le", 2, fc_tsvector_le),
        builtin(3618, "tsvector_eq", 2, fc_tsvector_eq),
        builtin(3619, "tsvector_ne", 2, fc_tsvector_ne),
        builtin(3620, "tsvector_ge", 2, fc_tsvector_ge),
        builtin(3621, "tsvector_gt", 2, fc_tsvector_gt),
        builtin(3622, "tsvector_cmp", 2, fc_tsvector_cmp),
        // ---- scalar manipulation ----
        builtin(3623, "tsvector_strip", 1, fc_tsvector_strip),
        builtin(3624, "tsvector_setweight", 2, fc_tsvector_setweight),
        builtin(3625, "tsvector_concat", 2, fc_tsvector_concat),
        builtin(3711, "tsvector_length", 1, fc_tsvector_length),
    ]);
}
