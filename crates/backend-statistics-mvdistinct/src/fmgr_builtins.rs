//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `pg_ndistinct` type I/O functions (`mvdistinct.c`).
//!
//! `pg_ndistinct` is the serialized multivariate n-distinct coefficient stored
//! in `pg_statistic_ext_data`. Text/binary INPUT is disallowed (the `_in`/`_recv`
//! cores raise `cannot accept a value of type pg_ndistinct`); OUTPUT is the
//! `{ "a, b": d, ... }` cstring (`pg_ndistinct_out`) and the raw serialized bytea
//! (`pg_ndistinct_send`, delegating to `byteasend`). Each entry is a `fc_<name>`
//! adapter that reads its argument off the fmgr call frame and calls the matching
//! value core. [`register_mvdistinct_builtins`] registers every row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`). OIDs / nargs / strict / retset
//! are transcribed exactly from `pg_proc.dat` (all strict, none retset).

use mcx::MemoryContext;
use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

const VARHDRSZ: usize = 4;

/// `VARDATA_ANY` of the detoasted `bytea` arg: the payload after the 4-byte
/// uncompressed length header.
#[inline]
fn arg_bytea_body<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("pg_ndistinct fn: by-ref bytea arg missing from by-ref lane");
    if image.len() >= VARHDRSZ {
        &image[VARHDRSZ..]
    } else {
        &[]
    }
}

/// Set a `cstring` (`_out`) result on the by-ref lane (the core returns the
/// NUL-terminated payload; drop the trailing NUL for the `String` carrier).
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, mut bytes: Vec<u8>) -> Datum {
    if bytes.last() == Some(&0) {
        bytes.pop();
    }
    fcinfo.set_ref_result(RefPayload::Cstring(String::from_utf8_lossy(&bytes).into_owned()));
    Datum::from_usize(0)
}

/// Set a `bytea` (`_send`) result: prepend the 4-byte varlena length header to
/// the raw serialized payload (`pq_endtypsend`'s `SET_VARSIZE`).
#[inline]
fn ret_bytea(fcinfo: &mut FunctionCallInfoBaseData, payload: &[u8]) -> Datum {
    let total = payload.len() + VARHDRSZ;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&types_datum::varlena::set_varsize_4b(total));
    img.extend_from_slice(payload);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Datum::from_usize(0)
}

fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

fn scratch_mcx() -> MemoryContext {
    MemoryContext::new("pg_ndistinct fmgr scratch")
}

fn fc_pg_ndistinct_in(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::pg_ndistinct_in() {
        Ok(()) => Datum::null(),
        Err(e) => raise(e),
    }
}
fn fc_pg_ndistinct_recv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::pg_ndistinct_recv() {
        Ok(()) => Datum::null(),
        Err(e) => raise(e),
    }
}
fn fc_pg_ndistinct_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let data = arg_bytea_body(fcinfo, 0).to_vec();
    match crate::pg_ndistinct_out(&data) {
        Ok(s) => ret_cstring(fcinfo, s),
        Err(e) => raise(e),
    }
}
fn fc_pg_ndistinct_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let data = arg_bytea_body(fcinfo, 0).to_vec();
    let payload = match crate::pg_ndistinct_send(m.mcx(), &data) {
        Ok(b) => b.as_slice().to_vec(),
        Err(e) => raise(e),
    };
    ret_bytea(fcinfo, &payload)
}

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

/// Register the `pg_ndistinct` I/O builtins (C: their `fmgr_builtins[]` rows).
pub fn register_mvdistinct_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(3355, "pg_ndistinct_in", 1, fc_pg_ndistinct_in),
        builtin(3356, "pg_ndistinct_out", 1, fc_pg_ndistinct_out),
        builtin(3357, "pg_ndistinct_recv", 1, fc_pg_ndistinct_recv),
        builtin(3358, "pg_ndistinct_send", 1, fc_pg_ndistinct_send),
    ]);
}
