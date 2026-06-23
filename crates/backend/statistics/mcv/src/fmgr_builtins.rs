//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `pg_mcv_list` type I/O functions (`mcv.c`).
//!
//! `pg_mcv_list` is the serialized multivariate MCV (most-common-values) list
//! stored in `pg_statistic_ext_data`. Text/binary INPUT is disallowed (the
//! `_in`/`_recv` cores raise `cannot accept a value of type pg_mcv_list`);
//! OUTPUT is the `\x`-prefixed hex cstring (`pg_mcv_list_out`, which C defines
//! as `return byteaout(fcinfo)`) and the raw serialized bytea
//! (`pg_mcv_list_send`, which C defines as `return byteasend(fcinfo)`).
//!
//! Each entry is a `fc_<name>` adapter that reads its argument off the fmgr call
//! frame and calls the matching value core (the `byteaout`/`byteasend` cores in
//! `backend-utils-adt-varlena`, exactly as C delegates).
//! [`register_mcv_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`). OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat` (all strict, none retset).

use ::mcx::MemoryContext;
use ::datum::Datum;
use ::fmgr::boundary::RefPayload;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

const VARHDRSZ: usize = 4;

/// `VARDATA_ANY` of the detoasted varlena arg: the payload after the 4-byte
/// uncompressed length header. The serialized `pg_mcv_list` crosses the by-ref
/// lane as its varlena image (header + payload).
#[inline]
fn arg_varlena_body<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("pg_mcv_list fn: by-ref varlena arg missing from by-ref lane");
    // `VARDATA_ANY`: skip ONE header byte for a short (1-byte, low-bit-set)
    // header, else `VARHDRSZ`. No-op while `SHORT_VARLENA_PACKING` is off.
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
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
    img.extend_from_slice(&::datum::varlena::set_varsize_4b(total));
    img.extend_from_slice(payload);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Datum::from_usize(0)
}

fn scratch_mcx() -> MemoryContext {
    MemoryContext::new("pg_mcv_list fmgr scratch")
}

fn fc_pg_mcv_list_in(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    crate::pg_mcv_list_in()?;
    Ok(Datum::null())
}
fn fc_pg_mcv_list_recv(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    crate::pg_mcv_list_recv()?;
    Ok(Datum::null())
}
fn fc_pg_mcv_list_out(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C (mcv.c:1498): `return byteaout(fcinfo)`.
    let m = scratch_mcx();
    let data = arg_varlena_body(fcinfo, 0).to_vec();
    let s = varlena::bytea::byteaout(m.mcx(), &data)?
        .as_slice()
        .to_vec();
    Ok(ret_cstring(fcinfo, s))
}
fn fc_pg_mcv_list_send(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C (mcv.c:1522): `return byteasend(fcinfo)`.
    let m = scratch_mcx();
    let data = arg_varlena_body(fcinfo, 0).to_vec();
    let payload = crate::pg_mcv_list_send(m.mcx(), &data)?.as_slice().to_vec();
    Ok(ret_bytea(fcinfo, &payload))
}

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict: true,
            retset: false,
            func: None,
        },
        native,
    )
}

/// Register the `pg_mcv_list` I/O builtins (C: their `fmgr_builtins[]` rows).
pub fn register_mcv_builtins() {
    fmgr_core::register_builtins_native([
        builtin(5018, "pg_mcv_list_in", 1, fc_pg_mcv_list_in),
        builtin(5019, "pg_mcv_list_out", 1, fc_pg_mcv_list_out),
        builtin(5020, "pg_mcv_list_recv", 1, fc_pg_mcv_list_recv),
        builtin(5021, "pg_mcv_list_send", 1, fc_pg_mcv_list_send),
    ]);
}
