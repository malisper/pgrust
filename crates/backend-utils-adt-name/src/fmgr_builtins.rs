//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `name.c` functions whose argument/result types are expressible at the
//! current fmgr boundary (the `name` I/O, the comparison operators, `btnamecmp`,
//! and `nameconcatoid`).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core (already ported in this crate),
//! and writes back the result word / by-reference payload. [`register_name_builtins`]
//! registers every row into the fmgr-core builtin table (C: `fmgr_builtins[]`),
//! so by-OID dispatch and the `fmgr_isbuiltin` fast path resolve them — early
//! catalog name-column scankeys (`nameeq`) need this before any catalog access.
//!
//! A `name` value is pass-by-reference; it crosses the boundary as a
//! `NAMEDATALEN`-byte varlena image (C passes the whole `NameData` by pointer).
//! `PG_GET_COLLATION()` is `fcinfo->fncollation`. The no-argument SQL functions
//! `getpgusername`/`current_user`/`session_user`/`current_schema` (zero fmgr-frame
//! inputs, seam-backed catalog reads, `name` result) are registered here too.
//! `btnamesortsupport` (an `internal` SortSupport arg) and `current_schemas`
//! (needs the array carrier) are NOT registered.

use types_core::{Oid, NAMEDATALEN};
use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};
use types_stringinfo::StringInfo;
use types_tuple::heaptuple::NameData;

use crate::{btnamecmp, nameconcatoid, namein, nameout, namerecv, namesend};
use crate::{current_schema, current_user, session_user};
use crate::{nameeq, namege, namegt, namele, namelt, namene};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_NAME(i)`: a `name` value as a `NameData` (a copy of the
/// `NAMEDATALEN`-byte varlena image, NUL-padded to the fixed size).
#[inline]
fn arg_name(fcinfo: &FunctionCallInfoBaseData, i: usize) -> NameData {
    let bytes = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("name fn: name arg missing from by-ref lane");
    let mut nd = NameData::default();
    let n = bytes.len().min(NAMEDATALEN as usize);
    nd.data[..n].copy_from_slice(&bytes[..n]);
    nd
}

#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("name fn: cstring arg missing from by-ref lane")
}

#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("name fn: by-ref arg missing from by-ref lane")
}

#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.arg(i).expect("name fn: missing arg").value.as_oid()
}

/// `PG_GET_COLLATION()` — `fcinfo->fncollation`.
#[inline]
fn get_collation(fcinfo: &FunctionCallInfoBaseData) -> Oid {
    fcinfo.fncollation
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}
#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}
/// Set a `name` result: the full `NAMEDATALEN`-byte image on the by-ref lane.
#[inline]
fn ret_name(fcinfo: &mut FunctionCallInfoBaseData, nd: &NameData) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(nd.data.to_vec()));
    Datum::from_usize(0)
}
/// Set a `bytea` (`namesend`) result.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("name fmgr scratch")
}

fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

macro_rules! ok_or_raise {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => raise(e),
        }
    };
}

// ---------------------------------------------------------------------------
// I/O
// ---------------------------------------------------------------------------

fn fc_namein(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let nd = ok_or_raise!(namein(arg_cstring(fcinfo, 0)));
    ret_name(fcinfo, &nd)
}
fn fc_nameout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let nd = arg_name(fcinfo, 0);
    let m = scratch_mcx();
    let s = ok_or_raise!(nameout(m.mcx(), &nd)).as_str().to_string();
    ret_cstring(fcinfo, s)
}
fn fc_namerecv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // Build a StringInfo over the wire bytes, charged to a scratch ctx.
    let m = scratch_mcx();
    let src = arg_varlena(fcinfo, 0);
    let mut data = mcx::PgVec::new_in(m.mcx());
    if data.try_reserve(src.len()).is_err() {
        raise(types_error::PgError::error("out of memory"));
    }
    data.extend_from_slice(src);
    let mut buf = StringInfo::from_vec(data);
    let nd = ok_or_raise!(namerecv(m.mcx(), &mut buf));
    ret_name(fcinfo, &nd)
}
fn fc_namesend(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let nd = arg_name(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = ok_or_raise!(namesend(m.mcx(), &nd)).as_bytes().to_vec();
    ret_varlena(fcinfo, bytes)
}

// ---------------------------------------------------------------------------
// Comparison operators + btnamecmp (collation via PG_GET_COLLATION).
// ---------------------------------------------------------------------------

macro_rules! namecmp_op {
    ($fc:ident, $core:ident) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let a = arg_name(fcinfo, 0);
            let b = arg_name(fcinfo, 1);
            let collid = get_collation(fcinfo);
            ret_bool(ok_or_raise!($core(&a, &b, collid)))
        }
    };
}

namecmp_op!(fc_nameeq, nameeq);
namecmp_op!(fc_namene, namene);
namecmp_op!(fc_namelt, namelt);
namecmp_op!(fc_namele, namele);
namecmp_op!(fc_namegt, namegt);
namecmp_op!(fc_namege, namege);

fn fc_btnamecmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_name(fcinfo, 0);
    let b = arg_name(fcinfo, 1);
    let collid = get_collation(fcinfo);
    ret_i32(ok_or_raise!(btnamecmp(&a, &b, collid)))
}

fn fc_nameconcatoid(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let nam = arg_name(fcinfo, 0);
    let oid = arg_oid(fcinfo, 1);
    let nd = ok_or_raise!(nameconcatoid(&nam, oid));
    ret_name(fcinfo, &nd)
}

// ---------------------------------------------------------------------------
// No-argument SQL functions (name result; seam-backed catalog reads).
// ---------------------------------------------------------------------------

/// `current_user()` (name.c:275). `getpgusername`/`current_user` share this
/// `prosrc => 'current_user'` body.
fn fc_current_user(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let nd = ok_or_raise!(current_user(m.mcx()));
    ret_name(fcinfo, &nd)
}

/// `session_user()` (name.c:281).
fn fc_session_user(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let nd = ok_or_raise!(session_user(m.mcx()));
    ret_name(fcinfo, &nd)
}

/// `current_schema()` (name.c:291). Returns SQL NULL (`PG_RETURN_NULL()`) when
/// the active search path is empty / its head namespace was recently deleted.
fn fc_current_schema(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    match ok_or_raise!(current_schema(m.mcx())) {
        Some(nd) => ret_name(fcinfo, &nd),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
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

/// Like [`builtin`] but with explicit `strict`/`retset` — the no-argument SQL
/// functions are `proisstrict => 'f'` in `pg_proc.dat`.
fn builtin_full(
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

/// Register every registerable `name.c` builtin (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs/nargs from
/// `pg_proc.dat`; all are `proisstrict => 't'` and not retset.
pub fn register_name_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- I/O ----
        builtin(34, "namein", 1, fc_namein),
        builtin(35, "nameout", 1, fc_nameout),
        builtin(2422, "namerecv", 1, fc_namerecv),
        builtin(2423, "namesend", 1, fc_namesend),
        // ---- comparison operators ----
        builtin(62, "nameeq", 2, fc_nameeq),
        builtin(659, "namene", 2, fc_namene),
        builtin(655, "namelt", 2, fc_namelt),
        builtin(656, "namele", 2, fc_namele),
        builtin(657, "namegt", 2, fc_namegt),
        builtin(658, "namege", 2, fc_namege),
        builtin(359, "btnamecmp", 2, fc_btnamecmp),
        // ---- misc ----
        builtin(266, "nameconcatoid", 2, fc_nameconcatoid),
        // ---- no-argument SQL functions (name result) ----
        // pg_proc.dat: proargtypes => '' (nargs 0), no proisstrict (strict 'f'),
        // no proretset (retset 'f').
        builtin_full(710, "current_user", 0, true, false, fc_current_user),
        builtin_full(745, "current_user", 0, true, false, fc_current_user),
        builtin_full(746, "session_user", 0, true, false, fc_session_user),
        builtin_full(1402, "current_schema", 0, true, false, fc_current_schema),
    ]);
}
