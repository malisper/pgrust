//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `dbsize.c` functions whose argument/result types are expressible at the
//! current fmgr boundary (the scalar `oid`/`int8` I/O, the `name`/`text` by-ref
//! lanes, and the `text` result).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in [`crate`], and writes back the
//! result word / by-reference payload. [`register_dbsize_builtins`] registers
//! every row into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID
//! dispatch (and the `fmgr_isbuiltin` fast path early catalog scankeys rely on)
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat` (all are strict by default, none retset).
//!
//! The `int8`-returning size cores return `PgResult<Option<i64>>` — `None` is
//! C's `PG_RETURN_NULL()`, mapped to `fcinfo.set_result_null(true)`. A `name`
//! arg arrives as its fixed `NAMEDATALEN` buffer bytes on the by-ref lane (the
//! cores take a `&str`, so the buffer is NUL-trimmed). A `text` arg arrives as
//! its detoasted `VARDATA_ANY` payload bytes (passed straight to the byte-taking
//! core). A `text` result is written back as a `Varlena` payload (the boundary
//! re-wraps it with the varlena header), mirroring `cstring_to_text`.
//!
//! Scope: every `dbsize.c` fmgr row whose value core is ported and whose
//! argument/result types are expressible at this boundary. A `regclass` arg
//! arrives on the by-val word as its `oid` (C: `regclass` is an `Oid` typedef);
//! a `regclass`-returning function writes its result `oid` on the by-val word.
//! The `Option<Oid>`/`Option<String>` cores map `None` to C's
//! `PG_RETURN_NULL()` via `fcinfo.set_result_null(true)`.

use ::datum::Datum;
use ::fmgr::boundary::RefPayload;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use ::types_core::Oid;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)` → `DatumGetObjectId`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.arg(i).expect("dbsize fn: missing arg").value.as_oid()
}

/// `PG_GETARG_INT64(i)` → `DatumGetInt64`: arg `i`'s word as a signed `int8`.
#[inline]
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("dbsize fn: missing arg").value.as_i64()
}

/// A `name` arg's fixed `NAMEDATALEN` buffer on the by-ref lane, as a `&str`
/// trimmed at the first NUL (C: `NameStr(*PG_GETARG_NAME(i))`).
#[inline]
fn arg_name<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let bytes = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("dbsize fn: name arg missing from by-ref lane");
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..end]).expect("dbsize fn: name arg not valid UTF-8")
}

/// A `text` arg's detoasted `VARDATA_ANY` payload bytes on the by-ref lane.
#[inline]
fn arg_text_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("dbsize fn: text arg missing from by-ref lane");
    // VARDATA_ANY: skip the 4-byte varlena header on the header-ful image.
    &image[::datum::varlena::VARHDRSZ..]
}

/// Write an `int8` result, or set the result NULL for `None` (C:
/// `PG_RETURN_NULL()`). Returns the result word (a dummy `0` when NULL).
#[inline]
fn ret_int64_opt(fcinfo: &mut FunctionCallInfoBaseData, v: Option<i64>) -> Datum {
    match v {
        Some(n) => Datum::from_i64(n),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}

/// Write a `text` result on the by-ref lane (C: `cstring_to_text(buf)` →
/// `PG_RETURN_TEXT_P`); the boundary re-wraps the payload with the varlena
/// header. Returns the dummy result word.
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    // cstring_to_text: prepend the 4-byte varlena header (header-ful image).
    let payload = s.into_bytes();
    let mut img = Vec::with_capacity(::datum::varlena::VARHDRSZ + payload.len());
    img.extend_from_slice(&::datum::varlena::set_varsize_4b(
        ::datum::varlena::VARHDRSZ + payload.len(),
    ));
    img.extend_from_slice(&payload);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Datum::from_usize(0)
}

/// Write an `oid` result, or set the result NULL for `None` (C:
/// `PG_RETURN_NULL()`). Returns the result word (a dummy `0` when NULL).
#[inline]
fn ret_oid_opt(fcinfo: &mut FunctionCallInfoBaseData, v: Option<Oid>) -> Datum {
    match v {
        Some(o) => Datum::from_oid(o),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}

/// Write a `text` result on the by-ref lane, or set the result NULL for `None`
/// (C: `PG_RETURN_NULL()`). Returns the dummy result word.
#[inline]
fn ret_text_opt(fcinfo: &mut FunctionCallInfoBaseData, v: Option<String>) -> Datum {
    match v {
        Some(s) => ret_text(fcinfo, s),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}

/// A `numeric` arg's full varlena byte image on the by-ref lane (C:
/// `PG_GETARG_NUMERIC(i)` / `DatumGetNumeric`). Unlike `text` (read via
/// `arg_text_bytes`, which strips `VARHDRSZ`), a `numeric` crosses VERBATIM —
/// the numeric value cores (`numeric_abs`/`numeric_div_trunc`/...) read the
/// on-disk `Numeric` image INCLUDING its `VARHDRSZ` header, matching the
/// numeric crate's own `arg_numeric` (no strip / re-stamp).
#[inline]
fn arg_numeric<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("dbsize fn: numeric arg missing from by-ref lane")
}

/// A scratch context for the cores that allocate their numeric round-trips
/// through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("dbsize fmgr scratch")
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_database_size_oid(oid)` (OID 2324).
fn fc_pg_database_size_oid(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let db_oid = arg_oid(fcinfo, 0);
    let size = crate::pg_database_size_oid(db_oid)?;
    Ok(ret_int64_opt(fcinfo, size))
}

/// `pg_database_size_name(name)` (OID 2168).
fn fc_pg_database_size_name(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let db_name = arg_name(fcinfo, 0).to_string();
    let size = crate::pg_database_size_name(&db_name)?;
    Ok(ret_int64_opt(fcinfo, size))
}

/// `pg_tablespace_size_oid(oid)` (OID 2322).
fn fc_pg_tablespace_size_oid(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let tblspc_oid = arg_oid(fcinfo, 0);
    let size = crate::pg_tablespace_size_oid(tblspc_oid)?;
    Ok(ret_int64_opt(fcinfo, size))
}

/// `pg_tablespace_size_name(name)` (OID 2323).
fn fc_pg_tablespace_size_name(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let tblspc_name = arg_name(fcinfo, 0).to_string();
    let size = crate::pg_tablespace_size_name(&tblspc_name)?;
    Ok(ret_int64_opt(fcinfo, size))
}

/// `pg_size_pretty(int8)` (OID 2288).
fn fc_pg_size_pretty(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let size = arg_int64(fcinfo, 0);
    let text = crate::pg_size_pretty(size);
    Ok(ret_text(fcinfo, text))
}

/// `pg_size_pretty_numeric(numeric)` (OID 3166).
fn fc_pg_size_pretty_numeric(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let size = arg_numeric(fcinfo, 0);
    let text = crate::pg_size_pretty_numeric(m.mcx(), size)?;
    Ok(ret_text(fcinfo, text))
}

/// `pg_size_bytes(text)` (OID 3334).
fn fc_pg_size_bytes(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let arg = arg_text_bytes(fcinfo, 0);
    let bytes = crate::pg_size_bytes(m.mcx(), arg)?;
    Ok(Datum::from_i64(bytes))
}

/// `pg_relation_size(regclass, text)` (OID 2332).
fn fc_pg_relation_size(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let rel_oid = arg_oid(fcinfo, 0);
    // C: text_to_cstring(PG_GETARG_TEXT_PP(1)). The fork name arrives as the
    // detoasted text payload bytes on the by-ref lane.
    let fork_bytes = arg_text_bytes(fcinfo, 1);
    let fork_name = core::str::from_utf8(fork_bytes)
        .expect("dbsize fn: fork-name text arg not valid UTF-8");
    let size = crate::pg_relation_size(rel_oid, fork_name)?;
    Ok(ret_int64_opt(fcinfo, size))
}

/// `pg_total_relation_size(regclass)` (OID 2286).
fn fc_pg_total_relation_size(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let rel_oid = arg_oid(fcinfo, 0);
    let size = crate::pg_total_relation_size(rel_oid)?;
    Ok(ret_int64_opt(fcinfo, size))
}

/// `pg_table_size(regclass)` (OID 2997).
fn fc_pg_table_size(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let rel_oid = arg_oid(fcinfo, 0);
    let size = crate::pg_table_size(rel_oid)?;
    Ok(ret_int64_opt(fcinfo, size))
}

/// `pg_indexes_size(regclass)` (OID 2998).
fn fc_pg_indexes_size(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let rel_oid = arg_oid(fcinfo, 0);
    let size = crate::pg_indexes_size(rel_oid)?;
    Ok(ret_int64_opt(fcinfo, size))
}

/// `pg_relation_filenode(regclass)` (OID 2999) → `oid` (or NULL).
fn fc_pg_relation_filenode(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let relid = arg_oid(fcinfo, 0);
    let filenode = crate::pg_relation_filenode(relid)?;
    Ok(ret_oid_opt(fcinfo, filenode))
}

/// `pg_filenode_relation(oid, oid)` (OID 3454) → `regclass` (an `oid`, or NULL).
fn fc_pg_filenode_relation(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let reltablespace = arg_oid(fcinfo, 0);
    let relfilenumber = arg_oid(fcinfo, 1);
    let relid = crate::pg_filenode_relation(reltablespace, relfilenumber)?;
    Ok(ret_oid_opt(fcinfo, relid))
}

/// `pg_relation_filepath(regclass)` (OID 3034) → `text` (or NULL).
fn fc_pg_relation_filepath(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let relid = arg_oid(fcinfo, 0);
    let path = crate::pg_relation_filepath(relid)?;
    Ok(ret_text_opt(fcinfo, path))
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

/// Register the `dbsize.c` fmgr builtins this crate owns (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs / nargs / strict / retset transcribed exactly from `pg_proc.dat`
/// (all `proisstrict` defaults to `'t'`; none set `proretset`).
pub fn register_dbsize_builtins() {
    fmgr_core::register_builtins_native([
        // pg_database_size(oid) -> int8
        builtin(2324, "pg_database_size_oid", 1, true, false, fc_pg_database_size_oid),
        // pg_database_size(name) -> int8
        builtin(2168, "pg_database_size_name", 1, true, false, fc_pg_database_size_name),
        // pg_tablespace_size(oid) -> int8
        builtin(2322, "pg_tablespace_size_oid", 1, true, false, fc_pg_tablespace_size_oid),
        // pg_tablespace_size(name) -> int8
        builtin(2323, "pg_tablespace_size_name", 1, true, false, fc_pg_tablespace_size_name),
        // pg_size_pretty(int8) -> text
        builtin(2288, "pg_size_pretty", 1, true, false, fc_pg_size_pretty),
        // pg_size_pretty(numeric) -> text
        builtin(3166, "pg_size_pretty_numeric", 1, true, false, fc_pg_size_pretty_numeric),
        // pg_size_bytes(text) -> int8
        builtin(3334, "pg_size_bytes", 1, true, false, fc_pg_size_bytes),
        // pg_relation_size(regclass, text) -> int8
        builtin(2332, "pg_relation_size", 2, true, false, fc_pg_relation_size),
        // pg_total_relation_size(regclass) -> int8
        builtin(2286, "pg_total_relation_size", 1, true, false, fc_pg_total_relation_size),
        // pg_table_size(regclass) -> int8
        builtin(2997, "pg_table_size", 1, true, false, fc_pg_table_size),
        // pg_indexes_size(regclass) -> int8
        builtin(2998, "pg_indexes_size", 1, true, false, fc_pg_indexes_size),
        // pg_relation_filenode(regclass) -> oid
        builtin(2999, "pg_relation_filenode", 1, true, false, fc_pg_relation_filenode),
        // pg_filenode_relation(oid, oid) -> regclass
        builtin(3454, "pg_filenode_relation", 2, true, false, fc_pg_filenode_relation),
        // pg_relation_filepath(regclass) -> text
        builtin(3034, "pg_relation_filepath", 1, true, false, fc_pg_relation_filepath),
    ]);
}
