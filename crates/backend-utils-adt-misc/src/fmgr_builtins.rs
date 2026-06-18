//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions in `misc.c` whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in [`crate`], and writes back the
//! result word / by-reference payload. [`register_misc_builtins`] registers
//! every row into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID
//! dispatch (and the `fmgr_isbuiltin` fast path early catalog scankeys rely on)
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.
//!
//! Result-type lanes follow the established convention (see
//! `backend-utils-adt-oid` / `-dbsize`): a `text` result is written back as a
//! `Varlena` payload (the boundary re-wraps it with the varlena header, like
//! `cstring_to_text`); a `name` result is the fixed `NAMEDATALEN` buffer image;
//! a SQL NULL (`PG_RETURN_NULL`) is `fcinfo.set_result_null(true)` with a dummy
//! word; a `void` result is the dummy `0` word.
//!
//! NOT registered here (see the crate docs / the lane's skip report):
//!
//! * `current_query` (OID 817) — its sole input `debug_query_string` is a
//!   tcop/postgres.c backend global. The only getters reachable from this leaf
//!   adt crate are the heavy `backend-tcop-postgres` owner (a dependency
//!   inversion / probable cycle) or the mis-homed `vacuum`-domain seam; neither
//!   is faithful at this boundary, and the global is not an fmgr-frame value.
//!   The value core (`current_query`) exists, but the input is not expressible
//!   here, so the row is skipped rather than hollow-registered.
//!
//! The set-returning / variadic `misc.c` rows (`pg_num_nulls`/`pg_num_nonnulls`
//! variadic shapes, `pg_get_keywords`, `pg_get_catalog_foreign_keys`,
//! `parse_ident`, `pg_tablespace_databases`, `pg_collation_for`/`pg_typeof`/
//! `any_value_transfn`) are not part of this lane's row list.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

use types_core::{AttrNumber, Oid};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)` / `PG_GETARG_OID` for a `regclass`/`regtype`/`oid` arg →
/// `DatumGetObjectId`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.arg(i).expect("misc fn: missing arg").value.as_oid()
}

/// `PG_GETARG_BOOL(i)` → `DatumGetBool`.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("misc fn: missing arg").value.as_bool()
}

/// `PG_GETARG_INT16(i)` → `DatumGetInt16` (an `int2`/`AttrNumber`).
#[inline]
fn arg_int16(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i16 {
    fcinfo.arg(i).expect("misc fn: missing arg").value.as_i16()
}

/// `PG_GETARG_FLOAT8(i)` → `DatumGetFloat8` (a `float8`).
#[inline]
fn arg_float8(fcinfo: &FunctionCallInfoBaseData, i: usize) -> f64 {
    fcinfo.arg(i).expect("misc fn: missing arg").value.as_f64()
}

/// A `text` arg's detoasted `VARDATA_ANY` payload bytes on the by-ref lane.
#[inline]
fn arg_text_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("misc fn: text arg missing from by-ref lane")
}

/// Write a `bool` result (C: `PG_RETURN_BOOL`).
#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

/// Write an `int4` result (C: `PG_RETURN_INT32`).
#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}

/// Write an `oid`/`regclass`/`regtype` result, or set the result NULL for
/// `None` (C: `PG_RETURN_NULL`). Returns the result word (a dummy `0` when
/// NULL).
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

/// Write a `void` result (C: `PG_RETURN_VOID`): a dummy `0` word.
#[inline]
fn ret_void() -> Datum {
    Datum::from_usize(0)
}

/// Write a `text` result on the by-ref lane (C: `cstring_to_text` →
/// `PG_RETURN_TEXT_P`); the boundary re-wraps the payload with the varlena
/// header. Returns the dummy result word.
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// Write an optional `text` result (C: `PG_RETURN_TEXT_P` vs `PG_RETURN_NULL`).
#[inline]
fn ret_text_opt(fcinfo: &mut FunctionCallInfoBaseData, bytes: Option<Vec<u8>>) -> Datum {
    match bytes {
        Some(b) => ret_text(fcinfo, b),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}

/// Write a `name` result (C: `namestrcpy(name, ...)` → `PG_RETURN_NAME`): the
/// fixed `NAMEDATALEN`-byte buffer image on the by-ref lane (the `name` value
/// the boundary passes by pointer), NUL-filled past the copied bytes.
#[inline]
fn ret_name(fcinfo: &mut FunctionCallInfoBaseData, name: &[u8]) -> Datum {
    const NAMEDATALEN: usize = 64;
    let mut buf = vec![0u8; NAMEDATALEN];
    // namestrcpy truncates at NAMEDATALEN-1 and always NUL-terminates.
    let n = name.len().min(NAMEDATALEN - 1);
    buf[..n].copy_from_slice(&name[..n]);
    fcinfo.set_ref_result(RefPayload::Varlena(buf));
    Datum::from_usize(0)
}

/// A scratch context for the cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("misc fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `current_database()` (misc.c:194) -> `name`.
///
/// C reads the backend global `MyDatabaseId`; here it comes from the backend
/// globals crate (`backend_utils_init_small::globals::MyDatabaseId`), exactly
/// as the fmgr shim supplies it. The result is a `name` (the `NameData` image).
fn fc_current_database(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let dboid = backend_utils_init_small::globals::MyDatabaseId();
    // Copy the name bytes out of `m` before it is dropped (the `PgVec` borrows
    // the scratch context).
    let name: Vec<u8> = match crate::current_database(m.mcx(), dboid) {
        Ok(name) => name.as_slice().to_vec(),
        Err(e) => raise(e),
    };
    ret_name(fcinfo, &name)
}

/// `pg_sleep(float8)` (misc.c:369) -> `void`.
fn fc_pg_sleep(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let secs = arg_float8(fcinfo, 0);
    match crate::pg_sleep(secs) {
        Ok(()) => ret_void(),
        Err(e) => raise(e),
    }
}

/// `pg_tablespace_location(oid)` (misc.c:300) -> `text`.
fn fc_pg_tablespace_location(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let tablespace_oid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let path: Vec<u8> = match crate::pg_tablespace_location(m.mcx(), tablespace_oid) {
        Ok(path) => path.as_slice().to_vec(),
        Err(e) => raise(e),
    };
    ret_text(fcinfo, path)
}

/// `pg_current_logfile()` (misc.c:1083) -> `text` (0-arg overload).
fn fc_pg_current_logfile(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    // No argument: logfmt = NULL (the C 0-arg entry passes no format).
    let out: Option<Vec<u8>> = match crate::pg_current_logfile(m.mcx(), None) {
        Ok(opt) => opt.map(|v| v.as_slice().to_vec()),
        Err(e) => raise(e),
    };
    ret_text_opt(fcinfo, out)
}

/// `pg_current_logfile_1arg(text)` (misc.c:1091) -> `text`.
///
/// Not strict (`proisstrict => 'f'`): a SQL NULL `logfmt` argument maps to the
/// C `PG_ARGISNULL(0)` -> NULL `logfmt` (i.e. `None`), exactly like the 0-arg
/// overload.
fn fc_pg_current_logfile_1arg(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let logfmt: Option<Vec<u8>> = if fcinfo.arg(0).map(|d| d.isnull).unwrap_or(true) {
        None
    } else {
        Some(arg_text_bytes(fcinfo, 0).to_vec())
    };
    let m = scratch_mcx();
    let out: Option<Vec<u8>> = match crate::pg_current_logfile_1arg(m.mcx(), logfmt.as_deref()) {
        Ok(opt) => opt.map(|v| v.as_slice().to_vec()),
        Err(e) => raise(e),
    };
    ret_text_opt(fcinfo, out)
}

/// `pg_relation_is_updatable(regclass, bool)` (misc.c:647) -> `int4`.
fn fc_pg_relation_is_updatable(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let reloid = arg_oid(fcinfo, 0);
    let include_triggers = arg_bool(fcinfo, 1);
    match crate::pg_relation_is_updatable(reloid, include_triggers) {
        Ok(events) => ret_i32(events),
        Err(e) => raise(e),
    }
}

/// `pg_column_is_updatable(regclass, int2, bool)` (misc.c:664) -> `bool`.
fn fc_pg_column_is_updatable(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let reloid = arg_oid(fcinfo, 0);
    let attnum: AttrNumber = arg_int16(fcinfo, 1);
    let include_triggers = arg_bool(fcinfo, 2);
    match crate::pg_column_is_updatable(reloid, attnum, include_triggers) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
}

/// `pg_get_replica_identity_index(regclass)` (misc.c:1100) -> `regclass`
/// (`PG_RETURN_OID` / `PG_RETURN_NULL`).
fn fc_pg_get_replica_identity_index(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let reloid = arg_oid(fcinfo, 0);
    match crate::pg_get_replica_identity_index(reloid) {
        Ok(opt) => ret_oid_opt(fcinfo, opt),
        Err(e) => raise(e),
    }
}

/// `pg_input_is_valid(text, text)` (misc.c:695) -> `bool`.
fn fc_pg_input_is_valid(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let str = arg_text_bytes(fcinfo, 0);
    let typname = arg_text_bytes(fcinfo, 1);
    match crate::pg_input_is_valid(str, typname) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
}

/// `pg_basetype(regtype)` (misc.c:582) -> `regtype` (`PG_RETURN_OID` /
/// `PG_RETURN_NULL` for a bogus OID).
///
/// The per-step `SearchSysCache1(TYPEOID, ...)` projection is supplied by the
/// syscache `pg_type_form` seam (`Form_pg_type.typtype`/`typbasetype`); the
/// domain-stack loop lives in [`crate::pg_basetype`]. `TYPTYPE_DOMAIN` is `'d'`.
fn fc_pg_basetype(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    const TYPTYPE_DOMAIN: i8 = b'd' as i8;
    let typid = arg_oid(fcinfo, 0);
    let step_lookup = |t: Oid| -> types_error::PgResult<Option<crate::TypeBaseStep>> {
        // SearchSysCache1(TYPEOID, ObjectIdGetDatum(t)); !HeapTupleIsValid -> None.
        match backend_utils_cache_syscache_seams::pg_type_form::call(t)? {
            Some(form) => Ok(Some(crate::TypeBaseStep {
                is_domain: form.typtype == TYPTYPE_DOMAIN,
                typbasetype: form.typbasetype,
            })),
            None => Ok(None),
        }
    };
    match crate::pg_basetype(typid, step_lookup) {
        Ok(opt) => ret_oid_opt(fcinfo, opt),
        Err(e) => raise(e),
    }
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

/// Register every `misc.c` builtin this lane covers (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs / nargs / strict /
/// retset are transcribed exactly from `pg_proc.dat` (none are `retset`).
pub fn register_misc_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // 861  current_database()              -> name   (strict default 't', 0 args)
        builtin(861, "current_database", 0, true, false, fc_current_database),
        // 2626 pg_sleep(float8)                -> void
        builtin(2626, "pg_sleep", 1, true, false, fc_pg_sleep),
        // 3778 pg_tablespace_location(oid)     -> text
        builtin(
            3778,
            "pg_tablespace_location",
            1,
            true,
            false,
            fc_pg_tablespace_location,
        ),
        // 3800 pg_current_logfile()            -> text   (proisstrict => 'f', 0 args)
        builtin(
            3800,
            "pg_current_logfile",
            0,
            false,
            false,
            fc_pg_current_logfile,
        ),
        // 3801 pg_current_logfile(text)        -> text   (proisstrict => 'f')
        // C: prosrc/funcName is `pg_current_logfile_1arg` (pg_proc.dat:6815).
        builtin(
            3801,
            "pg_current_logfile_1arg",
            1,
            false,
            false,
            fc_pg_current_logfile_1arg,
        ),
        // 3842 pg_relation_is_updatable(regclass, bool) -> int4
        builtin(
            3842,
            "pg_relation_is_updatable",
            2,
            true,
            false,
            fc_pg_relation_is_updatable,
        ),
        // 3843 pg_column_is_updatable(regclass, int2, bool) -> bool
        builtin(
            3843,
            "pg_column_is_updatable",
            3,
            true,
            false,
            fc_pg_column_is_updatable,
        ),
        // 6120 pg_get_replica_identity_index(regclass) -> regclass
        builtin(
            6120,
            "pg_get_replica_identity_index",
            1,
            true,
            false,
            fc_pg_get_replica_identity_index,
        ),
        // 6210 pg_input_is_valid(text, text)   -> bool
        builtin(6210, "pg_input_is_valid", 2, true, false, fc_pg_input_is_valid),
        // 6315 pg_basetype(regtype)            -> regtype
        builtin(6315, "pg_basetype", 1, true, false, fc_pg_basetype),
    ]);
}
