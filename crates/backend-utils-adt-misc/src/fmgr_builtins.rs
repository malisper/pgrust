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
//! The variadic count functions (`pg_num_nulls`/`pg_num_nonnulls`, OIDs
//! 438/440), `pg_collation_for` (OID 3162), `pg_typeof` (OID 1619) and
//! `any_value_transfn` (OID 6292) ARE registered: the variadic `VARIADIC "any"`
//! frame is read via `get_fn_expr_variadic` (separate args vs a single
//! `ArrayType` image), and the static arg-type / collation come off
//! `get_fn_expr_argtype` / `fcinfo.fncollation`. `parse_ident` (OID 1268) IS
//! registered: its `text[]` result is assembled from the identifier parts via
//! the arrayfuncs `build_text_array_nullable` seam.
//!
//! The set-returning `misc.c` rows (`pg_get_keywords`,
//! `pg_get_catalog_foreign_keys`, `pg_tablespace_databases`) are not part of
//! this lane's row list — they need the SRF tuplestore boundary.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

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
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("misc fn: text arg missing from by-ref lane");
    // VARDATA_ANY: a short (1-byte, low-bit-set) header skips ONE byte, an
    // ordinary 4-byte header skips `VARHDRSZ`. A small stored text reaches an
    // fmgr arg verbatim (the EEOP_FUNCEXPR boundary does not detoast/unpack), so
    // a fixed 4-byte strip would drop three payload bytes — and over-read a value
    // shorter than four bytes (e.g. an unnested `'1a'` array element) — once
    // `SHORT_VARLENA_PACKING` is on. No-op while the flag is off (4-byte stored).
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= types_datum::varlena::VARHDRSZ => {
            &image[types_datum::varlena::VARHDRSZ..]
        }
        _ => &[],
    }
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
    // cstring_to_text: prepend the 4-byte varlena header (header-ful image).
    let mut img = Vec::with_capacity(types_datum::varlena::VARHDRSZ + bytes.len());
    img.extend_from_slice(&types_datum::varlena::set_varsize_4b(
        types_datum::varlena::VARHDRSZ + bytes.len(),
    ));
    img.extend_from_slice(&bytes);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
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

// ---------------------------------------------------------------------------
// fc_ adapters (Result-native: `ereport(ERROR)` travels as `Err(PgError)`
// straight back to the fmgr dispatch `invoke_builtin`, no panic/catch_unwind).
// ---------------------------------------------------------------------------

/// `current_database()` (misc.c:194) -> `name`.
///
/// C reads the backend global `MyDatabaseId`; here it comes from the backend
/// globals crate (`backend_utils_init_small::globals::MyDatabaseId`), exactly
/// as the fmgr shim supplies it. The result is a `name` (the `NameData` image).
fn fc_current_database(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let dboid = backend_utils_init_small::globals::MyDatabaseId();
    // Copy the name bytes out of `m` before it is dropped (the `PgVec` borrows
    // the scratch context).
    let name: Vec<u8> = crate::current_database(m.mcx(), dboid)?.as_slice().to_vec();
    Ok(ret_name(fcinfo, &name))
}

/// `pg_sleep(float8)` (misc.c:369) -> `void`.
fn fc_pg_sleep(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let secs = arg_float8(fcinfo, 0);
    crate::pg_sleep(secs)?;
    Ok(ret_void())
}

/// `pg_tablespace_location(oid)` (misc.c:300) -> `text`.
fn fc_pg_tablespace_location(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let tablespace_oid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let path: Vec<u8> = crate::pg_tablespace_location(m.mcx(), tablespace_oid)?
        .as_slice()
        .to_vec();
    Ok(ret_text(fcinfo, path))
}

/// `pg_current_logfile()` (misc.c:1083) -> `text` (0-arg overload).
fn fc_pg_current_logfile(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    // No argument: logfmt = NULL (the C 0-arg entry passes no format).
    let out: Option<Vec<u8>> = crate::pg_current_logfile(m.mcx(), None)?
        .map(|v| v.as_slice().to_vec());
    Ok(ret_text_opt(fcinfo, out))
}

/// `pg_current_logfile_1arg(text)` (misc.c:1091) -> `text`.
///
/// Not strict (`proisstrict => 'f'`): a SQL NULL `logfmt` argument maps to the
/// C `PG_ARGISNULL(0)` -> NULL `logfmt` (i.e. `None`), exactly like the 0-arg
/// overload.
fn fc_pg_current_logfile_1arg(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let logfmt: Option<Vec<u8>> = if fcinfo.arg(0).map(|d| d.isnull).unwrap_or(true) {
        None
    } else {
        Some(arg_text_bytes(fcinfo, 0).to_vec())
    };
    let m = scratch_mcx();
    let out: Option<Vec<u8>> = crate::pg_current_logfile_1arg(m.mcx(), logfmt.as_deref())?
        .map(|v| v.as_slice().to_vec());
    Ok(ret_text_opt(fcinfo, out))
}

/// `pg_relation_is_updatable(regclass, bool)` (misc.c:647) -> `int4`.
fn fc_pg_relation_is_updatable(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let reloid = arg_oid(fcinfo, 0);
    let include_triggers = arg_bool(fcinfo, 1);
    Ok(ret_i32(crate::pg_relation_is_updatable(
        reloid,
        include_triggers,
    )?))
}

/// `pg_column_is_updatable(regclass, int2, bool)` (misc.c:664) -> `bool`.
fn fc_pg_column_is_updatable(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let reloid = arg_oid(fcinfo, 0);
    let attnum: AttrNumber = arg_int16(fcinfo, 1);
    let include_triggers = arg_bool(fcinfo, 2);
    Ok(ret_bool(crate::pg_column_is_updatable(
        reloid,
        attnum,
        include_triggers,
    )?))
}

/// `pg_get_replica_identity_index(regclass)` (misc.c:1100) -> `regclass`
/// (`PG_RETURN_OID` / `PG_RETURN_NULL`).
fn fc_pg_get_replica_identity_index(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let reloid = arg_oid(fcinfo, 0);
    let opt = crate::pg_get_replica_identity_index(reloid)?;
    Ok(ret_oid_opt(fcinfo, opt))
}

/// `pg_input_is_valid(text, text)` (misc.c:695) -> `bool`.
fn fc_pg_input_is_valid(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let str = arg_text_bytes(fcinfo, 0);
    let typname = arg_text_bytes(fcinfo, 1);
    Ok(ret_bool(crate::pg_input_is_valid(str, typname)?))
}

/// `pg_basetype(regtype)` (misc.c:582) -> `regtype` (`PG_RETURN_OID` /
/// `PG_RETURN_NULL` for a bogus OID).
///
/// The per-step `SearchSysCache1(TYPEOID, ...)` projection is supplied by the
/// syscache `pg_type_form` seam (`Form_pg_type.typtype`/`typbasetype`); the
/// domain-stack loop lives in [`crate::pg_basetype`]. `TYPTYPE_DOMAIN` is `'d'`.
fn fc_pg_basetype(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
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
    let opt = crate::pg_basetype(typid, step_lookup)?;
    Ok(ret_oid_opt(fcinfo, opt))
}

/// `parse_ident(text, bool)` (misc.c:860) -> `text[]` (OID 1268).
///
/// Reads the qualified-name `text` arg and the `strict` `bool` arg off the
/// frame, runs the [`crate::parse_ident`] scanner to split it into identifier
/// parts, and assembles the parts into a `text[]` `ArrayType` image via the
/// arrayfuncs `build_text_array_nullable` seam (C: the deferred
/// `accumArrayResult`/`makeArrayResult` assembly over `CStringGetTextDatum`
/// element Datums). The flat array varlena rides back on the by-ref `Varlena`
/// lane (C `PG_RETURN_ARRAYTYPE_P`). Both args are strict (`proisstrict`
/// default `'t'`), so the frame always carries non-NULL words.
fn fc_parse_ident(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let qualname = arg_text_bytes(fcinfo, 0).to_vec();
    let strict = arg_bool(fcinfo, 1);
    let m = scratch_mcx();
    let parts = crate::parse_ident(m.mcx(), &qualname, strict)?;
    // Each parsed part is a non-null `text` element (C `CStringGetTextDatum`).
    let elems: Vec<Option<&[u8]>> = parts.iter().map(|p| Some(p.as_slice())).collect();
    let image = backend_utils_adt_arrayfuncs_seams::build_text_array_nullable::call(m.mcx(), &elems)?
        .as_slice()
        .to_vec();
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Ok(Datum::from_usize(0))
}

/// `pg_typeof(any)` (misc.c:563) -> `regtype` (`PG_RETURN_OID`).
///
/// Not strict (`proisstrict => 'f'`): the result is the static argument type
/// regardless of whether the value is NULL. The arg-type OID comes from the
/// call expression via `get_fn_expr_argtype(fcinfo->flinfo, 0)` (the fmgr
/// shim's job), exactly as C does.
fn fc_pg_typeof(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let arg0_type =
        backend_utils_fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), 0);
    Ok(Datum::from_oid(crate::pg_typeof(arg0_type)))
}

/// `pg_collation_for(any)` (misc.c:1037) -> `text` (OID 3162). Not strict
/// (`proisstrict => 'f'`): the result depends on the argument's static type and
/// the call's collation, not its runtime value. The arg-type OID comes from
/// `get_fn_expr_argtype(fcinfo->flinfo, 0)` and the collation from
/// `PG_GET_COLLATION()` (`fcinfo.fncollation`), exactly as C does. Returns NULL
/// for the two `PG_RETURN_NULL` cases (no static type / no collation).
fn fc_pg_collation_for(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let arg0_type =
        backend_utils_fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), 0);
    let collation = fcinfo.fncollation;
    let m = scratch_mcx();
    let out: Option<Vec<u8>> = crate::pg_collation_for(m.mcx(), arg0_type, collation)?
        .map(|v| v.as_slice().to_vec());
    Ok(ret_text_opt(fcinfo, out))
}

/// `ARR_NDIM(a)` — the `ndim` field at offset 4 of the header-ful flat
/// `ArrayType` image.
#[inline]
fn arr_ndim(a: &[u8]) -> i32 {
    i32::from_ne_bytes([a[4], a[5], a[6], a[7]])
}

/// `ARR_HASNULL(a)` — `(a)->dataoffset != 0` (the `dataoffset` field at offset 8).
#[inline]
fn arr_hasnull(a: &[u8]) -> bool {
    i32::from_ne_bytes([a[8], a[9], a[10], a[11]]) != 0
}

/// `ARR_DIMS(a)[i]` — dimension `i` at `sizeof(ArrayType) + i*sizeof(int)`.
/// `sizeof(ArrayType)` is 16 (`vl_len_`, `ndim`, `dataoffset`, `elemtype`).
#[inline]
fn arr_dim(a: &[u8], i: usize) -> i32 {
    let off = 16 + i * 4;
    i32::from_ne_bytes([a[off], a[off + 1], a[off + 2], a[off + 3]])
}

/// `ARR_NULLBITMAP(a)` — the null bitmap bytes, or `None` when the array has no
/// bitmap (`!ARR_HASNULL`). C: `sizeof(ArrayType) + 2*sizeof(int)*ARR_NDIM(a)`.
#[inline]
fn arr_nullbitmap<'a>(a: &'a [u8]) -> Option<&'a [u8]> {
    if !arr_hasnull(a) {
        return None;
    }
    let off = 16 + 2 * 4 * arr_ndim(a) as usize;
    Some(&a[off..])
}

/// Build the `CountNullsArgs` view from the fmgr frame for `pg_num_nulls` /
/// `pg_num_nonnulls` (C: `count_nulls`). When the call is `VARIADIC arr`
/// (`get_fn_expr_variadic`), read the single array arg's header; otherwise count
/// the per-argument SQL-NULL flags. Returns `None` only in the variadic-NULL
/// case (C: `count_nulls` returns false → `PG_RETURN_NULL`).
///
/// The variadic array crosses on the by-ref lane as its header-ful `ArrayType`
/// image; the executor builds this array in memory, so it is never TOAST-ed
/// (C's defensive `PG_GETARG_ARRAYTYPE_P` detoast is a no-op here).
fn count_nulls_result(fcinfo: &FunctionCallInfoBaseData) -> Option<(i32, i32)> {
    if backend_utils_fmgr_core::get_fn_expr_variadic(fcinfo.flinfo.as_deref()) {
        // Assert(PG_NARGS() == 1).
        if fcinfo.arg(0).map(|d| d.isnull).unwrap_or(true) {
            // VARIADIC NULL -> NULL.
            return None;
        }
        let arr = fcinfo
            .ref_arg(0)
            .and_then(|p| p.as_varlena())
            .expect("pg_num_nulls: variadic array arg missing from by-ref lane");
        let ndim = arr_ndim(arr);
        // nitems = ArrayGetNItems(ndim, dims) = product of dims (0 when ndim==0).
        let mut nitems: i32 = if ndim > 0 { 1 } else { 0 };
        for i in 0..ndim as usize {
            nitems = nitems.saturating_mul(arr_dim(arr, i));
        }
        let bitmap = arr_nullbitmap(arr);
        crate::count_nulls(&crate::CountNullsArgs::Variadic {
            arg_is_null: false,
            nitems,
            bitmap,
        })
    } else {
        // Separate arguments: one isnull flag per fmgr arg.
        let nargs = fcinfo.nargs();
        let isnull: Vec<bool> = (0..nargs)
            .map(|i| fcinfo.arg(i).map(|d| d.isnull).unwrap_or(false))
            .collect();
        crate::count_nulls(&crate::CountNullsArgs::Separate(&isnull))
    }
}

/// `pg_num_nulls(VARIADIC "any")` (misc.c:161) -> `int4` (OID 438). Not strict
/// (`proisstrict => 'f'`) and variadic; nargs is the pg_proc `1`.
fn fc_pg_num_nulls(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    match count_nulls_result(fcinfo) {
        Some((_nargs, nulls)) => Ok(ret_i32(nulls)),
        None => {
            fcinfo.set_result_null(true);
            Ok(Datum::from_usize(0))
        }
    }
}

/// `pg_num_nonnulls(VARIADIC "any")` (misc.c:177) -> `int4` (OID 440).
fn fc_pg_num_nonnulls(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    match count_nulls_result(fcinfo) {
        Some((nargs, nulls)) => Ok(ret_i32(nargs - nulls)),
        None => {
            fcinfo.set_result_null(true);
            Ok(Datum::from_usize(0))
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

/// `any_value_transfn(state, value)` (misc.c:1120) — `PG_RETURN_DATUM(
/// PG_GETARG_DATUM(0))`: keep the running state unchanged. The aggregate is
/// `proisstrict => 't'` so a NULL is never the running state once any non-NULL
/// has been seen. Pass arg 0 through verbatim: the value word, and — for a
/// by-ref type (anyelement may be `text`, an array, ...) — its `RefPayload`.
fn fc_any_value_transfn(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let value = fcinfo
        .arg(0)
        .expect("any_value_transfn: missing state arg")
        .value;
    if let Some(payload) = fcinfo.take_ref_arg(0) {
        fcinfo.set_ref_result(payload);
    }
    Ok(value)
}

/// Register every `misc.c` builtin this lane covers (C: their `fmgr_builtins[]`
/// rows) as **Result-native** (the panic→Result migration; see
/// `docs/proposals/panic-to-result-migration.md`). Called from this crate's
/// `init_seams()`. OIDs / nargs / strict / retset are transcribed exactly from
/// `pg_proc.dat` (none are `retset`).
pub fn register_misc_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        // 861  current_database()              -> name   (strict default 't', 0 args)
        builtin(861, "current_database", 0, true, false, fc_current_database),
        // 6292 any_value_transfn(anyelement, anyelement) -> anyelement
        builtin(6292, "any_value_transfn", 2, true, false, fc_any_value_transfn),
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
        // 1268 parse_ident(text, bool)         -> text[]
        builtin(1268, "parse_ident", 2, true, false, fc_parse_ident),
        // 1619 pg_typeof(any)                  -> regtype (proisstrict => 'f')
        builtin(1619, "pg_typeof", 1, false, false, fc_pg_typeof),
        // 3162 pg_collation_for(any)           -> text    (proisstrict => 'f')
        builtin(3162, "pg_collation_for", 1, false, false, fc_pg_collation_for),
        // 438  num_nulls(VARIADIC "any")       -> int4    (variadic, proisstrict 'f')
        // C prosrc is `pg_num_nulls`; nargs is the pg_proc `1` (the variadic any).
        builtin(438, "pg_num_nulls", 1, false, false, fc_pg_num_nulls),
        // 440  num_nonnulls(VARIADIC "any")    -> int4    (variadic, proisstrict 'f')
        builtin(440, "pg_num_nonnulls", 1, false, false, fc_pg_num_nonnulls),
    ]);
}
