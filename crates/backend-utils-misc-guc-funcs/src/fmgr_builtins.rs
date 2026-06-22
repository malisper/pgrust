//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `guc_funcs.c` functions whose argument/result types are expressible at the
//! current fmgr boundary: the `text`-in / `text`-out GUC accessors
//! `current_setting` (`show_config_by_name` / `show_config_by_name_missing_ok`)
//! and `set_config` (`set_config_by_name`).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls this crate's real value core (`config_option_value` =
//! `GetConfigOptionByName`, and `set_config_option_call` = `set_config_option`),
//! and writes back the `text` result on the by-ref lane.
//! [`register_guc_funcs_builtins`] registers every row into the fmgr-core
//! builtin table (C: `fmgr_builtins[]`), so by-OID dispatch and the
//! `fmgr_isbuiltin` fast path resolve them. OIDs / nargs / strict / retset are
//! transcribed exactly from `pg_proc.dat`:
//!
//! * `current_setting(text) -> text` (oid 2077, `show_config_by_name`):
//!   `proargtypes => 'text'` (nargs 1), `proisstrict` defaulted `'t'`, not retset.
//! * `current_setting(text, bool) -> text` (oid 3294,
//!   `show_config_by_name_missing_ok`): `proargtypes => 'text bool'` (nargs 2),
//!   `proisstrict` defaulted `'t'`, not retset.
//! * `set_config(text, text, bool) -> text` (oid 2078, `set_config_by_name`):
//!   `proargtypes => 'text text bool'` (nargs 3), `proisstrict => 'f'` (NOT
//!   strict — the body NULL-checks each arg), not retset.
//!
//! A `text` arg arrives as its detoasted `VARDATA_ANY` payload on the by-ref
//! lane (the boundary strips the varlena header), so the payload bytes are the
//! string content (C: `TextDatumGetCString`). A `text` result is set on the
//! by-ref lane as the payload bytes (header stripped — byte-identical to
//! `cstring_to_text`'s payload minus the header). A `bool` arg is the low byte
//! of its by-value word (C: `PG_GETARG_BOOL`). `PG_ARGISNULL(i)` is the arg's
//! `isnull` flag; `PG_RETURN_NULL()` sets the frame's `isnull`.

use alloc::string::String;

use types_datum::Datum;
use types_error::PgResult;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use backend_utils_error::ereport;
use types_error::{ERROR, ERRCODE_NULL_VALUE_NOT_ALLOWED};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `TextDatumGetCString(PG_GETARG_DATUM(i))`: a `text` arg's detoasted
/// `VARDATA_ANY` payload (the boundary already stripped the varlena header), as
/// the string content.
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("guc_funcs fn: text arg missing from by-ref lane");
    let bytes = vardata_any(image);
    core::str::from_utf8(bytes).expect("guc_funcs fn: text arg is not valid UTF-8")
}

/// `VARDATA_ANY(ptr)` for an inline (non-compressed, non-external) varlena image:
/// skip ONE header byte for a short (1-byte, low-bit-set) header, else `VARHDRSZ`
/// (4). A small stored value arrives short-headed once `SHORT_VARLENA_PACKING` is
/// on; a fixed 4-byte strip would drop three payload bytes. No-op while off.
#[inline]
fn vardata_any(image: &[u8]) -> &[u8] {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= 4 => &image[4..],
        _ => &[],
    }
}

/// `PG_GETARG_BOOL(i)`: the low byte of arg `i`'s by-value word.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo
        .arg(i)
        .expect("guc_funcs fn: missing bool arg")
        .value
        .as_bool()
}

/// `PG_ARGISNULL(i)`: arg `i`'s SQL-NULL flag (missing slot is not null).
#[inline]
fn arg_is_null(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).map(|d| d.isnull).unwrap_or(false)
}

/// `PG_RETURN_TEXT_P(cstring_to_text(s))`: set a `text` result on the by-ref
/// lane (payload bytes, varlena header stripped) and return the dummy word.
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    // `cstring_to_text`: build a header-ful `text` image (4-byte length word).
    let payload = s.into_bytes();
    let total = payload.len() + 4;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(&payload);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Datum::from_usize(0)
}

/// `PG_RETURN_NULL()`: mark the frame result SQL NULL and return the dummy word.
#[inline]
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `show_config_by_name(PG_FUNCTION_ARGS)` (guc_funcs.c:807): `current_setting`.
/// `varval = GetConfigOptionByName(varname, NULL, false);
/// PG_RETURN_TEXT_P(cstring_to_text(varval));`
fn fc_show_config_by_name(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let varname = arg_text(fcinfo, 0);
    // missing_ok == false: the core errors for an unknown name and otherwise
    // returns the rendered value (never None here).
    let value = crate::config_option_value(varname, false)?;
    Ok(ret_text(fcinfo, value.unwrap_or_default()))
}

/// `show_config_by_name_missing_ok(PG_FUNCTION_ARGS)` (guc_funcs.c:825):
/// `current_setting(text, bool)`. `varval = GetConfigOptionByName(varname, NULL,
/// missing_ok); if (varval == NULL) PG_RETURN_NULL(); else
/// PG_RETURN_TEXT_P(cstring_to_text(varval));`
fn fc_show_config_by_name_missing_ok(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let varname = arg_text(fcinfo, 0);
    let missing_ok = arg_bool(fcinfo, 1);
    match crate::config_option_value(varname, missing_ok)? {
        Some(value) => Ok(ret_text(fcinfo, value)),
        // missing_ok == true and no such variable: return NULL.
        None => Ok(ret_null(fcinfo)),
    }
}

/// `set_config_by_name(PG_FUNCTION_ARGS)` (guc_funcs.c:332): `set_config`. NOT
/// strict — the body NULL-checks each arg (arg0 NULL is an error; arg1 NULL is a
/// RESET; arg2 NULL defaults `is_local` to false). After applying the option it
/// returns the new current value.
fn fc_set_config_by_name(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    // if (PG_ARGISNULL(0)) ereport(ERROR, NULL_VALUE_NOT_ALLOWED, "SET requires
    // parameter name");
    if arg_is_null(fcinfo, 0) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
            .errmsg("SET requires parameter name")
            .into_error());
    }
    let name = arg_text(fcinfo, 0);

    // value = PG_ARGISNULL(1) ? NULL : TextDatumGetCString(PG_GETARG_DATUM(1));
    let value: Option<&str> = if arg_is_null(fcinfo, 1) {
        None
    } else {
        Some(arg_text(fcinfo, 1))
    };

    // is_local = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
    let is_local = if arg_is_null(fcinfo, 2) {
        false
    } else {
        arg_bool(fcinfo, 2)
    };

    // (void) set_config_option(name, value, superuser()?PGC_SUSET:PGC_USERSET,
    //   PGC_S_SESSION, is_local?GUC_ACTION_LOCAL:GUC_ACTION_SET, true, 0, false);
    let action = if is_local {
        backend_utils_misc_guc::GUC_ACTION_LOCAL
    } else {
        backend_utils_misc_guc::GUC_ACTION_SET
    };
    crate::set_config_option_call(name, value, crate::suset_or_userset(), action)?;

    // new_value = GetConfigOptionByName(name, NULL, false); return as text.
    let new_value = crate::config_option_value(name, false)?;
    Ok(ret_text(fcinfo, new_value.unwrap_or_default()))
}

/// `pg_settings_get_flags(PG_FUNCTION_ARGS)` (guc_funcs.c:541):
/// `pg_settings_get_flags(text) -> text[]`. `record = find_option(varname,
/// false, true, ERROR); if (record == NULL) PG_RETURN_NULL();` then collect the
/// six externally-visible flag names that are set into a `text[]` array.
fn fc_pg_settings_get_flags(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    use types_guc::{
        GUC_EXPLAIN, GUC_NOT_IN_SAMPLE, GUC_NO_RESET, GUC_NO_RESET_ALL, GUC_NO_SHOW_ALL,
        GUC_RUNTIME_COMPUTED,
    };

    let varname = arg_text(fcinfo, 0);

    // record = find_option(varname, false, true, ERROR);
    // /* return NULL if no such variable */
    // if (record == NULL) PG_RETURN_NULL();
    let Some(flags) = crate::find_option_flags(varname) else {
        return Ok(ret_null(fcinfo));
    };

    // Collect the set flag names in the exact C order.
    let mut names: alloc::vec::Vec<&'static str> = alloc::vec::Vec::with_capacity(6);
    if flags & GUC_EXPLAIN != 0 {
        names.push("EXPLAIN");
    }
    if flags & GUC_NO_RESET != 0 {
        names.push("NO_RESET");
    }
    if flags & GUC_NO_RESET_ALL != 0 {
        names.push("NO_RESET_ALL");
    }
    if flags & GUC_NO_SHOW_ALL != 0 {
        names.push("NO_SHOW_ALL");
    }
    if flags & GUC_NOT_IN_SAMPLE != 0 {
        names.push("NOT_IN_SAMPLE");
    }
    if flags & GUC_RUNTIME_COMPUTED != 0 {
        names.push("RUNTIME_COMPUTED");
    }

    // a = construct_array_builtin(flags, cnt, TEXTOID);
    // PG_RETURN_ARRAYTYPE_P(a);
    //
    // Build the element `text` Datums and the result ArrayType image in a
    // scratch context, then copy the (header-ful) array varlena bytes onto the
    // by-ref result lane (the boundary owns the result image).
    let scratch = mcx::MemoryContext::new("pg_settings_get_flags");
    let mcx = scratch.mcx();

    let mut elems: alloc::vec::Vec<Datum> = alloc::vec::Vec::with_capacity(names.len());
    for n in &names {
        elems.push(backend_utils_adt_varlena_seams::cstring_to_text::call(mcx, n)?);
    }

    let arr = backend_utils_adt_arrayfuncs_seams::construct_array_builtin_v::call(
        mcx,
        elems.as_slice(),
        types_tuple::heaptuple::TEXTOID,
    )?;

    let image = arr.as_ref_bytes().to_vec();
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Ok(Datum::from_usize(0))
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
    func: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: alloc::string::ToString::to_string(name),
            nargs,
            strict,
            retset,
            func: None,
        },
        func,
    )
}

/// Register the `text`-boundary `guc_funcs.c` SQL functions (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs / nargs / strict / retset transcribed exactly from `pg_proc.dat`.
pub fn register_guc_funcs_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        // current_setting(text) -> text  (prosrc show_config_by_name)
        builtin(2077, "show_config_by_name", 1, true, false, fc_show_config_by_name),
        // current_setting(text, bool) -> text  (prosrc show_config_by_name_missing_ok)
        builtin(
            3294,
            "show_config_by_name_missing_ok",
            2,
            true,
            false,
            fc_show_config_by_name_missing_ok,
        ),
        // set_config(text, text, bool) -> text  (prosrc set_config_by_name, proisstrict='f')
        builtin(2078, "set_config_by_name", 3, false, false, fc_set_config_by_name),
        // pg_settings_get_flags(text) -> text[]  (prosrc pg_settings_get_flags)
        builtin(6240, "pg_settings_get_flags", 1, true, false, fc_pg_settings_get_flags),
    ]);
}
