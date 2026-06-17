//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `text` relational operators and the `name`<->`text` cross-type comparison
//! family from `varlena.c`.
//!
//! These carry the catalog-scankey equality `oprcode`s and the btree
//! `BTORDER_PROC`s for `text`-keyed catalog index columns
//! (`texteq`/`bttextcmp`) and for the `name`<->`text` cross-type entries the
//! `text_ops` opfamily declares (`nameeqtext`/`texteqname`/`btnametextcmp`/
//! `bttextnamecmp`, used e.g. when a `name` column is probed with a `text`
//! constant). They must be in the fmgr builtin fast-path table so
//! `fmgr_isbuiltin` resolves them during early catalog scans without recursing
//! into the not-yet-built syscache.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr call
//! frame and calls the matching value core. A `text` arg arrives as its
//! detoasted `VARDATA_ANY` payload on the by-ref lane (the boundary strips the
//! varlena header); a `name` arg arrives as its fixed `NAMEDATALEN` buffer bytes
//! (the cores trim at the first NUL via `name_str`). The collation is read from
//! `fcinfo.fncollation` (C: `PG_GET_COLLATION()`). OIDs / nargs / strict / retset
//! are transcribed exactly from `pg_proc.dat` (all strict, none retset).
//!
//! Scope: only the `comparison.rs` text relational family and the
//! `name_pattern.rs` name<->text family are registered here — the boot-critical
//! comparator subset. The rest of `varlena.c`'s broad fmgr surface is deferred.

use types_core::Oid;
use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// A `text`/`name` arg's by-ref payload bytes (the boundary strips the varlena
/// header for `text`; for `name` this is the fixed `NAMEDATALEN` buffer, which
/// the cores NUL-trim).
#[inline]
fn arg_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("varlena cmp fn: by-ref arg missing from by-ref lane")
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
#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
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
// fc_ adapters — text relational family (comparison.rs).
// ---------------------------------------------------------------------------

fn fc_texteq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::comparison::texteq(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_textne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::comparison::textne(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_text_lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::comparison::text_lt(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_text_le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::comparison::text_le(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_text_gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::comparison::text_gt(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_text_ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::comparison::text_ge(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_bttextcmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_i32(ok(crate::comparison::bttextcmp(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_text_starts_with(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::comparison::text_starts_with(
        arg_bytes(fcinfo, 0),
        arg_bytes(fcinfo, 1),
        c,
    )))
}
fn fc_btvarstrequalimage(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: btvarstrequalimage(internal) — arg0 (`opcintype`/internal) is ignored;
    // the answer depends only on the collation read from the frame.
    let c = collation(fcinfo);
    ret_bool(ok(crate::comparison::btvarstrequalimage(c)))
}

// ---------------------------------------------------------------------------
// fc_ adapters — name<->text family (name_pattern.rs). For `name`-first
// functions arg0 is the `name`; for `text`-first functions arg0 is the `text`.
// ---------------------------------------------------------------------------

fn fc_nameeqtext(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::name_pattern::nameeqtext(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_namenetext(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::name_pattern::namenetext(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_namelttext(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::name_pattern::namelttext(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_nameletext(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::name_pattern::nameletext(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_namegttext(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::name_pattern::namegttext(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_namegetext(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::name_pattern::namegetext(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_btnametextcmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_i32(ok(crate::name_pattern::btnametextcmp(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_texteqname(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::name_pattern::texteqname(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_textnename(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::name_pattern::textnename(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_textltname(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::name_pattern::textltname(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_textlename(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::name_pattern::textlename(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_textgtname(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::name_pattern::textgtname(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_textgename(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_bool(ok(crate::name_pattern::textgename(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_bttextnamecmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    ret_i32(ok(crate::name_pattern::bttextnamecmp(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
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

/// Register the boot-critical `text` / `name`<->`text` comparison builtins (C:
/// their `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs/nargs from `pg_proc.dat`; all are `proisstrict => 't'` and not retset.
pub fn register_varlena_compare_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- text relational (comparison.rs) ----
        builtin(67, "texteq", 2, fc_texteq),
        builtin(157, "textne", 2, fc_textne),
        builtin(740, "text_lt", 2, fc_text_lt),
        builtin(741, "text_le", 2, fc_text_le),
        builtin(742, "text_gt", 2, fc_text_gt),
        builtin(743, "text_ge", 2, fc_text_ge),
        builtin(360, "bttextcmp", 2, fc_bttextcmp),
        builtin(3696, "text_starts_with", 2, fc_text_starts_with),
        builtin(5050, "btvarstrequalimage", 1, fc_btvarstrequalimage),
        // ---- name <-> text (name_pattern.rs) ----
        builtin(240, "nameeqtext", 2, fc_nameeqtext),
        builtin(245, "namenetext", 2, fc_namenetext),
        builtin(241, "namelttext", 2, fc_namelttext),
        builtin(242, "nameletext", 2, fc_nameletext),
        builtin(244, "namegttext", 2, fc_namegttext),
        builtin(243, "namegetext", 2, fc_namegetext),
        builtin(246, "btnametextcmp", 2, fc_btnametextcmp),
        builtin(247, "texteqname", 2, fc_texteqname),
        builtin(252, "textnename", 2, fc_textnename),
        builtin(248, "textltname", 2, fc_textltname),
        builtin(249, "textlename", 2, fc_textlename),
        builtin(251, "textgtname", 2, fc_textgtname),
        builtin(250, "textgename", 2, fc_textgename),
        builtin(253, "bttextnamecmp", 2, fc_bttextnamecmp),
    ]);
}
