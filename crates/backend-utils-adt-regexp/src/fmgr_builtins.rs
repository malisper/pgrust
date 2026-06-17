//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions of `regexp.c`: the `~`/`!~`/`~*`/`!~*` operator family (over
//! `text` and `name`), `substring(string from pattern)`, the SQL-spec
//! `SIMILAR TO` escape converters, and the `regexp_replace` / `regexp_count` /
//! `regexp_instr` / `regexp_like` / `regexp_substr` scalar families.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame (a `text` arg arrives as its detoasted `VARDATA_ANY` payload on
//! the by-ref lane — the boundary strips the varlena header; a `name` arg
//! arrives as its fixed `NAMEDATALEN` buffer bytes, which the adapter NUL-trims
//! the way C's `NameStr()` does; scalar `int4` args read off the by-value word;
//! the optional trailing args of the variadic families are present iff the
//! frame `nargs` covers them). It then calls the matching value core and writes
//! back the result (`bool`/`int4` by value, `text` on the by-ref `Varlena`
//! lane, `PG_RETURN_NULL()` via `set_result_null`). The collation is read from
//! `fcinfo.fncollation` (C: `PG_GET_COLLATION()`).
//!
//! [`register_regexp_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`); it is invoked from this crate's `init_seams()`.
//! OIDs / nargs / strict / retset are transcribed exactly from `pg_proc.dat`
//! (all are strict except `similar_escape` (oid 1623, `proisstrict => 'f'`);
//! none are retset — the set-returning `regexp_matches` / `regexp_split_to_table`
//! are NOT registered here).

use types_core::Oid;
use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

use mcx::{Mcx, PgVec};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// A `text` arg's by-ref payload bytes (the boundary strips the varlena
/// header).
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("regexp fn: text arg missing from by-ref lane")
}

/// A `name` arg's value bytes: the fixed `NAMEDATALEN` buffer trimmed at the
/// first NUL (C: `NameStr(name)`).
#[inline]
fn arg_name<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let buf = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("regexp fn: name arg missing from by-ref lane");
    match buf.iter().position(|&b| b == 0) {
        Some(n) => &buf[..n],
        None => buf,
    }
}

/// `PG_GETARG_INT32(i)`: the low 32 bits of arg `i`'s by-value word.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("regexp fn: missing int4 arg").value.as_i32()
}

/// `PG_GET_COLLATION()`: the collation the function was invoked under.
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

/// Set a `text` result on the by-ref `Varlena` lane. The core's result lives in
/// `m`; copy its bytes out before `m` drops.
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, v: PgVec<'_, u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(v.as_slice().to_vec()));
    Datum::from_usize(0)
}

/// Set `PG_RETURN_NULL()` and return the dummy word.
#[inline]
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result / scratch through
/// `Mcx`. The owning `MemoryContext` is dropped by the caller after the bytes
/// are copied out.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("regexp fmgr scratch")
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
// fc_ adapters — operator family (~, !~, ~*, !~*).
// ---------------------------------------------------------------------------
//
// For the `name`-first operators arg0 is the `name`, arg1 the `text` pattern;
// the cores take both as raw bytes (the engine treats them as database-encoding
// byte strings).

fn fc_nameregexeq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    ret_bool(ok(crate::nameregexeq(m.mcx(), arg_name(fcinfo, 0), arg_text(fcinfo, 1), c)))
}
fn fc_nameregexne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    ret_bool(ok(crate::nameregexne(m.mcx(), arg_name(fcinfo, 0), arg_text(fcinfo, 1), c)))
}
fn fc_nameicregexeq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    ret_bool(ok(crate::nameicregexeq(m.mcx(), arg_name(fcinfo, 0), arg_text(fcinfo, 1), c)))
}
fn fc_nameicregexne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    ret_bool(ok(crate::nameicregexne(m.mcx(), arg_name(fcinfo, 0), arg_text(fcinfo, 1), c)))
}
fn fc_textregexeq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    ret_bool(ok(crate::textregexeq(m.mcx(), arg_text(fcinfo, 0), arg_text(fcinfo, 1), c)))
}
fn fc_textregexne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    ret_bool(ok(crate::textregexne(m.mcx(), arg_text(fcinfo, 0), arg_text(fcinfo, 1), c)))
}
fn fc_texticregexeq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    ret_bool(ok(crate::texticregexeq(m.mcx(), arg_text(fcinfo, 0), arg_text(fcinfo, 1), c)))
}
fn fc_texticregexne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    ret_bool(ok(crate::texticregexne(m.mcx(), arg_text(fcinfo, 0), arg_text(fcinfo, 1), c)))
}

// ---------------------------------------------------------------------------
// fc_ adapters — substring(string from pattern).
// ---------------------------------------------------------------------------

fn fc_textregexsubstr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    let r = ok(crate::textregexsubstr(m.mcx(), arg_text(fcinfo, 0), arg_text(fcinfo, 1), c));
    match r {
        Some(v) => ret_text(fcinfo, v),
        None => ret_null(fcinfo),
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters — SIMILAR TO escape converters.
// ---------------------------------------------------------------------------

fn fc_similar_to_escape_2(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let v = ok(crate::similar_to_escape_2(m.mcx(), arg_text(fcinfo, 0), arg_text(fcinfo, 1)));
    ret_text(fcinfo, v)
}
fn fc_similar_to_escape_1(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let v = ok(crate::similar_to_escape_1(m.mcx(), arg_text(fcinfo, 0)));
    ret_text(fcinfo, v)
}

/// `similar_escape(text, text)` — NON-strict (C: `proisstrict => 'f'`). A NULL
/// pattern returns NULL; a NULL escape selects the default escape character.
fn fc_similar_escape(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    // pattern (arg0): may be NULL → the core returns NULL.
    let pat: Option<&[u8]> = if fcinfo.arg(0).map(|d| d.isnull).unwrap_or(true) {
        None
    } else {
        Some(arg_text(fcinfo, 0))
    };
    // escape (arg1): may be NULL → select the default escape.
    let esc: Option<&[u8]> = if fcinfo.arg(1).map(|d| d.isnull).unwrap_or(true) {
        None
    } else {
        Some(arg_text(fcinfo, 1))
    };
    let r = ok(crate::similar_escape(m.mcx(), pat, esc));
    match r {
        Some(v) => ret_text(fcinfo, v),
        None => ret_null(fcinfo),
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters — regexp_replace family.
// ---------------------------------------------------------------------------

fn fc_textregexreplace_noopt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    let v = ok(crate::textregexreplace_noopt(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        arg_text(fcinfo, 2),
        c,
    ));
    ret_text(fcinfo, v)
}
fn fc_textregexreplace(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    let v = ok(crate::textregexreplace(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        arg_text(fcinfo, 2),
        arg_text(fcinfo, 3),
        c,
    ));
    ret_text(fcinfo, v)
}
fn fc_textregexreplace_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // (string, pattern, replacement, start, N, flags)
    let m = scratch_mcx();
    let c = collation(fcinfo);
    let flags = arg_text(fcinfo, 5);
    let v = ok(crate::textregexreplace_extended(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        arg_text(fcinfo, 2),
        Some(arg_i32(fcinfo, 3)),
        Some(arg_i32(fcinfo, 4)),
        Some(flags),
        c,
    ));
    ret_text(fcinfo, v)
}
fn fc_textregexreplace_extended_no_flags(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // (string, pattern, replacement, start, N)
    let m = scratch_mcx();
    let c = collation(fcinfo);
    let v = ok(crate::textregexreplace_extended_no_flags(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        arg_text(fcinfo, 2),
        arg_i32(fcinfo, 3),
        arg_i32(fcinfo, 4),
        c,
    ));
    ret_text(fcinfo, v)
}
fn fc_textregexreplace_extended_no_n(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // (string, pattern, replacement, start)
    let m = scratch_mcx();
    let c = collation(fcinfo);
    let v = ok(crate::textregexreplace_extended_no_n(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        arg_text(fcinfo, 2),
        arg_i32(fcinfo, 3),
        None,
        c,
    ));
    ret_text(fcinfo, v)
}

// ---------------------------------------------------------------------------
// fc_ adapters — regexp_count family.
// ---------------------------------------------------------------------------

fn fc_regexp_count_no_start(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    ret_i32(ok(crate::regexp_count_no_start(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        None,
        c,
    )))
}
fn fc_regexp_count_no_flags(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    ret_i32(ok(crate::regexp_count_no_flags(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        arg_i32(fcinfo, 2),
        c,
    )))
}
fn fc_regexp_count(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // (string, pattern, start, flags)
    let m = scratch_mcx();
    let c = collation(fcinfo);
    let flags = arg_text(fcinfo, 3);
    ret_i32(ok(crate::regexp_count(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        Some(arg_i32(fcinfo, 2)),
        Some(flags),
        c,
    )))
}

// ---------------------------------------------------------------------------
// fc_ adapters — regexp_instr family.
// ---------------------------------------------------------------------------

fn fc_regexp_instr_no_start(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    ret_i32(ok(crate::regexp_instr_no_start(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        c,
    )))
}
fn fc_regexp_instr_no_n(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    ret_i32(ok(crate::regexp_instr_no_n(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        arg_i32(fcinfo, 2),
        c,
    )))
}
fn fc_regexp_instr_no_endoption(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    ret_i32(ok(crate::regexp_instr_no_endoption(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        arg_i32(fcinfo, 2),
        arg_i32(fcinfo, 3),
        c,
    )))
}
fn fc_regexp_instr_no_flags(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    ret_i32(ok(crate::regexp_instr_no_flags(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        arg_i32(fcinfo, 2),
        arg_i32(fcinfo, 3),
        arg_i32(fcinfo, 4),
        c,
    )))
}
fn fc_regexp_instr_no_subexpr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // (string, pattern, start, N, endoption, flags)
    let m = scratch_mcx();
    let c = collation(fcinfo);
    let flags = arg_text(fcinfo, 5);
    ret_i32(ok(crate::regexp_instr_no_subexpr(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        arg_i32(fcinfo, 2),
        arg_i32(fcinfo, 3),
        arg_i32(fcinfo, 4),
        Some(flags),
        c,
    )))
}
fn fc_regexp_instr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // (string, pattern, start, N, endoption, flags, subexpr)
    let m = scratch_mcx();
    let c = collation(fcinfo);
    let flags = arg_text(fcinfo, 5);
    ret_i32(ok(crate::regexp_instr(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        Some(arg_i32(fcinfo, 2)),
        Some(arg_i32(fcinfo, 3)),
        Some(arg_i32(fcinfo, 4)),
        Some(flags),
        Some(arg_i32(fcinfo, 6)),
        c,
    )))
}

// ---------------------------------------------------------------------------
// fc_ adapters — regexp_like family.
// ---------------------------------------------------------------------------

fn fc_regexp_like_no_flags(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    ret_bool(ok(crate::regexp_like_no_flags(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        c,
    )))
}
fn fc_regexp_like(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // (string, pattern, flags)
    let m = scratch_mcx();
    let c = collation(fcinfo);
    let flags = arg_text(fcinfo, 2);
    ret_bool(ok(crate::regexp_like(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        Some(flags),
        c,
    )))
}

// ---------------------------------------------------------------------------
// fc_ adapters — regexp_substr family.
// ---------------------------------------------------------------------------

fn fc_regexp_substr_no_start(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    let r = ok(crate::regexp_substr_no_start(m.mcx(), arg_text(fcinfo, 0), arg_text(fcinfo, 1), c));
    match r {
        Some(v) => ret_text(fcinfo, v),
        None => ret_null(fcinfo),
    }
}
fn fc_regexp_substr_no_n(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    let r = ok(crate::regexp_substr_no_n(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        arg_i32(fcinfo, 2),
        c,
    ));
    match r {
        Some(v) => ret_text(fcinfo, v),
        None => ret_null(fcinfo),
    }
}
fn fc_regexp_substr_no_flags(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let c = collation(fcinfo);
    let r = ok(crate::regexp_substr_no_flags(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        arg_i32(fcinfo, 2),
        arg_i32(fcinfo, 3),
        c,
    ));
    match r {
        Some(v) => ret_text(fcinfo, v),
        None => ret_null(fcinfo),
    }
}
fn fc_regexp_substr_no_subexpr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // (string, pattern, start, N, flags)
    let m = scratch_mcx();
    let c = collation(fcinfo);
    let flags = arg_text(fcinfo, 4);
    let r = ok(crate::regexp_substr_no_subexpr(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        arg_i32(fcinfo, 2),
        arg_i32(fcinfo, 3),
        Some(flags),
        c,
    ));
    match r {
        Some(v) => ret_text(fcinfo, v),
        None => ret_null(fcinfo),
    }
}
fn fc_regexp_substr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // (string, pattern, start, N, flags, subexpr)
    let m = scratch_mcx();
    let c = collation(fcinfo);
    let flags = arg_text(fcinfo, 4);
    let r = ok(crate::regexp_substr(
        m.mcx(),
        arg_text(fcinfo, 0),
        arg_text(fcinfo, 1),
        Some(arg_i32(fcinfo, 2)),
        Some(arg_i32(fcinfo, 3)),
        Some(flags),
        Some(arg_i32(fcinfo, 5)),
        c,
    ));
    match r {
        Some(v) => ret_text(fcinfo, v),
        None => ret_null(fcinfo),
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
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict,
        retset: false,
        func: Some(func),
    }
}

/// Register every scalar `regexp.c` builtin (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`. OIDs/nargs/strict transcribed
/// exactly from `pg_proc.dat`; none are retset.
pub fn register_regexp_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- operator family (~ / !~ / ~* / !~*) ----
        builtin(79, "nameregexeq", 2, true, fc_nameregexeq),
        builtin(1252, "nameregexne", 2, true, fc_nameregexne),
        builtin(1254, "textregexeq", 2, true, fc_textregexeq),
        builtin(1256, "textregexne", 2, true, fc_textregexne),
        builtin(1238, "texticregexeq", 2, true, fc_texticregexeq),
        builtin(1239, "texticregexne", 2, true, fc_texticregexne),
        builtin(1240, "nameicregexeq", 2, true, fc_nameicregexeq),
        builtin(1241, "nameicregexne", 2, true, fc_nameicregexne),
        // ---- substring(string from pattern) ----
        builtin(2073, "substring", 2, true, fc_textregexsubstr),
        // ---- SIMILAR TO escape converters ----
        builtin(1623, "similar_escape", 2, false, fc_similar_escape),
        builtin(1986, "similar_to_escape", 2, true, fc_similar_to_escape_2),
        builtin(1987, "similar_to_escape", 1, true, fc_similar_to_escape_1),
        // ---- regexp_replace ----
        builtin(2284, "regexp_replace", 3, true, fc_textregexreplace_noopt),
        builtin(2285, "regexp_replace", 4, true, fc_textregexreplace),
        builtin(6251, "regexp_replace", 6, true, fc_textregexreplace_extended),
        builtin(6252, "regexp_replace", 5, true, fc_textregexreplace_extended_no_flags),
        builtin(6253, "regexp_replace", 4, true, fc_textregexreplace_extended_no_n),
        // ---- regexp_count ----
        builtin(6254, "regexp_count", 2, true, fc_regexp_count_no_start),
        builtin(6255, "regexp_count", 3, true, fc_regexp_count_no_flags),
        builtin(6256, "regexp_count", 4, true, fc_regexp_count),
        // ---- regexp_instr ----
        builtin(6257, "regexp_instr", 2, true, fc_regexp_instr_no_start),
        builtin(6258, "regexp_instr", 3, true, fc_regexp_instr_no_n),
        builtin(6259, "regexp_instr", 4, true, fc_regexp_instr_no_endoption),
        builtin(6260, "regexp_instr", 5, true, fc_regexp_instr_no_flags),
        builtin(6261, "regexp_instr", 6, true, fc_regexp_instr_no_subexpr),
        builtin(6262, "regexp_instr", 7, true, fc_regexp_instr),
        // ---- regexp_like ----
        builtin(6263, "regexp_like", 2, true, fc_regexp_like_no_flags),
        builtin(6264, "regexp_like", 3, true, fc_regexp_like),
        // ---- regexp_substr ----
        builtin(6265, "regexp_substr", 2, true, fc_regexp_substr_no_start),
        builtin(6266, "regexp_substr", 3, true, fc_regexp_substr_no_n),
        builtin(6267, "regexp_substr", 4, true, fc_regexp_substr_no_flags),
        builtin(6268, "regexp_substr", 5, true, fc_regexp_substr_no_subexpr),
        builtin(6269, "regexp_substr", 6, true, fc_regexp_substr),
    ]);
}
