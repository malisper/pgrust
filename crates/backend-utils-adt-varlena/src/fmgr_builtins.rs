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
use types_fmgr::boundary::RefPayload;
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
#[inline]
fn ret_i16(v: i16) -> Datum {
    Datum::from_i16(v)
}
#[inline]
fn ret_i64(v: i64) -> Datum {
    Datum::from_i64(v)
}

/// `PG_GETARG_INT16(i)`: the low 16 bits of arg `i`'s word.
#[inline]
fn arg_i16(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i16 {
    fcinfo.arg(i).expect("varlena fn: missing arg").value.as_i16()
}
/// `PG_GETARG_INT32(i)`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("varlena fn: missing arg").value.as_i32()
}
/// `PG_GETARG_INT64(i)`: arg `i`'s full word as a signed 64-bit int.
#[inline]
fn arg_i64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("varlena fn: missing arg").value.as_i64()
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("varlena fn: cstring arg missing from by-ref lane")
}

/// Set a `text`/`bytea`/`name` (by-reference) result on the by-ref lane. The
/// fmgr boundary carries `text`/`bytea`/`name` results header-stripped (the
/// payload bytes), symmetric with how the arg lane delivers them (the value
/// cores here build/consume the header-less payload); `_send` results carry the
/// wire bytes. Returns the dummy by-value word.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}
/// Set a `cstring` (`_out`) result on the by-ref lane and return the dummy word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`. The
/// resulting bytes are copied onto the by-ref lane before it is dropped (C: the
/// palloc'd result lives in the caller's context; here it crosses by value).
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("varlena fmgr scratch")
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
// fc_ adapters — text/bytea I/O, length, concat, substring/position/overlay,
// pattern ops, casts, base conversions, unicode. A `text`/`bytea` arg arrives
// header-stripped on the by-ref lane (`arg_bytes`); a `cstring` arg via
// `arg_cstring`; scalar ints by value. `text`/`bytea` results cross
// header-stripped (`ret_varlena`); `_out` results as `cstring` (`ret_cstring`).
// Cores that allocate take a `scratch_mcx` whose bytes are copied out before it
// drops. Collation-sensitive ops read `PG_GET_COLLATION()` off the frame.
// ---------------------------------------------------------------------------

// --- text/bytea wire I/O (wire_io.rs / bytea.rs) ---

fn fc_textin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0).as_bytes().to_vec();
    let m = scratch_mcx();
    let out = ok(crate::wire_io::textin(m.mcx(), &s)).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_textout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::wire_io::textout(m.mcx(), arg_bytes(fcinfo, 0)));
    ret_cstring(fcinfo, String::from_utf8_lossy(&out).into_owned())
}
fn fc_textsend(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let bytes = ok(crate::wire_io::textsend(m.mcx(), arg_bytes(fcinfo, 0))).as_bytes().to_vec();
    ret_varlena(fcinfo, bytes)
}
fn fc_byteain(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0).as_bytes().to_vec();
    let m = scratch_mcx();
    let out = ok(crate::bytea::byteain(m.mcx(), &s)).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_byteaout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::bytea::byteaout(m.mcx(), arg_bytes(fcinfo, 0)));
    ret_cstring(fcinfo, String::from_utf8_lossy(&out).into_owned())
}
fn fc_byteasend(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let bytes = ok(crate::bytea::byteasend(m.mcx(), arg_bytes(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, bytes)
}

// --- name <-> text casts (wire_io.rs) ---

fn fc_name_text(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: name -> text (proname `text`, prosrc name_text). arg0 is a `name`.
    let m = scratch_mcx();
    let out = ok(crate::wire_io::name_text(m.mcx(), arg_bytes(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_text_name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: text -> name (proname `name`, prosrc text_name). Result is the fixed
    // NAMEDATALEN buffer, crossed as its raw bytes on the by-ref lane.
    let nd = ok(crate::wire_io::text_name(arg_bytes(fcinfo, 0)));
    ret_varlena(fcinfo, nd.to_vec())
}

// --- length / octet-length / concat (wire_io.rs / bytea.rs) ---

fn fc_textlen(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(ok(crate::wire_io::textlen(arg_bytes(fcinfo, 0))))
}
fn fc_textoctetlen(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(ok(crate::wire_io::textoctetlen(arg_bytes(fcinfo, 0))))
}
fn fc_byteaoctetlen(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(ok(crate::bytea::byteaoctetlen(arg_bytes(fcinfo, 0))))
}
fn fc_textcat(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::wire_io::textcat(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_byteacat(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::bytea::byteacat(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))).to_vec();
    ret_varlena(fcinfo, out)
}

// --- text larger/smaller + bytea larger/smaller (comparison.rs / bytea.rs) ---

fn fc_text_larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    let out = ok(crate::comparison::text_larger(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_text_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    let out = ok(crate::comparison::text_smaller(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)).to_vec();
    ret_varlena(fcinfo, out)
}

// --- substring / position / overlay (position_ops.rs / bytea.rs) ---

fn fc_textpos(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    ret_i32(ok(crate::position_ops::textpos(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c)))
}
fn fc_byteapos(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(ok(crate::bytea::byteapos(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))))
}
fn fc_bytea_substr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let s = arg_i32(fcinfo, 1);
    let l = arg_i32(fcinfo, 2);
    let out = ok(crate::bytea::bytea_substr(m.mcx(), arg_bytes(fcinfo, 0), s, l)).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_bytea_substr_no_len(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let s = arg_i32(fcinfo, 1);
    let out = ok(crate::bytea::bytea_substr_no_len(m.mcx(), arg_bytes(fcinfo, 0), s)).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_byteaoverlay(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let sp = arg_i32(fcinfo, 2);
    let sl = arg_i32(fcinfo, 3);
    let out = ok(crate::bytea::byteaoverlay(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), sp, sl)).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_byteaoverlay_no_len(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let sp = arg_i32(fcinfo, 2);
    let out = ok(crate::bytea::byteaoverlay_no_len(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), sp)).to_vec();
    ret_varlena(fcinfo, out)
}

// --- left / right / reverse (position_ops.rs / bytea.rs) ---

fn fc_text_left(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let n = arg_i32(fcinfo, 1);
    let out = ok(crate::position_ops::text_left(m.mcx(), arg_bytes(fcinfo, 0), n)).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_text_right(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let n = arg_i32(fcinfo, 1);
    let out = ok(crate::position_ops::text_right(m.mcx(), arg_bytes(fcinfo, 0), n)).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_text_reverse(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::position_ops::text_reverse(m.mcx(), arg_bytes(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_bytea_reverse(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::bytea::bytea_reverse(m.mcx(), arg_bytes(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, out)
}

// --- replace / split_part (position_ops.rs / split_format.rs) ---

fn fc_replace_text(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    let out = ok(crate::position_ops::replace_text(
        m.mcx(),
        arg_bytes(fcinfo, 0),
        arg_bytes(fcinfo, 1),
        arg_bytes(fcinfo, 2),
        c,
    ))
    .to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_split_part(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    let fldnum = arg_i32(fcinfo, 2);
    let out = ok(crate::split_format::split_part(
        m.mcx(),
        arg_bytes(fcinfo, 0),
        arg_bytes(fcinfo, 1),
        fldnum,
        c,
    ))
    .to_vec();
    ret_varlena(fcinfo, out)
}

// --- bytea comparison operators + cmp (bytea.rs) ---

fn fc_byteaeq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(ok(crate::bytea::byteaeq(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))))
}
fn fc_byteane(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(ok(crate::bytea::byteane(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))))
}
fn fc_bytealt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(ok(crate::bytea::bytealt(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))))
}
fn fc_byteale(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(ok(crate::bytea::byteale(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))))
}
fn fc_byteagt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(ok(crate::bytea::byteagt(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))))
}
fn fc_byteage(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(ok(crate::bytea::byteage(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))))
}
fn fc_byteacmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(ok(crate::bytea::byteacmp(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))))
}
fn fc_bytea_larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::bytea::bytea_larger(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_bytea_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::bytea::bytea_smaller(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_bytea_bit_count(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i64(ok(crate::bytea::bytea_bit_count(arg_bytes(fcinfo, 0))))
}

// --- bytea <-> int casts (bytea.rs) ---

fn fc_bytea_int2(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i16(ok(crate::bytea::bytea_int2(arg_bytes(fcinfo, 0))))
}
fn fc_bytea_int4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(ok(crate::bytea::bytea_int4(arg_bytes(fcinfo, 0))))
}
fn fc_bytea_int8(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i64(ok(crate::bytea::bytea_int8(arg_bytes(fcinfo, 0))))
}
fn fc_int2_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::bytea::int2_bytea(m.mcx(), arg_i16(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_int4_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::bytea::int4_bytea(m.mcx(), arg_i32(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_int8_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::bytea::int8_bytea(m.mcx(), arg_i64(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, out)
}

// --- text_pattern_ops (name_pattern.rs) ---

fn fc_text_pattern_lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(ok(crate::name_pattern::text_pattern_lt(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))))
}
fn fc_text_pattern_le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(ok(crate::name_pattern::text_pattern_le(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))))
}
fn fc_text_pattern_ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(ok(crate::name_pattern::text_pattern_ge(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))))
}
fn fc_text_pattern_gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(ok(crate::name_pattern::text_pattern_gt(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))))
}
fn fc_bttext_pattern_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(ok(crate::name_pattern::bttext_pattern_cmp(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))))
}

// --- base conversions / to_hex / to_bin / to_oct (misc_encoding.rs) ---

fn fc_to_hex32(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::misc_encoding::to_hex32(m.mcx(), arg_i32(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_to_hex64(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::misc_encoding::to_hex64(m.mcx(), arg_i64(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_to_bin32(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::misc_encoding::to_bin32(m.mcx(), arg_i32(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_to_bin64(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::misc_encoding::to_bin64(m.mcx(), arg_i64(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_to_oct32(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::misc_encoding::to_oct32(m.mcx(), arg_i32(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_to_oct64(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::misc_encoding::to_oct64(m.mcx(), arg_i64(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, out)
}

// --- unicode / unistr (misc_encoding.rs) ---

fn fc_unicode_version(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::misc_encoding::unicode_version(m.mcx())).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_icu_unicode_version(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: returns the ICU collator's Unicode version; this build has no ICU
    // (the value core returns `None`), so the result is NULL.
    let m = scratch_mcx();
    let out = ok(crate::misc_encoding::icu_unicode_version(m.mcx())).map(|b| b.to_vec());
    match out {
        Some(b) => ret_varlena(fcinfo, b),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}
fn fc_unicode_assigned(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(ok(crate::misc_encoding::unicode_assigned(arg_bytes(fcinfo, 0))))
}
fn fc_unicode_normalize_func(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::misc_encoding::unicode_normalize_func(
        m.mcx(),
        arg_bytes(fcinfo, 0),
        arg_bytes(fcinfo, 1),
    ))
    .to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_unicode_is_normalized(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ret_bool(ok(crate::misc_encoding::unicode_is_normalized(
        m.mcx(),
        arg_bytes(fcinfo, 0),
        arg_bytes(fcinfo, 1),
    )))
}
fn fc_unistr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::misc_encoding::unistr(m.mcx(), arg_bytes(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, out)
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

/// Register the rest of `varlena.c`'s `fmgr_builtins[]` rows whose value cores
/// are ported and whose arg/result types are expressible at the fmgr boundary:
/// `text`/`bytea` I/O, length/concat, substring/position/overlay/left/right/
/// reverse, replace/split_part, `bytea` comparison + int casts, the
/// `text_pattern_ops` family, base conversions, and unicode/unistr. Called from
/// this crate's `init_seams()`. OIDs/nargs from `pg_proc.dat`; every row here is
/// `proisstrict => 't'` and not retset.
///
/// NOT registered (genuinely inexpressible / not faithfully mappable):
/// * `format` (3540) — VARIADIC `"any"` + `proisstrict => 'f'`; the format-arg
///   array (per-arg Datums with their types) is not carried at this fmgr
///   boundary (`text_format`/`text_format_nv` need the typed variadic args).
pub fn register_varlena_more_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- text/bytea wire I/O ----
        builtin(46, "textin", 1, fc_textin),
        builtin(47, "textout", 1, fc_textout),
        builtin(2415, "textsend", 1, fc_textsend),
        builtin(1244, "byteain", 1, fc_byteain),
        builtin(31, "byteaout", 1, fc_byteaout),
        builtin(2413, "byteasend", 1, fc_byteasend),
        // ---- name <-> text casts ----
        builtin(406, "text", 1, fc_name_text),
        builtin(407, "name", 1, fc_text_name),
        // ---- length / octet-length / concat ----
        builtin(1257, "textlen", 1, fc_textlen),
        builtin(1317, "length", 1, fc_textlen),
        builtin(1369, "character_length", 1, fc_textlen),
        builtin(1381, "char_length", 1, fc_textlen),
        builtin(1374, "octet_length", 1, fc_textoctetlen),
        builtin(720, "octet_length", 1, fc_byteaoctetlen),
        builtin(2010, "length", 1, fc_byteaoctetlen),
        builtin(1258, "textcat", 2, fc_textcat),
        builtin(2011, "byteacat", 2, fc_byteacat),
        // ---- text/bytea larger/smaller ----
        builtin(458, "text_larger", 2, fc_text_larger),
        builtin(459, "text_smaller", 2, fc_text_smaller),
        builtin(6393, "bytea_larger", 2, fc_bytea_larger),
        builtin(6394, "bytea_smaller", 2, fc_bytea_smaller),
        // ---- substring / position / overlay ----
        builtin(849, "position", 2, fc_textpos),
        builtin(868, "strpos", 2, fc_textpos),
        builtin(2014, "position", 2, fc_byteapos),
        builtin(2012, "substring", 3, fc_bytea_substr),
        builtin(2085, "substr", 3, fc_bytea_substr),
        builtin(2013, "substring", 2, fc_bytea_substr_no_len),
        builtin(2086, "substr", 2, fc_bytea_substr_no_len),
        builtin(749, "overlay", 4, fc_byteaoverlay),
        builtin(752, "overlay", 3, fc_byteaoverlay_no_len),
        // ---- left / right / reverse ----
        builtin(3060, "left", 2, fc_text_left),
        builtin(3061, "right", 2, fc_text_right),
        builtin(3062, "reverse", 1, fc_text_reverse),
        builtin(6382, "reverse", 1, fc_bytea_reverse),
        // ---- replace / split_part ----
        builtin(2087, "replace", 3, fc_replace_text),
        builtin(2088, "split_part", 3, fc_split_part),
        // ---- bytea comparison + cmp + bit_count ----
        builtin(1948, "byteaeq", 2, fc_byteaeq),
        builtin(1953, "byteane", 2, fc_byteane),
        builtin(1949, "bytealt", 2, fc_bytealt),
        builtin(1950, "byteale", 2, fc_byteale),
        builtin(1951, "byteagt", 2, fc_byteagt),
        builtin(1952, "byteage", 2, fc_byteage),
        builtin(1954, "byteacmp", 2, fc_byteacmp),
        builtin(6163, "bit_count", 1, fc_bytea_bit_count),
        // ---- bytea <-> int casts ----
        builtin(6370, "int2", 1, fc_bytea_int2),
        builtin(6371, "int4", 1, fc_bytea_int4),
        builtin(6372, "int8", 1, fc_bytea_int8),
        builtin(6367, "bytea", 1, fc_int2_bytea),
        builtin(6368, "bytea", 1, fc_int4_bytea),
        builtin(6369, "bytea", 1, fc_int8_bytea),
        // ---- text_pattern_ops ----
        builtin(2160, "text_pattern_lt", 2, fc_text_pattern_lt),
        builtin(2161, "text_pattern_le", 2, fc_text_pattern_le),
        builtin(2163, "text_pattern_ge", 2, fc_text_pattern_ge),
        builtin(2164, "text_pattern_gt", 2, fc_text_pattern_gt),
        builtin(2166, "bttext_pattern_cmp", 2, fc_bttext_pattern_cmp),
        // ---- base conversions ----
        builtin(2089, "to_hex", 1, fc_to_hex32),
        builtin(2090, "to_hex", 1, fc_to_hex64),
        builtin(6330, "to_bin", 1, fc_to_bin32),
        builtin(6331, "to_bin", 1, fc_to_bin64),
        builtin(6332, "to_oct", 1, fc_to_oct32),
        builtin(6333, "to_oct", 1, fc_to_oct64),
        // ---- unicode / unistr ----
        builtin(4549, "unicode_version", 0, fc_unicode_version),
        builtin(6099, "icu_unicode_version", 0, fc_icu_unicode_version),
        builtin(6105, "unicode_assigned", 1, fc_unicode_assigned),
        builtin(4350, "normalize", 2, fc_unicode_normalize_func),
        builtin(4351, "is_normalized", 2, fc_unicode_is_normalized),
        builtin(6198, "unistr", 1, fc_unistr),
    ]);
}
