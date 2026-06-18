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
    ret_cstring(fcinfo, cstring_lane(&out))
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
    ret_cstring(fcinfo, cstring_lane(&out))
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
// fc_ adapters — additional text/bytea by-ref builtins (the broadest type-area
// fan-out leg of the fmgr by-ref builtin-registration lever). Same header
// convention as the rest of this file: a `text`/`bytea` arg arrives
// header-stripped on the by-ref lane (`arg_bytes`); `text`/`bytea` results cross
// header-stripped (`ret_varlena`). `unknown` is a cstring-representation type
// (typlen -2), so it crosses on the cstring lane (`arg_cstring`/`ret_cstring`).
// OIDs/nargs/strict/retset transcribed exactly from `pg_proc.dat`.
// ---------------------------------------------------------------------------

// --- bytea get/set byte/bit (bytea.rs: byteaGetByte/Bit, byteaSetByte/Bit) ---

fn fc_byteaGetByte(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let n = arg_i32(fcinfo, 1);
    ret_i32(ok(crate::bytea::bytea_get_byte(arg_bytes(fcinfo, 0), n)))
}
fn fc_byteaGetBit(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let n = arg_i64(fcinfo, 1);
    ret_i32(ok(crate::bytea::bytea_get_bit(arg_bytes(fcinfo, 0), n)))
}
fn fc_byteaSetByte(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let n = arg_i32(fcinfo, 1);
    let newbyte = arg_i32(fcinfo, 2);
    let out = ok(crate::bytea::bytea_set_byte(m.mcx(), arg_bytes(fcinfo, 0), n, newbyte)).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_byteaSetBit(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let n = arg_i64(fcinfo, 1);
    let newbit = arg_i32(fcinfo, 2);
    let out = ok(crate::bytea::bytea_set_bit(m.mcx(), arg_bytes(fcinfo, 0), n, newbit)).to_vec();
    ret_varlena(fcinfo, out)
}

// --- text substring / overlay (position_ops.rs: text_substring/text_overlay) ---

fn fc_text_substr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: text_substr -> text_substring(str, start, length, false).
    let m = scratch_mcx();
    let start = arg_i32(fcinfo, 1);
    let length = arg_i32(fcinfo, 2);
    let out = ok(crate::position_ops::text_substring(m.mcx(), arg_bytes(fcinfo, 0), start, length, false)).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_text_substr_no_len(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: text_substr_no_len -> text_substring(str, start, -1, true).
    let m = scratch_mcx();
    let start = arg_i32(fcinfo, 1);
    let out = ok(crate::position_ops::text_substring(m.mcx(), arg_bytes(fcinfo, 0), start, -1, true)).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_textoverlay(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: textoverlay -> text_overlay(t1, t2, sp, sl).
    let m = scratch_mcx();
    let sp = arg_i32(fcinfo, 2);
    let sl = arg_i32(fcinfo, 3);
    let out = ok(crate::position_ops::text_overlay(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), sp, sl)).to_vec();
    ret_varlena(fcinfo, out)
}
fn fc_textoverlay_no_len(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: textoverlay_no_len -> text_overlay computes sl = textlen(t2) internally.
    // The value core `text_overlay` requires the explicit `sl`; C's no-len
    // variant passes `text_length(PG_GETARG_DATUM(1))`. Compute the replacement
    // string's character length (textlen core) and forward.
    let m = scratch_mcx();
    let t2 = arg_bytes(fcinfo, 1);
    let sl = ok(crate::wire_io::textlen(t2));
    let sp = arg_i32(fcinfo, 2);
    let out = ok(crate::position_ops::text_overlay(m.mcx(), arg_bytes(fcinfo, 0), t2, sp, sl)).to_vec();
    ret_varlena(fcinfo, out)
}

// --- unknown I/O (wire_io.rs: unknownin/unknownout). `unknown` is a
// cstring-representation type (typlen -2): both the arg and the result are the
// raw cstring bytes on the by-ref cstring lane. ---

fn fc_unknownin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: unknownin(cstring) -> unknown == pstrdup(str). `unknown` crosses on the
    // cstring lane (typlen -2), so the result is the raw bytes as a cstring.
    let m = scratch_mcx();
    let out = ok(crate::wire_io::unknownin(m.mcx(), arg_cstring(fcinfo, 0).as_bytes())).to_vec();
    ret_cstring(fcinfo, cstring_lane(&out))
}
fn fc_unknownout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: unknownout(unknown) -> cstring == pstrdup(str). The `unknown` arg is a
    // cstring on the by-ref lane.
    let m = scratch_mcx();
    let out = ok(crate::wire_io::unknownout(m.mcx(), arg_cstring(fcinfo, 0).as_bytes())).to_vec();
    ret_cstring(fcinfo, cstring_lane(&out))
}

/// The `unknown`/`cstring` cores `pstrdup` their argument, so the returned bytes
/// carry a trailing C NUL. The by-ref cstring lane carries the logical string
/// (no embedded NUL), so drop one trailing NUL if present.
fn cstring_lane(bytes: &[u8]) -> String {
    let body = match bytes.last() {
        Some(0) => &bytes[..bytes.len() - 1],
        _ => bytes,
    };
    String::from_utf8_lossy(body).into_owned()
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
        builtin(406, "name_text", 1, fc_name_text),
        builtin(407, "text_name", 1, fc_text_name),
        // ---- varchar <-> name casts (share the name<->text cores: varchar is
        // varlena-bodied like text, so it crosses header-stripped on the by-ref
        // lane exactly like a `text` arg/result) ----
        // 1400 `name`(varchar)->name, prosrc text_name (varchar -> name buffer).
        builtin(1400, "text_name", 1, fc_text_name),
        // 1401 `varchar`(name)->varchar, prosrc name_text (name buffer -> varlena).
        builtin(1401, "name_text", 1, fc_name_text),
        // ---- length / octet-length / concat ----
        builtin(1257, "textlen", 1, fc_textlen),
        builtin(1317, "textlen", 1, fc_textlen),
        builtin(1369, "textlen", 1, fc_textlen),
        builtin(1381, "textlen", 1, fc_textlen),
        builtin(1374, "textoctetlen", 1, fc_textoctetlen),
        builtin(720, "byteaoctetlen", 1, fc_byteaoctetlen),
        builtin(2010, "byteaoctetlen", 1, fc_byteaoctetlen),
        builtin(1258, "textcat", 2, fc_textcat),
        builtin(2011, "byteacat", 2, fc_byteacat),
        // ---- text/bytea larger/smaller ----
        builtin(458, "text_larger", 2, fc_text_larger),
        builtin(459, "text_smaller", 2, fc_text_smaller),
        builtin(6393, "bytea_larger", 2, fc_bytea_larger),
        builtin(6394, "bytea_smaller", 2, fc_bytea_smaller),
        // ---- substring / position / overlay ----
        builtin(849, "textpos", 2, fc_textpos),
        builtin(868, "textpos", 2, fc_textpos),
        builtin(2014, "byteapos", 2, fc_byteapos),
        builtin(2012, "bytea_substr", 3, fc_bytea_substr),
        builtin(2085, "bytea_substr", 3, fc_bytea_substr),
        builtin(2013, "bytea_substr_no_len", 2, fc_bytea_substr_no_len),
        builtin(2086, "bytea_substr_no_len", 2, fc_bytea_substr_no_len),
        builtin(749, "byteaoverlay", 4, fc_byteaoverlay),
        builtin(752, "byteaoverlay_no_len", 3, fc_byteaoverlay_no_len),
        // ---- left / right / reverse ----
        builtin(3060, "text_left", 2, fc_text_left),
        builtin(3061, "text_right", 2, fc_text_right),
        builtin(3062, "text_reverse", 1, fc_text_reverse),
        builtin(6382, "bytea_reverse", 1, fc_bytea_reverse),
        // ---- replace / split_part ----
        builtin(2087, "replace_text", 3, fc_replace_text),
        builtin(2088, "split_part", 3, fc_split_part),
        // ---- bytea comparison + cmp + bit_count ----
        builtin(1948, "byteaeq", 2, fc_byteaeq),
        builtin(1953, "byteane", 2, fc_byteane),
        builtin(1949, "bytealt", 2, fc_bytealt),
        builtin(1950, "byteale", 2, fc_byteale),
        builtin(1951, "byteagt", 2, fc_byteagt),
        builtin(1952, "byteage", 2, fc_byteage),
        builtin(1954, "byteacmp", 2, fc_byteacmp),
        builtin(6163, "bytea_bit_count", 1, fc_bytea_bit_count),
        // ---- bytea <-> int casts ----
        builtin(6370, "bytea_int2", 1, fc_bytea_int2),
        builtin(6371, "bytea_int4", 1, fc_bytea_int4),
        builtin(6372, "bytea_int8", 1, fc_bytea_int8),
        builtin(6367, "int2_bytea", 1, fc_int2_bytea),
        builtin(6368, "int4_bytea", 1, fc_int4_bytea),
        builtin(6369, "int8_bytea", 1, fc_int8_bytea),
        // ---- text_pattern_ops ----
        builtin(2160, "text_pattern_lt", 2, fc_text_pattern_lt),
        builtin(2161, "text_pattern_le", 2, fc_text_pattern_le),
        builtin(2163, "text_pattern_ge", 2, fc_text_pattern_ge),
        builtin(2164, "text_pattern_gt", 2, fc_text_pattern_gt),
        builtin(2166, "bttext_pattern_cmp", 2, fc_bttext_pattern_cmp),
        // ---- base conversions ----
        builtin(2089, "to_hex32", 1, fc_to_hex32),
        builtin(2090, "to_hex64", 1, fc_to_hex64),
        builtin(6330, "to_bin32", 1, fc_to_bin32),
        builtin(6331, "to_bin64", 1, fc_to_bin64),
        builtin(6332, "to_oct32", 1, fc_to_oct32),
        builtin(6333, "to_oct64", 1, fc_to_oct64),
        // ---- unicode / unistr ----
        builtin(4549, "unicode_version", 0, fc_unicode_version),
        builtin(6099, "icu_unicode_version", 0, fc_icu_unicode_version),
        builtin(6105, "unicode_assigned", 1, fc_unicode_assigned),
        builtin(4350, "unicode_normalize_func", 2, fc_unicode_normalize_func),
        builtin(4351, "unicode_is_normalized", 2, fc_unicode_is_normalized),
        builtin(6198, "unistr", 1, fc_unistr),
    ]);
}

/// Register the additional `text`/`bytea`/`unknown` by-reference builtins whose
/// value cores are ported and expressible at the fmgr boundary but were not yet
/// in the fmgr fast-path table: `bytea` get/set byte/bit, `text` `substring`
/// (with/without length), `text` `overlay` (with/without length), and the
/// `unknown` I/O pair. Called from this crate's `init_seams()`. OIDs / nargs /
/// strict / retset transcribed exactly from `pg_proc.dat`; every row here is
/// `proisstrict => 't'` and not `proretset`.
pub fn register_varlena_text_bytea_byref_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- bytea get/set byte/bit (bytea.rs) ----
        // builtin `name` is the `prosrc` C symbol (canonical fmgr_builtins[]
        // keys on prosrc, not the SQL proname).
        builtin(721, "byteaGetByte", 2, fc_byteaGetByte),
        builtin(723, "byteaGetBit", 2, fc_byteaGetBit),
        builtin(722, "byteaSetByte", 3, fc_byteaSetByte),
        builtin(724, "byteaSetBit", 3, fc_byteaSetBit),
        // ---- text substring (position_ops.rs) ----
        builtin(936, "text_substr", 3, fc_text_substr),
        builtin(937, "text_substr_no_len", 2, fc_text_substr_no_len),
        // ---- text overlay (position_ops.rs) ----
        builtin(1404, "textoverlay", 4, fc_textoverlay),
        builtin(1405, "textoverlay_no_len", 3, fc_textoverlay_no_len),
        // ---- unknown I/O (wire_io.rs) ----
        builtin(109, "unknownin", 1, fc_unknownin),
        builtin(110, "unknownout", 1, fc_unknownout),
    ]);
}

// ---------------------------------------------------------------------------
// End-to-end proof: invoke the newly-registered text/bytea/unknown by-ref
// builtins BY OID through the fmgr registry (`fmgr_isbuiltin(oid).func`),
// passing args on `fcinfo.ref_args` and reading the result off
// `fcinfo.take_ref_result()` / the returned by-value word — the canonical
// numeric test pattern from the fmgr by-ref recipe.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use types_datum::NullableDatum;

    fn register() {
        // The fmgr builtin table is thread-local in the test harness, so each
        // helper re-registers (mirroring the numeric test pattern's per-call
        // `register_numeric_builtins()`).
        register_varlena_text_bytea_byref_builtins();
        // text_substring/text_overlay consult the database encoding's max bytes
        // per char; under the test (SQL_ASCII-equivalent) it is 1. The mbutils
        // seam OnceLock panics on a second install, so guard it.
        if !backend_utils_mb_mbutils_seams::pg_database_encoding_max_length::is_installed() {
            backend_utils_mb_mbutils_seams::pg_database_encoding_max_length::set(|| 1);
        }
    }

    /// Call a registered by-ref builtin by OID: `n` by-ref `Varlena` args (raw
    /// header-stripped payloads, the form the cores consume) plus optional
    /// trailing by-value int args. Returns the produced `Varlena` payload.
    fn call_varlena_result(
        oid: u32,
        ref_args: &[&[u8]],
        val_args: &[Datum],
    ) -> Vec<u8> {
        register();
        let nargs = (ref_args.len() + val_args.len()) as i16;
        let mut fcinfo = FunctionCallInfoBaseData::new(None, nargs, 0, None, None);
        let mut args: Vec<NullableDatum> = Vec::new();
        let mut refs: Vec<Option<RefPayload>> = Vec::new();
        for b in ref_args {
            args.push(NullableDatum::value(Datum::null()));
            refs.push(Some(RefPayload::Varlena(b.to_vec())));
        }
        for v in val_args {
            args.push(NullableDatum::value(*v));
            refs.push(None);
        }
        fcinfo.args = args;
        fcinfo.ref_args = refs;
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("builtin registered");
        (entry.func.unwrap())(&mut fcinfo);
        match fcinfo.take_ref_result().expect("by-ref result produced") {
            RefPayload::Varlena(b) => b,
            other => panic!("unexpected result lane {other:?}"),
        }
    }

    /// Call a registered builtin returning a by-value int (get_byte/get_bit).
    fn call_int_result(oid: u32, ref_args: &[&[u8]], val_args: &[Datum]) -> i32 {
        register();
        let nargs = (ref_args.len() + val_args.len()) as i16;
        let mut fcinfo = FunctionCallInfoBaseData::new(None, nargs, 0, None, None);
        let mut args: Vec<NullableDatum> = Vec::new();
        let mut refs: Vec<Option<RefPayload>> = Vec::new();
        for b in ref_args {
            args.push(NullableDatum::value(Datum::null()));
            refs.push(Some(RefPayload::Varlena(b.to_vec())));
        }
        for v in val_args {
            args.push(NullableDatum::value(*v));
            refs.push(None);
        }
        fcinfo.args = args;
        fcinfo.ref_args = refs;
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("builtin registered");
        let d = (entry.func.unwrap())(&mut fcinfo);
        d.as_i32()
    }

    /// `get_byte('\x010203', 1) == 2` through the registry (oid 721).
    #[test]
    fn byref_byteaGetByte_through_registry() {
        let v = [0x01u8, 0x02, 0x03];
        assert_eq!(call_int_result(721, &[&v], &[Datum::from_i32(1)]), 2);
    }

    /// `get_bit('\x80', 7) == 1` (MSB of 0x80, bit index 7) through oid 723.
    #[test]
    fn byref_byteaGetBit_through_registry() {
        let v = [0x80u8];
        assert_eq!(call_int_result(723, &[&v], &[Datum::from_i64(7)]), 1);
    }

    /// `set_byte('\x010203', 1, 0xff) == '\x01ff03'` through oid 722, then read
    /// the changed byte back via get_byte — a real in->op->out round-trip.
    #[test]
    fn byref_byteaSetByte_through_registry() {
        let v = [0x01u8, 0x02, 0x03];
        let out = call_varlena_result(722, &[&v], &[Datum::from_i32(1), Datum::from_i32(0xff)]);
        assert_eq!(out, vec![0x01, 0xff, 0x03]);
        assert_eq!(call_int_result(721, &[&out], &[Datum::from_i32(1)]), 0xff);
    }

    /// `set_bit('\x00', 0, 1) == '\x01'` through oid 724 (bit 0 is the LSB:
    /// C uses `byte | (1 << (n % 8))`), then read it back via get_bit.
    #[test]
    fn byref_byteaSetBit_through_registry() {
        let v = [0x00u8];
        let out = call_varlena_result(724, &[&v], &[Datum::from_i64(0), Datum::from_i32(1)]);
        assert_eq!(out, vec![0x01]);
        assert_eq!(call_int_result(723, &[&out], &[Datum::from_i64(0)]), 1);
    }

    /// `substring('hello', 2, 3) == 'ell'` through oid 936 (text_substr).
    #[test]
    fn byref_text_substring_through_registry() {
        let out = call_varlena_result(
            936,
            &[b"hello"],
            &[Datum::from_i32(2), Datum::from_i32(3)],
        );
        assert_eq!(out, b"ell".to_vec());
    }

    /// `substring('hello', 3) == 'llo'` through oid 937 (text_substr_no_len).
    #[test]
    fn byref_text_substring_no_len_through_registry() {
        let out = call_varlena_result(937, &[b"hello"], &[Datum::from_i32(3)]);
        assert_eq!(out, b"llo".to_vec());
    }

    /// `overlay('Txxxxas' placing 'hom' from 2 for 4) == 'Thomas'` (the SQL
    /// docs' canonical example) through oid 1404 (textoverlay).
    #[test]
    fn byref_textoverlay_through_registry() {
        let out = call_varlena_result(
            1404,
            &[b"Txxxxas", b"hom"],
            &[Datum::from_i32(2), Datum::from_i32(4)],
        );
        assert_eq!(out, b"Thomas".to_vec());
    }

    /// `overlay('Txxxxas' placing 'hom' from 2) == 'Thomxas'` (no-len defaults
    /// `for` to length('hom') = 3, so only 3 of the x's are replaced) through
    /// oid 1405 (textoverlay_no_len).
    #[test]
    fn byref_textoverlay_no_len_through_registry() {
        let out = call_varlena_result(1405, &[b"Txxxxas", b"hom"], &[Datum::from_i32(2)]);
        assert_eq!(out, b"Thomxas".to_vec());
    }

    /// `unknownout(unknownin('abc')) == 'abc'` through oids 109/110, with the
    /// `unknown` value crossing on the cstring lane.
    #[test]
    fn byref_unknown_io_round_trip_through_registry() {
        register();
        // unknownin (109): cstring -> unknown.
        let mut fc = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fc.args = vec![NullableDatum::value(Datum::null())];
        fc.ref_args = vec![Some(RefPayload::Cstring("abc".to_string()))];
        let e_in = backend_utils_fmgr_core::fmgr_isbuiltin(109).expect("unknownin registered");
        (e_in.func.unwrap())(&mut fc);
        let mid = match fc.take_ref_result().expect("unknownin result") {
            RefPayload::Cstring(s) => s,
            other => panic!("unexpected lane {other:?}"),
        };
        assert_eq!(mid, "abc");
        // unknownout (110): unknown -> cstring.
        let mut fc2 = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fc2.args = vec![NullableDatum::value(Datum::null())];
        fc2.ref_args = vec![Some(RefPayload::Cstring(mid))];
        let e_out = backend_utils_fmgr_core::fmgr_isbuiltin(110).expect("unknownout registered");
        (e_out.func.unwrap())(&mut fc2);
        let out = match fc2.take_ref_result().expect("unknownout result") {
            RefPayload::Cstring(s) => s,
            other => panic!("unexpected lane {other:?}"),
        };
        assert_eq!(out, "abc");
    }
}
