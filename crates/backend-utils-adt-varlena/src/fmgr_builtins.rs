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
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `VARHDRSZ` — the 4-byte uncompressed varlena length word.
const VARHDRSZ: usize = 4;

/// `PG_GETARG_*_PP` per-arg detoast: rewrite every by-reference varlena arg
/// that is stored TOAST-external (`VARATT_IS_EXTERNAL`) or in-line compressed
/// (`VARATT_IS_COMPRESSED`) into its fully-detoasted image, in place. C does
/// this lazily in each `PG_GETARG_TEXT_PP(n)`; the owned model performs the
/// equivalent sweep at wrapper entry so the cores read a plain varlena via
/// [`crate::vardata_any_slice`] (which, by contract, never sees an
/// external/compressed image). A short or 4-byte-uncompressed image is left
/// untouched (the detoast is a no-op on it). `name` args (read via
/// [`arg_name_bytes`]) are never varlena-headered and are not swept.
fn detoast_varlena_args(fcinfo: &mut FunctionCallInfoBaseData) {
    let n = fcinfo.ref_args.len();
    for i in 0..n {
        let needs = match fcinfo.ref_arg(i).and_then(|p| p.as_varlena()) {
            Some(image) if !image.is_empty() => {
                let b0 = image[0];
                if b0 == 0x01 {
                    // VARATT_IS_EXTERNAL (1B-E): an on-disk TOAST pointer. A
                    // fixed-length pass-by-ref `name` buffer (read via
                    // arg_name_bytes, never this path) cannot begin with 0x01.
                    true
                } else if (b0 & 0x03) == 0x02 {
                    // VARATT_IS_COMPRESSED (4B-C). Guard against a fixed-length
                    // `name`/`bpchar` buffer whose first byte happens to have low
                    // bits 0b10 by insisting the encoded VARSIZE matches the whole
                    // image length (a real compressed varlena is exactly VARSIZE
                    // bytes; a name buffer's spurious header is not).
                    image.len() >= 4
                        && (u32::from_ne_bytes([image[0], image[1], image[2], image[3]]) >> 2)
                            as usize
                            == image.len()
                } else {
                    false
                }
            }
            _ => false,
        };
        if !needs {
            continue;
        }
        let m = scratch_mcx();
        let detoasted: Vec<u8> = {
            let image = fcinfo.ref_arg(i).and_then(|p| p.as_varlena()).unwrap();
            match backend_access_common_detoast_seams::detoast_attr::call(m.mcx(), image) {
                Ok(v) => v.as_slice().to_vec(),
                // A detoast failure (missing chunk, etc.) is an ereport(ERROR)
                // in C; leaving the original image in place surfaces it on the
                // next core read instead of swallowing it here.
                Err(_) => continue,
            }
        };
        if let Some(RefPayload::Varlena(b)) = fcinfo.ref_arg_mut(i) {
            *b = detoasted;
        }
    }
}

/// Fully detoast an ARRAY argument on the by-ref lane in place (C
/// `PG_GETARG_ARRAYTYPE_P(i)` == `DatumGetArrayTypeP` == `pg_detoast_datum`).
///
/// Unlike [`detoast_varlena_args`] — which deliberately leaves a SHORT (1-byte
/// header) varlena untouched because the `text`/`bytea` cores read it via
/// `vardata_any_slice` (header-size-aware) — the array cores and the
/// `array_to_text_elements` seam read the `ArrayType` struct fields at FIXED
/// 4-byte-header offsets (`arr_elemtype` at byte 12, `arr_ndim` at 4). A stored
/// array column (e.g. `pg_proc.proallargtypes`) arrives heap-packed with a
/// 1-byte short header; reading offset 12 off it lands inside `ARR_DIMS[0]`
/// (the element count) and yields a bogus element-type OID ("cache lookup
/// failed for type N"). C avoids this because `PG_GETARG_ARRAYTYPE_P` runs the
/// full `detoast_attr`, which expands a short header to a 4-byte one. Mirror
/// that here for the array arg so every `ARR_*` field read sees a 4-byte header.
fn detoast_array_arg(fcinfo: &mut FunctionCallInfoBaseData, i: usize) {
    let needs = match fcinfo.ref_arg(i).and_then(|p| p.as_varlena()) {
        // VARATT_IS_4B_U (uncompressed 4-byte header): already normalized, no-op.
        Some(image) if !image.is_empty() => (image[0] & 0x03) != 0x00,
        _ => false,
    };
    if !needs {
        return;
    }
    let m = scratch_mcx();
    let detoasted: Vec<u8> = {
        let image = fcinfo.ref_arg(i).and_then(|p| p.as_varlena()).unwrap();
        match backend_access_common_detoast_seams::detoast_attr::call(m.mcx(), image) {
            Ok(v) => v.as_slice().to_vec(),
            // A detoast failure (missing chunk, etc.) is an ereport(ERROR) in C;
            // leaving the original image surfaces it on the next core read.
            Err(_) => return,
        }
    };
    if let Some(RefPayload::Varlena(b)) = fcinfo.ref_arg_mut(i) {
        *b = detoasted;
    }
}

/// A `text`/`bytea`/`name` arg's `VARDATA_ANY` payload bytes. Under the
/// header-ful-everywhere convention the by-ref lane carries the full varlena
/// image; this reads the payload by skipping the header. The image may be EITHER
/// a 4-byte-header varlena (a freshly built Const / detoasted value) OR a short
/// 1-byte-header varlena (how the heap stores small values, what
/// `heap_deform_tuple` hands back); `VARDATA_ANY` skips the correct header size
/// for each. A naive fixed `VARHDRSZ` strip over a short image silently drops
/// three payload bytes from the front. For `name` (typlen 64, framed as a
/// varlena-headered NAMEDATALEN buffer) this yields the 64-byte buffer, which the
/// cores NUL-trim.
#[inline]
fn arg_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("varlena cmp fn: by-ref arg missing from by-ref lane");
    crate::vardata_any_slice(image)
}

/// A `name` arg's raw `NAMEDATALEN` buffer bytes. Unlike `text`/`bytea`, a
/// `name` is a fixed-length (typlen 64) pass-by-reference type that is NOT a
/// varlena: it crosses the by-ref lane as its raw NUL-padded `NameData` buffer
/// with NO length-word header in front (exactly as C's `Name`/`NameStr`). It
/// must be read VERBATIM — running it through `vardata_any_slice` would
/// misread the first buffer byte as a varlena header and chop leading
/// characters off the name. The cores NUL-trim it (`name_str` / `name_text`).
#[inline]
fn arg_name_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("varlena fn: name arg missing from by-ref lane")
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

/// Set a `text`/`bytea`/`name` (by-reference) result on the by-ref lane. Under
/// the header-ful-everywhere convention the cores produce a header-LESS payload
/// here, and this stamps the 4-byte uncompressed varlena length word in front
/// (`SET_VARSIZE`), symmetric with how `arg_bytes` reads args back (skipping the
/// header). `_send` results are themselves `bytea` values and are framed the
/// same way; the wire layer strips the header downstream. Returns the dummy
/// by-value word.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    let mut image = Vec::with_capacity(bytes.len() + VARHDRSZ);
    image.extend_from_slice(&types_datum::varlena::set_varsize_4b(bytes.len() + VARHDRSZ));
    image.extend_from_slice(&bytes);
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// Set an already-complete varlena image (e.g. a flat `ArrayType` produced by
/// `construct_md_array` / `makeArrayResult`, whose leading word is its own
/// `SET_VARSIZE` header) as the by-ref result, WITHOUT prepending another
/// varlena header. `ret_varlena` exists for raw payloads that still need a
/// `SET_VARSIZE` frame; an array image already carries one, so wrapping it again
/// would shift every field by `VARHDRSZ` (corrupting `ARR_ELEMTYPE`, which
/// `array_out` then reads as 0 -> "cache lookup failed for type 0").
fn ret_varlena_image(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(image));
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

// ---------------------------------------------------------------------------
// fc_ adapters — text relational family (comparison.rs).
//
// Result-native: a builtin's `ereport(ERROR)` travels as `Err(PgError)`
// straight back to the dispatch (`invoke_builtin`), with no panic /
// `catch_unwind` (see `docs/proposals/panic-to-result-migration.md`).
// ---------------------------------------------------------------------------

fn fc_texteq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::comparison::texteq(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c))?))
}
fn fc_textne(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::comparison::textne(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c))?))
}
fn fc_text_lt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::comparison::text_lt(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c))?))
}
fn fc_text_le(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::comparison::text_le(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c))?))
}
fn fc_text_gt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::comparison::text_gt(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c))?))
}
fn fc_text_ge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::comparison::text_ge(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c))?))
}
fn fc_bttextcmp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_i32((crate::comparison::bttextcmp(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c))?))
}
fn fc_text_starts_with(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::comparison::text_starts_with(
        arg_bytes(fcinfo, 0),
        arg_bytes(fcinfo, 1),
        c,
    ))?))
}
fn fc_btvarstrequalimage(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: btvarstrequalimage(internal) — arg0 (`opcintype`/internal) is ignored;
    // the answer depends only on the collation read from the frame.
    let c = collation(fcinfo);
    Ok(ret_bool((crate::comparison::btvarstrequalimage(c))?))
}

// ---------------------------------------------------------------------------
// fc_ adapters — name<->text family (name_pattern.rs). For `name`-first
// functions arg0 is the `name`; for `text`-first functions arg0 is the `text`.
// ---------------------------------------------------------------------------

fn fc_nameeqtext(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::name_pattern::nameeqtext(arg_name_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c))?))
}
fn fc_namenetext(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::name_pattern::namenetext(arg_name_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c))?))
}
fn fc_namelttext(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::name_pattern::namelttext(arg_name_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c))?))
}
fn fc_nameletext(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::name_pattern::nameletext(arg_name_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c))?))
}
fn fc_namegttext(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::name_pattern::namegttext(arg_name_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c))?))
}
fn fc_namegetext(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::name_pattern::namegetext(arg_name_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c))?))
}
fn fc_btnametextcmp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_i32((crate::name_pattern::btnametextcmp(arg_name_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c))?))
}
fn fc_texteqname(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::name_pattern::texteqname(arg_bytes(fcinfo, 0), arg_name_bytes(fcinfo, 1), c))?))
}
fn fc_textnename(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::name_pattern::textnename(arg_bytes(fcinfo, 0), arg_name_bytes(fcinfo, 1), c))?))
}
fn fc_textltname(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::name_pattern::textltname(arg_bytes(fcinfo, 0), arg_name_bytes(fcinfo, 1), c))?))
}
fn fc_textlename(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::name_pattern::textlename(arg_bytes(fcinfo, 0), arg_name_bytes(fcinfo, 1), c))?))
}
fn fc_textgtname(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::name_pattern::textgtname(arg_bytes(fcinfo, 0), arg_name_bytes(fcinfo, 1), c))?))
}
fn fc_textgename(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_bool((crate::name_pattern::textgename(arg_bytes(fcinfo, 0), arg_name_bytes(fcinfo, 1), c))?))
}
fn fc_bttextnamecmp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    Ok(ret_i32((crate::name_pattern::bttextnamecmp(arg_bytes(fcinfo, 0), arg_name_bytes(fcinfo, 1), c))?))
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

fn fc_textin(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let s = arg_cstring(fcinfo, 0).as_bytes().to_vec();
    let m = scratch_mcx();
    let out = (crate::wire_io::textin(m.mcx(), &s))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_textout(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let out = (crate::wire_io::textout(m.mcx(), arg_bytes(fcinfo, 0)))?;
    Ok(ret_cstring(fcinfo, cstring_lane(&out)))
}
fn fc_textsend(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let bytes = (crate::wire_io::textsend(m.mcx(), arg_bytes(fcinfo, 0)))?.as_bytes().to_vec();
    Ok(ret_varlena(fcinfo, bytes))
}
fn fc_byteain(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // Copy the cstring out first so the immutable arg borrow ends before we take
    // the `&mut` escontext frame (C: `byteain` reads `fcinfo->context`).
    let s = arg_cstring(fcinfo, 0).as_bytes().to_vec();
    let m = scratch_mcx();
    // C: `byteain` threads `fcinfo->context` (the soft-error sink) into the hex
    // and escape parse-error paths; a recoverable error lands in the frame
    // escontext and yields `Ok(None)` (SQL NULL), which `pg_input_is_valid`
    // reads back via `soft_error_occurred`.
    let out = (crate::bytea::byteain(m.mcx(), &s, fcinfo.escontext_mut()))?
        .map(|v| v.to_vec());
    Ok(match out {
        Some(bytes) => ret_varlena(fcinfo, bytes),
        None => Datum::null(),
    })
}
fn fc_byteaout(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let out = (crate::bytea::byteaout(m.mcx(), arg_bytes(fcinfo, 0)))?;
    Ok(ret_cstring(fcinfo, cstring_lane(&out)))
}
fn fc_byteasend(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let bytes = (crate::bytea::byteasend(m.mcx(), arg_bytes(fcinfo, 0)))?.to_vec();
    Ok(ret_varlena(fcinfo, bytes))
}

// --- name <-> text casts (wire_io.rs) ---

fn fc_name_text(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: name -> text (proname `text`, prosrc name_text). arg0 is a `name`.
    let m = scratch_mcx();
    let out = (crate::wire_io::name_text(m.mcx(), arg_name_bytes(fcinfo, 0)))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_text_name(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    // C: text -> name (proname `name`, prosrc text_name). The result is a
    // `Name` — a fixed NAMEDATALEN, zero-padded buffer with NO varlena header.
    // It crosses the by-ref lane as its RAW bytes (matching the name crate's
    // `ret_name`); `ret_varlena` would prepend a 4-byte varlena header and
    // corrupt the value (e.g. a text[]::name[] cast then renders each element
    // as the stray header bytes).
    let nd = (crate::wire_io::text_name(arg_bytes(fcinfo, 0)))?;
    fcinfo.set_ref_result(RefPayload::Varlena(nd.to_vec()));
    Ok(Datum::from_usize(0))
}

// --- length / octet-length / concat (wire_io.rs / bytea.rs) ---

fn fc_textlen(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_i32((crate::wire_io::textlen(arg_bytes(fcinfo, 0)))?))
}
fn fc_textoctetlen(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_i32((crate::wire_io::textoctetlen(arg_bytes(fcinfo, 0)))?))
}
fn fc_byteaoctetlen(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_i32((crate::bytea::byteaoctetlen(arg_bytes(fcinfo, 0)))?))
}
fn fc_textcat(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let out = (crate::wire_io::textcat(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1)))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_byteacat(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let out = (crate::bytea::byteacat(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1)))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

// --- text larger/smaller + bytea larger/smaller (comparison.rs / bytea.rs) ---

fn fc_text_larger(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    let m = scratch_mcx();
    let out = (crate::comparison::text_larger(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_text_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    let m = scratch_mcx();
    let out = (crate::comparison::text_smaller(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

// --- substring / position / overlay (position_ops.rs / bytea.rs) ---

fn fc_textpos(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    let m = scratch_mcx();
    Ok(ret_i32((crate::position_ops::textpos(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c))?))
}
fn fc_byteapos(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_i32((crate::bytea::byteapos(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1)))?))
}
fn fc_bytea_substr(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let s = arg_i32(fcinfo, 1);
    let l = arg_i32(fcinfo, 2);
    let out = (crate::bytea::bytea_substr(m.mcx(), arg_bytes(fcinfo, 0), s, l))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_bytea_substr_no_len(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let s = arg_i32(fcinfo, 1);
    let out = (crate::bytea::bytea_substr_no_len(m.mcx(), arg_bytes(fcinfo, 0), s))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_byteaoverlay(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let sp = arg_i32(fcinfo, 2);
    let sl = arg_i32(fcinfo, 3);
    let out = (crate::bytea::byteaoverlay(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), sp, sl))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_byteaoverlay_no_len(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let sp = arg_i32(fcinfo, 2);
    let out = (crate::bytea::byteaoverlay_no_len(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), sp))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

// --- left / right / reverse (position_ops.rs / bytea.rs) ---

fn fc_text_left(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let n = arg_i32(fcinfo, 1);
    let out = (crate::position_ops::text_left(m.mcx(), arg_bytes(fcinfo, 0), n))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_text_right(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let n = arg_i32(fcinfo, 1);
    let out = (crate::position_ops::text_right(m.mcx(), arg_bytes(fcinfo, 0), n))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_text_reverse(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let out = (crate::position_ops::text_reverse(m.mcx(), arg_bytes(fcinfo, 0)))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_bytea_reverse(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let out = (crate::bytea::bytea_reverse(m.mcx(), arg_bytes(fcinfo, 0)))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

// --- replace / split_part (position_ops.rs / split_format.rs) ---

fn fc_replace_text(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    let m = scratch_mcx();
    let out = (crate::position_ops::replace_text(
        m.mcx(),
        arg_bytes(fcinfo, 0),
        arg_bytes(fcinfo, 1),
        arg_bytes(fcinfo, 2),
        c,
    ))?
    .to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_split_part(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    let m = scratch_mcx();
    let fldnum = arg_i32(fcinfo, 2);
    let out = (crate::split_format::split_part(
        m.mcx(),
        arg_bytes(fcinfo, 0),
        arg_bytes(fcinfo, 1),
        fldnum,
        c,
    ))?
    .to_vec();
    Ok(ret_varlena(fcinfo, out))
}

// --- bytea comparison operators + cmp (bytea.rs) ---

fn fc_byteaeq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_bool((crate::bytea::byteaeq(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1)))?))
}
fn fc_byteane(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_bool((crate::bytea::byteane(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1)))?))
}
fn fc_bytealt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_bool((crate::bytea::bytealt(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1)))?))
}
fn fc_byteale(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_bool((crate::bytea::byteale(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1)))?))
}
fn fc_byteagt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_bool((crate::bytea::byteagt(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1)))?))
}
fn fc_byteage(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_bool((crate::bytea::byteage(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1)))?))
}
fn fc_byteacmp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_i32((crate::bytea::byteacmp(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1)))?))
}
fn fc_bytea_larger(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let out = (crate::bytea::bytea_larger(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1)))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_bytea_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let out = (crate::bytea::bytea_smaller(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1)))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_bytea_bit_count(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_i64((crate::bytea::bytea_bit_count(arg_bytes(fcinfo, 0)))?))
}

// --- bytea <-> int casts (bytea.rs) ---

fn fc_bytea_int2(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_i16((crate::bytea::bytea_int2(arg_bytes(fcinfo, 0)))?))
}
fn fc_bytea_int4(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_i32((crate::bytea::bytea_int4(arg_bytes(fcinfo, 0)))?))
}
fn fc_bytea_int8(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_i64((crate::bytea::bytea_int8(arg_bytes(fcinfo, 0)))?))
}
fn fc_int2_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let out = (crate::bytea::int2_bytea(m.mcx(), arg_i16(fcinfo, 0)))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_int4_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let out = (crate::bytea::int4_bytea(m.mcx(), arg_i32(fcinfo, 0)))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_int8_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let out = (crate::bytea::int8_bytea(m.mcx(), arg_i64(fcinfo, 0)))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

// --- text_pattern_ops (name_pattern.rs) ---

fn fc_text_pattern_lt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_bool((crate::name_pattern::text_pattern_lt(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1)))?))
}
fn fc_text_pattern_le(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_bool((crate::name_pattern::text_pattern_le(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1)))?))
}
fn fc_text_pattern_ge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_bool((crate::name_pattern::text_pattern_ge(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1)))?))
}
fn fc_text_pattern_gt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_bool((crate::name_pattern::text_pattern_gt(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1)))?))
}
fn fc_bttext_pattern_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_i32((crate::name_pattern::bttext_pattern_cmp(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1)))?))
}

// --- base conversions / to_hex / to_bin / to_oct (misc_encoding.rs) ---

fn fc_to_hex32(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let out = (crate::misc_encoding::to_hex32(m.mcx(), arg_i32(fcinfo, 0)))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_to_hex64(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let out = (crate::misc_encoding::to_hex64(m.mcx(), arg_i64(fcinfo, 0)))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_to_bin32(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let out = (crate::misc_encoding::to_bin32(m.mcx(), arg_i32(fcinfo, 0)))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_to_bin64(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let out = (crate::misc_encoding::to_bin64(m.mcx(), arg_i64(fcinfo, 0)))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_to_oct32(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let out = (crate::misc_encoding::to_oct32(m.mcx(), arg_i32(fcinfo, 0)))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_to_oct64(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let out = (crate::misc_encoding::to_oct64(m.mcx(), arg_i64(fcinfo, 0)))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

// --- unicode / unistr (misc_encoding.rs) ---

fn fc_unicode_version(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let out = (crate::misc_encoding::unicode_version(m.mcx()))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_icu_unicode_version(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: returns the ICU collator's Unicode version; this build has no ICU
    // (the value core returns `None`), so the result is NULL.
    let m = scratch_mcx();
    let out = (crate::misc_encoding::icu_unicode_version(m.mcx()))?.map(|b| b.to_vec());
    Ok(match out {
        Some(b) => ret_varlena(fcinfo, b),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    })
}
fn fc_unicode_assigned(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    Ok(ret_bool((crate::misc_encoding::unicode_assigned(arg_bytes(fcinfo, 0)))?))
}
fn fc_unicode_normalize_func(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let out = (crate::misc_encoding::unicode_normalize_func(
        m.mcx(),
        arg_bytes(fcinfo, 0),
        arg_bytes(fcinfo, 1),
    ))?
    .to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_unicode_is_normalized(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    Ok(ret_bool((crate::misc_encoding::unicode_is_normalized(
        m.mcx(),
        arg_bytes(fcinfo, 0),
        arg_bytes(fcinfo, 1),
    ))?))
}
fn fc_unistr(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let out = (crate::misc_encoding::unistr(m.mcx(), arg_bytes(fcinfo, 0)))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
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

fn fc_byteaGetByte(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let n = arg_i32(fcinfo, 1);
    Ok(ret_i32((crate::bytea::bytea_get_byte(arg_bytes(fcinfo, 0), n))?))
}
fn fc_byteaGetBit(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let n = arg_i64(fcinfo, 1);
    Ok(ret_i32((crate::bytea::bytea_get_bit(arg_bytes(fcinfo, 0), n))?))
}
fn fc_byteaSetByte(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let n = arg_i32(fcinfo, 1);
    let newbyte = arg_i32(fcinfo, 2);
    let out = (crate::bytea::bytea_set_byte(m.mcx(), arg_bytes(fcinfo, 0), n, newbyte))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_byteaSetBit(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let n = arg_i64(fcinfo, 1);
    let newbit = arg_i32(fcinfo, 2);
    let out = (crate::bytea::bytea_set_bit(m.mcx(), arg_bytes(fcinfo, 0), n, newbit))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

// --- text substring / overlay (position_ops.rs: text_substring/text_overlay) ---

fn fc_text_substr(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    // C: text_substr -> text_substring(str, start, length, false).
    let m = scratch_mcx();
    let start = arg_i32(fcinfo, 1);
    let length = arg_i32(fcinfo, 2);
    let out = (crate::position_ops::text_substring(m.mcx(), arg_bytes(fcinfo, 0), start, length, false))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_text_substr_no_len(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    // C: text_substr_no_len -> text_substring(str, start, -1, true).
    let m = scratch_mcx();
    let start = arg_i32(fcinfo, 1);
    let out = (crate::position_ops::text_substring(m.mcx(), arg_bytes(fcinfo, 0), start, -1, true))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_textoverlay(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    // C: textoverlay -> text_overlay(t1, t2, sp, sl).
    let m = scratch_mcx();
    let sp = arg_i32(fcinfo, 2);
    let sl = arg_i32(fcinfo, 3);
    let out = (crate::position_ops::text_overlay(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), sp, sl))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_textoverlay_no_len(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    // C: textoverlay_no_len -> text_overlay computes sl = textlen(t2) internally.
    // The value core `text_overlay` requires the explicit `sl`; C's no-len
    // variant passes `text_length(PG_GETARG_DATUM(1))`. Compute the replacement
    // string's character length (textlen core) and forward.
    let m = scratch_mcx();
    let t2 = arg_bytes(fcinfo, 1);
    let sl = (crate::wire_io::textlen(t2))?;
    let sp = arg_i32(fcinfo, 2);
    let out = (crate::position_ops::text_overlay(m.mcx(), arg_bytes(fcinfo, 0), t2, sp, sl))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

// --- unknown I/O (wire_io.rs: unknownin/unknownout). `unknown` is a
// cstring-representation type (typlen -2): both the arg and the result are the
// raw cstring bytes on the by-ref cstring lane. ---

fn fc_unknownin(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: unknownin(cstring) -> unknown == pstrdup(str). `unknown` crosses on the
    // cstring lane (typlen -2), so the result is the raw bytes as a cstring.
    let m = scratch_mcx();
    let out = (crate::wire_io::unknownin(m.mcx(), arg_cstring(fcinfo, 0).as_bytes()))?.to_vec();
    Ok(ret_cstring(fcinfo, cstring_lane(&out)))
}
fn fc_unknownout(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: unknownout(unknown) -> cstring == pstrdup(str). The `unknown` arg is a
    // cstring on the by-ref lane.
    let m = scratch_mcx();
    let out = (crate::wire_io::unknownout(m.mcx(), arg_cstring(fcinfo, 0).as_bytes()))?.to_vec();
    Ok(ret_cstring(fcinfo, cstring_lane(&out)))
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
// fc_ adapters — string_to_array / array_to_string (split_format.rs). These are
// the `text`<->`text[]` bridge functions. Both are `proisstrict => 'f'` for the
// variants that take a `null_string` (and `string_to_array` itself), so the
// adapters read each arg's null flag off the frame and pass `Option<&[u8]>` to
// the cores; a NULL `inputstring`/`fldsep` propagates per the C `PG_ARGISNULL`
// checks. The array argument/result rides the by-ref `Varlena` lane as the flat
// `ArrayType` image (C `DatumGetArrayTypeP` / `PG_RETURN_ARRAYTYPE_P`); the
// element type for `array_to_text` is read from the array header (C
// `ARR_ELEMTYPE`), at byte offset 12 of the flat image.
// ---------------------------------------------------------------------------

/// `text` arg `i` as `Option<&[u8]>`: `None` when the frame marks it SQL NULL
/// (C: `PG_ARGISNULL(i)`), else its by-ref `Varlena` payload.
#[inline]
fn opt_arg_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> Option<&'a [u8]> {
    if fcinfo.arg(i).map(|d| d.isnull).unwrap_or(false) {
        return None;
    }
    Some(arg_bytes(fcinfo, i))
}

/// The full, header-FUL flat `ArrayType` image on the by-ref lane (C
/// `PG_GETARG_ARRAYTYPE_P` / `DatumGetArrayTypeP`). Unlike `arg_bytes` (which
/// strips the leading 4-byte varlena length word via `vardata_any_slice` for
/// the `text`/`bytea` cores), the array cores and the `array_to_text_elements`
/// seam read the `ArrayType` struct fields off the contiguous block STARTING at
/// the `vl_len_` word (`arrayfuncs::foundation::arr_*` reads `ndim` at offset 4,
/// `dataoffset` at 8, `elemtype` at 12). Stripping the header here misaligns
/// every field by 4 bytes — `ARR_ELEMTYPE` then reads `ARR_DIMS[0]` (the element
/// count) as the element type OID ("cache lookup failed for type N").
#[inline]
fn arg_array_image<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("array fn: by-ref array arg missing from by-ref lane")
}

/// Wrap the header-FUL flat array image carried on the by-ref lane into the
/// canonical by-reference `Datum` the `split_format` cores consume (C
/// `DatumGetArrayTypeP` reads the same contiguous block, header word included).
/// Copies the bytes into `mcx`.
fn arg_array_datum<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    i: usize,
) -> types_error::PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    let bytes = arg_array_image(fcinfo, i);
    let v = mcx::slice_in(mcx, bytes)?;
    Ok(types_tuple::backend_access_common_heaptuple::Datum::ByRef(v))
}

/// `ARR_ELEMTYPE(array)` — the element type Oid stored in the header-FUL flat
/// `ArrayType` at byte offset 12 (`int32 vl_len_; int ndim; int32 dataoffset;
/// Oid elemtype`). `array` MUST be the header-ful image (`arg_array_image`),
/// matching `arrayfuncs::foundation::arr_elemtype`.
#[inline]
fn arr_elemtype(array: &[u8]) -> Oid {
    u32::from_ne_bytes([array[12], array[13], array[14], array[15]])
}

fn fc_text_to_array(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    let m = scratch_mcx();
    let inputstring = opt_arg_bytes(fcinfo, 0);
    let fldsep = opt_arg_bytes(fcinfo, 1);
    let out = (crate::split_format::text_to_array(m.mcx(), inputstring, fldsep, None, c))?;
    Ok(match out {
        // `d` is already a complete array varlena (`Datum::ByRef(ArrayType
        // image)`); set it as the varlena result directly — do NOT re-wrap it in
        // another `SET_VARSIZE` header.
        Some(d) => ret_varlena_image(fcinfo, d.as_ref_bytes().to_vec()),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    })
}
fn fc_text_to_array_null(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let c = collation(fcinfo);
    let m = scratch_mcx();
    let inputstring = opt_arg_bytes(fcinfo, 0);
    let fldsep = opt_arg_bytes(fcinfo, 1);
    let null_string = opt_arg_bytes(fcinfo, 2);
    let out =
        (crate::split_format::text_to_array_null(m.mcx(), inputstring, fldsep, null_string, c))?;
    Ok(match out {
        // `d` is already a complete array varlena (`Datum::ByRef(ArrayType
        // image)`); set it as the varlena result directly — do NOT re-wrap it in
        // another `SET_VARSIZE` header.
        Some(d) => ret_varlena_image(fcinfo, d.as_ref_bytes().to_vec()),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    })
}
fn fc_array_to_text(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    // C: arr = PG_GETARG_ARRAYTYPE_P(0) — fully detoasts (short→4-byte) before
    // ARR_ELEMTYPE reads offset 12. The text-arg sweep above leaves short
    // headers packed, so the array arg needs its own full detoast.
    detoast_array_arg(fcinfo, 0);
    let m = scratch_mcx();
    let elemtype = arr_elemtype(arg_array_image(fcinfo, 0));
    let v = arg_array_datum(m.mcx(), fcinfo, 0)?;
    let fldsep = arg_bytes(fcinfo, 1);
    let out = (crate::split_format::array_to_text(m.mcx(), v, elemtype, fldsep))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_array_to_text_null(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    // C `array_to_text_null` is `proisstrict => 'f'`: the array (arg0) and
    // `fldsep` (arg1) are still required (C reads them unconditionally), only
    // `null_string` (arg2) may be NULL.
    // C: PG_GETARG_ARRAYTYPE_P(0) fully detoasts (short→4-byte) before the
    // ARR_ELEMTYPE read at offset 12.
    detoast_array_arg(fcinfo, 0);
    let m = scratch_mcx();
    let elemtype = arr_elemtype(arg_array_image(fcinfo, 0));
    let v = arg_array_datum(m.mcx(), fcinfo, 0)?;
    let fldsep = arg_bytes(fcinfo, 1);
    let null_string = opt_arg_bytes(fcinfo, 2);
    let out =
        (crate::split_format::array_to_text_null(m.mcx(), v, elemtype, fldsep, null_string))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

// ---------------------------------------------------------------------------
// fc_ adapters — concat() / concat_ws() / format() (split_format.rs).
//
// These are `provariadic => 'any'`, `proisstrict => 'f'` SQL functions. The
// executor expands an un-keyworded variadic call (`concat(a,b,c)`) into N
// separate fmgr args; an explicit `VARIADIC arr` call arrives as a single array
// arg, detected via `get_fn_expr_variadic(flinfo)` (C: the same check inside
// `concat_internal`/`text_format`). Each per-argument view (`FormatArg`) carries
// the argument's canonical `Datum`, its SQL-NULL flag, and its actual type OID
// (`get_fn_expr_argtype(flinfo, i)`, C: `get_fn_expr_argtype`). A by-reference
// (varlena/cstring) arg crosses on the by-ref lane as its header-ful image and
// is wrapped verbatim into the canonical `Datum::ByRef`/`Datum::Cstring`
// (header-ful everywhere — `concat_internal`'s `output_function_call` reads the
// same framed image); a by-value arg is the bare word as `Datum::ByVal`.
// ---------------------------------------------------------------------------

use crate::split_format::FormatArg;
use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;

/// Build one `FormatArg` for fmgr arg `i`: its canonical value, SQL-NULL flag,
/// and actual type OID. NULL args carry a placeholder value (`concat_internal`
/// /`text_format` only consult `value` when `!is_null`).
fn format_arg<'mcx>(mcx: mcx::Mcx<'mcx>, fcinfo: &FunctionCallInfoBaseData, i: usize) -> types_error::PgResult<FormatArg<'mcx>> {
    let is_null = fcinfo.arg(i).map(|d| d.isnull).unwrap_or(true);
    let typid = backend_utils_fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), i as i32);
    let value = if is_null {
        CanonDatum::null()
    } else if let Some(p) = fcinfo.ref_arg(i) {
        // By-reference arg: the header-ful image crosses verbatim.
        match p {
            RefPayload::Varlena(b) => CanonDatum::ByRef(mcx::slice_in(mcx, b)?),
            RefPayload::Cstring(s) => CanonDatum::Cstring(s.clone()),
            _ => CanonDatum::ByRef(mcx::slice_in(mcx, p.as_varlena().unwrap_or(&[]))?),
        }
    } else {
        // By-value arg: the bare machine word.
        CanonDatum::ByVal(fcinfo.arg(i).map(|d| d.value.as_usize()).unwrap_or(0))
    };
    Ok(FormatArg { value, is_null, typid })
}

/// Collect all fmgr args (from `start`) into a `FormatArg` vector.
fn collect_format_args<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    start: usize,
) -> types_error::PgResult<Vec<FormatArg<'mcx>>> {
    (start..fcinfo.nargs()).map(|i| format_arg(mcx, fcinfo, i)).collect()
}

/// The `VARIADIC arr` fast path: `(array_datum, element_type)` when this call
/// passed exactly one trailing array argument with the VARIADIC keyword (C:
/// `PG_NARGS() == argidx + 1 && get_fn_expr_variadic(fcinfo->flinfo)`). `None`
/// (the array arg is SQL NULL) makes the whole result NULL upstream.
fn variadic_array<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    argidx: usize,
) -> types_error::PgResult<Option<Option<(CanonDatum<'mcx>, Oid)>>> {
    if fcinfo.nargs() != argidx + 1 {
        return Ok(None);
    }
    if !backend_utils_fmgr_core::get_fn_expr_variadic(fcinfo.flinfo.as_deref()) {
        return Ok(None);
    }
    // The single trailing arg is the variadic array.
    if fcinfo.arg(argidx).map(|d| d.isnull).unwrap_or(true) {
        // concat(VARIADIC NULL) / format(..., VARIADIC NULL) -> NULL.
        return Ok(Some(None));
    }
    let arrtype = backend_utils_fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), argidx as i32);
    let elemtype = backend_utils_cache_lsyscache_seams::get_base_element_type::call(arrtype)?;
    // The array image rides the by-ref lane; wrap it the same way the
    // `array_to_text` adapter does (header-less payload, the convention
    // `array_to_text_internal` reads).
    let v = arg_array_datum(mcx, fcinfo, argidx)?;
    Ok(Some(Some((v, elemtype))))
}

/// Write an `Option<Vec<u8>>` text result back onto the by-ref lane: `Some` is a
/// non-NULL `text` payload (framed by `ret_varlena`), `None` is the SQL-NULL
/// result a non-strict builder returns (e.g. a NULL format string / separator).
#[inline]
fn ret_opt_text(fcinfo: &mut FunctionCallInfoBaseData, result: Option<Vec<u8>>) -> Datum {
    match result {
        Some(bytes) => ret_varlena(fcinfo, bytes),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}

fn fc_text_concat(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let mcx = m.mcx();
    let result: Option<Vec<u8>> = match variadic_array(mcx, fcinfo, 0)? {
        Some(None) => None, // VARIADIC NULL -> NULL
        Some(Some(arr)) => {
            (crate::split_format::text_concat(mcx, &[], Some(arr)))?.map(|b| b.as_slice().to_vec())
        }
        None => {
            let args = collect_format_args(mcx, fcinfo, 0)?;
            (crate::split_format::text_concat(mcx, &args, None))?.map(|b| b.as_slice().to_vec())
        }
    };
    Ok(ret_opt_text(fcinfo, result))
}

fn fc_text_concat_ws(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let mcx = m.mcx();
    // arg0 is the separator (text, may be NULL -> whole result NULL).
    let sep = opt_arg_bytes(fcinfo, 0).map(|b| b.to_vec());
    let result: Option<Vec<u8>> = match variadic_array(mcx, fcinfo, 1)? {
        Some(None) => None,
        Some(Some(arr)) => (crate::split_format::text_concat_ws(mcx, sep.as_deref(), &[], Some(arr)))?
            .map(|b| b.as_slice().to_vec()),
        None => {
            let args = collect_format_args(mcx, fcinfo, 0)?;
            (crate::split_format::text_concat_ws(mcx, sep.as_deref(), &args, None))?
                .map(|b| b.as_slice().to_vec())
        }
    };
    Ok(ret_opt_text(fcinfo, result))
}

fn fc_text_format(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let m = scratch_mcx();
    let mcx = m.mcx();
    // arg0 is the format string (text, may be NULL -> NULL result).
    let fmt = opt_arg_bytes(fcinfo, 0).map(|b| b.to_vec());
    // The value args start at index 1; a VARIADIC array expands them.
    let result: Option<Vec<u8>> = if backend_utils_fmgr_core::get_fn_expr_variadic(fcinfo.flinfo.as_deref())
        && fcinfo.nargs() == 2
    {
        // format(fmt, VARIADIC arr): expand the array into the value list.
        if fcinfo.arg(1).map(|d| d.isnull).unwrap_or(true) {
            // format(fmt, VARIADIC NULL): an all-NULL/empty value list.
            (crate::split_format::text_format(mcx, fmt.as_deref(), &[]))?.map(|b| b.as_slice().to_vec())
        } else {
            let arrtype = backend_utils_fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), 1);
            let elemtype = (backend_utils_cache_lsyscache_seams::get_base_element_type::call(arrtype))?;
            let v = arg_array_datum(mcx, fcinfo, 1)?;
            let elems = (crate::split_format::array_to_format_args(mcx, v, elemtype))?;
            (crate::split_format::text_format(mcx, fmt.as_deref(), &elems))?
                .map(|b| b.as_slice().to_vec())
        }
    } else {
        let args = collect_format_args(mcx, fcinfo, 1)?;
        (crate::split_format::text_format(mcx, fmt.as_deref(), &args))?.map(|b| b.as_slice().to_vec())
    };
    Ok(ret_opt_text(fcinfo, result))
}

fn fc_text_format_nv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    // C: text_format_nv is a thin nonvariadic wrapper (it exists only for
    // opr_sanity); it never sees the VARIADIC keyword.
    let m = scratch_mcx();
    let mcx = m.mcx();
    let fmt = opt_arg_bytes(fcinfo, 0).map(|b| b.to_vec());
    let args = collect_format_args(mcx, fcinfo, 1)?;
    let result: Option<Vec<u8>> =
        (crate::split_format::text_format_nv(mcx, fmt.as_deref(), &args))?.map(|b| b.as_slice().to_vec());
    Ok(ret_opt_text(fcinfo, result))
}

// ---------------------------------------------------------------------------
// fc_ adapters — pg_column_* introspection of "any" datum (split_format.rs).
//
// Both take a single `any` argument; their answer depends on the argument's
// type `typlen`, resolved at the fmgr/Datum boundary (C: `get_typlen(
// get_fn_expr_argtype(fcinfo->flinfo, 0))`, cached in `fn_extra` — here looked
// up per call). The argument value itself is only dereferenced on the varlena
// (`typlen == -1`) path (`toast_datum_size` / `toast_chunk_id`, which read the
// header-ful `ByRef` image off the by-ref lane); the cstring (`-2`) and
// fixed-width (`>= 0`) paths never touch the value, so a varlena `any` rides the
// by-ref lane and a by-value `any` is wrapped as a bare `ByVal` word.
// ---------------------------------------------------------------------------

/// `(typlen, value)` for a `pg_column_*` `any` arg: resolve the argument type's
/// `typlen` (C: `get_typlen(get_fn_expr_argtype(flinfo, 0))`, `elog(ERROR)` on a
/// 0 / cache-miss), and build the canonical `Datum` the cores consume — the
/// header-ful varlena image for a by-reference type (`typlen == -1`), or the
/// bare machine word for a by-value type.
fn pg_column_arg<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
) -> types_error::PgResult<(i32, CanonDatum<'mcx>)> {
    let argtypeid = backend_utils_fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), 0);
    let typlen = backend_utils_cache_lsyscache_seams::get_typlen::call(argtypeid)?;
    if typlen == 0 {
        // C: elog(ERROR, "cache lookup failed for type %u", argtypeid).
        return Err(
            types_error::PgError::error(format!(
                "cache lookup failed for type {}",
                argtypeid
            ))
            .with_sqlstate(types_error::ERRCODE_INTERNAL_ERROR),
        );
    }
    let value = match fcinfo.ref_arg(0) {
        // By-reference arg (varlena / cstring): the header-ful image crosses
        // verbatim onto the canonical `Datum`.
        Some(RefPayload::Varlena(b)) => CanonDatum::ByRef(mcx::slice_in(mcx, b)?),
        Some(RefPayload::Cstring(s)) => CanonDatum::Cstring(s.clone()),
        Some(p) => CanonDatum::ByRef(mcx::slice_in(mcx, p.as_varlena().unwrap_or(&[]))?),
        // By-value arg: the bare machine word.
        None => CanonDatum::ByVal(fcinfo.arg(0).map(|d| d.value.as_usize()).unwrap_or(0)),
    };
    Ok((typlen as i32, value))
}

fn fc_pg_column_size(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let (typlen, value) = pg_column_arg(m.mcx(), fcinfo)?;
    Ok(ret_i32((crate::split_format::pg_column_size(m.mcx(), &value, typlen))?))
}

fn fc_pg_column_toast_chunk_id(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let (typlen, value) = pg_column_arg(m.mcx(), fcinfo)?;
    Ok(match (crate::split_format::pg_column_toast_chunk_id(&value, typlen))? {
        Some(oid) => Datum::from_oid(oid),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    })
}

/// `pg_column_compression(any)` (varlena.c:5321) -> `text` (OID 2121). The
/// compression method name of a compressed varlena, or SQL NULL for a
/// non-varlena type / an uncompressed value. The `text` result payload is
/// header-less (the boundary frames it via `ret_varlena`).
fn fc_pg_column_compression(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let (typlen, value) = pg_column_arg(m.mcx(), fcinfo)?;
    let out: Option<Vec<u8>> =
        (crate::split_format::pg_column_compression(m.mcx(), &value, typlen))?
            .map(|v| v.as_slice().to_vec());
    Ok(match out {
        Some(bytes) => ret_varlena(fcinfo, bytes),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    })
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

/// Build one strict Result-native builtin row (`func: None`; dispatch goes
/// through the native overlay) paired with its [`PgFnNative`] body.
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

/// Like [`builtin`] but `proisstrict => 'f'` (the function must run even when an
/// argument is SQL NULL; the adapter handles the null itself).
fn builtin_ns(
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
            strict: false,
            retset: false,
            func: None,
        },
        native,
    )
}

/// Register the boot-critical `text` / `name`<->`text` comparison builtins (C:
/// their `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs/nargs from `pg_proc.dat`; all are `proisstrict => 't'` and not retset.
pub fn register_varlena_compare_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
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
    backend_utils_fmgr_core::register_builtins_native([
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
/// `crc32_bytea(bytea) -> int8` (pg_crc.c). Traditional reflected CRC-32
/// (zlib/Ethernet polynomial): `INIT_TRADITIONAL_CRC32` (0xFFFFFFFF) then
/// `COMP_TRADITIONAL_CRC32` over the detoasted payload then
/// `FIN_TRADITIONAL_CRC32` (^0xFFFFFFFF). `port_crc32c::legacy_crc32_lexeme`
/// performs the whole INIT/COMP/FIN triple. C returns `PG_RETURN_INT64(crc)`
/// where `crc` is a `pg_crc32` (u32) — widened to a non-negative i64.
fn fc_crc32_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let crc = port_crc32c::legacy_crc32_lexeme(arg_bytes(fcinfo, 0));
    Ok(ret_i64(crc as u64 as i64))
}

/// `crc32c_bytea(bytea) -> int8` (pg_crc.c). Castagnoli CRC-32C:
/// `INIT_CRC32C` (0xFFFFFFFF) then `COMP_CRC32C` over the detoasted payload
/// (`pg_comp_crc32c_sb8`) then `FIN_CRC32C` (^0xFFFFFFFF). Result widened from
/// `pg_crc32c` (u32) to a non-negative i64 (`PG_RETURN_INT64`).
fn fc_crc32c_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    detoast_varlena_args(fcinfo);
    let mut crc: u32 = 0xFFFF_FFFF;
    crc = port_crc32c::pg_comp_crc32c_sb8(crc, arg_bytes(fcinfo, 0));
    crc ^= 0xFFFF_FFFF;
    Ok(ret_i64(crc as u64 as i64))
}

/// Register the `crc32` / `crc32c` `bytea -> int8` checksum builtins (pg_crc.c).
/// Both are `proisstrict => 't'`, not retset; OIDs / nargs from `pg_proc.dat`
/// (6364 `crc32_bytea`, 6365 `crc32c_bytea`). The builtin `name` is the
/// `prosrc` C symbol.
pub fn register_varlena_crc_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin(6364, "crc32_bytea", 1, fc_crc32_bytea),
        builtin(6365, "crc32c_bytea", 1, fc_crc32c_bytea),
    ]);
}

pub fn register_varlena_text_bytea_byref_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        // ---- bytea get/set byte/bit (bytea.rs) ----
        // builtin `name` is the `prosrc` C symbol (canonical fmgr_builtins[]
        // keys on prosrc, not the SQL proname).
        builtin(721, "byteaGetByte", 2, fc_byteaGetByte),
        builtin(723, "byteaGetBit", 2, fc_byteaGetBit),
        builtin(722, "byteaSetByte", 3, fc_byteaSetByte),
        builtin(724, "byteaSetBit", 3, fc_byteaSetBit),
        // ---- text substring (position_ops.rs) ----
        // 877/883 and 936/937 are duplicate pg_proc OIDs for the same prosrc
        // (text_substr / text_substr_no_len); both pairs share one adapter.
        builtin(877, "text_substr", 3, fc_text_substr),
        builtin(883, "text_substr_no_len", 2, fc_text_substr_no_len),
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

/// Register the `string_to_array` / `array_to_string` `text`<->`text[]` bridge
/// builtins (varlena.c `text_to_array{,_null}` / `array_to_text{,_null}`). The
/// array argument/result crosses the by-ref lane as the flat `ArrayType` image;
/// the element-deconstruction / array-construction logic is the already-installed
/// `backend-utils-adt-arrayfuncs` owner seam. OIDs / nargs / strict transcribed
/// from `pg_proc.dat`: `string_to_array` and the `_null` variants are
/// `proisstrict => 'f'`; `array_to_text` (395) is strict.
///
/// NOT registered here: `string_to_table` / `array`-SRF variants (6160/6161,
/// `proretset => 't'` — need the SRF tuplestore boundary) and the variadic
/// `concat`/`concat_ws`/`format` (no typed-variadic frame at this boundary).
pub fn register_varlena_array_string_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin_ns(394, "text_to_array", 2, fc_text_to_array),
        builtin_ns(376, "text_to_array_null", 3, fc_text_to_array_null),
        builtin(395, "array_to_text", 2, fc_array_to_text),
        builtin_ns(384, "array_to_text_null", 3, fc_array_to_text_null),
    ]);
}

/// Register the `pg_column_size` / `pg_column_toast_chunk_id` `any`-arg
/// introspection builtins (varlena.c). Both are `proisstrict => 't'`, not
/// retset; OIDs / nargs transcribed from `pg_proc.dat`. The builtin `name` is
/// the `prosrc` C symbol.
pub fn register_varlena_pg_column_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin(1269, "pg_column_size", 1, fc_pg_column_size),
        builtin(6316, "pg_column_toast_chunk_id", 1, fc_pg_column_toast_chunk_id),
        builtin(2121, "pg_column_compression", 1, fc_pg_column_compression),
    ]);
}

/// Register the variadic-`any` text builders (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`. OIDs / nargs from `pg_proc.dat`; all
/// are `provariadic => 'any'`, `proisstrict => 'f'` and not retset
/// (`text_format_nv` is the nonvariadic opr_sanity wrapper, still non-strict).
pub fn register_varlena_format_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin_ns(3058, "text_concat", 1, fc_text_concat),
        builtin_ns(3059, "text_concat_ws", 2, fc_text_concat_ws),
        builtin_ns(3539, "text_format", 2, fc_text_format),
        builtin_ns(3540, "text_format_nv", 1, fc_text_format_nv),
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

    /// Resolve a migrated builtin OID to its Result-native body. The metadata row
    /// is still registered (and asserted present below); since these builtins are
    /// now Native their callable lives in the fmgr-core `NATIVE` overlay (private),
    /// so the tests dispatch the body directly — exactly the function `invoke_builtin`
    /// runs. Panics on an OID the migrated set does not cover.
    fn native_body(oid: u32) -> PgFnNative {
        match oid {
            721 => fc_byteaGetByte,
            723 => fc_byteaGetBit,
            722 => fc_byteaSetByte,
            724 => fc_byteaSetBit,
            936 => fc_text_substr,
            937 => fc_text_substr_no_len,
            1404 => fc_textoverlay,
            1405 => fc_textoverlay_no_len,
            109 => fc_unknownin,
            110 => fc_unknownout,
            other => panic!("native_body: unmapped test OID {other}"),
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
        // Metadata row is registered (by-name/strict/etc. readers see it)...
        backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("builtin registered");
        // ...and the Result-native body is dispatched directly.
        native_body(oid)(&mut fcinfo).expect("native builtin returned Ok");
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
        backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("builtin registered");
        let d = native_body(oid)(&mut fcinfo).expect("native builtin returned Ok");
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
        backend_utils_fmgr_core::fmgr_isbuiltin(109).expect("unknownin registered");
        native_body(109)(&mut fc).expect("unknownin returned Ok");
        let mid = match fc.take_ref_result().expect("unknownin result") {
            RefPayload::Cstring(s) => s,
            other => panic!("unexpected lane {other:?}"),
        };
        assert_eq!(mid, "abc");
        // unknownout (110): unknown -> cstring.
        let mut fc2 = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fc2.args = vec![NullableDatum::value(Datum::null())];
        fc2.ref_args = vec![Some(RefPayload::Cstring(mid))];
        backend_utils_fmgr_core::fmgr_isbuiltin(110).expect("unknownout registered");
        native_body(110)(&mut fc2).expect("unknownout returned Ok");
        let out = match fc2.take_ref_result().expect("unknownout result") {
            RefPayload::Cstring(s) => s,
            other => panic!("unexpected lane {other:?}"),
        };
        assert_eq!(out, "abc");
    }
}
