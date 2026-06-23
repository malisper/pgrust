//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `LIKE`/`ILIKE`/`NOT LIKE` pattern-matching operators and the `like_escape`
//! pattern normalizers from `like.c` (the matcher template `like_match.c`).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result.
//! Under the header-ful-everywhere convention a `text`/`bytea` arg arrives as
//! its full self-describing varlena image (4-byte length word + payload) on the
//! by-ref lane; `arg_bytes` skips the header to the `VARDATA_ANY` payload. A
//! `name` arg is a fixed-length (typlen 64) non-varlena type: it arrives as its
//! raw `NAMEDATALEN` buffer bytes with NO header, read VERBATIM by
//! `arg_name_bytes` (the cores NUL-trim / `name_text` it). The collation is
//! read from `fcinfo.fncollation` (C:
//! `PG_GET_COLLATION()`); the `bytea` family takes no collation. The two
//! `like_escape` functions return a `text`/`bytea` varlena on the by-ref lane.
//!
//! [`register_like_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`), so by-OID dispatch (and the `fmgr_isbuiltin`
//! fast path) resolves them. OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat`: every row is `nargs => 2`, `proisstrict` default
//! `'t'`, `proretset` default `'f'`.
//!
//! `like_support.c` (the planner support functions / selectivity entry points)
//! is NOT registered here (see the crate docs): it operates on planner nodes
//! that are not modeled at this boundary and is dispatched only through the bare
//! `PGFunction` registry, which is deferred.

use ::types_core::Oid;
use ::datum::Datum;
use ::types_error::PgResult;
use ::fmgr::boundary::RefPayload;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `VARHDRSZ` — the 4-byte uncompressed varlena length word.
const VARHDRSZ: usize = 4;

/// A `text`/`bytea`/`name` arg's `VARDATA_ANY` payload bytes. The by-ref lane
/// carries an already-detoasted INLINE varlena image, which is EITHER a 4-byte
/// header (`VARATT_IS_4B_U` — freshly built Const / detoasted value) OR a short
/// 1-byte header (`VARATT_IS_1B` — how the heap stores small values, what
/// `heap_deform_tuple` hands back). `VARDATA_ANY` skips the correct header size
/// for each: a naive fixed `VARHDRSZ` strip over a short image silently drops
/// three payload bytes from the front (e.g. `multirange_constructor0` → matches
/// a prefix `LIKE` against the wrong start). For `name` (typlen 64, framed as a
/// varlena-headered NAMEDATALEN buffer) the 4-byte form is used and the cores
/// NUL-trim or `name_text`.
#[inline]
fn arg_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("like fn: by-ref arg missing from by-ref lane");
    vardata_any_slice(image)
}

/// C: `VARDATA_ANY(image) .. VARSIZE_ANY_EXHDR(image)` — borrow the payload of an
/// inline varlena image, handling BOTH the 4-byte-header (`VARATT_IS_4B_U`) and
/// the short 1-byte-header (`VARATT_IS_1B`) forms.
#[inline]
fn vardata_any_slice(image: &[u8]) -> &[u8] {
    if image.is_empty() {
        return &[];
    }
    let header = image[0];
    if header != 0x01 && header & 0x01 == 0x01 {
        // VARATT_IS_1B (and not 1B-E external): short 1-byte-header inline datum.
        let total = ((header >> 1) & 0x7F) as usize;
        let total = total.min(image.len());
        &image[1..total.max(1)]
    } else if image.len() >= VARHDRSZ {
        // VARATT_IS_4B_U (uncompressed) or the framed-`name` buffer: skip the
        // 4-byte header. (Compressed/external images never reach this adapter.)
        &image[VARHDRSZ..]
    } else {
        &[]
    }
}

/// A `name` arg's raw `NAMEDATALEN` buffer bytes. Unlike `text`/`bytea`, a
/// `name` is a fixed-length (typlen 64) pass-by-reference type that is NOT a
/// varlena: it crosses the by-ref lane as its raw NUL-padded `NameData` buffer
/// with NO 4-byte length word in front (exactly as C's `Name`/`NameStr`). It
/// must therefore be read VERBATIM — skipping a 4-byte header here would chop
/// off the first 4 characters of the name. The cores NUL-trim it (`name_str` /
/// `name_text`).
#[inline]
fn arg_name_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("like fn: name arg missing from by-ref lane")
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

/// Set a `text`/`bytea` varlena result on the by-ref lane and return the dummy
/// word. Under the header-ful-everywhere convention this stamps the 4-byte
/// uncompressed varlena length word in front of the payload (`SET_VARSIZE`),
/// symmetric with how `arg_bytes` reads args back.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    let mut image = Vec::with_capacity(bytes.len() + VARHDRSZ);
    image.extend_from_slice(&::datum::varlena::set_varsize_4b(bytes.len() + VARHDRSZ));
    image.extend_from_slice(&bytes);
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("like fmgr scratch")
}

// ---------------------------------------------------------------------------
// fc_ adapters — text/name LIKE family.
// ---------------------------------------------------------------------------

fn fc_textlike(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    Ok(ret_bool(crate::textlike(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c, m.mcx())?))
}
fn fc_textnlike(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    Ok(ret_bool(crate::textnlike(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c, m.mcx())?))
}
fn fc_namelike(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    Ok(ret_bool(crate::namelike(arg_name_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c, m.mcx())?))
}
fn fc_namenlike(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    Ok(ret_bool(crate::namenlike(arg_name_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c, m.mcx())?))
}

// ---------------------------------------------------------------------------
// fc_ adapters — text/name ILIKE family.
// ---------------------------------------------------------------------------

fn fc_texticlike(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    Ok(ret_bool(crate::texticlike(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c, m.mcx())?))
}
fn fc_texticnlike(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    Ok(ret_bool(crate::texticnlike(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c, m.mcx())?))
}
fn fc_nameiclike(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    Ok(ret_bool(crate::nameiclike(arg_name_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c, m.mcx())?))
}
fn fc_nameicnlike(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    Ok(ret_bool(crate::nameicnlike(arg_name_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), c, m.mcx())?))
}

// ---------------------------------------------------------------------------
// fc_ adapters — bytea LIKE family (no collation).
// ---------------------------------------------------------------------------

fn fc_bytealike(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    Ok(ret_bool(crate::bytealike(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), m.mcx())?))
}
fn fc_byteanlike(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    Ok(ret_bool(crate::byteanlike(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), m.mcx())?))
}

// ---------------------------------------------------------------------------
// fc_ adapters — like_escape (text) / like_escape_bytea (bytea).
// ---------------------------------------------------------------------------

fn fc_like_escape(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let payload = crate::like_escape(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), m.mcx())?;
    let bytes = payload.as_slice().to_vec();
    Ok(ret_varlena(fcinfo, bytes))
}
fn fc_like_escape_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let payload = crate::like_escape_bytea(arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1), m.mcx())?;
    let bytes = payload.as_slice().to_vec();
    Ok(ret_varlena(fcinfo, bytes))
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

/// Register every SQL-callable `like.c` builtin (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs / nargs / strict /
/// retset transcribed exactly from `pg_proc.dat` (all `nargs => 2`, all strict
/// by default, none retset).
pub fn register_like_builtins() {
    fmgr_core::register_builtins_native([
        // ---- text/name LIKE ----
        builtin(850, "textlike", 2, true, false, fc_textlike),
        builtin(851, "textnlike", 2, true, false, fc_textnlike),
        builtin(858, "namelike", 2, true, false, fc_namelike),
        builtin(859, "namenlike", 2, true, false, fc_namenlike),
        builtin(1569, "textlike", 2, true, false, fc_textlike),
        builtin(1570, "textnlike", 2, true, false, fc_textnlike),
        builtin(1571, "namelike", 2, true, false, fc_namelike),
        builtin(1572, "namenlike", 2, true, false, fc_namenlike),
        // ---- text/name ILIKE ----
        builtin(1633, "texticlike", 2, true, false, fc_texticlike),
        builtin(1634, "texticnlike", 2, true, false, fc_texticnlike),
        builtin(1635, "nameiclike", 2, true, false, fc_nameiclike),
        builtin(1636, "nameicnlike", 2, true, false, fc_nameicnlike),
        // ---- bpchar LIKE/ILIKE (prosrc = textlike/textnlike/texticlike/
        //      texticnlike; bpchar is binary-compatible with text as a varlena,
        //      so the same value cores apply to the detoasted by-ref payload) ----
        builtin(1631, "textlike", 2, true, false, fc_textlike),
        builtin(1632, "textnlike", 2, true, false, fc_textnlike),
        builtin(1660, "texticlike", 2, true, false, fc_texticlike),
        builtin(1661, "texticnlike", 2, true, false, fc_texticnlike),
        // ---- like_escape (text) ----
        builtin(1637, "like_escape", 2, true, false, fc_like_escape),
        // ---- bytea LIKE ----
        builtin(2005, "bytealike", 2, true, false, fc_bytealike),
        builtin(2006, "byteanlike", 2, true, false, fc_byteanlike),
        builtin(2007, "bytealike", 2, true, false, fc_bytealike),
        builtin(2008, "byteanlike", 2, true, false, fc_byteanlike),
        // ---- like_escape (bytea) ----
        builtin(2009, "like_escape_bytea", 2, true, false, fc_like_escape_bytea),
    ]);
}
