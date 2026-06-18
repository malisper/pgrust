//! `utils/adt/varlena.c` — the variable-length built-in scalar types
//! (`text`, `bytea`, `name`, `unknown`): I/O, comparison + SortSupport,
//! substring/position/overlay, encoding/escape, split/join/format, and the
//! `string_agg` aggregate.
//!
//! `varlena.c` is ~6900 lines — too large for one porting pass — so it is
//! **decomposed into a keystone + 8 function-cluster families**. This crate is
//! the decomposition scaffold: the [`keystone`] family (the shared carrier
//! model, ABI/lifetime conventions, and foundational pure free functions) is
//! ported for real, and the function-cluster families carry their real logic
//! (genuinely-external owners reached through per-owner seams that panic until
//! the owner lands).
//!
//! ## Layered shape
//!
//! Carrier types live in the layered `types-*` stack (`types-core` for `Oid`,
//! `types-datum` for the `Varlena`/`Bytea` header framing at the Datum
//! boundary, `types-locale` for the resolved collation, `types-sortsupport`
//! for `SortSupportData`); buffers charged to the caller are
//! `mcx::PgVec<'mcx, u8>` / `mcx::PgString<'mcx>`. Genuinely-external owners
//! are reached through per-owner seam crates:
//! - `backend-utils-mb-mbutils-seams` — multibyte encoding helpers (mbutils.c)
//! - `backend-utils-adt-pg-locale-seams` — collation/ICU providers
//!   (pg_locale.c)
//! - `backend-access-common-detoast-seams` — TOAST detoast (the carrier is
//!   the already-detoasted payload)
//!
//! and the regex engine (regexp.c / backend-regex-core) for
//! `replace_text_regexp`.
//!
//! This crate OWNS the `backend-utils-adt-varlena-seams` declarations (the
//! inward seams other units call: `cstring_to_text`, `text_to_cstring`,
//! `varstr_cmp`, `split_identifier_string`, `split_directories_string`,
//! `text_to_qualified_name_list`, `text_substr`, `replace_text_regexp`) and
//! installs every one of them from [`init_seams`].
//!
//! ## Family map (KEYSTONE first)
//! - [`keystone`] — carrier conventions + `cstring_to_text*` /
//!   `text_to_cstring*` / `text_length` / `text_catenate` /
//!   `charlen_to_bytelen` / `check_collation_set` + the shared state structs.
//!   **REAL in this scaffold.**
//! - [`comparison`] — `varstr_cmp`/`text_cmp` collation core + the `text`
//!   relational operators, `bttextcmp`, `text_larger`/`smaller`.
//! - [`sortsupport`] — `bttextsortsupport`/`bytea_sortsupport` + the
//!   comparator cores + abbreviated-key substrate.
//! - [`name_pattern`] — `name`<->`text` ops + `text_pattern_ops`.
//! - [`position_ops`] — substr/position/overlay/left/right/reverse + literal
//!   `replace_text`.
//! - [`bytea`] — bytea I/O + scalar ops + comparison + int casts.
//! - [`split_format`] — split/join, `Split*String`, `format()`/`concat()`,
//!   `string_agg`, `pg_column_*`.
//! - [`replace_regexp`] — `replace_text_regexp` (regex owner seam body).
//! - [`wire_io`] — text/unknown wire I/O + name<->text + length/concat
//!   entry points.
//! - [`misc_encoding`] — base conversions, levenshtein/closest-match, unicode.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

pub mod keystone;

pub mod bytea;
pub mod comparison;
mod fmgr_builtins;
pub mod misc_encoding;
pub mod name_pattern;
pub mod position_ops;
pub mod replace_regexp;
pub mod sortsupport;
pub mod split_format;
pub mod string_agg;
pub mod wire_io;

use mcx::{Mcx, PgString, PgVec};

// ---------------------------------------------------------------------------
// `bytea_output` GUC backing store (varlena.c: `int bytea_output =
// BYTEA_OUTPUT_HEX;`). In C the `config_enum` entry binds its `variable`
// pointer straight at this int, so the GUC machinery reads/writes it in place
// and `byteaout` reads the same global. Here the storage lives in this owning
// unit and the `guc_tables` enum slot reaches it through the installed
// `GucVarAccessors` (wired from `init_seams`).
// ---------------------------------------------------------------------------
use std::cell::Cell;

thread_local! {
    /// C: `int bytea_output = BYTEA_OUTPUT_HEX;` (varlena.c:48).
    static BYTEA_OUTPUT: Cell<i32> =
        const { Cell::new(backend_utils_misc_guc_tables::consts::BYTEA_OUTPUT_HEX) };
}

/// Read `bytea_output` (`*conf->variable`).
pub fn get_bytea_output() -> i32 {
    BYTEA_OUTPUT.with(|v| v.get())
}

/// Write `bytea_output` (the GUC assign path stores through `conf->variable`).
pub fn set_bytea_output(value: i32) {
    BYTEA_OUTPUT.with(|v| v.set(value));
}

// The bare-word newtype `types_datum::Datum` (aliased `BareDatum`) survives only
// at the externally pinned, still-bare-word seam ABI edges (the `cstring_to_text`
// / `bytes_to_varlena` / `text_to_cstring` shims — the `CStringGetTextDatum` /
// `TextDatumGetCString` macro shape that ~22 unmigrated consumers + this owner's
// `::set` still speak). `Varlena` is the layered varlena-header-framing helper,
// not the Datum shim. All other internal value handling builds/reads the
// canonical unified value `types_tuple::...::Datum<'mcx>` (aliased `DatumV`),
// which the migrated `_v` seam variants take/return.
use types_datum::{Datum as BareDatum, Varlena};
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;

// ---------------------------------------------------------------------------
// Varlena header helpers (varatt.h), little-endian (the build target). These
// read the header off a raw `struct varlena *` that a by-reference `text`
// Datum points at (C's `DatumGetPointer(d)`), the same way the range ADT's
// detoast path inspects an attribute pointer. `text_to_cstring` needs the
// total/payload sizes to slice the payload and to decide whether to detoast.
// ---------------------------------------------------------------------------

const VARHDRSZ: usize = 4;

/// `((varattrib_1b *) ptr)->va_header` — the physically first header byte.
#[inline]
unsafe fn va_header_1b(ptr: *const u8) -> u8 {
    *ptr
}

/// `VARATT_IS_4B_U(PTR)` (little-endian): `(va_header & 0x03) == 0x00`.
#[inline]
unsafe fn varatt_is_4b_u(ptr: *const u8) -> bool {
    (va_header_1b(ptr) & 0x03) == 0x00
}

/// `VARATT_IS_1B(PTR)` (little-endian): `(va_header & 0x01) == 0x01`.
#[inline]
unsafe fn varatt_is_1b(ptr: *const u8) -> bool {
    (va_header_1b(ptr) & 0x01) == 0x01
}

/// `VARATT_IS_1B_E(PTR)` (little-endian): `va_header == 0x01` — the external
/// (TOAST pointer) form.
#[inline]
unsafe fn varatt_is_1b_e(ptr: *const u8) -> bool {
    va_header_1b(ptr) == 0x01
}

/// `VARSIZE_4B(PTR)` (little-endian): `(va_header >> 2) & 0x3FFFFFFF` — total
/// length including the 4-byte header.
#[inline]
unsafe fn varsize_4b(ptr: *const u8) -> usize {
    let word = (ptr as *const u32).read_unaligned();
    ((word >> 2) & 0x3FFF_FFFF) as usize
}

/// `VARSIZE_1B(PTR)` (little-endian): `(va_header >> 1) & 0x7F` — total length
/// of a short (1-byte-header) datum including its header.
#[inline]
unsafe fn varsize_1b(ptr: *const u8) -> usize {
    ((va_header_1b(ptr) >> 1) & 0x7F) as usize
}

/// `VARSIZE_EXTERNAL(PTR)` — for the external (1B-E) form, the on-disk pointer
/// length is `VARHDRSZ_EXTERNAL (2) + VARTAG_SIZE(tag)`. C reads the tag from
/// the second header byte; we only need the total span to hand to the detoast
/// seam, which re-reads the structure itself.
#[inline]
unsafe fn varsize_external_span(ptr: *const u8) -> usize {
    // VARTAG_EXTERNAL(PTR) == *(ptr + 1); VARTAG_SIZE maps the tag to the
    // toast-pointer struct size. The detoast seam reparses the bytes, so an
    // over-read is what we must avoid, not under-precision: mirror C's
    // VARSIZE_ANY by computing 2 + VARTAG_SIZE(tag).
    const VARTAG_INDIRECT: u8 = 1;
    const VARTAG_EXPANDED_RO: u8 = 2;
    const VARTAG_EXPANDED_RW: u8 = 3;
    const VARTAG_ONDISK: u8 = 18;
    // sizeof(varatt_indirect)=8, sizeof(varatt_expanded)=8,
    // sizeof(varatt_external)=16 (ONDISK). VARHDRSZ_EXTERNAL == 2.
    let tag = *ptr.add(1);
    let tag_size = match tag {
        VARTAG_INDIRECT => 8,
        VARTAG_EXPANDED_RO | VARTAG_EXPANDED_RW => 8,
        VARTAG_ONDISK => 16,
        other => other as usize,
    };
    2 + tag_size
}

/// `VARDATA_4B(PTR)` — payload just past the 4-byte header.
#[inline]
unsafe fn vardata_4b(ptr: *const u8) -> *const u8 {
    ptr.add(VARHDRSZ)
}

/// `VARDATA_1B(PTR)` — payload just past the 1-byte short header.
#[inline]
unsafe fn vardata_1b(ptr: *const u8) -> *const u8 {
    ptr.add(1)
}

// ---------------------------------------------------------------------------
// Owner-seam adapters. Each `backend-utils-adt-varlena-seams` declaration is
// installed from `init_seams()` and routed to its family body. The carrier
// (payload-bytes) families are reached directly; the value-carrying seams
// (`cstring_to_text` / `bytes_to_varlena` / `text_to_cstring`) come in two
// shapes:
//
// * The migrated `_v` variants take/return the canonical unified value
//   `DatumV<'mcx>` and hold the REAL conversion logic. A `text`/`bytea` varlena
//   is always pass-by-reference, so the value is a `Datum::ByRef` whose bytes
//   are the verbatim varlena image (4-byte header + payload). These are the
//   migration target the migrated consumers (execTuples / nodeTableFuncscan)
//   call.
//
// * The bare-word `types_datum::Datum` (`BareDatum`) variants are the
//   transitional shims still pinned by ~22 unmigrated consumers (the
//   `CStringGetTextDatum` / `TextDatumGetCString` macro shape). They are the
//   genuinely still-bare-word seam ABI edge: the result/argument is a raw
//   `struct varlena *` pointer word (`PointerGetDatum` / `DatumGetPointer`).
//   They forge the one bare word at that edge and otherwise reuse the `_v`
//   logic / the shared header-parsing helper, so no bare-word `Datum` flows
//   anywhere internal.
// ---------------------------------------------------------------------------

/// C: `cstring_to_text(s)` framed as a full-header `text` varlena image. The
/// keystone builds the header-less payload; `Varlena::from_image` stamps
/// `SET_VARSIZE` over the reserved 4-byte header. Returns the verbatim image
/// bytes (header + payload), charged to `mcx`.
fn build_text_varlena_image<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let payload = keystone::cstring_to_text(mcx, s)?;
    let mut image = mcx::vec_with_capacity_in(mcx, VARHDRSZ + payload.len())?;
    image.extend_from_slice(&[0u8; VARHDRSZ]); // reserved header (SET_VARSIZE).
    image.extend_from_slice(&payload);
    Ok(Varlena::from_image(image).into_image())
}

/// C: `palloc(len + VARHDRSZ)` + memcpy + `SET_VARSIZE(buf, len + VARHDRSZ)` —
/// the genfile.c `read_binary_file` idiom for wrapping raw bytes into a
/// `bytea`/`text` varlena. Returns the verbatim image bytes, charged to `mcx`.
fn build_bytea_varlena_image<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // The carrier shape is identical to `cstring_to_text` (header + verbatim
    // payload); the keystone copies the payload, `from_image` stamps the header.
    build_text_varlena_image(mcx, bytes)
}

/// Shared detoast + payload-slice of a live `struct varlena *` image. Used by
/// both `text_to_cstring` shapes: the `_v` form passes the pointer to the
/// canonical `ByRef` bytes; the bare-word shim passes the raw `DatumGetPointer`
/// word. Mirrors C `text_to_cstring`'s `pg_detoast_datum_packed` + copy of
/// `VARSIZE_ANY_EXHDR` payload bytes; external/compressed forms route through
/// the detoast seam exactly as the range ADT does on its by-ref args.
///
/// SAFETY: `ptr` must point at a live varlena image (full-, short-, or
/// external-header) valid for at least this call; the header byte(s) are always
/// present.
unsafe fn text_payload_from_ptr<'mcx>(mcx: Mcx<'mcx>, ptr: *const u8) -> PgResult<PgVec<'mcx, u8>> {
    if varatt_is_1b_e(ptr) {
        // External (TOAST) pointer: fetch+decompress into `mcx`, then the
        // detoasted copy is a plain 4B varlena.
        let span = varsize_external_span(ptr);
        let bytes = core::slice::from_raw_parts(ptr, span);
        let copy = backend_access_common_detoast_seams::detoast_attr::call(mcx, bytes)?;
        // detoast_attr returns the whole detoasted image (header + data);
        // copy out just the payload past its 4B header.
        mcx::slice_in(mcx, &copy[VARHDRSZ..])
    } else if varatt_is_1b(ptr) {
        // Short 1-byte-header inline datum (never compressed/external).
        let total = varsize_1b(ptr);
        let data = vardata_1b(ptr);
        let len = total - 1;
        mcx::slice_in(mcx, core::slice::from_raw_parts(data, len))
    } else if varatt_is_4b_u(ptr) {
        // Plain uncompressed 4-byte-header datum.
        let total = varsize_4b(ptr);
        let data = vardata_4b(ptr);
        let len = total - VARHDRSZ;
        mcx::slice_in(mcx, core::slice::from_raw_parts(data, len))
    } else {
        // 4B compressed (the only remaining extended inline form): detoast.
        let total = varsize_4b(ptr);
        let bytes = core::slice::from_raw_parts(ptr, total);
        let copy = backend_access_common_detoast_seams::detoast_attr::call(mcx, bytes)?;
        mcx::slice_in(mcx, &copy[VARHDRSZ..])
    }
}

/// Safe-slice twin of `text_payload_from_ptr` for an owned, fully-bounded
/// varlena byte image (the canonical `ByRef` bytes). Same header dispatch as the
/// pointer form, but reads the header and slices the payload through bounds-
/// checked slice operations instead of forging a raw pointer. Used by the `_v`
/// (canonical-value) `text_to_cstring`, where the image is already a `&[u8]`.
fn text_payload_from_bytes<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let header = image[0];
    if header == 0x01 {
        // VARATT_IS_1B_E: external (TOAST) pointer — fetch+decompress, then copy
        // the detoasted payload past its 4B header.
        // VARSIZE_EXTERNAL: VARHDRSZ_EXTERNAL(2) + VARTAG_SIZE(tag), tag = image[1].
        const VARTAG_INDIRECT: u8 = 1;
        const VARTAG_EXPANDED_RO: u8 = 2;
        const VARTAG_EXPANDED_RW: u8 = 3;
        const VARTAG_ONDISK: u8 = 18;
        let tag = image[1];
        let tag_size = match tag {
            VARTAG_INDIRECT => 8,
            VARTAG_EXPANDED_RO | VARTAG_EXPANDED_RW => 8,
            VARTAG_ONDISK => 16,
            other => other as usize,
        };
        let span = 2 + tag_size;
        let copy = backend_access_common_detoast_seams::detoast_attr::call(mcx, &image[..span])?;
        mcx::slice_in(mcx, &copy[VARHDRSZ..])
    } else if header & 0x01 == 0x01 {
        // VARATT_IS_1B: short 1-byte-header inline datum.
        let total = ((header >> 1) & 0x7F) as usize;
        mcx::slice_in(mcx, &image[1..total])
    } else if header & 0x03 == 0x00 {
        // VARATT_IS_4B_U: plain uncompressed 4-byte-header datum.
        let word = u32::from_le_bytes([image[0], image[1], image[2], image[3]]);
        let total = ((word >> 2) & 0x3FFF_FFFF) as usize;
        mcx::slice_in(mcx, &image[VARHDRSZ..total])
    } else {
        // 4B compressed: detoast the whole image, copy payload past 4B header.
        let word = u32::from_le_bytes([image[0], image[1], image[2], image[3]]);
        let total = ((word >> 2) & 0x3FFF_FFFF) as usize;
        let copy = backend_access_common_detoast_seams::detoast_attr::call(mcx, &image[..total])?;
        mcx::slice_in(mcx, &copy[VARHDRSZ..])
    }
}

/// C: `text_to_cstring` post-detoast tail — copy the detoasted payload out as a
/// NUL-free `PgString` in `mcx`. The seam's contract is a NUL-free `PgString`,
/// so the keystone's trailing C NUL is dropped.
fn text_payload_to_pgstring<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<PgString<'mcx>> {
    let cstr = keystone::text_to_cstring(mcx, payload)?;
    let body = &cstr[..cstr.len().saturating_sub(1)];
    PgString::from_str_in(
        core::str::from_utf8(body).expect("text payload is database-encoding bytes"),
        mcx,
    )
}

// --- Canonical (`_v`) value seams — the migration target, holding the logic. ---

fn seam_cstring_to_text_v<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<DatumV<'mcx>> {
    // C: cstring_to_text(s). A `text` varlena is pass-by-reference, so the
    // canonical value is a `Datum::ByRef` holding the freshly built image.
    Ok(DatumV::ByRef(build_text_varlena_image(mcx, s.as_bytes())?))
}

fn seam_bytes_to_varlena_v<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<DatumV<'mcx>> {
    // C: genfile.c read_binary_file varlena idiom. Pass-by-reference => `ByRef`.
    Ok(DatumV::ByRef(build_bytea_varlena_image(mcx, bytes)?))
}

fn seam_text_to_cstring_v<'mcx>(mcx: Mcx<'mcx>, d: &DatumV<'_>) -> PgResult<PgString<'mcx>> {
    // C: text_to_cstring((text *) DatumGetPointer(d)). The canonical `text`
    // value is a `Datum::ByRef` whose bytes are the verbatim varlena image; we
    // parse its header (detoasting external/compressed forms) off a pointer into
    // those bytes — no bare-word `Datum` involved.
    let image = d.as_ref_bytes();
    // `image` is a fully-owned, bounded varlena byte image (header present);
    // parse it through bounds-checked slice ops — no raw pointer needed.
    let payload = text_payload_from_bytes(mcx, image)?;
    text_payload_to_pgstring(mcx, &payload)
}

// --- Bare-word shims — the still-pinned ABI edge; forge one word, reuse logic. ---

fn seam_cstring_to_text<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<BareDatum> {
    // C: CStringGetTextDatum(s) = PointerGetDatum(cstring_to_text(s)). Build the
    // image via the shared helper, then `PointerGetDatum` is the raw image
    // pointer word (`BareDatum::from_usize`) — the one genuinely-bare-word ABI
    // edge this transitional shim exists for. The image is `leak`ed into `mcx`,
    // so it stays valid for `'mcx` exactly as C's palloc'd `text *` lives in the
    // current memory context until reset (the caller's `pfree` is a no-op under
    // context-scoped ownership).
    let image = build_text_varlena_image(mcx, s.as_bytes())?;
    let leaked = image.leak();
    Ok(BareDatum::from_usize(leaked.as_ptr() as usize))
}

fn seam_bytes_to_varlena<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<BareDatum> {
    // C: PointerGetDatum of the built `bytea`/`text` varlena (genfile.c). Same
    // bare-word ABI edge as `cstring_to_text`.
    let image = build_bytea_varlena_image(mcx, bytes)?;
    let leaked = image.leak();
    Ok(BareDatum::from_usize(leaked.as_ptr() as usize))
}

fn seam_text_to_cstring<'mcx>(mcx: Mcx<'mcx>, d: BareDatum) -> PgResult<PgString<'mcx>> {
    // C: text_to_cstring((text *) DatumGetPointer(d)). The bare-word Datum
    // carries the raw `struct varlena *` pointer word (`DatumGetPointer` ==
    // `BareDatum::as_usize`) — the genuinely still-bare-word ABI edge. Read the
    // varatt header off that pointer via the shared helper, then copy out.
    let ptr = d.as_usize() as *const u8;
    // SAFETY: a by-reference `text` Datum points at a live `struct varlena`
    // image owned by the caller for at least this call.
    let payload = unsafe { text_payload_from_ptr(mcx, ptr)? };
    text_payload_to_pgstring(mcx, &payload)
}

fn seam_varstr_cmp(arg1: &[u8], arg2: &[u8], collid: types_core::Oid) -> PgResult<i32> {
    comparison::varstr_cmp(arg1, arg2, collid)
}

fn seam_split_identifier_string<'mcx>(
    mcx: Mcx<'mcx>,
    raw: &str,
    separator: char,
) -> PgResult<Option<PgVec<'mcx, PgString<'mcx>>>> {
    split_format::split_identifier_string(mcx, raw, separator)
}

fn seam_split_directories_string<'mcx>(
    mcx: Mcx<'mcx>,
    rawstring: &str,
) -> PgResult<Option<PgVec<'mcx, PgString<'mcx>>>> {
    split_format::split_directories_string(mcx, rawstring)
}

fn seam_text_to_qualified_name_list<'mcx>(
    mcx: Mcx<'mcx>,
    textval: &[u8],
) -> PgResult<PgVec<'mcx, PgString<'mcx>>> {
    split_format::text_to_qualified_name_list(mcx, textval)
}

fn seam_text_substr<'mcx>(
    mcx: Mcx<'mcx>,
    str: &[u8],
    start: i32,
    length: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    // C: text_substr -> text_substring(str, start, length, false).
    position_ops::text_substring(mcx, str, start, length, false)
}

#[allow(clippy::too_many_arguments)]
fn seam_replace_text_regexp<'mcx>(
    mcx: Mcx<'mcx>,
    src_text: &[u8],
    pattern_text: &[u8],
    replace_text: &[u8],
    cflags: i32,
    collation: types_core::Oid,
    search_start: i32,
    n: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    replace_regexp::replace_text_regexp(
        mcx,
        src_text,
        pattern_text,
        replace_text,
        cflags,
        collation,
        search_start,
        n,
    )
}

/// Install every `backend-utils-adt-varlena-seams` declaration. Wired into
/// `seams-init::init_all()`. The set is the full superset of the declared
/// seams so the declared-seams-are-set recurrence guard passes.
pub fn init_seams() {
    use backend_utils_adt_varlena_seams as s;

    // Register the boot-critical text / name<->text comparison builtins into the
    // fmgr fast-path table (C: `fmgr_builtins[]`) so `fmgr_isbuiltin` resolves
    // `texteq`/`bttextcmp` and the name<->text comparators during early catalog
    // scans without recursing into the not-yet-built syscache.
    fmgr_builtins::register_varlena_compare_builtins();

    // Register the rest of varlena.c's fmgr_builtins[] rows whose value cores
    // are ported and expressible at the fmgr boundary (text/bytea I/O, length,
    // concat, substring/position/overlay, replace/split_part, bytea comparison
    // + int casts, text_pattern_ops, base conversions, unicode/unistr).
    fmgr_builtins::register_varlena_more_builtins();

    // The broadest by-ref fan-out leg: additional text/bytea/unknown by-reference
    // builtins whose value cores are ported but were not yet in the fast-path
    // table (bytea get/set byte/bit, text substring/overlay, unknown I/O).
    fmgr_builtins::register_varlena_text_bytea_byref_builtins();

    // The string_to_array / array_to_string text<->text[] bridge builtins
    // (text_to_array{,_null} / array_to_text{,_null}); the array de/construction
    // is the already-installed arrayfuncs owner seam.
    fmgr_builtins::register_varlena_array_string_builtins();

    // The variadic-`any` text builders concat/concat_ws/format/format_nv
    // (varlena.c text_concat/text_concat_ws/text_format/text_format_nv). Each
    // stringifies its arguments through their type output functions and the
    // arrayfuncs deconstruct seam for the `VARIADIC array` form.
    fmgr_builtins::register_varlena_format_builtins();

    // The `internal`-transtype `string_agg` aggregate transition/final functions
    // (varlena.c) so `SELECT string_agg(x::text, ',') FROM t` resolves them by
    // OID. setup_peragg_finalfn resolves the finalfn via `fmgr_info` at
    // ExecInitAgg, so an unregistered builtin would abort the node before any
    // row is processed.
    string_agg::register_string_agg_builtins();

    // The `bytea_output` GUC variable accessor (varlena.c owns the storage;
    // guc_tables.c binds the config_enum's `variable` pointer here). The GUC
    // machinery reads/writes through these accessors and `byteaout` reads the
    // same store. Mirrors the aio.c io_method pattern.
    backend_utils_misc_guc_tables::vars::bytea_output.install(
        backend_utils_misc_guc_tables::GucVarAccessors {
            get: get_bytea_output,
            set: set_bytea_output,
        },
    );

    // Canonical value seams (migration target) + their bare-word transitional
    // shims (still pinned by unmigrated consumers + the genfile.c bytea path).
    s::cstring_to_text::set(seam_cstring_to_text);
    s::cstring_to_text_v::set(seam_cstring_to_text_v);
    s::bytes_to_varlena::set(seam_bytes_to_varlena);
    s::bytes_to_varlena_v::set(seam_bytes_to_varlena_v);
    s::text_to_cstring::set(seam_text_to_cstring);
    s::text_to_cstring_v::set(seam_text_to_cstring_v);
    s::varstr_cmp::set(seam_varstr_cmp);
    s::split_identifier_string::set(seam_split_identifier_string);
    s::split_directories_string::set(seam_split_directories_string);
    s::text_to_qualified_name_list::set(seam_text_to_qualified_name_list);
    s::text_substr::set(seam_text_substr);
    s::replace_text_regexp::set(seam_replace_text_regexp);
}
