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
pub mod misc_encoding;
pub mod name_pattern;
pub mod position_ops;
pub mod replace_regexp;
pub mod sortsupport;
pub mod split_format;
pub mod wire_io;

use mcx::{Mcx, PgString, PgVec};
use types_datum::{Datum, Varlena};
use types_error::PgResult;

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
// (payload-bytes) families are reached directly; the two Datum-framed seams
// (`cstring_to_text` / `text_to_cstring`) wrap the keystone carrier converters
// at the fmgr/Datum boundary, framing/unframing through the layered
// `types_datum::Varlena` header model: `cstring_to_text` builds a full-header
// `text` image and returns its pointer word (`PointerGetDatum`);
// `text_to_cstring` reads the varatt header off the `DatumGetPointer` pointer
// (detoasting external/compressed forms via the detoast seam) and copies the
// payload out.
// ---------------------------------------------------------------------------

fn seam_cstring_to_text<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum> {
    // C: CStringGetTextDatum(s) = PointerGetDatum(cstring_to_text(s)). The
    // keystone builds the payload bytes; framing them as a full-header `text`
    // varlena image is the layered Datum boundary. We reserve VARHDRSZ and
    // hand the image to `Varlena::from_image`, which stamps SET_VARSIZE, then
    // `PointerGetDatum` is the raw image pointer word (`Datum::from_usize`).
    // The image is `leak`ed into `mcx`, so it stays valid for `'mcx` exactly
    // as C's palloc'd `text *` lives in the current memory context until the
    // context is reset (the caller's `pfree(DatumGetPointer(...))` is a no-op
    // under context-scoped ownership).
    let payload = keystone::cstring_to_text(mcx, s.as_bytes())?;
    let mut image = mcx::vec_with_capacity_in(mcx, VARHDRSZ + payload.len())?;
    image.extend_from_slice(&[0u8; VARHDRSZ]); // reserved header (SET_VARSIZE).
    image.extend_from_slice(&payload);
    let varlena = Varlena::from_image(image);
    let leaked = varlena.into_image().leak();
    Ok(Datum::from_usize(leaked.as_ptr() as usize))
}

fn seam_text_to_cstring<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgString<'mcx>> {
    // C: text_to_cstring((text *) DatumGetPointer(d)). `text_to_cstring`
    // first does `pg_detoast_datum_packed(DatumGetPointer(t))`, then copies
    // `VARSIZE_ANY_EXHDR` payload bytes out as a NUL-terminated cstring. Here
    // the Datum carries the raw `struct varlena *` pointer word
    // (`DatumGetPointer` == `Datum::as_usize`); we read the varatt header to
    // slice the payload, detoasting external/compressed forms through the
    // detoast seam exactly as the range ADT does on its by-ref args.
    let ptr = d.as_usize() as *const u8;
    // SAFETY: a by-reference `text` Datum points at a live `struct varlena`
    // image (full-, short-, or external-header) owned by the caller for at
    // least this call; the header byte(s) are always present.
    let payload: PgVec<'mcx, u8> = unsafe {
        if varatt_is_1b_e(ptr) {
            // External (TOAST) pointer: fetch+decompress into `mcx`, then the
            // detoasted copy is a plain 4B varlena.
            let span = varsize_external_span(ptr);
            let bytes = core::slice::from_raw_parts(ptr, span);
            let copy = backend_access_common_detoast_seams::detoast_attr::call(mcx, bytes)?;
            // detoast_attr returns the whole detoasted image (header + data);
            // copy out just the payload past its 4B header.
            mcx::slice_in(mcx, &copy[VARHDRSZ..])?
        } else if varatt_is_1b(ptr) {
            // Short 1-byte-header inline datum (never compressed/external).
            let total = varsize_1b(ptr);
            let data = vardata_1b(ptr);
            let len = total - 1;
            mcx::slice_in(mcx, core::slice::from_raw_parts(data, len))?
        } else if varatt_is_4b_u(ptr) {
            // Plain uncompressed 4-byte-header datum.
            let total = varsize_4b(ptr);
            let data = vardata_4b(ptr);
            let len = total - VARHDRSZ;
            mcx::slice_in(mcx, core::slice::from_raw_parts(data, len))?
        } else {
            // 4B compressed (the only remaining extended inline form): detoast.
            let total = varsize_4b(ptr);
            let bytes = core::slice::from_raw_parts(ptr, total);
            let copy = backend_access_common_detoast_seams::detoast_attr::call(mcx, bytes)?;
            mcx::slice_in(mcx, &copy[VARHDRSZ..])?
        }
    };
    // C: text_to_cstring appends a trailing NUL; the seam's contract is a
    // NUL-free `PgString`, so hand the keystone the detoasted payload and copy
    // it into a `PgString` (the keystone's vector carries the C trailing NUL,
    // which `PgString` does not store).
    let cstr = keystone::text_to_cstring(mcx, &payload)?;
    // Drop the keystone's trailing NUL (last byte) for the String view.
    let body = &cstr[..cstr.len().saturating_sub(1)];
    PgString::from_str_in(
        core::str::from_utf8(body).expect("text payload is database-encoding bytes"),
        mcx,
    )
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
    s::cstring_to_text::set(seam_cstring_to_text);
    s::text_to_cstring::set(seam_text_to_cstring);
    s::varstr_cmp::set(seam_varstr_cmp);
    s::split_identifier_string::set(seam_split_identifier_string);
    s::split_directories_string::set(seam_split_directories_string);
    s::text_to_qualified_name_list::set(seam_text_to_qualified_name_list);
    s::text_substr::set(seam_text_substr);
    s::replace_text_regexp::set(seam_replace_text_regexp);
}
