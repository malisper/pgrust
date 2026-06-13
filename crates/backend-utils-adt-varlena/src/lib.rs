//! `utils/adt/varlena.c` — the variable-length built-in scalar types
//! (`text`, `bytea`, `name`, `unknown`): I/O, comparison + SortSupport,
//! substring/position/overlay, encoding/escape, split/join/format, and the
//! `string_agg` aggregate.
//!
//! `varlena.c` is ~6900 lines — too large for one porting pass — so it is
//! **decomposed into a keystone + 8 function-cluster families**. This crate is
//! the decomposition scaffold: the [`keystone`] family (the shared carrier
//! model, ABI/lifetime conventions, and foundational pure free functions) is
//! ported for real so the scaffold compiles; every other family module fixes
//! its public signatures with `todo!()` bodies for a follow-up porting pass.
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
use types_datum::Datum;
use types_error::PgResult;

// ---------------------------------------------------------------------------
// Owner-seam adapters. Each `backend-utils-adt-varlena-seams` declaration is
// installed from `init_seams()` and routed to its family body. The carrier
// (payload-bytes) families are reached directly; the two Datum-framed seams
// (`cstring_to_text` / `text_to_cstring`) wrap the keystone carrier converters
// at the fmgr/Datum boundary (the framing itself is the project-wide fmgr
// deferral — completed when this unit's Datum boundary lands).
// ---------------------------------------------------------------------------

fn seam_cstring_to_text<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum> {
    // C: CStringGetTextDatum(s) = PointerGetDatum(cstring_to_text(s)). The
    // payload is built by the keystone; the Datum header framing is the fmgr
    // boundary's job.
    let _payload = keystone::cstring_to_text(mcx, s.as_bytes())?;
    todo!("varlena: frame cstring_to_text payload as a text Datum (fmgr boundary)")
}

fn seam_text_to_cstring<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgString<'mcx>> {
    // C: text_to_cstring((text *) DatumGetPointer(d)). The Datum -> payload
    // detoast/unframe is the fmgr boundary's job; then the keystone copies it.
    let _ = (mcx, d);
    todo!("varlena: unframe text Datum to payload then keystone::text_to_cstring (fmgr boundary)")
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
