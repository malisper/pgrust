//! FAMILY: SortSupport + abbreviated keys.
//!
//! `bttextsortsupport`/`bttext_pattern_sortsupport`/`bytea_sortsupport`, the
//! `varstr_sortsupport` installer, the comparator cores (`varstrfastcmp_c`,
//! `bpcharfastcmp_c`, `namefastcmp_c`, the `*_locale` variants), and the
//! abbreviated-key machinery (`varstr_abbrev_convert`/`varstr_abbrev_abort`).
//!
//! The pure comparator cores operate on byte slices (matching the idiomatic
//! shape). The full `SortSupportData` substrate + HyperLogLog
//! abbreviated-key cardinality wiring is built against
//! `types-sortsupport` / `backend-utils-sort-sortsupport-seams` and the
//! locale providers (`pg_strxfrm` etc.) when this family is filled.
//!
//! Depends on the keystone for [`VarStringSortSupport`](crate::keystone).

#![allow(unused_variables)]

/// C: `varstrfastcmp_c(Datum x, Datum y, SortSupport ssup)` — the C-collation
/// comparator core: `memcmp` + length tiebreak. Pure on payload bytes.
pub fn varstrfastcmp_c(a: &[u8], b: &[u8]) -> i32 {
    todo!("sortsupport family: port varstrfastcmp_c")
}

/// C: `bpcharfastcmp_c(Datum x, Datum y, SortSupport ssup)` — bpchar core,
/// trims trailing blanks before the C-collation compare.
pub fn bpcharfastcmp_c(a: &[u8], b: &[u8]) -> i32 {
    todo!("sortsupport family: port bpcharfastcmp_c")
}

/// C: the bpchar trailing-blank-trim helper used by [`bpcharfastcmp_c`].
pub fn bpchartruelen(s: &[u8]) -> usize {
    todo!("sortsupport family: port bpchartruelen")
}

/// C: `namefastcmp_c(Datum x, Datum y, SortSupport ssup)` — `name` strncmp
/// over the fixed-width NUL-terminated buffers.
pub fn namefastcmp_c(a: &[u8; crate::keystone::NAMEDATALEN], b: &[u8; crate::keystone::NAMEDATALEN]) -> i32 {
    todo!("sortsupport family: port namefastcmp_c")
}

/// C: `bttextsortsupport(PG_FUNCTION_ARGS)` — install the text comparator/
/// abbreviator into the `SortSupport` slot.
pub fn bttextsortsupport(ssup: &mut types_sortsupport::SortSupportData<'_>, collid: types_core::Oid) -> types_error::PgResult<()> {
    todo!("sortsupport family: port bttextsortsupport (varstr_sortsupport)")
}

/// C: `bttext_pattern_sortsupport(PG_FUNCTION_ARGS)` — text_pattern_ops sort
/// support (always C-collation core).
pub fn bttext_pattern_sortsupport(ssup: &mut types_sortsupport::SortSupportData<'_>) -> types_error::PgResult<()> {
    todo!("sortsupport family: port bttext_pattern_sortsupport")
}

/// C: `bytea_sortsupport(PG_FUNCTION_ARGS)` — bytea sort support.
pub fn bytea_sortsupport(ssup: &mut types_sortsupport::SortSupportData<'_>) -> types_error::PgResult<()> {
    todo!("sortsupport family: port bytea_sortsupport")
}
