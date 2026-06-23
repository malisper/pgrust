//! Seam installation for `backend-utils-adt-jsonpath-gram`.
//!
//! This unit owns the INWARD `parse` seam declared in
//! `backend-utils-adt-jsonpath-gram-seams` — the `parsejsonpath` entry point
//! that `backend-utils-adt-jsonpath`'s `jsonPathFromCstring` calls to turn
//! jsonpath text into a `JsonPathParseResult`. We install it here so the
//! contract is wired the moment this crate is in the build.
//!
//! Every OUTWARD dependency the grammar reaches (`numeric_in` / `numeric_uminus`
//! from numeric.c, `pg_regcomp` / `pg_regerror` from the regex engine,
//! `pg_mb2wchar_with_len` from mbutils, `pg_strtoint32` from numutils, and the
//! scanner's token stream) is a real ported function called directly, not a
//! seam this crate must install.

use ::types_error::{PgResult, SoftErrorContext};
use ::types_jsonpath::parse::JsonPathParseResult;

/// Adapter for the `parse` inward seam (C: `parsejsonpath`).
fn seam_parse(
    str: &[u8],
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<JsonPathParseResult>> {
    crate::parsejsonpath(str, escontext)
}

/// Install this unit's inward seams.
pub fn init_seams() {
    jsonpath_gram_seams::parse::set(seam_parse);
}
