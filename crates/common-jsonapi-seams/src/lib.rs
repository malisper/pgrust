//! Seam declarations for the `common-jsonapi` unit (`src/common/jsonapi.c`):
//! the recursive-descent JSON lexer/parser (`makeJsonLexContext`, `json_lex`,
//! `pg_parse_json` and the `JsonSemAction` callbacks) plus `json.c`'s
//! `json_errsave_error` reporting bridge.
//!
//! `json.c`'s `json_in` / `json_recv` / `json_typeof` / `json_validate` reach
//! the parser through these. The owning unit installs them from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_error::PgResult;
use types_json::{JsonParseErrorType, JsonTokenType};

seam_core::seam!(
    /// `makeJsonLexContext(&lex, json, false)` then
    /// `pg_parse_json(&lex, &nullSemAction)` — validate that `json` is
    /// well-formed JSON, returning `JSON_SUCCESS` on success or the first
    /// `JsonParseErrorType` encountered. Engine behind `json_in` / `json_recv`
    /// / `json_validate` (without unique-key checking).
    pub fn parse_validate(json: &[u8]) -> JsonParseErrorType
);

seam_core::seam!(
    /// `pg_parse_json` driven by the `json_unique_*` semantic actions —
    /// validate `json` *and* report whether every object's keys are unique.
    /// Returns `(result, unique)`; `unique` is meaningful only when
    /// `result == JSON_SUCCESS`. Drives `json_validate(check_unique_keys)`.
    pub fn parse_validate_unique(json: &[u8]) -> (JsonParseErrorType, bool)
);

seam_core::seam!(
    /// `makeJsonLexContext(&lex, json, false)` then a single `json_lex` — lex
    /// exactly the first token and report its type. Returns
    /// `(result, token_type)`; `token_type` is meaningful only when
    /// `result == JSON_SUCCESS`. Drives `json_typeof`.
    pub fn lex_first_token(json: &[u8]) -> (JsonParseErrorType, JsonTokenType)
);

seam_core::seam!(
    /// `json_errsave_error(error, lex, escontext)` — convert a non-success
    /// `JsonParseErrorType` into the user-facing `ereport`/`errsave` error with
    /// the backend's message text and SQLSTATE. `json` is the original input
    /// (for context/position rendering). On a soft-error context the provider
    /// swallows the error and returns `Ok(())`; otherwise it raises the hard
    /// error as `Err`.
    pub fn errsave_error(error: JsonParseErrorType, json: &[u8]) -> PgResult<()>
);
