//! Seam declarations for the `common-jsonapi` unit (`src/common/jsonapi.c`):
//! the recursive-descent JSON lexer/parser (`makeJsonLexContext`, `json_lex`,
//! `pg_parse_json` and the `JsonSemAction` callbacks) plus `json.c`'s
//! `json_errsave_error` reporting bridge.
//!
//! `json.c`'s `json_in` / `json_recv` / `json_typeof` / `json_validate` reach
//! the parser through these. The owning unit installs them from its
//! `init_seams()` when it lands; until then a call panics loudly.

use ::types_json::{JsonLexContext, JsonParseErrorType, JsonSemAction, JsonTokenType};
use ::types_error::{PgResult, SoftErrorContext};

extern crate alloc;
use alloc::vec::Vec;

seam_core::seam!(
    /// `makeJsonLexContext(&lex, json, false)` then
    /// `pg_parse_json(&lex, &nullSemAction)` — validate that `json` is
    /// well-formed JSON, returning `JSON_SUCCESS` on success or the first
    /// `JsonParseErrorType` encountered. Engine behind `json_in` / `json_recv`
    /// / `json_validate` (without unique-key checking).
    ///
    /// Returns `Err` only for the recursive descent's `check_stack_depth()`
    /// `ereport(ERROR, "stack depth limit exceeded")`, which C raises
    /// immediately; a malformed-but-shallow input is reported through the
    /// returned `JsonParseErrorType` (rendered by `errsave_error`), not `Err`.
    pub fn parse_validate(json: &[u8]) -> PgResult<JsonParseErrorType>
);

seam_core::seam!(
    /// `pg_parse_json` driven by the `json_unique_*` semantic actions —
    /// validate `json` *and* report whether every object's keys are unique.
    /// Returns `(result, unique)`; `unique` is meaningful only when
    /// `result == JSON_SUCCESS`. Drives `json_validate(check_unique_keys)`.
    ///
    /// `Err` only for the `check_stack_depth()` hard error (see
    /// [`parse_validate`]); shallow parse failures ride the returned tuple.
    pub fn parse_validate_unique(json: &[u8]) -> PgResult<(JsonParseErrorType, bool)>
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
    /// (for context/position rendering). When `escontext` is `Some` (a live
    /// soft-error sink), the provider routes the error into it and returns
    /// `Ok(())`; with `None` it raises the hard error as `Err`.
    pub fn errsave_error<'a>(
        error: JsonParseErrorType,
        json: &[u8],
        need_escapes: bool,
        escontext: Option<&'a mut SoftErrorContext>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `makeJsonLexContext(&lex, json, need_escapes)` then
    /// `pg_parse_json(&lex, sem)` (common/jsonapi.c) — run the recursive-descent
    /// JSON parser over `json` in `encoding`, invoking the caller-supplied
    /// [`JsonSemAction`] callbacks (the SAX driver). Returns the first
    /// non-success [`JsonParseErrorType`] (`JSON_SUCCESS` on a clean parse); a
    /// callback that raises propagates as `Err`. The live `JsonLexContext` is
    /// threaded to each callback as the parser advances. This is the parse
    /// driver `jsonfuncs.c`'s json-text entry points (object_keys / each /
    /// elements / get_worker / strip_nulls / populate / iterate / transform)
    /// build their `sem` table for.
    pub fn pg_parse_json<'a>(
        json: &[u8],
        encoding: i32,
        need_escapes: bool,
        sem: &mut JsonSemAction<'a>,
    ) -> PgResult<JsonParseErrorType>
);

seam_core::seam!(
    /// `json_lex(&lex)` over a freshly-`makeJsonLexContext`'d `json` — lex
    /// exactly the first token and report `(result, JsonLexContext snapshot)`.
    /// Drives `json_get_first_token`. The snapshot's `token_type` is meaningful
    /// only when `result == JSON_SUCCESS`.
    pub fn json_lex_first(json: &[u8], encoding: i32) -> PgResult<(JsonParseErrorType, JsonLexContext)>
);

seam_core::seam!(
    /// `json_errdetail(error, lex)` (common/jsonapi.c) — the human-readable
    /// detail string for a parse error, in server encoding. `lex` is provided
    /// as the snapshot the error was detected at.
    pub fn json_errdetail(error: JsonParseErrorType, lex: &JsonLexContext) -> PgResult<Vec<u8>>
);

seam_core::seam!(
    /// `json_count_array_elements(&lex)` (common/jsonapi.c) — count the elements
    /// of the top-level array `lex` is positioned at (a lookahead parse).
    /// Drives `json_array_length`. `Err` carries the parse `ereport`.
    pub fn json_count_array_elements(json: &[u8], encoding: i32) -> PgResult<i32>
);

seam_core::seam!(
    /// `GetDatabaseEncoding()` (mb/mbutils.c) — the current database encoding,
    /// passed to `makeJsonLexContext`. (Backend-global; read by the owner.)
    pub fn get_database_encoding() -> i32
);

seam_core::seam!(
    /// `pg_mblen(s)` (mb/mbutils.c) — the byte length of the multibyte
    /// character starting `s`. Used by `report_json_context` to advance over
    /// multibyte characters.
    pub fn pg_mblen(s: &[u8]) -> usize
);
