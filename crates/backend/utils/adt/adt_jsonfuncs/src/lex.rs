//! JSON lexer / parse-error glue (`jsonfuncs.c`): `makeJsonLexContext`
//! (jsonfuncs.c:539), `json_errsave_error` (jsonfuncs.c:640),
//! `report_json_context` (jsonfuncs.c:676), `json_get_first_token`
//! (jsonfuncs.c:5971).
//!
//! In this repo the lexer is owned by `common/jsonapi.c` (unported): the json
//! (text) drivers reach the recursive-descent parser/lexer through
//! `common-jsonapi-seams`, so there is no live `JsonLexContext` handle held
//! here. `makeJsonLexContext` is absorbed (drivers call `pg_parse_json` /
//! `json_lex_first` directly); the position fields `report_json_context` reads
//! arrive as byte offsets into `lex.input`, so the C `const char *` pointer
//! arithmetic becomes index arithmetic.

use utils_error::ereport;
use types_error::error::{ERROR, ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_UNTRANSLATABLE_CHARACTER};
use types_error::{PgError, PgResult, SoftErrorContext};
use types_json::{JsonLexContext, JsonParseErrorType, JsonTokenType};

use alloc::string::String;

/// `IS_HIGHBIT_SET(c)` (c.h): the byte has its high bit set.
const HIGHBIT: u8 = 0x80;

/// `json_errsave_error` (jsonfuncs.c:640): report a JSON parse error.
///
/// Converts a non-success `JsonParseErrorType` detected by `lex` into the
/// user-facing soft/hard error with the exact message text, SQLSTATE and
/// `CONTEXT:` line PostgreSQL raises. With a soft-error context the error is
/// routed into it; otherwise it is raised.
pub fn json_errsave_error(
    error: JsonParseErrorType,
    lex: &JsonLexContext,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<()> {
    // jsonfuncs.c:644
    if error == JsonParseErrorType::JSON_UNICODE_HIGH_ESCAPE
        || error == JsonParseErrorType::JSON_UNICODE_UNTRANSLATABLE
        || error == JsonParseErrorType::JSON_UNICODE_CODE_POINT_ZERO
    {
        // jsonfuncs.c:647-651
        let detail = jsonapi_seams::json_errdetail::call(error, lex)?;
        let context = report_json_context(lex)?;
        let err = ereport(ERROR)
            .errcode(ERRCODE_UNTRANSLATABLE_CHARACTER)
            .errmsg("unsupported Unicode escape sequence")
            .errdetail_internal(bytes_to_string(&detail))
            .errcontext_msg(context)
            .into_error();
        errsave(escontext, err)
    } else if error == JsonParseErrorType::JSON_SEM_ACTION_FAILED {
        // jsonfuncs.c:652-657: semantic action function had better have
        // reported something.
        if !soft_error_occurred(escontext.as_deref()) {
            // elog(ERROR, "...")
            return Err(ereport(ERROR)
                .errmsg_internal(
                    "JSON semantic action function did not provide error information",
                )
                .into_error());
        }
        Ok(())
    } else {
        // jsonfuncs.c:659-663
        let detail = jsonapi_seams::json_errdetail::call(error, lex)?;
        let context = report_json_context(lex)?;
        let err = ereport(ERROR)
            .errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
            .errmsg(format!("invalid input syntax for type {}", "json"))
            .errdetail_internal(bytes_to_string(&detail))
            .errcontext_msg(context)
            .into_error();
        errsave(escontext, err)
    }
}

/// `report_json_context` (jsonfuncs.c:676): render the `CONTEXT: JSON data,
/// line N: ...excerpt...` line pointing at the spot where the lexer detected the
/// error.
///
/// `lex->token_terminator` identifies the error spot. The C return value is
/// meaningless (it is only non-`void` so the call can sit inside `ereport()`);
/// here we return the formatted context string the caller threads into
/// `.errcontext_msg(...)`. The C `const char *` position fields arrive as byte
/// offsets into `lex.input`, so `context_end - context_start`, `*context_start`
/// and the `pg_mblen` advance become index arithmetic.
fn report_json_context(lex: &JsonLexContext) -> PgResult<String> {
    // Choose boundaries for the part of the input we will display.
    // jsonfuncs.c:688-690
    let line_start = lex.line_start;
    let mut context_start = line_start;
    let context_end = lex.token_terminator;
    debug_assert!(context_end >= context_start); // Assert(context_end >= context_start)

    // Advance until we are close enough to context_end. (jsonfuncs.c:694-701)
    while context_end - context_start >= 50 {
        // Advance to next multibyte character.
        if (lex.byte_at(context_start) & HIGHBIT) != 0 {
            // pg_mblen(context_start)
            context_start += jsonapi_seams::pg_mblen::call(&lex.input[context_start..]);
        } else {
            context_start += 1;
        }
    }

    // We add "..." to indicate that the excerpt doesn't start at the beginning
    // of the line ... but if we're within 3 characters of the beginning of the
    // line, we might as well just show the whole line. (jsonfuncs.c:708-709)
    if context_start - line_start <= 3 {
        context_start = line_start;
    }

    // Get a copy of the data to present. (jsonfuncs.c:712-715)
    let ctxtlen = context_end - context_start;
    let ctxt = bytes_to_string(&lex.input[context_start..context_start + ctxtlen]);

    // Show the context, prefixing "..." if not starting at start of line, and
    // suffixing "..." if not ending at end of line. (jsonfuncs.c:721-724)
    let prefix = if context_start > line_start { "..." } else { "" };
    // C: `context_end - lex->input < lex->input_length`; here `context_end` is
    // already an offset from `lex->input`.
    let suffix = if lex.token_type != JsonTokenType::JSON_TOKEN_END
        && context_end < lex.input_length
        && lex.byte_at(context_end) != b'\n'
        && lex.byte_at(context_end) != b'\r'
    {
        "..."
    } else {
        ""
    };

    // errcontext("JSON data, line %d: %s%s%s", ...) (jsonfuncs.c:726-727)
    Ok(format!(
        "JSON data, line {}: {}{}{}",
        lex.line_number, prefix, ctxt, suffix
    ))
}

/// `json_get_first_token` (jsonfuncs.c:5971): lex exactly one token from `json`
/// and return its [`JsonTokenType`].
///
/// On a parse error this raises through `json_errsave_error` when `throw_error`
/// is set, otherwise it returns `JSON_TOKEN_INVALID` (invalid json). The
/// `makeJsonLexContext` + `json_lex` pair is the `json_lex_first` seam.
pub fn json_get_first_token(json: &[u8], throw_error: bool) -> PgResult<JsonTokenType> {
    // makeJsonLexContext(&lex, json, false); result = json_lex(&lex);
    let encoding = jsonapi_seams::get_database_encoding::call();
    let (result, lex) = jsonapi_seams::json_lex_first::call(json, encoding)?;

    // jsonfuncs.c:5982-5983
    if result == JsonParseErrorType::JSON_SUCCESS {
        return Ok(lex.token_type);
    }

    // jsonfuncs.c:5985-5986
    if throw_error {
        json_errsave_error(result, &lex, None)?;
    }

    // jsonfuncs.c:5988: invalid json
    Ok(JsonTokenType::JSON_TOKEN_INVALID)
}

/// `errsave(escontext, ...)`: route a built `PgError` softly into `escontext`
/// when present (matching `SoftErrorContext`'s save semantics), else raise it.
fn errsave(escontext: Option<&mut SoftErrorContext>, err: PgError) -> PgResult<()> {
    if let Some(ec) = escontext {
        ec.mark_error_occurred();
        if ec.details_wanted() {
            ec.save(err);
        }
        Ok(())
    } else {
        Err(err)
    }
}

/// `SOFT_ERROR_OCCURRED(escontext)` (elog.h): true iff a soft-error context is
/// present and an error has already been captured into it.
fn soft_error_occurred(escontext: Option<&SoftErrorContext>) -> bool {
    escontext.map(|c| c.error_occurred()).unwrap_or(false)
}

/// Render the seam-returned server-encoded message/detail bytes for an error
/// builder; like the C `errdetail_internal("%s", ...)`/`errcontext("... %s ...")`
/// they are passed straight through, here decoded losslessly into the owned
/// `String` the idiomatic error builder takes.
fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}
