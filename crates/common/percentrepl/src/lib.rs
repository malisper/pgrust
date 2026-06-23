//! Port of PostgreSQL's `src/common/percentrepl.c` — replace percent-letter
//! placeholders in strings.
//!
//! C builds the result in a `StringInfo` (`initStringInfo` /
//! `appendStringInfo*`) and returns the `palloc`'d buffer. Here the result is a
//! [`PgString`] charged to the caller-supplied `mcx`, returned to the caller
//! (the `pstrdup`/`palloc`-out analog). Every append grows fallibly through
//! `mcx`, so an allocator refusal surfaces as `Err` rather than aborting.
//!
//! The C variadic `(letters, ...)` pair — a NUL-terminated letter string plus
//! one `char *` argument per letter, any of which may be `NULL` — is modeled as
//! a `&[(char, Option<&str>)]` slice: each entry is a letter and its optional
//! value. The C lookup scans `letters` in order and stops at the first matching
//! letter (erroring if that match's value is `NULL`); the slice scan mirrors
//! that first-letter-wins behavior exactly.
//!
//! The two `ereport(ERROR, ERRCODE_INVALID_PARAMETER_VALUE, ...)` sites (and
//! their `FRONTEND` `pg_log_error`/`exit(1)` twins, which emit the same
//! message + detail text) become `Err(PgError)`.

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;
#[cfg(test)]
extern crate std;

use alloc::format;
use ::mcx::{Mcx, PgString};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};

/// `replace_percent_placeholders` — replace percent-letter placeholders in
/// `instr` with the supplied values.
///
/// `%%` is replaced by a single `%`. A `%` at the end of the input, or a
/// placeholder that is unsupported (not in `values`) or whose value is `None`
/// (the C `NULL` case), is an error (`ERRCODE_INVALID_PARAMETER_VALUE`).
/// `param_name` names the underlying GUC parameter for error reporting.
///
/// The returned buffer is charged to `mcx` (the `palloc`'d-result analog).
pub fn replace_percent_placeholders<'mcx>(
    mcx: Mcx<'mcx>,
    instr: &str,
    param_name: &str,
    values: &[(char, Option<&str>)],
) -> PgResult<PgString<'mcx>> {
    let mut result = PgString::new_in(mcx);

    // C iterates `const char *sp = instr; *sp; sp++`, peeking `sp[1]`. Mirror
    // that with a peekable char iterator: `chars.next()` is `*sp`, and a second
    // `chars.next()` peeks/consumes `sp[1]` (advancing `sp` as the C `sp++`
    // inside the `%` branch does).
    let mut chars = instr.chars();

    while let Some(ch) = chars.next() {
        if ch != '%' {
            result.try_push(ch)?;
            continue;
        }

        // `*sp == '%'`: examine the following character.
        let Some(next) = chars.next() else {
            // `sp[1] == '\0'`: incomplete escape sequence.
            return Err(PgError::error(format!(
                "invalid value for parameter \"{param_name}\": \"{instr}\""
            ))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_detail("String ends unexpectedly after escape character \"%\"."));
        };

        if next == '%' {
            // Convert `%%` to a single `%`.
            result.try_push('%')?;
            continue;
        }

        // Look up the placeholder character: scan `values` in order, stop at the
        // first matching letter. If that match's value is present, append it;
        // if it is `None` (C `NULL`), fall through to the unknown-placeholder
        // error, exactly as C breaks out with `found` still false.
        let mut found = false;
        for &(letter, val) in values {
            if next == letter {
                if let Some(val) = val {
                    result.try_push_str(val)?;
                    found = true;
                }
                // If val is None, we will report an error.
                break;
            }
        }

        if !found {
            // Unknown placeholder.
            return Err(PgError::error(format!(
                "invalid value for parameter \"{param_name}\": \"{instr}\""
            ))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_detail(format!(
                "String contains unexpected placeholder \"%{next}\"."
            )));
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::mcx::MemoryContext;

    fn run(
        instr: &str,
        param_name: &str,
        values: &[(char, Option<&str>)],
    ) -> Result<std::string::String, PgError> {
        let ctx = MemoryContext::new("percentrepl-test");
        replace_percent_placeholders(ctx.mcx(), instr, param_name, values)
            .map(|s| std::string::ToString::to_string(s.as_str()))
    }

    #[test]
    fn replaces_in_input_order() {
        assert_eq!(
            run(
                "restore %f from %p after %r",
                "restore_command",
                &[
                    ('f', Some("0000000100000000")),
                    ('r', Some("restart")),
                    ('p', Some("/tmp/wal")),
                ],
            )
            .unwrap(),
            "restore 0000000100000000 from /tmp/wal after restart"
        );
    }

    #[test]
    fn double_percent_to_literal() {
        assert_eq!(
            run("echo %% %f %%", "archive_command", &[('f', Some("file"))]).unwrap(),
            "echo % file %"
        );
    }

    #[test]
    fn repeated_placeholders() {
        assert_eq!(
            run("%f/%f/%f", "archive_command", &[('f', Some("wal"))]).unwrap(),
            "wal/wal/wal"
        );
    }

    #[test]
    fn none_value_is_unexpected() {
        let err = run("%f %p", "restore_command", &[('f', Some("file")), ('p', None)]).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
        assert_eq!(
            err.detail(),
            Some("String contains unexpected placeholder \"%p\".")
        );
    }

    #[test]
    fn unknown_placeholder_is_unexpected() {
        let err = run("%x", "archive_command", &[('f', Some("file"))]).unwrap_err();
        assert_eq!(
            err.message(),
            "invalid value for parameter \"archive_command\": \"%x\""
        );
        assert_eq!(
            err.detail(),
            Some("String contains unexpected placeholder \"%x\".")
        );
    }

    #[test]
    fn trailing_percent_is_error() {
        let err = run("copy %", "archive_command", &[('f', Some("file"))]).unwrap_err();
        assert_eq!(
            err.detail(),
            Some("String ends unexpectedly after escape character \"%\".")
        );
    }

    #[test]
    fn unicode_text_preserved() {
        assert_eq!(
            run("pre-%f-\u{2603}", "archive_command", &[('f', Some("wal"))]).unwrap(),
            "pre-wal-\u{2603}"
        );
    }

    #[test]
    fn first_matching_letter_wins() {
        // First tuple for a letter decides, even if a later tuple has a value.
        let err = run("%f", "archive_command", &[('f', None), ('f', Some("file"))]).unwrap_err();
        assert_eq!(
            err.detail(),
            Some("String contains unexpected placeholder \"%f\".")
        );
    }

    #[test]
    fn empty_input_yields_empty() {
        assert_eq!(
            run("", "archive_command", &[('f', Some("file"))]).unwrap(),
            ""
        );
    }
}
