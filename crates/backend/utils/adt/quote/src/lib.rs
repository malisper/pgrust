// NB: not `#![no_std]` — the fmgr builtin registration layer (`fmgr_builtins`)
// registers the `quote.c` builtins into the fmgr-core table (C: `fmgr_builtins[]`),
// which uses `String`/`std`. The value cores themselves remain `alloc`-only.
#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

//! Idiomatic Rust port of PostgreSQL 18.3 `src/backend/utils/adt/quote.c`
//! — functions for quoting identifiers and literals.
//!
//! Every C function defined in `quote.c` is ported in full:
//! `quote_literal_internal` (static helper), `quote_literal_cstr`, and the
//! SQL-callable `quote_literal` / `quote_nullable` / `quote_ident` entry points.
//!
//! `quote_identifier` is **not** ported here: it is defined in `ruleutils.c`
//! (declared in `utils/builtins.h`), so it is owned by the
//! `backend-utils-adt-ruleutils` unit and reached across that unit's seam
//! ([`ruleutils_seams::quote_identifier`]); it panics loudly
//! until ruleutils lands. `quote_ident` is the only `quote.c` function that
//! calls it.
//!
//! # fmgr / Datum boundary
//!
//! The SQL-callable functions are declared in C as `Datum fn(PG_FUNCTION_ARGS)`
//! and wrap their cores via `text_to_cstring` / `cstring_to_text` /
//! `DirectFunctionCall1`. Per the project-wide fmgr/Datum deferral, the
//! `text`<->cstring varlena envelope is not re-implemented here; the transform
//! each one performs is exposed on the decoded byte content, allocating its
//! result into the caller's [`Mcx`] (the `palloc`-in-current-context analog).

extern crate alloc;

use alloc::string::String;

use ::mcx::{Mcx, PgVec, MAX_ALLOC_SIZE};
use ::types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED};

mod fmgr_builtins;
mod seams;
pub use seams::init_seams;

/// `ESCAPE_STRING_SYNTAX` — `c.h`. The `'E'` prefix that marks a
/// non-standard-conforming string literal.
const ESCAPE_STRING_SYNTAX: u8 = b'E';

/// `SQL_STR_DOUBLE(ch, escape_backslash)` — `c.h`.
///
/// True when `ch` must be doubled inside a single-quoted SQL string: always for
/// a single quote, and for a backslash when `escape_backslash` is set.
#[inline]
fn sql_str_double(ch: u8, escape_backslash: bool) -> bool {
    ch == b'\'' || (ch == b'\\' && escape_backslash)
}

/// `!AllocSizeIsValid(size)` over-limit error, mirroring `palloc`'s
/// `elog(ERROR, "invalid memory alloc request size")`. Recoverable.
fn alloc_size_failure() -> PgError {
    PgError::error("invalid memory alloc request size")
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

/// `quote_literal_internal(dst, src, len)` — `quote.c` (static helper).
///
/// Faithful port over the input byte slice `src` (the C `src` + `len`). The C
/// version writes into a caller-supplied `dst` and returns the written length;
/// here we build and return the quoted bytes in `mcx`, whose `len()` equals the
/// C return value.
///
/// Control flow is identical to C:
///   1. Scan for a backslash; if found, emit the `E` prefix and stop scanning.
///   2. Emit the opening quote.
///   3. Copy each input byte, doubling it first when `SQL_STR_DOUBLE` holds.
///   4. Emit the closing quote.
///
/// NOTE (carried from C): do not make this depend on
/// `standard_conforming_strings`; the result must work with either setting.
///
/// The C callers `palloc` a worst-case `len * 2 + 3` buffer (every byte doubled,
/// plus the optional `'E'` and two quotes). We validate that bound against
/// `MaxAllocSize` and reserve it fallibly up front, returning a recoverable
/// error on overflow/OOM.
fn quote_literal_internal<'mcx>(mcx: Mcx<'mcx>, src: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // result = palloc(len * 2 + 3 [+ 1 NUL]); the NUL is a C-string artifact
    // dropped here. Validate the bound exactly like AllocSizeIsValid.
    let worst_case = src
        .len()
        .checked_mul(2)
        .and_then(|n| n.checked_add(3))
        .filter(|&n| n <= MAX_ALLOC_SIZE)
        .ok_or_else(alloc_size_failure)?;

    let mut dst: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    dst.try_reserve(worst_case).map_err(|_| mcx.oom(worst_case))?;

    // for (s = src; s < src + len; s++) { if (*s == '\\') { *dst++ = 'E'; break; } }
    for &s in src {
        if s == b'\\' {
            dst.push(ESCAPE_STRING_SYNTAX);
            break;
        }
    }

    // *dst++ = '\'';
    dst.push(b'\'');
    // while (len-- > 0) { if (SQL_STR_DOUBLE(*src, true)) *dst++ = *src; *dst++ = *src++; }
    for &c in src {
        if sql_str_double(c, true) {
            dst.push(c);
        }
        dst.push(c);
    }
    // *dst++ = '\'';
    dst.push(b'\'');

    Ok(dst)
}

/// `char *quote_literal_cstr(const char *rawstr)` — `quote.c`.
///
/// Returns a properly quoted literal. The C version computes
/// `len = strlen(rawstr)`, palloc's a worst-case buffer, fills it via
/// `quote_literal_internal`, and NUL-terminates. This is the unit's **inward
/// seam** ([`quote_seams::quote_literal_cstr`]): the result is
/// consumed transiently by callers (folded into a query string), so it crosses
/// as an owned `String` and the seam is infallible apart from the underlying
/// allocation (OOM panics like a failed `palloc` at this boundary).
///
/// `rawstr` is the raw content (no NUL); the returned `String` is the quoted
/// literal without the C NUL terminator. The quoting only emits ASCII delimiters
/// around the (already valid UTF-8) input, so the result is valid UTF-8.
pub fn quote_literal_cstr(rawstr: &str) -> String {
    let ctx = ::mcx::MemoryContext::new("quote_literal_cstr");
    let buf = quote_literal_internal(ctx.mcx(), rawstr.as_bytes())
        .expect("quote_literal_cstr: palloc failed");
    // pstrdup-out into the caller's representation (the C palloc'd C string).
    // Bytes are valid UTF-8 (ASCII delimiters around valid-UTF-8 input).
    String::from_utf8(buf.as_slice().to_vec())
        .expect("quote_literal_cstr: quoted literal is valid UTF-8")
}

/// `Datum quote_literal(PG_FUNCTION_ARGS)` — `quote.c`.
///
/// fmgr/Datum boundary: the C version detoasts the `text` argument, palloc's a
/// worst-case `text` result, runs `quote_literal_internal` into its VARDATA, and
/// `SET_VARSIZE`s it. The transform on the raw text *content* `t` is exactly
/// [`quote_literal_internal`]; exposed here on the decoded bytes, building the
/// result into `mcx`.
pub fn quote_literal<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    quote_literal_internal(mcx, t)
}

/// `Datum quote_nullable(PG_FUNCTION_ARGS)` — `quote.c`.
///
/// fmgr/Datum boundary: returns the text `'NULL'` when the argument is SQL NULL,
/// otherwise the result of [`quote_literal`] on the argument. `arg == None`
/// models `PG_ARGISNULL(0)`.
pub fn quote_nullable<'mcx>(mcx: Mcx<'mcx>, arg: Option<&[u8]>) -> PgResult<PgVec<'mcx, u8>> {
    match arg {
        None => {
            // cstring_to_text("NULL")
            let mut out: PgVec<'mcx, u8> = PgVec::new_in(mcx);
            out.try_reserve(4).map_err(|_| mcx.oom(4))?;
            out.extend_from_slice(b"NULL");
            Ok(out)
        }
        Some(t) => quote_literal(mcx, t),
    }
}

/// `Datum quote_ident(PG_FUNCTION_ARGS)` — `quote.c`.
///
/// fmgr/Datum boundary: the C version converts the `text` argument to a
/// cstring, calls `quote_identifier` (`ruleutils.c`), and converts back to
/// `text`. `quote_identifier` is owned by the ruleutils unit and reached across
/// its seam; the decision/quoting logic lives there, not here. The quoted
/// spelling is returned in `mcx`.
pub fn quote_ident<'mcx>(mcx: Mcx<'mcx>, t: &str) -> PgResult<PgVec<'mcx, u8>> {
    let qstr = ruleutils_seams::quote_identifier::call(mcx, t)?;
    // cstring_to_text(qstr)
    let mut out: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    let bytes = qstr.as_bytes();
    out.try_reserve(bytes.len()).map_err(|_| mcx.oom(bytes.len()))?;
    out.extend_from_slice(bytes);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit_cstr(s: &[u8]) -> alloc::vec::Vec<u8> {
        let ctx = ::mcx::MemoryContext::new("test");
        let out = quote_literal_internal(ctx.mcx(), s).unwrap().as_slice().to_vec();
        out
    }

    #[test]
    fn literal_no_special_chars() {
        assert_eq!(lit_cstr(b"hello"), b"'hello'");
    }

    #[test]
    fn literal_doubles_single_quotes() {
        // it's  ->  'it''s'
        assert_eq!(lit_cstr(b"it's"), b"'it''s'");
    }

    #[test]
    fn literal_with_backslash_gets_e_prefix_and_doubles_backslash() {
        // a\b  ->  E'a\\b'
        assert_eq!(lit_cstr(b"a\\b"), b"E'a\\\\b'");
    }

    #[test]
    fn literal_empty_string() {
        assert_eq!(lit_cstr(b""), b"''");
    }

    #[test]
    fn literal_backslash_and_quote() {
        // \'  ->  E'\\'''
        assert_eq!(lit_cstr(b"\\'"), b"E'\\\\'''");
    }

    #[test]
    fn quote_literal_cstr_str() {
        assert_eq!(quote_literal_cstr("it's a \\test"), "E'it''s a \\\\test'");
    }

    #[test]
    fn quote_nullable_none_is_text_null() {
        let ctx = ::mcx::MemoryContext::new("test");
        let out = quote_nullable(ctx.mcx(), None).unwrap().as_slice().to_vec();
        assert_eq!(out, b"NULL");
    }

    #[test]
    fn quote_nullable_some_delegates_to_quote_literal() {
        let ctx = ::mcx::MemoryContext::new("test");
        let out = quote_nullable(ctx.mcx(), Some(b"x")).unwrap().as_slice().to_vec();
        assert_eq!(out, b"'x'");
    }
}
