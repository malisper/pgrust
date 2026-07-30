#![no_std]
#![allow(non_snake_case)]

extern crate alloc;

use alloc::format;
use alloc::string::String;

use elog::ereport;
use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_error::{ErrorLocation, PgResult, ERRCODE_NAME_TOO_LONG, NOTICE};
use wchar::{pg_enc, pg_encoding_max_length};

pub const NAMEDATALEN: usize = types_core::fmgr::NAMEDATALEN as usize;

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

// Ambient DatabaseEncoding is threaded as a parameter (no ambient-global
// getter seams); callers pass the server encoding.
pub fn downcase_truncate_identifier<'mcx>(
    mcx: Mcx<'mcx>,
    ident: &[u8],
    warn: bool,
    encoding: pg_enc,
) -> PgResult<PgVec<'mcx, u8>> {
    downcase_identifier(mcx, ident, warn, true, encoding)
}

pub fn downcase_identifier<'mcx>(
    mcx: Mcx<'mcx>,
    ident: &[u8],
    warn: bool,
    truncate: bool,
    encoding: pg_enc,
) -> PgResult<PgVec<'mcx, u8>> {
    let enc_is_single_byte = pg_encoding_max_length(encoding) == 1;
    let mut result = vec_with_capacity_in(mcx, ident.len())?;
    for &b in ident {
        let ch = if b.is_ascii_uppercase() {
            b + (b'a' - b'A')
        } else if enc_is_single_byte && b & 0x80 != 0 && locale_isupper(b) {
            locale_tolower(b)
        } else {
            b
        };
        result.push(ch);
    }
    if ident.len() >= NAMEDATALEN && truncate {
        truncate_identifier(&mut result, warn, encoding)?;
    }
    Ok(result)
}

pub fn truncate_identifier(ident: &mut PgVec<'_, u8>, warn: bool, encoding: pg_enc) -> PgResult<()> {
    if ident.len() >= NAMEDATALEN {
        let clipped = mbutils::pg_encoding_mbcliplen(
            encoding,
            ident,
            ident.len() as i32,
            (NAMEDATALEN - 1) as i32,
        ) as usize;
        if warn {
            ereport(NOTICE)
                .errcode(ERRCODE_NAME_TOO_LONG)
                .errmsg(format!(
                    "identifier \"{}\" will be truncated to \"{}\"",
                    String::from_utf8_lossy(ident),
                    String::from_utf8_lossy(&ident[..clipped])
                ))
                .finish(loc("truncate_identifier"))?;
        }
        ident.truncate(clipped);
    }
    Ok(())
}

// Must match scan.l's {space} list.
pub fn scanner_isspace(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

// Value form of parse_node.c parser_errposition; the ParseState wrapper is
// parse_node::parser_errposition.
pub fn parser_errposition_source(
    sourcetext: Option<&[u8]>,
    location: i32,
    encoding: pg_enc,
) -> i32 {
    if location < 0 {
        return 0;
    }
    let Some(src) = sourcetext else {
        return 0;
    };
    let limit = (location as usize).min(src.len());
    // Defensive fallback: sourcetext is validated query/command text, so this
    // should never hit the encoding-error arm; no ereport() to route it to
    // from inside a cursor-position computation anyway.
    mbutils::pg_encoding_mbstrlen_with_len(encoding, &src[..limit]).unwrap_or(location) + 1
}

pub fn transformMergeStmt() -> ! {
    panic!(
        "transformMergeStmt (parse_merge.c): sibling parser layer \
         (parse_clause/parse_relation/parse_target/analyze) is unported"
    )
}

#[cfg(not(target_family = "wasm"))]
#[inline]
fn locale_isupper(ch: u8) -> bool {
    unsafe { libc::isupper(i32::from(ch)) != 0 }
}

#[cfg(not(target_family = "wasm"))]
#[inline]
fn locale_tolower(ch: u8) -> u8 {
    unsafe { libc::tolower(i32::from(ch)) as u8 }
}

// wasm libc has no ctype; the C/POSIX locale never classes a high-bit byte as
// upper-case, so false/identity is the single-locale answer.
#[cfg(target_family = "wasm")]
#[inline]
fn locale_isupper(_ch: u8) -> bool {
    false
}

#[cfg(target_family = "wasm")]
#[inline]
fn locale_tolower(ch: u8) -> u8 {
    ch
}

pub mod parse_enr;
pub mod parse_node;
pub mod parse_param;
pub mod udeescape;

pub use parse_enr::{get_visible_ENR, name_matches_visible_ENR};
pub use parse_node::{
    cancel_parser_errposition_callback, free_parsestate, make_const, make_parsestate,
    parser_errposition, setup_parser_errposition_callback, transformContainerSubscripts,
    transformContainerType, ParseExprKind, ParseNamespaceColumn, ParseNamespaceItem, ParseState, PreColumnRefHook,
};
pub use parse_param::{
    check_variable_parameters, fixed_paramref_hook, plpgsql_paramref_hook,
    plpgsql_resolve_column_ref, query_contains_extern_params, setup_parse_fixed_parameters,
    setup_parse_sql_fn_parameters, setup_parse_variable_parameters, sql_fn_make_param,
    sql_fn_paramref_hook, sql_fn_resolve_param_name, variable_coerce_param_hook,
    variable_paramref_hook, FixedParamState, ParseRefHookState, PlpgsqlHookState,
    PlpgsqlNameEntry, PlpgsqlResolveOption, SqlFnParamState, VarParamState,
};

#[cfg(test)]
mod tests;
