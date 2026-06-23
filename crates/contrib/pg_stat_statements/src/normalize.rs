#![allow(dead_code)] // generate_normalized_query/PgssJumble are driven by the
// post-parse-analyze hook (next increment, once the enriched hook threads the
// real jumble clocations); the executor path passes jstate=None.
//! Query normalization: `generate_normalized_query` + `fill_in_constant_lengths`
//! (pg_stat_statements.c:2807/2930), re-lexing the query via the core SQL
//! scanner to find constant token lengths and replacing constants with `$n`.
//!
//! The jumble's constant-location array is carried in [`PgssJumble`], a
//! lifetime-free snapshot of the fields pgss reads off the core `JumbleState`
//! (location/length/squashed/extern_param per constant, plus
//! `highest_extern_param_id`/`has_squashed_lists`). It is filled by the
//! post-parse-analyze path (when the enriched hook threads the real jumble
//! state).

use ::scan_fgram::{Scanner, ScannerSettings, YY_NULL};

/// One constant location (`LocationLen`).
#[derive(Clone, Copy)]
pub struct PgssLocationLen {
    pub location: i32,
    pub length: i32,
    pub squashed: bool,
    pub extern_param: bool,
}

/// The pgss-side snapshot of the jumble state's constant array (the C
/// `JumbleState *jstate` fields pgss reads).
pub struct PgssJumble {
    pub clocations: Vec<PgssLocationLen>,
    pub highest_extern_param_id: i32,
    pub has_squashed_lists: bool,
}

impl PgssJumble {
    pub fn clocations_count(&self) -> usize {
        self.clocations.len()
    }
}

/// `generate_normalized_query(jstate, query, query_loc, &query_len)`
/// (pg_stat_statements.c:2807). `query` is the cleaned text; returns the
/// normalized text and updates `*query_len_p`.
pub(crate) fn generate_normalized_query(
    jstate: &PgssJumble,
    query: &[u8],
    query_loc: i32,
    query_len_p: &mut i32,
) -> Vec<u8> {
    let query_len = *query_len_p;

    // Get constants' lengths (core only gives us locations). Also sorts by loc.
    let mut clocations = jstate.clocations.clone();
    fill_in_constant_lengths(&mut clocations, query, query_loc);

    // Allow for $n symbols to be longer than the constants they replace.
    let norm_query_buflen = query_len as usize + clocations.len() * 10;
    let mut norm_query = vec![0u8; norm_query_buflen + 1];

    let mut len_to_wrt;
    let mut quer_loc: usize = 0;
    let mut n_quer_loc: usize = 0;
    let mut last_off: i32 = 0;
    let mut last_tok_len: i32 = 0;
    let mut num_constants_replaced: i32 = 0;

    for c in &clocations {
        if c.extern_param && !jstate.has_squashed_lists {
            continue;
        }

        let mut off = c.location;
        off -= query_loc;
        let tok_len = c.length;
        if tok_len < 0 {
            continue; // ignore duplicates
        }

        // Copy next chunk (what precedes the next constant).
        len_to_wrt = off - last_off;
        len_to_wrt -= last_tok_len;
        debug_assert!(len_to_wrt >= 0);
        let len_to_wrt = len_to_wrt as usize;
        norm_query[n_quer_loc..n_quer_loc + len_to_wrt]
            .copy_from_slice(&query[quer_loc..quer_loc + len_to_wrt]);
        n_quer_loc += len_to_wrt;

        // Insert the param symbol.
        let placeholder = format!(
            "${}{}",
            num_constants_replaced + 1 + jstate.highest_extern_param_id,
            if c.squashed { " /*, ... */" } else { "" }
        );
        let pb = placeholder.as_bytes();
        norm_query[n_quer_loc..n_quer_loc + pb.len()].copy_from_slice(pb);
        n_quer_loc += pb.len();
        num_constants_replaced += 1;

        quer_loc = (off + tok_len) as usize;
        last_off = off;
        last_tok_len = tok_len;
    }

    // Copy the remaining bytes of the original query string.
    let len_to_wrt = query_len as usize - quer_loc;
    norm_query[n_quer_loc..n_quer_loc + len_to_wrt]
        .copy_from_slice(&query[quer_loc..quer_loc + len_to_wrt]);
    n_quer_loc += len_to_wrt;

    norm_query.truncate(n_quer_loc);
    *query_len_p = n_quer_loc as i32;
    norm_query
}

/// `fill_in_constant_lengths(jstate, query, query_loc)`
/// (pg_stat_statements.c:2930). Re-lex the query to find each constant's textual
/// length.
fn fill_in_constant_lengths(clocations: &mut [PgssLocationLen], query: &[u8], query_loc: i32) {
    // Sort the records by location.
    if clocations.len() > 1 {
        clocations.sort_by_key(|c| c.location);
    }

    let settings = ScannerSettings {
        backslash_quote: 0,
        // We don't want to re-emit any escape string warnings.
        escape_string_warning: false,
        standard_conforming_strings: true,
    };
    let mut scanner = Scanner::new(query, settings);

    let n = clocations.len();
    for i in 0..n {
        // Ignore constants after the first one in the same location.
        if i > 0 && clocations[i].location == clocations[i - 1].location {
            clocations[i].length = -1;
            continue;
        }
        if clocations[i].squashed {
            continue; // squashable list, ignore
        }

        let loc = clocations[i].location - query_loc;
        debug_assert!(loc >= 0);
        let loc = loc as usize;

        // Lex tokens until we find the desired constant.
        loop {
            let tok = match scanner.core_yylex() {
                Ok(t) => t,
                Err(_) => break,
            };
            if tok.token == YY_NULL {
                break;
            }
            if tok.location as usize >= loc {
                let mut endtok = tok.clone();
                if query.get(loc) == Some(&b'-') {
                    // Negative value: consume one more token.
                    endtok = match scanner.core_yylex() {
                        Ok(t) => t,
                        Err(_) => break,
                    };
                    if endtok.token == YY_NULL {
                        break;
                    }
                }
                // Length = bytes from `loc` to the end of the current token.
                // The scanner's position after the token is its `pos`; C uses
                // strlen(scanbuf + loc) which equals (end of current token -
                // loc) because flex NUL-terminates the token.
                let tok_end = token_end(&scanner, &endtok, query);
                clocations[i].length = (tok_end.saturating_sub(loc)) as i32;
                break;
            }
        }
    }
}

/// The end byte offset (exclusive) of the just-returned token. The scanner's
/// `pos()` is the start of the next match attempt, i.e. just past the current
/// token (modulo trailing whitespace already consumed). We clamp to the buffer.
fn token_end(scanner: &Scanner<'_>, _tok: &::scan_fgram::Token, query: &[u8]) -> usize {
    scanner.pos().min(query.len())
}
