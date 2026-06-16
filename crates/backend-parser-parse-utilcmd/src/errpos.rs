//! `parser_errposition` (`parse_node.c`) — compute the 1-based character cursor
//! position from `pstate->p_sourcetext` for an ereport `.errposition(...)`.
//!
//! Mirrors the local helper in the sibling parser crates: a byte offset into the
//! source string is converted into a 1-based character index for reporting; 0 if
//! no location or no source text. (Equivalent to
//! `backend_parser_small1::parser_errposition`; kept local to avoid a dep on the
//! whole small1 crate just for this helper.)

use types_nodes::parsestmt::ParseState;

/// `parser_errposition(pstate, location)` — byte offset → 1-based char position.
pub fn parser_errposition(pstate: &ParseState<'_>, location: i32) -> i32 {
    // No-op if location was not provided.
    if location < 0 {
        return 0;
    }
    // Can't do anything if source text is not available.
    let sourcetext = match pstate.p_sourcetext.as_ref() {
        Some(s) => s.as_str(),
        None => return 0,
    };
    let limit = (location as usize).min(sourcetext.len());
    // Count *characters* in the first `location` bytes (the C
    // `pg_mbstrlen_with_len`), then +1 for the 1-based cursor.
    sourcetext[..limit].chars().count() as i32 + 1
}
