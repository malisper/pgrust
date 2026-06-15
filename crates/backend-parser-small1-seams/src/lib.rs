//! Seam declarations for the `backend-parser-small1` unit
//! (`parser/parse_node.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

#![allow(non_snake_case)]

use types_cluster::ParseState;
use types_error::PgResult;

seam_core::seam!(
    /// `parser_errposition(pstate, location)` (parse_node.c): cursor position
    /// (1-based char index) for the error from a token location, or 0.
    pub fn parser_errposition(pstate: &ParseState<'_>, location: i32) -> PgResult<i32>
);
