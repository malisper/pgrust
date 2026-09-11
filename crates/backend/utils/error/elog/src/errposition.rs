//! setup_parser_errposition_callback / pcb_error_callback (parse_node.c): the
//! parser arms a parse location around a fallible callee, and every report
//! raised meanwhile that carries no cursor yet gets parser_errposition(),
//! except ERRCODE_QUERY_CANCELED. The ERROR path attaches on propagation
//! (the caller's map_err); this scope covers the non-ERROR reports that
//! `errfinish` emits inline (a DEBUG1 "relation ... does not exist" under
//! RVR_MISSING_OK, sitediff N-5d). One Cell per thread, no allocation: the
//! position is resolved only when a report is actually emitted, and the
//! guard restores the previous arming on drop, so the cell is None outside
//! a parser call (never session state; not part of the session envelope).

use core::cell::Cell;

#[derive(Clone, Copy)]
pub(crate) struct ArmedParserErrposition {
    source: *const u8,
    len: usize,
    location: i32,
    resolve: fn(&[u8], i32) -> i32,
}

impl ArmedParserErrposition {
    /// The 1-based character position for the armed location (0 = none).
    pub(crate) fn resolve(&self) -> i32 {
        // SAFETY: the guard borrows `source` for its whole life and restores
        // the previous arming on drop, so an armed pointer is live.
        let source = unsafe { core::slice::from_raw_parts(self.source, self.len) };
        (self.resolve)(source, self.location)
    }
}

thread_local! {
    static PARSER_ERRPOSITION: Cell<Option<ArmedParserErrposition>> = const { Cell::new(None) };
}

pub(crate) fn armed() -> Option<ArmedParserErrposition> {
    PARSER_ERRPOSITION.with(Cell::get)
}

/// Guard returned by [`arm_parser_errposition`]; restores the previously armed
/// location (C's `error_context_stack = pcbstate->errcallback.previous`).
pub struct ParserErrpositionScope<'a> {
    prev: Option<ArmedParserErrposition>,
    _source: core::marker::PhantomData<&'a [u8]>,
}

/// Arm `location` (a byte offset into `source`) for every non-ERROR report
/// emitted while the guard lives; `resolve` turns it into the 1-based
/// character position the P field carries (parse_node.c parser_errposition).
pub fn arm_parser_errposition<'a>(
    source: &'a [u8],
    location: i32,
    resolve: fn(&[u8], i32) -> i32,
) -> ParserErrpositionScope<'a> {
    let prev = PARSER_ERRPOSITION.with(|c| {
        c.replace(Some(ArmedParserErrposition {
            source: source.as_ptr(),
            len: source.len(),
            location,
            resolve,
        }))
    });
    ParserErrpositionScope { prev, _source: core::marker::PhantomData }
}

impl Drop for ParserErrpositionScope<'_> {
    fn drop(&mut self) {
        PARSER_ERRPOSITION.with(|c| c.set(self.prev));
    }
}
