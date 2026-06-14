//! Seam declarations for the `backend-utils-adt-tsvector-core` unit
//! (`utils/adt/tsvector_op.c`), the `tsvector`/`tsquery` operator core.
//!
//! The GIN (`tsginidx.c`), GiST (`tsgistidx.c`), and ranking (`tsrank.c`)
//! support functions call into this unit; the owner installs these from its
//! `init_seams()` when it lands. Until then a call panics loudly.

use mcx::Mcx;
use types_error::{PgResult, SoftErrorContext};
use types_tsearch::tsearch::{CheckCondition, QueryItem, TSTernaryValue, TsVectorParseStateHandle};

// ===========================================================================
// tsvector_parser.c — the shared tsvector/tsquery value tokenizer.
//
// C declares `struct TSVectorParseStateData` opaque ("opaque struct in
// tsvector_parser.c") and exposes init/reset/gettoken/close over an opaque
// `TSVectorParseState` pointer (ts_utils.h). The `tsquery` parser
// (`parse_tsquery`) drives this engine to tokenize each operand. The owner is
// the unported `backend-utils-adt-tsvector-core` unit; until it lands these
// panic. The opaque state lives behind a `TsVectorParseStateHandle` token the
// owner mints in `init_tsvector_parser` and resolves in the other three.
// ===========================================================================

seam_core::seam!(
    /// `init_tsvector_parser(input, flags, escontext)` (tsvector_parser.c) —
    /// allocate a parser state over `input` (a NUL-terminated cstring's bytes,
    /// excluding the terminator) with the `P_TSV_*` `flags`. The soft-error
    /// context is held by the owner for the state's lifetime. Returns the
    /// opaque state token.
    pub fn init_tsvector_parser(input: &[u8], flags: i32) -> PgResult<TsVectorParseStateHandle>
);

seam_core::seam!(
    /// `reset_tsvector_parser(state, input)` (tsvector_parser.c) — re-point an
    /// existing parser state at a new scan position within the original input
    /// (the `tsquery` parser hands it the absolute byte offset of its current
    /// scan point, `state->buf - state->buffer`).
    pub fn reset_tsvector_parser(state: TsVectorParseStateHandle, input_offset: usize)
);

/// The result of one [`gettoken_tsvector`] call. `Some` carries the decoded
/// operand bytes (`strval`/`lenval` in C — de-escaped, so owned, not a borrow
/// into the input) and `endptr_offset`, the absolute byte offset within the
/// original input where scanning of this token finished. `None` means
/// end-of-string (C `gettoken_tsvector` returned `false`); the caller must
/// then check the soft-error context.
pub struct TsVectorToken {
    /// the de-escaped operand bytes (`*strval` / `*lenval`)
    pub strval: alloc::vec::Vec<u8>,
    /// absolute offset into the original input where this token ended (`*endptr`)
    pub endptr_offset: usize,
}

seam_core::seam!(
    /// `gettoken_tsvector(state, &strval, &lenval, NULL, NULL, &endptr)`
    /// (tsvector_parser.c) — pull the next operand. The `tsquery` parser passes
    /// NULL for the position out-params, so only the lexeme and the end pointer
    /// are returned. Returns `Ok(Some(..))` for a token, `Ok(None)` at
    /// end-of-input or on a soft error (signalled via `escontext`). The owner
    /// charges the de-escaped operand bytes to `mcx`.
    pub fn gettoken_tsvector<'mcx>(
        mcx: Mcx<'mcx>,
        state: TsVectorParseStateHandle,
        escontext: Option<&mut SoftErrorContext>,
    ) -> PgResult<Option<TsVectorToken>>
);

seam_core::seam!(
    /// `close_tsvector_parser(state)` (tsvector_parser.c) — free the parser
    /// state and its internal buffers.
    pub fn close_tsvector_parser(state: TsVectorParseStateHandle)
);

extern crate alloc;

seam_core::seam!(
    /// `tsCompareString(a, lena, b, lenb, prefix)` (tsvector_op.c) — compare two
    /// lexeme strings (length-then-bytes order) with optional prefix matching.
    /// Pure, infallible byte comparison.
    pub fn ts_compare_string(a: &[u8], b: &[u8], prefix: bool) -> i32
);

seam_core::seam!(
    /// `tsquery_requires_match(curitem)` (tsvector_op.c) over the full
    /// `QueryItem` array (root at index 0) — whether a (sub)query requires at
    /// least one positive match. `PgResult` because the C recursion guards
    /// itself with `check_stack_depth()`.
    pub fn tsquery_requires_match(query_items: &[QueryItem]) -> PgResult<bool>
);

seam_core::seam!(
    /// `TS_execute(GETQUERY(query), arg, flags, chkcond)` (tsvector_op.c) —
    /// evaluate a `tsquery` tree via a per-operand callback, returning the
    /// boolean match result. `mcx` charges the transient phrase position
    /// buffers the engine allocates in the current context. `PgResult` because
    /// the C recursion guards itself with `check_stack_depth()` /
    /// `CHECK_FOR_INTERRUPTS()`.
    pub fn ts_execute<'mcx>(
        mcx: Mcx<'mcx>,
        query_items: &[QueryItem],
        flags: u32,
        chkcond: &mut CheckCondition<'_>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `TS_execute_ternary(GETQUERY(query), arg, flags, chkcond)`
    /// (tsvector_op.c) — like [`ts_execute`] but returns ternary logic
    /// (`TS_MAYBE` preserved).
    pub fn ts_execute_ternary<'mcx>(
        mcx: Mcx<'mcx>,
        query_items: &[QueryItem],
        flags: u32,
        chkcond: &mut CheckCondition<'_>,
    ) -> PgResult<TSTernaryValue>
);
