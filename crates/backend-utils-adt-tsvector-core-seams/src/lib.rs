//! Seam declarations for the `backend-utils-adt-tsvector-core` unit
//! (`utils/adt/tsvector_op.c`), the `tsvector`/`tsquery` operator core.
//!
//! The GIN (`tsginidx.c`), GiST (`tsgistidx.c`), and ranking (`tsrank.c`)
//! support functions call into this unit; the owner installs these from its
//! `init_seams()` when it lands. Until then a call panics loudly.

use mcx::Mcx;
use types_error::PgResult;
use types_tsearch::tsearch::{CheckCondition, QueryItem, TSTernaryValue};

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
