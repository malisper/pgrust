//! Seam declarations for the `backend-parser-small1` unit
//! (`parser/parse_node.c` + `parser/parse_merge.c`). The owning unit installs
//! the inward seams from its `init_seams()`; outward seams (whose real owner is
//! a not-yet-ported neighbor) panic loudly until that owner lands.

#![allow(non_snake_case)]

extern crate alloc;

use mcx::Mcx;
use types_cluster::ParseState;
use types_error::PgResult;
use types_nodes::primnodes::SubscriptingRef;
use types_nodes::rawnodes::A_Indices;

seam_core::seam!(
    /// `parser_errposition(pstate, location)` (parse_node.c): cursor position
    /// (1-based char index) for the error from a token location, or 0.
    pub fn parser_errposition(pstate: &ParseState<'_>, location: i32) -> PgResult<i32>
);

seam_core::seam!(
    /// `sbsroutines->transform(sbsref, indirection, pstate, isSlice, isAssignment)`
    /// (parse_node.c `transformContainerSubscripts`): the container-type-specific
    /// subscripting support method (fetched by `getSubscriptingRoutines`) that
    /// transforms the raw subscript list and fills in the result type on the
    /// `SubscriptingRef`. Mutates the node in place in C; here it takes the
    /// partially-built `SubscriptingRef` by value and returns the filled one.
    ///
    /// The transform method's C body (`array_subscript_transform` in arraysubs.c,
    /// `jsonb_subscript_transform` in jsonbsubs.c) transforms the raw subscript
    /// expressions via `transformExpr` + `coerce_to_target_type`/`coerce_type`,
    /// which are parser-layer entry points above the `utils/adt` handler files;
    /// the install therefore lives in `backend-parser-parse-expr` (which owns
    /// `transformExpr` and reaches the coerce seams) and dispatches on the
    /// container type's `SubscriptHandler`. `pstate` is `&mut` because
    /// `transformExpr` mutates it (`p_expr_kind`, `p_last_srf`), matching C's
    /// `ParseState *`.
    pub fn subscripting_transform<'mcx>(
        mcx: Mcx<'mcx>,
        sbsref: SubscriptingRef,
        indirection: &[A_Indices<'mcx>],
        pstate: &mut ParseState<'mcx>,
        is_slice: bool,
        is_assignment: bool,
    ) -> PgResult<SubscriptingRef>
);
