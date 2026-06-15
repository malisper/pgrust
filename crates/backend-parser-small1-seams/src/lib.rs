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
    /// OUTWARD seam: the real owners are the per-type subscript handlers
    /// (`array_subscript_handler` / `jsonb_subscript_handler`, utils/adt) reached
    /// through the fmgr `OidFunctionCall0` result kept opaque by
    /// `lsyscache::getSubscriptingRoutines`; none is ported, so this panics
    /// (mirror-PG-and-panic) until a subscript-support owner lands.
    pub fn subscripting_transform<'mcx>(
        mcx: Mcx<'mcx>,
        sbsref: SubscriptingRef,
        indirection: &[A_Indices<'mcx>],
        pstate: &ParseState<'mcx>,
        is_slice: bool,
        is_assignment: bool,
    ) -> PgResult<SubscriptingRef>
);
