//! Seam declarations for the `backend-optimizer-util-clauses` unit
//! (`optimizer/util/clauses.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `contain_subplans(Node *clause)` (clauses.c): walk the expression tree
    /// and return whether it contains any `SubPlan` or `AlternativeSubPlan`
    /// node. `ValuesScan` init passes one VALUES row's expression list (the C
    /// `(Node *) exprs`, an implicitly-AND'd `List *`); the walker descends it
    /// like any other node. A pure structural predicate, so infallible.
    pub fn contain_subplans(clause: &[types_nodes::primnodes::Expr]) -> bool
);
