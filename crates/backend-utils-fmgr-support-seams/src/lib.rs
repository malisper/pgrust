//! Seam declaration for the planner-support-function fmgr dispatch used by
//! indxpath.c's `get_index_clause_from_support`.
//!
//! The C code calls `OidFunctionCall1(prosupport, PointerGetDatum(&req))` with a
//! `SupportRequestIndexCondition` request node, and the support function returns
//! a `List *` of bare index-condition `Expr *`s (and sets `req.lossy`). The fmgr
//! planner-support protocol (the request node + the `OidFunctionCall1` call over
//! a `Node *` argument) is not modeled in the ported fmgr surface; this seam
//! crosses that boundary and defaults to a loud panic until it lands.

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::Oid;
use types_nodes::primnodes::Expr;
use types_pathnodes::{IndexOptInfo, NodeId, PlannerInfo};

seam_core::seam!(
    /// `OidFunctionCall1(prosupport, &SupportRequestIndexCondition{...})`
    /// (supportnodes.h / fmgr.c) — invoke the operator/function's planner
    /// support function with a `SupportRequestIndexCondition` populated from the
    /// arguments and return its result: the list of derived bare index-condition
    /// expressions (the C `List *`, here the produced [`Expr`] values; the caller
    /// wraps each in a `RestrictInfo`) plus the `lossy` flag the support function
    /// set. An empty list ⇒ the support function declined (C `NIL`).
    pub fn oid_function_call1_index_support(
        root: &PlannerInfo,
        prosupport: Oid,
        funcid: Oid,
        clause: NodeId,
        indexarg: i32,
        index: &IndexOptInfo,
        indexcol: i32
    ) -> (Vec<Expr>, bool)
);
