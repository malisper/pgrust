//! The OID-keyed `SupportRequestIndexCondition` dispatch registry.
//!
//! `get_index_clause_from_support` (indxpath.c:3069) calls
//! `OidFunctionCall1(prosupport, &SupportRequestIndexCondition{...})`, dispatching
//! on the function's `pg_proc.prosupport` OID. C resolves the actual support
//! function body by that OID; the owned model mirrors it with a process-global
//! `prosupport`-OID → kernel table, exactly as the `SupportRequestRows`/`Cost`
//! registries do (commit 59cf05ef8). Each support-bearing crate registers its
//! decomposed `SupportRequestIndexCondition` kernel here from its own
//! `init_seams`:
//!
//!   * `backend-utils-adt-selfuncs` registers `like_regex_support`'s
//!     index-condition leg under the five pattern-support OIDs (`textlike_support`
//!     et al.);
//!   * `backend-utils-adt-network-selfuncs` registers `network_subset_support`
//!     (OID 1173) — it owns the inet→Datum serialization the kernel needs and
//!     also has the nodes/lsyscache deps to build the bound `OpExpr`s, which the
//!     `backend-utils-adt-network` value crate cannot take without a dep cycle.
//!
//! A `prosupport` OID with no registered kernel returns the empty derived-clause
//! list (the C `NIL` decline): the planner falls through to the ordinary
//! operator-class match, exactly as when the support function does not handle the
//! `SupportRequestIndexCondition` request type.

extern crate alloc;

use alloc::vec::Vec;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use ::types_core::primitive::Oid;
use ::nodes::primnodes::Expr;
use pathnodes::{IndexOptInfo, NodeId, PlannerInfo};

/// A decomposed `SupportRequestIndexCondition` kernel. The parameters mirror the
/// `SupportRequestIndexCondition` request fields the C body reads (`req->node`
/// carried as the `clause` `NodeId`, `req->indexarg`, `req->funcid`, plus the
/// index column the planner is matching), and the result is the derived bare
/// index-condition expressions (the C `List *`; empty ⇒ `NIL`) and the `lossy`
/// flag the support function set.
pub type IndexConditionFn = fn(
    root: &PlannerInfo,
    prosupport: Oid,
    funcid: Oid,
    clause: NodeId,
    indexarg: i32,
    index: &IndexOptInfo,
    indexcol: i32,
) -> (Vec<Expr<'static>>, bool);

fn table() -> &'static Mutex<HashMap<Oid, IndexConditionFn>> {
    static T: OnceLock<Mutex<HashMap<Oid, IndexConditionFn>>> = OnceLock::new();
    T.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register a decomposed `SupportRequestIndexCondition` kernel under its
/// `prosupport` OID. Returns the previous registration if the OID was present.
pub fn register_index_condition(prosupport: Oid, func: IndexConditionFn) -> Option<IndexConditionFn> {
    table()
        .lock()
        .expect("index-condition support table lock")
        .insert(prosupport, func)
}

/// `OidFunctionCall1(prosupport, &SupportRequestIndexCondition{...})` — the
/// OID-keyed dispatch. Resolve `prosupport` in the table and run its kernel; an
/// OID with no registered kernel returns the empty list + not-lossy (the C `NIL`
/// decline). Installed on the `oid_function_call1_index_support` seam.
#[allow(clippy::too_many_arguments)]
pub fn dispatch_index_condition(
    root: &PlannerInfo,
    prosupport: Oid,
    funcid: Oid,
    clause: NodeId,
    indexarg: i32,
    index: &IndexOptInfo,
    indexcol: i32,
) -> (Vec<Expr<'static>>, bool) {
    let func = table()
        .lock()
        .expect("index-condition support table lock")
        .get(&prosupport)
        .copied();
    match func {
        Some(f) => f(root, prosupport, funcid, clause, indexarg, index, indexcol),
        None => (Vec::new(), false),
    }
}

/// Install the OID-keyed dispatcher on the `oid_function_call1_index_support`
/// seam. Idempotent: each support-bearing crate calls this after registering its
/// kernel, but the seam slot is one-shot, so only the first install wins (the
/// dispatcher is always the same function, so order does not matter).
pub fn install_dispatch() {
    if !crate::oid_function_call1_index_support::is_installed() {
        crate::oid_function_call1_index_support::set(dispatch_index_condition);
    }
}
