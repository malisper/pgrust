//! Per-planning-cycle syscache memos — HOISTED to `lsyscache::run_memo`
//! by the replanfix3 lane so the crates below the planner that already see
//! `PlannerRun` (lsyscache's own `ops_are_compatible` internals, indxpath's
//! clause-matching walk — the close-out's named run-inaccessible callers)
//! share the same per-cycle block. Design notes, the divergence argument,
//! and the kill switches (PGRUST_PLANNER_{OPSHAPE,ACLMASK,AMOP,AMOPLIST}_
//! MEMO=0) live in that module; this shim keeps the planner-internal call
//! sites unchanged.

pub(crate) use lsyscache::run_memo::{
    class_aclmask, comparison_ops_are_compatible, get_commutator, get_opcode,
    get_op_opfamily_strategy, get_opfamily_member, get_oprrest, op_mergejoinable,
};
