//! P4-2: the two-relation hash-join family. `ir` is the join plan-node
//! vocabulary, `family` the fail-closed admission constructor,
//! `hash_join` the stencil. Semantics reference = the ported row-engine
//! hash join (nodehashjoin). The server recognizer (phase 2) lives in
//! the shell's dispatch seam; `JoinRefuse` surfaces through the shell
//! lattice's `from_join` mapping (the family runs outside the
//! single-relation `ir::Family` registry — two banks, one stencil).
//! P4-5 adds the KEYLESS (nest-loop) shape of the same node: no equi-key
//! lanes, quals (Eq allowed) decide matched-ness per pair, and the
//! runner witness-gates the build x probe pair product.

pub mod family;
pub mod hash_join;
pub mod ir;
pub mod numcell;

pub use family::{
    attach_build_fold, attach_filter_stages, attach_map_goal_stage, attach_num_fold,
    attach_staged_or, join_agg_node, join_node, nest_loop_agg_node, nest_loop_node,
    set_map_goal_rows, MAX_DIM_STAGES, MAX_FILTER_STAGES, MAX_GROUP_KEYS, MAX_OR_ARMS,
};
pub use hash_join::{
    check_join_agg, run_hash_join, run_hash_join_agg, run_hash_join_agg_flt, run_hash_join_flt,
    JOIN_BUILD_BUDGET_BYTES, MAX_KEY_LANES, NL_PRODUCT_BUDGET_PAIRS,
};
pub use ir::{
    BuildFold, CaseTest, DimKey, DimSrc, DimStage, FilterKey, FilterQual, FilterStage,
    InSetFilter, JoinAggNode, JoinAggOp, JoinAggReq, JoinAggSpec, JoinArith, JoinCaseLeg,
    JoinCmp, JoinKey, JoinNode, JoinOut, JoinQual, JoinRefuse, JoinSide, JoinType, KeyXf,
    NumCellOp, NumCellQual, NumFold, OrTerm, StageFold, StageRows, StageSrc, StagedOr, TextEqTerm,
};
