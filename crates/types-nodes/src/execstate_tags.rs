//! Executor `PlanState` node tags (`nodes/nodetags.h`), the `*State` family.
//!
//! These are the tags the parallel executor (`execParallel.c`) dispatches on
//! while walking a live `PlanState` tree. Values are PostgreSQL 18.3's
//! generated enumeration order (verified against the c2rust rendering). Kept in
//! a dedicated module so executor-tree additions don't collide with the
//! plan-node tag table in `nodes.rs`.

use crate::nodes::NodeTag;

/// `T_SeqScanState`.
pub const T_SeqScanState: NodeTag = NodeTag(403);
/// `T_IndexScanState`.
pub const T_IndexScanState: NodeTag = NodeTag(405);
/// `T_IndexOnlyScanState`.
pub const T_IndexOnlyScanState: NodeTag = NodeTag(406);
/// `T_BitmapIndexScanState`.
pub const T_BitmapIndexScanState: NodeTag = NodeTag(407);
/// `T_BitmapHeapScanState`.
pub const T_BitmapHeapScanState: NodeTag = NodeTag(408);
/// `T_ForeignScanState`.
pub const T_ForeignScanState: NodeTag = NodeTag(418);
/// `T_CustomScanState`.
pub const T_CustomScanState: NodeTag = NodeTag(419);
/// `T_HashJoinState`.
pub const T_HashJoinState: NodeTag = NodeTag(423);
/// `T_MemoizeState`.
pub const T_MemoizeState: NodeTag = NodeTag(425);
/// `T_SortState`.
pub const T_SortState: NodeTag = NodeTag(426);
/// `T_IncrementalSortState`.
pub const T_IncrementalSortState: NodeTag = NodeTag(427);
/// `T_AggState`.
pub const T_AggState: NodeTag = NodeTag(429);
/// `T_HashState`.
pub const T_HashState: NodeTag = NodeTag(434);
/// `T_AppendState`.
pub const T_AppendState: NodeTag = NodeTag(397);
