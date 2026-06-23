//! Index-scan access-method ABI vocabulary shared by the index-scan executor
//! node crates (`nodeIndexscan`, `nodeIndexonlyscan`, `nodeBitmapIndexscan`).
//!
//! These structs cross the boundary between the executor node layer and the
//! generic index access method (`indexam.c`/`genam.h`) and the planner's index
//! plan nodes. They mirror the PostgreSQL 18.3 `#[repr(C)]` layout exactly so
//! the node-state crates can read/write the descriptor fields the index AM
//! fills in (`xs_itup`, `xs_hitup`, `xs_recheck`, ...) while every actual AM
//! call goes through the access-method seam. Compile-time size/align/offset
//! assertions pin the layout where it crosses the ABI.

use core::ffi::{c_int, c_void};

use crate::{
    pairingheap, pairingheap_node, uint64, AttrNumber, Buffer, Datum, ExprContext, ExprState,
    HeapTuple, IndexTuple, ItemPointerData, List, Oid, RelFileLocator, Relation, ScanDirection,
    ScanKeyData, ScanStateData, Size, SnapshotData, SortSupportData, TupleDescData,
};

/// `SortSupport` (`utils/sortsupport.h`) — a pointer to one or more
/// `SortSupportData` entries (the ORDER BY-distance comparators).
pub type SortSupport = *mut SortSupportData;

/// `IndexScanInstrumentation` (`access/genam.h`) — per-scan index search
/// counter maintained by all index AMs.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IndexScanInstrumentation {
    /// Index search count (incremented with `pgstat_count_index_scan`).
    pub nsearches: uint64,
}

/// `SharedIndexScanInstrumentation` (`access/genam.h`) — shared-memory header
/// for parallel index-scan instrumentation. The `winstrument` flexible array
/// member follows in the DSM allocation; only the header is modelled here.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SharedIndexScanInstrumentation {
    pub num_workers: i32,
    /// `IndexScanInstrumentation winstrument[FLEXIBLE_ARRAY_MEMBER]` — the
    /// per-worker counters live immediately after this header.
    pub winstrument: [IndexScanInstrumentation; 0],
}

/// `IndexRuntimeKeyInfo` (`nodes/execnodes.h`) — describes an index qual whose
/// right-hand side is not a simple constant and must be recomputed at runtime.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct IndexRuntimeKeyInfo {
    /// `struct ScanKeyData *scan_key` — scankey to put value into.
    pub scan_key: *mut ScanKeyData,
    /// `ExprState *key_expr` — expr to evaluate to get value.
    pub key_expr: *mut ExprState,
    /// `bool key_toastable` — is expr's result a toastable datatype?
    pub key_toastable: bool,
}

/// `IndexFetchTableData` (`access/tableam.h`) — opaque per-AM heap-fetch state
/// pointed to by `IndexScanDescData.xs_heapfetch`.
pub type IndexFetchTableData = c_void;

/// `IndexScanDescData` (`access/relscan.h`) — the index scan descriptor. The
/// index AM fills the `xs_*` result fields on each `index_getnext_tid`; the
/// index-only scan node reads them to build the output tuple. All mutation of
/// this struct happens inside the AM (reached through the seam); the node only
/// reads the result fields and toggles `xs_want_itup`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IndexScanDescData {
    /* scan parameters */
    /// heap relation descriptor, or NULL
    pub heapRelation: Relation,
    /// index relation descriptor
    pub indexRelation: Relation,
    /// snapshot to see
    pub xs_snapshot: *mut SnapshotData,
    /// number of index qualifier conditions
    pub numberOfKeys: i32,
    /// number of ordering operators
    pub numberOfOrderBys: i32,
    /// array of index qualifier descriptors
    pub keyData: *mut ScanKeyData,
    /// array of ordering op descriptors
    pub orderByData: *mut ScanKeyData,
    /// caller requests index tuples
    pub xs_want_itup: bool,
    /// unregister snapshot at scan end?
    pub xs_temp_snap: bool,

    /* signaling to index AM about killing index tuples */
    /// last-returned tuple is dead
    pub kill_prior_tuple: bool,
    /// do not return killed entries
    pub ignore_killed_tuples: bool,
    /// prevents killing/seeing killed tuples
    pub xactStartedInRecovery: bool,

    /* index access method's private state */
    /// access-method-specific info
    pub opaque: *mut c_void,

    /// instrumentation counters maintained by all index AMs
    pub instrument: *mut IndexScanInstrumentation,

    /* index-only scan result fields */
    /// index tuple returned by AM
    pub xs_itup: IndexTuple,
    /// rowtype descriptor of `xs_itup`
    pub xs_itupdesc: *mut TupleDescData,
    /// index data returned by AM, as HeapTuple
    pub xs_hitup: HeapTuple,
    /// rowtype descriptor of `xs_hitup`
    pub xs_hitupdesc: *mut TupleDescData,

    /// result TID
    pub xs_heaptid: ItemPointerData,
    /// T if must keep walking, potential further results
    pub xs_heap_continue: bool,
    /// per-AM heap-fetch state
    pub xs_heapfetch: *mut IndexFetchTableData,

    /// T means scan keys must be rechecked
    pub xs_recheck: bool,

    /* ordering-operator support */
    pub xs_orderbyvals: *mut crate::Datum,
    pub xs_orderbynulls: *mut bool,
    pub xs_recheckorderby: bool,

    /// parallel index scan information, in shared memory
    pub parallel_scan: *mut ParallelIndexScanDescData,
}

pub type IndexScanDesc = *mut IndexScanDescData;

/// `ParallelIndexScanDescData` (`access/relscan.h`) — shared-memory header for
/// a parallel index scan. The `ps_snapshot_data` flexible array member follows.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ParallelIndexScanDescData {
    /// physical table relation to scan
    pub ps_locator: RelFileLocator,
    /// physical index relation to scan
    pub ps_indexlocator: RelFileLocator,
    /// offset to `SharedIndexScanInstrumentation`
    pub ps_offset_ins: Size,
    /// offset to am-specific structure
    pub ps_offset_am: Size,
    /// `char ps_snapshot_data[FLEXIBLE_ARRAY_MEMBER]`
    pub ps_snapshot_data: [core::ffi::c_char; 0],
}

pub type ParallelIndexScanDesc = *mut ParallelIndexScanDescData;

/* ----------------------------------------------------------------
 * nodes/plannodes.h: index plan nodes (the parts the executor node reads)
 * ---------------------------------------------------------------- */

/// `Plan` (`nodes/plannodes.h`) — the abstract base of every plan-tree node.
/// The index-scan node reads `plan_node_id`, `qual`, and `parallel_aware`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Plan {
    pub type_: crate::NodeTag,
    pub disabled_nodes: i32,
    pub startup_cost: f64,
    pub total_cost: f64,
    pub plan_rows: f64,
    pub plan_width: i32,
    pub parallel_aware: bool,
    pub parallel_safe: bool,
    pub async_capable: bool,
    pub plan_node_id: i32,
    pub targetlist: *mut List,
    pub qual: *mut List,
    pub lefttree: *mut Plan,
    pub righttree: *mut Plan,
    pub initPlan: *mut List,
    pub extParam: *mut c_void,
    pub allParam: *mut c_void,
}

/// `Scan` (`nodes/plannodes.h`) — base of all scan plan nodes; adds the range
/// table index of the relation being scanned.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Scan {
    pub plan: Plan,
    /// `relid` is index into the range table
    pub scanrelid: crate::Index,
}

/// `IndexOnlyScan` (`nodes/plannodes.h`) — the index-only scan plan node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IndexOnlyScan {
    pub scan: Scan,
    /// OID of index to scan
    pub indexid: crate::Oid,
    /// list of index quals (usually OpExprs)
    pub indexqual: *mut List,
    /// index quals in recheckable form
    pub recheckqual: *mut List,
    /// list of index ORDER BY exprs
    pub indexorderby: *mut List,
    /// TargetEntry list describing index's cols
    pub indextlist: *mut List,
    /// forward or backward or don't care
    pub indexorderdir: ScanDirection,
}

/// `IndexScan` (`nodes/plannodes.h`) — the plain index-scan plan node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IndexScan {
    pub scan: Scan,
    /// OID of index to scan
    pub indexid: Oid,
    /// list of index quals (usually OpExprs)
    pub indexqual: *mut List,
    /// the same in original form
    pub indexqualorig: *mut List,
    /// list of index ORDER BY exprs
    pub indexorderby: *mut List,
    /// the same in original form
    pub indexorderbyorig: *mut List,
    /// OIDs of sort ops for ORDER BY exprs
    pub indexorderbyops: *mut List,
    /// forward or backward or don't care
    pub indexorderdir: ScanDirection,
}

/// `SubqueryScan` (`nodes/plannodes.h`) — the subquery-scan plan node. The
/// executor's `ExecSupportsBackwardScan` reads `subplan` to recurse into the
/// child plan tree.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SubqueryScan {
    pub scan: Scan,
    /// `Plan *subplan` — the child plan producing the subquery's rows.
    pub subplan: *mut Plan,
    /// `SubqueryScanStatus scanstatus`.
    pub scanstatus: c_int,
}

/// `CustomScan` (`nodes/plannodes.h`) — a custom-scan provider's plan node. The
/// executor's `ExecSupportsBackwardScan`/`ExecSupportsMarkRestore` read the
/// `flags` mask (`CUSTOMPATH_SUPPORT_*`). Only the leading `scan` and `flags`
/// are navigated by this crate; the remaining fields keep the C layout.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CustomScan {
    pub scan: Scan,
    /// `uint32 flags` — mask of `CUSTOMPATH_*` flags (see `nodes/extensible.h`).
    pub flags: crate::uint32,
    /// `List *custom_plans`.
    pub custom_plans: *mut List,
    /// `List *custom_exprs`.
    pub custom_exprs: *mut List,
    /// `List *custom_private`.
    pub custom_private: *mut List,
    /// `List *custom_scan_tlist`.
    pub custom_scan_tlist: *mut List,
    /// `Bitmapset *custom_relids`.
    pub custom_relids: *mut crate::Bitmapset,
    /// `const struct CustomScanMethods *methods`.
    pub methods: *const c_void,
}

/// `CUSTOMPATH_SUPPORT_BACKWARD_SCAN` (`nodes/extensible.h`).
pub const CUSTOMPATH_SUPPORT_BACKWARD_SCAN: crate::uint32 = 0x0001;
/// `CUSTOMPATH_SUPPORT_MARK_RESTORE` (`nodes/extensible.h`).
pub const CUSTOMPATH_SUPPORT_MARK_RESTORE: crate::uint32 = 0x0002;
/// `CUSTOMPATH_SUPPORT_PROJECTION` (`nodes/extensible.h`).
pub const CUSTOMPATH_SUPPORT_PROJECTION: crate::uint32 = 0x0004;

/// `IndexArrayKeyInfo` (`nodes/execnodes.h`) — a `ScalarArrayOpExpr` index qual
/// whose array is expanded client-side for AMs lacking `amsearcharray`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct IndexArrayKeyInfo {
    /// scankey to put value into
    pub scan_key: *mut ScanKeyData,
    /// array expression to evaluate
    pub array_expr: *mut ExprState,
    /// next array element to use
    pub next_elem: i32,
    /// number of elems in current array value
    pub num_elems: i32,
    /// array of num_elems Datums
    pub elem_values: *mut Datum,
    /// array of num_elems is-nulls
    pub elem_nulls: *mut bool,
}

/// `ReorderTuple` (`backend/executor/nodeIndexscan.c`, file-local) — a heap
/// tuple plus its ORDER BY distances, queued in the KNN reorder pairing heap.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ReorderTuple {
    pub ph_node: pairingheap_node,
    pub htup: HeapTuple,
    pub orderbyvals: *mut Datum,
    pub orderbynulls: *mut bool,
}

/// `IndexScanState` (`nodes/execnodes.h`) — the index-scan executor node state.
/// `ss.ps.type` is a `NodeTag`, so a `*mut IndexScanState` is a valid `Node *`.
/// Pointers to genuinely-external state are reached through the node crate's
/// runtime seam; they are pointer-typed here purely for ABI layout fidelity.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IndexScanState {
    /// `ScanState ss` — its first member's first member is `NodeTag`.
    pub ss: ScanStateData,
    pub indexqualorig: *mut ExprState,
    pub indexorderbyorig: *mut List,
    pub iss_ScanKeys: *mut ScanKeyData,
    pub iss_NumScanKeys: i32,
    pub iss_OrderByKeys: *mut ScanKeyData,
    pub iss_NumOrderByKeys: i32,
    pub iss_RuntimeKeys: *mut IndexRuntimeKeyInfo,
    pub iss_NumRuntimeKeys: i32,
    pub iss_RuntimeKeysReady: bool,
    pub iss_RuntimeContext: *mut ExprContext,
    pub iss_RelationDesc: Relation,
    pub iss_ScanDesc: *mut IndexScanDescData,
    pub iss_Instrument: IndexScanInstrumentation,
    pub iss_SharedInfo: *mut SharedIndexScanInstrumentation,
    pub iss_ReorderQueue: *mut pairingheap,
    pub iss_ReachedEnd: bool,
    pub iss_OrderByValues: *mut Datum,
    pub iss_OrderByNulls: *mut bool,
    pub iss_SortSupport: SortSupport,
    pub iss_OrderByTypByVals: *mut bool,
    pub iss_OrderByTypLens: *mut i16,
    pub iss_PscanLen: Size,
}

/// `IndexOnlyScanState` (`nodes/execnodes.h`) — the index-only-scan executor
/// node state. `ss.ps.type` is a `NodeTag`, so a `*mut IndexOnlyScanState` is a
/// valid `Node *`. Pointers to genuinely-external state are reached through the
/// node crate's runtime seam; they are pointer-typed here purely for ABI layout
/// fidelity.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IndexOnlyScanState {
    /// `ScanState ss` — its first member's first member is `NodeTag`.
    pub ss: ScanStateData,
    /// `ExprState *recheckqual`
    pub recheckqual: *mut ExprState,
    /// `struct ScanKeyData *ioss_ScanKeys`
    pub ioss_ScanKeys: *mut ScanKeyData,
    /// `int ioss_NumScanKeys`
    pub ioss_NumScanKeys: i32,
    /// `struct ScanKeyData *ioss_OrderByKeys`
    pub ioss_OrderByKeys: *mut ScanKeyData,
    /// `int ioss_NumOrderByKeys`
    pub ioss_NumOrderByKeys: i32,
    /// `IndexRuntimeKeyInfo *ioss_RuntimeKeys`
    pub ioss_RuntimeKeys: *mut IndexRuntimeKeyInfo,
    /// `int ioss_NumRuntimeKeys`
    pub ioss_NumRuntimeKeys: i32,
    /// `bool ioss_RuntimeKeysReady`
    pub ioss_RuntimeKeysReady: bool,
    /// `ExprContext *ioss_RuntimeContext`
    pub ioss_RuntimeContext: *mut ExprContext,
    /// `Relation ioss_RelationDesc`
    pub ioss_RelationDesc: Relation,
    /// `struct IndexScanDescData *ioss_ScanDesc`
    pub ioss_ScanDesc: *mut IndexScanDescData,
    /// `IndexScanInstrumentation ioss_Instrument`
    pub ioss_Instrument: IndexScanInstrumentation,
    /// `SharedIndexScanInstrumentation *ioss_SharedInfo`
    pub ioss_SharedInfo: *mut SharedIndexScanInstrumentation,
    /// `TupleTableSlot *ioss_TableSlot`
    pub ioss_TableSlot: *mut crate::TupleTableSlot,
    /// `Buffer ioss_VMBuffer`
    pub ioss_VMBuffer: Buffer,
    /// `Size ioss_PscanLen`
    pub ioss_PscanLen: Size,
    /// `AttrNumber *ioss_NameCStringAttNums`
    pub ioss_NameCStringAttNums: *mut AttrNumber,
    /// `int ioss_NameCStringCount`
    pub ioss_NameCStringCount: i32,
}

/// `ExecAuxRowMark` (`executor/executor.h`) — opaque; only pointers cross here.
pub type ExecAuxRowMark = c_void;

/// `EPQState` (`nodes/execnodes.h`) — EvalPlanQual recheck state. The
/// index-only scan mark/restore functions consult `relsubs_slot`,
/// `relsubs_rowmark`, and `relsubs_done` (all per-scanrelid arrays).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EPQState {
    pub parentestate: *mut crate::EState,
    pub epqParam: i32,
    pub resultRelations: *mut List,
    pub tuple_table: *mut List,
    pub relsubs_slot: *mut *mut crate::TupleTableSlot,
    pub plan: *mut Plan,
    pub arowMarks: *mut List,
    pub origslot: *mut crate::TupleTableSlot,
    pub recheckestate: *mut crate::EState,
    pub relsubs_rowmark: *mut *mut ExecAuxRowMark,
    pub relsubs_done: *mut bool,
    pub relsubs_blocked: *mut bool,
    pub recheckplanstate: *mut crate::PlanState,
}

/// `BlockNumber ItemPointerGetBlockNumber(ItemPointer)` (`storage/itemptr.h`).
///
/// Mirrors the C macro exactly: reassemble the 32-bit block number from the
/// split `BlockIdData` high/low 16-bit halves.
#[inline]
pub fn ItemPointerGetBlockNumber(pointer: &ItemPointerData) -> crate::BlockNumber {
    let blkid = &pointer.ip_blkid;
    ((blkid.bi_hi as crate::BlockNumber) << 16) | (blkid.bi_lo as crate::BlockNumber)
}

/// `Buffer InvalidBuffer` (`storage/buf.h`).
pub const InvalidBuffer: Buffer = 0;

/// `AttrNumber` index-scan name-conversion array element type alias.
pub type IndexNameCStringAttNum = AttrNumber;

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn instrumentation_layout() {
        assert_eq!(size_of::<IndexScanInstrumentation>(), 8);
        assert_eq!(align_of::<IndexScanInstrumentation>(), 8);
        // SharedIndexScanInstrumentation header is just an int, then the FAM
        // starts at offset 8 (8-byte aligned because of the uint64 member).
        assert_eq!(offset_of!(SharedIndexScanInstrumentation, num_workers), 0);
        assert_eq!(offset_of!(SharedIndexScanInstrumentation, winstrument), 8);
    }

    #[test]
    fn runtime_key_layout() {
        assert_eq!(offset_of!(IndexRuntimeKeyInfo, scan_key), 0);
        assert_eq!(offset_of!(IndexRuntimeKeyInfo, key_expr), 8);
        assert_eq!(offset_of!(IndexRuntimeKeyInfo, key_toastable), 16);
        assert_eq!(size_of::<IndexRuntimeKeyInfo>(), 24);
    }

    #[test]
    fn index_scan_desc_layout() {
        assert_eq!(align_of::<IndexScanDescData>(), 8);
        assert_eq!(offset_of!(IndexScanDescData, heapRelation), 0);
        assert_eq!(offset_of!(IndexScanDescData, indexRelation), 8);
        assert_eq!(offset_of!(IndexScanDescData, xs_snapshot), 16);
        assert_eq!(offset_of!(IndexScanDescData, numberOfKeys), 24);
        assert_eq!(offset_of!(IndexScanDescData, numberOfOrderBys), 28);
        assert_eq!(offset_of!(IndexScanDescData, keyData), 32);
        assert_eq!(offset_of!(IndexScanDescData, orderByData), 40);
        assert_eq!(offset_of!(IndexScanDescData, xs_want_itup), 48);
        assert_eq!(offset_of!(IndexScanDescData, xs_temp_snap), 49);
        assert_eq!(offset_of!(IndexScanDescData, kill_prior_tuple), 50);
        assert_eq!(offset_of!(IndexScanDescData, ignore_killed_tuples), 51);
        assert_eq!(offset_of!(IndexScanDescData, xactStartedInRecovery), 52);
        assert_eq!(offset_of!(IndexScanDescData, opaque), 56);
        assert_eq!(offset_of!(IndexScanDescData, instrument), 64);
        assert_eq!(offset_of!(IndexScanDescData, xs_itup), 72);
        assert_eq!(offset_of!(IndexScanDescData, xs_itupdesc), 80);
        assert_eq!(offset_of!(IndexScanDescData, xs_hitup), 88);
        assert_eq!(offset_of!(IndexScanDescData, xs_hitupdesc), 96);
        assert_eq!(offset_of!(IndexScanDescData, xs_heaptid), 104);
        // xs_heaptid is ItemPointerData (6 bytes, 2-aligned); xs_heap_continue
        // bool follows at 110, then padding to the 8-aligned pointer at 112.
        assert_eq!(offset_of!(IndexScanDescData, xs_heap_continue), 110);
        assert_eq!(offset_of!(IndexScanDescData, xs_heapfetch), 112);
        assert_eq!(offset_of!(IndexScanDescData, xs_recheck), 120);
        assert_eq!(offset_of!(IndexScanDescData, xs_orderbyvals), 128);
        assert_eq!(offset_of!(IndexScanDescData, xs_orderbynulls), 136);
        assert_eq!(offset_of!(IndexScanDescData, xs_recheckorderby), 144);
        assert_eq!(offset_of!(IndexScanDescData, parallel_scan), 152);
        assert_eq!(size_of::<IndexScanDescData>(), 160);
    }

    #[test]
    fn parallel_index_scan_desc_layout() {
        // Two RelFileLocators (12 bytes each) then two Size fields. The second
        // RelFileLocator starts at 12; the Size fields are 8-aligned => the
        // first lands at 24.
        assert_eq!(offset_of!(ParallelIndexScanDescData, ps_locator), 0);
        assert_eq!(offset_of!(ParallelIndexScanDescData, ps_indexlocator), 12);
        assert_eq!(offset_of!(ParallelIndexScanDescData, ps_offset_ins), 24);
        assert_eq!(offset_of!(ParallelIndexScanDescData, ps_offset_am), 32);
        assert_eq!(offset_of!(ParallelIndexScanDescData, ps_snapshot_data), 40);
    }

    #[test]
    fn plan_node_layout() {
        assert_eq!(offset_of!(Plan, type_), 0);
        assert_eq!(offset_of!(Plan, disabled_nodes), 4);
        assert_eq!(offset_of!(Plan, startup_cost), 8);
        assert_eq!(offset_of!(Plan, total_cost), 16);
        assert_eq!(offset_of!(Plan, plan_rows), 24);
        assert_eq!(offset_of!(Plan, plan_width), 32);
        assert_eq!(offset_of!(Plan, parallel_aware), 36);
        assert_eq!(offset_of!(Plan, parallel_safe), 37);
        assert_eq!(offset_of!(Plan, async_capable), 38);
        assert_eq!(offset_of!(Plan, plan_node_id), 40);
        assert_eq!(offset_of!(Plan, targetlist), 48);
        assert_eq!(offset_of!(Plan, qual), 56);
        assert_eq!(size_of::<Plan>(), 104);

        assert_eq!(offset_of!(Scan, plan), 0);
        assert_eq!(offset_of!(Scan, scanrelid), 104);
        assert_eq!(size_of::<Scan>(), 112);

        assert_eq!(offset_of!(IndexOnlyScan, scan), 0);
        assert_eq!(offset_of!(IndexOnlyScan, indexid), 112);
        assert_eq!(offset_of!(IndexOnlyScan, indexqual), 120);
        assert_eq!(offset_of!(IndexOnlyScan, recheckqual), 128);
        assert_eq!(offset_of!(IndexOnlyScan, indexorderby), 136);
        assert_eq!(offset_of!(IndexOnlyScan, indextlist), 144);
        assert_eq!(offset_of!(IndexOnlyScan, indexorderdir), 152);
        assert_eq!(size_of::<IndexOnlyScan>(), 160);
    }

    #[test]
    fn epqstate_layout() {
        assert_eq!(offset_of!(EPQState, parentestate), 0);
        assert_eq!(offset_of!(EPQState, epqParam), 8);
        assert_eq!(offset_of!(EPQState, resultRelations), 16);
        assert_eq!(offset_of!(EPQState, tuple_table), 24);
        assert_eq!(offset_of!(EPQState, relsubs_slot), 32);
        assert_eq!(offset_of!(EPQState, relsubs_rowmark), 72);
        assert_eq!(offset_of!(EPQState, relsubs_done), 80);
        assert_eq!(offset_of!(EPQState, relsubs_blocked), 88);
    }

    #[test]
    fn index_scan_plan_layout() {
        // IndexScan = Scan(112) + Oid(4)+pad(4) + 5 ptrs(40) + ScanDirection(4)+pad(4).
        assert_eq!(offset_of!(IndexScan, scan), 0);
        assert_eq!(offset_of!(IndexScan, indexid), 112);
        assert_eq!(offset_of!(IndexScan, indexqual), 120);
        assert_eq!(offset_of!(IndexScan, indexqualorig), 128);
        assert_eq!(offset_of!(IndexScan, indexorderby), 136);
        assert_eq!(offset_of!(IndexScan, indexorderbyorig), 144);
        assert_eq!(offset_of!(IndexScan, indexorderbyops), 152);
        assert_eq!(offset_of!(IndexScan, indexorderdir), 160);
        assert_eq!(size_of::<IndexScan>(), 168);
    }

    #[test]
    fn index_array_key_info_layout() {
        assert_eq!(offset_of!(IndexArrayKeyInfo, scan_key), 0);
        assert_eq!(offset_of!(IndexArrayKeyInfo, array_expr), 8);
        assert_eq!(offset_of!(IndexArrayKeyInfo, next_elem), 16);
        assert_eq!(offset_of!(IndexArrayKeyInfo, num_elems), 20);
        assert_eq!(offset_of!(IndexArrayKeyInfo, elem_values), 24);
        assert_eq!(offset_of!(IndexArrayKeyInfo, elem_nulls), 32);
        assert_eq!(size_of::<IndexArrayKeyInfo>(), 40);
    }

    #[test]
    fn reorder_tuple_layout() {
        // pairingheap_node is 3 pointers (24), then 3 pointers.
        assert_eq!(offset_of!(ReorderTuple, ph_node), 0);
        assert_eq!(offset_of!(ReorderTuple, htup), 24);
        assert_eq!(offset_of!(ReorderTuple, orderbyvals), 32);
        assert_eq!(offset_of!(ReorderTuple, orderbynulls), 40);
        assert_eq!(size_of::<ReorderTuple>(), 48);
    }

    #[test]
    fn index_scan_state_head_layout() {
        // ss (ScanStateData) is the head; iss_NumScanKeys etc. follow. We only
        // pin the early offsets that the node relies on.
        assert_eq!(offset_of!(IndexScanState, ss), 0);
        assert_eq!(align_of::<IndexScanState>(), 8);
        // The first index-specific pointer follows the ScanStateData head.
        assert_eq!(
            offset_of!(IndexScanState, indexqualorig),
            size_of::<ScanStateData>()
        );
    }

    #[test]
    fn index_only_scan_state_head_layout() {
        assert_eq!(offset_of!(IndexOnlyScanState, ss), 0);
        assert_eq!(align_of::<IndexOnlyScanState>(), 8);
        // The first index-only-specific pointer follows the ScanStateData head.
        assert_eq!(
            offset_of!(IndexOnlyScanState, recheckqual),
            size_of::<ScanStateData>()
        );
        // Internal consistency of the ioss_ block ordering.
        assert!(
            offset_of!(IndexOnlyScanState, ioss_NumScanKeys)
                > offset_of!(IndexOnlyScanState, ioss_ScanKeys)
        );
        assert!(
            offset_of!(IndexOnlyScanState, ioss_NameCStringCount)
                > offset_of!(IndexOnlyScanState, ioss_NameCStringAttNums)
        );
    }

    #[test]
    fn item_pointer_block_number() {
        let ip = ItemPointerData {
            ip_blkid: crate::BlockIdData {
                bi_hi: 0x0012,
                bi_lo: 0x3456,
            },
            ip_posid: 7,
        };
        assert_eq!(ItemPointerGetBlockNumber(&ip), 0x0012_3456);
    }
}
