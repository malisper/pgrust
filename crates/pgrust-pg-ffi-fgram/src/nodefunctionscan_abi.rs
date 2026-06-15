//! FunctionScan node ABI vocabulary (`nodeFunctionscan.c`).
//!
//! The `FunctionScan` plan node (plannodes.h), `RangeTblFunction`
//! (parsenodes.h), and `FunctionScanState` (execnodes.h) cross the executor
//! crate boundary as `#[repr(C)]` structs with compile-time size/align asserts.
//! The per-function runtime data (`FunctionScanPerFuncState`) is laid out and
//! owned by the `backend-executor-nodeFunctionscan` crate, so `funcstates`
//! travels here as an opaque pointer.
//!
//! The embedded scan/plan heads reuse the shared
//! [`crate::execnodes::ScanStateData`] / [`crate::execnodes::PlanStateData`] /
//! [`crate::execnodes::Scan`] layouts so a `*mut FunctionScanStateData` can be
//! navigated identically to the C `FunctionScanState`.

use core::ffi::{c_int, c_void};

use crate::execnodes::{PlanStateData, Scan, ScanStateData};
use crate::{Bitmapset, List, MemoryContext, Node, NodeTag};

/// `FunctionScan` plan node (plannodes.h). Embeds the abstract `Scan` base, then
/// the list of `RangeTblFunction`s and the `WITH ORDINALITY` flag.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FunctionScan {
    /// `Scan scan` — the abstract scan-plan base (embeds `Plan`).
    pub scan: Scan,
    /// `List *functions` — list of `RangeTblFunction` nodes.
    pub functions: *mut List,
    /// `bool funcordinality` — WITH ORDINALITY.
    pub funcordinality: bool,
}

/// `RangeTblFunction` (parsenodes.h): one function in a `FUNCTION` RTE, with its
/// expression tree and (optional) column-definition lists.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct RangeTblFunction {
    /// `NodeTag type`
    pub type_: NodeTag,
    /// `Node *funcexpr` — expression tree for the func call.
    pub funcexpr: *mut Node,
    /// `int funccolcount` — number of columns it contributes to the RTE.
    pub funccolcount: c_int,
    /// `List *funccolnames` — column names (list of `String`).
    pub funccolnames: *mut List,
    /// `List *funccoltypes` — OID list of column type OIDs.
    pub funccoltypes: *mut List,
    /// `List *funccoltypmods` — integer list of column typmods.
    pub funccoltypmods: *mut List,
    /// `List *funccolcollations` — OID list of column collation OIDs.
    pub funccolcollations: *mut List,
    /// `Bitmapset *funcparams` — PARAM_EXEC Param IDs affecting this func.
    pub funcparams: *mut Bitmapset,
}

/// `FunctionScanState` (execnodes.h): per-node execution state for a
/// `FunctionScan`. The embedded `ScanStateData` head crosses the boundary; the
/// `funcstates` array (`FunctionScanPerFuncState *`) is laid out by the node
/// crate and travels as an opaque pointer.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FunctionScanStateData {
    /// `ScanState ss` — its first field is the `PlanState`/`NodeTag` head.
    pub ss: ScanStateData,
    /// `int eflags`
    pub eflags: c_int,
    /// `bool ordinality`
    pub ordinality: bool,
    /// `bool simple`
    pub simple: bool,
    /// `int64 ordinal`
    pub ordinal: i64,
    /// `int nfuncs`
    pub nfuncs: c_int,
    /// `struct FunctionScanPerFuncState *funcstates` — array of length `nfuncs`,
    /// owned by the node crate.
    pub funcstates: *mut c_void,
    /// `MemoryContext argcontext`
    pub argcontext: MemoryContext,
}

impl FunctionScanStateData {
    /// `&node->ss.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData {
        &self.ss.ps
    }

    /// `&mut node->ss.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData {
        &mut self.ss.ps
    }
}

const _: () = {
    // FunctionScan plan node: `scan` at offset 0; total 128 bytes on LP64.
    assert!(core::mem::offset_of!(FunctionScan, scan) == 0);
    assert!(core::mem::offset_of!(FunctionScan, functions) == 112);
    assert!(core::mem::offset_of!(FunctionScan, funcordinality) == 120);
    assert!(core::mem::size_of::<FunctionScan>() == 128);
    // RangeTblFunction: 64 bytes on LP64.
    assert!(core::mem::offset_of!(RangeTblFunction, funcexpr) == 8);
    assert!(core::mem::offset_of!(RangeTblFunction, funccolcount) == 16);
    assert!(core::mem::offset_of!(RangeTblFunction, funccolnames) == 24);
    assert!(core::mem::offset_of!(RangeTblFunction, funccoltypes) == 32);
    assert!(core::mem::offset_of!(RangeTblFunction, funccoltypmods) == 40);
    assert!(core::mem::offset_of!(RangeTblFunction, funccolcollations) == 48);
    assert!(core::mem::offset_of!(RangeTblFunction, funcparams) == 56);
    assert!(core::mem::size_of::<RangeTblFunction>() == 64);
    // FunctionScanState: `ss` at offset 0; fields match C offsets; 264 bytes.
    assert!(core::mem::offset_of!(FunctionScanStateData, ss) == 0);
    assert!(core::mem::offset_of!(FunctionScanStateData, eflags) == 224);
    assert!(core::mem::offset_of!(FunctionScanStateData, ordinality) == 228);
    assert!(core::mem::offset_of!(FunctionScanStateData, simple) == 229);
    assert!(core::mem::offset_of!(FunctionScanStateData, ordinal) == 232);
    assert!(core::mem::offset_of!(FunctionScanStateData, nfuncs) == 240);
    assert!(core::mem::offset_of!(FunctionScanStateData, funcstates) == 248);
    assert!(core::mem::offset_of!(FunctionScanStateData, argcontext) == 256);
    assert!(core::mem::size_of::<FunctionScanStateData>() == 264);
    // The embedded ScanState/PlanState heads keep their C offsets.
    assert!(core::mem::offset_of!(ScanStateData, ss_currentRelation) == 200);
    assert!(core::mem::offset_of!(ScanStateData, ss_ScanTupleSlot) == 216);
    assert!(core::mem::size_of::<ScanStateData>() == 224);
    assert!(core::mem::size_of::<PlanStateData>() == 200);
};
