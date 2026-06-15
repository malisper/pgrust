//! ForeignScan / CustomScan / Material / NamedTuplestoreScan node ABI vocabulary.
//!
//! These four executor-node `State` structs were previously opaque `[u8; 0]`
//! handles. The per-node executor crates (`nodeForeignscan.c`, `nodeCustom.c`,
//! `nodeMaterial.c`, `nodeNamedtuplestorescan.c`) field-access them, so here they
//! are laid out as faithful `#[repr(C)]` structs matching PostgreSQL 18.3, with
//! compile-time size/align asserts.
//!
//! The embedded scan/plan heads reuse the shared
//! [`crate::execnodes::ScanStateData`] / [`crate::execnodes::PlanStateData`] /
//! [`crate::execnodes::Scan`] / [`crate::execnodes::PlanNode`] layouts so a
//! `*mut <Node>StateData` can be navigated identically to the C struct.
//!
//! The FDW provider callbacks (`FdwRoutine`) and custom-scan provider callbacks
//! (`CustomExecMethods`) are *genuinely external* — they are filled in by the
//! FDW / custom-scan extension and invoked through the routine pointer (e.g.
//! `node->fdwroutine->IterateForeignScan(node)`). The executor never lays out a
//! routine by value; it only navigates the function-pointer table. All routine
//! members are pointer-sized, so the table layout is independent of the exact
//! callback signature: scan-/exec-related members the executor invokes are typed
//! precisely, and the planner-/EXPLAIN-/ANALYZE-only members (never reached by
//! the executor node layer) carry the layout-equivalent opaque [`FdwFnPtr`].

use core::ffi::{c_int, c_void};

use crate::execnodes::{PlanNode, PlanStateData, Scan, ScanStateData};
use crate::funcapi::Tuplestorestate;
use crate::heaptuple::TupleDesc;
use crate::nodemodifytable_abi::CmdType;
use crate::nodemodifytable_state::ResultRelInfo;
use crate::{Bitmapset, List, NodeTag, Oid, TupleTableSlot};

// ===========================================================================
// FdwRoutine (foreign/fdwapi.h) — the FDW provider callback table.
// ===========================================================================

/// Layout-equivalent opaque FDW callback function pointer.
///
/// Every member of `FdwRoutine` is a C function pointer (pointer-sized). The
/// planner-/EXPLAIN-/ANALYZE-/import-only members are never invoked by the
/// executor node layer, so they carry this opaque type purely to preserve the
/// routine's field order and size; the FDW provider installs the real callbacks.
pub type FdwFnPtr = Option<unsafe extern "C" fn()>;

/// `BeginForeignScan_function` — `void (*)(ForeignScanState *node, int eflags)`.
pub type BeginForeignScan_function =
    Option<unsafe extern "C" fn(node: *mut ForeignScanState, eflags: c_int)>;
/// `IterateForeignScan_function` — `TupleTableSlot *(*)(ForeignScanState *node)`.
pub type IterateForeignScan_function =
    Option<unsafe extern "C" fn(node: *mut ForeignScanState) -> *mut TupleTableSlot>;
/// `ReScanForeignScan_function` — `void (*)(ForeignScanState *node)`.
pub type ReScanForeignScan_function = Option<unsafe extern "C" fn(node: *mut ForeignScanState)>;
/// `EndForeignScan_function` — `void (*)(ForeignScanState *node)`.
pub type EndForeignScan_function = Option<unsafe extern "C" fn(node: *mut ForeignScanState)>;
/// `RecheckForeignScan_function` — `bool (*)(ForeignScanState *node, TupleTableSlot *slot)`.
pub type RecheckForeignScan_function =
    Option<unsafe extern "C" fn(node: *mut ForeignScanState, slot: *mut TupleTableSlot) -> bool>;
/// `BeginDirectModify_function` — `void (*)(ForeignScanState *node, int eflags)`.
pub type BeginDirectModify_function =
    Option<unsafe extern "C" fn(node: *mut ForeignScanState, eflags: c_int)>;
/// `IterateDirectModify_function` — `TupleTableSlot *(*)(ForeignScanState *node)`.
pub type IterateDirectModify_function =
    Option<unsafe extern "C" fn(node: *mut ForeignScanState) -> *mut TupleTableSlot>;
/// `EndDirectModify_function` — `void (*)(ForeignScanState *node)`.
pub type EndDirectModify_function = Option<unsafe extern "C" fn(node: *mut ForeignScanState)>;
/// `ShutdownForeignScan_function` — `void (*)(ForeignScanState *node)`.
pub type ShutdownForeignScan_function = Option<unsafe extern "C" fn(node: *mut ForeignScanState)>;

/// `FdwRoutine` (foreign/fdwapi.h) — the foreign-data-wrapper handler table.
///
/// Field order matches PG 18.3 exactly. The executor node layer invokes only the
/// scan/direct-modify/shutdown members (typed precisely above); all other
/// members are planner-/modify-/EXPLAIN-/ANALYZE-/import-/parallel-/async-only
/// and carry the layout-equivalent opaque [`FdwFnPtr`].
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FdwRoutine {
    /// `NodeTag type`
    pub type_: NodeTag,

    /* Functions for scanning foreign tables */
    pub GetForeignRelSize: FdwFnPtr,
    pub GetForeignPaths: FdwFnPtr,
    pub GetForeignPlan: FdwFnPtr,
    pub BeginForeignScan: BeginForeignScan_function,
    pub IterateForeignScan: IterateForeignScan_function,
    pub ReScanForeignScan: ReScanForeignScan_function,
    pub EndForeignScan: EndForeignScan_function,

    /* Functions for remote-join planning */
    pub GetForeignJoinPaths: FdwFnPtr,

    /* Functions for remote upper-relation (post scan/join) planning */
    pub GetForeignUpperPaths: FdwFnPtr,

    /* Functions for updating foreign tables */
    pub AddForeignUpdateTargets: FdwFnPtr,
    pub PlanForeignModify: FdwFnPtr,
    pub BeginForeignModify: FdwFnPtr,
    pub ExecForeignInsert: FdwFnPtr,
    pub ExecForeignBatchInsert: FdwFnPtr,
    pub GetForeignModifyBatchSize: FdwFnPtr,
    pub ExecForeignUpdate: FdwFnPtr,
    pub ExecForeignDelete: FdwFnPtr,
    pub EndForeignModify: FdwFnPtr,
    pub BeginForeignInsert: FdwFnPtr,
    pub EndForeignInsert: FdwFnPtr,
    pub IsForeignRelUpdatable: FdwFnPtr,
    pub PlanDirectModify: FdwFnPtr,
    pub BeginDirectModify: BeginDirectModify_function,
    pub IterateDirectModify: IterateDirectModify_function,
    pub EndDirectModify: EndDirectModify_function,

    /* Functions for SELECT FOR UPDATE/SHARE row locking */
    pub GetForeignRowMarkType: FdwFnPtr,
    pub RefetchForeignRow: FdwFnPtr,
    pub RecheckForeignScan: RecheckForeignScan_function,

    /* Support functions for EXPLAIN */
    pub ExplainForeignScan: FdwFnPtr,
    pub ExplainForeignModify: FdwFnPtr,
    pub ExplainDirectModify: FdwFnPtr,

    /* Support functions for ANALYZE */
    pub AnalyzeForeignTable: FdwFnPtr,

    /* Support functions for IMPORT FOREIGN SCHEMA */
    pub ImportForeignSchema: FdwFnPtr,

    /* Support functions for TRUNCATE */
    pub ExecForeignTruncate: FdwFnPtr,

    /* Support functions for parallelism under Gather node */
    pub IsForeignScanParallelSafe: FdwFnPtr,
    pub EstimateDSMForeignScan: FdwFnPtr,
    pub InitializeDSMForeignScan: FdwFnPtr,
    pub ReInitializeDSMForeignScan: FdwFnPtr,
    pub InitializeWorkerForeignScan: FdwFnPtr,
    pub ShutdownForeignScan: ShutdownForeignScan_function,

    /* Support functions for path reparameterization. */
    pub ReparameterizeForeignPathByChild: FdwFnPtr,

    /* Support functions for asynchronous execution */
    pub IsForeignPathAsyncCapable: FdwFnPtr,
    pub ForeignAsyncRequest: FdwFnPtr,
    pub ForeignAsyncConfigureWait: FdwFnPtr,
    pub ForeignAsyncNotify: FdwFnPtr,
}

// ===========================================================================
// ForeignScan plan node (nodes/plannodes.h) and ForeignScanState (execnodes.h).
// ===========================================================================

/// `ForeignScan` plan node (plannodes.h). Embeds the abstract `Scan` base.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ForeignScan {
    /// `Scan scan` — the abstract scan-plan base (embeds `Plan`).
    pub scan: Scan,
    /// `CmdType operation` — SELECT/INSERT/UPDATE/DELETE.
    pub operation: CmdType,
    /// `Index resultRelation` — direct modification target's RT index.
    pub resultRelation: crate::Index,
    /// `Oid checkAsUser` — user to perform the scan as; 0 = current user.
    pub checkAsUser: Oid,
    /// `Oid fs_server` — OID of foreign server.
    pub fs_server: Oid,
    /// `List *fdw_exprs` — expressions that FDW may evaluate.
    pub fdw_exprs: *mut List,
    /// `List *fdw_private` — private data for FDW.
    pub fdw_private: *mut List,
    /// `List *fdw_scan_tlist` — optional tlist describing scan tuple.
    pub fdw_scan_tlist: *mut List,
    /// `List *fdw_recheck_quals` — original quals not in `scan.plan.qual`.
    pub fdw_recheck_quals: *mut List,
    /// `Bitmapset *fs_relids` — base+OJ RTIs generated by this scan.
    pub fs_relids: *mut Bitmapset,
    /// `Bitmapset *fs_base_relids` — base RTIs generated by this scan.
    pub fs_base_relids: *mut Bitmapset,
    /// `bool fsSystemCol` — true if any "system column" is needed.
    pub fsSystemCol: bool,
}

/// `ForeignScanState` (execnodes.h) — the foreign-scan executor node state.
///
/// The leading [`ScanStateData`] head's first member is a `NodeTag`, so a
/// `*mut ForeignScanState` is also a valid `Node *` / `PlanState *`. The FDW
/// callback table (`fdwroutine`) and the FDW's private state (`fdw_state`) are
/// genuinely external; they cross as raw pointers.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ForeignScanState {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData,
    /// `ExprState *fdw_recheck_quals` — original quals not in `ss.ps.qual`.
    pub fdw_recheck_quals: *mut crate::ExprState,
    /// `Size pscan_len` — size of parallel coordination information.
    pub pscan_len: usize,
    /// `ResultRelInfo *resultRelInfo` — result rel info, if UPDATE or DELETE.
    pub resultRelInfo: *mut ResultRelInfo,
    /// `struct FdwRoutine *fdwroutine` — the FDW handler table.
    pub fdwroutine: *mut FdwRoutine,
    /// `void *fdw_state` — foreign-data wrapper can keep state here.
    pub fdw_state: *mut c_void,
}

impl ForeignScanState {
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

// ===========================================================================
// CustomExecMethods (nodes/extensible.h), CustomScan (plannodes.h),
// CustomScanState (execnodes.h).
// ===========================================================================

/// `EState *` — opaque to this module (defined in `execnodes`).
type EState = crate::EState;
/// `ParallelContext *` — opaque DSM coordination context.
type ParallelContext = c_void;
/// `shm_toc *` — opaque shared-memory table of contents.
type shm_toc = c_void;
/// `ExplainState *` — opaque EXPLAIN output state.
type ExplainState = c_void;

/// `BeginCustomScan` — `void (*)(CustomScanState *node, EState *estate, int eflags)`.
pub type BeginCustomScan_function =
    Option<unsafe extern "C" fn(node: *mut CustomScanState, estate: *mut EState, eflags: c_int)>;
/// `ExecCustomScan` — `TupleTableSlot *(*)(CustomScanState *node)`.
pub type ExecCustomScan_function =
    Option<unsafe extern "C" fn(node: *mut CustomScanState) -> *mut TupleTableSlot>;
/// `EndCustomScan` — `void (*)(CustomScanState *node)`.
pub type EndCustomScan_function = Option<unsafe extern "C" fn(node: *mut CustomScanState)>;
/// `ReScanCustomScan` — `void (*)(CustomScanState *node)`.
pub type ReScanCustomScan_function = Option<unsafe extern "C" fn(node: *mut CustomScanState)>;
/// `MarkPosCustomScan` — `void (*)(CustomScanState *node)`.
pub type MarkPosCustomScan_function = Option<unsafe extern "C" fn(node: *mut CustomScanState)>;
/// `RestrPosCustomScan` — `void (*)(CustomScanState *node)`.
pub type RestrPosCustomScan_function = Option<unsafe extern "C" fn(node: *mut CustomScanState)>;
/// `EstimateDSMCustomScan` — `Size (*)(CustomScanState *node, ParallelContext *pcxt)`.
pub type EstimateDSMCustomScan_function =
    Option<unsafe extern "C" fn(node: *mut CustomScanState, pcxt: *mut ParallelContext) -> usize>;
/// `InitializeDSMCustomScan` — `void (*)(CustomScanState *node, ParallelContext *pcxt, void *coordinate)`.
pub type InitializeDSMCustomScan_function = Option<
    unsafe extern "C" fn(
        node: *mut CustomScanState,
        pcxt: *mut ParallelContext,
        coordinate: *mut c_void,
    ),
>;
/// `ReInitializeDSMCustomScan` — same signature as `InitializeDSMCustomScan`.
pub type ReInitializeDSMCustomScan_function = Option<
    unsafe extern "C" fn(
        node: *mut CustomScanState,
        pcxt: *mut ParallelContext,
        coordinate: *mut c_void,
    ),
>;
/// `InitializeWorkerCustomScan` — `void (*)(CustomScanState *node, shm_toc *toc, void *coordinate)`.
pub type InitializeWorkerCustomScan_function = Option<
    unsafe extern "C" fn(node: *mut CustomScanState, toc: *mut shm_toc, coordinate: *mut c_void),
>;
/// `ShutdownCustomScan` — `void (*)(CustomScanState *node)`.
pub type ShutdownCustomScan_function = Option<unsafe extern "C" fn(node: *mut CustomScanState)>;
/// `ExplainCustomScan` — `void (*)(CustomScanState *node, List *ancestors, ExplainState *es)`.
pub type ExplainCustomScan_function = Option<
    unsafe extern "C" fn(node: *mut CustomScanState, ancestors: *mut List, es: *mut ExplainState),
>;

/// `CustomExecMethods` (nodes/extensible.h) — the custom-scan provider's
/// executor method table. Field order matches PG 18.3 exactly.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CustomExecMethods {
    /// `const char *CustomName`.
    pub CustomName: *const core::ffi::c_char,

    /* Required executor methods */
    pub BeginCustomScan: BeginCustomScan_function,
    pub ExecCustomScan: ExecCustomScan_function,
    pub EndCustomScan: EndCustomScan_function,
    pub ReScanCustomScan: ReScanCustomScan_function,

    /* Optional methods: needed if mark/restore is supported */
    pub MarkPosCustomScan: MarkPosCustomScan_function,
    pub RestrPosCustomScan: RestrPosCustomScan_function,

    /* Optional methods: needed if parallel execution is supported */
    pub EstimateDSMCustomScan: EstimateDSMCustomScan_function,
    pub InitializeDSMCustomScan: InitializeDSMCustomScan_function,
    pub ReInitializeDSMCustomScan: ReInitializeDSMCustomScan_function,
    pub InitializeWorkerCustomScan: InitializeWorkerCustomScan_function,
    pub ShutdownCustomScan: ShutdownCustomScan_function,

    /* Optional: print additional information in EXPLAIN */
    pub ExplainCustomScan: ExplainCustomScan_function,
}

// The `CustomScan` plan node (plannodes.h) is defined in
// [`crate::nodeindexscan::CustomScan`]; the executor's
// `ExecSupportsBackwardScan`/`ExecSupportsMarkRestore` read its `flags`.

/// `CustomScanState` (execnodes.h) — the custom-scan executor node state.
///
/// The leading [`ScanStateData`] head's first member is a `NodeTag`, so a
/// `*mut CustomScanState` is also a valid `Node *` / `PlanState *`. The provider
/// method table (`methods`) and slot ops (`slotOps`) are genuinely external.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CustomScanState {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData,
    /// `uint32 flags` — mask of `CUSTOMPATH_*` flags.
    pub flags: crate::uint32,
    /// `List *custom_ps` — list of child `PlanState` nodes, if any.
    pub custom_ps: *mut List,
    /// `Size pscan_len` — size of parallel coordination information.
    pub pscan_len: usize,
    /// `const struct CustomExecMethods *methods` — the provider's method table.
    pub methods: *const CustomExecMethods,
    /// `const struct TupleTableSlotOps *slotOps`.
    pub slotOps: *const c_void,
}

impl CustomScanState {
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

// ===========================================================================
// Material plan node (plannodes.h) and MaterialState (execnodes.h).
// ===========================================================================

/// `Material` plan node (plannodes.h):
///
/// ```c
/// typedef struct Material { Plan plan; } Material;
/// ```
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Material {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: PlanNode,
}

/// `MaterialState` (execnodes.h):
///
/// ```c
/// typedef struct MaterialState {
///     ScanState   ss;                 /* its first field is NodeTag */
///     int         eflags;             /* capability flags to pass to tuplestore */
///     bool        eof_underlying;     /* reached end of underlying plan? */
///     Tuplestorestate *tuplestorestate;
/// } MaterialState;
/// ```
///
/// The leading [`ScanStateData`] head's first member is a `NodeTag`, so a
/// `*mut MaterialState` is also a valid `Node *` / `PlanState *`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MaterialState {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData,
    /// `int eflags` — capability flags to pass to the tuplestore.
    pub eflags: c_int,
    /// `bool eof_underlying` — reached end of underlying plan?
    pub eof_underlying: bool,
    /// `Tuplestorestate *tuplestorestate` — the materialized rows.
    pub tuplestorestate: *mut Tuplestorestate,
}

impl MaterialState {
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

// ===========================================================================
// NamedTuplestoreScan plan node (plannodes.h) and NamedTuplestoreScanState.
// ===========================================================================

/// `NamedTuplestoreScan` plan node (plannodes.h):
///
/// ```c
/// typedef struct NamedTuplestoreScan {
///     Scan        scan;
///     char       *enrname;    /* Name given to Ephemeral Named Relation */
/// } NamedTuplestoreScan;
/// ```
#[repr(C)]
#[derive(Clone, Copy)]
pub struct NamedTuplestoreScan {
    /// `Scan scan` — the abstract scan-plan base (embeds `Plan`).
    pub scan: Scan,
    /// `char *enrname` — name given to the Ephemeral Named Relation.
    pub enrname: *mut core::ffi::c_char,
}

/// `NamedTuplestoreScanState` (execnodes.h):
///
/// ```c
/// typedef struct NamedTuplestoreScanState {
///     ScanState   ss;             /* its first field is NodeTag */
///     int         readptr;        /* index of my tuplestore read pointer */
///     TupleDesc   tupdesc;        /* format of the tuples in the tuplestore */
///     Tuplestorestate *relation;  /* the rows */
/// } NamedTuplestoreScanState;
/// ```
///
/// The leading [`ScanStateData`] head's first member is a `NodeTag`, so a
/// `*mut NamedTuplestoreScanState` is also a valid `Node *` / `PlanState *`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct NamedTuplestoreScanState {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData,
    /// `int readptr` — index of my tuplestore read pointer.
    pub readptr: c_int,
    /// `TupleDesc tupdesc` — format of the tuples in the tuplestore.
    pub tupdesc: TupleDesc,
    /// `Tuplestorestate *relation` — the rows.
    pub relation: *mut Tuplestorestate,
}

impl NamedTuplestoreScanState {
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

// ===========================================================================
// Layout asserts (LP64): each State's embedded ScanState leads at offset 0 so a
// pointer is a valid Node*/PlanState*, and the trailing fields keep C offsets.
// ===========================================================================
const _: () = {
    // ScanState size/PlanState size used as reference offsets.
    let sss = core::mem::size_of::<ScanStateData>(); // 224 on LP64
    assert!(sss == 224);

    // FdwRoutine: NodeTag (4) + pad (4) + 45 function pointers (8 each).
    assert!(core::mem::offset_of!(FdwRoutine, type_) == 0);
    assert!(core::mem::offset_of!(FdwRoutine, GetForeignRelSize) == 8);
    assert!(core::mem::offset_of!(FdwRoutine, BeginForeignScan) == 32);
    assert!(core::mem::offset_of!(FdwRoutine, IterateForeignScan) == 40);
    assert!(core::mem::size_of::<FdwRoutine>() == 8 + 45 * 8);
    assert!(core::mem::align_of::<FdwRoutine>() == 8);

    // CustomExecMethods: CustomName ptr + 12 function pointers = 13 ptrs.
    assert!(core::mem::offset_of!(CustomExecMethods, CustomName) == 0);
    assert!(core::mem::offset_of!(CustomExecMethods, BeginCustomScan) == 8);
    assert!(core::mem::size_of::<CustomExecMethods>() == 13 * 8);

    // ForeignScanState: ss at 0; then ExprState* (224), Size (232),
    // ResultRelInfo* (240), FdwRoutine* (248), fdw_state (256); 264 bytes.
    assert!(core::mem::offset_of!(ForeignScanState, ss) == 0);
    assert!(core::mem::offset_of!(ForeignScanState, fdw_recheck_quals) == 224);
    assert!(core::mem::offset_of!(ForeignScanState, pscan_len) == 232);
    assert!(core::mem::offset_of!(ForeignScanState, resultRelInfo) == 240);
    assert!(core::mem::offset_of!(ForeignScanState, fdwroutine) == 248);
    assert!(core::mem::offset_of!(ForeignScanState, fdw_state) == 256);
    assert!(core::mem::size_of::<ForeignScanState>() == 264);

    // CustomScanState: ss at 0; flags (224), pad, custom_ps (232),
    // pscan_len (240), methods (248), slotOps (256); 264 bytes.
    assert!(core::mem::offset_of!(CustomScanState, ss) == 0);
    assert!(core::mem::offset_of!(CustomScanState, flags) == 224);
    assert!(core::mem::offset_of!(CustomScanState, custom_ps) == 232);
    assert!(core::mem::offset_of!(CustomScanState, pscan_len) == 240);
    assert!(core::mem::offset_of!(CustomScanState, methods) == 248);
    assert!(core::mem::offset_of!(CustomScanState, slotOps) == 256);
    assert!(core::mem::size_of::<CustomScanState>() == 264);

    // MaterialState: ss at 0; eflags (224), eof_underlying (228),
    // tuplestorestate (232); 240 bytes.
    assert!(core::mem::offset_of!(MaterialState, ss) == 0);
    assert!(core::mem::offset_of!(MaterialState, eflags) == 224);
    assert!(core::mem::offset_of!(MaterialState, eof_underlying) == 228);
    assert!(core::mem::offset_of!(MaterialState, tuplestorestate) == 232);
    assert!(core::mem::size_of::<MaterialState>() == 240);

    // NamedTuplestoreScanState: ss at 0; readptr (224), tupdesc (232),
    // relation (240); 248 bytes.
    assert!(core::mem::offset_of!(NamedTuplestoreScanState, ss) == 0);
    assert!(core::mem::offset_of!(NamedTuplestoreScanState, readptr) == 224);
    assert!(core::mem::offset_of!(NamedTuplestoreScanState, tupdesc) == 232);
    assert!(core::mem::offset_of!(NamedTuplestoreScanState, relation) == 240);
    assert!(core::mem::size_of::<NamedTuplestoreScanState>() == 248);

    // ForeignScan plan: Scan base at 0; operation/resultRelation are the first
    // trailing fields after the Scan head.
    assert!(core::mem::offset_of!(ForeignScan, scan) == 0);
    assert!(core::mem::offset_of!(ForeignScan, operation) == core::mem::size_of::<Scan>());

    // Material plan: just a Plan.
    assert!(core::mem::offset_of!(Material, plan) == 0);
    assert!(core::mem::size_of::<Material>() == core::mem::size_of::<PlanNode>());

    // NamedTuplestoreScan plan: Scan base at 0; enrname follows.
    assert!(core::mem::offset_of!(NamedTuplestoreScan, scan) == 0);
    assert!(core::mem::offset_of!(NamedTuplestoreScan, enrname) == core::mem::size_of::<Scan>());
};
