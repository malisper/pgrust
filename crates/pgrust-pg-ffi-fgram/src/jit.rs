use core::ffi::{c_int, c_void};
use core::mem::offset_of;

pub use crate::execexpr::ExprState;
use crate::executor::TupleTableSlot;
use crate::{instr_time, Instrumentation, NodeTag, Size, WorkerInstrumentation};

pub const PGJIT_NONE: c_int = 0;
pub const PGJIT_PERFORM: c_int = 1 << 0;
pub const PGJIT_OPT3: c_int = 1 << 1;
pub const PGJIT_INLINE: c_int = 1 << 2;
pub const PGJIT_EXPR: c_int = 1 << 3;
pub const PGJIT_DEFORM: c_int = 1 << 4;

pub const OFFSET_EXPRSTATE_PARENT: usize = offset_of!(ExprState, parent);
pub const OFFSET_PLANSTATE_STATE: usize = offset_of!(PlanState, state);
pub const OFFSET_ESTATE_ES_JIT_FLAGS: usize = offset_of!(EState, es_jit_flags);

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct JitInstrumentation {
    pub created_functions: Size,
    pub generation_counter: instr_time,
    pub deform_counter: instr_time,
    pub inlining_counter: instr_time,
    pub optimization_counter: instr_time,
    pub emission_counter: instr_time,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SharedJitInstrumentation {
    pub num_workers: c_int,
    pub jit_instr: [JitInstrumentation; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct JitContext {
    flags: c_int,
    instr: JitInstrumentation,
}

impl JitContext {
    pub const fn new(flags: c_int, instr: JitInstrumentation) -> Self {
        Self { flags, instr }
    }

    pub const fn flags(&self) -> c_int {
        self.flags
    }

    pub const fn instr(&self) -> &JitInstrumentation {
        &self.instr
    }

    pub fn instr_mut(&mut self) -> &mut JitInstrumentation {
        &mut self.instr
    }
}

pub type JitProviderResetAfterErrorCB = Option<unsafe extern "C" fn()>;
pub type JitProviderReleaseContextCB = Option<unsafe extern "C" fn(*mut JitContext)>;
pub type JitProviderCompileExprCB = Option<unsafe extern "C" fn(*mut ExprState) -> bool>;
pub type JitProviderInit = Option<unsafe extern "C" fn(*mut JitProviderCallbacks)>;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct JitProviderCallbacks {
    pub reset_after_error: JitProviderResetAfterErrorCB,
    pub release_context: JitProviderReleaseContextCB,
    pub compile_expr: JitProviderCompileExprCB,
}

// `ExprState` now lives in `crate::execexpr` (full PG layout). `jit.rs` re-uses
// it for its `offset_of!(ExprState, parent)` JIT-offset constant.

/// `PlanState` — the abstract base of every per-node execution state
/// (`execnodes.h`). Mirrors the full PostgreSQL 18.3 `repr(C)` layout: the
/// JIT-relevant prefix (`type_` .. `worker_jit_instrument`) keeps its exact
/// offsets, and the remaining "common structural" fields follow so the executor
/// node crates can read `qual`, `ps_ExprContext`, `ps_ResultTupleSlot`, ...
/// faithfully.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PlanState {
    pub type_: NodeTag,
    pub plan: *mut c_void,
    state: *mut EState,
    pub ExecProcNode: *mut c_void,
    pub ExecProcNodeReal: *mut c_void,
    pub instrument: *mut Instrumentation,
    pub worker_instrument: *mut WorkerInstrumentation,
    pub worker_jit_instrument: *mut SharedJitInstrumentation,
    // --- common structural data for all Plan types ---
    /// `ExprState *qual` — boolean qual condition.
    pub qual: *mut c_void,
    /// `struct PlanState *lefttree` — input plan tree.
    pub lefttree: *mut PlanState,
    /// `struct PlanState *righttree`.
    pub righttree: *mut PlanState,
    /// `List *initPlan` — Init SubPlanState nodes.
    pub initPlan: *mut c_void,
    /// `List *subPlan` — SubPlanState nodes in my expressions.
    pub subPlan: *mut c_void,
    /// `Bitmapset *chgParam` — set of IDs of changed Params.
    pub chgParam: *mut c_void,
    /// `TupleDesc ps_ResultTupleDesc` — node's return type.
    pub ps_ResultTupleDesc: *mut c_void,
    /// `TupleTableSlot *ps_ResultTupleSlot` — slot for my result tuples.
    pub ps_ResultTupleSlot: *mut TupleTableSlot,
    /// `ExprContext *ps_ExprContext` — node's expression-evaluation context.
    pub ps_ExprContext: *mut c_void,
    /// `ProjectionInfo *ps_ProjInfo` — info for doing tuple projection.
    pub ps_ProjInfo: *mut c_void,
    /// `bool async_capable`.
    pub async_capable: bool,
    /// `TupleDesc scandesc` — scanslot's descriptor if known.
    pub scandesc: *mut c_void,
    /// `const TupleTableSlotOps *scanops`.
    pub scanops: *const c_void,
    /// `const TupleTableSlotOps *outerops`.
    pub outerops: *const c_void,
    /// `const TupleTableSlotOps *innerops`.
    pub innerops: *const c_void,
    /// `const TupleTableSlotOps *resultops`.
    pub resultops: *const c_void,
    pub scanopsfixed: bool,
    pub outeropsfixed: bool,
    pub inneropsfixed: bool,
    pub resultopsfixed: bool,
    pub scanopsset: bool,
    pub outeropsset: bool,
    pub inneropsset: bool,
    pub resultopsset: bool,
}

impl PlanState {
    pub const fn new_for_jit(state: *mut EState) -> Self {
        Self {
            type_: 0,
            plan: core::ptr::null_mut(),
            state,
            ExecProcNode: core::ptr::null_mut(),
            ExecProcNodeReal: core::ptr::null_mut(),
            instrument: core::ptr::null_mut(),
            worker_instrument: core::ptr::null_mut(),
            worker_jit_instrument: core::ptr::null_mut(),
            qual: core::ptr::null_mut(),
            lefttree: core::ptr::null_mut(),
            righttree: core::ptr::null_mut(),
            initPlan: core::ptr::null_mut(),
            subPlan: core::ptr::null_mut(),
            chgParam: core::ptr::null_mut(),
            ps_ResultTupleDesc: core::ptr::null_mut(),
            ps_ResultTupleSlot: core::ptr::null_mut(),
            ps_ExprContext: core::ptr::null_mut(),
            ps_ProjInfo: core::ptr::null_mut(),
            async_capable: false,
            scandesc: core::ptr::null_mut(),
            scanops: core::ptr::null(),
            outerops: core::ptr::null(),
            innerops: core::ptr::null(),
            resultops: core::ptr::null(),
            scanopsfixed: false,
            outeropsfixed: false,
            inneropsfixed: false,
            resultopsfixed: false,
            scanopsset: false,
            outeropsset: false,
            inneropsset: false,
            resultopsset: false,
        }
    }

    pub fn state(&self) -> Option<&EState> {
        unsafe { self.state.as_ref() }
    }

    pub fn state_ptr(&self) -> *mut EState {
        self.state
    }

    pub fn set_state(&mut self, state: *mut EState) {
        self.state = state;
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct EState {
    _prefix: [usize; 38],
    pub es_jit_flags: c_int,
    pub es_jit: *mut JitContext,
    pub es_jit_worker_instr: *mut JitInstrumentation,
}

impl EState {
    pub const fn new_for_jit(es_jit_flags: c_int) -> Self {
        Self {
            _prefix: [0; 38],
            es_jit_flags,
            es_jit: core::ptr::null_mut(),
            es_jit_worker_instr: core::ptr::null_mut(),
        }
    }

    /// `EState.es_direction` — current scan direction (`ScanDirection`,
    /// byte offset 4, right after the `NodeTag` header).
    pub fn es_direction(&self) -> crate::ScanDirection {
        // Offset 4: NodeTag (i32) then ScanDirection (i32 enum).
        let raw = unsafe {
            core::ptr::read_unaligned((self as *const EState as *const u8).add(4).cast::<c_int>())
        };
        match raw {
            -1 => crate::ScanDirection::BackwardScanDirection,
            1 => crate::ScanDirection::ForwardScanDirection,
            _ => crate::ScanDirection::NoMovementScanDirection,
        }
    }

    /// Set `EState.es_direction` — current scan direction (`ScanDirection`,
    /// byte offset 4, right after the `NodeTag` header). Mirrors the C
    /// assignment `estate->es_direction = dir`.
    pub fn set_es_direction(&mut self, dir: crate::ScanDirection) {
        // Offset 4: NodeTag (i32) then ScanDirection (i32 enum).
        let raw: c_int = dir as c_int;
        unsafe {
            core::ptr::write_unaligned(
                (self as *mut EState as *mut u8).add(4).cast::<c_int>(),
                raw,
            );
        }
    }

    /// `EState.es_snapshot` — time qual to use (`Snapshot`, byte offset 8).
    pub fn es_snapshot(&self) -> *mut c_void {
        unsafe {
            core::ptr::read_unaligned(
                (self as *const EState as *const u8)
                    .add(8)
                    .cast::<*mut c_void>(),
            )
        }
    }

    /// `EState.es_param_exec_vals` — `ParamExecData *`, the values of internal
    /// (PARAM_EXEC) params (byte offset 176: after the `NodeTag`, `es_direction`
    /// and 22 pointer-/Index-/CommandId-sized fields, ending with
    /// `es_param_list_info`). The array is indexed by `Param.paramid`.
    pub fn es_param_exec_vals(&self) -> *mut crate::ParamExecData {
        unsafe {
            core::ptr::read_unaligned(
                (self as *const EState as *const u8)
                    .add(176)
                    .cast::<*mut crate::ParamExecData>(),
            )
        }
    }

    /// `EState.es_subplanstates` — `List *` of `PlanState` for SubPlans (byte
    /// offset 248). Used by `ExecInitCteScan` to find the already-initialized
    /// plan for the CTE query.
    pub fn es_subplanstates(&self) -> *mut crate::List {
        unsafe {
            core::ptr::read_unaligned(
                (self as *const EState as *const u8)
                    .add(248)
                    .cast::<*mut crate::List>(),
            )
        }
    }

    /// `EState.es_query_cxt` — the per-query `MemoryContext` in which the `EState`
    /// lives (byte offset 192: after the `NodeTag`, `es_direction` and the
    /// pointer-/Index-/CommandId-sized basic-state fields, the parameter fields
    /// `es_param_list_info`/`es_param_exec_vals` and `es_queryEnv`). Used by
    /// `ExecInitRecursiveUnion` to build the seen-tuples hash table in a
    /// query-lived metacontext.
    pub fn es_query_cxt(&self) -> crate::MemoryContext {
        unsafe {
            core::ptr::read_unaligned(
                (self as *const EState as *const u8)
                    .add(192)
                    .cast::<crate::MemoryContext>(),
            )
        }
    }

    /// `EState.es_range_table` — `List *` of `RangeTblEntry` (byte offset 24,
    /// after the `NodeTag`, `es_direction`, `es_snapshot` and
    /// `es_crosscheck_snapshot`). Used by `exec_rt_fetch`.
    pub fn es_range_table(&self) -> *mut crate::List {
        unsafe {
            core::ptr::read_unaligned(
                (self as *const EState as *const u8)
                    .add(24)
                    .cast::<*mut crate::List>(),
            )
        }
    }

    /// `EState.es_unpruned_relids` — `Bitmapset *` of RT indexes that survived
    /// initial pruning (byte offset 96). Used by `ExecInitLockRows` to skip
    /// pruned child rowmarks.
    pub fn es_unpruned_relids(&self) -> *mut crate::Bitmapset {
        unsafe {
            core::ptr::read_unaligned(
                (self as *const EState as *const u8)
                    .add(96)
                    .cast::<*mut crate::Bitmapset>(),
            )
        }
    }

    /// `EState.es_output_cid` — `CommandId` to mark inserted/deleted tuples with
    /// (byte offset 120). Passed to `table_tuple_lock`.
    pub fn es_output_cid(&self) -> crate::CommandId {
        unsafe {
            core::ptr::read_unaligned(
                (self as *const EState as *const u8)
                    .add(120)
                    .cast::<crate::CommandId>(),
            )
        }
    }
}
