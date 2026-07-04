use core::cell::{Cell, UnsafeCell};
use core::ptr::NonNull;

use ::mcx::{Mcx, MemoryContext};

use crate::fcinfo::{FmNode, FmNodePtr, FunctionCallInfoBaseData};

// nodetags.h value, parity-asserted in fmgr_core tests.
pub const T_AGG_STATE: u32 = 429;

/// The trans/final-fn-visible slice of C's `AggState`: rides `fcinfo->context`
/// (C `AggCheckCallContext`) and owns the aggcontext arena. Wholesale reset:
/// allocating transfns assert `!needs_drop` for their state (docs/no-drop.md).
/// `cur_agg` is C's curperagg/curpertrans pair erased to (Aggref ptr,
/// aggshared); typed access lives in nodeagg (AggGetAggref/AggStateIsShared).
/// `callbacks` is C's curaggcontext ExprContext_CB chain (AggRegisterCallback),
/// fired LIFO at group reset and at node teardown.
#[repr(C)]
pub struct AggStateNode {
    node: FmNode,
    aggcontext: MemoryContext,
    cur_agg: Cell<Option<(NonNull<()>, bool)>>,
    callbacks: UnsafeCell<::alloc::vec::Vec<(unsafe fn(*mut ()), *mut ())>>,
}

impl AggStateNode {
    pub fn new(aggcontext: MemoryContext) -> Self {
        Self {
            node: FmNode { tag: T_AGG_STATE },
            aggcontext,
            cur_agg: Cell::new(None),
            callbacks: UnsafeCell::new(::alloc::vec::Vec::new()),
        }
    }

    pub fn fm_node_ptr(&mut self) -> FmNodePtr {
        Some(NonNull::from(&mut *self).cast::<FmNode>())
    }

    pub fn aggcontext(&self) -> Mcx<'_> {
        self.aggcontext.mcx()
    }

    pub fn set_current_agg(&self, aggref: NonNull<()>, shared: bool) {
        self.cur_agg.set(Some((aggref, shared)));
    }

    pub fn clear_current_agg(&self) {
        self.cur_agg.set(None);
    }

    pub fn current_agg(&self) -> Option<(NonNull<()>, bool)> {
        self.cur_agg.get()
    }

    /// C `RegisterExprContextCallback` on the curaggcontext (nodeAgg.c
    /// AggRegisterCallback): fired LIFO before the group memory goes away.
    ///
    /// # Safety
    /// `arg` must stay valid until the callback fires (group reset or node
    /// teardown), and `func(arg)` must be safe to call exactly once.
    pub unsafe fn register_shutdown_callback(&self, func: unsafe fn(*mut ()), arg: *mut ()) {
        // SAFETY: single-threaded backend; no other borrow escapes this module.
        unsafe { (*self.callbacks.get()).push((func, arg)) };
    }

    fn run_shutdown_callbacks(&self) {
        // SAFETY: single-threaded backend; callbacks may re-enter register
        // only after the swap below.
        let cbs = core::mem::take(unsafe { &mut *self.callbacks.get() });
        for (func, arg) in cbs.into_iter().rev() {
            // SAFETY: registration contract (register_shutdown_callback).
            unsafe { func(arg) };
        }
    }

    pub fn reset(&mut self) {
        self.run_shutdown_callbacks();
        self.aggcontext.reset();
    }
}

// Guard-owner Drop (docs/no-drop.md): nodeagg's make_agg_state_node registers
// the drop_in_place on the query context, mirroring C's ExecutorEnd shutdown
// of the aggcontext ExprContexts.
impl Drop for AggStateNode {
    fn drop(&mut self) {
        self.run_shutdown_callbacks();
    }
}

impl FunctionCallInfoBaseData {
    /// C `AggCheckCallContext`: `Some(aggcontext)` iff called as an aggregate
    /// trans/final fn (the WindowAgg arm is unarmed — loud at the caller).
    ///
    /// # Safety
    /// `context`, if set, points at a live FmNode-led node outliving `'a`,
    /// with no `&mut` formed to it during the call.
    #[inline]
    pub unsafe fn agg_context<'a>(&self) -> Option<Mcx<'a>> {
        // SAFETY: caller contract; the tag check proves the concrete type.
        unsafe { self.agg_state_node().map(|n| n.aggcontext.mcx()) }
    }

    /// The `AggStateNode` behind `fcinfo->context`, if any (C
    /// `IsA(fcinfo->context, AggState)` demux).
    ///
    /// # Safety
    /// As [`Self::agg_context`].
    #[inline]
    pub unsafe fn agg_state_node<'a>(&self) -> Option<&'a AggStateNode> {
        let p = self.context?;
        // SAFETY: caller contract; the tag check proves the concrete type.
        unsafe {
            if p.as_ref().tag != T_AGG_STATE {
                return None;
            }
            Some(p.cast::<AggStateNode>().as_ref())
        }
    }
}
