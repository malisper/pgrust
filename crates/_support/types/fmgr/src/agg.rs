use core::ptr::NonNull;

use ::mcx::{Mcx, MemoryContext};

use crate::fcinfo::{FmNode, FmNodePtr, FunctionCallInfoBaseData};

// nodetags.h value, parity-asserted in fmgr_core tests.
pub const T_AGG_STATE: u32 = 429;

/// The trans/final-fn-visible slice of C's `AggState`: rides `fcinfo->context`
/// (C `AggCheckCallContext`) and owns the aggcontext arena. Wholesale reset:
/// allocating transfns assert `!needs_drop` for their state (docs/no-drop.md).
#[repr(C)]
pub struct AggStateNode {
    node: FmNode,
    aggcontext: MemoryContext,
}

impl AggStateNode {
    pub fn new(aggcontext: MemoryContext) -> Self {
        Self { node: FmNode { tag: T_AGG_STATE }, aggcontext }
    }

    pub fn fm_node_ptr(&mut self) -> FmNodePtr {
        Some(NonNull::from(&mut *self).cast::<FmNode>())
    }

    pub fn aggcontext(&self) -> Mcx<'_> {
        self.aggcontext.mcx()
    }

    pub fn reset(&mut self) {
        self.aggcontext.reset();
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
        let p = self.context?;
        // SAFETY: caller contract; the tag check proves the concrete type.
        unsafe {
            if p.as_ref().tag != T_AGG_STATE {
                return None;
            }
            let node: &'a AggStateNode = p.cast::<AggStateNode>().as_ref();
            Some(node.aggcontext.mcx())
        }
    }
}
