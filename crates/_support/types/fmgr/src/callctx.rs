use core::ptr::NonNull;

use crate::fcinfo::{FmNode, FmNodePtr, FunctionCallInfoBaseData};

// nodetags.h value, parity-asserted in fmgr_core tests (types_nodes sits above).
pub const T_CALL_CONTEXT: u32 = 214;

/// C parsenodes.h `CallContext`: rides `fcinfo->context` on a CALL so the
/// procedure language handler can tell whether transaction control is allowed.
#[repr(C)]
pub struct CallContext {
    node: FmNode,
    pub atomic: bool,
}

impl CallContext {
    pub fn new(atomic: bool) -> Self {
        Self { node: FmNode { tag: T_CALL_CONTEXT }, atomic }
    }

    pub fn fm_node_ptr(&mut self) -> FmNodePtr {
        Some(NonNull::from(&mut *self).cast::<FmNode>())
    }
}

impl FunctionCallInfoBaseData {
    /// # Safety
    /// `context`, if set, points at a live FmNode-led node armed for this call
    /// ([`CallContext::fm_node_ptr`]), with no `&mut` formed to it during the call.
    #[inline]
    pub unsafe fn call_context<'a>(&self) -> Option<&'a CallContext> {
        let p = self.context?;
        // SAFETY: caller contract; the tag check proves the concrete type.
        unsafe {
            if p.as_ref().tag != T_CALL_CONTEXT {
                return None;
            }
            Some(p.cast::<CallContext>().as_ref())
        }
    }
}
