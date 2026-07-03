// Planner support-request nodes (supportnodes.h). Stack-built by the planner,
// passed to prosupport functions as a pointer Datum; tag-first repr(C) so the
// callee can demux on the leading NodeTag alone. C's `root` is omitted:
// PlannerInfo never crosses the fmgr boundary here (Param estimation, its only
// consumer, is unported).
use crate::node_tree::Node;
use crate::tags::NodeTag;
use types_core::Oid;

#[repr(C)]
pub struct SupportRequestRows<'mcx> {
    tag: NodeTag,
    pub funcid: Oid,
    pub node: Option<Node<'mcx>>,
    pub rows: f64,
}

#[repr(C)]
pub struct SupportRequestSimplify<'mcx> {
    tag: NodeTag,
    pub fcall: Option<Node<'mcx>>,
}

impl<'mcx> SupportRequestSimplify<'mcx> {
    pub fn new(fcall: Option<Node<'mcx>>) -> Self {
        SupportRequestSimplify { tag: NodeTag::T_SupportRequestSimplify, fcall }
    }
}

#[repr(C)]
pub struct SupportRequestCost<'mcx> {
    tag: NodeTag,
    pub funcid: Oid,
    pub node: Option<Node<'mcx>>,
    pub startup: f64,
    pub per_tuple: f64,
}

const _: () = {
    assert!(core::mem::offset_of!(SupportRequestRows, tag) == 0);
    assert!(core::mem::offset_of!(SupportRequestCost, tag) == 0);
    assert!(core::mem::offset_of!(SupportRequestSimplify, tag) == 0);
};

impl<'mcx> SupportRequestRows<'mcx> {
    pub fn new(funcid: Oid, node: Option<Node<'mcx>>) -> Self {
        SupportRequestRows { tag: NodeTag::T_SupportRequestRows, funcid, node, rows: 0.0 }
    }
}

impl<'mcx> SupportRequestCost<'mcx> {
    pub fn new(funcid: Oid, node: Option<Node<'mcx>>) -> Self {
        SupportRequestCost {
            tag: NodeTag::T_SupportRequestCost,
            funcid,
            node,
            startup: 0.0,
            per_tuple: 0.0,
        }
    }
}

/// Demux a prosupport request pointer by its leading tag.
///
/// # Safety
/// `p` must point at a live support-request node built by the `new`
/// constructors above (tag-first repr(C)), exclusively borrowed for `'a`.
pub unsafe fn support_request_rows_mut<'a, 'mcx>(
    p: *mut (),
) -> Option<&'a mut SupportRequestRows<'mcx>> {
    // SAFETY: caller contract — tag-first node, live and exclusive.
    unsafe {
        if *p.cast::<NodeTag>() != NodeTag::T_SupportRequestRows {
            return None;
        }
        Some(&mut *p.cast::<SupportRequestRows<'mcx>>())
    }
}
