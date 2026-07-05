#![no_std]

use core::ptr::NonNull;

use types_fmgr::{FmNode, FmNodePtr, FunctionCallInfoBaseData};
use types_rel::Relation;
use types_trigger::Trigger;
use types_tuple::HeapTupleData;

pub const T_TRIGGER_DATA: u32 = 442;

#[repr(C)]
pub struct TriggerData<'a, 'mcx> {
    node: FmNode,
    pub tg_event: u32,
    pub tg_relation: &'a Relation<'mcx>,
    pub tg_trigtuple: Option<NonNull<HeapTupleData<'a>>>,
    pub tg_newtuple: Option<NonNull<HeapTupleData<'a>>>,
    pub tg_trigger: &'a Trigger<'mcx>,
    // TuplestoreHandle.0 for REFERENCING transition tables; 0 = C NULL.
    pub tg_oldtable: u64,
    pub tg_newtable: u64,
    // *const types_nodes::Bitmapset (attnums offset by
    // FirstLowInvalidHeapAttributeNumber); 0 = C NULL.
    pub tg_updatedcols: usize,
}

impl<'a, 'mcx> TriggerData<'a, 'mcx> {
    pub fn new<'t: 'a>(
        tg_event: u32,
        tg_relation: &'a Relation<'mcx>,
        tg_trigtuple: Option<&'a mut HeapTupleData<'t>>,
        tg_newtuple: Option<&'a mut HeapTupleData<'t>>,
        tg_trigger: &'a Trigger<'mcx>,
    ) -> Self {
        TriggerData {
            node: FmNode { tag: T_TRIGGER_DATA },
            tg_event,
            tg_relation,
            tg_trigtuple: tg_trigtuple.map(|r| NonNull::from(r).cast()),
            tg_newtuple: tg_newtuple.map(|r| NonNull::from(r).cast()),
            tg_trigger,
            tg_oldtable: 0,
            tg_newtable: 0,
            tg_updatedcols: 0,
        }
    }

    pub fn from_raw(
        tg_event: u32,
        tg_relation: &'a Relation<'mcx>,
        tg_trigtuple: Option<NonNull<HeapTupleData<'a>>>,
        tg_newtuple: Option<NonNull<HeapTupleData<'a>>>,
        tg_trigger: &'a Trigger<'mcx>,
    ) -> Self {
        TriggerData {
            node: FmNode { tag: T_TRIGGER_DATA },
            tg_event,
            tg_relation,
            tg_trigtuple,
            tg_newtuple,
            tg_trigger,
            tg_oldtable: 0,
            tg_newtable: 0,
            tg_updatedcols: 0,
        }
    }

    pub fn fm_node_ptr(&mut self) -> FmNodePtr {
        Some(NonNull::from(&mut *self).cast::<FmNode>())
    }

}

/// # Safety
/// A `T_TRIGGER_DATA`-tagged context points at a live `TriggerData` for `'a`.
pub unsafe fn trigger_data_from_fcinfo<'a, 'mcx, A: ?Sized>(
    fcinfo: &FunctionCallInfoBaseData<A>,
) -> Option<&'a TriggerData<'a, 'mcx>> {
    match fcinfo.context {
        Some(p) if unsafe { p.as_ref() }.tag == T_TRIGGER_DATA => {
            Some(unsafe { p.cast::<TriggerData<'a, 'mcx>>().as_ref() })
        }
        _ => None,
    }
}
