// TriggerData (trigger.h): the fcinfo->context payload of the trigger call
// convention. Tuples are raw pointers because the protocol is pointer
// identity: the function returns tg_trigtuple/tg_newtuple to proceed, another
// tuple to replace the row, or null to skip.
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
}

impl<'a, 'mcx> TriggerData<'a, 'mcx> {
    pub fn new(
        tg_event: u32,
        tg_relation: &'a Relation<'mcx>,
        tg_trigtuple: Option<&'a mut HeapTupleData<'a>>,
        tg_newtuple: Option<&'a mut HeapTupleData<'a>>,
        tg_trigger: &'a Trigger<'mcx>,
    ) -> Self {
        TriggerData {
            node: FmNode { tag: T_TRIGGER_DATA },
            tg_event,
            tg_relation,
            tg_trigtuple: tg_trigtuple.map(NonNull::from),
            tg_newtuple: tg_newtuple.map(NonNull::from),
            tg_trigger,
        }
    }

    pub fn fm_node_ptr(&mut self) -> FmNodePtr {
        Some(NonNull::from(&mut *self).cast::<FmNode>())
    }

}

/// C `CALLED_AS_TRIGGER` + downcast.
///
/// # Safety
/// A context tagged `T_TRIGGER_DATA` must point at a live `TriggerData`
/// outliving `'a` (the trigger call machinery's contract).
pub unsafe fn trigger_data_from_fcinfo<'a, 'mcx, A: ?Sized>(
    fcinfo: &FunctionCallInfoBaseData<A>,
) -> Option<&'a TriggerData<'a, 'mcx>> {
    match fcinfo.context {
        // SAFETY: caller contract.
        Some(p) if unsafe { p.as_ref() }.tag == T_TRIGGER_DATA => {
            // SAFETY: tag-checked; caller contract covers liveness and type.
            Some(unsafe { p.cast::<TriggerData<'a, 'mcx>>().as_ref() })
        }
        _ => None,
    }
}
