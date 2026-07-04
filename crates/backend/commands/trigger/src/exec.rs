// ExecCallTriggerFunc + TriggerEnabled's role gate (trigger.c). WHEN-clause
// evaluation lives with the executor's BR loops (nodemodifytable); the AFTER
// save path louds on WHEN/UPDATE OF in queue.rs.
use core::ptr::NonNull;

use mcx::Mcx;
use types_error::{PgError, PgResult, ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED};
use types_fmgr::{FmgrInfo, LocalFcinfo};
use types_trigger::{Trigger, TRIGGER_DISABLED, TRIGGER_FIRES_ON_REPLICA};
use types_trigger_call::TriggerData;
use types_tuple::HeapTupleData;

// Resolve-once carrier for a TriggerDesc's functions (C ri_TrigFunctions).
#[derive(Default)]
pub struct TriggerFmgrCache {
    finfo: Vec<Option<FmgrInfo>>,
}

impl TriggerFmgrCache {
    pub fn get(&mut self, tgindx: usize, tgfoid: types_core::Oid) -> PgResult<&mut FmgrInfo> {
        if self.finfo.len() <= tgindx {
            self.finfo.resize_with(tgindx + 1, || None);
        }
        let slot = &mut self.finfo[tgindx];
        if slot.is_none() {
            *slot = Some(fmgr_seams::fmgr_info::call(tgfoid)?);
        }
        Ok(slot.as_mut().expect("just filled"))
    }
}

// TriggerEnabled's tgenabled gate (SESSION_REPLICATION_ROLE_ORIGIN, the only
// ported role); tgattr/tgqual are the caller's to handle.
pub fn TriggerEnabled(t: &Trigger<'_>) -> bool {
    t.tgenabled != TRIGGER_DISABLED && t.tgenabled != TRIGGER_FIRES_ON_REPLICA
}

pub fn ExecCallTriggerFunc<'a, 'mcx>(
    per_tuple_mcx: Mcx<'_>,
    trigdata: &mut TriggerData<'a, 'mcx>,
    finfo: &mut FmgrInfo,
) -> PgResult<Option<NonNull<HeapTupleData<'a>>>> {
    debug_assert_eq!(finfo.fn_oid, trigdata.tg_trigger.tgfoid);
    let mut fcinfo = LocalFcinfo::<0>::fresh(types_core::InvalidOid);
    fcinfo.context = trigdata.fm_node_ptr();
    // SAFETY: the scratch context outlives this single invocation.
    unsafe { fcinfo.set_result_mcx(per_tuple_mcx) };
    let result = finfo.invoke(&mut fcinfo)?;
    if fcinfo.isnull {
        return Err(returned_null(finfo.fn_oid));
    }
    Ok(NonNull::new(result.as_usize() as *mut HeapTupleData<'a>))
}

#[cold]
#[inline(never)]
fn returned_null(fn_oid: types_core::Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("trigger function {fn_oid} returned null value"))
            .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
    )
}
