//! The fmgr builtin layer for the RI trigger procedures of `ri_triggers.c`
//! (`RI_FKey_check_ins`, …, `RI_FKey_setdefault_upd`).
//!
//! Each RI proc is a trigger function: C invokes it through `fmgr` with the
//! `TriggerData` riding on `fcinfo->context`, and the body returns
//! `PointerGetDatum(NULL)` (the trigger protocol disallows setting `isnull`).
//! Here the per-call `TriggerData` rides the trigger manager's thread-local
//! side-channel (installed by `exec_call_trigger_func`); the trigger-data
//! accessor seams read it, so the fmgr adapter only needs to forward to the
//! matching value core with the marker [`TriggerDataRef`].
//!
//! [`register_ri_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`), so by-OID dispatch (`function_call_invoke`)
//! resolves them. OIDs / nargs / strict / retset are transcribed from
//! `pg_proc.dat` (all RI procs are `pronargs 0`, not strict, not retset).

use mcx::Mcx;
use datum::Datum;
use types_error::PgResult;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};
use types_ri_triggers::TriggerDataRef;

use crate::triggers;
use crate::{
    F_RI_FKEY_CASCADE_DEL, F_RI_FKEY_CASCADE_UPD, F_RI_FKEY_CHECK_INS, F_RI_FKEY_CHECK_UPD,
    F_RI_FKEY_NOACTION_DEL, F_RI_FKEY_NOACTION_UPD, F_RI_FKEY_RESTRICT_DEL, F_RI_FKEY_RESTRICT_UPD,
    F_RI_FKEY_SETDEFAULT_DEL, F_RI_FKEY_SETDEFAULT_UPD, F_RI_FKEY_SETNULL_DEL, F_RI_FKEY_SETNULL_UPD,
};

/// The current-`TriggerData` marker handle (the firing path mints the same
/// value; the trigger-data accessors read the thread-local, not the handle).
const CURRENT_TRIGGER: TriggerDataRef = TriggerDataRef(1);

/// Adapt a `fn(Mcx, TriggerDataRef) -> PgResult<()>` RI core to the fmgr
/// native shape. The trigger protocol return value is
/// `PointerGetDatum(NULL)`, i.e. a null `Datum` with the `isnull` flag clear.
#[inline]
fn dispatch(
    core: impl FnOnce(Mcx<'_>, TriggerDataRef) -> types_error::PgResult<()>,
) -> PgResult<Datum> {
    let m = mcx::MemoryContext::new("RI trigger fmgr scratch");
    core(m.mcx(), CURRENT_TRIGGER)?;
    Ok(Datum::null())
}

fn fc_ri_fkey_check_ins(_fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dispatch(triggers::ri_fkey_check_ins)
}
fn fc_ri_fkey_check_upd(_fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dispatch(triggers::ri_fkey_check_upd)
}
fn fc_ri_fkey_noaction_del(_fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dispatch(triggers::ri_fkey_noaction_del)
}
fn fc_ri_fkey_noaction_upd(_fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dispatch(triggers::ri_fkey_noaction_upd)
}
fn fc_ri_fkey_restrict_del(_fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dispatch(triggers::ri_fkey_restrict_del)
}
fn fc_ri_fkey_restrict_upd(_fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dispatch(triggers::ri_fkey_restrict_upd)
}
fn fc_ri_fkey_cascade_del(_fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dispatch(triggers::ri_fkey_cascade_del)
}
fn fc_ri_fkey_cascade_upd(_fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dispatch(triggers::ri_fkey_cascade_upd)
}
fn fc_ri_fkey_setnull_del(_fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dispatch(triggers::ri_fkey_setnull_del)
}
fn fc_ri_fkey_setnull_upd(_fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dispatch(triggers::ri_fkey_setnull_upd)
}
fn fc_ri_fkey_setdefault_del(_fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dispatch(triggers::ri_fkey_setdefault_del)
}
fn fc_ri_fkey_setdefault_upd(_fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    dispatch(triggers::ri_fkey_setdefault_upd)
}

fn builtin(
    foid: u32,
    name: &str,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            // All RI trigger procs in pg_proc.dat: proargtypes '' (nargs 0), no
            // explicit proisstrict (defaults 't' => strict), no proretset (false).
            // These must match the `fmgr_builtins[]` canonical row so the
            // completeness guard does not flag a metadata mismatch.
            nargs: 0,
            strict: true,
            retset: false,
            func: None,
        },
        native,
    )
}

/// Register every `ri_triggers.c` trigger proc (C: their `fmgr_builtins[]`
/// rows). Called from this crate's [`crate::init_seams`]. Without this, the
/// FK-enforcing triggers installed by `ATAddForeignKeyConstraint` would
/// dispatch to a null `fn_addr` and the FK would not be enforced.
pub fn register_ri_builtins() {
    fmgr_core::register_builtins_native([
        builtin(F_RI_FKEY_CHECK_INS, "RI_FKey_check_ins", fc_ri_fkey_check_ins),
        builtin(F_RI_FKEY_CHECK_UPD, "RI_FKey_check_upd", fc_ri_fkey_check_upd),
        builtin(F_RI_FKEY_CASCADE_DEL, "RI_FKey_cascade_del", fc_ri_fkey_cascade_del),
        builtin(F_RI_FKEY_CASCADE_UPD, "RI_FKey_cascade_upd", fc_ri_fkey_cascade_upd),
        builtin(F_RI_FKEY_RESTRICT_DEL, "RI_FKey_restrict_del", fc_ri_fkey_restrict_del),
        builtin(F_RI_FKEY_RESTRICT_UPD, "RI_FKey_restrict_upd", fc_ri_fkey_restrict_upd),
        builtin(F_RI_FKEY_SETNULL_DEL, "RI_FKey_setnull_del", fc_ri_fkey_setnull_del),
        builtin(F_RI_FKEY_SETNULL_UPD, "RI_FKey_setnull_upd", fc_ri_fkey_setnull_upd),
        builtin(F_RI_FKEY_SETDEFAULT_DEL, "RI_FKey_setdefault_del", fc_ri_fkey_setdefault_del),
        builtin(F_RI_FKEY_SETDEFAULT_UPD, "RI_FKey_setdefault_upd", fc_ri_fkey_setdefault_upd),
        builtin(F_RI_FKEY_NOACTION_DEL, "RI_FKey_noaction_del", fc_ri_fkey_noaction_del),
        builtin(F_RI_FKEY_NOACTION_UPD, "RI_FKey_noaction_upd", fc_ri_fkey_noaction_upd),
    ]);
}
