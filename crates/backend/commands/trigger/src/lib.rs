#![allow(non_snake_case)]

mod catalog;
mod ddl;
mod exec;
mod queue;
mod state;

pub use catalog::{map_partition_qual, CreateTriggerFiringOn, CreateTriggerInternal, InternalTriggerArgs, TriggerSetParentTrigger};
pub use ddl::{get_trigger_oid, renametrig, CreateTrigger, EnableDisableTrigger, RemoveTriggerById};
pub use exec::{
    trigger_depth, ExecBRDeleteTriggers, ExecBRInsertTriggers, ExecBRUpdateTriggers,
    ExecBSInsertTriggers, ExecBSTruncateTriggers, ExecCallTriggerFunc, ExecIRInsertTriggers,
    TriggerEnabled, TriggerFmgrCache, TriggerWhenCache, TriggerWhenEval,
};
pub use queue::{
    before_stmt_triggers_fired, check_foreign_transition_capture, ri_trigger_kind, AfterTriggerBeginQuery, AfterTriggerBeginSubXact,
    AfterTriggerBeginXact, AfterTriggerEndQuery, AfterTriggerEndSubXact, AfterTriggerEndXact,
    AfterTriggerFireDeferred, AfterTriggerPendingOnRel, ExecARDeleteTriggers,
    ExecARInsertTriggers, ExecARUpdateTriggers, ExecASDeleteTriggers, ExecASInsertTriggers,
    ChildToRoot, ExecASTruncateTriggers, ExecASUpdateTriggers, MakeTransitionCaptureState,
    TransitionCaptureState,
};
pub use state::AfterTriggerSetState;

pub fn init_seams() {
    trigger_seams::after_trigger_begin_xact::set(AfterTriggerBeginXact);
    trigger_seams::after_trigger_fire_deferred::set(AfterTriggerFireDeferred);
    trigger_seams::after_trigger_end_xact::set(AfterTriggerEndXact);
    trigger_seams::after_trigger_begin_sub_xact::set(AfterTriggerBeginSubXact);
    trigger_seams::after_trigger_end_sub_xact::set(AfterTriggerEndSubXact);
    trigger_seams::after_trigger_pending_on_rel::set(AfterTriggerPendingOnRel);
    trigger_seams::my_trigger_depth::set(trigger_depth);
}

// Every one of these is `elog(ERROR, "cache lookup failed for <object> %u", oid)`
// in C: a catchable error whose SQLSTATE is elog's default XX000 /
// ERRCODE_INTERNAL_ERROR, never a backend abort.  pgrust used to panic!() at
// these probes, which kills the process instead.
#[track_caller]
#[cold]
#[inline(never)]
pub(crate) fn cache_lookup_failed(
    what: &str,
    oid: types_core::Oid,
) -> Box<types_error::PgError> {
    Box::new(types_error::PgError::error(format!(
        "cache lookup failed for {what} {oid}"
    )))
}

#[cfg(test)]
mod cache_lookup_error_tests {
    use super::cache_lookup_failed;

    // C: elog(ERROR, "cache lookup failed for relation %u") -- a catchable
    // XX000, not a backend abort.  These probes used to panic!(), killing the
    // process.
    #[test]
    fn cache_lookup_failure_is_a_catchable_xx000() {
        let e = cache_lookup_failed("relation", 16384);
        assert_eq!(e.message(), "cache lookup failed for relation 16384");
        assert_eq!(e.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
        assert_eq!(e.level(), types_error::ERROR);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_once() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(init_seams);
    }

    #[test]
    fn begin_end_query_depth_bookkeeping() {
        init_once();
        trigger_seams::after_trigger_begin_xact::call().unwrap();
        assert_eq!(queue::query_depth(), -1);
        AfterTriggerBeginQuery();
        assert_eq!(queue::query_depth(), 0);
        AfterTriggerBeginQuery();
        assert_eq!(queue::query_depth(), 1);
        AfterTriggerEndQuery().unwrap();
        AfterTriggerEndQuery().unwrap();
        assert_eq!(queue::query_depth(), -1);
        trigger_seams::after_trigger_end_xact::call(true).unwrap();
    }

    #[test]
    fn xact_lifecycle_arms() {
        init_once();
        trigger_seams::after_trigger_begin_xact::call().unwrap();
        assert_eq!(queue::firing_counter(), 1);
        assert_eq!(queue::query_depth(), -1);
        trigger_seams::after_trigger_fire_deferred::call().unwrap();
        trigger_seams::after_trigger_end_xact::call(true).unwrap();
        trigger_seams::after_trigger_begin_xact::call().unwrap();
        trigger_seams::after_trigger_end_xact::call(false).unwrap();
    }
}
