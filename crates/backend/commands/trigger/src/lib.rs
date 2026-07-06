#![allow(non_snake_case)]

mod catalog;
mod ddl;
mod exec;
mod queue;
mod state;

pub use catalog::{map_partition_qual, CreateTriggerFiringOn, CreateTriggerInternal, InternalTriggerArgs, TriggerSetParentTrigger};
pub use ddl::{get_trigger_oid, renametrig, CreateTrigger, EnableDisableTrigger, RemoveTriggerById};
pub use exec::{
    trigger_depth, ExecBRInsertTriggers, ExecBSInsertTriggers, ExecBSTruncateTriggers,
    ExecCallTriggerFunc, ExecIRInsertTriggers, TriggerEnabled, TriggerFmgrCache,
    TriggerWhenCache, TriggerWhenEval,
};
pub use queue::{
    before_stmt_triggers_fired, ri_trigger_kind, AfterTriggerBeginQuery, AfterTriggerBeginSubXact,
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
