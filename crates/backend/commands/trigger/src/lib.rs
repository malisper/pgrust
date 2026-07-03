#![allow(non_snake_case)]

use std::cell::Cell;

use types_core::CommandId;
use types_error::PgResult;

thread_local! {
    // afterTriggers reduced to the fields with live writers; events stay
    // empty while queueing (AfterTriggerSaveEvent) is unported.
    static FIRING_COUNTER: Cell<CommandId> = const { Cell::new(0) };
    static QUERY_DEPTH: Cell<i32> = const { Cell::new(-1) };
    static EVENTS_NONEMPTY: Cell<bool> = const { Cell::new(false) };
}

#[cold]
fn unported_events() -> ! {
    panic!("after-trigger events queued but commands/trigger.c firing is not ported");
}

pub fn AfterTriggerBeginXact() -> PgResult<()> {
    FIRING_COUNTER.with(|c| c.set(1));
    QUERY_DEPTH.with(|c| c.set(-1));
    debug_assert!(!EVENTS_NONEMPTY.with(|c| c.get()));
    Ok(())
}

pub fn AfterTriggerFireDeferred() -> PgResult<()> {
    debug_assert_eq!(QUERY_DEPTH.with(|c| c.get()), -1);
    if EVENTS_NONEMPTY.with(|c| c.get()) {
        unported_events();
    }
    Ok(())
}

pub fn AfterTriggerEndXact(_is_commit: bool) -> PgResult<()> {
    EVENTS_NONEMPTY.with(|c| c.set(false));
    QUERY_DEPTH.with(|c| c.set(-1));
    Ok(())
}

pub fn init_seams() {
    trigger_seams::after_trigger_begin_xact::set(AfterTriggerBeginXact);
    trigger_seams::after_trigger_fire_deferred::set(AfterTriggerFireDeferred);
    trigger_seams::after_trigger_end_xact::set(AfterTriggerEndXact);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xact_lifecycle_arms() {
        init_seams();
        trigger_seams::after_trigger_begin_xact::call().unwrap();
        assert_eq!(FIRING_COUNTER.with(|c| c.get()), 1);
        assert_eq!(QUERY_DEPTH.with(|c| c.get()), -1);
        trigger_seams::after_trigger_fire_deferred::call().unwrap();
        trigger_seams::after_trigger_end_xact::call(true).unwrap();
        trigger_seams::after_trigger_begin_xact::call().unwrap();
        trigger_seams::after_trigger_end_xact::call(false).unwrap();
        assert!(!trigger_seams::after_trigger_begin_sub_xact::is_installed());
        assert!(!trigger_seams::after_trigger_end_sub_xact::is_installed());
    }
}
