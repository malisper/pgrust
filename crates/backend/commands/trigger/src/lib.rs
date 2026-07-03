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

// trans_stack reduced to the live fields (state has no writer while SET
// CONSTRAINTS is unported; events reduced to the nonempty flag); C grows it
// in TopTransactionContext, this rendering is a fixed drop-free slab.
const TRANS_STACK_CAP: usize = 64;

#[derive(Clone, Copy, Default)]
struct SavedTrans {
    query_depth: i32,
    firing_counter: CommandId,
    events_nonempty: bool,
}

thread_local! {
    static TRANS_STACK: std::cell::UnsafeCell<[SavedTrans; TRANS_STACK_CAP]> =
        const { std::cell::UnsafeCell::new([SavedTrans { query_depth: 0, firing_counter: 0, events_nonempty: false }; TRANS_STACK_CAP]) };
    static MAX_TRANS_DEPTH: Cell<usize> = const { Cell::new(0) };
}

pub fn AfterTriggerBeginSubXact() -> PgResult<()> {
    let my_level = xact::GetCurrentTransactionNestLevel() as usize;
    if my_level >= TRANS_STACK_CAP {
        panic!(
            "trigger.c trans_stack beyond {TRANS_STACK_CAP} nested subtransactions \
             (fixed-cap rendering)"
        );
    }
    MAX_TRANS_DEPTH.with(|c| {
        let mut depth = c.get();
        while my_level >= depth {
            depth = if depth == 0 { 8 } else { depth * 2 };
        }
        c.set(depth);
    });
    let saved = SavedTrans {
        query_depth: QUERY_DEPTH.with(|c| c.get()),
        firing_counter: FIRING_COUNTER.with(|c| c.get()),
        events_nonempty: EVENTS_NONEMPTY.with(|c| c.get()),
    };
    // SAFETY: single-threaded backend TLS; the &mut is confined to this call.
    TRANS_STACK.with(|s| unsafe { (*s.get())[my_level] = saved });
    Ok(())
}

pub fn AfterTriggerEndSubXact(is_commit: bool) -> PgResult<()> {
    let my_level = xact::GetCurrentTransactionNestLevel() as usize;
    if is_commit {
        assert!(my_level < MAX_TRANS_DEPTH.with(|c| c.get()));
        // SAFETY: as AfterTriggerBeginSubXact.
        let saved = TRANS_STACK.with(|s| unsafe { (*s.get())[my_level] });
        debug_assert_eq!(QUERY_DEPTH.with(|c| c.get()), saved.query_depth);
    } else {
        if my_level >= MAX_TRANS_DEPTH.with(|c| c.get()) {
            return Ok(());
        }
        // SAFETY: as AfterTriggerBeginSubXact.
        let saved = TRANS_STACK.with(|s| unsafe { (*s.get())[my_level] });
        if EVENTS_NONEMPTY.with(|c| c.get()) || saved.events_nonempty {
            unported_events();
        }
        QUERY_DEPTH.with(|c| c.set(saved.query_depth));
    }
    Ok(())
}

pub fn init_seams() {
    trigger_seams::after_trigger_begin_xact::set(AfterTriggerBeginXact);
    trigger_seams::after_trigger_fire_deferred::set(AfterTriggerFireDeferred);
    trigger_seams::after_trigger_end_xact::set(AfterTriggerEndXact);
    trigger_seams::after_trigger_begin_sub_xact::set(AfterTriggerBeginSubXact);
    trigger_seams::after_trigger_end_sub_xact::set(AfterTriggerEndSubXact);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_once() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(init_seams);
    }

    #[test]
    fn xact_lifecycle_arms() {
        init_once();
        trigger_seams::after_trigger_begin_xact::call().unwrap();
        assert_eq!(FIRING_COUNTER.with(|c| c.get()), 1);
        assert_eq!(QUERY_DEPTH.with(|c| c.get()), -1);
        trigger_seams::after_trigger_fire_deferred::call().unwrap();
        trigger_seams::after_trigger_end_xact::call(true).unwrap();
        trigger_seams::after_trigger_begin_xact::call().unwrap();
        trigger_seams::after_trigger_end_xact::call(false).unwrap();
    }

    #[test]
    fn subxact_lifecycle_arms() {
        init_once();
        trigger_seams::after_trigger_begin_xact::call().unwrap();
        QUERY_DEPTH.with(|c| c.set(3));
        trigger_seams::after_trigger_begin_sub_xact::call().unwrap();
        assert_eq!(MAX_TRANS_DEPTH.with(|c| c.get()), 8);
        trigger_seams::after_trigger_end_sub_xact::call(true).unwrap();
        trigger_seams::after_trigger_begin_sub_xact::call().unwrap();
        QUERY_DEPTH.with(|c| c.set(7));
        trigger_seams::after_trigger_end_sub_xact::call(false).unwrap();
        assert_eq!(QUERY_DEPTH.with(|c| c.get()), 3);
        QUERY_DEPTH.with(|c| c.set(-1));
        trigger_seams::after_trigger_end_xact::call(true).unwrap();
    }
}
