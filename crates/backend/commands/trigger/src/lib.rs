#![allow(non_snake_case)]

mod catalog;
mod queue;

pub use catalog::{get_trigger_oid, CreateTriggerInternal, InternalTriggerArgs, RemoveTriggerById};
pub use queue::{
    AfterTriggerBeginQuery, AfterTriggerEndQuery, ExecARDeleteTriggers, ExecARInsertTriggers,
    ExecARUpdateTriggers,
};

use std::cell::Cell;

use types_core::CommandId;
use types_error::PgResult;

thread_local! {
    // afterTriggers reduced to the fields with live writers; the immediate
    // event queue lives in queue.rs, deferred events are a loud lane.
    pub(crate) static FIRING_COUNTER: Cell<CommandId> = const { Cell::new(0) };
    pub(crate) static QUERY_DEPTH: Cell<i32> = const { Cell::new(-1) };
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
    if EVENTS_NONEMPTY.with(|c| c.get()) || queue::query_stack_nonempty() {
        unported_events();
    }
    Ok(())
}

pub fn AfterTriggerEndXact(_is_commit: bool) -> PgResult<()> {
    EVENTS_NONEMPTY.with(|c| c.set(false));
    queue::query_stack_clear();
    QUERY_DEPTH.with(|c| c.set(-1));
    Ok(())
}

// trans_stack reduced to the live fields (state has no writer while SET
// CONSTRAINTS is unported; events reduced to the nonempty flag); C grows it
// in TopTransactionContext with doubling, this rendering doubles a leaked
// backend-lifetime slab (drop-free TLS; growth is bounded by max nest depth).
#[derive(Clone, Copy, Default)]
struct SavedTrans {
    query_depth: i32,
    firing_counter: CommandId,
    events_nonempty: bool,
}

thread_local! {
    static TRANS_STACK: Cell<(*mut SavedTrans, usize)> =
        const { Cell::new((std::ptr::null_mut(), 0)) };
    static MAX_TRANS_DEPTH: Cell<usize> = const { Cell::new(0) };
}

fn trans_stack_slot(level: usize) -> *mut SavedTrans {
    TRANS_STACK.with(|s| {
        let (mut ptr, mut cap) = s.get();
        if level >= cap {
            let mut new_cap = if cap == 0 { 8 } else { cap };
            while level >= new_cap {
                new_cap *= 2;
            }
            let grown: &'static mut [SavedTrans] =
                vec![SavedTrans::default(); new_cap].leak();
            if cap > 0 {
                // SAFETY: old slab is live (leaked) with cap valid entries.
                unsafe {
                    std::ptr::copy_nonoverlapping(ptr, grown.as_mut_ptr(), cap);
                }
            }
            ptr = grown.as_mut_ptr();
            cap = new_cap;
            s.set((ptr, cap));
        }
        // SAFETY: level < cap after growth.
        unsafe { ptr.add(level) }
    })
}

pub fn AfterTriggerBeginSubXact() -> PgResult<()> {
    let my_level = xact::GetCurrentTransactionNestLevel() as usize;
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
    // SAFETY: single-threaded backend TLS; slot is in-bounds via trans_stack_slot.
    unsafe { *trans_stack_slot(my_level) = saved };
    Ok(())
}

pub fn AfterTriggerEndSubXact(is_commit: bool) -> PgResult<()> {
    let my_level = xact::GetCurrentTransactionNestLevel() as usize;
    if is_commit {
        assert!(my_level < MAX_TRANS_DEPTH.with(|c| c.get()));
        // SAFETY: as AfterTriggerBeginSubXact.
        let saved = unsafe { *trans_stack_slot(my_level) };
        debug_assert_eq!(QUERY_DEPTH.with(|c| c.get()), saved.query_depth);
    } else {
        if my_level >= MAX_TRANS_DEPTH.with(|c| c.get()) {
            return Ok(());
        }
        // SAFETY: as AfterTriggerBeginSubXact.
        let saved = unsafe { *trans_stack_slot(my_level) };
        if EVENTS_NONEMPTY.with(|c| c.get())
            || saved.events_nonempty
            || queue::query_stack_nonempty()
        {
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
    fn begin_end_query_depth_bookkeeping() {
        init_once();
        trigger_seams::after_trigger_begin_xact::call().unwrap();
        assert_eq!(QUERY_DEPTH.with(|c| c.get()), -1);
        AfterTriggerBeginQuery();
        assert_eq!(QUERY_DEPTH.with(|c| c.get()), 0);
        AfterTriggerBeginQuery();
        assert_eq!(QUERY_DEPTH.with(|c| c.get()), 1);
        AfterTriggerEndQuery().unwrap();
        AfterTriggerEndQuery().unwrap();
        assert_eq!(QUERY_DEPTH.with(|c| c.get()), -1);
        trigger_seams::after_trigger_end_xact::call(true).unwrap();
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
