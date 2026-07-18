//! The spawn-door registration surface (contract §3.1): slot registry
//! entries are keyed by vpid from `reserve_child_pid` (stable identity, P3
//! inheritance) and registered at the launch_backend spawn door;
//! main/client-driver threads register through the same door
//! ([`register_self`], synthetic vpids in the `0xC000_0000..` range —
//! disjoint from both the pid range and `pgsync::thread`'s wrapper range).
//!
//! Everything here routes to the ONE global scheduler ([`sched::global`])
//! and is a no-op unless `PGRUST_SIM_SCHED=1` — scheduler-off sim runs
//! (dst-determinism-smoke, sim-net-e2e) are byte-unaffected.
//!
//! Registration is PARENT-side (before the OS spawn) so the slot exists —
//! and its SCHEDOP position is program-ordered — before any child action;
//! the child only binds and gates ([`enter_child`]). This door therefore
//! needs no spawn fence (unlike the child-registering `pgsync::thread`
//! wrapper path; see sched.rs module docs).
//!
//! Guard discipline at the door (rule 1, quantum-scoped teardown): declare
//! the [`SlotGuard`] FIRST in the child closure, before every TLS-owning
//! guard (local latch release, wait-event-set release, …). Rust drops in
//! reverse declaration order, so all shared-state TLS guards drop INSIDE
//! the final quantum, and the SlotGuard's drop — teardown hooks, deregister
//! (join-wake, rule 3), permit handoff — runs last.

use super::hooks::{SchedulerHooks, Vpid};
use super::sched::{self, SlotId, ROUTER};

/// Slot lifetime guard: drop = the spawn-door epilogue (teardown hooks in
/// the final quantum → deregister → Join-parked slots wake → handoff).
pub struct SlotGuard {
    _priv: (),
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        if let Some(v) = sched::current_vpid() {
            ROUTER.exit(v);
        }
    }
}

/// Parent side, BEFORE the OS spawn: create the child's slot so it exists
/// before any child action. No-op (None) when the scheduler is disabled.
#[track_caller]
pub fn register_child(vpid: Vpid, name: &str) -> Option<SlotId> {
    sched::global().map(|s| s.register(vpid, name))
}

/// Child side, first statement of the spawned closure: bind the thread to
/// its slot and park until the first grant. Returns the epilogue guard —
/// declare it before all TLS-owning guards (see module docs). No-op (None)
/// when the scheduler is disabled or the slot was never registered.
#[track_caller]
pub fn enter_child(slot: Option<SlotId>) -> Option<SlotGuard> {
    let sched = sched::global()?;
    let slot = slot?;
    sched.enter(slot);
    Some(SlotGuard { _priv: () })
}

/// The same door for threads with no reserved pid (the postmaster main
/// thread, client drivers, harness threads): synthetic vpid, immediate
/// registration + entry. No-op (None) when disabled.
#[track_caller]
pub fn register_self(name: &str) -> Option<SlotGuard> {
    let sched = sched::global()?;
    sched.register_self(name);
    Some(SlotGuard { _priv: () })
}

/// Parent side, when the OS spawn FAILED after [`register_child`] (the F3
/// ledger shape): retire the never-entered slot so it cannot leak a
/// Runnable ghost that wedges the schedule when granted. No-op when
/// disabled or never registered.
#[track_caller]
pub fn cancel_child(slot: Option<SlotId>) {
    if let (Some(sched), Some(slot)) = (sched::global(), slot) {
        sched.cancel_never_entered(slot);
    }
}

/// Rename the CALLING thread's slot to `new_vpid` — the wpool retention
/// shape (F2): a retained standby serves every task under a fresh synthetic
/// pid, and sim waiter-slot identity keys off `current_vpid`, so the model
/// identity follows MyProcPid across a claim. Call it exactly where the
/// product re-keys its own thread bookkeeping (`rekey_child_thread`).
/// No-op when the scheduler is off or the thread is unregistered.
#[track_caller]
pub fn rekey_self(new_vpid: Vpid) {
    sched::rekey_current(new_vpid);
}
