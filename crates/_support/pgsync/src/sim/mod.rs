//! `pgsync::sim` — the permit scheduler's home (sim world only).
//!
//! Ownership (permit-s1 contract §5 fences): WS-SYNC created this module
//! once, in its FIRST pushed commit, to pin the [`hooks`] seam; everything
//! under `src/sim/**` belongs to WS-CORE from that commit on (scheduler
//! core, slot registry, seeded picker, virtual-time advance, watchdog,
//! SCHEDOP log). WS-SYNC does not touch this directory again.
//!
//! WS-CORE (contract §3; design docs/design/permit-scheduler.md §1/§3/§4):
//! ONE global permit; exactly one runnable thread at any moment; a thread
//! runs from permit receipt to its next pgsync touch. Every OS-arbitrary
//! choice (which notify_one wakee, which blocked locker acquires, which
//! receiver wins) is a seeded pick over a stream derived from
//! `PGRUST_SIM_SEED` through the sanctioned SimEntropy funnel. Single-run
//! semantics: one seed = one schedule = full replay by re-running the seed;
//! the SCHEDOP log exists for diagnosis, not search.
//!
//! Layout:
//!  - [`hooks`]     — the pinned seam (WS-SYNC commit 1): the
//!                    [`hooks::SchedulerHooks`] trait the sim wrappers call
//!  - [`sched`]     — scheduler core: permit, vpid slot registry, seeded
//!                    uniform picker, mandatory handoff, optional seeded
//!                    preemption, TimedPark on virtual time, deterministic-
//!                    deadlock report, TLS-teardown rules (P3 §1.6 as
//!                    adapted by design §3); [`sched::Router`] implements
//!                    the hooks trait by TLS registration
//!  - [`schedlog`]  — the SCHEDOP op-seq schedule log (contract §3.3)
//!  - [`watchdog`]  — THE one detector:
//!                    permit-holder-blocked-outside-interception (§3.2)
//!  - [`spawn_door`]— launch_backend registration surface (§3.1) + the
//!                    main/client-driver registration door
//!  - [`config`]    — sim-knob parsing (R-KNOBS idiom: pure parse fns +
//!                    unit corpus)

pub mod config;
pub mod hooks;
pub mod sched;
pub mod schedlog;
pub mod spawn_door;
pub mod watchdog;

#[cfg(test)]
mod tests;

pub use sched::{ClockMode, FailAction, Scheduler, SchedulerConfig, SlotId, WatchdogSink};

/// The vpid of the calling thread's registered slot, or `None` when the
/// calling thread is outside the model (unregistered / scheduler disabled).
/// Sim waiter-slot identity is keyed by THIS (TLS-teardown rule 2, design
/// §3), not by SLOT_FREE pop order.
pub fn current_vpid() -> Option<hooks::Vpid> {
    sched::current_vpid()
}

/// STRICT permit probe (the shared-universe SimVfs access assert): true only
/// when the GLOBAL permit scheduler exists AND the calling thread's slot
/// currently holds the permit. False when the scheduler is off (sharing is
/// only legal under it), when the thread never entered a door, and when a
/// registered thread is somehow executing outside its quantum. Injected into
/// vfs as a plain fn pointer (`SimVfs::set_shared_access_probe`) — vfs is a
/// frozen leaf crate and cannot depend on pgsync.
pub fn current_thread_holds_permit() -> bool {
    sched::current_thread_holds_permit()
}
