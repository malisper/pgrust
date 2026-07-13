//! The morsel runtime crate (M0 foundation).
//!
//! This crate is the home of the parallelism-redesign runtime
//! (docs/design/parallelism-redesign-2026-07.md §2.1): worker pool,
//! ResourceGroup/TaskSet/Task structures, stride scheduling, and the task
//! lifecycle. The M0 harvest lands only the lifecycle foundation here; the
//! pool and scheduler build against these types in their own lane.
//!
//! Pinned M0 interface (agreed across M0 lanes):
//! - [`Generation`] — u64 newtype identifying one query-owned execution
//!   generation.
//! - [`TaskLifecycle`] — the combined-CAS lifecycle state machine
//!   (Idle/Armed/Running/Draining/Closed) with join/close.
//! - [`QueryTaskGuard`] — the query-task binder's RAII bind/unbind of
//!   xact + snapshot + temp-namespace for a foreign thread (re-exported
//!   from the `parallel` crate, where the binder lives with its fault
//!   matrix; drive it only through [`with_query_task_binding`]).

pub mod lifecycle;

pub use lifecycle::{
    ForeignParticipationDisabled, Generation, LifecycleState, ParticipantOwner, QueryTaskLifecycle,
    TaskHandle, TaskLifecycle, TaskParticipant,
};

pub use parallel::{
    with_query_task_binding, InstallQueryTaskBinding, QueryTaskBindingGuard as QueryTaskGuard,
    QueryTaskBindingPolicy,
};
