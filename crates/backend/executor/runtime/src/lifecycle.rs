//! Generation-keyed task lifecycle — PINNED INTERFACE shim.
//!
//! COORDINATION (M0, 2026-07): lane m0-harvest is independently landing
//! `lifecycle.rs` (harvested from the qualified scan-task lifecycle donor,
//! `morsel/real-scan-v93` @ f4f905dd8 combined-CAS design) into this same
//! crate. Both lanes build against the PINNED INTERFACE:
//!
//! ```text
//! Generation(u64)
//! TaskLifecycle    — single-word CAS state machine
//! QueryTaskGuard   — RAII participation guard, generation-checked entry
//! ```
//!
//! This file is the m0-runtime shim of that interface, cfg-gated so the
//! harvest lane's version replaces it wholesale at merge: building with
//! `--cfg m0_harvest_lifecycle` compiles this shim OUT. Reconcile by dropping
//! the harvested file in under the same module path and deleting the shim.
//!
//! Semantics (H1 structural fix, redesign doc §1/§2.3): every task carries
//! `(query_id, Generation)`; ALL shared-state consumption is guarded by a
//! generation check fused (single CAS word) with the participant count, so a
//! task from an aborted generation is UNCONSUMABLE BY CONSTRUCTION — there is
//! no window where an aborted generation admits a new participant, and a
//! generation cannot retire while any participant is still inside.
//!
//! Word layout (single AtomicU64, every transition one CAS):
//!   [ generation : 40 | active : 22 | ABORTED : 1 | OPEN : 1 ]

#![cfg(not(m0_harvest_lifecycle))]

use std::sync::Arc;

use crate::sync::atomic::{AtomicU64, Ordering};

const OPEN: u64 = 1;
const ABORTED: u64 = 1 << 1;
const ACTIVE_SHIFT: u32 = 2;
const ACTIVE_ONE: u64 = 1 << ACTIVE_SHIFT;
const ACTIVE_BITS: u32 = 22;
const ACTIVE_MASK: u64 = ((1 << ACTIVE_BITS) - 1) << ACTIVE_SHIFT;
const GEN_SHIFT: u32 = ACTIVE_SHIFT + ACTIVE_BITS;

/// Query-owned generation number. Aborted generations never come back;
/// state keyed by a dead generation is garbage by definition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Generation(pub u64);

fn gen_of(w: u64) -> u64 {
    w >> GEN_SHIFT
}

fn active_of(w: u64) -> u64 {
    (w & ACTIVE_MASK) >> ACTIVE_SHIFT
}

/// Combined-CAS lifecycle state machine: generation, participant count, and
/// abort/open flags live in ONE word so "check generation AND join" /
/// "verify drained AND retire" are single atomic transitions.
pub struct TaskLifecycle {
    word: AtomicU64,
}

impl TaskLifecycle {
    /// New lifecycle, generation 0 open.
    pub fn new() -> Self {
        TaskLifecycle { word: AtomicU64::new(OPEN) }
    }

    pub fn current_generation(&self) -> Generation {
        Generation(gen_of(self.word.load(Ordering::SeqCst)))
    }

    pub fn active(&self) -> usize {
        active_of(self.word.load(Ordering::SeqCst)) as usize
    }

    pub fn is_aborted(&self, gen: Generation) -> bool {
        let w = self.word.load(Ordering::SeqCst);
        // A superseded generation is as dead as an aborted one.
        gen_of(w) != gen.0 || (w & ABORTED) != 0
    }

    /// Abort generation `gen`: no NEW participants may enter it; existing
    /// guards drain normally. Returns false if `gen` is not current (already
    /// retired — nothing to abort).
    pub fn abort(&self, gen: Generation) -> bool {
        let mut w = self.word.load(Ordering::SeqCst);
        loop {
            if gen_of(w) != gen.0 {
                return false;
            }
            if w & ABORTED != 0 {
                return true; // idempotent
            }
            match self.word.compare_exchange(w, w | ABORTED, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return true,
                Err(cur) => w = cur,
            }
        }
    }

    /// Retire generation `gen` and open the next: requires the generation to
    /// be fully drained (active == 0). Returns the new generation, or None if
    /// participants are still inside or `gen` is not current.
    pub fn retire(&self, gen: Generation) -> Option<Generation> {
        let mut w = self.word.load(Ordering::SeqCst);
        loop {
            if gen_of(w) != gen.0 || active_of(w) != 0 {
                return None;
            }
            let next = (gen.0 + 1) << GEN_SHIFT | OPEN;
            match self.word.compare_exchange(w, next, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return Some(Generation(gen.0 + 1)),
                Err(cur) => w = cur,
            }
        }
    }

    fn try_join(&self, gen: Generation) -> bool {
        let mut w = self.word.load(Ordering::SeqCst);
        loop {
            if gen_of(w) != gen.0 || (w & ABORTED) != 0 || (w & OPEN) == 0 {
                return false;
            }
            debug_assert!(active_of(w) < (1 << ACTIVE_BITS) - 1, "active count overflow");
            match self.word.compare_exchange(
                w,
                w + ACTIVE_ONE,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(cur) => w = cur,
            }
        }
    }

    fn leave(&self, gen: Generation) {
        let mut w = self.word.load(Ordering::SeqCst);
        loop {
            debug_assert_eq!(
                gen_of(w),
                gen.0,
                "guard outlived its generation: retire ran with active > 0"
            );
            debug_assert!(active_of(w) > 0, "leave without join");
            match self.word.compare_exchange(
                w,
                w - ACTIVE_ONE,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return,
                Err(cur) => w = cur,
            }
        }
    }
}

impl Default for TaskLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII participation in one generation of one query's shared state. Holding
/// a guard proves the generation was live at entry and pins it against
/// retirement (not against abort — abort is a flag; the holder observes it at
/// its next morsel boundary and drains).
pub struct QueryTaskGuard {
    lifecycle: Arc<TaskLifecycle>,
    gen: Generation,
}

impl QueryTaskGuard {
    /// Enter generation `gen`. None ⇔ the generation is aborted, retired, or
    /// never opened — i.e., the task is unconsumable.
    pub fn enter(lifecycle: &Arc<TaskLifecycle>, gen: Generation) -> Option<QueryTaskGuard> {
        if lifecycle.try_join(gen) {
            Some(QueryTaskGuard { lifecycle: Arc::clone(lifecycle), gen })
        } else {
            None
        }
    }

    pub fn generation(&self) -> Generation {
        self.gen
    }

    /// Abort observed? Checked at morsel boundaries (Leis-style cancel
    /// points): the holder stops claiming and lets the guard drop.
    pub fn aborted(&self) -> bool {
        self.lifecycle.is_aborted(self.gen)
    }
}

impl Drop for QueryTaskGuard {
    fn drop(&mut self) {
        self.lifecycle.leave(self.gen);
    }
}
