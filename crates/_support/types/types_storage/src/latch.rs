//! Latch identity (`storage/latch.h`), trimmed to what consumers of the
//! unported latch unit need: a way to *name* a particular `Latch` (C passes
//! `Latch *`) when calling `SetLatch`/`ResetLatch`/`AddWaitEventToSet`.
//!
//! The C `struct Latch` itself (`is_set`, `maybe_sleeping`, `is_shared`,
//! `owner_pid`) lives in backend-private or shared memory owned by the latch
//! unit; no current consumer reads its fields, so only the identity token is
//! modeled. C call sites that read `MyLatch` (globals.c) translate to an
//! explicit `LatchHandle` parameter — the caller obtains it from its own
//! state when globals.c lands (AGENTS.md "no ambient-global seams").

use ::types_core::ProcNumber;

/// Identity of a `Latch *`. In C a latch is reached by raw pointer, and the
/// two backing allocations are distinct address spaces: backend-private /
/// process-global `Latch` storage (`LocalLatchData`, the recovery wakeup
/// latch, …) carved by the latch unit, and the `Latch` embedded in each
/// `PGPROC` of the shared `ProcGlobal->allProcs` array (`&proc->procLatch`).
/// A single `usize` cannot name both faithfully, so the handle is a *tagged*
/// union of the two spaces, mirroring which allocation the C `Latch *` points
/// into:
///
/// * **Local** — `id` is the latch unit's own registry slot (`index + 1`;
///   `0` is the never-valid NULL handle), the analogue of a C caller's own
///   `Latch` variable.
/// * **Proc** — `id` (with the [`PROC_TAG`] bit set) names
///   `ProcGlobal->allProcs[procno].procLatch` by its `ProcNumber`; the proc
///   unit owns that array, the latch unit reaches the embedded `Latch`
///   through the proc unit's seam.
///
/// `lookup`/`SetLatch` dispatch on [`kind`](LatchHandle::kind), so a
/// procno-derived handle resolves to the right PGPROC's latch instead of
/// indexing the local registry.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct LatchHandle(usize);

/// High bit of a [`LatchHandle`] id: set iff the handle names a per-`PGPROC`
/// `procLatch` (the [`LatchKind::Proc`] space) rather than the latch unit's
/// local registry.
pub const PROC_TAG: usize = 1usize << (usize::BITS - 1);

/// Which `Latch` allocation a [`LatchHandle`] names (see [`LatchHandle`]).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum LatchKind {
    /// A latch in the latch unit's own process-global registry: `id` is the
    /// registry slot id (`index + 1`).
    Local(usize),
    /// `ProcGlobal->allProcs[procno].procLatch`, named by its `ProcNumber`.
    Proc(ProcNumber),
}

impl LatchHandle {
    /// Mint a *local* handle (the latch unit's own registry id, or another
    /// process-global latch the latch unit carves). `id` must not set the
    /// [`PROC_TAG`] bit — that bit is reserved for the per-PGPROC space.
    pub fn new(id: usize) -> Self {
        debug_assert!(id & PROC_TAG == 0, "local LatchHandle id overflows PROC_TAG");
        LatchHandle(id)
    }

    /// Mint a handle naming `ProcGlobal->allProcs[procno].procLatch`
    /// (`&proc->procLatch`). The proc unit owns that array; the latch unit
    /// resolves the embedded `Latch` through the proc seam.
    pub fn proc(procno: ProcNumber) -> Self {
        debug_assert!(procno >= 0);
        LatchHandle(PROC_TAG | (procno as usize))
    }

    /// The raw owner-side id this handle names (tag bit included).
    pub fn as_usize(self) -> usize {
        self.0
    }

    /// Decode which `Latch` allocation this handle names.
    pub fn kind(self) -> LatchKind {
        if self.0 & PROC_TAG != 0 {
            LatchKind::Proc((self.0 & !PROC_TAG) as ProcNumber)
        } else {
            LatchKind::Local(self.0)
        }
    }
}

use core::sync::atomic::{AtomicBool, AtomicI32};

use ::types_core::sig_atomic_t;

/// `struct Latch` (`storage/latch.h`).
///
/// In C a latch is always reached through a pointer (`MyLatch`,
/// `&proc->procLatch`) and is mutated concurrently: `SetLatch` runs from
/// signal handlers and, for shared latches living in PGPROC shared memory,
/// from other backends. Every field is therefore an atomic, and a latch is
/// reached by shared reference (`&Latch`), never copied by value: the
/// `volatile sig_atomic_t` `is_set`/`maybe_sleeping` flags are written
/// cross-process, and `SetLatch` fetches `owner_pid` once "in case the latch
/// is concurrently getting owned or disowned" — both `OwnLatch`/`DisownLatch`
/// (writers) and `SetLatch` (reader) reach it through a shared `&Latch`, so it
/// is atomic too. This is the single `Latch` representation: the latch unit's
/// own registry latches and the `Latch` embedded in each `PGPROC` (`procLatch`)
/// are the same type.
#[derive(Debug)]
pub struct Latch {
    /// `sig_atomic_t is_set;`
    pub is_set: AtomicI32,
    /// `sig_atomic_t maybe_sleeping;`
    pub maybe_sleeping: AtomicI32,
    /// `bool is_shared;` — written at init through a shared `&Latch`
    /// (`InitLatch`/`InitSharedLatch`/`SwitchToLocalLatch`), so atomic.
    pub is_shared: AtomicBool,
    /// `int owner_pid;` — fetched once by `SetLatch`, set/cleared by
    /// `OwnLatch`/`DisownLatch`, all through a shared `&Latch`.
    pub owner_pid: AtomicI32,
}

impl Latch {
    /// A cleared latch (`is_set`/`maybe_sleeping` zero), as `InitLatch`
    /// leaves the flag fields.
    pub fn new(is_shared: bool, owner_pid: i32) -> Latch {
        Latch {
            is_set: AtomicI32::new(0),
            maybe_sleeping: AtomicI32::new(0),
            is_shared: AtomicBool::new(is_shared),
            owner_pid: AtomicI32::new(owner_pid),
        }
    }

    /// Read `latch->is_set` (the wait-event-set owner's poll loop reads it).
    pub fn is_set(&self) -> bool {
        self.is_set.load(core::sync::atomic::Ordering::SeqCst) != 0
    }

    /// Write `latch->maybe_sleeping` (waiteventset.c sets it around a wait).
    pub fn set_maybe_sleeping(&self, value: bool) {
        self.maybe_sleeping
            .store(value as i32, core::sync::atomic::Ordering::SeqCst);
    }

    /// Read `latch->owner_pid`.
    pub fn owner_pid(&self) -> i32 {
        self.owner_pid.load(core::sync::atomic::Ordering::SeqCst)
    }
}

/// Assert the C field widths: `sig_atomic_t` is `int` on every supported
/// target and `AtomicI32` has the same in-memory representation.
const _: () = assert!(core::mem::size_of::<AtomicI32>() == core::mem::size_of::<sig_atomic_t>());
