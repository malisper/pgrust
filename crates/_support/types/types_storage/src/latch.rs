// pgsync = THE single lock library (permit-s1 contract §0): native/sim these
// are std's atomics (std::sync::atomic IS core's — zero cost, identical
// types); under --cfg loom they are loom's checked atomics, which is what
// makes the latch protocol directly loom-modelable (LATCH-LOOM lane).
use pgsync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};

use ::types_core::ProcNumber;
#[cfg(not(loom))]
use ::types_core::sig_atomic_t;

// Tagged handle naming a C `Latch *`: the latch unit's own registry (Local),
// a PGPROC's embedded procLatch (Proc, PROC_TAG bit set), or the single
// recovery-wakeup latch (reserved id usize::MAX).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct LatchHandle(usize);

pub const PROC_TAG: usize = 1usize << (usize::BITS - 1);

pub const RECOVERY_WAKEUP_ID: usize = usize::MAX;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum LatchKind {
    Local(usize),
    Proc(ProcNumber),
    RecoveryWakeup,
}

impl LatchHandle {
    pub fn new(id: usize) -> Self {
        debug_assert!(id & PROC_TAG == 0, "local LatchHandle id overflows PROC_TAG");
        LatchHandle(id)
    }

    pub fn proc(procno: ProcNumber) -> Self {
        debug_assert!(procno >= 0);
        LatchHandle(PROC_TAG | (procno as usize))
    }

    pub fn recovery_wakeup() -> Self {
        LatchHandle(RECOVERY_WAKEUP_ID)
    }

    pub fn as_usize(self) -> usize {
        self.0
    }

    /// Rebuild a handle from `as_usize`'s value (stored-in-atomic round trip).
    pub fn from_raw(raw: usize) -> Self {
        LatchHandle(raw)
    }

    pub fn kind(self) -> LatchKind {
        if self.0 == RECOVERY_WAKEUP_ID {
            LatchKind::RecoveryWakeup
        } else if self.0 & PROC_TAG != 0 {
            LatchKind::Proc((self.0 & !PROC_TAG) as ProcNumber)
        } else {
            LatchKind::Local(self.0)
        }
    }
}

// Every field atomic: SetLatch runs from signal handlers and, for shared
// latches in PGPROC shmem, from other backends — all through `&Latch`.
#[derive(Debug)]
pub struct Latch {
    pub is_set: AtomicI32,
    pub maybe_sleeping: AtomicI32,
    pub is_shared: AtomicBool,
    pub owner_pid: AtomicI32,
    /// Packed waiter::WakerHandle of the owner's structured-wait slot
    /// (0 = none), re-published by the owner at every wait entry BEFORE it
    /// arms maybe_sleeping. This is the wake ROUTE — set_latch unparks it
    /// directly; no global pid-keyed wakeup registry exists. owner_pid stays
    /// for ownership assertions and diagnostics only.
    pub waker: AtomicU64,
}

impl Latch {
    /// Const on native/sim (the const-init static slab in the latch crate
    /// depends on it); loom's atomics are not const-constructible, so under
    /// `--cfg loom` this is a plain fn and the slab rides
    /// `pgsync::process_global!` (per-iteration lazy state).
    #[cfg(not(loom))]
    pub const fn new(is_shared: bool, owner_pid: i32) -> Latch {
        Latch {
            is_set: AtomicI32::new(0),
            maybe_sleeping: AtomicI32::new(0),
            is_shared: AtomicBool::new(is_shared),
            owner_pid: AtomicI32::new(owner_pid),
            waker: AtomicU64::new(0),
        }
    }

    #[cfg(loom)]
    pub fn new(is_shared: bool, owner_pid: i32) -> Latch {
        Latch {
            is_set: AtomicI32::new(0),
            maybe_sleeping: AtomicI32::new(0),
            is_shared: AtomicBool::new(is_shared),
            owner_pid: AtomicI32::new(owner_pid),
            waker: AtomicU64::new(0),
        }
    }

    // -- the Dekker protocol edges (SetLatch/WaitLatch/ResetLatch) ----------
    //
    // C latch.c orders PLAIN flag accesses with pg_memory_barrier() (a full
    // fence); the port renders each barrier as fence(SeqCst) in the latch
    // crate with the field accesses below (notes/latch-atomics.md — nothing
    // silently weakened; the native bodies here are byte-for-byte what the
    // latch crate open-coded before this refactor, asm letter in
    // notes/dst-latch-loom.md).
    //
    // LOOM DIALECT (cfg(loom) arms): loom 0.7 does not model the single
    // total order of SeqCst operations/fences — it admits the store-buffer
    // miss-miss outcome the C++ model forbids (PROVEN by
    // waiter/tests/loom_litmus.rs, which fails the day loom gains real SC
    // support). Fence-disciplined plain READS therefore become RMW reads
    // under loom ONLY (loom binds RMWs to the latest value in modification
    // order — exactly the recency the fences guarantee on real hardware, at
    // equal-or-stronger ordering); each read edge's RMW carries the adjacent
    // pg_memory_barrier()'s strength, and demoting one to its native text
    // models OMITTING that C barrier — the latch loom models must then fail
    // (the lane's red battery, worklog table).
    //
    // THE DIALECT LAW (LATCH-LOOM fidelity finding, red-battery R2):
    //   * READ edges  -> `fetch_add(0, SeqCst)`: an un-stale read (RMW reads
    //     the modification-order latest) with an acquire side that pairs
    //     with the write edge's release.
    //   * WRITE edges -> `swap(_, Release)`: an UN-BUFFERABLE write (loom
    //     may order a plain store after later RMWs in modification order —
    //     the litmus weakness restated — so a plain-store write edge
    //     readmits the miss-miss deadlock) whose RELAXED READ SIDE ACQUIRES
    //     NOTHING. That last property is load-bearing for model teeth: an
    //     RMW read also inserts a phantom write into the cell's history, and
    //     a SeqCst swap on a write edge acquire-chains through the OTHER
    //     side's phantom recheck-writes, causally publishing unrelated prior
    //     state. Concretely: with mark_set as a SeqCst swap, the setter
    //     chained waker-word visibility through the WAITER's own post-arm
    //     is_set recheck (phantom write -> swap acquire-read), and the
    //     publish-after-arm lost-wake red passed silently — a hidden bug
    //     class. Release-swap write edges close exactly that hole.
    //   * The one SeqCst RMW WRITE is reset_clear_is_set (see its doc — its
    //     acquire-read of the probe's phantom write IS the dialect's
    //     rendering of the fence-to-fence edge in the reset race).
    //   * FENCES: the protocol's pg_memory_barrier() calls ride
    //     [`pg_memory_barrier`] below — real fence(SeqCst) native/sim,
    //     NO-OP under loom. Two reasons, both proven in the lane's red
    //     battery: (1) loom's SC-op total order is disabled (loom
    //     rt/thread.rs seq_cst(): "as a quick fix, just disable it"), so a
    //     fence buys no SC ordering anyway — the RMW edges carry each
    //     barrier's strength; (2) loom's fence(SeqCst) IS an
    //     acquire-everything-seen fence (rt/atomic.rs fence_acq), which
    //     upgrades the write-edge swaps' RELAXED phantom reads into acquire
    //     syncs — with the fence live under loom, the setter acquire-chained
    //     the WAITER's post-arm recheck clock through mark_set's phantom
    //     read and the publish-after-arm red (R2) passed silently.
    //
    // Precedent: the latch-over-Waiter mirror in waiter/tests/loom.rs used
    // the coarser all-SeqCst-RMW translation (kept frozen there,
    // superseded — it carries the phantom-chain over-synchronization the
    // law above names); these methods move the refined dialect into the
    // production type so the models drive the REAL wait/set paths.

    /// Wait-loop read of `is_set` (loop top + the post-arm Dekker recheck),
    /// and the model-side observation read. C waiteventset.c
    /// WaitEventSetWait: `set->latch->maybe_sleeping = true;
    /// pg_memory_barrier(); /* and recheck */` then
    /// `if (set->latch && set->latch->is_set)` — that recheck is what pairs
    /// with SetLatch's second barrier. SeqCst load (the port's
    /// at-least-as-strong waiter-side vocabulary, pre-existing —
    /// notes/latch-atomics.md). No inline attribute: base-parity codegen
    /// (the wait path called this out-of-line before the LATCH-LOOM
    /// refactor and still does; LTO inlines it in the shipped binary).
    pub fn is_set(&self) -> bool {
        #[cfg(not(loom))]
        return self.is_set.load(Ordering::SeqCst) != 0;
        #[cfg(loom)]
        return self.is_set.fetch_add(0, Ordering::SeqCst) != 0;
    }

    /// Arm/disarm `maybe_sleeping` around the park. C waiteventset.c
    /// WaitEventSetWait: `/* about to sleep on a latch */
    /// set->latch->maybe_sleeping = true; pg_memory_barrier();
    /// /* and recheck */` — the SeqCst store carries that barrier (arming is
    /// the waiter's half of the Dekker pair). Disarm is ordinary (C clears
    /// it after the wait with no barrier of its own).
    /// Loom: a WRITE edge — Release swap (un-bufferable, non-acquiring; the
    /// dialect law above). The setter's SC-RMW read acquire-pairs with the
    /// release write, which is what publishes the waker word stored before
    /// the arm. No inline attribute: base-parity codegen (see is_set).
    pub fn set_maybe_sleeping(&self, value: bool) {
        #[cfg(not(loom))]
        self.maybe_sleeping.store(value as i32, Ordering::SeqCst);
        #[cfg(loom)]
        self.maybe_sleeping.swap(value as i32, Ordering::Release);
    }

    /// SetLatch's early-return probe. C latch.c SetLatch: `/* Quick exit if
    /// already set */ if (latch->is_set) return;` — ordered by the FIRST
    /// pg_memory_barrier() ("The memory barrier has to be placed here to
    /// ensure that any flag variables possibly changed by this process have
    /// been flushed to main memory, before we check/set is_set"), which is
    /// the caller-side fence(SeqCst) in the latch crate; the load itself is
    /// Relaxed, exactly C's plain read. The recency of this read against a
    /// concurrent ResetLatch is what ResetLatch's own barrier guarantees
    /// ("a concurrent SetLatch might falsely conclude that it needn't
    /// signal us").
    #[inline(always)]
    pub fn set_path_probe_is_set(&self) -> bool {
        #[cfg(not(loom))]
        return self.is_set.load(Ordering::Relaxed) != 0;
        #[cfg(loom)]
        return self.is_set.fetch_add(0, Ordering::SeqCst) != 0;
    }

    /// SetLatch's flag store: `latch->is_set = true;` — plain in C, ordered
    /// by the barriers on either side (the latch crate's two fences). Loom:
    /// a WRITE edge — Release swap, deliberately NOT SeqCst (the dialect
    /// law above: a SeqCst swap here acquire-chains through the waiter
    /// recheck's phantom write and hides wake-route ordering bugs — the
    /// red-battery R2 finding).
    #[inline(always)]
    pub fn set_path_mark_set(&self) {
        #[cfg(not(loom))]
        self.is_set.store(1, Ordering::Relaxed);
        #[cfg(loom)]
        self.is_set.swap(1, Ordering::Release);
    }

    /// SetLatch's sleeper probe. C latch.c SetLatch: "pg_memory_barrier();
    /// if (!latch->maybe_sleeping) return;" — the SECOND barrier is the
    /// Dekker store->load edge (Release/Acquire cannot express it,
    /// notes/latch-atomics.md). The Acquire on the load is load-bearing
    /// BEYOND the fence: it pairs with the waiter's SeqCst maybe_sleeping
    /// store, ordering the waker word published before it — with a Relaxed
    /// load a stale waker (0 on a first wait) can be read and the wake lost
    /// (found by the original latch-over-Waiter loom model).
    #[inline(always)]
    pub fn set_path_sleeping_armed(&self) -> bool {
        #[cfg(not(loom))]
        return self.maybe_sleeping.load(Ordering::Acquire) != 0;
        #[cfg(loom)]
        return self.maybe_sleeping.fetch_add(0, Ordering::SeqCst) != 0;
    }

    /// ResetLatch's clear: `latch->is_set = false;` followed by
    /// pg_memory_barrier() — C latch.c: "Ensure that the write to is_set
    /// gets flushed to main memory before we examine any flag variables.
    /// Otherwise a concurrent SetLatch might falsely conclude that it
    /// needn't signal us, even though we have missed seeing some flag
    /// updates that SetLatch was supposed to inform us of." The fence stays
    /// in the latch crate; this is the plain store it orders. Loom: the ONE
    /// deliberate RMW write — its acquire-read of the setter probe's
    /// phantom write is the dialect's rendering of that C comment's edge
    /// (probe-saw-stale-1 ⇒ the owner's post-reset flag re-read
    /// happens-after the setter's flag store, so the work is never missed).
    #[inline(always)]
    pub fn reset_clear_is_set(&self) {
        #[cfg(not(loom))]
        self.is_set.store(0, Ordering::Relaxed);
        #[cfg(loom)]
        self.is_set.swap(0, Ordering::SeqCst);
    }

    pub fn owner_pid(&self) -> i32 {
        self.owner_pid.load(Ordering::SeqCst)
    }
}

/// C's pg_memory_barrier() as placed by latch.c (`dmb ish` on aarch64;
/// rendered fence(SeqCst) with Relaxed field accesses —
/// notes/latch-atomics.md, nothing silently weakened). Under loom this is a
/// deliberate NO-OP: see the dialect law's FENCES row above — the protocol
/// edges' RMWs carry each barrier's strength in loom's dialect, and loom's
/// own fence(SeqCst) both buys nothing (SC-op order disabled) and poisons
/// the model (acquire-everything-seen upgrades the write edges' phantom
/// reads, hiding ordering bugs — the R2 finding).
#[inline(always)]
pub fn pg_memory_barrier() {
    #[cfg(not(loom))]
    core::sync::atomic::fence(core::sync::atomic::Ordering::SeqCst);
}

#[cfg(not(loom))]
const _: () = assert!(core::mem::size_of::<AtomicI32>() == core::mem::size_of::<sig_atomic_t>());

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latch_handle_kind_round_trip() {
        assert_eq!(LatchHandle::new(3).kind(), LatchKind::Local(3));
        assert_eq!(LatchHandle::proc(17).kind(), LatchKind::Proc(17));
        assert_eq!(
            LatchHandle::recovery_wakeup().kind(),
            LatchKind::RecoveryWakeup
        );
    }
}
