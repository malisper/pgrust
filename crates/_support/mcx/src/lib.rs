// Memory contexts with allocator-tied lifetimes; no ambient current context.

#![no_std]

extern crate alloc;

// Per-thread pool arm (see `local_pool_on`) and the session-root retiring
// flag; unit tests link std as well. Everything else stays no_std.
#[cfg(any(feature = "std", test))]
extern crate std;

use core::alloc::Layout;
use core::cell::{Cell, RefCell};
use core::fmt;
use core::ptr::NonNull;

pub use allocator_api2::alloc::Allocator;

use allocator_api2::alloc::{AllocError, Global};
use ::types_error::{PgError, PgResult, ERRCODE_OUT_OF_MEMORY, ERRCODE_PROGRAM_LIMIT_EXCEEDED};

mod arena_safe;
mod aset;
mod bump;
// upstream 3f3eefc28892 (18.4): Detect pfree or repalloc of a previously-freed memory chunk.
#[cfg(debug_assertions)]
mod freed;
mod generation;
mod owned;
mod slab;
mod string;
pub use arena_safe::{ArenaForget, ArenaSafe, ForgetSafe};
pub use aset::alloc_stats;
pub use owned::{Bind, McxOwned, PinnedContext};
pub use string::PgString;

/// # Safety: caller asserts the full [`ArenaSafe`] contract; the field list is only a guard.
#[macro_export]
macro_rules! assert_arena_safe {
    ($($ty:ty { $($field:ident : $fty:ty),* $(,)? }),+ $(,)?) => {
        $(
            // SAFETY: asserted by the caller per the ArenaSafe contract.
            unsafe impl $crate::ArenaSafe for $ty {}
            const _: fn() = || {
                fn _assert_field_arena_safe<T: $crate::ArenaSafe>() {}
                $( let _ = _assert_field_arena_safe::<$fty>; )*
            };
        )+
    };
    ($($ty:ty),+ $(,)?) => {
        $(
            // SAFETY: asserted by the caller per the ArenaSafe contract.
            unsafe impl $crate::ArenaSafe for $ty {}
        )+
    };
}

pub type PgVec<'mcx, T> = allocator_api2::vec::Vec<T, Mcx<'mcx>>;
pub type PgBox<'mcx, T> = allocator_api2::boxed::Box<T, Mcx<'mcx>>;
pub type PgHashMap<'mcx, K, V> =
    hashbrown::HashMap<K, V, hashbrown::hash_map::DefaultHashBuilder, Mcx<'mcx>>;
// AGENTS rule 9: internal tables default to FxHash.
pub type PgFxHashMap<'mcx, K, V> = hashbrown::HashMap<K, V, rustc_hash::FxBuildHasher, Mcx<'mcx>>;

enum Backend {
    // UnsafeCell, not RefCell: palloc's hot path pays no borrow flag (see aset_mut).
    Aset(core::cell::UnsafeCell<aset::AllocSet>),
    // C-parity backend (mcxt.c malloc-wrapper contexts); no constructor wired yet.
    #[allow(dead_code)]
    Malloc,
    Bump(core::cell::UnsafeCell<bump::BumpArena>),
    // Bump + drop list: leaked owned values run their destructor once at reset.
    BumpDrop(core::cell::UnsafeCell<bump::BumpArena>, RefCell<DropList>),
    BumpForget(core::cell::UnsafeCell<bump::BumpArena>),
    Generation(core::cell::UnsafeCell<generation::GenArena>),
    Slab(core::cell::UnsafeCell<slab::SlabArena>),
}

// Exposed-provenance address (not a borrow-stack sibling of the returned &mut).
struct DropEntry {
    addr: *mut u8,
    glue: unsafe fn(*mut u8),
}

struct DropList {
    entries: alloc::vec::Vec<DropEntry>,
}

impl DropList {
    fn new() -> Self {
        DropList { entries: alloc::vec::Vec::new() }
    }

    // Pop the most-recently-registered pending entry, leaving the rest in place.
    // Callers (run_drop_glue) pop under a brief scoped borrow and run the entry
    // with NO borrow held, so user drop glue — arbitrary safe code that can
    // re-enter this same context (allocate/reset, or register another drop) —
    // cannot alias the exclusive borrow (UB in reset_noncore's get_mut) or
    // double-borrow (MemoryContext::drop). Because un-run entries stay in the
    // list, a panicking destructor leaves the remainder for the next reset/drop
    // (they are never double-run, since each is removed before it runs).
    fn pop(&mut self) -> Option<DropEntry> {
        self.entries.pop()
    }
}

// Run all pending drop glue, popping each entry under a fresh scoped borrow via
// `next` (borrow released before the entry runs) so re-entrancy is safe, and a
// panicking destructor leaves the not-yet-popped remainder in the DropList for
// a subsequent reset/drop rather than losing or double-running them.
fn run_drop_glue(mut next: impl FnMut() -> Option<DropEntry>) {
    while let Some(entry) = next() {
        // SAFETY: live leaked value of glue's type, Drop suppressed; sole drop, before arena reset.
        unsafe { (entry.glue)(entry.addr) };
    }
}

/// # Safety: `addr` must be an exposed, live, aligned `T` never otherwise dropped.
unsafe fn drop_glue<T>(addr: *mut u8) {
    let p = core::ptr::with_exposed_provenance_mut::<T>(addr as usize);
    core::ptr::drop_in_place(p);
}

unsafe fn drop_glue_noop(_addr: *mut u8) {}

// Element-only glue for a leaked PgVec: never Vec::drop (deallocate through the leaked Mcx is the SB trap).
/// # Safety: `addr` — exposed address of a live arena `PgVec` header, element drops unrun.
unsafe fn drop_glue_vec_elems<T>(addr: *mut u8) {
    let header = core::ptr::with_exposed_provenance_mut::<PgVec<'static, T>>(addr as usize);
    let v: &mut PgVec<'static, T> = &mut *header;
    let len = v.len();
    let data: *mut T = v.as_mut_ptr();
    // len = 0 first guards a re-entrant double-drop.
    v.set_len(0);
    core::ptr::drop_in_place(core::ptr::slice_from_raw_parts_mut(data, len));
}

// Subtree totals summed on demand (C's recursive MemoryContextMemAllocated); charge never walks ancestors.
pub(crate) struct Acct {
    name: Cell<&'static str>,
    ident: RefCell<Option<alloc::string::String>>,
    pub(crate) self_used: Cell<usize>,
    pub(crate) self_peak: Cell<usize>,
    pub(crate) limit: Cell<usize>,
    pub(crate) limited_path: Cell<bool>,
    pub(crate) arena_footprint: Cell<usize>,
    pub(crate) arena_nblocks: Cell<usize>,
    // Active-window tail as of the last block transition (bump backends only);
    // per-alloc bumps never touch Acct, so this reads high vs C's live freeptr.
    pub(crate) window_tail: Cell<usize>,
    // aset.c:1545 AllocSetStats inputs at chunk grain (AllocSet backend only;
    // the block pair above is its block grain): the class-rounded bytes of
    // every live chunk (single-chunk blocks at their size), so that
    // arena_footprint - live_chunk_bytes is C's freespace — the block tails
    // (endptr - freeptr) plus every chunk parked on a freelist — and the
    // freelist population (C's freechunks).
    pub(crate) live_chunk_bytes: Cell<usize>,
    pub(crate) free_chunks: Cell<usize>,
    pub(crate) is_bump: bool,
    // An AllocSet whose reset discards live chunks by design (C's aggcontext:
    // transition states of any type are dropped by AllocSetReset), exempt
    // from the leak check exact-accounting contexts get.
    pub(crate) wholesale_reset: Cell<bool>,
    pub(crate) kind: &'static str,
    pub(crate) parent: Option<AcctRc>,
    pub(crate) children: RefCell<alloc::vec::Vec<AcctWeak>>,
}

impl Acct {
    // Block-grain snapshot (AllocSet backend; cold, block transitions only).
    pub(crate) fn set_blocks(&self, footprint: usize, nblocks: usize) {
        self.arena_footprint.set(footprint);
        self.arena_nblocks.set(nblocks);
    }

    // Free bytes as the allocator's stats method reports them: AllocSetStats'
    // block tails + freelist chunks for an AllocSet; the block-transition
    // window-tail snapshot for the bump backends (see window_tail).
    fn free_bytes(&self) -> usize {
        if self.is_bump {
            self.window_tail.get()
        } else {
            self.arena_footprint.get().saturating_sub(self.live_chunk_bytes.get())
        }
    }

    fn ancestors(&self) -> impl Iterator<Item = &Acct> {
        let mut cur: Option<&Acct> = Some(self);
        core::iter::from_fn(move || {
            let node = cur?;
            cur = node.parent.as_deref();
            Some(node)
        })
    }

    #[cold]
    #[inline(never)]
    fn check_limit_slow(&self, n: usize) -> Result<(), AllocError> {
        for node in self.ancestors() {
            let new = node.subtree_sum().checked_add(n).ok_or(AllocError)?;
            if new > node.limit.get() {
                return Err(AllocError);
            }
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn check_limit(&self, n: usize) -> Result<(), AllocError> {
        if self.limited_path.get() {
            return self.check_limit_slow(n);
        }
        Ok(())
    }

    // Block-granular charge (bump backends): the C mem_allocated shape, block transitions only.
    pub(crate) fn commit_block(&self, n: usize, footprint: usize, nblocks: usize) {
        let self_new = self.self_used.get() + n;
        self.self_used.set(self_new);
        if self_new > self.self_peak.get() {
            self.self_peak.set(self_new);
        }
        self.arena_footprint.set(footprint);
        self.arena_nblocks.set(nblocks);
    }

    // Block-granular release (generation/slab free whole blocks mid-life).
    pub(crate) fn uncommit_block(&self, n: usize, footprint: usize, nblocks: usize) {
        self.self_used.set(self.self_used.get().saturating_sub(n));
        self.arena_footprint.set(footprint);
        self.arena_nblocks.set(nblocks);
    }
    fn subtree_sum(&self) -> usize {
        let mut total = self.self_used.get();
        self.children.borrow_mut().retain(|w| match w.upgrade() {
            Some(c) => {
                total = total.saturating_add(c.subtree_sum());
                true
            }
            None => false,
        });
        total
    }

    fn subtree_allocated_sum(&self) -> usize {
        let mut total = if self.kind == "Malloc" {
            self.self_used.get()
        } else {
            self.arena_footprint.get()
        };
        self.children.borrow_mut().retain(|w| match w.upgrade() {
            Some(c) => {
                total = total.saturating_add(c.subtree_allocated_sum());
                true
            }
            None => false,
        });
        total
    }

    fn subtree_peak_sum(&self) -> usize {
        let mut total = self.self_peak.get();
        self.children.borrow_mut().retain(|w| match w.upgrade() {
            Some(c) => {
                total = total.saturating_add(c.subtree_peak_sum());
                true
            }
            None => false,
        });
        total
    }
}

use core::cell::UnsafeCell;
use core::mem::MaybeUninit;

// Pooled single-threaded Rc/Weak for Acct; parked only when both counts are 0, so reuse cannot alias.
struct AcctInner {
    strong: Cell<usize>,
    weak: Cell<usize>,
    val: MaybeUninit<Acct>,
}

const ACCT_POOL_MAX: usize = 256;

// Spin-locked pool: touched only at context create/drop (cold), and dependent
// crates' libtest binaries create contexts on many threads (notes/
// mcx-acct-pool-test-race.md) — the single-thread assumption is not global.
pub(crate) struct PoolMutex<T> {
    locked: core::sync::atomic::AtomicBool,
    val: UnsafeCell<T>,
}

// SAFETY: `with` holds the flag for the whole access; T's values are plain
// process-owned free-list nodes, safe to hand across threads under the lock.
unsafe impl<T> Sync for PoolMutex<T> {}

impl<T> PoolMutex<T> {
    pub(crate) const fn new(val: T) -> Self {
        PoolMutex {
            locked: core::sync::atomic::AtomicBool::new(false),
            val: UnsafeCell::new(val),
        }
    }

    // Single CAS attempt, no spin: a contended pool is bypassed (caller falls
    // through to Global/mimalloc). Spinning here collapsed the multi-backend
    // write gate at clients > vCPU (75% of cycles in __aarch64_cas1_acq;
    // m4mc-gate job pgrust-m4mc-gate-1783126728-37375).
    #[inline]
    pub(crate) fn try_with<R>(&self, f: impl FnOnce(&mut T) -> R) -> Option<R> {
        use core::sync::atomic::Ordering;
        if self
            .locked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return None;
        }
        // SAFETY: the flag serializes access; released below.
        let r = f(unsafe { &mut *self.val.get() });
        self.locked.store(false, Ordering::Release);
        Some(r)
    }
}

// ---------------------------------------------------------------------------
// Per-thread pool arm (std builds only; the no_std/global arm below is the
// fallback and the kill-switch target).
//
// The spin-locked global pools serialize every backend's context
// create/destroy on a handful of shared cache lines, and the acquire-CAS line
// itself becomes the dominant cost at high backend counts (global pool mutex
// contention; the try_with comment's no-spin rule bounds the damage but the
// line stays hot). One backend = one thread (AGENTS rule 10), and
// `AcctRc`/`MemoryContext` are `!Send`, so a context is created and destroyed
// on the same thread — pooling by the freeing thread needs no atomics at all.
// Entries are raw blocks / bare `Vec` capacity, so even an unsafely-moved
// handle would only change WHICH pool caches a block, never correctness.
//
// The TLS payloads carry Drop deliberately (a considered deviation from the
// rule-10 `!needs_drop` guidance): a rotating backend thread must hand its
// cached blocks back to the allocator at exit or every session leaks its pool
// residue into the shared process (the session-roots lesson, further down
// this file). Residency
// is bounded per thread and matches C, whose freelists are per-process =
// per-backend (aset.c context_freelists, MAX_FREE_CONTEXTS).
//
// Kill switch (t35 law): PGRUST_MCX_POOL_STRIPE=0 restores the global
// spin-locked pools, byte-identical semantics either way.
// ---------------------------------------------------------------------------

// not(test): mcx's own unit tests bypass pooling entirely (see acct_take).
#[cfg(all(feature = "std", not(test)))]
#[inline]
// pub for proofs/text-slice (Kani stubs the OnceLock/env read, selecting the
// global-pool arm; visibility-only change per the 2026-07-28 shipped-edits
// ruling).
pub fn local_pool_on() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !std::env::var("PGRUST_MCX_POOL_STRIPE").is_ok_and(|v| v.trim() == "0")
    })
}

/// Bounded per-thread free list. `Cell<Vec>` take/put keeps every access
/// panic-free: a (hypothetical) re-entrant access sees an empty list and
/// falls through to `Global`, never a borrow error. `dispose` runs on
/// overflow and at thread exit (the TLS Drop).
#[cfg(feature = "std")]
pub(crate) struct LocalStack<T> {
    items: Cell<alloc::vec::Vec<T>>,
    cap: usize,
    dispose: fn(T),
}

#[cfg(feature = "std")]
impl<T> LocalStack<T> {
    pub(crate) const fn new(cap: usize, dispose: fn(T)) -> Self {
        LocalStack { items: Cell::new(alloc::vec::Vec::new()), cap, dispose }
    }

    pub(crate) fn take(&self) -> Option<T> {
        let mut v = self.items.take();
        let r = v.pop();
        self.items.set(v);
        r
    }

    /// Full list: the incoming item is disposed (the ACCT/CHILD_VEC arm).
    pub(crate) fn give(&self, item: T) {
        let mut v = self.items.take();
        if v.len() >= self.cap {
            self.items.set(v);
            (self.dispose)(item);
            return;
        }
        v.push(item);
        self.items.set(v);
    }

    /// Full list: drain EVERYTHING, then keep the incoming item (the keeper
    /// arm; C's context_freelists overflow discipline).
    pub(crate) fn give_wholesale(&self, item: T) {
        let mut v = self.items.take();
        if v.len() >= self.cap {
            for it in v.drain(..) {
                (self.dispose)(it);
            }
        }
        v.push(item);
        self.items.set(v);
    }

    /// D3.4 idle passivation: dispose everything parked here, now.
    pub(crate) fn drain_all(&self) {
        for it in self.items.take() {
            (self.dispose)(it);
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        let v = self.items.take();
        let n = v.len();
        self.items.set(v);
        n
    }
}

#[cfg(feature = "std")]
impl<T> Drop for LocalStack<T> {
    fn drop(&mut self) {
        for it in self.items.take() {
            (self.dispose)(it);
        }
    }
}

#[cfg(all(feature = "std", not(test)))]
mod tls_pools {
    use super::*;

    fn dispose_acct(p: NonNull<AcctInner>) {
        // SAFETY: pool entries come from acct_alloc_global with `val` already
        // dropped; the list is the sole owner.
        unsafe { Global.deallocate(p.cast(), Layout::new::<AcctInner>()) };
    }

    std::thread_local! {
        pub(crate) static ACCT: LocalStack<NonNull<AcctInner>> =
            const { LocalStack::new(ACCT_POOL_MAX, dispose_acct) };
        pub(crate) static CHILD_VECS: LocalStack<alloc::vec::Vec<AcctWeak>> =
            const { LocalStack::new(CHILD_VEC_POOL_MAX, core::mem::drop) };
    }
}

type AcctPool = PoolMutex<alloc::vec::Vec<NonNull<AcctInner>>>;
static ACCT_POOL: AcctPool = AcctPool::new(alloc::vec::Vec::new());

// Children-vec capacity pool: C's child links are intrusive (no allocation);
// per-query context churn must not pay a malloc/free for a 1-2 entry Vec.
type ChildVecPool = PoolMutex<alloc::vec::Vec<alloc::vec::Vec<AcctWeak>>>;
static CHILD_VEC_POOL: ChildVecPool = ChildVecPool::new(alloc::vec::Vec::new());
const CHILD_VEC_POOL_MAX: usize = 64;

#[inline]
fn child_vec_take() -> alloc::vec::Vec<AcctWeak> {
    #[cfg(all(feature = "std", not(test)))]
    if local_pool_on() {
        // TLS gone (thread exiting) or empty either way: fresh Vec.
        return tls_pools::CHILD_VECS
            .try_with(|s| s.take())
            .ok()
            .flatten()
            .unwrap_or_default();
    }
    CHILD_VEC_POOL.try_with(|s| s.pop()).flatten().unwrap_or_default()
}

#[inline]
fn child_vec_give(v: alloc::vec::Vec<AcctWeak>) {
    debug_assert!(v.is_empty());
    if v.capacity() == 0 {
        return;
    }
    #[cfg(all(feature = "std", not(test)))]
    if local_pool_on() {
        // TLS gone: the unrun closure drops `v` — a plain empty Vec free.
        let _ = tls_pools::CHILD_VECS.try_with(move |s| s.give(v));
        return;
    }
    let _ = CHILD_VEC_POOL.try_with(move |s| {
        if s.len() < CHILD_VEC_POOL_MAX {
            s.push(v);
        }
    });
}

#[inline]
fn acct_take() -> NonNull<AcctInner> {
    #[cfg(not(test))]
    {
        #[cfg(feature = "std")]
        if local_pool_on() {
            if let Ok(Some(p)) = tls_pools::ACCT.try_with(|s| s.take()) {
                return p;
            }
            // TLS empty or gone (thread exiting): fresh block.
            return acct_alloc_global();
        }
        return acct_take_from(&ACCT_POOL);
    }
    #[cfg(test)]
    acct_alloc_global()
}

#[inline]
fn acct_give(p: NonNull<AcctInner>) {
    #[cfg(not(test))]
    {
        #[cfg(feature = "std")]
        if local_pool_on() {
            if tls_pools::ACCT.try_with(|s| s.give(p)).is_err() {
                // TLS gone (thread exiting; Err = closure never ran).
                // SAFETY: from acct_alloc_global; `val` already dropped,
                // nothing else owns it.
                unsafe { Global.deallocate(p.cast(), Layout::new::<AcctInner>()) };
            }
            return;
        }
        acct_give_to(&ACCT_POOL, p);
    }
    #[cfg(test)]
    // SAFETY: from acct_alloc_global; `val` already dropped, nothing else owns it.
    unsafe {
        Global.deallocate(p.cast(), Layout::new::<AcctInner>())
    };
}

#[inline]
fn acct_alloc_global() -> NonNull<AcctInner> {
    match Global.allocate(Layout::new::<AcctInner>()) {
        Ok(p) => p.cast(),
        Err(_) => alloc::alloc::handle_alloc_error(Layout::new::<AcctInner>()),
    }
}

#[inline]
fn acct_take_from(pool: &AcctPool) -> NonNull<AcctInner> {
    if let Some(Some(p)) = pool.try_with(|s| s.pop()) {
        return p;
    }
    acct_alloc_global()
}

#[inline]
fn acct_give_to(pool: &AcctPool, p: NonNull<AcctInner>) {
    let pooled = pool.try_with(|s| {
        if s.len() >= ACCT_POOL_MAX {
            false
        } else {
            s.push(p);
            true
        }
    });
    if pooled != Some(true) {
        // SAFETY: from acct_alloc_global; `val` already dropped, nothing else owns it.
        unsafe { Global.deallocate(p.cast(), Layout::new::<AcctInner>()) };
    }
}

struct AcctRc {
    ptr: NonNull<AcctInner>,
}

struct AcctWeak {
    ptr: NonNull<AcctInner>,
}

impl AcctRc {
    fn new(val: Acct) -> AcctRc {
        let ptr = acct_take();
        // SAFETY: fresh or recycled-with-val-dropped; raw-write all three fields.
        unsafe {
            let inner = ptr.as_ptr();
            core::ptr::addr_of_mut!((*inner).strong).write(Cell::new(1));
            core::ptr::addr_of_mut!((*inner).weak).write(Cell::new(1));
            core::ptr::addr_of_mut!((*inner).val).write(MaybeUninit::new(val));
        }
        AcctRc { ptr }
    }

    #[inline]
    fn downgrade(&self) -> AcctWeak {
        let weak = unsafe { &(*self.ptr.as_ptr()).weak };
        weak.set(weak.get() + 1);
        AcctWeak { ptr: self.ptr }
    }
}

impl Clone for AcctRc {
    #[inline]
    fn clone(&self) -> AcctRc {
        let strong = unsafe { &(*self.ptr.as_ptr()).strong };
        strong.set(strong.get() + 1);
        AcctRc { ptr: self.ptr }
    }
}

impl core::ops::Deref for AcctRc {
    type Target = Acct;
    #[inline]
    fn deref(&self) -> &Acct {
        // SAFETY: `val` is initialized while any strong (self) is live.
        unsafe { (*self.ptr.as_ptr()).val.assume_init_ref() }
    }
}

impl Drop for AcctRc {
    fn drop(&mut self) {
        let inner = self.ptr.as_ptr();
        // SAFETY: Rc discipline — drop val at last strong, reclaim at weak == 0.
        unsafe {
            let strong = &(*inner).strong;
            let s = strong.get() - 1;
            strong.set(s);
            if s != 0 {
                return;
            }
            {
                let val = (*inner).val.assume_init_ref();
                if debug_census::on() {
                    debug_census::dropped(val.name.get());
                }
                let mut children = val.children.borrow_mut();
                children.clear();
                child_vec_give(core::mem::take(&mut *children));
            }
            core::ptr::drop_in_place(core::ptr::addr_of_mut!((*inner).val).cast::<Acct>());
            let weak = &(*inner).weak;
            let w = weak.get() - 1;
            weak.set(w);
            if w == 0 {
                acct_give(self.ptr);
            }
        }
    }
}

impl AcctWeak {
    #[inline]
    fn strong_count(&self) -> usize {
        // SAFETY: a weak keeps the allocation (not the value) alive.
        unsafe { (*self.ptr.as_ptr()).strong.get() }
    }

    fn upgrade(&self) -> Option<AcctRc> {
        // SAFETY: allocation alive via the weak; strong == 0 must not resurrect.
        let strong = unsafe { &(*self.ptr.as_ptr()).strong };
        let s = strong.get();
        if s == 0 {
            None
        } else {
            strong.set(s + 1);
            Some(AcctRc { ptr: self.ptr })
        }
    }
}

impl Clone for AcctWeak {
    #[inline]
    fn clone(&self) -> AcctWeak {
        let weak = unsafe { &(*self.ptr.as_ptr()).weak };
        weak.set(weak.get() + 1);
        AcctWeak { ptr: self.ptr }
    }
}

impl Drop for AcctWeak {
    fn drop(&mut self) {
        let inner = self.ptr.as_ptr();
        // SAFETY: strong is 0 whenever weak reaches 0; val dropped by last AcctRc.
        unsafe {
            let weak = &(*inner).weak;
            let w = weak.get() - 1;
            weak.set(w);
            if w == 0 {
                debug_assert_eq!((*inner).strong.get(), 0);
                acct_give(self.ptr);
            }
        }
    }
}

/// Weak handle to a root (parentless) context's accounting node. Not Send:
/// the counters are Cells; a handle must stay on its creating thread.
pub struct RootWeak(AcctWeak);

impl RootWeak {
    pub fn is_live(&self) -> bool {
        self.0.strong_count() > 0
    }

    pub fn tree_stats(&self) -> Option<TreeStats> {
        self.0.upgrade().map(|rc| tree_stats_node(&rc))
    }
}

// Observer for root-context creation (mcxt.c's TopMemoryContext linkage has no
// ambient equivalent here). Installed once at boot; runs on the creating thread.
static ROOT_OBSERVER: core::sync::atomic::AtomicPtr<()> =
    core::sync::atomic::AtomicPtr::new(core::ptr::null_mut());

pub fn set_root_observer(f: fn(RootWeak)) {
    ROOT_OBSERVER.store(f as *mut (), core::sync::atomic::Ordering::Release);
}

#[inline]
fn notify_root_observer(acct: &AcctRc) {
    let p = ROOT_OBSERVER.load(core::sync::atomic::Ordering::Acquire);
    if !p.is_null() {
        // SAFETY: only set_root_observer stores here, always from fn(RootWeak).
        let f: fn(RootWeak) = unsafe { core::mem::transmute(p) };
        f(RootWeak(acct.downgrade()));
    }
}

/// Process-wide Σ of block bytes held by LIVE memory contexts (GL-MEMWATCH-1).
///
/// Maintained at BLOCK transitions only (arena block alloc/free, dedicated/
/// oversize chunks, recycled-keeper adoption/parking) — one relaxed atomic
/// add/sub beside a system (de)allocation, never on the per-chunk hot path.
/// The per-context accounting stays single-threaded (Cell-based, thread-
/// owned); this is the one cross-thread number: what all live contexts
/// together have committed from the heap. A sampler on any thread (the
/// memory watchdog) reads it against process RSS — the difference is the
/// untracked-allocation detector (heap estates outside mcx, allocator
/// retention, thread stacks) and, when the per-context ledgers drift from
/// reality, the accounting-drift detector.
///
/// Blocks parked in the keeper recycle pool are NOT counted (owned by the
/// pool, not by a live context); they surface in the RSS delta like any
/// other allocator retention.
pub mod global_footprint {
    use core::sync::atomic::{AtomicUsize, Ordering::Relaxed};

    static BYTES: AtomicUsize = AtomicUsize::new(0);

    #[inline]
    pub(crate) fn add(n: usize) {
        BYTES.fetch_add(n, Relaxed);
    }

    #[inline]
    pub(crate) fn sub(n: usize) {
        let prev = BYTES.fetch_sub(n, Relaxed);
        debug_assert!(prev >= n, "global footprint underflow: {prev} - {n}");
    }

    /// Block bytes currently committed to live contexts, process-wide,
    /// PLUS registered engine estates (below).
    pub fn bytes() -> usize {
        BYTES.load(Relaxed)
    }

    /// Registered ENGINE-ESTATE bytes (GL-CONCMEM-1): plain-Rust executor
    /// estates — the lane aggregation tables' chunked row stores, entry
    /// arrays and key arenas — charge their block bytes into the SAME
    /// process ledger, so the memory watchdog's accounted line and the
    /// GL-MEMCEIL-1 ceiling see engine memory, not only context blocks
    /// (the concurrent-window autopsy's 6.3GiB unaccounted delta was
    /// exactly this estate). Charge/uncharge at BLOCK grain only (growth
    /// events, clear, Drop) — never per row; an uncharge must never exceed
    /// its charge (the shared underflow debug-assert).
    pub fn charge_engine_estate(n: usize) {
        add(n);
    }

    pub fn uncharge_engine_estate(n: usize) {
        sub(n);
    }
}

/// Debug census of LIVE context nodes by name (FPBUDGET-1 instrumentation):
/// process-global and thread-safe (atomics only), so a sampler on any thread
/// sees contexts created — and never dropped — by dead session threads. OFF
/// unless the owning binary calls [`debug_census_enable`] (env-gated there);
/// when off the cost is one relaxed load per context create/destroy, both
/// cold paths.
pub mod debug_census {
    use core::sync::atomic::{AtomicBool, AtomicI64, AtomicPtr, AtomicUsize, Ordering::Relaxed};

    static ENABLED: AtomicBool = AtomicBool::new(false);
    const SLOTS: usize = 1024;
    // Open-addressed by name POINTER (each &'static str literal site is one
    // identity); snapshot merges equal strings from different sites.
    static NAME_PTR: [AtomicPtr<u8>; SLOTS] = [const { AtomicPtr::new(core::ptr::null_mut()) }; SLOTS];
    static NAME_LEN: [AtomicUsize; SLOTS] = [const { AtomicUsize::new(0) }; SLOTS];
    static LIVE: [AtomicI64; SLOTS] = [const { AtomicI64::new(0) }; SLOTS];

    pub fn enable() {
        ENABLED.store(true, Relaxed);
    }

    #[inline]
    pub fn on() -> bool {
        ENABLED.load(Relaxed)
    }

    fn slot(name: &'static str) -> Option<usize> {
        let p = name.as_ptr() as *mut u8;
        let mut i = (p as usize >> 3) % SLOTS;
        for _ in 0..SLOTS {
            let cur = NAME_PTR[i].load(Relaxed);
            if cur == p {
                return Some(i);
            }
            if cur.is_null() {
                match NAME_PTR[i].compare_exchange(core::ptr::null_mut(), p, Relaxed, Relaxed) {
                    Ok(_) => {
                        NAME_LEN[i].store(name.len(), Relaxed);
                        return Some(i);
                    }
                    Err(existing) if existing == p => return Some(i),
                    Err(_) => {}
                }
            }
            i = (i + 1) % SLOTS;
        }
        None // table full: drop the sample (debug instrument, never fail)
    }

    #[cold]
    pub(crate) fn created(name: &'static str) {
        if let Some(i) = slot(name) {
            LIVE[i].fetch_add(1, Relaxed);
        }
    }

    #[cold]
    pub(crate) fn dropped(name: &'static str) {
        if let Some(i) = slot(name) {
            LIVE[i].fetch_sub(1, Relaxed);
        }
    }

    /// (name, live-count) rows with nonzero counts, merged by string equality.
    pub fn snapshot() -> alloc::vec::Vec<(&'static str, i64)> {
        let mut rows: alloc::vec::Vec<(&'static str, i64)> = alloc::vec::Vec::new();
        for i in 0..SLOTS {
            let p = NAME_PTR[i].load(Relaxed);
            if p.is_null() {
                continue;
            }
            let n = LIVE[i].load(Relaxed);
            if n == 0 {
                continue;
            }
            // SAFETY: p/len came from one &'static str literal.
            let name: &'static str = unsafe {
                core::str::from_utf8_unchecked(core::slice::from_raw_parts(
                    p,
                    NAME_LEN[i].load(Relaxed),
                ))
            };
            match rows.iter_mut().find(|(rn, _)| *rn == name) {
                Some(r) => r.1 += n,
                None => rows.push((name, n)),
            }
        }
        rows.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        rows
    }
}

// ---------------------------------------------------------------------------
// Session-lifetime roots (FPBUDGET-1). C's process-per-backend model frees a
// backend's TopMemoryContext estate at process exit for free; the thread model
// must free it explicitly or every session leaks its whole cache estate into
// the shared process (measured ~2.2 MiB RSS per seed under the simharness
// DDL/session-churn campaign, C flat on the identical stream). The sink is a
// thread-local phased LIFO owned by mcxt_stats (std side); the backend runner
// drains it at clean task end. TLS payloads stay ManuallyDrop / !needs_drop —
// no hot-path TLS state machine is introduced; registration is once per root
// per thread (cold).
//
// PHASES (v2, the train-29 bounce fix). C's exit runs in a strict documented
// order: before_shmem_exit callbacks (ShutdownPostgres ->
// AbortOutOfAnyTransaction -> AtAbort_Portals/AtCleanup_Portals, "now safe to
// release portal memory") run FIRST, with every memory context still alive;
// memory itself dies LAST, all at once at process exit, with no per-object
// destructors at all. A single flat LIFO cannot express that: cleanup order
// then depends on lazy first-use registration order, and a cleanup that drops
// an object graph (a portal) can run after the context owning that graph's
// allocations was already freed — drop glue then deallocates through a dead
// arena, panics inside a destructor, and a panic in Drop is process-fatal in
// a threaded server (the t29 SIGABRT). The port of C's order:
//   Portals — object-graph teardown that must see EVERY context alive
//             (portal manager; C's AtCleanup_Portals slot).
//   State   — TLS state clears releasing global-heap Rc estates
//             (caches, scratch holders; C's exit-callback slot).
//   Roots   — session-root context frees, wholesale, no per-entry glue
//             (C's memory-dies-at-process-exit slot). Always last.
// Within a phase the order stays LIFO (C's callback discipline).
// ---------------------------------------------------------------------------

type SessionCleanup = alloc::boxed::Box<dyn FnOnce()>;

/// Teardown phase (drain order: Portals, then State, then Roots).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionCleanupPhase {
    /// Object-graph teardown needing every context alive (portal manager).
    Portals,
    /// TLS state clears (caches, scratch holders). The default.
    State,
    /// Session-root context frees; run only after all of the above.
    Roots,
}

static SESSION_CLEANUP_SINK: core::sync::atomic::AtomicPtr<()> =
    core::sync::atomic::AtomicPtr::new(core::ptr::null_mut());

/// Install the per-thread cleanup-list push (mcxt_stats owns the lists).
pub fn set_session_cleanup_sink(f: fn(SessionCleanupPhase, SessionCleanup)) {
    SESSION_CLEANUP_SINK.store(f as *mut (), core::sync::atomic::Ordering::Release);
}

/// Register a cleanup to run at this thread's session teardown (State phase,
/// LIFO within the phase). With no sink installed (unit tests, wasm
/// single-shot) the closure is dropped unrun — exactly the old
/// leak-at-thread-exit behavior.
pub fn register_session_cleanup(f: SessionCleanup) {
    register_session_cleanup_phase(SessionCleanupPhase::State, f);
}

/// [`register_session_cleanup`] with an explicit phase.
pub fn register_session_cleanup_phase(phase: SessionCleanupPhase, f: SessionCleanup) {
    let p = SESSION_CLEANUP_SINK.load(core::sync::atomic::Ordering::Acquire);
    if !p.is_null() {
        // SAFETY: only set_session_cleanup_sink stores here, always from
        // fn(SessionCleanupPhase, SessionCleanup).
        let sink: fn(SessionCleanupPhase, SessionCleanup) = unsafe { core::mem::transmute(p) };
        sink(phase, f);
    }
}

// Set while a `session_root*` context is being retired at teardown, so
// `reset_noncore` skips its mid-life exact-accounting leak-check for the
// final wholesale arena release (see `retire_session_root`). Per-thread
// wherever std is linked (the server graph enables `std`; unit tests link it
// too). A no_std consumer has no per-thread state (as the pools above) and
// shares one process-wide flag behind the same `.with(&Cell<bool>)` shape.
#[cfg(any(feature = "std", test))]
std::thread_local! {
    static SESSION_ROOT_RETIRING: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
}
#[cfg(not(any(feature = "std", test)))]
static SESSION_ROOT_RETIRING: RetiringFlag = RetiringFlag(core::sync::atomic::AtomicBool::new(false));
#[cfg(not(any(feature = "std", test)))]
struct RetiringFlag(core::sync::atomic::AtomicBool);
#[cfg(not(any(feature = "std", test)))]
impl RetiringFlag {
    fn with<R>(&self, f: impl FnOnce(&core::cell::Cell<bool>) -> R) -> R {
        use core::sync::atomic::Ordering::Relaxed;
        let cell = core::cell::Cell::new(self.0.load(Relaxed));
        let r = f(&cell);
        self.0.store(cell.get(), Relaxed);
        r
    }
}

/// Session-lifetime root context: the `Box::leak(Box::new(MemoryContext))`
/// shape (stable `&'static` handle, no TLS dtor state machine) plus a
/// registered teardown that reclaims the context's arena — and everything in
/// it, wholesale — when the session ends.
///
/// The shell struct itself is deliberately **never freed**: at teardown it is
/// reset (arena bulk released, destructors run) and poisoned, but left
/// allocated, so the handed-out `&'static` can never become dangling. Any
/// stray post-teardown *access* (alloc/reset/free through the retired context)
/// then fails closed with a deterministic panic in debug builds instead of
/// reading freed memory — see [`MemoryContext::check_live`]. The residual
/// leak is bounded (one shell + its keeper block per session root), matching
/// the crate's pre-existing "leak at thread exit" fallback.
pub fn session_root(name: &'static str) -> &'static MemoryContext {
    session_root_from(MemoryContext::new(name))
}

/// [`session_root`] for pre-built contexts (bump/limit variants).
pub fn session_root_from(ctx: MemoryContext) -> &'static MemoryContext {
    session_root_mut(ctx)
}

/// [`session_root_from`] with a `&'static mut` handle (reset-per-use
/// scratch holders). The cleanup closure holds only the address, never a
/// reference, so the exclusive borrow stays unique until teardown.
pub fn session_root_mut(ctx: MemoryContext) -> &'static mut MemoryContext {
    let raw: *mut MemoryContext = alloc::boxed::Box::into_raw(alloc::boxed::Box::new(ctx));
    let addr = raw as usize;
    // Roots phase: the arena dies only after every Portals/State cleanup has
    // dropped the object graphs whose allocations live in it (C: memory dies
    // at process exit, after all exit callbacks).
    register_session_cleanup_phase(
        SessionCleanupPhase::Roots,
        alloc::boxed::Box::new(move || {
            // SAFETY: `addr` is the leaked shell from `Box::into_raw` above.
            // The closure runs at most once, on the owning thread, at
            // teardown. We do NOT reconstruct+drop the Box: freeing the shell
            // would leave the handed-out `&'static` dangling. Instead we
            // retire it in place — release the arena bulk and poison the shell
            // — so the reference stays valid (poisoned) forever and any later
            // access fails closed via `check_live`.
            let shell = unsafe { &mut *(addr as *mut MemoryContext) };
            shell.retire_session_root();
        }),
    );
    // SAFETY: heap allocation; retired-but-never-freed by the closure above,
    // so the reference is valid for the process lifetime.
    unsafe { &mut *raw }
}

// Allocator-retention release hook (mimalloc mi_collect shape); installed by
// the binary that owns the global allocator, no-op when unset. Call sites are
// alloc-churn boundaries where freed-but-retained segments would otherwise
// hold RSS (hashagg spill batch resets).
static ALLOC_RELEASE: core::sync::atomic::AtomicPtr<()> =
    core::sync::atomic::AtomicPtr::new(core::ptr::null_mut());

pub fn set_allocator_release(f: fn()) {
    ALLOC_RELEASE.store(f as *mut (), core::sync::atomic::Ordering::Release);
}

/// True when an installed hook ran (false = no hook, nothing released).
#[cold]
#[inline(never)]
pub fn release_retained() -> bool {
    let p = ALLOC_RELEASE.load(core::sync::atomic::Ordering::Acquire);
    if !p.is_null() {
        // SAFETY: only set_allocator_release stores here, always from fn().
        let f: fn() = unsafe { core::mem::transmute(p) };
        f();
        true
    } else {
        false
    }
}

/// D3.4 idle passivation trim (docs/design/connection-scaling.md): return
/// this thread's retained allocator memory — the parked aset keeper blocks
/// (up to 100 × 8KiB of context-churn residue), the Acct-node and
/// children-vec pools — then run the allocator release hook (mi_collect) so
/// freed-but-retained segments leave RSS. Cold by contract: call only from an
/// idle backend; the pools refill lazily on the next query.
///
/// What this deliberately does NOT do: release free chunks inside live aset
/// contexts. Aset blocks are freed only at context reset/drop (aset.rs); a
/// mid-life block release would need per-chunk block back-pointers on the hot
/// dealloc path — rejected for a passivation-only win (the retained high-water
/// of the long-lived contexts is bounded and measured in the D3.4 notes).
#[cold]
#[inline(never)]
pub fn passivate_trim() -> bool {
    aset::trim_recycled_blocks();
    #[cfg(all(feature = "std", not(test)))]
    if local_pool_on() {
        let _ = tls_pools::ACCT.try_with(|s| s.drain_all());
        let _ = tls_pools::CHILD_VECS.try_with(|s| s.drain_all());
    }
    release_retained()
}

pub struct MemoryContext {
    acct: AcctRc,
    backend: Backend,
    reset_cbs: RefCell<alloc::vec::Vec<alloc::boxed::Box<dyn FnOnce()>>>,
    // C's isReset: cleared on every allocate/grow/register; reset() early-exits
    // on it (per-row ResetExprContext is 2 loads, not the arena walk).
    is_reset: Cell<bool>,
    // Retirement tripwire for `session_root*` shells: set true at session
    // teardown (see `session_root_mut`). The shell is deliberately kept alive
    // (never freed) after teardown so the handed-out `&'static` can never
    // dangle; this flag turns any post-teardown *access* into a deterministic,
    // fail-closed panic in debug builds instead of a silent read of a retired
    // context. Always false for ordinary (dropped) contexts. Read only under
    // `debug_assertions` (see `check_live`); maintained in every build.
    #[cfg_attr(not(debug_assertions), allow(dead_code))]
    poisoned: Cell<bool>,
}

impl MemoryContext {
    pub fn new(name: &'static str) -> Self {
        Self::with_backend(name, Backend::Aset(core::cell::UnsafeCell::new(aset::AllocSet::new())), None)
    }

    pub fn new_bump(name: &'static str) -> Self {
        Self::with_backend(name, Backend::Bump(new_arena()), None)
    }

    pub fn new_child(&self, name: &'static str) -> MemoryContext {
        Self::with_backend(
            name,
            Backend::Aset(core::cell::UnsafeCell::new(aset::AllocSet::new())),
            Some(self.acct.clone()),
        )
    }

    /// An AllocSet (individual frees work) whose reset is wholesale like C's
    /// AllocSetReset: live chunks are dropped, not reported as leaks.
    pub fn new_child_wholesale(&self, name: &'static str) -> MemoryContext {
        let ctx = self.new_child(name);
        ctx.acct.wholesale_reset.set(true);
        ctx
    }

    pub fn new_child_bump(&self, name: &'static str) -> MemoryContext {
        Self::with_backend(name, Backend::Bump(new_arena()), Some(self.acct.clone()))
    }

    pub fn new_child_bump_with_max_block_size(
        &self,
        name: &'static str,
        max_block_size: usize,
    ) -> MemoryContext {
        Self::with_backend(
            name,
            Backend::Bump(core::cell::UnsafeCell::new(bump::BumpArena::with_max_block_size(max_block_size))),
            Some(self.acct.clone()),
        )
    }

    pub fn new_bumpdrop(name: &'static str) -> Self {
        Self::with_backend(
            name,
            Backend::BumpDrop(new_arena(), RefCell::new(DropList::new())),
            None,
        )
    }

    pub fn new_child_bumpdrop(&self, name: &'static str) -> MemoryContext {
        Self::with_backend(
            name,
            Backend::BumpDrop(new_arena(), RefCell::new(DropList::new())),
            Some(self.acct.clone()),
        )
    }

    pub fn new_bumpforget(name: &'static str) -> Self {
        Self::with_backend(name, Backend::BumpForget(new_arena()), None)
    }

    pub fn new_child_bumpforget(&self, name: &'static str) -> MemoryContext {
        Self::with_backend(name, Backend::BumpForget(new_arena()), Some(self.acct.clone()))
    }

    pub fn new_generation(name: &'static str) -> Self {
        Self::with_backend(
            name,
            Backend::Generation(core::cell::UnsafeCell::new(generation::GenArena::new())),
            None,
        )
    }

    pub fn new_child_generation(&self, name: &'static str) -> MemoryContext {
        Self::with_backend(
            name,
            Backend::Generation(core::cell::UnsafeCell::new(generation::GenArena::new())),
            Some(self.acct.clone()),
        )
    }

    /// `block_size` must be a power of two >= 1024; every alloc must be exactly `chunk_size`.
    pub fn new_slab(name: &'static str, block_size: usize, chunk_size: usize) -> Self {
        Self::with_backend(
            name,
            Backend::Slab(core::cell::UnsafeCell::new(slab::SlabArena::new(
                block_size, chunk_size,
            ))),
            None,
        )
    }

    pub fn new_child_slab(
        &self,
        name: &'static str,
        block_size: usize,
        chunk_size: usize,
    ) -> MemoryContext {
        Self::with_backend(
            name,
            Backend::Slab(core::cell::UnsafeCell::new(slab::SlabArena::new(
                block_size, chunk_size,
            ))),
            Some(self.acct.clone()),
        )
    }

    fn with_backend(
        name: &'static str,
        backend: Backend,
        parent: Option<AcctRc>,
    ) -> Self {
        let (is_bump, kind, init_footprint, init_nblocks) = match &backend {
            Backend::Aset(a) => {
                // SAFETY: exclusive during construction.
                let a = unsafe { &*a.get() };
                // A recycled keeper (aset.c context_freelists) is a block already.
                (false, "AllocSet", a.footprint(), a.nblocks())
            }
            Backend::Malloc => (false, "Malloc", 0usize, 0usize),
            Backend::Bump(a) | Backend::BumpDrop(a, _) | Backend::BumpForget(a) => {
                // SAFETY: exclusive during construction.
                let a = unsafe { &*a.get() };
                (true, "Bump", a.footprint(), a.nblocks())
            }
            Backend::Generation(a) => {
                // SAFETY: exclusive during construction.
                let a = unsafe { &*a.get() };
                (true, "Generation", a.footprint(), a.nblocks())
            }
            Backend::Slab(a) => {
                // SAFETY: exclusive during construction.
                let a = unsafe { &*a.get() };
                (true, "Slab", a.footprint(), a.nblocks())
            }
        };
        let limited_path = parent.as_ref().is_some_and(|p| {
            p.limited_path.get() || p.limit.get() != usize::MAX
        });
        let acct = AcctRc::new(Acct {
            name: Cell::new(name),
            ident: RefCell::new(None),
            self_used: Cell::new(0),
            wholesale_reset: Cell::new(false),
            self_peak: Cell::new(0),
            limit: Cell::new(usize::MAX),
            limited_path: Cell::new(limited_path),
            arena_footprint: Cell::new(init_footprint),
            arena_nblocks: Cell::new(init_nblocks),
            window_tail: Cell::new(0),
            live_chunk_bytes: Cell::new(0),
            free_chunks: Cell::new(0),
            is_bump,
            kind,
            parent,
            children: RefCell::new(child_vec_take()),
        });
        if let Some(p) = &acct.parent {
            let mut children = p.children.borrow_mut();
            if children.len() == children.capacity() {
                children.retain(|w| w.strong_count() > 0);
            }
            children.push(acct.downgrade());
        } else {
            notify_root_observer(&acct);
        }
        if debug_census::on() {
            debug_census::created(name);
        }
        MemoryContext {
            acct,
            backend,
            reset_cbs: RefCell::new(alloc::vec::Vec::new()),
            is_reset: Cell::new(true),
            poisoned: Cell::new(false),
        }
    }

    /// Retire this context as part of `session_root*` teardown: free the arena
    /// bulk (reset frees blocks and runs destructors) and mark the shell
    /// poisoned so any later access fails closed. The shell itself is
    /// intentionally leaked by the caller so the handed-out `&'static` stays
    /// valid forever (poisoned, not freed) rather than dangling.
    fn retire_session_root(&mut self) {
        // Final teardown: release the arena wholesale, like C freeing
        // TopMemoryContext at proc_exit — which never asserts "no bytes still
        // charged". reset_noncore's exact-accounting leak-check targets
        // MID-LIFE resets (where surviving charged bytes signal a bug); a
        // session-root retirement legitimately discards transient charged data
        // (e.g. "PgStat Pending" holding unflushed pending stats, which C also
        // drops at exit). Suppress the leak-check for this path only, so the
        // RSS-reclaiming reset doesn't trip it. Restores the pre-retire
        // observable behavior (these roots were formerly leaked-at-exit, never
        // leak-checked) while keeping the arena reclaim.
        // Clear the flag on every exit path, including a panic unwinding out of
        // reset(), via a Drop guard — no catch_unwind (keeps the unwind policy
        // clean; a panicking reset still propagates unchanged).
        struct RetireFlagGuard;
        impl Drop for RetireFlagGuard {
            fn drop(&mut self) {
                SESSION_ROOT_RETIRING.with(|r| r.set(false));
            }
        }
        SESSION_ROOT_RETIRING.with(|r| r.set(true));
        let _guard = RetireFlagGuard;
        self.reset();
        self.poisoned.set(true);
    }

    /// Post-teardown tripwire. Ordinary contexts are never poisoned, so this is
    /// a no-op there. For a retired `session_root` shell it converts a
    /// use-after-teardown into a deterministic panic in debug builds instead of
    /// a silent access to a retired context. Kept out of release builds so the
    /// allocation hot path carries no extra load.
    #[inline(always)]
    fn check_live(&self) {
        #[cfg(debug_assertions)]
        if self.poisoned.get() {
            poisoned_access(self.acct.name.get());
        }
    }

    // upstream 3f3eefc28892 (18.4): Detect pfree or repalloc of a previously-freed memory chunk.
    /// C's MEMORY_CONTEXT_CHECKING test `chunk->requested_size == InvalidAllocSize`
    /// in AllocSetFree/Realloc, GenerationFree/Realloc and SlabFree (SlabRealloc
    /// never touches the chunk and has no test). The arenas keep the freed mark
    /// (aset/slab: `freed::FreedSet`, generation: zeroed chunk header); this is
    /// the single reporting point so the panic carries the context name like C's
    /// elog. Debug builds only: the release hot path is untouched.
    #[cfg(debug_assertions)]
    #[inline(always)]
    fn check_not_freed(&self, ptr: NonNull<u8>, layout: Layout, realloc: bool) {
        if layout.size() == 0 {
            return;
        }
        // SAFETY: shared read of arena bookkeeping; no &mut borrow is live (the
        // arenas are only ever borrowed for a single statement).
        let freed = unsafe {
            match &self.backend {
                Backend::Aset(set) => (*set.get()).is_freed(ptr, layout),
                Backend::Generation(a) => (*a.get()).is_freed(ptr),
                Backend::Slab(a) => !realloc && (*a.get()).is_freed(ptr),
                Backend::Malloc | Backend::Bump(_) | Backend::BumpDrop(..) | Backend::BumpForget(_) => {
                    false
                }
            }
        };
        if freed {
            freed::report(realloc, self.acct.name.get(), ptr);
        }
    }

    /// Contract: set the limit before creating children (limited_path cache).
    pub fn with_limit(self, limit: usize) -> Self {
        debug_assert!(
            self.acct.children.borrow().iter().all(|w| w.strong_count() == 0),
            "with_limit must be set before creating children (limited_path cache would go stale)",
        );
        if limit != usize::MAX {
            self.acct.limited_path.set(true);
        }
        self.acct.limit.set(limit);
        self
    }

    pub fn mcx(&self) -> Mcx<'_> {
        Mcx(self)
    }

    pub fn name(&self) -> &'static str {
        self.acct.name.get()
    }

    /// C's AllocSetContextCreate freelist reuse overwrites context->name.
    pub fn set_name(&self, name: &'static str) {
        self.acct.name.set(name);
    }

    pub fn set_ident(&self, id: Option<&str>) {
        *self.acct.ident.borrow_mut() = id.map(alloc::string::String::from);
    }

    pub fn ident(&self) -> Option<alloc::string::String> {
        self.acct.ident.borrow().clone()
    }

    pub fn used(&self) -> usize {
        self.acct.self_used.get()
    }

    pub fn subtree_used(&self) -> usize {
        self.acct.subtree_sum()
    }

    /// Retained arena blocks, recursively; malloc contexts contribute live requested bytes.
    pub fn subtree_allocated(&self) -> usize {
        self.acct.subtree_allocated_sum()
    }

    pub fn peak(&self) -> usize {
        self.acct.self_peak.get()
    }

    pub fn subtree_peak(&self) -> usize {
        self.acct.subtree_peak_sum()
    }

    pub fn limit(&self) -> usize {
        self.acct.limit.get()
    }

    pub fn register_reset_callback(&self, cb: impl FnOnce() + 'static) {
        self.is_reset.set(false);
        self.reset_cbs.borrow_mut().push(alloc::boxed::Box::new(cb));
    }

    // Bump re-opens the keeper's window at reset (BumpArena::reset), so the
    // keeper stays charged (C's mem_allocated shape). Per-tuple/per-query
    // resets ride the plain-Bump arm; every other backend is out of line.
    #[inline]
    pub fn reset(&mut self) {
        self.check_live();
        if *self.is_reset.get_mut() {
            return;
        }
        self.reset_dirty();
    }

    // is_reset flips only after the work: a panicking destructor unwinds with
    // the flag still false, so the retried reset re-drains (test-pinned).
    #[inline(never)]
    fn reset_dirty(&mut self) {
        if !self.reset_cbs.get_mut().is_empty() {
            self.fire_reset_callbacks();
        }
        if let Backend::Bump(a) | Backend::BumpForget(a) = &mut self.backend {
            let a = a.get_mut();
            a.reset();
            let footprint = a.footprint();
            let acct = &*self.acct;
            acct.self_used.set(footprint);
            acct.self_peak.set(footprint);
            acct.arena_footprint.set(footprint);
            acct.arena_nblocks.set(a.nblocks());
            acct.window_tail.set(a.window_tail());
            self.is_reset.set(true);
            return;
        }
        self.reset_noncore();
        self.is_reset.set(true);
    }

    #[cold]
    #[inline(never)]
    fn reset_noncore(&mut self) {
        // Leak check only for exact-accounting backends (bump charges release
        // wholesale here), and never during a session-root retirement — that is
        // a final wholesale teardown (C frees TopMemoryContext at proc_exit
        // without such a check), where transient charged data is discarded by
        // design, not leaked.
        if !self.acct.is_bump
            && !self.acct.wholesale_reset.get()
            && !SESSION_ROOT_RETIRING.with(core::cell::Cell::get)
        {
            debug_assert_eq!(
                self.acct.self_used.get(),
                0,
                "context {:?} reset with {} bytes still charged (leaked allocation?)",
                self.acct.name.get(),
                self.acct.self_used.get(),
            );
        }
        let acct = &*self.acct;
        match &mut self.backend {
            Backend::Aset(set) => {
                // aset.c:537-597 AllocSetReset: every chunk is released
                // whether or not it was pfree'd, so nothing stays charged
                // (a stale charge here outlived the arena it accounted for);
                // the arena's reset re-snapshots the block grain (the keeper
                // block stays, alone and free).
                set.get_mut().reset(acct);
                acct.self_used.set(0);
                acct.self_peak.set(0);
            }
            Backend::Malloc => {
                acct.self_used.set(0);
                acct.arena_footprint.set(0);
                acct.arena_nblocks.set(0);
                acct.self_peak.set(0);
            }
            Backend::Bump(_) | Backend::BumpForget(_) => unreachable!("handled in reset()"),
            Backend::BumpDrop(a, droplist) => {
                // Run destructors BEFORE the bytes are reclaimed (order load-bearing).
                // Pop each entry under a fresh scoped borrow and run it with the
                // exclusive borrow released, so a re-entrant callback cannot alias
                // the DropList and a panicking destructor leaves the remainder for
                // the next reset (never double-run).
                run_drop_glue(|| droplist.get_mut().pop());
                let a = a.get_mut();
                a.reset();
                let footprint = a.footprint();
                acct.self_used.set(footprint);
                acct.self_peak.set(footprint);
                acct.arena_footprint.set(footprint);
                acct.arena_nblocks.set(a.nblocks());
                acct.window_tail.set(a.window_tail());
            }
            Backend::Generation(a) => {
                let a = a.get_mut();
                a.reset();
                acct.self_used.set(0);
                acct.self_peak.set(0);
                acct.arena_footprint.set(a.footprint());
                acct.arena_nblocks.set(a.nblocks());
            }
            Backend::Slab(a) => {
                let a = a.get_mut();
                a.reset();
                acct.self_used.set(0);
                acct.self_peak.set(0);
                acct.arena_footprint.set(a.footprint());
                acct.arena_nblocks.set(a.nblocks());
            }
        }
    }

    pub fn stats(&self) -> ContextStats {
        ContextStats {
            name: self.acct.name.get(),
            ident: self.ident(),
            used: self.acct.self_used.get(),
            peak: self.acct.self_peak.get(),
            subtree_used: self.acct.subtree_sum(),
            subtree_peak: self.acct.subtree_peak_sum(),
            limit: self.acct.limit.get(),
            arena_footprint: match &self.backend {
                Backend::Aset(_) => self.acct.arena_footprint.get(),
                Backend::Malloc => self.acct.self_used.get(),
                Backend::Bump(a) | Backend::BumpDrop(a, _) | Backend::BumpForget(a) => {
                    // SAFETY: single-statement borrow, never re-entered (as aset_mut).
                    unsafe { &*a.get() }.footprint()
                }
                // SAFETY: as above.
                Backend::Generation(a) => unsafe { &*a.get() }.footprint(),
                // SAFETY: as above.
                Backend::Slab(a) => unsafe { &*a.get() }.footprint(),
            },
            nblocks: self.acct.arena_nblocks.get(),
            free_bytes: self.acct.free_bytes(),
            free_chunks: self.acct.free_chunks.get(),
        }
    }

    pub fn stats_tree(&self) -> TreeStats {
        tree_stats_node(&self.acct)
    }

    #[cold]
    pub fn oom(&self, request: usize) -> PgError {
        crate::oom_named(self.acct.name.get(), request)
    }

    pub fn is_bumpforget(&self) -> bool {
        matches!(self.backend, Backend::BumpForget(..))
    }

    /// # Safety: `addr` — live `T` (glue's type) in this arena, Drop suppressed, valid until reset.
    unsafe fn register_drop(&self, addr: *mut u8, glue: unsafe fn(*mut u8)) -> bool {
        if let Backend::BumpDrop(_, droplist) = &self.backend {
            self.is_reset.set(false);
            droplist.borrow_mut().entries.push(DropEntry { addr, glue });
            true
        } else {
            false
        }
    }

    fn fire_reset_callbacks(&self) {
        loop {
            let cb = self.reset_cbs.borrow_mut().pop();
            match cb {
                Some(cb) => cb(),
                None => break,
            }
        }
    }

    // Single-node charge, no ancestor walk (as C); the limit walk validates first.
    #[inline]
    fn charge(&self, n: usize) -> Result<(), AllocError> {
        let acct = &*self.acct;
        acct.check_limit(n)?;
        let self_new = acct.self_used.get() + n;
        acct.self_used.set(self_new);
        if self_new > acct.self_peak.get() {
            acct.self_peak.set(self_new);
        }
        Ok(())
    }

    fn uncharge(&self, n: usize) {
        let acct = &*self.acct;
        debug_assert!(
            acct.self_used.get() >= n,
            "context {:?} uncharging {} with only {} charged",
            acct.name.get(),
            n,
            acct.self_used.get(),
        );
        acct.self_used.set(acct.self_used.get().saturating_sub(n));
    }
}

// GL-MEMWATCH-1: C parity for aset.c's MemoryContextStats(TopMemoryContext)
// on allocation failure — the installed observer (mcxt_stats) dumps the
// failing thread's context forest before the error propagates. Set-once at
// boot, fn-pointer seam (root-observer pattern above).
static OOM_OBSERVER: core::sync::atomic::AtomicPtr<()> =
    core::sync::atomic::AtomicPtr::new(core::ptr::null_mut());

pub fn set_oom_observer(f: fn(context_name: &str, request: usize)) {
    OOM_OBSERVER.store(f as *mut (), core::sync::atomic::Ordering::Release);
}

#[cold]
pub fn oom_named(context_name: &str, request: usize) -> PgError {
    let p = OOM_OBSERVER.load(core::sync::atomic::Ordering::Acquire);
    if !p.is_null() {
        // SAFETY: only set_oom_observer stores here, always from the fn type.
        let f: fn(&str, usize) = unsafe { core::mem::transmute(p) };
        f(context_name, request);
    }
    PgError::error("out of memory")
        .with_sqlstate(ERRCODE_OUT_OF_MEMORY)
        .with_detail(alloc::format!(
            "Failed on request of size {request} in memory context \"{context_name}\"."
        ))
}

#[cfg(debug_assertions)]
#[cold]
#[inline(never)]
fn poisoned_access(name: &str) -> ! {
    panic!(
        "mcx: access to session-root context {name:?} after session teardown \
         (use-after-teardown)"
    );
}

impl Drop for MemoryContext {
    fn drop(&mut self) {
        self.fire_reset_callbacks();
        if let Backend::BumpDrop(_, droplist) = &self.backend {
            // Pop each entry under a short scoped borrow, dropping the borrow_mut
            // guard before running any user drop glue, so a re-entrant
            // register_drop/reset from a destructor cannot double-borrow or alias
            // the DropList, and a panicking destructor leaves the remainder for a
            // subsequent drop (never double-run).
            run_drop_glue(|| droplist.borrow_mut().pop());
        }
        self.acct.ident.borrow_mut().take();
        self.acct.self_used.set(0);
        self.acct.window_tail.set(0);
        // The arena goes with the context; a node kept alive by its children's
        // parent links reports no blocks from here on.
        self.acct.set_blocks(0, 0);
        self.acct.live_chunk_bytes.set(0);
        self.acct.free_chunks.set(0);
    }
}

impl fmt::Debug for MemoryContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryContext")
            .field("name", &self.acct.name.get())
            .field("used", &self.acct.self_used.get())
            .field("subtree_used", &self.acct.subtree_sum())
            .field("peak", &self.acct.self_peak.get())
            .field("limit", &self.acct.limit.get())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextStats {
    pub name: &'static str,
    pub ident: Option<alloc::string::String>,
    pub used: usize,
    pub peak: usize,
    pub subtree_used: usize,
    pub subtree_peak: usize,
    pub limit: usize,
    pub arena_footprint: usize,
    // The allocator's own stats figures (aset.c:1545 AllocSetStats for an
    // AllocSet: blocks on set->blocks, block tails + freelist chunks, freelist
    // population); bump backends report the window-tail snapshot as free
    // bytes and no chunks.
    pub nblocks: usize,
    pub free_bytes: usize,
    pub free_chunks: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeStats {
    pub name: &'static str,
    pub ident: Option<alloc::string::String>,
    pub kind: &'static str,
    pub used: usize,
    pub peak: usize,
    pub subtree_used: usize,
    pub subtree_peak: usize,
    pub limit: usize,
    pub is_bump: bool,
    // Block bytes; AllocSet: aset.c:1545 AllocSetStats' totalspace less the
    // context header (mem_allocated).
    pub arena_footprint: usize,
    pub nblocks: usize,
    // AllocSet: block tails + freelist chunks / freelist population
    // (AllocSetStats freespace / freechunks). Bump backends: the
    // block-transition window-tail snapshot (see Acct::window_tail), 0 chunks.
    pub free_bytes: usize,
    pub free_chunks: usize,
    pub children: alloc::vec::Vec<TreeStats>,
}

fn tree_stats_node(acct: &Acct) -> TreeStats {
    let mut children = alloc::vec::Vec::new();
    acct.children.borrow_mut().retain(|w| match w.upgrade() {
        Some(c) => {
            children.push(tree_stats_node(&c));
            true
        }
        None => false,
    });
    let used = acct.self_used.get();
    let peak = acct.self_peak.get();
    let mut subtree_used = used;
    let mut subtree_peak = peak;
    for child in &children {
        subtree_used = subtree_used.saturating_add(child.subtree_used);
        subtree_peak = subtree_peak.saturating_add(child.subtree_peak);
    }
    TreeStats {
        name: acct.name.get(),
        ident: acct.ident.borrow().clone(),
        kind: acct.kind,
        used,
        peak,
        subtree_used,
        subtree_peak,
        limit: acct.limit.get(),
        is_bump: acct.is_bump,
        arena_footprint: acct.arena_footprint.get(),
        nblocks: acct.arena_nblocks.get(),
        free_bytes: acct.free_bytes(),
        free_chunks: acct.free_chunks.get(),
        children,
    }
}

/// Copyable allocator handle tying every allocation to the context lifetime.
#[doc = "An allocation cannot outlive its context:"]
#[doc = "```compile_fail,E0597"]
#[doc = "let v;"]
#[doc = "{"]
#[doc = "    let ctx = mcx::MemoryContext::new(\"short-lived\");"]
#[doc = "    v = mcx::PgVec::<u8>::new_in(ctx.mcx());"]
#[doc = "} // `ctx` dropped here while `v` still borrows it"]
#[doc = "assert_eq!(v.len(), 0);"]
#[doc = "```"]
#[doc = "A reset is statically impossible while allocations are live:"]
#[doc = "```compile_fail,E0502"]
#[doc = "let mut ctx = mcx::MemoryContext::new_bump(\"per-tuple\");"]
#[doc = "let v = mcx::PgVec::<u8>::new_in(ctx.mcx());"]
#[doc = "ctx.reset(); // ERROR: `v` still borrows `ctx`"]
#[doc = "assert_eq!(v.len(), 0);"]
#[doc = "```"]
#[derive(Clone, Copy)]
pub struct Mcx<'mcx>(&'mcx MemoryContext);

impl<'mcx> Mcx<'mcx> {
    pub fn context(self) -> &'mcx MemoryContext {
        self.0
    }

    #[cold]
    pub fn oom(self, request: usize) -> PgError {
        self.0.oom(request)
    }

    // De-mono shell: backend dispatch emitted ONCE out of line, not inlined per
    // generic T. Hot per-tuple allocs keep the inline `Allocator::allocate` lane.
    #[inline(never)]
    pub fn alloc_uninit_bytes(self, layout: Layout) -> Result<NonNull<u8>, AllocError> {
        Allocator::allocate(&self, layout).map(|p| p.cast::<u8>())
    }
}

impl fmt::Debug for Mcx<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Mcx({:?})", self.0.acct.name.get())
    }
}

/// C mcxt.c: `palloc` rejects any request above `MaxAllocSize` before the
/// context sees it ("invalid memory alloc request size"). Out-of-line so the
/// allocate/grow fast paths carry only a compare + never-taken branch.
// mcxt.c MemoryContextSizeFailure: elog(ERROR), recovered at the statement
// boundary. Returning AllocError here would leave every infallible lane
// (PgVec::push, extend, resize) to handle_alloc_error — an abort of the whole
// server — where C raises a catchable error.
#[cold]
#[inline(never)]
fn alloc_ceiling_exceeded(size: usize) -> ! {
    panic!("invalid memory alloc request size {size}")
}

// SAFETY contract for callers: one-statement &mut, never re-entered; one context, one thread.
#[inline(always)]
unsafe fn aset_mut(set: &core::cell::UnsafeCell<aset::AllocSet>) -> &mut aset::AllocSet {
    &mut *set.get()
}

// SAFETY contract: as aset_mut.
#[inline(always)]
unsafe fn bump_mut(a: &core::cell::UnsafeCell<bump::BumpArena>) -> &mut bump::BumpArena {
    &mut *a.get()
}

#[inline]
fn new_arena() -> core::cell::UnsafeCell<bump::BumpArena> {
    core::cell::UnsafeCell::new(bump::BumpArena::new())
}

// SAFETY (trait contract): delegates to aset/Global/bump; accounting is undone on failure.
unsafe impl Allocator for Mcx<'_> {
    // always-inline: out-of-line, the fast lanes grow a fat frame + call (measured +12 instr/op).
    #[inline(always)]
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        // C palloc's MaxAllocSize admission, enforced by the allocator so it
        // holds no matter which helper (or bare PgVec growth) the caller used.
        // Huge (>1GB) requests must opt out via `alloc_uninit_bytes_huge` /
        // the `*_huge` helpers, mirroring C's palloc vs palloc_extended(HUGE).
        if layout.size() > MAX_ALLOC_SIZE {
            alloc_ceiling_exceeded(layout.size());
        }
        self.allocate_unchecked(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        self.0.check_live();
        #[cfg(debug_assertions)]
        self.0.check_not_freed(ptr, layout, false);
        match &self.0.backend {
            Backend::Aset(set) => {
                self.0.uncharge(layout.size());
                #[cfg(test)]
                crate::churn_probe::bump();
                // SAFETY: single-statement borrow, never re-entered (aset_mut).
                unsafe { aset_mut(set) }.dealloc(ptr, layout, &self.0.acct)
            }
            Backend::Malloc => {
                self.0.uncharge(layout.size());
                #[cfg(test)]
                crate::churn_probe::bump();
                Global.deallocate(ptr, layout)
            }
            // bump.c: no BumpFree — bytes and charge release wholesale at reset.
            Backend::Bump(_) | Backend::BumpDrop(..) | Backend::BumpForget(_) => {}
            // SAFETY: single-statement borrow (as bump_mut); ptr/layout per trait contract.
            Backend::Generation(a) => unsafe {
                (*a.get()).dealloc(ptr, layout, &self.0.acct)
            },
            // SAFETY: as above.
            Backend::Slab(a) => unsafe { (*a.get()).dealloc(ptr, layout, &self.0.acct) },
        }
    }

    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        // C repalloc's MaxAllocSize admission: unbounded growth (PgVec::push,
        // try_reserve) reallocates through here, so the ceiling must hold on
        // grow as well as allocate. Huge growth opts out via vec_reserve_huge.
        if new_layout.size() > MAX_ALLOC_SIZE {
            alloc_ceiling_exceeded(new_layout.size());
        }
        self.0.check_live();
        #[cfg(debug_assertions)]
        self.0.check_not_freed(ptr, old_layout, true);
        self.0.is_reset.set(false);
        match &self.0.backend {
            Backend::Aset(set) => {
                let delta = new_layout.size() - old_layout.size();
                self.0.charge(delta)?;
                // SAFETY: single-statement borrow, never re-entered (aset_mut).
                let result =
                    unsafe { aset_mut(set) }.realloc(ptr, old_layout, new_layout, &self.0.acct);
                if result.is_err() {
                    self.0.uncharge(delta);
                }
                result
            }
            Backend::Malloc => {
                let delta = new_layout.size() - old_layout.size();
                self.0.charge(delta)?;
                let result = Global.grow(ptr, old_layout, new_layout);
                if result.is_err() {
                    self.0.uncharge(delta);
                }
                result
            }
            Backend::Bump(a) | Backend::BumpDrop(a, _) | Backend::BumpForget(a) => {
                // SAFETY: single-statement borrow (bump_mut); ptr/layouts per trait contract.
                unsafe { bump_mut(a).grow(ptr, old_layout, new_layout, &self.0.acct) }
            }
            // SAFETY: single-statement borrow (as bump_mut); ptr/layouts per trait contract.
            Backend::Generation(a) => unsafe {
                (*a.get()).grow(ptr, old_layout, new_layout, &self.0.acct)
            },
            // slab.c: realloc only tolerates the identical chunk size.
            Backend::Slab(_) => {
                if old_layout.size() == new_layout.size()
                    && ptr.as_ptr() as usize % new_layout.align() == 0
                {
                    Ok(NonNull::slice_from_raw_parts(ptr, new_layout.size()))
                } else {
                    Err(AllocError)
                }
            }
        }
    }

    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        self.0.check_live();
        #[cfg(debug_assertions)]
        self.0.check_not_freed(ptr, old_layout, true);
        match &self.0.backend {
            Backend::Aset(set) => {
                // SAFETY: single-statement borrow, never re-entered (aset_mut).
                let result =
                    unsafe { aset_mut(set) }.realloc(ptr, old_layout, new_layout, &self.0.acct);
                if result.is_ok() {
                    self.0.uncharge(old_layout.size() - new_layout.size());
                }
                result
            }
            Backend::Malloc => {
                let result = Global.shrink(ptr, old_layout, new_layout);
                if result.is_ok() {
                    self.0.uncharge(old_layout.size() - new_layout.size());
                }
                result
            }
            // GenerationRealloc: a shrink stays in place (oldsize >= size branch).
            Backend::Generation(a) => {
                if ptr.as_ptr() as usize % new_layout.align() == 0 {
                    return Ok(NonNull::slice_from_raw_parts(ptr, new_layout.size()));
                }
                // SAFETY: single-statement borrow (as bump_mut); copy fits new_layout.
                unsafe {
                    let arena = &mut *a.get();
                    let new = arena.alloc(new_layout, &self.0.acct)?;
                    core::ptr::copy_nonoverlapping(
                        ptr.as_ptr(),
                        new.cast::<u8>().as_ptr(),
                        new_layout.size(),
                    );
                    arena.dealloc(ptr, old_layout, &self.0.acct);
                    Ok(new)
                }
            }
            Backend::Slab(_) => {
                if old_layout.size() == new_layout.size()
                    && ptr.as_ptr() as usize % new_layout.align() == 0
                {
                    Ok(NonNull::slice_from_raw_parts(ptr, new_layout.size()))
                } else {
                    Err(AllocError)
                }
            }
            // bump.c model: narrow in place, nothing to uncharge.
            Backend::Bump(a) | Backend::BumpDrop(a, _) | Backend::BumpForget(a) => {
                if ptr.as_ptr() as usize % new_layout.align() == 0 {
                    return Ok(NonNull::slice_from_raw_parts(ptr, new_layout.size()));
                }
                // SAFETY: single-statement borrow (bump_mut); copy fits new_layout.
                unsafe {
                    let new = bump_mut(a).alloc(new_layout, &self.0.acct)?;
                    core::ptr::copy_nonoverlapping(
                        ptr.as_ptr(),
                        new.cast::<u8>().as_ptr(),
                        new_layout.size(),
                    );
                    Ok(new)
                }
            }
        }
    }
    // `shrink` needs no ceiling: it never increases the request, and a
    // legitimately-huge allocation (hash bucket arrays) may shrink to a size
    // that is still above MaxAllocSize.
}

// Ceiling-exempt entry points. A separate inherent block placed AFTER the
// Allocator impl on purpose: pulling the backend dispatch out of the trait
// impl itself would strand deallocate/grow/shrink (they pattern-match the
// same backends), so the trait keeps its methods and only delegates here.
impl Mcx<'_> {
    /// The backend dispatch `Allocator::allocate` runs after its MaxAllocSize
    /// admission. Private: huge callers go through
    /// [`Mcx::alloc_uninit_bytes_huge`], which admits against
    /// `MAX_ALLOC_HUGE_SIZE` instead.
    #[inline(always)]
    fn allocate_unchecked(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.0.check_live();
        self.0.is_reset.set(false);
        match &self.0.backend {
            Backend::Aset(set) => {
                self.0.charge(layout.size())?;
                // SAFETY: single-statement borrow, never re-entered (aset_mut).
                let result = unsafe { aset_mut(set) }.alloc(layout, &self.0.acct);
                if result.is_err() {
                    self.0.uncharge(layout.size());
                }
                result
            }
            Backend::Malloc => {
                self.0.charge(layout.size())?;
                let result = Global.allocate(layout);
                if result.is_err() {
                    self.0.uncharge(layout.size());
                }
                result
            }
            Backend::Bump(a) | Backend::BumpDrop(a, _) | Backend::BumpForget(a) => {
                // SAFETY: single-statement borrow, never re-entered (bump_mut).
                unsafe { bump_mut(a) }.alloc(layout, &self.0.acct)
            }
            Backend::Generation(a) => {
                // SAFETY: single-statement borrow, never re-entered (as bump_mut).
                unsafe { &mut *a.get() }.alloc(layout, &self.0.acct)
            }
            Backend::Slab(a) => {
                // SAFETY: single-statement borrow, never re-entered (as bump_mut).
                unsafe { &mut *a.get() }.alloc(layout, &self.0.acct)
            }
        }
    }

    /// C `MemoryContextAllocExtended(.., MCXT_ALLOC_HUGE)`: the explicit
    /// huge-allocation opt-out, admitted against `MAX_ALLOC_HUGE_SIZE`
    /// (SIZE_MAX/2) instead of the 1GB `MAX_ALLOC_SIZE`.
    #[inline(never)]
    pub fn alloc_uninit_bytes_huge(self, layout: Layout) -> Result<NonNull<u8>, AllocError> {
        if layout.size() > MAX_ALLOC_HUGE_SIZE {
            alloc_ceiling_exceeded(layout.size());
        }
        self.allocate_unchecked(layout).map(|p| p.cast::<u8>())
    }
}

pub const MAX_ALLOC_SIZE: usize = 0x3FFF_FFFF;

/// C `MaxAllocHugeSize` (memutils.h): the ceiling for `MCXT_ALLOC_HUGE`
/// requests (`MemoryContextAllocExtended`, `repalloc_huge`, simplehash's
/// `SH_ALLOCATE`).
pub const MAX_ALLOC_HUGE_SIZE: usize = usize::MAX / 2;

#[cold]
fn invalid_alloc_size(request: usize) -> alloc::boxed::Box<PgError> {
    PgError::error(alloc::format!("invalid memory alloc request size {request}")).into()
}

#[inline]
pub fn check_alloc_size(request: usize) -> PgResult<()> {
    if request > MAX_ALLOC_SIZE {
        return Err(invalid_alloc_size(request));
    }
    Ok(())
}

/// C mcxt.c:1684 `add_size()`: overflow-checked `s1 + s2`
/// (ERRCODE_PROGRAM_LIMIT_EXCEEDED, mcxt.c:1694 add_size_error).
#[inline]
pub fn add_size(s1: usize, s2: usize) -> PgResult<usize> {
    match s1.checked_add(s2) {
        Some(result) => Ok(result),
        None => Err(add_size_error(s1, s2)),
    }
}

#[cold]
fn add_size_error(s1: usize, s2: usize) -> alloc::boxed::Box<PgError> {
    PgError::error(alloc::format!("invalid memory allocation request size {s1} + {s2}"))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .into()
}

/// C mcxt.c `mul_size()`: the overflow-checked `count * sizeof(type)` that
/// `palloc_array()` / `palloc_mul()` size through (ERRCODE_PROGRAM_LIMIT_EXCEEDED).
// upstream e1c30458a10f (18.4): Make palloc_array() and friends safe against integer overflow.
#[inline]
pub fn mul_size(s1: usize, s2: usize) -> PgResult<usize> {
    match s1.checked_mul(s2) {
        Some(result) => Ok(result),
        None => Err(mul_size_error(s1, s2)),
    }
}

#[cold]
fn mul_size_error(s1: usize, s2: usize) -> alloc::boxed::Box<PgError> {
    PgError::error(alloc::format!("invalid memory allocation request size {s1} * {s2}"))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .into()
}

/// Droppy `T` allowed: the returned box runs `Drop`.
#[inline]
pub fn alloc_in<'mcx, T>(mcx: Mcx<'mcx>, value: T) -> PgResult<PgBox<'mcx, T>> {
    check_alloc_size(core::mem::size_of::<T>())?;
    PgBox::try_new_in(value, mcx)
        .map_err(|_| mcx.oom(core::mem::size_of::<T>()).into())
}

pub fn alloc_leak_in<'mcx, T>(mcx: Mcx<'mcx>, value: T) -> PgResult<&'mcx T> {
    const { assert!(!core::mem::needs_drop::<T>()) };
    Ok(&*leak_in(alloc_in(mcx, value)?))
}

/// Leak into an honest `&'mcx mut T`; `Drop` never runs, hence the gate.
pub fn leak_in<'mcx, T>(b: PgBox<'mcx, T>) -> &'mcx mut T {
    const { assert!(!core::mem::needs_drop::<T>()) };
    PgBox::leak(b)
}

/// BumpDrop leak: registers the destructor; droppy `T` is the point.
pub fn arena_leak<'mcx, T>(b: PgBox<'mcx, T>) -> &'mcx mut T {
    // Register the raw pointer, then return a CHILD retag (a sibling would be invalidated: the SB trap).
    let (raw, alloc): (*mut T, Mcx<'mcx>) =
        allocator_api2::boxed::Box::into_raw_with_allocator(b);
    let addr = core::ptr::with_exposed_provenance_mut::<u8>(
        (raw as *mut u8).expose_provenance(),
    );
    // SAFETY: live T unboxed into `alloc`'s arena, Drop suppressed; glue is the unique destructor.
    let registered = unsafe { alloc.context().register_drop(addr, drop_glue::<T>) };
    debug_assert!(
        registered || !core::mem::needs_drop::<T>(),
        "arena_leak: value of a Drop type leaked into a non-BumpDrop context \
         (its destructor will never run); use a BumpDrop context",
    );
    // SAFETY: sole &'mcx mut (box consumed); child retag keeps the stored copy valid.
    unsafe { &mut *raw }
}

pub fn arena_box_in<'mcx, T>(mcx: Mcx<'mcx>, value: T) -> PgResult<&'mcx mut T> {
    check_alloc_size(core::mem::size_of::<T>())?;
    let b = PgBox::try_new_in(value, mcx)
        .map_err(|_| mcx.oom(core::mem::size_of::<T>()))?;
    Ok(arena_leak(b))
}

/// BumpDrop vec leak with element-only glue (the arena owns the buffer bytes).
pub fn arena_vec_in<'mcx, T>(
    mcx: Mcx<'mcx>,
    vec: PgVec<'mcx, T>,
) -> PgResult<&'mcx mut PgVec<'mcx, T>> {
    check_alloc_size(core::mem::size_of::<PgVec<'mcx, T>>())?;
    let b = PgBox::try_new_in(vec, mcx)
        .map_err(|_| mcx.oom(core::mem::size_of::<PgVec<'mcx, T>>()))?;
    let (raw, alloc): (*mut PgVec<'mcx, T>, Mcx<'mcx>) =
        allocator_api2::boxed::Box::into_raw_with_allocator(b);
    let addr =
        core::ptr::with_exposed_provenance_mut::<u8>((raw as *mut u8).expose_provenance());
    // SAFETY: live header leaked into `alloc`'s arena; element-only glue is the unique destructor.
    let registered = unsafe { alloc.context().register_drop(addr, drop_glue_vec_elems::<T>) };
    debug_assert!(
        registered || !core::mem::needs_drop::<T>(),
        "arena_vec_in: Vec of a Drop element type leaked into a non-BumpDrop \
         context (element destructors will never run); use a BumpDrop context",
    );
    // SAFETY: sole &'mcx mut to the live header; child retag of `raw`.
    Ok(unsafe { &mut *raw })
}

/// BumpDrop string leak (POD bytes; no-op glue only arms the context guard).
pub fn arena_string_in<'mcx>(
    mcx: Mcx<'mcx>,
    s: PgString<'mcx>,
) -> PgResult<&'mcx mut PgString<'mcx>> {
    let b = alloc_in(mcx, s)?;
    let raw: *mut PgString<'mcx> =
        allocator_api2::boxed::Box::into_raw_with_allocator(b).0;
    let addr =
        core::ptr::with_exposed_provenance_mut::<u8>((raw as *mut u8).expose_provenance());
    // SAFETY: PgString's only Drop is its POD-element Vec<u8>'s.
    let registered = unsafe { mcx.context().register_drop(addr, drop_glue_noop) };
    debug_assert!(registered, "arena_string_in: use a BumpDrop context");
    // SAFETY: sole &'mcx mut to the live header; child retag of `raw`.
    Ok(unsafe { &mut *raw })
}

/// Leak into `&'mcx T` with drop glue never run (C: objects die with the
/// context, zero per-object teardown); `ForgetSafe` bounds the loss to arena
/// bytes, so any arena backend qualifies.
pub fn forget_box_in<'mcx, T: ForgetSafe>(mcx: Mcx<'mcx>, value: T) -> PgResult<&'mcx mut T> {
    Ok(PgBox::leak(alloc_in(mcx, value)?))
}

pub fn arena_box_in_forget<'mcx, T: ArenaSafe>(
    mcx: Mcx<'mcx>,
    value: T,
) -> PgResult<&'mcx mut T> {
    const { assert!(!core::mem::needs_drop::<T>()) };
    debug_assert!(
        mcx.context().is_bumpforget(),
        "arena_box_in_forget: use a BumpForget context (forget-on-reset)",
    );
    let b = alloc_in(mcx, value)?;
    Ok(allocator_api2::boxed::Box::leak(b))
}

pub fn arena_vec_in_forget<'mcx, T: ArenaSafe>(
    mcx: Mcx<'mcx>,
    vec: PgVec<'mcx, T>,
) -> PgResult<&'mcx mut PgVec<'mcx, T>> {
    const { assert!(!core::mem::needs_drop::<T>()) };
    debug_assert!(
        mcx.context().is_bumpforget(),
        "arena_vec_in_forget: use a BumpForget context (forget-on-reset)",
    );
    let b = alloc_in(mcx, vec)?;
    Ok(allocator_api2::boxed::Box::leak(b))
}

/// Move out WITHOUT deallocate (the captured context may already be reset).
pub fn box_into_inner_leak<'mcx, T>(b: PgBox<'mcx, T>) -> T {
    let (raw, _alloc) = allocator_api2::boxed::Box::into_raw_with_allocator(b);
    // SAFETY: `raw` read exactly once; `_alloc` is a Copy handle, never dereferenced.
    unsafe { core::ptr::read(raw) }
}

/// Sized -> unsized `PgBox` coercion; caller supplies the thin->fat cast.
///
/// # Safety
///
/// `coerce` MUST return the genuine unsizing coercion of the pointer it is
/// handed and nothing else — i.e. the same allocation, same address, only a
/// pointer-metadata (vtable / slice-length) attachment, exactly what
/// `|p| p as *mut dyn Trait` or `|p| p as *mut [T]` produce. The returned
/// pointer is fed to `Box::from_raw_in` with the allocator just decomposed, so
/// any other pointer (null, dangling, an interior offset, a foreign or
/// integer-cast address, or forged slice metadata) makes the reconstructed
/// `PgBox` own memory it does not back — a later use or drop then dereferences
/// and frees that bogus address through the context allocator (heap
/// corruption). This is why the function is `unsafe`: the signature alone
/// (`FnOnce(*mut P) -> *mut U`, with `U: ?Sized` admitting even `Sized` `U`)
/// cannot express or check the derived-pointer precondition, so the caller
/// must uphold it.
pub unsafe fn box_unsize_dyn<'mcx, P, U>(
    sized: PgBox<'mcx, P>,
    coerce: impl FnOnce(*mut P) -> *mut U,
) -> PgBox<'mcx, U>
where
    P: 'mcx,
    U: ?Sized + 'mcx,
{
    let (raw, alloc) = allocator_api2::boxed::Box::into_raw_with_allocator(sized);
    let fat: *mut U = coerce(raw);
    // SAFETY: by this fn's contract `coerce` returns the unsizing coercion of
    // `raw` (same allocation+address, metadata only), and `alloc` is the exact
    // allocator just decomposed from `sized`.
    unsafe { allocator_api2::boxed::Box::from_raw_in(fat, alloc) }
}

/// Move payload `P` out of an unsized `PgBox` without dropping `P`.
/// # Safety: `data` — the payload's data pointer inside `sized`; runtime type `P` (tag-checked).
pub unsafe fn box_read_payload<'mcx, P, U>(sized: PgBox<'mcx, U>, data: *const P) -> P
where
    P: 'mcx,
    U: ?Sized + 'mcx,
{
    let (raw, alloc) = allocator_api2::boxed::Box::into_raw_with_allocator(sized);
    let layout = core::alloc::Layout::for_value(unsafe { &*raw });
    let value = unsafe { core::ptr::read(data) };
    if layout.size() != 0 {
        let nn = core::ptr::NonNull::new(raw as *mut u8)
            .expect("box_read_payload: box raw pointer was null");
        unsafe { allocator_api2::alloc::Allocator::deallocate(&alloc, nn, layout) };
    }
    value
}

/// One reserve + one memcpy: stable extend_from_slice is a per-element loop, ~10x a bare memcpy.
#[inline]
pub fn vec_append_bytes(v: &mut PgVec<'_, u8>, bytes: &[u8]) -> PgResult<()> {
    let n = bytes.len();
    if n == 0 {
        return Ok(());
    }
    let mcx = *v.allocator();
    let old = v.len();
    if v.capacity() - old < n {
        let target = grow_target(v.capacity(), old.saturating_add(n));
        v.try_reserve_exact(target - old).map_err(|_| mcx.oom(n))?;
    }
    // SAFETY: capacity >= old + n after the reserve; dst disjoint; set_len covers the n bytes.
    unsafe {
        core::ptr::copy_nonoverlapping(bytes.as_ptr(), v.as_mut_ptr().add(old), n);
        v.set_len(old + n);
    }
    Ok(())
}

/// enlargeStringInfo (stringinfo.c): double until `needed` fits, clamped to
/// MaxAllocSize while `needed` itself is admissible (an over-limit need is
/// requested as-is so the allocator reports that size).
#[inline]
pub fn grow_target(capacity: usize, needed: usize) -> usize {
    let mut newlen = capacity.max(1).saturating_mul(2);
    while needed > newlen {
        newlen = newlen.saturating_mul(2);
    }
    if newlen > MAX_ALLOC_SIZE {
        newlen = needed.max(MAX_ALLOC_SIZE);
    }
    newlen
}

#[inline]
pub fn vec_new_in<'mcx, T>(mcx: Mcx<'mcx>) -> PgVec<'mcx, T> {
    const { assert!(!core::mem::needs_drop::<T>()) };
    PgVec::new_in(mcx)
}

#[inline]
pub fn vec_with_capacity_in<'mcx, T>(mcx: Mcx<'mcx>, cap: usize) -> PgResult<PgVec<'mcx, T>> {
    const { assert!(!core::mem::needs_drop::<T>()) };
    let request = cap.saturating_mul(core::mem::size_of::<T>());
    check_alloc_size(request)?;
    if cap == 0 || core::mem::size_of::<T>() == 0 {
        return Ok(PgVec::new_in(mcx));
    }
    // Thin shell: alloc via non-generic `alloc_uninit_bytes` (no per-T grow machinery).
    let layout = core::alloc::Layout::array::<T>(cap).map_err(|_| mcx.oom(request))?;
    let p = mcx.alloc_uninit_bytes(layout).map_err(|_| mcx.oom(request))?;
    // SAFETY: `p` is a fresh arena allocation of exactly `Layout::array::<T>(cap)`
    // bytes from `mcx`; len 0 with capacity `cap` (the same layout Vec recomputes
    // on grow/drop), matching this allocator.
    Ok(unsafe { PgVec::from_raw_parts_in(p.cast::<T>().as_ptr(), 0, cap, mcx) })
}

/// C `MemoryContextAllocExtended(.., MCXT_ALLOC_HUGE)` sizing rules: the
/// request is checked against `MaxAllocHugeSize` (SIZE_MAX/2), not the 1GB
/// `MaxAllocSize`. simplehash's default `SH_ALLOCATE` allocates its element
/// array this way, so hash-table bucket arrays may legally exceed 1GB.
#[inline]
pub fn vec_with_capacity_huge_in<'mcx, T>(
    mcx: Mcx<'mcx>,
    cap: usize,
) -> PgResult<PgVec<'mcx, T>> {
    const { assert!(!core::mem::needs_drop::<T>()) };
    let request = cap.saturating_mul(core::mem::size_of::<T>());
    if request > MAX_ALLOC_HUGE_SIZE {
        return Err(invalid_alloc_size(request));
    }
    if cap == 0 || core::mem::size_of::<T>() == 0 {
        return Ok(PgVec::new_in(mcx));
    }
    // Thin shell mirroring `vec_with_capacity_in`: the huge entry point plus
    // from_raw_parts_in, so it never funnels through the ceiling-checked
    // `Allocator::allocate`.
    let layout = core::alloc::Layout::array::<T>(cap).map_err(|_| mcx.oom(request))?;
    let p = mcx.alloc_uninit_bytes_huge(layout).map_err(|_| mcx.oom(request))?;
    // SAFETY: `p` is a fresh allocation of exactly `Layout::array::<T>(cap)`
    // bytes from `mcx`; len 0 with capacity `cap` (the same layout Vec
    // recomputes on grow/drop), matching this allocator.
    Ok(unsafe { PgVec::from_raw_parts_in(p.cast::<T>().as_ptr(), 0, cap, mcx) })
}

/// C `repalloc_huge` / simplehash `SH_GROW` sizing rules for an existing
/// vector: ensure capacity for `len + additional` elements, admitted against
/// `MaxAllocHugeSize` rather than palloc's 1GB `MaxAllocSize` (which
/// `Allocator::grow` now enforces). Grows alloc-new + copy + free, exactly
/// simplehash's grow shape; amortized doubling is the CALLER's job (C's
/// grow_memtuples / SH_GROW both compute the new size themselves).
pub fn vec_reserve_huge<'mcx, T>(v: &mut PgVec<'mcx, T>, additional: usize) -> PgResult<()> {
    const { assert!(!core::mem::needs_drop::<T>()) };
    use allocator_api2::alloc::Allocator;
    let len = v.len();
    let mcx = *v.allocator();
    let need = len
        .checked_add(additional)
        .ok_or_else(|| invalid_alloc_size(usize::MAX))?;
    if need <= v.capacity() {
        return Ok(());
    }
    let request = need.saturating_mul(core::mem::size_of::<T>());
    if request > MAX_ALLOC_HUGE_SIZE {
        return Err(invalid_alloc_size(request));
    }
    debug_assert!(core::mem::size_of::<T>() != 0, "huge reserve of a ZST cannot fail");
    let new_layout = core::alloc::Layout::array::<T>(need).map_err(|_| mcx.oom(request))?;
    let p = mcx.alloc_uninit_bytes_huge(new_layout).map_err(|_| mcx.oom(request))?;
    let old_cap = v.capacity();
    let old_vec = core::mem::replace(v, PgVec::new_in(mcx));
    let (old_ptr, old_len, _old_cap, _mcx) = split_vec_raw_parts(old_vec);
    debug_assert_eq!(old_len, len);
    // SAFETY: `p` is a fresh `need`-element allocation (need > old_cap >= len),
    // disjoint from the old buffer; `len` initialized elements are copied and
    // the old buffer is returned to the same allocator with its true layout.
    unsafe {
        core::ptr::copy_nonoverlapping(old_ptr, p.cast::<T>().as_ptr(), len);
        if old_cap != 0 {
            let old_layout = core::alloc::Layout::array::<T>(old_cap)
                .expect("existing capacity has a valid layout");
            Allocator::deallocate(&mcx, NonNull::new_unchecked(old_ptr as *mut u8), old_layout);
        }
        *v = PgVec::from_raw_parts_in(p.cast::<T>().as_ptr(), len, need, mcx);
    }
    Ok(())
}

/// Decompose a `PgVec` without dropping it (no `Vec::into_raw_parts` on
/// allocator_api2's stable surface).
fn split_vec_raw_parts<'mcx, T>(v: PgVec<'mcx, T>) -> (*mut T, usize, usize, Mcx<'mcx>) {
    let mut v = core::mem::ManuallyDrop::new(v);
    (v.as_mut_ptr(), v.len(), v.capacity(), *v.allocator())
}

/// Aborts on failure (C palloc never returns NULL); droppy `T` allowed.
#[inline]
pub fn box_new_in<'mcx, T>(mcx: Mcx<'mcx>, value: T) -> PgBox<'mcx, T> {
    PgBox::new_in(value, mcx)
}

#[inline]
pub fn vec_with_capacity_in_infallible<'mcx, T>(mcx: Mcx<'mcx>, cap: usize) -> PgVec<'mcx, T> {
    const { assert!(!core::mem::needs_drop::<T>()) };
    PgVec::with_capacity_in(cap, mcx)
}

#[inline]
pub fn vec_from_elem_in<'mcx, T: Clone>(mcx: Mcx<'mcx>, value: T, n: usize) -> PgVec<'mcx, T> {
    const { assert!(!core::mem::needs_drop::<T>()) };
    let mut v = PgVec::with_capacity_in(n, mcx);
    v.resize(n, value);
    v
}

/// Frees through the box's allocator; context must be live (else box_into_inner_leak).
#[inline]
pub fn box_into_inner<'mcx, T>(b: PgBox<'mcx, T>) -> T {
    allocator_api2::boxed::Box::into_inner(b)
}

/// C palloc + memcpy, one-shot, len == capacity; Copy elements lower to one memcpy.
#[inline]
pub fn slice_in<'mcx, T: Clone>(mcx: Mcx<'mcx>, src: &[T]) -> PgResult<PgVec<'mcx, T>> {
    const { assert!(!core::mem::needs_drop::<T>()) };
    use allocator_api2::alloc::Allocator;
    let len = src.len();
    let request = len.saturating_mul(core::mem::size_of::<T>());
    check_alloc_size(request)?;
    if len == 0 {
        return Ok(PgVec::new_in(mcx));
    }
    let layout = core::alloc::Layout::array::<T>(len).map_err(|_| mcx.oom(request))?;
    let ptr = Allocator::allocate(&mcx, layout).map_err(|_| mcx.oom(request))?;
    let dst = ptr.as_ptr() as *mut T;

    // Frees the buffer if a clone() panics; the prefix needs no drops.
    struct FillGuard<'m, T> {
        dst: *mut T,
        layout: core::alloc::Layout,
        mcx: Mcx<'m>,
    }
    impl<T> Drop for FillGuard<'_, T> {
        fn drop(&mut self) {
            use allocator_api2::alloc::Allocator;
            // SAFETY: `dst` is the live allocation from `mcx`, solely guard-owned.
            unsafe {
                Allocator::deallocate(
                    &self.mcx,
                    core::ptr::NonNull::new_unchecked(self.dst as *mut u8),
                    self.layout,
                );
            }
        }
    }

    let guard = FillGuard { dst, layout, mcx };
    // SAFETY: fresh allocation for `len` Ts; each slot written once; src distinct.
    for (i, elem) in src.iter().enumerate() {
        unsafe { guard.dst.add(i).write(elem.clone()) };
    }
    let dst = guard.dst;
    core::mem::forget(guard);
    // SAFETY: `len` initialized Ts, capacity exactly `len`.
    Ok(unsafe { PgVec::from_raw_parts_in(dst, len, len, mcx) })
}

pub fn slice_borrow_in<'mcx, T: Clone>(mcx: Mcx<'mcx>, src: &[T]) -> PgResult<&'mcx [T]> {
    const { assert!(!core::mem::needs_drop::<T>()) };
    let v: PgVec<'mcx, T> = slice_in(mcx, src)?;
    vec_borrow_in(mcx, v)
}

pub fn vec_borrow_in<'mcx, T>(_mcx: Mcx<'mcx>, v: PgVec<'mcx, T>) -> PgResult<&'mcx [T]> {
    const { assert!(!core::mem::needs_drop::<T>()) };
    let boxed: allocator_api2::boxed::Box<[T], Mcx<'mcx>> = v.into_boxed_slice();
    Ok(allocator_api2::boxed::Box::leak(boxed))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod churn_probe {
    use core::sync::atomic::{AtomicU64, Ordering};
    pub(crate) static REAL_FREES: AtomicU64 = AtomicU64::new(0);
    pub(crate) fn bump() {
        REAL_FREES.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn take() -> u64 {
        REAL_FREES.swap(0, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod bump_policy_wrapper_tests {
    use super::*;

    #[test]
    fn capped_wrapper_grow_reset_and_shrink() {
        let parent = MemoryContext::new_bump("parent");
        let mut child = parent.new_child_bump_with_max_block_size("capped", 8192);
        for _ in 0..2 {
            {
                let mcx = child.mcx();
                let small = Layout::from_size_align(32, 8).unwrap();
                let large = Layout::from_size_align(1024, 8).unwrap();
                let aligned = Layout::from_size_align(16, 256).unwrap();
                let p = Allocator::allocate(&mcx, small).unwrap().cast::<u8>();
                // SAFETY: p owns 32 writable bytes; grow/shrink receive the live allocation's layout.
                unsafe {
                    p.as_ptr().write_bytes(0xa7, 32);
                    let q = Allocator::grow(&mcx, p, small, large).unwrap().cast::<u8>();
                    assert_eq!(core::slice::from_raw_parts(q.as_ptr(), 32), &[0xa7; 32]);
                    let r = Allocator::shrink(&mcx, q, large, aligned).unwrap().cast::<u8>();
                    assert_eq!(r.as_ptr() as usize % 256, 0);
                    assert_eq!(core::slice::from_raw_parts(r.as_ptr(), 16), &[0xa7; 16]);
                }
            }
            child.reset();
        }
    }
}
