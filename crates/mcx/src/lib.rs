//! Memory contexts with allocator-tied lifetimes. Design: `docs/mctx-design.md`.
//!
//! A [`MemoryContext`] is a named allocation domain: exact requested-bytes
//! accounting, a `work_mem`-style limit, LIFO reset callbacks, and a backend
//! (malloc-per-chunk or bump arena). Collections allocate through a copyable
//! [`Mcx<'mcx>`] handle and therefore cannot outlive their context; every
//! allocate / grow / shrink / drop passes through the handle, so the counters
//! cannot desync and there is no wrong-context argument to pass.
//!
//! There is deliberately **no ambient current context** — no thread-local, no
//! `MemoryContextSwitchTo`. C call sites that allocate in `CurrentMemoryContext`
//! translate to functions that take an `Mcx<'mcx>` parameter. The C context
//! *tree* (cascading delete) is Rust ownership: hold child contexts in your
//! state struct and they die with it.

#![no_std]

extern crate alloc;

use core::alloc::Layout;
use core::cell::{Cell, RefCell};
use core::fmt;
use core::ptr::NonNull;

use allocator_api2::alloc::{AllocError, Allocator, Global};
use types_error::{PgError, PgResult, ERRCODE_OUT_OF_MEMORY};

mod string;
pub use string::PgString;

/// Growable array allocating in a context: the real `Vec`, full API included.
pub type PgVec<'mcx, T> = allocator_api2::vec::Vec<T, Mcx<'mcx>>;
/// Owned heap value allocating in a context.
pub type PgBox<'mcx, T> = allocator_api2::boxed::Box<T, Mcx<'mcx>>;
/// Hash map allocating in a context.
pub type PgHashMap<'mcx, K, V> =
    hashbrown::HashMap<K, V, hashbrown::hash_map::DefaultHashBuilder, Mcx<'mcx>>;

/// Allocation backend, mirroring C's `mcxt_methods[]` dispatch. Variants are
/// closed: PG has exactly these context kinds (`aset.c`, `generation.c`,
/// `slab.c`, `bump.c`). Aset/generation/slab currently share the malloc-backed
/// implementation — same semantics, no block structure yet; upgrading them is
/// internal to this enum and does not change the API.
enum Backend {
    /// `aset.c` / `generation.c` / `slab.c` semantics: individually freed chunks.
    Malloc,
    /// `bump.c` semantics via bumpalo: no per-chunk free; `reset` reclaims all.
    Bump(bumpalo::Bump),
}

/// A named allocation domain with exact accounting and an optional byte limit.
///
/// `!Sync` by construction (interior `Cell`s): a context belongs to one
/// backend process/thread, as in PG.
pub struct MemoryContext {
    name: &'static str,
    /// Live requested bytes (collection capacities, not lengths).
    used: Cell<usize>,
    /// High-water mark of `used`.
    peak: Cell<usize>,
    /// Allocation ceiling in bytes; `usize::MAX` = unlimited. An allocation
    /// that would push `used` past this fails (surfacing through `try_`
    /// collection APIs), mirroring `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)`.
    limit: usize,
    backend: Backend,
    /// `MemoryContextRegisterResetCallback`: fired LIFO on `reset` and drop,
    /// popped-before-call so a re-entrant registration cannot double-fire.
    reset_cbs: RefCell<alloc::vec::Vec<alloc::boxed::Box<dyn FnOnce()>>>,
}

impl MemoryContext {
    /// Malloc-backed context (`AllocSetContextCreate` semantics).
    pub fn new(name: &'static str) -> Self {
        Self::with_backend(name, Backend::Malloc)
    }

    /// Bump-arena context (`BumpContextCreate` semantics): per-chunk free is a
    /// no-op; memory is reclaimed wholesale by [`reset`](Self::reset) / drop.
    pub fn new_bump(name: &'static str) -> Self {
        Self::with_backend(name, Backend::Bump(bumpalo::Bump::new()))
    }

    fn with_backend(name: &'static str, backend: Backend) -> Self {
        MemoryContext {
            name,
            used: Cell::new(0),
            peak: Cell::new(0),
            limit: usize::MAX,
            backend,
            reset_cbs: RefCell::new(alloc::vec::Vec::new()),
        }
    }

    /// Builder: cap `used` at `limit` bytes (0 is a real, always-full limit —
    /// pass `usize::MAX` for unlimited, or just don't call this).
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// The allocator handle collections store. Copy/Clone; borrowing it ties
    /// every allocation to this context's lifetime.
    pub fn mcx(&self) -> Mcx<'_> {
        Mcx(self)
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Live requested bytes.
    pub fn used(&self) -> usize {
        self.used.get()
    }

    /// High-water mark of [`used`](Self::used).
    pub fn peak(&self) -> usize {
        self.peak.get()
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    /// `MemoryContextRegisterResetCallback`. Callbacks run LIFO on
    /// [`reset`](Self::reset) and on drop.
    pub fn register_reset_callback(&self, cb: impl FnOnce() + 'static) {
        self.reset_cbs.borrow_mut().push(alloc::boxed::Box::new(cb));
    }

    /// `MemoryContextReset`. `&mut self` statically guarantees no allocation
    /// in this context survives (everything holding an `Mcx<'_>` has been
    /// dropped, returning its bytes), so for a bump backend the whole arena is
    /// reclaimed in O(1). Fires reset callbacks LIFO, then resets the
    /// high-water mark.
    pub fn reset(&mut self) {
        self.fire_reset_callbacks();
        debug_assert_eq!(
            self.used.get(),
            0,
            "context {:?} reset with {} bytes still charged (leaked allocation?)",
            self.name,
            self.used.get(),
        );
        if let Backend::Bump(bump) = &mut self.backend {
            bump.reset();
        }
        self.used.set(0);
        self.peak.set(0);
    }

    /// Point-in-time accounting snapshot (`MemoryContextStats` per-node line;
    /// hierarchy and log emission live with the eventual caller).
    pub fn stats(&self) -> ContextStats {
        ContextStats {
            name: self.name,
            used: self.used.get(),
            peak: self.peak.get(),
            limit: self.limit,
            arena_footprint: match &self.backend {
                Backend::Malloc => self.used.get(),
                Backend::Bump(b) => b.allocated_bytes(),
            },
        }
    }

    /// The `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)` this context produces when
    /// an allocation fails; message shape follows `mcxt.c`.
    pub fn oom(&self, request: usize) -> PgError {
        PgError::error("out of memory")
            .with_sqlstate(ERRCODE_OUT_OF_MEMORY)
            .with_detail(alloc::format!(
                "Failed on request of size {} in memory context \"{}\".",
                request,
                self.name
            ))
    }

    fn fire_reset_callbacks(&self) {
        // Popped-before-call: a callback registering another callback sees a
        // consistent stack and the new one fires too (still LIFO overall).
        loop {
            let cb = self.reset_cbs.borrow_mut().pop();
            match cb {
                Some(cb) => cb(),
                None => break,
            }
        }
    }

    #[inline]
    fn charge(&self, n: usize) -> Result<(), AllocError> {
        let used = self.used.get();
        let new = used.checked_add(n).ok_or(AllocError)?;
        if new > self.limit {
            return Err(AllocError);
        }
        self.used.set(new);
        if new > self.peak.get() {
            self.peak.set(new);
        }
        Ok(())
    }

    #[inline]
    fn uncharge(&self, n: usize) {
        debug_assert!(
            self.used.get() >= n,
            "context {:?} uncharging {} with only {} charged",
            self.name,
            n,
            self.used.get(),
        );
        self.used.set(self.used.get().saturating_sub(n));
    }
}

impl Drop for MemoryContext {
    /// `MemoryContextDelete` fires reset callbacks too.
    fn drop(&mut self) {
        self.fire_reset_callbacks();
    }
}

impl fmt::Debug for MemoryContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryContext")
            .field("name", &self.name)
            .field("used", &self.used.get())
            .field("peak", &self.peak.get())
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

/// One context's accounting numbers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContextStats {
    pub name: &'static str,
    /// Live requested bytes.
    pub used: usize,
    /// High-water mark since creation/reset.
    pub peak: usize,
    pub limit: usize,
    /// Bytes the backend actually holds (== `used` for malloc; the arena size
    /// for bump, which retains freed bytes until reset).
    pub arena_footprint: usize,
}

/// Copyable allocator handle: `&'mcx MemoryContext` + the `Allocator` impl.
/// Storing this inside `PgVec`/`PgBox`/`PgHashMap` is what ties the
/// collection's lifetime to the context and routes every byte through the
/// context's accounting.
///
/// An allocation cannot outlive its context:
///
/// ```compile_fail,E0597
/// let v;
/// {
///     let ctx = mcx::MemoryContext::new("short-lived");
///     v = mcx::PgVec::<u8>::new_in(ctx.mcx());
/// } // `ctx` dropped here while `v` still borrows it
/// assert_eq!(v.len(), 0);
/// ```
///
/// And a reset is statically impossible while allocations are live:
///
/// ```compile_fail,E0502
/// let mut ctx = mcx::MemoryContext::new_bump("per-tuple");
/// let v = mcx::PgVec::<u8>::new_in(ctx.mcx());
/// ctx.reset(); // ERROR: cannot borrow `ctx` mutably while `v` borrows it
/// assert_eq!(v.len(), 0);
/// ```
#[derive(Clone, Copy)]
pub struct Mcx<'mcx>(&'mcx MemoryContext);

impl<'mcx> Mcx<'mcx> {
    pub fn context(self) -> &'mcx MemoryContext {
        self.0
    }

    /// Convert a failed fallible-allocation result into the context's
    /// out-of-memory `PgError` (use with `try_reserve`-style APIs).
    pub fn oom(self, request: usize) -> PgError {
        self.0.oom(request)
    }
}

impl fmt::Debug for Mcx<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Mcx({:?})", self.0.name)
    }
}

// SAFETY (trait contract, not thread safety): we return blocks that are valid
// until deallocated, never alias live blocks, and delegate all pointer
// handling to Global / bumpalo, which uphold the same contract. Accounting
// happens strictly before delegation on the failure-free path and is undone
// on delegation failure, so counters never drift from real allocations.
unsafe impl Allocator for Mcx<'_> {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.0.charge(layout.size())?;
        let result = match &self.0.backend {
            Backend::Malloc => Global.allocate(layout),
            Backend::Bump(bump) => bump.allocate(layout),
        };
        if result.is_err() {
            self.0.uncharge(layout.size());
        }
        result
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        self.0.uncharge(layout.size());
        match &self.0.backend {
            Backend::Malloc => Global.deallocate(ptr, layout),
            Backend::Bump(bump) => bump.deallocate(ptr, layout),
        }
    }

    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let delta = new_layout.size() - old_layout.size();
        self.0.charge(delta)?;
        let result = match &self.0.backend {
            Backend::Malloc => Global.grow(ptr, old_layout, new_layout),
            Backend::Bump(bump) => bump.grow(ptr, old_layout, new_layout),
        };
        if result.is_err() {
            self.0.uncharge(delta);
        }
        result
    }

    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let result = match &self.0.backend {
            Backend::Malloc => Global.shrink(ptr, old_layout, new_layout),
            Backend::Bump(bump) => bump.shrink(ptr, old_layout, new_layout),
        };
        if result.is_ok() {
            self.0.uncharge(old_layout.size() - new_layout.size());
        }
        result
    }
}

/// `palloc`-shaped convenience: a boxed value in `mcx`, failing with the
/// context's OOM error (C: `MemoryContextAlloc`).
pub fn alloc_in<'mcx, T>(mcx: Mcx<'mcx>, value: T) -> PgResult<PgBox<'mcx, T>> {
    PgBox::try_new_in(value, mcx).map_err(|_| mcx.oom(core::mem::size_of::<T>()))
}

/// Fallible `Vec` construction with the context's OOM error.
pub fn vec_with_capacity_in<'mcx, T>(mcx: Mcx<'mcx>, cap: usize) -> PgResult<PgVec<'mcx, T>> {
    let mut v = PgVec::new_in(mcx);
    v.try_reserve_exact(cap)
        .map_err(|_| mcx.oom(cap.saturating_mul(core::mem::size_of::<T>())))?;
    Ok(v)
}

/// Copy a slice into a context (C: `palloc` + `memcpy` idiom).
pub fn slice_in<'mcx, T: Clone>(mcx: Mcx<'mcx>, src: &[T]) -> PgResult<PgVec<'mcx, T>> {
    let mut v = vec_with_capacity_in(mcx, src.len())?;
    v.extend_from_slice(src);
    Ok(v)
}

#[cfg(test)]
mod tests;
