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

pub use allocator_api2::alloc::Allocator;

use allocator_api2::alloc::{AllocError, Global};
use types_error::{PgError, PgResult, ERRCODE_OUT_OF_MEMORY};

mod owned;
mod string;
pub use owned::{Bind, McxOwned};
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

/// The accounting node: one per context, linked into a tree that is
/// **separate from ownership**. Parent links are strong (`Rc`) so a child's
/// charges always reach live ancestor counters; child links are weak, used
/// only for stats traversal. Cleanup-cascade is NOT this tree's job — that is
/// Rust ownership (hold child `MemoryContext`s in your state struct).
///
/// Charges propagate eagerly: `subtree_used` at every ancestor moves on each
/// allocate/free, so `MemoryContextMemAllocated(ctx, recurse=true)`-style
/// reads (nodeAgg's spill decision) are O(1).
struct Acct {
    name: &'static str,
    /// `MemoryContextSetIdentifier`: distinguishes same-kind contexts in
    /// stats (e.g. which prepared statement). Owned copy — C instead requires
    /// the caller to keep the string alive as long as the context.
    ident: RefCell<Option<alloc::string::String>>,
    /// Live requested bytes allocated *in this context* (collection
    /// capacities, not lengths).
    self_used: Cell<usize>,
    /// `self_used` + descendants', maintained eagerly.
    subtree_used: Cell<usize>,
    /// High-water marks of the two counters above.
    self_peak: Cell<usize>,
    subtree_peak: Cell<usize>,
    /// Ceiling on `subtree_used` at this node; `usize::MAX` = unlimited. An
    /// allocation that would push any ancestor past its limit fails
    /// (surfacing through `try_` collection APIs), mirroring
    /// `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)`.
    limit: Cell<usize>,
    parent: Option<alloc::rc::Rc<Acct>>,
    children: RefCell<alloc::vec::Vec<alloc::rc::Weak<Acct>>>,
}

impl Acct {
    fn ancestors(self: &alloc::rc::Rc<Self>) -> impl Iterator<Item = &Acct> {
        let mut cur: Option<&Acct> = Some(self);
        core::iter::from_fn(move || {
            let node = cur?;
            cur = node.parent.as_deref();
            Some(node)
        })
    }
}

/// A named allocation domain with exact accounting and an optional byte limit.
///
/// `!Sync` by construction (interior `Cell`s/`Rc`): a context belongs to one
/// backend process/thread, as in PG.
pub struct MemoryContext {
    acct: alloc::rc::Rc<Acct>,
    backend: Backend,
    /// `MemoryContextRegisterResetCallback`: fired LIFO on `reset` and drop,
    /// popped-before-call so a re-entrant registration cannot double-fire.
    reset_cbs: RefCell<alloc::vec::Vec<alloc::boxed::Box<dyn FnOnce()>>>,
}

impl MemoryContext {
    /// Malloc-backed root context (`AllocSetContextCreate` semantics).
    pub fn new(name: &'static str) -> Self {
        Self::with_backend(name, Backend::Malloc, None)
    }

    /// Bump-arena root context (`BumpContextCreate` semantics): per-chunk free
    /// is a no-op; memory is reclaimed wholesale by [`reset`](Self::reset) /
    /// drop.
    pub fn new_bump(name: &'static str) -> Self {
        Self::with_backend(name, Backend::Bump(bumpalo::Bump::new()), None)
    }

    /// Child context for accounting purposes: its allocations also count
    /// toward (and are limited by) this context and its ancestors, and it
    /// appears in [`stats_tree`](Self::stats_tree). The child is an
    /// independently owned value — *cleanup* nesting is expressed by storing
    /// it in the state struct that should own it, not by this link.
    pub fn new_child(&self, name: &'static str) -> MemoryContext {
        Self::with_backend(name, Backend::Malloc, Some(self.acct.clone()))
    }

    /// [`new_child`](Self::new_child) with a bump backend.
    pub fn new_child_bump(&self, name: &'static str) -> MemoryContext {
        Self::with_backend(name, Backend::Bump(bumpalo::Bump::new()), Some(self.acct.clone()))
    }

    fn with_backend(
        name: &'static str,
        backend: Backend,
        parent: Option<alloc::rc::Rc<Acct>>,
    ) -> Self {
        let acct = alloc::rc::Rc::new(Acct {
            name,
            ident: RefCell::new(None),
            self_used: Cell::new(0),
            subtree_used: Cell::new(0),
            self_peak: Cell::new(0),
            subtree_peak: Cell::new(0),
            limit: Cell::new(usize::MAX),
            parent,
            children: RefCell::new(alloc::vec::Vec::new()),
        });
        if let Some(p) = &acct.parent {
            let mut children = p.children.borrow_mut();
            // Amortized pruning: dead Weaks are otherwise only collected by
            // stats_tree() walks, and context churn (per-tuple children)
            // must not grow this list unboundedly when stats are never read.
            if children.len() == children.capacity() {
                children.retain(|w| w.strong_count() > 0);
            }
            children.push(alloc::rc::Rc::downgrade(&acct));
        }
        MemoryContext { acct, backend, reset_cbs: RefCell::new(alloc::vec::Vec::new()) }
    }

    /// Builder: cap this context's **subtree** bytes at `limit` (0 is a real,
    /// always-full limit — pass `usize::MAX` for unlimited, or just don't
    /// call this). Subtree semantics match how PG uses recursive
    /// `MemoryContextMemAllocated` for work_mem-style decisions.
    pub fn with_limit(self, limit: usize) -> Self {
        self.acct.limit.set(limit);
        self
    }

    /// The allocator handle collections store. Copy/Clone; borrowing it ties
    /// every allocation to this context's lifetime.
    pub fn mcx(&self) -> Mcx<'_> {
        Mcx(self)
    }

    pub fn name(&self) -> &'static str {
        self.acct.name
    }

    /// `MemoryContextSetIdentifier`: attach (or with `None`, forget) an
    /// identifier distinguishing same-kind contexts in stats dumps.
    pub fn set_ident(&self, id: Option<&str>) {
        *self.acct.ident.borrow_mut() = id.map(alloc::string::String::from);
    }

    pub fn ident(&self) -> Option<alloc::string::String> {
        self.acct.ident.borrow().clone()
    }

    /// Live requested bytes allocated in this context itself.
    pub fn used(&self) -> usize {
        self.acct.self_used.get()
    }

    /// Live requested bytes in this context plus all its accounting
    /// descendants — C's `MemoryContextMemAllocated(ctx, recurse=true)`,
    /// maintained eagerly so this is O(1).
    pub fn subtree_used(&self) -> usize {
        self.acct.subtree_used.get()
    }

    /// High-water mark of [`used`](Self::used).
    pub fn peak(&self) -> usize {
        self.acct.self_peak.get()
    }

    /// High-water mark of [`subtree_used`](Self::subtree_used).
    pub fn subtree_peak(&self) -> usize {
        self.acct.subtree_peak.get()
    }

    pub fn limit(&self) -> usize {
        self.acct.limit.get()
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
            self.acct.self_used.get(),
            0,
            "context {:?} reset with {} bytes still charged (leaked allocation?)",
            self.acct.name,
            self.acct.self_used.get(),
        );
        if let Backend::Bump(bump) = &mut self.backend {
            bump.reset();
        }
        // High-water marks restart; subtree_peak can't drop below what
        // descendants still hold.
        self.acct.self_peak.set(0);
        self.acct.subtree_peak.set(self.acct.subtree_used.get());
    }

    /// Point-in-time accounting snapshot (`MemoryContextStats` per-node line;
    /// log emission lives with the eventual caller).
    pub fn stats(&self) -> ContextStats {
        ContextStats {
            name: self.acct.name,
            ident: self.ident(),
            used: self.acct.self_used.get(),
            peak: self.acct.self_peak.get(),
            subtree_used: self.acct.subtree_used.get(),
            subtree_peak: self.acct.subtree_peak.get(),
            limit: self.acct.limit.get(),
            arena_footprint: match &self.backend {
                Backend::Malloc => self.acct.self_used.get(),
                Backend::Bump(b) => b.allocated_bytes(),
            },
        }
    }

    /// Hierarchical accounting snapshot of this context and its live
    /// accounting descendants (`MemoryContextStatsDetail` shape). Children
    /// whose contexts have been dropped are pruned as encountered.
    pub fn stats_tree(&self) -> TreeStats {
        fn node(acct: &Acct) -> TreeStats {
            let mut children = alloc::vec::Vec::new();
            acct.children.borrow_mut().retain(|w| match w.upgrade() {
                Some(c) => {
                    children.push(node(&c));
                    true
                }
                None => false,
            });
            TreeStats {
                name: acct.name,
                ident: acct.ident.borrow().clone(),
                used: acct.self_used.get(),
                peak: acct.self_peak.get(),
                subtree_used: acct.subtree_used.get(),
                subtree_peak: acct.subtree_peak.get(),
                limit: acct.limit.get(),
                children,
            }
        }
        node(&self.acct)
    }

    /// The `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)` this context produces when
    /// an allocation fails; message shape follows `mcxt.c`.
    pub fn oom(&self, request: usize) -> PgError {
        crate::oom_named(self.acct.name, request)
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

    /// Validate against every ancestor's limit, then apply to every
    /// ancestor's subtree counter — two passes so a failure applies nothing.
    fn charge(&self, n: usize) -> Result<(), AllocError> {
        for node in self.acct.ancestors() {
            let new = node.subtree_used.get().checked_add(n).ok_or(AllocError)?;
            if new > node.limit.get() {
                return Err(AllocError);
            }
        }
        let self_new = self.acct.self_used.get() + n;
        self.acct.self_used.set(self_new);
        if self_new > self.acct.self_peak.get() {
            self.acct.self_peak.set(self_new);
        }
        for node in self.acct.ancestors() {
            let new = node.subtree_used.get() + n;
            node.subtree_used.set(new);
            if new > node.subtree_peak.get() {
                node.subtree_peak.set(new);
            }
        }
        Ok(())
    }

    fn uncharge(&self, n: usize) {
        debug_assert!(
            self.acct.self_used.get() >= n,
            "context {:?} uncharging {} with only {} charged",
            self.acct.name,
            n,
            self.acct.self_used.get(),
        );
        self.acct.self_used.set(self.acct.self_used.get().saturating_sub(n));
        for node in self.acct.ancestors() {
            node.subtree_used.set(node.subtree_used.get().saturating_sub(n));
        }
    }
}

/// `mcxt.c`'s out-of-memory `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)` for an
/// allocation charged to a context identified by name. [`MemoryContext::oom`]
/// and [`Mcx::oom`] delegate here; ports whose backend-local state stands in
/// for a named C context (e.g. a `thread_local!` replacing
/// `TopTransactionContext` storage) call it directly rather than duplicating
/// the message shape.
pub fn oom_named(context_name: &str, request: usize) -> PgError {
    PgError::error("out of memory")
        .with_sqlstate(ERRCODE_OUT_OF_MEMORY)
        .with_detail(alloc::format!(
            "Failed on request of size {request} in memory context \"{context_name}\"."
        ))
}

impl Drop for MemoryContext {
    /// `MemoryContextDelete` fires reset callbacks too. Any bytes still
    /// charged (possible only via `mem::forget` of a collection) are returned
    /// to ancestor counters so the accounting tree never holds phantom bytes.
    fn drop(&mut self) {
        self.fire_reset_callbacks();
        // C MemoryContextDeleteOnly resets the ident; ours would otherwise
        // linger on the Acct node a surviving child keeps alive.
        self.acct.ident.borrow_mut().take();
        let residual = self.acct.self_used.get();
        if residual > 0 {
            for node in self.acct.ancestors() {
                node.subtree_used.set(node.subtree_used.get().saturating_sub(residual));
            }
            self.acct.self_used.set(0);
        }
    }
}

impl fmt::Debug for MemoryContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryContext")
            .field("name", &self.acct.name)
            .field("used", &self.acct.self_used.get())
            .field("subtree_used", &self.acct.subtree_used.get())
            .field("peak", &self.acct.self_peak.get())
            .field("limit", &self.acct.limit.get())
            .finish_non_exhaustive()
    }
}

/// One context's accounting numbers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextStats {
    pub name: &'static str,
    pub ident: Option<alloc::string::String>,
    /// Live requested bytes in this context itself.
    pub used: usize,
    /// High-water mark of `used` since creation/reset.
    pub peak: usize,
    /// Live requested bytes including accounting descendants.
    pub subtree_used: usize,
    pub subtree_peak: usize,
    pub limit: usize,
    /// Bytes the backend actually holds (== `used` for malloc; the arena size
    /// for bump, which retains freed bytes until reset).
    pub arena_footprint: usize,
}

/// Hierarchical stats: one context's numbers plus its accounting children's.
/// (No `arena_footprint` — backends live with each owned context, not in the
/// accounting tree.)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeStats {
    pub name: &'static str,
    pub ident: Option<alloc::string::String>,
    pub used: usize,
    pub peak: usize,
    pub subtree_used: usize,
    pub subtree_peak: usize,
    pub limit: usize,
    pub children: alloc::vec::Vec<TreeStats>,
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
        write!(f, "Mcx({:?})", self.0.acct.name)
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

/// `MaxAllocSize` (`memutils.h`): the cap `palloc` enforces on a single
/// request — `((Size) 0x3fffffff)`, 1 gigabyte minus one.
pub const MAX_ALLOC_SIZE: usize = 0x3FFF_FFFF;

/// `AllocSizeIsValid` gate: requests above [`MAX_ALLOC_SIZE`] fail the way
/// `mcxt.c` does — `elog(ERROR, "invalid memory alloc request size %zu")`.
/// Like `palloc`, this catches negative C sizes too: an `int` count converted
/// to `Size` becomes huge, so callers porting `palloc(n * sizeof(T))` with a
/// signed `n` should sign-extend `n` to `usize` and let this gate reject it.
pub fn check_alloc_size(request: usize) -> PgResult<()> {
    if request > MAX_ALLOC_SIZE {
        return Err(PgError::error(alloc::format!(
            "invalid memory alloc request size {request}"
        )));
    }
    Ok(())
}

/// `palloc`-shaped convenience: a boxed value in `mcx`, failing with the
/// context's OOM error (C: `MemoryContextAlloc`).
pub fn alloc_in<'mcx, T>(mcx: Mcx<'mcx>, value: T) -> PgResult<PgBox<'mcx, T>> {
    check_alloc_size(core::mem::size_of::<T>())?;
    PgBox::try_new_in(value, mcx).map_err(|_| mcx.oom(core::mem::size_of::<T>()))
}

/// Leak a [`PgBox`] into an honest `&'mcx mut T` borrow of the value, tied to
/// the box's own context lifetime `'mcx`.
///
/// The `PgBox<'mcx, T>` allocates `T` in a context whose handle is `Mcx<'mcx>`;
/// the value lives until that context is reset/dropped. `allocator_api2::Box::
/// leak` forgets the box's `Drop` (so the value is *not* individually freed) and
/// hands back a reference whose lifetime is bounded only by the allocator
/// (`A: 'a`, here `Mcx<'mcx>: 'mcx`). That makes the returned `&'mcx mut T` an
/// HONEST borrow, not a `transmute`: the memory genuinely lives for `'mcx`,
/// because the per-context drop is what reclaims it — faithful to C, where a
/// plan node is "freed with its context", never individually.
///
/// This is the one primitive the executor's `InitPlan` needs over the
/// run/teardown family: `ExecInitNode` is signed `node: Option<&'mcx Node<'mcx>>`
/// (C's `ExecInitNode(plannedstmt->planTree, ...)`), so the plan tree must cross
/// as a real `&'mcx Node`, while the bundle that owns it (`QueryWorkState`) hands
/// it out only through `for<'mcx>`-universal accessors that keep the borrow from
/// escaping.
pub fn leak_in<'mcx, T>(b: PgBox<'mcx, T>) -> &'mcx mut T {
    PgBox::leak(b)
}

/// Coerce a sized `PgBox<'mcx, P>` to an unsized `PgBox<'mcx, U>` (a trait
/// object `dyn Trait`), preserving the context allocator.
///
/// `PgBox` is `allocator_api2::boxed::Box<T, Mcx>` on stable Rust. Stable lacks
/// the `CoerceUnsized` impl for `Box<T, A>` (it is nightly-only), so the usual
/// implicit `Box<P>` -> `Box<dyn U>` coercion is unavailable here. We perform
/// the unsizing **manually**: take the box apart into its raw `*mut P` plus the
/// allocator, let the compiler perform the legal *unsizing pointer cast*
/// `*mut P -> *mut U` (this thin->fat cast IS stable — only the `Box` impl that
/// hides it is not), then reassemble the box from the fat pointer + allocator.
///
/// The caller supplies the cast closure `coerce: |*mut P| -> *mut U` so this
/// helper stays agnostic to which `dyn` trait `U` is (mcx does not depend on
/// `types-nodes`). Typical call site:
/// ```ignore
/// let fat: PgBox<'mcx, dyn NodePayload<'mcx> + 'mcx> =
///     mcx::box_unsize_dyn(sized, |p| p as *mut (dyn NodePayload + 'mcx));
/// ```
///
/// # Safety of the two `unsafe` operations
/// 1. `into_raw_with_allocator` yields ownership of a live, properly-aligned,
///    non-null `*mut P` allocated by `alloc`; it is `unsafe`-free (a `Box`
///    inverse of `new`). The closure performs `*mut P -> *mut U`, an *unsizing*
///    coercion the compiler validates (`P: Unsize<U>`); the data address is
///    unchanged, only a vtable is attached. No provenance or lifetime is
///    fabricated.
/// 2. `from_raw_in(raw, alloc)` reconstructs ownership of *exactly* the pointer
///    and allocator just taken apart — same allocation, same allocator, no
///    aliasing, the box has not been used in between — so it satisfies
///    `from_raw_in`'s contract (pointer came from `Box::into_raw_with_allocator`
///    with this allocator). The `Drop`/dealloc path is therefore identical to
///    the original sized box (the vtable's drop glue frees `P`).
pub fn box_unsize_dyn<'mcx, P, U>(
    sized: PgBox<'mcx, P>,
    coerce: impl FnOnce(*mut P) -> *mut U,
) -> PgBox<'mcx, U>
where
    P: 'mcx,
    U: ?Sized + 'mcx,
{
    // (1) Decompose the sized box; perform the thin->fat unsizing cast.
    let (raw, alloc) = allocator_api2::boxed::Box::into_raw_with_allocator(sized);
    let fat: *mut U = coerce(raw);
    // (2) Reassemble ownership from the fat pointer + the same allocator.
    unsafe { allocator_api2::boxed::Box::from_raw_in(fat, alloc) }
}

/// Fallible `Vec` construction with the context's OOM error. Enforces
/// `palloc`'s `MaxAllocSize` gate on the byte size of the request.
pub fn vec_with_capacity_in<'mcx, T>(mcx: Mcx<'mcx>, cap: usize) -> PgResult<PgVec<'mcx, T>> {
    let request = cap.saturating_mul(core::mem::size_of::<T>());
    check_alloc_size(request)?;
    let mut v = PgVec::new_in(mcx);
    v.try_reserve_exact(cap).map_err(|_| mcx.oom(request))?;
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
