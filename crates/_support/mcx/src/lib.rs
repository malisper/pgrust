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
use ::types_error::{PgError, PgResult, ERRCODE_OUT_OF_MEMORY};

mod aset;
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
    /// `aset.c`: block-pooling allocator with power-of-two size-class freelists.
    /// `allocate` bump-carves a chunk from a pooled block (or pops a freed one);
    /// `deallocate` returns the chunk to its freelist; `reset`/drop reclaims the
    /// blocks wholesale. This is the default backend for [`MemoryContext::new`] /
    /// [`MemoryContext::new_child`] — it amortizes `malloc` the way C does. See
    /// [`aset`].
    Aset(RefCell<aset::AllocSet>),
    /// Plain `malloc`-per-chunk (`Global`). Kept as a fallback / comparison
    /// backend; the default contexts use [`Aset`](Backend::Aset). `generation.c`
    /// and `slab.c` semantics still share this until they get their own ports.
    Malloc,
    /// `bump.c` semantics via bumpalo: no per-chunk free; `reset` reclaims all.
    /// The `BumpBlocks` model tracks `bump.c`'s block structure
    /// (`mem_allocated` / `nblocks` / current-block free space) deterministically
    /// alongside the bumpalo arena, so `MemoryContextStats`-style reads report a
    /// real `Bump` context's `totalspace`/`freespace`/`nblocks` as `bump.c` does
    /// (bumpalo's internal chunk geometry is unrelated to `bump.c`'s block
    /// algorithm and cannot be used directly).
    Bump(bumpalo::Bump, RefCell<BumpBlocks>),
    /// `bump.c`-style arena **augmented with a per-context drop list** — C's
    /// `AllocSetReset` model for *owned* Rust values. Bytes are bump-allocated
    /// like [`Bump`](Backend::Bump), but a value with a destructor allocated
    /// here registers `(ptr, drop_glue::<T>)` on [`DropList`] at construction and
    /// has its own `Drop` suppressed (leaked); on `reset`/drop the context runs
    /// the drop list LIFO, then `bump.reset()`, then zeroes the accounting
    /// counters. Net: N allocations + 1 reset with **zero** per-object
    /// `Drop`/`dealloc`/`uncharge`. The drop-glue registration is *collection-
    /// side* ([`arena_box_in`]/[`arena_vec_in`]/…): the type-erased `Allocator`
    /// trait can't capture `T`'s destructor, but the caller — who knows `T` —
    /// can. Opt-in via [`MemoryContext::new_bumpdrop`] /
    /// [`MemoryContext::new_child_bumpdrop`]; never a default.
    BumpDrop(bumpalo::Bump, RefCell<BumpBlocks>, RefCell<DropList>),
}

/// One registered destructor: the data pointer of a leaked owned value living
/// in the arena, and a monomorphized `fn` that runs that value's drop glue.
///
/// `glue` is `unsafe fn(NonNull<u8>)` — for a value of type `T` it is
/// `drop_glue::<T>`, which `ptr::drop_in_place`s the `T` at `ptr`. The `fn`
/// pointer is captured *by the caller* (which statically knows `T`); the
/// allocator backend never sees `T`. This is the crux that makes a type-erased
/// `Allocator` drop-aware.
struct DropEntry {
    /// The value's address as an **exposed-provenance** raw pointer
    /// ([`core::ptr::with_exposed_provenance_mut`] reconstructs a usable pointer
    /// in `glue`). Storing the value's pointer this way — rather than a tagged
    /// `NonNull` derived from the returned `&mut T` — is what keeps the drop
    /// list Miri/Stacked-Borrows clean: the returned reference can be used
    /// arbitrarily by the caller without invalidating the drop list's copy,
    /// because an exposed pointer is not a tracked sibling in the borrow stack.
    /// (The provenance was exposed at registration in [`arena_leak`].)
    addr: *mut u8,
    glue: unsafe fn(*mut u8),
}

/// Per-`BumpDrop`-context list of registered destructors, run LIFO on
/// reset/drop (mirroring `reset_cbs` and C's reset-callback order). Stored in a
/// plain `alloc::vec::Vec` on the heap (NOT inside the bump arena): a separate
/// allocation keeps the list's own growth from aliasing the arena bytes it
/// points into, which is what keeps the raw drop-glue path Miri-clean. The
/// per-entry cost is a pointer + an `fn` pointer — vastly cheaper than the
/// `malloc`/`free` pair an `Aset`/`Malloc` context pays per object.
struct DropList {
    entries: alloc::vec::Vec<DropEntry>,
}

impl DropList {
    fn new() -> Self {
        DropList { entries: alloc::vec::Vec::new() }
    }

    /// Run every registered destructor **LIFO**, consuming the list. Panic-safe:
    /// each entry is `pop`ped *before* its glue runs, so a panicking destructor
    /// cannot leave a half-consumed entry that a later pass (the context's own
    /// `Drop`) would run again — at most the still-unrun (earlier-registered)
    /// entries leak, which is sound (leaking is always safe), never a double
    /// free / double drop.
    fn run(&mut self) {
        while let Some(entry) = self.entries.pop() {
            // SAFETY: `entry.ptr` is the live, aligned data pointer of a value of
            // the exact type `glue` was monomorphized for (`drop_glue::<T>`),
            // allocated into this context's arena and leaked (its own `Drop`
            // suppressed) at registration. It has not been dropped before — this
            // is the sole drop, run exactly once — and the arena bytes are still
            // mapped (we run the list *before* `bump.reset()`). No live borrow of
            // the value can exist: `reset`/drop take `&mut self`/own the context.
            unsafe { (entry.glue)(entry.addr) };
        }
    }
}

/// Monomorphized drop glue for `T`: run `T`'s destructor in place at `p`. The
/// caller (a `BumpDrop` registration site that knows `T`) hands the *pointer to
/// this function* to the type-erased backend; this is the only place `T`'s
/// `Drop` is invoked for an arena value.
///
/// # Safety
/// `p` must point to a live, properly-aligned, initialized `T` that has not been
/// (and will not otherwise be) dropped.
unsafe fn drop_glue<T>(addr: *mut u8) {
    // Reconstruct a usable pointer from the exposed-provenance address recorded
    // at registration, then run `T`'s destructor in place.
    let p = core::ptr::with_exposed_provenance_mut::<T>(addr as usize);
    core::ptr::drop_in_place(p);
}

/// No-op drop glue: the leaked value has no destructor to run (POD); reclamation
/// is purely the `bump.reset()` + wholesale counter-zero. Registering it still
/// asserts the context is `BumpDrop`.
unsafe fn drop_glue_noop(_addr: *mut u8) {}

/// Drop glue for a [`PgVec`] leaked into the arena that runs **only the
/// elements'** destructors, never the `Vec`'s own `deallocate`.
///
/// The arena owns the `Vec`'s buffer bytes (bump-allocated) and reclaims them
/// wholesale at `bump.reset()`; the buffer must *not* be freed individually
/// (bump has no per-chunk free, exactly as `bump.c`). Running the full
/// `Vec::drop` would call `Mcx::deallocate` on the leaked allocator handle —
/// the same `Mcx`-carried-in-a-leaked-value path that trips Stacked Borrows for
/// any leaked collection. Dropping the elements in place via the *current* `len`
/// (read from the still-live header in the arena) frees what needs freeing
/// without touching the allocator handle, which is both C-faithful and
/// Miri-clean.
///
/// # Safety
/// `addr` is the exposed-provenance address of a live, initialized
/// `PgVec<'mcx, T>` header in the arena (registered by [`arena_vec_in`]); its
/// element drops have not run and will not run elsewhere.
unsafe fn drop_glue_vec_elems<T>(addr: *mut u8) {
    // The header's allocator field (`Mcx<'mcx>` == `&MemoryContext`) is never
    // touched here — we only read `len`/`as_mut_ptr` and drop the `[T]`, none of
    // which dereference the allocator — so the concrete lifetime is irrelevant
    // and `'static` is a sound stand-in for the layout-identical header type.
    let header = core::ptr::with_exposed_provenance_mut::<PgVec<'static, T>>(addr as usize);
    // Read the data pointer and length off the live header, then drop the
    // initialized `[T]` in place. We deliberately do NOT drop the `Vec` header
    // itself (which would `deallocate` the buffer); the bump arena reclaims it.
    let v: &mut PgVec<'static, T> = &mut *header;
    let len = v.len();
    let data: *mut T = v.as_mut_ptr();
    // Setting len to 0 first guards against a re-entrant double-drop if an
    // element's destructor somehow observes the header.
    v.set_len(0);
    core::ptr::drop_in_place(core::ptr::slice_from_raw_parts_mut(data, len));
}

/// `ALLOCSET_DEFAULT_INITSIZE` (memutils.h) — `bump.c` keeper/first block size.
const BUMP_INIT_BLOCK_SIZE: usize = 8 * 1024;
/// `ALLOCSET_DEFAULT_MAXSIZE` (memutils.h) — max block size before a request is
/// served from a dedicated (large) block.
const BUMP_MAX_BLOCK_SIZE: usize = 8 * 1024 * 1024;
/// `Bump_BLOCKHDRSZ` analog: per-block bookkeeping overhead counted into
/// `mem_allocated`. `MAXALIGN(sizeof(BumpBlock))` is small and fixed; the exact
/// value is immaterial to the SRF (which only observes `total_bytes > 0` /
/// `free_bytes > 0`), but counting it keeps `totalspace` strictly above the
/// requested-byte sum the way `bump.c` does.
const BUMP_BLOCK_HDR_SZ: usize = 40;

/// A faithful model of `bump.c`'s block list: enough to report
/// `context->mem_allocated` (total block bytes), the block count, and the
/// current (head) block's remaining free space. Mirrors `BumpAlloc` /
/// `BumpAllocLarge` / `BumpAllocFromNewBlock` block decisions without holding
/// the actual memory (bumpalo owns that).
struct BumpBlocks {
    /// `context->mem_allocated` — total bytes of all blocks (incl. headers).
    mem_allocated: usize,
    /// Number of blocks currently allocated (== `bump.c`'s block-list length).
    nblocks: usize,
    /// Remaining free bytes in the current (head) block — the block new small
    /// chunks are bump-allocated from.
    head_free: usize,
    /// `set->allocChunkLimit`: requests larger than this get a dedicated block
    /// (`BumpAllocLarge`); see `BumpContextCreate`.
    alloc_chunk_limit: usize,
    /// `set->nextBlockSize`: size of the next non-large block (doubles up to
    /// `maxBlockSize`).
    next_block_size: usize,
}

impl BumpBlocks {
    /// `BumpContextCreate`: one keeper block of `initBlockSize`, and the
    /// `allocChunkLimit` halving loop.
    fn new() -> Self {
        // allocChunkLimit = Min(maxBlockSize, MEMORYCHUNK_MAX_VALUE); halve while
        // (allocChunkLimit + CHUNKHDRSZ) > (maxBlockSize - BLOCKHDRSZ) / 8.
        let mut alloc_chunk_limit = BUMP_MAX_BLOCK_SIZE;
        let bound = (BUMP_MAX_BLOCK_SIZE - BUMP_BLOCK_HDR_SZ) / 8;
        while alloc_chunk_limit > bound {
            alloc_chunk_limit >>= 1;
        }
        BumpBlocks {
            // Keeper block allocated with the context (initBlockSize).
            mem_allocated: BUMP_INIT_BLOCK_SIZE,
            nblocks: 1,
            head_free: BUMP_INIT_BLOCK_SIZE - BUMP_BLOCK_HDR_SZ,
            alloc_chunk_limit,
            // BumpContextCreate sets nextBlockSize = initBlockSize.
            next_block_size: BUMP_INIT_BLOCK_SIZE,
        }
    }

    /// `BumpAlloc(context, size)` block bookkeeping for a chunk of `size` bytes.
    fn alloc(&mut self, size: usize) {
        // MAXALIGN(size); CHUNKHDRSZ is 0 in a non-checking build.
        let chunk_size = (size + 7) & !7;

        if chunk_size > self.alloc_chunk_limit {
            // BumpAllocLarge: a dedicated, completely-full block (no free space).
            let blksize = chunk_size + BUMP_BLOCK_HDR_SZ;
            self.mem_allocated += blksize;
            self.nblocks += 1;
            // The current (head) block is unchanged — its free space stays.
            return;
        }

        if chunk_size <= self.head_free {
            // Fits in the current block; bump the freeptr.
            self.head_free -= chunk_size;
            return;
        }

        // BumpAllocFromNewBlock: grow nextBlockSize toward maxBlockSize until the
        // chunk fits, allocate the new block, make it the head.
        let mut blksize = self.next_block_size;
        let required = chunk_size + BUMP_BLOCK_HDR_SZ;
        while blksize < required {
            blksize = (blksize * 2).min(BUMP_MAX_BLOCK_SIZE);
            if blksize >= required {
                break;
            }
            if blksize == BUMP_MAX_BLOCK_SIZE {
                // Can't grow further; the block must still fit the chunk.
                blksize = required;
                break;
            }
        }
        self.mem_allocated += blksize;
        self.nblocks += 1;
        self.head_free = blksize - BUMP_BLOCK_HDR_SZ - chunk_size;
        // nextBlockSize doubles, capped at maxBlockSize.
        self.next_block_size = (self.next_block_size * 2).min(BUMP_MAX_BLOCK_SIZE);
    }

    /// `BumpReset`: keep the keeper block, drop the rest.
    fn reset(&mut self) {
        *self = BumpBlocks::new();
    }
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
    /// Hot-path cache: `true` iff this context **or any ancestor** carries a
    /// finite `limit`. When `false`, `charge`'s limit-validation ancestor walk
    /// is provably a no-op (nothing on the path to the root can reject the
    /// charge) and is skipped — the common case, since default contexts are
    /// unlimited. Computed at creation from the parent's flag-or-limit; a
    /// `with_limit` setting a finite limit sets it on the context (which, per
    /// the `with_limit` contract, has no children yet, so no descendant flags
    /// need updating). This caches the path predicate so the common allocate
    /// path never re-walks ancestors just to discover nothing is limited.
    limited_path: Cell<bool>,
    /// Backend arena footprint (`bump.c` block bytes), snapshotted on each bump
    /// allocate/grow. `0` for malloc-backed contexts (their footprint == the
    /// per-chunk `self_used`). This lives on `Acct` (not just on the backend in
    /// `MemoryContext`) so the accounting-tree walk `stats_tree()` — which has
    /// only `Acct`, not the owning `MemoryContext` — can report a bump context's
    /// real `totalspace`/`freespace`/`nblocks` the way C's `BumpStats` does.
    arena_footprint: Cell<usize>,
    /// Backend block count (`bump.c` `nblocks`), snapshotted alongside
    /// `arena_footprint`. `0` for malloc.
    arena_nblocks: Cell<usize>,
    /// `true` for a `bump.c`-backed context — the stats walk (which sees only
    /// `Acct`) reports `type` = `"Bump"` and sources
    /// `totalspace`/`freespace`/`nblocks` from the block model.
    is_bump: bool,
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
    /// AllocSet-backed root context (`AllocSetContextCreate` semantics): the
    /// default block-pooling allocator (`aset.c`).
    pub fn new(name: &'static str) -> Self {
        Self::with_backend(name, Backend::Aset(RefCell::new(aset::AllocSet::new())), None)
    }

    /// Bump-arena root context (`BumpContextCreate` semantics): per-chunk free
    /// is a no-op; memory is reclaimed wholesale by [`reset`](Self::reset) /
    /// drop.
    pub fn new_bump(name: &'static str) -> Self {
        Self::with_backend(name, Backend::Bump(bumpalo::Bump::new(), RefCell::new(BumpBlocks::new())), None)
    }

    /// Child context for accounting purposes: its allocations also count
    /// toward (and are limited by) this context and its ancestors, and it
    /// appears in [`stats_tree`](Self::stats_tree). The child is an
    /// independently owned value — *cleanup* nesting is expressed by storing
    /// it in the state struct that should own it, not by this link.
    pub fn new_child(&self, name: &'static str) -> MemoryContext {
        Self::with_backend(
            name,
            Backend::Aset(RefCell::new(aset::AllocSet::new())),
            Some(self.acct.clone()),
        )
    }

    /// [`new_child`](Self::new_child) with a bump backend.
    pub fn new_child_bump(&self, name: &'static str) -> MemoryContext {
        Self::with_backend(name, Backend::Bump(bumpalo::Bump::new(), RefCell::new(BumpBlocks::new())), Some(self.acct.clone()))
    }

    /// Drop-aware bump-arena root context: bump-allocates like
    /// [`new_bump`](Self::new_bump), but owned values registered via
    /// [`arena_box_in`]/[`arena_vec_in`]/[`arena_string_in`] have their
    /// destructor run **once at reset/drop** (LIFO) instead of per-object. C's
    /// `AllocSetReset` model for owned Rust data. Opt-in; never a default.
    pub fn new_bumpdrop(name: &'static str) -> Self {
        Self::with_backend(
            name,
            Backend::BumpDrop(
                bumpalo::Bump::new(),
                RefCell::new(BumpBlocks::new()),
                RefCell::new(DropList::new()),
            ),
            None,
        )
    }

    /// [`new_child`](Self::new_child) with the drop-aware bump backend
    /// ([`new_bumpdrop`](Self::new_bumpdrop)).
    pub fn new_child_bumpdrop(&self, name: &'static str) -> MemoryContext {
        Self::with_backend(
            name,
            Backend::BumpDrop(
                bumpalo::Bump::new(),
                RefCell::new(BumpBlocks::new()),
                RefCell::new(DropList::new()),
            ),
            Some(self.acct.clone()),
        )
    }

    fn with_backend(
        name: &'static str,
        backend: Backend,
        parent: Option<alloc::rc::Rc<Acct>>,
    ) -> Self {
        // A bump context starts with its keeper block (BumpContextCreate),
        // present even before any allocation — so an empty "Caller tuples"
        // already reports nblocks=1 and a positive footprint, as C does.
        let (is_bump, init_footprint, init_nblocks) = match &backend {
            // Aset reports like the old malloc backend for stats (its real block
            // footprint is tracked but not surfaced, keeping the stats SRF output
            // unchanged); the lazy keeper means an empty context holds 0 blocks.
            Backend::Aset(_) => (false, 0usize, 0usize),
            Backend::Malloc => (false, 0usize, 0usize),
            Backend::Bump(_, blocks) | Backend::BumpDrop(_, blocks, _) => {
                let b = blocks.borrow();
                (true, b.mem_allocated, b.nblocks)
            }
        };
        // A fresh context is itself unlimited (`with_limit` may set a finite
        // limit after); it is on a limited path iff its parent already is, or
        // the parent carries a finite limit of its own.
        let limited_path = parent.as_ref().is_some_and(|p| {
            p.limited_path.get() || p.limit.get() != usize::MAX
        });
        let acct = alloc::rc::Rc::new(Acct {
            name,
            ident: RefCell::new(None),
            self_used: Cell::new(0),
            subtree_used: Cell::new(0),
            self_peak: Cell::new(0),
            subtree_peak: Cell::new(0),
            limit: Cell::new(usize::MAX),
            limited_path: Cell::new(limited_path),
            arena_footprint: Cell::new(init_footprint),
            arena_nblocks: Cell::new(init_nblocks),
            is_bump,
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
    ///
    /// Contract: call this at construction time, before creating any accounting
    /// child of this context (the sole caller pattern). It updates the
    /// `limited_path` cache for *this* node so the charge fast-path is correct;
    /// children created afterward inherit a now-finite limit through
    /// [`with_backend`]'s parent check. (Setting a finite limit on a context
    /// that already has descendants would leave their cached `limited_path`
    /// stale; that is not a supported usage and is `debug_assert`ed against.)
    pub fn with_limit(self, limit: usize) -> Self {
        debug_assert!(
            self.acct.children.borrow().iter().all(|w| w.strong_count() == 0),
            "with_limit must be set before creating children (limited_path cache would go stale)",
        );
        // Setting a finite limit puts this node itself on a limited path, so the
        // charge fast-path will perform the validation walk for it.
        if limit != usize::MAX {
            self.acct.limited_path.set(true);
        }
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
        // `BumpDrop` is the C `AllocSetReset` model for *owned* values: live
        // collections are still charged at reset (their `Drop` was suppressed —
        // they are reclaimed wholesale here), so the "everything already dropped"
        // assertion does NOT apply. For `Aset`/`Malloc`/`Bump` it still does:
        // those require every collection to have dropped (returning its bytes)
        // before reset, so a nonzero `self_used` signals a genuine leak.
        if !matches!(self.backend, Backend::BumpDrop(..)) {
            debug_assert_eq!(
                self.acct.self_used.get(),
                0,
                "context {:?} reset with {} bytes still charged (leaked allocation?)",
                self.acct.name,
                self.acct.self_used.get(),
            );
        }
        if let Backend::Aset(set) = &mut self.backend {
            // AllocSetReset: free every block but the keeper, empty freelists.
            set.get_mut().reset();
        }
        if let Backend::Bump(bump, blocks) = &mut self.backend {
            bump.reset();
            blocks.get_mut().reset();
        }
        if let Backend::BumpDrop(bump, blocks, droplist) = &mut self.backend {
            // §3.2/§3.3: run every registered destructor LIFO FIRST (while the
            // arena bytes the entries point into are still mapped), THEN reclaim
            // the bytes wholesale, THEN zero the counters below. Order is
            // load-bearing — running glue after `bump.reset()` would touch freed
            // memory.
            droplist.get_mut().run();
            bump.reset();
            blocks.get_mut().reset();
        }
        // §3.3: under `BumpDrop`, per-object `uncharge` never ran (each value's
        // `Drop` was suppressed), so the whole live charge is released here in a
        // single counter-zero — propagated to ancestors so `subtree_used`
        // (work_mem/spill input) stays exact.
        let residual = self.acct.self_used.get();
        if matches!(self.backend, Backend::BumpDrop(..)) && residual > 0 {
            for node in self.acct.ancestors() {
                node.subtree_used
                    .set(node.subtree_used.get().saturating_sub(residual));
            }
            self.acct.self_used.set(0);
        }
        // Reset the block-footprint snapshot the accounting tree reads.
        self.acct.arena_footprint.set(0);
        self.acct.arena_nblocks.set(0);
        if let Backend::Bump(_, blocks) | Backend::BumpDrop(_, blocks, _) = &self.backend {
            let b = blocks.borrow();
            self.acct.arena_footprint.set(b.mem_allocated);
            self.acct.arena_nblocks.set(b.nblocks);
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
                // Aset mirrors the old malloc backend's stats (requested bytes),
                // keeping pg_backend_memory_contexts output unchanged.
                Backend::Aset(_) | Backend::Malloc => self.acct.self_used.get(),
                Backend::Bump(_, blocks) | Backend::BumpDrop(_, blocks, _) => {
                    blocks.borrow().mem_allocated
                }
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
                is_bump: acct.is_bump,
                arena_footprint: acct.arena_footprint.get(),
                nblocks: acct.arena_nblocks.get(),
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

    /// Register `(ptr, glue)` on this context's drop list, to run LIFO at
    /// reset/drop. Only meaningful for a [`BumpDrop`](Backend::BumpDrop) context;
    /// for any other backend this is a no-op (values there are dropped
    /// individually through the `Allocator`, not via the drop list).
    ///
    /// # Safety
    /// `ptr` must point to a live `T` (the type `glue == drop_glue::<T>` was
    /// monomorphized for) allocated in *this* context's arena and whose own
    /// `Drop` has been suppressed (leaked), so the registered glue is the unique
    /// owner that runs `T::drop`. The value must remain valid until this context
    /// is reset/dropped (guaranteed by the `'mcx` borrow on the returned
    /// reference and `reset`'s `&mut self`).
    unsafe fn register_drop(&self, addr: *mut u8, glue: unsafe fn(*mut u8)) -> bool {
        if let Backend::BumpDrop(_, _, droplist) = &self.backend {
            droplist.borrow_mut().entries.push(DropEntry { addr, glue });
            true
        } else {
            false
        }
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
    /// ancestor's subtree counter. The validation pass is elided when neither
    /// this context nor any ancestor carries a finite limit (the cached
    /// `limited_path` flag, the common case) — there is then nothing on the
    /// path to the root that could reject the charge, and `subtree_used` cannot
    /// overflow `usize::MAX` without a finite ceiling first being hit. When a
    /// limit exists on the path, the two-pass form is kept (validate everything
    /// first, so a failure applies nothing). The single-node (root, no parent)
    /// case skips the ancestor iterator entirely.
    fn charge(&self, n: usize) -> Result<(), AllocError> {
        if self.acct.limited_path.get() {
            for node in self.acct.ancestors() {
                let new = node.subtree_used.get().checked_add(n).ok_or(AllocError)?;
                if new > node.limit.get() {
                    return Err(AllocError);
                }
            }
        }
        let acct = &*self.acct;
        let self_new = acct.self_used.get() + n;
        acct.self_used.set(self_new);
        if self_new > acct.self_peak.get() {
            acct.self_peak.set(self_new);
        }
        if acct.parent.is_none() {
            // Root context: no ancestors to propagate to, so update this one
            // node's subtree counter directly (still incremental — a root may
            // have accounting descendants whose bytes are already included)
            // rather than spinning up the ancestor iterator.
            let new = acct.subtree_used.get() + n;
            acct.subtree_used.set(new);
            if new > acct.subtree_peak.get() {
                acct.subtree_peak.set(new);
            }
            return Ok(());
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
        let acct = &*self.acct;
        debug_assert!(
            acct.self_used.get() >= n,
            "context {:?} uncharging {} with only {} charged",
            acct.name,
            n,
            acct.self_used.get(),
        );
        acct.self_used.set(acct.self_used.get().saturating_sub(n));
        if acct.parent.is_none() {
            // Root: subtree counter mirrors self_used exactly.
            acct.subtree_used.set(acct.subtree_used.get().saturating_sub(n));
            return;
        }
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
        // For a `BumpDrop` context, every owned value's `Drop` was suppressed at
        // construction (leaked into the arena); their destructors run here, LIFO,
        // before the bump bytes go away with `self`. Without this the arena
        // values' destructors would never run (a leak of any owned resource they
        // hold — `PgVec`/`PgString` element drops, `Box<dyn …>`, etc.).
        if let Backend::BumpDrop(_, _, droplist) = &self.backend {
            droplist.borrow_mut().run();
        }
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
///
/// The bump-arena footprint is surfaced here (snapshotted into `Acct` on each
/// bump allocate/grow) so a tree walk that has only `Acct` — not the owning
/// `MemoryContext` that holds the backend — can still report a `bump.c`
/// context's real `totalspace`/`freespace`/`nblocks` (`MemoryContextStats` /
/// `pg_get_backend_memory_contexts`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeStats {
    pub name: &'static str,
    pub ident: Option<alloc::string::String>,
    pub used: usize,
    pub peak: usize,
    pub subtree_used: usize,
    pub subtree_peak: usize,
    pub limit: usize,
    /// `true` for a `bump.c`-backed context (`type` = `"Bump"` in the SRF;
    /// `totalspace`/`freespace`/`nblocks` come from the block model below).
    pub is_bump: bool,
    /// `context->mem_allocated` for a bump context (total block bytes); `0` for
    /// malloc-backed (its footprint == `used`).
    pub arena_footprint: usize,
    /// `bump.c` block count (`nblocks`); `0` for malloc-backed.
    pub nblocks: usize,
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
            Backend::Aset(set) => set.borrow_mut().alloc(layout),
            Backend::Malloc => Global.allocate(layout),
            Backend::Bump(bump, blocks) | Backend::BumpDrop(bump, blocks, _) => {
                let r = bump.allocate(layout);
                if r.is_ok() {
                    // Model bump.c's block decision for this chunk and refresh
                    // the Acct footprint/nblocks snapshot the stats walk reads.
                    let mut b = blocks.borrow_mut();
                    b.alloc(layout.size());
                    self.0.acct.arena_footprint.set(b.mem_allocated);
                    self.0.acct.arena_nblocks.set(b.nblocks);
                }
                r
            }
        };
        if result.is_err() {
            self.0.uncharge(layout.size());
        }
        result
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        self.0.uncharge(layout.size());
        match &self.0.backend {
            // AllocSetFree: pooled chunk -> its size-class freelist; dedicated
            // chunk -> Global. The routing recomputes from `layout`, matching
            // `alloc`.
            Backend::Aset(set) => set.borrow_mut().dealloc(ptr, layout),
            Backend::Malloc => Global.deallocate(ptr, layout),
            // bump.c never frees individual chunks (reset reclaims wholesale), so
            // the block model is untouched on deallocate. (Reached only for
            // collections allocated into a BumpDrop context that are NOT routed
            // through the leaking `arena_*_in` API — those drop normally and the
            // matching uncharge above keeps `self_used` exact; the arena-leaked
            // values never call deallocate.)
            Backend::Bump(bump, _) | Backend::BumpDrop(bump, _, _) => {
                bump.deallocate(ptr, layout)
            }
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
            Backend::Aset(set) => set.borrow_mut().realloc(ptr, old_layout, new_layout),
            Backend::Malloc => Global.grow(ptr, old_layout, new_layout),
            Backend::Bump(bump, blocks) | Backend::BumpDrop(bump, blocks, _) => {
                let r = bump.grow(ptr, old_layout, new_layout);
                if r.is_ok() {
                    // bump.c can't grow in place; bumpalo bump-allocates the
                    // grown buffer. Charge only the incremental bytes to the
                    // block model (the moved-from bytes become dead arena space,
                    // already counted), so a growing collection doesn't spuriously
                    // multiply the block count.
                    let mut b = blocks.borrow_mut();
                    b.alloc(delta);
                    self.0.acct.arena_footprint.set(b.mem_allocated);
                    self.0.acct.arena_nblocks.set(b.nblocks);
                }
                r
            }
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
            Backend::Aset(set) => set.borrow_mut().realloc(ptr, old_layout, new_layout),
            Backend::Malloc => Global.shrink(ptr, old_layout, new_layout),
            Backend::Bump(bump, _) | Backend::BumpDrop(bump, _, _) => {
                bump.shrink(ptr, old_layout, new_layout)
            }
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

/// Take a value already boxed in a [`BumpDrop`](Backend::BumpDrop) context,
/// register its monomorphized destructor on the context's drop list, suppress
/// the box's own `Drop` (leak it into the arena), and hand back an honest
/// `&'mcx mut T`.
///
/// This is the collection-side half of the drop-aware arena: the type-erased
/// `Allocator` can't capture `T`'s destructor (it only sees `Layout`), but this
/// function — which statically knows `T` — registers `drop_glue::<T>`. The
/// value is then *not* freed individually (`Box::leak` forgets its `Drop`); it
/// lives in the arena until the context resets/drops, at which point the
/// registered glue runs its destructor exactly once — C's "freed with its
/// context" for owned Rust values.
///
/// The returned borrow is tied to `'mcx` (the context lifetime), so the borrow
/// checker still forbids use-after-reset exactly as for [`leak_in`].
///
/// Requires `b`'s context to be `BumpDrop` (the only backend with a drop list).
/// On any other backend the registration cannot happen; that is a programming
/// error — `debug_assert`ed — and in release the value would leak without its
/// destructor running (always memory-safe, never a double free). Use the
/// `arena_*_in` constructors below rather than calling this directly.
pub fn arena_leak<'mcx, T>(b: PgBox<'mcx, T>) -> &'mcx mut T {
    // Decompose the box into a raw `*mut T` (full-allocation provenance) plus the
    // context handle, suppressing the box's own `Drop`. We register the drop glue
    // against THIS raw pointer and then return `&mut *raw` — a *child* retag of
    // `raw`. Under Stacked Borrows a child Unique retag does not invalidate its
    // parent, so the pointer the drop list holds stays valid when the glue later
    // runs. (Deriving the stored pointer from the returned `&mut` instead — a
    // sibling — would be invalidated the moment the reference is used; that is
    // the Miri SB violation this ordering avoids.)
    let (raw, alloc): (*mut T, Mcx<'mcx>) =
        allocator_api2::boxed::Box::into_raw_with_allocator(b);
    // Expose `raw`'s provenance and record its address. The drop glue rebuilds a
    // usable pointer via `with_exposed_provenance_mut`. This severs the stored
    // pointer from the borrow stack so the returned `&mut T` can be used freely
    // without invalidating it.
    let addr = core::ptr::with_exposed_provenance_mut::<u8>(
        (raw as *mut u8).expose_provenance(),
    );
    // SAFETY: `raw` is the live, aligned `T` just yielded by `into_raw_with_
    // allocator`, allocated in `alloc`'s arena; the box's `Drop` is suppressed,
    // so the registered `drop_glue::<T>` is the unique destructor. The value
    // stays valid until the context resets/drops (the `'mcx` borrow returned
    // below enforces this statically).
    let registered = unsafe { alloc.context().register_drop(addr, drop_glue::<T>) };
    debug_assert!(
        registered || !core::mem::needs_drop::<T>(),
        "arena_leak: value of a Drop type leaked into a non-BumpDrop context \
         (its destructor will never run); use a BumpDrop context",
    );
    // SAFETY: `raw` points to a live, aligned, initialized `T`; we hand out a
    // single `&'mcx mut T` (no other reference to it exists — the box is
    // consumed) tied to the context lifetime. This borrow is a child of `raw`,
    // leaving the drop-list's copy of `raw` valid.
    unsafe { &mut *raw }
}

/// Allocate `value` in a [`BumpDrop`](Backend::BumpDrop) context, registering
/// its destructor to run at reset/drop, and return an honest `&'mcx mut T`.
/// The arena analog of [`alloc_in`] — bump-allocates, never frees individually.
pub fn arena_box_in<'mcx, T>(mcx: Mcx<'mcx>, value: T) -> PgResult<&'mcx mut T> {
    let b = alloc_in(mcx, value)?;
    Ok(arena_leak(b))
}

/// Build a [`PgVec`] in a [`BumpDrop`](Backend::BumpDrop) context, register the
/// *vec's own* `drop_in_place` (which drops every element then the buffer), and
/// leak it into an honest `&'mcx mut PgVec<'mcx, T>`. Pushing into the returned
/// vec bump-grows in the arena; the whole thing — elements included — is
/// reclaimed in one shot at reset/drop.
pub fn arena_vec_in<'mcx, T>(
    mcx: Mcx<'mcx>,
    vec: PgVec<'mcx, T>,
) -> PgResult<&'mcx mut PgVec<'mcx, T>> {
    // Box the Vec header in the arena and register *element-only* drop glue
    // (`drop_glue_vec_elems`): it drops the `[T]` in place via the header's
    // live len, but NEVER calls the Vec's own `deallocate` (the bump arena owns
    // the buffer bytes and reclaims them at reset — `bump.c` has no per-chunk
    // free). This is both C-faithful and avoids the `Mcx`-carried-in-a-leaked-
    // value deallocate path that trips Stacked Borrows.
    let b = alloc_in(mcx, vec)?;
    let (raw, alloc): (*mut PgVec<'mcx, T>, Mcx<'mcx>) =
        allocator_api2::boxed::Box::into_raw_with_allocator(b);
    let addr =
        core::ptr::with_exposed_provenance_mut::<u8>((raw as *mut u8).expose_provenance());
    // SAFETY: `raw` is the live, aligned `PgVec` header just leaked into `alloc`'s
    // arena; its elements have not been dropped and the element-only glue is the
    // unique destructor for them. Valid until reset/drop (the `'mcx` borrow
    // returned enforces this statically).
    let registered = unsafe { alloc.context().register_drop(addr, drop_glue_vec_elems::<T>) };
    debug_assert!(
        registered || !core::mem::needs_drop::<T>(),
        "arena_vec_in: Vec of a Drop element type leaked into a non-BumpDrop \
         context (element destructors will never run); use a BumpDrop context",
    );
    // SAFETY: single `&'mcx mut` to the live header; child retag of `raw`,
    // leaving the drop list's exposed address valid.
    Ok(unsafe { &mut *raw })
}

/// Build a [`PgString`] in a [`BumpDrop`](Backend::BumpDrop) context, register
/// its destructor (element-only, like [`arena_vec_in`]), and leak it into an
/// honest `&'mcx mut PgString<'mcx>`. A `PgString`'s elements are POD `u8`, so
/// there is nothing to destruct — the registration is what reclaims accounting
/// at reset; the byte buffer dies with the arena.
pub fn arena_string_in<'mcx>(
    mcx: Mcx<'mcx>,
    s: PgString<'mcx>,
) -> PgResult<&'mcx mut PgString<'mcx>> {
    // PgString is a thin wrapper over PgVec<u8>; its only Drop is the inner
    // Vec's, whose elements (u8) are Drop-trivial — so there is nothing to
    // destruct, the byte buffer dies with the arena, and accounting is reclaimed
    // by the wholesale counter-zero at reset. We register a no-op element-glue
    // entry (`drop_glue_vec_elems::<u8>` over the inner buffer, which drops 0
    // elements) purely so the BumpDrop-context guard fires on misuse and the
    // path stays uniform with `arena_vec_in`.
    let b = alloc_in(mcx, s)?;
    let raw: *mut PgString<'mcx> =
        allocator_api2::boxed::Box::into_raw_with_allocator(b).0;
    let addr =
        core::ptr::with_exposed_provenance_mut::<u8>((raw as *mut u8).expose_provenance());
    // SAFETY: the only Drop a PgString carries is its inner Vec<u8>'s, whose u8
    // elements are POD — there is genuinely nothing to destruct. Register a
    // no-op glue (so the BumpDrop guard still fires); the byte buffer and the
    // header are reclaimed wholesale at `bump.reset()`.
    let registered = unsafe { mcx.context().register_drop(addr, drop_glue_noop) };
    debug_assert!(registered, "arena_string_in: use a BumpDrop context");
    // SAFETY: single `&'mcx mut` to the live header; child retag of `raw`.
    Ok(unsafe { &mut *raw })
}

/// Move the value out of a [`PgBox`] **without** invoking the box allocator's
/// `deallocate`, leaking the box's backing storage.
///
/// [`PgBox::into_inner`]/[`allocator_api2::boxed::Box::into_inner`] move the value
/// out *and* call `Mcx::deallocate` against the allocator captured in the box.
/// That is unsound when the captured allocator is a `MemoryContext` that has
/// already been reset/freed (e.g. a sub-`Query` carrier built in a transient
/// parse/analyze context, taken apart later in the planner): `deallocate`
/// dereferences the dangling context and faults. C never `pfree`s such nodes
/// individually — they are reclaimed wholesale when their context resets — so
/// leaking the one box's storage here is the faithful behavior. This decomposes
/// the box via `into_raw_with_allocator`, `ptr::read`s the value out (a move),
/// and drops the (possibly dangling) allocator handle without ever calling it.
pub fn box_into_inner_leak<'mcx, T>(b: PgBox<'mcx, T>) -> T {
    let (raw, _alloc) = allocator_api2::boxed::Box::into_raw_with_allocator(b);
    // SAFETY: `raw` is the live, aligned, non-null data pointer just yielded by
    // `into_raw_with_allocator`; we read the `T` out exactly once and never touch
    // `raw` again (its storage is leaked, not freed). `_alloc` is a `Copy`-style
    // `Mcx` reference handle whose drop is a no-op — it is never dereferenced.
    unsafe { core::ptr::read(raw) }
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

/// Move a concrete payload `P` out of an unsized `PgBox<'mcx, U>` (a trait
/// object) and free the box's backing allocation **without** dropping `P`.
///
/// This is the move-out dual of [`box_unsize_dyn`]: it takes the fat box plus a
/// thin `*const P` data pointer (the `repr(transparent)` payload address inside
/// the trait object — the same address the downcast uses), `ptr::read`s the `P`
/// value out of it, then deallocates the box's storage with the box's own
/// allocator using the trait object's *runtime* layout (`Layout::for_value`,
/// which for a `repr(transparent)` adapter over `P` equals `Layout::new::<P>()`).
/// The box's `Drop` is suppressed (`ManuallyDrop`) so the value is not dropped a
/// second time after the move.
///
/// # Safety
/// - `data` MUST be the data pointer of the payload stored in `sized` (i.e. the
///   `repr(transparent)` adapter's address, == the box's data address), and the
///   concrete stored type MUST have the same size/align as `P` (guaranteed by the
///   generator's tag<->adapter bijection + `repr(transparent)`).
/// - The caller MUST have already established (via a tag check) that the runtime
///   type is `P`.
pub unsafe fn box_read_payload<'mcx, P, U>(sized: PgBox<'mcx, U>, data: *const P) -> P
where
    P: 'mcx,
    U: ?Sized + 'mcx,
{
    // Decompose: take the fat raw pointer + the allocator, suppress the box drop.
    let (raw, alloc) = allocator_api2::boxed::Box::into_raw_with_allocator(sized);
    // Runtime layout of the (unsized) value — for a transparent adapter over P
    // this is exactly Layout::new::<P>(), but read it off the live value so it is
    // correct regardless.
    let layout = core::alloc::Layout::for_value(unsafe { &*raw });
    // Read the payload out of its data address (the move). After this the storage
    // contains a logically-uninitialized P that we must NOT drop.
    let value = unsafe { core::ptr::read(data) };
    // Free the box's backing storage with its own allocator. The dyn value is
    // not dropped (we moved P out and never run U's drop glue).
    if layout.size() != 0 {
        let nn = core::ptr::NonNull::new(raw as *mut u8)
            .expect("box_read_payload: box raw pointer was null");
        unsafe { allocator_api2::alloc::Allocator::deallocate(&alloc, nn, layout) };
    }
    value
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
