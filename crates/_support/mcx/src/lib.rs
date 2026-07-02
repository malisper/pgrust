// Memory contexts with allocator-tied lifetimes; no ambient current context.

#![no_std]

extern crate alloc;

use core::alloc::Layout;
use core::cell::{Cell, RefCell};
use core::fmt;
use core::ptr::NonNull;

pub use allocator_api2::alloc::Allocator;

use allocator_api2::alloc::{AllocError, Global};
use ::types_error::{PgError, PgResult, ERRCODE_OUT_OF_MEMORY};

mod arena_safe;
mod aset;
mod owned;
mod string;
pub use arena_safe::ArenaSafe;
pub use aset::alloc_stats;
pub use owned::{Bind, McxOwned};
pub use string::PgString;

/// # Safety
/// Caller asserts the full [`ArenaSafe`] contract; the field list is only a guard.
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

enum Backend {
    // UnsafeCell, not RefCell: palloc's hot path pays no borrow flag (see aset_mut).
    Aset(core::cell::UnsafeCell<aset::AllocSet>),
    Malloc,
    Bump(bumpalo::Bump, RefCell<BumpBlocks>),
    // Bump + drop list: leaked owned values run their destructor once at reset.
    BumpDrop(bumpalo::Bump, RefCell<BumpBlocks>, RefCell<DropList>),
    BumpForget(bumpalo::Bump, RefCell<BumpBlocks>),
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

    // Pop-before-run: a panicking destructor leaks unrun entries, never double-runs.
    fn run(&mut self) {
        while let Some(entry) = self.entries.pop() {
            // SAFETY: live value of glue's type, leaked with Drop suppressed; sole
            // drop, before bump.reset(), no live borrows.
            unsafe { (entry.glue)(entry.addr) };
        }
    }
}

/// # Safety
/// `addr` must be an exposed, live, aligned `T` never otherwise dropped.
unsafe fn drop_glue<T>(addr: *mut u8) {
    let p = core::ptr::with_exposed_provenance_mut::<T>(addr as usize);
    core::ptr::drop_in_place(p);
}

unsafe fn drop_glue_noop(_addr: *mut u8) {}

// Element-only glue for a leaked PgVec: never Vec::drop (deallocate through the leaked Mcx is the SB trap).
/// # Safety
/// `addr`: exposed address of a live arena `PgVec` header, element drops unrun.
unsafe fn drop_glue_vec_elems<T>(addr: *mut u8) {
    let header = core::ptr::with_exposed_provenance_mut::<PgVec<'static, T>>(addr as usize);
    let v: &mut PgVec<'static, T> = &mut *header;
    let len = v.len();
    let data: *mut T = v.as_mut_ptr();
    // len = 0 first guards a re-entrant double-drop.
    v.set_len(0);
    core::ptr::drop_in_place(core::ptr::slice_from_raw_parts_mut(data, len));
}

const BUMP_INIT_BLOCK_SIZE: usize = 8 * 1024;
const BUMP_MAX_BLOCK_SIZE: usize = 8 * 1024 * 1024;
const BUMP_BLOCK_HDR_SZ: usize = 40;

struct BumpBlocks {
    mem_allocated: usize,
    nblocks: usize,
    head_free: usize,
    alloc_chunk_limit: usize,
    next_block_size: usize,
}

impl BumpBlocks {
    fn new() -> Self {
        let mut alloc_chunk_limit = BUMP_MAX_BLOCK_SIZE;
        let bound = (BUMP_MAX_BLOCK_SIZE - BUMP_BLOCK_HDR_SZ) / 8;
        while alloc_chunk_limit > bound {
            alloc_chunk_limit >>= 1;
        }
        BumpBlocks {
            mem_allocated: BUMP_INIT_BLOCK_SIZE,
            nblocks: 1,
            head_free: BUMP_INIT_BLOCK_SIZE - BUMP_BLOCK_HDR_SZ,
            alloc_chunk_limit,
            next_block_size: BUMP_INIT_BLOCK_SIZE,
        }
    }

    fn alloc(&mut self, size: usize) {
        let chunk_size = (size + 7) & !7;

        if chunk_size > self.alloc_chunk_limit {
            let blksize = chunk_size + BUMP_BLOCK_HDR_SZ;
            self.mem_allocated += blksize;
            self.nblocks += 1;
            return;
        }

        if chunk_size <= self.head_free {
            self.head_free -= chunk_size;
            return;
        }

        let mut blksize = self.next_block_size;
        let required = chunk_size + BUMP_BLOCK_HDR_SZ;
        while blksize < required {
            blksize = (blksize * 2).min(BUMP_MAX_BLOCK_SIZE);
            if blksize >= required {
                break;
            }
            if blksize == BUMP_MAX_BLOCK_SIZE {
                blksize = required;
                break;
            }
        }
        self.mem_allocated += blksize;
        self.nblocks += 1;
        self.head_free = blksize - BUMP_BLOCK_HDR_SZ - chunk_size;
        self.next_block_size = (self.next_block_size * 2).min(BUMP_MAX_BLOCK_SIZE);
    }

    fn reset(&mut self) {
        *self = BumpBlocks::new();
    }
}

// Subtree totals summed on demand (C's recursive MemoryContextMemAllocated); charge never walks ancestors.
struct Acct {
    name: &'static str,
    ident: RefCell<Option<alloc::string::String>>,
    self_used: Cell<usize>,
    self_peak: Cell<usize>,
    limit: Cell<usize>,
    limited_path: Cell<bool>,
    arena_footprint: Cell<usize>,
    arena_nblocks: Cell<usize>,
    is_bump: bool,
    // bump.c: no BumpFree — frees don't uncharge until reset (see with_accounted_free).
    uncharge_on_free: Cell<bool>,
    parent: Option<AcctRc>,
    children: RefCell<alloc::vec::Vec<AcctWeak>>,
}

impl AcctRc {
    fn ancestors(&self) -> impl Iterator<Item = &Acct> {
        let mut cur: Option<&Acct> = Some(&**self);
        core::iter::from_fn(move || {
            let node = cur?;
            cur = node.parent.as_deref();
            Some(node)
        })
    }
}

impl Acct {
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

struct AcctPool {
    stack: UnsafeCell<alloc::vec::Vec<NonNull<AcctInner>>>,
}
// SAFETY: the single-threaded backend never accesses `stack` concurrently.
unsafe impl Sync for AcctPool {}
static ACCT_POOL: AcctPool = AcctPool { stack: UnsafeCell::new(alloc::vec::Vec::new()) };

impl AcctPool {
    #[inline]
    fn with<R>(&self, f: impl FnOnce(&mut alloc::vec::Vec<NonNull<AcctInner>>) -> R) -> R {
        // SAFETY: single-threaded backend — no other live access to `stack`.
        f(unsafe { &mut *self.stack.get() })
    }
}

#[inline]
fn acct_take() -> NonNull<AcctInner> {
    #[cfg(not(test))]
    {
        return acct_take_from(&ACCT_POOL);
    }
    #[cfg(test)]
    acct_alloc_global()
}

#[inline]
fn acct_give(p: NonNull<AcctInner>) {
    #[cfg(not(test))]
    {
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
    if let Some(p) = pool.with(|s| s.pop()) {
        return p;
    }
    acct_alloc_global()
}

#[inline]
fn acct_give_to(pool: &AcctPool, p: NonNull<AcctInner>) {
    let full = pool.with(|s| {
        if s.len() >= ACCT_POOL_MAX {
            true
        } else {
            s.push(p);
            false
        }
    });
    if full {
        // SAFETY: from acct_alloc_global; `val` already dropped, nothing else owns it.
        unsafe { Global.deallocate(p.cast(), Layout::new::<AcctInner>()) };
    }
}

struct BumpPool {
    stack: UnsafeCell<alloc::vec::Vec<bumpalo::Bump>>,
}
// SAFETY: single-threaded backend, as ACCT_POOL.
unsafe impl Sync for BumpPool {}
static BUMP_POOL: BumpPool = BumpPool { stack: UnsafeCell::new(alloc::vec::Vec::new()) };

const BUMP_POOL_MAX: usize = 8;
const BUMP_POOL_MAX_RETAINED: usize = 1 << 20;

impl BumpPool {
    #[inline]
    fn with<R>(&self, f: impl FnOnce(&mut alloc::vec::Vec<bumpalo::Bump>) -> R) -> R {
        // SAFETY: single-threaded backend — no other live access to `stack`.
        f(unsafe { &mut *self.stack.get() })
    }
}

#[inline]
fn bump_take() -> bumpalo::Bump {
    #[cfg(not(test))]
    {
        return bump_take_from(&BUMP_POOL);
    }
    #[cfg(test)]
    bumpalo::Bump::new()
}

#[inline]
fn bump_give(b: bumpalo::Bump) {
    #[cfg(not(test))]
    {
        bump_give_to(&BUMP_POOL, b);
    }
    #[cfg(test)]
    drop(b);
}

#[inline]
fn bump_take_from(pool: &BumpPool) -> bumpalo::Bump {
    pool.with(|s| s.pop()).unwrap_or_default()
}

#[inline]
fn bump_give_to(pool: &BumpPool, mut b: bumpalo::Bump) {
    b.reset();
    if b.chunk_capacity() <= BUMP_POOL_MAX_RETAINED {
        let parked = pool.with(|s| {
            if s.len() < BUMP_POOL_MAX {
                s.push(b);
                true
            } else {
                false
            }
        });
        if parked {
            return;
        }
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

pub struct MemoryContext {
    acct: AcctRc,
    backend: Backend,
    reset_cbs: RefCell<alloc::vec::Vec<alloc::boxed::Box<dyn FnOnce()>>>,
}

impl MemoryContext {
    pub fn new(name: &'static str) -> Self {
        Self::with_backend(name, Backend::Aset(core::cell::UnsafeCell::new(aset::AllocSet::new())), None)
    }

    pub fn new_bump(name: &'static str) -> Self {
        Self::with_backend(name, Backend::Bump(bump_take(), RefCell::new(BumpBlocks::new())), None)
    }

    pub fn new_child(&self, name: &'static str) -> MemoryContext {
        Self::with_backend(
            name,
            Backend::Aset(core::cell::UnsafeCell::new(aset::AllocSet::new())),
            Some(self.acct.clone()),
        )
    }

    pub fn new_child_bump(&self, name: &'static str) -> MemoryContext {
        Self::with_backend(name, Backend::Bump(bump_take(), RefCell::new(BumpBlocks::new())), Some(self.acct.clone()))
    }

    pub fn new_bumpdrop(name: &'static str) -> Self {
        Self::with_backend(
            name,
            Backend::BumpDrop(
                bump_take(),
                RefCell::new(BumpBlocks::new()),
                RefCell::new(DropList::new()),
            ),
            None,
        )
    }

    pub fn new_child_bumpdrop(&self, name: &'static str) -> MemoryContext {
        Self::with_backend(
            name,
            Backend::BumpDrop(
                bump_take(),
                RefCell::new(BumpBlocks::new()),
                RefCell::new(DropList::new()),
            ),
            Some(self.acct.clone()),
        )
    }

    pub fn new_bumpforget(name: &'static str) -> Self {
        Self::with_backend(
            name,
            Backend::BumpForget(bump_take(), RefCell::new(BumpBlocks::new())),
            None,
        )
    }

    pub fn new_child_bumpforget(&self, name: &'static str) -> MemoryContext {
        Self::with_backend(
            name,
            Backend::BumpForget(bump_take(), RefCell::new(BumpBlocks::new())),
            Some(self.acct.clone()),
        )
    }

    fn with_backend(
        name: &'static str,
        backend: Backend,
        parent: Option<AcctRc>,
    ) -> Self {
        let (is_bump, init_footprint, init_nblocks) = match &backend {
            Backend::Aset(_) => (false, 0usize, 0usize),
            Backend::Malloc => (false, 0usize, 0usize),
            Backend::Bump(_, blocks)
            | Backend::BumpDrop(_, blocks, _)
            | Backend::BumpForget(_, blocks) => {
                let b = blocks.borrow();
                (true, b.mem_allocated, b.nblocks)
            }
        };
        let limited_path = parent.as_ref().is_some_and(|p| {
            p.limited_path.get() || p.limit.get() != usize::MAX
        });
        let acct = AcctRc::new(Acct {
            name,
            ident: RefCell::new(None),
            self_used: Cell::new(0),
            self_peak: Cell::new(0),
            limit: Cell::new(usize::MAX),
            limited_path: Cell::new(limited_path),
            arena_footprint: Cell::new(init_footprint),
            arena_nblocks: Cell::new(init_nblocks),
            is_bump,
            uncharge_on_free: Cell::new(!is_bump),
            parent,
            children: RefCell::new(alloc::vec::Vec::new()),
        });
        if let Some(p) = &acct.parent {
            let mut children = p.children.borrow_mut();
            if children.len() == children.capacity() {
                children.retain(|w| w.strong_count() > 0);
            }
            children.push(acct.downgrade());
        }
        MemoryContext { acct, backend, reset_cbs: RefCell::new(alloc::vec::Vec::new()) }
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

    pub fn with_accounted_free(self) -> Self {
        self.acct.uncharge_on_free.set(true);
        self
    }

    pub fn mcx(&self) -> Mcx<'_> {
        Mcx(self)
    }

    pub fn name(&self) -> &'static str {
        self.acct.name
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
        self.reset_cbs.borrow_mut().push(alloc::boxed::Box::new(cb));
    }

    pub fn reset(&mut self) {
        self.fire_reset_callbacks();
        // Leak check only for exact-accounting backends.
        if !self.acct.is_bump
            || (matches!(self.backend, Backend::Bump(..)) && self.acct.uncharge_on_free.get())
        {
            debug_assert_eq!(
                self.acct.self_used.get(),
                0,
                "context {:?} reset with {} bytes still charged (leaked allocation?)",
                self.acct.name,
                self.acct.self_used.get(),
            );
        }
        if let Backend::Aset(set) = &mut self.backend {
            set.get_mut().reset();
        }
        if let Backend::Bump(bump, blocks) = &mut self.backend {
            bump.reset();
            blocks.get_mut().reset();
        }
        if let Backend::BumpDrop(bump, blocks, droplist) = &mut self.backend {
            // Run destructors BEFORE the bytes are reclaimed (order load-bearing).
            droplist.get_mut().run();
            bump.reset();
            blocks.get_mut().reset();
        }
        if let Backend::BumpForget(bump, blocks) = &mut self.backend {
            bump.reset();
            blocks.get_mut().reset();
        }
        if matches!(self.backend, Backend::BumpDrop(..) | Backend::BumpForget(..))
            || (matches!(self.backend, Backend::Bump(..)) && !self.acct.uncharge_on_free.get())
        {
            self.acct.self_used.set(0);
        }
        self.acct.arena_footprint.set(0);
        self.acct.arena_nblocks.set(0);
        if let Backend::Bump(_, blocks)
        | Backend::BumpDrop(_, blocks, _)
        | Backend::BumpForget(_, blocks) = &self.backend
        {
            let b = blocks.borrow();
            self.acct.arena_footprint.set(b.mem_allocated);
            self.acct.arena_nblocks.set(b.nblocks);
        }
        self.acct.self_peak.set(0);
    }

    pub fn stats(&self) -> ContextStats {
        ContextStats {
            name: self.acct.name,
            ident: self.ident(),
            used: self.acct.self_used.get(),
            peak: self.acct.self_peak.get(),
            subtree_used: self.acct.subtree_sum(),
            subtree_peak: self.acct.subtree_peak_sum(),
            limit: self.acct.limit.get(),
            arena_footprint: match &self.backend {
                Backend::Aset(_) | Backend::Malloc => self.acct.self_used.get(),
                Backend::Bump(_, blocks)
                | Backend::BumpDrop(_, blocks, _)
                | Backend::BumpForget(_, blocks) => blocks.borrow().mem_allocated,
            },
        }
    }

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
            let used = acct.self_used.get();
            let peak = acct.self_peak.get();
            let mut subtree_used = used;
            let mut subtree_peak = peak;
            for child in &children {
                subtree_used = subtree_used.saturating_add(child.subtree_used);
                subtree_peak = subtree_peak.saturating_add(child.subtree_peak);
            }
            TreeStats {
                name: acct.name,
                ident: acct.ident.borrow().clone(),
                used,
                peak,
                subtree_used,
                subtree_peak,
                limit: acct.limit.get(),
                is_bump: acct.is_bump,
                arena_footprint: acct.arena_footprint.get(),
                nblocks: acct.arena_nblocks.get(),
                children,
            }
        }
        node(&self.acct)
    }

    #[cold]
    pub fn oom(&self, request: usize) -> PgError {
        crate::oom_named(self.acct.name, request)
    }

    pub fn is_bumpforget(&self) -> bool {
        matches!(self.backend, Backend::BumpForget(..))
    }

    /// # Safety
    /// `addr`: live `T` (glue's type) in this arena, Drop suppressed, valid until reset.
    unsafe fn register_drop(&self, addr: *mut u8, glue: unsafe fn(*mut u8)) -> bool {
        if let Backend::BumpDrop(_, _, droplist) = &self.backend {
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
        if self.acct.limited_path.get() {
            for node in self.acct.ancestors() {
                let new = node.subtree_sum().checked_add(n).ok_or(AllocError)?;
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
    }
}

#[cold]
pub fn oom_named(context_name: &str, request: usize) -> PgError {
    PgError::error("out of memory")
        .with_sqlstate(ERRCODE_OUT_OF_MEMORY)
        .with_detail(alloc::format!(
            "Failed on request of size {request} in memory context \"{context_name}\"."
        ))
}

impl Drop for MemoryContext {
    fn drop(&mut self) {
        self.fire_reset_callbacks();
        if let Backend::BumpDrop(_, _, droplist) = &self.backend {
            droplist.borrow_mut().run();
        }
        self.acct.ident.borrow_mut().take();
        self.acct.self_used.set(0);
        if let Backend::Bump(bump, _)
        | Backend::BumpDrop(bump, _, _)
        | Backend::BumpForget(bump, _) = &mut self.backend
        {
            bump_give(core::mem::replace(bump, bumpalo::Bump::new()));
        }
    }
}

impl fmt::Debug for MemoryContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryContext")
            .field("name", &self.acct.name)
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeStats {
    pub name: &'static str,
    pub ident: Option<alloc::string::String>,
    pub used: usize,
    pub peak: usize,
    pub subtree_used: usize,
    pub subtree_peak: usize,
    pub limit: usize,
    pub is_bump: bool,
    pub arena_footprint: usize,
    pub nblocks: usize,
    pub children: alloc::vec::Vec<TreeStats>,
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
}

impl fmt::Debug for Mcx<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Mcx({:?})", self.0.acct.name)
    }
}

// SAFETY contract for callers: one-statement &mut, never re-entered; one context, one thread.
#[inline(always)]
unsafe fn aset_mut(set: &core::cell::UnsafeCell<aset::AllocSet>) -> &mut aset::AllocSet {
    &mut *set.get()
}

// SAFETY (trait contract): delegates to aset/Global/bumpalo; accounting is undone on failure.
unsafe impl Allocator for Mcx<'_> {
    #[inline]
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.0.charge(layout.size())?;
        let result = match &self.0.backend {
            // SAFETY: single-statement borrow, never re-entered (aset_mut).
            Backend::Aset(set) => unsafe { aset_mut(set) }.alloc(layout),
            Backend::Malloc => Global.allocate(layout),
            Backend::Bump(bump, blocks)
            | Backend::BumpDrop(bump, blocks, _)
            | Backend::BumpForget(bump, blocks) => {
                let r = bump.allocate(layout);
                if r.is_ok() {
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
        match &self.0.backend {
            Backend::Aset(set) => {
                self.0.uncharge(layout.size());
                #[cfg(test)]
                crate::churn_probe::bump();
                // SAFETY: single-statement borrow, never re-entered (aset_mut).
                unsafe { aset_mut(set) }.dealloc(ptr, layout)
            }
            Backend::Malloc => {
                self.0.uncharge(layout.size());
                #[cfg(test)]
                crate::churn_probe::bump();
                Global.deallocate(ptr, layout)
            }
            // bump.c: no BumpFree; with_accounted_free keeps the exact path.
            Backend::Bump(bump, _)
            | Backend::BumpDrop(bump, _, _)
            | Backend::BumpForget(bump, _) => {
                if self.0.acct.uncharge_on_free.get() {
                    self.0.uncharge(layout.size());
                    bump.deallocate(ptr, layout)
                }
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
            // SAFETY: single-statement borrow, never re-entered (aset_mut).
            Backend::Aset(set) => unsafe { aset_mut(set) }.realloc(ptr, old_layout, new_layout),
            Backend::Malloc => Global.grow(ptr, old_layout, new_layout),
            Backend::Bump(bump, blocks)
            | Backend::BumpDrop(bump, blocks, _)
            | Backend::BumpForget(bump, blocks) => {
                let r = bump.grow(ptr, old_layout, new_layout);
                if r.is_ok() {
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
            // SAFETY: single-statement borrow, never re-entered (aset_mut).
            Backend::Aset(set) => unsafe { aset_mut(set) }.realloc(ptr, old_layout, new_layout),
            Backend::Malloc => Global.shrink(ptr, old_layout, new_layout),
            Backend::Bump(bump, _)
            | Backend::BumpDrop(bump, _, _)
            | Backend::BumpForget(bump, _) => bump.shrink(ptr, old_layout, new_layout),
        };
        if result.is_ok()
            && (!self.0.acct.is_bump || self.0.acct.uncharge_on_free.get())
        {
            self.0.uncharge(old_layout.size() - new_layout.size());
        }
        result
    }
}

pub const MAX_ALLOC_SIZE: usize = 0x3FFF_FFFF;

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

/// Droppy `T` allowed: the returned box runs `Drop`.
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
pub fn box_unsize_dyn<'mcx, P, U>(
    sized: PgBox<'mcx, P>,
    coerce: impl FnOnce(*mut P) -> *mut U,
) -> PgBox<'mcx, U>
where
    P: 'mcx,
    U: ?Sized + 'mcx,
{
    let (raw, alloc) = allocator_api2::boxed::Box::into_raw_with_allocator(sized);
    let fat: *mut U = coerce(raw);
    // SAFETY: the exact pointer+allocator just decomposed; unsizing only attaches a vtable.
    unsafe { allocator_api2::boxed::Box::from_raw_in(fat, alloc) }
}

/// Move payload `P` out of an unsized `PgBox` without dropping `P`.
/// # Safety
/// `data`: the payload's data pointer inside `sized`; runtime type `P` (tag-checked).
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
    v.try_reserve(n).map_err(|_| mcx.oom(n))?;
    let old = v.len();
    // SAFETY: capacity >= old + n after try_reserve; dst disjoint; set_len covers the n bytes.
    unsafe {
        core::ptr::copy_nonoverlapping(bytes.as_ptr(), v.as_mut_ptr().add(old), n);
        v.set_len(old + n);
    }
    Ok(())
}

#[inline]
pub fn vec_with_capacity_in<'mcx, T>(mcx: Mcx<'mcx>, cap: usize) -> PgResult<PgVec<'mcx, T>> {
    const { assert!(!core::mem::needs_drop::<T>()) };
    let request = cap.saturating_mul(core::mem::size_of::<T>());
    check_alloc_size(request)?;
    let mut v = PgVec::new_in(mcx);
    v.try_reserve_exact(cap).map_err(|_| mcx.oom(request))?;
    Ok(v)
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
