//! Adaptive radix tree over u64 keys (`lib/radixtree.h`), generic over the
//! stored value type. `RadixTree` is the local-memory variant (slab-backed
//! nodes, aset-backed single-value leaves). `SharedRadixTree` is the
//! thread-native stand-in for C's `RT_SHMEM` flavor: the same tree behind an
//! `RwLock` with the C lock discipline (exclusive for set/delete, share for
//! find/iterate), nodes from the global allocator with a byte counter standing
//! in for `dsa_get_total_size`.

use core::marker::PhantomData;
use core::mem::{align_of, offset_of, size_of};
use core::ptr::{self, addr_of, addr_of_mut, null_mut, NonNull};
use std::alloc::Layout;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use mcx::MemoryContext;
use types_error::PgResult;

const RT_SPAN: i32 = 8;
const RT_NODE_MAX_SLOTS: usize = 1 << RT_SPAN;
const RT_CHUNK_MASK: u64 = (RT_NODE_MAX_SLOTS - 1) as u64;
const RT_MAX_SHIFT: i32 = 56;
const RT_MAX_LEVEL: usize = 8;
const RT_INVALID_SLOT_IDX: u8 = 0xFF;
const BITS_PER_BITMAPWORD: usize = 64;

const RT_NODE_KIND_4: u8 = 0x00;
const RT_NODE_KIND_16: u8 = 0x01;
const RT_NODE_KIND_48: u8 = 0x02;
const RT_NODE_KIND_256: u8 = 0x03;

const RT_FANOUT_4_MAX: usize = 8 - size_of::<RtNode>();
const RT_FANOUT_4: usize = 4;
const RT_FANOUT_16_MAX: usize = 32;
const RT_FANOUT_16_LO: usize = 16;
const RT_FANOUT_16_HI: usize = RT_FANOUT_16_MAX;
const RT_FANOUT_48_MAX: usize = 64;
const RT_FANOUT_48: usize = RT_FANOUT_48_MAX;
const RT_FANOUT_256: usize = RT_NODE_MAX_SLOTS;

const SLAB_DEFAULT_BLOCK_SIZE: usize = 8 * 1024;

#[repr(C)]
struct RtNode {
    kind: u8,
    // node256's fanout wraps to 0 and its count wraps to 0 when full; only
    // smaller kinds introspect these.
    fanout: u8,
    count: u8,
}

type RtSlot = *mut RtNode;

#[repr(C)]
struct Node4 {
    base: RtNode,
    chunks: [u8; RT_FANOUT_4_MAX],
    children: [RtSlot; 0],
}

#[repr(C)]
struct Node16 {
    base: RtNode,
    chunks: [u8; RT_FANOUT_16_MAX],
    children: [RtSlot; 0],
}

#[repr(C)]
struct Node48 {
    base: RtNode,
    isset: [u64; RT_FANOUT_48_MAX / BITS_PER_BITMAPWORD],
    slot_idxs: [u8; RT_NODE_MAX_SLOTS],
    children: [RtSlot; 0],
}

#[repr(C)]
struct Node256 {
    base: RtNode,
    isset: [u64; RT_FANOUT_256 / BITS_PER_BITMAPWORD],
    children: [RtSlot; RT_FANOUT_256],
}

// Layout parity with the C structs (transcription-error guard). 64-bit
// layout pin: RtSlot is a pointer, so every children offset shrinks on
// wasm32 (ILP32); the tree is heap-internal and stays self-consistent there.
#[cfg(not(target_family = "wasm"))]
const _: () = {
    assert!(size_of::<RtNode>() == 3);
    assert!(offset_of!(Node4, children) == 8);
    assert!(offset_of!(Node16, children) == 40);
    assert!(offset_of!(Node48, isset) == 8);
    assert!(offset_of!(Node48, slot_idxs) == 16);
    assert!(offset_of!(Node48, children) == 272);
    assert!(offset_of!(Node256, children) == 40);
    assert!(size_of::<Node256>() == 2088);
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(usize)]
pub enum SizeClass {
    Class4 = 0,
    Class16Lo,
    Class16Hi,
    Class48,
    Class256,
}

const NUM_SIZE_CLASSES: usize = 5;

const CLASS_FANOUT: [usize; NUM_SIZE_CLASSES] = [
    RT_FANOUT_4,
    RT_FANOUT_16_LO,
    RT_FANOUT_16_HI,
    RT_FANOUT_48,
    RT_FANOUT_256,
];

const CLASS_ALLOC_SIZE: [usize; NUM_SIZE_CLASSES] = [
    offset_of!(Node4, children) + RT_FANOUT_4 * size_of::<RtSlot>(),
    offset_of!(Node16, children) + RT_FANOUT_16_LO * size_of::<RtSlot>(),
    offset_of!(Node16, children) + RT_FANOUT_16_HI * size_of::<RtSlot>(),
    offset_of!(Node48, children) + RT_FANOUT_48 * size_of::<RtSlot>(),
    size_of::<Node256>(),
];

const CLASS_NAME: [&str; NUM_SIZE_CLASSES] = [
    "radix_tree node4",
    "radix_tree node16_lo",
    "radix_tree node16_hi",
    "radix_tree node48",
    "radix_tree node256",
];

const fn slab_block_size(allocsize: usize) -> usize {
    let want = (allocsize * 32).next_power_of_two();
    if want > SLAB_DEFAULT_BLOCK_SIZE {
        want
    } else {
        SLAB_DEFAULT_BLOCK_SIZE
    }
}

#[inline(always)]
fn get_key_chunk(key: u64, shift: i32) -> u8 {
    ((key >> shift) & RT_CHUNK_MASK) as u8
}

#[inline(always)]
fn key_get_shift(key: u64) -> i32 {
    if key == 0 {
        0
    } else {
        ((63 - key.leading_zeros() as i32) / RT_SPAN) * RT_SPAN
    }
}

fn shift_get_max_val(shift: i32) -> u64 {
    if shift == RT_MAX_SHIFT {
        u64::MAX
    } else {
        (1u64 << (shift + RT_SPAN)) - 1
    }
}

/// Values stored in the tree. Fixed-size values that fit a slot word are
/// embedded in leaf-level child slots; larger or variable-length values live
/// in single-value leaves.
///
/// # Safety
/// - The value must be plain bytes (no drop glue, no borrowed data): it is
///   stored and returned by `memcpy`.
/// - If `VARLEN`, `Self` is the fixed header prefix of the image
///   (`size_of::<Self>()` <= every image size), and `value_size` returns the
///   full image size from the header alone. Pointers passed to `set_ptr` and
///   returned by `find_ptr`/`next_ptr` cover the full image; `&Self` accessors
///   cover only the header.
/// - If `RUNTIME_EMBEDDABLE`, bit 0 of the value's first byte is the embedded
///   tag (C's low pointer-bit tag, little-endian): values whose size is <= 8
///   must keep that bit set, and the type's alignment must be >= 2.
pub unsafe trait RtValue: Sized {
    const VARLEN: bool = false;
    const RUNTIME_EMBEDDABLE: bool = false;

    fn value_size(&self) -> usize {
        size_of::<Self>()
    }
}

// SAFETY: fixed-size plain integer, embedded (test_radixtree.c's TestValueType).
unsafe impl RtValue for u64 {}

#[inline(always)]
fn value_is_embeddable<V: RtValue>(value: &V) -> bool {
    if V::VARLEN {
        V::RUNTIME_EMBEDDABLE && value.value_size() <= size_of::<RtSlot>()
    } else {
        size_of::<V>() <= size_of::<RtSlot>()
    }
}

#[inline(always)]
unsafe fn childptr_is_value<V: RtValue>(slot: *const RtSlot) -> bool {
    if V::VARLEN {
        // LE: the C low-bit pointer tag lives in byte 0; reading only that byte
        // avoids touching the uninit tail of a short embedded value.
        V::RUNTIME_EMBEDDABLE && (*(slot as *const u8)) & 1 == 1
    } else {
        size_of::<V>() <= size_of::<RtSlot>()
    }
}

#[inline(always)]
unsafe fn slot_value<V: RtValue>(slot: *mut RtSlot) -> *mut V {
    if childptr_is_value::<V>(slot) {
        slot as *mut V
    } else {
        *slot as *mut V
    }
}

fn leaf_layout<V: RtValue>(size: usize) -> Layout {
    // MAXALIGN parity with C's palloc'd leaves.
    let align = if align_of::<V>() > 8 { align_of::<V>() } else { 8 };
    Layout::from_size_align(size, align).unwrap()
}

/// Node and leaf storage behind the tree; implemented by [`LocalStore`] and
/// [`SharedStore`] only.
pub trait RtStore {
    const RECURSIVE_FREE: bool;

    fn alloc_node(&self, class: SizeClass) -> PgResult<NonNull<u8>>;
    /// # Safety: `ptr` came from `alloc_node(class)` on this store.
    unsafe fn free_node(&self, ptr: NonNull<u8>, class: SizeClass);
    fn alloc_leaf(&self, layout: Layout) -> PgResult<NonNull<u8>>;
    /// # Safety: `ptr` came from `alloc_leaf(layout)` on this store.
    unsafe fn free_leaf(&self, ptr: NonNull<u8>, layout: Layout);
    fn memory_usage(&self) -> u64;
}

pub struct LocalStore {
    leaf_ctx: MemoryContext,
    node_slabs: [MemoryContext; NUM_SIZE_CLASSES],
}

impl LocalStore {
    fn new(parent: &MemoryContext) -> LocalStore {
        Self::in_ctx(parent.new_child("radix_tree"))
    }

    fn in_ctx(leaf_ctx: MemoryContext) -> LocalStore {
        let node_slabs = core::array::from_fn(|i| {
            leaf_ctx.new_child_slab(
                CLASS_NAME[i],
                slab_block_size(CLASS_ALLOC_SIZE[i]),
                CLASS_ALLOC_SIZE[i],
            )
        });
        LocalStore { leaf_ctx, node_slabs }
    }
}

impl RtStore for LocalStore {
    const RECURSIVE_FREE: bool = false;

    // inline(always): the slab fast path must inline into the tree walk (C
    // inlines RT_ALLOC_NODE; outlined this pays a fat prologue per node).
    #[inline(always)]
    fn alloc_node(&self, class: SizeClass) -> PgResult<NonNull<u8>> {
        let size = CLASS_ALLOC_SIZE[class as usize];
        // SAFETY: every CLASS_ALLOC_SIZE is a small nonzero multiple of 8.
        let layout = unsafe { Layout::from_size_align_unchecked(size, 8) };
        let ctx = &self.node_slabs[class as usize];
        match mcx::Allocator::allocate(&ctx.mcx(), layout) {
            Ok(p) => Ok(p.cast()),
            Err(_) => Err(ctx.oom(size).into()),
        }
    }

    #[inline]
    unsafe fn free_node(&self, ptr: NonNull<u8>, class: SizeClass) {
        let size = CLASS_ALLOC_SIZE[class as usize];
        // SAFETY: as alloc_node.
        let layout = Layout::from_size_align_unchecked(size, 8);
        mcx::Allocator::deallocate(&self.node_slabs[class as usize].mcx(), ptr, layout);
    }

    #[inline]
    fn alloc_leaf(&self, layout: Layout) -> PgResult<NonNull<u8>> {
        match mcx::Allocator::allocate(&self.leaf_ctx.mcx(), layout) {
            Ok(p) => Ok(p.cast()),
            Err(_) => Err(self.leaf_ctx.oom(layout.size()).into()),
        }
    }

    #[inline]
    unsafe fn free_leaf(&self, ptr: NonNull<u8>, layout: Layout) {
        mcx::Allocator::deallocate(&self.leaf_ctx.mcx(), ptr, layout);
    }

    fn memory_usage(&self) -> u64 {
        // C: MemoryContextMemAllocated(leaf_context, recurse = true).
        let mut total = self.leaf_ctx.stats().arena_footprint as u64;
        for slab in &self.node_slabs {
            total += slab.stats().arena_footprint as u64;
        }
        total
    }
}

// C's RT_SHMEM flavor allocates from DSA, outside memory contexts;
// dsa_get_total_size maps to this live-byte counter.
pub struct SharedStore {
    total: AtomicUsize,
}

impl RtStore for SharedStore {
    const RECURSIVE_FREE: bool = true;

    #[inline]
    fn alloc_node(&self, class: SizeClass) -> PgResult<NonNull<u8>> {
        let size = CLASS_ALLOC_SIZE[class as usize];
        // SAFETY: as LocalStore::alloc_node.
        self.alloc_leaf(unsafe { Layout::from_size_align_unchecked(size, 8) })
    }

    #[inline]
    unsafe fn free_node(&self, ptr: NonNull<u8>, class: SizeClass) {
        let size = CLASS_ALLOC_SIZE[class as usize];
        // SAFETY: as LocalStore::alloc_node.
        self.free_leaf(ptr, Layout::from_size_align_unchecked(size, 8));
    }

    #[inline]
    fn alloc_leaf(&self, layout: Layout) -> PgResult<NonNull<u8>> {
        // SAFETY: layout.size() > 0 for every node class and leaf.
        let p = unsafe { std::alloc::alloc(layout) };
        match NonNull::new(p) {
            Some(p) => {
                self.total.fetch_add(layout.size(), Ordering::Relaxed);
                Ok(p)
            }
            None => Err(mcx::oom_named("radix_tree shared", layout.size()).into()),
        }
    }

    #[inline]
    unsafe fn free_leaf(&self, ptr: NonNull<u8>, layout: Layout) {
        self.total.fetch_sub(layout.size(), Ordering::Relaxed);
        std::alloc::dealloc(ptr.as_ptr(), layout);
    }

    fn memory_usage(&self) -> u64 {
        self.total.load(Ordering::Relaxed) as u64
    }
}

pub struct Tree<V: RtValue, S: RtStore> {
    root: RtSlot,
    max_val: u64,
    num_keys: i64,
    start_shift: i32,
    store: S,
    _values: PhantomData<V>,
}

/// The local-memory variant (C's non-`RT_SHMEM` template instantiation).
pub type RadixTree<V> = Tree<V, LocalStore>;

impl<V: RtValue> RadixTree<V> {
    pub fn create(parent: &MemoryContext) -> PgResult<RadixTree<V>> {
        Tree::with_store(LocalStore::new(parent))
    }

    /// Leaves allocate directly in `ctx` and node slabs as its children —
    /// C's varlen RT_CREATE shape (`tree->leaf_context = ctx`), so the
    /// caller's insert-only/bump context choice reaches the leaves.
    pub fn create_in(ctx: MemoryContext) -> PgResult<RadixTree<V>> {
        Tree::with_store(LocalStore::in_ctx(ctx))
    }
}

impl<V: RtValue, S: RtStore> Tree<V, S> {
    fn with_store(store: S) -> PgResult<Tree<V, S>> {
        let mut tree = Tree {
            root: null_mut(),
            max_val: shift_get_max_val(0),
            num_keys: 0,
            start_shift: 0,
            store,
            _values: PhantomData,
        };
        tree.root = unsafe { alloc_node(&tree.store, RT_NODE_KIND_4, SizeClass::Class4)? };
        Ok(tree)
    }

    pub fn num_keys(&self) -> i64 {
        self.num_keys
    }

    pub fn memory_usage(&self) -> u64 {
        self.store.memory_usage()
    }

    #[inline]
    fn find_slot(&self, key: u64) -> *mut RtSlot {
        if key > self.max_val {
            return null_mut();
        }
        debug_assert!(!self.root.is_null());
        let mut node = self.root;
        let mut shift = self.start_shift;
        loop {
            // SAFETY: slots above the leaf level hold live node pointers; the
            // shift bookkeeping stops before a leaf-level slot is followed.
            unsafe {
                let slot = node_search(node, get_key_chunk(key, shift));
                if slot.is_null() {
                    return null_mut();
                }
                if shift == 0 {
                    return slot;
                }
                node = *slot;
            }
            shift -= RT_SPAN;
        }
    }

    /// Borrowed view of the stored value; the borrow pins the tree until the
    /// caller is done (C leaves that to the caller's locking). For `VARLEN`
    /// values this reference covers the header only; trailing image bytes must
    /// be read through [`Tree::find_ptr`].
    #[inline]
    pub fn find(&self, key: u64) -> Option<&V> {
        let slot = self.find_slot(key);
        if slot.is_null() {
            None
        } else {
            // SAFETY: slot holds either an embedded value image or a live leaf.
            unsafe { Some(&*slot_value::<V>(slot)) }
        }
    }

    /// As [`Tree::find`], but the pointer keeps provenance over the whole
    /// stored image (valid until the next `set`/`delete`/drop).
    #[inline]
    pub fn find_ptr(&self, key: u64) -> Option<NonNull<V>> {
        let slot = self.find_slot(key);
        if slot.is_null() {
            None
        } else {
            // SAFETY: non-null slot from find_slot.
            unsafe { Some(NonNull::new_unchecked(slot_value::<V>(slot))) }
        }
    }

    #[inline]
    pub fn find_mut(&mut self, key: u64) -> Option<&mut V> {
        let slot = self.find_slot(key);
        if slot.is_null() {
            None
        } else {
            // SAFETY: as find(); &mut self gives exclusive access.
            unsafe { Some(&mut *slot_value::<V>(slot)) }
        }
    }

    /// Sets `key` to a copy of `value`. Returns true if the key already existed.
    /// For `VARLEN` values the full image must be readable behind the
    /// reference (RtValue contract); otherwise use [`Tree::set_ptr`].
    pub fn set(&mut self, key: u64, value: &V) -> PgResult<bool> {
        // SAFETY: `value` is readable for value_size() bytes per RtValue.
        unsafe { self.set_ptr(key, value) }
    }

    /// # Safety: `value` must be readable for `(*value).value_size()` bytes.
    pub unsafe fn set_ptr(&mut self, key: u64, value: *const V) -> PgResult<bool> {
        let value_sz = (*value).value_size();
        let mut found = false;
        debug_assert!(!self.root.is_null());
        // SAFETY: structural invariants (kind/count/fanout, slot liveness) are
        // maintained by every mutation path below, mirroring the C template;
        // the root slot raw pointer is used only while no other access to
        // self.root is live.
        unsafe {
            let slot: *mut RtSlot;
            if key > self.max_val {
                if self.num_keys == 0 {
                    let start_shift = key_get_shift(key);
                    let n4 = self.root as *mut Node4;
                    (*n4).base.count = 1;
                    (*n4).chunks[0] = get_key_chunk(key, start_shift);
                    slot = extend_down(&self.store, node4_children(n4), key, start_shift)?;
                    self.start_shift = start_shift;
                    self.max_val = shift_get_max_val(start_shift);
                } else {
                    self.extend_up(key)?;
                    let root_slot: *mut RtSlot = &mut self.root;
                    slot =
                        get_slot_recursive(&self.store, root_slot, key, self.start_shift, &mut found)?;
                }
            } else {
                let root_slot: *mut RtSlot = &mut self.root;
                slot = get_slot_recursive(&self.store, root_slot, key, self.start_shift, &mut found)?;
            }

            if value_is_embeddable::<V>(&*value) {
                if found && !childptr_is_value::<V>(slot) {
                    free_leaf::<V, S>(&self.store, *slot);
                }
                ptr::copy_nonoverlapping(value.cast::<u8>(), slot as *mut u8, value_sz);
                if V::VARLEN && V::RUNTIME_EMBEDDABLE {
                    // LE: tag bit 0 of the slot word == bit 0 of byte 0.
                    *(slot as *mut u8) |= 1;
                }
            } else {
                let leaf: *mut u8;
                if found && !childptr_is_value::<V>(slot) {
                    let old = *slot as *mut u8;
                    if (*(old as *const V)).value_size() != value_sz {
                        free_leaf::<V, S>(&self.store, *slot);
                        leaf = self.store.alloc_leaf(leaf_layout::<V>(value_sz))?.as_ptr();
                        *slot = leaf as RtSlot;
                    } else {
                        leaf = old;
                    }
                } else {
                    leaf = self.store.alloc_leaf(leaf_layout::<V>(value_sz))?.as_ptr();
                    *slot = leaf as RtSlot;
                }
                ptr::copy_nonoverlapping(value.cast::<u8>(), leaf, value_sz);
            }
        }
        if !found {
            self.num_keys += 1;
        }
        Ok(found)
    }

    #[cold]
    fn extend_up(&mut self, key: u64) -> PgResult<()> {
        let target_shift = key_get_shift(key);
        let mut shift = self.start_shift;
        debug_assert!(shift < target_shift);
        while shift < target_shift {
            // SAFETY: fresh node4; the old root becomes its single child.
            unsafe {
                let node = alloc_node(&self.store, RT_NODE_KIND_4, SizeClass::Class4)?;
                let n4 = node as *mut Node4;
                (*n4).base.count = 1;
                (*n4).chunks[0] = 0;
                *node4_children(n4) = self.root;
                self.root = node;
            }
            shift += RT_SPAN;
        }
        self.max_val = shift_get_max_val(target_shift);
        self.start_shift = target_shift;
        Ok(())
    }

    /// Deletes `key`, returning true if it was present.
    pub fn delete(&mut self, key: u64) -> bool {
        if key > self.max_val {
            return false;
        }
        debug_assert!(!self.root.is_null());
        let mut root_slot = self.root;
        let mut root_emptied = false;
        // SAFETY: as set(); the root slot round-trips through a local so the
        // recursion's parent_slot writes never alias &mut self.
        let deleted = unsafe {
            delete_recursive::<V, S>(
                &self.store,
                &mut root_slot,
                key,
                self.start_shift,
                true,
                &mut root_emptied,
            )
        };
        self.root = root_slot;
        if root_emptied {
            self.start_shift = 0;
            self.max_val = shift_get_max_val(0);
        }
        if deleted {
            self.num_keys -= 1;
            debug_assert!(self.num_keys >= 0);
        }
        deleted
    }

    /// Iteration produces key/value pairs in ascending key order.
    pub fn begin_iterate(&self) -> TreeIter<'_, V, S> {
        debug_assert!(!self.root.is_null());
        let top_level = (self.start_shift / RT_SPAN) as usize;
        let mut iter = TreeIter {
            tree: self,
            node_iters: [NodeIter { node: null_mut(), idx: 0 }; RT_MAX_LEVEL],
            top_level,
            cur_level: top_level as isize,
            key: 0,
        };
        iter.node_iters[top_level].node = self.root;
        iter
    }
}

impl<V: RtValue, S: RtStore> Drop for Tree<V, S> {
    fn drop(&mut self) {
        if S::RECURSIVE_FREE && !self.root.is_null() {
            // SAFETY: exclusive access on drop; frees every node and leaf once.
            unsafe { free_recurse::<V, S>(&self.store, self.root, self.start_shift) };
        }
    }
}

// SAFETY: all node/leaf memory is exclusively owned by the tree; V crosses
// threads only by value copy or reference. Cross-thread aliasing is mediated
// by SharedRadixTree's RwLock (C: the RT_SHMEM LWLock).
unsafe impl<V: RtValue + Send + Sync> Send for Tree<V, SharedStore> {}
// SAFETY: as above; &Tree only permits reads.
unsafe impl<V: RtValue + Send + Sync> Sync for Tree<V, SharedStore> {}

/// Thread-native replacement for the C `RT_SHMEM` instantiation: parallel
/// vacuum workers share one address space here, so the DSA-backed tree becomes
/// one tree behind an `RwLock`. Lock discipline mirrors C: `lock_exclusive`
/// around set/delete, `lock_share` around find/iterate.
pub struct SharedRadixTree<V: RtValue> {
    lock: RwLock<Tree<V, SharedStore>>,
}

impl<V: RtValue + Send + Sync> SharedRadixTree<V> {
    pub fn create() -> PgResult<SharedRadixTree<V>> {
        let tree = Tree::with_store(SharedStore { total: AtomicUsize::new(0) })?;
        Ok(SharedRadixTree { lock: RwLock::new(tree) })
    }

    pub fn lock_exclusive(&self) -> RwLockWriteGuard<'_, Tree<V, SharedStore>> {
        self.lock.write().unwrap()
    }

    pub fn lock_share(&self) -> RwLockReadGuard<'_, Tree<V, SharedStore>> {
        self.lock.read().unwrap()
    }

    pub fn memory_usage(&self) -> u64 {
        self.lock.read().unwrap().memory_usage()
    }
}

#[derive(Clone, Copy)]
struct NodeIter {
    node: *mut RtNode,
    idx: usize,
}

pub struct TreeIter<'a, V: RtValue, S: RtStore> {
    tree: &'a Tree<V, S>,
    node_iters: [NodeIter; RT_MAX_LEVEL],
    top_level: usize,
    cur_level: isize,
    key: u64,
}

impl<'a, V: RtValue, S: RtStore> TreeIter<'a, V, S> {
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<(u64, &'a V)> {
        // SAFETY: pointer from next_ptr is live while the tree borrow holds.
        self.next_ptr().map(|(k, p)| (k, unsafe { &*p.as_ptr() }))
    }

    pub fn next_ptr(&mut self) -> Option<(u64, NonNull<V>)> {
        let _ = self.tree;
        while self.cur_level >= 0 && self.cur_level as usize <= self.top_level {
            let level = self.cur_level as usize;
            // SAFETY: node_iters holds live nodes for every level in range.
            unsafe {
                let slot = node_iterate_next(&mut self.node_iters[level], &mut self.key, level);
                if level == 0 && !slot.is_null() {
                    return Some((self.key, NonNull::new_unchecked(slot_value::<V>(slot))));
                }
                if !slot.is_null() {
                    self.cur_level -= 1;
                    let ni = &mut self.node_iters[self.cur_level as usize];
                    ni.node = *slot;
                    ni.idx = 0;
                } else {
                    self.cur_level += 1;
                }
            }
        }
        None
    }
}

#[inline(always)]
unsafe fn node4_children(n: *mut Node4) -> *mut RtSlot {
    addr_of_mut!((*n).children).cast()
}

#[inline(always)]
unsafe fn node16_children(n: *mut Node16) -> *mut RtSlot {
    addr_of_mut!((*n).children).cast()
}

#[inline(always)]
unsafe fn node48_children(n: *mut Node48) -> *mut RtSlot {
    addr_of_mut!((*n).children).cast()
}

#[inline(always)]
unsafe fn node256_children(n: *mut Node256) -> *mut RtSlot {
    addr_of_mut!((*n).children).cast()
}

#[inline(always)]
unsafe fn node48_is_chunk_used(n: *mut Node48, chunk: usize) -> bool {
    (*n).slot_idxs[chunk] != RT_INVALID_SLOT_IDX
}

#[inline(always)]
unsafe fn node48_get_child(n: *mut Node48, chunk: usize) -> *mut RtSlot {
    node48_children(n).add((*n).slot_idxs[chunk] as usize)
}

#[inline(always)]
unsafe fn node256_is_chunk_used(n: *mut Node256, chunk: usize) -> bool {
    (*n).isset[chunk / BITS_PER_BITMAPWORD] & (1u64 << (chunk % BITS_PER_BITMAPWORD)) != 0
}

#[inline(always)]
unsafe fn node256_get_child(n: *mut Node256, chunk: usize) -> *mut RtSlot {
    debug_assert!(node256_is_chunk_used(n, chunk));
    node256_children(n).add(chunk)
}

// inline(always): C's RT_ALLOC_NODE is static inline; outlined this is a
// fat-prologue call on every grow/extend (measured on radix_set_grow_sparse).
#[inline(always)]
unsafe fn alloc_node<S: RtStore>(store: &S, kind: u8, class: SizeClass) -> PgResult<*mut RtNode> {
    let ptr = store.alloc_node(class)?.as_ptr();
    match kind {
        RT_NODE_KIND_4 => ptr::write_bytes(ptr, 0, offset_of!(Node4, children)),
        RT_NODE_KIND_16 => ptr::write_bytes(ptr, 0, offset_of!(Node16, children)),
        RT_NODE_KIND_48 => {
            ptr::write_bytes(ptr, 0, offset_of!(Node48, slot_idxs));
            ptr::write_bytes(
                ptr.add(offset_of!(Node48, slot_idxs)),
                RT_INVALID_SLOT_IDX,
                RT_NODE_MAX_SLOTS,
            );
        }
        RT_NODE_KIND_256 => ptr::write_bytes(ptr, 0, offset_of!(Node256, children)),
        _ => unreachable!(),
    }
    let node = ptr as *mut RtNode;
    (*node).kind = kind;
    (*node).fanout = CLASS_FANOUT[class as usize] as u8;
    Ok(node)
}

unsafe fn free_node<S: RtStore>(store: &S, node: *mut RtNode) {
    let class = match (*node).fanout as usize {
        RT_FANOUT_4 => SizeClass::Class4,
        RT_FANOUT_16_LO => SizeClass::Class16Lo,
        RT_FANOUT_16_HI => SizeClass::Class16Hi,
        RT_FANOUT_48 => SizeClass::Class48,
        0 => SizeClass::Class256,
        // SAFETY: fanout is one of the five values written by alloc_node.
        _ => core::hint::unreachable_unchecked(),
    };
    store.free_node(NonNull::new_unchecked(node as *mut u8), class);
}

unsafe fn free_leaf<V: RtValue, S: RtStore>(store: &S, leaf: RtSlot) {
    let size = (*(leaf as *const V)).value_size();
    store.free_leaf(NonNull::new_unchecked(leaf as *mut u8), leaf_layout::<V>(size));
}

#[inline(always)]
unsafe fn copy_common(newnode: *mut RtNode, oldnode: *mut RtNode) {
    (*newnode).count = (*oldnode).count;
}

#[cfg(all(target_arch = "aarch64", not(miri)))]
mod simd {
    use core::arch::aarch64::*;

    // vshrn nibble-mask idiom: 4 mask bits per byte lane; a match at lane i
    // sets bits [4i, 4i+4).
    #[inline(always)]
    pub unsafe fn eq_nibble_mask16(p: *const u8, chunk: u8) -> u64 {
        let cmp = vceqq_u8(vld1q_u8(p), vdupq_n_u8(chunk));
        let n = vshrn_n_u16::<4>(vreinterpretq_u16_u8(cmp));
        vget_lane_u64::<0>(vreinterpret_u64_u8(n))
    }

    #[inline(always)]
    pub unsafe fn ge_nibble_mask16(p: *const u8, chunk: u8) -> u64 {
        let cmp = vcgeq_u8(vld1q_u8(p), vdupq_n_u8(chunk));
        let n = vshrn_n_u16::<4>(vreinterpretq_u16_u8(cmp));
        vget_lane_u64::<0>(vreinterpret_u64_u8(n))
    }

    #[inline(always)]
    pub fn nibble_count_mask(lanes: usize) -> u64 {
        if lanes >= 16 {
            !0
        } else {
            (1u64 << (4 * lanes)) - 1
        }
    }
}

#[inline(always)]
unsafe fn node_16_search_eq(n: *mut Node16, chunk: u8) -> *mut RtSlot {
    let count = (*n).base.count as usize;
    let chunks = addr_of!((*n).chunks).cast::<u8>();
    let children = node16_children(n);

    #[cfg(all(target_arch = "aarch64", not(miri)))]
    return {
        use crate::simd::*;
        let m = eq_nibble_mask16(chunks, chunk) & nibble_count_mask(count);
        if m != 0 {
            children.add((m.trailing_zeros() / 4) as usize)
        } else if count > 16 {
            let m = eq_nibble_mask16(chunks.add(16), chunk) & nibble_count_mask(count - 16);
            if m != 0 {
                children.add(16 + (m.trailing_zeros() / 4) as usize)
            } else {
                null_mut()
            }
        } else {
            null_mut()
        }
    };

    #[cfg(not(all(target_arch = "aarch64", not(miri))))]
    {
        for i in 0..count {
            if *chunks.add(i) == chunk {
                return children.add(i);
            }
        }
        null_mut()
    }
}

#[inline(always)]
unsafe fn node_16_get_insertpos(n: *mut Node16, chunk: u8) -> usize {
    let count = (*n).base.count as usize;
    let chunks = addr_of!((*n).chunks).cast::<u8>();

    // Branch on the last element first: ordered inserts exit here, and it
    // guarantees a >= match exists below at an index < count (C does the same;
    // that also makes count-masking the vector results unnecessary).
    debug_assert!(count > 0);
    if *chunks.add(count - 1) < chunk {
        return count;
    }

    #[cfg(all(target_arch = "aarch64", not(miri)))]
    return {
        use crate::simd::*;
        let m = ge_nibble_mask16(chunks, chunk);
        if m != 0 {
            (m.trailing_zeros() / 4) as usize
        } else {
            let m = ge_nibble_mask16(chunks.add(16), chunk);
            debug_assert!(m != 0);
            16 + (m.trailing_zeros() / 4) as usize
        }
    };

    #[cfg(not(all(target_arch = "aarch64", not(miri))))]
    {
        let mut index = 0;
        while index < count {
            if *chunks.add(index) >= chunk {
                break;
            }
            index += 1;
        }
        index
    }
}

#[inline(always)]
unsafe fn node_search(node: *mut RtNode, chunk: u8) -> *mut RtSlot {
    match (*node).kind {
        RT_NODE_KIND_4 => {
            let n4 = node as *mut Node4;
            let count = (*n4).base.count as usize;
            for i in 0..count {
                if (*n4).chunks[i] == chunk {
                    return node4_children(n4).add(i);
                }
            }
            null_mut()
        }
        RT_NODE_KIND_16 => node_16_search_eq(node as *mut Node16, chunk),
        RT_NODE_KIND_48 => {
            let n48 = node as *mut Node48;
            if !node48_is_chunk_used(n48, chunk as usize) {
                return null_mut();
            }
            node48_get_child(n48, chunk as usize)
        }
        RT_NODE_KIND_256 => {
            let n256 = node as *mut Node256;
            if !node256_is_chunk_used(n256, chunk as usize) {
                return null_mut();
            }
            node256_get_child(n256, chunk as usize)
        }
        // SAFETY: kind is one of the four values written by alloc_node.
        _ => core::hint::unreachable_unchecked(),
    }
}

#[inline(always)]
unsafe fn node_4_get_insertpos(n: *mut Node4, chunk: u8, count: usize) -> usize {
    let mut idx = 0;
    while idx < count {
        if (*n).chunks[idx] >= chunk {
            break;
        }
        idx += 1;
    }
    idx
}

// memmove loses at these sizes; simple loops as C.
#[inline(always)]
unsafe fn shift_arrays_for_insert(
    chunks: *mut u8,
    children: *mut RtSlot,
    count: usize,
    insertpos: usize,
) {
    let mut i = count;
    while i > insertpos {
        *chunks.add(i) = *chunks.add(i - 1);
        *children.add(i) = *children.add(i - 1);
        i -= 1;
    }
}

#[inline(always)]
unsafe fn shift_arrays_and_delete(
    chunks: *mut u8,
    children: *mut RtSlot,
    count: usize,
    deletepos: usize,
) {
    let mut i = deletepos;
    while i < count - 1 {
        *chunks.add(i) = *chunks.add(i + 1);
        *children.add(i) = *children.add(i + 1);
        i += 1;
    }
}

#[inline(always)]
unsafe fn copy_arrays_for_insert(
    dst_chunks: *mut u8,
    dst_children: *mut RtSlot,
    src_chunks: *const u8,
    src_children: *const RtSlot,
    count: usize,
    insertpos: usize,
) {
    for i in 0..count {
        let destidx = i + (i >= insertpos) as usize;
        *dst_chunks.add(destidx) = *src_chunks.add(i);
        *dst_children.add(destidx) = *src_children.add(i);
    }
}

#[inline(always)]
unsafe fn copy_arrays_and_delete(
    dst_chunks: *mut u8,
    dst_children: *mut RtSlot,
    src_chunks: *const u8,
    src_children: *const RtSlot,
    count: usize,
    deletepos: usize,
) {
    for i in 0..count - 1 {
        let sourceidx = i + (i >= deletepos) as usize;
        *dst_chunks.add(i) = *src_chunks.add(sourceidx);
        *dst_children.add(i) = *src_children.add(sourceidx);
    }
}

unsafe fn add_child_256(n256: *mut Node256, chunk: u8) -> *mut RtSlot {
    let idx = chunk as usize / BITS_PER_BITMAPWORD;
    let bitnum = chunk as usize % BITS_PER_BITMAPWORD;
    (*n256).isset[idx] |= 1u64 << bitnum;
    (*n256).base.count = (*n256).base.count.wrapping_add(1);
    verify_node(n256 as *mut RtNode);
    node256_children(n256).add(chunk as usize)
}

#[inline(never)]
unsafe fn grow_node_48<S: RtStore>(
    store: &S,
    parent_slot: *mut RtSlot,
    node: *mut RtNode,
    chunk: u8,
) -> PgResult<*mut RtSlot> {
    let n48 = node as *mut Node48;
    let newnode = alloc_node(store, RT_NODE_KIND_256, SizeClass::Class256)?;
    let new256 = newnode as *mut Node256;

    copy_common(newnode, node);
    let mut i = 0usize;
    for word_num in 0..RT_NODE_MAX_SLOTS / BITS_PER_BITMAPWORD {
        let mut bitmap = 0u64;
        // Word-at-a-time isset stores (per-chunk bit ops dominated; as C).
        for bit in 0..BITS_PER_BITMAPWORD {
            let offset = (*n48).slot_idxs[i];
            if offset != RT_INVALID_SLOT_IDX {
                bitmap |= 1u64 << bit;
                *node256_children(new256).add(i) = *node48_children(n48).add(offset as usize);
            }
            i += 1;
        }
        (*new256).isset[word_num] = bitmap;
    }

    *parent_slot = newnode;
    free_node(store, node);
    Ok(add_child_256(new256, chunk))
}

unsafe fn add_child_48(n48: *mut Node48, chunk: u8) -> *mut RtSlot {
    let w = (*n48).isset[0];
    let insertpos = (!w).trailing_zeros() as usize;
    debug_assert!(insertpos < (*n48).base.fanout as usize);
    (*n48).isset[0] = w | (w + 1);
    (*n48).slot_idxs[chunk as usize] = insertpos as u8;
    (*n48).base.count += 1;
    verify_node(n48 as *mut RtNode);
    node48_children(n48).add(insertpos)
}

#[inline(never)]
unsafe fn grow_node_16<S: RtStore>(
    store: &S,
    parent_slot: *mut RtSlot,
    node: *mut RtNode,
    chunk: u8,
) -> PgResult<*mut RtSlot> {
    let n16 = node as *mut Node16;
    if ((*n16).base.fanout as usize) < RT_FANOUT_16_HI {
        debug_assert_eq!((*n16).base.fanout as usize, RT_FANOUT_16_LO);
        let newnode = alloc_node(store, RT_NODE_KIND_16, SizeClass::Class16Hi)?;
        let new16 = newnode as *mut Node16;

        copy_common(newnode, node);
        debug_assert_eq!((*n16).base.count as usize, RT_FANOUT_16_LO);
        let insertpos = node_16_get_insertpos(n16, chunk);
        copy_arrays_for_insert(
            addr_of_mut!((*new16).chunks).cast(),
            node16_children(new16),
            addr_of!((*n16).chunks).cast(),
            node16_children(n16),
            RT_FANOUT_16_LO,
            insertpos,
        );
        (*new16).chunks[insertpos] = chunk;
        (*new16).base.count += 1;
        verify_node(newnode);

        free_node(store, node);
        *parent_slot = newnode;
        Ok(node16_children(new16).add(insertpos))
    } else {
        debug_assert_eq!((*n16).base.fanout as usize, RT_FANOUT_16_HI);
        let newnode = alloc_node(store, RT_NODE_KIND_48, SizeClass::Class48)?;
        let new48 = newnode as *mut Node48;

        copy_common(newnode, node);
        for i in 0..RT_FANOUT_16_HI {
            (*new48).slot_idxs[(*n16).chunks[i] as usize] = i as u8;
        }
        ptr::copy_nonoverlapping(node16_children(n16), node48_children(new48), RT_FANOUT_16_HI);
        (*new48).isset[0] = (1u64 << RT_FANOUT_16_HI) - 1;

        let insertpos = RT_FANOUT_16_HI;
        (*new48).isset[insertpos / BITS_PER_BITMAPWORD] |=
            1u64 << (insertpos % BITS_PER_BITMAPWORD);
        (*new48).slot_idxs[chunk as usize] = insertpos as u8;
        (*new48).base.count += 1;
        verify_node(newnode);

        *parent_slot = newnode;
        free_node(store, node);
        Ok(node48_children(new48).add(insertpos))
    }
}

unsafe fn add_child_16(n16: *mut Node16, chunk: u8) -> *mut RtSlot {
    let insertpos = node_16_get_insertpos(n16, chunk);
    shift_arrays_for_insert(
        addr_of_mut!((*n16).chunks).cast(),
        node16_children(n16),
        (*n16).base.count as usize,
        insertpos,
    );
    (*n16).chunks[insertpos] = chunk;
    (*n16).base.count += 1;
    verify_node(n16 as *mut RtNode);
    node16_children(n16).add(insertpos)
}

#[inline(never)]
unsafe fn grow_node_4<S: RtStore>(
    store: &S,
    parent_slot: *mut RtSlot,
    node: *mut RtNode,
    chunk: u8,
) -> PgResult<*mut RtSlot> {
    let n4 = node as *mut Node4;
    let newnode = alloc_node(store, RT_NODE_KIND_16, SizeClass::Class16Lo)?;
    let new16 = newnode as *mut Node16;

    copy_common(newnode, node);
    debug_assert_eq!((*n4).base.count as usize, RT_FANOUT_4);
    let insertpos = node_4_get_insertpos(n4, chunk, RT_FANOUT_4);
    copy_arrays_for_insert(
        addr_of_mut!((*new16).chunks).cast(),
        node16_children(new16),
        addr_of!((*n4).chunks).cast(),
        node4_children(n4),
        RT_FANOUT_4,
        insertpos,
    );
    (*new16).chunks[insertpos] = chunk;
    (*new16).base.count += 1;
    verify_node(newnode);

    *parent_slot = newnode;
    free_node(store, node);
    Ok(node16_children(new16).add(insertpos))
}

unsafe fn add_child_4(n4: *mut Node4, chunk: u8) -> *mut RtSlot {
    let count = (*n4).base.count as usize;
    let insertpos = node_4_get_insertpos(n4, chunk, count);
    shift_arrays_for_insert(
        addr_of_mut!((*n4).chunks).cast(),
        node4_children(n4),
        count,
        insertpos,
    );
    (*n4).chunks[insertpos] = chunk;
    (*n4).base.count += 1;
    verify_node(n4 as *mut RtNode);
    node4_children(n4).add(insertpos)
}

#[inline(always)]
unsafe fn node_must_grow(node: *mut RtNode) -> bool {
    (*node).count == (*node).fanout
}

unsafe fn node_insert<S: RtStore>(
    store: &S,
    parent_slot: *mut RtSlot,
    node: *mut RtNode,
    chunk: u8,
) -> PgResult<*mut RtSlot> {
    match (*node).kind {
        RT_NODE_KIND_4 => {
            if node_must_grow(node) {
                return grow_node_4(store, parent_slot, node, chunk);
            }
            Ok(add_child_4(node as *mut Node4, chunk))
        }
        RT_NODE_KIND_16 => {
            if node_must_grow(node) {
                return grow_node_16(store, parent_slot, node, chunk);
            }
            Ok(add_child_16(node as *mut Node16, chunk))
        }
        RT_NODE_KIND_48 => {
            if node_must_grow(node) {
                return grow_node_48(store, parent_slot, node, chunk);
            }
            Ok(add_child_48(node as *mut Node48, chunk))
        }
        RT_NODE_KIND_256 => Ok(add_child_256(node as *mut Node256, chunk)),
        // SAFETY: kind invariant as node_search.
        _ => core::hint::unreachable_unchecked(),
    }
}

#[inline(never)]
unsafe fn extend_down<S: RtStore>(
    store: &S,
    parent_slot: *mut RtSlot,
    key: u64,
    mut shift: i32,
) -> PgResult<*mut RtSlot> {
    let mut node = alloc_node(store, RT_NODE_KIND_4, SizeClass::Class4)?;
    *parent_slot = node;
    shift -= RT_SPAN;

    while shift > 0 {
        let child = alloc_node(store, RT_NODE_KIND_4, SizeClass::Class4)?;
        let n4 = node as *mut Node4;
        (*n4).base.count = 1;
        (*n4).chunks[0] = get_key_chunk(key, shift);
        *node4_children(n4) = child;
        node = child;
        shift -= RT_SPAN;
    }
    debug_assert_eq!(shift, 0);

    let n4 = node as *mut Node4;
    (*n4).chunks[0] = get_key_chunk(key, 0);
    (*n4).base.count = 1;
    Ok(node4_children(n4))
}

unsafe fn get_slot_recursive<S: RtStore>(
    store: &S,
    parent_slot: *mut RtSlot,
    key: u64,
    shift: i32,
    found: &mut bool,
) -> PgResult<*mut RtSlot> {
    let chunk = get_key_chunk(key, shift);
    let node = *parent_slot;
    let slot = node_search(node, chunk);

    if slot.is_null() {
        *found = false;
        let slot = node_insert(store, parent_slot, node, chunk)?;
        if shift == 0 {
            Ok(slot)
        } else {
            extend_down(store, slot, key, shift)
        }
    } else if shift == 0 {
        *found = true;
        Ok(slot)
    } else {
        get_slot_recursive(store, slot, key, shift - RT_SPAN, found)
    }
}

unsafe fn node_iterate_next(node_iter: &mut NodeIter, key: &mut u64, level: usize) -> *mut RtSlot {
    let node = node_iter.node;
    debug_assert!(!node.is_null());
    let key_chunk: u8;
    let slot: *mut RtSlot;

    match (*node).kind {
        RT_NODE_KIND_4 => {
            let n4 = node as *mut Node4;
            if node_iter.idx >= (*n4).base.count as usize {
                return null_mut();
            }
            slot = node4_children(n4).add(node_iter.idx);
            key_chunk = (*n4).chunks[node_iter.idx];
            node_iter.idx += 1;
        }
        RT_NODE_KIND_16 => {
            let n16 = node as *mut Node16;
            if node_iter.idx >= (*n16).base.count as usize {
                return null_mut();
            }
            slot = node16_children(n16).add(node_iter.idx);
            key_chunk = (*n16).chunks[node_iter.idx];
            node_iter.idx += 1;
        }
        RT_NODE_KIND_48 => {
            let n48 = node as *mut Node48;
            let mut chunk = node_iter.idx;
            while chunk < RT_NODE_MAX_SLOTS {
                if node48_is_chunk_used(n48, chunk) {
                    break;
                }
                chunk += 1;
            }
            if chunk >= RT_NODE_MAX_SLOTS {
                return null_mut();
            }
            slot = node48_get_child(n48, chunk);
            key_chunk = chunk as u8;
            node_iter.idx = chunk + 1;
        }
        RT_NODE_KIND_256 => {
            let n256 = node as *mut Node256;
            let mut chunk = node_iter.idx;
            while chunk < RT_NODE_MAX_SLOTS {
                if node256_is_chunk_used(n256, chunk) {
                    break;
                }
                chunk += 1;
            }
            if chunk >= RT_NODE_MAX_SLOTS {
                return null_mut();
            }
            slot = node256_get_child(n256, chunk);
            key_chunk = chunk as u8;
            node_iter.idx = chunk + 1;
        }
        // SAFETY: kind invariant as node_search.
        _ => core::hint::unreachable_unchecked(),
    }

    *key &= !(RT_CHUNK_MASK << (level * RT_SPAN as usize));
    *key |= (key_chunk as u64) << (level * RT_SPAN as usize);
    slot
}

#[inline(never)]
unsafe fn shrink_node_256<S: RtStore>(store: &S, parent_slot: *mut RtSlot, node: *mut RtNode) {
    let n256 = node as *mut Node256;
    // Shrink allocations mirror C's failure surface (ereport on OOM): loud
    // panic rather than a silent shape divergence.
    let newnode = alloc_node(store, RT_NODE_KIND_48, SizeClass::Class48)
        .expect("radix_tree: OOM while shrinking node256");
    let new48 = newnode as *mut Node48;

    copy_common(newnode, node);
    let mut slot_idx = 0usize;
    for i in 0..RT_NODE_MAX_SLOTS {
        if node256_is_chunk_used(n256, i) {
            (*new48).slot_idxs[i] = slot_idx as u8;
            *node48_children(new48).add(slot_idx) = *node256_children(n256).add(i);
            slot_idx += 1;
        }
    }

    debug_assert!(((*n256).base.count as usize) <= BITS_PER_BITMAPWORD);
    (*new48).isset[0] = (1u64 << (*n256).base.count) - 1;

    *parent_slot = newnode;
    free_node(store, node);
}

unsafe fn remove_child_256<S: RtStore>(
    store: &S,
    parent_slot: *mut RtSlot,
    node: *mut RtNode,
    chunk: u8,
) {
    let n256 = node as *mut Node256;
    let idx = chunk as usize / BITS_PER_BITMAPWORD;
    let bitnum = chunk as usize % BITS_PER_BITMAPWORD;
    (*n256).isset[idx] &= !(1u64 << bitnum);
    // A full node256 reads count == 0 (overflow); delete first, then test.
    (*n256).base.count = (*n256).base.count.wrapping_sub(1);
    debug_assert!((*n256).base.count > 0);

    let shrink_threshold = BITS_PER_BITMAPWORD.min(RT_FANOUT_48 / 4 * 3);
    if ((*n256).base.count as usize) <= shrink_threshold {
        shrink_node_256(store, parent_slot, node);
    }
}

#[inline(never)]
unsafe fn shrink_node_48<S: RtStore>(store: &S, parent_slot: *mut RtSlot, node: *mut RtNode) {
    let n48 = node as *mut Node48;
    let newnode = alloc_node(store, RT_NODE_KIND_16, SizeClass::Class16Lo)
        .expect("radix_tree: OOM while shrinking node48");
    let new16 = newnode as *mut Node16;

    copy_common(newnode, node);
    let mut destidx = 0usize;
    for chunk in 0..RT_NODE_MAX_SLOTS {
        if (*n48).slot_idxs[chunk] != RT_INVALID_SLOT_IDX {
            (*new16).chunks[destidx] = chunk as u8;
            *node16_children(new16).add(destidx) =
                *node48_children(n48).add((*n48).slot_idxs[chunk] as usize);
            destidx += 1;
        }
    }
    debug_assert!(destidx < (*new16).base.fanout as usize);
    verify_node(newnode);

    *parent_slot = newnode;
    free_node(store, node);
}

unsafe fn remove_child_48<S: RtStore>(
    store: &S,
    parent_slot: *mut RtSlot,
    node: *mut RtNode,
    chunk: u8,
) {
    let n48 = node as *mut Node48;
    let deletepos = (*n48).slot_idxs[chunk as usize];
    debug_assert!(deletepos != RT_INVALID_SLOT_IDX);

    let idx = deletepos as usize / BITS_PER_BITMAPWORD;
    let bitnum = deletepos as usize % BITS_PER_BITMAPWORD;
    (*n48).isset[idx] &= !(1u64 << bitnum);
    (*n48).slot_idxs[chunk as usize] = RT_INVALID_SLOT_IDX;
    (*n48).base.count -= 1;

    let shrink_threshold = RT_FANOUT_16_LO / 4 * 3;
    if ((*n48).base.count as usize) <= shrink_threshold {
        shrink_node_48(store, parent_slot, node);
    }
}

#[inline(never)]
unsafe fn shrink_node_16<S: RtStore>(
    store: &S,
    parent_slot: *mut RtSlot,
    node: *mut RtNode,
    deletepos: usize,
) {
    let n16 = node as *mut Node16;
    let newnode = alloc_node(store, RT_NODE_KIND_4, SizeClass::Class4)
        .expect("radix_tree: OOM while shrinking node16");
    let new4 = newnode as *mut Node4;

    copy_common(newnode, node);
    copy_arrays_and_delete(
        addr_of_mut!((*new4).chunks).cast(),
        node4_children(new4),
        addr_of!((*n16).chunks).cast(),
        node16_children(n16),
        (*n16).base.count as usize,
        deletepos,
    );
    (*new4).base.count -= 1;
    verify_node(newnode);

    *parent_slot = newnode;
    free_node(store, node);
}

unsafe fn remove_child_16<S: RtStore>(
    store: &S,
    parent_slot: *mut RtSlot,
    node: *mut RtNode,
    chunk: u8,
    slot: *mut RtSlot,
) {
    let n16 = node as *mut Node16;
    let deletepos = slot.offset_from(node16_children(n16)) as usize;

    // Shrink to node4 at count 4: the post-shrink count of 3 is the largest
    // where linear search beats SIMD.
    if (*n16).base.count <= 4 {
        shrink_node_16(store, parent_slot, node, deletepos);
        return;
    }

    debug_assert_eq!((*n16).chunks[deletepos], chunk);
    shift_arrays_and_delete(
        addr_of_mut!((*n16).chunks).cast(),
        node16_children(n16),
        (*n16).base.count as usize,
        deletepos,
    );
    (*n16).base.count -= 1;
}

unsafe fn remove_child_4<S: RtStore>(
    store: &S,
    parent_slot: *mut RtSlot,
    node: *mut RtNode,
    chunk: u8,
    slot: *mut RtSlot,
    is_root: bool,
    root_emptied: &mut bool,
) {
    let n4 = node as *mut Node4;
    if (*n4).base.count == 1 {
        debug_assert_eq!((*n4).chunks[0], chunk);
        if is_root {
            // Keep the empty root node so set() can assume it exists; the
            // caller resets start_shift/max_val.
            (*n4).base.count = 0;
            *root_emptied = true;
        } else {
            free_node(store, node);
            *parent_slot = null_mut();
        }
    } else {
        let deletepos = slot.offset_from(node4_children(n4)) as usize;
        debug_assert_eq!((*n4).chunks[deletepos], chunk);
        shift_arrays_and_delete(
            addr_of_mut!((*n4).chunks).cast(),
            node4_children(n4),
            (*n4).base.count as usize,
            deletepos,
        );
        (*n4).base.count -= 1;
    }
}

unsafe fn node_delete<S: RtStore>(
    store: &S,
    parent_slot: *mut RtSlot,
    node: *mut RtNode,
    chunk: u8,
    slot: *mut RtSlot,
    is_root: bool,
    root_emptied: &mut bool,
) {
    match (*node).kind {
        RT_NODE_KIND_4 => {
            remove_child_4(store, parent_slot, node, chunk, slot, is_root, root_emptied)
        }
        RT_NODE_KIND_16 => remove_child_16(store, parent_slot, node, chunk, slot),
        RT_NODE_KIND_48 => remove_child_48(store, parent_slot, node, chunk),
        RT_NODE_KIND_256 => remove_child_256(store, parent_slot, node, chunk),
        // SAFETY: kind invariant as node_search.
        _ => core::hint::unreachable_unchecked(),
    }
}

unsafe fn delete_recursive<V: RtValue, S: RtStore>(
    store: &S,
    parent_slot: *mut RtSlot,
    key: u64,
    shift: i32,
    is_root: bool,
    root_emptied: &mut bool,
) -> bool {
    let chunk = get_key_chunk(key, shift);
    let node = *parent_slot;
    let slot = node_search(node, chunk);

    if slot.is_null() {
        return false;
    }

    if shift == 0 {
        if !childptr_is_value::<V>(slot) {
            free_leaf::<V, S>(store, *slot);
        }
        node_delete(store, parent_slot, node, chunk, slot, is_root, root_emptied);
        true
    } else {
        let deleted =
            delete_recursive::<V, S>(store, slot, key, shift - RT_SPAN, false, root_emptied);
        if (*slot).is_null() {
            debug_assert!(deleted);
            node_delete(store, parent_slot, node, chunk, slot, is_root, root_emptied);
        }
        deleted
    }
}

unsafe fn free_recurse<V: RtValue, S: RtStore>(store: &S, node: *mut RtNode, shift: i32) {
    // SAFETY: child_slot is a live slot of `node`; leaf-level discrimination
    // follows the shift bookkeeping exactly as the walk that built the tree.
    let free_child = |child_slot: *mut RtSlot| unsafe {
        if shift > 0 {
            free_recurse::<V, S>(store, *child_slot, shift - RT_SPAN);
        } else if !childptr_is_value::<V>(child_slot) {
            free_leaf::<V, S>(store, *child_slot);
        }
    };
    match (*node).kind {
        RT_NODE_KIND_4 => {
            let n4 = node as *mut Node4;
            for i in 0..(*n4).base.count as usize {
                free_child(node4_children(n4).add(i));
            }
        }
        RT_NODE_KIND_16 => {
            let n16 = node as *mut Node16;
            for i in 0..(*n16).base.count as usize {
                free_child(node16_children(n16).add(i));
            }
        }
        RT_NODE_KIND_48 => {
            let n48 = node as *mut Node48;
            for chunk in 0..RT_NODE_MAX_SLOTS {
                if node48_is_chunk_used(n48, chunk) {
                    free_child(node48_get_child(n48, chunk));
                }
            }
        }
        RT_NODE_KIND_256 => {
            let n256 = node as *mut Node256;
            for chunk in 0..RT_NODE_MAX_SLOTS {
                if node256_is_chunk_used(n256, chunk) {
                    free_child(node256_get_child(n256, chunk));
                }
            }
        }
        _ => unreachable!(),
    }
    free_node(store, node);
}

#[cfg(debug_assertions)]
unsafe fn verify_node(node: *mut RtNode) {
    match (*node).kind {
        RT_NODE_KIND_4 => {
            let n4 = node as *mut Node4;
            for i in 1..(*n4).base.count as usize {
                assert!((*n4).chunks[i - 1] < (*n4).chunks[i]);
            }
        }
        RT_NODE_KIND_16 => {
            let n16 = node as *mut Node16;
            for i in 1..(*n16).base.count as usize {
                assert!((*n16).chunks[i - 1] < (*n16).chunks[i]);
            }
        }
        RT_NODE_KIND_48 => {
            let n48 = node as *mut Node48;
            let mut cnt = 0;
            for i in 0..RT_NODE_MAX_SLOTS {
                if !node48_is_chunk_used(n48, i) {
                    continue;
                }
                let slot = (*n48).slot_idxs[i];
                assert!(slot < (*node).fanout);
                assert!(
                    (*n48).isset[slot as usize / BITS_PER_BITMAPWORD]
                        & (1u64 << (slot as usize % BITS_PER_BITMAPWORD))
                        != 0
                );
                cnt += 1;
            }
            assert_eq!((*n48).base.count as usize, cnt);
        }
        RT_NODE_KIND_256 => {
            let n256 = node as *mut Node256;
            let mut cnt = 0usize;
            for w in (*n256).isset {
                cnt += w.count_ones() as usize;
            }
            if cnt == RT_FANOUT_256 {
                assert_eq!((*n256).base.count, 0);
            } else {
                assert_eq!((*n256).base.count as usize, cnt);
            }
        }
        _ => unreachable!(),
    }
}

#[cfg(not(debug_assertions))]
#[inline(always)]
unsafe fn verify_node(_node: *mut RtNode) {}

#[cfg(test)]
mod tests;
