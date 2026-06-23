//! The radix-tree storage template (`src/include/lib/radixtree.h`), as
//! instantiated by `access/common/tidstore.c` for the `BlocktableEntry` value
//! type (`RT_PREFIX local_ts` and `shared_ts`).
//!
//! `radixtree.h` is a generic container template `tidstore.c` `#include`s twice
//! — once for a backend-local (process-heap) tree (`local_ts_*`) and once for a
//! DSA-shared tree guarded by the tree's own LWLock (`shared_ts_*`, the
//! `RT_SHMEM` flavor). This crate owns that container: the node kinds and size
//! classes, the grow/insert/search/iterate walk, the embedded-value tagging,
//! and both the local and the DSA-shared storage flavors. It installs the
//! `backend-lib-radixtree-seams` decls `tidstore.c` calls through.
//!
//! ## Substrate reconciliation
//!
//! The DSA-shared flavor uses the real DSA allocator through
//! `backend-utils-mmgr-dsa-seams` (the `dsa_area *`-keyed surface
//! `dsa_create_ext` / `dsa_attach` / `dsa_allocate_extended` / `dsa_free` /
//! `dsa_get_address` / `dsa_detach` / `dsa_get_total_size`): the radix node
//! pointers are real `dsa_pointer`s, every node/leaf allocation goes through
//! the shared DSA, and the tree's LWLock lives in the shared control object in
//! the DSA segment, taken/released through `backend-storage-lmgr-lwlock-seams`.
//! The local flavor's radix algorithm needs stable raw node addresses, which
//! the idiomatic `Allocation`-token mmgr cannot hand back, so its node/leaf
//! bytes come from the global allocator with a recorded `Layout` for `free`
//! (the C local path is process-heap allocation in `rt_context`; the adaptive
//! radix *algorithm* is identical to the shared one).
//!
//! ## Identity and value wire format
//!
//! A live tree is named by the [`TidStore`] handle `tidstore.c` threads; an
//! in-progress iteration by a [`TidStoreIterHandle`]. The radix *value* is a
//! `BlocktableEntry` crossing the seam as the `Vec<bitmapword>` byte image
//! `tidstore.c` packs/unpacks (`wire[0]` = the packed header slot, `wire[1..]`
//! = the bitmap `words`), mirroring the C in-memory layout where the header
//! occupies one pointer-sized slot immediately followed by `words[]`.

#![allow(non_snake_case)]

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::mem::{align_of, size_of};
use std::ptr::{self, NonNull};
use std::slice;

use lwlock_seams as lwlock;
use ::lmgr_proc_seams::my_proc_number;
use dsa_seams as dsa;

use ::types_core::{BlockNumber, OffsetNumber, Size};
use ::types_dsa::{
    DsaHandle, DsaPointer, DSA_ALLOC_ZERO, DSA_HANDLE_INVALID, INVALID_DSA_POINTER,
};
use ::types_error::{PgError, PgResult};
use ::nodes::bitmapset::{bitmapword, BITS_PER_BITMAPWORD};
use ::types_storage::{DsaArea, LWLock, LWLockMode, LW_EXCLUSIVE, LW_SHARED};
use ::types_vacuum::vacuumlazy::{TidStore, TidStoreIterHandle};

// ===========================================================================
// Radix-tree constants (radixtree.h).
// ===========================================================================

/// A radix slot: a backend-local raw machine address (LOCAL flavor) or a
/// `dsa_pointer` (SHARED flavor). `dsa_pointer` is `u64`, so one type serves
/// both; the storage descriptor resolves it per flavor.
type RtSlot = u64;

const RT_SPAN: usize = 8;
const RT_CHUNK_MASK: u64 = (1 << RT_SPAN) - 1;
const RT_MAX_LEVEL: usize = size_of::<u64>();
const RT_INVALID_SLOT: RtSlot = 0;
const RT_EMBEDDED_VALUE_TAG: RtSlot = 1;

const RT_NODE_KIND_4: u8 = 0;
const RT_NODE_KIND_16: u8 = 1;
const RT_NODE_KIND_48: u8 = 2;
const RT_NODE_KIND_256: u8 = 3;

const RT_NODE_MAX_SLOTS: usize = 1 << RT_SPAN;
const RT_FANOUT_4_MAX: usize = 8 - size_of::<RtNodeHeader>();
const RT_FANOUT_4: usize = 4;
const RT_FANOUT_16_MAX: usize = 32;
const RT_FANOUT_48_MAX: usize = 64;
const RT_FANOUT_256: usize = RT_NODE_MAX_SLOTS;
const RT_BM_WORDS_48: usize = RT_FANOUT_48_MAX / BITS_PER_BITMAPWORD;
const RT_BM_WORDS_256: usize = RT_FANOUT_256 / BITS_PER_BITMAPWORD;
const RT_INVALID_SLOT_IDX: u8 = u8::MAX;
const RT_RADIX_TREE_MAGIC: u32 = 0x54A4_8167;

const NUM_FULL_OFFSETS: usize =
    (size_of::<RtSlot>() - size_of::<u8>() - size_of::<i8>()) / size_of::<OffsetNumber>();

// ===========================================================================
// BlocktableEntry in-memory image (the radix value).
//
// Identical byte layout to the C `BlocktableEntry`: a pointer-sized header
// (`flags` u8, `nwords` i8, NUM_FULL_OFFSETS OffsetNumbers) immediately followed
// by `nwords` bitmap words. This is the same image `tidstore.c` encodes onto the
// `Vec<bitmapword>` wire; the provider converts wire<->image at the seam.
// ===========================================================================

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct BlocktableEntryHeader {
    flags: u8,
    nwords: i8,
    full_offsets: [OffsetNumber; NUM_FULL_OFFSETS],
}

/// An owned radix value built from the seam's `Vec<bitmapword>` wire image.
#[derive(Clone, Debug)]
struct EntryValue {
    header: BlocktableEntryHeader,
    words: Vec<bitmapword>,
}

impl EntryValue {
    /// Decode the wire image (matches `tidstore.c`'s `BlocktableEntry` encode):
    /// `wire[0]` = packed header (byte 0 = flags, byte 1 = nwords as a byte,
    /// then NUM_FULL_OFFSETS little-endian OffsetNumbers), `wire[1..]` = words.
    fn from_wire(wire: &[bitmapword]) -> PgResult<Self> {
        let header_word = *wire
            .first()
            .ok_or_else(|| PgError::error("TidStore radix entry is empty"))?;

        let mut header = BlocktableEntryHeader {
            flags: (header_word & 0xff) as u8,
            nwords: ((header_word >> 8) & 0xff) as u8 as i8,
            full_offsets: [0; NUM_FULL_OFFSETS],
        };
        for (i, slot) in header.full_offsets.iter_mut().enumerate() {
            *slot = ((header_word >> (16 + i * 16)) & 0xffff) as OffsetNumber;
        }

        let body = &wire[1..];
        let mut words: Vec<bitmapword> = Vec::new();
        words
            .try_reserve_exact(body.len())
            .map_err(|_| PgError::error("out of memory"))?;
        for &w in body {
            words.push(w as bitmapword);
        }

        Ok(Self { header, words })
    }

    fn byte_size(&self) -> usize {
        size_of::<BlocktableEntryHeader>() + self.words.len() * size_of::<bitmapword>()
    }

    fn is_embeddable(&self) -> bool {
        self.byte_size() <= size_of::<RtSlot>()
    }

    fn embedded_slot(&self) -> RtSlot {
        debug_assert!(self.is_embeddable());
        let mut slot = 0 as RtSlot;
        unsafe {
            ptr::copy_nonoverlapping(
                ptr::addr_of!(self.header).cast::<u8>(),
                ptr::addr_of_mut!(slot).cast::<u8>(),
                size_of::<BlocktableEntryHeader>(),
            );
        }
        slot | RT_EMBEDDED_VALUE_TAG
    }

    fn write_to(&self, destination: NonNull<BlocktableEntryHeader>) {
        unsafe {
            ptr::copy_nonoverlapping(
                ptr::addr_of!(self.header).cast::<u8>(),
                destination.as_ptr().cast::<u8>(),
                size_of::<BlocktableEntryHeader>(),
            );
            if !self.words.is_empty() {
                let words = destination.as_ptr().add(1).cast::<bitmapword>();
                ptr::copy_nonoverlapping(self.words.as_ptr(), words, self.words.len());
            }
        }
    }
}

/// A borrowed view of a stored radix value, used to produce the wire image on
/// `find` / `iterate`.
#[derive(Clone, Copy)]
struct EntryRef {
    header: NonNull<BlocktableEntryHeader>,
}

impl EntryRef {
    fn header(self) -> &'static BlocktableEntryHeader {
        unsafe { self.header.as_ref() }
    }

    fn words(self) -> &'static [bitmapword] {
        let header = self.header();
        if header.nwords <= 0 {
            return &[];
        }
        unsafe {
            slice::from_raw_parts(
                self.header.as_ptr().add(1).cast::<bitmapword>(),
                header.nwords as usize,
            )
        }
    }

    fn byte_size(self) -> usize {
        size_of::<BlocktableEntryHeader>() + self.words().len() * size_of::<bitmapword>()
    }

    /// Encode this stored value back onto the wire image.
    fn to_wire(self) -> PgResult<Vec<bitmapword>> {
        let header = self.header();
        let words = self.words();

        let mut wire: Vec<bitmapword> = Vec::new();
        wire.try_reserve_exact(1 + words.len())
            .map_err(|_| PgError::error("out of memory"))?;

        let mut header_word: bitmapword = 0;
        header_word |= header.flags as bitmapword;
        header_word |= ((header.nwords as u8) as bitmapword) << 8;
        for (i, &off) in header.full_offsets.iter().enumerate() {
            header_word |= (off as bitmapword) << (16 + i * 16);
        }
        wire.push(header_word);
        for &w in words {
            wire.push(w);
        }
        Ok(wire)
    }
}

// ===========================================================================
// Radix node layouts (radixtree.h).
// ===========================================================================

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct RtNodeHeader {
    kind: u8,
    fanout: u8,
    count: u8,
}

#[repr(C)]
struct RtNode4Prefix {
    base: RtNodeHeader,
    chunks: [u8; RT_FANOUT_4_MAX],
}

#[repr(C)]
struct RtNode16Prefix {
    base: RtNodeHeader,
    chunks: [u8; RT_FANOUT_16_MAX],
}

#[repr(C)]
struct RtNode48Prefix {
    base: RtNodeHeader,
    isset: [bitmapword; RT_BM_WORDS_48],
    slot_idxs: [u8; RT_NODE_MAX_SLOTS],
}

#[repr(C)]
struct RtNode256 {
    base: RtNodeHeader,
    isset: [bitmapword; RT_BM_WORDS_256],
    children: [RtSlot; RT_FANOUT_256],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct LocalRadixControl {
    root: RtSlot,
    max_val: u64,
    num_keys: i64,
    start_shift: i32,
}

// The shared control object's POD layout (everything but the LWLock, which is
// neither `Copy` nor `Clone` and is initialised in place via the lwlock seam).
// `SharedControl` views the DSA control bytes; the LWLock sits at a fixed
// offset after the POD prefix.
#[repr(C)]
struct SharedRadixControl {
    handle: DsaPointer,
    magic: u32,
    lock: LWLock,
    root: RtSlot,
    max_val: u64,
    num_keys: i64,
    start_shift: i32,
}

// ===========================================================================
// Local raw-block allocator.
//
// The faithful local store allocates radix nodes/leaves out of slab
// MemoryContexts and uses their raw addresses as RtSlots. The idiomatic mmgr's
// Allocation tokens are not stable raw pointers, so the LOCAL flavor uses the
// global allocator. Each block is prefixed with an aligned size word so `free`
// can recover its `Layout` (the C MemoryContext chunk header plays the same
// role). The user payload begins `LOCAL_PREFIX` bytes in.
// ===========================================================================

const LOCAL_ALIGN: usize = 16;
const LOCAL_PREFIX: usize = LOCAL_ALIGN; // >= size_of::<usize>(), keeps payload aligned

fn local_alloc_zeroed(size: usize) -> PgResult<NonNull<u8>> {
    let total = size + LOCAL_PREFIX;
    let layout = Layout::from_size_align(total, LOCAL_ALIGN)
        .map_err(|_| PgError::error("invalid TidStore allocation layout"))?;
    // SAFETY: total > 0 (LOCAL_PREFIX > 0).
    let raw = unsafe { alloc_zeroed(layout) };
    let raw = NonNull::new(raw).ok_or_else(|| PgError::error("out of memory"))?;
    unsafe {
        // Record the user `size` in the prefix word for `free`.
        raw.as_ptr().cast::<usize>().write(size);
        Ok(NonNull::new_unchecked(raw.as_ptr().add(LOCAL_PREFIX)))
    }
}

unsafe fn local_free(payload: *mut u8) {
    let base = payload.sub(LOCAL_PREFIX);
    let size = base.cast::<usize>().read();
    let total = size + LOCAL_PREFIX;
    if let Ok(layout) = Layout::from_size_align(total, LOCAL_ALIGN) {
        dealloc(base, layout);
    }
}

// ===========================================================================
// Live trees (registry payloads).
// ===========================================================================

#[derive(Debug)]
struct LocalRadixTree {
    control: LocalRadixControl,
    /// Sum of all live node/leaf byte allocations (the C
    /// `MemoryContextMemAllocated` is unavailable for global-allocator blocks;
    /// this tracks the same quantity the radix tree charged).
    allocated: usize,
}

#[derive(Debug)]
struct SharedRadixTree {
    area: *mut DsaArea,
    control: DsaPointer,
}

#[derive(Clone, Copy, Debug)]
enum StorageFlavor {
    Local,
    Shared,
}

#[derive(Clone, Copy, Debug)]
enum RtSizeClass {
    Class4,
    Class16Lo,
    Class16Hi,
    Class48,
    Class256,
}

#[derive(Clone, Copy)]
struct RtSizeClassInfo {
    fanout: usize,
    allocsize: usize,
}

impl RtSizeClassInfo {
    fn for_class(flavor: StorageFlavor, class: RtSizeClass) -> Self {
        let shared = matches!(flavor, StorageFlavor::Shared);
        match class {
            RtSizeClass::Class4 => Self {
                fanout: RT_FANOUT_4,
                allocsize: node4_children_offset() + RT_FANOUT_4 * size_of::<RtSlot>(),
            },
            RtSizeClass::Class16Lo => {
                let fanout = if shared { rt_fanout_16_lo_shared() } else { 16 };
                Self {
                    fanout,
                    allocsize: node16_children_offset() + fanout * size_of::<RtSlot>(),
                }
            }
            RtSizeClass::Class16Hi => {
                let fanout = if shared {
                    rt_fanout_16_hi_shared()
                } else {
                    RT_FANOUT_16_MAX
                };
                Self {
                    fanout,
                    allocsize: node16_children_offset() + fanout * size_of::<RtSlot>(),
                }
            }
            RtSizeClass::Class48 => {
                let fanout = if shared {
                    rt_fanout_48_shared()
                } else {
                    RT_FANOUT_48_MAX
                };
                Self {
                    fanout,
                    allocsize: node48_children_offset() + fanout * size_of::<RtSlot>(),
                }
            }
            RtSizeClass::Class256 => Self {
                fanout: RT_FANOUT_256,
                allocsize: size_of::<RtNode256>(),
            },
        }
    }
}

// ===========================================================================
// RadixOps — the storage-flavored cursor over a control + backing store.
// ===========================================================================

#[derive(Clone, Copy)]
enum ControlPtr {
    Local(NonNull<LocalRadixControl>),
    Shared(NonNull<SharedRadixControl>),
}

#[derive(Clone, Copy)]
enum Storage {
    Local { allocated: NonNull<usize> },
    Shared { area: *mut DsaArea },
}

#[derive(Clone, Copy)]
struct RadixOps {
    control: ControlPtr,
    storage: Storage,
}

impl RadixOps {
    fn local(tree: &mut LocalRadixTree) -> Self {
        Self {
            control: ControlPtr::Local(NonNull::from(&mut tree.control)),
            storage: Storage::Local {
                allocated: NonNull::from(&mut tree.allocated),
            },
        }
    }

    fn shared(tree: &SharedRadixTree) -> PgResult<Self> {
        let control = shared_control(tree.area, tree.control)?;
        Ok(Self {
            control: ControlPtr::Shared(control),
            storage: Storage::Shared { area: tree.area },
        })
    }

    fn flavor(self) -> StorageFlavor {
        match self.storage {
            Storage::Local { .. } => StorageFlavor::Local,
            Storage::Shared { .. } => StorageFlavor::Shared,
        }
    }

    fn control_ref(self) -> &'static LocalRadixControl {
        match self.control {
            ControlPtr::Local(control) => unsafe { &*control.as_ptr() },
            ControlPtr::Shared(_) => unreachable!("shared controls use shared_control_ref"),
        }
    }

    fn shared_control_ref(self) -> &'static SharedRadixControl {
        match self.control {
            ControlPtr::Shared(control) => unsafe { &*control.as_ptr() },
            ControlPtr::Local(_) => unreachable!("local controls use control_ref"),
        }
    }

    fn root(self) -> RtSlot {
        match self.control {
            ControlPtr::Local(_) => self.control_ref().root,
            ControlPtr::Shared(_) => self.shared_control_ref().root,
        }
    }

    fn max_val(self) -> u64 {
        match self.control {
            ControlPtr::Local(_) => self.control_ref().max_val,
            ControlPtr::Shared(_) => self.shared_control_ref().max_val,
        }
    }

    fn num_keys(self) -> i64 {
        match self.control {
            ControlPtr::Local(_) => self.control_ref().num_keys,
            ControlPtr::Shared(_) => self.shared_control_ref().num_keys,
        }
    }

    fn start_shift(self) -> i32 {
        match self.control {
            ControlPtr::Local(_) => self.control_ref().start_shift,
            ControlPtr::Shared(_) => self.shared_control_ref().start_shift,
        }
    }

    fn set_root(self, root: RtSlot) {
        unsafe {
            *self.root_slot_mut_ptr() = root;
        }
    }

    fn set_max_val(self, max_val: u64) {
        match self.control {
            ControlPtr::Local(mut control) => unsafe { control.as_mut().max_val = max_val },
            ControlPtr::Shared(mut control) => unsafe { control.as_mut().max_val = max_val },
        }
    }

    fn set_start_shift(self, start_shift: i32) {
        match self.control {
            ControlPtr::Local(mut control) => unsafe { control.as_mut().start_shift = start_shift },
            ControlPtr::Shared(mut control) => unsafe {
                control.as_mut().start_shift = start_shift
            },
        }
    }

    fn increment_num_keys(self) {
        match self.control {
            ControlPtr::Local(mut control) => unsafe { control.as_mut().num_keys += 1 },
            ControlPtr::Shared(mut control) => unsafe { control.as_mut().num_keys += 1 },
        }
    }

    fn root_slot_mut_ptr(self) -> *mut RtSlot {
        match self.control {
            ControlPtr::Local(control) => unsafe { ptr::addr_of_mut!((*control.as_ptr()).root) },
            ControlPtr::Shared(control) => unsafe { ptr::addr_of_mut!((*control.as_ptr()).root) },
        }
    }

    fn initialize_root(self) -> PgResult<()> {
        let root = self.alloc_node(RT_NODE_KIND_4, RtSizeClass::Class4)?;
        self.set_root(root);
        self.set_start_shift(0);
        self.set_max_val(shift_get_max_val(0));
        Ok(())
    }

    fn set(self, key: u64, value: &EntryValue) -> PgResult<bool> {
        let mut found = false;

        if key > self.max_val() {
            if self.num_keys() == 0 {
                let start_shift = key_get_shift(key);
                let root = self.root();
                let root_node = self.node(root)?;
                unsafe {
                    let n4 = root_node.as_ptr().cast::<RtNode4Prefix>();
                    (*n4).base.count = 1;
                    (*n4).chunks[0] = get_key_chunk(key, start_shift);
                }
                let root_children = self.node_children(root_node)?;
                let slot = self.extend_down(root_children, key, start_shift)?;
                self.write_value_slot(slot, false, value)?;
                self.set_start_shift(start_shift as i32);
                self.set_max_val(shift_get_max_val(start_shift));
                self.increment_num_keys();
                return Ok(false);
            }

            self.extend_up(key)?;
        }

        let slot = self.get_slot_recursive(
            self.root_slot_mut_ptr(),
            key,
            self.start_shift(),
            &mut found,
        )?;
        self.write_value_slot(slot, found, value)?;
        if !found {
            self.increment_num_keys();
        }
        Ok(found)
    }

    fn find(self, key: u64) -> PgResult<Option<EntryRef>> {
        if key > self.max_val() {
            return Ok(None);
        }

        let mut slot: *mut RtSlot = ptr::null_mut();
        let mut node_slot = self.root();
        let mut shift = self.start_shift();

        while shift >= 0 {
            let node = self.node(node_slot)?;
            let Some(found_slot) = self.node_search(node, get_key_chunk(key, shift as usize))?
            else {
                return Ok(None);
            };
            slot = found_slot;
            node_slot = unsafe { *slot };
            shift -= RT_SPAN as i32;
        }

        if slot.is_null() {
            return Ok(None);
        }

        self.entry_from_slot(slot).map(Some)
    }

    fn begin_iterate(self) -> RadixIterState {
        let top_level = self.start_shift() / RT_SPAN as i32;
        let mut node_iters = [RtNodeIter::default(); RT_MAX_LEVEL];
        node_iters[top_level as usize] = RtNodeIter {
            node: self.root(),
            idx: 0,
        };

        RadixIterState {
            top_level,
            cur_level: top_level,
            key: 0,
            node_iters,
        }
    }

    fn iterate_next(self, iter: &mut RadixIterState) -> PgResult<Option<(u64, EntryRef)>> {
        while iter.cur_level <= iter.top_level {
            let level = iter.cur_level;
            let Some((slot, key_chunk)) =
                self.node_iterate_next(&mut iter.node_iters[level as usize])?
            else {
                iter.cur_level += 1;
                continue;
            };

            let shift = level as usize * RT_SPAN;
            iter.key &= !(RT_CHUNK_MASK << shift);
            iter.key |= (key_chunk as u64) << shift;

            if level == 0 {
                return self
                    .entry_from_slot(slot)
                    .map(|entry| Some((iter.key, entry)));
            }

            let node = unsafe { *slot };
            iter.cur_level -= 1;
            iter.node_iters[iter.cur_level as usize] = RtNodeIter { node, idx: 0 };
        }

        Ok(None)
    }

    fn free_shared_tree(self) -> PgResult<()> {
        debug_assert!(matches!(self.storage, Storage::Shared { .. }));
        if self.root() != RT_INVALID_SLOT {
            self.free_recurse(self.root(), self.start_shift())?;
        }
        if let ControlPtr::Shared(mut control) = self.control {
            unsafe {
                control.as_mut().magic = 0;
            }
        }
        self.free_slot(self.shared_control_ref().handle)
    }

    fn free_recurse(self, node_slot: RtSlot, shift: i32) -> PgResult<()> {
        let node = self.node(node_slot)?;
        let kind = node_kind(node);

        match kind {
            RT_NODE_KIND_4 | RT_NODE_KIND_16 => {
                let count = node_count(node);
                let children = self.node_children(node)?;
                for i in 0..count {
                    let child = unsafe { *children.add(i) };
                    if shift > 0 {
                        self.free_recurse(child, shift - RT_SPAN as i32)?;
                    } else if !slot_is_embedded_value(child) {
                        self.free_slot(child)?;
                    }
                }
            }
            RT_NODE_KIND_48 => {
                let n48 = node.as_ptr().cast::<RtNode48Prefix>();
                for chunk in 0..RT_NODE_MAX_SLOTS {
                    if node48_chunk_used(n48, chunk as u8) {
                        let child = unsafe { *node48_child_slot(n48, chunk as u8) };
                        if shift > 0 {
                            self.free_recurse(child, shift - RT_SPAN as i32)?;
                        } else if !slot_is_embedded_value(child) {
                            self.free_slot(child)?;
                        }
                    }
                }
            }
            RT_NODE_KIND_256 => {
                let n256 = node.as_ptr().cast::<RtNode256>();
                for chunk in 0..RT_NODE_MAX_SLOTS {
                    if node256_chunk_used(n256, chunk as u8) {
                        let child = unsafe { (*n256).children[chunk] };
                        if shift > 0 {
                            self.free_recurse(child, shift - RT_SPAN as i32)?;
                        } else if !slot_is_embedded_value(child) {
                            self.free_slot(child)?;
                        }
                    }
                }
            }
            _ => return Err(PgError::error("invalid TidStore radix node kind")),
        }

        self.free_slot(node_slot)
    }

    fn extend_up(self, key: u64) -> PgResult<()> {
        let target_shift = key_get_shift(key);
        let mut shift = self.start_shift() as usize;

        while shift < target_shift {
            let node = self.alloc_node(RT_NODE_KIND_4, RtSizeClass::Class4)?;
            let header = self.node(node)?;
            unsafe {
                let n4 = header.as_ptr().cast::<RtNode4Prefix>();
                (*n4).base.count = 1;
                (*n4).chunks[0] = 0;
                *self.node_children(header)? = self.root();
            }
            self.set_root(node);
            shift += RT_SPAN;
        }

        self.set_max_val(shift_get_max_val(target_shift));
        self.set_start_shift(target_shift as i32);
        Ok(())
    }

    fn extend_down(
        self,
        parent_slot: *mut RtSlot,
        key: u64,
        mut shift: usize,
    ) -> PgResult<*mut RtSlot> {
        if shift == 0 {
            return Ok(parent_slot);
        }

        let child = self.alloc_node(RT_NODE_KIND_4, RtSizeClass::Class4)?;
        unsafe {
            *parent_slot = child;
        }

        let mut node = child;
        shift -= RT_SPAN;
        while shift > 0 {
            let child = self.alloc_node(RT_NODE_KIND_4, RtSizeClass::Class4)?;
            let header = self.node(node)?;
            unsafe {
                let n4 = header.as_ptr().cast::<RtNode4Prefix>();
                (*n4).base.count = 1;
                (*n4).chunks[0] = get_key_chunk(key, shift);
                *self.node_children(header)? = child;
            }
            node = child;
            shift -= RT_SPAN;
        }

        let header = self.node(node)?;
        unsafe {
            let n4 = header.as_ptr().cast::<RtNode4Prefix>();
            (*n4).base.count = 1;
            (*n4).chunks[0] = get_key_chunk(key, 0);
        }
        self.node_children(header)
    }

    fn get_slot_recursive(
        self,
        parent_slot: *mut RtSlot,
        key: u64,
        shift: i32,
        found: &mut bool,
    ) -> PgResult<*mut RtSlot> {
        let chunk = get_key_chunk(key, shift as usize);
        let node_slot = unsafe { *parent_slot };
        let node = self.node(node_slot)?;

        if let Some(slot) = self.node_search(node, chunk)? {
            if shift == 0 {
                *found = true;
                return Ok(slot);
            }
            return self.get_slot_recursive(slot, key, shift - RT_SPAN as i32, found);
        }

        *found = false;
        let slot = self.node_insert(parent_slot, node_slot, node, chunk)?;
        if shift == 0 {
            Ok(slot)
        } else {
            self.extend_down(slot, key, shift as usize)
        }
    }

    fn write_value_slot(self, slot: *mut RtSlot, found: bool, value: &EntryValue) -> PgResult<()> {
        let current = unsafe { *slot };
        if value.is_embeddable() {
            if found && !slot_is_embedded_value(current) {
                self.free_slot(current)?;
            }
            unsafe {
                *slot = value.embedded_slot();
            }
            return Ok(());
        }

        let size = value.byte_size();
        let leaf = if found && !slot_is_embedded_value(current) {
            let current_entry = self.entry_from_child(current)?;
            if current_entry.byte_size() == size {
                current
            } else {
                self.free_slot(current)?;
                self.alloc_leaf(size)?
            }
        } else {
            self.alloc_leaf(size)?
        };

        value.write_to(self.leaf_header(leaf)?);
        unsafe {
            *slot = leaf;
        }
        Ok(())
    }

    fn entry_from_slot(self, slot: *mut RtSlot) -> PgResult<EntryRef> {
        let child = unsafe { *slot };
        if slot_is_embedded_value(child) {
            let header = NonNull::new(slot.cast::<BlocktableEntryHeader>())
                .ok_or_else(|| PgError::error("TidStore embedded value pointer is null"))?;
            Ok(EntryRef { header })
        } else {
            self.entry_from_child(child)
        }
    }

    fn entry_from_child(self, child: RtSlot) -> PgResult<EntryRef> {
        self.leaf_header(child).map(|header| EntryRef { header })
    }

    fn node_insert(
        self,
        parent_slot: *mut RtSlot,
        node_slot: RtSlot,
        node: NonNull<RtNodeHeader>,
        chunk: u8,
    ) -> PgResult<*mut RtSlot> {
        if node_must_grow(node) {
            return match node_kind(node) {
                RT_NODE_KIND_4 => self.grow_node4(parent_slot, node_slot, node, chunk),
                RT_NODE_KIND_16 => self.grow_node16(parent_slot, node_slot, node, chunk),
                RT_NODE_KIND_48 => self.grow_node48(parent_slot, node_slot, node, chunk),
                RT_NODE_KIND_256 => self.add_child256(node, chunk),
                _ => Err(PgError::error("invalid TidStore radix node kind")),
            };
        }

        match node_kind(node) {
            RT_NODE_KIND_4 => self.add_child4(node, chunk),
            RT_NODE_KIND_16 => self.add_child16(node, chunk),
            RT_NODE_KIND_48 => self.add_child48(node, chunk),
            RT_NODE_KIND_256 => self.add_child256(node, chunk),
            _ => Err(PgError::error("invalid TidStore radix node kind")),
        }
    }

    fn grow_node4(
        self,
        parent_slot: *mut RtSlot,
        old_slot: RtSlot,
        old_node: NonNull<RtNodeHeader>,
        chunk: u8,
    ) -> PgResult<*mut RtSlot> {
        let new_slot = self.alloc_node(RT_NODE_KIND_16, RtSizeClass::Class16Lo)?;
        let new_node = self.node(new_slot)?;
        let insertpos = node4_insertpos(old_node.as_ptr().cast(), chunk, RT_FANOUT_4);
        unsafe {
            copy_arrays_for_insert(
                node16_chunks_mut(new_node).as_mut_ptr(),
                self.node_children(new_node)?,
                node4_chunks(old_node).as_ptr(),
                self.node_children(old_node)?,
                RT_FANOUT_4,
                insertpos,
            );
            *node16_chunks_mut(new_node).get_unchecked_mut(insertpos) = chunk;
            (*new_node.as_ptr()).count = node_count(old_node).wrapping_add(1) as u8;
            *parent_slot = new_slot;
        }
        self.free_node(old_slot)?;
        unsafe { Ok(self.node_children(new_node)?.add(insertpos)) }
    }

    fn grow_node16(
        self,
        parent_slot: *mut RtSlot,
        old_slot: RtSlot,
        old_node: NonNull<RtNodeHeader>,
        chunk: u8,
    ) -> PgResult<*mut RtSlot> {
        let old_fanout = node_fanout(old_node);
        let high_fanout = RtSizeClassInfo::for_class(self.flavor(), RtSizeClass::Class16Hi).fanout;
        if old_fanout < high_fanout {
            let new_slot = self.alloc_node(RT_NODE_KIND_16, RtSizeClass::Class16Hi)?;
            let new_node = self.node(new_slot)?;
            let insertpos = node16_insertpos(old_node.as_ptr().cast(), chunk);
            unsafe {
                copy_arrays_for_insert(
                    node16_chunks_mut(new_node).as_mut_ptr(),
                    self.node_children(new_node)?,
                    node16_chunks(old_node).as_ptr(),
                    self.node_children(old_node)?,
                    old_fanout,
                    insertpos,
                );
                *node16_chunks_mut(new_node).get_unchecked_mut(insertpos) = chunk;
                (*new_node.as_ptr()).count = node_count(old_node).wrapping_add(1) as u8;
                *parent_slot = new_slot;
            }
            self.free_node(old_slot)?;
            unsafe { Ok(self.node_children(new_node)?.add(insertpos)) }
        } else {
            let new_slot = self.alloc_node(RT_NODE_KIND_48, RtSizeClass::Class48)?;
            let new_node = self.node(new_slot)?;
            let n48 = new_node.as_ptr().cast::<RtNode48Prefix>();
            let count = node_count(old_node);
            unsafe {
                (*new_node.as_ptr()).count = count as u8;
                for i in 0..count {
                    let old_chunk = node16_chunks(old_node)[i];
                    (*n48).slot_idxs[old_chunk as usize] = i as u8;
                    *self.node_children(new_node)?.add(i) = *self.node_children(old_node)?.add(i);
                    node48_set_child_slot_used(n48, i);
                }
                let insertpos = count;
                (*n48).slot_idxs[chunk as usize] = insertpos as u8;
                node48_set_child_slot_used(n48, insertpos);
                (*new_node.as_ptr()).count = (*new_node.as_ptr()).count.wrapping_add(1);
                *parent_slot = new_slot;
                self.free_node(old_slot)?;
                Ok(self.node_children(new_node)?.add(insertpos))
            }
        }
    }

    fn grow_node48(
        self,
        parent_slot: *mut RtSlot,
        old_slot: RtSlot,
        old_node: NonNull<RtNodeHeader>,
        chunk: u8,
    ) -> PgResult<*mut RtSlot> {
        let new_slot = self.alloc_node(RT_NODE_KIND_256, RtSizeClass::Class256)?;
        let new_node = self.node(new_slot)?;
        let old48 = old_node.as_ptr().cast::<RtNode48Prefix>();
        let new256 = new_node.as_ptr().cast::<RtNode256>();
        unsafe {
            (*new_node.as_ptr()).count = (*old_node.as_ptr()).count;
            for old_chunk in 0..RT_NODE_MAX_SLOTS {
                if node48_chunk_used(old48, old_chunk as u8) {
                    let child = *node48_child_slot(old48, old_chunk as u8);
                    (*new256).children[old_chunk] = child;
                    node256_set_chunk_used(new256, old_chunk as u8);
                }
            }
            *parent_slot = new_slot;
        }
        self.free_node(old_slot)?;
        self.add_child256(new_node, chunk)
    }

    fn add_child4(self, node: NonNull<RtNodeHeader>, chunk: u8) -> PgResult<*mut RtSlot> {
        let count = node_count(node);
        let insertpos = node4_insertpos(node.as_ptr().cast(), chunk, count);
        unsafe {
            shift_arrays_for_insert(
                node4_chunks_mut(node).as_mut_ptr(),
                self.node_children(node)?,
                count,
                insertpos,
            );
            node4_chunks_mut(node)[insertpos] = chunk;
            (*node.as_ptr()).count = (*node.as_ptr()).count.wrapping_add(1);
            Ok(self.node_children(node)?.add(insertpos))
        }
    }

    fn add_child16(self, node: NonNull<RtNodeHeader>, chunk: u8) -> PgResult<*mut RtSlot> {
        let count = node_count(node);
        let insertpos = node16_insertpos(node.as_ptr().cast(), chunk);
        unsafe {
            shift_arrays_for_insert(
                node16_chunks_mut(node).as_mut_ptr(),
                self.node_children(node)?,
                count,
                insertpos,
            );
            node16_chunks_mut(node)[insertpos] = chunk;
            (*node.as_ptr()).count = (*node.as_ptr()).count.wrapping_add(1);
            Ok(self.node_children(node)?.add(insertpos))
        }
    }

    fn add_child48(self, node: NonNull<RtNodeHeader>, chunk: u8) -> PgResult<*mut RtSlot> {
        let n48 = node.as_ptr().cast::<RtNode48Prefix>();
        let fanout = node_fanout(node);
        for insertpos in 0..fanout {
            if !node48_child_slot_used(n48, insertpos) {
                unsafe {
                    node48_set_child_slot_used(n48, insertpos);
                    (*n48).slot_idxs[chunk as usize] = insertpos as u8;
                    (*node.as_ptr()).count = (*node.as_ptr()).count.wrapping_add(1);
                    return Ok(self.node_children(node)?.add(insertpos));
                }
            }
        }
        Err(PgError::error("TidStore node48 has no free child slot"))
    }

    fn add_child256(self, node: NonNull<RtNodeHeader>, chunk: u8) -> PgResult<*mut RtSlot> {
        let n256 = node.as_ptr().cast::<RtNode256>();
        unsafe {
            node256_set_chunk_used(n256, chunk);
            (*node.as_ptr()).count = (*node.as_ptr()).count.wrapping_add(1);
            Ok(ptr::addr_of_mut!((*n256).children[chunk as usize]))
        }
    }

    fn node_search(self, node: NonNull<RtNodeHeader>, chunk: u8) -> PgResult<Option<*mut RtSlot>> {
        match node_kind(node) {
            RT_NODE_KIND_4 => {
                let chunks = node4_chunks(node);
                for i in 0..node_count(node) {
                    if chunks[i] == chunk {
                        return unsafe { Ok(Some(self.node_children(node)?.add(i))) };
                    }
                }
                Ok(None)
            }
            RT_NODE_KIND_16 => {
                let chunks = node16_chunks(node);
                for i in 0..node_count(node) {
                    if chunks[i] == chunk {
                        return unsafe { Ok(Some(self.node_children(node)?.add(i))) };
                    }
                }
                Ok(None)
            }
            RT_NODE_KIND_48 => {
                let n48 = node.as_ptr().cast::<RtNode48Prefix>();
                if node48_chunk_used(n48, chunk) {
                    Ok(Some(node48_child_slot(n48, chunk)))
                } else {
                    Ok(None)
                }
            }
            RT_NODE_KIND_256 => {
                let n256 = node.as_ptr().cast::<RtNode256>();
                if node256_chunk_used(n256, chunk) {
                    unsafe { Ok(Some(ptr::addr_of_mut!((*n256).children[chunk as usize]))) }
                } else {
                    Ok(None)
                }
            }
            _ => Err(PgError::error("invalid TidStore radix node kind")),
        }
    }

    fn node_iterate_next(self, iter: &mut RtNodeIter) -> PgResult<Option<(*mut RtSlot, u8)>> {
        let node = self.node(iter.node)?;
        match node_kind(node) {
            RT_NODE_KIND_4 => {
                if iter.idx >= node_count(node) {
                    return Ok(None);
                }
                let idx = iter.idx;
                iter.idx += 1;
                unsafe {
                    Ok(Some((
                        self.node_children(node)?.add(idx),
                        node4_chunks(node)[idx],
                    )))
                }
            }
            RT_NODE_KIND_16 => {
                if iter.idx >= node_count(node) {
                    return Ok(None);
                }
                let idx = iter.idx;
                iter.idx += 1;
                unsafe {
                    Ok(Some((
                        self.node_children(node)?.add(idx),
                        node16_chunks(node)[idx],
                    )))
                }
            }
            RT_NODE_KIND_48 => {
                let n48 = node.as_ptr().cast::<RtNode48Prefix>();
                for chunk in iter.idx..RT_NODE_MAX_SLOTS {
                    if node48_chunk_used(n48, chunk as u8) {
                        iter.idx = chunk + 1;
                        return Ok(Some((node48_child_slot(n48, chunk as u8), chunk as u8)));
                    }
                }
                Ok(None)
            }
            RT_NODE_KIND_256 => {
                let n256 = node.as_ptr().cast::<RtNode256>();
                for chunk in iter.idx..RT_NODE_MAX_SLOTS {
                    if node256_chunk_used(n256, chunk as u8) {
                        iter.idx = chunk + 1;
                        unsafe {
                            return Ok(Some((
                                ptr::addr_of_mut!((*n256).children[chunk]),
                                chunk as u8,
                            )));
                        }
                    }
                }
                Ok(None)
            }
            _ => Err(PgError::error("invalid TidStore radix node kind")),
        }
    }

    fn alloc_node(self, kind: u8, class: RtSizeClass) -> PgResult<RtSlot> {
        let info = RtSizeClassInfo::for_class(self.flavor(), class);
        let (slot, node_address) = self.alloc_bytes(info.allocsize)?;
        let node = node_address.cast::<RtNodeHeader>();
        unsafe {
            ptr::write_bytes(node.as_ptr().cast::<u8>(), 0, info.allocsize);
            if kind == RT_NODE_KIND_48 {
                let n48 = node.as_ptr().cast::<RtNode48Prefix>();
                (*n48).slot_idxs.fill(RT_INVALID_SLOT_IDX);
            }
            (*node.as_ptr()).kind = kind;
            (*node.as_ptr()).fanout = info.fanout as u8;
            Ok(slot)
        }
    }

    fn alloc_leaf(self, size: usize) -> PgResult<RtSlot> {
        match self.storage {
            Storage::Local { mut allocated } => {
                let payload = local_alloc_zeroed(size)?;
                unsafe { *allocated.as_mut() += size }
                Ok(payload.as_ptr() as RtSlot)
            }
            Storage::Shared { area } => {
                let dp = dsa::dsa_allocate_extended::call(area, size, DSA_ALLOC_ZERO)?;
                if dp == INVALID_DSA_POINTER {
                    return Err(PgError::error("out of memory"));
                }
                Ok(dp)
            }
        }
    }

    fn alloc_bytes(self, size: usize) -> PgResult<(RtSlot, NonNull<u8>)> {
        match self.storage {
            Storage::Local { mut allocated } => {
                let payload = local_alloc_zeroed(size)?;
                unsafe { *allocated.as_mut() += size }
                Ok((payload.as_ptr() as RtSlot, payload))
            }
            Storage::Shared { area } => {
                let dp = dsa::dsa_allocate_extended::call(area, size, DSA_ALLOC_ZERO)?;
                if dp == INVALID_DSA_POINTER {
                    return Err(PgError::error("out of memory"));
                }
                let address = dsa::dsa_get_address_ptr::call(area, dp)?;
                if address == 0 {
                    return Err(PgError::error("could not map TidStore DSA pointer"));
                }
                Ok((dp, unsafe {
                    NonNull::new_unchecked(address as usize as *mut u8)
                }))
            }
        }
    }

    fn free_node(self, slot: RtSlot) -> PgResult<()> {
        self.free_slot(slot)
    }

    fn free_slot(self, slot: RtSlot) -> PgResult<()> {
        match self.storage {
            Storage::Local { .. } => {
                unsafe { local_free(slot as usize as *mut u8) };
                Ok(())
            }
            Storage::Shared { area } => {
                if slot == INVALID_DSA_POINTER {
                    return Err(PgError::error("invalid TidStore DSA pointer"));
                }
                dsa::dsa_free_ptr::call(area, slot)
            }
        }
    }

    fn node(self, slot: RtSlot) -> PgResult<NonNull<RtNodeHeader>> {
        self.address(slot).map(NonNull::cast)
    }

    fn leaf_header(self, slot: RtSlot) -> PgResult<NonNull<BlocktableEntryHeader>> {
        self.address(slot).map(NonNull::cast)
    }

    fn address(self, slot: RtSlot) -> PgResult<NonNull<u8>> {
        if slot == RT_INVALID_SLOT || slot_is_embedded_value(slot) {
            return Err(PgError::error("invalid TidStore radix pointer"));
        }

        match self.storage {
            Storage::Local { .. } => NonNull::new(slot as usize as *mut u8)
                .ok_or_else(|| PgError::error("TidStore local pointer is null")),
            Storage::Shared { area } => {
                let address = dsa::dsa_get_address_ptr::call(area, slot)?;
                if address == 0 {
                    return Err(PgError::error("could not map TidStore DSA pointer"));
                }
                Ok(unsafe { NonNull::new_unchecked(address as usize as *mut u8) })
            }
        }
    }

    fn node_children(self, node: NonNull<RtNodeHeader>) -> PgResult<*mut RtSlot> {
        let offset = match node_kind(node) {
            RT_NODE_KIND_4 => node4_children_offset(),
            RT_NODE_KIND_16 => node16_children_offset(),
            RT_NODE_KIND_48 => node48_children_offset(),
            RT_NODE_KIND_256 => {
                return unsafe {
                    Ok(ptr::addr_of_mut!(
                        (*node.as_ptr().cast::<RtNode256>()).children[0]
                    ))
                }
            }
            _ => return Err(PgError::error("invalid TidStore radix node kind")),
        };
        unsafe { Ok(node.as_ptr().cast::<u8>().add(offset).cast::<RtSlot>()) }
    }
}

#[derive(Clone, Copy, Default)]
struct RtNodeIter {
    node: RtSlot,
    idx: usize,
}

#[derive(Clone, Copy)]
struct RadixIterState {
    top_level: i32,
    cur_level: i32,
    key: u64,
    node_iters: [RtNodeIter; RT_MAX_LEVEL],
}

// ===========================================================================
// Tree lifecycle.
// ===========================================================================

impl LocalRadixTree {
    fn create() -> PgResult<Self> {
        let mut tree = Self {
            control: LocalRadixControl::default(),
            allocated: 0,
        };
        RadixOps::local(&mut tree).initialize_root()?;
        Ok(tree)
    }

    fn destroy(&mut self) -> PgResult<()> {
        // Free the whole node/leaf graph; the C local path deletes rt_context.
        let ops = RadixOps::local(self);
        if ops.root() != RT_INVALID_SLOT {
            ops.free_recurse(ops.root(), ops.start_shift())?;
        }
        Ok(())
    }

    fn memory_usage(&self) -> usize {
        self.allocated
    }
}

impl SharedRadixTree {
    fn create(dsa_init_size: Size, dsa_max_size: Size, tranche_id: i32) -> PgResult<Self> {
        // The caller (`tidstore.c` `TidStoreCreateShared`) has already derived the
        // init/max segment sizes from `max_bytes`; pass them straight to
        // `dsa_create_ext`, as C does.
        let area = dsa::dsa_create_ext::call(tranche_id, dsa_init_size, dsa_max_size)?;
        let control = allocate_shared_control(area, tranche_id)?;
        let tree = Self { area, control };
        RadixOps::shared(&tree)?.initialize_root()?;
        Ok(tree)
    }

    fn attach(area_handle: DsaHandle, handle: DsaPointer) -> PgResult<Self> {
        debug_assert_ne!(area_handle, DSA_HANDLE_INVALID);
        debug_assert_ne!(handle, INVALID_DSA_POINTER);

        let area = dsa::dsa_attach::call(area_handle as ::types_storage::dsa_handle)?;
        let control = shared_control(area, handle)?;
        if unsafe { control.as_ref().magic } != RT_RADIX_TREE_MAGIC {
            return Err(PgError::error("invalid TidStore shared radix tree"));
        }
        Ok(Self {
            area,
            control: handle,
        })
    }

    /// `shared_ts_free` + `dsa_detach` — fully destroy the tree.
    fn destroy(self) -> PgResult<()> {
        let ops = RadixOps::shared(&self)?;
        ops.free_shared_tree()?;
        dsa::dsa_detach_ptr::call(self.area)
    }

    /// `shared_ts_detach` + `dsa_detach` — detach a backend's mapping. The tree
    /// lives entirely inside the DSA area, so detaching the area drops this
    /// backend's mappings without touching the shared data.
    fn detach(self) -> PgResult<()> {
        dsa::dsa_detach_ptr::call(self.area)
    }

    fn lock(&self, mode: LWLockMode) -> PgResult<()> {
        let control = shared_control(self.area, self.control)?;
        let lock: &LWLock = unsafe { &control.as_ref().lock };
        let guard = lwlock::lwlock_acquire::call(lock, mode, my_proc_number::call())?;
        // The lock is held across the seam return (the C `shared_ts_lock_*`
        // holds it until a matching `shared_ts_unlock`); recompute it from the
        // control in `unlock`. C's `LWLockReleaseAll` abort backstop is the
        // process-wide one, not a per-call guard.
        core::mem::forget(guard);
        Ok(())
    }

    fn unlock(&self) -> PgResult<()> {
        let control = shared_control(self.area, self.control)?;
        let lock: &LWLock = unsafe { &control.as_ref().lock };
        lwlock::lwlock_release::call(lock)
    }

    fn memory_usage(&self) -> PgResult<usize> {
        dsa::dsa_get_total_size_ptr::call(self.area)
    }
}

fn allocate_shared_control(area: *mut DsaArea, tranche_id: i32) -> PgResult<DsaPointer> {
    let dp = dsa::dsa_allocate_extended::call(area, size_of::<SharedRadixControl>(), DSA_ALLOC_ZERO)?;
    if dp == INVALID_DSA_POINTER {
        return Err(PgError::error("out of memory"));
    }
    let address = dsa::dsa_get_address_ptr::call(area, dp)?;
    if address == 0 {
        return Err(PgError::error("could not map TidStore shared control"));
    }
    let control = address as usize as *mut SharedRadixControl;
    // The DSA block is zeroed; fill the POD fields and initialise the LWLock in
    // place (LWLock is neither `Copy` nor `Clone` — it can't be assigned).
    unsafe {
        (*control).handle = dp;
        (*control).magic = RT_RADIX_TREE_MAGIC;
        (*control).root = RT_INVALID_SLOT;
        (*control).max_val = 0;
        (*control).num_keys = 0;
        (*control).start_shift = 0;
        lwlock::lwlock_initialize::call(&mut (*control).lock, tranche_id);
    }
    Ok(dp)
}

fn shared_control(area: *mut DsaArea, pointer: DsaPointer) -> PgResult<NonNull<SharedRadixControl>> {
    if pointer == INVALID_DSA_POINTER {
        return Err(PgError::error("invalid TidStore DSA pointer"));
    }
    let address = dsa::dsa_get_address_ptr::call(area, pointer)?;
    if address == 0 {
        return Err(PgError::error("could not map TidStore shared control"));
    }
    Ok(unsafe { NonNull::new_unchecked(address as usize as *mut SharedRadixControl) })
}

// ===========================================================================
// Bit / pointer helpers (radixtree.h).
// ===========================================================================

fn slot_is_embedded_value(slot: RtSlot) -> bool {
    slot & RT_EMBEDDED_VALUE_TAG != 0
}

fn node_kind(node: NonNull<RtNodeHeader>) -> u8 {
    unsafe { node.as_ref().kind }
}

fn node_fanout(node: NonNull<RtNodeHeader>) -> usize {
    if node_kind(node) == RT_NODE_KIND_256 {
        RT_FANOUT_256
    } else {
        unsafe { node.as_ref().fanout as usize }
    }
}

fn node_count(node: NonNull<RtNodeHeader>) -> usize {
    if node_kind(node) == RT_NODE_KIND_256 && unsafe { node.as_ref().count } == 0 {
        RT_FANOUT_256
    } else {
        unsafe { node.as_ref().count as usize }
    }
}

fn node_must_grow(node: NonNull<RtNodeHeader>) -> bool {
    node_kind(node) != RT_NODE_KIND_256 && node_count(node) == node_fanout(node)
}

fn node4_chunks(node: NonNull<RtNodeHeader>) -> &'static [u8; RT_FANOUT_4_MAX] {
    unsafe { &(*node.as_ptr().cast::<RtNode4Prefix>()).chunks }
}

fn node4_chunks_mut(node: NonNull<RtNodeHeader>) -> &'static mut [u8; RT_FANOUT_4_MAX] {
    unsafe { &mut (*node.as_ptr().cast::<RtNode4Prefix>()).chunks }
}

fn node16_chunks(node: NonNull<RtNodeHeader>) -> &'static [u8; RT_FANOUT_16_MAX] {
    unsafe { &(*node.as_ptr().cast::<RtNode16Prefix>()).chunks }
}

fn node16_chunks_mut(node: NonNull<RtNodeHeader>) -> &'static mut [u8; RT_FANOUT_16_MAX] {
    unsafe { &mut (*node.as_ptr().cast::<RtNode16Prefix>()).chunks }
}

fn node4_insertpos(node: *const RtNode4Prefix, chunk: u8, count: usize) -> usize {
    let chunks = unsafe { &(*node).chunks };
    chunks[..count]
        .iter()
        .position(|existing| *existing >= chunk)
        .unwrap_or(count)
}

fn node16_insertpos(node: *const RtNode16Prefix, chunk: u8) -> usize {
    let count = unsafe { (*node).base.count as usize };
    let chunks = unsafe { &(*node).chunks };
    chunks[..count]
        .iter()
        .position(|existing| *existing >= chunk)
        .unwrap_or(count)
}

unsafe fn shift_arrays_for_insert(
    chunks: *mut u8,
    children: *mut RtSlot,
    count: usize,
    insertpos: usize,
) {
    for i in (insertpos..count).rev() {
        unsafe {
            *chunks.add(i + 1) = *chunks.add(i);
            *children.add(i + 1) = *children.add(i);
        }
    }
}

unsafe fn copy_arrays_for_insert(
    dst_chunks: *mut u8,
    dst_children: *mut RtSlot,
    src_chunks: *const u8,
    src_children: *const RtSlot,
    count: usize,
    insertpos: usize,
) {
    for i in 0..count {
        let dest = i + usize::from(i >= insertpos);
        unsafe {
            *dst_chunks.add(dest) = *src_chunks.add(i);
            *dst_children.add(dest) = *src_children.add(i);
        }
    }
}

fn node48_chunk_used(node: *const RtNode48Prefix, chunk: u8) -> bool {
    unsafe { (*node).slot_idxs[chunk as usize] != RT_INVALID_SLOT_IDX }
}

fn node48_child_slot_used(node: *const RtNode48Prefix, slot: usize) -> bool {
    let idx = slot / BITS_PER_BITMAPWORD;
    let bit = slot % BITS_PER_BITMAPWORD;
    unsafe { ((*node).isset[idx] & ((1 as bitmapword) << bit)) != 0 }
}

unsafe fn node48_set_child_slot_used(node: *mut RtNode48Prefix, slot: usize) {
    let idx = slot / BITS_PER_BITMAPWORD;
    let bit = slot % BITS_PER_BITMAPWORD;
    unsafe {
        (*node).isset[idx] |= (1 as bitmapword) << bit;
    }
}

fn node48_child_slot(node: *const RtNode48Prefix, chunk: u8) -> *mut RtSlot {
    unsafe {
        let index = (*node).slot_idxs[chunk as usize] as usize;
        (node
            .cast_mut()
            .cast::<u8>()
            .add(node48_children_offset())
            .cast::<RtSlot>())
        .add(index)
    }
}

fn node256_chunk_used(node: *const RtNode256, chunk: u8) -> bool {
    let idx = chunk as usize / BITS_PER_BITMAPWORD;
    let bit = chunk as usize % BITS_PER_BITMAPWORD;
    unsafe { ((*node).isset[idx] & ((1 as bitmapword) << bit)) != 0 }
}

unsafe fn node256_set_chunk_used(node: *mut RtNode256, chunk: u8) {
    let idx = chunk as usize / BITS_PER_BITMAPWORD;
    let bit = chunk as usize % BITS_PER_BITMAPWORD;
    unsafe {
        (*node).isset[idx] |= (1 as bitmapword) << bit;
    }
}

const fn align_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

const fn node4_children_offset() -> usize {
    align_up(size_of::<RtNode4Prefix>(), align_of::<RtSlot>())
}

const fn node16_children_offset() -> usize {
    align_up(size_of::<RtNode16Prefix>(), align_of::<RtSlot>())
}

const fn node48_children_offset() -> usize {
    align_up(size_of::<RtNode48Prefix>(), align_of::<RtSlot>())
}

const fn rt_fanout_16_lo_shared() -> usize {
    if size_of::<DsaPointer>() < 8 {
        (96 - node16_children_offset()) / size_of::<DsaPointer>()
    } else {
        (160 - node16_children_offset()) / size_of::<DsaPointer>()
    }
}

const fn rt_fanout_16_hi_shared() -> usize {
    let fanout = if size_of::<DsaPointer>() < 8 {
        (160 - node16_children_offset()) / size_of::<DsaPointer>()
    } else {
        (320 - node16_children_offset()) / size_of::<DsaPointer>()
    };
    if fanout < RT_FANOUT_16_MAX {
        fanout
    } else {
        RT_FANOUT_16_MAX
    }
}

const fn rt_fanout_48_shared() -> usize {
    let fanout = if size_of::<DsaPointer>() < 8 {
        (512 - node48_children_offset()) / size_of::<DsaPointer>()
    } else {
        (768 - node48_children_offset()) / size_of::<DsaPointer>()
    };
    if fanout < RT_FANOUT_48_MAX {
        fanout
    } else {
        RT_FANOUT_48_MAX
    }
}

fn key_get_shift(key: u64) -> usize {
    if key == 0 {
        0
    } else {
        ((u64::BITS - 1 - key.leading_zeros()) as usize / RT_SPAN) * RT_SPAN
    }
}

fn shift_get_max_val(shift: usize) -> u64 {
    if shift >= 56 {
        u64::MAX
    } else {
        (1u64 << (shift + RT_SPAN)) - 1
    }
}

fn get_key_chunk(key: u64, shift: usize) -> u8 {
    ((key >> shift) & RT_CHUNK_MASK) as u8
}

// ===========================================================================
// Registry: maps a `TidStore` id to its live tree, and iterator handles to
// in-progress iterations. Single-writer backend model (matches PostgreSQL): a
// thread-local registry, no cross-thread sharing of a backend's stores.
// ===========================================================================

enum Tree {
    Local(LocalRadixTree),
    Shared(SharedRadixTree),
}

struct IterState {
    store_id: u64,
    state: RadixIterState,
}

struct Registry {
    next_id: u64,
    stores: BTreeMap<u64, Tree>,
    iters: BTreeMap<u64, IterState>,
}

thread_local! {
    static REGISTRY: RefCell<Registry> = const {
        RefCell::new(Registry {
            next_id: 1,
            stores: BTreeMap::new(),
            iters: BTreeMap::new(),
        })
    };
}

fn with_registry<R>(f: impl FnOnce(&mut Registry) -> R) -> R {
    REGISTRY.with(|r| f(&mut r.borrow_mut()))
}

// ===========================================================================
// Seam providers. Each marshals the handle -> live tree and delegates to the
// radix algorithm; the radix value crosses as the `Vec<bitmapword>` wire image.
// ===========================================================================

fn provider_create_local(
    _min_context_size: usize,
    _init_block_size: usize,
    _max_block_size: usize,
    _insert_only: bool,
) -> PgResult<TidStore> {
    let tree = LocalRadixTree::create()?;
    with_registry(|reg| {
        let id = reg.next_id;
        reg.next_id += 1;
        reg.stores.insert(id, Tree::Local(tree));
        Ok(TidStore::new(id))
    })
}

fn provider_create_shared(
    dsa_init_size: usize,
    dsa_max_size: usize,
    tranche_id: i32,
) -> PgResult<TidStore> {
    let tree = SharedRadixTree::create(dsa_init_size, dsa_max_size, tranche_id)?;
    with_registry(|reg| {
        let id = reg.next_id;
        reg.next_id += 1;
        reg.stores.insert(id, Tree::Shared(tree));
        Ok(TidStore::new(id))
    })
}

fn provider_attach(area_handle: DsaHandle, handle: DsaPointer) -> PgResult<TidStore> {
    let tree = SharedRadixTree::attach(area_handle, handle)?;
    with_registry(|reg| {
        let id = reg.next_id;
        reg.next_id += 1;
        reg.stores.insert(id, Tree::Shared(tree));
        Ok(TidStore::new(id))
    })
}

fn provider_detach(ts: TidStore) -> PgResult<()> {
    let tree = with_registry(|reg| reg.stores.remove(&ts.id));
    match tree {
        Some(Tree::Shared(tree)) => tree.detach(),
        Some(Tree::Local(_)) => Err(PgError::error("cannot detach from a local TidStore")),
        None => Err(PgError::error("TidStore handle not found")),
    }
}

fn provider_free(ts: TidStore) -> PgResult<()> {
    let tree = with_registry(|reg| reg.stores.remove(&ts.id));
    match tree {
        Some(Tree::Local(mut tree)) => tree.destroy(),
        Some(Tree::Shared(tree)) => tree.destroy(),
        None => Err(PgError::error("TidStore handle not found")),
    }
}

fn provider_lock(ts: TidStore, exclusive: Option<bool>) -> PgResult<()> {
    with_registry(|reg| match reg.stores.get(&ts.id) {
        Some(Tree::Shared(tree)) => match exclusive {
            Some(true) => tree.lock(LW_EXCLUSIVE),
            Some(false) => tree.lock(LW_SHARED),
            None => tree.unlock(),
        },
        // Local stores have no lock (the C TidStoreLock* are no-ops there).
        Some(Tree::Local(_)) => Ok(()),
        None => Err(PgError::error("TidStore handle not found")),
    })
}

fn provider_set(ts: TidStore, blkno: BlockNumber, wire: Vec<bitmapword>) -> PgResult<()> {
    let value = EntryValue::from_wire(&wire)?;
    with_registry(|reg| match reg.stores.get_mut(&ts.id) {
        Some(Tree::Local(tree)) => {
            RadixOps::local(tree).set(blkno as u64, &value)?;
            Ok(())
        }
        Some(Tree::Shared(tree)) => {
            RadixOps::shared(tree)?.set(blkno as u64, &value)?;
            Ok(())
        }
        None => Err(PgError::error("TidStore handle not found")),
    })
}

fn provider_find(ts: TidStore, blkno: BlockNumber) -> PgResult<Option<Vec<bitmapword>>> {
    with_registry(|reg| {
        let ops = match reg.stores.get_mut(&ts.id) {
            Some(Tree::Local(tree)) => RadixOps::local(tree),
            Some(Tree::Shared(tree)) => RadixOps::shared(tree)?,
            None => return Err(PgError::error("TidStore handle not found")),
        };
        match ops.find(blkno as u64)? {
            Some(entry) => Ok(Some(entry.to_wire()?)),
            None => Ok(None),
        }
    })
}

fn provider_memory_usage(ts: TidStore) -> PgResult<usize> {
    with_registry(|reg| match reg.stores.get(&ts.id) {
        Some(Tree::Local(tree)) => Ok(tree.memory_usage()),
        Some(Tree::Shared(tree)) => tree.memory_usage(),
        None => Err(PgError::error("TidStore handle not found")),
    })
}

fn provider_get_handle(ts: TidStore) -> PgResult<DsaPointer> {
    with_registry(|reg| match reg.stores.get(&ts.id) {
        Some(Tree::Shared(tree)) => Ok(tree.control),
        Some(Tree::Local(_)) => Err(PgError::error(
            "TidStoreGetHandle called on a local TidStore",
        )),
        None => Err(PgError::error("TidStore handle not found")),
    })
}

fn provider_get_dsa(ts: TidStore) -> PgResult<DsaHandle> {
    with_registry(|reg| match reg.stores.get(&ts.id) {
        Some(Tree::Shared(tree)) => Ok(dsa::dsa_get_handle::call(tree.area) as DsaHandle),
        Some(Tree::Local(_)) => Err(PgError::error("TidStoreGetDSA called on a local TidStore")),
        None => Err(PgError::error("TidStore handle not found")),
    })
}

fn provider_begin_iterate(ts: TidStore) -> PgResult<TidStoreIterHandle> {
    with_registry(|reg| {
        let state = match reg.stores.get_mut(&ts.id) {
            Some(Tree::Local(tree)) => RadixOps::local(tree).begin_iterate(),
            Some(Tree::Shared(tree)) => RadixOps::shared(tree)?.begin_iterate(),
            None => return Err(PgError::error("TidStore handle not found")),
        };
        let id = reg.next_id;
        reg.next_id += 1;
        reg.iters.insert(
            id,
            IterState {
                store_id: ts.id,
                state,
            },
        );
        Ok(TidStoreIterHandle::new(id))
    })
}

fn provider_iterate_next(
    iter: TidStoreIterHandle,
) -> PgResult<Option<(BlockNumber, Vec<bitmapword>)>> {
    with_registry(|reg| {
        // Pull the iterator state out, run the step against its store, write back.
        let mut it = match reg.iters.remove(&iter.id) {
            Some(it) => it,
            None => return Err(PgError::error("TidStore iterator not found")),
        };
        let result = {
            let ops = match reg.stores.get_mut(&it.store_id) {
                Some(Tree::Local(tree)) => RadixOps::local(tree),
                Some(Tree::Shared(tree)) => RadixOps::shared(tree)?,
                None => {
                    return Err(PgError::error("TidStore iterator's store not found"));
                }
            };
            ops.iterate_next(&mut it.state)?
        };
        let out = match result {
            Some((key, entry)) => Some((key as BlockNumber, entry.to_wire()?)),
            None => None,
        };
        reg.iters.insert(iter.id, it);
        Ok(out)
    })
}

fn provider_end_iterate(iter: TidStoreIterHandle) -> PgResult<()> {
    with_registry(|reg| {
        reg.iters.remove(&iter.id);
        Ok(())
    })
}

/// Install the real radix-tree provider for the `backend-lib-radixtree-seams`
/// declarations. The LOCAL store path is self-contained; the SHARED path issues
/// real `dsa_*` / `LWLock*` calls and therefore requires the DSM/DSA
/// shared-memory provider to be initialised first (the same prerequisite the C
/// `shared_ts_*` faces).
pub fn init_seams() {
    use radixtree_seams as seams;
    seams::radixtree_create_local::set(provider_create_local);
    seams::radixtree_create_shared::set(provider_create_shared);
    seams::radixtree_attach::set(provider_attach);
    seams::radixtree_detach::set(provider_detach);
    seams::radixtree_free::set(provider_free);
    seams::radixtree_lock::set(provider_lock);
    seams::radixtree_set::set(provider_set);
    seams::radixtree_find::set(provider_find);
    seams::radixtree_begin_iterate::set(provider_begin_iterate);
    seams::radixtree_iterate_next::set(provider_iterate_next);
    seams::radixtree_end_iterate::set(provider_end_iterate);
    seams::radixtree_memory_usage::set(provider_memory_usage);
    seams::radixtree_get_handle::set(provider_get_handle);
    seams::radixtree_get_dsa::set(provider_get_dsa);
}
