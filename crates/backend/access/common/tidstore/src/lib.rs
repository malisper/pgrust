//! tidstore.c: TID set keyed by block number; the radix value is a
//! BlocktableEntry (inline header offsets or an offset bitmap).
//! `SharedTidStore` renders the dsa/LWLock flavor thread-native.

use core::marker::PhantomData;
use core::mem::offset_of;
use core::ptr::NonNull;
use std::sync::{RwLockReadGuard, RwLockWriteGuard};

use mcx::Mcx;
use radixtree::{LocalStore, RadixTree, RtStore, RtValue, SharedRadixTree, SharedStore, Tree};
use types_core::{BlockNumber, OffsetNumber};
use types_error::{PgError, PgResult};
use types_tuple::{
    InvalidOffsetNumber, ItemPointerData, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
    MaxOffsetNumber,
};

type BitmapWord = u64;
const BITS_PER_BITMAPWORD: usize = 64;

const NUM_FULL_OFFSETS: usize = (8 - 1 - 1) / core::mem::size_of::<OffsetNumber>();

pub const MAX_OFFSET_IN_BITMAP: OffsetNumber = {
    let bitmap_max = (BITS_PER_BITMAPWORD * i8::MAX as usize - 1) as u32;
    if bitmap_max < MaxOffsetNumber as u32 {
        bitmap_max as OffsetNumber
    } else {
        MaxOffsetNumber
    }
};

const fn words_per_page(n: usize) -> usize {
    n / BITS_PER_BITMAPWORD + 1
}

pub const MAX_BLOCKTABLE_ENTRY_SIZE: usize = offset_of!(BlocktableEntry, words)
    + core::mem::size_of::<BitmapWord>() * words_per_page(MAX_OFFSET_IN_BITMAP as usize);

#[repr(C)]
pub struct BlocktableEntry {
    // Byte 0: reserved so the radix tree can tag embedded entries' low bit.
    #[allow(dead_code)]
    flags: u8,
    nwords: i8,
    full_offsets: [OffsetNumber; NUM_FULL_OFFSETS],
    words: [BitmapWord; 0],
}

const _: () = {
    assert!(offset_of!(BlocktableEntry, words) == 8);
    assert!(core::mem::align_of::<BlocktableEntry>() == 8);
    assert!(MAX_BLOCKTABLE_ENTRY_SIZE == 8 + 8 * 33);
};

// SAFETY: plain bytes; the header alone determines the image size; embedded
// images (nwords == 0) get the tree's tag bit in `flags`, which no reader uses.
unsafe impl RtValue for BlocktableEntry {
    const VARLEN: bool = true;
    const RUNTIME_EMBEDDABLE: bool = true;

    fn value_size(&self) -> usize {
        offset_of!(BlocktableEntry, words)
            + core::mem::size_of::<BitmapWord>() * self.nwords as usize
    }
}

impl BlocktableEntry {
    // The full image extends past size_of::<BlocktableEntry>() (flexible
    // array): tail reads must go through the tree's raw pointer — a `&self`
    // retag grants only the 8-byte header (Miri).
    #[inline]
    unsafe fn word(page: NonNull<BlocktableEntry>, wordnum: usize) -> BitmapWord {
        // SAFETY: caller guarantees wordnum < nwords; the full image sits
        // behind `page`.
        unsafe {
            debug_assert!(wordnum < (*page.as_ptr()).nwords as usize);
            page.as_ptr()
                .cast::<u8>()
                .add(offset_of!(BlocktableEntry, words))
                .cast::<BitmapWord>()
                .add(wordnum)
                .read()
        }
    }

    unsafe fn is_member(page: NonNull<BlocktableEntry>, off: OffsetNumber) -> bool {
        // SAFETY: live entry image behind the tree; no mutation while read.
        unsafe {
            let nwords = (*page.as_ptr()).nwords;
            if nwords == 0 {
                (*page.as_ptr()).full_offsets.contains(&off)
            } else {
                let wordnum = off as usize / BITS_PER_BITMAPWORD;
                let bitnum = off as usize % BITS_PER_BITMAPWORD;
                if wordnum >= nwords as usize {
                    return false;
                }
                Self::word(page, wordnum) & (1 << bitnum) != 0
            }
        }
    }
}

#[repr(C, align(8))]
struct EntryBuf {
    #[allow(dead_code)]
    bytes: [u8; MAX_BLOCKTABLE_ENTRY_SIZE],
}

fn set_block_offsets_in_tree<S: RtStore>(
    tree: &mut Tree<BlocktableEntry, S>,
    blkno: BlockNumber,
    offsets: &[OffsetNumber],
) -> PgResult<()> {
    debug_assert!(!offsets.is_empty());
    #[cfg(debug_assertions)]
    for i in 1..offsets.len() {
        debug_assert!(offsets[i] > offsets[i - 1]);
    }

    let mut buf = core::mem::MaybeUninit::<EntryBuf>::uninit();
    let page = buf.as_mut_ptr().cast::<BlocktableEntry>();
    let num_offsets = offsets.len();

    // SAFETY: aligned max-size image; as C, only the header is zeroed — the
    // copied image is header + words[0..nwords], all written below (the range
    // checks bound nwords).
    unsafe {
        core::ptr::write_bytes(page.cast::<u8>(), 0, offset_of!(BlocktableEntry, words));
        if num_offsets <= NUM_FULL_OFFSETS {
            // zip: no bounds checks (num_offsets <= NUM_FULL_OFFSETS here).
            for (slot, &off) in (*page).full_offsets.iter_mut().zip(offsets.iter()) {
                if off == InvalidOffsetNumber || off > MAX_OFFSET_IN_BITMAP {
                    return Err(offset_out_of_range(off));
                }
                *slot = off;
            }
            (*page).nwords = 0;
        } else {
            let words = page
                .cast::<u8>()
                .add(offset_of!(BlocktableEntry, words))
                .cast::<BitmapWord>();
            let last = offsets[num_offsets - 1] as usize;
            let mut idx = 0usize;
            let mut wordnum = 0usize;
            let mut next_word_threshold = BITS_PER_BITMAPWORD;
            while wordnum <= last / BITS_PER_BITMAPWORD {
                let mut word: BitmapWord = 0;
                while idx < num_offsets {
                    let off = offsets[idx];
                    if off == InvalidOffsetNumber || off > MAX_OFFSET_IN_BITMAP {
                        return Err(offset_out_of_range(off));
                    }
                    if off as usize >= next_word_threshold {
                        break;
                    }
                    word |= 1 << (off as usize % BITS_PER_BITMAPWORD);
                    idx += 1;
                }
                words.add(wordnum).write(word);
                wordnum += 1;
                next_word_threshold += BITS_PER_BITMAPWORD;
            }
            (*page).nwords = wordnum as i8;
            debug_assert_eq!(wordnum, words_per_page(last));
        }

        tree.set_ptr(blkno as u64, page)?;
    }
    Ok(())
}

fn is_member_in_tree<S: RtStore>(tree: &Tree<BlocktableEntry, S>, tid: &ItemPointerData) -> bool {
    let blk = ItemPointerGetBlockNumber(tid);
    let off = ItemPointerGetOffsetNumber(tid);

    let Some(page) = tree.find_ptr(blk as u64) else {
        return false;
    };
    unsafe { BlocktableEntry::is_member(page, off) }
}

pub struct TidStore {
    tree: RadixTree<BlocktableEntry>,
}

impl TidStore {
    /// `max_bytes` is a sizing hint only; drop is TidStoreDestroy. C caps the
    /// context maxBlockSize at max_bytes/16 — mcx arenas grow on their own ladder.
    pub fn create_local(parent: Mcx<'_>, _max_bytes: usize, insert_only: bool) -> PgResult<TidStore> {
        let rt_context = if insert_only {
            parent.context().new_child_bump("TID storage")
        } else {
            parent.context().new_child("TID storage")
        };
        Ok(TidStore { tree: RadixTree::create_in(rt_context)? })
    }

    /// Creates or replaces the entry for `blkno`; `offsets` sorted ascending.
    pub fn set_block_offsets(
        &mut self,
        blkno: BlockNumber,
        offsets: &[OffsetNumber],
    ) -> PgResult<()> {
        set_block_offsets_in_tree(&mut self.tree, blkno, offsets)
    }

    pub fn is_member(&self, tid: &ItemPointerData) -> bool {
        is_member_in_tree(&self.tree, tid)
    }

    /// Ascending block order; the borrow enforces C's no-modify contract.
    pub fn begin_iterate(&self) -> TidStoreIter<'_> {
        TidStoreIter { iter: self.tree.begin_iterate() }
    }

    pub fn memory_usage(&self) -> usize {
        self.tree.memory_usage() as usize
    }

    pub fn num_blocks(&self) -> i64 {
        self.tree.num_keys()
    }
}

/// C's caller-held LockExclusive/LockShare/Unlock become RAII guards; workers
/// share the store by reference instead of dsa handles.
pub struct SharedTidStore {
    tree: SharedRadixTree<BlocktableEntry>,
}

impl SharedTidStore {
    /// `max_bytes` sized C's dsa segments; `tranche_id` named the LWLock tranche.
    pub fn create_shared(_max_bytes: usize, _tranche_id: i32) -> PgResult<SharedTidStore> {
        Ok(SharedTidStore { tree: SharedRadixTree::create()? })
    }

    pub fn attach() -> ! {
        unported("TidStoreAttach (parallel-vacuum lane: thread-native handle plumbing)");
    }

    pub fn detach(&self) -> ! {
        unported("TidStoreDetach (parallel-vacuum lane: thread-native handle plumbing)");
    }

    pub fn get_handle(&self) -> ! {
        unported("TidStoreGetHandle (parallel-vacuum lane: thread-native handle plumbing)");
    }

    pub fn get_dsa(&self) -> ! {
        unported("TidStoreGetDSA (parallel-vacuum lane: thread-native handle plumbing)");
    }

    pub fn lock_exclusive(&self) -> SharedTidStoreExclusive<'_> {
        SharedTidStoreExclusive { tree: self.tree.lock_exclusive() }
    }

    pub fn lock_share(&self) -> SharedTidStoreShare<'_> {
        SharedTidStoreShare { tree: self.tree.lock_share() }
    }

    pub fn memory_usage(&self) -> usize {
        self.tree.memory_usage() as usize
    }
}

pub struct SharedTidStoreExclusive<'a> {
    tree: RwLockWriteGuard<'a, Tree<BlocktableEntry, SharedStore>>,
}

impl SharedTidStoreExclusive<'_> {
    pub fn set_block_offsets(
        &mut self,
        blkno: BlockNumber,
        offsets: &[OffsetNumber],
    ) -> PgResult<()> {
        set_block_offsets_in_tree(&mut self.tree, blkno, offsets)
    }

    pub fn is_member(&self, tid: &ItemPointerData) -> bool {
        is_member_in_tree(&self.tree, tid)
    }
}

pub struct SharedTidStoreShare<'a> {
    tree: RwLockReadGuard<'a, Tree<BlocktableEntry, SharedStore>>,
}

impl SharedTidStoreShare<'_> {
    pub fn is_member(&self, tid: &ItemPointerData) -> bool {
        is_member_in_tree(&self.tree, tid)
    }

    pub fn begin_iterate(&self) -> TidStoreIter<'_, SharedStore> {
        TidStoreIter { iter: self.tree.begin_iterate() }
    }
}

pub struct TidStoreIter<'t, S: RtStore = LocalStore> {
    iter: radixtree::TreeIter<'t, BlocktableEntry, S>,
}

pub struct TidStoreIterResult<'t> {
    pub blkno: BlockNumber,
    internal_page: NonNull<BlocktableEntry>,
    _tree: PhantomData<&'t BlocktableEntry>,
}

impl<'t, S: RtStore> TidStoreIter<'t, S> {
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<TidStoreIterResult<'t>> {
        let (key, page) = self.iter.next_ptr()?;
        Some(TidStoreIterResult {
            blkno: key as BlockNumber,
            internal_page: page,
            _tree: PhantomData,
        })
    }
}

impl TidStoreIterResult<'_> {
    /// Fills `offsets` ascending; a return value exceeding `offsets.len()`
    /// reports the required buffer size.
    pub fn block_offsets(&self, offsets: &mut [OffsetNumber]) -> usize {
        // SAFETY: the entry image is pinned by the iterator's tree borrow;
        // reads stay on the raw pointer (header retag covers 8 bytes only).
        let page = self.internal_page;
        let nwords = unsafe { (*page.as_ptr()).nwords };
        let max_offsets = offsets.len();
        let mut num_offsets = 0usize;

        if nwords == 0 {
            for &off in unsafe { &(*page.as_ptr()).full_offsets } {
                if off != InvalidOffsetNumber {
                    if num_offsets < max_offsets {
                        offsets[num_offsets] = off;
                    }
                    num_offsets += 1;
                }
            }
        } else {
            for wordnum in 0..nwords as usize {
                let mut w = unsafe { BlocktableEntry::word(page, wordnum) };
                let mut off = wordnum * BITS_PER_BITMAPWORD;
                while w != 0 {
                    if w & 1 != 0 {
                        if num_offsets < max_offsets {
                            offsets[num_offsets] = off as OffsetNumber;
                        }
                        num_offsets += 1;
                    }
                    off += 1;
                    w >>= 1;
                }
            }
        }
        num_offsets
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn offset_out_of_range(off: OffsetNumber) -> Box<PgError> {
    PgError::error(format!("tuple offset out of range: {off}")).into()
}

#[cold]
#[inline(never)]
fn unported(unit: &'static str) -> ! {
    panic!("unported callee reached from tidstore.c: {unit}");
}

#[cfg(test)]
mod tests;
