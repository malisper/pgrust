//! Family: **tidbitmap** — `nodes/tidbitmap.c`, the `TIDBitmap` used by bitmap
//! index/heap scans.
//!
//! A `TIDBitmap` maps block numbers to (exact or lossy) per-page tuple-offset
//! bitmaps, with a private hash table (`pagetable`), lossification under a
//! work-mem budget, union/intersect, and private + DSA-shared iterators.
//!
//! # Representation & opacity
//!
//! In C `struct TIDBitmap`, `struct TBMPrivateIterator`, and `struct
//! TBMSharedIterator` are genuinely private — the header forward-declares them
//! and `tidbitmap.c` owns the layout (opacity inherited, not introduced). This
//! module IS `tidbitmap.c`, so it defines the real internal structs:
//!  * [`PagetableEntry`] — the per-page hash entry (`words[]` bitmap), 1:1 with
//!    the C struct.
//!  * [`Pagetable`] — the `lib/simplehash.h` table `tidbitmap.c` instantiates
//!    (`SH_PREFIX pagetable`, keyed by `blockno`, `murmurhash32`), ported as the
//!    same Robin-Hood open-addressing layout over an owned `Vec`.
//!  * [`TidBitmapInner`] — `struct TIDBitmap`, owned.
//!
//! The opaque carriers in `types_tidbitmap` (`TIDBitmap`,
//! `TBMPrivateIterator`, `TBMSharedIterator`) hold the real owned interior by
//! value (boxed as `dyn Any`, the only way a below-it vocabulary crate can name
//! a struct private to this module), exactly as C palloc's the real `struct
//! TIDBitmap`. There is no side table and no integer key: the carrier *is* the
//! owning `TIDBitmap *`. The consumers (executor, table-AM, index AMs through
//! `amgetbitmap`) hold and pass that real carrier; `tbm_add_tuple` /
//! `tbm_add_tuples` take `&mut TIDBitmap` and mutate the live bitmap directly.
//!
//! # Owned seams
//!
//! This family owns and installs: `tbm_add_tuple` (in
//! `backend-nodes-core-seams`) and `tbm_prepare_shared_iterate` /
//! `tbm_begin_iterate` / `tbm_end_iterate` / `tbm_free` /
//! `tbm_free_shared_area` (in `backend-nodes-core-tidbitmap-seams`). The
//! private path is fully in-crate. The shared (parallel, DSA-backed) path needs
//! the DSA area, its cross-backend `refcount` atomics, and the iterator
//! `LWLock` — primitives owned by a not-yet-ported DSA-backed shared-TBM
//! provider — so its DSA marshaling routes through the
//! [`dsa_shared_tbm_prepare`] / [`dsa_shared_tbm_free`] sub-seams, which
//! seam-and-panic until that owner lands (`mirror-pg-and-panic`).

#![allow(clippy::identity_op)]

use common_hashfn::murmurhash32;
use types_core::{BlockNumber, InvalidBlockNumber, OffsetNumber, BLCKSZ};
use types_error::{PgError, PgResult};
use types_execparallel::DsaAreaHandle;
use types_nodes::bitmapset::bitmapword;
use types_storage::bufpage::MaxHeapTuplesPerPage;
use types_tuple::heaptuple::ItemPointerData;

// ===========================================================================
// Macro-equivalent constants/helpers (tidbitmap.c / bitmapset.h)
// ===========================================================================

/// `BITS_PER_BITMAPWORD` (`nodes/bitmapset.h`): bits in a `bitmapword`
/// (`bitmapword == u64`).
const BITS_PER_BITMAPWORD: usize = core::mem::size_of::<bitmapword>() * 8;

/// `TBM_MAX_TUPLES_PER_PAGE` (`nodes/tidbitmap.h`): `MaxHeapTuplesPerPage`.
pub const TBM_MAX_TUPLES_PER_PAGE: usize = MaxHeapTuplesPerPage;

/// `PAGES_PER_CHUNK` (`tidbitmap.c`): `BLCKSZ / 32`.
const PAGES_PER_CHUNK: usize = BLCKSZ / 32;

/// `WORDS_PER_PAGE` (`tidbitmap.c`): active words for an exact page.
const WORDS_PER_PAGE: usize = (TBM_MAX_TUPLES_PER_PAGE - 1) / BITS_PER_BITMAPWORD + 1;

/// `WORDS_PER_CHUNK` (`tidbitmap.c`): active words for a lossy chunk.
const WORDS_PER_CHUNK: usize = (PAGES_PER_CHUNK - 1) / BITS_PER_BITMAPWORD + 1;

/// `Max(WORDS_PER_PAGE, WORDS_PER_CHUNK)`: the size of `PagetableEntry.words[]`.
const WORDS_PER_ENTRY: usize = if WORDS_PER_PAGE > WORDS_PER_CHUNK {
    WORDS_PER_PAGE
} else {
    WORDS_PER_CHUNK
};

/// `WORDNUM(x)` (`tidbitmap.c`): `x / BITS_PER_BITMAPWORD`.
#[inline]
fn wordnum(x: usize) -> usize {
    x / BITS_PER_BITMAPWORD
}

/// `BITNUM(x)` (`tidbitmap.c`): `x % BITS_PER_BITMAPWORD`.
#[inline]
fn bitnum(x: usize) -> usize {
    x % BITS_PER_BITMAPWORD
}

/// `pg_cmp_u32(a, b)` (`common/int.h`), as an `Ordering`.
#[inline]
fn pg_cmp_u32(a: u32, b: u32) -> core::cmp::Ordering {
    a.cmp(&b)
}

fn out_of_memory(what: &str) -> PgError {
    PgError::error(format!("out of memory (tidbitmap: {what})"))
}

fn hash_table_too_large() -> PgError {
    PgError::error("hash table too large")
}

// ===========================================================================
// PagetableEntry — the hash entry (tidbitmap.c).
// ===========================================================================

/// `struct PagetableEntry` (`tidbitmap.c`): a per-page hash entry. For an exact
/// page, `blockno` is the page number and bit `k` of `words` represents tuple
/// offset `k + 1`. For a lossy chunk, `blockno` is the first page in the chunk
/// and bit `k` represents page `blockno + k`.
#[derive(Clone, Debug)]
struct PagetableEntry {
    /// `BlockNumber blockno` — page number (hashtable key).
    blockno: BlockNumber,
    /// `char status` — simplehash entry status byte.
    status: i8,
    /// `bool ischunk` — T = lossy storage, F = exact.
    ischunk: bool,
    /// `bool recheck` — should the tuples be rechecked? (exact pages only).
    recheck: bool,
    /// `bitmapword words[Max(WORDS_PER_PAGE, WORDS_PER_CHUNK)]`.
    words: [bitmapword; WORDS_PER_ENTRY],
}

impl Default for PagetableEntry {
    fn default() -> Self {
        // MemSet(page, 0, ...) — all fields zeroed.
        PagetableEntry {
            blockno: 0,
            status: SH_STATUS_EMPTY,
            ischunk: false,
            recheck: false,
            words: [0; WORDS_PER_ENTRY],
        }
    }
}

// ===========================================================================
// Pagetable — `lib/simplehash.h` open-addressing (Robin Hood) table,
// instantiated by tidbitmap.c (SH_PREFIX pagetable). Ported 1:1.
// ===========================================================================

/// `SH_STATUS_EMPTY` (`simplehash.h`): the `status` byte of an unused slot.
const SH_STATUS_EMPTY: i8 = 0x00;
/// `SH_STATUS_IN_USE` (`simplehash.h`): the `status` byte of an occupied slot.
const SH_STATUS_IN_USE: i8 = 0x01;

/// `SH_MAX_SIZE` (`simplehash.h`): `(uint64) PG_UINT32_MAX + 1`.
const SH_MAX_SIZE: u64 = (u32::MAX as u64) + 1;
/// `SH_FILLFACTOR` (`simplehash.h`).
const SH_FILLFACTOR: f64 = 0.9;
/// `SH_MAX_FILLFACTOR` (`simplehash.h`).
const SH_MAX_FILLFACTOR: f64 = 0.98;
/// `SH_GROW_MAX_DIB` (`simplehash.h`).
const SH_GROW_MAX_DIB: u32 = 25;
/// `SH_GROW_MAX_MOVE` (`simplehash.h`).
const SH_GROW_MAX_MOVE: i32 = 150;
/// `SH_GROW_MIN_FILLFACTOR` (`simplehash.h`).
const SH_GROW_MIN_FILLFACTOR: f64 = 0.1;

/// `pg_nextpower2_64(num)` (`port/pg_bitutils.h`): smallest power of 2 >= `num`,
/// for `num >= 1`.
fn pg_nextpower2_64(num: u64) -> u64 {
    debug_assert!(num > 0);
    1u64 << (64 - (num - 1).leading_zeros())
}

/// `SH_COMPUTE_SIZE(newsize)` (`simplehash.h`).
fn sh_compute_size(newsize: u64) -> u64 {
    // supporting zero sized hashes would complicate matters
    let mut size = newsize.max(2);
    // round up size to the next power of 2, that's how bucketing works
    size = pg_nextpower2_64(size);
    debug_assert!(size <= SH_MAX_SIZE);
    size
}

/// `pagetable_iterator` (the `SH_ITERATOR`).
#[derive(Clone, Copy, Debug)]
struct PagetableIterator {
    cur: u32,
    end: u32,
    done: bool,
}

#[inline]
fn iter_new() -> PagetableIterator {
    PagetableIterator {
        cur: 0,
        end: 0,
        done: false,
    }
}

/// `pagetable_hash` (the `SH_TYPE`): the open-addressing table keyed by
/// `blockno`. Owns its bucket array as a `Vec<PagetableEntry>`; an empty slot is
/// one whose `status == SH_STATUS_EMPTY`.
///
/// The `SH_USE_NONDEFAULT_ALLOCATOR` callbacks (`pagetable_allocate` /
/// `pagetable_free`) exist in C only to route element allocation through DSA
/// when a DSA is present; the owned `Vec` makes that unnecessary for the
/// in-crate (private) path, and the DSA layout is produced separately by
/// [`tbm_prepare_shared_iterate`].
struct Pagetable {
    /// size of bucket array (power of two)
    size: u64,
    /// how many elements have valid contents
    members: u32,
    /// mask for bucket and size calculations, based on size
    sizemask: u32,
    /// boundary after which to grow hashtable
    grow_threshold: u32,
    /// hash buckets
    data: Vec<PagetableEntry>,
}

impl Pagetable {
    /// `SH_INITIAL_BUCKET(tb, hash)`.
    #[inline]
    fn initial_bucket(&self, hash: u32) -> u32 {
        hash & self.sizemask
    }

    /// `SH_NEXT(tb, curelem, startelem)`.
    #[inline]
    fn next(&self, curelem: u32) -> u32 {
        curelem.wrapping_add(1) & self.sizemask
    }

    /// `SH_PREV(tb, curelem, startelem)`.
    #[inline]
    fn prev(&self, curelem: u32) -> u32 {
        curelem.wrapping_sub(1) & self.sizemask
    }

    /// `SH_DISTANCE_FROM_OPTIMAL(tb, optimal, bucket)`.
    #[inline]
    fn distance_from_optimal(&self, optimal: u32, bucket: u32) -> u32 {
        if optimal <= bucket {
            bucket - optimal
        } else {
            ((self.size + bucket as u64) - optimal as u64) as u32
        }
    }

    /// `SH_ENTRY_HASH(tb, entry)`: `murmurhash32(entry->blockno)`.
    #[inline]
    fn entry_hash(&self, idx: usize) -> u32 {
        murmurhash32(self.data[idx].blockno)
    }

    /// `SH_UPDATE_PARAMETERS(tb, newsize)`.
    fn update_parameters(&mut self, newsize: u64) {
        let size = sh_compute_size(newsize);
        self.size = size;
        self.sizemask = (size - 1) as u32;
        if self.size == SH_MAX_SIZE {
            self.grow_threshold = (self.size as f64 * SH_MAX_FILLFACTOR) as u32;
        } else {
            self.grow_threshold = (self.size as f64 * SH_FILLFACTOR) as u32;
        }
    }

    /// `pagetable_create(ctx, nelements, private_data)` (`SH_CREATE`).
    fn create(nelements: u32) -> PgResult<Self> {
        // increase nelements by fillfactor, want to store nelements elements
        let mut size = (SH_MAX_SIZE as f64).min(nelements as f64 / SH_FILLFACTOR) as u64;
        size = sh_compute_size(size);

        let mut tb = Pagetable {
            size: 0,
            members: 0,
            sizemask: 0,
            grow_threshold: 0,
            data: Vec::new(),
        };
        tb.alloc_data(size)?;
        tb.update_parameters(size);
        Ok(tb)
    }

    /// Allocate the (zeroed) bucket array of `size` entries (the
    /// `pagetable_allocate` callback, plus the `SH_COMPUTE_SIZE` "hash table too
    /// large" guard). OOM is a recoverable error rather than an abort.
    fn alloc_data(&mut self, size: u64) -> PgResult<()> {
        let size = usize::try_from(size).map_err(|_| hash_table_too_large())?;
        if core::mem::size_of::<PagetableEntry>()
            .checked_mul(size)
            .map(|bytes| bytes >= usize::MAX / 2)
            .unwrap_or(true)
        {
            return Err(hash_table_too_large());
        }
        let mut data: Vec<PagetableEntry> = Vec::new();
        data.try_reserve_exact(size)
            .map_err(|_| out_of_memory("pagetable bucket array"))?;
        data.resize_with(size, PagetableEntry::default);
        self.data = data;
        Ok(())
    }

    /// `pagetable_grow(tb, newsize)` (`SH_GROW`): rehash into a larger array.
    fn grow(&mut self, newsize: u64) -> PgResult<()> {
        let oldsize = self.size;
        debug_assert!(oldsize == pg_nextpower2_64(oldsize));
        debug_assert!(oldsize != SH_MAX_SIZE);
        debug_assert!(oldsize < newsize);

        let newsize = sh_compute_size(newsize);

        let olddata = core::mem::take(&mut self.data);
        self.alloc_data(newsize)?;
        self.update_parameters(newsize);

        // search for the first element in the hash that's not wrapped around
        let mut startelem: u32 = 0;
        for i in 0..oldsize as u32 {
            let oldentry = &olddata[i as usize];
            if oldentry.status != SH_STATUS_IN_USE {
                startelem = i;
                break;
            }
            let hash = murmurhash32(oldentry.blockno);
            let optimal = self.initial_bucket(hash);
            if optimal == i {
                startelem = i;
                break;
            }
        }

        // and copy all elements in the old table
        let mut copyelem = startelem;
        for _ in 0..oldsize {
            if olddata[copyelem as usize].status == SH_STATUS_IN_USE {
                let hash = murmurhash32(olddata[copyelem as usize].blockno);
                let startelem2 = self.initial_bucket(hash);
                let mut curelem = startelem2;

                // find empty element to put data into
                while self.data[curelem as usize].status != SH_STATUS_EMPTY {
                    curelem = self.next(curelem);
                }

                // copy entry to new slot
                self.data[curelem as usize] = olddata[copyelem as usize].clone();
            }

            // can't use SH_NEXT here, would use new size
            copyelem += 1;
            if (copyelem as u64) >= oldsize {
                copyelem = 0;
            }
        }
        Ok(())
    }

    /// `SH_INSERT_HASH_INTERNAL(tb, key, hash, found)`.
    fn insert_hash_internal(&mut self, key: BlockNumber, hash: u32) -> PgResult<(usize, bool)> {
        'restart: loop {
            let mut insertdist: u32 = 0;

            if self.members >= self.grow_threshold {
                if self.size == SH_MAX_SIZE {
                    return Err(PgError::error("hash table size exceeded"));
                }
                self.grow(self.size * 2)?;
            }

            let startelem = self.initial_bucket(hash);
            let mut curelem = startelem;
            loop {
                // any empty bucket can directly be used
                if self.data[curelem as usize].status == SH_STATUS_EMPTY {
                    self.members += 1;
                    self.data[curelem as usize].blockno = key;
                    self.data[curelem as usize].status = SH_STATUS_IN_USE;
                    return Ok((curelem as usize, false));
                }

                // SH_COMPARE_KEYS: a == b
                if self.data[curelem as usize].blockno == key {
                    debug_assert!(self.data[curelem as usize].status == SH_STATUS_IN_USE);
                    return Ok((curelem as usize, true));
                }

                let curhash = self.entry_hash(curelem as usize);
                let curoptimal = self.initial_bucket(curhash);
                let curdist = self.distance_from_optimal(curoptimal, curelem);

                if insertdist > curdist {
                    let mut emptyelem = curelem;
                    let mut emptydist: i32 = 0;

                    // find next empty bucket
                    loop {
                        emptyelem = self.next(emptyelem);
                        if self.data[emptyelem as usize].status == SH_STATUS_EMPTY {
                            break;
                        }
                        emptydist += 1;
                        if emptydist > SH_GROW_MAX_MOVE
                            && (self.members as f64 / self.size as f64) >= SH_GROW_MIN_FILLFACTOR
                        {
                            self.grow_threshold = 0;
                            continue 'restart;
                        }
                    }

                    // shift forward, starting at last occupied element
                    let mut lastelem = emptyelem;
                    let mut moveelem = emptyelem;
                    while moveelem != curelem {
                        moveelem = self.prev(moveelem);
                        self.data[lastelem as usize] = self.data[moveelem as usize].clone();
                        lastelem = moveelem;
                    }

                    // and fill the now empty spot
                    self.members += 1;
                    self.data[curelem as usize].blockno = key;
                    self.data[curelem as usize].status = SH_STATUS_IN_USE;
                    return Ok((curelem as usize, false));
                }

                curelem = self.next(curelem);
                insertdist += 1;

                if insertdist > SH_GROW_MAX_DIB
                    && (self.members as f64 / self.size as f64) >= SH_GROW_MIN_FILLFACTOR
                {
                    self.grow_threshold = 0;
                    continue 'restart;
                }
            }
        }
    }

    /// `pagetable_insert(tb, key, found)` (`SH_INSERT`).
    fn insert(&mut self, key: BlockNumber) -> PgResult<(usize, bool)> {
        let hash = murmurhash32(key);
        self.insert_hash_internal(key, hash)
    }

    /// `SH_LOOKUP_HASH_INTERNAL(tb, key, hash)`.
    fn lookup_hash_internal(&self, key: BlockNumber, hash: u32) -> Option<usize> {
        let startelem = self.initial_bucket(hash);
        let mut curelem = startelem;
        loop {
            let entry = &self.data[curelem as usize];
            if entry.status == SH_STATUS_EMPTY {
                return None;
            }
            debug_assert!(entry.status == SH_STATUS_IN_USE);
            if entry.blockno == key {
                return Some(curelem as usize);
            }
            curelem = self.next(curelem);
        }
    }

    /// `pagetable_lookup(tb, key)` (`SH_LOOKUP`).
    fn lookup(&self, key: BlockNumber) -> Option<usize> {
        let hash = murmurhash32(key);
        self.lookup_hash_internal(key, hash)
    }

    /// `pagetable_delete(tb, key)` (`SH_DELETE`): whether `key` was present
    /// (backward-shift delete).
    fn delete(&mut self, key: BlockNumber) -> bool {
        let hash = murmurhash32(key);
        let startelem = self.initial_bucket(hash);
        let mut curelem = startelem;
        loop {
            let entry = &self.data[curelem as usize];
            if entry.status == SH_STATUS_EMPTY {
                return false;
            }

            if entry.status == SH_STATUS_IN_USE && entry.blockno == key {
                let mut lastelem = curelem;
                self.members -= 1;

                // Backward shift following elements.
                loop {
                    curelem = self.next(curelem);
                    if self.data[curelem as usize].status != SH_STATUS_IN_USE {
                        self.data[lastelem as usize].status = SH_STATUS_EMPTY;
                        break;
                    }

                    let curhash = self.entry_hash(curelem as usize);
                    let curoptimal = self.initial_bucket(curhash);

                    // current is at optimal position, done
                    if curoptimal == curelem {
                        self.data[lastelem as usize].status = SH_STATUS_EMPTY;
                        break;
                    }

                    // shift
                    self.data[lastelem as usize] = self.data[curelem as usize].clone();
                    lastelem = curelem;
                }
                return true;
            }

            curelem = self.next(curelem);
        }
    }

    /// `pagetable_start_iterate(tb, iter)` (`SH_START_ITERATE`).
    fn start_iterate(&self, iter: &mut PagetableIterator) {
        let mut startelem: u64 = u64::MAX;
        // Search for the first empty element.
        for i in 0..self.size as u32 {
            if self.data[i as usize].status != SH_STATUS_IN_USE {
                startelem = i as u64;
                break;
            }
        }
        debug_assert!(startelem < SH_MAX_SIZE);
        iter.cur = startelem as u32;
        iter.end = iter.cur;
        iter.done = false;
    }

    /// `pagetable_start_iterate_at(tb, iter, at)` (`SH_START_ITERATE_AT`).
    fn start_iterate_at(&self, iter: &mut PagetableIterator, at: u32) {
        iter.cur = at & self.sizemask; // ensure at is within a valid range
        iter.end = iter.cur;
        iter.done = false;
    }

    /// `pagetable_iterate(tb, iter)` (`SH_ITERATE`): index of the next occupied
    /// entry (backwards), or `None`.
    fn iterate(&self, iter: &mut PagetableIterator) -> Option<usize> {
        debug_assert!((iter.cur as u64) < self.size);
        debug_assert!((iter.end as u64) < self.size);

        while !iter.done {
            let elem = iter.cur as usize;
            // next element in backward direction
            iter.cur = iter.cur.wrapping_sub(1) & self.sizemask;
            if (iter.cur & self.sizemask) == (iter.end & self.sizemask) {
                iter.done = true;
            }
            if self.data[elem].status == SH_STATUS_IN_USE {
                return Some(elem);
            }
        }
        None
    }
}

// ===========================================================================
// TBMStatus / TBMIteratingState (tidbitmap.c).
// ===========================================================================

/// `TBMStatus` (`tidbitmap.c`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TBMStatus {
    /// no hashtable, nentries == 0
    Empty,
    /// entry1 contains the single entry
    OnePage,
    /// pagetable is valid, entry1 is not
    Hash,
}

/// `TBMIteratingState` (`tidbitmap.c`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TBMIteratingState {
    /// not yet converted to page and chunk array
    NotIterating,
    /// converted to local page and chunk array
    IteratingPrivate,
    /// converted to shared page and chunk array
    IteratingShared,
}

// ===========================================================================
// TidBitmapInner — `struct TIDBitmap` (tidbitmap.c), owned.
// ===========================================================================

/// `struct TIDBitmap` (`tidbitmap.c`): the whole TID bitmap. `pagetable` is the
/// real owned [`Pagetable`]; the `dsa*` pointer fields are the `dsa_pointer`s
/// the shared path threads to the provider (mirror the C
/// `dsapagetable`/`ptpages`/`ptchunks`/`dsa` fields).
struct TidBitmapInner {
    /// see [`TBMStatus`]
    status: TBMStatus,
    /// hash table of `PagetableEntry`s (valid when `status == Hash`)
    pagetable: Option<Pagetable>,
    /// number of entries in pagetable
    nentries: i32,
    /// limit on same to meet maxbytes
    maxentries: i32,
    /// number of exact entries in pagetable
    npages: i32,
    /// number of lossy entries in pagetable
    nchunks: i32,
    /// `tbm_begin_iterate` called?
    iterating: TBMIteratingState,
    /// offset to start lossifying hashtable at
    lossify_start: u32,
    /// used when `status == OnePage`
    entry1: PagetableEntry,
    /// sorted exact-page list (entry indices into [`Self::pagetable`])
    spages: Vec<usize>,
    /// sorted lossy-chunk list (entry indices into [`Self::pagetable`])
    schunks: Vec<usize>,
    /// `dsa_pointer` to the element array
    dsapagetable: u64,
    /// `dsa_pointer` to the page array. Mirrors the C `ptpages` field, but the
    /// live DSA layout is owned by the (unported) shared-TBM provider, so it is
    /// only stamped, never re-read in-crate yet — hence `dead_code`.
    #[allow(dead_code)]
    ptpages: u64,
    /// `dsa_pointer` to the chunk array (see [`Self::ptpages`]).
    #[allow(dead_code)]
    ptchunks: u64,
    /// reference to per-query dsa area (`None` => backend-local)
    dsa: Option<DsaAreaHandle>,
}

/// `InvalidDsaPointer` (`utils/dsa.h`).
const INVALID_DSA_POINTER: u64 = 0;

/// `DsaPointerIsValid(x)` (`utils/dsa.h`).
#[inline]
fn dsa_pointer_is_valid(dp: u64) -> bool {
    dp != INVALID_DSA_POINTER
}

/// Selects which [`PagetableEntry`] a lookup/create returned: the fixed `entry1`
/// slot, or index `i` of the pagetable's bucket array. Replaces the C
/// `PagetableEntry *` (which points at either `&tbm->entry1` or a hash slot).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PageRef {
    Entry1,
    Hash(usize),
}

impl TidBitmapInner {
    /// `tbm_create` (`tidbitmap.c`): create an initially-empty bitmap with a
    /// memory budget of `maxbytes`. A non-`None` `dsa` makes the bitmap
    /// shareable.
    fn create(maxbytes: usize, dsa: Option<DsaAreaHandle>) -> Self {
        // Create the TIDBitmap struct and zero all its fields (makeNode).
        TidBitmapInner {
            status: TBMStatus::Empty,
            pagetable: None,
            nentries: 0,
            maxentries: tbm_calculate_entries(maxbytes),
            npages: 0,
            nchunks: 0,
            iterating: TBMIteratingState::NotIterating,
            lossify_start: 0,
            entry1: PagetableEntry::default(),
            spages: Vec::new(),
            schunks: Vec::new(),
            dsapagetable: INVALID_DSA_POINTER,
            ptpages: INVALID_DSA_POINTER,
            ptchunks: INVALID_DSA_POINTER,
            dsa,
        }
    }

    /// `tbm_create_pagetable` (`tidbitmap.c`): create the hashtable lazily,
    /// pushing `entry1` into it if in one-page mode.
    fn create_pagetable(&mut self) -> PgResult<()> {
        debug_assert!(self.status != TBMStatus::Hash);
        debug_assert!(self.pagetable.is_none());

        let mut pagetable = Pagetable::create(128)?;

        // If entry1 is valid, push it into the hashtable.
        if self.status == TBMStatus::OnePage {
            let (idx, found) = pagetable.insert(self.entry1.blockno)?;
            debug_assert!(!found);
            let oldstatus = pagetable.data[idx].status;
            pagetable.data[idx] = self.entry1.clone();
            pagetable.data[idx].status = oldstatus;
        }

        self.pagetable = Some(pagetable);
        self.status = TBMStatus::Hash;
        Ok(())
    }

    /// `tbm_is_empty` (`tidbitmap.c`).
    fn is_empty(&self) -> bool {
        self.nentries == 0
    }

    /// `tbm_add_tuples` (`tidbitmap.c`): add `tids` to the bitmap, setting the
    /// recheck flag on touched pages if `recheck`.
    fn add_tuples(&mut self, tids: &[ItemPointerData], recheck: bool) -> PgResult<()> {
        let mut currblk: BlockNumber = InvalidBlockNumber;
        // `Option<PageRef>` only valid when currblk is valid (C: `page` only
        // valid when currblk is valid).
        let mut page: Option<PageRef> = None;

        debug_assert!(self.iterating == TBMIteratingState::NotIterating);
        for tid in tids {
            let blk = tid.ip_blkid.block_number();
            let off = tid.ip_posid;

            // safety check to ensure we don't overrun bit array bounds
            if (off as i32) < 1 || off as usize > TBM_MAX_TUPLES_PER_PAGE {
                return Err(PgError::error(format!("tuple offset out of range: {off}")));
            }

            // Look up target page unless we already did.
            if blk != currblk {
                if self.page_is_lossy(blk) {
                    page = None; // remember page is lossy
                } else {
                    page = Some(self.get_pageentry(blk)?);
                }
                currblk = blk;
            }

            let page_ref = match page {
                Some(p) => p,
                None => continue, // whole page is already marked
            };

            let entry = self.page_mut(page_ref)?;
            let (wnum, bnum) = if entry.ischunk {
                // The page is a lossy chunk header, set bit for itself.
                (0usize, 0usize)
            } else {
                // Page is exact, so set bit for individual tuple.
                (wordnum(off as usize - 1), bitnum(off as usize - 1))
            };
            entry.words[wnum] |= (1 as bitmapword) << bnum;
            entry.recheck |= recheck;

            if self.nentries > self.maxentries {
                self.lossify()?;
                // Page could have been converted to lossy, force new lookup.
                currblk = InvalidBlockNumber;
                page = None;
            }
        }
        Ok(())
    }

    /// `tbm_add_page` (`tidbitmap.c`): mark an entire page lossy.
    fn add_page(&mut self, pageno: BlockNumber) -> PgResult<()> {
        // Enter the page in the bitmap, or mark it lossy if already present.
        self.mark_page_lossy(pageno)?;
        // If we went over the memory limit, lossify some more pages.
        if self.nentries > self.maxentries {
            self.lossify()?;
        }
        Ok(())
    }

    /// `tbm_union` (`tidbitmap.c`): set `self` to the union of `self` and `b`.
    fn union(&mut self, b: &TidBitmapInner) -> PgResult<()> {
        debug_assert!(self.iterating == TBMIteratingState::NotIterating);
        // Nothing to do if b is empty.
        if b.nentries == 0 {
            return Ok(());
        }
        // Scan through chunks and pages in b, merge into self.
        if b.status == TBMStatus::OnePage {
            let bpage = b.entry1.clone();
            self.union_page(&bpage)?;
        } else {
            debug_assert!(b.status == TBMStatus::Hash);
            let bpt = b
                .pagetable
                .as_ref()
                .ok_or_else(|| PgError::error("union: TBM_HASH implies pagetable"))?;
            let mut i = iter_new();
            bpt.start_iterate(&mut i);
            while let Some(idx) = bpt.iterate(&mut i) {
                // Clone the entry so the shared borrow of b ends before we mutate
                // self (C reads through `const PagetableEntry *bpage`).
                let bpage = bpt.data[idx].clone();
                self.union_page(&bpage)?;
            }
        }
        Ok(())
    }

    /// `tbm_union_page` (`tidbitmap.c`): process one page of b during a union.
    fn union_page(&mut self, bpage: &PagetableEntry) -> PgResult<()> {
        if bpage.ischunk {
            // Scan b's chunk, mark each indicated page lossy in self.
            for wnum in 0..WORDS_PER_CHUNK {
                let mut w = bpage.words[wnum];
                if w != 0 {
                    let mut pg =
                        bpage.blockno + (wnum as BlockNumber) * (BITS_PER_BITMAPWORD as u32);
                    while w != 0 {
                        if w & 1 != 0 {
                            self.mark_page_lossy(pg)?;
                        }
                        pg += 1;
                        w >>= 1;
                    }
                }
            }
        } else if self.page_is_lossy(bpage.blockno) {
            // page is already lossy in self, nothing to do
            return Ok(());
        } else {
            let apage = self.get_pageentry(bpage.blockno)?;
            let entry = self.page_mut(apage)?;
            if entry.ischunk {
                // The page is a lossy chunk header, set bit for itself.
                entry.words[0] |= (1 as bitmapword) << 0;
            } else {
                // Both pages are exact, merge at the bit level.
                for wnum in 0..WORDS_PER_PAGE {
                    entry.words[wnum] |= bpage.words[wnum];
                }
                entry.recheck |= bpage.recheck;
            }
        }

        if self.nentries > self.maxentries {
            self.lossify()?;
        }
        Ok(())
    }

    /// `tbm_intersect` (`tidbitmap.c`): set `self` to the intersection of `self`
    /// and `b`.
    fn intersect(&mut self, b: &TidBitmapInner) -> PgResult<()> {
        debug_assert!(self.iterating == TBMIteratingState::NotIterating);
        // Nothing to do if self is empty.
        if self.nentries == 0 {
            return Ok(());
        }
        // Scan through chunks and pages in self, try to match to b.
        if self.status == TBMStatus::OnePage {
            let mut apage = self.entry1.clone();
            let empty = tbm_intersect_page(&mut apage, b);
            self.entry1 = apage;
            if empty {
                // Page is now empty, remove it from self.
                debug_assert!(!self.entry1.ischunk);
                self.npages -= 1;
                self.nentries -= 1;
                debug_assert!(self.nentries == 0);
                self.status = TBMStatus::Empty;
            }
        } else {
            debug_assert!(self.status == TBMStatus::Hash);
            // C iterates the live table and may delete the current element.
            // Gather the blocknos in that backward iteration order first, then
            // visit each, re-looking-it-up (a delete only relocates elements
            // *within* the table, never drops a not-yet-visited key).
            let order = {
                let pt = self
                    .pagetable
                    .as_ref()
                    .ok_or_else(|| PgError::error("intersect: TBM_HASH implies pagetable"))?;
                let mut i = iter_new();
                pt.start_iterate(&mut i);
                let mut order = Vec::new();
                while let Some(idx) = pt.iterate(&mut i) {
                    order.push(pt.data[idx].blockno);
                }
                order
            };
            for blockno in order {
                let idx = match self.pagetable.as_ref().and_then(|pt| pt.lookup(blockno)) {
                    Some(idx) => idx,
                    None => continue,
                };
                let mut apage = self
                    .pagetable
                    .as_ref()
                    .ok_or_else(|| PgError::error("intersect: pagetable is NULL"))?
                    .data[idx]
                    .clone();
                let empty = tbm_intersect_page(&mut apage, b);
                let ischunk = apage.ischunk;
                // Write back the (possibly mutated) page bits.
                self.pagetable
                    .as_mut()
                    .ok_or_else(|| PgError::error("intersect: pagetable is NULL"))?
                    .data[idx] = apage;
                if empty {
                    // Page or chunk is now empty, remove it from self.
                    if ischunk {
                        self.nchunks -= 1;
                    } else {
                        self.npages -= 1;
                    }
                    self.nentries -= 1;
                    if !self
                        .pagetable
                        .as_mut()
                        .ok_or_else(|| PgError::error("intersect: pagetable is NULL"))?
                        .delete(blockno)
                    {
                        return Err(PgError::error("hash table corrupted"));
                    }
                }
            }
        }
        Ok(())
    }

    /// `tbm_find_pageentry` (`tidbitmap.c`): a non-lossy entry for `pageno`, or
    /// `None`.
    fn find_pageentry(&self, pageno: BlockNumber) -> Option<PageRef> {
        if self.nentries == 0 {
            // in case pagetable doesn't exist
            return None;
        }
        if self.status == TBMStatus::OnePage {
            if self.entry1.blockno != pageno {
                return None;
            }
            debug_assert!(!self.entry1.ischunk);
            return Some(PageRef::Entry1);
        }
        let pt = self.pagetable.as_ref()?;
        let idx = pt.lookup(pageno)?;
        if pt.data[idx].ischunk {
            return None; // don't want a lossy chunk header
        }
        Some(PageRef::Hash(idx))
    }

    /// `tbm_get_pageentry` (`tidbitmap.c`): find or create an exact entry.
    fn get_pageentry(&mut self, pageno: BlockNumber) -> PgResult<PageRef> {
        let (page, found) = if self.status == TBMStatus::Empty {
            // Use the fixed slot.
            self.status = TBMStatus::OnePage;
            (PageRef::Entry1, false)
        } else {
            if self.status == TBMStatus::OnePage {
                if self.entry1.blockno == pageno {
                    return Ok(PageRef::Entry1);
                }
                // Time to switch from one page to a hashtable.
                self.create_pagetable()?;
            }
            // Look up or create an entry.
            let (idx, found) = self
                .pagetable
                .as_mut()
                .ok_or_else(|| PgError::error("get_pageentry: pagetable is NULL"))?
                .insert(pageno)?;
            (PageRef::Hash(idx), found)
        };

        // Initialize it if not present before.
        if !found {
            let entry = self.page_mut(page)?;
            let oldstatus = entry.status;
            // MemSet(page, 0, sizeof(PagetableEntry)) — reset all fields.
            *entry = PagetableEntry::default();
            entry.status = oldstatus;
            entry.blockno = pageno;
            // must count it too
            self.nentries += 1;
            self.npages += 1;
        }
        Ok(page)
    }

    /// `tbm_page_is_lossy` (`tidbitmap.c`): is the page marked lossily stored?
    fn page_is_lossy(&self, pageno: BlockNumber) -> bool {
        // we can skip the lookup if there are no lossy chunks
        if self.nchunks == 0 {
            return false;
        }
        debug_assert!(self.status == TBMStatus::Hash);

        let bitno = pageno as usize % PAGES_PER_CHUNK;
        let chunk_pageno = pageno - bitno as u32;

        let pt = match self.pagetable.as_ref() {
            Some(pt) => pt,
            None => return false,
        };
        if let Some(idx) = pt.lookup(chunk_pageno) {
            let page = &pt.data[idx];
            if page.ischunk {
                let wnum = wordnum(bitno);
                let bnum = bitnum(bitno);
                if page.words[wnum] & ((1 as bitmapword) << bnum) != 0 {
                    return true;
                }
            }
        }
        false
    }

    /// `tbm_mark_page_lossy` (`tidbitmap.c`): mark `pageno` lossily stored.
    fn mark_page_lossy(&mut self, pageno: BlockNumber) -> PgResult<()> {
        // We force the bitmap into hashtable mode whenever it's lossy.
        if self.status != TBMStatus::Hash {
            self.create_pagetable()?;
        }

        let bitno = pageno as usize % PAGES_PER_CHUNK;
        let chunk_pageno = pageno - bitno as u32;

        // Remove any extant non-lossy entry for the page. If the page is its own
        // chunk header (bitno == 0), skip this and handle the case below.
        if bitno != 0
            && self
                .pagetable
                .as_mut()
                .ok_or_else(|| PgError::error("mark_page_lossy: pagetable is NULL"))?
                .delete(pageno)
        {
            // It was present, so adjust counts.
            self.nentries -= 1;
            self.npages -= 1; // assume it must have been non-lossy
        }

        // Look up or create entry for chunk-header page.
        let (idx, found) = self
            .pagetable
            .as_mut()
            .ok_or_else(|| PgError::error("mark_page_lossy: pagetable is NULL"))?
            .insert(chunk_pageno)?;

        // Initialize it if not present before.
        if !found {
            let pt = self
                .pagetable
                .as_mut()
                .ok_or_else(|| PgError::error("mark_page_lossy: pagetable is NULL"))?;
            let oldstatus = pt.data[idx].status;
            pt.data[idx] = PagetableEntry::default();
            pt.data[idx].status = oldstatus;
            pt.data[idx].blockno = chunk_pageno;
            pt.data[idx].ischunk = true;
            // must count it too
            self.nentries += 1;
            self.nchunks += 1;
        } else if !self
            .pagetable
            .as_ref()
            .ok_or_else(|| PgError::error("mark_page_lossy: pagetable is NULL"))?
            .data[idx]
            .ischunk
        {
            let pt = self
                .pagetable
                .as_mut()
                .ok_or_else(|| PgError::error("mark_page_lossy: pagetable is NULL"))?;
            let oldstatus = pt.data[idx].status;
            // chunk header page was formerly non-lossy, make it lossy
            pt.data[idx] = PagetableEntry::default();
            pt.data[idx].status = oldstatus;
            pt.data[idx].blockno = chunk_pageno;
            pt.data[idx].ischunk = true;
            // we assume it had some tuple bit(s) set, so mark it lossy
            pt.data[idx].words[0] = (1 as bitmapword) << 0;
            // adjust counts
            self.nchunks += 1;
            self.npages -= 1;
        }

        // Now set the original target page's bit.
        let wnum = wordnum(bitno);
        let bnum = bitnum(bitno);
        self.pagetable
            .as_mut()
            .ok_or_else(|| PgError::error("mark_page_lossy: pagetable is NULL"))?
            .data[idx]
            .words[wnum] |= (1 as bitmapword) << bnum;
        Ok(())
    }

    /// `tbm_lossify` (`tidbitmap.c`): lose information to get back under the
    /// memory limit.
    fn lossify(&mut self) -> PgResult<()> {
        debug_assert!(self.iterating == TBMIteratingState::NotIterating);
        debug_assert!(self.status == TBMStatus::Hash);

        let mut i = iter_new();
        self.pagetable
            .as_ref()
            .ok_or_else(|| PgError::error("lossify: pagetable is NULL"))?
            .start_iterate_at(&mut i, self.lossify_start);
        loop {
            // Iterate the live table (C continues the same scan across the
            // mark-lossy mutations; that is safe because missing/revisiting one
            // element is non-fatal).
            let idx = match self
                .pagetable
                .as_ref()
                .ok_or_else(|| PgError::error("lossify: pagetable is NULL"))?
                .iterate(&mut i)
            {
                Some(idx) => idx,
                None => break,
            };
            let (ischunk, blockno) = {
                let entry = &self
                    .pagetable
                    .as_ref()
                    .ok_or_else(|| PgError::error("lossify: pagetable is NULL"))?
                    .data[idx];
                (entry.ischunk, entry.blockno)
            };
            if ischunk {
                continue; // already a chunk header
            }
            // If the page would become a chunk header, skip it.
            if blockno as usize % PAGES_PER_CHUNK == 0 {
                continue;
            }

            // This does the dirty work ...
            self.mark_page_lossy(blockno)?;

            if self.nentries <= self.maxentries / 2 {
                // We have made enough room; remember where to start next round.
                self.lossify_start = i.cur;
                break;
            }
        }

        // With a big bitmap and small work_mem, we might not get under
        // maxentries; force maxentries up to avoid uselessly calling lossify
        // over and over.
        if self.nentries > self.maxentries / 2 {
            self.maxentries = self.nentries.min((i32::MAX - 1) / 2) * 2;
        }
        Ok(())
    }

    /// Mutable access to a [`PageRef`]'s entry (`entry1` or a pagetable slot).
    fn page_mut(&mut self, page: PageRef) -> PgResult<&mut PagetableEntry> {
        match page {
            PageRef::Entry1 => Ok(&mut self.entry1),
            PageRef::Hash(idx) => Ok(&mut self
                .pagetable
                .as_mut()
                .ok_or_else(|| PgError::error("page_mut: PageRef::Hash but pagetable is NULL"))?
                .data[idx]),
        }
    }

    /// Shared (immutable) access to a [`PageRef`]'s entry.
    fn page_ref(&self, page: PageRef) -> &PagetableEntry {
        match page {
            PageRef::Entry1 => &self.entry1,
            PageRef::Hash(idx) => &self.pagetable.as_ref().unwrap().data[idx],
        }
    }
}

/// `tbm_intersect_page` (`tidbitmap.c`): process one page of a during an
/// intersection op against `b`. Returns `true` if `apage` is now empty and
/// should be deleted. The C `a` argument is unused, so it is omitted.
fn tbm_intersect_page(apage: &mut PagetableEntry, b: &TidBitmapInner) -> bool {
    if apage.ischunk {
        // Scan each bit in chunk, try to clear.
        let mut candelete = true;
        for wnum in 0..WORDS_PER_CHUNK {
            let w = apage.words[wnum];
            if w != 0 {
                let mut neww = w;
                let mut pg = apage.blockno + (wnum as BlockNumber) * (BITS_PER_BITMAPWORD as u32);
                let mut bit = 0;
                let mut wv = w;
                while wv != 0 {
                    if wv & 1 != 0 && !b.page_is_lossy(pg) && b.find_pageentry(pg).is_none() {
                        // Page is not in b at all, lose lossy bit.
                        neww &= !((1 as bitmapword) << bit);
                    }
                    pg += 1;
                    bit += 1;
                    wv >>= 1;
                }
                apage.words[wnum] = neww;
                if neww != 0 {
                    candelete = false;
                }
            }
        }
        candelete
    } else if b.page_is_lossy(apage.blockno) {
        // Some of the tuples in 'a' might not satisfy the quals for 'b', but
        // because the page 'b' is lossy, we don't know which ones; mark 'a' as
        // requiring rechecks.
        apage.recheck = true;
        false
    } else {
        let mut candelete = true;
        if let Some(bpage_ref) = b.find_pageentry(apage.blockno) {
            let bpage = b.page_ref(bpage_ref);
            // Both pages are exact, merge at the bit level.
            debug_assert!(!bpage.ischunk);
            for wnum in 0..WORDS_PER_PAGE {
                apage.words[wnum] &= bpage.words[wnum];
                if apage.words[wnum] != 0 {
                    candelete = false;
                }
            }
            apage.recheck |= bpage.recheck;
        }
        // If there is no matching b page, we can just delete the a page.
        candelete
    }
}

/// `tbm_calculate_entries` (`tidbitmap.c`): the maximum number of pagetable
/// entries that fit within a `maxbytes` memory budget.
pub fn tbm_calculate_entries(maxbytes: usize) -> i32 {
    // Estimate hash cost as sizeof(PagetableEntry), plus two Pointers per entry
    // for the iteration-readout arrays.
    let pointer = core::mem::size_of::<usize>();
    let mut nbuckets = maxbytes / (core::mem::size_of::<PagetableEntry>() + pointer + pointer);
    nbuckets = nbuckets.min(i32::MAX as usize - 1); // safety limit
    nbuckets = nbuckets.max(16); // sanity limit
    nbuckets as i32
}

// ===========================================================================
// TBMIterateResult / private iteration (tidbitmap.c).
// ===========================================================================

/// `struct TBMIterateResult` (`nodes/tidbitmap.h`): the result of one iteration
/// step. The table-AM scan owns these calls (it is unported), so these are
/// in-crate public functions ready for it; they are not seamed.
#[derive(Clone, Debug, Default)]
pub struct TBMIterateResult {
    /// `BlockNumber blockno` — page containing tuples from the bitmap.
    pub blockno: BlockNumber,
    /// `bool lossy` — whether the bitmap is lossy for this page.
    pub lossy: bool,
    /// `bool recheck` — whether to recheck the qual conditions.
    pub recheck: bool,
    /// `void *internal_page` — the per-page bitmap (an exact page's
    /// `PagetableEntry`), handed back to [`tbm_extract_page_tuple`]; `None` for
    /// a lossy page.
    internal_page: Option<Box<PagetableEntry>>,
}

/// `struct TBMPrivateIterator` (`tidbitmap.c`): private-iteration progress.
/// Unlike C it does not store a back-pointer to the bitmap (the bitmap is passed
/// to [`tbm_private_iterate`] explicitly; it is read-only during iteration).
#[derive(Clone, Debug, Default)]
pub struct TbmPrivateIterator {
    spageptr: i32,
    schunkptr: i32,
    schunkbit: i32,
}

/// `tbm_begin_private_iterate` (`tidbitmap.c`): start a private iteration,
/// sorting the bitmap's pages/chunks into numerical order.
fn begin_private_iterate(tbm: &mut TidBitmapInner) -> PgResult<TbmPrivateIterator> {
    debug_assert!(tbm.iterating != TBMIteratingState::IteratingShared);

    // If we have a hashtable, create and fill the sorted page lists, unless we
    // already did that for a previous iterator.
    if tbm.status == TBMStatus::Hash && tbm.iterating == TBMIteratingState::NotIterating {
        let mut spages: Vec<usize> = Vec::new();
        let mut schunks: Vec<usize> = Vec::new();
        if tbm.npages > 0 {
            spages
                .try_reserve_exact(tbm.npages as usize)
                .map_err(|_| out_of_memory("spages"))?;
        }
        if tbm.nchunks > 0 {
            schunks
                .try_reserve_exact(tbm.nchunks as usize)
                .map_err(|_| out_of_memory("schunks"))?;
        }

        let pt = tbm
            .pagetable
            .as_ref()
            .ok_or_else(|| PgError::error("tbm_begin_private_iterate: pagetable is NULL"))?;
        let mut i = iter_new();
        pt.start_iterate(&mut i);
        while let Some(idx) = pt.iterate(&mut i) {
            if pt.data[idx].ischunk {
                schunks.push(idx);
            } else {
                spages.push(idx);
            }
        }
        debug_assert!(spages.len() as i32 == tbm.npages);
        debug_assert!(schunks.len() as i32 == tbm.nchunks);
        if spages.len() > 1 {
            spages.sort_by(|&l, &r| pg_cmp_u32(pt.data[l].blockno, pt.data[r].blockno));
        }
        if schunks.len() > 1 {
            schunks.sort_by(|&l, &r| pg_cmp_u32(pt.data[l].blockno, pt.data[r].blockno));
        }
        tbm.spages = spages;
        tbm.schunks = schunks;
    }

    tbm.iterating = TBMIteratingState::IteratingPrivate;

    Ok(TbmPrivateIterator {
        spageptr: 0,
        schunkptr: 0,
        schunkbit: 0,
    })
}

/// `tbm_advance_schunkbit` (`tidbitmap.c`): advance `schunkbit` to the next set
/// bit in `chunk`.
fn advance_schunkbit(chunk: &PagetableEntry, schunkbitp: &mut i32) {
    let mut schunkbit = *schunkbitp;
    while (schunkbit as usize) < PAGES_PER_CHUNK {
        let wnum = wordnum(schunkbit as usize);
        let bnum = bitnum(schunkbit as usize);
        if chunk.words[wnum] & ((1 as bitmapword) << bnum) != 0 {
            break;
        }
        schunkbit += 1;
    }
    *schunkbitp = schunkbit;
}

/// The `iterator.spageptr`-th exact page: `&tbm->entry1` in one-page mode (where
/// no `spages[]` array is allocated), else `tbm->spages[spageptr]`.
fn spage_entry(tbm: &TidBitmapInner, spageptr: usize) -> &PagetableEntry {
    if tbm.status == TBMStatus::OnePage {
        &tbm.entry1
    } else {
        &tbm.pagetable.as_ref().unwrap().data[tbm.spages[spageptr]]
    }
}

/// `tbm_private_iterate` (`tidbitmap.c`): advance a private iteration, filling
/// `tbmres` and returning `false` at end (with `tbmres.blockno ==
/// InvalidBlockNumber`). Pages are delivered in numerical order.
fn private_iterate(
    tbm: &TidBitmapInner,
    iterator: &mut TbmPrivateIterator,
    tbmres: &mut TBMIterateResult,
) -> bool {
    debug_assert!(tbm.iterating == TBMIteratingState::IteratingPrivate);

    // Advance schunkptr/schunkbit to the next set bit if any chunk pages remain.
    while iterator.schunkptr < tbm.nchunks {
        let chunk =
            &tbm.pagetable.as_ref().unwrap().data[tbm.schunks[iterator.schunkptr as usize]];
        let mut schunkbit = iterator.schunkbit;
        advance_schunkbit(chunk, &mut schunkbit);
        if (schunkbit as usize) < PAGES_PER_CHUNK {
            iterator.schunkbit = schunkbit;
            break;
        }
        // advance to next chunk
        iterator.schunkptr += 1;
        iterator.schunkbit = 0;
    }

    // If both chunk and per-page data remain, output the numerically earlier
    // page.
    if iterator.schunkptr < tbm.nchunks {
        let chunk =
            &tbm.pagetable.as_ref().unwrap().data[tbm.schunks[iterator.schunkptr as usize]];
        let chunk_blockno = chunk.blockno + iterator.schunkbit as u32;
        let spage_blockno = if iterator.spageptr < tbm.npages {
            Some(spage_entry(tbm, iterator.spageptr as usize).blockno)
        } else {
            None
        };
        if spage_blockno.is_none() || chunk_blockno < spage_blockno.unwrap() {
            // Return a lossy page indicator from the chunk.
            tbmres.blockno = chunk_blockno;
            tbmres.lossy = true;
            tbmres.recheck = true;
            tbmres.internal_page = None;
            iterator.schunkbit += 1;
            return true;
        }
    }

    if iterator.spageptr < tbm.npages {
        let page = spage_entry(tbm, iterator.spageptr as usize);
        // C hands a raw pointer into the read-only bitmap; the bitmap is
        // immutable during iteration, so an owned clone is observationally
        // identical.
        tbmres.blockno = page.blockno;
        tbmres.lossy = false;
        tbmres.recheck = page.recheck;
        tbmres.internal_page = Some(Box::new(page.clone()));
        iterator.spageptr += 1;
        return true;
    }

    // Nothing more in the bitmap.
    tbmres.blockno = InvalidBlockNumber;
    false
}

/// `tbm_extract_page_tuple` (`tidbitmap.c`): extract the offsets recorded for the
/// page referenced by `iteritem.internal_page` into `offsets`, returning the
/// total number of set offsets (filling at most `offsets.len()`).
pub fn tbm_extract_page_tuple(iteritem: &TBMIterateResult, offsets: &mut [OffsetNumber]) -> i32 {
    let page = match iteritem.internal_page.as_ref() {
        Some(p) => p,
        // An exact page always carries its internal_page; a lossy page has none
        // and yields no offsets.
        None => return 0,
    };
    let max_offsets = offsets.len();
    let mut ntuples = 0i32;
    for wnum in 0..WORDS_PER_PAGE {
        let mut w = page.words[wnum];
        if w != 0 {
            let mut off = (wnum * BITS_PER_BITMAPWORD + 1) as i32;
            while w != 0 {
                if w & 1 != 0 {
                    if (ntuples as usize) < max_offsets {
                        offsets[ntuples as usize] = off as OffsetNumber;
                    }
                    ntuples += 1;
                }
                off += 1;
                w >>= 1;
            }
        }
    }
    ntuples
}

// ===========================================================================
// Opaque carrier bridge.
//
// In C `tidbitmap.c` palloc's a `struct TIDBitmap` and threads the real
// `TIDBitmap *` everywhere (executor `node->tbm`, the index-AM `amgetbitmap`
// argument). The header keeps the struct private, so consumers only ever store
// and hand back the pointer (opacity inherited, not introduced).
//
// `types_tidbitmap::TIDBitmap` is the opaque carrier those consumers hold. It
// can't name `TidBitmapInner` (that would make the below-it vocabulary crate
// depend on this one), so it stays a type-erased `Box<dyn Any>` — but the box
// now holds the *real* [`TidBitmapInner`] by value, exactly as C palloc's the
// real struct. There is no side table and no integer key: the carrier IS the
// owning pointer.
// ===========================================================================

/// Shared (immutable) access to a [`TidBitmapInner`] held inside a
/// [`types_tidbitmap::TIDBitmap`] carrier.
fn carrier_inner(tbm: &types_tidbitmap::TIDBitmap) -> PgResult<&TidBitmapInner> {
    tbm.0
        .as_ref()
        .and_then(|p| p.downcast_ref::<TidBitmapInner>())
        .ok_or_else(|| PgError::error("tidbitmap: TIDBitmap carrier is NULL or foreign"))
}

/// Mutable access to a [`TidBitmapInner`] held inside a
/// [`types_tidbitmap::TIDBitmap`] carrier.
fn carrier_inner_mut(tbm: &mut types_tidbitmap::TIDBitmap) -> PgResult<&mut TidBitmapInner> {
    tbm.0
        .as_mut()
        .and_then(|p| p.downcast_mut::<TidBitmapInner>())
        .ok_or_else(|| PgError::error("tidbitmap: TIDBitmap carrier is NULL or foreign"))
}

// ===========================================================================
// Public (in-crate) constructors / accessors — for the bitmap-scan executor
// and table-AM scan once they are ported. Wrap the opaque carrier bridge.
// ===========================================================================

/// `tbm_create(maxbytes, dsa)` (`tidbitmap.c`): create an initially-empty bitmap
/// and return its opaque carrier (which owns the real `TIDBitmap` by value, the
/// C palloc'd struct). A non-`None` `dsa` makes the bitmap shareable.
pub fn tbm_create(maxbytes: usize, dsa: Option<DsaAreaHandle>) -> types_tidbitmap::TIDBitmap {
    let inner = TidBitmapInner::create(maxbytes, dsa);
    types_tidbitmap::TIDBitmap(Some(Box::new(inner)))
}

/// `tbm_is_empty(tbm)` (`tidbitmap.c`).
pub fn tbm_is_empty(tbm: &types_tidbitmap::TIDBitmap) -> PgResult<bool> {
    Ok(carrier_inner(tbm)?.is_empty())
}

/// `tbm_add_tuples(tbm, tids, recheck)` (`tidbitmap.c`).
pub fn tbm_add_tuples(
    tbm: &mut types_tidbitmap::TIDBitmap,
    tids: &[ItemPointerData],
    recheck: bool,
) -> PgResult<()> {
    carrier_inner_mut(tbm)?.add_tuples(tids, recheck)
}

/// `tbm_add_page(tbm, pageno)` (`tidbitmap.c`).
pub fn tbm_add_page(tbm: &mut types_tidbitmap::TIDBitmap, pageno: BlockNumber) -> PgResult<()> {
    carrier_inner_mut(tbm)?.add_page(pageno)
}

/// `tbm_union(a, b)` (`tidbitmap.c`): `a = a ∪ b` (a modified in place). `a` and
/// `b` are distinct carriers owning distinct boxes, so the borrows don't alias.
pub fn tbm_union(
    a: &mut types_tidbitmap::TIDBitmap,
    b: &types_tidbitmap::TIDBitmap,
) -> PgResult<()> {
    let bmap = carrier_inner(b)?;
    // Read b's interior, then mutate a's interior. Clone b out first so the
    // shared borrow of `b` ends before the mutable borrow of `a` (they are
    // separate allocations; this only sidesteps the two simultaneous downcasts).
    let bmap = bmap as *const TidBitmapInner;
    // SAFETY: `bmap` points at b's owned box, which is not mutated here (a != b
    // are distinct carriers; union reads b and writes a).
    let bmap = unsafe { &*bmap };
    carrier_inner_mut(a)?.union(bmap)
}

/// `tbm_intersect(a, b)` (`tidbitmap.c`): `a = a ∩ b` (a modified in place).
pub fn tbm_intersect(
    a: &mut types_tidbitmap::TIDBitmap,
    b: &types_tidbitmap::TIDBitmap,
) -> PgResult<()> {
    let bmap = carrier_inner(b)? as *const TidBitmapInner;
    // SAFETY: `bmap` points at b's owned box, which is not mutated here (a != b
    // are distinct carriers; intersect reads b and writes a).
    let bmap = unsafe { &*bmap };
    carrier_inner_mut(a)?.intersect(bmap)
}

// ===========================================================================
// GIN private-iteration bridge (ginget.c).
//
// `ginget.c` drives a private iteration directly: each `GinScanEntry` owns a
// `TIDBitmap *matchBitmap`, a `TBMPrivateIterator *matchIterator`, and an
// embedded `TBMIterateResult matchResult`, and calls `tbm_private_iterate`
// per item. The repo carries `matchResult` as the consumer-side
// `types_gin::TBMIterateResult` (with an opaque `Box<dyn Any>` `internal_page`),
// so the tidbitmap owner fills that carrier here — the same "owner fills the
// consumer's carrier" pattern used for the seam providers.
// ===========================================================================

/// `tbm_begin_private_iterate(tbm)` (`tidbitmap.c`) for the GIN scan: build a
/// private iterator that keeps a back-pointer to `tbm` (which the `GinScanEntry`
/// owns for the whole iteration; it is read-only while iterating, mirroring the
/// C raw `iterator->tbm`).
pub fn gin_tbm_begin_private_iterate(
    tbm: &mut types_tidbitmap::TIDBitmap,
) -> PgResult<types_tidbitmap::TBMPrivateIterator> {
    let inner = carrier_inner_mut(tbm)?;
    let inner_ptr = inner as *const TidBitmapInner;
    let inner_it = begin_private_iterate(inner)?;
    Ok(types_tidbitmap::TBMPrivateIterator(Some(Box::new(
        PrivateIterCarrier {
            tbm: inner_ptr,
            iter: inner_it,
        },
    ))))
}

/// `tbm_private_iterate(iterator, tbmres)` (`tidbitmap.c`) for the GIN scan:
/// advance the private iteration, filling the consumer-side
/// `types_gin::TBMIterateResult`. Returns `false` at end (with `blockno ==
/// InvalidBlockNumber`).
pub fn gin_tbm_private_iterate(
    iterator: &mut types_tidbitmap::TBMPrivateIterator,
    tbmres: &mut types_gin::TBMIterateResult,
) -> bool {
    let carrier = iterator
        .0
        .as_mut()
        .and_then(|p| p.downcast_mut::<PrivateIterCarrier>())
        .expect("tbm_private_iterate: iterator carrier is NULL or foreign");
    // SAFETY: `carrier.tbm` points at the `TidBitmapInner` owned by the
    // `GinScanEntry.matchBitmap` carrier, which lives for the whole iteration
    // and is read-only here (mirrors the C raw `iterator->tbm`).
    let inner = unsafe { &*carrier.tbm };
    let mut local = TBMIterateResult::default();
    let more = private_iterate(inner, &mut carrier.iter, &mut local);
    // Copy the result into the consumer carrier, moving the opaque per-page
    // bitmap into the `Box<dyn Any>` slot.
    tbmres.blockno = local.blockno;
    tbmres.lossy = local.lossy;
    tbmres.recheck = local.recheck;
    tbmres.internal_page = local
        .internal_page
        .map(|p| p as Box<dyn core::any::Any>);
    more
}

/// `tbm_extract_page_tuple(iteritem, offsets, max)` (`tidbitmap.c`) for the GIN
/// scan, reading the consumer-side `types_gin::TBMIterateResult`.
pub fn gin_tbm_extract_page_tuple(
    iteritem: &types_gin::TBMIterateResult,
    offsets: &mut [OffsetNumber],
) -> i32 {
    let page = match iteritem
        .internal_page
        .as_ref()
        .and_then(|p| p.downcast_ref::<PagetableEntry>())
    {
        Some(p) => p,
        None => return 0,
    };
    let max_offsets = offsets.len();
    let mut ntuples = 0i32;
    for wnum in 0..WORDS_PER_PAGE {
        let mut w = page.words[wnum];
        if w != 0 {
            let mut off = (wnum * BITS_PER_BITMAPWORD + 1) as i32;
            while w != 0 {
                if w & 1 != 0 {
                    if (ntuples as usize) < max_offsets {
                        offsets[ntuples as usize] = off as OffsetNumber;
                    }
                    ntuples += 1;
                }
                off += 1;
                w >>= 1;
            }
        }
    }
    ntuples
}

/// `tbm_end_private_iterate(iterator)` (`tidbitmap.c`): free the private
/// iterator (here, drop the owned carrier).
pub fn gin_tbm_end_private_iterate(_iterator: types_tidbitmap::TBMPrivateIterator) {
    // Dropping the owned `Box` is the C `pfree(iterator)`.
}

/// `tbm_free(tbm)` (`tidbitmap.c`): free the bitmap (here, drop the owned
/// carrier). Mirrors C `pfree`.
pub fn tbm_free(tbm: types_tidbitmap::TIDBitmap) {
    // Dropping the owned `Box` releases the `TidBitmapInner`.
    let _ = tbm;
}

// ===========================================================================
// Seam providers (the OWNED seams) + their installers.
// ===========================================================================

/// Provider for `backend_nodes_core_seams::tbm_add_tuple`: add one heap TID to
/// the bitmap `tbm` (`tbm_add_tuples(tbm, &tid, 1, false)`).
fn provide_tbm_add_tuple(
    tbm: &mut types_tidbitmap::TIDBitmap,
    tid: ItemPointerData,
) -> PgResult<()> {
    carrier_inner_mut(tbm)?.add_tuples(&[tid], false)
}

/// Provider for `backend_nodes_core_seams::tbm_add_tuples`: add an array of heap
/// TIDs to the bitmap `tbm` (`tbm_add_tuples(tbm, tids, ntids, recheck)`).
fn provide_tbm_add_tuples(
    tbm: &mut types_tidbitmap::TIDBitmap,
    tids: &[ItemPointerData],
    recheck: bool,
) -> PgResult<()> {
    carrier_inner_mut(tbm)?.add_tuples(tids, recheck)
}

/// Provider for `backend_nodes_core_seams::tbm_add_page`: mark the whole heap
/// page `pageno` lossy in the bitmap `tbm` (`tbm_add_page(tbm, pageno)`).
fn provide_tbm_add_page(tbm: &mut types_tidbitmap::TIDBitmap, pageno: BlockNumber) -> PgResult<()> {
    tbm_add_page(tbm, pageno)
}

/// Provider for `tbm_free(tbm)` (`tidbitmap.c`): free the bitmap and any buffers
/// it holds. Dropping the carrier's boxed [`TidBitmapInner`] is the C
/// `pfree(pagetable)` / `pfree(spages/schunks)` / `pfree(tbm)`.
fn provide_tbm_free(tbm: &mut types_tidbitmap::TIDBitmap) {
    tbm.0 = None;
}

/// Provider for `tbm_begin_iterate(tbm, dsa, dsp)` (`tidbitmap.c`): build the
/// unified iterator — shared when `dsp` is valid, else private.
fn provide_tbm_begin_iterate(
    tbm: Option<&mut types_tidbitmap::TIDBitmap>,
    dsa: Option<DsaAreaHandle>,
    dsp: types_tidbitmap::dsa_pointer,
) -> PgResult<types_tidbitmap::TBMIterator> {
    if dsa_pointer_is_valid(dsp) {
        // Shared: attach the shared iterator state allocated in the DSA.
        let dsa = dsa.ok_or_else(|| {
            PgError::error("tbm_begin_iterate: shared iterate requires a dsa_area")
        })?;
        let it = attach_shared_iterate(dsa, dsp)?;
        Ok(types_tidbitmap::TBMIterator {
            shared: true,
            private_iterator: None,
            shared_iterator: Some(Box::new(it)),
        })
    } else {
        // Private: build the backend-local iterator. C dereferences `tbm` here
        // (`tbm_begin_private_iterate(tbm)`), so the private path always has a
        // non-NULL bitmap.
        let tbm = tbm.expect("tbm_begin_iterate: private iterate requires a non-NULL tbm");
        let inner = carrier_inner_mut(tbm)?;
        // C: `iterator->tbm = tbm` — the iterator keeps a back-pointer to the
        // (heap-stable) bitmap, which the executor owns for the whole scan
        // (`node->tbm`). Capture the boxed inner's stable address now.
        let inner_ptr = inner as *const TidBitmapInner;
        let inner_it = begin_private_iterate(inner)?;
        let carrier =
            types_tidbitmap::TBMPrivateIterator(Some(Box::new(PrivateIterCarrier {
                tbm: inner_ptr,
                iter: inner_it,
            })));
        Ok(types_tidbitmap::TBMIterator {
            shared: false,
            private_iterator: Some(Box::new(carrier)),
            shared_iterator: None,
        })
    }
}

/// The payload stored in a private [`types_tidbitmap::TBMPrivateIterator`]
/// carrier: the iteration progress plus the back-pointer to the bitmap it scans
/// (C `TBMPrivateIterator.tbm`, a `TIDBitmap *`). The pointed-at bitmap is owned
/// by the executor's `node->tbm` for the whole scan and is read-only during
/// iteration, mirroring the C raw back-pointer.
struct PrivateIterCarrier {
    tbm: *const TidBitmapInner,
    iter: TbmPrivateIterator,
}

/// Provider for `tbm_end_iterate(iterator)` (`tidbitmap.c`): release the
/// iterator's resources and NULL out its pointers (so `tbm_exhausted` reports
/// done). Both arms are owned `Box`es, so dropping them is the C `pfree`.
fn provide_tbm_end_iterate(iterator: &mut types_tidbitmap::TBMIterator) {
    debug_assert!(!iterator.exhausted());
    // *iterator = (TBMIterator){0}
    iterator.shared = false;
    iterator.private_iterator = None;
    iterator.shared_iterator = None;
}

/// Provider for `tbm_prepare_shared_iterate(tbm)` (`tidbitmap.c`): build the
/// shared (DSA) representation for parallel iteration and return its
/// `dsa_pointer`. The in-crate code walks the bitmap and builds the sorted
/// exact/lossy index arrays (1:1 with C); the DSA allocation, refcount init, and
/// `LWLock` init are done by the not-yet-ported DSA-backed shared-TBM provider
/// via [`dsa_shared_tbm_prepare`].
fn provide_tbm_prepare_shared_iterate(
    tbm: &mut types_tidbitmap::TIDBitmap,
) -> PgResult<types_tidbitmap::dsa_pointer> {
    let (dsa, layout) = {
        let inner = carrier_inner(tbm)?;
        debug_assert!(inner.dsa.is_some());
        debug_assert!(inner.iterating != TBMIteratingState::IteratingPrivate);

        let mut layout = SharedBitmapLayout {
            nentries: inner.nentries,
            maxentries: inner.maxentries,
            npages: inner.npages,
            nchunks: inner.nchunks,
            entries: Vec::new(),
            spages: Vec::new(),
            schunks: Vec::new(),
        };

        // If we're not already iterating, create and fill the sorted page lists.
        if inner.iterating == TBMIteratingState::NotIterating {
            if inner.status == TBMStatus::Hash {
                let pt = inner.pagetable.as_ref().unwrap();
                let mut i = iter_new();
                pt.start_iterate(&mut i);
                while let Some(idx) = pt.iterate(&mut i) {
                    let dense_idx = layout.entries.len() as i32;
                    layout.entries.push(pt.data[idx].clone());
                    if pt.data[idx].ischunk {
                        layout.schunks.push(dense_idx);
                    } else {
                        layout.spages.push(dense_idx);
                    }
                }
                debug_assert!(layout.spages.len() as i32 == inner.npages);
                debug_assert!(layout.schunks.len() as i32 == inner.nchunks);
            } else if inner.status == TBMStatus::OnePage {
                // In one page mode store the single entry and index 0.
                layout.entries.push(inner.entry1.clone());
                layout.spages.push(0);
            }

            if layout.spages.len() > 1 {
                let entries = &layout.entries;
                layout.spages.sort_by(|&l, &r| {
                    pg_cmp_u32(entries[l as usize].blockno, entries[r as usize].blockno)
                });
            }
            if layout.schunks.len() > 1 {
                let entries = &layout.entries;
                layout.schunks.sort_by(|&l, &r| {
                    pg_cmp_u32(entries[l as usize].blockno, entries[r as usize].blockno)
                });
            }
        }

        (inner.dsa, layout)
    };

    let dsa =
        dsa.ok_or_else(|| PgError::error("tbm_prepare_shared_iterate: tbm->dsa is NULL"))?;
    let dp = dsa_shared_tbm_prepare(dsa, layout)?;

    // Record the DSA head pointer; mark the bitmap as shared-iterating.
    let inner = carrier_inner_mut(tbm)?;
    inner.dsapagetable = dp;
    inner.iterating = TBMIteratingState::IteratingShared;
    Ok(dp)
}

/// Provider for `tbm_free_shared_area(dsa, dp)` (`tidbitmap.c`): free a shared
/// iterator state DSA allocation made by `tbm_prepare_shared_iterate`. The
/// refcount atomics + `dsa_free` are owned by the DSA-backed shared-TBM
/// provider, so this routes through [`dsa_shared_tbm_free`].
fn provide_tbm_free_shared_area(dsa: DsaAreaHandle, dp: types_tidbitmap::dsa_pointer) {
    // The C is infallible (void); surface a provider OOM/absence as a panic, the
    // same observable failure as the unported owner's seam-and-panic.
    dsa_shared_tbm_free(dsa, dp).expect("tbm_free_shared_area: shared-TBM provider failed");
}

/// `tbm_attach_shared_iterate(dsa, dp)` (`tidbitmap.c`): attach to a shared
/// iteration representation in the DSA area `dsa` at `dp`. Routed through the
/// DSA-backed shared-TBM provider.
fn attach_shared_iterate(
    dsa: DsaAreaHandle,
    dp: types_tidbitmap::dsa_pointer,
) -> PgResult<types_tidbitmap::TBMSharedIterator> {
    dsa_shared_tbm_attach(dsa, dp)
}

// ===========================================================================
// Shared (parallel, DSA-backed) sub-seams.
//
// The DSA area, its `dsa_pointer`s, the cross-backend `refcount` atomics, and
// the `TBMSharedIteratorState.lock` `LWLock` are shared-memory primitives this
// in-crate state cannot own. They belong to a DSA-backed shared-TBM provider
// that is NOT YET PORTED. The in-crate code does all the bitmap walking/sorting
// (see provide_tbm_prepare_shared_iterate); these sub-seams do only the DSA
// marshaling and panic until that owner lands (mirror-pg-and-panic).
// ===========================================================================

/// The bitmap layout the in-crate code marshals to the shared-TBM provider:
/// the dense `PagetableEntry` array (`PTEntryArray.ptentry`) plus the sorted
/// exact/lossy index arrays (`PTIterationArray.index`) into it, and the scalar
/// `TBMSharedIteratorState` members.
struct SharedBitmapLayout {
    // The scalar `TBMSharedIteratorState` members are read by the (unported)
    // shared-TBM provider sub-seam, not in-crate yet — hence `dead_code`.
    #[allow(dead_code)]
    nentries: i32,
    #[allow(dead_code)]
    maxentries: i32,
    #[allow(dead_code)]
    npages: i32,
    #[allow(dead_code)]
    nchunks: i32,
    entries: Vec<PagetableEntry>,
    spages: Vec<i32>,
    schunks: Vec<i32>,
}

/// SEAM (unported owner: the DSA-backed shared-TIDBitmap provider — `dsa_*`
/// allocation of `TBMSharedIteratorState` / `PTEntryArray` / `PTIterationArray`,
/// `pg_atomic_*` refcounts, `LWLockInitialize(LWTRANCHE_SHARED_TIDBITMAP)`).
/// Mirrors the DSA-marshaling tail of `tbm_prepare_shared_iterate`.
fn dsa_shared_tbm_prepare(
    _dsa: DsaAreaHandle,
    _layout: SharedBitmapLayout,
) -> PgResult<types_tidbitmap::dsa_pointer> {
    panic!(
        "SEAM tbm_prepare_shared_iterate (DSA path): the DSA-backed shared-TIDBitmap \
         provider (TBMSharedIteratorState/PTEntryArray/PTIterationArray dsa_allocate, \
         refcount atomics, LWLockInitialize) is not yet ported"
    )
}

/// SEAM (unported owner: the DSA-backed shared-TIDBitmap provider). Mirrors
/// `tbm_attach_shared_iterate`: `dsa_get_address` of the shared state + arrays.
fn dsa_shared_tbm_attach(
    _dsa: DsaAreaHandle,
    _dp: types_tidbitmap::dsa_pointer,
) -> PgResult<types_tidbitmap::TBMSharedIterator> {
    panic!(
        "SEAM tbm_attach_shared_iterate: the DSA-backed shared-TIDBitmap provider \
         (dsa_get_address of the shared state/arrays) is not yet ported"
    )
}

/// SEAM (unported owner: the DSA-backed shared-TIDBitmap provider). Mirrors
/// `tbm_free_shared_area`: refcount `pg_atomic_sub_fetch_u32` + `dsa_free`.
fn dsa_shared_tbm_free(
    _dsa: DsaAreaHandle,
    _dp: types_tidbitmap::dsa_pointer,
) -> PgResult<()> {
    panic!(
        "SEAM tbm_free_shared_area: the DSA-backed shared-TIDBitmap provider \
         (refcount atomics + dsa_free) is not yet ported"
    )
}

/// `tbm_shared_iterate(iterator, tbmres)` (`tidbitmap.c`): advance a shared
/// iteration (the table-AM scan owns this call once ported). Routes through the
/// DSA-backed shared-TBM provider (it holds the iterator `LWLock`).
pub fn tbm_shared_iterate(
    _iterator: &mut types_tidbitmap::TBMSharedIterator,
    _tbmres: &mut TBMIterateResult,
) -> PgResult<bool> {
    panic!(
        "SEAM tbm_shared_iterate: the DSA-backed shared-TIDBitmap provider \
         (LWLock + shared state walk) is not yet ported"
    )
}

/// `tbm_iterate(iterator, tbmres)` (`tidbitmap.c`): advance the unified iterator
/// (the table-AM scan owns this call once ported). The private arm walks the
/// in-crate bitmap; the shared arm routes through the provider.
pub fn tbm_iterate(
    iterator: &mut types_tidbitmap::TBMIterator,
    tbmres: &mut TBMIterateResult,
) -> PgResult<bool> {
    if iterator.shared {
        let it = iterator
            .shared_iterator
            .as_mut()
            .ok_or_else(|| PgError::error("tbm_iterate: shared iterator is NULL"))?;
        tbm_shared_iterate(it, tbmres)
    } else {
        let carrier = iterator
            .private_iterator
            .as_mut()
            .ok_or_else(|| PgError::error("tbm_iterate: private iterator is NULL"))?;
        let payload = carrier
            .0
            .as_mut()
            .and_then(|p| p.downcast_mut::<PrivateIterCarrier>())
            .ok_or_else(|| PgError::error("tbm_iterate: private iterator carrier is foreign"))?;
        // C: `tbm = iterator->tbm`. The bitmap is owned by the executor's
        // `node->tbm` for the whole scan and is read-only during iteration
        // (private_iterate takes &self).
        // SAFETY: `payload.tbm` is the back-pointer captured at
        // tbm_begin_iterate; the pointed-at bitmap outlives the iterator (it is
        // the executor's node->tbm) and is not mutated during iteration.
        let inner = unsafe { &*payload.tbm };
        Ok(private_iterate(inner, &mut payload.iter, tbmres))
    }
}

/// Seam provider for `tbm_create(maxbytes, dsa)`: build the bitmap with the
/// in-crate constructor, then box the opaque carrier into the caller's query
/// context (the C palloc lives in `CurrentMemoryContext`).
fn provide_tbm_create<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    maxbytes: usize,
    dsa: Option<DsaAreaHandle>,
) -> PgResult<mcx::PgBox<'mcx, types_tidbitmap::TIDBitmap>> {
    let tbm = tbm_create(maxbytes, dsa);
    Ok(mcx::PgBox::new_in(tbm, mcx))
}

/// Seam provider for `tbm_union(a, b)`.
fn provide_tbm_union(
    a: &mut types_tidbitmap::TIDBitmap,
    b: &types_tidbitmap::TIDBitmap,
) -> PgResult<()> {
    tbm_union(a, b)
}

/// Install this family's inward seams. Called from
/// [`crate::init_seams`] once the family is filled.
pub fn init_seams() {
    backend_nodes_core_seams::tbm_add_tuple::set(provide_tbm_add_tuple);
    backend_nodes_core_seams::tbm_add_tuples::set(provide_tbm_add_tuples);
    backend_nodes_core_seams::tbm_add_page::set(provide_tbm_add_page);

    backend_nodes_core_tidbitmap_seams::tbm_create::set(provide_tbm_create);
    backend_nodes_core_tidbitmap_seams::tbm_union::set(provide_tbm_union);
    backend_nodes_core_tidbitmap_seams::tbm_prepare_shared_iterate::set(
        provide_tbm_prepare_shared_iterate,
    );
    backend_nodes_core_tidbitmap_seams::tbm_begin_iterate::set(provide_tbm_begin_iterate);
    backend_nodes_core_tidbitmap_seams::tbm_end_iterate::set(provide_tbm_end_iterate);
    backend_nodes_core_tidbitmap_seams::tbm_free::set(provide_tbm_free);
    backend_nodes_core_tidbitmap_seams::tbm_free_shared_area::set(provide_tbm_free_shared_area);
    backend_nodes_core_tidbitmap_seams::tbm_intersect::set(tbm_intersect);
    backend_nodes_core_tidbitmap_seams::tbm_is_empty::set(tbm_is_empty);

    // `tbm_calculate_entries(maxbytes)` (tidbitmap.c) — the cost code
    // (`cost_bitmap_heap_scan` / `compute_bitmap_pages`) consumes the estimated
    // max pagetable entries as a float; the in-crate function returns the C
    // `int`, so the seam adapter widens it to f64.
    backend_optimizer_path_costsize_seams::tbm_calculate_entries::set(|maxbytes| {
        tbm_calculate_entries(maxbytes) as f64
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tid(block: BlockNumber, off: OffsetNumber) -> ItemPointerData {
        ItemPointerData::new(block, off)
    }

    /// Drive a private iteration of `tbm` to completion. Operates directly on
    /// the owned [`TidBitmapInner`] (the private struct), no carrier needed.
    fn drain(tbm: &mut TidBitmapInner) -> Vec<(BlockNumber, bool, bool, Vec<OffsetNumber>)> {
        let mut out = Vec::new();
        let mut it = begin_private_iterate(tbm).unwrap();
        let mut res = TBMIterateResult::default();
        while private_iterate(tbm, &mut it, &mut res) {
            let mut offsets = Vec::new();
            if !res.lossy {
                let mut buf = [0u16; TBM_MAX_TUPLES_PER_PAGE];
                let n = tbm_extract_page_tuple(&res, &mut buf[..]);
                offsets.extend_from_slice(&buf[..n as usize]);
            }
            out.push((res.blockno, res.lossy, res.recheck, offsets));
        }
        assert_eq!(res.blockno, InvalidBlockNumber);
        out
    }

    fn make(maxbytes: usize) -> TidBitmapInner {
        TidBitmapInner::create(maxbytes, None)
    }

    #[test]
    fn create_is_empty() {
        let bm = make(1024 * 1024);
        assert!(bm.is_empty());
    }

    #[test]
    fn single_page_exact_iterate() {
        let mut bm = make(1024 * 1024);
        let tids = [tid(5, 1), tid(5, 3), tid(5, 7)];
        bm.add_tuples(&tids, false).unwrap();
        let pages = drain(&mut bm);
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].0, 5);
        assert!(!pages[0].1);
        assert!(!pages[0].2);
        assert_eq!(pages[0].3, vec![1, 3, 7]);
    }

    #[test]
    fn multi_page_sorted_and_recheck() {
        let mut bm = make(1024 * 1024);
        let tids = [tid(9, 2), tid(2, 1), tid(2, 4), tid(7, 6)];
        bm.add_tuples(&tids, true).unwrap();
        let pages = drain(&mut bm);
        let blocks: Vec<_> = pages.iter().map(|p| p.0).collect();
        assert_eq!(blocks, vec![2, 7, 9]);
        for p in &pages {
            assert!(!p.1);
            assert!(p.2);
        }
        assert_eq!(pages[0].3, vec![1, 4]);
        assert_eq!(pages[1].3, vec![6]);
        assert_eq!(pages[2].3, vec![2]);
    }

    #[test]
    fn offset_out_of_range_errors() {
        let mut bm = make(1024 * 1024);
        let bad = [tid(1, 0)];
        let err = bm.add_tuples(&bad, false).unwrap_err();
        assert!(err.message().contains("tuple offset out of range: 0"));
    }

    #[test]
    fn add_page_is_lossy() {
        let mut bm = make(1024 * 1024);
        bm.add_page(42).unwrap();
        let pages = drain(&mut bm);
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].0, 42);
        assert!(pages[0].1);
        assert!(pages[0].2);
        assert!(pages[0].3.is_empty());
    }

    #[test]
    fn lossify_under_memory_pressure() {
        let mut bm = make(1);
        for blk in 1..400u32 {
            bm.add_tuples(&[tid(blk, 1)], false).unwrap();
        }
        let pages = drain(&mut bm);
        let mut seen = std::collections::BTreeSet::new();
        for (blk, _l, _r, _o) in &pages {
            seen.insert(*blk);
        }
        for blk in 1..400u32 {
            assert!(seen.contains(&blk), "block {blk} missing after lossify");
        }
        assert!(pages.iter().any(|p| p.1), "expected some lossy pages");
    }

    #[test]
    fn union_merges_pages() {
        let mut a = make(1024 * 1024);
        let mut b = make(1024 * 1024);
        a.add_tuples(&[tid(1, 1), tid(3, 2)], false).unwrap();
        b.add_tuples(&[tid(3, 5), tid(8, 1)], false).unwrap();
        a.union(&b).unwrap();
        let pages = drain(&mut a);
        let blocks: Vec<_> = pages.iter().map(|p| p.0).collect();
        assert_eq!(blocks, vec![1, 3, 8]);
        let p3 = pages.iter().find(|p| p.0 == 3).unwrap();
        assert_eq!(p3.3, vec![2, 5]);
    }

    #[test]
    fn intersect_keeps_common_and_drops_rest() {
        let mut a = make(1024 * 1024);
        let mut b = make(1024 * 1024);
        a.add_tuples(&[tid(1, 1), tid(3, 2), tid(3, 5), tid(8, 1)], false)
            .unwrap();
        b.add_tuples(&[tid(3, 5), tid(8, 1)], false).unwrap();
        a.intersect(&b).unwrap();
        let pages = drain(&mut a);
        let blocks: Vec<_> = pages.iter().map(|p| p.0).collect();
        assert_eq!(blocks, vec![3, 8]);
        let p3 = pages.iter().find(|p| p.0 == 3).unwrap();
        assert_eq!(p3.3, vec![5]);
    }

    #[test]
    fn intersect_with_lossy_b_sets_recheck() {
        let mut a = make(1024 * 1024);
        let mut b = make(1024 * 1024);
        a.add_tuples(&[tid(10, 2), tid(10, 4)], false).unwrap();
        b.add_page(10).unwrap();
        a.intersect(&b).unwrap();
        let pages = drain(&mut a);
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].0, 10);
        assert!(!pages[0].1);
        assert!(pages[0].2);
        assert_eq!(pages[0].3, vec![2, 4]);
    }

    #[test]
    fn calculate_entries_clamps() {
        assert_eq!(tbm_calculate_entries(0), 16);
        assert_eq!(tbm_calculate_entries(1), 16);
        let per = core::mem::size_of::<PagetableEntry>() + 2 * core::mem::size_of::<usize>();
        assert_eq!(tbm_calculate_entries(per * 100), 100);
    }

    #[test]
    fn extract_page_tuple_truncates_and_reports_total() {
        let mut bm = make(1024 * 1024);
        bm.add_tuples(&[tid(1, 1), tid(1, 2), tid(1, 3)], false)
            .unwrap();
        let mut it = begin_private_iterate(&mut bm).unwrap();
        let mut res = TBMIterateResult::default();
        let more = private_iterate(&bm, &mut it, &mut res);
        assert!(more);
        let mut buf = [0u16; 2];
        let n = tbm_extract_page_tuple(&res, &mut buf[..]);
        assert_eq!(n, 3);
        assert_eq!(buf, [1, 2]);
    }

    /// The opaque carrier owns the real inner by value (no side table / no
    /// integer key); round-tripping through the public seam-level entry points
    /// reaches the same live bitmap.
    #[test]
    fn carrier_holds_real_inner_by_value() {
        let mut tbm = tbm_create(1024 * 1024, None);
        assert!(tbm_is_empty(&tbm).unwrap());
        tbm_add_tuples(&mut tbm, &[tid(5, 1), tid(5, 3)], false).unwrap();
        assert!(!tbm_is_empty(&tbm).unwrap());
        // tbm_free drops the boxed inner (C pfree).
        provide_tbm_free(&mut tbm);
        assert!(tbm.0.is_none());
    }
}
