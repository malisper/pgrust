//! The claim-scoped detoast/rehome arena (C1).
//!
//! Laws (each is a test in `tests::arena`):
//!
//! - **Alignment law**: every allocation starts ≥8-byte-aligned — the
//!   jsonb/numeric 4-align container law with margin (jsonb container
//!   internals require a ≥4-aligned container base so embedded numeric
//!   digit arrays stay 2-aligned; blob-packed offsets violate it SILENTLY,
//!   and fresh-Vec-backed allocators hide the violation because system
//!   mallocs 16-align everything — the arena rounds its own cursor, so the
//!   guarantee is structural, not inherited).
//! - **Value stability**: allocations never move until [`ClaimArena::reset`]
//!   (chunk-list growth, no realloc) — datum cells may alias arena values
//!   for the whole batch.
//! - **Claim-scoped lifetime + the decode-arena reuse trap**: `reset()`
//!   recycles the memory (reset-not-free — chunks are retained) and bumps
//!   [`ClaimArena::epoch`]; a pointer minted under an older epoch reads
//!   scrambled bytes, not a crash. Consumers pin the epoch at staging and
//!   assert it at consumption ([`ClaimArena::assert_epoch`]).
//! - **Worker-private** (R2): the arena is `!Send`/`!Sync` by construction.

use core::alloc::Layout;
use core::ptr::NonNull;

/// Per-value alignment guarantee (the container law's margin).
pub(crate) const ARENA_ALIGN: usize = 8;

const FIRST_CHUNK_BYTES: usize = 4 * 1024;
const MAX_CHUNK_BYTES: usize = 1024 * 1024;

struct Chunk {
    base: NonNull<u8>,
    cap: usize,
}

impl Chunk {
    fn layout(cap: usize) -> Layout {
        // Cap is a multiple of ARENA_ALIGN and nonzero (alloc_chunk).
        Layout::from_size_align(cap, ARENA_ALIGN).expect("arena chunk layout")
    }

    fn new(cap: usize) -> Chunk {
        debug_assert!(cap > 0 && cap % ARENA_ALIGN == 0);
        // SAFETY: nonzero size.
        let raw = unsafe { std::alloc::alloc(Self::layout(cap)) };
        let Some(base) = NonNull::new(raw) else {
            std::alloc::handle_alloc_error(Self::layout(cap));
        };
        Chunk { base, cap }
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        // SAFETY: allocated in Chunk::new with the same layout.
        unsafe { std::alloc::dealloc(self.base.as_ptr(), Self::layout(self.cap)) };
    }
}

/// The claim-scoped bump arena detoast and rehoming write into. See the
/// module docs for the four laws.
pub struct ClaimArena {
    chunks: Vec<Chunk>,
    /// Index of the chunk the cursor is in (`chunks[..cur]` are full).
    cur: usize,
    /// Byte offset of the cursor within `chunks[cur]`; ALWAYS a multiple of
    /// [`ARENA_ALIGN`] (advances round up — the alignment law's mechanism).
    offset: usize,
    /// Reuse-trap witness: bumped by every [`ClaimArena::reset`].
    epoch: u64,
}

impl Default for ClaimArena {
    fn default() -> Self {
        Self::new()
    }
}

impl ClaimArena {
    pub fn new() -> ClaimArena {
        ClaimArena { chunks: Vec::new(), cur: 0, offset: 0, epoch: 1 }
    }

    /// Allocate `len` bytes; the returned pointer is ≥8-aligned (alignment
    /// law) and valid, unmoved, until the next [`ClaimArena::reset`] (value
    /// stability + claim scope). `len == 0` is legal (returns a live
    /// aligned cursor).
    pub fn alloc(&mut self, len: usize) -> NonNull<u8> {
        let advance = len.checked_add(ARENA_ALIGN - 1).expect("arena alloc overflow")
            & !(ARENA_ALIGN - 1);
        loop {
            if let Some(chunk) = self.chunks.get(self.cur) {
                debug_assert!(self.offset % ARENA_ALIGN == 0);
                if advance <= chunk.cap - self.offset {
                    // SAFETY: offset + advance <= cap.
                    let p = unsafe { chunk.base.as_ptr().add(self.offset) };
                    self.offset += advance;
                    // SAFETY: derived from a NonNull base.
                    return unsafe { NonNull::new_unchecked(p) };
                }
                if self.cur + 1 < self.chunks.len() {
                    // Reuse the next retained chunk (reset-not-free).
                    self.cur += 1;
                    self.offset = 0;
                    continue;
                }
            }
            self.grow(advance);
        }
    }

    #[cold]
    fn grow(&mut self, need: usize) {
        let last_cap = self.chunks.last().map_or(0, |c| c.cap);
        let cap = need.max((last_cap * 2).clamp(FIRST_CHUNK_BYTES, MAX_CHUNK_BYTES));
        self.chunks.push(Chunk::new(cap));
        self.cur = self.chunks.len() - 1;
        self.offset = 0;
    }

    /// Copy `bytes` into the arena (≥8-aligned start), returning the value
    /// pointer.
    pub fn alloc_copy(&mut self, bytes: &[u8]) -> NonNull<u8> {
        let dst = self.alloc(bytes.len());
        // SAFETY: dst is a fresh arena range of bytes.len() bytes; arena
        // memory never overlaps a caller slice.
        unsafe { core::ptr::copy_nonoverlapping(bytes.as_ptr(), dst.as_ptr(), bytes.len()) };
        dst
    }

    /// Recycle every value: cursor back to the first retained chunk
    /// (reset-not-free), epoch bumped. Every pointer minted before this
    /// call is DEAD — reading it is the decode-arena reuse trap (stale
    /// bytes, not a crash), which is why consumers carry the epoch.
    pub fn reset(&mut self) {
        self.cur = 0;
        self.offset = 0;
        self.epoch += 1;
    }

    /// The current lifetime epoch (bumped by every reset). Consumers that
    /// stage arena pointers record it and re-assert before consuming.
    #[inline]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The reuse-trap tripwire: panics when `staged` is not the live epoch
    /// (a staged arena pointer survived a reset).
    #[inline]
    #[track_caller]
    pub fn assert_epoch(&self, staged: u64) {
        assert!(
            staged == self.epoch,
            "decode-arena reuse trap: staged epoch {staged} vs live {} — an arena pointer \
             outlived its claim (R1/R4)",
            self.epoch
        );
    }

    /// Bytes currently allocated (this epoch).
    pub fn used(&self) -> usize {
        self.chunks[..self.cur].iter().map(|c| c.cap).sum::<usize>() + self.offset
    }

    /// Retained capacity across resets (reset-not-free witness).
    pub fn retained_capacity(&self) -> usize {
        self.chunks.iter().map(|c| c.cap).sum()
    }
}
