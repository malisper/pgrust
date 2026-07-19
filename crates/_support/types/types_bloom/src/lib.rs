//! bloom.h on-disk layouts, signature math, and scan/state vocabulary
//! (C contrib/bloom, 18.3), byte-for-byte. Pages carry no line pointers:
//! tuples are fixed-size cells laid end-to-end from PageGetContents,
//! addressed by offset arithmetic, with pd_lower tracking the end of the
//! used area (the metapage does the same with its single BloomMetaPageData
//! blob). Lives under _support/types so relscan's IndexScanOpaque can carry
//! the scan opaque without a cycle through the contrib crate.

use types_core::{BlockNumber, Oid, BLCKSZ};

#[cfg(test)]
mod tests;

pub type BloomSignatureWord = u16;

/// Support procedure numbers (bloom.h).
pub const BLOOM_HASH_PROC: u16 = 1;
pub const BLOOM_OPTIONS_PROC: u16 = 2;
pub const BLOOM_NPROC: u16 = 2;

/// Scan strategies: equality only.
pub const BLOOM_EQUAL_STRATEGY: u16 = 1;
pub const BLOOM_NSTRATEGIES: u16 = 1;

pub const INDEX_MAX_KEYS: usize = 32;

pub const SIGNWORDBITS: i32 = 16;
/// Default/max signature length in BITS (reloption "length" is in bits).
pub const DEFAULT_BLOOM_LENGTH: i32 = 5 * SIGNWORDBITS;
pub const MAX_BLOOM_LENGTH: i32 = 256 * SIGNWORDBITS;
/// Default/max bits generated per index key (reloptions col1..col32).
pub const DEFAULT_BLOOM_BITS: i32 = 2;
pub const MAX_BLOOM_BITS: i32 = MAX_BLOOM_LENGTH - 1;

/// Bloom page flags.
pub const BLOOM_META: u16 = 1 << 0;
pub const BLOOM_DELETED: u16 = 2;

/// Last 2 bytes of every bloom page (pg_filedump aid).
pub const BLOOM_PAGE_ID: u16 = 0xFF83;
pub const BLOOM_MAGICK_NUMBER: u32 = 0xDBAC0DED;

pub const BLOOM_METAPAGE_BLKNO: BlockNumber = 0;
pub const BLOOM_HEAD_BLKNO: BlockNumber = 1;

pub const MAXALIGN: usize = 8;
pub const SIZE_OF_PAGE_HEADER: usize = 24;
/// PageGetContents == page + MAXALIGN(SizeOfPageHeaderData).
pub const PAGE_CONTENTS_OFF: usize = 24;
/// sizeof(BloomPageOpaqueData): maxoff u16 + flags u16 + unused u16 + page_id u16.
pub const BLOOM_PAGE_OPAQUE_SIZE: usize = 8;
pub const OPAQUE_OFF: usize = BLCKSZ - BLOOM_PAGE_OPAQUE_SIZE;

/// offsetof(BloomTuple, sign): ItemPointerData is 3 bare u16s.
pub const BLOOM_TUPLE_HDR_SZ: usize = 6;

/// sizeof(BloomOptions): vl_len_ i32 + bloomLength i32 + bitSize[32] i32.
pub const BLOOM_OPTIONS_SIZE: usize = 8 + 4 * INDEX_MAX_KEYS;
/// BloomMetaPageData offsets within page contents.
pub const META_MAGICK_OFF: usize = 0;
pub const META_NSTART_OFF: usize = 4;
pub const META_NEND_OFF: usize = 6;
pub const META_OPTS_OFF: usize = 8;
pub const META_NOTFULL_OFF: usize = META_OPTS_OFF + BLOOM_OPTIONS_SIZE; // 144

/// Number of block numbers that fit in the metapage's FreeBlockNumberArray:
/// MAXALIGN_DOWN(BLCKSZ - SizeOfPageHeaderData - MAXALIGN(opaque)
///               - MAXALIGN(2*u16 + u32 + BloomOptions)) / sizeof(BlockNumber).
pub const BLOOM_META_BLOCK_N: usize = {
    let inner = 2 * 2 + 4 + BLOOM_OPTIONS_SIZE; // nStart+nEnd+magick+opts
    let aligned_inner = (inner + MAXALIGN - 1) & !(MAXALIGN - 1);
    let free = BLCKSZ - SIZE_OF_PAGE_HEADER - BLOOM_PAGE_OPAQUE_SIZE - aligned_inner;
    (free & !(MAXALIGN - 1)) / 4
};
/// sizeof(BloomMetaPageData).
pub const BLOOM_META_DATA_SIZE: usize = META_NOTFULL_OFF + 4 * BLOOM_META_BLOCK_N;

// ---------------------------------------------------------------------------
// Page header / opaque accessors over raw BLCKSZ byte slices.
// ---------------------------------------------------------------------------

#[inline]
fn get_u16(page: &[u8], off: usize) -> u16 {
    u16::from_ne_bytes([page[off], page[off + 1]])
}

#[inline]
fn set_u16(page: &mut [u8], off: usize, v: u16) {
    page[off..off + 2].copy_from_slice(&v.to_ne_bytes());
}

#[inline]
pub fn pd_lower(page: &[u8]) -> u16 {
    get_u16(page, 12)
}

#[inline]
pub fn set_pd_lower(page: &mut [u8], v: u16) {
    set_u16(page, 12, v);
}

#[inline]
pub fn pd_upper(page: &[u8]) -> u16 {
    get_u16(page, 14)
}

/// PageIsNew: pd_upper == 0 (all-zeroes page).
#[inline]
pub fn page_is_new(page: &[u8]) -> bool {
    pd_upper(page) == 0
}

#[inline]
pub fn opaque_maxoff(page: &[u8]) -> u16 {
    get_u16(page, OPAQUE_OFF)
}

#[inline]
pub fn set_opaque_maxoff(page: &mut [u8], v: u16) {
    set_u16(page, OPAQUE_OFF, v);
}

#[inline]
pub fn opaque_flags(page: &[u8]) -> u16 {
    get_u16(page, OPAQUE_OFF + 2)
}

#[inline]
pub fn set_opaque_flags(page: &mut [u8], v: u16) {
    set_u16(page, OPAQUE_OFF + 2, v);
}

#[inline]
pub fn page_is_meta(page: &[u8]) -> bool {
    opaque_flags(page) & BLOOM_META != 0
}

#[inline]
pub fn page_is_deleted(page: &[u8]) -> bool {
    opaque_flags(page) & BLOOM_DELETED != 0
}

#[inline]
pub fn page_set_deleted(page: &mut [u8]) {
    let f = opaque_flags(page);
    set_opaque_flags(page, f | BLOOM_DELETED);
}

/// BloomInitPage: PageInit(page, BLCKSZ, sizeof(BloomPageOpaqueData)) + flags
/// + page id. PageInit zero-fills and sets pd_lower/pd_upper/pd_special.
pub fn bloom_init_page(page: &mut [u8], flags: u16) {
    page.fill(0);
    // PageInit: pd_lower = SizeOfPageHeaderData, pd_upper = pd_special = the
    // MAXALIGN'd special offset; pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION.
    set_u16(page, 12, SIZE_OF_PAGE_HEADER as u16); // pd_lower
    set_u16(page, 14, OPAQUE_OFF as u16); // pd_upper
    set_u16(page, 16, OPAQUE_OFF as u16); // pd_special
    set_u16(page, 18, (BLCKSZ as u16) | 4); // pd_pagesize_version
    set_opaque_maxoff(page, 0);
    set_opaque_flags(page, flags);
    set_u16(page, OPAQUE_OFF + 4, 0); // unused
    set_u16(page, OPAQUE_OFF + 6, BLOOM_PAGE_ID);
}

// ---------------------------------------------------------------------------
// BloomOptions (the parsed-reloptions struct, stored verbatim in the metapage).
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BloomOptions {
    /// Signature length in WORDS (bloptions converts the bits reloption).
    pub bloom_length: i32,
    /// Bits generated per index column (col1..col32 reloptions).
    pub bit_size: [i32; INDEX_MAX_KEYS],
}

impl Default for BloomOptions {
    /// makeDefaultBloomOptions.
    fn default() -> Self {
        BloomOptions {
            bloom_length: (DEFAULT_BLOOM_LENGTH + SIGNWORDBITS - 1) / SIGNWORDBITS,
            bit_size: [DEFAULT_BLOOM_BITS; INDEX_MAX_KEYS],
        }
    }
}

impl BloomOptions {
    /// Deserialize from the C struct image (vl_len_ header skipped).
    pub fn read(b: &[u8]) -> BloomOptions {
        let g4 = |o: usize| i32::from_ne_bytes(b[o..o + 4].try_into().unwrap());
        let mut bit_size = [0i32; INDEX_MAX_KEYS];
        for (i, bs) in bit_size.iter_mut().enumerate() {
            *bs = g4(8 + 4 * i);
        }
        BloomOptions {
            bloom_length: g4(4),
            bit_size,
        }
    }

    /// Serialize as the C struct image: SET_VARSIZE(opts, sizeof(BloomOptions)).
    pub fn write(&self, b: &mut [u8]) {
        let vl = (BLOOM_OPTIONS_SIZE as u32) << 2;
        b[0..4].copy_from_slice(&vl.to_ne_bytes());
        b[4..8].copy_from_slice(&self.bloom_length.to_ne_bytes());
        for i in 0..INDEX_MAX_KEYS {
            b[8 + 4 * i..12 + 4 * i].copy_from_slice(&self.bit_size[i].to_ne_bytes());
        }
    }

    /// sizeOfBloomTuple for these options.
    pub fn size_of_bloom_tuple(&self) -> usize {
        BLOOM_TUPLE_HDR_SZ + 2 * self.bloom_length as usize
    }
}

// ---------------------------------------------------------------------------
// Metapage accessors (offsets relative to the raw page).
// ---------------------------------------------------------------------------

#[inline]
pub fn meta_magick(page: &[u8]) -> u32 {
    let o = PAGE_CONTENTS_OFF + META_MAGICK_OFF;
    u32::from_ne_bytes(page[o..o + 4].try_into().unwrap())
}

#[inline]
pub fn meta_nstart(page: &[u8]) -> u16 {
    get_u16(page, PAGE_CONTENTS_OFF + META_NSTART_OFF)
}

#[inline]
pub fn meta_set_nstart(page: &mut [u8], v: u16) {
    set_u16(page, PAGE_CONTENTS_OFF + META_NSTART_OFF, v);
}

#[inline]
pub fn meta_nend(page: &[u8]) -> u16 {
    get_u16(page, PAGE_CONTENTS_OFF + META_NEND_OFF)
}

#[inline]
pub fn meta_set_nend(page: &mut [u8], v: u16) {
    set_u16(page, PAGE_CONTENTS_OFF + META_NEND_OFF, v);
}

#[inline]
pub fn meta_opts(page: &[u8]) -> BloomOptions {
    BloomOptions::read(&page[PAGE_CONTENTS_OFF + META_OPTS_OFF..])
}

#[inline]
pub fn meta_notfull(page: &[u8], i: usize) -> BlockNumber {
    let o = PAGE_CONTENTS_OFF + META_NOTFULL_OFF + 4 * i;
    u32::from_ne_bytes(page[o..o + 4].try_into().unwrap())
}

#[inline]
pub fn meta_set_notfull(page: &mut [u8], i: usize, blkno: BlockNumber) {
    let o = PAGE_CONTENTS_OFF + META_NOTFULL_OFF + 4 * i;
    page[o..o + 4].copy_from_slice(&blkno.to_ne_bytes());
}

/// BloomFillMetapage over an already-BloomInitPage'd BLOOM_META page.
pub fn fill_metapage(page: &mut [u8], opts: &BloomOptions) {
    let c = PAGE_CONTENTS_OFF;
    page[c..c + BLOOM_META_DATA_SIZE].fill(0);
    page[c..c + 4].copy_from_slice(&BLOOM_MAGICK_NUMBER.to_ne_bytes());
    opts.write(&mut page[c + META_OPTS_OFF..]);
    // pd_lower += sizeof(BloomMetaPageData)
    let lower = pd_lower(page) + BLOOM_META_DATA_SIZE as u16;
    set_pd_lower(page, lower);
    debug_assert!(pd_lower(page) <= pd_upper(page));
}

// ---------------------------------------------------------------------------
// Tuples.
// ---------------------------------------------------------------------------

/// Byte offset of tuple `offset` (1-based) on a data page.
#[inline]
pub fn tuple_off(size_of_tuple: usize, offset: u16) -> usize {
    PAGE_CONTENTS_OFF + size_of_tuple * (offset as usize - 1)
}

/// BloomPageGetFreeSpace.
#[inline]
pub fn page_free_space(size_of_tuple: usize, maxoff: u16) -> isize {
    (BLCKSZ - SIZE_OF_PAGE_HEADER - BLOOM_PAGE_OPAQUE_SIZE) as isize
        - (maxoff as usize * size_of_tuple) as isize
}

/// BloomPageAddItem: copy `tuple` (heapPtr+signature image, size_of_tuple
/// bytes) to the end of the page; false if it doesn't fit.
pub fn page_add_item(page: &mut [u8], size_of_tuple: usize, tuple: &[u8]) -> bool {
    debug_assert!(!page_is_new(page) && !page_is_deleted(page));
    debug_assert_eq!(tuple.len(), size_of_tuple);
    let maxoff = opaque_maxoff(page);
    if page_free_space(size_of_tuple, maxoff) < size_of_tuple as isize {
        return false;
    }
    let off = tuple_off(size_of_tuple, maxoff + 1);
    page[off..off + size_of_tuple].copy_from_slice(tuple);
    set_opaque_maxoff(page, maxoff + 1);
    let lower = tuple_off(size_of_tuple, maxoff + 2);
    set_pd_lower(page, lower as u16);
    debug_assert!(pd_lower(page) <= pd_upper(page));
    true
}

// ---------------------------------------------------------------------------
// Signature math: the private Park-Miller generator (blutils.c myRand/mySrand).
// C keeps the state in a file-static; it never survives one signValue call,
// so the port keeps it in a local value.
// ---------------------------------------------------------------------------

pub struct BlRng {
    next: i32,
}

impl BlRng {
    /// mySrand: the uint32->int32 assignment wraps (two's complement) and C's
    /// truncated % keeps the sign, so `next` can leave [1, 0x7ffffffe] for
    /// seeds >= 2^31; myRand below reproduces C's arithmetic for those too.
    pub fn new(seed: u32) -> BlRng {
        let next = seed as i32;
        BlRng {
            next: (next % 0x7ffffffe) + 1,
        }
    }

    /// myRand: x = (7^5 * x) mod (2^31 - 1), Park & Miller.
    pub fn next(&mut self) -> i32 {
        let hi = self.next / 127773;
        let lo = self.next % 127773;
        // |x| < 2^31 for all reachable states; wrapping_* documents intent.
        let mut x = 16807i32
            .wrapping_mul(lo)
            .wrapping_sub(2836i32.wrapping_mul(hi));
        if x < 0 {
            x += 0x7fffffff;
        }
        self.next = x;
        x - 1
    }
}

/// signValue's bit-selection tail: hash already computed by the caller.
/// C's SETBIT(sign, nBit) with the (astronomically rare) nBit == -1 — the
/// stuck next==0 state returns -1 forever — compiles on mainstream hardware
/// to `1 << 31` truncated to uint16 == 0 on word 0 (shift count masked mod
/// 32): a no-op. We reproduce that exactly rather than panic on a negative
/// shift.
pub fn add_value_bits(
    sign: &mut [BloomSignatureWord],
    attno: usize,
    hash_val: u32,
    bit_size: i32,
    bloom_length_words: i32,
) {
    let mut rng = BlRng::new(attno as u32);
    let mixed = hash_val ^ (rng.next() as u32);
    let mut rng = BlRng::new(mixed);
    for _ in 0..bit_size {
        let n_bit = rng.next() % (bloom_length_words * SIGNWORDBITS);
        let word = (n_bit / SIGNWORDBITS) as usize; // trunc division: -1 -> 0
        let sh = (n_bit % SIGNWORDBITS) as u32 & 31;
        sign[word] |= 1u32.wrapping_shl(sh) as u16;
    }
}

/// The blgetbitmap signature-containment test: every bit of the scan
/// signature must be set in the stored tuple signature.
#[inline]
pub fn signature_matches(tuple_sign: &[u8], scan_sign: &[BloomSignatureWord]) -> bool {
    for (i, &s) in scan_sign.iter().enumerate() {
        let t = u16::from_ne_bytes([tuple_sign[2 * i], tuple_sign[2 * i + 1]]);
        if t & s != s {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// BloomState / scan opaque (bloom.h BloomState, BloomScanOpaqueData).
// ---------------------------------------------------------------------------

/// C BloomState: per-index scratch built by initBloomState. `opts` always
/// comes from the METAPAGE (frozen at build), never current reloptions —
/// that's why ALTER INDEX ... SET (length=...) doesn't change a live index.
pub struct BloomState {
    /// BLOOM_HASH_PROC FmgrInfo per key column.
    pub hash_fn: Vec<types_fmgr::FmgrInfo>,
    pub collations: Vec<Oid>,
    pub opts: BloomOptions,
    pub ncolumns: usize,
    /// Precomputed BLOOMTUPLEHDRSZ + 2 * opts.bloom_length.
    pub size_of_bloom_tuple: usize,
}

/// C BloomScanOpaqueData: the scan signature is built lazily on the first
/// blgetbitmap call (None until then; reset by blrescan).
pub struct BloomScanOpaqueData {
    pub sign: Option<Vec<BloomSignatureWord>>,
    pub state: BloomState,
}
