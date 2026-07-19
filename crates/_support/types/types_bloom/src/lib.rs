//! bloom.h on-disk layouts + signature math (C contrib/bloom, 18.3),
//! byte-for-byte. Bloom pages carry no line pointers: fixed-size tuple cells
//! from PageGetContents, pd_lower tracking the used end. In _support/types so
//! relscan's IndexScanOpaque can carry the scan opaque without a crate cycle.

use types_core::{BlockNumber, Oid, BLCKSZ};

#[cfg(test)]
mod tests;

pub type BloomSignatureWord = u16;

pub const BLOOM_HASH_PROC: u16 = 1;
pub const BLOOM_OPTIONS_PROC: u16 = 2;
pub const BLOOM_NPROC: u16 = 2;

pub const BLOOM_EQUAL_STRATEGY: u16 = 1;
pub const BLOOM_NSTRATEGIES: u16 = 1;

pub const INDEX_MAX_KEYS: usize = 32;

pub const SIGNWORDBITS: i32 = 16;
/// Signature lengths in BITS ("length" reloption); bloom_length is in WORDS.
pub const DEFAULT_BLOOM_LENGTH: i32 = 5 * SIGNWORDBITS;
pub const MAX_BLOOM_LENGTH: i32 = 256 * SIGNWORDBITS;
pub const DEFAULT_BLOOM_BITS: i32 = 2;
pub const MAX_BLOOM_BITS: i32 = MAX_BLOOM_LENGTH - 1;

pub const BLOOM_META: u16 = 1 << 0;
pub const BLOOM_DELETED: u16 = 2;

pub const BLOOM_PAGE_ID: u16 = 0xFF83;
pub const BLOOM_MAGICK_NUMBER: u32 = 0xDBAC0DED;

pub const BLOOM_METAPAGE_BLKNO: BlockNumber = 0;
pub const BLOOM_HEAD_BLKNO: BlockNumber = 1;

pub const MAXALIGN: usize = 8;
pub const SIZE_OF_PAGE_HEADER: usize = 24;
pub const PAGE_CONTENTS_OFF: usize = 24; // PageGetContents == MAXALIGN(header)
pub const BLOOM_PAGE_OPAQUE_SIZE: usize = 8; // maxoff, flags, unused, page_id u16s
pub const OPAQUE_OFF: usize = BLCKSZ - BLOOM_PAGE_OPAQUE_SIZE;

pub const BLOOM_TUPLE_HDR_SZ: usize = 6; // ItemPointerData: 3 bare u16s

pub const BLOOM_OPTIONS_SIZE: usize = 8 + 4 * INDEX_MAX_KEYS; // incl. vl_len_
pub const META_MAGICK_OFF: usize = 0;
pub const META_NSTART_OFF: usize = 4;
pub const META_NEND_OFF: usize = 6;
pub const META_OPTS_OFF: usize = 8;
pub const META_NOTFULL_OFF: usize = META_OPTS_OFF + BLOOM_OPTIONS_SIZE; // 144

pub const BLOOM_META_BLOCK_N: usize = { // FreeBlockNumberArray: 2004 @ 8K
    let inner = 2 * 2 + 4 + BLOOM_OPTIONS_SIZE; // nStart+nEnd+magick+opts
    let aligned_inner = (inner + MAXALIGN - 1) & !(MAXALIGN - 1);
    let free = BLCKSZ - SIZE_OF_PAGE_HEADER - BLOOM_PAGE_OPAQUE_SIZE - aligned_inner;
    (free & !(MAXALIGN - 1)) / 4
};
pub const BLOOM_META_DATA_SIZE: usize = META_NOTFULL_OFF + 4 * BLOOM_META_BLOCK_N;

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

pub fn bloom_init_page(page: &mut [u8], flags: u16) {
    page.fill(0);
    set_u16(page, 12, SIZE_OF_PAGE_HEADER as u16); // pd_lower
    set_u16(page, 14, OPAQUE_OFF as u16); // pd_upper
    set_u16(page, 16, OPAQUE_OFF as u16); // pd_special
    set_u16(page, 18, (BLCKSZ as u16) | 4); // pd_pagesize_version
    set_opaque_maxoff(page, 0);
    set_opaque_flags(page, flags);
    set_u16(page, OPAQUE_OFF + 4, 0); // unused
    set_u16(page, OPAQUE_OFF + 6, BLOOM_PAGE_ID);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BloomOptions {
    /// Signature length in WORDS (bloptions converts the bits reloption).
    pub bloom_length: i32,
    pub bit_size: [i32; INDEX_MAX_KEYS],
}

impl Default for BloomOptions {
    fn default() -> Self {
        BloomOptions {
            bloom_length: (DEFAULT_BLOOM_LENGTH + SIGNWORDBITS - 1) / SIGNWORDBITS,
            bit_size: [DEFAULT_BLOOM_BITS; INDEX_MAX_KEYS],
        }
    }
}

impl BloomOptions {
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

    pub fn write(&self, b: &mut [u8]) {
        let vl = (BLOOM_OPTIONS_SIZE as u32) << 2; // SET_VARSIZE(sizeof(BloomOptions))
        b[0..4].copy_from_slice(&vl.to_ne_bytes());
        b[4..8].copy_from_slice(&self.bloom_length.to_ne_bytes());
        for i in 0..INDEX_MAX_KEYS {
            b[8 + 4 * i..12 + 4 * i].copy_from_slice(&self.bit_size[i].to_ne_bytes());
        }
    }

    pub fn size_of_bloom_tuple(&self) -> usize {
        BLOOM_TUPLE_HDR_SZ + 2 * self.bloom_length as usize
    }
}

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

/// Caller has already bloom_init_page'd (C BloomFillMetapage inits itself).
pub fn fill_metapage(page: &mut [u8], opts: &BloomOptions) {
    let c = PAGE_CONTENTS_OFF;
    page[c..c + BLOOM_META_DATA_SIZE].fill(0);
    page[c..c + 4].copy_from_slice(&BLOOM_MAGICK_NUMBER.to_ne_bytes());
    opts.write(&mut page[c + META_OPTS_OFF..]);
    let lower = pd_lower(page) + BLOOM_META_DATA_SIZE as u16;
    set_pd_lower(page, lower);
    debug_assert!(pd_lower(page) <= pd_upper(page));
}

#[inline]
pub fn tuple_off(size_of_tuple: usize, offset: u16) -> usize {
    PAGE_CONTENTS_OFF + size_of_tuple * (offset as usize - 1) // 1-based
}

#[inline]
pub fn page_free_space(size_of_tuple: usize, maxoff: u16) -> isize {
    (BLCKSZ - SIZE_OF_PAGE_HEADER - BLOOM_PAGE_OPAQUE_SIZE) as isize
        - (maxoff as usize * size_of_tuple) as isize
}

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

// C's file-static Park-Miller state never survives one signValue call.
pub struct BlRng {
    next: i32,
}

impl BlRng {
    /// mySrand: the uint32->int32 wrap + C's truncated % keep the sign, so
    /// `next` can leave [1, 0x7ffffffe] for seeds >= 2^31; next() copes.
    pub fn new(seed: u32) -> BlRng {
        let next = seed as i32;
        BlRng {
            next: (next % 0x7ffffffe) + 1,
        }
    }

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

/// signValue's bit-selection tail. C's SETBIT with nBit == -1 (stuck next==0
/// state) compiles to a shift-count-masked `1 << 31` truncated to u16 == 0 on
/// word 0 — a no-op, reproduced exactly instead of a negative-shift panic.
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

/// `opts` always comes from the METAPAGE (frozen at build), never current
/// reloptions; ALTER INDEX SET (length=...) can't change a live index.
pub struct BloomState {
    pub hash_fn: Vec<types_fmgr::FmgrInfo>,
    pub collations: Vec<Oid>,
    pub opts: BloomOptions,
    pub ncolumns: usize,
    pub size_of_bloom_tuple: usize,
}

pub struct BloomScanOpaqueData {
    /// Built lazily on the first blgetbitmap call; reset by blrescan.
    pub sign: Option<Vec<BloomSignatureWord>>,
    pub state: BloomState,
}
