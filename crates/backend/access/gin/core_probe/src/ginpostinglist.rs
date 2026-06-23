//! `src/backend/access/gin/ginpostinglist.c` (PostgreSQL 18.3) — routines for
//! dealing with GIN posting lists (posting-list compression).
//!
//! The complete set of C functions ported 1:1: `itemptr_to_uint64` /
//! `uint64_to_itemptr`, `encode_varbyte` / `decode_varbyte`,
//! `ginCompressPostingList`, `ginPostingListDecode`,
//! `ginPostingListDecodeAllSegments`, `ginPostingListDecodeAllSegmentsToTbm`,
//! and `ginMergeItemPointers`; plus `ginCompareItemPointers` (gin_private.h:495),
//! the `(blkno << 32) | offnum` three-way comparison the merge/decode logic
//! depends on.
//!
//! # On-disk format
//!
//! A `GinPostingList` is the on-disk struct `{ ItemPointerData first; uint16
//! nbytes; uint8 bytes[]; }` (ginblock.h); `bytes` is a varbyte stream of the
//! deltas between successive (uint64-encoded) item pointers.
//! `SizeOfGinPostingList(plist) = offsetof(bytes) + SHORTALIGN(nbytes)`, so a
//! posting list always occupies an even number of bytes and a run of segments
//! is walked by stepping that distance. This port operates directly on the
//! on-disk `&[u8]` byte representation so the encoded form is byte-for-byte
//! identical to C and the segment walk matches `GinNextPostingListSegment`
//! exactly.
//!
//! `palloc`/`repalloc` become owned `Vec`s (`pfree` is the `Vec` drop). The
//! whole codec is pure byte math — the single genuine external is the
//! `tbm_add_tuples` inside [`ginPostingListDecodeAllSegmentsToTbm`]: the
//! `TIDBitmap` is owned by the `tidbitmap` subsystem, reached through the
//! `tbm_add_tuples` seam (the bitmap is the real `TIDBitmap` carrier).

use types_error::PgResult;
use types_tuple::heaptuple::ItemPointerData;

use nodes_core_seams::tbm_add_tuples;

/// `MaxHeapTuplesPerPageBits` (ginpostinglist.c:81) — bits used to encode the
/// offset-number portion of an item pointer.
const MAX_HEAP_TUPLES_PER_PAGE_BITS: u32 = 11;

/// `MaxBytesPerInteger` (ginpostinglist.c:84) — max bytes for the largest
/// supported (43-bit) integer in varbyte encoding.
const MAX_BYTES_PER_INTEGER: usize = 7;

/// `offsetof(GinPostingList, bytes)` == 8 (`ItemPointerData`(6) + `uint16`(2)).
const SIZE_OF_GIN_POSTING_LIST_HEADER: usize = 8;

/// `SHORTALIGN(LEN)` — round up to the next multiple of `ALIGNOF_SHORT` (2).
#[inline]
const fn shortalign(len: usize) -> usize {
    (len + (2 - 1)) & !(2 - 1)
}

/// `SHORTALIGN_DOWN(LEN)` — round down to a multiple of `ALIGNOF_SHORT` (2).
#[inline]
const fn shortalign_down(len: usize) -> usize {
    len & !(2 - 1)
}

/// `GinItemPointerGetBlockNumber(pointer)` (ginblock.h:144).
#[inline]
fn gin_item_pointer_get_block_number(iptr: &ItemPointerData) -> u32 {
    iptr.ip_blkid.block_number()
}

/// `GinItemPointerGetOffsetNumber(pointer)` (ginblock.h:147).
#[inline]
fn gin_item_pointer_get_offset_number(iptr: &ItemPointerData) -> u16 {
    iptr.ip_posid
}

/// `GinItemPointerSetBlockNumber(pointer, blkno)` (ginblock.h:150).
#[inline]
fn gin_item_pointer_set_block_number(iptr: &mut ItemPointerData, blkno: u32) {
    iptr.ip_blkid.set_block_number(blkno);
}

/// `GinItemPointerSetOffsetNumber(pointer, offnum)` (ginblock.h:153).
#[inline]
fn gin_item_pointer_set_offset_number(iptr: &mut ItemPointerData, offnum: u16) {
    iptr.ip_posid = offnum;
}

/// `pg_cmp_u64(a, b)` — the three-way unsigned comparison from `c.h`.
#[inline]
fn pg_cmp_u64(a: u64, b: u64) -> i32 {
    (a > b) as i32 - (a < b) as i32
}

/// `ginCompareItemPointers(a, b)` (gin_private.h:495) — compare two item
/// pointers as `(blkno << 32) | offnum`.
///
/// Note: this uses a *32-bit* offset shift (full block number in the high 32
/// bits), distinct from the *11-bit* packing used by [`itemptr_to_uint64`].
#[inline]
pub fn ginCompareItemPointers(a: &ItemPointerData, b: &ItemPointerData) -> i32 {
    let ia: u64 = ((gin_item_pointer_get_block_number(a) as u64) << 32)
        | gin_item_pointer_get_offset_number(a) as u64;
    let ib: u64 = ((gin_item_pointer_get_block_number(b) as u64) << 32)
        | gin_item_pointer_get_offset_number(b) as u64;
    pg_cmp_u64(ia, ib)
}

/// `itemptr_to_uint64(iptr)` (ginpostinglist.c:86) — pack an item pointer into a
/// 43-bit integer: `blkno << 11 | offnum`.
#[inline]
fn itemptr_to_uint64(iptr: &ItemPointerData) -> u64 {
    let mut val: u64 = gin_item_pointer_get_block_number(iptr) as u64;
    val <<= MAX_HEAP_TUPLES_PER_PAGE_BITS;
    val |= gin_item_pointer_get_offset_number(iptr) as u64;
    val
}

/// `uint64_to_itemptr(val, iptr)` (ginpostinglist.c:101) — unpack a 43-bit
/// integer back into an item pointer.
#[inline]
fn uint64_to_itemptr(mut val: u64, iptr: &mut ItemPointerData) {
    gin_item_pointer_set_offset_number(
        iptr,
        (val & ((1u64 << MAX_HEAP_TUPLES_PER_PAGE_BITS) - 1)) as u16,
    );
    val >>= MAX_HEAP_TUPLES_PER_PAGE_BITS;
    gin_item_pointer_set_block_number(iptr, val as u32);
}

/// `encode_varbyte(val, &ptr)` (ginpostinglist.c:114) — varbyte-encode `val`,
/// appending the bytes to `out` (the C `*ptr` cursor advance is the `Vec` push).
#[inline]
fn encode_varbyte(mut val: u64, out: &mut Vec<u8>) {
    while val > 0x7F {
        out.push((0x80 | (val & 0x7F)) as u8);
        val >>= 7;
    }
    out.push(val as u8);
}

/// `decode_varbyte(&ptr)` (ginpostinglist.c:132) — decode the varbyte-encoded
/// integer at `buf[*pos..]`, advancing `*pos` past it.
#[inline]
fn decode_varbyte(buf: &[u8], pos: &mut usize) -> u64 {
    let mut val: u64;
    let mut c: u64;

    // 1st byte
    c = buf[*pos] as u64;
    *pos += 1;
    val = c & 0x7F;
    if c & 0x80 != 0 {
        // 2nd byte
        c = buf[*pos] as u64;
        *pos += 1;
        val |= (c & 0x7F) << 7;
        if c & 0x80 != 0 {
            // 3rd byte
            c = buf[*pos] as u64;
            *pos += 1;
            val |= (c & 0x7F) << 14;
            if c & 0x80 != 0 {
                // 4th byte
                c = buf[*pos] as u64;
                *pos += 1;
                val |= (c & 0x7F) << 21;
                if c & 0x80 != 0 {
                    // 5th byte
                    c = buf[*pos] as u64;
                    *pos += 1;
                    val |= (c & 0x7F) << 28;
                    if c & 0x80 != 0 {
                        // 6th byte
                        c = buf[*pos] as u64;
                        *pos += 1;
                        val |= (c & 0x7F) << 35;
                        if c & 0x80 != 0 {
                            // 7th byte, should not have continuation bit
                            c = buf[*pos] as u64;
                            *pos += 1;
                            val |= c << 42;
                        }
                    }
                }
            }
        }
    }

    val
}

/// An owned, encoded posting-list segment, as produced by
/// [`ginCompressPostingList`].
///
/// `bytes` holds the exact on-disk image of one `GinPostingList`: the
/// [`ItemPointerData`] `first`, the `uint16` `nbytes`, and the
/// `SHORTALIGN(nbytes)` varbyte payload (the padding byte, if any, is zeroed).
/// Its length is `SizeOfGinPostingList`, so a run of these can be concatenated
/// and walked exactly as C walks `GinNextPostingListSegment`. This mirrors the
/// `palloc`'d `GinPostingList *` that `ginCompressPostingList` returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompressedPostingList {
    /// The on-disk bytes of the segment (length == `SizeOfGinPostingList`).
    pub bytes: Vec<u8>,
}

impl CompressedPostingList {
    /// `result->first` — the unpacked first item pointer of the segment.
    pub fn first(&self) -> ItemPointerData {
        read_first(&self.bytes)
    }

    /// `result->nbytes` — the number of varbyte payload bytes (before
    /// short-alignment).
    pub fn nbytes(&self) -> u16 {
        read_nbytes(&self.bytes)
    }

    /// `SizeOfGinPostingList(result)` — the short-aligned total on-disk size.
    pub fn size(&self) -> usize {
        self.bytes.len()
    }
}

/// Read the `first` item pointer out of an on-disk posting-list image.
#[inline]
fn read_first(buf: &[u8]) -> ItemPointerData {
    // first = { BlockIdData { bi_hi: u16, bi_lo: u16 }, ip_posid: u16 } at offset 0.
    let bi_hi = u16::from_ne_bytes([buf[0], buf[1]]);
    let bi_lo = u16::from_ne_bytes([buf[2], buf[3]]);
    let ip_posid = u16::from_ne_bytes([buf[4], buf[5]]);
    let mut iptr = ItemPointerData::new(0, ip_posid);
    iptr.ip_blkid.bi_hi = bi_hi;
    iptr.ip_blkid.bi_lo = bi_lo;
    iptr
}

/// Read the `nbytes` field out of an on-disk posting-list image.
#[inline]
fn read_nbytes(buf: &[u8]) -> u16 {
    u16::from_ne_bytes([buf[6], buf[7]])
}

/// Serialize the `first` item pointer into the on-disk header layout.
#[inline]
fn write_first(out: &mut Vec<u8>, first: &ItemPointerData) {
    out.extend_from_slice(&first.ip_blkid.bi_hi.to_ne_bytes());
    out.extend_from_slice(&first.ip_blkid.bi_lo.to_ne_bytes());
    out.extend_from_slice(&first.ip_posid.to_ne_bytes());
}

/// `ginCompressPostingList(ipd, nipd, maxsize, &nwritten)` (ginpostinglist.c:196)
///
/// Encode a posting list. The encoded list is at most `maxsize` bytes. The
/// number of items packed is returned in `nwritten`; if it is less than `nipd`,
/// only the first `nwritten` items fit in `maxsize`. `ipd` must be non-empty and
/// strictly increasing (C asserts `val > prev`).
pub fn ginCompressPostingList(
    ipd: &[ItemPointerData],
    nipd: i32,
    maxsize: i32,
    nwritten: Option<&mut i32>,
) -> CompressedPostingList {
    let maxsize = shortalign_down(maxsize as usize);

    let maxbytes = maxsize - SIZE_OF_GIN_POSTING_LIST_HEADER;
    debug_assert!(maxbytes > 0);

    // Store the first special item.
    let first = ipd[0];

    let mut prev = itemptr_to_uint64(&first);

    // ptr/endptr in C index into result->bytes; here `payload` accumulates the
    // varbyte stream and `endptr - ptr` is `maxbytes - payload.len()`.
    let mut payload: Vec<u8> = Vec::new();

    let mut totalpacked: i32 = 1;
    while totalpacked < nipd {
        let val = itemptr_to_uint64(&ipd[totalpacked as usize]);
        let delta = val.wrapping_sub(prev);

        debug_assert!(val > prev);

        let remaining = maxbytes - payload.len();
        if remaining >= MAX_BYTES_PER_INTEGER {
            encode_varbyte(delta, &mut payload);
        } else {
            // There are less than 7 bytes left. Have to check if the next item
            // fits in that space before writing it out.
            let mut buf: Vec<u8> = Vec::new();
            encode_varbyte(delta, &mut buf);
            if buf.len() > remaining {
                break; // output is full
            }
            payload.extend_from_slice(&buf);
        }
        prev = val;
        totalpacked += 1;
    }

    let nbytes = payload.len();
    debug_assert!(nbytes <= u16::MAX as usize);

    // Assemble the on-disk image: first (6) + nbytes (2) + SHORTALIGN(payload).
    let mut bytes = Vec::with_capacity(SIZE_OF_GIN_POSTING_LIST_HEADER + shortalign(nbytes));
    write_first(&mut bytes, &first);
    bytes.extend_from_slice(&(nbytes as u16).to_ne_bytes());
    bytes.extend_from_slice(&payload);

    // If we wrote an odd number of bytes, zero out the padding byte at the end.
    if nbytes != shortalign(nbytes) {
        bytes.push(0);
    }

    if let Some(nwritten) = nwritten {
        *nwritten = totalpacked;
    }

    debug_assert!(bytes.len() <= maxsize);

    CompressedPostingList { bytes }
}

/// `ginPostingListDecode(plist, &ndecoded)` (ginpostinglist.c:283)
///
/// Decode a single compressed posting-list segment into an array of item
/// pointers. `plist` is the on-disk image of exactly one segment.
pub fn ginPostingListDecode(plist: &[u8], ndecoded_out: Option<&mut i32>) -> Vec<ItemPointerData> {
    // SizeOfGinPostingList(plist) = offsetof(bytes) + SHORTALIGN(nbytes).
    let len = SIZE_OF_GIN_POSTING_LIST_HEADER + shortalign(read_nbytes(plist) as usize);
    ginPostingListDecodeAllSegments(plist, len as i32, ndecoded_out)
}

/// `ginPostingListDecodeAllSegments(segment, len, &ndecoded)`
/// (ginpostinglist.c:296)
///
/// Decode multiple posting-list segments, stored one after another with total
/// size `len` bytes, into an array of item pointers.
pub fn ginPostingListDecodeAllSegments(
    segment: &[u8],
    len: i32,
    ndecoded_out: Option<&mut i32>,
) -> Vec<ItemPointerData> {
    let endseg = len as usize;
    // C guesses an initial array size of `segment->nbytes * 2 + 1`, reading
    // `segment->nbytes` from the page region (always a non-empty buffer in C).
    // Here `segment` is a slice that is exactly empty when the leaf's posting
    // region holds no segments (`len == 0`, e.g. a vacuum-emptied leaf), so the
    // header read must be guarded — with no segments the result is empty and the
    // loop below never runs.
    let nallocated = if endseg < SIZE_OF_GIN_POSTING_LIST_HEADER {
        1
    } else {
        read_nbytes(&segment[0..]) as usize * 2 + 1
    };
    let mut result: Vec<ItemPointerData> = Vec::with_capacity(nallocated);

    let mut seg_off: usize = 0;
    while seg_off < endseg {
        let seg = &segment[seg_off..];
        let seg_nbytes = read_nbytes(seg) as usize;

        // Copy the first item.
        let first = read_first(seg);
        result.push(first);

        let mut val = itemptr_to_uint64(&first);
        // ptr/endptr index into seg->bytes (which starts at the header end).
        let bytes_start = SIZE_OF_GIN_POSTING_LIST_HEADER;
        let mut pos = bytes_start;
        let endptr = bytes_start + seg_nbytes;
        while pos < endptr {
            val = val.wrapping_add(decode_varbyte(seg, &mut pos));

            let mut iptr = ItemPointerData::default();
            uint64_to_itemptr(val, &mut iptr);
            result.push(iptr);
        }

        // segment = GinNextPostingListSegment(segment)
        seg_off += SIZE_OF_GIN_POSTING_LIST_HEADER + shortalign(seg_nbytes);
    }

    if let Some(ndecoded_out) = ndecoded_out {
        *ndecoded_out = result.len() as i32;
    }
    result
}

/// `ginPostingListDecodeAllSegmentsToTbm(ptr, len, tbm)` (ginpostinglist.c:357)
///
/// Add all item pointers from a bunch of posting lists to a `TIDBitmap`,
/// returning the number of items added. The `tbm_add_tuples` call is routed
/// through the `tbm_add_tuples` seam; `tbm` is the real `TIDBitmap` carrier
/// (C: `TIDBitmap *`) the caller owns.
pub fn ginPostingListDecodeAllSegmentsToTbm(
    ptr: &[u8],
    len: i32,
    tbm: &mut tidbitmap::TIDBitmap,
) -> PgResult<i32> {
    let mut ndecoded: i32 = 0;
    let items = ginPostingListDecodeAllSegments(ptr, len, Some(&mut ndecoded));
    tbm_add_tuples::call(tbm, &items, false)?;
    Ok(ndecoded)
}

/// `ginMergeItemPointers(a, na, b, nb, &nmerged)` (ginpostinglist.c:377)
///
/// Merge two ordered arrays of item pointers, eliminating duplicates. Returns
/// the merged array; `nmerged` is set to its length.
pub fn ginMergeItemPointers(
    a: &[ItemPointerData],
    na: u32,
    b: &[ItemPointerData],
    nb: u32,
    nmerged: &mut i32,
) -> Vec<ItemPointerData> {
    let na = na as usize;
    let nb = nb as usize;
    let mut dst: Vec<ItemPointerData> = Vec::with_capacity(na + nb);

    // If the argument arrays don't overlap, we can just append them to each
    // other.
    if na == 0 || nb == 0 || ginCompareItemPointers(&a[na - 1], &b[0]) < 0 {
        dst.extend_from_slice(&a[..na]);
        dst.extend_from_slice(&b[..nb]);
        *nmerged = (na + nb) as i32;
    } else if ginCompareItemPointers(&b[nb - 1], &a[0]) < 0 {
        dst.extend_from_slice(&b[..nb]);
        dst.extend_from_slice(&a[..na]);
        *nmerged = (na + nb) as i32;
    } else {
        let mut ai: usize = 0;
        let mut bi: usize = 0;

        while ai < na && bi < nb {
            let cmp = ginCompareItemPointers(&a[ai], &b[bi]);

            if cmp > 0 {
                dst.push(b[bi]);
                bi += 1;
            } else if cmp == 0 {
                // only keep one copy of the identical items
                dst.push(b[bi]);
                bi += 1;
                ai += 1;
            } else {
                dst.push(a[ai]);
                ai += 1;
            }
        }

        while ai < na {
            dst.push(a[ai]);
            ai += 1;
        }

        while bi < nb {
            dst.push(b[bi]);
            bi += 1;
        }

        *nmerged = dst.len() as i32;
    }

    dst
}
