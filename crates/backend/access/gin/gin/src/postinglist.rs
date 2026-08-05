//! ginpostinglist.c: varbyte-encoded posting lists. On-disk format —
//! byte-exact with C (43-bit item words, little-endian varbyte, SHORTALIGN'd
//! zero padding).

use ::gin_vocab::{
    gin_item_pointer_block, gin_item_pointer_offset, ginCompareItemPointers,
    size_of_gin_posting_list, SizeOfGinPostingListHeader, SHORTALIGN,
};
use ::mcx::{vec_append_bytes, Mcx, PgVec};
use ::types_error::PgResult;
use ::types_tuple::itemptr::{ItemPointerData, OffsetNumberIsValid};

use crate::vec_append;

const MaxHeapTuplesPerPageBits: u32 = 11;
const MaxBytesPerInteger: usize = 7;

#[inline]
pub(crate) fn itemptr_to_uint64(iptr: &ItemPointerData) -> u64 {
    debug_assert!(gin_item_pointer_offset(iptr) < (1 << MaxHeapTuplesPerPageBits));
    ((gin_item_pointer_block(iptr) as u64) << MaxHeapTuplesPerPageBits)
        | gin_item_pointer_offset(iptr) as u64
}

#[inline]
pub(crate) fn uint64_to_itemptr(val: u64) -> ItemPointerData {
    ItemPointerData::new(
        (val >> MaxHeapTuplesPerPageBits) as u32,
        (val & ((1 << MaxHeapTuplesPerPageBits) - 1)) as u16,
    )
}

#[inline]
fn encode_varbyte(mut val: u64, out: &mut [u8]) -> usize {
    let mut i = 0;
    while val > 0x7F {
        out[i] = 0x80 | (val & 0x7F) as u8;
        val >>= 7;
        i += 1;
    }
    out[i] = val as u8;
    i + 1
}

#[inline]
fn decode_varbyte(p: &[u8], pos: &mut usize) -> u64 {
    let mut val = 0u64;
    let mut shift = 0u32;
    loop {
        let c = p[*pos] as u64;
        *pos += 1;
        if shift == 42 {
            // 7th byte carries no continuation bit (43-bit words).
            debug_assert!(c & 0x80 == 0);
            return val | (c << 42);
        }
        val |= (c & 0x7F) << shift;
        if c & 0x80 == 0 {
            return val;
        }
        shift += 7;
    }
}


#[inline]
pub fn seg_first(seg: &[u8]) -> ItemPointerData {
    debug_assert!(seg.len() >= SizeOfGinPostingListHeader);
    // SAFETY: bounds asserted; ItemPointerData tolerates unaligned reads.
    unsafe { seg.as_ptr().cast::<ItemPointerData>().read_unaligned() }
}

#[inline]
pub fn seg_nbytes(seg: &[u8]) -> usize {
    u16::from_ne_bytes([seg[6], seg[7]]) as usize
}

#[inline]
pub fn seg_size(seg: &[u8]) -> usize {
    size_of_gin_posting_list(seg_nbytes(seg))
}

/// ginCompressPostingList: encode into a fresh short-aligned segment image of
/// at most SHORTALIGN_DOWN(maxsize) bytes; returns (image, nwritten).
pub(crate) fn ginCompressPostingList<'mcx>(
    mcx: Mcx<'mcx>,
    ipd: &[ItemPointerData],
    maxsize: usize,
) -> PgResult<(PgVec<'mcx, u8>, usize)> {
    let maxsize = maxsize & !1;
    let maxbytes = maxsize - SizeOfGinPostingListHeader;

    let mut out: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, maxsize)?;
    let first = ipd[0];
    vec_append_bytes(&mut out, &[0u8; SizeOfGinPostingListHeader])?;
    // SAFETY: 8-byte header written above.
    unsafe {
        out.as_mut_ptr().cast::<ItemPointerData>().write_unaligned(first);
    }

    let mut prev = itemptr_to_uint64(&first);
    let mut buf = [0u8; MaxBytesPerInteger];
    let mut nbytes = 0usize;
    let mut totalpacked = 1usize;
    while totalpacked < ipd.len() {
        let val = itemptr_to_uint64(&ipd[totalpacked]);
        debug_assert!(val > prev);
        let n = encode_varbyte(val - prev, &mut buf);
        if nbytes + n > maxbytes {
            break;
        }
        vec_append_bytes(&mut out, &buf[..n])?;
        nbytes += n;
        prev = val;
        totalpacked += 1;
    }
    out[6..8].copy_from_slice(&(nbytes as u16).to_ne_bytes());
    if nbytes != SHORTALIGN(nbytes) {
        vec_append_bytes(&mut out, &[0u8])?;
    }
    debug_assert!(out.len() <= maxsize && out.len() == size_of_gin_posting_list(nbytes));

    #[cfg(debug_assertions)]
    {
        let mut tmp = mcx::vec_new_in::<ItemPointerData>(mcx);
        ginPostingListDecodeAllSegments(&out, &mut tmp).unwrap();
        debug_assert!(tmp.len() == totalpacked);
        debug_assert!(tmp.as_slice() == &ipd[..totalpacked]);
    }

    Ok((out, totalpacked))
}

/// ginPostingListDecodeAllSegments: append every item of the consecutive
/// segments in `data` to `out`.
pub fn ginPostingListDecodeAllSegments(
    data: &[u8],
    out: &mut PgVec<'_, ItemPointerData>,
) -> PgResult<()> {
    let mcx = *out.allocator();
    let mut segoff = 0usize;
    while segoff < data.len() {
        let seg = &data[segoff..];
        let first = seg_first(seg);
        debug_assert!(OffsetNumberIsValid(gin_item_pointer_offset(&first)));
        debug_assert!(out.is_empty() || ginCompareItemPointers(&first, out.last().unwrap()) > 0);

        let nbytes = seg_nbytes(seg);
        out.try_reserve(nbytes + 1).map_err(|_| mcx.oom(nbytes + 1))?;
        out.push(first);

        let mut val = itemptr_to_uint64(&first);
        let payload = &seg[SizeOfGinPostingListHeader..SizeOfGinPostingListHeader + nbytes];
        let mut pos = 0usize;
        while pos < nbytes {
            val += decode_varbyte(payload, &mut pos);
            out.push(uint64_to_itemptr(val));
        }
        segoff += size_of_gin_posting_list(nbytes);
    }
    Ok(())
}

/// ginPostingListDecodeAllSegmentsToTbm.
pub(crate) fn ginPostingListDecodeAllSegmentsToTbm(
    mcx: Mcx<'_>,
    data: &[u8],
    tbm: &mut ::tidbitmap::TIDBitmap<'_>,
) -> PgResult<i64> {
    let mut items = mcx::vec_new_in::<ItemPointerData>(mcx);
    ginPostingListDecodeAllSegments(data, &mut items)?;
    tbm.add_tuples(items.as_slice(), false)?;
    Ok(items.len() as i64)
}

/// ginMergeItemPointers: merge two ordered TID arrays, dropping duplicates.
pub(crate) fn ginMergeItemPointers<'mcx>(
    mcx: Mcx<'mcx>,
    a: &[ItemPointerData],
    b: &[ItemPointerData],
) -> PgResult<PgVec<'mcx, ItemPointerData>> {
    let mut dst: PgVec<'mcx, ItemPointerData> =
        mcx::vec_with_capacity_in(mcx, a.len() + b.len())?;

    if a.is_empty() || b.is_empty() || ginCompareItemPointers(&a[a.len() - 1], &b[0]) < 0 {
        vec_append(&mut dst, a)?;
        vec_append(&mut dst, b)?;
    } else if ginCompareItemPointers(&b[b.len() - 1], &a[0]) < 0 {
        vec_append(&mut dst, b)?;
        vec_append(&mut dst, a)?;
    } else {
        let (mut ai, mut bi) = (0usize, 0usize);
        while ai < a.len() && bi < b.len() {
            let cmp = ginCompareItemPointers(&a[ai], &b[bi]);
            if cmp > 0 {
                dst.push(b[bi]);
                bi += 1;
            } else if cmp == 0 {
                dst.push(b[bi]);
                bi += 1;
                ai += 1;
            } else {
                dst.push(a[ai]);
                ai += 1;
            }
        }
        vec_append(&mut dst, &a[ai..])?;
        vec_append(&mut dst, &b[bi..])?;
    }
    Ok(dst)
}
