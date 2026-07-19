//! hashovfl.c: overflow-page and bitmap-page management, bucket squeeze.

use ::bufmgr_seams as bm;
use ::types_core::{BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, InvalidBuffer, OffsetNumber, BLCKSZ};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
use ::types_hash::*;
use ::types_rel::Relation;
use ::types_storage::buf::BufferAccessStrategy;
use ::types_storage::bufpage::{PageMut, PageRef, SizeOfPageHeaderData};
use ::xloginsert_seams::{XLogRegBuf, REGBUF_NO_CHANGE, REGBUF_NO_IMAGE, REGBUF_STANDARD, REGBUF_WILL_INIT};

use crate::insert::_hash_pgaddmultitup;
use crate::page::{
    page_mut, page_opaque, page_ref, with_meta, with_meta_mut, write_opaque,
    _hash_getbuf, _hash_getbuf_with_strategy, _hash_getinitbuf, _hash_getnewbuf,
    _hash_pageinit, _hash_relbuf,
};
use crate::{relation_needs_wal, RM_HASH};

const fn maxalign(sz: usize) -> usize {
    (sz + 7) & !7
}

fn bmpg_shift(bmshift: u16) -> u32 {
    bmshift as u32
}

fn bmpg_mask(bmshift: u16) -> u32 {
    (1u32 << bmpg_shift(bmshift)) - 1
}

/// bitno_to_blkno.
fn bitno_to_blkno(metap: &HashMetaPageData, ovflbitnum: u32) -> BlockNumber {
    let splitnum = metap.hashm_ovflpoint;
    let ovflbitnum = ovflbitnum + 1;
    let mut i = 1;
    while i < splitnum && ovflbitnum > metap.hashm_spares[i as usize] {
        i += 1;
    }
    _hash_get_totalbuckets(i) + ovflbitnum
}

/// _hash_ovflblkno_to_bitno.
pub fn _hash_ovflblkno_to_bitno(
    metap: &HashMetaPageData,
    ovflblkno: BlockNumber,
) -> PgResult<u32> {
    let splitnum = metap.hashm_ovflpoint;
    for i in 1..=splitnum {
        if ovflblkno <= _hash_get_totalbuckets(i) {
            break;
        }
        let bitnum = ovflblkno - _hash_get_totalbuckets(i);
        if bitnum > metap.hashm_spares[i as usize - 1] && bitnum <= metap.hashm_spares[i as usize] {
            return Ok(bitnum - 1);
        }
    }
    Err(Box::new(
        PgError::error(format!("invalid overflow block number {ovflblkno}"))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    ))
}

/// HashPageGetBitmap word read/write.
fn bitmap_word(page: &PageRef<'_>, j: usize) -> u32 {
    // SAFETY: bitmap words live in the page contents region (checked bitmap
    // page under the caller's lock).
    unsafe {
        page.as_ptr().add(SizeOfPageHeaderData).cast::<u32>().add(j).read()
    }
}

fn set_bitmap_bit(page: &mut PageMut<'_>, bit: u32, set: bool) {
    let j = (bit / BITS_PER_MAP) as usize;
    let mask = 1u32 << (bit % BITS_PER_MAP);
    // SAFETY: exclusive lock held; word within the bitmap region.
    unsafe {
        let p = page.as_ref().as_ptr().cast_mut().add(SizeOfPageHeaderData).cast::<u32>().add(j);
        let w = p.read();
        p.write(if set { w | mask } else { w & !mask });
    }
}

pub(crate) fn is_bitmap_bit_set(page: &PageRef<'_>, bit: u32) -> bool {
    bitmap_word(page, (bit / BITS_PER_MAP) as usize) & (1u32 << (bit % BITS_PER_MAP)) != 0
}

/// _hash_firstfreebit.
fn _hash_firstfreebit(map: u32) -> u32 {
    debug_assert!(map != ALL_SET, "firstfreebit found no free bit");
    (!map).trailing_zeros()
}

/// _hash_addovflpage: add an overflow page to the bucket whose last page is
/// `buf`. On entry `buf` is pinned, unlocked; returns the new overflow page
/// pinned and write-locked.
pub(crate) fn _hash_addovflpage(
    rel: &Relation<'_>,
    metabuf: Buffer,
    mut buf: Buffer,
    mut retain_pin: bool,
) -> PgResult<Buffer> {
    bm::lock_buffer::call(buf, bm::BUFFER_LOCK_EXCLUSIVE)?;
    crate::util::_hash_checkpage(rel, buf, LH_BUCKET_PAGE | LH_OVERFLOW_PAGE)?;

    // find the current tail page
    loop {
        // SAFETY: write lock held on buf.
        let opaque = page_opaque(&unsafe { page_ref(buf) });
        let nextblkno = opaque.hasho_nextblkno;
        if nextblkno == InvalidBlockNumber {
            break;
        }
        if retain_pin {
            debug_assert!(opaque.hasho_flag & LH_PAGE_TYPE == LH_BUCKET_PAGE);
            bm::lock_buffer::call(buf, bm::BUFFER_LOCK_UNLOCK)?;
        } else {
            _hash_relbuf(buf)?;
        }
        retain_pin = false;
        buf = _hash_getbuf(rel, nextblkno, HASH_WRITE, LH_OVERFLOW_PAGE)?;
    }

    bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_EXCLUSIVE)?;
    crate::util::_hash_checkpage(rel, metabuf, LH_META_PAGE)?;

    let orig_firstfree = with_meta(metabuf, |m| m.hashm_firstfree);
    let bmshift = with_meta(metabuf, |m| m.hashm_bmshift);
    let first_page = orig_firstfree >> bmpg_shift(bmshift);
    let mut bit = orig_firstfree & bmpg_mask(bmshift);
    let mut i = first_page;
    let mut j = (bit / BITS_PER_MAP) as usize;
    bit &= !(BITS_PER_MAP - 1);

    let mut mapbuf = InvalidBuffer;
    let mut newmapbuf = InvalidBuffer;
    let mut page_found = false;
    let mut bitmap_page_bit = 0u32;
    let mut splitnum;
    let mut last_bit;
    let mut ovflbuf = InvalidBuffer;

    'search: loop {
        // one iteration per bitmap page
        let (sn, spares_sn, nmaps, bmsize_bits) = with_meta(metabuf, |m| {
            (
                m.hashm_ovflpoint,
                m.hashm_spares[m.hashm_ovflpoint as usize],
                m.hashm_nmaps,
                (m.hashm_bmsize as u32) << BYTE_TO_BIT,
            )
        });
        splitnum = sn;
        let max_ovflpg = spares_sn - 1;
        let last_page = max_ovflpg >> bmpg_shift(bmshift);
        last_bit = max_ovflpg & bmpg_mask(bmshift);

        if i > last_page {
            break 'search;
        }

        debug_assert!(i < nmaps);
        let mapblkno = with_meta(metabuf, |m| m.hashm_mapp[i as usize]);

        let last_inpage = if i == last_page { last_bit } else { bmsize_bits - 1 };

        bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_UNLOCK)?;

        mapbuf = _hash_getbuf(rel, mapblkno, HASH_WRITE, LH_BITMAP_PAGE)?;

        while bit <= last_inpage {
            // SAFETY: write lock held on mapbuf.
            let word = bitmap_word(&unsafe { page_ref(mapbuf) }, j);
            if word != ALL_SET {
                page_found = true;
                bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_EXCLUSIVE)?;
                bit += _hash_firstfreebit(word);
                bitmap_page_bit = bit;
                bit += i << bmpg_shift(bmshift);
                let blkno = with_meta(metabuf, |m| bitno_to_blkno(m, bit));
                ovflbuf = _hash_getinitbuf(rel, blkno)?;
                break 'search;
            }
            j += 1;
            bit += BITS_PER_MAP;
        }

        _hash_relbuf(mapbuf)?;
        mapbuf = InvalidBuffer;
        i += 1;
        j = 0;
        bit = 0;

        bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_EXCLUSIVE)?;
    }

    if !page_found {
        // No free pages: extend the relation, possibly with a new bitmap page.
        let bmsize_bits = with_meta(metabuf, |m| (m.hashm_bmsize as u32) << BYTE_TO_BIT);
        if last_bit == bmsize_bits - 1 {
            let newbit = with_meta(metabuf, |m| m.hashm_spares[splitnum as usize]);
            with_meta(metabuf, |m| -> PgResult<()> {
                if m.hashm_nmaps as usize >= HASH_MAX_BITMAPS {
                    return Err(Box::new(
                        PgError::error(format!(
                            "out of overflow pages in hash index \"{}\"",
                            rel.name()
                        ))
                        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    ));
                }
                Ok(())
            })?;
            let bmblkno = with_meta(metabuf, |m| bitno_to_blkno(m, newbit));
            newmapbuf = _hash_getnewbuf(rel, bmblkno, ForkNumber::MAIN_FORKNUM)?;
        }

        let bit2 = with_meta(metabuf, |m| {
            if newmapbuf != InvalidBuffer {
                m.hashm_spares[splitnum as usize] + 1
            } else {
                m.hashm_spares[splitnum as usize]
            }
        });
        bit = bit2;
        let blkno = with_meta(metabuf, |m| bitno_to_blkno(m, bit));
        ovflbuf = _hash_getnewbuf(rel, blkno, ForkNumber::MAIN_FORKNUM)?;
    }
    debug_assert!(ovflbuf != InvalidBuffer);

    // Update phase (C's critical section).
    let bmsize = with_meta(metabuf, |m| m.hashm_bmsize);
    if page_found {
        debug_assert!(mapbuf != InvalidBuffer);
        // SAFETY: write lock held on mapbuf.
        set_bitmap_bit(&mut unsafe { page_mut(mapbuf) }, bitmap_page_bit, true);
        bm::mark_buffer_dirty::call(mapbuf)?;
    } else {
        with_meta_mut(metabuf, |m| {
            m.hashm_spares[splitnum as usize] += 1;
            if newmapbuf != InvalidBuffer {
                m.hashm_mapp[m.hashm_nmaps as usize] = bm::buffer_get_block_number::call(newmapbuf);
                m.hashm_nmaps += 1;
                m.hashm_spares[splitnum as usize] += 1;
            }
        });
        if newmapbuf != InvalidBuffer {
            _hash_initbitmapbuffer(newmapbuf, bmsize, false);
            bm::mark_buffer_dirty::call(newmapbuf)?;
        }
        bm::mark_buffer_dirty::call(metabuf)?;
    }

    let firstfree_updated = with_meta_mut(metabuf, |m| {
        if m.hashm_firstfree == orig_firstfree {
            m.hashm_firstfree = bit + 1;
            true
        } else {
            false
        }
    });
    if firstfree_updated {
        bm::mark_buffer_dirty::call(metabuf)?;
    }

    let bucket;
    {
        // SAFETY: write lock held on buf (tail page).
        let pageopaque = page_opaque(&unsafe { page_ref(buf) });
        bucket = pageopaque.hasho_bucket;
        // SAFETY: ovflbuf freshly initialized, write-locked.
        let mut ovflpage = unsafe { page_mut(ovflbuf) };
        write_opaque(
            &mut ovflpage,
            &HashPageOpaqueData {
                hasho_prevblkno: bm::buffer_get_block_number::call(buf),
                hasho_nextblkno: InvalidBlockNumber,
                hasho_bucket: bucket,
                hasho_flag: LH_OVERFLOW_PAGE,
                hasho_page_id: HASHO_PAGE_ID,
            },
        );
    }
    bm::mark_buffer_dirty::call(ovflbuf)?;

    {
        // SAFETY: write lock held on buf.
        let mut page = unsafe { page_mut(buf) };
        let mut pageopaque = page_opaque(&page.as_ref());
        pageopaque.hasho_nextblkno = bm::buffer_get_block_number::call(ovflbuf);
        write_opaque(&mut page, &pageopaque);
    }
    bm::mark_buffer_dirty::call(buf)?;

    if relation_needs_wal(rel) {
        let xlrec = crate::wal::xl_hash_add_ovfl_page(bmsize, page_found);
        let bucket_bytes = bucket.to_ne_bytes();
        let firstfree = with_meta(metabuf, |m| m.hashm_firstfree);
        let firstfree_bytes = firstfree.to_ne_bytes();
        let bitmap_bit_bytes = bitmap_page_bit.to_ne_bytes();

        let bucket_data: [&[u8]; 1] = [&bucket_bytes];
        let bitmap_data: [&[u8]; 1] = [&bitmap_bit_bytes];
        let firstfree_data: [&[u8]; 1] = [&firstfree_bytes];
        let mut bufs: Vec<XLogRegBuf<'_>> = Vec::with_capacity(5);
        bufs.push(XLogRegBuf {
            block_id: 0,
            buffer: ovflbuf,
            flags: REGBUF_WILL_INIT,
            bufdata: &bucket_data,
        });
        bufs.push(XLogRegBuf { block_id: 1, buffer: buf, flags: REGBUF_STANDARD, bufdata: &[] });
        if mapbuf != InvalidBuffer {
            bufs.push(XLogRegBuf {
                block_id: 2,
                buffer: mapbuf,
                flags: REGBUF_STANDARD,
                bufdata: &bitmap_data,
            });
        }
        if newmapbuf != InvalidBuffer {
            bufs.push(XLogRegBuf { block_id: 3, buffer: newmapbuf, flags: REGBUF_WILL_INIT, bufdata: &[] });
        }
        bufs.push(XLogRegBuf {
            block_id: 4,
            buffer: metabuf,
            flags: REGBUF_STANDARD,
            bufdata: &firstfree_data,
        });

        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            RM_HASH,
            XLOG_HASH_ADD_OVFL_PAGE,
            0,
            &[&xlrec],
            &bufs,
        )?;

        // SAFETY: pins + exclusive locks held on each registered buffer.
        unsafe {
            page_mut(ovflbuf).set_lsn(recptr);
            page_mut(buf).set_lsn(recptr);
            if mapbuf != InvalidBuffer {
                page_mut(mapbuf).set_lsn(recptr);
            }
            if newmapbuf != InvalidBuffer {
                page_mut(newmapbuf).set_lsn(recptr);
            }
            page_mut(metabuf).set_lsn(recptr);
        }
    }

    if retain_pin {
        bm::lock_buffer::call(buf, bm::BUFFER_LOCK_UNLOCK)?;
    } else {
        _hash_relbuf(buf)?;
    }

    if mapbuf != InvalidBuffer {
        _hash_relbuf(mapbuf)?;
    }

    bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_UNLOCK)?;

    if newmapbuf != InvalidBuffer {
        _hash_relbuf(newmapbuf)?;
    }

    Ok(ovflbuf)
}

/// _hash_freeovflpage: unlink `ovflbuf` from its bucket chain and mark it
/// free; adds `itups` (slices into `storage`) to `wbuf` in the same WAL-atomic
/// action. Returns the next block in the chain.
pub(crate) fn _hash_freeovflpage(
    rel: &Relation<'_>,
    bucketbuf: Buffer,
    ovflbuf: Buffer,
    wbuf: Buffer,
    storage: &[u8],
    itups: &[(usize, usize)],
    itup_offsets: &mut [OffsetNumber],
    bstrategy: BufferAccessStrategy,
) -> PgResult<BlockNumber> {
    crate::util::_hash_checkpage(rel, ovflbuf, LH_OVERFLOW_PAGE)?;
    let ovflblkno = bm::buffer_get_block_number::call(ovflbuf);
    let (nextblkno, prevblkno, _bucket) = {
        // SAFETY: write lock held on ovflbuf.
        let o = page_opaque(&unsafe { page_ref(ovflbuf) });
        (o.hasho_nextblkno, o.hasho_prevblkno, o.hasho_bucket)
    };
    let writeblkno = bm::buffer_get_block_number::call(wbuf);

    let mut prevbuf = InvalidBuffer;
    let mut nextbuf = InvalidBuffer;
    if prevblkno != InvalidBlockNumber {
        if prevblkno == writeblkno {
            prevbuf = wbuf;
        } else {
            prevbuf = _hash_getbuf_with_strategy(
                rel,
                prevblkno,
                HASH_WRITE,
                LH_BUCKET_PAGE | LH_OVERFLOW_PAGE,
                bstrategy.clone(),
            )?;
        }
    }
    if nextblkno != InvalidBlockNumber {
        nextbuf = _hash_getbuf_with_strategy(
            rel,
            nextblkno,
            HASH_WRITE,
            LH_OVERFLOW_PAGE,
            bstrategy,
        )?;
    }

    let metabuf = _hash_getbuf(rel, HASH_METAPAGE, HASH_READ, LH_META_PAGE)?;
    let ovflbitno = with_meta(metabuf, |m| _hash_ovflblkno_to_bitno(m, ovflblkno))?;
    let bmshift = with_meta(metabuf, |m| m.hashm_bmshift);
    let bitmappage = ovflbitno >> bmpg_shift(bmshift);
    let bitmapbit = ovflbitno & bmpg_mask(bmshift);

    let (nmaps, blkno) = with_meta(metabuf, |m| {
        (m.hashm_nmaps, m.hashm_mapp.get(bitmappage as usize).copied().unwrap_or(0))
    });
    if bitmappage >= nmaps {
        panic!("invalid overflow bit number {ovflbitno}");
    }

    bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_UNLOCK)?;

    let mapbuf = _hash_getbuf(rel, blkno, HASH_WRITE, LH_BITMAP_PAGE)?;
    // SAFETY: write lock held on mapbuf.
    debug_assert!(is_bitmap_bit_set(&unsafe { page_ref(mapbuf) }, bitmapbit));

    bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_EXCLUSIVE)?;

    if !itups.is_empty() {
        _hash_pgaddmultitup(rel, wbuf, storage, itups, itup_offsets)?;
        bm::mark_buffer_dirty::call(wbuf)?;
    }

    {
        // SAFETY: write lock held on ovflbuf; reinitialize the freed page.
        let mut ovflpage = unsafe { page_mut(ovflbuf) };
        _hash_pageinit(&mut ovflpage);
        write_opaque(
            &mut ovflpage,
            &HashPageOpaqueData {
                hasho_prevblkno: InvalidBlockNumber,
                hasho_nextblkno: InvalidBlockNumber,
                hasho_bucket: InvalidBucket,
                hasho_flag: LH_UNUSED_PAGE,
                hasho_page_id: HASHO_PAGE_ID,
            },
        );
    }
    bm::mark_buffer_dirty::call(ovflbuf)?;

    if prevbuf != InvalidBuffer {
        // SAFETY: write lock held (wbuf or fetched above).
        let mut prevpage = unsafe { page_mut(prevbuf) };
        let mut prevopaque = page_opaque(&prevpage.as_ref());
        prevopaque.hasho_nextblkno = nextblkno;
        write_opaque(&mut prevpage, &prevopaque);
        bm::mark_buffer_dirty::call(prevbuf)?;
    }
    if nextbuf != InvalidBuffer {
        // SAFETY: write lock held.
        let mut nextpage = unsafe { page_mut(nextbuf) };
        let mut nextopaque = page_opaque(&nextpage.as_ref());
        nextopaque.hasho_prevblkno = prevblkno;
        write_opaque(&mut nextpage, &nextopaque);
        bm::mark_buffer_dirty::call(nextbuf)?;
    }

    // SAFETY: write lock held on mapbuf.
    set_bitmap_bit(&mut unsafe { page_mut(mapbuf) }, bitmapbit, false);
    bm::mark_buffer_dirty::call(mapbuf)?;

    let update_metap = with_meta_mut(metabuf, |m| {
        if ovflbitno < m.hashm_firstfree {
            m.hashm_firstfree = ovflbitno;
            true
        } else {
            false
        }
    });
    if update_metap {
        bm::mark_buffer_dirty::call(metabuf)?;
    }

    if relation_needs_wal(rel) {
        let is_prim_bucket_same_wrt = wbuf == bucketbuf;
        let is_prev_bucket_same_wrt = wbuf == prevbuf;
        let xlrec = crate::wal::xl_hash_squeeze_page(
            prevblkno,
            nextblkno,
            itups.len() as u16,
            is_prim_bucket_same_wrt,
            is_prev_bucket_same_wrt,
        );
        let mut mod_wbuf = false;

        let mut offbytes: Vec<u8> = Vec::with_capacity(itups.len() * 2);
        for &off in &itup_offsets[..itups.len()] {
            offbytes.extend_from_slice(&off.to_ne_bytes());
        }
        let mut wbufdata: Vec<&[u8]> = Vec::with_capacity(1 + itups.len());
        let bitmapbit_bytes = bitmapbit.to_ne_bytes();
        let firstfree = with_meta(metabuf, |m| m.hashm_firstfree);
        let firstfree_bytes = firstfree.to_ne_bytes();

        let mut bufs: Vec<XLogRegBuf<'_>> = Vec::with_capacity(7);
        if !is_prim_bucket_same_wrt {
            bufs.push(XLogRegBuf {
                block_id: 0,
                buffer: bucketbuf,
                flags: REGBUF_STANDARD | REGBUF_NO_IMAGE | REGBUF_NO_CHANGE,
                bufdata: &[],
            });
        }
        if !itups.is_empty() {
            wbufdata.push(&offbytes);
            for &(start, len) in itups {
                wbufdata.push(&storage[start..start + len]);
            }
            mod_wbuf = true;
            bufs.push(XLogRegBuf {
                block_id: 1,
                buffer: wbuf,
                flags: REGBUF_STANDARD,
                bufdata: &wbufdata,
            });
        } else if is_prim_bucket_same_wrt || is_prev_bucket_same_wrt {
            let mut wbuf_flags = REGBUF_STANDARD;
            if !is_prev_bucket_same_wrt {
                wbuf_flags |= REGBUF_NO_CHANGE;
            } else {
                mod_wbuf = true;
            }
            bufs.push(XLogRegBuf { block_id: 1, buffer: wbuf, flags: wbuf_flags, bufdata: &[] });
        }
        bufs.push(XLogRegBuf { block_id: 2, buffer: ovflbuf, flags: REGBUF_STANDARD, bufdata: &[] });
        if prevbuf != InvalidBuffer && !is_prev_bucket_same_wrt {
            bufs.push(XLogRegBuf { block_id: 3, buffer: prevbuf, flags: REGBUF_STANDARD, bufdata: &[] });
        }
        if nextbuf != InvalidBuffer {
            bufs.push(XLogRegBuf { block_id: 4, buffer: nextbuf, flags: REGBUF_STANDARD, bufdata: &[] });
        }
        let bitmapbit_data: [&[u8]; 1] = [&bitmapbit_bytes];
        let firstfree_data: [&[u8]; 1] = [&firstfree_bytes];
        bufs.push(XLogRegBuf {
            block_id: 5,
            buffer: mapbuf,
            flags: REGBUF_STANDARD,
            bufdata: &bitmapbit_data,
        });
        if update_metap {
            bufs.push(XLogRegBuf {
                block_id: 6,
                buffer: metabuf,
                flags: REGBUF_STANDARD,
                bufdata: &firstfree_data,
            });
        }

        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            RM_HASH,
            XLOG_HASH_SQUEEZE_PAGE,
            0,
            &[&xlrec],
            &bufs,
        )?;

        // SAFETY: pins + exclusive locks held on each modified buffer.
        unsafe {
            if mod_wbuf {
                page_mut(wbuf).set_lsn(recptr);
            }
            page_mut(ovflbuf).set_lsn(recptr);
            if prevbuf != InvalidBuffer && !is_prev_bucket_same_wrt {
                page_mut(prevbuf).set_lsn(recptr);
            }
            if nextbuf != InvalidBuffer {
                page_mut(nextbuf).set_lsn(recptr);
            }
            page_mut(mapbuf).set_lsn(recptr);
            if update_metap {
                page_mut(metabuf).set_lsn(recptr);
            }
        }
    }

    if prevbuf != InvalidBuffer && prevblkno != writeblkno {
        _hash_relbuf(prevbuf)?;
    }
    _hash_relbuf(ovflbuf)?;
    if nextbuf != InvalidBuffer {
        _hash_relbuf(nextbuf)?;
    }
    _hash_relbuf(mapbuf)?;
    _hash_relbuf(metabuf)?;

    Ok(nextblkno)
}

/// _hash_initbitmapbuffer: fresh bitmap page, all bits "in use".
pub(crate) fn _hash_initbitmapbuffer(buf: Buffer, bmsize: u16, initpage: bool) {
    // SAFETY: caller holds pin + exclusive lock.
    let mut page = unsafe { page_mut(buf) };
    if initpage {
        _hash_pageinit(&mut page);
    }
    write_opaque(
        &mut page,
        &HashPageOpaqueData {
            hasho_prevblkno: InvalidBlockNumber,
            hasho_nextblkno: InvalidBlockNumber,
            hasho_bucket: InvalidBucket,
            hasho_flag: LH_BITMAP_PAGE,
            hasho_page_id: HASHO_PAGE_ID,
        },
    );
    // SAFETY: bitmap region [contents, contents+bmsize) is in-page.
    unsafe {
        core::ptr::write_bytes(
            page.as_ref().as_ptr().cast_mut().add(SizeOfPageHeaderData),
            0xFF,
            bmsize as usize,
        );
    }
    page.set_pd_lower((SizeOfPageHeaderData + bmsize as usize) as u16);
}

/// _hash_squeezebucket. Caller holds cleanup lock on the primary page.
pub(crate) fn _hash_squeezebucket(
    rel: &Relation<'_>,
    bucket: Bucket,
    bucket_blkno: BlockNumber,
    bucket_buf: Buffer,
    bstrategy: BufferAccessStrategy,
) -> PgResult<()> {
    let mut wblkno = bucket_blkno;
    let mut wbuf = bucket_buf;

    // SAFETY: cleanup lock held on the primary page.
    let mut wopaque = page_opaque(&unsafe { page_ref(wbuf) });

    if wopaque.hasho_nextblkno == InvalidBlockNumber {
        bm::lock_buffer::call(wbuf, bm::BUFFER_LOCK_UNLOCK)?;
        return Ok(());
    }

    // find the last page in the bucket chain
    let mut rbuf = InvalidBuffer;
    let mut ropaque = wopaque;
    let mut rblkno;
    loop {
        rblkno = ropaque.hasho_nextblkno;
        if rbuf != InvalidBuffer {
            _hash_relbuf(rbuf)?;
        }
        rbuf = _hash_getbuf_with_strategy(
            rel,
            rblkno,
            HASH_WRITE,
            LH_OVERFLOW_PAGE,
            bstrategy.clone(),
        )?;
        // SAFETY: write lock acquired above.
        ropaque = page_opaque(&unsafe { page_ref(rbuf) });
        debug_assert!(ropaque.hasho_bucket == bucket);
        if ropaque.hasho_nextblkno == InvalidBlockNumber {
            break;
        }
    }

    let mut tup_storage: Vec<u8> = Vec::with_capacity(BLCKSZ);
    let mut itups: Vec<(usize, usize)> = Vec::with_capacity(MaxIndexTuplesPerPage);
    let mut itup_offsets = [0 as OffsetNumber; MaxIndexTuplesPerPage];
    let mut deletable = [0 as OffsetNumber; MaxIndexTuplesPerPage];

    loop {
        let mut ndeletable = 0usize;
        itups.clear();
        tup_storage.clear();
        let mut all_tups_size = 0usize;
        let mut retain_pin = false;

        'readpage: loop {
            // SAFETY: write lock held on rbuf.
            let maxroffnum = unsafe { page_ref(rbuf) }.max_offset_number();
            let mut roffnum: OffsetNumber = 1;
            while roffnum <= maxroffnum {
                // SAFETY: bounded offset under the rbuf lock.
                let (is_dead, itup_ptr, itup_len) = unsafe {
                    let rpage = page_ref(rbuf);
                    let id = rpage.item_id(roffnum);
                    let (p, l) = rpage.item_raw(id);
                    (id.is_dead(), p, l as usize)
                };
                if is_dead {
                    roffnum += 1;
                    continue;
                }
                let itemsz = maxalign(itup_len);

                // walk up the chain while the accumulated batch won't fit
                loop {
                    let fits = {
                        // SAFETY: write lock held on wbuf.
                        let wpage = unsafe { page_ref(wbuf) };
                        crate::page::page_get_free_space_for_multiple_tuples(
                            &wpage,
                            itups.len() + 1,
                        ) >= all_tups_size + itemsz
                    };
                    if fits {
                        break;
                    }

                    let mut next_wbuf = InvalidBuffer;
                    let mut tups_moved = false;

                    if wblkno == bucket_blkno {
                        retain_pin = true;
                    }
                    wblkno = wopaque.hasho_nextblkno;
                    debug_assert!(wblkno != InvalidBlockNumber);

                    if wblkno != rblkno {
                        next_wbuf = _hash_getbuf_with_strategy(
                            rel,
                            wblkno,
                            HASH_WRITE,
                            LH_OVERFLOW_PAGE,
                            bstrategy.clone(),
                        )?;
                    }

                    if !itups.is_empty() {
                        debug_assert!(itups.len() == ndeletable);

                        _hash_pgaddmultitup(rel, wbuf, &tup_storage, &itups, &mut itup_offsets)?;
                        bm::mark_buffer_dirty::call(wbuf)?;

                        {
                            // SAFETY: write lock held on rbuf.
                            let mut rpage = unsafe { page_mut(rbuf) };
                            rpage.index_multi_delete(&deletable[..ndeletable]);
                        }
                        bm::mark_buffer_dirty::call(rbuf)?;

                        if relation_needs_wal(rel) {
                            let is_prim = wbuf == bucket_buf;
                            let xlrec =
                                crate::wal::xl_hash_move_page_contents(itups.len() as u16, is_prim);

                            let mut offbytes: Vec<u8> = Vec::with_capacity(itups.len() * 2);
                            for &off in &itup_offsets[..itups.len()] {
                                offbytes.extend_from_slice(&off.to_ne_bytes());
                            }
                            let mut wbufdata: Vec<&[u8]> = Vec::with_capacity(1 + itups.len());
                            wbufdata.push(&offbytes);
                            for &(start, len) in &itups {
                                wbufdata.push(&tup_storage[start..start + len]);
                            }
                            let mut delbytes: Vec<u8> = Vec::with_capacity(ndeletable * 2);
                            for &off in &deletable[..ndeletable] {
                                delbytes.extend_from_slice(&off.to_ne_bytes());
                            }

                            let del_data: [&[u8]; 1] = [&delbytes];
                            let mut bufs: Vec<XLogRegBuf<'_>> = Vec::with_capacity(3);
                            if !is_prim {
                                bufs.push(XLogRegBuf {
                                    block_id: 0,
                                    buffer: bucket_buf,
                                    flags: REGBUF_STANDARD | REGBUF_NO_IMAGE | REGBUF_NO_CHANGE,
                                    bufdata: &[],
                                });
                            }
                            bufs.push(XLogRegBuf {
                                block_id: 1,
                                buffer: wbuf,
                                flags: REGBUF_STANDARD,
                                bufdata: &wbufdata,
                            });
                            bufs.push(XLogRegBuf {
                                block_id: 2,
                                buffer: rbuf,
                                flags: REGBUF_STANDARD,
                                bufdata: &del_data,
                            });

                            let recptr = ::xloginsert_seams::xlog_insert_record::call(
                                RM_HASH,
                                XLOG_HASH_MOVE_PAGE_CONTENTS,
                                0,
                                &[&xlrec],
                                &bufs,
                            )?;

                            // SAFETY: pins + exclusive locks held.
                            unsafe {
                                page_mut(wbuf).set_lsn(recptr);
                                page_mut(rbuf).set_lsn(recptr);
                            }
                        }

                        tups_moved = true;
                    }

                    if retain_pin {
                        bm::lock_buffer::call(wbuf, bm::BUFFER_LOCK_UNLOCK)?;
                    } else {
                        _hash_relbuf(wbuf)?;
                    }

                    if rblkno == wblkno {
                        _hash_relbuf(rbuf)?;
                        return Ok(());
                    }

                    wbuf = next_wbuf;
                    // SAFETY: write lock acquired when next_wbuf was fetched.
                    wopaque = page_opaque(&unsafe { page_ref(wbuf) });
                    debug_assert!(wopaque.hasho_bucket == bucket);
                    retain_pin = false;

                    itups.clear();
                    tup_storage.clear();
                    all_tups_size = 0;
                    ndeletable = 0;

                    if tups_moved {
                        continue 'readpage;
                    }
                }

                deletable[ndeletable] = roffnum;
                ndeletable += 1;

                // WAL in _hash_freeovflpage needs owned copies (the source
                // page is freed in the same action).
                let start = tup_storage.len();
                // SAFETY: itup_len bytes live under the rbuf lock.
                tup_storage
                    .extend_from_slice(unsafe { core::slice::from_raw_parts(itup_ptr, itup_len) });
                itups.push((start, itup_len));
                all_tups_size += itemsz;
                roffnum += 1;
            }
            break;
        }

        // no live tuples remain on the read page: free it
        rblkno = ropaque.hasho_prevblkno;
        debug_assert!(rblkno != InvalidBlockNumber);

        _hash_freeovflpage(
            rel,
            bucket_buf,
            rbuf,
            wbuf,
            &tup_storage,
            &itups,
            &mut itup_offsets,
            bstrategy.clone(),
        )?;

        if rblkno == wblkno {
            if wblkno == bucket_blkno {
                bm::lock_buffer::call(wbuf, bm::BUFFER_LOCK_UNLOCK)?;
            } else {
                _hash_relbuf(wbuf)?;
            }
            return Ok(());
        }

        rbuf = _hash_getbuf_with_strategy(
            rel,
            rblkno,
            HASH_WRITE,
            LH_OVERFLOW_PAGE,
            bstrategy.clone(),
        )?;
        // SAFETY: write lock acquired above.
        ropaque = page_opaque(&unsafe { page_ref(rbuf) });
        debug_assert!(ropaque.hasho_bucket == bucket);
    }
}
