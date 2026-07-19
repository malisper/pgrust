//! blutils.c: BloomState, signValue, BloomFormTuple, page/buffer helpers.

use bufmgr::{
    ConditionalLockBuffer, LockBuffer, ReleaseBuffer, UnlockReleaseBuffer, BUFFER_LOCK_EXCLUSIVE,
    BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK,
};
use datum::Datum;
use generic_xlog::{GenericXLogFinish, GenericXLogStart};
use mcx::Mcx;
use types_bloom::*;
use types_core::{BlockNumber, Buffer, ForkNumber, BLCKSZ};
use types_error::{PgError, PgResult};
use types_rel::Relation;

const BLOOM_AMSUPPORT: usize = BLOOM_NPROC as usize; // rd_support row width

/// PageGetContents pointer; caller holds the needed content lock.
#[inline]
pub fn buf_page_bytes<'a>(buffer: Buffer) -> &'a [u8] {
    // SAFETY: caller holds at least a share lock on `buffer`.
    unsafe {
        core::slice::from_raw_parts(
            bufmgr_seams::buffer_get_page::call(buffer).as_ptr(),
            BLCKSZ,
        )
    }
}

#[inline]
pub fn buf_page_bytes_mut<'a>(buffer: Buffer) -> &'a mut [u8] {
    // SAFETY: caller holds the exclusive content lock on `buffer`.
    unsafe {
        core::slice::from_raw_parts_mut(
            bufmgr_seams::buffer_get_page::call(buffer).as_ptr(),
            BLCKSZ,
        )
    }
}

fn hash_proc_oid(index: &Relation<'_>, attno_0based: usize) -> types_core::Oid {
    index
        .rd_support
        .get(attno_0based * BLOOM_AMSUPPORT + (BLOOM_HASH_PROC as usize - 1))
        .copied()
        .unwrap_or(0)
}

/// initBloomState. C caches the metapage options in rd_amcache; this Relation
/// has no bloom amcache slot, so each construction re-reads the metapage's
/// frozen options (one shared-locked buffer read; behaviorally identical).
pub fn init_bloom_state(index: &Relation<'_>) -> PgResult<BloomState> {
    let ncolumns = index.rd_att.natts as usize;
    let mut hash_fn = Vec::with_capacity(ncolumns);
    let mut collations = Vec::with_capacity(ncolumns);
    for i in 0..ncolumns {
        let oid = hash_proc_oid(index, i);
        hash_fn.push(fmgr_core::fmgr_info(oid)?);
        collations.push(index.rd_indcollation.get(i).copied().unwrap_or(0));
    }

    let opts = read_metapage_options(index)?;
    let size_of_bloom_tuple = opts.size_of_bloom_tuple();

    Ok(BloomState {
        hash_fn,
        collations,
        opts,
        ncolumns,
        size_of_bloom_tuple,
    })
}

fn read_metapage_options(index: &Relation<'_>) -> PgResult<BloomOptions> {
    let buffer = bufmgr::ReadBuffer(index, BLOOM_METAPAGE_BLKNO)?;
    LockBuffer(buffer, BUFFER_LOCK_SHARE)?;
    let page = buf_page_bytes(buffer);
    if !page_is_meta(page) || meta_magick(page) != BLOOM_MAGICK_NUMBER {
        UnlockReleaseBuffer(buffer)?;
        return Err(PgError::error("Relation is not a bloom index").into());
    }
    let opts = meta_opts(page);
    UnlockReleaseBuffer(buffer)?;
    Ok(opts)
}

pub fn sign_value(
    state: &mut BloomState,
    sign: &mut [BloomSignatureWord],
    value: Datum,
    attno: usize,
) -> PgResult<()> {
    let collation = state.collations[attno];
    let hash_val = types_fmgr::function_call1_coll(&mut state.hash_fn[attno], collation, value)?.as_i32()
        as u32;
    add_value_bits(
        sign,
        attno,
        hash_val,
        state.opts.bit_size[attno],
        state.opts.bloom_length,
    );
    Ok(())
}

pub fn bloom_form_tuple(
    state: &mut BloomState,
    iptr: &types_tuple::itemptr::ItemPointerData,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<Vec<u8>> {
    let mut tuple = vec![0u8; state.size_of_bloom_tuple];
    let blk = types_tuple::itemptr::ItemPointerGetBlockNumberNoCheck(iptr);
    let off = types_tuple::itemptr::ItemPointerGetOffsetNumberNoCheck(iptr);
    tuple[0..2].copy_from_slice(&((blk >> 16) as u16).to_ne_bytes());
    tuple[2..4].copy_from_slice(&((blk & 0xFFFF) as u16).to_ne_bytes());
    tuple[4..6].copy_from_slice(&off.to_ne_bytes());

    let mut sign = vec![0u16; state.opts.bloom_length as usize];
    for i in 0..state.ncolumns {
        if isnull[i] {
            continue;
        }
        sign_value(state, &mut sign, values[i], i)?;
    }
    for (i, w) in sign.iter().enumerate() {
        tuple[BLOOM_TUPLE_HDR_SZ + 2 * i..BLOOM_TUPLE_HDR_SZ + 2 * i + 2]
            .copy_from_slice(&w.to_ne_bytes());
    }
    Ok(tuple)
}

/// Returns a pinned, exclusive-locked buffer; caller must bloom_init_page it.
pub fn bloom_new_buffer(index: &Relation<'_>) -> PgResult<Buffer> {
    loop {
        let blkno = freespace::GetFreeIndexPage(index)?;
        if blkno == types_core::InvalidBlockNumber {
            break;
        }
        let buffer = bufmgr::ReadBuffer(index, blkno)?;
        if ConditionalLockBuffer(buffer)? {
            let page = buf_page_bytes(buffer);
            if page_is_new(page) || page_is_deleted(page) {
                return Ok(buffer); // OK to use
            }
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK)?;
        }
        ReleaseBuffer(buffer)?;
    }
    let (buffer, extended_by) = bufmgr_seams::extend_buffered_rel_by::call(
        index,
        ForkNumber::MAIN_FORKNUM,
        None,
        bufmgr_seams::EB_LOCK_FIRST,
        1,
    )?;
    debug_assert_eq!(extended_by, 1);
    Ok(buffer)
}

pub fn bloom_init_metapage<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    forknum: ForkNumber,
) -> PgResult<()> {
    // C's ReadBufferExtended(P_NEW) + LockBuffer becomes
    // ExtendBufferedRel(EB_LOCK_FIRST): same new zeroed block 0, locked.
    let (meta_buffer, extended_by) = bufmgr_seams::extend_buffered_rel_by::call(
        index,
        forknum,
        None,
        bufmgr_seams::EB_LOCK_FIRST,
        1,
    )?;
    debug_assert_eq!(extended_by, 1);
    debug_assert_eq!(bufmgr::BufferGetBlockNumber(meta_buffer), BLOOM_METAPAGE_BLKNO);

    let opts = index_options_or_default(index);

    let mut state = GenericXLogStart(mcx, index)?;
    let page = state.register_buffer(meta_buffer, GENERIC_XLOG_FULL_IMAGE)?;
    bloom_init_page(page, BLOOM_META);
    fill_metapage(page, &opts);
    GenericXLogFinish(state)?;

    UnlockReleaseBuffer(meta_buffer)?;
    Ok(())
}

pub fn index_options_or_default(index: &Relation<'_>) -> BloomOptions {
    index
        .rd_options
        .as_ref()
        .and_then(|o| o.bloom())
        .unwrap_or_default()
}

pub const GENERIC_XLOG_FULL_IMAGE: i32 = generic_xlog::GENERIC_XLOG_FULL_IMAGE;
