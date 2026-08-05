//! blscan.c: bloom index scan — amgetbitmap only (lossy; every hit rechecked).

use crate::state::{buf_page_bytes, init_bloom_state, sign_value};
use bufmgr::{GetAccessStrategy, LockBuffer, UnlockReleaseBuffer, BUFFER_LOCK_SHARE};
use mcx::Mcx;
use types_bloom::*;
use types_core::ForkNumber;
use types_error::PgResult;
use types_rel::Relation;
use types_relscan::{relation_get_index_scan, IndexScanDescData, IndexScanOpaque};
use types_scan::scankey::{ScanKeyData, SK_ISNULL};
use types_storage::buf::BufferAccessStrategyType;

pub fn blbeginscan<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDescData<'mcx>> {
    let so = BloomScanOpaqueData {
        sign: None,
        state: init_bloom_state(index)?,
    };
    relation_get_index_scan(
        mcx,
        index,
        nkeys,
        norderbys,
        IndexScanOpaque::Bloom(mcx::alloc_in(mcx, so)?),
        xact::TransactionStartedDuringRecovery(),
    )
}

/// Drops the computed signature; new keys arrive from the dispatcher.
pub fn blrescan(
    scan: &mut IndexScanDescData<'_>,
    keys: Option<&[ScanKeyData]>,
) -> PgResult<()> {
    let IndexScanOpaque::Bloom(so) = &mut scan.opaque else {
        unreachable!("blrescan on non-bloom opaque")
    };
    so.sign = None;

    if let Some(keys) = keys {
        if scan.numberOfKeys > 0 {
            for (dst, src) in scan.keyData.iter_mut().zip(keys.iter()) {
                *dst = src.clone();
            }
        }
    }
    Ok(())
}

pub fn blendscan(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    let IndexScanOpaque::Bloom(so) = &mut scan.opaque else {
        unreachable!("blendscan on non-bloom opaque")
    };
    so.sign = None;
    Ok(())
}

/// Whole-index sweep; every hit is added with recheck=true (lossy contract).
pub fn blgetbitmap(
    scan: &mut IndexScanDescData<'_>,
    tbm: &mut tidbitmap::TIDBitmap<'_>,
) -> PgResult<i64> {
    let index = scan.indexRelation.as_ref().expect("index open").alias();
    let nkeys = scan.numberOfKeys.max(0) as usize;

    let mut ntids: i64 = 0;

    {
        let (keys, so) = {
            let IndexScanOpaque::Bloom(so) = &mut scan.opaque else {
                unreachable!("blgetbitmap on non-bloom opaque")
            };
            (&scan.keyData, &mut **so)
        };
        if so.sign.is_none() {
            let mut sign = vec![0 as BloomSignatureWord; so.state.opts.bloom_length as usize];
            for skey in keys.iter().take(nkeys) {
                // Bloom-indexable operators are assumed strict: NULL matches nothing.
                if skey.sk_flags & SK_ISNULL != 0 {
                    so.sign = None;
                    return Ok(0);
                }
                sign_value(
                    &mut so.state,
                    &mut sign,
                    skey.sk_argument,
                    skey.sk_attno as usize - 1,
                )?;
            }
            so.sign = Some(sign);
        }
    }

    let bas = GetAccessStrategy(BufferAccessStrategyType::BasBulkread);
    let npages = bufmgr::RelationGetNumberOfBlocksInFork(&index, ForkNumber::MAIN_FORKNUM)?;
    if index.pgstat_enabled.get() {
        scan.xs_pgstat_index_scans += 1;
    }
    scan.xs_nsearches += 1;

    for blkno in BLOOM_HEAD_BLKNO..npages {
        let buffer = bufmgr::ReadBufferExtended(
            &index,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            types_storage::storage::ReadBufferMode::Normal,
            bas.clone(),
        )?;
        LockBuffer(buffer, BUFFER_LOCK_SHARE)?;
        let page = buf_page_bytes(buffer);

        if !page_is_new(page) && !page_is_deleted(page) {
            let IndexScanOpaque::Bloom(so) = &mut scan.opaque else {
                unreachable!()
            };
            let sign = so.sign.as_ref().expect("signature computed above");
            let size = so.state.size_of_bloom_tuple;
            let max_offset = opaque_maxoff(page);
            for offset in 1..=max_offset {
                let toff = tuple_off(size, offset);
                let tuple = &page[toff..toff + size];
                if signature_matches(&tuple[BLOOM_TUPLE_HDR_SZ..], sign) {
                    // heapPtr: 3 bare u16s (block hi, block lo, offset).
                    let hi = u16::from_ne_bytes([tuple[0], tuple[1]]) as u32;
                    let lo = u16::from_ne_bytes([tuple[2], tuple[3]]) as u32;
                    let off = u16::from_ne_bytes([tuple[4], tuple[5]]);
                    let tid = types_tuple::itemptr::ItemPointerData::new((hi << 16) | lo, off);
                    tbm.add_tuples(&[tid], true)?;
                    ntids += 1;
                }
            }
        }

        UnlockReleaseBuffer(buffer)?;
        postgres_seams::check_for_interrupts::call()?;
    }
    bufmgr::FreeAccessStrategy(bas);

    Ok(ntids)
}
