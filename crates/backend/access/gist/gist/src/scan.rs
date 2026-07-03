//! gistscan.c: gistbeginscan / gistrescan / gistendscan.

use ::mcx::Mcx;
use ::types_core::{InvalidBlockNumber, Oid};
use ::types_error::PgResult;
use ::types_fmgr::FmgrInfo;
use ::types_gist::pairingheap::PairingHeap;
use ::types_gist::state::{gist_search_item_cmp, GISTScanOpaqueData};
use ::types_rel::Relation;
use ::types_relscan::{relation_get_index_scan, IndexScanDescData, IndexScanOpaque};
use ::types_scan::scankey::{ScanKeyData, SK_ISNULL, SK_SEARCHNOTNULL, SK_SEARCHNULL};

use crate::state::initGISTstate;

fn fmgr_info_copy(src: &FmgrInfo) -> FmgrInfo {
    FmgrInfo::new(
        src.fn_addr,
        src.fn_oid,
        src.fn_nargs,
        src.fn_strict,
        src.fn_retset,
    )
}

/// gistbeginscan.
pub fn gistbeginscan<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDescData<'mcx>> {
    if norderbys > 0 {
        panic!("unported: gist ordered (KNN) scans (distance/pairing-heap lane)");
    }

    let giststate = initGISTstate(r)?;
    let so = GISTScanOpaqueData {
        giststate,
        temp: ::mcx::MemoryContext::new("GiST temporary context"),
        queue: PairingHeap::new(
            gist_search_item_cmp
                as fn(
                    &::types_gist::state::GISTSearchItem,
                    &::types_gist::state::GISTSearchItem,
                ) -> i32,
        ),
        qual_ok: true,
        firstCall: true,
        killedItems: None,
        numKilled: 0,
        curBlkno: InvalidBlockNumber,
        curPageLSN: 0,
        pageData: Vec::new(),
        nPageData: 0,
        curPageData: 0,
        fetch_buf: Vec::new(),
    };

    let so = ::mcx::PgBox::new_in(so, mcx);
    let scan = relation_get_index_scan(
        mcx,
        r,
        nkeys,
        norderbys,
        IndexScanOpaque::Gist(so),
        xact::TransactionStartedDuringRecovery(),
    )?;
    Ok(scan)
}

/// gistrescan. `key: None` restarts with the keys already in scan.keyData.
pub fn gistrescan(
    scan: &mut IndexScanDescData<'_>,
    key: Option<&[ScanKeyData]>,
    orderbys: Option<&[ScanKeyData]>,
) -> PgResult<()> {
    if orderbys.is_some_and(|o| !o.is_empty()) || scan.numberOfOrderBys > 0 {
        panic!("unported: gist ordered (KNN) scans (distance/pairing-heap lane)");
    }

    let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
        crate::non_gist_opaque()
    };

    // queue reuse replaces C's scanCxt/queueCxt dance: reset + reuse slots.
    so.queue.reset();

    if scan.xs_want_itup && so.giststate.fetchTupdesc.is_none() {
        // C builds fetchTupdesc from rd_opcintype; the closed opclass set has
        // no opckeytype overrides live (amstorage lanes are loud), so the
        // index descriptor's types ARE the opcintype set.
        for (i, &opcintype) in scan.indexRelation.rd_opcintype.iter().enumerate() {
            let att = scan.indexRelation.rd_att.attr(i);
            if att.atttypid != opcintype {
                panic!(
                    "unported: gist fetchTupdesc with opckeytype storage \
                     (att {} type {} vs opcintype {opcintype})",
                    i + 1,
                    att.atttypid
                );
            }
        }
        so.giststate.fetchTupdesc = Some(scan.indexRelation.rd_att.clone());
        scan.xs_itupdesc = so.giststate.fetchTupdesc.clone();
    }

    so.firstCall = true;

    if let Some(keys) = key {
        if scan.numberOfKeys > 0 {
            debug_assert!(keys.len() == scan.numberOfKeys as usize);
            so.qual_ok = true;

            scan.keyData.clear();
            for (i, k) in keys.iter().enumerate() {
                let _ = i;
                let skey = ScanKeyData {
                    sk_flags: k.sk_flags,
                    sk_attno: k.sk_attno,
                    sk_strategy: k.sk_strategy,
                    sk_subtype: k.sk_subtype,
                    sk_collation: k.sk_collation,
                    sk_func: fmgr_info_copy(
                        &so.giststate.consistentFn[k.sk_attno as usize - 1],
                    ),
                    sk_argument: k.sk_argument,
                };
                if skey.sk_flags & SK_ISNULL != 0
                    && skey.sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL) == 0
                {
                    so.qual_ok = false;
                }
                scan.keyData.push(skey);
            }
        }
    } else {
        // restart: re-arm sk_func from consistentFn (fn_extra preserved).
        for skey in scan.keyData.iter_mut() {
            let attno = skey.sk_attno as usize;
            let src = &so.giststate.consistentFn[attno - 1];
            skey.sk_func.fn_addr = src.fn_addr;
            skey.sk_func.fn_oid = src.fn_oid;
            skey.sk_func.fn_nargs = src.fn_nargs;
            skey.sk_func.fn_strict = src.fn_strict;
            skey.sk_func.fn_retset = src.fn_retset;
        }
    }

    scan.xs_itup = None;
    Ok(())
}

/// gistendscan: state is dropped with the scan value.
pub fn gistendscan(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    let IndexScanOpaque::Gist(_so) = &mut scan.opaque else {
        crate::non_gist_opaque()
    };
    Ok(())
}

/// gistcanreturn.
pub fn gistcanreturn(index: &Relation<'_>, attno: i32) -> bool {
    if attno > index.indnkeyatts() {
        return true;
    }
    let att0 = (attno - 1) as usize;
    let fetch = crate::state::index_getprocid(index, att0, ::types_gist::GIST_FETCH_PROC);
    let compress = crate::state::index_getprocid(index, att0, ::types_gist::GIST_COMPRESS_PROC);
    const InvalidOid: Oid = 0;
    fetch != InvalidOid || compress == InvalidOid
}
