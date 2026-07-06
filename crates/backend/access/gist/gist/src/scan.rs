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
    let mut giststate = initGISTstate(mcx, r)?;
    // gistscan.c:180-203 (in C, lazily under gistrescan's xs_want_itup arm;
    // rescan here has no mcx, so the divergent-type descriptor is built at
    // beginscan — same contents, once per scan either way). Key columns get
    // rd_opcintype (opckeytype storage, e.g. point_ops' box leaves), included
    // columns keep the leaf descriptor's type.
    if (0..r.indnkeyatts() as usize).any(|i| r.rd_att.attr(i).atttypid != r.rd_opcintype[i]) {
        let natts = r.rd_att.natts;
        let nkeyatts = r.indnkeyatts() as usize;
        let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, natts)?;
        for attno in 1..=natts as usize {
            let typid = if attno <= nkeyatts {
                r.rd_opcintype[attno - 1]
            } else {
                r.rd_att.attr(attno - 1).atttypid
            };
            tupdesc::TupleDescInitEntry(&mut desc, attno as i16, None, typid, -1, 0)?;
        }
        giststate.fetchTupdesc = Some(std::rc::Rc::new(desc));
    }
    let so = GISTScanOpaqueData {
        giststate,
        temp: ::mcx::MemoryContext::new_bump("GiST temporary context"),
        queue: PairingHeap::new(
            gist_search_item_cmp
                as fn(
                    &::types_gist::state::GISTSearchItem,
                    &::types_gist::state::GISTSearchItem,
                ) -> i32,
        ),
        qual_ok: true,
        firstCall: true,
        // gistscan.c:99-107: distance workspace sized to norderbys.
        distances: vec![
            ::types_gist::state::IndexOrderByDistance::default();
            norderbys.max(0) as usize
        ],
        orderByTypes: Vec::new(),
        cur_recontup: None,
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
    let index_rel = scan.indexRelation.as_ref().expect("index scan parked (skeleton)").alias();
    let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
        crate::non_gist_opaque()
    };

    // queue reuse replaces C's scanCxt/queueCxt dance: reset + reuse slots.
    so.queue.reset();
    so.cur_recontup = None;

    if scan.xs_want_itup {
        // Storage types == opcintypes: the index descriptor IS C's built
        // descriptor; the divergent case was built at beginscan.
        if so.giststate.fetchTupdesc.is_none() {
            so.giststate.fetchTupdesc = Some(index_rel.rd_att.clone());
        }
        scan.xs_itupdesc = so.giststate.fetchTupdesc.clone();
    }

    so.firstCall = true;

    if let Some(keys) = key {
        if scan.numberOfKeys > 0 {
            debug_assert!(keys.len() == scan.numberOfKeys as usize);
            so.qual_ok = true;

            // C preserves sk_func.fn_extra across rescans (consistentFn memos).
            let mut fn_extras: Vec<Option<::types_fmgr::FnExtra>> = scan
                .keyData
                .iter_mut()
                .map(|k| k.sk_func.fn_extra.take())
                .collect();

            scan.keyData.clear();
            for (i, k) in keys.iter().enumerate() {
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
                let mut skey = skey;
                if let Some(extra) = fn_extras.get_mut(i).and_then(|e| e.take()) {
                    skey.sk_func.fn_extra = Some(extra);
                }
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

    // gistscan.c:279-341: swap the ordering operator's fn for the opclass
    // Distance method; sk_strategy/sk_subtype carry the operator identity.
    // orderByTypes records the ordering operator's result type before the
    // swap — getNextNearest converts the float8 distances to it.
    if let Some(orderbys) = orderbys.filter(|_| scan.numberOfOrderBys > 0) {
        debug_assert!(orderbys.len() == scan.numberOfOrderBys as usize);
        let mut fn_extras: Vec<Option<::types_fmgr::FnExtra>> = scan
            .orderByData
            .iter_mut()
            .map(|k| k.sk_func.fn_extra.take())
            .collect();

        so.orderByTypes.clear();
        scan.orderByData.clear();
        for (i, k) in orderbys.iter().enumerate() {
            let finfo = fmgr_info_copy(&so.giststate.distanceFn[k.sk_attno as usize - 1]);
            if finfo.fn_oid == 0 {
                return Err(Box::new(::types_error::PgError::error(format!(
                    "missing support function {} for attribute {} of index \"{}\"",
                    ::types_gist::GIST_DISTANCE_PROC,
                    k.sk_attno,
                    index_rel.name(),
                ))));
            }
            so.orderByTypes
                .push(indexam_seams::get_func_rettype::call(k.sk_func.fn_oid)?);
            let mut skey = ScanKeyData {
                sk_flags: k.sk_flags,
                sk_attno: k.sk_attno,
                sk_strategy: k.sk_strategy,
                sk_subtype: k.sk_subtype,
                sk_collation: k.sk_collation,
                sk_func: finfo,
                sk_argument: k.sk_argument,
            };
            if let Some(extra) = fn_extras.get_mut(i).and_then(|e| e.take()) {
                skey.sk_func.fn_extra = Some(extra);
            }
            scan.orderByData.push(skey);
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
