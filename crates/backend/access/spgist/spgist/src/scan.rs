//! spgscan.c: spgbeginscan / spgrescan / spgendscan / spggettuple /
//! spggetbitmap, including ordered (KNN) scans over the pairing-heap queue.

use ::types_gist::state::IndexOrderByDistance;

use ::bufmgr_seams::{self as bufmgr};
use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext, PgBox};
use ::types_core::fmgr::INDEX_MAX_KEYS;
use ::types_core::{BlockNumber, Buffer, InvalidBlockNumber};
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_relscan::{relation_get_index_scan, IndexScanDescData, IndexScanOpaque};
use ::types_scan::scankey::{ScanKeyData, SK_ISNULL, SK_SEARCHNOTNULL, SK_SEARCHNULL};
use ::types_scan::sdir::ScanDirection;
use ::types_spgist::state::{
    spgInnerConsistentIn, spgInnerConsistentOut, spgLeafConsistentIn, spgLeafConsistentOut,
    spg_search_item_cmp, SpGistScanOpaqueData, SpGistSearchItem, SpGistScanQueue,
};
use ::types_spgist::*;
use ::types_tuple::itemptr::{ItemPointerData, ItemPointerGetBlockNumber, ItemPointerIsValid};

use crate::utils::*;
use crate::utils::ItupExt as _;

const K: usize = INDEX_MAX_KEYS as usize;
const MaxOffsetNumber: u16 = (::types_core::BLCKSZ / 4) as u16;
const SpGistBreakOffsetNumber: u16 = InvalidOffsetNumber;
const SpGistRedirectOffsetNumber: u16 = MaxOffsetNumber + 1;

fn fmgr_info_copy(src: &::types_fmgr::FmgrInfo) -> ::types_fmgr::FmgrInfo {
    ::types_fmgr::FmgrInfo::new(
        src.fn_addr,
        src.fn_oid,
        src.fn_nargs,
        src.fn_strict,
        src.fn_retset,
    )
}

/// spgbeginscan.
pub fn spgbeginscan<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDescData<'mcx>> {
    let state = crate::initSpGistState(mcx, r)?;
    let recon_tup_desc = crate::utils::getSpGistTupleDesc(mcx, r, &state.attType)?;

    let inner_oid = index_getprocid(r, spgKeyColumn, SPGIST_INNER_CONSISTENT_PROC);
    let leaf_oid = index_getprocid(r, spgKeyColumn, SPGIST_LEAF_CONSISTENT_PROC);
    if inner_oid == 0 || leaf_oid == 0 {
        panic!(
            "missing support function for attribute 1 of index \"{}\"",
            r.name()
        );
    }
    let inner_fn = fmgr_seams::fmgr_info::call(inner_oid)?;
    let leaf_fn = fmgr_seams::fmgr_info::call(leaf_oid)?;
    let index_collation = r.rd_indcollation.first().copied().unwrap_or(0);

    let so = SpGistScanOpaqueData {
        state,
        scanQueue: SpGistScanQueue::new(spg_search_item_cmp),
        tempCxt: MemoryContext::new_bump("SP-GiST search temporary context"),
        traversalCxt: MemoryContext::new_bump("SP-GiST traversal-value context"),
        searchNulls: false,
        searchNonNulls: false,
        numberOfKeys: 0,
        keyData: Vec::new(),
        indexCollation: index_collation,
        numberOfOrderBys: 0,
        numberOfNonNullOrderBys: 0,
        orderByData: Vec::new(),
        nonNullOrderByOffsets: vec![0; norderbys.max(0) as usize],
        orderByTypes: vec![0; norderbys.max(0) as usize],
        zeroDistances: vec![0.0; norderbys.max(0) as usize],
        infDistances: vec![f64::INFINITY; norderbys.max(0) as usize],
        innerConsistentFn: inner_fn,
        leafConsistentFn: leaf_fn,
        frame2: ::types_fmgr::LocalFcinfo::<2>::new(0),
        want_itup: false,
        reconTupDesc: Some(recon_tup_desc),
        nPtrs: 0,
        iPtr: 0,
        heapPtrs: [ItemPointerData::invalid(); ::types_storage::bufpage::MaxIndexTuplesPerPage],
        recheck: [false; ::types_storage::bufpage::MaxIndexTuplesPerPage],
        recheckDistances: [false; ::types_storage::bufpage::MaxIndexTuplesPerPage],
        distances: Vec::new(),
        recon_buf: Vec::new(),
        recon_offs: [0u32; ::types_storage::bufpage::MaxIndexTuplesPerPage],
    };

    let so = PgBox::new_in(so, mcx);
    let mut scan = relation_get_index_scan(
        mcx,
        r,
        nkeys,
        norderbys,
        IndexScanOpaque::Spgist(so),
        xact::TransactionStartedDuringRecovery(),
    )?;
    let IndexScanOpaque::Spgist(so) = &scan.opaque else {
        unreachable!()
    };
    scan.xs_itupdesc = so.reconTupDesc.clone();
    Ok(scan)
}

// spgPrepareScanKeys.
fn spg_prepare_scan_keys(scan: &mut IndexScanDescData<'_>) {
    let nkeys_in = scan.numberOfKeys;
    let norderbys_in = scan.numberOfOrderBys;
    let IndexScanOpaque::Spgist(so) = &mut scan.opaque else {
        crate::non_spgist_opaque()
    };
    let so = &mut **so;

    so.numberOfOrderBys = norderbys_in;
    so.orderByData.clear();
    if norderbys_in <= 0 {
        so.numberOfNonNullOrderBys = 0;
    } else {
        // Remove all NULL keys, remembering their original offsets.
        let mut j = 0i32;
        for (i, skey) in scan.orderByData.iter().enumerate() {
            if skey.sk_flags & SK_ISNULL != 0 {
                so.nonNullOrderByOffsets[i] = -1;
            } else {
                so.orderByData.push(ScanKeyData {
                    sk_flags: skey.sk_flags,
                    sk_attno: skey.sk_attno,
                    sk_strategy: skey.sk_strategy,
                    sk_subtype: skey.sk_subtype,
                    sk_collation: skey.sk_collation,
                    sk_func: fmgr_info_copy(&skey.sk_func),
                    sk_argument: skey.sk_argument,
                });
                so.nonNullOrderByOffsets[i] = j;
                j += 1;
            }
        }
        so.numberOfNonNullOrderBys = j;
    }

    if nkeys_in <= 0 {
        so.searchNulls = true;
        so.searchNonNulls = true;
        so.numberOfKeys = 0;
        so.keyData.clear();
        return;
    }

    let mut qual_ok = true;
    let mut have_is_null = false;
    let mut have_not_null = false;
    so.keyData.clear();

    for skey in scan.keyData.iter() {
        if skey.sk_flags & SK_SEARCHNULL != 0 {
            have_is_null = true;
        } else if skey.sk_flags & SK_SEARCHNOTNULL != 0 {
            have_not_null = true;
        } else if skey.sk_flags & SK_ISNULL != 0 {
            qual_ok = false;
            break;
        } else {
            so.keyData.push(ScanKeyData {
                sk_flags: skey.sk_flags,
                sk_attno: skey.sk_attno,
                sk_strategy: skey.sk_strategy,
                sk_subtype: skey.sk_subtype,
                sk_collation: skey.sk_collation,
                sk_func: fmgr_info_copy(&skey.sk_func),
                sk_argument: skey.sk_argument,
            });
            have_not_null = true;
        }
    }

    if have_is_null && have_not_null {
        qual_ok = false;
    }

    if qual_ok {
        so.searchNulls = have_is_null;
        so.searchNonNulls = have_not_null;
        so.numberOfKeys = so.keyData.len() as i32;
    } else {
        so.searchNulls = false;
        so.searchNonNulls = false;
        so.numberOfKeys = 0;
        so.keyData.clear();
    }
}

fn spg_add_start_item(so: &mut SpGistScanOpaqueData<'_>, isnull: bool) {
    let distances = if isnull {
        Vec::new()
    } else {
        so.zeroDistances[..so.numberOfNonNullOrderBys.max(0) as usize].to_vec()
    };
    so.scanQueue.add(SpGistSearchItem {
        value: Datum::null(),
        leafTuple: None,
        traversalValue: 0,
        level: 0,
        heapPtr: ItemPointerData::new(
            if isnull { SPGIST_NULL_BLKNO } else { SPGIST_ROOT_BLKNO },
            FirstOffsetNumber,
        ),
        isNull: isnull,
        isLeaf: false,
        recheck: false,
        recheckDistances: false,
        distances,
    });
}

fn reset_scan_opaque(so: &mut SpGistScanOpaqueData<'_>) {
    so.traversalCxt.reset();
    so.scanQueue.reset();
    if so.searchNulls {
        spg_add_start_item(so, true);
    }
    if so.searchNonNulls {
        spg_add_start_item(so, false);
    }
    so.iPtr = 0;
    so.nPtrs = 0;
    so.distances.clear();
    so.recon_buf.clear();
}

/// spgrescan.
pub fn spgrescan(
    scan: &mut IndexScanDescData<'_>,
    keys: Option<&[ScanKeyData]>,
    orderbys: Option<&[ScanKeyData]>,
) -> PgResult<()> {
    if let Some(keys) = keys {
        if scan.numberOfKeys > 0 {
            debug_assert!(keys.len() as i32 == scan.numberOfKeys);
            scan.keyData.clear();
            for k in keys {
                scan.keyData.push(ScanKeyData {
                    sk_flags: k.sk_flags,
                    sk_attno: k.sk_attno,
                    sk_strategy: k.sk_strategy,
                    sk_subtype: k.sk_subtype,
                    sk_collation: k.sk_collation,
                    sk_func: fmgr_info_copy(&k.sk_func),
                    sk_argument: k.sk_argument,
                });
            }
        }
    }

    // Copy order-by keys and resolve each ordering operator's result type
    // (SP-GiST distances are always float8; the operator may return float4).
    if let Some(orderbys) = orderbys.filter(|_| scan.numberOfOrderBys > 0) {
        debug_assert!(orderbys.len() as i32 == scan.numberOfOrderBys);
        scan.orderByData.clear();
        let mut order_by_types = Vec::with_capacity(orderbys.len());
        for k in orderbys {
            order_by_types.push(indexam_seams::get_func_rettype::call(k.sk_func.fn_oid)?);
            scan.orderByData.push(ScanKeyData {
                sk_flags: k.sk_flags,
                sk_attno: k.sk_attno,
                sk_strategy: k.sk_strategy,
                sk_subtype: k.sk_subtype,
                sk_collation: k.sk_collation,
                sk_func: fmgr_info_copy(&k.sk_func),
                sk_argument: k.sk_argument,
            });
        }
        let IndexScanOpaque::Spgist(so) = &mut scan.opaque else {
            crate::non_spgist_opaque()
        };
        so.orderByTypes = order_by_types;
    }

    spg_prepare_scan_keys(scan);

    {
        let IndexScanOpaque::Spgist(so) = &mut scan.opaque else {
            crate::non_spgist_opaque()
        };
        reset_scan_opaque(so);
    }

    scan.xs_pgstat_index_scans += 1;
    scan.xs_nsearches += 1;
    scan.xs_itup = None;
    Ok(())
}

/// spgendscan: state drops with the scan value.
pub fn spgendscan(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    let IndexScanOpaque::Spgist(_so) = &mut scan.opaque else {
        crate::non_spgist_opaque()
    };
    Ok(())
}

/// spgcanreturn.
pub fn spgcanreturn(index: &Relation<'_>, attno: i32) -> PgResult<bool> {
    if attno > 1 {
        return Ok(true);
    }
    let cache = spgGetCache(index)?;
    Ok(cache.config.canReturnData)
}

// datumCopy into the traversal context; the arena owns the bytes.
fn datum_copy_traversal(
    traversal: &MemoryContext,
    value: Datum,
    attbyval: bool,
    attlen: i16,
) -> Datum {
    if attbyval || value.as_usize() == 0 {
        return value;
    }
    let mcx = traversal.mcx();
    let size = if attlen > 0 {
        attlen as usize
    } else {
        // SAFETY: by-ref varlena datum carries a live pointer.
        unsafe { ::types_tuple::varatt::varsize_any(value.as_usize() as *const u8) }
    };
    let mut buf: ::mcx::PgVec<'_, u8> =
        ::mcx::vec_with_capacity_in(mcx, size).expect("traversal datumCopy");
    // SAFETY: source live for size bytes.
    buf.extend_from_slice(unsafe {
        core::slice::from_raw_parts(value.as_usize() as *const u8, size)
    });
    let p = buf.as_ptr() as usize;
    core::mem::forget(buf);
    Datum::from_usize(p)
}

enum StoreDest<'t, 'b> {
    Bitmap(&'t mut tidbitmap::TIDBitmap<'b>, &'t mut i64),
    Tuples,
}

#[allow(clippy::too_many_arguments)]
fn store_result(
    so: &mut SpGistScanOpaqueData<'_>,
    dest: &mut StoreDest<'_, '_>,
    heap_ptr: &ItemPointerData,
    leaf_value: Datum,
    isnull: bool,
    leaf_tuple: Option<&[u8]>,
    recheck: bool,
    recheck_distances: bool,
    non_null_distances: Option<&[f64]>,
) -> PgResult<()> {
    match dest {
        StoreDest::Bitmap(tbm, ntids) => {
            debug_assert!(!recheck_distances && non_null_distances.is_none());
            tbm.add_tuples(core::slice::from_ref(heap_ptr), recheck)?;
            **ntids += 1;
        }
        StoreDest::Tuples => {
            debug_assert!(so.nPtrs < ::types_storage::bufpage::MaxIndexTuplesPerPage);
            so.heapPtrs[so.nPtrs] = *heap_ptr;
            so.recheck[so.nPtrs] = recheck;
            so.recheckDistances[so.nPtrs] = recheck_distances;

            if so.numberOfOrderBys > 0 {
                debug_assert_eq!(so.distances.len(), so.nPtrs);
                let row = match non_null_distances {
                    Some(d) if !isnull && so.numberOfNonNullOrderBys > 0 => {
                        let mut out =
                            Vec::with_capacity(so.numberOfOrderBys.max(0) as usize);
                        for i in 0..so.numberOfOrderBys as usize {
                            let offset = so.nonNullOrderByOffsets[i];
                            if offset >= 0 {
                                out.push(IndexOrderByDistance {
                                    value: d[offset as usize],
                                    isnull: false,
                                });
                            } else {
                                out.push(IndexOrderByDistance { value: 0.0, isnull: true });
                            }
                        }
                        Some(out)
                    }
                    _ => None,
                };
                so.distances.push(row);
            }

            if so.want_itup {
                let mut leaf_datums = [Datum::null(); K];
                let mut leaf_isnulls = [false; K];
                let recon_desc = so.reconTupDesc.clone().expect("reconTupDesc set");
                let natts = recon_desc.natts as usize;

                if so.state.leafTupDesc.natts > 1 {
                    spgDeformLeafTuple(
                        leaf_tuple.expect("leaf tuple image for INCLUDE deform"),
                        &so.state.leafTupDesc,
                        &mut leaf_datums,
                        &mut leaf_isnulls,
                        isnull,
                    );
                }
                leaf_datums[spgKeyColumn] = leaf_value;
                leaf_isnulls[spgKeyColumn] = isnull;

                let mcx = so.tempCxt.mcx();
                let formed = ::nbtree::itup::index_form_tuple(
                    mcx,
                    &recon_desc,
                    &leaf_datums[..natts],
                    &leaf_isnulls[..natts],
                )?;
                let off = so.recon_buf.len() as u32;
                so.recon_buf.extend_from_slice(formed.as_slice());
                while so.recon_buf.len() % 8 != 0 {
                    so.recon_buf.push(0);
                }
                so.recon_offs[so.nPtrs] = off;
            }
            so.nPtrs += 1;
        }
    }
    Ok(())
}

// spgLeafTest (unordered arms).
#[allow(clippy::too_many_arguments)]
fn spg_leaf_test(
    so: &mut SpGistScanOpaqueData<'_>,
    dest: &mut StoreDest<'_, '_>,
    item: &SpGistSearchItem,
    leaf_tuple: &[u8],
    isnull: bool,
    reported_some: &mut bool,
) -> PgResult<bool> {
    let heap_ptr = SpGistLeafTupleHeader::decode(leaf_tuple).heapPtr;

    let (result, leaf_value, recheck, recheck_distances, distances) = if isnull {
        debug_assert!(so.searchNulls);
        (true, Datum::null(), false, false, Vec::new())
    } else {
        let leaf_in = spgLeafConsistentIn {
            scankeys: so.keyData.as_ptr(),
            orderbys: so.orderByData.as_ptr(),
            nkeys: so.numberOfKeys,
            norderbys: so.numberOfNonNullOrderBys,
            reconstructedValue: item.value,
            traversalValue: item.traversalValue,
            level: item.level,
            returnData: so.want_itup,
            leafDatum: leaf_datum(leaf_tuple, &so.state),
        };
        let mut leaf_out = spgLeafConsistentOut::default();

        so.frame2.rearm(so.indexCollation);
        let mcx = so.tempCxt.mcx();
        // SAFETY: tempCxt outlives consumption of the outputs (reset at item end).
        unsafe { so.frame2.set_result_mcx(mcx) };
        so.frame2.set_arg(
            0,
            Datum::from_usize(&leaf_in as *const spgLeafConsistentIn as usize),
        );
        so.frame2.set_arg(
            1,
            Datum::from_usize(&mut leaf_out as *mut spgLeafConsistentOut as usize),
        );
        let r = so.leafConsistentFn.invoke(&mut so.frame2)?;
        let distances = if r.as_bool()
            && so.numberOfNonNullOrderBys > 0
            && !leaf_out.distances.is_null()
        {
            // SAFETY: opclass contract — norderbys distances in the armed mcx.
            unsafe {
                core::slice::from_raw_parts(
                    leaf_out.distances,
                    so.numberOfNonNullOrderBys as usize,
                )
            }
            .to_vec()
        } else {
            Vec::new()
        };
        (
            r.as_bool(),
            leaf_out.leafValue,
            leaf_out.recheck,
            leaf_out.recheckDistances,
            distances,
        )
    };

    if result {
        if so.numberOfNonNullOrderBys > 0 {
            // Ordered scan: queue the heap item (spgNewHeapItem).
            let value = if so.want_itup && !isnull {
                datum_copy_traversal(
                    &so.traversalCxt,
                    leaf_value,
                    so.state.attType.attbyval,
                    so.state.attType.attlen,
                )
            } else {
                Datum::null()
            };
            let leaf_copy = if so.want_itup && so.state.leafTupDesc.natts > 1 {
                Some(leaf_tuple.to_vec())
            } else {
                None
            };
            so.scanQueue.add(SpGistSearchItem {
                value,
                leafTuple: leaf_copy,
                traversalValue: 0,
                level: item.level,
                heapPtr: heap_ptr,
                isNull: isnull,
                isLeaf: true,
                recheck,
                recheckDistances: recheck_distances,
                distances,
            });
        } else {
            debug_assert!(!recheck_distances);
            store_result(
                so,
                dest,
                &heap_ptr,
                leaf_value,
                isnull,
                Some(leaf_tuple),
                recheck,
                false,
                None,
            )?;
            *reported_some = true;
        }
    }

    Ok(result)
}

// spgInnerTest.
fn spg_inner_test(
    so: &mut SpGistScanOpaqueData<'_>,
    item: &SpGistSearchItem,
    inner_tuple: &[u8],
    isnull: bool,
) -> PgResult<()> {
    let hdr = SpGistInnerTupleHeader::decode(inner_tuple);
    let n_nodes = hdr.nNodes as usize;

    let mut node_numbers: Vec<i32> = Vec::new();
    let mut level_adds: Vec<i32> = Vec::new();
    let mut recon_values: Vec<Datum> = Vec::new();
    let mut traversal_values: Vec<usize> = Vec::new();
    let mut distance_rows: Vec<Vec<f64>> = Vec::new();
    let mut have_level_adds = false;
    let mut have_recon = false;
    let mut have_traversal = false;
    let mut have_distances = false;

    if !isnull {
        let mut labels_scratch: Vec<Datum> = Vec::new();
        let has_labels = spgExtractNodeLabels(&so.state, inner_tuple, &mut labels_scratch);

        let traversal_mcx = so.traversalCxt.mcx();
        let inner_in = spgInnerConsistentIn {
            scankeys: so.keyData.as_ptr(),
            orderbys: so.orderByData.as_ptr(),
            nkeys: so.numberOfKeys,
            norderbys: so.numberOfNonNullOrderBys,
            reconstructedValue: item.value,
            traversalValue: item.traversalValue,
            traversalMemoryContext: traversal_mcx,
            level: item.level,
            returnData: so.want_itup,
            allTheSame: hdr.allTheSame,
            hasPrefix: hdr.prefixSize > 0,
            prefixDatum: inner_prefix_datum(inner_tuple, &so.state),
            nNodes: hdr.nNodes as i32,
            nodeLabels: if has_labels {
                labels_scratch.as_ptr()
            } else {
                core::ptr::null()
            },
        };
        let mut inner_out = spgInnerConsistentOut::default();

        so.frame2.rearm(so.indexCollation);
        let mcx = so.tempCxt.mcx();
        // SAFETY: tempCxt outlives consumption of the outputs.
        unsafe { so.frame2.set_result_mcx(mcx) };
        so.frame2.set_arg(
            0,
            Datum::from_usize(&inner_in as *const spgInnerConsistentIn as usize),
        );
        so.frame2.set_arg(
            1,
            Datum::from_usize(&mut inner_out as *mut spgInnerConsistentOut as usize),
        );
        so.innerConsistentFn.invoke(&mut so.frame2)?;

        let out_n = inner_out.nNodes.max(0) as usize;
        if !inner_out.nodeNumbers.is_null() {
            // SAFETY: opclass contract — out_n entries in the armed mcx.
            node_numbers
                .extend_from_slice(unsafe { core::slice::from_raw_parts(inner_out.nodeNumbers, out_n) });
        }
        if !inner_out.levelAdds.is_null() {
            have_level_adds = true;
            // SAFETY: as above.
            level_adds
                .extend_from_slice(unsafe { core::slice::from_raw_parts(inner_out.levelAdds, out_n) });
        }
        if !inner_out.reconstructedValues.is_null() {
            have_recon = true;
            // SAFETY: as above.
            recon_values.extend_from_slice(unsafe {
                core::slice::from_raw_parts(inner_out.reconstructedValues, out_n)
            });
        }
        if !inner_out.distances.is_null() {
            have_distances = true;
            let nn = so.numberOfNonNullOrderBys.max(0) as usize;
            // SAFETY: out_n rows of norderbys distances in the armed mcx.
            let rows = unsafe { core::slice::from_raw_parts(inner_out.distances, out_n) };
            for &row in rows {
                distance_rows.push(if row.is_null() {
                    Vec::new()
                } else {
                    // SAFETY: as above.
                    unsafe { core::slice::from_raw_parts(row, nn) }.to_vec()
                });
            }
        }
        if !inner_out.traversalValues.is_null() {
            have_traversal = true;
            // These datums outlive tempCxt.reset(): the opclass must allocate
            // them in traversalMemoryContext (C contract), never the armed
            // per-call context. No shipped opclass sets them yet.
            // SAFETY: as above.
            traversal_values.extend_from_slice(unsafe {
                core::slice::from_raw_parts(inner_out.traversalValues, out_n)
            });
        }

        if hdr.allTheSame && out_n != 0 && out_n != n_nodes {
            panic!("inconsistent inner_consistent results for allTheSame inner tuple");
        }
    } else {
        node_numbers.extend(0..n_nodes as i32);
    }

    if node_numbers.is_empty() {
        return Ok(());
    }

    // collect node downlink tids
    let mut node_tids = [ItemPointerData::invalid(); 8192 / 8];
    let mut node_count = 0usize;
    for (_, off) in inner_tuple_nodes(inner_tuple) {
        node_tids[node_count] = node_tuple_tid(&inner_tuple[off..]);
        node_count += 1;
    }
    debug_assert_eq!(node_count, n_nodes);

    for (i, &node_n) in node_numbers.iter().enumerate() {
        debug_assert!(node_n >= 0 && (node_n as usize) < n_nodes);
        let tid = node_tids[node_n as usize];
        if !ItemPointerIsValid(&tid) {
            continue;
        }
        let value = if have_recon {
            datum_copy_traversal(
                &so.traversalCxt,
                recon_values[i],
                so.state.attLeafType.attbyval,
                so.state.attLeafType.attlen,
            )
        } else {
            Datum::null()
        };
        // Infinity distances when the opclass returned none, or for NULL
        // items (their distances are really unused).
        let distances = if isnull || so.numberOfNonNullOrderBys <= 0 {
            Vec::new()
        } else if have_distances && !distance_rows[i].is_empty() {
            distance_rows[i].clone()
        } else {
            so.infDistances[..so.numberOfNonNullOrderBys as usize].to_vec()
        };
        so.scanQueue.add(SpGistSearchItem {
            value,
            leafTuple: None,
            traversalValue: if have_traversal { traversal_values[i] } else { 0 },
            level: item.level + if have_level_adds { level_adds[i] } else { 0 },
            heapPtr: tid,
            isNull: isnull,
            isLeaf: false,
            recheck: false,
            recheckDistances: false,
            distances,
        });
    }

    Ok(())
}

// spgTestLeafTuple: returns the next offset in the chain, or a sentinel.
#[allow(clippy::too_many_arguments)]
fn spg_test_leaf_tuple(
    so: &mut SpGistScanOpaqueData<'_>,
    dest: &mut StoreDest<'_, '_>,
    item: &mut SpGistSearchItem,
    buffer: Buffer,
    offset: u16,
    isnull: bool,
    isroot: bool,
    reported_some: &mut bool,
) -> PgResult<u16> {
    let pm = buf_page_mut(buffer);
    let pr = pm.as_ref();
    let leaf_tuple = item_slice(&pr, offset);
    let st = tuple_state(leaf_tuple);

    if st != SPGIST_LIVE {
        if !isroot {
            if st == SPGIST_REDIRECT {
                debug_assert!(offset == item.heapPtr.ip_posid);
                let dt = SpGistDeadTupleHeader::decode(leaf_tuple);
                item.heapPtr = dt.pointer;
                debug_assert!(
                    ItemPointerGetBlockNumber(&item.heapPtr) != SPGIST_METAPAGE_BLKNO
                );
                return Ok(SpGistRedirectOffsetNumber);
            }
            if st == SPGIST_DEAD {
                debug_assert!(offset == item.heapPtr.ip_posid);
                return Ok(SpGistBreakOffsetNumber);
            }
        }
        tuple_state_error(st);
    }

    debug_assert!(ItemPointerIsValid(
        &SpGistLeafTupleHeader::decode(leaf_tuple).heapPtr
    ));

    spg_leaf_test(so, dest, item, leaf_tuple, isnull, reported_some)?;

    Ok(leaf_next_offset(leaf_tuple))
}

// spgWalk.
fn spg_walk(
    scan: &mut IndexScanDescData<'_>,
    scan_whole_index: bool,
    mut dest: StoreDest<'_, '_>,
) -> PgResult<()> {
    let rel = scan.index_rel().alias();
    let IndexScanOpaque::Spgist(so) = &mut scan.opaque else {
        crate::non_spgist_opaque()
    };
    let so = &mut **so;

    let mut buffer: Buffer = InvalidBuffer;
    let mut buffer_blkno: BlockNumber = InvalidBlockNumber;
    let mut reported_some = false;

    'items: while scan_whole_index || !reported_some {
        let Some(mut item) = so.scanQueue.remove_first() else {
            break;
        };

        // redirect loop
        loop {
            crate::check_for_interrupts()?;

            if item.isLeaf {
                // Heap items are queued only in ordered scans.
                debug_assert!(so.numberOfNonNullOrderBys > 0);
                let leaf_copy = item.leafTuple.take();
                store_result(
                    so,
                    &mut dest,
                    &item.heapPtr,
                    item.value,
                    item.isNull,
                    leaf_copy.as_deref(),
                    item.recheck,
                    item.recheckDistances,
                    if item.isNull { None } else { Some(&item.distances) },
                )?;
                reported_some = true;
                so.tempCxt.reset();
                continue 'items;
            }

            let blkno = ItemPointerGetBlockNumber(&item.heapPtr);
            let mut offset = item.heapPtr.ip_posid;

            if buffer == InvalidBuffer {
                buffer = bufmgr::read_buffer::call(&rel, blkno)?;
                bufmgr::lock_buffer::call(buffer, bufmgr::BUFFER_LOCK_SHARE)?;
                buffer_blkno = blkno;
            } else if blkno != buffer_blkno {
                unlock_release(buffer)?;
                buffer = bufmgr::read_buffer::call(&rel, blkno)?;
                bufmgr::lock_buffer::call(buffer, bufmgr::BUFFER_LOCK_SHARE)?;
                buffer_blkno = blkno;
            }

            let (page_is_leaf, stores_nulls, max) = {
                let pm = buf_page_mut(buffer);
                let pr = pm.as_ref();
                (
                    SpGistPageIsLeaf(&pr),
                    SpGistPageStoresNulls(&pr),
                    pr.max_offset_number(),
                )
            };
            let isnull = stores_nulls;

            if page_is_leaf {
                if SpGistBlockIsRoot(blkno) {
                    for off in FirstOffsetNumber..=max {
                        spg_test_leaf_tuple(
                            so,
                            &mut dest,
                            &mut item,
                            buffer,
                            off,
                            isnull,
                            true,
                            &mut reported_some,
                        )?;
                    }
                } else {
                    let mut redirected = false;
                    while offset != InvalidOffsetNumber {
                        debug_assert!(offset >= FirstOffsetNumber && offset <= max);
                        offset = spg_test_leaf_tuple(
                            so,
                            &mut dest,
                            &mut item,
                            buffer,
                            offset,
                            isnull,
                            false,
                            &mut reported_some,
                        )?;
                        if offset == SpGistRedirectOffsetNumber {
                            redirected = true;
                            break;
                        }
                    }
                    if redirected {
                        continue; // goto redirect
                    }
                }
            } else {
                let st = {
                    let pm = buf_page_mut(buffer);
                    let pr = pm.as_ref();
                    tuple_state(item_slice(&pr, offset))
                };
                if st != SPGIST_LIVE {
                    if st == SPGIST_REDIRECT {
                        let pm = buf_page_mut(buffer);
                        let pr = pm.as_ref();
                        let dt = SpGistDeadTupleHeader::decode(item_slice(&pr, offset));
                        item.heapPtr = dt.pointer;
                        debug_assert!(
                            ItemPointerGetBlockNumber(&item.heapPtr) != SPGIST_METAPAGE_BLKNO
                        );
                        continue; // goto redirect
                    }
                    tuple_state_error(st);
                }
                // SAFETY: the item stays live under our share lock for the
                // duration of spg_inner_test (C reads it in place too).
                let inner: &[u8] = unsafe {
                    let pm = buf_page_mut(buffer);
                    let pr = pm.as_ref();
                    let it = item_slice(&pr, offset);
                    core::slice::from_raw_parts(it.as_ptr(), it.len())
                };
                spg_inner_test(so, &item, inner, isnull)?;
            }

            so.tempCxt.reset();
            continue 'items;
        }
    }

    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }
    Ok(())
}

/// spggetbitmap.
pub fn spggetbitmap(
    scan: &mut IndexScanDescData<'_>,
    tbm: &mut tidbitmap::TIDBitmap<'_>,
) -> PgResult<i64> {
    {
        let IndexScanOpaque::Spgist(so) = &mut scan.opaque else {
            crate::non_spgist_opaque()
        };
        so.want_itup = false;
    }
    let mut ntids: i64 = 0;
    spg_walk(scan, true, StoreDest::Bitmap(tbm, &mut ntids))?;
    Ok(ntids)
}

/// spggettuple.
pub fn spggettuple(scan: &mut IndexScanDescData<'_>, dir: ScanDirection) -> PgResult<bool> {
    if dir != ::types_scan::sdir::ForwardScanDirection {
        panic!("SP-GiST only supports forward scan direction");
    }

    let want_itup = scan.xs_want_itup;
    {
        let IndexScanOpaque::Spgist(so) = &mut scan.opaque else {
            crate::non_spgist_opaque()
        };
        so.want_itup = want_itup;
    }

    loop {
        {
            let IndexScanOpaque::Spgist(so) = &mut scan.opaque else {
                crate::non_spgist_opaque()
            };
            let so = &mut **so;
            if so.iPtr < so.nPtrs {
                scan.xs_heaptid = so.heapPtrs[so.iPtr];
                scan.xs_recheck = so.recheck[so.iPtr];
                if want_itup {
                    let off = so.recon_offs[so.iPtr] as usize;
                    scan.xs_itup =
                        core::ptr::NonNull::new(so.recon_buf[off..].as_ptr() as *mut u8);
                }
                if so.numberOfOrderBys > 0 {
                    // index_store_float8_orderby_distances (indexam.c).
                    scan.xs_recheckorderby = so.recheckDistances[so.iPtr];
                    for i in 0..so.numberOfOrderBys as usize {
                        let d = so.distances[so.iPtr].as_ref().and_then(|v| v.get(i));
                        match so.orderByTypes[i] {
                            ::types_core::FLOAT8OID => match d {
                                Some(d) if !d.isnull => {
                                    scan.xs_orderbyvals[i] = Datum::from_f64(d.value);
                                    scan.xs_orderbynulls[i] = false;
                                }
                                _ => {
                                    scan.xs_orderbyvals[i] = Datum::null();
                                    scan.xs_orderbynulls[i] = true;
                                }
                            },
                            ::types_core::FLOAT4OID => match d {
                                Some(d) if !d.isnull => {
                                    scan.xs_orderbyvals[i] = Datum::from_f32(d.value as f32);
                                    scan.xs_orderbynulls[i] = false;
                                }
                                _ => {
                                    scan.xs_orderbyvals[i] = Datum::null();
                                    scan.xs_orderbynulls[i] = true;
                                }
                            },
                            _ => {
                                // Only float distances convert; a lossy
                                // distance with another type can't be
                                // rechecked.
                                if scan.xs_recheckorderby {
                                    return Err(Box::new(::types_error::PgError::error(
                                        "ORDER BY operator must return float8 or float4 if the distance function is lossy"
                                            .to_string(),
                                    )));
                                }
                                scan.xs_orderbynulls[i] = true;
                            }
                        }
                    }
                }
                so.iPtr += 1;
                return Ok(true);
            }
            so.iPtr = 0;
            so.nPtrs = 0;
            so.distances.clear();
            so.recon_buf.clear();
            scan.xs_itup = None;
        }

        spg_walk(scan, false, StoreDest::Tuples)?;

        let done = {
            let IndexScanOpaque::Spgist(so) = &mut scan.opaque else {
                crate::non_spgist_opaque()
            };
            so.nPtrs == 0
        };
        if done {
            return Ok(false);
        }
    }
}
