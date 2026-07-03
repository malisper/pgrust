//! brin.c: the BRIN index access method (build, insert, bitmap scan).
//! Loud lanes: summarize/desummarize SQL paths, autosummarize, vacuum,
//! parallel build, reloptions, inclusion/bloom opclasses.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

use ::bufmgr_seams::{
    buffer_get_page, lock_buffer, read_buffer, release_buffer, BUFFER_LOCK_SHARE,
    BUFFER_LOCK_UNLOCK,
};
use ::datum::Datum;
use ::mcx::{vec_with_capacity_in, Mcx, MemoryContext, PgVec};
use ::types_brin::*;
use ::types_core::{BlockNumber, Buffer, ForkNumber, InvalidBuffer};
use ::types_error::PgResult;
use ::types_rel::{AccessShareLock, Relation};
use ::types_relscan::{relation_get_index_scan, IndexScanDescData, IndexScanOpaque};
use ::types_scan::scankey::{ScanKeyData, SK_ISNULL, SK_SEARCHNULL, SK_SEARCHNOTNULL};
use ::types_storage::bufpage::PageRef;
use ::types_tuple::itemptr::{ItemPointerData, ItemPointerGetBlockNumber};
use ::types_tuple::{CompactAttribute, FormData_pg_attribute, TupleDescData};

use brin_pageops::{
    brinGetTupleForHeapBlock, brinRevmapInitialize, brinRevmapTerminate,
    brin_can_do_samepage_update, brin_doupdate,
};
use brin_tuple::{brin_deform_tuple, brin_form_tuple, brin_new_memtuple};

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: {what}")
}

/// BrinGetPagesPerRange: reloptions are loud at DefineIndex, so every live
/// index carries the default.
pub fn brin_get_pages_per_range(rel: &Relation<'_>) -> BlockNumber {
    if rel.rd_options.is_some() {
        unported("BRIN reloptions (pages_per_range/autosummarize)");
    }
    BRIN_DEFAULT_PAGES_PER_RANGE
}

fn brin_get_auto_summarize(rel: &Relation<'_>) -> bool {
    if rel.rd_options.is_some() {
        unported("BRIN reloptions (pages_per_range/autosummarize)");
    }
    false
}

pub fn brin_build_desc<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<BrinDesc<'mcx>> {
    let tupdesc = rel.rd_att.clone();
    let natts = tupdesc.natts as usize;

    let mut info: Vec<BrinColInfo> = Vec::with_capacity(natts);
    let mut totalstored = 0usize;
    for keyno in 0..natts {
        let opfamily = rel.rd_opfamily[keyno];
        let opcintype = rel.rd_opcintype[keyno];
        let opcinfo_proc = lsyscache::get_opfamily_proc(
            opfamily,
            opcintype,
            opcintype,
            BRIN_PROCNUM_OPCINFO as i16,
        )?;
        let col = match opcinfo_proc {
            F_BRIN_MINMAX_OPCINFO => brin_minmax::brin_minmax_opcinfo(opcintype),
            F_BRIN_MINMAX_MULTI_OPCINFO => {
                brin_minmax_multi::brin_minmax_multi_opcinfo(opcintype)
            }
            F_BRIN_INCLUSION_OPCINFO => unported("BRIN inclusion opclass"),
            F_BRIN_BLOOM_OPCINFO => unported("BRIN bloom opclass"),
            other => panic!("unported: BRIN opclass with opcinfo proc {other}"),
        };
        debug_assert!((col.oi_nstored as usize) <= BRIN_MAX_NSTORED);
        totalstored += col.oi_nstored as usize;
        info.push(col);
    }

    let mut attrs: PgVec<'mcx, FormData_pg_attribute> = vec_with_capacity_in(mcx, totalstored)?;
    let mut compact: PgVec<'mcx, CompactAttribute> = vec_with_capacity_in(mcx, totalstored)?;
    for keyno in 0..natts {
        for i in 0..info[keyno].oi_nstored as usize {
            let mut att = tupdesc.attr(keyno).clone();
            att.attnum = (attrs.len() + 1) as i16;
            att.atttypmod = -1;
            att.attndims = 0;
            att.attnotnull = false;
            let stored_typid = info[keyno].oi_typids[i];
            if stored_typid != att.atttypid {
                // TupleDescInitEntry over the opclass's summary type; only the
                // fill/deform-relevant pg_type fields matter for the disk desc.
                debug_assert!(stored_typid == PG_BRIN_MINMAX_MULTI_SUMMARYOID);
                att.atttypid = stored_typid;
                att.attlen = -1;
                att.attbyval = false;
                att.attalign = types_tuple::TYPALIGN_INT;
                att.attstorage = types_tuple::TYPSTORAGE_EXTENDED;
                att.attcompression = 0;
                att.attcollation = types_core::DEFAULT_COLLATION_OID;
            }
            compact.push(CompactAttribute::populate_from(&att));
            attrs.push(att);
        }
    }
    let disktdesc = TupleDescData {
        natts: totalstored as i32,
        tdtypeid: types_core::RECORDOID,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    };

    Ok(BrinDesc {
        bd_tupdesc: tupdesc,
        bd_disktdesc: disktdesc,
        bd_totalstored: totalstored,
        bd_opfamily: rel.rd_opfamily.iter().copied().collect(),
        bd_opcintype: rel.rd_opcintype.iter().copied().collect(),
        bd_indcollation: rel.rd_indcollation.iter().copied().collect(),
        bd_pages_per_range: brin_get_pages_per_range(rel),
        bd_info: info,
    })
}

pub struct BrinStatsData {
    pub pagesPerRange: BlockNumber,
    pub revmapNumPages: BlockNumber,
}

pub fn brinGetStats(index: &Relation<'_>) -> PgResult<BrinStatsData> {
    let metabuffer = read_buffer::call(index, BRIN_METAPAGE_BLKNO)?;
    lock_buffer::call(metabuffer, BUFFER_LOCK_SHARE)?;
    // SAFETY: pinned + share locked.
    let metadata =
        brin_meta_read(unsafe { &PageRef::from_raw(buffer_get_page::call(metabuffer)) });
    lock_buffer::call(metabuffer, BUFFER_LOCK_UNLOCK)?;
    release_buffer::call(metabuffer)?;
    Ok(BrinStatsData {
        pagesPerRange: metadata.pagesPerRange,
        revmapNumPages: metadata.lastRevmapPage.wrapping_sub(1),
    })
}

fn initialize_brin_insertstate<'mcx>(
    mcx: Mcx<'mcx>,
    idxRel: &Relation<'mcx>,
) -> PgResult<BrinInsertState<'mcx>> {
    let bis_desc = brin_build_desc(mcx, idxRel)?;
    let (bis_rmAccess, bis_pages_per_range) = brinRevmapInitialize(idxRel)?;
    Ok(BrinInsertState { bis_rmAccess, bis_desc, bis_pages_per_range })
}

/// brininsert. `amcache` is C's indexInfo->ii_AmCache slot, owned by the
/// executor's per-command index state.
pub fn brininsert<'mcx>(
    mcx: Mcx<'mcx>,
    idxRel: &Relation<'mcx>,
    values: &[Datum],
    nulls: &[bool],
    heaptid: &ItemPointerData,
    amcache: &mut Option<BrinInsertState<'mcx>>,
) -> PgResult<bool> {
    let autosummarize = brin_get_auto_summarize(idxRel);

    if amcache.is_none() {
        *amcache = Some(initialize_brin_insertstate(mcx, idxRel)?);
    }
    let bistate = amcache.as_ref().expect("initialized above");
    let revmap = &bistate.bis_rmAccess;
    let bdesc = &bistate.bis_desc;
    let pagesPerRange = bistate.bis_pages_per_range;

    let origHeapBlk = ItemPointerGetBlockNumber(heaptid);
    let heapBlk = (origHeapBlk / pagesPerRange) * pagesPerRange;

    let mut buf: Buffer = InvalidBuffer;

    loop {
        if autosummarize && heapBlk > 0 && heapBlk == origHeapBlk && heaptid.ip_posid == 1 {
            unported("BRIN autosummarize (AutoVacuumRequestWork)");
        }

        let tupcxt = MemoryContext::new_bump("brininsert cxt");
        let cmcx = tupcxt.mcx();
        let mut brtup: PgVec<'_, u8> = PgVec::new_in(cmcx);
        let got = brinGetTupleForHeapBlock(
            idxRel,
            revmap,
            heapBlk,
            &mut buf,
            &mut brtup,
            BUFFER_LOCK_SHARE,
            true,
        )?;

        let Some(off) = got else {
            break;
        };

        let mut dtup = brin_new_memtuple(bdesc);
        brin_deform_tuple(bdesc, &brtup, &mut dtup)?;

        let need_insert = add_values_to_range(bdesc, &mut dtup, values, nulls)?;

        if !need_insert {
            lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
        } else {
            let newtup = brin_form_tuple(cmcx, bdesc, heapBlk, &mut dtup)?;
            let samepage = brin_can_do_samepage_update(buf, brtup.len(), newtup.len());
            lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;

            if !brin_doupdate(
                idxRel,
                pagesPerRange,
                revmap,
                heapBlk,
                buf,
                off,
                &brtup,
                &newtup,
                samepage,
            )? {
                continue;
            }
        }

        break;
    }

    if buf != InvalidBuffer {
        release_buffer::call(buf)?;
    }

    Ok(false)
}

pub fn brininsertcleanup(amcache: &mut Option<BrinInsertState<'_>>) -> PgResult<()> {
    let Some(bistate) = amcache.take() else {
        return Ok(());
    };
    brinRevmapTerminate(bistate.bis_rmAccess)
}

pub fn brinbeginscan<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDescData<'mcx>> {
    let (bo_rmAccess, bo_pagesPerRange) = brinRevmapInitialize(r)?;
    let bo_bdesc = brin_build_desc(mcx, r)?;
    let opaque =
        ::mcx::alloc_in(mcx, BrinOpaque { bo_pagesPerRange, bo_rmAccess, bo_bdesc })?;
    relation_get_index_scan(
        mcx,
        r,
        nkeys,
        norderbys,
        IndexScanOpaque::Brin(opaque),
        xact::TransactionStartedDuringRecovery(),
    )
}

pub fn bringetbitmap(
    scan: &mut IndexScanDescData<'_>,
    tbm: &mut tidbitmap::TIDBitmap<'_>,
) -> PgResult<i64> {
    if scan.indexRelation.pgstat_enabled.get() {
        scan.xs_pgstat_index_scans += 1;
    }
    scan.xs_nsearches += 1;

    let idxRel = scan.indexRelation.alias();
    let scan_mcx = *scan.keyData.allocator();
    let nkeys_total = scan.numberOfKeys.max(0) as usize;

    let IndexScanOpaque::Brin(opaque) = &mut scan.opaque else {
        unreachable!("bringetbitmap on non-BRIN opaque")
    };
    let bdesc = &opaque.bo_bdesc;
    let natts = bdesc.natts();
    let mut totalpages: i64 = 0;

    let heapOid = idxRel.rd_index.as_ref().expect("index").indrelid;
    let heapRel = table::table_open(scan_mcx, heapOid, AccessShareLock)?;
    let nblocks = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
        &heapRel,
        ForkNumber::MAIN_FORKNUM,
    )?;
    heapRel.close(AccessShareLock)?;

    let mut keys: Vec<Vec<&ScanKeyData>> =
        (0..natts).map(|_| Vec::with_capacity(nkeys_total)).collect();
    let mut nullkeys: Vec<Vec<&ScanKeyData>> =
        (0..natts).map(|_| Vec::with_capacity(nkeys_total)).collect();
    for key in scan.keyData.iter().take(nkeys_total) {
        let keyattno = key.sk_attno as usize;
        debug_assert!(
            (key.sk_flags & SK_ISNULL) != 0
                || key.sk_collation == bdesc.bd_tupdesc.attr(keyattno - 1).attcollation
        );
        if key.sk_flags & SK_ISNULL != 0 {
            nullkeys[keyattno - 1].push(key);
        } else {
            keys[keyattno - 1].push(key);
        }
    }

    let mut dtup = brin_new_memtuple(bdesc);

    let mut per_range = MemoryContext::new_bump("bringetbitmap cxt");
    let mut buf: Buffer = InvalidBuffer;
    let mut btup: PgVec<'_, u8> = PgVec::new_in(scan_mcx);

    // u64 iteration: heapBlk could wrap for tables near 2^32 pages.
    let mut heapBlk: u64 = 0;
    while heapBlk < nblocks as u64 {
        per_range.reset();

        let gottuple = brinGetTupleForHeapBlock(
            &idxRel,
            &opaque.bo_rmAccess,
            heapBlk as BlockNumber,
            &mut buf,
            &mut btup,
            BUFFER_LOCK_SHARE,
            false,
        )?
        .is_some();

        let addrange = if !gottuple {
            true
        } else {
            brin_deform_tuple(bdesc, &btup, &mut dtup)?;
            if dtup.bt_placeholder {
                true
            } else {
                let mut addrange = true;
                for attno in 1..=natts {
                    if keys[attno - 1].is_empty() && nullkeys[attno - 1].is_empty() {
                        continue;
                    }

                    if dtup.bt_empty_range {
                        addrange = false;
                        break;
                    }

                    let bval = &dtup.bt_columns[attno - 1];

                    if bdesc.bd_info[attno - 1].oi_regular_nulls
                        && !check_null_keys(bval, &nullkeys[attno - 1])
                    {
                        addrange = false;
                        break;
                    }

                    if keys[attno - 1].is_empty() {
                        continue;
                    }

                    if bval.bv_allnulls {
                        addrange = false;
                        break;
                    }

                    match bdesc.bd_info[attno - 1].kind {
                        BrinOpcKind::MinMax => {
                            for key in &keys[attno - 1] {
                                addrange = brin_minmax::brin_minmax_consistent(
                                    per_range.mcx(),
                                    bdesc,
                                    bval,
                                    key,
                                )?;
                                if !addrange {
                                    break;
                                }
                            }
                        }
                        // Multi-key consistent (C fn_nargs >= 4): all keys at
                        // once, collation from the first key.
                        BrinOpcKind::MinMaxMulti => {
                            addrange = brin_minmax_multi::brin_minmax_multi_consistent(
                                per_range.mcx(),
                                bdesc,
                                bval,
                                &keys[attno - 1],
                                keys[attno - 1][0].sk_collation,
                            )?;
                        }
                    }
                    if !addrange {
                        break;
                    }
                }
                addrange
            }
        };

        if addrange {
            let last = core::cmp::min(nblocks as u64, heapBlk + opaque.bo_pagesPerRange as u64);
            for pageno in heapBlk..last {
                tbm.add_page(pageno as BlockNumber)?;
                totalpages += 1;
            }
        }

        heapBlk += opaque.bo_pagesPerRange as u64;
    }

    if buf != InvalidBuffer {
        release_buffer::call(buf)?;
    }

    Ok(totalpages * 10)
}

pub fn brinrescan(
    scan: &mut IndexScanDescData<'_>,
    scankey: Option<&[ScanKeyData]>,
) -> PgResult<()> {
    if let Some(keys) = scankey {
        if scan.numberOfKeys > 0 {
            scan.keyData.clear();
            for k in keys.iter().take(scan.numberOfKeys as usize) {
                scan.keyData.push(k.clone());
            }
        }
    }
    Ok(())
}

pub fn brinendscan(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    let IndexScanOpaque::Brin(opaque) = &mut scan.opaque else {
        unreachable!("brinendscan on non-BRIN opaque")
    };
    let rm = core::mem::replace(
        &mut opaque.bo_rmAccess,
        BrinRevmap {
            rm_pagesPerRange: 0,
            rm_lastRevmapPage: core::cell::Cell::new(0),
            rm_metaBuf: InvalidBuffer,
            rm_currBuf: core::cell::Cell::new(InvalidBuffer),
        },
    );
    release_buffer::call(rm.rm_metaBuf)?;
    if rm.rm_currBuf.get() != InvalidBuffer {
        release_buffer::call(rm.rm_currBuf.get())?;
    }
    Ok(())
}

pub fn add_values_to_range(
    bdesc: &BrinDesc<'_>,
    dtup: &mut BrinMemTuple,
    values: &[Datum],
    nulls: &[bool],
) -> PgResult<bool> {
    let empty_range_at_entry = dtup.bt_empty_range;
    let mut modified = empty_range_at_entry;

    let BrinMemTuple { bt_context, bt_columns, .. } = dtup;
    let mcx = bt_context.mcx();

    for keyno in 0..bdesc.natts() {
        let bval = &mut bt_columns[keyno];

        let has_nulls = !empty_range_at_entry && (bval.bv_hasnulls || bval.bv_allnulls);

        if bdesc.bd_info[keyno].oi_regular_nulls && nulls[keyno] {
            if !bval.bv_hasnulls {
                bval.bv_hasnulls = true;
                modified = true;
            }
            continue;
        }

        let result = match bdesc.bd_info[keyno].kind {
            BrinOpcKind::MinMax => brin_minmax::brin_minmax_add_value(
                mcx,
                bdesc,
                bval,
                values[keyno],
                nulls[keyno],
                bdesc.bd_indcollation[keyno],
            )?,
            BrinOpcKind::MinMaxMulti => brin_minmax_multi::brin_minmax_multi_add_value(
                mcx,
                bdesc,
                bval,
                values[keyno],
                nulls[keyno],
                bdesc.bd_indcollation[keyno],
            )?,
        };
        modified |= result;

        if has_nulls && !(bval.bv_hasnulls || bval.bv_allnulls) {
            debug_assert!(modified);
            bval.bv_hasnulls = true;
        }
    }

    debug_assert!(!dtup.bt_empty_range || modified);
    dtup.bt_empty_range = false;

    Ok(modified)
}

fn check_null_keys(bval: &BrinValues, nullkeys: &[&ScanKeyData]) -> bool {
    for key in nullkeys {
        debug_assert!(key.sk_attno as u16 == bval.bv_attno);
        if key.sk_flags & SK_ISNULL == 0 {
            continue;
        }
        if key.sk_flags & SK_SEARCHNULL != 0 {
            if !bval.bv_allnulls && !bval.bv_hasnulls {
                return false;
            }
        } else if key.sk_flags & SK_SEARCHNOTNULL != 0 {
            if bval.bv_allnulls {
                return false;
            }
        } else {
            return false;
        }
    }
    true
}

/// union_tuples (summarize_range's merge step; the summarize lanes are loud,
/// so this is exercised only by tests until they land).
pub fn union_tuples(bdesc: &BrinDesc<'_>, a: &mut BrinMemTuple, b: &[u8]) -> PgResult<()> {
    let mut db = brin_new_memtuple(bdesc);
    brin_deform_tuple(bdesc, b, &mut db)?;

    if db.bt_empty_range {
        return Ok(());
    }

    let BrinMemTuple { bt_context, bt_columns, bt_empty_range, .. } = a;
    let mcx = bt_context.mcx();

    if *bt_empty_range {
        for keyno in 0..bdesc.natts() {
            let col_a = &mut bt_columns[keyno];
            let col_b = &db.bt_columns[keyno];
            col_a.bv_allnulls = col_b.bv_allnulls;
            col_a.bv_hasnulls = col_b.bv_hasnulls;
            if col_b.bv_allnulls {
                continue;
            }
            for i in 0..bdesc.bd_info[keyno].oi_nstored as usize {
                let (byval, len) = disk_attr_for(bdesc, keyno, i);
                col_a.bv_values[i] = brin_tuple::datum_copy(mcx, col_b.bv_values[i], byval, len)?;
            }
        }
        *bt_empty_range = false;
        return Ok(());
    }

    for keyno in 0..bdesc.natts() {
        let col_a = &mut bt_columns[keyno];
        let col_b = &db.bt_columns[keyno];

        if bdesc.bd_info[keyno].oi_regular_nulls {
            let b_has_nulls = col_b.bv_hasnulls || col_b.bv_allnulls;
            if !col_a.bv_allnulls && b_has_nulls {
                col_a.bv_hasnulls = true;
            }
            if col_b.bv_allnulls {
                continue;
            }
            if col_a.bv_allnulls {
                col_a.bv_allnulls = false;
                col_a.bv_hasnulls = true;
                for i in 0..bdesc.bd_info[keyno].oi_nstored as usize {
                    let (byval, len) = disk_attr_for(bdesc, keyno, i);
                    col_a.bv_values[i] =
                        brin_tuple::datum_copy(mcx, col_b.bv_values[i], byval, len)?;
                }
                continue;
            }
        }

        match bdesc.bd_info[keyno].kind {
            BrinOpcKind::MinMax => brin_minmax::brin_minmax_union(
                mcx,
                bdesc,
                bdesc.bd_indcollation[keyno],
                col_a,
                col_b,
            )?,
            BrinOpcKind::MinMaxMulti => brin_minmax_multi::brin_minmax_multi_union(
                mcx,
                bdesc,
                bdesc.bd_indcollation[keyno],
                col_a,
                col_b,
            )?,
        }
    }
    Ok(())
}

fn disk_attr_for(bdesc: &BrinDesc<'_>, keyno: usize, i: usize) -> (bool, i16) {
    let mut stored = 0usize;
    for k in 0..keyno {
        stored += bdesc.bd_info[k].oi_nstored as usize;
    }
    let att = bdesc.bd_disktdesc.compact_attr(stored + i);
    (att.attbyval, att.attlen)
}

/// summarize_range/brinsummarize and the SQL entry points are loud lanes.
pub fn brinsummarize(_index: &Relation<'_>, _heapRel: &Relation<'_>) -> ! {
    unported("brinsummarize (brin.c; summarize lane)")
}

pub fn brinbulkdelete() -> ! {
    unported("brinbulkdelete (vacuum lane)")
}

pub fn brinvacuumcleanup() -> ! {
    unported("brinvacuumcleanup (vacuum lane; calls brinsummarize)")
}

pub fn brinoptions() -> ! {
    unported("brinoptions (reloptions lane)")
}
