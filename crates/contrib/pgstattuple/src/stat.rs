//! pgstattuple.c — the exact scan family.

use crate::*;
use std::rc::Rc;
use types_core::{BlockNumber, BTREE_AM_OID, GIN_AM_OID, GIST_AM_OID, HASH_AM_OID, SPGIST_AM_OID};
use types_error::ERRCODE_FEATURE_NOT_SUPPORTED;
use types_gist::GistPageIsLeaf;
use types_hash::hashpage::{LH_BUCKET_PAGE, LH_OVERFLOW_PAGE, LH_PAGE_TYPE, LH_UNUSED_PAGE};
use types_nbtree::page::{P_FIRSTDATAKEY, P_IGNORE, P_ISLEAF};
use types_rel::pg_class::{RELKIND_HAS_TABLE_AM, RELKIND_INDEX, RELKIND_SEQUENCE};
use types_scan::sdir::ScanDirection;
use types_snapshot::{SnapshotData, SnapshotType};

#[derive(Default)]
struct PgStatTuple {
    table_len: u64,
    tuple_count: u64,
    tuple_len: u64,
    dead_tuple_count: u64,
    dead_tuple_len: u64,
    free_space: u64,
}

fn build_pgstattuple_type(
    stat: &PgStatTuple,
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let (tuple_percent, dead_tuple_percent, free_percent) = if stat.table_len == 0 {
        (0.0, 0.0, 0.0)
    } else {
        (
            100.0 * stat.tuple_len as f64 / stat.table_len as f64,
            100.0 * stat.dead_tuple_len as f64 / stat.table_len as f64,
            100.0 * stat.free_space as f64 / stat.table_len as f64,
        )
    };

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let tupdesc = composite_tupdesc(mcx, flinfo)?;
    let values: Vec<Option<String>> = vec![
        Some(format!("{}", stat.table_len)),
        Some(format!("{}", stat.tuple_count)),
        Some(format!("{}", stat.tuple_len)),
        Some(format!("{tuple_percent:.2}")),
        Some(format!("{}", stat.dead_tuple_count)),
        Some(format!("{}", stat.dead_tuple_len)),
        Some(format!("{dead_tuple_percent:.2}")),
        Some(format!("{}", stat.free_space)),
        Some(format!("{free_percent:.2}")),
    ];
    let d = cstrings_composite_result(mcx, &tupdesc, &values)?;
    Ok(d)
}

fn pgstat_relation(
    rel: types_rel::Relation<'_>,
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if relation_is_other_temp(&rel) {
        return Err(other_temp_tables_err());
    }

    if RELKIND_HAS_TABLE_AM(rel.rd_rel.relkind) || rel.rd_rel.relkind == RELKIND_SEQUENCE {
        return pgstat_heap(rel, flinfo, fcinfo);
    } else if rel.rd_rel.relkind == RELKIND_INDEX {
        // see pgstatindex_impl
        if !rel.rd_index.as_ref().is_some_and(|i| i.indisvalid) {
            return Err(index_not_valid_err(rel.name()));
        }
        let err = match rel.rd_rel.relam {
            BTREE_AM_OID => return pgstat_index(rel, 1, IndexKind::Btree, flinfo, fcinfo),
            HASH_AM_OID => return pgstat_index(rel, 1, IndexKind::Hash, flinfo, fcinfo),
            GIST_AM_OID => return pgstat_index(rel, 1, IndexKind::Gist, flinfo, fcinfo),
            GIN_AM_OID => "gin index",
            SPGIST_AM_OID => "spgist index",
            types_core::BRIN_AM_OID => "brin index",
            _ => "unknown index",
        };
        Err(Box::new(
            PgError::error(format!("index \"{}\" ({}) is not supported", rel.name(), err))
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ))
    } else {
        let detail = pg_class_seams::errdetail_relkind_not_supported::call(rel.rd_rel.relkind)?;
        Err(Box::new(
            PgError::error(format!(
                "cannot get tuple-level statistics for relation \"{}\"",
                rel.name()
            ))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail(detail),
        ))
    }
}

fn pgstat_heap(
    rel: types_rel::Relation<'_>,
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // Sequences always use heap AM without showing it in the catalogs.
    if rel.rd_rel.relkind != RELKIND_SEQUENCE
        && rel.rd_rel.relam != tableam::HEAP_TABLE_AM_OID
    {
        return Err(Box::new(
            PgError::error("only heap AM is supported").with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    // Disable syncscan: the free-space chase assumes a scan from block zero.
    let snapshot_any = Rc::new(SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_ANY));
    let scan = tableam::table_beginscan_strat(
        mcx,
        &rel,
        Some(snapshot_any),
        0,
        mcx::PgVec::new_in(mcx),
        true,
        false,
    )?;
    let tableam::TableScanDesc::Heap(mut hscan) = scan else {
        unreachable!("heap AM checked above");
    };

    let mut dirty = SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_DIRTY);

    let nblocks: BlockNumber = hscan.rs_nblocks;
    let mut block: BlockNumber = 0; // next block to count free space in
    let mut stat = PgStatTuple::default();

    loop {
        let (t_len, t_self, t_table_oid, hdr) =
            match heapam::heap_getnext(&mut hscan, ScanDirection::ForwardScanDirection)? {
                Some(t) => (t.t_len, t.t_self, t.t_tableOid, t.header_ptr()),
                None => break,
            };
        postgres_seams::check_for_interrupts::call()?;

        // A buffer lock must be held to call HeapTupleSatisfiesVisibility.
        let buf = hscan.rs_cbuf.as_ref().expect("current scan buffer").buffer();
        bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_SHARE)?;
        // SAFETY: the tuple image lives in the pinned current buffer.
        let mut htup =
            unsafe { types_tuple::htup::HeapTupleData::from_raw_parts(hdr, t_len, t_self, t_table_oid) };
        if heapam_visibility::HeapTupleSatisfiesVisibility(&mut htup, &mut dirty, buf)? {
            stat.tuple_len += t_len as u64;
            stat.tuple_count += 1;
        } else {
            stat.dead_tuple_len += t_len as u64;
            stat.dead_tuple_count += 1;
        }
        bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_UNLOCK)?;

        // Chase the free-space scan alongside the heap scan.
        let tupblock: BlockNumber =
            ((t_self.ip_blkid.bi_hi as u32) << 16) | t_self.ip_blkid.bi_lo as u32;
        while block <= tupblock {
            postgres_seams::check_for_interrupts::call()?;
            let page = read_rel_page(&rel, block, &hscan.rs_strategy)?;
            stat.free_space += page_exact_free_space(&page) as u64;
            block += 1;
        }
    }

    while block < nblocks {
        postgres_seams::check_for_interrupts::call()?;
        let page = read_rel_page(&rel, block, &hscan.rs_strategy)?;
        stat.free_space += page_exact_free_space(&page) as u64;
        block += 1;
    }

    heapam::heap_endscan(hscan)?;
    rel.close(types_rel::AccessShareLock)?;

    stat.table_len = nblocks as u64 * BLCKSZ as u64;

    build_pgstattuple_type(&stat, flinfo, fcinfo)
}

#[derive(Clone, Copy)]
enum IndexKind {
    Btree,
    Hash,
    Gist,
}

fn pgstat_index_page(stat: &mut PgStatTuple, b: &[u8], minoff: usize, maxoff: usize) {
    stat.free_space += page_exact_free_space(b) as u64;

    for off in minoff..=maxoff {
        let id = page_item_id(b, off);
        if id.is_dead() {
            stat.dead_tuple_count += 1;
            stat.dead_tuple_len += id.len as u64;
        } else {
            stat.tuple_count += 1;
            stat.tuple_len += id.len as u64;
        }
    }
}

fn pgstat_one_page(stat: &mut PgStatTuple, kind: IndexKind, b: &[u8]) {
    match kind {
        IndexKind::Btree => {
            if page_is_new(b) {
                stat.free_space += BLCKSZ as u64;
            } else if page_special_size(b) as usize == maxalign(16) {
                let sp = pd_special(b) as usize;
                let o = types_nbtree::page::BTPageOpaqueData {
                    btpo_prev: r_u32(b, sp),
                    btpo_next: r_u32(b, sp + 4),
                    btpo_level: r_u32(b, sp + 8),
                    btpo_flags: r_u16(b, sp + 12),
                    btpo_cycleid: r_u16(b, sp + 14),
                };
                if P_IGNORE(&o) {
                    // deleted or half-dead page
                    stat.free_space += BLCKSZ as u64;
                } else if P_ISLEAF(&o) {
                    pgstat_index_page(stat, b, P_FIRSTDATAKEY(&o) as usize, page_max_offset_number(b));
                } else {
                    // internal page
                }
            }
        }
        IndexKind::Hash => {
            if page_is_new(b) {
                stat.free_space += BLCKSZ as u64;
            } else if page_special_size(b) as usize
                == maxalign(core::mem::size_of::<types_hash::hashpage::HashPageOpaqueData>())
            {
                let sp = pd_special(b) as usize;
                let flag = r_u16(b, sp + 12);
                match flag & LH_PAGE_TYPE {
                    LH_UNUSED_PAGE => stat.free_space += BLCKSZ as u64,
                    LH_BUCKET_PAGE | LH_OVERFLOW_PAGE => {
                        pgstat_index_page(stat, b, 1, page_max_offset_number(b));
                    }
                    // bitmap and meta pages (and anything else) count nothing
                    _ => {}
                }
            } else {
                // maybe corrupted
            }
        }
        IndexKind::Gist => {
            if page_is_new(b) {
                stat.free_space += BLCKSZ as u64;
            } else if page_special_size(b) as usize == maxalign(16) {
                // SAFETY: read view over the copied page.
                let pref = unsafe {
                    types_storage::bufpage::PageRef::from_raw(core::ptr::NonNull::from(&b[0]))
                };
                if GistPageIsLeaf(&pref) {
                    pgstat_index_page(stat, b, 1, page_max_offset_number(b));
                } else {
                    // root or node
                }
            }
        }
    }
}

fn pgstat_index(
    rel: types_rel::Relation<'_>,
    start: BlockNumber,
    kind: IndexKind,
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let bstrategy = bufmgr::GetAccessStrategy(types_storage::buf::BufferAccessStrategyType::BasBulkread);

    let mut stat = PgStatTuple::default();
    let mut blkno = start;
    loop {
        // Get the current relation length.
        lmgr::LockRelationForExtension(&rel, types_rel::ExclusiveLock)?;
        let nblocks =
            bufmgr::RelationGetNumberOfBlocksInFork(&rel, types_core::ForkNumber::MAIN_FORKNUM)?;
        lmgr::UnlockRelationForExtension(&rel, types_rel::ExclusiveLock)?;

        if blkno >= nblocks {
            stat.table_len = nblocks as u64 * BLCKSZ as u64;
            break;
        }

        while blkno < nblocks {
            postgres_seams::check_for_interrupts::call()?;
            let page = read_rel_page(&rel, blkno, &bstrategy)?;
            pgstat_one_page(&mut stat, kind, &page);
            blkno += 1;
        }
    }

    rel.close(types_rel::AccessShareLock)?;

    build_pgstattuple_type(&stat, flinfo, fcinfo)
}

pub(crate) fn fc_pgstattuple(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgstattuple: resolved FmgrInfo required");
    // Pre-1.5 installs could call this as any user; the check must stay.
    require_superuser()?;
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation_open_by_text_arg(mcx, fcinfo, 0, types_rel::AccessShareLock)?;
    pgstat_relation(rel, flinfo, fcinfo)
}

pub(crate) fn fc_pgstattuple_v1_5(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgstattuple: resolved FmgrInfo required");
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation_open_by_text_arg(mcx, fcinfo, 0, types_rel::AccessShareLock)?;
    pgstat_relation(rel, flinfo, fcinfo)
}

pub(crate) fn fc_pgstattuplebyid(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgstattuplebyid: resolved FmgrInfo required");
    require_superuser()?;
    let relid = fcinfo.arg(0).as_oid();
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation::relation_open(mcx, relid, types_rel::AccessShareLock)?;
    pgstat_relation(rel, flinfo, fcinfo)
}

pub(crate) fn fc_pgstattuplebyid_v1_5(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgstattuplebyid: resolved FmgrInfo required");
    let relid = fcinfo.arg(0).as_oid();
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation::relation_open(mcx, relid, types_rel::AccessShareLock)?;
    pgstat_relation(rel, flinfo, fcinfo)
}
