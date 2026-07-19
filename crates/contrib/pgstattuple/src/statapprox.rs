//! pgstatapprox.c — pgstattuple_approx over the visibility and free space
//! maps.

use crate::*;
use types_error::ERRCODE_FEATURE_NOT_SUPPORTED;
use types_rel::pg_class::{RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_TOASTVALUE};
use types_snapshot::HTSV_Result;
use types_tuple::itemptr::ItemPointerData;

#[derive(Default)]
struct OutputType {
    table_len: u64,
    scanned_percent: f64,
    tuple_count: u64,
    tuple_len: u64,
    tuple_percent: f64,
    dead_tuple_count: u64,
    dead_tuple_len: u64,
    dead_tuple_percent: f64,
    free_space: u64,
    free_percent: f64,
}

/// Loosely lazy_scan_heap: skip all-visible pages via the VM, approximating
/// their tuple_len from the FSM; scan the rest exactly.
fn statapprox_heap(rel: &types_rel::RelationData<'_>, stat: &mut OutputType) -> PgResult<()> {
    let oldest_xmin = procarray::GetOldestNonRemovableTransactionId(rel)?;
    let bstrategy =
        bufmgr::GetAccessStrategy(types_storage::buf::BufferAccessStrategyType::BasBulkread);

    let nblocks =
        bufmgr::RelationGetNumberOfBlocksInFork(rel, types_core::ForkNumber::MAIN_FORKNUM)?;
    let mut scanned: u32 = 0;
    let mut vmbuffer = visibilitymap::VmBuffer::new();

    for blkno in 0..nblocks {
        postgres_seams::check_for_interrupts::call()?;

        // All-visible page: take the free space from the FSM and move on.
        if visibilitymap::vm_all_visible(rel, blkno, &mut vmbuffer)? {
            let freespace = freespace::GetRecordedFreeSpace(rel, blkno)?;
            stat.tuple_len += (BLCKSZ - freespace) as u64;
            stat.free_space += freespace as u64;
            continue;
        }

        let buf = bufmgr::ReadBufferExtended(
            rel,
            types_core::ForkNumber::MAIN_FORKNUM,
            blkno,
            types_storage::storage::ReadBufferMode::Normal,
            bstrategy.clone(),
        )?;
        bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_SHARE)?;

        let pageptr = bufmgr::BufferGetPagePtr(buf);
        // SAFETY: a locked, pinned buffer page is BLCKSZ readable; HTSV hint
        // writes go through the tuple pointer under the same lock, as in C.
        let b: &[u8] = unsafe { core::slice::from_raw_parts(pageptr.as_ptr(), BLCKSZ) };

        stat.free_space += page_exact_free_space(b) as u64;

        // The page counts as scanned even when new/empty.
        scanned += 1;

        if page_is_new(b) || page_is_empty(b) {
            bufmgr::UnlockReleaseBuffer(buf)?;
            continue;
        }

        for offnum in 1..=page_max_offset_number(b) {
            let itemid = page_item_id(b, offnum);
            if !itemid.is_used() || itemid.is_redirected() || itemid.is_dead() {
                continue;
            }
            debug_assert!(itemid.is_normal());

            let lp_off = itemid.off as usize;
            let lp_len = itemid.len as u32;
            if lp_off + lp_len as usize > BLCKSZ || lp_len < 23 {
                continue; // corrupt line pointer; C would overread
            }

            // SAFETY: the tuple image lives in the locked, pinned buffer.
            let mut tuple = unsafe {
                types_tuple::htup::HeapTupleData::from_raw_parts(
                    pageptr.as_ptr().add(lp_off),
                    lp_len,
                    ItemPointerData::new(blkno, offnum as u16),
                    rel.rd_id,
                )
            };

            // Follow VACUUM: INSERT_IN_PROGRESS counts dead,
            // DELETE_IN_PROGRESS counts live.
            match heapam_visibility::HeapTupleSatisfiesVacuum(&mut tuple, oldest_xmin, buf)? {
                HTSV_Result::HEAPTUPLE_LIVE | HTSV_Result::HEAPTUPLE_DELETE_IN_PROGRESS => {
                    stat.tuple_len += lp_len as u64;
                    stat.tuple_count += 1;
                }
                HTSV_Result::HEAPTUPLE_DEAD
                | HTSV_Result::HEAPTUPLE_RECENTLY_DEAD
                | HTSV_Result::HEAPTUPLE_INSERT_IN_PROGRESS => {
                    stat.dead_tuple_len += lp_len as u64;
                    stat.dead_tuple_count += 1;
                }
            }
        }

        bufmgr::UnlockReleaseBuffer(buf)?;
    }

    stat.table_len = nblocks as u64 * BLCKSZ as u64;

    // Extrapolate the live-tuple count the way VACUUM does.
    let estimated = commands_vacuum::vac_estimate_reltuples(rel, nblocks, scanned, stat.tuple_count as f64);
    stat.tuple_count = if estimated < 0.0 { 0 } else { estimated as u64 };

    if nblocks != 0 {
        stat.scanned_percent = 100.0 * scanned as f64 / nblocks as f64;
        stat.tuple_percent = 100.0 * stat.tuple_len as f64 / stat.table_len as f64;
        stat.dead_tuple_percent = 100.0 * stat.dead_tuple_len as f64 / stat.table_len as f64;
        stat.free_percent = 100.0 * stat.free_space as f64 / stat.table_len as f64;
    }

    vmbuffer.release();
    Ok(())
}

fn pgstattuple_approx_internal(
    relid: types_core::Oid,
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let tupdesc = composite_tupdesc(mcx, flinfo)?;
    if tupdesc.natts != 10 {
        return Err(Box::new(PgError::error("incorrect number of output arguments")));
    }

    let rel = relation::relation_open(mcx, relid, types_rel::AccessShareLock)?;

    if relation_is_other_temp(&rel) {
        return Err(other_temp_tables_err());
    }

    // Only relation kinds with a visibility map and a free space map.
    if !(rel.rd_rel.relkind == RELKIND_RELATION
        || rel.rd_rel.relkind == RELKIND_MATVIEW
        || rel.rd_rel.relkind == RELKIND_TOASTVALUE)
    {
        let detail = pg_class_seams::errdetail_relkind_not_supported::call(rel.rd_rel.relkind)?;
        return Err(Box::new(
            PgError::error(format!(
                "relation \"{}\" is of wrong relation kind",
                rel.name()
            ))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail(detail),
        ));
    }

    if rel.rd_rel.relam != tableam::HEAP_TABLE_AM_OID {
        return Err(Box::new(
            PgError::error("only heap AM is supported").with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    let mut stat = OutputType::default();
    statapprox_heap(&rel, &mut stat)?;

    rel.close(types_rel::AccessShareLock)?;

    let values = [
        Datum::from_i64(stat.table_len as i64),
        Datum::from_f64(stat.scanned_percent),
        Datum::from_i64(stat.tuple_count as i64),
        Datum::from_i64(stat.tuple_len as i64),
        Datum::from_f64(stat.tuple_percent),
        Datum::from_i64(stat.dead_tuple_count as i64),
        Datum::from_i64(stat.dead_tuple_len as i64),
        Datum::from_f64(stat.dead_tuple_percent),
        Datum::from_i64(stat.free_space as i64),
        Datum::from_f64(stat.free_percent),
    ];
    composite_result(mcx, &tupdesc, &values, &[false; 10])
}

pub(crate) fn fc_pgstattuple_approx(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgstattuple_approx: resolved FmgrInfo required");
    require_superuser()?;
    let relid = fcinfo.arg(0).as_oid();
    pgstattuple_approx_internal(relid, flinfo, fcinfo)
}

pub(crate) fn fc_pgstattuple_approx_v1_5(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgstattuple_approx: resolved FmgrInfo required");
    let relid = fcinfo.arg(0).as_oid();
    pgstattuple_approx_internal(relid, flinfo, fcinfo)
}
