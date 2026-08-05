//! pgstatindex.c — pgstatindex, pg_relpages, pgstatginindex, pgstathashindex.

use crate::*;
use types_core::{BlockNumber, BTREE_AM_OID, GIN_AM_OID, HASH_AM_OID};
use types_error::{ERRCODE_INDEX_CORRUPTED, ERRCODE_WRONG_OBJECT_TYPE};
use types_hash::hashpage::{
    HashMetaPageData, HashPageOpaqueData, HASH_MAGIC, HASH_VERSION, LH_BITMAP_PAGE,
    LH_BUCKET_PAGE, LH_META_PAGE, LH_OVERFLOW_PAGE, LH_PAGE_TYPE, LH_UNUSED_PAGE,
};
use types_nbtree::page::{P_IGNORE, P_ISDELETED, P_ISLEAF, P_NONE};
use types_rel::pg_class::{RELKIND_HAS_STORAGE, RELKIND_INDEX};

struct BTIndexStat {
    version: u32,
    level: u32,
    root_blkno: BlockNumber,
    internal_pages: u64,
    leaf_pages: u64,
    empty_pages: u64,
    deleted_pages: u64,
    max_avail: u64,
    free_space: u64,
    fragments: u64,
}

fn pgstatindex_impl(
    rel: types_rel::Relation<'_>,
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let bstrategy =
        bufmgr::GetAccessStrategy(types_storage::buf::BufferAccessStrategyType::BasBulkread);

    if rel.rd_rel.relkind != RELKIND_INDEX || rel.rd_rel.relam != BTREE_AM_OID {
        return Err(Box::new(
            PgError::error(format!("relation \"{}\" is not a btree index", rel.name()))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }

    if relation_is_other_temp(&rel) {
        return Err(other_temp_tables_err());
    }

    // A !indisvalid index could yield ERRCODE_DATA_CORRUPTED later.
    if !rel.rd_index.as_ref().is_some_and(|i| i.indisvalid) {
        return Err(index_not_valid_err(rel.name()));
    }

    // Read the metapage.
    let meta = read_rel_page(&rel, 0, &bstrategy)?;
    let m = SizeOfPageHeaderData;
    let mut stat = BTIndexStat {
        version: r_u32(&meta, m + 4),
        level: r_u32(&meta, m + 12),
        root_blkno: r_u32(&meta, m + 8),
        internal_pages: 0,
        leaf_pages: 0,
        empty_pages: 0,
        deleted_pages: 0,
        max_avail: 0,
        free_space: 0,
        fragments: 0,
    };

    // Scan all blocks except the metapage.
    let nblocks =
        bufmgr::RelationGetNumberOfBlocksInFork(&rel, types_core::ForkNumber::MAIN_FORKNUM)?;
    for blkno in 1..nblocks {
        postgres_seams::check_for_interrupts::call()?;

        let page = read_rel_page(&rel, blkno, &bstrategy)?;
        let sp = pd_special(&page) as usize;
        let o = types_nbtree::page::BTPageOpaqueData {
            btpo_prev: r_u32(&page, sp),
            btpo_next: r_u32(&page, sp + 4),
            btpo_level: r_u32(&page, sp + 8),
            btpo_flags: r_u16(&page, sp + 12),
            btpo_cycleid: r_u16(&page, sp + 14),
        };

        // Bucket deleted pages regardless of leaf/internal.
        if P_ISDELETED(&o) {
            stat.deleted_pages += 1;
        } else if P_IGNORE(&o) {
            stat.empty_pages += 1; // the "half dead" state
        } else if P_ISLEAF(&o) {
            let max_avail = BLCKSZ - (BLCKSZ - sp + SizeOfPageHeaderData);
            stat.max_avail += max_avail as u64;
            stat.free_space += page_exact_free_space(&page) as u64;
            stat.leaf_pages += 1;

            // A next leaf on an earlier block means fragmentation.
            if o.btpo_next != P_NONE && o.btpo_next < blkno {
                stat.fragments += 1;
            }
        } else {
            stat.internal_pages += 1;
        }
    }

    rel.close(types_rel::AccessShareLock)?;

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let tupdesc = composite_tupdesc(mcx, flinfo)?;

    let index_size = (1 + stat.leaf_pages + stat.internal_pages + stat.deleted_pages
        + stat.empty_pages)
        * BLCKSZ as u64;
    let avg_leaf_density = if stat.max_avail > 0 {
        format!(
            "{:.2}",
            100.0 - stat.free_space as f64 / stat.max_avail as f64 * 100.0
        )
    } else {
        "NaN".to_string()
    };
    let leaf_fragmentation = if stat.leaf_pages > 0 {
        format!(
            "{:.2}",
            stat.fragments as f64 / stat.leaf_pages as f64 * 100.0
        )
    } else {
        "NaN".to_string()
    };

    let values: Vec<Option<String>> = vec![
        Some(format!("{}", stat.version)),
        Some(format!("{}", stat.level)),
        Some(format!("{index_size}")),
        Some(format!("{}", stat.root_blkno)),
        Some(format!("{}", stat.internal_pages)),
        Some(format!("{}", stat.leaf_pages)),
        Some(format!("{}", stat.empty_pages)),
        Some(format!("{}", stat.deleted_pages)),
        Some(avg_leaf_density),
        Some(leaf_fragmentation),
    ];
    cstrings_composite_result(mcx, &tupdesc, &values)
}

pub(crate) fn fc_pgstatindex(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgstatindex: resolved FmgrInfo required");
    require_superuser()?;
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation_open_by_text_arg(mcx, fcinfo, 0, types_rel::AccessShareLock)?;
    pgstatindex_impl(rel, flinfo, fcinfo)
}

pub(crate) fn fc_pgstatindex_v1_5(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgstatindex: resolved FmgrInfo required");
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation_open_by_text_arg(mcx, fcinfo, 0, types_rel::AccessShareLock)?;
    pgstatindex_impl(rel, flinfo, fcinfo)
}

pub(crate) fn fc_pgstatindexbyid(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgstatindexbyid: resolved FmgrInfo required");
    require_superuser()?;
    let relid = fcinfo.arg(0).as_oid();
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation::relation_open(mcx, relid, types_rel::AccessShareLock)?;
    pgstatindex_impl(rel, flinfo, fcinfo)
}

pub(crate) fn fc_pgstatindexbyid_v1_5(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgstatindexbyid: resolved FmgrInfo required");
    let relid = fcinfo.arg(0).as_oid();
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation::relation_open(mcx, relid, types_rel::AccessShareLock)?;
    pgstatindex_impl(rel, flinfo, fcinfo)
}

fn pg_relpages_impl(rel: types_rel::Relation<'_>) -> PgResult<Datum> {
    if !RELKIND_HAS_STORAGE(rel.rd_rel.relkind) {
        let detail = pg_class_seams::errdetail_relkind_not_supported::call(rel.rd_rel.relkind)?;
        return Err(Box::new(
            PgError::error(format!("cannot get page count of relation \"{}\"", rel.name()))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
                .with_detail(detail),
        ));
    }

    // Works OK on non-local temp tables.
    let relpages =
        bufmgr::RelationGetNumberOfBlocksInFork(&rel, types_core::ForkNumber::MAIN_FORKNUM)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(Datum::from_i64(relpages as i64))
}

pub(crate) fn fc_pg_relpages(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    require_superuser()?;
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation_open_by_text_arg(mcx, fcinfo, 0, types_rel::AccessShareLock)?;
    pg_relpages_impl(rel)
}

pub(crate) fn fc_pg_relpages_v1_5(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation_open_by_text_arg(mcx, fcinfo, 0, types_rel::AccessShareLock)?;
    pg_relpages_impl(rel)
}

pub(crate) fn fc_pg_relpagesbyid(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    require_superuser()?;
    let relid = fcinfo.arg(0).as_oid();
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation::relation_open(mcx, relid, types_rel::AccessShareLock)?;
    pg_relpages_impl(rel)
}

pub(crate) fn fc_pg_relpagesbyid_v1_5(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let relid = fcinfo.arg(0).as_oid();
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation::relation_open(mcx, relid, types_rel::AccessShareLock)?;
    pg_relpages_impl(rel)
}

fn pgstatginindex_internal(
    relid: types_core::Oid,
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation::relation_open(mcx, relid, types_rel::AccessShareLock)?;

    if rel.rd_rel.relkind != RELKIND_INDEX || rel.rd_rel.relam != GIN_AM_OID {
        return Err(Box::new(
            PgError::error(format!("relation \"{}\" is not a GIN index", rel.name()))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }
    if relation_is_other_temp(&rel) {
        return Err(other_temp_indexes_err());
    }
    if !rel.rd_index.as_ref().is_some_and(|i| i.indisvalid) {
        return Err(index_not_valid_err(rel.name()));
    }

    // Read the metapage (GIN_METAPAGE_BLKNO = 0).
    let page = read_rel_page(&rel, 0, &None)?;
    let m = SizeOfPageHeaderData;
    let version = r_u32(&page, m + 48) as i32;
    let pending_pages = r_u32(&page, m + 12);
    let pending_tuples = i64::from_ne_bytes(page[m + 16..m + 24].try_into().unwrap());

    rel.close(types_rel::AccessShareLock)?;

    let tupdesc = composite_tupdesc(mcx, flinfo)?;
    let values = [
        Datum::from_i32(version),
        Datum::from_i32(pending_pages as i32),
        Datum::from_i64(pending_tuples),
    ];
    composite_result(mcx, &tupdesc, &values, &[false; 3])
}

pub(crate) fn fc_pgstatginindex(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgstatginindex: resolved FmgrInfo required");
    require_superuser()?;
    let relid = fcinfo.arg(0).as_oid();
    pgstatginindex_internal(relid, flinfo, fcinfo)
}

pub(crate) fn fc_pgstatginindex_v1_5(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgstatginindex: resolved FmgrInfo required");
    let relid = fcinfo.arg(0).as_oid();
    pgstatginindex_internal(relid, flinfo, fcinfo)
}

#[derive(Default)]
struct HashIndexStat {
    version: i32,
    space_per_page: i32,
    bucket_pages: u32,
    overflow_pages: u32,
    bitmap_pages: u32,
    unused_pages: u32,
    live_items: i64,
    dead_items: i64,
    free_space: u64,
}

fn hash_page_stats_into(b: &[u8], stats: &mut HashIndexStat) {
    for off in 1..=page_max_offset_number(b) {
        if page_item_id(b, off).is_dead() {
            stats.dead_items += 1;
        } else {
            stats.live_items += 1;
        }
    }
    stats.free_space += page_exact_free_space(b) as u64;
}

pub(crate) fn fc_pgstathashindex(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgstathashindex: resolved FmgrInfo required");
    let relid = fcinfo.arg(0).as_oid();

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation::relation_open(mcx, relid, types_rel::AccessShareLock)?;

    if rel.rd_rel.relkind != RELKIND_INDEX || rel.rd_rel.relam != HASH_AM_OID {
        return Err(Box::new(
            PgError::error(format!("relation \"{}\" is not a hash index", rel.name()))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }
    if relation_is_other_temp(&rel) {
        return Err(other_temp_indexes_err());
    }
    if !rel.rd_index.as_ref().is_some_and(|i| i.indisvalid) {
        return Err(index_not_valid_err(rel.name()));
    }

    // Metapage (block 0), with _hash_checkpage's meta checks.
    let mut stats = HashIndexStat::default();
    {
        let meta_page = read_rel_page(&rel, 0, &None)?;
        // _hash_checkpage(LH_META_PAGE): all-zero page, then special-area size,
        // then the page-type flag bit, before magic/version.
        if page_is_new(&meta_page) {
            return Err(Box::new(
                PgError::error(format!(
                    "index \"{}\" contains unexpected zero page at block 0",
                    rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_hint("Please REINDEX it."),
            ));
        }
        if page_special_size(&meta_page) as usize
            != maxalign(core::mem::size_of::<HashPageOpaqueData>())
        {
            return Err(Box::new(
                PgError::error(format!(
                    "index \"{}\" contains corrupted page at block 0",
                    rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_hint("Please REINDEX it."),
            ));
        }
        if r_u16(&meta_page, pd_special(&meta_page) as usize + 12) & LH_META_PAGE == 0 {
            return Err(Box::new(
                PgError::error(format!(
                    "index \"{}\" contains corrupted page at block 0",
                    rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_hint("Please REINDEX it."),
            ));
        }
        // SAFETY: repr(C) metapage struct fits in the page.
        let metap = unsafe {
            meta_page
                .as_ptr()
                .add(SizeOfPageHeaderData)
                .cast::<HashMetaPageData>()
                .read_unaligned()
        };
        if metap.hashm_magic != HASH_MAGIC {
            return Err(Box::new(
                PgError::error(format!("index \"{}\" is not a hash index", rel.name()))
                    .with_sqlstate(ERRCODE_INDEX_CORRUPTED),
            ));
        }
        if metap.hashm_version != HASH_VERSION {
            return Err(Box::new(
                PgError::error(format!("index \"{}\" has wrong hash version", rel.name()))
                    .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                    .with_hint("Please REINDEX it."),
            ));
        }
        stats.version = metap.hashm_version as i32;
        stats.space_per_page = metap.hashm_bsize as i32;
    }

    let nblocks =
        bufmgr::RelationGetNumberOfBlocksInFork(&rel, types_core::ForkNumber::MAIN_FORKNUM)?;
    let bstrategy =
        bufmgr::GetAccessStrategy(types_storage::buf::BufferAccessStrategyType::BasBulkread);

    // Block 0 is the metapage.
    for blkno in 1..nblocks {
        postgres_seams::check_for_interrupts::call()?;

        let page = read_rel_page(&rel, blkno, &bstrategy)?;
        if page_is_new(&page) {
            stats.unused_pages += 1;
        } else if page_special_size(&page) as usize
            != maxalign(core::mem::size_of::<HashPageOpaqueData>())
        {
            return Err(Box::new(
                PgError::error(format!(
                    "index \"{}\" contains corrupted page at block {}",
                    rel.name(),
                    blkno
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED),
            ));
        } else {
            let sp = pd_special(&page) as usize;
            let flag = r_u16(&page, sp + 12);
            match flag & LH_PAGE_TYPE {
                LH_BUCKET_PAGE => {
                    stats.bucket_pages += 1;
                    hash_page_stats_into(&page, &mut stats);
                }
                LH_OVERFLOW_PAGE => {
                    stats.overflow_pages += 1;
                    hash_page_stats_into(&page, &mut stats);
                }
                LH_BITMAP_PAGE => stats.bitmap_pages += 1,
                LH_UNUSED_PAGE => stats.unused_pages += 1,
                _ => {
                    return Err(Box::new(
                        PgError::error(format!(
                            "unexpected page type 0x{:04X} in HASH index \"{}\" block {}",
                            flag,
                            rel.name(),
                            blkno
                        ))
                        .with_sqlstate(ERRCODE_INDEX_CORRUPTED),
                    ))
                }
            }
        }
    }

    rel.close(types_rel::AccessShareLock)?;

    // Unused pages count as free space; total space excludes the metapage
    // and bitmap pages.
    stats.free_space += stats.unused_pages as u64 * stats.space_per_page as u64;
    let total_space =
        (nblocks as u64 - (stats.bitmap_pages as u64 + 1)) * stats.space_per_page as u64;
    let free_percent = if total_space == 0 {
        0.0
    } else {
        100.0 * stats.free_space as f64 / total_space as f64
    };

    let tupdesc = composite_tupdesc(mcx, flinfo)?;
    let values = [
        Datum::from_i32(stats.version),
        Datum::from_i64(stats.bucket_pages as i64),
        Datum::from_i64(stats.overflow_pages as i64),
        Datum::from_i64(stats.bitmap_pages as i64),
        Datum::from_i64(stats.unused_pages as i64),
        Datum::from_i64(stats.live_items),
        Datum::from_i64(stats.dead_items),
        Datum::from_f64(free_percent),
    ];
    composite_result(mcx, &tupdesc, &values, &[false; 8])
}
