//! btreefuncs.c — bt_metap, bt_page_stats, bt_multi_page_stats, bt_page_items.

use crate::*;
use types_core::{BlockNumber, MaxBlockNumber, BTREE_AM_OID};
use types_error::ERRCODE_WRONG_OBJECT_TYPE;
use types_nbtree::page::{
    P_HAS_FULLXID, P_HIKEY, P_IGNORE, P_ISDELETED, P_ISLEAF, P_ISMETA, P_ISROOT, P_RIGHTMOST,
};
use types_rel::pg_class::RELKIND_INDEX;

pub(crate) struct BtOpaque {
    pub prev: u32,
    pub next: u32,
    pub level: u32,
    pub flags: u16,
    pub cycleid: u16,
}

pub(crate) fn bt_opaque(b: &[u8]) -> BtOpaque {
    let sp = pd_special(b) as usize;
    if sp + 16 > b.len() {
        return BtOpaque { prev: 0, next: 0, level: 0, flags: 0, cycleid: 0 };
    }
    BtOpaque {
        prev: r_u32(b, sp),
        next: r_u32(b, sp + 4),
        level: r_u32(b, sp + 8),
        flags: r_u16(b, sp + 12),
        cycleid: r_u16(b, sp + 14),
    }
}

fn opaque_data(o: &BtOpaque) -> types_nbtree::page::BTPageOpaqueData {
    types_nbtree::page::BTPageOpaqueData {
        btpo_prev: o.prev,
        btpo_next: o.next,
        btpo_level: o.level,
        btpo_flags: o.flags,
        btpo_cycleid: o.cycleid,
    }
}

struct BTPageStat {
    blkno: u32,
    live_items: u32,
    dead_items: u32,
    page_size: u32,
    free_size: u32,
    avg_item_size: u32,
    typ: char,
    btpo_prev: u32,
    btpo_next: u32,
    btpo_level: u32,
    btpo_flags: u16,
    btpo_cycleid: u16,
}

fn get_bt_page_statistics(blkno: BlockNumber, b: &[u8]) -> BTPageStat {
    let mut maxoff = page_max_offset_number(b);
    let o = opaque_data(&bt_opaque(b));

    let typ = if P_ISDELETED(&o) {
        // Deleted pages split into leaf ('d') and internal ('D').
        maxoff = 0; // don't interpret BTDeletedPageData as index tuples
        if P_ISLEAF(&o) || !P_HAS_FULLXID(&o) {
            'd'
        } else {
            'D'
        }
    } else if P_IGNORE(&o) {
        'e'
    } else if P_ISLEAF(&o) {
        'l'
    } else if P_ISROOT(&o) {
        'r'
    } else {
        'i'
    };

    let mut live_items = 0u32;
    let mut dead_items = 0u32;
    let mut item_size = 0usize;
    for off in 1..=maxoff {
        let id = page_item_id(b, off);
        let pos = id.off as usize;
        if pos + 8 <= b.len() {
            // SAFETY: t_info read bounded above.
            item_size += unsafe { nbtree::itup::index_tuple_size(b.as_ptr().add(pos)) };
        }
        if !id.is_dead() {
            live_items += 1;
        } else {
            dead_items += 1;
        }
    }

    BTPageStat {
        blkno,
        live_items,
        dead_items,
        page_size: page_size(b) as u32,
        free_size: page_free_space(b) as u32,
        avg_item_size: if live_items + dead_items > 0 {
            item_size as u32 / (live_items + dead_items)
        } else {
            0
        },
        typ,
        btpo_prev: o.btpo_prev,
        btpo_next: o.btpo_next,
        btpo_level: o.btpo_level,
        btpo_flags: o.btpo_flags,
        btpo_cycleid: o.btpo_cycleid,
    }
}

fn stat_cstrings(stat: &BTPageStat) -> Vec<Option<String>> {
    let _ = stat.btpo_cycleid;
    vec![
        Some(format!("{}", stat.blkno)),
        Some(format!("{}", stat.typ)),
        Some(format!("{}", stat.live_items)),
        Some(format!("{}", stat.dead_items)),
        Some(format!("{}", stat.avg_item_size)),
        Some(format!("{}", stat.page_size)),
        Some(format!("{}", stat.free_size)),
        Some(format!("{}", stat.btpo_prev)),
        Some(format!("{}", stat.btpo_next)),
        Some(format!("{}", stat.btpo_level)),
        Some(format!("{}", stat.btpo_flags)),
    ]
}

fn check_relation_block_range(rel: &types_rel::RelationData<'_>, blkno: i64) -> PgResult<()> {
    if blkno < 0 || blkno > MaxBlockNumber as i64 {
        return Err(param_err(format!("invalid block number {blkno}")));
    }
    if blkno as BlockNumber
        >= bufmgr::RelationGetNumberOfBlocksInFork(rel, types_core::ForkNumber::MAIN_FORKNUM)?
    {
        return Err(param_err(format!("block number {blkno} is out of range")));
    }
    Ok(())
}

fn bt_index_block_validate(rel: &types_rel::RelationData<'_>, blkno: i64) -> PgResult<()> {
    if rel.rd_rel.relkind != RELKIND_INDEX || rel.rd_rel.relam != BTREE_AM_OID {
        return Err(Box::new(
            PgError::error(format!("\"{}\" is not a {} index", rel.name(), "btree"))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }
    if relation_is_other_temp(rel) {
        return Err(other_temp_err());
    }
    if blkno == 0 {
        return Err(param_err("block 0 is a meta page"));
    }
    check_relation_block_range(rel, blkno)
}

fn bt_page_stats_internal(
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
    version: PiVersion,
) -> PgResult<Datum> {
    require_superuser("pageinspect")?;

    let blkno: i64 = if version == PiVersion::V1_8 {
        fcinfo.arg(1).as_i32() as u32 as i64
    } else {
        fcinfo.arg(1).as_i64()
    };

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation_open_by_text_arg(mcx, fcinfo, 0, types_rel::AccessShareLock)?;
    bt_index_block_validate(&rel, blkno)?;

    let page = read_rel_page(&rel, blkno as BlockNumber)?;
    rel.close(types_rel::AccessShareLock)?;

    let stat = get_bt_page_statistics(blkno as BlockNumber, page.bytes());

    let tupdesc = composite_tupdesc(mcx, flinfo)?;
    cstrings_composite_result(mcx, &tupdesc, &stat_cstrings(&stat))
}

pub(crate) fn fc_bt_page_stats_1_9(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("bt_page_stats: resolved FmgrInfo required");
    bt_page_stats_internal(flinfo, fcinfo, PiVersion::V1_9)
}

pub(crate) fn fc_bt_page_stats(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("bt_page_stats: resolved FmgrInfo required");
    bt_page_stats_internal(flinfo, fcinfo, PiVersion::V1_8)
}

pub(crate) fn fc_bt_multi_page_stats(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("bt_multi_page_stats: resolved FmgrInfo required");
    require_superuser("pageinspect")?;

    let rows = if !flinfo.has_fn_extra() {
        // SAFETY: the arming context outlives this call.
        let mcx = unsafe { fcinfo.result_mcx_detached() };
        let blkno = fcinfo.arg(1).as_i64();
        let blk_count = fcinfo.arg(2).as_i64();

        let rel = relation_open_by_text_arg(mcx, fcinfo, 0, types_rel::AccessShareLock)?;
        bt_index_block_validate(&rel, blkno)?;
        if blk_count > 1 {
            check_relation_block_range(&rel, blkno + blk_count - 1)?;
        }

        let nblocks =
            bufmgr::RelationGetNumberOfBlocksInFork(&rel, types_core::ForkNumber::MAIN_FORKNUM)?
                as i64;
        let count = if blk_count < 0 { nblocks - blkno } else { blk_count };

        let tupdesc = composite_tupdesc(mcx, flinfo)?;
        let mut rows = Vec::new();
        for i in 0..count {
            postgres_seams::check_for_interrupts::call()?;
            let cur = blkno + i;
            if cur as BlockNumber >= nblocks as BlockNumber {
                break;
            }
            let page = read_rel_page(&rel, cur as BlockNumber)?;
            let stat = get_bt_page_statistics(cur as BlockNumber, page.bytes());
            rows.push(cstrings_tuple_image(mcx, &tupdesc, &stat_cstrings(&stat))?);
        }
        rel.close(types_rel::AccessShareLock)?;
        Some(rows)
    } else {
        None
    };
    srf_stream(flinfo, fcinfo, rows)
}

/// bt_page_print_tuples: one output row for the index tuple at `offnum`.
fn bt_page_print_tuple(
    mcx: Mcx<'_>,
    tupdesc: &TupleDescData<'_>,
    b: &[u8],
    offnum: usize,
    leafpage: bool,
    rightmost: bool,
) -> PgResult<Vec<u8>> {
    let id = page_item_id(b, offnum);
    if !id.is_valid() {
        return Err(Box::new(PgError::error("invalid ItemId")));
    }
    let pos = id.off as usize;
    if pos + 8 > b.len() {
        return Err(Box::new(PgError::error("invalid ItemId")));
    }
    // SAFETY: itup header reads bounded above; data reads bounded below.
    let itup = unsafe { b.as_ptr().add(pos) };
    let size = unsafe { nbtree::itup::index_tuple_size(itup) };
    if pos + size > b.len() || size < 8 {
        return Err(Box::new(PgError::error(format!(
            "invalid tuple length {size} for tuple at offset number {offnum}"
        ))));
    }

    let natts = tupdesc.natts as usize;
    let mut values = vec![Datum::null(); natts];
    let mut nulls = vec![false; natts];
    let mut j = 0usize;

    let t_info = unsafe { nbtree::itup::t_info(itup) };

    values[j] = Datum::from_i16(offnum as i16);
    j += 1;
    values[j] = tid_datum(mcx, &b[pos..pos + 6])?;
    j += 1;
    values[j] = Datum::from_i32(size as i32);
    j += 1;
    values[j] = Datum::from_bool(t_info & nbtree::itup::INDEX_NULL_MASK != 0);
    j += 1;
    values[j] = Datum::from_bool(t_info & nbtree::itup::INDEX_VAR_MASK != 0);
    j += 1;

    let data_off = nbtree::itup::index_info_find_data_offset(t_info);
    let mut dlen = size as isize - data_off as isize;

    // Exclude posting lists and pivot heap TIDs from the data column.
    let is_posting = unsafe { nbtree::itup::bt_tuple_is_posting(itup) };
    let is_pivot = unsafe { nbtree::itup::bt_tuple_is_pivot(itup) };
    if is_posting {
        let posting_off = unsafe { nbtree::itup::bt_tuple_get_posting_offset(itup) };
        dlen -= size as isize - posting_off as isize;
    } else if is_pivot && unsafe { nbtree::itup::bt_tuple_get_heap_tid(itup) }.is_some() {
        dlen -= 8; // MAXALIGN(sizeof(ItemPointerData))
    }

    if dlen < 0 || dlen as usize > nbtree::itup::INDEX_SIZE_MASK as usize {
        return Err(Box::new(PgError::error(format!(
            "invalid tuple length {dlen} for tuple at offset number {offnum}"
        ))));
    }
    let dlen = dlen as usize;
    if pos + data_off + dlen > b.len() {
        return Err(Box::new(PgError::error(format!(
            "invalid tuple length {dlen} for tuple at offset number {offnum}"
        ))));
    }
    let mut dump = String::with_capacity(dlen * 3);
    for (k, byte) in b[pos + data_off..pos + data_off + dlen].iter().enumerate() {
        if k > 0 {
            dump.push(' ');
        }
        dump.push_str(&format!("{byte:02x}"));
    }
    values[j] = text_datum(mcx, dump.as_bytes())?;
    j += 1;

    if j < natts {
        // 1.9 columns: dead, htid, tids.
        let ispivottuple = !leafpage || (!rightmost && offnum == P_HIKEY as usize);

        if !ispivottuple {
            values[j] = Datum::from_bool(id.is_dead());
        } else {
            nulls[j] = true;
        }
        j += 1;

        let mut htid = unsafe { nbtree::itup::bt_tuple_get_heap_tid(itup) };
        if ispivottuple && !is_pivot {
            // Don't show bogus heap TID in !heapkeyspace pivot tuple.
            htid = None;
        }
        match htid {
            Some(ip) => values[j] = tid_datum(mcx, &tid_bytes(ip))?,
            None => nulls[j] = true,
        }
        j += 1;

        if is_posting {
            let nposting = unsafe { nbtree::itup::bt_tuple_get_nposting(itup) };
            let mut tids = Vec::with_capacity(nposting);
            for n in 0..nposting {
                let ip = unsafe { nbtree::itup::bt_tuple_get_posting_n(itup, n) };
                tids.push(tid_datum(mcx, &tid_bytes(ip))?);
            }
            values[j] = tid_array_datum(mcx, &tids)?;
        } else {
            nulls[j] = true;
        }
        j += 1;
    }

    debug_assert!(j == natts);
    tuple_image(mcx, tupdesc, &values, &nulls)
}

fn bt_page_items_rows(
    mcx: Mcx<'_>,
    flinfo: &FmgrInfo,
    page: &RawPage,
    deleted_notice: &str,
) -> PgResult<Vec<Vec<u8>>> {
    let b = page.bytes();
    let o = opaque_data(&bt_opaque(b));
    let leafpage = P_ISLEAF(&o);
    let rightmost = P_RIGHTMOST(&o);

    let maxoff = if !P_ISDELETED(&o) {
        page_max_offset_number(b)
    } else {
        // Don't interpret BTDeletedPageData as index tuples.
        notice(deleted_notice)?;
        0
    };

    let tupdesc = composite_tupdesc(mcx, flinfo)?;
    let mut rows = Vec::with_capacity(maxoff);
    for offnum in 1..=maxoff {
        rows.push(bt_page_print_tuple(mcx, &tupdesc, b, offnum, leafpage, rightmost)?);
    }
    Ok(rows)
}

fn bt_page_items_internal(
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
    version: PiVersion,
) -> PgResult<Datum> {
    require_superuser("pageinspect")?;

    let rows = if !flinfo.has_fn_extra() {
        // SAFETY: the arming context outlives this call.
        let mcx = unsafe { fcinfo.result_mcx_detached() };
        let blkno: i64 = if version == PiVersion::V1_8 {
            fcinfo.arg(1).as_i32() as u32 as i64
        } else {
            fcinfo.arg(1).as_i64()
        };
        let rel = relation_open_by_text_arg(mcx, fcinfo, 0, types_rel::AccessShareLock)?;
        bt_index_block_validate(&rel, blkno)?;
        let page = read_rel_page(&rel, blkno as BlockNumber)?;
        rel.close(types_rel::AccessShareLock)?;
        Some(bt_page_items_rows(
            mcx,
            flinfo,
            &page,
            &format!("page from block {blkno} is deleted"),
        )?)
    } else {
        None
    };
    srf_stream(flinfo, fcinfo, rows)
}

pub(crate) fn fc_bt_page_items_1_9(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("bt_page_items: resolved FmgrInfo required");
    bt_page_items_internal(flinfo, fcinfo, PiVersion::V1_9)
}

pub(crate) fn fc_bt_page_items(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("bt_page_items: resolved FmgrInfo required");
    bt_page_items_internal(flinfo, fcinfo, PiVersion::V1_8)
}

pub(crate) fn fc_bt_page_items_bytea(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("bt_page_items_bytea: resolved FmgrInfo required");
    require_superuser("raw page")?;

    let rows = if !flinfo.has_fn_extra() {
        // SAFETY: the arming context outlives this call.
        let mcx = unsafe { fcinfo.result_mcx_detached() };
        let page = RawPage::arg(fcinfo, 0)?;
        let b = page.bytes();

        if page_is_new(b) {
            return Ok(fcinfo.return_null());
        }

        if page_special_size(b) as usize != maxalign(16) {
            return Err(Box::new(
                PgError::error(format!("input page is not a valid {} page", "btree"))
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                    .with_detail(format!(
                        "Expected special size {}, got {}.",
                        maxalign(16),
                        page_special_size(b)
                    )),
            ));
        }

        let o = opaque_data(&bt_opaque(b));
        if P_ISMETA(&o) {
            return Err(param_err("block is a meta page"));
        }
        if P_ISLEAF(&o) && o.btpo_level != 0 {
            return Err(param_err("block is not a valid btree leaf page"));
        }
        if P_ISDELETED(&o) {
            notice("page is deleted")?;
        }

        Some(bt_page_items_rows(mcx, flinfo, &page, "page from block is deleted")?)
    } else {
        None
    };
    srf_stream(flinfo, fcinfo, rows)
}

pub(crate) fn fc_bt_metap(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("bt_metap: resolved FmgrInfo required");
    require_superuser("pageinspect")?;

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation_open_by_text_arg(mcx, fcinfo, 0, types_rel::AccessShareLock)?;

    if rel.rd_rel.relkind != RELKIND_INDEX || rel.rd_rel.relam != BTREE_AM_OID {
        return Err(Box::new(
            PgError::error(format!("\"{}\" is not a {} index", rel.name(), "btree"))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }
    if relation_is_other_temp(&rel) {
        return Err(other_temp_err());
    }

    let page = read_rel_page(&rel, 0)?;
    rel.close(types_rel::AccessShareLock)?;
    let b = page.bytes();

    // BTMetaPageData at PageGetContents.
    let m = SizeOfPageHeaderData;
    let btm_magic = r_u32(b, m);
    let btm_version = r_u32(b, m + 4);
    let btm_root = r_u32(b, m + 8);
    let btm_level = r_u32(b, m + 12);
    let btm_fastroot = r_u32(b, m + 16);
    let btm_fastlevel = r_u32(b, m + 20);
    let btm_last_cleanup_num_delpages = r_u32(b, m + 24);
    let btm_last_cleanup_num_heap_tuples = f64::from_bits(r_u64(b, m + 32));
    let btm_allequalimage = b[m + 40] != 0;

    let tupdesc = composite_tupdesc(mcx, flinfo)?;

    // Versions before 1.8 used int4 for columns the C code prints as int64;
    // insist on the 1.8+ definition, as C does.
    if (tupdesc.natts as usize) < 9 {
        return Err(Box::new(
            PgError::error("function has wrong number of declared columns")
                .with_sqlstate(types_error::ERRCODE_INVALID_FUNCTION_DEFINITION)
                .with_hint(
                    "To resolve the problem, update the \"pageinspect\" extension to the latest version.",
                ),
        ));
    }

    let mut values: Vec<Option<String>> = vec![
        Some(format!("{}", btm_magic as i32)),
        Some(format!("{}", btm_version as i32)),
        Some(format!("{}", btm_root as i64)),
        Some(format!("{}", btm_level as i64)),
        Some(format!("{}", btm_fastroot as i64)),
        Some(format!("{}", btm_fastlevel as i64)),
    ];
    // Extended metadata is only valid on version-3+ meta pages
    // (_bt_metaversion's zero-initialized assumption for older ones).
    if btm_version >= types_nbtree::page::BTREE_NOVAC_VERSION {
        values.push(Some(format!("{}", btm_last_cleanup_num_delpages as i64)));
        values.push(Some(format!("{:.6}", btm_last_cleanup_num_heap_tuples)));
        values.push(Some(if btm_allequalimage { "t".into() } else { "f".into() }));
    } else {
        values.push(Some("0".into()));
        values.push(Some("-1".into()));
        values.push(Some("f".into()));
    }

    cstrings_composite_result(mcx, &tupdesc, &values)
}
