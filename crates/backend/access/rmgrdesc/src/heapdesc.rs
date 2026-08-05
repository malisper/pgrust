use crate::{appendf, array_desc, block_data, has_block_data, rec_data, rec_info, Rec, XLR_INFO_MASK};
use heapam_xlog::{
    XLOG_HEAP2_LOCK_UPDATED, XLOG_HEAP2_MULTI_INSERT, XLOG_HEAP2_NEW_CID,
    XLOG_HEAP2_PRUNE_ON_ACCESS, XLOG_HEAP2_PRUNE_VACUUM_CLEANUP, XLOG_HEAP2_PRUNE_VACUUM_SCAN,
    XLOG_HEAP2_REWRITE, XLOG_HEAP2_VISIBLE, XLOG_HEAP_CONFIRM, XLOG_HEAP_DELETE,
    XLOG_HEAP_HOT_UPDATE, XLOG_HEAP_INIT_PAGE, XLOG_HEAP_INPLACE, XLOG_HEAP_INSERT,
    XLOG_HEAP_LOCK, XLOG_HEAP_OPMASK, XLOG_HEAP_TRUNCATE, XLOG_HEAP_UPDATE, XLHL_KEYS_UPDATED,
    XLHL_XMAX_EXCL_LOCK, XLHL_XMAX_IS_MULTI, XLHL_XMAX_KEYSHR_LOCK, XLHL_XMAX_LOCK_ONLY,
};
use stringinfo::StringInfo;
use types_error::PgResult;
use xlogreader_seams::XLogReaderState;

pub const XLH_TRUNCATE_CASCADE: u8 = 1 << 0;
pub const XLH_TRUNCATE_RESTART_SEQS: u8 = 1 << 1;

// xl_heap_prune flags (heapam_xlog.h).
pub const XLHP_IS_CATALOG_REL: u8 = 1 << 1;
pub const XLHP_HAS_CONFLICT_HORIZON: u8 = 1 << 3;
pub const XLHP_HAS_FREEZE_PLANS: u8 = 1 << 4;
pub const XLHP_HAS_REDIRECTIONS: u8 = 1 << 5;
pub const XLHP_HAS_DEAD_ITEMS: u8 = 1 << 6;
pub const XLHP_HAS_NOW_UNUSED_ITEMS: u8 = 1 << 7;

// xl_heap_prune is { uint8 reason; uint8 flags; }.
pub const SizeOfHeapPrune: usize = 2;

// keyname must not end in space/punctuation (matches the C contract).
fn infobits_desc(buf: &mut StringInfo<'_>, infobits: u8, keyname: &str) -> PgResult<()> {
    appendf!(buf, "{keyname}: [")?;
    debug_assert!(buf.as_bytes().last() != Some(&b' '));
    if infobits & XLHL_XMAX_IS_MULTI != 0 {
        buf.append_str("IS_MULTI, ")?;
    }
    if infobits & XLHL_XMAX_LOCK_ONLY != 0 {
        buf.append_str("LOCK_ONLY, ")?;
    }
    if infobits & XLHL_XMAX_EXCL_LOCK != 0 {
        buf.append_str("EXCL_LOCK, ")?;
    }
    if infobits & XLHL_XMAX_KEYSHR_LOCK != 0 {
        buf.append_str("KEYSHR_LOCK, ")?;
    }
    if infobits & XLHL_KEYS_UPDATED != 0 {
        buf.append_str("KEYS_UPDATED, ")?;
    }
    if buf.as_bytes().last() == Some(&b' ') {
        debug_assert!(buf.as_bytes()[buf.len() - 2] == b',');
        buf.truncate(buf.len() - 2);
    }
    buf.append_byte(b']')
}

fn truncate_flags_desc(buf: &mut StringInfo<'_>, flags: u8) -> PgResult<()> {
    buf.append_str("flags: [")?;
    if flags & XLH_TRUNCATE_CASCADE != 0 {
        buf.append_str("CASCADE, ")?;
    }
    if flags & XLH_TRUNCATE_RESTART_SEQS != 0 {
        buf.append_str("RESTART_SEQS, ")?;
    }
    if buf.as_bytes().last() == Some(&b' ') {
        debug_assert!(buf.as_bytes()[buf.len() - 2] == b',');
        buf.truncate(buf.len() - 2);
    }
    buf.append_byte(b']')
}

// xlhp_freeze_plan: xmax 0, t_infomask2 4, t_infomask 6, frzflags 8, ntuples 10; size 12.
const FREEZE_PLAN_SIZE: usize = 12;

struct PruneArrays<'a> {
    nplans: usize,
    plans: Rec<'a>,
    frz_offsets: Rec<'a>,
    nredirected: usize,
    redirected: Rec<'a>,
    ndead: usize,
    nowdead: Rec<'a>,
    nunused: usize,
    nowunused: Rec<'a>,
}

// heap_xlog_deserialize_prune_and_freeze (heapdesc.c).
fn deserialize_prune_and_freeze<'a>(cursor: &'a [u8], flags: u8) -> PgResult<PruneArrays<'a>> {
    let what = "XLOG_HEAP2_PRUNE payload";
    let mut off = 0usize;
    let r = Rec(cursor);

    let (nplans, plans) = if flags & XLHP_HAS_FREEZE_PLANS != 0 {
        let n = r.u16(off, what)? as usize;
        // offsetof(xlhp_freeze_plans, plans) == 4 (nplans + align padding).
        let start = off + 4;
        off = start + FREEZE_PLAN_SIZE * n;
        (n, Rec(cursor.get(start..).unwrap_or(&[])))
    } else {
        (0, Rec(&[]))
    };

    let items = |flag: u8, off: &mut usize, stride: usize| -> PgResult<(usize, Rec<'a>)> {
        if flags & flag != 0 {
            let n = r.u16(*off, what)? as usize;
            let start = *off + 2;
            *off = start + stride * n;
            Ok((n, Rec(cursor.get(start..).unwrap_or(&[]))))
        } else {
            Ok((0, Rec(&[])))
        }
    };

    let (nredirected, redirected) = items(XLHP_HAS_REDIRECTIONS, &mut off, 4)?;
    let (ndead, nowdead) = items(XLHP_HAS_DEAD_ITEMS, &mut off, 2)?;
    let (nunused, nowunused) = items(XLHP_HAS_NOW_UNUSED_ITEMS, &mut off, 2)?;

    let frz_offsets = Rec(cursor.get(off..).unwrap_or(&[]));
    Ok(PruneArrays {
        nplans,
        plans,
        frz_offsets,
        nredirected,
        redirected,
        ndead,
        nowdead,
        nunused,
        nowunused,
    })
}

fn offset_array_desc(buf: &mut StringInfo<'_>, data: Rec<'_>, count: usize) -> PgResult<()> {
    array_desc(buf, count, |buf, i| {
        appendf!(buf, "{}", data.u16(2 * i, "offset array")?)
    })
}

pub fn heap_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = rec_info(record) & !XLR_INFO_MASK & XLOG_HEAP_OPMASK;

    if info == XLOG_HEAP_INSERT {
        // xl_heap_insert: offnum 0, flags 2.
        appendf!(buf, "off: {}, flags: 0x{:02X}", rec.u16(0, "xl_heap_insert")?, rec.u8(2, "xl_heap_insert")?)?;
    } else if info == XLOG_HEAP_DELETE {
        // xl_heap_delete: xmax 0, offnum 4, infobits_set 6, flags 7.
        appendf!(buf, "xmax: {}, off: {}, ", rec.u32(0, "xl_heap_delete")?, rec.u16(4, "xl_heap_delete")?)?;
        infobits_desc(buf, rec.u8(6, "xl_heap_delete")?, "infobits")?;
        appendf!(buf, ", flags: 0x{:02X}", rec.u8(7, "xl_heap_delete")?)?;
    } else if info == XLOG_HEAP_UPDATE || info == XLOG_HEAP_HOT_UPDATE {
        // old_xmax 0, old_offnum 4, old_infobits_set 6, flags 7, new_xmax 8, new_offnum 12.
        appendf!(buf, "old_xmax: {}, old_off: {}, ", rec.u32(0, "xl_heap_update")?, rec.u16(4, "xl_heap_update")?)?;
        infobits_desc(buf, rec.u8(6, "xl_heap_update")?, "old_infobits")?;
        appendf!(
            buf,
            ", flags: 0x{:02X}, new_xmax: {}, new_off: {}",
            rec.u8(7, "xl_heap_update")?,
            rec.u32(8, "xl_heap_update")?,
            rec.u16(12, "xl_heap_update")?
        )?;
    } else if info == XLOG_HEAP_TRUNCATE {
        // xl_heap_truncate: dbId 0, nrelids 4, flags 8, relids[] 12.
        let nrelids = rec.u32(4, "xl_heap_truncate")? as usize;
        truncate_flags_desc(buf, rec.u8(8, "xl_heap_truncate")?)?;
        appendf!(buf, ", nrelids: {nrelids}")?;
        buf.append_str(", relids:")?;
        array_desc(buf, nrelids, |buf, i| {
            appendf!(buf, "{}", rec.u32(12 + 4 * i, "xl_heap_truncate relids")?)
        })?;
    } else if info == XLOG_HEAP_CONFIRM {
        appendf!(buf, "off: {}", rec.u16(0, "xl_heap_confirm")?)?;
    } else if info == XLOG_HEAP_LOCK {
        // xl_heap_lock: xmax 0, offnum 4, infobits_set 6, flags 7.
        appendf!(buf, "xmax: {}, off: {}, ", rec.u32(0, "xl_heap_lock")?, rec.u16(4, "xl_heap_lock")?)?;
        infobits_desc(buf, rec.u8(6, "xl_heap_lock")?, "infobits")?;
        appendf!(buf, ", flags: 0x{:02X}", rec.u8(7, "xl_heap_lock")?)?;
    } else if info == XLOG_HEAP_INPLACE {
        // offnum 0, dbId 4, tsId 8, relcacheInitFileInval 12, nmsgs 16, msgs[] 20.
        appendf!(buf, "off: {}", rec.u16(0, "xl_heap_inplace")?)?;
        let nmsgs = rec.i32(16, "xl_heap_inplace")?.max(0) as usize;
        let raw = rec.0.get(20..20 + nmsgs * 16).ok_or_else(|| crate::record_truncated("xl_heap_inplace"))?;
        crate::standbydesc::standby_desc_invalidations_raw(
            buf,
            nmsgs,
            raw,
            rec.u32(4, "xl_heap_inplace")?,
            rec.u32(8, "xl_heap_inplace")?,
            rec.u8(12, "xl_heap_inplace")? != 0,
        )?;
    }
    Ok(())
}

pub fn heap2_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let full_info = rec_info(record);
    let info = full_info & !XLR_INFO_MASK & XLOG_HEAP_OPMASK;

    if info == XLOG_HEAP2_PRUNE_ON_ACCESS
        || info == XLOG_HEAP2_PRUNE_VACUUM_SCAN
        || info == XLOG_HEAP2_PRUNE_VACUUM_CLEANUP
    {
        let flags = rec.u8(1, "xl_heap_prune")?;

        if flags & XLHP_HAS_CONFLICT_HORIZON != 0 {
            let conflict_xid = rec.u32(SizeOfHeapPrune, "xl_heap_prune")?;
            appendf!(buf, "snapshotConflictHorizon: {conflict_xid}")?;
        }

        appendf!(buf, ", isCatalogRel: {}", if flags & XLHP_IS_CATALOG_REL != 0 { 'T' } else { 'F' })?;

        if has_block_data(record, 0) {
            let a = deserialize_prune_and_freeze(block_data(record, 0), flags)?;

            appendf!(
                buf,
                ", nplans: {}, nredirected: {}, ndead: {}, nunused: {}",
                a.nplans, a.nredirected, a.ndead, a.nunused
            )?;

            if a.nplans > 0 {
                buf.append_str(", plans:")?;
                let mut frz_off = 0usize;
                array_desc(buf, a.nplans, |buf, i| {
                    let base = FREEZE_PLAN_SIZE * i;
                    let what = "xlhp_freeze_plan";
                    let ntuples = a.plans.u16(base + 10, what)? as usize;
                    appendf!(
                        buf,
                        "{{ xmax: {}, infomask: {}, infomask2: {}, ntuples: {}",
                        a.plans.u32(base, what)?,
                        a.plans.u16(base + 6, what)?,
                        a.plans.u16(base + 4, what)?,
                        ntuples
                    )?;
                    buf.append_str(", offsets:")?;
                    let start = frz_off;
                    array_desc(buf, ntuples, |buf, j| {
                        appendf!(buf, "{}", a.frz_offsets.u16(2 * (start + j), what)?)
                    })?;
                    frz_off += ntuples;
                    buf.append_str(" }")
                })?;
            }

            if a.nredirected > 0 {
                buf.append_str(", redirected:")?;
                array_desc(buf, a.nredirected, |buf, i| {
                    let what = "redirect array";
                    appendf!(buf, "{}->{}", a.redirected.u16(4 * i, what)?, a.redirected.u16(4 * i + 2, what)?)
                })?;
            }
            if a.ndead > 0 {
                buf.append_str(", dead:")?;
                offset_array_desc(buf, a.nowdead, a.ndead)?;
            }
            if a.nunused > 0 {
                buf.append_str(", unused:")?;
                offset_array_desc(buf, a.nowunused, a.nunused)?;
            }
        }
    } else if info == XLOG_HEAP2_VISIBLE {
        // xl_heap_visible: snapshotConflictHorizon 0, flags 4.
        appendf!(
            buf,
            "snapshotConflictHorizon: {}, flags: 0x{:02X}",
            rec.u32(0, "xl_heap_visible")?,
            rec.u8(4, "xl_heap_visible")?
        )?;
    } else if info == XLOG_HEAP2_MULTI_INSERT {
        // xl_heap_multi_insert: flags 0, ntuples 2, offsets[] 4.
        let ntuples = rec.u16(2, "xl_heap_multi_insert")? as i32;
        let isinit = full_info & XLOG_HEAP_INIT_PAGE != 0;
        appendf!(buf, "ntuples: {ntuples}, flags: 0x{:02X}", rec.u8(0, "xl_heap_multi_insert")?)?;
        if has_block_data(record, 0) && !isinit {
            buf.append_str(", offsets:")?;
            let offs = Rec(rec.0.get(4..).unwrap_or(&[]));
            offset_array_desc(buf, offs, ntuples.max(0) as usize)?;
        }
    } else if info == XLOG_HEAP2_LOCK_UPDATED {
        // xl_heap_lock_updated: xmax 0, offnum 4, infobits_set 6, flags 7.
        appendf!(buf, "xmax: {}, off: {}, ", rec.u32(0, "xl_heap_lock_updated")?, rec.u16(4, "xl_heap_lock_updated")?)?;
        infobits_desc(buf, rec.u8(6, "xl_heap_lock_updated")?, "infobits")?;
        appendf!(buf, ", flags: 0x{:02X}", rec.u8(7, "xl_heap_lock_updated")?)?;
    } else if info == XLOG_HEAP2_NEW_CID {
        // top_xid 0, cmin 4, cmax 8, combocid 12, target_locator 16,
        // target_tid 28 (ip_blkid hi 28, lo 30, posid 32).
        let what = "xl_heap_new_cid";
        let blkno = ((rec.u16(28, what)? as u32) << 16) | rec.u16(30, what)? as u32;
        appendf!(
            buf,
            "rel: {}/{}/{}, tid: {}/{}",
            rec.u32(16, what)?,
            rec.u32(20, what)?,
            rec.u32(24, what)?,
            blkno,
            rec.u16(32, what)?
        )?;
        appendf!(
            buf,
            ", cmin: {}, cmax: {}, combo: {}",
            rec.u32(4, what)?,
            rec.u32(8, what)?,
            rec.u32(12, what)?
        )?;
    }
    Ok(())
}

const XLOG_HEAP_INSERT_INIT: u8 = XLOG_HEAP_INSERT | XLOG_HEAP_INIT_PAGE;
const XLOG_HEAP_UPDATE_INIT: u8 = XLOG_HEAP_UPDATE | XLOG_HEAP_INIT_PAGE;
const XLOG_HEAP_HOT_UPDATE_INIT: u8 = XLOG_HEAP_HOT_UPDATE | XLOG_HEAP_INIT_PAGE;
const XLOG_HEAP2_MULTI_INSERT_INIT: u8 = XLOG_HEAP2_MULTI_INSERT | XLOG_HEAP_INIT_PAGE;

pub fn heap_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_HEAP_INSERT => Some("INSERT"),
        XLOG_HEAP_INSERT_INIT => Some("INSERT+INIT"),
        XLOG_HEAP_DELETE => Some("DELETE"),
        XLOG_HEAP_UPDATE => Some("UPDATE"),
        XLOG_HEAP_UPDATE_INIT => Some("UPDATE+INIT"),
        XLOG_HEAP_HOT_UPDATE => Some("HOT_UPDATE"),
        XLOG_HEAP_HOT_UPDATE_INIT => Some("HOT_UPDATE+INIT"),
        XLOG_HEAP_TRUNCATE => Some("TRUNCATE"),
        XLOG_HEAP_CONFIRM => Some("HEAP_CONFIRM"),
        XLOG_HEAP_LOCK => Some("LOCK"),
        XLOG_HEAP_INPLACE => Some("INPLACE"),
        _ => None,
    }
}

pub fn heap2_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_HEAP2_PRUNE_ON_ACCESS => Some("PRUNE_ON_ACCESS"),
        XLOG_HEAP2_PRUNE_VACUUM_SCAN => Some("PRUNE_VACUUM_SCAN"),
        XLOG_HEAP2_PRUNE_VACUUM_CLEANUP => Some("PRUNE_VACUUM_CLEANUP"),
        XLOG_HEAP2_VISIBLE => Some("VISIBLE"),
        XLOG_HEAP2_MULTI_INSERT => Some("MULTI_INSERT"),
        XLOG_HEAP2_MULTI_INSERT_INIT => Some("MULTI_INSERT+INIT"),
        XLOG_HEAP2_LOCK_UPDATED => Some("LOCK_UPDATED"),
        XLOG_HEAP2_NEW_CID => Some("NEW_CID"),
        XLOG_HEAP2_REWRITE => Some("REWRITE"),
        _ => None,
    }
}
