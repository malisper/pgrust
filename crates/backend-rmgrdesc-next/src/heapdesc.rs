//! `access/rmgrdesc/heapdesc.c` — rmgr descriptor routines for heapam,
//! including `heap_xlog_deserialize_prune_and_freeze` (shared with heap2_redo
//! and frontend pg_waldump).

use crate::standbydesc::standby_desc_invalidations;
use crate::appendf;
use backend_rmgrdesc_small_seams::{array_desc, offset_elem_desc, oid_elem_desc,
                                   redirect_elem_desc};
use mcx::PgString;
use types_error::PgResult;
use types_wal::{DecodedXLogRecord, XLR_INFO_MASK};
use types_xlog_records::arrays::{OffsetNumberPairs, OffsetNumbers, Oids, SIZEOF_OFFSET_NUMBER,
                                 SIZEOF_OID};
use types_xlog_records::heapam_xlog::{FreezePlans, xl_heap_confirm, xl_heap_delete,
                                      xl_heap_inplace, xl_heap_insert, xl_heap_lock,
                                      xl_heap_lock_updated, xl_heap_multi_insert,
                                      xl_heap_new_cid, xl_heap_prune, xl_heap_truncate,
                                      xl_heap_update, xl_heap_visible, xlhp_freeze_plans,
                                      xlhp_prune_items, SIZEOF_XLHP_FREEZE_PLAN};

// access/heapam_xlog.h — RM_HEAP_ID opcodes
pub const XLOG_HEAP_INSERT: u8 = 0x00;
pub const XLOG_HEAP_DELETE: u8 = 0x10;
pub const XLOG_HEAP_UPDATE: u8 = 0x20;
pub const XLOG_HEAP_TRUNCATE: u8 = 0x30;
pub const XLOG_HEAP_HOT_UPDATE: u8 = 0x40;
pub const XLOG_HEAP_CONFIRM: u8 = 0x50;
pub const XLOG_HEAP_LOCK: u8 = 0x60;
pub const XLOG_HEAP_INPLACE: u8 = 0x70;
pub const XLOG_HEAP_OPMASK: u8 = 0x70;
pub const XLOG_HEAP_INIT_PAGE: u8 = 0x80;

// RM_HEAP2_ID opcodes
pub const XLOG_HEAP2_REWRITE: u8 = 0x00;
pub const XLOG_HEAP2_PRUNE_ON_ACCESS: u8 = 0x10;
pub const XLOG_HEAP2_PRUNE_VACUUM_SCAN: u8 = 0x20;
pub const XLOG_HEAP2_PRUNE_VACUUM_CLEANUP: u8 = 0x30;
pub const XLOG_HEAP2_VISIBLE: u8 = 0x40;
pub const XLOG_HEAP2_MULTI_INSERT: u8 = 0x50;
pub const XLOG_HEAP2_LOCK_UPDATED: u8 = 0x60;
pub const XLOG_HEAP2_NEW_CID: u8 = 0x70;

// xl_heap_truncate flags
pub const XLH_TRUNCATE_CASCADE: u8 = 1 << 0;
pub const XLH_TRUNCATE_RESTART_SEQS: u8 = 1 << 1;

// xl_heap_prune flags
pub const XLHP_IS_CATALOG_REL: u8 = 1 << 1;
pub const XLHP_CLEANUP_LOCK: u8 = 1 << 2;
pub const XLHP_HAS_CONFLICT_HORIZON: u8 = 1 << 3;
pub const XLHP_HAS_FREEZE_PLANS: u8 = 1 << 4;
pub const XLHP_HAS_REDIRECTIONS: u8 = 1 << 5;
pub const XLHP_HAS_DEAD_ITEMS: u8 = 1 << 6;
pub const XLHP_HAS_NOW_UNUSED_ITEMS: u8 = 1 << 7;

// infomask bits in *_desc records
pub const XLHL_XMAX_IS_MULTI: u8 = 0x01;
pub const XLHL_XMAX_LOCK_ONLY: u8 = 0x02;
pub const XLHL_XMAX_EXCL_LOCK: u8 = 0x04;
pub const XLHL_XMAX_KEYSHR_LOCK: u8 = 0x08;
pub const XLHL_KEYS_UPDATED: u8 = 0x10;

/// NOTE: "keyname" argument cannot have trailing spaces or punctuation
/// characters.
fn infobits_desc(buf: &mut PgString<'_>, infobits: u8, keyname: &str) -> PgResult<()> {
    appendf!(buf, "{}: [", keyname);

    debug_assert!(!buf.as_bytes().ends_with(b" "));

    if infobits & XLHL_XMAX_IS_MULTI != 0 {
        buf.try_push_str("IS_MULTI, ")?;
    }
    if infobits & XLHL_XMAX_LOCK_ONLY != 0 {
        buf.try_push_str("LOCK_ONLY, ")?;
    }
    if infobits & XLHL_XMAX_EXCL_LOCK != 0 {
        buf.try_push_str("EXCL_LOCK, ")?;
    }
    if infobits & XLHL_XMAX_KEYSHR_LOCK != 0 {
        buf.try_push_str("KEYSHR_LOCK, ")?;
    }
    if infobits & XLHL_KEYS_UPDATED != 0 {
        buf.try_push_str("KEYS_UPDATED, ")?;
    }

    if buf.as_bytes().ends_with(b" ") {
        // Truncate-away final unneeded ", "
        debug_assert!(buf.as_bytes().ends_with(b", "));
        buf.truncate(buf.len() - 2);
    }

    buf.try_push_str("]")
}

fn truncate_flags_desc(buf: &mut PgString<'_>, flags: u8) -> PgResult<()> {
    buf.try_push_str("flags: [")?;

    if flags & XLH_TRUNCATE_CASCADE != 0 {
        buf.try_push_str("CASCADE, ")?;
    }
    if flags & XLH_TRUNCATE_RESTART_SEQS != 0 {
        buf.try_push_str("RESTART_SEQS, ")?;
    }

    if buf.as_bytes().ends_with(b" ") {
        // Truncate-away final unneeded ", "
        debug_assert!(buf.as_bytes().ends_with(b", "));
        buf.truncate(buf.len() - 2);
    }

    buf.try_push_str("]")
}

/// The sub-records of an `XLOG_HEAP2_PRUNE_*` record's block 0 data, as the C
/// out-parameters of `heap_xlog_deserialize_prune_and_freeze`. The typed array
/// views borrow the block data; `plans`/`redirected`/`nowdead`/`nowunused`
/// start at the respective arrays (empty when the corresponding flag is
/// unset). `frz_offsets` is a cursor over the shared page-offset array that
/// each plan's `ntuples` entries consume in turn.
pub struct PruneFreezeSubRecords<'a> {
    pub nplans: i32,
    pub plans: FreezePlans<'a>,
    pub frz_offsets: OffsetNumbers<'a>,
    pub nredirected: i32,
    pub redirected: OffsetNumberPairs<'a>,
    pub ndead: i32,
    pub nowdead: OffsetNumbers<'a>,
    pub nunused: i32,
    pub nowunused: OffsetNumbers<'a>,
}

/// `heap_xlog_deserialize_prune_and_freeze(char *cursor, uint8 flags, ...)`.
///
/// Given a MAXALIGNed buffer returned by `XLogRecGetBlockData()` and any
/// `xl_heap_prune` flags, deserialize the arrays of OffsetNumbers contained
/// in an `XLOG_HEAP2_PRUNE_*` record. Shared between heap2_redo and
/// heap2_desc code, the latter of which is used in frontend (pg_waldump)
/// code.
pub fn heap_xlog_deserialize_prune_and_freeze(
    cursor: &[u8],
    flags: u8,
) -> PruneFreezeSubRecords<'_> {
    let mut cur = cursor;

    let (nplans, plans) = if flags & XLHP_HAS_FREEZE_PLANS != 0 {
        let freeze_plans = xlhp_freeze_plans::from_bytes(cur);
        let nplans = freeze_plans.nplans as i32;
        debug_assert!(nplans > 0);
        let plans = xlhp_freeze_plans::plans(cur);
        cur = &cur[xlhp_freeze_plans::OFFSETOF_PLANS
            + SIZEOF_XLHP_FREEZE_PLAN * nplans as usize..];
        (nplans, plans)
    } else {
        (0, FreezePlans::from_bytes(&[]))
    };

    let (nredirected, redirected) = if flags & XLHP_HAS_REDIRECTIONS != 0 {
        let items = xlhp_prune_items::from_bytes(cur);
        let ntargets = items.ntargets as i32;
        debug_assert!(ntargets > 0);
        let data = OffsetNumberPairs::from_bytes(&cur[xlhp_prune_items::OFFSETOF_DATA..]);
        cur = &cur[xlhp_prune_items::OFFSETOF_DATA
            + 2 * SIZEOF_OFFSET_NUMBER * ntargets as usize..];
        (ntargets, data)
    } else {
        (0, OffsetNumberPairs::from_bytes(&[]))
    };

    let (ndead, nowdead) = if flags & XLHP_HAS_DEAD_ITEMS != 0 {
        let items = xlhp_prune_items::from_bytes(cur);
        let ntargets = items.ntargets as i32;
        debug_assert!(ntargets > 0);
        let data = xlhp_prune_items::data(cur);
        cur = &cur[xlhp_prune_items::OFFSETOF_DATA
            + SIZEOF_OFFSET_NUMBER * ntargets as usize..];
        (ntargets, data)
    } else {
        (0, OffsetNumbers::from_bytes(&[]))
    };

    let (nunused, nowunused) = if flags & XLHP_HAS_NOW_UNUSED_ITEMS != 0 {
        let items = xlhp_prune_items::from_bytes(cur);
        let ntargets = items.ntargets as i32;
        debug_assert!(ntargets > 0);
        let data = xlhp_prune_items::data(cur);
        cur = &cur[xlhp_prune_items::OFFSETOF_DATA
            + SIZEOF_OFFSET_NUMBER * ntargets as usize..];
        (ntargets, data)
    } else {
        (0, OffsetNumbers::from_bytes(&[]))
    };

    PruneFreezeSubRecords {
        nplans,
        plans,
        frz_offsets: OffsetNumbers::from_bytes(cur),
        nredirected,
        redirected,
        ndead,
        nowdead,
        nunused,
        nowunused,
    }
}

/// `heap_desc(StringInfo buf, XLogReaderState *record)`.
pub fn heap_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let rec = record.data();
    let mut info = record.info() & !XLR_INFO_MASK;

    info &= XLOG_HEAP_OPMASK;
    if info == XLOG_HEAP_INSERT {
        let xlrec = xl_heap_insert::from_bytes(rec);
        appendf!(buf, "off: {}, flags: 0x{:02X}", xlrec.offnum, xlrec.flags);
    } else if info == XLOG_HEAP_DELETE {
        let xlrec = xl_heap_delete::from_bytes(rec);
        appendf!(buf, "xmax: {}, off: {}, ", xlrec.xmax, xlrec.offnum);
        infobits_desc(buf, xlrec.infobits_set, "infobits")?;
        appendf!(buf, ", flags: 0x{:02X}", xlrec.flags);
    } else if info == XLOG_HEAP_UPDATE || info == XLOG_HEAP_HOT_UPDATE {
        let xlrec = xl_heap_update::from_bytes(rec);
        appendf!(buf, "old_xmax: {}, old_off: {}, ", xlrec.old_xmax, xlrec.old_offnum);
        infobits_desc(buf, xlrec.old_infobits_set, "old_infobits")?;
        appendf!(
            buf,
            ", flags: 0x{:02X}, new_xmax: {}, new_off: {}",
            xlrec.flags,
            xlrec.new_xmax,
            xlrec.new_offnum
        );
    } else if info == XLOG_HEAP_TRUNCATE {
        let xlrec = xl_heap_truncate::from_bytes(rec);
        truncate_flags_desc(buf, xlrec.flags)?;
        appendf!(buf, ", nrelids: {}", xlrec.nrelids);
        buf.try_push_str(", relids:")?;
        array_desc::call(
            buf,
            xl_heap_truncate::relids(rec).bytes_of(xlrec.nrelids as usize),
            SIZEOF_OID,
            xlrec.nrelids as i32,
            &mut |buf, elem| oid_elem_desc::call(buf, Oids::from_bytes(elem).get(0)),
        )?;
    } else if info == XLOG_HEAP_CONFIRM {
        let xlrec = xl_heap_confirm::from_bytes(rec);
        appendf!(buf, "off: {}", xlrec.offnum);
    } else if info == XLOG_HEAP_LOCK {
        let xlrec = xl_heap_lock::from_bytes(rec);
        appendf!(buf, "xmax: {}, off: {}, ", xlrec.xmax, xlrec.offnum);
        infobits_desc(buf, xlrec.infobits_set, "infobits")?;
        appendf!(buf, ", flags: 0x{:02X}", xlrec.flags);
    } else if info == XLOG_HEAP_INPLACE {
        let xlrec = xl_heap_inplace::from_bytes(rec);
        appendf!(buf, "off: {}", xlrec.offnum);
        standby_desc_invalidations(
            buf,
            xlrec.nmsgs,
            xl_heap_inplace::msgs(rec),
            xlrec.dbId,
            xlrec.tsId,
            xlrec.relcacheInitFileInval,
        )?;
    }
    Ok(())
}

/// `heap2_desc(StringInfo buf, XLogReaderState *record)`.
pub fn heap2_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let rec = record.data();
    let mut info = record.info() & !XLR_INFO_MASK;

    info &= XLOG_HEAP_OPMASK;
    if info == XLOG_HEAP2_PRUNE_ON_ACCESS
        || info == XLOG_HEAP2_PRUNE_VACUUM_SCAN
        || info == XLOG_HEAP2_PRUNE_VACUUM_CLEANUP
    {
        let xlrec = xl_heap_prune::from_bytes(rec);

        if xlrec.flags & XLHP_HAS_CONFLICT_HORIZON != 0 {
            // conflict horizon XID follows the struct, unaligned
            let conflict_xid = xl_heap_prune::conflict_horizon(rec);
            appendf!(buf, "snapshotConflictHorizon: {}", conflict_xid);
        }

        appendf!(
            buf,
            ", isCatalogRel: {}",
            if xlrec.flags & XLHP_IS_CATALOG_REL != 0 { 'T' } else { 'F' }
        );

        if record.has_block_data(0) {
            let cursor = record.block_data(0).expect("checked has_block_data");
            let pf = heap_xlog_deserialize_prune_and_freeze(cursor, xlrec.flags);

            appendf!(
                buf,
                ", nplans: {}, nredirected: {}, ndead: {}, nunused: {}",
                pf.nplans,
                pf.nredirected,
                pf.ndead,
                pf.nunused
            );

            if pf.nplans > 0 {
                buf.try_push_str(", plans:")?;
                // plan_elem_desc: prints one xlhp_freeze_plan and consumes its
                // ntuples offsets from the shared frz_offsets cursor
                let mut frz_offsets = pf.frz_offsets;
                array_desc::call(
                    buf,
                    pf.plans.bytes_of(pf.nplans as usize),
                    SIZEOF_XLHP_FREEZE_PLAN,
                    pf.nplans,
                    &mut |buf, plan_bytes| {
                        let plan = FreezePlans::from_bytes(plan_bytes).get(0);
                        crate::append(
                            buf,
                            format_args!(
                                "{{ xmax: {}, infomask: {}, infomask2: {}, ntuples: {}",
                                plan.xmax,
                                plan.t_infomask,
                                plan.t_infomask2,
                                plan.ntuples
                            ),
                        )?;

                        buf.try_push_str(", offsets:")?;
                        array_desc::call(
                            buf,
                            frz_offsets.bytes_of(plan.ntuples as usize),
                            SIZEOF_OFFSET_NUMBER,
                            plan.ntuples as i32,
                            &mut |buf, elem| {
                                offset_elem_desc::call(buf, OffsetNumbers::from_bytes(elem).get(0))
                            },
                        )?;
                        frz_offsets = frz_offsets.skip(plan.ntuples as usize);

                        buf.try_push_str(" }")
                    },
                )?;
            }

            if pf.nredirected > 0 {
                buf.try_push_str(", redirected:")?;
                array_desc::call(
                    buf,
                    pf.redirected.bytes_of(pf.nredirected as usize),
                    SIZEOF_OFFSET_NUMBER * 2,
                    pf.nredirected,
                    &mut |buf, elem| {
                        let (from, to) = OffsetNumberPairs::from_bytes(elem).get(0);
                        redirect_elem_desc::call(buf, from, to)
                    },
                )?;
            }

            if pf.ndead > 0 {
                buf.try_push_str(", dead:")?;
                array_desc::call(
                    buf,
                    pf.nowdead.bytes_of(pf.ndead as usize),
                    SIZEOF_OFFSET_NUMBER,
                    pf.ndead,
                    &mut |buf, elem| {
                        offset_elem_desc::call(buf, OffsetNumbers::from_bytes(elem).get(0))
                    },
                )?;
            }

            if pf.nunused > 0 {
                buf.try_push_str(", unused:")?;
                array_desc::call(
                    buf,
                    pf.nowunused.bytes_of(pf.nunused as usize),
                    SIZEOF_OFFSET_NUMBER,
                    pf.nunused,
                    &mut |buf, elem| {
                        offset_elem_desc::call(buf, OffsetNumbers::from_bytes(elem).get(0))
                    },
                )?;
            }
        }
    } else if info == XLOG_HEAP2_VISIBLE {
        let xlrec = xl_heap_visible::from_bytes(rec);
        appendf!(
            buf,
            "snapshotConflictHorizon: {}, flags: 0x{:02X}",
            xlrec.snapshotConflictHorizon,
            xlrec.flags
        );
    } else if info == XLOG_HEAP2_MULTI_INSERT {
        let xlrec = xl_heap_multi_insert::from_bytes(rec);
        let isinit = record.info() & XLOG_HEAP_INIT_PAGE != 0;

        appendf!(buf, "ntuples: {}, flags: 0x{:02X}", xlrec.ntuples, xlrec.flags);

        if record.has_block_data(0) && !isinit {
            buf.try_push_str(", offsets:")?;
            array_desc::call(
                buf,
                xl_heap_multi_insert::offsets(rec).bytes_of(xlrec.ntuples as usize),
                SIZEOF_OFFSET_NUMBER,
                xlrec.ntuples as i32,
                &mut |buf, elem| {
                    offset_elem_desc::call(buf, OffsetNumbers::from_bytes(elem).get(0))
                },
            )?;
        }
    } else if info == XLOG_HEAP2_LOCK_UPDATED {
        let xlrec = xl_heap_lock_updated::from_bytes(rec);
        appendf!(buf, "xmax: {}, off: {}, ", xlrec.xmax, xlrec.offnum);
        infobits_desc(buf, xlrec.infobits_set, "infobits")?;
        appendf!(buf, ", flags: 0x{:02X}", xlrec.flags);
    } else if info == XLOG_HEAP2_NEW_CID {
        let xlrec = xl_heap_new_cid::from_bytes(rec);
        appendf!(
            buf,
            "rel: {}/{}/{}, tid: {}/{}",
            xlrec.target_locator.spcOid,
            xlrec.target_locator.dbOid,
            xlrec.target_locator.relNumber,
            xlrec.target_tid.ip_blkid.block_number(),
            xlrec.target_tid.ip_posid
        );
        appendf!(
            buf,
            ", cmin: {}, cmax: {}, combo: {}",
            xlrec.cmin,
            xlrec.cmax,
            xlrec.combocid
        );
    }
    Ok(())
}

/// `heap_identify(uint8 info)` — `None` where C returns NULL.
pub fn heap_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_HEAP_INSERT => Some("INSERT"),
        x if x == XLOG_HEAP_INSERT | XLOG_HEAP_INIT_PAGE => Some("INSERT+INIT"),
        XLOG_HEAP_DELETE => Some("DELETE"),
        XLOG_HEAP_UPDATE => Some("UPDATE"),
        x if x == XLOG_HEAP_UPDATE | XLOG_HEAP_INIT_PAGE => Some("UPDATE+INIT"),
        XLOG_HEAP_HOT_UPDATE => Some("HOT_UPDATE"),
        x if x == XLOG_HEAP_HOT_UPDATE | XLOG_HEAP_INIT_PAGE => Some("HOT_UPDATE+INIT"),
        XLOG_HEAP_TRUNCATE => Some("TRUNCATE"),
        XLOG_HEAP_CONFIRM => Some("HEAP_CONFIRM"),
        XLOG_HEAP_LOCK => Some("LOCK"),
        XLOG_HEAP_INPLACE => Some("INPLACE"),
        _ => None,
    }
}

/// `heap2_identify(uint8 info)` — `None` where C returns NULL.
pub fn heap2_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_HEAP2_PRUNE_ON_ACCESS => Some("PRUNE_ON_ACCESS"),
        XLOG_HEAP2_PRUNE_VACUUM_SCAN => Some("PRUNE_VACUUM_SCAN"),
        XLOG_HEAP2_PRUNE_VACUUM_CLEANUP => Some("PRUNE_VACUUM_CLEANUP"),
        XLOG_HEAP2_VISIBLE => Some("VISIBLE"),
        XLOG_HEAP2_MULTI_INSERT => Some("MULTI_INSERT"),
        x if x == XLOG_HEAP2_MULTI_INSERT | XLOG_HEAP_INIT_PAGE => Some("MULTI_INSERT+INIT"),
        XLOG_HEAP2_LOCK_UPDATED => Some("LOCK_UPDATED"),
        XLOG_HEAP2_NEW_CID => Some("NEW_CID"),
        XLOG_HEAP2_REWRITE => Some("REWRITE"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::record;
    use mcx::MemoryContext;
    use types_storage::sinval::SIZEOF_SHARED_INVALIDATION_MESSAGE;

    fn desc(info: u8, data: &[u8]) -> String {
        let ctx = MemoryContext::new("test");
        let mut buf = PgString::new_in(ctx.mcx());
        let record = record(ctx.mcx(), info, data, &[]);
        heap_desc(&mut buf, &record).unwrap();
        buf.as_str().to_string()
    }

    #[test]
    fn formats_insert_and_delete() {
        let mut rec = vec![0u8; 3];
        rec[0..2].copy_from_slice(&5u16.to_ne_bytes());
        rec[2] = 0x0A;
        assert_eq!(desc(XLOG_HEAP_INSERT, &rec), "off: 5, flags: 0x0A");

        let mut rec = vec![0u8; 8];
        rec[0..4].copy_from_slice(&77u32.to_ne_bytes());
        rec[4..6].copy_from_slice(&3u16.to_ne_bytes());
        rec[6] = XLHL_XMAX_IS_MULTI | XLHL_KEYS_UPDATED;
        rec[7] = 0x01;
        assert_eq!(
            desc(XLOG_HEAP_DELETE, &rec),
            "xmax: 77, off: 3, infobits: [IS_MULTI, KEYS_UPDATED], flags: 0x01"
        );
    }

    #[test]
    fn empty_infobits_bracket() {
        let ctx = MemoryContext::new("test");
        let mut buf = PgString::new_in(ctx.mcx());
        infobits_desc(&mut buf, 0, "infobits").unwrap();
        assert_eq!(buf.as_str(), "infobits: []");
    }

    #[test]
    fn formats_inplace_with_invalidations() {
        // offnum 4; dbId 5; tsId 6; inval=false; 1 catcache msg id 9
        let mut rec = vec![0u8; 20];
        rec[0..2].copy_from_slice(&4u16.to_ne_bytes());
        rec[4..8].copy_from_slice(&5u32.to_ne_bytes());
        rec[8..12].copy_from_slice(&6u32.to_ne_bytes());
        rec[12] = 0;
        rec[16..20].copy_from_slice(&1i32.to_ne_bytes());
        let mut msg = [0u8; SIZEOF_SHARED_INVALIDATION_MESSAGE];
        msg[0] = 9;
        rec.extend_from_slice(&msg);
        assert_eq!(desc(XLOG_HEAP_INPLACE, &rec), "off: 4; inval msgs: catcache 9");
    }

    #[test]
    fn formats_new_cid() {
        let ctx = MemoryContext::new("test");
        let mut buf = PgString::new_in(ctx.mcx());
        let mut rec = vec![0u8; 34];
        rec[0..4].copy_from_slice(&1u32.to_ne_bytes()); // top_xid
        rec[4..8].copy_from_slice(&2u32.to_ne_bytes()); // cmin
        rec[8..12].copy_from_slice(&3u32.to_ne_bytes()); // cmax
        rec[12..16].copy_from_slice(&4u32.to_ne_bytes()); // combocid
        rec[16..20].copy_from_slice(&10u32.to_ne_bytes());
        rec[20..24].copy_from_slice(&11u32.to_ne_bytes());
        rec[24..28].copy_from_slice(&12u32.to_ne_bytes());
        // tid: block 9 (hi 0, lo 9), posid 2
        rec[28..30].copy_from_slice(&0u16.to_ne_bytes());
        rec[30..32].copy_from_slice(&9u16.to_ne_bytes());
        rec[32..34].copy_from_slice(&2u16.to_ne_bytes());
        let record = record(ctx.mcx(), XLOG_HEAP2_NEW_CID, &rec, &[]);
        heap2_desc(&mut buf, &record).unwrap();
        assert_eq!(
            buf.as_str(),
            "rel: 10/11/12, tid: 9/2, cmin: 2, cmax: 3, combo: 4"
        );
    }

    #[test]
    fn deserializes_prune_and_freeze() {
        // one freeze plan (ntuples=2), redirections (1 pair), dead (1), unused (1),
        // then 2 freeze offsets
        let mut cursor = Vec::new();
        cursor.extend_from_slice(&1u16.to_ne_bytes()); // nplans
        cursor.extend_from_slice(&[0u8; 2]); // pad to plans @4
        cursor.extend_from_slice(&100u32.to_ne_bytes()); // xmax
        cursor.extend_from_slice(&7u16.to_ne_bytes()); // t_infomask2
        cursor.extend_from_slice(&8u16.to_ne_bytes()); // t_infomask
        cursor.push(1); // frzflags
        cursor.push(0); // pad
        cursor.extend_from_slice(&2u16.to_ne_bytes()); // ntuples
        cursor.extend_from_slice(&1u16.to_ne_bytes()); // redirect ntargets
        cursor.extend_from_slice(&3u16.to_ne_bytes());
        cursor.extend_from_slice(&4u16.to_ne_bytes());
        cursor.extend_from_slice(&1u16.to_ne_bytes()); // dead ntargets
        cursor.extend_from_slice(&5u16.to_ne_bytes());
        cursor.extend_from_slice(&1u16.to_ne_bytes()); // unused ntargets
        cursor.extend_from_slice(&6u16.to_ne_bytes());
        cursor.extend_from_slice(&7u16.to_ne_bytes()); // frz offsets
        cursor.extend_from_slice(&8u16.to_ne_bytes());

        let flags = XLHP_HAS_FREEZE_PLANS
            | XLHP_HAS_REDIRECTIONS
            | XLHP_HAS_DEAD_ITEMS
            | XLHP_HAS_NOW_UNUSED_ITEMS;
        let pf = heap_xlog_deserialize_prune_and_freeze(&cursor, flags);
        assert_eq!(pf.nplans, 1);
        assert_eq!(pf.nredirected, 1);
        assert_eq!(pf.ndead, 1);
        assert_eq!(pf.nunused, 1);
        let plan = pf.plans.get(0);
        assert_eq!(plan.xmax, 100);
        assert_eq!(plan.t_infomask2, 7);
        assert_eq!(plan.t_infomask, 8);
        assert_eq!(plan.frzflags, 1);
        assert_eq!(plan.ntuples, 2);
        assert_eq!(pf.redirected.get(0), (3, 4));
        assert_eq!(pf.nowdead.get(0), 5);
        assert_eq!(pf.nowunused.get(0), 6);
        assert_eq!(pf.frz_offsets.get(0), 7);
        assert_eq!(pf.frz_offsets.get(1), 8);
    }

    #[test]
    fn identifies() {
        assert_eq!(heap_identify(XLOG_HEAP_INSERT | XLOG_HEAP_INIT_PAGE), Some("INSERT+INIT"));
        assert_eq!(heap_identify(XLOG_HEAP_CONFIRM), Some("HEAP_CONFIRM"));
        assert_eq!(
            heap2_identify(XLOG_HEAP2_MULTI_INSERT | XLOG_HEAP_INIT_PAGE),
            Some("MULTI_INSERT+INIT")
        );
        assert_eq!(heap2_identify(XLOG_HEAP2_REWRITE), Some("REWRITE"));
        assert_eq!(heap2_identify(0x80), None);
    }
}
