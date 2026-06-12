//! `access/rmgrdesc/heapdesc.c` — rmgr descriptor routines for heapam,
//! including `heap_xlog_deserialize_prune_and_freeze` (shared with heap2_redo
//! and frontend pg_waldump).

use crate::standbydesc::standby_desc_invalidations;
use crate::{appendf, bool_at, i32_at, u16_at, u32_at, u8_at};
use backend_rmgrdesc_small_seams::{array_desc, offset_elem_desc, oid_elem_desc,
                                   redirect_elem_desc};
use mcx::PgString;
use types_error::PgResult;
use types_wal::{XLogRecordView, XLR_INFO_MASK};

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

/// `SizeOfHeapPrune` — `offsetof(xl_heap_prune, flags) + sizeof(uint8)`.
pub const SIZEOF_HEAP_PRUNE: usize = 2;
/// `sizeof(xlhp_freeze_plan)`: xmax u32, t_infomask2 u16, t_infomask u16,
/// frzflags u8, ntuples u16 — 12 bytes, 4-aligned.
const SIZEOF_XLHP_FREEZE_PLAN: usize = 12;
/// `offsetof(xlhp_freeze_plans, plans)` — nplans u16 then 4-aligned array.
const OFFSETOF_XLHP_FREEZE_PLANS_PLANS: usize = 4;
/// `offsetof(xlhp_prune_items, data)` — ntargets u16 then u16 array.
const OFFSETOF_XLHP_PRUNE_ITEMS_DATA: usize = 2;
/// `sizeof(OffsetNumber)`.
const SIZEOF_OFFSET_NUMBER: usize = 2;
/// `sizeof(Oid)`.
const SIZEOF_OID: usize = 4;

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
/// out-parameters of `heap_xlog_deserialize_prune_and_freeze`. The slices
/// borrow the block data; `plans`/`redirected`/`nowdead`/`nowunused` start at
/// the respective arrays (empty when the corresponding flag is unset).
pub struct PruneFreezeSubRecords<'a> {
    pub nplans: i32,
    pub plans: &'a [u8],
    pub frz_offsets: &'a [u8],
    pub nredirected: i32,
    pub redirected: &'a [u8],
    pub ndead: i32,
    pub nowdead: &'a [u8],
    pub nunused: i32,
    pub nowunused: &'a [u8],
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
        let nplans = u16_at(cur, 0) as i32;
        debug_assert!(nplans > 0);
        let plans = &cur[OFFSETOF_XLHP_FREEZE_PLANS_PLANS..];
        cur = &plans[SIZEOF_XLHP_FREEZE_PLAN * nplans as usize..];
        (nplans, plans)
    } else {
        (0, &[][..])
    };

    let (nredirected, redirected) = if flags & XLHP_HAS_REDIRECTIONS != 0 {
        let ntargets = u16_at(cur, 0) as i32;
        debug_assert!(ntargets > 0);
        let data = &cur[OFFSETOF_XLHP_PRUNE_ITEMS_DATA..];
        cur = &data[2 * SIZEOF_OFFSET_NUMBER * ntargets as usize..];
        (ntargets, data)
    } else {
        (0, &[][..])
    };

    let (ndead, nowdead) = if flags & XLHP_HAS_DEAD_ITEMS != 0 {
        let ntargets = u16_at(cur, 0) as i32;
        debug_assert!(ntargets > 0);
        let data = &cur[OFFSETOF_XLHP_PRUNE_ITEMS_DATA..];
        cur = &data[SIZEOF_OFFSET_NUMBER * ntargets as usize..];
        (ntargets, data)
    } else {
        (0, &[][..])
    };

    let (nunused, nowunused) = if flags & XLHP_HAS_NOW_UNUSED_ITEMS != 0 {
        let ntargets = u16_at(cur, 0) as i32;
        debug_assert!(ntargets > 0);
        let data = &cur[OFFSETOF_XLHP_PRUNE_ITEMS_DATA..];
        cur = &data[SIZEOF_OFFSET_NUMBER * ntargets as usize..];
        (ntargets, data)
    } else {
        (0, &[][..])
    };

    PruneFreezeSubRecords {
        nplans,
        plans,
        frz_offsets: cur,
        nredirected,
        redirected,
        ndead,
        nowdead,
        nunused,
        nowunused,
    }
}

/// `heap_desc(StringInfo buf, XLogReaderState *record)`.
pub fn heap_desc(buf: &mut PgString<'_>, record: &XLogRecordView<'_>) -> PgResult<()> {
    let rec = record.data();
    let mut info = record.info() & !XLR_INFO_MASK;

    info &= XLOG_HEAP_OPMASK;
    if info == XLOG_HEAP_INSERT {
        // xl_heap_insert: offnum u16 @0, flags u8 @2
        appendf!(buf, "off: {}, flags: 0x{:02X}", u16_at(rec, 0), u8_at(rec, 2));
    } else if info == XLOG_HEAP_DELETE {
        // xl_heap_delete: xmax u32 @0, offnum u16 @4, infobits_set u8 @6, flags u8 @7
        appendf!(buf, "xmax: {}, off: {}, ", u32_at(rec, 0), u16_at(rec, 4));
        infobits_desc(buf, u8_at(rec, 6), "infobits")?;
        appendf!(buf, ", flags: 0x{:02X}", u8_at(rec, 7));
    } else if info == XLOG_HEAP_UPDATE || info == XLOG_HEAP_HOT_UPDATE {
        // xl_heap_update: old_xmax u32 @0, old_offnum u16 @4, old_infobits_set u8 @6,
        // flags u8 @7, new_xmax u32 @8, new_offnum u16 @12
        appendf!(buf, "old_xmax: {}, old_off: {}, ", u32_at(rec, 0), u16_at(rec, 4));
        infobits_desc(buf, u8_at(rec, 6), "old_infobits")?;
        appendf!(
            buf,
            ", flags: 0x{:02X}, new_xmax: {}, new_off: {}",
            u8_at(rec, 7),
            u32_at(rec, 8),
            u16_at(rec, 12)
        );
    } else if info == XLOG_HEAP_TRUNCATE {
        // xl_heap_truncate: dbId u32 @0, nrelids u32 @4, flags u8 @8, relids @12
        let nrelids = u32_at(rec, 4);
        truncate_flags_desc(buf, u8_at(rec, 8))?;
        appendf!(buf, ", nrelids: {}", nrelids);
        buf.try_push_str(", relids:")?;
        array_desc::call(
            buf,
            &rec[12..12 + nrelids as usize * SIZEOF_OID],
            SIZEOF_OID,
            nrelids as i32,
            &mut |buf, elem| oid_elem_desc::call(buf, elem),
        )?;
    } else if info == XLOG_HEAP_CONFIRM {
        // xl_heap_confirm: offnum u16 @0
        appendf!(buf, "off: {}", u16_at(rec, 0));
    } else if info == XLOG_HEAP_LOCK {
        // xl_heap_lock: xmax u32 @0, offnum u16 @4, infobits_set u8 @6, flags u8 @7
        appendf!(buf, "xmax: {}, off: {}, ", u32_at(rec, 0), u16_at(rec, 4));
        infobits_desc(buf, u8_at(rec, 6), "infobits")?;
        appendf!(buf, ", flags: 0x{:02X}", u8_at(rec, 7));
    } else if info == XLOG_HEAP_INPLACE {
        // xl_heap_inplace: offnum u16 @0, dbId u32 @4, tsId u32 @8,
        // relcacheInitFileInval bool @12, nmsgs i32 @16, msgs @20
        appendf!(buf, "off: {}", u16_at(rec, 0));
        let nmsgs = i32_at(rec, 16);
        standby_desc_invalidations(
            buf,
            nmsgs,
            &rec[20..],
            u32_at(rec, 4),
            u32_at(rec, 8),
            bool_at(rec, 12),
        )?;
    }
    Ok(())
}

/// `heap2_desc(StringInfo buf, XLogReaderState *record)`.
pub fn heap2_desc(buf: &mut PgString<'_>, record: &XLogRecordView<'_>) -> PgResult<()> {
    let rec = record.data();
    let mut info = record.info() & !XLR_INFO_MASK;

    info &= XLOG_HEAP_OPMASK;
    if info == XLOG_HEAP2_PRUNE_ON_ACCESS
        || info == XLOG_HEAP2_PRUNE_VACUUM_SCAN
        || info == XLOG_HEAP2_PRUNE_VACUUM_CLEANUP
    {
        // xl_heap_prune: reason u8 @0, flags u8 @1
        let flags = u8_at(rec, 1);

        if flags & XLHP_HAS_CONFLICT_HORIZON != 0 {
            // conflict horizon XID follows the struct, unaligned
            let conflict_xid = u32_at(rec, SIZEOF_HEAP_PRUNE);
            appendf!(buf, "snapshotConflictHorizon: {}", conflict_xid);
        }

        appendf!(
            buf,
            ", isCatalogRel: {}",
            if flags & XLHP_IS_CATALOG_REL != 0 { 'T' } else { 'F' }
        );

        if record.has_block_data(0) {
            let cursor = record.block_data(0).expect("checked has_block_data");
            let pf = heap_xlog_deserialize_prune_and_freeze(cursor, flags);

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
                    &pf.plans[..pf.nplans as usize * SIZEOF_XLHP_FREEZE_PLAN],
                    SIZEOF_XLHP_FREEZE_PLAN,
                    pf.nplans,
                    &mut |buf, plan| {
                        // xlhp_freeze_plan: xmax u32 @0, t_infomask2 u16 @4,
                        // t_infomask u16 @6, frzflags u8 @8, ntuples u16 @10
                        let ntuples = u16_at(plan, 10);
                        crate::append(
                            buf,
                            format_args!(
                                "{{ xmax: {}, infomask: {}, infomask2: {}, ntuples: {}",
                                u32_at(plan, 0),
                                u16_at(plan, 6),
                                u16_at(plan, 4),
                                ntuples
                            ),
                        )?;

                        buf.try_push_str(", offsets:")?;
                        array_desc::call(
                            buf,
                            &frz_offsets[..ntuples as usize * SIZEOF_OFFSET_NUMBER],
                            SIZEOF_OFFSET_NUMBER,
                            ntuples as i32,
                            &mut |buf, elem| offset_elem_desc::call(buf, elem),
                        )?;
                        frz_offsets = &frz_offsets[ntuples as usize * SIZEOF_OFFSET_NUMBER..];

                        buf.try_push_str(" }")
                    },
                )?;
            }

            if pf.nredirected > 0 {
                buf.try_push_str(", redirected:")?;
                array_desc::call(
                    buf,
                    &pf.redirected[..pf.nredirected as usize * 2 * SIZEOF_OFFSET_NUMBER],
                    SIZEOF_OFFSET_NUMBER * 2,
                    pf.nredirected,
                    &mut |buf, elem| redirect_elem_desc::call(buf, elem),
                )?;
            }

            if pf.ndead > 0 {
                buf.try_push_str(", dead:")?;
                array_desc::call(
                    buf,
                    &pf.nowdead[..pf.ndead as usize * SIZEOF_OFFSET_NUMBER],
                    SIZEOF_OFFSET_NUMBER,
                    pf.ndead,
                    &mut |buf, elem| offset_elem_desc::call(buf, elem),
                )?;
            }

            if pf.nunused > 0 {
                buf.try_push_str(", unused:")?;
                array_desc::call(
                    buf,
                    &pf.nowunused[..pf.nunused as usize * SIZEOF_OFFSET_NUMBER],
                    SIZEOF_OFFSET_NUMBER,
                    pf.nunused,
                    &mut |buf, elem| offset_elem_desc::call(buf, elem),
                )?;
            }
        }
    } else if info == XLOG_HEAP2_VISIBLE {
        // xl_heap_visible: snapshotConflictHorizon u32 @0, flags u8 @4
        appendf!(
            buf,
            "snapshotConflictHorizon: {}, flags: 0x{:02X}",
            u32_at(rec, 0),
            u8_at(rec, 4)
        );
    } else if info == XLOG_HEAP2_MULTI_INSERT {
        // xl_heap_multi_insert: flags u8 @0, ntuples u16 @2, offsets @4
        let ntuples = u16_at(rec, 2);
        let isinit = record.info() & XLOG_HEAP_INIT_PAGE != 0;

        appendf!(buf, "ntuples: {}, flags: 0x{:02X}", ntuples, u8_at(rec, 0));

        if record.has_block_data(0) && !isinit {
            buf.try_push_str(", offsets:")?;
            array_desc::call(
                buf,
                &rec[4..4 + ntuples as usize * SIZEOF_OFFSET_NUMBER],
                SIZEOF_OFFSET_NUMBER,
                ntuples as i32,
                &mut |buf, elem| offset_elem_desc::call(buf, elem),
            )?;
        }
    } else if info == XLOG_HEAP2_LOCK_UPDATED {
        // xl_heap_lock_updated: xmax u32 @0, offnum u16 @4, infobits_set u8 @6,
        // flags u8 @7
        appendf!(buf, "xmax: {}, off: {}, ", u32_at(rec, 0), u16_at(rec, 4));
        infobits_desc(buf, u8_at(rec, 6), "infobits")?;
        appendf!(buf, ", flags: 0x{:02X}", u8_at(rec, 7));
    } else if info == XLOG_HEAP2_NEW_CID {
        // xl_heap_new_cid: top_xid u32 @0, cmin u32 @4, cmax u32 @8, combocid u32
        // @12, target_locator u32x3 @16, target_tid @28 (blkid @28, posid @32)
        let tid_block = ((u16_at(rec, 28) as u32) << 16) | u16_at(rec, 30) as u32;
        appendf!(
            buf,
            "rel: {}/{}/{}, tid: {}/{}",
            u32_at(rec, 16),
            u32_at(rec, 20),
            u32_at(rec, 24),
            tid_block,
            u16_at(rec, 32)
        );
        appendf!(
            buf,
            ", cmin: {}, cmax: {}, combo: {}",
            u32_at(rec, 4),
            u32_at(rec, 8),
            u32_at(rec, 12)
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
    use crate::standbydesc::SIZEOF_SHARED_INVALIDATION_MESSAGE;
    use mcx::MemoryContext;

    fn desc(info: u8, data: &[u8]) -> String {
        let ctx = MemoryContext::new("test");
        let mut buf = PgString::new_in(ctx.mcx());
        let record = XLogRecordView::new(info, data, &[]);
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
        let record = XLogRecordView::new(XLOG_HEAP2_NEW_CID, &rec, &[]);
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
        assert_eq!(u16_at(pf.plans, 10), 2);
        assert_eq!(u16_at(pf.redirected, 0), 3);
        assert_eq!(u16_at(pf.redirected, 2), 4);
        assert_eq!(u16_at(pf.nowdead, 0), 5);
        assert_eq!(u16_at(pf.nowunused, 0), 6);
        assert_eq!(u16_at(pf.frz_offsets, 0), 7);
        assert_eq!(u16_at(pf.frz_offsets, 2), 8);
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
