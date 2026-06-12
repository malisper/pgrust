//! `access/rmgrdesc/hashdesc.c` — rmgr descriptor routines for hash indexes.

use crate::{appendf, bool_at, f64_at, u16_at, u32_at, GFmt};
use mcx::PgString;
use types_error::PgResult;
use types_wal::{XLogRecordView, XLR_INFO_MASK};

// access/hash_xlog.h
pub const XLOG_HASH_INIT_META_PAGE: u8 = 0x00;
pub const XLOG_HASH_INIT_BITMAP_PAGE: u8 = 0x10;
pub const XLOG_HASH_INSERT: u8 = 0x20;
pub const XLOG_HASH_ADD_OVFL_PAGE: u8 = 0x30;
pub const XLOG_HASH_SPLIT_ALLOCATE_PAGE: u8 = 0x40;
pub const XLOG_HASH_SPLIT_PAGE: u8 = 0x50;
pub const XLOG_HASH_SPLIT_COMPLETE: u8 = 0x60;
pub const XLOG_HASH_MOVE_PAGE_CONTENTS: u8 = 0x70;
pub const XLOG_HASH_SQUEEZE_PAGE: u8 = 0x80;
pub const XLOG_HASH_DELETE: u8 = 0x90;
pub const XLOG_HASH_SPLIT_CLEANUP: u8 = 0xA0;
pub const XLOG_HASH_UPDATE_META_PAGE: u8 = 0xB0;
pub const XLOG_HASH_VACUUM_ONE_PAGE: u8 = 0xC0;

pub const XLH_SPLIT_META_UPDATE_MASKS: u8 = 1 << 0;
pub const XLH_SPLIT_META_UPDATE_SPLITPOINT: u8 = 1 << 1;

/// `hash_desc(StringInfo buf, XLogReaderState *record)`.
pub fn hash_desc(buf: &mut PgString<'_>, record: &XLogRecordView<'_>) -> PgResult<()> {
    let rec = record.data();
    let info = record.info() & !XLR_INFO_MASK;

    match info {
        XLOG_HASH_INIT_META_PAGE => {
            // xl_hash_init_meta_page: num_tuples f64 @0, procid u32 @8, ffactor u16 @12
            appendf!(
                buf,
                "num_tuples {}, fillfactor {}",
                GFmt(f64_at(rec, 0)),
                u16_at(rec, 12)
            );
        }
        XLOG_HASH_INIT_BITMAP_PAGE => {
            // xl_hash_init_bitmap_page: bmsize u16 @0
            appendf!(buf, "bmsize {}", u16_at(rec, 0));
        }
        XLOG_HASH_INSERT => {
            // xl_hash_insert: offnum u16 @0
            appendf!(buf, "off {}", u16_at(rec, 0));
        }
        XLOG_HASH_ADD_OVFL_PAGE => {
            // xl_hash_add_ovfl_page: bmsize u16 @0, bmpage_found bool @2
            appendf!(
                buf,
                "bmsize {}, bmpage_found {}",
                u16_at(rec, 0),
                if bool_at(rec, 2) { 'T' } else { 'F' }
            );
        }
        XLOG_HASH_SPLIT_ALLOCATE_PAGE => {
            // xl_hash_split_allocate_page: new_bucket u32 @0, old_bucket_flag u16 @4,
            // new_bucket_flag u16 @6, flags u8 @8
            let flags = rec[8];
            appendf!(
                buf,
                "new_bucket {}, meta_page_masks_updated {}, issplitpoint_changed {}",
                u32_at(rec, 0),
                if flags & XLH_SPLIT_META_UPDATE_MASKS != 0 { 'T' } else { 'F' },
                if flags & XLH_SPLIT_META_UPDATE_SPLITPOINT != 0 { 'T' } else { 'F' }
            );
        }
        XLOG_HASH_SPLIT_COMPLETE => {
            // xl_hash_split_complete: old_bucket_flag u16 @0, new_bucket_flag u16 @2
            appendf!(
                buf,
                "old_bucket_flag {}, new_bucket_flag {}",
                u16_at(rec, 0),
                u16_at(rec, 2)
            );
        }
        XLOG_HASH_MOVE_PAGE_CONTENTS => {
            // xl_hash_move_page_contents: ntups u16 @0, is_prim_bucket_same_wrt bool @2
            appendf!(
                buf,
                "ntups {}, is_primary {}",
                u16_at(rec, 0),
                if bool_at(rec, 2) { 'T' } else { 'F' }
            );
        }
        XLOG_HASH_SQUEEZE_PAGE => {
            // xl_hash_squeeze_page: prevblkno u32 @0, nextblkno u32 @4, ntups u16 @8,
            // is_prim_bucket_same_wrt bool @10
            appendf!(
                buf,
                "prevblkno {}, nextblkno {}, ntups {}, is_primary {}",
                u32_at(rec, 0),
                u32_at(rec, 4),
                u16_at(rec, 8),
                if bool_at(rec, 10) { 'T' } else { 'F' }
            );
        }
        XLOG_HASH_DELETE => {
            // xl_hash_delete: clear_dead_marking bool @0, is_primary_bucket_page bool @1
            appendf!(
                buf,
                "clear_dead_marking {}, is_primary {}",
                if bool_at(rec, 0) { 'T' } else { 'F' },
                if bool_at(rec, 1) { 'T' } else { 'F' }
            );
        }
        XLOG_HASH_UPDATE_META_PAGE => {
            // xl_hash_update_meta_page: ntuples f64 @0
            appendf!(buf, "ntuples {}", GFmt(f64_at(rec, 0)));
        }
        XLOG_HASH_VACUUM_ONE_PAGE => {
            // xl_hash_vacuum_one_page: snapshotConflictHorizon u32 @0, ntuples u16 @4,
            // isCatalogRel bool @6
            appendf!(
                buf,
                "ntuples {}, snapshotConflictHorizon {}, isCatalogRel {}",
                u16_at(rec, 4),
                u32_at(rec, 0),
                if bool_at(rec, 6) { 'T' } else { 'F' }
            );
        }
        _ => {}
    }
    Ok(())
}

/// `hash_identify(uint8 info)` — `None` where C returns NULL.
pub fn hash_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_HASH_INIT_META_PAGE => Some("INIT_META_PAGE"),
        XLOG_HASH_INIT_BITMAP_PAGE => Some("INIT_BITMAP_PAGE"),
        XLOG_HASH_INSERT => Some("INSERT"),
        XLOG_HASH_ADD_OVFL_PAGE => Some("ADD_OVFL_PAGE"),
        XLOG_HASH_SPLIT_ALLOCATE_PAGE => Some("SPLIT_ALLOCATE_PAGE"),
        XLOG_HASH_SPLIT_PAGE => Some("SPLIT_PAGE"),
        XLOG_HASH_SPLIT_COMPLETE => Some("SPLIT_COMPLETE"),
        XLOG_HASH_MOVE_PAGE_CONTENTS => Some("MOVE_PAGE_CONTENTS"),
        XLOG_HASH_SQUEEZE_PAGE => Some("SQUEEZE_PAGE"),
        XLOG_HASH_DELETE => Some("DELETE"),
        XLOG_HASH_SPLIT_CLEANUP => Some("SPLIT_CLEANUP"),
        XLOG_HASH_UPDATE_META_PAGE => Some("UPDATE_META_PAGE"),
        XLOG_HASH_VACUUM_ONE_PAGE => Some("VACUUM_ONE_PAGE"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

    fn desc(info: u8, data: &[u8]) -> String {
        let ctx = MemoryContext::new("test");
        let mut buf = PgString::new_in(ctx.mcx());
        let record = XLogRecordView::new(info, data, &[]);
        hash_desc(&mut buf, &record).unwrap();
        buf.as_str().to_string()
    }

    #[test]
    fn formats_records() {
        let mut rec = vec![0u8; 14];
        rec[0..8].copy_from_slice(&1000.0f64.to_ne_bytes());
        rec[12..14].copy_from_slice(&80u16.to_ne_bytes());
        assert_eq!(desc(XLOG_HASH_INIT_META_PAGE, &rec), "num_tuples 1000, fillfactor 80");

        let mut rec = vec![0u8; 7];
        rec[0..4].copy_from_slice(&77u32.to_ne_bytes());
        rec[4..6].copy_from_slice(&3u16.to_ne_bytes());
        rec[6] = 1;
        assert_eq!(
            desc(XLOG_HASH_VACUUM_ONE_PAGE, &rec),
            "ntuples 3, snapshotConflictHorizon 77, isCatalogRel T"
        );

        assert_eq!(desc(XLOG_HASH_SPLIT_PAGE, &[]), "");
    }

    #[test]
    fn identifies() {
        assert_eq!(hash_identify(XLOG_HASH_SPLIT_PAGE), Some("SPLIT_PAGE"));
        assert_eq!(hash_identify(XLOG_HASH_VACUUM_ONE_PAGE), Some("VACUUM_ONE_PAGE"));
        assert_eq!(hash_identify(0xD0), None);
    }
}
