//! `access/rmgrdesc/hashdesc.c` — rmgr descriptor routines for hash indexes.

use crate::{appendf, GFmt};
use ::mcx::PgString;
use ::types_error::PgResult;
use wal::{DecodedXLogRecord, XLR_INFO_MASK};
use ::xlog_records::hash_xlog::{xl_hash_add_ovfl_page, xl_hash_delete,
                                    xl_hash_init_bitmap_page, xl_hash_init_meta_page,
                                    xl_hash_insert, xl_hash_move_page_contents,
                                    xl_hash_split_allocate_page, xl_hash_split_complete,
                                    xl_hash_squeeze_page, xl_hash_update_meta_page,
                                    xl_hash_vacuum_one_page};

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
pub fn hash_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let rec = record.data();
    let info = record.info() & !XLR_INFO_MASK;

    match info {
        XLOG_HASH_INIT_META_PAGE => {
            let xlrec = xl_hash_init_meta_page::from_bytes(rec);
            appendf!(
                buf,
                "num_tuples {}, fillfactor {}",
                GFmt(xlrec.num_tuples),
                xlrec.ffactor
            );
        }
        XLOG_HASH_INIT_BITMAP_PAGE => {
            let xlrec = xl_hash_init_bitmap_page::from_bytes(rec);
            appendf!(buf, "bmsize {}", xlrec.bmsize);
        }
        XLOG_HASH_INSERT => {
            let xlrec = xl_hash_insert::from_bytes(rec);
            appendf!(buf, "off {}", xlrec.offnum);
        }
        XLOG_HASH_ADD_OVFL_PAGE => {
            let xlrec = xl_hash_add_ovfl_page::from_bytes(rec);
            appendf!(
                buf,
                "bmsize {}, bmpage_found {}",
                xlrec.bmsize,
                if xlrec.bmpage_found { 'T' } else { 'F' }
            );
        }
        XLOG_HASH_SPLIT_ALLOCATE_PAGE => {
            let xlrec = xl_hash_split_allocate_page::from_bytes(rec);
            appendf!(
                buf,
                "new_bucket {}, meta_page_masks_updated {}, issplitpoint_changed {}",
                xlrec.new_bucket,
                if xlrec.flags & XLH_SPLIT_META_UPDATE_MASKS != 0 { 'T' } else { 'F' },
                if xlrec.flags & XLH_SPLIT_META_UPDATE_SPLITPOINT != 0 { 'T' } else { 'F' }
            );
        }
        XLOG_HASH_SPLIT_COMPLETE => {
            let xlrec = xl_hash_split_complete::from_bytes(rec);
            appendf!(
                buf,
                "old_bucket_flag {}, new_bucket_flag {}",
                xlrec.old_bucket_flag,
                xlrec.new_bucket_flag
            );
        }
        XLOG_HASH_MOVE_PAGE_CONTENTS => {
            let xlrec = xl_hash_move_page_contents::from_bytes(rec);
            appendf!(
                buf,
                "ntups {}, is_primary {}",
                xlrec.ntups,
                if xlrec.is_prim_bucket_same_wrt { 'T' } else { 'F' }
            );
        }
        XLOG_HASH_SQUEEZE_PAGE => {
            let xlrec = xl_hash_squeeze_page::from_bytes(rec);
            appendf!(
                buf,
                "prevblkno {}, nextblkno {}, ntups {}, is_primary {}",
                xlrec.prevblkno,
                xlrec.nextblkno,
                xlrec.ntups,
                if xlrec.is_prim_bucket_same_wrt { 'T' } else { 'F' }
            );
        }
        XLOG_HASH_DELETE => {
            let xlrec = xl_hash_delete::from_bytes(rec);
            appendf!(
                buf,
                "clear_dead_marking {}, is_primary {}",
                if xlrec.clear_dead_marking { 'T' } else { 'F' },
                if xlrec.is_primary_bucket_page { 'T' } else { 'F' }
            );
        }
        XLOG_HASH_UPDATE_META_PAGE => {
            let xlrec = xl_hash_update_meta_page::from_bytes(rec);
            appendf!(buf, "ntuples {}", GFmt(xlrec.ntuples));
        }
        XLOG_HASH_VACUUM_ONE_PAGE => {
            let xlrec = xl_hash_vacuum_one_page::from_bytes(rec);
            appendf!(
                buf,
                "ntuples {}, snapshotConflictHorizon {}, isCatalogRel {}",
                xlrec.ntuples,
                xlrec.snapshotConflictHorizon,
                if xlrec.isCatalogRel { 'T' } else { 'F' }
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
    use crate::test_support::record;
    use ::mcx::MemoryContext;

    fn desc(info: u8, data: &[u8]) -> String {
        let ctx = MemoryContext::new("test");
        let mut buf = PgString::new_in(ctx.mcx());
        let record = record(ctx.mcx(), info, data, &[]);
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
