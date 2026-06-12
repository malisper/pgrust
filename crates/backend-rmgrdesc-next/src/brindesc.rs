//! `access/rmgrdesc/brindesc.c` — rmgr descriptor routines for BRIN indexes.

use crate::{appendf, u16_at, u32_at};
use mcx::PgString;
use types_error::PgResult;
use types_wal::{XLogRecordView, XLR_INFO_MASK};

// access/brin_xlog.h
pub const XLOG_BRIN_CREATE_INDEX: u8 = 0x00;
pub const XLOG_BRIN_INSERT: u8 = 0x10;
pub const XLOG_BRIN_UPDATE: u8 = 0x20;
pub const XLOG_BRIN_SAMEPAGE_UPDATE: u8 = 0x30;
pub const XLOG_BRIN_REVMAP_EXTEND: u8 = 0x40;
pub const XLOG_BRIN_DESUMMARIZE: u8 = 0x50;
pub const XLOG_BRIN_OPMASK: u8 = 0x70;
pub const XLOG_BRIN_INIT_PAGE: u8 = 0x80;

// C struct offsets:
//   xl_brin_createidx:       pagesPerRange u32 @0, version u16 @4
//   xl_brin_insert:          heapBlk u32 @0, pagesPerRange u32 @4, offnum u16 @8
//   xl_brin_update:          oldOffnum u16 @0, insert @4
//   xl_brin_samepage_update: offnum u16 @0
//   xl_brin_revmap_extend:   targetBlk u32 @0
//   xl_brin_desummarize:     pagesPerRange u32 @0, heapBlk u32 @4, regOffset u16 @8

/// `brin_desc(StringInfo buf, XLogReaderState *record)`.
pub fn brin_desc(buf: &mut PgString<'_>, record: &XLogRecordView<'_>) -> PgResult<()> {
    let rec = record.data();
    let mut info = record.info() & !XLR_INFO_MASK;

    info &= XLOG_BRIN_OPMASK;
    if info == XLOG_BRIN_CREATE_INDEX {
        appendf!(buf, "v{} pagesPerRange {}", u16_at(rec, 4), u32_at(rec, 0));
    } else if info == XLOG_BRIN_INSERT {
        appendf!(
            buf,
            "heapBlk {} pagesPerRange {} offnum {}",
            u32_at(rec, 0),
            u32_at(rec, 4),
            u16_at(rec, 8)
        );
    } else if info == XLOG_BRIN_UPDATE {
        appendf!(
            buf,
            "heapBlk {} pagesPerRange {} old offnum {}, new offnum {}",
            u32_at(rec, 4),
            u32_at(rec, 8),
            u16_at(rec, 0),
            u16_at(rec, 12)
        );
    } else if info == XLOG_BRIN_SAMEPAGE_UPDATE {
        appendf!(buf, "offnum {}", u16_at(rec, 0));
    } else if info == XLOG_BRIN_REVMAP_EXTEND {
        appendf!(buf, "targetBlk {}", u32_at(rec, 0));
    } else if info == XLOG_BRIN_DESUMMARIZE {
        appendf!(
            buf,
            "pagesPerRange {}, heapBlk {}, page offset {}",
            u32_at(rec, 0),
            u32_at(rec, 4),
            u16_at(rec, 8)
        );
    }
    Ok(())
}

/// `brin_identify(uint8 info)` — `None` where C returns NULL.
pub fn brin_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_BRIN_CREATE_INDEX => Some("CREATE_INDEX"),
        XLOG_BRIN_INSERT => Some("INSERT"),
        x if x == XLOG_BRIN_INSERT | XLOG_BRIN_INIT_PAGE => Some("INSERT+INIT"),
        XLOG_BRIN_UPDATE => Some("UPDATE"),
        x if x == XLOG_BRIN_UPDATE | XLOG_BRIN_INIT_PAGE => Some("UPDATE+INIT"),
        XLOG_BRIN_SAMEPAGE_UPDATE => Some("SAMEPAGE_UPDATE"),
        XLOG_BRIN_REVMAP_EXTEND => Some("REVMAP_EXTEND"),
        XLOG_BRIN_DESUMMARIZE => Some("DESUMMARIZE"),
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
        brin_desc(&mut buf, &record).unwrap();
        buf.as_str().to_string()
    }

    #[test]
    fn formats_records() {
        let mut createidx = Vec::new();
        createidx.extend_from_slice(&128u32.to_ne_bytes());
        createidx.extend_from_slice(&2u16.to_ne_bytes());
        assert_eq!(desc(XLOG_BRIN_CREATE_INDEX, &createidx), "v2 pagesPerRange 128");

        let mut insert = Vec::new();
        insert.extend_from_slice(&10u32.to_ne_bytes());
        insert.extend_from_slice(&128u32.to_ne_bytes());
        insert.extend_from_slice(&5u16.to_ne_bytes());
        // INIT_PAGE bit masked away by OPMASK
        assert_eq!(
            desc(XLOG_BRIN_INSERT | XLOG_BRIN_INIT_PAGE, &insert),
            "heapBlk 10 pagesPerRange 128 offnum 5"
        );

        let mut update = vec![0u8; 14];
        update[0..2].copy_from_slice(&7u16.to_ne_bytes()); // oldOffnum
        update[4..8].copy_from_slice(&10u32.to_ne_bytes()); // insert.heapBlk
        update[8..12].copy_from_slice(&128u32.to_ne_bytes()); // insert.pagesPerRange
        update[12..14].copy_from_slice(&9u16.to_ne_bytes()); // insert.offnum
        assert_eq!(
            desc(XLOG_BRIN_UPDATE, &update),
            "heapBlk 10 pagesPerRange 128 old offnum 7, new offnum 9"
        );

        assert_eq!(desc(0x60, &[]), "");
    }

    #[test]
    fn identifies() {
        assert_eq!(brin_identify(XLOG_BRIN_INSERT | XLOG_BRIN_INIT_PAGE), Some("INSERT+INIT"));
        assert_eq!(brin_identify(XLOG_BRIN_DESUMMARIZE), Some("DESUMMARIZE"));
        assert_eq!(brin_identify(0x60), None);
    }
}
