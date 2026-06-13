//! `access/rmgrdesc/brindesc.c` — rmgr descriptor routines for BRIN indexes.

use crate::appendf;
use mcx::PgString;
use types_error::PgResult;
use types_wal::{DecodedXLogRecord, XLR_INFO_MASK};
use types_xlog_records::brin_xlog::{xl_brin_createidx, xl_brin_desummarize, xl_brin_insert,
                                    xl_brin_revmap_extend, xl_brin_samepage_update,
                                    xl_brin_update};

// access/brin_xlog.h
pub const XLOG_BRIN_CREATE_INDEX: u8 = 0x00;
pub const XLOG_BRIN_INSERT: u8 = 0x10;
pub const XLOG_BRIN_UPDATE: u8 = 0x20;
pub const XLOG_BRIN_SAMEPAGE_UPDATE: u8 = 0x30;
pub const XLOG_BRIN_REVMAP_EXTEND: u8 = 0x40;
pub const XLOG_BRIN_DESUMMARIZE: u8 = 0x50;
pub const XLOG_BRIN_OPMASK: u8 = 0x70;
pub const XLOG_BRIN_INIT_PAGE: u8 = 0x80;

/// `brin_desc(StringInfo buf, XLogReaderState *record)`.
pub fn brin_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let rec = record.data();
    let mut info = record.info() & !XLR_INFO_MASK;

    info &= XLOG_BRIN_OPMASK;
    if info == XLOG_BRIN_CREATE_INDEX {
        let xlrec = xl_brin_createidx::from_bytes(rec);
        appendf!(buf, "v{} pagesPerRange {}", xlrec.version, xlrec.pagesPerRange);
    } else if info == XLOG_BRIN_INSERT {
        let xlrec = xl_brin_insert::from_bytes(rec);
        appendf!(
            buf,
            "heapBlk {} pagesPerRange {} offnum {}",
            xlrec.heapBlk,
            xlrec.pagesPerRange,
            xlrec.offnum
        );
    } else if info == XLOG_BRIN_UPDATE {
        let xlrec = xl_brin_update::from_bytes(rec);
        appendf!(
            buf,
            "heapBlk {} pagesPerRange {} old offnum {}, new offnum {}",
            xlrec.insert.heapBlk,
            xlrec.insert.pagesPerRange,
            xlrec.oldOffnum,
            xlrec.insert.offnum
        );
    } else if info == XLOG_BRIN_SAMEPAGE_UPDATE {
        let xlrec = xl_brin_samepage_update::from_bytes(rec);
        appendf!(buf, "offnum {}", xlrec.offnum);
    } else if info == XLOG_BRIN_REVMAP_EXTEND {
        let xlrec = xl_brin_revmap_extend::from_bytes(rec);
        appendf!(buf, "targetBlk {}", xlrec.targetBlk);
    } else if info == XLOG_BRIN_DESUMMARIZE {
        let xlrec = xl_brin_desummarize::from_bytes(rec);
        appendf!(
            buf,
            "pagesPerRange {}, heapBlk {}, page offset {}",
            xlrec.pagesPerRange,
            xlrec.heapBlk,
            xlrec.regOffset
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
    use crate::test_support::record;
    use mcx::MemoryContext;

    fn desc(info: u8, data: &[u8]) -> String {
        let ctx = MemoryContext::new("test");
        let mut buf = PgString::new_in(ctx.mcx());
        let record = record(ctx.mcx(), info, data, &[]);
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
