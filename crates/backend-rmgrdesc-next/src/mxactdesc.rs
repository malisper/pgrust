//! `access/rmgrdesc/mxactdesc.c` — rmgr descriptor routines for multixacts.

use crate::{appendf, i32_at, i64_at, u32_at};
use mcx::PgString;
use types_error::PgResult;
use types_wal::{XLogRecordView, XLR_INFO_MASK};

// access/multixact.h
pub const XLOG_MULTIXACT_ZERO_OFF_PAGE: u8 = 0x00;
pub const XLOG_MULTIXACT_ZERO_MEM_PAGE: u8 = 0x10;
pub const XLOG_MULTIXACT_CREATE_ID: u8 = 0x20;
pub const XLOG_MULTIXACT_TRUNCATE_ID: u8 = 0x30;

// MultiXactStatus
pub const MULTI_XACT_STATUS_FOR_KEY_SHARE: i32 = 0x00;
pub const MULTI_XACT_STATUS_FOR_SHARE: i32 = 0x01;
pub const MULTI_XACT_STATUS_FOR_NO_KEY_UPDATE: i32 = 0x02;
pub const MULTI_XACT_STATUS_FOR_UPDATE: i32 = 0x03;
pub const MULTI_XACT_STATUS_NO_KEY_UPDATE: i32 = 0x04;
pub const MULTI_XACT_STATUS_UPDATE: i32 = 0x05;

/// `sizeof(MultiXactMember)` — `{TransactionId xid; MultiXactStatus status;}`.
const SIZEOF_MULTI_XACT_MEMBER: usize = 8;
/// `SizeOfMultiXactCreate` — `offsetof(xl_multixact_create, members)`.
const SIZEOF_MULTI_XACT_CREATE: usize = 12;

/// `out_member(StringInfo buf, MultiXactMember *member)`.
fn out_member(buf: &mut PgString<'_>, xid: u32, status: i32) -> PgResult<()> {
    appendf!(buf, "{} ", xid);
    match status {
        MULTI_XACT_STATUS_FOR_KEY_SHARE => buf.try_push_str("(keysh) ")?,
        MULTI_XACT_STATUS_FOR_SHARE => buf.try_push_str("(sh) ")?,
        MULTI_XACT_STATUS_FOR_NO_KEY_UPDATE => buf.try_push_str("(fornokeyupd) ")?,
        MULTI_XACT_STATUS_FOR_UPDATE => buf.try_push_str("(forupd) ")?,
        MULTI_XACT_STATUS_NO_KEY_UPDATE => buf.try_push_str("(nokeyupd) ")?,
        MULTI_XACT_STATUS_UPDATE => buf.try_push_str("(upd) ")?,
        _ => buf.try_push_str("(unk) ")?,
    }
    Ok(())
}

/// `multixact_desc(StringInfo buf, XLogReaderState *record)`.
pub fn multixact_desc(buf: &mut PgString<'_>, record: &XLogRecordView<'_>) -> PgResult<()> {
    let rec = record.data();
    let info = record.info() & !XLR_INFO_MASK;

    if info == XLOG_MULTIXACT_ZERO_OFF_PAGE || info == XLOG_MULTIXACT_ZERO_MEM_PAGE {
        // int64 pageno, memcpy'd from the (possibly unaligned) record start
        appendf!(buf, "{}", i64_at(rec, 0));
    } else if info == XLOG_MULTIXACT_CREATE_ID {
        // xl_multixact_create: mid u32 @0, moff u32 @4, nmembers i32 @8, members @12
        let nmembers = i32_at(rec, 8);
        appendf!(
            buf,
            "{} offset {} nmembers {}: ",
            u32_at(rec, 0),
            u32_at(rec, 4),
            nmembers
        );
        for i in 0..nmembers.max(0) as usize {
            let off = SIZEOF_MULTI_XACT_CREATE + i * SIZEOF_MULTI_XACT_MEMBER;
            out_member(buf, u32_at(rec, off), i32_at(rec, off + 4))?;
        }
    } else if info == XLOG_MULTIXACT_TRUNCATE_ID {
        // xl_multixact_truncate: oldestMultiDB u32 @0, startTruncOff @4,
        // endTruncOff @8, startTruncMemb @12, endTruncMemb @16
        appendf!(
            buf,
            "offsets [{}, {}), members [{}, {})",
            u32_at(rec, 4),
            u32_at(rec, 8),
            u32_at(rec, 12),
            u32_at(rec, 16)
        );
    }
    Ok(())
}

/// `multixact_identify(uint8 info)` — `None` where C returns NULL.
pub fn multixact_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_MULTIXACT_ZERO_OFF_PAGE => Some("ZERO_OFF_PAGE"),
        XLOG_MULTIXACT_ZERO_MEM_PAGE => Some("ZERO_MEM_PAGE"),
        XLOG_MULTIXACT_CREATE_ID => Some("CREATE_ID"),
        XLOG_MULTIXACT_TRUNCATE_ID => Some("TRUNCATE_ID"),
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
        multixact_desc(&mut buf, &record).unwrap();
        buf.as_str().to_string()
    }

    #[test]
    fn formats_records() {
        assert_eq!(desc(XLOG_MULTIXACT_ZERO_OFF_PAGE, &42i64.to_ne_bytes()), "42");

        let mut rec = Vec::new();
        rec.extend_from_slice(&100u32.to_ne_bytes()); // mid
        rec.extend_from_slice(&7u32.to_ne_bytes()); // moff
        rec.extend_from_slice(&2i32.to_ne_bytes()); // nmembers
        rec.extend_from_slice(&11u32.to_ne_bytes());
        rec.extend_from_slice(&MULTI_XACT_STATUS_FOR_SHARE.to_ne_bytes());
        rec.extend_from_slice(&12u32.to_ne_bytes());
        rec.extend_from_slice(&MULTI_XACT_STATUS_UPDATE.to_ne_bytes());
        assert_eq!(
            desc(XLOG_MULTIXACT_CREATE_ID, &rec),
            "100 offset 7 nmembers 2: 11 (sh) 12 (upd) "
        );

        let mut rec = Vec::new();
        rec.extend_from_slice(&1u32.to_ne_bytes());
        rec.extend_from_slice(&2u32.to_ne_bytes());
        rec.extend_from_slice(&3u32.to_ne_bytes());
        rec.extend_from_slice(&4u32.to_ne_bytes());
        rec.extend_from_slice(&5u32.to_ne_bytes());
        assert_eq!(desc(XLOG_MULTIXACT_TRUNCATE_ID, &rec), "offsets [2, 3), members [4, 5)");
    }

    #[test]
    fn identifies() {
        assert_eq!(multixact_identify(XLOG_MULTIXACT_CREATE_ID), Some("CREATE_ID"));
        assert_eq!(multixact_identify(0x40), None);
    }
}
