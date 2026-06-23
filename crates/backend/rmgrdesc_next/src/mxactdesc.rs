//! `access/rmgrdesc/mxactdesc.c` — rmgr descriptor routines for multixacts.

use crate::{appendf, i64_at};
use ::mcx::PgString;
use ::types_error::PgResult;
use ::wal::{DecodedXLogRecord, XLR_INFO_MASK};
use ::xlog_records::multixact::{MultiXactMember, MultiXactStatus, xl_multixact_create,
                                    xl_multixact_truncate};

// access/multixact.h
pub const XLOG_MULTIXACT_ZERO_OFF_PAGE: u8 = 0x00;
pub const XLOG_MULTIXACT_ZERO_MEM_PAGE: u8 = 0x10;
pub const XLOG_MULTIXACT_CREATE_ID: u8 = 0x20;
pub const XLOG_MULTIXACT_TRUNCATE_ID: u8 = 0x30;

/// `out_member(StringInfo buf, MultiXactMember *member)`.
fn out_member(buf: &mut PgString<'_>, member: &MultiXactMember) -> PgResult<()> {
    appendf!(buf, "{} ", member.xid);
    match member.status {
        Some(MultiXactStatus::ForKeyShare) => buf.try_push_str("(keysh) ")?,
        Some(MultiXactStatus::ForShare) => buf.try_push_str("(sh) ")?,
        Some(MultiXactStatus::ForNoKeyUpdate) => buf.try_push_str("(fornokeyupd) ")?,
        Some(MultiXactStatus::ForUpdate) => buf.try_push_str("(forupd) ")?,
        Some(MultiXactStatus::NoKeyUpdate) => buf.try_push_str("(nokeyupd) ")?,
        Some(MultiXactStatus::Update) => buf.try_push_str("(upd) ")?,
        None => buf.try_push_str("(unk) ")?,
    }
    Ok(())
}

/// `multixact_desc(StringInfo buf, XLogReaderState *record)`.
pub fn multixact_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let rec = record.data();
    let info = record.info() & !XLR_INFO_MASK;

    if info == XLOG_MULTIXACT_ZERO_OFF_PAGE || info == XLOG_MULTIXACT_ZERO_MEM_PAGE {
        // int64 pageno, memcpy'd from the (possibly unaligned) record start
        appendf!(buf, "{}", i64_at(rec, 0));
    } else if info == XLOG_MULTIXACT_CREATE_ID {
        let xlrec = xl_multixact_create::from_bytes(rec);
        appendf!(
            buf,
            "{} offset {} nmembers {}: ",
            xlrec.mid,
            xlrec.moff,
            xlrec.nmembers
        );
        let members = xl_multixact_create::members(rec);
        for i in 0..xlrec.nmembers.max(0) as usize {
            out_member(buf, &members.get(i))?;
        }
    } else if info == XLOG_MULTIXACT_TRUNCATE_ID {
        let xlrec = xl_multixact_truncate::from_bytes(rec);
        appendf!(
            buf,
            "offsets [{}, {}), members [{}, {})",
            xlrec.startTruncOff,
            xlrec.endTruncOff,
            xlrec.startTruncMemb,
            xlrec.endTruncMemb
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
    use crate::test_support::record;
    use ::mcx::MemoryContext;

    fn desc(info: u8, data: &[u8]) -> String {
        let ctx = MemoryContext::new("test");
        let mut buf = PgString::new_in(ctx.mcx());
        let record = record(ctx.mcx(), info, data, &[]);
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
        rec.extend_from_slice(&(MultiXactStatus::ForShare as i32).to_ne_bytes());
        rec.extend_from_slice(&12u32.to_ne_bytes());
        rec.extend_from_slice(&(MultiXactStatus::Update as i32).to_ne_bytes());
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
