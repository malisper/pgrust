//! `access/rmgrdesc/standbydesc.c` — rmgr descriptor routines for
//! storage/ipc/standby.c records, plus `standby_desc_invalidations` (shared
//! with the xact and heap-inplace descriptors).

use crate::appendf;
use mcx::PgString;
use types_core::Oid;
use types_error::PgResult;
use types_storage::sinval::{SharedInvalMessages, SharedInvalidationMessage};
use wal::{DecodedXLogRecord, XLR_INFO_MASK};
use xlog_records::standbydefs::{xl_invalidations, xl_running_xacts, xl_standby_locks};

// storage/standbydefs.h
pub const XLOG_STANDBY_LOCK: u8 = 0x00;
pub const XLOG_RUNNING_XACTS: u8 = 0x10;
pub const XLOG_INVALIDATIONS: u8 = 0x20;

/// `standby_desc_running_xacts(StringInfo buf, xl_running_xacts *xlrec)`.
fn standby_desc_running_xacts(buf: &mut PgString<'_>, rec: &[u8]) -> PgResult<()> {
    let xlrec = xl_running_xacts::from_bytes(rec);

    appendf!(
        buf,
        "nextXid {} latestCompletedXid {} oldestRunningXid {}",
        xlrec.nextXid,
        xlrec.latestCompletedXid,
        xlrec.oldestRunningXid
    );
    if xlrec.xcnt > 0 {
        appendf!(buf, "; {} xacts:", xlrec.xcnt);
        for i in 0..xlrec.xcnt as usize {
            appendf!(buf, " {}", xl_running_xacts::xid(rec, i));
        }
    }

    if xlrec.subxid_overflow {
        buf.try_push_str("; subxid overflowed")?;
    }

    if xlrec.subxcnt > 0 {
        appendf!(buf, "; {} subxacts:", xlrec.subxcnt);
        for i in 0..xlrec.subxcnt as usize {
            appendf!(buf, " {}", xl_running_xacts::xid(rec, xlrec.xcnt as usize + i));
        }
    }
    Ok(())
}

/// `standby_desc(StringInfo buf, XLogReaderState *record)`.
pub fn standby_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let rec = record.data();
    let info = record.info() & !XLR_INFO_MASK;

    if info == XLOG_STANDBY_LOCK {
        let xlrec = xl_standby_locks::from_bytes(rec);
        let locks = xl_standby_locks::locks(rec);
        for i in 0..xlrec.nlocks.max(0) as usize {
            let lock = locks.get(i);
            appendf!(buf, "xid {} db {} rel {} ", lock.xid, lock.dbOid, lock.relOid);
        }
    } else if info == XLOG_RUNNING_XACTS {
        standby_desc_running_xacts(buf, rec)?;
    } else if info == XLOG_INVALIDATIONS {
        let xlrec = xl_invalidations::from_bytes(rec);
        standby_desc_invalidations(
            buf,
            xlrec.nmsgs,
            xl_invalidations::msgs(rec),
            xlrec.dbId,
            xlrec.tsId,
            xlrec.relcacheInitFileInval,
        )?;
    }
    Ok(())
}

/// `standby_identify(uint8 info)` — `None` where C returns NULL.
pub fn standby_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_STANDBY_LOCK => Some("LOCK"),
        XLOG_RUNNING_XACTS => Some("RUNNING_XACTS"),
        XLOG_INVALIDATIONS => Some("INVALIDATIONS"),
        _ => None,
    }
}

/// `standby_desc_invalidations(StringInfo buf, int nmsgs,
/// SharedInvalidationMessage *msgs, Oid dbId, Oid tsId,
/// bool relcacheInitFileInval)` — also used by non-standby records having
/// analogous invalidation fields (xact commit/abort, heap inplace).
pub fn standby_desc_invalidations(
    buf: &mut PgString<'_>,
    nmsgs: i32,
    msgs: SharedInvalMessages<'_>,
    db_id: Oid,
    ts_id: Oid,
    relcache_init_file_inval: bool,
) -> PgResult<()> {
    // Do nothing if there are no invalidation messages
    if nmsgs <= 0 {
        return Ok(());
    }

    if relcache_init_file_inval {
        appendf!(buf, "; relcache init file inval dbid {} tsid {}", db_id, ts_id);
    }

    buf.try_push_str("; inval msgs:")?;
    for i in 0..nmsgs as usize {
        match msgs.get(i) {
            Some(SharedInvalidationMessage::Catcache(m)) => appendf!(buf, " catcache {}", m.id),
            Some(SharedInvalidationMessage::Catalog(m)) => appendf!(buf, " catalog {}", m.catId),
            Some(SharedInvalidationMessage::Relcache(m)) => appendf!(buf, " relcache {}", m.relId),
            // not expected, but print something anyway
            Some(SharedInvalidationMessage::Smgr(_)) => buf.try_push_str(" smgr")?,
            // not expected, but print something anyway
            Some(SharedInvalidationMessage::Relmap(m)) => appendf!(buf, " relmap db {}", m.dbId),
            Some(SharedInvalidationMessage::Snapshot(m)) => appendf!(buf, " snapshot {}", m.relId),
            Some(SharedInvalidationMessage::RelSync(m)) => appendf!(buf, " relsync {}", m.relid),
            None => {
                // An id outside the union's vocabulary.
                appendf!(buf, " unrecognized id {}", msgs.raw_id(i))
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::record;
    use mcx::MemoryContext;
    use types_storage::sinval::{SHAREDINVALRELCACHE_ID, SHAREDINVALSMGR_ID};

    fn desc(info: u8, data: &[u8]) -> String {
        let ctx = MemoryContext::new("test");
        let mut buf = PgString::new_in(ctx.mcx());
        let record = record(ctx.mcx(), info, data, &[]);
        standby_desc(&mut buf, &record).unwrap();
        buf.as_str().to_string()
    }

    #[test]
    fn formats_locks() {
        let mut rec = Vec::new();
        rec.extend_from_slice(&1i32.to_ne_bytes());
        rec.extend_from_slice(&9u32.to_ne_bytes());
        rec.extend_from_slice(&5u32.to_ne_bytes());
        rec.extend_from_slice(&77u32.to_ne_bytes());
        assert_eq!(desc(XLOG_STANDBY_LOCK, &rec), "xid 9 db 5 rel 77 ");
    }

    #[test]
    fn formats_running_xacts() {
        let mut rec = Vec::new();
        rec.extend_from_slice(&2i32.to_ne_bytes()); // xcnt
        rec.extend_from_slice(&1i32.to_ne_bytes()); // subxcnt
        rec.extend_from_slice(&[1, 0, 0, 0]); // subxid_overflow + pad
        rec.extend_from_slice(&100u32.to_ne_bytes()); // nextXid
        rec.extend_from_slice(&90u32.to_ne_bytes()); // oldestRunningXid
        rec.extend_from_slice(&99u32.to_ne_bytes()); // latestCompletedXid
        rec.extend_from_slice(&91u32.to_ne_bytes());
        rec.extend_from_slice(&92u32.to_ne_bytes());
        rec.extend_from_slice(&93u32.to_ne_bytes());
        assert_eq!(
            desc(XLOG_RUNNING_XACTS, &rec),
            "nextXid 100 latestCompletedXid 99 oldestRunningXid 90; \
             2 xacts: 91 92; subxid overflowed; 1 subxacts: 93"
        );
    }

    #[test]
    fn formats_invalidations() {
        let mut rec = Vec::new();
        rec.extend_from_slice(&5u32.to_ne_bytes()); // dbId
        rec.extend_from_slice(&6u32.to_ne_bytes()); // tsId
        rec.extend_from_slice(&[1, 0, 0, 0]); // relcacheInitFileInval + pad
        rec.extend_from_slice(&3i32.to_ne_bytes()); // nmsgs
        // catcache msg id 7
        let mut msg = [0u8; 16];
        msg[0] = 7;
        rec.extend_from_slice(&msg);
        // relcache msg, relId 123 @8
        let mut msg = [0u8; 16];
        msg[0] = SHAREDINVALRELCACHE_ID as u8;
        msg[8..12].copy_from_slice(&123u32.to_ne_bytes());
        rec.extend_from_slice(&msg);
        // smgr msg
        let mut msg = [0u8; 16];
        msg[0] = SHAREDINVALSMGR_ID as u8;
        rec.extend_from_slice(&msg);
        assert_eq!(
            desc(XLOG_INVALIDATIONS, &rec),
            "; relcache init file inval dbid 5 tsid 6; inval msgs: catcache 7 relcache 123 smgr"
        );
    }

    #[test]
    fn identifies() {
        assert_eq!(standby_identify(XLOG_STANDBY_LOCK), Some("LOCK"));
        assert_eq!(standby_identify(XLOG_INVALIDATIONS), Some("INVALIDATIONS"));
        assert_eq!(standby_identify(0x30), None);
    }
}
