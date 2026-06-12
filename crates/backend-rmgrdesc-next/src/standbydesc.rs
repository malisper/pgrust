//! `access/rmgrdesc/standbydesc.c` — rmgr descriptor routines for
//! storage/ipc/standby.c records, plus `standby_desc_invalidations` (shared
//! with the xact and heap-inplace descriptors).

use crate::{appendf, bool_at, i32_at, i8_at, u32_at};
use mcx::PgString;
use types_core::Oid;
use types_error::PgResult;
use types_wal::{XLogRecordView, XLR_INFO_MASK};

// storage/standbydefs.h
pub const XLOG_STANDBY_LOCK: u8 = 0x00;
pub const XLOG_RUNNING_XACTS: u8 = 0x10;
pub const XLOG_INVALIDATIONS: u8 = 0x20;

// storage/sinval.h message type ids
pub const SHAREDINVALCATALOG_ID: i8 = -1;
pub const SHAREDINVALRELCACHE_ID: i8 = -2;
pub const SHAREDINVALSMGR_ID: i8 = -3;
pub const SHAREDINVALRELMAP_ID: i8 = -4;
pub const SHAREDINVALSNAPSHOT_ID: i8 = -5;
pub const SHAREDINVALRELSYNC_ID: i8 = -6;

/// `sizeof(SharedInvalidationMessage)` (storage/sinval.h union, 16 bytes).
pub const SIZEOF_SHARED_INVALIDATION_MESSAGE: usize = 16;

/// `sizeof(xl_standby_lock)` — `{TransactionId xid; Oid dbOid; Oid relOid;}`.
const SIZEOF_XL_STANDBY_LOCK: usize = 12;

/// `standby_desc_running_xacts(StringInfo buf, xl_running_xacts *xlrec)`.
/// Layout: xcnt i32 @0, subxcnt i32 @4, subxid_overflow bool @8, nextXid u32
/// @12, oldestRunningXid u32 @16, latestCompletedXid u32 @20, xids @24.
fn standby_desc_running_xacts(buf: &mut PgString<'_>, rec: &[u8]) -> PgResult<()> {
    let xcnt = i32_at(rec, 0);
    let subxcnt = i32_at(rec, 4);
    let xid_at = |i: usize| u32_at(rec, 24 + i * 4);

    appendf!(
        buf,
        "nextXid {} latestCompletedXid {} oldestRunningXid {}",
        u32_at(rec, 12),
        u32_at(rec, 20),
        u32_at(rec, 16)
    );
    if xcnt > 0 {
        appendf!(buf, "; {} xacts:", xcnt);
        for i in 0..xcnt as usize {
            appendf!(buf, " {}", xid_at(i));
        }
    }

    if bool_at(rec, 8) {
        buf.try_push_str("; subxid overflowed")?;
    }

    if subxcnt > 0 {
        appendf!(buf, "; {} subxacts:", subxcnt);
        for i in 0..subxcnt as usize {
            appendf!(buf, " {}", xid_at(xcnt as usize + i));
        }
    }
    Ok(())
}

/// `standby_desc(StringInfo buf, XLogReaderState *record)`.
pub fn standby_desc(buf: &mut PgString<'_>, record: &XLogRecordView<'_>) -> PgResult<()> {
    let rec = record.data();
    let info = record.info() & !XLR_INFO_MASK;

    if info == XLOG_STANDBY_LOCK {
        // xl_standby_locks: nlocks i32 @0, locks @4
        let nlocks = i32_at(rec, 0);
        for i in 0..nlocks.max(0) as usize {
            let off = 4 + i * SIZEOF_XL_STANDBY_LOCK;
            appendf!(
                buf,
                "xid {} db {} rel {} ",
                u32_at(rec, off),
                u32_at(rec, off + 4),
                u32_at(rec, off + 8)
            );
        }
    } else if info == XLOG_RUNNING_XACTS {
        standby_desc_running_xacts(buf, rec)?;
    } else if info == XLOG_INVALIDATIONS {
        // xl_invalidations: dbId u32 @0, tsId u32 @4, relcacheInitFileInval
        // bool @8, nmsgs i32 @12, msgs @16
        let nmsgs = i32_at(rec, 12);
        standby_desc_invalidations(
            buf,
            nmsgs,
            &rec[16..],
            u32_at(rec, 0),
            u32_at(rec, 4),
            bool_at(rec, 8),
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
///
/// `msgs` is the raw bytes of `nmsgs` `SharedInvalidationMessage` entries
/// (16 bytes each).
pub fn standby_desc_invalidations(
    buf: &mut PgString<'_>,
    nmsgs: i32,
    msgs: &[u8],
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
        let msg = &msgs[i * SIZEOF_SHARED_INVALIDATION_MESSAGE..];
        let id = i8_at(msg, 0);

        if id >= 0 {
            appendf!(buf, " catcache {}", id);
        } else if id == SHAREDINVALCATALOG_ID {
            // SharedInvalCatalogMsg.catId: u32 @8
            appendf!(buf, " catalog {}", u32_at(msg, 8));
        } else if id == SHAREDINVALRELCACHE_ID {
            // SharedInvalRelcacheMsg.relId: u32 @8
            appendf!(buf, " relcache {}", u32_at(msg, 8));
        } else if id == SHAREDINVALSMGR_ID {
            // not expected, but print something anyway
            buf.try_push_str(" smgr")?;
        } else if id == SHAREDINVALRELMAP_ID {
            // not expected, but print something anyway
            appendf!(buf, " relmap db {}", u32_at(msg, 4));
        } else if id == SHAREDINVALSNAPSHOT_ID {
            // SharedInvalSnapshotMsg.relId: u32 @8
            appendf!(buf, " snapshot {}", u32_at(msg, 8));
        } else if id == SHAREDINVALRELSYNC_ID {
            // SharedInvalRelSyncMsg.relid: u32 @8
            appendf!(buf, " relsync {}", u32_at(msg, 8));
        } else {
            appendf!(buf, " unrecognized id {}", id);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

    fn desc(info: u8, data: &[u8]) -> String {
        let ctx = MemoryContext::new("test");
        let mut buf = PgString::new_in(ctx.mcx());
        let record = XLogRecordView::new(info, data, &[]);
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
