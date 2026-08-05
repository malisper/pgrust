use crate::{appendf, rec_data, rec_info, Rec, XLR_INFO_MASK};
use stringinfo::StringInfo;
use types_error::PgResult;
use types_storage::sinval::{SharedInvalidationMessage, SHARED_INVALIDATION_MESSAGE_SIZE};
use xlogreader_seams::XLogReaderState;

// standbydefs.h info bytes (the standby crate keeps its copies private).
pub const XLOG_STANDBY_LOCK: u8 = 0x00;
pub const XLOG_RUNNING_XACTS: u8 = 0x10;
pub const XLOG_INVALIDATIONS: u8 = 0x20;

fn standby_desc_running_xacts(buf: &mut StringInfo<'_>, rec: Rec<'_>) -> PgResult<()> {
    // xcnt 0, subxcnt 4, subxid_overflow 8, nextXid 12, oldestRunningXid 16,
    // latestCompletedXid 20, xids[] 24.
    let what = "xl_running_xacts";
    let xcnt = rec.i32(0, what)?.max(0) as usize;
    let subxcnt = rec.i32(4, what)?.max(0) as usize;
    appendf!(
        buf,
        "nextXid {} latestCompletedXid {} oldestRunningXid {}",
        rec.u32(12, what)?,
        rec.u32(20, what)?,
        rec.u32(16, what)?
    )?;
    if xcnt > 0 {
        appendf!(buf, "; {xcnt} xacts:")?;
        for i in 0..xcnt {
            appendf!(buf, " {}", rec.u32(24 + 4 * i, what)?)?;
        }
    }
    if rec.u8(8, what)? != 0 {
        buf.append_str("; subxid overflowed")?;
    }
    if subxcnt > 0 {
        appendf!(buf, "; {subxcnt} subxacts:")?;
        for i in 0..subxcnt {
            appendf!(buf, " {}", rec.u32(24 + 4 * (xcnt + i), what)?)?;
        }
    }
    Ok(())
}

pub fn standby_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = rec_info(record) & !XLR_INFO_MASK;

    if info == XLOG_STANDBY_LOCK {
        // nlocks 0, then { xid, dbOid, relOid } triples at 4.
        let what = "xl_standby_locks";
        let nlocks = rec.i32(0, what)?.max(0) as usize;
        for i in 0..nlocks {
            let base = 4 + 12 * i;
            appendf!(
                buf,
                "xid {} db {} rel {} ",
                rec.u32(base, what)?,
                rec.u32(base + 4, what)?,
                rec.u32(base + 8, what)?
            )?;
        }
    } else if info == XLOG_RUNNING_XACTS {
        standby_desc_running_xacts(buf, rec)?;
    } else if info == XLOG_INVALIDATIONS {
        // dbId 0, tsId 4, relcacheInitFileInval 8, nmsgs 12, msgs[] 16.
        let what = "xl_invalidations";
        let nmsgs = rec.i32(12, what)?.max(0) as usize;
        let raw = rec.0.get(16..16 + nmsgs * 16).ok_or_else(|| crate::record_truncated(what))?;
        standby_desc_invalidations_raw(
            buf,
            nmsgs,
            raw,
            rec.u32(0, what)?,
            rec.u32(4, what)?,
            rec.u8(8, what)? != 0,
        )?;
    }
    Ok(())
}

pub fn standby_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_STANDBY_LOCK => Some("LOCK"),
        XLOG_RUNNING_XACTS => Some("RUNNING_XACTS"),
        XLOG_INVALIDATIONS => Some("INVALIDATIONS"),
        _ => None,
    }
}

fn desc_one_invalidation(buf: &mut StringInfo<'_>, msg: &SharedInvalidationMessage) -> PgResult<()> {
    match msg {
        SharedInvalidationMessage::Catcache(m) => appendf!(buf, " catcache {}", m.id),
        SharedInvalidationMessage::Catalog(m) => appendf!(buf, " catalog {}", m.catId),
        SharedInvalidationMessage::Relcache(m) => appendf!(buf, " relcache {}", m.relId),
        SharedInvalidationMessage::Smgr(_) => buf.append_str(" smgr"),
        SharedInvalidationMessage::Relmap(m) => appendf!(buf, " relmap db {}", m.dbId),
        SharedInvalidationMessage::Snapshot(m) => appendf!(buf, " snapshot {}", m.relId),
        SharedInvalidationMessage::RelSync(m) => appendf!(buf, " relsync {}", m.relid),
    }
}

// standby_desc_invalidations (standbydesc.c) over pre-parsed messages.
pub fn standby_desc_invalidations(
    buf: &mut StringInfo<'_>,
    msgs: &[SharedInvalidationMessage],
    dbId: u32,
    tsId: u32,
    relcacheInitFileInval: bool,
) -> PgResult<()> {
    if msgs.is_empty() {
        return Ok(());
    }
    if relcacheInitFileInval {
        appendf!(buf, "; relcache init file inval dbid {dbId} tsid {tsId}")?;
    }
    buf.append_str("; inval msgs:")?;
    for msg in msgs {
        desc_one_invalidation(buf, msg)?;
    }
    Ok(())
}

// Same, over the raw wire array (standby/heap-inplace/xact-invalidations
// records); an undecodable id renders as C's "unrecognized id" arm.
pub fn standby_desc_invalidations_raw(
    buf: &mut StringInfo<'_>,
    nmsgs: usize,
    raw: &[u8],
    dbId: u32,
    tsId: u32,
    relcacheInitFileInval: bool,
) -> PgResult<()> {
    if nmsgs == 0 {
        return Ok(());
    }
    if relcacheInitFileInval {
        appendf!(buf, "; relcache init file inval dbid {dbId} tsid {tsId}")?;
    }
    buf.append_str("; inval msgs:")?;
    for chunk in raw.chunks_exact(SHARED_INVALIDATION_MESSAGE_SIZE).take(nmsgs) {
        let arr: [u8; SHARED_INVALIDATION_MESSAGE_SIZE] = chunk.try_into().unwrap();
        match SharedInvalidationMessage::from_wire_bytes(arr) {
            Some(msg) => desc_one_invalidation(buf, &msg)?,
            None => appendf!(buf, " unrecognized id {}", arr[0] as i8)?,
        }
    }
    Ok(())
}
