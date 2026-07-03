use crate::{appendf, append_timestamptz, rec_data, rec_info, Rec};
use xlogreader_seams::XLogReaderState;
use stringinfo::StringInfo;
use types_core::{RepOriginId, TimestampTz, TransactionId, XLogRecPtr, XlXactStatsItem};
use types_error::PgResult;
use types_storage::RelFileLocator;
use xact::{
    parse_abort_record, parse_commit_record, XactCompletionApplyFeedback,
    XactCompletionForceSyncCommit, XactCompletionRelcacheInitFileInval, XACT_XINFO_HAS_ORIGIN, XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_ASSIGNMENT,
    XLOG_XACT_COMMIT, XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_INVALIDATIONS, XLOG_XACT_OPMASK,
    XLOG_XACT_PREPARE,
};

const InvalidRepOriginId: RepOriginId = 0;

fn MAXALIGN(n: usize) -> usize {
    (n + 7) & !7
}

// xl_xact_parsed_prepare, reduced to what xact_desc_prepare prints.
struct ParsedPrepare<'a> {
    xact_time: TimestampTz,
    origin_lsn: XLogRecPtr,
    origin_timestamp: TimestampTz,
    dbId: u32,
    gid: &'a [u8],
    subxacts: Vec<TransactionId>,
    xlocators: Vec<RelFileLocator>,
    abortlocators: Vec<RelFileLocator>,
    stats: Vec<XlXactStatsItem>,
    abortstats: Vec<XlXactStatsItem>,
    msgs_raw: &'a [u8],
    nmsgs: usize,
    initfileinval: bool,
    tsId: u32,
}

// ParsePrepareRecord (xactdesc.c) over the xl_xact_prepare layout (xact.h):
// magic 0, total_len 4, xid 8, database 12, prepared_at 16, owner 24,
// nsubxacts 28, ncommitrels 32, nabortrels 36, ncommitstats 40,
// nabortstats 44, ninvalmsgs 48, initfileinval 52, gidlen 54, origin_lsn 56,
// origin_timestamp 64; MAXALIGN(sizeof) = 72.
fn parse_prepare_record<'a>(data: &'a [u8]) -> PgResult<(ParsedPrepare<'a>, TransactionId)> {
    let r = Rec(data);
    let what = "xl_xact_prepare";
    let xid = r.u32(8, what)?;
    let database = r.u32(12, what)?;
    let prepared_at = r.i64(16, what)?;
    let nsubxacts = r.i32(28, what)? as usize;
    let ncommitrels = r.i32(32, what)? as usize;
    let nabortrels = r.i32(36, what)? as usize;
    let ncommitstats = r.i32(40, what)? as usize;
    let nabortstats = r.i32(44, what)? as usize;
    let ninvalmsgs = r.i32(48, what)? as usize;
    let initfileinval = r.u8(52, what)? != 0;
    let gidlen = r.u16(54, what)? as usize;
    let origin_lsn = r.u64(56, what)?;
    let origin_timestamp = r.i64(64, what)?;

    let mut off = 72;
    let gid_full = data.get(off..off + gidlen).ok_or_else(|| crate::record_truncated(what))?;
    let gid = match gid_full.iter().position(|&b| b == 0) {
        Some(n) => &gid_full[..n],
        None => gid_full,
    };
    off += MAXALIGN(gidlen);

    let mut subxacts = Vec::with_capacity(nsubxacts);
    for i in 0..nsubxacts {
        subxacts.push(r.u32(off + 4 * i, what)?);
    }
    off += MAXALIGN(nsubxacts * 4);

    let rels = |n: usize, off: &mut usize| -> PgResult<Vec<RelFileLocator>> {
        let mut v = Vec::with_capacity(n);
        for i in 0..n {
            v.push(RelFileLocator::new(
                r.u32(*off + 12 * i, what)?,
                r.u32(*off + 12 * i + 4, what)?,
                r.u32(*off + 12 * i + 8, what)?,
            ));
        }
        *off += MAXALIGN(n * 12);
        Ok(v)
    };
    let xlocators = rels(ncommitrels, &mut off)?;
    let abortlocators = rels(nabortrels, &mut off)?;

    // xl_xact_stats_item: kind, dboid, objid_lo, objid_hi (4×u32).
    let stats_at = |n: usize, off: &mut usize| -> PgResult<Vec<XlXactStatsItem>> {
        let mut v = Vec::with_capacity(n);
        for i in 0..n {
            let base = *off + 16 * i;
            let lo = r.u32(base + 8, what)? as u64;
            let hi = r.u32(base + 12, what)? as u64;
            v.push(XlXactStatsItem {
                kind: r.i32(base, what)?,
                dboid: r.u32(base + 4, what)?,
                objid: hi << 32 | lo,
            });
        }
        *off += MAXALIGN(n * 16);
        Ok(v)
    };
    let stats = stats_at(ncommitstats, &mut off)?;
    let abortstats = stats_at(nabortstats, &mut off)?;

    let msgs_raw = data
        .get(off..off + ninvalmsgs * 16)
        .ok_or_else(|| crate::record_truncated(what))?;

    Ok((
        ParsedPrepare {
            xact_time: prepared_at,
            origin_lsn,
            origin_timestamp,
            dbId: database,
            gid,
            subxacts,
            xlocators,
            abortlocators,
            stats,
            abortstats,
            msgs_raw,
            nmsgs: ninvalmsgs,
            initfileinval,
            tsId: 0,
        },
        xid,
    ))
}

fn xact_desc_relations(
    buf: &mut StringInfo<'_>,
    label: &str,
    xlocators: &[RelFileLocator],
) -> PgResult<()> {
    if !xlocators.is_empty() {
        appendf!(buf, "; {label}:")?;
        for loc in xlocators {
            let path = relpath_seams::relpathperm::call(*loc, types_core::ForkNumber::MAIN_FORKNUM);
            appendf!(buf, " {path}")?;
        }
    }
    Ok(())
}

fn xact_desc_subxacts(buf: &mut StringInfo<'_>, subxacts: &[TransactionId]) -> PgResult<()> {
    if !subxacts.is_empty() {
        buf.append_str("; subxacts:")?;
        for x in subxacts {
            appendf!(buf, " {x}")?;
        }
    }
    Ok(())
}

fn xact_desc_stats(
    buf: &mut StringInfo<'_>,
    label: &str,
    dropped_stats: &[XlXactStatsItem],
) -> PgResult<()> {
    if !dropped_stats.is_empty() {
        appendf!(buf, "; {label}dropped stats:")?;
        for it in dropped_stats {
            appendf!(buf, " {}/{}/{}", it.kind, it.dboid, it.objid)?;
        }
    }
    Ok(())
}

fn xact_desc_commit(
    buf: &mut StringInfo<'_>,
    info: u8,
    data: &[u8],
    origin_id: RepOriginId,
) -> PgResult<()> {
    let parsed = parse_commit_record(info, data)?;

    if parsed.twophase_xid != 0 {
        appendf!(buf, "{}: ", parsed.twophase_xid)?;
    }
    append_timestamptz(buf, parsed.xact_time)?;

    xact_desc_relations(buf, "rels", &parsed.xlocators)?;
    xact_desc_subxacts(buf, &parsed.subxacts)?;
    xact_desc_stats(buf, "", &parsed.stats)?;

    crate::standbydesc::standby_desc_invalidations(
        buf,
        &parsed.msgs,
        parsed.db_id,
        parsed.ts_id,
        XactCompletionRelcacheInitFileInval(parsed.xinfo),
    )?;

    if XactCompletionApplyFeedback(parsed.xinfo) {
        buf.append_str("; apply_feedback")?;
    }
    if XactCompletionForceSyncCommit(parsed.xinfo) {
        buf.append_str("; sync")?;
    }
    if parsed.xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        desc_origin(buf, origin_id, parsed.origin_lsn, parsed.origin_timestamp)?;
    }
    Ok(())
}

fn xact_desc_abort(
    buf: &mut StringInfo<'_>,
    info: u8,
    data: &[u8],
    origin_id: RepOriginId,
) -> PgResult<()> {
    let parsed = parse_abort_record(info, data)?;

    if parsed.twophase_xid != 0 {
        appendf!(buf, "{}: ", parsed.twophase_xid)?;
    }
    append_timestamptz(buf, parsed.xact_time)?;

    xact_desc_relations(buf, "rels", &parsed.xlocators)?;
    xact_desc_subxacts(buf, &parsed.subxacts)?;

    if parsed.xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        desc_origin(buf, origin_id, parsed.origin_lsn, parsed.origin_timestamp)?;
    }

    xact_desc_stats(buf, "", &parsed.stats)?;
    Ok(())
}

fn xact_desc_prepare(
    buf: &mut StringInfo<'_>,
    _info: u8,
    data: &[u8],
    origin_id: RepOriginId,
) -> PgResult<()> {
    let (parsed, _xid) = parse_prepare_record(data)?;

    buf.append_str("gid ")?;
    buf.append_bytes(parsed.gid)?;
    buf.append_str(": ")?;
    append_timestamptz(buf, parsed.xact_time)?;

    xact_desc_relations(buf, "rels(commit)", &parsed.xlocators)?;
    xact_desc_relations(buf, "rels(abort)", &parsed.abortlocators)?;
    xact_desc_stats(buf, "commit ", &parsed.stats)?;
    xact_desc_stats(buf, "abort ", &parsed.abortstats)?;
    xact_desc_subxacts(buf, &parsed.subxacts)?;

    crate::standbydesc::standby_desc_invalidations_raw(
        buf,
        parsed.nmsgs,
        parsed.msgs_raw,
        parsed.dbId,
        parsed.tsId,
        parsed.initfileinval,
    )?;

    if origin_id != InvalidRepOriginId {
        desc_origin(buf, origin_id, parsed.origin_lsn, parsed.origin_timestamp)?;
    }
    Ok(())
}

fn desc_origin(
    buf: &mut StringInfo<'_>,
    origin_id: RepOriginId,
    lsn: XLogRecPtr,
    timestamp: TimestampTz,
) -> PgResult<()> {
    appendf!(buf, "; origin: node {origin_id}, lsn {:X}/{:X}, at ", (lsn >> 32) as u32, lsn as u32)?;
    append_timestamptz(buf, timestamp)
}

pub fn xact_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let data = rec_data(record);
    let full_info = rec_info(record);
    let info = full_info & XLOG_XACT_OPMASK;
    let origin_id = record.record.as_ref().map_or(0, |r| r.record_origin);

    if info == XLOG_XACT_COMMIT || info == XLOG_XACT_COMMIT_PREPARED {
        xact_desc_commit(buf, full_info, data, origin_id)?;
    } else if info == XLOG_XACT_ABORT || info == XLOG_XACT_ABORT_PREPARED {
        xact_desc_abort(buf, full_info, data, origin_id)?;
    } else if info == XLOG_XACT_PREPARE {
        xact_desc_prepare(buf, full_info, data, origin_id)?;
    } else if info == XLOG_XACT_ASSIGNMENT {
        // xl_xact_assignment: xtop 0, nsubxacts 4, xsub[] 8.
        let r = Rec(data);
        let xtop = r.u32(0, "xl_xact_assignment")?;
        let nsubxacts = r.i32(4, "xl_xact_assignment")?.max(0) as usize;
        appendf!(buf, "xtop {xtop}: ")?;
        buf.append_str("subxacts:")?;
        for i in 0..nsubxacts {
            appendf!(buf, " {}", r.u32(8 + 4 * i, "xl_xact_assignment")?)?;
        }
    } else if info == XLOG_XACT_INVALIDATIONS {
        // xl_xact_invals: nmsgs 0, msgs[] 4.
        let r = Rec(data);
        let nmsgs = r.i32(0, "xl_xact_invals")?.max(0) as usize;
        let raw = data.get(4..4 + nmsgs * 16).ok_or_else(|| crate::record_truncated("xl_xact_invals"))?;
        crate::standbydesc::standby_desc_invalidations_raw(buf, nmsgs, raw, 0, 0, false)?;
    }
    Ok(())
}

pub fn xact_identify(info: u8) -> Option<&'static str> {
    match info & XLOG_XACT_OPMASK {
        XLOG_XACT_COMMIT => Some("COMMIT"),
        XLOG_XACT_PREPARE => Some("PREPARE"),
        XLOG_XACT_ABORT => Some("ABORT"),
        XLOG_XACT_COMMIT_PREPARED => Some("COMMIT_PREPARED"),
        XLOG_XACT_ABORT_PREPARED => Some("ABORT_PREPARED"),
        XLOG_XACT_ASSIGNMENT => Some("ASSIGNMENT"),
        XLOG_XACT_INVALIDATIONS => Some("INVALIDATION"),
        _ => None,
    }
}
