use crate::{appendf, append_timestamptz, rec_data, rec_info, Rec, XLR_INFO_MASK};
use stringinfo::StringInfo;
use types_core::ForkNumber;
use types_error::PgResult;
use xlogreader_seams::XLogReaderState;

// XLOG rmgr info bytes (pg_control.h) + BKPIMAGE flags (xlogrecord.h); local
// copies, transam_xlog/xlogreader would cycle via rmgr.
pub const XLOG_CHECKPOINT_SHUTDOWN: u8 = 0x00;
pub const XLOG_CHECKPOINT_ONLINE: u8 = 0x10;
pub const XLOG_NOOP: u8 = 0x20;
pub const XLOG_NEXTOID: u8 = 0x30;
pub const XLOG_SWITCH: u8 = 0x40;
pub const XLOG_BACKUP_END: u8 = 0x50;
pub const XLOG_PARAMETER_CHANGE: u8 = 0x60;
pub const XLOG_RESTORE_POINT: u8 = 0x70;
pub const XLOG_FPW_CHANGE: u8 = 0x80;
pub const XLOG_END_OF_RECOVERY: u8 = 0x90;
pub const XLOG_FPI_FOR_HINT: u8 = 0xA0;
pub const XLOG_FPI: u8 = 0xB0;
pub const XLOG_OVERWRITE_CONTRECORD: u8 = 0xD0;
pub const XLOG_CHECKPOINT_REDO: u8 = 0xE0;

const BKPIMAGE_HAS_HOLE: u8 = 0x01;
const BKPIMAGE_APPLY: u8 = 0x02;
const BKPIMAGE_COMPRESS_PGLZ: u8 = 0x04;
const BKPIMAGE_COMPRESS_LZ4: u8 = 0x08;
const BKPIMAGE_COMPRESS_ZSTD: u8 = 0x10;
const _: () = assert!(BKPIMAGE_HAS_HOLE != 0 && BKPIMAGE_APPLY != 0);

fn BKPIMAGE_COMPRESSED(bimg_info: u8) -> bool {
    bimg_info & (BKPIMAGE_COMPRESS_PGLZ | BKPIMAGE_COMPRESS_LZ4 | BKPIMAGE_COMPRESS_ZSTD) != 0
}

const BLCKSZ: u32 = 8192;

const WAL_LEVEL_MINIMAL: i32 = 0;
const WAL_LEVEL_REPLICA: i32 = 1;
const WAL_LEVEL_LOGICAL: i32 = 2;

// wal_level_options[] GUC table (owned by xlogdesc.c).
pub const wal_level_options: &[(&str, i32, bool)] = &[
    ("minimal", WAL_LEVEL_MINIMAL, false),
    ("replica", WAL_LEVEL_REPLICA, false),
    ("archive", WAL_LEVEL_REPLICA, true),
    ("hot_standby", WAL_LEVEL_REPLICA, true),
    ("logical", WAL_LEVEL_LOGICAL, false),
];

fn get_wal_level_string(wal_level: i32) -> &'static str {
    for &(name, val, _) in wal_level_options {
        if val == wal_level {
            return name;
        }
    }
    "?"
}

fn cstr_prefix(bytes: &[u8]) -> &[u8] {
    match bytes.iter().position(|&b| b == 0) {
        Some(n) => &bytes[..n],
        None => bytes,
    }
}

pub fn xlog_desc(buf: &mut StringInfo<'_>, record: &XLogReaderState) -> PgResult<()> {
    let rec = Rec(rec_data(record));
    let info = rec_info(record) & !XLR_INFO_MASK;

    if info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_CHECKPOINT_ONLINE {
        let checkpoint = controldata_utils::CheckPoint::from_bytes(rec.0);
        appendf!(
            buf,
            "redo {:X}/{:X}; \
             tli {}; prev tli {}; fpw {}; wal_level {}; xid {}:{}; oid {}; multi {}; offset {}; \
             oldest xid {} in DB {}; oldest multi {} in DB {}; \
             oldest/newest commit timestamp xid: {}/{}; \
             oldest running xid {}; {}",
            (checkpoint.redo >> 32) as u32,
            checkpoint.redo as u32,
            checkpoint.ThisTimeLineID,
            checkpoint.PrevTimeLineID,
            if checkpoint.fullPageWrites { "true" } else { "false" },
            get_wal_level_string(checkpoint.wal_level),
            checkpoint.nextXid.epoch(),
            checkpoint.nextXid.xid(),
            checkpoint.nextOid,
            checkpoint.nextMulti,
            checkpoint.nextMultiOffset,
            checkpoint.oldestXid,
            checkpoint.oldestXidDB,
            checkpoint.oldestMulti,
            checkpoint.oldestMultiDB,
            checkpoint.oldestCommitTsXid,
            checkpoint.newestCommitTsXid,
            checkpoint.oldestActiveXid,
            if info == XLOG_CHECKPOINT_SHUTDOWN { "shutdown" } else { "online" }
        )?;
    } else if info == XLOG_NEXTOID {
        let nextOid = rec.u32(0, "XLOG_NEXTOID")?;
        appendf!(buf, "{nextOid}")?;
    } else if info == XLOG_RESTORE_POINT {
        // xl_restore_point: rp_time at 0, rp_name[MAXFNAMELEN] at 8.
        buf.append_bytes(cstr_prefix(rec.0.get(8..).unwrap_or(&[])))?;
    } else if info == XLOG_FPI || info == XLOG_FPI_FOR_HINT {
        // no further information to print
    } else if info == XLOG_BACKUP_END {
        let startpoint = rec.u64(0, "XLOG_BACKUP_END")?;
        appendf!(buf, "{:X}/{:X}", (startpoint >> 32) as u32, startpoint as u32)?;
    } else if info == XLOG_PARAMETER_CHANGE {
        // xl_parameter_change (xlog_internal.h): six ints then two bools.
        let max_connections = rec.i32(0, "xl_parameter_change")?;
        let max_worker_processes = rec.i32(4, "xl_parameter_change")?;
        let max_wal_senders = rec.i32(8, "xl_parameter_change")?;
        let max_prepared_xacts = rec.i32(12, "xl_parameter_change")?;
        let max_locks_per_xact = rec.i32(16, "xl_parameter_change")?;
        let wal_level = rec.i32(20, "xl_parameter_change")?;
        let wal_log_hints = rec.u8(24, "xl_parameter_change")? != 0;
        let track_commit_timestamp = rec.u8(25, "xl_parameter_change")? != 0;
        appendf!(
            buf,
            "max_connections={max_connections} max_worker_processes={max_worker_processes} \
             max_wal_senders={max_wal_senders} max_prepared_xacts={max_prepared_xacts} \
             max_locks_per_xact={max_locks_per_xact} wal_level={} \
             wal_log_hints={} track_commit_timestamp={}",
            get_wal_level_string(wal_level),
            if wal_log_hints { "on" } else { "off" },
            if track_commit_timestamp { "on" } else { "off" }
        )?;
    } else if info == XLOG_FPW_CHANGE {
        let fpw = rec.u8(0, "XLOG_FPW_CHANGE")? != 0;
        buf.append_str(if fpw { "true" } else { "false" })?;
    } else if info == XLOG_END_OF_RECOVERY {
        // xl_end_of_recovery: end_time 0, ThisTimeLineID 8, PrevTimeLineID 12,
        // wal_level 16.
        let end_time = rec.i64(0, "xl_end_of_recovery")?;
        let this_tli = rec.u32(8, "xl_end_of_recovery")?;
        let prev_tli = rec.u32(12, "xl_end_of_recovery")?;
        let wal_level = rec.i32(16, "xl_end_of_recovery")?;
        appendf!(buf, "tli {this_tli}; prev tli {prev_tli}; time ")?;
        append_timestamptz(buf, end_time)?;
        appendf!(buf, "; wal_level {}", get_wal_level_string(wal_level))?;
    } else if info == XLOG_OVERWRITE_CONTRECORD {
        // xl_overwrite_contrecord: overwritten_lsn 0, overwrite_time 8.
        let lsn = rec.u64(0, "xl_overwrite_contrecord")?;
        let time = rec.i64(8, "xl_overwrite_contrecord")?;
        appendf!(buf, "lsn {:X}/{:X}; time ", (lsn >> 32) as u32, lsn as u32)?;
        append_timestamptz(buf, time)?;
    } else if info == XLOG_CHECKPOINT_REDO {
        let wal_level = rec.i32(0, "XLOG_CHECKPOINT_REDO")?;
        appendf!(buf, "wal_level {}", get_wal_level_string(wal_level))?;
    }
    Ok(())
}

pub fn xlog_identify(info: u8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_CHECKPOINT_SHUTDOWN => Some("CHECKPOINT_SHUTDOWN"),
        XLOG_CHECKPOINT_ONLINE => Some("CHECKPOINT_ONLINE"),
        XLOG_NOOP => Some("NOOP"),
        XLOG_NEXTOID => Some("NEXTOID"),
        XLOG_SWITCH => Some("SWITCH"),
        XLOG_BACKUP_END => Some("BACKUP_END"),
        XLOG_PARAMETER_CHANGE => Some("PARAMETER_CHANGE"),
        XLOG_RESTORE_POINT => Some("RESTORE_POINT"),
        XLOG_FPW_CHANGE => Some("FPW_CHANGE"),
        XLOG_END_OF_RECOVERY => Some("END_OF_RECOVERY"),
        XLOG_OVERWRITE_CONTRECORD => Some("OVERWRITE_CONTRECORD"),
        XLOG_FPI => Some("FPI"),
        XLOG_FPI_FOR_HINT => Some("FPI_FOR_HINT"),
        XLOG_CHECKPOINT_REDO => Some("CHECKPOINT_REDO"),
        _ => None,
    }
}

pub fn XLogRecGetBlockRefInfo(
    record: &XLogReaderState,
    pretty: bool,
    detailed_format: bool,
    buf: &mut StringInfo<'_>,
    fpi_len: Option<&mut u32>,
) -> PgResult<()> {
    let max_block_id = record.record.as_ref().map_or(-1, |r| r.max_block_id);
    let mut fpi_acc: u32 = 0;

    if detailed_format && pretty {
        buf.append_byte(b'\n')?;
    }

    for block_id in 0..=max_block_id.max(-1) {
        let block_id = block_id as u8;
        let Some((rlocator, forknum, blk, _)) = record.block_tag_extended(block_id) else {
            continue;
        };

        if detailed_format {
            if pretty {
                buf.append_byte(b'\t')?;
            } else if block_id > 0 {
                buf.append_byte(b' ')?;
            }
            appendf!(
                buf,
                "blkref #{block_id}: rel {}/{}/{} fork {} blk {blk}",
                rlocator.spcOid,
                rlocator.dbOid,
                rlocator.relNumber,
                fork_name(forknum)
            )?;
            if record.has_block_image(block_id) {
                let bb = record.block(block_id);
                fpi_acc += bb.bimg_len as u32;
                if BKPIMAGE_COMPRESSED(bb.bimg_info) {
                    let method = if bb.bimg_info & BKPIMAGE_COMPRESS_PGLZ != 0 {
                        "pglz"
                    } else if bb.bimg_info & BKPIMAGE_COMPRESS_LZ4 != 0 {
                        "lz4"
                    } else if bb.bimg_info & BKPIMAGE_COMPRESS_ZSTD != 0 {
                        "zstd"
                    } else {
                        "unknown"
                    };
                    appendf!(
                        buf,
                        " (FPW{}); hole: offset: {}, length: {}, \
                         compression saved: {}, method: {method}",
                        if record.block_image_apply(block_id) { "" } else { " for WAL verification" },
                        bb.hole_offset,
                        bb.hole_length,
                        BLCKSZ - bb.hole_length as u32 - bb.bimg_len as u32
                    )?;
                } else {
                    appendf!(
                        buf,
                        " (FPW{}); hole: offset: {}, length: {}",
                        if record.block_image_apply(block_id) { "" } else { " for WAL verification" },
                        bb.hole_offset,
                        bb.hole_length
                    )?;
                }
            }
            if pretty {
                buf.append_byte(b'\n')?;
            }
        } else {
            if forknum != ForkNumber::MAIN_FORKNUM {
                appendf!(
                    buf,
                    ", blkref #{block_id}: rel {}/{}/{} fork {} blk {blk}",
                    rlocator.spcOid,
                    rlocator.dbOid,
                    rlocator.relNumber,
                    fork_name(forknum)
                )?;
            } else {
                appendf!(
                    buf,
                    ", blkref #{block_id}: rel {}/{}/{} blk {blk}",
                    rlocator.spcOid,
                    rlocator.dbOid,
                    rlocator.relNumber
                )?;
            }
            if record.has_block_image(block_id) {
                fpi_acc += record.block(block_id).bimg_len as u32;
                if record.block_image_apply(block_id) {
                    buf.append_str(" FPW")?;
                } else {
                    buf.append_str(" FPW for WAL verification")?;
                }
            }
        }
    }

    if !detailed_format && pretty {
        buf.append_byte(b'\n')?;
    }

    if let Some(out) = fpi_len {
        *out += fpi_acc;
    }
    Ok(())
}

// forkNames[] (common/relpath.c).
fn fork_name(forknum: ForkNumber) -> &'static str {
    match forknum {
        ForkNumber::MAIN_FORKNUM => "main",
        ForkNumber::FSM_FORKNUM => "fsm",
        ForkNumber::VISIBILITYMAP_FORKNUM => "vm",
        ForkNumber::INIT_FORKNUM => "init",
        ForkNumber::InvalidForkNumber => "???",
    }
}
