//! WAL-record description helpers used by recovery logging
//! (`rm_redo_error_callback`, `xlog_outdesc`, `xlog_outrec`, `xlog_block_info`).
//!
//! Ported 1:1 from `src/backend/access/transam/xlogrecovery.c` (lines
//! 2293-2389), driving the held recovery reader ([`crate::walrecovery`]) and the
//! real rmgr dispatch table ([`::rmgr::GetRmgr`]).
//!
//! # Held-reader model
//!
//! C threads a `XLogReaderState *record` into these functions. Here the opaque
//! [`RecordRef`] names "the held reader's current record"; these helpers resolve
//! it against the live reader via [`crate::walrecovery::reader_state`], exactly
//! as the C dereferences `record`.
//!
//! # `StringInfo` -> `PgString`
//!
//! C's `StringInfo` output buffer pallocs in `CurrentMemoryContext`. The owned
//! shape threads an explicit allocation context ([`Mcx`]) and renders into a
//! context-allocated [`PgString`], matching the `rm_desc` seam contract
//! (`fn(&mut PgString, &XLogReaderState)`). Appends can `ereport(ERROR)` on OOM,
//! carried on `Err` (`PgString::try_push_str`).

extern crate alloc;

use alloc::format;
use alloc::string::String;

use ::mcx::{Mcx, PgString};
use ::types_core::primitive::ForkNumber;
use ::types_error::PgError;
use ::wal::wal::XLR_INFO_MASK;

use ::rmgr::GetRmgr;

use crate::core::{lsn_fmt, RecordRef};
use crate::walrecovery::reader_state;

/// `static void rm_redo_error_callback(void *arg)` (xlogrecovery.c:2296).
///
/// The error-context callback that renders the failing record:
/// `errcontext("WAL redo at %X/%X for %s", LSN_FORMAT_ARGS(record->ReadRecPtr),
/// buf.data)`. The ambient `error_context_stack` chain is retired in this repo
/// (see [`crate::replay`]); this returns the assembled context text so the
/// caller can attach it on error propagation.
pub(crate) fn rm_redo_error_callback(mcx: Mcx<'_>, record: RecordRef) -> Result<String, PgError> {
    let mut buf = PgString::new_in(mcx);

    xlog_outdesc(mcx, &mut buf, record)?;
    xlog_block_info(&mut buf, record)?;

    // translator: %s is a WAL record description
    Ok(format!(
        "WAL redo at {} for {}",
        lsn_fmt(reader_state().ReadRecPtr),
        buf.as_str()
    ))
}

/// `void xlog_outdesc(StringInfo buf, XLogReaderState *record)`
/// (xlogrecovery.c:2318).
///
/// Returns a string describing an `XLogRecord`, consisting of its identity
/// optionally followed by a colon, a space, and a further description.
pub fn xlog_outdesc<'mcx>(
    _mcx: Mcx<'mcx>,
    buf: &mut PgString<'mcx>,
    _record: RecordRef,
) -> Result<(), PgError> {
    let rmid = xlogreader::XLogRecGetRmid(reader_state());
    let rmgr = GetRmgr(rmid)?;
    let info = xlogreader::XLogRecGetInfo(reader_state());

    buf.try_push_str(rmgr.rm_name.expect("described record has a registered rmgr"))?;
    buf.try_push('/')?;

    let id = rmgr.rm_identify.and_then(|f| f(info));
    match id {
        None => buf.try_push_str(&format!("UNKNOWN ({:X}): ", info & !XLR_INFO_MASK))?,
        Some(id) => buf.try_push_str(&format!("{}: ", id))?,
    }

    let rm_desc = rmgr
        .rm_desc
        .expect("described record's rmgr has a desc routine");
    rm_desc(buf, reader_state())
}

/// `static void xlog_outrec(StringInfo buf, XLogReaderState *record)`
/// (xlogrecovery.c:2340). `WAL_DEBUG`-only record dump.
#[allow(dead_code)]
pub(crate) fn xlog_outrec(buf: &mut PgString<'_>, record: RecordRef) -> Result<(), PgError> {
    buf.try_push_str(&format!(
        "prev {}; xid {}",
        lsn_fmt(xlogreader::XLogRecGetPrev(reader_state())),
        xlogreader::XLogRecGetXid(reader_state())
    ))?;

    buf.try_push_str(&format!(
        "; len {}",
        xlogreader::XLogRecGetDataLen(reader_state())
    ))?;

    xlog_block_info(buf, record)
}

/// `static void xlog_block_info(StringInfo buf, XLogReaderState *record)`
/// (xlogrecovery.c:2357).
///
/// Returns a string giving information about all the blocks in an `XLogRecord`.
pub(crate) fn xlog_block_info(buf: &mut PgString<'_>, _record: RecordRef) -> Result<(), PgError> {
    let max_block_id = xlogreader::reader_max_block_id(reader_state());

    // decode block references
    let mut block_id: i32 = 0;
    while block_id <= max_block_id {
        let bid = block_id as u8;
        block_id += 1;

        let tag = match xlogreader::xlog_rec_get_block_tag_extended(
            reader_state(),
            bid,
        ) {
            Some(t) => t,
            None => continue,
        };

        if tag.forknum != ForkNumber::MAIN_FORKNUM {
            buf.try_push_str(&format!(
                "; blkref #{}: rel {}/{}/{}, fork {}, blk {}",
                bid,
                tag.rlocator.spcOid,
                tag.rlocator.dbOid,
                tag.rlocator.relNumber,
                tag.forknum as u32,
                tag.blkno
            ))?;
        } else {
            buf.try_push_str(&format!(
                "; blkref #{}: rel {}/{}/{}, blk {}",
                bid,
                tag.rlocator.spcOid,
                tag.rlocator.dbOid,
                tag.rlocator.relNumber,
                tag.blkno
            ))?;
        }

        if xlogreader::xlog_rec_has_block_image(reader_state(), bid) {
            buf.try_push_str(" FPW")?;
        }
    }

    Ok(())
}
