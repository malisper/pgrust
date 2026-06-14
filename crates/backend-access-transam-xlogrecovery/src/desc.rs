//! WAL-record description helpers used by recovery logging
//! (`rm_redo_error_callback`, `xlog_outdesc`, `xlog_outrec`, `xlog_block_info`).
//!
//! **Scaffold module.** Faithful signatures, honest `panic!` bodies the
//! family-fill lanes replace; the record fields are read through the
//! `xlog_rec_*` seams keyed by [`crate::core::RecordRef`].
//!
//! Ported from `src/backend/access/transam/xlogrecovery.c`.

use alloc::string::String;

use crate::core::RecordRef;

/// `static void rm_redo_error_callback(void *arg)` (xlogrecovery.c) — the
/// error-context callback that renders the failing record.
pub(crate) fn rm_redo_error_callback(_record: RecordRef) -> String {
    panic!("decomp: xlogrecovery::desc::rm_redo_error_callback not yet filled")
}

/// `void xlog_outdesc(StringInfo buf, XLogReaderState *record)` (xlogrecovery.c)
pub fn xlog_outdesc(_buf: &mut String, _record: RecordRef) {
    panic!("decomp: xlogrecovery::desc::xlog_outdesc not yet filled")
}

/// `static void xlog_outrec(StringInfo buf, XLogReaderState *record)`
/// (xlogrecovery.c)
pub(crate) fn xlog_outrec(_buf: &mut String, _record: RecordRef) {
    panic!("decomp: xlogrecovery::desc::xlog_outrec not yet filled")
}

/// `static void xlog_block_info(StringInfo buf, XLogReaderState *record)`
/// (xlogrecovery.c)
pub(crate) fn xlog_block_info(_buf: &mut String, _record: RecordRef) {
    panic!("decomp: xlogrecovery::desc::xlog_block_info not yet filled")
}
