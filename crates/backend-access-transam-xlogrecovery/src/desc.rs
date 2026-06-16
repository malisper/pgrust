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
    panic!(
        "blocked: xlogrecovery::desc::rm_redo_error_callback — renders xlog_outdesc + \
         xlog_block_info; pending desc-family fill"
    )
}

/// `void xlog_outdesc(StringInfo buf, XLogReaderState *record)` (xlogrecovery.c)
pub fn xlog_outdesc(_buf: &mut String, _record: RecordRef) {
    panic!(
        "blocked: xlogrecovery::desc::xlog_outdesc — rmgr name/identify/desc dispatch \
         (GetRmgr().rm_desc takes Mcx+PgString, requires re-signing these debug fns to thread \
         an allocation context); pending desc-family fill"
    )
}

/// `static void xlog_outrec(StringInfo buf, XLogReaderState *record)`
/// (xlogrecovery.c)
pub(crate) fn xlog_outrec(_buf: &mut String, _record: RecordRef) {
    panic!(
        "blocked: xlogrecovery::desc::xlog_outrec — WAL_DEBUG-only record dump (prev/xid/len + \
         xlog_block_info over the held reader); pending desc-family fill"
    )
}

/// `static void xlog_block_info(StringInfo buf, XLogReaderState *record)`
/// (xlogrecovery.c)
pub(crate) fn xlog_block_info(_buf: &mut String, _record: RecordRef) {
    panic!(
        "blocked: xlogrecovery::desc::xlog_block_info — per-block-ref rendering over the held \
         reader's block tags (xlog_rec_get_block_tag_extended/has_block_image); pending desc-family fill"
    )
}
