//! Resource-manager vocabulary (`access/rmgr.h`, the `RmgrData` descriptor of
//! `access/xlog_internal.h`).
//!
//! `RmgrData` is the per-rmgr callback table populated from
//! `access/rmgrlist.h` and dispatched by `access/transam/rmgr.c`. The
//! callbacks are nullable C function pointers, modeled as `Option<fn>` over
//! the owned types; each callback type's failure surface mirrors the C
//! implementations (a callback family where any implementation can
//! `ereport(ERROR)` returns `PgResult`).
//!
//! [`XLogReaderState`], [`LogicalDecodingContext`], and [`XLogRecordBuffer`]
//! are the real C structs (access/xlogreader.h, replication/logical.h,
//! replication/decode.h), trimmed per docs/types.md rule 3 to the fields
//! current ports consume; the xlogreader and logical-decoding ports widen
//! them as they land.

use mcx::{Mcx, PgString};

use types_core::{BlockNumber, XLogRecPtr};
use types_error::PgResult;

use crate::wal::DecodedXLogRecord;

// ---------------------------------------------------------------------------
// access/rmgr.h
// ---------------------------------------------------------------------------

/// `RM_NEXT_ID` — one past the last built-in rmgr id (the `RmgrIds` enum
/// sentinel after the 22 `rmgrlist.h` entries).
pub const RM_NEXT_ID: usize = 22;
/// `RM_MAX_BUILTIN_ID` (= `RM_NEXT_ID - 1`).
pub const RM_MAX_BUILTIN_ID: usize = RM_NEXT_ID - 1;
/// `RM_MIN_CUSTOM_ID`.
pub const RM_MIN_CUSTOM_ID: usize = 128;
/// `RM_MAX_CUSTOM_ID` (= `UINT8_MAX`).
pub const RM_MAX_CUSTOM_ID: usize = u8::MAX as usize;
/// `RM_N_IDS` (= `UINT8_MAX + 1`).
pub const RM_N_IDS: usize = u8::MAX as usize + 1;
/// `RM_N_BUILTIN_IDS` (= `RM_MAX_BUILTIN_ID + 1`).
pub const RM_N_BUILTIN_IDS: usize = RM_MAX_BUILTIN_ID + 1;
/// `RM_N_CUSTOM_IDS`.
pub const RM_N_CUSTOM_IDS: usize = RM_MAX_CUSTOM_ID - RM_MIN_CUSTOM_ID + 1;
/// `RM_EXPERIMENTAL_ID` — for extensions still in development that have not
/// reserved a unique RmgrId.
pub const RM_EXPERIMENTAL_ID: usize = 128;

/// `RmgrIdIsBuiltin(int rmid)` (access/rmgr.h).
pub const fn RmgrIdIsBuiltin(rmid: i32) -> bool {
    rmid <= RM_MAX_BUILTIN_ID as i32
}

/// `RmgrIdIsCustom(int rmid)` (access/rmgr.h).
pub const fn RmgrIdIsCustom(rmid: i32) -> bool {
    rmid >= RM_MIN_CUSTOM_ID as i32 && rmid <= RM_MAX_CUSTOM_ID as i32
}

/// `RmgrIdIsValid(rmid)` (access/rmgr.h).
pub const fn RmgrIdIsValid(rmid: i32) -> bool {
    RmgrIdIsBuiltin(rmid) || RmgrIdIsCustom(rmid)
}

// ---------------------------------------------------------------------------
// Callback parameter types (access/xlogreader.h, replication/logical.h,
// replication/decode.h), trimmed; the xlogreader / logical-decoding ports
// widen them as they land.
// ---------------------------------------------------------------------------

/// `XLogReaderState` (access/xlogreader.h), trimmed to the record-cursor
/// fields the rmgr callbacks consume. `'mcx` is the reader's decode-buffer
/// context (C pallocs oversized decoded records in the reader's context).
#[derive(Debug)]
pub struct XLogReaderState<'mcx> {
    /// `XLogRecPtr ReadRecPtr` — start of last record read.
    pub ReadRecPtr: XLogRecPtr,
    /// `XLogRecPtr EndRecPtr` — end+1 of last record read.
    pub EndRecPtr: XLogRecPtr,
    /// `DecodedXLogRecord *record` — last record returned by
    /// `XLogReadRecord()`; `None` is the C `NULL`.
    pub record: Option<DecodedXLogRecord<'mcx>>,
}

/// `LogicalDecodingContext` (replication/logical.h), trimmed.
pub struct LogicalDecodingContext<'mcx> {
    /// `MemoryContext context` — the context this is all allocated in.
    pub context: Mcx<'mcx>,
    /// `bool fast_forward` — fast-forward decoding context (no output
    /// plugin loaded).
    pub fast_forward: bool,
}

/// `XLogRecordBuffer` (replication/decode.h) — the unit of WAL data handed
/// to the `rm_decode` callbacks. Complete: the C struct has exactly these
/// three fields.
pub struct XLogRecordBuffer<'r, 'mcx> {
    /// `XLogRecPtr origptr`.
    pub origptr: XLogRecPtr,
    /// `XLogRecPtr endptr`.
    pub endptr: XLogRecPtr,
    /// `XLogReaderState *record` — the reader positioned on the record being
    /// decoded (decode.c only reads through it).
    pub record: &'r XLogReaderState<'mcx>,
}

// ---------------------------------------------------------------------------
// RmgrData (access/xlog_internal.h)
// ---------------------------------------------------------------------------

/// `void (*rm_redo) (XLogReaderState *record)`. Redo routines can
/// `ereport(ERROR)` (corrupt records, I/O failures), carried on `Err`.
pub type RmRedo = fn(record: &mut XLogReaderState<'_>) -> PgResult<()>;

/// `void (*rm_desc) (StringInfo buf, XLogReaderState *record)`. The C
/// `StringInfo` output buffer pallocs in `CurrentMemoryContext`, so the
/// owned buffer is a context-allocated [`PgString`]; appending allocates,
/// and the C OOM `ereport(ERROR)` surface is `Err`.
pub type RmDesc = fn(buf: &mut PgString<'_>, record: &XLogReaderState<'_>) -> PgResult<()>;

/// `const char *(*rm_identify) (uint8 info)`. The C callbacks return string
/// literals or `NULL` for an unrecognized info; infallible.
pub type RmIdentify = fn(info: u8) -> Option<&'static str>;

/// `void (*rm_startup) (void)`. The implementations (btree/gin/gist/spgist)
/// do `AllocSetContextCreate(CurrentMemoryContext, ...)` — they create their
/// recovery context under the caller's current context, so the owned shape
/// takes the parent context explicitly; OOM `ereport(ERROR)` is `Err`.
pub type RmStartup = fn(parent: Mcx<'_>) -> PgResult<()>;

/// `void (*rm_cleanup) (void)`. The implementations delete the recovery
/// context; infallible.
pub type RmCleanup = fn();

/// `void (*rm_mask) (char *pagedata, BlockNumber blkno)`. Mask routines call
/// the bufmask helpers, which `elog(ERROR)` on invalid page bounds.
pub type RmMask = fn(pagedata: &mut [u8], blkno: BlockNumber) -> PgResult<()>;

/// `void (*rm_decode) (struct LogicalDecodingContext *ctx,
/// struct XLogRecordBuffer *buf)`. Decode routines `elog(ERROR)` on
/// unexpected record info, carried on `Err`.
pub type RmDecode =
    fn(ctx: &mut LogicalDecodingContext<'_>, buf: &mut XLogRecordBuffer<'_, '_>) -> PgResult<()>;

/// `typedef struct RmgrData` (access/xlog_internal.h). Field order matches C.
///
/// `rm_name == None` marks an unused (unregistered custom) slot, exactly like
/// C's `rm_name == NULL`. `Copy`, like C's plain-old-data struct: every field
/// is a borrowed static `&str` or a function pointer.
#[derive(Clone, Copy, Debug)]
pub struct RmgrData {
    /// `const char *rm_name`. `None` marks an unused slot.
    pub rm_name: Option<&'static str>,
    pub rm_redo: Option<RmRedo>,
    pub rm_desc: Option<RmDesc>,
    pub rm_identify: Option<RmIdentify>,
    pub rm_startup: Option<RmStartup>,
    pub rm_cleanup: Option<RmCleanup>,
    pub rm_mask: Option<RmMask>,
    pub rm_decode: Option<RmDecode>,
}

impl RmgrData {
    /// An all-`None` slot (unregistered custom rmgr id).
    pub const EMPTY: RmgrData = RmgrData {
        rm_name: None,
        rm_redo: None,
        rm_desc: None,
        rm_identify: None,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
        rm_decode: None,
    };
}

impl Default for RmgrData {
    fn default() -> Self {
        RmgrData::EMPTY
    }
}
