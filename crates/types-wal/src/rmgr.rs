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

use mcx::{Mcx, PgBox, PgString, PgVec};

use types_core::{
    uint32, BlockNumber, TimeLineID, XLogRecPtr, XLogSegNo, MAXPGPATH,
};
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

/// `WALOpenSegment` (access/xlogreader.h) — the WAL segment currently open for
/// reading. `ws_file` is the OS file descriptor (`-1` when closed, like the C
/// negative-fd convention).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WALOpenSegment {
    /// `int ws_file` — segment file descriptor (`-1` when none open).
    pub ws_file: i32,
    /// `XLogSegNo ws_segno` — segment number.
    pub ws_segno: XLogSegNo,
    /// `TimeLineID ws_tli` — timeline of the currently open file.
    pub ws_tli: TimeLineID,
}

impl Default for WALOpenSegment {
    fn default() -> Self {
        // C `WALOpenSegmentInit` resets ws_file to -1 (no fd open).
        Self {
            ws_file: -1,
            ws_segno: 0,
            ws_tli: 0,
        }
    }
}

/// `WALSegmentContext` (access/xlogreader.h) — context describing the WAL
/// segments being read. `ws_dir` holds the directory path as server-encoding
/// bytes (the C `char ws_dir[MAXPGPATH]`), NUL-padded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WALSegmentContext {
    /// `char ws_dir[MAXPGPATH]`.
    pub ws_dir: [u8; MAXPGPATH],
    /// `int ws_segsize`.
    pub ws_segsize: i32,
}

impl Default for WALSegmentContext {
    fn default() -> Self {
        Self {
            ws_dir: [0; MAXPGPATH],
            ws_segsize: 0,
        }
    }
}

/// `XLogPageReadCB` (access/xlogreader.h) — the data-input callback:
/// `int (*)(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen,
/// XLogRecPtr targetRecPtr, char *readBuf)`. Returns the number of bytes read
/// (an [`XLogPageReadResult`] code on failure); reads into the reader's
/// `readBuf` and may `ereport(ERROR)`, carried on `Err`. The substrate
/// implementation is `xlogutils::read_local_xlog_page`.
pub type XLogPageReadCB = fn(
    reader: &mut XLogReaderState<'_>,
    target_page_ptr: XLogRecPtr,
    req_len: i32,
    target_rec_ptr: XLogRecPtr,
) -> PgResult<i32>;

/// `WALSegmentOpenCB` (access/xlogreader.h) — the segment-open callback:
/// `void (*)(XLogReaderState *xlogreader, XLogSegNo nextSegNo,
/// TimeLineID *tli_p)`. On success it sets `reader->seg.ws_file` to the open
/// file descriptor; on failure it `ereport(ERROR)`s and does not return
/// (carried on `Err`). `tli_p` is an in/out argument: the caller passes the
/// timeline to look the segment up in, and the callback may overwrite it with
/// the TLI it actually opened — modeled as `&mut TimeLineID`.
pub type WALSegmentOpenCB = fn(
    reader: &mut XLogReaderState<'_>,
    next_seg_no: XLogSegNo,
    tli_p: &mut TimeLineID,
) -> PgResult<()>;

/// `WALSegmentCloseCB` (access/xlogreader.h) — the segment-close callback:
/// `void (*)(XLogReaderState *xlogreader)`. Sets `reader->seg.ws_file` to a
/// negative number. Infallible.
pub type WALSegmentCloseCB = fn(reader: &mut XLogReaderState<'_>);

/// `XLogReaderRoutine` (access/xlogreader.h) — the operational callbacks an
/// `XLogReaderState` dispatches through. The C struct holds three function
/// pointers; a caller that never drives a record read may leave any of them
/// `NULL` (modeled as `None`). `page_read` is the data-input callback;
/// `segment_open`/`segment_close` are required only by callers that use the
/// built-in `page_read` helpers (`read_local_xlog_page` / `WALRead`).
#[derive(Clone, Copy, Debug, Default)]
pub struct XLogReaderRoutine {
    /// `XLogPageReadCB page_read` — data input. `None` is the C NULL.
    pub page_read: Option<XLogPageReadCB>,
    /// `WALSegmentOpenCB segment_open`. `None` is the C NULL.
    pub segment_open: Option<WALSegmentOpenCB>,
    /// `WALSegmentCloseCB segment_close`. `None` is the C NULL.
    pub segment_close: Option<WALSegmentCloseCB>,
}

/// `XLogPageReadResult` (access/xlogreader.h) — return-code sentinels from
/// `XLogPageReadCB`.
pub const XLREAD_SUCCESS: i32 = 0;
/// `XLREAD_FAIL` — failed during reading a record.
pub const XLREAD_FAIL: i32 = -1;
/// `XLREAD_WOULDBLOCK` — nonblocking mode only, no data available.
pub const XLREAD_WOULDBLOCK: i32 = -2;

/// `XLogReaderState` (access/xlogreader.h) — the full reader state. `'mcx` is
/// the reader's decode-buffer context: the C struct pallocs oversized decoded
/// records and the circular decode buffer in the reader's `MemoryContext`, so
/// the decoded payload (`record`, `main_data`, per-block data) lives in this
/// external arena and is *not* self-referential into the reader.
///
/// The decode circular-buffer ring (`decode_buffer*`) and the per-segment OS
/// state (`readBuf`/`seg`/`segcxt`) are modeled as owned bytes/handles; the
/// `decode_arena` `Mcx` handle is the C `MemoryContext` the reader allocates
/// decoded records in (`None` before `XLogReaderAllocate` wires it, mirroring
/// the C all-zero pre-init state). The operational callbacks live in
/// `routine` (`page_read`/`segment_open`/`segment_close`).
#[derive(Default)]
pub struct XLogReaderState<'mcx> {
    // ---- Operational callbacks ----
    /// `XLogReaderRoutine routine` — the data-input / segment-open /
    /// segment-close callbacks (`page_read`, `segment_open`, `segment_close`).
    /// All-`None` (the C all-NULL routine) is the `Default` pre-init state.
    pub routine: XLogReaderRoutine,

    // ---- Public parameters ----
    /// `uint64 system_identifier` — system identifier of the xlog files
    /// (0 when unknown/unimportant).
    pub system_identifier: u64,

    /// `void *private_data` — opaque data for the reader's callbacks (not used
    /// by xlogreader itself). The C `void *` block is allocated in the
    /// reader's allocator and cast to the owner's concrete private struct
    /// (e.g. `ReadLocalXLogPageNoWaitPrivate`); modeled as a type-erased
    /// arena box the owner downcasts. `None` is the C NULL.
    pub private_data: Option<PgBox<'mcx, dyn core::any::Any>>,

    /// `XLogRecPtr ReadRecPtr` — start of last record read.
    pub ReadRecPtr: XLogRecPtr,
    /// `XLogRecPtr EndRecPtr` — end+1 of last record read.
    pub EndRecPtr: XLogRecPtr,

    /// `XLogRecPtr abortedRecPtr` — start of a partial record at the end of WAL
    /// (set at the end of recovery; `InvalidXLogRecPtr` if none).
    pub abortedRecPtr: XLogRecPtr,
    /// `XLogRecPtr missingContrecPtr` — start location of the missing
    /// contrecord of `abortedRecPtr`.
    pub missingContrecPtr: XLogRecPtr,
    /// `XLogRecPtr overwrittenRecPtr` — set when
    /// `XLP_FIRST_IS_OVERWRITE_CONTRECORD` is found.
    pub overwrittenRecPtr: XLogRecPtr,

    // ---- Decoded representation of current record ----
    /// `XLogRecPtr DecodeRecPtr` — start of last record decoded.
    pub DecodeRecPtr: XLogRecPtr,
    /// `XLogRecPtr NextRecPtr` — end+1 of last record decoded (position to
    /// decode next).
    pub NextRecPtr: XLogRecPtr,
    /// `XLogRecPtr PrevRecPtr` — start of previous record decoded.
    pub PrevRecPtr: XLogRecPtr,

    /// `DecodedXLogRecord *record` — last record returned by
    /// `XLogReadRecord()`; `None` is the C `NULL`. The payload borrows the
    /// external `decode_arena`, so the reader is not self-referential.
    pub record: Option<DecodedXLogRecord<'mcx>>,

    // ---- private/internal state ----
    /// `MemoryContext` the reader pallocs decoded records / the decode buffer
    /// in. `None` before `XLogReaderAllocate` wires it (the C all-zero
    /// pre-init state). `Copy` handle into an external arena.
    pub decode_arena: Option<Mcx<'mcx>>,

    /// `char *decode_buffer` — circular decode buffer (owned bytes; `None` is
    /// the C NULL, before `XLogReaderSetDecodeBuffer`). Individual records are
    /// never split across the wrap; oversized records are allocated separately.
    pub decode_buffer: Option<PgVec<'mcx, u8>>,
    /// `size_t decode_buffer_size`.
    pub decode_buffer_size: usize,
    /// `bool free_decode_buffer` — whether the reader owns/must free the
    /// decode buffer.
    pub free_decode_buffer: bool,
    /// `char *decode_buffer_head` — read cursor, as an offset into
    /// `decode_buffer`.
    pub decode_buffer_head: usize,
    /// `char *decode_buffer_tail` — write cursor, as an offset into
    /// `decode_buffer`.
    pub decode_buffer_tail: usize,

    /// `DecodedXLogRecord *decode_queue_head` / `*decode_queue_tail` — the
    /// queue of decoded records (a C linked list chaining `DecodedXLogRecord`
    /// via `->next`, the records usually consecutive in `decode_buffer` but
    /// some `palloc`'d separately when oversized).
    ///
    /// Modeled as a FIFO over the *external* `decode_arena` (per the borrowed
    /// `'mcx` payload contract, **not** the src-idiomatic owned-Vec): the
    /// records live in `decode_queue` (allocated in `decode_arena`, like the C
    /// records live in the reader's context), and `decode_queue_head` is the
    /// read cursor — the oldest still-queued record is
    /// `decode_queue[decode_queue_head]` (the C `decode_queue_head`), the
    /// newest is `decode_queue.last()` (the C `decode_queue_tail`). Records
    /// before `decode_queue_head` have been consumed by `XLogNextRecord`.
    /// `None`/empty is the C all-NULL empty queue. Because the records' byte
    /// slices borrow `decode_arena` and not the reader, this is not
    /// self-referential and preserves the `&'mcx [u8]` the consumers read.
    pub decode_queue: Option<PgVec<'mcx, DecodedXLogRecord<'mcx>>>,
    /// Read cursor into [`Self::decode_queue`]: the index of the C
    /// `decode_queue_head` (oldest still-queued) record. Equal to
    /// `decode_queue.len()` when the queue is empty (`decode_queue_head ==
    /// NULL`).
    pub decode_queue_head: usize,

    /// `char *readBuf` — buffer for the currently read page (`XLOG_BLCKSZ`
    /// bytes, valid up to at least `readLen`). Owned bytes; `None` is the C
    /// NULL (allocated by `XLogReaderAllocate`).
    pub readBuf: Option<PgVec<'mcx, u8>>,
    /// `uint32 readLen` — valid byte count in `readBuf`.
    pub readLen: uint32,

    /// `WALSegmentContext segcxt` — last read XLOG segment context.
    pub segcxt: WALSegmentContext,
    /// `WALOpenSegment seg` — currently open WAL segment.
    pub seg: WALOpenSegment,
    /// `uint32 segoff` — offset within the open segment for the data in
    /// `readBuf`.
    pub segoff: uint32,

    /// `XLogRecPtr latestPagePtr` — beginning of prior page read.
    pub latestPagePtr: XLogRecPtr,
    /// `TimeLineID latestPageTLI` — its TLI (timeline sanity checks).
    pub latestPageTLI: TimeLineID,

    /// `XLogRecPtr currRecPtr` — beginning of the WAL record being read.
    pub currRecPtr: XLogRecPtr,
    /// `TimeLineID currTLI` — timeline to read it from (0 if a lookup is
    /// required).
    pub currTLI: TimeLineID,
    /// `XLogRecPtr currTLIValidUntil` — safe point to read to in `currTLI` if
    /// it is historical (`InvalidXLogRecPtr` on the current timeline).
    pub currTLIValidUntil: XLogRecPtr,
    /// `TimeLineID nextTLI` — next timeline to read from once
    /// `currTLIValidUntil` is reached.
    pub nextTLI: TimeLineID,

    /// `char *readRecordBuf` — expandable buffer for a record that crosses a
    /// page boundary. Owned bytes; `None` is the C NULL.
    pub readRecordBuf: Option<PgVec<'mcx, u8>>,
    /// `uint32 readRecordBufSize`.
    pub readRecordBufSize: uint32,

    /// `char *errormsg_buf` — buffer holding the last error message
    /// (`None` is the C NULL, before `XLogReaderAllocate`); borrowed as
    /// `*errmsg` by `XLogReadRecord`/`XLogNextRecord`.
    pub errormsg_buf: Option<PgString<'mcx>>,
    /// `bool errormsg_deferred` — an error is pending return.
    pub errormsg_deferred: bool,

    /// `bool nonblocking` — tell `XLogPageReadCB` not to block waiting for
    /// data.
    pub nonblocking: bool,

    /// Interior-mutable slot for the `RestoreBlockImage` failure message. The
    /// C `RestoreBlockImage(XLogReaderState *record, ...)` writes
    /// `record->errormsg_buf` through the shared decoder pointer, but the
    /// xlogreader seam takes `&XLogReaderState` (the redo consumer holds the
    /// reader shared), so the message is stashed here via interior mutability
    /// and read back by `reader_errormsg_buf`. Empty when no restore error.
    pub restore_errmsg: core::cell::RefCell<alloc::string::String>,
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
