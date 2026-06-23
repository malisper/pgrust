//! `access/rmgrdesc/xactdesc.c` — rmgr descriptor routines for
//! `access/transam/xact.c`, plus the commit/abort/prepare WAL-record parsers
//! shared with `pg_waldump`.
//!
//! `xact_desc(StringInfo buf, XLogReaderState *record)` formats an xact WAL
//! record into `buf`; `xact_identify(uint8 info)` maps the op-code to its
//! symbolic name (the `rm_desc` / `rm_identify` slots of the `RM_XACT_ID`
//! rmgr). Like the sibling describers (standbydesc / mxactdesc), this reads the
//! record body lazily through bounds-checked byte views; the only failure is
//! `appendStringInfo`'s palloc OOM `ereport(ERROR)`, surfaced as `PgResult`.
//!
//! The parsers `ParseCommitRecord` / `ParseAbortRecord` / `ParsePrepareRecord`
//! deconstruct a record body into a `Parsed*` value carrying the scalar fields
//! plus the byte offset of each variable-length array; the describer reads each
//! element through the accessors here, exactly as the C describer dereferences
//! the parsed pointers. They are `pub` because xactdesc.c shares them with the
//! frontend (`pg_waldump`) as the file comment explains.
//!
//! External reaches (all without restructuring): `standby_desc_invalidations`
//! is the ported sibling describer (`backend-rmgrdesc-next`, direct call);
//! `relpathperm` (`common/relpath.c`) and `timestamptz_to_str`
//! (`utils/adt/timestamp.c`) are unported leaves reached through their owners'
//! per-owner seams (they panic until those owners land).

#![allow(non_upper_case_globals)]

use mcx::{Mcx, PgString, PgVec};
use types_core::{ForkNumber, Oid, RelFileNumber, RepOriginId, TimestampTz, TransactionId, XLogRecPtr, INVALID_PROC_NUMBER, InvalidOid, InvalidRepOriginId, InvalidTransactionId};
use types_error::{PgError, PgResult};
use ::types_storage::sinval::SharedInvalMessages;
use ::types_storage::RelFileLocator;
use ::wal::rmgr::XLogReaderState;
use wal::{
    xact_completion_apply_feedback, xact_completion_force_sync_commit,
    xact_completion_relcache_init_file_inval, XACT_XINFO_HAS_DBINFO, XACT_XINFO_HAS_DROPPED_STATS,
    XACT_XINFO_HAS_GID, XACT_XINFO_HAS_INVALS, XACT_XINFO_HAS_ORIGIN,
    XACT_XINFO_HAS_RELFILELOCATORS, XACT_XINFO_HAS_SUBXACTS, XACT_XINFO_HAS_TWOPHASE,
    XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_ASSIGNMENT, XLOG_XACT_COMMIT,
    XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_HAS_INFO, XLOG_XACT_INVALIDATIONS, XLOG_XACT_OPMASK,
    XLOG_XACT_PREPARE,
};

use ::rmgrdesc_next::standby_desc_invalidations;
use timestamp_seams as timestamp_seams;
use common_relpath_seams as relpath_seams;

// ---------------------------------------------------------------------------
// appendStringInfo: format into the buffer, surfacing the allocation failure
// as the OOM PgError (not fmt::Error). Mirrors backend-rmgrdesc-next's local
// helper (which is pub(crate) there).
// ---------------------------------------------------------------------------

fn append(buf: &mut PgString<'_>, args: core::fmt::Arguments<'_>) -> PgResult<()> {
    struct Adapter<'a, 'mcx> {
        buf: &'a mut PgString<'mcx>,
        err: Option<PgError>,
    }
    impl core::fmt::Write for Adapter<'_, '_> {
        fn write_str(&mut self, s: &str) -> core::fmt::Result {
            self.buf.try_push_str(s).map_err(|e| {
                self.err = Some(e);
                core::fmt::Error
            })
        }
    }
    let mut a = Adapter { buf, err: None };
    if core::fmt::Write::write_fmt(&mut a, args).is_ok() {
        return Ok(());
    }
    let err = a.err.take();
    Err(err.unwrap_or_else(|| a.buf.allocator().oom(0)))
}

macro_rules! appendf {
    ($buf:expr, $($arg:tt)*) => {
        append($buf, core::format_args!($($arg)*))?
    };
}

const TRUNCATED: &str = "transaction WAL record shorter than the C struct it must hold";

fn truncated() -> PgError {
    PgError::error(TRUNCATED)
}

// ---------------------------------------------------------------------------
// Record-layout constants (access/xact.h): the byte sizes/offsets of the
// `xl_xact_*` sub-records and the `MinSizeOf*` macros, from the C field
// order/alignment.
// ---------------------------------------------------------------------------

/// `#define GIDSIZE 200` (`access/xact.h`).
const GIDSIZE: usize = 200;

const SIZE_OF_TRANSACTION_ID: usize = 4;
const SIZE_OF_REL_FILE_LOCATOR: usize = 12; // { Oid; Oid; RelFileNumber; }
const SIZE_OF_XACT_STATS_ITEM: usize = 16; // { int; Oid; uint32; uint32; }
const SIZE_OF_SHARED_INVALIDATION_MESSAGE: usize = 16;
const SIZE_OF_XACT_XINFO: usize = 4; // { uint32 xinfo; }
const SIZE_OF_XACT_DBINFO: usize = 8; // { Oid dbId; Oid tsId; }
const SIZE_OF_XACT_TWOPHASE: usize = 4; // { TransactionId xid; }

/// `MinSizeOfXactCommit` / `MinSizeOfXactAbort` = `offsetof(.., xact_time) +
/// sizeof(TimestampTz)` = 8 (both structs are `{ TimestampTz xact_time; }`).
const MIN_SIZE_OF_XACT_COMMIT: usize = 8;
const MIN_SIZE_OF_XACT_ABORT: usize = 8;
const MIN_SIZE_OF_XACT_SUBXACTS: usize = 4; // offsetof(.., subxacts) = sizeof(int)
const MIN_SIZE_OF_XACT_RELFILELOCATORS: usize = 4; // offsetof(.., xlocators)
const MIN_SIZE_OF_XACT_STATS_ITEMS: usize = 4; // offsetof(.., items)
const MIN_SIZE_OF_XACT_INVALS: usize = 4; // offsetof(.., msgs)

/// `sizeof(xl_xact_prepare)` (`access/xact.h`), 8-byte max alignment:
/// magic@0, total_len@4, xid@8, database@12, prepared_at@16, owner@24,
/// nsubxacts@28, ncommitrels@32, nabortrels@36, ncommitstats@40,
/// nabortstats@44, ninvalmsgs@48, initfileinval@52, gidlen@54, origin_lsn@56,
/// origin_timestamp@64 ⇒ 72.
const SIZE_OF_XL_PREPARE: usize = 72;

// ---------------------------------------------------------------------------
// Bounds-checked native-endian byte readers (the WAL record is the in-memory
// `xl_xact_*` struct image, read on the same architecture).
// ---------------------------------------------------------------------------

fn bytes_at<const N: usize>(data: &[u8], offset: usize) -> PgResult<[u8; N]> {
    let end = offset.checked_add(N).ok_or_else(truncated)?;
    let bytes = data.get(offset..end).ok_or_else(truncated)?;
    bytes.try_into().map_err(|_| truncated())
}

fn u16_at(data: &[u8], offset: usize) -> PgResult<u16> {
    Ok(u16::from_ne_bytes(bytes_at(data, offset)?))
}
fn u32_at(data: &[u8], offset: usize) -> PgResult<u32> {
    Ok(u32::from_ne_bytes(bytes_at(data, offset)?))
}
fn i32_at(data: &[u8], offset: usize) -> PgResult<i32> {
    Ok(i32::from_ne_bytes(bytes_at(data, offset)?))
}
fn u64_at(data: &[u8], offset: usize) -> PgResult<u64> {
    Ok(u64::from_ne_bytes(bytes_at(data, offset)?))
}
fn i64_at(data: &[u8], offset: usize) -> PgResult<i64> {
    Ok(i64::from_ne_bytes(bytes_at(data, offset)?))
}

/// `MAXALIGN(len)` — round up to `MAXIMUM_ALIGNOF` (8); used by
/// `ParsePrepareRecord` to step over the maxaligned sub-arrays.
fn maxalign(len: usize) -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// Validate an on-disk element count against the bytes available at
/// `elements_offset`, returning it as `usize`. A record claiming more elements
/// than its remaining bytes can hold is truncated, so a tiny record cannot
/// drive a huge amount of work. Counts here are non-negative; `<= 0` ⇒ zero
/// (matching the C `if (n > 0)` guards).
fn validate_count(
    data: &[u8],
    count: i32,
    elements_offset: usize,
    element_size: usize,
) -> PgResult<usize> {
    if count <= 0 {
        return Ok(0);
    }
    let count = count as usize;
    let needed = count.checked_mul(element_size).ok_or_else(truncated)?;
    let available = data.len().saturating_sub(elements_offset);
    if needed > available {
        return Err(truncated());
    }
    Ok(count)
}

fn count_bytes(n: i32, element_size: usize) -> PgResult<usize> {
    if n <= 0 {
        return Ok(0);
    }
    (n as usize).checked_mul(element_size).ok_or_else(truncated)
}

fn element_offset(base: usize, index: usize, element_size: usize) -> PgResult<usize> {
    base.checked_add(index.checked_mul(element_size).ok_or_else(truncated)?)
        .ok_or_else(truncated)
}

/// `strlen(data)` from `offset`, bounded by the record length.
fn nul_terminated_len(data: &[u8], offset: usize) -> PgResult<usize> {
    let tail = data.get(offset..).ok_or_else(truncated)?;
    tail.iter().position(|&b| b == 0).ok_or_else(truncated)
}

// ---------------------------------------------------------------------------
// Parsed commit/abort: scalar fields + offsets to the variable-length arrays.
// ---------------------------------------------------------------------------

/// `xl_xact_parsed_commit` / `xl_xact_parsed_abort` (`access/xact.h`), reduced
/// to the fields the describers read. The variable-length arrays are the byte
/// offset within the record at which each begins, plus the count; elements are
/// read lazily through the accessors below.
#[derive(Clone, Copy, Debug, Default)]
pub struct ParsedCommitAbort {
    pub xinfo: u32,
    pub xact_time: TimestampTz,
    pub db_id: Oid,
    pub ts_id: Oid,
    pub nsubxacts: i32,
    pub subxacts_offset: usize,
    pub nrels: i32,
    pub xlocators_offset: usize,
    pub nstats: i32,
    pub stats_offset: usize,
    pub nmsgs: i32,
    pub msgs_offset: usize,
    pub twophase_xid: TransactionId,
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
    /// Byte offset of the NUL-terminated `twophase_gid` string within the
    /// record (only valid when `XACT_XINFO_HAS_GID` is set), with its length
    /// (excluding the NUL). `0`/`0` when no GID is present.
    pub twophase_gid_offset: usize,
    pub twophase_gid_len: usize,
}

/// `ParseCommitRecord(uint8 info, xl_xact_commit *xlrec, ..)` (xactdesc.c).
/// `info` is the full `xl_info` byte; `data` is `(char *) xlrec`.
pub fn parse_commit_record(info: u8, data: &[u8]) -> PgResult<ParsedCommitAbort> {
    let mut parsed = ParsedCommitAbort {
        xinfo: 0,
        xact_time: i64_at(data, 0)?,
        ..ParsedCommitAbort::default()
    };
    let mut offset = MIN_SIZE_OF_XACT_COMMIT;
    if info & XLOG_XACT_HAS_INFO != 0 {
        parsed.xinfo = u32_at(data, offset)?;
        offset += SIZE_OF_XACT_XINFO;
    }
    parse_commit_abort_body(&mut parsed, data, offset)?;
    Ok(parsed)
}

/// `ParseAbortRecord(uint8 info, xl_xact_abort *xlrec, ..)` (xactdesc.c).
/// Identical sub-record walk to the commit parser.
pub fn parse_abort_record(info: u8, data: &[u8]) -> PgResult<ParsedCommitAbort> {
    let mut parsed = ParsedCommitAbort {
        xinfo: 0,
        xact_time: i64_at(data, 0)?,
        ..ParsedCommitAbort::default()
    };
    let mut offset = MIN_SIZE_OF_XACT_ABORT;
    if info & XLOG_XACT_HAS_INFO != 0 {
        parsed.xinfo = u32_at(data, offset)?;
        offset += SIZE_OF_XACT_XINFO;
    }
    parse_commit_abort_body(&mut parsed, data, offset)?;
    Ok(parsed)
}

/// The shared tail of `ParseCommitRecord` / `ParseAbortRecord` (the sub-record
/// walk after the optional `xl_xact_xinfo`).
fn parse_commit_abort_body(
    parsed: &mut ParsedCommitAbort,
    data: &[u8],
    mut offset: usize,
) -> PgResult<()> {
    if parsed.xinfo & XACT_XINFO_HAS_DBINFO != 0 {
        parsed.db_id = u32_at(data, offset)?;
        parsed.ts_id = u32_at(data, offset + 4)?;
        offset += SIZE_OF_XACT_DBINFO;
    }
    if parsed.xinfo & XACT_XINFO_HAS_SUBXACTS != 0 {
        parsed.nsubxacts = i32_at(data, offset)?;
        offset += MIN_SIZE_OF_XACT_SUBXACTS;
        parsed.subxacts_offset = offset;
        let n = validate_count(data, parsed.nsubxacts, offset, SIZE_OF_TRANSACTION_ID)?;
        offset += n * SIZE_OF_TRANSACTION_ID;
    }
    if parsed.xinfo & XACT_XINFO_HAS_RELFILELOCATORS != 0 {
        parsed.nrels = i32_at(data, offset)?;
        offset += MIN_SIZE_OF_XACT_RELFILELOCATORS;
        parsed.xlocators_offset = offset;
        let n = validate_count(data, parsed.nrels, offset, SIZE_OF_REL_FILE_LOCATOR)?;
        offset += n * SIZE_OF_REL_FILE_LOCATOR;
    }
    if parsed.xinfo & XACT_XINFO_HAS_DROPPED_STATS != 0 {
        parsed.nstats = i32_at(data, offset)?;
        offset += MIN_SIZE_OF_XACT_STATS_ITEMS;
        parsed.stats_offset = offset;
        let n = validate_count(data, parsed.nstats, offset, SIZE_OF_XACT_STATS_ITEM)?;
        offset += n * SIZE_OF_XACT_STATS_ITEM;
    }
    if parsed.xinfo & XACT_XINFO_HAS_INVALS != 0 {
        parsed.nmsgs = i32_at(data, offset)?;
        offset += MIN_SIZE_OF_XACT_INVALS;
        parsed.msgs_offset = offset;
        let n = validate_count(data, parsed.nmsgs, offset, SIZE_OF_SHARED_INVALIDATION_MESSAGE)?;
        offset += n * SIZE_OF_SHARED_INVALIDATION_MESSAGE;
    }
    if parsed.xinfo & XACT_XINFO_HAS_TWOPHASE != 0 {
        parsed.twophase_xid = u32_at(data, offset)?;
        offset += SIZE_OF_XACT_TWOPHASE;
        if parsed.xinfo & XACT_XINFO_HAS_GID != 0 {
            // Record the GID offset/length (decode.c forwards the real gid to
            // ReorderBufferFinishPrepared); the describers do not print it but
            // we still step over the NUL-terminated string to keep the origin
            // offset correct.
            let gid_len = nul_terminated_len(data, offset)?;
            parsed.twophase_gid_offset = offset;
            parsed.twophase_gid_len = gid_len;
            offset += gid_len + 1;
        }
    }
    // Note: no alignment is guaranteed after this point.
    if parsed.xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        parsed.origin_lsn = u64_at(data, offset)?;
        parsed.origin_timestamp = i64_at(data, offset + 8)?;
    }
    Ok(())
}

/// Collect `parsed.xlocators[0..parsed.nrels]` from a parsed commit/abort into
/// an `mcx`-owned vector. The C `ParseCommitRecord`/`ParseAbortRecord` point
/// `parsed.xlocators` into the record buffer; consumers that outlive the
/// describer (`pg_waldump`, the WAL summarizer) need the locators materialized,
/// which is why the seam returns them owned in `mcx`.
fn parsed_xlocators<'mcx>(
    mcx: Mcx<'mcx>,
    parsed: &ParsedCommitAbort,
    data: &[u8],
) -> PgResult<PgVec<'mcx, RelFileLocator>> {
    let n = validate_count(data, parsed.nrels, parsed.xlocators_offset, SIZE_OF_REL_FILE_LOCATOR)?;
    let mut out = ::mcx::vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        out.push(rellocator_at(data, parsed.xlocators_offset, i)?);
    }
    Ok(out)
}

/// `ParseCommitRecord(info, xlrec, &parsed)` (xactdesc.c) — seam form. Parses
/// the commit body and returns the relations removed on commit
/// (`parsed.xlocators[0..nrels]`), owned in `mcx`. Used by the WAL summarizer.
pub fn parse_commit_record_seam<'mcx>(
    mcx: Mcx<'mcx>,
    info: u8,
    data: &[u8],
) -> PgResult<PgVec<'mcx, RelFileLocator>> {
    let parsed = parse_commit_record(info, data)?;
    parsed_xlocators(mcx, &parsed, data)
}

/// `ParseAbortRecord(info, xlrec, &parsed)` (xactdesc.c) — seam form. Parses
/// the abort body and returns the relations removed on abort
/// (`parsed.xlocators[0..nrels]`), owned in `mcx`. Used by the WAL summarizer.
pub fn parse_abort_record_seam<'mcx>(
    mcx: Mcx<'mcx>,
    info: u8,
    data: &[u8],
) -> PgResult<PgVec<'mcx, RelFileLocator>> {
    let parsed = parse_abort_record(info, data)?;
    parsed_xlocators(mcx, &parsed, data)
}

// ---------------------------------------------------------------------------
// Parsed prepare: scalar fields + offsets to the maxaligned sub-arrays.
// ---------------------------------------------------------------------------

/// `xl_xact_parsed_prepare` (`access/xact.h`), reduced to the fields
/// `xact_desc_prepare` reads. `twophase_gid` is the NUL-trimmed GID stored
/// inline (the C `char twophase_gid[GIDSIZE]`).
#[derive(Clone, Copy, Debug)]
pub struct ParsedPrepare {
    pub xact_time: TimestampTz,
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
    pub twophase_xid: TransactionId,
    pub db_id: Oid,
    pub nsubxacts: i32,
    pub subxacts_offset: usize,
    pub nrels: i32,
    pub xlocators_offset: usize,
    pub nabortrels: i32,
    pub abortlocators_offset: usize,
    pub nstats: i32,
    pub stats_offset: usize,
    pub nabortstats: i32,
    pub abortstats_offset: usize,
    pub nmsgs: i32,
    pub msgs_offset: usize,
    pub initfileinval: bool,
    twophase_gid: [u8; GIDSIZE],
    twophase_gid_len: usize,
}

impl ParsedPrepare {
    /// `parsed->twophase_gid` — the GID, trimmed at the first NUL (as the
    /// describer's `%s` reads it). ASCII 2PC identifier; lossy if not UTF-8.
    pub fn twophase_gid(&self) -> &str {
        core::str::from_utf8(&self.twophase_gid[..self.twophase_gid_len]).unwrap_or("")
    }
}

/// `ParsePrepareRecord(uint8 info, xl_xact_prepare *xlrec, ..)` (xactdesc.c).
/// `info` is unused by the C body (the header carries its own counts); `data`
/// is `(char *) xlrec` — the `xl_xact_prepare` header followed by the
/// maxaligned sub-arrays.
pub fn parse_prepare_record(data: &[u8]) -> PgResult<ParsedPrepare> {
    let xid = u32_at(data, 8)?;
    let database = u32_at(data, 12)?;
    let prepared_at = i64_at(data, 16)?;
    let nsubxacts = i32_at(data, 28)?;
    let ncommitrels = i32_at(data, 32)?;
    let nabortrels = i32_at(data, 36)?;
    let ncommitstats = i32_at(data, 40)?;
    let nabortstats = i32_at(data, 44)?;
    let ninvalmsgs = i32_at(data, 48)?;
    let initfileinval = *data.get(52).ok_or_else(truncated)? != 0;
    let gidlen = u16_at(data, 54)? as usize;
    let origin_lsn = u64_at(data, 56)?;
    let origin_timestamp = i64_at(data, 64)?;

    // bufptr = ((char *) xlrec) + MAXALIGN(sizeof(xl_xact_prepare));
    let mut bufptr = maxalign(SIZE_OF_XL_PREPARE);

    // strncpy(parsed->twophase_gid, bufptr, xlrec->gidlen);
    let gid_end = bufptr.checked_add(gidlen).ok_or_else(truncated)?;
    let gid_src = data.get(bufptr..gid_end).ok_or_else(truncated)?;
    let mut twophase_gid = [0u8; GIDSIZE];
    let mut twophase_gid_len = 0;
    for &b in gid_src.iter().take(GIDSIZE) {
        if b == 0 {
            break;
        }
        twophase_gid[twophase_gid_len] = b;
        twophase_gid_len += 1;
    }
    bufptr += maxalign(gidlen);

    let subxacts_offset = bufptr;
    validate_count(data, nsubxacts, bufptr, SIZE_OF_TRANSACTION_ID)?;
    bufptr += maxalign(count_bytes(nsubxacts, SIZE_OF_TRANSACTION_ID)?);

    let xlocators_offset = bufptr;
    validate_count(data, ncommitrels, bufptr, SIZE_OF_REL_FILE_LOCATOR)?;
    bufptr += maxalign(count_bytes(ncommitrels, SIZE_OF_REL_FILE_LOCATOR)?);

    let abortlocators_offset = bufptr;
    validate_count(data, nabortrels, bufptr, SIZE_OF_REL_FILE_LOCATOR)?;
    bufptr += maxalign(count_bytes(nabortrels, SIZE_OF_REL_FILE_LOCATOR)?);

    let stats_offset = bufptr;
    validate_count(data, ncommitstats, bufptr, SIZE_OF_XACT_STATS_ITEM)?;
    bufptr += maxalign(count_bytes(ncommitstats, SIZE_OF_XACT_STATS_ITEM)?);

    let abortstats_offset = bufptr;
    validate_count(data, nabortstats, bufptr, SIZE_OF_XACT_STATS_ITEM)?;
    bufptr += maxalign(count_bytes(nabortstats, SIZE_OF_XACT_STATS_ITEM)?);

    let msgs_offset = bufptr;
    validate_count(data, ninvalmsgs, bufptr, SIZE_OF_SHARED_INVALIDATION_MESSAGE)?;

    Ok(ParsedPrepare {
        xact_time: prepared_at,
        origin_lsn,
        origin_timestamp,
        twophase_xid: xid,
        db_id: database,
        nsubxacts,
        subxacts_offset,
        nrels: ncommitrels,
        xlocators_offset,
        nabortrels,
        abortlocators_offset,
        nstats: ncommitstats,
        stats_offset,
        nabortstats,
        abortstats_offset,
        nmsgs: ninvalmsgs,
        msgs_offset,
        initfileinval,
        twophase_gid,
        twophase_gid_len,
    })
}

// ---------------------------------------------------------------------------
// Element accessors (lazy reads of the variable-length arrays).
// ---------------------------------------------------------------------------

/// `parsed->subxacts[index]` — the `index`-th subtransaction xid of a parsed
/// commit/abort/prepare record (decode.c iterates these).
pub fn subxact_at(data: &[u8], subxacts_offset: usize, index: usize) -> PgResult<TransactionId> {
    let off = element_offset(subxacts_offset, index, SIZE_OF_TRANSACTION_ID)?;
    u32_at(data, off)
}

/// `parsed->twophase_gid` — the NUL-stripped GID bytes of a parsed
/// commit/abort 2PC record (`COMMIT_PREPARED` / `ABORT_PREPARED`), or an empty
/// slice when no GID was logged. decode.c forwards these to `FilterPrepare` /
/// `ReorderBufferFinishPrepared`.
pub fn commit_abort_gid<'a>(data: &'a [u8], parsed: &ParsedCommitAbort) -> &'a [u8] {
    if parsed.twophase_gid_len == 0 {
        return &[];
    }
    data.get(parsed.twophase_gid_offset..parsed.twophase_gid_offset + parsed.twophase_gid_len)
        .unwrap_or(&[])
}

fn rellocator_at(data: &[u8], xlocators_offset: usize, index: usize) -> PgResult<RelFileLocator> {
    let base = element_offset(xlocators_offset, index, SIZE_OF_REL_FILE_LOCATOR)?;
    Ok(RelFileLocator {
        spcOid: u32_at(data, base)?,
        dbOid: u32_at(data, base + 4)?,
        relNumber: u32_at(data, base + 8)? as RelFileNumber,
    })
}

/// One dropped-stats element, with the 64-bit objid reassembled from its two
/// halves (`xl_xact_stats_item { int kind; Oid dboid; uint32 objid_lo;
/// uint32 objid_hi; }`).
struct StatsItem {
    kind: i32,
    dboid: Oid,
    objid: u64,
}

fn stats_item_at(data: &[u8], stats_offset: usize, index: usize) -> PgResult<StatsItem> {
    let base = element_offset(stats_offset, index, SIZE_OF_XACT_STATS_ITEM)?;
    let kind = i32_at(data, base)?;
    let dboid = u32_at(data, base + 4)?;
    let objid_lo = u32_at(data, base + 8)?;
    let objid_hi = u32_at(data, base + 12)?;
    Ok(StatsItem {
        kind,
        dboid,
        objid: ((objid_hi as u64) << 32) | (objid_lo as u64),
    })
}

// ---------------------------------------------------------------------------
// Helpers shared with the C describer.
// ---------------------------------------------------------------------------

/// `TransactionIdIsValid(xid)` (`access/transam.h`).
fn transaction_id_is_valid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `LSN_FORMAT_ARGS(lsn)` — `(uint32)((lsn) >> 32), (uint32) (lsn)`.
fn lsn_format(lsn: XLogRecPtr) -> (u32, u32) {
    ((lsn >> 32) as u32, lsn as u32)
}

/// `relpathperm(rlocator, MAIN_FORKNUM).str` (`common/relpath.h` macro over
/// `relpathbackend(.., INVALID_PROC_NUMBER, ..)`), reached through the owner's
/// per-owner seam.
fn relpathperm(rlocator: RelFileLocator, fork: ForkNumber) -> String {
    relpath_seams::relpathbackend::call(rlocator, INVALID_PROC_NUMBER, fork)
}

/// `timestamptz_to_str(dt)` (`utils/adt/timestamp.c`), through the owner's seam.
fn timestamptz_to_str(buf: &mut PgString<'_>, dt: TimestampTz) -> PgResult<()> {
    let s = timestamp_seams::timestamptz_to_str::call(buf.allocator(), dt)?;
    buf.try_push_str(s.as_str())
}

// ---------------------------------------------------------------------------
// Describers (static fns of xactdesc.c).
// ---------------------------------------------------------------------------

/// `xact_desc_relations(StringInfo buf, char *label, int nrels,
/// RelFileLocator *xlocators)` (xactdesc.c).
fn xact_desc_relations(
    buf: &mut PgString<'_>,
    label: &str,
    nrels: i32,
    xlocators_offset: usize,
    data: &[u8],
) -> PgResult<()> {
    if nrels > 0 {
        appendf!(buf, "; {}:", label);
        let n = validate_count(data, nrels, xlocators_offset, SIZE_OF_REL_FILE_LOCATOR)?;
        for i in 0..n {
            let rlocator = rellocator_at(data, xlocators_offset, i)?;
            appendf!(buf, " {}", relpathperm(rlocator, ForkNumber::MAIN_FORKNUM));
        }
    }
    Ok(())
}

/// `xact_desc_subxacts(StringInfo buf, int nsubxacts, TransactionId *subxacts)`.
fn xact_desc_subxacts(
    buf: &mut PgString<'_>,
    nsubxacts: i32,
    subxacts_offset: usize,
    data: &[u8],
) -> PgResult<()> {
    if nsubxacts > 0 {
        buf.try_push_str("; subxacts:")?;
        let n = validate_count(data, nsubxacts, subxacts_offset, SIZE_OF_TRANSACTION_ID)?;
        for i in 0..n {
            appendf!(buf, " {}", subxact_at(data, subxacts_offset, i)?);
        }
    }
    Ok(())
}

/// `xact_desc_stats(StringInfo buf, const char *label, int ndropped,
/// xl_xact_stats_item *dropped_stats)`.
fn xact_desc_stats(
    buf: &mut PgString<'_>,
    label: &str,
    ndropped: i32,
    stats_offset: usize,
    data: &[u8],
) -> PgResult<()> {
    if ndropped > 0 {
        appendf!(buf, "; {}dropped stats:", label);
        let n = validate_count(data, ndropped, stats_offset, SIZE_OF_XACT_STATS_ITEM)?;
        for i in 0..n {
            let item = stats_item_at(data, stats_offset, i)?;
            appendf!(buf, " {}/{}/{}", item.kind, item.dboid, item.objid);
        }
    }
    Ok(())
}

/// `standby_desc_invalidations(buf, nmsgs, msgs, dbId, tsId, ..)` over the
/// record's inval-message region, delegating to the ported sibling describer.
fn desc_invalidations(
    buf: &mut PgString<'_>,
    nmsgs: i32,
    data: &[u8],
    msgs_offset: usize,
    db_id: Oid,
    ts_id: Oid,
    relcache_init_file_inval: bool,
) -> PgResult<()> {
    // standby_desc_invalidations does nothing for nmsgs <= 0; the msg region
    // need not exist in that case (C passes the bare pointer).
    let msgs = if nmsgs > 0 {
        let n = validate_count(data, nmsgs, msgs_offset, SIZE_OF_SHARED_INVALIDATION_MESSAGE)?;
        let end = msgs_offset + n * SIZE_OF_SHARED_INVALIDATION_MESSAGE;
        SharedInvalMessages::from_bytes(data.get(msgs_offset..end).ok_or_else(truncated)?)
    } else {
        SharedInvalMessages::from_bytes(&[])
    };
    standby_desc_invalidations(buf, nmsgs, msgs, db_id, ts_id, relcache_init_file_inval)
}

/// `xact_desc_commit(StringInfo buf, uint8 info, xl_xact_commit *xlrec,
/// RepOriginId origin_id)` (xactdesc.c).
fn xact_desc_commit(
    buf: &mut PgString<'_>,
    info: u8,
    data: &[u8],
    origin_id: RepOriginId,
) -> PgResult<()> {
    let parsed = parse_commit_record(info, data)?;

    if transaction_id_is_valid(parsed.twophase_xid) {
        appendf!(buf, "{}: ", parsed.twophase_xid);
    }

    timestamptz_to_str(buf, parsed.xact_time)?;

    xact_desc_relations(buf, "rels", parsed.nrels, parsed.xlocators_offset, data)?;
    xact_desc_subxacts(buf, parsed.nsubxacts, parsed.subxacts_offset, data)?;
    xact_desc_stats(buf, "", parsed.nstats, parsed.stats_offset, data)?;

    desc_invalidations(
        buf,
        parsed.nmsgs,
        data,
        parsed.msgs_offset,
        parsed.db_id,
        parsed.ts_id,
        xact_completion_relcache_init_file_inval(parsed.xinfo),
    )?;

    if xact_completion_apply_feedback(parsed.xinfo) {
        buf.try_push_str("; apply_feedback")?;
    }
    if xact_completion_force_sync_commit(parsed.xinfo) {
        buf.try_push_str("; sync")?;
    }

    if parsed.xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        let (hi, lo) = lsn_format(parsed.origin_lsn);
        append(buf, core::format_args!("; origin: node {}, lsn {:X}/{:X}, at ", origin_id, hi, lo))?;
        timestamptz_to_str(buf, parsed.origin_timestamp)?;
    }
    Ok(())
}

/// `xact_desc_abort(StringInfo buf, uint8 info, xl_xact_abort *xlrec,
/// RepOriginId origin_id)` (xactdesc.c).
fn xact_desc_abort(
    buf: &mut PgString<'_>,
    info: u8,
    data: &[u8],
    origin_id: RepOriginId,
) -> PgResult<()> {
    let parsed = parse_abort_record(info, data)?;

    if transaction_id_is_valid(parsed.twophase_xid) {
        appendf!(buf, "{}: ", parsed.twophase_xid);
    }

    timestamptz_to_str(buf, parsed.xact_time)?;

    xact_desc_relations(buf, "rels", parsed.nrels, parsed.xlocators_offset, data)?;
    xact_desc_subxacts(buf, parsed.nsubxacts, parsed.subxacts_offset, data)?;

    if parsed.xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        let (hi, lo) = lsn_format(parsed.origin_lsn);
        append(buf, core::format_args!("; origin: node {}, lsn {:X}/{:X}, at ", origin_id, hi, lo))?;
        timestamptz_to_str(buf, parsed.origin_timestamp)?;
    }

    xact_desc_stats(buf, "", parsed.nstats, parsed.stats_offset, data)?;
    Ok(())
}

/// `xact_desc_prepare(StringInfo buf, uint8 info, xl_xact_prepare *xlrec,
/// RepOriginId origin_id)` (xactdesc.c).
fn xact_desc_prepare(buf: &mut PgString<'_>, data: &[u8], origin_id: RepOriginId) -> PgResult<()> {
    let parsed = parse_prepare_record(data)?;

    appendf!(buf, "gid {}: ", parsed.twophase_gid());
    timestamptz_to_str(buf, parsed.xact_time)?;

    xact_desc_relations(buf, "rels(commit)", parsed.nrels, parsed.xlocators_offset, data)?;
    xact_desc_relations(buf, "rels(abort)", parsed.nabortrels, parsed.abortlocators_offset, data)?;
    xact_desc_stats(buf, "commit ", parsed.nstats, parsed.stats_offset, data)?;
    xact_desc_stats(buf, "abort ", parsed.nabortstats, parsed.abortstats_offset, data)?;
    xact_desc_subxacts(buf, parsed.nsubxacts, parsed.subxacts_offset, data)?;

    // tsId is not parsed for prepare records (C leaves it zero); the prefix
    // standby_desc_invalidations prints uses dbId/tsId together.
    desc_invalidations(
        buf,
        parsed.nmsgs,
        data,
        parsed.msgs_offset,
        parsed.db_id,
        InvalidOid,
        parsed.initfileinval,
    )?;

    // Check if the replication origin has been set, as PrepareRedoAdd() does.
    if origin_id != InvalidRepOriginId {
        let (hi, lo) = lsn_format(parsed.origin_lsn);
        append(buf, core::format_args!("; origin: node {}, lsn {:X}/{:X}, at ", origin_id, hi, lo))?;
        timestamptz_to_str(buf, parsed.origin_timestamp)?;
    }
    Ok(())
}

/// `xact_desc_assignment(StringInfo buf, xl_xact_assignment *xlrec)`.
/// `xl_xact_assignment { TransactionId xtop; int nsubxacts;
/// TransactionId xsub[]; }` — `nsubxacts`@4, `xsub`@8.
fn xact_desc_assignment(buf: &mut PgString<'_>, data: &[u8]) -> PgResult<()> {
    buf.try_push_str("subxacts:")?;
    const NSUBXACTS_OFFSET: usize = 4;
    const XSUB_OFFSET: usize = 8;
    let nsubxacts = i32_at(data, NSUBXACTS_OFFSET)?;
    let n = validate_count(data, nsubxacts, XSUB_OFFSET, SIZE_OF_TRANSACTION_ID)?;
    for i in 0..n {
        appendf!(buf, " {}", subxact_at(data, XSUB_OFFSET, i)?);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// rmgr callbacks (the seam slots).
// ---------------------------------------------------------------------------

/// `xact_desc(StringInfo buf, XLogReaderState *record)` (xactdesc.c). `record`
/// is the reader positioned on the record; the describer reads `XLogRecGetData`
/// / `XLogRecGetInfo` / `XLogRecGetOrigin` through it.
pub fn xact_desc(buf: &mut PgString<'_>, record: &XLogReaderState<'_>) -> PgResult<()> {
    let decoded = record
        .record
        .as_ref()
        .expect("xact_desc called with no decoded record (XLogReadRecord NULL)");
    let data = decoded.data();
    let full_info = decoded.info();
    let origin_id = decoded.record_origin();
    let info = full_info & XLOG_XACT_OPMASK;

    if info == XLOG_XACT_COMMIT || info == XLOG_XACT_COMMIT_PREPARED {
        xact_desc_commit(buf, full_info, data, origin_id)?;
    } else if info == XLOG_XACT_ABORT || info == XLOG_XACT_ABORT_PREPARED {
        xact_desc_abort(buf, full_info, data, origin_id)?;
    } else if info == XLOG_XACT_PREPARE {
        xact_desc_prepare(buf, data, origin_id)?;
    } else if info == XLOG_XACT_ASSIGNMENT {
        // We ignore the WAL record's xid; we report xtop and the assigned xids.
        let xtop = u32_at(data, 0)?;
        appendf!(buf, "xtop {}: ", xtop);
        xact_desc_assignment(buf, data)?;
    } else if info == XLOG_XACT_INVALIDATIONS {
        // xl_xact_invals { int nmsgs; SharedInvalidationMessage msgs[]; }
        let nmsgs = i32_at(data, 0)?;
        const MSGS_OFFSET: usize = 4;
        desc_invalidations(buf, nmsgs, data, MSGS_OFFSET, InvalidOid, InvalidOid, false)?;
    }
    Ok(())
}

/// `const char *xact_identify(uint8 info)` (xactdesc.c).
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

/// Install this crate's seams (the `RM_XACT_ID` `rm_desc` / `rm_identify`
/// slots).
pub fn init_seams() {
    xactdesc_seams::xact_desc::set(xact_desc);
    xactdesc_seams::xact_identify::set(xact_identify);
    xactdesc_seams::parse_commit_record::set(parse_commit_record_seam);
    xactdesc_seams::parse_abort_record::set(parse_abort_record_seam);
}

#[cfg(test)]
mod tests;
