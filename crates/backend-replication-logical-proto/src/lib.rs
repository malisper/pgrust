//! `src/backend/replication/logical/proto.c` — the logical replication wire
//! protocol (PostgreSQL 18.3), plus the `replication/logicalproto.h`
//! vocabulary it owns ([`LogicalRepMsgType`], the `LogicalRep*` read-side
//! structures, the `LOGICALREP_COLUMN_*` status bytes).
//!
//! The write side appends into a [`StringInfo`] with the `pqformat` routines
//! (proto.c does not frame its messages — the output plugin frames the
//! assembled buffer, so the first `pq_sendbyte` is the first body byte). The
//! read side consumes a [`StringInfo`] cursor and fills the owned
//! `LogicalRep*` structures, allocating through the caller's `Mcx` where the
//! C pallocs.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use backend_executor_execTuples_seams as exectuples_seams;
use backend_libpq_pqformat::{
    pq_getmsgbyte, pq_getmsgbytes, pq_getmsgint, pq_getmsgint64, pq_getmsgstring, pq_sendbyte,
    pq_sendbytes, pq_sendcountedtext, pq_sendint, pq_sendint16, pq_sendint32, pq_sendint64,
    pq_sendint8, pq_sendstring, PqString,
};
use backend_nodes_core_seams as bms_seams;
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use backend_utils_cache_relcache_seams as relcache_seams;
use backend_utils_cache_syscache::{
    ReleaseSysCache, SearchSysCache1, SysCacheGetAttrNotNull, TYPEOID,
};
use backend_utils_fmgr_fmgr_seams as fmgr_seams;
use mcx::{slice_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_cache::SysCacheKey;
use types_catalog::catalog::PG_CATALOG_NAMESPACE;
pub use types_catalog::pg_publication::PublishGencolsType;
use types_core::{
    GIDSIZE, InvalidTransactionId, InvalidXLogRecPtr, Oid, OidIsValid, TimestampTz, TransactionId,
    TransactionIdIsValid, XLogRecPtr,
};
// Bare-word machine-word `Datum` (`types_datum::Datum`), aliased `ScalarWord`
// to disambiguate from the canonical per-attribute value enum below. Used only
// at the `SearchSysCache*` key edge, where the cache-key currency
// (`SysCacheKey::Value`) is an audited bare word (C: `Datum key1..key4`).
use types_datum::Datum as ScalarWord;
use types_error::{PgError, PgResult};
use types_nodes::{Bitmapset, TupleTableSlot};
use types_rel::RelationData;
use types_stringinfo::StringInfo;
// The canonical per-attribute value model (C's per-column `Datum`): a by-value
// scalar word (`ByVal`) or the verbatim by-reference bytes (`ByRef`).
use types_tuple::backend_access_common_heaptuple::{Datum, FormedTuple};
use types_tuple::heaptuple::{FirstLowInvalidHeapAttributeNumber, FormData_pg_attribute};
use types_tuple::{ATTRIBUTE_GENERATED_STORED, REPLICA_IDENTITY_DEFAULT, REPLICA_IDENTITY_FULL,
    REPLICA_IDENTITY_INDEX};
use types_wal::reorderbuffer::ReorderBufferTXN;

#[cfg(test)]
mod tests;

/// No seams of this crate's own to install (no cyclic callers yet); kept so
/// `seams-init` registers the crate uniformly.
pub fn init_seams() {}

/*
 * Protocol message flags.
 */
const LOGICALREP_IS_REPLICA_IDENTITY: u8 = 1;

const MESSAGE_TRANSACTIONAL: u8 = 1 << 0;
const TRUNCATE_CASCADE: u8 = 1 << 0;
const TRUNCATE_RESTART_SEQS: u8 = 1 << 1;

// ---------------------------------------------------------------------------
// replication/logicalproto.h — owned by this unit.
// ---------------------------------------------------------------------------

/// `LogicalRepMsgType` (`logicalproto.h`): the single-byte wire message-type
/// tags. The discriminants are on-the-wire bytes and must not change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum LogicalRepMsgType {
    Begin = b'B' as i32,
    Commit = b'C' as i32,
    Origin = b'O' as i32,
    Insert = b'I' as i32,
    Update = b'U' as i32,
    Delete = b'D' as i32,
    Truncate = b'T' as i32,
    Relation = b'R' as i32,
    Type = b'Y' as i32,
    Message = b'M' as i32,
    BeginPrepare = b'b' as i32,
    Prepare = b'P' as i32,
    CommitPrepared = b'K' as i32,
    RollbackPrepared = b'r' as i32,
    StreamStart = b'S' as i32,
    StreamStop = b'E' as i32,
    StreamCommit = b'c' as i32,
    StreamAbort = b'A' as i32,
    StreamPrepare = b'p' as i32,
}

impl LogicalRepMsgType {
    /// The wire byte for this message type.
    pub const fn as_byte(self) -> u8 {
        self as i32 as u8
    }
}

const LOGICAL_REP_MSG_BEGIN: u8 = LogicalRepMsgType::Begin.as_byte();
const LOGICAL_REP_MSG_COMMIT: u8 = LogicalRepMsgType::Commit.as_byte();
const LOGICAL_REP_MSG_ORIGIN: u8 = LogicalRepMsgType::Origin.as_byte();
const LOGICAL_REP_MSG_INSERT: u8 = LogicalRepMsgType::Insert.as_byte();
const LOGICAL_REP_MSG_UPDATE: u8 = LogicalRepMsgType::Update.as_byte();
const LOGICAL_REP_MSG_DELETE: u8 = LogicalRepMsgType::Delete.as_byte();
const LOGICAL_REP_MSG_TRUNCATE: u8 = LogicalRepMsgType::Truncate.as_byte();
const LOGICAL_REP_MSG_RELATION: u8 = LogicalRepMsgType::Relation.as_byte();
const LOGICAL_REP_MSG_TYPE: u8 = LogicalRepMsgType::Type.as_byte();
const LOGICAL_REP_MSG_MESSAGE: u8 = LogicalRepMsgType::Message.as_byte();
const LOGICAL_REP_MSG_BEGIN_PREPARE: u8 = LogicalRepMsgType::BeginPrepare.as_byte();
const LOGICAL_REP_MSG_PREPARE: u8 = LogicalRepMsgType::Prepare.as_byte();
const LOGICAL_REP_MSG_COMMIT_PREPARED: u8 = LogicalRepMsgType::CommitPrepared.as_byte();
const LOGICAL_REP_MSG_ROLLBACK_PREPARED: u8 = LogicalRepMsgType::RollbackPrepared.as_byte();
const LOGICAL_REP_MSG_STREAM_START: u8 = LogicalRepMsgType::StreamStart.as_byte();
const LOGICAL_REP_MSG_STREAM_STOP: u8 = LogicalRepMsgType::StreamStop.as_byte();
const LOGICAL_REP_MSG_STREAM_COMMIT: u8 = LogicalRepMsgType::StreamCommit.as_byte();
const LOGICAL_REP_MSG_STREAM_ABORT: u8 = LogicalRepMsgType::StreamAbort.as_byte();
const LOGICAL_REP_MSG_STREAM_PREPARE: u8 = LogicalRepMsgType::StreamPrepare.as_byte();

/// `#define LOGICALREP_COLUMN_NULL 'n'`
pub const LOGICALREP_COLUMN_NULL: u8 = b'n';
/// `#define LOGICALREP_COLUMN_UNCHANGED 'u'`
pub const LOGICALREP_COLUMN_UNCHANGED: u8 = b'u';
/// `#define LOGICALREP_COLUMN_TEXT 't'`
pub const LOGICALREP_COLUMN_TEXT: u8 = b't';
/// `#define LOGICALREP_COLUMN_BINARY 'b'`
pub const LOGICALREP_COLUMN_BINARY: u8 = b'b';

/// `LogicalRepRelId` (`logicalproto.h`).
pub type LogicalRepRelId = u32;

/// `LogicalRepTupleData` (`logicalproto.h`): a tuple received via logical
/// replication; columns correspond to the remote table.
pub struct LogicalRepTupleData<'mcx> {
    /// `StringInfoData *colvalues` — per-column values; an entry is
    /// meaningful only when the matching `colstatus` is text/binary (the C
    /// palloc0 zeroes the rest; here those entries are empty buffers).
    pub colvalues: PgVec<'mcx, StringInfo<'mcx>>,
    /// `char *colstatus` — per-column `LOGICALREP_COLUMN_*` markers.
    pub colstatus: PgVec<'mcx, u8>,
    /// `int ncols` — length of the above arrays.
    pub ncols: i32,
}

impl<'mcx> LogicalRepTupleData<'mcx> {
    /// An empty tuple for the readers to fill.
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        LogicalRepTupleData {
            colvalues: PgVec::new_in(mcx),
            colstatus: PgVec::new_in(mcx),
            ncols: 0,
        }
    }
}

/// `LogicalRepRelation` (`logicalproto.h`): relation metadata received via
/// logical replication. String fields are the C `char *` bytes (NUL
/// excluded).
pub struct LogicalRepRelation<'mcx> {
    /// `LogicalRepRelId remoteid` — unique id of the relation.
    pub remoteid: LogicalRepRelId,
    /// `char *nspname` — schema name.
    pub nspname: PgVec<'mcx, u8>,
    /// `char *relname` — relation name.
    pub relname: PgVec<'mcx, u8>,
    /// `int natts` — number of columns.
    pub natts: i32,
    /// `char **attnames` — column names.
    pub attnames: PgVec<'mcx, PgVec<'mcx, u8>>,
    /// `Oid *atttyps` — column types.
    pub atttyps: PgVec<'mcx, Oid>,
    /// `char replident` — replica identity.
    pub replident: u8,
    /// `char relkind` — remote relation kind (not set by the protocol
    /// reader; filled by the apply worker).
    pub relkind: u8,
    /// `Bitmapset *attkeys` — the set of key-column indexes (`0..natts`).
    pub attkeys: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
}

/// `LogicalRepTyp` (`logicalproto.h`).
pub struct LogicalRepTyp<'mcx> {
    /// `Oid remoteid` — unique id of the remote type.
    pub remoteid: Oid,
    /// `char *nspname` — schema name of the remote type.
    pub nspname: PgVec<'mcx, u8>,
    /// `char *typname` — name of the remote type.
    pub typname: PgVec<'mcx, u8>,
}

/// `LogicalRepBeginData` (`logicalproto.h`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LogicalRepBeginData {
    pub final_lsn: XLogRecPtr,
    pub committime: TimestampTz,
    pub xid: TransactionId,
}

/// `LogicalRepCommitData` (`logicalproto.h`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LogicalRepCommitData {
    pub commit_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub committime: TimestampTz,
}

/// `LogicalRepPreparedTxnData` (`logicalproto.h`): prepare/begin-prepare
/// info. `gid` is the C `char gid[GIDSIZE]` fixed buffer (NUL-terminated).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalRepPreparedTxnData {
    pub prepare_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub prepare_time: TimestampTz,
    pub xid: TransactionId,
    pub gid: [u8; GIDSIZE],
}

impl Default for LogicalRepPreparedTxnData {
    fn default() -> Self {
        LogicalRepPreparedTxnData {
            prepare_lsn: 0,
            end_lsn: 0,
            prepare_time: 0,
            xid: 0,
            gid: [0; GIDSIZE],
        }
    }
}

/// `LogicalRepCommitPreparedTxnData` (`logicalproto.h`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalRepCommitPreparedTxnData {
    pub commit_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub commit_time: TimestampTz,
    pub xid: TransactionId,
    pub gid: [u8; GIDSIZE],
}

impl Default for LogicalRepCommitPreparedTxnData {
    fn default() -> Self {
        LogicalRepCommitPreparedTxnData {
            commit_lsn: 0,
            end_lsn: 0,
            commit_time: 0,
            xid: 0,
            gid: [0; GIDSIZE],
        }
    }
}

/// `LogicalRepRollbackPreparedTxnData` (`logicalproto.h`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalRepRollbackPreparedTxnData {
    pub prepare_end_lsn: XLogRecPtr,
    pub rollback_end_lsn: XLogRecPtr,
    pub prepare_time: TimestampTz,
    pub rollback_time: TimestampTz,
    pub xid: TransactionId,
    pub gid: [u8; GIDSIZE],
}

impl Default for LogicalRepRollbackPreparedTxnData {
    fn default() -> Self {
        LogicalRepRollbackPreparedTxnData {
            prepare_end_lsn: 0,
            rollback_end_lsn: 0,
            prepare_time: 0,
            rollback_time: 0,
            xid: 0,
            gid: [0; GIDSIZE],
        }
    }
}

/// `LogicalRepStreamAbortData` (`logicalproto.h`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LogicalRepStreamAbortData {
    pub xid: TransactionId,
    pub subxid: TransactionId,
    pub abort_lsn: XLogRecPtr,
    pub abort_time: TimestampTz,
}

// ---------------------------------------------------------------------------
// Small helpers.
// ---------------------------------------------------------------------------

/// `elog(ERROR, ...)`: an internal (errmsg_internal-style) error, exactly as
/// `proto.c` uses `elog`.
fn elog_error(message: String) -> PgError {
    PgError::error(message)
}

/// `strlcpy(dst, src, GIDSIZE)`: copy at most `GIDSIZE - 1` bytes and
/// NUL-terminate (the unwritten tail is also zeroed here, which `strlcpy`
/// leaves untouched — invisible past the terminator).
fn strlcpy_gid(dst: &mut [u8; GIDSIZE], src: &[u8]) {
    let copy = src.len().min(GIDSIZE - 1);
    dst[..copy].copy_from_slice(&src[..copy]);
    for byte in dst[copy..].iter_mut() {
        *byte = 0;
    }
}

/// `pstrdup(pq_getmsgstring(in))`: read a NUL-terminated wire string and copy
/// it into `mcx`. (A converted read is already a fresh `mcx` allocation; the
/// borrowed case is copied, which is the `pstrdup`.)
fn pstrdup_msgstring<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &mut StringInfo<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    match pq_getmsgstring(mcx, in_)? {
        PqString::Borrowed(b) => slice_in(mcx, b),
        PqString::Converted(v) => Ok(v),
    }
}

/// `txn->gid` unconditional dereference sites (`logicalrep_write_begin_prepare`
/// and friends): the C reads through the pointer without a check, so an unset
/// gid is a caller bug, not an error path.
fn txn_gid<'a>(txn: &'a ReorderBufferTXN<'_>) -> &'a [u8] {
    txn.gid
        .as_deref()
        .expect("txn->gid must be set for two-phase messages (C dereferences unconditionally)")
}

// `catalog/pg_type.h` attribute numbers (`pg_type_d.h`).
const Anum_pg_type_typname: i32 = 2;
const Anum_pg_type_typnamespace: i32 = 3;
const Anum_pg_type_typoutput: i32 = 17;
const Anum_pg_type_typsend: i32 = 19;

/// `GETSTRUCT(tup)->...` for a by-value pg_type field (Oid columns).
fn pg_type_attr_oid(mcx: Mcx<'_>, tup: &FormedTuple<'_>, attnum: i32) -> PgResult<Oid> {
    match SysCacheGetAttrNotNull(mcx, TYPEOID, tup, attnum)? {
        Datum::ByVal(d) => Ok(d.as_oid()),
        Datum::ByRef(_) => Err(elog_error(
            "proto: expected a by-value pg_type attribute".into(),
        )),
    }
}

/// `NameStr(GETSTRUCT(tup)->typname)`: the NUL-trimmed `name` bytes.
fn pg_type_attr_name<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &FormedTuple<'_>,
    attnum: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    match SysCacheGetAttrNotNull(mcx, TYPEOID, tup, attnum)? {
        Datum::ByRef(b) => {
            let len = b.iter().position(|&c| c == 0).unwrap_or(b.len());
            slice_in(mcx, &b[..len])
        }
        Datum::ByVal(_) => Err(elog_error(
            "proto: pg_type name attribute is by-value".into(),
        )),
    }
}

/// `VARTAG_ONDISK` (`varatt.h`).
const VARTAG_ONDISK: u8 = 18;

/// `VARATT_IS_EXTERNAL_ONDISK(values[i])` over the per-attribute value model:
/// `VARATT_IS_1B_E` (header byte `0x01`) with `va_tag == VARTAG_ONDISK`. Only
/// a by-reference value can be a toast pointer; the C call site has already
/// checked `att->attlen == -1`.
fn varatt_is_external_ondisk(value: &Datum<'_>) -> bool {
    match value {
        Datum::ByRef(b) => b.len() >= 2 && b[0] == 0x01 && b[1] == VARTAG_ONDISK,
        Datum::ByVal(_) => false,
    }
}

// ===========================================================================
// proto.c, in source order.
// ===========================================================================

/// `logicalrep_write_begin`: write BEGIN to the output stream.
pub fn logicalrep_write_begin(
    out: &mut StringInfo<'_>,
    txn: &ReorderBufferTXN<'_>,
) -> PgResult<()> {
    pq_sendbyte(out, LOGICAL_REP_MSG_BEGIN)?;

    /* fixed fields */
    pq_sendint64(out, txn.final_lsn)?;
    pq_sendint64(out, txn.xact_time as u64)?; /* txn->xact_time.commit_time */
    pq_sendint32(out, txn.xid)?;
    Ok(())
}

/// `logicalrep_read_begin`: read transaction BEGIN from the stream.
pub fn logicalrep_read_begin(
    in_: &mut StringInfo<'_>,
    begin_data: &mut LogicalRepBeginData,
) -> PgResult<()> {
    /* read fields */
    begin_data.final_lsn = pq_getmsgint64(in_)? as XLogRecPtr;
    if begin_data.final_lsn == InvalidXLogRecPtr {
        return Err(elog_error("final_lsn not set in begin message".into()));
    }
    begin_data.committime = pq_getmsgint64(in_)?;
    begin_data.xid = pq_getmsgint(in_, 4)?;
    Ok(())
}

/// `logicalrep_write_commit`: write COMMIT to the output stream.
pub fn logicalrep_write_commit(
    out: &mut StringInfo<'_>,
    txn: &ReorderBufferTXN<'_>,
    commit_lsn: XLogRecPtr,
) -> PgResult<()> {
    let flags: u8 = 0;

    pq_sendbyte(out, LOGICAL_REP_MSG_COMMIT)?;

    /* send the flags field (unused for now) */
    pq_sendbyte(out, flags)?;

    /* send fields */
    pq_sendint64(out, commit_lsn)?;
    pq_sendint64(out, txn.end_lsn)?;
    pq_sendint64(out, txn.xact_time as u64)?; /* txn->xact_time.commit_time */
    Ok(())
}

/// `logicalrep_read_commit`: read transaction COMMIT from the stream.
pub fn logicalrep_read_commit(
    in_: &mut StringInfo<'_>,
    commit_data: &mut LogicalRepCommitData,
) -> PgResult<()> {
    /* read flags (unused for now) */
    let flags = pq_getmsgbyte(in_)? as u8;

    if flags != 0 {
        return Err(elog_error(format!(
            "unrecognized flags {flags} in commit message"
        )));
    }

    /* read fields */
    commit_data.commit_lsn = pq_getmsgint64(in_)? as XLogRecPtr;
    commit_data.end_lsn = pq_getmsgint64(in_)? as XLogRecPtr;
    commit_data.committime = pq_getmsgint64(in_)?;
    Ok(())
}

/// `logicalrep_write_begin_prepare`: write BEGIN PREPARE to the output
/// stream.
pub fn logicalrep_write_begin_prepare(
    out: &mut StringInfo<'_>,
    txn: &ReorderBufferTXN<'_>,
) -> PgResult<()> {
    pq_sendbyte(out, LOGICAL_REP_MSG_BEGIN_PREPARE)?;

    /* fixed fields */
    pq_sendint64(out, txn.final_lsn)?;
    pq_sendint64(out, txn.end_lsn)?;
    pq_sendint64(out, txn.xact_time as u64)?; /* txn->xact_time.prepare_time */
    pq_sendint32(out, txn.xid)?;

    /* send gid */
    pq_sendstring(out, txn_gid(txn))?;
    Ok(())
}

/// `logicalrep_read_begin_prepare`: read transaction BEGIN PREPARE from the
/// stream.
pub fn logicalrep_read_begin_prepare(
    mcx: Mcx<'_>,
    in_: &mut StringInfo<'_>,
    begin_data: &mut LogicalRepPreparedTxnData,
) -> PgResult<()> {
    /* read fields */
    begin_data.prepare_lsn = pq_getmsgint64(in_)? as XLogRecPtr;
    if begin_data.prepare_lsn == InvalidXLogRecPtr {
        return Err(elog_error(
            "prepare_lsn not set in begin prepare message".into(),
        ));
    }
    begin_data.end_lsn = pq_getmsgint64(in_)? as XLogRecPtr;
    if begin_data.end_lsn == InvalidXLogRecPtr {
        return Err(elog_error("end_lsn not set in begin prepare message".into()));
    }
    begin_data.prepare_time = pq_getmsgint64(in_)?;
    begin_data.xid = pq_getmsgint(in_, 4)?;

    /* read gid (copy it into a pre-allocated buffer) */
    let gid = pq_getmsgstring(mcx, in_)?;
    strlcpy_gid(&mut begin_data.gid, gid.as_bytes());
    Ok(())
}

/// `logicalrep_write_prepare_common`: the core functionality for
/// `logicalrep_write_prepare` and `logicalrep_write_stream_prepare`.
fn logicalrep_write_prepare_common(
    out: &mut StringInfo<'_>,
    type_: LogicalRepMsgType,
    txn: &ReorderBufferTXN<'_>,
    prepare_lsn: XLogRecPtr,
) -> PgResult<()> {
    let flags: u8 = 0;

    pq_sendbyte(out, type_.as_byte())?;

    /*
     * This should only ever happen for two-phase commit transactions, in
     * which case we expect to have a valid GID.
     */
    debug_assert!(txn.gid.is_some());
    debug_assert!(txn.is_prepared());
    debug_assert!(TransactionIdIsValid(txn.xid));

    /* send the flags field */
    pq_sendbyte(out, flags)?;

    /* send fields */
    pq_sendint64(out, prepare_lsn)?;
    pq_sendint64(out, txn.end_lsn)?;
    pq_sendint64(out, txn.xact_time as u64)?; /* txn->xact_time.prepare_time */
    pq_sendint32(out, txn.xid)?;

    /* send gid */
    pq_sendstring(out, txn_gid(txn))?;
    Ok(())
}

/// `logicalrep_write_prepare`: write PREPARE to the output stream.
pub fn logicalrep_write_prepare(
    out: &mut StringInfo<'_>,
    txn: &ReorderBufferTXN<'_>,
    prepare_lsn: XLogRecPtr,
) -> PgResult<()> {
    logicalrep_write_prepare_common(out, LogicalRepMsgType::Prepare, txn, prepare_lsn)
}

/// `logicalrep_read_prepare_common`: the core functionality for
/// `logicalrep_read_prepare` and `logicalrep_read_stream_prepare`.
fn logicalrep_read_prepare_common(
    mcx: Mcx<'_>,
    in_: &mut StringInfo<'_>,
    msgtype: &str,
    prepare_data: &mut LogicalRepPreparedTxnData,
) -> PgResult<()> {
    /* read flags */
    let flags = pq_getmsgbyte(in_)? as u8;

    if flags != 0 {
        return Err(elog_error(format!(
            "unrecognized flags {flags} in {msgtype} message"
        )));
    }

    /* read fields */
    prepare_data.prepare_lsn = pq_getmsgint64(in_)? as XLogRecPtr;
    if prepare_data.prepare_lsn == InvalidXLogRecPtr {
        return Err(elog_error(format!(
            "prepare_lsn is not set in {msgtype} message"
        )));
    }
    prepare_data.end_lsn = pq_getmsgint64(in_)? as XLogRecPtr;
    if prepare_data.end_lsn == InvalidXLogRecPtr {
        return Err(elog_error(format!("end_lsn is not set in {msgtype} message")));
    }
    prepare_data.prepare_time = pq_getmsgint64(in_)?;
    prepare_data.xid = pq_getmsgint(in_, 4)?;
    if prepare_data.xid == InvalidTransactionId {
        return Err(elog_error(format!(
            "invalid two-phase transaction ID in {msgtype} message"
        )));
    }

    /* read gid (copy it into a pre-allocated buffer) */
    let gid = pq_getmsgstring(mcx, in_)?;
    strlcpy_gid(&mut prepare_data.gid, gid.as_bytes());
    Ok(())
}

/// `logicalrep_read_prepare`: read transaction PREPARE from the stream.
pub fn logicalrep_read_prepare(
    mcx: Mcx<'_>,
    in_: &mut StringInfo<'_>,
    prepare_data: &mut LogicalRepPreparedTxnData,
) -> PgResult<()> {
    logicalrep_read_prepare_common(mcx, in_, "prepare", prepare_data)
}

/// `logicalrep_write_commit_prepared`: write COMMIT PREPARED to the output
/// stream.
pub fn logicalrep_write_commit_prepared(
    out: &mut StringInfo<'_>,
    txn: &ReorderBufferTXN<'_>,
    commit_lsn: XLogRecPtr,
) -> PgResult<()> {
    let flags: u8 = 0;

    pq_sendbyte(out, LOGICAL_REP_MSG_COMMIT_PREPARED)?;

    /*
     * This should only ever happen for two-phase commit transactions, in
     * which case we expect to have a valid GID.
     */
    debug_assert!(txn.gid.is_some());

    /* send the flags field */
    pq_sendbyte(out, flags)?;

    /* send fields */
    pq_sendint64(out, commit_lsn)?;
    pq_sendint64(out, txn.end_lsn)?;
    pq_sendint64(out, txn.xact_time as u64)?; /* txn->xact_time.commit_time */
    pq_sendint32(out, txn.xid)?;

    /* send gid */
    pq_sendstring(out, txn_gid(txn))?;
    Ok(())
}

/// `logicalrep_read_commit_prepared`: read transaction COMMIT PREPARED from
/// the stream.
pub fn logicalrep_read_commit_prepared(
    mcx: Mcx<'_>,
    in_: &mut StringInfo<'_>,
    prepare_data: &mut LogicalRepCommitPreparedTxnData,
) -> PgResult<()> {
    /* read flags */
    let flags = pq_getmsgbyte(in_)? as u8;

    if flags != 0 {
        return Err(elog_error(format!(
            "unrecognized flags {flags} in commit prepared message"
        )));
    }

    /* read fields */
    prepare_data.commit_lsn = pq_getmsgint64(in_)? as XLogRecPtr;
    if prepare_data.commit_lsn == InvalidXLogRecPtr {
        return Err(elog_error(
            "commit_lsn is not set in commit prepared message".into(),
        ));
    }
    prepare_data.end_lsn = pq_getmsgint64(in_)? as XLogRecPtr;
    if prepare_data.end_lsn == InvalidXLogRecPtr {
        return Err(elog_error(
            "end_lsn is not set in commit prepared message".into(),
        ));
    }
    prepare_data.commit_time = pq_getmsgint64(in_)?;
    prepare_data.xid = pq_getmsgint(in_, 4)?;

    /* read gid (copy it into a pre-allocated buffer) */
    let gid = pq_getmsgstring(mcx, in_)?;
    strlcpy_gid(&mut prepare_data.gid, gid.as_bytes());
    Ok(())
}

/// `logicalrep_write_rollback_prepared`: write ROLLBACK PREPARED to the
/// output stream.
pub fn logicalrep_write_rollback_prepared(
    out: &mut StringInfo<'_>,
    txn: &ReorderBufferTXN<'_>,
    prepare_end_lsn: XLogRecPtr,
    prepare_time: TimestampTz,
) -> PgResult<()> {
    let flags: u8 = 0;

    pq_sendbyte(out, LOGICAL_REP_MSG_ROLLBACK_PREPARED)?;

    /*
     * This should only ever happen for two-phase commit transactions, in
     * which case we expect to have a valid GID.
     */
    debug_assert!(txn.gid.is_some());

    /* send the flags field */
    pq_sendbyte(out, flags)?;

    /* send fields */
    pq_sendint64(out, prepare_end_lsn)?;
    pq_sendint64(out, txn.end_lsn)?;
    pq_sendint64(out, prepare_time as u64)?;
    pq_sendint64(out, txn.xact_time as u64)?; /* txn->xact_time.commit_time */
    pq_sendint32(out, txn.xid)?;

    /* send gid */
    pq_sendstring(out, txn_gid(txn))?;
    Ok(())
}

/// `logicalrep_read_rollback_prepared`: read transaction ROLLBACK PREPARED
/// from the stream.
pub fn logicalrep_read_rollback_prepared(
    mcx: Mcx<'_>,
    in_: &mut StringInfo<'_>,
    rollback_data: &mut LogicalRepRollbackPreparedTxnData,
) -> PgResult<()> {
    /* read flags */
    let flags = pq_getmsgbyte(in_)? as u8;

    if flags != 0 {
        return Err(elog_error(format!(
            "unrecognized flags {flags} in rollback prepared message"
        )));
    }

    /* read fields */
    rollback_data.prepare_end_lsn = pq_getmsgint64(in_)? as XLogRecPtr;
    if rollback_data.prepare_end_lsn == InvalidXLogRecPtr {
        return Err(elog_error(
            "prepare_end_lsn is not set in rollback prepared message".into(),
        ));
    }
    rollback_data.rollback_end_lsn = pq_getmsgint64(in_)? as XLogRecPtr;
    if rollback_data.rollback_end_lsn == InvalidXLogRecPtr {
        return Err(elog_error(
            "rollback_end_lsn is not set in rollback prepared message".into(),
        ));
    }
    rollback_data.prepare_time = pq_getmsgint64(in_)?;
    rollback_data.rollback_time = pq_getmsgint64(in_)?;
    rollback_data.xid = pq_getmsgint(in_, 4)?;

    /* read gid (copy it into a pre-allocated buffer) */
    let gid = pq_getmsgstring(mcx, in_)?;
    strlcpy_gid(&mut rollback_data.gid, gid.as_bytes());
    Ok(())
}

/// `logicalrep_write_stream_prepare`: write STREAM PREPARE to the output
/// stream.
pub fn logicalrep_write_stream_prepare(
    out: &mut StringInfo<'_>,
    txn: &ReorderBufferTXN<'_>,
    prepare_lsn: XLogRecPtr,
) -> PgResult<()> {
    logicalrep_write_prepare_common(out, LogicalRepMsgType::StreamPrepare, txn, prepare_lsn)
}

/// `logicalrep_read_stream_prepare`: read STREAM PREPARE from the stream.
pub fn logicalrep_read_stream_prepare(
    mcx: Mcx<'_>,
    in_: &mut StringInfo<'_>,
    prepare_data: &mut LogicalRepPreparedTxnData,
) -> PgResult<()> {
    logicalrep_read_prepare_common(mcx, in_, "stream prepare", prepare_data)
}

/// `logicalrep_write_origin`: write ORIGIN to the output stream.
pub fn logicalrep_write_origin(
    out: &mut StringInfo<'_>,
    origin: &[u8],
    origin_lsn: XLogRecPtr,
) -> PgResult<()> {
    pq_sendbyte(out, LOGICAL_REP_MSG_ORIGIN)?;

    /* fixed fields */
    pq_sendint64(out, origin_lsn)?;

    /* origin string */
    pq_sendstring(out, origin)?;
    Ok(())
}

/// `logicalrep_read_origin`: read ORIGIN from the output stream; returns the
/// origin name (`pstrdup`ed into `mcx`).
pub fn logicalrep_read_origin<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &mut StringInfo<'_>,
    origin_lsn: &mut XLogRecPtr,
) -> PgResult<PgVec<'mcx, u8>> {
    /* fixed fields */
    *origin_lsn = pq_getmsgint64(in_)? as XLogRecPtr;

    /* return origin */
    pstrdup_msgstring(mcx, in_)
}

/// `logicalrep_write_insert`: write INSERT to the output stream.
pub fn logicalrep_write_insert(
    out: &mut StringInfo<'_>,
    xid: TransactionId,
    rel: &RelationData<'_>,
    newslot: &TupleTableSlot,
    binary: bool,
    columns: Option<&Bitmapset<'_>>,
    include_gencols_type: PublishGencolsType,
) -> PgResult<()> {
    pq_sendbyte(out, LOGICAL_REP_MSG_INSERT)?;

    /* transaction ID (if not valid, we're not streaming) */
    if TransactionIdIsValid(xid) {
        pq_sendint32(out, xid)?;
    }

    /* use Oid as relation identifier */
    pq_sendint32(out, rel.rd_id)?;

    pq_sendbyte(out, b'N')?; /* new tuple follows */
    logicalrep_write_tuple(out, rel, newslot, binary, columns, include_gencols_type)
}

/// `logicalrep_read_insert`: read INSERT from stream; fills the new tuple.
pub fn logicalrep_read_insert<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &mut StringInfo<'_>,
    newtup: &mut LogicalRepTupleData<'mcx>,
) -> PgResult<LogicalRepRelId> {
    /* read the relation id */
    let relid = pq_getmsgint(in_, 4)?;

    let action = pq_getmsgbyte(in_)?;
    if action != b'N' as i32 {
        return Err(elog_error(format!("expected new tuple but got {action}")));
    }

    logicalrep_read_tuple(mcx, in_, newtup)?;

    Ok(relid)
}

/// `logicalrep_write_update`: write UPDATE to the output stream.
pub fn logicalrep_write_update(
    out: &mut StringInfo<'_>,
    xid: TransactionId,
    rel: &RelationData<'_>,
    oldslot: Option<&TupleTableSlot>,
    newslot: &TupleTableSlot,
    binary: bool,
    columns: Option<&Bitmapset<'_>>,
    include_gencols_type: PublishGencolsType,
) -> PgResult<()> {
    pq_sendbyte(out, LOGICAL_REP_MSG_UPDATE)?;

    debug_assert!(
        rel.rd_rel.relreplident == REPLICA_IDENTITY_DEFAULT
            || rel.rd_rel.relreplident == REPLICA_IDENTITY_FULL
            || rel.rd_rel.relreplident == REPLICA_IDENTITY_INDEX
    );

    /* transaction ID (if not valid, we're not streaming) */
    if TransactionIdIsValid(xid) {
        pq_sendint32(out, xid)?;
    }

    /* use Oid as relation identifier */
    pq_sendint32(out, rel.rd_id)?;

    if let Some(oldslot) = oldslot {
        if rel.rd_rel.relreplident == REPLICA_IDENTITY_FULL {
            pq_sendbyte(out, b'O')?; /* old tuple follows */
        } else {
            pq_sendbyte(out, b'K')?; /* old key follows */
        }
        logicalrep_write_tuple(out, rel, oldslot, binary, columns, include_gencols_type)?;
    }

    pq_sendbyte(out, b'N')?; /* new tuple follows */
    logicalrep_write_tuple(out, rel, newslot, binary, columns, include_gencols_type)
}

/// `logicalrep_read_update`: read UPDATE from stream.
pub fn logicalrep_read_update<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &mut StringInfo<'_>,
    has_oldtuple: &mut bool,
    oldtup: &mut LogicalRepTupleData<'mcx>,
    newtup: &mut LogicalRepTupleData<'mcx>,
) -> PgResult<LogicalRepRelId> {
    /* read the relation id */
    let relid = pq_getmsgint(in_, 4)?;

    /* read and verify action */
    let mut action = pq_getmsgbyte(in_)?;
    if action != b'K' as i32 && action != b'O' as i32 && action != b'N' as i32 {
        return Err(elog_error(format!(
            "expected action 'N', 'O' or 'K', got {}",
            action as u8 as char
        )));
    }

    /* check for old tuple */
    if action == b'K' as i32 || action == b'O' as i32 {
        logicalrep_read_tuple(mcx, in_, oldtup)?;
        *has_oldtuple = true;

        action = pq_getmsgbyte(in_)?;
    } else {
        *has_oldtuple = false;
    }

    /* check for new  tuple */
    if action != b'N' as i32 {
        return Err(elog_error(format!(
            "expected action 'N', got {}",
            action as u8 as char
        )));
    }

    logicalrep_read_tuple(mcx, in_, newtup)?;

    Ok(relid)
}

/// `logicalrep_write_delete`: write DELETE to the output stream.
pub fn logicalrep_write_delete(
    out: &mut StringInfo<'_>,
    xid: TransactionId,
    rel: &RelationData<'_>,
    oldslot: &TupleTableSlot,
    binary: bool,
    columns: Option<&Bitmapset<'_>>,
    include_gencols_type: PublishGencolsType,
) -> PgResult<()> {
    debug_assert!(
        rel.rd_rel.relreplident == REPLICA_IDENTITY_DEFAULT
            || rel.rd_rel.relreplident == REPLICA_IDENTITY_FULL
            || rel.rd_rel.relreplident == REPLICA_IDENTITY_INDEX
    );

    pq_sendbyte(out, LOGICAL_REP_MSG_DELETE)?;

    /* transaction ID (if not valid, we're not streaming) */
    if TransactionIdIsValid(xid) {
        pq_sendint32(out, xid)?;
    }

    /* use Oid as relation identifier */
    pq_sendint32(out, rel.rd_id)?;

    if rel.rd_rel.relreplident == REPLICA_IDENTITY_FULL {
        pq_sendbyte(out, b'O')?; /* old tuple follows */
    } else {
        pq_sendbyte(out, b'K')?; /* old key follows */
    }

    logicalrep_write_tuple(out, rel, oldslot, binary, columns, include_gencols_type)
}

/// `logicalrep_read_delete`: read DELETE from stream; fills the old tuple.
pub fn logicalrep_read_delete<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &mut StringInfo<'_>,
    oldtup: &mut LogicalRepTupleData<'mcx>,
) -> PgResult<LogicalRepRelId> {
    /* read the relation id */
    let relid = pq_getmsgint(in_, 4)?;

    /* read and verify action */
    let action = pq_getmsgbyte(in_)?;
    if action != b'K' as i32 && action != b'O' as i32 {
        return Err(elog_error(format!(
            "expected action 'O' or 'K', got {}",
            action as u8 as char
        )));
    }

    logicalrep_read_tuple(mcx, in_, oldtup)?;

    Ok(relid)
}

/// `logicalrep_write_truncate`: write TRUNCATE to the output stream.
pub fn logicalrep_write_truncate(
    out: &mut StringInfo<'_>,
    xid: TransactionId,
    nrelids: i32,
    relids: &[Oid],
    cascade: bool,
    restart_seqs: bool,
) -> PgResult<()> {
    let mut flags: u8 = 0;

    pq_sendbyte(out, LOGICAL_REP_MSG_TRUNCATE)?;

    /* transaction ID (if not valid, we're not streaming) */
    if TransactionIdIsValid(xid) {
        pq_sendint32(out, xid)?;
    }

    pq_sendint32(out, nrelids as u32)?;

    /* encode and send truncate flags */
    if cascade {
        flags |= TRUNCATE_CASCADE;
    }
    if restart_seqs {
        flags |= TRUNCATE_RESTART_SEQS;
    }
    pq_sendint8(out, flags)?;

    for i in 0..nrelids {
        pq_sendint32(out, relids[i as usize])?;
    }
    Ok(())
}

/// `logicalrep_read_truncate`: read TRUNCATE from stream; returns the relid
/// list (the C `List *` of OIDs).
pub fn logicalrep_read_truncate<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &mut StringInfo<'_>,
    cascade: &mut bool,
    restart_seqs: &mut bool,
) -> PgResult<PgVec<'mcx, Oid>> {
    let nrelids = pq_getmsgint(in_, 4)? as i32;

    /* read and decode truncate flags */
    let flags = pq_getmsgint(in_, 1)? as u8;
    *cascade = (flags & TRUNCATE_CASCADE) > 0;
    *restart_seqs = (flags & TRUNCATE_RESTART_SEQS) > 0;

    let mut relids = vec_with_capacity_in(mcx, nrelids.max(0) as usize)?; /* NIL */
    for _i in 0..nrelids {
        relids.push(pq_getmsgint(in_, 4)?); /* lappend_oid */
    }

    Ok(relids)
}

/// `logicalrep_write_message`: write MESSAGE to stream.
pub fn logicalrep_write_message(
    out: &mut StringInfo<'_>,
    xid: TransactionId,
    lsn: XLogRecPtr,
    transactional: bool,
    prefix: &[u8],
    sz: usize,
    message: &[u8],
) -> PgResult<()> {
    let mut flags: u8 = 0;

    pq_sendbyte(out, LOGICAL_REP_MSG_MESSAGE)?;

    /* encode and send message flags */
    if transactional {
        flags |= MESSAGE_TRANSACTIONAL;
    }

    /* transaction ID (if not valid, we're not streaming) */
    if TransactionIdIsValid(xid) {
        pq_sendint32(out, xid)?;
    }

    pq_sendint8(out, flags)?;
    pq_sendint64(out, lsn)?;
    pq_sendstring(out, prefix)?;
    pq_sendint32(out, sz as u32)?;
    pq_sendbytes(out, &message[..sz])?;
    Ok(())
}

/// `logicalrep_write_rel`: write relation description to the output stream.
pub fn logicalrep_write_rel(
    out: &mut StringInfo<'_>,
    xid: TransactionId,
    rel: &RelationData<'_>,
    columns: Option<&Bitmapset<'_>>,
    include_gencols_type: PublishGencolsType,
) -> PgResult<()> {
    pq_sendbyte(out, LOGICAL_REP_MSG_RELATION)?;

    /* transaction ID (if not valid, we're not streaming) */
    if TransactionIdIsValid(xid) {
        pq_sendint32(out, xid)?;
    }

    /* use Oid as relation identifier */
    pq_sendint32(out, rel.rd_id)?;

    /* send qualified relation name */
    logicalrep_write_namespace(out, rel.rd_rel.relnamespace)?;
    pq_sendstring(out, rel.rd_rel.relname.as_bytes())?;

    /* send replica identity */
    pq_sendbyte(out, rel.rd_rel.relreplident)?;

    /* send the attribute info */
    logicalrep_write_attrs(out, rel, columns, include_gencols_type)
}

/// `logicalrep_read_rel`: read the relation info from stream and return it as
/// a `LogicalRepRelation` (the C palloc's the struct in the caller's
/// context).
pub fn logicalrep_read_rel<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &mut StringInfo<'_>,
) -> PgResult<LogicalRepRelation<'mcx>> {
    let remoteid = pq_getmsgint(in_, 4)?;

    /* Read relation name from stream */
    let nspname = logicalrep_read_namespace(mcx, in_)?;
    let relname = pstrdup_msgstring(mcx, in_)?;

    /* Read the replica identity. */
    let replident = pq_getmsgbyte(in_)? as u8;

    let mut rel = LogicalRepRelation {
        remoteid,
        nspname,
        relname,
        natts: 0,
        attnames: PgVec::new_in(mcx),
        atttyps: PgVec::new_in(mcx),
        replident,
        relkind: 0,
        attkeys: None,
    };

    /* Get attribute description */
    logicalrep_read_attrs(mcx, in_, &mut rel)?;

    Ok(rel)
}

/// `logicalrep_write_typ`: write type info to the output stream (always base
/// type info).
pub fn logicalrep_write_typ(
    out: &mut StringInfo<'_>,
    xid: TransactionId,
    typoid: Oid,
) -> PgResult<()> {
    let basetypoid = lsyscache_seams::get_base_type::call(typoid)?;

    pq_sendbyte(out, LOGICAL_REP_MSG_TYPE)?;

    /* transaction ID (if not valid, we're not streaming) */
    if TransactionIdIsValid(xid) {
        pq_sendint32(out, xid)?;
    }

    let mcx = out.allocator();
    let tup = SearchSysCache1(mcx, TYPEOID, SysCacheKey::Value(ScalarWord::from_oid(basetypoid)))?;
    let Some(tup) = tup else {
        return Err(elog_error(format!(
            "cache lookup failed for type {basetypoid}"
        )));
    };

    /* use Oid as type identifier */
    pq_sendint32(out, typoid)?;

    /* send qualified type name */
    let typnamespace = pg_type_attr_oid(mcx, &tup, Anum_pg_type_typnamespace)?;
    let typname = pg_type_attr_name(mcx, &tup, Anum_pg_type_typname)?;
    logicalrep_write_namespace(out, typnamespace)?;
    pq_sendstring(out, &typname)?;

    ReleaseSysCache(tup);
    Ok(())
}

/// `logicalrep_read_typ`: read type info from the output stream.
pub fn logicalrep_read_typ<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &mut StringInfo<'_>,
    ltyp: &mut LogicalRepTyp<'mcx>,
) -> PgResult<()> {
    ltyp.remoteid = pq_getmsgint(in_, 4)?;

    /* Read type name from stream */
    ltyp.nspname = logicalrep_read_namespace(mcx, in_)?;
    ltyp.typname = pstrdup_msgstring(mcx, in_)?;
    Ok(())
}

/// `logicalrep_write_tuple`: write a tuple to the output stream, in the most
/// efficient format possible.
fn logicalrep_write_tuple(
    out: &mut StringInfo<'_>,
    rel: &RelationData<'_>,
    slot: &TupleTableSlot,
    binary: bool,
    columns: Option<&Bitmapset<'_>>,
    include_gencols_type: PublishGencolsType,
) -> PgResult<()> {
    let desc = &rel.rd_att;
    let mut nliveatts: u16 = 0;

    for i in 0..desc.natts {
        let att = &desc.attrs[i as usize];

        if !logicalrep_should_publish_column(att, columns, include_gencols_type) {
            continue;
        }

        nliveatts += 1;
    }
    pq_sendint16(out, nliveatts)?;

    let mcx = out.allocator();

    /* slot_getallattrs(slot); values = slot->tts_values; isnull = slot->tts_isnull */
    let cols = exectuples_seams::slot_getallattrs::call(mcx, slot)?;

    /* Write the values */
    for i in 0..desc.natts {
        let att = &desc.attrs[i as usize];

        if !logicalrep_should_publish_column(att, columns, include_gencols_type) {
            continue;
        }

        let (value, isnull) = &cols[i as usize];

        if *isnull {
            pq_sendbyte(out, LOGICALREP_COLUMN_NULL)?;
            continue;
        }

        if att.attlen == -1 && varatt_is_external_ondisk(value) {
            /*
             * Unchanged toasted datum.  (Note that we don't promise to detect
             * unchanged data in general; this is just a cheap check to avoid
             * sending large values unnecessarily.)
             */
            pq_sendbyte(out, LOGICALREP_COLUMN_UNCHANGED)?;
            continue;
        }

        let typtup =
            SearchSysCache1(mcx, TYPEOID, SysCacheKey::Value(ScalarWord::from_oid(att.atttypid)))?;
        let Some(typtup) = typtup else {
            return Err(elog_error(format!(
                "cache lookup failed for type {}",
                att.atttypid
            )));
        };
        let typsend = pg_type_attr_oid(mcx, &typtup, Anum_pg_type_typsend)?;
        let typoutput = pg_type_attr_oid(mcx, &typtup, Anum_pg_type_typoutput)?;

        /*
         * Send in binary if requested and type has suitable send function.
         */
        if binary && OidIsValid(typsend) {
            pq_sendbyte(out, LOGICALREP_COLUMN_BINARY)?;
            let outputbytes = fmgr_seams::oid_send_function_call::call(mcx, typsend, value)?;
            let len = outputbytes.len(); /* VARSIZE - VARHDRSZ (header stripped by the seam) */
            pq_sendint(out, len as u32, 4)?; /* length */
            pq_sendbytes(out, &outputbytes)?; /* data */
            /* pfree(outputbytes) on drop */
        } else {
            pq_sendbyte(out, LOGICALREP_COLUMN_TEXT)?;
            let outputstr = fmgr_seams::oid_output_function_call::call(mcx, typoutput, value)?;
            pq_sendcountedtext(out, &outputstr)?;
            /* pfree(outputstr) on drop */
        }

        ReleaseSysCache(typtup);
    }
    Ok(())
}

/// `logicalrep_read_tuple`: read a tuple in logical replication format from
/// the stream.
fn logicalrep_read_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &mut StringInfo<'_>,
    tuple: &mut LogicalRepTupleData<'mcx>,
) -> PgResult<()> {
    /* Get number of attributes */
    let natts = pq_getmsgint(in_, 2)? as i32;

    /* Allocate space for per-column values; zero out unused StringInfoDatas */
    let mut colvalues: PgVec<'mcx, StringInfo<'mcx>> =
        vec_with_capacity_in(mcx, natts as usize)?;
    let mut colstatus: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, natts as usize)?;

    /* Read the data */
    for _i in 0..natts {
        let kind = pq_getmsgbyte(in_)? as u8;
        colstatus.push(kind);

        match kind {
            LOGICALREP_COLUMN_NULL => {
                /* nothing more to do */
                colvalues.push(StringInfo::new_in(mcx));
            }
            LOGICALREP_COLUMN_UNCHANGED => {
                /* we don't receive the value of an unchanged column */
                colvalues.push(StringInfo::new_in(mcx));
            }
            LOGICALREP_COLUMN_TEXT | LOGICALREP_COLUMN_BINARY => {
                let len = pq_getmsgint(in_, 4)? as i32; /* read length */

                /*
                 * and data: palloc(len + 1) + pq_copymsgbytes + the
                 * NUL-termination the text input functions require. The owned
                 * StringInfo stores no trailing sentinel; the `len`-byte
                 * content is identical.
                 */
                let raw = pq_getmsgbytes(in_, len as usize)?;
                let buff = slice_in(mcx, raw)?;
                colvalues.push(StringInfo::from_vec(buff)); /* initStringInfoFromString */
            }
            _ => {
                return Err(elog_error(format!(
                    "unrecognized data representation type '{}'",
                    kind as char
                )));
            }
        }
    }

    tuple.colvalues = colvalues;
    tuple.colstatus = colstatus;
    tuple.ncols = natts;
    Ok(())
}

/// `logicalrep_write_attrs`: write relation attribute metadata to the
/// stream.
fn logicalrep_write_attrs(
    out: &mut StringInfo<'_>,
    rel: &RelationData<'_>,
    columns: Option<&Bitmapset<'_>>,
    include_gencols_type: PublishGencolsType,
) -> PgResult<()> {
    let desc = &rel.rd_att;
    let mut nliveatts: u16 = 0;

    /* send number of live attributes */
    for i in 0..desc.natts {
        let att = &desc.attrs[i as usize];

        if !logicalrep_should_publish_column(att, columns, include_gencols_type) {
            continue;
        }

        nliveatts += 1;
    }
    pq_sendint16(out, nliveatts)?;

    /* fetch bitmap of REPLICATION IDENTITY attributes */
    let replidentfull = rel.rd_rel.relreplident == REPLICA_IDENTITY_FULL;
    let idattrs = if !replidentfull {
        relcache_seams::relation_get_identity_key_bitmap::call(out.allocator(), rel)?
    } else {
        None
    };

    /* send the attributes */
    for i in 0..desc.natts {
        let att = &desc.attrs[i as usize];
        let mut flags: u8 = 0;

        if !logicalrep_should_publish_column(att, columns, include_gencols_type) {
            continue;
        }

        /* REPLICA IDENTITY FULL means all columns are sent as part of key. */
        if replidentfull
            || bms_seams::bms_is_member::call(
                att.attnum as i32 - FirstLowInvalidHeapAttributeNumber as i32,
                idattrs.as_deref(),
            )
        {
            flags |= LOGICALREP_IS_REPLICA_IDENTITY;
        }

        pq_sendbyte(out, flags)?;

        /* attribute name */
        pq_sendstring(out, att.attname.name_str())?;

        /* attribute type id */
        pq_sendint32(out, att.atttypid)?;

        /* attribute mode */
        pq_sendint32(out, att.atttypmod as u32)?;
    }

    /* bms_free(idattrs) */
    drop(idattrs);
    Ok(())
}

/// `logicalrep_read_attrs`: read relation attribute metadata from the
/// stream.
fn logicalrep_read_attrs<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &mut StringInfo<'_>,
    rel: &mut LogicalRepRelation<'mcx>,
) -> PgResult<()> {
    let natts = pq_getmsgint(in_, 2)? as i32;
    let mut attnames: PgVec<'mcx, PgVec<'mcx, u8>> = vec_with_capacity_in(mcx, natts as usize)?;
    let mut atttyps: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, natts as usize)?;
    let mut attkeys: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None; /* NULL */

    /* read the attributes */
    for i in 0..natts {
        /* Check for replica identity column */
        let flags = pq_getmsgbyte(in_)? as u8;
        if flags & LOGICALREP_IS_REPLICA_IDENTITY != 0 {
            attkeys = Some(bms_seams::bms_add_member::call(mcx, attkeys.take(), i)?);
        }

        /* attribute name */
        attnames.push(pstrdup_msgstring(mcx, in_)?);

        /* attribute type id */
        atttyps.push(pq_getmsgint(in_, 4)?); /* (Oid) pq_getmsgint(in, 4) */

        /* we ignore attribute mode for now */
        let _ = pq_getmsgint(in_, 4)?;
    }

    rel.attnames = attnames;
    rel.atttyps = atttyps;
    rel.attkeys = attkeys;
    rel.natts = natts;
    Ok(())
}

/// `logicalrep_write_namespace`: write the namespace name, or empty string
/// for pg_catalog (to save space).
fn logicalrep_write_namespace(out: &mut StringInfo<'_>, nspid: Oid) -> PgResult<()> {
    if nspid == PG_CATALOG_NAMESPACE {
        pq_sendbyte(out, b'\0')?;
    } else {
        let nspname = lsyscache_seams::get_namespace_name::call(out.allocator(), nspid)?;

        let Some(nspname) = nspname else {
            return Err(elog_error(format!(
                "cache lookup failed for namespace {nspid}"
            )));
        };

        pq_sendstring(out, nspname.as_bytes())?;
    }
    Ok(())
}

/// `logicalrep_read_namespace`: read the namespace name while treating empty
/// string as pg_catalog. (The single `pstrdup` the C caller performs is the
/// copy made here.)
fn logicalrep_read_namespace<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &mut StringInfo<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let nspname = pstrdup_msgstring(mcx, in_)?;

    if nspname.is_empty() {
        return slice_in(mcx, b"pg_catalog");
    }

    Ok(nspname)
}

/// `logicalrep_write_stream_start`: write the start stream message to the
/// output stream.
pub fn logicalrep_write_stream_start(
    out: &mut StringInfo<'_>,
    xid: TransactionId,
    first_segment: bool,
) -> PgResult<()> {
    pq_sendbyte(out, LOGICAL_REP_MSG_STREAM_START)?;

    debug_assert!(TransactionIdIsValid(xid));

    /* transaction ID (we're starting to stream, so must be valid) */
    pq_sendint32(out, xid)?;

    /* 1 if this is the first streaming segment for this xid */
    pq_sendbyte(out, if first_segment { 1 } else { 0 })?;
    Ok(())
}

/// `logicalrep_read_stream_start`: read the start stream message.
pub fn logicalrep_read_stream_start(
    in_: &mut StringInfo<'_>,
    first_segment: &mut bool,
) -> PgResult<TransactionId> {
    /* Assert(first_segment): the out-param reference is always present. */

    let xid = pq_getmsgint(in_, 4)?;
    *first_segment = pq_getmsgbyte(in_)? == 1;

    Ok(xid)
}

/// `logicalrep_write_stream_stop`: write the stop stream message.
pub fn logicalrep_write_stream_stop(out: &mut StringInfo<'_>) -> PgResult<()> {
    pq_sendbyte(out, LOGICAL_REP_MSG_STREAM_STOP)
}

/// `logicalrep_write_stream_commit`: write STREAM COMMIT to the output
/// stream.
pub fn logicalrep_write_stream_commit(
    out: &mut StringInfo<'_>,
    txn: &ReorderBufferTXN<'_>,
    commit_lsn: XLogRecPtr,
) -> PgResult<()> {
    let flags: u8 = 0;

    pq_sendbyte(out, LOGICAL_REP_MSG_STREAM_COMMIT)?;

    debug_assert!(TransactionIdIsValid(txn.xid));

    /* transaction ID */
    pq_sendint32(out, txn.xid)?;

    /* send the flags field (unused for now) */
    pq_sendbyte(out, flags)?;

    /* send fields */
    pq_sendint64(out, commit_lsn)?;
    pq_sendint64(out, txn.end_lsn)?;
    pq_sendint64(out, txn.xact_time as u64)?; /* txn->xact_time.commit_time */
    Ok(())
}

/// `logicalrep_read_stream_commit`: read STREAM COMMIT from the output
/// stream.
pub fn logicalrep_read_stream_commit(
    in_: &mut StringInfo<'_>,
    commit_data: &mut LogicalRepCommitData,
) -> PgResult<TransactionId> {
    let xid = pq_getmsgint(in_, 4)?;

    /* read flags (unused for now) */
    let flags = pq_getmsgbyte(in_)? as u8;

    if flags != 0 {
        return Err(elog_error(format!(
            "unrecognized flags {flags} in commit message"
        )));
    }

    /* read fields */
    commit_data.commit_lsn = pq_getmsgint64(in_)? as XLogRecPtr;
    commit_data.end_lsn = pq_getmsgint64(in_)? as XLogRecPtr;
    commit_data.committime = pq_getmsgint64(in_)?;

    Ok(xid)
}

/// `logicalrep_write_stream_abort`: write STREAM ABORT to the output stream.
/// Note that `xid` and `subxid` will be the same for the top-level
/// transaction abort. If `write_abort_info` is true, send the `abort_lsn`
/// and `abort_time` fields, otherwise don't.
pub fn logicalrep_write_stream_abort(
    out: &mut StringInfo<'_>,
    xid: TransactionId,
    subxid: TransactionId,
    abort_lsn: XLogRecPtr,
    abort_time: TimestampTz,
    write_abort_info: bool,
) -> PgResult<()> {
    pq_sendbyte(out, LOGICAL_REP_MSG_STREAM_ABORT)?;

    debug_assert!(TransactionIdIsValid(xid) && TransactionIdIsValid(subxid));

    /* transaction ID */
    pq_sendint32(out, xid)?;
    pq_sendint32(out, subxid)?;

    if write_abort_info {
        pq_sendint64(out, abort_lsn)?;
        pq_sendint64(out, abort_time as u64)?;
    }
    Ok(())
}

/// `logicalrep_read_stream_abort`: read STREAM ABORT from the output stream.
/// If `read_abort_info` is true, read the `abort_lsn` and `abort_time`
/// fields, otherwise don't.
pub fn logicalrep_read_stream_abort(
    in_: &mut StringInfo<'_>,
    abort_data: &mut LogicalRepStreamAbortData,
    read_abort_info: bool,
) -> PgResult<()> {
    /* Assert(abort_data): the out-param reference is always present. */

    abort_data.xid = pq_getmsgint(in_, 4)?;
    abort_data.subxid = pq_getmsgint(in_, 4)?;

    if read_abort_info {
        abort_data.abort_lsn = pq_getmsgint64(in_)? as XLogRecPtr;
        abort_data.abort_time = pq_getmsgint64(in_)?;
    } else {
        abort_data.abort_lsn = InvalidXLogRecPtr;
        abort_data.abort_time = 0;
    }
    Ok(())
}

/// `logicalrep_message_type`: get a string representing a
/// `LogicalRepMsgType`.
///
/// Takes the raw enum integer value (the C enum holds any `int`); unknown
/// values yield the C `"??? (%d)"` indicator rather than throwing, because
/// the result provides context for another error being raised.
pub fn logicalrep_message_type(action: i32) -> String {
    let s = match action {
        x if x == LOGICAL_REP_MSG_BEGIN as i32 => "BEGIN",
        x if x == LOGICAL_REP_MSG_COMMIT as i32 => "COMMIT",
        x if x == LOGICAL_REP_MSG_ORIGIN as i32 => "ORIGIN",
        x if x == LOGICAL_REP_MSG_INSERT as i32 => "INSERT",
        x if x == LOGICAL_REP_MSG_UPDATE as i32 => "UPDATE",
        x if x == LOGICAL_REP_MSG_DELETE as i32 => "DELETE",
        x if x == LOGICAL_REP_MSG_TRUNCATE as i32 => "TRUNCATE",
        x if x == LOGICAL_REP_MSG_RELATION as i32 => "RELATION",
        x if x == LOGICAL_REP_MSG_TYPE as i32 => "TYPE",
        x if x == LOGICAL_REP_MSG_MESSAGE as i32 => "MESSAGE",
        x if x == LOGICAL_REP_MSG_BEGIN_PREPARE as i32 => "BEGIN PREPARE",
        x if x == LOGICAL_REP_MSG_PREPARE as i32 => "PREPARE",
        x if x == LOGICAL_REP_MSG_COMMIT_PREPARED as i32 => "COMMIT PREPARED",
        x if x == LOGICAL_REP_MSG_ROLLBACK_PREPARED as i32 => "ROLLBACK PREPARED",
        x if x == LOGICAL_REP_MSG_STREAM_START as i32 => "STREAM START",
        x if x == LOGICAL_REP_MSG_STREAM_STOP as i32 => "STREAM STOP",
        x if x == LOGICAL_REP_MSG_STREAM_COMMIT as i32 => "STREAM COMMIT",
        x if x == LOGICAL_REP_MSG_STREAM_ABORT as i32 => "STREAM ABORT",
        x if x == LOGICAL_REP_MSG_STREAM_PREPARE as i32 => "STREAM PREPARE",
        _ => {
            /*
             * This message provides context in the error raised when applying
             * a logical message. So we can't throw an error here. Return an
             * unknown indicator value so that the original error is still
             * reported.
             */
            return format!("??? ({action})");
        }
    };
    s.into()
}

/// `logicalrep_should_publish_column`: check if the column `att` of a table
/// should be published.
///
/// `columns` represents the publication column list (if any) for that table;
/// `include_gencols_type` indicates whether generated columns should be
/// published when there is no column list. Note that generated columns can be
/// published only when present in a publication column list, or when
/// `include_gencols_type` is `PUBLISH_GENCOLS_STORED`.
pub fn logicalrep_should_publish_column(
    att: &FormData_pg_attribute,
    columns: Option<&Bitmapset<'_>>,
    include_gencols_type: PublishGencolsType,
) -> bool {
    if att.attisdropped {
        return false;
    }

    /* If a column list is provided, publish only the cols in that list. */
    if columns.is_some() {
        return bms_seams::bms_is_member::call(att.attnum as i32, columns);
    }

    /* All non-generated columns are always published. */
    if att.attgenerated == 0 {
        return true;
    }

    /*
     * Stored generated columns are only published when the user sets
     * publish_generated_columns as stored.
     */
    if att.attgenerated == ATTRIBUTE_GENERATED_STORED {
        return include_gencols_type == PublishGencolsType::Stored;
    }

    false
}
