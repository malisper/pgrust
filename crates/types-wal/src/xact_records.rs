//! Argument bundles for the `XactLogCommitRecord` / `XactLogAbortRecord`
//! emitters owned by `access/transam/xact.c` (`access/xact.h`). These are the
//! marshaled inputs the 2PC commit/abort emitters hand to the xact crate's
//! WAL-record builders; trimmed to the fields the twophase consumer fills.

use crate::wal::RelFileLocator;
use alloc::string::String;
use alloc::vec::Vec;
use types_core::xact::XlXactStatsItem;
use types_core::{Oid, RepOriginId, TimestampTz, TransactionId, XLogRecPtr};

/// Replication-origin metadata carried on a commit/abort record, matching
/// C's `xl_xact_origin` (`{ XLogRecPtr origin_lsn; TimestampTz origin_timestamp; }`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct XlXactOrigin {
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

/// Full argument list for `XactLogCommitRecord`, mirroring the C signature.
/// The xact crate uses these to derive `xinfo` + opcode and assemble the
/// record body.
#[derive(Clone, Debug)]
pub struct XactLogCommitRecordArgs {
    pub commit_time: TimestampTz,
    pub subxacts: Vec<TransactionId>,
    pub rels: Vec<RelFileLocator>,
    pub dropped_stats: Vec<XlXactStatsItem>,
    pub msgs: Vec<u8>,
    pub nmsgs: i32,
    pub relcache_inval: bool,
    pub xactflags: i32,
    pub twophase_xid: TransactionId,
    pub twophase_gid: Option<String>,
    pub force_sync_commit: bool,
    pub synchronous_commit: i32,
    pub xlog_logical_info_active: bool,
    pub my_database_id: Oid,
    pub my_database_table_space: Oid,
    pub replorigin_session_origin: RepOriginId,
    pub origin: Option<XlXactOrigin>,
}

/// Full argument list for `XactLogAbortRecord`, mirroring the C signature.
#[derive(Clone, Debug)]
pub struct XactLogAbortRecordArgs {
    pub abort_time: TimestampTz,
    pub subxacts: Vec<TransactionId>,
    pub rels: Vec<RelFileLocator>,
    pub dropped_stats: Vec<XlXactStatsItem>,
    pub xactflags: i32,
    pub twophase_xid: TransactionId,
    pub twophase_gid: Option<String>,
    pub xlog_logical_info_active: bool,
    pub my_database_id: Oid,
    pub my_database_table_space: Oid,
    pub replorigin_session_origin: RepOriginId,
    pub origin: Option<XlXactOrigin>,
}
