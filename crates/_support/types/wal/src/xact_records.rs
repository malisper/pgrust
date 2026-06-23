//! Argument bundles for the `XactLogCommitRecord` / `XactLogAbortRecord`
//! emitters owned by `access/transam/xact.c` (`access/xact.h`). These are the
//! marshaled inputs the 2PC commit/abort emitters hand to the xact crate's
//! WAL-record builders; trimmed to the fields the twophase consumer fills.

use crate::wal::RelFileLocator;
use alloc::string::String;
use alloc::vec::Vec;
use ::types_core::xact::XlXactStatsItem;
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

/// Argument bundle the xact consumer (`PrepareTransaction`) hands to the
/// `StartPrepare` seam. C's `StartPrepare(gxact)` reads these from the current
/// backend transaction (`xactGetCommittedChildren`, `smgrGetPendingDeletes`,
/// `pgstat_get_transactional_drops`, `xactGetCommittedInvalidationMessages`,
/// `proc->databaseId`); the consumer gathers them — the data is the committing
/// backend's — and the owner writes them into the 2PC state-file builder.
///
/// Each `*` segment arrives already serialized into the C on-disk byte layout
/// the 2PC state file expects (`RelFileLocator` = 12B, `xl_xact_stats_item` =
/// 16B, `SharedInvalidationMessage`), with the element count alongside. The
/// consumer serializes them because it holds the canonical struct mirrors and
/// already has the WAL-record serializers; the owner appends the bytes raw,
/// matching C's `save_state_data(ptr, n * sizeof(...))`. `children` stays typed
/// because the owner also stuffs it into the dummy PGPROC (`GXactLoadSubxactData`).
#[derive(Clone, Debug)]
pub struct StartPrepareArgs {
    pub xid: TransactionId,
    pub gid: String,
    pub prepared_at: TimestampTz,
    pub owner: Oid,
    pub databaseid: Oid,
    pub children: Vec<TransactionId>,
    /// Serialized commit `RelFileLocator[]` and its element count.
    pub commitrels: Vec<u8>,
    pub ncommitrels: i32,
    /// Serialized abort `RelFileLocator[]` and its element count.
    pub abortrels: Vec<u8>,
    pub nabortrels: i32,
    pub commitstats: Vec<u8>,
    pub ncommitstats: i32,
    pub abortstats: Vec<u8>,
    pub nabortstats: i32,
    pub invalmsgs: Vec<u8>,
    pub ninvalmsgs: i32,
    pub initfileinval: bool,
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
