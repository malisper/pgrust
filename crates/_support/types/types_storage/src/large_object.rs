//! Large-object vocabulary (`storage/large_object.h`): the open-descriptor
//! [`LargeObjectDesc`], the page/size limits, and the descriptor flag bits.

use alloc::rc::Rc;

use ::types_core::{int64, uint64, Oid, SubTransactionId, BLCKSZ};
use ::snapshot::SnapshotData;

/// `Snapshot` (`utils/snapshot.h`) — the C `Snapshot` is a shared pointer; the
/// owned model carries it as `Option<Rc<SnapshotData>>` (`None` is the C
/// `NULL`).
pub type Snapshot = Option<Rc<SnapshotData>>;

/// `IFS_RDLOCK` (`storage/large_object.h`) — the LO was opened for reading
/// (and the SELECT permission has been checked).
pub const IFS_RDLOCK: i32 = 1 << 0;
/// `IFS_WRLOCK` (`storage/large_object.h`) — the LO was opened for writing
/// (and the UPDATE permission has been checked).
pub const IFS_WRLOCK: i32 = 1 << 1;

/// `LOBLKSIZE` (`storage/large_object.h`) — each "page" (tuple) of a large
/// object holds this much data: `BLCKSZ / 4`.
pub const LOBLKSIZE: i32 = (BLCKSZ / 4) as i32;

/// `MAX_LARGE_OBJECT_SIZE` (`storage/large_object.h`) — maximum length in
/// bytes for a large object: `(int64) INT_MAX * LOBLKSIZE`.
pub const MAX_LARGE_OBJECT_SIZE: int64 = i32::MAX as int64 * LOBLKSIZE as int64;

/// `LargeObjectDesc` (`storage/large_object.h`) — data about a currently-open
/// large object.
///
/// NOTE: as of v11, permission checks are made when the large object is opened;
/// therefore [`IFS_RDLOCK`]/[`IFS_WRLOCK`] indicate that read or write mode has
/// been requested *and* the corresponding permission has been checked.
#[derive(Debug)]
pub struct LargeObjectDesc {
    /// `Oid id` — the LO's logical identifier.
    pub id: Oid,
    /// `Snapshot snapshot` — snapshot to use for read/write operations.
    pub snapshot: Snapshot,
    /// `SubTransactionId subid` — owning subtransaction ID.
    pub subid: SubTransactionId,
    /// `uint64 offset` — current seek pointer within the LO.
    pub offset: uint64,
    /// `int flags` — see [`IFS_RDLOCK`] / [`IFS_WRLOCK`].
    pub flags: i32,
}
