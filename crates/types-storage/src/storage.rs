//! Trimmed copy of the src-idiomatic `types::storage` module: the LWLock
//! handle and its supporting pieces.

use types_core::{uint8, uint16, uint32, Oid, ProcNumber, RelFileNumber, TransactionId, INVALID_PROC_NUMBER};

/// `LWLockMode` (`storage/lwlock.h`).
pub type LWLockMode = u32;
pub const LW_EXCLUSIVE: LWLockMode = 0;
pub const LW_SHARED: LWLockMode = 1;
pub const LW_WAIT_UNTIL_FREE: LWLockMode = 2;

/// `pg_atomic_uint32` (`port/atomics.h`) — a shmem-resident atomic word.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct pg_atomic_uint32 {
    pub value: uint32,
}

/// `proclist_head` (`storage/proclist_types.h`) — head/tail pgprocno indexes of
/// a doubly-linked PGPROC list; `INVALID_PROC_NUMBER` at the ends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct proclist_head {
    pub head: ProcNumber,
    pub tail: ProcNumber,
}

impl Default for proclist_head {
    fn default() -> Self {
        Self {
            head: INVALID_PROC_NUMBER,
            tail: INVALID_PROC_NUMBER,
        }
    }
}

/// `LWLock` (`storage/lwlock.h`): tranche id, atomic lock state, and the list
/// of waiting PGPROCs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LWLock {
    pub tranche: uint16,
    pub state: pg_atomic_uint32,
    pub waiters: proclist_head,
}

/// `NUM_INDIVIDUAL_LWLOCKS` — generated from `lwlocklist.h`.
pub const NUM_INDIVIDUAL_LWLOCKS: i32 = 54;

// `BuiltinTrancheIds` (`storage/lwlock.h`) — the chain from
// `LWTRANCHE_XACT_BUFFER = NUM_INDIVIDUAL_LWLOCKS` down to the tranche the
// pgstat ports consume (`LWTRANCHE_PGSTATS_DATA`).
pub const LWTRANCHE_XACT_BUFFER: i32 = NUM_INDIVIDUAL_LWLOCKS;
pub const LWTRANCHE_COMMITTS_BUFFER: i32 = LWTRANCHE_XACT_BUFFER + 1;
pub const LWTRANCHE_SUBTRANS_BUFFER: i32 = LWTRANCHE_COMMITTS_BUFFER + 1;
pub const LWTRANCHE_MULTIXACTOFFSET_BUFFER: i32 = LWTRANCHE_SUBTRANS_BUFFER + 1;
pub const LWTRANCHE_MULTIXACTMEMBER_BUFFER: i32 = LWTRANCHE_MULTIXACTOFFSET_BUFFER + 1;
pub const LWTRANCHE_NOTIFY_BUFFER: i32 = LWTRANCHE_MULTIXACTMEMBER_BUFFER + 1;
pub const LWTRANCHE_SERIAL_BUFFER: i32 = LWTRANCHE_NOTIFY_BUFFER + 1;
pub const LWTRANCHE_WAL_INSERT: i32 = LWTRANCHE_SERIAL_BUFFER + 1;
pub const LWTRANCHE_BUFFER_CONTENT: i32 = LWTRANCHE_WAL_INSERT + 1;
pub const LWTRANCHE_REPLICATION_ORIGIN_STATE: i32 = LWTRANCHE_BUFFER_CONTENT + 1;
pub const LWTRANCHE_REPLICATION_SLOT_IO: i32 = LWTRANCHE_REPLICATION_ORIGIN_STATE + 1;
pub const LWTRANCHE_LOCK_FASTPATH: i32 = LWTRANCHE_REPLICATION_SLOT_IO + 1;
pub const LWTRANCHE_BUFFER_MAPPING: i32 = LWTRANCHE_LOCK_FASTPATH + 1;
pub const LWTRANCHE_LOCK_MANAGER: i32 = LWTRANCHE_BUFFER_MAPPING + 1;
pub const LWTRANCHE_PREDICATE_LOCK_MANAGER: i32 = LWTRANCHE_LOCK_MANAGER + 1;
pub const LWTRANCHE_PARALLEL_HASH_JOIN: i32 = LWTRANCHE_PREDICATE_LOCK_MANAGER + 1;
pub const LWTRANCHE_PARALLEL_BTREE_SCAN: i32 = LWTRANCHE_PARALLEL_HASH_JOIN + 1;
pub const LWTRANCHE_PARALLEL_QUERY_DSA: i32 = LWTRANCHE_PARALLEL_BTREE_SCAN + 1;
pub const LWTRANCHE_PER_SESSION_DSA: i32 = LWTRANCHE_PARALLEL_QUERY_DSA + 1;
pub const LWTRANCHE_PER_SESSION_RECORD_TYPE: i32 = LWTRANCHE_PER_SESSION_DSA + 1;
pub const LWTRANCHE_PER_SESSION_RECORD_TYPMOD: i32 = LWTRANCHE_PER_SESSION_RECORD_TYPE + 1;
pub const LWTRANCHE_SHARED_TUPLESTORE: i32 = LWTRANCHE_PER_SESSION_RECORD_TYPMOD + 1;
pub const LWTRANCHE_SHARED_TIDBITMAP: i32 = LWTRANCHE_SHARED_TUPLESTORE + 1;
pub const LWTRANCHE_PARALLEL_APPEND: i32 = LWTRANCHE_SHARED_TIDBITMAP + 1;
pub const LWTRANCHE_PER_XACT_PREDICATE_LIST: i32 = LWTRANCHE_PARALLEL_APPEND + 1;
pub const LWTRANCHE_PGSTATS_DSA: i32 = LWTRANCHE_PER_XACT_PREDICATE_LIST + 1;
pub const LWTRANCHE_PGSTATS_HASH: i32 = LWTRANCHE_PGSTATS_DSA + 1;
pub const LWTRANCHE_PGSTATS_DATA: i32 = LWTRANCHE_PGSTATS_HASH + 1;

// ---------------------------------------------------------------------------
// Lock-manager vocabulary (`storage/lockdefs.h`, `storage/lock.h`), trimmed.
// ---------------------------------------------------------------------------

/// `LOCKMODE` (`storage/lockdefs.h`) — was C `int`.
pub type LOCKMODE = i32;
pub const NoLock: LOCKMODE = 0;
pub const AccessShareLock: LOCKMODE = 1;
pub const RowShareLock: LOCKMODE = 2;
pub const RowExclusiveLock: LOCKMODE = 3;
pub const ShareUpdateExclusiveLock: LOCKMODE = 4;
pub const ShareLock: LOCKMODE = 5;
pub const ShareRowExclusiveLock: LOCKMODE = 6;
pub const ExclusiveLock: LOCKMODE = 7;
pub const AccessExclusiveLock: LOCKMODE = 8;

/// `LOCKMETHODID` (`storage/lock.h`).
pub type LOCKMETHODID = uint16;
pub const DEFAULT_LOCKMETHOD: LOCKMETHODID = 1;
pub const USER_LOCKMETHOD: LOCKMETHODID = 2;

/// `LockTagType` (`storage/lock.h`) — trimmed to the tags ports consume.
pub type LockTagType = uint8;
pub const LOCKTAG_RELATION: LockTagType = 0;

/// `LOCKTAG` (`storage/lock.h`): the 16-byte key identifying any lockable
/// object.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LOCKTAG {
    pub locktag_field1: uint32,
    pub locktag_field2: uint32,
    pub locktag_field3: uint32,
    pub locktag_field4: uint16,
    pub locktag_type: uint8,
    pub locktag_lockmethodid: uint8,
}

/// `LockAcquireResult` (`storage/lock.h`).
pub type LockAcquireResult = u32;
pub const LOCKACQUIRE_NOT_AVAIL: LockAcquireResult = 0;
pub const LOCKACQUIRE_OK: LockAcquireResult = 1;
pub const LOCKACQUIRE_ALREADY_HELD: LockAcquireResult = 2;
pub const LOCKACQUIRE_ALREADY_CLEAR: LockAcquireResult = 3;

/// `VirtualTransactionId` (`storage/lock.h`).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct VirtualTransactionId {
    pub procNumber: ProcNumber,
    pub localTransactionId: types_core::LocalTransactionId,
}

impl VirtualTransactionId {
    /// `SetInvalidVirtualTransactionId(vxid)`.
    pub const fn invalid() -> Self {
        Self {
            procNumber: INVALID_PROC_NUMBER,
            localTransactionId: 0,
        }
    }

    /// `VirtualTransactionIdIsValid(vxid)` —
    /// `LocalTransactionIdIsValid((vxid).localTransactionId)`.
    pub const fn is_valid(self) -> bool {
        self.localTransactionId != 0
    }
}

// ---------------------------------------------------------------------------
// `storage/procsignal.h`, trimmed.
// ---------------------------------------------------------------------------

/// `ProcSignalReason` (`storage/procsignal.h`).
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcSignalReason {
    PROCSIG_CATCHUP_INTERRUPT = 0,
    PROCSIG_NOTIFY_INTERRUPT = 1,
    PROCSIG_PARALLEL_MESSAGE = 2,
    PROCSIG_WALSND_INIT_STOPPING = 3,
    PROCSIG_BARRIER = 4,
    PROCSIG_LOG_MEMORY_CONTEXT = 5,
    PROCSIG_PARALLEL_APPLY_MESSAGE = 6,
    PROCSIG_RECOVERY_CONFLICT_DATABASE = 7,
    PROCSIG_RECOVERY_CONFLICT_TABLESPACE = 8,
    PROCSIG_RECOVERY_CONFLICT_LOCK = 9,
    PROCSIG_RECOVERY_CONFLICT_SNAPSHOT = 10,
    PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT = 11,
    PROCSIG_RECOVERY_CONFLICT_BUFFERPIN = 12,
    PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK = 13,
}

pub const PROCSIG_RECOVERY_CONFLICT_FIRST: ProcSignalReason =
    ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_DATABASE;
pub const PROCSIG_RECOVERY_CONFLICT_LAST: ProcSignalReason =
    ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK;
pub const NUM_PROCSIGNALS: usize =
    ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK as usize + 1;

// ---------------------------------------------------------------------------
// `storage/standby.h` / `storage/procarray.h` running-xacts vocabulary.
// ---------------------------------------------------------------------------

/// `subxids_array_status` (`storage/standby.h`).
pub type subxids_array_status = u32;
pub const SUBXIDS_IN_ARRAY: subxids_array_status = 0;
pub const SUBXIDS_MISSING: subxids_array_status = 1;
pub const SUBXIDS_IN_SUBTRANS: subxids_array_status = 2;

/// `RunningTransactionsData` (`storage/standby.h`). The C `xids` pointer
/// (length `xcnt + subxcnt`) is context-allocated (C builds it in
/// TopMemoryContext / the current context), so it is a `PgVec` carrying its
/// allocator lifetime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunningTransactionsData<'mcx> {
    pub xcnt: i32,
    pub subxcnt: i32,
    pub subxid_status: subxids_array_status,
    pub nextXid: TransactionId,
    pub oldestRunningXid: TransactionId,
    pub oldestDatabaseRunningXid: TransactionId,
    pub latestCompletedXid: TransactionId,
    pub xids: mcx::PgVec<'mcx, TransactionId>,
}

/// Handle to the LWLocks `GetRunningTransactionData` (`procarray.c`) holds
/// while its caller's callback runs: the C contract "returns with
/// ProcArrayLock and XidGenLock held" becomes a with-locks callback shape.
/// The owner releases every lock still held when the callback returns —
/// success and error path alike — so no lock is ever held across `?` without
/// a guard.
pub trait RunningTransactionLocksHeld {
    /// `LWLockRelease(ProcArrayLock)` before the callback finishes — the
    /// hot-standby (`wal_level < logical`) path in `LogStandbySnapshot`.
    /// `Err` carries the C `elog(ERROR, "lock ... is not held")`.
    fn release_proc_array_lock(&mut self) -> types_error::PgResult<()>;
}

/// `xl_standby_lock` (`storage/standbydefs.h`): one logged
/// AccessExclusiveLock — 12 bytes, no padding.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct xl_standby_lock {
    /// xid of the holding transaction.
    pub xid: TransactionId,
    /// `InvalidOid` when locking a shared relation.
    pub dbOid: Oid,
    pub relOid: Oid,
}

// ---------------------------------------------------------------------------
// `storage/sinval.h`: the shared-invalidation message union as a Rust enum.
// ---------------------------------------------------------------------------

/// `sizeof(SharedInvalidationMessage)` — the C union is 16 bytes.
pub const SHARED_INVALIDATION_MESSAGE_SIZE: usize = 16;

/// `SHAREDINVALCATALOG_ID` (`storage/sinval.h`).
pub const SHAREDINVALCATALOG_ID: i8 = -1;
/// `SHAREDINVALRELCACHE_ID` (`storage/sinval.h`).
pub const SHAREDINVALRELCACHE_ID: i8 = -2;
/// `SHAREDINVALSMGR_ID` (`storage/sinval.h`).
pub const SHAREDINVALSMGR_ID: i8 = -3;
/// `SHAREDINVALRELMAP_ID` (`storage/sinval.h`).
pub const SHAREDINVALRELMAP_ID: i8 = -4;
/// `SHAREDINVALSNAPSHOT_ID` (`storage/sinval.h`).
pub const SHAREDINVALSNAPSHOT_ID: i8 = -5;
/// `SHAREDINVALRELSYNC_ID` (`storage/sinval.h`).
pub const SHAREDINVALRELSYNC_ID: i8 = -6;

/// `SharedInvalCatcacheMsg` — invalidate one catcache tuple. A zero-or-positive
/// `id` is both the discriminator and the catcache id.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SharedInvalCatcacheMsg {
    /// Cache ID (>= 0).
    pub id: i8,
    /// Database ID, or 0 if a shared relation.
    pub dbId: Oid,
    /// Hash value of the key for this catcache.
    pub hashValue: uint32,
}

/// `SharedInvalCatalogMsg` — invalidate all catcache entries from a catalog.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SharedInvalCatalogMsg {
    /// Database ID, or 0 if a shared catalog.
    pub dbId: Oid,
    /// ID of the catalog whose contents are invalid.
    pub catId: Oid,
}

/// `SharedInvalRelcacheMsg` — invalidate a relcache entry.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SharedInvalRelcacheMsg {
    /// Database ID, or 0 if a shared relation.
    pub dbId: Oid,
    /// Relation ID, or 0 for the whole relcache.
    pub relId: Oid,
}

/// `SharedInvalSmgrMsg` — invalidate an smgr cache entry. Field layout chosen
/// in C to pack into 16 bytes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SharedInvalSmgrMsg {
    /// High bits of the backend procno, if a temp relation.
    pub backend_hi: i8,
    /// Low bits of the backend procno, if a temp relation.
    pub backend_lo: uint16,
    /// spcOid, dbOid, relNumber.
    pub rlocator: RelFileLocator,
}

/// `SharedInvalRelmapMsg` — invalidate the mapped-relation mapping of a
/// database.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SharedInvalRelmapMsg {
    /// Database ID, or 0 for shared catalogs.
    pub dbId: Oid,
}

/// `SharedInvalSnapshotMsg` — invalidate saved snapshots that might scan a
/// relation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SharedInvalSnapshotMsg {
    /// Database ID, or 0 if a shared relation.
    pub dbId: Oid,
    /// Relation ID.
    pub relId: Oid,
}

/// `SharedInvalRelSyncMsg` — invalidate a RelationSyncCache entry.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SharedInvalRelSyncMsg {
    /// Database ID.
    pub dbId: Oid,
    /// Relation ID, or 0 for the whole RelationSyncCache.
    pub relid: Oid,
}

/// `SharedInvalidationMessage` (`storage/sinval.h`) — the C union of message
/// variants, discriminated by the first `int8` field (zero or positive =
/// catcache message, negative = the `SHAREDINVAL*_ID` codes).
///
/// The WAL/sinval-queue representation is the 16-byte C union image;
/// [`Self::to_wire_bytes`] / [`Self::from_wire_bytes`] convert (native
/// endianness, matching the C in-memory layout).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SharedInvalidationMessage {
    Catcache(SharedInvalCatcacheMsg),
    Catalog(SharedInvalCatalogMsg),
    Relcache(SharedInvalRelcacheMsg),
    Smgr(SharedInvalSmgrMsg),
    Relmap(SharedInvalRelmapMsg),
    Snapshot(SharedInvalSnapshotMsg),
    RelSync(SharedInvalRelSyncMsg),
}

impl SharedInvalidationMessage {
    /// Serialize as the 16-byte C union image. C padding bytes are zero.
    pub fn to_wire_bytes(&self) -> [u8; SHARED_INVALIDATION_MESSAGE_SIZE] {
        let mut raw = [0u8; SHARED_INVALIDATION_MESSAGE_SIZE];
        match *self {
            Self::Catcache(m) => {
                debug_assert!(m.id >= 0);
                raw[0] = m.id as u8;
                raw[4..8].copy_from_slice(&m.dbId.to_ne_bytes());
                raw[8..12].copy_from_slice(&m.hashValue.to_ne_bytes());
            }
            Self::Catalog(m) => {
                raw[0] = SHAREDINVALCATALOG_ID as u8;
                raw[4..8].copy_from_slice(&m.dbId.to_ne_bytes());
                raw[8..12].copy_from_slice(&m.catId.to_ne_bytes());
            }
            Self::Relcache(m) => {
                raw[0] = SHAREDINVALRELCACHE_ID as u8;
                raw[4..8].copy_from_slice(&m.dbId.to_ne_bytes());
                raw[8..12].copy_from_slice(&m.relId.to_ne_bytes());
            }
            Self::Smgr(m) => {
                raw[0] = SHAREDINVALSMGR_ID as u8;
                raw[1] = m.backend_hi as u8;
                raw[2..4].copy_from_slice(&m.backend_lo.to_ne_bytes());
                raw[4..8].copy_from_slice(&m.rlocator.spcOid.to_ne_bytes());
                raw[8..12].copy_from_slice(&m.rlocator.dbOid.to_ne_bytes());
                raw[12..16].copy_from_slice(&m.rlocator.relNumber.to_ne_bytes());
            }
            Self::Relmap(m) => {
                raw[0] = SHAREDINVALRELMAP_ID as u8;
                raw[4..8].copy_from_slice(&m.dbId.to_ne_bytes());
            }
            Self::Snapshot(m) => {
                raw[0] = SHAREDINVALSNAPSHOT_ID as u8;
                raw[4..8].copy_from_slice(&m.dbId.to_ne_bytes());
                raw[8..12].copy_from_slice(&m.relId.to_ne_bytes());
            }
            Self::RelSync(m) => {
                raw[0] = SHAREDINVALRELSYNC_ID as u8;
                raw[4..8].copy_from_slice(&m.dbId.to_ne_bytes());
                raw[8..12].copy_from_slice(&m.relid.to_ne_bytes());
            }
        }
        raw
    }

    /// Decode a 16-byte C union image; `None` for an unrecognized type code
    /// (C reaches the same state via `elog(FATAL, "unrecognized SI message
    /// ID")` when the message is eventually processed).
    pub fn from_wire_bytes(raw: [u8; SHARED_INVALIDATION_MESSAGE_SIZE]) -> Option<Self> {
        #[inline]
        fn u32_at(raw: &[u8], off: usize) -> u32 {
            u32::from_ne_bytes(raw[off..off + 4].try_into().expect("4-byte slice"))
        }
        let id = raw[0] as i8;
        if id >= 0 {
            return Some(Self::Catcache(SharedInvalCatcacheMsg {
                id,
                dbId: u32_at(&raw, 4),
                hashValue: u32_at(&raw, 8),
            }));
        }
        match id {
            SHAREDINVALCATALOG_ID => Some(Self::Catalog(SharedInvalCatalogMsg {
                dbId: u32_at(&raw, 4),
                catId: u32_at(&raw, 8),
            })),
            SHAREDINVALRELCACHE_ID => Some(Self::Relcache(SharedInvalRelcacheMsg {
                dbId: u32_at(&raw, 4),
                relId: u32_at(&raw, 8),
            })),
            SHAREDINVALSMGR_ID => Some(Self::Smgr(SharedInvalSmgrMsg {
                backend_hi: raw[1] as i8,
                backend_lo: u16::from_ne_bytes(raw[2..4].try_into().expect("2-byte slice")),
                rlocator: RelFileLocator {
                    spcOid: u32_at(&raw, 4),
                    dbOid: u32_at(&raw, 8),
                    relNumber: u32_at(&raw, 12),
                },
            })),
            SHAREDINVALRELMAP_ID => Some(Self::Relmap(SharedInvalRelmapMsg {
                dbId: u32_at(&raw, 4),
            })),
            SHAREDINVALSNAPSHOT_ID => Some(Self::Snapshot(SharedInvalSnapshotMsg {
                dbId: u32_at(&raw, 4),
                relId: u32_at(&raw, 8),
            })),
            SHAREDINVALRELSYNC_ID => Some(Self::RelSync(SharedInvalRelSyncMsg {
                dbId: u32_at(&raw, 4),
                relid: u32_at(&raw, 8),
            })),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// `storage/relfilelocator.h`.
// ---------------------------------------------------------------------------

/// `RelFileLocator` (`storage/relfilelocator.h`).
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RelFileLocator {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: RelFileNumber,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sinval_wire_roundtrip_every_variant() {
        let msgs = [
            SharedInvalidationMessage::Catcache(SharedInvalCatcacheMsg {
                id: 7,
                dbId: 5,
                hashValue: 0xDEAD_BEEF,
            }),
            SharedInvalidationMessage::Catalog(SharedInvalCatalogMsg { dbId: 5, catId: 1259 }),
            SharedInvalidationMessage::Relcache(SharedInvalRelcacheMsg { dbId: 5, relId: 16384 }),
            SharedInvalidationMessage::Smgr(SharedInvalSmgrMsg {
                backend_hi: -1,
                backend_lo: 0xFFFF,
                rlocator: RelFileLocator { spcOid: 1663, dbOid: 5, relNumber: 16384 },
            }),
            SharedInvalidationMessage::Relmap(SharedInvalRelmapMsg { dbId: 0 }),
            SharedInvalidationMessage::Snapshot(SharedInvalSnapshotMsg { dbId: 5, relId: 1259 }),
            SharedInvalidationMessage::RelSync(SharedInvalRelSyncMsg { dbId: 5, relid: 16384 }),
        ];
        for msg in msgs {
            let raw = msg.to_wire_bytes();
            assert_eq!(SharedInvalidationMessage::from_wire_bytes(raw), Some(msg));
        }
    }

    #[test]
    fn sinval_wire_layout_matches_c_union() {
        // SharedInvalCatcacheMsg { int8 id; Oid dbId; uint32 hashValue; }:
        // id at 0, dbId at 4 (after alignment padding), hashValue at 8.
        let raw = SharedInvalidationMessage::Catcache(SharedInvalCatcacheMsg {
            id: 41,
            dbId: 0x0102_0304,
            hashValue: 0x0506_0708,
        })
        .to_wire_bytes();
        assert_eq!(raw[0], 41);
        assert_eq!(u32::from_ne_bytes(raw[4..8].try_into().unwrap()), 0x0102_0304);
        assert_eq!(u32::from_ne_bytes(raw[8..12].try_into().unwrap()), 0x0506_0708);
        assert_eq!(&raw[12..16], &[0; 4]);

        // SharedInvalSmgrMsg packs into all 16 bytes: id, backend_hi,
        // backend_lo, then the 12-byte RelFileLocator at offset 4.
        let raw = SharedInvalidationMessage::Smgr(SharedInvalSmgrMsg {
            backend_hi: 1,
            backend_lo: 2,
            rlocator: RelFileLocator { spcOid: 3, dbOid: 4, relNumber: 5 },
        })
        .to_wire_bytes();
        assert_eq!(raw[0] as i8, SHAREDINVALSMGR_ID);
        assert_eq!(raw[1] as i8, 1);
        assert_eq!(u16::from_ne_bytes(raw[2..4].try_into().unwrap()), 2);
        assert_eq!(u32::from_ne_bytes(raw[4..8].try_into().unwrap()), 3);
        assert_eq!(u32::from_ne_bytes(raw[8..12].try_into().unwrap()), 4);
        assert_eq!(u32::from_ne_bytes(raw[12..16].try_into().unwrap()), 5);
    }

    #[test]
    fn sinval_unknown_id_decodes_to_none() {
        let mut raw = [0u8; SHARED_INVALIDATION_MESSAGE_SIZE];
        raw[0] = -7i8 as u8;
        assert_eq!(SharedInvalidationMessage::from_wire_bytes(raw), None);
    }
}
