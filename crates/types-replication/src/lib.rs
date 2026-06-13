//! Replication vocabulary shared across the slot-synchronization / walreceiver
//! ports (`replication/slot.h`, `replication/walreceiver.h`).
//!
//! Trimmed to the items the slotsync port consumes. The `ReplicationSlot`,
//! `WalReceiverConn`, `WalRcvExecResult`, and `TupleTableSlot` objects are
//! shared-memory / receiver-owned structures the slotsync unit does not own;
//! it reaches them only through the owner's seams, which take the slot/conn
//! identity as an explicit parameter. They are modeled here as `Copy` handle
//! newtypes — seam-marshaling vocabulary only — matching the established
//! precedent for the genuinely owner-resident `WaitEventSet` handle
//! (`types-storage`). The enums and constants C spells out are real Rust
//! types, with values verified against the headers.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use types_core::primitive::Oid;

// ---------------------------------------------------------------------------
// Opaque owner-resident handles (carried by value through the owner seams; the
// owner maps each handle to the live object when it installs the seam). `0` is
// the null sentinel.
// ---------------------------------------------------------------------------

/// `ReplicationSlot *` (`replication/slot.h`) — a shared-memory replication
/// slot, identified to the slot.c owner by this handle. NOT the executor
/// `TupleTableSlot`.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub struct ReplicationSlotHandle(pub u64);

impl ReplicationSlotHandle {
    /// Sentinel for a `NULL` `ReplicationSlot *`.
    pub const NONE: ReplicationSlotHandle = ReplicationSlotHandle(0);

    /// `slot == NULL`.
    #[inline]
    pub fn is_none(self) -> bool {
        self == ReplicationSlotHandle::NONE
    }
}

/// `WalReceiverConn *` (`replication/walreceiver.h`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub struct WrConnHandle(pub u64);

impl WrConnHandle {
    /// Sentinel for a `NULL` `WalReceiverConn *` (failed `walrcv_connect`).
    pub const NONE: WrConnHandle = WrConnHandle(0);

    /// `wrconn == NULL`.
    #[inline]
    pub fn is_none(self) -> bool {
        self == WrConnHandle::NONE
    }
}

/// `WalRcvExecResult *` (`replication/walreceiver.h`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub struct WalRcvExecResultHandle(pub u64);

/// `TupleTableSlot *` (`executor/tuptable.h`), as produced by `walrcv_exec`'s
/// result tuplestore.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub struct TupleTableSlotHandle(pub u64);

// ---------------------------------------------------------------------------
// ReplicationSlotPersistency (replication/slot.h).
// ---------------------------------------------------------------------------

/// `typedef enum ReplicationSlotPersistency` (`replication/slot.h`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
#[repr(i32)]
pub enum ReplicationSlotPersistency {
    RS_PERSISTENT = 0,
    RS_EPHEMERAL = 1,
    RS_TEMPORARY = 2,
}
pub use ReplicationSlotPersistency::*;

// ---------------------------------------------------------------------------
// ReplicationSlotInvalidationCause (replication/slot.h).
// ---------------------------------------------------------------------------

/// `typedef enum ReplicationSlotInvalidationCause` (`replication/slot.h`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
#[repr(i32)]
pub enum ReplicationSlotInvalidationCause {
    /// Slot is valid.
    RS_INVAL_NONE = 0,
    /// Required WAL has been removed.
    RS_INVAL_WAL_REMOVED = 1 << 0,
    /// Required rows have been removed.
    RS_INVAL_HORIZON = 1 << 1,
    /// `wal_level` insufficient for slot.
    RS_INVAL_WAL_LEVEL = 1 << 2,
    /// Idle slot timeout has occurred.
    RS_INVAL_IDLE_TIMEOUT = 1 << 3,
}
pub use ReplicationSlotInvalidationCause::*;

/// `RS_INVAL_MAX_CAUSES` (`replication/slot.h`).
pub const RS_INVAL_MAX_CAUSES: i32 = 4;

// ---------------------------------------------------------------------------
// WalRcvExecStatus (replication/walreceiver.h).
// ---------------------------------------------------------------------------

/// `typedef enum WalRcvExecStatus` (`replication/walreceiver.h`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
#[repr(i32)]
pub enum WalRcvExecStatus {
    /// There was an error when executing the query.
    WALRCV_ERROR = 0,
    /// Query executed a utility or replication command.
    WALRCV_OK_COMMAND = 1,
    /// Query returned tuples.
    WALRCV_OK_TUPLES = 2,
    /// Query started COPY FROM.
    WALRCV_OK_COPY_IN = 3,
    /// Query started COPY TO.
    WALRCV_OK_COPY_OUT = 4,
    /// Query started COPY BOTH replication protocol.
    WALRCV_OK_COPY_BOTH = 5,
}
pub use WalRcvExecStatus::*;

// ---------------------------------------------------------------------------
// Catalog OID used by the slot-drop conflict lock (catalog/pg_database_d.h).
// ---------------------------------------------------------------------------

/// `DatabaseRelationId` — `pg_database` OID (`catalog/pg_database_d.h` = 1262).
pub const DatabaseRelationId: Oid = 1262;
