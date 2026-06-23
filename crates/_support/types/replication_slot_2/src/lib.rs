//! Replication-slot data vocabulary (`replication/slot.h` + the `slot.c`
//! file-private on-disk format).
//!
//! These are the persistent / on-disk shapes that `slot.c` shares with its
//! consumers (`slotfuncs`, `slotsync`, `pgstat_replslot`). The live
//! shared-memory `ReplicationSlot` struct (with its embedded spinlock,
//! `io_in_progress_lock` LWLock, and `active_cv` condition variable) is owned
//! by the `backend-replication-slot` crate, not here, because those embedded
//! primitives are real lock types rather than plain data.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use ::types_core::{Oid, TransactionId, XLogRecPtr, NAMEDATALEN};
use ::types_tuple::heaptuple::NameData;

/// `PG_REPLSLOT_DIR` (slot.h) — directory to store replication slot data in.
pub const PG_REPLSLOT_DIR: &str = "pg_replslot";

/// `ReplicationSlot *` identified to the `slot.c` owner by its index into
/// `ReplicationSlotCtl->replication_slots[]`. The live struct embeds real lock
/// primitives and is owner-private; non-`MyReplicationSlot` slots (e.g. the
/// `slotsync` array scan in `get_local_synced_slots`) are reached through the
/// owner's by-handle seams, which map the index back to `&replication_slots[i]`.
/// `NONE` is the `NULL` sentinel (no in-range index equals it).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub struct ReplicationSlotHandle(pub i32);

impl ReplicationSlotHandle {
    /// Sentinel for a `NULL` `ReplicationSlot *`.
    pub const NONE: ReplicationSlotHandle = ReplicationSlotHandle(-1);

    /// `slot == NULL`.
    #[inline]
    pub fn is_none(self) -> bool {
        self == ReplicationSlotHandle::NONE
    }
}

/// `ReplicationSlotPersistency` (slot.h) — behaviour of replication slots upon
/// release or crash.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicationSlotPersistency {
    RS_PERSISTENT = 0,
    RS_EPHEMERAL = 1,
    RS_TEMPORARY = 2,
}

/// `ReplicationSlotInvalidationCause` (slot.h) — reason a slot was invalidated.
///
/// The non-`NONE` values are powers of two so they can be combined / tested
/// with bitwise operations (see `possible_causes` in
/// `InvalidatePossiblyObsoleteSlot`).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicationSlotInvalidationCause {
    RS_INVAL_NONE = 0,
    /// required WAL has been removed
    RS_INVAL_WAL_REMOVED = 1 << 0,
    /// required rows have been removed
    RS_INVAL_HORIZON = 1 << 1,
    /// wal_level insufficient for slot
    RS_INVAL_WAL_LEVEL = 1 << 2,
    /// idle slot timeout has occurred
    RS_INVAL_IDLE_TIMEOUT = 1 << 3,
}

/// `RS_INVAL_MAX_CAUSES` (slot.h) — maximum number of invalidation causes.
pub const RS_INVAL_MAX_CAUSES: usize = 4;

/// `ReplicationSlotPersistentData` (slot.h) — on-disk data of a replication
/// slot, preserved across restarts.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ReplicationSlotPersistentData {
    /// The slot's identifier.
    pub name: NameData,
    /// database the slot is active on.
    pub database: Oid,
    /// The slot's behaviour when being dropped (or restored after a crash).
    pub persistency: ReplicationSlotPersistency,
    /// xmin horizon for data.
    pub xmin: TransactionId,
    /// xmin horizon for catalog tuples.
    pub catalog_xmin: TransactionId,
    /// oldest LSN that might be required by this replication slot.
    pub restart_lsn: XLogRecPtr,
    /// `RS_INVAL_NONE` if valid, or the reason for having been invalidated.
    pub invalidated: ReplicationSlotInvalidationCause,
    /// Oldest LSN that the client has acked receipt for.
    pub confirmed_flush: XLogRecPtr,
    /// LSN at which two_phase commit was enabled / consistent point found.
    pub two_phase_at: XLogRecPtr,
    /// Allow decoding of prepared transactions?
    pub two_phase: bool,
    /// plugin name.
    pub plugin: NameData,
    /// Was this slot synchronized from the primary server? (C `char`).
    pub synced: i8,
    /// Is this a failover slot (sync candidate for standbys)?
    pub failover: bool,
}

impl Default for ReplicationSlotPersistentData {
    fn default() -> Self {
        // The C code `memset`s the whole struct to zero before filling fields.
        Self {
            name: NameData {
                data: [0u8; NAMEDATALEN as usize],
            },
            database: 0,
            persistency: ReplicationSlotPersistency::RS_PERSISTENT,
            xmin: 0,
            catalog_xmin: 0,
            restart_lsn: 0,
            invalidated: ReplicationSlotInvalidationCause::RS_INVAL_NONE,
            confirmed_flush: 0,
            two_phase_at: 0,
            two_phase: false,
            plugin: NameData {
                data: [0u8; NAMEDATALEN as usize],
            },
            synced: 0,
            failover: false,
        }
    }
}

/// `ReplicationSlotOnDisk` (slot.c, file-private) — replication slot on-disk
/// data structure. The leading `magic`/`checksum` are not covered by the
/// checksum; `version`, `length`, and the embedded `slotdata` are.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ReplicationSlotOnDisk {
    /// format identifier (not checksummed).
    pub magic: u32,
    /// `pg_crc32c` checksum of the data that follows `version` (not checksummed).
    pub checksum: u32,
    /// on-disk format version.
    pub version: u32,
    /// length of the version-dependent data that follows.
    pub length: u32,
    /// the actual version-dependent slot data.
    pub slotdata: ReplicationSlotPersistentData,
}

impl Default for ReplicationSlotOnDisk {
    fn default() -> Self {
        Self {
            magic: 0,
            checksum: 0,
            version: 0,
            length: 0,
            slotdata: ReplicationSlotPersistentData::default(),
        }
    }
}

/// `SlotInvalidationCauseMap` (slot.c, file-private) — one row of the
/// invalidation-cause lookup table mapping a cause to its name.
#[derive(Clone, Copy, Debug)]
pub struct SlotInvalidationCauseMap {
    pub cause: ReplicationSlotInvalidationCause,
    pub cause_name: &'static str,
}

/// `SlotIsPhysical(slot)` — `slot->data.database == InvalidOid`.
#[inline]
pub fn slot_is_physical(data: &ReplicationSlotPersistentData) -> bool {
    data.database == ::types_core::InvalidOid
}

/// `SlotIsLogical(slot)` — `slot->data.database != InvalidOid`.
#[inline]
pub fn slot_is_logical(data: &ReplicationSlotPersistentData) -> bool {
    data.database != ::types_core::InvalidOid
}
