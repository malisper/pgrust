//! Shared-invalidation message vocabulary (`storage/sinval.h`), trimmed to the
//! fields current ports consume. C's `SharedInvalidationMessage` is a union
//! dispatched on its leading `int8 id`; here it is the equivalent Rust enum.

use types_core::Oid;

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

/// `sizeof(SharedInvalidationMessage)` — the union packs into 16 bytes
/// (`SharedInvalSmgrMsg`: `int8` + `int8` + `uint16` + `RelFileLocator`).
pub const SIZEOF_SHARED_INVALIDATION_MESSAGE: usize = 16;

/// `SharedInvalidationMessage` (`storage/sinval.h`) — the union's members as
/// enum variants, dispatched on the leading `int8 id` exactly as C code does.
/// Each variant carries only the fields ports consume so far; `Unrecognized`
/// covers ids no union member claims (C code prints them as unrecognized).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(non_snake_case)]
pub enum SharedInvalidationMessage {
    /// `SharedInvalCatcacheMsg` — `id >= 0`; the id doubles as the catcache id.
    Catcache { id: i8 },
    /// `SharedInvalCatalogMsg` (`SHAREDINVALCATALOG_ID`).
    Catalog { catId: Oid },
    /// `SharedInvalRelcacheMsg` (`SHAREDINVALRELCACHE_ID`).
    Relcache { relId: Oid },
    /// `SharedInvalSmgrMsg` (`SHAREDINVALSMGR_ID`).
    Smgr,
    /// `SharedInvalRelmapMsg` (`SHAREDINVALRELMAP_ID`).
    Relmap { dbId: Oid },
    /// `SharedInvalSnapshotMsg` (`SHAREDINVALSNAPSHOT_ID`).
    Snapshot { relId: Oid },
    /// `SharedInvalRelSyncMsg` (`SHAREDINVALRELSYNC_ID`).
    RelSync { relid: Oid },
    /// An id outside the union's vocabulary.
    Unrecognized { id: i8 },
}

impl SharedInvalidationMessage {
    /// Decode one 16-byte message from its on-disk/WAL bytes (native layout:
    /// `int8 id` at 0; the member Oids at their C offsets). Panics if `bytes`
    /// is shorter than the fields the id requires — C reads garbage there.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let oid_at = |off: usize| -> Oid {
            Oid::from_ne_bytes(
                bytes[off..off + 4]
                    .try_into()
                    .expect("SharedInvalidationMessage bytes shorter than the union"),
            )
        };
        let id = bytes[0] as i8;
        if id >= 0 {
            Self::Catcache { id }
        } else if id == SHAREDINVALCATALOG_ID {
            // SharedInvalCatalogMsg: { int8 id; Oid dbId; Oid catId; }
            Self::Catalog { catId: oid_at(8) }
        } else if id == SHAREDINVALRELCACHE_ID {
            // SharedInvalRelcacheMsg: { int8 id; Oid dbId; Oid relId; }
            Self::Relcache { relId: oid_at(8) }
        } else if id == SHAREDINVALSMGR_ID {
            Self::Smgr
        } else if id == SHAREDINVALRELMAP_ID {
            // SharedInvalRelmapMsg: { int8 id; Oid dbId; }
            Self::Relmap { dbId: oid_at(4) }
        } else if id == SHAREDINVALSNAPSHOT_ID {
            // SharedInvalSnapshotMsg: { int8 id; Oid dbId; Oid relId; }
            Self::Snapshot { relId: oid_at(8) }
        } else if id == SHAREDINVALRELSYNC_ID {
            // SharedInvalRelSyncMsg: { int8 id; Oid dbId; Oid relid; }
            Self::RelSync { relid: oid_at(8) }
        } else {
            Self::Unrecognized { id }
        }
    }
}

/// A borrowed `SharedInvalidationMessage *` array as WAL records carry it:
/// 16-byte entries decoded on access (the bytes may be unaligned, so no
/// in-place `&[SharedInvalidationMessage]` is possible).
#[derive(Clone, Copy, Debug)]
pub struct SharedInvalMessages<'a> {
    bytes: &'a [u8],
}

impl<'a> SharedInvalMessages<'a> {
    pub const fn from_bytes(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Decode message `i` (`msgs[i]` in C). Panics past the end of the bytes,
    /// where C would read garbage.
    pub fn get(&self, i: usize) -> SharedInvalidationMessage {
        SharedInvalidationMessage::from_bytes(&self.bytes[i * SIZEOF_SHARED_INVALIDATION_MESSAGE..])
    }
}
