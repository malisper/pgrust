//! Shared-invalidation message vocabulary (`storage/sinval.h`). C's
//! `SharedInvalidationMessage` is a 16-byte union dispatched on its leading
//! `int8 id`; here it is the equivalent Rust enum, with wire-image
//! conversions for the WAL/sinval-queue representation.

use ::types_core::{uint16, uint32, Oid};

use crate::storage::RelFileLocator;

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

/// Alias kept for the rmgrdesc consumers.
pub const SIZEOF_SHARED_INVALIDATION_MESSAGE: usize = SHARED_INVALIDATION_MESSAGE_SIZE;

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

    /// Decode message `i` (`msgs[i]` in C); `None` for an unrecognized id.
    /// Panics past the end of the bytes, where C would read garbage.
    /// The raw leading `int8 id` of message `i` (for "unrecognized" prints).
    pub fn raw_id(&self, i: usize) -> i8 {
        self.bytes[i * SHARED_INVALIDATION_MESSAGE_SIZE] as i8
    }

    pub fn get(&self, i: usize) -> Option<SharedInvalidationMessage> {
        let off = i * SHARED_INVALIDATION_MESSAGE_SIZE;
        let raw: [u8; SHARED_INVALIDATION_MESSAGE_SIZE] = self.bytes
            [off..off + SHARED_INVALIDATION_MESSAGE_SIZE]
            .try_into()
            .expect("SharedInvalidationMessage bytes shorter than the union");
        SharedInvalidationMessage::from_wire_bytes(raw)
    }
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
