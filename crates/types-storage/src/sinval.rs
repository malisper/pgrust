//! Shared cache invalidation messages (`storage/sinval.h`).
//!
//! C spells this as a 16-byte tagged union discriminated by the leading
//! `int8 id` (zero or positive = a catcache message whose id IS the catcache
//! id; negative values select the other variants). Here it is a real enum;
//! the WAL byte form (the raw union, padding zeroed) is produced/consumed
//! only at the XLog boundary via [`SharedInvalidationMessage::to_wal_bytes`]
//! and [`SharedInvalidationMessage::from_wal_bytes`].

use crate::relfilelocator::RelFileLocator;
use types_core::primitive::Oid;

/// `sizeof(SharedInvalidationMessage)`: the union packs into 16 bytes
/// (`SharedInvalSmgrMsg`'s comment in sinval.h documents the layout choice).
pub const SHARED_INVAL_MESSAGE_SIZE: usize = 16;

pub const SHAREDINVALCATALOG_ID: i8 = -1;
pub const SHAREDINVALRELCACHE_ID: i8 = -2;
pub const SHAREDINVALSMGR_ID: i8 = -3;
pub const SHAREDINVALRELMAP_ID: i8 = -4;
pub const SHAREDINVALSNAPSHOT_ID: i8 = -5;
pub const SHAREDINVALRELSYNC_ID: i8 = -6;

/// `SharedInvalidationMessage` (`storage/sinval.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SharedInvalidationMessage {
    /// `SharedInvalCatcacheMsg` — `id >= 0` (the id is the catcache id).
    Catcache { id: i8, db_id: Oid, hash_value: u32 },
    /// `SharedInvalCatalogMsg` (`SHAREDINVALCATALOG_ID`).
    Catalog { db_id: Oid, cat_id: Oid },
    /// `SharedInvalRelcacheMsg` (`SHAREDINVALRELCACHE_ID`).
    Relcache { db_id: Oid, rel_id: Oid },
    /// `SharedInvalSmgrMsg` (`SHAREDINVALSMGR_ID`).
    Smgr {
        backend_hi: i8,
        backend_lo: u16,
        rlocator: RelFileLocator,
    },
    /// `SharedInvalRelmapMsg` (`SHAREDINVALRELMAP_ID`).
    Relmap { db_id: Oid },
    /// `SharedInvalSnapshotMsg` (`SHAREDINVALSNAPSHOT_ID`).
    Snapshot { db_id: Oid, rel_id: Oid },
    /// `SharedInvalRelSyncMsg` (`SHAREDINVALRELSYNC_ID`).
    RelSync { db_id: Oid, rel_id: Oid },
}

impl SharedInvalidationMessage {
    /// The leading `int8 id` discriminator.
    pub const fn id(&self) -> i8 {
        match self {
            Self::Catcache { id, .. } => *id,
            Self::Catalog { .. } => SHAREDINVALCATALOG_ID,
            Self::Relcache { .. } => SHAREDINVALRELCACHE_ID,
            Self::Smgr { .. } => SHAREDINVALSMGR_ID,
            Self::Relmap { .. } => SHAREDINVALRELMAP_ID,
            Self::Snapshot { .. } => SHAREDINVALSNAPSHOT_ID,
            Self::RelSync { .. } => SHAREDINVALRELSYNC_ID,
        }
    }

    /// Serialize to the raw C-union form carried in WAL records (id at
    /// offset 0, payload fields at their C offsets, padding zeroed).
    pub fn to_wal_bytes(&self) -> [u8; SHARED_INVAL_MESSAGE_SIZE] {
        let mut b = [0u8; SHARED_INVAL_MESSAGE_SIZE];
        b[0] = self.id() as u8;
        match *self {
            Self::Catcache {
                db_id, hash_value, ..
            } => {
                b[4..8].copy_from_slice(&db_id.to_ne_bytes());
                b[8..12].copy_from_slice(&hash_value.to_ne_bytes());
            }
            Self::Catalog { db_id, cat_id } => {
                b[4..8].copy_from_slice(&db_id.to_ne_bytes());
                b[8..12].copy_from_slice(&cat_id.to_ne_bytes());
            }
            Self::Relcache { db_id, rel_id }
            | Self::Snapshot { db_id, rel_id }
            | Self::RelSync { db_id, rel_id } => {
                b[4..8].copy_from_slice(&db_id.to_ne_bytes());
                b[8..12].copy_from_slice(&rel_id.to_ne_bytes());
            }
            Self::Smgr {
                backend_hi,
                backend_lo,
                rlocator,
            } => {
                b[1] = backend_hi as u8;
                b[2..4].copy_from_slice(&backend_lo.to_ne_bytes());
                b[4..8].copy_from_slice(&rlocator.spc_oid.to_ne_bytes());
                b[8..12].copy_from_slice(&rlocator.db_oid.to_ne_bytes());
                b[12..16].copy_from_slice(&rlocator.rel_number.to_ne_bytes());
            }
            Self::Relmap { db_id } => {
                b[4..8].copy_from_slice(&db_id.to_ne_bytes());
            }
        }
        b
    }

    /// Decode the raw C-union form; `None` on an id no variant claims.
    pub fn from_wal_bytes(b: &[u8; SHARED_INVAL_MESSAGE_SIZE]) -> Option<Self> {
        let id = b[0] as i8;
        let u32_at = |off: usize| u32::from_ne_bytes(b[off..off + 4].try_into().unwrap());
        Some(match id {
            0.. => Self::Catcache {
                id,
                db_id: u32_at(4),
                hash_value: u32_at(8),
            },
            SHAREDINVALCATALOG_ID => Self::Catalog {
                db_id: u32_at(4),
                cat_id: u32_at(8),
            },
            SHAREDINVALRELCACHE_ID => Self::Relcache {
                db_id: u32_at(4),
                rel_id: u32_at(8),
            },
            SHAREDINVALSMGR_ID => Self::Smgr {
                backend_hi: b[1] as i8,
                backend_lo: u16::from_ne_bytes(b[2..4].try_into().unwrap()),
                rlocator: RelFileLocator::new(u32_at(4), u32_at(8), u32_at(12)),
            },
            SHAREDINVALRELMAP_ID => Self::Relmap { db_id: u32_at(4) },
            SHAREDINVALSNAPSHOT_ID => Self::Snapshot {
                db_id: u32_at(4),
                rel_id: u32_at(8),
            },
            SHAREDINVALRELSYNC_ID => Self::RelSync {
                db_id: u32_at(4),
                rel_id: u32_at(8),
            },
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_bytes_round_trip() {
        let msgs = [
            SharedInvalidationMessage::Catcache {
                id: 41,
                db_id: 5,
                hash_value: 0xdead_beef,
            },
            SharedInvalidationMessage::Catalog { db_id: 5, cat_id: 1259 },
            SharedInvalidationMessage::Relcache { db_id: 5, rel_id: 16384 },
            SharedInvalidationMessage::Smgr {
                backend_hi: 1,
                backend_lo: 7,
                rlocator: RelFileLocator::new(1663, 5, 16385),
            },
            SharedInvalidationMessage::Relmap { db_id: 0 },
            SharedInvalidationMessage::Snapshot { db_id: 5, rel_id: 2619 },
            SharedInvalidationMessage::RelSync { db_id: 5, rel_id: 0 },
        ];
        for msg in msgs {
            let bytes = msg.to_wal_bytes();
            assert_eq!(SharedInvalidationMessage::from_wal_bytes(&bytes), Some(msg));
        }
    }

    #[test]
    fn unknown_id_is_rejected() {
        let mut b = [0u8; SHARED_INVAL_MESSAGE_SIZE];
        b[0] = (-7i8) as u8;
        assert_eq!(SharedInvalidationMessage::from_wal_bytes(&b), None);
    }
}
