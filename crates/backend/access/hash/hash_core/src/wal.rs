//! Hash rmgr id (`access/rmgrlist.h`). `RM_HASH_ID` is not exported by
//! `wal` yet; the hash AM defines it locally, mirroring how
//! `backend-access-nbt-dedup` defines `RM_BTREE_ID = 11`.

use types_core::RmgrId;

/// `RM_HASH_ID` (rmgrlist.h) — the hash resource-manager id (12).
pub(crate) const RM_HASH_ID: RmgrId = 12;
