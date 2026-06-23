//! GiST rmgr WAL record bodies (`access/gistxlog.h`), trimmed to the fields
//! ports consume so far.

use crate::bytes::{bool_at, full_xid_at, locator_at, u16_at, u32_at};
use types_core::{BlockNumber, FullTransactionId, OffsetNumber, TransactionId};
use types_storage::RelFileLocator;

/// `gistxlogPageReuse`: `{RelFileLocator locator; BlockNumber block;
/// FullTransactionId snapshotConflictHorizon /*8-aligned at 16*/;
/// bool isCatalogRel;}`.
#[derive(Clone, Copy, Debug)]
pub struct gistxlogPageReuse {
    pub locator: RelFileLocator,
    pub block: BlockNumber,
    pub snapshotConflictHorizon: FullTransactionId,
    pub isCatalogRel: bool,
}

impl gistxlogPageReuse {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            locator: locator_at(rec, 0),
            block: u32_at(rec, 12),
            snapshotConflictHorizon: full_xid_at(rec, 16),
            isCatalogRel: bool_at(rec, 24),
        }
    }
}

/// `gistxlogDelete`: `{TransactionId snapshotConflictHorizon;
/// uint16 ntodelete; bool isCatalogRel; OffsetNumber offsets[];}` — trimmed
/// of the trailing offsets.
#[derive(Clone, Copy, Debug)]
pub struct gistxlogDelete {
    pub snapshotConflictHorizon: TransactionId,
    pub ntodelete: u16,
    pub isCatalogRel: bool,
}

impl gistxlogDelete {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            snapshotConflictHorizon: u32_at(rec, 0),
            ntodelete: u16_at(rec, 4),
            isCatalogRel: bool_at(rec, 6),
        }
    }
}

/// `gistxlogPageSplit`: trimmed to `npage`; layout
/// `{BlockNumber origrlink /*0*/; GistNSN orignsn /*8-aligned at 8*/;
/// bool origleaf /*16*/; uint16 npage /*18*/; bool markfollowright /*20*/;}`.
#[derive(Clone, Copy, Debug)]
pub struct gistxlogPageSplit {
    pub npage: u16,
}

impl gistxlogPageSplit {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { npage: u16_at(rec, 18) }
    }
}

/// `gistxlogPageDelete`: `{FullTransactionId deleteXid;
/// OffsetNumber downlinkOffset;}`.
#[derive(Clone, Copy, Debug)]
pub struct gistxlogPageDelete {
    pub deleteXid: FullTransactionId,
    pub downlinkOffset: OffsetNumber,
}

impl gistxlogPageDelete {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            deleteXid: full_xid_at(rec, 0),
            downlinkOffset: u16_at(rec, 8),
        }
    }
}
