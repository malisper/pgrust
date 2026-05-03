use serde::{Deserialize, Serialize};

pub const BLCKSZ: usize = 8192;
pub const ITEM_ID_SIZE: usize = 4;
pub const SIZE_OF_PAGE_HEADER_DATA: usize = 24;
pub const MAXALIGN: usize = 8;

/// 1-based offset of an item within a page.
pub type OffsetNumber = u16;

/// Identifies the physical on-disk location of a relation.
///
/// In PostgreSQL this is `RelFileLocator` (struct with spcOid, dbOid,
/// relNumber).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RelFileLocator {
    pub spc_oid: u32,
    pub db_oid: u32,
    pub rel_number: u32,
}
