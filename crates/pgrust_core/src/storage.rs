use serde::{Deserialize, Serialize};

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
