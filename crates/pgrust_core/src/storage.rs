use serde::{Deserialize, Serialize};

pub const BLCKSZ: usize = 8192;
pub const PAGE_SIZE: usize = BLCKSZ;
pub const ITEM_ID_SIZE: usize = 4;
pub const SIZE_OF_PAGE_HEADER_DATA: usize = 24;
pub const MAXALIGN: usize = 8;
pub const RELSEG_SIZE: u32 = 131_072;
pub const INVALID_BLOCK_NUMBER: u32 = u32::MAX;
pub const MAX_IO_COMBINE_LIMIT: u32 = 64;

pub type Page = [u8; PAGE_SIZE];
pub type ClientId = u32;
pub type BufferId = usize;
pub type BlockNumber = u32;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ForkNumber {
    Main,
    Fsm,
    VisibilityMap,
    Init,
    Other(u8),
}

impl ForkNumber {
    pub fn from_u8(n: u8) -> Self {
        match n {
            0 => Self::Main,
            1 => Self::Fsm,
            2 => Self::VisibilityMap,
            3 => Self::Init,
            other => Self::Other(other),
        }
    }

    pub fn as_u8(self) -> u8 {
        match self {
            Self::Main => 0,
            Self::Fsm => 1,
            Self::VisibilityMap => 2,
            Self::Init => 3,
            Self::Other(n) => n,
        }
    }

    pub fn suffix(self) -> String {
        match self {
            Self::Main => String::new(),
            Self::Fsm => "_fsm".to_string(),
            Self::VisibilityMap => "_vm".to_string(),
            Self::Init => "_init".to_string(),
            Self::Other(n) => format!("_fork{}", n),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BufferTag {
    pub rel: RelFileLocator,
    pub fork: ForkNumber,
    pub block: BlockNumber,
}
