use serde::{Deserialize, Serialize};

use crate::storage::OffsetNumber;

pub const BRIN_DEFAULT_PAGES_PER_RANGE: u32 = 128;
pub const SIZEOF_HEAP_TUPLE_HEADER: usize = 23;
pub const SPGIST_CONFIG_PROC: i16 = 1;

const BRIN_SPECIAL_SIZE: usize = crate::storage::MAXALIGN;
const BRIN_PAGE_CONTENT_OFFSET: usize = (crate::storage::SIZE_OF_PAGE_HEADER_DATA
    + (crate::storage::MAXALIGN - 1))
    & !(crate::storage::MAXALIGN - 1);
const REVMAP_ENTRY_SIZE: usize = 6;
const REVMAP_CONTENT_SIZE: usize =
    crate::storage::BLCKSZ - BRIN_PAGE_CONTENT_OFFSET - BRIN_SPECIAL_SIZE;
pub const REVMAP_PAGE_MAXITEMS: usize = REVMAP_CONTENT_SIZE / REVMAP_ENTRY_SIZE;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash, Serialize, Deserialize,
)]
pub struct ItemPointerData {
    pub block_number: u32,
    pub offset_number: OffsetNumber,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttributeAlign {
    Char,
    Short,
    Int,
    Double,
}

impl AttributeAlign {
    pub const fn as_char(self) -> char {
        match self {
            Self::Char => 'c',
            Self::Short => 's',
            Self::Int => 'i',
            Self::Double => 'd',
        }
    }

    pub const fn from_char(value: char) -> Option<Self> {
        match value {
            'c' => Some(Self::Char),
            's' => Some(Self::Short),
            'i' => Some(Self::Int),
            'd' => Some(Self::Double),
            _ => None,
        }
    }

    pub fn align_offset(self, off: usize) -> usize {
        match self {
            Self::Char => off,
            Self::Short => (off + 1) & !1,
            Self::Int => (off + 3) & !3,
            Self::Double => (off + 7) & !7,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttributeStorage {
    Plain,
    External,
    Extended,
    Main,
}

impl AttributeStorage {
    pub const fn as_char(self) -> char {
        match self {
            Self::Plain => 'p',
            Self::External => 'e',
            Self::Extended => 'x',
            Self::Main => 'm',
        }
    }

    pub const fn from_char(value: char) -> Option<Self> {
        match value {
            'p' => Some(Self::Plain),
            'e' => Some(Self::External),
            'x' => Some(Self::Extended),
            'm' => Some(Self::Main),
            _ => None,
        }
    }
}

impl Default for AttributeStorage {
    fn default() -> Self {
        Self::Plain
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttributeCompression {
    Default,
    Pglz,
    Lz4,
}

impl AttributeCompression {
    pub const fn as_char(self) -> char {
        match self {
            Self::Default => '\0',
            Self::Pglz => 'p',
            Self::Lz4 => 'l',
        }
    }

    pub const fn from_char(value: char) -> Option<Self> {
        match value {
            '\0' => Some(Self::Default),
            'p' => Some(Self::Pglz),
            'l' => Some(Self::Lz4),
            _ => None,
        }
    }
}

impl Default for AttributeCompression {
    fn default() -> Self {
        Self::Default
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttributeDesc {
    pub name: String,
    pub attlen: i16,
    pub attalign: AttributeAlign,
    pub attstorage: AttributeStorage,
    pub attcompression: AttributeCompression,
    pub nullable: bool,
}
