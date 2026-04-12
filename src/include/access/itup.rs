use crate::backend::storage::page::bufpage::max_align;
use crate::include::access::itemptr::ItemPointerData;

pub const INDEX_MAX_KEYS: usize = 32;
pub const INDEX_SIZE_MASK: u16 = 0x1FFF;
pub const INDEX_AM_RESERVED_BIT: u16 = 0x2000;
pub const INDEX_VAR_MASK: u16 = 0x4000;
pub const INDEX_NULL_MASK: u16 = 0x8000;
pub const INDEX_ATTRIBUTE_BITMAP_SIZE: usize = INDEX_MAX_KEYS.div_ceil(8);
pub const SIZE_OF_INDEX_TUPLE_DATA: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexTupleError {
    TooShort,
    SizeBitsMismatch { expected: usize, actual: usize },
    InvalidSize(usize),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IndexTupleData {
    pub t_tid: ItemPointerData,
    pub t_info: u16,
    pub payload: Vec<u8>,
}

pub type IndexTuple = IndexTupleData;

impl IndexTupleData {
    pub fn new_raw(
        t_tid: ItemPointerData,
        has_nulls: bool,
        has_varwidths: bool,
        am_reserved: bool,
        payload: Vec<u8>,
    ) -> Self {
        let mut t_info = (SIZE_OF_INDEX_TUPLE_DATA + payload.len()) as u16 & INDEX_SIZE_MASK;
        if has_nulls {
            t_info |= INDEX_NULL_MASK;
        }
        if has_varwidths {
            t_info |= INDEX_VAR_MASK;
        }
        if am_reserved {
            t_info |= INDEX_AM_RESERVED_BIT;
        }
        Self {
            t_tid,
            t_info,
            payload,
        }
    }

    pub fn size(&self) -> usize {
        usize::from(self.t_info & INDEX_SIZE_MASK)
    }

    pub fn has_nulls(&self) -> bool {
        self.t_info & INDEX_NULL_MASK != 0
    }

    pub fn has_varwidths(&self) -> bool {
        self.t_info & INDEX_VAR_MASK != 0
    }

    pub fn has_am_reserved_bit(&self) -> bool {
        self.t_info & INDEX_AM_RESERVED_BIT != 0
    }

    pub fn data_offset_from_info(t_info: u16) -> usize {
        if t_info & INDEX_NULL_MASK == 0 {
            max_align(SIZE_OF_INDEX_TUPLE_DATA)
        } else {
            max_align(SIZE_OF_INDEX_TUPLE_DATA + INDEX_ATTRIBUTE_BITMAP_SIZE)
        }
    }

    pub fn data_offset(&self) -> usize {
        Self::data_offset_from_info(self.t_info)
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.size());
        out.extend_from_slice(&self.t_tid.block_number.to_le_bytes());
        out.extend_from_slice(&self.t_tid.offset_number.to_le_bytes());
        out.extend_from_slice(&self.t_info.to_le_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, IndexTupleError> {
        if bytes.len() < SIZE_OF_INDEX_TUPLE_DATA {
            return Err(IndexTupleError::TooShort);
        }
        let t_info = u16::from_le_bytes([bytes[6], bytes[7]]);
        let size = usize::from(t_info & INDEX_SIZE_MASK);
        if size < SIZE_OF_INDEX_TUPLE_DATA {
            return Err(IndexTupleError::InvalidSize(size));
        }
        if size != bytes.len() {
            return Err(IndexTupleError::SizeBitsMismatch {
                expected: size,
                actual: bytes.len(),
            });
        }
        Ok(Self {
            t_tid: ItemPointerData {
                block_number: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
                offset_number: u16::from_le_bytes([bytes[4], bytes[5]]),
            },
            t_info,
            payload: bytes[SIZE_OF_INDEX_TUPLE_DATA..].to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_tuple_roundtrips_raw_header() {
        let tuple = IndexTupleData::new_raw(
            ItemPointerData {
                block_number: 42,
                offset_number: 7,
            },
            false,
            true,
            false,
            vec![1, 2, 3, 4, 5],
        );
        let parsed = IndexTupleData::parse(&tuple.serialize()).unwrap();
        assert_eq!(parsed, tuple);
        assert!(parsed.has_varwidths());
        assert!(!parsed.has_nulls());
    }

    #[test]
    fn index_tuple_data_offset_matches_postgres_shape() {
        let plain =
            IndexTupleData::new_raw(ItemPointerData::default(), false, false, false, vec![]);
        assert_eq!(plain.data_offset(), 8);

        let with_nulls =
            IndexTupleData::new_raw(ItemPointerData::default(), true, false, false, vec![]);
        assert_eq!(with_nulls.data_offset(), 16);
    }
}
