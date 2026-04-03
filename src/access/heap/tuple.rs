use crate::storage::page::{
    max_align, page_add_item, page_get_item, page_get_item_id, page_init, OffsetNumber,
    PageError,
};
use crate::storage::smgr::BLCKSZ;

pub const HEAP_HASNULL: u16 = 0x0001;
pub const SIZEOF_HEAP_TUPLE_HEADER: usize = 23;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ItemPointerData {
    pub block_number: u32,
    pub offset_number: OffsetNumber,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeapTupleHeaderData {
    pub xmin: u32,
    pub xmax: u32,
    pub cid_or_xvac: u32,
    pub ctid: ItemPointerData,
    pub infomask2: u16,
    pub infomask: u16,
    pub hoff: u8,
    pub null_bitmap: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeapTuple {
    pub header: HeapTupleHeaderData,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TupleError {
    HeaderTooShort,
    InvalidHeaderOffset,
    Page(PageError),
}

impl From<PageError> for TupleError {
    fn from(value: PageError) -> Self {
        Self::Page(value)
    }
}

impl HeapTupleHeaderData {
    pub fn new(natts: u16, null_bitmap: Vec<u8>) -> Self {
        let has_nulls = !null_bitmap.is_empty();
        let bitmap_len = if has_nulls { null_bitmap.len() } else { 0 };
        let hoff = max_align(SIZEOF_HEAP_TUPLE_HEADER + bitmap_len) as u8;
        Self {
            xmin: 0,
            xmax: 0,
            cid_or_xvac: 0,
            ctid: ItemPointerData::default(),
            infomask2: natts,
            infomask: if has_nulls { HEAP_HASNULL } else { 0 },
            hoff,
            null_bitmap,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![0u8; usize::from(self.hoff)];
        buf[0..4].copy_from_slice(&self.xmin.to_le_bytes());
        buf[4..8].copy_from_slice(&self.xmax.to_le_bytes());
        buf[8..12].copy_from_slice(&self.cid_or_xvac.to_le_bytes());
        buf[12..16].copy_from_slice(&self.ctid.block_number.to_le_bytes());
        buf[16..18].copy_from_slice(&self.ctid.offset_number.to_le_bytes());
        buf[18..20].copy_from_slice(&self.infomask2.to_le_bytes());
        buf[20..22].copy_from_slice(&self.infomask.to_le_bytes());
        buf[22] = self.hoff;
        let bitmap_end = SIZEOF_HEAP_TUPLE_HEADER + self.null_bitmap.len();
        if bitmap_end <= buf.len() {
            buf[SIZEOF_HEAP_TUPLE_HEADER..bitmap_end].copy_from_slice(&self.null_bitmap);
        }
        buf
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, TupleError> {
        if bytes.len() < SIZEOF_HEAP_TUPLE_HEADER {
            return Err(TupleError::HeaderTooShort);
        }
        let hoff = bytes[22];
        if usize::from(hoff) < SIZEOF_HEAP_TUPLE_HEADER || usize::from(hoff) > bytes.len() {
            return Err(TupleError::InvalidHeaderOffset);
        }
        let infomask2 = u16::from_le_bytes([bytes[18], bytes[19]]);
        let infomask = u16::from_le_bytes([bytes[20], bytes[21]]);
        let bitmap_len = if infomask & HEAP_HASNULL != 0 {
            usize::from(infomask2 & 0x07ff).div_ceil(8)
        } else {
            0
        };
        Ok(Self {
            xmin: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            xmax: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            cid_or_xvac: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            ctid: ItemPointerData {
                block_number: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
                offset_number: u16::from_le_bytes([bytes[16], bytes[17]]),
            },
            infomask2,
            infomask,
            hoff,
            null_bitmap: bytes[SIZEOF_HEAP_TUPLE_HEADER..SIZEOF_HEAP_TUPLE_HEADER + bitmap_len]
                .to_vec(),
        })
    }
}

impl HeapTuple {
    pub fn new_raw(natts: u16, data: Vec<u8>) -> Self {
        Self {
            header: HeapTupleHeaderData::new(natts, Vec::new()),
            data,
        }
    }

    pub fn new_raw_with_null_bitmap(natts: u16, null_bitmap: Vec<u8>, data: Vec<u8>) -> Self {
        Self {
            header: HeapTupleHeaderData::new(natts, null_bitmap),
            data,
        }
    }

    pub fn serialized_len(&self) -> usize {
        usize::from(self.header.hoff) + self.data.len()
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = self.header.serialize();
        bytes.extend_from_slice(&self.data);
        bytes
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, TupleError> {
        let header = HeapTupleHeaderData::parse(bytes)?;
        Ok(Self {
            data: bytes[usize::from(header.hoff)..].to_vec(),
            header,
        })
    }
}

pub fn heap_page_init(page: &mut [u8; BLCKSZ]) {
    page_init(page, 0);
}

pub fn heap_page_add_tuple(
    page: &mut [u8; BLCKSZ],
    block_number: u32,
    tuple: &HeapTuple,
) -> Result<OffsetNumber, TupleError> {
    let mut stored = tuple.clone();
    stored.header.ctid = ItemPointerData {
        block_number,
        offset_number: 0,
    };
    let offset = page_add_item(page, &stored.serialize())?;
    let item_bytes = page_get_item(page, offset)?;
    let mut parsed = HeapTuple::parse(item_bytes)?;
    parsed.header.ctid.offset_number = offset;
    let rewritten = parsed.serialize();

    let item_id = page_get_item_id(page, offset)?;
    let start = usize::from(item_id.lp_off);
    let end = start + usize::from(item_id.lp_len);
    page[start..end].copy_from_slice(&rewritten);
    Ok(offset)
}

pub fn heap_page_get_tuple(
    page: &[u8; BLCKSZ],
    offset: OffsetNumber,
) -> Result<HeapTuple, TupleError> {
    Ok(HeapTuple::parse(page_get_item(page, offset)?)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heap_tuple_header_roundtrip_without_nulls() {
        let tuple = HeapTuple::new_raw(3, vec![1, 2, 3, 4]);
        let parsed = HeapTuple::parse(&tuple.serialize()).unwrap();
        assert_eq!(parsed, tuple);
        assert_eq!(usize::from(parsed.header.hoff), max_align(SIZEOF_HEAP_TUPLE_HEADER));
    }

    #[test]
    fn heap_tuple_header_roundtrip_with_null_bitmap() {
        let tuple = HeapTuple::new_raw_with_null_bitmap(10, vec![0b1111_1011, 0b0000_0011], vec![9, 8, 7]);
        let parsed = HeapTuple::parse(&tuple.serialize()).unwrap();
        assert_eq!(parsed.header.infomask & HEAP_HASNULL, HEAP_HASNULL);
        assert_eq!(parsed.header.null_bitmap, vec![0b1111_1011, 0b0000_0011]);
        assert_eq!(parsed.data, vec![9, 8, 7]);
        assert_eq!(usize::from(parsed.header.hoff), max_align(SIZEOF_HEAP_TUPLE_HEADER + 2));
    }

    #[test]
    fn heap_page_add_tuple_sets_ctid_and_keeps_bytes() {
        let mut page = [0u8; BLCKSZ];
        heap_page_init(&mut page);

        let tuple = HeapTuple::new_raw(2, vec![0xAA, 0xBB, 0xCC]);
        let off = heap_page_add_tuple(&mut page, 42, &tuple).unwrap();
        assert_eq!(off, 1);

        let stored = heap_page_get_tuple(&page, off).unwrap();
        assert_eq!(stored.data, tuple.data);
        assert_eq!(stored.header.ctid.block_number, 42);
        assert_eq!(stored.header.ctid.offset_number, 1);
        assert_eq!(stored.header.infomask2 & 0x07ff, 2);
    }
}
