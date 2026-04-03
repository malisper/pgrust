use crate::storage::page::{
    OffsetNumber, PageError, max_align, page_add_item, page_get_item, page_get_item_id, page_init,
};
use crate::storage::smgr::BLCKSZ;

pub const HEAP_HASNULL: u16 = 0x0001;
pub const HEAP_HASVARWIDTH: u16 = 0x0002;
pub const HEAP_NATTS_MASK: u16 = 0x07ff;
pub const SIZEOF_HEAP_TUPLE_HEADER: usize = 23;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttributeAlign {
    Char,
    Short,
    Int,
    Double,
}

impl AttributeAlign {
    fn align_offset(self, off: usize) -> usize {
        match self {
            Self::Char => off,
            Self::Short => (off + 1) & !1,
            Self::Int => (off + 3) & !3,
            Self::Double => (off + 7) & !7,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributeDesc {
    pub name: String,
    pub attlen: i16,
    pub attalign: AttributeAlign,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleValue {
    Null,
    Bytes(Vec<u8>),
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleError {
    HeaderTooShort,
    InvalidHeaderOffset,
    LengthMismatch {
        expected: usize,
        actual: usize,
    },
    WrongValueCount {
        expected: usize,
        actual: usize,
    },
    NullNotAllowed {
        attnum: usize,
        name: String,
    },
    InvalidValueLength {
        attnum: usize,
        name: String,
        expected: usize,
        actual: usize,
    },
    UnsupportedAttributeType {
        attnum: usize,
        name: String,
        attlen: i16,
    },
    AttributeCountTooLarge(usize),
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

    pub fn from_values(desc: &[AttributeDesc], values: &[TupleValue]) -> Result<Self, TupleError> {
        if desc.len() != values.len() {
            return Err(TupleError::WrongValueCount {
                expected: desc.len(),
                actual: values.len(),
            });
        }
        if desc.len() > usize::from(HEAP_NATTS_MASK) {
            return Err(TupleError::AttributeCountTooLarge(desc.len()));
        }

        let has_nulls = values.iter().any(|v| matches!(v, TupleValue::Null));
        let mut null_bitmap = if has_nulls {
            vec![0u8; bitmap_len(desc.len() as u16)]
        } else {
            Vec::new()
        };

        let mut infomask = 0u16;
        let mut data = Vec::new();

        for (i, (attr, value)) in desc.iter().zip(values.iter()).enumerate() {
            match value {
                TupleValue::Null => {
                    if !attr.nullable {
                        return Err(TupleError::NullNotAllowed {
                            attnum: i + 1,
                            name: attr.name.clone(),
                        });
                    }
                }
                TupleValue::Bytes(bytes) => {
                    if has_nulls {
                        null_bitmap[i / 8] |= 1 << (i % 8);
                    }

                    match attr.attlen {
                        len if len > 0 => {
                            let expected = len as usize;
                            if bytes.len() != expected {
                                return Err(TupleError::InvalidValueLength {
                                    attnum: i + 1,
                                    name: attr.name.clone(),
                                    expected,
                                    actual: bytes.len(),
                                });
                            }
                            let aligned = attr.attalign.align_offset(data.len());
                            if aligned > data.len() {
                                data.resize(aligned, 0);
                            }
                            data.extend_from_slice(bytes);
                        }
                        -1 => {
                            infomask |= HEAP_HASVARWIDTH;
                            let aligned = attr.attalign.align_offset(data.len());
                            if aligned > data.len() {
                                data.resize(aligned, 0);
                            }
                            let total_len = 4 + bytes.len();
                            data.extend_from_slice(&(total_len as u32).to_le_bytes());
                            data.extend_from_slice(bytes);
                        }
                        -2 => {
                            infomask |= HEAP_HASVARWIDTH;
                            data.extend_from_slice(bytes);
                            data.push(0);
                        }
                        other => {
                            return Err(TupleError::UnsupportedAttributeType {
                                attnum: i + 1,
                                name: attr.name.clone(),
                                attlen: other,
                            });
                        }
                    }
                }
            }
        }

        if has_nulls {
            infomask |= HEAP_HASNULL;
        }

        let mut header = HeapTupleHeaderData::new(desc.len() as u16, null_bitmap);
        header.infomask |= infomask;

        Ok(Self { header, data })
    }

    pub fn deform(&self, desc: &[AttributeDesc]) -> Result<Vec<Option<Vec<u8>>>, TupleError> {
        let natts = usize::from(self.header.infomask2 & HEAP_NATTS_MASK);
        if natts != desc.len() {
            return Err(TupleError::WrongValueCount {
                expected: natts,
                actual: desc.len(),
            });
        }

        let mut values = Vec::with_capacity(desc.len());
        let mut off = 0usize;

        for (i, attr) in desc.iter().enumerate() {
            let is_null =
                self.header.infomask & HEAP_HASNULL != 0 && att_isnull(i, &self.header.null_bitmap);
            if is_null {
                values.push(None);
                continue;
            }

            match attr.attlen {
                len if len > 0 => {
                    off = attr.attalign.align_offset(off);
                    let end = off + len as usize;
                    values.push(Some(self.data[off..end].to_vec()));
                    off = end;
                }
                -1 => {
                    off = attr.attalign.align_offset(off);
                    let total_len = u32::from_le_bytes([
                        self.data[off],
                        self.data[off + 1],
                        self.data[off + 2],
                        self.data[off + 3],
                    ]) as usize;
                    let start = off + 4;
                    let end = off + total_len;
                    values.push(Some(self.data[start..end].to_vec()));
                    off = end;
                }
                -2 => {
                    let mut end = off;
                    while self.data[end] != 0 {
                        end += 1;
                    }
                    values.push(Some(self.data[off..end].to_vec()));
                    off = end + 1;
                }
                other => {
                    return Err(TupleError::UnsupportedAttributeType {
                        attnum: i + 1,
                        name: attr.name.clone(),
                        attlen: other,
                    });
                }
            }
        }

        Ok(values)
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

pub fn heap_page_replace_tuple(
    page: &mut [u8; BLCKSZ],
    offset: OffsetNumber,
    tuple: &HeapTuple,
) -> Result<(), TupleError> {
    let item_id = page_get_item_id(page, offset)?;
    let expected = usize::from(item_id.lp_len);
    let rewritten = tuple.serialize();
    if rewritten.len() != expected {
        return Err(TupleError::LengthMismatch {
            expected,
            actual: rewritten.len(),
        });
    }

    let start = usize::from(item_id.lp_off);
    let end = start + expected;
    page[start..end].copy_from_slice(&rewritten);
    Ok(())
}

fn bitmap_len(natts: u16) -> usize {
    usize::from(natts).div_ceil(8)
}

fn att_isnull(attnum: usize, bits: &[u8]) -> bool {
    (bits[attnum >> 3] & (1 << (attnum & 0x07))) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heap_tuple_header_roundtrip_without_nulls() {
        let tuple = HeapTuple::new_raw(3, vec![1, 2, 3, 4]);
        let parsed = HeapTuple::parse(&tuple.serialize()).unwrap();
        assert_eq!(parsed, tuple);
        assert_eq!(
            usize::from(parsed.header.hoff),
            max_align(SIZEOF_HEAP_TUPLE_HEADER)
        );
    }

    #[test]
    fn heap_tuple_header_roundtrip_with_null_bitmap() {
        let tuple =
            HeapTuple::new_raw_with_null_bitmap(10, vec![0b1111_1011, 0b0000_0011], vec![9, 8, 7]);
        let parsed = HeapTuple::parse(&tuple.serialize()).unwrap();
        assert_eq!(parsed.header.infomask & HEAP_HASNULL, HEAP_HASNULL);
        assert_eq!(parsed.header.null_bitmap, vec![0b1111_1011, 0b0000_0011]);
        assert_eq!(parsed.data, vec![9, 8, 7]);
        assert_eq!(
            usize::from(parsed.header.hoff),
            max_align(SIZEOF_HEAP_TUPLE_HEADER + 2)
        );
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

    #[test]
    fn typed_tuple_layout_matches_postgres_style_alignment_and_bitmap() {
        let desc = vec![
            AttributeDesc {
                name: "a".into(),
                attlen: 2,
                attalign: AttributeAlign::Short,
                nullable: false,
            },
            AttributeDesc {
                name: "b".into(),
                attlen: 4,
                attalign: AttributeAlign::Int,
                nullable: false,
            },
            AttributeDesc {
                name: "c".into(),
                attlen: -1,
                attalign: AttributeAlign::Int,
                nullable: true,
            },
        ];

        let tuple = HeapTuple::from_values(
            &desc,
            &[
                TupleValue::Bytes(vec![0x11, 0x22]),
                TupleValue::Bytes(vec![0x33, 0x44, 0x55, 0x66]),
                TupleValue::Bytes(b"hello".to_vec()),
            ],
        )
        .unwrap();

        assert_eq!(tuple.header.infomask & HEAP_HASVARWIDTH, HEAP_HASVARWIDTH);
        assert_eq!(tuple.header.infomask2 & HEAP_NATTS_MASK, 3);
        assert_eq!(tuple.header.null_bitmap, Vec::<u8>::new());

        // a at offset 0, b aligned to 4, c aligned to 8 with 4-byte varlena len.
        assert_eq!(&tuple.data[0..2], &[0x11, 0x22]);
        assert_eq!(&tuple.data[4..8], &[0x33, 0x44, 0x55, 0x66]);
        assert_eq!(u32::from_le_bytes(tuple.data[8..12].try_into().unwrap()), 9);
        assert_eq!(&tuple.data[12..17], b"hello");

        let deformed = tuple.deform(&desc).unwrap();
        assert_eq!(deformed[0], Some(vec![0x11, 0x22]));
        assert_eq!(deformed[1], Some(vec![0x33, 0x44, 0x55, 0x66]));
        assert_eq!(deformed[2], Some(b"hello".to_vec()));
    }

    #[test]
    fn typed_tuple_layout_handles_null_bitmap() {
        let desc = vec![
            AttributeDesc {
                name: "a".into(),
                attlen: 4,
                attalign: AttributeAlign::Int,
                nullable: false,
            },
            AttributeDesc {
                name: "b".into(),
                attlen: 2,
                attalign: AttributeAlign::Short,
                nullable: true,
            },
            AttributeDesc {
                name: "c".into(),
                attlen: 4,
                attalign: AttributeAlign::Int,
                nullable: false,
            },
        ];

        let tuple = HeapTuple::from_values(
            &desc,
            &[
                TupleValue::Bytes(vec![1, 2, 3, 4]),
                TupleValue::Null,
                TupleValue::Bytes(vec![9, 10, 11, 12]),
            ],
        )
        .unwrap();

        assert_eq!(tuple.header.infomask & HEAP_HASNULL, HEAP_HASNULL);
        assert_eq!(tuple.header.null_bitmap, vec![0b0000_0101]);

        let deformed = tuple.deform(&desc).unwrap();
        assert_eq!(deformed[0], Some(vec![1, 2, 3, 4]));
        assert_eq!(deformed[1], None);
        assert_eq!(deformed[2], Some(vec![9, 10, 11, 12]));
    }
}
