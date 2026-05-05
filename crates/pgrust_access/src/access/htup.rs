pub use crate::access::itemptr::ItemPointerData;
pub use crate::access::tupdesc::{
    AttributeAlign, AttributeCompression, AttributeDesc, AttributeStorage,
};
pub use crate::common::heaptuple::{
    att_isnull, deform_raw, heap_page_add_tuple, heap_page_get_ctid, heap_page_get_tuple,
    heap_page_init, heap_page_replace_tuple,
};
use pgrust_storage::page::bufpage::PageError;

pub const HEAP_HASNULL: u16 = 0x0001;
pub const HEAP_HASVARWIDTH: u16 = 0x0002;
pub const HEAP_COMBOCID: u16 = 0x0020;
pub const HEAP_NATTS_MASK: u16 = 0x07ff;
pub const SIZEOF_HEAP_TUPLE_HEADER: usize = 23;

// Hint bits in infomask — set lazily on first visibility check to avoid
// repeated transaction status lookups.
pub const HEAP_XMIN_COMMITTED: u16 = 0x0100;
pub const HEAP_XMIN_INVALID: u16 = 0x0200;
pub const HEAP_XMAX_COMMITTED: u16 = 0x0400;
pub const HEAP_XMAX_INVALID: u16 = 0x0800;

/// Byte offset of the infomask field within a heap tuple header.
pub const INFOMASK_OFFSET: usize = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleValue {
    Null,
    Bytes(Vec<u8>),
    EncodedVarlena(Vec<u8>),
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
    Oversized {
        size: usize,
        max_size: usize,
    },
    Page(PageError),
}

impl From<PageError> for TupleError {
    fn from(value: PageError) -> Self {
        Self::Page(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_storage::page::bufpage::max_align;
    use pgrust_storage::smgr::BLCKSZ;

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
                attstorage: AttributeStorage::Plain,
                attcompression: AttributeCompression::Default,
                nullable: false,
            },
            AttributeDesc {
                name: "b".into(),
                attlen: 4,
                attalign: AttributeAlign::Int,
                attstorage: AttributeStorage::Plain,
                attcompression: AttributeCompression::Default,
                nullable: false,
            },
            AttributeDesc {
                name: "c".into(),
                attlen: -1,
                attalign: AttributeAlign::Int,
                attstorage: AttributeStorage::Extended,
                attcompression: AttributeCompression::Default,
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

        // a at offset 0, b aligned to 4, c is short varlena (1-byte header, no alignment).
        assert_eq!(&tuple.data[0..2], &[0x11, 0x22]);
        assert_eq!(&tuple.data[4..8], &[0x33, 0x44, 0x55, 0x66]);
        // Short varlena: header byte = (total_len << 1) | 0x01 = (6 << 1) | 1 = 0x0D
        assert_eq!(tuple.data[8], 0x0D);
        assert_eq!(&tuple.data[9..14], b"hello");

        let deformed = tuple.deform(&desc).unwrap();
        assert_eq!(deformed[0], Some(&[0x11, 0x22][..]));
        assert_eq!(deformed[1], Some(&[0x33, 0x44, 0x55, 0x66][..]));
        assert_eq!(deformed[2], Some(&b"hello"[..]));
    }

    #[test]
    fn typed_tuple_layout_handles_null_bitmap() {
        let desc = vec![
            AttributeDesc {
                name: "a".into(),
                attlen: 4,
                attalign: AttributeAlign::Int,
                attstorage: AttributeStorage::Plain,
                attcompression: AttributeCompression::Default,
                nullable: false,
            },
            AttributeDesc {
                name: "b".into(),
                attlen: 2,
                attalign: AttributeAlign::Short,
                attstorage: AttributeStorage::Plain,
                attcompression: AttributeCompression::Default,
                nullable: true,
            },
            AttributeDesc {
                name: "c".into(),
                attlen: 4,
                attalign: AttributeAlign::Int,
                attstorage: AttributeStorage::Plain,
                attcompression: AttributeCompression::Default,
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
        assert_eq!(deformed[0], Some(&[1, 2, 3, 4][..]));
        assert_eq!(deformed[1], None);
        assert_eq!(deformed[2], Some(&[9, 10, 11, 12][..]));
    }

    #[test]
    fn replace_tuple_only_writes_header_preserves_user_data() {
        let mut page = [0u8; BLCKSZ];
        heap_page_init(&mut page);

        let tuple = HeapTuple::new_raw(2, vec![0xAA, 0xBB, 0xCC, 0xDD]);
        let off = heap_page_add_tuple(&mut page, 0, &tuple).unwrap();

        // Read back and verify original data.
        let original = heap_page_get_tuple(&page, off).unwrap();
        assert_eq!(original.data, vec![0xAA, 0xBB, 0xCC, 0xDD]);
        assert_eq!(original.header.xmax, 0);

        // Modify only header fields and replace.
        let mut modified = original.clone();
        modified.header.xmax = 42;
        modified.header.infomask |= 0x0400; // set some flag
        heap_page_replace_tuple(&mut page, off, &modified).unwrap();

        // Read back: header fields changed, user data untouched.
        let after = heap_page_get_tuple(&page, off).unwrap();
        assert_eq!(after.header.xmax, 42);
        assert_eq!(after.header.infomask & 0x0400, 0x0400);
        assert_eq!(after.data, vec![0xAA, 0xBB, 0xCC, 0xDD]);
    }

    #[test]
    fn replace_tuple_preserves_tuple_layout_bits() {
        let mut page = [0u8; BLCKSZ];
        heap_page_init(&mut page);

        let desc = vec![
            AttributeDesc {
                name: "a".into(),
                attlen: 4,
                attalign: AttributeAlign::Int,
                attstorage: AttributeStorage::Plain,
                attcompression: AttributeCompression::Default,
                nullable: false,
            },
            AttributeDesc {
                name: "b".into(),
                attlen: -1,
                attalign: AttributeAlign::Int,
                attstorage: AttributeStorage::Extended,
                attcompression: AttributeCompression::Default,
                nullable: true,
            },
        ];
        let tuple = HeapTuple::from_values(
            &desc,
            &[TupleValue::Bytes(vec![1, 0, 0, 0]), TupleValue::Null],
        )
        .unwrap();
        let off = heap_page_add_tuple(&mut page, 0, &tuple).unwrap();
        let original = heap_page_get_tuple(&page, off).unwrap();

        let mut modified = original.clone();
        modified.header.xmax = 42;
        modified.header.infomask &= !HEAP_HASNULL;
        modified.header.null_bitmap = Vec::new();
        heap_page_replace_tuple(&mut page, off, &modified).unwrap();

        let after = heap_page_get_tuple(&page, off).unwrap();
        assert_eq!(after.header.xmax, 42);
        assert_eq!(after.header.infomask & HEAP_HASNULL, HEAP_HASNULL);
        assert_eq!(after.header.null_bitmap, original.header.null_bitmap);
        assert_eq!(after.deform(&desc).unwrap()[1], None);
    }
}
