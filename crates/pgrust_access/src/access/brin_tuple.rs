use pgrust_storage::page::bufpage::MAXALIGN;

pub const BRIN_OFFSET_MASK: u8 = 0x1F;
pub const BRIN_EMPTY_RANGE_MASK: u8 = 0x20;
pub const BRIN_PLACEHOLDER_MASK: u8 = 0x40;
pub const BRIN_NULLS_MASK: u8 = 0x80;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct BrinTuple {
    pub bt_blkno: u32,
    pub bt_info: u8,
}

impl BrinTuple {
    pub const SIZE: usize = 5;
}

pub fn brin_tuple_data_offset(bt_info: u8) -> usize {
    usize::from(bt_info & BRIN_OFFSET_MASK)
}

pub fn brin_tuple_has_nulls(bt_info: u8) -> bool {
    bt_info & BRIN_NULLS_MASK != 0
}

pub fn brin_tuple_is_placeholder(bt_info: u8) -> bool {
    bt_info & BRIN_PLACEHOLDER_MASK != 0
}

pub fn brin_tuple_is_empty_range(bt_info: u8) -> bool {
    bt_info & BRIN_EMPTY_RANGE_MASK != 0
}

pub fn brin_null_bitmap_len(natts: usize) -> usize {
    (natts.saturating_mul(2)).div_ceil(8)
}

pub fn brin_header_size_with_bitmap(natts: usize, has_nulls: bool) -> usize {
    let header = BrinTuple::SIZE
        + if has_nulls {
            brin_null_bitmap_len(natts)
        } else {
            0
        };
    (header + (MAXALIGN - 1)) & !(MAXALIGN - 1)
}
