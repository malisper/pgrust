use crate::varatt::{VARLENA_EXTSIZE_BITS, VARLENA_EXTSIZE_MASK};

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToastCompressHeader {
    pub vl_len_: i32,
    pub tcinfo: u32,
}

pub const fn toast_compress_extsize(header: ToastCompressHeader) -> u32 {
    header.tcinfo & VARLENA_EXTSIZE_MASK
}

pub const fn toast_compress_method(header: ToastCompressHeader) -> u32 {
    header.tcinfo >> VARLENA_EXTSIZE_BITS
}

pub const fn toast_compress_set_size_and_compression_method(len: u32, cm: u32) -> u32 {
    (len & VARLENA_EXTSIZE_MASK) | (cm << VARLENA_EXTSIZE_BITS)
}
