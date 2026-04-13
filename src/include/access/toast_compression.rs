#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToastCompressionId {
    Pglz = 0,
    Lz4 = 1,
    Invalid = 2,
}

pub const TOAST_PGLZ_COMPRESSION: char = 'p';
pub const TOAST_LZ4_COMPRESSION: char = 'l';
pub const INVALID_COMPRESSION_METHOD: char = '\0';

pub const fn compression_method_is_valid(method: char) -> bool {
    method != INVALID_COMPRESSION_METHOD
}
