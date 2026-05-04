#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToastCompressionId {
    Pglz = 0,
    Lz4 = 1,
    Invalid = 2,
}

impl ToastCompressionId {
    pub const fn from_u32(value: u32) -> Option<Self> {
        match value {
            0 => Some(Self::Pglz),
            1 => Some(Self::Lz4),
            2 => Some(Self::Invalid),
            _ => None,
        }
    }

    pub const fn method(self) -> char {
        match self {
            Self::Pglz => TOAST_PGLZ_COMPRESSION,
            Self::Lz4 => TOAST_LZ4_COMPRESSION,
            Self::Invalid => INVALID_COMPRESSION_METHOD,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Pglz => "pglz",
            Self::Lz4 => "lz4",
            Self::Invalid => "invalid",
        }
    }
}

pub const TOAST_PGLZ_COMPRESSION: char = 'p';
pub const TOAST_LZ4_COMPRESSION: char = 'l';
pub const INVALID_COMPRESSION_METHOD: char = '\0';
pub const DEFAULT_TOAST_COMPRESSION: char = TOAST_PGLZ_COMPRESSION;

pub const fn compression_method_is_valid(method: char) -> bool {
    method != INVALID_COMPRESSION_METHOD
}
