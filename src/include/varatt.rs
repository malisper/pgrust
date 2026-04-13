use std::mem::size_of;

pub const VARHDRSZ: usize = 4;
pub const VARHDRSZ_SHORT: usize = 1;
pub const VARHDRSZ_EXTERNAL: usize = 2;

pub const VARLENA_EXTSIZE_BITS: u32 = 30;
pub const VARLENA_EXTSIZE_MASK: u32 = (1u32 << VARLENA_EXTSIZE_BITS) - 1;

pub const VARATT_EXTERNAL_HEADER: u8 = 0x01;
pub const VARTAG_ONDISK: u8 = 18;

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VarattExternal {
    pub va_rawsize: i32,
    pub va_extinfo: u32,
    pub va_valueid: u32,
    pub va_toastrelid: u32,
}

pub const TOAST_POINTER_SIZE: usize = VARHDRSZ_EXTERNAL + size_of::<VarattExternal>();

pub const fn varatt_external_get_extsize(pointer: VarattExternal) -> u32 {
    pointer.va_extinfo & VARLENA_EXTSIZE_MASK
}

pub const fn varatt_external_get_compression_method(pointer: VarattExternal) -> u32 {
    pointer.va_extinfo >> VARLENA_EXTSIZE_BITS
}

pub const fn varatt_external_is_compressed(pointer: VarattExternal) -> bool {
    varatt_external_get_extsize(pointer)
        < (pointer.va_rawsize as u32).saturating_sub(VARHDRSZ as u32)
}

pub const fn varatt_external_set_size_and_compression_method(len: u32, cm: u32) -> u32 {
    (len & VARLENA_EXTSIZE_MASK) | (cm << VARLENA_EXTSIZE_BITS)
}

pub fn is_ondisk_toast_pointer(bytes: &[u8]) -> bool {
    bytes.len() >= TOAST_POINTER_SIZE
        && bytes[0] == VARATT_EXTERNAL_HEADER
        && bytes[1] == VARTAG_ONDISK
}

pub fn decode_ondisk_toast_pointer(bytes: &[u8]) -> Option<VarattExternal> {
    if !is_ondisk_toast_pointer(bytes) {
        return None;
    }
    let data = &bytes[VARHDRSZ_EXTERNAL..VARHDRSZ_EXTERNAL + size_of::<VarattExternal>()];
    Some(VarattExternal {
        va_rawsize: i32::from_le_bytes(data[0..4].try_into().ok()?),
        va_extinfo: u32::from_le_bytes(data[4..8].try_into().ok()?),
        va_valueid: u32::from_le_bytes(data[8..12].try_into().ok()?),
        va_toastrelid: u32::from_le_bytes(data[12..16].try_into().ok()?),
    })
}

pub fn encode_ondisk_toast_pointer(pointer: VarattExternal) -> [u8; TOAST_POINTER_SIZE] {
    let mut bytes = [0u8; TOAST_POINTER_SIZE];
    bytes[0] = VARATT_EXTERNAL_HEADER;
    bytes[1] = VARTAG_ONDISK;
    bytes[2..6].copy_from_slice(&pointer.va_rawsize.to_le_bytes());
    bytes[6..10].copy_from_slice(&pointer.va_extinfo.to_le_bytes());
    bytes[10..14].copy_from_slice(&pointer.va_valueid.to_le_bytes());
    bytes[14..18].copy_from_slice(&pointer.va_toastrelid.to_le_bytes());
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ondisk_pointer_roundtrips() {
        let pointer = VarattExternal {
            va_rawsize: 1024,
            va_extinfo: varatt_external_set_size_and_compression_method(512, 0),
            va_valueid: 42,
            va_toastrelid: 99,
        };
        let encoded = encode_ondisk_toast_pointer(pointer);
        assert!(is_ondisk_toast_pointer(&encoded));
        assert_eq!(decode_ondisk_toast_pointer(&encoded), Some(pointer));
    }
}
