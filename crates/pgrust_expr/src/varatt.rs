use std::mem::size_of;

pub const VARHDRSZ: usize = 4;
pub const VARHDRSZ_SHORT: usize = 1;
pub const VARHDRSZ_EXTERNAL: usize = 2;
pub const VARHDRSZ_COMPRESSED: usize = 8;

pub const VARLENA_EXTSIZE_BITS: u32 = 30;
pub const VARLENA_EXTSIZE_MASK: u32 = (1u32 << VARLENA_EXTSIZE_BITS) - 1;

pub const VARATT_EXTERNAL_HEADER: u8 = 0x01;
pub const VARTAG_INDIRECT: u8 = 1;
pub const VARTAG_ONDISK: u8 = 18;
pub const VARATT_4B_COMPRESSED_TAG: u8 = 0x02;
pub const VARATT_4B_TAG_MASK: u8 = 0x03;

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VarattExternal {
    pub va_rawsize: i32,
    pub va_extinfo: u32,
    pub va_valueid: u32,
    pub va_toastrelid: u32,
}

pub const TOAST_POINTER_SIZE: usize = VARHDRSZ_EXTERNAL + size_of::<VarattExternal>();
pub const INDIRECT_POINTER_SIZE: usize = VARHDRSZ_EXTERNAL + size_of::<usize>();

pub const fn varsize_4b(header: u32) -> usize {
    ((header >> 2) & 0x3fff_ffff) as usize
}

pub fn compressed_inline_total_size(bytes: &[u8]) -> Option<usize> {
    if bytes.len() < VARHDRSZ {
        return None;
    }
    let header = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
    Some(varsize_4b(header))
}

pub fn compressed_inline_extsize(bytes: &[u8]) -> Option<u32> {
    let tcinfo = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
    Some(tcinfo & VARLENA_EXTSIZE_MASK)
}

pub fn compressed_inline_compression_method(bytes: &[u8]) -> Option<u32> {
    let tcinfo = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
    Some(tcinfo >> VARLENA_EXTSIZE_BITS)
}

pub fn is_compressed_inline_datum(bytes: &[u8]) -> bool {
    bytes.len() >= VARHDRSZ_COMPRESSED
        && bytes[0] & VARATT_4B_TAG_MASK == VARATT_4B_COMPRESSED_TAG
        && compressed_inline_total_size(bytes).is_some_and(|len| len <= bytes.len())
}

pub fn encode_compressed_inline_datum(tcinfo: u32, payload: &[u8]) -> Vec<u8> {
    let total_len = u32::try_from(VARHDRSZ_COMPRESSED + payload.len()).unwrap_or(u32::MAX);
    let header = (total_len << 2) | u32::from(VARATT_4B_COMPRESSED_TAG);
    let mut bytes = Vec::with_capacity(VARHDRSZ_COMPRESSED + payload.len());
    bytes.extend_from_slice(&header.to_le_bytes());
    bytes.extend_from_slice(&tcinfo.to_le_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

pub fn decode_compressed_inline_datum(bytes: &[u8]) -> Option<(&[u8], u32, u32)> {
    if !is_compressed_inline_datum(bytes) {
        return None;
    }
    let total_len = compressed_inline_total_size(bytes)?;
    let payload = bytes.get(VARHDRSZ_COMPRESSED..total_len)?;
    Some((
        payload,
        compressed_inline_extsize(bytes)?,
        compressed_inline_compression_method(bytes)?,
    ))
}

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

pub fn is_indirect_toast_pointer(bytes: &[u8]) -> bool {
    bytes.len() >= INDIRECT_POINTER_SIZE
        && bytes[0] == VARATT_EXTERNAL_HEADER
        && bytes[1] == VARTAG_INDIRECT
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
