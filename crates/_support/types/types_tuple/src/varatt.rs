// Raw varlena header decoding for the tuple walk; the typed 4B-U lane is datum::VarlenaRef.

pub const VARHDRSZ: usize = 4;
pub const VARHDRSZ_SHORT: usize = 1;
pub const VARHDRSZ_EXTERNAL: usize = 2;
pub const VARATT_SHORT_MAX: usize = 0x7F;

pub const VARTAG_INDIRECT: u8 = 1;
pub const VARTAG_EXPANDED_RO: u8 = 2;
pub const VARTAG_EXPANDED_RW: u8 = 3;
pub const VARTAG_ONDISK: u8 = 18;

// sizeof: varatt_indirect = 8 (pointer), varatt_expanded = 8, varatt_external = 16.
#[inline]
pub const fn vartag_size(tag: u8) -> usize {
    match tag {
        VARTAG_INDIRECT => 8,
        VARTAG_EXPANDED_RO | VARTAG_EXPANDED_RW => 8,
        VARTAG_ONDISK => 16,
        _ => panic!("unrecognized TOAST vartag"),
    }
}

#[inline]
pub const fn vartag_is_expanded(tag: u8) -> bool {
    (tag & !1) == VARTAG_EXPANDED_RO
}

#[cfg(target_endian = "little")]
#[inline]
pub const fn varsize_4b_word(word: u32) -> u32 {
    (word >> 2) & 0x3FFF_FFFF
}

#[cfg(target_endian = "big")]
#[inline]
pub const fn varsize_4b_word(word: u32) -> u32 {
    word & 0x3FFF_FFFF
}

#[cfg(target_endian = "little")]
#[inline]
pub const fn set_varsize_4b_word(len: u32) -> u32 {
    len << 2
}

#[cfg(target_endian = "big")]
#[inline]
pub const fn set_varsize_4b_word(len: u32) -> u32 {
    len & 0x3FFF_FFFF
}

#[cfg(target_endian = "little")]
#[inline]
pub const fn set_varsize_4b_c_word(len: u32) -> u32 {
    (len << 2) | 0x02
}

#[cfg(target_endian = "big")]
#[inline]
pub const fn set_varsize_4b_c_word(len: u32) -> u32 {
    (len & 0x3FFF_FFFF) | 0x4000_0000
}

/// # Safety
/// `p` points to a live varlena's first byte.
#[inline]
pub unsafe fn varatt_is_1b(p: *const u8) -> bool {
    #[cfg(target_endian = "little")]
    return (*p & 0x01) == 0x01;
    #[cfg(target_endian = "big")]
    return (*p & 0x80) == 0x80;
}

/// # Safety
/// As [`varatt_is_1b`].
#[inline]
pub unsafe fn varatt_is_1b_e(p: *const u8) -> bool {
    #[cfg(target_endian = "little")]
    return *p == 0x01;
    #[cfg(target_endian = "big")]
    return *p == 0x80;
}

/// # Safety
/// As [`varatt_is_1b`].
#[inline]
pub unsafe fn varatt_is_4b_u(p: *const u8) -> bool {
    #[cfg(target_endian = "little")]
    return (*p & 0x03) == 0x00;
    #[cfg(target_endian = "big")]
    return (*p & 0xC0) == 0x00;
}

/// # Safety
/// `p` points to a live byte.
#[inline]
pub unsafe fn varatt_not_pad_byte(p: *const u8) -> bool {
    *p != 0
}

/// # Safety
/// `p` points to a live 1-byte-header varlena.
#[inline]
pub unsafe fn varsize_1b(p: *const u8) -> usize {
    #[cfg(target_endian = "little")]
    return ((*p >> 1) & 0x7F) as usize;
    #[cfg(target_endian = "big")]
    return (*p & 0x7F) as usize;
}

/// # Safety
/// `p` points to a live varlena readable through its 4-byte header.
#[inline]
pub unsafe fn varsize_4b(p: *const u8) -> usize {
    varsize_4b_word(p.cast::<u32>().read_unaligned()) as usize
}

/// # Safety
/// `p` points to a live external TOAST pointer (`varattrib_1b_e`).
#[inline]
pub unsafe fn vartag_external(p: *const u8) -> u8 {
    *p.add(1)
}

/// # Safety
/// As [`varatt_is_1b`].
#[inline]
pub unsafe fn varatt_is_external_expanded(p: *const u8) -> bool {
    varatt_is_1b_e(p) && vartag_is_expanded(vartag_external(p))
}

/// # Safety
/// `p` points to a live varlena readable through its 4-byte header.
#[inline]
pub unsafe fn varatt_can_make_short(p: *const u8) -> bool {
    varatt_is_4b_u(p) && (varsize_4b(p) - VARHDRSZ + VARHDRSZ_SHORT) <= VARATT_SHORT_MAX
}

/// # Safety
/// As [`varatt_can_make_short`].
#[inline]
pub unsafe fn varatt_converted_short_size(p: *const u8) -> usize {
    varsize_4b(p) - VARHDRSZ + VARHDRSZ_SHORT
}

/// # Safety
/// `p` is writable; `len <= VARATT_SHORT_MAX`.
#[inline]
pub unsafe fn set_varsize_short(p: *mut u8, len: usize) {
    #[cfg(target_endian = "little")]
    {
        *p = ((len as u8) << 1) | 0x01;
    }
    #[cfg(target_endian = "big")]
    {
        *p = (len as u8) | 0x80;
    }
}

/// # Safety
/// `p` points to a live external TOAST pointer.
#[inline]
pub unsafe fn varsize_external(p: *const u8) -> usize {
    VARHDRSZ_EXTERNAL + vartag_size(vartag_external(p))
}

/// # Safety
/// `p` points to a live varlena image of any form, readable through its header.
#[inline]
pub unsafe fn varsize_any(p: *const u8) -> usize {
    if varatt_is_1b_e(p) {
        varsize_external(p)
    } else if varatt_is_1b(p) {
        varsize_1b(p)
    } else {
        varsize_4b(p)
    }
}
