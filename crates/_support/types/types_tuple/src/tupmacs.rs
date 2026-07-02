use ::datum::Datum;

use crate::htup::bits8;
use crate::tupdesc::CompactAttribute;
use crate::varatt::{varatt_not_pad_byte, varsize_any};

/// # Safety
/// `bits` points to a null bitmap covering at least bit `att`.
#[inline]
pub unsafe fn att_isnull(att: usize, bits: *const bits8) -> bool {
    (*bits.add(att >> 3) & (1 << (att & 0x07))) == 0
}

#[cold]
#[inline(never)]
fn unsupported_byval_length(attlen: i32) -> ! {
    panic!("unsupported byval length: {attlen}")
}

/// # Safety
/// `t` points to live tuple data of the given byval/len shape, attalignby-aligned.
#[inline]
pub unsafe fn fetch_att(t: *const u8, attbyval: bool, attlen: i32) -> Datum {
    if attbyval {
        match attlen {
            1 => Datum::from_char(t.cast::<i8>().read()),
            2 => Datum::from_i16(t.cast::<i16>().read_unaligned()),
            4 => Datum::from_i32(t.cast::<i32>().read_unaligned()),
            8 => Datum::from_i64(t.cast::<i64>().read_unaligned()),
            _ => unsupported_byval_length(attlen),
        }
    } else {
        Datum::from_usize(t as usize)
    }
}

/// # Safety
/// As [`fetch_att`].
#[inline]
pub unsafe fn fetchatt(att: &CompactAttribute, t: *const u8) -> Datum {
    fetch_att(t, att.attbyval, att.attlen as i32)
}

/// # Safety
/// `t` is writable for `attlen` bytes, aligned per the attribute.
#[inline]
pub unsafe fn store_att_byval(t: *mut u8, newdatum: Datum, attlen: i32) {
    match attlen {
        1 => t.cast::<i8>().write(newdatum.as_char()),
        2 => t.cast::<i16>().write_unaligned(newdatum.as_i16()),
        4 => t.cast::<i32>().write_unaligned(newdatum.as_i32()),
        8 => t.cast::<u64>().write_unaligned(newdatum.as_u64()),
        _ => unsupported_byval_length(attlen),
    }
}

#[inline]
pub const fn TYPEALIGN(alignval: usize, len: usize) -> usize {
    (len + alignval - 1) & !(alignval - 1)
}

#[inline]
pub const fn att_nominal_alignby(cur_offset: usize, attalignby: u8) -> usize {
    TYPEALIGN(attalignby as usize, cur_offset)
}

/// # Safety
/// For `attlen == -1`, `attptr` points to a live byte (the pad-byte peek).
#[inline]
pub unsafe fn att_pointer_alignby(
    cur_offset: usize,
    attalignby: u8,
    attlen: i32,
    attptr: *const u8,
) -> usize {
    if attlen == -1 && varatt_not_pad_byte(attptr) {
        cur_offset
    } else {
        TYPEALIGN(attalignby as usize, cur_offset)
    }
}

/// # Safety
/// For `attlen == -1` a live varlena at `attptr`; for `-2` a live NUL-terminated cstring.
#[inline]
pub unsafe fn att_addlength_pointer(cur_offset: usize, attlen: i32, attptr: *const u8) -> usize {
    if attlen > 0 {
        cur_offset + attlen as usize
    } else if attlen == -1 {
        cur_offset + varsize_any(attptr)
    } else {
        debug_assert!(attlen == -2);
        let mut n = 0usize;
        while *attptr.add(n) != 0 {
            n += 1;
        }
        cur_offset + n + 1
    }
}
