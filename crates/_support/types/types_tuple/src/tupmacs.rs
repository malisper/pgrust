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
    // Mixed ==/> ladder: eq-chains over {1,2,4,8} trip LLVM 22's pow2 switch
    // lowering (+6 insns/fetch), panic arms break inlining (parity journal).
    if attbyval {
        debug_assert!(matches!(attlen, 1 | 2 | 4 | 8), "unsupported byval length: {attlen}");
        if attlen == 4 {
            Datum::from_i32(t.cast::<i32>().read_unaligned())
        } else if attlen > 4 {
            Datum::from_i64(t.cast::<i64>().read_unaligned())
        } else if attlen == 2 {
            Datum::from_i16(t.cast::<i16>().read_unaligned())
        } else {
            Datum::from_char(t.cast::<i8>().read())
        }
    } else {
        Datum::from_usize(t as usize)
    }
}

/// # Safety
/// As [`fetch_att`].
#[inline]
pub unsafe fn fetchatt(att: &CompactAttribute, t: *const u8) -> Datum {
    // Bit-tested (one tbnz/arm): wins at the getattr sites, not in deform.
    let attlen = att.attlen as i32;
    if att.attbyval {
        debug_assert!(matches!(attlen, 1 | 2 | 4 | 8), "unsupported byval length: {attlen}");
        if attlen & 4 != 0 {
            Datum::from_i32(t.cast::<i32>().read_unaligned())
        } else if attlen & 8 != 0 {
            Datum::from_i64(t.cast::<i64>().read_unaligned())
        } else if attlen & 2 != 0 {
            Datum::from_i16(t.cast::<i16>().read_unaligned())
        } else {
            Datum::from_char(t.cast::<i8>().read())
        }
    } else {
        Datum::from_usize(t as usize)
    }
}

/// # Safety
/// `t` is writable for `attlen` bytes, aligned per the attribute.
#[inline]
pub unsafe fn store_att_byval(t: *mut u8, newdatum: Datum, attlen: i32) {
    // Inequality tree, not `match`/eq-ladder: fetch_att's pow2-switch trap.
    if attlen >= 4 {
        if attlen == 4 {
            t.cast::<i32>().write_unaligned(newdatum.as_i32())
        } else if attlen == 8 {
            t.cast::<u64>().write_unaligned(newdatum.as_u64())
        } else {
            unsupported_byval_length(attlen)
        }
    } else if attlen == 2 {
        t.cast::<i16>().write_unaligned(newdatum.as_i16())
    } else if attlen == 1 {
        t.cast::<i8>().write(newdatum.as_char())
    } else {
        unsupported_byval_length(attlen)
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
/// For `attlen == -1`, the datum points to a live varlena's first byte.
#[inline]
pub unsafe fn att_datum_alignby(
    cur_offset: usize,
    attalignby: u8,
    attlen: i32,
    attdatum: Datum,
) -> usize {
    if attlen == -1 && crate::varatt::varatt_is_1b(attdatum.as_usize() as *const u8) {
        cur_offset
    } else {
        TYPEALIGN(attalignby as usize, cur_offset)
    }
}

/// # Safety
/// As [`att_addlength_pointer`], with the pointer carried in the datum.
#[inline]
pub unsafe fn att_addlength_datum(cur_offset: usize, attlen: i32, attdatum: Datum) -> usize {
    att_addlength_pointer(cur_offset, attlen, attdatum.as_usize() as *const u8)
}

// Outlined: the scan idiom becomes a strlen() CALL (~5 spills in deform).
#[cold]
#[inline(never)]
unsafe fn addlength_cstring(cur_offset: usize, attptr: *const u8) -> usize {
    let mut n = 0usize;
    while *attptr.add(n) != 0 {
        n += 1;
    }
    cur_offset + n + 1
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
        addlength_cstring(cur_offset, attptr)
    }
}
