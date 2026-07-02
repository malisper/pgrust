use core::arch::aarch64::{__crc32cb, __crc32cd, __crc32ch, __crc32cw};

// Each aligned read below is guarded: the pointer reaches 2/4/8-byte
// alignment before any same-width read, or the remaining length is too short
// for that read to be attempted.
#[target_feature(enable = "crc")]
pub fn pg_comp_crc32c_armv8(mut crc: u32, data: &[u8]) -> u32 {
    let mut p = data.as_ptr();
    let mut len = data.len();

    // SAFETY: every read stays inside `data` (tracked by `len`) and is
    // aligned per the invariant above.
    unsafe {
        if p as usize & 1 != 0 && len >= 1 {
            crc = __crc32cb(crc, *p);
            p = p.add(1);
            len -= 1;
        }
        if p as usize & 3 != 0 && len >= 2 {
            crc = __crc32ch(crc, p.cast::<u16>().read());
            p = p.add(2);
            len -= 2;
        }
        if p as usize & 7 != 0 && len >= 4 {
            crc = __crc32cw(crc, p.cast::<u32>().read());
            p = p.add(4);
            len -= 4;
        }

        while len >= 8 {
            crc = __crc32cd(crc, p.cast::<u64>().read());
            p = p.add(8);
            len -= 8;
        }

        if len >= 4 {
            crc = __crc32cw(crc, p.cast::<u32>().read());
            p = p.add(4);
            len -= 4;
        }
        if len >= 2 {
            crc = __crc32ch(crc, p.cast::<u16>().read());
            p = p.add(2);
            len -= 2;
        }
        if len >= 1 {
            crc = __crc32cb(crc, *p);
        }
    }

    crc
}
