use core::arch::x86_64::{_mm_crc32_u32, _mm_crc32_u64, _mm_crc32_u8};

#[target_feature(enable = "sse4.2")]
pub fn pg_comp_crc32c_sse42(mut crc: u32, data: &[u8]) -> u32 {
    let mut p = data.as_ptr();
    let mut len = data.len();

    // SAFETY: every read stays inside `data` (tracked by `len`); reads are
    // deliberately unaligned like the C sse42 path, via read_unaligned.
    unsafe {
        let mut crc64 = crc as u64;
        while len >= 8 {
            crc64 = _mm_crc32_u64(crc64, p.cast::<u64>().read_unaligned());
            p = p.add(8);
            len -= 8;
        }
        crc = crc64 as u32;

        if len >= 4 {
            crc = _mm_crc32_u32(crc, p.cast::<u32>().read_unaligned());
            p = p.add(4);
            len -= 4;
        }

        while len >= 1 {
            crc = _mm_crc32_u8(crc, *p);
            p = p.add(1);
            len -= 1;
        }
    }

    crc
}
