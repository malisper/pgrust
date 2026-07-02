// Slicing-by-8 tables for the Castagnoli polynomial, little-endian layout.
// The const generator reproduces pg_crc32c_sb8.c's literal tables exactly
// (verified value-for-value against the vendored C at port time).
static PG_CRC32C_TABLE: [[u32; 256]; 8] = build_tables();

const fn build_tables() -> [[u32; 256]; 8] {
    let mut t = [[0u32; 256]; 8];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { 0x82F6_3B78 ^ (c >> 1) } else { c >> 1 };
            k += 1;
        }
        t[0][i] = c;
        i += 1;
    }
    let mut k = 1;
    while k < 8 {
        let mut i = 0;
        while i < 256 {
            t[k][i] = (t[k - 1][i] >> 8) ^ t[0][(t[k - 1][i] & 0xFF) as usize];
            i += 1;
        }
        k += 1;
    }
    t
}

#[inline(always)]
fn crc8(crc: u32, x: u8) -> u32 {
    PG_CRC32C_TABLE[0][((crc ^ x as u32) & 0xFF) as usize] ^ (crc >> 8)
}

pub fn pg_comp_crc32c_sb8(mut crc: u32, data: &[u8]) -> u32 {
    let mut p = data.as_ptr();
    let mut len = data.len();

    // SAFETY: reads stay inside `data` (tracked by `len`); the u32 reads run
    // only after `p` reaches 4-byte alignment.
    unsafe {
        while len > 0 && p as usize & 3 != 0 {
            crc = crc8(crc, *p);
            p = p.add(1);
            len -= 1;
        }

        while len >= 8 {
            let a = p.cast::<u32>().read() ^ crc;
            let b = p.add(4).cast::<u32>().read();

            crc = PG_CRC32C_TABLE[0][(b >> 24) as usize]
                ^ PG_CRC32C_TABLE[1][(b >> 16 & 0xFF) as usize]
                ^ PG_CRC32C_TABLE[2][(b >> 8 & 0xFF) as usize]
                ^ PG_CRC32C_TABLE[3][(b & 0xFF) as usize]
                ^ PG_CRC32C_TABLE[4][(a >> 24) as usize]
                ^ PG_CRC32C_TABLE[5][(a >> 16 & 0xFF) as usize]
                ^ PG_CRC32C_TABLE[6][(a >> 8 & 0xFF) as usize]
                ^ PG_CRC32C_TABLE[7][(a & 0xFF) as usize];

            p = p.add(8);
            len -= 8;
        }

        while len > 0 {
            crc = crc8(crc, *p);
            p = p.add(1);
            len -= 1;
        }
    }

    crc
}
