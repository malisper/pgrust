// pg_crc.c's reflected-0xEDB88320 table; the const generator reproduces the
// C literals exactly (verified at port time).
static PG_CRC32_TABLE: [u32; 256] = build_table();

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

// The pre-9.5 "bogus" CRC-32: normal table driven with reflected code
// (COMP_CRC32_REFLECTED_TABLE). Not the standard CRC-32 and not CRC-32C;
// tsquery/tsvector on-disk values depend on this exact variant.
pub fn legacy_crc32_lexeme(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc = PG_CRC32_TABLE[(((crc >> 24) ^ b as u32) & 0xFF) as usize] ^ (crc << 8);
    }
    crc ^ 0xFFFF_FFFF
}

// Standard reflected CRC-32 (zlib/Ethernet): COMP_CRC32_NORMAL_TABLE, the
// algorithm behind the SQL crc32(bytea) builtin.
pub fn traditional_crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc = PG_CRC32_TABLE[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}
