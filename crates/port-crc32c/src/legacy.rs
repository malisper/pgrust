//! Legacy CRC-32 (`src/common/pg_crc.c` + `src/include/common/pg_crc.h`).
//!
//! PostgreSQL's "legacy" CRC-32 is the standard reflected CRC-32 (zlib/Ethernet
//! polynomial, reflected form 0xEDB88320), distinct from the CRC-32C
//! (Castagnoli) used elsewhere in this crate. `pg_crc.c` ships the 256-entry
//! lookup table `pg_crc32_table[]`; the `INIT_LEGACY_CRC32` /
//! `COMP_LEGACY_CRC32` / `FIN_LEGACY_CRC32` macros from `pg_crc.h` drive it:
//!
//!   INIT: crc = 0xFFFFFFFF
//!   COMP: for each byte b: crc = pg_crc32_table[(crc ^ b) & 0xFF] ^ (crc >> 8)
//!   FIN:  crc ^= 0xFFFFFFFF
//!
//! The table is the verbatim reflected-0xEDB88320 table from `pg_crc.c`; it is
//! generated here by the same reflected-polynomial recurrence PostgreSQL used
//! to produce the literal table, yielding byte-identical entries.

/// The legacy CRC-32 lookup table (`pg_crc32_table[256]` from `pg_crc.c`),
/// reflected polynomial 0xEDB88320.
const PG_CRC32_TABLE: [u32; 256] = build_table();

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

/// Full legacy CRC-32 of `data`: `INIT_LEGACY_CRC32` then
/// `COMP_LEGACY_CRC32(crc, data, len)` then `FIN_LEGACY_CRC32`. Pure,
/// infallible. Mirrors the `INIT/COMP/FIN_LEGACY_CRC32` macro triple a
/// consumer (e.g. `gtsvector_compress`) wraps around one lexeme's bytes.
pub fn legacy_crc32_lexeme(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc = PG_CRC32_TABLE[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Standard CRC-32 check vector: CRC-32 of "123456789" is 0xCBF43926.
    #[test]
    fn check_vector() {
        assert_eq!(legacy_crc32_lexeme(b"123456789"), 0xCBF4_3926);
    }

    /// Empty input: INIT 0xFFFFFFFF then FIN xor 0xFFFFFFFF => 0.
    #[test]
    fn empty() {
        assert_eq!(legacy_crc32_lexeme(b""), 0);
    }
}
