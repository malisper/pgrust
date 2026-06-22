//! Legacy CRC-32 (`src/backend/utils/hash/pg_crc.c` + `src/include/utils/pg_crc.h`).
//!
//! PostgreSQL's "legacy" CRC-32 is the *bogus pre-9.5* algorithm: it drives the
//! "normal" `pg_crc32_table[]` (the reflected-0xEDB88320 table) with the
//! **reflected** Sarwate loop. As the `pg_crc.h` comment puts it, "we use the
//! 'normal' table, but with 'reflected' code. That's bogus, but it was like that
//! for years before anyone noticed. It does not correspond to any polynomial."
//! It is NOT the standard reflected CRC-32 (zlib/Ethernet) and NOT CRC-32C.
//!
//! The `INIT_LEGACY_CRC32` / `COMP_LEGACY_CRC32` (`COMP_CRC32_REFLECTED_TABLE`) /
//! `FIN_LEGACY_CRC32` macros from `pg_crc.h` drive it:
//!
//!   INIT: crc = 0xFFFFFFFF
//!   COMP: for each byte b:
//!           idx = ((crc >> 24) ^ b) & 0xFF
//!           crc = pg_crc32_table[idx] ^ (crc << 8)
//!   FIN:  crc ^= 0xFFFFFFFF
//!
//! (Compare `COMP_TRADITIONAL_CRC32`, the *standard* reflected CRC-32, which is
//! the `crc >> 8` / low-byte loop. tsquery `valcrc`, tsvector GIN/GiST
//! signatures, etc. all use the LEGACY variant, so they must use this loop —
//! the earlier `>> 8` implementation here computed the standard CRC and put
//! tsquery operands in the wrong sort order.)
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
    // COMP_CRC32_REFLECTED_TABLE: the bogus "normal table, reflected code" loop.
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        let idx = (((crc >> 24) ^ b as u32) & 0xFF) as usize;
        crc = PG_CRC32_TABLE[idx] ^ (crc << 8);
    }
    crc ^ 0xFFFF_FFFF
}

/// Full *traditional* CRC-32 of `data`: `INIT_TRADITIONAL_CRC32` then
/// `COMP_TRADITIONAL_CRC32` (`COMP_CRC32_NORMAL_TABLE`) then
/// `FIN_TRADITIONAL_CRC32`. This is the standard reflected CRC-32
/// (zlib/Ethernet) — `tab = (crc ^ byte) & 0xFF; crc = table[tab] ^ (crc >> 8)`
/// over the reflected-0xEDB88320 table. It is the algorithm behind the
/// SQL-callable `crc32(bytea)` builtin, and is DISTINCT from the bogus
/// [`legacy_crc32_lexeme`] used by tsquery/tsvector.
pub fn traditional_crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc = PG_CRC32_TABLE[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    /// PG legacy (bogus normal-table/reflected-code) CRC of "123456789".
    /// NOT the standard 0xCBF43926 — this variant yields 0xC40ED0B0.
    #[test]
    fn check_vector() {
        assert_eq!(legacy_crc32_lexeme(b"123456789"), 0xC40E_D0B0);
    }

    /// Empty input: INIT 0xFFFFFFFF then FIN xor 0xFFFFFFFF => 0.
    #[test]
    fn empty() {
        assert_eq!(legacy_crc32_lexeme(b""), 0);
    }
}

