//! CRC-32C checksum via the slicing-by-8 algorithm (`src/port/pg_crc32c_sb8.c`).
//!
//! Owns `pg_comp_crc32c_sb8`, the table-driven software CRC-32C accumulator
//! (Castagnoli polynomial 0x1EDC6F41). The `INIT_CRC32C`/`FIN_CRC32C`/
//! `EQ_CRC32C` macros from `port/pg_crc32c.h` are trivial and performed at the
//! call site; the accumulation step is the seam consumers reach through.
//!
//! Big-endian support is omitted: PostgreSQL's big-endian path keeps the
//! intermediate value byte-reversed (and stores the tables byte-reversed), but
//! every consumer in this tree runs little-endian and the seam signature is
//! the little-endian `u32`. The table carries the little-endian values only.

pub mod legacy;
mod table;

pub use legacy::{legacy_crc32_lexeme, traditional_crc32};

use table::PG_CRC32C_TABLE;

/// Accumulate one input byte (the `CRC8` macro, little-endian variant).
#[inline(always)]
fn crc8(crc: u32, x: u8) -> u32 {
    PG_CRC32C_TABLE[0][((crc ^ x as u32) & 0xFF) as usize] ^ (crc >> 8)
}

/// `pg_comp_crc32c_sb8(crc, data, len)` — fold `data` into the running CRC-32C
/// `crc` and return the new value, using the slicing-by-8 algorithm.
pub fn pg_comp_crc32c_sb8(mut crc: u32, data: &[u8]) -> u32 {
    let mut p = data;

    // Handle 0-3 initial bytes one at a time, so the slicing loop below starts
    // on a four-byte boundary relative to the input.
    while !p.is_empty() && (p.as_ptr() as usize & 3) != 0 {
        crc = crc8(crc, p[0]);
        p = &p[1..];
    }

    // Process eight bytes of data at a time.
    while p.len() >= 8 {
        let a = u32::from_le_bytes([p[0], p[1], p[2], p[3]]) ^ crc;
        let b = u32::from_le_bytes([p[4], p[5], p[6], p[7]]);

        let c0 = (b >> 24) as u8;
        let c1 = (b >> 16) as u8;
        let c2 = (b >> 8) as u8;
        let c3 = b as u8;
        let c4 = (a >> 24) as u8;
        let c5 = (a >> 16) as u8;
        let c6 = (a >> 8) as u8;
        let c7 = a as u8;

        crc = PG_CRC32C_TABLE[0][c0 as usize]
            ^ PG_CRC32C_TABLE[1][c1 as usize]
            ^ PG_CRC32C_TABLE[2][c2 as usize]
            ^ PG_CRC32C_TABLE[3][c3 as usize]
            ^ PG_CRC32C_TABLE[4][c4 as usize]
            ^ PG_CRC32C_TABLE[5][c5 as usize]
            ^ PG_CRC32C_TABLE[6][c6 as usize]
            ^ PG_CRC32C_TABLE[7][c7 as usize];

        p = &p[8..];
    }

    // Handle any remaining bytes one at a time.
    while !p.is_empty() {
        crc = crc8(crc, p[0]);
        p = &p[1..];
    }

    crc
}

/// Install every seam this crate owns.
pub fn init_seams() {
    crc32c_seams::comp_crc32c::set(pg_comp_crc32c_sb8);
    pg_crc32c_seams::pg_comp_crc32c::set(pg_comp_crc32c_sb8);
    // Legacy CRC-32 (`pg_crc.c`, combined-into this unit): full INIT/COMP/FIN
    // over one lexeme's bytes, used by `gtsvector_compress` as the array key.
    hash_small_seams::legacy_crc32_lexeme::set(legacy::legacy_crc32_lexeme);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Full INIT/COMP/FIN round trip over the standard "123456789" check
    /// vector. CRC-32C of "123456789" is 0xE3069283.
    #[test]
    fn check_vector() {
        let mut crc: u32 = 0xFFFF_FFFF;
        crc = pg_comp_crc32c_sb8(crc, b"123456789");
        crc ^= 0xFFFF_FFFF;
        assert_eq!(crc, 0xE306_9283);
    }

    /// Empty input leaves the accumulator unchanged.
    #[test]
    fn empty() {
        assert_eq!(pg_comp_crc32c_sb8(0xFFFF_FFFF, b""), 0xFFFF_FFFF);
    }

    /// `init_seams()` installs the legacy CRC-32 seam, and a `::call` through
    /// the real seam (the path `gtsvector_compress` reaches) returns the PG
    /// *legacy* (bogus normal-table/reflected-code) check vector — no "seam not
    /// installed" panic.
    #[test]
    fn legacy_seam_installed_and_returns_check_vector() {
        init_seams();
        let v = hash_small_seams::legacy_crc32_lexeme::call(b"123456789");
        assert_eq!(v, 0xC40E_D0B0);
    }

    /// Splitting the input across two COMP calls matches a single call,
    /// covering the unaligned-prefix and tail-byte paths.
    #[test]
    fn split_matches_whole() {
        let data: Vec<u8> = (0u8..=200).collect();
        let whole = pg_comp_crc32c_sb8(0xFFFF_FFFF, &data);
        let mut split = 0xFFFF_FFFFu32;
        split = pg_comp_crc32c_sb8(split, &data[..5]);
        split = pg_comp_crc32c_sb8(split, &data[5..]);
        assert_eq!(whole, split);
    }
}
