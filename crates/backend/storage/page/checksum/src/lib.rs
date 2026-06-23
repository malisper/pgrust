//! Checksum implementation for data pages.
//!
//! This is an idiomatic Rust port of PostgreSQL's
//! `src/backend/storage/page/checksum.c`, whose actual implementation lives in
//! `src/include/storage/checksum_impl.h`.
//!
//! The checksum algorithm is based on the FNV-1a hash. The page is treated as a
//! `CHECKSUM_ROWS x N_SUMS` two-dimensional array of 32-bit values; each column
//! is aggregated separately into a partial checksum using a distinct initial
//! offset, then two extra rounds of zeroes mix in the final value and the
//! partial checksums are xor-folded into a single 32-bit result.

pub use ::types_core::{BlockNumber, BLCKSZ};

/// This crate is a pure-computation leaf with no inward seam declarations;
/// nothing to install.
pub fn init_seams() {}

/// Number of checksums to calculate in parallel (`N_SUMS`).
const N_SUMS: usize = 32;

/// Number of rows in the `CHECKSUM_ROWS x N_SUMS` view of the page,
/// i.e. `BLCKSZ / (sizeof(uint32) * N_SUMS)`.
const CHECKSUM_ROWS: usize = BLCKSZ / (size_of::<u32>() * N_SUMS);

/// Prime multiplier of the FNV-1a hash (`FNV_PRIME`).
const FNV_PRIME: u32 = 16_777_619;

/// Byte offset of `pd_checksum` within `PageHeaderData`.
///
/// `PageHeaderData` starts with `pd_lsn` (an 8-byte `PageXLogRecPtr`), so the
/// `pd_checksum` `uint16` lives at byte offset 8.
const CHECKSUM_OFFSET: usize = 8;

/// Base offsets to initialize each of the parallel FNV hashes into a different
/// initial state (`checksumBaseOffsets`).
const CHECKSUM_BASE_OFFSETS: [u32; N_SUMS] = [
    0x5B1F36E9, 0xB8525960, 0x02AB50AA, 0x1DE66D2A, 0x79FF467A, 0x9BB9F8A3, 0x217E7CD2, 0x83E13D2C,
    0xF8D4474F, 0xE39EB970, 0x42C6AE16, 0x993216FA, 0x7B093B5D, 0x98DAFF3C, 0xF718902A, 0x0B1C9CDB,
    0xE58F764B, 0x187636BC, 0x5D7B3BB1, 0xE73DE7DE, 0x92BEC979, 0xCCA6C0B2, 0x304A0979, 0x85AA43D4,
    0x783125BB, 0x6CA8EAA2, 0xE407EAC6, 0x4B5CFC3E, 0x9FBF8C76, 0x15CA20BE, 0xF2CA9FD3, 0x959BD756,
];

/// Computes the checksum for an 8192-byte PostgreSQL page.
///
/// The function name matches PostgreSQL's `pg_checksum_page`; the safe Rust API
/// accepts a fixed-size page buffer instead of a raw page pointer.
///
/// The checksum includes the block number (to detect the case where a page is
/// somehow moved to a different location), the page header (excluding the
/// checksum itself), and the page data.
///
/// As in PostgreSQL, the `pd_checksum` field is transiently zeroed during the
/// calculation and then restored: updating the checksum is not part of this
/// function's contract.
pub fn pg_checksum_page(page: &mut [u8; BLCKSZ], blkno: BlockNumber) -> u16 {
    // Save pd_checksum and temporarily set it to zero, so that the checksum
    // calculation isn't affected by the old checksum stored on the page.
    // Restore it after.
    let saved_checksum = read_checksum(page);

    write_checksum(page, 0);
    let mut checksum = checksum_block(page);
    write_checksum(page, saved_checksum);

    // Mix in the block number to detect transposed pages.
    checksum ^= blkno;

    // Reduce to a u16 (to fit in the pd_checksum field) with an offset of one.
    // That avoids checksums of zero.
    (checksum % 65_535 + 1) as u16
}

/// Reads the `pd_checksum` field from the page header.
fn read_checksum(page: &[u8; BLCKSZ]) -> u16 {
    u16::from_ne_bytes([page[CHECKSUM_OFFSET], page[CHECKSUM_OFFSET + 1]])
}

/// Writes `checksum` into the `pd_checksum` field of the page header.
fn write_checksum(page: &mut [u8; BLCKSZ], checksum: u16) {
    let bytes = checksum.to_ne_bytes();
    page[CHECKSUM_OFFSET] = bytes[0];
    page[CHECKSUM_OFFSET + 1] = bytes[1];
}

/// Block checksum algorithm (`pg_checksum_block`).
fn checksum_block(page: &[u8; BLCKSZ]) -> u32 {
    // Initialize partial checksums to their corresponding offsets.
    let mut sums = CHECKSUM_BASE_OFFSETS;
    let mut words = page.chunks_exact(size_of::<u32>());

    // Main checksum calculation.
    for _ in 0..CHECKSUM_ROWS {
        for sum in &mut sums {
            let word = words.next().expect("checksum rows cover full page");
            let value = u32::from_ne_bytes([word[0], word[1], word[2], word[3]]);
            checksum_comp(sum, value);
        }
    }

    // Finally add in two rounds of zeroes for additional mixing.
    for _ in 0..2 {
        for sum in &mut sums {
            checksum_comp(sum, 0);
        }
    }

    // Xor-fold partial checksums together.
    sums.into_iter().fold(0, |acc, sum| acc ^ sum)
}

/// Calculate one round of the checksum (`CHECKSUM_COMP`).
#[inline(always)]
fn checksum_comp(checksum: &mut u32, value: u32) {
    let mixed = *checksum ^ value;
    *checksum = mixed.wrapping_mul(FNV_PRIME) ^ (mixed >> 17);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn patterned_page() -> [u8; BLCKSZ] {
        let mut page = [0; BLCKSZ];
        for (idx, byte) in page.iter_mut().enumerate() {
            *byte = (idx.wrapping_mul(37).wrapping_add(11) & 0xff) as u8;
        }
        page
    }

    #[test]
    fn checksum_is_deterministic() {
        let mut page = patterned_page();
        let mut same_page = page;

        assert_eq!(
            pg_checksum_page(&mut page, 42),
            pg_checksum_page(&mut same_page, 42)
        );
    }

    #[test]
    fn checksum_depends_on_block_number() {
        let mut page = patterned_page();
        let mut same_page = page;

        assert_ne!(
            pg_checksum_page(&mut page, 1),
            pg_checksum_page(&mut same_page, 2)
        );
    }

    #[test]
    fn checksum_field_is_restored() {
        let mut page = patterned_page();
        write_checksum(&mut page, 0xace1);

        let _ = pg_checksum_page(&mut page, 99);

        assert_eq!(read_checksum(&page), 0xace1);
    }

    #[test]
    fn checksum_ignores_existing_checksum_field() {
        let mut page = patterned_page();
        let mut same_page = page;
        write_checksum(&mut page, 0x1111);
        write_checksum(&mut same_page, 0x2222);

        assert_eq!(
            pg_checksum_page(&mut page, 7),
            pg_checksum_page(&mut same_page, 7)
        );
    }

    /// The checksum is never zero: the algorithm reduces modulo 65535 and adds
    /// one, so the result is always in the range 1..=65535.
    #[test]
    fn checksum_is_never_zero() {
        let mut page = [0u8; BLCKSZ];
        for blkno in 0..256 {
            assert_ne!(pg_checksum_page(&mut page, blkno), 0);
        }
    }
}
