//! Seam declarations for `port/pg_crc32c*.c` — the CRC-32C primitive.
//!
//! `INIT_CRC32C(crc)` is `crc = 0xFFFFFFFF`, `FIN_CRC32C(crc)` is
//! `crc ^= 0xFFFFFFFF`, and `EQ_CRC32C(a, b)` is `a == b` — all trivial and
//! done at the call site. The accumulation step `COMP_CRC32C(crc, data, len)`
//! dispatches to a hardware/software implementation chosen at startup, so it
//! is the seam.

seam_core::seam!(
    /// `pg_comp_crc32c(crc, data, len)` (`port/pg_crc32c.h`) — fold `data`
    /// into the running CRC-32C and return the new value.
    pub fn pg_comp_crc32c(crc: u32, data: &[u8]) -> u32
);
