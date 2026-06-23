//! Seam declaration for the CRC-32C primitive (`src/port/pg_crc32c*.c`,
//! catalog unit `port-batch23`). A pure, deterministic computation with no
//! failure surface; consumers accumulate file/WAL CRCs through it.
//!
//! Installed by the owning port unit's `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `COMP_CRC32C(crc, data, len)` — accumulate `data` into the running
    /// CRC-32C `crc` (Castagnoli polynomial). The caller seeds with
    /// `INIT_CRC32C` (0xFFFFFFFF) and finalizes with `FIN_CRC32C` (XOR
    /// 0xFFFFFFFF). Pure; cannot fail.
    pub fn comp_crc32c(crc: u32, data: &[u8]) -> u32
);
