//! Seam declarations for the `backend-utils-hash-small` unit
//! (`utils/hash/pg_crc.c` and the `common/pg_crc.h` legacy-CRC macros).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// The legacy CRC32 of one lexeme's bytes
    /// (`INIT_LEGACY_CRC32`/`COMP_LEGACY_CRC32`/`FIN_LEGACY_CRC32` over `data`),
    /// owned by `common/pg_crc.h`. `gtsvector_compress` stores the result as the
    /// lexeme's `int32` array-key hash. Pure, infallible.
    pub fn legacy_crc32_lexeme(data: &[u8]) -> u32
);
