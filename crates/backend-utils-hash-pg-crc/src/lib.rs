//! Idiomatic 1:1 port of the SQL-callable functions in
//! `src/backend/utils/hash/pg_crc.c`: `crc32(bytea)` and `crc32c(bytea)`.
//!
//! Reference: `postgres-18.3/src/backend/utils/hash/pg_crc.c`.
//!
//! The C file owns the `pg_crc32_table[]` lookup table and these two
//! SQL-callable bodies; the table itself (and the running-accumulator
//! algorithms behind the `INIT/COMP/FIN_TRADITIONAL_CRC32` and
//! `INIT/COMP/FIN_CRC32C` macro triples) live in `port-crc32c`, which this
//! crate drives.
//!
//! # Faithfulness notes
//!
//! * **`crc32_bytea`** runs the input through the *traditional* CRC-32 macros:
//!   `INIT_TRADITIONAL_CRC32(crc)` (crc = 0xFFFFFFFF), `COMP_TRADITIONAL_CRC32`
//!   over `VARDATA_ANY`/`VARSIZE_ANY_EXHDR`, then `FIN_TRADITIONAL_CRC32`
//!   (crc ^= 0xFFFFFFFF). `COMP_TRADITIONAL_CRC32` is `COMP_CRC32_NORMAL_TABLE`
//!   over `pg_crc32_table[]` — `tab = (crc ^ byte) & 0xFF; crc = table[tab] ^
//!   (crc >> 8)` — which over the reflected-0xEDB88320 `pg_crc32_table[]` is the
//!   standard reflected CRC-32. That is exactly
//!   [`port_crc32c::legacy::legacy_crc32_lexeme`] (same table, same normal-read
//!   recurrence, same INIT/FIN), so this body delegates to it.
//!
//! * **`crc32c_bytea`** runs the input through the CRC-32C macros:
//!   `INIT_CRC32C(crc)` (0xFFFFFFFF), `COMP_CRC32C` over `VARDATA_ANY`, then
//!   `FIN_CRC32C` (crc ^= 0xFFFFFFFF). The accumulation step is
//!   [`port_crc32c::pg_comp_crc32c_sb8`]; INIT and FIN are performed here at the
//!   call site, matching `pg_crc32c.h`.
//!
//! Both return `pg_crc32` (a `uint32`) via `PG_RETURN_INT64`, which
//! zero-extends to `int8` (handled at the fmgr boundary in [`fmgr_builtins`]).

pub mod fmgr_builtins;

/// `crc32_bytea(PG_FUNCTION_ARGS)` value core: traditional CRC-32 of `in_bytes`
/// (the detoasted `VARDATA_ANY` payload).
pub fn crc32_bytea(in_bytes: &[u8]) -> u32 {
    port_crc32c::legacy::legacy_crc32_lexeme(in_bytes)
}

/// `crc32c_bytea(PG_FUNCTION_ARGS)` value core: CRC-32C of `in_bytes` — the
/// `INIT_CRC32C` / `COMP_CRC32C` / `FIN_CRC32C` macro triple.
pub fn crc32c_bytea(in_bytes: &[u8]) -> u32 {
    /* INIT_CRC32C(crc) */
    let crc = 0xFFFF_FFFFu32;
    /* COMP_CRC32C(crc, VARDATA_ANY(in), VARSIZE_ANY_EXHDR(in)) */
    let crc = port_crc32c::pg_comp_crc32c_sb8(crc, in_bytes);
    /* FIN_CRC32C(crc) */
    crc ^ 0xFFFF_FFFF
}

/// Register this crate's two SQL-callable fmgr builtins. Called from the startup
/// aggregator (`seams-init`). `pg_crc.c` declares no outward seams of its own.
pub fn init_seams() {
    fmgr_builtins::register_pg_crc_builtins();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Standard CRC-32 check vector: "123456789" -> 0xCBF43926.
    #[test]
    fn crc32_check_vector() {
        assert_eq!(crc32_bytea(b"123456789"), 0xCBF4_3926);
    }

    /// `crc32('')` is 0, `crc32('The quick brown fox jumps over the lazy
    /// dog.')` is 1368401385 (per the `strings` regression test).
    #[test]
    fn crc32_regress_vectors() {
        assert_eq!(crc32_bytea(b""), 0);
        assert_eq!(
            crc32_bytea(b"The quick brown fox jumps over the lazy dog.") as i64,
            1368401385
        );
    }

    /// Standard CRC-32C (Castagnoli) check vector: "123456789" -> 0xE3069283.
    #[test]
    fn crc32c_check_vector() {
        assert_eq!(crc32c_bytea(b"123456789"), 0xE306_9283);
    }

    /// `crc32c('')` is 0, `crc32c('The quick brown fox jumps over the lazy
    /// dog.')` is 1368401385 in the regress test... actually 419469235.
    #[test]
    fn crc32c_regress_vectors() {
        assert_eq!(crc32c_bytea(b""), 0);
        assert_eq!(
            crc32c_bytea(b"The quick brown fox jumps over the lazy dog.") as i64,
            419469235
        );
    }
}
