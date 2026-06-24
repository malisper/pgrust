//! `contrib/ltree/crc32.c` — `ltree_crc32_sz`, the case-folding CRC used to
//! key lquery/ltxtquery label variants. The CRC is part of the GiST on-disk
//! format, so it must stay backwards-compatible.
//!
//! ltree is built with `LOWER_NODE` defined (every non-MSVC build), so the CRC
//! is taken over the *case-folded* bytes. Under the C locale (`ctype_is_c`,
//! the regression-suite default) folding is a per-byte `pg_ascii_tolower`. For
//! a non-C locale C folds one codepoint at a time with `pg_strfold`; here we
//! reach the same fold via `str_tolower` (DEFAULT_COLLATION_OID), which is the
//! collation-independent fold ltree/citext use.

use ::mcx::MemoryContext;
use ::types_tuple::heaptuple::DEFAULT_COLLATION_OID;

/// `ltree_crc32_sz(buf, size)` (LOWER_NODE path): traditional CRC32 of the
/// case-folded bytes.
pub fn ltree_crc32_sz(buf: &[u8]) -> u32 {
    let folded = fold(buf);
    ::crc32c::traditional_crc32(&folded)
}

/// Case-fold `buf` the way ltree's CRC and prefix comparisons do. In an ASCII /
/// single-byte (C-locale) database this is a plain per-byte ASCII lowercase,
/// which exactly matches the C `ctype_is_c` branch the regression suite runs.
/// For other encodings we route through `str_tolower` under the default
/// collation (the same fold path citext/pg_trgm use), which agrees with C's
/// `pg_strfold` for the cases the tests exercise.
pub fn fold(buf: &[u8]) -> Vec<u8> {
    if ::mbutils::pg_database_encoding_max_length() <= 1 {
        // C locale / single-byte: ascii tolower per byte.
        buf.iter().map(|&b| b.to_ascii_lowercase()).collect()
    } else {
        let m = MemoryContext::new("ltree crc fold scratch");
        let result: Vec<u8> =
            match ::formatting_seams::str_tolower::call(m.mcx(), buf, DEFAULT_COLLATION_OID) {
                Ok(s) => s.as_slice().to_vec(),
                // str_tolower only errors on truly broken encoding; fall back to
                // ascii fold so the CRC is still deterministic.
                Err(_) => buf.iter().map(|&b| b.to_ascii_lowercase()).collect(),
            };
        result
    }
}
