//! `contrib/ltree/crc32.c` — `ltree_crc32_sz`, the case-folding CRC used to
//! key lquery/ltxtquery label variants. The CRC is part of the GiST on-disk
//! format, so it must stay backwards-compatible.

use ::mcx::MemoryContext;
const DEFAULT_COLLATION_OID: ::types_core::Oid = 100;

pub fn ltree_crc32_sz(buf: &[u8]) -> u32 {
    let folded = fold(buf);
    ::crc32c::traditional_crc32(&folded)
}

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
