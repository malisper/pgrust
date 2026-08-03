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
    // C crc32.c branches on `pg_newlocale_from_collation(DEFAULT_COLLATION_OID)
    // ->ctype_is_c`, NOT on the encoding width: a UTF8 database with LC_CTYPE=C
    // (initdb --locale=C --encoding=UTF8) takes the ascii-tolower arm in C.
    // Branching on the encoding width sent that very common configuration down
    // the casemap arm, changing the CRC — which is the lquery_variant `val`
    // and part of the GiST on-disk format. `database_ctype_is_c` is the same
    // datctype-derived flag the default locale's ctype_is_c carries, and it is
    // the signal ts_locale's t_isalnum already uses on these same labels.
    if ::pg_locale::database_ctype_is_c() {
        // C ctype: ascii tolower per byte.
        buf.iter().map(|&b| b.to_ascii_lowercase()).collect()
    } else {
        // DIVERGENCE (open, not witnessable under the campaign's C-ctype
        // pin): 18.3 folds this arm with pg_strfold (Unicode case FOLDING,
        // per codepoint), not formatting.c's str_tolower (SQL lower()).
        // They differ for codepoints whose fold and lowercase disagree.
        // Recorded in the lane report; the oracle abort()s on this arm under
        // the pin rather than fabricate a fold.
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
