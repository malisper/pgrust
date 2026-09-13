//! `contrib/ltree/crc32.c` — `ltree_crc32_sz`, the case-folding CRC used to
//! key lquery/ltxtquery label variants. The CRC is part of the GiST on-disk
//! format, so it must stay backwards-compatible.

use ::mcx::MemoryContext;
const DEFAULT_COLLATION_OID: ::types_core::Oid = 100;

pub fn ltree_crc32_sz(buf: &[u8]) -> u32 {
    let folded = fold(buf);
    ::crc32c::traditional_crc32(&folded)
}

// C crc32.c / lquery_op.c branch on `pg_newlocale_from_collation(
// DEFAULT_COLLATION_OID)->ctype_is_c`, not on datctype: an ICU default
// locale answers false even with LC_CTYPE=C. A lookup failure is
// unreachable once the default locale is stamped at boot; fall back to the
// C arm so the CRC stays deterministic.
pub(crate) fn default_ctype_is_c() -> bool {
    ::pg_locale::pg_newlocale_from_collation(DEFAULT_COLLATION_OID)
        .map(|l| l.ctype_is_c)
        .unwrap_or(true)
}

pub fn fold(buf: &[u8]) -> Vec<u8> {
    if default_ctype_is_c() {
        // C ctype: ascii tolower per byte.
        buf.iter().map(|&b| b.to_ascii_lowercase()).collect()
    } else {
        // crc32.c:47-61 / lquery_op.c:123-141: the non-C-ctype arm case-FOLDS
        // with pg_strfold under the default collation's locale, one codepoint
        // at a time (pg_mblen_range + UNICODE_CASEMAP_BUFSZ scratch), not
        // formatting.c's str_tolower (SQL lower()). Folding and lowercasing
        // differ where the fold mapping is not the lowercase mapping (final
        // sigma -> sigma, sharp s -> "ss" under full folding, Cherokee, ...),
        // and both the CRC (the lquery_variant `val`, GiST on-disk) and the
        // '@' label match go through here.
        match fold_strfold(buf) {
            Ok(v) => v,
            // Unreachable on a validated label (the default locale is loaded
            // at boot; pg_mblen_range only fails on a truncated multibyte
            // tail): keep the CRC deterministic rather than fabricate a fold.
            Err(_) => buf.iter().map(|&b| b.to_ascii_lowercase()).collect(),
        }
    }
}

/// `UNICODE_CASEMAP_BUFSZ` (pg_locale.h:39) = UNICODE_CASEMAP_LEN (3) *
/// MAX_MULTIBYTE_CHAR_LEN (4): the per-codepoint fold scratch of crc32.c.
const UNICODE_CASEMAP_BUFSZ: usize = 3 * 4;

fn fold_strfold(buf: &[u8]) -> ::types_error::PgResult<Vec<u8>> {
    let locale = ::pg_locale::pg_newlocale_from_collation(DEFAULT_COLLATION_OID)?;
    let m = MemoryContext::new("ltree crc fold scratch");
    let mut out = Vec::with_capacity(buf.len());
    let mut p = buf;
    while !p.is_empty() {
        let srclen = ::mbutils::pg_mblen_range(p)? as usize;
        // C hands pg_strfold a UNICODE_CASEMAP_BUFSZ buffer and consumes the
        // returned length (+1 here for the NUL the providers append).
        let mut foldstr = [0u8; UNICODE_CASEMAP_BUFSZ + 1];
        let foldlen = ::pg_locale::pg_strfold(m.mcx(), &mut foldstr, &p[..srclen], locale)?;
        out.extend_from_slice(&foldstr[..foldlen.min(UNICODE_CASEMAP_BUFSZ)]);
        p = &p[srclen..];
    }
    Ok(out)
}
