//! Port of `contrib/citext/citext.c` — the case-insensitive `citext` type.
//!
//! `citext` reuses `text`'s I/O (`textin`/`textout`/`textrecv`/`textsend`,
//! created by the SQL install script); the only C code is the
//! comparison/hash/aggregate functions, each lowercasing its operand(s)
//! before comparing. `str_tolower` always folds under
//! [`DEFAULT_COLLATION_OID`] (not the call collation), so equality/hashing
//! are collation-independent — see the C comment in `citextcmp` this mirrors.

pub mod builtins;
pub use builtins::init_seams;

use mcx::MemoryContext;
use types_core::Oid;
use types_error::PgResult;

/// `DEFAULT_COLLATION_OID` (catalog/pg_collation_d.h) — the fixed collation
/// `str_tolower` folds under, independent of the operands' actual collation.
const DEFAULT_COLLATION_OID: Oid = 100;

fn lower(buff: &[u8]) -> PgResult<Vec<u8>> {
    let scratch = MemoryContext::new("citext lower");
    let folded = formatting_seams::str_tolower::call(scratch.mcx(), buff, DEFAULT_COLLATION_OID)?;
    Ok(folded.as_slice().to_vec())
}

/// `citextcmp` (citext.c): lowercase both operands under
/// `DEFAULT_COLLATION_OID`, then `varstr_cmp` the folded strings under the
/// *call* collation `collid`.
pub fn citextcmp(left: &[u8], right: &[u8], collid: Oid) -> PgResult<i32> {
    let lcstr = lower(left)?;
    let rcstr = lower(right)?;
    varlena::varstr_cmp(&lcstr, &rcstr, collid)
}

/// `internal_citext_pattern_cmp` (citext.c): byte comparison of the
/// lowercased operands with a length tiebreak (no collation).
pub fn citext_pattern_cmp(left: &[u8], right: &[u8]) -> PgResult<i32> {
    let lcstr = lower(left)?;
    let rcstr = lower(right)?;
    let cmp_len = lcstr.len().min(rcstr.len());
    Ok(match lcstr[..cmp_len].cmp(&rcstr[..cmp_len]) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Greater => 1,
        core::cmp::Ordering::Equal => match lcstr.len().cmp(&rcstr.len()) {
            core::cmp::Ordering::Less => -1,
            core::cmp::Ordering::Greater => 1,
            core::cmp::Ordering::Equal => 0,
        },
    })
}

/// `citext_eq`/`citext_ne` core: equality needs no call collation (C compares
/// the folded strings with `strcmp`).
pub fn citext_eq(left: &[u8], right: &[u8]) -> PgResult<bool> {
    Ok(lower(left)? == lower(right)?)
}

/// `citext_hash`: `hash_any(lower(txt))`.
pub fn citext_hash(txt: &[u8]) -> PgResult<u32> {
    Ok(hashfn::hash_bytes(&lower(txt)?))
}

/// `citext_hash_extended`.
pub fn citext_hash_extended(txt: &[u8], seed: u64) -> PgResult<u64> {
    Ok(hashfn::hash_bytes_extended(&lower(txt)?, seed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::sync::Once;

    // str_tolower(DEFAULT_COLLATION_OID) always resolves a locale (even the
    // "C" one) through pg_locale, which reads MyDatabaseId's pg_database
    // row. Seam .set() calls are process-global set-once — install them once
    // for the whole test binary — then each test thread supplies its own
    // fixed libc/C row through this thread-local (init_database_collation's
    // cached locale is itself thread-local, so this is safe per test thread).
    thread_local! {
        static DB_ROW: Cell<bool> = const { Cell::new(false) };
    }

    fn init() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            adt_formatting::init_seams();
            pg_database_seams::search_database_syscache::set(|mcx, dboid| {
                if !DB_ROW.with(Cell::get) {
                    return Ok(None);
                }
                let s = |v: &str| mcx::PgString::from_str_in(v, mcx);
                Ok(Some(pg_database_seams::PgDatabaseForm {
                    oid: dboid,
                    datname: s("testdb")?,
                    datdba: 10,
                    datistemplate: false,
                    dattablespace: 1663,
                    datallowconn: true,
                    dathasloginevt: false,
                    datconnlimit: -1,
                    datfrozenxid: 0,
                    datminmxid: 0,
                    encoding: 6,
                    datlocprovider: pg_database_seams::COLLPROVIDER_LIBC,
                    datcollate: s("C")?,
                    datctype: s("C")?,
                    datlocale: None,
                    daticurules: None,
                    datcollversion: None,
                }))
            });
        });
        DB_ROW.with(|c| c.set(true));
        pg_locale::init_database_collation().unwrap();
    }

    const C_COLLATION: Oid = types_core::C_COLLATION_OID;

    #[test]
    fn eq_is_case_insensitive() {
        init();
        assert!(citext_eq(b"Hello", b"HELLO").unwrap());
        assert!(!citext_eq(b"Hello", b"World").unwrap());
    }

    #[test]
    fn cmp_orders_case_insensitively() {
        init();
        assert_eq!(citextcmp(b"abc", b"ABC", C_COLLATION).unwrap(), 0);
        assert!(citextcmp(b"abc", b"abd", C_COLLATION).unwrap() < 0);
    }

    #[test]
    fn pattern_cmp_uses_length_tiebreak() {
        init();
        assert_eq!(citext_pattern_cmp(b"ab", b"AB").unwrap(), 0);
        assert!(citext_pattern_cmp(b"AB", b"ABC").unwrap() < 0);
        assert!(citext_pattern_cmp(b"ABC", b"AB").unwrap() > 0);
    }

    #[test]
    fn hash_is_case_insensitive() {
        init();
        assert_eq!(citext_hash(b"Foo").unwrap(), citext_hash(b"foo").unwrap());
        assert_eq!(
            citext_hash_extended(b"Foo", 42).unwrap(),
            citext_hash_extended(b"foo", 42).unwrap()
        );
    }
}
