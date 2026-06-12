//! Fmgr-adjacent catalog limits (`pg_config_manual.h`).
//!
//! Populated incrementally from ../pgrust/src-idiomatic/crates/types/src/fmgr.rs
//! as ports need items; only the items currently consumed are present.

pub const INDEX_MAX_KEYS: i32 = 32;
pub const NAMEDATALEN: i32 = 64;

/// `FmgrInfo` (`fmgr.h`), trimmed to the lookup key. C's struct caches the
/// resolved function pointer and call metadata; consumers here (e.g.
/// `ScanKeyInit`) only stamp `fn_oid` and defer the real fmgr lookup to the
/// code that invokes the function.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FmgrInfo {
    /// OID of the function (`pg_proc` OID).
    pub fn_oid: crate::primitive::Oid,
}

impl FmgrInfo {
    /// An unresolved `FmgrInfo` (`fn_oid = InvalidOid`).
    pub const fn empty() -> Self {
        Self { fn_oid: 0 }
    }
}
