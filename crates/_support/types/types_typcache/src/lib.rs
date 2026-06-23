//! Type-cache vocabulary (`utils/typcache.h`): the `TypeCacheEntry` row of
//! per-type catalog information cached by `utils/cache/typcache.c`.
//!
//! Trimmed to the fields consumers ported so far actually read (the storage
//! parameters copied from the `pg_type` row); the owning unit
//! (`backend-utils-cache-typcache`) populates a richer entry when it lands.
//! Field names and `pg_type` semantics verified against `utils/typcache.h`.

#![no_std]
#![allow(non_snake_case)]
#![forbid(unsafe_code)]

use ::types_core::Oid;

/// `typstorage` value `TYPSTORAGE_PLAIN` (`pg_type.h`): never toasted.
pub const TYPSTORAGE_PLAIN: i8 = b'p' as i8;
/// `typstorage` value `TYPSTORAGE_EXTERNAL` (`pg_type.h`): toastable, no
/// compression.
pub const TYPSTORAGE_EXTERNAL: i8 = b'e' as i8;
/// `typstorage` value `TYPSTORAGE_EXTENDED` (`pg_type.h`): toastable +
/// compressible (the default for varlena).
pub const TYPSTORAGE_EXTENDED: i8 = b'x' as i8;
/// `typstorage` value `TYPSTORAGE_MAIN` (`pg_type.h`): compressible, kept in
/// the main tuple if possible.
pub const TYPSTORAGE_MAIN: i8 = b'm' as i8;

/// `TypeCacheEntry` (`utils/typcache.h`), trimmed to the subsidiary `pg_type`
/// fields its current consumers read. `type_id` is the hash key (and OID of the
/// data type); `typlen`/`typbyval`/`typalign`/`typstorage`/`typtype` are copied
/// verbatim from the `pg_type` row, as in C.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TypeCacheEntry {
    /// `type_id`: OID of the data type (the cache hash key, MUST BE FIRST in C).
    pub type_id: Oid,
    /// `typlen`: `pg_type.typlen` (-1 varlena, -2 cstring, >0 fixed).
    pub typlen: i16,
    /// `typbyval`: `pg_type.typbyval`.
    pub typbyval: bool,
    /// `typalign`: `pg_type.typalign` (`TYPALIGN_*`).
    pub typalign: i8,
    /// `typstorage`: `pg_type.typstorage` (`TYPSTORAGE_*`).
    pub typstorage: i8,
    /// `typtype`: `pg_type.typtype`.
    pub typtype: i8,
}
