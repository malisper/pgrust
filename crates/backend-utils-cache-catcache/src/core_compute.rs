//! The node-independent computational core (`catcache.c`): the hard-coded
//! hash/equality fast functions, the key-type → `(CCFastKind, eqfunc)` table
//! (`GetCCHashEqFuncs`), the position-dependent hash combine
//! (`CatalogCacheComputeHashValue`), and the key comparison
//! (`CatalogCacheCompareTuple`).
//!
//! By-reference key types (`name`, `text`, `oidvector`) need their payload
//! bytes resolved from the key `Datum`; in the owned model the key already
//! crosses the seam as a [`SysCacheKey`], so the fast functions take the
//! resolved bytes / scalar directly rather than dereferencing a pointer.

use types_cache::backend_utils_cache_catcache::CCFastKind;
use types_core::Oid;
use types_datum::Datum;
use types_error::PgResult;

/* ----------------------------------------------------------------------------
 * `RegProcedure` OIDs for the supported catcache key equality operators
 * (`utils/fmgroids.h`, generated from pg_proc.dat).
 * ------------------------------------------------------------------------- */
pub const F_BOOLEQ: Oid = 60;
pub const F_CHAREQ: Oid = 61;
pub const F_NAMEEQ: Oid = 62;
pub const F_INT2EQ: Oid = 63;
pub const F_INT4EQ: Oid = 65;
pub const F_TEXTEQ: Oid = 67;
pub const F_OIDEQ: Oid = 184;
pub const F_OIDVECTOREQ: Oid = 679;

/* ----------------------------------------------------------------------------
 * Datum scalar accessors (`postgres.h` `DatumGet*` for the by-value key types).
 * ------------------------------------------------------------------------- */

/// `DatumGetChar(d)` — the low byte as a signed `char`.
#[inline]
pub fn datum_get_char(_d: Datum) -> i8 {
    todo!("catcache::core_compute::datum_get_char")
}

/// `DatumGetInt16(d)`.
#[inline]
pub fn datum_get_int16(_d: Datum) -> i16 {
    todo!("catcache::core_compute::datum_get_int16")
}

/// `DatumGetInt32(d)`.
#[inline]
pub fn datum_get_int32(_d: Datum) -> i32 {
    todo!("catcache::core_compute::datum_get_int32")
}

/* ----------------------------------------------------------------------------
 * The hard-coded hash/equality fast functions (catcache.c).
 * ------------------------------------------------------------------------- */

/// `chareqfast` — `DatumGetChar(a) == DatumGetChar(b)`.
#[inline]
pub fn chareqfast(_a: Datum, _b: Datum) -> bool {
    todo!("catcache::core_compute::chareqfast")
}

/// `charhashfast` — `murmurhash32((int32) DatumGetChar(datum))`.
#[inline]
pub fn charhashfast(_datum: Datum) -> u32 {
    todo!("catcache::core_compute::charhashfast")
}

/// `nameeqfast` — `strncmp(ca, cb, NAMEDATALEN) == 0` over the significant bytes.
#[inline]
pub fn nameeqfast(_a: &[u8], _b: &[u8]) -> bool {
    todo!("catcache::core_compute::nameeqfast")
}

/// `namehashfast` — `hash_any((unsigned char *) key, strlen(key))`.
#[inline]
pub fn namehashfast(_key: &[u8]) -> u32 {
    todo!("catcache::core_compute::namehashfast")
}

/// `int2eqfast` — `DatumGetInt16(a) == DatumGetInt16(b)`.
#[inline]
pub fn int2eqfast(_a: Datum, _b: Datum) -> bool {
    todo!("catcache::core_compute::int2eqfast")
}

/// `int2hashfast` — `murmurhash32((int32) DatumGetInt16(datum))`.
#[inline]
pub fn int2hashfast(_datum: Datum) -> u32 {
    todo!("catcache::core_compute::int2hashfast")
}

/// `int4eqfast` — `DatumGetInt32(a) == DatumGetInt32(b)`.
#[inline]
pub fn int4eqfast(_a: Datum, _b: Datum) -> bool {
    todo!("catcache::core_compute::int4eqfast")
}

/// `int4hashfast` — `murmurhash32((int32) DatumGetInt32(datum))`.
#[inline]
pub fn int4hashfast(_datum: Datum) -> u32 {
    todo!("catcache::core_compute::int4hashfast")
}

/// `texteqfast` — the deterministic fast path reduces `texteq` to byte
/// equality of the two detoasted payloads.
#[inline]
pub fn texteqfast(_a: &[u8], _b: &[u8]) -> bool {
    todo!("catcache::core_compute::texteqfast")
}

/// `texthashfast` — `hashtext` for a deterministic collation is
/// `hash_any(payload)` of the detoasted `text`.
#[inline]
pub fn texthashfast(_payload: &[u8]) -> u32 {
    todo!("catcache::core_compute::texthashfast")
}

/// `oidvectoreqfast` — element-by-element equality of two 1-D no-null vectors.
#[inline]
pub fn oidvectoreqfast(_a: &[Oid], _b: &[Oid]) -> bool {
    todo!("catcache::core_compute::oidvectoreqfast")
}

/// `oidvectorhashfast` — `hashoidvector` is `hash_any` over the contiguous
/// `Oid` element bytes.
pub fn oidvectorhashfast(_v: &[Oid]) -> PgResult<u32> {
    todo!("catcache::core_compute::oidvectorhashfast")
}

/* ----------------------------------------------------------------------------
 * Fast-function dispatch over a `CCFastKind` (the C `cc_hashfunc[i]` /
 * `cc_fastequal[i]` indirect call). The argument is the resolved key, since
 * the search key crossed as a `SysCacheKey`.
 * ------------------------------------------------------------------------- */

/// Apply the fast hash function for a key kind to one key datum.
pub fn fast_hash(_kind: CCFastKind, _datum: Datum) -> PgResult<u32> {
    todo!("catcache::core_compute::fast_hash")
}

/// Apply the fast equality function for a key kind to two key datums.
pub fn fast_eq(_kind: CCFastKind, _a: Datum, _b: Datum) -> PgResult<bool> {
    todo!("catcache::core_compute::fast_eq")
}

/* ----------------------------------------------------------------------------
 * GetCCHashEqFuncs — the key-type → `(CCFastKind, eqfunc RegProcedure)` table.
 * ------------------------------------------------------------------------- */

/// `GetCCHashEqFuncs(keytype, &hashfunc, &eqfunc, &fasteqfunc)`. An unsupported
/// key type is the C `elog(FATAL, "type %u not supported as catcache key")`.
pub fn GetCCHashEqFuncs(_keytype: Oid) -> PgResult<(CCFastKind, Oid)> {
    todo!("catcache::core_compute::GetCCHashEqFuncs")
}

/* ----------------------------------------------------------------------------
 * Hash computation + comparison (catcache.c).
 * ------------------------------------------------------------------------- */

/// `pg_rotate_left32(word, n)` (`port/pg_bitutils.h`).
#[inline]
pub fn pg_rotate_left32(word: u32, n: u32) -> u32 {
    word.rotate_left(n)
}

/// `CatalogCacheComputeHashValue(cache, nkeys, v1..v4)` — the position-dependent
/// rotate-and-XOR combine. Out-of-range `nkeys` is the C `elog(FATAL, "wrong
/// number of hash keys")`.
pub fn CatalogCacheComputeHashValue(
    _kinds: &[CCFastKind],
    _nkeys: i32,
    _keys: &[Datum],
) -> PgResult<u32> {
    todo!("catcache::core_compute::CatalogCacheComputeHashValue")
}

/// `CatalogCacheCompareTuple(cache, nkeys, cachekeys, searchkeys)`.
pub fn CatalogCacheCompareTuple(
    _kinds: &[CCFastKind],
    _nkeys: i32,
    _cachekeys: &[Datum],
    _searchkeys: &[Datum],
) -> PgResult<bool> {
    todo!("catcache::core_compute::CatalogCacheCompareTuple")
}

/* ----------------------------------------------------------------------------
 * Small pure helpers shared by the graph/search families.
 * ------------------------------------------------------------------------- */

/// `HASH_INDEX(h, sz)` — bucket index for a power-of-two table.
#[inline]
pub fn HASH_INDEX(h: u32, sz: i32) -> usize {
    (h & (sz as u32).wrapping_sub(1)) as usize
}

/// `ItemPointerEquals(a, b)` (`itemptr.c`).
#[inline]
pub fn item_pointer_equals(
    a: types_cache::backend_utils_cache_catcache::ItemPointer,
    b: types_cache::backend_utils_cache_catcache::ItemPointer,
) -> bool {
    a == b
}
