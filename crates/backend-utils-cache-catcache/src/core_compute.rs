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
 * Key-type OIDs (`pg_type.dat`) used by `GetCCHashEqFuncs`.
 * ------------------------------------------------------------------------- */
const BOOLOID: Oid = 16;
const CHAROID: Oid = 18;
const NAMEOID: Oid = 19;
const INT2OID: Oid = 21;
const INT4OID: Oid = 23;
const TEXTOID: Oid = 25;
const OIDOID: Oid = 26;
const OIDVECTOROID: Oid = 30;
const REGPROCOID: Oid = 24;
const REGPROCEDUREOID: Oid = 2202;
const REGOPEROID: Oid = 2203;
const REGOPERATOROID: Oid = 2204;
const REGCLASSOID: Oid = 2205;
const REGTYPEOID: Oid = 2206;
const REGCOLLATIONOID: Oid = 4191;
const REGCONFIGOID: Oid = 3734;
const REGDICTIONARYOID: Oid = 3769;
const REGROLEOID: Oid = 4096;
const REGNAMESPACEOID: Oid = 4089;

/// `NAMEDATALEN` (`pg_config_manual.h`): fixed width of a `name` field.
pub const NAMEDATALEN: usize = 64;

/* ----------------------------------------------------------------------------
 * `murmurhash32` (`common/hashfn.h`): the inline 32-bit integer mixer used by
 * the by-value fast hash functions.
 * ------------------------------------------------------------------------- */

/// `murmurhash32(data)` (`common/hashfn.h`).
#[inline]
pub fn murmurhash32(data: u32) -> u32 {
    let mut h: u32 = data;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h
}

/* ----------------------------------------------------------------------------
 * Datum scalar accessors (`postgres.h` `DatumGet*` for the by-value key types).
 * ------------------------------------------------------------------------- */

/// `DatumGetChar(d)` — the low byte as a signed `char`.
#[inline]
pub fn datum_get_char(d: Datum) -> i8 {
    d.as_char()
}

/// `DatumGetInt16(d)`.
#[inline]
pub fn datum_get_int16(d: Datum) -> i16 {
    d.as_i16()
}

/// `DatumGetInt32(d)`.
#[inline]
pub fn datum_get_int32(d: Datum) -> i32 {
    d.as_i32()
}

/* ----------------------------------------------------------------------------
 * The hard-coded hash/equality fast functions (catcache.c).
 * ------------------------------------------------------------------------- */

/// `chareqfast` — `DatumGetChar(a) == DatumGetChar(b)`.
#[inline]
pub fn chareqfast(a: Datum, b: Datum) -> bool {
    datum_get_char(a) == datum_get_char(b)
}

/// `charhashfast` — `murmurhash32((int32) DatumGetChar(datum))`.
#[inline]
pub fn charhashfast(datum: Datum) -> u32 {
    // C: `murmurhash32((int32) DatumGetChar(datum))` — the signed char widens
    // to int32 (sign-extending) before the cast to the uint32 hash input.
    murmurhash32(datum_get_char(datum) as i32 as u32)
}

/// `nameeqfast` — `strncmp(ca, cb, NAMEDATALEN) == 0` over the significant bytes.
///
/// In C the keys are `Name` pointers into fixed `NAMEDATALEN`-wide,
/// NUL-padded buffers and `strncmp` stops at the first NUL or `NAMEDATALEN`.
/// Here the resolved payloads are the NUL-free significant bytes, so equality
/// of the byte slices (each already truncated at its NUL, capped at
/// `NAMEDATALEN`) reproduces `strncmp(..., NAMEDATALEN) == 0`.
#[inline]
pub fn nameeqfast(a: &[u8], b: &[u8]) -> bool {
    let la = name_significant_len(a);
    let lb = name_significant_len(b);
    a[..la] == b[..lb]
}

/// Significant length of a `name` payload: up to the first NUL, capped at
/// `NAMEDATALEN` (the bytes `strncmp(_, _, NAMEDATALEN)` / `strlen` see).
#[inline]
fn name_significant_len(key: &[u8]) -> usize {
    let cap = key.len().min(NAMEDATALEN);
    match key[..cap].iter().position(|&c| c == 0) {
        Some(nul) => nul,
        None => cap,
    }
}

/// `namehashfast` — `hash_any((unsigned char *) key, strlen(key))`.
#[inline]
pub fn namehashfast(key: &[u8]) -> u32 {
    let len = name_significant_len(key);
    // C: `hash_any(key, strlen(key))` == `UInt32GetDatum(hash_bytes(key,
    // strlen(key)))`. `hash_bytes` is owned by common-hashfn (seam panics
    // until it lands).
    common_hashfn_seams::hash_bytes::call(&key[..len])
}

/// `int2eqfast` — `DatumGetInt16(a) == DatumGetInt16(b)`.
#[inline]
pub fn int2eqfast(a: Datum, b: Datum) -> bool {
    datum_get_int16(a) == datum_get_int16(b)
}

/// `int2hashfast` — `murmurhash32((int32) DatumGetInt16(datum))`.
#[inline]
pub fn int2hashfast(datum: Datum) -> u32 {
    murmurhash32(datum_get_int16(datum) as i32 as u32)
}

/// `int4eqfast` — `DatumGetInt32(a) == DatumGetInt32(b)`.
#[inline]
pub fn int4eqfast(a: Datum, b: Datum) -> bool {
    datum_get_int32(a) == datum_get_int32(b)
}

/// `int4hashfast` — `murmurhash32((int32) DatumGetInt32(datum))`.
#[inline]
pub fn int4hashfast(datum: Datum) -> u32 {
    murmurhash32(datum_get_int32(datum) as u32)
}

/// `texteqfast` — the deterministic fast path reduces `texteq` to byte
/// equality of the two detoasted payloads.
///
/// C calls `texteq` under `DEFAULT_COLLATION_OID` purely to take the
/// deterministic fast path, which (for equal-length input) is
/// `memcmp(VARDATA_ANY(a), VARDATA_ANY(b), len) == 0` and otherwise `false`.
/// The resolved payloads here are those `VARDATA_ANY` byte images, so plain
/// slice equality reproduces it.
#[inline]
pub fn texteqfast(a: &[u8], b: &[u8]) -> bool {
    a == b
}

/// `texthashfast` — `hashtext` for a deterministic collation is
/// `hash_any(payload)` of the detoasted `text`.
#[inline]
pub fn texthashfast(payload: &[u8]) -> u32 {
    // C: `hashtext` under a deterministic collation is
    // `hash_any(VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key))`. The resolved
    // `payload` is that `VARDATA_ANY` image. `hash_bytes` owner: common-hashfn.
    common_hashfn_seams::hash_bytes::call(payload)
}

/// `oidvectoreqfast` — element-by-element equality of two 1-D no-null vectors.
///
/// C calls `oidvectoreq`, which (after the `dim1` length compare) is a
/// `memcmp` of the `Oid` element arrays. Equal-length slice equality
/// reproduces it.
#[inline]
pub fn oidvectoreqfast(a: &[Oid], b: &[Oid]) -> bool {
    a == b
}

/// `oidvectorhashfast` — `hashoidvector` is `hash_any` over the contiguous
/// `Oid` element bytes.
pub fn oidvectorhashfast(v: &[Oid]) -> PgResult<u32> {
    // C `hashoidvector`: `hash_any((unsigned char *) key->values,
    // key->dim1 * sizeof(Oid))`. Build the little-/native-endian element byte
    // image (`Oid` is a 32-bit unsigned int) and hash it. `hash_bytes` owner:
    // common-hashfn.
    let mut bytes = alloc::vec::Vec::with_capacity(v.len() * core::mem::size_of::<Oid>());
    for &oid in v {
        bytes.extend_from_slice(&oid.to_ne_bytes());
    }
    Ok(common_hashfn_seams::hash_bytes::call(&bytes))
}

/* ----------------------------------------------------------------------------
 * Fast-function dispatch over a `CCFastKind` (the C `cc_hashfunc[i]` /
 * `cc_fastequal[i]` indirect call). The argument is the resolved key, since
 * the search key crossed as a `SysCacheKey`.
 *
 * Only the by-value kinds (`Char`, `Int2`, `Int4`) are dispatchable from a
 * bare scalar `Datum`. The by-reference kinds (`Name`, `Text`, `OidVector`)
 * carry their payload as resolved bytes, not in the `Datum` word, so they are
 * served by the byte/slice fast functions ([`namehashfast`], [`texthashfast`],
 * [`oidvectorhashfast`], [`nameeqfast`], [`texteqfast`], [`oidvectoreqfast`])
 * which callers reach directly with the resolved payload — they are not
 * routed through this scalar dispatch.
 * ------------------------------------------------------------------------- */

/// Apply the fast hash function for a by-value key kind to one key datum.
pub fn fast_hash(kind: CCFastKind, datum: Datum) -> PgResult<u32> {
    match kind {
        CCFastKind::Char => Ok(charhashfast(datum)),
        CCFastKind::Int2 => Ok(int2hashfast(datum)),
        CCFastKind::Int4 => Ok(int4hashfast(datum)),
        CCFastKind::Name | CCFastKind::Text | CCFastKind::OidVector => panic!(
            "catcache::core_compute::fast_hash: by-reference key kind {kind:?} \
             must be hashed from its resolved payload bytes \
             (namehashfast/texthashfast/oidvectorhashfast), not the scalar Datum dispatch"
        ),
    }
}

/// Apply the fast equality function for a by-value key kind to two key datums.
pub fn fast_eq(kind: CCFastKind, a: Datum, b: Datum) -> PgResult<bool> {
    match kind {
        CCFastKind::Char => Ok(chareqfast(a, b)),
        CCFastKind::Int2 => Ok(int2eqfast(a, b)),
        CCFastKind::Int4 => Ok(int4eqfast(a, b)),
        CCFastKind::Name | CCFastKind::Text | CCFastKind::OidVector => panic!(
            "catcache::core_compute::fast_eq: by-reference key kind {kind:?} \
             must be compared from its resolved payload bytes \
             (nameeqfast/texteqfast/oidvectoreqfast), not the scalar Datum dispatch"
        ),
    }
}

/* ----------------------------------------------------------------------------
 * GetCCHashEqFuncs — the key-type → `(CCFastKind, eqfunc RegProcedure)` table.
 * ------------------------------------------------------------------------- */

/// `GetCCHashEqFuncs(keytype, &hashfunc, &eqfunc, &fasteqfunc)`. An unsupported
/// key type is the C `elog(FATAL, "type %u not supported as catcache key")`.
pub fn GetCCHashEqFuncs(keytype: Oid) -> PgResult<(CCFastKind, Oid)> {
    let pair = match keytype {
        BOOLOID => (CCFastKind::Char, F_BOOLEQ),
        CHAROID => (CCFastKind::Char, F_CHAREQ),
        NAMEOID => (CCFastKind::Name, F_NAMEEQ),
        INT2OID => (CCFastKind::Int2, F_INT2EQ),
        INT4OID => (CCFastKind::Int4, F_INT4EQ),
        TEXTOID => (CCFastKind::Text, F_TEXTEQ),
        OIDOID | REGPROCOID | REGPROCEDUREOID | REGOPEROID | REGOPERATOROID | REGCLASSOID
        | REGTYPEOID | REGCOLLATIONOID | REGCONFIGOID | REGDICTIONARYOID | REGROLEOID
        | REGNAMESPACEOID => (CCFastKind::Int4, F_OIDEQ),
        OIDVECTOROID => (CCFastKind::OidVector, F_OIDVECTOREQ),
        _ => {
            // C: `elog(FATAL, "type %u not supported as catcache key", keytype)`.
            panic!("type {keytype} not supported as catcache key");
        }
    };
    Ok(pair)
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
    kinds: &[CCFastKind],
    nkeys: i32,
    keys: &[Datum],
) -> PgResult<u32> {
    let mut hash_value: u32 = 0;
    let mut one_hash: u32;

    // C `switch (nkeys)` with fall-through, combining each position's hash with
    // a position-dependent left-rotate before XOR.
    match nkeys {
        4 => {
            one_hash = fast_hash(kinds[3], keys[3])?;
            hash_value ^= pg_rotate_left32(one_hash, 24);
            one_hash = fast_hash(kinds[2], keys[2])?;
            hash_value ^= pg_rotate_left32(one_hash, 16);
            one_hash = fast_hash(kinds[1], keys[1])?;
            hash_value ^= pg_rotate_left32(one_hash, 8);
            one_hash = fast_hash(kinds[0], keys[0])?;
            hash_value ^= one_hash;
        }
        3 => {
            one_hash = fast_hash(kinds[2], keys[2])?;
            hash_value ^= pg_rotate_left32(one_hash, 16);
            one_hash = fast_hash(kinds[1], keys[1])?;
            hash_value ^= pg_rotate_left32(one_hash, 8);
            one_hash = fast_hash(kinds[0], keys[0])?;
            hash_value ^= one_hash;
        }
        2 => {
            one_hash = fast_hash(kinds[1], keys[1])?;
            hash_value ^= pg_rotate_left32(one_hash, 8);
            one_hash = fast_hash(kinds[0], keys[0])?;
            hash_value ^= one_hash;
        }
        1 => {
            one_hash = fast_hash(kinds[0], keys[0])?;
            hash_value ^= one_hash;
        }
        _ => {
            // C: `elog(FATAL, "wrong number of hash keys: %d", nkeys)`.
            panic!("wrong number of hash keys: {nkeys}");
        }
    }

    Ok(hash_value)
}

/// `CatalogCacheCompareTuple(cache, nkeys, cachekeys, searchkeys)`.
pub fn CatalogCacheCompareTuple(
    kinds: &[CCFastKind],
    nkeys: i32,
    cachekeys: &[Datum],
    searchkeys: &[Datum],
) -> PgResult<bool> {
    // C: `for (i = 0; i < nkeys; i++) if (!cc_fastequal[i](cachekeys[i],
    // searchkeys[i])) return false; return true;`
    for i in 0..(nkeys as usize) {
        if !fast_eq(kinds[i], cachekeys[i], searchkeys[i])? {
            return Ok(false);
        }
    }
    Ok(true)
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
