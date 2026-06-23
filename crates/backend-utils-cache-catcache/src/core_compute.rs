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

use types_cache::backend_utils_cache_catcache::{CatKey, CCFastKind};
use types_core::Oid;
// Bare-word machine-word `Datum` (`types_datum::Datum`), aliased `ScalarWord`:
// these fast hash/equality functions consume the by-value scalar key word (C's
// `DatumGetChar/Int16/Int32`) directly. Pass-by-value scalar keys stay the
// audited bare word, not the canonical `types_tuple::Datum<'mcx>` enum (which is
// for deformed tuple values). By-reference keys (name/text/oidvector) re-resolve
// their payload bytes elsewhere and never inhabit this word.
//
// Datum-unification (Wave 6) note: this is a sanctioned still-bare-word *type*
// edge, not an internal shim site. The catcache key-storage struct fields
// (`types_cache::backend_utils_cache_catcache::{CatCacheData,CatCTup,CatCList}.
// keys: [ScalarWord; CATCACHE_MAXKEYS]`) pin this scalar lane to the bare word;
// these compute functions feed/compare against those fields verbatim. Migrating
// to canonical `Datum<'mcx>` here would diverge from that out-of-scope
// types-cache contract. The crate's deformed-tuple values are already canonical.
use types_datum::Datum as ScalarWord;
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
pub fn datum_get_char(d: ScalarWord) -> i8 {
    d.as_char()
}

/// `DatumGetInt16(d)`.
#[inline]
pub fn datum_get_int16(d: ScalarWord) -> i16 {
    d.as_i16()
}

/// `DatumGetInt32(d)`.
#[inline]
pub fn datum_get_int32(d: ScalarWord) -> i32 {
    d.as_i32()
}

/* ----------------------------------------------------------------------------
 * The hard-coded hash/equality fast functions (catcache.c).
 * ------------------------------------------------------------------------- */

/// `chareqfast` — `DatumGetChar(a) == DatumGetChar(b)`.
#[inline]
pub fn chareqfast(a: ScalarWord, b: ScalarWord) -> bool {
    datum_get_char(a) == datum_get_char(b)
}

/// `charhashfast` — `murmurhash32((int32) DatumGetChar(datum))`.
#[inline]
pub fn charhashfast(datum: ScalarWord) -> u32 {
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
pub fn int2eqfast(a: ScalarWord, b: ScalarWord) -> bool {
    datum_get_int16(a) == datum_get_int16(b)
}

/// `int2hashfast` — `murmurhash32((int32) DatumGetInt16(datum))`.
#[inline]
pub fn int2hashfast(datum: ScalarWord) -> u32 {
    murmurhash32(datum_get_int16(datum) as i32 as u32)
}

/// `int4eqfast` — `DatumGetInt32(a) == DatumGetInt32(b)`.
#[inline]
pub fn int4eqfast(a: ScalarWord, b: ScalarWord) -> bool {
    datum_get_int32(a) == datum_get_int32(b)
}

/// `int4hashfast` — `murmurhash32((int32) DatumGetInt32(datum))`.
#[inline]
pub fn int4hashfast(datum: ScalarWord) -> u32 {
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

/// Canonicalize a by-reference key column's *deformed on-disk* `Datum::ByRef`
/// image into the catcache key payload the fast hash/equality functions consume.
///
/// The fast functions for `text` work on the detoasted, header-LESS
/// `VARDATA_ANY` payload (C's `hashtext`/`texteq` strip the varlena header), and
/// a by-name *search* key already crosses as that bare payload
/// (`SysCacheKey::Str(s)` -> `s.as_bytes()`, no varlena header). But a key column
/// deformed out of a catalog tuple (`heap_deform_tuple`/`nocachegetattr`) is the
/// FULL on-disk varlena image (4-byte/short header + payload). Caching or hashing
/// that header-ful image directly makes its hash differ from the header-less
/// search key's, so a positive entry never matches a search key (forcing a
/// re-scan every time) and — worse — an inval request computed from an inserted
/// tuple never matches the negative entry left by an earlier by-name miss, so the
/// stale negative entry is never cleared (a by-name lookup keeps returning "not
/// found" after the row is created in the same session).
///
/// This strips the varlena header for [`CCFastKind::Text`] (mirroring
/// `VARDATA_ANY`), so the cached entry's keys and the inval-request hash use the
/// same header-less payload the search key carries. `name` passes through
/// verbatim (the fixed `NAMEDATALEN` buffer; [`name_significant_len`] truncates
/// it at its NUL, matching the bare search key); `oidvector` and the scalar
/// kinds are not header-framed.
///
/// External/compressed TOAST images never reach here: `build_fetched` flattens
/// any out-of-line toasted column before key extraction, and a catalog key
/// column is never stored compressed-in-line.
pub fn canonicalize_byref_key(kind: CCFastKind, image: &[u8]) -> alloc::vec::Vec<u8> {
    match kind {
        CCFastKind::Text => vardata_any_image(image).to_vec(),
        // `oidvector` is normalized to a 4-byte-header (`VARATT_IS_4B_U`)
        // `ArrayType` image. The search-key side (`buildoidvector`, the
        // `SysCacheKey::Bytes` payload) is always a freshly-built 4-byte-header
        // oidvector; a STORED oidvector (`pg_proc.proargtypes`, deformed out of a
        // catalog tuple) may instead carry a 1-byte ("short") varlena header under
        // short-varlena packing, which drops 3 header bytes from the image. Since
        // the fast functions (`oidvectorhashfast`/`oidvectoreqfast`) chunk the
        // WHOLE resolved payload into `Oid` words, a short-packed stored image
        // would chunk to a different word count than the 4-byte-header search key
        // — so a 0-arg function's `PROCNAMEARGSNSP` probe never matches its own
        // row, and `CREATE OR REPLACE FUNCTION` re-inserts -> "duplicate key value
        // violates unique constraint pg_proc_proname_args_nsp_index". Un-pack the
        // short header so both sides present the identical 4-byte-header image.
        // Behavior-preserving while short packing is OFF (the stored image is
        // already 4-byte and passes through unchanged).
        CCFastKind::OidVector => unpack_short_varlena_to_4b(image),
        _ => image.to_vec(),
    }
}

/// Normalize an inline varlena image to a 4-byte-header (`VARATT_IS_4B_U`) form.
/// A short (1-byte) header carries the total length in `header >> 1` and its
/// payload at `[1..total]`; the 4-byte form is `SET_VARSIZE(len + VARHDRSZ)`
/// (low two bits `00`, native-endian `(len + 4) << 2`) followed by that payload.
/// A value that is already 4-byte (or any non-short form) passes through verbatim.
/// Compressed/external images do not reach here (the caller flattens TOAST first).
fn unpack_short_varlena_to_4b(image: &[u8]) -> alloc::vec::Vec<u8> {
    if image.is_empty() {
        return alloc::vec::Vec::new();
    }
    let header = image[0];
    // VARATT_IS_1B (short inline) but not VARATT_IS_1B_E (external, exactly 0x01).
    if header != 0x01 && (header & 0x01) == 0x01 {
        let total = ((header >> 1) & 0x7F) as usize;
        let total = total.min(image.len()).max(1);
        let payload = &image[1..total];
        // SET_VARSIZE(buf, payload.len() + VARHDRSZ): (len4 << 2), low bits 00.
        let len4 = (payload.len() + 4) as u32;
        let mut out = alloc::vec::Vec::with_capacity(4 + payload.len());
        out.extend_from_slice(&(len4 << 2).to_ne_bytes());
        out.extend_from_slice(payload);
        out
    } else {
        image.to_vec()
    }
}

/// `VARDATA_ANY(image)` / `VARSIZE_ANY_EXHDR(image)` (`varatt.h`): the
/// header-less payload slice of an inline varlena. Handles a 1-byte ("short")
/// header and a 4-byte uncompressed header; external/compressed images do not
/// reach this (the caller flattens TOAST first).
#[inline]
fn vardata_any_image(image: &[u8]) -> &[u8] {
    if image.is_empty() {
        return &[];
    }
    let header = image[0];
    // VARATT_IS_1B (short inline) but not VARATT_IS_1B_E (external 0x01): the low
    // bit is set and the byte is not exactly 0x01. The total length (incl. the
    // 1-byte header) is `header >> 1`; payload is `[1..total]`.
    if header != 0x01 && (header & 0x01) == 0x01 {
        let total = ((header >> 1) & 0x7F) as usize;
        let total = total.min(image.len()).max(1);
        &image[1..total]
    } else if image.len() >= 4 {
        // VARATT_IS_4B_U: skip the 4-byte (`VARHDRSZ`) header.
        &image[4..]
    } else {
        &[]
    }
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

/// The scalar word of a by-value key slot. The C `cc_hashfunc`/`cc_fastequal`
/// for a by-value kind reads the `Datum` word; in this model that is a
/// [`CatKey::Scalar`]. A by-reference payload in a by-value position (or vice
/// versa) is a key-kind/key-storage mismatch — the C model never produces it.
#[inline]
fn scalar_word(kind: CCFastKind, key: &CatKey) -> ScalarWord {
    match key {
        CatKey::Scalar(w) => *w,
        CatKey::ByRef(_) => panic!(
            "catcache::core_compute: by-value key kind {kind:?} carries a \
             by-reference payload (key-storage mismatch)"
        ),
    }
}

/// The resolved payload bytes of a by-reference key slot (C's `name`/`text`/
/// `oidvector` payload behind the key `Datum`'s pointer).
#[inline]
fn byref_payload(kind: CCFastKind, key: &CatKey) -> &[u8] {
    match key {
        CatKey::ByRef(bytes) => bytes,
        CatKey::Scalar(_) => panic!(
            "catcache::core_compute: by-reference key kind {kind:?} carries a \
             by-value scalar word (key-storage mismatch)"
        ),
    }
}

/// Apply the fast hash function for a key kind to one key slot (the C indirect
/// `cc_hashfunc[i](key)` call). By-value kinds read the scalar word; by-reference
/// kinds (`name`/`text`/`oidvector`) hash their resolved payload bytes via
/// [`namehashfast`]/[`texthashfast`]/[`oidvectorhashfast`].
pub fn fast_hash(kind: CCFastKind, key: &CatKey) -> PgResult<u32> {
    match kind {
        CCFastKind::Char => Ok(charhashfast(scalar_word(kind, key))),
        CCFastKind::Int2 => Ok(int2hashfast(scalar_word(kind, key))),
        CCFastKind::Int4 => Ok(int4hashfast(scalar_word(kind, key))),
        CCFastKind::Name => Ok(namehashfast(byref_payload(kind, key))),
        CCFastKind::Text => Ok(texthashfast(byref_payload(kind, key))),
        CCFastKind::OidVector => {
            oidvectorhashfast(&oid_slice_from_bytes(byref_payload(kind, key)))
        }
    }
}

/// Apply the fast equality function for a key kind to two key slots (the C
/// indirect `cc_fastequal[i](a, b)` call). By-value kinds compare scalar words;
/// by-reference kinds compare resolved payload bytes via [`nameeqfast`]/
/// [`texteqfast`]/[`oidvectoreqfast`].
pub fn fast_eq(kind: CCFastKind, a: &CatKey, b: &CatKey) -> PgResult<bool> {
    match kind {
        CCFastKind::Char => Ok(chareqfast(scalar_word(kind, a), scalar_word(kind, b))),
        CCFastKind::Int2 => Ok(int2eqfast(scalar_word(kind, a), scalar_word(kind, b))),
        CCFastKind::Int4 => Ok(int4eqfast(scalar_word(kind, a), scalar_word(kind, b))),
        CCFastKind::Name => Ok(nameeqfast(byref_payload(kind, a), byref_payload(kind, b))),
        CCFastKind::Text => Ok(texteqfast(byref_payload(kind, a), byref_payload(kind, b))),
        CCFastKind::OidVector => Ok(oidvectoreqfast(
            &oid_slice_from_bytes(byref_payload(kind, a)),
            &oid_slice_from_bytes(byref_payload(kind, b)),
        )),
    }
}

/// Reinterpret an `oidvector` key's contiguous native-endian `Oid` element
/// bytes (as produced by [`oidvectorhashfast`]'s inverse) back into `Oid`s.
#[inline]
fn oid_slice_from_bytes(bytes: &[u8]) -> alloc::vec::Vec<Oid> {
    bytes
        .chunks_exact(core::mem::size_of::<Oid>())
        .map(|c| Oid::from_ne_bytes(c.try_into().unwrap()))
        .collect()
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
    keys: &[CatKey],
) -> PgResult<u32> {
    let mut hash_value: u32 = 0;
    let mut one_hash: u32;

    // C `switch (nkeys)` with fall-through, combining each position's hash with
    // a position-dependent left-rotate before XOR.
    match nkeys {
        4 => {
            one_hash = fast_hash(kinds[3], &keys[3])?;
            hash_value ^= pg_rotate_left32(one_hash, 24);
            one_hash = fast_hash(kinds[2], &keys[2])?;
            hash_value ^= pg_rotate_left32(one_hash, 16);
            one_hash = fast_hash(kinds[1], &keys[1])?;
            hash_value ^= pg_rotate_left32(one_hash, 8);
            one_hash = fast_hash(kinds[0], &keys[0])?;
            hash_value ^= one_hash;
        }
        3 => {
            one_hash = fast_hash(kinds[2], &keys[2])?;
            hash_value ^= pg_rotate_left32(one_hash, 16);
            one_hash = fast_hash(kinds[1], &keys[1])?;
            hash_value ^= pg_rotate_left32(one_hash, 8);
            one_hash = fast_hash(kinds[0], &keys[0])?;
            hash_value ^= one_hash;
        }
        2 => {
            one_hash = fast_hash(kinds[1], &keys[1])?;
            hash_value ^= pg_rotate_left32(one_hash, 8);
            one_hash = fast_hash(kinds[0], &keys[0])?;
            hash_value ^= one_hash;
        }
        1 => {
            one_hash = fast_hash(kinds[0], &keys[0])?;
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
    cachekeys: &[CatKey],
    searchkeys: &[CatKey],
) -> PgResult<bool> {
    // C: `for (i = 0; i < nkeys; i++) if (!cc_fastequal[i](cachekeys[i],
    // searchkeys[i])) return false; return true;`
    for i in 0..(nkeys as usize) {
        if !fast_eq(kinds[i], &cachekeys[i], &searchkeys[i])? {
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
