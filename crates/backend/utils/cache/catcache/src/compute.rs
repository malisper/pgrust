use datum::Datum;
use types_core::Oid;

pub const CATCACHE_MAXKEYS: usize = 4;
pub const NAMEDATALEN: usize = 64;

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

pub const F_BOOLEQ: Oid = 60;
pub const F_CHAREQ: Oid = 61;
pub const F_NAMEEQ: Oid = 62;
pub const F_INT2EQ: Oid = 63;
pub const F_INT4EQ: Oid = 65;
pub const F_TEXTEQ: Oid = 67;
pub const F_OIDEQ: Oid = 184;
pub const F_OIDVECTOREQ: Oid = 679;

/// C's `(CCHashFN, CCFastEqualFN)` fn-pointer pair as a closed-set tag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CCFastKind {
    // Word kinds first: "is word kind" is one compare, not a ladder.
    Char,
    Int2,
    Int4,
    Name,
    Text,
    OidVector,
}

/// One search key (C's `Datum v1..v4`); by-reference keys borrow the
/// caller's payload — C never copies a search key to hash/compare it.
#[derive(Clone, Copy, Debug)]
pub enum CatCKey<'a> {
    Value(Datum),
    Str(&'a str),
    Bytes(&'a [u8]),
}

impl CatCKey<'_> {
    pub const UNUSED: CatCKey<'static> = CatCKey::Value(Datum::null());

    #[inline]
    pub(crate) fn word(&self) -> Datum {
        match self {
            CatCKey::Value(d) => *d,
            _ => panic!("catcache: by-value key slot holds a by-reference payload"),
        }
    }

    #[inline]
    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            CatCKey::Str(s) => s.as_bytes(),
            CatCKey::Bytes(b) => b,
            CatCKey::Value(_) => panic!("catcache: by-reference key slot holds a scalar word"),
        }
    }
}

/// `strncmp(a, b, NAMEDATALEN) == 0` over NUL-terminated / NUL-free images.
#[inline]
pub fn name_eq(a: &[u8], b: &[u8]) -> bool {
    let la = name_len(a);
    let lb = name_len(b);
    la == lb && a[..la] == b[..lb]
}

#[inline]
fn name_len(k: &[u8]) -> usize {
    let cap = k.len().min(NAMEDATALEN);
    memchr_nul(&k[..cap]).unwrap_or(cap)
}

#[inline]
fn memchr_nul(s: &[u8]) -> Option<usize> {
    s.iter().position(|&c| c == 0)
}

#[inline]
pub fn char_hash(d: Datum) -> u32 {
    hashfn::murmurhash32(d.as_char() as i32 as u32)
}

#[inline]
pub fn int2_hash(d: Datum) -> u32 {
    hashfn::murmurhash32(d.as_i16() as i32 as u32)
}

#[inline]
pub fn int4_hash(d: Datum) -> u32 {
    hashfn::murmurhash32(d.as_i32() as u32)
}

/// `namehashfast`: `hash_any(key, strlen(key))` — uncapped, as C.
#[inline]
pub fn name_hash(payload: &[u8]) -> u32 {
    let len = memchr_nul(payload).unwrap_or(payload.len());
    hashfn::hash_bytes(&payload[..len])
}

// Word arms inline; byte-slice arms outlined (fused, every hash is a call).
#[inline(always)]
pub fn fast_hash_probe(kind: CCFastKind, key: &CatCKey<'_>) -> u32 {
    match kind {
        // Canonical word datums: one low-32 murmur IS all three hashfasts.
        CCFastKind::Char | CCFastKind::Int2 | CCFastKind::Int4 => {
            hashfn::murmurhash32(key.word().as_i32() as u32)
        }
        _ => bytes_hash_outlined(kind, key),
    }
}

#[inline(never)]
fn bytes_hash_outlined(kind: CCFastKind, key: &CatCKey<'_>) -> u32 {
    match kind {
        CCFastKind::Name => name_hash(key.bytes()),
        CCFastKind::Text | CCFastKind::OidVector => hashfn::hash_bytes(key.bytes()),
        _ => unreachable!("word kinds are handled inline"),
    }
}

/// `CatalogCacheComputeHashValue` — position-dependent rotate-XOR combine.
#[inline]
pub fn compute_hash_value(kinds: &[CCFastKind; 4], nkeys: i32, keys: &[CatCKey<'_>; 4]) -> u32 {
    let mut hash: u32 = 0;
    match nkeys {
        4 => {
            hash ^= fast_hash_probe(kinds[3], &keys[3]).rotate_left(24);
            hash ^= fast_hash_probe(kinds[2], &keys[2]).rotate_left(16);
            hash ^= fast_hash_probe(kinds[1], &keys[1]).rotate_left(8);
            hash ^= fast_hash_probe(kinds[0], &keys[0]);
        }
        3 => {
            hash ^= fast_hash_probe(kinds[2], &keys[2]).rotate_left(16);
            hash ^= fast_hash_probe(kinds[1], &keys[1]).rotate_left(8);
            hash ^= fast_hash_probe(kinds[0], &keys[0]);
        }
        2 => {
            hash ^= fast_hash_probe(kinds[1], &keys[1]).rotate_left(8);
            hash ^= fast_hash_probe(kinds[0], &keys[0]);
        }
        1 => {
            hash ^= fast_hash_probe(kinds[0], &keys[0]);
        }
        _ => panic!("wrong number of hash keys: {nkeys}"),
    }
    hash
}

#[inline]
pub fn hash_index(h: u32, nbuckets: u32) -> usize {
    (h & (nbuckets - 1)) as usize
}

/// `GetCCHashEqFuncs(keytype)` → `(fast kind, eqfunc RegProcedure)`.
pub fn get_cc_hash_eq_funcs(keytype: Oid) -> (CCFastKind, Oid) {
    match keytype {
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
        _ => panic!("type {keytype} not supported as catcache key"),
    }
}
