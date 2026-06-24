//! `contrib/pg_trgm/trgm_gist.c` — the `gist_trgm_ops` GiST opclass support
//! functions, ported over the generic catalog-driven GiST opclass dispatch.
//!
//! The GiST core resolves each opclass support proc into an `FmgrInfo` and
//! `gist-proc`'s `extdispatch` invokes the body here through a real fmgr frame,
//! passing the `GISTENTRY`/`GistEntryVector`/`GIST_SPLITVEC` + the `*recheck`/
//! `*penalty`/`*size` out-parameters through the [`::gist::extproc`] internal
//! protocol structs (slot 0), and the `consistent`/`distance` query `text` on
//! the by-ref lane (slot 1). The body reads its typed inputs off the protocol
//! struct, runs the trgm signature logic, and writes its outputs back.
//!
//! ## Key representation
//!
//! A `TRGM` GiST key is a varlena: `[VARHDR(4) | flag(1) | data]`
//! (`TRGMHDRSIZE == 5`). Three key kinds (`trgm.h`):
//! * `ARRKEY` — `data` is `n * 3` trigram bytes (a leaf key);
//! * `SIGNKEY` — `data` is `siglen` signature bytes (an inner key);
//! * `SIGNKEY | ALLISTRUE` — no `data` (the all-ones signature).
//!
//! ## `siglen`
//!
//! C reads the opclass `siglen` option via `GET_SIGLEN()`. The owned GiST
//! dispatch does not thread opclass options to the support procs (the documented
//! `tsvector_ops` divergence), so the build side uses [`SIGLEN_DEFAULT`] and the
//! read side reads the stored signature's own length. This is exact for the
//! default index (no explicit `siglen`), which is what the regression tests use;
//! only an index built with an explicit non-default `siglen` would differ in its
//! physical signature length (queries still recheck on the heap, so results are
//! correct regardless).
//!
//! The RegExp / RegExpICase strategies need the `trgm_regexp.c` NFA engine
//! (`createTrgmNFA` / `trigramsMatchGraph`), which is NOT ported; those
//! strategies raise a clear "unported" error.

use ::types_error::{PgError, PgResult};

use crate::trgm::{cmp_trgm, generate_trgm, generate_wildcard_trgm, trgm2int, Trgm};
use crate::{legacy_crc32, make_env, raise};

// trgm.h key-flag bits.
const ARRKEY: u8 = 0x01;
const SIGNKEY: u8 = 0x02;
const ALLISTRUE: u8 = 0x04;

/// `TRGMHDRSIZE = VARHDRSZ + sizeof(uint8)`.
const TRGMHDRSIZE: usize = 4 + 1;

/// `SIGLEN_DEFAULT = sizeof(int) * 3` (12 bytes).
pub const SIGLEN_DEFAULT: usize = 12;

const BITBYTE: usize = 8;

// SIGLEN_MAX = GISTMaxIndexKeySize (trgm.h / access/gist.h). Mirrors the
// `tsgistidx::SIGLEN_MAX` derivation so the `gtrgm_options` siglen reloption has
// the same upper bound as C, without pulling the tsgistidx crate as a dep.
const fn maxalign(len: usize) -> usize {
    (len + 7) & !7usize
}
const fn maxalign_down(len: usize) -> usize {
    len & !7usize
}
const SIZE_OF_PAGE_HEADER_DATA: usize = 24;
const SIZEOF_GIST_PAGE_OPAQUE_DATA: usize = 16;
const SIZEOF_ITEM_ID_DATA: usize = 4;
const SIZEOF_INDEX_TUPLE_DATA: usize = 8;
const GIST_MAX_INDEX_TUPLE_SIZE: usize = maxalign_down(
    (8192 - SIZE_OF_PAGE_HEADER_DATA - SIZEOF_GIST_PAGE_OPAQUE_DATA) / 4 - SIZEOF_ITEM_ID_DATA,
);
/// `SIGLEN_MAX` — the `siglen` reloption upper bound.
pub const SIGLEN_MAX: i32 = (GIST_MAX_INDEX_TUPLE_SIZE - maxalign(SIZEOF_INDEX_TUPLE_DATA)) as i32;

/// `offsetof(TrgmGistOptions, siglen)` — `siglen` follows the 4-byte varlena
/// header.
const OFFSETOF_TRGM_GIST_OPTIONS_SIGLEN: i32 = 4;
/// `sizeof(TrgmGistOptions)` (`int32 vl_len_` + `int siglen`).
const SIZEOF_TRGM_GIST_OPTIONS: usize = 8;

/// `gtrgm_options(relopts)` (trgm_gist.c:962) — register the `siglen` opclass
/// option on the `local_relopts` parse table. (The configured value is not
/// threaded back to the support procs — the documented default-siglen
/// divergence — but the option must still be REGISTERED so `CREATE INDEX (...
/// WITH (siglen = N))` parses rather than erroring, and so the relcache's
/// local-reloptions build produces a well-formed buffer.)
pub fn gtrgm_options(relopts: &mut ::types_reloptions::local_relopts) {
    ::reloptions_seams::init_local_reloptions::call(relopts, SIZEOF_TRGM_GIST_OPTIONS);
    ::reloptions_seams::add_local_int_reloption::call(
        relopts,
        "siglen",
        Some("signature length in bytes"),
        SIGLEN_DEFAULT as i32,
        1,
        SIGLEN_MAX,
        OFFSETOF_TRGM_GIST_OPTIONS_SIGLEN,
    );
}

// gist_trgm_ops strategy numbers (trgm.h).
const SIMILARITY_STRATEGY: u16 = 1;
const DISTANCE_STRATEGY: u16 = 2;
const LIKE_STRATEGY: u16 = 3;
const ILIKE_STRATEGY: u16 = 4;
const REGEXP_STRATEGY: u16 = 5;
const REGEXP_ICASE_STRATEGY: u16 = 6;
const WORD_SIMILARITY_STRATEGY: u16 = 7;
const WORD_DISTANCE_STRATEGY: u16 = 8;
const STRICT_WORD_SIMILARITY_STRATEGY: u16 = 9;
const STRICT_WORD_DISTANCE_STRATEGY: u16 = 10;
const EQUAL_STRATEGY: u16 = 11;

// ===========================================================================
// TRGM key decode / encode (varlena image <-> typed value).
// ===========================================================================

/// A decoded GiST `TRGM` key.
#[derive(Clone, Debug)]
enum TrgmKey {
    /// `ARRKEY` — a sorted trigram array (the leaf key form).
    Arr(Vec<Trgm>),
    /// `SIGNKEY` (not ALLISTRUE) — a `siglen`-byte signature bitmap.
    Sign(Vec<u8>),
    /// `SIGNKEY | ALLISTRUE`.
    AllTrue,
}

impl TrgmKey {
    fn flag(&self) -> u8 {
        match self {
            TrgmKey::Arr(_) => ARRKEY,
            TrgmKey::Sign(_) => SIGNKEY,
            TrgmKey::AllTrue => SIGNKEY | ALLISTRUE,
        }
    }
}

/// Decode a header-ful `TRGM` varlena image into a [`TrgmKey`].
///
/// The image may carry trailing alignment padding (the GiST core stores index
/// keys MAXALIGN'ed), so the payload is bounded by the varlena length word
/// (`VARSIZE`), NOT by `image.len()`.
fn decode_key(image: &[u8]) -> PgResult<TrgmKey> {
    if image.len() < TRGMHDRSIZE {
        return Err(PgError::error("corrupt gtrgm GiST key (short header)"));
    }
    // VARSIZE of a 4-byte (non-short, non-compressed) varlena: the length word
    // is `len << 2` native-endian (the low 2 bits are the varlena tag).
    let raw = u32::from_ne_bytes([image[0], image[1], image[2], image[3]]);
    let varsize = (raw >> 2) as usize;
    let end = varsize.min(image.len()).max(TRGMHDRSIZE);
    let flag = image[4];
    let data = &image[TRGMHDRSIZE..end];
    if flag & ARRKEY != 0 {
        if data.len() % 3 != 0 {
            return Err(PgError::error("corrupt gtrgm GiST ARRKEY (length not a multiple of 3)"));
        }
        let mut arr = Vec::with_capacity(data.len() / 3);
        for chunk in data.chunks_exact(3) {
            arr.push([chunk[0], chunk[1], chunk[2]]);
        }
        Ok(TrgmKey::Arr(arr))
    } else if flag & ALLISTRUE != 0 {
        Ok(TrgmKey::AllTrue)
    } else {
        // SIGNKEY: the rest is the signature.
        Ok(TrgmKey::Sign(data.to_vec()))
    }
}

/// `SET_VARSIZE(ptr, size)` — the 4-byte ("4B-U") varlena length word: the high
/// 30 bits hold `size`, the low 2 bits are the uncompressed-unaligned tag (0).
/// Native-endian, matching the on-disk / in-memory image the GiST core reads via
/// `VARSIZE = word >> 2`.
fn varsize_header(size: usize) -> [u8; 4] {
    ((size as u32) << 2).to_ne_bytes()
}

/// Encode a trigram array as an `ARRKEY` varlena image.
fn encode_arrkey(arr: &[Trgm]) -> Vec<u8> {
    let size = TRGMHDRSIZE + arr.len() * 3;
    let mut img = Vec::with_capacity(size);
    img.extend_from_slice(&varsize_header(size));
    img.push(ARRKEY);
    for t in arr {
        img.extend_from_slice(t);
    }
    img
}

/// Encode a signature / allistrue key as a `SIGNKEY` varlena image
/// (`gtrgm_alloc`). When `isalltrue`, no signature data is stored.
fn encode_signkey(isalltrue: bool, sign: &[u8]) -> Vec<u8> {
    let flag = SIGNKEY | if isalltrue { ALLISTRUE } else { 0 };
    let datalen = if isalltrue { 0 } else { sign.len() };
    let size = TRGMHDRSIZE + datalen;
    let mut img = Vec::with_capacity(size);
    img.extend_from_slice(&varsize_header(size));
    img.push(flag);
    if !isalltrue {
        img.extend_from_slice(sign);
    }
    img
}

// ===========================================================================
// Signature bitmap primitives (trgm.h macros).
// ===========================================================================

/// `SIGLENBIT(siglen) = siglen * BITBYTE - 1` — the last (unused) bit, used as
/// the divisor in `HASHVAL` and reserved by `makesign`.
fn siglenbit(siglen: usize) -> usize {
    siglen * BITBYTE - 1
}

/// `HASHVAL(val, siglen) = val % SIGLENBIT(siglen)`.
fn hashval(val: u32, siglen: usize) -> usize {
    (val as usize) % siglenbit(siglen)
}

/// `GETBIT(sign, i)`.
fn getbit(sign: &[u8], i: usize) -> bool {
    (sign[i / BITBYTE] >> (i % BITBYTE)) & 0x01 != 0
}

/// `SETBIT(sign, i)`.
fn setbit(sign: &mut [u8], i: usize) {
    sign[i / BITBYTE] |= 0x01 << (i % BITBYTE);
}

/// `makesign(sign, a, siglen)` — hash every trigram of the array key into a
/// fresh signature (and set the last unused bit).
fn makesign(arr: &[Trgm], siglen: usize) -> Vec<u8> {
    let mut sign = vec![0u8; siglen];
    setbit(&mut sign, siglenbit(siglen));
    for t in arr {
        let v = trgm2int(t);
        setbit(&mut sign, hashval(v, siglen));
    }
    sign
}

/// `pg_popcount(sign, siglen)` — number of set bits.
fn sizebitvec(sign: &[u8]) -> i32 {
    sign.iter().map(|b| b.count_ones() as i32).sum()
}

/// `hemdistsign(a, b, siglen)` — Hamming distance of two equal-length signatures.
fn hemdistsign(a: &[u8], b: &[u8], siglen: usize) -> i32 {
    let mut dist = 0;
    for i in 0..siglen {
        dist += (a[i] ^ b[i]).count_ones() as i32;
    }
    dist
}

/// `cnt_sml_sign_common(qtrg, sign, siglen)` — number of query trigrams whose
/// hash bit is set in the signature.
fn cnt_sml_sign_common(qtrg: &[Trgm], sign: &[u8], siglen: usize) -> i32 {
    let mut count = 0;
    for t in qtrg {
        let v = trgm2int(t);
        if getbit(sign, hashval(v, siglen)) {
            count += 1;
        }
    }
    count
}

/// `cnt_sml(trg1, trg2, inexact)` over two trigram arrays — reuse the trgm core.
fn cnt_sml_arr(trg1: &[Trgm], trg2: &[Trgm], inexact: bool) -> f32 {
    crate::trgm::cnt_sml(trg1, trg2, inexact)
}

/// `trgm_contained_by(trg1, trg2)` — is every trigram of the (sorted) `trg1`
/// present in the (sorted) `trg2`? (trgm_op.c).
fn trgm_contained_by(trg1: &[Trgm], trg2: &[Trgm]) -> bool {
    let mut i = 0usize;
    let mut j = 0usize;
    while i < trg1.len() && j < trg2.len() {
        match cmp_trgm(&trg1[i], &trg2[j]) {
            core::cmp::Ordering::Less => return false,
            core::cmp::Ordering::Greater => j += 1,
            core::cmp::Ordering::Equal => {
                i += 1;
                j += 1;
            }
        }
    }
    i >= trg1.len()
}

// ===========================================================================
// index_strategy_get_limit — the similarity thresholds (GUC-backed).
// ===========================================================================

fn index_strategy_get_limit(strategy: u16) -> f64 {
    match strategy {
        SIMILARITY_STRATEGY => crate::get_similarity_threshold(),
        WORD_SIMILARITY_STRATEGY => crate::get_word_similarity_threshold(),
        STRICT_WORD_SIMILARITY_STRATEGY => crate::get_strict_word_similarity_threshold(),
        other => raise(PgError::error(format!(
            "unrecognized strategy number: {other}"
        ))),
    }
}

// ===========================================================================
// gtrgm_compress / gtrgm_decompress
// ===========================================================================

/// `gtrgm_compress(entry)` — convert a leaf `text` value into its `ARRKEY`
/// trigram signature; rewrite an all-0xff inner `SIGNKEY` to `ALLISTRUE`;
/// otherwise pass through. Inputs: `leafkey`, the (detoasted) leaf `text` payload
/// or the stored key image. Returns `Some(new_key_image)` or `None`
/// (pass-through).
pub fn gtrgm_compress(leafkey: bool, key_image: &[u8], key_is_null: bool) -> PgResult<Option<Vec<u8>>> {
    if leafkey {
        // res = generate_trgm(VARDATA_ANY(val), VARSIZE_ANY_EXHDR(val))
        // The dispatch supplies the detoasted leaf image; strip its 4-byte
        // header to reach VARDATA_ANY (the dispatch normalized to a 4-byte
        // header).
        if key_is_null || key_image.len() < 4 {
            return Err(PgError::error("gtrgm_compress: NULL leaf key"));
        }
        // VARDATA_ANY(val) bounded by VARSIZE_ANY_EXHDR(val) — the detoasted leaf
        // `text` carries a 4-byte (len << 2) header.
        let raw = u32::from_ne_bytes([key_image[0], key_image[1], key_image[2], key_image[3]]);
        let varsize = ((raw >> 2) as usize).min(key_image.len()).max(4);
        let payload = &key_image[4..varsize];
        let env = make_env();
        let trg = generate_trgm(payload, &env, &legacy_crc32);
        return Ok(Some(encode_arrkey(&trg)));
    }
    // Inner entry. A NULL key is not a SIGNKEY → pass through.
    if key_is_null {
        return Ok(None);
    }
    let k = decode_key(key_image)?;
    if let TrgmKey::Sign(sign) = k {
        // ISSIGNKEY && !ISALLTRUE: rewrite to ALLISTRUE iff every byte is 0xff.
        let key_siglen = sign.len();
        let all_ff = sign.iter().all(|&b| b == 0xff);
        if all_ff {
            // gtrgm_alloc(true, siglen, sign): allistrue (no signature stored).
            let _ = key_siglen;
            return Ok(Some(encode_signkey(true, &[])));
        }
    }
    Ok(None)
}

/// `gtrgm_decompress(entry)` — only `PG_DETOAST`s the key; the owned by-ref lane
/// already delivers a plain image, so it is the identity (pass-through).
pub fn gtrgm_decompress() -> Option<Vec<u8>> {
    None
}

// ===========================================================================
// gtrgm_consistent
// ===========================================================================

/// `gtrgm_consistent(entry, query, strategy, subtype, recheck)`. Returns
/// `(matched, recheck)`. The `query` is the by-ref `text` payload (header-ful).
pub fn gtrgm_consistent(
    is_leaf: bool,
    key_image: &[u8],
    key_is_null: bool,
    query_payload: &[u8],
    strategy: u16,
) -> PgResult<(bool, bool)> {
    // `query_payload` is VARDATA_ANY(query) — the dispatch's by-ref lane
    // delivered the `text` and the fmgr boundary already stripped the header.
    let env = make_env();

    // Extract query trigrams (the C consistent cache; we recompute each call —
    // correct, just not cached).
    let qtrg: Option<Vec<Trgm>> = match strategy {
        SIMILARITY_STRATEGY
        | WORD_SIMILARITY_STRATEGY
        | STRICT_WORD_SIMILARITY_STRATEGY
        | EQUAL_STRATEGY => Some(generate_trgm(query_payload, &env, &legacy_crc32)),
        LIKE_STRATEGY | ILIKE_STRATEGY => {
            Some(generate_wildcard_trgm(query_payload, &env, &legacy_crc32))
        }
        REGEXP_STRATEGY | REGEXP_ICASE_STRATEGY => {
            return Err(unported_regexp("gtrgm_consistent"));
        }
        other => {
            return Err(PgError::error(format!(
                "unrecognized strategy number: {other}"
            )))
        }
    };
    let qtrg = qtrg.expect("non-regexp strategies always produce a trigram array");

    let key = if key_is_null {
        // A NULL key cannot occur for a consistent call (GiST never passes a
        // NULL key to consistent); treat conservatively.
        return Err(PgError::error("gtrgm_consistent: NULL key"));
    } else {
        decode_key(key_image)?
    };

    let (res, recheck) = match strategy {
        SIMILARITY_STRATEGY | WORD_SIMILARITY_STRATEGY | STRICT_WORD_SIMILARITY_STRATEGY => {
            let recheck = strategy != SIMILARITY_STRATEGY;
            let nlimit = index_strategy_get_limit(strategy);
            let res = if is_leaf {
                // leaf contains orig trgm (ARRKEY)
                let key_arr = expect_arr(&key)?;
                let tmpsml = cnt_sml_arr(&qtrg, key_arr, recheck);
                (tmpsml as f64) >= nlimit
            } else {
                match &key {
                    TrgmKey::AllTrue => true,
                    TrgmKey::Sign(sign) => {
                        let siglen = sign.len();
                        let count = cnt_sml_sign_common(&qtrg, sign, siglen);
                        let len = qtrg.len() as i32;
                        if len == 0 {
                            false
                        } else {
                            ((count as f64) / (len as f64)) >= nlimit
                        }
                    }
                    TrgmKey::Arr(_) => {
                        // shouldn't happen on an inner page, but handle exactly.
                        let key_arr = expect_arr(&key)?;
                        let tmpsml = cnt_sml_arr(&qtrg, key_arr, recheck);
                        (tmpsml as f64) >= nlimit
                    }
                }
            };
            (res, recheck)
        }
        LIKE_STRATEGY | ILIKE_STRATEGY | EQUAL_STRATEGY => {
            // Wildcard and equal search are inexact.
            let res = if is_leaf {
                let key_arr = expect_arr(&key)?;
                trgm_contained_by(&qtrg, key_arr)
            } else {
                match &key {
                    TrgmKey::AllTrue => true,
                    TrgmKey::Sign(sign) => {
                        let siglen = sign.len();
                        let mut ok = true;
                        for t in &qtrg {
                            let v = trgm2int(t);
                            if !getbit(sign, hashval(v, siglen)) {
                                ok = false;
                                break;
                            }
                        }
                        ok
                    }
                    TrgmKey::Arr(_) => {
                        let key_arr = expect_arr(&key)?;
                        trgm_contained_by(&qtrg, key_arr)
                    }
                }
            };
            (res, true)
        }
        _ => unreachable!("strategy filtered above"),
    };

    Ok((res, recheck))
}

/// Expect an ARRKEY (leaf), erroring otherwise.
fn expect_arr(key: &TrgmKey) -> PgResult<&[Trgm]> {
    match key {
        TrgmKey::Arr(a) => Ok(a),
        _ => Err(PgError::error(
            "gtrgm GiST: expected a leaf ARRKEY but found a signature key",
        )),
    }
}

// ===========================================================================
// gtrgm_distance
// ===========================================================================

/// `gtrgm_distance(entry, query, strategy, subtype, recheck)`. Returns
/// `(distance, recheck)`.
pub fn gtrgm_distance(
    is_leaf: bool,
    key_image: &[u8],
    key_is_null: bool,
    query_payload: &[u8],
    strategy: u16,
) -> PgResult<(f64, bool)> {
    let env = make_env();
    let qtrg = generate_trgm(query_payload, &env, &legacy_crc32);

    let key = if key_is_null {
        return Err(PgError::error("gtrgm_distance: NULL key"));
    } else {
        decode_key(key_image)?
    };

    match strategy {
        DISTANCE_STRATEGY | WORD_DISTANCE_STRATEGY | STRICT_WORD_DISTANCE_STRATEGY => {
            let recheck = strategy != DISTANCE_STRATEGY;
            let res = if is_leaf {
                let key_arr = expect_arr(&key)?;
                let sml = cnt_sml_arr(&qtrg, key_arr, recheck);
                1.0 - sml as f64
            } else {
                match &key {
                    TrgmKey::AllTrue => 0.0,
                    TrgmKey::Sign(sign) => {
                        let siglen = sign.len();
                        let count = cnt_sml_sign_common(&qtrg, sign, siglen);
                        let len = qtrg.len() as i32;
                        if len == 0 {
                            -1.0
                        } else {
                            1.0 - (count as f64) / (len as f64)
                        }
                    }
                    TrgmKey::Arr(_) => {
                        let key_arr = expect_arr(&key)?;
                        let sml = cnt_sml_arr(&qtrg, key_arr, recheck);
                        1.0 - sml as f64
                    }
                }
            };
            Ok((res, recheck))
        }
        other => Err(PgError::error(format!(
            "unrecognized strategy number: {other}"
        ))),
    }
}

// ===========================================================================
// gtrgm_union
// ===========================================================================

/// `unionkey(sbase, add, siglen)` — OR the `add` key's contribution into the
/// signature; returns `true` if the result must become ALLISTRUE.
fn unionkey(sbase: &mut [u8], add: &TrgmKey, siglen: usize) -> bool {
    match add {
        TrgmKey::Sign(sadd) => {
            for i in 0..siglen {
                sbase[i] |= sadd[i];
            }
            false
        }
        TrgmKey::AllTrue => true,
        TrgmKey::Arr(arr) => {
            for t in arr {
                let v = trgm2int(t);
                setbit(sbase, hashval(v, siglen));
            }
            false
        }
    }
}

/// `gtrgm_union(entryvec, &size)`. `entries` is every member's key image.
pub fn gtrgm_union(entries: &[(Vec<u8>, bool)]) -> PgResult<Vec<u8>> {
    let siglen = SIGLEN_DEFAULT;
    // result = gtrgm_alloc(false, siglen, NULL): a zeroed signature.
    let mut base = vec![0u8; siglen];
    let mut allistrue = false;
    for (img, is_null) in entries {
        if *is_null {
            // a NULL key contributes nothing (shouldn't occur).
            continue;
        }
        let k = decode_key(img)?;
        if unionkey(&mut base, &k, siglen) {
            allistrue = true;
            break;
        }
    }
    if allistrue {
        Ok(encode_signkey(true, &base))
    } else {
        Ok(encode_signkey(false, &base))
    }
}

// ===========================================================================
// gtrgm_same
// ===========================================================================

/// `gtrgm_same(a, b, &result)`.
pub fn gtrgm_same(a_image: &[u8], b_image: &[u8]) -> PgResult<bool> {
    let a = decode_key(a_image)?;
    let b = decode_key(b_image)?;
    // C: `if (ISSIGNKEY(a)) { /* then b also ISSIGNKEY */ ... } else { /* a,b ISARRKEY */ ... }`.
    let res = if a.flag() & SIGNKEY != 0 {
        match (&a, &b) {
            (TrgmKey::AllTrue, TrgmKey::AllTrue) => true,
            (TrgmKey::AllTrue, _) => false,
            (_, TrgmKey::AllTrue) => false,
            (TrgmKey::Sign(sa), TrgmKey::Sign(sb)) => sa == sb,
            // ISSIGNKEY(a) guarantees b is a signature key too.
            _ => false,
        }
    } else {
        match (&a, &b) {
            (TrgmKey::Arr(pa), TrgmKey::Arr(pb)) => {
                pa.len() == pb.len()
                    && pa
                        .iter()
                        .zip(pb.iter())
                        .all(|(x, y)| cmp_trgm(x, y) == core::cmp::Ordering::Equal)
            }
            _ => false,
        }
    };
    Ok(res)
}

// ===========================================================================
// gtrgm_penalty
// ===========================================================================

/// `hemdist(a, b, siglen)` over decoded keys.
fn hemdist(a: &TrgmKey, asign: &[u8], b: &TrgmKey, bsign: &[u8], siglen: usize) -> i32 {
    let a_all = matches!(a, TrgmKey::AllTrue);
    let b_all = matches!(b, TrgmKey::AllTrue);
    if a_all {
        if b_all {
            0
        } else {
            siglenbit(siglen) as i32 - sizebitvec(bsign)
        }
    } else if b_all {
        siglenbit(siglen) as i32 - sizebitvec(asign)
    } else {
        hemdistsign(asign, bsign, siglen)
    }
}

/// `gtrgm_penalty(origentry, newentry, &penalty)`. `origval` is always a
/// signature key (build-time signature); `newval` may be an ARRKEY (leaf) or a
/// signature key (inner).
pub fn gtrgm_penalty(orig_image: &[u8], new_image: &[u8]) -> PgResult<f32> {
    let siglen = SIGLEN_DEFAULT;
    let origval = decode_key(orig_image)?;
    let newval = decode_key(new_image)?;

    let penalty: f32 = match &newval {
        TrgmKey::Arr(arr) => {
            // sign = makesign(newval); then distance from origval's signature.
            let sign = makesign(arr, siglen);
            match &origval {
                TrgmKey::AllTrue => {
                    ((siglenbit(siglen) as i32 - sizebitvec(&sign)) as f32)
                        / ((siglenbit(siglen) + 1) as f32)
                }
                TrgmKey::Sign(orig) => hemdistsign(&sign, orig, siglen) as f32,
                TrgmKey::Arr(_) => {
                    // origval always ISSIGNKEY per C; fall back exactly via hemdist.
                    hemdistsign(&sign, &makesign(expect_arr(&origval)?, siglen), siglen) as f32
                }
            }
        }
        _ => {
            // newval is a signature key: hemdist(origval, newval).
            let osign = signbytes(&origval);
            let nsign = signbytes(&newval);
            hemdist(&origval, &osign, &newval, &nsign, siglen) as f32
        }
    };
    Ok(penalty)
}

/// Extract the signature bytes of a key for distance math (empty for ALLISTRUE;
/// ARRKEY produces a fresh signature).
fn signbytes(key: &TrgmKey) -> Vec<u8> {
    match key {
        TrgmKey::Sign(s) => s.clone(),
        TrgmKey::AllTrue => Vec::new(),
        TrgmKey::Arr(a) => makesign(a, SIGLEN_DEFAULT),
    }
}

// ===========================================================================
// gtrgm_picksplit
// ===========================================================================

/// One cached signature for picksplit (`CACHESIGN`).
struct CacheSign {
    allistrue: bool,
    sign: Vec<u8>,
}

/// `fillcache(item, key, siglen)`.
fn fillcache(key: &TrgmKey, siglen: usize) -> CacheSign {
    match key {
        TrgmKey::Arr(arr) => CacheSign {
            allistrue: false,
            sign: makesign(arr, siglen),
        },
        TrgmKey::AllTrue => CacheSign {
            allistrue: true,
            sign: vec![0u8; siglen],
        },
        TrgmKey::Sign(s) => CacheSign {
            allistrue: false,
            sign: s.clone(),
        },
    }
}

/// `hemdistcache(a, b, siglen)`.
fn hemdistcache(a: &CacheSign, b: &CacheSign, siglen: usize) -> i32 {
    if a.allistrue {
        if b.allistrue {
            0
        } else {
            siglenbit(siglen) as i32 - sizebitvec(&b.sign)
        }
    } else if b.allistrue {
        siglenbit(siglen) as i32 - sizebitvec(&a.sign)
    } else {
        hemdistsign(&a.sign, &b.sign, siglen)
    }
}

/// `WISH_F(a,b,c)` — the split-balance penalty.
fn wish_f(a: i32, b: i32, c: f64) -> f64 {
    let d = (a - b) as f64;
    -(d * d * d) * c
}

/// `gtrgm_picksplit(entryvec, &v)`. `entries` is indexed 1-based (index 0 is the
/// unread placeholder). Returns `(spl_left, spl_right, ldatum_image,
/// rdatum_image)`.
pub fn gtrgm_picksplit(
    entries: &[(Vec<u8>, bool)],
) -> PgResult<(Vec<u16>, Vec<u16>, Vec<u8>, Vec<u8>)> {
    let siglen = SIGLEN_DEFAULT;
    // maxoff = entryvec->n - 1; offsets run FirstOffsetNumber..=maxoff (1-based).
    let n = entries.len();
    if n < 3 {
        return Err(PgError::error(
            "gtrgm_picksplit: fewer than two entries to split",
        ));
    }
    let maxoff: usize = n - 1;

    // cache[k] for k in 1..=maxoff.
    let mut cache: Vec<Option<CacheSign>> = (0..=maxoff).map(|_| None).collect();
    for k in 1..=maxoff {
        let (img, is_null) = &entries[k];
        if *is_null {
            return Err(PgError::error("gtrgm_picksplit: NULL entry key"));
        }
        let key = decode_key(img)?;
        cache[k] = Some(fillcache(&key, siglen));
    }

    // find two furthest-apart items.
    let mut waste = -1i32;
    let mut seed_1 = 0usize;
    let mut seed_2 = 0usize;
    for k in 1..maxoff {
        for j in (k + 1)..=maxoff {
            let sw = hemdistcache(
                cache[j].as_ref().unwrap(),
                cache[k].as_ref().unwrap(),
                siglen,
            );
            if sw > waste {
                waste = sw;
                seed_1 = k;
                seed_2 = j;
            }
        }
    }
    if seed_1 == 0 || seed_2 == 0 {
        seed_1 = 1;
        seed_2 = 2;
    }

    let mut spl_left: Vec<u16> = Vec::new();
    let mut spl_right: Vec<u16> = Vec::new();

    // form initial datum_l/datum_r from the two seeds. C never flips the
    // allistrue flags mid-loop (a non-ALLISTRUE group that fills to all-0xff
    // stays a plain SIGNKEY), so they are fixed at seed time.
    let datum_l_allistrue = cache[seed_1].as_ref().unwrap().allistrue;
    let mut union_l = cache[seed_1].as_ref().unwrap().sign.clone();
    let datum_r_allistrue = cache[seed_2].as_ref().unwrap().allistrue;
    let mut union_r = cache[seed_2].as_ref().unwrap().sign.clone();

    // sort cost vector by abs(size_alpha - size_beta).
    let mut costvector: Vec<(usize, i32)> = Vec::with_capacity(maxoff);
    for j in 1..=maxoff {
        let size_alpha = hemdistcache(
            cache[seed_1].as_ref().unwrap(),
            cache[j].as_ref().unwrap(),
            siglen,
        );
        let size_beta = hemdistcache(
            cache[seed_2].as_ref().unwrap(),
            cache[j].as_ref().unwrap(),
            siglen,
        );
        costvector.push((j, (size_alpha - size_beta).abs()));
    }
    // C qsort with comparecost: ascending by cost. The C comparator returns 0
    // for ties (not a stable order); a stable sort by cost reproduces the
    // platform's typical qsort result for these inputs.
    costvector.sort_by(|a, b| a.1.cmp(&b.1));

    for (j, _cost) in costvector.iter().copied() {
        if j == seed_1 {
            spl_left.push(j as u16);
            continue;
        } else if j == seed_2 {
            spl_right.push(j as u16);
            continue;
        }

        let cj = cache[j].as_ref().unwrap();

        let size_alpha = if datum_l_allistrue || cj.allistrue {
            if datum_l_allistrue && cj.allistrue {
                0
            } else {
                // size = SIGLENBIT - sizebitvec( (cj.allistrue) ? union_l : cj.sign )
                let s = if cj.allistrue { &union_l } else { &cj.sign };
                siglenbit(siglen) as i32 - sizebitvec(s)
            }
        } else {
            hemdistsign(&cj.sign, &union_l, siglen)
        };

        let size_beta = if datum_r_allistrue || cj.allistrue {
            if datum_r_allistrue && cj.allistrue {
                0
            } else {
                let s = if cj.allistrue { &union_r } else { &cj.sign };
                siglenbit(siglen) as i32 - sizebitvec(s)
            }
        } else {
            hemdistsign(&cj.sign, &union_r, siglen)
        };

        if (size_alpha as f64)
            < size_beta as f64 + wish_f(spl_left.len() as i32, spl_right.len() as i32, 0.1)
        {
            if datum_l_allistrue || cj.allistrue {
                if !datum_l_allistrue {
                    // memset(union_l, 0xff, siglen) — datum_l becomes all-true in
                    // its signature (flag stays SIGNKEY; ALLISTRUE is decided at
                    // encode by all-0xff? No: C keeps datum_l as SIGNKEY with an
                    // all-0xff signature here). Mark its bytes 0xff.
                    for b in union_l.iter_mut() {
                        *b = 0xff;
                    }
                }
            } else {
                for i in 0..siglen {
                    union_l[i] |= cj.sign[i];
                }
            }
            spl_left.push(j as u16);
        } else {
            if datum_r_allistrue || cj.allistrue {
                if !datum_r_allistrue {
                    for b in union_r.iter_mut() {
                        *b = 0xff;
                    }
                }
            } else {
                for i in 0..siglen {
                    union_r[i] |= cj.sign[i];
                }
            }
            spl_right.push(j as u16);
        }
        // Note: C never flips datum_{l,r}_allistrue mid-loop — a non-ALLISTRUE
        // group whose signature fills to all-0xff stays a plain SIGNKEY.
    }

    // v->spl_ldatum / v->spl_rdatum = the seed keys gtrgm_alloc'd with their
    // (possibly 0xff-filled) signatures.
    let ldatum = encode_signkey(datum_l_allistrue, &union_l);
    let rdatum = encode_signkey(datum_r_allistrue, &union_r);

    Ok((spl_left, spl_right, ldatum, rdatum))
}

// ===========================================================================
// Unported regexp NFA engine.
// ===========================================================================

/// The RegExp / RegExpICase strategies require the `trgm_regexp.c` NFA engine
/// (`createTrgmNFA` / `trigramsMatchGraph`, ~2360 lines), which is not ported.
pub fn unported_regexp(fname: &str) -> PgError {
    PgError::error(format!(
        "pg_trgm: the regexp index strategy (~ / ~* via {fname}) is not ported — \
         it needs the trgm_regexp.c NFA engine (createTrgmNFA / trigramsMatchGraph). \
         Use a non-regexp pg_trgm GiST/GIN strategy (LIKE / %% similarity / <-> distance)."
    ))
}
