//! `contrib/hstore/hstore_gist.c` — the `gist_hstore_ops` GiST opclass support
//! functions (`ghstore_*`), ported over the generic catalog-driven GiST opclass
//! dispatch.
//!
//! The GiST core resolves each opclass support proc into an `FmgrInfo` and
//! `gist-proc`'s `extdispatch` invokes the body here through a real fmgr frame,
//! passing the `GISTENTRY`/`GistEntryVector`/`GIST_SPLITVEC` + the `*recheck`/
//! `*penalty`/`*size` out-parameters through the [`::gist::extproc`] internal
//! protocol structs (slot 0), and the `consistent` query (hstore / text /
//! text[]) on the by-ref lane (slot 1). The body reads its typed inputs off the
//! protocol struct, runs the ghstore signature logic, and writes its outputs
//! back.
//!
//! ## Key representation (`GISTTYPE`)
//!
//! A ghstore GiST key is a varlena:
//! `[ VARHDR(4) | int32 flag | data ]` (`GTHDRSIZE == 8`). Two key kinds:
//! * a plain signature — `data` is `siglen` signature bytes;
//! * `ALLISTRUE` (`flag & 0x04`) — no `data` (the all-ones signature).
//!
//! Unlike `pg_trgm`, ghstore has NO leaf-array form: the leaf `compress` builds
//! a signature directly from the hstore key/value CRCs.
//!
//! ## `siglen`
//!
//! C reads the opclass `siglen` option via `GET_SIGLEN()`. The owned GiST
//! dispatch does not thread opclass options to the support procs (the documented
//! `tsvector_ops` / `pg_trgm` divergence), so the build side uses
//! [`SIGLEN_DEFAULT`] and the read side reads the stored signature's own length.
//! This is exact for the default index (no explicit `siglen`), which is what the
//! regression tests use; only an index built with an explicit non-default
//! `siglen` would differ in its physical signature length (queries still recheck
//! on the heap, so results are correct regardless).

use ::types_error::{PgError, PgResult};

use crate::repr::HstoreView;
use crate::TEXTOID;

// hstore strategy numbers (hstore.h).
const HSTORE_CONTAINS_STRATEGY: u16 = 7;
const HSTORE_EXISTS_STRATEGY: u16 = 9;
const HSTORE_EXISTS_ANY_STRATEGY: u16 = 10;
const HSTORE_EXISTS_ALL_STRATEGY: u16 = 11;
const HSTORE_OLD_CONTAINS_STRATEGY: u16 = 13;

const BITBYTE: usize = 8;

/// `SIGLEN_DEFAULT = sizeof(int32) * 4` (16 bytes).
pub const SIGLEN_DEFAULT: usize = 16;

/// `ALLISTRUE` flag bit (`hstore_gist.c`).
const ALLISTRUE: i32 = 0x04;

/// `GTHDRSIZE = VARHDRSZ + sizeof(int32)`.
const GTHDRSIZE: usize = 4 + 4;

// SIGLEN_MAX = GISTMaxIndexKeySize (access/gist.h). Mirror of the trgm_gist
// derivation so the `ghstore_options` siglen reloption has the same upper bound
// as C, without pulling another crate as a dep.
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

/// `offsetof(GistHstoreOptions, siglen)` — `siglen` follows the 4-byte varlena
/// header.
const OFFSETOF_GIST_HSTORE_OPTIONS_SIGLEN: i32 = 4;
/// `sizeof(GistHstoreOptions)` (`int32 vl_len_` + `int siglen`).
const SIZEOF_GIST_HSTORE_OPTIONS: usize = 8;

// ===========================================================================
// Signature bitmap primitives (hstore_gist.c macros).
// ===========================================================================

/// `SIGLENBIT(siglen) = siglen * BITBYTE`. (NB: hstore does NOT reserve a final
/// bit, unlike pg_trgm.)
fn siglenbit(siglen: usize) -> usize {
    siglen * BITBYTE
}

/// `HASHVAL(val, siglen) = ((unsigned int) val) % SIGLENBIT(siglen)`.
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

/// `HASH(sign, val, siglen)` — set the hashed bit.
fn hash(sign: &mut [u8], val: u32, siglen: usize) {
    setbit(sign, hashval(val, siglen));
}

/// `pg_popcount(sign)` — number of set bits (`sizebitvec`).
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

/// `crc32_sz(buf)` — the traditional (legacy) CRC-32 of a chunk of data.
fn crc32_sz(buf: &[u8]) -> u32 {
    ::crc32c::legacy::traditional_crc32(buf)
}

// ===========================================================================
// GISTTYPE key decode / encode (varlena image <-> typed value).
// ===========================================================================

/// A decoded ghstore GiST key.
#[derive(Clone, Debug)]
enum GhKey {
    /// A plain `siglen`-byte signature bitmap.
    Sign(Vec<u8>),
    /// `ALLISTRUE` — no signature stored.
    AllTrue,
}

impl GhKey {
    fn is_alltrue(&self) -> bool {
        matches!(self, GhKey::AllTrue)
    }
}

/// `SET_VARSIZE(ptr, size)` — the 4-byte ("4B-U") varlena length word.
fn varsize_header(size: usize) -> [u8; 4] {
    ((size as u32) << 2).to_ne_bytes()
}

/// Decode a header-ful `GISTTYPE` varlena image into a [`GhKey`].
///
/// The image may carry trailing alignment padding (the GiST core stores index
/// keys MAXALIGN'ed), so the payload is bounded by the varlena length word
/// (`VARSIZE`), NOT by `image.len()`.
fn decode_key(image: &[u8]) -> PgResult<GhKey> {
    if image.len() < GTHDRSIZE {
        return Err(PgError::error("corrupt ghstore GiST key (short header)"));
    }
    // VARSIZE of a 4-byte (non-short) varlena: the length word is `len << 2`
    // native-endian.
    let raw = u32::from_ne_bytes([image[0], image[1], image[2], image[3]]);
    let varsize = ((raw >> 2) as usize).min(image.len()).max(GTHDRSIZE);
    let flag = i32::from_ne_bytes([image[4], image[5], image[6], image[7]]);
    if flag & ALLISTRUE != 0 {
        Ok(GhKey::AllTrue)
    } else {
        Ok(GhKey::Sign(image[GTHDRSIZE..varsize].to_vec()))
    }
}

/// `ghstore_alloc(allistrue, siglen, sign)` — build a `GISTTYPE` varlena image.
/// When `allistrue`, no signature data is stored.
fn ghstore_alloc(allistrue: bool, siglen: usize, sign: Option<&[u8]>) -> Vec<u8> {
    let flag: i32 = if allistrue { ALLISTRUE } else { 0 };
    let datalen = if allistrue { 0 } else { siglen };
    let size = GTHDRSIZE + datalen;
    let mut img = Vec::with_capacity(size);
    img.extend_from_slice(&varsize_header(size));
    img.extend_from_slice(&flag.to_ne_bytes());
    if !allistrue {
        match sign {
            Some(s) => {
                debug_assert_eq!(s.len(), siglen);
                img.extend_from_slice(s);
            }
            None => img.extend_from_slice(&vec![0u8; siglen]),
        }
    }
    img
}

// ===========================================================================
// ghstore_options
// ===========================================================================

/// `ghstore_options(relopts)` (hstore_gist.c) — register the `siglen` opclass
/// option on the `local_relopts` parse table. (The configured value is not
/// threaded back to the support procs — the documented default-siglen
/// divergence — but the option must still be REGISTERED so `CREATE INDEX (...
/// WITH (siglen = N))` parses and the local-reloptions build produces a
/// well-formed buffer.)
pub fn ghstore_options(relopts: &mut ::types_reloptions::local_relopts) {
    ::reloptions_seams::init_local_reloptions::call(relopts, SIZEOF_GIST_HSTORE_OPTIONS);
    ::reloptions_seams::add_local_int_reloption::call(
        relopts,
        "siglen",
        Some("signature length in bytes"),
        SIGLEN_DEFAULT as i32,
        1,
        SIGLEN_MAX,
        OFFSETOF_GIST_HSTORE_OPTIONS_SIGLEN,
    );
}

// ===========================================================================
// ghstore_compress / ghstore_decompress
// ===========================================================================

/// `ghstore_compress(entry)` — convert a leaf hstore value into its signature;
/// rewrite an all-0xff inner signature to `ALLISTRUE`; otherwise pass through.
/// Inputs: `leafkey`, the (detoasted) leaf hstore image or the stored key image.
/// Returns `Some(new_key_image)` or `None` (pass-through).
pub fn ghstore_compress(
    leafkey: bool,
    key_image: &[u8],
    key_is_null: bool,
) -> PgResult<Option<Vec<u8>>> {
    let siglen = SIGLEN_DEFAULT;
    if leafkey {
        if key_is_null || key_image.len() < 4 {
            return Err(PgError::error("ghstore_compress: NULL leaf key"));
        }
        // DatumGetHStoreP(entry->key) — the detoasted leaf hstore carries a
        // 4-byte header; the body is VARDATA.
        let raw = u32::from_ne_bytes([key_image[0], key_image[1], key_image[2], key_image[3]]);
        let varsize = ((raw >> 2) as usize).min(key_image.len()).max(4);
        let body = &key_image[4..varsize];
        let val = HstoreView::from_vardata(body);
        let mut sign = vec![0u8; siglen];
        let count = val.count();
        for i in 0..count {
            let h = crc32_sz(val.key(i));
            hash(&mut sign, h, siglen);
            if !val.val_isnull(i) {
                let h = crc32_sz(val.val(i));
                hash(&mut sign, h, siglen);
            }
        }
        return Ok(Some(ghstore_alloc(false, siglen, Some(&sign))));
    }
    // Inner entry. A NULL key is not a signature → pass through.
    if key_is_null {
        return Ok(None);
    }
    let k = decode_key(key_image)?;
    if let GhKey::Sign(sign) = k {
        // !ISALLTRUE: rewrite to ALLISTRUE iff every byte is 0xff.
        if sign.iter().all(|&b| b == 0xff) {
            return Ok(Some(ghstore_alloc(true, siglen, None)));
        }
    }
    Ok(None)
}

/// `ghstore_decompress(entry)` — no-op (ghstore isn't toastable). The owned
/// by-ref lane already delivers a plain image, so it is the identity
/// (pass-through).
pub fn ghstore_decompress() -> Option<Vec<u8>> {
    None
}

// ===========================================================================
// ghstore_same
// ===========================================================================

/// `ghstore_same(a, b, &result)`.
pub fn ghstore_same(a_image: &[u8], b_image: &[u8]) -> PgResult<bool> {
    let a = decode_key(a_image)?;
    let b = decode_key(b_image)?;
    let res = match (&a, &b) {
        (GhKey::AllTrue, GhKey::AllTrue) => true,
        (GhKey::AllTrue, _) => false,
        (_, GhKey::AllTrue) => false,
        (GhKey::Sign(sa), GhKey::Sign(sb)) => sa == sb,
    };
    Ok(res)
}

// ===========================================================================
// ghstore_union
// ===========================================================================

/// `hemdist(a, b, siglen)` over decoded keys.
fn hemdist(a: &GhKey, b: &GhKey, siglen: usize) -> i32 {
    if a.is_alltrue() {
        if b.is_alltrue() {
            0
        } else {
            siglenbit(siglen) as i32 - sizebitvec(sign_of(b))
        }
    } else if b.is_alltrue() {
        siglenbit(siglen) as i32 - sizebitvec(sign_of(a))
    } else {
        hemdistsign(sign_of(a), sign_of(b), siglen)
    }
}

/// `unionkey(sbase, add, siglen)` — OR the `add` key into the signature; returns
/// `true` if the result must become ALLISTRUE.
fn unionkey(sbase: &mut [u8], add: &GhKey, siglen: usize) -> bool {
    match add {
        GhKey::AllTrue => true,
        GhKey::Sign(sadd) => {
            for i in 0..siglen {
                sbase[i] |= sadd[i];
            }
            false
        }
    }
}

/// `ghstore_union(entryvec, &size)`. `entries` is every member's key image.
pub fn ghstore_union(entries: &[(Vec<u8>, bool)]) -> PgResult<Vec<u8>> {
    let siglen = SIGLEN_DEFAULT;
    // result = ghstore_alloc(false, siglen, NULL): a zeroed signature.
    let mut base = vec![0u8; siglen];
    let mut allistrue = false;
    for (img, is_null) in entries {
        if *is_null {
            continue;
        }
        let k = decode_key(img)?;
        if unionkey(&mut base, &k, siglen) {
            allistrue = true;
            break;
        }
    }
    if allistrue {
        Ok(ghstore_alloc(true, siglen, None))
    } else {
        Ok(ghstore_alloc(false, siglen, Some(&base)))
    }
}

// ===========================================================================
// ghstore_penalty
// ===========================================================================

/// `ghstore_penalty(origentry, newentry, &penalty)`. Both keys are signature
/// keys (build-time signatures).
pub fn ghstore_penalty(orig_image: &[u8], new_image: &[u8]) -> PgResult<f32> {
    let siglen = SIGLEN_DEFAULT;
    let origval = decode_key(orig_image)?;
    let newval = decode_key(new_image)?;
    Ok(hemdist(&origval, &newval, siglen) as f32)
}

// ===========================================================================
// ghstore_picksplit
// ===========================================================================

/// `WISH_F(a,b,c)` — the split-balance penalty.
fn wish_f(a: i32, b: i32, c: f64) -> f64 {
    let d = (a - b) as f64;
    -(d * d * d) * c
}

/// `ghstore_picksplit(entryvec, &v)`. `entries` is indexed 1-based (index 0 is
/// the unread placeholder). Returns `(spl_left, spl_right, ldatum_image,
/// rdatum_image)`.
///
/// NB: C's `ghstore_picksplit` sets `maxoff = entryvec->n - 2` for the seed
/// search, then `maxoff = OffsetNumberNext(maxoff)` (== n - 1) for the
/// distribution loop. We mirror that exactly: `entries.len() == n`, so the
/// distribution range is `1..=n-1` and the seed search range is `1..n-1`.
pub fn ghstore_picksplit(
    entries: &[(Vec<u8>, bool)],
) -> PgResult<(Vec<u16>, Vec<u16>, Vec<u8>, Vec<u8>)> {
    let siglen = SIGLEN_DEFAULT;
    let n = entries.len();
    if n < 3 {
        return Err(PgError::error(
            "ghstore_picksplit: fewer than two entries to split",
        ));
    }
    // C: maxoff = entryvec->n - 2 (seed search).
    let seed_maxoff: usize = n - 2;

    // Decode each member key (1-based; index 0 is the placeholder).
    let mut keys: Vec<Option<GhKey>> = (0..n).map(|_| None).collect();
    for k in 1..n {
        let (img, is_null) = &entries[k];
        if *is_null {
            return Err(PgError::error("ghstore_picksplit: NULL entry key"));
        }
        keys[k] = Some(decode_key(img)?);
    }

    // find two furthest-apart items (C: k in First..maxoff-1, j in k+1..=maxoff).
    let mut waste = -1i32;
    let mut seed_1 = 0usize;
    let mut seed_2 = 0usize;
    for k in 1..seed_maxoff {
        for j in (k + 1)..=seed_maxoff {
            let sw = hemdist(
                keys[k].as_ref().unwrap(),
                keys[j].as_ref().unwrap(),
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

    // C: maxoff = OffsetNumberNext(maxoff) → distribution range 1..=n-1.
    let maxoff: usize = n - 1;

    // form initial datum_l/datum_r from the two seeds (ghstore_alloc with the
    // seed's allistrue flag + signature). C keeps these as plain working
    // signatures; the all-true flag is fixed at seed time.
    let datum_l_allistrue = keys[seed_1].as_ref().unwrap().is_alltrue();
    let scratch_all = vec![0u8; siglen];
    let mut union_l: Vec<u8> = match keys[seed_1].as_ref().unwrap() {
        GhKey::Sign(s) => s.clone(),
        GhKey::AllTrue => scratch_all.clone(),
    };
    let datum_r_allistrue = keys[seed_2].as_ref().unwrap().is_alltrue();
    let mut union_r: Vec<u8> = match keys[seed_2].as_ref().unwrap() {
        GhKey::Sign(s) => s.clone(),
        GhKey::AllTrue => scratch_all.clone(),
    };

    // sort cost vector by abs(size_alpha - size_beta), ascending. The cost
    // pre-pass uses the freshly-allocated seed datums (C: `datum_l`/`datum_r`
    // before the distribution loop mutates them), which are exactly the seed
    // (allistrue, signature) pairs == the initial union_l/union_r.
    let mut costvector: Vec<(usize, i32)> = Vec::with_capacity(maxoff);
    for j in 1..=maxoff {
        let kj = keys[j].as_ref().unwrap();
        let size_alpha = hemdist_working(datum_l_allistrue, &union_l, kj, siglen);
        let size_beta = hemdist_working(datum_r_allistrue, &union_r, kj, siglen);
        costvector.push((j, (size_alpha - size_beta).abs()));
    }
    // C qsort with comparecost (ascending by cost). A stable sort by cost
    // reproduces the platform's typical qsort result for these inputs.
    costvector.sort_by(|a, b| a.1.cmp(&b.1));

    let mut spl_left: Vec<u16> = Vec::new();
    let mut spl_right: Vec<u16> = Vec::new();

    for (j, _cost) in costvector.iter().copied() {
        if j == seed_1 {
            spl_left.push(j as u16);
            continue;
        } else if j == seed_2 {
            spl_right.push(j as u16);
            continue;
        }
        let kj = keys[j].as_ref().unwrap();
        let kj_alltrue = kj.is_alltrue();

        // datum_l = (datum_l_allistrue, union_l); datum_r = (datum_r_allistrue,
        // union_r). hemdist against the working datum.
        let size_alpha = hemdist_working(datum_l_allistrue, &union_l, kj, siglen);
        let size_beta = hemdist_working(datum_r_allistrue, &union_r, kj, siglen);

        if (size_alpha as f64)
            < size_beta as f64 + wish_f(spl_left.len() as i32, spl_right.len() as i32, 0.0001)
        {
            if datum_l_allistrue || kj_alltrue {
                if !datum_l_allistrue {
                    for b in union_l.iter_mut() {
                        *b = 0xff;
                    }
                }
            } else {
                let ptr = sign_of(kj);
                for i in 0..siglen {
                    union_l[i] |= ptr[i];
                }
            }
            spl_left.push(j as u16);
        } else {
            if datum_r_allistrue || kj_alltrue {
                if !datum_r_allistrue {
                    for b in union_r.iter_mut() {
                        *b = 0xff;
                    }
                }
            } else {
                let ptr = sign_of(kj);
                for i in 0..siglen {
                    union_r[i] |= ptr[i];
                }
            }
            spl_right.push(j as u16);
        }
    }

    let ldatum = ghstore_alloc(datum_l_allistrue, siglen, Some(&union_l));
    let rdatum = ghstore_alloc(datum_r_allistrue, siglen, Some(&union_r));

    Ok((spl_left, spl_right, ldatum, rdatum))
}

/// The signature bytes of a non-alltrue key.
fn sign_of(key: &GhKey) -> &[u8] {
    match key {
        GhKey::Sign(s) => s,
        GhKey::AllTrue => &[],
    }
}

/// `hemdist` of the working (mutable) seed datum (an allistrue flag + signature
/// bytes) against an entry key — mirrors C's `hemdist(datum_l, _j, siglen)`
/// where `datum_l` is the in-progress union.
fn hemdist_working(datum_allistrue: bool, union: &[u8], other: &GhKey, siglen: usize) -> i32 {
    let other_alltrue = other.is_alltrue();
    if datum_allistrue {
        if other_alltrue {
            0
        } else {
            siglenbit(siglen) as i32 - sizebitvec(sign_of(other))
        }
    } else if other_alltrue {
        siglenbit(siglen) as i32 - sizebitvec(union)
    } else {
        hemdistsign(union, sign_of(other), siglen)
    }
}

// ===========================================================================
// ghstore_consistent
// ===========================================================================

/// `ghstore_consistent(entry, query, strategy, subtype, recheck)`. Returns
/// `(matched, recheck)`.
///
/// The `query` rides the by-ref lane; its shape depends on the strategy:
/// * Contains / OldContains — an hstore (`query_image` is the header-ful hstore);
/// * Exists — a `text` (the key);
/// * ExistsAll / ExistsAny — a `text[]`.
///
/// `query_image` is the FULL header-ful varlena image (the dispatch's by-ref
/// lane delivers the raw bytes). `deconstruct_text_array` parses the array form.
pub fn ghstore_consistent(
    key_image: &[u8],
    key_is_null: bool,
    query_image: &[u8],
    strategy: u16,
) -> PgResult<(bool, bool)> {
    // All cases served by this function are inexact.
    let recheck = true;

    if key_is_null {
        return Err(PgError::error("ghstore_consistent: NULL key"));
    }
    let entry = decode_key(key_image)?;

    // ISALLTRUE(entry) → return true.
    if entry.is_alltrue() {
        return Ok((true, recheck));
    }
    let sign = sign_of(&entry);
    let siglen = sign.len();

    let res = match strategy {
        HSTORE_CONTAINS_STRATEGY | HSTORE_OLD_CONTAINS_STRATEGY => {
            // query is an hstore.
            let body = crate::varlena_payload(query_image);
            let query = HstoreView::from_vardata(body);
            let count = query.count();
            let mut res = true;
            let mut i = 0;
            while res && i < count {
                let crc = crc32_sz(query.key(i));
                if getbit(sign, hashval(crc, siglen)) {
                    if !query.val_isnull(i) {
                        let crc = crc32_sz(query.val(i));
                        if !getbit(sign, hashval(crc, siglen)) {
                            res = false;
                        }
                    }
                } else {
                    res = false;
                }
                i += 1;
            }
            res
        }
        HSTORE_EXISTS_STRATEGY => {
            // query is a text key.
            let key = crate::varlena_payload(query_image);
            let crc = crc32_sz(key);
            getbit(sign, hashval(crc, siglen))
        }
        HSTORE_EXISTS_ALL_STRATEGY => {
            let keys = deconstruct_text_array_local(query_image)?;
            let mut res = true;
            for k in &keys {
                let k = match k {
                    Some(b) => b,
                    None => continue,
                };
                let crc = crc32_sz(k);
                if !getbit(sign, hashval(crc, siglen)) {
                    res = false;
                    break;
                }
            }
            res
        }
        HSTORE_EXISTS_ANY_STRATEGY => {
            let keys = deconstruct_text_array_local(query_image)?;
            let mut res = false;
            for k in &keys {
                let k = match k {
                    Some(b) => b,
                    None => continue,
                };
                let crc = crc32_sz(k);
                if getbit(sign, hashval(crc, siglen)) {
                    res = true;
                    break;
                }
            }
            res
        }
        other => {
            return Err(PgError::error(format!(
                "Unsupported strategy number: {other}"
            )))
        }
    };

    Ok((res, recheck))
}

/// `deconstruct_array_builtin(query, TEXTOID, ...)` over the header-ful array
/// image — returns each element as `Option<Vec<u8>>` (None == SQL NULL).
fn deconstruct_text_array_local(image: &[u8]) -> PgResult<Vec<Option<Vec<u8>>>> {
    let scratch = ::mcx::MemoryContext::new("ghstore consistent text[]");
    let mcx = scratch.mcx();
    let v = arrayfuncs::construct::deconstruct_text_array_nullable(mcx, image)?;
    Ok(v.iter()
        .map(|o| o.as_ref().map(|s| s.as_str().as_bytes().to_vec()))
        .collect())
}

// Keep TEXTOID referenced (documents the array element type, mirroring C's
// deconstruct_array_builtin(query, TEXTOID, ...)).
const _: ::types_core::Oid = TEXTOID;
