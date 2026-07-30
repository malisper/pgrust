//! contrib/pg_trgm/trgm_gist.c — gist_trgm_ops support-proc cores.
//! Key: varlena [VARHDR(4) | flag(1) | data] (TRGMHDRSIZE 5); ARRKEY = n*3
//! trigram bytes (leaf), SIGNKEY = siglen signature bytes, SIGNKEY|ALLISTRUE
//! = no data. siglen comes from the opclass options (default 12).

use types_error::{PgError, PgResult};

use crate::trgm::{cmp_trgm, trgm2int, trgm_contained_by, Trgm};

pub const ARRKEY: u8 = 0x01;
pub const SIGNKEY: u8 = 0x02;
pub const ALLISTRUE: u8 = 0x04;

pub const TRGMHDRSIZE: usize = 4 + 1;
pub const SIGLEN_DEFAULT: usize = 12;
const BITBYTE: usize = 8;

const fn maxalign(len: usize) -> usize {
    (len + 7) & !7usize
}
const fn maxalign_down(len: usize) -> usize {
    len & !7usize
}
const GIST_MAX_INDEX_TUPLE_SIZE: usize =
    maxalign_down((8192 - 24 - 16) / 4 - 4);
pub const SIGLEN_MAX: i32 = (GIST_MAX_INDEX_TUPLE_SIZE - maxalign(8)) as i32;

pub const SIMILARITY_STRATEGY: u16 = 1;
pub const DISTANCE_STRATEGY: u16 = 2;
pub const LIKE_STRATEGY: u16 = 3;
pub const ILIKE_STRATEGY: u16 = 4;
pub const REGEXP_STRATEGY: u16 = 5;
pub const REGEXP_ICASE_STRATEGY: u16 = 6;
pub const WORD_SIMILARITY_STRATEGY: u16 = 7;
pub const WORD_DISTANCE_STRATEGY: u16 = 8;
pub const STRICT_WORD_SIMILARITY_STRATEGY: u16 = 9;
pub const STRICT_WORD_DISTANCE_STRATEGY: u16 = 10;
pub const EQUAL_STRATEGY: u16 = 11;

#[derive(Clone, Debug)]
pub enum TrgmKey {
    Arr(Vec<Trgm>),
    Sign(Vec<u8>),
    AllTrue,
}

// The image may carry trailing MAXALIGN padding: payload bounded by VARSIZE.
pub fn decode_key(image: &[u8]) -> PgResult<TrgmKey> {
    if image.len() < TRGMHDRSIZE {
        return Err(PgError::error("corrupt gtrgm GiST key (short header)").into());
    }
    let raw = u32::from_ne_bytes([image[0], image[1], image[2], image[3]]);
    let varsize = (raw >> 2) as usize;
    let end = varsize.min(image.len()).max(TRGMHDRSIZE);
    let flag = image[4];
    let data = &image[TRGMHDRSIZE..end];
    if flag & ARRKEY != 0 {
        if data.len() % 3 != 0 {
            return Err(
                PgError::error("corrupt gtrgm GiST ARRKEY (length not a multiple of 3)").into(),
            );
        }
        Ok(TrgmKey::Arr(data.chunks_exact(3).map(|c| [c[0], c[1], c[2]]).collect()))
    } else if flag & ALLISTRUE != 0 {
        Ok(TrgmKey::AllTrue)
    } else {
        Ok(TrgmKey::Sign(data.to_vec()))
    }
}

fn varsize_header(size: usize) -> [u8; 4] {
    ((size as u32) << 2).to_ne_bytes()
}

pub fn encode_arrkey(arr: &[Trgm]) -> Vec<u8> {
    let size = TRGMHDRSIZE + arr.len() * 3;
    let mut img = Vec::with_capacity(size);
    img.extend_from_slice(&varsize_header(size));
    img.push(ARRKEY);
    for t in arr {
        img.extend_from_slice(t);
    }
    img
}

pub fn encode_signkey(isalltrue: bool, sign: &[u8]) -> Vec<u8> {
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

// SIGLENBIT(siglen) = siglen * BITBYTE - 1 (last bit reserved).
fn siglenbit(siglen: usize) -> usize {
    siglen * BITBYTE - 1
}

fn hashval(val: u32, siglen: usize) -> usize {
    (val as usize) % siglenbit(siglen)
}

fn getbit(sign: &[u8], i: usize) -> bool {
    (sign[i / BITBYTE] >> (i % BITBYTE)) & 0x01 != 0
}

fn setbit(sign: &mut [u8], i: usize) {
    sign[i / BITBYTE] |= 0x01 << (i % BITBYTE);
}

pub fn makesign(arr: &[Trgm], siglen: usize) -> Vec<u8> {
    let mut sign = vec![0u8; siglen];
    setbit(&mut sign, siglenbit(siglen));
    for t in arr {
        setbit(&mut sign, hashval(trgm2int(t), siglen));
    }
    sign
}

fn sizebitvec(sign: &[u8]) -> i32 {
    sign.iter().map(|b| b.count_ones() as i32).sum()
}

fn hemdistsign(a: &[u8], b: &[u8], siglen: usize) -> i32 {
    (0..siglen).map(|i| (a[i] ^ b[i]).count_ones() as i32).sum()
}

fn cnt_sml_sign_common(qtrg: &[Trgm], sign: &[u8], siglen: usize) -> i32 {
    qtrg.iter()
        .filter(|t| getbit(sign, hashval(trgm2int(t), siglen)))
        .count() as i32
}

#[track_caller]
#[cold]
fn unrecognized_strategy(strategy: u16) -> Box<PgError> {
    Box::new(PgError::error(format!("unrecognized strategy number: {strategy}")))
}

fn expect_arr(key: &TrgmKey) -> PgResult<&[Trgm]> {
    match key {
        TrgmKey::Arr(a) => Ok(a),
        _ => Err(PgError::error("gtrgm GiST: expected a leaf ARRKEY but found a signature key")
            .into()),
    }
}

// gtrgm_consistent core: (matched, recheck). qtrg per strategy is extracted
// by the caller (which owns the TrgmEnv services).
pub fn consistent(
    is_leaf: bool,
    key: &TrgmKey,
    qtrg: &[Trgm],
    strategy: u16,
    nlimit: f64,
) -> PgResult<(bool, bool)> {
    match strategy {
        SIMILARITY_STRATEGY | WORD_SIMILARITY_STRATEGY | STRICT_WORD_SIMILARITY_STRATEGY => {
            let recheck = strategy != SIMILARITY_STRATEGY;
            let res = if is_leaf {
                let key_arr = expect_arr(key)?;
                (crate::trgm::cnt_sml(qtrg, key_arr, recheck) as f64) >= nlimit
            } else {
                match key {
                    TrgmKey::AllTrue => true,
                    TrgmKey::Sign(sign) => {
                        let count = cnt_sml_sign_common(qtrg, sign, sign.len());
                        let len = qtrg.len() as i32;
                        len != 0 && ((count as f64) / (len as f64)) >= nlimit
                    }
                    TrgmKey::Arr(_) => {
                        let key_arr = expect_arr(key)?;
                        (crate::trgm::cnt_sml(qtrg, key_arr, recheck) as f64) >= nlimit
                    }
                }
            };
            Ok((res, recheck))
        }
        LIKE_STRATEGY | ILIKE_STRATEGY | EQUAL_STRATEGY => {
            let res = if is_leaf {
                trgm_contained_by(qtrg, expect_arr(key)?)
            } else {
                match key {
                    TrgmKey::AllTrue => true,
                    TrgmKey::Sign(sign) => qtrg
                        .iter()
                        .all(|t| getbit(sign, hashval(trgm2int(t), sign.len()))),
                    TrgmKey::Arr(_) => trgm_contained_by(qtrg, expect_arr(key)?),
                }
            };
            Ok((res, true))
        }
        REGEXP_STRATEGY | REGEXP_ICASE_STRATEGY => {
            unreachable!("regexp strategies take the consistent_regexp path")
        }
        other => Err(unrecognized_strategy(other)),
    }
}

// gtrgm_consistent regexp arms: evaluate the packed graph against the
// trigrams that can be present under this key. `qtrg` None = regex too
// complex; everything matches pending recheck. Always inexact.
pub fn consistent_regexp(
    is_leaf: bool,
    key: &TrgmKey,
    qtrg: Option<&[Trgm]>,
    graph: &mut gin_vocab::TrgmPackedGraph,
) -> PgResult<bool> {
    let Some(qtrg) = qtrg else {
        return Ok(true);
    };
    Ok(match key {
        TrgmKey::Arr(arr) if is_leaf => {
            graph.matches(&crate::trgm::trgm_presence_map(qtrg, arr))
        }
        TrgmKey::AllTrue => true,
        TrgmKey::Sign(sign) => {
            // Signature bits give false positives only; the graph is
            // monotone, so evaluating over them can't produce a false
            // negative.
            let check: Vec<bool> = qtrg
                .iter()
                .map(|t| getbit(sign, hashval(trgm2int(t), sign.len())))
                .collect();
            graph.matches(&check)
        }
        TrgmKey::Arr(arr) => graph.matches(&crate::trgm::trgm_presence_map(qtrg, arr)),
    })
}

// gtrgm_distance core: (distance, recheck).
pub fn distance(
    is_leaf: bool,
    key: &TrgmKey,
    qtrg: &[Trgm],
    strategy: u16,
) -> PgResult<(f64, bool)> {
    match strategy {
        DISTANCE_STRATEGY | WORD_DISTANCE_STRATEGY | STRICT_WORD_DISTANCE_STRATEGY => {
            let recheck = strategy != DISTANCE_STRATEGY;
            let res = if is_leaf {
                1.0 - crate::trgm::cnt_sml(qtrg, expect_arr(key)?, recheck) as f64
            } else {
                match key {
                    TrgmKey::AllTrue => 0.0,
                    TrgmKey::Sign(sign) => {
                        let count = cnt_sml_sign_common(qtrg, sign, sign.len());
                        let len = qtrg.len() as i32;
                        if len == 0 {
                            -1.0
                        } else {
                            1.0 - (count as f64) / (len as f64)
                        }
                    }
                    TrgmKey::Arr(_) => {
                        1.0 - crate::trgm::cnt_sml(qtrg, expect_arr(key)?, recheck) as f64
                    }
                }
            };
            Ok((res, recheck))
        }
        other => Err(unrecognized_strategy(other)),
    }
}

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
                setbit(sbase, hashval(trgm2int(t), siglen));
            }
            false
        }
    }
}

pub fn union(keys: &[TrgmKey], siglen: usize) -> Vec<u8> {
    let mut base = vec![0u8; siglen];
    for k in keys {
        if unionkey(&mut base, k, siglen) {
            return encode_signkey(true, &base);
        }
    }
    encode_signkey(false, &base)
}

pub fn same(a: &TrgmKey, b: &TrgmKey, siglen: usize) -> bool {
    match a {
        TrgmKey::AllTrue => matches!(b, TrgmKey::AllTrue),
        TrgmKey::Sign(sa) => match b {
            TrgmKey::Sign(sb) => sa[..siglen] == sb[..siglen],
            _ => false,
        },
        TrgmKey::Arr(pa) => match b {
            TrgmKey::Arr(pb) => {
                pa.len() == pb.len()
                    && pa
                        .iter()
                        .zip(pb.iter())
                        .all(|(x, y)| cmp_trgm(x, y) == core::cmp::Ordering::Equal)
            }
            _ => false,
        },
    }
}

fn hemdist_keys(a: &TrgmKey, b: &TrgmKey, siglen: usize) -> i32 {
    let a_all = matches!(a, TrgmKey::AllTrue);
    let b_all = matches!(b, TrgmKey::AllTrue);
    if a_all {
        if b_all {
            0
        } else {
            siglenbit(siglen) as i32 - sizebitvec(sign_of(b, siglen).as_ref())
        }
    } else if b_all {
        siglenbit(siglen) as i32 - sizebitvec(sign_of(a, siglen).as_ref())
    } else {
        hemdistsign(sign_of(a, siglen).as_ref(), sign_of(b, siglen).as_ref(), siglen)
    }
}

fn sign_of(key: &TrgmKey, siglen: usize) -> std::borrow::Cow<'_, [u8]> {
    match key {
        TrgmKey::Sign(s) => std::borrow::Cow::Borrowed(&s[..]),
        TrgmKey::AllTrue => std::borrow::Cow::Owned(vec![0u8; siglen]),
        TrgmKey::Arr(a) => std::borrow::Cow::Owned(makesign(a, siglen)),
    }
}

pub fn penalty(origval: &TrgmKey, newval: &TrgmKey, siglen: usize) -> f32 {
    match newval {
        TrgmKey::Arr(arr) => {
            let sign = makesign(arr, siglen);
            match origval {
                TrgmKey::AllTrue => {
                    ((siglenbit(siglen) as i32 - sizebitvec(&sign)) as f32)
                        / ((siglenbit(siglen) + 1) as f32)
                }
                _ => hemdistsign(&sign, sign_of(origval, siglen).as_ref(), siglen) as f32,
            }
        }
        _ => hemdist_keys(origval, newval, siglen) as f32,
    }
}

struct CacheSign {
    allistrue: bool,
    sign: Vec<u8>,
}

fn fillcache(key: &TrgmKey, siglen: usize) -> CacheSign {
    match key {
        TrgmKey::Arr(arr) => CacheSign { allistrue: false, sign: makesign(arr, siglen) },
        TrgmKey::AllTrue => CacheSign { allistrue: true, sign: vec![0u8; siglen] },
        TrgmKey::Sign(s) => CacheSign { allistrue: false, sign: s.clone() },
    }
}

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

fn wish_f(a: i32, b: i32, c: f64) -> f64 {
    let d = (a - b) as f64;
    -(d * d * d) * c
}

// gtrgm_picksplit: keys indexed 1..=maxoff (index 0 unused, matching the
// entryvec layout). Returns (spl_left, spl_right, ldatum_img, rdatum_img).
pub fn picksplit(
    keys: &[Option<TrgmKey>],
    siglen: usize,
) -> PgResult<(Vec<u16>, Vec<u16>, Vec<u8>, Vec<u8>)> {
    let maxoff = keys.len() - 1;
    if maxoff < 2 {
        return Err(PgError::error("gtrgm_picksplit: fewer than two entries to split").into());
    }

    let mut cache: Vec<Option<CacheSign>> = Vec::with_capacity(maxoff + 1);
    cache.push(None);
    for k in 1..=maxoff {
        let key = keys[k]
            .as_ref()
            .ok_or_else(|| PgError::error("gtrgm_picksplit: NULL entry key"))?;
        cache.push(Some(fillcache(key, siglen)));
    }

    let mut waste = -1i32;
    let mut seed_1 = 0usize;
    let mut seed_2 = 0usize;
    for k in 1..maxoff {
        for j in (k + 1)..=maxoff {
            let sw = hemdistcache(cache[j].as_ref().unwrap(), cache[k].as_ref().unwrap(), siglen);
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

    // C never flips the seeds' allistrue flags mid-loop; a non-ALLISTRUE
    // group that fills to all-0xff stays a plain SIGNKEY.
    let datum_l_allistrue = cache[seed_1].as_ref().unwrap().allistrue;
    let mut union_l = cache[seed_1].as_ref().unwrap().sign.clone();
    let datum_r_allistrue = cache[seed_2].as_ref().unwrap().allistrue;
    let mut union_r = cache[seed_2].as_ref().unwrap().sign.clone();

    let mut costvector: Vec<(usize, i32)> = Vec::with_capacity(maxoff);
    for j in 1..=maxoff {
        let size_alpha =
            hemdistcache(cache[seed_1].as_ref().unwrap(), cache[j].as_ref().unwrap(), siglen);
        let size_beta =
            hemdistcache(cache[seed_2].as_ref().unwrap(), cache[j].as_ref().unwrap(), siglen);
        costvector.push((j, (size_alpha - size_beta).abs()));
    }
    costvector.sort_by(|a, b| a.1.cmp(&b.1));

    for (j, _) in costvector {
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
                    union_l.fill(0xff);
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
                    union_r.fill(0xff);
                }
            } else {
                for i in 0..siglen {
                    union_r[i] |= cj.sign[i];
                }
            }
            spl_right.push(j as u16);
        }
    }

    let ldatum = encode_signkey(datum_l_allistrue, &union_l);
    let rdatum = encode_signkey(datum_r_allistrue, &union_r);
    Ok((spl_left, spl_right, ldatum, rdatum))
}
