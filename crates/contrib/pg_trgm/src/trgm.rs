//! Trigram extraction + similarity core of contrib/pg_trgm/trgm_op.c.
//! Pure logic over byte buffers; backend services (pg_mblen / ISWORDCHR /
//! str_tolower) thread in via TrgmEnv so the unit tests can stub them.

pub type Trgm = [u8; 3];

pub const LPADDING: usize = 2;
pub const RPADDING: usize = 1;

pub const TRGM_BOUND_LEFT: u8 = 0x01;
pub const TRGM_BOUND_RIGHT: u8 = 0x02;

pub const WORD_SIMILARITY_CHECK_ONLY: u8 = 0x01;
pub const WORD_SIMILARITY_STRICT: u8 = 0x02;

pub struct TrgmEnv<'a> {
    pub max_encoding_len: i32,
    pub mblen: &'a dyn Fn(&[u8]) -> i32,
    pub isalnum: &'a dyn Fn(&[u8]) -> bool,
    pub tolower: &'a dyn Fn(&[u8]) -> Vec<u8>,
}

// CMPTRGM with the historical signed-char ordering (the trigrams in the
// regression data are ASCII, where signed and unsigned agree; high-bit CRC
// bytes take the signed ordering the reference platform produced).
#[inline]
pub fn cmp_trgm(a: &Trgm, b: &Trgm) -> core::cmp::Ordering {
    for i in 0..3 {
        match (a[i] as i8).cmp(&(b[i] as i8)) {
            core::cmp::Ordering::Equal => continue,
            other => return other,
        }
    }
    core::cmp::Ordering::Equal
}

// CALCSML with DIVUNION: count / (len1 + len2 - count), in f32 as the C macro.
#[inline]
fn calc_sml(count: i32, len1: i32, len2: i32) -> f32 {
    (count as f32) / ((len1 + len2 - count) as f32)
}

#[inline]
pub fn trgm2int(t: &Trgm) -> u32 {
    ((t[0] as u32) << 16) | ((t[1] as u32) << 8) | (t[2] as u32)
}

// compact_trigram: three single-byte chars as-is; else CPTRGM of the legacy
// CRC32 (little-endian low three bytes).
pub fn compact_trigram(bytes: &[u8], legacy_crc32: &dyn Fn(&[u8]) -> u32) -> Trgm {
    if bytes.len() == 3 {
        [bytes[0], bytes[1], bytes[2]]
    } else {
        let le = legacy_crc32(bytes).to_le_bytes();
        [le[0], le[1], le[2]]
    }
}

fn make_trigrams(
    dst: &mut Vec<Trgm>,
    buf: &[u8],
    bytelen: usize,
    env: &TrgmEnv<'_>,
    legacy_crc32: &dyn Fn(&[u8]) -> u32,
) {
    if bytelen < 3 {
        return;
    }
    let str = &buf[..bytelen];

    if env.max_encoding_len == 1 {
        for ptr in 0..bytelen - 2 {
            dst.push([str[ptr], str[ptr + 1], str[ptr + 2]]);
        }
        return;
    }

    let is_highbit = |b: u8| b & 0x80 != 0;
    let mut ptr = 0usize;
    let lenfirst;
    let mut lenmiddle;
    let mut lenlast;

    if !is_highbit(str[0]) && !is_highbit(str[1]) {
        loop {
            if is_highbit(str[ptr + 2]) {
                break;
            }
            dst.push([str[ptr], str[ptr + 1], str[ptr + 2]]);
            ptr += 1;
            if ptr == bytelen - 2 {
                return;
            }
        }
        lenfirst = 1;
        lenmiddle = 1;
        lenlast = (env.mblen)(&str[ptr + 2..]) as usize;
    } else {
        lenfirst = (env.mblen)(&str[ptr..]) as usize;
        if ptr + lenfirst >= bytelen {
            return;
        }
        lenmiddle = (env.mblen)(&str[ptr + lenfirst..]) as usize;
        if ptr + lenfirst + lenmiddle >= bytelen {
            return;
        }
        lenlast = (env.mblen)(&str[ptr + lenfirst + lenmiddle..]) as usize;
    }

    let mut endptr = ptr + lenfirst + lenmiddle + lenlast;
    let mut lf = lenfirst;
    while endptr <= bytelen {
        dst.push(compact_trigram(&str[ptr..endptr], legacy_crc32));
        if endptr == bytelen {
            break;
        }
        ptr += lf;
        lf = lenmiddle;
        lenmiddle = lenlast;
        lenlast = (env.mblen)(&str[endptr..]) as usize;
        endptr += lenlast;
    }
}

fn find_word(s: &[u8], start: usize, env: &TrgmEnv<'_>) -> Option<(usize, usize)> {
    let lenstr = s.len();
    let mut begin = start;
    while begin < lenstr {
        let clen = (env.mblen)(&s[begin..]) as usize;
        if (env.isalnum)(&s[begin..]) {
            break;
        }
        begin += clen;
    }
    if begin >= lenstr {
        return None;
    }
    let mut end = begin;
    while end < lenstr {
        let clen = (env.mblen)(&s[end..]) as usize;
        if !(env.isalnum)(&s[end..]) {
            break;
        }
        end += clen;
    }
    Some((begin, end))
}

fn generate_trgm_only(
    s: &[u8],
    want_bounds: bool,
    env: &TrgmEnv<'_>,
    legacy_crc32: &dyn Fn(&[u8]) -> u32,
) -> (Vec<Trgm>, Option<Vec<u8>>) {
    let slen = s.len();
    let mut dst: Vec<Trgm> = Vec::with_capacity(slen + 1);
    let mut bounds: Option<Vec<u8>> = if want_bounds { Some(Vec::new()) } else { None };

    if slen + LPADDING + RPADDING < 3 || slen == 0 {
        return (dst, bounds);
    }

    let mut eword = 0usize;
    while let Some((bword, eword2)) = find_word(s, eword, env) {
        eword = eword2;
        let lowered = (env.tolower)(&s[bword..eword2]);

        let mut buf: Vec<u8> = Vec::with_capacity(lowered.len() + 4);
        buf.extend_from_slice(&[b' '; LPADDING]);
        buf.extend_from_slice(&lowered);
        buf.push(b' ');
        buf.push(b' ');

        let oldlen = dst.len();
        make_trigrams(&mut dst, &buf, lowered.len() + LPADDING + RPADDING, env, legacy_crc32);

        if let Some(b) = bounds.as_mut() {
            while b.len() < dst.len() {
                b.push(0);
            }
            if dst.len() > oldlen {
                b[oldlen] |= TRGM_BOUND_LEFT;
                let last = dst.len() - 1;
                b[last] |= TRGM_BOUND_RIGHT;
            }
        }
    }

    if let Some(b) = bounds.as_mut() {
        while b.len() < dst.len() {
            b.push(0);
        }
    }
    (dst, bounds)
}

fn sort_unique(v: &mut Vec<Trgm>) {
    if v.len() > 1 {
        v.sort_by(cmp_trgm);
        v.dedup_by(|a, b| cmp_trgm(a, b) == core::cmp::Ordering::Equal);
    }
}

pub fn generate_trgm(
    s: &[u8],
    env: &TrgmEnv<'_>,
    legacy_crc32: &dyn Fn(&[u8]) -> u32,
) -> Vec<Trgm> {
    let (mut v, _) = generate_trgm_only(s, false, env, legacy_crc32);
    sort_unique(&mut v);
    v
}

fn is_wildcard_char(b: u8) -> bool {
    b == b'%' || b == b'_'
}

fn is_escape_char(b: u8) -> bool {
    b == b'\\'
}

// get_wildcard_part: the next non-wildcard word of a LIKE pattern, blank-
// padded, escape-stripped, not yet case-folded; plus the resume offset.
fn get_wildcard_part(s: &[u8], start: usize, env: &TrgmEnv<'_>) -> Option<(Vec<u8>, usize)> {
    let endstr = s.len();
    let mut beginword = start;
    let mut in_leading_wildcard_meta = false;
    let mut in_escape = false;

    while beginword < endstr {
        let clen = (env.mblen)(&s[beginword..]) as usize;
        if in_escape {
            if (env.isalnum)(&s[beginword..]) {
                break;
            }
            in_escape = false;
            in_leading_wildcard_meta = false;
        } else if is_escape_char(s[beginword]) {
            in_escape = true;
        } else if is_wildcard_char(s[beginword]) {
            in_leading_wildcard_meta = true;
        } else if (env.isalnum)(&s[beginword..]) {
            break;
        } else {
            in_leading_wildcard_meta = false;
        }
        beginword += clen;
    }

    if beginword >= endstr {
        return None;
    }

    let mut buf: Vec<u8> = Vec::new();
    if !in_leading_wildcard_meta {
        buf.extend_from_slice(&[b' '; LPADDING]);
    }

    let mut endword = beginword;
    let mut in_trailing_wildcard_meta = false;
    while endword < endstr {
        let clen = (env.mblen)(&s[endword..]) as usize;
        if in_escape {
            if (env.isalnum)(&s[endword..]) {
                buf.extend_from_slice(&s[endword..endword + clen]);
            } else {
                // Restart the next call at the escape char (single-byte).
                endword -= 1;
                break;
            }
            in_escape = false;
        } else if is_escape_char(s[endword]) {
            in_escape = true;
        } else if is_wildcard_char(s[endword]) {
            in_trailing_wildcard_meta = true;
            break;
        } else if (env.isalnum)(&s[endword..]) {
            buf.extend_from_slice(&s[endword..endword + clen]);
        } else {
            break;
        }
        endword += clen;
    }

    if !in_trailing_wildcard_meta {
        buf.push(b' ');
    }

    Some((buf, endword))
}

// generate_wildcard_trgm: the trigrams that MUST occur in any string
// matching the LIKE pattern.
pub fn generate_wildcard_trgm(
    s: &[u8],
    env: &TrgmEnv<'_>,
    legacy_crc32: &dyn Fn(&[u8]) -> u32,
) -> Vec<Trgm> {
    let slen = s.len();
    let mut dst: Vec<Trgm> = Vec::with_capacity(slen + 1);

    if slen + LPADDING + RPADDING < 3 || slen == 0 {
        return dst;
    }

    let mut eword = 0usize;
    while let Some((buf, next)) = get_wildcard_part(s, eword, env) {
        eword = next;
        // IGNORECASE: fold the padded word (spaces fold to themselves).
        let word = (env.tolower)(&buf);
        make_trigrams(&mut dst, &word, word.len(), env, legacy_crc32);
        if eword >= slen {
            break;
        }
    }

    sort_unique(&mut dst);
    dst
}

pub fn cnt_sml(trg1: &[Trgm], trg2: &[Trgm], inexact: bool) -> f32 {
    let len1 = trg1.len() as i32;
    let len2 = trg2.len() as i32;
    if len1 <= 0 || len2 <= 0 {
        return 0.0;
    }
    let (mut i, mut j, mut count) = (0usize, 0usize, 0i32);
    while (i as i32) < len1 && (j as i32) < len2 {
        match cmp_trgm(&trg1[i], &trg2[j]) {
            core::cmp::Ordering::Less => i += 1,
            core::cmp::Ordering::Greater => j += 1,
            core::cmp::Ordering::Equal => {
                i += 1;
                j += 1;
                count += 1;
            }
        }
    }
    calc_sml(count, len1, if inexact { count } else { len2 })
}

// trgm_presence_map: which query trigrams occur in the (sorted) key array.
pub fn trgm_presence_map(query: &[Trgm], key: &[Trgm]) -> Vec<bool> {
    query
        .iter()
        .map(|q| key.binary_search_by(|k| cmp_trgm(k, q)).is_ok())
        .collect()
}

// trgm_contained_by over two sorted arrays.
pub fn trgm_contained_by(trg1: &[Trgm], trg2: &[Trgm]) -> bool {
    let (mut i, mut j) = (0usize, 0usize);
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

#[derive(Clone, Copy)]
struct PosTrgm {
    trg: Trgm,
    index: i32,
}

fn comp_ptrgm(a: &PosTrgm, b: &PosTrgm) -> core::cmp::Ordering {
    match cmp_trgm(&a.trg, &b.trg) {
        core::cmp::Ordering::Equal => a.index.cmp(&b.index),
        other => other,
    }
}

#[allow(clippy::too_many_arguments)]
fn iterate_word_similarity(
    trg2indexes: &[i32],
    found: &[bool],
    ulen1: i32,
    len2: usize,
    len: usize,
    flags: u8,
    bounds: Option<&[u8]>,
    word_similarity_threshold: f64,
    strict_word_similarity_threshold: f64,
) -> f32 {
    let strict = flags & WORD_SIMILARITY_STRICT != 0;
    let check_only = flags & WORD_SIMILARITY_CHECK_ONLY != 0;
    let threshold = if strict {
        strict_word_similarity_threshold
    } else {
        word_similarity_threshold
    };

    let mut ulen2: i32 = 0;
    let mut count: i32 = 0;
    let mut smlr_max: f32 = 0.0;
    let mut lower: i32 = if strict { 0 } else { -1 };
    let mut lastpos = vec![-1i32; len];

    for i in 0..len2 {
        let trgindex = trg2indexes[i] as usize;

        if lower >= 0 || found[trgindex] {
            if lastpos[trgindex] < 0 {
                ulen2 += 1;
                if found[trgindex] {
                    count += 1;
                }
            }
            lastpos[trgindex] = i as i32;
        }

        let is_upper = if strict {
            bounds.unwrap()[i] & TRGM_BOUND_RIGHT != 0
        } else {
            found[trgindex]
        };

        if is_upper {
            let upper = i as i32;
            if lower == -1 {
                lower = i as i32;
                ulen2 = 1;
            }

            let mut smlr_cur = calc_sml(count, ulen1, ulen2);

            let mut tmp_count = count;
            let mut tmp_ulen2 = ulen2;
            let prev_lower = lower;
            let mut tmp_lower = lower;
            while tmp_lower <= upper {
                let consider = if strict {
                    bounds.unwrap()[tmp_lower as usize] & TRGM_BOUND_LEFT != 0
                } else {
                    true
                };
                if consider {
                    let smlr_tmp = calc_sml(tmp_count, ulen1, tmp_ulen2);
                    if smlr_tmp > smlr_cur {
                        smlr_cur = smlr_tmp;
                        ulen2 = tmp_ulen2;
                        lower = tmp_lower;
                        count = tmp_count;
                    }
                    if check_only && smlr_cur as f64 >= threshold {
                        break;
                    }
                }

                let tmp_trgindex = trg2indexes[tmp_lower as usize] as usize;
                if lastpos[tmp_trgindex] == tmp_lower {
                    tmp_ulen2 -= 1;
                    if found[tmp_trgindex] {
                        tmp_count -= 1;
                    }
                }
                tmp_lower += 1;
            }

            smlr_max = smlr_max.max(smlr_cur);
            if check_only && smlr_max as f64 >= threshold {
                break;
            }

            let mut tl = prev_lower;
            while tl < lower {
                let tmp_trgindex = trg2indexes[tl as usize] as usize;
                if lastpos[tmp_trgindex] == tl {
                    lastpos[tmp_trgindex] = -1;
                }
                tl += 1;
            }
        }
    }

    smlr_max
}

pub fn calc_word_similarity(
    str1: &[u8],
    str2: &[u8],
    flags: u8,
    env: &TrgmEnv<'_>,
    legacy_crc32: &dyn Fn(&[u8]) -> u32,
    word_similarity_threshold: f64,
    strict_word_similarity_threshold: f64,
) -> f32 {
    let strict = flags & WORD_SIMILARITY_STRICT != 0;

    let (trg1, _) = generate_trgm_only(str1, false, env, legacy_crc32);
    let len1 = trg1.len();
    let (trg2, bounds) = generate_trgm_only(str2, strict, env, legacy_crc32);
    let len2 = trg2.len();

    let len = len1 + len2;
    let mut ptrg: Vec<PosTrgm> = Vec::with_capacity(len);
    for t in &trg1 {
        ptrg.push(PosTrgm { trg: *t, index: -1 });
    }
    for (i, t) in trg2.iter().enumerate() {
        ptrg.push(PosTrgm { trg: *t, index: i as i32 });
    }
    ptrg.sort_by(comp_ptrgm);

    let mut trg2indexes = vec![0i32; len2];
    let mut found = vec![false; len];

    let mut ulen1: i32 = 0;
    let mut j = 0usize;
    for i in 0..len {
        if i > 0 && cmp_trgm(&ptrg[i - 1].trg, &ptrg[i].trg) != core::cmp::Ordering::Equal {
            if found[j] {
                ulen1 += 1;
            }
            j += 1;
        }
        if ptrg[i].index >= 0 {
            trg2indexes[ptrg[i].index as usize] = j as i32;
        } else {
            found[j] = true;
        }
    }
    if len > 0 && found[j] {
        ulen1 += 1;
    }

    iterate_word_similarity(
        &trg2indexes,
        &found,
        ulen1,
        len2,
        len,
        flags,
        bounds.as_deref(),
        word_similarity_threshold,
        strict_word_similarity_threshold,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ascii_env<'a>() -> TrgmEnv<'a> {
        TrgmEnv {
            max_encoding_len: 1,
            mblen: &|_s| 1,
            isalnum: &|s| s.first().map(|c| c.is_ascii_alphanumeric()).unwrap_or(false),
            tolower: &|s| s.iter().map(|c| c.to_ascii_lowercase()).collect(),
        }
    }

    fn crc(_b: &[u8]) -> u32 {
        0
    }

    fn show(v: &[Trgm]) -> Vec<String> {
        v.iter().map(|t| String::from_utf8_lossy(t).into_owned()).collect()
    }

    #[test]
    fn show_trgm_reference_values() {
        let env = ascii_env();
        assert!(generate_trgm(b"", &env, &crc).is_empty());
        assert!(generate_trgm(b"(*&^$@%@", &env, &crc).is_empty());
        assert_eq!(
            show(&generate_trgm(b"a b c", &env, &crc)),
            vec!["  a", "  b", "  c", " a ", " b ", " c "]
        );
        assert_eq!(
            show(&generate_trgm(b"a b C0*%^", &env, &crc)),
            vec!["  a", "  b", "  c", " a ", " b ", " c0", "c0 "]
        );
    }

    #[test]
    fn similarity_reference_values() {
        let env = ascii_env();
        let a = generate_trgm(b"wow", &env, &crc);
        let b = generate_trgm(b"WOWa ", &env, &crc);
        assert!((cnt_sml(&a, &b, false) - 0.5).abs() < 1e-6);
        let b = generate_trgm(b" WOW ", &env, &crc);
        assert!((cnt_sml(&a, &b, false) - 1.0).abs() < 1e-6);
        let a = generate_trgm(b"---", &env, &crc);
        let b = generate_trgm(b"####---", &env, &crc);
        assert_eq!(cnt_sml(&a, &b, false), 0.0);
    }

    #[test]
    fn wildcard_trgm() {
        let env = ascii_env();
        // like_escape('20%') pattern "20%" -> {"  2"," 20"}
        let t = generate_wildcard_trgm(b"20%", &env, &crc);
        assert_eq!(show(&t), vec!["  2", " 20"]);
    }

    #[test]
    fn word_similarity_reference() {
        let env = ascii_env();
        // word_similarity('Sunday', 'Saturday') = 0.2857143 (2/7) in PG
        let s = calc_word_similarity(b"Sunday", b"Saturday", 0, &env, &crc, 0.6, 0.5);
        assert!((s - 2.0 / 7.0).abs() < 1e-6, "got {s}");
    }
}
