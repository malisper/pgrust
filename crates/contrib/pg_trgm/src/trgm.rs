//! The trigram extraction + similarity core of `contrib/pg_trgm/trgm_op.c`,
//! ported 1:1.
//!
//! The C code carries trigrams as a varlena `TRGM` whose payload is an array of
//! `trgm` (`char[3]`). Here a trigram is a `[u8; 3]` and a `TRGM` is a `Vec` of
//! them (sorted + de-duplicated where the C does so). The fmgr-boundary
//! varlena packing/unpacking lives in `lib.rs`; this module is pure logic over
//! byte buffers, so it is unit-testable without a running backend.
//!
//! Multibyte handling, case-folding and the alphanumeric word-character test
//! are delegated to the same backend services the C uses
//! (`pg_mblen`/`str_tolower`/`t_isalnum`), threaded in as closures so this
//! module stays free of those crates' build deps (and so the tests can stub
//! them with the single-byte/identity behavior).

/// A trigram is exactly three bytes (`typedef char trgm[3]`).
pub type Trgm = [u8; 3];

/// Options (`trgm.h`). `trgm_regexp.c` assumes these exact values.
pub const LPADDING: usize = 2;
pub const RPADDING: usize = 1;

/// `TrgmBound` flags (`trgm_op.c`).
pub const TRGM_BOUND_LEFT: u8 = 0x01;
pub const TRGM_BOUND_RIGHT: u8 = 0x02;

/// Word-similarity flags (`trgm_op.c`).
pub const WORD_SIMILARITY_CHECK_ONLY: u8 = 0x01;
pub const WORD_SIMILARITY_STRICT: u8 = 0x02;

/// Services the backend provides to the trigram core. Threaded in so this
/// module does not depend on the mb/formatting/tsearch crates directly (and so
/// the unit tests can substitute single-byte behavior).
pub struct TrgmEnv<'a> {
    /// `pg_encoding_max_length(GetDatabaseEncoding())` — 1 for single-byte
    /// encodings (the C fast path).
    pub max_encoding_len: i32,
    /// `pg_mblen(ptr)` — byte length of the multibyte char at the start of the
    /// slice. The slice is non-empty at every call.
    pub mblen: &'a dyn Fn(&[u8]) -> i32,
    /// `ISWORDCHR(c, len)` = `t_isalnum_with_len(c, len)` — whether the leading
    /// char of the slice is alphanumeric.
    pub isalnum: &'a dyn Fn(&[u8]) -> bool,
    /// `str_tolower(buff, len, DEFAULT_COLLATION_OID)` — case-fold to lower.
    /// IGNORECASE is always defined for pg_trgm.
    pub tolower: &'a dyn Fn(&[u8]) -> Vec<u8>,
}

/// `CMPTRGM` — the per-byte trigram comparison. The C chooses signed/unsigned
/// char comparison from `GetDefaultCharSignedness()`; the default char
/// signedness on the platforms pg_trgm's regression tests target (and the data
/// used) is the historical *signed* char ordering, which `qsort`/`qunique` and
/// the merge in `cnt_sml` all rely on. We use signed-char ordering to match
/// the reference `.out` files (the trigrams in the tests are ASCII, where
/// signed and unsigned agree; for high-bit hash bytes the signed ordering is
/// what the reference platform produced).
#[inline]
pub fn cmp_trgm(a: &Trgm, b: &Trgm) -> core::cmp::Ordering {
    for i in 0..3 {
        let x = a[i] as i8;
        let y = b[i] as i8;
        match x.cmp(&y) {
            core::cmp::Ordering::Equal => continue,
            other => return other,
        }
    }
    core::cmp::Ordering::Equal
}

/// `CALCSML(count, len1, len2)` with `DIVUNION` defined:
/// `count / (len1 + len2 - count)`, computed in f32 as the C macro is.
#[inline]
fn calc_sml(count: i32, len1: i32, len2: i32) -> f32 {
    (count as f32) / ((len1 + len2 - count) as f32)
}

/// `trgm2int(ptr)` — pack the three bytes big-endian into the low 24 bits.
#[inline]
pub fn trgm2int(t: &Trgm) -> u32 {
    let mut v: u32 = t[0] as u32;
    v <<= 8;
    v |= t[1] as u32;
    v <<= 8;
    v |= t[2] as u32;
    v
}

/// `compact_trigram(tptr, str, bytelen)` — reduce three (possibly multibyte)
/// characters spanning `bytes` to a 3-byte trigram. Three single-byte chars are
/// used as-is; otherwise the legacy CRC32 of the bytes is taken and its low
/// three bytes (little-endian `CPTRGM(tptr, &crc)`) become the trigram.
pub fn compact_trigram(bytes: &[u8], legacy_crc32: &dyn Fn(&[u8]) -> u32) -> Trgm {
    if bytes.len() == 3 {
        [bytes[0], bytes[1], bytes[2]]
    } else {
        let crc = legacy_crc32(bytes);
        // CPTRGM(tptr, &crc): copy the first three bytes of the uint32 crc in
        // memory order, i.e. the little-endian low three bytes.
        let le = crc.to_le_bytes();
        [le[0], le[1], le[2]]
    }
}

/// `make_trigrams(dst, str, bytelen)` — append the trigrams of one padded word
/// (`str[..bytelen]`) to `dst`.
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
        // while (ptr < str + bytelen - 2) { CPTRGM(tptr, ptr); ptr++; }
        let end = bytelen - 2;
        let mut ptr = 0usize;
        while ptr < end {
            dst.push([str[ptr], str[ptr + 1], str[ptr + 2]]);
            ptr += 1;
        }
        return;
    }

    // Multibyte path.
    let mut ptr = 0usize;
    let lenfirst;
    let mut lenmiddle;
    let mut lenlast;

    let is_highbit = |b: u8| b & 0x80 != 0;

    // Fast path as long as there are no multibyte characters.
    if !is_highbit(str[0]) && !is_highbit(str[1]) {
        // while (!IS_HIGHBIT_SET(ptr[2])) { CPTRGM; ptr++; if ptr==end goto done }
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

    // Slow path. 'ptr' is the start of the current three-char string, 'endptr'
    // just past it.
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

/// `find_word(str, lenstr, &endword)` — return `(begin, end)` byte offsets of
/// the first word at/after `start`, or `None`. A "word" is a maximal run of
/// alphanumeric characters.
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

/// `generate_trgm_only(dst, str, slen, bounds_p)` — extract all trigrams (with
/// duplicates, unsorted) from `s`, optionally returning the per-trigram bounds.
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

    // buf holds case-folded, blank-padded words. LPADDING leading spaces.
    let mut eword = 0usize;
    while let Some((bword, eword2)) = find_word(s, eword, env) {
        eword = eword2;

        // Convert word to lower case (IGNORECASE always defined).
        let lowered = (env.tolower)(&s[bword..eword2]);

        // buf = LPADDING spaces + lowered + RPADDING(+1) trailing spaces.
        let mut buf: Vec<u8> = Vec::with_capacity(lowered.len() + 4);
        for _ in 0..LPADDING {
            buf.push(b' ');
        }
        buf.extend_from_slice(&lowered);
        // C writes buf[LPADDING+bytelen]=' ' and buf[LPADDING+bytelen+1]=' ';
        // make_trigrams is called with bytelen + LPADDING + RPADDING.
        buf.push(b' ');
        buf.push(b' ');

        let oldlen = dst.len();
        let total = lowered.len() + LPADDING + RPADDING;
        make_trigrams(&mut dst, &buf, total, env, legacy_crc32);

        if let Some(b) = bounds.as_mut() {
            // Grow bounds to dst.len(), then mark this word's first/last.
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

/// Sort + unique a trigram vector in place (`qsort` + `qunique` with
/// `comp_trgm`).
fn sort_unique(v: &mut Vec<Trgm>) {
    if v.len() > 1 {
        v.sort_by(cmp_trgm);
        v.dedup_by(|a, b| cmp_trgm(a, b) == core::cmp::Ordering::Equal);
    }
}

/// `generate_trgm(str, slen)` — the sorted, de-duplicated trigram array.
pub fn generate_trgm(
    s: &[u8],
    env: &TrgmEnv<'_>,
    legacy_crc32: &dyn Fn(&[u8]) -> u32,
) -> Vec<Trgm> {
    let (mut v, _) = generate_trgm_only(s, false, env, legacy_crc32);
    sort_unique(&mut v);
    v
}

/// `ISWILDCARDCHAR(a)` — a LIKE wildcard meta-character (`%` or `_`).
fn is_wildcard_char(b: u8) -> bool {
    b == b'%' || b == b'_'
}

/// `ISESCAPECHAR(a)` — the LIKE escape character (`\`).
fn is_escape_char(b: u8) -> bool {
    b == b'\\'
}

/// `get_wildcard_part(str, lenstr, buf, &bytelen)` (trgm_op.c) — extract the
/// next non-wildcard word from a LIKE pattern, bounded by `%`/`_`
/// meta-characters, non-word characters or string end. Returns the
/// (blank-padded, escape-stripped, not-yet-case-folded) word bytes plus the
/// offset in `s` to resume from, or `None` at string end.
fn get_wildcard_part(s: &[u8], start: usize, env: &TrgmEnv<'_>) -> Option<(Vec<u8>, usize)> {
    let endstr = s.len();
    let mut beginword = start;
    let mut in_leading_wildcard_meta = false;
    let mut in_escape = false;

    // Find the first word character, remembering whether the preceding
    // character was a wildcard meta-character. in_escape persists into the
    // copy loop below.
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
    // Left padding spaces if the preceding char wasn't a wildcard meta-char.
    if !in_leading_wildcard_meta {
        for _ in 0..LPADDING {
            buf.push(b' ');
        }
    }

    // Copy data into buf until wildcard meta-char, non-word char or string end.
    let mut endword = beginword;
    let mut in_trailing_wildcard_meta = false;
    while endword < endstr {
        let clen = (env.mblen)(&s[endword..]) as usize;
        if in_escape {
            if (env.isalnum)(&s[endword..]) {
                buf.extend_from_slice(&s[endword..endword + clen]);
            } else {
                // Back up endword to the escape char (single-byte) so the next
                // call restarts there.
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

    // Right padding spaces if the next char isn't a wildcard meta-char.
    if !in_trailing_wildcard_meta {
        for _ in 0..RPADDING {
            buf.push(b' ');
        }
    }

    Some((buf, endword))
}

/// `generate_wildcard_trgm(str, slen)` (trgm_op.c) — the sorted, de-duplicated
/// trigrams that MUST occur in any string matching the LIKE pattern. For
/// pattern `a%bcd%` this yields `" a"`, `"bcd"`-derived trigrams.
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
        if eword <= 0 && buf.is_empty() {
            break;
        }
        // IGNORECASE: case-fold the padded word before extracting trigrams.
        // The padding spaces fold to themselves.
        let word = (env.tolower)(&buf);
        let bytelen = word.len();
        make_trigrams(&mut dst, &word, bytelen, env, legacy_crc32);
        // Guard against a non-advancing get_wildcard_part on a degenerate
        // pattern (it always advances past at least one char in practice).
        if eword >= slen {
            break;
        }
    }

    sort_unique(&mut dst);
    dst
}

/// `cnt_sml(trg1, trg2, inexact)` — similarity of two sorted trigram arrays.
pub fn cnt_sml(trg1: &[Trgm], trg2: &[Trgm], inexact: bool) -> f32 {
    let len1 = trg1.len() as i32;
    let len2 = trg2.len() as i32;
    if len1 <= 0 || len2 <= 0 {
        return 0.0;
    }
    let mut i = 0usize;
    let mut j = 0usize;
    let mut count = 0i32;
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

/// A positional trigram (`pos_trgm`): the trigram and its index (`-1` for a
/// pattern trigram whose position doesn't matter).
#[derive(Clone, Copy)]
struct PosTrgm {
    trg: Trgm,
    index: i32,
}

/// `comp_ptrgm` — order by trigram then by index.
fn comp_ptrgm(a: &PosTrgm, b: &PosTrgm) -> core::cmp::Ordering {
    match cmp_trgm(&a.trg, &b.trg) {
        core::cmp::Ordering::Equal => a.index.cmp(&b.index),
        other => other,
    }
}

/// `iterate_word_similarity(...)` — the sliding-window maximum word similarity.
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

        // Update last position of this trigram.
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
                    if check_only && smlr_cur >= threshold as f32 {
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

            if check_only && smlr_max >= threshold as f32 {
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

/// `calc_word_similarity(str1, str2, flags)`.
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

    // make_positional_trgm: trg1 entries (index -1) then trg2 entries (index i).
    let len = len1 + len2;
    let mut ptrg: Vec<PosTrgm> = Vec::with_capacity(len);
    for t in &trg1 {
        ptrg.push(PosTrgm { trg: *t, index: -1 });
    }
    for (i, t) in trg2.iter().enumerate() {
        ptrg.push(PosTrgm { trg: *t, index: i as i32 });
    }
    ptrg.sort_by(comp_ptrgm);

    // Merge positional trigrams.
    let mut trg2indexes = vec![0i32; len2];
    let mut found = vec![false; len];

    let mut ulen1: i32 = 0;
    let mut j = 0usize;
    for i in 0..len {
        if i > 0 {
            let cmp = cmp_trgm(&ptrg[i - 1].trg, &ptrg[i].trg);
            if cmp != core::cmp::Ordering::Equal {
                if found[j] {
                    ulen1 += 1;
                }
                j += 1;
            }
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

    // Single-byte test environment (ASCII): mblen=1, isalnum=C alnum,
    // tolower=ASCII lower, encoding length 1.
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
        v.iter()
            .map(|t| String::from_utf8_lossy(t).into_owned())
            .collect()
    }

    #[test]
    fn show_trgm_empty() {
        let env = ascii_env();
        assert!(generate_trgm(b"", &env, &crc).is_empty());
    }

    #[test]
    fn show_trgm_abc_words() {
        // select show_trgm('a b c') => {"  a","  b","  c"," a "," b "," c "}
        let env = ascii_env();
        let t = generate_trgm(b"a b c", &env, &crc);
        assert_eq!(
            show(&t),
            vec!["  a", "  b", "  c", " a ", " b ", " c "]
        );
    }

    #[test]
    fn show_trgm_spaced() {
        // select show_trgm(' a b c ') => same as 'a b c'
        let env = ascii_env();
        let t = generate_trgm(b" a b c ", &env, &crc);
        assert_eq!(show(&t), vec!["  a", "  b", "  c", " a ", " b ", " c "]);
    }

    #[test]
    fn show_trgm_punct_only() {
        // select show_trgm('(*&^$@%@') => {} (no word chars)
        let env = ascii_env();
        assert!(generate_trgm(b"(*&^$@%@", &env, &crc).is_empty());
    }

    #[test]
    fn show_trgm_mixed() {
        // select show_trgm('a b C0*%^') =>
        //   {"  a","  b","  c"," a "," b "," c0","c0 "}
        // ("C0" folds to a single word "c0"; the digit shares the word.)
        let env = ascii_env();
        let t = generate_trgm(b"a b C0*%^", &env, &crc);
        assert_eq!(
            show(&t),
            vec!["  a", "  b", "  c", " a ", " b ", " c0", "c0 "]
        );
    }

    #[test]
    fn similarity_dashes() {
        // select similarity('---', '####---') ; '---' has no word chars => 0
        let env = ascii_env();
        let a = generate_trgm(b"---", &env, &crc);
        let b = generate_trgm(b"####---", &env, &crc);
        assert_eq!(cnt_sml(&a, &b, false), 0.0);
    }

    #[test]
    fn similarity_wow() {
        // select similarity('wow','WOWa ') => 0.5 in PG
        let env = ascii_env();
        let a = generate_trgm(b"wow", &env, &crc);
        let b = generate_trgm(b"WOWa ", &env, &crc);
        let s = cnt_sml(&a, &b, false);
        assert!((s - 0.5).abs() < 1e-6, "got {s}");
    }

    #[test]
    fn similarity_wow2() {
        // select similarity('wow',' WOW ') => 1 in PG
        let env = ascii_env();
        let a = generate_trgm(b"wow", &env, &crc);
        let b = generate_trgm(b" WOW ", &env, &crc);
        let s = cnt_sml(&a, &b, false);
        assert!((s - 1.0).abs() < 1e-6, "got {s}");
    }
}
