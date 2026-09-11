//! [R6d] LIKE / NOT LIKE entry-predicate evaluator — a byte-exact port of
//! C postgres's `like_match.c` `MatchText` compiled as the UTF8 stamp
//! (`UTF8_MatchText`: `_` consumes exactly ONE CHARACTER via the fast
//! UTF-8 NextChar, GETCHAR is identity, escape is `\`). This is the first
//! proving predicate class of the R6d dict-predicate survivor law
//! (election-breadth-audit.md §2): the matcher is a pure function of
//! entry bytes, so a LIKE conjunct over a dict-published column is
//! entry-decidable by construction.
//!
//! Grain law: C postgres picks the matcher stamp by DATABASE ENCODING,
//! not collation (like.c `GenericMatchText`) — under C collation in a
//! UTF-8 database the `_` wildcard is still character-grain
//! (`UTF8_MatchText`). The bank's server-encoding law is UTF-8, so this
//! is the ONE stamp ported; the seam refuses unescaped-`_` patterns in
//! any non-UTF-8 database (SB byte-grain and non-UTF8-MB stamps are not
//! ported — and the row engine's non-UTF8-MB arm is itself a loud
//! feature gap, adt/like `mb_matchtext_unported`).
//!
//! Vocabulary bounds (typed refusals upstream, never here):
//!   - C collation only (`planner::check_collation`); no ILIKE (the
//!     MATCH_LOWER arm is not ported), no nondeterministic locales.
//!   - escape is the standard `\` (the `like_escape` rewrite to a custom
//!     ESCAPE character is seam work; a pattern arriving here is already
//!     in backslash convention).
//!   - a pattern ending with a bare escape is INVALID (postgres raises
//!     ERRCODE_INVALID_ESCAPE_SEQUENCE mid-match; we refuse at plan time
//!     via `like_valid` so eval never errors).

/// like_match.c's tri-state: LIKE_FALSE keeps scanning outer `%` starts;
/// LIKE_ABORT proves no later start can match either.
const LIKE_TRUE: i8 = 1;
const LIKE_FALSE: i8 = 0;
const LIKE_ABORT: i8 = -1;

const ESCAPE: u8 = b'\\';

/// Plan-time pattern validation: postgres's only pattern ERROR is
/// "LIKE pattern must not end with escape character" — everything else
/// is a matchable pattern. Returns the error cause for the typed refusal.
pub fn like_valid(pattern: &[u8]) -> Result<(), &'static str> {
    let mut i = 0usize;
    while i < pattern.len() {
        if pattern[i] == ESCAPE {
            if i + 1 >= pattern.len() {
                return Err("like-pattern-trailing-escape");
            }
            i += 2;
        } else {
            i += 1;
        }
    }
    Ok(())
}

/// The normalization classes a LIKE pattern lowers to (fingerprint-
/// stability law: the `%x%` class lowers to the EXISTING Contains /
/// NotContains ops so it shares their condcache identity; §3.2 widening
/// adds fingerprints, never changes existing ones).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LikeClass {
    /// `%inner%` with a nonempty, wildcard-free, escape-free inner:
    /// byte-contains (the shipped infix hot-shape class).
    Contains(Vec<u8>),
    /// Everything else: the general matcher.
    General,
}

/// Classify a (valid) pattern for lowering.
pub fn classify(pattern: &[u8]) -> LikeClass {
    if pattern.len() >= 2 && pattern[0] == b'%' && pattern[pattern.len() - 1] == b'%' {
        let inner = &pattern[1..pattern.len() - 1];
        if !inner.is_empty()
            && !inner.iter().any(|&b| b == b'%' || b == b'_' || b == ESCAPE)
        {
            return LikeClass::Contains(inner.to_vec());
        }
    }
    LikeClass::General
}

/// `text LIKE pattern` under C collation in a UTF-8 database. The caller
/// guarantees `like_valid(pattern)` — a trailing bare escape here matches
/// nothing (defensive; postgres would have errored at that point).
#[inline]
pub fn like_match(text: &[u8], pattern: &[u8]) -> bool {
    match_text(text, pattern) == LIKE_TRUE
}

/// C's UTF8 NextChar: `do { p++; plen--; } while (plen > 0 &&
/// (*p & 0xC0) == 0x80)` — advance one byte, then swallow continuation
/// bytes. Malformed-UTF-8 law comes straight from this shape (C never
/// validates here): a bare lead byte with no continuations is one
/// "character"; a stray continuation byte at the scan position starts a
/// "character" that also swallows any continuation bytes after it; a
/// truncated sequence at end-of-text is one character.
#[inline]
fn next_char(mut t: &[u8]) -> &[u8] {
    loop {
        t = &t[1..];
        if t.is_empty() || (t[0] & 0xC0) != 0x80 {
            return t;
        }
    }
}

/// The MatchText port — the `UTF8_MatchText` stamp. Byte-for-byte control
/// flow of like_match.c with GETCHAR = identity and NextChar = the UTF-8
/// fast NextChar. Wildcards (`_`, and `%`'s restart scan) advance the
/// TEXT by whole characters so recursion re-enters char-synced; all
/// literal comparisons and lockstep advances stay byte-wise (like_match.c:
/// no backend-legal encoding lets ASCII bytes like `%` appear as
/// non-first bytes, and mid-character positions are only reachable after
/// at least one byte of the character already matched on both sides).
fn match_text(mut t: &[u8], mut p: &[u8]) -> i8 {
    // Fast path for the match-everything pattern.
    if p.len() == 1 && p[0] == b'%' {
        return LIKE_TRUE;
    }
    // like_match.c:89 check_stack_depth(): one recursion per '%' group; the
    // ERROR unwinds through the kernel to the statement boundary.
    ::stack_depth_core::check_stack_depth_or_panic();
    while !t.is_empty() && !p.is_empty() {
        if p[0] == b'%' {
            // Collapse the wildcard run: N `_`s and one-or-more `%`s ==
            // N `_`s and one `%`.
            p = &p[1..];
            while !p.is_empty() {
                if p[0] == b'%' {
                    p = &p[1..];
                } else if p[0] == b'_' {
                    if t.is_empty() {
                        return LIKE_ABORT;
                    }
                    t = next_char(t);
                    p = &p[1..];
                } else {
                    break; // a non-wildcard pattern byte
                }
            }
            // Trailing % matches any remaining text.
            if p.is_empty() {
                return LIKE_TRUE;
            }
            // Scan for a text position matching the rest; the first
            // remaining pattern byte is a (possibly escaped) literal.
            let firstpat = if p[0] == ESCAPE {
                if p.len() < 2 {
                    return LIKE_FALSE; // invalid trailing escape (refused at plan time)
                }
                p[1]
            } else {
                p[0]
            };
            while !t.is_empty() {
                if t[0] == firstpat {
                    let m = match_text(t, p);
                    if m != LIKE_FALSE {
                        return m; // TRUE or ABORT
                    }
                }
                t = next_char(t);
            }
            // End of text with no match: later outer starts are hopeless.
            return LIKE_ABORT;
        } else if p[0] == b'_' {
            // _ matches any single CHARACTER, and we know there is one.
            t = next_char(t);
            p = &p[1..];
            continue;
        } else if p[0] == ESCAPE {
            // Next byte must be taken literally.
            if p.len() < 2 {
                return LIKE_FALSE; // invalid trailing escape (refused at plan time)
            }
            p = &p[1..];
            if p[0] != t[0] {
                return LIKE_FALSE;
            }
        } else if p[0] != t[0] {
            return LIKE_FALSE;
        }
        t = &t[1..];
        p = &p[1..];
    }
    if !t.is_empty() {
        return LIKE_FALSE; // end of pattern, but not of text
    }
    // End of text: match iff the remaining pattern is zero or more %'s.
    while !p.is_empty() && p[0] == b'%' {
        p = &p[1..];
    }
    if p.is_empty() {
        return LIKE_TRUE;
    }
    LIKE_ABORT
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lm(t: &str, p: &str) -> bool {
        like_match(t.as_bytes(), p.as_bytes())
    }

    /// Independent CHARACTER-grain reference for valid UTF-8: plain
    /// recursion over Rust `char` slices (rustc's own decoder — nothing
    /// shared with the byte matcher). `%` tries every char-suffix, `_`
    /// consumes one char, escape takes the next pattern char literally.
    fn naive_chars(t: &[char], p: &[char]) -> bool {
        if p.is_empty() {
            return t.is_empty();
        }
        match p[0] {
            '%' => (0..=t.len()).any(|k| naive_chars(&t[k..], &p[1..])),
            '_' => !t.is_empty() && naive_chars(&t[1..], &p[1..]),
            '\\' => {
                if p.len() < 2 {
                    return false; // invalid pattern (plan-time refusal)
                }
                !t.is_empty() && t[0] == p[1] && naive_chars(&t[1..], &p[2..])
            }
            c => !t.is_empty() && t[0] == c && naive_chars(&t[1..], &p[1..]),
        }
    }

    fn ref_chars(t: &str, p: &str) -> bool {
        let tc: Vec<char> = t.chars().collect();
        let pc: Vec<char> = p.chars().collect();
        naive_chars(&tc, &pc)
    }

    /// Independent naive BYTE-grain reference (no % skip optimization, no
    /// ABORT pruning): plain recursion over the same escape convention.
    /// Still the truth for all-ASCII domains, where byte == char.
    fn naive(t: &[u8], p: &[u8]) -> bool {
        if p.is_empty() {
            return t.is_empty();
        }
        match p[0] {
            b'%' => {
                // % matches any 0..len suffix start.
                (0..=t.len()).any(|k| naive(&t[k..], &p[1..]))
            }
            b'_' => !t.is_empty() && naive(&t[1..], &p[1..]),
            ESCAPE => {
                if p.len() < 2 {
                    return false; // invalid pattern (plan-time refusal)
                }
                !t.is_empty() && t[0] == p[1] && naive(&t[1..], &p[2..])
            }
            c => !t.is_empty() && t[0] == c && naive(&t[1..], &p[1..]),
        }
    }

    #[test]
    fn pg_edge_cases() {
        // empty pattern matches only empty text
        assert!(lm("", ""));
        assert!(!lm("a", ""));
        // match-everything fast path (including empty text)
        assert!(lm("", "%"));
        assert!(lm("abc", "%"));
        assert!(lm("", "%%%"));
        // exact literal
        assert!(lm("abc", "abc"));
        assert!(!lm("abc", "abd"));
        assert!(!lm("abc", "ab"));
        assert!(!lm("ab", "abc"));
        // _ is exactly one byte
        assert!(lm("abc", "a_c"));
        assert!(!lm("ac", "a_c"));
        assert!(!lm("abbc", "a_c"));
        assert!(lm("a", "_"));
        assert!(!lm("", "_"));
        // trailing %
        assert!(lm("abcdef", "abc%"));
        assert!(lm("abc", "abc%"));
        // leading %
        assert!(lm("xxabc", "%abc"));
        assert!(!lm("xxabcx", "%abc"));
        // infix
        assert!(lm("xxgoogleyy", "%google%"));
        assert!(!lm("xxgoogl", "%google%"));
        assert!(lm("google", "%google%"));
        // wildcard-run collapse: %_ needs at least one byte
        assert!(!lm("", "%_"));
        assert!(lm("a", "%_"));
        assert!(lm("abc", "%_"));
        assert!(lm("ab", "_%_"));
        assert!(!lm("a", "_%_"));
        // % between literals
        assert!(lm("a123b", "a%b"));
        assert!(lm("ab", "a%b"));
        assert!(!lm("a123c", "a%b"));
        // multiple %
        assert!(lm("a1b2c", "a%b%c"));
        assert!(!lm("a1c2b", "a%b%c"));
        // escapes: literal %, _, and backslash
        assert!(lm("100%", "100\\%"));
        assert!(!lm("100x", "100\\%"));
        assert!(lm("a_b", "a\\_b"));
        assert!(!lm("axb", "a\\_b"));
        assert!(lm("a\\b", "a\\\\b"));
        assert!(!lm("ab", "a\\\\b"));
        // escaped literal after %
        assert!(lm("xx50%yy", "%50\\%%"));
        assert!(!lm("xx50xyy", "%50\\%%"));
        // escaped ordinary char behaves as the char
        assert!(lm("abc", "a\\bc"));
        // empty needle infix: '%%' == '%'
        assert!(lm("", "%%"));
        assert!(lm("anything", "%%"));
    }

    /// Character-grain `_` over multibyte UTF-8 — the witnessed PG 18
    /// truths that the byte-grain lane inverted, plus like_match.c MB
    /// boundary shapes. Each expectation hand-checked against the C
    /// UTF8_MatchText law (and the witnessed rows against live PG 18).
    #[test]
    fn mb_underscore_char_grain() {
        // The seam-report witnesses: byte-grain inverts BOTH of these.
        assert!(lm("é", "_")); // E'é' LIKE '_' = true in PG 18/UTF-8
        assert!(lm("日本", "__")); // '日本' LIKE '__' = true
        assert!(!lm("日本", "_"));
        assert!(!lm("é", "__"));
        // _ at pattern end, after multibyte literals.
        assert!(lm("日本語", "日本_"));
        assert!(lm("abcé", "abc_"));
        assert!(!lm("abcé", "abc__"));
        // _ runs across mixed widths (1/2/3-byte chars).
        assert!(lm("aé日", "___"));
        assert!(lm("aé日", "a_日"));
        assert!(lm("aé日", "_é_"));
        assert!(!lm("aé日", "__"));
        // multibyte literals at pattern boundaries.
        assert!(lm("éx", "é_"));
        assert!(lm("xé", "_é"));
        assert!(lm("éé", "é%"));
        assert!(!lm("é", "日"));
        // wildcard-run collapse (%_ needs one CHARACTER).
        assert!(lm("日", "%_"));
        assert!(lm("日本", "_%_"));
        assert!(!lm("日", "_%_"));
        assert!(lm("日本語です", "%__"));
        // mixed %_ with literals; % restart scan advances by char.
        assert!(lm("x日y", "x_y"));
        assert!(lm("日本0abc", "__0%"));
        assert!(lm("ドメイン名", "%イ__"));
        assert!(!lm("ドメイン名", "%イ___"));
        // escaped _ stays a literal underscore, char-grain untouched.
        assert!(lm("a_b", "a\\_b"));
        assert!(!lm("aéb", "a\\_b"));
        assert!(lm("é_é", "_\\__"));
        // infix % with multibyte needle bytes (byte-wise scan is sound:
        // UTF-8 self-synchronization — a literal can't match mid-char).
        assert!(lm("xx日本yy", "%日本%"));
        assert!(!lm("xx日体yy", "%日本%")); // 本/体 share no full byte seq
    }

    /// Malformed-UTF-8 DATA under the C UTF8 NextChar law: `do { p++;
    /// plen--; } while (plen > 0 && (*p & 0xC0) == 0x80)`. C never
    /// validates or errors here — a stray/truncated sequence is simply
    /// consumed by the loop shape. Pins ported straight from that law.
    #[test]
    fn mb_malformed_utf8_matches_c_law() {
        fn lmb(t: &[u8], p: &[u8]) -> bool {
            like_match(t, p)
        }
        // stray continuation byte after an ASCII char: swallowed into the
        // PRECEDING char's step (NextChar always skips continuations).
        assert!(lmb(b"a\x80", b"_"));
        assert!(!lmb(b"a\x80", b"__"));
        // leading continuation byte swallows FOLLOWING continuations.
        assert!(lmb(b"\x80", b"_"));
        assert!(lmb(b"\x80\x80", b"_")); // one "char" per the loop shape
        assert!(!lmb(b"\x80\x80", b"__"));
        // bare lead byte followed by ASCII: lead alone is one "char".
        assert!(lmb(b"\xE9A", b"__"));
        assert!(!lmb(b"\xE9A", b"_"));
        // truncated sequence at end of text: one "char".
        assert!(lmb(b"\xC3", b"_"));
        // valid char + stray continuation: swallowed into ONE "char".
        assert!(lmb(b"\xC3\xA9\xA9", b"_"));
        // literal matching stays byte-wise regardless of validity.
        assert!(lmb(b"\xC3\xA9", b"\xC3\xA9"));
        assert!(!lmb(b"\xC3\xA9", b"\xC3\xA8"));
        // % restart scan over malformed data still terminates/matches.
        assert!(lmb(b"a\x80b", b"%b"));
        assert!(lmb(b"a\x80b", b"a%"));
        assert!(!lmb(b"a\x80", b"%b"));
    }

    #[test]
    fn trailing_escape_is_invalid() {
        assert_eq!(like_valid(b"abc\\"), Err("like-pattern-trailing-escape"));
        assert_eq!(like_valid(b"%\\"), Err("like-pattern-trailing-escape"));
        assert!(like_valid(b"a\\\\").is_ok()); // escaped backslash is fine
        assert!(like_valid(b"a\\%b\\_c").is_ok());
        assert!(like_valid(b"").is_ok());
    }

    #[test]
    fn classify_contains_normalization() {
        assert_eq!(classify(b"%google%"), LikeClass::Contains(b"google".to_vec()));
        assert_eq!(classify(b"%a b%"), LikeClass::Contains(b"a b".to_vec()));
        // interior wildcard / escape / empty inner => general
        assert_eq!(classify(b"%a%b%"), LikeClass::General);
        assert_eq!(classify(b"%a_b%"), LikeClass::General);
        assert_eq!(classify(b"%a\\%b%"), LikeClass::General);
        assert_eq!(classify(b"%%"), LikeClass::General);
        assert_eq!(classify(b"abc"), LikeClass::General);
        assert_eq!(classify(b"abc%"), LikeClass::General);
        assert_eq!(classify(b"%abc"), LikeClass::General);
        assert_eq!(classify(b"%"), LikeClass::General);
    }

    /// Exhaustive small-domain equivalence vs the naive reference: every
    /// VALID pattern of length <= 4 over {a, b, %, _, \} against every
    /// text of length <= 4 over {a, b, \}.
    #[test]
    fn exhaustive_vs_reference() {
        let pat_alpha = [b'a', b'b', b'%', b'_', ESCAPE];
        let txt_alpha = [b'a', b'b', ESCAPE];
        let mut pats: Vec<Vec<u8>> = vec![Vec::new()];
        for _ in 0..4 {
            let mut next = Vec::new();
            for p in &pats {
                for &c in &pat_alpha {
                    let mut q = p.clone();
                    q.push(c);
                    next.push(q);
                }
            }
            pats.extend(next.clone());
            pats = {
                let mut all: Vec<Vec<u8>> = vec![Vec::new()];
                all.extend(pats.into_iter().filter(|p| !p.is_empty()));
                all
            };
        }
        pats.sort();
        pats.dedup();
        let mut txts: Vec<Vec<u8>> = vec![Vec::new()];
        let mut layer: Vec<Vec<u8>> = vec![Vec::new()];
        for _ in 0..4 {
            let mut next = Vec::new();
            for t in &layer {
                for &c in &txt_alpha {
                    let mut u = t.clone();
                    u.push(c);
                    next.push(u);
                }
            }
            txts.extend(next.clone());
            layer = next;
        }
        let mut checked = 0u64;
        for p in &pats {
            if like_valid(p).is_err() {
                continue;
            }
            for t in &txts {
                assert_eq!(
                    like_match(t, p),
                    naive(t, p),
                    "text={:?} pattern={:?}",
                    String::from_utf8_lossy(t),
                    String::from_utf8_lossy(p)
                );
                checked += 1;
            }
        }
        assert!(checked > 50_000, "exhaustive sweep too small: {checked}");
    }

    /// Exhaustive multibyte equivalence vs the independent char-grain
    /// reference (rustc's decoder, plain recursion): every VALID pattern
    /// of length <= 3 chars over {a, é, 日, %, _, \} against every text
    /// of length <= 3 chars over {a, b, é, 日} (1-, 2-, and 3-byte
    /// characters at every boundary position).
    #[test]
    fn exhaustive_mb_vs_char_reference() {
        let pat_alpha = ['a', 'é', '日', '%', '_', '\\'];
        let txt_alpha = ['a', 'b', 'é', '日'];
        fn grow(alpha: &[char], max: usize) -> Vec<String> {
            let mut all: Vec<String> = vec![String::new()];
            let mut layer: Vec<String> = vec![String::new()];
            for _ in 0..max {
                let mut next = Vec::new();
                for s in &layer {
                    for &c in alpha {
                        let mut u = s.clone();
                        u.push(c);
                        next.push(u);
                    }
                }
                all.extend(next.iter().cloned());
                layer = next;
            }
            all
        }
        let pats = grow(&pat_alpha, 3);
        let txts = grow(&txt_alpha, 3);
        let mut checked = 0u64;
        for p in &pats {
            if like_valid(p.as_bytes()).is_err() {
                continue;
            }
            let pc: Vec<char> = p.chars().collect();
            for t in &txts {
                let tc: Vec<char> = t.chars().collect();
                assert_eq!(
                    like_match(t.as_bytes(), p.as_bytes()),
                    naive_chars(&tc, &pc),
                    "text={t:?} pattern={p:?}"
                );
                checked += 1;
            }
        }
        assert!(checked > 15_000, "mb sweep too small: {checked}");
    }

    /// The `ref_chars` helper is itself sanity-pinned on the PG-witnessed
    /// truths (so the sweep's reference can't drift silently).
    #[test]
    fn char_reference_matches_pg_witnesses() {
        assert!(ref_chars("é", "_"));
        assert!(ref_chars("日本", "__"));
        assert!(!ref_chars("日本", "_"));
        assert!(ref_chars("a_b", "a\\_b"));
        assert!(!ref_chars("aéb", "a\\_b"));
    }
}
