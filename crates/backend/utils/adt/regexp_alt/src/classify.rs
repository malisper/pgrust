//! FAIL-CLOSED compile-time compatibility classifier for the auto dispatch:
//! a pattern is admitted to RE2 only when every construct in it is on the
//! proven-equivalent whitelist (RE2 in POSIX longest-match mode vs the
//! Spencer ARE port). Anything unrecognized, ambiguous, or known-divergent
//! (docs/design/regex-engine-ab-verdict.md) classifies as Spencer.
//!
//! Rejected by construction (the documented delta list):
//! - backreferences and lookaround (`\1`..`\9`, `(?=`, `(?!`, `(?<`);
//! - ctype/collation-sensitive classes and escapes (`\w \s \d \b \m \M \y
//!   \Y \W \S \D \B \Z \A`, `[[:alpha:]]`, `[[=x=]]`, `[[.x.]]`);
//! - non-greedy quantifiers (Spencer preference rules vs leftmost-first);
//! - REG_ICASE (Unicode simple folding vs collation-driven pg_wc_tolower),
//!   REG_EXPANDED, REG_NLSTOP/REG_NLANCH newline modes;
//! - non-ARE modes other than 'q' (REG_QUOTE);
//! - escapes inside bracket expressions, POSIX named classes, collating
//!   elements, equivalence classes, non-ASCII range endpoints;
//! - repeat bounds above 255 (Spencer's DUPMAX) or malformed bounds;
//! - inline option/director groups (`(?i)`, `***:`);
//! - non-UTF8 databases and patterns that are not valid UTF-8 or contain
//!   NUL (Spencer's pattern view stops at the first NUL);
//! Additionally, capture POSITIONS are only trusted ("capture-safe") when
//! (a) no quantifier applies to a subtree containing a capturing group —
//! RE2's longest-match submatch resolution and Spencer's iteration rules
//! disagree on the last-iteration capture of shapes like `(x?|...)+` — and
//! (b) the pattern has no alternation anywhere when it has captures:
//! overlapping branches make branch selection (and thus captures) diverge
//! even under identical whole-match spans, e.g. `(é|.[^a])` (both found by
//! the adversarial corpus). Non-capture-safe patterns still dispatch for
//! whole-match-only uses — leftmost-longest whole-match spans are uniquely
//! defined and agree — but every submatch-consuming call falls to Spencer.
//!
//! - patterns beyond the complexity budget (MAX_PATTERN_BYTES /
//!   MAX_QUANTIFIERS): Spencer enforces NFA state/arc limits ("regular
//!   expression is too complex") that RE2 does not share, so a large
//!   admitted pattern could succeed under RE2 where Spencer errors (caught
//!   live by the regex regress suite's repeat('x*y*z*', 1000) case). The
//!   budget keeps admitted patterns far below Spencer's limits.

use ::regex_spencer::{REG_ADVANCED, REG_NOSUB, REG_QUOTE};

const PG_UTF8: i32 = wchar::PG_UTF8;

const MAX_PATTERN_BYTES: usize = 256;
const MAX_QUANTIFIERS: u32 = 32;
const MAX_GROUP_DEPTH: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compat {
    Incompatible,
    // Whole-match spans provably agree; capture positions do not.
    WholeMatch,
    // Capture positions provably agree too.
    CaptureSafe,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Classification {
    pub tier: Compat,
    // POSIX longest-match mode is only NEEDED to disambiguate alternation
    // (`a|ab`); with `|` absent every choice point is a greedy quantifier
    // and Perl first-match backtracking order IS the leftmost-longest
    // disambiguation, so the faster first-match engine configuration is
    // provably identical (and keeps RE2's one-pass capture paths).
    pub needs_longest: bool,
}

const INCOMPATIBLE: Classification =
    Classification { tier: Compat::Incompatible, needs_longest: false };

pub fn classify(pattern: &[u8], cflags: i32) -> Classification {
    if pattern.len() > MAX_PATTERN_BYTES {
        return INCOMPATIBLE;
    }
    if mbutils::GetDatabaseEncoding() != PG_UTF8 {
        return INCOMPATIBLE;
    }
    // REG_NOSUB is an execution hint callers OR in, not a semantic mode.
    let base = cflags & !REG_NOSUB;
    let quoted = base == REG_QUOTE;
    if !quoted && base != REG_ADVANCED {
        return INCOMPATIBLE;
    }
    // NUL is valid UTF-8, but Spencer compiles the pattern through
    // pg_mb2wchar (stops at the first NUL) while RE2 would compile the full
    // byte string.
    if core::str::from_utf8(pattern).is_err() || pattern.contains(&0) {
        return INCOMPATIBLE;
    }
    if quoted {
        return Classification { tier: Compat::CaptureSafe, needs_longest: false };
    }
    scan_are(pattern)
}

pub fn re2_compatible(pattern: &[u8], cflags: i32) -> bool {
    classify(pattern, cflags).tier != Compat::Incompatible
}

fn utf8_char_len(b: u8) -> usize {
    match b {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        _ => 4,
    }
}

// Escapes equivalent as pure literals in both engines: the C-style control
// escapes plus escaped ASCII punctuation (Spencer: any escaped non-word
// character is that literal; RE2 agrees for these, and where RE2 instead
// errors the auto path falls back to Spencer at compile).
fn literal_escape_ok(c: u8) -> bool {
    matches!(c, b'n' | b't' | b'r' | b'f' | b'v' | b'a')
        || (c.is_ascii() && !c.is_ascii_alphanumeric())
}

// Parses {m}, {m,}, {m,n} with m <= n <= 255; returns the index just past
// '}' or None when the brace is not a well-formed bound (Spencer and RE2
// disagree on literal-brace fallback, so malformed means incompatible).
fn parse_bound(pat: &[u8], mut i: usize) -> Option<usize> {
    debug_assert_eq!(pat[i], b'{');
    i += 1;
    let mut m: u32 = 0;
    let m_start = i;
    while i < pat.len() && pat[i].is_ascii_digit() {
        m = m * 10 + (pat[i] - b'0') as u32;
        if m > 255 {
            return None;
        }
        i += 1;
    }
    if i == m_start {
        return None;
    }
    if i < pat.len() && pat[i] == b'}' {
        return Some(i + 1);
    }
    if i >= pat.len() || pat[i] != b',' {
        return None;
    }
    i += 1;
    if i < pat.len() && pat[i] == b'}' {
        return Some(i + 1);
    }
    let mut n: u32 = 0;
    let n_start = i;
    while i < pat.len() && pat[i].is_ascii_digit() {
        n = n * 10 + (pat[i] - b'0') as u32;
        if n > 255 {
            return None;
        }
        i += 1;
    }
    if i == n_start || n < m || i >= pat.len() || pat[i] != b'}' {
        return None;
    }
    Some(i + 1)
}

// Returns the index just past the closing ']' when the bracket expression is
// on the whitelist: plain members and ASCII-endpoint ranges only.
fn parse_bracket(pat: &[u8], mut i: usize) -> Option<usize> {
    debug_assert_eq!(pat[i], b'[');
    i += 1;
    if i < pat.len() && pat[i] == b'^' {
        i += 1;
    }
    // Leading ']' is a member under POSIX but an error under RE2: reject.
    // prev_ascii: Some(true) after an ASCII member, Some(false) after a
    // multibyte member, None at the start or after a range/dash.
    let mut prev_ascii: Option<bool> = None;
    let mut any_member = false;
    while i < pat.len() {
        match pat[i] {
            b']' if any_member => return Some(i + 1),
            b']' => return None,
            b'\\' => return None,
            b'[' if i + 1 < pat.len() && matches!(pat[i + 1], b':' | b'.' | b'=') => {
                return None
            }
            b'-' => {
                // Literal at start or end; otherwise a range: both endpoints
                // must be ASCII (code-point ranges match; wider left closed).
                if i + 1 < pat.len() && pat[i + 1] == b']' {
                    i += 1;
                    any_member = true;
                    prev_ascii = None;
                } else if prev_ascii == Some(true) {
                    i += 1;
                    if i >= pat.len() || !pat[i].is_ascii() || matches!(pat[i], b'\\' | b'[') {
                        return None;
                    }
                    i += 1;
                    prev_ascii = None;
                } else if prev_ascii.is_none() && !any_member {
                    i += 1;
                    any_member = true;
                    prev_ascii = None;
                } else {
                    return None;
                }
            }
            b => {
                let len = utf8_char_len(b);
                if i + len > pat.len() {
                    return None;
                }
                prev_ascii = Some(len == 1);
                any_member = true;
                i += len;
            }
        }
    }
    None
}

fn scan_are(pat: &[u8]) -> Classification {
    let mut i = 0usize;
    let mut nquant = 0u32;
    let mut capture_safe = true;
    let mut has_capture = false;
    let mut has_alternation = false;
    // True when the previous item is an atom a quantifier may apply to.
    let mut quantifiable = false;
    // Did the just-closed atom's subtree contain a capturing group? A
    // quantifier over such a subtree makes capture positions untrusted
    // (last-iteration submatch semantics diverge).
    let mut last_atom_captures = false;
    // Per open group: (is_capturing, subtree_contains_capture_so_far).
    let mut stack: [(bool, bool); MAX_GROUP_DEPTH] = [(false, false); MAX_GROUP_DEPTH];
    let mut depth = 0usize;

    while i < pat.len() {
        match pat[i] {
            b'\\' => {
                if i + 1 >= pat.len() || !literal_escape_ok(pat[i + 1]) {
                    return INCOMPATIBLE;
                }
                i += 2;
                quantifiable = true;
                last_atom_captures = false;
            }
            b'[' => match parse_bracket(pat, i) {
                Some(next) => {
                    i = next;
                    quantifiable = true;
                    last_atom_captures = false;
                }
                None => return INCOMPATIBLE,
            },
            b'(' => {
                let capturing;
                if i + 1 < pat.len() && pat[i + 1] == b'?' {
                    // Only the non-capturing group; every other (?...) form
                    // (inline options, lookaround, named) is off-list.
                    if i + 2 >= pat.len() || pat[i + 2] != b':' {
                        return INCOMPATIBLE;
                    }
                    capturing = false;
                    i += 3;
                } else {
                    capturing = true;
                    has_capture = true;
                    i += 1;
                }
                if depth >= MAX_GROUP_DEPTH {
                    return INCOMPATIBLE;
                }
                stack[depth] = (capturing, false);
                depth += 1;
                quantifiable = false;
                last_atom_captures = false;
            }
            b')' => {
                if depth == 0 {
                    return INCOMPATIBLE;
                }
                depth -= 1;
                let (capturing, contains) = stack[depth];
                last_atom_captures = capturing || contains;
                if last_atom_captures && depth > 0 {
                    stack[depth - 1].1 = true;
                }
                i += 1;
                quantifiable = true;
            }
            b'*' | b'+' | b'?' => {
                if !quantifiable {
                    return INCOMPATIBLE;
                }
                nquant += 1;
                if nquant > MAX_QUANTIFIERS {
                    return INCOMPATIBLE;
                }
                if last_atom_captures {
                    capture_safe = false;
                }
                i += 1;
                if i < pat.len() && pat[i] == b'?' {
                    return INCOMPATIBLE;
                }
                quantifiable = false;
                last_atom_captures = false;
            }
            b'{' => {
                if !quantifiable {
                    return INCOMPATIBLE;
                }
                nquant += 1;
                if nquant > MAX_QUANTIFIERS {
                    return INCOMPATIBLE;
                }
                if last_atom_captures {
                    capture_safe = false;
                }
                match parse_bound(pat, i) {
                    Some(next) => i = next,
                    None => return INCOMPATIBLE,
                }
                if i < pat.len() && pat[i] == b'?' {
                    return INCOMPATIBLE;
                }
                quantifiable = false;
                last_atom_captures = false;
            }
            b'|' => {
                has_alternation = true;
                i += 1;
                quantifiable = false;
                last_atom_captures = false;
            }
            b'^' | b'$' => {
                i += 1;
                quantifiable = false;
                last_atom_captures = false;
            }
            b'.' => {
                i += 1;
                quantifiable = true;
                last_atom_captures = false;
            }
            b => {
                let len = utf8_char_len(b);
                if i + len > pat.len() {
                    return INCOMPATIBLE;
                }
                i += len;
                quantifiable = true;
                last_atom_captures = false;
            }
        }
    }
    if depth != 0 {
        return INCOMPATIBLE;
    }
    let tier = if capture_safe && !(has_capture && has_alternation) {
        Compat::CaptureSafe
    } else {
        Compat::WholeMatch
    };
    Classification { tier, needs_longest: has_alternation }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::regex_spencer::{REG_EXPANDED, REG_ICASE, REG_NEWLINE, REG_NLANCH, REG_NLSTOP};

    fn setup_utf8() {
        let _ = mbutils::SetDatabaseEncoding(wchar::PG_UTF8);
    }

    fn ok(p: &str) -> bool {
        setup_utf8();
        re2_compatible(p.as_bytes(), REG_ADVANCED)
    }

    #[test]
    fn admits_compatible_class() {
        for p in [
            r"^https?://(?:www\.)?([^/]+)/.*$",
            "",
            "abc",
            "a|b|",
            "(a)(b)(c)",
            "a{2}b{3,}c{4,5}",
            "[abc]",
            "[^abc]",
            "[a-z0-9]",
            "[-a]",
            "[a-]",
            "[a^b]",
            "x*y+z?",
            "^(foo|bar)$",
            r"\.\*\+\(\)",
            r"a\nb\tc",
            "déjà vu",
            "[é]",
            "((a|b)*c)+",
            "a{0,255}",
        ] {
            assert!(ok(p), "should admit {p:?}");
        }
    }

    #[test]
    fn rejects_incompatible_class() {
        for p in [
            r"(a)\1",       // backref
            r"\d+",         // ctype escape
            r"\w",
            r"\bword\b",
            r"\Aabc",
            r"a*?",         // non-greedy
            r"a{1,2}?",
            r"(?=x)",       // lookaround
            r"(?!x)",
            r"(?<=x)",
            r"(?i)abc",     // inline options
            "[[:alpha:]]",  // named class
            "[[=a=]]",
            "[[.a.]]",
            r"[\d]",        // escape inside bracket
            "[]a]",         // POSIX leading-]: RE2 errors
            "[é-z]",        // non-ASCII range endpoint
            "[a-é]",
            "a{256}",       // beyond Spencer DUPMAX
            "a{2,1}",       // malformed bound
            "a{}",
            "a{,2}",
            "{2}",          // nothing to repeat
            "*a",
            "a**",
            "(a",           // unbalanced
            "a)",
            r"a\",          // trailing backslash
        ] {
            assert!(!ok(p), "should reject {p:?}");
        }
    }

    #[test]
    fn rejects_incompatible_flags() {
        setup_utf8();
        let p = b"abc";
        assert!(re2_compatible(p, REG_ADVANCED));
        assert!(re2_compatible(p, REG_ADVANCED | REG_NOSUB));
        assert!(re2_compatible(p, REG_QUOTE));
        assert!(re2_compatible(p, REG_QUOTE | REG_NOSUB));
        for f in [
            REG_ADVANCED | REG_ICASE,
            REG_ADVANCED | REG_EXPANDED,
            REG_ADVANCED | REG_NLSTOP,
            REG_ADVANCED | REG_NLANCH,
            REG_ADVANCED | REG_NEWLINE,
            REG_QUOTE | REG_ICASE,
            0, // basic
            1, // extended
        ] {
            assert!(!re2_compatible(p, f), "should reject cflags {f:o}");
        }
    }

    #[test]
    fn rejects_non_utf8_pattern() {
        setup_utf8();
        assert!(!re2_compatible(b"a\xffb", REG_ADVANCED));
    }

    #[test]
    fn rejects_nul_bearing_pattern() {
        setup_utf8();
        assert!(!re2_compatible(b"a\x00b", REG_ADVANCED));
        assert!(!re2_compatible(b"\x00", REG_ADVANCED));
        assert!(!re2_compatible(b"a\x00b", REG_QUOTE));
    }

    #[test]
    fn capture_safety_tiers() {
        setup_utf8();
        let tier = |p: &str| classify(p.as_bytes(), REG_ADVANCED).tier;
        // Unquantified captures (the anchored URL-capture shape) stay capture-safe.
        assert_eq!(tier(r"^https?://(?:www\.)?([^/]+)/.*$"), Compat::CaptureSafe);
        assert_eq!(tier("(a)(b)(c)"), Compat::CaptureSafe);
        // Quantified non-capturing subtrees stay capture-safe.
        assert_eq!(tier("(?:ab)+(c)"), Compat::CaptureSafe);
        // Alternation without captures stays capture-safe (nothing to
        // misreport); alternation WITH captures is whole-match only.
        assert_eq!(tier("a|ab|abc"), Compat::CaptureSafe);
        assert_eq!(tier("^(foo|bar)$"), Compat::WholeMatch);
        assert_eq!(tier("(a)|b"), Compat::WholeMatch);
        assert_eq!(tier("(é|.[^a])"), Compat::WholeMatch);
        // A quantifier over a capture-bearing subtree: whole-match only
        // (the adversarial corpus found last-iteration capture divergence).
        assert_eq!(tier("(a)+"), Compat::WholeMatch);
        assert_eq!(tier("(a)?b"), Compat::WholeMatch);
        assert_eq!(tier("(a|b){2,3}"), Compat::WholeMatch);
        assert_eq!(tier("(?:(a)b)+"), Compat::WholeMatch);
        assert_eq!(tier("((a)b)*c"), Compat::WholeMatch);
        assert_eq!(
            tier(r"(.?|é|(?:0{2,}|é[^a][^/,]+ ?|\*)+é(?:c?0|\n{2})+)+|[a-c0-9]?|[ab]0+"),
            Compat::WholeMatch
        );
        // Quoted literals have no captures.
        assert_eq!(classify(b"a.c", REG_QUOTE).tier, Compat::CaptureSafe);
    }

    #[test]
    fn longest_mode_only_for_alternation() {
        setup_utf8();
        let c = |p: &str| classify(p.as_bytes(), REG_ADVANCED);
        assert!(!c(r"^https?://(?:www\.)?([^/]+)/.*$").needs_longest);
        assert!(!c("a*b+c{2,3}").needs_longest);
        assert!(c("a|ab").needs_longest);
        assert!(c("(?:a|b)c").needs_longest);
        assert!(!classify(b"a|b", REG_QUOTE).needs_longest);
    }

    #[test]
    fn rejects_beyond_complexity_budget() {
        setup_utf8();
        // The regex regress suite's Spencer-ETOOBIG case: RE2 compiles it,
        // Spencer errors — must never be admitted.
        assert!(!ok(&"x*y*z*".repeat(1000)));
        // Length budget (also applies to quoted literals: Spencer states
        // scale with literal length).
        assert!(!ok(&"a".repeat(MAX_PATTERN_BYTES + 1)));
        assert!(ok(&"a".repeat(MAX_PATTERN_BYTES)));
        assert!(!re2_compatible("a".repeat(MAX_PATTERN_BYTES + 1).as_bytes(), REG_QUOTE));
        // Quantifier budget.
        assert!(!ok(&"x*".repeat(MAX_QUANTIFIERS as usize + 1)));
        assert!(ok(&"x*".repeat(MAX_QUANTIFIERS as usize)));
    }
}
