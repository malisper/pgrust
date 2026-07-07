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
//! - non-UTF8 databases and patterns that are not valid UTF-8.

use ::regex_spencer::{REG_ADVANCED, REG_NOSUB, REG_QUOTE};

const PG_UTF8: i32 = wchar::PG_UTF8;

pub fn re2_compatible(pattern: &[u8], cflags: i32) -> bool {
    if mbutils::GetDatabaseEncoding() != PG_UTF8 {
        return false;
    }
    // REG_NOSUB is an execution hint callers OR in, not a semantic mode.
    let base = cflags & !REG_NOSUB;
    let quoted = base == REG_QUOTE;
    if !quoted && base != REG_ADVANCED {
        return false;
    }
    if core::str::from_utf8(pattern).is_err() {
        return false;
    }
    if quoted {
        return true;
    }
    scan_are(pattern)
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

fn scan_are(pat: &[u8]) -> bool {
    let mut i = 0usize;
    let mut depth = 0i32;
    // True when the previous item is an atom a quantifier may apply to.
    let mut quantifiable = false;

    while i < pat.len() {
        match pat[i] {
            b'\\' => {
                if i + 1 >= pat.len() || !literal_escape_ok(pat[i + 1]) {
                    return false;
                }
                i += 2;
                quantifiable = true;
            }
            b'[' => match parse_bracket(pat, i) {
                Some(next) => {
                    i = next;
                    quantifiable = true;
                }
                None => return false,
            },
            b'(' => {
                if i + 1 < pat.len() && pat[i + 1] == b'?' {
                    // Only the non-capturing group; every other (?...) form
                    // (inline options, lookaround, named) is off-list.
                    if i + 2 >= pat.len() || pat[i + 2] != b':' {
                        return false;
                    }
                    i += 3;
                } else {
                    i += 1;
                }
                depth += 1;
                quantifiable = false;
            }
            b')' => {
                depth -= 1;
                if depth < 0 {
                    return false;
                }
                i += 1;
                quantifiable = true;
            }
            b'*' | b'+' | b'?' => {
                if !quantifiable {
                    return false;
                }
                i += 1;
                if i < pat.len() && pat[i] == b'?' {
                    return false;
                }
                quantifiable = false;
            }
            b'{' => {
                if !quantifiable {
                    return false;
                }
                match parse_bound(pat, i) {
                    Some(next) => i = next,
                    None => return false,
                }
                if i < pat.len() && pat[i] == b'?' {
                    return false;
                }
                quantifiable = false;
            }
            b'|' => {
                i += 1;
                quantifiable = false;
            }
            b'^' | b'$' => {
                i += 1;
                quantifiable = false;
            }
            b'.' => {
                i += 1;
                quantifiable = true;
            }
            b => {
                let len = utf8_char_len(b);
                if i + len > pat.len() {
                    return false;
                }
                i += len;
                quantifiable = true;
            }
        }
    }
    depth == 0
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
}
