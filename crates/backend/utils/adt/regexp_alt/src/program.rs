//! Anchored pattern-program fast tier — the scoped response to ClickHouse
//! PR #108004 (JIT compilation of simple regexps), built without LLVM.
//!
//! A CONSTANT pattern that (a) the fail-closed classifier already admitted
//! to RE2 as CaptureSafe and (b) fits the even smaller subset compiled here
//! is turned once into a tiny op program executed by a tight interpreter:
//! memcmp for literal runs, memchr/memchr2/memchr3 for negated byte-set
//! spans, a 256-bit table for the rest. The flagship anchored-URL pattern
//! `^https?://(?:www\.)?([^/]+)/.*$` compiles to
//! `Lit "http"; Opt "s"; Lit "://"; Opt "www."; CapStart;
//!  Set(not '/', min 1); CapEnd; Lit "/"; TailAny`.
//!
//! Subset (v1, the anchored-URL shape family and PR #108004's core):
//! - `^`-anchored only; `$` only as the final atom;
//! - literal runs (escapes decoded per the classifier's whitelist);
//! - optional atoms `X?`, `(?:literal)?` — greedy try-with-first;
//! - ASCII-member bracket classes, plain or negated, quantified:
//!   positive classes take `* + ? {n[,m]}` (bytes == runes), negated
//!   classes only `*`/`+` (unbounded — the span end and every give-back
//!   step land on UTF-8 rune boundaries, so byte scanning equals RE2's
//!   rune semantics; counted/0-or-1 rune forms would not);
//! - at most ONE capture group, unquantified, containing only the above;
//! - `.` only as a trailing `.*`/`.+` (optionally `$`-closed) — anywhere
//!   else runes-vs-bytes counting diverges.
//! Anything else -> compile returns None and the pattern runs on RE2
//! exactly as before. Byte-identical semantics are the bar: the executor
//! is a greedy leftmost-first backtracker, provably identical to RE2's
//! Perl mode on this subset (no alternation => needs_longest is false, so
//! the RE2 arm it must mirror is first-match too).
//!
//! Preconditions the caller guarantees (both enforced by the auto
//! dispatch): the pattern was classified CaptureSafe with needs_longest
//! false, and every subject reaching exec is NUL-free valid UTF-8 (the
//! per-evaluation data guard re-routes everything else to Spencer before
//! any RE2-arm code runs).
//!
//! Any runtime anomaly — a backtracking step budget overrun on
//! pathological subjects — returns None from exec and the caller falls
//! back to the compiled RE2 pattern, never a wrong answer.

use super::GroupSpan;

const MAX_OPS: usize = 64;

// Backtracking budget: generous for real subjects (the URL pattern uses ~1 step per
// op), tripped only by adversarial span-stacking; overruns fall back to
// RE2, which is linear-time.
fn step_budget(hay_len: usize) -> u64 {
    1024 + 8 * hay_len as u64
}

#[derive(Clone)]
struct ByteSet([u64; 4]);

impl ByteSet {
    fn empty() -> Self {
        ByteSet([0; 4])
    }

    fn insert(&mut self, b: u8) {
        self.0[(b >> 6) as usize] |= 1u64 << (b & 63);
    }

    fn insert_range(&mut self, lo: u8, hi: u8) {
        for b in lo..=hi {
            self.insert(b);
        }
    }

    fn negate(&mut self) {
        for w in &mut self.0 {
            *w = !*w;
        }
    }

    #[inline]
    fn contains(&self, b: u8) -> bool {
        self.0[(b >> 6) as usize] & (1u64 << (b & 63)) != 0
    }

    // The ASCII bytes NOT in the set (used to drive memchr on negated
    // classes, where the excluded members are the scan stoppers).
    fn excluded_ascii(&self) -> Vec<u8> {
        (0u8..128).filter(|&b| !self.contains(b)).collect()
    }
}

enum Scan {
    // Negated class with 1-3 excluded bytes: memchr family.
    Not1(u8),
    Not2(u8, u8),
    Not3(u8, u8, u8),
    Table,
}

enum Op {
    // Exact literal run.
    Lit(Box<[u8]>),
    // Optional literal run, greedy (with-branch first).
    Opt(Box<[u8]>),
    // Greedy byte-set span with give-back. min/max are in BYTES; the
    // compiler only emits byte bounds where they provably equal RE2's rune
    // bounds (ascii_only sets, or negated sets with min<=1, max unbounded).
    // ascii_only means the set can never match a non-ASCII byte, so every
    // position inside the span is a rune boundary; otherwise give-back
    // steps skip UTF-8 continuation bytes to stay on boundaries.
    Set { set: ByteSet, scan: Scan, min: u32, max: u32, ascii_only: bool },
    CapStart,
    CapEnd,
    // Trailing `.*`/`.+`: consume every remaining byte (min_one asserts at
    // least one byte, == at least one rune on valid UTF-8).
    TailAny { min_one: bool },
    // `$`: end of subject.
    End,
}

pub struct Program {
    ops: Vec<Op>,
    ngroups: usize,
}

// ---------------------------------------------------------------------
// Compiler
// ---------------------------------------------------------------------

fn utf8_char_len(b: u8) -> usize {
    match b {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        _ => 4,
    }
}

// Decodes the classifier-admitted literal escapes (literal_escape_ok):
// C-style controls plus escaped ASCII punctuation.
fn decode_escape(c: u8) -> Option<u8> {
    Some(match c {
        b'n' => b'\n',
        b't' => b'\t',
        b'r' => b'\r',
        b'f' => 0x0C,
        b'v' => 0x0B,
        b'a' => 0x07,
        _ if c.is_ascii() && !c.is_ascii_alphanumeric() => c,
        _ => return None,
    })
}

struct Compiler<'a> {
    pat: &'a [u8],
    i: usize,
    ops: Vec<Op>,
    lit: Vec<u8>,
    ngroups: usize,
}

impl<'a> Compiler<'a> {
    fn peek(&self) -> Option<u8> {
        self.pat.get(self.i).copied()
    }

    fn flush_lit(&mut self) {
        if !self.lit.is_empty() {
            let run = core::mem::take(&mut self.lit);
            self.ops.push(Op::Lit(run.into_boxed_slice()));
        }
    }

    fn push(&mut self, op: Op) -> Option<()> {
        self.flush_lit();
        if self.ops.len() >= MAX_OPS {
            return None;
        }
        self.ops.push(op);
        Some(())
    }

    // One literal atom (plain char or decoded escape) starting at self.i;
    // returns its bytes and leaves self.i past it. None = out of subset.
    fn parse_literal_atom(&mut self) -> Option<Vec<u8>> {
        let b = self.peek()?;
        match b {
            b'\\' => {
                let c = *self.pat.get(self.i + 1)?;
                let lit = decode_escape(c)?;
                self.i += 2;
                Some(vec![lit])
            }
            b'(' | b')' | b'[' | b']' | b'{' | b'*' | b'+' | b'?' | b'|' | b'^' | b'$' | b'.' => {
                None
            }
            _ => {
                let len = utf8_char_len(b);
                if self.i + len > self.pat.len() {
                    return None;
                }
                let bytes = self.pat[self.i..self.i + len].to_vec();
                self.i += len;
                Some(bytes)
            }
        }
    }

    // Parses `[...]` starting at self.i (on '['), all-ASCII members only.
    // Returns (set, negated) with self.i just past ']'.
    fn parse_class(&mut self) -> Option<(ByteSet, bool)> {
        debug_assert_eq!(self.pat[self.i], b'[');
        self.i += 1;
        let negated = self.peek() == Some(b'^');
        if negated {
            self.i += 1;
        }
        let mut set = ByteSet::empty();
        let mut prev: Option<u8> = None;
        let mut any = false;
        loop {
            let b = self.peek()?;
            match b {
                b']' if any => {
                    self.i += 1;
                    break;
                }
                b']' => return None,
                b'\\' | b'[' => return None,
                b'-' => {
                    // Literal at start or end; otherwise an ASCII range.
                    if self.pat.get(self.i + 1) == Some(&b']') || (!any && prev.is_none()) {
                        set.insert(b'-');
                        self.i += 1;
                        any = true;
                        prev = None;
                    } else if let Some(lo) = prev {
                        let hi = *self.pat.get(self.i + 1)?;
                        if !hi.is_ascii() || matches!(hi, b'\\' | b'[' | b']') || hi < lo {
                            return None;
                        }
                        set.insert_range(lo, hi);
                        self.i += 2;
                        any = true;
                        prev = None;
                    } else {
                        return None;
                    }
                }
                _ => {
                    // Program subset: ASCII members only (the classifier
                    // admits multibyte members, but byte-set scanning of
                    // those diverges from rune semantics).
                    if !b.is_ascii() {
                        return None;
                    }
                    set.insert(b);
                    self.i += 1;
                    any = true;
                    prev = Some(b);
                }
            }
        }
        Some((set, negated))
    }

    // Parses an optional quantifier at self.i. Returns (min, max) with
    // u32::MAX meaning unbounded, or (1, 1) when no quantifier follows.
    fn parse_quantifier(&mut self) -> Option<(u32, u32)> {
        match self.peek() {
            Some(b'*') => {
                self.i += 1;
                Some((0, u32::MAX))
            }
            Some(b'+') => {
                self.i += 1;
                Some((1, u32::MAX))
            }
            Some(b'?') => {
                self.i += 1;
                Some((0, 1))
            }
            Some(b'{') => {
                self.i += 1;
                let mut m: u32 = 0;
                let start = self.i;
                while let Some(d @ b'0'..=b'9') = self.peek() {
                    m = m.checked_mul(10)?.checked_add((d - b'0') as u32)?;
                    if m > 255 {
                        return None;
                    }
                    self.i += 1;
                }
                if self.i == start {
                    return None;
                }
                match self.peek() {
                    Some(b'}') => {
                        self.i += 1;
                        Some((m, m))
                    }
                    Some(b',') => {
                        self.i += 1;
                        if self.peek() == Some(b'}') {
                            self.i += 1;
                            return Some((m, u32::MAX));
                        }
                        let mut n: u32 = 0;
                        let nstart = self.i;
                        while let Some(d @ b'0'..=b'9') = self.peek() {
                            n = n.checked_mul(10)?.checked_add((d - b'0') as u32)?;
                            if n > 255 {
                                return None;
                            }
                            self.i += 1;
                        }
                        if self.i == nstart || n < m || self.peek() != Some(b'}') {
                            return None;
                        }
                        self.i += 1;
                        Some((m, n))
                    }
                    _ => None,
                }
            }
            _ => Some((1, 1)),
        }
    }

    fn push_set(&mut self, set: ByteSet, negated: bool, min: u32, max: u32) -> Option<()> {
        let (set, ascii_only) = if negated {
            let mut s = set;
            s.negate();
            (s, false)
        } else {
            (set, true)
        };
        if !ascii_only {
            // Byte bounds equal rune bounds on a non-ASCII-capable set only
            // for min <= 1 with unbounded max (span ends and give-back
            // steps stay on rune boundaries; counting does not).
            if min > 1 || max != u32::MAX {
                return None;
            }
        }
        let scan = if !ascii_only {
            match set.excluded_ascii().as_slice() {
                [a] => Scan::Not1(*a),
                [a, b] => Scan::Not2(*a, *b),
                [a, b, c] => Scan::Not3(*a, *b, *c),
                _ => Scan::Table,
            }
        } else {
            Scan::Table
        };
        self.push(Op::Set { set, scan, min, max, ascii_only })
    }

    // Parses a sequence of atoms; in_group parses a capture group body
    // (stops at ')'), toplevel parses to the end of the pattern.
    fn parse_seq(&mut self, in_group: bool) -> Option<()> {
        while let Some(b) = self.peek() {
            match b {
                b')' if in_group => return Some(()),
                b')' | b'|' | b'^' => return None,
                b'$' => {
                    // Only as the final atom of the whole pattern.
                    if in_group || self.i + 1 != self.pat.len() {
                        return None;
                    }
                    self.i += 1;
                    self.push(Op::End)?;
                }
                b'.' => {
                    // Only as a trailing `.*` / `.+` (optionally `$`).
                    if in_group {
                        return None;
                    }
                    let q = *self.pat.get(self.i + 1)?;
                    if q != b'*' && q != b'+' {
                        return None;
                    }
                    let rest = &self.pat[self.i + 2..];
                    if !(rest.is_empty() || rest == b"$") {
                        return None;
                    }
                    self.i += 2;
                    self.push(Op::TailAny { min_one: q == b'+' })?;
                }
                b'(' => {
                    if self.pat.get(self.i + 1) == Some(&b'?') {
                        // Only `(?:` with a pure literal body.
                        if self.pat.get(self.i + 2) != Some(&b':') {
                            return None;
                        }
                        self.i += 3;
                        let mut body = Vec::new();
                        while self.peek() != Some(b')') {
                            body.extend_from_slice(&self.parse_literal_atom()?);
                        }
                        self.i += 1;
                        match self.parse_quantifier()? {
                            (1, 1) => self.lit.extend_from_slice(&body),
                            (0, 1) => self.push(Op::Opt(body.into_boxed_slice()))?,
                            _ => return None,
                        }
                    } else {
                        // THE capture group: one, toplevel, unquantified.
                        if in_group || self.ngroups > 0 {
                            return None;
                        }
                        self.ngroups = 1;
                        self.i += 1;
                        self.push(Op::CapStart)?;
                        self.parse_seq(true)?;
                        if self.peek() != Some(b')') {
                            return None;
                        }
                        self.i += 1;
                        self.push(Op::CapEnd)?;
                        if matches!(self.peek(), Some(b'*' | b'+' | b'?' | b'{')) {
                            return None;
                        }
                    }
                }
                b'[' => {
                    let (set, negated) = self.parse_class()?;
                    let (min, max) = self.parse_quantifier()?;
                    self.push_set(set, negated, min, max)?;
                }
                _ => {
                    let atom = self.parse_literal_atom()?;
                    match self.parse_quantifier()? {
                        (1, 1) => self.lit.extend_from_slice(&atom),
                        (0, 1) => self.push(Op::Opt(atom.into_boxed_slice()))?,
                        (min, max) => {
                            // Quantified single ASCII char == singleton
                            // positive class; multibyte would count runes.
                            let [b] = *atom else { return None };
                            if !b.is_ascii() {
                                return None;
                            }
                            let mut set = ByteSet::empty();
                            set.insert(b);
                            self.push_set(set, false, min, max)?;
                        }
                    }
                }
            }
        }
        if in_group {
            return None; // unclosed group
        }
        Some(())
    }
}

// Compiles a classifier-admitted (CaptureSafe, needs_longest=false, valid
// UTF-8, NUL-free, ARE-mode) pattern into a Program when it fits the
// anchored subset; None = run RE2 as before.
pub fn compile(pattern: &[u8]) -> Option<Program> {
    let mut c = Compiler { pat: pattern, i: 0, ops: Vec::new(), lit: Vec::new(), ngroups: 0 };
    if c.peek() != Some(b'^') {
        return None;
    }
    c.i += 1;
    c.parse_seq(false)?;
    c.flush_lit();
    if c.ops.len() > MAX_OPS {
        return None;
    }
    Some(Program { ops: c.ops, ngroups: c.ngroups })
}

// ---------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------

#[inline]
fn is_utf8_cont(b: u8) -> bool {
    (b & 0xC0) == 0x80
}

struct Run<'a> {
    hay: &'a [u8],
    ops: &'a [Op],
    steps: u64,
    cap: GroupSpan,
    end: i64,
}

enum Bail {
    Budget,
}

impl<'a> Run<'a> {
    fn run(&mut self, ip: usize, pos: usize) -> Result<bool, Bail> {
        if self.steps == 0 {
            return Err(Bail::Budget);
        }
        self.steps -= 1;
        let Some(op) = self.ops.get(ip) else {
            self.end = pos as i64;
            return Ok(true);
        };
        match op {
            Op::Lit(lit) => {
                if self.hay[pos..].starts_with(lit) {
                    self.run(ip + 1, pos + lit.len())
                } else {
                    Ok(false)
                }
            }
            Op::Opt(lit) => {
                if self.hay[pos..].starts_with(lit) && self.run(ip + 1, pos + lit.len())? {
                    return Ok(true);
                }
                self.run(ip + 1, pos)
            }
            Op::Set { set, scan, min, max, ascii_only } => {
                let rest = &self.hay[pos..];
                let cap_bytes = if *max == u32::MAX { rest.len() } else { rest.len().min(*max as usize) };
                let k = match scan {
                    Scan::Not1(a) => memchr::memchr(*a, &rest[..cap_bytes]).unwrap_or(cap_bytes),
                    Scan::Not2(a, b) => {
                        memchr::memchr2(*a, *b, &rest[..cap_bytes]).unwrap_or(cap_bytes)
                    }
                    Scan::Not3(a, b, c) => {
                        memchr::memchr3(*a, *b, *c, &rest[..cap_bytes]).unwrap_or(cap_bytes)
                    }
                    Scan::Table => {
                        let mut k = 0usize;
                        while k < cap_bytes && set.contains(rest[k]) {
                            k += 1;
                        }
                        k
                    }
                };
                if k < *min as usize {
                    return Ok(false);
                }
                // Greedy with give-back; every tried end is a rune boundary
                // (ascii_only spans contain only ASCII; otherwise we step
                // over continuation bytes, and the greedy end itself stops
                // at an excluded ASCII byte or end of subject).
                let floor = pos + *min as usize;
                let mut e = pos + k;
                loop {
                    if self.run(ip + 1, e)? {
                        return Ok(true);
                    }
                    if e <= floor {
                        return Ok(false);
                    }
                    e -= 1;
                    if !*ascii_only {
                        while e > floor && is_utf8_cont(self.hay[e]) {
                            e -= 1;
                        }
                        // A boundary below floor means floor itself is
                        // mid-rune only if min crosses a rune — impossible:
                        // non-ascii sets carry min <= 1 and hay[pos] starts
                        // a rune, so floor is pos or pos+1<=first boundary.
                        if e < floor || is_utf8_cont(self.hay[e]) {
                            return Ok(false);
                        }
                    }
                }
            }
            Op::CapStart => {
                self.cap.0 = pos as i64;
                self.run(ip + 1, pos)
            }
            Op::CapEnd => {
                self.cap.1 = pos as i64;
                self.run(ip + 1, pos)
            }
            Op::TailAny { min_one } => {
                if *min_one && pos == self.hay.len() {
                    return Ok(false);
                }
                self.run(ip + 1, self.hay.len())
            }
            Op::End => {
                if pos == self.hay.len() {
                    self.run(ip + 1, pos)
                } else {
                    Ok(false)
                }
            }
        }
    }
}

impl Program {
    pub fn ngroups(&self) -> usize {
        self.ngroups
    }

    // Match at the start of hay (the anchored subset can only match there;
    // callers with a nonzero start offset must use the RE2 arm). Fills out
    // exactly like Re2Re::match_at. None = step budget exceeded (fall back
    // to RE2); the answer is never wrong, only occasionally refused.
    pub fn exec(&self, hay: &[u8], out: &mut [GroupSpan]) -> Option<bool> {
        let mut st = Run {
            hay,
            ops: &self.ops,
            steps: step_budget(hay.len()),
            cap: (-1, -1),
            end: 0,
        };
        match st.run(0, 0) {
            Err(Bail::Budget) => None,
            Ok(false) => Some(false),
            Ok(true) => {
                let n = out.len().min(self.ngroups + 1);
                if n >= 1 {
                    out[0] = (0, st.end);
                }
                if n >= 2 {
                    out[1] = st.cap;
                }
                for slot in out.iter_mut().skip(n) {
                    *slot = (-1, -1);
                }
                Some(true)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prog(p: &str) -> Program {
        compile(p.as_bytes()).unwrap_or_else(|| panic!("should compile {p:?}"))
    }

    fn m(p: &str, s: &str) -> Option<(GroupSpan, GroupSpan)> {
        let pr = prog(p);
        let mut out = [(-2, -2); 2];
        match pr.exec(s.as_bytes(), &mut out) {
            Some(true) => Some((out[0], out[1])),
            Some(false) => None,
            None => panic!("budget bail on {p:?} / {s:?}"),
        }
    }

    #[test]
    fn q29_shape() {
        let p = r"^https?://(?:www\.)?([^/]+)/.*$";
        let (g0, g1) = m(p, "http://www.example.com/path/x?y=1").unwrap();
        assert_eq!(g0, (0, 33));
        assert_eq!(g1, (11, 22)); // example.com
        let (g0, g1) = m(p, "https://sub.host.ru/").unwrap();
        assert_eq!(g0, (0, 20));
        assert_eq!(g1, (8, 19));
        assert!(m(p, "not-a-url").is_none());
        assert!(m(p, "http://hostonly.com").is_none());
        assert!(m(p, "").is_none());
        // No "www." prefix: capture starts right after "://".
        let (_, g1) = m(p, "http://www/").unwrap();
        assert_eq!(g1, (7, 10)); // "www"
        // Multibyte host bytes ride through the negated span.
        let s = "https://пример.рф/страница";
        let (_, g1) = m(p, s).unwrap();
        let host_end = s.find("/страница").unwrap();
        assert_eq!(g1, (8, host_end as i64));
    }

    #[test]
    fn subset_membership() {
        for p in [
            r"^https?://(?:www\.)?([^/]+)/.*$",
            "^abc",
            "^abc$",
            "^",
            "^$",
            "^a?b",
            "^(?:foo)?bar",
            "^[a-z]+$",
            "^[^,]*x",
            "^a{2,3}b",
            "^x*y+z?",
            "^([0-9]{4})-",
            "^a.*",
            "^a.+$",
            r"^\.\*x",
            "^é?x",
        ] {
            assert!(compile(p.as_bytes()).is_some(), "should compile {p:?}");
        }
        for p in [
            "abc",           // unanchored
            "^a|b",          // alternation
            "^a.b",          // mid-pattern dot
            "^.*a",          // dot not trailing
            "^(a)(b)",       // two captures
            "^(a)?",         // quantified capture
            "^((a))",        // nested capture
            "^[é]+",         // multibyte class member
            "^[^/]",         // 0-or-1-rune negated class (bare)
            "^[^/]{2,3}",    // counted negated class
            "^é+",           // quantified multibyte literal
            "^a$b",          // interior $
            "^a^b",          // interior ^
            "^(?:a[b])?",    // non-literal (?:) body
            "^(a$)",         // $ inside group
            r"^\d",          // (never classifier-admitted anyway)
        ] {
            assert!(compile(p.as_bytes()).is_none(), "should refuse {p:?}");
        }
    }

    #[test]
    fn greedy_give_back() {
        // Span must give back for the trailing literal.
        let (_, g1) = m("^([a-z]+)z$", "abcz").unwrap();
        assert_eq!(g1, (0, 3));
        // Give-back to the minimum.
        let (_, g1) = m("^([a-z]+)bc$", "abc").unwrap();
        assert_eq!(g1, (0, 1));
        assert!(m("^[a-z]+z$", "z").is_none()); // min 1 then 'z' unmet
        // Counted bounds.
        assert!(m("^a{2,3}$", "a").is_none());
        assert!(m("^a{2,3}$", "aa").is_some());
        assert!(m("^a{2,3}$", "aaa").is_some());
        assert!(m("^a{2,3}$", "aaaa").is_none());
        // Optional atom give-back: with-branch fails deeper, without wins.
        assert!(m("^ab?bc$", "abc").is_some());
        // Optional group give-back (the www. shape).
        let (_, g1) = m(r"^(?:www\.)?([^/]+)$", "www.x").unwrap();
        assert_eq!(g1, (4, 5));
        let (_, g1) = m(r"^(?:www\.)?([^/]+)$", "www").unwrap();
        assert_eq!(g1, (0, 3));
    }

    #[test]
    fn rune_boundary_give_back() {
        // Negated-span give-back over multibyte content must stay on rune
        // boundaries: "xé" + "y", trailing literal "y".
        let (_, g1) = m("^([^a]+)y$", "xéy").unwrap();
        assert_eq!(g1, (0, 3)); // "xé" — never 0..2 (mid-é)
        // Adversarial shape from the design analysis: ^([^a]+)[^b]+X$ on
        // x é X — byte give-back would capture mid-rune; rune-stepped
        // give-back must agree with RE2's "x".
        let (_, g1) = m("^([^a]+)[^b]+X$", "xéX").unwrap();
        assert_eq!(g1, (0, 1));
    }

    #[test]
    fn anchors_and_tails() {
        assert_eq!(m("^", "abc").unwrap().0, (0, 0));
        assert_eq!(m("^$", "").unwrap().0, (0, 0));
        assert!(m("^$", "x").is_none());
        assert_eq!(m("^abc", "abcdef").unwrap().0, (0, 3));
        assert!(m("^abc$", "abcdef").is_none());
        assert_eq!(m("^a.*", "abc").unwrap().0, (0, 3));
        assert_eq!(m("^a.*$", "a").unwrap().0, (0, 1));
        assert!(m("^a.+$", "a").is_none());
        assert!(m("^a.+$", "ab").is_some());
    }

    #[test]
    fn budget_bail_falls_back() {
        // Stacked overlapping spans: exponential backtracking on a
        // non-matching subject must trip the budget, not hang.
        let p = "^[ab]*[ab]*[ab]*[ab]*[ab]*[ab]*[ab]*[ab]*c$";
        let pr = prog(p);
        let hay = "ab".repeat(40);
        assert_eq!(pr.exec(hay.as_bytes(), &mut []), None);
    }
}
