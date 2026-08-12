//! Extended-protocol mode planning for the differential runner (lanes
//! X1 + X2).
//!
//! Per statement, a deterministic seeded choice between the simple-query
//! path and the extended-protocol path (Parse/Bind/Describe/Execute/Sync),
//! applied IDENTICALLY on both sides of the differential. Extended-mode
//! SELECT-ish statements sometimes get parameterized — constant literals
//! lifted into $n placeholders — and sometimes get an Execute row limit,
//! which the client resumes after PortalSuspended (the portal-resume
//! surface). X2 adds the binary wire formats: extended statements sometimes
//! request ALL-BINARY results (Bind result-format 1 — the server's *_send
//! family becomes the compared surface, byte-for-byte for non-float
//! non-text types; see crate::client's decode policy), and each lifted
//! parameter is sometimes sent binary-format (the *_recv family:
//! int4recv/int8recv/textrecv/numeric_recv). Targets exec_parse_message /
//! exec_bind_message / exec_execute_message binary arms with zero new
//! grammar.
//!
//! Determinism law: the mode is a pure function of (xproto seed, statement
//! TEXT) — never of stream position — so ddmin reduction (which reindexes
//! statements) and `--replay` of a banked repro under the same `--xproto`
//! seed re-derive the exact same mode per statement. The mode is recorded
//! per finding in the JSONL output (`"mode"` key).
//!
//! Lifting disciplines. Every one of them is conservative — a skipped lift
//! costs nothing, a wrong lift would manufacture a both-side 42xxx (the
//! generator's zero-42xxx gate) or silently change semantics:
//!   - only statements starting with SELECT/WITH are lifted at all (DML and
//!     DDL keep their literal text; they still ride Parse/Bind/Execute);
//!   - the liftable region is exactly the WHERE clause: from the first
//!     ` WHERE ` up to the next clause keyword (GROUP BY / HAVING / WINDOW
//!     / ORDER BY / LIMIT / OFFSET / UNION / RETURNING). Outside it lie
//!     three real hazards: an ORDER BY ordinal or LIMIT count is not a
//!     value (lifting changes ordering or the row set); a lifted select-list
//!     constant no longer matches its GROUP BY key textually (42803); and a
//!     bare `SELECT $1` has no inferable type (42P18). WHERE quals have
//!     none of these, and are the interesting parameter surface anyway
//!     (generic-plan parameter evaluation);
//!   - every lift preserves the literal's own type EXACTLY, so semantics
//!     never move: a bare integer literal is int4 / int8 / numeric by its
//!     width (the parser's own rule) and lifts as `$n::int4` / `$n::int8` /
//!     `$n::numeric`; a bare decimal literal (`12.5`) is numeric and lifts
//!     as `$n::numeric`; a generator-shaped cast string literal
//!     (`('body')::text` / `('body')::varchar`) lifts whole as `$n::text` /
//!     `$n::varchar`. Naked string literals stay unlifted — `DATE '...'`
//!     typed literals and unknown-type coercions are exactly the hazards
//!     the cast form dodges;
//!   - number tokens must be standalone (never adjacent to an identifier
//!     char, `.`, or `$`; `1e3` exponent forms are skipped).

use crate::client::{Client, ConnLost, RawResult, WireParam};
use crate::rng::Rng;

/// Domain separator so the mode stream never collides with generator
/// streams derived from the same seed.
const XPROTO_DOMAIN: u64 = 0x7870_726f_746f_3131;

/// Seed flag: force EVERY statement onto the extended path with all-binary
/// results (no lifts, no row limit). The hand-verification and replay
/// vehicle for the binary result surface: `--xproto $((1<<63))` + a replay
/// deck compares each statement's *_send output byte-for-byte. Still a
/// pure function of (seed, text).
pub const XPROTO_FORCE_RESULT_BINARY: u64 = 1 << 63;

/// The type a lifted parameter is cast to; decides its binary encoder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParamKind {
    Int4,
    Int8,
    Numeric,
    Text,
    Varchar,
}

impl ParamKind {
    /// The cast suffix rendered after `$n`.
    fn cast(self) -> &'static str {
        match self {
            ParamKind::Int4 => "::int4",
            ParamKind::Int8 => "::int8",
            ParamKind::Numeric => "::numeric",
            ParamKind::Text => "::text",
            ParamKind::Varchar => "::varchar",
        }
    }
}

/// One planned parameter: its text value (None = NULL, never produced by
/// lifting), declared kind, and wire format.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedParam {
    pub value: String,
    pub kind: ParamKind,
    pub binary: bool,
}

impl PlannedParam {
    /// Encode for the wire per the planned format.
    pub fn to_wire(&self) -> WireParam {
        if self.binary {
            WireParam { bytes: Some(encode_binary_param(self.kind, &self.value)), binary: true }
        } else {
            WireParam::text(Some(&self.value))
        }
    }
}

/// Binary-format parameter encodings (the *_recv input surface).
/// int4/int8: network-order two's complement. text/varchar: the bytes.
/// numeric: the base-10000 digit-word format numeric_recv expects
/// (ndigits, weight, sign, dscale, digits) — hand-verified against C's
/// numeric_send output (see docs/fuzzing/findings-x2-binproto.md).
pub fn encode_binary_param(kind: ParamKind, value: &str) -> Vec<u8> {
    match kind {
        ParamKind::Int4 => value
            .parse::<i32>()
            .expect("planner lifted a non-int4 as Int4")
            .to_be_bytes()
            .to_vec(),
        ParamKind::Int8 => value
            .parse::<i64>()
            .expect("planner lifted a non-int8 as Int8")
            .to_be_bytes()
            .to_vec(),
        ParamKind::Text | ParamKind::Varchar => value.as_bytes().to_vec(),
        ParamKind::Numeric => encode_numeric_binary(value),
    }
}

/// pg numeric wire format for a plain decimal string (`-?\d+(\.\d+)?`).
/// Layout: i16 ndigits, i16 weight (base-10000 exponent of the first
/// digit word), u16 sign (0x0000 +, 0x4000 -), u16 dscale, ndigits x u16
/// base-10000 words, leading/trailing zero words stripped.
fn encode_numeric_binary(value: &str) -> Vec<u8> {
    let (neg, rest) = match value.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, value),
    };
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (i, f),
        None => (rest, ""),
    };
    debug_assert!(
        !int_part.is_empty()
            && int_part.bytes().all(|b| b.is_ascii_digit())
            && frac_part.bytes().all(|b| b.is_ascii_digit()),
        "planner lifted a non-decimal as Numeric: {value:?}"
    );
    let dscale = frac_part.len() as u16;

    // Left-pad the integer part to a multiple of 4, right-pad the fraction:
    // base-10000 digit words aligned to the decimal point.
    let int_pad = (4 - int_part.len() % 4) % 4;
    let mut digits_str = String::with_capacity(int_pad + int_part.len() + frac_part.len() + 3);
    for _ in 0..int_pad {
        digits_str.push('0');
    }
    digits_str.push_str(int_part);
    digits_str.push_str(frac_part);
    while digits_str.len() % 4 != 0 {
        digits_str.push('0');
    }
    let mut words: Vec<u16> = digits_str
        .as_bytes()
        .chunks(4)
        .map(|c| {
            c.iter().fold(0u16, |acc, b| acc * 10 + u16::from(b - b'0'))
        })
        .collect();
    // weight: base-10000 exponent of the first word.
    let mut weight = ((int_part.len() + int_pad) / 4) as i16 - 1;
    // Strip leading zero words (weight moves down with each).
    while words.first() == Some(&0) && words.len() > 1 {
        words.remove(0);
        weight -= 1;
    }
    // Strip trailing zero words (dscale already fixed).
    while words.last() == Some(&0) && words.len() > 1 {
        words.pop();
    }
    // True zero: ndigits 0, weight 0, positive.
    let is_zero = words == [0];
    if is_zero {
        words.clear();
        weight = 0;
    }
    let sign: u16 = if neg && !is_zero { 0x4000 } else { 0x0000 };
    let mut out = Vec::with_capacity(8 + 2 * words.len());
    out.extend_from_slice(&(words.len() as i16).to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&dscale.to_be_bytes());
    for w in &words {
        out.extend_from_slice(&w.to_be_bytes());
    }
    out
}

/// Extended-mode plan for one statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct XPlan {
    /// Statement text, with lifted literals replaced by $1..$n.
    pub sql: String,
    /// Parameter values, in placeholder order.
    pub params: Vec<PlannedParam>,
    /// Execute row limit; 0 = run to completion (no suspension).
    pub row_limit: u32,
    /// Bind requests every result column in binary format (X2: the *_send
    /// byte-compare surface).
    pub result_binary: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mode {
    Simple,
    Extended(XPlan),
}

fn fnv64(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn is_ident_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

fn is_selectish(sql: &str) -> bool {
    let t = sql.trim_start();
    t.starts_with("SELECT ") || t.starts_with("WITH ")
}

/// Statements whose portal can return rows (row-limit candidates).
fn returns_rows(sql: &str) -> bool {
    is_selectish(sql) || sql.contains(" RETURNING ")
}

/// One liftable literal occurrence (byte span + value + declared kind).
#[derive(Clone, Debug)]
struct Candidate {
    start: usize,
    end: usize,
    value: String,
    kind: ParamKind,
}

/// Clause keywords that end the WHERE clause (see module docs).
const WHERE_STOPS: &[&str] = &[
    " GROUP BY ",
    " HAVING ",
    " WINDOW ",
    " ORDER BY ",
    " LIMIT ",
    " OFFSET ",
    " UNION ",
    " INTERSECT ",
    " EXCEPT ",
    " RETURNING ",
];

/// The liftable byte range: from the first ` WHERE ` to the next clause
/// keyword at or after it (end of statement when there is none). Empty when
/// the statement carries no WHERE. A WHERE belonging to a subquery is
/// liftable too — it is still a qual, with none of the ordinal/grouping/
/// untypable hazards.
fn liftable_region(sql: &str) -> (usize, usize) {
    let Some(w) = sql.find(" WHERE ") else { return (0, 0) };
    let start = w + " WHERE ".len();
    let end = WHERE_STOPS
        .iter()
        .filter_map(|k| sql.find(k))
        .filter(|&p| p >= start)
        .min()
        .unwrap_or(sql.len());
    if end <= start {
        (0, 0)
    } else {
        (start, end)
    }
}

/// At `i` (pointing at an opening `'` whose preceding byte is `(`): match
/// the generator's cast-string shape `('body')::text` / `('body')::varchar`.
/// Returns (span_end, unescaped_body, kind).
fn match_cast_string(b: &[u8], sql: &str, i: usize) -> Option<(usize, String, ParamKind)> {
    // Find the closing quote ('' is an embedded quote).
    let mut j = i + 1;
    let mut body = String::new();
    loop {
        if j >= b.len() {
            return None;
        }
        if b[j] == b'\'' {
            if j + 1 < b.len() && b[j + 1] == b'\'' {
                body.push('\'');
                j += 2;
                continue;
            }
            j += 1;
            break;
        }
        // Multi-byte UTF-8 is copied byte-wise via the source slice below;
        // walk bytes but slice chars.
        let ch_len = {
            let s = &sql[j..];
            s.chars().next().map(|c| c.len_utf8()).unwrap_or(1)
        };
        body.push_str(&sql[j..j + ch_len]);
        j += ch_len;
    }
    // After the closing quote: `)::text` or `)::varchar`.
    for (suffix, kind) in [(")::text", ParamKind::Text), (")::varchar", ParamKind::Varchar)] {
        if sql[j..].starts_with(suffix) {
            let end = j + suffix.len();
            // The cast must not continue: a longer identifier, an array
            // cast (`::text[]` — the value is an array literal, and a
            // binary-format "text" param would hit array_recv as
            // insufficient-data 08P01), or a chained cast (`::text::x`).
            if end >= b.len()
                || !(is_ident_char(b[end]) || b[end] == b'[' || b[end] == b':')
            {
                return Some((end, body, kind));
            }
        }
    }
    None
}

/// Collect liftable literals in the WHERE region: standalone numbers
/// (int4/int8/numeric by width, decimals as numeric) and the generator's
/// cast-string shape. Other string literals are skipped over wholesale
/// (digits inside them are not candidates).
fn candidates(sql: &str) -> Vec<Candidate> {
    let b = sql.as_bytes();
    let (mut i, end) = liftable_region(sql);
    let mut out = Vec::new();
    while i < end {
        let c = b[i];
        if c == b'\'' {
            // A cast-string lift candidate iff preceded by '(' and shaped
            // ('body')::text|varchar; otherwise skip the literal wholesale.
            if i > 0 && b[i - 1] == b'(' {
                if let Some((cast_end, body, kind)) = match_cast_string(b, sql, i) {
                    if cast_end <= end {
                        out.push(Candidate {
                            start: i - 1,
                            end: cast_end,
                            value: body,
                            kind,
                        });
                        i = cast_end;
                        continue;
                    }
                }
            }
            // Skip the whole string literal ('' is an embedded quote).
            let mut j = i + 1;
            while j < b.len() {
                if b[j] == b'\'' {
                    if j + 1 < b.len() && b[j + 1] == b'\'' {
                        j += 2;
                        continue;
                    }
                    j += 1;
                    break;
                }
                j += 1;
            }
            i = j;
        } else if c.is_ascii_digit() {
            let start = i;
            let mut j = i + 1;
            while j < end && b[j].is_ascii_digit() {
                j += 1;
            }
            // Optional fraction: digits '.' digits (a trailing '.' or a
            // '.' not followed by a digit is not part of the number).
            let mut is_decimal = false;
            if j + 1 < end && b[j] == b'.' && b[j + 1].is_ascii_digit() {
                is_decimal = true;
                j += 1;
                while j < end && b[j].is_ascii_digit() {
                    j += 1;
                }
            }
            let prev_ok = start == 0 || {
                let p = b[start - 1];
                !is_ident_char(p) && p != b'.' && p != b'$'
            };
            let next_ok = j >= b.len() || {
                let n = b[j];
                !is_ident_char(n) && n != b'.'
            };
            if prev_ok && next_ok {
                let text = &sql[start..j];
                // The literal's own type by the parser's width rule:
                // decimal -> numeric; else int4 / int8 / numeric by fit.
                let kind = if is_decimal {
                    Some(ParamKind::Numeric)
                } else if text.parse::<i32>().is_ok() {
                    Some(ParamKind::Int4)
                } else if text.parse::<i64>().is_ok() {
                    Some(ParamKind::Int8)
                } else {
                    Some(ParamKind::Numeric)
                };
                if let Some(kind) = kind {
                    out.push(Candidate {
                        start,
                        end: j,
                        value: text.to_string(),
                        kind,
                    });
                }
            }
            // Consume any identifier/number tail so a rejected fragment
            // (1e3, t100) is never re-scanned mid-token; an exponent tail
            // also invalidates the candidate just pushed.
            let mut tail = j;
            while tail < end && (is_ident_char(b[tail]) || b[tail] == b'.') {
                tail += 1;
            }
            if tail != j {
                // Adjacent tail (exponent form or identifier): not a
                // standalone literal after all.
                if prev_ok
                    && out
                        .last()
                        .is_some_and(|cand| cand.start == start && cand.end == j)
                {
                    out.pop();
                }
                i = tail;
            } else {
                i = j;
            }
        } else {
            i += 1;
        }
    }
    out
}

/// Lift up to `want` literals into $n parameters. None when the statement
/// exposes no liftable literal. Each lifted parameter is independently
/// text- or binary-format (seeded).
fn lift_params(
    sql: &str,
    want: usize,
    rng: &mut Rng,
) -> Option<(String, Vec<PlannedParam>)> {
    let cands = candidates(sql);
    if cands.is_empty() || want == 0 {
        return None;
    }
    // Pick up to `want` distinct candidate indices, order-preserving.
    let mut chosen: Vec<usize> = Vec::new();
    for _ in 0..want.min(cands.len()) {
        let pick = rng.below_usize(cands.len());
        if !chosen.contains(&pick) {
            chosen.push(pick);
        }
    }
    chosen.sort_unstable();
    let mut out = String::with_capacity(sql.len());
    let mut params = Vec::with_capacity(chosen.len());
    let mut cursor = 0;
    for (n, &ci) in chosen.iter().enumerate() {
        let c = &cands[ci];
        out.push_str(&sql[cursor..c.start]);
        out.push_str(&format!("${}{}", n + 1, c.kind.cast()));
        params.push(PlannedParam {
            value: c.value.clone(),
            kind: c.kind,
            binary: rng.chance(1, 2),
        });
        cursor = c.end;
    }
    out.push_str(&sql[cursor..]);
    Some((out, params))
}

/// The per-statement mode decision: pure in (xseed, statement text).
pub fn plan_mode(xseed: u64, sql: &str) -> Mode {
    if xseed & XPROTO_FORCE_RESULT_BINARY != 0 {
        // Hand-verification / replay vehicle: every statement extended,
        // all-binary results, untouched text.
        return Mode::Extended(XPlan {
            sql: sql.to_string(),
            params: Vec::new(),
            row_limit: 0,
            result_binary: true,
        });
    }
    let mut rng = Rng::new(xseed ^ XPROTO_DOMAIN ^ fnv64(sql));
    if rng.chance(1, 2) {
        return Mode::Simple;
    }
    let mut out_sql = sql.to_string();
    let mut params: Vec<PlannedParam> = Vec::new();
    if is_selectish(sql) && rng.chance(1, 2) {
        let want = 1 + rng.below_usize(2); // 1-2 lifted literals
        if let Some((s, p)) = lift_params(sql, want, &mut rng) {
            out_sql = s;
            params = p;
        }
    }
    let row_limit = if returns_rows(sql) && rng.chance(1, 3) {
        [1u32, 2, 5][rng.below_usize(3)]
    } else {
        0
    };
    // X2: a third of extended statements request all-binary results.
    let result_binary = rng.chance(1, 3);
    Mode::Extended(XPlan { sql: out_sql, params, row_limit, result_binary })
}

/// Short per-statement mode tag for findings JSONL (`"mode"` key):
/// "simple" or "extended" with optional `:p<n>` (n params, of which
/// `b<m>` binary), `:l<k>` (row limit), `:rb` (binary results) — e.g.
/// "extended:p2b1:l5:rb".
pub fn mode_string(xseed: u64, sql: &str) -> String {
    match plan_mode(xseed, sql) {
        Mode::Simple => "simple".to_string(),
        Mode::Extended(x) => {
            let mut s = "extended".to_string();
            if !x.params.is_empty() {
                s.push_str(&format!(":p{}", x.params.len()));
                let nbin = x.params.iter().filter(|p| p.binary).count();
                if nbin > 0 {
                    s.push_str(&format!("b{nbin}"));
                }
            }
            if x.row_limit > 0 {
                s.push_str(&format!(":l{}", x.row_limit));
            }
            if x.result_binary {
                s.push_str(":rb");
            }
            s
        }
    }
}

/// Apply one statement over the wire in its planned mode.
pub fn apply_moded<S: std::io::Read + std::io::Write>(
    client: &mut Client<S>,
    xseed: u64,
    sql: &str,
) -> Result<Vec<RawResult>, ConnLost> {
    match plan_mode(xseed, sql) {
        Mode::Simple => client.simple_query(sql),
        Mode::Extended(x) => {
            let wire: Vec<WireParam> = x.params.iter().map(|p| p.to_wire()).collect();
            client.extended_query(&x.sql, &wire, x.row_limit, x.result_binary)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_params(x: &XPlan) -> Vec<(String, ParamKind)> {
        x.params.iter().map(|p| (p.value.clone(), p.kind)).collect()
    }

    #[test]
    fn mode_is_deterministic_and_text_keyed() {
        let sql = "SELECT t0.k_int FROM fz_scalar AS t0 WHERE t0.k_int > 5;";
        assert_eq!(plan_mode(7, sql), plan_mode(7, sql));
        // Some statement flips mode across seeds and across texts.
        let texts: Vec<String> =
            (0..64).map(|i| format!("SELECT {i} FROM fz_one AS t0;")).collect();
        let modes: Vec<bool> = texts
            .iter()
            .map(|t| matches!(plan_mode(1, t), Mode::Simple))
            .collect();
        assert!(modes.iter().any(|m| *m) && modes.iter().any(|m| !*m));
    }

    #[test]
    fn force_binary_seed_extends_everything() {
        let seed = XPROTO_FORCE_RESULT_BINARY | 7;
        for sql in [
            "SELECT t0.k_int FROM fz_scalar AS t0 WHERE t0.k_int > 5;",
            "INSERT INTO fz_scalar (pk) VALUES (100);",
            "CREATE TABLE zz (a int4);",
        ] {
            match plan_mode(seed, sql) {
                Mode::Extended(x) => {
                    assert_eq!(x.sql, sql);
                    assert!(x.params.is_empty());
                    assert_eq!(x.row_limit, 0);
                    assert!(x.result_binary);
                }
                Mode::Simple => panic!("force-binary seed produced simple mode"),
            }
        }
        assert_eq!(mode_string(seed, "SELECT 1;"), "extended:rb");
    }

    #[test]
    fn lift_replaces_standalone_where_integers_only() {
        let mut rng = Rng::new(0);
        // Identifier-adjacent digits (t100, c0) are never lifted; the
        // placeholder carries its ::int4 type.
        let sql = "SELECT t100.c0 FROM fz_scalar AS t100 WHERE t100.c0 > 42;";
        let (out, params) = lift_params(sql, 2, &mut rng).unwrap();
        assert_eq!(
            out,
            "SELECT t100.c0 FROM fz_scalar AS t100 WHERE t100.c0 > $1::int4;"
        );
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].value, "42");
        assert_eq!(params[0].kind, ParamKind::Int4);
    }

    #[test]
    fn lift_types_follow_the_literals_own_type() {
        let mut rng = Rng::new(0);
        // Wider than int4 -> int8 (the parser's own width rule).
        let sql = "SELECT t0.c FROM fz_one AS t0 WHERE t0.c > 9000000000;";
        let (out, params) = lift_params(sql, 1, &mut rng).unwrap();
        assert!(out.contains("$1::int8;"), "{out}");
        assert_eq!(text_params(&XPlan {
            sql: out,
            params,
            row_limit: 0,
            result_binary: false
        }), vec![("9000000000".to_string(), ParamKind::Int8)]);
        // Wider than int8 -> numeric.
        let sql = "SELECT t0.c FROM fz_one AS t0 WHERE t0.c > 99999999999999999999;";
        let (out, params) = lift_params(sql, 1, &mut rng).unwrap();
        assert!(out.contains("$1::numeric;"), "{out}");
        assert_eq!(params[0].kind, ParamKind::Numeric);
        // Decimal -> numeric (a bare decimal literal IS numeric).
        let sql = "SELECT t0.c FROM fz_one AS t0 WHERE t0.c > 1.5;";
        let (out, params) = lift_params(sql, 1, &mut rng).unwrap();
        assert!(out.contains("WHERE t0.c > $1::numeric;"), "{out}");
        assert_eq!(params[0].value, "1.5");
        assert_eq!(params[0].kind, ParamKind::Numeric);
        // Generator-shaped cast strings lift whole, unescaped.
        let sql = "SELECT t0.c FROM fz_one AS t0 WHERE t0.t = ('it''s')::text;";
        let (out, params) = lift_params(sql, 1, &mut rng).unwrap();
        assert!(out.contains("WHERE t0.t = $1::text;"), "{out}");
        assert_eq!(params[0].value, "it's");
        assert_eq!(params[0].kind, ParamKind::Text);
        let sql = "SELECT t0.c FROM fz_one AS t0 WHERE t0.v = ('')::varchar;";
        let (out, params) = lift_params(sql, 1, &mut rng).unwrap();
        assert!(out.contains("WHERE t0.v = $1::varchar;"), "{out}");
        assert_eq!(params[0].value, "");
        assert_eq!(params[0].kind, ParamKind::Varchar);
    }

    #[test]
    fn lift_skips_exponents_naked_strings_and_typed_literals() {
        let mut rng = Rng::new(0);
        for sql in [
            // Exponent form: not a standalone number token.
            "SELECT t0.c FROM fz_one AS t0 WHERE t0.c > 1e3;",
            // Digits inside naked string literals are invisible; the
            // literal itself is not the generator cast shape.
            "SELECT t0.c FROM fz_one AS t0 WHERE t0.d = DATE '2024-01-02';",
            "SELECT t0.c FROM fz_one AS t0 WHERE t0.t = 'a1b2';",
            // Array and chained casts are NOT the text cast shape: a
            // binary "text" param against array_recv is 08P01 (observed
            // live: ('{0}')::text[] lifted as $1::text left the [] behind).
            "SELECT t0.c FROM fz_one AS t0 WHERE t0.a = ('{0}')::text[];",
            "SELECT t0.c FROM fz_one AS t0 WHERE t0.t = ('x')::text::varchar;",
        ] {
            assert!(lift_params(sql, 2, &mut rng).is_none(), "lifted in {sql}");
        }
    }

    #[test]
    fn lift_is_confined_to_the_where_clause() {
        let mut rng = Rng::new(3);
        // ORDER BY ordinals, LIMIT/OFFSET counts, GROUP BY keys and
        // select-list constants all stay literal; only the WHERE integer
        // moves.
        let sql = "SELECT t0.k_int, 7 FROM fz_scalar AS t0 WHERE t0.k_int > 3 \
                   GROUP BY (7) ORDER BY 1, 2 LIMIT 5 OFFSET 1;";
        let (out, params) = lift_params(sql, 2, &mut rng).unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].value, "3");
        assert!(out.contains("SELECT t0.k_int, 7 FROM"), "{out}");
        assert!(out.contains("GROUP BY (7) ORDER BY 1, 2 LIMIT 5 OFFSET 1"), "{out}");
        assert!(out.contains("WHERE t0.k_int > $1::int4"), "{out}");
        // No WHERE at all -> no candidates.
        assert!(lift_params(
            "SELECT t0.k_int FROM fz_scalar AS t0 ORDER BY 1;",
            2,
            &mut rng
        )
        .is_none());
        // An inline window ORDER BY before the WHERE is not a clause stop:
        // the WHERE integer still lifts, the window key does not.
        let (out, params) = lift_params(
            "SELECT rank() OVER (ORDER BY t0.c) FROM fz_one AS t0 WHERE t0.c > 4;",
            2,
            &mut rng,
        )
        .unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].value, "4");
        assert!(out.contains("OVER (ORDER BY t0.c)"), "{out}");
        assert!(out.ends_with("WHERE t0.c > $1::int4;"), "{out}");
    }

    #[test]
    fn plan_only_parameterizes_selectish() {
        // Across many seeds, INSERTs may go extended but never carry params.
        let sql = "INSERT INTO fz_scalar (pk, k_int) VALUES (100, 7);";
        for seed in 0..64 {
            if let Mode::Extended(x) = plan_mode(seed, sql) {
                assert!(x.params.is_empty(), "params on non-select at seed {seed}");
                assert_eq!(x.sql, sql);
                assert_eq!(x.row_limit, 0, "row limit on non-row statement");
            }
        }
        // And SELECTs do get parameterized/limited/binaried at some seed.
        let sel = "SELECT t0.k_int, 5 FROM fz_scalar AS t0 WHERE t0.k_int <> 3;";
        let mut saw_params = false;
        let mut saw_limit = false;
        let mut saw_rb = false;
        let mut saw_binary_param = false;
        for seed in 0..256 {
            if let Mode::Extended(x) = plan_mode(seed, sel) {
                saw_params |= !x.params.is_empty();
                saw_limit |= x.row_limit > 0;
                saw_rb |= x.result_binary;
                saw_binary_param |= x.params.iter().any(|p| p.binary);
                for (i, _) in x.params.iter().enumerate() {
                    assert!(x.sql.contains(&format!("${}::int4", i + 1)), "{}", x.sql);
                }
                // The select-list constant is never the lifted one.
                assert!(x.sql.starts_with("SELECT t0.k_int, 5 FROM"), "{}", x.sql);
            }
        }
        assert!(saw_params, "no seed parameterized the SELECT");
        assert!(saw_limit, "no seed set a row limit");
        assert!(saw_rb, "no seed requested binary results");
        assert!(saw_binary_param, "no seed sent a binary parameter");
    }

    #[test]
    fn mode_string_shapes() {
        let sql = "SELECT t0.k_int, 5 FROM fz_scalar AS t0 WHERE t0.k_int <> 3;";
        let mut shapes: Vec<String> = (0..512u64).map(|s| mode_string(s, sql)).collect();
        shapes.sort();
        shapes.dedup();
        assert!(shapes.iter().any(|s| s == "simple"));
        assert!(shapes.iter().any(|s| s.starts_with("extended")));
        assert!(shapes.iter().any(|s| s.contains(":p")));
        assert!(shapes.iter().any(|s| s.ends_with(":rb")));
        for s in &shapes {
            assert!(
                s == "simple"
                    || s == "extended"
                    || s.starts_with("extended:"),
                "unexpected mode string {s}"
            );
        }
    }

    // ------------------------------------------------------------------
    // Binary parameter encoders.
    // ------------------------------------------------------------------

    #[test]
    fn int_and_text_encoders() {
        assert_eq!(encode_binary_param(ParamKind::Int4, "42"), 42i32.to_be_bytes());
        assert_eq!(encode_binary_param(ParamKind::Int4, "-1"), (-1i32).to_be_bytes());
        assert_eq!(
            encode_binary_param(ParamKind::Int8, "9000000000"),
            9000000000i64.to_be_bytes()
        );
        assert_eq!(encode_binary_param(ParamKind::Text, "it's"), b"it's");
        assert_eq!(encode_binary_param(ParamKind::Varchar, ""), b"");
    }

    #[test]
    fn numeric_encoder_matches_pg_wire_format() {
        // Expected bytes per the numeric_send layout (ndigits, weight,
        // sign, dscale, base-10000 words), hand-computed and live-verified
        // against C numeric_send (see the findings record).
        let enc = |v: &str| encode_numeric_binary(v);
        // 1.5 -> words [1, 5000], weight 0, dscale 1.
        assert_eq!(
            enc("1.5"),
            [0, 2, 0, 0, 0, 0, 0, 1, 0x00, 0x01, 0x13, 0x88].to_vec()
        );
        // 42 -> [42], weight 0, dscale 0.
        assert_eq!(enc("42"), [0, 1, 0, 0, 0, 0, 0, 0, 0, 42].to_vec());
        // 12345 -> words [1, 2345], weight 1.
        assert_eq!(
            enc("12345"),
            [0, 2, 0, 1, 0, 0, 0, 0, 0x00, 0x01, 0x09, 0x29].to_vec()
        );
        // 0.00001 -> word [1000] at weight -2, dscale 5.
        assert_eq!(
            enc("0.00001"),
            [0, 1, 0xff, 0xfe, 0, 0, 0, 5, 0x03, 0xe8].to_vec()
        );
        // -3.14 -> words [3, 1400], sign 0x4000, dscale 2.
        assert_eq!(
            enc("-3.14"),
            [0, 2, 0, 0, 0x40, 0, 0, 2, 0x00, 0x03, 0x05, 0x78].to_vec()
        );
        // 0 / 0.00 -> ndigits 0, weight 0, positive, dscale = frac len.
        assert_eq!(enc("0"), [0, 0, 0, 0, 0, 0, 0, 0].to_vec());
        assert_eq!(enc("0.00"), [0, 0, 0, 0, 0, 0, 0, 2].to_vec());
        assert_eq!(enc("-0.0"), [0, 0, 0, 0, 0, 0, 0, 1].to_vec());
        // 10000 -> word [1] at weight 1 (trailing zero word stripped).
        assert_eq!(enc("10000"), [0, 1, 0, 1, 0, 0, 0, 0, 0, 1].to_vec());
        // 9999999999 -> words [99, 9999, 9999], weight 2.
        assert_eq!(
            enc("9999999999"),
            [0, 3, 0, 2, 0, 0, 0, 0, 0, 99, 0x27, 0x0f, 0x27, 0x0f].to_vec()
        );
    }
}
