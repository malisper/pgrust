//! STRINGFUNC lane (covdiff campaign): the SQL string-function surface in
//! backend/utils/adt/{varlena.c, oracle_compat.c, formatting.c
//! (non-numeric), ascii.c}. Everything is emitted as `StmtKind::Raw` over
//! deterministic literal inputs (empty, ASCII, multibyte UTF-8, combining
//! sequences, whitespace, NUL-adjacent) drawn from the session PRNG, so the
//! same seed reproduces a byte-identical stream. The AST scoping checker has
//! nothing to check on Raw statements; the differ's ::text-cast scalar
//! compare covers value identity and its SQLSTATE compare covers the
//! deliberate error arms.
//!
//! Non-overlap: the standing string coverage is a handful of hand-picked
//! examples scattered through `adtmisc` (adtm:strfns/trim2/ident/bit2/
//! byteax) plus single error-arm probes in `earm3`; none is a systematic
//! randomized generator over this surface, and the families here
//! (substring/overlay/position/left/right/pad/repeat/replace/translate/
//! split_part/string_to_*/concat/format/quote_*/case/ascii/chr/reverse/
//! starts_with/length/normalize/unistr/hash) had zero SQL-emitting fuzz
//! coverage before this lane.
//!
//! Validity / determinism disciplines:
//!   - scalar outputs are compared byte-exact; text-returning functions
//!     compare directly, bytea (sha*) is cast `::text` (\x-hex), and the
//!     integer/boolean returns (position/length/ascii/hashtext/
//!     starts_with/IS NORMALIZED) are already exact;
//!   - upper/lower/initcap read the database CTYPE, which is locale-
//!     dependent, so the string argument is pinned `COLLATE "C"`: under C
//!     ctype the case map touches ASCII only and leaves bytes > 127
//!     unchanged — deterministic and identical on both differential sides
//!     regardless of the rig locale (the charter's collation pin);
//!   - normalize / IS NORMALIZED / unistr are Unicode-standard for the
//!     pinned server version (NFC/NFD/NFKC/NFKD tables are engine-
//!     independent), so multibyte + combining inputs compare byte-exact;
//!   - md5/sha*/hashtext are fixed, portable algorithms (hashtext feeds
//!     hash partitioning, so it is stable by contract) — identical on both
//!     engines;
//!   - no nondeterministic function is ever emitted (no now/random/uuid);
//!   - deliberate error fuel (chr(0), negative substring length,
//!     over-large allocation, out-of-range chr) rides a single strf:err
//!     shape, biased low per the findings-budget rule so both-sides-error
//!     statements stay a small share of the stream.

use crate::stmt::{Gen, StmtKind};

/// Statement shapes (top-level weighted pick; every entry is registered in
/// weights::PROD_WEIGHTS).
const SHAPES: &[&str] = &[
    "strf:substr",
    "strf:overlay",
    "strf:pos",
    "strf:leftright",
    "strf:pad",
    "strf:trim",
    "strf:reprep",
    "strf:translate",
    "strf:split",
    "strf:concat",
    "strf:format",
    "strf:quote",
    "strf:case",
    "strf:asciichr",
    "strf:revstart",
    "strf:len",
    "strf:norm",
    "strf:hash",
    "strf:err",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_stringfunc_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("stringfunc");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire(shape);
    match shape {
        "strf:substr" => gen_substr(g),
        "strf:overlay" => gen_overlay(g),
        "strf:pos" => gen_pos(g),
        "strf:leftright" => gen_leftright(g),
        "strf:pad" => gen_pad(g),
        "strf:trim" => gen_trim(g),
        "strf:reprep" => gen_reprep(g),
        "strf:translate" => gen_translate(g),
        "strf:split" => gen_split(g),
        "strf:concat" => gen_concat(g),
        "strf:format" => gen_format(g),
        "strf:quote" => gen_quote(g),
        "strf:case" => gen_case(g),
        "strf:asciichr" => gen_asciichr(g),
        "strf:revstart" => gen_revstart(g),
        "strf:len" => gen_len(g),
        "strf:norm" => gen_norm(g),
        "strf:hash" => gen_hash(g),
        "strf:err" => gen_err(g),
        other => unreachable!("unknown stringfunc shape {other}"),
    }
}

// ---- shared pools -------------------------------------------------------

/// General text inputs: empty, ASCII, whitespace runs, multibyte UTF-8
/// (accented, CJK, emoji), and a NUL-adjacent low control byte. Every entry
/// is paren-balanced (the module test counts raw '(' vs ')').
const STRS: &[&str] = &[
    "",
    "abc",
    "Hello, World",
    "  spaced  ",
    "\ttab\tsep",
    "aXbXcX",
    "abcabcabc",
    "MiXeD cAsE",
    "über älter",
    "café",
    "ﬁ ligature",
    "²³ super",
    "日本語だ",
    "北京市",
    "emoji 😀 mix",
    "e\u{0301}decomposed",
    "e\u{0301}\u{0301}double",
    "line one two",
    "a.b.c",
    "one,two,,three",
    "\u{0001}ctrl",
];

/// Single-character / small class strings for trim/translate set arguments.
const SETS: &[&str] = &["x", "xy", "xyz", " ", "ab", "だ", "él ", "", "0", ",-"];

/// Numeric position/offset arguments (negative, zero, boundary, small).
const OFFS: &[i64] = &[-3, -1, 0, 1, 2, 3, 4, 6, 10];

/// Non-negative length arguments for pad/for (kept small; the "too large"
/// allocation arm lives in strf:err).
const LENS: &[u64] = &[0, 1, 2, 3, 5, 8, 20, 100];

fn esc(s: &str) -> String {
    s.replace('\'', "''")
}

/// A quoted, escaped text literal.
fn q(s: &str) -> String {
    format!("'{}'", esc(s))
}

fn raw(sql: String) -> Vec<StmtKind> {
    vec![StmtKind::Raw(sql)]
}

fn pick_str(g: &mut Gen) -> String {
    g.rng.pick(STRS).to_string()
}

fn pick_off(g: &mut Gen) -> i64 {
    *g.rng.pick(OFFS)
}

fn pick_len(g: &mut Gen) -> u64 {
    *g.rng.pick(LENS)
}

// ---- shape generators ---------------------------------------------------

/// substr / substring, both the function-call and SQL `FROM..FOR` forms,
/// with negative/zero/huge-ish start and optional length.
fn gen_substr(g: &mut Gen) -> Vec<StmtKind> {
    let s = pick_str(g);
    let from = pick_off(g);
    let sql = match g.rng.below(4) {
        0 => format!("SELECT substr({}, {})::text;", q(&s), from),
        1 => {
            let len = pick_len(g);
            format!("SELECT substr({}, {}, {})::text;", q(&s), from, len)
        }
        2 => format!("SELECT substring({} from {})::text;", q(&s), from),
        _ => {
            let len = pick_len(g);
            format!("SELECT substring({} from {} for {})::text;", q(&s), from, len)
        }
    };
    raw(sql)
}

/// overlay(s placing r from y [for z]) over text.
fn gen_overlay(g: &mut Gen) -> Vec<StmtKind> {
    let s = pick_str(g);
    let r = pick_str(g);
    let from = 1 + g.rng.below(6) as i64;
    let sql = if g.rng.chance(1, 2) {
        format!("SELECT overlay({} placing {} from {})::text;", q(&s), q(&r), from)
    } else {
        let len = pick_len(g);
        format!(
            "SELECT overlay({} placing {} from {} for {})::text;",
            q(&s),
            q(&r),
            from,
            len
        )
    };
    raw(sql)
}

/// position(sub in s) and strpos(s, sub).
fn gen_pos(g: &mut Gen) -> Vec<StmtKind> {
    let s = pick_str(g);
    let sub = if g.rng.chance(1, 3) { pick_str(g) } else { g.rng.pick(SETS).to_string() };
    let sql = if g.rng.chance(1, 2) {
        format!("SELECT position({} in {});", q(&sub), q(&s))
    } else {
        format!("SELECT strpos({}, {});", q(&s), q(&sub))
    };
    raw(sql)
}

/// left/right with positive, negative (from-end), zero and large counts.
fn gen_leftright(g: &mut Gen) -> Vec<StmtKind> {
    let s = pick_str(g);
    let n = pick_off(g);
    let f = if g.rng.chance(1, 2) { "left" } else { "right" };
    raw(format!("SELECT {}({}, {})::text;", f, q(&s), n))
}

/// lpad/rpad with default and explicit fill (incl. multibyte), and
/// negative/zero/truncating lengths.
fn gen_pad(g: &mut Gen) -> Vec<StmtKind> {
    let s = pick_str(g);
    let len = *g.rng.pick(&[-3i64, 0, 1, 2, 3, 5, 8, 20]);
    let f = if g.rng.chance(1, 2) { "lpad" } else { "rpad" };
    let sql = if g.rng.chance(1, 2) {
        format!("SELECT {}({}, {})::text;", f, q(&s), len)
    } else {
        let fill = g.rng.pick(SETS).to_string();
        format!("SELECT {}({}, {}, {})::text;", f, q(&s), len, q(&fill))
    };
    raw(sql)
}

/// ltrim/rtrim/btrim plus the SQL trim(LEADING|TRAILING|BOTH set FROM s)
/// grammar and the default-whitespace forms.
fn gen_trim(g: &mut Gen) -> Vec<StmtKind> {
    let s = pick_str(g);
    let set = g.rng.pick(SETS).to_string();
    let sql = match g.rng.below(6) {
        0 => format!("SELECT ltrim({}, {})::text;", q(&s), q(&set)),
        1 => format!("SELECT rtrim({}, {})::text;", q(&s), q(&set)),
        2 => format!("SELECT btrim({}, {})::text;", q(&s), q(&set)),
        3 => format!("SELECT btrim({})::text, ltrim({})::text, rtrim({})::text;", q(&s), q(&s), q(&s)),
        4 => {
            let dir = *g.rng.pick(&["LEADING", "TRAILING", "BOTH"]);
            format!("SELECT trim({} {} FROM {})::text;", dir, q(&set), q(&s))
        }
        _ => format!("SELECT trim(BOTH FROM {})::text, trim({})::text;", q(&s), q(&s)),
    };
    raw(sql)
}

/// repeat + replace.
fn gen_reprep(g: &mut Gen) -> Vec<StmtKind> {
    let s = pick_str(g);
    let sql = if g.rng.chance(1, 2) {
        let n = *g.rng.pick(&[-1i64, 0, 1, 2, 3, 5]);
        format!("SELECT repeat({}, {})::text;", q(&s), n)
    } else {
        let from = g.rng.pick(SETS).to_string();
        let to = g.rng.pick(SETS).to_string();
        format!("SELECT replace({}, {}, {})::text;", q(&s), q(&from), q(&to))
    };
    raw(sql)
}

/// translate(s, from, to) incl. to shorter than from (deletion) and
/// multibyte class members.
fn gen_translate(g: &mut Gen) -> Vec<StmtKind> {
    let s = pick_str(g);
    let from = g.rng.pick(SETS).to_string();
    let to = g.rng.pick(SETS).to_string();
    raw(format!("SELECT translate({}, {}, {})::text;", q(&s), q(&from), q(&to)))
}

/// split_part (incl. negative field, PG14+), string_to_array and
/// string_to_table with the optional null-string argument.
fn gen_split(g: &mut Gen) -> Vec<StmtKind> {
    let s = g.rng.pick(&["a,b,c", "one,two,,three", "x", "", "a||b||c", "日,本,語"]).to_string();
    let delim = g.rng.pick(&[",", "||", "", "X", "，"]).to_string();
    let sql = match g.rng.below(4) {
        0 => {
            let field = *g.rng.pick(&[-2i64, -1, 0, 1, 2, 3, 9]);
            format!("SELECT split_part({}, {}, {});", q(&s), q(&delim), field)
        }
        1 => format!("SELECT string_to_array({}, {})::text;", q(&s), q(&delim)),
        2 => {
            let ns = g.rng.pick(&["two", "", "a"]).to_string();
            format!("SELECT string_to_array({}, {}, {})::text;", q(&s), q(&delim), q(&ns))
        }
        _ => format!("SELECT x FROM string_to_table({}, {}) t(x);", q(&s), q(&delim)),
    };
    raw(sql)
}

/// concat / concat_ws with NULL arguments (NULLs are skipped, not
/// stringified).
fn gen_concat(g: &mut Gen) -> Vec<StmtKind> {
    let a = pick_str(g);
    let b = pick_str(g);
    let sql = if g.rng.chance(1, 2) {
        format!("SELECT concat({}, NULL, {}, {})::text;", q(&a), q(&b), g.rng.below(100))
    } else {
        let sep = g.rng.pick(&[", ", "", "-", "だ"]).to_string();
        format!("SELECT concat_ws({}, {}, NULL, {}, {})::text;", q(&sep), q(&a), q(&b), g.rng.below(100))
    };
    raw(sql)
}

/// format() with %s/%I/%L, positional $n, and field width / left-justify
/// flags. Arguments include NULL (%s->"", %I/%L->quoted null forms).
fn gen_format(g: &mut Gen) -> Vec<StmtKind> {
    let a = pick_str(g);
    let b = g.rng.pick(&["col name", "Weird\"Id", "public", "x"]).to_string();
    let sql = match g.rng.below(5) {
        0 => format!("SELECT format('%s | %s', {}, NULL)::text;", q(&a)),
        1 => format!("SELECT format('%I = %L', {}, {})::text;", q(&b), q(&a)),
        2 => format!("SELECT format('%2$s-%1$s', {}, {})::text;", q(&a), q(&b)),
        3 => format!("SELECT format('[%10s][%-10s]', {}, {})::text;", q(&a), q(&a)),
        _ => format!(
            "SELECT format('%1$s/%1$I/%1$L', {})::text, format('%L', NULL::text)::text;",
            q(&b)
        ),
    };
    raw(sql)
}

/// quote_ident / quote_literal / quote_nullable, incl. embedded quotes,
/// backslashes and NULL.
fn gen_quote(g: &mut Gen) -> Vec<StmtKind> {
    let s = g.rng.pick(&[
        "simple", "Weird Id", "has\"quote", "has'apos", "with\\back", "SELECT", "café", "",
    ]).to_string();
    let sql = match g.rng.below(4) {
        0 => format!("SELECT quote_ident({})::text;", q(&s)),
        1 => format!("SELECT quote_literal({})::text;", q(&s)),
        2 => format!("SELECT quote_nullable({})::text;", q(&s)),
        _ => "SELECT quote_literal(NULL::text), quote_nullable(NULL::text);".to_string(),
    };
    raw(sql)
}

/// upper / lower / initcap, argument pinned COLLATE "C" for locale-
/// independent case mapping (see module docs).
fn gen_case(g: &mut Gen) -> Vec<StmtKind> {
    let s = pick_str(g);
    let arg = format!("{} COLLATE \"C\"", q(&s));
    let sql = match g.rng.below(4) {
        0 => format!("SELECT upper({})::text;", arg),
        1 => format!("SELECT lower({})::text;", arg),
        2 => format!("SELECT initcap({})::text;", arg),
        _ => format!(
            "SELECT upper({a})::text, lower({a})::text, initcap({a})::text;",
            a = arg
        ),
    };
    raw(sql)
}

/// ascii / chr / to_hex.
fn gen_asciichr(g: &mut Gen) -> Vec<StmtKind> {
    let sql = match g.rng.below(3) {
        0 => {
            let s = pick_str(g);
            format!("SELECT ascii({});", q(&s))
        }
        1 => {
            let c = *g.rng.pick(&[1i64, 32, 65, 66, 233, 946, 955, 26085, 128512]);
            format!("SELECT chr({})::text;", c)
        }
        _ => {
            let v = g.rng.range_i64(-2000000, 2000000);
            format!("SELECT to_hex({}), to_hex({}::bigint);", v, v)
        }
    };
    raw(sql)
}

/// reverse + starts_with.
fn gen_revstart(g: &mut Gen) -> Vec<StmtKind> {
    let s = pick_str(g);
    let sql = if g.rng.chance(1, 2) {
        format!("SELECT reverse({})::text;", q(&s))
    } else {
        let pre = g.rng.pick(SETS).to_string();
        format!("SELECT starts_with({}, {});", q(&s), q(&pre))
    };
    raw(sql)
}

/// char_length / length / octet_length / bit_length — byte vs character
/// counts diverge on the multibyte inputs.
fn gen_len(g: &mut Gen) -> Vec<StmtKind> {
    let s = pick_str(g);
    raw(format!(
        "SELECT char_length({a}), length({a}), octet_length({a}), bit_length({a});",
        a = q(&s)
    ))
}

/// normalize + IS [NOT] NORMALIZED across all four forms, plus unistr
/// (PG16). Combining/compatibility inputs make the forms diverge.
fn gen_norm(g: &mut Gen) -> Vec<StmtKind> {
    // Precomposed vs decomposed and compatibility characters.
    let s = g.rng.pick(&[
        "e\u{0301}",       // e + combining acute
        "é",               // precomposed
        "ﬁ",               // compatibility ligature (NFKC/NFKD split it)
        "²",               // superscript two
        "Å",               // angstrom-ish precomposed
        "A\u{030A}",       // A + combining ring
        "①",               // circled digit
        "ẛ",               // long s with dot
    ]).to_string();
    let form = *g.rng.pick(&["NFC", "NFD", "NFKC", "NFKD"]);
    let sql = match g.rng.below(4) {
        0 => format!("SELECT normalize({}, {})::text;", q(&s), form),
        1 => format!("SELECT normalize({})::text;", q(&s)),
        2 => format!("SELECT {} IS {} NORMALIZED;", q(&s), form),
        _ => format!(
            "SELECT unistr('d\\0061t\\+000061')::text, unistr('\\00e9')::text, unistr({})::text;",
            q(&s)
        ),
    };
    raw(sql)
}

/// md5 / sha224 / sha256 / sha384 / sha512 / hashtext. Text is cast to
/// bytea for the sha family; sha output (bytea) is cast ::text (\x-hex).
fn gen_hash(g: &mut Gen) -> Vec<StmtKind> {
    let s = pick_str(g);
    let sql = match g.rng.below(4) {
        0 => format!("SELECT md5({});", q(&s)),
        1 => format!("SELECT sha256({}::bytea)::text, sha224({}::bytea)::text;", q(&s), q(&s)),
        2 => format!("SELECT sha384({}::bytea)::text, sha512({}::bytea)::text;", q(&s), q(&s)),
        _ => format!("SELECT hashtext({}), md5({}::bytea);", q(&s), q(&s)),
    };
    raw(sql)
}

/// Deliberate matched-error fuel (SQLSTATE identity): null character,
/// negative substring length, over-large allocation, out-of-range chr.
fn gen_err(g: &mut Gen) -> Vec<StmtKind> {
    let sql = match g.rng.below(5) {
        0 => "SELECT chr(0);",
        1 => "SELECT substring('abcdef' from 2 for -1);",
        2 => "SELECT lpad('x', 1000000000);",
        3 => "SELECT repeat('abc', 1000000000);",
        _ => "SELECT chr(1114112);",
    };
    raw(sql.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Every shape produces a single-line, terminated, paren-balanced
    /// statement over 2000 seeds, and every SHAPES name is a registered
    /// production weight (else weights::get would debug_assert).
    #[test]
    fn shapes_well_formed() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0x57121);
        for _ in 0..2000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmts = gen_stringfunc_module(&mut g);
            assert!(!stmts.is_empty());
            for stmt in &stmts {
                let sql = stmt.to_sql();
                assert!(!sql.contains('\n'), "multi-line: {sql}");
                assert!(sql.ends_with(';'), "unterminated: {sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
            }
        }
    }

    /// Same seed reproduces a byte-identical group.
    #[test]
    fn seed_deterministic() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let sql = |seed| {
            let mut rng = Rng::new(seed);
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            gen_stringfunc_module(&mut g)
                .iter()
                .map(|s| s.to_sql())
                .collect::<Vec<_>>()
        };
        assert_eq!(sql(123), sql(123));
    }

    /// Every shape is reachable under defaults within a bounded budget.
    #[test]
    fn all_shapes_reachable() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(9);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..20000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let _ = gen_stringfunc_module(&mut g);
            for p in prods {
                if let Some(s) = p.strip_prefix("strf:") {
                    seen.insert(s.to_string());
                }
            }
        }
        for shape in SHAPES {
            let name = shape.strip_prefix("strf:").unwrap();
            assert!(seen.contains(name), "shape {shape} never fired");
        }
    }
}
