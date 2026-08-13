//! LIKE / ILIKE / SIMILAR TO pattern-match drain: literal-only scalar
//! SELECTs (plus one VALUES-driven non-constant-pattern shape) over the
//! `%`/`_` wildcard, ESCAPE-clause and escaped-literal surface — the adt
//! target crates/backend/utils/adt/like (like.c / like_match.c ported:
//! match_text/generic_match_text/generic_text_ic_like and the
//! do_like_escape/like_escape_into escape machinery).
//!
//! Distinct from the regexp_* surface the REGEX lane drained: the SIMILAR
//! TO / substring(... SIMILAR ...) productions here are included for
//! completeness (they compile to the regex engine via similar_to_escape,
//! a separate crate) but the emphasis and the hollow-line target is the
//! LIKE/ILIKE operator family and its escape processing.
//!
//! Emitted as `StmtKind::Raw`: every operand is a fixed literal drawn from
//! the seeded pools below — no now()/random()/current_* — so the same seed
//! reproduces a byte-identical stream and both differential sides evaluate
//! identical text. Standard_conforming_strings is on (the pinned session
//! default), so a backslash inside a single-quoted literal is a literal
//! backslash: `'a\%b'` is the four bytes a,\,%,b, which LIKE reads as an
//! escaped `%`. Every subject/pattern is single-quote-safe (see `q`).
//!
//! Result identity is a scalar bool (single row, order-independent) for the
//! literal shapes; the `like:col` shape drives the non-constant-pattern
//! planner path over an inline VALUES list with a *total* ORDER BY so the
//! row set compares order-deterministically. Error identity is exercised
//! too: multi-character ESCAPE -> 22019 (invalid escape string), a pattern
//! that ends with the escape character -> 22025-class invalid-escape.
//!
//! ILIKE is pinned to `COLLATE "C"` so the case-fold path is the
//! deterministic ASCII lower_into arm (like_match.c SbIc / the C-ctype
//! UTF-8 arm). The non-C ICU str_tolower tail of generic_text_ic_like
//! (case-folding non-ASCII, and the nondeterministic-collation reject in
//! match_text) needs a pinned ICU/nondeterministic collation and is left
//! to a collation-aware follow-up — see findings-like.md.
//!
//! Productions (weights in weights.rs, all `like:` prefixed):
//!   basic   LIKE / NOT LIKE / ~~ / !~~ over ascii subjects+patterns
//!   ilike   ILIKE / NOT ILIKE / ~~* / !~~* (COLLATE "C")
//!   escape  LIKE ... ESCAPE 'x' — custom char, empty '' (no-escape), and
//!           backslash '\' (the passthrough branch of do_like_escape)
//!   esclit  escaped-literal patterns (\%, \_, \\) without an ESCAPE clause
//!   mb      multibyte UTF-8 subjects+patterns (wildcards span whole chars)
//!   bytea   bytea LIKE / NOT LIKE (the SbCs byte-lockstep path)
//!   similar SIMILAR TO / NOT SIMILAR TO (+ ESCAPE) — regex surface
//!   substr  substring(text SIMILAR pattern ESCAPE e) — SQL:2003 form
//!   col     non-constant pattern: `s LIKE p` over inline VALUES, ORDER BY
//!   err     deliberate error fuel: multichar ESCAPE (22019), trailing
//!           escape (invalid-escape), all-wildcard / empty adversaries

use crate::stmt::{Gen, StmtKind};

/// Single-quote-safe rendering: double embedded quotes. Backslashes are
/// left as-is (standard_conforming_strings=on), so an escaped-literal
/// pattern like `a\%b` reaches LIKE with its backslash intact.
fn q(s: &str) -> String {
    s.replace('\'', "''")
}

/// ASCII subjects: empty, single char, repeats (so `%x%` backtracks),
/// literal wildcards in the *data* (100%, a_b), an embedded backslash and
/// an embedded quote, mixed case (for the ILIKE fold).
const SUBJECTS: &[&str] = &[
    "",
    "a",
    "abc",
    "abcabc",
    "aXbXc",
    "100%",
    "a_b",
    "a%b",
    "a\\b",
    "Hello World",
    "AaBbCc",
    "___",
    "%%%",
    "don't",
    "  spaced  ",
];

/// ASCII patterns: bare wildcards, anchored/floating wildcards, single-char
/// `_`, escaped literals (\%,\_,\\), empty, double-`%` (the collapse arm),
/// and a pattern whose only content is a trailing-escape-free backslash
/// run. The trailing-escape *error* pattern is emitted by `gen_err`.
const PATTERNS: &[&str] = &[
    "%",
    "_",
    "a%",
    "%c",
    "%b%",
    "a_c",
    "___",
    "abc",
    "",
    "%%",
    "a%%c",
    "a\\%b",
    "a\\_b",
    "\\\\",
    "%a%b%",
    "_%_",
    "100\\%",
];

/// Multibyte UTF-8 subjects (2/3/4-byte code points, combining forms).
const MB_SUBJECTS: &[&str] = &[
    "café",
    "naïve",
    "Ñandú",
    "Ωμέγα",
    "日本語",
    "Grüße",
    "😀x😀y",
    "ＡｂＣ",
];

/// Multibyte patterns: `_` must advance one whole character, `%` spans
/// runs of multibyte characters.
const MB_PATTERNS: &[&str] = &[
    "%é%",
    "_a_",
    "café",
    "caf_",
    "日%",
    "%語",
    "Ω_έγα",
    "😀%😀_",
    "%",
    "____",
];

/// bytea subjects/patterns as text cast to bytea: the UTF-8 bytes flow
/// through match_text::<SbCs>. `%`/`_` are their ASCII bytes 0x25/0x5f
/// (wildcards); a doubled backslash reaches the pattern as one byte and
/// escapes the next (byteain reads `\\` as a single 0x5c).
const BYTEA_SUBJECTS: &[&str] = &["", "abc", "a\\x62c", "aXbc", "hello"];
const BYTEA_PATTERNS: &[&str] = &["%", "a%", "a_c", "%b%", "abc", "a\\\\%c"];

/// SIMILAR TO patterns (SQL-regex dialect): alternation, quantifiers,
/// bracket classes, the `#"..."#` substring markers, and `%`/`_` (which
/// SIMILAR maps to `.*`/`.`).
const SIMILAR_PATTERNS: &[&str] = &[
    "%",
    "(a|b)%",
    "a_c",
    "%[bc]%",
    "(ab)*c+",
    "a{2,3}",
    "%#\"c#\"%",
    "a|bc|def",
    "[[:alpha:]]+",
    "abc",
];

/// Custom ESCAPE characters: a printable non-special char, `#` (the SQL
/// standard convention), backslash (the do_like_escape passthrough branch),
/// and the empty string (no-escape: doubles backslashes).
const ESCAPES: &[&str] = &["#", "!", "\\", ""];

/// LIKE operator family: keyword forms and their operator spellings, both
/// affirmative and negated (textlike/textnlike + the ~~/!~~ operators).
const LIKE_OPS: &[&str] = &["LIKE", "NOT LIKE", "~~", "!~~"];
const ILIKE_OPS: &[&str] = &["ILIKE", "NOT ILIKE", "~~*", "!~~*"];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_like_module(g: &mut Gen) -> Vec<StmtKind> {
    let pick = g.weights.pick(
        g.rng,
        &[
            "like:basic",
            "like:ilike",
            "like:escape",
            "like:esclit",
            "like:mb",
            "like:bytea",
            "like:similar",
            "like:substr",
            "like:col",
            "like:err",
        ],
    );
    let sql = match pick {
        "like:basic" => gen_basic(g),
        "like:ilike" => gen_ilike(g),
        "like:escape" => gen_escape(g),
        "like:esclit" => gen_esclit(g),
        "like:mb" => gen_mb(g),
        "like:bytea" => gen_bytea(g),
        "like:similar" => gen_similar(g),
        "like:substr" => gen_substr(g),
        "like:col" => return gen_col(g),
        _ => gen_err(g),
    };
    vec![StmtKind::Raw(sql)]
}

fn gen_basic(g: &mut Gen) -> String {
    g.fire("like:basic");
    let op = *g.rng.pick(LIKE_OPS);
    let s = q(g.rng.pick(SUBJECTS));
    let p = q(g.rng.pick(PATTERNS));
    format!("SELECT '{s}' {op} '{p}';")
}

fn gen_ilike(g: &mut Gen) -> String {
    g.fire("like:ilike");
    let op = *g.rng.pick(ILIKE_OPS);
    let s = q(g.rng.pick(SUBJECTS));
    let p = q(g.rng.pick(PATTERNS));
    // Pin the case-fold to the deterministic ASCII arm.
    format!("SELECT '{s}' COLLATE \"C\" {op} '{p}';")
}

fn gen_escape(g: &mut Gen) -> String {
    g.fire("like:escape");
    let op = if g.rng.chance(1, 4) { "NOT LIKE" } else { "LIKE" };
    let s = q(g.rng.pick(SUBJECTS));
    let esc = *g.rng.pick(ESCAPES);
    // Choose a pattern whose escape marker matches the chosen ESCAPE char so
    // the escaped-literal branch of do_like_escape is exercised. For `#`/`!`
    // the marker is that char; for `\` and `` the standard patterns already
    // carry backslashes.
    let p = match esc {
        "#" => q(g.rng.pick(&["a#%b", "#_x", "100#%", "a%#_c"])),
        "!" => q(g.rng.pick(&["a!%b", "!_x", "100!%"])),
        _ => q(g.rng.pick(PATTERNS)),
    };
    format!("SELECT '{s}' {op} '{p}' ESCAPE '{}';", q(esc))
}

fn gen_esclit(g: &mut Gen) -> String {
    g.fire("like:esclit");
    let op = *g.rng.pick(LIKE_OPS);
    // Subjects that contain the literal wildcard/backslash bytes so the
    // escaped-literal pattern can actually match.
    let s = q(g.rng.pick(&["a%b", "a_b", "a\\b", "100%", "a_b_c", "\\"]));
    let p = q(g.rng.pick(&["a\\%b", "a\\_b", "\\\\", "100\\%", "a\\_b\\_c", "%\\\\%"]));
    format!("SELECT '{s}' {op} '{p}';")
}

fn gen_mb(g: &mut Gen) -> String {
    g.fire("like:mb");
    let ic = g.rng.chance(1, 3);
    let s = q(g.rng.pick(MB_SUBJECTS));
    let p = q(g.rng.pick(MB_PATTERNS));
    if ic {
        // ILIKE over multibyte, still COLLATE "C" (ASCII-only fold; the
        // non-ASCII code points compare byte-exact — the C-ctype UTF-8 arm).
        let op = *g.rng.pick(&["ILIKE", "NOT ILIKE"]);
        format!("SELECT '{s}' COLLATE \"C\" {op} '{p}';")
    } else {
        let op = *g.rng.pick(LIKE_OPS);
        format!("SELECT '{s}' {op} '{p}';")
    }
}

fn gen_bytea(g: &mut Gen) -> String {
    g.fire("like:bytea");
    let op = *g.rng.pick(&["LIKE", "NOT LIKE", "~~", "!~~"]);
    let s = q(g.rng.pick(BYTEA_SUBJECTS));
    let p = q(g.rng.pick(BYTEA_PATTERNS));
    format!("SELECT '{s}'::bytea {op} '{p}'::bytea;")
}

fn gen_similar(g: &mut Gen) -> String {
    g.fire("like:similar");
    let op = if g.rng.chance(1, 3) { "NOT SIMILAR TO" } else { "SIMILAR TO" };
    let s = q(g.rng.pick(SUBJECTS));
    let p = q(g.rng.pick(SIMILAR_PATTERNS));
    if g.rng.chance(1, 3) {
        format!("SELECT '{s}' {op} '{p}' ESCAPE '#';")
    } else {
        format!("SELECT '{s}' {op} '{p}';")
    }
}

fn gen_substr(g: &mut Gen) -> String {
    g.fire("like:substr");
    let s = q(g.rng.pick(&["abcdef", "xabcx", "foo123bar", "café-99", ""]));
    // SIMILAR-substring patterns carry the `#"..."#` capture markers.
    let p = q(g.rng.pick(&["%#\"cd#\"%", "x#\"abc#\"x", "%#\"[0-9]+#\"%", "%#\"_#\"%"]));
    format!("SELECT substring('{s}' SIMILAR '{p}' ESCAPE '#');")
}

fn gen_col(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("like:col");
    // Non-constant pattern path: both operands come from an inline VALUES
    // list, so the planner cannot const-fold the pattern. A total ORDER BY
    // over both output columns makes the row set order-deterministic.
    let op = *g.rng.pick(&["LIKE", "NOT LIKE", "ILIKE"]);
    let rows: &[(&str, &str)] = &[
        ("abc", "a%"),
        ("a_b", "a\\_b"),
        ("100%", "100\\%"),
        ("café", "caf_"),
        ("", "%"),
        ("XYZ", "x%"),
    ];
    let mut vals = String::new();
    for (i, (s, p)) in rows.iter().enumerate() {
        if i > 0 {
            vals.push_str(", ");
        }
        vals.push_str(&format!("('{}', '{}')", q(s), q(p)));
    }
    let coll = if op == "ILIKE" { " COLLATE \"C\"" } else { "" };
    let sql = format!(
        "SELECT v.s, (v.s{coll} {op} v.p) AS m FROM (VALUES {vals}) AS v(s, p) ORDER BY v.s, m;"
    );
    vec![StmtKind::Raw(sql)]
}

fn gen_err(g: &mut Gen) -> String {
    g.fire("like:err");
    let s = q(g.rng.pick(SUBJECTS));
    match g.rng.below(4) {
        // Multi-character ESCAPE -> 22019 invalid escape string.
        0 => {
            let e = *g.rng.pick(&["xy", "##", "ab"]);
            format!("SELECT '{s}' LIKE 'a%' ESCAPE '{e}';")
        }
        // Pattern ends with the (default backslash) escape character.
        1 => format!("SELECT '{s}' LIKE 'abc\\';"),
        // Pattern ends with a custom escape character.
        2 => format!("SELECT '{s}' LIKE 'abc#' ESCAPE '#';"),
        // Adversarial-but-legal: all-wildcard and empty edges (no error,
        // exercises the fast `%`-only and empty-pattern arms).
        _ => {
            let p = *g.rng.pick(&["%", "%%%%", "", "_", "%_%"]);
            format!("SELECT '{s}' LIKE '{}';", q(p))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Textual invariants for every like production: single line,
    /// ';'-terminated, balanced parens, single-quote balance (even count of
    /// unescaped quotes), and no nondeterministic constructs.
    #[test]
    fn like_statements_are_deterministic_shapes() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0x11ce);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..6000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmts = gen_like_module(&mut g);
            assert_eq!(stmts.len(), 1, "like emits one statement per pick");
            for stmt in &stmts {
                let sql = stmt.to_sql();
                assert!(!sql.contains('\n'), "multi-line: {sql}");
                assert!(sql.ends_with(';'), "unterminated: {sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
                // Every single quote is either an opener/closer or a doubled
                // literal quote; the total count is therefore even.
                assert_eq!(sql.matches('\'').count() % 2, 0, "unbalanced quotes: {sql}");
                let low = sql.to_ascii_lowercase();
                assert!(!low.contains("now("), "nondeterministic now(): {sql}");
                assert!(!low.contains("random("), "nondeterministic random(): {sql}");
                assert!(!low.contains("current_"), "nondeterministic current_*: {sql}");
            }
            for p in &prods {
                seen.insert(p.clone());
            }
        }
        for p in [
            "like:basic", "like:ilike", "like:escape", "like:esclit",
            "like:mb", "like:bytea", "like:similar", "like:substr",
            "like:col", "like:err",
        ] {
            assert!(seen.contains(p), "production {p} never fired");
        }
    }

    /// Same seed -> byte-identical statements (the reproducibility law).
    #[test]
    fn like_is_seed_deterministic() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let run = || {
            let mut rng = Rng::new(99);
            let mut out = Vec::new();
            for _ in 0..800 {
                let mut prods = Vec::new();
                let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
                out.push(gen_like_module(&mut g)[0].to_sql());
            }
            out
        };
        assert_eq!(run(), run());
    }
}
