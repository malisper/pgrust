//! REGEX crafted-pattern drain module: the regular-expression engine
//! (backend/regex — regcomp/regexec, Spencer's hybrid DFA/NFA ARE) is a
//! SQL-reachable crash/DoS surface — any unprivileged client sends
//! `SELECT t ~ '<pattern>'` or a `regexp_*` call with an attacker-chosen
//! pattern directly. This module drives every user-facing regex entry
//! point (`regexp_match/matches/replace/count/substr/instr/like`,
//! `regexp_split_to_table/array`, and the `~`/`~*`/`!~`/`!~*` operators)
//! over deterministic small subjects with deliberately adversarial
//! patterns, plus the two engine-defense prongs:
//!
//!   (a) DIFFERENTIAL: result identity + error identity vs the pinned
//!       REL_18_3 C engine. All outputs are scalar `::text` (or SRF rows
//!       under `WITH ORDINALITY ORDER BY n`, a total order) so the wire
//!       compare is byte-exact; error probes are standalone autocommit
//!       statements so a REG_* → SQLSTATE mismatch is isolated to one
//!       statement.
//!
//!   (b) COMPLEXITY GUARD: C rejects an over-complex regex (deep nesting,
//!       huge bounded quantifiers) with a clean error — REG_ETOOBIG
//!       ("regular expression is too complex", SQLSTATE 54000) via the
//!       byte-based `stack_is_too_deep()` guard, or a real syntax error —
//!       and pgrust must reject the SAME way, never hang, stack-overflow,
//!       or abort. (The p1-deadguard release blocker was exactly a dead
//!       frame-count cap that let deep nesting overflow the thread stack;
//!       this module keeps that guard exercised.) The ETOOBIG stack-band
//!       asymmetry — pgrust's larger parse frame trips the guard at a
//!       shallower depth than clang-built C at the same 2048kB budget —
//!       is a RATIFIED environmental non-surface (see
//!       regex_core/tests/etoobig_error_priority.rs); the differential
//!       runner carves error-vs-error on those bands. We still generate
//!       the shapes: the point is that pgrust reaches a clean REG_ error,
//!       not a crash/hang.
//!
//! Disciplines: deterministic subjects, no float, no nondeterministic
//! functions, total ORDER BY on every SRF, single-quote-escaped literals,
//! no GUC mutation (max_stack_depth is left at the server default so the
//! guard band matches the ratified measurements). Catastrophic-
//! backtracking shapes `(a+)+$` / `([a-z]+)*$` over short non-matching
//! subjects are the DoS canary: Postgres' engine is not a backtracker, so
//! both sides must return promptly — a pgrust hang here is the finding.

use crate::stmt::{Gen, StmtKind};

/// Statement shapes (top-level weighted pick).
const SHAPES: &[&str] = &[
    "regex:match",
    "regex:matches",
    "regex:replace",
    "regex:count",
    "regex:substr",
    "regex:split",
    "regex:op",
    "regex:cat",
    "regex:bigquant",
    "regex:deepnest",
    "regex:backref",
    "regex:charclass",
    "regex:anchor",
    "regex:flags",
    "regex:invalid",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_regex_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("regex");
    match g.weights.pick(g.rng, SHAPES) {
        "regex:match" => gen_match(g),
        "regex:matches" => gen_matches(g),
        "regex:replace" => gen_replace(g),
        "regex:count" => gen_count(g),
        "regex:substr" => gen_substr(g),
        "regex:split" => gen_split(g),
        "regex:op" => gen_op(g),
        "regex:cat" => gen_catastrophic(g),
        "regex:bigquant" => gen_bigquant(g),
        "regex:deepnest" => gen_deepnest(g),
        "regex:backref" => gen_backref(g),
        "regex:charclass" => gen_charclass(g),
        "regex:anchor" => gen_anchor(g),
        "regex:flags" => gen_flags(g),
        "regex:invalid" => gen_invalid(g),
        other => unreachable!("unknown regex shape {other}"),
    }
}

/// One-knob error-fuel bias: err arms host deliberate matched errors,
/// biased low per the findings-budget rule (matched both-sides errors are
/// low-value once the SQLSTATE agrees).
fn err_arm(g: &mut Gen) -> bool {
    if g.weights.pick(g.rng, &["regex:ok", "regex:err"]) == "regex:err" {
        g.fire("regex:err");
        true
    } else {
        false
    }
}

fn esc(s: &str) -> String {
    // Standard-conforming strings: backslash is literal, only quotes double.
    s.replace('\'', "''")
}

fn raw(sql: String) -> Vec<StmtKind> {
    vec![StmtKind::Raw(sql)]
}

/// Render a pattern as a SQL text expression whose SOURCE text is always
/// paren-balanced (a structural invariant the fuzzgen harness enforces on
/// every generated statement). Patterns with balanced parens stay plain
/// literals; a pattern with UNBALANCED parens (the malformed compile-error
/// probes) has each paren emitted as `chr(40)`/`chr(41)` so no literal
/// paren char appears in the source — the runtime pattern VALUE is
/// unchanged, so the engine still sees the malformed regex.
fn pat_expr(pat: &str) -> String {
    let opens = pat.matches('(').count();
    let closes = pat.matches(')').count();
    if opens == closes {
        return format!("'{}'", esc(pat));
    }
    let mut parts: Vec<String> = Vec::new();
    let mut buf = String::new();
    for c in pat.chars() {
        if c == '(' || c == ')' {
            if !buf.is_empty() {
                parts.push(format!("'{}'", esc(&buf)));
                buf.clear();
            }
            parts.push(format!("chr({})", c as u32));
        } else {
            buf.push(c);
        }
    }
    if !buf.is_empty() {
        parts.push(format!("'{}'", esc(&buf)));
    }
    if parts.is_empty() {
        return "''".to_string();
    }
    parts.join(" || ")
}

/// Deterministic small subjects. No newline literals in the SQL text;
/// the newline-sensitive flag arms use chr(10) forms instead.
const SUBJECTS: &[&str] = &[
    "abcabc",
    "aaaa",
    "aXbYcZ",
    "the quick brown fox",
    "foo123bar456",
    "",
    "a.b.c.d",
    "AaBbCc",
    "  spaced  words  ",
    "hello-world_42",
];

fn subject<'a>(g: &mut Gen) -> &'a str {
    SUBJECTS[g.rng.below_usize(SUBJECTS.len())]
}

/// A subject rendered as a SQL literal, sometimes with an embedded newline
/// (via `E'..\n..'`) so the m/n/p newline flags have something to bite on.
fn subject_lit(g: &mut Gen) -> String {
    if g.rng.chance(1, 5) {
        // Escape-string form with a real newline between two words.
        format!("E'{}\\n{}'", esc(subject(g)), esc(subject(g)))
    } else {
        format!("'{}'", esc(subject(g)))
    }
}

/// Everyday well-formed ARE patterns (the differential result-identity
/// corpus): metacharacters, alternation, bounded quantifiers, classes.
const OK_PATS: &[&str] = &[
    "a.c",
    "a+b*",
    "(ab)+",
    "a{2,4}",
    "[a-z]+",
    "[^0-9]",
    "foo|bar",
    "\\w+",
    "\\d{2,3}",
    "\\s",
    "^a",
    "c$",
    ".",
    "(a|b|c)*",
    "[[:alpha:]]+",
    "x?y?z?",
    "a.*d",
    "[0-9]+[a-z]+",
];

fn ok_pat<'a>(g: &mut Gen) -> &'a str {
    OK_PATS[g.rng.below_usize(OK_PATS.len())]
}

/// Flag strings for the flags argument of the regexp_* functions.
const FLAGS: &[&str] = &["", "i", "g", "n", "p", "w", "x", "s", "m", "gi", "in", "ig", "c"];

fn flag<'a>(g: &mut Gen) -> &'a str {
    FLAGS[g.rng.below_usize(FLAGS.len())]
}

// -------------------------------------------------------- result identity --

/// regexp_match: returns text[] (NULL when no match) — cast ::text for a
/// byte-exact scalar compare.
fn gen_match(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("regex:match");
    let s = subject_lit(g);
    let p = ok_pat(g);
    let sql = if g.rng.chance(1, 2) {
        format!("SELECT regexp_match({}, '{}')::text;", s, esc(p))
    } else {
        format!("SELECT regexp_match({}, '{}', '{}')::text;", s, esc(p), flag(g))
    };
    raw(sql)
}

/// regexp_matches SRF: each row is a text[]; total order via WITH
/// ORDINALITY so the ordered compare is deterministic even under 'g'.
fn gen_matches(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("regex:matches");
    let s = subject_lit(g);
    let p = ok_pat(g);
    let f = if g.rng.chance(2, 3) { "g" } else { flag(g) };
    raw(format!(
        "SELECT m::text FROM regexp_matches({}, '{}', '{}') WITH ORDINALITY AS x(m, n) ORDER BY n;",
        s,
        esc(p),
        f
    ))
}

/// regexp_replace: replacement text may carry `\1..\9` back-substitutions
/// and `\&`; 'g' replaces all.
fn gen_replace(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("regex:replace");
    let s = subject_lit(g);
    let (p, repl): (&str, &str) = *g.rng.pick(&[
        ("(a)(b)", "\\2\\1"),
        ("[aeiou]", "_"),
        ("(\\w)(\\w)", "\\2\\1"),
        ("(.)", "[\\1]"),
        ("\\s+", " "),
        ("(foo)", "<\\1>"),
        ("(a+)", "\\&\\&"),
        ("([a-z])([0-9])", "\\2\\1"),
    ]);
    let sql = match g.rng.below(3) {
        0 => format!("SELECT regexp_replace({}, '{}', '{}');", s, esc(p), esc(repl)),
        1 => format!("SELECT regexp_replace({}, '{}', '{}', 'g');", s, esc(p), esc(repl)),
        _ => format!(
            "SELECT regexp_replace({}, '{}', '{}', {}, {}, '{}');",
            s,
            esc(p),
            esc(repl),
            1 + g.rng.below(3),
            g.rng.below(3),
            flag(g)
        ),
    };
    raw(sql)
}

fn gen_count(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("regex:count");
    let s = subject_lit(g);
    let p = ok_pat(g);
    let sql = match g.rng.below(3) {
        0 => format!("SELECT regexp_count({}, '{}');", s, esc(p)),
        1 => format!("SELECT regexp_count({}, '{}', {});", s, esc(p), 1 + g.rng.below(4)),
        _ => format!(
            "SELECT regexp_count({}, '{}', {}, '{}');",
            s,
            esc(p),
            1 + g.rng.below(4),
            flag(g)
        ),
    };
    raw(sql)
}

fn gen_substr(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("regex:substr");
    let s = subject_lit(g);
    let p = ok_pat(g);
    let sql = match g.rng.below(3) {
        0 => format!("SELECT regexp_substr({}, '{}');", s, esc(p)),
        1 => format!(
            "SELECT regexp_substr({}, '{}', {}, {});",
            s,
            esc(p),
            1 + g.rng.below(4),
            1 + g.rng.below(2)
        ),
        _ => format!(
            "SELECT regexp_instr({}, '{}', {}, {}, {}, '{}', {});",
            s,
            esc(p),
            1 + g.rng.below(4),
            1 + g.rng.below(2),
            g.rng.below(2),
            flag(g),
            g.rng.below(3)
        ),
    };
    raw(sql)
}

fn gen_split(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("regex:split");
    let s = subject_lit(g);
    let p: &str = *g.rng.pick(&["\\s+", "[.-]", "", "a", "[^a-z]+", "b*"]);
    if g.rng.chance(1, 2) {
        // SRF form: total order via ordinality.
        raw(format!(
            "SELECT w FROM regexp_split_to_table({}, '{}') WITH ORDINALITY AS x(w, n) ORDER BY n;",
            s,
            esc(p)
        ))
    } else {
        raw(format!(
            "SELECT regexp_split_to_array({}, '{}', '{}')::text;",
            s,
            esc(p),
            flag(g)
        ))
    }
}

/// The `~`/`~*`/`!~`/`!~*` operator surface (the most-reached entry point:
/// any WHERE clause), plus the LIKE-family for contrast is out of scope.
fn gen_op(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("regex:op");
    let s = subject_lit(g);
    let op: &str = *g.rng.pick(&["~", "~*", "!~", "!~*"]);
    let p = ok_pat(g);
    raw(format!("SELECT {} {} '{}';", s, op, esc(p)))
}

// ------------------------------------------------------- complexity guard --

/// Catastrophic-backtracking shapes over short NON-matching subjects: on a
/// true backtracking engine these are exponential; Postgres' hybrid engine
/// must return promptly on BOTH sides. A pgrust hang here is a HIGH finding
/// (the differential runner's per-statement timeout surfaces it).
fn gen_catastrophic(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("regex:cat");
    // Evil patterns anchored so a mismatch forces the whole search.
    let pat: &str = *g.rng.pick(&[
        "(a+)+$",
        "(a*)*$",
        "(a|a)*$",
        "([a-z]+)*$",
        "(a+)+b",
        "(.*a){15}$",
        "(a?){20}a{20}",
        "(x+x+)+y",
        "((a)*)*$",
    ]);
    // Short 'a' run + a tail that defeats the anchor → maximal work.
    let n = 12 + g.rng.below(20); // 12..=31 'a's, safely non-hanging on a real engine
    let subj = format!("{}{}", "a".repeat(n as usize), "!");
    let op: &str = *g.rng.pick(&["~", "!~"]);
    raw(format!("SELECT '{}' {} '{}';", subj, op, esc(pat)))
}

/// Large bounded quantifiers: compile-space / REG_ETOOBIG probe. Both
/// engines either compile cleanly or both raise the too-complex error;
/// the differential runner carves the ratified stack-band asymmetry.
fn gen_bigquant(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("regex:bigquant");
    let s = subject_lit(g);
    // {min,max} in the interesting band around the 255 DUPMAX-per-node and
    // the compile-space accumulation of chained/nested large repeats.
    let pat = match g.rng.below(6) {
        0 => format!("a{{{}}}", 100 + g.rng.below(900)),
        1 => format!("a{{{},{}}}", 50 + g.rng.below(200), 300 + g.rng.below(1700)),
        2 => format!("(ab){{{},{}}}", 10 + g.rng.below(90), 100 + g.rng.below(500)),
        3 => format!("[a-z]{{{},{}}}", 50 + g.rng.below(150), 200 + g.rng.below(800)),
        // Nested bounded repeats multiply the compiled NFA size.
        4 => format!("(a{{{}}}){{{}}}", 20 + g.rng.below(60), 20 + g.rng.below(60)),
        _ => format!("(a|bb|ccc){{{},{}}}", 20 + g.rng.below(80), 200 + g.rng.below(800)),
    };
    raw(format!("SELECT regexp_match({}, '{}') IS NOT NULL;", s, esc(&pat)))
}

/// Deep parenthesis / bracket nesting: drives the byte-based stack guard
/// (`stack_is_too_deep`) in the parser. Depth kept in a moderate band —
/// enough to reach the guard on the smaller-frame side without a runaway
/// literal. pgrust must reach a clean REG_ETOOBIG, never abort.
fn gen_deepnest(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("regex:deepnest");
    let depth = 50 + g.rng.below(350); // 50..=399
    let (open, close): (&str, &str) = match g.rng.below(4) {
        0 => ("(", ")"),
        1 => ("(?:", ")"),
        2 => ("(?=", ")"),
        _ => ("[", "]"), // bracket-expression nesting is shallow in ARE; still a parser path
    };
    let pat = if close == "]" {
        // Bracket expressions don't nest; instead build a long alternation
        // chain, another deep-parse path.
        let mut s = String::new();
        for i in 0..depth {
            if i > 0 {
                s.push('|');
            }
            s.push('a');
        }
        s
    } else {
        format!(
            "{}a{}",
            open.repeat(depth as usize),
            close.repeat(depth as usize)
        )
    };
    raw(format!("SELECT regexp_match('aaaa', '{}') IS NOT NULL;", esc(&pat)))
}

/// Back-references (a REG_ADVANCED-only feature that forces the NFA/backref
/// executor): valid and (err arm) dangling.
fn gen_backref(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("regex:backref");
    let err = err_arm(g);
    let (pat, subj): (&str, &str) = if err {
        // \9 with no 9th group → invalid back reference (2201C).
        *g.rng.pick(&[("(a)\\9", "aa"), ("(a)(b)\\3", "abab"), ("\\1(a)", "aa")])
    } else {
        *g.rng.pick(&[
            ("(a)\\1", "aa"),
            ("(ab)(cd)\\2\\1", "abcdcdab"),
            ("(\\w)\\1", "book"),
            ("(.)\\1{2}", "aaab"),
            ("(\\w+) \\1", "hi hi"),
            ("(a|b)\\1", "aa"),
        ])
    };
    let op: &str = *g.rng.pick(&["~", "!~"]);
    raw(format!("SELECT '{}' {} '{}';", esc(subj), op, esc(pat)))
}

/// Character-class edges: POSIX classes, negation, range/edge cases,
/// collating-element and equivalence-class brackets, and (err arm) the
/// invalid forms C rejects with a specific SQLSTATE.
fn gen_charclass(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("regex:charclass");
    let err = err_arm(g);
    let pat: &str = if err {
        *g.rng.pick(&[
            "[z-a]",       // invalid range (2201B)
            "[[:foo:]]",   // unknown char class
            "[a",          // unterminated bracket
            "[[.no-such.]]", // unknown collating element
            "[[=@=]]",     // maybe-empty equivalence class edge
        ])
    } else {
        *g.rng.pick(&[
            "[abc]",
            "[^abc]",
            "[a-z0-9]",
            "[]a]",     // ] as first member is literal
            "[^]a]",
            "[-a]",     // leading - is literal
            "[a-]",     // trailing - is literal
            "[[:digit:]]",
            "[[:alpha:][:space:]]",
            "[\\d\\w]",
            "[[.hyphen.]]",
            "[[=a=]]",
        ])
    };
    let s = subject_lit(g);
    raw(format!("SELECT regexp_match({}, '{}')::text;", s, esc(pat)))
}

/// Anchors and lookaround constraints (ARE): string/line anchors, word
/// boundaries, and the non-capturing lookahead/lookbehind constraints.
fn gen_anchor(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("regex:anchor");
    let pat: &str = *g.rng.pick(&[
        "^abc",
        "abc$",
        "\\Aabc",
        "abc\\Z",
        "\\ba\\b",
        "\\Bb",
        "\\mword\\M",
        "\\yfox\\y",
        "a(?=b)",
        "a(?!b)",
        "(?<=a)b",
        "(?<!a)b",
        "^$",
        "\\A\\Z",
    ]);
    let s = subject_lit(g);
    let f = if g.rng.chance(1, 3) { flag(g) } else { "" };
    raw(format!("SELECT regexp_match({}, '{}', '{}')::text;", s, esc(pat), f))
}

/// Embedded option directors and the ARE `***` prefixes, plus the expanded
/// (?x) whitespace mode. (err arm) directors that are illegal mid-pattern.
fn gen_flags(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("regex:flags");
    let err = err_arm(g);
    let pat: &str = if err {
        *g.rng.pick(&[
            "a(?i)b",   // embedded option not at start → error in ARE
            "(?z)a",    // unknown embedded option
            "***?a",    // bad *** director
        ])
    } else {
        *g.rng.pick(&[
            "(?i)abc",
            "(?x) a b c",
            "(?s).",
            "(?n).",
            "(?p).",
            "(?w).",
            "***:abc",     // force ARE
            "***=a.c",     // literal string match (metachars inert)
            "(?i)(?s)a.b",
        ])
    };
    let s = subject_lit(g);
    raw(format!("SELECT regexp_match({}, '{}')::text;", s, esc(pat)))
}

/// Deliberately malformed patterns: the compile-error surface (REG_* →
/// SQLSTATE). Standalone autocommit statement → the error is isolated;
/// the differential compare is on error identity (same SQLSTATE + message
/// class on both engines).
fn gen_invalid(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("regex:invalid");
    g.fire("regex:err");
    let pat: &str = *g.rng.pick(&[
        "(",         // unbalanced (
        ")",         // unbalanced )
        "(a",        // unclosed group
        "a)",        // extra )
        "[",         // unterminated bracket
        "a{",        // unbounded brace (actually literal in ARE — kept as a probe)
        "a{2,1}",    // reversed bound → invalid repeat
        "a{,}",      // empty bound
        "*",         // quantifier with no atom
        "+",
        "?",
        "a**",       // double quantifier
        "\\",        // trailing backslash
        "(?P<n>a)",  // Python-style named group — not ARE
        "(?<n>a)",   // .NET-style named group — not ARE
        "\\x{110000}", // out-of-range hex escape
        "[a-\\d]",   // range endpoint is a class
    ]);
    let s = subject_lit(g);
    raw(format!("SELECT regexp_match({}, {});", s, pat_expr(pat)))
}
