//! C3: locale-sensitive collation BEHAVIOR differential module.
//!
//! Closes the standing rig blind spot: every cluster the rig boots is
//! C-locale, so real libc comparison semantics (strcoll/strxfrm order,
//! locale case rules, abbreviated-key boundaries — the historical
//! strxfrm/selfuncs bug surface) were unexercised. This module drives them
//! through SQL on ANY cluster: each group CREATE COLLATIONs a libc
//! collation from a fixed, manifest-recorded locale pool and exercises
//! comparison/sort/case/pattern/index/range surface under it.
//!
//! Validity argument (same-host law): both engines of a differential pair
//! call the SAME libc on the same host, so C-reference behavior IS libc's
//! behavior and pgrust must match through the same calls. KNOWN TRAP
//! (memory law): macOS qsort/strcoll can hide tie-order divergences that
//! glibc exposes — anything interesting found locally is flagged for a
//! CI cluster confirm leg, never dismissed.
//!
//! Determinism disciplines:
//!   - the locale pool is a fixed constant (below); same seed = same
//!     statements regardless of host locale support. A locale unsupported
//!     by an engine fails its CREATE COLLATION (and the group's dependent
//!     statements) — identically on both sides if support matches, and as
//!     a banked inventory finding if it does not. Error paths are targets,
//!     never hazards.
//!   - libc collations are deterministic in PostgreSQL (strcoll ties are
//!     broken by memcmp), so `ORDER BY s COLLATE x, pk` is a total order
//!     even over strcoll-equal distinct strings; every table probe is
//!     total-ordered or projects a sorted array
//!     (`array_agg(s ORDER BY s COLLATE x, pk)`).
//!   - groups are self-contained plpg-style: create collation (+ table /
//!     index / range type), exercise, probe, drop everything, no
//!     cross-group edges. Name counters live in `CollState` so names stay
//!     session-unique even when a drop failed.
//!   - forced-index-scan probes bracket with SET/RESET enable_seqscan as
//!     standalone statements (module groups are never inside txn
//!     brackets), and the probe itself carries a full ORDER BY ... , pk.

use crate::stmt::{Gen, StmtKind};

/// Session-persistent name counters (objects themselves are group-local).
#[derive(Clone, Debug, Default)]
pub struct CollState {
    next_coll: u32,
    next_table: u32,
    next_range: u32,
}

impl CollState {
    pub fn new() -> CollState {
        CollState::default()
    }
}

/// The fixed libc locale pool (part of the witness; recorded in findings
/// manifests). Host-verified present on macOS for both engines at lane
/// start; C/POSIX are universal. de_DE (ß/umlauts), sv_SE (å ä ö sort
/// AFTER z), tr_TR (dotless ı / dotted İ case rules), ja_JP (multibyte,
/// non-Latin) are the interestingly-different members.
pub const C3_LOCALES: &[&str] = &[
    "C",
    "POSIX",
    "en_US.UTF-8",
    "de_DE.UTF-8",
    "sv_SE.UTF-8",
    "tr_TR.UTF-8",
    "ja_JP.UTF-8",
];

/// Text-comparison edge fuel: strings that C-locale and real locales rank
/// differently, case/accent/punctuation near-pairs, Turkish dotted/dotless
/// i, combining-character vs precomposed forms, strxfrm-length-boundary
/// strings (the abbreviated-key surface behind the selfuncs history), and
/// empty/space edges. No single quotes: every entry is SQL-literal-safe
/// as 'entry'.
pub const C3_STRINGS: &[&str] = &[
    // case pairs (en_US ranks aA together; C ranks all uppercase first)
    "a", "A", "b", "B", "z", "Z",
    // German: sharp s vs ss/SS, umlauts vs base vowels
    "\u{00df}", "ss", "SS", "Stra\u{00df}e", "Strasse", "STRASSE",
    "\u{00e4}", "\u{00c4}", "ae", "\u{00f6}", "\u{00d6}", "oe",
    "\u{00fc}", "\u{00dc}", "ue",
    // Swedish: å ä ö sort after z in sv_SE, near a/o in de_DE/en_US
    "\u{00e5}", "\u{00c5}", "\u{00e5}ngstr\u{00f6}m", "zebra", "Zebra",
    // Turkish: dotless ı, dotted İ, ASCII i/I (case rules differ by locale)
    "\u{0131}", "I", "i", "\u{0130}", "istanbul", "ISTANBUL", "\u{0130}stanbul",
    // accents + combining vs precomposed (NFC é vs e + U+0301)
    "\u{00e9}", "e\u{0301}", "e", "E", "caf\u{00e9}", "cafe", "cafe\u{0301}",
    "r\u{00e9}sum\u{00e9}", "resume",
    // Japanese: hiragana/katakana/kanji (multibyte, non-Latin ranges).
    // HOST-LIBC HAZARD (banked, findings-c3-collation.md F2): macOS libc
    // ASSERTS in collate.c lookup_substsearch on strxfrm of kana carrying
    // substitution entries (prolonged-sound mark U+30FC, some voiced kana
    // like U+30B4) under ja_JP.UTF-8 — it kills the backend on BOTH
    // engines through ANALYZE-stats inequality selectivity
    // (convert_string_datum -> strxfrm). Every string here is
    // strxfrm-verified safe on macOS; re-run the perl sweep in the
    // findings doc before adding kana.
    "\u{3042}", "\u{30a2}", "\u{3041}", "\u{65e5}\u{672c}", "\u{30c8}\u{30ad}\u{30aa}",
    // punctuation/space near-pairs (locales disagree with C on their weight)
    "a b", "a-b", "ab", "a.b", "a_b", "A B", "a  b",
    // leading/trailing space, empty
    " a", "a ", "", " ",
    // digits vs letters
    "0", "9", "a1", "A1", "1a",
    // strxfrm / abbreviated-key length boundaries (7/8/15/16/23/24 bytes
    // with a multibyte or case flip at the boundary)
    "aaaaaaa", "aaaaaaaa", "aaaaaaaA", "aaaaaaa\u{00e4}",
    "aaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaB",
    "aaaaaaaaaaaaaaa\u{00df}", "aaaaaaaaaaaaaaaaaaaaaaa\u{00e5}",
    "bbbbbbbbbbbbbbbbbbbbbbbb",
];

/// ICU-VERIFY weave (F1 adjudication lane): pinned, catalog-resident
/// ICU-provider collations exercised WITHOUT create/drop. `"unicode"` is in
/// BOTH engines' catalogs (C3 inventory: byte-identical pg_collation, the
/// one collprovider='i' row) — C serves it only when built --with-icu
/// (cpg-icu, scripts/pgicu-build.sh); pgrust serves it natively. Any
/// ordering divergence under these collations is the F1 keep-condition
/// verdict. Names are SQL-quoted as stored; entries must exist on both
/// sides of the pair (inventory asymmetries belong in the findings doc,
/// not this pool).
pub const C3_PINNED_COLLATIONS: &[&str] = &["unicode"];

/// A collation choice for one group: SQL-usable name, catalog-name literal
/// fuel, prologue (CREATE COLLATION or catalog probe) and epilogue (DROP or
/// nothing — pinned collations are never dropped).
struct CollPick {
    name: String,
    bare: String,
    pre: Vec<StmtKind>,
    post: Vec<StmtKind>,
}

/// LIKE/regex pattern fuel (paired with the string pool above; literal-
/// safe, and every regex is valid so pattern errors can never diverge).
const C3_LIKE_PATTERNS: &[&str] = &[
    "%a%", "a%", "%e", "_", "__", "%ss%", "%\u{00df}%", "%i%", "%I%",
    "%stra%", "STRA%", "a_b", "% %", "%",
];

const C3_REGEXES: &[&str] = &[
    "^a", "a$", "[a-z]+", "[[:alpha:]]+", "^[[:upper:]]", "i", "^$",
    "s{2}", "a.b", "[\u{00e0}-\u{00ff}]",
];

fn pick_locale(g: &mut Gen) -> &'static str {
    C3_LOCALES[g.rng.below_usize(C3_LOCALES.len())]
}

fn pick_string(g: &mut Gen) -> &'static str {
    C3_STRINGS[g.rng.below_usize(C3_STRINGS.len())]
}

fn lit(s: &str) -> String {
    format!("'{}'", s)
}

/// Sample n pool strings as (pk, literal) VALUES rows, pks 1..=n.
fn values_rows(g: &mut Gen, n: usize) -> String {
    let mut rows = Vec::with_capacity(n);
    for pk in 1..=n {
        rows.push(format!("({}, {})", pk, lit(pick_string(g))));
    }
    rows.join(", ")
}

/// One collation-module group. Every group is self-contained (create,
/// exercise, probe, drop) per the plpg discipline.
pub fn gen_coll_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("coll");
    let form = g.weights.pick(
        g.rng,
        &[
            "coll:cmp",
            "coll:table",
            "coll:like",
            "coll:case",
            "coll:index",
            "coll:range",
        ],
    );
    g.fire(form);
    match form {
        "coll:cmp" => gen_cmp_group(g),
        "coll:table" => gen_table_group(g),
        "coll:like" => gen_like_group(g),
        "coll:case" => gen_case_group(g),
        "coll:index" => gen_index_group(g),
        _ => gen_range_group(g),
    }
}

/// Collation choice shared by every group: usually a fresh libc
/// CREATE COLLATION from the fixed pool (dropped in-group), sometimes
/// (default 1-in-4, weight-steerable via coll:fresh/coll:pinned) a
/// pinned catalog ICU-provider collation (probed, never created/dropped
/// — the ICU-VERIFY surface; steer coll:pinned=0 on non-ICU A rigs).
fn new_collation(g: &mut Gen) -> CollPick {
    if g.weights.pick(g.rng, &["coll:fresh", "coll:pinned"]) == "coll:pinned" {
        let bare =
            C3_PINNED_COLLATIONS[g.rng.below_usize(C3_PINNED_COLLATIONS.len())].to_string();
        g.fire("coll:pinned");
        let probe = StmtKind::Raw(format!(
            "SELECT collname, collprovider::text, collisdeterministic FROM pg_collation WHERE collname = '{}';",
            bare
        ));
        return CollPick {
            name: format!("\"{}\"", bare),
            bare,
            pre: vec![probe],
            post: vec![],
        };
    }
    let loc = pick_locale(g);
    let cname = format!("fz_c3c_{}", g.coll.next_coll);
    g.coll.next_coll += 1;
    let create = StmtKind::Raw(format!(
        "CREATE COLLATION {} (locale = '{}');",
        cname, loc
    ));
    let drop = StmtKind::Raw(format!("DROP COLLATION {};", cname));
    CollPick {
        name: cname.clone(),
        bare: cname,
        pre: vec![create],
        post: vec![drop],
    }
}

/// Scalar comparison / greatest-least surface: operators under COLLATE on
/// pool literals (the varstr_cmp entry), plus a pg_collation probe.
fn gen_cmp_group(g: &mut Gen) -> Vec<StmtKind> {
    let pick = new_collation(g);
    let c = pick.name.clone();
    let mut out = pick.pre;
    for _ in 0..3 {
        let (a, b) = (lit(pick_string(g)), lit(pick_string(g)));
        let op = *g.rng.pick(&["<", "<=", "=", ">=", ">", "<>"]);
        g.fire2("coll:cmp:", op);
        out.push(StmtKind::Raw(format!(
            "SELECT {} {} {} COLLATE {}, ({} COLLATE {}) {} {};",
            a, op, b, c, b, c, op, a
        )));
    }
    let (x, y, z) = (
        lit(pick_string(g)),
        lit(pick_string(g)),
        lit(pick_string(g)),
    );
    out.push(StmtKind::Raw(format!(
        "SELECT greatest({} COLLATE {}, {}, {}), least({} COLLATE {}, {}, {});",
        x, c, y, z, x, c, y, z
    )));
    out.push(StmtKind::Raw(format!(
        "SELECT collname, collprovider::text, collisdeterministic FROM pg_collation WHERE collname = '{}';",
        pick.bare
    )));
    out.extend(pick.post);
    out
}

/// Collated-column table surface: ORDER BY (with pk tiebreak), sorted-
/// array projections, min/max, string_agg ORDER BY, DISTINCT and GROUP BY
/// on the collated column.
fn gen_table_group(g: &mut Gen) -> Vec<StmtKind> {
    let pick = new_collation(g);
    let c = pick.name.clone();
    let t = format!("fz_c3t_{}", g.coll.next_table);
    g.coll.next_table += 1;
    let n = 6 + g.rng.below_usize(7);
    let mut out = pick.pre;
    out.push(StmtKind::Raw(format!(
        "CREATE TABLE {} (pk int4 PRIMARY KEY, s text COLLATE {});",
        t, c
    )));
    out.push(StmtKind::Raw(format!("INSERT INTO {} VALUES {};", t, values_rows(g, n))));
    g.fire("coll:orderby");
    out.push(StmtKind::Raw(format!(
        "SELECT pk, s FROM {} ORDER BY s COLLATE {}, pk;",
        t, c
    )));
    out.push(StmtKind::Raw(format!(
        "SELECT pk, s FROM {} ORDER BY s DESC, pk DESC;",
        t
    )));
    g.fire("coll:sortarr");
    out.push(StmtKind::Raw(format!(
        "SELECT array_agg(s ORDER BY s COLLATE {}, pk), min(s), max(s) FROM {};",
        c, t
    )));
    g.fire("coll:stragg");
    out.push(StmtKind::Raw(format!(
        "SELECT string_agg(s, ',' ORDER BY s COLLATE {}, pk) FROM {};",
        c, t
    )));
    g.fire("coll:distinct");
    out.push(StmtKind::Raw(format!(
        "SELECT array_agg(x ORDER BY x, octet_length(x)) FROM (SELECT DISTINCT s COLLATE {} AS x FROM {}) d;",
        c, t
    )));
    g.fire("coll:groupby");
    out.push(StmtKind::Raw(format!(
        "SELECT s, count(*), array_agg(pk ORDER BY pk) FROM {} GROUP BY s ORDER BY s COLLATE {}, s;",
        t, c
    )));
    // Cross-collation re-sort of the same data (explicit COLLATE beats the
    // column collation): the C-vs-locale disagreement on the same rows IS
    // the surface.
    out.push(StmtKind::Raw(format!(
        "SELECT array_agg(s ORDER BY s COLLATE \"C\", pk) FROM {};",
        t
    )));
    out.push(StmtKind::Raw(format!(
        "SELECT pk FROM {} WHERE s BETWEEN {} AND {} ORDER BY pk;",
        t,
        lit(pick_string(g)),
        lit(pick_string(g))
    )));
    out.push(StmtKind::Raw(format!("DROP TABLE {};", t)));
    out.extend(pick.post);
    out
}

/// LIKE / ILIKE / regex match under an explicit collation, scalar and
/// table-filtered forms (the pattern-compare + lower() locale surface).
fn gen_like_group(g: &mut Gen) -> Vec<StmtKind> {
    let pick = new_collation(g);
    let c = pick.name.clone();
    let mut out = pick.pre;
    for _ in 0..2 {
        let s = lit(pick_string(g));
        let p = lit(C3_LIKE_PATTERNS[g.rng.below_usize(C3_LIKE_PATTERNS.len())]);
        let op = *g.rng.pick(&["LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE"]);
        g.fire2("coll:like:", op);
        out.push(StmtKind::Raw(format!(
            "SELECT ({} COLLATE {}) {} {};",
            s, c, op, p
        )));
    }
    let s = lit(pick_string(g));
    let re = lit(C3_REGEXES[g.rng.below_usize(C3_REGEXES.len())]);
    let op = *g.rng.pick(&["~", "~*", "!~", "!~*"]);
    g.fire2("coll:re:", op);
    out.push(StmtKind::Raw(format!(
        "SELECT ({} COLLATE {}) {} {};",
        s, c, op, re
    )));
    let t = format!("fz_c3t_{}", g.coll.next_table);
    g.coll.next_table += 1;
    let p = lit(C3_LIKE_PATTERNS[g.rng.below_usize(C3_LIKE_PATTERNS.len())]);
    out.push(StmtKind::Raw(format!(
        "CREATE TABLE {} (pk int4 PRIMARY KEY, s text COLLATE {});",
        t, c
    )));
    out.push(StmtKind::Raw(format!("INSERT INTO {} VALUES {};", t, values_rows(g, 8))));
    out.push(StmtKind::Raw(format!(
        "SELECT pk, s FROM {} WHERE s ILIKE {} ORDER BY s COLLATE {}, pk;",
        t, p, c
    )));
    out.push(StmtKind::Raw(format!("DROP TABLE {};", t)));
    out.extend(pick.post);
    out
}

/// Locale case-mapping surface: upper/lower/initcap under an explicit
/// collation (Turkish dotted/dotless i, ß, combining marks) — the
/// formatting-case family.
fn gen_case_group(g: &mut Gen) -> Vec<StmtKind> {
    let pick = new_collation(g);
    let c = pick.name.clone();
    let mut out = pick.pre;
    for _ in 0..3 {
        let s = lit(pick_string(g));
        g.fire("coll:casefn");
        out.push(StmtKind::Raw(format!(
            "SELECT upper({} COLLATE {}), lower({} COLLATE {}), initcap({} COLLATE {});",
            s, c, s, c, s, c
        )));
    }
    let (a, b) = (lit(pick_string(g)), lit(pick_string(g)));
    out.push(StmtKind::Raw(format!(
        "SELECT lower({} COLLATE {}) = lower({} COLLATE {}), upper({} COLLATE {}) < upper({} COLLATE {});",
        a, c, b, c, a, c, b, c
    )));
    out.extend(pick.post);
    out
}

/// Collated btree index + forced index-scan agreement probe (the
/// abbreviated-key / strxfrm surface: index order must agree with the
/// seqscan sort order on both engines), plus a text_pattern_ops variant.
fn gen_index_group(g: &mut Gen) -> Vec<StmtKind> {
    let pick = new_collation(g);
    let c = pick.name.clone();
    let t = format!("fz_c3t_{}", g.coll.next_table);
    g.coll.next_table += 1;
    let pattern_ops = g.rng.chance(1, 3);
    g.fire(if pattern_ops { "coll:idx:patops" } else { "coll:idx:btree" });
    let idxcols = if pattern_ops {
        format!("(s COLLATE {} text_pattern_ops)", c)
    } else {
        format!("(s COLLATE {})", c)
    };
    let bound = lit(pick_string(g));
    let mut out = pick.pre;
    out.extend([
        StmtKind::Raw(format!(
            "CREATE TABLE {} (pk int4 PRIMARY KEY, s text COLLATE {});",
            t, c
        )),
        StmtKind::Raw(format!("INSERT INTO {} VALUES {};", t, values_rows(g, 10))),
        StmtKind::Raw(format!("CREATE INDEX fz_c3i_{} ON {} {};", t, t, idxcols)),
        StmtKind::Raw(format!("ANALYZE {};", t)),
        StmtKind::Raw("SET enable_seqscan = off;".to_string()),
    ]);
    if pattern_ops {
        out.push(StmtKind::Raw(format!(
            "SELECT pk, s FROM {} WHERE s LIKE 'a%' ORDER BY s COLLATE \"C\", pk;",
            t
        )));
    }
    out.push(StmtKind::Raw(format!(
        "SELECT pk, s FROM {} WHERE (s COLLATE {}) > {} ORDER BY s COLLATE {}, pk;",
        t, c, bound, c
    )));
    out.push(StmtKind::Raw("RESET enable_seqscan;".to_string()));
    out.push(StmtKind::Raw(format!(
        "SELECT array_agg(s ORDER BY s COLLATE {}, pk) FROM {};",
        c, t
    )));
    out.push(StmtKind::Raw(format!("DROP TABLE {};", t)));
    out.extend(pick.post);
    out
}

/// Range type over collated text: bound comparisons route through the
/// collation for containment/overlap/emptiness.
fn gen_range_group(g: &mut Gen) -> Vec<StmtKind> {
    let pick = new_collation(g);
    let c = pick.name.clone();
    let r = format!("fz_c3r_{}", g.coll.next_range);
    g.coll.next_range += 1;
    let (lo, hi, probe) = (
        lit(pick_string(g)),
        lit(pick_string(g)),
        lit(pick_string(g)),
    );
    let (lo2, hi2) = (lit(pick_string(g)), lit(pick_string(g)));
    let mut out = pick.pre;
    out.extend([
        StmtKind::Raw(format!(
            "CREATE TYPE {} AS RANGE (subtype = text, collation = {});",
            r, c
        )),
        StmtKind::Raw(format!(
            "SELECT {}({}, {}) @> {}::text, isempty({}({}, {}));",
            r, lo, hi, probe, r, lo, hi
        )),
        StmtKind::Raw(format!(
            "SELECT {}({}, {}) && {}({}, {}), {}({}, {}) << {}({}, {});",
            r, lo, hi, r, lo2, hi2, r, lo, hi, r, lo2, hi2
        )),
        StmtKind::Raw(format!(
            "SELECT lower({}({}, {})), upper({}({}, {}));",
            r, lo, hi, r, lo, hi
        )),
        StmtKind::Raw(format!("DROP TYPE {};", r)),
    ]);
    out.extend(pick.post);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_groups(seed: u64, n: usize, spec: &str) -> (Vec<Vec<String>>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse(spec).unwrap();
        let mut rng = Rng::new(seed);
        let mut state = CollState::new();
        let mut groups = Vec::new();
        let mut prods = Vec::new();
        for _ in 0..n {
            let mut p = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut p, 3);
            std::mem::swap(&mut g.coll, &mut state);
            let kinds = gen_coll_module(&mut g);
            std::mem::swap(&mut g.coll, &mut state);
            assert!(!kinds.is_empty());
            groups.push(kinds.iter().map(|k| k.to_sql()).collect());
            prods.extend(p);
        }
        (groups, prods)
    }

    #[test]
    fn coll_is_deterministic() {
        let (a, _) = gen_groups(31, 400, "");
        let (b, _) = gen_groups(31, 400, "");
        assert_eq!(a, b);
        let (c, _) = gen_groups(32, 400, "");
        assert_ne!(a, c);
    }

    #[test]
    fn coll_variety_and_shape() {
        let (groups, prods) = gen_groups(0xC3, 700, "");
        let all: Vec<String> = groups.iter().flatten().cloned().collect();
        let hay = all.join("\n");
        for frag in [
            "CREATE COLLATION fz_c3c_",
            "DROP COLLATION fz_c3c_",
            "COLLATE fz_c3c_",
            "locale = 'en_US.UTF-8'",
            "locale = 'de_DE.UTF-8'",
            "locale = 'sv_SE.UTF-8'",
            "locale = 'tr_TR.UTF-8'",
            "locale = 'ja_JP.UTF-8'",
            "locale = 'C'",
            "locale = 'POSIX'",
            "ORDER BY s COLLATE fz_c3c_",
            ", pk;",
            "array_agg(s ORDER BY s COLLATE fz_c3c_",
            "string_agg(s, ',' ORDER BY s COLLATE",
            "SELECT DISTINCT s COLLATE",
            "GROUP BY s ORDER BY s COLLATE",
            "greatest(",
            "least(",
            " LIKE ",
            " ILIKE ",
            " ~ ",
            "upper(",
            "lower(",
            "initcap(",
            "CREATE INDEX fz_c3i_",
            "text_pattern_ops",
            "SET enable_seqscan = off;",
            "RESET enable_seqscan;",
            "AS RANGE (subtype = text, collation = fz_c3c_",
            "isempty(",
            "pg_collation",
            "COLLATE \"C\"",
            "COLLATE \"unicode\"",
            "collname = 'unicode'",
            "Stra\u{00df}e",
            "\u{0130}",
        ] {
            assert!(hay.contains(frag), "coll flavor {frag:?} never generated");
        }
        for p in [
            "coll:cmp",
            "coll:table",
            "coll:like",
            "coll:case",
            "coll:index",
            "coll:range",
            "coll:orderby",
            "coll:sortarr",
            "coll:stragg",
            "coll:distinct",
            "coll:groupby",
            "coll:casefn",
            "coll:idx:btree",
            "coll:idx:patops",
            "coll:pinned",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
    }

    /// Statement shape invariants (mirrors the registry-wide gate) plus
    /// group self-containment: every created object is dropped within its
    /// group.
    #[test]
    fn coll_groups_are_self_contained() {
        let (groups, _) = gen_groups(0x51, 500, "");
        for group in &groups {
            for sql in group {
                assert!(!sql.contains('\n'), "multi-line statement: {sql}");
                assert!(sql.ends_with(';'), "unterminated: {sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
            }
            let hay = group.join("\n");
            for (create, drop) in [
                ("CREATE COLLATION ", "DROP COLLATION "),
                ("CREATE TABLE ", "DROP TABLE "),
                ("CREATE TYPE ", "DROP TYPE "),
            ] {
                let ncreate = hay.matches(create).count();
                let ndrop = hay.matches(drop).count();
                assert_eq!(ncreate, ndrop, "unbalanced {create}/{drop} in group:\n{hay}");
            }
            // SET enable_seqscan always has its RESET in-group.
            assert_eq!(
                hay.matches("SET enable_seqscan = off").count(),
                hay.matches("RESET enable_seqscan").count(),
                "unbalanced SET/RESET in group:\n{hay}"
            );
        }
    }

    /// The locale pool is the fixed constant the manifest records: nothing
    /// outside it is ever emitted.
    #[test]
    fn coll_locales_from_fixed_pool_only() {
        let (groups, _) = gen_groups(0x77, 600, "");
        for group in &groups {
            for sql in group {
                if let Some(ix) = sql.find("locale = '") {
                    let rest = &sql[ix + "locale = '".len()..];
                    let loc = &rest[..rest.find('\'').unwrap()];
                    assert!(
                        C3_LOCALES.contains(&loc),
                        "locale {loc:?} outside the fixed pool"
                    );
                }
            }
        }
    }
}
