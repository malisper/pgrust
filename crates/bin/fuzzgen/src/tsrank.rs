//! FTS-RANK statement module: self-contained ranking / headline /
//! tsvector-operator / ts_stat / tsquery-operator probe SELECTs over
//! *literal* tsvector and tsquery inputs.
//!
//! Motivation (covdiff FTS-RANK lane): the expression-level `ts:*`
//! productions in `rich` carry the stemmer + `to_tsvector`/`to_tsquery`
//! surface, but the ranking and operator machinery below it stayed dark —
//! `ts_rank_cd` (cover density), `ts_stat`, `ts_rewrite`, and most of the
//! `tsvector_op` operators were at 0% because `ts:rank` is gated behind
//! the rare `float4` expression slot and never carried positional
//! tsvectors or the full tsquery operator grammar. This module fires the
//! surface directly with hand-authored literals so every rank
//! normalization bit, cover-density path, headline option, and tsvector /
//! tsquery operator is reached.
//!
//! Determinism / comparability: every input is a literal, so both engines
//! see byte-identical arguments. `ts_rank[_cd]` returns `float4` (ulp-soft
//! compare via the wire float4 oid — never cast to text). tsvector /
//! tsquery / headline results are canonical text, exact-compared. Multi-
//! row producers (`ts_stat`, `unnest`) carry a total `ORDER BY` so row
//! order is engine-independent. All configs used (`simple`, `english`)
//! are the shipped ones, verified byte-identical on both engines by the
//! T2 tsdl lane.

use crate::stmt::{Gen, StmtKind};

/// Literal tsvectors carrying positions and (sometimes) per-position
/// weights — the shapes the rank/cover-density and operator code walk.
/// Every entry parses on both engines (`'…'::tsvector`).
const TSVECS: &[&str] = &[
    "fox:1A quick:2B brown:3 dog:8C lazy:5,9",
    "the:1 quick:2 brown:3 fox:4 jumps:5 over:6 the:7 lazy:8 dog:9",
    "cat:1,3,5 dog:2,4 bird:6A mouse:7B",
    "w:1A x:2B y:3C z:4D",
    "hello:1 world:2 hello:4 world:5 test:3",
    "a:1 b:2 c:3 d:4 e:5 f:6 g:7 h:8",
    "single:1",
    "alpha beta gamma delta",
    "run:1A running:2B ran:3C runs:4",
    "database:1,10 system:2 query:3A search:4B index:5",
];

/// Literal tsqueries spanning the whole operator grammar: AND / OR / NOT,
/// phrase `<->` and `<N>`, grouping, prefix `:*`, weight restriction
/// `:AB`, and single terms.
const TSQUERIES: &[&str] = &[
    "fox & dog",
    "fox | cat",
    "fox <-> quick",
    "quick <-> brown",
    "fox <2> dog",
    "!lazy & dog",
    "(cat | bird) & dog",
    "brown & fox & dog",
    "a <-> b <-> c",
    "the & quick & brown & fox",
    "dog | bird | cat | mouse",
    "fox:A & dog:C",
    "fo:* & do:*",
    "quick:AB",
    "database & (query | search)",
    "!nonsense",
    "run <-> running",
];

/// float4[] weight vectors for the 4-slot {D,C,B,A} rank weight arg.
/// The final two include an out-of-range value (>1.0) to reach the
/// "weight out of range" guard in `calc_rank_cd`; both engines raise the
/// same error, so the pair still compares equal.
const WEIGHT_ARRS: &[&str] = &[
    "{0.1, 0.2, 0.4, 1.0}",
    "{0.05, 0.1, 0.5, 1.0}",
    "{1.0, 1.0, 1.0, 1.0}",
    "{0.0, 0.0, 0.0, 1.0}",
    "{0.25, 0.5, 0.75, 1.0}",
    "{0.1, 0.2, 0.4, 2.0}",
];

/// Normalization bitmask values: each single bit (1,2,4,8,16,32) plus 0
/// and multi-bit combinations, so every `method & RANK_NORM_*` arm in
/// both `calc_rank` and `calc_rank_cd` is taken.
const NORMS: &[&str] = &[
    "0", "1", "2", "4", "8", "16", "32", "3", "5", "10", "20", "12", "24", "30", "31",
];

/// Text-search configurations used for headline / to_tsquery probes.
const CFGS: &[&str] = &["simple", "english"];

/// English documents for `ts_headline` — multi-sentence so fragment mode
/// (`MaxFragments`) has several covers to choose among; the mark / cover
/// machinery in `wparser_def::headline` walks these deterministically.
const HL_DOCS: &[&str] = &[
    "The quick brown fox jumps over the lazy dog. The dog was not amused. \
     A second fox appeared near the river and the dog gave chase.",
    "PostgreSQL provides powerful full text search. The search engine ranks \
     documents by relevance. Ranking uses cover density and term frequency.",
    "Cats and dogs are common pets. The cat sat on the mat while the dog \
     slept nearby. Later the cat chased a mouse across the room.",
    "Databases store and index large volumes of data. Query performance \
     depends on good indexes. A slow query can be tuned with an index.",
];

/// Headline query word sets (space-separated) fed through the config's
/// `plainto_tsquery` / `to_tsquery`.
const HL_QUERIES: &[&str] = &[
    "fox dog", "search ranking", "cat mouse", "query index", "dog", "database index",
];

/// Headline option strings covering every documented option and the two
/// distinct code paths (word-mark vs fragment-mark).
const HL_OPTS: &[&str] = &[
    "StartSel=<b>, StopSel=</b>",
    "MaxWords=7, MinWords=3",
    "ShortWord=2",
    "HighlightAll=true",
    "HighlightAll=false",
    "MaxFragments=2, FragmentDelimiter= ... ",
    "MaxFragments=3, MaxWords=6, MinWords=2",
    "StartSel=**, StopSel=**, MaxWords=10, MinWords=4, ShortWord=1",
    "MaxFragments=1, StartSel=[, StopSel=]",
];

fn pick<'a>(g: &mut Gen, xs: &'a [&'a str]) -> &'a str {
    xs[g.rng.below_usize(xs.len())]
}

/// `ts_rank[_cd](weights?, tsvector, tsquery, norm?)` -> float4. Covers
/// all four fmgr arities and, via positional tsvectors + phrase/AND/OR
/// tsqueries, the cover-density and calc_rank arms.
fn rank_probe(g: &mut Gen) -> String {
    let cd = g.rng.chance(2, 5);
    if cd {
        g.fire("tsr:rankcd");
    } else {
        g.fire("tsr:rank");
    }
    let name = if cd { "ts_rank_cd" } else { "ts_rank" };
    let vec = pick(g, TSVECS);
    let q = pick(g, TSQUERIES);
    let with_w = g.rng.chance(2, 5);
    let with_n = g.rng.chance(3, 5);
    let wexpr = format!("'{}'::tsvector", vec);
    let qexpr = format!("'{}'::tsquery", q);
    match (with_w, with_n) {
        (true, true) => format!(
            "SELECT {}('{}'::float4[], {}, {}, {});",
            name, pick(g, WEIGHT_ARRS), wexpr, qexpr, pick(g, NORMS)
        ),
        (true, false) => format!(
            "SELECT {}('{}'::float4[], {}, {});",
            name, pick(g, WEIGHT_ARRS), wexpr, qexpr
        ),
        (false, true) => {
            format!("SELECT {}({}, {}, {});", name, wexpr, qexpr, pick(g, NORMS))
        }
        (false, false) => format!("SELECT {}({}, {});", name, wexpr, qexpr),
    }
}

/// `ts_headline([cfg,] doc, query [, opts])` -> text (exact compare).
fn headline_probe(g: &mut Gen) -> String {
    g.fire("tsr:headline");
    let cfg = pick(g, CFGS);
    let doc = pick(g, HL_DOCS);
    let qw = pick(g, HL_QUERIES);
    let qfn = if g.rng.chance(1, 3) { "to_tsquery" } else { "plainto_tsquery" };
    // to_tsquery needs a boolean-joined query; plainto_tsquery takes raw words.
    let qtext = if qfn == "to_tsquery" {
        qw.split_whitespace().collect::<Vec<_>>().join(" | ")
    } else {
        qw.to_string()
    };
    let query = format!("{}('{}', '{}')", qfn, cfg, qtext);
    if g.rng.chance(3, 4) {
        let opts = pick(g, HL_OPTS);
        format!("SELECT ts_headline('{}', '{}', {}, '{}');", cfg, doc, query, opts)
    } else {
        format!("SELECT ts_headline('{}', '{}', {});", cfg, doc, query)
    }
}

/// tsvector operators: setweight (char + by-lexeme), ts_delete (single +
/// array), ts_filter, concat `||`, strip, length, tsvector_to_array,
/// array_to_tsvector, unnest, and the comparison operators.
fn vecop_probe(g: &mut Gen) -> String {
    g.fire("tsr:vecop");
    let v = format!("'{}'::tsvector", pick(g, TSVECS));
    match g.rng.below(12) {
        0 => {
            let w = pick(g, &["A", "B", "C", "D"]);
            format!("SELECT setweight({}, '{}')::text;", v, w)
        }
        1 => {
            let w = pick(g, &["A", "B", "C", "D"]);
            let lexemes = pick(g, &["{fox,dog}", "{cat,mouse,bird}", "{a,b,c}", "{quick}"]);
            format!("SELECT setweight({}, '{}', '{}'::text[])::text;", v, w, lexemes)
        }
        2 => {
            let lex = pick(g, &["fox", "dog", "cat", "the", "a", "run"]);
            format!("SELECT ts_delete({}, '{}')::text;", v, lex)
        }
        3 => {
            let arr = pick(g, &["{fox,dog}", "{a,b,c}", "{the,quick}", "{nonexistent}"]);
            format!("SELECT ts_delete({}, '{}'::text[])::text;", v, arr)
        }
        4 => {
            let wts = pick(g, &["{a,b}", "{a}", "{b,c,d}", "{d}", "{a,b,c,d}"]);
            format!("SELECT ts_filter({}, '{}'::\"char\"[])::text;", v, wts)
        }
        5 => {
            let v2 = format!("'{}'::tsvector", pick(g, TSVECS));
            format!("SELECT ({} || {})::text;", v, v2)
        }
        6 => format!("SELECT strip({})::text;", v),
        7 => format!("SELECT length({});", v),
        8 => format!("SELECT tsvector_to_array({});", v),
        9 => {
            let arr = pick(g, &["{fox,dog,cat}", "{a,b,c,d}", "{single}", "{x,y,z}"]);
            format!("SELECT array_to_tsvector('{}'::text[])::text;", arr)
        }
        10 => format!(
            "SELECT lexeme, positions, weights FROM unnest({}) ORDER BY lexeme, positions;",
            v
        ),
        _ => {
            let v2 = format!("'{}'::tsvector", pick(g, TSVECS));
            let op = pick(g, &["=", "<", ">", "<=", ">=", "<>"]);
            format!("SELECT ({} {} {});", v, op, v2)
        }
    }
}

/// tsquery operators: `&&`, `||`, `!!`, `<->`, tsquery_phrase, querytree,
/// numnode, and the comparison operators.
fn tsqop_probe(g: &mut Gen) -> String {
    g.fire("tsr:tsqop");
    let q = format!("'{}'::tsquery", pick(g, TSQUERIES));
    match g.rng.below(9) {
        0 => {
            let q2 = format!("'{}'::tsquery", pick(g, TSQUERIES));
            format!("SELECT ({} && {})::text;", q, q2)
        }
        1 => {
            let q2 = format!("'{}'::tsquery", pick(g, TSQUERIES));
            format!("SELECT ({} || {})::text;", q, q2)
        }
        2 => format!("SELECT (!! {})::text;", q),
        3 => {
            let q2 = format!("'{}'::tsquery", pick(g, TSQUERIES));
            format!("SELECT ({} <-> {})::text;", q, q2)
        }
        4 => {
            let q2 = format!("'{}'::tsquery", pick(g, TSQUERIES));
            let d = g.rng.below(5) + 1;
            format!("SELECT tsquery_phrase({}, {}, {})::text;", q, q2, d)
        }
        5 => format!("SELECT querytree({});", q),
        6 => format!("SELECT numnode({});", q),
        7 => {
            let q2 = format!("'{}'::tsquery", pick(g, TSQUERIES));
            let op = pick(g, &["=", "<", ">", "<>"]);
            format!("SELECT ({} {} {});", q, op, q2)
        }
        _ => {
            // ts_rewrite: replace a subtree, both the (query,target,subst)
            // and the SPI-query form.
            g.fire("tsr:rewrite");
            if g.rng.chance(1, 2) {
                let target = pick(g, &["fox", "dog", "cat", "a", "the"]);
                let subst = pick(g, &["cat | mouse", "wolf", "bird & fish", "a <-> b"]);
                format!(
                    "SELECT ts_rewrite({}, '{}'::tsquery, '{}'::tsquery)::text;",
                    q, target, subst
                )
            } else {
                // The (query, sql-text) form: the SQL returns (target,
                // substitute) tsquery pairs over literal rows.
                format!(
                    "SELECT ts_rewrite({}, $$SELECT 'fox'::tsquery, 'cat | wolf'::tsquery$$)::text;",
                    q
                )
            }
        }
    }
}

/// `ts_stat(sqlquery [, weights])` -> SETOF (word, ndoc, nentry). The SPI
/// query returns literal tsvectors (a UNION so ndoc > 1). A total ORDER BY
/// makes the row set engine-independent.
fn stat_probe(g: &mut Gen) -> String {
    g.fire("tsr:stat");
    let docs = pick(g, &[
        "SELECT 'fox:1 quick:2 fox:3'::tsvector UNION ALL SELECT 'fox:1 dog:2'::tsvector \
         UNION ALL SELECT 'quick:1 brown:2 dog:3'::tsvector",
        "SELECT 'a:1A b:2B c:3'::tsvector UNION ALL SELECT 'a:1 b:2C'::tsvector",
        "SELECT 'cat:1 dog:2 cat:3 dog:4'::tsvector UNION ALL SELECT 'cat:1 bird:2'::tsvector",
    ]);
    if g.rng.chance(1, 2) {
        let wts = pick(g, &["a", "ab", "abcd", "d"]);
        format!(
            "SELECT word, ndoc, nentry FROM ts_stat($${}$$, '{}') ORDER BY word, ndoc, nentry;",
            docs, wts
        )
    } else {
        format!(
            "SELECT word, ndoc, nentry FROM ts_stat($${}$$) ORDER BY word, ndoc, nentry;",
            docs
        )
    }
}

/// One statement group for the FTS-RANK module.
pub fn gen_tsrank_module(g: &mut Gen) -> Vec<StmtKind> {
    let options = ["tsr:rank", "tsr:headline", "tsr:vecop", "tsr:tsqop", "tsr:stat"];
    let pick = g.weights.pick(g.rng, &options);
    let sql = match pick {
        "tsr:headline" => headline_probe(g),
        "tsr:vecop" => vecop_probe(g),
        "tsr:tsqop" => tsqop_probe(g),
        "tsr:stat" => stat_probe(g),
        _ => rank_probe(g),
    };
    vec![StmtKind::Raw(sql)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    #[test]
    fn corpora_nonempty_and_bounded() {
        assert!(!TSVECS.is_empty() && !TSQUERIES.is_empty());
        assert!(!HL_DOCS.is_empty() && !HL_OPTS.is_empty());
        // Every literal stays well under any statement-length limit.
        for s in TSVECS.iter().chain(TSQUERIES.iter()).chain(HL_DOCS.iter()) {
            assert!(s.len() < 400, "literal too long: {s}");
        }
    }

    #[test]
    fn probes_are_select_statements() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        for seed in 0..500u64 {
            let mut rng = Rng::new(seed);
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            for s in gen_tsrank_module(&mut g) {
                let sql = s.to_sql();
                assert!(sql.starts_with("SELECT "), "not a SELECT: {sql}");
                assert!(sql.ends_with(';'), "missing terminator: {sql}");
                // Balanced single quotes (all literals are well-formed).
                assert_eq!(sql.matches('\'').count() % 2, 0, "unbalanced quotes: {sql}");
            }
        }
    }
}
