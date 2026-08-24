//! gramwalk: grammar-derived statement generation over the REAL parser
//! automaton tables (gram_core::tables — the vendored 18.3 bison output the
//! server parses with; nothing duplicated).
//!
//! Generation is a top-down random derivation over the grammar's production
//! tables (YYPRHS/YYRHS + YYR1): starting from the `toplevel_stmt`
//! nonterminal, every nonterminal on the frontier is expanded by a
//! rng-chosen production (per-production visit caps keep any one rule from
//! dominating a statement); once the expansion budget is spent, every
//! remaining nonterminal expands through its MIN-COST WITNESS production —
//! the rule recorded during the min-terminal-derivation fixpoint, whose
//! strict-improvement bookkeeping makes the witness derivation well-founded,
//! so closing always terminates. Productions mentioning unrenderable
//! terminals (MODE_*, the U& composite) carry infinite cost and are never
//! chosen.
//!
//! Terminals render to lexemes that re-lex to the same token: keywords from
//! the scanner's keyword list (crates/common/keywords x
//! scan_fgram::SCAN_KEYWORD_TOKENS), scan.l's {self} chars, the named
//! multi-char specials, Op from a fixed pool of multi-char operators, and
//! the literal classes from small pools — IDENT draws from the fixture/live
//! catalog's table and column names plus a vocabulary, so a useful fraction
//! of statements survives parse analysis and reaches execution. gram.y
//! already carries WITH_LA/NOT_LA/... duplicate productions for parser.c
//! base_yylex's lookahead merges, so derivations stay lexable; the rare
//! cross-production merge collision simply parses differently and lands in
//! the (differentially compared) error lane. Determinism is law: all
//! randomness flows through Gen::rng — same seed = byte-identical stream.
//!
//! Differential bar: unchanged rig law (result identity for successes,
//! SQLSTATE identity for failures) — with one special rule wired into
//! crate::diff::classify: a pgrust-side "this SQL construct is not yet
//! implemented (grammar rule N ...)" error is ALWAYS a finding naming the
//! rule, never noise, even when C errors too.

use crate::stmt::{Gen, StmtKind};
use gram_core::tables as gt;
use gram_core::tables::names::YYTNAME;
use scan_fgram::tokens as tk;
use pgsync::OnceLock;

/// How one terminal class renders. Literal-valued classes draw a concrete
/// lexeme from a pool at emission time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Class {
    Kw,
    Chr,
    Special,
    Ident,
    Sconst,
    Iconst,
    Fconst,
    Bconst,
    Xconst,
    Param,
    Op,
}

#[derive(Clone, Copy, Debug)]
struct TokDef {
    code: i32,
    class: Class,
    /// Fixed lexeme for Kw/Chr/Special; ignored for valued classes.
    fixed: &'static str,
}

/// scan.l's {self} set: single chars that lex as themselves. Any other
/// punctuation would lex as a (possibly multi-char) Op.
const SELF_CHARS: &[u8] = b",()[].;:+-*/%^<>=";

/// Multi-char operators scan.l lexes as Op (none end in +/- after a
/// non-op char, none embed comment starters).
const OP_POOL: &[&str] = &["~", "!~", "@>", "<@", "&&", "||", "##", "<->", "@@", "#", "?", "&"];

const IDENT_POOL: &[&str] = &["gw_a", "gw_b", "gw_c", "v0", "q7", "zz"];
const SCONST_POOL: &[&str] = &["x", "a b", "1", "2024-01-01", "on", "{1,2}"];
const ICONST_POOL: &[&str] = &["0", "1", "2", "7", "42", "100"];
const FCONST_POOL: &[&str] = &["1.5", "0.5", "3.25e2"];
const BCONST_POOL: &[&str] = &["B'0101'", "B'1'"];
const XCONST_POOL: &[&str] = &["X'1f'", "X'0'"];

fn special_lexeme(code: i32) -> Option<&'static str> {
    Some(match code {
        c if c == tk::TYPECAST => "::",
        c if c == tk::DOT_DOT => "..",
        c if c == tk::COLON_EQUALS => ":=",
        c if c == tk::EQUALS_GREATER => "=>",
        c if c == tk::LESS_EQUALS => "<=",
        c if c == tk::GREATER_EQUALS => ">=",
        c if c == tk::NOT_EQUALS => "<>",
        c if c == tk::FORMAT_LA => "FORMAT",
        c if c == tk::NOT_LA => "NOT",
        c if c == tk::NULLS_LA => "NULLS",
        c if c == tk::WITH_LA => "WITH",
        c if c == tk::WITHOUT_LA => "WITHOUT",
        _ => return None,
    })
}

struct Grammar {
    /// Renderable terminal alphabet.
    defs: Vec<TokDef>,
    /// Internal (translated) terminal symbol -> index into `defs`.
    def_of_internal: Vec<Option<usize>>,
    /// Rule r (1..=YYNRULES) -> RHS internal symbols.
    rhs: Vec<Vec<usize>>,
    /// Nonterminal (sym - YYNTOKENS) -> its rule numbers.
    rules_of: Vec<Vec<usize>>,
    /// Symbol -> minimal number of terminals derivable (u32::MAX =
    /// unrenderable/unreachable).
    mincost: Vec<u32>,
    /// Nonterminal -> the rule that achieved `mincost` (recorded on strict
    /// improvement during the fixpoint, so the witness derivation is
    /// well-founded and finite).
    witness: Vec<usize>,
    /// Internal symbol number of `stmt`, the derivation entry (uniform over
    /// every statement type; `toplevel_stmt` would waste half the stream on
    /// TransactionStmtLegacy).
    entry: usize,
}

fn build_alphabet() -> Vec<TokDef> {
    let mut defs: Vec<TokDef> = Vec::new();
    for &c in SELF_CHARS {
        if gt::yytranslate(c as i32) != gt::YYUNDEFTOK {
            let s: &'static str =
                Box::leak(String::from_utf8(vec![c]).unwrap().into_boxed_str());
            defs.push(TokDef { code: c as i32, class: Class::Chr, fixed: s });
        }
    }
    // Keywords: scanner keyword list x token codes (first spelling wins for
    // tokens with alternate spellings, e.g. analyse/analyze).
    for (n, &code) in scan_fgram::SCAN_KEYWORD_TOKENS.iter().enumerate() {
        let code = code as i32;
        if defs.iter().any(|d| d.code == code) {
            continue;
        }
        let text = keywords::keyword_text(n).expect("keyword text");
        defs.push(TokDef { code, class: Class::Kw, fixed: text });
    }
    for code in [
        tk::TYPECAST,
        tk::DOT_DOT,
        tk::COLON_EQUALS,
        tk::EQUALS_GREATER,
        tk::LESS_EQUALS,
        tk::GREATER_EQUALS,
        tk::NOT_EQUALS,
        tk::FORMAT_LA,
        tk::NOT_LA,
        tk::NULLS_LA,
        tk::WITH_LA,
        tk::WITHOUT_LA,
    ] {
        defs.push(TokDef { code, class: Class::Special, fixed: special_lexeme(code).unwrap() });
    }
    for (code, class) in [
        (tk::IDENT, Class::Ident),
        (tk::SCONST, Class::Sconst),
        (tk::ICONST, Class::Iconst),
        (tk::FCONST, Class::Fconst),
        (tk::BCONST, Class::Bconst),
        (tk::XCONST, Class::Xconst),
        (tk::PARAM, Class::Param),
        (tk::Op, Class::Op),
    ] {
        defs.push(TokDef { code, class, fixed: "" });
    }
    // Excluded on purpose (their productions get infinite cost): MODE_*
    // (parser-injected), UIDENT/USCONST/UESCAPE-composites (base_yylex
    // consumes extra lookahead).
    defs
}

fn grammar() -> &'static Grammar {
    static GRAMMAR: OnceLock<Grammar> = OnceLock::new();
    GRAMMAR.get_or_init(|| {
        let defs = build_alphabet();
        let ntokens = gt::YYNTOKENS as usize;
        let nsyms = YYTNAME.len();
        let nnts = nsyms - ntokens;

        let mut def_of_internal: Vec<Option<usize>> = vec![None; ntokens];
        for (i, d) in defs.iter().enumerate() {
            let internal = gt::yytranslate(d.code) as usize;
            assert!(def_of_internal[internal].is_none(), "alphabet collision at {internal}");
            def_of_internal[internal] = Some(i);
        }

        let mut rhs: Vec<Vec<usize>> = vec![Vec::new(); gt::YYNRULES + 1];
        let mut rules_of: Vec<Vec<usize>> = vec![Vec::new(); nnts];
        for r in 1..=gt::YYNRULES {
            let start = gt::YYPRHS[r] as usize;
            let mut syms = Vec::new();
            let mut k = start;
            while gt::YYRHS[k] >= 0 {
                syms.push(gt::YYRHS[k] as usize);
                k += 1;
            }
            debug_assert_eq!(syms.len(), gt::YYR2[r] as usize);
            rhs[r] = syms;
            let lhs = gt::YYR1[r] as usize;
            rules_of[lhs - ntokens].push(r);
        }

        // Min-terminal-derivation fixpoint with witness recording.
        let mut mincost: Vec<u32> = (0..nsyms)
            .map(|s| {
                if s < ntokens {
                    if def_of_internal[s].is_some() { 1 } else { u32::MAX }
                } else {
                    u32::MAX
                }
            })
            .collect();
        let mut witness: Vec<usize> = vec![0; nnts];
        loop {
            let mut changed = false;
            for r in 1..=gt::YYNRULES {
                let lhs = gt::YYR1[r] as usize;
                let mut cost: u32 = 0;
                let mut ok = true;
                for &s in &rhs[r] {
                    match mincost[s] {
                        u32::MAX => {
                            ok = false;
                            break;
                        }
                        c => cost = cost.saturating_add(c),
                    }
                }
                if ok && cost < mincost[lhs] {
                    mincost[lhs] = cost;
                    witness[lhs - ntokens] = r;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }

        let entry = YYTNAME
            .iter()
            .position(|&n| n == "stmt")
            .expect("stmt nonterminal");
        assert!(entry >= ntokens && mincost[entry] != u32::MAX);
        Grammar { defs, def_of_internal, rhs, rules_of, mincost, witness, entry }
    })
}

fn render(g: &mut Gen, def: &TokDef) -> String {
    match def.class {
        Class::Kw | Class::Chr | Class::Special => def.fixed.to_string(),
        Class::Ident => {
            // Catalog identifiers (fixture or live) keep a useful fraction
            // of statements alive through parse analysis.
            if !g.catalog.tables.is_empty() && g.rng.chance(3, 5) {
                let t = &g.catalog.tables[g.rng.below_usize(g.catalog.tables.len())];
                if !t.columns.is_empty() && g.rng.chance(1, 2) {
                    t.columns[g.rng.below_usize(t.columns.len())].name.clone()
                } else {
                    t.name.clone()
                }
            } else {
                g.rng.pick(IDENT_POOL).to_string()
            }
        }
        Class::Sconst => format!("'{}'", g.rng.pick(SCONST_POOL)),
        Class::Iconst => g.rng.pick(ICONST_POOL).to_string(),
        Class::Fconst => g.rng.pick(FCONST_POOL).to_string(),
        Class::Bconst => g.rng.pick(BCONST_POOL).to_string(),
        Class::Xconst => g.rng.pick(XCONST_POOL).to_string(),
        Class::Param => format!("${}", 1 + g.rng.below(3)),
        Class::Op => g.rng.pick(OP_POOL).to_string(),
    }
}

fn class_weight_name(c: Class) -> &'static str {
    match c {
        Class::Kw => "gramwalk:kw",
        Class::Chr | Class::Special => "gramwalk:char",
        Class::Ident => "gramwalk:ident",
        Class::Sconst | Class::Iconst | Class::Fconst | Class::Bconst | Class::Xconst => {
            "gramwalk:lit"
        }
        Class::Param => "gramwalk:param",
        Class::Op => "gramwalk:op",
    }
}

/// Any one production may fire at most this often per statement (visit cap;
/// min-cost closing is exempt — witness derivations must always complete).
const RULE_CAP: u32 = 6;

/// Frontier size bound: beyond it the derivation closes through witnesses
/// regardless of remaining budget.
const MAX_FRONTIER: usize = 400;

fn walk_statement(g: &mut Gen) -> String {
    let gr = grammar();
    let budget = match g.weights.pick(
        g.rng,
        &["gramwalk:len:short", "gramwalk:len:mid", "gramwalk:len:long"],
    ) {
        "gramwalk:len:long" => 60 + g.rng.below(60),
        "gramwalk:len:mid" => 24 + g.rng.below(24),
        _ => 8 + g.rng.below(10),
    } as usize;

    let mut rule_uses: Vec<u32> = vec![0; gr.rhs.len()];
    let mut expansions = 0usize;
    // Frontier stack: top = next symbol of the leftmost derivation.
    let mut frontier: Vec<usize> = vec![gr.entry];
    let mut sql = String::new();
    while let Some(sym) = frontier.pop() {
        if sym < gt::YYNTOKENS as usize {
            let def = &gr.defs[gr.def_of_internal[sym].expect("renderable terminal")];
            let lex = render(g, def);
            if !sql.is_empty() {
                sql.push(' ');
            }
            sql.push_str(&lex);
            continue;
        }
        let nt = sym - gt::YYNTOKENS as usize;
        let closing = expansions >= budget || frontier.len() >= MAX_FRONTIER;
        let rule = if closing {
            gr.witness[nt]
        } else {
            // Candidate productions: finite cost, under the visit cap.
            let cands: Vec<usize> = gr.rules_of[nt]
                .iter()
                .copied()
                .filter(|&r| {
                    rule_uses[r] < RULE_CAP
                        && gr.rhs[r].iter().all(|&s| gr.mincost[s] != u32::MAX)
                })
                .collect();
            if cands.is_empty() {
                gr.witness[nt]
            } else {
                // Class steering: when this nonterminal offers several
                // straight-to-one-terminal productions (leaf choice
                // points), bias by terminal class weight — keeps idents
                // and literals flowing without a full grammar-cost model.
                let leaf_names: Vec<&str> = cands
                    .iter()
                    .filter_map(|&c| {
                        (gr.rhs[c].len() == 1 && gr.rhs[c][0] < gt::YYNTOKENS as usize)
                            .then(|| {
                                class_weight_name(
                                    gr.defs[gr.def_of_internal[gr.rhs[c][0]].unwrap()].class,
                                )
                            })
                    })
                    .collect();
                if leaf_names.len() == cands.len() && leaf_names.len() > 1 {
                    let want = g.weights.pick(g.rng, &leaf_names);
                    let of_class: Vec<usize> = cands
                        .iter()
                        .copied()
                        .filter(|&c| {
                            class_weight_name(
                                gr.defs[gr.def_of_internal[gr.rhs[c][0]].unwrap()].class,
                            ) == want
                        })
                        .collect();
                    of_class[g.rng.below_usize(of_class.len())]
                } else {
                    cands[g.rng.below_usize(cands.len())]
                }
            }
        };
        expansions += 1;
        rule_uses[rule] += 1;
        for &s in gr.rhs[rule].iter().rev() {
            frontier.push(s);
        }
    }
    if !sql.ends_with(';') {
        sql.push_str(" ;");
    }
    sql
}

pub fn gen_gramwalk_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("gramwalk");
    vec![StmtKind::Raw(walk_statement(g))]
}

/// Round-7 FP-1: gramwalk derives database DDL over the whole grammar
/// (`alter database v0`, `create database q7`, ...) whose name operands
/// land in the CLUSTER-GLOBAL namespace — shared across the 3 concurrent
/// driver instances and across batches. Crash residue from a fault on one
/// side (a db legitimately left invalid mid-DROP) then FATALs a later
/// batch's `ALTER DATABASE` on that side only, and short names race
/// concurrent `CREATE DATABASE` into 42P04-vs-23505 splits (FP-7).
///
/// Rewrite every database-name operand — the identifier after the
/// `DATABASE` keyword (skipping IF [NOT] EXISTS) and the `RENAME TO`
/// target of an ALTER DATABASE — into the batch-unique namespace
/// `{tag}_{name}`, where `tag` is the batch's private scratch-db name.
/// The mapping is injective per batch, so within-batch create/alter/drop
/// coherence is preserved, and the statement TEXT is identical on both
/// sides, so differential parity is untouched. Statements that touched
/// protected databases (template1, postgres) become matched
/// does-not-exist errors instead of cluster vandalism — a bonus, not the
/// goal. Applied by the runner to gramwalk statements only; every other
/// module draws from fixture/suite-tagged namespaces already.
pub fn rebase_database_names(sql: &str, tag: &str) -> String {
    let is_word = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'$';
    let b = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + tag.len() + 1);
    let mut i = 0usize;
    // 0 = idle; 1 = expect db name (after DATABASE / RENAME TO); words
    // still to skip first ("if"/"not"/"exists") are handled inline.
    let mut expect_name = false;
    let mut saw_database = false;
    let mut prev_word = String::new();
    while i < b.len() {
        if b[i].is_ascii_alphabetic() || b[i] == b'_' {
            let start = i;
            while i < b.len() && is_word(b[i]) {
                i += 1;
            }
            let word = &sql[start..i];
            let lower = word.to_ascii_lowercase();
            if expect_name {
                if matches!(lower.as_str(), "if" | "not" | "exists") {
                    out.push_str(word);
                    prev_word = lower;
                    continue;
                }
                expect_name = false;
                let mut rebased = format!("{tag}_{lower}");
                rebased.truncate(63);
                out.push_str(&rebased);
                prev_word = lower;
                continue;
            }
            match lower.as_str() {
                "database" => {
                    saw_database = true;
                    expect_name = true;
                }
                "to" if prev_word == "rename" && saw_database => expect_name = true,
                _ => {}
            }
            out.push_str(word);
            prev_word = lower;
        } else {
            // Any non-word, non-space char (';', ',', operators) ends a
            // pending name expectation: `DATABASE ;` has no operand.
            if b[i] != b' ' {
                expect_name = false;
            }
            let c = sql[i..].chars().next().unwrap();
            out.push(c);
            i += c.len_utf8();
        }
    }
    out
}

/// Round-9 RB-9: tablespace names are CLUSTER-GLOBAL exactly like
/// database names (FP-1), but several stream modules create them under
/// fixed names (ddldeep's dd_ts/dd_ts2, vacuum's fz_vac_ts<n>) and
/// gramwalk derives raw tablespace DDL from the grammar. Concurrent
/// driver instances race each other's CREATE/RENAME/DROP, and crash
/// residue from a batch killed mid-group surfaces on ONE side only as
/// 42710 `tablespace "dd_ts2" already exists` on a later batch's
/// RENAME, then 55000 on its DROP (run b627b97f...-59-13, seeds
/// 1368837889297096578 / 1425779072044192746). Tablespace DIRECTORIES
/// cannot collide: every in-stream CREATE TABLESPACE is an in-place
/// tablespace (LOCATION '' under allow_in_place_tablespaces) living
/// inside each side's own datadir — only the shared NAME namespace
/// needs rebasing.
///
/// Rewrite every tablespace-name operand into the batch-unique
/// namespace `{tag}_{name}` (tag = the batch's private scratch-db name,
/// as for databases): the identifier after the TABLESPACE keyword
/// (CREATE/ALTER/DROP TABLESPACE, the TABLESPACE clause of CREATE
/// TABLE/INDEX/DATABASE, SET TABLESPACE, ALL IN TABLESPACE, REINDEX
/// (TABLESPACE ...)), the RENAME TO target of an ALTER TABLESPACE, and
/// the value list of SET default_tablespace / temp_tablespaces.
/// Built-in `pg_*` tablespaces (pg_default, pg_global) and quoted
/// material stay untouched. The mapping is injective per batch, so
/// within-batch create/alter/drop coherence is preserved, and the
/// statement TEXT is identical on both sides, so differential parity is
/// untouched. Applied by the runner to the WHOLE statement stream;
/// helper_diffrun reclaims `{tag}_*` tablespaces at batch cleanup.
pub fn rebase_tablespace_names(sql: &str, tag: &str) -> String {
    #[derive(PartialEq)]
    enum Mode {
        Idle,
        /// Expect one tablespace name (after TABLESPACE / RENAME TO).
        Name,
        /// Expect a name list (after SET default_tablespace /
        /// temp_tablespaces) until the statement ends.
        List,
    }
    let is_word = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'$';
    let b = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + tag.len() + 1);
    let mut i = 0usize;
    let mut mode = Mode::Idle;
    let mut saw_tablespace = false;
    let mut prev_word = String::new();
    while i < b.len() {
        // Quoted material is opaque — never rebase inside '...'/"...".
        if b[i] == b'\'' || b[i] == b'"' {
            let q = b[i];
            out.push(q as char);
            i += 1;
            while i < b.len() {
                if b[i] == q {
                    if i + 1 < b.len() && b[i + 1] == q {
                        out.push_str(&sql[i..i + 2]); // doubled quote
                        i += 2;
                        continue;
                    }
                    out.push(q as char);
                    i += 1;
                    break;
                }
                let c = sql[i..].chars().next().unwrap();
                out.push(c);
                i += c.len_utf8();
            }
            if mode == Mode::Name {
                mode = Mode::Idle;
            }
            continue;
        }
        if b[i].is_ascii_alphabetic() || b[i] == b'_' {
            let start = i;
            while i < b.len() && is_word(b[i]) {
                i += 1;
            }
            let word = &sql[start..i];
            let lower = word.to_ascii_lowercase();
            match mode {
                Mode::Name => {
                    if matches!(lower.as_str(), "if" | "not" | "exists") {
                        out.push_str(word);
                        prev_word = lower;
                        continue;
                    }
                    mode = Mode::Idle;
                    if lower.starts_with("pg_") {
                        out.push_str(word);
                    } else {
                        let mut rebased = format!("{tag}_{lower}");
                        rebased.truncate(63);
                        out.push_str(&rebased);
                    }
                    prev_word = lower;
                    continue;
                }
                Mode::List => {
                    // `SET x TO val` / `SET x TO DEFAULT` keywords and
                    // built-ins pass through; other bare identifiers
                    // are tablespace names.
                    if matches!(lower.as_str(), "to" | "default") || lower.starts_with("pg_") {
                        out.push_str(word);
                    } else {
                        let mut rebased = format!("{tag}_{lower}");
                        rebased.truncate(63);
                        out.push_str(&rebased);
                    }
                    prev_word = lower;
                    continue;
                }
                Mode::Idle => {
                    match lower.as_str() {
                        "tablespace" => {
                            saw_tablespace = true;
                            mode = Mode::Name;
                        }
                        "to" if prev_word == "rename" && saw_tablespace => mode = Mode::Name,
                        "default_tablespace" | "temp_tablespaces"
                            if matches!(prev_word.as_str(), "set" | "local" | "session") =>
                        {
                            mode = Mode::List;
                        }
                        _ => {}
                    }
                    out.push_str(word);
                    prev_word = lower;
                }
            }
        } else {
            match mode {
                // `TABLESPACE ;` has no operand; any non-space
                // punctuation ends a single-name expectation.
                Mode::Name if b[i] != b' ' => mode = Mode::Idle,
                // The list survives '=' and ',' separators only.
                Mode::List if !matches!(b[i], b' ' | b'=' | b',') => mode = Mode::Idle,
                _ => {}
            }
            let c = sql[i..].chars().next().unwrap();
            out.push(c);
            i += c.len_utf8();
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn statements(seed: u64, n: usize) -> Vec<String> {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(seed);
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            for s in gen_gramwalk_module(&mut g) {
                out.push(s.to_sql());
            }
        }
        out
    }

    /// Determinism guard: same seed = byte-identical statement stream.
    #[test]
    fn same_seed_byte_identical_stream() {
        let a = statements(0xC0FFEE, 200);
        let b = statements(0xC0FFEE, 200);
        assert_eq!(a, b);
        let c = statements(0xC0FFEF, 200);
        assert_ne!(a, c, "different seeds should differ");
    }

    /// The grammar tables load, the entry symbol derives cheaply, and every
    /// finite-cost witness rule lands on its own nonterminal.
    #[test]
    fn grammar_tables_are_coherent() {
        let gr = grammar();
        assert!(gr.defs.len() > 450, "alphabet too small: {}", gr.defs.len());
        assert!(gr.mincost[gr.entry] <= 2, "stmt mincost {}", gr.mincost[gr.entry]);
        for (nt, &w) in gr.witness.iter().enumerate() {
            let sym = nt + gt::YYNTOKENS as usize;
            if gr.mincost[sym] == u32::MAX {
                continue;
            }
            assert_eq!(gt::YYR1[w] as usize, sym, "witness rule {w} lhs mismatch");
        }
    }

    /// Generated statements re-lex and run through the real parser. The
    /// derivation is grammar-exact CONTEXT-FREE, so failures come only
    /// from (a) grammar ACTIONS raising errors (check_qualified_name-style
    /// semantic rejects inside gram.y — real parser surface both engines
    /// must agree on), (b) derivations bison's precedence/conflict
    /// resolution removed from the automaton (`a > b >= c` under %nonassoc
    /// comparisons), and (c) rare cross-production base_yylex merge
    /// collisions. All three fail identically on BOTH engines, so they are
    /// productive error-parity inputs, not waste. The pinned floor keeps
    /// re-lex fidelity honest: a renderer regression craters the rate.
    #[test]
    fn generated_statements_parse() {
        let stmts = statements(0xF00D, 400);
        let mut ok = 0usize;
        for sql in &stmts {
            let ctx = mcx::MemoryContext::new("gramwalk-test");
            let r = gram_core::raw_parser(
                ctx.mcx(),
                sql,
                parser_seams::RawParseMode::RAW_PARSE_DEFAULT,
            );
            if r.is_ok() {
                ok += 1;
            } else if std::env::var_os("GRAMWALK_DEBUG").is_some() {
                eprintln!("PARSE_FAIL {:?} => {}", r.err().map(|e| e.message.clone()), sql);
            }
        }
        assert!(
            ok * 10 >= stmts.len() * 7,
            "only {ok}/{} generated statements parse",
            stmts.len()
        );
    }

    /// FP-1: database-name operands land in the batch-unique namespace;
    /// everything else in the statement is untouched.
    #[test]
    fn rebase_database_names_rewrites_operands_only() {
        let t = "fuzz_gw_71_3";
        assert_eq!(
            rebase_database_names("alter database v0 ;", t),
            "alter database fuzz_gw_71_3_v0 ;"
        );
        assert_eq!(
            rebase_database_names("create database q7 ;", t),
            "create database fuzz_gw_71_3_q7 ;"
        );
        assert_eq!(
            rebase_database_names("drop database if exists zz ;", t),
            "drop database if exists fuzz_gw_71_3_zz ;"
        );
        // RENAME TO target of an ALTER DATABASE is rebased too.
        assert_eq!(
            rebase_database_names("alter database v0 rename to zz ;", t),
            "alter database fuzz_gw_71_3_v0 rename to fuzz_gw_71_3_zz ;"
        );
        // TEMPLATE source and unrelated identifiers are untouched.
        assert_eq!(
            rebase_database_names("create database v0 template template0 ;", t),
            "create database fuzz_gw_71_3_v0 template template0 ;"
        );
        // Protected names get rebased into the private namespace (matched
        // does-not-exist errors instead of cluster vandalism).
        assert_eq!(
            rebase_database_names("alter database template1 refresh collation version ;", t),
            "alter database fuzz_gw_71_3_template1 refresh collation version ;"
        );
        // No operand (derivation closed early): nothing to rewrite.
        assert_eq!(rebase_database_names("drop database ;", t), "drop database ;");
        // Non-database statements never change.
        let s = "select k_int from fz_scalar order by 1 ;";
        assert_eq!(rebase_database_names(s, t), s.to_string());
        // RENAME TO outside a database statement never changes.
        let s = "alter table fz_scalar rename to zz ;";
        assert_eq!(rebase_database_names(s, t), s.to_string());
        // 63-byte identifier bound holds.
        let long = rebase_database_names(
            "create database abcdefghijklmnopqrstuvwxyz0123456789 ;",
            "fuzz_gramwalk_1234567_99",
        );
        let name = long.split_whitespace().nth(2).unwrap();
        assert!(name.len() <= 63, "{name}");
    }

    /// RB-9: tablespace-name operands land in the batch-unique
    /// namespace across every stream shape that names one; built-ins,
    /// quoted material, and everything else stay untouched.
    #[test]
    fn rebase_tablespace_names_rewrites_operands_only() {
        let t = "fuzz_dd_71_3";
        // ddldeep TBLSPC deck shapes.
        assert_eq!(
            rebase_tablespace_names("CREATE TABLESPACE dd_ts LOCATION '';", t),
            "CREATE TABLESPACE fuzz_dd_71_3_dd_ts LOCATION '';"
        );
        assert_eq!(
            rebase_tablespace_names(
                "CREATE TABLE dd_tsp (a int, b text) TABLESPACE dd_ts;",
                t
            ),
            "CREATE TABLE dd_tsp (a int, b text) TABLESPACE fuzz_dd_71_3_dd_ts;"
        );
        assert_eq!(
            rebase_tablespace_names(
                "ALTER TABLE ALL IN TABLESPACE dd_ts SET TABLESPACE pg_default;",
                t
            ),
            "ALTER TABLE ALL IN TABLESPACE fuzz_dd_71_3_dd_ts SET TABLESPACE pg_default;"
        );
        // RENAME TO target of an ALTER TABLESPACE is rebased too.
        assert_eq!(
            rebase_tablespace_names("ALTER TABLESPACE dd_ts RENAME TO dd_ts2;", t),
            "ALTER TABLESPACE fuzz_dd_71_3_dd_ts RENAME TO fuzz_dd_71_3_dd_ts2;"
        );
        assert_eq!(
            rebase_tablespace_names("COMMENT ON TABLESPACE dd_ts2 IS 'q8 ts';", t),
            "COMMENT ON TABLESPACE fuzz_dd_71_3_dd_ts2 IS 'q8 ts';"
        );
        assert_eq!(
            rebase_tablespace_names("DROP TABLESPACE IF EXISTS dd_ts2;", t),
            "DROP TABLESPACE IF EXISTS fuzz_dd_71_3_dd_ts2;"
        );
        // default_tablespace / temp_tablespaces value lists.
        assert_eq!(
            rebase_tablespace_names("SET default_tablespace = dd_ts2;", t),
            "SET default_tablespace = fuzz_dd_71_3_dd_ts2;"
        );
        assert_eq!(
            rebase_tablespace_names("SET temp_tablespaces = dd_ts, dd_ts2;", t),
            "SET temp_tablespaces = fuzz_dd_71_3_dd_ts, fuzz_dd_71_3_dd_ts2;"
        );
        let s = "SET default_tablespace TO DEFAULT;";
        assert_eq!(rebase_tablespace_names(s, t), s.to_string());
        let s = "RESET default_tablespace;";
        assert_eq!(rebase_tablespace_names(s, t), s.to_string());
        // vacuum REINDEX arm (parenthesized option) stays coherent with
        // its CREATE/DROP.
        assert_eq!(
            rebase_tablespace_names(
                "REINDEX (TABLESPACE fz_vac_ts1, CONCURRENTLY, VERBOSE) INDEX fz_vac_ti1;",
                t
            ),
            "REINDEX (TABLESPACE fuzz_dd_71_3_fz_vac_ts1, CONCURRENTLY, VERBOSE) INDEX fz_vac_ti1;"
        );
        // Built-in pg_* tablespaces never move.
        let s = "ALTER INDEX dd_tsp_a SET TABLESPACE pg_default;";
        assert_eq!(rebase_tablespace_names(s, t), s.to_string());
        let s = "REINDEX (TABLESPACE pg_global) TABLE ea3_plain;";
        assert_eq!(rebase_tablespace_names(s, t), s.to_string());
        // Quoted material is opaque; unrelated statements never change.
        let s = "SELECT 'tablespace dd_ts stays', \"dd_ts\" FROM fz_rich;";
        assert_eq!(rebase_tablespace_names(s, t), s.to_string());
        let s = "SET allow_in_place_tablespaces = on;";
        assert_eq!(rebase_tablespace_names(s, t), s.to_string());
        let s = "ALTER TABLE fz_scalar RENAME TO zz;";
        assert_eq!(rebase_tablespace_names(s, t), s.to_string());
        // No operand: nothing to rewrite.
        assert_eq!(rebase_tablespace_names("DROP TABLESPACE ;", t), "DROP TABLESPACE ;");
        // 63-byte identifier bound holds.
        let long = rebase_tablespace_names(
            "DROP TABLESPACE abcdefghijklmnopqrstuvwxyz0123456789 ;",
            "fuzz_gramwalk_1234567_99",
        );
        let name = long.split_whitespace().nth(2).unwrap();
        assert!(name.len() <= 63, "{name}");
    }

    /// Textual stream invariants the rig relies on (single line, terminated,
    /// balanced parens — gram.y derivations always balance them).
    #[test]
    fn stream_invariants() {
        for sql in statements(7, 300) {
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
