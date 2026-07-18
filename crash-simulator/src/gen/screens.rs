//! Screen LINT — the mechanical backstop for the generation-time screens
//! (contract §2.1.3; provenance notes/wrongresults-triage.md via spec §3.2).
//!
//! The generator enforces R2/R3/R6/R7 at the source; this lint re-checks the
//! rendered plan so the smoke gate (G-G2) catches any future grammar change
//! that silently violates a screen. Each rule has a planted-red unit test.

use std::collections::BTreeMap;

use crate::plan::{Plan, PlanItem, Step};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// R2 | R3R6 | R7
    pub rule: &'static str,
    pub detail: String,
}

/// Volatile / nondeterministic / engine-constant-metadata function names that
/// must never appear in compared positions (R3/R6 backstop; superset of the
/// triage.py VOLATILE_RE/NONDET_RE families, incl. the lo_creat/lo_import
/// lesson).
const FORBIDDEN_FNS: &[&str] = &[
    "random",
    "setseed",
    "now",
    "transaction_timestamp",
    "statement_timestamp",
    "clock_timestamp",
    "timeofday",
    "localtimestamp",
    "nextval",
    "currval",
    "lastval",
    "setval",
    "txid_current",
    "pg_current_xact_id",
    "pg_backend_pid",
    "pg_sleep",
    "version",
    "current_setting",
    "pg_conf_load_time",
    "pg_postmaster_start_time",
    "inet_client_addr",
    "inet_client_port",
    "inet_server_addr",
    "inet_server_port",
    "lo_creat",
    "lo_create",
    "lo_import",
    "gen_random_uuid",
    "uuid_generate_v4",
];

/// Order-sensitive float-reduction aggregates (R7). max/min are deliberately
/// absent: they are order-insensitive.
const FLOAT_SENSITIVE_AGGS: &[&str] = &["sum", "avg", "stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop"];

/// Tokenize SQL into lowercase words annotated with paren depth, skipping
/// single-quoted string literals ('' escape) — the quote-aware paren-depth
/// scan (triage.py post-f19d9db69 discipline).
fn words_with_depth(text: &str) -> Vec<(String, usize)> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut in_str = false;
    let mut cur = String::new();
    let mut cur_depth = 0usize;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if in_str {
            if c == '\'' {
                if chars.peek() == Some(&'\'') {
                    chars.next();
                } else {
                    in_str = false;
                }
            }
            continue;
        }
        match c {
            '\'' => {
                if !cur.is_empty() {
                    out.push((std::mem::take(&mut cur), cur_depth));
                }
                in_str = true;
            }
            '(' => {
                if !cur.is_empty() {
                    out.push((std::mem::take(&mut cur), cur_depth));
                }
                depth += 1;
            }
            ')' => {
                if !cur.is_empty() {
                    out.push((std::mem::take(&mut cur), cur_depth));
                }
                depth = depth.saturating_sub(1);
            }
            c if c.is_ascii_alphanumeric() || c == '_' => {
                if cur.is_empty() {
                    cur_depth = depth;
                }
                cur.push(c.to_ascii_lowercase());
            }
            _ => {
                if !cur.is_empty() {
                    out.push((std::mem::take(&mut cur), cur_depth));
                }
            }
        }
    }
    if !cur.is_empty() {
        out.push((cur, cur_depth));
    }
    out
}

/// Is word `i` a function call, i.e. immediately(-ish) followed by `(` in the
/// raw text? Approximated: the next word (if any) is at a deeper depth, or the
/// word is the last token and text contains "word(" — we just re-scan the raw
/// lowered text for "word" followed by optional spaces and '('.
fn is_called(text_lower: &str, name: &str) -> bool {
    let mut start = 0;
    while let Some(pos) = text_lower[start..].find(name) {
        let at = start + pos;
        let before_ok = at == 0
            || !text_lower[..at]
                .chars()
                .next_back()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_');
        let after = &text_lower[at + name.len()..];
        let after_trim = after.trim_start();
        if before_ok && after_trim.starts_with('(') {
            return true;
        }
        start = at + name.len();
    }
    false
}

/// R2: LIMIT/OFFSET requires a same-depth ORDER BY, unless the Sql is marked
/// order-underdetermined.
fn check_r2(text: &str, order_underdetermined: bool, out: &mut Vec<Violation>) {
    if order_underdetermined {
        return;
    }
    let words = words_with_depth(text);
    let mut order_by_depths: Vec<usize> = Vec::new();
    for i in 0..words.len() {
        if words[i].0 == "order" && words.get(i + 1).map(|w| w.0.as_str()) == Some("by") {
            order_by_depths.push(words[i].1);
        }
    }
    for (w, d) in &words {
        if (w == "limit" || w == "offset") && !order_by_depths.contains(d) {
            out.push(Violation {
                rule: "R2",
                detail: format!("{} at depth {d} without same-depth ORDER BY: {text}", w.to_uppercase()),
            });
        }
    }
}

/// R3/R6: no volatile / metadata functions in compared positions.
fn check_r3_r6(text: &str, out: &mut Vec<Violation>) {
    let lower = text.to_ascii_lowercase();
    for f in FORBIDDEN_FNS {
        if is_called(&lower, f) {
            out.push(Violation {
                rule: "R3R6",
                detail: format!("volatile/metadata function {f}() in compared position: {text}"),
            });
        }
    }
}

/// Column-type map reconstructed from the plan's own CREATE TABLE / RENAME
/// DDL (our DDL renders in a fixed shape, so this parse is exact for
/// generator output; for foreign SQL it is best-effort — the lint is a
/// backstop, not a general SQL analyzer).
fn schema_of(plan: &Plan) -> BTreeMap<String, BTreeMap<String, String>> {
    let mut tables: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let steps = plan.items.iter().flat_map(|it| match it {
        PlanItem::Step(s) => std::slice::from_ref(s).iter(),
        PlanItem::Property { steps, .. } => steps.iter(),
    });
    for step in steps {
        let Step::Ddl(sqlv) = step else { continue };
        let text = sqlv.text();
        if let Some(rest) = text.strip_prefix("CREATE TABLE ") {
            let Some((name, cols)) = rest.split_once(" (") else { continue };
            let cols = cols.trim_end_matches(';').trim_end_matches(')');
            let mut colmap = BTreeMap::new();
            for coldef in cols.split(", ") {
                let mut parts = coldef.split(' ');
                if let (Some(cn), Some(ct)) = (parts.next(), parts.next()) {
                    colmap.insert(cn.to_string(), ct.to_string());
                }
            }
            tables.insert(name.to_string(), colmap);
        } else if let Some(rest) = text.strip_prefix("ALTER TABLE ") {
            if let Some((old, new)) = rest.split_once(" RENAME TO ") {
                let new = new.trim_end_matches(';');
                if let Some(cols) = tables.remove(old) {
                    tables.insert(new.to_string(), cols);
                }
            }
        } else if let Some(rest) = text.strip_prefix("DROP TABLE ") {
            tables.remove(rest.trim_end_matches(';'));
        }
    }
    tables
}

/// R7: order-sensitive aggregate over a float column (or an explicit ::float8
/// cast) in a compared position requires the float-lenient tag.
fn check_r7(
    text: &str,
    float_lenient: bool,
    tables: &BTreeMap<String, BTreeMap<String, String>>,
    out: &mut Vec<Violation>,
) {
    if float_lenient {
        return;
    }
    let lower = text.to_ascii_lowercase();
    let words = words_with_depth(text);
    // Find the FROM table (single-table grammar; best-effort for foreign SQL).
    let mut from_table: Option<&str> = None;
    for i in 0..words.len() {
        if words[i].0 == "from" {
            if let Some((t, _)) = words.get(i + 1) {
                from_table = Some(t.as_str());
            }
        }
    }
    let float_cols: Vec<&String> = from_table
        .and_then(|t| tables.get(t))
        .map(|cols| {
            cols.iter().filter(|(_, ty)| ty.as_str() == "float8").map(|(c, _)| c).collect()
        })
        .unwrap_or_default();
    for agg in FLOAT_SENSITIVE_AGGS {
        if !is_called(&lower, agg) {
            continue;
        }
        // Words at depth >= 1 immediately after the agg word are its args.
        for i in 0..words.len() {
            if words[i].0 == *agg {
                let agg_depth = words[i].1;
                let mut j = i + 1;
                while j < words.len() && words[j].1 > agg_depth {
                    let arg = &words[j].0;
                    if arg == "float8" || float_cols.iter().any(|c| *c == arg) {
                        out.push(Violation {
                            rule: "R7",
                            detail: format!(
                                "float aggregate {agg}({arg}) in compared position without float-lenient tag: {text}"
                            ),
                        });
                    }
                    j += 1;
                }
            }
        }
    }
}

/// Lint every compared position (Query steps) of a plan.
pub fn lint_plan(plan: &Plan) -> Vec<Violation> {
    let tables = schema_of(plan);
    let mut out = Vec::new();
    let steps = plan.items.iter().flat_map(|it| match it {
        PlanItem::Step(s) => std::slice::from_ref(s).iter(),
        PlanItem::Property { steps, .. } => steps.iter(),
    });
    for step in steps {
        let Step::Query(q) = step else { continue };
        check_r2(q.text(), q.flags.order_underdetermined, &mut out);
        check_r3_r6(q.text(), &mut out);
        check_r7(q.text(), q.flags.float_lenient, &tables, &mut out);
    }
    out
}
