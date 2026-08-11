//! Session plumbing: (seed, toggle vector, statement budget) → deterministic
//! statement stream with per-statement production metadata.

use crate::catalog::Catalog;
use crate::expr::ExprGen;
use crate::render::gen_select;
use crate::rng::Rng;
use crate::toggles::ToggleVector;

#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub seed: u64,
    pub toggles: ToggleVector,
    pub budget: u32,
    pub max_depth: u32,
}

#[derive(Clone, Debug)]
pub struct Statement {
    pub stmt_index: u32,
    pub sql: String,
    /// Sorted, deduplicated production names that fired for this statement.
    pub productions: Vec<String>,
}

/// Generate the full statement stream for a session. Same config (seed,
/// toggles, budget, depth) + same catalog = byte-identical output.
pub fn run_session(cfg: &SessionConfig, catalog: &Catalog) -> Vec<Statement> {
    let mut rng = Rng::new(cfg.seed);
    let mut out = Vec::with_capacity(cfg.budget as usize);
    for stmt_index in 0..cfg.budget {
        let module = cfg.toggles.pick_module(&mut rng);
        let mut productions = Vec::new();
        let sql = match module {
            "expr" => {
                let table = &catalog.tables[rng.below_usize(catalog.tables.len())];
                let mut g = ExprGen {
                    rng: &mut rng,
                    table,
                    productions: &mut productions,
                };
                gen_select(&mut g, cfg.max_depth).to_sql()
            }
            other => unreachable!("module {} registered but not implemented", other),
        };
        productions.sort();
        productions.dedup();
        out.push(Statement { stmt_index, sql, productions });
    }
    out
}

/// Minimal JSON string escaping for JSONL output.
pub fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// One JSONL record: {"seed":..,"stmt_index":..,"sql":"..","productions":[..]}
pub fn jsonl_record(seed: u64, stmt: &Statement) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{{\"seed\":{},\"stmt_index\":{},\"sql\":\"{}\",\"productions\":[",
        seed,
        stmt.stmt_index,
        json_escape(&stmt.sql)
    ));
    for (i, p) in stmt.productions.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        out.push_str(&json_escape(p));
        out.push('"');
    }
    out.push_str("]}");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};

    fn cfg(seed: u64) -> SessionConfig {
        SessionConfig {
            seed,
            toggles: ToggleVector::all_on(),
            budget: 50,
            max_depth: 4,
        }
    }

    #[test]
    fn same_seed_byte_identical() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let a = run_session(&cfg(1234), &cat);
        let b = run_session(&cfg(1234), &cat);
        let render = |stmts: &[Statement]| {
            stmts
                .iter()
                .map(|s| jsonl_record(1234, s))
                .collect::<Vec<_>>()
                .join("\n")
        };
        assert_eq!(render(&a), render(&b));
    }

    #[test]
    fn different_seeds_differ() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let a = run_session(&cfg(1), &cat);
        let b = run_session(&cfg(2), &cat);
        let sqls = |stmts: &[Statement]| stmts.iter().map(|s| s.sql.clone()).collect::<Vec<_>>();
        assert_ne!(sqls(&a), sqls(&b));
    }

    #[test]
    fn budget_and_metadata() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let stmts = run_session(&cfg(77), &cat);
        assert_eq!(stmts.len(), 50);
        for (i, s) in stmts.iter().enumerate() {
            assert_eq!(s.stmt_index as usize, i);
            assert!(s.productions.contains(&"select".to_string()));
            // Sorted + deduped.
            let mut sorted = s.productions.clone();
            sorted.sort();
            sorted.dedup();
            assert_eq!(sorted, s.productions);
        }
        // Across a 50-statement session the core productions all fire.
        let all: Vec<String> = stmts.iter().flat_map(|s| s.productions.clone()).collect();
        for prefix in ["colref", "lit:", "case", "cmp:", "cast:"] {
            assert!(
                all.iter().any(|p| p.starts_with(prefix)),
                "production {} never fired in 50 statements",
                prefix
            );
        }
    }

    #[test]
    fn jsonl_escapes_quotes() {
        let s = Statement {
            stmt_index: 0,
            sql: "SELECT '\"';".to_string(),
            productions: vec!["lit:text".to_string()],
        };
        let rec = jsonl_record(9, &s);
        assert!(rec.contains("\\\""));
        assert!(rec.starts_with("{\"seed\":9,"));
    }
}
