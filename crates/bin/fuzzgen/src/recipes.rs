//! recipes: the recipe bank loader (plan v2 §3.1 "recipes.rs", lane L0.6).
//!
//! The bank is the directory tree `recipes/<subsystem>/<cfile-stem>/
//! <id>.sql` (workspace-relative). `Bank::load` walks it deterministically
//! (sorted paths), parses every recipe with `recipe::Recipe::parse`,
//! validates that the header `id` equals the path-derived id
//! (`<subsystem>/<cfile-stem>/<stem>`), deduplicates by id (first path in
//! sort order wins, later twins are reported), and builds the indexes the
//! generator and the ledger use: by target unit id, by env cell, by origin
//! kind. Files at the wrong depth and parse failures are collected in
//! `errors`, never panicked on, so a half-authored recipe cannot take the
//! whole bank down; `Bank::load_strict` refuses a bank with any error.
//!
//! `Bank::regression_floor` selects the recipes whose `origin:` is
//! `audit` or `audit:<file>:<id>` — the 99 verified audit repros are the
//! regression floor (plan §5.4); they are replayed before every soak.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::contracts::Ordered;
use crate::recipe::Recipe;

/// One loaded recipe with its bank location.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BankEntry {
    /// Path relative to the bank root, `/`-separated.
    pub rel_path: String,
    pub subsystem: String,
    pub cfile: String,
    pub recipe: Recipe,
}

impl BankEntry {
    pub fn id(&self) -> &str {
        &self.recipe.header.id
    }

    /// `audit` | `mined` | `author` | `propagate` | `catalog` — the first
    /// `:`-separated segment of `origin:`.
    pub fn origin_kind(&self) -> &str {
        origin_kind(&self.recipe.header.origin)
    }
}

pub fn origin_kind(origin: &str) -> &str {
    origin.split(':').next().unwrap_or("").trim()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BankError {
    pub rel_path: String,
    pub message: String,
}

#[derive(Clone, Debug, Default)]
pub struct Bank {
    pub root: PathBuf,
    /// id → entry.
    pub recipes: BTreeMap<String, BankEntry>,
    /// unit id → recipe ids (sorted, deduplicated).
    pub by_target: BTreeMap<String, Vec<String>>,
    /// env cell → recipe ids.
    pub by_env: BTreeMap<String, Vec<String>>,
    /// origin kind → recipe ids.
    pub by_origin: BTreeMap<String, Vec<String>>,
    pub errors: Vec<BankError>,
}

fn walk_sql(dir: &Path, depth: usize, out: &mut Vec<(PathBuf, usize)>) -> std::io::Result<()> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)?.map(|e| e.map(|e| e.path())).collect::<Result<_, _>>()?;
    entries.sort();
    for p in entries {
        if p.is_dir() {
            walk_sql(&p, depth + 1, out)?;
        } else if p.extension().map(|e| e == "sql").unwrap_or(false) {
            out.push((p, depth));
        }
    }
    Ok(())
}

fn push_index(idx: &mut BTreeMap<String, Vec<String>>, key: &str, id: &str) {
    let v = idx.entry(key.to_string()).or_default();
    if !v.iter().any(|x| x == id) {
        v.push(id.to_string());
    }
}

impl Bank {
    /// Load the bank under `root`. Missing root ⇒ an empty bank (not an
    /// error: a fresh checkout before the first import).
    pub fn load(root: &Path) -> std::io::Result<Bank> {
        let mut bank = Bank { root: root.to_path_buf(), ..Default::default() };
        if !root.exists() {
            return Ok(bank);
        }
        let mut files = Vec::new();
        walk_sql(root, 0, &mut files)?;
        for (path, depth) in files {
            let rel = path.strip_prefix(root).unwrap_or(&path);
            let rel_path = rel.components().map(|c| c.as_os_str().to_string_lossy().into_owned()).collect::<Vec<_>>().join("/");
            if depth != 2 {
                bank.errors.push(BankError {
                    rel_path,
                    message: format!("recipe at depth {} (expected recipes/<subsystem>/<cfile-stem>/<id>.sql)", depth + 1),
                });
                continue;
            }
            let parts: Vec<&str> = rel_path.split('/').collect();
            let subsystem = parts[0].to_string();
            let cfile = parts[1].to_string();
            let stem = parts[2].trim_end_matches(".sql").to_string();
            let expected_id = format!("{}/{}/{}", subsystem, cfile, stem);
            let text = match std::fs::read_to_string(&path) {
                Ok(t) => t,
                Err(e) => {
                    bank.errors.push(BankError { rel_path, message: format!("read: {}", e) });
                    continue;
                }
            };
            let recipe = match Recipe::parse(&text) {
                Ok(r) => r,
                Err(e) => {
                    bank.errors.push(BankError { rel_path, message: format!("parse: {}", e) });
                    continue;
                }
            };
            if recipe.header.id != expected_id {
                bank.errors.push(BankError {
                    rel_path,
                    message: format!("header id {:?} does not match path id {:?}", recipe.header.id, expected_id),
                });
                continue;
            }
            if bank.recipes.contains_key(&expected_id) {
                bank.errors.push(BankError { rel_path, message: format!("duplicate recipe id {:?}", expected_id) });
                continue;
            }
            let entry = BankEntry { rel_path, subsystem, cfile, recipe };
            for t in &entry.recipe.header.targets {
                if t != "-" {
                    push_index(&mut bank.by_target, t, &expected_id);
                }
            }
            push_index(&mut bank.by_env, &entry.recipe.header.env, &expected_id);
            push_index(&mut bank.by_origin, entry.origin_kind(), &expected_id);
            bank.recipes.insert(expected_id, entry);
        }
        Ok(bank)
    }

    /// `load`, then refuse any error.
    pub fn load_strict(root: &Path) -> Result<Bank, String> {
        let bank = Bank::load(root).map_err(|e| format!("bank {}: {}", root.display(), e))?;
        if bank.errors.is_empty() {
            Ok(bank)
        } else {
            let mut msg = format!("bank {}: {} error(s)", root.display(), bank.errors.len());
            for e in &bank.errors {
                msg.push_str(&format!("\n  {}: {}", e.rel_path, e.message));
            }
            Err(msg)
        }
    }

    pub fn get(&self, id: &str) -> Option<&BankEntry> {
        self.recipes.get(id)
    }

    pub fn len(&self) -> usize {
        self.recipes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.recipes.is_empty()
    }

    fn select(&self, ids: Option<&Vec<String>>) -> Vec<&BankEntry> {
        ids.map(|v| v.iter().filter_map(|id| self.recipes.get(id)).collect()).unwrap_or_default()
    }

    /// Recipes targeting a unit id.
    pub fn for_target(&self, unit: &str) -> Vec<&BankEntry> {
        self.select(self.by_target.get(unit))
    }

    /// Recipes runnable in an env cell.
    pub fn for_env(&self, env: &str) -> Vec<&BankEntry> {
        self.select(self.by_env.get(env))
    }

    /// Recipes by origin kind (`audit`, `mined`, `author`, `propagate`, `catalog`).
    pub fn for_origin(&self, kind: &str) -> Vec<&BankEntry> {
        self.select(self.by_origin.get(kind))
    }

    /// The regression floor: every recipe of origin `audit` (plan §5.4).
    pub fn regression_floor(&self) -> Vec<&BankEntry> {
        self.for_origin("audit")
    }

    /// Unit ids covered by at least one recipe.
    pub fn targets(&self) -> Vec<&str> {
        self.by_target.keys().map(String::as_str).collect()
    }

    /// A short deterministic summary (counts per subsystem, env, origin,
    /// ordered summary, wire/non-wire).
    pub fn summary(&self) -> String {
        let mut by_sub: BTreeMap<&str, usize> = BTreeMap::new();
        let mut by_ordered: BTreeMap<&str, usize> = BTreeMap::new();
        let mut wire = 0usize;
        for e in self.recipes.values() {
            *by_sub.entry(&e.subsystem).or_default() += 1;
            *by_ordered.entry(e.recipe.header.ordered.as_str()).or_default() += 1;
            if e.recipe.is_wire() {
                wire += 1;
            }
        }
        let fmt = |m: &BTreeMap<&str, usize>| m.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(" ");
        let mut out = format!("recipes={} wire={} non_wire={} errors={}\n", self.len(), wire, self.len() - wire, self.errors.len());
        out.push_str(&format!("subsystem: {}\n", fmt(&by_sub)));
        out.push_str(&format!("env: {}\n", self.by_env.iter().map(|(k, v)| format!("{}={}", k, v.len())).collect::<Vec<_>>().join(" ")));
        out.push_str(&format!("origin: {}\n", self.by_origin.iter().map(|(k, v)| format!("{}={}", k, v.len())).collect::<Vec<_>>().join(" ")));
        out.push_str(&format!("ordered: {}\n", fmt(&by_ordered)));
        out.push_str(&format!("targets: {}\n", self.by_target.len()));
        out
    }

    /// Every recipe whose header `ordered:` disagrees with the value the
    /// parser pass computes (`Recipe::ordered_summary`).
    pub fn ordered_mismatches(&self) -> Vec<(String, Ordered, Ordered)> {
        self.recipes
            .values()
            .filter_map(|e| {
                let computed = e.recipe.ordered_summary();
                if computed != e.recipe.header.ordered {
                    Some((e.id().to_string(), e.recipe.header.ordered, computed))
                } else {
                    None
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn workspace_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..").canonicalize().unwrap()
    }

    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("fuzzgen-recipes-{}-{}", name, std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn write(root: &Path, rel: &str, id: &str, extra: &str, body: &str) {
        let p = root.join(rel);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        let text = format!(
            "-- id: {}\n-- targets: ereport:src/x.c:1\n-- env: base\n-- session: superuser\n-- protocol: simple\n-- ordered: total\n{}-- oracle: transcript\n-- origin: audit:errpath/x.verified.json:{}\n{}",
            id, extra, id, body
        );
        fs::write(p, text).unwrap();
    }

    #[test]
    fn load_indexes_dedupes_and_reports() {
        let root = scratch("load");
        write(&root, "audit/x/a-1.sql", "audit/x/a-1", "", "select 1;\n");
        write(&root, "audit/x/a-2.sql", "audit/x/a-2", "", "select 1;\n");
        // duplicate id under a different cfile dir → path/id mismatch
        write(&root, "audit/y/a-1.sql", "audit/x/a-1", "", "select 1;\n");
        // wrong depth
        write(&root, "audit/loose.sql", "audit/loose", "", "select 1;\n");
        // parse failure (unknown directive)
        write(&root, "author/z/bad.sql", "author/z/bad", "", "-- @bogus\nselect 1;\n");
        // an authored recipe in another env
        fs::write(
            root.join("author/z/ok.sql"),
            "-- id: author/z/ok\n-- targets: elog:src/y.c:9, ereport:src/x.c:1\n-- env: astm\n-- session: superuser\n-- protocol: simple\n-- ordered: total\n-- oracle: transcript\n-- origin: author\nselect a from t order by a;\n",
        )
        .unwrap();
        let bank = Bank::load(&root).unwrap();
        assert_eq!(bank.len(), 3, "{:?}", bank.errors);
        let msgs: Vec<String> = bank.errors.iter().map(|e| format!("{}: {}", e.rel_path, e.message)).collect();
        assert_eq!(bank.errors.len(), 3, "{:?}", msgs);
        assert!(msgs.iter().any(|m| m.starts_with("audit/y/a-1.sql: header id")));
        assert!(msgs.iter().any(|m| m.starts_with("audit/loose.sql: recipe at depth")));
        assert!(msgs.iter().any(|m| m.starts_with("author/z/bad.sql: parse:")));
        assert_eq!(bank.for_target("ereport:src/x.c:1").len(), 3);
        assert_eq!(bank.for_target("elog:src/y.c:9").len(), 1);
        assert_eq!(bank.for_env("astm").len(), 1);
        assert_eq!(bank.for_env("base").len(), 2);
        assert_eq!(bank.regression_floor().iter().map(|e| e.id()).collect::<Vec<_>>(), vec!["audit/x/a-1", "audit/x/a-2"]);
        assert_eq!(bank.for_origin("author").len(), 1);
        assert!(bank.load_strict_err());
        assert!(bank.ordered_mismatches().is_empty());
        assert!(bank.summary().starts_with("recipes=3 wire=3 non_wire=0 errors=3\n"));
        let _ = fs::remove_dir_all(&root);
    }

    impl Bank {
        fn load_strict_err(&self) -> bool {
            Bank::load_strict(&self.root).is_err()
        }
    }

    #[test]
    fn missing_root_is_empty() {
        let bank = Bank::load(Path::new("/nonexistent/recipes-bank")).unwrap();
        assert!(bank.is_empty() && bank.errors.is_empty());
    }

    /// The committed bank: every file parses, round-trips byte-for-byte,
    /// carries the id its path implies, and its header `ordered:` equals
    /// the value the parser pass computes (the importer and this crate
    /// implement the same rule).
    #[test]
    fn committed_bank_is_canonical() {
        let root = workspace_root().join("recipes");
        assert!(root.exists(), "recipes/ bank missing at {}", root.display());
        let bank = Bank::load_strict(&root).unwrap();
        assert!(bank.len() >= 90, "bank has {} recipes", bank.len());
        for e in bank.recipes.values() {
            let text = fs::read_to_string(root.join(&e.rel_path)).unwrap();
            assert_eq!(e.recipe.render(), text, "{} is not canonical", e.rel_path);
            let again = Recipe::parse(&e.recipe.render()).unwrap();
            assert_eq!(again, e.recipe, "{} does not round-trip", e.rel_path);
            assert!(!e.recipe.to_steps("bank", 0).is_empty(), "{} has no steps", e.rel_path);
            let origin = &e.recipe.header.origin;
            assert!(origin.starts_with("audit:") || origin.starts_with("author:"), "{} origin {:?}", e.rel_path, origin);
            if e.subsystem == "composition" {
                assert!(origin.starts_with("author:composition"), "{} origin {:?}", e.rel_path, origin);
                assert!(e.recipe.note_features().len() >= 2, "{} composes fewer than two features", e.rel_path);
            }
        }
        let mism = bank.ordered_mismatches();
        assert!(mism.is_empty(), "ordered mismatches: {:?}", mism);
        assert_eq!(bank.regression_floor().len(), bank.for_origin("audit").len());
        assert!(bank.regression_floor().len() >= 90);
        // The composition bank (RECIPES.md "Composition bank"): at least 40
        // authored recipes, each composing two or more features.
        assert!(bank.for_origin("author").len() >= 40, "composition bank has {} recipes", bank.for_origin("author").len());
    }
}
