//! Known-wart allowlist loader (contract §3.1.5) — warts.toml.
//!
//! Checked-in, schema'd entries for adjudicated non-bugs and harness limits.
//! Matchers are SQLSTATE and/or a normalized-statement regex — NEVER raw
//! error text vs C (text drift is its own class). A wart hit is counted
//! (`SIMHARNESS|wart:<id>|n`) — suppressed, never invisible. Entries may be
//! proposed by any WS, merged only by WS-ORACLE with evidence.

use std::path::Path;

use regex::Regex;
use serde::Deserialize;

use crate::vocab::Class;

#[derive(Debug, Deserialize)]
struct WartFile {
    #[serde(default)]
    wart: Vec<WartEntry>,
}

#[derive(Debug, Deserialize)]
struct WartEntry {
    id: String,
    /// Which vocabulary class this suppresses (must name a pinned class).
    class: String,
    /// Optional SQLSTATE matcher: exact ("23505") or class prefix ("23").
    #[serde(default)]
    sqlstate: Option<String>,
    /// Optional regex over the whitespace-normalized statement.
    #[serde(default)]
    stmt_pattern: Option<String>,
    /// Commit/note reference for the adjudication.
    evidence: String,
    /// Review-by train tag (e.g. "t30") or "standing".
    expires: String,
}

#[derive(Debug)]
pub struct Wart {
    pub id: String,
    pub class: Class,
    pub sqlstate: Option<String>,
    pub stmt_pattern: Option<Regex>,
    pub evidence: String,
    pub expires: String,
}

impl Wart {
    /// Train-tag expiry: "standing" never expires; "tNN" expires when the
    /// current train tag is >= it. Unparseable tags never expire (lint job).
    pub fn expired(&self, current_train: &str) -> bool {
        fn tag_num(s: &str) -> Option<u64> {
            s.strip_prefix('t')?.parse().ok()
        }
        if self.expires == "standing" {
            return false;
        }
        match (tag_num(&self.expires), tag_num(current_train)) {
            (Some(exp), Some(cur)) => cur >= exp,
            _ => false,
        }
    }
}

#[derive(Debug, Default)]
pub struct Warts {
    entries: Vec<Wart>,
}

fn normalize_stmt(stmt: &str) -> String {
    let mut out = String::with_capacity(stmt.len());
    let mut ws = false;
    for c in stmt.trim().chars() {
        if c.is_whitespace() {
            ws = true;
        } else {
            if ws && !out.is_empty() {
                out.push(' ');
            }
            ws = false;
            out.push(c);
        }
    }
    out
}

impl Warts {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("read {}: {e}", path.display()))?;
        Self::parse(&text)
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let file: WartFile = toml::from_str(text).map_err(|e| format!("warts.toml: {e}"))?;
        let mut entries = Vec::new();
        for e in file.wart {
            let class = Class::from_str(&e.class)
                .ok_or_else(|| format!("wart {}: unknown class '{}'", e.id, e.class))?;
            if e.sqlstate.is_none() && e.stmt_pattern.is_none() {
                return Err(format!(
                    "wart {}: needs at least one matcher (sqlstate or stmt_pattern)",
                    e.id
                ));
            }
            let stmt_pattern = match &e.stmt_pattern {
                None => None,
                Some(p) => Some(
                    Regex::new(p)
                        .map_err(|err| format!("wart {}: bad stmt_pattern: {err}", e.id))?,
                ),
            };
            entries.push(Wart {
                id: e.id,
                class,
                sqlstate: e.sqlstate,
                stmt_pattern,
                evidence: e.evidence,
                expires: e.expires,
            });
        }
        Ok(Warts { entries })
    }

    pub fn entries(&self) -> &[Wart] {
        &self.entries
    }

    /// First matching wart for a finding, if any. All present matchers must
    /// hit (AND semantics): class always; sqlstate by exact-or-class-prefix;
    /// stmt_pattern over the whitespace-normalized statement.
    pub fn match_finding(&self, class: Class, sqlstate: Option<&str>, stmt: &str) -> Option<&Wart> {
        let norm = normalize_stmt(stmt);
        self.entries.iter().find(|w| {
            if w.class != class {
                return false;
            }
            if let Some(ws) = &w.sqlstate {
                match sqlstate {
                    None => return false,
                    Some(s) => {
                        let hit = if ws.len() == 2 {
                            s.starts_with(ws.as_str())
                        } else {
                            s == ws
                        };
                        if !hit {
                            return false;
                        }
                    }
                }
            }
            if let Some(re) = &w.stmt_pattern {
                if !re.is_match(&norm) {
                    return false;
                }
            }
            true
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
[[wart]]
id = "todate-era-fetch"
class = "harness-fetch"
stmt_pattern = "(?i)to_date\\s*\\(|to_timestamp\\s*\\(|\\bBC\\b"
evidence = "020d147b7"
expires = "t30"

[[wart]]
id = "dup-key-known"
class = "err-state-mismatch"
sqlstate = "23"
evidence = "example"
expires = "standing"
"#;

    #[test]
    fn parses_and_matches() {
        let w = Warts::parse(SAMPLE).unwrap();
        assert_eq!(w.entries().len(), 2);
        let hit = w.match_finding(
            Class::HarnessFetch,
            None,
            "select   to_date('0001-01-01 BC', 'YYYY-MM-DD BC')",
        );
        assert_eq!(hit.map(|x| x.id.as_str()), Some("todate-era-fetch"));
        // non-matching: right class, wrong statement
        assert!(w.match_finding(Class::HarnessFetch, None, "select 1").is_none());
        // non-matching: right statement, wrong class
        assert!(w
            .match_finding(Class::WrongResults, None, "select to_date('x','y')")
            .is_none());
        // sqlstate class-prefix matcher
        assert!(w
            .match_finding(Class::ErrStateMismatch, Some("23505"), "insert into t values (1)")
            .is_some());
        assert!(w
            .match_finding(Class::ErrStateMismatch, Some("42601"), "insert into t values (1)")
            .is_none());
    }

    #[test]
    fn rejects_unknown_class_and_matcherless() {
        assert!(Warts::parse(
            "[[wart]]\nid='x'\nclass='no-such-class'\nsqlstate='23'\nevidence='e'\nexpires='standing'\n"
        )
        .is_err());
        assert!(Warts::parse(
            "[[wart]]\nid='x'\nclass='ok'\nevidence='e'\nexpires='standing'\n"
        )
        .is_err());
    }

    #[test]
    fn expiry() {
        let w = Warts::parse(SAMPLE).unwrap();
        assert!(!w.entries()[0].expired("t29"));
        assert!(w.entries()[0].expired("t30"));
        assert!(!w.entries()[1].expired("t99"));
    }
}
