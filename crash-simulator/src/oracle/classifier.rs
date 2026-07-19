//! C-differential classifier (`--diff-c`) — contract §3.1.4.
//!
//! A statement-level port of scripts/sqlsmith/triage.py @ ef070d066:
//! the same screens (VOLATILE_RE / NONDET_RE / COVERAGE_RE /
//! ORDER_SENSITIVE_AGG_RE / the quote-aware paren-depth `underdetermined`
//! scan) and the same decision ladder, emitting the pinned vocab classes.
//! The G-O2 parity gate (tests/parity/) holds this port equal to triage.py
//! on real corpora — any drift is a gate failure, not a judgment call.
//!
//! Comparison ladder posture: byte-compare is digest equality; the semantic
//! rung is the sort-normalized multiset digest (relaxed order law is the
//! ratified product default — wrongresults R1 — so order divergence alone is
//! never a finding here); errors compare by SQLSTATE class. Mutation-outcome
//! divergence => HALT (state forked; the dualexec --cpg law, kept verbatim).

use std::sync::OnceLock;

use regex::Regex;

use crate::oracle::pstep::Mark;
use crate::oracle::warts::Warts;
use crate::vocab::{Class, Severity};

// ---- screens, verbatim from triage.py @ ef070d066 (single-sourced there;
// these are the compare-side backstop — generation-side screens are WS-GEN's) ----

fn volatile_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?i)\b(random|setseed|now|nextval|currval|lastval|clock_timestamp|statement_timestamp|transaction_timestamp|timeofday|current_date|current_time|current_timestamp|localtime|localtimestamp|current_user|session_user|current_setting|version|pg_[a-z_]+|txid_[a-z_]+|inet_client_addr|inet_client_port|inet_server_addr|inet_server_port|gen_random_uuid|has_[a-z_]+_privilege|obj_description|col_description|shobj_description|row_security_active|format_type|regexp_[a-z_]*|lo_creat|lo_create|lo_import|lo_unlink|loread|lowrite|lo_open)\s*\(",
        )
        .expect("VOLATILE_RE compiles")
    })
}

fn coverage_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)\bunported\b|not (yet )?ported|unimplemented|not (yet )?supported|internal error")
            .expect("COVERAGE_RE compiles")
    })
}

fn nondet_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?i)tablesample|uuidv\d|gen_random_uuid|icu_unicode_version|current_query|\brandom\b|setseed",
        )
        .expect("NONDET_RE compiles")
    })
}

fn order_sensitive_agg_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?i)\b(?:json_agg|jsonb_agg|json_agg_strict|jsonb_agg_strict|array_agg|string_agg|xmlagg|json_object_agg[a-z_]*|jsonb_object_agg[a-z_]*)\s*\(",
        )
        .expect("ORDER_SENSITIVE_AGG_RE compiles")
    })
}

fn limit_offset_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\blimit\s+\d+|\boffset\s+\d+").expect("compiles"))
}

fn order_by_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\border\s+by\b").expect("compiles"))
}

pub fn is_volatile(stmt: &str) -> bool {
    volatile_re().is_match(stmt)
}
pub fn is_nondet(stmt: &str) -> bool {
    nondet_re().is_match(stmt)
}
pub fn is_coverage_msg(msg: &str) -> bool {
    coverage_re().is_match(msg)
}

/// Port of triage.underdetermined(): true when the statement's correct
/// answer is not unique — LIMIT/OFFSET without a same-depth ORDER BY, or an
/// order-sensitive aggregate without an aggregate-level ORDER BY.
/// Byte-based scan (decision-equivalent to the Python char scan: depth only
/// changes at ASCII parens/quotes).
pub fn underdetermined(stmt: &str) -> bool {
    let s = stmt.to_lowercase();
    let b = s.as_bytes();
    let mut depths: Vec<i32> = Vec::with_capacity(b.len());
    let mut d: i32 = 0;
    let mut in_sq = false;
    for &c in b {
        if in_sq {
            depths.push(d);
            if c == b'\'' {
                in_sq = false;
            }
            continue;
        }
        if c == b'\'' {
            in_sq = true;
        } else if c == b'(' {
            d += 1;
        } else if c == b')' {
            d -= 1;
        }
        depths.push(d);
    }
    for m in limit_offset_re().find_iter(&s) {
        let pos = m.start();
        let dep = depths[pos];
        let mut start = 0usize;
        let mut i = pos as i64 - 1;
        while i >= 0 {
            if depths[i as usize] < dep {
                start = i as usize + 1;
                break;
            }
            i -= 1;
        }
        let determined = order_by_re()
            .find_iter(&s[start..pos])
            .any(|om| depths[start + om.start()] == dep);
        if !determined {
            return true;
        }
    }
    for m in order_sensitive_agg_re().find_iter(&s) {
        let open_paren = m.end() - 1;
        let mut dep = 0i32;
        let mut e = open_paren;
        while e < b.len() {
            if b[e] == b'(' {
                dep += 1;
            } else if b[e] == b')' {
                dep -= 1;
                if dep == 0 {
                    break;
                }
            }
            e += 1;
        }
        let end = e.min(s.len());
        if !order_by_re().is_match(&s[open_paren..end]) {
            return true;
        }
    }
    false
}

// ---- per-statement outcome types (the runner feeds these) ----

/// Sort-normalized result digest, mirroring triage.py's
/// (ncols, rowcount, hash, capped) tuple. `norm_hash` is over the
/// row-multiset canonical form (each engine's digest computed identically by
/// the same runner, so equality semantics match). `raw_hash` optionally
/// carries the pre-sort byte form for a future strict order-law mode; it
/// does not participate in classification (relaxed is the product default).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Digest {
    pub ncols: u32,
    pub nrows: i64,
    pub norm_hash: String,
    pub capped: bool,
    pub raw_hash: Option<String>,
}

impl Digest {
    fn cmp_key(&self) -> (u32, i64, &str, bool) {
        (self.ncols, self.nrows, self.norm_hash.as_str(), self.capped)
    }
}

/// One engine's outcome for one statement (triage.py Engine.run statuses).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunStatus {
    Ok(Digest),
    Error { sqlstate: Option<String>, msg: String },
    /// Client-side fetch failure on a healthy connection (harness-fetch).
    Fetch { msg: String },
    Crash { msg: String },
}

/// Compute a stable digest for a normalized row multiset. sha2 over the
/// canonical form (rows stringified, sorted) — process-stable, so verdict
/// streams are byte-identical across runs (G-O4).
pub fn digest_rows(ncols: u32, rows: &[Vec<Option<String>>], capped: bool) -> Digest {
    use sha2::{Digest as _, Sha256};
    if capped {
        return Digest { ncols, nrows: -1, norm_hash: "capped".into(), capped: true, raw_hash: None };
    }
    let mut norm: Vec<Vec<String>> = rows
        .iter()
        .map(|r| r.iter().map(|v| v.clone().unwrap_or_else(|| "\\N".into())).collect())
        .collect();
    let mut raw_hasher = Sha256::new();
    for r in &norm {
        for v in r {
            raw_hasher.update(v.as_bytes());
            raw_hasher.update([0u8]);
        }
        raw_hasher.update([1u8]);
    }
    let raw = format!("{:x}", raw_hasher.finalize());
    norm.sort();
    let mut hasher = Sha256::new();
    for r in &norm {
        for v in r {
            hasher.update(v.as_bytes());
            hasher.update([0u8]);
        }
        hasher.update([1u8]);
    }
    Digest {
        ncols,
        nrows: rows.len() as i64,
        norm_hash: format!("{:x}", hasher.finalize()),
        capped: false,
        raw_hash: Some(raw),
    }
}

/// The triage.py classify() ladder, verbatim.
pub fn classify(stmt: &str, rust: &RunStatus, cpg: &RunStatus) -> (Class, Severity) {
    let nd = is_nondet(stmt);
    let cls = match (rust, cpg) {
        (RunStatus::Crash { .. }, _) => Class::RustCrash,
        (_, RunStatus::Crash { .. }) => Class::CCrash,
        (RunStatus::Fetch { .. }, _) | (_, RunStatus::Fetch { .. }) => Class::HarnessFetch,
        (RunStatus::Ok(rd), RunStatus::Ok(cd)) => {
            if rd.capped || cd.capped {
                Class::BigResultSkipped
            } else if rd.cmp_key() == cd.cmp_key() {
                Class::Ok
            } else if nd {
                Class::WrongResultsNondet
            } else if is_volatile(stmt) {
                Class::WrongResultsVolatile
            } else if underdetermined(stmt) {
                Class::WrongResultsUnderdetermined
            } else {
                Class::WrongResults
            }
        }
        (RunStatus::Error { sqlstate, msg }, RunStatus::Ok(_)) => {
            let state = sqlstate.as_deref().unwrap_or("");
            if state.starts_with("XX") && is_coverage_msg(msg) {
                Class::RustErrCOkCoverage
            } else if nd && !state.starts_with("XX") {
                Class::RustErrCOkNondet
            } else {
                Class::RustErrCOk
            }
        }
        (RunStatus::Ok(_), RunStatus::Error { .. }) => {
            if nd {
                Class::CErrRustOkNondet
            } else {
                Class::CErrRustOk
            }
        }
        (
            RunStatus::Error { sqlstate: rstate, msg: rmsg },
            RunStatus::Error { sqlstate: cstate, msg: cmsg },
        ) => {
            let rs = rstate.as_deref().unwrap_or("??");
            let cs = cstate.as_deref().unwrap_or("??");
            if rs.get(..2) == cs.get(..2) {
                if rstate == cstate && rmsg != cmsg {
                    Class::ErrTextDrift
                } else {
                    Class::ErrMatch
                }
            } else if rs == "57014" || cs == "57014" {
                Class::ErrTimeoutOneSide
            } else {
                Class::ErrStateMismatch
            }
        }
    };
    (cls, cls.severity())
}

// ---- step-level verdict with the mutation-divergence HALT law ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffStepVerdict {
    pub class: Class,
    pub severity: Severity,
    /// Mutation-outcome divergence => state forked; the runner must HALT the
    /// plan and record the halt index (dualexec --cpg law).
    pub halt: bool,
    /// Wart-allowlist hit id, when the finding is suppressed (counted via
    /// `SIMHARNESS|wart:<id>|n`, never invisible). Suppression downgrades
    /// severity to Fine; it never clears `halt`.
    pub wart: Option<String>,
}

fn mutation_diverged(rust: &RunStatus, cpg: &RunStatus) -> bool {
    match (rust, cpg) {
        (RunStatus::Ok(rd), RunStatus::Ok(cd)) => rd.cmp_key() != cd.cmp_key(),
        (RunStatus::Error { sqlstate: rs, .. }, RunStatus::Error { sqlstate: cs, .. }) => {
            rs.as_deref().unwrap_or("??").get(..2) != cs.as_deref().unwrap_or("??").get(..2)
        }
        _ => true, // ok-vs-err, crash, fetch: outcome kinds differ => forked
    }
}

pub fn classify_step(
    mark: Mark,
    stmt: &str,
    rust: &RunStatus,
    cpg: &RunStatus,
    warts: &Warts,
) -> DiffStepVerdict {
    let (class, severity) = classify(stmt, rust, cpg);
    let halt = mark == Mark::Mutation && mutation_diverged(rust, cpg);
    let sqlstate = match rust {
        RunStatus::Error { sqlstate, .. } => sqlstate.as_deref(),
        _ => None,
    };
    match warts.match_finding(class, sqlstate, stmt) {
        Some(w) => DiffStepVerdict {
            class,
            severity: Severity::Fine,
            halt,
            wart: Some(w.id.clone()),
        },
        None => DiffStepVerdict { class, severity, halt, wart: None },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(h: &str) -> RunStatus {
        RunStatus::Ok(Digest { ncols: 1, nrows: 3, norm_hash: h.into(), capped: false, raw_hash: None })
    }
    fn err(state: &str, msg: &str) -> RunStatus {
        RunStatus::Error { sqlstate: Some(state.into()), msg: msg.into() }
    }

    #[test]
    fn ladder_matches_triage() {
        let w = Warts::empty();
        let _ = &w;
        let s = "select a from t";
        assert_eq!(classify(s, &ok("h"), &ok("h")).0, Class::Ok);
        assert_eq!(classify(s, &ok("h1"), &ok("h2")).0, Class::WrongResults);
        assert_eq!(
            classify("select random() from t", &ok("h1"), &ok("h2")).0,
            Class::WrongResultsNondet
        );
        assert_eq!(
            classify("select version() from t", &ok("h1"), &ok("h2")).0,
            Class::WrongResultsVolatile
        );
        assert_eq!(
            classify("select a from t limit 1", &ok("h1"), &ok("h2")).0,
            Class::WrongResultsUnderdetermined
        );
        assert_eq!(
            classify(s, &err("XX000", "not yet ported: foo"), &ok("h")).0,
            Class::RustErrCOkCoverage
        );
        assert_eq!(classify(s, &err("42601", "syntax"), &ok("h")).0, Class::RustErrCOk);
        assert_eq!(classify(s, &ok("h"), &err("22012", "div")).0, Class::CErrRustOk);
        assert_eq!(
            classify(s, &err("42601", "syntax a"), &err("42601", "syntax b")).0,
            Class::ErrTextDrift
        );
        assert_eq!(
            classify(s, &err("42601", "x"), &err("42601", "x")).0,
            Class::ErrMatch
        );
        assert_eq!(
            classify(s, &err("42601", "x"), &err("42P01", "y")).0,
            Class::ErrMatch,
            "same 2-char class 42 => err-match per triage"
        );
        assert_eq!(
            classify(s, &err("57014", "x"), &err("22012", "y")).0,
            Class::ErrTimeoutOneSide
        );
        assert_eq!(
            classify(s, &err("23505", "x"), &err("22012", "y")).0,
            Class::ErrStateMismatch
        );
        assert_eq!(
            classify(s, &RunStatus::Crash { msg: "boom".into() }, &ok("h")).0,
            Class::RustCrash
        );
        assert_eq!(
            classify(s, &ok("h"), &RunStatus::Crash { msg: "boom".into() }).0,
            Class::CCrash
        );
        assert_eq!(
            classify(s, &RunStatus::Fetch { msg: "bc date".into() }, &ok("h")).0,
            Class::HarnessFetch
        );
    }

    #[test]
    fn underdetermined_cases() {
        // triage docstring cases
        assert!(underdetermined("select a from t limit 1"));
        assert!(underdetermined("select (select c from u limit 1 offset 3) from t order by a"));
        assert!(!underdetermined("select a from t order by a limit 1"));
        // ORDER BY at a shallower depth does not determinize an inner LIMIT
        assert!(underdetermined("select * from (select a from t limit 2) s order by 1"));
        // order-sensitive aggregate without inner ORDER BY
        assert!(underdetermined("select array_agg(a) from t"));
        assert!(!underdetermined("select array_agg(a order by a) from t"));
        // Bug-for-bug parity with triage.py: quotes shield PAREN DEPTH only;
        // the LIMIT regex still scans inside string literals, so this is
        // (conservatively) underdetermined there and must be here too.
        assert!(underdetermined("select 'limit 1' from t"));
        // ...but a quoted paren does not corrupt the depth scan.
        assert!(!underdetermined("select ')' , a from t order by a limit 1"));
    }

    #[test]
    fn mutation_halt_law() {
        let w = Warts::empty();
        let v = classify_step(
            Mark::Mutation,
            "insert into t values (1)",
            &RunStatus::Ok(Digest { ncols: 0, nrows: 1, norm_hash: "a".into(), capped: false, raw_hash: None }),
            &RunStatus::Error { sqlstate: Some("23505".into()), msg: "dup".into() },
            &w,
        );
        assert!(v.halt, "mutation ok-vs-err forks state => HALT");
        let v2 = classify_step(
            Mark::Read,
            "select * from t",
            &ok("h1"),
            &ok("h2"),
            &w,
        );
        assert!(!v2.halt, "read divergence never halts");
    }
}
