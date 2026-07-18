//! H5 rung B: plan-species census + Good-Turing residual-risk bound, and the
//! per-campaign metrics.json assembly (rung A's k-path report rides in here
//! too).
//!
//! Species = a canonicalized query-plan fingerprint per executed statement,
//! collected via `EXPLAIN (COSTS OFF)` against the DUT. Canonicalization
//! follows QPG's discipline — keep the node tree + operator classes, strip
//! relation/index names (Ba & Rigger, *Testing Database Engines via Query
//! Plan Guidance*, ICSE 2023: plans canonicalized "by stripping table/view/
//! index names"; unique-plan coverage is the DBMS-native exploration signal).
//! Literals never appear in structural EXPLAIN lines (they live on Filter:/
//! Cond: detail lines, which are dropped), so literal changes cannot mint a
//! species while plan-shape changes (Seq Scan -> Index Scan, Hash Join ->
//! Nested Loop, Gather appearing) do.
//!
//! Residual risk: STADS Good-Turing U(n) = f1/n — the number of species seen
//! exactly once over the number of sightings — estimates the probability the
//! NEXT statement exhibits a brand-new plan species, and upper-bounds the
//! probability that the next statement is the first bug-revealing one
//! (Böhme, *STADS*, ACM TOSEM 2018).
//!
//! ## BLIND (BLACKBOX) ARMS ONLY — read this before reusing these numbers
//!
//! These estimators are sound for the simharness precisely because it is a
//! BLACKBOX generator: no coverage feedback, no seed corpus evolution, every
//! seed drawn independently. Verbatim from the verified source (Böhme,
//! Liyanage & Wüstholz, *Estimating Residual Risk in Greybox Fuzzing*,
//! ESEC/FSE 2021): "blackbox fuzzing is not subject to adaptive bias and the
//! probability to generate a bug-revealing input remains constant throughout
//! the fuzzing campaign." The SAME paper proves that once a campaign steers
//! itself by what it found (greybox/guided arms), these classical estimators
//! "underestimate the true discovery probability ... up to four orders of
//! magnitude". ANY future guided arm (plan-steered generation, coverage
//! feedback) MUST be excluded from these statistics — or switch to the
//! corrected (mean-local) estimators the same day.
//!
//! Honesty seams (carried from docs/research/fuzzer-exploration-2026-07-18*):
//! - Plugging the plan fingerprint into Good-Turing AS the species is OUR
//!   composition of two verified results (QPG validates plan coverage; STADS
//!   validates f1/n for blackbox) — no single paper reports it. Flagged in
//!   metrics.json as `species_composition: "inference"`.
//! - U bounds unexplored GROUND, not correctness: a wrong-result bug in an
//!   already-seen plan species (our differential oracle's specialty) is
//!   invisible to this number.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::Path;

use crate::gen::prodreg::KpathReport;

// ---------------------------------------------------------------------------
// EXPLAIN canonicalization -> plan fingerprint
// ---------------------------------------------------------------------------

/// Canonicalize one EXPLAIN (COSTS OFF) text output into a species
/// fingerprint. Structural rows are the first line and every `->` line;
/// detail rows (Filter:, Sort Key:, Index Cond:, Workers Planned:, one-off
/// SubPlan headers, ...) are dropped. Node text is cut at ` on ` / ` using `
/// so relation, alias, and index names never enter the fingerprint. Nesting
/// is reconstructed from indentation into a stable parenthesized tree.
pub fn canonicalize_explain(lines: &[String]) -> String {
    struct Node {
        name: String,
        children: Vec<Node>,
    }
    fn render(n: &Node, out: &mut String) {
        out.push_str(&n.name);
        if !n.children.is_empty() {
            out.push('(');
            for (i, c) in n.children.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                render(c, out);
            }
            out.push(')');
        }
    }

    let mut root: Option<Node> = None;
    // Stack of (indent-of-arrow, node-path index chain) — we store the chain
    // of child indexes to reach the current attachment point.
    let mut stack: Vec<(usize, Vec<usize>)> = Vec::new();

    for raw in lines {
        let line = raw.as_str();
        let trimmed = line.trim_start();
        let indent = line.len() - trimmed.len();
        let (is_struct, name_part) = if let Some(rest) = trimmed.strip_prefix("->  ") {
            (true, rest)
        } else if root.is_none() && !trimmed.is_empty() && indent == 0 {
            (true, trimmed)
        } else {
            (false, "")
        };
        if !is_struct {
            continue;
        }
        let name = canonical_node_name(name_part);
        if root.is_none() {
            root = Some(Node { name, children: Vec::new() });
            stack.push((0, Vec::new()));
            continue;
        }
        let root_node = root.as_mut().expect("set above");
        // Pop to the nearest ancestor with strictly smaller indent.
        while stack.len() > 1 && stack.last().map(|(i, _)| *i >= indent).unwrap_or(false) {
            stack.pop();
        }
        let parent_chain = stack.last().map(|(_, c)| c.clone()).unwrap_or_default();
        let mut cur: &mut Node = root_node;
        for i in &parent_chain {
            cur = &mut cur.children[*i];
        }
        cur.children.push(Node { name, children: Vec::new() });
        let mut chain = parent_chain;
        chain.push(cur.children.len() - 1);
        stack.push((indent, chain));
    }

    match root {
        None => "<no-plan>".to_string(),
        Some(r) => {
            let mut out = String::new();
            render(&r, &mut out);
            out
        }
    }
}

/// Node-name canonicalization: cut at ` on ` (relation + alias) and
/// ` using ` (index name), strip a trailing cost/rows parenthetical if the
/// caller ran without COSTS OFF. Keeps operator-class-bearing prefixes
/// (Parallel, Finalize/Partial, join sides) — parallelism and join strategy
/// ARE plan shape.
fn canonical_node_name(s: &str) -> String {
    let s = s.split(" (cost=").next().unwrap_or(s);
    let s = s.split("  (").next().unwrap_or(s);
    let s = s.split(" using ").next().unwrap_or(s);
    let s = s.split(" on ").next().unwrap_or(s);
    s.trim_end().to_string()
}

// ---------------------------------------------------------------------------
// Species census + Good-Turing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct SpeciesCensus {
    /// fingerprint -> sighting count.
    pub counts: BTreeMap<String, u64>,
    /// total sightings n.
    pub n: u64,
    /// species-accumulation curve: (sightings, distinct species) checkpoints
    /// (one per seed in campaign use).
    pub curve: Vec<(u64, u64)>,
    /// H6: singleton counts at the same checkpoints (parallel to `curve`),
    /// so U(n) = f1/n can be plotted OVER TIME, not just at campaign end.
    pub curve_f1: Vec<u64>,
    /// EXPLAIN statements that errored (counted here, never in the outcome
    /// census — metrics must not perturb the pinned verdict vocabulary).
    pub explain_errors: u64,
    /// First few EXPLAIN error signatures (diagnosability; a DUT EXPLAIN
    /// coverage gap is itself a finding).
    pub explain_error_samples: Vec<String>,
}

impl SpeciesCensus {
    pub fn add_sighting(&mut self, fingerprint: &str) {
        *self.counts.entry(fingerprint.to_string()).or_insert(0) += 1;
        self.n += 1;
    }

    pub fn checkpoint(&mut self) {
        self.curve.push((self.n, self.distinct() as u64));
        self.curve_f1.push(self.f1());
    }

    /// U-over-time: (n, f1/n) per checkpoint (n = 0 checkpoints skipped).
    pub fn u_curve(&self) -> Vec<(u64, f64)> {
        self.curve
            .iter()
            .zip(self.curve_f1.iter())
            .filter(|((n, _), _)| *n > 0)
            .map(|((n, _), f1)| (*n, *f1 as f64 / *n as f64))
            .collect()
    }

    pub fn distinct(&self) -> usize {
        self.counts.len()
    }

    /// f1: species seen exactly once (singletons).
    pub fn f1(&self) -> u64 {
        self.counts.values().filter(|c| **c == 1).count() as u64
    }

    /// f2: species seen exactly twice (doubletons).
    pub fn f2(&self) -> u64 {
        self.counts.values().filter(|c| **c == 2).count() as u64
    }

    /// Good-Turing discovery probability U(n) = f1/n (STADS, TOSEM 2018):
    /// the estimated probability that the next sighting is a NEW species;
    /// an upper bound on P(next input is the first bug-revealing one).
    /// None when n = 0 (no samples -> no estimate, never a fake 0).
    pub fn good_turing_u(&self) -> Option<f64> {
        if self.n == 0 {
            None
        } else {
            Some(self.f1() as f64 / self.n as f64)
        }
    }

    /// Chao1 species-richness lower bound: S + f1^2/(2*f2) (f2>0), else the
    /// bias-corrected S + f1*(f1-1)/2. G-hat = S/Chao1 is then an OPTIMISTIC
    /// species-coverage estimate (Chao1 is a lower bound on true richness).
    pub fn chao1(&self) -> Option<f64> {
        if self.n == 0 {
            return None;
        }
        let s = self.distinct() as f64;
        let f1 = self.f1() as f64;
        let f2 = self.f2() as f64;
        Some(if f2 > 0.0 { s + f1 * f1 / (2.0 * f2) } else { s + f1 * (f1 - 1.0) / 2.0 })
    }

    pub fn merge(&mut self, other: &SpeciesCensus) {
        for (k, v) in &other.counts {
            *self.counts.entry(k.clone()).or_insert(0) += v;
        }
        self.n += other.n;
        self.explain_errors += other.explain_errors;
    }
}

// ---------------------------------------------------------------------------
// metrics.json assembly
// ---------------------------------------------------------------------------

pub struct MetricsArtifact<'a> {
    pub kpath: &'a KpathReport,
    pub species: &'a SpeciesCensus,
    /// EXPLAIN sampling: every Nth executed query is fingerprinted (0 = off).
    pub explain_sample_every: u32,
    pub profile_name: &'a str,
    pub seed_base: u64,
    pub seed_count: u64,
    /// H6: QPG-lite scheduling stats. `enabled` inside decides the arm kind:
    /// a GUIDED arm gets its Good-Turing U suppressed (FSE'21 adaptive bias —
    /// U is only sound on blind arms).
    pub schedule: Option<&'a crate::runner::schedule::ScheduleStats>,
}

impl MetricsArtifact<'_> {
    pub fn to_json(&self) -> serde_json::Value {
        let s = self.species;
        let top: Vec<(String, u64)> = {
            let mut v: Vec<(String, u64)> =
                s.counts.iter().map(|(k, c)| (k.clone(), *c)).collect();
            v.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            v.truncate(20);
            v
        };
        let guided = self.schedule.map(|st| st.enabled).unwrap_or(false);
        // FSE'21 blind-arm law: a guided arm's U is UNSOUND (adaptive bias,
        // underestimates by up to 4 orders of magnitude). Suppress the value
        // outright so nobody quotes it, and say why in-band.
        let (u_value, u_note) = if guided {
            (
                serde_json::Value::Null,
                Some(
                    "SUPPRESSED: guided (species-scheduled) arm. Good-Turing U=f1/n \
                     is only sound on BLIND arms (FSE 2021 adaptive-bias result); \
                     run a flag-off arm to estimate U.",
                ),
            )
        } else {
            (serde_json::json!(self.species.good_turing_u()), None)
        };
        serde_json::json!({
            "blind_arm": !guided,
            "estimator_notes": {
                "soundness": "Good-Turing U=f1/n is sound for BLACKBOX campaigns only \
                    (Böhme/Liyanage/Wüstholz, FSE 2021: blackbox fuzzing is not subject \
                    to adaptive bias). Guided/feedback arms MUST be excluded or switch \
                    to corrected estimators.",
                "species_composition": "inference — plan-fingerprint-as-species composes \
                    QPG (ICSE 2023, plan coverage) with STADS (TOSEM 2018, f1/n); \
                    no single paper reports this composition.",
                "bound_scope": "U bounds unexplored ground, not correctness; wrong-result \
                    bugs in already-seen species are invisible to it.",
            },
            "kpath": self.kpath,
            "reach_gate": {
                "red": !self.kpath.reach_gap.is_empty(),
                "uncovered_default_reachable": self.kpath.reach_gap,
            },
            "species": {
                "n": s.n,
                "distinct": s.distinct(),
                "f1": s.f1(),
                "f2": s.f2(),
                "good_turing_u": u_value,
                "good_turing_u_note": u_note,
                "chao1_richness_lower_bound": s.chao1(),
                "curve": s.curve,
                // H6 over-time views: S(n) is `curve` verbatim; U(n)=f1/n at
                // the same checkpoints. On guided arms the u curve carries
                // the same unsoundness as the endpoint U — the note above
                // governs both.
                "u_curve": s.u_curve(),
                "explain_sample_every": self.explain_sample_every,
                "explain_errors": s.explain_errors,
                "explain_error_samples": s.explain_error_samples,
                "top_species": top,
            },
            "schedule": self.schedule.map(|st| serde_json::json!({
                "enabled": st.enabled,
                "neighbors": st.neighbors,
                "decay": st.decay,
                "seeds_total": st.seeds_total,
                "fresh_seeds": st.fresh_seeds,
                "guided_seeds": st.guided_seeds,
                "productive_seeds": st.productive_seeds,
                "productive_guided": st.productive_guided,
                "productive_fraction": st.productive_fraction(),
                "decays": st.decays,
            })),
            "campaign": {
                "profile": self.profile_name,
                "seed_base": self.seed_base,
                "seed_count": self.seed_count,
            },
        })
    }

    /// Write `metrics.json` + the full `species.tsv` table into `out_dir`.
    pub fn write(&self, out_dir: &Path) -> Result<(), String> {
        std::fs::create_dir_all(out_dir).map_err(|e| e.to_string())?;
        std::fs::write(
            out_dir.join("metrics.json"),
            serde_json::to_string_pretty(&self.to_json()).map_err(|e| e.to_string())? + "\n",
        )
        .map_err(|e| e.to_string())?;
        let mut f = std::fs::File::create(out_dir.join("species.tsv"))
            .map_err(|e| e.to_string())?;
        writeln!(f, "count\tfingerprint").map_err(|e| e.to_string())?;
        let mut rows: Vec<(&String, &u64)> = self.species.counts.iter().collect();
        rows.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
        for (fp, c) in rows {
            writeln!(f, "{c}\t{fp}").map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// One-line stdout summaries (`SIMHARNESS-METRICS|...`), distinct from
    /// the pinned `SIMHARNESS|class|n` census grammar.
    pub fn emit_stdout(&self) {
        let k = self.kpath;
        println!(
            "SIMHARNESS-METRICS|kpath|k1={}/{}|k2={}/{}|k3={}/{}|reach-gap={}",
            k.k1.covered,
            k.k1.total,
            k.k2.covered,
            k.k2.total,
            k.k3.covered,
            k.k3.total,
            k.reach_gap.len()
        );
        let s = self.species;
        let guided = self.schedule.map(|st| st.enabled).unwrap_or(false);
        let u_str = if guided {
            // FSE'21: guided arms must never quote U.
            "UNSOUND-guided-arm".to_string()
        } else {
            s.good_turing_u().map(|u| format!("{u:.6}")).unwrap_or_else(|| "-".into())
        };
        println!(
            "SIMHARNESS-METRICS|species|n={}|distinct={}|f1={}|f2={}|U={}",
            s.n,
            s.distinct(),
            s.f1(),
            s.f2(),
            u_str
        );
        if let Some(st) = self.schedule {
            println!(
                "SIMHARNESS-METRICS|schedule|enabled={}|fresh={}|guided={}|productive={}|productive-guided={}|fraction={}|decays={}",
                st.enabled,
                st.fresh_seeds,
                st.guided_seeds,
                st.productive_seeds,
                st.productive_guided,
                st.productive_fraction()
                    .map(|f| format!("{f:.4}"))
                    .unwrap_or_else(|| "-".into()),
                st.decays
            );
        }
        if !k.reach_gap.is_empty() {
            println!("SIMHARNESS-METRICS|reach-gap|{}", k.reach_gap.join(","));
        }
    }
}

/// Census class minted when the reach gate is RED (`vocab` pins it P1).
/// Distinct from bug findings: it means the CAMPAIGN could not exercise a
/// production the profile says is reachable — a harness/generator defect
/// class (the H3 0/9 shape), not an engine-divergence class.
pub const REACH_GAP: &str = "reach-gap";
