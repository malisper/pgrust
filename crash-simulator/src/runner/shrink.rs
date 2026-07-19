//! Shrinker v1 (contract §4.1.5) — phase-1 heuristic only:
//!   1. truncate after the failing interaction;
//!   2. drop property spans whose table-dependency set (WS-GEN API; carried
//!      on the begin-property line in the scaffold format) does not
//!      intersect the failing span's set;
//!   3. keep faults / tx-control / arm-control steps unconditionally;
//!   4. keep the shrunk plan iff the re-run reproduces the SAME failure
//!      signature (never a weaker check).
//! Brute-force one-property-at-a-time removal is phase 2 (follow-on).

use super::driver::Signature;
use super::planface::{Plan, Step};
use std::collections::BTreeSet;

/// Property span: [begin_idx, end_idx] inclusive, with dependency tables.
#[derive(Debug, Clone)]
struct Span {
    begin: usize,
    end: usize,
    tables: BTreeSet<String>,
}

fn spans(plan: &Plan) -> Vec<Span> {
    let mut out = Vec::new();
    let mut open: Option<(usize, BTreeSet<String>)> = None;
    for (i, s) in plan.steps.iter().enumerate() {
        match s {
            Step::BeginProperty { tables, .. } => {
                open = Some((i, tables.iter().cloned().collect()));
            }
            Step::EndProperty { .. } => {
                if let Some((b, t)) = open.take() {
                    out.push(Span { begin: b, end: i, tables: t });
                }
            }
            _ => {}
        }
    }
    out
}

/// Compute the shrink candidate (pure; no re-run). Returns None if the
/// failure index is out of range.
pub fn shrink_candidate(plan: &Plan, failing_idx: usize) -> Option<Plan> {
    if failing_idx >= plan.steps.len() {
        return None;
    }
    let all = spans(plan);
    let failing_span = all.iter().find(|sp| sp.begin <= failing_idx && failing_idx <= sp.end);
    let failing_tables: BTreeSet<String> =
        failing_span.map(|sp| sp.tables.clone()).unwrap_or_default();

    // Step 1: truncate after the failing interaction.
    let keep_upto = failing_idx;

    // Step 2/3: decide survival per step.
    let mut drop_idx: BTreeSet<usize> = BTreeSet::new();
    for sp in &all {
        if sp.end > keep_upto {
            continue; // already truncated (or the failing span itself, kept)
        }
        let touches = !failing_tables.is_empty() && !sp.tables.is_disjoint(&failing_tables);
        let is_failing = sp.begin <= failing_idx && failing_idx <= sp.end;
        if touches || is_failing || failing_tables.is_empty() {
            continue; // keep: shares tables with the failure (or unknown deps: keep everything)
        }
        for i in sp.begin..=sp.end {
            match &plan.steps[i] {
                // Keep faults/tx/arm unconditionally even inside dropped spans.
                Step::Fault(_) | Step::Tx(_) | Step::Arm(_) => {}
                _ => {
                    drop_idx.insert(i);
                }
            }
        }
    }

    let steps: Vec<Step> = plan
        .steps
        .iter()
        .enumerate()
        .take(keep_upto + 1)
        .filter(|(i, _)| !drop_idx.contains(i))
        .map(|(_, s)| s.clone())
        .collect();
    Some(Plan { header: plan.header.clone(), steps })
}

/// Full shrink: candidate + same-signature re-run check via `rerun`.
/// `rerun(plan) -> Option<Signature>` returns the failure signature the
/// candidate produces (None = no failure). Keep iff identical signature.
pub fn shrink(
    plan: &Plan,
    failing_idx: usize,
    original_sig: &Signature,
    rerun: &mut dyn FnMut(&Plan) -> Option<Signature>,
) -> Option<Plan> {
    let cand = shrink_candidate(plan, failing_idx)?;
    if cand.steps.len() >= plan.steps.len() {
        return None; // no shrink achieved
    }
    match rerun(&cand) {
        Some(sig) if sig == *original_sig => Some(cand),
        _ => None, // signature changed or failure vanished: discard candidate
    }
}
