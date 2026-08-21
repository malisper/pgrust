//! Path election (memo §2.2): counted over the chunk's JEntry trees,
//! deterministic (counts + canonical total-order tie-break), capped.

use crate::manifest::{ElectedPath, Lane, ShredManifest};
use crate::path::JsonPath;
use crate::walk::{walk_row, PathValue};
use adt_jsonb::container::JsonbItem;
use std::collections::BTreeMap;

/// Election caps (memo D-C4 defaults). All are election-time inputs; the
/// facts a later chunk needs (max_depth, nbuckets) ride the manifest.
#[derive(Clone, Debug)]
pub struct ShredBudgets {
    /// `jsonb_max_paths`: elected substreams per column, hints included.
    pub max_paths: usize,
    /// `max_depth`: longest electable key chain.
    pub max_depth: u8,
    /// `jsonb_residual_buckets`.
    pub nbuckets: u8,
    /// `jsonb_min_presence` divisor: floor = max(1, nrows / div) — the
    /// anti-CH-Object guard (a 10k-unique-path stream elects nothing).
    pub min_presence_div: u32,
}

impl Default for ShredBudgets {
    fn default() -> ShredBudgets {
        ShredBudgets {
            max_paths: 64,
            max_depth: 4,
            nbuckets: 8,
            min_presence_div: 64,
        }
    }
}

/// A typed-path hint (memo §2.3): a standing offer, not an assertion — a
/// hinted path is elected unconditionally with the hinted lane; values that
/// never verify simply produce exception-heavy chunks, never errors.
#[derive(Clone, Debug)]
pub struct PathHint {
    pub path: JsonPath,
    pub lane: Lane,
}

#[derive(Default)]
struct PathStats {
    presence: u64,
    nstring: u64,
    nnumber: u64,
    nbool: u64,
    // jsonb nulls count presence (they occupy the position) but never a lane
    // (§2.3: `->` must distinguish jsonb null from absent; exceptions always).
}

impl PathStats {
    fn majority_lane(&self) -> Option<Lane> {
        // Fixed tie-break order Text > NumericFs > Bool: strict-greater
        // comparisons keep earlier classes on ties (deterministic).
        let (mut lane, mut best) = (None, 0u64);
        for (n, l) in [
            (self.nstring, Lane::Text),
            (self.nnumber, Lane::NumericFs),
            (self.nbool, Lane::Bool),
        ] {
            if n > best {
                best = n;
                lane = Some(l);
            }
        }
        lane
    }
}

/// Elect a manifest over one chunk of rows (container payloads, headers
/// stripped). Deterministic: identical rows + hints + budgets yield an
/// identical manifest, so serial re-ingest of identical data yields
/// identical parts (the e2e byte-parity property).
///
/// Election caps beyond the memo's, both flagged in the C0 PR:
/// - the elected set is PREFIX-FREE (no elected path is a strict prefix of
///   another). With prefix-freeness, an elected path's value — when the row
///   has one — is always either in its typed lane or in the residual at
///   exactly that path (the §2.4 exception rule swallows whole subtrees at
///   elected paths, so a nested elected descendant could otherwise lose its
///   rows into an ancestor's exception images). First-admitted wins: hints
///   in caller order, then frequency order.
/// - hints deeper than max_depth are refused (the walker would never reach
///   them; electing them would create permanently-dead lanes).
pub fn elect_manifest(
    rows: &[&[u8]],
    hints: &[PathHint],
    budgets: &ShredBudgets,
) -> ShredManifest {
    let mut stats: BTreeMap<JsonPath, PathStats> = BTreeMap::new();
    for payload in rows {
        walk_row(payload, budgets.max_depth, &mut |_| false, &mut |segs, v| {
            if segs.is_empty() {
                return; // non-object root: never electable, rides residual
            }
            let entry = stats
                .entry(JsonPath::from_borrowed(segs))
                .or_default();
            entry.presence += 1;
            if let PathValue::Scalar(item) = v {
                match item {
                    JsonbItem::String(_) => entry.nstring += 1,
                    JsonbItem::Numeric(_) => entry.nnumber += 1,
                    JsonbItem::Bool(_) => entry.nbool += 1,
                    _ => {}
                }
            }
        });
    }

    let mut manifest = ShredManifest::new(budgets.nbuckets, budgets.max_depth);

    // Admission with the prefix-freeness cap.
    let mut admitted: Vec<(JsonPath, Lane, bool, u64)> = Vec::new();
    let admit = |cand: &JsonPath, lane: Lane, hinted: bool, presence: u64,
                     admitted: &mut Vec<(JsonPath, Lane, bool, u64)>| {
        if admitted.len() >= budgets.max_paths {
            return;
        }
        let conflict = admitted.iter().any(|(p, ..)| {
            p == cand || p.is_strict_prefix_of(cand) || cand.is_strict_prefix_of(p)
        });
        if !conflict {
            admitted.push((cand.clone(), lane, hinted, presence));
        }
    };

    for hint in hints {
        if hint.path.is_root() || hint.path.depth() > budgets.max_depth as usize {
            continue;
        }
        let presence = stats.get(&hint.path).map_or(0, |s| s.presence);
        admit(&hint.path, hint.lane, true, presence, &mut admitted);
    }

    let floor = 1.max(rows.len() as u64 / budgets.min_presence_div.max(1) as u64);
    let mut candidates: Vec<(&JsonPath, &PathStats)> = stats
        .iter()
        .filter(|(_, s)| s.presence >= floor)
        .filter(|(_, s)| s.nstring + s.nnumber + s.nbool > 0)
        .collect();
    // Descending presence, canonical path order on ties (BTreeMap iteration
    // already yields canonical order; the sort is stable).
    candidates.sort_by(|a, b| b.1.presence.cmp(&a.1.presence));
    for (path, s) in candidates {
        if admitted.len() >= budgets.max_paths {
            break;
        }
        let Some(lane) = s.majority_lane() else {
            continue;
        };
        admit(path, lane, false, s.presence, &mut admitted);
    }

    // Intern every observed path (elected and residual share one dictionary,
    // §2.5) plus hinted-but-unobserved paths; then the elected rows.
    for (path, _) in &stats {
        manifest.intern(path.clone());
    }
    for (path, ..) in &admitted {
        manifest.intern(path.clone());
    }
    // Elected order: canonical path order (deterministic; substream_idx is
    // the position).
    admitted.sort_by(|a, b| a.0.cmp(&b.0));
    for (idx, (path, lane, hinted, presence)) in admitted.into_iter().enumerate() {
        let path_id = manifest.lookup(&path).expect("interned above");
        manifest.elected.push(ElectedPath {
            path_id,
            lane,
            substream_idx: idx as u16,
            hinted,
            presence,
        });
    }
    manifest.residual_substream_base = manifest.elected.len() as u16;
    manifest
}
