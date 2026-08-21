//! FAMILY ELECTION — engine logic separated from the rig's SQL parser
//! (port-study/port-map.md: "elect_family is engine logic wearing rig
//! clothes — it moves WITH the engine; it will be re-fed from lowered PG
//! plans at P2-1"). Every rule fires on SHAPE properties + bank STATS
//! (dict/code-domain availability, stats-answerability, exact domain
//! bounds, the frame-pair shape). No query identity anywhere.

use crate::bank::Bank;
use crate::engine::Faces;
use crate::planner::AFamily;
use crate::refuse::Refuse;

/// Parser-independent shape facts the election reads. The rig derives
/// these from its SQL AST; P2-1 derives them from PG plan trees.
#[derive(Debug, Clone, Default)]
pub struct ShapeFacts {
    pub has_contains: bool,
    pub has_count_distinct: bool,
    /// how many aggregates are COUNT(DISTINCT ..) — the distinct-pipeline
    /// family election keys on the whole agg set being distinct-shaped
    /// (hot-shape lineage), never on one leg in an agg mix (hot-shape rides
    /// the hash plane per the RON law).
    pub n_count_distinct: usize,
    /// [aggqual] how many aggregates are general distinct folds
    /// (sum/avg DISTINCT). Ungrouped they ride the distinct pipeline
    /// with the count legs; grouped they elect the hash plane's
    /// dense-distinct form. Default 0 keeps every election identical.
    pub n_distinct_general: usize,
    /// [aggqual] any aggregate carries a FILTER (WHERE ...) — the
    /// stats plane is falsified per-leg exactly like a predicate, and
    /// the ungrouped serve is the filtered scalar fold even without a
    /// statement conjunct. Default false keeps elections identical.
    pub has_agg_filters: bool,
    pub n_aggs: usize,
    /// every aggregate answerable from the flat stats plane (CountStar,
    /// or Sum/Avg/Min/Max over a byval column)
    pub aggs_all_stats_answerable: bool,
    pub aggs_all_count_star: bool,
    pub proj_cols: Vec<u32>,
    pub pred_cols: Vec<u32>,
    pub ne_empty_cols: Vec<u32>,
    /// one Eq-nonzero + one Range in the conjunction
    pub frame_pair: bool,
    pub group: Vec<u32>,
    /// Group-key EXPRESSIONS when any key is not a plain column (empty =
    /// `group` are the keys). Additive: an empty vector keeps every
    /// existing election byte-identical.
    pub key_exprs: Vec<crate::planner::AKeyExpr>,
    pub npreds: usize,
    /// [sqe-avglen] the fused entry-length mix: exactly one
    /// AVG(octet_length(x)) leg (the rest COUNT(*)) whose input x is
    /// ALSO the query's one `<> ''` conjunct — the fold's len!=0 gate
    /// consumes the predicate, so the two-level entry-length arm may
    /// own the shape.
    pub avglen_ne_fused: bool,
    /// every predicate is a `= 0` / `<> 0` zero-count witness
    pub preds_all_zero_witness: bool,
    pub has_order: bool,
    pub n_order_keys: usize,
    pub has_limit: bool,
    /// [sortgrp v1] how many aggregates are order-sensitive/holistic
    /// tier-2 legs (string_agg/array_agg with ORDER BY, the WITHIN GROUP
    /// ordered-set class). The SortGrouped election keys on the WHOLE
    /// agg set being tier-2 (the distinct-pipeline all-or-nothing law);
    /// a mix refuses at lowering (`sortagg-unsupported:agg-mix`) — never
    /// a one-leg hijack. Default 0 keeps every existing election
    /// byte-identical.
    pub n_order_aggs: usize,
}

/// Elect the stencil family from shape + stats. The fired rule + its
/// stats inputs trace under the phase flag (the rig's traceability gate
/// arms it; gate runs keep stdout clean — risks.md §7).
pub fn elect_family(
    bank: &Bank,
    faces: &Faces,
    s: &ShapeFacts,
    tag: &str,
) -> Result<AFamily, Refuse> {
    let trace = |fam: &str, rule: &str, stats: String| {
        if crate::engine::phase_on() {
            println!("SQEELECT|lower|{tag}|family={fam}|rule={rule}|{stats}");
        }
    };
    // R0 [sortgrp v1]: ANY order-sensitive aggregate elects the tier-2
    // SortGrouped family (grouped and ungrouped alike) — the shape fact
    // IS the physical law (a per-group sort exists in every lawful
    // serve). Mixed agg sets refuse downstream at lowering; predicate
    // conjuncts ride the family's own pass-A filter, so this rule
    // precedes the predicate-keyed arms.
    if s.n_order_aggs > 0 {
        trace(
            "SortGrouped",
            "order_sensitive_aggs",
            format!("n_order_aggs={} n_aggs={}", s.n_order_aggs, s.n_aggs),
        );
        return Ok(AFamily::SortGrouped);
    }
    // R1: varlena Contains/NotContains conjuncts -> the verdict/condcache
    // family (dict-entry verdict lowering; predicate ladder step 2).
    if s.has_contains {
        trace("WindowReplay", "contains_pred", format!("var_pred_cols={:?}", s.pred_cols));
        return Ok(AFamily::WindowReplay);
    }
    if s.group.is_empty() {
        // R2: bare projection + single int equality -> answer plane with
        // the EmitMatches reconstruction rewrite (plan_from_ap owns it).
        if s.n_aggs == 0 && !s.has_order {
            trace("MetadataAnswer", "eq_projection", format!("proj={:?}", s.proj_cols));
            return Ok(AFamily::MetadataAnswer);
        }
        // R3: every aggregate stats-answerable and the predicate (if any)
        // is a =0/<>0 zero-count witness -> metadata answer, zero rows.
        if s.n_aggs > 0
            && s.aggs_all_stats_answerable
            && !s.has_agg_filters
            && s.ne_empty_cols.is_empty()
            && s.npreds <= 1
            && s.preds_all_zero_witness
        {
            trace(
                "MetadataAnswer",
                "stats_answerable",
                format!("aggs={} pred_zero_witness={}", s.n_aggs, s.npreds > 0),
            );
            return Ok(AFamily::MetadataAnswer);
        }
        // R4: COUNT(DISTINCT) -> the distinct pipeline (scatter-own-dedupe;
        // never a merged set — the negative-ledger law). Same whole-agg-set
        // law as the grouped arm (R6): a mixed distinct/plain agg set falls
        // through, never a one-leg hijack.
        // [aggqual] the whole-agg-set law extends to the general
        // distinct folds: an all-distinct set (count/sum/avg DISTINCT)
        // rides the pipeline's one dedup; filters never compose here.
        if (s.n_count_distinct + s.n_distinct_general) == s.n_aggs
            && s.n_aggs > 0
            && (s.has_count_distinct || s.n_distinct_general > 0)
            && !s.has_agg_filters
        {
            trace("DistinctPipeline", "count_distinct", String::new());
            return Ok(AFamily::DistinctPipeline);
        }
        // R3b (P3 rung B): ungrouped aggregates under a REAL predicate ->
        // the fused filter-agg scalar fold. Fires only after R3 declined,
        // so the zero-witness count(*) fast path (metaanswer) is retained;
        // this is the rule that REPLACES the C8 filtered-stats refusal
        // (min/max/sum/avg under a predicate) with real execution.
        // [aggqual] a per-agg FILTER is a predicate the stats plane
        // cannot answer — the fold serves it even with zero statement
        // conjuncts.
        if s.n_aggs > 0 && (s.npreds >= 1 || s.has_agg_filters) && s.ne_empty_cols.is_empty()
        {
            trace(
                "FusedFilterAgg",
                "filtered_scalar_fold",
                format!("aggs={} npreds={} filters={}", s.n_aggs, s.npreds, s.has_agg_filters),
            );
            return Ok(AFamily::FusedFilterAgg);
        }
        // R5: no aggregates, ORDER BY col LIMIT k -> zone-order/dict-head
        // walk (the standing order structures family).
        if s.n_aggs == 0 && s.has_order && s.has_limit {
            trace("ZoneOrderWalk", "projection_topk", format!("order_keys={}", s.n_order_keys));
            return Ok(AFamily::ZoneOrderWalk);
        }
        return Err(Refuse::NoFamilyRule { what: "ungrouped-shape" });
    }
    // grouped shapes ------------------------------------------------------
    // R6: grouped COUNT(DISTINCT) elects the distinct pipeline only when
    // the WHOLE agg set is distinct-shaped. An agg MIX
    // carrying one distinct leg (hot-shape: sum/count/avg + count-distinct) rides
    // the hash plane — the RON plan of record's election (PLANDIFF close).
    // [aggqual] a general distinct fold (sum/avg DISTINCT) grouped
    // elects the hash plane's dense-distinct form — the DistinctPipeline
    // grouped arms answer (key, count) only.
    if s.n_distinct_general > 0 {
        trace(
            "HashPlaneOwnedGroup",
            "grouped_distinct_general",
            format!("n_distinct_general={}", s.n_distinct_general),
        );
        return Ok(AFamily::HashPlaneOwnedGroup);
    }
    if s.has_count_distinct && s.n_count_distinct == s.n_aggs {
        trace("DistinctPipeline", "grouped_count_distinct", String::new());
        return Ok(AFamily::DistinctPipeline);
    }
    // [sqe-expr-keys] expression-key classes elect on the KEY-EXPR shape
    // (never query identity), ahead of the plain-column arms whose facts
    // (text-ness, frame pair) would misroute the derived grouping:
    //   - a single derived-text key (regexp-extract class) with the
    //     `<> ''` residue on its source -> the per-dict-entry derivation
    //     fold;
    //   - a single minute-bucket key under a frame pair -> the dense
    //     minute-domain fold;
    //   - a gated-text key (CASE class) among plain columns -> the hash
    //     plane's frame ord-pack route;
    //   - injective per-column exprs (col-minus-const) and the minute-of-
    //     hour class fall through: the plain arms elect over the deduped
    //     source columns exactly as before.
    if !s.key_exprs.is_empty() {
        use crate::planner::AKeyExpr as K;
        if let [K::HostRegex { col }] = s.key_exprs.as_slice() {
            if s.ne_empty_cols.contains(col) {
                trace("DerivedKeyFold", "keyexpr_derived_text", format!("col={col}"));
                return Ok(AFamily::DerivedKeyFold);
            }
        }
        if let [K::TruncMinute { col }] = s.key_exprs.as_slice() {
            if s.frame_pair {
                trace("DenseDomainGroup", "keyexpr_trunc_minute_frame", format!("col={col}"));
                return Ok(AFamily::DenseDomainGroup);
            }
        }
        if s.key_exprs.iter().any(|e| matches!(e, K::CaseSrc { .. })) {
            trace("HashPlaneOwnedGroup", "keyexpr_gated_text", String::new());
            return Ok(AFamily::HashPlaneOwnedGroup);
        }
    }
    let widths: Vec<u8> =
        s.group.iter().map(|&c| crate::stencils::col_width(bank, c)).collect();
    let n_text = widths.iter().filter(|&&w| w == 0).count();
    // R6a: single int key, predicate confined to that key, exactly-known
    // small domain -> fused filter-agg over a dense count array.
    // [ruling 3, F7 fix] the domain bound is planner::dense_domain — the
    // ONE authority the admission gate and the stencil's agg_tier read
    // (the old local `1 << 20` disagreed with the dense tier's 256K and
    // manufactured elect-then-refuse near-misses).
    if widths.len() == 1 && widths[0] > 0 && n_text == 0 {
        let g = s.group[0];
        let only_key_pred = s.pred_cols.iter().all(|&c| c == g) && s.ne_empty_cols.is_empty();
        if only_key_pred && !s.pred_cols.is_empty() && s.aggs_all_count_star {
            if let Some((lo, hi)) = crate::planner::dense_domain(&faces.stats(bank, g)) {
                let range = (hi as i128 - lo as i128) + 1;
                trace(
                    "FusedFilterAgg",
                    "dense_key_domain",
                    format!("minmax_exact=[{lo},{hi}] slots={range}"),
                );
                return Ok(AFamily::FusedFilterAgg);
            }
        }
    }
    if n_text > 0 {
        // varlena keys: the code-domain property (dict availability) is
        // what the two-level family consumes; the label election keys on
        // the KEY ITSELF being filtered nonempty (<>'' on the group key —
        // hot-shape) -> selective code-domain fold. A frame pair
        // WITHOUT the key filter (hot-shape residue-laden frame shape) is the
        // window-replay frame lane's — the RON plan of record's election
        // (PLANDIFF close); exhaustive unfiltered text group-bys ride the
        // hash plane (§1.3 measured boundary).
        let key_filter = s.ne_empty_cols.iter().any(|c| s.group.contains(c));
        if n_text == 1 && widths.len() == 1 && key_filter {
            let dict_parts = (0..bank.parts.len())
                .filter(|&pi| faces.dict(bank, pi, s.group[0]).dh.is_some())
                .count();
            trace(
                "TwoLevelCodeAgg",
                "dict_key_filtered",
                format!(
                    "dict_parts={dict_parts}/{} frame_pair={} ne_empty_key={}",
                    bank.parts.len(),
                    s.frame_pair,
                    !s.ne_empty_cols.is_empty()
                ),
            );
            return Ok(AFamily::TwoLevelCodeAgg);
        }
        if s.frame_pair {
            // R6b: the (Eq!=0, Between) frame pair over an UNFILTERED text
            // key (hot-shape: residues beyond the pair, no key <>'') -> the
            // window-replay frame lane (shared-frame + condcache family).
            trace("WindowReplay", "text_key_frame_pair", String::new());
            return Ok(AFamily::WindowReplay);
        }
        trace(
            "HashPlaneOwnedGroup",
            "text_keys_hash_plane",
            format!("text_keys={n_text} total_keys={}", widths.len()),
        );
        return Ok(AFamily::HashPlaneOwnedGroup);
    }
    // all-int keys
    if s.frame_pair {
        // R6c: the (Eq!=0, Between) frame pair over int group keys ->
        // window-replay frame lane (shared-frame + condcache family).
        trace("WindowReplay", "int_keys_frame_pair", String::new());
        return Ok(AFamily::WindowReplay);
    }
    // R6e ([sqe-avglen]): the fused entry-length mix over ONE
    // small-domain int key -> the two-level dense-int entry-length arm
    // (per-part dict entry byte lengths; hydrated fallback per part).
    // Domain authority: the arm's own dense bound — election, the server
    // admission gate, and the stencil prep read the SAME function.
    if s.avglen_ne_fused && widths.len() == 1 {
        if let Some((lo, n)) = crate::stencils::code_agg::entrylen_dense_bounds(
            &faces.stats(bank, s.group[0]),
        ) {
            trace("TwoLevelCodeAgg", "entrylen_dense_key", format!("lo={lo} slots={n}"));
            return Ok(AFamily::TwoLevelCodeAgg);
        }
    }
    // R6d: int group keys with a `<> ''` residue on a NON-key varlena
    // column (hot-shape SearchPhrase filter) -> survivor gather: the
    // ne-empty survivor plane is the plan's replayable identity (the
    // NeEmpty->var-term promotion fires for this family in the shared
    // lowering, carrying the goal fingerprint the hash plane drops) —
    // the RON plan of record's election (PLANDIFF close).
    if !s.ne_empty_cols.is_empty() && s.ne_empty_cols.iter().all(|c| !s.group.contains(c)) {
        trace(
            "SurvivorGather",
            "int_keys_ne_empty_residue",
            format!("ne_empty_cols={:?}", s.ne_empty_cols),
        );
        return Ok(AFamily::SurvivorGather);
    }
    let ndv: u64 =
        s.group.iter().map(|&c| faces.stats(bank, c).ndv_est_sum()).max().unwrap_or(0);
    trace(
        "HashPlaneOwnedGroup",
        "int_keys_hash_plane",
        format!("widths={widths:?} ndv_est_max={ndv}"),
    );
    Ok(AFamily::HashPlaneOwnedGroup)
}

// ---------------------------------------------------------------------------
// [R6d] the dict-predicate survivor law — the GENERAL election path
// (election-breadth-audit.md §2). Additive: `ShapeFacts` and
// `elect_family` are untouched (the seam constructs ShapeFacts with an
// exhaustive literal; its admission rung adopts these facts later) —
// callers with entry-decidable conjuncts beyond the specific op facts
// feed them here, everyone else's election is byte-identical.
// ---------------------------------------------------------------------------

/// The R6d shape facts a lowering derives alongside `ShapeFacts` when the
/// predicate carries entry-decidable varlena conjuncts OUTSIDE the
/// specific op vocabulary (today: general LIKE / NOT LIKE; the `%x%`
/// class normalizes into `has_contains` upstream and never lands here).
#[derive(Debug, Clone, Default)]
pub struct EntryFacts {
    /// Columns carrying general entry-decidable varlena conjuncts.
    pub entry_var_cols: Vec<u32>,
    /// Every aggregate is in the var-lane grouped vocabulary
    /// (COUNT(*) / COUNT(DISTINCT c) / MIN(text-col) → MinBytes) — the
    /// consumer-shape witness for the grouped verdict/condcache lane.
    pub aggs_all_var_grouped: bool,
    /// Every aggregate is in the ungrouped fused-fold vocabulary
    /// (COUNT(*) / SUM/AVG/MIN/MAX over a word or packed lane, no
    /// FILTER) — the scalar-fold consumer witness: the fused fold
    /// evaluates entry-decidable varlena conjuncts at row grain.
    pub aggs_all_var_fold: bool,
}

/// R6d election law: when every varlena conjunct is entry-decidable, the
/// family is elected from the CONSUMER shape exactly as if the varlena
/// conjuncts were int terms — no varlena op identity appears in any
/// condition below, only the entry-decidable property. The elected
/// consumers are those whose stencil bodies already evaluate arbitrary
/// `VarPredTerm` entry predicates (the verdict/condcache var lane);
/// consumer shapes without a landed body refuse TYPED
/// (`no-family-rule:r6d-consumer-shape`) until their rung. The
/// entry-vs-row GRAIN (the two stats-witnessed guards) is a tier-2
/// planner election (`planner::elect_entry_grain`) — guard failure lowers
/// to the row-grain plane, NEVER a refusal. NullableDict refusal is
/// downstream in check_currency, unchanged.
///
/// Order law: the specific op-fact families are PREFERRED — `has_contains`
/// still elects through the R1 arm, NeEmpty residues through R6d-today's
/// SurvivorGather arm; this path fires only when `entry_var_cols` is
/// non-empty, and delegates to `elect_family` when it is empty.
pub fn elect_family_entry(
    bank: &Bank,
    faces: &Faces,
    s: &ShapeFacts,
    ef: &EntryFacts,
    tag: &str,
) -> Result<AFamily, Refuse> {
    if ef.entry_var_cols.is_empty() {
        return elect_family(bank, faces, s, tag);
    }
    // Contains conjuncts compose: the specific R1 arm wins the election;
    // the general conjuncts ride the same var lane as residues.
    if s.has_contains {
        return elect_family(bank, faces, s, tag);
    }
    let trace = |fam: &str, rule: &str, stats: String| {
        if crate::engine::phase_on() {
            println!("SQEELECT|lower|{tag}|family={fam}|rule={rule}|{stats}");
        }
    };
    if s.group.is_empty() {
        // Selection-count consumer (the fused verdict fold; scalar answer).
        if s.n_aggs > 0 && s.aggs_all_count_star {
            trace(
                "WindowReplay",
                "r6d_entry_count",
                format!("entry_var_cols={:?}", ef.entry_var_cols),
            );
            return Ok(AFamily::WindowReplay);
        }
        // Word/packed scalar-fold consumer: the fused fold owns the
        // shape, evaluating the varlena conjuncts per row (eval_v).
        if s.n_aggs > 0 && ef.aggs_all_var_fold {
            trace(
                "FusedFilterAgg",
                "r6d_entry_fold",
                format!("aggs={} entry_var_cols={:?}", s.n_aggs, ef.entry_var_cols),
            );
            return Ok(AFamily::FusedFilterAgg);
        }
        // Row top-k consumer (ORDER BY col ASC LIMIT k over survivors).
        if s.n_aggs == 0 && s.has_order && s.has_limit && s.n_order_keys == 1 {
            trace(
                "WindowReplay",
                "r6d_entry_topk",
                format!("entry_var_cols={:?}", ef.entry_var_cols),
            );
            return Ok(AFamily::WindowReplay);
        }
        return Err(Refuse::NoFamilyRule { what: "r6d-consumer-shape" });
    }
    // Grouped consumer: single varlena key (or that key plus an hour
    // bucket over a word lane) + the var-lane grouped agg vocabulary.
    let single_text_key = s.key_exprs.is_empty()
        && s.group.len() == 1
        && crate::stencils::col_width(bank, s.group[0]) == 0;
    let text_hour_key = matches!(
        s.key_exprs.as_slice(),
        [crate::planner::AKeyExpr::Col(t), crate::planner::AKeyExpr::HourBucket { .. }]
            | [crate::planner::AKeyExpr::HourBucket { .. }, crate::planner::AKeyExpr::Col(t)]
            if crate::stencils::col_width(bank, *t) == 0
    );
    if (single_text_key || text_hour_key) && s.n_aggs > 0 && ef.aggs_all_var_grouped {
        trace(
            "WindowReplay",
            "r6d_entry_grouped",
            format!("key={} entry_var_cols={:?}", s.group[0], ef.entry_var_cols),
        );
        return Ok(AFamily::WindowReplay);
    }
    Err(Refuse::NoFamilyRule { what: "r6d-consumer-shape" })
}
