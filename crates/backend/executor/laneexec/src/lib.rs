// Lane executor text/qual tiers over columnar (pgrcolumnar) scans, harvested
// from the old lane-executor line onto lane-v2 (pgrcolumnar-v2 plan Stage 1.4,
// docs/design/pgrcolumnar-v2-beat-clickhouse-plan.md; spike
// notes/phase4-pgrcolumnar-source-design-2026-07-12.md §5):
//
// - shape:     the structural clause vocabulary (LaneQualShape). The
//              ExprState walker producing it lands with the executor-wiring
//              tranche; nothing here touches compiled programs.
// - translate: fail-closed translation of a qual shape into a lane program —
//              comparator whitelist, hybrid requal-tail split, StagedClause
//              ordering by static cost class (prewhere), StagedZoneSrc.
// - interp:    the self-contained int-clause interpreter backing translate
//              (v1 backend; stitched bodies route to lanestitch later).
// - dict:      the once-per-code-per-epoch text-predicate memo tier
//              (eq/ne/LIKE/ILIKE + lazy regex memo) over SoaDictLane.
// - dicteval:  generic pure-expression evaluation memoized per dict code
//              (dict-pushdown), lazy-by-default with eager proofs.
//
// Everything unsupported refuses loudly and keeps the per-row drive.
// (A third length-kernel surface, textlen.rs, was deleted 2026-07-21: it
// shipped with no non-test caller — lanefold owns the fold-path length
// kernels and dicteval owns the dict-expr-key ones, so the intended
// consumer never materialized.)
mod dict;
mod dicteval;
pub mod interp;
pub mod shape;
mod translate;

pub use dicteval::{
    compile_value as dicteval_compile_value, compile_value_chain, func_args_concrete,
    prepare_batch as dicteval_prepare_batch, ColView, DictCallSpec, DictEvalProg, DictExprSpec,
    Prepared as DictEvalPrepared, ValueChain, DICTEVAL_MEMO_CAP,
};

/// Inline (non-toasted) varlena admission probe for walker-side Const checks.
pub fn inline_const_ok(d: datum::Datum) -> bool {
    dict::inline_varlena_payload(d).is_some()
}

/// Catalog result type for a walker-built `DictCallSpec.rettype` (dicteval
/// re-checks it against pg_proc, so supplying the catalog value keeps one
/// authority). None = unresolvable (seam missing / not in pg_proc) — the
/// caller refuses, exactly as dicteval's own compile would.
pub fn func_catalog_rettype(fn_oid: types_core::Oid) -> Option<types_core::Oid> {
    syscache_seams::lookup_pg_proc_shape::call(fn_oid).ok().flatten().map(|s| s.prorettype)
}
pub use dict::{collation_usable, inline_varlena_payload, non_inline_lane_datum};
pub use translate::{
    eval_lane_qual, lane_cmp_for_fn_oid, translate_scan_qual, LaneQualProg, StagedZoneSrc,
};

pub fn log_compiled(nclauses: u32, requal: bool) {
    if requal {
        log(format!(
            "lane_executor: compiled qual ({nclauses} clauses vectorized + per-row suffix)"
        ));
    } else {
        log(format!("lane_executor: compiled qual ({nclauses} clauses)"));
    }
}

pub fn log_refused(reason: &str) {
    log(format!("lane_executor: refused ({reason})"));
}

pub fn log_staged_qual(nclauses: usize) {
    log(format!("lane_executor: prewhere staged qual ({nclauses} clauses)"));
}

// Arm-time marker: the condition cache engaged on this scan's staged prefix.
pub fn log_condcache_armed() {
    log("lane_executor: condition cache armed".to_string());
}

// End-of-scan stats line (cumulative process counters; the battery greps
// this from the server log like the other lane stats).
pub fn log_condcache_stats(hits: u64, misses: u64, insertions: u64, evictions: u64) {
    log(format!(
        "lane_executor: condition cache stats \
         (hits={hits} misses={misses} insertions={insertions} evictions={evictions})"
    ));
}

// Arm-time marker: n text clauses joined the prefix on the dict tier.
pub fn log_dict_clauses(n: u32) {
    log(format!("lane_executor: dict clauses ({n})"));
}

// First DictCoded window for a clause: the memo tier actually engaged
// (windows the AM answers Raw run the per-row fallback and never log this).
pub(crate) fn log_dict_memo(col: u16, ndict: u32) {
    log(format!("lane_executor: dict-memo engaged (col {col}, {ndict} entries)"));
}

// First sorted-dict window for a prefix clause: the code-range tier engaged
// (two boundary binary searches per RG replace the per-entry memo fill).
pub(crate) fn log_dict_range(col: u16, ndict: u32) {
    log(format!("lane_executor: dict-range engaged (col {col}, {ndict} entries)"));
}

// Translate-time marker: a LIKE/eq clause classified into a byte kernel
// (exact/prefix/suffix/contains/prefix+suffix/any) — both the memo fill and
// the Raw-window pass run the kernel instead of the scalar predicate.
pub(crate) fn log_dict_regex(col: u16, icase: bool) {
    let tag = if icase { "~*" } else { "~" };
    log(format!("lane_executor: dict regex clause armed (col {col}, {tag}, lazy memo)"));
}

pub(crate) fn log_dict_kernel(col: u16, shape: &str) {
    log(format!("lane_executor: dict kernel compiled (col {col}, {shape})"));
}

// First-engagement marker: the contains kernel ran blob-wide over a
// contiguous text-span window (likeband; once per clause per scan).
pub(crate) fn log_blob_kernel(col: u16) {
    log(format!("lane_executor: blob contains kernel engaged (col {col})"));
}

// First-engagement marker: the per-epoch dict memo was filled by ONE
// arena-wide contains sweep instead of per-entry finder calls (q22fix2).
pub(crate) fn log_dict_sweep(col: u16, ndict: u32) {
    log(format!("lane_executor: dict arena sweep engaged (col {col}, {ndict} entries)"));
}

// Arm-time marker: DictEval progs admitted onto a surface (dict-pushdown §7).
pub fn log_dicteval_armed(surface: &str, n: u32, nfallible: u32) {
    log(format!("lane_executor: dicteval armed ({surface}, {n} progs, {nfallible} lazy-fallible)"));
}

pub fn log_dicteval_refused(reason: &str) {
    log(format!("lane_executor: dicteval refused ({reason})"));
}

// First DictCoded window for a prog: the memo tier actually engaged.
pub(crate) fn log_dicteval_engaged(col: u16, ndict: u32, eager: bool) {
    let mode = if eager { "eager" } else { "lazy" };
    log(format!("lane_executor: dicteval memo engaged (col {col}, {ndict} entries, {mode})"));
}

pub fn log_dicteval_demoted(reason: &str) {
    log(format!("lane_executor: dicteval demoted ({reason})"));
}

// The battery script greps these lines from the server log (DEBUG1); tests
// without an installed elog seam just drop them.
fn log(msg: String) {
    if elog_seams::ereport_msg::is_installed() {
        let _ = elog_seams::ereport_msg::call(types_error::DEBUG1, msg, None);
    }
}
