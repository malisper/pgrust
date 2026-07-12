// nodeSeqscan.c. ExecProcNode dispatch is the variant enum resolved once at
// init (C installs one of five function pointers).
#![allow(non_snake_case)]

extern crate alloc;

use ::execexpr::exec_init_qual;
use ::execscan::{exec_scan_epq, exec_scan_extended, ScanNode, ScanState};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, PgVec};
use ::tableam::{
    table_beginscan, table_beginscan_parallel, table_endscan, table_parallelscan_initialize,
    table_parallelscan_reinitialize, table_rescan, table_scan_getnextslot, table_slot_callbacks,
    ParallelTableScanDescShared,
};
use ::types_error::{PgError, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE};
use ::types_nodes::plannodes::SeqScan;
use ::types_rel::Relation;
use ::types_slot::{
    SlotData, EXEC_FLAG_BACKWARD, EXEC_FLAG_EXPLAIN_ONLY, EXEC_FLAG_MARK, EXEC_FLAG_WITH_NO_DATA,
};

pub fn init_seams() {}

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeqScanVariant {
    Plain,
    WithQual,
    WithProject,
    WithQualProject,
    // Hashjoin-pushed Bloom filter over the scan's key column (pure filter,
    // false positives only); reverts to Plain on disarm.
    PlainBloom,
    Epq,
}

pub struct SeqScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    variant: SeqScanVariant,
    plan_node_id: i32,
    parallel_aware: bool,
    // Keeps the scan desc's NonNull target alive for the scan's lifetime.
    parallel: Option<std::sync::Arc<ParallelTableScanDescShared>>,
    // Boxed: PlanStateNode carries a 1024-byte size assert.
    batch_soa: Option<::mcx::PgBox<'mcx, BatchSoa<'mcx>>>,
    scan_batch: ScanBatchMode,
    batch_allowed: bool,
    bloom: Option<::mcx::PgBox<'mcx, BloomScan<'mcx>>>,
    // Lane-executor-v2 page-batch cursor (driven by `execmain::lanev2`):
    // position within the currently-staged page batch and its row count.
    // `lane_pos == lane_n` (both 0 initially) means "pull the next batch".
    // Reset on rescan/park. Only touched via the accessors below; the lane
    // drive itself lives entirely in the `lanev2` module.
    lane_pos: u32,
    lane_n: u32,
    // Lane-executor-v2 memoized STATIC fusibility verdict (plan shape + AM
    // page-batch support), computed once at the first dispatch: the refuse
    // verdict must be stable across Volcano calls — a mid-scan REFUSE→OWN
    // flip would skip the staged remainder of the current page — and the
    // fusibility cascade must not run per pulled tuple. Dynamic per-call
    // gates (EPQ, direction) stay in the lane. None = not yet evaluated.
    // Reset on park (rebind may change the backing scan).
    lane_verdict: Option<bool>,
    // cbstore relations only: plan-derived column need-set + zone-mappable
    // conjuncts, installed on the scan desc at open (cbstore-impl.md §7.3).
    cb_scan: Option<std::boxed::Box<CbScanInfo>>,
}

/// Plan-derived cbstore scan settings (built once at init, applied to every
/// freshly opened scan desc — serial open and both parallel init paths).
struct CbScanInfo {
    /// Columns the scan reads (qual + targetlist Vars; whole row when a
    /// whole-row Var appears). Only these columns' chunks decode.
    needed: Vec<bool>,
    /// Zone-map-mappable `Var CMP Const` conjuncts of the scan qual
    /// (advisory pruning only; the executor still evaluates the full qual
    /// on surviving rows).
    zone: Vec<::tableam::ZoneQual>,
}

// Hashjoin Bloom pushdown state: key-column-only SoA deform per staged page,
// selection bits from the filter; survivors store like the per-row path.
struct BloomScan<'mcx> {
    filter: std::rc::Rc<::nodehash::ProbeBloom<'mcx>>,
    plan: ::exectuples::SoaDeformPlan<'mcx>,
    soa: ::exectuples::SoaBatch<'mcx>,
    col: u16,
    sel: [u64; ::exectuples::SOA_BM_WORDS],
    nwords: u32,
    cur_word: u32,
    cur_bits: u64,
    seen: u32,
    kept: u32,
}

impl BloomScan<'_> {
    #[inline(always)]
    fn next_selected(&mut self) -> Option<u32> {
        loop {
            if self.cur_bits != 0 {
                let bit = self.cur_bits.trailing_zeros();
                self.cur_bits &= self.cur_bits - 1;
                return Some(self.cur_word * 64 + bit);
            }
            if self.cur_word + 1 >= self.nwords {
                return None;
            }
            self.cur_word += 1;
            self.cur_bits = self.sel[self.cur_word as usize];
        }
    }

    fn reset_staged(&mut self) {
        self.nwords = 0;
        self.cur_word = 0;
        self.cur_bits = 0;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanBatchMode {
    Unknown,
    Off,
    On,
}

struct BatchSoa<'mcx> {
    plan: ::exectuples::SoaDeformPlan<'mcx>,
    soa: ::exectuples::SoaBatch<'mcx>,
    // Bitmap-able kernel qual (QualScanVarCmpConst on a prefix column).
    qual_armed: bool,
    // Scan-node drive: deform the qual column only; survivors deform lazily.
    qual_only: bool,
    // Fused-sort direct key feed: deform this column only, never publish the
    // prefix onto the slot (other prefix cells stay stale).
    key_col: Option<u16>,
    // Varlena key: staged into soa column 0 via the varkey pass.
    varkey: Option<::exectuples::SoaVarKeyPlan>,
    // Precomputed emit_key read column (0 for varkey, key_col for fixed):
    // one load on the per-row read path.
    key_read_col: u16,
    // Precomputed !qual_only && key_col.is_none(): one test on the store path.
    publish: bool,
    // The kernel-qual clause list (AND of scan-Var-CMP-Const; 1 = the fused
    // kernel, 2+ = the multi-clause census the lane admits).
    quals: [(u16, ::execexpr::CmpOp, ::datum::Datum); ::execexpr::SCAN_CMP_MAX_CLAUSES],
    nquals: u8,
    // Tier-2 stitched-JIT state; armed only by the lane driver on drain
    // pipelines feeding breakers (`seq_scan_stitch_arm`).
    stitch: Option<QualStitch>,
    // Stitched-projection state (Phase-3 projection stitching); armed only
    // by the lane driver on drain pipelines (`seq_scan_proj_stitch_arm`).
    proj: Option<ProjStitch<'mcx>>,
    sel: [u64; ::exectuples::SOA_BM_WORDS],
    nwords: u32,
    cur_word: u32,
    cur_bits: u64,
}

/// Tier-2 (stitched-JIT) state for the kernel-qual filter segment — the JIT
/// ladder per design doc §3a: interpreter (oracle/floor, inside
/// `StitchedProgram::run`) → AOT bitmap passes (`qual_bitmap_cmp_const`) →
/// the stitched body. Lives on the `BatchSoa` so the row census and the
/// sticky refusal are per plan-node arming; `exec_end_seq_scan` releases it
/// (the deform-JIT Rc precedent).
struct QualStitch {
    /// The clause program (LoadLane/LoadConst/Cmp/Qual per clause), the
    /// translation of `BatchSoa::quals` — also the replay/oracle source the
    /// stitched body falls back to on drift or refuse-and-replay.
    prog: ::lanestitch::Program,
    /// Lane-view width the body compiles against (max clause col + 1).
    ncols: usize,
    /// Compiled once past the row floor; None below it (AOT tier owns).
    body: Option<::lanestitch::StitchedProgram>,
    /// Rows staged through the armed qual so far (the tier-2 row floor).
    rows_seen: u64,
    /// Sticky per-plan refusal (classification / arch / arena refuse).
    refused: bool,
    // Engagement telemetry (PGRUST_LANE_V2_TRACE summary at scan end).
    n_stitched: u64,
    n_aot: u64,
    n_interp: u64,
}

/// Stitched-projection state for a lane-owned projected scan (Phase-3
/// projection stitching): the vocabulary-covered target list (Var
/// passthrough / same-width int2/4/8 arith — `ScanProjCols`) compiled over
/// the staged SoA lanes, computing per-batch OUTPUT lanes for the qual
/// bitmap's true survivors (forced-fallback rows are masked out — their
/// lanes are undeformed; they keep the per-row path). The emit's fast lane
/// fills the projection result slot from the output lanes; everything the
/// vocabulary does not cover refuses at arm time and leaves the per-row
/// `exec_project` path untouched.
///
/// Refuse-and-replay (charter discipline): an arith trap (overflow / zero
/// divisor) makes the body exit refused having constructed NO error and
/// this batch's `staged` stays false — every row of the batch then projects
/// per-row through the C-ported `exec_project`, which raises C's exact
/// error text on C's row after consuming the preceding survivors. Sticky
/// per plan: after one replay the body never runs again.
struct ProjStitch<'mcx> {
    /// The tlist translation (LoadLane/LoadConst/Arith/StoreOut per column).
    prog: ::lanestitch::Program,
    /// Lane-view width the body compiles against (max read attnum + 1).
    ncols: usize,
    /// Output-lane count == tlist arity == result-slot natts.
    nouts: u16,
    /// Compiled once past the row floor; None below it (per-row tier owns).
    body: Option<::lanestitch::StitchedProjection>,
    rows_seen: u64,
    /// Sticky per-plan refusal (classification / arch / arena / replay).
    refused: bool,
    /// Outputs valid for the CURRENTLY staged batch (set at staging).
    staged: bool,
    /// The selectivity disarm applies: hosting WIDENED the per-batch deform
    /// beyond what the qual alone stages (the single-clause col-only case),
    /// so low-selectivity scans pay full-prefix deform for few saved
    /// projections. `stitched_rows`/`stitched_survivors` (rows staged /
    /// true survivors through the stitched body) feed the one-shot check in
    /// `stitch_project`.
    adapt: bool,
    adapt_checked: bool,
    stitched_rows: u64,
    stitched_survivors: u64,
    /// Output lanes, nouts x SOA_MAX_ROWS (column-major, SoaBatch layout).
    out_values: ::mcx::PgVec<'mcx, ::datum::Datum>,
    out_isnull: ::mcx::PgVec<'mcx, bool>,
    // Engagement telemetry (PGRUST_LANE_V2_TRACE summary at scan end).
    n_stitched: u64,
    n_perrow: u64,
}

/// Selectivity floor for the ADAPTIVE projection disarm (admission
/// economics, measured 2026-07-12 on the 10M-row lane-bench dataset, warm
/// best-of-3x3 interleaved): when hosting widened a single-clause col-only
/// deform to the full projection prefix, ~10%-selectivity shapes ran +1-2%
/// (p1/p4: extra 4-5 col deform on every staged row, few saved projections)
/// while ~50%-selectivity shapes won 13-19% (p2/p3). One-shot check after
/// PROJ_ADAPT_ROWS staged rows: below the floor, drop the projection arm —
/// staging returns to the qual-only col deform and the per-row projection
/// path (exactly the pre-projstitch lane). Ratchet only with a measurement.
const PROJ_MIN_SELECTIVITY_PCT: u64 = 20;
// 16k rows: >=1.6k survivors even at the 10% floor case — ample signal; the
// widened-deform probe window stays ~0.2% of a 10M-row scan.
const PROJ_ADAPT_ROWS: u64 = 16384;

/// Tier-2 row floor (the batchexec POC admission number): the stitched body
/// engages only once ~2048 rows have flowed through the armed qual — OLTP-
/// sized scans never pay a stitch.
const STITCH_ROW_FLOOR: u64 = 2048;

/// Tier-2 fusion floor (admission economics, design §4 — never preempt a
/// measured-faster path): the stitched body engages only when it FUSES
/// something the AOT tier runs as separate passes, i.e. >= 2 clauses. A
/// single-clause body re-runs exactly the AOT kernel's one pass plus the
/// per-batch call/params overhead — measured 2026-07-12 (10M-row filtered
/// drain shapes, warm best-of-6 interleaved): 1-clause agg feeds 0.998x
/// (parity), 1-clause sort feed 1.04x (loss); 3-clause shapes 0.97-0.98x
/// (fusion win). Ratchet DOWN only with a measurement.
const STITCH_MIN_CLAUSES: u8 = 2;

/// Engagement trace (verification aid, no perf path): mirrors lanev2's
/// `PGRUST_LANE_V2_TRACE` switch so one env var traces the whole lane.
fn lane_trace(event: &str) {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_LANE_V2_TRACE").as_deref(), Ok("1") | Ok("on"))
    }) {
        eprintln!("[lane-v2] {event}");
    }
}

impl BatchSoa<'_> {
    #[inline(always)]
    fn next_selected(&mut self) -> Option<u32> {
        loop {
            if self.cur_bits != 0 {
                let bit = self.cur_bits.trailing_zeros();
                self.cur_bits &= self.cur_bits - 1;
                return Some(self.cur_word * 64 + bit);
            }
            if self.cur_word + 1 >= self.nwords {
                return None;
            }
            self.cur_word += 1;
            self.cur_bits = self.sel[self.cur_word as usize];
        }
    }

    fn reset_staged(&mut self) {
        self.nwords = 0;
        self.cur_word = 0;
        self.cur_bits = 0;
        if let Some(p) = self.proj.as_mut() {
            // The staged batch is gone; its output lanes go with it. (The
            // emit fast lane is additionally gated on nwords > 0, so this
            // is belt-and-braces.)
            p.staged = false;
        }
    }
}

impl<'mcx> SeqScanState<'mcx> {
    pub fn variant(&self) -> SeqScanVariant {
        self.variant
    }

    pub fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    pub fn parallel_aware(&self) -> bool {
        self.parallel_aware
    }

    /// Parallel leader or worker. (The lane-v2 SeqScan drive now admits
    /// parallel scans — the batched page feed rides the shared DSM block
    /// cursor; kept for gates that still refuse parallel.)
    pub fn is_parallel(&self) -> bool {
        self.parallel_aware || self.parallel.is_some()
    }

    /// Forward, non-mark eflags at init (`ExecInitSeqScan`). False for a
    /// scrollable/backward or mergejoin-mark cursor — the lane-v2 page-batch
    /// drive is forward-only, so it refuses these.
    pub fn batch_allowed(&self) -> bool {
        self.batch_allowed
    }

    /// Lane-executor-v2 page-batch cursor `(pos, n)`: the drive lives in the
    /// `lanev2` module, this only stores its position across the Volcano
    /// per-call boundary.
    pub fn lane_cursor(&self) -> (u32, u32) {
        (self.lane_pos, self.lane_n)
    }

    pub fn set_lane_cursor(&mut self, pos: u32, n: u32) {
        self.lane_pos = pos;
        self.lane_n = n;
    }

    /// Memoized static lane fusibility verdict; `None` = not yet evaluated.
    pub fn lane_verdict(&self) -> Option<bool> {
        self.lane_verdict
    }

    pub fn set_lane_verdict(&mut self, v: bool) {
        self.lane_verdict = Some(v);
    }

    pub fn release_parallel(&mut self) {
        self.parallel = None;
    }
}

impl<'mcx> ScanNode<'mcx> for SeqScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    /// `SeqRecheck`: seqscans have no access-method conditions to re-verify.
    #[inline(always)]
    fn epq_recheck(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _slot: ExecSlotId,
    ) -> PgResult<bool> {
        Ok(true)
    }

    /// `SeqNext`.
    #[inline(always)]
    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let mcx = estate.es_query_cxt;
        let direction = estate.es_direction;

        self.ensure_scandesc(estate)?;

        // SAFETY: written by ensure_scandesc when None; single test+branch
        // like C's scandesc == NULL check.
        let scandesc = unsafe { self.ss.ss_currentScanDesc.as_mut().unwrap_unchecked() };
        let slot = estate.slot_mut(self.ss.ss_ScanTupleSlot);
        table_scan_getnextslot(mcx, scandesc, direction, slot)
    }
}

impl<'mcx> SeqScanState<'mcx> {
    // Hot per-row check stays a single inlined test+branch (C's scandesc ==
    // NULL check); the once-per-scan open is outlined.
    #[inline(always)]
    fn ensure_scandesc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        if self.ss.ss_currentScanDesc.is_none() {
            self.open_scandesc(estate)?;
        }
        Ok(())
    }

    #[inline(never)]
    fn open_scandesc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        let mcx = estate.es_query_cxt;
        let snapshot = estate.es_snapshot.clone();
        self.ss.ss_currentScanDesc = Some(table_beginscan(
            mcx,
            self.ss.ss_currentRelation.as_ref().expect("seqscan has a relation"),
            snapshot,
            0,
            PgVec::new_in(mcx),
        )?);
        self.apply_cb_scan_settings();
        self.arm_slot_jit_deform(estate);
        Ok(())
    }

    // cbstore need-set + zone quals onto a freshly opened scan desc (serial
    // open_scandesc and both parallel init paths).
    fn apply_cb_scan_settings(&mut self) {
        if let Some(cb) = self.cb_scan.as_deref() {
            let sd = self.ss.ss_currentScanDesc.as_mut().unwrap();
            ::tableam::table_scan_set_needed_attrs(sd, &cb.needed);
            ::tableam::table_scan_push_zone_quals(sd, &cb.zone);
        }
    }

    // Rung 1 (per-row lazy path): arm the scan slot with a kernel sized to
    // what the scan actually fetches (qual + projection max_fetch; whole row
    // when absent or shape-unknown), clamped to the fixed prefix; 1-2-column
    // fetches stay on the interpreter (JIT_DEFORM_ROW_MIN_COLS).
    fn arm_slot_jit_deform(&mut self, estate: &mut EStateData<'mcx>) {
        let scandesc = self.ss.ss_currentScanDesc.as_ref().expect("armed after beginscan");
        let nblocks = ::tableam::table_scan_nblocks(scandesc);
        let rel = self.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
        let natts = rel.rd_att.natts;
        let mut need = 0i32;
        match self.ss.ps_ProjInfo.as_ref() {
            Some(p) => need = need.max(p.pi_state.max_fetch(::execexpr::SlotSrc::Scan).unwrap_or(natts)),
            None => need = natts,
        }
        if let Some(q) = self.ss.qual.as_deref() {
            need = need.max(q.max_fetch(::execexpr::SlotSrc::Scan).unwrap_or(natts));
        }
        let prefix = ::jit_deform::fixed_prefix(&rel.rd_att.compact_attrs);
        let ncols = prefix.min(need.max(0) as usize);
        if ncols < JIT_DEFORM_ROW_MIN_COLS {
            return;
        }
        let Some(k) = jit_deform_kernel(rel, ncols, nblocks, JIT_DEFORM_ROW_MIN_PAGES) else {
            return;
        };
        match estate.slot_mut(self.ss.ss_ScanTupleSlot) {
            SlotData::Heap(h) => h.jit_deform = Some(k),
            SlotData::BufferHeap(b) => b.base.jit_deform = Some(k),
            _ => {}
        }
    }
}

// Deform-JIT gates (docs/optimizations/jit-deform.md). Break-even vs the
// interpreted walk is <2 pages; gated with 2x margin. Both rungs share it
// since rung 3 removed the AOT column pass (the old 48-page batch gate
// priced JIT against AOT's ~23-page break-even). Relation-local page counts
// stand in for C's query-level jit_above_cost shape: a ~5us stencil install
// cannot use thresholds sized for ~ms LLVM compiles. C's jit +
// jit_tuple_deforming GUCs stay the kill switches.
const JIT_DEFORM_ROW_MIN_PAGES: u32 = 4;
const JIT_DEFORM_BATCH_MIN_PAGES: u32 = 4;
// Kernel + double-call overhead vs the warm inline walk crosses between 2
// and 3 fetched columns (v2 train: sort_limit need-3 -3.2%, distinct
// need-2 +1.3%).
const JIT_DEFORM_ROW_MIN_COLS: usize = 3;
// The floor survives the AOT removal: LLVM unswitches the generic fetch
// loop back to monomorphic shape (JIT-off A/B ran flat vs the AOT loops),
// so the kernel still loses below 4 columns (rung-3 first cut armed c=1
// hash-build and c=3 agg batches: joins +0.8%, group_agg +0.7%).
const JIT_DEFORM_BATCH_MIN_COLS: usize = 4;

fn jit_deform_kernel(
    rel: &Relation<'_>,
    ncols: usize,
    nblocks: u32,
    min_pages: u32,
) -> Option<std::rc::Rc<::jit_deform::DeformKernel>> {
    if ncols == 0 || nblocks < min_pages || !::jit_deform::available() {
        return None;
    }
    let jit_on = ::guc_tables::vars::jit_enabled.installed()
        && ::guc_tables::vars::jit_tuple_deforming.installed()
        && ::guc_tables::vars::jit_enabled.read()
        && ::guc_tables::vars::jit_tuple_deforming.read();
    if !jit_on || !relcache_seams::relation_get_deform_kernel::is_installed() {
        return None;
    }
    let k = relcache_seams::relation_get_deform_kernel::call(rel.rd_id, ncols as u16)?;
    // A held-but-rebuilt relation must never run the current entry's kernel.
    k.matches(&rel.rd_att).then_some(k)
}

/// Fused page-batch drive support (upstream batch scan, CF 6176). The caller
/// owns qual/projection evaluation and gates its own variant set.
pub fn seq_scan_batch_supported<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if matches!(node.variant, SeqScanVariant::Epq) {
        return Ok(false);
    }
    node.ensure_scandesc(estate)?;
    let scandesc = node.ss.ss_currentScanDesc.as_ref().unwrap();
    Ok(::tableam::table_scan_supports_pagebatch(scandesc))
}

/// As `seq_scan_batch_supported`, but also admits parallel scan descriptors
/// (the batched page feed routes block acquisition through the shared DSM
/// block cursor). Lane-v2 SeqScan drive only — the fused agg/sort/hash
/// drives keep the conservative serial-only gate.
pub fn seq_scan_batch_supported_parallel<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if matches!(node.variant, SeqScanVariant::Epq) {
        return Ok(false);
    }
    node.ensure_scandesc(estate)?;
    let scandesc = node.ss.ss_currentScanDesc.as_ref().unwrap();
    Ok(::tableam::table_scan_supports_pagebatch_parallel(scandesc))
}

/// Arm SoA batch deform of the `prefix`-column prefix for the fused drive;
/// stays disarmed (per-row lazy deform) unless the prefix is all fixed-width.
/// `multi`: admit multi-clause kernel quals (AND of scan-Var-CMP-Const) to
/// the selection bitmap — lane-v2 callers only; the incumbent fused drives
/// pass false and keep their exact single-kernel admission.
pub fn seq_scan_batch_soa_prepare<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    prefix: i32,
    qual_only: bool,
    force: bool,
    multi: bool,
) {
    if prefix <= 0 {
        node.batch_soa = None;
        return;
    }
    if let Some(b) = &node.batch_soa {
        if b.plan.ncols() as i32 == prefix && b.qual_only == qual_only && b.key_col.is_none() {
            return;
        }
    }
    let mcx = estate.es_query_cxt;
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    let atts: &[_] = &rel.rd_att.compact_attrs;
    let qual = node
        .ss
        .qual
        .as_deref()
        .and_then(|q| q.scan_cmp_const_clauses())
        .filter(|c| {
            (multi || c.n == 1)
                && c.clauses[..c.n as usize].iter().all(|&(col, _, _)| (col as i32) < prefix)
        });
    // Break-even: at <=2 fixed columns the deform+gather double-copy loses to
    // the per-row walk (distinct +2.3% instr) unless a bitmap qual skips the
    // gather for non-survivors; group_agg's 3-column prefix wins -4.9%.
    if qual.is_none() && prefix < 3 && !force {
        node.batch_soa = None;
        return;
    }
    node.batch_soa = ::exectuples::SoaDeformPlan::try_new(mcx, atts, prefix as usize).map(|plan| {
        // Rung 2 (dense batch pass): the JIT batch kernel replaces the AOT
        // column loops on dense full-prefix deforms; col-only passes and
        // mixed batches keep the AOT/interpreted paths.
        let mut plan = plan;
        if plan.ncols() as usize >= JIT_DEFORM_BATCH_MIN_COLS {
            if let Some(sd) = node.ss.ss_currentScanDesc.as_ref() {
                let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
                if let Some(k) = jit_deform_kernel(
                    rel,
                    plan.ncols() as usize,
                    ::tableam::table_scan_nblocks(sd),
                    JIT_DEFORM_BATCH_MIN_PAGES,
                ) {
                    plan.arm_jit(k);
                }
            }
        }
        ::mcx::PgBox::new_in(
            BatchSoa {
                soa: ::exectuples::SoaBatch::new_in(mcx, plan.ncols()),
                plan,
                qual_armed: qual.is_some(),
                qual_only: qual_only && qual.is_some(),
                key_col: None,
                varkey: None,
                key_read_col: 0,
                publish: !(qual_only && qual.is_some()),
                quals: qual.map_or(
                    [(0, ::execexpr::CmpOp::Int4Eq, ::datum::Datum::null());
                        ::execexpr::SCAN_CMP_MAX_CLAUSES],
                    |c| c.clauses,
                ),
                nquals: qual.map_or(0, |c| c.n),
                stitch: None,
                proj: None,
                sel: [0; ::exectuples::SOA_BM_WORDS],
                nwords: 0,
                cur_word: 0,
                cur_bits: 0,
            },
            mcx,
        )
    });
}

/// Arm the fused-sort direct key feed: output column 0 must be exactly one
/// scan Var (bare single-column scan or a lone `JustAssignVar` projection)
/// the fixed-width SoA plan covers, no qual. False leaves the per-row path.
pub fn seq_scan_sortkey_direct<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> bool {
    if node.ss.qual.is_some() {
        return false;
    }
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    let attnum = match node.ss.ps_ProjInfo.as_ref() {
        None if rel.rd_att.natts == 1 => 0u16,
        None => return false,
        Some(p) => match p.pi_state.kernel() {
            ::execexpr::Kernel::JustAssignVar {
                src: ::execexpr::SlotSrc::Scan,
                attnum,
                resultnum: 0,
            } => attnum,
            _ => return false,
        },
    };
    if let Some(b) = &node.batch_soa {
        if b.key_col == Some(attnum) {
            return true;
        }
    }
    let mcx = estate.es_query_cxt;
    let atts: &[_] = &rel.rd_att.compact_attrs;
    let (plan, varkey) =
        match ::exectuples::SoaDeformPlan::try_new(mcx, atts, attnum as usize + 1) {
            Some(plan) => (plan, None),
            None => {
                let Some(vk) = ::exectuples::SoaVarKeyPlan::try_new(atts, attnum as usize)
                else {
                    return false;
                };
                (::exectuples::SoaDeformPlan::unused(mcx), Some(vk))
            }
        };
    let soa_cols = if varkey.is_some() { 1 } else { plan.ncols() };
    let key_read_col = if varkey.is_some() { 0 } else { attnum };
    node.batch_soa = Some(::mcx::PgBox::new_in(
        BatchSoa {
            soa: ::exectuples::SoaBatch::new_in(mcx, soa_cols),
            plan,
            qual_armed: false,
            qual_only: false,
            key_col: Some(attnum),
            varkey,
            key_read_col,
            publish: false,
            quals: [(0, ::execexpr::CmpOp::Int4Eq, ::datum::Datum::null());
                ::execexpr::SCAN_CMP_MAX_CLAUSES],
            nquals: 0,
            stitch: None,
            proj: None,
            sel: [0; ::exectuples::SOA_BM_WORDS],
            nwords: 0,
            cur_word: 0,
            cur_bits: 0,
        },
        mcx,
    ));
    true
}

/// Arm the varlena lane feed for the lane-v2 agg fold: stage per-row datum
/// pointers to varlena column `attnum` into SoA column 0 via the varkey pass
/// (the fixed-width prefix deform cannot host an `attlen == -1` column).
/// Publish stays off — the fold feed stores every emitted row per-row, so
/// slot deform semantics are untouched. False = the column's tuple walk is
/// not stageable (an `attlen == -2` attribute precedes it); the caller keeps
/// its per-row path.
pub fn seq_scan_batch_soa_prepare_varlane<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    attnum: u16,
) -> bool {
    if let Some(b) = &node.batch_soa {
        if b.key_col == Some(attnum) && b.varkey.is_some() {
            return true;
        }
    }
    let mcx = estate.es_query_cxt;
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    let atts: &[_] = &rel.rd_att.compact_attrs;
    let Some(vk) = ::exectuples::SoaVarKeyPlan::try_new(atts, attnum as usize) else {
        return false;
    };
    node.batch_soa = Some(::mcx::PgBox::new_in(
        BatchSoa {
            soa: ::exectuples::SoaBatch::new_in(mcx, 1),
            plan: ::exectuples::SoaDeformPlan::unused(mcx),
            qual_armed: false,
            qual_only: false,
            key_col: Some(attnum),
            varkey: Some(vk),
            key_read_col: 0,
            publish: false,
            quals: [(0, ::execexpr::CmpOp::Int4Eq, ::datum::Datum::null());
                ::execexpr::SCAN_CMP_MAX_CLAUSES],
            nquals: 0,
            stitch: None,
            proj: None,
            sel: [0; ::exectuples::SOA_BM_WORDS],
            nwords: 0,
            cur_word: 0,
            cur_bits: 0,
        },
        mcx,
    ));
    true
}

/// Direct key read for staged row `i`; None = fallback row (narrow tuple),
/// the caller must take the full emit path.
#[inline(always)]
pub fn seq_scan_batch_key<'mcx>(
    node: &SeqScanState<'mcx>,
    i: u32,
) -> Option<(::datum::Datum, bool)> {
    let b = node.batch_soa.as_deref().expect("direct key feed armed");
    debug_assert!(b.key_col.is_some());
    let c = b.key_read_col as usize;
    if b.soa.is_fallback(i) {
        return None;
    }
    Some((b.soa.col_values(c)[i as usize], b.soa.col_isnull(c)[i as usize]))
}

/// Kernel-qual selection bitmap armed on the batch SoA — the lane-v2
/// filtered-scan fast path (also true under the fused full-prefix deform,
/// where one deform serves both the qual bitmap and the fold lanes).
#[inline(always)]
pub fn seq_scan_batch_qual_bitmap_armed(node: &SeqScanState<'_>) -> bool {
    node.batch_soa.as_deref().is_some_and(|b| b.qual_armed)
}

/// Bitmap computed for the CURRENTLY staged page batch (armed + a non-empty
/// selection word set). False for a batch staged before arming — the caller
/// must keep the per-row walk for that batch.
#[inline(always)]
pub fn seq_scan_batch_qual_bitmap_ready(node: &SeqScanState<'_>) -> bool {
    node.batch_soa.as_deref().is_some_and(|b| b.qual_armed && b.nwords > 0)
}

/// Pop the next selection-bitmap survivor of the staged batch (ascending
/// staged-row index): bitmap hits plus forced fallback bits — the SoA prefix
/// deform skipped those rows, so `seq_scan_batch_fetch` re-checks them
/// per-row. The iterator cursor is node-resident (`cur_word`/`cur_bits`),
/// surviving the Volcano call boundary; `exec_rescan_seq_scan` resets it.
#[inline(always)]
pub fn seq_scan_batch_next_selected(node: &mut SeqScanState<'_>) -> Option<u32> {
    let b = node.batch_soa.as_deref_mut()?;
    debug_assert!(b.qual_armed);
    b.next_selected()
}

/// Staged SoA batch when the full-prefix deform is armed (columnar readers).
#[inline]
pub fn seq_scan_batch_soa<'a, 'mcx>(
    node: &'a SeqScanState<'mcx>,
) -> Option<&'a ::exectuples::SoaBatch<'mcx>> {
    let b = node.batch_soa.as_deref()?;
    (!b.qual_only).then_some(&b.soa)
}

/// Staged kernel-qual selection bitmap when the SoA deform armed the batch
/// qual (bits over the current staged batch; forced fallback rows carry a
/// set bit and must be re-checked per-row — compose with `fallback_words`).
/// None = no bitmap qual staged; the per-row fetch path owns the qual. A
/// `Some` covers the scan's WHOLE qual (the kernel is the entire program).
#[inline]
pub fn seq_scan_batch_qual_sel<'a, 'mcx>(node: &'a SeqScanState<'mcx>) -> Option<&'a [u64]> {
    let b = node.batch_soa.as_deref()?;
    b.qual_armed.then_some(&b.sel[..])
}

pub fn seq_scan_next_pagebatch<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<u32> {
    node.ensure_scandesc(estate)?;
    let SeqScanState { ss, batch_soa, .. } = node;
    // SAFETY: written by ensure_scandesc when None.
    let scandesc = unsafe { ss.ss_currentScanDesc.as_mut().unwrap_unchecked() };
    let n = ::tableam::table_scan_getnextpagebatch(scandesc)?;
    if n > 0 {
        if let Some(b) = batch_soa.as_mut() {
            let b = &mut **b;
            if let Some(vk) = &b.varkey {
                ::tableam::table_scan_batch_stage_varkey(scandesc, vk, &mut b.soa);
                return Ok(n);
            }
            // Single-clause qual-only staging deforms just the qual column;
            // a multi-clause qual needs every clause column, so it stages
            // the full (fixed-width) prefix. An armed stitched projection
            // reads its tlist columns from the lanes too, so it also forces
            // the full prefix.
            let qual_col_only =
                (b.qual_only && b.qual_armed && b.nquals == 1 && b.proj.is_none())
                    .then_some(b.quals[0].0)
                    .or(b.key_col);
            ::tableam::table_scan_batch_deform(scandesc, &b.plan, &mut b.soa, qual_col_only);
            if b.qual_armed {
                let nwords = (n as usize).div_ceil(64);
                // Tier ladder (design §3a): tier 2 = the stitched body over
                // the staged lanes (drain pipelines only, past the row
                // floor); tier 1 = the AOT bitmap kernel, one pass per
                // clause ANDed; tier 0 = the lanestitch interpreter, run
                // inside `StitchedProgram::run` on per-batch drift or after
                // a sticky refuse-and-replay. All tiers produce the same
                // selection bits over the same staged lanes (the lanestitch
                // equivalence contract + the strict-compare AND identity).
                if !stitch_qual_bitmap(b, n)? {
                    for (ci, &(col, cmp, konst)) in
                        b.quals[..b.nquals as usize].iter().enumerate()
                    {
                        if ci == 0 {
                            ::execexpr::qual_bitmap_cmp_const(
                                cmp,
                                konst,
                                b.soa.col_values(col as usize),
                                b.soa.col_isnull(col as usize),
                                &mut b.sel,
                            );
                        } else {
                            let mut tmp = [0u64; ::exectuples::SOA_BM_WORDS];
                            ::execexpr::qual_bitmap_cmp_const(
                                cmp,
                                konst,
                                b.soa.col_values(col as usize),
                                b.soa.col_isnull(col as usize),
                                &mut tmp,
                            );
                            for (w, t) in b.sel[..nwords].iter_mut().zip(&tmp[..nwords]) {
                                *w &= t;
                            }
                        }
                    }
                }
                // Stitched projection over the TRUE qual survivors: runs on
                // the pure qual bits BEFORE the forced-fallback OR below
                // (fallback rows carry no lane values — they keep the
                // per-row store+qual+project path; a garbage lane value must
                // never reach an erroring arith stencil). A true return =
                // the adaptive selectivity floor tripped: drop the arm, so
                // the NEXT staging returns to the qual-only column deform.
                if stitch_project(b, n) {
                    b.proj = None;
                }
                // Skipped rows carry a forced bit; the fetch re-checks them.
                for (w, fb) in b.sel[..nwords].iter_mut().zip(b.soa.fallback_words()) {
                    *w |= fb;
                }
                b.nwords = nwords as u32;
                b.cur_word = 0;
                b.cur_bits = b.sel[0];
            } else if let Some(p) = &mut b.proj {
                // No qual bitmap staged for this batch (bitmap disarmed):
                // the per-row path owns projection too.
                p.staged = false;
            }
        }
    }
    Ok(n)
}

/// Tier-2 attempt for one staged batch: run the stitched body (compiling it
/// first once past the row floor) over the staged SoA lanes into `b.sel`.
/// false = the AOT tier owns this batch (below floor / sticky refused /
/// never armed). The one-deform-two-consumers property holds by
/// construction: the lanes handed to the body are views over the SAME
/// staged `SoaBatch` the fold/emit consumers read; the selection bitmap is
/// the only coupling currency.
fn stitch_qual_bitmap(b: &mut BatchSoa<'_>, n: u32) -> PgResult<bool> {
    // Disjoint field borrows: the body reads `soa` lanes and the runner
    // writes `sel`; `stitch` carries the program + telemetry.
    let BatchSoa { soa, sel, stitch, .. } = b;
    let Some(st) = stitch.as_mut() else { return Ok(false) };
    let mut ran = false;
    if !st.refused {
        if st.body.is_none() && st.rows_seen >= STITCH_ROW_FLOOR {
            match ::lanestitch::StitchedProgram::compile(&st.prog, st.ncols) {
                Some(p) => {
                    lane_trace(&format!(
                        "stitch compiled (cols={} bytes={} nanos={} simd={})",
                        st.ncols,
                        p.code_bytes,
                        p.stitch_nanos,
                        p.is_simd(),
                    ));
                    st.body = Some(p);
                }
                None => {
                    // Sticky per plan: classification / arch / kill switch /
                    // arena refuse — the AOT tier owns every later batch.
                    st.refused = true;
                    lane_trace("stitch refused (compile)");
                }
            }
        }
        if let Some(body) = &st.body {
            // Stack lane views over the staged SoA (zero allocation on the
            // per-batch path — doctrine rule 7).
            let mut lanes =
                [::lanestitch::Lane { values: &[], isnull: &[] }; ::lanestitch::MAX_COLS];
            for (c, lane) in lanes[..st.ncols].iter_mut().enumerate() {
                *lane = ::lanestitch::Lane {
                    values: soa.col_values(c),
                    isnull: soa.col_isnull(c),
                };
            }
            // The body writes the pipeline's own selection words (all-ones
            // over n on entry, tail clear; only failures store).
            let nwords = (n as usize).div_ceil(64);
            sel[..nwords].fill(!0u64);
            if n % 64 != 0 {
                sel[nwords - 1] = (1u64 << (n % 64)) - 1;
            }
            // Per-batch signature check + refuse-and-replay live in the
            // runner: lane drift or an oversize batch interprets this batch
            // (fail-open); an erroring stitched exit replays the batch on
            // the interpreter and refuses the body for good. Our compare
            // programs are non-erroring, so the error arm is unreachable —
            // kept because fail-open must never become wrong-answer.
            match body.run_into(&st.prog, n, &lanes[..st.ncols], &mut sel[..nwords])? {
                ::lanestitch::RunOutcome::Stitched => st.n_stitched += 1,
                ::lanestitch::RunOutcome::InterpretedDrift
                | ::lanestitch::RunOutcome::InterpretedSticky => st.n_interp += 1,
            }
            ran = true;
        }
    }
    if !ran {
        st.n_aot += 1;
    }
    st.rows_seen += n as u64;
    Ok(ran)
}

/// Stitched-projection attempt for one staged batch: compute the output
/// lanes for the TRUE qual survivors (the pure qual bits, fallback rows
/// masked out — their lanes are undeformed garbage). Sets `proj.staged`;
/// on any refuse/drift the batch's rows project per-row (`exec_project`),
/// and a runtime trap additionally refuses the body for good (sticky
/// refuse-and-replay: the body constructed NO error; the per-row replay
/// raises C's exact error on C's row).
/// Returns true when the caller must DISARM projection hosting (the
/// adaptive selectivity floor tripped): dropping the arm returns staging to
/// the qual-only column deform, i.e. the pre-projstitch lane behavior.
fn stitch_project(b: &mut BatchSoa<'_>, n: u32) -> bool {
    let BatchSoa { soa, sel, proj, .. } = b;
    let Some(p) = proj.as_mut() else { return false };
    p.staged = false;
    if !p.refused {
        if p.body.is_none() && p.rows_seen >= STITCH_ROW_FLOOR {
            match ::lanestitch::StitchedProjection::compile(&p.prog, p.ncols, p.nouts as usize) {
                Some(body) => {
                    lane_trace(&format!(
                        "proj stitch compiled (cols={} outs={} bytes={} nanos={})",
                        p.ncols, p.nouts, body.code_bytes, body.stitch_nanos,
                    ));
                    p.body = Some(body);
                }
                None => {
                    p.refused = true;
                    lane_trace("proj stitch refused (compile)");
                }
            }
        }
        if let Some(body) = &p.body {
            let nwords = (n as usize).div_ceil(64);
            // True survivors only: qual bits minus forced-fallback bits
            // (the AOT/stitched qual computed garbage bits for undeformed
            // fallback rows; they must never reach an erroring stencil).
            let mut proj_sel = [0u64; ::exectuples::SOA_BM_WORDS];
            for ((d, s), fb) in proj_sel[..nwords]
                .iter_mut()
                .zip(&sel[..nwords])
                .zip(soa.fallback_words())
            {
                *d = s & !fb;
            }
            let mut lanes =
                [::lanestitch::Lane { values: &[], isnull: &[] }; ::lanestitch::MAX_COLS];
            for (c, lane) in lanes[..p.ncols].iter_mut().enumerate() {
                *lane = ::lanestitch::Lane {
                    values: soa.col_values(c),
                    isnull: soa.col_isnull(c),
                };
            }
            // Output-lane views over the arm-time buffers (zero per-batch
            // allocation): one SOA_MAX_ROWS chunk per tlist column.
            let mut outs: [::lanestitch::OutLane<'_>; ::lanestitch::MAX_OUTS] = {
                let mut vch = p.out_values.chunks_mut(::exectuples::SOA_MAX_ROWS);
                let mut nch = p.out_isnull.chunks_mut(::exectuples::SOA_MAX_ROWS);
                core::array::from_fn(|_| ::lanestitch::OutLane {
                    values: vch.next().map(|c| &mut c[..n as usize]).unwrap_or(&mut []),
                    isnull: nch.next().map(|c| &mut c[..n as usize]).unwrap_or(&mut []),
                })
            };
            match body.run_into(n, &lanes[..p.ncols], &proj_sel[..nwords], &mut outs[..p.nouts as usize]) {
                ::lanestitch::ProjOutcome::Stitched => {
                    p.staged = true;
                    p.n_stitched += 1;
                    p.stitched_rows += n as u64;
                    p.stitched_survivors +=
                        proj_sel[..nwords].iter().map(|w| w.count_ones() as u64).sum::<u64>();
                }
                ::lanestitch::ProjOutcome::Drift => {
                    p.n_perrow += 1;
                }
                ::lanestitch::ProjOutcome::Refused => {
                    // Sticky refuse-and-replay: this plan's data errors —
                    // the per-row C path owns the batch (and all later
                    // ones), raising the exact error on the exact row.
                    p.refused = true;
                    p.n_perrow += 1;
                    lane_trace("proj stitch refused (replay: data error)");
                }
            }
        } else {
            p.n_perrow += 1;
        }
    } else {
        p.n_perrow += 1;
    }
    p.rows_seen += n as u64;
    // Adaptive selectivity disarm (one-shot, PROJ_MIN_SELECTIVITY_PCT):
    // only when hosting widened the deform; the caller drops the arm.
    if p.adapt && !p.adapt_checked && p.stitched_rows >= PROJ_ADAPT_ROWS {
        p.adapt_checked = true;
        if p.stitched_survivors * 100 < p.stitched_rows * PROJ_MIN_SELECTIVITY_PCT {
            lane_trace(&format!(
                "proj stitch disarmed (selectivity {}/{} below {}%)",
                p.stitched_survivors, p.stitched_rows, PROJ_MIN_SELECTIVITY_PCT
            ));
            return true;
        }
    }
    false
}

/// Map an execexpr comparator + its const onto the stitcher vocabulary,
/// canonicalizing the const to the lanestitch canonical-datum contract
/// (sign-extended integer image at the const's own width — `Datum::from_iN`).
fn stitch_cmp(
    cmp: ::execexpr::CmpOp,
    konst: ::datum::Datum,
) -> (::lanestitch::CmpOp, ::datum::Datum) {
    use ::execexpr::CmpOp as E;
    use ::lanestitch::CmpOp as S;
    let op = match cmp {
        E::Int4Eq => S::Int4Eq,
        E::Int4Ne => S::Int4Ne,
        E::Int4Lt => S::Int4Lt,
        E::Int4Le => S::Int4Le,
        E::Int4Gt => S::Int4Gt,
        E::Int4Ge => S::Int4Ge,
        E::Int8Eq => S::Int8Eq,
        E::Int8Ne => S::Int8Ne,
        E::Int8Lt => S::Int8Lt,
        E::Int8Le => S::Int8Le,
        E::Int8Gt => S::Int8Gt,
        E::Int8Ge => S::Int8Ge,
        E::Int2Eq => S::Int2Eq,
        E::Int2Ne => S::Int2Ne,
        E::Int2Lt => S::Int2Lt,
        E::Int2Le => S::Int2Le,
        E::Int2Gt => S::Int2Gt,
        E::Int2Ge => S::Int2Ge,
        E::Int84Eq => S::Int84Eq,
        E::Int84Ne => S::Int84Ne,
        E::Int84Lt => S::Int84Lt,
        E::Int84Le => S::Int84Le,
        E::Int84Gt => S::Int84Gt,
        E::Int84Ge => S::Int84Ge,
        E::Int48Eq => S::Int48Eq,
        E::Int48Ne => S::Int48Ne,
        E::Int48Lt => S::Int48Lt,
        E::Int48Le => S::Int48Le,
        E::Int48Gt => S::Int48Gt,
        E::Int48Ge => S::Int48Ge,
        E::Int24Eq => S::Int24Eq,
        E::Int24Ne => S::Int24Ne,
        E::Int24Lt => S::Int24Lt,
        E::Int24Le => S::Int24Le,
        E::Int24Gt => S::Int24Gt,
        E::Int24Ge => S::Int24Ge,
        E::Int42Eq => S::Int42Eq,
        E::Int42Ne => S::Int42Ne,
        E::Int42Lt => S::Int42Lt,
        E::Int42Le => S::Int42Le,
        E::Int42Gt => S::Int42Gt,
        E::Int42Ge => S::Int42Ge,
        E::OidEq => S::OidEq,
        E::OidNe => S::OidNe,
        E::OidLt => S::OidLt,
        E::OidLe => S::OidLe,
        E::OidGt => S::OidGt,
        E::OidGe => S::OidGe,
        E::Float4Eq => S::Float4Eq,
        E::Float4Ne => S::Float4Ne,
        E::Float4Lt => S::Float4Lt,
        E::Float4Le => S::Float4Le,
        E::Float4Gt => S::Float4Gt,
        E::Float4Ge => S::Float4Ge,
        E::Float8Eq => S::Float8Eq,
        E::Float8Ne => S::Float8Ne,
        E::Float8Lt => S::Float8Lt,
        E::Float8Le => S::Float8Le,
        E::Float8Gt => S::Float8Gt,
        E::Float8Ge => S::Float8Ge,
        E::Float48Eq => S::Float48Eq,
        E::Float48Ne => S::Float48Ne,
        E::Float48Lt => S::Float48Lt,
        E::Float48Le => S::Float48Le,
        E::Float48Gt => S::Float48Gt,
        E::Float48Ge => S::Float48Ge,
        E::Float84Eq => S::Float84Eq,
        E::Float84Ne => S::Float84Ne,
        E::Float84Lt => S::Float84Lt,
        E::Float84Le => S::Float84Le,
        E::Float84Gt => S::Float84Gt,
        E::Float84Ge => S::Float84Ge,
    };
    // The const operand's own width per comparator family (the b side).
    let k = match cmp {
        E::Int2Eq | E::Int2Ne | E::Int2Lt | E::Int2Le | E::Int2Gt | E::Int2Ge
        | E::Int42Eq | E::Int42Ne | E::Int42Lt | E::Int42Le | E::Int42Gt | E::Int42Ge => {
            ::datum::Datum::from_i16(konst.as_i16())
        }
        E::Int4Eq | E::Int4Ne | E::Int4Lt | E::Int4Le | E::Int4Gt | E::Int4Ge
        | E::Int84Eq | E::Int84Ne | E::Int84Lt | E::Int84Le | E::Int84Gt | E::Int84Ge
        | E::Int24Eq | E::Int24Ne | E::Int24Lt | E::Int24Le | E::Int24Gt | E::Int24Ge => {
            ::datum::Datum::from_i32(konst.as_i32())
        }
        E::Int8Eq | E::Int8Ne | E::Int8Lt | E::Int8Le | E::Int8Gt | E::Int8Ge
        | E::Int48Eq | E::Int48Ne | E::Int48Lt | E::Int48Le | E::Int48Gt | E::Int48Ge => {
            ::datum::Datum::from_i64(konst.as_i64())
        }
        // Oid: sign-extend the u32 image (the stitcher's canonical-datum
        // contract — makes the 2x64 unsigned NEON compares exact).
        E::OidEq | E::OidNe | E::OidLt | E::OidLe | E::OidGt | E::OidGe => {
            ::datum::Datum::from_i32(konst.as_u32() as i32)
        }
        // Float consts: raw bit patterns at the const's own width (low-word
        // f32 / full-word f64 — the b side of each family).
        E::Float4Eq | E::Float4Ne | E::Float4Lt | E::Float4Le | E::Float4Gt | E::Float4Ge
        | E::Float84Eq | E::Float84Ne | E::Float84Lt | E::Float84Le | E::Float84Gt
        | E::Float84Ge => ::datum::Datum::from_f32(konst.as_f32()),
        E::Float8Eq | E::Float8Ne | E::Float8Lt | E::Float8Le | E::Float8Gt | E::Float8Ge
        | E::Float48Eq | E::Float48Ne | E::Float48Lt | E::Float48Le | E::Float48Gt
        | E::Float48Ge => ::datum::Datum::from_f64(konst.as_f64()),
    };
    (op, k)
}

/// Arm the tier-2 stitched body for an armed kernel-qual bitmap. Called ONLY
/// by the lane driver on drain pipelines feeding breakers (design rule: the
/// stitched segment never runs on pull-one-tuple pipelines). Idempotent; a
/// no-op when the bitmap is not armed, the stitcher is unavailable, or a
/// clause column exceeds the stitcher's lane window. Compilation itself is
/// deferred past the row floor (`stitch_qual_bitmap`); this only translates
/// the clause list into the stitch program.
pub fn seq_scan_stitch_arm(node: &mut SeqScanState<'_>) {
    let Some(b) = node.batch_soa.as_deref_mut() else { return };
    if !b.qual_armed
        || b.nquals < STITCH_MIN_CLAUSES
        || b.stitch.is_some()
        || !::lanestitch::available()
    {
        return;
    }
    let mut prog = ::lanestitch::Program::new();
    let mut ncols = 0usize;
    for &(col, cmp, konst) in &b.quals[..b.nquals as usize] {
        if col as usize >= ::lanestitch::MAX_COLS {
            return;
        }
        let (op, k) = stitch_cmp(cmp, konst);
        let kix = prog.push_const(::datum::NullableDatum { value: k, isnull: false });
        prog.steps.push(::lanestitch::Step::LoadLane { col, out: 0 });
        prog.steps.push(::lanestitch::Step::LoadConst { k: kix, out: 1 });
        prog.steps.push(::lanestitch::Step::Cmp { op, a: 0, b: 1, out: 2 });
        prog.steps.push(::lanestitch::Step::Qual { a: 2 });
        ncols = ncols.max(col as usize + 1);
    }
    let nquals = b.nquals;
    b.stitch = Some(QualStitch {
        prog,
        ncols,
        body: None,
        rows_seen: 0,
        refused: false,
        n_stitched: 0,
        n_aot: 0,
        n_interp: 0,
    });
    lane_trace(&format!("stitch armed (clauses={nquals})"));
}

/// PGRUST_LANE_V2_TRACE engagement summary, emitted when the scan releases
/// its batch state (end / park).
fn stitch_trace_summary(node: &SeqScanState<'_>) {
    if let Some(b) = node.batch_soa.as_deref() {
        if let Some(st) = &b.stitch {
            lane_trace(&format!(
                "stitch summary: stitched={} aot={} interp={} refused={}",
                st.n_stitched, st.n_aot, st.n_interp, st.refused
            ));
        }
        if let Some(p) = &b.proj {
            lane_trace(&format!(
                "proj stitch summary: stitched={} perrow={} refused={}",
                p.n_stitched, p.n_perrow, p.refused
            ));
        }
    }
}

/// Kill switch for measurement: PGRUST_LANESTITCH_PROJ=0|off disables the
/// stitched-projection tier (the per-row `exec_project` path owns projected
/// scans, i.e. exactly the pre-projstitch lane behavior).
fn proj_stitch_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANESTITCH_PROJ").as_deref(), Ok("0") | Ok("off"))
    })
}

fn proj_arith(op: ::execexpr::ProjArithOp) -> ::lanestitch::ArithOp {
    use ::execexpr::ProjArithOp as E;
    use ::lanestitch::ArithOp as S;
    match op {
        E::Add2 => S::Add2,
        E::Sub2 => S::Sub2,
        E::Mul2 => S::Mul2,
        E::Div2 => S::Div2,
        E::Add4 => S::Add4,
        E::Sub4 => S::Sub4,
        E::Mul4 => S::Mul4,
        E::Div4 => S::Div4,
        E::Add8 => S::Add8,
        E::Sub8 => S::Sub8,
        E::Mul8 => S::Mul8,
        E::Div8 => S::Div8,
    }
}

/// Canonicalize an arith const to the lanestitch canonical-datum contract
/// (sign-extended image at the op's own width — same-width families only).
fn proj_arith_konst(op: ::execexpr::ProjArithOp, konst: ::datum::Datum) -> ::datum::Datum {
    use ::execexpr::ProjArithOp as E;
    match op {
        E::Add2 | E::Sub2 | E::Mul2 | E::Div2 => ::datum::Datum::from_i16(konst.as_i16()),
        E::Add4 | E::Sub4 | E::Mul4 | E::Div4 => ::datum::Datum::from_i32(konst.as_i32()),
        E::Add8 | E::Sub8 | E::Mul8 | E::Div8 => ::datum::Datum::from_i64(konst.as_i64()),
    }
}

/// The SoA prefix a stitched projection needs (max read attnum + 1), when
/// this scan's projection is census-covered and hostable: lane driver
/// callers widen their `seq_scan_batch_soa_prepare` prefix by this BEFORE
/// arming (`seq_scan_proj_stitch_arm` requires the staged prefix to cover
/// it). None = no hostable projection (no ProjInfo / census refused /
/// out-of-window / kill switch / stitcher unavailable).
///
/// Admission economics (design §4 — fail closed until measured): Var-only
/// tlists are refused (`any_arith`) — the stitched fill would only replace
/// the per-row Assign walk while WIDENING the deform prefix (a real
/// per-batch deform cost on every staged row), an unproven trade. Computed
/// columns are where the fused lanes carry a measured win (see the
/// projstitch A/B in the branch log). Ratchet DOWN (admit Var-only) only
/// with a measurement, STITCH_MIN_CLAUSES-style.
pub fn seq_scan_proj_stitch_prefix(node: &SeqScanState<'_>) -> Option<i32> {
    if !proj_stitch_enabled() || !::lanestitch::available() {
        return None;
    }
    let proj = node.ss.ps_ProjInfo.as_ref()?;
    let cols = proj.pi_state.scan_proj_cols()?;
    if !cols.any_arith() {
        return None;
    }
    if cols.n as usize > ::lanestitch::MAX_OUTS
        || cols.max_attnum() as usize >= ::lanestitch::MAX_COLS
    {
        return None;
    }
    Some(cols.max_attnum() as i32 + 1)
}

/// Arm the stitched-projection tier for an armed kernel-qual bitmap whose
/// staged prefix covers the projection's read columns. Called ONLY by the
/// lane driver on drain pipelines (the stitched segments never run on
/// pull-one-tuple pipelines). Idempotent; a no-op when unhostable — the
/// per-row `exec_project` path stays untouched (fail closed). Compilation
/// defers past the row floor (`stitch_project`); this translates the census
/// into the stitch program and allocates the output lanes once.
pub fn seq_scan_proj_stitch_arm<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    let Some(prefix) = seq_scan_proj_stitch_prefix(node) else { return };
    let Some(proj) = node.ss.ps_ProjInfo.as_ref() else { return };
    let Some(cols) = proj.pi_state.scan_proj_cols() else { return };
    let result_slot = proj.pi_result_slot;
    let Some(b) = node.batch_soa.as_deref_mut() else { return };
    if !b.qual_armed || b.proj.is_some() || (b.plan.ncols() as i32) < prefix {
        return;
    }
    // The projection writes the result slot's value arrays positionally;
    // its descriptor arity must equal the census arity (defense in depth —
    // the projection program was compiled against this slot).
    if estate.slot_mut(result_slot).base_mut().tts_values.len() != cols.n as usize {
        return;
    }
    let mut prog = ::lanestitch::Program::new();
    for (j, col) in cols.cols[..cols.n as usize].iter().enumerate() {
        match *col {
            ::execexpr::ScanProjCol::Var { attnum } => {
                prog.steps.push(::lanestitch::Step::LoadLane { col: attnum, out: 0 });
                prog.steps.push(::lanestitch::Step::StoreOut { a: 0, out: j as u16 });
            }
            ::execexpr::ScanProjCol::ArithVV { op, a, b: bcol } => {
                prog.steps.push(::lanestitch::Step::LoadLane { col: a, out: 0 });
                prog.steps.push(::lanestitch::Step::LoadLane { col: bcol, out: 1 });
                prog.steps.push(::lanestitch::Step::Arith {
                    op: proj_arith(op),
                    a: 0,
                    b: 1,
                    out: 2,
                });
                prog.steps.push(::lanestitch::Step::StoreOut { a: 2, out: j as u16 });
            }
            ::execexpr::ScanProjCol::ArithVK { op, attnum, konst, var_is_arg0 } => {
                let k = proj_arith_konst(op, konst);
                let kix = prog
                    .push_const(::datum::NullableDatum { value: k, isnull: false });
                prog.steps.push(::lanestitch::Step::LoadLane { col: attnum, out: 0 });
                prog.steps.push(::lanestitch::Step::LoadConst { k: kix, out: 1 });
                let (a, bb) = if var_is_arg0 { (0u8, 1u8) } else { (1u8, 0u8) };
                prog.steps.push(::lanestitch::Step::Arith {
                    op: proj_arith(op),
                    a,
                    b: bb,
                    out: 2,
                });
                prog.steps.push(::lanestitch::Step::StoreOut { a: 2, out: j as u16 });
            }
        }
    }
    let mcx = estate.es_query_cxt;
    let cells = cols.n as usize * ::exectuples::SOA_MAX_ROWS;
    // The adaptive selectivity disarm applies iff hosting WIDENS the
    // per-batch deform beyond the qual's own staging: single-clause
    // qual-only staging deforms one column, multi-clause the clause-covering
    // prefix; anything wider is projection-hosting cost that low-selectivity
    // scans cannot amortize (PROJ_MIN_SELECTIVITY_PCT).
    let qual_deform_cols = if b.qual_only && b.nquals == 1 {
        1
    } else {
        b.quals[..b.nquals as usize].iter().map(|&(c, _, _)| c as usize + 1).max().unwrap_or(0)
    };
    b.proj = Some(ProjStitch {
        prog,
        ncols: cols.max_attnum() as usize + 1,
        nouts: cols.n as u16,
        body: None,
        rows_seen: 0,
        refused: false,
        staged: false,
        adapt: b.plan.ncols() as usize > qual_deform_cols,
        adapt_checked: false,
        stitched_rows: 0,
        stitched_survivors: 0,
        out_values: ::mcx::vec_from_elem_in(mcx, ::datum::Datum::null(), cells),
        out_isnull: ::mcx::vec_from_elem_in(mcx, false, cells),
        n_stitched: 0,
        n_perrow: 0,
    });
    lane_trace(&format!("proj stitch armed (cols={})", cols.n));
}

/// Bitmap-armed batch census: rows of the staged batch passing the kernel
/// qual. Bitmap hits count with no per-row work; forced fallback rows (the
/// SoA prefix deform skipped them) run the per-row store+qual path. None =
/// no bitmap qual staged, the per-row drain owns the batch.
pub fn seq_scan_batch_qual_count<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    n: u32,
) -> PgResult<Option<u32>> {
    let nwords = (n as usize).div_ceil(64);
    let mut fallback = [0u64; ::exectuples::SOA_BM_WORDS];
    let mut count = 0u32;
    {
        let Some(b) = node.batch_soa.as_deref() else { return Ok(None) };
        if !b.qual_armed {
            return Ok(None);
        }
        for (w, fb) in b.soa.fallback_words()[..nwords].iter().enumerate() {
            count += (b.sel[w] & !fb).count_ones();
            fallback[w] = *fb;
        }
    }
    for (w, mut bits) in fallback[..nwords].iter().copied().enumerate() {
        while bits != 0 {
            let i = (w as u32) * 64 + bits.trailing_zeros();
            bits &= bits - 1;
            if seq_scan_batch_fetch(node, estate, i)? {
                count += 1;
            }
        }
    }
    Ok(Some(count))
}

/// Store row `i` of the staged batch and apply the scan qual; false =
/// filtered (bitmap-armed batches test the selection bit, not the kernel).
#[inline(always)]
pub fn seq_scan_batch_fetch<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    i: u32,
) -> PgResult<bool> {
    if let Some(b) = node.batch_soa.as_deref() {
        if b.qual_armed {
            if b.sel[(i / 64) as usize] & (1u64 << (i % 64)) == 0 {
                return Ok(false);
            }
            if !b.soa.is_fallback(i) {
                seq_scan_batch_store(node, estate, i);
                return Ok(true);
            }
        }
    }
    seq_scan_batch_store(node, estate, i);
    let ecxt = node.ss.ps_ExprContext;
    match node.ss.qual.as_deref_mut() {
        None => Ok(true),
        Some(q) => {
            // Per-tuple result mcx for arg-detoasting quals (C's
            // ecxt_per_tuple_memory; the emit-entry ExprContext reset frees
            // it) — mirrors `exec_scan_impl`'s per-row arming; es_query_cxt
            // would otherwise accumulate over the whole fused feed.
            let per_tuple = estate.ecxt(ecxt).per_tuple_mcx();
            // SAFETY: reset-only context, arena-boxed (address-stable),
            // outlives the plan.
            unsafe { q.arm_result_mcx_raw(per_tuple) };
            let slot_id = node.ss.ss_ScanTupleSlot;
            let mut slots = ::execexpr::EvalSlots {
                scan: Some(estate.slot_mut(slot_id)),
                inner: None,
                outer: None,
            };
            ::execexpr::exec_qual(Some(q), &mut slots)
        }
    }
}

#[inline(always)]
pub fn seq_scan_batch_store<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    i: u32,
) {
    let mcx = estate.es_query_cxt;
    let scandesc =
        node.ss.ss_currentScanDesc.as_mut().expect("batch store before batch fetch");
    let slot = estate.slot_mut(node.ss.ss_ScanTupleSlot);
    ::tableam::table_scan_batch_store_slot(mcx, scandesc, i, slot);
    if let Some(b) = node.batch_soa.as_ref() {
        if b.publish {
            ::exectuples::soa_store_prefix(slot, &b.soa, i);
        }
    }
}

/// Fused-feed emit: reset the per-tuple context, fetch row `i`, apply the
/// qual, project — `ExecScanExtended`'s body over a staged batch row. None =
/// filtered; Some = the scan's output slot.
///
/// Subplan- and param-bearing quals/projections run `exec_scan_impl`'s exact
/// arms (pending-initplan param evaluation, then the suspension-driven
/// subplan qual/projection drivers) — same per-row program, same order, same
/// per-tuple context discipline → byte-identical to the per-tuple path.
#[inline(always)]
pub fn seq_scan_batch_emit<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    i: u32,
) -> PgResult<Option<ExecSlotId>> {
    estate.ecxt_mut(node.ss.ps_ExprContext).reset();
    // Stitched-projection fast lane: this batch's output lanes are staged
    // (qual bitmap computed, projection body ran over the true survivors),
    // so a bitmap hit fills the result slot straight from the output lanes —
    // no scan-slot store, no per-row `exec_project`. Same values, same
    // isnull, same result-slot state as the per-row path (the census admits
    // only Var images and strict int arith, whose outputs are exactly the
    // per-row program's Datums). Fallback rows (no lane values) fall through
    // to the per-row path below, as do batches the body refused/drifted on.
    {
        let SeqScanState { ss, batch_soa, .. } = node;
        if let Some(b) = batch_soa.as_deref() {
            if b.qual_armed && b.nwords > 0 {
                if let Some(p) = &b.proj {
                    if p.staged {
                        if b.sel[(i / 64) as usize] & (1u64 << (i % 64)) == 0 {
                            return Ok(None);
                        }
                        if !b.soa.is_fallback(i) {
                            let proj =
                                ss.ps_ProjInfo.as_ref().expect("proj stitch armed with ProjInfo");
                            let result_id = proj.pi_result_slot;
                            let mcx = estate.es_query_cxt;
                            let slot = estate.slot_mut(result_id);
                            ::exectuples::exec_clear_tuple(slot, mcx);
                            let base = slot.base_mut();
                            let idx = i as usize;
                            for j in 0..p.nouts as usize {
                                base.tts_values[j] =
                                    p.out_values[j * ::exectuples::SOA_MAX_ROWS + idx];
                                base.tts_isnull[j] =
                                    p.out_isnull[j * ::exectuples::SOA_MAX_ROWS + idx];
                            }
                            ::exectuples::exec_store_virtual_tuple(slot);
                            return Ok(Some(result_id));
                        }
                    }
                }
            }
        }
    }
    let qual_hosted = node
        .ss
        .qual
        .as_deref()
        .is_some_and(|q| q.has_subplan() || !q.param_exec_deps().is_empty());
    let passes = if qual_hosted {
        // Subplan/param quals never arm the kernel bitmap (the kernel shapes
        // are subplan- and param-free), so the plain store path is the only
        // one live here.
        debug_assert!(node.batch_soa.as_deref().is_none_or(|b| !b.qual_armed));
        seq_scan_batch_store(node, estate, i);
        let scan_id = node.ss.ss_ScanTupleSlot;
        let ecxt = node.ss.ps_ExprContext;
        estate.ecxt_mut(ecxt).ecxt_scantuple = Some(scan_id);
        // ExecEvalParamExec pending-initplan arm, hoisted out of the
        // interpreter — mirrors `exec_scan_impl`.
        let deps = node.ss.qual.as_deref().unwrap().param_exec_deps();
        if !deps.is_empty() {
            ::executils::exec_eval_param_exec_params(estate, deps)?;
        }
        if node.ss.qual.as_deref().is_some_and(|q| q.has_subplan()) {
            ::executils::exec_qual_with_subplans(node.ss.qual.as_deref_mut(), estate, ecxt)?
        } else {
            // Param-only qual (initplan or correlated exec params, no subplan
            // steps): the params are plain datum reads once evaluated above —
            // `exec_scan_impl`'s ordinary per-row qual arm.
            let per_tuple = estate.ecxt(ecxt).per_tuple_mcx();
            // SAFETY: reset-only context, arena-boxed (address-stable),
            // outlives the plan.
            unsafe { node.ss.qual.as_deref_mut().unwrap().arm_result_mcx_raw(per_tuple) };
            let mut slots = ::execexpr::EvalSlots {
                scan: Some(estate.slot_mut(scan_id)),
                inner: None,
                outer: None,
            };
            ::execexpr::exec_qual(node.ss.qual.as_deref_mut(), &mut slots)?
        }
    } else {
        seq_scan_batch_fetch(node, estate, i)?
    };
    if !passes {
        return Ok(None);
    }
    let scan_id = node.ss.ss_ScanTupleSlot;
    let ecxt = node.ss.ps_ExprContext;
    estate.ecxt_mut(ecxt).ecxt_scantuple = Some(scan_id);
    if node.ss.ps_ProjInfo.is_none() {
        return Ok(Some(scan_id));
    };
    // C reads projection initplan params inside the projection, which never
    // runs on a qual-rejected tuple — mirrors `exec_scan_impl`.
    {
        let deps = node.ss.ps_ProjInfo.as_ref().unwrap().pi_state.param_exec_deps();
        if !deps.is_empty() {
            ::executils::exec_eval_param_exec_params(estate, deps)?;
        }
    }
    let proj = node.ss.ps_ProjInfo.as_mut().unwrap();
    let result_id = proj.pi_result_slot;
    if proj.pi_state.has_subplan() {
        ::executils::exec_project_with_subplans(&mut proj.pi_state, estate, ecxt, result_id)?;
        return Ok(Some(result_id));
    }
    // By-ref projection results (and callee scratch) must live in the
    // per-tuple memory reset at the next emit entry (C projects into
    // ecxt_per_tuple_memory) — mirrors `exec_scan_impl`; es_query_cxt would
    // otherwise accumulate over the whole fused feed.
    // SAFETY: reset-only context, arena-boxed (address-stable), outlives the
    // plan.
    unsafe {
        let per_tuple = estate.ecxt(ecxt).per_tuple_mcx();
        proj.pi_state.arm_result_mcx_raw(per_tuple);
    }
    let mcx = estate.es_query_cxt;
    let result_id = proj.pi_result_slot;
    let (scan_slot, result_slot) = ::execscan::slot_pair(estate, scan_id, result_id);
    let mut slots = ::execexpr::EvalSlots { scan: Some(scan_slot), inner: None, outer: None };
    ::execexpr::exec_project_prearmed(&mut proj.pi_state, &mut slots, result_slot, mcx)?;
    Ok(Some(result_id))
}

/// `ExecSeqScan` + its four specialized variants, dispatched on the enum
/// selected at init instead of C's per-variant function pointers.
pub fn exec_seq_scan<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    match node.variant {
        SeqScanVariant::Plain => exec_scan_extended::<_, false, false>(node, estate),
        SeqScanVariant::WithQual => {
            if scan_batch_ready(node, estate)? {
                return exec_seq_scan_batch::<false>(node, estate);
            }
            exec_scan_extended::<_, true, false>(node, estate)
        }
        SeqScanVariant::PlainBloom => exec_seq_scan_bloom(node, estate),
        SeqScanVariant::WithProject => exec_scan_extended::<_, false, true>(node, estate),
        SeqScanVariant::WithQualProject => {
            if scan_batch_ready(node, estate)? {
                return exec_seq_scan_batch::<true>(node, estate);
            }
            exec_scan_extended::<_, true, true>(node, estate)
        }
        SeqScanVariant::Epq => exec_scan_epq(node, estate),
    }
}

#[inline(always)]
fn scan_batch_ready<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    match node.scan_batch {
        ScanBatchMode::On => Ok(true),
        ScanBatchMode::Off => Ok(false),
        ScanBatchMode::Unknown => scan_batch_probe(node, estate),
    }
}

// Once per scan: the page-batch bitmap-qual drive covers uninstrumented
// forward-only kernel-qual scans (subplan-free projection); everything else
// keeps the per-tuple drive.
#[inline(never)]
fn scan_batch_probe<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    node.scan_batch = ScanBatchMode::Off;
    if !node.batch_allowed || node.ss.instr_idx.is_some() || estate.es_epq_active {
        return Ok(false);
    }
    if let Some(p) = node.ss.ps_ProjInfo.as_ref() {
        if p.pi_state.has_subplan() || !p.pi_state.param_exec_deps().is_empty() {
            return Ok(false);
        }
    }
    let Some(q) = node.ss.qual.as_deref() else { return Ok(false) };
    let ::execexpr::Kernel::QualScanVarCmpConst { attnum, .. } = q.kernel() else {
        return Ok(false);
    };
    node.ensure_scandesc(estate)?;
    if !::tableam::table_scan_supports_pagebatch(node.ss.ss_currentScanDesc.as_ref().unwrap()) {
        return Ok(false);
    }
    seq_scan_batch_soa_prepare(node, estate, attnum as i32 + 1, true, false, false);
    if node.batch_soa.as_deref().is_some_and(|b| b.qual_armed) {
        node.scan_batch = ScanBatchMode::On;
        return Ok(true);
    }
    node.batch_soa = None;
    Ok(false)
}

#[inline(never)]
fn exec_seq_scan_batch<'mcx, const PROJ: bool>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    estate.ecxt_mut(node.ss.ps_ExprContext).reset();
    loop {
        let next = node.batch_soa.as_deref_mut().expect("batch drive armed").next_selected();
        let Some(i) = next else {
            let n = seq_scan_next_pagebatch(node, estate)?;
            if n == 0 {
                let mcx = estate.es_query_cxt;
                if PROJ {
                    let proj = node.ss.ps_ProjInfo.as_ref().unwrap();
                    ::exectuples::exec_clear_tuple(estate.slot_mut(proj.pi_result_slot), mcx);
                }
                return Ok(None);
            }
            continue;
        };
        if !seq_scan_batch_fetch(node, estate, i)? {
            continue;
        }
        let scan_id = node.ss.ss_ScanTupleSlot;
        estate.ecxt_mut(node.ss.ps_ExprContext).ecxt_scantuple = Some(scan_id);
        if !PROJ {
            return Ok(Some(scan_id));
        }
        let mcx = estate.es_query_cxt;
        let proj = node.ss.ps_ProjInfo.as_mut().unwrap();
        let result_id = proj.pi_result_slot;
        let (scan_slot, result_slot) = ::execscan::slot_pair(estate, scan_id, result_id);
        let mut slots =
            ::execexpr::EvalSlots { scan: Some(scan_slot), inner: None, outer: None };
        ::execexpr::exec_project(&mut proj.pi_state, &mut slots, result_slot, mcx)?;
        return Ok(Some(result_id));
    }
}

/// Hashjoin Bloom pushdown seat: arm (Some) or disarm (None) after a hash
/// build. Runtime gate only — plans are untouched; Instrumented outers never
/// reach here, so EXPLAIN ANALYZE keeps the per-tuple drive and its counters.
pub fn seq_scan_set_bloom<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    push: Option<(std::rc::Rc<::nodehash::ProbeBloom<'mcx>>, u16)>,
) -> PgResult<bool> {
    if node.variant == SeqScanVariant::PlainBloom {
        node.variant = SeqScanVariant::Plain;
        node.bloom = None;
    }
    let Some((filter, col)) = push else { return Ok(false) };
    if node.variant != SeqScanVariant::Plain
        || !node.batch_allowed
        || node.ss.instr_idx.is_some()
        || estate.es_epq_active
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
    {
        return Ok(false);
    }
    node.ensure_scandesc(estate)?;
    if !::tableam::table_scan_supports_pagebatch(node.ss.ss_currentScanDesc.as_ref().unwrap()) {
        return Ok(false);
    }
    let mcx = estate.es_query_cxt;
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    let atts: &[_] = &rel.rd_att.compact_attrs;
    let Some(plan) = ::exectuples::SoaDeformPlan::try_new(mcx, atts, col as usize + 1) else {
        return Ok(false);
    };
    node.bloom = Some(::mcx::PgBox::new_in(
        BloomScan {
            soa: ::exectuples::SoaBatch::new_in(mcx, plan.ncols()),
            plan,
            filter,
            col,
            sel: [0; ::exectuples::SOA_BM_WORDS],
            nwords: 0,
            cur_word: 0,
            cur_bits: 0,
            seen: 0,
            kept: 0,
        },
        mcx,
    ));
    node.variant = SeqScanVariant::PlainBloom;
    Ok(true)
}

// Plain-scan Bloom drive: stage a page, deform the key column only, keep
// rows the filter admits (misses prove no hash match; NULL keys test hash 0
// like the Hash32Var kernel; fallback rows pass conservatively). Same tuple
// order and slot state as the per-row Plain path for every surviving row.
#[inline(never)]
fn exec_seq_scan_bloom<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    estate.ecxt_mut(node.ss.ps_ExprContext).reset();
    loop {
        let next = node.bloom.as_deref_mut().expect("bloom drive armed").next_selected();
        let Some(i) = next else {
            // Page boundary: rs_cindex parks at page end, so the per-tuple
            // walk resumes on the NEXT page — disarming here is order-exact.
            // Break-even ~9% rejected (filter ~45 instr/row vs ~500 saved).
            {
                let b = node.bloom.as_deref().expect("bloom drive armed");
                if b.seen >= 1024 && 8 * (b.kept as u64) > 7 * (b.seen as u64) {
                    node.bloom = None;
                    node.variant = SeqScanVariant::Plain;
                    return exec_scan_extended::<_, false, false>(node, estate);
                }
            }
            node.ensure_scandesc(estate)?;
            let SeqScanState { ss, bloom, .. } = node;
            // SAFETY: written by ensure_scandesc when None.
            let scandesc = unsafe { ss.ss_currentScanDesc.as_mut().unwrap_unchecked() };
            let n = ::tableam::table_scan_getnextpagebatch(scandesc)?;
            if n == 0 {
                return Ok(None);
            }
            let b = &mut **bloom.as_mut().expect("bloom drive armed");
            ::tableam::table_scan_batch_deform(scandesc, &b.plan, &mut b.soa, Some(b.col));
            b.filter.sel_hash32_low32(
                b.soa.col_values(b.col as usize),
                b.soa.col_isnull(b.col as usize),
                &mut b.sel,
            );
            let nwords = (n as usize).div_ceil(64);
            // Skipped rows carry a forced bit: no columnar key, pass through.
            for (w, fb) in b.sel[..nwords].iter_mut().zip(b.soa.fallback_words()) {
                *w |= fb;
            }
            b.nwords = nwords as u32;
            b.cur_word = 0;
            b.cur_bits = b.sel[0];
            b.seen += n;
            b.kept += b.sel[..nwords].iter().map(|w| w.count_ones()).sum::<u32>();
            continue;
        };
        let mcx = estate.es_query_cxt;
        let scandesc =
            node.ss.ss_currentScanDesc.as_mut().expect("bloom fetch after page stage");
        let slot = estate.slot_mut(node.ss.ss_ScanTupleSlot);
        ::tableam::table_scan_batch_store_slot(mcx, scandesc, i, slot);
        return Ok(Some(node.ss.ss_ScanTupleSlot));
    }
}

/// `ExecInitSeqScan`; opens the scan relation through the estate range table.
pub fn exec_init_seq_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &SeqScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<SeqScanState<'mcx>> {
    let rel = exec_open_scan_relation(estate, node, eflags)?;
    let mut state = exec_init_seq_scan_rel(mcx, node, estate, rel)?;
    state.batch_allowed = eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0;
    Ok(state)
}

/// `ExecOpenScanRelation`.
fn exec_open_scan_relation<'mcx>(
    estate: &mut EStateData<'mcx>,
    node: &SeqScan<'mcx>,
    eflags: i32,
) -> PgResult<Relation<'mcx>> {
    let rel = estate.exec_get_range_table_relation(node.scan.scanrelid, false)?;
    if eflags & (EXEC_FLAG_EXPLAIN_ONLY | EXEC_FLAG_WITH_NO_DATA) == 0
        && !rel.rd_rel.relispopulated
    {
        return Err(unpopulated_matview(rel));
    }
    Ok(rel.alias())
}

#[cold]
#[inline(never)]
fn unpopulated_matview(rel: &Relation<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "materialized view \"{}\" has not been populated",
            rel.name()
        ))
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .with_hint("Use the REFRESH MATERIALIZED VIEW command."),
    )
}

/// C divergence: init over a caller-opened relation (test surface;
/// `exec_init_seq_scan` is the real path through the estate range table).
pub fn exec_init_seq_scan_rel<'mcx>(
    mcx: Mcx<'mcx>,
    node: &SeqScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    rel: Relation<'mcx>,
) -> PgResult<SeqScanState<'mcx>> {
    debug_assert!(node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none());

    let ps_ExprContext = estate.exec_assign_expr_context();
    let kind = table_slot_callbacks(&rel);
    let ss_ScanTupleSlot = estate.exec_init_extra_tuple_slot(Some(rel.rd_att.clone()), kind);

    let mut ss = ScanState {
        qual: None,
        ps_ProjInfo: None,
        ps_ExprContext,
        scanrelid: node.scan.scanrelid,
        ss_currentRelation: Some(rel),
        ss_currentScanDesc: None,
        ss_ScanTupleSlot,
        instr_idx: None,
    };
    execscan::exec_assign_scan_projection_info(mcx, estate, &mut ss, &node.scan.plan.targetlist)?;
    let params = estate.param_bind();
    ss.qual = ::executils::with_subplan_compile_env(estate, |env| {
        ::execexpr::exec_init_qual_subplans(mcx, &node.scan.plan.qual, params, env)
    })?;

    let variant = if estate.es_epq_active {
        SeqScanVariant::Epq
    } else {
        match (ss.qual.is_some(), ss.ps_ProjInfo.is_some()) {
            (false, false) => SeqScanVariant::Plain,
            (true, false) => SeqScanVariant::WithQual,
            (false, true) => SeqScanVariant::WithProject,
            (true, true) => SeqScanVariant::WithQualProject,
        }
    };
    let cb_scan = match rel_am_is_cbstore(ss.ss_currentRelation.as_ref().unwrap()) {
        false => None,
        true => Some(std::boxed::Box::new(cb_scan_info(node, &ss)?)),
    };
    Ok(SeqScanState {
        ss,
        variant,
        plan_node_id: node.scan.plan.plan_node_id,
        parallel_aware: node.scan.plan.parallel_aware,
        parallel: None,
        batch_soa: None,
        scan_batch: ScanBatchMode::Unknown,
        batch_allowed: false,
        bloom: None,
        lane_pos: 0,
        lane_n: 0,
        lane_verdict: None,
        cb_scan,
    })
}

fn rel_am_is_cbstore(rel: &Relation<'_>) -> bool {
    ::tableam::TableAm::of(rel) == Some(::tableam::TableAm::Cbstore)
}

/// A cbstore relation drives this scan (lane arm gates; the lane's cbscan
/// engagement class ticks on this).
pub fn seq_scan_is_cbstore(node: &SeqScanState<'_>) -> bool {
    node.cb_scan.is_some()
}

// Plan-derived need-set + zone-mappable conjuncts for a cbstore scan.
fn cb_scan_info<'mcx>(
    node: &SeqScan<'mcx>,
    ss: &ScanState<'mcx>,
) -> PgResult<CbScanInfo> {
    use ::nodes_core::NodeWalker as _;
    use ::types_nodes::NodeTag;

    let rel = ss.ss_currentRelation.as_ref().unwrap();
    let natts = rel.rd_att.natts as usize;
    let scanrelid = node.scan.scanrelid as i32;

    struct Cx {
        scanrelid: i32,
        needed: Vec<bool>,
        wholerow: bool,
        syscol: bool,
    }
    impl<'mcx> ::nodes_core::NodeWalker<'mcx> for Cx {
        fn visit(&mut self, n: ::types_nodes::Node<'mcx>) -> PgResult<bool> {
            if n.node_tag() == NodeTag::T_Var {
                let v = n.as_var().unwrap();
                if v.varno == self.scanrelid && v.varlevelsup == 0 {
                    if v.varattno == 0 {
                        self.wholerow = true;
                    } else if v.varattno < 0 {
                        self.syscol = true;
                    } else if (v.varattno as usize) <= self.needed.len() {
                        self.needed[(v.varattno - 1) as usize] = true;
                    }
                }
                return Ok(false);
            }
            ::nodes_core::expression_tree_walker(n, self)
        }
    }
    let mut cx = Cx { scanrelid, needed: vec![false; natts], wholerow: false, syscol: false };
    for n in node.scan.plan.qual.iter() {
        cx.visit(n)?;
    }
    for n in node.scan.plan.targetlist.iter() {
        cx.visit(n)?;
    }
    if cx.syscol {
        return Err(Box::new(PgError::error(
            "cbstore does not support system columns".to_string(),
        )));
    }
    if cx.wholerow {
        cx.needed.iter_mut().for_each(|b| *b = true);
    }

    let mut zone: Vec<::tableam::ZoneQual> = Vec::new();
    for n in node.scan.plan.qual.iter() {
        if let Some((attnum, op, val)) = cb_zone_conjunct(n, scanrelid) {
            zone.push(::tableam::ZoneQual { attnum, op, val });
        }
    }
    Ok(CbScanInfo { needed: cx.needed, zone })
}

// Zone-mappable scan-qual conjunct: a top-level `Var CMP Const` OpExpr of
// this relation over the int/date/timestamp cross-type compare families.
fn cb_zone_conjunct(
    n: ::types_nodes::Node<'_>,
    scanrelid: i32,
) -> Option<(u16, ::tableam::ZoneCmp, i64)> {
    use ::types_nodes::NodeTag;
    if n.node_tag() != NodeTag::T_OpExpr {
        return None;
    }
    let op = n.as_op_expr()?;
    if op.args.len() != 2 {
        return None;
    }
    let a = op.args.iter().next()?;
    let b = op.args.iter().nth(1)?;
    let (var, konst, flip) = match (a.node_tag(), b.node_tag()) {
        (NodeTag::T_Var, NodeTag::T_Const) => (a.as_var()?, b.as_const()?, false),
        (NodeTag::T_Const, NodeTag::T_Var) => (b.as_var()?, a.as_const()?, true),
        _ => return None,
    };
    if var.varno != scanrelid || var.varlevelsup != 0 || var.varattno <= 0 || konst.constisnull {
        return None;
    }
    cb_zone_from_parts(var.varattno as u16, op.opfuncid, flip, konst.constvalue)
}

// Shared zone-qual extraction (op/const-width/flip) for a `Var CMP Const`
// with the const on the `commuted` side. attnum is 1-based. Also the staged
// prewhere fold's source, so folded verdicts derive from byte-identical
// (attnum, op, val) to the pruning path.
fn cb_zone_from_parts(
    attnum: u16,
    fn_oid: u32,
    commuted: bool,
    konst: ::datum::Datum,
) -> Option<(u16, ::tableam::ZoneCmp, i64)> {
    use ::tableam::ZoneCmp as Z;
    let (cmp, cw) = cb_zone_cmp(fn_oid)?;
    let val = match cw {
        2 => konst.as_i16() as i64,
        4 => konst.as_i32() as i64,
        _ => konst.as_i64(),
    };
    let cmp = if commuted {
        match cmp {
            Z::Lt => Z::Gt,
            Z::Le => Z::Ge,
            Z::Gt => Z::Lt,
            Z::Ge => Z::Le,
            other => other,
        }
    } else {
        cmp
    };
    Some((attnum, cmp, val))
}

// (comparison, const width) by pg_proc oid; const width is the CONST side
// of the cross-type families (int2/4/8 x int2/4/8, date, timestamp,
// date-vs-timestamp).
#[rustfmt::skip]
fn cb_zone_cmp(fnoid: u32) -> Option<(::tableam::ZoneCmp, u8)> {
    use ::tableam::ZoneCmp as Z;
    Some(match fnoid {
        63 => (Z::Eq, 2), 145 => (Z::Ne, 2), 64 => (Z::Lt, 2), 148 => (Z::Le, 2),
        146 => (Z::Gt, 2), 151 => (Z::Ge, 2),
        65 => (Z::Eq, 4), 144 => (Z::Ne, 4), 66 => (Z::Lt, 4), 149 => (Z::Le, 4),
        147 => (Z::Gt, 4), 150 => (Z::Ge, 4),
        467 => (Z::Eq, 8), 468 => (Z::Ne, 8), 469 => (Z::Lt, 8), 471 => (Z::Le, 8),
        470 => (Z::Gt, 8), 472 => (Z::Ge, 8),
        158 => (Z::Eq, 4), 164 => (Z::Ne, 4), 160 => (Z::Lt, 4), 166 => (Z::Le, 4),
        162 => (Z::Gt, 4), 168 => (Z::Ge, 4),
        159 => (Z::Eq, 2), 165 => (Z::Ne, 2), 161 => (Z::Lt, 2), 167 => (Z::Le, 2),
        163 => (Z::Gt, 2), 169 => (Z::Ge, 2),
        474 => (Z::Eq, 4), 475 => (Z::Ne, 4), 476 => (Z::Lt, 4), 478 => (Z::Le, 4),
        477 => (Z::Gt, 4), 479 => (Z::Ge, 4),
        852 => (Z::Eq, 8), 853 => (Z::Ne, 8), 854 => (Z::Lt, 8), 856 => (Z::Le, 8),
        855 => (Z::Gt, 8), 857 => (Z::Ge, 8),
        1086 => (Z::Eq, 4), 1091 => (Z::Ne, 4), 1087 => (Z::Lt, 4), 1088 => (Z::Le, 4),
        1089 => (Z::Gt, 4), 1090 => (Z::Ge, 4),
        2052 => (Z::Eq, 8), 2053 => (Z::Ne, 8), 2054 => (Z::Lt, 8), 2055 => (Z::Le, 8),
        2057 => (Z::Gt, 8), 2056 => (Z::Ge, 8),
        1152 => (Z::Eq, 8), 1153 => (Z::Ne, 8), 1154 => (Z::Lt, 8), 1155 => (Z::Le, 8),
        1157 => (Z::Gt, 8), 1156 => (Z::Ge, 8),
        _ => return None,
    })
}

/// `ExecEndSeqScan`.
pub fn exec_end_seq_scan(node: &mut SeqScanState<'_>) -> PgResult<()> {
    node.bloom = None;
    node.cb_scan = None;
    stitch_trace_summary(node);
    // Releases the plan's deform-JIT kernel Rc and the stitched body's code
    // block (forget-exempt in batch.rs / here).
    node.batch_soa = None;
    if let Some(scandesc) = node.ss.ss_currentScanDesc.take() {
        table_endscan(scandesc)?;
    }
    node.parallel = None;
    Ok(())
}

/// Executor-skeleton park gate: EPQ and parallel scans never park.
pub fn skeleton_parkable(node: &SeqScanState<'_>) -> bool {
    !matches!(node.variant, SeqScanVariant::Epq) && !node.parallel_aware && node.parallel.is_none()
}

/// Executor-skeleton park: release everything per-run (scan descriptor,
/// relation pin, pushed filters, staged batches); compiled expressions and
/// slots stay armed. Pairs with `skeleton_rebind`.
pub fn skeleton_park(node: &mut SeqScanState<'_>) -> PgResult<()> {
    node.bloom = None;
    stitch_trace_summary(node);
    node.batch_soa = None;
    node.scan_batch = ScanBatchMode::Unknown;
    node.lane_pos = 0;
    node.lane_n = 0;
    node.lane_verdict = None;
    if let Some(scandesc) = node.ss.ss_currentScanDesc.take() {
        table_endscan(scandesc)?;
    }
    node.ss.ss_currentRelation = None;
    Ok(())
}

/// Executor-skeleton re-arm: re-pin the scan relation for a new execution,
/// with C ExecOpenScanRelation's per-run relispopulated probe.
pub fn skeleton_rebind<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(node.ss.ss_currentScanDesc.is_none());
    let eflags = estate.es_top_eflags;
    let rel = estate.exec_get_range_table_relation(node.ss.scanrelid, false)?;
    if eflags & (EXEC_FLAG_EXPLAIN_ONLY | EXEC_FLAG_WITH_NO_DATA) == 0
        && !rel.rd_rel.relispopulated
    {
        return Err(unpopulated_matview(rel));
    }
    node.ss.ss_currentRelation = Some(rel.alias());
    Ok(())
}

/// `ExecReScanSeqScan`.
pub fn exec_rescan_seq_scan<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    if let Some(scan) = node.ss.ss_currentScanDesc.as_mut() {
        table_rescan(mcx, scan, None)?;
    }
    node.lane_pos = 0;
    node.lane_n = 0;
    if let Some(b) = node.batch_soa.as_deref_mut() {
        b.reset_staged();
    }
    if let Some(b) = node.bloom.as_deref_mut() {
        b.reset_staged();
    }
    execscan::exec_scan_rescan(&mut node.ss, estate);
    Ok(())
}

/// `ExecSeqScanEstimate`: no DSM thread-native (docs/parallel-query-design.md).
pub fn exec_seq_scan_estimate(_node: &mut SeqScanState<'_>) {}

/// `ExecSeqScanInitializeDSM`.
pub fn exec_seq_scan_initialize_dsm<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<std::sync::Arc<ParallelTableScanDescShared>> {
    let mcx = estate.es_query_cxt;
    let mut shared = std::sync::Arc::new(ParallelTableScanDescShared::default());
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    table_parallelscan_initialize(
        rel,
        std::sync::Arc::get_mut(&mut shared).expect("freshly created shared descriptor"),
        &estate.es_snapshot,
    )?;
    debug_assert!(node.ss.ss_currentScanDesc.is_none());
    node.ss.ss_currentScanDesc = Some(table_beginscan_parallel(mcx, rel, &shared)?);
    node.apply_cb_scan_settings();
    node.arm_slot_jit_deform(estate);
    node.parallel = Some(std::sync::Arc::clone(&shared));
    Ok(shared)
}

/// `ExecSeqScanReInitializeDSM`.
pub fn exec_seq_scan_reinitialize_dsm(node: &mut SeqScanState<'_>) {
    let shared = node.parallel.as_ref().expect("parallel seqscan was initialized");
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    table_parallelscan_reinitialize(rel, &shared.pscan);
}

/// `ExecSeqScanInitializeWorker`.
pub fn exec_seq_scan_initialize_worker<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    shared: std::sync::Arc<ParallelTableScanDescShared>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    debug_assert!(node.ss.ss_currentScanDesc.is_none());
    node.ss.ss_currentScanDesc = Some(table_beginscan_parallel(mcx, rel, &shared)?);
    node.apply_cb_scan_settings();
    node.arm_slot_jit_deform(estate);
    node.parallel = Some(shared);
    Ok(())
}

mcx::forget_safe_nodrop!(SeqScanVariant);

mcx::forget_safe_nodrop!(ScanBatchMode);

// bloom/parallel exempt: released in exec_end_seq_scan / release_parallel.
mcx::forget_safe_struct!(
    SeqScanState<'_> {
        ss, variant, plan_node_id, parallel_aware, batch_soa, scan_batch, batch_allowed,
        lane_pos, lane_n, lane_verdict;
        bloom, parallel, cb_scan
    },
    // stitch/proj exempt: the stitched programs (heap Vecs + the W^X code
    // blocks) are released in exec_end_seq_scan / skeleton_park via
    // `batch_soa = None` (the deform-JIT kernel Rc precedent).
    BatchSoa<'_> {
        plan, soa, qual_armed, qual_only, key_col, varkey, key_read_col, publish, quals,
        nquals, sel, nwords, cur_word, cur_bits; stitch, proj,
    },
    BloomScan<'_> { plan, soa, col, sel, nwords, cur_word, cur_bits, seen, kept; filter },
);
