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
    qual_col: u16,
    qual_cmp: ::execexpr::CmpOp,
    qual_konst: ::datum::Datum,
    sel: [u64; ::exectuples::SOA_BM_WORDS],
    nwords: u32,
    cur_word: u32,
    cur_bits: u64,
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
        self.arm_slot_jit_deform(estate);
        Ok(())
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
pub fn seq_scan_batch_soa_prepare<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    prefix: i32,
    qual_only: bool,
    force: bool,
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
    let qual = match node.ss.qual.as_deref().map(|q| q.kernel()) {
        Some(::execexpr::Kernel::QualScanVarCmpConst { attnum, konst, cmp })
            if (attnum as i32) < prefix =>
        {
            Some((attnum, cmp, konst))
        }
        _ => None,
    };
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
                qual_col: qual.map_or(0, |(a, _, _)| a),
                qual_cmp: qual.map_or(::execexpr::CmpOp::Int4Eq, |(_, c, _)| c),
                qual_konst: qual.map_or(::datum::Datum::null(), |(_, _, k)| k),
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
            qual_col: 0,
            qual_cmp: ::execexpr::CmpOp::Int4Eq,
            qual_konst: ::datum::Datum::null(),
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

/// Staged SoA batch when the full-prefix deform is armed (columnar readers).
#[inline]
pub fn seq_scan_batch_soa<'a, 'mcx>(
    node: &'a SeqScanState<'mcx>,
) -> Option<&'a ::exectuples::SoaBatch<'mcx>> {
    let b = node.batch_soa.as_deref()?;
    (!b.qual_only).then_some(&b.soa)
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
            let qual_col_only =
                (b.qual_only && b.qual_armed).then_some(b.qual_col).or(b.key_col);
            ::tableam::table_scan_batch_deform(scandesc, &b.plan, &mut b.soa, qual_col_only);
            if b.qual_armed {
                ::execexpr::qual_bitmap_cmp_const(
                    b.qual_cmp,
                    b.qual_konst,
                    b.soa.col_values(b.qual_col as usize),
                    b.soa.col_isnull(b.qual_col as usize),
                    &mut b.sel,
                );
                let nwords = (n as usize).div_ceil(64);
                // Skipped rows carry a forced bit; the fetch re-checks them.
                for (w, fb) in b.sel[..nwords].iter_mut().zip(b.soa.fallback_words()) {
                    *w |= fb;
                }
                b.nwords = nwords as u32;
                b.cur_word = 0;
                b.cur_bits = b.sel[0];
            }
        }
    }
    Ok(n)
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
    match node.ss.qual.as_deref_mut() {
        None => Ok(true),
        Some(q) => {
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
#[inline(always)]
pub fn seq_scan_batch_emit<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    i: u32,
) -> PgResult<Option<ExecSlotId>> {
    estate.ecxt_mut(node.ss.ps_ExprContext).reset();
    if !seq_scan_batch_fetch(node, estate, i)? {
        return Ok(None);
    }
    let scan_id = node.ss.ss_ScanTupleSlot;
    estate.ecxt_mut(node.ss.ps_ExprContext).ecxt_scantuple = Some(scan_id);
    let Some(proj) = node.ss.ps_ProjInfo.as_mut() else {
        return Ok(Some(scan_id));
    };
    let mcx = estate.es_query_cxt;
    let result_id = proj.pi_result_slot;
    let (scan_slot, result_slot) = ::execscan::slot_pair(estate, scan_id, result_id);
    let mut slots = ::execexpr::EvalSlots { scan: Some(scan_slot), inner: None, outer: None };
    ::execexpr::exec_project(&mut proj.pi_state, &mut slots, result_slot, mcx)?;
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
    seq_scan_batch_soa_prepare(node, estate, attnum as i32 + 1, true, false);
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
    })
}

/// `ExecEndSeqScan`.
pub fn exec_end_seq_scan(node: &mut SeqScanState<'_>) -> PgResult<()> {
    node.bloom = None;
    // Releases the plan's deform-JIT kernel Rc (forget-exempt in batch.rs).
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
    node.batch_soa = None;
    node.scan_batch = ScanBatchMode::Unknown;
    node.lane_pos = 0;
    node.lane_n = 0;
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
        lane_pos, lane_n;
        bloom, parallel
    },
    BatchSoa<'_> {
        plan, soa, qual_armed, qual_only, key_col, varkey, key_read_col, publish, qual_col,
        qual_cmp, qual_konst, sel, nwords, cur_word, cur_bits,
    },
    BloomScan<'_> { plan, soa, col, sel, nwords, cur_word, cur_bits, seen, kept; filter },
);
