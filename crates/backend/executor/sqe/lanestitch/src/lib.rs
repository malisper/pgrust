// sqe_lanestitch: Tier B of the sqe engine — the runtime stitcher that
// lowers Step-IR programs (step-ir.md §2) to fused AArch64 bodies.
// Curated port of the lanestitch lineage (R5); one IR, one semantics
// source (R2): the oracle-feature interpreter is the executable
// specification, compiled for CI parity suites only and reachable from
// no production path.
//
// # Equivalence contract
//
// For every program `compile` accepts and every batch honoring the
// canonical-datum contract (spec.rs), a body that exits `Stitched`
// produced exactly the oracle's pass bits / output cells. A body that
// exits `Refused` tripped an erroring stencil having constructed NO
// error; the DRIVER replays the batch on the error-owning path — in
// production the precompiled per-op AOT error kernels (production-plan
// §3; the kernels land under the P6-2 error corpus), in CI the oracle —
// which raises the exact error on the exact row. Refusal is STICKY per
// body: after one data-error refusal the body never runs again.
//
// # Rails (permanent by design)
//
// - Fail-CLOSED classification: an unclassified shape refuses to compile;
//   the caller surfaces a typed refusal (R1) — there is no slow path.
// - Per-batch `Drift` (short/missing lanes, oversize batch): the body
//   declines the batch without touching outputs; the caller replays it on
//   its own row path. The body stays armed.
// - No environment reads: tier pinning (SIMD off, SVE2 off/force) is the
//   caller's explicit `StitchOpts`; the engine reads no env vars.

mod cache;
mod emit;
#[cfg(feature = "oracle")]
mod interp;
mod spec;
mod stitch;

use std::cell::Cell;
use std::sync::Arc;

pub use cache::{stitch_cache, CacheStats, StitchCache, STITCH_CACHE_CAP_BYTES};
#[cfg(feature = "oracle")]
pub use interp::{eval_project, eval_qual, eval_row, eval_row_outs};
pub use spec::{
    ArithOp, Batch, BoolTestKind, CmpOp, Lane, NullTestKind, OutLane, Program, SelVec, Step,
    MAX_COLS, MAX_OUTS, MAX_REGS, MAX_ROWS, SEL_WORDS,
};

/// Architecture gate: Tier B emits AArch64 only. On other targets every
/// compile refuses and the caller's lowering surfaces a typed refusal.
pub fn available() -> bool {
    cfg!(target_arch = "aarch64")
}

/// SVE2 pin for one compile. Inert on non-SVE2 hardware.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sve2Pin {
    /// Hardware probe decides (SVE+SVE2 HWCAPs, VL within slack).
    Auto,
    /// Pin the NEON tier on SVE2 hardware (A/B arms).
    Off,
    /// Drop the survivor crossover to 0 (parity coverage of the SVE
    /// extraction path at any selectivity).
    Force,
}

/// Per-compile options. The default is the production shape; the pins
/// exist for A/B arms and parity coverage — always caller-explicit,
/// never environment-read.
#[derive(Clone, Copy, Debug)]
pub struct StitchOpts {
    pub simd: bool,
    pub sve2: Sve2Pin,
}

impl Default for StitchOpts {
    fn default() -> StitchOpts {
        StitchOpts { simd: true, sve2: Sve2Pin::Auto }
    }
}

/// True = the SVE2 stencil tier would engage for new compiles under
/// `Sve2Pin::Auto` on this host.
pub fn sve2_active() -> bool {
    matches!(stitch::simd_tier(Sve2Pin::Auto), stitch::SimdTier::Sve2 { .. })
}

// The per-batch params block the body reads. Lane binding: p0 = the Datum
// values array, isnull = the bool bytes array.
#[repr(C)]
struct LaneParam {
    p0: *const u8,
    isnull: *const u8,
}

#[repr(C)]
struct JitParams {
    lanes: [LaneParam; MAX_COLS],
    sel: *mut u64,
    nrows: u64,
}

#[repr(C)]
struct ProjJitParams {
    lanes: [LaneParam; MAX_COLS],
    outs: [LaneParam; MAX_OUTS],
    sel: *const u64,
    nrows: u64,
}

const _: () = assert!(core::mem::size_of::<datum::Datum>() == 8);
const _: () = assert!(core::mem::size_of::<bool>() == 1);
const _: () = assert!(core::mem::offset_of!(JitParams, lanes) == 0);
const _: () = assert!(core::mem::offset_of!(ProjJitParams, lanes) == 0);
#[cfg(not(target_family = "wasm"))]
const _: () = assert!(core::mem::size_of::<LaneParam>() == 16);

fn params_layout() -> stitch::ParamsLayout {
    stitch::ParamsLayout {
        lane_stride: core::mem::size_of::<LaneParam>() as u32,
        lane_p0: core::mem::offset_of!(LaneParam, p0) as u32,
        lane_isnull: core::mem::offset_of!(LaneParam, isnull) as u32,
        sel: core::mem::offset_of!(JitParams, sel) as u32,
        nrows: core::mem::offset_of!(JitParams, nrows) as u32,
        outs_base: 0, // qual bodies have no outs (StoreOut refuses)
    }
}

fn proj_params_layout() -> stitch::ParamsLayout {
    stitch::ParamsLayout {
        lane_stride: core::mem::size_of::<LaneParam>() as u32,
        lane_p0: core::mem::offset_of!(LaneParam, p0) as u32,
        lane_isnull: core::mem::offset_of!(LaneParam, isnull) as u32,
        sel: core::mem::offset_of!(ProjJitParams, sel) as u32,
        nrows: core::mem::offset_of!(ProjJitParams, nrows) as u32,
        outs_base: core::mem::offset_of!(ProjJitParams, outs) as u32,
    }
}

type PipelineFn = unsafe extern "C" fn(*mut JitParams) -> i64;
type ProjPipelineFn = unsafe extern "C" fn(*mut ProjJitParams) -> i64;

/// The code pages behind one stitched handle: either handle-owned on the
/// thread-local W^X arena (the direct `compile` path) or a refcounted
/// share of a cache-installed body ([`StitchCache`]). Shared pages are
/// immutable RX for the Arc's whole lifetime, so a handle keeps its body
/// mapped across every execution — eviction only drops the CACHE's
/// reference, never a live handle's.
enum Body {
    Local(jit_deform::CodeBlock),
    Shared(Arc<jit_deform::SharedCode>),
}

impl Body {
    #[inline]
    fn base(&self) -> *const u8 {
        match self {
            Body::Local(b) => b.base(),
            Body::Shared(c) => c.base(),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            Body::Local(b) => b.len(),
            Body::Shared(c) => c.len(),
        }
    }
}

/// Everything `compile` derives from (program, ncols, opts) besides the
/// instruction words — the cacheable classification byproducts.
#[derive(Clone)]
pub(crate) struct QualMeta {
    pub(crate) ncols: usize,
    pub(crate) used_cols: Vec<u16>,
    pub(crate) simd: bool,
    pub(crate) sve_survivors: bool,
    pub(crate) sve_match_clauses: usize,
}

/// Classify + emit one qual body: the single emission path under BOTH the
/// direct `compile` path and the cache's miss path, which is what makes
/// golden-encoding stability (cached bytes == fresh-stitch bytes) hold by
/// construction.
pub(crate) fn stitch_qual_words(
    prog: &Program,
    ncols: usize,
    opts: StitchOpts,
) -> Option<(Vec<u32>, QualMeta)> {
    let plan = stitch::plan_clauses(prog, ncols)?;
    let simd = opts.simd && stitch::classify_simd(&plan);
    let tier = stitch::simd_tier(opts.sve2);
    let words = stitch::emit_pipeline(prog, &plan, &params_layout(), simd, tier);
    let (sve_survivors, sve_match_clauses) = stitch::plan_sve2_info(prog, &plan, simd, tier);
    Some((
        words,
        QualMeta {
            ncols,
            used_cols: plan.used_cols.clone(),
            simd,
            sve_survivors,
            sve_match_clauses,
        },
    ))
}

#[derive(Clone)]
pub(crate) struct ProjMeta {
    pub(crate) ncols: usize,
    pub(crate) nouts: usize,
    pub(crate) used_cols: Vec<u16>,
}

/// Classify + emit one projection body (same single-emission-path law as
/// [`stitch_qual_words`]).
pub(crate) fn stitch_proj_words(
    prog: &Program,
    ncols: usize,
    nouts: usize,
) -> Option<(Vec<u32>, ProjMeta)> {
    let plan = stitch::plan_project(prog, ncols, nouts)?;
    let words = stitch::emit_project_pipeline(prog, &plan, &proj_params_layout());
    Some((words, ProjMeta { ncols, nouts, used_cols: plan.used_cols.clone() }))
}

/// How one qual batch was handled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QualOutcome {
    /// The stitched body consumed the batch; `sel` holds the final bits.
    Stitched,
    /// Per-batch decline (lane drift / oversize batch): `sel` untouched;
    /// the caller evaluates this batch on its own path. Body stays armed.
    Drift,
    /// An erroring stencil tripped: NO error was constructed and the sel
    /// words are partial garbage. The caller must replay the batch on the
    /// error-owning path (AOT error kernels in production; the CI oracle
    /// in tests), which raises the exact error on the exact row. Sticky:
    /// this body never runs again.
    Refused,
}

/// One stitched qual body plus its runtime rails. Owns the code block
/// (the W^X arena chunk stays alive while any body on it is).
pub struct StitchedProgram {
    block: Body,
    entry: PipelineFn,
    ncols: usize,
    used_cols: Vec<u16>,
    simd: bool,
    sve_survivors: bool,
    sve_match_clauses: usize,
    refused: Cell<bool>,
    /// Wall-clock nanos in classification + emission + install (the
    /// µs-class stitch budget the tests assert).
    pub stitch_nanos: u64,
    pub code_bytes: usize,
}

impl StitchedProgram {
    /// Stitch a qual body for `prog` over batches of `ncols` lanes with
    /// default options. None = refused (classification, arch, arena
    /// full): a typed refusal at the caller.
    pub fn compile(prog: &Program, ncols: usize) -> Option<StitchedProgram> {
        Self::compile_with(prog, ncols, StitchOpts::default())
    }

    pub fn compile_with(
        prog: &Program,
        ncols: usize,
        opts: StitchOpts,
    ) -> Option<StitchedProgram> {
        if !available() {
            return None;
        }
        let t0 = std::time::Instant::now();
        let (words, meta) = stitch_qual_words(prog, ncols, opts)?;
        let block = jit_deform::install_code(&words)?;
        // SAFETY: block holds a complete body starting at base, RX-mapped
        // and icache-flushed by install_code.
        let entry: PipelineFn = unsafe { core::mem::transmute(block.base()) };
        Some(Self::from_parts(Body::Local(block), entry, meta, t0.elapsed().as_nanos() as u64))
    }

    /// Assemble a handle around installed pages. The `refused` rail starts
    /// clear: sticky refusal is PER-HANDLE state (one statement's data
    /// error must not poison other statements sharing cached pages).
    pub(crate) fn from_parts(
        block: Body,
        entry: PipelineFn,
        meta: QualMeta,
        stitch_nanos: u64,
    ) -> StitchedProgram {
        let code_bytes = block.len();
        StitchedProgram {
            block,
            entry,
            ncols: meta.ncols,
            used_cols: meta.used_cols,
            simd: meta.simd,
            sve_survivors: meta.sve_survivors,
            sve_match_clauses: meta.sve_match_clauses,
            refused: Cell::new(false),
            stitch_nanos,
            code_bytes,
        }
    }

    /// The installed instruction bytes (golden-encoding comparisons).
    pub fn code(&self) -> &[u8] {
        // SAFETY: base..base+len is inside the body's RX mapping,
        // readable and immutable for self's lifetime.
        unsafe { core::slice::from_raw_parts(self.block.base(), self.block.len()) }
    }

    /// Evaluate the qual over one staged batch: failing rows' bits are
    /// cleared in `sel`, which MUST be all-ones for batch.nrows on entry
    /// (only failures store). See [`QualOutcome`] for the non-Stitched
    /// exits — the body itself never errors.
    pub fn run(&self, batch: &Batch<'_>, sel: &mut SelVec) -> QualOutcome {
        self.run_lanes(batch.nrows, &batch.lanes, sel)
    }

    /// [`run`](Self::run) over a bare lane-view slice (zero-allocation
    /// pipeline entry).
    pub fn run_lanes(&self, nrows: u32, lane_views: &[Lane<'_>], sel: &mut SelVec) -> QualOutcome {
        debug_assert_eq!(sel.nrows, nrows);
        let nwords = (nrows as usize).div_ceil(64);
        self.run_into(nrows, lane_views, &mut sel.words[..nwords])
    }

    /// [`run_lanes`](Self::run_lanes) writing directly into the caller's
    /// selection words. `sel_words` must span exactly `ceil(nrows/64)`
    /// words and be all-ones over `nrows` on entry (tail bits of the last
    /// word clear; only failures store).
    pub fn run_into(
        &self,
        nrows: u32,
        lane_views: &[Lane<'_>],
        sel_words: &mut [u64],
    ) -> QualOutcome {
        let nwords = (nrows as usize).div_ceil(64);
        debug_assert_eq!(sel_words.len(), nwords);
        if self.refused.get() {
            return QualOutcome::Refused;
        }
        if nrows as usize > MAX_ROWS || lane_views.len() < self.ncols {
            return QualOutcome::Drift;
        }
        let n = nrows as usize;
        for &col in &self.used_cols {
            let lane = &lane_views[col as usize];
            if lane.values.len() < n || lane.isnull.len() < n {
                return QualOutcome::Drift;
            }
        }
        let mut lanes: [LaneParam; MAX_COLS] = core::array::from_fn(|_| LaneParam {
            p0: core::ptr::null(),
            isnull: core::ptr::null(),
        });
        for &col in &self.used_cols {
            let lane = &lane_views[col as usize];
            lanes[col as usize] =
                LaneParam { p0: lane.values.as_ptr().cast(), isnull: lane.isnull.as_ptr().cast() };
        }
        let mut params = JitParams { lanes, sel: sel_words.as_mut_ptr(), nrows: nrows as u64 };
        // SAFETY: body compiled for ncols-lane batches; every used lane
        // pointer covers nrows rows (checked above); sel spans
        // ceil(nrows/64) words and the body indexes sel words by row/64
        // with row < nrows only; the body only reads lanes and clears sel
        // bits.
        let rc = unsafe { (self.entry)(&mut params) };
        if rc == stitch::RC_OK {
            return QualOutcome::Stitched;
        }
        debug_assert_eq!(rc, stitch::RC_REFUSE);
        self.refused.set(true);
        QualOutcome::Refused
    }

    /// True = the body runs the 64-row NEON block tier (the scalar loop
    /// owns the n % 64 tail).
    pub fn is_simd(&self) -> bool {
        self.simd
    }

    /// True = the body carries the adaptive SVE COMPACT survivor path.
    pub fn has_sve_survivor_path(&self) -> bool {
        self.sve_survivors
    }

    /// Number of IN-list clauses on the SVE2 MATCH stencil.
    pub fn sve_match_clauses(&self) -> usize {
        self.sve_match_clauses
    }

    pub fn entry_addr(&self) -> usize {
        self.block.base() as usize
    }
}

/// How one projection batch was handled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjOutcome {
    /// Every selected row's outputs were computed.
    Stitched,
    /// Per-batch decline (lane/out drift, oversize batch): outputs
    /// untouched; the caller projects this batch on its own row path.
    Drift,
    /// An erroring stencil tripped: NO error constructed, outputs are
    /// garbage. The caller replays per-row on the error-owning path and
    /// this body never runs again (sticky).
    Refused,
}

/// One stitched projection body plus its runtime rails. Owns the code
/// block. Contract: for every accepted program and canonical batch,
/// `run_into == eval_project` on all SELECTED rows' outputs, except that
/// an erroring program exits `Refused` where the oracle raises — the
/// driver's replay owns error identity.
pub struct StitchedProjection {
    block: Body,
    entry: ProjPipelineFn,
    ncols: usize,
    nouts: usize,
    used_cols: Vec<u16>,
    refused: Cell<bool>,
    pub stitch_nanos: u64,
    pub code_bytes: usize,
}

impl StitchedProjection {
    /// Stitch a projection body over `ncols` input lanes and `nouts`
    /// output lanes. None = refused: a typed refusal at the caller.
    pub fn compile(prog: &Program, ncols: usize, nouts: usize) -> Option<StitchedProjection> {
        if !available() {
            return None;
        }
        let t0 = std::time::Instant::now();
        let (words, meta) = stitch_proj_words(prog, ncols, nouts)?;
        let block = jit_deform::install_code(&words)?;
        // SAFETY: block holds a complete body starting at base, RX-mapped
        // and icache-flushed by install_code.
        let entry: ProjPipelineFn = unsafe { core::mem::transmute(block.base()) };
        Some(Self::from_parts(Body::Local(block), entry, meta, t0.elapsed().as_nanos() as u64))
    }

    /// Assemble a handle around installed pages; `refused` starts clear
    /// (per-handle rail — see [`StitchedProgram::from_parts`]).
    pub(crate) fn from_parts(
        block: Body,
        entry: ProjPipelineFn,
        meta: ProjMeta,
        stitch_nanos: u64,
    ) -> StitchedProjection {
        let code_bytes = block.len();
        StitchedProjection {
            block,
            entry,
            ncols: meta.ncols,
            nouts: meta.nouts,
            used_cols: meta.used_cols,
            refused: Cell::new(false),
            stitch_nanos,
            code_bytes,
        }
    }

    /// The installed instruction bytes (golden-encoding comparisons).
    pub fn code(&self) -> &[u8] {
        // SAFETY: base..base+len is inside the body's RX mapping,
        // readable and immutable for self's lifetime.
        unsafe { core::slice::from_raw_parts(self.block.base(), self.block.len()) }
    }

    /// Compute output lanes for every SELECTED row of one staged batch.
    /// `sel_words` spans exactly `ceil(nrows/64)` words (tail bits
    /// clear); rows with a set bit get all outputs written, clear rows
    /// are untouched. Outputs must each span >= nrows rows.
    pub fn run_into(
        &self,
        nrows: u32,
        lane_views: &[Lane<'_>],
        sel_words: &[u64],
        outs: &mut [OutLane<'_>],
    ) -> ProjOutcome {
        let nwords = (nrows as usize).div_ceil(64);
        debug_assert_eq!(sel_words.len(), nwords);
        if self.refused.get() {
            return ProjOutcome::Refused;
        }
        if nrows as usize > MAX_ROWS
            || lane_views.len() < self.ncols
            || outs.len() < self.nouts
        {
            return ProjOutcome::Drift;
        }
        let n = nrows as usize;
        for &col in &self.used_cols {
            let lane = &lane_views[col as usize];
            if lane.values.len() < n || lane.isnull.len() < n {
                return ProjOutcome::Drift;
            }
        }
        for out in outs[..self.nouts].iter() {
            if out.values.len() < n || out.isnull.len() < n {
                return ProjOutcome::Drift;
            }
        }
        let mut lanes: [LaneParam; MAX_COLS] = core::array::from_fn(|_| LaneParam {
            p0: core::ptr::null(),
            isnull: core::ptr::null(),
        });
        for &col in &self.used_cols {
            let lane = &lane_views[col as usize];
            lanes[col as usize] =
                LaneParam { p0: lane.values.as_ptr().cast(), isnull: lane.isnull.as_ptr().cast() };
        }
        let mut outps: [LaneParam; MAX_OUTS] = core::array::from_fn(|_| LaneParam {
            p0: core::ptr::null(),
            isnull: core::ptr::null(),
        });
        for (op, out) in outps[..self.nouts].iter_mut().zip(outs.iter_mut()) {
            *op = LaneParam {
                p0: out.values.as_mut_ptr().cast(),
                isnull: out.isnull.as_mut_ptr().cast(),
            };
        }
        let mut params = ProjJitParams {
            lanes,
            outs: outps,
            sel: sel_words.as_ptr(),
            nrows: nrows as u64,
        };
        // SAFETY: body compiled for (ncols, nouts); every used lane and
        // out pointer covers nrows rows (checked above); sel spans
        // ceil(nrows/64) words indexed by row/64 with row < nrows only;
        // the body reads lanes/sel and writes outs only.
        let rc = unsafe { (self.entry)(&mut params) };
        if rc == stitch::RC_OK {
            return ProjOutcome::Stitched;
        }
        debug_assert_eq!(rc, stitch::RC_REFUSE);
        self.refused.set(true);
        ProjOutcome::Refused
    }

    pub fn entry_addr(&self) -> usize {
        self.block.base() as usize
    }
}
