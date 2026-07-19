// lanestitch: the copy-and-patch stencil JIT stitcher for lane-executor-v2
// (Phase 3 of docs/design/lane-executor-v2.md; codegen structure fixed by
// docs/research/jit-compiler-structure.md — stencils only, NO IR / LLVM /
// Cranelift; grow along the clause-fusion + NEON-width axes; the
// interpreter stays the permanent parity oracle and fail-open floor).
//
// Built standalone-and-parity-proven now, wired into the pipeline later —
// the lanefold pattern. Machinery lineage: the batchexec POC stitcher
// (poc/batchexec/src/jit/), the production execexpr jitq Emitter, and the
// production jit_deform W^X arena (a real dependency here, not a copy: the
// arena reuse surface `jit_deform::install_code` exists for exactly this).
//
// # Equivalence contract
//
// For every program that `StitchedProgram::compile` accepts, and every
// batch honoring the canonical-datum contract (spec.rs):
//
//   run(prog, batch, sel)  ==  interp::eval_qual(prog, batch, sel)
//
// — same surviving sel bits, same error (message + sqlstate), same erroring
// row, with rows before the erroring row fully consumed. `interp` IS the
// specification; the parity fuzzer in tests/parity.rs is the evidence
// standard (Miri cannot run generated code).
//
// # Rails (permanent by design, not scaffolding)
//
// - Fail-CLOSED classification: `plan_clauses` is exhaustive over the step
//   and comparator vocabulary with no wildcard admission — an unclassified
//   shape refuses to compile and the caller stays on the interpreter.
// - Fail-OPEN runtime: arena exhaustion, non-aarch64, the kill switch
//   (PGRUST_LANESTITCH=0|off), oversize batches, and per-batch lane drift
//   all land on the interpreter tier for that batch.
// - Refuse-and-replay (the design-doc §3a / emit_inline_strict2 discipline
//   for erroring ops): an int-arith trap (overflow / zero divisor) makes
//   the body exit with RC_REFUSE having constructed NO error; the driver
//   replays the batch on the interpreter, which raises C's exact error on
//   C's row. Stitched code never fabricates an error object.
// - STICKY refusal per program: after a runtime replay the body never runs
//   again for this StitchedProgram — every later batch interprets.
//
// # Phase-3 wiring point (documented, deliberately NOT implemented here)
//
// The stitcher compiles the qual half of ONE pipeline segment:
// deform -> filter -> probe/fold (design doc §1). The wiring plan:
//
// 1. The lane-v2 scan pipeline's segment compiler translates its admitted
//    scan-qual prefix (the `lane_scan_qual` whitelist output) into a
//    `Program` over the staged SoA lane indices, calls
//    `StitchedProgram::compile` once per (plan node, lane signature), and
//    keeps the interpreter plan as the mandatory oracle/floor. Admission
//    consults the §3a batch-function registry: all-ops-batchable -> whole
//    program stitched; else stitched batchable prefix + per-row residual
//    (the requal-tail split generalized).
// 2. One-deform-two-consumers: the segment stages each page batch once
//    (jit_deform SoA kernels), runs the stitched body to produce the qual
//    bitmap (`SelVec`), then feeds the SAME staged lanes plus the bitmap to
//    the fold consumer (`lanefold::fold_rows_grouped`) — the bitmap is the
//    only coupling currency between the two consumers, so the stitcher
//    needs no knowledge of aggregation (and vice versa).
// 3. Probe/fold tails move INTO the stitched body only after the breaker
//    seam stabilizes (the POC proved the shapes: flat agg state + probe
//    helper calls); they extend `Plan`/`emit_pipeline`, not the API.
// 4. The row-count floor + sticky per-program row counter (`lane_jit_floor`
//    lineage) live in the caller: stitch eagerly above the floor, never on
//    OLTP-sized scans. `stitch_nanos`/`code_bytes` feed the admission
//    economics telemetry.

mod emit;
mod interp;
mod spec;
mod stitch;

use std::cell::Cell;

use types_error::PgResult;

pub use interp::{eval_project, eval_qual, eval_row, eval_row_chain};
// RB-R1 (SE18): the compiled stitched row-chain body (`StitchedRowChain`,
// rowchain.rs) is DELETED — the trigger-INSERT chain never earned a default
// (wave-7 letter +0.47%/+0.54% NO-WIN, wave-9 AG re-read NO-WIN; Michael
// ratified probe verdict (b) FLOOR). The RowChainHost vocabulary and the
// interpreter twin (`eval_row_chain`) STAY: library machinery — the semantic
// specification any future chain family hosts on.
pub use spec::{
    ArithOp, Batch, BoolTestKind, ChainCursor, ChainOutcome, ChainVerdict, CmpOp, Lane,
    NullTestKind, OutLane, Program, RowChainHost, SelVec, Step, MAX_COLS, MAX_OUTS, MAX_REGS,
    MAX_ROWS, SEL_WORDS,
};

/// AIO-style availability gate + kill switch (PGRUST_LANESTITCH=0|off).
pub fn available() -> bool {
    #[cfg(target_arch = "aarch64")]
    {
        static OFF: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        !*OFF.get_or_init(|| {
            matches!(std::env::var("PGRUST_LANESTITCH").as_deref(), Ok("0") | Ok("off"))
        })
    }
    #[cfg(not(target_arch = "aarch64"))]
    false
}

/// True = the SVE2 stencil tier is active for new compiles: SVE + SVE2 in
/// HWCAP (boot-time getauxval probe), the vector length within the emitted
/// bodies' slack bound, and not pinned off (PGRUST_LANESTITCH_SVE2=0|off).
/// Apple Silicon and non-SVE Graviton read false and keep the NEON tier.
pub fn sve2_active() -> bool {
    matches!(stitch::simd_tier(), stitch::SimdTier::Sve2 { .. })
}

// RB-R1 (SE18): the wave-9 rung-0 arena-fault lever and the
// `rowchain_available()` gate (knob `PGRUST_LANESTITCH_ROWCHAIN`, default
// OFF at every tip since wave-7) deleted with the stitched chain body —
// the compile-once path they served no longer exists. The env spelling is
// inert everywhere it still appears in harness scripts.

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

// Projection params: input lanes + output lanes (mutable) + the qual
// segment's selection words (read-only here — only set bits project).
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
// 64-bit layout pin (two pointers); wasm32 shrinks it. The stitcher JIT is
// aarch64-only anyway — params_layout() derives strides per target.
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

/// How one batch was actually evaluated (telemetry / test introspection).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunOutcome {
    /// The stitched body consumed the batch.
    Stitched,
    /// Per-batch fail-open: lane drift (short arrays / missing lanes) or an
    /// oversize batch — this batch interpreted; the body stays armed.
    InterpretedDrift,
    /// Sticky refusal: a previous batch replayed; every batch interprets.
    InterpretedSticky,
}

/// One stitched qual body plus its runtime rails. Owns the code block (the
/// W^X arena chunk stays alive while any body on it is).
pub struct StitchedProgram {
    block: jit_deform::CodeBlock,
    entry: PipelineFn,
    ncols: usize,
    used_cols: Vec<u16>,
    simd: bool,
    sve_survivors: bool,
    sve_match_clauses: usize,
    refused: Cell<bool>,
    /// Wall-clock nanos spent in classification + emission + install
    /// (the µs-class stitch budget the tests assert).
    pub stitch_nanos: u64,
    pub code_bytes: usize,
}

impl StitchedProgram {
    /// Stitch a body for `prog` over batches of `ncols` lanes. None =
    /// refused (classification, arch, kill switch, arena full): the caller
    /// stays on the interpreter tier.
    pub fn compile(prog: &Program, ncols: usize) -> Option<StitchedProgram> {
        if !available() {
            return None;
        }
        let t0 = std::time::Instant::now();
        let plan = stitch::plan_clauses(prog, ncols)?;
        let words = stitch::emit_pipeline(prog, &plan, &params_layout());
        let block = jit_deform::install_code(&words)?;
        // SAFETY: block holds a complete body starting at base, RX-mapped
        // and icache-flushed by install_code.
        let entry: PipelineFn = unsafe { core::mem::transmute(block.base()) };
        let (sve_survivors, sve_match_clauses) = stitch::plan_sve2_info(prog, &plan);
        Some(StitchedProgram {
            block,
            entry,
            ncols,
            used_cols: plan.used_cols.clone(),
            simd: stitch::plan_is_simd(&plan),
            sve_survivors,
            sve_match_clauses,
            refused: Cell::new(false),
            stitch_nanos: t0.elapsed().as_nanos() as u64,
            code_bytes: words.len() * 4,
        })
    }

    /// Evaluate the qual over one staged batch: failing rows' bits are
    /// cleared in `sel`, which MUST be all-ones for batch.nrows on entry
    /// (only failures store). Equivalence contract: identical to
    /// `interp::eval_qual` in bits, error identity, and erroring row.
    ///
    /// `prog` must be the same program this body was compiled from (it is
    /// the replay/fallback source; consts are baked, so a divergent program
    /// would silently diverge — debug builds cannot check identity cheaply,
    /// callers keep them paired the way JitPipeline callers did).
    pub fn run(
        &self,
        prog: &Program,
        batch: &Batch<'_>,
        sel: &mut SelVec,
    ) -> PgResult<RunOutcome> {
        self.run_lanes(prog, batch.nrows, &batch.lanes, sel)
    }

    /// [`run`](Self::run) over a bare lane-view slice — the zero-allocation
    /// pipeline entry (the caller keeps its views in a stack array instead
    /// of building a `Batch`'s `Vec` per staged page).
    pub fn run_lanes(
        &self,
        prog: &Program,
        nrows: u32,
        lane_views: &[Lane<'_>],
        sel: &mut SelVec,
    ) -> PgResult<RunOutcome> {
        debug_assert_eq!(sel.nrows, nrows);
        let nwords = (nrows as usize).div_ceil(64);
        self.run_into(prog, nrows, lane_views, &mut sel.words[..nwords])
    }

    /// [`run_lanes`](Self::run_lanes) writing directly into the caller's
    /// selection words (the pipeline's own bitmap — no `SelVec` staging or
    /// copy-out on the per-batch path). `sel_words` must span exactly
    /// `ceil(nrows/64)` words and be all-ones over `nrows` on entry (tail
    /// bits of the last word clear; only failures store) — both the body
    /// and the interpreter tiers touch no word beyond that span (the scalar
    /// loop and the 64-row SIMD block pass index words by row/64 only).
    pub fn run_into(
        &self,
        prog: &Program,
        nrows: u32,
        lane_views: &[Lane<'_>],
        sel_words: &mut [u64],
    ) -> PgResult<RunOutcome> {
        let nwords = (nrows as usize).div_ceil(64);
        debug_assert_eq!(sel_words.len(), nwords);
        // Interpreter tiers (cold: sticky refuse / per-batch drift /
        // refuse-and-replay): materialize the Batch + SelVec currency.
        let interp_into = |sel_words: &mut [u64]| -> PgResult<()> {
            let batch = Batch { nrows, lanes: lane_views.to_vec() };
            let mut sv = SelVec::all(nrows);
            interp::eval_qual(prog, &batch, &mut sv)?;
            sel_words.copy_from_slice(&sv.words[..nwords]);
            Ok(())
        };
        if self.refused.get() {
            interp_into(sel_words)?;
            return Ok(RunOutcome::InterpretedSticky);
        }
        // Per-batch fail-open: drifted staging interprets this batch.
        if nrows as usize > MAX_ROWS || lane_views.len() < self.ncols {
            interp_into(sel_words)?;
            return Ok(RunOutcome::InterpretedDrift);
        }
        let n = nrows as usize;
        for &col in &self.used_cols {
            let lane = &lane_views[col as usize];
            if lane.values.len() < n || lane.isnull.len() < n {
                interp_into(sel_words)?;
                return Ok(RunOutcome::InterpretedDrift);
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
        let mut params =
            JitParams { lanes, sel: sel_words.as_mut_ptr(), nrows: nrows as u64 };
        // SAFETY: body compiled for ncols-lane batches; every used lane
        // pointer covers nrows rows (checked above); sel spans
        // ceil(nrows/64) words and the body indexes sel words by row/64
        // with row < nrows only; the body only reads lanes and clears sel
        // bits.
        let rc = unsafe { (self.entry)(&mut params) };
        if rc == stitch::RC_OK {
            return Ok(RunOutcome::Stitched);
        }
        debug_assert_eq!(rc, stitch::RC_REFUSE);
        // Refuse-and-replay: the body tripped an erroring stencil (int
        // overflow / zero divisor) and constructed no error. Replay the
        // whole batch on the interpreter from a fresh all-ones sel — pure
        // deterministic quals recompute the identical prefix bits, and the
        // interpreter raises C's exact error on C's row. Sticky: this
        // program's data errors; stop stitching it.
        self.refused.set(true);
        interp_into(sel_words)?;
        // Defensive completeness: if the replay did NOT error (can only
        // happen if a trap condition raced with... nothing — lanes are
        // immutable for the call; kept because fail-open must never turn
        // into wrong-answer), the interpreter's bits are the answer.
        Ok(RunOutcome::InterpretedSticky)
    }

    /// True = the body runs the 64-row NEON block tier (scalar loop owns
    /// the n % 64 tail). Test/telemetry introspection.
    pub fn is_simd(&self) -> bool {
        self.simd
    }

    /// True = the body carries the adaptive SVE COMPACT survivor-extraction
    /// path (SVE2 tier + a SIMD body with non-vector clauses). Whether a
    /// given block takes it is decided at runtime by the measured survivor
    /// crossover. Test/telemetry introspection.
    pub fn has_sve_survivor_path(&self) -> bool {
        self.sve_survivors
    }

    /// Number of IN-list clauses this body runs on the SVE2 MATCH stencil.
    pub fn sve_match_clauses(&self) -> usize {
        self.sve_match_clauses
    }

    pub fn entry_addr(&self) -> usize {
        self.block.base() as usize
    }
}

/// How one projection batch was handled (telemetry / driver dispatch).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjOutcome {
    /// The stitched body computed every selected row's outputs.
    Stitched,
    /// Per-batch fail-open: lane/out drift (short arrays) or an oversize
    /// batch. Outputs untouched; the caller projects this batch through its
    /// per-row path. The body stays armed.
    Drift,
    /// An erroring stencil tripped (int overflow / zero divisor) — the body
    /// constructed NO error and the outputs are garbage. The caller must
    /// replay the batch per-row through the C-ported projection path (which
    /// raises C's exact error on C's row) and stop running this body
    /// (sticky, enforced here like the qual body's refusal).
    Refused,
}

/// One stitched projection body plus its runtime rails. Owns the code block.
///
/// Contract (parity-fuzzed in tests/parity.rs): for every accepted program
/// and every batch honoring the canonical-datum contract,
/// `run == interp::eval_project` on all SELECTED rows' outputs, except that
/// an erroring program exits `Refused` with no error constructed where the
/// interpreter raises — the caller's per-row replay owns error identity.
pub struct StitchedProjection {
    block: jit_deform::CodeBlock,
    entry: ProjPipelineFn,
    ncols: usize,
    nouts: usize,
    used_cols: Vec<u16>,
    refused: Cell<bool>,
    pub stitch_nanos: u64,
    pub code_bytes: usize,
}

impl StitchedProjection {
    /// Stitch a projection body for `prog` over `ncols` input lanes and
    /// `nouts` output lanes. None = refused (classification, arch, kill
    /// switch, arena full): the caller stays on its per-row path.
    pub fn compile(prog: &Program, ncols: usize, nouts: usize) -> Option<StitchedProjection> {
        if !available() {
            return None;
        }
        let t0 = std::time::Instant::now();
        let plan = stitch::plan_project(prog, ncols, nouts)?;
        let words = stitch::emit_project_pipeline(prog, &plan, &proj_params_layout());
        let block = jit_deform::install_code(&words)?;
        // SAFETY: block holds a complete body starting at base, RX-mapped
        // and icache-flushed by install_code.
        let entry: ProjPipelineFn = unsafe { core::mem::transmute(block.base()) };
        Some(StitchedProjection {
            block,
            entry,
            ncols,
            nouts,
            used_cols: plan.used_cols.clone(),
            refused: Cell::new(false),
            stitch_nanos: t0.elapsed().as_nanos() as u64,
            code_bytes: words.len() * 4,
        })
    }

    /// Compute output lanes for every SELECTED row of one staged batch.
    /// `sel_words` spans exactly `ceil(nrows/64)` words (tail bits of the
    /// last word clear); rows with a set bit get all their outputs written,
    /// clear rows are untouched. Outputs must each span >= nrows rows.
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
        // Per-batch fail-open: drifted staging falls back per-row.
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
        // SAFETY: body compiled for (ncols, nouts); every used lane pointer
        // and every out pointer covers nrows rows (checked above); sel spans
        // ceil(nrows/64) words and the body indexes them by row/64 with
        // row < nrows only; the body reads lanes/sel and writes outs only.
        let rc = unsafe { (self.entry)(&mut params) };
        if rc == stitch::RC_OK {
            return ProjOutcome::Stitched;
        }
        debug_assert_eq!(rc, stitch::RC_REFUSE);
        // Refuse-and-replay, sticky: this program's data errors — the caller
        // replays per-row (C error identity) and this body never runs again.
        self.refused.set(true);
        ProjOutcome::Refused
    }

    pub fn entry_addr(&self) -> usize {
        self.block.base() as usize
    }
}
