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

pub use interp::{eval_qual, eval_row};
pub use spec::{
    ArithOp, Batch, BoolTestKind, CmpOp, Lane, NullTestKind, Program, SelVec, Step, MAX_COLS,
    MAX_REGS, MAX_ROWS, SEL_WORDS,
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

const _: () = assert!(core::mem::size_of::<datum::Datum>() == 8);
const _: () = assert!(core::mem::size_of::<bool>() == 1);
const _: () = assert!(core::mem::offset_of!(JitParams, lanes) == 0);
const _: () = assert!(core::mem::size_of::<LaneParam>() == 16);

fn params_layout() -> stitch::ParamsLayout {
    stitch::ParamsLayout {
        lane_stride: core::mem::size_of::<LaneParam>() as u32,
        lane_p0: core::mem::offset_of!(LaneParam, p0) as u32,
        lane_isnull: core::mem::offset_of!(LaneParam, isnull) as u32,
        sel: core::mem::offset_of!(JitParams, sel) as u32,
        nrows: core::mem::offset_of!(JitParams, nrows) as u32,
    }
}

type PipelineFn = unsafe extern "C" fn(*mut JitParams) -> i64;

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
        Some(StitchedProgram {
            block,
            entry,
            ncols,
            used_cols: plan.used_cols.clone(),
            simd: stitch::plan_is_simd(&plan),
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
        debug_assert_eq!(sel.nrows, batch.nrows);
        debug_assert!(sel.is_all(), "run requires an all-ones sel (only failures store)");
        if self.refused.get() {
            interp::eval_qual(prog, batch, sel)?;
            return Ok(RunOutcome::InterpretedSticky);
        }
        // Per-batch fail-open: drifted staging interprets this batch.
        if batch.nrows as usize > MAX_ROWS || batch.lanes.len() < self.ncols {
            interp::eval_qual(prog, batch, sel)?;
            return Ok(RunOutcome::InterpretedDrift);
        }
        let n = batch.nrows as usize;
        for &col in &self.used_cols {
            let lane = &batch.lanes[col as usize];
            if lane.values.len() < n || lane.isnull.len() < n {
                interp::eval_qual(prog, batch, sel)?;
                return Ok(RunOutcome::InterpretedDrift);
            }
        }
        let mut lanes: [LaneParam; MAX_COLS] = core::array::from_fn(|_| LaneParam {
            p0: core::ptr::null(),
            isnull: core::ptr::null(),
        });
        for &col in &self.used_cols {
            let lane = &batch.lanes[col as usize];
            lanes[col as usize] =
                LaneParam { p0: lane.values.as_ptr().cast(), isnull: lane.isnull.as_ptr().cast() };
        }
        let mut params =
            JitParams { lanes, sel: sel.words.as_mut_ptr(), nrows: batch.nrows as u64 };
        // SAFETY: body compiled for ncols-lane batches; every used lane
        // pointer covers nrows rows (checked above); sel covers MAX_ROWS
        // bits >= nrows; the body only reads lanes and clears sel bits.
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
        *sel = SelVec::all(batch.nrows);
        interp::eval_qual(prog, batch, sel)?;
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

    pub fn entry_addr(&self) -> usize {
        self.block.base() as usize
    }
}
