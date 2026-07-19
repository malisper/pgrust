// The RowOp row-chain stitched tier (WS-AA wave-7, fusion inc-0 + inc-1a;
// wave-9 WS-AG rung 3 = the D1b indirection kill; docs/design/
// rowmode-endgame.md §2). One chain drive = one aarch64 body whose loop
// body is still strictly per-row: `NextRow` and `ProtocolCall` steps
// compile to `blr` calls through Rust trampolines into the chain host (the
// node's own helpers — BR trigger fire, heap+index write, AR epilogue).
// Fusion removes interpretation/dispatch overhead, never ordering: every
// protocol step executes at exactly its Volcano position.
//
// D1b (wave-9 rung 3) killed the indirection tax on that boundary:
// * The trampolines are MONOMORPHIZED per concrete host type (`run` is
//   generic over `H: RowChainHost + ?Sized`): `tramp_protocol::<H>` calls
//   `H::protocol_call` statically — the `dyn` vtable hop is gone, and for
//   the DML chain the concrete `MtInsertChainHost` dispatch inlines into
//   the trampoline body. `run::<dyn RowChainHost>` still compiles for
//   callers that want the erased form (it monomorphizes to the old dyn
//   behavior — the parity suite pins both worlds identical).
// * ctx and both trampoline addresses load ONCE in the body prologue into
//   callee-saved x20-x22; call sites are `mov x0, x20; blr x21/x22` — the
//   three per-call params-block reloads are gone.
// * Verdict dispatch is tightened: the hot verdict (Continue / row-staged)
//   is one not-taken compare-branch per call; Skip/Pause/error/misuse
//   dispatch lives in shared out-of-line cold blocks.
//
// # Equivalence contract
//
// For every program `plan_rowchain` accepts and every host:
//
//   StitchedRowChain::run(host) == interp::eval_row_chain(prog, host)
//
// — same host-call sequence (call order == row order, the parity fuzzer's
// recorder standard), same outcome, same error (the host's own PgError,
// parked by the trampoline and re-raised by `run`; protocol targets are
// the node's own Rust helpers, so error identity is by construction — the
// two-regime error law's effectful half needs NO replay).
//
// # Rails (permanent by design)
//
// - Fail-CLOSED classification: v1 stitches ONLY {exactly one NextRow +
//   ProtocolCall steps}. Pure steps (junk-filter / projection segments)
//   refuse to compile and the chain stays on its portable host — they
//   graduate into this body with their scalar stencils in a later
//   increment (recorded in notes/se-wave7-fusion.md).
// - Fail-OPEN runtime: arena exhaustion, non-aarch64, the master kill
//   switch (PGRUST_LANESTITCH=0|off), and the per-family kill knob
//   (PGRUST_LANESTITCH_ROWCHAIN, default OFF) all land compile() on None;
//   the caller drives its portable host (for the DML chain: DmlInsertOp
//   under pull_step_rows — the two-stage x86 law's floor).
// - Host panics abort: the trampolines are extern "C" (Rust aborts an
//   unwind crossing them), so a panic inside a protocol target can never
//   unwind through the stitched frame. PgError is the error currency.

use core::ffi::c_void;

use types_error::{PgError, PgResult};

use crate::emit::{Cond, Emitter};
use crate::interp::{chain_next_pos, rowop_misuse, LOOP_TOP_MISUSE};
use crate::spec::{ChainOutcome, ChainVerdict, Program, RowChainHost, Step};

/// Body exit codes (x0).
const RC_CHAIN_DONE: i64 = 0;
const RC_CHAIN_PAUSED: i64 = 1;
/// A loop-top protocol call answered a row verdict (SkipRow/EmitPause with
/// no current row): the twin's loud loop-top-law error, raised by `run` —
/// never a silent re-loop or a bogus pause (the stitched body must diverge
/// from `eval_row_chain` on NO host, conforming or not).
const RC_CHAIN_LOOPTOP_MISUSE: i64 = 2;

// Trampoline verdict codes (host -> body). next_row: 1 = row staged,
// 0 = exhausted, negative = error parked. protocol: 0 = Continue,
// 1 = SkipRow, 2 = EmitPause, negative = error parked.
const TR_NEXT_ROW: i64 = 1;
const TR_NEXT_DONE: i64 = 0;
const TR_CONT: i64 = 0;
const TR_SKIP: i64 = 1;
const TR_PAUSE: i64 = 2;
const TR_ERR: i64 = -1;

/// The per-drive params block. Wave-9 rung 3 (D1b): the body reads it ONCE
/// in the prologue — ctx into x20, the two trampoline addresses into
/// x21/x22 (callee-saved) — instead of reloading through x19 at every call
/// site.
#[repr(C)]
struct RowChainJitParams {
    ctx: *mut c_void,
    next_row: unsafe extern "C" fn(*mut c_void) -> i64,
    protocol: unsafe extern "C" fn(*mut c_void, u64) -> i64,
}

const _: () = assert!(core::mem::offset_of!(RowChainJitParams, ctx) == 0);
const _: () = assert!(core::mem::offset_of!(RowChainJitParams, next_row) == 8);
const _: () = assert!(core::mem::offset_of!(RowChainJitParams, protocol) == 16);

type RowChainFn = unsafe extern "C" fn(*mut RowChainJitParams) -> i64;

/// Trampoline context: the host plus the parked error (the body constructs
/// no error object — a negative trampoline verdict routes `run` to the
/// parked PgError, which is the host's own unwind, byte-identical by
/// construction). Wave-9 rung 3 (D1b): generic over the CONCRETE host type
/// — the trampolines monomorphize per host and dispatch statically (no
/// `dyn` vtable hop; the DML chain's `MtInsertChainHost` dispatch inlines
/// into its trampoline instantiation).
struct TrampCtx<'a, H: RowChainHost + ?Sized> {
    host: &'a mut H,
    err: Option<Box<PgError>>,
}

unsafe extern "C" fn tramp_next_row<H: RowChainHost + ?Sized>(ctx: *mut c_void) -> i64 {
    // SAFETY: ctx is the TrampCtx<H> `run::<H>` passed in params, live for
    // the call (the monomorphized trampoline is only ever paired with its
    // own H's ctx — `run` constructs both from the same H).
    let c = unsafe { &mut *(ctx as *mut TrampCtx<'_, H>) };
    match c.host.next_row() {
        Ok(true) => TR_NEXT_ROW,
        Ok(false) => TR_NEXT_DONE,
        Err(e) => {
            c.err = Some(e);
            TR_ERR
        }
    }
}

unsafe extern "C" fn tramp_protocol<H: RowChainHost + ?Sized>(ctx: *mut c_void, call: u64) -> i64 {
    // SAFETY: ctx is the TrampCtx<H> `run::<H>` passed in params, live for
    // the call (same pairing argument as tramp_next_row).
    let c = unsafe { &mut *(ctx as *mut TrampCtx<'_, H>) };
    match c.host.protocol_call(call as u16) {
        Ok(ChainVerdict::Continue) => TR_CONT,
        Ok(ChainVerdict::SkipRow) => TR_SKIP,
        Ok(ChainVerdict::EmitPause) => TR_PAUSE,
        Err(e) => {
            c.err = Some(e);
            TR_ERR
        }
    }
}

/// Fail-closed classification of the v1 stitched chain vocabulary: exactly
/// one `NextRow` (with the protocol-only loop top `chain_next_pos` already
/// demands), every step a RowOp, no consts/arrays, not volatile-pinned.
/// Anything else refuses — the chain stays on its portable host. Refusals
/// are NAMED (wave-9 WS-AG rung 1, M5-1 funnel discipline): the reason
/// string is pinnable by tests and traceable by callers.
fn plan_rowchain(prog: &Program) -> Result<(), &'static str> {
    if prog.volatile {
        return Err("rowchain-volatile");
    }
    if prog.steps.is_empty() {
        return Err("rowchain-empty");
    }
    if !prog.consts.is_empty() || !prog.arrays.is_empty() {
        return Err("rowchain-consts");
    }
    if chain_next_pos(prog).is_none() {
        return Err("rowchain-shape");
    }
    if !prog
        .steps
        .iter()
        .all(|s| matches!(s, Step::NextRow | Step::ProtocolCall { .. }))
    {
        // Pure steps (junk-filter / projection segments) are twin-only until
        // their scalar stencils land (rung 4). The D2 cursor/lane bound is
        // in the twin (interp.rs, wave-9 rung 1), but the stitched body has
        // neither the bound nor the pure stencils — fail closed with the
        // contract-named reason.
        return Err("rowchain-pure-step-unbounded");
    }
    Ok(())
}

/// The named classification refusal for `prog`, or None = the v1 stitched
/// vocabulary admits it. Doc-hidden test/trace surface (the parity suite
/// pins the wave-9-contract-named `rowchain-pure-step-unbounded` reason).
#[doc(hidden)]
pub fn rowchain_plan_refusal(prog: &Program) -> Option<&'static str> {
    plan_rowchain(prog).err()
}

/// Emits the chain body: the row loop INSIDE one aarch64 body, steps in
/// program order, `blr` per RowOp step, verdict dispatch on x0.
///
/// Wave-9 rung 3 (D1b) register discipline: the prologue loads ctx into
/// x20 and the next_row/protocol trampoline addresses into x21/x22 (all
/// callee-saved, so they survive the `blr` calls) ONCE per drive; every
/// call site is then `mov x0, x20` (+ `movz x1, call`) + `blr`. Verdict
/// dispatch is hot-path-tightened: Continue / row-staged falls through on
/// ONE not-taken branch; the Skip/Pause/Done/error/misuse dispatch lives
/// in three SHARED out-of-line cold blocks (every per-row call skips to
/// the same targets, so one block serves all sites of its segment kind).
fn emit_rowchain(prog: &Program) -> Vec<u32> {
    // plan_rowchain validated the shape, so the NextRow position exists;
    // steps before it are the loop-top segment (ProtocolCall-only).
    let next_pos = chain_next_pos(prog).expect("plan_rowchain validated the chain shape");
    let mut e = Emitter::new();
    e.raw(0xA9BD_7BFD); // stp x29, x30, [sp, #-0x30]!
    e.raw(0x9100_03FD); // mov x29, sp
    e.str_x(20, 31, 0x10); // str x20, [sp, #0x10]
    e.str_x(21, 31, 0x18); // str x21, [sp, #0x18]
    e.str_x(22, 31, 0x20); // str x22, [sp, #0x20]
    // The D1b once-per-drive loads (x0 = params on entry).
    e.ldr_x(20, 0, 0); // x20 = ctx
    e.ldr_x(21, 0, 8); // x21 = next_row trampoline
    e.ldr_x(22, 0, 16); // x22 = protocol trampoline

    let ltop = e.new_label();
    let ldone = e.new_label();
    let lpaused = e.new_label();
    let lmisuse = e.new_label();
    let lerr = e.new_label();
    let lout = e.new_label();
    // Shared cold dispatch blocks (one per segment kind).
    let lnr_slow = e.new_label(); // NextRow: not-staged (done / error)
    let ltop_slow = e.new_label(); // loop-top call: nonzero (misuse / error)
    let lrow_slow = e.new_label(); // per-row call: nonzero (skip/pause/error)

    e.bind(ltop);
    for (i, step) in prog.steps.iter().enumerate() {
        match *step {
            Step::NextRow => {
                e.mov_x(0, 20); // ctx
                e.raw(0xD63F_02A0); // blr x21
                // Hot verdict TR_NEXT_ROW (1) falls through on one
                // not-taken branch; 0/negative dispatch is out of line.
                e.cmp_x_imm(0, 1);
                e.b_cond(Cond::Lt, lnr_slow);
            }
            Step::ProtocolCall { call } => {
                e.mov_x(0, 20); // ctx
                e.movz_x(1, call as u32);
                e.raw(0xD63F_02C0); // blr x22
                // Hot verdict TR_CONT (0) falls through on one not-taken
                // branch; everything else dispatches out of line.
                if i < next_pos {
                    // Loop-top segment: the loop-top law says Continue is
                    // the ONLY legal row verdict here (no current row). Any
                    // nonzero verdict exits loud — the twin's error, not an
                    // infinite SkipRow re-loop or a phantom pause.
                    e.cbnz_x(0, ltop_slow);
                } else {
                    e.cbnz_x(0, lrow_slow);
                }
            }
            _ => unreachable!("plan_rowchain admits RowOp steps only"),
        }
    }
    e.b(ltop); // row fully processed: loop

    // Cold: NextRow answered 0 (exhausted) or negative (error parked).
    e.bind(lnr_slow);
    e.cbz_x(0, ldone);
    e.b(lerr);
    // Cold: loop-top call answered nonzero — negative = error parked, any
    // row verdict = the loop-top-law violation.
    e.bind(ltop_slow);
    e.cmp_x_imm(0, 0);
    e.b_cond(Cond::Lt, lerr);
    e.b(lmisuse);
    // Cold: per-row call answered nonzero — negative = error parked,
    // TR_SKIP = back to the loop top, else (TR_PAUSE, the only remaining
    // trampoline verdict) = pause.
    e.bind(lrow_slow);
    e.cmp_x_imm(0, 0);
    e.b_cond(Cond::Lt, lerr);
    e.cmp_x_imm(0, TR_SKIP as u32);
    e.b_cond(Cond::Eq, ltop);
    e.b(lpaused);

    e.bind(ldone);
    e.movz_x(0, RC_CHAIN_DONE as u32);
    e.b(lout);
    e.bind(lpaused);
    e.movz_x(0, RC_CHAIN_PAUSED as u32);
    e.b(lout);
    e.bind(lmisuse);
    e.movz_x(0, RC_CHAIN_LOOPTOP_MISUSE as u32);
    e.b(lout);
    e.bind(lerr);
    e.movn_x(0, 0); // -1: the parked host error
    e.bind(lout);
    e.ldr_x(20, 31, 0x10);
    e.ldr_x(21, 31, 0x18);
    e.ldr_x(22, 31, 0x20);
    e.raw(0xA8C3_7BFD); // ldp x29, x30, [sp], #0x30
    e.ret();
    e.finish()
}

/// One stitched row-chain body plus its runtime rails. Owns the code block
/// (the W^X arena chunk stays alive while any body on it is). The program
/// is shape-static per chain family, so callers compile ONCE and reuse the
/// body across statements — the host varies per drive, never the code.
pub struct StitchedRowChain {
    block: jit_deform::CodeBlock,
    entry: RowChainFn,
    /// Wall-clock nanos in classification + emission + install.
    pub stitch_nanos: u64,
    pub code_bytes: usize,
}

impl StitchedRowChain {
    /// Stitch a chain body for `prog`. None = refused (classification, arch,
    /// master kill switch, the ROWCHAIN family knob, arena full): the caller
    /// drives its portable host for the WHOLE statement (interpreter-host
    /// deopt — no partial engagement).
    pub fn compile(prog: &Program) -> Option<StitchedRowChain> {
        if !crate::rowchain_available() {
            return None;
        }
        Self::compile_gated(prog)
    }

    /// [`compile`](Self::compile) minus the ROWCHAIN family knob — the
    /// parity oracle's entry (tests/parity.rs must exercise the stitched
    /// body under the default-OFF knob). Still arch- and master-kill-gated,
    /// still fail-closed. NOT for production callers: production dispatch
    /// goes through `compile` so the kill knob stays live (fault-injection
    /// leg of the acceptance ladder).
    #[doc(hidden)]
    pub fn compile_for_parity(prog: &Program) -> Option<StitchedRowChain> {
        if !crate::available() {
            return None;
        }
        Self::compile_gated(prog)
    }

    fn compile_gated(prog: &Program) -> Option<StitchedRowChain> {
        let t0 = std::time::Instant::now();
        plan_rowchain(prog).ok()?;
        let words = emit_rowchain(prog);
        // Wave-9 rung 0 fault lever: refuse at the INSTALL step exactly as
        // an exhausted arena would (fail-open landing under armed knobs).
        if crate::rowchain_arena_fault_for_tests() {
            return None;
        }
        let block = jit_deform::install_code(&words)?;
        // SAFETY: block holds a complete body starting at base, RX-mapped
        // and icache-flushed by install_code.
        let entry: RowChainFn = unsafe { core::mem::transmute(block.base()) };
        Some(StitchedRowChain {
            block,
            entry,
            stitch_nanos: t0.elapsed().as_nanos() as u64,
            code_bytes: words.len() * 4,
        })
    }

    /// One chain drive at the capacity-one boundary: identical to
    /// `interp::eval_row_chain` in host-call sequence, outcome, and error.
    /// `Paused` = a protocol step emitted one row; drive again to resume at
    /// the loop top. Errors are the host's own PgError, re-raised here.
    ///
    /// Generic over the concrete host (wave-9 rung 3 / D1b): the trampoline
    /// pair monomorphizes per `H`, so the host dispatch is static (for the
    /// DML chain, `MtInsertChainHost::protocol_call` inlines into its
    /// trampoline). `H = dyn RowChainHost` still compiles for erased
    /// callers and behaves identically (parity-pinned).
    pub fn run<H: RowChainHost + ?Sized>(&self, host: &mut H) -> PgResult<ChainOutcome> {
        let mut tc = TrampCtx { host, err: None };
        let mut params = RowChainJitParams {
            ctx: (&mut tc as *mut TrampCtx<'_, H>).cast::<c_void>(),
            next_row: tramp_next_row::<H>,
            protocol: tramp_protocol::<H>,
        };
        // SAFETY: the body only calls the two trampolines with ctx and
        // branches on their verdicts; ctx outlives the call; the trampolines
        // are extern "C" (unwind aborts, never crosses the stitched frame).
        let rc = unsafe { (self.entry)(&mut params) };
        match rc {
            RC_CHAIN_DONE => Ok(ChainOutcome::Done),
            RC_CHAIN_PAUSED => Ok(ChainOutcome::Paused),
            // Loop-top law violated by the host: the twin's identical loud
            // error (shared string — error identity by construction).
            RC_CHAIN_LOOPTOP_MISUSE => Err(rowop_misuse(LOOP_TOP_MISUSE)),
            _ => Err(tc
                .err
                .take()
                .expect("negative chain rc without a parked host error")),
        }
    }

    pub fn entry_addr(&self) -> usize {
        self.block.base() as usize
    }
}
