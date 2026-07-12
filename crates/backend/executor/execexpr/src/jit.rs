// Copy-and-patch expression JIT (C jit.c + llvmjit_expr.c analog with
// hand-encoded AArch64 stencils instead of LLVM; docs/optimizations/
// jit-qual.md). Compiled at ready_expr behind the planner's PGJIT_EXPR gate.
//
// Contract map (C llvm_compile_expr parity): every Step opcode is either an
// open-coded stencil or a `bl jitq_step` call into the interpreter's own arm
// (C's external-function emission). Program state is the interpreter's own:
// out cells / fcinfo images / anynull scratch are mcx-address-stable, so
// stencils bake their absolute addresses; kernels and the interpreter are
// interchangeable mid-program. Errors return through the env stash (PgResult
// carries no unwinding); SubPlan suspends exactly like run_program and
// resumes by re-entering the kernel at the label table.
//
// Kernel ABI: extern "C" fn(ctx: *mut JitCtx, start_step: u32) -> i64;
// returns RET_DONE_RETURN / RET_DONE_NORETURN (result already in the state's
// res cell / result slot) / RET_ERR (env.err set) / RET_SUSPEND (env.suspend
// set). x19 = ctx, x20 = kernel entry base; literal pool + per-step label
// table appended after the code.
//
// Kernel lifetime: blocks are owned by the executor session collector
// (execmain moves them onto the EState after InitPlan); ExprState holds only
// the Copy entry pointer. ExprStates never outlive their estate.

use core::ptr::NonNull;

use ::datum::{Datum, NullableDatum};
use ::types_error::{PgError, PgResult};
use ::types_fmgr::FmgrInfo;
use ::types_slot::SlotData;

use crate::interp::{EvalOutcome, EvalSlots, Resume, RetSlots, Suspension};
use crate::steps::{ExprState, FuncCall, OutRef, Step};

pub const PGJIT_PERFORM: i32 = 1 << 0;
pub const PGJIT_EXPR: i32 = 1 << 3;

const RET_DONE_RETURN: i64 = 0;
const RET_DONE_NORETURN: i64 = 1;
const RET_ERR: i64 = -1;
const RET_SUSPEND: i64 = -2;

// jitq_step returns the next step index, or one of these.
const STEP_ERR: i64 = -1;
const STEP_SUSPEND: i64 = -2;

type KernelFn = unsafe extern "C" fn(*mut JitCtx, u32) -> i64;

/// Per-call kernel arguments: slot arrays are extracted by the driver (their
/// backing is slot-creation-stable; FETCHSOME refills them in place).
#[repr(C)]
struct JitCtx {
    scan_v: *const Datum,
    scan_n: *const bool,
    inner_v: *const Datum,
    inner_n: *const bool,
    outer_v: *const Datum,
    outer_n: *const bool,
    env: *mut HelperEnv,
}

/// Rust-side environment for helper calls; pointers are live exactly for the
/// kernel invocation (driver stack frame).
struct HelperEnv {
    state: *mut (),
    slots: *mut (),
    ret: *mut (),
    result_slot: *mut (),
    err: Option<Box<PgError>>,
    // Panic payload caught at the extern "C" boundary: unwinding through a
    // kernel frame (no unwind info) is a guaranteed abort, so helpers catch
    // and the driver resumes the unwind on the Rust side (interpreter-
    // identical panic semantics).
    panic: Option<Box<dyn core::any::Any + Send>>,
    suspend: Option<(NonNull<()>, u32)>,
}

/// Copy handle stored on the ExprState; the block itself lives in the
/// session collector (estate-owned).
#[derive(Clone, Copy)]
pub struct JitHandle {
    entry: KernelFn,
    // Head FETCHSOME bounds hoisted out of the kernel (scan/inner/outer
    // last_var; 0 = none): one direct slot_getsomeattrs per call replaces a
    // per-row helper round trip.
    fetch: [u16; 3],
}

pub use ::jit_deform::JitInstrumentation as JitInstr;

pub struct JitCollector {
    pub blocks: Vec<::jit_deform::CodeBlock>,
    pub instr: JitInstr,
}

// Executor-session compile gate + kernel collector. Set for the InitPlan
// window by execmain (C reads estate->es_jit_flags through the PlanState
// parent; expression compile has no estate linkage here, so the flags ride a
// thread-local for the init window). Compiles outside a session (utility
// paths, EPQ) fall back to the interpreter.
thread_local! {
    static SESSION: core::cell::RefCell<Option<SessionState>> =
        const { core::cell::RefCell::new(None) };
}

struct SessionState {
    flags: i32,
    prev: Option<Box<SessionState>>,
    blocks: Vec<::jit_deform::CodeBlock>,
    instr: JitInstr,
}

// Process-wide open-window count: the compile hot path (every Program-shape
// ready_expr, including below-cost queries) pre-gates on one Relaxed load
// before touching the thread-local (TL reads cost ~40 instr; backing-atomic
// doctrine).
static OPEN_SESSIONS: core::sync::atomic::AtomicU32 = core::sync::atomic::AtomicU32::new(0);

/// Opens a compile window (nestable: SPI executors inside InitPlan).
pub fn session_begin(flags: i32) {
    OPEN_SESSIONS.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    if log_enabled() {
        eprintln!("jitq session begin: flags={flags:#x}");
    }
    SESSION.with(|s| {
        let prev = s.borrow_mut().take().map(Box::new);
        *s.borrow_mut() = Some(SessionState {
            flags,
            prev,
            blocks: Vec::new(),
            instr: JitInstr::default(),
        });
    });
}

/// Closes the window, returning this window's kernels + counters.
pub fn session_end() -> JitCollector {
    OPEN_SESSIONS.fetch_sub(1, core::sync::atomic::Ordering::Relaxed);
    SESSION.with(|s| {
        let cur = s.borrow_mut().take().expect("jit session_end without begin");
        *s.borrow_mut() = cur.prev.map(|b| *b);
        JitCollector { blocks: cur.blocks, instr: cur.instr }
    })
}

fn session_flags() -> i32 {
    SESSION.with(|s| s.borrow().as_ref().map_or(0, |c| c.flags))
}

/// AIO-style availability + kill switch (`PGRUST_JIT_QUAL=0|off`).
pub fn available() -> bool {
    #[cfg(target_arch = "aarch64")]
    {
        static OFF: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        !*OFF.get_or_init(|| {
            matches!(std::env::var("PGRUST_JIT_QUAL").as_deref(), Ok("0") | Ok("off"))
        })
    }
    #[cfg(not(target_arch = "aarch64"))]
    false
}

/// ready_expr hook: compile the program if the session gate and the stencil
/// coverage allow; refusal falls open to the interpreter.
pub(crate) fn try_compile(state: &mut ExprState<'_>) {
    if OPEN_SESSIONS.load(core::sync::atomic::Ordering::Relaxed) == 0 || !available() {
        return;
    }
    let flags = session_flags();
    if flags & PGJIT_PERFORM == 0 || flags & PGJIT_EXPR == 0 {
        return;
    }
    let t0 = std::time::Instant::now();
    let Some((words, fetch)) = emit::emit_program(state) else {
        note_refusal(state);
        return;
    };
    let Some(block) = ::jit_deform::install_code(&words) else {
        stats().arena_full.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        return;
    };
    stats().compiled.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    if log_enabled() {
        eprintln!("jitq compile: {} steps, {} bytes", state.steps().len(), words.len() * 4);
    }
    // SAFETY: block holds a complete kernel starting at base, RX-mapped.
    let entry: KernelFn = unsafe { core::mem::transmute(block.base()) };
    let nanos = t0.elapsed().as_nanos() as u64;
    SESSION.with(|s| {
        let mut b = s.borrow_mut();
        let cur = b.as_mut().expect("gate checked above");
        cur.blocks.push(block);
        cur.instr.created_functions += 1;
        cur.instr.generation_nanos += nanos;
    });
    state.jit = Some(JitHandle { entry, fetch });
}

// Coverage accounting (journal evidence; PGRUST_JITQ_LOG=1 additionally logs
// each refusal's first unsupported step to stderr). Zero refusals on the
// regress corpus with JIT forced is the landing bar.
#[derive(Default)]
pub struct JitStats {
    pub compiled: core::sync::atomic::AtomicU64,
    pub refused: core::sync::atomic::AtomicU64,
    pub arena_full: core::sync::atomic::AtomicU64,
    pub runs: core::sync::atomic::AtomicU64,
}

pub fn stats() -> &'static JitStats {
    static STATS: std::sync::OnceLock<JitStats> = std::sync::OnceLock::new();
    STATS.get_or_init(JitStats::default)
}

fn log_enabled() -> bool {
    static LOG: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *LOG.get_or_init(|| {
        matches!(std::env::var("PGRUST_JITQ_LOG").as_deref(), Ok(v) if !v.is_empty() && v != "0")
    })
}

fn note_refusal(state: &ExprState<'_>) {
    stats().refused.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    if log_enabled() {
        for s in state.steps() {
            if !emit::step_supported(s) {
                let d = format!("{s:?}");
                let name = d.split([' ', '{']).next().unwrap_or(&d);
                eprintln!("jitq refuse: {name} (program {} steps)", state.steps().len());
                break;
            }
        }
    }
}

fn slot_arrays(slot: Option<&mut SlotData<'_>>) -> (*const Datum, *const bool) {
    match slot {
        Some(s) => {
            let b = s.base();
            (b.tts_values.as_ptr(), b.tts_isnull.as_ptr())
        }
        None => (core::ptr::null(), core::ptr::null()),
    }
}

/// The driver: run_program's kernel twin. Result/suspension protocol matches
/// run_program exactly (interp::eval dispatches here when a kernel exists).
pub(crate) fn run_jit<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    ret: &mut RetSlots<'_, 'mcx>,
    result_slot: Option<&mut SlotData<'mcx>>,
    resume: Option<Resume>,
) -> PgResult<EvalOutcome> {
    let handle = state.jit.expect("run_jit without a kernel");
    if log_enabled() {
        let n = stats().runs.fetch_add(1, core::sync::atomic::Ordering::Relaxed) + 1;
        if n.is_power_of_two() {
            eprintln!("jitq runs: {n}");
        }
    }
    let res = state.resnd;
    let mut start = 0u32;
    if let Some(r) = resume {
        let (regs, result, step) = r.into_parts();
        // SAFETY: res is the state's live result cell.
        unsafe { res.write(regs) };
        let Step::SubPlan { out, .. } = state.steps[step as usize] else {
            panic!("resume target is not a SubPlan step")
        };
        // SAFETY: out cells are 'mcx-live (compile-time invariant).
        unsafe { out.0.write(result) };
        start = step + 1;
    }
    if handle.fetch[0] > 0 {
        exectuples::slot_getsomeattrs(
            slots.scan.as_deref_mut().expect("scan fetch without slot"),
            handle.fetch[0] as i32,
        );
    }
    if handle.fetch[1] > 0 {
        exectuples::slot_getsomeattrs(
            slots.inner.as_deref_mut().expect("inner fetch without slot"),
            handle.fetch[1] as i32,
        );
    }
    if handle.fetch[2] > 0 {
        exectuples::slot_getsomeattrs(
            slots.outer.as_deref_mut().expect("outer fetch without slot"),
            handle.fetch[2] as i32,
        );
    }
    let (scan_v, scan_n) = slot_arrays(slots.scan.as_deref_mut());
    let (inner_v, inner_n) = slot_arrays(slots.inner.as_deref_mut());
    let (outer_v, outer_n) = slot_arrays(slots.outer.as_deref_mut());
    let mut env = HelperEnv {
        state: (state as *mut ExprState<'mcx>).cast(),
        slots: (slots as *mut EvalSlots<'_, 'mcx>).cast(),
        ret: (ret as *mut RetSlots<'_, 'mcx>).cast(),
        result_slot: match result_slot {
            Some(r) => (r as *mut SlotData<'mcx>).cast(),
            None => core::ptr::null_mut(),
        },
        err: None,
        panic: None,
        suspend: None,
    };
    let mut ctx = JitCtx {
        scan_v,
        scan_n,
        inner_v,
        inner_n,
        outer_v,
        outer_n,
        env: &mut env,
    };
    // SAFETY: kernel compiled from this state's live program; ctx/env outlive
    // the call; all baked addresses are 'mcx-stable allocations of the state.
    let rc = unsafe { (handle.entry)(&mut ctx, start) };
    match rc {
        RET_DONE_RETURN => {
            // SAFETY: res is the state's live result cell.
            Ok(EvalOutcome::Done(unsafe { res.read() }))
        }
        RET_DONE_NORETURN => Ok(EvalOutcome::Done(NullableDatum::null())),
        RET_ERR => {
            if let Some(p) = env.panic.take() {
                std::panic::resume_unwind(p);
            }
            Err(env.err.take().expect("jit kernel error without a stashed PgError"))
        }
        RET_SUSPEND => {
            let (sstate, step) = env.suspend.take().expect("jit suspend without state");
            // SAFETY: res is the state's live result cell.
            Ok(EvalOutcome::Suspended(Suspension::new(sstate, step, unsafe { res.read() })))
        }
        other => panic!("jit kernel returned unknown code {other}"),
    }
}

/// One interpreter step, callable from kernel code (C's external-function
/// emission). Returns the next step index or STEP_ERR/STEP_SUSPEND.
/// # Safety
/// `env` is the live HelperEnv of the current kernel invocation; `ix` indexes
/// a step of the compiled program.
unsafe extern "C" fn jitq_step(env: *mut HelperEnv, ix: u32) -> i64 {
    // SAFETY: driver-owned env, live for the kernel call; the pointers were
    // created from exclusive borrows the driver holds across the call.
    let env = unsafe { &mut *env };
    let state = unsafe { &mut *(env.state as *mut ExprState<'static>) };
    let slots = unsafe { &mut *(env.slots as *mut EvalSlots<'static, 'static>) };
    let ret = unsafe { &mut *(env.ret as *mut RetSlots<'static, 'static>) };
    let result_slot = if env.result_slot.is_null() {
        None
    } else {
        Some(unsafe { &mut *(env.result_slot as *mut SlotData<'static>) })
    };
    let r = std::panic::catch_unwind(core::panic::AssertUnwindSafe(|| {
        crate::interp::exec_one_step(state, slots, ret, result_slot, ix)
    }));
    match r {
        Ok(Ok(crate::interp::StepFlow::Next)) => ix as i64 + 1,
        Ok(Ok(crate::interp::StepFlow::Jump(t))) => t as i64,
        Ok(Ok(crate::interp::StepFlow::Suspend(sstate))) => {
            env.suspend = Some((sstate, ix));
            STEP_SUSPEND
        }
        Ok(Err(e)) => {
            env.err = Some(e);
            STEP_ERR
        }
        Err(p) => {
            env.panic = Some(p);
            STEP_ERR
        }
    }
}

// Packed ctl word for jitq_call: nargs | strict<<14 | fusage<<15 (fits movz).
const CALL_STRICT: u32 = 1 << 14;
const CALL_FUSAGE: u32 = 1 << 15;

/// Generic fmgr trampoline: the Func* opcode body (strict check + invoke +
/// write out). PGFunction is Rust-ABI, so kernels can only reach it here.
/// # Safety
/// Baked pointers from a live compiled program (flinfo/fcinfo image/out cell).
unsafe extern "C" fn jitq_call(
    env: *mut HelperEnv,
    flinfo: *mut FmgrInfo,
    fcinfo: *mut u8,
    out: *mut NullableDatum,
    ctl: u64,
) -> i64 {
    let nargs = (ctl as u32 & 0x3FFF) as u16;
    let strict = ctl as u32 & CALL_STRICT != 0;
    let fusage = ctl as u32 & CALL_FUSAGE != 0;
    let fcinfo_nn = unsafe { NonNull::new_unchecked(fcinfo) };
    if strict {
        // SAFETY: reads nargs arg slots of the call's live image.
        let anynull = (0..nargs as usize)
            .any(|i| unsafe { crate::steps::arg_slot_of(fcinfo_nn, i).read().isnull });
        if anynull {
            // SAFETY: live out cell.
            unsafe { out.write(NullableDatum::null()) };
            return 0;
        }
    }
    let call = FuncCall {
        fcinfo: fcinfo_nn,
        // SAFETY: live mcx-boxed FmgrInfo baked at compile.
        flinfo: unsafe { NonNull::new_unchecked(flinfo) },
        frame: u32::MAX,
        nargs,
    };
    let r = std::panic::catch_unwind(core::panic::AssertUnwindSafe(|| {
        if fusage {
            crate::interp::invoke_fusage(&call)
        } else {
            crate::interp::invoke(&call)
        }
    }));
    match r {
        Ok(Ok((value, isnull))) => {
            // SAFETY: live out cell.
            unsafe { out.write(NullableDatum { value, isnull }) };
            0
        }
        Ok(Err(e)) => {
            // SAFETY: driver-owned env, live for the kernel call.
            unsafe { (*env).err = Some(e) };
            STEP_ERR
        }
        Err(p) => {
            // SAFETY: as above.
            unsafe { (*env).panic = Some(p) };
            STEP_ERR
        }
    }
}

#[cfg(target_arch = "aarch64")]
mod emit {
    use super::*;
    use crate::steps::CmpOp;

    // Layout facts the stencils bake; asserted, not assumed.
    const ND_VALUE: u32 = core::mem::offset_of!(NullableDatum, value) as u32;
    const ND_ISNULL: u32 = core::mem::offset_of!(NullableDatum, isnull) as u32;
    const _: () = assert!(core::mem::size_of::<NullableDatum>() == 16);

    const CTX_SCAN_V: u32 = core::mem::offset_of!(JitCtx, scan_v) as u32;
    const CTX_SCAN_N: u32 = core::mem::offset_of!(JitCtx, scan_n) as u32;
    const CTX_INNER_V: u32 = core::mem::offset_of!(JitCtx, inner_v) as u32;
    const CTX_INNER_N: u32 = core::mem::offset_of!(JitCtx, inner_n) as u32;
    const CTX_OUTER_V: u32 = core::mem::offset_of!(JitCtx, outer_v) as u32;
    const CTX_OUTER_N: u32 = core::mem::offset_of!(JitCtx, outer_n) as u32;
    const CTX_ENV: u32 = core::mem::offset_of!(JitCtx, env) as u32;

    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    enum Target {
        Step(u32),
        ExitErr,
        ExitDoneRet,
        ExitDoneNoRet,
        Dispatch,
        NegSentinel,
        Local(u32),
    }

    #[derive(Clone, Copy)]
    #[allow(dead_code)]
    enum Cond {
        Eq = 0,
        Ne = 1,
        Ge = 10,
        Lt = 11,
        Gt = 12,
        Le = 13,
        Vs = 6,
        // Unsigned compares (oid comparators): HS/LO/HI/LS.
        Hs = 2,
        Lo = 3,
        Hi = 8,
        Ls = 9,
    }

    struct Emitter {
        code: Vec<u32>,
        // (instruction index, target, kind); kind: 0 = b, 1 = b.cond (cond
        // pre-encoded in the placeholder), 2 = cbz/cbnz/tbnz-style imm19/14
        // already positioned by the placeholder mask.
        fixups: Vec<(usize, Target)>,
        lits: Vec<u64>,
        lit_uses: Vec<(usize, u32)>,
        local_seq: u32,
    }

    impl Emitter {
        fn new() -> Emitter {
            Emitter {
                code: Vec::with_capacity(256),
                fixups: Vec::new(),
                lits: Vec::new(),
                lit_uses: Vec::new(),
                local_seq: 0,
            }
        }

        fn raw(&mut self, w: u32) {
            self.code.push(w);
        }

        fn new_local(&mut self) -> Target {
            self.local_seq += 1;
            Target::Local(self.local_seq - 1)
        }

        // LDR Xt, <literal>: 64-bit constant from the pool.
        fn ldr_lit(&mut self, rt: u32, v: u64) {
            let id = match self.lits.iter().position(|&l| l == v) {
                Some(i) => i as u32,
                None => {
                    self.lits.push(v);
                    (self.lits.len() - 1) as u32
                }
            };
            self.lit_uses.push((self.code.len(), id));
            self.code.push(0x5800_0000 | rt);
        }

        fn ldr_x(&mut self, rt: u32, rn: u32, off: u32) {
            debug_assert!(off % 8 == 0 && off / 8 <= 4095);
            self.raw(0xF940_0000 | ((off / 8) << 10) | (rn << 5) | rt);
        }

        fn str_x(&mut self, rt: u32, rn: u32, off: u32) {
            debug_assert!(off % 8 == 0 && off / 8 <= 4095);
            self.raw(0xF900_0000 | ((off / 8) << 10) | (rn << 5) | rt);
        }

        fn ldrb(&mut self, rt: u32, rn: u32, off: u32) {
            debug_assert!(off <= 4095);
            self.raw(0x3940_0000 | (off << 10) | (rn << 5) | rt);
        }

        fn strb(&mut self, rt: u32, rn: u32, off: u32) {
            debug_assert!(off <= 4095);
            self.raw(0x3900_0000 | (off << 10) | (rn << 5) | rt);
        }

        fn movz_w(&mut self, rd: u32, imm16: u32) {
            debug_assert!(imm16 <= 0xFFFF);
            self.raw(0x5280_0000 | (imm16 << 5) | rd);
        }

        fn movz_x(&mut self, rd: u32, imm16: u32) {
            debug_assert!(imm16 <= 0xFFFF);
            self.raw(0xD280_0000 | (imm16 << 5) | rd);
        }

        // MOVN Xd, #imm16: xd = !imm16 (so imm16=0 -> -1, 1 -> -2).
        fn movn_x(&mut self, rd: u32, imm16: u32) {
            self.raw(0x9280_0000 | (imm16 << 5) | rd);
        }

        fn mov_x(&mut self, rd: u32, rm: u32) {
            self.raw(0xAA00_03E0 | (rm << 16) | rd);
        }

        fn cmp_w_imm(&mut self, rn: u32, imm12: u32) {
            debug_assert!(imm12 <= 4095);
            self.raw(0x7100_001F | (imm12 << 10) | (rn << 5));
        }

        fn cmp_w_w(&mut self, rn: u32, rm: u32) {
            self.raw(0x6B00_001F | (rm << 16) | (rn << 5));
        }

        fn cmp_x_x(&mut self, rn: u32, rm: u32) {
            self.raw(0xEB00_001F | (rm << 16) | (rn << 5));
        }

        fn orr_w(&mut self, rd: u32, rn: u32, rm: u32) {
            self.raw(0x2A00_0000 | (rm << 16) | (rn << 5) | rd);
        }

        fn cset_x(&mut self, rd: u32, cond: Cond) {
            // CSINC Xd, XZR, XZR, inv(cond).
            self.raw(0x9A9F_07E0 | (((cond as u32) ^ 1) << 12) | rd);
        }

        fn sxtw(&mut self, xd: u32, wn: u32) {
            self.raw(0x9340_7C00 | (wn << 5) | xd);
        }

        fn adds_w(&mut self, rd: u32, rn: u32, rm: u32) {
            self.raw(0x2B00_0000 | (rm << 16) | (rn << 5) | rd);
        }

        fn subs_w(&mut self, rd: u32, rn: u32, rm: u32) {
            self.raw(0x6B00_0000 | (rm << 16) | (rn << 5) | rd);
        }

        fn adds_x(&mut self, rd: u32, rn: u32, rm: u32) {
            self.raw(0xAB00_0000 | (rm << 16) | (rn << 5) | rd);
        }

        fn subs_x(&mut self, rd: u32, rn: u32, rm: u32) {
            self.raw(0xEB00_0000 | (rm << 16) | (rn << 5) | rd);
        }

        fn smull(&mut self, xd: u32, wn: u32, wm: u32) {
            self.raw(0x9B20_7C00 | (wm << 16) | (wn << 5) | xd);
        }

        fn mul_x(&mut self, xd: u32, xn: u32, xm: u32) {
            self.raw(0x9B00_7C00 | (xm << 16) | (xn << 5) | xd);
        }

        fn smulh(&mut self, xd: u32, xn: u32, xm: u32) {
            self.raw(0x9B40_7C00 | (xm << 16) | (xn << 5) | xd);
        }

        // SDIV Wd, Wn, Wm (int24div body; divisor pre-checked non-zero).
        fn sdiv_w(&mut self, wd: u32, wn: u32, wm: u32) {
            self.raw(0x1AC0_0C00 | (wm << 16) | (wn << 5) | wd);
        }

        // CMP Xn, Wm SXTW (sext-compare, int4mul product check).
        fn cmp_x_w_sxtw(&mut self, rn: u32, rm: u32) {
            self.raw(0xEB20_C01F | (rm << 16) | (rn << 5));
        }

        // ASR Xd, Xn, #63 (int8mul high-part check).
        fn asr63(&mut self, rd: u32, rn: u32) {
            self.raw(0x937F_FC00 | (rn << 5) | rd);
        }

        fn blr(&mut self, rn: u32) {
            self.raw(0xD63F_0000 | (rn << 5));
        }

        fn br(&mut self, rn: u32) {
            self.raw(0xD61F_0000 | (rn << 5));
        }

        fn ret(&mut self) {
            self.raw(0xD65F_03C0);
        }

        fn b(&mut self, t: Target) {
            self.fixups.push((self.code.len(), t));
            self.raw(0x1400_0000);
        }

        fn b_cond(&mut self, cond: Cond, t: Target) {
            self.fixups.push((self.code.len(), t));
            self.raw(0x5400_0000 | cond as u32);
        }

        fn cbz_w(&mut self, rt: u32, t: Target) {
            self.fixups.push((self.code.len(), t));
            self.raw(0x3400_0000 | rt);
        }

        fn cbnz_w(&mut self, rt: u32, t: Target) {
            self.fixups.push((self.code.len(), t));
            self.raw(0x3500_0000 | rt);
        }

        fn cbz_x(&mut self, rt: u32, t: Target) {
            self.fixups.push((self.code.len(), t));
            self.raw(0xB400_0000 | rt);
        }

        fn cbnz_x(&mut self, rt: u32, t: Target) {
            self.fixups.push((self.code.len(), t));
            self.raw(0xB500_0000 | rt);
        }

        // TBNZ Xt, #63 (sign test on a returned i64).
        fn tbnz_sign(&mut self, rt: u32, t: Target) {
            self.fixups.push((self.code.len(), t));
            self.raw(0xB7F8_0000 | rt);
        }

        // ADR Xd, <target> (label address within the kernel).
        fn adr(&mut self, rd: u32, t: Target) {
            self.fixups.push((self.code.len(), t));
            self.raw(0x1000_0000 | rd);
        }
    }

    // x19 = ctx, x20 = entry base; x0-x17 scratch.
    const CTX: u32 = 19;
    const BASE: u32 = 20;

    // Reserved Target::Local ids (per-step locals count up from 0).
    const LOCAL_ENTRY: u32 = u32::MAX;
    const LOCAL_EPILOGUE: u32 = u32::MAX - 1;
    const LOCAL_TABLE: u32 = u32::MAX - 2;

    fn slot_ctx_offsets(step: &Step) -> Option<(u32, u32)> {
        Some(match step {
            Step::ScanVar { .. } => (CTX_SCAN_V, CTX_SCAN_N),
            Step::InnerVar { .. } => (CTX_INNER_V, CTX_INNER_N),
            Step::OuterVar { .. } => (CTX_OUTER_V, CTX_OUTER_N),
            _ => return None,
        })
    }

    // Inline-able strict-2 integer bodies: the inline-supported subset of the
    // CmpOp census (cmp_cond returns Some) + int add/sub/mul with C-exact
    // overflow semantics (overflow branches to the generic call, which raises
    // the real ereport) + int24div (zero divisor branches to the generic call
    // for C's division-by-zero ereport). The int2/int4 mixed add/sub/mul
    // reuse the Int4* arms verbatim: Datum int words are canonical
    // (sign-extended), so the int2 operand's w-register view IS C's promoted
    // int32, and the result type is int4 either way.
    #[derive(Clone, Copy)]
    enum InlineOp {
        Cmp(CmpOp),
        Int4Pl,
        Int4Mi,
        Int4Mul,
        Int8Pl,
        Int8Mi,
        Int8Mul,
        Int24Div,
    }

    fn inline_op(fn_oid: ::types_core::Oid) -> Option<InlineOp> {
        if let Some(c) = CmpOp::for_fn_oid(fn_oid) {
            // Census comparators without an inline stencil (the float
            // families: NaN-aware bodies, no single-cond compare) fall to the
            // generic call emission, same as before their census admission.
            return cmp_cond(c).map(|_| InlineOp::Cmp(c));
        }
        // The JIT arithmetic admission set now lives in the central registry
        // (`lanereg`, design §3a) as the JitArith tier; decode its neutral
        // ArithShape into this JIT's selector. Every in-tree JitArith shape
        // (W4/W8/W24 × Add/Sub/Mul, W24 × Div) decodes.
        let s = ::lanereg::jit_arith(fn_oid)?;
        use ::lanereg::{ArithKind as K, ArithWidth as A};
        Some(match (s.width, s.op) {
            (A::W4 | A::W24, K::Add) => InlineOp::Int4Pl,
            (A::W4 | A::W24, K::Sub) => InlineOp::Int4Mi,
            (A::W4 | A::W24, K::Mul) => InlineOp::Int4Mul,
            (A::W24, K::Div) => InlineOp::Int24Div,
            (A::W8, K::Add) => InlineOp::Int8Pl,
            (A::W8, K::Sub) => InlineOp::Int8Mi,
            (A::W8, K::Mul) => InlineOp::Int8Mul,
            _ => return None,
        })
    }

    fn cmp_cond(op: CmpOp) -> Option<(bool, Cond)> {
        use CmpOp::*;
        // (wide compare?, condition); Int48/84 sign-extend the 32-bit side.
        // Int24/42 need no extend: Datum int words are canonical
        // (sign-extended), so the narrow compare already sees C's promoted
        // int32 on the int2 side. Oid compares the low word unsigned. The
        // float comparators have no inline stencil (None): their NaN-aware
        // total order is not a single-condition compare, so they keep the
        // generic per-row call under the JIT (the AOT bitmap tier still owns
        // their quals).
        Some(match op {
            Int4Eq | Int2Eq | Int24Eq | Int42Eq => (false, Cond::Eq),
            Int4Ne | Int2Ne | Int24Ne | Int42Ne => (false, Cond::Ne),
            Int4Lt | Int2Lt | Int24Lt | Int42Lt => (false, Cond::Lt),
            Int4Le | Int2Le | Int24Le | Int42Le => (false, Cond::Le),
            Int4Gt | Int2Gt | Int24Gt | Int42Gt => (false, Cond::Gt),
            Int4Ge | Int2Ge | Int24Ge | Int42Ge => (false, Cond::Ge),
            Int8Eq | Int84Eq | Int48Eq => (true, Cond::Eq),
            Int8Ne | Int84Ne | Int48Ne => (true, Cond::Ne),
            Int8Lt | Int84Lt | Int48Lt => (true, Cond::Lt),
            Int8Le | Int84Le | Int48Le => (true, Cond::Le),
            Int8Gt | Int84Gt | Int48Gt => (true, Cond::Gt),
            Int8Ge | Int84Ge | Int48Ge => (true, Cond::Ge),
            OidEq => (false, Cond::Eq),
            OidNe => (false, Cond::Ne),
            OidLt => (false, Cond::Lo),
            OidLe => (false, Cond::Ls),
            OidGt => (false, Cond::Hi),
            OidGe => (false, Cond::Hs),
            Float4Eq | Float4Ne | Float4Lt | Float4Le | Float4Gt | Float4Ge | Float8Eq
            | Float8Ne | Float8Lt | Float8Le | Float8Gt | Float8Ge | Float48Eq | Float48Ne
            | Float48Lt | Float48Le | Float48Gt | Float48Ge | Float84Eq | Float84Ne
            | Float84Lt | Float84Le | Float84Gt | Float84Ge => return None,
        })
    }

    fn cmp_extends(op: CmpOp) -> (bool, bool) {
        use CmpOp::*;
        // (sign-extend a?, sign-extend b?) before a wide compare.
        match op {
            Int48Eq | Int48Ne | Int48Lt | Int48Le | Int48Gt | Int48Ge => (true, false),
            Int84Eq | Int84Ne | Int84Lt | Int84Le | Int84Gt | Int84Ge => (false, true),
            _ => (false, false),
        }
    }

    fn out_addr(out: &OutRef) -> u64 {
        out.0.as_ptr() as u64
    }

    // Which steps route through the generic jitq_step helper (C's
    // external-function emission tier). Everything not stenciled below and
    // not in this list refuses the program (coverage grows by rung; the
    // refusal set must reach zero on the regress corpus).
    fn helper_supported(step: &Step) -> bool {
        crate::interp::step_has_helper(step)
    }

    pub(super) fn step_supported(step: &Step) -> bool {
        step_stencilable(step) || helper_supported(step)
    }

    // Straight-line register cache over the caller-saved x2..x7 pairs: cell
    // (value, isnull) copies live in registers between adjacent stencils.
    // Cells stay store-coherent (every write still hits memory) so helper
    // steps and the interpreter contract see current state; the cache only
    // elides reloads. Flushed at every jump target and after any bl.
    struct RegCache {
        entries: Vec<(u64, u32, u32)>,
        next: usize,
    }

    const CACHE_PAIRS: [(u32, u32); 3] = [(2, 3), (4, 5), (6, 7)];

    impl RegCache {
        fn new() -> RegCache {
            RegCache { entries: Vec::with_capacity(3), next: 0 }
        }

        fn lookup(&self, addr: u64) -> Option<(u32, u32)> {
            self.entries.iter().find(|(a, _, _)| *a == addr).map(|(_, v, n)| (*v, *n))
        }

        fn alloc(&mut self, addr: u64) -> (u32, u32) {
            let (v, n) = CACHE_PAIRS[self.next];
            self.next = (self.next + 1) % CACHE_PAIRS.len();
            self.entries.retain(|(a, ev, _)| *a != addr && *ev != v);
            self.entries.push((addr, v, n));
            (v, n)
        }

        fn invalidate(&mut self, addr: u64) {
            self.entries.retain(|(a, _, _)| *a != addr);
        }

        fn flush(&mut self) {
            self.entries.clear();
            self.next = 0;
        }
    }

    // Every index reachable by an emitted or helper-returned jump; the cache
    // must be empty at these (dispatch can land mid-stream). CONTRACT: any
    // new step arm that can return StepFlow::Jump must either have its
    // targets enumerated here or force whole-program flushing (JsonExprPath
    // precedent in emit_program).
    fn jump_targets(steps: &[Step]) -> Vec<bool> {
        let mut t = vec![false; steps.len()];
        for s in steps {
            match s {
                Step::Qual { jumpdone }
                | Step::Jump { jumpdone }
                | Step::JumpIfNotTrue { jumpdone, .. }
                | Step::JumpIfNotNull { jumpdone, .. }
                | Step::JumpIfNull { jumpdone, .. }
                | Step::BoolAndStepFirst { jumpdone, .. }
                | Step::BoolAndStep { jumpdone, .. }
                | Step::BoolOrStepFirst { jumpdone, .. }
                | Step::BoolOrStep { jumpdone, .. }
                | Step::SbsrefSubscripts { jumpdone, .. }
                | Step::JsonbSbsrefSubscripts { jumpdone, .. }
                | Step::ReturningExprStep { jumpdone, .. } => t[*jumpdone as usize] = true,
                Step::AggStrictInputCheck { jumpnull, .. }
                | Step::AggStrictInputCheck1 { jumpnull, .. }
                | Step::AggStrictDeserialize { jumpnull, .. } => t[*jumpnull as usize] = true,
                Step::RowCompareStep { jumpnull, jumpdone, .. } => {
                    t[*jumpnull as usize] = true;
                    t[*jumpdone as usize] = true;
                }
                _ => {}
            }
        }
        t
    }

    fn emit_write_out_const(e: &mut Emitter, out: u64, value: Datum, isnull: bool) {
        e.ldr_lit(9, out);
        if value.as_usize() == 0 {
            e.str_x(31, 9, ND_VALUE);
        } else {
            e.ldr_lit(10, value.as_usize() as u64);
            e.str_x(10, 9, ND_VALUE);
        }
        if isnull {
            e.movz_w(10, 1);
            e.strb(10, 9, ND_ISNULL);
        } else {
            e.strb(31, 9, ND_ISNULL);
        }
    }

    // Load an out cell: value -> x{vr}, isnull byte -> w{nr}; cell addr in x{ar}.
    fn emit_read_cell(e: &mut Emitter, addr: u64, ar: u32, vr: u32, nr: u32) {
        e.ldr_lit(ar, addr);
        e.ldr_x(vr, ar, ND_VALUE);
        e.ldrb(nr, ar, ND_ISNULL);
    }

    // Cache-aware read: registers holding the cell if cached, else a load
    // into the provided scratch pair.
    fn read_cell_cached(
        e: &mut Emitter,
        cache: &RegCache,
        addr: u64,
        ar: u32,
        vr: u32,
        nr: u32,
    ) -> (u32, u32) {
        match cache.lookup(addr) {
            Some(rn) => rn,
            None => {
                emit_read_cell(e, addr, ar, vr, nr);
                (vr, nr)
            }
        }
    }

    fn emit_helper_call(e: &mut Emitter, ix: u32, nsteps: u32) {
        e.ldr_x(0, CTX, CTX_ENV);
        e.movz_w(1, ix & 0xFFFF);
        debug_assert!(ix <= 0xFFFF);
        e.ldr_lit(8, super::jitq_step as usize as u64);
        e.blr(8);
        e.tbnz_sign(0, Target::NegSentinel);
        let next = ix + 1;
        if next < nsteps {
            if next <= 4095 {
                e.cmp_w_imm(0, next);
            } else {
                e.movz_w(9, next & 0xFFFF);
                e.cmp_w_w(0, 9);
            }
            e.b_cond(Cond::Ne, Target::Dispatch);
        } else {
            e.b(Target::Dispatch);
        }
    }

    fn emit_func_call(e: &mut Emitter, call: &FuncCall, out: u64, strict: bool, fusage: bool) {
        // SAFETY (emit-time read): live mcx-boxed FmgrInfo.
        let flinfo = call.flinfo.as_ptr() as u64;
        let fcinfo = call.fcinfo.as_ptr() as u64;
        let mut ctl = call.nargs as u32;
        if strict {
            ctl |= super::CALL_STRICT;
        }
        if fusage {
            ctl |= super::CALL_FUSAGE;
        }
        e.ldr_x(0, CTX, CTX_ENV);
        e.ldr_lit(1, flinfo);
        e.ldr_lit(2, fcinfo);
        e.ldr_lit(3, out);
        e.movz_x(4, ctl);
        e.ldr_lit(8, super::jitq_call as usize as u64);
        e.blr(8);
        e.tbnz_sign(0, Target::ExitErr);
    }

    // Strict-2 inline body; args are the call's fcinfo arg cells (stable
    // addresses the producing steps already stored to). Datum int words are
    // canonical (from_i32/fetch_att sign-extend), so mixed-width compares
    // sign-extend into scratch without mutating cached registers.
    fn emit_inline_strict2(
        e: &mut Emitter,
        cache: &mut RegCache,
        call: &FuncCall,
        out: u64,
        op: InlineOp,
    ) {
        let a0 = crate::steps::call_arg_addr(call, 0) as u64;
        let a1 = crate::steps::call_arg_addr(call, 1) as u64;
        let done = e.new_local();
        let nullout = e.new_local();
        let (v0, n0) = read_cell_cached(e, cache, a0, 8, 10, 12);
        let (v1, n1) = read_cell_cached(e, cache, a1, 9, 11, 13);
        e.orr_w(14, n0, n1);
        e.cbnz_w(14, nullout);
        match op {
            InlineOp::Cmp(c) => {
                let (wide, cond) = cmp_cond(c).expect("inline_op admitted a stencil-less CmpOp");
                let (sxa, sxb) = cmp_extends(c);
                let (mut lv, mut rv) = (v0, v1);
                if sxa {
                    e.sxtw(15, v0);
                    lv = 15;
                }
                if sxb {
                    e.sxtw(16, v1);
                    rv = 16;
                }
                if wide {
                    e.cmp_x_x(lv, rv);
                } else {
                    e.cmp_w_w(lv, rv);
                }
                let (ov, on) = cache.alloc(out);
                e.cset_x(ov, cond);
                e.movz_w(on, 0);
                e.ldr_lit(9, out);
                e.str_x(ov, 9, ND_VALUE);
                e.strb(on, 9, ND_ISNULL);
                e.b(done);
                bind_local(e, nullout);
                e.mov_x(ov, 31);
                e.movz_w(on, 1);
                e.ldr_lit(9, out);
                e.str_x(ov, 9, ND_VALUE);
                e.strb(on, 9, ND_ISNULL);
                bind_local(e, done);
                return;
            }
            InlineOp::Int4Pl | InlineOp::Int4Mi | InlineOp::Int4Mul => {
                let ovf = e.new_local();
                match op {
                    InlineOp::Int4Pl => {
                        e.adds_w(15, v0, v1);
                        e.b_cond(Cond::Vs, ovf);
                    }
                    InlineOp::Int4Mi => {
                        e.subs_w(15, v0, v1);
                        e.b_cond(Cond::Vs, ovf);
                    }
                    _ => {
                        e.smull(15, v0, v1);
                        e.cmp_x_w_sxtw(15, 15);
                        e.b_cond(Cond::Ne, ovf);
                    }
                }
                e.sxtw(15, 15);
                e.ldr_lit(9, out);
                e.str_x(15, 9, ND_VALUE);
                e.strb(31, 9, ND_ISNULL);
                e.b(done);
                bind_local(e, ovf);
                // Overflow: the real function raises the C-exact ereport.
                emit_func_call(e, call, out, true, false);
            }
            InlineOp::Int8Pl | InlineOp::Int8Mi | InlineOp::Int8Mul => {
                let ovf = e.new_local();
                match op {
                    InlineOp::Int8Pl => {
                        e.adds_x(15, v0, v1);
                        e.b_cond(Cond::Vs, ovf);
                    }
                    InlineOp::Int8Mi => {
                        e.subs_x(15, v0, v1);
                        e.b_cond(Cond::Vs, ovf);
                    }
                    _ => {
                        e.mul_x(15, v0, v1);
                        e.smulh(16, v0, v1);
                        e.asr63(17, 15);
                        e.cmp_x_x(16, 17);
                        e.b_cond(Cond::Ne, ovf);
                    }
                }
                e.ldr_lit(9, out);
                e.str_x(15, 9, ND_VALUE);
                e.strb(31, 9, ND_ISNULL);
                e.b(done);
                bind_local(e, ovf);
                emit_func_call(e, call, out, true, false);
            }
            InlineOp::Int24Div => {
                // int.c int24div: a zero divisor raises division_by_zero (the
                // generic call replays for C's exact ereport); otherwise the
                // int16 dividend (canonical sign-extended word = C's promoted
                // int32) over the int32 divisor cannot overflow int32
                // (INT16_MIN / -1 = 32768), so the quotient needs no check —
                // only the canonical sign-extension of the int4 result.
                let divzero = e.new_local();
                e.cbz_w(v1, divzero);
                e.sdiv_w(15, v0, v1);
                e.sxtw(15, 15);
                e.ldr_lit(9, out);
                e.str_x(15, 9, ND_VALUE);
                e.strb(31, 9, ND_ISNULL);
                e.b(done);
                bind_local(e, divzero);
                emit_func_call(e, call, out, true, false);
            }
        }
        e.b(done);
        bind_local(e, nullout);
        e.ldr_lit(9, out);
        e.str_x(31, 9, ND_VALUE);
        e.movz_w(10, 1);
        e.strb(10, 9, ND_ISNULL);
        bind_local(e, done);
        // Arith arms carry a call on the overflow path: registers are gone.
        cache.flush();
    }

    // Local (intra-step) label binding: rewrite pending fixups to a resolved
    // Step-space-free target by patching them immediately.
    fn bind_local(e: &mut Emitter, t: Target) {
        let here = e.code.len();
        let mut i = 0;
        while i < e.fixups.len() {
            if e.fixups[i].1 == t {
                let (pos, _) = e.fixups.remove(i);
                patch_branch(&mut e.code, pos, here);
            } else {
                i += 1;
            }
        }
    }

    fn patch_branch(code: &mut [u32], pos: usize, target: usize) {
        let delta = (target as i64 - pos as i64) as i32;
        let w = code[pos];
        code[pos] = match w >> 24 {
            0x14 => 0x1400_0000 | ((delta as u32) & 0x03FF_FFFF),
            // adr: imm21 in bytes.
            0x10 | 0x30 | 0x50 | 0x70 => {
                let byte = delta << 2;
                let immlo = (byte & 3) as u32;
                let immhi = ((byte >> 2) as u32) & 0x7FFFF;
                (w & 0x9F00_001F) | (immlo << 29) | (immhi << 5)
            }
            0xB7 | 0x37 | 0xB6 | 0x36 => (w & 0xFFF8_001F) | (((delta as u32) & 0x3FFF) << 5),
            _ => (w & 0xFF00_001F) | (((delta as u32) & 0x7FFFF) << 5),
        };
    }

    /// Emits the whole program; None = a step outside the supported set (the
    /// caller falls open to the interpreter). Also returns the hoisted head
    /// FETCHSOME bounds the driver applies per call.
    pub(super) fn emit_program(state: &ExprState<'_>) -> Option<(Vec<u32>, [u16; 3])> {
        let steps = state.steps();
        let nsteps = steps.len() as u32;
        if nsteps > 0xFFFF {
            return None;
        }
        // Coverage pre-pass: refuse before emitting anything expensive.
        for s in steps {
            if !step_stencilable(s) && !helper_supported(s) {
                return None;
            }
        }
        let mut targets = jump_targets(steps);
        // JsonExprPath jumps to RUNTIME targets (jsestate jump fields) the
        // static prepass cannot enumerate: the register cache must be empty
        // at every step of such programs.
        if steps.iter().any(|st| matches!(st, Step::JsonExprPath { .. })) {
            targets.iter_mut().for_each(|t| *t = true);
        }
        // Head FETCHSOME runs hoist into the driver (one direct call per
        // evaluation instead of a per-row helper round trip).
        let mut fetch = [0u16; 3];
        let mut hoisted = vec![false; steps.len()];
        for (ix, step) in steps.iter().enumerate() {
            if targets[ix] {
                break;
            }
            match step {
                Step::ScanFetchSome { last_var } => fetch[0] = fetch[0].max(*last_var),
                Step::InnerFetchSome { last_var } => fetch[1] = fetch[1].max(*last_var),
                Step::OuterFetchSome { last_var } => fetch[2] = fetch[2].max(*last_var),
                _ => break,
            }
            hoisted[ix] = true;
        }
        let mut e = Emitter::new();
        // Prologue: save fp/lr + x19/x20, bind ctx/base, resume dispatch.
        e.raw(0xA9BD_7BFD); // stp x29, x30, [sp, #-48]!
        e.raw(0x9100_03FD); // mov x29, sp
        e.raw(0xA901_53F3); // stp x19, x20, [sp, #16]
        e.mov_x(CTX, 0);
        e.adr(BASE, Target::Local(LOCAL_ENTRY)); // entry base (offset 0)
        e.cbz_w(1, Target::Step(0));
        e.mov_x(0, 1); // dispatch index (resume entry) into x0
        e.b(Target::Dispatch);

        let mut step_offsets = vec![0u32; steps.len()];
        let mut cache = RegCache::new();
        for (ix, step) in steps.iter().enumerate() {
            step_offsets[ix] = e.code.len() as u32;
            if hoisted[ix] {
                continue;
            }
            if targets[ix] {
                cache.flush();
            }
            emit_step(&mut e, state, &mut cache, ix as u32, step, nsteps);
        }

        // Shared blocks.
        let mut shared: Vec<(Target, u32)> = Vec::new();
        shared.push((Target::ExitDoneRet, e.code.len() as u32));
        e.movz_x(0, 0);
        e.b(Target::Local(LOCAL_EPILOGUE));
        shared.push((Target::ExitDoneNoRet, e.code.len() as u32));
        e.movz_x(0, 1);
        e.b(Target::Local(LOCAL_EPILOGUE));
        shared.push((Target::ExitErr, e.code.len() as u32));
        e.movn_x(0, 0); // -1
        e.b(Target::Local(LOCAL_EPILOGUE));
        // Suspend exit: x0 = -2.
        shared.push((Target::NegSentinel, e.code.len() as u32));
        // jitq_step returned -1 (err) or -2 (suspend): map to kernel codes,
        // which are identical, so return it as-is.
        e.b(Target::Local(LOCAL_EPILOGUE));
        // Dispatch: w0 = target step index.
        shared.push((Target::Dispatch, e.code.len() as u32));
        e.adr(9, Target::Local(LOCAL_TABLE)); // table base
        // ldr w10, [x9, w0, uxtw #2]
        e.raw(0xB860_5800 | (0 << 16) | (9 << 5) | 10);
        // add x11, x20, w10, uxtw
        e.raw(0x8B20_4000 | (10 << 16) | (BASE << 5) | 11);
        e.br(11);
        // Epilogue.
        let epilogue_pos = e.code.len();
        e.raw(0xA941_53F3); // ldp x19, x20, [sp, #16]
        e.raw(0xA8C3_7BFD); // ldp x29, x30, [sp], #48
        e.ret();

        // Label table (4-byte code offsets from entry).
        if e.code.len() % 2 != 0 {
            e.raw(0xD503_201F); // nop: 8-align the pool after the table
        }
        let table_pos = e.code.len();
        for &off in &step_offsets {
            e.raw(off * 4);
        }

        // Literal pool (8-aligned).
        if e.code.len() % 2 != 0 {
            e.raw(0xD503_201F);
        }
        let pool_pos = e.code.len();
        let lits = core::mem::take(&mut e.lits);
        for v in &lits {
            e.raw(*v as u32);
            e.raw((*v >> 32) as u32);
        }

        // Resolve literal loads.
        let lit_uses = core::mem::take(&mut e.lit_uses);
        for (pos, id) in lit_uses {
            let target = pool_pos + id as usize * 2;
            let delta = (target - pos) as u32;
            e.code[pos] |= (delta & 0x7_FFFF) << 5;
        }

        // Resolve branch fixups.
        let fixups = core::mem::take(&mut e.fixups);
        for (pos, t) in fixups {
            let target = match t {
                Target::Step(ix) => step_offsets[ix as usize] as usize,
                Target::ExitDoneRet => shared[0].1 as usize,
                Target::ExitDoneNoRet => shared[1].1 as usize,
                Target::ExitErr => shared[2].1 as usize,
                Target::NegSentinel => shared[3].1 as usize,
                Target::Dispatch => shared[4].1 as usize,
                Target::Local(LOCAL_ENTRY) => 0, // entry base adr
                Target::Local(LOCAL_EPILOGUE) => epilogue_pos,
                Target::Local(LOCAL_TABLE) => table_pos,
                Target::Local(l) => panic!("unbound local label {l}"),
            };
            patch_branch(&mut e.code, pos, target);
        }
        Some((e.code, fetch))
    }

    fn step_stencilable(step: &Step) -> bool {
        match step {
            Step::DoneReturn
            | Step::DoneNoReturn
            | Step::ScanVar { .. }
            | Step::InnerVar { .. }
            | Step::OuterVar { .. }
            | Step::Const { .. }
            | Step::CaseTestVal { .. }
            | Step::Qual { .. }
            | Step::Jump { .. }
            | Step::JumpIfNotTrue { .. }
            | Step::JumpIfNotNull { .. }
            | Step::JumpIfNull { .. }
            | Step::BoolAndStepFirst { .. }
            | Step::BoolAndStep { .. }
            | Step::BoolAndStepLast { .. }
            | Step::BoolOrStepFirst { .. }
            | Step::BoolOrStep { .. }
            | Step::BoolOrStepLast { .. }
            | Step::BoolNotStep { .. }
            | Step::NullTestIsNull { .. }
            | Step::NullTestIsNotNull { .. }
            | Step::BoolTestIsTrue { .. }
            | Step::BoolTestIsNotTrue { .. }
            | Step::BoolTestIsFalse { .. }
            | Step::BoolTestIsNotFalse { .. }
            | Step::FuncExpr { .. }
            | Step::FuncExprStrict1 { .. }
            | Step::FuncExprStrict2 { .. }
            | Step::FuncExprStrict { .. }
            | Step::FuncExprFusage { .. }
            | Step::FuncExprStrictFusage { .. } => true,
            _ => false,
        }
    }

    fn emit_step(
        e: &mut Emitter,
        state: &ExprState<'_>,
        cache: &mut RegCache,
        ix: u32,
        step: &Step,
        nsteps: u32,
    ) {
        match step {
            Step::DoneReturn => e.b(Target::ExitDoneRet),
            Step::DoneNoReturn => e.b(Target::ExitDoneNoRet),
            Step::ScanVar { attnum, out, .. }
            | Step::InnerVar { attnum, out, .. }
            | Step::OuterVar { attnum, out, .. } => {
                let (voff, noff) = slot_ctx_offsets(step).expect("var step");
                let out = out_addr(out);
                let (cv, cn) = cache.alloc(out);
                e.ldr_x(8, CTX, voff);
                e.ldr_x(9, CTX, noff);
                let a = *attnum as u32;
                e.ldr_x(cv, 8, a * 8);
                e.ldrb(cn, 9, a);
                e.ldr_lit(12, out);
                e.str_x(cv, 12, ND_VALUE);
                e.strb(cn, 12, ND_ISNULL);
            }
            Step::Const { value, isnull, out } => {
                cache.invalidate(out_addr(out));
                emit_write_out_const(e, out_addr(out), *value, *isnull);
            }
            Step::CaseTestVal { slot, out } => {
                let out = out_addr(out);
                let (cv, cn) = cache.alloc(out);
                e.ldr_lit(8, slot.as_ptr() as u64);
                e.ldr_x(cv, 8, ND_VALUE);
                e.ldrb(cn, 8, ND_ISNULL);
                e.ldr_lit(12, out);
                e.str_x(cv, 12, ND_VALUE);
                e.strb(cn, 12, ND_ISNULL);
            }
            Step::Qual { jumpdone } => {
                let fail = e.new_local();
                let next = e.new_local();
                let res = state.result_addr() as u64;
                let (v, n) = read_cell_cached(e, cache, res, 8, 10, 11);
                e.cbnz_w(n, fail);
                e.cbz_x(v, fail);
                e.b(next);
                bind_local(e, fail);
                e.ldr_lit(8, res);
                e.str_x(31, 8, ND_VALUE);
                e.strb(31, 8, ND_ISNULL);
                e.b(Target::Step(*jumpdone));
                bind_local(e, next);
            }
            Step::Jump { jumpdone } => e.b(Target::Step(*jumpdone)),
            Step::JumpIfNotTrue { jumpdone, out } => {
                let (v, n) = read_cell_cached(e, cache, out_addr(out), 8, 10, 11);
                e.cbnz_w(n, Target::Step(*jumpdone));
                e.cbz_x(v, Target::Step(*jumpdone));
            }
            Step::JumpIfNotNull { jumpdone, out } => {
                let (_, n) = read_cell_cached(e, cache, out_addr(out), 8, 10, 11);
                e.cbz_w(n, Target::Step(*jumpdone));
            }
            Step::JumpIfNull { jumpdone, out } => {
                let (_, n) = read_cell_cached(e, cache, out_addr(out), 8, 10, 11);
                e.cbnz_w(n, Target::Step(*jumpdone));
            }
            Step::BoolAndStepFirst { anynull, jumpdone, out }
            | Step::BoolAndStep { anynull, jumpdone, out } => {
                let an = anynull.as_ptr() as u64;
                if matches!(step, Step::BoolAndStepFirst { .. }) {
                    e.ldr_lit(9, an);
                    e.strb(31, 9, 0);
                }
                let next = e.new_local();
                let isnull = e.new_local();
                let (v, n) = read_cell_cached(e, cache, out_addr(out), 8, 10, 11);
                e.cbnz_w(n, isnull);
                e.cbz_x(v, Target::Step(*jumpdone));
                e.b(next);
                bind_local(e, isnull);
                e.ldr_lit(9, an);
                e.movz_w(12, 1);
                e.strb(12, 9, 0);
                bind_local(e, next);
            }
            Step::BoolAndStepLast { anynull, out } => {
                let next = e.new_local();
                let out = out_addr(out);
                cache.invalidate(out);
                emit_read_cell(e, out, 8, 10, 11);
                e.cbnz_w(11, next);
                e.cbz_x(10, next);
                e.ldr_lit(9, anynull.as_ptr() as u64);
                e.ldrb(12, 9, 0);
                e.cbz_w(12, next);
                e.str_x(31, 8, ND_VALUE);
                e.movz_w(12, 1);
                e.strb(12, 8, ND_ISNULL);
                bind_local(e, next);
            }
            Step::BoolOrStepFirst { anynull, jumpdone, out }
            | Step::BoolOrStep { anynull, jumpdone, out } => {
                let an = anynull.as_ptr() as u64;
                if matches!(step, Step::BoolOrStepFirst { .. }) {
                    e.ldr_lit(9, an);
                    e.strb(31, 9, 0);
                }
                let next = e.new_local();
                let isnull = e.new_local();
                let (v, n) = read_cell_cached(e, cache, out_addr(out), 8, 10, 11);
                e.cbnz_w(n, isnull);
                e.cbnz_x(v, Target::Step(*jumpdone));
                e.b(next);
                bind_local(e, isnull);
                e.ldr_lit(9, an);
                e.movz_w(12, 1);
                e.strb(12, 9, 0);
                bind_local(e, next);
            }
            Step::BoolOrStepLast { anynull, out } => {
                let next = e.new_local();
                let out = out_addr(out);
                cache.invalidate(out);
                emit_read_cell(e, out, 8, 10, 11);
                e.cbnz_w(11, next);
                e.cbnz_x(10, next);
                e.ldr_lit(9, anynull.as_ptr() as u64);
                e.ldrb(12, 9, 0);
                e.cbz_w(12, next);
                e.str_x(31, 8, ND_VALUE);
                e.movz_w(12, 1);
                e.strb(12, 8, ND_ISNULL);
                bind_local(e, next);
            }
            Step::BoolNotStep { out } => {
                // NULL rides through; the datum still flips (C parity).
                cache.invalidate(out_addr(out));
                e.ldr_lit(8, out_addr(out));
                e.ldr_x(10, 8, ND_VALUE);
                e.cmp_x_x(10, 31);
                e.cset_x(11, Cond::Eq);
                e.str_x(11, 8, ND_VALUE);
            }
            Step::NullTestIsNull { out } => {
                cache.invalidate(out_addr(out));
                e.ldr_lit(8, out_addr(out));
                e.ldrb(10, 8, ND_ISNULL);
                e.str_x(10, 8, ND_VALUE);
                e.strb(31, 8, ND_ISNULL);
            }
            Step::NullTestIsNotNull { out } => {
                cache.invalidate(out_addr(out));
                e.ldr_lit(8, out_addr(out));
                e.ldrb(10, 8, ND_ISNULL);
                e.cmp_w_imm(10, 0);
                e.cset_x(11, Cond::Eq);
                e.str_x(11, 8, ND_VALUE);
                e.strb(31, 8, ND_ISNULL);
            }
            Step::BoolTestIsTrue { out }
            | Step::BoolTestIsNotTrue { out }
            | Step::BoolTestIsFalse { out }
            | Step::BoolTestIsNotFalse { out } => {
                // result = f(isnull, value); all four are branchless forms.
                let a = out_addr(out);
                cache.invalidate(a);
                e.ldr_lit(8, a);
                e.ldr_x(10, 8, ND_VALUE);
                e.ldrb(11, 8, ND_ISNULL);
                // norm = (value != 0) as x12
                e.cmp_x_x(10, 31);
                e.cset_x(12, Cond::Ne);
                match step {
                    Step::BoolTestIsTrue { .. } => {
                        // !isnull && value: bic-style: x12 & (isnull==0)
                        e.cmp_w_imm(11, 0);
                        e.cset_x(13, Cond::Eq);
                        e.raw(0x8A0D_018C); // and x12, x12, x13
                    }
                    Step::BoolTestIsNotTrue { .. } => {
                        // isnull || !value
                        e.cmp_x_x(10, 31);
                        e.cset_x(12, Cond::Eq);
                        e.cmp_w_imm(11, 0);
                        e.cset_x(13, Cond::Ne);
                        e.raw(0xAA0D_018C); // orr x12, x12, x13
                    }
                    Step::BoolTestIsFalse { .. } => {
                        // !isnull && !value
                        e.cmp_x_x(10, 31);
                        e.cset_x(12, Cond::Eq);
                        e.cmp_w_imm(11, 0);
                        e.cset_x(13, Cond::Eq);
                        e.raw(0x8A0D_018C); // and x12, x12, x13
                    }
                    _ => {
                        // IsNotFalse: isnull || value
                        e.cmp_w_imm(11, 0);
                        e.cset_x(13, Cond::Ne);
                        e.raw(0xAA0D_018C); // orr x12, x12, x13
                    }
                }
                e.str_x(12, 8, ND_VALUE);
                e.strb(31, 8, ND_ISNULL);
            }
            Step::FuncExprStrict2 { call, out } => {
                // SAFETY (emit-time read): live mcx-boxed FmgrInfo.
                let oid = unsafe { call.flinfo.as_ref() }.fn_oid;
                match inline_op(oid) {
                    Some(op) => emit_inline_strict2(e, cache, call, out_addr(out), op),
                    None => {
                        emit_func_call(e, call, out_addr(out), true, false);
                        cache.flush();
                    }
                }
            }
            Step::FuncExprStrict1 { call, out } | Step::FuncExprStrict { call, out } => {
                emit_func_call(e, call, out_addr(out), true, false);
                cache.flush();
            }
            Step::FuncExpr { call, out } => {
                emit_func_call(e, call, out_addr(out), false, false);
                cache.flush();
            }
            Step::FuncExprFusage { call, out } => {
                emit_func_call(e, call, out_addr(out), false, true);
                cache.flush();
            }
            Step::FuncExprStrictFusage { call, out } => {
                emit_func_call(e, call, out_addr(out), true, true);
                cache.flush();
            }
            other => {
                debug_assert!(helper_supported(other), "emit pre-pass admitted {other:?}");
                emit_helper_call(e, ix, nsteps);
                cache.flush();
            }
        }
    }
}

#[cfg(not(target_arch = "aarch64"))]
mod emit {
    use super::*;

    pub(super) fn emit_program(_state: &ExprState<'_>) -> Option<(Vec<u32>, [u16; 3])> {
        None
    }

    pub(super) fn step_supported(_step: &Step) -> bool {
        false
    }
}
