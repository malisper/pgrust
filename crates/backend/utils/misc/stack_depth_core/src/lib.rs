//! stack_depth_core: the recursion guard itself (C
//! src/backend/utils/misc/stack_depth.c's stack_is_too_deep /
//! check_stack_depth / set_stack_base / the max_stack_depth state).
//!
//! SPLIT OUT OF `stack_depth` (lane p1-nodes, behavior-identical): the guard
//! is needed by low-level walkers (nodes/readfuncs, nodes/outfuncs,
//! nodes/copyfuncs — C calls check_stack_depth in all three), but
//! `stack_depth`'s GUC hook/seam half depends on `guc`, which depends
//! transitively on those very crates. The guard core has no GUC dependency;
//! the GUC check-hook/assign-hook/seam half stays in `stack_depth`, which
//! re-exports everything here so existing callers are unchanged.

#![allow(non_camel_case_types)]


use std::cell::Cell;

use elog::ereport;
use types_error::{PgResult, ERRCODE_STATEMENT_TOO_COMPLEX, ERROR};

// A stack address, only ever subtracted, never dereferenced; 0 is C's NULL.
pub type pg_stack_base_t = usize;

pub const STACK_DEPTH_SLOP: isize = 512 * 1024;

/// Rust frames on the guarded recursion chains cost a multiple of their C
/// counterparts, so the same `max_stack_depth` GUC value buys far less
/// recursion than it does in C: at 2048kB, C 18.3 reaches 932 plpgsql
/// self-recursion levels vs 34 here (~27x, dev build, macOS aarch64), and
/// two driver suites (DBD::Pg `foreign_key_info`'s 9-join catalog query,
/// pgjdbc `testOidUpdatable`) hit "stack depth limit exceeded" at stock
/// config on queries C handles. The GUC keeps its C-parity value/limits
/// (SHOW, accepted range, rlimit cap are byte-identical to C); the byte
/// budget the guard *enforces* is scaled by this per-profile constant so
/// the same GUC setting buys comparable recursion DEPTH.
///
/// debug = 32: covers the measured 27x worst chain with slop.
/// release = 4: PLACEHOLDER — NOT a measurement. It MUST be calibrated
/// with `scripts/stack-calibrate.sh` against a release build before any
/// perf-claim or conformance run leans on release-mode depth behavior
/// (the 27x/1.9x figures above are dev-profile; release frames are
/// smaller but not C-sized).
pub const STACK_DEPTH_SCALE: isize = if cfg!(debug_assertions) { 32 } else { 4 };

thread_local! {
    static MAX_STACK_DEPTH: Cell<i32> = const { Cell::new(100) };
    static MAX_STACK_DEPTH_BYTES: Cell<isize> =
        const { Cell::new(100 * 1024 * STACK_DEPTH_SCALE) };
    // The scaled budget before the thread-stack ceiling clamp; kept so the
    // clamp can be (re)applied whichever of assign/ceiling happens first.
    static SCALED_STACK_BUDGET: Cell<isize> =
        const { Cell::new(100 * 1024 * STACK_DEPTH_SCALE) };
    // Enforceable ceiling derived from this thread's real stack reservation
    // (0 = unknown: no clamp). The guard must fire before the actual stack
    // ends, or recursion dies by SIGSEGV instead of SQLSTATE 54001.
    static THREAD_STACK_CEILING: Cell<isize> = const { Cell::new(0) };
    static STACK_BASE_PTR: Cell<usize> = const { Cell::new(0) };
    // 0 is C's "not yet computed" sentinel (a real rlimit is never 0).
    static STACK_DEPTH_RLIMIT_CACHE: Cell<isize> = const { Cell::new(0) };
}

pub fn max_stack_depth() -> i32 {
    MAX_STACK_DEPTH.get()
}

pub fn set_max_stack_depth(value: i32) {
    MAX_STACK_DEPTH.set(value);
}

pub fn max_stack_depth_bytes() -> isize {
    MAX_STACK_DEPTH_BYTES.get()
}

// C's __builtin_frame_address(0); inline(never) keeps the frame real; no black_box (it spills — docs/benchmarks/stack_depth.md).
#[inline(never)]
fn current_stack_addr() -> usize {
    let stack_loc: u8 = 0;
    &raw const stack_loc as usize
}

// One backend = one thread: recorded at backend-thread spawn (C: in main()).
pub fn set_stack_base() -> pg_stack_base_t {
    let addr = current_stack_addr();
    STACK_BASE_PTR.with(|c| c.replace(addr))
}

pub fn restore_stack_base(base: pg_stack_base_t) {
    STACK_BASE_PTR.set(base);
}

// C's shape: address of an own-frame local, pointer subtraction, compare.
#[inline(never)]
pub fn stack_is_too_deep() -> bool {
    let stack_top_loc: u8 = 0;
    let stack_base_ptr = STACK_BASE_PTR.get();
    let stack_depth = stack_base_ptr.abs_diff(&raw const stack_top_loc as usize) as isize;
    // base != 0 (NULL) guard last: no wasted cycles in the normal case.
    stack_depth > MAX_STACK_DEPTH_BYTES.get() && stack_base_ptr != 0
}

#[inline]
pub fn check_stack_depth() -> PgResult<()> {
    if stack_is_too_deep() {
        return Err(stack_depth_exceeded());
    }
    Ok(())
}

/// Runs `f` in its own, never-inlined stack frame.
///
/// Recursive giant-`match` dispatchers (ExecInitNode's plan-node match,
/// ExecInitExprRec's expression match) are the C recursion sites this
/// crate's guard bounds. In Rust, every arm's by-value temporaries get a
/// distinct stack slot in the DISPATCHER's frame — at opt-level 0 LLVM
/// does not merge the mutually-exclusive arm slots, so the frame is the
/// SUM over all ~40 arms (measured 380kB for exec_init_node, 84kB for
/// init_expr_rec on arm64 dev builds). Five plan levels then trip the
/// C-parity max_stack_depth=2048kB guard on plans C inits in a few kB
/// (the LD7-F1 / TGT-F5/F7/F8 family). Wrapping each arm body in this
/// trampoline moves the arm's temporaries into a per-arm callee frame,
/// so the recursion's per-level cost is one arm, not the sum of all —
/// in every build profile (`inline(never)` is release-effective).
#[inline(never)]
pub fn with_own_frame<R>(f: impl FnOnce() -> R) -> R {
    f()
}

/// Guard for recursion sites whose signatures cannot carry `PgResult`
/// (e.g. `equal()`): raises the C-identical 54001 as a `Box<PgError>` panic
/// payload, which `pg_error_from_panic` restores losslessly at the
/// statement boundary.
#[inline]
pub fn check_stack_depth_or_panic() {
    if stack_is_too_deep() {
        std::panic::panic_any(stack_depth_exceeded());
    }
}

#[cold]
#[inline(never)]
fn stack_depth_exceeded() -> Box<types_error::PgError> {
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_STATEMENT_TOO_COMPLEX)
            .errmsg("stack depth limit exceeded")
            .errhint(format!(
                "Increase the configuration parameter \"max_stack_depth\" (currently {}kB), \
                 after ensuring the platform's stack depth limit is adequate.",
                max_stack_depth()
            ))
            .into_error(),
    )
}

// C InitializeGUCOptionsFromEnvironment's stack-rlimit branch (guc.c): the
// boot default is 100kB; a usable platform limit raises it to
// min((rlimit - slop)/1024, 2048) kB, as PGC_S_ENV_VAR so conf/argv override.
//
// The enforced budget is the GUC's bytes x STACK_DEPTH_SCALE (see the
// constant's comment), clamped to the thread's real stack when known.
pub fn assign_max_stack_depth(newval: i32) {
    let scaled = (newval as isize)
        .saturating_mul(1024)
        .saturating_mul(STACK_DEPTH_SCALE);
    SCALED_STACK_BUDGET.set(scaled);
    MAX_STACK_DEPTH_BYTES.set(effective_budget(scaled));
}

fn effective_budget(scaled: isize) -> isize {
    let ceiling = THREAD_STACK_CEILING.get();
    if ceiling > 0 { scaled.min(ceiling) } else { scaled }
}

/// The scaled budget BEFORE the per-thread ceiling clamp — what thread
/// provisioning must accommodate (launch_backend sizes backend stacks from
/// this, plus slop).
pub fn scaled_max_stack_depth_bytes() -> isize {
    SCALED_STACK_BUDGET.get()
}

/// Test/calibration hook: set the ENFORCED byte budget directly, bypassing
/// STACK_DEPTH_SCALE and the thread-stack ceiling. Guard-witness tests pin
/// the 54001 mechanism at exact byte budgets (readfuncs/tsquery deep-nesting
/// witnesses); production code never calls this — the GUC assign hook is the
/// only production writer.
pub fn set_enforced_stack_budget_for_tests(bytes: isize) {
    SCALED_STACK_BUDGET.set(bytes);
    THREAD_STACK_CEILING.set(0);
    MAX_STACK_DEPTH_BYTES.set(bytes);
}

/// Record this thread's real stack reservation so the guard always fires
/// before the stack actually ends. Call once at thread start, next to
/// set_stack_base(). Keeps `min(8MiB, size/2)` in reserve below the
/// reservation for the frames past the last check (elog machinery etc.).
pub fn set_thread_stack_ceiling(stack_size: usize) {
    let size = stack_size.min(isize::MAX as usize) as isize;
    let reserve = (size / 2).min(8 << 20);
    let ceiling = (size - reserve).max(0);
    THREAD_STACK_CEILING.set(ceiling);
    MAX_STACK_DEPTH_BYTES.set(effective_budget(SCALED_STACK_BUDGET.get()));
}

// Platform stack limit in bytes, -1 if unknown; cached after first call.
pub fn get_stack_depth_rlimit() -> isize {
    // Miri has no getrlimit; -1 is C's "limit unknown" (accept any value).
    // wasm32: WASI has no rlimits either — the same C no-getrlimit arm.
    #[cfg(any(miri, target_family = "wasm"))]
    return -1;
    #[cfg(not(any(miri, target_family = "wasm")))]
    {
        let cached = STACK_DEPTH_RLIMIT_CACHE.get();
        if cached != 0 {
            return cached;
        }

        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit writes into the provided rlimit struct.
        let val = if unsafe { libc::getrlimit(libc::RLIMIT_STACK, &mut rlim) } < 0 {
            -1
        } else if rlim.rlim_cur == libc::RLIM_INFINITY {
            isize::MAX
        } else if rlim.rlim_cur >= isize::MAX as libc::rlim_t {
            isize::MAX
        } else {
            rlim.rlim_cur as isize
        };

        STACK_DEPTH_RLIMIT_CACHE.set(val);
        val
    }
}
