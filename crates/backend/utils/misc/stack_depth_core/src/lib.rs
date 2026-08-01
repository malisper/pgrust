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

thread_local! {
    static MAX_STACK_DEPTH: Cell<i32> = const { Cell::new(100) };
    static MAX_STACK_DEPTH_BYTES: Cell<isize> = const { Cell::new(100 * 1024) };
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
pub fn assign_max_stack_depth(newval: i32) {
    MAX_STACK_DEPTH_BYTES.set(newval as isize * 1024);
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
