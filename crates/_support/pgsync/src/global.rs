//! `process_global!` — the process-global sync-state shim (LATCH-LOOM lane;
//! the pattern that makes const-init static sync state loom-modelable).
//!
//! Problem it solves: crates like `latch` keep their thread-visible state in
//! const-initialized `static`s (a slab of `Latch`es, a bump counter, a free
//! list). Under `--cfg loom` two things break: loom's primitives are not
//! const-constructible, and a plain `static` would LEAK MODEL STATE ACROSS
//! ITERATIONS (every `loom::model` iteration must start from a fresh world).
//! Historically that made such crates "not loom-buildable → mirror models
//! only" (the standing disclosure this lane retires).
//!
//! One macro, three worlds — same call-site text in all of them:
//!
//! | world  | expansion | cost |
//! |--------|-----------|------|
//! | native | plain `static NAME: T = init;` | ZERO — byte-identical asm (multiset letter in the lane worklog) |
//! | loom   | a `Deref` front over [`loom::lazy_static::Lazy`]: per-ITERATION lazy init, so every model run sees fresh state | model-only |
//! | sim    | identical to native (sim's `pgsync` types are const-constructible and statics of them are house convention — see `waiter`'s slab) | ZERO |
//!
//! Requirements on the initializer expression: it must be CONST-evaluable on
//! native/sim (it becomes a plain static initializer) and runtime-evaluable
//! under loom (it becomes the lazy init body). `AtomicUsize::new(0)`,
//! `Mutex::new(Vec::new())`, and calls to fns that are `const fn` outside
//! loom all qualify. An inline `const { .. }` block does NOT (it would force
//! const evaluation of loom types) — hoist it into a helper fn that is
//! `const fn` under `cfg(not(loom))` and plain `fn` under `cfg(loom)`
//! (see latch's `local_latch_slab()` for the worked example).
//!
//! Law update (crate doc "no statics hold loom types"): that law exists
//! because loom types are non-const AND per-iteration. This macro is the
//! sanctioned exception — the loom arm stores only a const `Lazy` cell
//! (an init fn pointer); the VALUE lives in the execution and is reset every
//! iteration, which is exactly what the law protects. Touch a shim global
//! only from inside `loom::model` (loom panics otherwise, by design).
//!
//! Under loom the access is `Deref`-mediated (`NAME.field`, `&NAME[i]`,
//! method calls all work unchanged); each first-per-iteration access runs
//! the initializer, and loom orders the lazy init like `lazy_static` does
//! (Once-equivalent Acquire/AcqRel edges).

/// Declare process-global sync state usable in all three pgsync worlds.
///
/// ```ignore
/// pgsync::process_global! {
///     /// Bump-allocation cursor for the slab.
///     static NEXT: pgsync::atomic::AtomicUsize = pgsync::atomic::AtomicUsize::new(0);
///     static REGISTRY: pgsync::Mutex<Vec<usize>> = pgsync::Mutex::new(Vec::new());
/// }
/// ```
///
/// See the module doc for the three-world contract and the initializer
/// requirements.
#[macro_export]
macro_rules! process_global {
    ($($(#[$attr:meta])* $vis:vis static $NAME:ident : $t:ty = $init:expr;)+) => {
        $(
            $crate::__process_global_one!($(#[$attr])* $vis static $NAME : $t = $init;);
        )+
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __process_global_one {
    ($(#[$attr:meta])* $vis:vis static $NAME:ident : $t:ty = $init:expr;) => {
        // Native + sim: the initializer IS the static initializer — zero
        // cost, provably identical codegen (the shim adds nothing).
        #[cfg(not(loom))]
        $(#[$attr])*
        $vis static $NAME: $t = $init;

        // Loom: same-named unit struct + Deref (the lazy_static shape) over
        // loom's per-iteration `Lazy` cell. The struct/static name pun keeps
        // call sites identical to the native arm.
        #[cfg(loom)]
        $(#[$attr])*
        #[allow(non_camel_case_types)]
        $vis struct $NAME {
            __private: (),
        }

        #[cfg(loom)]
        $vis static $NAME: $NAME = $NAME { __private: () };

        #[cfg(loom)]
        impl ::core::ops::Deref for $NAME {
            type Target = $t;

            fn deref(&self) -> &$t {
                fn __init() -> $t {
                    $init
                }
                // `Lazy`'s fields are public precisely for const-init statics
                // (loom's own lazy_static! expands to this same shape); the
                // static Lazy cell holds only the init fn pointer — the VALUE
                // is per-execution state inside loom's runtime.
                static __LAZY: $crate::__loom::lazy_static::Lazy<$t> =
                    $crate::__loom::lazy_static::Lazy {
                        init: __init,
                        _p: ::core::marker::PhantomData,
                    };
                __LAZY.get()
            }
        }
    };
}

#[cfg(all(test, not(any(loom, pgrust_sim))))]
mod tests {
    // Exercise the native arm end-to-end: plain statics, direct access.
    crate::process_global! {
        /// doc-comment attribute passthrough is part of the contract.
        static COUNTER: crate::atomic::AtomicUsize = crate::atomic::AtomicUsize::new(0);
        static REGISTRY: crate::Mutex<Vec<usize>> = crate::Mutex::new(Vec::new());
    }

    #[test]
    fn native_arm_is_a_plain_static() {
        COUNTER.fetch_add(3, crate::atomic::Ordering::Relaxed);
        assert!(COUNTER.load(crate::atomic::Ordering::Relaxed) >= 3);
        crate::lock(&REGISTRY).push(7);
        assert!(crate::lock(&REGISTRY).contains(&7));
        // &'static projection through the static works (the latch slab shape).
        let r: &'static crate::atomic::AtomicUsize = &COUNTER;
        let _ = r.load(crate::atomic::Ordering::Relaxed);
    }
}
