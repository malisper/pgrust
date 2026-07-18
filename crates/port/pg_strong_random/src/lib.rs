//! `pg_strong_random` — the DST §2.3 entropy seam (P2 contract, WS-RNG).
//!
//! The [`EntropySource`] trait exists for **shape enforcement and SimEntropy
//! conformance**, not dynamic dispatch — the P1 Vfs mechanism, verbatim
//! (`vfs/src/lib.rs`, tag `vfs-trait-v1`). Product builds monomorphize
//! [`ActiveEntropy`] = `os::OsEntropy`; the sim harness selects `SimEntropy`
//! with the non-default `--cfg pgrust_sim` (set exclusively by the
//! sim-harness RUSTFLAGS — never in `.cargo/config`, product profiles, or
//! fleet submit envs). Product codegen is byte-identical to the raw
//! getentropy(2)/dev-urandom ladder by construction: `ActiveEntropy` is a
//! ZST and [`pg_strong_random`] is an `#[inline]` shim. No
//! `OnceLock<&'static dyn …>` on any entropy-fill path in product cfg
//! (contract law 0.1).
//!
//! This crate IS the sanctioned entropy funnel: its callers (SCRAM/md5
//! salts, cancel keys, uuid v4, pgcrypto, miscinit's per-backend prng seed)
//! are never lint-flagged; only the raw internals in `src/os.rs` appear in
//! the determinism ledger (`crates/_support/seams_init/tests/lint-determinism.allow`). Under
//! `pgrust_sim`, `SimEntropy` makes the per-backend prng seed deterministic,
//! which makes every `pg_prng` consumer downstream (SQL `random()`, ANALYZE
//! sampling, gist/gin tie-breaks, dsm handles) same-seed deterministic for
//! free — `pg_prng` itself is a pure xoroshiro port and is untouched.

/// Shape-enforcement + Sim-conformance trait. NOT dyn-dispatched in product
/// (contract law 0.1: the VFS mechanism, verbatim).
pub trait EntropySource {
    /// Fill `buf` with entropy. Returns `false` only on OS entropy failure;
    /// `SimEntropy` never fails.
    fn fill(&self, buf: &mut [u8]) -> bool;
}

// Under pgrust_sim the Os impl is compiled (conformance) but not dispatched.
#[cfg_attr(pgrust_sim, allow(dead_code))]
mod os;
#[cfg(pgrust_sim)]
mod sim;

#[cfg(not(pgrust_sim))]
type ActiveEntropy = os::OsEntropy;
#[cfg(pgrust_sim)]
type ActiveEntropy = sim::SimEntropy;

/// The one active instance. Both impls are ZSTs with `const fn new()`; sim
/// state lives in process-global atomics inside `sim`.
const ACTIVE: ActiveEntropy = ActiveEntropy::new();

// Conformance assert: both impls must satisfy the trait even though dispatch
// is static (the P1 Vfs contract §1.2 mechanism).
const _: () = {
    fn assert_conforms<T: EntropySource>() {}
    #[allow(dead_code)]
    fn _both() {
        assert_conforms::<os::OsEntropy>();
        #[cfg(pgrust_sim)]
        assert_conforms::<sim::SimEntropy>();
    }
};

pub fn pg_strong_random_init() {}

/// C's `pg_strong_random`. Signature unchanged from the pre-seam crate; the
/// twelve callers are untouched by the seam.
#[must_use]
#[inline]
pub fn pg_strong_random(buf: &mut [u8]) -> bool {
    ACTIVE.fill(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Conformance battery, product arm (contract §2.1 / gate §3.4): the
    // active source fills nonzero and distinct. Under pgrust_sim the same
    // two tests hold for SimEntropy (distinct via the fill counter); the
    // sim determinism battery proper lives in sim.rs on the pure stream fn.

    #[test]
    fn fills_and_varies() {
        let mut a = [0u8; 64];
        let mut b = [0u8; 64];
        assert!(pg_strong_random(&mut a));
        assert!(pg_strong_random(&mut b));
        assert_ne!(a, b);
        assert_ne!(a, [0u8; 64]);
    }

    #[test]
    fn large_request_chunks() {
        // getentropy caps a request at 256 bytes; the chunk loop (os arm) and
        // the 8-byte stream loop (sim arm) must both cover long buffers.
        let mut a = vec![0u8; 700];
        assert!(pg_strong_random(&mut a));
        assert!(a.iter().any(|&b| b != 0));
    }

    #[test]
    fn trait_object_shape_holds() {
        // The trait must stay object-safe-shaped for the wasm consumer
        // ("one indirection, two consumers") even though product dispatch is
        // monomorphized. Generic use, not dyn: proves the bound compiles.
        fn fill_via<T: EntropySource>(src: &T, buf: &mut [u8]) -> bool {
            src.fill(buf)
        }
        let mut a = [0u8; 16];
        assert!(fill_via(&ACTIVE, &mut a));
    }
}
