//! Kani C≡Rust equivalence: bytea hex codec (encode.c hex lane).
//! Rust side: shipped crates/backend/utils/adt/varlena/src/bytea.rs
//! (hex_encode_into / hex_decode_into, incl. the private get_hex via its
//! only caller). C side: vendored REL_15_STABLE encode.c (csrc/hex_shim.c).
//!
//! Adjudicates the bytea.rs:29 note: Rust widened HEXLOOKUP to 256 entries
//! to drop C's `c < 127` guard — the decode harness covers all 256 byte
//! values, so any behavioral difference in 127..=255 would be found.
//!
//! Harness scaffolding (not part of the claim):
//! - proof_support's mcx stub set replaces the arena: `Mcx::allocate` →
//!   static-buffer bump (`mcx_stubs::stub_mcx_allocate`), `std::env::var` →
//!   "0" (pool stripe OFF: the TLS-pool arm registers thread-local
//!   destructors via `_tlv_atexit`, a Kani-unsupported foreign fn), and
//!   `OnceLock::get_or_init` → recompute (std's queue Once reaches
//!   `ptr_mask` via thread parking). The harnesses end in mem::forget of
//!   the vec + context so teardown machinery (AcctWeak registry drops)
//!   stays out of symex. SOUNDNESS: the allocation strategy, config
//!   memoization, and teardown are not part of the equivalence claim — the
//!   shipped hex logic's writes/reads/lengths/verdicts all stay in the
//!   theorem. Ledger wording: "modulo static-buffer allocator model".
//! - The decode reject path additionally stubs the error-MESSAGE plumbing
//!   (PgError::error / format! / from_utf8_lossy — proof_support canonical
//!   stubs): message text leaves the proof, the accept/reject verdict
//!   stays in (C's ereport is likewise shimmed to a sentinel).
//! - mbutils_seams::pg_mblen_range is installed as a stub returning Ok(1) —
//!   it only feeds the error MESSAGE text (mbchar length).
//!
//! Run recipe (measured 2026-07-28, Kani 0.67.0, HEAVILY shared laptop —
//! load avg 14-19 from concurrent proof agents; re-time on a quiet box):
//!   timeout 600 cargo kani -Z c-ffi -Z stubbing --c-lib csrc/hex_shim.c \
//!       --solver kissat --harness <h>
//!   hex_encode_equiv: PROVED, 89.5s solve — release-gate tier (>30s).
//!   hex_decode_equiv (len 4): symex COMPLETES (~9.7k VCCs post-slice) but
//!     the SAT solve walls >580s under load on BOTH kissat and the default
//!     solver, with unwind 12 and 6 — post-symex SAT wall, no longer the
//!     arena symex wall. hex_decode_equiv_len2: same status at 900s under
//!     load; UNRESOLVED pending a quiet-box run.
//!   control_hex_encode_mismatch_must_fail: DEFAULT solver; FAILED as
//!     required in 102s on exactly the mismatch assert — the rig incl. the
//!     mcx stub set is live (a passing control is a broken gate).

#[cfg(kani)]
mod proofs {
    use proof_support::{mcx_stubs, stubs};

    extern "C" {
        fn pgc_hex_encode(src: *const u8, len: u64, dst: *mut u8) -> u64;
        fn pgc_hex_decode(src: *const u8, len: u64, dst: *mut u8) -> i64;
    }

    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    fn hex_encode_equiv() {
        const N: usize = 4;
        let src: [u8; N] = kani::any();

        let ctx = mcx::MemoryContext::new_bump("kani-hex");
        let mut out = mcx::vec_with_capacity_in::<u8>(ctx.mcx(), 2 * N).unwrap();
        varlena::bytea::hex_encode_into(&src, &mut out);

        let mut cbuf = [0u8; 2 * N];
        let clen = unsafe { pgc_hex_encode(src.as_ptr(), N as u64, cbuf.as_mut_ptr()) };

        assert!(out.len() as u64 == clen);
        for i in 0..2 * N {
            assert!(out[i] == cbuf[i]);
        }
        // Harness scaffolding: skip the context/vec DROP machinery (acct
        // registry, pooled child vecs) — teardown is not part of the claim.
        core::mem::forget(out);
        core::mem::forget(ctx);
    }

    fn mblen_one(_s: &[u8]) -> types_error::PgResult<i32> {
        Ok(1)
    }

    #[kani::proof]
    #[kani::unwind(6)] // decode loop <= N+1 iterations; slack unwind is catastrophic (TRIAGE)
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
    fn hex_decode_equiv() {
        check_decode::<4>();
    }

    /// Shared decode driver: all 256^N inputs, accept + both reject paths.
    fn check_decode<const N: usize>() {
        let src: [u8; N] = kani::any();

        mbutils_seams::pg_mblen_range::set(mblen_one);

        let ctx = mcx::MemoryContext::new_bump("kani-hex");
        let mut out = mcx::vec_with_capacity_in::<u8>(ctx.mcx(), N).unwrap();
        let rust = varlena::bytea::hex_decode_into(&src, None, &mut out);

        let mut cbuf = [0u8; N];
        let c = unsafe { pgc_hex_decode(src.as_ptr(), N as u64, cbuf.as_mut_ptr()) };

        match rust {
            Ok(Some(())) => {
                // accepted: C must accept with the same bytes
                assert!(c >= 0, "Rust accepted, C rejected");
                assert!(out.len() as i64 == c);
                for i in 0..out.len() {
                    assert!(out[i] == cbuf[i]);
                }
            }
            Ok(None) => unreachable!("escontext is None"),
            Err(_) => {
                // rejected: C must reject too (kind not distinguished:
                // both C paths carry ERRCODE_INVALID_PARAMETER_VALUE)
                assert!(c < 0, "Rust rejected, C accepted");
            }
        }
        // Harness scaffolding: skip drop machinery (see hex_encode_equiv).
        core::mem::forget(out);
        core::mem::forget(ctx);
    }
    /// len-2 slice of the decode domain (per-length ladder step for the
    /// len-4 SAT wall; both still unresolved under shared-box load — see
    /// module doc). Same stub set, same claim shape.
    #[kani::proof]
    #[kani::unwind(4)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
    fn hex_decode_equiv_len2() {
        check_decode::<2>();
    }

    /// MUST FAIL: C and Rust get deliberately different inputs — proves the
    /// rig (incl. the mcx stub set) is non-vacuous. DEFAULT solver only
    /// (kissat never terminates on failing harnesses).
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    fn control_hex_encode_mismatch_must_fail() {
        const N: usize = 4;
        let src: [u8; N] = kani::any();
        let mut skewed = src;
        skewed[0] ^= 0x01;

        let ctx = mcx::MemoryContext::new_bump("kani-hex-ctl");
        let mut out = mcx::vec_with_capacity_in::<u8>(ctx.mcx(), 2 * N).unwrap();
        varlena::bytea::hex_encode_into(&skewed, &mut out);

        let mut cbuf = [0u8; 2 * N];
        unsafe { pgc_hex_encode(src.as_ptr(), N as u64, cbuf.as_mut_ptr()) };

        let mut same = true;
        for i in 0..2 * N {
            same = same && out[i] == cbuf[i];
        }
        assert!(same, "expected failure: control mismatch detected (rig is live)");
        core::mem::forget(out);
        core::mem::forget(ctx);
    }

}
