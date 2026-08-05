//! Kani C≡Rust equivalence: BRIN bloom opclass summary type I/O
//! (w2-brin lane 2026-07-30; ledger rows 4596 summary_in, 4598
//! summary_recv, 4599 summary_send).
//!
//! Rust side (shipped, path-dep — never copied):
//! crates/backend/access/brin/brin_bloom/src/builtins.rs
//! fc_summary_{in,recv,send}, reached WRAPPER-LEVEL through the public
//! BLOOM_BUILTINS registration table via a compile-time oid lookup — the
//! oid -> function wiring of the dispatch table is pinned by every harness.
//!
//! C side: c/pg_brin_bloom.c — verbatim REL_18_STABLE brin_bloom.c bodies
//! (see its header for every shim).
//!
//! Claims (exactly what each harness covers, never more):
//!  - eq_summary_in / eq_summary_recv (4596/4598): error-class-only —
//!    verdict (always-Err) + level (ERROR) + sqlstate (0A000 via the
//!    verbatim MAKE_SQLSTATE encoder, bit-compared against SqlState(i32)).
//!    PgError constructor stubbed field-identically; message text out of
//!    proof (value-space only). Mirrors proofs/brin-minmax
//!    dist_proofs::eq_summary_{in,recv} (proved precedent).
//!  - eq_summary_send_l0/_l8 (4599): the shipped body delegates to
//!    fc_byteasend exactly as C's body is `return byteasend(fcinfo);` —
//!    result payload byte-parity + varlena size parity at literal-length
//!    cells (derived-length-copy law; bytea-cmp eq_byteasend_l0/l8
//!    precedent), fully symbolic content, modulo static-buffer allocator
//!    model.
//!  - control_summary_send_lastbyte: MUST FAIL (DEFAULT solver) — C-side
//!    last-byte flip; rig non-vacuity for the family.
//!
//! Run (from proofs/brin-bloom/):
//!   timeout 450 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_brin_bloom.c \
//!       --harness proofs::<h> --exact [--solver kissat]
//! DEFAULT solver for the send cells (bytea-cmp precedent) and the control
//! (kissat never terminates on failing harnesses).

#[cfg(kani)]
mod proofs {
    use datum::Datum;
    use proof_support::{call, mcx_stubs, stubs};
    use std::os::raw::c_int;
    use types_fmgr::{LocalFcinfo, PGFunction, PackedVarlena};

    extern "C" {
        fn pg_bloom_summary_in() -> c_int;
        fn pg_bloom_summary_recv() -> c_int;
        fn pg_bloom_errcode_get() -> i32;
        fn pg_bloom_summary_send(d: *const u8, len: c_int, out: *mut u8) -> c_int;
        // negative control only — NOT postgres code
        fn pg_bloom_summary_send_wrong(d: *const u8, len: c_int, out: *mut u8) -> c_int;
    }

    /// Compile-time oid -> shipped fc_* lookup through the PUBLIC
    /// registration table (brin-minmax dist lane pattern).
    const fn builtin(oid: u32) -> PGFunction {
        let t = brin_bloom::BLOOM_BUILTINS;
        let mut i = 0;
        while i < t.len() {
            if t[i].foid == oid {
                return t[i].func;
            }
            i += 1;
        }
        panic!("oid not registered in BLOOM_BUILTINS")
    }

    const FC_SUMMARY_IN: PGFunction = builtin(4596);
    const FC_SUMMARY_RECV: PGFunction = builtin(4598);
    const FC_SUMMARY_SEND: PGFunction = builtin(4599);

    fn ok(r: Result<Datum, Box<types_error::PgError>>) -> Datum {
        match r {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("summary_send wrapper errored")
            }
        }
    }

    // ---------- summary_in / summary_recv (error-class-only) ----------

    /// Verdict + sqlstate + level parity: shipped .with_sqlstate stays
    /// load-bearing over the stubbed constructor; C's MAKE_SQLSTATE int is
    /// bit-compatible with pgrust's SqlState(i32) (same sixbit encoder,
    /// machine-checked here).
    macro_rules! summary_reject_harness {
        ($h:ident, $fc:ident, $pg:ident) => {
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let c = unsafe { $pg() };
                let c_code = unsafe { pg_bloom_errcode_get() };
                let r = call::<1, _>($fc, [Datum::from_usize(0)]);
                match r {
                    Ok(_) => panic!("summary reject wrapper returned Ok"),
                    Err(e) => {
                        assert!(c == 1);
                        assert!(e.level == types_error::ERROR);
                        assert!(e.sqlstate.0 == c_code);
                        core::mem::forget(e);
                    }
                }
            }
        };
    }

    summary_reject_harness!(eq_summary_in, FC_SUMMARY_IN, pg_bloom_summary_in);
    summary_reject_harness!(eq_summary_recv, FC_SUMMARY_RECV, pg_bloom_summary_recv);

    // ---------- summary_send (byteasend delegation, literal-len cells) ----

    /// 4B-uncompressed varlena input image (literal header prunes the
    /// detoast-copy seam of arg_varlena_packed; unused bytes literal zero).
    #[repr(C, align(8))]
    struct ByteaImg([u8; 12]);

    macro_rules! send_cell {
        ($h:ident, $w:literal, $cfn:ident) => {
            #[kani::proof]
            #[kani::unwind(14)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let payload: [u8; 8] = kani::any();
                let len: usize = $w; // LITERAL cell (derived-length-copy law)

                let mut img = ByteaImg([0u8; 12]);
                img.0[0..4].copy_from_slice(&((((4 + len) as u32) << 2).to_ne_bytes()));
                let mut i = 0;
                while i < len {
                    img.0[4 + i] = payload[i]; // symbolic content
                    i += 1;
                }

                let mut cimg = [0u8; 8];
                unsafe { $cfn(payload.as_ptr(), len as c_int, cimg.as_mut_ptr()) };

                let ctx = mcx::MemoryContext::new_bump("kani-bloom-send");
                let mut fci = LocalFcinfo::<1>::new(0);
                // SAFETY: ctx outlives the call (both forgotten at end).
                unsafe { fci.set_result_mcx(ctx.mcx()) };
                fci.set_arg(0, Datum::from_usize(img.0.as_ptr() as usize));
                let d = ok(FC_SUMMARY_SEND(None, &mut fci));

                // SAFETY: byteasend returns an inline bytea image.
                let v = unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) };
                let out = v.data();
                assert!(out.len() == len);
                let mut i = 0;
                while i < len {
                    assert!(out[i] == cimg[i]);
                    i += 1;
                }
                core::mem::forget(fci);
                core::mem::forget(ctx);
            }
        };
    }

    send_cell!(eq_summary_send_l0, 0, pg_bloom_summary_send);
    send_cell!(eq_summary_send_l8, 8, pg_bloom_summary_send);

    // negative control (MUST FAIL, DEFAULT solver): C flips the last
    // payload byte — decodable counterexample at any payload.
    send_cell!(control_summary_send_lastbyte, 8, pg_bloom_summary_send_wrong);
}
