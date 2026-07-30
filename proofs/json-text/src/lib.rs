//! Kani C≡Rust equivalence: TEXT-json (json.c) scalar builtins — oids
//! 322 json_out, 324 json_send, 3199 json_build_array_noargs,
//! 3201 json_build_object_noargs. (json_typeof 3968 lives in its own
//! module once the lexer vendoring lands; the jsonb family is a SIBLING
//! lane — nothing binary-json here.)
//!
//! Rust side (shipped code, path-dep — never copied):
//!  - adt_json::json_out (lib.rs:70, = varlena::text_to_cstring)
//!  - adt_json::json_send (lib.rs:88, pq_begintypsend + pq_sendtext +
//!    pq_endtypsend; the pg_server_to_client SEAM is installed as
//!    Ok(None) = identity, the same no-conversion branch the C shim
//!    takes — seam skew control below proves the model is load-bearing)
//!  - adt_json::builtins::{fc_json_build_array_noargs,
//!    fc_json_build_object_noargs} — full wrapper level (LocalFcinfo
//!    frame; the Datum pack and cstring_to_text are in-theorem).
//!
//! C side: proofs/json-text/c/pg_json_text.c (REL_18_STABLE json.c /
//! varlena.c / pqformat.c, provenance + shims documented there).
//!
//! Plane and fences (recorded per ledger row):
//!  - json payloads ride PRE-DETOASTED as (ptr, len) — the
//!    post-PG_GETARG_TEXT_PP contract (bytea-cmp precedent). Symbolic
//!    contents, symbolic len <= CAP_IN (8).
//!  - json is validated text: json_out/json_send never parse, so the
//!    payload needs no well-formedness fence — the claims hold for ALL
//!    byte contents (superset of valid json), asserted as such.
//!  - Allocator: mcx-stubs recipe ("modulo static-buffer allocator
//!    model"); no error text in any of these paths.
//!  - Result images are byte-compared IN FULL (output length is
//!    input-length + constant — offsets are not data-dependent, so the
//!    result-image-wall law does not bite; bytea SetByte precedent).
//!
//! Run (one at a time, RSS watchdog):
//!   timeout 450 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_json_text.c \
//!     --harness <h> --exact --solver kissat

use std::os::raw::c_int;

extern "C" {
    fn pg_json_out(vardata: *const u8, len: c_int, result: *mut u8) -> c_int;
    fn pg_json_send(vardata: *const u8, len: c_int, out: *mut u8) -> c_int;
    fn pg_json_build_object_noargs(out: *mut u8) -> c_int;
    fn pg_json_build_array_noargs(out: *mut u8) -> c_int;
}

/// symbolic payload cap (bytes); output caps = CAP_IN + 5 worst case.
pub const CAP_IN: usize = 8;

#[cfg(kani)]
mod proofs {
    use super::*;
    use proof_support::{mcx_stubs, stubs};

    /// pg_server_to_client seam model: identity (no conversion), the
    /// branch C takes when client_encoding == server_encoding — exactly
    /// shim 3 in the C file. Skew control below proves it load-bearing.
    fn seam_identity<'m>(
        _mcx: mcx::Mcx<'m>,
        _s: &[u8],
    ) -> types_error::PgResult<Option<mcx::PgVec<'m, u8>>> {
        Ok(None)
    }

    macro_rules! recipe {
        ($(#[$m:meta])* fn $name:ident() $body:block) => {
            #[kani::proof]
            #[kani::unwind(14)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            $(#[$m])*
            fn $name() $body
        };
    }

    /// oid 322 json_out cells — LITERAL length per cell (symbolic length
    /// puts the NUL at a data-dependent offset and walled SAT at 450s;
    /// literals prune — TRIAGE law), symbolic contents.
    macro_rules! json_out_cell {
        ($name:ident, $n:literal) => {
            recipe! {
                fn $name() {
                    let buf: [u8; CAP_IN] = kani::any();
                    let len: usize = $n;
                    let mut c_out = [0u8; CAP_IN + 1];
                    let c_len = unsafe {
                        pg_json_out(buf.as_ptr(), len as c_int, c_out.as_mut_ptr())
                    } as usize;
                    let ctx = mcx::MemoryContext::new_bump("kani-json");
                    match adt_json::json_out(ctx.mcx(), &buf[..len]) {
                        Ok(v) => {
                            // shipped json_out returns the cstring INCLUDING its NUL
                            assert!(v.len() == c_len + 1);
                            let mut i = 0;
                            while i <= c_len {
                                assert!(v[i] == c_out[i]);
                                i += 1;
                            }
                            core::mem::forget(v);
                        }
                        Err(e) => {
                            core::mem::forget(e);
                            panic!("json_out errored");
                        }
                    }
                    core::mem::forget(ctx);
                }
            }
        };
    }
    json_out_cell!(eq_json_out_len0, 0);
    json_out_cell!(eq_json_out_len1, 1);
    json_out_cell!(eq_json_out_len8, 8);

    /// oid 324 json_send cells — LITERAL length per cell (same SAT-wall
    /// remedy as json_out), symbolic contents; identity conversion seam.
    macro_rules! json_send_cell {
        ($name:ident, $n:literal) => {
            recipe! {
                fn $name() {
                    detoast_install_conversion();
                    let buf: [u8; CAP_IN] = kani::any();
                    let len: usize = $n;
                    let mut c_out = [0u8; CAP_IN + 4];
                    let c_total = unsafe {
                        pg_json_send(buf.as_ptr(), len as c_int, c_out.as_mut_ptr())
                    } as usize;
                    let ctx = mcx::MemoryContext::new_bump("kani-json");
                    match adt_json::json_send(ctx.mcx(), &buf[..len]) {
                        Ok(b) => {
                            assert!(b.varsize() == c_total);
                            let d = b.data();
                            assert!(d.len() == c_total - 4);
                            let img = b.as_bytes();
                            let mut i = 0;
                            while i < c_total {
                                assert!(img[i] == c_out[i]);
                                i += 1;
                            }
                            core::mem::forget(b);
                        }
                        Err(e) => {
                            core::mem::forget(e);
                            panic!("json_send errored");
                        }
                    }
                    core::mem::forget(ctx);
                }
            }
        };
    }
    json_send_cell!(eq_json_send_len0, 0);
    json_send_cell!(eq_json_send_len1, 1);
    json_send_cell!(eq_json_send_len8, 8);

    fn detoast_install_conversion() {
        mbutils_seams::pg_server_to_client::set(seam_identity);
    }

    recipe! {
        /// NEGATIVE CONTROL (gate non-vacuity): C is fed a 2-byte payload,
        /// shipped Rust a 1-byte payload — the size assert MUST FAIL (run
        /// with the DEFAULT solver). Single failable property; the Err arm
        /// is forget-only (its unreachability is attested by the PROVED
        /// eq_json_send cells, whose Err-arm panics verified unreachable).
        ///
        /// NOTE: a first-cut control skewed the pg_server_to_client SEAM
        /// instead (prepend one byte); its extra PgVec allocation EXHAUSTS
        /// the 2 KiB tiny proof heap, so the harness failed on the Err-arm
        /// alloc panic, not the divergence — wrong-reason gate (integrity
        /// rule). The seam model's load-bearing-ness is demonstrated
        /// natively instead: tests/native_seam_skew.rs (separate process,
        /// real allocator).
        fn control_json_send_input_skew_must_fail() {
            detoast_install_conversion();
            let buf = [b'1', b'2'];
            let mut c_out = [0u8; CAP_IN + 4];
            let c_total =
                unsafe { pg_json_send(buf.as_ptr(), 2, c_out.as_mut_ptr()) } as usize;
            let ctx = mcx::MemoryContext::new_bump("kani-json");
            match adt_json::json_send(ctx.mcx(), &buf[..1]) {
                Ok(b) => {
                    assert!(b.varsize() == c_total); // skew: sizes must diverge
                    core::mem::forget(b);
                }
                Err(e) => {
                    core::mem::forget(e);
                }
            }
            core::mem::forget(ctx);
        }
    }

    recipe! {
        /// oid 3201 json_build_object_noargs — wrapper level; text image
        /// "{}" with header, via the real fcinfo frame.
        fn eq_json_build_object_noargs() {
            let mut c_out = [0u8; 8];
            let c_total = unsafe { pg_json_build_object_noargs(c_out.as_mut_ptr()) } as usize;
            let ctx = mcx::MemoryContext::new_bump("kani-json");
            let mut f = proof_support::fcinfo::fci::<0>([]);
            // SAFETY(harness): ctx outlives the call; forgotten below.
            unsafe { f.set_result_mcx(ctx.mcx()) };
            match adt_json::builtins::fc_json_build_object_noargs(None, &mut f) {
                Ok(d) => {
                    assert!(!f.isnull);
                    let p = d.as_usize() as *const u8;
                    // SAFETY(harness): d is a live text varlena in the ctx.
                    let img = unsafe { core::slice::from_raw_parts(p, c_total) };
                    let mut i = 0;
                    while i < c_total {
                        assert!(img[i] == c_out[i]);
                        i += 1;
                    }
                }
                Err(e) => {
                    core::mem::forget(e);
                    panic!("fc_json_build_object_noargs errored");
                }
            }
            core::mem::forget(ctx);
        }
    }

    recipe! {
        /// oid 3199 json_build_array_noargs — wrapper level; text image
        /// "[]" with header.
        fn eq_json_build_array_noargs() {
            let mut c_out = [0u8; 8];
            let c_total = unsafe { pg_json_build_array_noargs(c_out.as_mut_ptr()) } as usize;
            let ctx = mcx::MemoryContext::new_bump("kani-json");
            let mut f = proof_support::fcinfo::fci::<0>([]);
            // SAFETY(harness): ctx outlives the call; forgotten below.
            unsafe { f.set_result_mcx(ctx.mcx()) };
            match adt_json::builtins::fc_json_build_array_noargs(None, &mut f) {
                Ok(d) => {
                    assert!(!f.isnull);
                    let p = d.as_usize() as *const u8;
                    // SAFETY(harness): d is a live text varlena in the ctx.
                    let img = unsafe { core::slice::from_raw_parts(p, c_total) };
                    let mut i = 0;
                    while i < c_total {
                        assert!(img[i] == c_out[i]);
                        i += 1;
                    }
                }
                Err(e) => {
                    core::mem::forget(e);
                    panic!("fc_json_build_array_noargs errored");
                }
            }
            core::mem::forget(ctx);
        }
    }
}
