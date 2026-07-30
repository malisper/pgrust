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

    fn sym_payload() -> ([u8; CAP_IN], usize) {
        let buf: [u8; CAP_IN] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= CAP_IN);
        (buf, len)
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

    recipe! {
        /// oid 322 json_out — output cstring == payload + NUL; symbolic
        /// contents, symbolic len <= 8.
        fn eq_json_out() {
            let (buf, len) = sym_payload();
            let mut c_out = [0u8; CAP_IN + 1];
            let c_len =
                unsafe { pg_json_out(buf.as_ptr(), len as c_int, c_out.as_mut_ptr()) } as usize;
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

    recipe! {
        /// oid 324 json_send — bytea image == 4B LE header + payload;
        /// symbolic contents, symbolic len <= 8; identity conversion seam.
        fn eq_json_send() {
            detoast_install_conversion();
            let (buf, len) = sym_payload();
            let mut c_out = [0u8; CAP_IN + 4];
            let c_total =
                unsafe { pg_json_send(buf.as_ptr(), len as c_int, c_out.as_mut_ptr()) } as usize;
            let ctx = mcx::MemoryContext::new_bump("kani-json");
            match adt_json::json_send(ctx.mcx(), &buf[..len]) {
                Ok(b) => {
                    assert!(b.varsize() == c_total);
                    let d = b.data();
                    assert!(d.len() == c_total - 4);
                    // full image compare: header word ...
                    let hdr = (c_total as u32) << 2;
                    let hb = hdr.to_le_bytes();
                    let img = b.as_bytes();
                    let mut i = 0;
                    while i < 4 {
                        assert!(img[i] == hb[i] && img[i] == c_out[i]);
                        i += 1;
                    }
                    // ... then payload bytes
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

    fn detoast_install_conversion() {
        mbutils_seams::pg_server_to_client::set(seam_identity);
    }

    /// non-identity seam impl for the skew control: prepends one byte.
    fn seam_skew<'m>(
        mcx: mcx::Mcx<'m>,
        s: &[u8],
    ) -> types_error::PgResult<Option<mcx::PgVec<'m, u8>>> {
        let mut v = mcx::vec_with_capacity_in(mcx, s.len() + 1)?;
        mcx::vec_append_bytes(&mut v, b"X")?;
        mcx::vec_append_bytes(&mut v, s)?;
        Ok(Some(v))
    }

    recipe! {
        /// NEGATIVE CONTROL (seam model is load-bearing + gate
        /// non-vacuity): a skewed conversion seam MUST make the image
        /// compare fail. Run with the DEFAULT solver.
        fn control_json_send_seam_skew_must_fail() {
            mbutils_seams::pg_server_to_client::set(seam_skew);
            let buf = [b'1'; 1];
            let mut c_out = [0u8; CAP_IN + 4];
            let c_total =
                unsafe { pg_json_send(buf.as_ptr(), 1, c_out.as_mut_ptr()) } as usize;
            let ctx = mcx::MemoryContext::new_bump("kani-json");
            match adt_json::json_send(ctx.mcx(), &buf[..1]) {
                Ok(b) => {
                    assert!(b.varsize() == c_total, "seam skew: sizes must diverge");
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
