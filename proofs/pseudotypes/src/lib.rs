//! Kani C≡Rust equivalence: the pseudotype I/O family (pseudotypes.c —
//! 51 pg_proc rows: 43 ereport-only error stubs, void_in/out/recv/send,
//! cstring_in/cstring_out, pg_node_tree_out/pg_node_tree_send).
//!
//! Error-stub rows (43): the harness calls the SHIPPED fmgr wrapper
//! (`adt_pseudotypes::builtins::fc_*`) through a real `LocalFcinfo<1>` frame
//! (proof_support::call1; the wrappers ignore their args, so a null Datum
//! stands for any argument — the strict-null fmgr-core gate is above the
//! per-function proof tier). Assertions per row, against the vendored C
//! (c/pg_pseudotypes.c, ereport rewired to (level, sqlstate) out-params):
//!   * verdict parity: Rust Err ⇔ C ereport path (both sides are
//!     unconditional, and the Ok arm asserts false so a Rust accept would
//!     fail the proof);
//!   * sqlstate parity: `e.sqlstate.0 == c_sqlstate` — the C side COMPUTES
//!     elog.h's MAKE_SQLSTATE('0','A','0','0','0') and the Rust side is the
//!     shipped `.with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)` value, so the
//!     six-bit-encoding transcription is inside the theorem;
//!   * level parity: `e.level.0 == c_level` (elog.h ERROR = 21 both sides).
//! Stub scaffolding: `PgError::error` → proof_support's field-identical
//! constructor (Location::caller is Kani-unsupported) and `format!` →
//! empty String. Message TEXT and error location leave the proof
//! (value-space + verdict + sqlstate + level stay in). The Err box is
//! mem::forget-ed after the asserts (Box<PgError> drop glue walls symex —
//! TRIAGE error-drop trap); teardown is not part of the claim.
//!
//! Value rows (8):
//!   * void_in / void_recv: shipped fc wrappers vs C `(Datum) 0`.
//!   * void_out: shipped fc wrapper's static empty cstring, byte-compared
//!     against C pstrdup("") (plus the core void_out(mcx) lane).
//!   * cstring_in / cstring_out / void_send / pg_node_tree_out /
//!     pg_node_tree_send: the shipped CORE fns (the fc wrappers need a live
//!     fn_extra scratch / result_mcx frame — they stay in the tested tier),
//!     under proof_support's mcx static-buffer stub set with feature
//!     tiny-proof-heap (the 64 KiB default's bit-blasted backing array
//!     walled these on MEMORY at >6 GiB RSS); ledger wording "modulo
//!     static-buffer allocator model".
//!     - cstring rows: concrete SPOT proofs (empty / len 1 / len 3 at cap 4,
//!       garbage after the NUL) — every symbolic-content shape walls on
//!       memory; census at the cstring_spot macro.
//!     - pg_node_tree rows: payload content fully symbolic, LENGTH
//!       case-split per value 0..=8 with exact per-cell unwind (slack
//!       walled cells) + cover_node_tree_len_partition union witness.
//!     pg_node_tree_send pins the pg_server_to_client seam to its identity
//!     arm (ClientEncoding == ServerEncoding), same arm as vendored C —
//!     encoding CONVERSION is out of scope, byte transmission is in.
//!
//! Negative control: control_shell_in_wrong_sqlstate asserts the shipped
//! shell_in error carries ERRCODE_INTERNAL_ERROR — MUST FAIL, proving the
//! Err arm is reached, the stub does not swallow the shipped
//! with_sqlstate, and the sqlstate assertion bites. DEFAULT solver for the
//! control (kissat never terminates on failing harnesses).
//!
//! Run recipe (error stubs + void_in/out/recv + control):
//!   cd proofs/pseudotypes
//!   timeout 30 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_pseudotypes.c \
//!       --harness proofs::<h> --exact
//! mcx-lane harnesses (void_out_core/void_send/cstring spots/node_tree
//! cells) additionally take --no-assertion-reach-checks (reach checks
//! double the formula; cover! witnesses carry vacuity insurance instead);
//! node_tree cells are the family's >30s release-gate tier (solver
//! 12-65s measured at box load ~7).

#[cfg(kani)]
mod proofs {
    use proof_support::{call1, mcx_stubs, stubs};
    use types_error::ERRCODE_INTERNAL_ERROR;

    use std::os::raw::c_int;

    extern "C" {
        // 43 error stubs: write (elog ERROR level, MAKE_SQLSTATE errcode),
        // return 1 at the ereport program point.
        fn pg_anyarray_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_anyarray_recv(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_anycompatiblearray_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_anycompatiblearray_recv(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_anyenum_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_anyrange_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_anycompatiblerange_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_anymultirange_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_anycompatiblemultirange_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_shell_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_shell_out(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_pg_node_tree_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_pg_node_tree_recv(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_pg_ddl_command_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_pg_ddl_command_out(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_pg_ddl_command_recv(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_pg_ddl_command_send(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_any_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_any_out(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_trigger_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_trigger_out(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_event_trigger_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_event_trigger_out(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_language_handler_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_language_handler_out(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_fdw_handler_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_fdw_handler_out(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_table_am_handler_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_table_am_handler_out(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_index_am_handler_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_index_am_handler_out(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_tsm_handler_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_tsm_handler_out(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_internal_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_internal_out(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_anyelement_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_anyelement_out(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_anynonarray_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_anynonarray_out(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_anycompatible_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_anycompatible_out(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_anycompatiblenonarray_in(level: *mut i32, sqlstate: *mut i32) -> c_int;
        fn pg_anycompatiblenonarray_out(level: *mut i32, sqlstate: *mut i32) -> c_int;

        // value functions
        fn pg_void_in() -> u64;
        fn pg_void_recv() -> u64;
        fn pg_void_out(out: *mut u8) -> c_int;
        fn pg_void_send(out: *mut u8) -> c_int;
        fn pg_cstring_in(s: *const u8, out: *mut u8) -> c_int;
        fn pg_cstring_out(s: *const u8, out: *mut u8) -> c_int;
        fn pg_pg_node_tree_out(payload: *const u8, plen: c_int, out: *mut u8) -> c_int;
        fn pg_pg_node_tree_send(payload: *const u8, plen: c_int, out: *mut u8) -> c_int;
    }

    // ---------- 43 error-stub rows: verdict + sqlstate + level parity ----------

    macro_rules! err_stub_eq {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let mut clevel: i32 = 0;
                let mut csqlstate: i32 = 0;
                let cflag = unsafe { $pg(&mut clevel, &mut csqlstate) };
                match call1(adt_pseudotypes::builtins::$fc, datum::Datum::null()) {
                    Ok(_) => {
                        // C unconditionally ereports; a Rust accept is a divergence.
                        assert!(false, "Rust accepted where C ereports");
                    }
                    Err(e) => {
                        // Vacuity insurance: the Err arm (the entire theorem
                        // for a stub row) must be REACHED, not just never
                        // contradicted.
                        kani::cover!(true, "Err arm reached");
                        assert!(cflag == 1);
                        assert!(e.sqlstate.0 == csqlstate);
                        assert!(e.level.0 == clevel);
                        // Box<PgError> drop glue walls symex (TRIAGE); teardown
                        // is not part of the claim.
                        core::mem::forget(e);
                    }
                }
            }
        )*};
    }

    err_stub_eq! {
        eq_anyarray_in: fc_anyarray_in / pg_anyarray_in;
        eq_anyarray_recv: fc_anyarray_recv / pg_anyarray_recv;
        eq_anycompatiblearray_in: fc_anycompatiblearray_in / pg_anycompatiblearray_in;
        eq_anycompatiblearray_recv: fc_anycompatiblearray_recv / pg_anycompatiblearray_recv;
        eq_anyenum_in: fc_anyenum_in / pg_anyenum_in;
        eq_anyrange_in: fc_anyrange_in / pg_anyrange_in;
        eq_anycompatiblerange_in: fc_anycompatiblerange_in / pg_anycompatiblerange_in;
        eq_anymultirange_in: fc_anymultirange_in / pg_anymultirange_in;
        eq_anycompatiblemultirange_in: fc_anycompatiblemultirange_in / pg_anycompatiblemultirange_in;
        eq_shell_in: fc_shell_in / pg_shell_in;
        eq_shell_out: fc_shell_out / pg_shell_out;
        eq_pg_node_tree_in: fc_pg_node_tree_in / pg_pg_node_tree_in;
        eq_pg_node_tree_recv: fc_pg_node_tree_recv / pg_pg_node_tree_recv;
        eq_pg_ddl_command_in: fc_pg_ddl_command_in / pg_pg_ddl_command_in;
        eq_pg_ddl_command_out: fc_pg_ddl_command_out / pg_pg_ddl_command_out;
        eq_pg_ddl_command_recv: fc_pg_ddl_command_recv / pg_pg_ddl_command_recv;
        eq_pg_ddl_command_send: fc_pg_ddl_command_send / pg_pg_ddl_command_send;
        eq_any_in: fc_any_in / pg_any_in;
        eq_any_out: fc_any_out / pg_any_out;
        eq_trigger_in: fc_trigger_in / pg_trigger_in;
        eq_trigger_out: fc_trigger_out / pg_trigger_out;
        eq_event_trigger_in: fc_event_trigger_in / pg_event_trigger_in;
        eq_event_trigger_out: fc_event_trigger_out / pg_event_trigger_out;
        eq_language_handler_in: fc_language_handler_in / pg_language_handler_in;
        eq_language_handler_out: fc_language_handler_out / pg_language_handler_out;
        eq_fdw_handler_in: fc_fdw_handler_in / pg_fdw_handler_in;
        eq_fdw_handler_out: fc_fdw_handler_out / pg_fdw_handler_out;
        eq_table_am_handler_in: fc_table_am_handler_in / pg_table_am_handler_in;
        eq_table_am_handler_out: fc_table_am_handler_out / pg_table_am_handler_out;
        eq_index_am_handler_in: fc_index_am_handler_in / pg_index_am_handler_in;
        eq_index_am_handler_out: fc_index_am_handler_out / pg_index_am_handler_out;
        eq_tsm_handler_in: fc_tsm_handler_in / pg_tsm_handler_in;
        eq_tsm_handler_out: fc_tsm_handler_out / pg_tsm_handler_out;
        eq_internal_in: fc_internal_in / pg_internal_in;
        eq_internal_out: fc_internal_out / pg_internal_out;
        eq_anyelement_in: fc_anyelement_in / pg_anyelement_in;
        eq_anyelement_out: fc_anyelement_out / pg_anyelement_out;
        eq_anynonarray_in: fc_anynonarray_in / pg_anynonarray_in;
        eq_anynonarray_out: fc_anynonarray_out / pg_anynonarray_out;
        eq_anycompatible_in: fc_anycompatible_in / pg_anycompatible_in;
        eq_anycompatible_out: fc_anycompatible_out / pg_anycompatible_out;
        eq_anycompatiblenonarray_in: fc_anycompatiblenonarray_in / pg_anycompatiblenonarray_in;
        eq_anycompatiblenonarray_out: fc_anycompatiblenonarray_out / pg_anycompatiblenonarray_out;
    }

    // ---------- void_in / void_recv: (Datum) 0 parity, shipped wrappers ----------

    #[kani::proof]
    fn eq_void_in() {
        let d = match call1(adt_pseudotypes::builtins::fc_void_in, datum::Datum::null()) {
            Ok(d) => d,
            Err(_) => panic!("void_in errored"),
        };
        kani::cover!(true, "Ok arm reached");
        let c = unsafe { pg_void_in() };
        assert!(d.as_usize() as u64 == c);
    }

    #[kani::proof]
    fn eq_void_recv() {
        let d = match call1(adt_pseudotypes::builtins::fc_void_recv, datum::Datum::null()) {
            Ok(d) => d,
            Err(_) => panic!("void_recv errored"),
        };
        kani::cover!(true, "Ok arm reached");
        let c = unsafe { pg_void_recv() };
        assert!(d.as_usize() as u64 == c);
    }

    // ---------- void_out: empty-cstring VALUE parity (shipped wrapper) ----------

    #[kani::proof]
    fn eq_void_out() {
        let d = match call1(adt_pseudotypes::builtins::fc_void_out, datum::Datum::null()) {
            Ok(d) => d,
            Err(_) => panic!("void_out errored"),
        };
        kani::cover!(true, "Ok arm reached");
        let mut cbuf = [0xAAu8; 2];
        let clen = unsafe { pg_void_out(cbuf.as_mut_ptr()) };
        assert!(clen == 0);
        // cstring content parity: both are "" (single NUL byte).
        let r0 = unsafe { *(d.as_usize() as *const u8) };
        assert!(r0 == cbuf[0]);
    }

    /// Core lane of void_out (the wrapper short-circuits to a static "");
    /// proves the shipped core pstrdup("") produces the same empty cstring,
    /// modulo static-buffer allocator model.
    #[kani::proof]
    #[kani::unwind(4)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    // grow/deallocate too: the cores' (dead) Vec-regrow and drop paths
    // otherwise drag the real arena machinery into symex (jsonb-probe
    // precedent).
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    // The cores' OOM arms (mcx.oom -> PgError::error + format!) are dead in
    // these harnesses but their message machinery walls symex — stub it
    // (message text was never part of the claim).
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
    fn eq_void_out_core() {
        let ctx = mcx::MemoryContext::new_bump("kani-pseudo");
        let out = match adt_pseudotypes::void_out(ctx.mcx()) {
            Ok(o) => o,
            Err(e) => { core::mem::forget(e); panic!("void_out errored") }
        };
        kani::cover!(true, "Ok arm reached");
        let mut cbuf = [0xAAu8; 2];
        let clen = unsafe { pg_void_out(cbuf.as_mut_ptr()) };
        assert!(out.len() == (clen as usize) + 1); // payload + NUL
        assert!(out[0] == cbuf[0]);
        core::mem::forget(out);
        core::mem::forget(ctx);
    }

    // ---------- cstring_in / cstring_out: pstrdup parity, len<=8 ----------

    // cstring_in / cstring_out — SPOT proofs (escalation ladder step 5).
    // Census of the wall: symbolic-content harnesses (symbolic NUL position,
    // per-NUL-position cells, cap 4, cap 2, kissat, --no-assertion-reach-
    // checks, tiny-proof-heap) ALL die on MEMORY (6.7-9.7 GiB RSS in
    // symex/SSA, 25-43s): pstrdup's copy length is position()-derived —
    // symbolic at formula-build time even when assumptions pin it — and a
    // symbolic-length memcpy through the static-heap model havocs the heap
    // array per SSA version. Concrete images make every copy length a
    // constant (node_tree per-length cells prove the same Vec machinery
    // green). Spots cover: empty string, len 1, len 3 (cap boundary), and
    // bytes-after-NUL-ignored (garbage tail).
    macro_rules! cstring_spot {
        ($($h:ident: $core:ident / $pg:ident = $src:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(6)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
            fn $h() {
                const N: usize = 4;
                let src: [u8; N] = $src;

                let ctx = mcx::MemoryContext::new_bump("kani-pseudo");
                let out = match adt_pseudotypes::$core(ctx.mcx(), &src) {
                    Ok(o) => o,
                    Err(e) => { core::mem::forget(e); panic!("core errored") }
                };
                kani::cover!(true, "Ok arm reached");

                let mut cbuf = [0xAAu8; N];
                let clen = unsafe { $pg(src.as_ptr(), cbuf.as_mut_ptr()) };

                // pstrdup: bytes through the first NUL, re-terminated.
                assert!(out.len() == (clen as usize) + 1);
                for i in 0..out.len() {
                    assert!(out[i] == cbuf[i]);
                }
                core::mem::forget(out);
                core::mem::forget(ctx);
            }
        )*};
    }

    cstring_spot! {
        eq_cstring_in_spot_empty: cstring_in / pg_cstring_in = [0, 0xAA, 0x55, 0x7F];
        eq_cstring_in_spot_len1: cstring_in / pg_cstring_in = [b'a', 0, 0xFF, 0x01];
        eq_cstring_in_spot_len3: cstring_in / pg_cstring_in = [b'x', 0x80, b'z', 0];
        eq_cstring_out_spot_empty: cstring_out / pg_cstring_out = [0, 0xAA, 0x55, 0x7F];
        eq_cstring_out_spot_len1: cstring_out / pg_cstring_out = [b'a', 0, 0xFF, 0x01];
        eq_cstring_out_spot_len3: cstring_out / pg_cstring_out = [b'x', 0x80, b'z', 0];
    }

    // ---------- void_send: empty bytea image parity ----------

    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    // grow/deallocate too: the cores' (dead) Vec-regrow and drop paths
    // otherwise drag the real arena machinery into symex (jsonb-probe
    // precedent).
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    // The cores' OOM arms (mcx.oom -> PgError::error + format!) are dead in
    // these harnesses but their message machinery walls symex — stub it
    // (message text was never part of the claim).
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
    fn eq_void_send() {
        let ctx = mcx::MemoryContext::new_bump("kani-pseudo");
        let b = match adt_pseudotypes::void_send(ctx.mcx()) {
            Ok(o) => o,
            Err(e) => { core::mem::forget(e); panic!("void_send errored") }
        };
        kani::cover!(true, "Ok arm reached");

        let mut cbuf = [0xAAu8; 8];
        let clen = unsafe { pg_void_send(cbuf.as_mut_ptr()) };

        // full image parity: 4-byte SET_VARSIZE header, empty payload.
        let img = b.as_bytes();
        assert!(img.len() == clen as usize);
        for i in 0..img.len() {
            assert!(img[i] == cbuf[i]);
        }
        core::mem::forget(b);
        core::mem::forget(ctx);
    }

    // ---------- pg_node_tree_out / pg_node_tree_send: textout/textsend ----------

    // Per-payload-length case split (escalation ladder step 4): the
    // symbolic-length harnesses wall at 30s (mcx symex floor + length
    // branching); each concrete length solves. Exhaustiveness over the
    // cap is witnessed by cover_node_tree_len_partition below.
    macro_rules! node_tree_out_eq {
        ($($h:ident @ $len:expr, $uw:literal;)*) => {$(
    #[kani::proof]
    // exact per-cell bound: longest loop is the len+1-byte compare
    // (unwind slack leaves dead loop copies in the formula — intout law).
    #[kani::unwind($uw)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    // grow/deallocate too: the cores' (dead) Vec-regrow and drop paths
    // otherwise drag the real arena machinery into symex (jsonb-probe
    // precedent).
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    // The cores' OOM arms (mcx.oom -> PgError::error + format!) are dead in
    // these harnesses but their message machinery walls symex — stub it
    // (message text was never part of the claim).
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
            fn $h() {
                const N: usize = 8;
                let payload: [u8; N] = kani::any();
                let len: usize = $len;

        let ctx = mcx::MemoryContext::new_bump("kani-pseudo");
        let out = match adt_pseudotypes::pg_node_tree_out(ctx.mcx(), &payload[..len]) {
            Ok(o) => o,
            Err(e) => { core::mem::forget(e); panic!("pg_node_tree_out errored") }
        };
        kani::cover!(true, "Ok arm reached");

        let mut cbuf = [0xAAu8; N + 1];
        let clen = unsafe { pg_pg_node_tree_out(payload.as_ptr(), len as c_int, cbuf.as_mut_ptr()) };

        // text_to_cstring: all len payload bytes (embedded NULs included) + NUL.
        assert!(clen as usize == len);
        assert!(out.len() == len + 1);
        for i in 0..out.len() {
            assert!(out[i] == cbuf[i]);
        }
        core::mem::forget(out);
        core::mem::forget(ctx);
    }
        )*};
    }

    node_tree_out_eq! {
        eq_pg_node_tree_out_len0 @ 0, 3;
        eq_pg_node_tree_out_len1 @ 1, 4;
        eq_pg_node_tree_out_len2 @ 2, 5;
        eq_pg_node_tree_out_len3 @ 3, 6;
        eq_pg_node_tree_out_len4 @ 4, 7;
        eq_pg_node_tree_out_len5 @ 5, 8;
        eq_pg_node_tree_out_len6 @ 6, 9;
        eq_pg_node_tree_out_len7 @ 7, 10;
        eq_pg_node_tree_out_len8 @ 8, 11;
    }

    /// pg_server_to_client seam pinned to its identity arm (ClientEncoding ==
    /// ServerEncoding): C returns the caller's pointer, Rust returns Ok(None)
    /// — the same "no conversion" arm modeled on both sides. Encoding
    /// conversion itself is out of scope of this row.
    fn s2c_identity<'mcx>(
        _mcx: mcx::Mcx<'mcx>,
        _s: &[u8],
    ) -> types_error::PgResult<Option<mcx::PgVec<'mcx, u8>>> {
        Ok(None)
    }

    macro_rules! node_tree_send_eq {
        ($($h:ident @ $len:expr, $uw:literal;)*) => {$(
    #[kani::proof]
    // exact per-cell bound: longest loop is the VARHDRSZ+len image compare
    // (unwind(10) fired the unwinding assertion at len 8 — a bound
    // artifact, not a divergence; slack leaves dead copies — intout law).
    #[kani::unwind($uw)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    // grow/deallocate too: the cores' (dead) Vec-regrow and drop paths
    // otherwise drag the real arena machinery into symex (jsonb-probe
    // precedent).
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    // The cores' OOM arms (mcx.oom -> PgError::error + format!) are dead in
    // these harnesses but their message machinery walls symex — stub it
    // (message text was never part of the claim).
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
            fn $h() {
                const N: usize = 8;
                let payload: [u8; N] = kani::any();
                let len: usize = $len;

        mbutils_seams::pg_server_to_client::set(s2c_identity);

        let ctx = mcx::MemoryContext::new_bump("kani-pseudo");
        let b = match adt_pseudotypes::pg_node_tree_send(ctx.mcx(), &payload[..len]) {
            Ok(o) => o,
            Err(e) => { core::mem::forget(e); panic!("pg_node_tree_send errored") }
        };
        kani::cover!(true, "Ok arm reached");

        let mut cbuf = [0xAAu8; N + 4];
        let clen = unsafe { pg_pg_node_tree_send(payload.as_ptr(), len as c_int, cbuf.as_mut_ptr()) };

        // full bytea image parity: SET_VARSIZE header + payload bytes.
        let img = b.as_bytes();
        assert!(img.len() == clen as usize);
        for i in 0..img.len() {
            assert!(img[i] == cbuf[i]);
        }
        core::mem::forget(b);
        core::mem::forget(ctx);
    }
        )*};
    }

    node_tree_send_eq! {
        eq_pg_node_tree_send_len0 @ 0, 6;
        eq_pg_node_tree_send_len1 @ 1, 7;
        eq_pg_node_tree_send_len2 @ 2, 8;
        eq_pg_node_tree_send_len3 @ 3, 9;
        eq_pg_node_tree_send_len4 @ 4, 10;
        eq_pg_node_tree_send_len5 @ 5, 11;
        eq_pg_node_tree_send_len6 @ 6, 12;
        eq_pg_node_tree_send_len7 @ 7, 13;
        eq_pg_node_tree_send_len8 @ 8, 14;
    }

    /// Union-coverage witness for the per-length split (MANDATORY per the
    /// case-split rule): a usize length fenced to the cap is one of the
    /// nine concrete cells.
    #[kani::proof]
    fn cover_node_tree_len_partition() {
        let len: usize = kani::any();
        kani::assume(len <= 8);
        assert!(len == 0 || len == 1 || len == 2 || len == 3 || len == 4
             || len == 5 || len == 6 || len == 7 || len == 8);
    }


    // ---------- negative control: rig must be able to fail ----------

    /// MUST FAIL: asserts shell_in's error carries the pre-with_sqlstate
    /// default (XX000 internal_error) instead of the shipped 0A000 — proves
    /// the Err arm executes shipped code, the message stub does not swallow
    /// `.with_sqlstate(..)`, and the sqlstate assertion is live. DEFAULT
    /// solver only (kissat never terminates on failing harnesses).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn control_shell_in_wrong_sqlstate() {
        match call1(adt_pseudotypes::builtins::fc_shell_in, datum::Datum::null()) {
            Ok(_) => {}
            Err(e) => {
                let s = e.sqlstate;
                core::mem::forget(e);
                assert!(s == ERRCODE_INTERNAL_ERROR, "expected failure: shipped sqlstate is 0A000, not XX000 (rig is live)");
            }
        }
    }
}
