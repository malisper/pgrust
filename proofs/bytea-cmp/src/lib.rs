//! Kani C≡Rust equivalence: the bytea comparator family
//! (byteaeq/ne/lt/le/gt/ge/cmp) — first VARLENA-input family.
//!
//! Rust side (shipped code, path-dep — never copied):
//!  - varlena::bytea::{byteaeq, byteane, bytealt, byteale, byteagt,
//!    byteage, byteacmp} (crates/backend/utils/adt/varlena/src/bytea.rs;
//!    cores are already pure &[u8] slices, no shipped edit needed —
//!    lt..cmp route through varlena::varstrfastcmp_c, lib.rs:128).
//!
//! C side: proofs/bytea-cmp/c/pg_bytea_cmp.c (postgres MASTER bytea.c,
//! provenance + shims documented there).
//!
//! VARLENA HARNESS PATTERN (established here): the C fmgr wrapper's
//! varlena plumbing (PG_GETARG_BYTEA_PP + VARDATA_ANY/VARSIZE_ANY_EXHDR)
//! reduces to a (ptr, len) pair once the datum is detoasted, so the
//! symbolic input is a fixed-cap byte array + symbolic length
//! (kani::assume(len <= CAP)) per side, passed as raw ptr/len to C and
//! as &buf[..len] to Rust.  DETOASTING IS OUT OF SCOPE: inputs model the
//! post-PG_GETARG_BYTEA_PP caller contract (pre-detoasted payloads).
//! byteaeq/ne's toast_raw_datum_size fast path is covered via the
//! rawlen = len + VARHDRSZ shim (see the C file).
//!
//! Claims and fences:
//!  - eq/ne/lt/le/gt/ge: exact verdict equality (C int 0/1 vs Rust bool).
//!  - cmp: exact VALUE equality — C returns the raw memcmp result, whose
//!    magnitude is SQL-visible (int32 return); CBMC's memcmp model returns
//!    the difference of the first mismatching unsigned chars, matching the
//!    glibc convention the shipped Rust core documents (lib.rs:122).  The
//!    ISO-C guarantee is sign-only; the proof ratifies the byte-difference
//!    convention, same ruling as the network-family memcmp proofs.
//!  - Bounds: symbolic lengths 0..=8 EACH side over fully symbolic byte
//!    buffers (all 81 length combos in one harness; contents exhaustive).
//!  - byteaeq/ne fast-path coverage: kani::cover witnesses in the eq
//!    harness prove the domain reaches (a) unequal lengths (length
//!    shortcut), (b) equal lengths with differing contents (memcmp path),
//!    and (c) equal inputs — the gate cannot silently narrow.
//!
//! Negative control: control_byteacmp_short_c_len feeds C a one-shorter
//! length and must FAIL (run with the DEFAULT solver, not kissat).
//!
//! ------------------------------------------------------------------
//! GET/SET BYTE/BIT WAVE (oids 721/723/722/724, extraction-gap triage
//! 2026-07-28) — see the section at the bottom of the proofs module.
//!
//! Rust cores: varlena::bytea::{bytea_get_byte, bytea_get_bit,
//! bytea_set_byte, bytea_set_bit} (bytea.rs:395-456).  C: REL_18_STABLE
//! varlena.c byteaGetByte/GetBit/SetByte/SetBit (vendored in
//! c/pg_bytea_cmp.c with the PROOF_EREPORT_FLAG convention: flag 1 =
//! 2202E ERRCODE_ARRAY_SUBSCRIPT_ERROR, flag 2 = 22023
//! ERRCODE_INVALID_PARAMETER_VALUE).
//!
//! Claims:
//!  - Get*: scalar VALUE parity on the Ok arm + verdict/sqlstate/level
//!    parity on the Err arm over symbolic len<=8 payloads with FULL-i32
//!    (GetByte) / FULL-i64 (GetBit) index domains.  Error message TEXT is
//!    out of proof (PgError::error + fmt::format stubbed, ledger wording
//!    "value-space only"); the shipped .with_sqlstate stays load-bearing.
//!  - Set*: fixed-width RESULT IMAGE parity — out len == in len asserted
//!    (result-image-wall law does not bite: offsets are index math, not
//!    data-dependent) — plus the same err planes, "modulo static-buffer
//!    allocator model" (proof_support mcx-stubs + tiny-proof-heap; the
//!    context/image teardown is out of the claim via mem::forget).
//!  - byteaSetByte's (char)-truncating store of new_byte (C
//!    `res[n] = newByte` on unsigned char vs Rust `new_byte as u8`) is
//!    IN-theorem over full-i32 new_byte; byteaSetBit's 22023 "new bit
//!    must be 0 or 1" plane is in-theorem over full-i32 new_bit, with the
//!    C range-check-before-bit-check order mirrored by the shipped core.
//!  - Both arms (all three for SetBit) carry kani::cover! witnesses.
//!
//! Extra negative control for the wave: control_get_byte_short_c_len
//! (C sees len-1 — verdict mismatch at n == len-1; MUST FAIL, DEFAULT
//! solver).

#[cfg(kani)]
mod proofs {
    use std::os::raw::c_int;

    extern "C" {
        fn pg_byteaeq(d1: *const u8, l1: usize, d2: *const u8, l2: usize) -> c_int;
        fn pg_byteane(d1: *const u8, l1: usize, d2: *const u8, l2: usize) -> c_int;
        fn pg_bytealt(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int) -> c_int;
        fn pg_byteale(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int) -> c_int;
        fn pg_byteagt(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int) -> c_int;
        fn pg_byteage(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int) -> c_int;
        fn pg_byteacmp(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int) -> c_int;
    }

    const N: usize = 8;

    /// One pre-detoasted bytea payload: fixed-cap symbolic bytes +
    /// symbolic length (the varlena harness pattern).
    fn sym_bytea() -> ([u8; N], usize) {
        let buf: [u8; N] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= N);
        (buf, len)
    }

    // ---- eq/ne: length-shortcut fast path + memcmp path ----

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_byteaeq() {
        let (b1, l1) = sym_bytea();
        let (b2, l2) = sym_bytea();
        // Fast-path coverage witnesses: the domain must reach the length
        // shortcut, the equal-length/differing-content memcmp path, and
        // true equality.
        kani::cover!(l1 != l2);
        kani::cover!(l1 == l2 && b1[..l1] != b2[..l2]);
        kani::cover!(l1 == l2 && b1[..l1] == b2[..l2]);
        let c = unsafe { pg_byteaeq(b1.as_ptr(), l1, b2.as_ptr(), l2) };
        let r = varlena::bytea::byteaeq(&b1[..l1], &b2[..l2]);
        assert!((c != 0) == r);
    }

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_byteane() {
        let (b1, l1) = sym_bytea();
        let (b2, l2) = sym_bytea();
        let c = unsafe { pg_byteane(b1.as_ptr(), l1, b2.as_ptr(), l2) };
        let r = varlena::bytea::byteane(&b1[..l1], &b2[..l2]);
        assert!((c != 0) == r);
    }

    // ---- lt/le/gt/ge: memcmp + length tiebreak, verdict parity ----

    macro_rules! ord_harness {
        ($harness:ident, $cfn:ident, $rfn:ident) => {
            #[kani::proof]
            #[kani::unwind(10)]
            fn $harness() {
                let (b1, l1) = sym_bytea();
                let (b2, l2) = sym_bytea();
                let c = unsafe { $cfn(b1.as_ptr(), l1 as c_int, b2.as_ptr(), l2 as c_int) };
                let r = varlena::bytea::$rfn(&b1[..l1], &b2[..l2]);
                assert!((c != 0) == r);
            }
        };
    }

    ord_harness!(eq_bytealt, pg_bytealt, bytealt);
    ord_harness!(eq_byteale, pg_byteale, byteale);
    ord_harness!(eq_byteagt, pg_byteagt, byteagt);
    ord_harness!(eq_byteage, pg_byteage, byteage);

    // ---- cmp: exact SQL-visible int32 value ----

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_byteacmp() {
        let (b1, l1) = sym_bytea();
        let (b2, l2) = sym_bytea();
        let c = unsafe { pg_byteacmp(b1.as_ptr(), l1 as c_int, b2.as_ptr(), l2 as c_int) };
        let r = varlena::bytea::byteacmp(&b1[..l1], &b2[..l2]);
        assert!(c == r);
    }

    // ---- bytea_larger / bytea_smaller (oids 6393/6394) ----
    // Winning-INPUT identity (network_larger pattern): C returns arg1 or
    // arg2 (shimmed to 1/2); Rust returns &v1 or &v2 — assert the same
    // input wins, by pointer identity.  Content-equal inputs are the
    // interesting tie class and are covered (both sides pick arg2/v2).

    extern "C" {
        fn pg_bytea_larger(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int) -> c_int;
        fn pg_bytea_smaller(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int) -> c_int;
    }

    macro_rules! minmax_harness {
        ($harness:ident, $cfn:ident, $rfn:ident) => {
            #[kani::proof]
            #[kani::unwind(10)]
            fn $harness() {
                let (b1, l1) = sym_bytea();
                let (b2, l2) = sym_bytea();
                // Tie coverage: equal contents must be reachable.
                kani::cover!(l1 == l2 && b1[..l1] == b2[..l2]);
                let c = unsafe { $cfn(b1.as_ptr(), l1 as c_int, b2.as_ptr(), l2 as c_int) };
                let s1 = &b1[..l1];
                let s2 = &b2[..l2];
                let r = varlena::bytea::$rfn(s1, s2);
                let r_is_arg1 = core::ptr::eq(r.as_ptr(), s1.as_ptr()) && r.len() == s1.len();
                assert!((c == 1) == r_is_arg1);
            }
        };
    }

    minmax_harness!(eq_bytea_larger, pg_bytea_larger, bytea_larger);
    minmax_harness!(eq_bytea_smaller, pg_bytea_smaller, bytea_smaller);

    // ---- negative control: rig is non-vacuous ----
    // C sees a one-byte-shorter left input than Rust; MUST fail with a
    // decodable counterexample.  Run with the DEFAULT solver (kissat
    // re-enumerates SAT passes on failing harnesses and never terminates).
    #[kani::proof]
    #[kani::unwind(10)]
    fn control_byteacmp_short_c_len() {
        let (b1, l1) = sym_bytea();
        kani::assume(l1 >= 1);
        let (b2, l2) = sym_bytea();
        let c = unsafe { pg_byteacmp(b1.as_ptr(), (l1 - 1) as c_int, b2.as_ptr(), l2 as c_int) };
        let r = varlena::bytea::byteacmp(&b1[..l1], &b2[..l2]);
        assert!(c == r);
    }

    // ================================================================
    // byteaGetByte / byteaGetBit / byteaSetByte / byteaSetBit
    // (oids 721 / 723 / 722 / 724) — see the module doc's wave section.
    // ================================================================

    use proof_support::{mcx_stubs, stubs};
    use types_error::{
        ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_INVALID_PARAMETER_VALUE, ERROR,
    };

    extern "C" {
        // err-flag convention: 0 ok / 1 = 2202E / 2 = 22023 (SetBit only)
        fn pg_byteaGetByte(vdata: *const u8, len: c_int, n: i32, err: *mut c_int) -> c_int;
        fn pg_byteaGetBit(vdata: *const u8, len: c_int, n: i64, err: *mut c_int) -> c_int;
        fn pg_byteaSetByte(
            res: *mut u8,
            len: c_int,
            n: i32,
            new_byte: i32,
            err: *mut c_int,
        ) -> c_int;
        fn pg_byteaSetBit(
            res: *mut u8,
            len: c_int,
            n: i64,
            new_bit: i32,
            err: *mut c_int,
        ) -> c_int;
    }

    // ---- Get*: scalar verdict + value + sqlstate parity ----

    #[kani::proof]
    // message plumbing out of proof (value-space only): PgError::error ->
    // field-identical constructor; format! -> empty String
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_bytea_get_byte() {
        let (buf, len) = sym_bytea();
        let n: i32 = kani::any(); // full i32 index domain
        let mut cerr: c_int = 0;
        let c = unsafe { pg_byteaGetByte(buf.as_ptr(), len as c_int, n, &mut cerr) };
        match varlena::bytea::bytea_get_byte(&buf[..len], n) {
            Ok(r) => {
                assert!(cerr == 0);
                assert!(r == c);
            }
            Err(e) => {
                assert!(cerr == 1);
                assert!(e.sqlstate == ERRCODE_ARRAY_SUBSCRIPT_ERROR);
                assert!(e.level == ERROR);
                // ERROR-DROP trap: Box<PgError> drop glue out of the claim
                core::mem::forget(e);
            }
        }
        kani::cover!(cerr == 0);
        kani::cover!(cerr == 1);
    }

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_bytea_get_bit() {
        let (buf, len) = sym_bytea();
        let n: i64 = kani::any(); // full i64 index domain (PG13+ int64 arg)
        let mut cerr: c_int = 0;
        let c = unsafe { pg_byteaGetBit(buf.as_ptr(), len as c_int, n, &mut cerr) };
        match varlena::bytea::bytea_get_bit(&buf[..len], n) {
            Ok(r) => {
                assert!(cerr == 0);
                assert!(r == c);
            }
            Err(e) => {
                assert!(cerr == 1);
                assert!(e.sqlstate == ERRCODE_ARRAY_SUBSCRIPT_ERROR);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        kani::cover!(cerr == 0);
        kani::cover!(cerr == 1);
    }

    // ---- Set*: fixed-width result image + err planes ----
    //
    // Harness scaffolding (not part of the claim): proof_support mcx-stubs
    // recipe — Mcx::allocate -> static bump (tiny-proof-heap 2 KiB; largest
    // allocation here is a 12-byte image), env::var -> "0", OnceLock ->
    // recompute, fmt::format stubbed; image/ctx mem::forget at harness end.
    // Theorem qualifier: "modulo static-buffer allocator model".
    //
    // unwind 14: image build copies VARHDRSZ + len <= 12 bytes and the
    // result compare loops len <= 8 times; max loop count 12, +1 for exit,
    // slack for the AcctWeak registry retain loop (kept TIGHT per the
    // dead-divider-copies trap).

    #[kani::proof]
    #[kani::unwind(14)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    // grow/deallocate stubs are LOAD-BEARING: vec_append_bytes has a
    // reachable try_reserve grow branch (json-escape round-2 lesson)
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_bytea_set_byte() {
        let (buf, len) = sym_bytea();
        let n: i32 = kani::any(); // full i32 index domain
        let new_byte: i32 = kani::any(); // full i32 — (char) truncation in-theorem
        // C mutates its private copy (PG_GETARG_BYTEA_P_COPY shim)
        let mut cbuf = buf;
        let mut cerr: c_int = 0;
        unsafe { pg_byteaSetByte(cbuf.as_mut_ptr(), len as c_int, n, new_byte, &mut cerr) };
        let ctx = mcx::MemoryContext::new_bump("kani-bytea-set");
        match varlena::bytea::bytea_set_byte(ctx.mcx(), &buf[..len], n, new_byte) {
            Ok(v) => {
                assert!(cerr == 0);
                // fixed-width result image: out len == in len
                assert!(v.varsize() == varlena::VARHDRSZ + len);
                let d = v.data();
                assert!(d.len() == len);
                let mut i = 0;
                while i < len {
                    assert!(d[i] == cbuf[i]);
                    i += 1;
                }
                core::mem::forget(v); // image teardown out of the claim
            }
            Err(e) => {
                assert!(cerr == 1);
                assert!(e.sqlstate == ERRCODE_ARRAY_SUBSCRIPT_ERROR);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        kani::cover!(cerr == 0);
        kani::cover!(cerr == 1);
        core::mem::forget(ctx); // context teardown walls symex (mcx recipe)
    }

    #[kani::proof]
    #[kani::unwind(14)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    // grow/deallocate stubs are LOAD-BEARING: vec_append_bytes has a
    // reachable try_reserve grow branch (json-escape round-2 lesson)
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_bytea_set_bit() {
        let (buf, len) = sym_bytea();
        let n: i64 = kani::any(); // full i64 index domain
        let new_bit: i32 = kani::any(); // full i32 — 22023 plane in-theorem
        let mut cbuf = buf;
        let mut cerr: c_int = 0;
        unsafe { pg_byteaSetBit(cbuf.as_mut_ptr(), len as c_int, n, new_bit, &mut cerr) };
        let ctx = mcx::MemoryContext::new_bump("kani-bytea-set");
        match varlena::bytea::bytea_set_bit(ctx.mcx(), &buf[..len], n, new_bit) {
            Ok(v) => {
                assert!(cerr == 0);
                assert!(v.varsize() == varlena::VARHDRSZ + len);
                let d = v.data();
                assert!(d.len() == len);
                let mut i = 0;
                while i < len {
                    assert!(d[i] == cbuf[i]);
                    i += 1;
                }
                core::mem::forget(v);
            }
            Err(e) => {
                // C checks range (flag 1 / 2202E) BEFORE bit value
                // (flag 2 / 22023); the shipped core mirrors that order.
                assert!(cerr == 1 || cerr == 2);
                if cerr == 1 {
                    assert!(e.sqlstate == ERRCODE_ARRAY_SUBSCRIPT_ERROR);
                } else {
                    assert!(e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE);
                }
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        kani::cover!(cerr == 0);
        kani::cover!(cerr == 1);
        kani::cover!(cerr == 2);
        core::mem::forget(ctx);
    }


    // ---- ladder probe (RVR lane 2026-07-28): width-1 cell, literal len=4.
    // Classifies the set_byte CNF wall as width-bound (symbolic copy len)
    // vs depth-bound. Literal length per the "assume never constant-folds" law.
    #[kani::proof]
    #[kani::unwind(14)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn probe_set_byte_len4() {
        let buf: [u8; 8] = kani::any();
        let len: usize = 4; // LITERAL width-1 cell
        let n: i32 = kani::any();
        let new_byte: i32 = kani::any();
        let mut cbuf = buf;
        let mut cerr: c_int = 0;
        unsafe { pg_byteaSetByte(cbuf.as_mut_ptr(), len as c_int, n, new_byte, &mut cerr) };
        let ctx = mcx::MemoryContext::new_bump("kani-bytea-set");
        match varlena::bytea::bytea_set_byte(ctx.mcx(), &buf[..len], n, new_byte) {
            Ok(v) => {
                assert!(cerr == 0);
                assert!(v.varsize() == varlena::VARHDRSZ + len);
                let d = v.data();
                assert!(d.len() == len);
                let mut i = 0;
                while i < len {
                    assert!(d[i] == cbuf[i]);
                    i += 1;
                }
                core::mem::forget(v);
            }
            Err(e) => {
                assert!(cerr == 1);
                assert!(e.sqlstate == ERRCODE_ARRAY_SUBSCRIPT_ERROR);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        kani::cover!(cerr == 0);
        kani::cover!(cerr == 1);
        core::mem::forget(ctx);
    }

    // ---- wave negative control: rig is non-vacuous ----
    // C sees a one-shorter payload length: at n == len-1 C raises 2202E
    // while Rust returns Ok — MUST FAIL with a decodable counterexample.
    // Run with the DEFAULT solver (kissat never terminates on failures).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn control_get_byte_short_c_len() {
        let (buf, len) = sym_bytea();
        kani::assume(len >= 1);
        let n: i32 = kani::any();
        let mut cerr: c_int = 0;
        let c = unsafe { pg_byteaGetByte(buf.as_ptr(), (len - 1) as c_int, n, &mut cerr) };
        match varlena::bytea::bytea_get_byte(&buf[..len], n) {
            Ok(r) => {
                assert!(cerr == 0); // fails at n == len-1
                assert!(r == c);
            }
            Err(e) => {
                assert!(cerr == 1);
                core::mem::forget(e);
            }
        }
    }
}
