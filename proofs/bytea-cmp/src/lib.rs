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
//!
//! ------------------------------------------------------------------
//! BYTEA <-> INT CASTS (ledger 6368/6369/6371/6372, varbit W10
//! continuation 2026-07-30) — see the casts section of the proofs
//! module.  bytea_int4/bytea_int8: scalar value + 22003 parity, both
//! arms covered (int8 cap 9 so its error arm is reachable).
//! int4_bytea/int8_bytea (alias intNsend): fixed-width big-endian
//! payload image + varsize parity over fully symbolic arguments,
//! modulo static-buffer allocator model.

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

    // ---- set_bit width-1 cell (varbit W10 continuation 2026-07-30):
    // literal len=4 per the set_byte precedent — clears the CNF width wall
    // the fully-symbolic-length harness hits; n/new_bit stay full-domain so
    // all three arms (Ok / 2202E / 22023) remain in-theorem.
    #[kani::proof]
    #[kani::unwind(14)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn probe_set_bit_len4() {
        let buf: [u8; 8] = kani::any();
        let len: usize = 4; // LITERAL width-1 cell
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

    // ================================================================
    // bytea <-> int casts (ledger 6368/6369/6371/6372) — varbit W10
    // continuation 2026-07-30. C: REL_18_STABLE varlena.c bytea_int4/
    // bytea_int8 (BE fold + length check) and int4_bytea/int8_bytea
    // (alias intNsend: the 4/8-byte big-endian payload image).
    //
    // Claims:
    //  - bytea_intN: scalar value parity over fully symbolic content and
    //    symbolic len <= 8, both arms (Ok value / 22003 sqlstate+level),
    //    arm covers. Value-space only (message text/location stubbed).
    //  - intN_bytea: full payload image parity (LITERAL length 4/8) over
    //    a fully symbolic argument + varsize check; modulo static-buffer
    //    allocator model (mcx-stubs recipe).
    // ================================================================

    use types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE;

    extern "C" {
        // err-flag convention: 0 ok / 1 = 22003
        fn pg_bytea_int4(d: *const u8, len: c_int, err: *mut c_int) -> i32;
        fn pg_bytea_int8(d: *const u8, len: c_int, err: *mut c_int) -> i64;
        fn pg_int4_bytea(a: i32, out4: *mut u8) -> c_int;
        fn pg_int8_bytea(a: i64, out8: *mut u8) -> c_int;
    }

    macro_rules! bytea_int_harness {
        // $cap must exceed the C sizeof(result) so the 22003 arm is
        // REACHABLE (the int8 arm needs len 9 — a plain sym_bytea cap of
        // 8 made that cover vacuous, caught by the cover witness).
        ($harness:ident, $cfn:ident, $rfn:ident, $rty:ty, $cap:expr) => {
            #[kani::proof]
            #[kani::unwind(12)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $harness() {
                let buf: [u8; $cap] = kani::any();
                let len: usize = kani::any();
                kani::assume(len <= $cap);
                let mut cerr: c_int = 0;
                let c = unsafe { $cfn(buf.as_ptr(), len as c_int, &mut cerr) };
                match varlena::bytea::$rfn(&buf[..len]) {
                    Ok(r) => {
                        assert!(cerr == 0);
                        assert!(r == c as $rty);
                    }
                    Err(e) => {
                        assert!(cerr == 1);
                        assert!(e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                    }
                }
                kani::cover!(cerr == 0);
                kani::cover!(cerr == 1);
            }
        };
    }

    bytea_int_harness!(eq_bytea_int4, pg_bytea_int4, bytea_int4, i32, 8);
    bytea_int_harness!(eq_bytea_int8, pg_bytea_int8, bytea_int8, i64, 9);

    macro_rules! int_bytea_harness {
        ($harness:ident, $cfn:ident, $aty:ty, $w:expr) => {
            #[kani::proof]
            #[kani::unwind(14)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $harness() {
                let a: $aty = kani::any(); // fully symbolic argument
                let mut cimg = [0u8; $w];
                unsafe { $cfn(a, cimg.as_mut_ptr()) };
                let ctx = mcx::MemoryContext::new_bump("kani-int-bytea");
                match varlena::bytea::int_bytea(ctx.mcx(), &a.to_be_bytes()) {
                    Ok(v) => {
                        // fixed-width image: varsize + full payload parity
                        assert!(v.varsize() == varlena::VARHDRSZ + $w);
                        let d = v.data();
                        assert!(d.len() == $w);
                        let mut i = 0;
                        while i < $w {
                            assert!(d[i] == cimg[i]);
                            i += 1;
                        }
                        core::mem::forget(v);
                    }
                    Err(e) => {
                        // alloc failure is outside the static-buffer model
                        core::mem::forget(e);
                        // unreachable under the mcx-stub allocator
                        assert!(false);
                    }
                }
                core::mem::forget(ctx);
            }
        };
    }

    int_bytea_harness!(eq_int4_bytea, pg_int4_bytea, i32, 4);
    int_bytea_harness!(eq_int8_bytea, pg_int8_bytea, i64, 8);

    // ---- int2 siblings (ledger 6367/6370): same rigs, width 2 ----

    extern "C" {
        fn pg_bytea_int2(d: *const u8, len: c_int, err: *mut c_int) -> i16;
        fn pg_int2_bytea(a: i16, out2: *mut u8) -> c_int;
    }

    bytea_int_harness!(eq_bytea_int2, pg_bytea_int2, bytea_int2, i16, 3);
    int_bytea_harness!(eq_int2_bytea, pg_int2_bytea, i16, 2);

    // ---- bytea_bit_count (ledger 6163) ----
    // Shipped core delegates pg_bitutils::pg_popcount, which takes the
    // PG_NUMBER_OF_ONES table walk for len < 8 and popcount_optimized
    // (NEON intrinsics on this host — Kani unsupported_construct, see the
    // bitutils family) for len >= 8. Claim is therefore fenced to
    // len <= 7: the scalar table path, fully symbolic content. The SIMD
    // path is excluded(blocked:simd) in the ledger, bitutils precedent.

    extern "C" {
        fn pg_bytea_bit_count(d: *const u8, len: c_int) -> i64;
    }

    #[kani::proof]
    #[kani::unwind(9)]
    fn eq_bytea_bit_count() {
        let buf: [u8; 7] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 7); // scalar table path fence (< 8)
        let c = unsafe { pg_bytea_bit_count(buf.as_ptr(), len as c_int) };
        let r = varlena::bytea::bytea_bit_count(&buf[..len]);
        assert!(r == c);
    }

    // ---- bytea_reverse (ledger 6382) ----
    // Same-length image, reversed bytes. The shipped core pushes per byte
    // into a PgVec (std-Vec-wall class at symbolic len), so the cells are
    // LITERAL lengths 0/4/8 with fully symbolic content (per-cell literal
    // law), under the mcx-stubs recipe.

    extern "C" {
        fn pg_bytea_reverse(d: *const u8, len: c_int, out: *mut u8) -> c_int;
    }

    macro_rules! reverse_cell {
        ($harness:ident, $w:expr) => {
            #[kani::proof]
            #[kani::unwind(14)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $harness() {
                let buf: [u8; 8] = kani::any();
                let len: usize = $w; // LITERAL cell
                let mut cimg = [0u8; 8];
                unsafe { pg_bytea_reverse(buf.as_ptr(), len as c_int, cimg.as_mut_ptr()) };
                let ctx = mcx::MemoryContext::new_bump("kani-bytea-rev");
                match varlena::bytea::bytea_reverse(ctx.mcx(), &buf[..len]) {
                    Ok(v) => {
                        assert!(v.varsize() == varlena::VARHDRSZ + len);
                        let d = v.data();
                        assert!(d.len() == len);
                        let mut i = 0;
                        while i < len {
                            assert!(d[i] == cimg[i]);
                            i += 1;
                        }
                        core::mem::forget(v);
                    }
                    Err(e) => {
                        core::mem::forget(e);
                        // unreachable under the mcx-stub allocator
                        assert!(false);
                    }
                }
                core::mem::forget(ctx);
            }
        };
    }

    reverse_cell!(eq_bytea_reverse_l0, 0);
    reverse_cell!(eq_bytea_reverse_l4, 4);
    reverse_cell!(eq_bytea_reverse_l8, 8);

    // ---- byteain, traditional escaped arm (ledger 1244) ----
    // Input is the post-protocol cstring contract: symbolic bytes with a
    // trailing NUL and NUL-FREE content (fence); the "\x" hex arm is
    // fenced out (hex decode separately proved, proofs/bytea-varbit).
    // The combined verdict+image claim at symbolic len<=8 WALLS at 450s
    // on BOTH solvers (result-image law: the 1/2/4-byte unit boundaries
    // make every output offset data-dependent), so the claim is split:
    //  - eq_byteain_esc_verdict: symbolic len<=8 — accept/reject verdict,
    //    22P02 sqlstate/level parity, and decoded-LENGTH parity (scalar
    //    projection of the image);
    //  - eq_byteain_esc_img_l4/_l8: literal input lengths, fully symbolic
    //    content — full payload image parity.
    // Modulo static-buffer allocator model; escontext = None (hard-error
    // path; soft-error routing is fmgr-tier).

    extern "C" {
        fn pg_byteain_esc(
            input: *const u8,
            out: *mut u8,
            outlen: *mut c_int,
            err: *mut c_int,
        ) -> c_int;
    }

    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_byteain_esc_verdict() {
        const CAP: usize = 8;
        let mut buf: [u8; CAP + 1] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= CAP);
        let mut i = 0;
        while i < CAP + 1 {
            if i < len {
                kani::assume(buf[i] != 0); // cstring contract: no interior NUL
            } else {
                buf[i] = 0; // literal NUL fill (dead-symbolic-byte rule)
            }
            i += 1;
        }
        // fence out the hex arm ("\x" prefix) — both sides dispatch on it
        kani::assume(!(len >= 2 && buf[0] == b'\\' && buf[1] == b'x'));

        let mut cout = [0u8; CAP];
        let mut coutlen: c_int = 0;
        let mut cerr: c_int = 0;
        unsafe { pg_byteain_esc(buf.as_ptr(), cout.as_mut_ptr(), &mut coutlen, &mut cerr) };

        let ctx = mcx::MemoryContext::new_bump("kani-byteain");
        match varlena::bytea::byteain(ctx.mcx(), &buf[..len], None) {
            Ok(Some(v)) => {
                assert!(cerr == 0);
                // scalar projection: decoded length parity
                assert!(v.data().len() == coutlen as usize);
                core::mem::forget(v);
            }
            Ok(None) => {
                // soft-error return needs an escontext; unreachable here
                assert!(false);
            }
            Err(e) => {
                assert!(cerr == 1);
                assert!(e.sqlstate == types_error::ERRCODE_INVALID_TEXT_REPRESENTATION);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        kani::cover!(cerr == 0 && coutlen < len as c_int); // an escape actually decoded
        kani::cover!(cerr == 1);
        core::mem::forget(ctx);
    }

    macro_rules! byteain_img_cell {
        ($harness:ident, $w:expr, $pin:expr) => {
            #[kani::proof]
            #[kani::unwind(12)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $harness() {
                const CAP: usize = 8;
                let len: usize = $w; // LITERAL cell
                let mut buf: [u8; CAP + 1] = kani::any();
                let mut i = 0;
                while i < CAP + 1 {
                    if i < len {
                        kani::assume(buf[i] != 0); // no interior NUL
                    } else {
                        buf[i] = 0; // literal NUL fill
                    }
                    i += 1;
                }
                #[allow(clippy::redundant_closure_call)]
                ($pin)(&mut buf);
                // fence out the hex arm
                kani::assume(!(len >= 2 && buf[0] == b'\\' && buf[1] == b'x'));

                let mut cout = [0u8; CAP];
                let mut coutlen: c_int = 0;
                let mut cerr: c_int = 0;
                unsafe {
                    pg_byteain_esc(buf.as_ptr(), cout.as_mut_ptr(), &mut coutlen, &mut cerr)
                };

                let ctx = mcx::MemoryContext::new_bump("kani-byteain");
                match varlena::bytea::byteain(ctx.mcx(), &buf[..len], None) {
                    Ok(Some(v)) => {
                        assert!(cerr == 0);
                        let d = v.data();
                        assert!(d.len() == coutlen as usize);
                        let mut i = 0;
                        while i < d.len() {
                            assert!(d[i] == cout[i]);
                            i += 1;
                        }
                        core::mem::forget(v);
                    }
                    Ok(None) => {
                        assert!(false);
                    }
                    Err(e) => {
                        assert!(cerr == 1);
                        assert!(e.sqlstate == types_error::ERRCODE_INVALID_TEXT_REPRESENTATION);
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                    }
                }
                kani::cover!(cerr == 0 && coutlen < len as c_int);
                kani::cover!(cerr == 1);
                core::mem::forget(ctx);
            }
        };
    }

    // Fully-symbolic-content cells wall in CNF even at literal len=4
    // (both solvers, 450s; symex completes — the 1/2/4-byte unit
    // segmentation is itself content-dependent, so every output offset
    // remains a byte-mux). Form-PINNED cells instead (varbit bits_in
    // precedent: a literal first byte pins the parse form), decode-kernel
    // coverage per unit shape:
    //  - plain_l4: no backslash anywhere — identity copy;
    //  - octal1_l4: literal '\\' at 0, symbolic tail — valid-octal
    //    accept (1-byte unit), '\\\\' + plain tail, and reject all live;
    //  - bs_l2: literal '\\' at 0, len 2 — '\\\\' unit vs reject.
    // Plain-form cells WALL whenever the pin is an ASSUME (never folds;
    // measured: assume-pinned l4 AND l2 both 450s walls while literal
    // bs_l2 proves in 71s). Only LITERAL pins prune: byte 0 is a literal
    // plain byte, byte 1 stays fully symbolic — identity copy, trailing-
    // backslash reject, and '\\\\'-after-plain planes all live.
    byteain_img_cell!(eq_byteain_esc_plain1_l2, 2, |buf: &mut [u8; 9]| {
        buf[0] = b'A'; // LITERAL plain byte
    });
    // octal cell with only buf[0] literal WALLS at len 4 (450s both the
    // one-literal pin and fully-symbolic variants); two literal bytes
    // ('\\','1') leave the two octal-range bytes symbolic — accept
    // (single 1-byte unit) and reject planes both live.
    byteain_img_cell!(eq_byteain_esc_octal2_l4, 4, |buf: &mut [u8; 9]| {
        buf[0] = b'\\'; // LITERAL form selector
        buf[1] = b'1'; // LITERAL first octal digit
    });
    byteain_img_cell!(eq_byteain_esc_bs_l2, 2, |buf: &mut [u8; 9]| {
        buf[0] = b'\\'; // LITERAL form selector
    });

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
