//! Kani C≡Rust equivalence: datum.c copy/serialize kernels
//! (`adt/scalar/src/datum_ops.rs` — campaign lane p1-lanep, routes rows
//! `proofs/scalar-datum` in docs/verification/phase1-routes.tsv).
//!
//! Rust side: the SHIPPED `adt_scalar::datum_ops` entry points
//! (datum_get_size, datum_copy, datum_transfer, datum_is_equal,
//! datum_estimate_space, datum_serialize, datum_restore).
//! C side: c/pg_datum.c — verbatim REL_18_STABLE datum.c bodies (see its
//! provenance header for the shim census: ereport→err-flag, palloc→static
//! bump, EOH machinery→trapping stubs, strlen→pg_proof_strlen).
//!
//! DOMAINS AND FENCES (ledger wording per harness in README.md):
//! - by-val arms: full symbolic Datum word; typlen at the C-contract
//!   literals {1,2,4,8} (Kani CHECKS the Rust debug_assert!, so the
//!   out-of-contract by-val plane is deliberately out of domain — C's
//!   Assert compiles out and it returns typLen for any value; the Rust
//!   debug_assert is a ported-in constraint, not a behavior difference in
//!   release).
//! - varlena arms: symbolic 4-byte header images, EXPANDED-OBJECT ARM
//!   FENCED OUT (first byte != 0x01 or tag fenced to non-expanded): the
//!   EOH flatten/transfer machinery is session state, out of this family's
//!   scope. Both sides' expanded arms carry reachability traps
//!   (pg_proof_eoh_reached asserted 0).
//! - external headers: tag fenced to the four defined vartags {1,2,3,18}.
//!   OUT-OF-FENCE DEVIATION (documented, not proved): on an undefined tag
//!   C's VARTAG_SIZE yields 0 (AssertMacro compiles out) so VARSIZE_ANY
//!   returns 2; the Rust port panics ("unrecognized TOAST vartag") —
//!   pgrust hardening.
//! - datum_restore: domain fenced to well-formed images (header ∈ {-2,-1}
//!   ∪ 1..=cap with sufficient payload). The Rust port carries RELEASE
//!   asserts on corrupt/short input where C memcpy's garbage — pgrust
//!   hardening, out of proof.
//! - allocation: "modulo static-buffer allocator model" both sides (Rust
//!   proof_support mcx stubs / C pg_proof_palloc); allocation strategy is
//!   not part of any claim.
//! - error arms: PgError message TEXT out of proof (canonical
//!   proof_support stubs); verdict + sqlstate (via the C err-flag value
//!   convention: 1 = ERRCODE_DATA_EXCEPTION, 2 = internal/elog) stay in.
//!
//! Negative control: control_dgs_fixedlen_skew_must_fail (DEFAULT solver —
//! kissat never terminates on failing harnesses) MUST fail on its
//! `assert!(r == c)`.
//!
//! Run recipe (see proofs/SUITE.tsv rows; measured 2026-07-30 under
//! multi-lane laptop load — re-time idle):
//!   timeout 30 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_datum.c \
//!       --solver kissat --harness <h> --exact

#[cfg(kani)]
mod proofs {
    use adt_scalar::datum_ops::{
        datum_copy, datum_estimate_space, datum_get_size, datum_is_equal, datum_restore,
        datum_serialize, datum_transfer,
    };
    use datum::Datum;
    use proof_support::{mcx_stubs, stubs};
    use types_error::ERRCODE_DATA_EXCEPTION;

    extern "C" {
        fn pg_fxid_from_allowable_at(nextFullXid: u64, xid: u32) -> u64;
        fn pg_datumGetSize(value: usize, typByVal: i32, typLen: i32, err: *mut i32) -> usize;
        fn pg_datumCopy(value: usize, typByVal: i32, typLen: i32, err: *mut i32) -> usize;
        fn pg_datumTransfer(value: usize, typByVal: i32, typLen: i32, err: *mut i32) -> usize;
        fn pg_datumIsEqual(
            value1: usize,
            value2: usize,
            typByVal: i32,
            typLen: i32,
            err: *mut i32,
        ) -> i32;
        fn pg_datumEstimateSpace(
            value: usize,
            isnull: i32,
            typByVal: i32,
            typLen: i32,
            err: *mut i32,
        ) -> usize;
        fn pg_datumSerialize(
            value: usize,
            isnull: i32,
            typByVal: i32,
            typLen: i32,
            start_address: *mut *mut u8,
            err: *mut i32,
        ) -> i32; // C void rides as int (goto-cc Unit-vs-void trap)
        fn pg_datumRestore(start_address: *mut *mut u8, isnull: *mut i32) -> usize;
        static mut pg_proof_eoh_reached: i32;
    }

    fn eoh_trap_clean() -> bool {
        // SAFETY: single-threaded under Kani; plain int read.
        unsafe { pg_proof_eoh_reached == 0 }
    }

    /// Unwrap a PgResult on a path proven error-free WITHOUT dragging the
    /// PgError Debug-format machinery into symex (measured: a plain
    /// .unwrap() on a symbolic-typlen path hangs symex in <str as
    /// Debug>::fmt even though the Err arm is unreachable).
    fn ok_or_fail<T: Default>(r: types_error::PgResult<T>) -> T {
        match r {
            Ok(v) => v,
            Err(_) => {
                assert!(false, "unexpected Err arm");
                T::default()
            }
        }
    }

    /// C reads varlena headers through `varattrib_4b` union derefs, which
    /// CBMC bounds-checks against the WHOLE union object — back symbolic
    /// headers with a 16-byte aligned buffer (real varlena datums are
    /// maxaligned with trailing payload, so this matches the C contract).
    #[repr(align(8))]
    struct HdrBuf([u8; 16]);

    // ---------------- datum_get_size ----------------

    /// by-val arm, full symbolic Datum word, C-contract typlens {1,2,4,8}.
    #[kani::proof]
    fn eq_dgs_byval() {
        let v: u64 = kani::any();
        for typlen in [1i16, 2, 4, 8] {
            let r = datum_get_size(Datum::from_u64(v), true, typlen).unwrap();
            let mut err = 0i32;
            let c = unsafe { pg_datumGetSize(v as usize, 1, typlen as i32, &mut err) };
            assert!(err == 0);
            assert!(r == c);
        }
    }

    /// fixed-length by-ref arm: literal typlen spot cells spanning the
    /// i16-positive range (the arm is the identity size=typlen; literals
    /// are the case-split law's cure for the symbolic-typlen symex hang —
    /// assume(typlen>0) leaves the dead -1/-2 deref arms in the formula).
    #[kani::proof]
    fn eq_dgs_fixedlen() {
        let v: u64 = kani::any();
        for typlen in [1i16, 2, 4, 7, 8, 42, 6666, 32767] {
            let r = datum_get_size(Datum::from_u64(v), false, typlen).unwrap();
            let mut err = 0i32;
            let c = unsafe { pg_datumGetSize(v as usize, 0, typlen as i32, &mut err) };
            assert!(err == 0);
            assert!(r == c);
        }
    }

    /// varlena arm (typlen -1): fully symbolic 4-byte header, 1B and 4B
    /// forms (external fenced out: first byte != 0x01).
    #[kani::proof]
    fn eq_dgs_varlena() {
        let hdr = HdrBuf(kani::any());
        kani::assume(hdr.0[0] != 0x01);
        let p = hdr.0.as_ptr() as usize;
        let r = datum_get_size(Datum::from_usize(p), false, -1).unwrap();
        let mut err = 0i32;
        let c = unsafe { pg_datumGetSize(p, 0, -1, &mut err) };
        assert!(err == 0);
        assert!(r == c);
        assert!(eoh_trap_clean());
    }

    /// varlena arm, external headers: tag symbolic over the four defined
    /// vartags. Out-of-fence (undefined tag): C returns 2, Rust panics —
    /// documented pgrust hardening, out of domain.
    #[kani::proof]
    fn eq_dgs_external() {
        let tag: u8 = kani::any();
        kani::assume(tag == 1 || tag == 2 || tag == 3 || tag == 18);
        let buf: [u8; 2] = [0x01, tag];
        let p = buf.as_ptr() as usize;
        let r = datum_get_size(Datum::from_usize(p), false, -1).unwrap();
        let mut err = 0i32;
        let c = unsafe { pg_datumGetSize(p, 0, -1, &mut err) };
        assert!(err == 0);
        assert!(r == c);
    }

    /// cstring arm (typlen -2): symbolic 8-byte buffer with a guaranteed
    /// terminator at index 7 (interior NULs stay symbolic — strlen finds
    /// the first).
    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_dgs_cstring() {
        let mut buf: [u8; 8] = kani::any();
        buf[7] = 0;
        let p = buf.as_ptr() as usize;
        let r = datum_get_size(Datum::from_usize(p), false, -2).unwrap();
        let mut err = 0i32;
        let c = unsafe { pg_datumGetSize(p, 0, -2, &mut err) };
        assert!(err == 0);
        assert!(r == c);
    }

    /// null-pointer error arm, typlen ∈ {-1,-2}: verdict + sqlstate parity
    /// (C err-flag 1 == ERRCODE_DATA_EXCEPTION). Message text out of proof.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_dgs_err_nullptr() {
        for typlen in [-1i16, -2] {
            let r = datum_get_size(Datum::null(), false, typlen);
            let mut err = 0i32;
            let _ = unsafe { pg_datumGetSize(0, 0, typlen as i32, &mut err) };
            match r {
                Err(e) => {
                    assert!(err == 1, "C accepted where Rust errored");
                    assert!(e.sqlstate() == ERRCODE_DATA_EXCEPTION);
                }
                Ok(_) => assert!(false, "Rust accepted a NULL by-ref datum"),
            }
        }
    }

    /// invalid-typlen error arm (typlen <= 0, not -1/-2): verdict parity
    /// against C's elog class (err-flag 2).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_dgs_err_badtyplen() {
        let typlen: i16 = kani::any();
        kani::assume(typlen <= 0 && typlen != -1 && typlen != -2);
        let buf = [0u8; 1];
        let p = buf.as_ptr() as usize;
        let r = datum_get_size(Datum::from_usize(p), false, typlen);
        let mut err = 0i32;
        let _ = unsafe { pg_datumGetSize(p, 0, typlen as i32, &mut err) };
        assert!(r.is_err());
        assert!(err == 2);
    }

    // ---------------- datum_copy / datum_transfer ----------------

    /// by-val copy: identity both sides, full symbolic word.
    #[kani::proof]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_dcopy_byval() {
        let v: u64 = kani::any();
        let ctx = mcx::MemoryContext::new_bump("kani-datum");
        let r = datum_copy(ctx.mcx(), Datum::from_u64(v), true, 8).unwrap();
        let mut err = 0i32;
        let c = unsafe { pg_datumCopy(v as usize, 1, 8, &mut err) };
        assert!(err == 0);
        assert!(r.as_u64() == c as u64);
        core::mem::forget(ctx);
    }

    /// fixed-length by-ref copy (typlen 5): symbolic payload, copied image
    /// compared byte-for-byte. Modulo static-buffer allocator model.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_dcopy_fixedlen() {
        const N: usize = 5;
        let src: [u8; N] = kani::any();
        let p = src.as_ptr() as usize;
        let ctx = mcx::MemoryContext::new_bump("kani-datum");
        let r = datum_copy(ctx.mcx(), Datum::from_usize(p), false, N as i16).unwrap();
        let mut err = 0i32;
        let c = unsafe { pg_datumCopy(p, 0, N as i32, &mut err) };
        assert!(err == 0);
        let rs = unsafe { core::slice::from_raw_parts(r.as_usize() as *const u8, N) };
        let cs = unsafe { core::slice::from_raw_parts(c as *const u8, N) };
        for i in 0..N {
            assert!(rs[i] == cs[i]);
        }
        core::mem::forget(ctx);
    }

    /// varlena copy, 1-byte-header form: symbolic total size 1..=6 (size
    /// includes the header byte), symbolic payload; copied images compared.
    /// Expanded arm fenced out by construction (header byte is odd).
    #[kani::proof]
    #[kani::unwind(9)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_dcopy_varlena1b() {
        const CAP: usize = 6;
        let mut buf: [u8; CAP] = kani::any();
        let len: usize = kani::any();
        kani::assume(len >= 1 && len <= CAP);
        buf[0] = ((len as u8) << 1) | 0x01; // 1B header: never 0x01 since len >= 1
        let p = buf.as_ptr() as usize;
        let ctx = mcx::MemoryContext::new_bump("kani-datum");
        let r = datum_copy(ctx.mcx(), Datum::from_usize(p), false, -1).unwrap();
        let mut err = 0i32;
        let c = unsafe { pg_datumCopy(p, 0, -1, &mut err) };
        assert!(err == 0);
        let rs = unsafe { core::slice::from_raw_parts(r.as_usize() as *const u8, len) };
        let cs = unsafe { core::slice::from_raw_parts(c as *const u8, len) };
        for i in 0..len {
            assert!(rs[i] == cs[i]);
        }
        assert!(eoh_trap_clean());
        core::mem::forget(ctx);
    }

    /// datum_transfer on the non-expanded plane (== datum_copy semantics):
    /// varlena 1B cell; the EXPANDED_RW reparent arm is fenced out by
    /// construction and trap-guarded.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_dtransfer_nonexpanded() {
        const CAP: usize = 5;
        let mut buf: [u8; CAP] = kani::any();
        let len: usize = kani::any();
        kani::assume(len >= 1 && len <= CAP);
        buf[0] = ((len as u8) << 1) | 0x01;
        let p = buf.as_ptr() as usize;
        let ctx = mcx::MemoryContext::new_bump("kani-datum");
        let r = datum_transfer(ctx.mcx(), Datum::from_usize(p), false, -1).unwrap();
        let mut err = 0i32;
        let c = unsafe { pg_datumTransfer(p, 0, -1, &mut err) };
        assert!(err == 0);
        let rs = unsafe { core::slice::from_raw_parts(r.as_usize() as *const u8, len) };
        let cs = unsafe { core::slice::from_raw_parts(c as *const u8, len) };
        for i in 0..len {
            assert!(rs[i] == cs[i]);
        }
        assert!(eoh_trap_clean());
        core::mem::forget(ctx);
    }

    // ---------------- datum_is_equal ----------------

    /// by-val equality: full symbolic word × word.
    #[kani::proof]
    fn eq_disequal_byval() {
        let v1: u64 = kani::any();
        let v2: u64 = kani::any();
        let r = datum_is_equal(Datum::from_u64(v1), Datum::from_u64(v2), true, 8).unwrap();
        let mut err = 0i32;
        let c = unsafe { pg_datumIsEqual(v1 as usize, v2 as usize, 1, 8, &mut err) };
        assert!(err == 0);
        assert!(r == (c != 0));
        kani::cover!(r);
        kani::cover!(!r);
    }

    /// fixed-length by-ref equality (typlen 4): two symbolic images,
    /// both verdicts cover-witnessed.
    #[kani::proof]
    #[kani::unwind(7)]
    fn eq_disequal_fixedlen() {
        const N: usize = 4;
        let a: [u8; N] = kani::any();
        let b: [u8; N] = kani::any();
        let (pa, pb) = (a.as_ptr() as usize, b.as_ptr() as usize);
        let r =
            datum_is_equal(Datum::from_usize(pa), Datum::from_usize(pb), false, N as i16).unwrap();
        let mut err = 0i32;
        let c = unsafe { pg_datumIsEqual(pa, pb, 0, N as i32, &mut err) };
        assert!(err == 0);
        assert!(r == (c != 0));
        kani::cover!(r);
        kani::cover!(!r);
    }

    /// varlena equality: two 1B-header images with independent symbolic
    /// sizes (size-mismatch fast path + byte-compare path both in domain).
    #[kani::proof]
    #[kani::unwind(8)]
    fn eq_disequal_varlena() {
        const CAP: usize = 5;
        let mut a: [u8; CAP] = kani::any();
        let mut b: [u8; CAP] = kani::any();
        let (la, lb): (usize, usize) = (kani::any(), kani::any());
        kani::assume(la >= 1 && la <= CAP && lb >= 1 && lb <= CAP);
        a[0] = ((la as u8) << 1) | 0x01;
        b[0] = ((lb as u8) << 1) | 0x01;
        let (pa, pb) = (a.as_ptr() as usize, b.as_ptr() as usize);
        let r = datum_is_equal(Datum::from_usize(pa), Datum::from_usize(pb), false, -1).unwrap();
        let mut err = 0i32;
        let c = unsafe { pg_datumIsEqual(pa, pb, 0, -1, &mut err) };
        assert!(err == 0);
        assert!(r == (c != 0));
        kani::cover!(r);
        kani::cover!(!r);
    }

    // ---------------- datum_estimate_space ----------------

    /// scalar cells: null (any typbyval/typlen), by-val, fixed-length
    /// symbolic typlen. Datum backed by real memory for the dead by-ref
    /// arms (see eq_dgs_fixedlen).
    #[kani::proof]
    #[kani::unwind(20)]
    fn eq_destimate_scalar() {
        let mut buf = HdrBuf(kani::any());
        buf.0[15] = 0;
        let p = buf.0.as_ptr() as usize;
        let v: u64 = kani::any();
        // null: 4 bytes regardless
        let r = datum_estimate_space(Datum::from_u64(v), true, true, 8).unwrap();
        let mut err = 0i32;
        let c = unsafe { pg_datumEstimateSpace(v as usize, 1, 1, 8, &mut err) };
        assert!(err == 0 && r == c);
        // by-val
        let r = datum_estimate_space(Datum::from_u64(v), false, true, 8).unwrap();
        let mut err = 0i32;
        let c = unsafe { pg_datumEstimateSpace(v as usize, 0, 1, 8, &mut err) };
        assert!(err == 0 && r == c);
        // fixed-length by-ref: literal typlen cells (case-split law; the
        // live arm never derefs)
        for typlen in [1i16, 3, 8, 32767] {
            let r = datum_estimate_space(Datum::from_usize(p), false, false, typlen).unwrap();
            let mut err = 0i32;
            let c = unsafe { pg_datumEstimateSpace(p, 0, 0, typlen as i32, &mut err) };
            assert!(err == 0 && r == c);
        }
    }

    /// varlena cell: symbolic 4-byte header, expanded arm fenced + trapped.
    #[kani::proof]
    fn eq_destimate_varlena() {
        let hdr = HdrBuf(kani::any());
        kani::assume(hdr.0[0] != 0x01);
        let p = hdr.0.as_ptr() as usize;
        let r = datum_estimate_space(Datum::from_usize(p), false, false, -1).unwrap();
        let mut err = 0i32;
        let c = unsafe { pg_datumEstimateSpace(p, 0, 0, -1, &mut err) };
        assert!(err == 0);
        assert!(r == c);
        assert!(eoh_trap_clean());
    }

    // ---------------- datum_serialize ----------------

    fn serialize_both(
        value: Datum,
        cvalue: usize,
        isnull: bool,
        typbyval: bool,
        typlen: i16,
        expect: usize,
    ) {
        let ctx = mcx::MemoryContext::new_bump("kani-datum");
        let mut out = mcx::vec_with_capacity_in::<u8>(ctx.mcx(), 32).unwrap();
        datum_serialize(value, isnull, typbyval, typlen, &mut out).unwrap();

        let mut cbuf = [0u8; 32];
        let mut cursor: *mut u8 = cbuf.as_mut_ptr();
        let mut err = 0i32;
        let _ = unsafe {
            pg_datumSerialize(
                cvalue,
                isnull as i32,
                typbyval as i32,
                typlen as i32,
                &mut cursor,
                &mut err,
            )
        };
        assert!(err == 0);
        let cwritten = cursor as usize - cbuf.as_ptr() as usize;
        assert!(out.len() == expect);
        assert!(cwritten == expect);
        for i in 0..expect {
            assert!(out[i] == cbuf[i]);
        }
        core::mem::forget(out);
        core::mem::forget(ctx);
    }

    /// null image: 4-byte -2 header, nothing else.
    #[kani::proof]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_dserialize_null() {
        let v: u64 = kani::any();
        serialize_both(Datum::from_u64(v), v as usize, true, true, 8, 4);
    }

    /// by-val image: -1 header + full 8-byte Datum word, symbolic value.
    #[kani::proof]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_dserialize_byval() {
        let v: u64 = kani::any();
        serialize_both(Datum::from_u64(v), v as usize, false, true, 8, 12);
    }

    /// fixed-length by-ref image (typlen 4): length header + payload.
    #[kani::proof]
    #[kani::unwind(11)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_dserialize_fixedlen() {
        const N: usize = 4;
        let src: [u8; N] = kani::any();
        let p = src.as_ptr() as usize;
        serialize_both(Datum::from_usize(p), p, false, false, N as i16, 4 + N);
    }

    /// varlena 1B-header image, symbolic size 1..=6: header + verbatim
    /// varlena bytes (no expansion). Expanded arm fenced + trapped.
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_dserialize_varlena1b() {
        const CAP: usize = 6;
        let mut buf: [u8; CAP] = kani::any();
        let len: usize = kani::any();
        kani::assume(len >= 1 && len <= CAP);
        buf[0] = ((len as u8) << 1) | 0x01;
        let p = buf.as_ptr() as usize;

        let ctx = mcx::MemoryContext::new_bump("kani-datum");
        let mut out = mcx::vec_with_capacity_in::<u8>(ctx.mcx(), 32).unwrap();
        datum_serialize(Datum::from_usize(p), false, false, -1, &mut out).unwrap();

        let mut cbuf = [0u8; 32];
        let mut cursor: *mut u8 = cbuf.as_mut_ptr();
        let mut err = 0i32;
        let _ = unsafe { pg_datumSerialize(p, 0, 0, -1, &mut cursor, &mut err) };
        assert!(err == 0);
        let cwritten = cursor as usize - cbuf.as_ptr() as usize;
        assert!(out.len() == 4 + len);
        assert!(cwritten == 4 + len);
        for i in 0..4 + len {
            assert!(out[i] == cbuf[i]);
        }
        assert!(eoh_trap_clean());
        core::mem::forget(out);
        core::mem::forget(ctx);
    }

    // ---------------- datum_restore ----------------

    /// null image restore: (Datum(0), isnull=true) both sides.
    #[kani::proof]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_drestore_null() {
        let img: [u8; 4] = (-2i32).to_ne_bytes();
        let ctx = mcx::MemoryContext::new_bump("kani-datum");
        let mut cur: &[u8] = &img;
        let (rv, risnull) = datum_restore(ctx.mcx(), &mut cur).unwrap();
        let mut cbytes = img;
        let mut ccur: *mut u8 = cbytes.as_mut_ptr();
        let mut cisnull = 0i32;
        let cv = unsafe { pg_datumRestore(&mut ccur, &mut cisnull) };
        assert!(risnull && cisnull != 0);
        assert!(rv.as_u64() == cv as u64);
        assert!(cur.is_empty());
        core::mem::forget(ctx);
    }

    /// by-val image restore: -1 header + symbolic 8-byte word.
    #[kani::proof]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_drestore_byval() {
        let word: [u8; 8] = kani::any();
        let mut img = [0u8; 12];
        img[..4].copy_from_slice(&(-1i32).to_ne_bytes());
        for i in 0..8 {
            img[4 + i] = word[i]; // typed per-element staging (CBMC memcpy law)
        }
        let ctx = mcx::MemoryContext::new_bump("kani-datum");
        let mut cur: &[u8] = &img;
        let (rv, risnull) = datum_restore(ctx.mcx(), &mut cur).unwrap();
        let mut cbytes = img;
        let mut ccur: *mut u8 = cbytes.as_mut_ptr();
        let mut cisnull = 1i32;
        let cv = unsafe { pg_datumRestore(&mut ccur, &mut cisnull) };
        assert!(!risnull && cisnull == 0);
        assert!(rv.as_u64() == cv as u64);
        core::mem::forget(ctx);
    }

    /// by-ref image restore (header 5 + symbolic payload): restored images
    /// compared byte-for-byte. Well-formed-image fence (see module doc).
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_drestore_byref() {
        const N: usize = 5;
        let payload: [u8; N] = kani::any();
        let mut img = [0u8; 4 + N];
        img[..4].copy_from_slice(&(N as i32).to_ne_bytes());
        for i in 0..N {
            img[4 + i] = payload[i];
        }
        let ctx = mcx::MemoryContext::new_bump("kani-datum");
        let mut cur: &[u8] = &img;
        let (rv, risnull) = datum_restore(ctx.mcx(), &mut cur).unwrap();
        let mut cbytes = img;
        let mut ccur: *mut u8 = cbytes.as_mut_ptr();
        let mut cisnull = 1i32;
        let cv = unsafe { pg_datumRestore(&mut ccur, &mut cisnull) };
        assert!(!risnull && cisnull == 0);
        let rs = unsafe { core::slice::from_raw_parts(rv.as_usize() as *const u8, N) };
        let cs = unsafe { core::slice::from_raw_parts(cv as *const u8, N) };
        for i in 0..N {
            assert!(rs[i] == cs[i]);
        }
        core::mem::forget(ctx);
    }

    // ---------------- roundtrip (Rust-only property) ----------------

    /// serialize→restore identity across the three header classes
    /// (null / by-val / by-ref), Rust side only — the dual-exec harnesses
    /// above pin each side to C; this pins the composition.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn rt_serialize_restore() {
        let ctx = mcx::MemoryContext::new_bump("kani-datum");

        // by-val cell
        let v: u64 = kani::any();
        let mut out = mcx::vec_with_capacity_in::<u8>(ctx.mcx(), 16).unwrap();
        datum_serialize(Datum::from_u64(v), false, true, 8, &mut out).unwrap();
        let mut cur: &[u8] = &out;
        let (rv, risnull) = datum_restore(ctx.mcx(), &mut cur).unwrap();
        assert!(!risnull && rv.as_u64() == v && cur.is_empty());
        core::mem::forget(out);

        // null cell
        let mut out = mcx::vec_with_capacity_in::<u8>(ctx.mcx(), 16).unwrap();
        datum_serialize(Datum::from_u64(v), true, true, 8, &mut out).unwrap();
        let mut cur: &[u8] = &out;
        let (_, risnull) = datum_restore(ctx.mcx(), &mut cur).unwrap();
        assert!(risnull && cur.is_empty());
        core::mem::forget(out);

        // by-ref cell (typlen 3)
        let payload: [u8; 3] = kani::any();
        let p = payload.as_ptr() as usize;
        let mut out = mcx::vec_with_capacity_in::<u8>(ctx.mcx(), 16).unwrap();
        datum_serialize(Datum::from_usize(p), false, false, 3, &mut out).unwrap();
        let mut cur: &[u8] = &out;
        let (rv, risnull) = datum_restore(ctx.mcx(), &mut cur).unwrap();
        assert!(!risnull && cur.is_empty());
        let rs = unsafe { core::slice::from_raw_parts(rv.as_usize() as *const u8, 3) };
        for i in 0..3 {
            assert!(rs[i] == payload[i]);
        }
        core::mem::forget(out);
        core::mem::forget(ctx);
    }

    // ---------------- full_xid_from_allowable_at (adt/xid8funcs) ----------------

    /// PROVED full-domain modulo the transam.h Assert fence: nextFullXid and
    /// xid both fully symbolic EXCEPT the C-Assert-violating precondition
    /// region (epoch==0 && xid > lo32(nextFullXid)), where the Rust
    /// debug_assert! is the same ported-in constraint C compiles out
    /// (debug-assert-masking law: fenced, not proved). Oracle = verbatim
    /// transam.h FullTransactionIdFromAllowableAt (see c/pg_datum.c tail).
    #[kani::proof]
    fn eq_fxid_from_allowable_at() {
        let next: u64 = kani::any();
        let xid: u32 = kani::any();
        // transam.h precondition fence (Assert(epoch != 0) in the C body):
        kani::assume(!((next >> 32) == 0 && xid > next as u32));
        let r = ::xid8funcs::full_xid_from_allowable_at(next, xid);
        let c = unsafe { pg_fxid_from_allowable_at(next, xid) };
        assert!(r == c);
    }

    // ---------------- negative control ----------------

    /// MUST FAIL (DEFAULT solver): compares Rust typlen-5 size against C
    /// typlen-6 — proves the rig (C linkage, err-flag plumbing, assert
    /// plane) is non-vacuous. A passing control is a broken gate.
    #[kani::proof]
    fn control_dgs_fixedlen_skew_must_fail() {
        let v: u64 = kani::any();
        let r = datum_get_size(Datum::from_u64(v), false, 5).unwrap();
        let mut err = 0i32;
        let c = unsafe { pg_datumGetSize(v as usize, 0, 6, &mut err) };
        assert!(err == 0);
        assert!(r == c); // intended failure: 5 != 6
    }
}
