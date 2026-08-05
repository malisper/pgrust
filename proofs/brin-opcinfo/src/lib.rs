//! Kani C≡Rust equivalence: BRIN opclass opcinfo metadata procs
//! (w2-fmgr lane, 2026-07-30).
//!
//! Ledger rows: 4591 brin_bloom_opcinfo, 4616 brin_minmax_multi_opcinfo,
//! 4105 brin_inclusion_opcinfo. Direct mirrors of the PROVED 3383
//! brin_minmax_opcinfo pattern (proofs/brin-minmax eq_opcinfo, 0.12s):
//! full symbolic typoid, metadata parity (nstored / regular_nulls /
//! per-column stored typids), catalog typcache seam modeled as a named
//! static carrying only the REQUESTED type_id (typcache machinery out of
//! scope, the requested oid in-theorem). The Rust side's opaque state
//! (BloomOpaque/InclusionOpaque behind Option<Box<..>>) is lazily
//! initialized allocation layout on the C side, not compared — the kind
//! discriminant stands in for "which opaque family" and IS asserted.
//!
//! Negative control: control_bloom_wrong_summary_oid asserts the bloom
//! summary column stores the MINMAX-MULTI summary oid — MUST FAIL,
//! proving the typid assertions bite. DEFAULT solver for the control.
//!
//! Run (expected fast-class, <1s each):
//!   cd proofs/brin-opcinfo
//!   timeout 30 cargo kani -Z c-ffi --c-lib c/pg_brin_opcinfo.c \
//!       --solver kissat --harness proofs::<h> --exact

#[cfg(kani)]
mod proofs {
    use std::os::raw::c_int;
    use types_core::Oid;

    extern "C" {
        fn pg_run_bloom_opcinfo(
            typoid: Oid,
            nstored: *mut u16,
            regular_nulls: *mut c_int,
            typid0: *mut Oid,
        ) -> c_int;
        fn pg_run_mmm_opcinfo(
            typoid: Oid,
            nstored: *mut u16,
            regular_nulls: *mut c_int,
            typid0: *mut Oid,
        ) -> c_int;
        fn pg_run_inclusion_opcinfo(
            typoid: Oid,
            nstored: *mut u16,
            regular_nulls: *mut c_int,
            typid0: *mut Oid,
            typid1: *mut Oid,
            typid2: *mut Oid,
        ) -> c_int;
    }

    #[kani::proof]
    fn eq_bloom_opcinfo() {
        let typoid: Oid = kani::any();
        let r = brin_bloom::brin_bloom_opcinfo(typoid);

        let (mut ns, mut rn, mut t0) = (0u16, 0 as c_int, 0 as Oid);
        unsafe { pg_run_bloom_opcinfo(typoid, &mut ns, &mut rn, &mut t0) };

        assert!(r.oi_nstored == ns);
        assert!(r.oi_regular_nulls as c_int == rn);
        assert!(r.oi_typids[0] == t0);
        assert!(matches!(r.kind, types_brin::BrinOpcKind::Bloom));
        assert!(r.bloom.is_some() && r.inclusion.is_none());
        core::mem::forget(r);
    }

    #[kani::proof]
    fn eq_minmax_multi_opcinfo() {
        let typoid: Oid = kani::any();
        let r = brin_minmax_multi::brin_minmax_multi_opcinfo(typoid);

        let (mut ns, mut rn, mut t0) = (0u16, 0 as c_int, 0 as Oid);
        unsafe { pg_run_mmm_opcinfo(typoid, &mut ns, &mut rn, &mut t0) };

        assert!(r.oi_nstored == ns);
        assert!(r.oi_regular_nulls as c_int == rn);
        assert!(r.oi_typids[0] == t0);
        assert!(matches!(r.kind, types_brin::BrinOpcKind::MinMaxMulti));
        assert!(r.bloom.is_none() && r.inclusion.is_none());
        core::mem::forget(r);
    }

    #[kani::proof]
    fn eq_inclusion_opcinfo() {
        let typoid: Oid = kani::any();
        let r = brin_inclusion::brin_inclusion_opcinfo(typoid);

        let (mut ns, mut rn) = (0u16, 0 as c_int);
        let (mut t0, mut t1, mut t2) = (0 as Oid, 0 as Oid, 0 as Oid);
        unsafe {
            pg_run_inclusion_opcinfo(typoid, &mut ns, &mut rn, &mut t0, &mut t1, &mut t2)
        };

        assert!(r.oi_nstored == ns);
        assert!(r.oi_regular_nulls as c_int == rn);
        assert!(r.oi_typids[0] == t0);
        assert!(r.oi_typids[1] == t1);
        assert!(r.oi_typids[2] == t2);
        assert!(matches!(r.kind, types_brin::BrinOpcKind::Inclusion));
        assert!(r.inclusion.is_some() && r.bloom.is_none());
        core::mem::forget(r);
    }

    /// MUST FAIL: asserts the bloom summary column stores the minmax-multi
    /// summary oid (4601) — proves the typid parity assertions are live.
    /// DEFAULT solver (kissat never terminates on failing harnesses).
    #[kani::proof]
    fn control_bloom_wrong_summary_oid() {
        let typoid: Oid = kani::any();
        let r = brin_bloom::brin_bloom_opcinfo(typoid);
        assert!(
            r.oi_typids[0] == types_brin::PG_BRIN_MINMAX_MULTI_SUMMARYOID,
            "expected failure: bloom stores 4600, not 4601 (rig is live)"
        );
        core::mem::forget(r);
    }
}
