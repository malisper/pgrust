//! Kani C≡Rust equivalence: SP-GiST point/box opclasses + text config.
//!
//! Ledger rows hosted here:
//!   4018 spg_quad_config, 4019 spg_quad_choose, 4021 spg_quad_inner_consistent,
//!   4022 spg_quad_leaf_consistent (spgquadtreeproc.c);
//!   4023 spg_kd_config, 4024 spg_kd_choose, 4026 spg_kd_inner_consistent
//!   (spgkdtreeproc.c);
//!   5010 spg_bbox_quad_config, 5011 spg_poly_quad_compress,
//!   5012 spg_box_quad_config, 5013 spg_box_quad_choose,
//!   5015 spg_box_quad_inner_consistent, 5016 spg_box_quad_leaf_consistent
//!   (geo_spgist.c);
//!   4027 spg_text_config (spgtextproc.c).
//!
//! Both sides at the fmgr level: Rust through the shipped *_BUILTINS fc_*
//! entries on a real LocalFcinfo frame; C through plain (in, out) wrappers
//! around verbatim bodies (c/pg_spgist_geo.c; see its header for every
//! shim, incl. the mandatory canonical-NAN shim).
//!
//! Float plane: coordinates are FULLY SYMBOLIC f64 (NaN and ±0 planes in),
//! per the geo-family bug history.  Kani float harness rule: run with
//! --no-overflow-checks is NOT needed (no float arithmetic here beyond
//! FP-epsilon adds, which are ordinary IEEE adds — overflow checks do not
//! apply to floats).
//!
//! Fences (documented caller/datatype invariants, never narrowings of a
//! defined plane; each stated in the harness):
//!   - norderbys == 0 everywhere (KNN distance machinery out of scope; the
//!     C side sets a loud pg_spg_trap if an orderby arm is reached).
//!   - strategies fenced to each opclass's recognized set (elog on other
//!     values both sides: C errflag / Rust panic).
//!   - the quadtree "impossible quadrant" plane (NaN coordinates can fail
//!     all four quadrant gates): C elogs, Rust panics.  Main harnesses
//!     assume the C errflag clean; eq_quad_getquadrant_err_plane proves the
//!     Rust gates all fail exactly there (panic-parity), and the cover
//!     harness witnesses the plane is reachable.
//!   - out structs zero-initialized (spgdoinsert/spgWalk memset contract).

#![allow(non_snake_case)]

#[cfg(kani)]
mod proofs {
    use datum::Datum;
    use mcx::{Mcx, MemoryContext};
    // Load-bearing for #[kani::stub] path resolution.
    #[allow(unused_imports)]
    use proof_support;
    use std::os::raw::c_int;
    use types_scan::scankey::ScanKeyData;
    use types_spgist::spgConfigOut;
    use types_spgist::state::{
        spgChooseIn, spgChooseOut, spgInnerConsistentIn, spgInnerConsistentOut,
        spgLeafConsistentIn, spgLeafConsistentOut,
    };

    // ---- C-side protocol mirrors (repr(C); bool rides as u8) ----

    #[repr(C)]
    struct CConfigOut {
        prefixType: u32,
        labelType: u32,
        leafType: u32,
        canReturnData: u8,
        longValuesOK: u8,
    }

    impl CConfigOut {
        fn zeroed() -> Self {
            CConfigOut {
                prefixType: 0,
                labelType: 0,
                leafType: 0,
                canReturnData: 0,
                longValuesOK: 0,
            }
        }
    }

    #[repr(C)]
    struct CChooseIn {
        datum: usize,
        leafDatum: usize,
        level: c_int,
        allTheSame: u8,
        hasPrefix: u8,
        prefixDatum: usize,
        nNodes: c_int,
        nodeLabels: usize,
    }

    /// MatchNode-only mirror (every function here emits MatchNode).
    #[repr(C)]
    struct CChooseOut {
        resultType: c_int,
        nodeN: c_int,
        levelAdd: c_int,
        restDatum: usize,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct CScanKey {
        sk_strategy: u16,
        sk_subtype: u32,
        sk_argument: usize,
    }

    #[repr(C)]
    struct CInnerIn {
        scankeys: usize,
        orderbys: usize,
        nkeys: c_int,
        norderbys: c_int,
        reconstructedValue: usize,
        traversalValue: usize,
        traversalMemoryContext: usize,
        level: c_int,
        returnData: u8,
        allTheSame: u8,
        hasPrefix: u8,
        prefixDatum: usize,
        nNodes: c_int,
        nodeLabels: usize,
    }

    /// Fixed-frame mirror of the palloc'd out arrays (named-slot law).
    #[repr(C)]
    struct CInnerOut {
        nNodes: c_int,
        nodeNumbers: [c_int; 16],
        levelAdds: [c_int; 16],
    }

    impl CInnerOut {
        fn zeroed() -> Self {
            CInnerOut { nNodes: 0, nodeNumbers: [0; 16], levelAdds: [0; 16] }
        }
    }

    #[repr(C)]
    struct CLeafIn {
        scankeys: usize,
        orderbys: usize,
        nkeys: c_int,
        norderbys: c_int,
        reconstructedValue: usize,
        traversalValue: usize,
        level: c_int,
        returnData: u8,
        leafDatum: usize,
    }

    #[repr(C)]
    struct CLeafOut {
        leafValue: usize,
        recheck: u8,
        recheckDistances: u8,
    }

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct CBox {
        hx: f64,
        hy: f64,
        lx: f64,
        ly: f64,
    }

    extern "C" {
        static mut pg_spg_trap: c_int;
        fn pg_quad_getQuadrant(cx: f64, cy: f64, tx: f64, ty: f64, q: *mut i16) -> c_int;
        fn pg_spg_quad_config(cfg: *mut CConfigOut) -> c_int;
        fn pg_spg_quad_choose(input: *const CChooseIn, out: *mut CChooseOut) -> c_int;
        fn pg_spg_quad_inner_consistent(input: *const CInnerIn, out: *mut CInnerOut) -> c_int;
        fn pg_spg_quad_leaf_consistent(
            input: *const CLeafIn,
            out: *mut CLeafOut,
            res: *mut c_int,
        ) -> c_int;
        fn pg_spg_kd_config(cfg: *mut CConfigOut) -> c_int;
        fn pg_spg_kd_choose(
            input: *const CChooseIn,
            prefix_coord: f64,
            out: *mut CChooseOut,
        ) -> c_int;
        fn pg_spg_kd_inner_consistent(
            input: *const CInnerIn,
            prefix_coord: f64,
            out: *mut CInnerOut,
        ) -> c_int;
        fn pg_spg_box_quad_config(cfg: *mut CConfigOut) -> c_int;
        fn pg_spg_bbox_quad_config(cfg: *mut CConfigOut) -> c_int;
        fn pg_spg_box_quad_choose(input: *const CChooseIn, out: *mut CChooseOut) -> c_int;
        fn pg_spg_box_quad_inner_consistent(input: *const CInnerIn, out: *mut CInnerOut)
            -> c_int;
        fn pg_spg_box_quad_leaf_consistent(
            input: *const CLeafIn,
            out: *mut CLeafOut,
            res: *mut c_int,
        ) -> c_int;
        fn pg_spg_poly_quad_compress(polygon_image: *const u8, b: *mut CBox) -> c_int;
        fn pg_spg_text_config(cfg: *mut CConfigOut) -> c_int;
    }

    const BOXOID: u32 = 603;
    const POLYGONOID: u32 = 604;

    // point-opclass strategy set (quad + kd): Left/Right/Same/Below/Above/
    // OldBelow/OldAbove/ContainedBy
    fn assume_point_strategy(s: u16) {
        kani::assume(
            s == 1 || s == 5 || s == 6 || s == 10 || s == 11 || s == 29 || s == 30 || s == 8,
        );
    }

    // box-opclass strategy set: RT 1..=12
    fn assume_box_strategy(s: u16) {
        kani::assume((1..=12).contains(&s));
    }

    // =====================================================================
    // shared scaffolding (mirrors proofs/spgist-inet)
    // =====================================================================

    macro_rules! sg_proof {
        ($(#[$attr:meta])* fn $name:ident() $body:block) => {
            #[kani::proof]
            // Family-wide unwind bound: mcx AcctWeak-registry retain loops
            // (empty-vec, infeasible-deep) hang symex without a bound
            // (measured in proofs/spgist-inet 2026-07-30); also bounds the
            // <=16-iteration opclass loops (+ slack for scankey loops).
            #[kani::unwind(18)]
            $(#[$attr])*
            #[kani::stub(mcx::Mcx::allocate, proof_support::mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, proof_support::mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, proof_support::mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(mcx::vec_with_capacity_in, proof_support::mcx_stubs::stub_vec_with_capacity_in)]
            #[kani::stub(mcx::local_pool_on, stub_local_pool_on)]
            #[kani::stub(types_error::PgError::error, proof_support::stub_pg_error_error)]
            #[kani::stub(alloc::fmt::format, proof_support::stub_format)]
            #[kani::stub(std::env::var, proof_support::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, proof_support::stub_once_lock_get_or_init)]
            fn $name() $body
        };
    }

    /// Stub for `mcx::local_pool_on` (OnceLock machinery walls symex); pool
    /// selection is allocation strategy, out of every claim.
    #[allow(dead_code)]
    pub fn stub_local_pool_on() -> bool {
        false
    }

    fn dptr<T>(p: *const T) -> Datum {
        Datum::from_usize(p as usize)
    }

    /// Invoke a shipped fc_* builtin on a real 2-arg frame.
    fn call_fc2(
        mcx: Mcx<'_>,
        entry: &types_fmgr::FmgrBuiltin,
        foid: u32,
        arg0: Datum,
        arg1: Datum,
    ) -> Datum {
        assert!(entry.foid == foid, "builtins table order changed");
        let mut f = proof_support::fci([arg0, arg1]);
        // SAFETY: harness context outlives the call and result reads.
        unsafe { f.set_result_mcx(mcx) };
        match (entry.func)(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("spgist support fn errored under proof-heap allocator");
            }
        }
    }

    fn call_fc1(mcx: Mcx<'_>, entry: &types_fmgr::FmgrBuiltin, foid: u32, arg0: Datum) -> Datum {
        assert!(entry.foid == foid, "builtins table order changed");
        let mut f = proof_support::fci([arg0]);
        // SAFETY: as call_fc2.
        unsafe { f.set_result_mcx(mcx) };
        match (entry.func)(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("spgist support fn errored under proof-heap allocator");
            }
        }
    }

    fn with_ctx<R>(f: impl FnOnce(Mcx<'_>) -> R) -> R {
        let ctx = MemoryContext::new_bump("proof");
        let r = f(ctx.mcx());
        core::mem::forget(ctx);
        r
    }

    fn no_trap() {
        // SAFETY: single-threaded harness; C sets this on out-of-plane arms.
        unsafe {
            assert!(pg_spg_trap == 0, "orderby machinery reached under norderbys==0 fence");
        }
    }

    fn scankey(strategy: u16, subtype: u32, arg: Datum) -> ScanKeyData {
        let mut k = ScanKeyData::empty();
        k.sk_strategy = strategy;
        k.sk_subtype = subtype;
        k.sk_argument = arg;
        k
    }

    fn assert_config_eq(c: &CConfigOut, r: &spgConfigOut) {
        assert!(c.prefixType == r.prefixType);
        assert!(c.labelType == r.labelType);
        assert!(c.leafType == r.leafType, "leafType (incl untouched-zero) parity");
        assert!((c.canReturnData != 0) == r.canReturnData);
        assert!((c.longValuesOK != 0) == r.longValuesOK);
    }

    /// nodeN/levelAdd/restDatum parity for the MatchNode-only choose shape.
    fn assert_choose_match(c: &CChooseOut, r: &spgChooseOut, skip_nodeN: bool) {
        assert!(c.resultType == 1, "C wrote MatchNode");
        match *r {
            spgChooseOut::MatchNode { nodeN, levelAdd, restDatum } => {
                if !skip_nodeN {
                    assert!(c.nodeN == nodeN, "nodeN mismatch");
                }
                assert!(c.levelAdd == levelAdd, "levelAdd mismatch");
                assert!(c.restDatum == restDatum.as_usize(), "restDatum passthrough");
            }
            _ => panic!("Rust choose arm is not MatchNode"),
        }
    }

    // =====================================================================
    // 4018 / 4023 / 5010 / 5012 / 4027: config rows
    // =====================================================================

    /// One config comparison: C fn vs a shipped builtins entry.
    fn run_config(
        c_fn: unsafe extern "C" fn(*mut CConfigOut) -> c_int,
        entry: &types_fmgr::FmgrBuiltin,
        foid: u32,
        arg0: Datum,
    ) {
        let mut c_out = CConfigOut::zeroed();
        unsafe { c_fn(&mut c_out) };
        with_ctx(|mcx| {
            let mut r_out = spgConfigOut::default();
            let _ = call_fc2(mcx, entry, foid, arg0, dptr(&mut r_out as *mut spgConfigOut));
            assert_config_eq(&c_out, &r_out);
        });
    }

    // One harness per config row: five chained fmgr frames in one harness
    // is a symex wall (measured >450s; single-config ~1 frame is the
    // proven-cheap inet shape).

    sg_proof! {
        /// row 4018: literal spgConfigOut stores.
        fn eq_spg_quad_config_h() {
            let cfgin = types_spgist::spgConfigIn { attType: kani::any() };
            let a0 = dptr(&cfgin as *const types_spgist::spgConfigIn);
            run_config(pg_spg_quad_config, &spgist_quadtree::SPGIST_QUAD_BUILTINS[0], 4018, a0);
        }
    }

    sg_proof! {
        /// row 4023: literal spgConfigOut stores.
        fn eq_spg_kd_config_h() {
            let cfgin = types_spgist::spgConfigIn { attType: kani::any() };
            let a0 = dptr(&cfgin as *const types_spgist::spgConfigIn);
            run_config(pg_spg_kd_config, &spgist_kdtree::SPGIST_KD_BUILTINS[0], 4023, a0);
        }
    }

    sg_proof! {
        /// row 5012: literal spgConfigOut stores.
        fn eq_spg_box_config_h() {
            let cfgin = types_spgist::spgConfigIn { attType: kani::any() };
            let a0 = dptr(&cfgin as *const types_spgist::spgConfigIn);
            run_config(pg_spg_box_quad_config, &spgist_box::SPGIST_BOX_BUILTINS[2], 5012, a0);
        }
    }

    sg_proof! {
        /// row 5010: literal stores incl the leafType split vs 5012.
        fn eq_spg_bbox_config_h() {
            let cfgin = types_spgist::spgConfigIn { attType: kani::any() };
            let a0 = dptr(&cfgin as *const types_spgist::spgConfigIn);
            run_config(pg_spg_bbox_quad_config, &spgist_box::SPGIST_BOX_BUILTINS[0], 5010, a0);
        }
    }

    sg_proof! {
        /// row 4027 at the CORE level (spg_text_config is pub): touching
        /// SPGIST_TEXT_BUILTINS reaches fc_spghandler -> the whole AM ->
        /// ipc_seams::proc_exit, a Kani codegen ICE. The fc_ shell is the
        /// same one-liner shape as the four fmgr-level config proofs.
        fn eq_spg_text_config_core() {
            let cfgin = types_spgist::spgConfigIn { attType: kani::any() };
            let mut c_out = CConfigOut::zeroed();
            unsafe { pg_spg_text_config(&mut c_out) };
            let mut r_out = spgConfigOut::default();
            spgist_text::spg_text_config(&cfgin, &mut r_out);
            assert_config_eq(&c_out, &r_out);
        }
    }

    // =====================================================================
    // 4019 spg_quad_choose
    // =====================================================================

    sg_proof! {
        /// row 4019: quadrant selection over fully symbolic f64 point +
        /// centroid (NaN/±0 planes in) + the allTheSame passthrough arm.
        /// Impossible-quadrant plane fenced to C-clean (see err-plane thm).
        fn eq_spg_quad_choose() {
            let pt: [f64; 2] = kani::any();
            let centroid: [f64; 2] = kani::any();
            let all_the_same: bool = kani::any();

            let c_in = CChooseIn {
                datum: pt.as_ptr() as usize,
                leafDatum: 0,
                level: kani::any(),
                allTheSame: all_the_same as u8,
                hasPrefix: 1,
                prefixDatum: centroid.as_ptr() as usize,
                nNodes: 4,
                nodeLabels: 0,
            };
            // SAFETY: all-zero is the caller memset contract.
            let mut c_out: CChooseOut = unsafe { core::mem::zeroed() };
            let c_err = unsafe { pg_spg_quad_choose(&c_in, &mut c_out) };
            // Fence: the impossible-quadrant plane (C elog / Rust panic);
            // panic-parity proven by eq_quad_getquadrant_err_plane.
            kani::assume(c_err == 0);

            with_ctx(|mcx| {
                let r_in = spgChooseIn {
                    datum: dptr(pt.as_ptr()),
                    leafDatum: Datum::null(),
                    level: c_in.level,
                    allTheSame: all_the_same,
                    hasPrefix: true,
                    prefixDatum: dptr(centroid.as_ptr()),
                    nNodes: 4,
                    nodeLabels: core::ptr::null(),
                };
                let mut r_out = spgChooseOut::None;
                let _ = call_fc2(
                    mcx,
                    &spgist_quadtree::SPGIST_QUAD_BUILTINS[1],
                    4019,
                    dptr(&r_in as *const spgChooseIn),
                    dptr(&mut r_out as *mut spgChooseOut),
                );
                // allTheSame: C leaves nodeN zero-init ("set by core");
                // Rust writes 0 — same value under the memset contract.
                assert_choose_match(&c_out, &r_out, false);
            });
        }
    }

    sg_proof! {
        /// row 4019 err-plane panic-parity: where C's getQuadrant elogs
        /// (impossible case), the shipped Rust predicates fail all four
        /// quadrant gates too — i.e. Rust getQuadrant would panic exactly
        /// there. Cover witnesses the plane is real (NaN coords reach it).
        fn eq_quad_getquadrant_err_plane() {
            use adt_geo::point::{point_above, point_below, point_horiz, point_left,
                point_right, point_vert};
            use types_core::geo::Point;

            let cx: f64 = kani::any();
            let cy: f64 = kani::any();
            let tx: f64 = kani::any();
            let ty: f64 = kani::any();
            let mut q: i16 = 0;
            let c_err = unsafe { pg_quad_getQuadrant(cx, cy, tx, ty, &mut q) };
            kani::cover!(c_err != 0, "impossible-quadrant plane reachable");

            let c = Point { x: cx, y: cy };
            let t = Point { x: tx, y: ty };
            let gate1 = (point_above(&t, &c) || point_horiz(&t, &c))
                && (point_right(&t, &c) || point_vert(&t, &c));
            let gate2 = point_below(&t, &c) && (point_right(&t, &c) || point_vert(&t, &c));
            let gate3 = (point_below(&t, &c) || point_horiz(&t, &c)) && point_left(&t, &c);
            let gate4 = point_above(&t, &c) && point_left(&t, &c);
            if c_err != 0 {
                assert!(!gate1 && !gate2 && !gate3 && !gate4, "Rust would not panic where C elogs");
            } else {
                assert!(gate1 || gate2 || gate3 || gate4, "Rust would panic where C succeeds");
            }
        }
    }

    // =====================================================================
    // 4022 spg_quad_leaf_consistent
    // =====================================================================

    sg_proof! {
        /// row 4022: verdict + recheck + leafValue passthrough, nkeys<=1,
        /// point-opclass strategies, fully symbolic coordinates (the
        /// ContainedBy arm reads the argument as a box: 4 symbolic f64s).
        /// Fences: norderbys=0 (trap-checked), RTSame's getQuadrant not
        /// reached (leaf uses point_eq, no quadrant call).
        fn eq_spg_quad_leaf() {
            let leaf: [f64; 2] = kani::any();
            let arg: [f64; 4] = kani::any(); // point reads [0..2], box all 4
            let s0: u16 = kani::any();
            assume_point_strategy(s0);
            let nkeys: i32 = kani::any();
            kani::assume((0..=1).contains(&nkeys));

            let keys_c = [CScanKey { sk_strategy: s0, sk_subtype: 0, sk_argument: arg.as_ptr() as usize }];
            let c_in = CLeafIn {
                scankeys: keys_c.as_ptr() as usize,
                orderbys: 0,
                nkeys,
                norderbys: 0,
                reconstructedValue: 0,
                traversalValue: 0,
                level: 0,
                returnData: 0,
                leafDatum: leaf.as_ptr() as usize,
            };
            // SAFETY: zeroed per caller memset contract.
            let mut c_out: CLeafOut = unsafe { core::mem::zeroed() };
            let mut c_res: c_int = 0;
            let c_err = unsafe { pg_spg_quad_leaf_consistent(&c_in, &mut c_out, &mut c_res) };
            kani::assume(c_err == 0);
            no_trap();

            with_ctx(|mcx| {
                let keys_r = [scankey(s0, 0, dptr(arg.as_ptr()))];
                let r_in = spgLeafConsistentIn {
                    scankeys: keys_r.as_ptr(),
                    orderbys: core::ptr::null(),
                    nkeys,
                    norderbys: 0,
                    reconstructedValue: Datum::null(),
                    traversalValue: 0,
                    level: 0,
                    returnData: false,
                    leafDatum: dptr(leaf.as_ptr()),
                };
                let mut r_out = spgLeafConsistentOut {
                    leafValue: Datum::null(),
                    recheck: false,
                    recheckDistances: false,
                    distances: core::ptr::null(),
                };
                let d = call_fc2(
                    mcx,
                    &spgist_quadtree::SPGIST_QUAD_BUILTINS[4],
                    4022,
                    dptr(&r_in as *const spgLeafConsistentIn),
                    dptr(&mut r_out as *mut spgLeafConsistentOut),
                );
                assert!((c_res != 0) == d.as_bool(), "leaf verdict mismatch");
                assert!((c_out.recheck != 0) == r_out.recheck, "recheck mismatch");
                assert!(c_out.leafValue == r_out.leafValue.as_usize(), "leafValue passthrough");
            });
        }
    }

    // =====================================================================
    // 4021 spg_quad_inner_consistent
    // =====================================================================

    fn run_inner_common(
        c_fn: impl FnOnce(&CInnerIn, &mut CInnerOut) -> c_int,
        entry: &types_fmgr::FmgrBuiltin,
        foid: u32,
        mcx: Mcx<'_>,
        keys_r: &[ScanKeyData],
        keys_c: &[CScanKey],
        nkeys: i32,
        level: i32,
        all_the_same: bool,
        has_prefix: bool,
        n_nodes: i32,
        prefix: Datum,
        prefix_c: usize,
        compare_level_adds: usize,
    ) {
        let c_in = CInnerIn {
            scankeys: keys_c.as_ptr() as usize,
            orderbys: 0,
            nkeys,
            norderbys: 0,
            reconstructedValue: 0,
            traversalValue: 0,
            traversalMemoryContext: 0,
            level,
            returnData: 0,
            allTheSame: all_the_same as u8,
            hasPrefix: has_prefix as u8,
            prefixDatum: prefix_c,
            nNodes: n_nodes,
            nodeLabels: 0,
        };
        let mut c_out = CInnerOut::zeroed();
        let c_err = c_fn(&c_in, &mut c_out);
        kani::assume(c_err == 0);
        no_trap();

        let r_in = spgInnerConsistentIn {
            scankeys: keys_r.as_ptr(),
            orderbys: core::ptr::null(),
            nkeys,
            norderbys: 0,
            reconstructedValue: Datum::null(),
            traversalValue: 0,
            traversalMemoryContext: mcx,
            level,
            returnData: false,
            allTheSame: all_the_same,
            hasPrefix: has_prefix,
            prefixDatum: prefix,
            nNodes: n_nodes,
            nodeLabels: core::ptr::null(),
        };
        let mut r_out = spgInnerConsistentOut::default();
        let _ = call_fc2(
            mcx,
            entry,
            foid,
            dptr(&r_in as *const spgInnerConsistentIn),
            dptr(&mut r_out as *mut spgInnerConsistentOut),
        );

        assert!(c_out.nNodes == r_out.nNodes, "visited node count mismatch");
        let mut i = 0;
        while i < c_out.nNodes as usize {
            // SAFETY: nNodes entries written (C fixed frame / Rust proof heap).
            let rn = unsafe { *r_out.nodeNumbers.add(i) };
            assert!(c_out.nodeNumbers[i] == rn, "nodeNumbers entry mismatch");
            i += 1;
        }
        if compare_level_adds > 0 && !r_out.levelAdds.is_null() {
            let mut i = 0;
            while i < compare_level_adds {
                // SAFETY: fixed-count levelAdds written by both sides.
                let ra = unsafe { *r_out.levelAdds.add(i) };
                assert!(c_out.levelAdds[i] == ra, "levelAdds entry mismatch");
                i += 1;
            }
        }
    }

    sg_proof! {
        /// row 4021: which-bitmask lattice, nkeys<=1 point-opclass
        /// strategies (incl RTSame's centroid-quadrant call and the
        /// ContainedBy 4-corner fold), fully symbolic coords; plus the
        /// allTheSame visit-everything arm. Fences: norderbys=0,
        /// C-clean err plane (RTSame/corner getQuadrant can elog on NaN).
        fn eq_spg_quad_inner() {
            let centroid: [f64; 2] = kani::any();
            let arg: [f64; 4] = kani::any();
            let s0: u16 = kani::any();
            assume_point_strategy(s0);
            let nkeys: i32 = kani::any();
            kani::assume((0..=1).contains(&nkeys));
            let all_the_same: bool = kani::any();

            let keys_c = [CScanKey { sk_strategy: s0, sk_subtype: 0, sk_argument: arg.as_ptr() as usize }];
            with_ctx(|mcx| {
                let keys_r = [scankey(s0, 0, dptr(arg.as_ptr()))];
                run_inner_common(
                    |i, o| unsafe { pg_spg_quad_inner_consistent(i, o) },
                    &spgist_quadtree::SPGIST_QUAD_BUILTINS[3],
                    4021,
                    mcx,
                    &keys_r,
                    &keys_c,
                    nkeys,
                    0,
                    all_the_same,
                    true,
                    4,
                    dptr(centroid.as_ptr()),
                    centroid.as_ptr() as usize,
                    if all_the_same { 0 } else { 4 },
                );
            });
        }
    }

    // =====================================================================
    // 4024 spg_kd_choose
    // =====================================================================

    sg_proof! {
        /// row 4024: axis pick (level%2) + getSide over fully symbolic
        /// point/coord (NaN plane in: NaN coord compares all-false ->
        /// nodeN 1 both sides). Fences: allTheSame=false (C elog / Rust
        /// panic — same plane, out of theorem), hasPrefix, nNodes=2.
        fn eq_spg_kd_choose() {
            let pt: [f64; 2] = kani::any();
            let coord: f64 = kani::any();
            let level: i32 = kani::any();
            kani::assume(level >= 0);

            let c_in = CChooseIn {
                datum: pt.as_ptr() as usize,
                leafDatum: 0,
                level,
                allTheSame: 0,
                hasPrefix: 1,
                prefixDatum: 0, // by-value f64 datum rides the extra C arg
                nNodes: 2,
                nodeLabels: 0,
            };
            // SAFETY: caller memset contract.
            let mut c_out: CChooseOut = unsafe { core::mem::zeroed() };
            let c_err = unsafe { pg_spg_kd_choose(&c_in, coord, &mut c_out) };
            assert!(c_err == 0);

            with_ctx(|mcx| {
                let r_in = spgChooseIn {
                    datum: dptr(pt.as_ptr()),
                    leafDatum: Datum::null(),
                    level,
                    allTheSame: false,
                    hasPrefix: true,
                    prefixDatum: Datum::from_f64(coord),
                    nNodes: 2,
                    nodeLabels: core::ptr::null(),
                };
                let mut r_out = spgChooseOut::None;
                let _ = call_fc2(
                    mcx,
                    &spgist_kdtree::SPGIST_KD_BUILTINS[1],
                    4024,
                    dptr(&r_in as *const spgChooseIn),
                    dptr(&mut r_out as *mut spgChooseOut),
                );
                assert_choose_match(&c_out, &r_out, false);
            });
        }
    }

    // =====================================================================
    // 4026 spg_kd_inner_consistent
    // =====================================================================

    sg_proof! {
        /// row 4026: FP-epsilon lattice + axis split (level%2) + hi/lo
        /// ContainedBy arm, nkeys<=1 point-opclass strategies, fully
        /// symbolic coords/coord. Fences: norderbys=0, allTheSame=false,
        /// level>=0.
        fn eq_spg_kd_inner() {
            let coord: f64 = kani::any();
            let arg: [f64; 4] = kani::any();
            let s0: u16 = kani::any();
            assume_point_strategy(s0);
            let nkeys: i32 = kani::any();
            kani::assume((0..=1).contains(&nkeys));
            let level: i32 = kani::any();
            kani::assume(level >= 0);

            let keys_c = [CScanKey { sk_strategy: s0, sk_subtype: 0, sk_argument: arg.as_ptr() as usize }];
            with_ctx(|mcx| {
                let keys_r = [scankey(s0, 0, dptr(arg.as_ptr()))];
                run_inner_common(
                    |i, o| unsafe { pg_spg_kd_inner_consistent(i, coord, o) },
                    &spgist_kdtree::SPGIST_KD_BUILTINS[3],
                    4026,
                    mcx,
                    &keys_r,
                    &keys_c,
                    nkeys,
                    level,
                    false,
                    true,
                    2,
                    Datum::from_f64(coord),
                    0,
                    2,
                );
            });
        }
    }

    // =====================================================================
    // 5013 spg_box_quad_choose
    // =====================================================================

    sg_proof! {
        /// row 5013: 4 raw f64 > compares (NO epsilon — NaN plane in, all
        /// compares false) + the allTheSame skip-nodeN arm (C leaves nodeN
        /// zero-init; Rust writes 0 — compared equal under memset contract).
        fn eq_spg_box_quad_choose() {
            let leaf: [f64; 4] = kani::any();
            let centroid: [f64; 4] = kani::any();
            let all_the_same: bool = kani::any();

            let c_in = CChooseIn {
                datum: 0,
                leafDatum: leaf.as_ptr() as usize,
                level: 0,
                allTheSame: all_the_same as u8,
                hasPrefix: 1,
                prefixDatum: centroid.as_ptr() as usize,
                nNodes: 16,
                nodeLabels: 0,
            };
            // SAFETY: caller memset contract.
            let mut c_out: CChooseOut = unsafe { core::mem::zeroed() };
            let c_err = unsafe { pg_spg_box_quad_choose(&c_in, &mut c_out) };
            assert!(c_err == 0);

            with_ctx(|mcx| {
                let r_in = spgChooseIn {
                    datum: Datum::null(),
                    leafDatum: dptr(leaf.as_ptr()),
                    level: 0,
                    allTheSame: all_the_same,
                    hasPrefix: true,
                    prefixDatum: dptr(centroid.as_ptr()),
                    nNodes: 16,
                    nodeLabels: core::ptr::null(),
                };
                let mut r_out = spgChooseOut::None;
                let _ = call_fc2(
                    mcx,
                    &spgist_box::SPGIST_BOX_BUILTINS[3],
                    5013,
                    dptr(&r_in as *const spgChooseIn),
                    dptr(&mut r_out as *mut spgChooseOut),
                );
                assert_choose_match(&c_out, &r_out, false);
            });
        }
    }

    // =====================================================================
    // 5016 spg_box_quad_leaf_consistent
    // =====================================================================

    sg_proof! {
        /// row 5016 BOXOID-key plane: verdict + recheck + returnData-gated
        /// leafValue, nkeys<=1, all 12 box strategies, fully symbolic f64s
        /// (box predicates PROVED value-level in geo-cmp; this adds the
        /// fmgr/protocol tier). Polygon-key arm = ladder follow-up.
        fn eq_spg_box_quad_leaf() {
            let leaf: [f64; 4] = kani::any();
            let arg: [f64; 4] = kani::any();
            let s0: u16 = kani::any();
            assume_box_strategy(s0);
            let nkeys: i32 = kani::any();
            kani::assume((0..=1).contains(&nkeys));
            let return_data: bool = kani::any();

            let keys_c = [CScanKey { sk_strategy: s0, sk_subtype: BOXOID, sk_argument: arg.as_ptr() as usize }];
            let c_in = CLeafIn {
                scankeys: keys_c.as_ptr() as usize,
                orderbys: 0,
                nkeys,
                norderbys: 0,
                reconstructedValue: 0,
                traversalValue: 0,
                level: 0,
                returnData: return_data as u8,
                leafDatum: leaf.as_ptr() as usize,
            };
            // SAFETY: caller memset contract.
            let mut c_out: CLeafOut = unsafe { core::mem::zeroed() };
            let mut c_res: c_int = 0;
            let c_err = unsafe { pg_spg_box_quad_leaf_consistent(&c_in, &mut c_out, &mut c_res) };
            assert!(c_err == 0);
            no_trap();

            with_ctx(|mcx| {
                let keys_r = [scankey(s0, BOXOID, dptr(arg.as_ptr()))];
                let r_in = spgLeafConsistentIn {
                    scankeys: keys_r.as_ptr(),
                    orderbys: core::ptr::null(),
                    nkeys,
                    norderbys: 0,
                    reconstructedValue: Datum::null(),
                    traversalValue: 0,
                    level: 0,
                    returnData: return_data,
                    leafDatum: dptr(leaf.as_ptr()),
                };
                let mut r_out = spgLeafConsistentOut {
                    leafValue: Datum::null(),
                    recheck: false,
                    recheckDistances: false,
                    distances: core::ptr::null(),
                };
                let d = call_fc2(
                    mcx,
                    &spgist_box::SPGIST_BOX_BUILTINS[6],
                    5016,
                    dptr(&r_in as *const spgLeafConsistentIn),
                    dptr(&mut r_out as *mut spgLeafConsistentOut),
                );
                assert!((c_res != 0) == d.as_bool(), "leaf verdict mismatch");
                assert!((c_out.recheck != 0) == r_out.recheck, "recheck mismatch");
                assert!(c_out.leafValue == r_out.leafValue.as_usize(), "leafValue gate parity");
            });
        }
    }

    // =====================================================================
    // 5015 spg_box_quad_inner_consistent
    // =====================================================================

    sg_proof! {
        /// row 5015: nextRectBox + 4D FP predicates over the 16-quadrant
        /// loop, nkeys<=1 box strategies, root traversal (traversalValue=0
        /// -> initRectBox both sides), fully symbolic centroid/query; plus
        /// the allTheSame arm. Fences: norderbys=0. Expected ladder tier.
        fn eq_spg_box_quad_inner() {
            let centroid: [f64; 4] = kani::any();
            let arg: [f64; 4] = kani::any();
            let s0: u16 = kani::any();
            assume_box_strategy(s0);
            let nkeys: i32 = kani::any();
            kani::assume((0..=1).contains(&nkeys));
            let all_the_same: bool = kani::any();

            let keys_c = [CScanKey { sk_strategy: s0, sk_subtype: BOXOID, sk_argument: arg.as_ptr() as usize }];
            with_ctx(|mcx| {
                let keys_r = [scankey(s0, BOXOID, dptr(arg.as_ptr()))];
                run_inner_common(
                    |i, o| unsafe { pg_spg_box_quad_inner_consistent(i, o) },
                    &spgist_box::SPGIST_BOX_BUILTINS[5],
                    5015,
                    mcx,
                    &keys_r,
                    &keys_c,
                    nkeys,
                    0,
                    all_the_same,
                    true,
                    16,
                    dptr(centroid.as_ptr()),
                    centroid.as_ptr() as usize,
                    0, // box inner never writes levelAdds (either side)
                );
            });
        }
    }

    // =====================================================================
    // 5011 spg_poly_quad_compress
    // =====================================================================

    /// 4B-header polygon varlena image, npts=1: header, npts, boundbox
    /// (4 f64), one point (2 f64). 8-aligned for the C-side BOX reads.
    #[repr(C, align(8))]
    struct PolyImage {
        bytes: [u8; 56],
    }

    sg_proof! {
        /// row 5011: boundbox slice out of the polygon image -> fresh box
        /// datum; POINTEE image parity (4 f64 bit-compare), npts=1 plane
        /// (boundbox path is npts-independent both sides).
        fn eq_spg_poly_quad_compress() {
            let boundbox: [f64; 4] = kani::any();
            let p0: [f64; 2] = kani::any();
            let mut img = PolyImage { bytes: [0u8; 56] };
            img.bytes[..4].copy_from_slice(&datum::varlena::set_varsize_4b(56));
            img.bytes[4..8].copy_from_slice(&1i32.to_ne_bytes());
            let mut off = 8;
            for v in boundbox {
                img.bytes[off..off + 8].copy_from_slice(&v.to_ne_bytes());
                off += 8;
            }
            for v in p0 {
                img.bytes[off..off + 8].copy_from_slice(&v.to_ne_bytes());
                off += 8;
            }

            let mut c_box = CBox::default();
            unsafe { pg_spg_poly_quad_compress(img.bytes.as_ptr(), &mut c_box) };

            with_ctx(|mcx| {
                let d = call_fc1(
                    mcx,
                    &spgist_box::SPGIST_BOX_BUILTINS[1],
                    5011,
                    dptr(img.bytes.as_ptr()),
                );
                // SAFETY: fresh 4-f64 box image written by the shipped code.
                let r = unsafe { core::slice::from_raw_parts(d.as_usize() as *const f64, 4) };
                assert!(c_box.hx.to_bits() == r[0].to_bits(), "high.x image");
                assert!(c_box.hy.to_bits() == r[1].to_bits(), "high.y image");
                assert!(c_box.lx.to_bits() == r[2].to_bits(), "low.x image");
                assert!(c_box.ly.to_bits() == r[3].to_bits(), "low.y image");
            });
        }
    }

    // =====================================================================
    // rig coverage + control
    // =====================================================================

    sg_proof! {
        /// Regime coverage (vacuity insurance): quad leaf verdict both
        /// ways; box leaf verdict both ways; ContainedBy arm reachable.
        /// DEFAULT solver.
        fn cover_spgist_geo_regimes() {
            let leaf: [f64; 2] = kani::any();
            let arg: [f64; 4] = kani::any();
            let s0: u16 = kani::any();
            assume_point_strategy(s0);
            let keys_c = [CScanKey { sk_strategy: s0, sk_subtype: 0, sk_argument: arg.as_ptr() as usize }];
            let c_in = CLeafIn {
                scankeys: keys_c.as_ptr() as usize,
                orderbys: 0,
                nkeys: 1,
                norderbys: 0,
                reconstructedValue: 0,
                traversalValue: 0,
                level: 0,
                returnData: 0,
                leafDatum: leaf.as_ptr() as usize,
            };
            // SAFETY: caller memset contract.
            let mut c_out: CLeafOut = unsafe { core::mem::zeroed() };
            let mut c_res: c_int = 0;
            let c_err = unsafe { pg_spg_quad_leaf_consistent(&c_in, &mut c_out, &mut c_res) };
            kani::cover!(c_err == 0 && c_res != 0, "quad leaf accept reachable");
            kani::cover!(c_err == 0 && c_res == 0, "quad leaf reject reachable");
            kani::cover!(c_err == 0 && s0 == 8 && c_res != 0, "ContainedBy accept reachable");
        }
    }

    sg_proof! {
        /// MUST FAIL (rig non-vacuity control): C sees RTLeft where Rust
        /// sees RTRight on the quad leaf — any strictly-left pair is a
        /// counterexample. DEFAULT solver.
        fn control_spgist_geo_strategy_skew() {
            let leaf: [f64; 2] = kani::any();
            let arg: [f64; 4] = kani::any();

            let keys_c = [CScanKey { sk_strategy: 1, sk_subtype: 0, sk_argument: arg.as_ptr() as usize }]; // RTLeft
            let c_in = CLeafIn {
                scankeys: keys_c.as_ptr() as usize,
                orderbys: 0,
                nkeys: 1,
                norderbys: 0,
                reconstructedValue: 0,
                traversalValue: 0,
                level: 0,
                returnData: 0,
                leafDatum: leaf.as_ptr() as usize,
            };
            // SAFETY: caller memset contract.
            let mut c_out: CLeafOut = unsafe { core::mem::zeroed() };
            let mut c_res: c_int = 0;
            let c_err = unsafe { pg_spg_quad_leaf_consistent(&c_in, &mut c_out, &mut c_res) };
            kani::assume(c_err == 0);

            with_ctx(|mcx| {
                let keys_r = [scankey(5, 0, dptr(arg.as_ptr()))]; // RTRight (SKEW)
                let r_in = spgLeafConsistentIn {
                    scankeys: keys_r.as_ptr(),
                    orderbys: core::ptr::null(),
                    nkeys: 1,
                    norderbys: 0,
                    reconstructedValue: Datum::null(),
                    traversalValue: 0,
                    level: 0,
                    returnData: false,
                    leafDatum: dptr(leaf.as_ptr()),
                };
                let mut r_out = spgLeafConsistentOut {
                    leafValue: Datum::null(),
                    recheck: false,
                    recheckDistances: false,
                    distances: core::ptr::null(),
                };
                let d = call_fc2(
                    mcx,
                    &spgist_quadtree::SPGIST_QUAD_BUILTINS[4],
                    4022,
                    dptr(&r_in as *const spgLeafConsistentIn),
                    dptr(&mut r_out as *mut spgLeafConsistentOut),
                );
                assert!((c_res != 0) == d.as_bool());
            });
        }
    }
}
