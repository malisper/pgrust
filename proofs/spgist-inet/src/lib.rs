//! Kani C≡Rust equivalence: SP-GiST inet_ops opclass (network_spgist.c).
//! Tail-triage lane, pre-built 2026-07-29 — compile-gated only, ZERO solves.
//!
//! Ledger rows hosted here (screened for return-value parity viability):
//!   3795 inet_spg_config          — literal spgConfigOut stores (fast).
//!   3796 inet_spg_choose          — arm-parity theorem (MatchNode /
//!        SplitTuple tag + per-arm fields; the split arm's fresh cidr
//!        prefix compared as a POINTEE IMAGE, both sides reading their own
//!        allocation). Pure inet bit logic, no typcache.
//!   3798 inet_spg_inner_consistent — which-bitmap scalar verdict projected
//!        through the emitted nodeNumbers array (nkeys<=2).
//!   3799 inet_spg_leaf_consistent — bool verdict + recheck + leafValue
//!        passthrough; best row of the family.
//!   3797 inet_spg_picksplit — NOT harnessed: stays
//!        blocked(refactor: symbolic nTuples + mcx node arrays), per ledger.
//!
//! Both sides are called at the fmgr level: the Rust side through the
//! shipped NETWORK_SPGIST_BUILTINS table's fc_* entries on a real
//! LocalFcinfo frame (datum unwrap + struct-pointer protocol in-theorem),
//! the C side through plain (in, out) wrappers around verbatim bodies.
//! In/out protocol structs are built PER LANGUAGE with identical symbolic
//! field values; inet datums point at ONE shared image buffer per value, so
//! datum passthroughs (restDatum, leafValue, postfixPrefixDatum) are
//! pointer-equal by construction and asserted as such.
//!
//! Fences (all documented datatype/caller invariants, not narrowings of a
//! defined domain):
//!   - family in {PGSQL_AF_INET, PGSQL_AF_INET6} and bits <= maxbits(family)
//!     (the inet datatype invariant every C caller guarantees);
//!   - out structs zero-initialized (spgdoinsert/spgWalk memset contract) —
//!     fields an arm leaves unset are compared against zero;
//!   - AF_INET-only planes where noted (AF6 equal-family walks are the
//!     16-byte-element cost tier — runqueue note, not harnessed tonight).
//!
//! C side: c/pg_network_spgist.c (REL_18_STABLE verbatim; see its header
//! for every shim) + c/varatt_rel18.h. Allocation modulo static-buffer
//! allocator model both sides (Rust: proof_support mcx-stubs; C: named
//! static slots per the jsonb field-sensitivity law).
//!
//! Runqueue: ./runqueue.txt. Compile gate:
//!   cargo kani --only-codegen -Z c-ffi -Z stubbing --c-lib c/pg_network_spgist.c

#![allow(non_snake_case)]

#[cfg(kani)]
mod proofs {
    use datum::Datum;
    use mcx::{Mcx, MemoryContext, PgVec};
    // Load-bearing for #[kani::stub] path resolution.
    #[allow(unused_imports)]
    use proof_support;
    use std::os::raw::c_int;
    use types_error::PgResult;
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

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct CMatchNode {
        nodeN: c_int,
        levelAdd: c_int,
        restDatum: usize,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct CAddNode {
        nodeLabel: usize,
        nodeN: c_int,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct CSplitTuple {
        prefixHasPrefix: u8,
        prefixPrefixDatum: usize,
        prefixNNodes: c_int,
        prefixNodeLabels: usize,
        childNodeN: c_int,
        postfixHasPrefix: u8,
        postfixPrefixDatum: usize,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    union CChooseResult {
        matchNode: CMatchNode,
        addNode: CAddNode,
        splitTuple: CSplitTuple,
    }

    #[repr(C)]
    struct CChooseOut {
        resultType: c_int, // spgMatchNode = 1 / spgAddNode = 2 / spgSplitTuple = 3
        result: CChooseResult,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct CScanKey {
        sk_strategy: u16,
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

    #[repr(C)]
    struct CInnerOut {
        nNodes: c_int,
        nodeNumbers: usize,
        levelAdds: usize,
        reconstructedValues: usize,
        traversalValues: usize,
        distances: usize,
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
        distances: usize,
    }

    extern "C" {
        fn pg_inet_spg_config(cfg: *mut CConfigOut) -> c_int;
        fn pg_inet_spg_choose(input: *const CChooseIn, out: *mut CChooseOut) -> c_int;
        fn pg_inet_spg_inner_consistent(input: *const CInnerIn, out: *mut CInnerOut) -> c_int;
        fn pg_inet_spg_leaf_consistent(input: *const CLeafIn, out: *mut CLeafOut) -> c_int;
    }

    const AF4: u8 = 2; // PGSQL_AF_INET
    const AF6: u8 = 3; // PGSQL_AF_INET6

    // =====================================================================
    // shared scaffolding
    // =====================================================================

    /// Stub for `mcx::local_pool_on` (OnceLock machinery walls symex); pool
    /// selection is allocation strategy, out of every claim.
    #[allow(dead_code)]
    pub fn stub_local_pool_on() -> bool {
        false
    }

    /// Stub for `mcx::vec_append_bytes` (within-reserved-capacity appends
    /// only; overrun fails loudly).
    #[allow(dead_code)]
    pub fn stub_vec_append_bytes(v: &mut PgVec<'_, u8>, bytes: &[u8]) -> PgResult<()> {
        assert!(v.len() + bytes.len() <= v.capacity());
        let old = v.len();
        // SAFETY: capacity checked; disjoint; set_len covers written bytes.
        unsafe {
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), v.as_mut_ptr().add(old), bytes.len());
            v.set_len(old + bytes.len());
        }
        Ok(())
    }

    macro_rules! sg_proof {
        ($(#[$attr:meta])* fn $name:ident() $body:block) => {
            #[kani::proof]
            $(#[$attr])*
            #[kani::stub(mcx::Mcx::allocate, proof_support::mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, proof_support::mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, proof_support::mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(mcx::vec_with_capacity_in, proof_support::mcx_stubs::stub_vec_with_capacity_in)]
            #[kani::stub(mcx::vec_append_bytes, stub_vec_append_bytes)]
            #[kani::stub(mcx::local_pool_on, stub_local_pool_on)]
            #[kani::stub(types_error::PgError::error, proof_support::stub_pg_error_error)]
            #[kani::stub(alloc::fmt::format, proof_support::stub_format)]
            #[kani::stub(std::env::var, proof_support::stub_env_var)]
            fn $name() $body
        };
    }

    /// AF_INET inet varlena image: 4B header + family + bits + 4 addr bytes.
    /// bits fenced to the datatype invariant (<= 32).
    fn sym_inet4() -> [u8; 10] {
        let bits: u8 = kani::any();
        kani::assume(bits <= 32);
        let addr: [u8; 4] = kani::any();
        let mut img = [0u8; 10];
        img[..4].copy_from_slice(&datum::varlena::set_varsize_4b(10));
        img[4] = AF4;
        img[5] = bits;
        img[6..10].copy_from_slice(&addr);
        img
    }

    /// Either-family inet image in a fixed 22-byte buffer (AF6 tail bytes
    /// zero-literal-filled on the AF4 plane — dead-symbolic-bytes law).
    fn sym_inet_any() -> [u8; 22] {
        let fam6: bool = kani::any();
        let bits: u8 = kani::any();
        let mut img = [0u8; 22];
        if fam6 {
            kani::assume(bits <= 128);
            let addr: [u8; 16] = kani::any();
            img[..4].copy_from_slice(&datum::varlena::set_varsize_4b(22));
            img[4] = AF6;
            img[5] = bits;
            img[6..22].copy_from_slice(&addr);
        } else {
            kani::assume(bits <= 32);
            let addr: [u8; 4] = kani::any();
            img[..4].copy_from_slice(&datum::varlena::set_varsize_4b(10));
            img[4] = AF4;
            img[5] = bits;
            img[6..10].copy_from_slice(&addr);
        }
        img
    }

    fn dptr<T>(p: *const T) -> Datum {
        Datum::from_usize(p as usize)
    }

    /// Invoke a shipped fc_* entry from NETWORK_SPGIST_BUILTINS on a real
    /// 2-arg frame (in-struct ptr, out-struct ptr), result mcx armed.
    fn call_fc(
        mcx: Mcx<'_>,
        idx: usize,
        foid: u32,
        arg0: Datum,
        arg1: Datum,
    ) -> (Datum, bool) {
        let entry = &network_spgist::NETWORK_SPGIST_BUILTINS[idx];
        assert!(entry.foid == foid, "builtins table order changed");
        let mut f = proof_support::fci([arg0, arg1]);
        // SAFETY: harness context outlives the call and result reads.
        unsafe { f.set_result_mcx(mcx) };
        match (entry.func)(None, &mut f) {
            Ok(d) => (d, f.isnull),
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

    /// Read a varlena image back from a datum produced by the shipped code
    /// (wave-6: by-ref result image read-back does not trip provenance).
    /// Length = 4B-header VARSIZE.
    unsafe fn image_at<'a>(d: usize) -> &'a [u8] {
        let p = d as *const u8;
        let w = u32::from_ne_bytes([*p, *p.add(1), *p.add(2), *p.add(3)]);
        core::slice::from_raw_parts(p, (w >> 2) as usize)
    }

    fn scankey(strategy: u16, arg: Datum) -> ScanKeyData {
        let mut k = ScanKeyData::empty();
        k.sk_strategy = strategy;
        k.sk_argument = arg;
        k
    }

    // =====================================================================
    // 3795 inet_spg_config
    // =====================================================================

    sg_proof! {
        /// row 3795: four literal stores; untouched fields stay zero both
        /// sides (out structs zero-init per the spgWalk memset contract).
        fn eq_inet_spg_config() {
            let mut c_out = CConfigOut {
                prefixType: 0,
                labelType: 0,
                leafType: 0,
                canReturnData: 0,
                longValuesOK: 0,
            };
            unsafe { pg_inet_spg_config(&mut c_out) };
            with_ctx(|mcx| {
                let mut r_out = spgConfigOut::default();
                let (_, _) = call_fc(
                    mcx,
                    0,
                    3795,
                    Datum::null(),
                    dptr(&mut r_out as *mut spgConfigOut),
                );
                assert!(c_out.prefixType == r_out.prefixType);
                assert!(c_out.labelType == r_out.labelType);
                assert!(c_out.leafType == r_out.leafType, "leafType must stay untouched");
                assert!((c_out.canReturnData != 0) == r_out.canReturnData);
                assert!((c_out.longValuesOK != 0) == r_out.longValuesOK);
            });
        }
    }

    // =====================================================================
    // 3796 inet_spg_choose — arm-parity theorems
    // =====================================================================

    fn run_choose(
        mcx: Mcx<'_>,
        val_img: &[u8],
        has_prefix: bool,
        all_the_same: bool,
        n_nodes: i32,
        prefix_img: Option<&[u8]>,
    ) -> (CChooseOut, spgChooseOut) {
        let c_in = CChooseIn {
            datum: val_img.as_ptr() as usize,
            leafDatum: 0,
            level: 0,
            allTheSame: all_the_same as u8,
            hasPrefix: has_prefix as u8,
            prefixDatum: prefix_img.map_or(0, |p| p.as_ptr() as usize),
            nNodes: n_nodes,
            nodeLabels: 0,
        };
        // SAFETY: all-zero is a valid initial CChooseOut (caller memset
        // contract; the union's zero state is compared only where an arm
        // leaves it unset).
        let mut c_out: CChooseOut = unsafe { core::mem::zeroed() };
        unsafe { pg_inet_spg_choose(&c_in, &mut c_out) };

        let r_in = spgChooseIn {
            datum: dptr(val_img.as_ptr()),
            leafDatum: Datum::null(),
            level: 0,
            allTheSame: all_the_same,
            hasPrefix: has_prefix,
            prefixDatum: prefix_img.map_or(Datum::null(), |p| dptr(p.as_ptr())),
            nNodes: n_nodes,
            nodeLabels: core::ptr::null(),
        };
        let mut r_out = spgChooseOut::None;
        let (_, _) = call_fc(
            mcx,
            1,
            3796,
            dptr(&r_in as *const spgChooseIn),
            dptr(&mut r_out as *mut spgChooseOut),
        );
        (c_out, r_out)
    }

    fn assert_choose_parity(c_out: &CChooseOut, r_out: &spgChooseOut, val_ptr: usize) {
        match *r_out {
            spgChooseOut::MatchNode {
                nodeN,
                levelAdd,
                restDatum,
            } => {
                assert!(c_out.resultType == 1, "arm tag mismatch (Rust MatchNode)");
                // SAFETY: tag checked; C wrote the matchNode member.
                let m = unsafe { c_out.result.matchNode };
                assert!(m.nodeN == nodeN, "nodeN mismatch");
                // C leaves levelAdd zero-init; Rust writes 0 — same value.
                assert!(m.levelAdd == levelAdd);
                assert!(levelAdd == 0);
                assert!(m.restDatum == restDatum.as_usize(), "restDatum passthrough");
                assert!(m.restDatum == val_ptr);
            }
            spgChooseOut::SplitTuple {
                prefixHasPrefix,
                prefixPrefixDatum,
                prefixNNodes,
                prefixNodeLabels,
                childNodeN,
                postfixHasPrefix,
                postfixPrefixDatum,
            } => {
                assert!(c_out.resultType == 3, "arm tag mismatch (Rust SplitTuple)");
                // SAFETY: tag checked.
                let s = unsafe { c_out.result.splitTuple };
                assert!((s.prefixHasPrefix != 0) == prefixHasPrefix);
                assert!(s.prefixNNodes == prefixNNodes);
                assert!(s.prefixNodeLabels == 0 && prefixNodeLabels.is_null());
                assert!(s.childNodeN == childNodeN, "childNodeN mismatch");
                assert!((s.postfixHasPrefix != 0) == postfixHasPrefix);
                assert!(s.postfixPrefixDatum == postfixPrefixDatum.as_usize());
                if prefixHasPrefix {
                    // Fresh cidr prefix: compare POINTEE images (each side
                    // allocated its own; C in a named static slot, Rust via
                    // the proof heap).
                    // SAFETY: both datums are live images written above.
                    let ci = unsafe { image_at(s.prefixPrefixDatum) };
                    let ri = unsafe { image_at(prefixPrefixDatum.as_usize()) };
                    assert!(ci.len() == ri.len(), "cidr prefix image length");
                    let mut i = 0;
                    while i < ci.len() {
                        assert!(ci[i] == ri[i], "cidr prefix image byte");
                        i += 1;
                    }
                } else {
                    assert!(s.prefixPrefixDatum == 0 && prefixPrefixDatum.as_usize() == 0);
                }
            }
            spgChooseOut::AddNode { .. } => {
                panic!("inet_spg_choose never emits AddNode");
            }
            spgChooseOut::None => panic!("Rust choose wrote no arm"),
        }
    }

    sg_proof! {
        /// row 3796 no-prefix (family-split) tuple: MatchNode nodeN by
        /// family + restDatum passthrough, either family.
        fn eq_inet_spg_choose_nopfx() {
            let val = sym_inet_any();
            with_ctx(|mcx| {
                let (c_out, r_out) = run_choose(mcx, &val, false, false, 2, None);
                assert_choose_parity(&c_out, &r_out, val.as_ptr() as usize);
            });
        }
    }

    sg_proof! {
        /// row 3796 prefixed tuple, families DIFFER: 2-node SplitTuple
        /// literal fields + postfix passthrough.
        fn eq_inet_spg_choose_famsplit() {
            let val = sym_inet_any();
            let prefix = sym_inet_any();
            kani::assume(val[4] != prefix[4]);
            with_ctx(|mcx| {
                let (c_out, r_out) = run_choose(mcx, &val, true, false, 4, Some(&prefix));
                assert_choose_parity(&c_out, &r_out, val.as_ptr() as usize);
            });
        }
    }

    sg_proof! {
        /// row 3796 prefixed tuple, both AF_INET: the full split-vs-match
        /// decision (bitncmp + bitncommon + cidr_set_masklen + node_number
        /// in-theorem, fresh-prefix image parity). Expected ladder tier.
        #[kani::unwind(12)]
        fn eq_inet_spg_choose_af4() {
            let val = sym_inet4();
            let prefix = sym_inet4();
            with_ctx(|mcx| {
                let (c_out, r_out) = run_choose(mcx, &val, true, false, 4, Some(&prefix));
                assert_choose_parity(&c_out, &r_out, val.as_ptr() as usize);
                kani::cover!(c_out.resultType == 1, "match arm reachable");
                kani::cover!(c_out.resultType == 3, "split arm reachable");
            });
        }
    }

    // =====================================================================
    // 3798 inet_spg_inner_consistent
    // =====================================================================

    fn run_inner(
        mcx: Mcx<'_>,
        keys_r: &[ScanKeyData],
        keys_c: &[CScanKey],
        nkeys: i32,
        has_prefix: bool,
        all_the_same: bool,
        n_nodes: i32,
        prefix_img: Option<&[u8]>,
    ) {
        let c_in = CInnerIn {
            scankeys: keys_c.as_ptr() as usize,
            orderbys: 0,
            nkeys,
            norderbys: 0,
            reconstructedValue: 0,
            traversalValue: 0,
            traversalMemoryContext: 0,
            level: 0,
            returnData: 0,
            allTheSame: all_the_same as u8,
            hasPrefix: has_prefix as u8,
            prefixDatum: prefix_img.map_or(0, |p| p.as_ptr() as usize),
            nNodes: n_nodes,
            nodeLabels: 0,
        };
        // SAFETY: zeroed out struct per caller memset contract.
        let mut c_out: CInnerOut = unsafe { core::mem::zeroed() };
        unsafe { pg_inet_spg_inner_consistent(&c_in, &mut c_out) };

        let r_in = spgInnerConsistentIn {
            scankeys: keys_r.as_ptr(),
            orderbys: core::ptr::null(),
            nkeys,
            norderbys: 0,
            reconstructedValue: Datum::null(),
            traversalValue: 0,
            traversalMemoryContext: mcx,
            level: 0,
            returnData: false,
            allTheSame: all_the_same,
            hasPrefix: has_prefix,
            prefixDatum: prefix_img.map_or(Datum::null(), |p| dptr(p.as_ptr())),
            nNodes: n_nodes,
            nodeLabels: core::ptr::null(),
        };
        let mut r_out = spgInnerConsistentOut::default();
        let (_, _) = call_fc(
            mcx,
            3,
            3798,
            dptr(&r_in as *const spgInnerConsistentIn),
            dptr(&mut r_out as *mut spgInnerConsistentOut),
        );

        assert!(c_out.nNodes == r_out.nNodes, "visited node count mismatch");
        let mut i = 0;
        while i < c_out.nNodes as usize {
            // SAFETY: both arrays live (C static slot / Rust proof heap),
            // nNodes entries written above.
            let cn = unsafe { *(c_out.nodeNumbers as *const c_int).add(i) };
            let rn = unsafe { *r_out.nodeNumbers.add(i) };
            assert!(cn == rn, "nodeNumbers entry mismatch");
            i += 1;
        }
    }

    sg_proof! {
        /// row 3798 no-prefix (family-split) tuple: which-lattice over
        /// nkeys<=2 fully symbolic strategies x either-family arguments.
        fn eq_inet_spg_inner_nopfx() {
            let nkeys: i32 = kani::any();
            kani::assume((0..=2).contains(&nkeys));
            let a0 = sym_inet_any();
            let a1 = sym_inet_any();
            let s0: u16 = kani::any();
            let s1: u16 = kani::any();
            let keys_r = [
                scankey(s0, dptr(a0.as_ptr())),
                scankey(s1, dptr(a1.as_ptr())),
            ];
            let keys_c = [
                CScanKey { sk_strategy: s0, sk_argument: a0.as_ptr() as usize },
                CScanKey { sk_strategy: s1, sk_argument: a1.as_ptr() as usize },
            ];
            with_ctx(|mcx| {
                run_inner(mcx, &keys_r, &keys_c, nkeys, false, false, 2, None);
            });
        }
    }

    sg_proof! {
        /// row 3798 4-node prefixed tuple, AF_INET plane, nkeys<=1: the full
        /// consistent_bitmap checks 0-5 in-theorem. Expected ladder tier.
        #[kani::unwind(12)]
        fn eq_inet_spg_inner_af4_nkeys1() {
            let nkeys: i32 = kani::any();
            kani::assume((0..=1).contains(&nkeys));
            let prefix = sym_inet4();
            let a0 = sym_inet4();
            let s0: u16 = kani::any();
            let keys_r = [scankey(s0, dptr(a0.as_ptr()))];
            let keys_c = [CScanKey { sk_strategy: s0, sk_argument: a0.as_ptr() as usize }];
            with_ctx(|mcx| {
                run_inner(mcx, &keys_r, &keys_c, nkeys, true, false, 4, Some(&prefix));
            });
        }
    }

    sg_proof! {
        /// row 3798 allTheSame arm: visit-everything bitmap (~0) projected
        /// through the node emission loop.
        fn eq_inet_spg_inner_allsame() {
            let prefix = sym_inet4();
            with_ctx(|mcx| {
                run_inner(mcx, &[], &[], 0, true, true, 4, Some(&prefix));
            });
        }
    }

    // =====================================================================
    // 3799 inet_spg_leaf_consistent
    // =====================================================================

    fn run_leaf(
        mcx: Mcx<'_>,
        leaf_img: &[u8],
        keys_r: &[ScanKeyData],
        keys_c: &[CScanKey],
        nkeys: i32,
    ) -> (c_int, bool) {
        let c_in = CLeafIn {
            scankeys: keys_c.as_ptr() as usize,
            orderbys: 0,
            nkeys,
            norderbys: 0,
            reconstructedValue: 0,
            traversalValue: 0,
            level: 0,
            returnData: 0,
            leafDatum: leaf_img.as_ptr() as usize,
        };
        // SAFETY: zeroed out struct per caller memset contract.
        let mut c_out: CLeafOut = unsafe { core::mem::zeroed() };
        let c = unsafe { pg_inet_spg_leaf_consistent(&c_in, &mut c_out) };

        let r_in = spgLeafConsistentIn {
            scankeys: keys_r.as_ptr(),
            orderbys: core::ptr::null(),
            nkeys,
            norderbys: 0,
            reconstructedValue: Datum::null(),
            traversalValue: 0,
            level: 0,
            returnData: false,
            leafDatum: dptr(leaf_img.as_ptr()),
        };
        let mut r_out = spgLeafConsistentOut::default();
        let (d, _) = call_fc(
            mcx,
            4,
            3799,
            dptr(&r_in as *const spgLeafConsistentIn),
            dptr(&mut r_out as *mut spgLeafConsistentOut),
        );
        assert!((c_out.recheck != 0) == r_out.recheck, "recheck mismatch");
        assert!(
            c_out.leafValue == r_out.leafValue.as_usize(),
            "leafValue passthrough"
        );
        assert!(c_out.leafValue == leaf_img.as_ptr() as usize);
        (c, d.as_bool())
    }

    sg_proof! {
        /// row 3799 AF_INET plane, nkeys<=2 fully symbolic strategies: bool
        /// verdict + recheck + leafValue passthrough (checks 0-6 incl the
        /// whole-address bitncmp in-theorem). Expected ladder tier.
        #[kani::unwind(12)]
        fn eq_inet_spg_leaf_af4_nkeys2() {
            let nkeys: i32 = kani::any();
            kani::assume((0..=2).contains(&nkeys));
            let leaf = sym_inet4();
            let a0 = sym_inet4();
            let a1 = sym_inet4();
            let s0: u16 = kani::any();
            let s1: u16 = kani::any();
            let keys_r = [
                scankey(s0, dptr(a0.as_ptr())),
                scankey(s1, dptr(a1.as_ptr())),
            ];
            let keys_c = [
                CScanKey { sk_strategy: s0, sk_argument: a0.as_ptr() as usize },
                CScanKey { sk_strategy: s1, sk_argument: a1.as_ptr() as usize },
            ];
            with_ctx(|mcx| {
                let (c, r) = run_leaf(mcx, &leaf, &keys_r, &keys_c, nkeys);
                assert!((c != 0) == r, "leaf verdict mismatch");
            });
        }
    }

    sg_proof! {
        /// row 3799 cross-family arm: leaf and argument families DIFFER
        /// (check 0 only), one key, fully symbolic strategy.
        fn eq_inet_spg_leaf_mixed_family() {
            let leaf = sym_inet_any();
            let a0 = sym_inet_any();
            kani::assume(leaf[4] != a0[4]);
            let s0: u16 = kani::any();
            let keys_r = [scankey(s0, dptr(a0.as_ptr()))];
            let keys_c = [CScanKey { sk_strategy: s0, sk_argument: a0.as_ptr() as usize }];
            with_ctx(|mcx| {
                let (c, r) = run_leaf(mcx, &leaf, &keys_r, &keys_c, 1);
                assert!((c != 0) == r, "leaf verdict mismatch");
            });
        }
    }

    // =====================================================================
    // rig coverage + control
    // =====================================================================

    sg_proof! {
        /// Regime coverage (vacuity insurance): leaf verdict both ways and
        /// the sub/super strategy arms reachable on the AF4 plane. DEFAULT
        /// solver.
        #[kani::unwind(12)]
        fn cover_spgist_regimes() {
            let leaf = sym_inet4();
            let a0 = sym_inet4();
            let s0: u16 = kani::any();
            let c_in = CLeafIn {
                scankeys: &CScanKey { sk_strategy: s0, sk_argument: a0.as_ptr() as usize }
                    as *const CScanKey as usize,
                orderbys: 0,
                nkeys: 1,
                norderbys: 0,
                reconstructedValue: 0,
                traversalValue: 0,
                level: 0,
                returnData: 0,
                leafDatum: leaf.as_ptr() as usize,
            };
            // SAFETY: zeroed out struct per caller memset contract.
            let mut c_out: CLeafOut = unsafe { core::mem::zeroed() };
            let c = unsafe { pg_inet_spg_leaf_consistent(&c_in, &mut c_out) };
            kani::cover!(c != 0, "leaf accept reachable");
            kani::cover!(c == 0, "leaf reject reachable");
            kani::cover!(c != 0 && s0 == 24, "sub strategy accept");
            kani::cover!(c != 0 && s0 == 27, "supeq strategy accept");
            kani::cover!(c != 0 && s0 == 18, "equal strategy accept");
        }
    }

    sg_proof! {
        /// MUST FAIL (rig non-vacuity control): C sees a SKEWED strategy
        /// (RTLess where Rust sees RTLessEqual) — any exact-equal leaf/arg
        /// pair is a counterexample. DEFAULT solver.
        #[kani::unwind(12)]
        fn control_spgist_leaf_strategy_skew() {
            let leaf = sym_inet4();
            let a0 = sym_inet4();
            let keys_r = [scankey(21, dptr(a0.as_ptr()))]; // RTLessEqual
            let keys_c = [CScanKey { sk_strategy: 20, sk_argument: a0.as_ptr() as usize }]; // RTLess
            with_ctx(|mcx| {
                let (c, r) = run_leaf(mcx, &leaf, &keys_r, &keys_c, 1);
                assert!((c != 0) == r);
            });
        }
    }
}
