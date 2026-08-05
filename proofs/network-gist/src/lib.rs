//! Kani C≡Rust equivalence: GiST inet_ops support procs (network_gist.c).
//! Rust side: shipped crates/backend/utils/adt/network_gist. C side: vendored
//! c/pg_network_gist.c (REL_18_STABLE, see its header for shims) linking the
//! bitncmp/bitncommon copy in c/pg_net_bits.c (== proofs/network/csrc/
//! net_shim.c, proved equivalent to the shipped Rust helpers there).
//!
//! Key-image model: both sides read the SAME 20-byte GistInetKey image
//! (1B short-varlena header, family, minbits, commonbits, ipaddr[16]) —
//! C through the verbatim struct, Rust through GkRef::from_image.  Fenced
//! to the datatype invariant every producer (compress, union, picksplit)
//! establishes:
//!   family in {0, PGSQL_AF_INET, PGSQL_AF_INET6};
//!   family == 0  =>  minbits == 0 && commonbits == 0
//!     (the file-head union definition: multi-family union is the dummy
//!      all-zero key; its address is also all-zero, but no proc consulted
//!      here reads a family-0 address beyond the 4-byte v4 width, so the
//!      address stays symbolic except where noted);
//!   otherwise minbits <= maxbits(family) && commonbits <= maxbits(family).
//! Addresses are FULLY symbolic 16-byte arrays (producers zero bits past
//! commonbits, but no consumer proved here relies on it; proving over dirty
//! addresses is strictly wider).
//!
//! unwind 18 everywhere a 16-byte address loop runs: the 16-byte ipaddr
//! memcmp fabricates a counterexample at unwind 10 (network-lane finding,
//! 2026-07-30) — unwind >= 18 is mandatory.
//!
//! Per-row shape:
//! - 3553 consistent: CORE-LEVEL consistent_internal vs verbatim C, one
//!   harness per strategy LITERAL (11 strategies; literals fold, and the
//!   Rust unknown-strategy panic arm / C elog flag leave the formula) +
//!   cover_strategy_partition (mandatory union coverage) +
//!   cover_consistent_regimes (leaf/inner, family-0, cross-family covers).
//!   The fc wrapper tier above it (GISTENTRY datum unwrap, recheck store)
//!   stays in the tested tier.
//! - 3554 union: CORE-LEVEL calc_inet_union_params (pub for proofs) +
//!   gk_image vs verbatim C calc + build_inet_union_key, full param 4-tuple
//!   + all-20-byte result-image equality, concrete n=2 and n=3 cells
//!   (GistEntryVector construction stays in the tested tier).
//! - 3555 compress / 3573 fetch: WRAPPER-LEVEL — the shipped fc_* invoked
//!   via NETWORK_GIST_BUILTINS through a real LocalFcinfo frame, modulo the
//!   static-buffer allocator model (mcx-stubs; byref_result arena leaves the
//!   proof).  Per-family (4/16) cells so the result-image length is
//!   concrete; gistentryinit protocol fields asserted against the C spec
//!   (offset/page preserved, leafkey false).  Compress non-leaf and NULL-key
//!   arms are spec harnesses (C's arms are structurally identical returns).
//! - 3557 penalty: CORE-LEVEL penalty_internal vs verbatim C, bit-exact f32
//!   (to_bits) over the full fenced key plane; cover_penalty_regimes
//!   witnesses all four arms.  This is the port-rewrite row (Rust
//!   xor/leading_zeros vs C per-bit loop).
//! - 3559 same: WRAPPER-LEVEL via NETWORK_GIST_BUILTINS (no allocation on
//!   this path — the bool rides an out-pointer datum).
//! - control_same_minbits_swap: negative control, must FAIL (run with the
//!   DEFAULT solver; expected-green harnesses with kissat).
//!
//! Run (one at a time, RSS watchdog per prove-target):
//!   timeout 450 cargo kani -Z c-ffi -Z stubbing --solver kissat \
//!     --c-lib c/pg_network_gist.c --c-lib c/pg_net_bits.c \
//!     --harness <name> --exact

#[cfg(kani)]
mod proofs {
    use adt_network::{InetRef, InetValue};
    use datum::{Datum, NullableDatum};
    use network_gist::{
        calc_inet_union_params, consistent_internal, gk_image, penalty_internal, GkRef,
        NETWORK_GIST_BUILTINS,
    };
    use proof_support::{mcx_stubs, stubs};
    use types_fmgr::LocalFcinfo;
    use types_gist::GISTENTRY;

    use std::os::raw::c_int;

    /// C-side inet value model (c/pg_network_gist.c pgc_inet).
    #[repr(C)]
    struct CInet {
        family: u8,
        bits: u8,
        addr: [u8; 16],
    }

    extern "C" {
        fn pg_inet_gist_consistent(
            key: *const u8,
            query: *const CInet,
            strategy: u16,
            leaf_flag: c_int,
            err: *mut c_int,
        ) -> c_int;
        fn pg_calc_union_params_2(
            k1: *const u8,
            k2: *const u8,
            minfamily: *mut c_int,
            maxfamily: *mut c_int,
            minbits: *mut c_int,
            commonbits: *mut c_int,
        ) -> c_int;
        fn pg_calc_union_params_3(
            k1: *const u8,
            k2: *const u8,
            k3: *const u8,
            minfamily: *mut c_int,
            maxfamily: *mut c_int,
            minbits: *mut c_int,
            commonbits: *mut c_int,
        ) -> c_int;
        fn pg_build_inet_union_key(
            family: c_int,
            minbits: c_int,
            commonbits: c_int,
            addr: *const u8,
            result: *mut u8,
        ) -> c_int;
        fn pg_inet_gist_compress_key(input: *const CInet, r: *mut u8) -> c_int;
        fn pg_inet_gist_fetch_val(key: *const u8, dst: *mut CInet) -> c_int;
        fn pg_inet_gist_penalty(orig: *const u8, newk: *const u8, penalty: *mut f32) -> c_int;
        fn pg_inet_gist_same(left: *const u8, right: *const u8) -> c_int;
        fn pgc_bitncommon(l: *const u8, r: *const u8, n: c_int) -> c_int;
    }

    const AF_INET: u8 = 2;
    const AF_INET6: u8 = 3;
    // NETWORK_GIST_BUILTINS positions (wiring asserted in each harness).
    const BI_COMPRESS: usize = 2;
    const BI_SAME: usize = 5;
    const BI_FETCH: usize = 6;

    fn maxbits(family: u8) -> u8 {
        if family == AF_INET6 {
            128
        } else {
            32
        }
    }

    fn gk_len(family: u8) -> usize {
        if family == AF_INET6 {
            20
        } else {
            8
        }
    }

    /// Symbolic GistInetKey image under the producer invariant (module doc).
    fn any_gk_image() -> [u8; 20] {
        let family: u8 = kani::any();
        kani::assume(family == 0 || family == AF_INET || family == AF_INET6);
        any_gk_image_fam(family)
    }

    /// Same, with the family supplied by the caller (literal at split call
    /// sites; literals fold, assumes don't).
    fn any_gk_image_fam(family: u8) -> [u8; 20] {
        let minbits: u8 = kani::any();
        let commonbits: u8 = kani::any();
        if family == 0 {
            kani::assume(minbits == 0 && commonbits == 0);
        } else {
            kani::assume(minbits <= maxbits(family) && commonbits <= maxbits(family));
        }
        let addr: [u8; 16] = kani::any();
        let mut img = [0u8; 20];
        img[0] = ((gk_len(family) << 1) | 1) as u8;
        img[1] = family;
        img[2] = minbits;
        img[3] = commonbits;
        img[4..20].copy_from_slice(&addr);
        img
    }

    /// Symbolic query inet (family in {v4, v6}, bits <= maxbits, dirty addr).
    fn any_query() -> CInet {
        let family: u8 = kani::any();
        kani::assume(family == AF_INET || family == AF_INET6);
        let bits: u8 = kani::any();
        kani::assume(bits <= maxbits(family));
        CInet {
            family,
            bits,
            addr: kani::any(),
        }
    }

    // =================================================================
    // 3553 inet_gist_consistent — core-level, per-strategy literals
    // =================================================================

    fn consistent_case(strategy: u16) {
        let img = any_gk_image();
        let q = any_query();
        let leaf: bool = kani::any();
        // C Assert(!GIST_LEAF) / Rust debug_assert: family-0 keys are
        // inner-only (multi-family unions never reach leaves).
        kani::assume(!(img[1] == 0 && leaf));

        let mut err: c_int = 0;
        let c = unsafe {
            pg_inet_gist_consistent(img.as_ptr(), &q, strategy, leaf as c_int, &mut err)
        };
        assert!(err == 0, "C elog arm unreachable under fenced strategy");

        let r = consistent_internal(
            GkRef::from_image(&img),
            InetRef {
                family: q.family,
                bits: q.bits,
                addr: &q.addr,
            },
            strategy,
            leaf,
        );
        assert!((c != 0) == r);
    }

    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_consistent_overlaps() {
        consistent_case(3);
    }
    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_consistent_eq() {
        consistent_case(18);
    }
    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_consistent_ne() {
        consistent_case(19);
    }
    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_consistent_lt() {
        consistent_case(20);
    }
    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_consistent_le() {
        consistent_case(21);
    }
    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_consistent_gt() {
        consistent_case(22);
    }
    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_consistent_ge() {
        consistent_case(23);
    }
    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_consistent_sub() {
        consistent_case(24);
    }
    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_consistent_subeq() {
        consistent_case(25);
    }
    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_consistent_sup() {
        consistent_case(26);
    }
    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_consistent_supeq() {
        consistent_case(27);
    }

    /// MANDATORY union coverage for the per-strategy split: every strategy
    /// the inet_ops opclass registers is one of the 11 literal cases.
    /// (pg_amop for gist/inet_ops: 3, 18..=27 — access/stratnum.h values.)
    #[kani::proof]
    fn cover_strategy_partition() {
        let s: u16 = kani::any();
        kani::assume(s == 3 || (18..=27).contains(&s));
        assert!(matches!(s, 3 | 18 | 19 | 20 | 21 | 22 | 23 | 24 | 25 | 26 | 27));
    }

    /// Vacuity insurance: leaf and inner pages, the family-0 fast path,
    /// cross-family keys, and both verdicts are all reachable inside the
    /// fence (checked on one representative strategy; the input fence is
    /// strategy-independent). DEFAULT solver (covers are SAT calls).
    #[kani::proof]
    #[kani::unwind(18)]
    fn cover_consistent_regimes() {
        let img = any_gk_image();
        let q = any_query();
        let leaf: bool = kani::any();
        kani::assume(!(img[1] == 0 && leaf));
        let r = consistent_internal(
            GkRef::from_image(&img),
            InetRef {
                family: q.family,
                bits: q.bits,
                addr: &q.addr,
            },
            18, // EQ exercises checks 2..5
            leaf,
        );
        kani::cover!(img[1] == 0);
        kani::cover!(img[1] != 0 && img[1] != q.family);
        kani::cover!(leaf && r);
        kani::cover!(leaf && !r);
        kani::cover!(!leaf && r);
        kani::cover!(!leaf && !r);
    }

    // =================================================================
    // 3554 inet_gist_union — core-level params + image, n = 2 / 3 cells
    // =================================================================

    /// Shared body. `refs` are GkRefs PRE-BUILT by the caller: constructing
    /// GkRef inside the iterator closure trips a measured Kani 0.67 defect
    /// (probe_slice_across_iter_loop, MUST-FAIL witness) that fabricates
    /// non-reproducing fold counterexamples; pre-built refs are clean
    /// (probe_slice_prebuilt_refs / probe_slice_no_iter, MUST-PASS pair).
    fn union_check(imgs: &[[u8; 20]], refs: &[GkRef<'_>]) {
        let (mut mf, mut xf, mut mb, mut cb) = (0 as c_int, 0 as c_int, 0 as c_int, 0 as c_int);
        unsafe {
            match imgs.len() {
                2 => pg_calc_union_params_2(
                    imgs[0].as_ptr(),
                    imgs[1].as_ptr(),
                    &mut mf,
                    &mut xf,
                    &mut mb,
                    &mut cb,
                ),
                3 => pg_calc_union_params_3(
                    imgs[0].as_ptr(),
                    imgs[1].as_ptr(),
                    imgs[2].as_ptr(),
                    &mut mf,
                    &mut xf,
                    &mut mb,
                    &mut cb,
                ),
                _ => panic!("unsupported N"),
            };
        }
        let cfam: c_int = if mf != xf { 0 } else { mf };
        let mut ckey = [0u8; 20];
        unsafe { pg_build_inet_union_key(cfam, mb, cb, imgs[0][4..].as_ptr(), ckey.as_mut_ptr()) };

        // Rust side: shipped calc (pub for proofs) + shipped gk_image,
        // exactly as fc_inet_gist_union composes them.
        let p = calc_inet_union_params(refs.iter().copied());
        assert!(p.minfamily as c_int == mf);
        assert!(p.maxfamily as c_int == xf);
        assert!(p.minbits == mb);
        assert!(p.commonbits == cb);

        let rfam = if p.minfamily != p.maxfamily {
            0
        } else {
            p.minfamily
        };
        let (rimg, rlen) = gk_image(rfam, p.minbits, p.commonbits, refs[0].addr());
        assert!(rlen == (ckey[0] >> 1) as usize);
        assert!(rimg == ckey, "union key image divergence");
    }

    #[kani::proof]
    #[kani::unwind(22)]
    fn eq_union_n2() {
        let a = any_gk_image();
        let b = any_gk_image();
        let refs = [GkRef::from_image(&a), GkRef::from_image(&b)];
        union_check(&[a, b], &refs);
    }

    #[kani::proof]
    #[kani::unwind(22)]
    fn eq_union_n3() {
        let a = any_gk_image();
        let b = any_gk_image();
        let c = any_gk_image();
        let refs = [
            GkRef::from_image(&a),
            GkRef::from_image(&b),
            GkRef::from_image(&c),
        ];
        union_check(&[a, b, c], &refs);
    }

    /// Union regimes reachable at n=2: same-family fold, multi-family
    /// zeroing, family-0 member, partial-byte mask.
    #[kani::proof]
    #[kani::unwind(18)]
    fn cover_union_regimes() {
        let a = any_gk_image();
        let b = any_gk_image();
        let refs = [GkRef::from_image(&a), GkRef::from_image(&b)];
        let p = calc_inet_union_params(refs.iter().copied());
        kani::cover!(p.minfamily == p.maxfamily && p.commonbits > 0);
        kani::cover!(p.minfamily != p.maxfamily);
        kani::cover!(a[1] == 0 || b[1] == 0);
        kani::cover!(p.commonbits % 8 != 0);
    }

    /// Reduced probe (union-divergence localization): the proved network-lane
    /// bitncommon theorem extended to the v6 domain — 16-byte arrays, n<=128.
    #[kani::proof]
    #[kani::unwind(22)]
    fn probe_bitncommon16() {
        let l: [u8; 16] = kani::any();
        let r: [u8; 16] = kani::any();
        let n: i32 = kani::any();
        kani::assume((0..=128).contains(&n));
        let rust = adt_network::bitncommon(&l, &r, n);
        let c = unsafe { pgc_bitncommon(l.as_ptr(), r.as_ptr(), n) };
        assert!(rust == c);
    }

    /// Diagnostic: C composition vs direct C bitncommon on the same inputs
    /// (C-vs-C). If THIS fails, the vendored calc's struct-typed reads of the
    /// Rust-allocated key images are mis-modeled, not the logic.
    #[kani::proof]
    #[kani::unwind(22)]
    fn probe_union_c_vs_c() {
        let a = any_gk_image_fam(AF_INET6);
        let b = any_gk_image_fam(AF_INET6);
        let (mut mf, mut xf, mut mb, mut cb) = (0 as c_int, 0 as c_int, 0 as c_int, 0 as c_int);
        unsafe {
            pg_calc_union_params_2(a.as_ptr(), b.as_ptr(), &mut mf, &mut xf, &mut mb, &mut cb);
        }
        let min_cb = (a[3] as c_int).min(b[3] as c_int);
        let expect = if min_cb > 0 {
            unsafe { pgc_bitncommon(a[4..].as_ptr(), b[4..].as_ptr(), min_cb) }
        } else {
            0
        };
        assert!(cb == expect, "C calc vs direct C bitncommon");
        assert!(mb == (a[2] as c_int).min(b[2] as c_int));
    }

    /// Diagnostic: shipped Rust calc vs direct C bitncommon (Rust-vs-C).
    #[kani::proof]
    #[kani::unwind(22)]
    fn probe_union_rust_vs_c() {
        let a = any_gk_image_fam(AF_INET6);
        let b = any_gk_image_fam(AF_INET6);
        let imgs = [a, b];
        let p = calc_inet_union_params(imgs.iter().map(|i| GkRef::from_image(i)));
        let min_cb = (a[3] as c_int).min(b[3] as c_int);
        let expect = if min_cb > 0 {
            unsafe { pgc_bitncommon(a[4..].as_ptr(), b[4..].as_ptr(), min_cb) }
        } else {
            0
        };
        assert!(p.commonbits == expect, "Rust calc vs direct C bitncommon");
    }

    /// Diagnostic: shipped Rust calc vs direct RUST bitncommon (Rust-vs-Rust).
    #[kani::proof]
    #[kani::unwind(22)]
    fn probe_union_rust_vs_rust() {
        let a = any_gk_image_fam(AF_INET6);
        let b = any_gk_image_fam(AF_INET6);
        let imgs = [a, b];
        let p = calc_inet_union_params(imgs.iter().map(|i| GkRef::from_image(i)));
        let min_cb = (a[3] as i32).min(b[3] as i32);
        let expect = if min_cb > 0 {
            adt_network::bitncommon(&a[4..20], &b[4..20], min_cb)
        } else {
            0
        };
        assert!(p.commonbits == expect, "Rust calc vs direct Rust bitncommon");
    }

    /// Minimal witness A: a from_raw_parts slice taken from an
    /// iterator-yielded GkRef and HELD ACROSS the iterator loop, exactly as
    /// shipped calc_inet_union_params holds `addr`.
    #[kani::proof]
    #[kani::unwind(22)]
    fn probe_slice_across_iter_loop() {
        let a = any_gk_image_fam(AF_INET6);
        let b = any_gk_image_fam(AF_INET6);
        let imgs = [a, b];
        let mut it = imgs.iter().map(|i| GkRef::from_image(i));
        let first = it.next().unwrap();
        let addr = first.addr();
        let mut out = -1;
        for tmp in it {
            out = adt_network::bitncommon(addr, tmp.addr(), 128);
        }
        assert!(out == adt_network::bitncommon(&a[4..20], &b[4..20], 128));
    }

    /// Minimal witness C: same loop shape, iterator over PRE-BUILT GkRefs
    /// (from_image outside the closure). If green, the defect needs the
    /// GkRef construction inside the iterator adapter.
    #[kani::proof]
    #[kani::unwind(22)]
    fn probe_slice_prebuilt_refs() {
        let a = any_gk_image_fam(AF_INET6);
        let b = any_gk_image_fam(AF_INET6);
        let refs = [GkRef::from_image(&a), GkRef::from_image(&b)];
        let mut it = refs.iter().copied();
        let first = it.next().unwrap();
        let addr = first.addr();
        let mut out = -1;
        for tmp in it {
            out = adt_network::bitncommon(addr, tmp.addr(), 128);
        }
        assert!(out == adt_network::bitncommon(&a[4..20], &b[4..20], 128));
    }

    /// Minimal witness B: same shape without the iterator (plain GkRefs).
    #[kani::proof]
    #[kani::unwind(22)]
    fn probe_slice_no_iter() {
        let a = any_gk_image_fam(AF_INET6);
        let b = any_gk_image_fam(AF_INET6);
        let first = GkRef::from_image(&a);
        let second = GkRef::from_image(&b);
        let addr = first.addr();
        let out = adt_network::bitncommon(addr, second.addr(), 128);
        assert!(out == adt_network::bitncommon(&a[4..20], &b[4..20], 128));
    }

    // =================================================================
    // 3557 inet_gist_penalty — core-level, bit-exact f32
    // =================================================================

    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_penalty() {
        let a = any_gk_image();
        let b = any_gk_image();
        let mut cp: f32 = 0.0;
        unsafe { pg_inet_gist_penalty(a.as_ptr(), b.as_ptr(), &mut cp) };
        let r = penalty_internal(GkRef::from_image(&a), GkRef::from_image(&b));
        assert!(r.to_bits() == cp.to_bits(), "penalty f32 divergence");
    }

    /// All four penalty arms reachable inside the fence.
    #[kani::proof]
    #[kani::unwind(18)]
    fn cover_penalty_regimes() {
        let a = any_gk_image();
        let b = any_gk_image();
        let r = penalty_internal(GkRef::from_image(&a), GkRef::from_image(&b));
        kani::cover!(r == 4.0); // family mismatch
        kani::cover!(r == 3.0); // minbits degradation
        kani::cover!(r == 2.0); // zero common bits
        kani::cover!(r < 2.0); // 1/commonbits
    }

    // =================================================================
    // 3559 inet_gist_same — wrapper-level via NETWORK_GIST_BUILTINS
    // =================================================================

    fn call_same(a: &[u8; 20], b: &[u8; 20]) -> bool {
        assert!(NETWORK_GIST_BUILTINS[BI_SAME].foid == 3559);
        let mut out: bool = false;
        let mut f = LocalFcinfo::<3>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_usize(a.as_ptr() as usize));
        f.args[1] = NullableDatum::value(Datum::from_usize(b.as_ptr() as usize));
        f.args[2] = NullableDatum::value(Datum::from_usize(&mut out as *mut bool as usize));
        let fc = NETWORK_GIST_BUILTINS[BI_SAME].func;
        match fc(None, &mut f) {
            Ok(d) => assert!(d.as_usize() == &mut out as *mut bool as usize),
            Err(e) => {
                core::mem::forget(e);
                panic!("inet_gist_same errored");
            }
        }
        out
    }

    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_same() {
        let a = any_gk_image();
        let b = any_gk_image();
        let r = call_same(&a, &b);
        let c = unsafe { pg_inet_gist_same(a.as_ptr(), b.as_ptr()) };
        assert!((c != 0) == r);
    }

    /// Negative control (rig non-vacuity): shipped same(a, b) against
    /// C same(a, b') where b' swaps minbits — MUST FAIL with a decodable
    /// counterexample (equal keys, differing minbits). DEFAULT solver.
    #[kani::proof]
    #[kani::unwind(18)]
    fn control_same_minbits_swap() {
        let a = any_gk_image();
        let b = any_gk_image();
        let mut b2 = b;
        b2[2] = b2[2].wrapping_add(1);
        let r = call_same(&a, &b);
        let c = unsafe { pg_inet_gist_same(a.as_ptr(), b2.as_ptr()) };
        assert!((c != 0) == r);
    }

    // =================================================================
    // 3555 inet_gist_compress — wrapper-level, mcx-stubs, per-family cells
    // =================================================================

    fn compress_leaf_case(family: u8) {
        assert!(NETWORK_GIST_BUILTINS[BI_COMPRESS].foid == 3555);
        let bits: u8 = kani::any();
        kani::assume(bits <= maxbits(family));
        let ipaddr: [u8; 16] = kani::any();
        let v = InetValue {
            family,
            bits,
            ipaddr,
        };
        let (inimg, _inlen) = v.image();

        let ctx = mcx::MemoryContext::new_bump("kani-netgist");
        let offset: u16 = kani::any();
        let page_is_leaf: bool = kani::any();
        let entry = GISTENTRY {
            key: Datum::from_usize(inimg.as_ptr() as usize),
            offset,
            leafkey: true,
            page_is_leaf,
            rel_natts: 0,
        };
        let mut f = LocalFcinfo::<1>::new(0);
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(&entry as *const GISTENTRY as usize));
        let fc = NETWORK_GIST_BUILTINS[BI_COMPRESS].func;
        let d = match fc(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("inet_gist_compress errored");
            }
        };
        // gistentryinit protocol (C spec): offset/page preserved, leafkey off.
        let ret = unsafe { &*(d.as_usize() as *const GISTENTRY) };
        assert!(ret.offset == offset);
        assert!(!ret.leafkey);
        assert!(ret.page_is_leaf == page_is_leaf);

        // C side: verbatim key-construction fragment into a zeroed struct.
        let cin = CInet {
            family,
            bits,
            addr: ipaddr,
        };
        let mut ckey = [0u8; 20];
        unsafe { pg_inet_gist_compress_key(&cin, ckey.as_mut_ptr()) };

        let klen = gk_len(family); // concrete per cell
        assert!((ckey[0] >> 1) as usize == klen);
        let rkey = unsafe { core::slice::from_raw_parts(d_key_ptr(ret), klen) };
        for i in 0..klen {
            assert!(rkey[i] == ckey[i], "compress key image divergence");
        }
        core::mem::forget(ctx);
    }

    fn d_key_ptr(e: &GISTENTRY) -> *const u8 {
        e.key.as_usize() as *const u8
    }

    #[kani::proof]
    #[kani::unwind(20)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    fn eq_compress_leaf_v4() {
        compress_leaf_case(AF_INET);
    }

    // unwind 24: the harness's own klen=20 byte-compare loop needs > 20
    // (the v6 kissat timeout at unwind 20 was this unwinding assertion).
    #[kani::proof]
    #[kani::unwind(24)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    fn eq_compress_leaf_v6() {
        compress_leaf_case(AF_INET6);
    }

    /// Non-leaf arm: C returns the entry pointer unchanged; the shipped fc
    /// returns arg0 unchanged. Spec harness (both arms are structural
    /// returns; no C call needed).
    #[kani::proof]
    fn spec_compress_nonleaf_identity() {
        assert!(NETWORK_GIST_BUILTINS[BI_COMPRESS].foid == 3555);
        let entry = GISTENTRY {
            key: Datum::from_usize(kani::any()),
            offset: kani::any(),
            leafkey: false,
            page_is_leaf: kani::any(),
            rel_natts: 0,
        };
        let mut f = LocalFcinfo::<1>::new(0);
        let ep = &entry as *const GISTENTRY as usize;
        f.args[0] = NullableDatum::value(Datum::from_usize(ep));
        let fc = NETWORK_GIST_BUILTINS[BI_COMPRESS].func;
        match fc(None, &mut f) {
            Ok(d) => assert!(d.as_usize() == ep),
            Err(e) => {
                core::mem::forget(e);
                panic!("compress nonleaf errored");
            }
        }
    }

    /// NULL leaf key arm: C gistentryinit's key is (Datum) 0; shipped fc
    /// stores Datum::null(). Spec harness, mcx model (entry_result copy).
    #[kani::proof]
    #[kani::unwind(20)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    fn spec_compress_null_leafkey() {
        let ctx = mcx::MemoryContext::new_bump("kani-netgist");
        let offset: u16 = kani::any();
        let entry = GISTENTRY {
            key: Datum::from_usize(0),
            offset,
            leafkey: true,
            page_is_leaf: kani::any(),
            rel_natts: 0,
        };
        let mut f = LocalFcinfo::<1>::new(0);
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(&entry as *const GISTENTRY as usize));
        let fc = NETWORK_GIST_BUILTINS[BI_COMPRESS].func;
        match fc(None, &mut f) {
            Ok(d) => {
                let ret = unsafe { &*(d.as_usize() as *const GISTENTRY) };
                assert!(ret.key.as_usize() == 0);
                assert!(ret.offset == offset);
                assert!(!ret.leafkey);
            }
            Err(e) => {
                core::mem::forget(e);
                panic!("compress null-key errored");
            }
        }
        core::mem::forget(ctx);
    }

    // =================================================================
    // 3573 inet_gist_fetch — wrapper-level, mcx-stubs, per-family cells
    // =================================================================

    fn fetch_case(family: u8) {
        assert!(NETWORK_GIST_BUILTINS[BI_FETCH].foid == 3573);
        let img = any_gk_image_fam(family);

        let ctx = mcx::MemoryContext::new_bump("kani-netgist");
        let offset: u16 = kani::any();
        let page_is_leaf: bool = kani::any();
        let entry = GISTENTRY {
            key: Datum::from_usize(img.as_ptr() as usize),
            offset,
            leafkey: false,
            page_is_leaf,
            rel_natts: 0,
        };
        let mut f = LocalFcinfo::<1>::new(0);
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(&entry as *const GISTENTRY as usize));
        let fc = NETWORK_GIST_BUILTINS[BI_FETCH].func;
        let d = match fc(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("inet_gist_fetch errored");
            }
        };
        let ret = unsafe { &*(d.as_usize() as *const GISTENTRY) };
        assert!(ret.offset == offset);
        assert!(!ret.leafkey);
        assert!(ret.page_is_leaf == page_is_leaf);

        // C side: verbatim value-construction fragment into a zeroed inet.
        let mut cdst = CInet {
            family: 0,
            bits: 0,
            addr: [0u8; 16],
        };
        unsafe { pg_inet_gist_fetch_val(img.as_ptr(), &mut cdst) };

        // Result datum: 4B-header inet varlena image. addrsize here is the
        // INET-keyed ip_addrsize (4 for v4, 16 otherwise) — concrete per
        // cell. The varlena header is Rust-side spec (SET_INET_VARSIZE is
        // header housekeeping outside the compared C value).
        let addrsize: usize = if family == AF_INET { 4 } else { 16 };
        let vlen = 4 + 2 + addrsize;
        let rimg = unsafe { core::slice::from_raw_parts(d_key_ptr(ret), vlen) };
        let hdr = datum::varlena::set_varsize_4b(vlen);
        for i in 0..4 {
            assert!(rimg[i] == hdr[i], "fetch varlena header");
        }
        assert!(rimg[4] == cdst.family, "fetch family divergence");
        assert!(rimg[5] == cdst.bits, "fetch bits divergence");
        for i in 0..addrsize {
            assert!(rimg[6 + i] == cdst.addr[i], "fetch addr divergence");
        }
        core::mem::forget(ctx);
    }

    #[kani::proof]
    #[kani::unwind(20)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    fn eq_fetch_v4() {
        fetch_case(AF_INET);
    }

    #[kani::proof]
    #[kani::unwind(20)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    fn eq_fetch_v6() {
        fetch_case(AF_INET6);
    }
}
