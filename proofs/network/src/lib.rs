//! Kani C≡Rust equivalence: inet bit helpers bitncmp / bitncommon.
//! Rust side: shipped crates/backend/utils/adt/network. C side: vendored
//! network.c (csrc/net_shim.c). Two symbolic 8-byte arrays, symbolic bit
//! count 0..=64 (covers the IPv4 domain 0..=32 and the general byte/bit
//! split logic; IPv6 uses the same code over 16 bytes).
//!
//! EXTENSION (net-ops, 2026-07-28): the inet/cidr operator rows, composed
//! on the proved bitncmp/bitncommon kernels. C side: csrc/net_ops.c
//! (REL_18_STABLE network.c, see its header for shims).
//!
//! - Comparators/predicates/scalars (network_{eq,ne,lt,le,gt,ge,cmp,sub,
//!   subeq,sup,supeq,overlap,smaller,larger,masklen,family},
//!   inet_same_family): WRAPPER-LEVEL — the shipped fc_* is invoked through
//!   a real LocalFcinfo frame whose args are pointer datums at 4B-header
//!   inet varlena images (InetValue::image()), so datum unwrap
//!   (arg_varlena_packed -> InetRef::from_payload) and Datum packing are
//!   inside each theorem.
//! - Value model: symbolic (family, bits, addr[16]) fenced to the datatype
//!   invariant every constructor (network_in/network_recv) enforces:
//!   family in {PGSQL_AF_INET, PGSQL_AF_INET6}, bits <= maxbits(family).
//!   addr is FULLY symbolic — C's inet does NOT require masked-clean
//!   address bits (only cidr values are masked at construction), and
//!   network_cmp's masklen tiebreak + full-width third compare are proved
//!   over dirty addresses too. network_family/masklen are proved over
//!   FULLY symbolic (family, bits) u8 pairs (no fence): both sides'
//!   default arms are in the theorem.
//! - inet-BUILDING rows (network_{broadcast,netmask,hostmask}, inet_merge):
//!   CORE-LEVEL (crate::network_broadcast etc. vs the verbatim C bodies),
//!   asserting FULL output struct equality (family, bits, all 16 addr
//!   bytes; C dst is palloc0-zeroed, harness dst starts zeroed the same).
//!   The fc_* wrapper tier above them (byref_result arena allocation) is
//!   the measured mcx harness-blocker (hex precedent) and stays in the
//!   tested tier.
//! - inet_merge error arm: eq_inet_merge_mismatch proves the Err verdict +
//!   sqlstate/level parity over mismatched-family pairs with PgError::error
//!   stubbed field-identically (message text + Location leave the proof —
//!   cash precedent; needs `-Z stubbing`); eq_inet_merge proves the
//!   stub-free same-family value path; cover_merge_cases witnesses the
//!   two-case partition.
//! - Comparator/predicate harnesses are family-case-split (_v4/_v6/_xf,
//!   union coverage cover_family_cases): fully-symbolic-family versions
//!   measured 10-30s+ under load; the split holds every member <10s.
//! - Negative control: control_network_sub_vs_c_subeq (shipped fc_network_sub
//!   vs C network_subeq) — must FAIL (counterexample at bits1 == bits2,
//!   same network). Run with the DEFAULT solver, expected-green harnesses
//!   with kissat.

#[cfg(kani)]
mod proofs {
    extern "C" {
        fn pgc_bitncmp(l: *const u8, r: *const u8, n: i32) -> i32;
        fn pgc_bitncommon(l: *const u8, r: *const u8, n: i32) -> i32;
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn bitncmp_equiv() {
        let l: [u8; 8] = kani::any();
        let r: [u8; 8] = kani::any();
        let n: i32 = kani::any();
        kani::assume((0..=64).contains(&n));
        // n == 64 would read l[8] in the partial-byte branch — but 64 % 8 == 0
        // so neither implementation does; both sides stay in-bounds.

        let rust = adt_network::bitncmp(&l, &r, n);
        let c = unsafe { pgc_bitncmp(l.as_ptr(), r.as_ptr(), n) };
        assert_eq!(rust, c, "bitncmp exact-value divergence");
        assert_eq!(rust.signum(), c.signum(), "bitncmp sign divergence");
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn bitncommon_equiv() {
        let l: [u8; 8] = kani::any();
        let r: [u8; 8] = kani::any();
        let n: i32 = kani::any();
        // n == 64 with all bytes equal would read l[8] (nbits path is skipped
        // only when nbits == 0 AND no byte differed... it is skipped: nbits=0).
        kani::assume((0..=64).contains(&n));

        let rust = adt_network::bitncommon(&l, &r, n);
        let c = unsafe { pgc_bitncommon(l.as_ptr(), r.as_ptr(), n) };
        assert_eq!(rust, c);
    }
}

// ================= net-ops extension =================

#[cfg(kani)]
mod op_proofs {
    use adt_network::{InetValue, PGSQL_AF_INET, PGSQL_AF_INET6};
    use datum::{Datum, NullableDatum};
    use types_error::{PgError, ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_PARAMETER_VALUE, ERROR};
    use proof_support::stubs;
    use types_fmgr::LocalFcinfo;

    use std::os::raw::c_int;

    /// C-side inet value model (csrc/net_ops.c pgc_inet).
    #[repr(C)]
    struct CInet {
        family: u8,
        bits: u8,
        addr: [u8; 16],
    }

    extern "C" {
        fn pg_network_lt(a1: *const CInet, a2: *const CInet) -> c_int;
        fn pg_network_le(a1: *const CInet, a2: *const CInet) -> c_int;
        fn pg_network_eq(a1: *const CInet, a2: *const CInet) -> c_int;
        fn pg_network_ge(a1: *const CInet, a2: *const CInet) -> c_int;
        fn pg_network_gt(a1: *const CInet, a2: *const CInet) -> c_int;
        fn pg_network_ne(a1: *const CInet, a2: *const CInet) -> c_int;
        fn pg_network_cmp(a1: *const CInet, a2: *const CInet) -> c_int;
        fn pg_network_smaller(a1: *const CInet, a2: *const CInet) -> c_int;
        fn pg_network_larger(a1: *const CInet, a2: *const CInet) -> c_int;
        fn pg_network_sub(a1: *const CInet, a2: *const CInet) -> c_int;
        fn pg_network_subeq(a1: *const CInet, a2: *const CInet) -> c_int;
        fn pg_network_sup(a1: *const CInet, a2: *const CInet) -> c_int;
        fn pg_network_supeq(a1: *const CInet, a2: *const CInet) -> c_int;
        fn pg_network_overlap(a1: *const CInet, a2: *const CInet) -> c_int;
        fn pg_network_masklen(ip: *const CInet) -> c_int;
        fn pg_network_family(ip: *const CInet) -> c_int;
        // int-not-void shims: Kani's Rust () lowers as `struct Unit`, which
        // goto-cc rejects against C void — the C side returns a dummy 0.
        fn pg_network_broadcast(ip: *const CInet, dst: *mut CInet) -> c_int;
        fn pg_network_network(ip: *const CInet, dst: *mut CInet) -> c_int;
        fn pg_inetnot(ip: *const CInet, dst: *mut CInet) -> c_int;
        fn pg_network_netmask(ip: *const CInet, dst: *mut CInet) -> c_int;
        fn pg_network_hostmask(ip: *const CInet, dst: *mut CInet) -> c_int;
        fn pg_inet_same_family(a1: *const CInet, a2: *const CInet) -> c_int;
        fn pg_inet_merge(a1: *const CInet, a2: *const CInet, dst: *mut CInet) -> c_int;
    }

    /// Symbolic inet fenced to the datatype invariant (see module doc).
    fn any_inet() -> InetValue {
        let family: u8 = kani::any();
        kani::assume(family == PGSQL_AF_INET || family == PGSQL_AF_INET6);
        let v = InetValue {
            family,
            bits: kani::any(),
            ipaddr: kani::any(),
        };
        kani::assume(v.bits <= v.maxbits());
        v
    }

    fn cin(v: &InetValue) -> CInet {
        CInet {
            family: v.family,
            bits: v.bits,
            addr: v.ipaddr,
        }
    }

    /// palloc0 shim counterpart: C fills a zeroed dst.
    fn czero() -> CInet {
        CInet {
            family: 0,
            bits: 0,
            addr: [0u8; 16],
        }
    }

    /// 4B-header varlena image of an inet value (the exact fmgr arg form).
    fn img(v: &InetValue) -> [u8; 22] {
        v.image().0
    }

    /// Run a shipped 2-arg fc_* wrapper on a LocalFcinfo frame; these
    /// wrappers never error on inline varlena args.
    fn call2<E>(
        fc: fn(
            Option<&mut types_fmgr::FmgrInfo>,
            &mut types_fmgr::FunctionCallInfoBaseData,
        ) -> Result<Datum, E>,
        a: Datum,
        b: Datum,
    ) -> Datum {
        let mut f = LocalFcinfo::<2>::new(0);
        f.args[0] = NullableDatum::value(a);
        f.args[1] = NullableDatum::value(b);
        match fc(None, &mut f) {
            Ok(d) => d,
            Err(_) => panic!("network op errored"),
        }
    }

    fn call1<E>(
        fc: fn(
            Option<&mut types_fmgr::FmgrInfo>,
            &mut types_fmgr::FunctionCallInfoBaseData,
        ) -> Result<Datum, E>,
        a: Datum,
    ) -> Datum {
        let mut f = LocalFcinfo::<1>::new(0);
        f.args[0] = NullableDatum::value(a);
        match fc(None, &mut f) {
            Ok(d) => d,
            Err(_) => panic!("network op errored"),
        }
    }

    // ---------- bool operators, wrapper-level ----------

    /// Symbolic inet of a FIXED family (family case-split, ladder step 4:
    /// the fully-symbolic-family comparator harnesses sit in the 10-30s
    /// band / wall; per-family harnesses tighten the address-loop depth).
    fn any_inet_fam(family: u8) -> InetValue {
        let v = InetValue {
            family,
            bits: kani::any(),
            ipaddr: kani::any(),
        };
        kani::assume(v.bits <= v.maxbits());
        v
    }

    /// Union-coverage witness for the (family, family) case-split: every
    /// fenced input pair falls in exactly one of v4/v6/xf. MANDATORY
    /// companion of the split harnesses below.
    #[kani::proof]
    fn cover_family_cases() {
        let a = any_inet();
        let b = any_inet();
        let v4 = a.family == PGSQL_AF_INET && b.family == PGSQL_AF_INET;
        let v6 = a.family == PGSQL_AF_INET6 && b.family == PGSQL_AF_INET6;
        let xf = a.family != b.family;
        assert!(v4 || v6 || xf);
    }

    /// bool ops over a fully symbolic fenced domain (single harness — these
    /// measured within budget without a split).
    macro_rules! net_bool2 {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(18)]
            fn $h() {
                let a = any_inet();
                let b = any_inet();
                net_bool2_body!(a, b, $fc, $pg);
            }
        )*};
    }

    /// bool ops family-case-split: _v4 (both AF_INET, unwind 9 = 4-byte
    /// memcmp + 7-bit partial loop + 1), _v6 (both AF_INET6, unwind 18),
    /// _xf (mixed families — address loops unreached on both sides).
    macro_rules! net_bool2_split {
        ($($v4:ident / $v6:ident / $xf:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(9)]
            fn $v4() {
                let a = any_inet_fam(PGSQL_AF_INET);
                let b = any_inet_fam(PGSQL_AF_INET);
                net_bool2_body!(a, b, $fc, $pg);
            }
            #[kani::proof]
            #[kani::unwind(18)]
            fn $v6() {
                let a = any_inet_fam(PGSQL_AF_INET6);
                let b = any_inet_fam(PGSQL_AF_INET6);
                net_bool2_body!(a, b, $fc, $pg);
            }
            #[kani::proof]
            #[kani::unwind(18)]
            fn $xf() {
                let a = any_inet();
                let b = any_inet();
                kani::assume(a.family != b.family);
                net_bool2_body!(a, b, $fc, $pg);
            }
        )*};
    }

    macro_rules! net_bool2_body {
        ($a:ident, $b:ident, $fc:ident, $pg:ident) => {
            let (ia, ib) = (img(&$a), img(&$b));
            let r = call2(
                adt_network::builtins::$fc,
                Datum::from_usize(ia.as_ptr() as usize),
                Datum::from_usize(ib.as_ptr() as usize),
            );
            let (ca, cb) = (cin(&$a), cin(&$b));
            let c = unsafe { $pg(&ca, &cb) };
            assert!(r.as_bool() as c_int == c);
        };
    }

    net_bool2! {
        eq_inet_same_family: fc_inet_same_family / pg_inet_same_family;
    }

    net_bool2_split! {
        eq_network_ne_v4 / eq_network_ne_v6 / eq_network_ne_xf:
            fc_network_ne / pg_network_ne;
        eq_network_sup_v4 / eq_network_sup_v6 / eq_network_sup_xf:
            fc_network_sup / pg_network_sup;
        eq_network_supeq_v4 / eq_network_supeq_v6 / eq_network_supeq_xf:
            fc_network_supeq / pg_network_supeq;
        eq_network_overlap_v4 / eq_network_overlap_v6 / eq_network_overlap_xf:
            fc_network_overlap / pg_network_overlap;
        eq_network_eq_v4 / eq_network_eq_v6 / eq_network_eq_xf:
            fc_network_eq / pg_network_eq;
        eq_network_lt_v4 / eq_network_lt_v6 / eq_network_lt_xf:
            fc_network_lt / pg_network_lt;
        eq_network_le_v4 / eq_network_le_v6 / eq_network_le_xf:
            fc_network_le / pg_network_le;
        eq_network_gt_v4 / eq_network_gt_v6 / eq_network_gt_xf:
            fc_network_gt / pg_network_gt;
        eq_network_ge_v4 / eq_network_ge_v6 / eq_network_ge_xf:
            fc_network_ge / pg_network_ge;
        eq_network_sub_v4 / eq_network_sub_v6 / eq_network_sub_xf:
            fc_network_sub / pg_network_sub;
        eq_network_subeq_v4 / eq_network_subeq_v6 / eq_network_subeq_xf:
            fc_network_subeq / pg_network_subeq;
    }

    macro_rules! net_cmp_body {
        ($a:ident, $b:ident) => {
            let (ia, ib) = (img(&$a), img(&$b));
            let r = call2(
                adt_network::builtins::fc_network_cmp,
                Datum::from_usize(ia.as_ptr() as usize),
                Datum::from_usize(ib.as_ptr() as usize),
            );
            let (ca, cb) = (cin(&$a), cin(&$b));
            let c = unsafe { pg_network_cmp(&ca, &cb) };
            assert!(r.as_i32() == c);
        };
    }

    #[kani::proof]
    #[kani::unwind(9)]
    fn eq_network_cmp_v4() {
        let a = any_inet_fam(PGSQL_AF_INET);
        let b = any_inet_fam(PGSQL_AF_INET);
        net_cmp_body!(a, b);
    }

    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_network_cmp_v6() {
        let a = any_inet_fam(PGSQL_AF_INET6);
        let b = any_inet_fam(PGSQL_AF_INET6);
        net_cmp_body!(a, b);
    }

    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_network_cmp_xf() {
        let a = any_inet();
        let b = any_inet();
        kani::assume(a.family != b.family);
        net_cmp_body!(a, b);
    }

    /// smaller/larger return the winning INPUT datum (C: the winning input
    /// pointer); the C shim reports the winning arg index.
    macro_rules! net_minmax_body {
        ($a:ident, $b:ident, $fc:ident, $pg:ident) => {
            let (ia, ib) = (img(&$a), img(&$b));
            let r = call2(
                adt_network::builtins::$fc,
                Datum::from_usize(ia.as_ptr() as usize),
                Datum::from_usize(ib.as_ptr() as usize),
            );
            let (ca, cb) = (cin(&$a), cin(&$b));
            let c = unsafe { $pg(&ca, &cb) };
            let want = if c == 0 { ia.as_ptr() } else { ib.as_ptr() } as usize;
            assert!(r.as_usize() == want);
        };
    }

    macro_rules! net_minmax_split {
        ($($v4:ident / $v6:ident / $xf:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(9)]
            fn $v4() {
                let a = any_inet_fam(PGSQL_AF_INET);
                let b = any_inet_fam(PGSQL_AF_INET);
                net_minmax_body!(a, b, $fc, $pg);
            }
            #[kani::proof]
            #[kani::unwind(18)]
            fn $v6() {
                let a = any_inet_fam(PGSQL_AF_INET6);
                let b = any_inet_fam(PGSQL_AF_INET6);
                net_minmax_body!(a, b, $fc, $pg);
            }
            #[kani::proof]
            #[kani::unwind(18)]
            fn $xf() {
                let a = any_inet();
                let b = any_inet();
                kani::assume(a.family != b.family);
                net_minmax_body!(a, b, $fc, $pg);
            }
        )*};
    }

    net_minmax_split! {
        eq_network_smaller_v4 / eq_network_smaller_v6 / eq_network_smaller_xf:
            fc_network_smaller / pg_network_smaller;
        eq_network_larger_v4 / eq_network_larger_v6 / eq_network_larger_xf:
            fc_network_larger / pg_network_larger;
    }

    // ---------- scalar extractors: FULLY symbolic (family, bits) ----------

    #[kani::proof]
    fn eq_network_masklen() {
        let v = InetValue {
            family: kani::any(),
            bits: kani::any(),
            ipaddr: kani::any(),
        };
        let ia = img(&v);
        let r = call1(
            adt_network::builtins::fc_network_masklen,
            Datum::from_usize(ia.as_ptr() as usize),
        );
        let ca = cin(&v);
        let c = unsafe { pg_network_masklen(&ca) };
        assert!(r.as_i32() == c);
    }

    #[kani::proof]
    fn eq_network_family() {
        let v = InetValue {
            family: kani::any(),
            bits: kani::any(),
            ipaddr: kani::any(),
        };
        let ia = img(&v);
        let r = call1(
            adt_network::builtins::fc_network_family,
            Datum::from_usize(ia.as_ptr() as usize),
        );
        let ca = cin(&v);
        let c = unsafe { pg_network_family(&ca) };
        assert!(r.as_i32() == c);
    }

    // ---------- inet-building rows, core-level (see module doc) ----------

    macro_rules! net_build1 {
        ($($h:ident: $core:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(18)]
            fn $h() {
                let a = any_inet();
                let r = adt_network::$core(a.iref());
                let ca = cin(&a);
                let mut cd = czero();
                let _ = unsafe { $pg(&ca, &mut cd) };
                assert!(r.family == cd.family, "family");
                assert!(r.bits == cd.bits, "bits");
                assert!(r.ipaddr == cd.addr, "addr");
            }
        )*};
    }

    net_build1! {
        eq_network_broadcast: network_broadcast / pg_network_broadcast;
        eq_network_netmask: network_netmask / pg_network_netmask;
        eq_network_hostmask: network_hostmask / pg_network_hostmask;
        eq_network_network: network_network / pg_network_network;
        eq_inetnot: inetnot / pg_inetnot;
    }

    // inetand/inetor (oids 2628/2629): same-family value path core-level
    // (full output struct equality), plus the family-mismatch Err arm
    // (verdict + sqlstate/level parity, inet_merge pattern).

    extern "C" {
        fn pg_inetand(ip: *const CInet, ip2: *const CInet, dst: *mut CInet) -> c_int;
        fn pg_inetor(ip: *const CInet, ip2: *const CInet, dst: *mut CInet) -> c_int;
    }

    macro_rules! net_bitop2 {
        ($($h:ident / $herr:ident: $core:ident / $pg:ident;)*) => {$(
            // The (unreachable, same-family fence) Err arm builds its
            // message via format! — stub the error+format machinery so the
            // dead arm's alloc/fmt plumbing stays out of symex (the merge
            // row's literal-message arm didn't need this; format! does).
            #[kani::proof]
            #[kani::unwind(18)]
            #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
            #[kani::stub(alloc::fmt::format, stubs::stub_format)]
            fn $h() {
                let a = any_inet();
                let mut b = any_inet();
                b.family = a.family;
                kani::assume(b.bits <= b.maxbits());
                let r = match adt_network::$core(a.iref(), b.iref()) {
                    Ok(v) => v,
                    Err(_) => panic!("same-family bitop errored"),
                };
                let (ca, cb) = (cin(&a), cin(&b));
                let mut cd = czero();
                let cerr = unsafe { $pg(&ca, &cb, &mut cd) };
                assert!(cerr == 0);
                assert!(r.family == cd.family, "family");
                assert!(r.bits == cd.bits, "bits");
                assert!(r.ipaddr == cd.addr, "addr");
            }

            #[kani::proof]
            #[kani::unwind(18)]
            #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
            #[kani::stub(alloc::fmt::format, stubs::stub_format)]
            fn $herr() {
                let a = any_inet();
                let b = any_inet();
                kani::assume(a.family != b.family);
                let (ca, cb) = (cin(&a), cin(&b));
                let mut cd = czero();
                let cerr = unsafe { $pg(&ca, &cb, &mut cd) };
                match adt_network::$core(a.iref(), b.iref()) {
                    Ok(_) => assert!(false, "Rust accepted a family mismatch"),
                    Err(e) => {
                        let ok = cerr == -1
                            && e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE
                            && e.level == ERROR;
                        core::mem::forget(e);
                        assert!(ok);
                    }
                }
            }
        )*};
    }

    net_bitop2! {
        eq_inetand / eq_inetand_mismatch: inetand / pg_inetand;
        eq_inetor / eq_inetor_mismatch: inetor / pg_inetor;
    }

    /// Same-family value path, zero stubs: full output struct equality.
    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_inet_merge() {
        let a = any_inet();
        let mut b = any_inet();
        b.family = a.family;
        kani::assume(b.bits <= b.maxbits());
        let r = match adt_network::inet_merge(a.iref(), b.iref()) {
            Ok(v) => v,
            Err(_) => panic!("same-family inet_merge errored"),
        };
        let (ca, cb) = (cin(&a), cin(&b));
        let mut cd = czero();
        let cerr = unsafe { pg_inet_merge(&ca, &cb, &mut cd) };
        assert!(cerr == 0);
        assert!(r.family == cd.family, "family");
        assert!(r.bits == cd.bits, "bits");
        assert!(r.ipaddr == cd.addr, "addr");
    }

    /// Stub for `PgError::error`: field-identical to the shipped constructor
    /// minus message text and `Location::caller()` (Kani-unsupported); the
    /// shipped `.with_sqlstate(..)` stays load-bearing (cash precedent).
    fn stub_pg_error_error(_message: impl Into<String>) -> PgError {
        PgError {
            level: ERROR,
            sqlstate: ERRCODE_INTERNAL_ERROR,
            message: String::new(),
            message_raw: None,
            detail: None,
            detail_log: None,
            hint: None,
            context: None,
            backtrace: None,
            message_id: None,
            domain: None,
            context_domain: None,
            hide_statement: false,
            hide_context: false,
            location: None,
            saved_errno: None,
            cursor_position: None,
            internal_position: None,
            internal_query: None,
            schema_name: None,
            table_name: None,
            column_name: None,
            datatype_name: None,
            constraint_name: None,
            plpgsql_context_attached: false,
        }
    }

    /// Family-mismatch arm: Err verdict + sqlstate/level parity against C's
    /// ereport sentinel (error message text left out of the proof — see
    /// module doc). With eq_inet_merge (same-family, Ok asserted both
    /// sides) this partitions the fenced domain: verdict parity everywhere.
    #[kani::proof]
    #[kani::unwind(18)]
    #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
    fn eq_inet_merge_mismatch() {
        let a = any_inet();
        let b = any_inet();
        kani::assume(a.family != b.family);
        let (ca, cb) = (cin(&a), cin(&b));
        let mut cd = czero();
        let cerr = unsafe { pg_inet_merge(&ca, &cb, &mut cd) };
        assert!(cerr == -1);
        match adt_network::inet_merge(a.iref(), b.iref()) {
            Ok(_) => panic!("mismatched families merged"),
            Err(e) => {
                assert!(e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE);
                assert!(e.level == ERROR);
            }
        }
    }

    /// Union-coverage witness for the merge two-case split (same-family
    /// value proof / mismatch verdict proof).
    #[kani::proof]
    fn cover_merge_cases() {
        let a = any_inet();
        let b = any_inet();
        assert!(a.family == b.family || a.family != b.family);
    }

    // ---------- negative control: rig must be able to fail ----------

    /// Deliberate mismatch: shipped fc_network_sub vs C network_subeq. MUST
    /// fail with a counterexample at equal networks. DEFAULT solver (kissat
    /// is non-incremental and grinds forever on failing harnesses).
    #[kani::proof]
    #[kani::unwind(18)]
    fn control_network_sub_vs_c_subeq() {
        let a = any_inet();
        let b = any_inet();
        let (ia, ib) = (img(&a), img(&b));
        let r = call2(
            adt_network::builtins::fc_network_sub,
            Datum::from_usize(ia.as_ptr() as usize),
            Datum::from_usize(ib.as_ptr() as usize),
        );
        let (ca, cb) = (cin(&a), cin(&b));
        let c = unsafe { pg_network_subeq(&ca, &cb) };
        assert!(r.as_bool() as c_int == c);
    }
}

// ================= net-arith extension (WAVE 11) =================
//
// inet arithmetic + set_masklen + hashinet rows, composed on the same
// symbolic value model as op_proofs. C side: csrc/net_arith.c
// (REL_18_STABLE network.c, see its header for shims + error sentinels).
//
// - inetpl 2630 / inetmi_int8 2632 / inetmi 2633: CORE-LEVEL
//   (adt_network::internal_inetpl / inetmi vs the verbatim C bodies),
//   family-case-split (v4/v6; xf only where the C body branches on it —
//   inetmi). Both result arms are in each theorem: Ok asserts full output
//   struct (or i64) equality, Err asserts verdict + sqlstate/level parity
//   against the C sentinel (message text + Location out of proof via the
//   canonical proof_support stubs; the shipped .with_sqlstate stays
//   load-bearing — cash precedent). Err Boxes are mem::forget-ed
//   (Box<PgError> drop-glue trap, proofs/varbit-rows).
//   inetmi_int8 negation plane: C computes -addend under -fwrapv (wraps at
//   INT64_MIN), CBMC models two's-complement wrap, shipped Rust uses
//   wrapping_neg() — addend is full-i64 INCLUDING i64::MIN, in-theorem.
//   inetpl v6 note: any i64 addend is absorbed by a 16-byte address, so the
//   out-of-range arm is v4-only (cover_inetpl_arms witnesses both v4 arms).
// - inet_to_cidr 1715, inet_set_masklen 605, cidr_set_masklen 635:
//   CORE-LEVEL, single total harness per row over the fenced inet ×
//   FULL-i32 bits domain, match on arms (Ok: full struct eq incl. the
//   masked partial byte and palloc0-zeroed tail; Err: verdict + sqlstate
//   22023 parity). inet_to_cidr's C elog arm (invalid stored bit length)
//   is unreachable under the datatype invariant BOTH sides — asserted
//   unreachable (cerr != -3), not fenced.
// - hashinet 422 / hashinetextended 779: COMPOSED — C hashes
//   VARDATA_ANY(addr) for addrsize+2 bytes with hash_any; pg_hashinet_view
//   reproduces exactly that byte string (see net_arith.c header), and the
//   theorem asserts shipped hashinet_bytes[_extended] == shipped
//   hash_bytes[_extended] over the C-assembled view. Equivalence
//   hash_bytes ≡ hash_any is the already-proved proofs/hash row; this
//   theorem contributes the byte-view (prefix assembly + length) parity.
// - Negative control (one per new C section): control_inetpl_vs_c_inetmi_int8
//   (sign flip) — MUST FAIL; DEFAULT solver. control_hashinet_view_skew
//   (length-skewed view) — MUST FAIL; DEFAULT solver.
#[cfg(kani)]
mod arith_proofs {
    use adt_network::{InetValue, PGSQL_AF_INET, PGSQL_AF_INET6};
    use proof_support::stubs;
    use types_error::{ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERROR};

    use std::os::raw::c_int;

    /// C-side inet value model (csrc/net_arith.c pgc_inet).
    #[repr(C)]
    struct CInet {
        family: u8,
        bits: u8,
        addr: [u8; 16],
    }

    extern "C" {
        // error sentinels (net_arith.c header): 0 ok; -1 = 22023
        // invalid_parameter_value; -2 = 22003 numeric_value_out_of_range;
        // -3 = inet_to_cidr's internal elog.
        fn pg_inetpl(ip: *const CInet, addend: i64, dst: *mut CInet) -> c_int;
        fn pg_inetmi_int8(ip: *const CInet, addend: i64, dst: *mut CInet) -> c_int;
        fn pg_inetmi(ip: *const CInet, ip2: *const CInet, out: *mut i64) -> c_int;
        fn pg_inet_to_cidr(src: *const CInet, dst: *mut CInet) -> c_int;
        fn pg_inet_set_masklen(src: *const CInet, bits: c_int, dst: *mut CInet) -> c_int;
        fn pg_cidr_set_masklen(src: *const CInet, bits: c_int, dst: *mut CInet) -> c_int;
        fn pg_hashinet_view(addr: *const CInet, out: *mut u8) -> c_int;
    }

    /// Symbolic inet fenced to the datatype invariant (op_proofs precedent).
    fn any_inet() -> InetValue {
        let family: u8 = kani::any();
        kani::assume(family == PGSQL_AF_INET || family == PGSQL_AF_INET6);
        let v = InetValue {
            family,
            bits: kani::any(),
            ipaddr: kani::any(),
        };
        kani::assume(v.bits <= v.maxbits());
        v
    }

    /// Symbolic inet of a FIXED family (family case-split).
    fn any_inet_fam(family: u8) -> InetValue {
        let v = InetValue {
            family,
            bits: kani::any(),
            ipaddr: kani::any(),
        };
        kani::assume(v.bits <= v.maxbits());
        v
    }

    fn cin(v: &InetValue) -> CInet {
        CInet {
            family: v.family,
            bits: v.bits,
            addr: v.ipaddr,
        }
    }

    /// palloc0 shim counterpart: C fills a zeroed dst.
    fn czero() -> CInet {
        CInet {
            family: 0,
            bits: 0,
            addr: [0u8; 16],
        }
    }

    /// Union-coverage witness for the single-arg family split (v4/v6
    /// harness pairs below): the fenced domain is exactly the union.
    #[kani::proof]
    fn cover_arith_family_cases() {
        let a = any_inet();
        assert!(a.family == PGSQL_AF_INET || a.family == PGSQL_AF_INET6);
    }

    // ---------- inetpl / inetmi_int8 (oids 2630 / 2632) ----------

    /// Both arms of one addend-carry row: Ok = full struct eq; Err =
    /// verdict + 22003 sqlstate/level parity (v4 only — see module doc).
    macro_rules! net_pl_body {
        ($a:ident, $addend:ident, $core_addend:expr, $pg:ident) => {
            let ca = cin(&$a);
            let mut cd = czero();
            let cerr = unsafe { $pg(&ca, $addend, &mut cd) };
            match adt_network::internal_inetpl($a.iref(), $core_addend) {
                Ok(r) => {
                    assert!(cerr == 0, "C errored where Rust succeeded");
                    assert!(r.family == cd.family, "family");
                    assert!(r.bits == cd.bits, "bits");
                    assert!(r.ipaddr == cd.addr, "addr");
                }
                Err(e) => {
                    let ok = cerr == -2
                        && e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
                        && e.level == ERROR;
                    core::mem::forget(e);
                    assert!(ok, "out-of-range arm parity");
                }
            }
        };
    }

    macro_rules! net_pl_split {
        ($($v4:ident / $v6:ident: $pg:ident, $neg:expr;)*) => {$(
            #[kani::proof]
            // unwind 18 (NOT the family's v4 default 9): the result-image
            // memcmp runs over the full 16-byte addr buffer even for v4
            // images — fleet batchB failed "unwinding assertion memcmp.0
            // iteration 9" at unwind(9); image compares need
            // unwind > image bytes + 1.
            #[kani::unwind(18)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $v4() {
                let a = any_inet_fam(PGSQL_AF_INET);
                let addend: i64 = kani::any();
                let core_addend = if $neg { addend.wrapping_neg() } else { addend };
                net_pl_body!(a, addend, core_addend, $pg);
            }
            #[kani::proof]
            #[kani::unwind(18)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $v6() {
                let a = any_inet_fam(PGSQL_AF_INET6);
                let addend: i64 = kani::any();
                let core_addend = if $neg { addend.wrapping_neg() } else { addend };
                net_pl_body!(a, addend, core_addend, $pg);
            }
        )*};
    }

    net_pl_split! {
        eq_inetpl_v4 / eq_inetpl_v6: pg_inetpl, false;
        eq_inetmi_int8_v4 / eq_inetmi_int8_v6: pg_inetmi_int8, true;
    }

    /// Arm witnesses for the inetpl/inetmi_int8 theorems (covers hoisted
    /// into one harness — per-inline-cover SAT-call cost, varbit lesson).
    #[kani::proof]
    #[kani::unwind(9)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn cover_inetpl_arms() {
        let a = any_inet_fam(PGSQL_AF_INET);
        let addend: i64 = kani::any();
        let r = adt_network::internal_inetpl(a.iref(), addend);
        kani::cover!(r.is_ok(), "inetpl Ok arm reachable");
        kani::cover!(r.is_err(), "inetpl out-of-range arm reachable (v4)");
        core::mem::forget(r);
    }

    // ---------- inetmi (oid 2633) ----------

    macro_rules! net_mi_body {
        ($a:ident, $b:ident) => {
            let (ca, cb) = (cin(&$a), cin(&$b));
            let mut cres: i64 = 0;
            let cerr = unsafe { pg_inetmi(&ca, &cb, &mut cres) };
            match adt_network::inetmi($a.iref(), $b.iref()) {
                Ok(r) => {
                    assert!(cerr == 0, "C errored where Rust succeeded");
                    assert!(r == cres, "difference value");
                }
                Err(e) => {
                    let want = if e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE {
                        -1 // cannot subtract inet values of different sizes
                    } else {
                        -2 // result is out of range
                    };
                    let ok = cerr == want && e.level == ERROR;
                    core::mem::forget(e);
                    assert!(ok, "error arm parity");
                }
            }
        };
    }

    #[kani::proof]
    #[kani::unwind(9)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(alloc::fmt::format, stubs::stub_format)]
    fn eq_inetmi_v4() {
        let a = any_inet_fam(PGSQL_AF_INET);
        let b = any_inet_fam(PGSQL_AF_INET);
        net_mi_body!(a, b);
    }

    #[kani::proof]
    #[kani::unwind(18)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(alloc::fmt::format, stubs::stub_format)]
    fn eq_inetmi_v6() {
        let a = any_inet_fam(PGSQL_AF_INET6);
        let b = any_inet_fam(PGSQL_AF_INET6);
        net_mi_body!(a, b);
    }

    #[kani::proof]
    #[kani::unwind(18)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(alloc::fmt::format, stubs::stub_format)]
    fn eq_inetmi_xf() {
        let a = any_inet();
        let b = any_inet();
        kani::assume(a.family != b.family);
        net_mi_body!(a, b);
    }

    /// Arm witnesses for inetmi: Ok, out-of-range (v6-only: a 16-byte
    /// difference can exceed i64), and family-mismatch.
    #[kani::proof]
    #[kani::unwind(18)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(alloc::fmt::format, stubs::stub_format)]
    fn cover_inetmi_arms() {
        let a = any_inet();
        let b = any_inet();
        let r = adt_network::inetmi(a.iref(), b.iref());
        kani::cover!(r.is_ok(), "inetmi Ok arm reachable");
        match &r {
            Ok(_) => {}
            Err(e) => {
                kani::cover!(
                    e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE,
                    "family-mismatch arm reachable"
                );
                kani::cover!(
                    e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                    "out-of-range arm reachable"
                );
            }
        }
        core::mem::forget(r);
    }

    // ---------- inet_to_cidr / set_masklen (oids 1715 / 605 / 635) ----------

    /// inet_to_cidr: fenced domain makes both sides' invalid-bit-length arms
    /// unreachable; value path asserted over the full fenced domain.
    #[kani::proof]
    #[kani::unwind(18)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(alloc::fmt::format, stubs::stub_format)]
    fn eq_inet_to_cidr() {
        let a = any_inet();
        let ca = cin(&a);
        let mut cd = czero();
        let cerr = unsafe { pg_inet_to_cidr(&ca, &mut cd) };
        assert!(cerr != -3, "C elog arm must be unreachable under the invariant");
        assert!(cerr == 0);
        match adt_network::inet_to_cidr(a.iref()) {
            Ok(r) => {
                assert!(r.family == cd.family, "family");
                assert!(r.bits == cd.bits, "bits");
                assert!(r.ipaddr == cd.addr, "addr");
            }
            Err(e) => {
                core::mem::forget(e);
                assert!(false, "Rust errored on an invariant-clean inet");
            }
        }
    }

    /// Total set_masklen theorems: fenced inet × FULL-i32 bits, both arms.
    macro_rules! net_setmask {
        ($($h:ident: $core:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(18)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(alloc::fmt::format, stubs::stub_format)]
            fn $h() {
                let a = any_inet();
                let bits: i32 = kani::any();
                let ca = cin(&a);
                let mut cd = czero();
                let cerr = unsafe { $pg(&ca, bits, &mut cd) };
                match adt_network::$core(a.iref(), bits) {
                    Ok(r) => {
                        assert!(cerr == 0, "C errored where Rust succeeded");
                        assert!(r.family == cd.family, "family");
                        assert!(r.bits == cd.bits, "bits");
                        assert!(r.ipaddr == cd.addr, "addr");
                    }
                    Err(e) => {
                        let ok = cerr == -1
                            && e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE
                            && e.level == ERROR;
                        core::mem::forget(e);
                        assert!(ok, "invalid-mask-length arm parity");
                    }
                }
            }
        )*};
    }

    net_setmask! {
        eq_inet_set_masklen: inet_set_masklen / pg_inet_set_masklen;
        eq_cidr_set_masklen: cidr_set_masklen / pg_cidr_set_masklen;
    }

    /// Arm witnesses for the set_masklen theorems (incl. the bits == -1
    /// maxbits alias inside the Ok arm).
    #[kani::proof]
    #[kani::unwind(18)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(alloc::fmt::format, stubs::stub_format)]
    fn cover_set_masklen_arms() {
        let a = any_inet();
        let bits: i32 = kani::any();
        let r = adt_network::inet_set_masklen(a.iref(), bits);
        kani::cover!(r.is_ok(), "valid-mask arm reachable");
        kani::cover!(bits == -1 && r.is_ok(), "bits == -1 maxbits alias reachable");
        kani::cover!(r.is_err(), "invalid-mask arm reachable");
        core::mem::forget(r);
    }

    // ---------- hashinet / hashinetextended (oids 422 / 779) ----------

    macro_rules! net_hash_split {
        ($($v4:ident / $v6:ident: $rust:ident, $chash:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(9)]
            fn $v4() {
                let a = any_inet_fam(PGSQL_AF_INET);
                net_hash_body!(a, $rust, $chash);
            }
            #[kani::proof]
            #[kani::unwind(24)]
            fn $v6() {
                let a = any_inet_fam(PGSQL_AF_INET6);
                net_hash_body!(a, $rust, $chash);
            }
        )*};
    }

    macro_rules! net_hash_body {
        ($a:ident, $rust:ident, $chash:expr) => {
            let ca = cin(&$a);
            let mut view = [0u8; 18];
            let n = unsafe { pg_hashinet_view(&ca, view.as_mut_ptr()) } as usize;
            let seed: u64 = kani::any();
            let _ = seed; // v4/v6 macro shares the body; seed used below only
            #[allow(clippy::redundant_closure_call)]
            {
                let (r, c) = ($rust(&$a, seed), ($chash)(&view[..n], seed));
                assert!(r == c, "hash over the C-assembled byte view");
            }
        };
    }

    fn rust_hashinet(a: &InetValue, _seed: u64) -> u64 {
        adt_network::hashinet_bytes(a.iref()) as u64
    }
    fn rust_hashinetextended(a: &InetValue, seed: u64) -> u64 {
        adt_network::hashinet_bytes_extended(a.iref(), seed)
    }

    net_hash_split! {
        eq_hashinet_v4 / eq_hashinet_v6:
            rust_hashinet, |b: &[u8], _s: u64| hashfn::hash_bytes(b) as u64;
        eq_hashinetextended_v4 / eq_hashinetextended_v6:
            rust_hashinetextended, |b: &[u8], s: u64| hashfn::hash_bytes_extended(b, s);
    }

    // ---------- negative controls: rig must be able to fail ----------

    /// Deliberate sign flip: Rust inetpl(+addend) vs C inetmi_int8(-addend).
    /// MUST fail (counterexample at any nonzero addend with both arms Ok).
    /// DEFAULT solver.
    #[kani::proof]
    #[kani::unwind(9)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn control_inetpl_vs_c_inetmi_int8() {
        let a = any_inet_fam(PGSQL_AF_INET);
        let addend: i64 = kani::any();
        let ca = cin(&a);
        let mut cd = czero();
        let cerr = unsafe { pg_inetmi_int8(&ca, addend, &mut cd) };
        let r = adt_network::internal_inetpl(a.iref(), addend);
        match r {
            Ok(v) => {
                if cerr == 0 {
                    assert!(v.ipaddr == cd.addr, "sign-flip control");
                }
            }
            Err(e) => core::mem::forget(e),
        }
    }

    /// Deliberate view-length skew: hash the C view one byte SHORT. MUST
    /// fail. DEFAULT solver.
    #[kani::proof]
    #[kani::unwind(9)]
    fn control_hashinet_view_skew() {
        let a = any_inet_fam(PGSQL_AF_INET);
        let ca = cin(&a);
        let mut view = [0u8; 18];
        let n = unsafe { pg_hashinet_view(&ca, view.as_mut_ptr()) } as usize;
        let r = adt_network::hashinet_bytes(a.iref());
        let c = hashfn::hash_bytes(&view[..n - 1]);
        assert!(r == c, "length-skew control");
    }
}

// ================= net-ntop spot extension (WAVE 11) =================
//
// DERIVED-LENGTH text-output rows: inet_out 911 / cidr_out 1427 (via the
// common network_out), network_host 699, network_show 730, inet_abbrev 598,
// cidr_abbrev 599. Per the result-image wall law (proofs/TRIAGE.md: output
// length is data-dependent, so symbolic-input image claims are CNF
// width-bound walls), these get CONCRETE SPOT theorems only — each spot
// pins the input, so both sides' formatting loops run at concrete trip
// counts and the full output image (length + bytes) is compared exactly.
// C side: csrc/net_ntop.c (REL_18_STABLE src/port/inet_net_ntop.c +
// inet_cidr_ntop.c + network.c output wrappers; see its header for shims —
// notably the fixed-format sprintf model).
//
// Spot grid covers, per formatting engine: v4 all-octets + /mask suppress/
// append arms; v6 ::-compression (leading, middle, trailing-zero /0),
// v4-embedded arms (::ffff:a.b.c.d incl. the cidr drop-last-octet quirk),
// cidr partial-octet masking, and the bits==0 special forms. The symbolic
// remainder of these rows is recorded wall(CNF width-bound / derived-length
// copy) in the ledger — do NOT ladder against it (text-slice precedent).
//
// Negative control (one for this C section): control_ntop_host_vs_show —
// MUST fail (host suppresses the /mask network_show appends). DEFAULT
// solver.
#[cfg(kani)]
mod ntop_spot_proofs {
    use adt_network::{InetValue, INET_OUT_BUFLEN, PGSQL_AF_INET, PGSQL_AF_INET6};
    use types_error::PgResult;

    use std::os::raw::c_int;

    /// C-side inet value model (csrc/net_ntop.c pgc_inet).
    #[repr(C)]
    struct CInet {
        family: u8,
        bits: u8,
        addr: [u8; 16],
    }

    extern "C" {
        // all return the text length written to out (NUL-terminated), or
        // -1 for the could-not-format ereport arm (verdict only).
        fn pg_network_out(src: *const CInet, is_cidr: c_int, out: *mut u8) -> c_int;
        fn pg_network_host(ip: *const CInet, out: *mut u8) -> c_int;
        fn pg_network_show(ip: *const CInet, out: *mut u8) -> c_int;
        fn pg_inet_abbrev(ip: *const CInet, out: *mut u8) -> c_int;
        fn pg_cidr_abbrev(ip: *const CInet, out: *mut u8) -> c_int;
    }

    fn v4(a: u8, b: u8, c: u8, d: u8, bits: u8) -> InetValue {
        let mut ipaddr = [0u8; 16];
        ipaddr[..4].copy_from_slice(&[a, b, c, d]);
        InetValue {
            family: PGSQL_AF_INET,
            bits,
            ipaddr,
        }
    }

    fn v6(ipaddr: [u8; 16], bits: u8) -> InetValue {
        InetValue {
            family: PGSQL_AF_INET6,
            bits,
            ipaddr,
        }
    }

    /// Unwrap a shipped *_into result without dragging Box<PgError> Debug +
    /// drop glue into symex (forget + static panic, TRIAGE law).
    fn ok_len(r: PgResult<usize>) -> usize {
        match r {
            Ok(n) => n,
            Err(e) => {
                core::mem::forget(e);
                panic!("shipped formatter errored on a well-formed spot value");
            }
        }
    }

    /// Compare the full C text image against the Rust buffer image.
    fn assert_image_eq(clen: c_int, cbuf: &[u8], rlen: usize, rbuf: &[u8]) {
        assert!(clen >= 0, "C could-not-format arm fired on a spot value");
        assert!(clen as usize == rlen, "text length");
        let mut i = 0;
        while i < rlen {
            assert!(cbuf[i] == rbuf[i], "text byte");
            i += 1;
        }
    }

    macro_rules! ntop_spot {
        ($($h:ident: $val:expr => $kind:ident $(($cidr:literal))?;)*) => {$(
            #[kani::proof]
            #[kani::unwind(56)]
            fn $h() {
                let val: InetValue = $val;
                let ca = CInet { family: val.family, bits: val.bits, addr: val.ipaddr };
                let mut cbuf = [0u8; 64];
                let mut rbuf = [0u8; INET_OUT_BUFLEN];
                ntop_spot_call!(val, ca, cbuf, rbuf, $kind $(($cidr))?);
            }
        )*};
    }

    macro_rules! ntop_spot_call {
        ($val:ident, $ca:ident, $cbuf:ident, $rbuf:ident, out($cidr:literal)) => {
            let clen = unsafe { pg_network_out(&$ca, $cidr as c_int, $cbuf.as_mut_ptr()) };
            let rlen = ok_len(adt_network::network_out_into($val.iref(), $cidr, &mut $rbuf));
            assert_image_eq(clen, &$cbuf, rlen, &$rbuf);
        };
        ($val:ident, $ca:ident, $cbuf:ident, $rbuf:ident, host) => {
            let clen = unsafe { pg_network_host(&$ca, $cbuf.as_mut_ptr()) };
            let rlen = ok_len(adt_network::network_host_into($val.iref(), &mut $rbuf));
            assert_image_eq(clen, &$cbuf, rlen, &$rbuf);
        };
        ($val:ident, $ca:ident, $cbuf:ident, $rbuf:ident, show) => {
            let clen = unsafe { pg_network_show(&$ca, $cbuf.as_mut_ptr()) };
            let rlen = ok_len(adt_network::network_show_into($val.iref(), &mut $rbuf));
            assert_image_eq(clen, &$cbuf, rlen, &$rbuf);
        };
        ($val:ident, $ca:ident, $cbuf:ident, $rbuf:ident, abbrev) => {
            let clen = unsafe { pg_inet_abbrev(&$ca, $cbuf.as_mut_ptr()) };
            let rlen = ok_len(adt_network::inet_abbrev_into($val.iref(), &mut $rbuf));
            assert_image_eq(clen, &$cbuf, rlen, &$rbuf);
        };
        ($val:ident, $ca:ident, $cbuf:ident, $rbuf:ident, cabbrev) => {
            let clen = unsafe { pg_cidr_abbrev(&$ca, $cbuf.as_mut_ptr()) };
            let rlen = ok_len(adt_network::cidr_abbrev_into($val.iref(), &mut $rbuf));
            assert_image_eq(clen, &$cbuf, rlen, &$rbuf);
        };
    }

    const V6_2001_DB8_1: [u8; 16] = [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    const V6_2001_DB8: [u8; 16] = [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    const V6_2001_DB8_5: [u8; 16] = [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5];
    const V6_2001_1: [u8; 16] = [0x20, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    const V6_MAPPED_192_0_2_128: [u8; 16] =
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 0, 2, 128];
    const V6_MAPPED_192_0_2_0: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 0, 2, 0];
    const V6_ZERO: [u8; 16] = [0; 16];

    ntop_spot! {
        // network_out (inet_out 911 / cidr_out 1427): mask suppress/append
        spot_inet_out_v4_full:      v4(192, 0, 2, 1, 32)   => out(false); // "192.0.2.1"
        spot_inet_out_v4_mask:      v4(192, 0, 2, 0, 24)   => out(false); // "192.0.2.0/24"
        spot_cidr_out_v4_mask:      v4(10, 0, 0, 0, 8)     => out(true);  // "10.0.0.0/8" (ntop prints /8)
        spot_cidr_out_v4_full:      v4(10, 1, 2, 3, 32)    => out(true);  // "10.1.2.3/32" (appended)
        spot_inet_out_v6_compress:  v6(V6_2001_DB8_1, 128) => out(false); // "2001:db8::1"
        spot_cidr_out_v6_mask:      v6(V6_2001_DB8, 64)    => out(true);  // "2001:db8::/64"
        spot_inet_out_v6_v4mapped:  v6(V6_MAPPED_192_0_2_128, 128) => out(false); // "::ffff:192.0.2.128"
        // network_host 699 / network_show 730: maxbits render, / handling
        spot_network_host_v4:       v4(192, 0, 2, 5, 24)   => host; // "192.0.2.5"
        spot_network_show_v4:       v4(192, 0, 2, 5, 24)   => show; // "192.0.2.5/24"
        spot_network_host_v6:       v6(V6_2001_DB8_5, 64)  => host; // "2001:db8::5"
        spot_network_show_v6:       v6(V6_2001_DB8_5, 64)  => show; // "2001:db8::5/64"
        // inet_abbrev 598 (net_ntop with value bits)
        spot_inet_abbrev_v4:        v4(10, 1, 0, 0, 16)    => abbrev; // "10.1.0.0/16" (v4 keeps all octets)
        spot_inet_abbrev_v6:        v6(V6_2001_1, 60)      => abbrev; // "2001::1/60"
        // cidr_abbrev 599 (cidr_ntop: truncating, partial octets, /0 forms)
        spot_cidr_abbrev_v4_partial: v4(10, 128, 0, 0, 9)  => cabbrev; // "10.128/9"
        spot_cidr_abbrev_v4_zero:   v4(0, 0, 0, 0, 0)      => cabbrev; // "0/0"
        spot_cidr_abbrev_v6_zero:   v6(V6_ZERO, 0)         => cabbrev; // "::/0"
        spot_cidr_abbrev_v6_words:  v6(V6_2001_DB8, 32)    => cabbrev; // "2001:db8/32"
        spot_cidr_abbrev_v6_v4mapped: v6(V6_MAPPED_192_0_2_0, 120) => cabbrev; // "::ffff:192.0.2/120" (drop-last-octet quirk)
    }

    /// Deliberate mismatch: Rust network_host (suppresses /mask) vs C
    /// network_show (appends it) on a /24 spot value. MUST fail. DEFAULT
    /// solver.
    #[kani::proof]
    #[kani::unwind(56)]
    fn control_ntop_host_vs_show() {
        let val = v4(192, 0, 2, 5, 24);
        let ca = CInet {
            family: val.family,
            bits: val.bits,
            addr: val.ipaddr,
        };
        let mut cbuf = [0u8; 64];
        let mut rbuf = [0u8; INET_OUT_BUFLEN];
        let clen = unsafe { pg_network_show(&ca, cbuf.as_mut_ptr()) };
        let rlen = ok_len(adt_network::network_host_into(val.iref(), &mut rbuf));
        assert_image_eq(clen, &cbuf, rlen, &rbuf);
    }
}
