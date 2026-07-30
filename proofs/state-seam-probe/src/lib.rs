//! Kani C≡Rust equivalence — STATE-SEAM feasibility probe.
//!
//! Two families of state-entangled logic, proved with the tz-seam pattern
//! generalized: every state read feeding the logic is shimmed OUT of both
//! sides to one shared, universally-quantified symbolic value, the pure
//! downstream logic is proved over all such values, and a skew control
//! (different values on the two sides) must FAIL, witnessing the seam model
//! is load-bearing.
//!
//! FAMILY 1 — sequence nextval arithmetic (sequence.c nextval_internal):
//!   Rust side: the SHIPPED `sequence::nextval_advance` — the pure core
//!   factored out of nextval_internal (behavior-identical shipped edit, see
//!   crates/backend/commands/sequence/src/lib.rs). C side: the verbatim
//!   arithmetic section of nextval_internal (c/pg_seq_nextval.c).
//!   SEAM MODEL: the sequence tuple (last_value, log_cnt, is_called), the
//!   pg_sequence catalog form (increment, max, min, cache, cycle), and the
//!   page-LSN-vs-redo-pointer test are each ONE symbolic value fed
//!   identically to both sides. Fences = the catalog invariants the state
//!   machinery guarantees (documented per-harness). Everything downstream —
//!   the pre-log/WAL-force decision, the fetch loop, overflow-avoiding bound
//!   checks, cycle wrap, at-bound error verdicts (max vs min), and all five
//!   outputs (result, cached last, WAL image value, new log_cnt, logit) — is
//!   inside the theorem.
//!
//! FAMILY 2 — ACL permission-bit logic (acl.c):
//!   aclmask: the catalog role-membership read has_privs_of_role(roleid, X)
//!   is the seam. Within one aclmask call its first argument is fixed, so
//!   the reachable seam surface is a boolean function of the queried role.
//!   SEAM MODEL: a first-match oracle table whose keys are EXACTLY the roles
//!   aclmask can query (ownerId + every grantee in the bounded acl) and
//!   whose answers + default are fully symbolic — i.e. the proof quantifies
//!   over every boolean membership assignment on the reachable query set;
//!   only the seam internals (catalog walk, superuser check, caching) are
//!   outside the proof. Both sides read the same table (Rust via kani::stub
//!   of adt_acl::has_privs_of_role, C via extern globals);
//!   control_aclmask_oracle_skew arms them differently and must FAIL.
//!   aclmask_direct / aclcontains / aclitem_eq / hash_aclitem[_extended] /
//!   makeaclitem's bit assembly are pure — no seam residue at all.
//!
//! Negative controls (run with the DEFAULT solver; kissat never terminates
//! on failing harnesses): control_nextval_state_skew,
//! control_aclmask_oracle_skew, control_aclcontains_vs_eq.
//!
//! Run recipe (family root):
//!   green harnesses:  timeout 30 cargo kani -Z c-ffi -Z stubbing \
//!       --c-lib c/pg_acl.c --c-lib c/pg_seq_nextval.c --solver kissat \
//!       --harness proofs::<h> --exact
//!   nextval split pair additionally needs --no-assertion-reach-checks and a
//!   ~600s budget (each per-assert reach check is a separate non-incremental
//!   kissat call on the 33-deep unrolled formula; the theorem itself is one
//!   UNSAT query). Controls + cover harness: drop --solver kissat (default
//!   incremental CaDiCaL; NOTE it can wedge in a propositional-reduction
//!   pass on the GREEN nextval formula — that wedge is a known trap, not a
//!   verdict).

#[cfg(kani)]
mod proofs {
    use adt_acl::{AclItem, AclMaskHow};
    use datum::{Datum, NullableDatum};
    use types_core::Oid;
    use types_error::PgResult;
    use types_fmgr::LocalFcinfo;

    use std::os::raw::c_int;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::Relaxed};

    extern "C" {
        // family 1: sequence
        fn pg_nextval_advance(
            last_value: i64,
            log_cnt: i64,
            is_called: c_int,
            incby: i64,
            maxv: i64,
            minv: i64,
            cache: i64,
            cycle: c_int,
            lsn_le_redo: c_int,
            out_result: *mut i64,
            out_last: *mut i64,
            out_next: *mut i64,
            out_log: *mut i64,
            out_logit: *mut c_int,
        ) -> c_int;

        // family 2: acl
        fn pg_aclmask(
            aidat: *const AclItem,
            num: c_int,
            roleid: Oid,
            owner_id: Oid,
            mask: u64,
            how: c_int,
        ) -> u64;
        fn pg_aclmask_direct(
            aidat: *const AclItem,
            num: c_int,
            roleid: Oid,
            owner_id: Oid,
            mask: u64,
            how: c_int,
        ) -> u64;
        fn pg_aclitem_eq(a1: *const AclItem, a2: *const AclItem) -> c_int;
        fn pg_hash_aclitem(a: *const AclItem) -> u32;
        fn pg_hash_aclitem_extended(a: *const AclItem, seed: u64) -> u64;
        fn pg_aclcontains(aidat: *const AclItem, num: c_int, aip: *const AclItem) -> c_int;
        fn pg_makeaclitem_bits(
            grantee: Oid,
            grantor: Oid,
            priv_: u64,
            goption: c_int,
            result: *mut AclItem,
        ) -> c_int;

        // membership oracle: the shared state-seam model (see c/pg_acl.c)
        static mut pg_oracle_role: [Oid; 6];
        static mut pg_oracle_ans: [c_int; 6];
        static mut pg_oracle_default: c_int;
    }

    // =====================================================================
    // FAMILY 1 — sequence nextval arithmetic
    // =====================================================================

    /// Catalog invariants enforced by DefineSequence/AlterSequence
    /// (init_params) and read_seq_tuple — the contract the state seam
    /// guarantees for every reachable call:
    ///   increment != 0        (init_params: "INCREMENT must not be zero")
    ///   minv < maxv           (init_params: "MINVALUE must be less than
    ///                          MAXVALUE"; harness widens to minv <= maxv)
    ///   minv <= last <= maxv  (init_params clamps start/restart; nextval
    ///                          writes back only in-bound values)
    ///   log_cnt >= 0          (written as 0 or the loop's non-negative log)
    ///   cache >= 1            (init_params: "CACHE must be greater than
    ///                          zero"); capped at 2 for the unwind bound —
    ///                          the loop runs cache + SEQ_LOG_VALS(=32)
    ///                          iterations, so the cap bounds circuit depth,
    ///                          not value ranges (all i64 stay symbolic).
    fn nextval_fence(
        last_value: i64,
        log_cnt: i64,
        incby: i64,
        maxv: i64,
        minv: i64,
        cache: i64,
        cache_cap: i64,
    ) -> bool {
        incby != 0
            && minv <= maxv
            && last_value >= minv
            && last_value <= maxv
            && log_cnt >= 0
            && cache >= 1
            && cache <= cache_cap
    }

    /// Vacuity insurance for the nextval fences: both verdict arms,
    /// the cycle wrap, and the WAL-force path are reachable inside the fenced
    /// domain. DEFAULT solver (covers are SAT calls).
    #[kani::proof]
    #[kani::unwind(36)]
    fn cover_nextval_regimes() {
        let last_value: i64 = kani::any();
        let log_cnt: i64 = kani::any();
        let is_called: bool = kani::any();
        let incby: i64 = kani::any();
        let maxv: i64 = kani::any();
        let minv: i64 = kani::any();
        let cache: i64 = kani::any();
        let cycle: bool = kani::any();
        let lsn_le_redo: bool = kani::any();
        kani::assume(nextval_fence(last_value, log_cnt, incby, maxv, minv, cache, 2));

        let r = sequence::nextval_advance(
            last_value,
            log_cnt,
            is_called,
            incby,
            maxv,
            minv,
            cache,
            cycle,
            move || lsn_le_redo,
        );
        match &r {
            Ok(a) => {
                kani::cover!(true, "ok arm reachable");
                kani::cover!(!a.logit, "no-WAL steady state reachable");
                kani::cover!(a.logit, "WAL-forced path reachable");
                kani::cover!(cycle && a.result == minv && incby > 0, "cycle wrap reachable");
            }
            Err(sequence::NextvalBound::Max) => {
                kani::cover!(true, "max-bound error reachable");
            }
            Err(sequence::NextvalBound::Min) => {
                kani::cover!(true, "min-bound error reachable");
            }
        }
    }

    /// nextval: full parity of all five value outputs AND the at-bound
    /// error/cycle verdict (max=1/min=2), over fully symbolic (current,
    /// increment, min, max, cycle) plus the (log_cnt, is_called, lsn<=redo)
    /// WAL-decision state, fenced to the catalog invariants in nextval_fence
    /// (cache capped at 2 = unwind-depth cap only). Case-split per increment
    /// sign (prove-target ladder step 4): the unsplit harness walls >500s;
    /// each half solves in ~460-490s (kissat + --no-assertion-reach-checks,
    /// measured under multi-agent load) — release-gate tier, not per-commit
    /// tier. The union trivially
    /// covers the fenced domain — the fence requires incby != 0, and
    /// i64 != 0 <=> (incby > 0 || incby < 0), so no separate union-coverage
    /// SAT call is needed (total split by construction).
    macro_rules! nextval_split {
        ($($h:ident: $sign:tt;)*) => {$(
            #[kani::proof]
            #[kani::unwind(35)]
            fn $h() {
                let last_value: i64 = kani::any();
                let log_cnt: i64 = kani::any();
                let is_called: bool = kani::any();
                let incby: i64 = kani::any();
                let maxv: i64 = kani::any();
                let minv: i64 = kani::any();
                let cache: i64 = kani::any();
                let cycle: bool = kani::any();
                let lsn_le_redo: bool = kani::any();
                kani::assume(nextval_fence(last_value, log_cnt, incby, maxv, minv, cache, 2));
                kani::assume(incby $sign 0);

                let r = sequence::nextval_advance(
                    last_value, log_cnt, is_called, incby, maxv, minv, cache, cycle,
                    move || lsn_le_redo,
                );

                let (mut c_result, mut c_last, mut c_next, mut c_log) = (0i64, 0i64, 0i64, 0i64);
                let mut c_logit: c_int = 0;
                let c = unsafe {
                    pg_nextval_advance(
                        last_value, log_cnt, is_called as c_int, incby, maxv, minv, cache,
                        cycle as c_int, lsn_le_redo as c_int,
                        &mut c_result, &mut c_last, &mut c_next, &mut c_log, &mut c_logit,
                    )
                };

                let same = match r {
                    Ok(a) => {
                        c == 0
                            && a.result == c_result
                            && a.last == c_last
                            && a.next == c_next
                            && a.log == c_log
                            && a.logit == (c_logit != 0)
                    }
                    Err(sequence::NextvalBound::Max) => c == 1,
                    Err(sequence::NextvalBound::Min) => c == 2,
                };
                assert!(same);
            }
        )*};
    }

    nextval_split! {
        eq_nextval_advance_asc: >;
        eq_nextval_advance_desc: <;
    }

    /// Seam-skew control: the C side reads a DIFFERENT current value than the
    /// Rust side (last_value+1) — must FAIL, witnessing the symbolic state
    /// value is load-bearing on both sides. DEFAULT solver.
    #[kani::proof]
    #[kani::unwind(36)]
    fn control_nextval_state_skew() {
        let last_value: i64 = kani::any();
        let log_cnt: i64 = kani::any();
        let is_called: bool = kani::any();
        let incby: i64 = kani::any();
        let maxv: i64 = kani::any();
        let minv: i64 = kani::any();
        let cache: i64 = kani::any();
        let cycle: bool = kani::any();
        let lsn_le_redo: bool = kani::any();
        kani::assume(nextval_fence(last_value, log_cnt, incby, maxv, minv, cache, 1));
        // keep the skewed C-side state inside the same fence
        kani::assume(last_value < maxv);

        let r = sequence::nextval_advance(
            last_value,
            log_cnt,
            is_called,
            incby,
            maxv,
            minv,
            cache,
            cycle,
            move || lsn_le_redo,
        );

        let (mut c_result, mut c_last, mut c_next, mut c_log) = (0i64, 0i64, 0i64, 0i64);
        let mut c_logit: c_int = 0;
        let c = unsafe {
            pg_nextval_advance(
                last_value + 1, // SKEW
                log_cnt,
                is_called as c_int,
                incby,
                maxv,
                minv,
                cache,
                cycle as c_int,
                lsn_le_redo as c_int,
                &mut c_result,
                &mut c_last,
                &mut c_next,
                &mut c_log,
                &mut c_logit,
            )
        };

        match r {
            Ok(a) => {
                assert!(c == 0);
                assert!(a.result == c_result);
            }
            Err(b) => {
                let want = match b {
                    sequence::NextvalBound::Max => 1,
                    sequence::NextvalBound::Min => 2,
                };
                assert!(c == want);
            }
        }
    }

    // =====================================================================
    // FAMILY 2 — ACL bit logic
    // =====================================================================

    const ORACLE_N: usize = 6;

    static ORACLE_ROLE: [AtomicU32; ORACLE_N] = [const { AtomicU32::new(0) }; ORACLE_N];
    static ORACLE_ANS: [AtomicBool; ORACLE_N] = [const { AtomicBool::new(false) }; ORACLE_N];
    static ORACLE_DEFAULT: AtomicBool = AtomicBool::new(false);

    /// Stub for adt_acl's has_privs_of_role: the shared membership-oracle
    /// seam model. `member` is deliberately ignored — within one aclmask call
    /// it is always the fixed roleid, and the C model ignores it identically.
    fn model_has_privs(_member: Oid, role: Oid) -> PgResult<bool> {
        for i in 0..ORACLE_N {
            if ORACLE_ROLE[i].load(Relaxed) == role {
                return Ok(ORACLE_ANS[i].load(Relaxed));
            }
        }
        Ok(ORACLE_DEFAULT.load(Relaxed))
    }

    /// Arm BOTH sides of the membership seam with the same table.
    fn set_oracle(roles: [Oid; ORACLE_N], ans: [bool; ORACLE_N], def: bool) {
        for i in 0..ORACLE_N {
            ORACLE_ROLE[i].store(roles[i], Relaxed);
            ORACLE_ANS[i].store(ans[i], Relaxed);
            unsafe {
                pg_oracle_role[i] = roles[i];
                pg_oracle_ans[i] = ans[i] as c_int;
            }
        }
        ORACLE_DEFAULT.store(def, Relaxed);
        unsafe { pg_oracle_default = def as c_int };
    }

    const ZERO_ITEM: AclItem = AclItem { ai_grantee: 0, ai_grantor: 0, ai_privs: 0 };

    fn any_items<const N: usize>() -> [AclItem; N] {
        let mut items = [ZERO_ITEM; N];
        for it in items.iter_mut() {
            it.ai_grantee = kani::any();
            it.ai_grantor = kani::any();
            it.ai_privs = kani::any();
        }
        items
    }

    /// aclmask: symbolic acl (n<=4), roleid, ownerId, mask, how — modulo the
    /// membership-oracle seam model. The oracle keys are exactly the roles
    /// aclmask can query (ownerId + all four grantees), with fully symbolic
    /// answers and default, so parity holds for EVERY membership assignment
    /// on the reachable query set.
    #[kani::proof]
    #[kani::unwind(8)] // acl passes: n<=4 (+1); oracle table walk: 6 (+1)
    #[kani::stub(adt_acl::has_privs_of_role, model_has_privs)]
    fn eq_aclmask() {
        let n: usize = kani::any();
        kani::assume(n <= 4);
        let items = any_items::<4>();
        let roleid: Oid = kani::any();
        let owner: Oid = kani::any();
        let mask: u64 = kani::any();
        let how_any: bool = kani::any();
        let how = if how_any { AclMaskHow::AclmaskAny } else { AclMaskHow::AclmaskAll };

        set_oracle(
            [
                owner,
                items[0].ai_grantee,
                items[1].ai_grantee,
                items[2].ai_grantee,
                items[3].ai_grantee,
                kani::any(),
            ],
            kani::any(),
            kani::any(),
        );

        let r = run_aclmask(&items[..n], roleid, owner, mask, how);
        let c = unsafe {
            pg_aclmask(items.as_ptr(), n as c_int, roleid, owner, mask, how_any as c_int)
        };
        assert!(r == c);
    }

    /// Unwrap without dragging Box<PgError> drop glue / Debug format into the
    /// formula; with the seam stubbed the Err arm is unconstructible.
    fn run_aclmask(acl: &[AclItem], roleid: Oid, owner: Oid, mask: u64, how: AclMaskHow) -> u64 {
        match adt_acl::aclmask(acl, roleid, owner, mask, how) {
            Ok(v) => v,
            Err(e) => {
                core::mem::forget(e);
                panic!("aclmask errored (stubbed seam cannot)")
            }
        }
    }

    /// Seam-skew control: identical inputs, but the two sides' membership
    /// oracles answer differently — must FAIL, witnessing the oracle is
    /// load-bearing on both sides. DEFAULT solver.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(adt_acl::has_privs_of_role, model_has_privs)]
    fn control_aclmask_oracle_skew() {
        let n: usize = kani::any();
        kani::assume(n <= 4);
        let items = any_items::<4>();
        let roleid: Oid = kani::any();
        let owner: Oid = kani::any();
        let mask: u64 = kani::any();

        set_oracle(
            [
                owner,
                items[0].ai_grantee,
                items[1].ai_grantee,
                items[2].ai_grantee,
                items[3].ai_grantee,
                kani::any(),
            ],
            [false; 6],
            false,
        );
        // SKEW the C side: answers all true
        unsafe {
            for i in 0..ORACLE_N {
                pg_oracle_ans[i] = 1;
            }
            pg_oracle_default = 1;
        }

        let r = run_aclmask(&items[..n], roleid, owner, mask, AclMaskHow::AclmaskAll);
        let c = unsafe { pg_aclmask(items.as_ptr(), n as c_int, roleid, owner, mask, 0) };
        assert!(r == c);
    }

    /// aclmask_direct: pure (no membership seam), full symbolic domain, n<=4.
    #[kani::proof]
    #[kani::unwind(6)]
    fn eq_aclmask_direct() {
        let n: usize = kani::any();
        kani::assume(n <= 4);
        let items = any_items::<4>();
        let roleid: Oid = kani::any();
        let owner: Oid = kani::any();
        let mask: u64 = kani::any();
        let how_any: bool = kani::any();
        let how = if how_any { AclMaskHow::AclmaskAny } else { AclMaskHow::AclmaskAll };

        let r = adt_acl::aclmask_direct(&items[..n], roleid, owner, mask, how);
        let c = unsafe {
            pg_aclmask_direct(items.as_ptr(), n as c_int, roleid, owner, mask, how_any as c_int)
        };
        assert!(r == c);
    }

    // ---- wrapper-level harnesses: SHIPPED fc_* through a real fcinfo frame,
    // so the 16-byte aclitem image unpack is inside the theorem. Images use
    // the arg_aclitem layout (LE grantee/grantor/privs), identical to the C
    // AclItem struct on this (little-endian) target.

    fn aclitem_img(it: &AclItem) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0..4].copy_from_slice(&it.ai_grantee.to_le_bytes());
        b[4..8].copy_from_slice(&it.ai_grantor.to_le_bytes());
        b[8..16].copy_from_slice(&it.ai_privs.to_le_bytes());
        b
    }

    fn fci2(a: Datum, b: Datum) -> LocalFcinfo<2> {
        let mut f = LocalFcinfo::<2>::new(0);
        f.args[0] = NullableDatum::value(a);
        f.args[1] = NullableDatum::value(b);
        f
    }

    /// Run a shipped infallible fc_* wrapper on a 2-arg frame.
    fn call2(
        fc: fn(
            Option<&mut types_fmgr::FmgrInfo>,
            &mut types_fmgr::FunctionCallInfoBaseData,
        ) -> types_error::PgResult<Datum>,
        a: Datum,
        b: Datum,
    ) -> Datum {
        let mut f = fci2(a, b);
        match fc(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("wrapper errored")
            }
        }
    }

    /// aclitem_eq: SHIPPED fc_aclitem_eq (datum unpack + compare in-theorem)
    /// vs C aclitem_eq, full symbolic domain.
    #[kani::proof]
    fn eq_aclitem_eq() {
        let [a1, a2] = any_items::<2>();
        let (i1, i2) = (aclitem_img(&a1), aclitem_img(&a2));
        let r = call2(
            adt_acl::builtins::fc_aclitem_eq,
            Datum::from_usize(i1.as_ptr() as usize),
            Datum::from_usize(i2.as_ptr() as usize),
        );
        let c = unsafe { pg_aclitem_eq(&a1, &a2) };
        assert!(r.as_bool() as c_int == c);
    }

    /// hash_aclitem: SHIPPED fc_hash_aclitem vs C, full symbolic domain.
    /// Machine-checks the shipped truncate-then-add-u32 == C's u64-add-then-
    /// truncate claim.
    #[kani::proof]
    fn eq_hash_aclitem() {
        let [a] = any_items::<1>();
        let img = aclitem_img(&a);
        let r = call2(
            adt_acl::builtins::fc_hash_aclitem,
            Datum::from_usize(img.as_ptr() as usize),
            Datum::from_i32(0), // unused second arg slot
        );
        let c = unsafe { pg_hash_aclitem(&a) };
        assert!(r.as_i32() as u32 == c);
    }

    /// hash_aclitem_extended: SHIPPED fc_hash_aclitem_extended (incl. its
    /// local hash_uint32_extended reimplementation) vs C hash_aclitem_extended
    /// + verbatim hashfn.c hash_bytes_uint32_extended; full symbolic item and
    /// seed (both the seed==0 shortcut and the mix/final path in-theorem).
    #[kani::proof]
    fn eq_hash_aclitem_extended() {
        let [a] = any_items::<1>();
        let seed: i64 = kani::any();
        let img = aclitem_img(&a);
        let r = call2(
            adt_acl::builtins::fc_hash_aclitem_extended,
            Datum::from_usize(img.as_ptr() as usize),
            Datum::from_i64(seed),
        );
        let c = unsafe { pg_hash_aclitem_extended(&a, seed as u64) };
        assert!(r.as_i64() as u64 == c);
    }

    /// aclcontains core (ops::aclcontains): full symbolic domain, n<=4. The
    /// fmgr wrapper's varlena aclitem[] decode is out of proof.
    #[kani::proof]
    #[kani::unwind(6)]
    fn eq_aclcontains() {
        let n: usize = kani::any();
        kani::assume(n <= 4);
        let items = any_items::<4>();
        let [aip] = any_items::<1>();
        let r = adt_acl::aclcontains(&items[..n], &aip);
        let c = unsafe { pg_aclcontains(items.as_ptr(), n as c_int, &aip) };
        assert!(r as c_int == c);
    }

    /// makeaclitem bit assembly: shipped aclitem_set_privs_goptions +
    /// field stores vs C's verbatim ACLITEM_SET_PRIVS_GOPTIONS, full symbolic
    /// (grantee, grantor, priv, goption). Text privilege parsing
    /// (convert_any_priv_string) and palloc are out of proof.
    #[kani::proof]
    fn eq_makeaclitem_bits() {
        let grantee: Oid = kani::any();
        let grantor: Oid = kani::any();
        let priv_: u64 = kani::any();
        let goption: bool = kani::any();

        // shipped assembly, exactly as fc_makeaclitem performs it
        let mut item = AclItem { ai_grantee: grantee, ai_grantor: grantor, ai_privs: 0 };
        adt_acl::aclitem_set_privs_goptions(
            &mut item,
            priv_,
            if goption { priv_ } else { adt_acl::ACL_NO_RIGHTS },
        );

        let mut c_item = ZERO_ITEM;
        unsafe { pg_makeaclitem_bits(grantee, grantor, priv_, goption as c_int, &mut c_item) };
        assert!(item == c_item);
    }

    /// Plain-logic negative control: aclcontains (subset-of-rights) vs C
    /// aclitem_eq — must FAIL (e.g. matching grantee/grantor, rights strict
    /// subset). DEFAULT solver.
    #[kani::proof]
    fn control_aclcontains_vs_eq() {
        let [it, aip] = any_items::<2>();
        let items = [it];
        let r = adt_acl::aclcontains(&items[..], &aip);
        let c = unsafe { pg_aclitem_eq(&items[0], &aip) };
        assert!(r as c_int == c);
    }
}
