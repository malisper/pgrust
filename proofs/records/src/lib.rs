//! Kani C≡Rust equivalence: RECORDS tier of the typcache-instantiation
//! pattern — record_cmp / record_eq column loops proven per CONCRETE column
//! descriptor, with the typcache seam (per-column comparator resolution +
//! comparator semantics) modeled IDENTICALLY on both sides.
//!
//! THE CLAIM IS PER-DESCRIPTOR, NOT GENERIC: each theorem reads "given a
//! record descriptor whose columns are int4/date (typcache seam concrete:
//! resolve succeeds exactly for {int4, date}, comparator = verbatim
//! btint4cmp/int4eq/date_cmp/date_eq semantics), the record comparison
//! loops behave identically for ALL column values, null flags, dropped
//! flags, and per-column collations".  Nothing is claimed about other
//! column types or typcache lookup internals.
//!
//! Rust side (shipped code, path-dep — never copied):
//!   adt_rowtypes::{record_cmp_core, record_eq_core} — the complete
//!   column-scan slice of C record_cmp/record_eq over deformed columns
//!   (dropped-skip pairing, dissimilar-type check, collation select,
//!   resolve-before-null-check placement, NULLs-sort-last / short-circuit,
//!   trailing column-count check).  The ops traits are implemented here
//!   CONCRETELY with the C comparators' semantics; the error type E is a
//!   small Copy u8 (no Box<PgError> drop glue in the formula — the cores'
//!   generic E exists precisely for this).
//! C side: c/pg_records.c — verbatim REL_18_STABLE rowtypes.c column loops
//!   + btint4cmp/int4eq/date_cmp/date_eq bodies (provenance + all shims in
//!   its header).
//!
//! Claim boundaries (mirror into the ledger):
//!   - PER-DESCRIPTOR: (int4, X) 2-column descriptors with X per side drawn
//!     symbolically from {int4, date, <unsupported oid>} — so the
//!     dissimilar-column and no-support-function arms are IN-theorem — plus
//!     a 2-vs-3-column all-int4 pair.  Column values fully symbolic i32,
//!     null flags / dropped flags / collations (per column, from {0, 100,
//!     200}) fully symbolic and independent per side.
//!   - DEFORM TIER TESTED: detoast + tupdesc lookup + heap_deform_tuple
//!     (C) vs deform_record (Rust) are shimmed to the same deformed-columns
//!     interface on both sides; the loop semantics are the theorem.
//!   - fn_extra memo tier tested: both sides model the first-call-of-a-
//!     series state (C: fresh zeroed RecordCompareData; Rust: pure resolve).
//!   - TYPCACHE SEAM CONCRETE: resolution mapping + comparator bodies are
//!     harness-provided on the Rust side and static entries on the C side,
//!     identical by construction; skew controls prove both halves of the
//!     seam (comparator semantics, resolve set) are load-bearing.
//!   - Error paths: verdict + error-class parity (distinct code per C
//!     ereport site: 1 dissimilar / 2 column-count / 3 no-support-fn);
//!     message text/sqlstate out of proof (never constructed — the shipped
//!     PgError mapping in map_core_err is the wrappers' tested tier).
//!   - Call-trace parity: the collation-select line is observable — both
//!     sides accumulate (collation + 1) per comparator invocation and the
//!     harness asserts the accumulators equal (also pins compare-call
//!     count, hence the resolve/null-check short-circuit structure).
//!   - The fc_record_{eq,ne,lt,gt,le,ge}/fc_btrecordcmp wrappers' one-line
//!     result mappings over these cores are code-identical to C's
//!     (record_cmp(fcinfo) < 0 etc.) and stay in the tested tier.
//!
//! Cover witnesses (vacuity insurance, each in-theorem):
//!   null-ordering arm, dropped-skip arm, short-circuit-before-
//!   count-mismatch arm (result decided while effective column counts
//!   differ — no error, C parity), column-count error, dissimilar-types
//!   error, and resolve-precedes-null-check (no-support error fires with
//!   BOTH columns null — the C quirk the refactor deliberately preserved).
//!
//! Controls (DEFAULT solver, must FAIL):
//!   - control_cmp_skewed_comparator — Rust comparator sign-flipped: rig
//!     non-vacuity.
//!   - control_eq_seam_skew_resolve — Rust resolve accepts the unsupported
//!     oid while C rejects it: the resolve half of the seam is load-bearing.
//!
//! Run recipe (measured 2026-07-28):
//!   cd proofs/records
//!   # parity theorems (eq_*): 11.6-21.3s
//!   timeout 30 cargo kani -Z c-ffi --c-lib c/pg_records.c \
//!       --solver kissat --no-assertion-reach-checks \
//!       --harness proofs::<eq_*> --exact
//!   # cover harnesses (cover_*): default solver, 2.0-21.1s, all covers
//!   # must satisfy
//!   timeout 30 cargo kani -Z c-ffi --c-lib c/pg_records.c \
//!       --no-assertion-reach-checks --harness proofs::<cover_*> --exact
//!   # controls (control_*): default solver, expect VERIFICATION FAILED
//!   # (21.2-30.3s; kissat is a fake wall on failing harnesses)
//!   timeout 60 cargo kani -Z c-ffi --c-lib c/pg_records.c \
//!       --harness proofs::<control_*> --exact
//! Ladder note: covers inside the parity theorems walled them at 30s
//! (kissat re-solves per property batch) — hence the dedicated cover
//! harnesses + --no-assertion-reach-checks; the residual 11-21s is
//! symex-depth-bound (14s symex), so case-splits would not help.

#[cfg(kani)]
mod proofs {
    use adt_rowtypes::{
        record_cmp_core, record_eq_core, RecordColumnCmp, RecordColumnEq, RecordColumnMeta,
        RecordCoreError,
    };
    use datum::Datum;
    use types_core::Oid;

    extern "C" {
        fn pg_c_get_err() -> i32;
        fn pg_c_get_coll_acc() -> u64;
        #[allow(clippy::too_many_arguments)]
        fn pg_c_record_cmp(
            natts1: i32,
            dropped1: *const u8,
            types1: *const u32,
            colls1: *const u32,
            vals1: *const i32,
            nulls1: *const u8,
            natts2: i32,
            dropped2: *const u8,
            types2: *const u32,
            colls2: *const u32,
            vals2: *const i32,
            nulls2: *const u8,
        ) -> i32;
        #[allow(clippy::too_many_arguments)]
        fn pg_c_record_eq(
            natts1: i32,
            dropped1: *const u8,
            types1: *const u32,
            colls1: *const u32,
            vals1: *const i32,
            nulls1: *const u8,
            natts2: i32,
            dropped2: *const u8,
            types2: *const u32,
            colls2: *const u32,
            vals2: *const i32,
            nulls2: *const u8,
        ) -> i32;
    }

    const INT4OID: Oid = 23;
    const DATEOID: Oid = 1082;
    /// An oid the seam resolves to "no comparison/equality support".
    const NOCMPOID: Oid = 600; // point: genuinely has no btree cmp/eq in PG

    const ERR_DISSIMILAR: u8 = 1;
    const ERR_COLCOUNT: u8 = 2;
    const ERR_NOSUPPORT: u8 = 3;

    /// The concrete typcache seam: resolve succeeds exactly for {int4,
    /// date}; compare/equal are the verbatim btint4cmp / date_cmp /
    /// int4eq / date_eq value semantics (identical for int4 and date at
    /// the i32 level, as in C).  `coll_acc` mirrors the C shim's
    /// FunctionCallInvoke accumulator: (collation + 1) per invocation.
    struct SeamOps {
        coll_acc: u64,
    }

    fn seam_resolve(typid: Oid) -> Result<(), u8> {
        if typid == INT4OID || typid == DATEOID {
            Ok(())
        } else {
            Err(ERR_NOSUPPORT)
        }
    }

    impl RecordColumnCmp for SeamOps {
        type Err = u8;
        fn resolve(&mut self, _j: usize, typid: Oid) -> Result<(), u8> {
            seam_resolve(typid)
        }
        fn compare(&mut self, _j: usize, coll: Oid, d1: Datum, d2: Datum) -> Result<i32, u8> {
            self.coll_acc += coll as u64 + 1;
            let (a, b) = (d1.as_i32(), d2.as_i32());
            Ok(if a > b {
                1
            } else if a == b {
                0
            } else {
                -1
            })
        }
    }

    impl RecordColumnEq for SeamOps {
        type Err = u8;
        fn resolve(&mut self, _j: usize, typid: Oid) -> Result<(), u8> {
            seam_resolve(typid)
        }
        fn equal(&mut self, _j: usize, coll: Oid, d1: Datum, d2: Datum) -> Result<bool, u8> {
            self.coll_acc += coll as u64 + 1;
            Ok(d1.as_i32() == d2.as_i32())
        }
    }

    fn err_code<E: Copy + Into<u8>>(e: &RecordCoreError<E>) -> u8 {
        match e {
            RecordCoreError::DissimilarColumns { .. } => ERR_DISSIMILAR,
            RecordCoreError::ColumnCountMismatch => ERR_COLCOUNT,
            RecordCoreError::Column(c) => (*c).into(),
        }
    }

    /// One fully symbolic side of N physical columns: (dropped, typid,
    /// collation, value, isnull) per column.  Column 0 is pinned int4;
    /// later columns draw their type from {int4, date, NOCMP}.
    struct Side<const N: usize> {
        dropped: [bool; N],
        types: [u32; N],
        colls: [u32; N],
        vals: [i32; N],
        nulls: [bool; N],
    }

    fn any_side<const N: usize>() -> Side<N> {
        let dropped: [bool; N] = kani::any();
        let vals: [i32; N] = kani::any();
        let nulls: [bool; N] = kani::any();
        let mut types = [INT4OID; N];
        let mut colls = [0u32; N];
        for i in 0..N {
            let c: u32 = kani::any();
            kani::assume(c == 0 || c == 100 || c == 200);
            colls[i] = c;
            if i > 0 {
                let t: u32 = kani::any();
                kani::assume(t == INT4OID || t == DATEOID || t == NOCMPOID);
                types[i] = t;
            }
        }
        Side { dropped, types, colls, vals, nulls }
    }

    fn meta<const N: usize>(s: &Side<N>) -> [RecordColumnMeta; N] {
        let mut m = [RecordColumnMeta { attisdropped: false, atttypid: 0, attcollation: 0 }; N];
        for i in 0..N {
            m[i] = RecordColumnMeta {
                attisdropped: s.dropped[i],
                atttypid: s.types[i],
                attcollation: s.colls[i],
            };
        }
        m
    }

    fn datums<const N: usize>(s: &Side<N>) -> [Datum; N] {
        let mut d = [Datum::null(); N];
        for i in 0..N {
            d[i] = Datum::from_i32(s.vals[i]);
        }
        d
    }

    fn u8s<const N: usize>(b: &[bool; N]) -> [u8; N] {
        let mut o = [0u8; N];
        for i in 0..N {
            o[i] = b[i] as u8;
        }
        o
    }

    /// (err_code, result) from the shipped cmp core with the concrete seam.
    fn rust_cmp<const N1: usize, const N2: usize>(
        s1: &Side<N1>,
        s2: &Side<N2>,
    ) -> (u8, i32, u64) {
        let mut ops = SeamOps { coll_acc: 0 };
        let r = record_cmp_core(
            &meta(s1),
            &datums(s1),
            &s1.nulls,
            &meta(s2),
            &datums(s2),
            &s2.nulls,
            &mut ops,
        );
        match r {
            Ok(v) => (0, v, ops.coll_acc),
            Err(e) => (err_code(&e), 0, ops.coll_acc),
        }
    }

    fn rust_eq<const N1: usize, const N2: usize>(s1: &Side<N1>, s2: &Side<N2>) -> (u8, bool, u64) {
        let mut ops = SeamOps { coll_acc: 0 };
        let r = record_eq_core(
            &meta(s1),
            &datums(s1),
            &s1.nulls,
            &meta(s2),
            &datums(s2),
            &s2.nulls,
            &mut ops,
        );
        match r {
            Ok(v) => (0, v, ops.coll_acc),
            Err(e) => (err_code(&e), false, ops.coll_acc),
        }
    }

    unsafe fn c_cmp<const N1: usize, const N2: usize>(
        s1: &Side<N1>,
        s2: &Side<N2>,
    ) -> (u8, i32, u64) {
        let r = pg_c_record_cmp(
            N1 as i32,
            u8s(&s1.dropped).as_ptr(),
            s1.types.as_ptr(),
            s1.colls.as_ptr(),
            s1.vals.as_ptr(),
            u8s(&s1.nulls).as_ptr(),
            N2 as i32,
            u8s(&s2.dropped).as_ptr(),
            s2.types.as_ptr(),
            s2.colls.as_ptr(),
            s2.vals.as_ptr(),
            u8s(&s2.nulls).as_ptr(),
        );
        (pg_c_get_err() as u8, r, pg_c_get_coll_acc())
    }

    unsafe fn c_eq<const N1: usize, const N2: usize>(
        s1: &Side<N1>,
        s2: &Side<N2>,
    ) -> (u8, bool, u64) {
        let r = pg_c_record_eq(
            N1 as i32,
            u8s(&s1.dropped).as_ptr(),
            s1.types.as_ptr(),
            s1.colls.as_ptr(),
            s1.vals.as_ptr(),
            u8s(&s1.nulls).as_ptr(),
            N2 as i32,
            u8s(&s2.dropped).as_ptr(),
            s2.types.as_ptr(),
            s2.colls.as_ptr(),
            s2.vals.as_ptr(),
            u8s(&s2.nulls).as_ptr(),
        );
        (pg_c_get_err() as u8, r != 0, pg_c_get_coll_acc())
    }

    /// The shared cover-witness battery for a 2x2 harness (see module doc).
    /// Lives in DEDICATED cover harnesses (kissat re-solves per property
    /// batch — covers inside the parity theorems walled them at 30s).
    fn covers_2x2<const N: usize>(s1: &Side<N>, s2: &Side<N>, r_err: u8, decided: bool) {
        // null-ordering arm decided the result at logical column 0
        kani::cover!(
            r_err == 0
                && decided
                && !s1.dropped[0]
                && !s2.dropped[0]
                && s1.nulls[0] != s2.nulls[0]
        );
        // dropped-skip arm on a green run
        kani::cover!(r_err == 0 && (s1.dropped[0] || s2.dropped[0]));
        // short-circuit BEFORE the count-mismatch check: result decided
        // while the effective (non-dropped) column counts differ — C
        // deliberately does not report the mismatch then.
        kani::cover!(r_err == 0 && decided && s1.dropped[1] != s2.dropped[1]);
        // both error arms
        kani::cover!(r_err == ERR_COLCOUNT);
        kani::cover!(r_err == ERR_DISSIMILAR);
        // resolve precedes the null checks: the no-support error fires even
        // though BOTH paired columns are NULL (the preserved C quirk).
        kani::cover!(r_err == ERR_NOSUPPORT && s1.nulls[1] && s2.nulls[1]);
    }

    // -------------------- theorems (kissat) --------------------

    // Loop bound: each iteration advances i1 and/or i2, so <= N1+N2
    // iterations; +1 for the exit check.

    #[kani::proof]
    #[kani::unwind(6)]
    fn eq_record_cmp_int4x_2col() {
        let s1: Side<2> = any_side();
        let s2: Side<2> = any_side();
        let (r_err, r_res, r_acc) = rust_cmp(&s1, &s2);
        let (c_err, c_res, c_acc) = unsafe { c_cmp(&s1, &s2) };
        assert!(c_err == r_err);
        assert!(c_acc == r_acc);
        if r_err == 0 {
            assert!(c_res == r_res);
        }
    }

    /// Cover-witness harness for the cmp theorem: same input construction,
    /// same shipped core + concrete seam — proves the theorem's domain
    /// reaches every adjudicated arm (vacuity insurance).
    #[kani::proof]
    #[kani::unwind(6)]
    fn cover_record_cmp_int4x_2col() {
        let s1: Side<2> = any_side();
        let s2: Side<2> = any_side();
        let (r_err, r_res, _) = rust_cmp(&s1, &s2);
        covers_2x2(&s1, &s2, r_err, r_err == 0 && r_res != 0);
    }

    #[kani::proof]
    #[kani::unwind(6)]
    fn eq_record_eq_int4x_2col() {
        let s1: Side<2> = any_side();
        let s2: Side<2> = any_side();
        let (r_err, r_res, r_acc) = rust_eq(&s1, &s2);
        let (c_err, c_res, c_acc) = unsafe { c_eq(&s1, &s2) };
        assert!(c_err == r_err);
        assert!(c_acc == r_acc);
        if r_err == 0 {
            assert!(c_res == r_res);
        }
    }

    /// Cover-witness harness for the eq theorem (see cmp counterpart).
    #[kani::proof]
    #[kani::unwind(6)]
    fn cover_record_eq_int4x_2col() {
        let s1: Side<2> = any_side();
        let s2: Side<2> = any_side();
        let (r_err, r_res, _) = rust_eq(&s1, &s2);
        covers_2x2(&s1, &s2, r_err, r_err == 0 && !r_res);
    }

    #[kani::proof]
    #[kani::unwind(7)]
    fn eq_record_cmp_int4_2v3col() {
        let s1: Side<2> = any_side();
        let s2: Side<3> = any_side();
        let (r_err, r_res, r_acc) = rust_cmp(&s1, &s2);
        let (c_err, c_res, c_acc) = unsafe { c_cmp(&s1, &s2) };
        assert!(c_err == r_err);
        assert!(c_acc == r_acc);
        if r_err == 0 {
            assert!(c_res == r_res);
        }
    }

    /// Cover-witness harness for the asymmetric-descriptor theorems:
    /// green result reachable only via dropped-column rebalance, and the
    /// count-mismatch arm reachable.
    #[kani::proof]
    #[kani::unwind(7)]
    fn cover_record_cmp_int4_2v3col() {
        let s1: Side<2> = any_side();
        let s2: Side<3> = any_side();
        let (r_err, r_res, _) = rust_cmp(&s1, &s2);
        kani::cover!(r_err == 0 && r_res == 0);
        kani::cover!(r_err == ERR_COLCOUNT);
    }

    #[kani::proof]
    #[kani::unwind(7)]
    fn eq_record_eq_int4_2v3col() {
        let s1: Side<2> = any_side();
        let s2: Side<3> = any_side();
        let (r_err, r_res, r_acc) = rust_eq(&s1, &s2);
        let (c_err, c_res, c_acc) = unsafe { c_eq(&s1, &s2) };
        assert!(c_err == r_err);
        assert!(c_acc == r_acc);
        if r_err == 0 {
            assert!(c_res == r_res);
        }
    }

    /// Cover-witness harness for the eq 2v3 theorem.
    #[kani::proof]
    #[kani::unwind(7)]
    fn cover_record_eq_int4_2v3col() {
        let s1: Side<2> = any_side();
        let s2: Side<3> = any_side();
        let (r_err, r_res, _) = rust_eq(&s1, &s2);
        kani::cover!(r_err == 0 && r_res);
        kani::cover!(r_err == ERR_COLCOUNT);
    }

    // -------------------- controls (default solver, must FAIL) ----------

    /// Sign-flipped comparator: rig non-vacuity (comparator semantics half
    /// of the seam is load-bearing).
    struct SkewCmpOps {
        coll_acc: u64,
    }
    impl RecordColumnCmp for SkewCmpOps {
        type Err = u8;
        fn resolve(&mut self, _j: usize, typid: Oid) -> Result<(), u8> {
            seam_resolve(typid)
        }
        fn compare(&mut self, _j: usize, coll: Oid, d1: Datum, d2: Datum) -> Result<i32, u8> {
            self.coll_acc += coll as u64 + 1;
            let (a, b) = (d1.as_i32(), d2.as_i32());
            Ok(if a > b {
                -1 // SKEW: inverted sign
            } else if a == b {
                0
            } else {
                1
            })
        }
    }

    #[kani::proof]
    #[kani::unwind(6)]
    fn control_cmp_skewed_comparator() {
        let s1: Side<2> = any_side();
        let s2: Side<2> = any_side();
        let mut ops = SkewCmpOps { coll_acc: 0 };
        let (r_err, r_res) = match record_cmp_core(
            &meta(&s1),
            &datums(&s1),
            &s1.nulls,
            &meta(&s2),
            &datums(&s2),
            &s2.nulls,
            &mut ops,
        ) {
            Ok(v) => (0u8, v),
            Err(e) => (err_code(&e), 0),
        };
        let (c_err, c_res, _) = unsafe { c_cmp(&s1, &s2) };
        assert!(c_err == r_err);
        if r_err == 0 {
            assert!(c_res == r_res); // must FAIL: skewed seam cannot pass
        }
    }

    /// Resolve-set skew: Rust accepts the unsupported oid while C's
    /// typcache stub rejects it — proves the resolve half of the seam is
    /// load-bearing (and, with all-null columns in the counterexample
    /// space, that resolve placement is observable).
    struct SkewResolveOps;
    impl RecordColumnEq for SkewResolveOps {
        type Err = u8;
        fn resolve(&mut self, _j: usize, _typid: Oid) -> Result<(), u8> {
            Ok(()) // SKEW: accepts everything
        }
        fn equal(&mut self, _j: usize, _coll: Oid, d1: Datum, d2: Datum) -> Result<bool, u8> {
            Ok(d1.as_i32() == d2.as_i32())
        }
    }

    #[kani::proof]
    #[kani::unwind(6)]
    fn control_eq_seam_skew_resolve() {
        let s1: Side<2> = any_side();
        let s2: Side<2> = any_side();
        let mut ops = SkewResolveOps;
        let r_err = match record_eq_core(
            &meta(&s1),
            &datums(&s1),
            &s1.nulls,
            &meta(&s2),
            &datums(&s2),
            &s2.nulls,
            &mut ops,
        ) {
            Ok(_) => 0u8,
            Err(e) => err_code(&e),
        };
        let (c_err, _, _) = unsafe { c_eq(&s1, &s2) };
        assert!(c_err == r_err); // must FAIL when a NOCMP column is reached
    }
}
