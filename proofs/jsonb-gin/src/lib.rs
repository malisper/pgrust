//! Kani C≡Rust equivalence: jsonb GIN opclass support (jsonb_gin.c) and the
//! GIN array-opclass consistent pair (ginarrayproc.c).
//!
//! Ledger rows: 3482 gin_extract_jsonb, 3484 gin_consistent_jsonb,
//! 3485 gin_extract_jsonb_path, 3487 gin_consistent_jsonb_path,
//! 3488 gin_triconsistent_jsonb, 3489 gin_triconsistent_jsonb_path,
//! 2744 ginarrayconsistent, 3920 ginarraytriconsistent.
//! (3483/3486 gin_extract_jsonb_query[_path] are NOT harnessed here — see
//! the lane report: the jsonpath-query arms need a jsonpath binary payload
//! builder and the text[]-strategy arms need arrayfuncs; their Contains
//! arms delegate to the extract cores proven here.)
//!
//! Rust side (SHIPPED code, path-dep — never copied):
//!   adt_jsonb::gin::{gin_consistent_jsonb, gin_triconsistent_jsonb,
//!     gin_consistent_jsonb_path, gin_triconsistent_jsonb_path,
//!     execute_jsp_gin_ops (via the four above), gin_extract_jsonb,
//!     gin_extract_jsonb_path}
//!   gin::opclass::{consistent, tri_consistent} (ArrayOps arm ==
//!     ginarrayconsistent / ginarraytriconsistent; visibility-only shipped
//!     edit, see the lane report)
//! C side: c/pg_jsonb_gin.c + c/pg_ginarray.c — verbatim REL_18_STABLE
//! (provenance + every shim documented there). Run each harness with ONLY
//! its family's --c-lib (mbconv law: whole-family linking inflates every
//! goto program).
//!
//! THEOREM SHAPES
//! - consistent/triconsistent (jsonb_ops + path_ops): fully symbolic check
//!   arrays (binary cells fence check bytes to {0,1} = the C bool caller
//!   contract; ternary cells to {0,1,2}), symbolic nkeys <= MAXK, one
//!   concrete strategy per harness (symbolic-selector-with-cold-panic-arm
//!   trap). The jsonpath arms run over a BOUNDED OPS-SHAPE FENCE: per-shape
//!   cells (preorder ops array on the Rust side, the equivalent
//!   JsonPathGinNode tree built by C-side scaffolding) with symbolic entry
//!   indices < nkeys, plus cover_jsp_shapes_len4 — the union-coverage
//!   theorem that the enumerated shapes are ALL well-formed extractor
//!   outputs (OR arity 2, AND arity 2..=3, per the shipped extractor's
//!   expr_node_binary/and_nodes emitters) up to 4 ops; the two 5-op nested
//!   shapes are spot cells beyond the covered bound.
//! - ginarray[tri]consistent: symbolic check + queryCategories (fenced
//!   {0,1}; C reads the same bytes as its bool nullFlags — GIN_CAT_NULL_KEY
//!   == 1), symbolic nkeys <= MAXK, per-strategy cells, recheck parity
//!   in-theorem. Wrapper level: the shipped gin::opclass::consistent /
//!   tri_consistent dispatch with a concrete ArrayOps GinColState.
//! - gin_extract_jsonb[_path]: per-n cells (n <= 3; a symbolic element
//!   count makes builder addresses symbolic — jsonb-probe law) over the
//!   trusted-builder jsonb images (layout-valid containers; scalars fenced
//!   to null/bool/string, string lengths PINNED per cell so output store
//!   offsets stay concrete — result-image law mitigation; numeric out of
//!   fence). jsonb_ops output = text-key IMAGES compared byte-wise through
//!   C-side pointee reads (brin provenance lesson); path_ops output =
//!   uint32 hash datums (scalar-verdict class). mcx-stubs +
//!   tiny-proof-heap recipe; token-ctx scaffolding (jsonb-probe
//!   precedent).
//!
//! SEAM (extract path_ops string cell only): hash_any / hashfn::hash_bytes
//! replaced on BOTH sides by the identical FNV-1a-32 model
//! (pg_seam_hash_bytes / stub_hash_bytes) — hash internals leave the proof;
//! hashfn is a separately-PROVED family. control_extract_hash_seam_skew
//! (C side skewed, MUST FAIL) proves the seam model is load-bearing.
//!
//! CONTROLS (MUST FAIL, run with the DEFAULT solver — kissat never
//! terminates on failing harnesses): control_jsp_wrong_entry (C tree wired
//! to a different entry index), control_ginarray_null_skew (C nullFlags
//! inverted), control_extract_hash_seam_skew.
//!
//! Expected classes: consistent/ginarray cells fast (pure ternary logic /
//! bounded scans, no dividers); extract cells = mcx-stubs tier (hex/jsonb
//! precedent: tens of seconds, release-gate). The jsonb_ops extract images
//! are the honest result-image risk — lengths are pinned so offsets stay
//! concrete, but if a cell still walls in CNF, record
//! wall(CNF width-bound) per the result-image law and keep the path_ops
//! (scalar) cells.

#[cfg(kani)]
mod proofs {
    use datum::Datum;
    use gin_vocab::{
        GinColState, GinElemCmp, GinOpclass, JspGinOp, JSP_GIN_AND, JSP_GIN_ENTRY, JSP_GIN_OR,
    };
    use proof_support::{mcx_stubs, stubs};
    use std::os::raw::c_int;

    extern "C" {
        // c/pg_jsonb_gin.c
        fn pgg_reset() -> c_int;
        fn pgg_take_abort() -> c_int;
        fn pgg_set_hash_skew(on: c_int) -> c_int;
        fn pgg_mk_entry(entry_index: c_int) -> c_int;
        fn pgg_mk_expr2(typ: c_int, a: c_int, b: c_int) -> c_int;
        fn pgg_mk_expr3(typ: c_int, a: c_int, b: c_int, c: c_int) -> c_int;
        fn pgg_consistent_jsonb_h(
            check: *const u8,
            strategy: u32,
            nkeys: c_int,
            handle: c_int,
            recheck_out: *mut c_int,
            err: *mut c_int,
        ) -> c_int;
        fn pgg_triconsistent_jsonb_h(
            check: *const i8,
            strategy: u32,
            nkeys: c_int,
            handle: c_int,
            err: *mut c_int,
        ) -> c_int;
        fn pgg_consistent_jsonb_path_h(
            check: *const u8,
            strategy: u32,
            nkeys: c_int,
            handle: c_int,
            recheck_out: *mut c_int,
            err: *mut c_int,
        ) -> c_int;
        fn pgg_triconsistent_jsonb_path_h(
            check: *const i8,
            strategy: u32,
            nkeys: c_int,
            handle: c_int,
            err: *mut c_int,
        ) -> c_int;
        fn pgg_extract_jsonb(container: *const u8) -> c_int;
        fn pgg_extract_jsonb_path(container: *const u8) -> c_int;
        fn pgg_entry_len(i: c_int) -> c_int;
        fn pgg_entry_byte(i: c_int, off: c_int) -> c_int;
        fn pgg_entry_u32(i: c_int) -> u32;

        // c/pg_ginarray.c
        fn pga_consistent(
            check: *const u8,
            strategy: u32,
            nkeys: c_int,
            null_flags: *const u8,
            recheck_out: *mut c_int,
            err: *mut c_int,
        ) -> c_int;
        fn pga_triconsistent(
            check: *const i8,
            strategy: u32,
            nkeys: c_int,
            null_flags: *const u8,
            err: *mut c_int,
        ) -> c_int;
    }

    // strategy numbers (jsonb.h verbatim values; the shipped constants are
    // asserted against these in eq_strategy_constants below)
    const CONTAINS: u16 = 7;
    const EXISTS: u16 = 9;
    const EXISTS_ANY: u16 = 10;
    const EXISTS_ALL: u16 = 11;
    const JSP_EXISTS: u16 = 15;
    const JSP_PREDICATE: u16 = 16;

    const GIN_FALSE: i8 = 0;
    const GIN_TRUE: i8 = 1;
    const GIN_MAYBE: i8 = 2;

    /// shipped-vs-vendored constant wiring (table-transcription style)
    #[kani::proof]
    fn eq_strategy_constants() {
        assert!(adt_jsonb::gin::JsonbContainsStrategyNumber == CONTAINS);
        assert!(adt_jsonb::gin::JsonbExistsStrategyNumber == EXISTS);
        assert!(adt_jsonb::gin::JsonbExistsAnyStrategyNumber == EXISTS_ANY);
        assert!(adt_jsonb::gin::JsonbExistsAllStrategyNumber == EXISTS_ALL);
        assert!(adt_jsonb::gin::JsonbJsonpathExistsStrategyNumber == JSP_EXISTS);
        assert!(adt_jsonb::gin::JsonbJsonpathPredicateStrategyNumber == JSP_PREDICATE);
        assert!(JSP_GIN_OR == 0 && JSP_GIN_AND == 1 && JSP_GIN_ENTRY == 2);
        assert!(gin_vocab::GIN_CAT_NULL_KEY == 1);
    }

    /// check-array cap (GIN nkeys is unbounded in production; the scan
    /// loops are length-linear with no dividers, so MAXK=4 exercises every
    /// break/continue path class).
    const MAXK: usize = 4;

    fn any_nkeys(min: usize) -> usize {
        let nk: usize = kani::any();
        kani::assume(nk >= min && nk <= MAXK);
        nk
    }

    /// binary check array: C reads bool (ginlogic caller contract 0/1).
    fn any_check_bool() -> [i8; MAXK] {
        let c: [i8; MAXK] = kani::any();
        for i in 0..MAXK {
            kani::assume(c[i] == 0 || c[i] == 1);
        }
        c
    }

    /// ternary check array: GinTernaryValue in {FALSE, TRUE, MAYBE}.
    fn any_check_tern() -> [i8; MAXK] {
        let c: [i8; MAXK] = kani::any();
        for i in 0..MAXK {
            kani::assume(c[i] >= 0 && c[i] <= 2);
        }
        c
    }

    fn any_entry_idx(nkeys: usize) -> u32 {
        let i: u32 = kani::any();
        kani::assume((i as usize) < nkeys);
        i
    }

    fn assert_no_abort() {
        assert!(unsafe { pgg_take_abort() } == 0);
    }

    // =================== jsonpath ops-shape scaffolding ===================

    const OPSCAP: usize = 5;

    /// One shape cell: Rust preorder ops + the C node-tree handle.
    struct Shape {
        ops: [JspGinOp; OPSCAP],
        len: usize,
        handle: c_int,
    }

    fn e(val: u32) -> JspGinOp {
        JspGinOp {
            kind: JSP_GIN_ENTRY,
            val,
        }
    }
    fn x(kind: u8, nargs: u32) -> JspGinOp {
        JspGinOp { kind, val: nargs }
    }
    const PAD: JspGinOp = JspGinOp {
        kind: JSP_GIN_ENTRY,
        val: 0,
    };

    fn shape_entry(nk: usize) -> Shape {
        let i0 = any_entry_idx(nk);
        let h = unsafe { pgg_mk_entry(i0 as c_int) };
        Shape {
            ops: [e(i0), PAD, PAD, PAD, PAD],
            len: 1,
            handle: h,
        }
    }

    fn shape_bin(kind: u8, nk: usize) -> Shape {
        let (i0, i1) = (any_entry_idx(nk), any_entry_idx(nk));
        let h = unsafe {
            let a = pgg_mk_entry(i0 as c_int);
            let b = pgg_mk_entry(i1 as c_int);
            pgg_mk_expr2(kind as c_int, a, b)
        };
        Shape {
            ops: [x(kind, 2), e(i0), e(i1), PAD, PAD],
            len: 3,
            handle: h,
        }
    }

    fn shape_and3(nk: usize) -> Shape {
        let (i0, i1, i2) = (any_entry_idx(nk), any_entry_idx(nk), any_entry_idx(nk));
        let h = unsafe {
            let a = pgg_mk_entry(i0 as c_int);
            let b = pgg_mk_entry(i1 as c_int);
            let c = pgg_mk_entry(i2 as c_int);
            pgg_mk_expr3(JSP_GIN_AND as c_int, a, b, c)
        };
        Shape {
            ops: [x(JSP_GIN_AND, 3), e(i0), e(i1), e(i2), PAD],
            len: 4,
            handle: h,
        }
    }

    /// outer(inner(E,E), E) — the two 5-op spot shapes.
    fn shape_nested(outer: u8, inner: u8, nk: usize) -> Shape {
        let (i0, i1, i2) = (any_entry_idx(nk), any_entry_idx(nk), any_entry_idx(nk));
        let h = unsafe {
            let a = pgg_mk_entry(i0 as c_int);
            let b = pgg_mk_entry(i1 as c_int);
            let inner_h = pgg_mk_expr2(inner as c_int, a, b);
            let c = pgg_mk_entry(i2 as c_int);
            pgg_mk_expr2(outer as c_int, inner_h, c)
        };
        Shape {
            ops: [x(outer, 2), x(inner, 2), e(i0), e(i1), e(i2)],
            len: 5,
            handle: h,
        }
    }

    /// Binary jsonpath-arm cell: shipped gin_consistent_jsonb[_path] vs
    /// verbatim C; res + recheck parity, cover witnesses on both verdicts.
    fn check_jsp_bin(shape: fn(usize) -> Shape, strategy: u16, path_variant: bool) {
        unsafe { pgg_reset() };
        let nk = any_nkeys(1);
        let s = shape(nk);
        let check = any_check_bool();
        let mut recheck = false;
        let r = if path_variant {
            adt_jsonb::gin::gin_consistent_jsonb_path(
                &check[..],
                strategy,
                nk,
                &mut recheck,
                &s.ops[..s.len],
            )
        } else {
            adt_jsonb::gin::gin_consistent_jsonb(
                &check[..],
                strategy,
                nk,
                &mut recheck,
                &s.ops[..s.len],
            )
        };
        let mut c_recheck: c_int = 0;
        let mut err: c_int = 0;
        let c = unsafe {
            if path_variant {
                pgg_consistent_jsonb_path_h(
                    check.as_ptr() as *const u8,
                    strategy as u32,
                    nk as c_int,
                    s.handle,
                    &mut c_recheck,
                    &mut err,
                )
            } else {
                pgg_consistent_jsonb_h(
                    check.as_ptr() as *const u8,
                    strategy as u32,
                    nk as c_int,
                    s.handle,
                    &mut c_recheck,
                    &mut err,
                )
            }
        };
        assert!(err == 0);
        assert_no_abort();
        kani::cover!(r);
        kani::cover!(!r);
        assert!(r == (c != 0));
        assert!(recheck == (c_recheck != 0));
    }

    /// Ternary jsonpath-arm cell (GIN_TRUE -> GIN_MAYBE mapping in-theorem).
    fn check_jsp_tri(shape: fn(usize) -> Shape, strategy: u16, path_variant: bool) {
        unsafe { pgg_reset() };
        let nk = any_nkeys(1);
        let s = shape(nk);
        let check = any_check_tern();
        let r = if path_variant {
            adt_jsonb::gin::gin_triconsistent_jsonb_path(&check[..], strategy, nk, &s.ops[..s.len])
        } else {
            adt_jsonb::gin::gin_triconsistent_jsonb(&check[..], strategy, nk, &s.ops[..s.len])
        };
        let mut err: c_int = 0;
        let c = unsafe {
            if path_variant {
                pgg_triconsistent_jsonb_path_h(
                    check.as_ptr(),
                    strategy as u32,
                    nk as c_int,
                    s.handle,
                    &mut err,
                )
            } else {
                pgg_triconsistent_jsonb_h(
                    check.as_ptr(),
                    strategy as u32,
                    nk as c_int,
                    s.handle,
                    &mut err,
                )
            }
        };
        assert!(err == 0);
        assert_no_abort();
        kani::cover!(r == GIN_FALSE);
        kani::cover!(r == GIN_MAYBE);
        // never GIN_TRUE (recheck forcing) — a shipped comment-claim, now a
        // machine-checked invariant
        assert!(r != GIN_TRUE);
        assert!(r as c_int == c);
    }

    fn sh_and2(nk: usize) -> Shape {
        shape_bin(JSP_GIN_AND, nk)
    }
    fn sh_or2(nk: usize) -> Shape {
        shape_bin(JSP_GIN_OR, nk)
    }
    fn sh_and_or(nk: usize) -> Shape {
        shape_nested(JSP_GIN_AND, JSP_GIN_OR, nk)
    }
    fn sh_or_and(nk: usize) -> Shape {
        shape_nested(JSP_GIN_OR, JSP_GIN_AND, nk)
    }

    macro_rules! jsp_cells {
        ($($name:ident[$unwind:literal]: $check:ident($shape:expr, $strat:expr, $path:expr);)*) => {$(
            #[kani::proof]
            #[kani::unwind($unwind)]
            fn $name() { $check($shape, $strat, $path); }
        )*};
    }

    jsp_cells! {
        // jsonb_ops wrapper, strategy 15 (JsonpathExists), binary
        eq_jsp_bin_entry[6]: check_jsp_bin(shape_entry, JSP_EXISTS, false);
        eq_jsp_bin_and2[6]: check_jsp_bin(sh_and2, JSP_EXISTS, false);
        eq_jsp_bin_or2[6]: check_jsp_bin(sh_or2, JSP_EXISTS, false);
        eq_jsp_bin_and3[6]: check_jsp_bin(shape_and3, JSP_EXISTS, false);
        eq_jsp_bin_and_or[6]: check_jsp_bin(sh_and_or, JSP_EXISTS, false);
        eq_jsp_bin_or_and[6]: check_jsp_bin(sh_or_and, JSP_EXISTS, false);
        // ternary
        eq_jsp_tri_entry[6]: check_jsp_tri(shape_entry, JSP_EXISTS, false);
        eq_jsp_tri_and2[6]: check_jsp_tri(sh_and2, JSP_EXISTS, false);
        eq_jsp_tri_or2[6]: check_jsp_tri(sh_or2, JSP_EXISTS, false);
        eq_jsp_tri_and3[6]: check_jsp_tri(shape_and3, JSP_EXISTS, false);
        eq_jsp_tri_and_or[6]: check_jsp_tri(sh_and_or, JSP_EXISTS, false);
        eq_jsp_tri_or_and[6]: check_jsp_tri(sh_or_and, JSP_EXISTS, false);
        // strategy 16 (JsonpathPredicate) aliases the same arm — one cell each
        eq_jsp_bin_pred_and2[6]: check_jsp_bin(sh_and2, JSP_PREDICATE, false);
        eq_jsp_tri_pred_or2[6]: check_jsp_tri(sh_or2, JSP_PREDICATE, false);
        // jsonb_path_ops wrapper (identical jsonpath arms, separate shipped fn)
        eq_jsp_path_bin_and_or[6]: check_jsp_bin(sh_and_or, JSP_EXISTS, true);
        eq_jsp_path_tri_or_and[6]: check_jsp_tri(sh_or_and, JSP_EXISTS, true);
    }

    /// nkeys == 0 jsonpath arm: res forced true / MAYBE, no tree walk.
    fn check_jsp_nokeys(path_variant: bool) {
        unsafe { pgg_reset() };
        let check: [i8; MAXK] = [0; MAXK];
        let mut recheck = false;
        let (r, rt) = if path_variant {
            (
                adt_jsonb::gin::gin_consistent_jsonb_path(
                    &check[..],
                    JSP_EXISTS,
                    0,
                    &mut recheck,
                    &[],
                ),
                adt_jsonb::gin::gin_triconsistent_jsonb_path(&check[..], JSP_EXISTS, 0, &[]),
            )
        } else {
            (
                adt_jsonb::gin::gin_consistent_jsonb(&check[..], JSP_EXISTS, 0, &mut recheck, &[]),
                adt_jsonb::gin::gin_triconsistent_jsonb(&check[..], JSP_EXISTS, 0, &[]),
            )
        };
        let mut c_recheck: c_int = 0;
        let mut err: c_int = 0;
        let (c, ct) = unsafe {
            if path_variant {
                (
                    pgg_consistent_jsonb_path_h(
                        check.as_ptr() as *const u8,
                        JSP_EXISTS as u32,
                        0,
                        -1,
                        &mut c_recheck,
                        &mut err,
                    ),
                    pgg_triconsistent_jsonb_path_h(
                        check.as_ptr(),
                        JSP_EXISTS as u32,
                        0,
                        -1,
                        &mut err,
                    ),
                )
            } else {
                (
                    pgg_consistent_jsonb_h(
                        check.as_ptr() as *const u8,
                        JSP_EXISTS as u32,
                        0,
                        -1,
                        &mut c_recheck,
                        &mut err,
                    ),
                    pgg_triconsistent_jsonb_h(check.as_ptr(), JSP_EXISTS as u32, 0, -1, &mut err),
                )
            }
        };
        assert!(err == 0);
        assert_no_abort();
        assert!(r == (c != 0));
        assert!(recheck == (c_recheck != 0));
        assert!(rt as c_int == ct);
    }

    #[kani::proof]
    #[kani::unwind(6)]
    fn eq_jsp_nokeys() {
        check_jsp_nokeys(false);
    }

    #[kani::proof]
    #[kani::unwind(6)]
    fn eq_jsp_path_nokeys() {
        check_jsp_nokeys(true);
    }

    /// UNION COVERAGE (mandatory for the shape case-split): every
    /// well-formed extractor-emittable preorder ops sequence of <= 4 ops
    /// (ENTRY leaves; OR arity exactly 2 — expr_node_binary is the only OR
    /// emitter; AND arity 2..=3 within this bound — and_nodes emits n-ary
    /// AND) is one of the enumerated cells: ENTRY / AND2(E,E) / OR2(E,E) /
    /// AND3(E,E,E). The 5-op nested cells are spot shapes beyond this
    /// covered bound (fence documented in the ledger rows).
    #[kani::proof]
    #[kani::unwind(7)]
    fn cover_jsp_shapes_len4() {
        let len: usize = kani::any();
        kani::assume(len >= 1 && len <= 4);
        let kinds: [u8; 4] = kani::any();
        let vals: [u32; 4] = kani::any();

        // well-formedness: preorder arity walk consumes exactly `len` ops
        let mut need = 1usize;
        let mut ok = true;
        for i in 0..4 {
            if i < len {
                if need == 0 {
                    ok = false;
                } else {
                    need -= 1;
                    if kinds[i] == JSP_GIN_ENTRY {
                    } else if kinds[i] == JSP_GIN_OR {
                        if vals[i] != 2 {
                            ok = false;
                        }
                        need += 2;
                    } else if kinds[i] == JSP_GIN_AND {
                        if vals[i] != 2 && vals[i] != 3 {
                            ok = false;
                        }
                        need += vals[i] as usize;
                    } else {
                        ok = false;
                    }
                }
            }
        }
        kani::assume(ok && need == 0);

        // coverage: the sequence must be one of the harnessed shapes
        let is_entry = len == 1 && kinds[0] == JSP_GIN_ENTRY;
        let is_bin2 = len == 3
            && (kinds[0] == JSP_GIN_AND || kinds[0] == JSP_GIN_OR)
            && vals[0] == 2
            && kinds[1] == JSP_GIN_ENTRY
            && kinds[2] == JSP_GIN_ENTRY;
        let is_and3 = len == 4
            && kinds[0] == JSP_GIN_AND
            && vals[0] == 3
            && kinds[1] == JSP_GIN_ENTRY
            && kinds[2] == JSP_GIN_ENTRY
            && kinds[3] == JSP_GIN_ENTRY;
        kani::cover!(is_entry);
        kani::cover!(is_bin2);
        kani::cover!(is_and3);
        assert!(is_entry || is_bin2 || is_and3);
    }

    // ============ non-jsonpath jsonb_ops / path_ops strategies ============

    fn check_plain_bin(strategy: u16, path_variant: bool) {
        unsafe { pgg_reset() };
        let nk = any_nkeys(0);
        let check = any_check_bool();
        let mut recheck = false;
        let r = if path_variant {
            adt_jsonb::gin::gin_consistent_jsonb_path(&check[..], strategy, nk, &mut recheck, &[])
        } else {
            adt_jsonb::gin::gin_consistent_jsonb(&check[..], strategy, nk, &mut recheck, &[])
        };
        let mut c_recheck: c_int = 0;
        let mut err: c_int = 0;
        let c = unsafe {
            if path_variant {
                pgg_consistent_jsonb_path_h(
                    check.as_ptr() as *const u8,
                    strategy as u32,
                    nk as c_int,
                    -1,
                    &mut c_recheck,
                    &mut err,
                )
            } else {
                pgg_consistent_jsonb_h(
                    check.as_ptr() as *const u8,
                    strategy as u32,
                    nk as c_int,
                    -1,
                    &mut c_recheck,
                    &mut err,
                )
            }
        };
        assert!(err == 0);
        assert_no_abort();
        kani::cover!(r);
        kani::cover!(!r);
        assert!(r == (c != 0));
        assert!(recheck == (c_recheck != 0));
    }

    fn check_plain_tri(strategy: u16, path_variant: bool) {
        unsafe { pgg_reset() };
        let nk = any_nkeys(0);
        let check = any_check_tern();
        let r = if path_variant {
            adt_jsonb::gin::gin_triconsistent_jsonb_path(&check[..], strategy, nk, &[])
        } else {
            adt_jsonb::gin::gin_triconsistent_jsonb(&check[..], strategy, nk, &[])
        };
        let mut err: c_int = 0;
        let c = unsafe {
            if path_variant {
                pgg_triconsistent_jsonb_path_h(
                    check.as_ptr(),
                    strategy as u32,
                    nk as c_int,
                    -1,
                    &mut err,
                )
            } else {
                pgg_triconsistent_jsonb_h(check.as_ptr(), strategy as u32, nk as c_int, -1, &mut err)
            }
        };
        assert!(err == 0);
        assert_no_abort();
        kani::cover!(r == GIN_FALSE);
        kani::cover!(r == GIN_MAYBE);
        assert!(r != GIN_TRUE);
        assert!(r as c_int == c);
    }

    macro_rules! plain_cells {
        ($($name:ident: $check:ident($strat:expr, $path:expr);)*) => {$(
            #[kani::proof]
            #[kani::unwind(6)]
            fn $name() { $check($strat, $path); }
        )*};
    }

    plain_cells! {
        eq_consistent_jsonb_contains: check_plain_bin(CONTAINS, false);
        eq_consistent_jsonb_exists: check_plain_bin(EXISTS, false);
        eq_consistent_jsonb_exists_any: check_plain_bin(EXISTS_ANY, false);
        eq_consistent_jsonb_exists_all: check_plain_bin(EXISTS_ALL, false);
        eq_triconsistent_jsonb_contains: check_plain_tri(CONTAINS, false);
        eq_triconsistent_jsonb_exists: check_plain_tri(EXISTS, false);
        eq_triconsistent_jsonb_exists_any: check_plain_tri(EXISTS_ANY, false);
        eq_triconsistent_jsonb_exists_all: check_plain_tri(EXISTS_ALL, false);
        eq_consistent_jsonb_path_contains: check_plain_bin(CONTAINS, true);
        eq_triconsistent_jsonb_path_contains: check_plain_tri(CONTAINS, true);
    }

    // ============== ginarray[tri]consistent (gin::opclass) ================

    /// Token Mcx handle (jsonb-probe precedent): the ArrayOps consistent
    /// arms never allocate, so the handle is never dereferenced; the zeroed
    /// image is never read.
    fn token_ctx() -> &'static mcx::MemoryContext {
        static CTX: [u8; 256] = [0u8; 256];
        assert!(core::mem::size_of::<mcx::MemoryContext>() <= 256);
        unsafe { &*(CTX.as_ptr() as *const mcx::MemoryContext) }
    }

    fn array_col() -> GinColState {
        GinColState {
            opclass: GinOpclass::ArrayOps,
            // elem_cmp/key layout only reach compare/extract, not consistent
            elem_cmp: GinElemCmp::Int4,
            support_collation: 0,
            can_partial_match: false,
            key_byval: true,
            key_len: 4,
        }
    }

    /// queryCategories bytes, fenced to {GIN_CAT_NORM_KEY, GIN_CAT_NULL_KEY}
    /// = {0,1} (the ginNewScanKey population for array keys); the SAME bytes
    /// are C's bool nullFlags.
    fn any_categories() -> [i8; MAXK] {
        let c: [i8; MAXK] = kani::any();
        for i in 0..MAXK {
            kani::assume(c[i] == 0 || c[i] == 1);
        }
        c
    }

    fn run_ginarray_bin(
        check: &[i8; MAXK],
        cats: &[i8; MAXK],
        strategy: u16,
        nk: usize,
    ) -> (bool, bool) {
        let col = array_col();
        let mut recheck = false;
        let r = gin::opclass::consistent(
            token_ctx().mcx(),
            &col,
            &check[..],
            strategy,
            Datum::null(),
            nk,
            &[],
            &cats[..],
            &[],
            &[],
            None,
            &mut recheck,
        );
        match r {
            Ok(v) => (v, recheck),
            Err(e) => {
                core::mem::forget(e); // Err drop-glue trap (varbit lesson)
                panic!("ginarray consistent errored");
            }
        }
    }

    fn check_ginarray_bin(strategy: u16) {
        let nk = any_nkeys(0);
        let check = any_check_bool();
        let cats = any_categories();
        let (r, recheck) = run_ginarray_bin(&check, &cats, strategy, nk);
        let mut c_recheck: c_int = 0;
        let mut err: c_int = 0;
        let c = unsafe {
            pga_consistent(
                check.as_ptr() as *const u8,
                strategy as u32,
                nk as c_int,
                cats.as_ptr() as *const u8,
                &mut c_recheck,
                &mut err,
            )
        };
        assert!(err == 0);
        kani::cover!(r);
        kani::cover!(!r);
        assert!(r == (c != 0));
        assert!(recheck == (c_recheck != 0));
    }

    fn run_ginarray_tri(check: &[i8; MAXK], cats: &[i8; MAXK], strategy: u16, nk: usize) -> i8 {
        let col = array_col();
        let r = gin::opclass::tri_consistent(
            token_ctx().mcx(),
            &col,
            &check[..],
            strategy,
            Datum::null(),
            nk,
            &[],
            &cats[..],
            &[],
            &[],
            None,
        );
        match r {
            Ok(v) => v,
            Err(e) => {
                core::mem::forget(e);
                panic!("ginarray tri_consistent errored");
            }
        }
    }

    fn check_ginarray_tri(strategy: u16) {
        let nk = any_nkeys(0);
        let check = any_check_tern();
        let cats = any_categories();
        let r = run_ginarray_tri(&check, &cats, strategy, nk);
        let mut err: c_int = 0;
        let c = unsafe {
            pga_triconsistent(
                check.as_ptr(),
                strategy as u32,
                nk as c_int,
                cats.as_ptr() as *const u8,
                &mut err,
            )
        };
        assert!(err == 0);
        kani::cover!(r == GIN_FALSE);
        kani::cover!(r == GIN_MAYBE);
        assert!(r as c_int == c);
    }

    macro_rules! ginarray_cells {
        ($($name:ident: $check:ident($strat:literal);)*) => {$(
            #[kani::proof]
            #[kani::unwind(6)]
            fn $name() { $check($strat); }
        )*};
    }

    ginarray_cells! {
        eq_ginarray_consistent_overlap: check_ginarray_bin(1);
        eq_ginarray_consistent_contains: check_ginarray_bin(2);
        eq_ginarray_consistent_contained: check_ginarray_bin(3);
        eq_ginarray_consistent_equal: check_ginarray_bin(4);
        eq_ginarray_tri_overlap: check_ginarray_tri(1);
        eq_ginarray_tri_contains: check_ginarray_tri(2);
        eq_ginarray_tri_contained: check_ginarray_tri(3);
        eq_ginarray_tri_equal: check_ginarray_tri(4);
    }

    // ======================= extract cells =================================

    // trusted builder (re-implemented from proofs/jsonb-probe by reading —
    // that crate is owned by a live lane): depth-1 arrays/objects, scalars
    // null/false/true/string, string lengths PINNED per cell.
    const MAXN: usize = 3;
    const KL: usize = 3;
    const VL: usize = 3;
    const CAP: usize = 96;

    const JB_FARRAY: u32 = 0x4000_0000;
    const JB_FOBJECT: u32 = 0x2000_0000;
    const JENTRY_HAS_OFF: u32 = 0x8000_0000;
    const JENTRY_ISSTRING: u32 = 0x0000_0000;
    const JENTRY_ISBOOL_FALSE: u32 = 0x2000_0000;
    const JENTRY_ISBOOL_TRUE: u32 = 0x3000_0000;
    const JENTRY_ISNULL: u32 = 0x4000_0000;

    #[repr(align(8))]
    struct Img([u8; CAP]);

    fn put_u32(buf: &mut Img, off: usize, v: u32) {
        buf.0[off..off + 4].copy_from_slice(&v.to_ne_bytes());
    }

    /// kinds: 0 null, 1 false, 2 true, 3 string(slen pinned by the cell)
    #[derive(Clone, Copy)]
    struct Scalar {
        kind: u8,
        sbytes: [u8; VL],
        slen: usize,
    }

    fn any_scalar_len(slen: usize) -> Scalar {
        let kind: u8 = kani::any();
        kani::assume(kind <= 3);
        let sbytes: [u8; VL] = kani::any();
        Scalar { kind, sbytes, slen }
    }

    /// null/bool only (no string data, no hash seam)
    fn any_scalar_nb() -> Scalar {
        let kind: u8 = kani::any();
        kani::assume(kind <= 2);
        Scalar {
            kind,
            sbytes: [0; VL],
            slen: 0,
        }
    }

    fn scalar_len(s: &Scalar) -> usize {
        if s.kind == 3 {
            s.slen
        } else {
            0
        }
    }

    fn scalar_jentry_type(s: &Scalar) -> u32 {
        match s.kind {
            0 => JENTRY_ISNULL,
            1 => JENTRY_ISBOOL_FALSE,
            2 => JENTRY_ISBOOL_TRUE,
            _ => JENTRY_ISSTRING,
        }
    }

    /// JEntry: length-form or (symbolic choice) HAS_OFF cumulative-end form
    /// — the reader-side format superset (jsonb.h).
    fn jentry(ty: u32, len: usize, end: u32) -> u32 {
        let hoff: bool = kani::any();
        ty | if hoff { end | JENTRY_HAS_OFF } else { len as u32 }
    }

    fn build_array(elems: &[Scalar; MAXN], n: usize) -> Img {
        let mut buf = Img([0u8; CAP]);
        put_u32(&mut buf, 0, n as u32 | JB_FARRAY);
        let base = 4 + 4 * n;
        let mut end: u32 = 0;
        let mut pos = base;
        for i in 0..MAXN {
            if i < n {
                let l = scalar_len(&elems[i]);
                end += l as u32;
                let je = jentry(scalar_jentry_type(&elems[i]), l, end);
                put_u32(&mut buf, 4 + 4 * i, je);
                if elems[i].kind == 3 {
                    for j in 0..VL {
                        if j < elems[i].slen {
                            buf.0[pos + j] = elems[i].sbytes[j];
                        }
                    }
                    pos += elems[i].slen;
                }
            }
        }
        buf
    }

    #[derive(Clone, Copy)]
    struct Key {
        bytes: [u8; KL],
        len: usize,
    }

    fn any_key_len(len: usize) -> Key {
        let bytes: [u8; KL] = kani::any();
        Key { bytes, len }
    }

    /// depth-1 object {n pairs}; single-key cells need no sort assume
    /// (multi-key object cells would need the lengthCompareJsonbString
    /// sorted-keys writer invariant, as in jsonb-probe).
    fn build_object(keys: &[Key; MAXN], vals: &[Scalar; MAXN], n: usize) -> Img {
        let mut buf = Img([0u8; CAP]);
        put_u32(&mut buf, 0, n as u32 | JB_FOBJECT);
        let base = 4 + 8 * n;
        let mut end: u32 = 0;
        let mut pos = base;
        for i in 0..MAXN {
            if i < n {
                end += keys[i].len as u32;
                let je = jentry(JENTRY_ISSTRING, keys[i].len, end);
                put_u32(&mut buf, 4 + 4 * i, je);
                for j in 0..KL {
                    if j < keys[i].len {
                        buf.0[pos + j] = keys[i].bytes[j];
                    }
                }
                pos += keys[i].len;
            }
        }
        for i in 0..MAXN {
            if i < n {
                let l = scalar_len(&vals[i]);
                end += l as u32;
                let je = jentry(scalar_jentry_type(&vals[i]), l, end);
                put_u32(&mut buf, 4 + 4 * (n + i), je);
                if vals[i].kind == 3 {
                    for j in 0..VL {
                        if j < vals[i].slen {
                            buf.0[pos + j] = vals[i].sbytes[j];
                        }
                    }
                    pos += vals[i].slen;
                }
            }
        }
        buf
    }

    /// Rust half of the shared hash seam — literally pg_seam_hash_bytes
    /// (FNV-1a 32) minus the skew term. Installed over hashfn::hash_bytes
    /// via -Z stubbing on the extract cells.
    fn stub_hash_bytes(k: &[u8]) -> u32 {
        let mut h: u32 = 0x811c_9dc5;
        let mut i = 0;
        while i < k.len() {
            h ^= k[i] as u32;
            h = h.wrapping_mul(0x0100_0193);
            i += 1;
        }
        h
    }

    /// Compare the shipped gin_extract_jsonb PgVec<Datum> (text-key images
    /// in the proof heap) against C's entries (pool images) byte-wise; the
    /// C side is read through pointee accessors (provenance lesson).
    fn check_extract_jsonb(img: &Img, expected_max: usize) {
        unsafe { pgg_reset() };
        let ctx = token_ctx();
        let r = adt_jsonb::gin::gin_extract_jsonb(ctx.mcx(), &img.0[..]);
        let entries = match r {
            Ok(v) => v,
            Err(e) => {
                core::mem::forget(e);
                panic!("gin_extract_jsonb errored");
            }
        };
        let cn = unsafe { pgg_extract_jsonb(img.0.as_ptr()) };
        assert_no_abort();
        assert!(entries.len() == cn as usize);
        assert!(entries.len() <= expected_max);
        for i in 0..8 {
            if i < entries.len() {
                let p = entries[i].as_usize() as *const u8;
                // SAFETY: entry datums point at 4-byte-header text images
                // the shipped code just built in the (stubbed) proof heap.
                let rlen = unsafe { types_tuple::varatt::varsize_4b(p) };
                let clen = unsafe { pgg_entry_len(i as c_int) } as usize;
                assert!(rlen == clen);
                for off in 0..12 {
                    if off < rlen {
                        let rb = unsafe { *p.add(off) };
                        let cb = unsafe { pgg_entry_byte(i as c_int, off as c_int) } as u8;
                        assert!(rb == cb);
                    }
                }
            }
        }
        core::mem::forget(entries);
    }

    /// path_ops extract: entries are uint32 path-hash datums (scalar class).
    fn check_extract_jsonb_path(img: &Img, expected_max: usize) {
        unsafe { pgg_reset() };
        let ctx = token_ctx();
        let r = adt_jsonb::gin::gin_extract_jsonb_path(ctx.mcx(), &img.0[..]);
        let entries = match r {
            Ok(v) => v,
            Err(e) => {
                core::mem::forget(e);
                panic!("gin_extract_jsonb_path errored");
            }
        };
        let cn = unsafe { pgg_extract_jsonb_path(img.0.as_ptr()) };
        assert_no_abort();
        assert!(entries.len() == cn as usize);
        assert!(entries.len() <= expected_max);
        for i in 0..8 {
            if i < entries.len() {
                let rh = entries[i].as_usize() as u32;
                let ch = unsafe { pgg_entry_u32(i as c_int) };
                assert!(rh == ch);
            }
        }
        core::mem::forget(entries);
    }

    /// per-cell string-length pins (offsets stay concrete — result-image
    /// law mitigation)
    const SLENS: [usize; MAXN] = [1, 2, 1];

    macro_rules! extract_cell {
        ($name:ident[$unwind:literal]: $body:block) => {
            #[kani::proof]
            #[kani::unwind($unwind)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::vec_with_capacity_in, mcx_stubs::stub_vec_with_capacity_in)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(hashfn::hash_bytes, stub_hash_bytes)]
            fn $name() $body
        };
    }

    extract_cell!(eq_extract_jsonb_arr_n0[5]: {
        let es: [Scalar; MAXN] = [any_scalar_nb(), any_scalar_nb(), any_scalar_nb()];
        let img = build_array(&es, 0);
        check_extract_jsonb(&img, 0);
    });

    extract_cell!(eq_extract_jsonb_arr_n1[10]: {
        let es: [Scalar; MAXN] = [any_scalar_len(SLENS[0]), any_scalar_nb(), any_scalar_nb()];
        let img = build_array(&es, 1);
        check_extract_jsonb(&img, 1);
    });

    extract_cell!(eq_extract_jsonb_arr_n2[12]: {
        let es: [Scalar; MAXN] =
            [any_scalar_len(SLENS[0]), any_scalar_len(SLENS[1]), any_scalar_nb()];
        let img = build_array(&es, 2);
        check_extract_jsonb(&img, 2);
    });

    extract_cell!(eq_extract_jsonb_arr_n3[14]: {
        let es: [Scalar; MAXN] = [
            any_scalar_len(SLENS[0]),
            any_scalar_len(SLENS[1]),
            any_scalar_len(SLENS[2]),
        ];
        let img = build_array(&es, 3);
        check_extract_jsonb(&img, 3);
    });

    extract_cell!(eq_extract_jsonb_obj_n1[12]: {
        let ks: [Key; MAXN] = [any_key_len(2), any_key_len(0), any_key_len(0)];
        let vs: [Scalar; MAXN] = [any_scalar_len(1), any_scalar_nb(), any_scalar_nb()];
        let img = build_object(&ks, &vs, 1);
        check_extract_jsonb(&img, 2);
    });

    extract_cell!(eq_extract_jsonb_path_arr_n1[10]: {
        let es: [Scalar; MAXN] = [any_scalar_nb(), any_scalar_nb(), any_scalar_nb()];
        let img = build_array(&es, 1);
        check_extract_jsonb_path(&img, 1);
    });

    extract_cell!(eq_extract_jsonb_path_arr_n2[12]: {
        let es: [Scalar; MAXN] = [any_scalar_nb(), any_scalar_nb(), any_scalar_nb()];
        let img = build_array(&es, 2);
        check_extract_jsonb_path(&img, 2);
    });

    extract_cell!(eq_extract_jsonb_path_arr_n3[14]: {
        let es: [Scalar; MAXN] = [any_scalar_nb(), any_scalar_nb(), any_scalar_nb()];
        let img = build_array(&es, 3);
        check_extract_jsonb_path(&img, 3);
    });

    /// hash-seam-bearing cell: one string element, both sides through the
    /// shared FNV model.
    extract_cell!(eq_extract_jsonb_path_str_n1[10]: {
        let mut e0 = any_scalar_len(2);
        e0.kind = 3; // force string: the seam must be exercised
        let es: [Scalar; MAXN] = [e0, any_scalar_nb(), any_scalar_nb()];
        let img = build_array(&es, 1);
        check_extract_jsonb_path(&img, 1);
    });

    // =========================== controls =================================

    /// MUST FAIL (rig non-vacuity): C tree wired to a different entry index
    /// than the Rust ops. Default solver.
    #[kani::proof]
    #[kani::unwind(6)]
    fn control_jsp_wrong_entry() {
        unsafe { pgg_reset() };
        let nk = 2usize;
        let i0: u32 = kani::any();
        kani::assume((i0 as usize) < nk);
        let wrong = (i0 + 1) % nk as u32;
        let h = unsafe { pgg_mk_entry(wrong as c_int) };
        let ops = [e(i0)];
        let check = any_check_bool();
        let mut recheck = false;
        let r = adt_jsonb::gin::gin_consistent_jsonb(
            &check[..],
            JSP_EXISTS,
            nk,
            &mut recheck,
            &ops[..],
        );
        let mut c_recheck: c_int = 0;
        let mut err: c_int = 0;
        let c = unsafe {
            pgg_consistent_jsonb_h(
                check.as_ptr() as *const u8,
                JSP_EXISTS as u32,
                nk as c_int,
                h,
                &mut c_recheck,
                &mut err,
            )
        };
        assert!(err == 0);
        assert!(r == (c != 0)); // deliberately wrong wiring: must FAIL
    }

    /// MUST FAIL: the shared nullFlags/queryCategories bytes are
    /// load-bearing — C sees them inverted. Default solver.
    #[kani::proof]
    #[kani::unwind(6)]
    fn control_ginarray_null_skew() {
        let nk = any_nkeys(1);
        let check = any_check_bool();
        let cats = any_categories();
        let (r, _recheck) = run_ginarray_bin(&check, &cats, 2 /* contains */, nk);
        let mut skewed = [0i8; MAXK];
        for i in 0..MAXK {
            skewed[i] = 1 - cats[i];
        }
        let mut c_recheck: c_int = 0;
        let mut err: c_int = 0;
        let c = unsafe {
            pga_consistent(
                check.as_ptr() as *const u8,
                2,
                nk as c_int,
                skewed.as_ptr() as *const u8,
                &mut c_recheck,
                &mut err,
            )
        };
        assert!(err == 0);
        assert!(r == (c != 0)); // skewed seam: must FAIL
    }

    /// MUST FAIL: the shared hash-seam model is load-bearing — C side
    /// skewed by +1. Default solver.
    #[kani::proof]
    #[kani::unwind(10)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::vec_with_capacity_in, mcx_stubs::stub_vec_with_capacity_in)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(hashfn::hash_bytes, stub_hash_bytes)]
    fn control_extract_hash_seam_skew() {
        unsafe {
            pgg_reset();
            pgg_set_hash_skew(1);
        }
        let mut e0 = any_scalar_len(1);
        e0.kind = 3;
        let es: [Scalar; MAXN] = [e0, any_scalar_nb(), any_scalar_nb()];
        let img = build_array(&es, 1);
        let ctx = token_ctx();
        let r = adt_jsonb::gin::gin_extract_jsonb_path(ctx.mcx(), &img.0[..]);
        let entries = match r {
            Ok(v) => v,
            Err(e) => {
                core::mem::forget(e);
                panic!("gin_extract_jsonb_path errored");
            }
        };
        let cn = unsafe { pgg_extract_jsonb_path(img.0.as_ptr()) };
        assert!(entries.len() == cn as usize);
        let rh = entries[0].as_usize() as u32;
        let ch = unsafe { pgg_entry_u32(0) };
        core::mem::forget(entries);
        assert!(rh == ch); // skewed seam: must FAIL
    }
}
