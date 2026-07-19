use super::*;

#[test]
fn oids_are_unique() {
    let mut seen = std::collections::HashSet::new();
    for e in ENTRIES {
        assert!(seen.insert(e.oid), "duplicate OID {} ({})", e.oid, e.name);
    }
}

#[test]
fn every_cov_row_is_shape_consistent() {
    for e in ENTRIES {
        for c in e.cov {
            match (c.tier, &e.shape) {
                (Tier::AotQualCmp | Tier::StitchCmp | Tier::StitchSaop, Shape::Cmp(_)) => {}
                (Tier::JitArith | Tier::StitchArith | Tier::FoldAffine, Shape::Arith(_)) => {}
                (Tier::Fold, Shape::Fold(_)) => {}
                (t, s) => panic!("OID {} ({}): tier {:?} inconsistent with shape {:?}", e.oid, e.name, t, s),
            }
        }
    }
}

// The in-tree AOT qual comparator set: exactly the 90 OIDs execexpr's
// CmpOp::for_fn_oid admits (the legacy 30 int families + the 42 censusgaps
// additions: int24/int42/oid/float4/float8/float48/float84 + the 18
// ne-admission census-close date/timestamp/timestamptz aliases), each with
// the correct (width, pred). This is the golden set the execexpr conformance
// test binds `for_fn_oid` to.
#[test]
fn aot_qual_cmp_golden_set() {
    let golden: &[(Oid, CmpWidth, CmpPred)] = &[
        (65, I4, Eq), (144, I4, Ne), (66, I4, Lt), (149, I4, Le), (147, I4, Gt), (150, I4, Ge),
        (467, I8, Eq), (468, I8, Ne), (469, I8, Lt), (471, I8, Le), (470, I8, Gt), (472, I8, Ge),
        (63, I2, Eq), (145, I2, Ne), (64, I2, Lt), (148, I2, Le), (146, I2, Gt), (151, I2, Ge),
        (474, I84, Eq), (475, I84, Ne), (476, I84, Lt), (478, I84, Le), (477, I84, Gt), (479, I84, Ge),
        (852, I48, Eq), (853, I48, Ne), (854, I48, Lt), (856, I48, Le), (855, I48, Gt), (857, I48, Ge),
        (158, I24, Eq), (164, I24, Ne), (160, I24, Lt), (166, I24, Le), (162, I24, Gt), (168, I24, Ge),
        (159, I42, Eq), (165, I42, Ne), (161, I42, Lt), (167, I42, Le), (163, I42, Gt), (169, I42, Ge),
        (184, Oid, Eq), (185, Oid, Ne), (716, Oid, Lt), (717, Oid, Le), (1638, Oid, Gt), (1639, Oid, Ge),
        (287, F4, Eq), (288, F4, Ne), (289, F4, Lt), (290, F4, Le), (291, F4, Gt), (292, F4, Ge),
        (293, F8, Eq), (294, F8, Ne), (295, F8, Lt), (296, F8, Le), (297, F8, Gt), (298, F8, Ge),
        (299, F48, Eq), (300, F48, Ne), (301, F48, Lt), (302, F48, Le), (303, F48, Gt), (304, F48, Ge),
        (305, F84, Eq), (306, F84, Ne), (307, F84, Lt), (308, F84, Le), (309, F84, Gt), (310, F84, Ge),
        // ne-admission census close: date (int32 days), timestamp and
        // timestamptz (int64 usecs) are plain int compares incl. their
        // infinity sentinels (date.c / timestamp.c) — the same fact
        // laneexec's translate whitelist and the pgrcolumnar zone-qual
        // extraction already carried; registered so the central census
        // matches its consumers.
        (1086, I4, Eq), (1091, I4, Ne), (1087, I4, Lt), (1088, I4, Le), (1089, I4, Gt), (1090, I4, Ge),
        (2052, I8, Eq), (2053, I8, Ne), (2054, I8, Lt), (2055, I8, Le), (2057, I8, Gt), (2056, I8, Ge),
        (1152, I8, Eq), (1153, I8, Ne), (1154, I8, Lt), (1155, I8, Le), (1157, I8, Gt), (1156, I8, Ge),
    ];
    for &(oid, w, p) in golden {
        assert_eq!(aot_qual_cmp(oid), Some(CmpShape { width: w, pred: p }), "oid {oid}");
    }
    let in_tree_aot = ENTRIES.iter().filter(|e| aot_qual_cmp(e.oid).is_some()).count();
    assert_eq!(in_tree_aot, golden.len(), "AOT in-tree set drifted from the golden 90");
}

// The stitch-saop set: exactly the non-float comparator rows (the lanestitch
// SaopQ stencil admits every whitelisted non-float comparator; float element
// compares refuse — no NaN-exact scalar cond). The SVE2 MATCH sub-tier
// (lane-v2-sve2tier) further gates at admission time on Eq relation +
// u16-domain elements + the register budget, inside the same coverage row.
#[test]
fn stitch_saop_covers_exactly_the_nonfloat_comparators() {
    for e in ENTRIES {
        let saop = e.tier(Tier::StitchSaop).is_some_and(|c| c.is_intree());
        match e.shape {
            Shape::Cmp(CmpShape { width, .. }) => {
                let is_float = matches!(width, F4 | F8 | F48 | F84);
                assert_eq!(saop, !is_float, "OID {} ({}): stitch-saop drift", e.oid, e.name);
                if saop {
                    let cov = e.tier(Tier::StitchSaop).unwrap();
                    assert_eq!(cov.guard, GuardTier::NonErroring);
                    assert_eq!(cov.coll, CollGate::NotApplicable);
                }
            }
            _ => assert!(!saop, "OID {} ({}): stitch-saop on a non-comparator", e.oid, e.name),
        }
    }
    let n = ENTRIES
        .iter()
        .filter(|e| e.tier(Tier::StitchSaop).is_some_and(|c| c.is_intree()))
        .count();
    assert_eq!(n, 66, "stitch-saop set drifted from the 66 non-float comparators");
}

#[test]
fn jit_arith_golden_set() {
    let golden: &[(Oid, ArithWidth, ArithKind)] = &[
        (177, ArithWidth::W4, ArithKind::Add),
        (181, ArithWidth::W4, ArithKind::Sub),
        (141, ArithWidth::W4, ArithKind::Mul),
        (463, ArithWidth::W8, ArithKind::Add),
        (464, ArithWidth::W8, ArithKind::Sub),
        (465, ArithWidth::W8, ArithKind::Mul),
        // censusgaps: the int2/int4 mixed family the JIT now inlines.
        (178, ArithWidth::W24, ArithKind::Add),
        (179, ArithWidth::W24, ArithKind::Add),
        (182, ArithWidth::W24, ArithKind::Sub),
        (183, ArithWidth::W24, ArithKind::Sub),
        (170, ArithWidth::W24, ArithKind::Mul),
        (171, ArithWidth::W24, ArithKind::Mul),
        (172, ArithWidth::W24, ArithKind::Div),
    ];
    for &(oid, w, op) in golden {
        assert_eq!(jit_arith(oid), Some(ArithShape { width: w, op }), "oid {oid}");
    }
    let n = ENTRIES.iter().filter(|e| jit_arith(e.oid).is_some()).count();
    assert_eq!(n, golden.len());
}

#[test]
fn fold_in_tree_golden_set() {
    // Base set + int8_avg_accum (lane-v2-int8fold) + tier 2 (lane-v2-foldcov)
    // + text/bpchar MIN/MAX (lane-v2-textfold) — the third-train landings
    // flipped the pending rows to in-tree (migration recipe step 1) — + the
    // fold-trans float tier (lane-v2-lanefold-trans, knob-gated default OFF:
    // float4pl/float8pl sum, float4_accum/float8_accum avg/var/stddev) + the
    // fold-trans increment 2 (lane aggseq-fold2, same knob:
    // numeric_avg_accum 2858, float8_regr_accum 2806,
    // int8inc_float8_float8 2805).
    let golden: &[Oid] = &[
        1219, 2804, 1840, 1841, 1962, 1963, 2746, 768, 769, 770, 771, 1236, 1237, 1138, 1139,
        2036, 2035, 1196, 1195, 209, 211, 223, 224, 2515, 2516, 1892, 1893, 1898, 1899, 1904,
        1905, 458, 459, 1063, 1064, 204, 218, 208, 222, 2858, 2806, 2805,
    ];
    for &oid in golden {
        assert!(fold_desc(oid).is_some(), "fold oid {oid} missing in-tree");
    }
    let n = ENTRIES.iter().filter(|e| fold_desc(e.oid).is_some()).count();
    assert_eq!(n, golden.len(), "in-tree fold set drifted");
}

#[test]
fn drift_findings() {
    // censusgaps closed all three cross-tier drift classes:
    // - stencil-but-no-census (was 42): every stitch-vocabulary comparator now
    //   has the in-tree AOT qual tier.
    let stencil = ENTRIES.iter().filter(|e| drift_of(e).contains(&Drift::StencilNoCensus)).count();
    assert_eq!(stencil, 0);
    // - fold-affine-but-no-jit (was 7): the int24/int42 mixed arith family is
    //   now JIT-inlined too.
    let fa = ENTRIES.iter().filter(|e| drift_of(e).contains(&Drift::FoldAffineNoJit)).count();
    assert_eq!(fa, 0);
    // - jit-but-no-fold-affine (was 3): int8 pl/mi/mul carry a documented
    //   FoldAffine REFUSAL (i128 interval proofs missing), not silent drift.
    let jf = ENTRIES.iter().filter(|e| drift_of(e).contains(&Drift::JitNoFoldAffine)).count();
    assert_eq!(jf, 0);
    let refused: Vec<_> = ENTRIES
        .iter()
        .filter(|e| e.cov.iter().any(|c| c.is_refused()))
        .map(|e| e.oid)
        .collect();
    assert_eq!(refused, vec![463, 464, 465], "documented refusals: int8 pl/mi/mul fold-affine");
}

// WS-AA wave-7 fusion inc-0: the RowOp tier exists with ZERO coverage rows —
// the shipped consumer (the trigger-DML row chain) carries no OID-backed
// steps, and the conformance law says rows land in the same edit as their
// consumer arm. This pin makes silently adding a row without a consumer (or
// vice versa) a test failure: whoever admits the first OID-backed chain step
// updates the consumer, the registry row, AND this count together (and adds
// the report column then).
#[test]
fn rowop_rows_ship_with_their_consumer() {
    let n = ENTRIES
        .iter()
        .filter(|e| e.tier(Tier::RowOp).is_some())
        .count();
    assert_eq!(n, 0, "RowOp coverage rows appeared; ship them with the consumer arm");
}

// The coverage report is a checked-in artifact regenerated from the registry.
// If this fails, run with LANEREG_WRITE_REPORT=1 to refresh the doc.
#[test]
fn coverage_report_matches_checked_in_doc() {
    let doc = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../../crates/backend/executor/lanereg/lane-batchreg-coverage.md");
    let generated = coverage_report();
    if std::env::var_os("LANEREG_WRITE_REPORT").is_some() {
        std::fs::write(doc, &generated).unwrap();
        return;
    }
    match std::fs::read_to_string(doc) {
        Ok(on_disk) => assert_eq!(
            generated, on_disk,
            "coverage doc stale; regenerate with LANEREG_WRITE_REPORT=1"
        ),
        Err(_) => panic!("missing {doc}; regenerate with LANEREG_WRITE_REPORT=1"),
    }
}
