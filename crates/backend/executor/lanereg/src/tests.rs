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
                (Tier::AotQualCmp | Tier::StitchCmp, Shape::Cmp(_)) => {}
                (Tier::JitArith | Tier::StitchArith | Tier::FoldAffine, Shape::Arith(_)) => {}
                (Tier::Fold, Shape::Fold(_)) => {}
                (t, s) => panic!("OID {} ({}): tier {:?} inconsistent with shape {:?}", e.oid, e.name, t, s),
            }
        }
    }
}

// The in-tree AOT qual comparator set: exactly the 30 int OIDs execexpr's
// CmpOp::for_fn_oid admits, each with the correct (width, pred). This is the
// golden set the execexpr conformance test binds `for_fn_oid` to.
#[test]
fn aot_qual_cmp_golden_set() {
    let golden: &[(Oid, CmpWidth, CmpPred)] = &[
        (65, I4, Eq), (144, I4, Ne), (66, I4, Lt), (149, I4, Le), (147, I4, Gt), (150, I4, Ge),
        (467, I8, Eq), (468, I8, Ne), (469, I8, Lt), (471, I8, Le), (470, I8, Gt), (472, I8, Ge),
        (63, I2, Eq), (145, I2, Ne), (64, I2, Lt), (148, I2, Le), (146, I2, Gt), (151, I2, Ge),
        (474, I84, Eq), (475, I84, Ne), (476, I84, Lt), (478, I84, Le), (477, I84, Gt), (479, I84, Ge),
        (852, I48, Eq), (853, I48, Ne), (854, I48, Lt), (856, I48, Le), (855, I48, Gt), (857, I48, Ge),
    ];
    for &(oid, w, p) in golden {
        assert_eq!(aot_qual_cmp(oid), Some(CmpShape { width: w, pred: p }), "oid {oid}");
    }
    let in_tree_aot = ENTRIES.iter().filter(|e| aot_qual_cmp(e.oid).is_some()).count();
    assert_eq!(in_tree_aot, golden.len(), "AOT in-tree set drifted from the golden 30");
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
    ];
    for &(oid, w, op) in golden {
        assert_eq!(jit_arith(oid), Some(ArithShape { width: w, op }), "oid {oid}");
    }
    let n = ENTRIES.iter().filter(|e| jit_arith(e.oid).is_some()).count();
    assert_eq!(n, golden.len());
}

#[test]
fn fold_in_tree_golden_set() {
    let golden: &[Oid] = &[
        1219, 2804, 1840, 1841, 1962, 1963, 768, 769, 770, 771, 1236, 1237, 1138, 1139, 2036,
        2035, 1196, 1195,
    ];
    for &oid in golden {
        assert!(fold_desc(oid).is_some(), "fold oid {oid} missing in-tree");
    }
    let n = ENTRIES.iter().filter(|e| fold_desc(e.oid).is_some()).count();
    assert_eq!(n, golden.len(), "in-tree fold set drifted");
}

#[test]
fn drift_findings() {
    // stencil-but-no-census: int24(6)+int42(6)+oid(6)+float4/8/48/84(24) = 42.
    let stencil = ENTRIES.iter().filter(|e| drift_of(e).contains(&Drift::StencilNoCensus)).count();
    assert_eq!(stencil, 42);
    // fold-affine-but-no-jit: int24/int42 pl/mi/mul + int24div = 7.
    let fa = ENTRIES.iter().filter(|e| drift_of(e).contains(&Drift::FoldAffineNoJit)).count();
    assert_eq!(fa, 7);
    // jit-but-no-fold-affine: int8 pl/mi/mul = 3.
    let jf = ENTRIES.iter().filter(|e| drift_of(e).contains(&Drift::JitNoFoldAffine)).count();
    assert_eq!(jf, 3);
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
