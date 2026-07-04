use crate::builtins::SLOTFUNCS_BUILTINS;

#[test]
fn builtin_table_shape() {
    assert_eq!(SLOTFUNCS_BUILTINS.len(), 3);
    let srf = SLOTFUNCS_BUILTINS.iter().find(|b| b.foid == 3781).unwrap();
    assert!(srf.retset && !srf.strict && srf.nargs == 0);
    let create = SLOTFUNCS_BUILTINS.iter().find(|b| b.foid == 3779).unwrap();
    assert!(create.strict && !create.retset && create.nargs == 3);
    let drop = SLOTFUNCS_BUILTINS.iter().find(|b| b.foid == 3780).unwrap();
    assert!(drop.strict && !drop.retset && drop.nargs == 1);
}

#[test]
fn xseg_conversions() {
    // 16MB segments: 1024MB == 64 segments.
    assert_eq!(crate::convert_to_xsegs(1024, 16 * 1024 * 1024), 64);
    assert_eq!(crate::convert_to_xsegs(0, 16 * 1024 * 1024), 0);
}
