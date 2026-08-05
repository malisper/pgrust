use crate::builtins::SLOTFUNCS_BUILTINS;

#[test]
fn builtin_table_shape() {
    assert_eq!(SLOTFUNCS_BUILTINS.len(), 11);
    let srf = SLOTFUNCS_BUILTINS.iter().find(|b| b.foid == 3781).unwrap();
    assert!(srf.retset && !srf.strict && srf.nargs == 0);
    let create_logical = SLOTFUNCS_BUILTINS.iter().find(|b| b.foid == 3786).unwrap();
    assert!(create_logical.strict && !create_logical.retset && create_logical.nargs == 5);
    let create = SLOTFUNCS_BUILTINS.iter().find(|b| b.foid == 3779).unwrap();
    assert!(create.strict && !create.retset && create.nargs == 3);
    let drop = SLOTFUNCS_BUILTINS.iter().find(|b| b.foid == 3780).unwrap();
    assert!(drop.strict && !drop.retset && drop.nargs == 1);

    let advance = SLOTFUNCS_BUILTINS.iter().find(|b| b.foid == 3878).unwrap();
    assert!(advance.strict && !advance.retset && advance.nargs == 2);

    for (foid, nargs) in [(4220, 3), (4221, 2), (4222, 4), (4223, 3), (4224, 2)] {
        let b = SLOTFUNCS_BUILTINS.iter().find(|b| b.foid == foid).unwrap();
        assert!(b.strict && !b.retset && b.nargs == nargs, "foid {foid}");
    }

    let sync = SLOTFUNCS_BUILTINS.iter().find(|b| b.foid == 6344).unwrap();
    assert!(sync.strict && !sync.retset && sync.nargs == 0);
}

#[test]
fn xseg_conversions() {
    // 16MB segments: 1024MB == 64 segments.
    assert_eq!(crate::convert_to_xsegs(1024, 16 * 1024 * 1024), 64);
    assert_eq!(crate::convert_to_xsegs(0, 16 * 1024 * 1024), 0);
}

// KeepLogSeg's unsummarized arm (xlog.c:8033): the kept segment moves back to
// the oldest unsummarized LSN's segment, never forward.
#[test]
fn unsummarized_keep_arm() {
    const SEG: i32 = 16 * 1024 * 1024;
    let lsn_in_seg = |seg: u64| seg * SEG as u64 + 0x1234;

    // WAL summarization off (InvalidXLogRecPtr): segno unchanged.
    assert_eq!(crate::apply_unsummarized_keep(5, 0, SEG), 5);
    // Unsummarized WAL older than the current keep: pull segno back.
    assert_eq!(crate::apply_unsummarized_keep(5, lsn_in_seg(2), SEG), 2);
    // Unsummarized WAL newer than the current keep: no forward movement.
    assert_eq!(crate::apply_unsummarized_keep(2, lsn_in_seg(5), SEG), 2);
    // Equal segment: unchanged (C keeps only strictly-older).
    assert_eq!(crate::apply_unsummarized_keep(3, lsn_in_seg(3), SEG), 3);
}
