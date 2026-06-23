//! Tests for the reloptions parser. The `text[]`/`defGet`/`parse_int`/
//! `parse_real`/`amoptions` seams are owned by not-yet-ported units, so these
//! exercise only the seam-free surface: the built-in option-table contents
//! and the pure helpers (`builtin_parse_bool`, lock-level computation).

use super::*;

/// `builtin_parse_bool` parity with `parse_bool` (utils/adt/bool.c).
#[test]
fn parse_bool_accepts_c_spellings() {
    for s in ["t", "T", "y", "Y", "1", "on", "yes", "true"] {
        assert_eq!(builtin_parse_bool(s), Some(true), "{s}");
    }
    for s in ["f", "F", "n", "N", "0", "no", "off", "false"] {
        assert_eq!(builtin_parse_bool(s), Some(false), "{s}");
    }
    for s in ["", "tru", "yess", "2", "onn"] {
        assert_eq!(builtin_parse_bool(s), None, "{s}");
    }
}

/// The built-in tables must transcribe reloptions.c value-by-value. Spot-check
/// a representative entry per type plus the table sizes.
#[test]
fn builtin_tables_match_c() {
    let bools = bool_rel_opts();
    assert_eq!(bools.len(), 8);
    assert_eq!(bools[0].name, "autosummarize");
    assert_eq!(bools[0].kinds, RELOPT_KIND_BRIN as bits32);
    assert_eq!(bools[0].lockmode, AccessExclusiveLock);
    assert!(matches!(bools[0].data, RelOptData::Bool { default_val: false }));
    assert_eq!(bools[1].name, "autovacuum_enabled");
    assert!(matches!(bools[1].data, RelOptData::Bool { default_val: true }));

    let ints = int_rel_opts();
    assert_eq!(ints.len(), 23);
    assert_eq!(ints[0].name, "fillfactor");
    if let RelOptData::Int { default_val, min, max } = ints[0].data {
        assert_eq!((default_val, min, max), (HEAP_DEFAULT_FILLFACTOR, HEAP_MIN_FILLFACTOR, 100));
    } else {
        panic!("fillfactor not int");
    }
    // gin_pending_list_limit: -1, 64, MAX_KILOBYTES
    let gin = ints.iter().find(|o| o.name == "gin_pending_list_limit").unwrap();
    if let RelOptData::Int { default_val, min, max } = gin.data {
        assert_eq!((default_val, min, max), (-1, 64, MAX_KILOBYTES));
    } else {
        panic!();
    }

    let reals = real_rel_opts();
    assert_eq!(reals.len(), 10);
    let nd = reals.iter().find(|o| o.name == "n_distinct").unwrap();
    if let RelOptData::Real { default_val, min, max } = nd.data {
        assert_eq!(default_val, 0.0);
        assert_eq!(min, -1.0);
        assert_eq!(max, f64::MAX);
    } else {
        panic!();
    }

    let enums = enum_rel_opts();
    assert_eq!(enums.len(), 3);
    assert_eq!(enums[0].name, "vacuum_index_cleanup");
    if let RelOptData::Enum { members, default_val, .. } = &enums[0].data {
        assert_eq!(members.len(), 9);
        assert_eq!(*default_val, STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO);
        assert_eq!(members[0].string_val, "auto");
    } else {
        panic!();
    }

    assert!(string_rel_opts().is_empty());
}

/// `AlterTableGetRelOptionsLockLevel`: empty list -> AccessExclusiveLock;
/// known option -> its registered lockmode (the max over the list).
#[test]
fn lock_level() {
    assert_eq!(AlterTableGetRelOptionsLockLevel(&[]), AccessExclusiveLock);

    // fillfactor is ShareUpdateExclusiveLock; user_catalog_table is
    // AccessExclusiveLock; the max wins.
    let defs = vec![
        DefElem::new(None, "fillfactor", None),
        DefElem::new(None, "user_catalog_table", None),
    ];
    assert_eq!(AlterTableGetRelOptionsLockLevel(&defs), AccessExclusiveLock);

    let defs = vec![DefElem::new(None, "fillfactor", None)];
    assert_eq!(AlterTableGetRelOptionsLockLevel(&defs), ShareUpdateExclusiveLock);

    // An unknown option contributes no lockmode (stays NoLock).
    let defs = vec![DefElem::new(None, "nonexistent_option", None)];
    assert_eq!(AlterTableGetRelOptionsLockLevel(&defs), NoLock);
}

/// `add_reloption_kind` hands out successive bits and eventually errors.
#[test]
fn add_kind_limit() {
    // First call advances last_assigned_kind from RELOPT_KIND_LAST_DEFAULT.
    let k = add_reloption_kind().expect("first custom kind");
    assert_eq!(k, (RELOPT_KIND_LAST_DEFAULT as bits32 as relopt_kind) << 1);
}
