//! Verifies that `init::init_all()` registers every `enum.c` builtin into
//! the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch
//! resolves the enum I/O, comparison and programming-support functions. The
//! catalog-touching bodies (`enum_in`/`out`/etc.) cannot be exercised without a
//! user-defined enum type (a fresh initdb has none, and `CREATE TYPE ... AS
//! ENUM` walls on the unported `DefineEnum`/XLog driver path), so this asserts
//! the registration wiring this crate's `init_seams` performs.

use ::fmgr_core::{fmgr_isbuiltin, native_builtin};

/// (oid, name, nargs, strict) transcribed from `fmgrtab.c`.
const ENUM_BUILTINS: &[(u32, &str, i16, bool)] = &[
    (3506, "enum_in", 2, true),
    (3507, "enum_out", 1, true),
    (3532, "enum_recv", 2, true),
    (3533, "enum_send", 1, true),
    (3508, "enum_eq", 2, true),
    (3509, "enum_ne", 2, true),
    (3510, "enum_lt", 2, true),
    (3511, "enum_gt", 2, true),
    (3512, "enum_le", 2, true),
    (3513, "enum_ge", 2, true),
    (3514, "enum_cmp", 2, true),
    (3524, "enum_smaller", 2, true),
    (3525, "enum_larger", 2, true),
    (3528, "enum_first", 1, false),
    (3529, "enum_last", 1, false),
    (3530, "enum_range_bounds", 2, false),
    (3531, "enum_range_all", 1, false),
];

#[test]
fn enum_builtins_resolve_by_oid_after_init() {
    init::init_all();

    for &(oid, name, nargs, strict) in ENUM_BUILTINS {
        let entry = fmgr_isbuiltin(oid)
            .unwrap_or_else(|| panic!("enum builtin oid {oid} ({name}) not registered"));
        assert_eq!(entry.name, name, "name mismatch for oid {oid}");
        assert_eq!(entry.nargs, nargs, "nargs mismatch for {name} (oid {oid})");
        assert_eq!(entry.strict, strict, "strict mismatch for {name} (oid {oid})");
        assert!(!entry.retset, "{name} (oid {oid}) must not be retset");
        // enum.c builtins have been migrated to the Result-native fmgr shape
        // (panic→Result Phase 2): the metadata row carries `func: None`, and the
        // callable lives in the `NATIVE` overlay. Accept either a legacy
        // `PGFunction` or a registered Result-native body.
        assert!(
            entry.func.is_some() || native_builtin(oid).is_some(),
            "{name} (oid {oid}) has no callable (neither legacy nor native)"
        );
    }
}
