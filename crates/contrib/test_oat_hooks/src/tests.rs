use super::*;
use objectaccess::{ObjectAccessDrop, ObjectAccessNamespaceSearch, ObjectAccessPostAlter, ObjectAccessPostCreate};

// accesstype_to_string (test_oat_hooks.c:434-473): the type word, the hex
// subId and the ACL_SET / ACL_ALTER_SYSTEM suffixes.
#[test]
fn accesstype_to_string_matches_c() {
    assert_eq!(accesstype_to_string(OAT_POST_CREATE, 0), "create (subId=0x0)");
    assert_eq!(accesstype_to_string(OAT_DROP, 0), "drop (subId=0x0)");
    assert_eq!(accesstype_to_string(OAT_POST_ALTER, 0), "alter (subId=0x0)");
    assert_eq!(accesstype_to_string(OAT_NAMESPACE_SEARCH, 0), "namespace search (subId=0x0)");
    assert_eq!(accesstype_to_string(OAT_FUNCTION_EXECUTE, 0), "execute (subId=0x0)");
    assert_eq!(accesstype_to_string(OAT_TRUNCATE, 0), "truncate (subId=0x0)");
    // psprintf("%s (subId=0x%x)"): a column number prints in hex.
    assert_eq!(accesstype_to_string(OAT_POST_ALTER, 12), "alter (subId=0xc)");
    assert_eq!(accesstype_to_string(OAT_POST_ALTER, ACL_SET as i32), "alter (subId=0x1000, set)");
    assert_eq!(
        accesstype_to_string(OAT_POST_ALTER, ACL_ALTER_SYSTEM as i32),
        "alter (subId=0x2000, alter system)"
    );
    assert_eq!(
        accesstype_to_string(OAT_POST_ALTER, (ACL_SET | ACL_ALTER_SYSTEM) as i32),
        "alter (subId=0x3000, all privileges)"
    );
}

// accesstype_arg_to_string (test_oat_hooks.c:475-539).
#[test]
fn accesstype_arg_to_string_matches_c() {
    assert_eq!(accesstype_arg_to_string(OAT_TRUNCATE, &ObjectAccessArg::None), "extra info null");
    assert_eq!(
        accesstype_arg_to_string(OAT_FUNCTION_EXECUTE, &ObjectAccessArg::None),
        "extra info null"
    );

    let pc = ObjectAccessPostCreate { is_internal: false };
    assert_eq!(accesstype_arg_to_string(OAT_POST_CREATE, &ObjectAccessArg::PostCreate(&pc)), "explicit");
    let pc = ObjectAccessPostCreate { is_internal: true };
    assert_eq!(accesstype_arg_to_string(OAT_POST_CREATE, &ObjectAccessArg::PostCreate(&pc)), "internal");

    let d = ObjectAccessDrop { dropflags: 0 };
    assert_eq!(accesstype_arg_to_string(OAT_DROP, &ObjectAccessArg::Drop(&d)), "");
    let d = ObjectAccessDrop { dropflags: PERFORM_DELETION_INTERNAL | PERFORM_DELETION_CONCURRENT_LOCK };
    assert_eq!(
        accesstype_arg_to_string(OAT_DROP, &ObjectAccessArg::Drop(&d)),
        "internal action,normal concurrent drop,"
    );
    let d = ObjectAccessDrop {
        dropflags: PERFORM_DELETION_CONCURRENTLY
            | PERFORM_DELETION_QUIETLY
            | PERFORM_DELETION_SKIP_ORIGINAL
            | PERFORM_DELETION_SKIP_EXTENSIONS,
    };
    assert_eq!(
        accesstype_arg_to_string(OAT_DROP, &ObjectAccessArg::Drop(&d)),
        "concurrent drop,suppress notices,keep original object,keep extensions,"
    );

    let pa = ObjectAccessPostAlter { auxiliary_id: InvalidOid, is_internal: false };
    assert_eq!(
        accesstype_arg_to_string(OAT_POST_ALTER, &ObjectAccessArg::PostAlter(&pa)),
        "explicit without auxiliary object"
    );
    let pa = ObjectAccessPostAlter { auxiliary_id: 16384, is_internal: true };
    assert_eq!(
        accesstype_arg_to_string(OAT_POST_ALTER, &ObjectAccessArg::PostAlter(&pa)),
        "internal with auxiliary object"
    );

    let mut ns = ObjectAccessNamespaceSearch { ereport_on_violation: true, result: true };
    assert_eq!(
        accesstype_arg_to_string(OAT_NAMESPACE_SEARCH, &ObjectAccessArg::NamespaceSearch(&mut ns)),
        "report on violation, allowed"
    );
    let mut ns = ObjectAccessNamespaceSearch { ereport_on_violation: false, result: false };
    assert_eq!(
        accesstype_arg_to_string(OAT_NAMESPACE_SEARCH, &ObjectAccessArg::NamespaceSearch(&mut ns)),
        "no report on violation, denied"
    );

    // An arg shape the access type does not own.
    let pc = ObjectAccessPostCreate { is_internal: false };
    assert_eq!(accesstype_arg_to_string(OAT_DROP, &ObjectAccessArg::PostCreate(&pc)), "unknown");
}

// The ten GUCs of _PG_init, in C's definition order, under the reserved prefix.
#[test]
fn guc_roster_matches_c() {
    let names: Vec<&str> = GUCS.iter().map(|g| g.0).collect();
    assert_eq!(
        names,
        [
            "test_oat_hooks.deny_set_variable",
            "test_oat_hooks.deny_alter_system",
            "test_oat_hooks.deny_object_access",
            "test_oat_hooks.deny_exec_perms",
            "test_oat_hooks.deny_utility_commands",
            "test_oat_hooks.audit",
            "test_oat_hooks.user_var1",
            "test_oat_hooks.user_var2",
            "test_oat_hooks.super_var1",
            "test_oat_hooks.super_var2",
        ]
    );
    for (name, _, context) in GUCS {
        assert!(name.starts_with("test_oat_hooks."));
        let userset = name.contains("user_var");
        assert_eq!(*context == types_guc::PGC_USERSET, userset, "{name}");
    }
}
