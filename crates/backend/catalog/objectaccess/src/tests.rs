use super::*;
use std::cell::RefCell;

thread_local! {
    static CALLS: RefCell<Vec<(ObjectAccessType, Oid, Oid, i32, String)>> =
        const { RefCell::new(Vec::new()) };
    static STR_CALLS: RefCell<Vec<(ObjectAccessType, Oid, String, i32)>> =
        const { RefCell::new(Vec::new()) };
}

fn recording_hook(
    access: ObjectAccessType,
    class_id: Oid,
    object_id: Oid,
    sub_id: i32,
    arg: &mut ObjectAccessArg<'_>,
) -> PgResult<()> {
    let detail = match arg {
        ObjectAccessArg::None => "none".to_string(),
        ObjectAccessArg::PostCreate(pc) => format!("internal={}", pc.is_internal),
        ObjectAccessArg::Drop(d) => format!("dropflags={}", d.dropflags),
        ObjectAccessArg::PostAlter(pa) => {
            format!("aux={} internal={}", pa.auxiliary_id, pa.is_internal)
        }
        ObjectAccessArg::NamespaceSearch(ns) => {
            let s = format!("ereport={} result={}", ns.ereport_on_violation, ns.result);
            ns.result = false;
            s
        }
    };
    CALLS.with(|c| c.borrow_mut().push((access, class_id, object_id, sub_id, detail)));
    Ok(())
}

fn recording_hook_str(
    access: ObjectAccessType,
    class_id: Oid,
    object_name: &str,
    sub_id: i32,
    _arg: &mut ObjectAccessArg<'_>,
) -> PgResult<()> {
    STR_CALLS.with(|c| c.borrow_mut().push((access, class_id, object_name.to_string(), sub_id)));
    Ok(())
}

fn drain() -> Vec<(ObjectAccessType, Oid, Oid, i32, String)> {
    CALLS.with(|c| std::mem::take(&mut *c.borrow_mut()))
}

#[test]
fn invoke_without_hook_is_noop() {
    assert!(object_access_hook().is_none());
    InvokeObjectPostCreateHook(1259, 10, 0).unwrap();
    InvokeObjectDropHook(1259, 10, 0).unwrap();
    InvokeObjectPostAlterHook(1259, 10, 0).unwrap();
    InvokeObjectTruncateHook(10).unwrap();
    InvokeFunctionExecuteHook(10).unwrap();
    assert!(InvokeNamespaceSearchHook(11, true).unwrap());
    assert!(drain().is_empty());
}

#[test]
fn oid_hook_dispatch_matches_c_events() {
    let prev = set_object_access_hook(Some(recording_hook));
    assert!(prev.is_none());

    InvokeObjectPostCreateHook(1259, 100, 0).unwrap();
    InvokeObjectPostCreateHookArg(1259, 101, 2, true).unwrap();
    InvokeObjectDropHook(1247, 200, 0).unwrap();
    InvokeObjectDropHookArg(1247, 201, 0, 0x0001).unwrap();
    InvokeObjectPostAlterHook(1255, 300, 0).unwrap();
    InvokeObjectPostAlterHookArg(1255, 301, 1, 777, true).unwrap();
    InvokeObjectTruncateHook(400).unwrap();
    InvokeFunctionExecuteHook(500).unwrap();
    // Hook flips ns_arg.result to false; C returns ns_arg.result.
    assert!(!InvokeNamespaceSearchHook(600, true).unwrap());

    let calls = drain();
    assert_eq!(
        calls,
        vec![
            (OAT_POST_CREATE, 1259, 100, 0, "internal=false".to_string()),
            (OAT_POST_CREATE, 1259, 101, 2, "internal=true".to_string()),
            (OAT_DROP, 1247, 200, 0, "dropflags=0".to_string()),
            (OAT_DROP, 1247, 201, 0, "dropflags=1".to_string()),
            (OAT_POST_ALTER, 1255, 300, 0, "aux=0 internal=false".to_string()),
            (OAT_POST_ALTER, 1255, 301, 1, "aux=777 internal=true".to_string()),
            // OAT_TRUNCATE fixes classId = RelationRelationId, no arg.
            (OAT_TRUNCATE, 1259, 400, 0, "none".to_string()),
            // OAT_FUNCTION_EXECUTE fixes classId = ProcedureRelationId.
            (OAT_FUNCTION_EXECUTE, 1255, 500, 0, "none".to_string()),
            // OAT_NAMESPACE_SEARCH fixes classId = NamespaceRelationId,
            // ns_arg.result starts true.
            (OAT_NAMESPACE_SEARCH, 2615, 600, 0, "ereport=true result=true".to_string()),
        ]
    );

    set_object_access_hook(None);
    InvokeObjectPostCreateHook(1259, 999, 0).unwrap();
    assert!(drain().is_empty());
}

#[test]
fn str_hook_dispatch() {
    // The two hook slots are independent (C: object_access_hook vs _str).
    assert!(object_access_hook_str().is_none());
    InvokeObjectPostCreateHookStr(6243, "work_mem", 0).unwrap();
    assert!(STR_CALLS.with(|c| c.borrow().is_empty()));

    set_object_access_hook_str(Some(recording_hook_str));
    InvokeObjectPostCreateHookStr(6243, "work_mem", 0).unwrap();
    InvokeObjectDropHookStr(6243, "work_mem", 0).unwrap();
    assert!(InvokeNamespaceSearchHookStr("pg_catalog", false).unwrap());
    set_object_access_hook_str(None);

    let calls = STR_CALLS.with(|c| std::mem::take(&mut *c.borrow_mut()));
    assert_eq!(
        calls,
        vec![
            (OAT_POST_CREATE, 6243, "work_mem".to_string(), 0),
            (OAT_DROP, 6243, "work_mem".to_string(), 0),
            (OAT_NAMESPACE_SEARCH, 2615, "pg_catalog".to_string(), 0),
        ]
    );
}

#[test]
fn hook_error_propagates() {
    fn failing_hook(
        _: ObjectAccessType,
        _: Oid,
        _: Oid,
        _: i32,
        _: &mut ObjectAccessArg<'_>,
    ) -> PgResult<()> {
        Err(Box::new(types_error::PgError::error("permission denied".to_string())))
    }
    set_object_access_hook(Some(failing_hook));
    let err = InvokeObjectDropHook(1259, 42, 0).unwrap_err();
    assert_eq!(err.message, "permission denied");
    set_object_access_hook(None);
}
