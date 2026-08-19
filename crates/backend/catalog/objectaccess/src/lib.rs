#![allow(non_snake_case, non_upper_case_globals, non_camel_case_types)]

use std::cell::Cell;

use types_core::{Oid, NAMESPACE_RELATION_ID, PROCEDURE_RELATION_ID, RELATION_RELATION_ID};
use types_error::PgResult;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectAccessType {
    OAT_POST_CREATE,
    OAT_DROP,
    OAT_POST_ALTER,
    OAT_NAMESPACE_SEARCH,
    OAT_FUNCTION_EXECUTE,
    OAT_TRUNCATE,
}
pub use ObjectAccessType::*;

#[derive(Clone, Copy, Debug, Default)]
pub struct ObjectAccessPostCreate {
    pub is_internal: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ObjectAccessDrop {
    pub dropflags: i32,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ObjectAccessPostAlter {
    pub auxiliary_id: Oid,
    pub is_internal: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct ObjectAccessNamespaceSearch {
    pub ereport_on_violation: bool,
    pub result: bool,
}

pub enum ObjectAccessArg<'a> {
    None,
    PostCreate(&'a ObjectAccessPostCreate),
    Drop(&'a ObjectAccessDrop),
    PostAlter(&'a ObjectAccessPostAlter),
    NamespaceSearch(&'a mut ObjectAccessNamespaceSearch),
}

pub type ObjectAccessHook =
    fn(ObjectAccessType, Oid, Oid, i32, &mut ObjectAccessArg<'_>) -> PgResult<()>;
pub type ObjectAccessHookStr =
    fn(ObjectAccessType, Oid, &str, i32, &mut ObjectAccessArg<'_>) -> PgResult<()>;

thread_local! {
    static OBJECT_ACCESS_HOOK: Cell<Option<ObjectAccessHook>> = const { Cell::new(None) };
    static OBJECT_ACCESS_HOOK_STR: Cell<Option<ObjectAccessHookStr>> = const { Cell::new(None) };
}

pub fn object_access_hook() -> Option<ObjectAccessHook> {
    OBJECT_ACCESS_HOOK.with(Cell::get)
}

pub fn set_object_access_hook(hook: Option<ObjectAccessHook>) -> Option<ObjectAccessHook> {
    OBJECT_ACCESS_HOOK.with(|c| c.replace(hook))
}

pub fn object_access_hook_str() -> Option<ObjectAccessHookStr> {
    OBJECT_ACCESS_HOOK_STR.with(Cell::get)
}

pub fn set_object_access_hook_str(
    hook: Option<ObjectAccessHookStr>,
) -> Option<ObjectAccessHookStr> {
    OBJECT_ACCESS_HOOK_STR.with(|c| c.replace(hook))
}

pub fn RunObjectPostCreateHook(
    classId: Oid,
    objectId: Oid,
    subId: i32,
    is_internal: bool,
) -> PgResult<()> {
    let hook = object_access_hook().expect("caller checks object_access_hook");
    let pc_arg = ObjectAccessPostCreate { is_internal };
    hook(OAT_POST_CREATE, classId, objectId, subId, &mut ObjectAccessArg::PostCreate(&pc_arg))
}

pub fn RunObjectDropHook(classId: Oid, objectId: Oid, subId: i32, dropflags: i32) -> PgResult<()> {
    let hook = object_access_hook().expect("caller checks object_access_hook");
    let drop_arg = ObjectAccessDrop { dropflags };
    hook(OAT_DROP, classId, objectId, subId, &mut ObjectAccessArg::Drop(&drop_arg))
}

pub fn RunObjectTruncateHook(objectId: Oid) -> PgResult<()> {
    let hook = object_access_hook().expect("caller checks object_access_hook");
    hook(OAT_TRUNCATE, RELATION_RELATION_ID, objectId, 0, &mut ObjectAccessArg::None)
}

pub fn RunObjectPostAlterHook(
    classId: Oid,
    objectId: Oid,
    subId: i32,
    auxiliaryId: Oid,
    is_internal: bool,
) -> PgResult<()> {
    let hook = object_access_hook().expect("caller checks object_access_hook");
    let pa_arg = ObjectAccessPostAlter { auxiliary_id: auxiliaryId, is_internal };
    hook(OAT_POST_ALTER, classId, objectId, subId, &mut ObjectAccessArg::PostAlter(&pa_arg))
}

pub fn RunNamespaceSearchHook(objectId: Oid, ereport_on_violation: bool) -> PgResult<bool> {
    let hook = object_access_hook().expect("caller checks object_access_hook");
    let mut ns_arg = ObjectAccessNamespaceSearch { ereport_on_violation, result: true };
    hook(
        OAT_NAMESPACE_SEARCH,
        NAMESPACE_RELATION_ID,
        objectId,
        0,
        &mut ObjectAccessArg::NamespaceSearch(&mut ns_arg),
    )?;
    Ok(ns_arg.result)
}

pub fn RunFunctionExecuteHook(objectId: Oid) -> PgResult<()> {
    let hook = object_access_hook().expect("caller checks object_access_hook");
    hook(OAT_FUNCTION_EXECUTE, PROCEDURE_RELATION_ID, objectId, 0, &mut ObjectAccessArg::None)
}

pub fn RunObjectPostCreateHookStr(
    classId: Oid,
    objectName: &str,
    subId: i32,
    is_internal: bool,
) -> PgResult<()> {
    let hook = object_access_hook_str().expect("caller checks object_access_hook_str");
    let pc_arg = ObjectAccessPostCreate { is_internal };
    hook(OAT_POST_CREATE, classId, objectName, subId, &mut ObjectAccessArg::PostCreate(&pc_arg))
}

pub fn RunObjectDropHookStr(
    classId: Oid,
    objectName: &str,
    subId: i32,
    dropflags: i32,
) -> PgResult<()> {
    let hook = object_access_hook_str().expect("caller checks object_access_hook_str");
    let drop_arg = ObjectAccessDrop { dropflags };
    hook(OAT_DROP, classId, objectName, subId, &mut ObjectAccessArg::Drop(&drop_arg))
}

pub fn RunObjectTruncateHookStr(objectName: &str) -> PgResult<()> {
    let hook = object_access_hook_str().expect("caller checks object_access_hook_str");
    hook(OAT_TRUNCATE, RELATION_RELATION_ID, objectName, 0, &mut ObjectAccessArg::None)
}

pub fn RunObjectPostAlterHookStr(
    classId: Oid,
    objectName: &str,
    subId: i32,
    auxiliaryId: Oid,
    is_internal: bool,
) -> PgResult<()> {
    let hook = object_access_hook_str().expect("caller checks object_access_hook_str");
    let pa_arg = ObjectAccessPostAlter { auxiliary_id: auxiliaryId, is_internal };
    hook(OAT_POST_ALTER, classId, objectName, subId, &mut ObjectAccessArg::PostAlter(&pa_arg))
}

pub fn RunNamespaceSearchHookStr(objectName: &str, ereport_on_violation: bool) -> PgResult<bool> {
    let hook = object_access_hook_str().expect("caller checks object_access_hook_str");
    let mut ns_arg = ObjectAccessNamespaceSearch { ereport_on_violation, result: true };
    hook(
        OAT_NAMESPACE_SEARCH,
        NAMESPACE_RELATION_ID,
        objectName,
        0,
        &mut ObjectAccessArg::NamespaceSearch(&mut ns_arg),
    )?;
    Ok(ns_arg.result)
}

pub fn RunFunctionExecuteHookStr(objectName: &str) -> PgResult<()> {
    let hook = object_access_hook_str().expect("caller checks object_access_hook_str");
    hook(OAT_FUNCTION_EXECUTE, PROCEDURE_RELATION_ID, objectName, 0, &mut ObjectAccessArg::None)
}

// objectaccess.h's Invoke* macros: no-ops unless a hook is installed.

pub fn InvokeObjectPostCreateHook(classId: Oid, objectId: Oid, subId: i32) -> PgResult<()> {
    InvokeObjectPostCreateHookArg(classId, objectId, subId, false)
}

pub fn InvokeObjectPostCreateHookArg(
    classId: Oid,
    objectId: Oid,
    subId: i32,
    is_internal: bool,
) -> PgResult<()> {
    if object_access_hook().is_some() {
        RunObjectPostCreateHook(classId, objectId, subId, is_internal)?;
    }
    Ok(())
}

pub fn InvokeObjectDropHook(classId: Oid, objectId: Oid, subId: i32) -> PgResult<()> {
    InvokeObjectDropHookArg(classId, objectId, subId, 0)
}

pub fn InvokeObjectDropHookArg(
    classId: Oid,
    objectId: Oid,
    subId: i32,
    dropflags: i32,
) -> PgResult<()> {
    if object_access_hook().is_some() {
        RunObjectDropHook(classId, objectId, subId, dropflags)?;
    }
    Ok(())
}

pub fn InvokeObjectTruncateHook(objectId: Oid) -> PgResult<()> {
    if object_access_hook().is_some() {
        RunObjectTruncateHook(objectId)?;
    }
    Ok(())
}

pub fn InvokeObjectPostAlterHook(classId: Oid, objectId: Oid, subId: i32) -> PgResult<()> {
    InvokeObjectPostAlterHookArg(classId, objectId, subId, 0, false)
}

pub fn InvokeObjectPostAlterHookArg(
    classId: Oid,
    objectId: Oid,
    subId: i32,
    auxiliaryId: Oid,
    is_internal: bool,
) -> PgResult<()> {
    if object_access_hook().is_some() {
        RunObjectPostAlterHook(classId, objectId, subId, auxiliaryId, is_internal)?;
    }
    Ok(())
}

pub fn InvokeNamespaceSearchHook(objectId: Oid, ereport_on_violation: bool) -> PgResult<bool> {
    if object_access_hook().is_some() {
        RunNamespaceSearchHook(objectId, ereport_on_violation)
    } else {
        Ok(true)
    }
}

pub fn InvokeFunctionExecuteHook(objectId: Oid) -> PgResult<()> {
    if object_access_hook().is_some() {
        RunFunctionExecuteHook(objectId)?;
    }
    Ok(())
}

pub fn InvokeObjectPostCreateHookStr(classId: Oid, objectName: &str, subId: i32) -> PgResult<()> {
    InvokeObjectPostCreateHookStrArg(classId, objectName, subId, false)
}

pub fn InvokeObjectPostCreateHookStrArg(
    classId: Oid,
    objectName: &str,
    subId: i32,
    is_internal: bool,
) -> PgResult<()> {
    if object_access_hook_str().is_some() {
        RunObjectPostCreateHookStr(classId, objectName, subId, is_internal)?;
    }
    Ok(())
}

pub fn InvokeObjectDropHookStr(classId: Oid, objectName: &str, subId: i32) -> PgResult<()> {
    InvokeObjectDropHookStrArg(classId, objectName, subId, 0)
}

pub fn InvokeObjectDropHookStrArg(
    classId: Oid,
    objectName: &str,
    subId: i32,
    dropflags: i32,
) -> PgResult<()> {
    if object_access_hook_str().is_some() {
        RunObjectDropHookStr(classId, objectName, subId, dropflags)?;
    }
    Ok(())
}

pub fn InvokeObjectTruncateHookStr(objectName: &str) -> PgResult<()> {
    if object_access_hook_str().is_some() {
        RunObjectTruncateHookStr(objectName)?;
    }
    Ok(())
}

pub fn InvokeObjectPostAlterHookStr(classId: Oid, objectName: &str, subId: i32) -> PgResult<()> {
    InvokeObjectPostAlterHookStrArg(classId, objectName, subId, 0, false)
}

pub fn InvokeObjectPostAlterHookStrArg(
    classId: Oid,
    objectName: &str,
    subId: i32,
    auxiliaryId: Oid,
    is_internal: bool,
) -> PgResult<()> {
    if object_access_hook_str().is_some() {
        RunObjectPostAlterHookStr(classId, objectName, subId, auxiliaryId, is_internal)?;
    }
    Ok(())
}

pub fn InvokeNamespaceSearchHookStr(
    objectName: &str,
    ereport_on_violation: bool,
) -> PgResult<bool> {
    if object_access_hook_str().is_some() {
        RunNamespaceSearchHookStr(objectName, ereport_on_violation)
    } else {
        Ok(true)
    }
}

pub fn InvokeFunctionExecuteHookStr(objectName: &str) -> PgResult<()> {
    if object_access_hook_str().is_some() {
        RunFunctionExecuteHookStr(objectName)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
