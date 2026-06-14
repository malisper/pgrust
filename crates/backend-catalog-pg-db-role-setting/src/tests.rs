//! In-crate decision-tree tests for `AlterSetting` / `DropSetting` /
//! `ApplySetting` / `process_db_role_settings`.
//!
//! Each test installs bare-`fn` seam fakes (the `seam!` slots take a `fn`
//! pointer, not a closure) backed by module-level `static mut` recorders,
//! configures the inputs the C reads (the `ExtractSetVariableArgs` result, the
//! found tuple + its decoded `setconfig`, and what `GUCArray{Reset,Add,Delete}`
//! return), drives the function, and asserts the recorded operation sequence
//! matches the C three-way branch. Tests run single-threaded
//! (`--test-threads=1`), so the shared recorders and set-once seam slots are
//! safe.

use super::*;

use mcx::MemoryContext;
use types_core::primitive::INVALID_OID;
use types_guc::guc::GucSource;
use types_parsenodes::VariableSetKind;

use backend_commands_functioncmds_seams as fseam;
use seam::{AlterLookup, SettingScan};

const SCAN: SettingScan = SettingScan(99);

/// What the fakes recorded, in order, for assertions. Mirrors the C branch
/// outcomes of `AlterSetting`.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Op {
    Reset(Vec<String>),
    Add(Option<Vec<String>>, String, String),
    Delete(Option<Vec<String>>, String),
    Update(Vec<String>),
    DeleteTuple,
    Insert(Oid, Oid, Vec<String>),
    Finish(Oid, Oid),
}

static mut VALUESTR: Option<String> = None;
static mut FOUND: Option<Option<Vec<String>>> = None;
static mut RESET_RESULT: Option<Vec<String>> = None;
static mut ADD_RESULT: Vec<String> = Vec::new();
static mut DELETE_RESULT: Option<Vec<String>> = None;
static mut OPS: Vec<Op> = Vec::new();

fn reset() {
    unsafe {
        VALUESTR = None;
        FOUND = None;
        RESET_RESULT = None;
        ADD_RESULT = Vec::new();
        DELETE_RESULT = None;
        OPS = Vec::new();
    }
}

fn ops() -> Vec<Op> {
    unsafe { OPS.clone() }
}

fn fake_extract(_setstmt: VariableSetStmt) -> PgResult<Option<String>> {
    Ok(unsafe { VALUESTR.clone() })
}

fn fake_alter_find(_databaseid: Oid, _roleid: Oid) -> PgResult<AlterLookup> {
    Ok(AlterLookup {
        scan: SCAN,
        tuple: unsafe { FOUND.clone() },
    })
}

fn fake_guc_array_reset(array: Vec<String>) -> PgResult<Option<Vec<String>>> {
    unsafe {
        OPS.push(Op::Reset(array));
        Ok(RESET_RESULT.clone())
    }
}

fn fake_guc_array_add(
    array: Option<Vec<String>>,
    name: String,
    value: String,
) -> PgResult<Vec<String>> {
    unsafe {
        OPS.push(Op::Add(array, name, value));
        Ok(ADD_RESULT.clone())
    }
}

fn fake_guc_array_delete(
    array: Option<Vec<String>>,
    name: String,
) -> PgResult<Option<Vec<String>>> {
    unsafe {
        OPS.push(Op::Delete(array, name));
        Ok(DELETE_RESULT.clone())
    }
}

fn fake_update_setconfig(scan: SettingScan, new_array: Vec<String>) -> PgResult<()> {
    assert_eq!(scan, SCAN, "update must run against the open scan handle");
    unsafe { OPS.push(Op::Update(new_array)) };
    Ok(())
}

fn fake_delete_found_tuple(scan: SettingScan) -> PgResult<()> {
    assert_eq!(scan, SCAN);
    unsafe { OPS.push(Op::DeleteTuple) };
    Ok(())
}

fn fake_insert_setting(
    scan: SettingScan,
    databaseid: Oid,
    roleid: Oid,
    array: Vec<String>,
) -> PgResult<()> {
    assert_eq!(scan, SCAN);
    unsafe { OPS.push(Op::Insert(databaseid, roleid, array)) };
    Ok(())
}

fn fake_alter_finish(
    _mcx: Mcx<'_>,
    scan: SettingScan,
    databaseid: Oid,
    roleid: Oid,
) -> PgResult<()> {
    assert_eq!(scan, SCAN);
    unsafe { OPS.push(Op::Finish(databaseid, roleid)) };
    Ok(())
}

fn fake_drop_settings(
    _has_databaseid: bool,
    _databaseid: Oid,
    _has_roleid: bool,
    _roleid: Oid,
) -> PgResult<()> {
    Ok(())
}

fn fake_apply_setting(
    _scan: SettingScan,
    _databaseid: Oid,
    _roleid: Oid,
    _source: GucSource,
) -> PgResult<()> {
    Ok(())
}

fn install_seams() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        fseam::extract_set_variable_args::set(fake_extract);
        fseam::guc_array_reset::set(fake_guc_array_reset);
        fseam::guc_array_add::set(fake_guc_array_add);
        fseam::guc_array_delete::set(fake_guc_array_delete);
        seam::alter_find::set(fake_alter_find);
        seam::update_setconfig::set(fake_update_setconfig);
        seam::delete_found_tuple::set(fake_delete_found_tuple);
        seam::insert_setting::set(fake_insert_setting);
        seam::alter_finish::set(fake_alter_finish);
        seam::drop_settings::set(fake_drop_settings);
        seam::apply_setting::set(fake_apply_setting);
    });
}

fn make_stmt(kind: VariableSetKind, name: &str) -> VariableSetStmt {
    VariableSetStmt {
        kind,
        name: Some(name.to_string()),
        args: Vec::new(),
        is_local: false,
        location: -1,
    }
}

fn arr(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

fn run<F: FnOnce(Mcx<'_>)>(f: F) {
    let ctx = MemoryContext::new("pg_db_role_setting-test");
    f(ctx.mcx());
}

#[test]
fn set_value_no_existing_tuple_inserts() {
    install_seams();
    reset();
    unsafe {
        VALUESTR = Some("on".to_string());
        FOUND = None;
        ADD_RESULT = arr(&["work_mem=on"]);
    }
    let stmt = make_stmt(VariableSetKind::SetValue, "work_mem");
    run(|mcx| AlterSetting(mcx, 100, 200, &stmt).unwrap());

    assert_eq!(
        ops(),
        vec![
            Op::Add(None, "work_mem".to_string(), "on".to_string()),
            Op::Insert(100, 200, arr(&["work_mem=on"])),
            Op::Finish(100, 200),
        ]
    );
}

#[test]
fn set_value_existing_tuple_updates() {
    install_seams();
    reset();
    unsafe {
        VALUESTR = Some("4MB".to_string());
        FOUND = Some(Some(arr(&["x=1"])));
        ADD_RESULT = arr(&["x=1", "work_mem=4MB"]);
    }
    let stmt = make_stmt(VariableSetKind::SetValue, "work_mem");
    run(|mcx| AlterSetting(mcx, 1, 2, &stmt).unwrap());

    assert_eq!(
        ops(),
        vec![
            Op::Add(Some(arr(&["x=1"])), "work_mem".to_string(), "4MB".to_string()),
            Op::Update(arr(&["x=1", "work_mem=4MB"])),
            Op::Finish(1, 2),
        ]
    );
}

#[test]
fn set_value_existing_tuple_null_setconfig_adds_from_none() {
    install_seams();
    reset();
    unsafe {
        VALUESTR = Some("x".to_string());
        FOUND = Some(None); // tuple found, setconfig column NULL
        ADD_RESULT = arr(&["v=x"]);
    }
    let stmt = make_stmt(VariableSetKind::SetValue, "v");
    run(|mcx| AlterSetting(mcx, 1, 2, &stmt).unwrap());

    let o = ops();
    assert_eq!(o[0], Op::Add(None, "v".to_string(), "x".to_string()));
    assert_eq!(o[1], Op::Update(arr(&["v=x"])));
}

#[test]
fn reset_var_existing_tuple_deletes_entry_then_deletes_tuple_when_empty() {
    install_seams();
    reset();
    unsafe {
        VALUESTR = None; // RESET => valuestr NULL
        FOUND = Some(Some(arr(&["work_mem=4MB"])));
        DELETE_RESULT = None; // GUCArrayDelete empties the array
    }
    let stmt = make_stmt(VariableSetKind::Reset, "work_mem");
    run(|mcx| AlterSetting(mcx, 1, 2, &stmt).unwrap());

    assert_eq!(
        ops(),
        vec![
            Op::Delete(Some(arr(&["work_mem=4MB"])), "work_mem".to_string()),
            Op::DeleteTuple,
            Op::Finish(1, 2),
        ]
    );
}

#[test]
fn reset_var_existing_tuple_deletes_entry_then_updates_when_nonempty() {
    install_seams();
    reset();
    unsafe {
        VALUESTR = None;
        FOUND = Some(Some(arr(&["a=1", "work_mem=4MB"])));
        DELETE_RESULT = Some(arr(&["a=1"]));
    }
    let stmt = make_stmt(VariableSetKind::Reset, "work_mem");
    run(|mcx| AlterSetting(mcx, 1, 2, &stmt).unwrap());

    assert_eq!(
        ops(),
        vec![
            Op::Delete(Some(arr(&["a=1", "work_mem=4MB"])), "work_mem".to_string()),
            Op::Update(arr(&["a=1"])),
            Op::Finish(1, 2),
        ]
    );
}

#[test]
fn reset_var_no_tuple_is_noop_except_finish() {
    install_seams();
    reset();
    unsafe {
        VALUESTR = None;
        FOUND = None;
    }
    let stmt = make_stmt(VariableSetKind::Reset, "work_mem");
    run(|mcx| AlterSetting(mcx, 1, 2, &stmt).unwrap());

    assert_eq!(ops(), vec![Op::Finish(1, 2)]);
}

#[test]
fn reset_all_existing_tuple_resets_then_updates() {
    install_seams();
    reset();
    unsafe {
        VALUESTR = None;
        FOUND = Some(Some(arr(&["a=1", "b=2"])));
        RESET_RESULT = Some(arr(&["a=1"]));
    }
    let stmt = make_stmt(VariableSetKind::ResetAll, "ignored");
    run(|mcx| AlterSetting(mcx, 1, 2, &stmt).unwrap());

    assert_eq!(
        ops(),
        vec![
            Op::Reset(arr(&["a=1", "b=2"])),
            Op::Update(arr(&["a=1"])),
            Op::Finish(1, 2),
        ]
    );
}

#[test]
fn reset_all_existing_tuple_resets_to_empty_deletes_tuple() {
    install_seams();
    reset();
    unsafe {
        VALUESTR = None;
        FOUND = Some(Some(arr(&["a=1"])));
        RESET_RESULT = None;
    }
    let stmt = make_stmt(VariableSetKind::ResetAll, "ignored");
    run(|mcx| AlterSetting(mcx, 1, 2, &stmt).unwrap());

    assert_eq!(
        ops(),
        vec![Op::Reset(arr(&["a=1"])), Op::DeleteTuple, Op::Finish(1, 2)]
    );
}

#[test]
fn reset_all_null_setconfig_skips_reset_but_deletes_tuple() {
    install_seams();
    reset();
    unsafe {
        VALUESTR = None;
        FOUND = Some(None); // tuple found but setconfig NULL
    }
    let stmt = make_stmt(VariableSetKind::ResetAll, "ignored");
    run(|mcx| AlterSetting(mcx, 1, 2, &stmt).unwrap());

    // `new` stays NULL (no GUCArrayReset call when isnull), so the `else`
    // branch runs CatalogTupleDelete. No `Reset` op; tuple is deleted.
    assert_eq!(ops(), vec![Op::DeleteTuple, Op::Finish(1, 2)]);
}

#[test]
fn reset_all_no_tuple_is_noop_except_finish() {
    install_seams();
    reset();
    unsafe {
        VALUESTR = None;
        FOUND = None;
    }
    let stmt = make_stmt(VariableSetKind::ResetAll, "ignored");
    run(|mcx| AlterSetting(mcx, 1, 2, &stmt).unwrap());

    assert_eq!(ops(), vec![Op::Finish(1, 2)]);
}

#[test]
fn drop_setting_passes_oids_through() {
    install_seams();
    reset();
    DropSetting(100, INVALID_OID).unwrap();
    DropSetting(INVALID_OID, 200).unwrap();
    DropSetting(100, 200).unwrap();
}

#[test]
fn apply_setting_runs_scan() {
    install_seams();
    reset();
    ApplySetting(SCAN, 100, 200, GucSource::PGC_S_DATABASE).unwrap();
}
