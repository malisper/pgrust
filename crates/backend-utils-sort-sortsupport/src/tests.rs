//! Unit tests for the `backend-utils-sort-sortsupport` port.
//!
//! The seam slots this crate calls outward (lsyscache, relcache) and the
//! inward fmgr machinery are process-global, so tests serialize through
//! `SEAM_LOCK` and install non-capturing `fn` dispatchers reading per-thread
//! scripted state.

extern crate std;

use std::cell::RefCell;
use std::string::ToString;
use std::sync::Mutex;
use std::vec::Vec;

use mcx::MemoryContext;
use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

use super::*;

static SEAM_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Default)]
struct Script {
    /// `(opfamily, lefttype, righttype, procnum) -> Oid` for `get_opfamily_proc`.
    opfamily_proc: Vec<((Oid, Oid, Oid, i16), Oid)>,
    opfamily_proc_default: Oid,
    /// `get_ordering_op_properties` result `(opfamily, opcintype, cmptype)`.
    ordering_props: Option<(Oid, Oid, i32)>,
}

std::thread_local! {
    static SCRIPT: RefCell<Script> = RefCell::new(Script::default());
}

fn with_script<R>(f: impl FnOnce(&mut Script) -> R) -> R {
    SCRIPT.with(|c| f(&mut c.borrow_mut()))
}

fn reset_script() {
    with_script(|s| *s = Script::default());
    // Drop any comparator tokens registered by a prior test.
    SHIMS.with(|s| s.borrow_mut().clear());
}

/// A builtin btree comparison function: returns `arg0 - arg1` clamped to a sign.
fn cmp_builtin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = backend_utils_fmgr_core::arg_value(fcinfo, 0).as_i32();
    let b = backend_utils_fmgr_core::arg_value(fcinfo, 1).as_i32();
    let r = (a > b) as i32 - (a < b) as i32;
    Datum::from_i32(r)
}

const CMP_OID: Oid = 9001;

fn install_seams() {
    // The builtin registry is per-thread (thread_local); re-register each call
    // so it survives a `clear_builtins` by another path.
    backend_utils_fmgr_core::register_builtin(BuiltinFunction {
        foid: CMP_OID,
        name: "cmp_builtin".to_string(),
        nargs: 2,
        strict: true,
        retset: false,
        func: Some(cmp_builtin),
    });

    // Seam slots are `OnceLock` — install at most once per process.
    if !lsyscache::get_opfamily_proc::is_installed() {
        lsyscache::get_opfamily_proc::set(|opfamily, lefttype, righttype, procnum| {
            with_script(|s| {
                for (key, oid) in &s.opfamily_proc {
                    if *key == (opfamily, lefttype, righttype, procnum) {
                        return Ok(*oid);
                    }
                }
                Ok(s.opfamily_proc_default)
            })
        });
    }
    if !lsyscache::get_ordering_op_properties::is_installed() {
        lsyscache::get_ordering_op_properties::set(|_op| with_script(|s| Ok(s.ordering_props)));
    }
}

// ---------------------------------------------------------------------------
// PrepareSortSupportFromOrderingOp + FinishSortSupportFunction
// ---------------------------------------------------------------------------

#[test]
fn ordering_op_invalid_operator_errors() {
    let _g = SEAM_LOCK.lock().unwrap();
    install_seams();
    reset_script();
    with_script(|s| s.ordering_props = None);

    let cx = MemoryContext::new("t");
    let mut ssup = SortSupportData::new(cx.mcx());
    let err = PrepareSortSupportFromOrderingOp(42, &mut ssup).unwrap_err();
    assert!(err
        .message
        .contains("operator 42 is not a valid ordering operator"));
}

#[test]
fn ordering_op_falls_back_to_shim_and_sets_reverse() {
    let _g = SEAM_LOCK.lock().unwrap();
    install_seams();
    reset_script();
    with_script(|s| {
        s.ordering_props = Some((100, 23, COMPARE_GT));
        // No BTSORTSUPPORT proc; a valid BTORDER proc (the builtin comparator).
        s.opfamily_proc.push(((100, 23, 23, BTSORTSUPPORT_PROC), 0));
        s.opfamily_proc.push(((100, 23, 23, BTORDER_PROC), CMP_OID));
    });

    let cx = MemoryContext::new("t");
    let mut ssup = SortSupportData::new(cx.mcx());
    PrepareSortSupportFromOrderingOp(2, &mut ssup).unwrap();

    // cmptype == COMPARE_GT => reversed.
    assert!(ssup.ssup_reverse);
    // A shim comparator was installed from the btree comparator.
    assert!(ssup.comparator.is_some());

    // Invoking it runs the builtin: cmp(7, 9) = -1, cmp(9, 7) = 1, cmp(5,5)=0.
    assert_eq!(
        apply_sort_comparator(Datum::from_i32(7), Datum::from_i32(9), &ssup).unwrap(),
        -1
    );
    assert_eq!(
        apply_sort_comparator(Datum::from_i32(9), Datum::from_i32(7), &ssup).unwrap(),
        1
    );
    assert_eq!(
        apply_sort_comparator(Datum::from_i32(5), Datum::from_i32(5), &ssup).unwrap(),
        0
    );
}

#[test]
fn ordering_op_missing_support_function_errors() {
    let _g = SEAM_LOCK.lock().unwrap();
    install_seams();
    reset_script();
    with_script(|s| {
        s.ordering_props = Some((9, 8, 1 /* COMPARE_LT */));
        s.opfamily_proc_default = 0;
    });

    let cx = MemoryContext::new("t");
    let mut ssup = SortSupportData::new(cx.mcx());
    let err = PrepareSortSupportFromOrderingOp(3, &mut ssup).unwrap_err();
    assert!(err.message.contains(&std::format!(
        "missing support function {}(8,8) in opfamily 9",
        BTORDER_PROC
    )));
}

// ---------------------------------------------------------------------------
// PrepareSortSupportComparisonShim + comparison_shim
// ---------------------------------------------------------------------------

#[test]
fn prepare_comparison_shim_installs_and_runs() {
    let _g = SEAM_LOCK.lock().unwrap();
    install_seams();
    reset_script();

    let cx = MemoryContext::new("t");
    let mut ssup = SortSupportData::new(cx.mcx());
    PrepareSortSupportComparisonShim(CMP_OID, &mut ssup).unwrap();

    assert!(ssup.comparator.is_some());
    let r = apply_sort_comparator(Datum::from_i32(3), Datum::from_i32(10), &ssup).unwrap();
    assert_eq!(r, -1);
}
