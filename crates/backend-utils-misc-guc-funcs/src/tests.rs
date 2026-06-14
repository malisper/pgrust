//! In-crate logic tests for `guc_funcs.c`.
//!
//! These drive the ported control flow that does not require the still-unbuilt
//! `DestReceiver` receiver-value router (the `SHOW` tuple-output path) or the
//! mcx-threaded `superuser`/`quote_identifier` owners: the `flatten` arms, the
//! list-arity error, the `unrecognized node type` arm, the SET switch's
//! parallel-mode guard, and the visibility rule. The GUC-core lookups run
//! against the W1 core's live store, seeded once by `initialize_guc_options`.

extern crate std;

use super::*;

use std::sync::{Mutex, MutexGuard, Once};

use types_parsenodes::{Integer, Float, StringNode};

static TEST_LOCK: Mutex<()> = Mutex::new(());

/// Seed the W1 core's global GUC store once (its `InitializeGUCOptions` analog),
/// and serialize the suite (the store is process-global).
fn begin() -> MutexGuard<'static, ()> {
    static INIT: Once = Once::new();
    let g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    INIT.call_once(|| {
        backend_utils_misc_guc::initialize_guc_options();
    });
    g
}

fn int_node(v: i32) -> Node {
    Node::Integer(Integer { ival: v })
}

fn float_node(s: &str) -> Node {
    Node::Float(Float {
        fval: Some(s.to_string()),
    })
}

fn string_node(s: &str) -> Node {
    Node::String(StringNode {
        sval: Some(s.to_string()),
    })
}

// --- flatten_set_variable_args -----------------------------------------------

#[test]
fn flatten_empty_is_default() {
    let _g = begin();
    // SET ... TO DEFAULT -> NULL flat value.
    assert_eq!(flatten_set_variable_args("work_mem", &[]).unwrap(), None);
}

#[test]
fn flatten_integer_arm() {
    let _g = begin();
    let args = [int_node(42)];
    assert_eq!(
        flatten_set_variable_args("work_mem", &args).unwrap(),
        Some("42".to_string())
    );
}

#[test]
fn flatten_float_arm_copies_string() {
    let _g = begin();
    // T_Float is kept as its source string and copied verbatim.
    let args = [float_node("2.5")];
    assert_eq!(
        flatten_set_variable_args("seq_page_cost", &args).unwrap(),
        Some("2.5".to_string())
    );
}

#[test]
fn flatten_string_arm_plain() {
    let _g = begin();
    // A non-GUC_LIST_QUOTE variable copies the string literal verbatim (no
    // quote_identifier seam crossed).
    let args = [string_node("verbose")];
    assert_eq!(
        flatten_set_variable_args("client_min_messages", &args).unwrap(),
        Some("verbose".to_string())
    );
}

#[test]
fn flatten_non_list_takes_only_one_argument() {
    let _g = begin();
    // A non-GUC_LIST_INPUT variable rejects multiple args.
    let args = [int_node(1), int_node(2)];
    let err = flatten_set_variable_args("work_mem", &args).unwrap_err();
    assert!(format!("{err:?}").contains("takes only one argument"));
}

#[test]
fn flatten_unrecognized_node_type() {
    let _g = begin();
    // A node that is not a value literal hits the C `default:` arm.
    let args = [Node::A_Star];
    let err = flatten_set_variable_args("work_mem", &args).unwrap_err();
    assert!(format!("{err:?}").contains("unrecognized node type"));
}

// --- ExtractSetVariableArgs --------------------------------------------------

#[test]
fn extract_reset_is_none() {
    let _g = begin();
    let stmt = VariableSetStmt {
        kind: VariableSetKind::Reset,
        name: Some("work_mem".to_string()),
        args: Vec::new(),
        is_local: false,
        location: -1,
    };
    assert_eq!(ExtractSetVariableArgs(&stmt).unwrap(), None);
}

#[test]
fn extract_set_value_flattens() {
    let _g = begin();
    let stmt = VariableSetStmt {
        kind: VariableSetKind::SetValue,
        name: Some("work_mem".to_string()),
        args: vec![int_node(64)],
        is_local: false,
        location: -1,
    };
    assert_eq!(
        ExtractSetVariableArgs(&stmt).unwrap(),
        Some("64".to_string())
    );
}

// --- the installed inward seam re-homes ExtractSetVariableArgs ---------------

#[test]
fn installed_seam_extract_set_variable_args() {
    let _g = begin();
    init_seams();
    let stmt = VariableSetStmt {
        kind: VariableSetKind::SetValue,
        name: Some("work_mem".to_string()),
        args: vec![int_node(7)],
        is_local: false,
        location: -1,
    };
    let out =
        backend_utils_misc_guc_funcs_seams::extract_set_variable_args::call(stmt).unwrap();
    assert_eq!(out, Some("7".to_string()));
}

// --- VariableSetKind subid mapping ------------------------------------------

#[test]
fn subid_mapping_matches_enum_order() {
    assert_eq!(variable_set_kind_as_subid(VariableSetKind::SetValue), 0);
    assert_eq!(variable_set_kind_as_subid(VariableSetKind::SetDefault), 1);
    assert_eq!(variable_set_kind_as_subid(VariableSetKind::SetCurrent), 2);
    assert_eq!(variable_set_kind_as_subid(VariableSetKind::SetMulti), 3);
    assert_eq!(variable_set_kind_as_subid(VariableSetKind::Reset), 4);
    assert_eq!(variable_set_kind_as_subid(VariableSetKind::ResetAll), 5);
}
