//! Installation of the `TableFuncRoutine` vtable seams for `JsonbTableRoutine`
//! (jsonpath_exec.c). The `TableFuncScanState`'s table-builder dispatch
//! (`InitOpaque` / `SetDocument` / `FetchRow` / `GetValue` / `DestroyOpaque`,
//! plus the unused `SetNamespace` / `SetRowFilter` / `SetColumnFilter`) is keyed
//! by [`TableFuncRoutineKind`]; this unit owns the `JsonbTable` arm
//! (`XmlTable`'s arm belongs to the unported xml.c owner).
//!
//! The builder's private space (`state->opaque`) holds a [`JsonTableExecContext`]
//! the impls in [`crate::json_table`] read and mutate; the adapters below
//! downcast it out of the type-erased [`Opaque`] carrier.

use types_error::PgResult;
use types_nodes::execnodes::Opaque;
use types_nodes::{TableFuncRoutineKind, TableFuncScanState};
use types_tuple::backend_access_common_heaptuple::Datum;

use crate::json_table::{
    JsonTableDestroyOpaque, JsonTableExecContext, JsonTableFetchRow, JsonTableGetValue,
    JsonTableInitOpaque, JsonTableSetDocument,
};

/// Borrow the `JsonTableExecContext` out of an `Opaque` (`state->opaque`),
/// panicking if it is absent or of the wrong kind (a programming error —
/// `InitOpaque` runs first). Takes the `opaque` field directly so the caller
/// can hold a disjoint borrow of `state->perTableCxt` for the `Mcx`.
fn exec_context(opaque: &mut Opaque) -> &mut JsonTableExecContext {
    opaque
        .0
        .as_mut()
        .expect("JsonbTableRoutine: state->opaque is NULL (InitOpaque not run)")
        .downcast_mut::<JsonTableExecContext>()
        .expect("JsonbTableRoutine: state->opaque is not a JsonTableExecContext")
}

fn assert_jsonb(kind: TableFuncRoutineKind) {
    if kind != TableFuncRoutineKind::JsonbTable {
        panic!("tablefuncRoutine: XmlTableRoutine (xml.c) is not yet ported");
    }
}

/// `JsonbTableRoutine.InitOpaque` — allocate the builder context into
/// `state->opaque`.
fn routine_init_opaque(
    state: &mut TableFuncScanState<'_>,
    kind: TableFuncRoutineKind,
    natts: i32,
) -> PgResult<()> {
    assert_jsonb(kind);
    let cxt = JsonTableInitOpaque(natts)?;
    state.opaque = Opaque(Some(Box::new(cxt)));
    Ok(())
}

/// `JsonbTableRoutine.SetDocument` — install the document jsonb into the
/// builder context.
fn routine_set_document<'mcx>(
    state: &mut TableFuncScanState<'mcx>,
    kind: TableFuncRoutineKind,
    value: Datum<'mcx>,
) -> PgResult<()> {
    assert_jsonb(kind);
    let bytes = value.as_ref_bytes().to_vec();
    // Disjoint field borrows: perTableCxt (for the Mcx) and opaque (the context).
    let ctx = state
        .perTableCxt
        .as_ref()
        .expect("JsonbTableRoutine: perTableCxt not initialized");
    let cxt = exec_context(&mut state.opaque);
    JsonTableSetDocument(ctx.mcx(), cxt, &bytes)
}

/// `JsonbTableRoutine` leaves `SetNamespace` NULL — only XMLTABLE reaches it.
fn routine_set_namespace(
    _state: &mut TableFuncScanState<'_>,
    kind: TableFuncRoutineKind,
    _name: Option<&str>,
    _uri: &str,
) -> PgResult<()> {
    assert_jsonb(kind);
    panic!("JsonbTableRoutine has no SetNamespace method (NULL in C); only XMLTABLE uses it")
}

/// `routine->SetRowFilter != NULL` — false for JSON_TABLE, true for XMLTABLE.
fn routine_has_set_row_filter(kind: TableFuncRoutineKind) -> bool {
    matches!(kind, TableFuncRoutineKind::XmlTable)
}

/// `JsonbTableRoutine` leaves `SetRowFilter` NULL.
fn routine_set_row_filter(
    _state: &mut TableFuncScanState<'_>,
    kind: TableFuncRoutineKind,
    _path: &str,
) -> PgResult<()> {
    assert_jsonb(kind);
    panic!("JsonbTableRoutine has no SetRowFilter method (NULL in C); only XMLTABLE uses it")
}

/// `JsonbTableRoutine` leaves `SetColumnFilter` NULL.
fn routine_set_column_filter(
    _state: &mut TableFuncScanState<'_>,
    kind: TableFuncRoutineKind,
    _path: &str,
    _colnum: i32,
) -> PgResult<()> {
    assert_jsonb(kind);
    panic!("JsonbTableRoutine has no SetColumnFilter method (NULL in C); only XMLTABLE uses it")
}

/// `JsonbTableRoutine.FetchRow` — advance to the next row.
fn routine_fetch_row(
    state: &mut TableFuncScanState<'_>,
    kind: TableFuncRoutineKind,
) -> PgResult<bool> {
    assert_jsonb(kind);
    let ctx = state
        .perTableCxt
        .as_ref()
        .expect("JsonbTableRoutine: perTableCxt not initialized");
    let cxt = exec_context(&mut state.opaque);
    JsonTableFetchRow(ctx.mcx(), cxt)
}

/// `JsonbTableRoutine.GetValue` — fetch the current row's value for `colnum`.
fn routine_get_value<'mcx>(
    state: &mut TableFuncScanState<'mcx>,
    kind: TableFuncRoutineKind,
    colnum: i32,
    typid: types_core::primitive::Oid,
    typmod: i32,
) -> PgResult<(Datum<'mcx>, bool)> {
    assert_jsonb(kind);
    let cxt = exec_context(&mut state.opaque);
    let (datum, isnull) = JsonTableGetValue(cxt, colnum, typid, typmod)?;
    // JsonTableGetValue hands back a bare-word `types_datum::Datum`. A by-value
    // column result maps directly to the rich `Datum::ByVal`. A by-reference
    // column result cannot be carried by the bare word (the genuine by-ref
    // substrate gap), but the eval_column seam this dispatches into is itself
    // uninstalled, so a real by-ref value never reaches here yet.
    Ok((Datum::from_usize(datum.as_usize()), isnull))
}

/// `JsonbTableRoutine.DestroyOpaque` — tear down the builder context.
fn routine_destroy_opaque(
    state: &mut TableFuncScanState<'_>,
    kind: TableFuncRoutineKind,
) -> PgResult<()> {
    assert_jsonb(kind);
    {
        let cxt = exec_context(&mut state.opaque);
        JsonTableDestroyOpaque(cxt)?;
    }
    state.opaque = Opaque(None);
    Ok(())
}

/// The `TableFuncRoutine` vtable seams are owned and installed by
/// `backend-executor-nodeTableFuncscan` (the single kind-dispatching owner that
/// also serves the `XmlTable` arm via `xml.c`). This unit no longer installs
/// them — doing so collided (double-install) with that owner and would have
/// regressed the `XmlTable` arm, since these adapters panic on it. The
/// `JsonbTable` adapter bodies below (and the `JsonTable*` impls in
/// [`crate::json_table`]) remain available for the JSON_TABLE
/// executor-integration keystone (`init_table_func` / `eval_column`); the
/// dispatcher's `JsonbTable` arm will route into them once that substrate lands.
pub fn install_routines() {
    // Reference the adapters so they (and the `json_table` impls they call)
    // stay compiled and ready for the keystone, without registering the seams.
    let _ = (
        routine_init_opaque as fn(_, _, _) -> _,
        routine_set_document as fn(_, _, _) -> _,
        routine_set_namespace as fn(_, _, _, _) -> _,
        routine_has_set_row_filter as fn(_) -> _,
        routine_set_row_filter as fn(_, _, _) -> _,
        routine_set_column_filter as fn(_, _, _, _) -> _,
        routine_fetch_row as fn(_, _) -> _,
        routine_get_value as fn(_, _, _, _, _) -> _,
        routine_destroy_opaque as fn(_, _) -> _,
    );
}
