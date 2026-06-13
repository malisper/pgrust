//! Seam declarations for the `TableFuncRoutine` table-builder vtable
//! (`executor/tablefunc.h`), whose two instances live in the unported owners
//! `XmlTableRoutine` (`utils/adt/xml.c`) and `JsonbTableRoutine`
//! (`utils/adt/jsonpath_exec.c`).
//!
//! C dispatches through a `const TableFuncRoutine *routine` function-pointer
//! table held on the node; the owned model carries the routine identity as
//! [`TableFuncRoutineKind`] and reaches each method through the seam, keyed by
//! that kind. The owners install these when they land (a thin dispatch
//! installer marshals to the concrete XML / JSON_TABLE builder methods).
//!
//! The builder methods read and mutate the node's `opaque` private space and
//! allocate in its `perTableCxt`; each runs in `ereport`-capable code, so the
//! seams are fallible (`PgResult`). `JsonbTableRoutine` leaves `SetNamespace`,
//! `SetRowFilter`, and `SetColumnFilter` NULL — [`routine_has_set_row_filter`]
//! reports the optional `SetRowFilter` presence the C tests (`if
//! (routine->SetRowFilter)`); the other two are only reached for XMLTABLE,
//! whose lists drive the calls.

#![allow(non_snake_case)]

use types_core::primitive::Oid;
use types_datum::Datum;
use types_error::PgResult;
use types_nodes::{TableFuncRoutineKind, TableFuncScanState};

seam_core::seam!(
    /// `routine->InitOpaque(state, natts)` (tablefunc.h): allocate and
    /// initialize the table builder's private space (`state->opaque`).
    pub fn routine_init_opaque(
        state: &mut TableFuncScanState<'_>,
        kind: TableFuncRoutineKind,
        natts: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `routine->SetDocument(state, value)` (tablefunc.h): install the
    /// (possibly-toasted) document Datum into the builder context.
    pub fn routine_set_document(
        state: &mut TableFuncScanState<'_>,
        kind: TableFuncRoutineKind,
        value: Datum,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `routine->SetNamespace(state, name, uri)` (tablefunc.h): declare a
    /// namespace. `name` is `None` for the DEFAULT namespace (the C `NULL`).
    pub fn routine_set_namespace(
        state: &mut TableFuncScanState<'_>,
        kind: TableFuncRoutineKind,
        name: Option<&str>,
        uri: &str,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `routine->SetRowFilter != NULL` (tablefunc.h): does this routine
    /// implement a row filter? (XMLTABLE yes, JSON_TABLE no.) Infallible.
    pub fn routine_has_set_row_filter(kind: TableFuncRoutineKind) -> bool
);

seam_core::seam!(
    /// `routine->SetRowFilter(state, path)` (tablefunc.h): install the row
    /// filter expression. Only valid when [`routine_has_set_row_filter`].
    pub fn routine_set_row_filter(
        state: &mut TableFuncScanState<'_>,
        kind: TableFuncRoutineKind,
        path: &str,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `routine->SetColumnFilter(state, path, colnum)` (tablefunc.h): install
    /// the column filter expression for column `colnum`.
    pub fn routine_set_column_filter(
        state: &mut TableFuncScanState<'_>,
        kind: TableFuncRoutineKind,
        path: &str,
        colnum: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `routine->FetchRow(state)` (tablefunc.h): advance to the next row;
    /// returns `false` when there are no more rows.
    pub fn routine_fetch_row(
        state: &mut TableFuncScanState<'_>,
        kind: TableFuncRoutineKind,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `routine->GetValue(state, colnum, typid, typmod, &isnull)`
    /// (tablefunc.h): fetch the current row's value for column `colnum`,
    /// converted to type `typid`/`typmod`. Returns `(Datum, is_null)`.
    pub fn routine_get_value(
        state: &mut TableFuncScanState<'_>,
        kind: TableFuncRoutineKind,
        colnum: i32,
        typid: Oid,
        typmod: i32,
    ) -> PgResult<(Datum, bool)>
);

seam_core::seam!(
    /// `routine->DestroyOpaque(state)` (tablefunc.h): tear down the builder's
    /// private space and clear `state->opaque`.
    pub fn routine_destroy_opaque(
        state: &mut TableFuncScanState<'_>,
        kind: TableFuncRoutineKind,
    ) -> PgResult<()>
);
