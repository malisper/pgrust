//! Seam declarations for `backend-utils-misc-guc-funcs`
//! (`utils/misc/guc_funcs.c`: the SET / SHOW / RESET statement layer).
//!
//! These are the cross-subsystem calls guc_funcs.c makes. The GUC **core**
//! operations (`set_config_option`, `GetConfigOptionByName`, `find_option`,
//! `ResetAllOptions`, `get_guc_variables`, `ShowGUCOption`) are NOT seamed —
//! the funcs crate calls the GUC core (`backend-utils-misc-guc`) directly,
//! since it depends on it. Everything declared here is an **outward** seam: the
//! funcs crate `::call`s it, and the real owner (`xact.c`, `snapmgr.c`,
//! `objectaccess`, the executor tuple-output path / `superuser.c` / `acl.c` /
//! `ruleutils.c`) installs it from its own `init_seams()`. Until that owner
//! lands a call panics loudly with the C symbol name — never a silent no-op.
//!
//! The executor tuple-output trio (`begin/do/end_tup_output`) crosses the
//! `DestReceiver` boundary, which in this tree is the still-unbuilt
//! receiver-value router keystone (the executor `dest_*` router seams have no
//! installer yet). The seams here carry the `SHOW` projection as rendered text
//! rows (`Option<String>`), matching `guc_funcs.c`'s `values[]`/`isnull[]`
//! text columns; the owner converts to the canonical slot/`Datum` form.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use types_core::primitive::Oid;
use types_error::PgResult;
use types_parsenodes::VariableSetStmt;

// --- INWARD seam: guc_funcs.c's own ExtractSetVariableArgs --------------------
//
// `ExtractSetVariableArgs(sstmt)` (guc_funcs.c) — the SET arg string, or `None`
// for a RESET; owns the `A_Const` arg-list flattening. Unlike the outward seams
// below, this one's real body lives in THIS unit (guc_funcs.c), so the owner
// crate installs it from its own `init_seams()`. Consumers (functioncmds
// ddl_core, pg-db-role-setting) `::call` it.

seam_core::seam!(
    /// `ExtractSetVariableArgs(sstmt)` (utils/misc/guc_funcs.c) — the SET arg
    /// string, or `None` for a RESET. Owns the `A_Const` arg-list flattening.
    pub fn extract_set_variable_args(sstmt: VariableSetStmt) -> PgResult<Option<String>>
);

// --- process / permission / transaction state -----------------------------

seam_core::seam!(
    /// `superuser()` (utils/init/miscinit.c → superuser.c): is the current user
    /// a superuser? Selects `PGC_SUSET` vs `PGC_USERSET` for the SET.
    pub fn superuser() -> bool
);

seam_core::seam!(
    /// `IsInParallelMode()` (access/transam/xact.c): SET is blocked during a
    /// parallel operation.
    pub fn is_in_parallel_mode() -> bool
);

seam_core::seam!(
    /// `WarnNoTransactionBlock(isTopLevel, stmtType)` (access/transam/xact.c):
    /// warn that a `SET LOCAL` / `SET TRANSACTION` has no effect outside a
    /// transaction block.
    pub fn warn_no_transaction_block(is_top_level: bool, stmt_type: String)
);

seam_core::seam!(
    /// `GetUserId()` (utils/init/miscinit.c): the current effective user OID.
    pub fn get_user_id() -> Oid
);

seam_core::seam!(
    /// `has_privs_of_role(member, role)` (utils/adt/acl.c): does `member`
    /// inherit the privileges of `role` (the `pg_read_all_settings` visibility
    /// check)?
    pub fn has_privs_of_role(member: Oid, role: Oid) -> bool
);

// --- SET TRANSACTION SNAPSHOT / SET TIME ZONE INTERVAL ----------------------

seam_core::seam!(
    /// `ImportSnapshot(idstr)` (utils/time/snapmgr.c): adopt the snapshot named
    /// by the `SET TRANSACTION SNAPSHOT '...'` argument. Can `ereport(ERROR)`.
    pub fn import_snapshot(idstr: String) -> PgResult<()>
);

// NOTE: the `SET TIME ZONE INTERVAL '...'` normalization
// (`interval_normalize`, guc_funcs.c:272-284) is reached only through a
// `TypeCast` wrapping an `A_Const`. The K1 parse-tree `Node` model
// (`types-parsenodes`) does not carry a `TypeCast` (nor an `A_Const` wrapper —
// value literals are stored directly as `Node::Integer`/`Float`/`String`), so
// that branch is structurally unreachable here; no seam is declared for it
// until the K1 model gains `TypeCast`. See `flatten_set_variable_args`.

// --- catalog hook + identifier quoting -------------------------------------

seam_core::seam!(
    /// `InvokeObjectPostAlterHookArgStr(ParameterAclRelationId, name, ACL_SET,
    /// subId, is_internal)` (catalog/objectaccess.h): the post-alter object
    /// access hook for a GUC change, addressed by name. `subid` is `stmt->kind`
    /// (the `VariableSetKind`) cast to `int`.
    pub fn invoke_object_post_alter_hook_arg_str(
        class_id: Oid,
        object_name: String,
        subid: i32,
        is_internal: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `quote_identifier(val)` (utils/adt/ruleutils.c): quote `val` if it is not
    /// a vanilla identifier (the `GUC_LIST_QUOTE` flatten branch).
    pub fn quote_identifier(val: String) -> String
);

// --- executor tuple-output path (execTuples.c) -----------------------------

/// A `TupOutputState` (execTuples.c) for the `SHOW` projection. The real C
/// struct carries the destination receiver and a `TupleTableSlot`; over the
/// (still-unbuilt) receiver-value router boundary, the owner threads its own
/// slot — this opaque carrier holds the destination handle so the rows can be
/// routed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TupOutputState {
    /// `tstate->dest` — the destination receiver handle.
    pub dest: types_nodes::parsestmt::DestReceiverHandle,
}

seam_core::seam!(
    /// `begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual)` (execTuples.c):
    /// prepare a `TupOutputState` for projecting rows of `tupdesc` to `dest`.
    /// `tupdesc` is carried as the rendered column count (the SHOW result is
    /// one or three TEXT columns); the owner builds the real slot.
    pub fn begin_tup_output_tupdesc(
        dest: types_nodes::parsestmt::DestReceiverHandle,
        ncols: i32,
    ) -> TupOutputState
);

seam_core::seam!(
    /// `do_text_output_oneline(tstate, value)` (execTuples.c): emit one
    /// single-text-column tuple (used by `SHOW <var>`).
    pub fn do_text_output_oneline(tstate: TupOutputState, value: String)
);

seam_core::seam!(
    /// `do_tup_output(tstate, values, isnull)` (execTuples.c): emit one tuple
    /// whose columns are the supplied text values (`None` is the C `isnull`
    /// flag). Used by `SHOW ALL` (a three-text-column row).
    pub fn do_tup_output(tstate: TupOutputState, values: Vec<Option<String>>)
);

seam_core::seam!(
    /// `end_tup_output(tstate)` (execTuples.c): finish and tear down a
    /// `TupOutputState`.
    pub fn end_tup_output(tstate: TupOutputState)
);
