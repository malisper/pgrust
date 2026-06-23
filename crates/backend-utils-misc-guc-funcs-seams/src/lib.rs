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
//! The executor tuple-output trio (`begin/do/end_tup_output`) threads the real
//! `TupOutputState<'mcx>` (a live `TupleTableSlot` + the `DestReceiver` handle)
//! built by `backend-executor-execTuples`. The funcs crate builds the SHOW
//! result `TupleDesc` (all-TEXT columns) and hands the rendered rows across as
//! text (`Option<String>`), matching `guc_funcs.c`'s `values[]`/`isnull[]`; the
//! `execTuples` owner converts each text column to the canonical `text` `Datum`,
//! stores it into the slot, and routes it through the `dest_*` receiver seams.

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

seam_core::seam!(
    /// `GetConfigOptionFlags(name, missing_ok)` (guc.c) — the `GUC_*` flag word
    /// for a named GUC variable (0 if unknown and `missing_ok`). Used by
    /// `pg_get_functiondef`'s proconfig rendering to detect `GUC_LIST_QUOTE`.
    /// The real body lives in `backend-utils-misc-guc`; it installs this from
    /// its own `init_seams()`.
    pub fn get_config_option_flags(name: String, missing_ok: bool) -> PgResult<i32>
);

seam_core::seam!(
    /// `SplitGUCList(rawstring, separator, &namelist)` (varlena.c) — parse a
    /// `GUC_LIST_QUOTE` value into its identifier elements (already dequoted).
    /// `Ok(None)` is the C `false` return (invalid list syntax). Owned by
    /// `backend-utils-adt-varlena`.
    pub fn split_guc_list(rawstring: String, separator: u8) -> PgResult<Option<Vec<String>>>
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

seam_core::seam!(
    /// `pg_parameter_aclcheck(name, roleid, mode) == ACLCHECK_OK` (catalog/aclchk.c):
    /// whether `roleid` holds the `mode` privilege on the configuration
    /// parameter `name`. `AlterSystemSetConfigFile` uses this with
    /// `mode == ACL_ALTER_SYSTEM` to gate a non-superuser ALTER SYSTEM on the
    /// target variable; `true` is the C `ACLCHECK_OK`. `mode` carries the raw
    /// `AclMode` bits (`ACL_ALTER_SYSTEM = 1 << 13`).
    pub fn pg_parameter_aclcheck_ok(name: String, roleid: Oid, mode: u64) -> PgResult<bool>
);

// --- SET TRANSACTION SNAPSHOT / SET TIME ZONE INTERVAL ----------------------

// NOTE: `import_snapshot` was re-homed to `backend-utils-time-snapmgr-seams`
// (its true C owner is `utils/time/snapmgr.c`); guc_funcs now calls it through
// that crate.

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
    /// stmt->kind, is_internal)` (catalog/objectaccess.h): the post-alter object
    /// access hook for a GUC change, addressed by name. C call (guc_funcs.c:156)
    /// is `(ParameterAclRelationId, stmt->name, ACL_SET, stmt->kind, false)`, so
    /// `subid` is `ACL_SET` and `auxiliary_id` is `stmt->kind` (the
    /// `VariableSetKind`) cast to `int`.
    pub fn invoke_object_post_alter_hook_arg_str(
        class_id: Oid,
        object_name: String,
        subid: i32,
        auxiliary_id: Oid,
        is_internal: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `quote_identifier(val)` (utils/adt/ruleutils.c): quote `val` if it is not
    /// a vanilla identifier (the `GUC_LIST_QUOTE` flatten branch).
    pub fn quote_identifier(val: String) -> String
);

// --- executor tuple-output path (execTuples.c) -----------------------------
//
// The real `TupOutputState<'mcx>` (a live `TupleTableSlot` + the `DestReceiver`
// handle) lives in `types_nodes::tuptable`; the `execTuples` owner installs
// these onto its real bodies. The funcs crate builds the SHOW result
// `TupleDesc` (all-TEXT columns) and passes rendered text rows; the owner
// converts each column to a `text` `Datum`.

seam_core::seam!(
    /// `begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual)` (execTuples.c):
    /// prepare a `TupOutputState` for projecting rows of `tupdesc` to `dest`.
    pub fn begin_tup_output_tupdesc<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        dest: types_nodes::parsestmt::DestReceiverHandle,
        tupdesc: types_tuple::heaptuple::TupleDesc<'mcx>,
    ) -> PgResult<types_nodes::tuptable::TupOutputState<'mcx>>
);

seam_core::seam!(
    /// `do_text_output_oneline(tstate, value)` (execTuples.c): emit one
    /// single-text-column tuple (used by `SHOW <var>`).
    pub fn do_text_output_oneline<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        tstate: &mut types_nodes::tuptable::TupOutputState<'mcx>,
        value: String,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `do_tup_output(tstate, values, isnull)` (execTuples.c): emit one tuple
    /// whose columns are the supplied text values (`None` is the C `isnull`
    /// flag). Used by `SHOW ALL` (a three-text-column row).
    pub fn do_tup_output<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        tstate: &mut types_nodes::tuptable::TupOutputState<'mcx>,
        values: Vec<Option<String>>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `end_tup_output(tstate)` (execTuples.c): finish and tear down a
    /// `TupOutputState`.
    pub fn end_tup_output<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        tstate: types_nodes::tuptable::TupOutputState<'mcx>,
    ) -> PgResult<()>
);
