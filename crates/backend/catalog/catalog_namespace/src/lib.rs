#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]
// These lints fire on constructs that mirror the C control flow 1:1 (C-style
// late init, the nested `if`s of the *IsVisibleExt quick checks, and indexed
// catlist walks); keeping the C shape aids the audit.
#![allow(clippy::needless_late_init)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::needless_range_loop)]

//! `backend/catalog/namespace.c` — code to support accessing and searching
//! namespaces.
//!
//! The search-path machinery, the `RangeVarGetRelidExtended` family, the
//! `*GetCandidates` / `*GetOid` / `*IsVisible(Ext)` predicates for every
//! catalog object kind, `DeconstructQualifiedName` / `Lookup*Namespace` /
//! `get_namespace_oid`, the temp-namespace lifecycle, the search-path cache
//! (`spcache_*`, `recomputeNamespacePath`, ...), the `search_path` GUC hooks,
//! and the SQL-callable `pg_*_is_visible` wrappers.
//!
//! The per-backend file-scope statics (`activeSearchPath`, `myTempNamespace`,
//! `baseSearchPathValid`, the search-path cache, the `namespace_search_path`
//! GUC string, ...) live in a `thread_local!`, matching PostgreSQL's
//! one-backend-per-process model (AGENTS.md "Backend-global state"). The C
//! memory-context choreography around them (`SearchPathCacheContext`,
//! `TopMemoryContext` list copies) is replaced by Rust ownership: dropping /
//! clearing the owned containers is the context reset.
//!
//! Genuine externals — syscache reads, ACL checks, locks, the `pg_namespace`
//! row creation, `performDeletion`, the object-access hook, xact/miscadmin
//! globals — cross per-owner seam crates and panic loudly until their owner
//! units land.
//!
//! Owned-model adaptations:
//!   - a qualified-name `List *` of `String`/`A_Star` value nodes is
//!     `&[Option<String>]` (`None` = `A_Star`);
//!   - `_FuncCandidateList` is `PgVec<FuncCandidate>` in C list order;
//!   - out-parameters become return values;
//!   - functions that allocate in C's `CurrentMemoryContext` — results and
//!     transient catalog-row copies alike — take an explicit `Mcx<'mcx>`
//!     (`docs/mctx-design.md`). Error-message text is the one exception: it
//!     travels in the `PgError` carrier (C: `ErrorContext`). The per-backend
//!     `NamespaceState` (C: `TopMemoryContext` / `SearchPathCacheContext`
//!     allocations) remains std-allocated; see DESIGN_DEBT.md.

use std::cell::RefCell;
use std::collections::HashMap;

use ::utils_error::ereport;
use ::mcx::{slice_in, vec_with_capacity_in, Mcx, MemoryContext, PgString, PgVec};
use ::types_acl::{
    AclResult, ACLCHECK_NOT_OWNER, ACLCHECK_OK, ACL_CREATE, ACL_CREATE_TEMP, ACL_MAINTAIN,
    ACL_USAGE,
};
use ::types_core::{
    InvalidOid, InvalidSubTransactionId, Oid, OidIsValid, ProcNumber, SubTransactionId,
    BOOTSTRAP_SUPERUSERID, DATABASE_RELATION_ID, FUNC_MAX_ARGS, INVALID_PROC_NUMBER,
    NAMESPACE_RELATION_ID, OIDOID, PG_CATALOG_NAMESPACE, PG_TOAST_NAMESPACE,
    RELATION_RELATION_ID,
    RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP,
};
// Residual bare-word `Datum` (the `datum` newtype) survives ONLY at the
// `CacheRegisterSyscacheCallback` ABI edge: the `cache_register_syscache_callback`
// seam takes its opaque callback token by the bare-word contract
// (`arg: ScalarWord`, where `ScalarWord = ::datum::Datum`), owned by the
// out-of-batch `backend-utils-cache-inval-seams` crate, so it stays on the shim
// until that contract migrates. The `before_shmem_exit` edge already rides the
// canonical `::types_tuple::Datum<'static>`. This crate's own logic constructs/reads
// no scalars, so there is nothing else to move onto the canonical
// `types_tuple::heaptuple::Datum<'mcx>` enum; the token here
// is `(Datum) 0` in C, i.e. `Datum::null()`.
use ::datum::Datum;
use ::types_error::{
    ErrorLocation, PgError, PgResult, DEBUG1, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_LOCK_NOT_AVAILABLE,
    ERRCODE_READ_ONLY_SQL_TRANSACTION, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_UNDEFINED_SCHEMA, ERRCODE_UNDEFINED_TABLE, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use ::types_namespace::{FuncArgInfo, FuncCandidate, ProcRow};
use ::nodes::parsenodes::{OBJECT_INDEX, OBJECT_SCHEMA};
use ::types_tuple::access::{
    RangeVar, RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION, RELKIND_TOASTVALUE,
};
use ::types_syscache::{AUTHMEMROLEMEM, AUTHOID, DATABASEOID, NAMESPACEOID};
use ::types_storage::lock::{
    AccessShareLock, ShareLock, ShareUpdateExclusiveLock, LOCKMODE, NoLock,
};
use index_seams as index_seams;

pub use ::types_namespace::{
    FuncCandidateList, SearchPathMatcher, TempNamespaceStatus, RVR_MISSING_OK, RVR_NOWAIT,
    RVR_SKIP_LOCKED,
};
pub use ::types_namespace::namespace::{
    TEMP_NAMESPACE_IDLE, TEMP_NAMESPACE_IN_USE, TEMP_NAMESPACE_NOT_TEMP,
};

use transam_parallel as parallel_seams;
use transam_xact_seams as xact_seams;
use transam_xlog_seams as xlog_seams;
use aclchk_seams as aclchk_seams;
use dependency_seams as dependency_seams;
use ::dependency_seams::{
    PERFORM_DELETION_INTERNAL, PERFORM_DELETION_QUIETLY, PERFORM_DELETION_SKIP_EXTENSIONS,
    PERFORM_DELETION_SKIP_ORIGINAL,
};
use objectaccess_seams as objectaccess_seams;
use objectaddress_seams as objectaddress_seams;
use pg_namespace_seams as pg_namespace_seams;
use dbcommands_seams as dbcommands_seams;
use pg_conversion_seams as pg_conversion_seams;
use procarray_seams as procarray_seams;
use dsm_core_seams as ipc_seams;
use sinval_seams as sinval_seams;
use lmgr_seams as lmgr_seams;
use lmgr_proc_seams as proc_seams;
use ruleutils_seams as ruleutils_seams;
use varlena_seams as varlena_seams;
use inval_seams as inval_seams;
use lsyscache_seams as lsyscache_seams;
use syscache_seams as syscache_seams;
use funcapi_seams as funcapi_seams;
use miscinit_seams as miscinit_seams;
use init_small_seams as globals_seams;
use mbutils_seams as mbutils_seams;
use guc_seams as guc_seams;
use snapmgr_seams as snapmgr_seams;

pub mod fmgr_builtins;

/// Install this crate's seam implementations (`backend-catalog-namespace-seams`).
pub fn init_seams() {
    fmgr_builtins::register_namespace_builtins();
    namespace_seams::check_set_namespace::set(|old_nsp, nsp| {
        let scratch = MemoryContext::new("CheckSetNamespace seam");
        crate::CheckSetNamespace(scratch.mcx(), old_nsp, nsp)
    });
    namespace_seams::get_namespace_oid::set(crate::get_namespace_oid);
    namespace_seams::range_var_get_relid::set(crate::RangeVarGetRelid);
    vacuum_seams::range_var_get_relid_extended::set(
        seam_vacuum_range_var_get_relid_extended,
    );
    namespace_seams::range_var_get_relid_maintains_table::set(
        crate::RangeVarGetRelidMaintainsTable,
    );
    namespace_seams::range_var_get_relid_for_reindex_index::set(
        crate::RangeVarGetRelidForReindexIndex,
    );
    // REFRESH MATERIALIZED VIEW (matview.c ExecRefreshMatView) resolves+locks its
    // target through `RangeVarGetRelidExtended(.., RangeVarCallbackMaintainsTable)`.
    // The matview-deps seam carries the RangeVar by its resolved schema/relation
    // names (the callback is folded in); marshal them onto the access-layer
    // RangeVar and dispatch the shared maintains-table resolver, spinning a
    // scratch context (the only outputs are the by-value Oid and the lock taken).
    matview_deps_seams::rangevar_get_relid_extended::set(
        |schemaname, relname, lockmode| {
            let ctx = ::mcx::MemoryContext::new("RangeVarGetRelidMaintainsTable");
            let relation = RangeVar {
                schemaname,
                relname,
                ..RangeVar::default()
            };
            crate::RangeVarGetRelidMaintainsTable(ctx.mcx(), &relation, lockmode)
        },
    );
    namespace_seams::range_var_get_relid_from_text::set(
        seam_range_var_get_relid_from_text,
    );
    namespace_seams::range_var_get_and_check_creation_namespace::set(
        seam_range_var_get_and_check_creation_namespace,
    );
    namespace_seams::range_var_get_relid_owns_seq::set(
        seam_range_var_get_relid_owns_seq,
    );
    namespace_seams::lookup_explicit_namespace::set(crate::LookupExplicitNamespace);
    namespace_seams::funcname_get_candidates::set(seam_funcname_get_candidates);
    namespace_seams::opername_get_candidates::set(seam_opername_get_candidates);
    namespace_seams::opername_get_oprid::set(seam_opername_get_oprid);
    namespace_seams::get_collation_oid::set(seam_get_collation_oid);
    namespace_seams::get_ts_dict_oid::set(seam_get_ts_dict_oid);
    namespace_seams::make_range_var_from_name_list::set(seam_make_range_var_from_name_list);
    namespace_seams::relation_is_visible::set(crate::RelationIsVisible);
    namespace_seams::function_is_visible::set(crate::FunctionIsVisible);
    namespace_seams::operator_is_visible::set(crate::OperatorIsVisible);
    namespace_seams::collation_is_visible::set(crate::CollationIsVisible);
    namespace_seams::ts_config_is_visible::set(crate::TSConfigIsVisible);
    namespace_seams::ts_dictionary_is_visible::set(crate::TSDictionaryIsVisible);
    namespace_seams::conversion_is_visible::set(crate::ConversionIsVisible);
    namespace_seams::opclass_is_visible::set(crate::OpclassIsVisible);
    namespace_seams::opfamily_is_visible::set(crate::OpfamilyIsVisible);
    namespace_seams::statistics_obj_is_visible::set(crate::StatisticsObjIsVisible);
    namespace_seams::ts_parser_is_visible::set(crate::TSParserIsVisible);
    namespace_seams::ts_template_is_visible::set(crate::TSTemplateIsVisible);
    namespace_seams::at_eoxact_namespace::set(seam_at_eoxact_namespace);
    namespace_seams::at_eosubxact_namespace::set(seam_at_eosubxact_namespace);
    namespace_seams::get_ts_config_oid::set(seam_get_ts_config_oid);
    namespace_seams::type_is_visible::set(crate::TypeIsVisible);
    namespace_seams::is_temp_namespace::set(seam_is_temp_namespace);
    namespace_seams::is_any_temp_namespace::set(crate::isAnyTempNamespace);
    namespace_seams::is_temp_or_temp_toast_namespace::set(
        crate::isTempOrTempToastNamespace,
    );
    namespace_seams::get_temp_namespace_proc_number::set(
        crate::get_temp_namespace_proc_number_no_mcx,
    );
    namespace_seams::lookup_creation_namespace::set(seam_lookup_creation_namespace);
    namespace_seams::get_conversion_oid::set(seam_get_conversion_oid);
    namespace_seams::get_ts_parser_oid::set(seam_get_ts_parser_oid);
    namespace_seams::get_ts_template_oid::set(seam_get_ts_template_oid);
    namespace_seams::get_statistics_object_oid::set(seam_get_statistics_object_oid);
    namespace_seams::fetch_search_path::set(crate::fetch_search_path);
    namespace_seams::get_search_path_matcher_value::set(
        crate::GetSearchPathMatcher,
    );
    namespace_seams::search_path_matches_current_environment_value::set(
        crate::SearchPathMatchesCurrentEnvironment,
    );

    // Install the GUC machinery's typed accessors for the `search_path` GUC
    // (C's `char *namespace_search_path`, guc_tables.c:4513). C points
    // `conf->variable` at the `namespace_search_path` global and stores
    // `*conf->variable` itself before invoking `assign_search_path`; this
    // crate owns that storage, so the generic store folds into the `set`
    // accessor here. `search_path` boots to a non-NULL value (`"$user",
    // public`) and is never NULL, but the slot's `Option<String>` carries
    // C's NULL/empty distinction faithfully.
    guc_tables::vars::namespace_search_path.install(
        guc_tables::GucVarAccessors {
            get: || Some(crate::namespace_search_path()),
            set: |v| crate::set_namespace_search_path(v.as_deref().unwrap_or("")),
        },
    );

    // Parallel-worker bring-up: restore the leader's temp-namespace OIDs
    // (parallel.c ParallelWorkerMain `SetTempNamespaceState`). The body is
    // namespace.c's `SetTempNamespaceState` (void); install the parallel-rt seam
    // slot from the real owner. The parallel-rt seam crate is a leaf (no cycle).
    parallel_rt_seams::set_temp_namespace_state::set(|ns, toast_ns| {
        crate::SetTempNamespaceState(ns, toast_ns);
        Ok(())
    });
    namespace_seams::get_temp_namespace_state::set(crate::GetTempNamespaceState);

    // `search_path` GUC check/assign hooks (namespace.c). Installed here so
    // set_config_option("search_path", ...) — e.g. RestrictSearchPath during
    // CREATE INDEX / VACUUM — finds the owning unit's hooks.
    guc_tables::hooks::check_search_path.install(check_search_path_hook);
    guc_tables::hooks::assign_search_path.install(assign_search_path_hook);
}

/// Adapt a seam-borne `&[&str]` qualified name into the owned `NameList`
/// image (`&[Option<String>]`) the in-crate functions take.
fn name_list_owned(names: &[&str]) -> Vec<Option<String>> {
    names.iter().map(|s| Some((*s).to_string())).collect()
}

fn seam_funcname_get_candidates<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[&str],
    nargs: i32,
    argnames: &[&str],
    expand_variadic: bool,
    expand_defaults: bool,
    include_out_arguments: bool,
    missing_ok: bool,
) -> PgResult<FuncCandidateList<'mcx>> {
    let owned = name_list_owned(names);
    let argnames_owned: Vec<String> = argnames.iter().map(|s| (*s).to_string()).collect();
    FuncnameGetCandidates(
        mcx,
        &owned,
        nargs,
        &argnames_owned,
        expand_variadic,
        expand_defaults,
        include_out_arguments,
        missing_ok,
    )
}

fn seam_opername_get_candidates<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[&str],
    oprkind: u8,
    missing_schema_ok: bool,
) -> PgResult<FuncCandidateList<'mcx>> {
    let owned = name_list_owned(names);
    OpernameGetCandidates(mcx, &owned, oprkind, missing_schema_ok)
}

fn seam_opername_get_oprid(
    mcx: Mcx<'_>,
    names: &[&str],
    oprleft: Oid,
    oprright: Oid,
) -> PgResult<Oid> {
    let owned = name_list_owned(names);
    OpernameGetOprid(mcx, &owned, oprleft, oprright)
}

fn seam_get_collation_oid(mcx: Mcx<'_>, collname: &[&str], missing_ok: bool) -> PgResult<Oid> {
    let owned = name_list_owned(collname);
    get_collation_oid(mcx, &owned, missing_ok)
}

fn seam_get_ts_dict_oid(mcx: Mcx<'_>, names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    let owned = name_list_owned(names);
    get_ts_dict_oid(mcx, &owned, missing_ok)
}

fn seam_get_conversion_oid(mcx: Mcx<'_>, names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    let owned = name_list_owned(names);
    get_conversion_oid(mcx, &owned, missing_ok)
}

fn seam_get_ts_parser_oid(mcx: Mcx<'_>, names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    let owned = name_list_owned(names);
    get_ts_parser_oid(mcx, &owned, missing_ok)
}

fn seam_get_ts_template_oid(mcx: Mcx<'_>, names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    let owned = name_list_owned(names);
    get_ts_template_oid(mcx, &owned, missing_ok)
}

fn seam_get_statistics_object_oid(mcx: Mcx<'_>, names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    let owned = name_list_owned(names);
    get_statistics_object_oid(mcx, &owned, missing_ok)
}

fn seam_make_range_var_from_name_list(names: &[&str]) -> PgResult<RangeVar> {
    let owned = name_list_owned(names);
    makeRangeVarFromNameList(&owned)
}

/// Seam shim: the seam declares `fn(bool, bool)` (infallible surface); the
/// implementation returns `PgResult<()>` because `before_shmem_exit` can
/// ereport on OOM. OOM during transaction-end cleanup is always fatal, so
/// `.expect` is the correct escalation here.
fn seam_at_eoxact_namespace(is_commit: bool, parallel: bool) {
    crate::AtEOXact_Namespace(is_commit, parallel)
        .expect("AtEOXact_Namespace: before_shmem_exit OOM");
}

/// Seam shim: same pattern as `seam_at_eoxact_namespace`.
fn seam_at_eosubxact_namespace(
    is_commit: bool,
    my_subid: ::types_core::SubTransactionId,
    parent_subid: ::types_core::SubTransactionId,
) {
    crate::AtEOSubXact_Namespace(is_commit, my_subid, parent_subid)
        .expect("AtEOSubXact_Namespace");
}

/// Seam shim: the seam accepts `&[&str]` (name parts already extracted by
/// the consumer); the implementation expects `NameList` (`&[Option<String>]`)
/// plus an `Mcx` for the cross-database check in the 3-part case. Convert
/// here and use a scratch memory context.
fn seam_get_ts_config_oid(names: &[&str], missing_ok: bool) -> ::types_error::PgResult<::types_core::Oid> {
    let names_owned: Vec<Option<String>> = names.iter().map(|s| Some(s.to_string())).collect();
    let scratch = MemoryContext::new("get_ts_config_oid seam");
    crate::get_ts_config_oid(scratch.mcx(), &names_owned, missing_ok)
}

/// Seam shim: the implementation is infallible (`bool`); the seam carries the
/// per-owner error channel, so wrap in `Ok`.
fn seam_is_temp_namespace(namespace_id: ::types_core::Oid) -> ::types_error::PgResult<bool> {
    Ok(crate::isTempNamespace(namespace_id))
}

/// Seam shim: the seam has no `Mcx`; the implementation needs one for the
/// transient catalog copies the ACL/lookup makes. Use a scratch context, same
/// pattern as `seam_get_ts_config_oid`.
fn seam_lookup_creation_namespace(nspname: &str) -> ::types_error::PgResult<::types_core::Oid> {
    let scratch = MemoryContext::new("LookupCreationNamespace seam");
    crate::LookupCreationNamespace(scratch.mcx(), nspname)
}

/// `FUNC_PARAM_IN` / `FUNC_PARAM_INOUT` / `FUNC_PARAM_VARIADIC`
/// (`catalog/pg_proc.h`) — the proargmode chars `MatchNamedCall` accepts.
const FUNC_PARAM_IN: u8 = b'i';
const FUNC_PARAM_INOUT: u8 = b'b';
const FUNC_PARAM_VARIADIC: u8 = b'v';

/// `COLLPROVIDER_ICU` (`catalog/pg_collation.h`).
const COLLPROVIDER_ICU: u8 = b'i';

/// A possibly-qualified name list, the owned image of a `List *` of
/// `String` / `A_Star` value nodes. `None` is an `A_Star` element (only
/// `NameListToString` tolerates it).
pub type NameList<'a> = &'a [Option<String>];

/// `RangeVarGetRelidCallback` — a caller-supplied hook invoked by
/// [`RangeVarGetRelidExtended`]. The owned image is a borrowed closure.
pub type RangeVarGetRelidCallback<'a> =
    Option<&'a mut dyn FnMut(&RangeVar, Oid, Oid) -> PgResult<()>>;

/* ---------------------------------------------------------------------------
 * Per-backend module state (the namespace.c file-scope statics)
 * ------------------------------------------------------------------------- */

/// `SearchPathCacheEntry` (namespace.c). The C `List *` fields are `NIL` both
/// when never computed and when legitimately empty; an empty `Vec` carries the
/// same double meaning, so the recompute conditions match C exactly.
#[derive(Default)]
struct SearchPathCacheEntry {
    oidlist: Vec<Oid>,
    final_path: Vec<Oid>,
    first_ns: Oid,
    temp_missing: bool,
    force_recompute: bool,
}

/// `SearchPathCacheKey` — `(searchPath, roleid)`.
type SearchPathCacheKey = (String, Oid);

/// Mirror of namespace.c's file-scope statics, held per-backend.
struct NamespaceState {
    /* These variables define the actually active state: */
    /// `activeSearchPath`. In C this aliases `baseSearchPath` (a pointer
    /// copy); here it is a derived snapshot cloned from `base_search_path`.
    active_search_path: Vec<Oid>,
    /// `activeCreationNamespace` — InvalidOid means no default.
    active_creation_namespace: Oid,
    /// `activeTempCreationPending`.
    active_temp_creation_pending: bool,
    /// `activePathGeneration` — never zero.
    active_path_generation: u64,

    /* Values last derived from namespace_search_path: */
    base_search_path: Vec<Oid>,
    base_creation_namespace: Oid,
    base_temp_creation_pending: bool,
    namespace_user: Oid,
    /// The above four values are valid only if `base_search_path_valid`.
    base_search_path_valid: bool,

    /* Search path cache. */
    search_path_cache_valid: bool,
    /// `SearchPathCacheContext != NULL` — whether the cache machinery has
    /// ever been set up (gates `check_search_path`'s `use_cache`).
    search_path_cache_context_created: bool,
    /// `SearchPathCache` — `None` mirrors the C `SearchPathCache == NULL`.
    search_path_cache: Option<HashMap<SearchPathCacheKey, SearchPathCacheEntry>>,
    /// `LastSearchPathCacheEntry` — memo key of the last entry touched.
    last_cache_key: Option<SearchPathCacheKey>,

    /// The `namespace_search_path` GUC variable (a C global defined in
    /// namespace.c, assigned by the GUC machinery). Defaults to the GUC
    /// boot value `"$user", public`.
    namespace_search_path: String,

    /* Temp namespace state. */
    my_temp_namespace: Oid,
    my_temp_toast_namespace: Oid,
    my_temp_namespace_sub_id: SubTransactionId,
}

impl NamespaceState {
    fn new() -> Self {
        Self {
            active_search_path: Vec::new(),
            active_creation_namespace: InvalidOid,
            active_temp_creation_pending: false,
            active_path_generation: 1, /* MUST be never zero */
            base_search_path: Vec::new(),
            base_creation_namespace: InvalidOid,
            base_temp_creation_pending: false,
            namespace_user: InvalidOid,
            base_search_path_valid: true,
            search_path_cache_valid: false,
            search_path_cache_context_created: false,
            search_path_cache: None,
            last_cache_key: None,
            namespace_search_path: "\"$user\", public".to_string(),
            my_temp_namespace: InvalidOid,
            my_temp_toast_namespace: InvalidOid,
            my_temp_namespace_sub_id: InvalidSubTransactionId,
        }
    }
}

thread_local! {
    static STATE: RefCell<NamespaceState> = RefCell::new(NamespaceState::new());
}

/// Helper accessor for `myTempNamespace`.
fn my_temp_namespace() -> Oid {
    STATE.with(|s| s.borrow().my_temp_namespace)
}
/// Helper accessor for `myTempToastNamespace`.
fn my_temp_toast_namespace() -> Oid {
    STATE.with(|s| s.borrow().my_temp_toast_namespace)
}
/// Snapshot of `activeSearchPath`.
fn active_search_path() -> Vec<Oid> {
    STATE.with(|s| s.borrow().active_search_path.clone())
}

/// Read the `namespace_search_path` GUC string.
pub fn namespace_search_path() -> String {
    STATE.with(|s| s.borrow().namespace_search_path.clone())
}

/// The GUC machinery's write of the `namespace_search_path` variable (the
/// generic string-GUC assignment that precedes `assign_search_path`).
pub fn set_namespace_search_path(value: &str) {
    STATE.with(|s| s.borrow_mut().namespace_search_path = value.to_string());
}

/// `elog(ERROR, ...)` — internal error, `ERRCODE_INTERNAL_ERROR`.
fn elog_error<T>(message: String) -> PgResult<T> {
    Err(PgError::error(message))
}

/// `ErrorLocation` for `ereport(...).finish(...)` in this module.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("namespace.c", 0, funcname)
}

/// `list_member_oid(list, oid)`.
fn list_member_oid(list: &[Oid], oid: Oid) -> bool {
    list.contains(&oid)
}

/// `object_aclcheck(NamespaceRelationId, nspid, roleid, mode)`.
fn namespace_aclcheck(nspid: Oid, roleid: Oid, mode: ::types_acl::AclMode) -> PgResult<AclResult> {
    aclchk_seams::object_aclcheck::call(NAMESPACE_RELATION_ID, nspid, roleid, mode)
}

/// `aclcheck_error(aclresult, OBJECT_SCHEMA, name)`.
fn aclcheck_error_schema(aclresult: AclResult, name: Option<String>) -> PgResult<()> {
    aclchk_seams::aclcheck_error::call(aclresult, OBJECT_SCHEMA, name)
}

/// The `strcmp(catalogname, get_database_name(MyDatabaseId)) != 0` check
/// shared by the three cross-database error sites. `get_database_name`
/// returning NULL (no such database — cannot happen for `MyDatabaseId`)
/// compares unequal.
fn catalogname_differs_from_database(mcx: Mcx<'_>, catalogname: &str) -> PgResult<bool> {
    let dbname =
        dbcommands_seams::get_database_name::call(mcx, globals_seams::my_database_id::call())?;
    Ok(dbname.as_ref().map(|s| s.as_str()) != Some(catalogname))
}

/* ===========================================================================
 * Search path cache (spcache_*) (C lines 240-415)
 * ======================================================================== */

/// `SPCACHE_RESET_THRESHOLD`.
const SPCACHE_RESET_THRESHOLD: usize = 256;

/// `spcache_init` — create or reset the search_path cache as necessary.
fn spcache_init() {
    STATE.with(|s| {
        let mut st = s.borrow_mut();

        if st.search_path_cache.is_some()
            && st.search_path_cache_valid
            && st.search_path_cache.as_ref().unwrap().len() < SPCACHE_RESET_THRESHOLD
        {
            return;
        }

        st.search_path_cache_valid = false;
        st.base_search_path_valid = false;

        /*
         * Make sure we don't leave dangling pointers if a failure happens
         * during initialization.
         */
        st.search_path_cache = None;
        st.last_cache_key = None;

        /* SearchPathCacheContext creation / MemoryContextReset: dropping the
         * old map above released its allocations; mark the "context" made. */
        st.search_path_cache_context_created = true;

        /* arbitrary initial starting size of 16 elements */
        st.search_path_cache = Some(HashMap::with_capacity(16));
        st.search_path_cache_valid = true;
    });
}

/// `spcache_lookup` — look up an entry without inserting; `false` if not
/// present. (The only caller, `check_search_path`, tests presence only.)
fn spcache_lookup(searchPath: &str, roleid: Oid) -> bool {
    STATE.with(|s| {
        let mut st = s.borrow_mut();
        if let Some(last) = &st.last_cache_key {
            if last.1 == roleid && last.0 == searchPath {
                return true;
            }
        }
        let key = (searchPath.to_string(), roleid);
        let found = st
            .search_path_cache
            .as_ref()
            .is_some_and(|c| c.contains_key(&key));
        if found {
            st.last_cache_key = Some(key);
        }
        found
    })
}

/// `spcache_insert` — look up or insert an entry (with empty contents).
fn spcache_insert(searchPath: &str, roleid: Oid) {
    STATE.with(|s| {
        let mut st = s.borrow_mut();
        if let Some(last) = &st.last_cache_key {
            if last.1 == roleid && last.0 == searchPath {
                return;
            }
        }
        let key = (searchPath.to_string(), roleid);
        st.search_path_cache
            .as_mut()
            .expect("spcache_insert before spcache_init")
            .entry(key.clone())
            .or_default();
        st.last_cache_key = Some(key);
    });
}

/* ===========================================================================
 * RangeVarGetRelidExtended (C lines 441-642)
 * ======================================================================== */

/// `RangeVarGetRelid` (`catalog/namespace.h` macro): no callback, missing_ok
/// per flag.
pub fn RangeVarGetRelid(
    mcx: Mcx<'_>,
    relation: &RangeVar,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<Oid> {
    let flags = if missing_ok { RVR_MISSING_OK } else { 0 };
    RangeVarGetRelidExtended(mcx, relation, lockmode, flags, None)
}

/// `RangeVarCallbackMaintainsTable(relation, relId, oldRelId, arg)`
/// (tablecmds.c): the `RangeVarGetRelidExtended` callback shared by CLUSTER,
/// REINDEX TABLE, and REFRESH MATERIALIZED VIEW. It rejects non-table
/// relkinds and checks `ACL_MAINTAIN` on the resolved relation. Exposed here
/// (rather than in the unported tablecmds unit) so the
/// `range_var_get_relid_maintains_table` seam — declared on this crate's
/// seam crate — can be installed by its owner.
fn RangeVarCallbackMaintainsTable(
    relation: &RangeVar,
    rel_id: Oid,
    _old_rel_id: Oid,
) -> PgResult<()> {
    /* Nothing to do if the relation was not found. */
    if !OidIsValid(rel_id) {
        return Ok(());
    }

    /*
     * If the relation does exist, check whether it's an index.  But note that
     * the relation might have been dropped between the time we did the name
     * lookup and now.  In that case, there's nothing to do.
     */
    let relkind = lsyscache_seams::get_rel_relkind::call(rel_id)?;
    if relkind == 0 {
        return Ok(());
    }
    if relkind != RELKIND_RELATION
        && relkind != RELKIND_TOASTVALUE
        && relkind != RELKIND_MATVIEW
        && relkind != RELKIND_PARTITIONED_TABLE
    {
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "\"{}\" is not a table or materialized view",
                relation.relname.as_str()
            ))
            .finish(here("RangeVarCallbackMaintainsTable"));
    }

    /* Check permissions */
    let aclresult = aclchk_seams::pg_class_aclcheck::call(
        rel_id,
        miscinit_seams::get_user_id::call(),
        ACL_MAINTAIN,
    )?;
    if aclresult != ACLCHECK_OK {
        aclchk_seams::aclcheck_error::call(
            aclresult,
            objectaddress_seams::get_relkind_objtype::call(
                lsyscache_seams::get_rel_relkind::call(rel_id)?,
            ),
            Some(relation.relname.clone()),
        )?;
    }

    Ok(())
}

/// `RangeVarGetRelidExtended(relation, lockmode, 0,
/// RangeVarCallbackMaintainsTable, NULL)` — resolve+lock a CLUSTER / REINDEX
/// TABLE / REFRESH MATERIALIZED VIEW target, running the maintains-table
/// permission callback. (cluster.c / matview.c pass `AccessExclusiveLock`.)
pub fn RangeVarGetRelidMaintainsTable(
    mcx: Mcx<'_>,
    relation: &RangeVar,
    lockmode: LOCKMODE,
) -> PgResult<Oid> {
    let mut callback =
        |relation: &RangeVar, rel_id: Oid, old_rel_id: Oid| -> PgResult<()> {
            RangeVarCallbackMaintainsTable(relation, rel_id, old_rel_id)
        };
    RangeVarGetRelidExtended(mcx, relation, lockmode, 0, Some(&mut callback))
}

/// `RangeVarCallbackForReindexIndex(relation, relId, oldRelId, arg)`
/// (indexcmds.c): the `REINDEX INDEX` name-lookup callback. Check permissions
/// on the index's table before acquiring the relation lock; also lock the heap
/// before the index lock is taken, to avoid deadlocks.
///
/// `table_lockmode` is the heap lock level (`ShareLock` for the non-concurrent
/// case, `ShareUpdateExclusiveLock` for concurrent — matching `reindex_index()`
/// / `index_concurrently_*()`). `locked_table_oid` tracks the heap lock we hold
/// across a retry so it can be released if the name now refers to a different
/// relation. The heap lock acquired here is `keep()`'d (held until transaction
/// end), mirroring C where it lives in the backend lock table.
fn RangeVarCallbackForReindexIndex(
    relation: &RangeVar,
    rel_id: Oid,
    old_rel_id: Oid,
    table_lockmode: LOCKMODE,
    locked_table_oid: &mut Oid,
) -> PgResult<()> {
    /*
     * If we previously locked some other index's heap, and the name we're
     * looking up no longer refers to that relation, release the now-useless
     * lock.
     */
    if rel_id != old_rel_id && OidIsValid(*locked_table_oid) {
        lmgr_seams::unlock_relation_oid::call(*locked_table_oid, table_lockmode)?;
        *locked_table_oid = InvalidOid;
    }

    /* If the relation does not exist, there's nothing more to do. */
    if !OidIsValid(rel_id) {
        return Ok(());
    }

    /*
     * If the relation does exist, check whether it's an index.  But note that
     * the relation might have been dropped between the time we did the name
     * lookup and now.  In that case, there's nothing to do.
     */
    let relkind = lsyscache_seams::get_rel_relkind::call(rel_id)?;
    if relkind == 0 {
        return Ok(());
    }
    if relkind != RELKIND_INDEX && relkind != RELKIND_PARTITIONED_INDEX {
        return ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("\"{}\" is not an index", relation.relname.as_str()))
            .finish(here("RangeVarCallbackForReindexIndex"));
    }

    /* Check permissions */
    let table_oid = index_seams::index_get_relation::call(rel_id, true)?;
    if OidIsValid(table_oid) {
        let aclresult = aclchk_seams::pg_class_aclcheck::call(
            table_oid,
            miscinit_seams::get_user_id::call(),
            ACL_MAINTAIN,
        )?;
        if aclresult != ACLCHECK_OK {
            aclchk_seams::aclcheck_error::call(
                aclresult,
                OBJECT_INDEX,
                Some(relation.relname.clone()),
            )?;
        }
    }

    /* Lock heap before index to avoid deadlock. */
    if rel_id != old_rel_id {
        /*
         * If the OID isn't valid, it means the index was concurrently dropped,
         * which is not a problem for us; just return normally.
         */
        if OidIsValid(table_oid) {
            let guard = lmgr_seams::lock_relation_oid::call(table_oid, table_lockmode)?;
            guard.keep();
            *locked_table_oid = table_oid;
        }
    }

    Ok(())
}

/// `RangeVarGetRelidExtended(relation, lockmode, 0,
/// RangeVarCallbackForReindexIndex, &state)` — resolve+lock a `REINDEX INDEX`
/// target, running the reindex-index permission callback. Exposed to the
/// indexcmds command driver via the `range_var_get_relid_for_reindex_index`
/// seam.
pub fn RangeVarGetRelidForReindexIndex(
    mcx: Mcx<'_>,
    relation: &RangeVar,
    lockmode: LOCKMODE,
) -> PgResult<Oid> {
    /*
     * The heap lock level should match the table lock in reindex_index() for
     * the non-concurrent case (ShareLock) and the table locks used by
     * index_concurrently_*() for the concurrent case (ShareUpdateExclusiveLock).
     * We derive it from the index lock the caller asked for.
     */
    let table_lockmode = if lockmode == ShareUpdateExclusiveLock {
        ShareUpdateExclusiveLock
    } else {
        ShareLock
    };
    let mut locked_table_oid = InvalidOid;
    let mut callback =
        |relation: &RangeVar, rel_id: Oid, old_rel_id: Oid| -> PgResult<()> {
            RangeVarCallbackForReindexIndex(
                relation,
                rel_id,
                old_rel_id,
                table_lockmode,
                &mut locked_table_oid,
            )
        };
    RangeVarGetRelidExtended(mcx, relation, lockmode, 0, Some(&mut callback))
}

/* ===========================================================================
 * K1 owned-tree `RangeVar` node bridges + the three sequence.c-facing seams.
 *
 * The namespace owner still consumes `::types_tuple::access::RangeVar` (the
 * non-lifetime, owned-string struct that `RangeVarGetRelid{,Extended}` and
 * `RangeVarGetAndCheckCreationNamespace` operate on). The K1 owned-tree node
 * `::nodes::rawnodes::RangeVar<'mcx>` carries the same fields; we copy
 * them into the access struct so the existing core functions resolve it.
 * ======================================================================== */

/// Faithful field copy of a K1 owned-tree `RangeVar` node into the access-layer
/// `RangeVar` the namespace core consumes. `relname` is never NULL in a
/// well-formed parse node; `alias` is irrelevant to the lookup (the core never
/// reads it), so it is dropped.
fn rangevar_from_node(rv: &::nodes::rawnodes::RangeVar<'_>) -> RangeVar {
    RangeVar {
        catalogname: rv.catalogname.as_ref().map(|s| s.as_str().to_string()),
        schemaname: rv.schemaname.as_ref().map(|s| s.as_str().to_string()),
        relname: rv
            .relname
            .as_ref()
            .map(|s| s.as_str().to_string())
            .unwrap_or_default(),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
        ..RangeVar::default()
    }
}

/// `RangeVarGetRelid(makeRangeVarFromNameList(textToQualifiedNameList(name)),
/// lockmode, missing_ok)` (the SQL-function relation-name-to-OID idiom).
fn seam_range_var_get_relid_from_text(
    mcx: Mcx<'_>,
    name: &str,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<Oid> {
    /* textToQualifiedNameList(textval) — split the (possibly qualified) name
     * on '.' per SplitIdentifierString, downcasing/dequoting. */
    let parts = varlena_seams::text_to_qualified_name_list::call(mcx, name.as_bytes())?;
    let names: Vec<Option<String>> =
        parts.iter().map(|p| Some(p.as_str().to_string())).collect();
    let relation = makeRangeVarFromNameList(&names)?;
    RangeVarGetRelid(mcx, &relation, lockmode, missing_ok)
}

/// `RangeVarGetRelidExtended(relation, lockmode, flags, NULL, NULL)` — the
/// no-callback form used by VACUUM/ANALYZE relation expansion
/// (commands/vacuum.c `expand_vacuum_rel`). The vacuum-seams declaration drops
/// the caller's `Mcx` (a scratch context suffices: the only outputs are the
/// by-value `Oid` and the lock taken, neither context-bound) and the callback
/// (NULL on this path).
fn seam_vacuum_range_var_get_relid_extended(
    relation: ::nodes::rawnodes::RangeVar<'_>,
    lockmode: i32,
    rvr_opts: i32,
) -> PgResult<Oid> {
    let scratch = ::mcx::MemoryContext::new("vacuum RangeVarGetRelidExtended");
    let rv = rangevar_from_node(&relation);
    // rvr_opts carries the RVR_* flag bits (non-negative); the owner signature
    // types `flags` as u32.
    RangeVarGetRelidExtended(scratch.mcx(), &rv, lockmode, rvr_opts as u32, None)
}

/// `RangeVarGetAndCheckCreationNamespace(relation, NoLock, &existing_relid)`
/// (sequence.c `DefineSequence` if_not_exists pre-check) over the K1 node.
fn seam_range_var_get_and_check_creation_namespace(
    relation: &::nodes::rawnodes::RangeVar<'_>,
) -> PgResult<Oid> {
    let scratch = ::mcx::MemoryContext::new("RangeVarGetAndCheckCreationNamespace");
    let mut rv = rangevar_from_node(relation);
    let mut existing_relid: Oid = InvalidOid;
    RangeVarGetAndCheckCreationNamespace(
        scratch.mcx(),
        &mut rv,
        NoLock,
        Some(&mut existing_relid),
    )?;
    Ok(existing_relid)
}

/// `RangeVarGetRelidExtended(relation, ShareRowExclusiveLock,
/// missing_ok ? RVR_MISSING_OK : 0, RangeVarCallbackOwnsRelation, NULL)`
/// (sequence.c `AlterSequence` open-and-own-check) over the K1 node.
///
/// The `RangeVarCallbackOwnsRelation` callback is tablecmds.c's, not the
/// namespace's; this bridge only marshals (the callback's only input from the
/// `RangeVar` is `relname`) and delegates to the tablecmds owner via its seam.
fn seam_range_var_get_relid_owns_seq(
    relation: &::nodes::rawnodes::RangeVar<'_>,
    missing_ok: bool,
) -> PgResult<Oid> {
    let scratch = ::mcx::MemoryContext::new("RangeVarGetRelidOwnsSeq");
    let mcx = scratch.mcx();
    let rv = rangevar_from_node(relation);
    let flags = if missing_ok { RVR_MISSING_OK } else { 0 };
    let mut callback =
        |relation: &RangeVar, rel_id: Oid, old_rel_id: Oid| -> PgResult<()> {
            tablecmds_seams::range_var_callback_owns_relation::call(
                relation.relname.as_str(),
                rel_id,
                old_rel_id,
            )
        };
    RangeVarGetRelidExtended(
        mcx,
        &rv,
        ::types_storage::lock::ShareRowExclusiveLock,
        flags,
        Some(&mut callback),
    )
}

/// `RangeVarGetRelidExtended(stmt->relation, lockmode, 0,
/// RangeVarCallbackOwnsRelation, NULL)` (utility.c `ProcessUtilitySlow`,
/// the CREATE INDEX relation-OID lookup) over the K1 parse node.
///
/// The `RangeVarCallbackOwnsRelation` callback is tablecmds.c's; this bridge
/// only marshals (`relname` is the callback's sole `RangeVar` input) and
/// delegates to the tablecmds owner via its seam.
pub fn RangeVarGetRelidOwnsRelation<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &::nodes::rawnodes::RangeVar<'_>,
    lockmode: LOCKMODE,
) -> PgResult<Oid> {
    let rv = rangevar_from_node(relation);
    let mut callback =
        |relation: &RangeVar, rel_id: Oid, old_rel_id: Oid| -> PgResult<()> {
            tablecmds_seams::range_var_callback_owns_relation::call(
                relation.relname.as_str(),
                rel_id,
                old_rel_id,
            )
        };
    RangeVarGetRelidExtended(mcx, &rv, lockmode, 0, Some(&mut callback))
}

/// `RangeVarGetRelidExtended` — given a `RangeVar` describing an existing
/// relation, select the proper namespace and look up the relation OID.
pub fn RangeVarGetRelidExtended(
    mcx: Mcx<'_>,
    relation: &RangeVar,
    lockmode: LOCKMODE,
    flags: u32,
    mut callback: RangeVarGetRelidCallback,
) -> PgResult<Oid> {
    let mut inval_count: u64;
    let mut relId: Oid;
    let mut oldRelId: Oid = InvalidOid;
    let mut retry = false;
    /* The currently-held relation lock (C: an entry in the backend's lock
     * table, released by transaction abort on error). */
    let mut held_lock: Option<lmgr_seams::LockGuard> = None;
    let missing_ok = (flags & RVR_MISSING_OK) != 0;

    /* verify that flags do no conflict */
    debug_assert!(!((flags & RVR_NOWAIT) != 0 && (flags & RVR_SKIP_LOCKED) != 0));

    /*
     * We check the catalog name and then ignore it.
     */
    if let Some(catalogname) = relation.catalogname.as_deref() {
        if catalogname_differs_from_database(mcx, catalogname)? {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "cross-database references are not implemented: \"{}.{}.{}\"",
                    catalogname,
                    relation.schemaname.as_deref().unwrap_or_default(),
                    relation.relname.as_str()
                ))
                .finish(here("RangeVarGetRelidExtended"))
                .map(|()| InvalidOid);
        }
    }

    /*
     * DDL operations can change the results of a name lookup.  Since all such
     * operations will generate invalidation messages, we keep track of
     * whether any such messages show up while we're performing the operation,
     * and retry until either (1) no more invalidation messages show up or (2)
     * the answer doesn't change.
     */
    loop {
        /*
         * Remember this value, so that, after looking up the relation name
         * and locking its OID, we can check whether any invalidation messages
         * have been processed that might require a do-over.
         */
        inval_count = sinval_seams::shared_invalid_message_counter::call();

        /*
         * Some non-default relpersistence value may have been specified.  The
         * parser never generates such a RangeVar in simple DML, but it can
         * happen in contexts such as "CREATE TEMP TABLE foo (f1 int PRIMARY
         * KEY)".  Such a command will generate an added CREATE INDEX
         * operation, which must be careful to find the temp table, even when
         * pg_temp is not first in the search path.
         */
        if relation.relpersistence == RELPERSISTENCE_TEMP {
            if !OidIsValid(my_temp_namespace()) {
                relId = InvalidOid; /* this probably can't happen? */
            } else {
                if let Some(schemaname) = relation.schemaname.as_deref() {
                    let namespaceId = LookupExplicitNamespace(schemaname, missing_ok)?;

                    /*
                     * For missing_ok, allow a non-existent schema name to
                     * return InvalidOid.
                     */
                    if namespaceId != my_temp_namespace() {
                        return ereport(ERROR)
                            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                            .errmsg("temporary tables cannot specify a schema name")
                            .finish(here("RangeVarGetRelidExtended"))
                            .map(|()| InvalidOid);
                    }
                }

                relId = lsyscache_seams::get_relname_relid::call(
                    relation.relname.as_str(),
                    my_temp_namespace(),
                )?;
            }
        } else if let Some(schemaname) = relation.schemaname.as_deref() {
            /* use exact schema given */
            let namespaceId = LookupExplicitNamespace(schemaname, missing_ok)?;
            if missing_ok && !OidIsValid(namespaceId) {
                relId = InvalidOid;
            } else {
                relId = lsyscache_seams::get_relname_relid::call(
                    relation.relname.as_str(),
                    namespaceId,
                )?;
            }
        } else {
            /* search the namespace path */
            relId = RelnameGetRelid(mcx, relation.relname.as_str())?;
        }

        /*
         * Invoke caller-supplied callback, if any.
         */
        if let Some(cb) = callback.as_deref_mut() {
            cb(relation, relId, oldRelId)?;
        }

        /*
         * If no lock requested, we assume the caller knows what they're
         * doing.  They should have already acquired a heavyweight lock on
         * this relation earlier in the processing of this same statement, so
         * it wouldn't be appropriate to AcceptInvalidationMessages() here.
         */
        if lockmode == NoLock {
            break;
        }

        /*
         * If, upon retry, we get back the same OID we did last time, then the
         * invalidation messages we processed did not change the final answer.
         * So we're done.
         *
         * If we got a different OID, we've locked the relation that used to
         * have this name rather than the one that does now.  So release the
         * lock.
         */
        if retry {
            if relId == oldRelId {
                break;
            }
            if OidIsValid(oldRelId) {
                if let Some(guard) = held_lock.take() {
                    guard.release()?;
                }
            }
        }

        /*
         * Lock relation.  This will also accept any pending invalidation
         * messages.  If we got back InvalidOid, indicating not found, then
         * there's nothing to lock, but we accept invalidation messages, so
         * that our lookup result is at least self-consistent.
         */
        if !OidIsValid(relId) {
            inval_seams::accept_invalidation_messages::call()?;
        } else if (flags & (RVR_NOWAIT | RVR_SKIP_LOCKED)) == 0 {
            held_lock = Some(lmgr_seams::lock_relation_oid::call(relId, lockmode)?);
        } else if let Some(guard) =
            lmgr_seams::conditional_lock_relation_oid::call(relId, lockmode)?
        {
            held_lock = Some(guard);
        } else {
            let elevel = if (flags & RVR_SKIP_LOCKED) != 0 {
                DEBUG1
            } else {
                ERROR
            };

            if let Some(schemaname) = relation.schemaname.as_deref() {
                ereport(elevel)
                    .errcode(ERRCODE_LOCK_NOT_AVAILABLE)
                    .errmsg(format!(
                        "could not obtain lock on relation \"{}.{}\"",
                        schemaname,
                        relation.relname.as_str()
                    ))
                    .finish(here("RangeVarGetRelidExtended"))?;
            } else {
                ereport(elevel)
                    .errcode(ERRCODE_LOCK_NOT_AVAILABLE)
                    .errmsg(format!(
                        "could not obtain lock on relation \"{}\"",
                        relation.relname.as_str()
                    ))
                    .finish(here("RangeVarGetRelidExtended"))?;
            }

            return Ok(InvalidOid);
        }

        /*
         * If no invalidation message were processed, we're done!
         */
        if inval_count == sinval_seams::shared_invalid_message_counter::call() {
            break;
        }

        /*
         * Something may have changed.  Let's repeat the name lookup, making
         * sure this time that we lock the right thing.
         */
        retry = true;
        oldRelId = relId;
    }

    if !OidIsValid(relId) {
        let elevel = if missing_ok { DEBUG1 } else { ERROR };

        if let Some(schemaname) = relation.schemaname.as_deref() {
            ereport(elevel)
                .errcode(ERRCODE_UNDEFINED_TABLE)
                .errmsg(format!(
                    "relation \"{}.{}\" does not exist",
                    schemaname,
                    relation.relname.as_str()
                ))
                .finish(here("RangeVarGetRelidExtended"))?;
        } else {
            ereport(elevel)
                .errcode(ERRCODE_UNDEFINED_TABLE)
                .errmsg(format!(
                    "relation \"{}\" does not exist",
                    relation.relname.as_str()
                ))
                .finish(here("RangeVarGetRelidExtended"))?;
        }
    }
    /* C returns with the lock held until transaction end; the guard moves to
     * the (future) transaction owner. */
    if let Some(guard) = held_lock {
        guard.keep();
    }
    Ok(relId)
}

/* ===========================================================================
 * RangeVarGetCreationNamespace (C lines 654-710)
 * ======================================================================== */

/// `RangeVarGetCreationNamespace` — given a `RangeVar` describing a
/// to-be-created relation, choose which namespace to create it in.
pub fn RangeVarGetCreationNamespace(mcx: Mcx<'_>, newRelation: &RangeVar) -> PgResult<Oid> {
    let namespaceId: Oid;

    /*
     * We check the catalog name and then ignore it.
     */
    if let Some(catalogname) = newRelation.catalogname.as_deref() {
        if catalogname_differs_from_database(mcx, catalogname)? {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "cross-database references are not implemented: \"{}.{}.{}\"",
                    catalogname,
                    newRelation.schemaname.as_deref().unwrap_or_default(),
                    newRelation.relname.as_str()
                ))
                .finish(here("RangeVarGetCreationNamespace"))
                .map(|()| InvalidOid);
        }
    }

    if let Some(schemaname) = newRelation.schemaname.as_deref() {
        /* check for pg_temp alias */
        if schemaname == "pg_temp" {
            /* Initialize temp namespace */
            AccessTempTableNamespace(mcx, false)?;
            return Ok(my_temp_namespace());
        }
        /* use exact schema given */
        namespaceId = get_namespace_oid(schemaname, false)?;
        /* we do not check for USAGE rights here! */
    } else if newRelation.relpersistence == RELPERSISTENCE_TEMP {
        /* Initialize temp namespace */
        AccessTempTableNamespace(mcx, false)?;
        return Ok(my_temp_namespace());
    } else {
        /* use the default creation namespace */
        recomputeNamespacePath(mcx)?;
        if STATE.with(|s| s.borrow().active_temp_creation_pending) {
            /* Need to initialize temp namespace */
            AccessTempTableNamespace(mcx, true)?;
            return Ok(my_temp_namespace());
        }
        namespaceId = STATE.with(|s| s.borrow().active_creation_namespace);
        if !OidIsValid(namespaceId) {
            return ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_SCHEMA)
                .errmsg("no schema has been selected to create in")
                .finish(here("RangeVarGetCreationNamespace"))
                .map(|()| InvalidOid);
        }
    }

    /* Note: callers will check for CREATE rights when appropriate */

    Ok(namespaceId)
}

/* ===========================================================================
 * RangeVarGetAndCheckCreationNamespace (C lines 739-839)
 * ======================================================================== */

/// `RangeVarGetAndCheckCreationNamespace` — return the namespace to create
/// in, after a CREATE-rights check and (optionally) locking the existing
/// relation of the same name.
pub fn RangeVarGetAndCheckCreationNamespace(
    mcx: Mcx<'_>,
    relation: &mut RangeVar,
    lockmode: LOCKMODE,
    existing_relation_id: Option<&mut Oid>,
) -> PgResult<Oid> {
    let mut inval_count: u64;
    let mut relid: Oid;
    let mut oldrelid: Oid = InvalidOid;
    let mut nspid: Oid;
    let mut oldnspid: Oid = InvalidOid;
    let mut retry = false;
    let want_existing = existing_relation_id.is_some();
    let mut held_nsp_lock: Option<lmgr_seams::LockGuard> = None;
    let mut held_rel_lock: Option<lmgr_seams::LockGuard> = None;

    /*
     * We check the catalog name and then ignore it.
     */
    if let Some(catalogname) = relation.catalogname.as_deref() {
        if catalogname_differs_from_database(mcx, catalogname)? {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "cross-database references are not implemented: \"{}.{}.{}\"",
                    catalogname,
                    relation.schemaname.as_deref().unwrap_or_default(),
                    relation.relname.as_str()
                ))
                .finish(here("RangeVarGetAndCheckCreationNamespace"))
                .map(|()| InvalidOid);
        }
    }

    /*
     * As in RangeVarGetRelidExtended(), we guard against concurrent DDL
     * operations by tracking whether any invalidation messages are processed
     * while we're doing the name lookups and acquiring locks.
     */
    loop {
        inval_count = sinval_seams::shared_invalid_message_counter::call();

        /* Look up creation namespace and check for existing relation. */
        nspid = RangeVarGetCreationNamespace(mcx, relation)?;
        debug_assert!(OidIsValid(nspid));
        if want_existing {
            relid = lsyscache_seams::get_relname_relid::call(
                relation.relname.as_str(),
                nspid,
            )?;
        } else {
            relid = InvalidOid;
        }

        /*
         * In bootstrap processing mode, we don't bother with permissions or
         * locking.  Permissions might not be working yet, and locking is
         * unnecessary.
         */
        if miscinit_seams::is_bootstrap_processing_mode::call() {
            break;
        }

        /* Check namespace permissions. */
        let aclresult =
            namespace_aclcheck(nspid, miscinit_seams::get_user_id::call(), ACL_CREATE)?;
        if aclresult != AclResult::AclcheckOk {
            aclcheck_error_schema(
                aclresult,
                lsyscache_seams::get_namespace_name::call(mcx, nspid)?
                    .map(|s| s.as_str().to_string()),
            )?;
        }

        if retry {
            /* If nothing changed, we're done. */
            if relid == oldrelid && nspid == oldnspid {
                break;
            }
            /* If creation namespace has changed, give up old lock. */
            if nspid != oldnspid {
                if let Some(guard) = held_nsp_lock.take() {
                    guard.release()?;
                }
            }
            /* If name points to something different, give up old lock. */
            if relid != oldrelid && OidIsValid(oldrelid) && lockmode != NoLock {
                if let Some(guard) = held_rel_lock.take() {
                    guard.release()?;
                }
            }
        }

        /* Lock namespace. */
        if nspid != oldnspid {
            held_nsp_lock = Some(lmgr_seams::lock_database_object::call(
                NAMESPACE_RELATION_ID,
                nspid,
                0,
                AccessShareLock,
            )?);
        }

        /* Lock relation, if required if and we have permission. */
        if lockmode != NoLock && OidIsValid(relid) {
            if !aclchk_seams::object_ownercheck::call(
                RELATION_RELATION_ID,
                relid,
                miscinit_seams::get_user_id::call(),
            )? {
                aclchk_seams::aclcheck_error::call(
                    ACLCHECK_NOT_OWNER,
                    objectaddress_seams::get_relkind_objtype::call(
                        lsyscache_seams::get_rel_relkind::call(relid)?,
                    ),
                    Some(relation.relname.clone()),
                )?;
            }
            if relid != oldrelid {
                held_rel_lock = Some(lmgr_seams::lock_relation_oid::call(relid, lockmode)?);
            }
        }

        /* If no invalidation message were processed, we're done! */
        if inval_count == sinval_seams::shared_invalid_message_counter::call() {
            break;
        }

        /* Something may have changed, so recheck our work. */
        retry = true;
        oldrelid = relid;
        oldnspid = nspid;
    }

    RangeVarAdjustRelationPersistence(mcx, relation, nspid)?;
    if let Some(out) = existing_relation_id {
        *out = relid;
    }
    /* C returns with the locks held until transaction end; the guards move
     * to the (future) transaction owner. */
    if let Some(guard) = held_nsp_lock {
        guard.keep();
    }
    if let Some(guard) = held_rel_lock {
        guard.keep();
    }
    Ok(nspid)
}

/* ===========================================================================
 * RangeVarAdjustRelationPersistence (C lines 846-877)
 * ======================================================================== */

/// `RangeVarAdjustRelationPersistence` — adjust the relpersistence of an
/// about-to-be-created relation based on the creation namespace, and throw
/// an error for invalid combinations.
pub fn RangeVarAdjustRelationPersistence(
    mcx: Mcx<'_>,
    newRelation: &mut RangeVar,
    nspid: Oid,
) -> PgResult<()> {
    match newRelation.relpersistence {
        RELPERSISTENCE_TEMP => {
            if !isTempOrTempToastNamespace(nspid)? {
                if isAnyTempNamespace(mcx, nspid)? {
                    return ereport(ERROR)
                        .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                        .errmsg("cannot create relations in temporary schemas of other sessions")
                        .finish(here("RangeVarAdjustRelationPersistence"));
                } else {
                    return ereport(ERROR)
                        .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                        .errmsg("cannot create temporary relation in non-temporary schema")
                        .finish(here("RangeVarAdjustRelationPersistence"));
                }
            }
        }
        RELPERSISTENCE_PERMANENT => {
            if isTempOrTempToastNamespace(nspid)? {
                newRelation.relpersistence = RELPERSISTENCE_TEMP;
            } else if isAnyTempNamespace(mcx, nspid)? {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                    .errmsg("cannot create relations in temporary schemas of other sessions")
                    .finish(here("RangeVarAdjustRelationPersistence"));
            }
        }
        _ => {
            if isAnyTempNamespace(mcx, nspid)? {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                    .errmsg("only temporary relations may be created in temporary schemas")
                    .finish(here("RangeVarAdjustRelationPersistence"));
            }
        }
    }
    Ok(())
}

/* ===========================================================================
 * RelnameGetRelid (C lines 885-903)
 * ======================================================================== */

/// `RelnameGetRelid` — try to resolve an unqualified relation name.
/// Returns OID if relation found in search path, else InvalidOid.
pub fn RelnameGetRelid(mcx: Mcx<'_>, relname: &str) -> PgResult<Oid> {
    recomputeNamespacePath(mcx)?;

    for namespaceId in active_search_path() {
        let relid = lsyscache_seams::get_relname_relid::call(relname, namespaceId)?;
        if OidIsValid(relid) {
            return Ok(relid);
        }
    }

    /* Not found in path */
    Ok(InvalidOid)
}

/* ===========================================================================
 * RelationIsVisible / RelationIsVisibleExt (C lines 913-987)
 * ======================================================================== */

/// `RelationIsVisible` — whether a relation is visible in the search path.
pub fn RelationIsVisible(mcx: Mcx<'_>, relid: Oid) -> PgResult<bool> {
    RelationIsVisibleExt(mcx, relid, None)
}

/// `RelationIsVisibleExt` — as above, but if `is_missing` is given, a lookup
/// failure sets `*is_missing` instead of raising.
fn RelationIsVisibleExt(mcx: Mcx<'_>, relid: Oid, is_missing: Option<&mut bool>) -> PgResult<bool> {
    let visible: bool;

    let reltup = match syscache_seams::relation_namespace_and_name::call(mcx, relid)? {
        Some(t) => t,
        None => {
            if let Some(m) = is_missing {
                *m = true;
                return Ok(false);
            }
            return elog_error(format!("cache lookup failed for relation {relid}"));
        }
    };

    recomputeNamespacePath(mcx)?;

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    let relnamespace = reltup.namespace;
    let path = active_search_path();
    if relnamespace != PG_CATALOG_NAMESPACE && !list_member_oid(&path, relnamespace) {
        visible = false;
    } else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another relation of the same name earlier in the path. So
         * we must do a slow check for conflicting relations.
         */
        let relname = reltup.name.as_str();
        let mut found = false;
        for namespaceId in &path {
            let namespaceId = *namespaceId;
            if namespaceId == relnamespace {
                /* Found it first in path */
                found = true;
                break;
            }
            if OidIsValid(lsyscache_seams::get_relname_relid::call(relname, namespaceId)?) {
                /* Found something else first in path */
                break;
            }
        }
        visible = found;
    }

    Ok(visible)
}

/* ===========================================================================
 * TypenameGetTypid / TypenameGetTypidExtended (C lines 995-1031)
 * ======================================================================== */

/// `TypenameGetTypid` — wrapper for binary compatibility.
pub fn TypenameGetTypid(mcx: Mcx<'_>, typname: &str) -> PgResult<Oid> {
    TypenameGetTypidExtended(mcx, typname, true)
}

/// `TypenameGetTypidExtended` — try to resolve an unqualified datatype name.
/// Returns OID if type found in search path, else InvalidOid.
pub fn TypenameGetTypidExtended(mcx: Mcx<'_>, typname: &str, temp_ok: bool) -> PgResult<Oid> {
    recomputeNamespacePath(mcx)?;

    for namespaceId in active_search_path() {
        if !temp_ok && namespaceId == my_temp_namespace() {
            continue; /* do not look in temp namespace */
        }

        let typid = syscache_seams::get_type_oid::call(typname, namespaceId)?;
        if OidIsValid(typid) {
            return Ok(typid);
        }
    }

    /* Not found in path */
    Ok(InvalidOid)
}

/* ===========================================================================
 * TypeIsVisible / TypeIsVisibleExt (C lines 1040-1116)
 * ======================================================================== */

/// `TypeIsVisible` — whether a type is visible in the search path.
pub fn TypeIsVisible(mcx: Mcx<'_>, typid: Oid) -> PgResult<bool> {
    TypeIsVisibleExt(mcx, typid, None)
}

/// `TypeIsVisibleExt`.
fn TypeIsVisibleExt(mcx: Mcx<'_>, typid: Oid, is_missing: Option<&mut bool>) -> PgResult<bool> {
    let visible: bool;

    let typtup = match syscache_seams::type_namespace_and_name::call(mcx, typid)? {
        Some(t) => t,
        None => {
            if let Some(m) = is_missing {
                *m = true;
                return Ok(false);
            }
            return elog_error(format!("cache lookup failed for type {typid}"));
        }
    };

    recomputeNamespacePath(mcx)?;

    let typnamespace = typtup.namespace;
    let path = active_search_path();
    if typnamespace != PG_CATALOG_NAMESPACE && !list_member_oid(&path, typnamespace) {
        visible = false;
    } else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another type of the same name earlier in the path.
         */
        let typname = typtup.name.as_str();
        let mut found = false;
        for namespaceId in &path {
            let namespaceId = *namespaceId;
            if namespaceId == typnamespace {
                /* Found it first in path */
                found = true;
                break;
            }
            if syscache_seams::type_exists::call(typname, namespaceId)? {
                /* Found something else first in path */
                break;
            }
        }
        visible = found;
    }

    Ok(visible)
}

/* ===========================================================================
 * FuncnameGetCandidates (C lines 1192-1561)
 * ======================================================================== */

/// `FuncnameGetCandidates` — given a possibly-qualified function name and
/// argument count, retrieve a list of the possible matches.
pub fn FuncnameGetCandidates<'mcx>(
    mcx: Mcx<'mcx>,
    names: NameList,
    nargs: i32,
    argnames: &[String],
    expand_variadic: bool,
    expand_defaults: bool,
    include_out_arguments: bool,
    missing_ok: bool,
) -> PgResult<FuncCandidateList<'mcx>> {
    let mut resultList: FuncCandidateList<'mcx> = PgVec::new_in(mcx);
    let mut any_special = false;
    let namespaceId: Oid;

    /* check for caller error */
    debug_assert!(nargs >= 0 || !(expand_variadic | expand_defaults));

    /* deconstruct the name list */
    let (schemaname, funcname) = DeconstructQualifiedName(mcx, names)?;

    if let Some(schemaname) = schemaname {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok)?;
        if !OidIsValid(namespaceId) {
            return Ok(PgVec::new_in(mcx));
        }
    } else {
        /* flag to indicate we need namespace search */
        namespaceId = InvalidOid;
        recomputeNamespacePath(mcx)?;
    }

    /* Search syscache by name only */
    let (catlist, catlist_ordered) = syscache_seams::proc_catlist::call(mcx, funcname)?;
    let path = active_search_path();

    for i in 0..catlist.len() {
        let procform = &catlist[i];
        /* C: a pointer swap between proargtypes.values and the
         * proallargtypes array data — a borrowed slice here. */
        let mut proargtypes: &[Oid] = &procform.proargtypes;
        let mut pronargs = procform.pronargs;
        let effective_nargs: i32;
        let mut pathpos = 0;
        let variadic;
        let use_defaults;
        let va_elem_type;
        let mut argnumbers: PgVec<'mcx, i32> = PgVec::new_in(mcx);

        if OidIsValid(namespaceId) {
            /* Consider only procs in specified namespace */
            if procform.pronamespace != namespaceId {
                continue;
            }
        } else {
            /*
             * Consider only procs that are in the search path and are not in
             * the temp namespace.
             */
            let mut in_path = false;
            for nsp in &path {
                if procform.pronamespace == *nsp && procform.pronamespace != my_temp_namespace() {
                    in_path = true;
                    break;
                }
                pathpos += 1;
            }
            if !in_path {
                continue; /* proc is not in search path */
            }
        }

        /*
         * If we are asked to match to OUT arguments, then use the
         * proallargtypes array (which includes those); otherwise use
         * proargtypes (which doesn't). Of course, if proallargtypes is null,
         * we always use proargtypes.
         */
        if include_out_arguments {
            if let Some(arr) = &procform.proallargtypes {
                pronargs = arr.dim0;
                if arr.ndim != 1 || pronargs < 0 || arr.hasnull || arr.elemtype != OIDOID {
                    return elog_error(
                        "proallargtypes is not a 1-D Oid array or it contains nulls".to_string(),
                    );
                }
                debug_assert!(pronargs >= procform.pronargs);
                proargtypes = &arr.values;
            }
        }

        if !argnames.is_empty() {
            /*
             * Call uses named or mixed notation
             *
             * Named or mixed notation can match a variadic function only if
             * expand_variadic is off; otherwise there is no way to match the
             * presumed-nameless parameters expanded from the variadic array.
             */
            if OidIsValid(procform.provariadic) && expand_variadic {
                continue;
            }
            va_elem_type = InvalidOid;
            variadic = false;

            /*
             * Check argument count.
             */
            debug_assert!(nargs >= 0); /* -1 not supported with argnames */

            if pronargs > nargs && expand_defaults {
                /* Ignore if not enough default expressions */
                if nargs + procform.pronargdefaults < pronargs {
                    continue;
                }
                use_defaults = true;
            } else {
                use_defaults = false;
            }

            /* Ignore if it doesn't match requested argument count */
            if pronargs != nargs && !use_defaults {
                continue;
            }

            /* Check for argument name match, generate positional mapping */
            if !MatchNamedCall(
                mcx,
                procform,
                nargs,
                argnames,
                include_out_arguments,
                pronargs,
                &mut argnumbers,
            )? {
                continue;
            }

            /* Named argument matching is always "special" */
            any_special = true;
        } else {
            /*
             * Call uses positional notation
             *
             * Check if function is variadic, and get variadic element type
             * if so. If expand_variadic is false, we should just ignore
             * variadic-ness.
             */
            if pronargs <= nargs && expand_variadic {
                va_elem_type = procform.provariadic;
                variadic = OidIsValid(va_elem_type);
                any_special |= variadic;
            } else {
                va_elem_type = InvalidOid;
                variadic = false;
            }

            /*
             * Check if function can match by using parameter defaults.
             */
            if pronargs > nargs && expand_defaults {
                /* Ignore if not enough default expressions */
                if nargs + procform.pronargdefaults < pronargs {
                    continue;
                }
                use_defaults = true;
                any_special = true;
            } else {
                use_defaults = false;
            }

            /* Ignore if it doesn't match requested argument count */
            if nargs >= 0 && pronargs != nargs && !variadic && !use_defaults {
                continue;
            }
        }

        /*
         * We must compute the effective argument list so that we can easily
         * compare it to earlier results.
         */
        effective_nargs = pronargs.max(nargs);
        let args_len = effective_nargs.max(0) as usize;
        let mut args: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, args_len)?;
        args.resize(args_len, InvalidOid);
        /* C moves the palloc'd argnumbers pointer into the candidate. */
        let mut newResult = FuncCandidate {
            pathpos,
            oid: procform.oid,
            nominalnargs: pronargs,
            nargs: effective_nargs,
            nvargs: 0,
            ndargs: 0,
            argnumbers,
            args,
        };
        if !newResult.argnumbers.is_empty() {
            /* Re-order the argument types into call's logical order */
            for j in 0..pronargs as usize {
                let an = newResult.argnumbers[j] as usize;
                newResult.args[j] = proargtypes[an];
            }
        } else {
            /* Simple positional case, just copy proargtypes as-is */
            for j in 0..pronargs as usize {
                newResult.args[j] = proargtypes[j];
            }
        }
        if variadic {
            newResult.nvargs = effective_nargs - pronargs + 1;
            /* Expand variadic argument into N copies of element type */
            for j in (pronargs - 1)..effective_nargs {
                newResult.args[j as usize] = va_elem_type;
            }
        } else {
            newResult.nvargs = 0;
        }
        newResult.ndargs = if use_defaults { pronargs - nargs } else { 0 };

        /*
         * Does it have the same arguments as something we already accepted?
         * If so, decide what to do to avoid returning duplicate argument
         * lists. We can skip this check for the single-namespace case if no
         * special (named, variadic or defaults) match has been made, since
         * then the unique index on pg_proc guarantees all the matches have
         * different argument lists.
         */
        if !resultList.is_empty() && (any_special || !OidIsValid(namespaceId)) {
            /*
             * If we have an ordered list from SearchSysCacheList (the normal
             * case), then any conflicting proc must immediately adjoin this
             * one in the list, so we only need to look at the newest result
             * item. If we have an unordered list, we have to scan the whole
             * result list. Also, if either the current candidate or any
             * previous candidate is a special match, we can't assume that
             * conflicts are adjacent.
             *
             * We ignore defaulted arguments in deciding what is a match.
             */
            let prevResult: Option<usize>;

            if catlist_ordered && !any_special {
                /* ndargs must be 0 if !any_special */
                if effective_nargs == resultList[0].nargs
                    && oid_args_equal(&newResult.args, &resultList[0].args, effective_nargs as usize)
                {
                    prevResult = Some(0);
                } else {
                    prevResult = None;
                }
            } else {
                let cmp_nargs = newResult.nargs - newResult.ndargs;
                let mut found = None;
                for (idx, prev) in resultList.iter().enumerate() {
                    if cmp_nargs == prev.nargs - prev.ndargs
                        && oid_args_equal(&newResult.args, &prev.args, cmp_nargs as usize)
                    {
                        found = Some(idx);
                        break;
                    }
                }
                prevResult = found;
            }

            if let Some(prev_idx) = prevResult {
                /*
                 * We have a match with a previous result. Decide which one to
                 * keep, or mark it ambiguous if we can't decide. The logic
                 * here is preference > 0 means prefer the old result,
                 * preference < 0 means prefer the new, preference = 0 means
                 * ambiguous.
                 */
                let preference: i32;

                let prev_pathpos = resultList[prev_idx].pathpos;
                let prev_nvargs = resultList[prev_idx].nvargs;
                if pathpos != prev_pathpos {
                    /*
                     * Prefer the one that's earlier in the search path.
                     */
                    preference = pathpos - prev_pathpos;
                } else if variadic && prev_nvargs == 0 {
                    /*
                     * With variadic functions we could have, for example,
                     * both foo(numeric) and foo(variadic numeric[]) in the
                     * same namespace; if so we prefer the non-variadic match
                     * on efficiency grounds.
                     */
                    preference = 1;
                } else if !variadic && prev_nvargs > 0 {
                    preference = -1;
                } else {
                    /* We can't decide. */
                    preference = 0;
                }

                if preference > 0 {
                    /* keep previous result */
                    continue;
                } else if preference < 0 {
                    /* remove previous result from the list */
                    resultList.remove(prev_idx);
                    /* fall through to add newResult to list */
                } else {
                    /* mark old result as ambiguous, discard new */
                    resultList[prev_idx].oid = InvalidOid;
                    continue;
                }
            }
        }

        /*
         * Okay to add it to result list
         */
        resultList
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<FuncCandidate>()))?;
        resultList.insert(0, newResult);
    }

    Ok(resultList)
}

/// `memcmp(a, b, n * sizeof(Oid)) == 0` over two `args` slices.
fn oid_args_equal(a: &[Oid], b: &[Oid], n: usize) -> bool {
    a[..n] == b[..n]
}

/* ===========================================================================
 * MatchNamedCall (C lines 1585-1687)
 * ======================================================================== */

/// `MatchNamedCall` — given a pg_proc row and a call's list of argument
/// names, check whether the function could match the call. On match, fills
/// `argnumbers` with the mapping from call argument positions to actual
/// function argument numbers (defaulted arguments included, after the last
/// supplied argument).
fn MatchNamedCall<'mcx>(
    mcx: Mcx<'mcx>,
    procform: &ProcRow<'_>,
    nargs: i32,
    argnames: &[String],
    include_out_arguments: bool,
    pronargs: i32,
    argnumbers: &mut PgVec<'mcx, i32>,
) -> PgResult<bool> {
    let numposargs = nargs - argnames.len() as i32;
    let pronallargs: i32;

    let mut arggiven = [false; FUNC_MAX_ARGS];
    let mut ap: i32; /* call args position */
    let mut pp: i32; /* proargs position */

    debug_assert!(!argnames.is_empty());
    debug_assert!(numposargs >= 0);
    debug_assert!(nargs <= pronargs);

    /* Ignore this function if its proargnames is null */
    if syscache_seams::proc_proargnames_isnull::call(procform.oid)? {
        return Ok(false);
    }

    /* OK, let's extract the argument names and types */
    let info = funcapi_seams::get_func_arg_info::call(mcx, procform.oid)?;
    let FuncArgInfo {
        argtypes: p_argtypes,
        argnames: p_argnames,
        argmodes: p_argmodes,
    } = info;
    pronallargs = p_argtypes.len() as i32;
    debug_assert!(!p_argnames.is_empty());

    debug_assert!(if include_out_arguments {
        pronargs == pronallargs
    } else {
        pronargs <= pronallargs
    });

    /* initialize state for matching */
    *argnumbers = {
        let n = pronargs.max(0) as usize;
        let mut v = vec_with_capacity_in(mcx, n)?;
        v.resize(n, 0);
        v
    };
    for slot in arggiven.iter_mut().take(pronargs as usize) {
        *slot = false;
    }

    /* there are numposargs positional args before the named args */
    ap = 0;
    while ap < numposargs {
        argnumbers[ap as usize] = ap;
        arggiven[ap as usize] = true;
        ap += 1;
    }

    /* now examine the named args */
    for argname in argnames {
        let mut found;

        pp = 0;
        found = false;
        for i in 0..pronallargs as usize {
            /* consider only input params, except with include_out_arguments */
            if !include_out_arguments
                && !p_argmodes.is_empty()
                && (p_argmodes[i] != FUNC_PARAM_IN
                    && p_argmodes[i] != FUNC_PARAM_INOUT
                    && p_argmodes[i] != FUNC_PARAM_VARIADIC)
            {
                continue;
            }
            if let Some(n) = &p_argnames[i] {
                if n.as_str() == argname {
                    /* fail if argname matches a positional argument */
                    if arggiven[pp as usize] {
                        return Ok(false);
                    }
                    arggiven[pp as usize] = true;
                    argnumbers[ap as usize] = pp;
                    found = true;
                    break;
                }
            }
            /* increase pp only for considered parameters */
            pp += 1;
        }
        /* if name isn't in proargnames, fail */
        if !found {
            return Ok(false);
        }
        ap += 1;
    }

    debug_assert_eq!(ap, nargs); /* processed all actual parameters */

    /* Check for default arguments */
    if nargs < pronargs {
        let first_arg_with_default = pronargs - procform.pronargdefaults;

        pp = numposargs;
        while pp < pronargs {
            if arggiven[pp as usize] {
                pp += 1;
                continue;
            }
            /* fail if arg not given and no default available */
            if pp < first_arg_with_default {
                return Ok(false);
            }
            argnumbers[ap as usize] = pp;
            ap += 1;
            pp += 1;
        }
    }

    debug_assert_eq!(ap, pronargs); /* processed all function parameters */

    Ok(true)
}

/* ===========================================================================
 * FunctionIsVisible / FunctionIsVisibleExt (C lines 1696-1770)
 * ======================================================================== */

/// `FunctionIsVisible` — whether a function is visible in the search path.
pub fn FunctionIsVisible(mcx: Mcx<'_>, funcid: Oid) -> PgResult<bool> {
    FunctionIsVisibleExt(mcx, funcid, None)
}

/// `FunctionIsVisibleExt`.
fn FunctionIsVisibleExt(mcx: Mcx<'_>, funcid: Oid, is_missing: Option<&mut bool>) -> PgResult<bool> {
    let visible: bool;

    let procform = match syscache_seams::proc_row_by_oid::call(mcx, funcid)? {
        Some(p) => p,
        None => {
            if let Some(m) = is_missing {
                *m = true;
                return Ok(false);
            }
            return elog_error(format!("cache lookup failed for function {funcid}"));
        }
    };

    recomputeNamespacePath(mcx)?;

    let pronamespace = procform.pronamespace;
    let path = active_search_path();
    if pronamespace != PG_CATALOG_NAMESPACE && !list_member_oid(&path, pronamespace) {
        visible = false;
    } else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another proc of the same name and arguments earlier in
         * the path. So we must do a slow check to see if this is the same
         * proc that would be found by FuncnameGetCandidates.
         */
        let proname = procform.proname.as_str();
        let nargs = procform.pronargs;
        let mut found = false;

        /* C: list_make1(makeString(proname)) in the current context. */
        let names_list = [Some(proname.to_string())];
        let clist =
            FuncnameGetCandidates(mcx, &names_list, nargs, &[], false, false, false, false)?;

        for cand in &clist {
            if oid_args_equal(&cand.args, &procform.proargtypes, nargs as usize) {
                /* Found the expected entry; is it the right proc? */
                found = cand.oid == funcid;
                break;
            }
        }
        visible = found;
    }

    Ok(visible)
}

/* ===========================================================================
 * OpernameGetOprid (C lines 1785-1868)
 * ======================================================================== */

/// `OpernameGetOprid` — given a possibly-qualified operator name and exact
/// input datatypes, look up the operator. Returns InvalidOid if not found.
pub fn OpernameGetOprid(
    mcx: Mcx<'_>,
    names: NameList,
    oprleft: Oid,
    oprright: Oid,
) -> PgResult<Oid> {
    /* deconstruct the name list */
    let (schemaname, opername) = DeconstructQualifiedName(mcx, names)?;

    if let Some(schemaname) = schemaname {
        /* search only in exact schema given */
        let namespaceId = LookupExplicitNamespace(schemaname, true)?;
        if OidIsValid(namespaceId) {
            let result = syscache_seams::oper_exact::call(opername, oprleft, oprright, namespaceId)?;
            if OidIsValid(result) {
                return Ok(result);
            }
        }

        return Ok(InvalidOid);
    }

    /* Search syscache by name and argument types */
    let (catlist, _ordered) = syscache_seams::oper_catlist3::call(mcx, opername, oprleft, oprright)?;

    if catlist.is_empty() {
        /* no hope, fall out early */
        return Ok(InvalidOid);
    }

    /*
     * We have to find the list member that is first in the search path, if
     * there's more than one. This doubly-nested loop looks ugly, but in
     * practice there should usually be few catlist members.
     */
    recomputeNamespacePath(mcx)?;

    for namespaceId in active_search_path() {
        if namespaceId == my_temp_namespace() {
            continue; /* do not look in temp namespace */
        }

        for operform in &catlist {
            if operform.oprnamespace == namespaceId {
                return Ok(operform.oid);
            }
        }
    }

    Ok(InvalidOid)
}

/* ===========================================================================
 * OpernameGetCandidates (C lines 1888-2040)
 * ======================================================================== */

/// `OpernameGetCandidates` — given a possibly-qualified operator name and
/// operator kind, retrieve a list of the possible matches.
pub fn OpernameGetCandidates<'mcx>(
    mcx: Mcx<'mcx>,
    names: NameList,
    oprkind: u8,
    missing_schema_ok: bool,
) -> PgResult<FuncCandidateList<'mcx>> {
    let mut resultList: FuncCandidateList<'mcx> = PgVec::new_in(mcx);
    let namespaceId: Oid;

    /* deconstruct the name list */
    let (schemaname, opername) = DeconstructQualifiedName(mcx, names)?;

    if let Some(schemaname) = schemaname {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname, missing_schema_ok)?;
        if missing_schema_ok && !OidIsValid(namespaceId) {
            return Ok(PgVec::new_in(mcx));
        }
    } else {
        /* flag to indicate we need namespace search */
        namespaceId = InvalidOid;
        recomputeNamespacePath(mcx)?;
    }

    /* Search syscache by name only */
    let (catlist, catlist_ordered) = syscache_seams::oper_catlist1::call(mcx, opername)?;
    let path = active_search_path();

    for operform in &catlist {
        let mut pathpos = 0;

        /* Ignore operators of wrong kind, if specific kind requested */
        if oprkind != 0 && operform.oprkind != oprkind {
            continue;
        }

        if OidIsValid(namespaceId) {
            /* Consider only opers in specified namespace */
            if operform.oprnamespace != namespaceId {
                continue;
            }
            /* No need to check args, they must all be different */
        } else {
            /*
             * Consider only opers that are in the search path and are not in
             * the temp namespace.
             */
            let mut in_path = false;
            for nsp in &path {
                if operform.oprnamespace == *nsp && operform.oprnamespace != my_temp_namespace() {
                    in_path = true;
                    break;
                }
                pathpos += 1;
            }
            if !in_path {
                continue; /* oper is not in search path */
            }

            /*
             * Okay, it's in the search path, but does it have the same
             * arguments as something we already accepted? If so, keep only
             * the one that appears earlier in the search path.
             *
             * If we have an ordered list from SearchSysCacheList (the normal
             * case), then any conflicting oper must immediately adjoin this
             * one in the list, so we only need to look at the newest result
             * item. If we have an unordered list, we have to scan the whole
             * result list.
             */
            if !resultList.is_empty() {
                let prevResult: Option<usize>;

                if catlist_ordered {
                    if operform.oprleft == resultList[0].args[0]
                        && operform.oprright == resultList[0].args[1]
                    {
                        prevResult = Some(0);
                    } else {
                        prevResult = None;
                    }
                } else {
                    let mut found = None;
                    for (idx, prev) in resultList.iter().enumerate() {
                        if operform.oprleft == prev.args[0] && operform.oprright == prev.args[1] {
                            found = Some(idx);
                            break;
                        }
                    }
                    prevResult = found;
                }
                if let Some(prev_idx) = prevResult {
                    /* We have a match with a previous result */
                    debug_assert!(pathpos != resultList[prev_idx].pathpos);
                    if pathpos > resultList[prev_idx].pathpos {
                        continue; /* keep previous result */
                    }
                    /* replace previous result */
                    resultList[prev_idx].pathpos = pathpos;
                    resultList[prev_idx].oid = operform.oid;
                    continue; /* args are same, of course */
                }
            }
        }

        /*
         * Okay to add it to result list
         */
        let mut args: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, 2)?;
        args.push(operform.oprleft);
        args.push(operform.oprright);
        let newResult = FuncCandidate {
            pathpos,
            oid: operform.oid,
            nominalnargs: 2,
            nargs: 2,
            nvargs: 0,
            ndargs: 0,
            argnumbers: PgVec::new_in(mcx),
            args,
        };
        resultList
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<FuncCandidate>()))?;
        resultList.insert(0, newResult);
    }

    Ok(resultList)
}

/* ===========================================================================
 * OperatorIsVisible / OperatorIsVisibleExt (C lines 2049-2109)
 * ======================================================================== */

/// `OperatorIsVisible` — whether an operator is visible in the search path.
pub fn OperatorIsVisible(mcx: Mcx<'_>, oprid: Oid) -> PgResult<bool> {
    OperatorIsVisibleExt(mcx, oprid, None)
}

/// `OperatorIsVisibleExt`.
fn OperatorIsVisibleExt(mcx: Mcx<'_>, oprid: Oid, is_missing: Option<&mut bool>) -> PgResult<bool> {
    let visible: bool;

    let oprform = match syscache_seams::oper_row_by_oid::call(mcx, oprid)? {
        Some(o) => o,
        None => {
            if let Some(m) = is_missing {
                *m = true;
                return Ok(false);
            }
            return elog_error(format!("cache lookup failed for operator {oprid}"));
        }
    };

    recomputeNamespacePath(mcx)?;

    let oprnamespace = oprform.oprnamespace;
    let path = active_search_path();
    if oprnamespace != PG_CATALOG_NAMESPACE && !list_member_oid(&path, oprnamespace) {
        visible = false;
    } else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another operator of the same name and arguments earlier
         * in the path. So we must do a slow check to see if this is the same
         * operator that would be found by OpernameGetOprid.
         */
        /* C: list_make1(makeString(oprname)) in the current context. */
        let names = [Some(oprform.oprname.as_str().to_string())];
        visible = OpernameGetOprid(mcx, &names, oprform.oprleft, oprform.oprright)? == oprid;
    }

    Ok(visible)
}

/* ===========================================================================
 * OpclassnameGetOpcid / OpclassIsVisible(Ext) (C lines 2121-2212)
 * ======================================================================== */

/// `OpclassnameGetOpcid` — try to resolve an unqualified index opclass name.
/// Returns OID if opclass found in search path, else InvalidOid.
pub fn OpclassnameGetOpcid(mcx: Mcx<'_>, amid: Oid, opcname: &str) -> PgResult<Oid> {
    recomputeNamespacePath(mcx)?;

    for namespaceId in active_search_path() {
        if namespaceId == my_temp_namespace() {
            continue; /* do not look in temp namespace */
        }

        let opcid = syscache_seams::get_opclass_oid::call(amid, opcname, namespaceId)?;
        if OidIsValid(opcid) {
            return Ok(opcid);
        }
    }

    /* Not found in path */
    Ok(InvalidOid)
}

/// `OpclassIsVisible` — whether an opclass is visible in the search path.
pub fn OpclassIsVisible(mcx: Mcx<'_>, opcid: Oid) -> PgResult<bool> {
    OpclassIsVisibleExt(mcx, opcid, None)
}

/// `OpclassIsVisibleExt`.
fn OpclassIsVisibleExt(mcx: Mcx<'_>, opcid: Oid, is_missing: Option<&mut bool>) -> PgResult<bool> {
    let visible: bool;

    let (opcnamespace, opcmethod, opcname) =
        match syscache_seams::opclass_namespace_method_name::call(mcx, opcid)? {
            Some(t) => t,
            None => {
                if let Some(m) = is_missing {
                    *m = true;
                    return Ok(false);
                }
                return elog_error(format!("cache lookup failed for opclass {opcid}"));
            }
        };

    recomputeNamespacePath(mcx)?;

    let path = active_search_path();
    if opcnamespace != PG_CATALOG_NAMESPACE && !list_member_oid(&path, opcnamespace) {
        /* If it isn't in the path at all, it ain't visible. */
        visible = false;
    } else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another opclass of the same name earlier in the path. So
         * we must do a slow check to see if this opclass would be found by
         * OpclassnameGetOpcid.
         */
        visible = OpclassnameGetOpcid(mcx, opcmethod, opcname.as_str())? == opcid;
    }

    Ok(visible)
}

/* ===========================================================================
 * OpfamilynameGetOpfid / OpfamilyIsVisible(Ext) (C lines 2223-2314)
 * ======================================================================== */

/// `OpfamilynameGetOpfid` — try to resolve an unqualified index opfamily name.
pub fn OpfamilynameGetOpfid(mcx: Mcx<'_>, amid: Oid, opfname: &str) -> PgResult<Oid> {
    recomputeNamespacePath(mcx)?;

    for namespaceId in active_search_path() {
        if namespaceId == my_temp_namespace() {
            continue; /* do not look in temp namespace */
        }

        let opfid = syscache_seams::get_opfamily_oid::call(amid, opfname, namespaceId)?;
        if OidIsValid(opfid) {
            return Ok(opfid);
        }
    }

    /* Not found in path */
    Ok(InvalidOid)
}

/// `OpfamilyIsVisible` — whether an opfamily is visible in the search path.
pub fn OpfamilyIsVisible(mcx: Mcx<'_>, opfid: Oid) -> PgResult<bool> {
    OpfamilyIsVisibleExt(mcx, opfid, None)
}

/// `OpfamilyIsVisibleExt`.
fn OpfamilyIsVisibleExt(mcx: Mcx<'_>, opfid: Oid, is_missing: Option<&mut bool>) -> PgResult<bool> {
    let visible: bool;

    let (opfnamespace, opfmethod, opfname) =
        match syscache_seams::opfamily_namespace_method_name::call(mcx, opfid)? {
            Some(t) => t,
            None => {
                if let Some(m) = is_missing {
                    *m = true;
                    return Ok(false);
                }
                return elog_error(format!("cache lookup failed for opfamily {opfid}"));
            }
        };

    recomputeNamespacePath(mcx)?;

    let path = active_search_path();
    if opfnamespace != PG_CATALOG_NAMESPACE && !list_member_oid(&path, opfnamespace) {
        visible = false;
    } else {
        visible = OpfamilynameGetOpfid(mcx, opfmethod, opfname.as_str())? == opfid;
    }

    Ok(visible)
}

/* ===========================================================================
 * lookup_collation / CollationGetCollid / CollationIsVisible(Ext)
 * (C lines 2322-2466)
 * ======================================================================== */

/// `lookup_collation` — if there's a collation of the given name/namespace,
/// and it works with the given encoding, return its OID. Else InvalidOid.
fn lookup_collation(collname: &str, collnamespace: Oid, encoding: i32) -> PgResult<Oid> {
    /* Check for encoding-specific entry (exact match) */
    let collid = syscache_seams::get_collation_oid_by_name_enc_nsp::call(
        collname,
        encoding,
        collnamespace,
    )?;
    if OidIsValid(collid) {
        return Ok(collid);
    }

    /*
     * Check for any-encoding entry. This takes a bit more work: while libc
     * collations with collencoding = -1 do work with all encodings, ICU
     * collations only work with certain encodings, so we have to check that
     * aspect before deciding it's a match.
     */
    let (oid, collprovider) =
        match syscache_seams::collation_any_encoding_row::call(collname, collnamespace)? {
            Some(row) => row,
            None => return Ok(InvalidOid),
        };
    let collid = if collprovider == COLLPROVIDER_ICU {
        if mbutils_seams::is_encoding_supported_by_icu::call(encoding) {
            oid
        } else {
            InvalidOid
        }
    } else {
        oid
    };
    Ok(collid)
}

/// `CollationGetCollid` — try to resolve an unqualified collation name.
/// Returns OID if collation found in search path, else InvalidOid.
///
/// Note that this will only find collations that work with the current
/// database's encoding.
pub fn CollationGetCollid(mcx: Mcx<'_>, collname: &str) -> PgResult<Oid> {
    let dbencoding = mbutils_seams::get_database_encoding::call();

    recomputeNamespacePath(mcx)?;

    for namespaceId in active_search_path() {
        if namespaceId == my_temp_namespace() {
            continue; /* do not look in temp namespace */
        }

        let collid = lookup_collation(collname, namespaceId, dbencoding)?;
        if OidIsValid(collid) {
            return Ok(collid);
        }
    }

    /* Not found in path */
    Ok(InvalidOid)
}

/// `CollationIsVisible` — whether a collation is visible in the search path.
pub fn CollationIsVisible(mcx: Mcx<'_>, collid: Oid) -> PgResult<bool> {
    CollationIsVisibleExt(mcx, collid, None)
}

/// `CollationIsVisibleExt`.
fn CollationIsVisibleExt(mcx: Mcx<'_>, collid: Oid, is_missing: Option<&mut bool>) -> PgResult<bool> {
    let visible: bool;

    let collform = match syscache_seams::collation_namespace_and_name::call(mcx, collid)? {
        Some(c) => c,
        None => {
            if let Some(m) = is_missing {
                *m = true;
                return Ok(false);
            }
            return elog_error(format!("cache lookup failed for collation {collid}"));
        }
    };

    recomputeNamespacePath(mcx)?;

    let collnamespace = collform.namespace;
    let path = active_search_path();
    if collnamespace != PG_CATALOG_NAMESPACE && !list_member_oid(&path, collnamespace) {
        visible = false;
    } else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another collation of the same name earlier in the path,
         * or it might not work with the database encoding. So we must do a
         * slow check.
         */
        visible = CollationGetCollid(mcx, collform.name.as_str())? == collid;
    }

    Ok(visible)
}

/* ===========================================================================
 * ConversionGetConid / ConversionIsVisible(Ext) (C lines 2477-2567)
 * ======================================================================== */

/// `ConversionGetConid` — try to resolve an unqualified conversion name.
pub fn ConversionGetConid(mcx: Mcx<'_>, conname: &str) -> PgResult<Oid> {
    recomputeNamespacePath(mcx)?;

    for namespaceId in active_search_path() {
        if namespaceId == my_temp_namespace() {
            continue; /* do not look in temp namespace */
        }

        let conid = syscache_seams::get_conversion_oid_cached::call(conname, namespaceId)?;
        if OidIsValid(conid) {
            return Ok(conid);
        }
    }

    /* Not found in path */
    Ok(InvalidOid)
}

/// `ConversionIsVisible` — whether a conversion is visible in the search path.
pub fn ConversionIsVisible(mcx: Mcx<'_>, conid: Oid) -> PgResult<bool> {
    ConversionIsVisibleExt(mcx, conid, None)
}

/// `ConversionIsVisibleExt`.
fn ConversionIsVisibleExt(mcx: Mcx<'_>, conid: Oid, is_missing: Option<&mut bool>) -> PgResult<bool> {
    let visible: bool;

    let conform = match syscache_seams::conversion_namespace_and_name::call(mcx, conid)? {
        Some(c) => c,
        None => {
            if let Some(m) = is_missing {
                *m = true;
                return Ok(false);
            }
            return elog_error(format!("cache lookup failed for conversion {conid}"));
        }
    };

    recomputeNamespacePath(mcx)?;

    let connamespace = conform.namespace;
    let path = active_search_path();
    if connamespace != PG_CATALOG_NAMESPACE && !list_member_oid(&path, connamespace) {
        visible = false;
    } else {
        visible = ConversionGetConid(mcx, conform.name.as_str())? == conid;
    }

    Ok(visible)
}

/* ===========================================================================
 * get_statistics_object_oid / StatisticsObjIsVisible(Ext) (C 2575-2711)
 * ======================================================================== */

/// `get_statistics_object_oid` — find a statistics object by possibly
/// qualified name.
pub fn get_statistics_object_oid(mcx: Mcx<'_>, names: NameList, missing_ok: bool) -> PgResult<Oid> {
    let namespaceId: Oid;
    let mut stats_oid: Oid = InvalidOid;

    /* deconstruct the name list */
    let (schemaname, stats_name) = DeconstructQualifiedName(mcx, names)?;

    if let Some(schemaname) = schemaname {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok)?;
        if missing_ok && !OidIsValid(namespaceId) {
            stats_oid = InvalidOid;
        } else {
            stats_oid = syscache_seams::get_statext_oid::call(stats_name, namespaceId)?;
        }
    } else {
        /* search for it in search path */
        recomputeNamespacePath(mcx)?;

        for ns in active_search_path() {
            if ns == my_temp_namespace() {
                continue; /* do not look in temp namespace */
            }
            stats_oid = syscache_seams::get_statext_oid::call(stats_name, ns)?;
            if OidIsValid(stats_oid) {
                break;
            }
        }
    }

    if !OidIsValid(stats_oid) && !missing_ok {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "statistics object \"{}\" does not exist",
                name_list_to_string(names)
            ))
            .finish(here("get_statistics_object_oid"))
            .map(|()| InvalidOid);
    }

    Ok(stats_oid)
}

/// `StatisticsObjIsVisible` — whether a statistics object is visible.
pub fn StatisticsObjIsVisible(mcx: Mcx<'_>, stxid: Oid) -> PgResult<bool> {
    StatisticsObjIsVisibleExt(mcx, stxid, None)
}

/// `StatisticsObjIsVisibleExt`.
fn StatisticsObjIsVisibleExt(mcx: Mcx<'_>, stxid: Oid, is_missing: Option<&mut bool>) -> PgResult<bool> {
    let visible: bool;

    let stxform = match syscache_seams::statext_namespace_and_name::call(mcx, stxid)? {
        Some(s) => s,
        None => {
            if let Some(m) = is_missing {
                *m = true;
                return Ok(false);
            }
            return elog_error(format!("cache lookup failed for statistics object {stxid}"));
        }
    };

    recomputeNamespacePath(mcx)?;

    let stxnamespace = stxform.namespace;
    let path = active_search_path();
    if stxnamespace != PG_CATALOG_NAMESPACE && !list_member_oid(&path, stxnamespace) {
        visible = false;
    } else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another statistics object of the same name earlier in the
         * path.
         */
        let stxname = stxform.name.as_str();
        let mut found = false;
        for namespaceId in &path {
            let namespaceId = *namespaceId;
            if namespaceId == my_temp_namespace() {
                continue; /* do not look in temp namespace */
            }
            if namespaceId == stxnamespace {
                /* Found it first in path */
                found = true;
                break;
            }
            if syscache_seams::statext_exists::call(stxname, namespaceId)? {
                /* Found something else first in path */
                break;
            }
        }
        visible = found;
    }

    Ok(visible)
}

/* ===========================================================================
 * get_ts_parser_oid / TSParserIsVisible(Ext) (C 2719-2856)
 * ======================================================================== */

/// `get_ts_parser_oid` — find a TS parser by possibly qualified name.
pub fn get_ts_parser_oid(mcx: Mcx<'_>, names: NameList, missing_ok: bool) -> PgResult<Oid> {
    let namespaceId: Oid;
    let mut prsoid: Oid = InvalidOid;

    let (schemaname, parser_name) = DeconstructQualifiedName(mcx, names)?;

    if let Some(schemaname) = schemaname {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok)?;
        if missing_ok && !OidIsValid(namespaceId) {
            prsoid = InvalidOid;
        } else {
            prsoid = syscache_seams::get_ts_parser_oid_cached::call(parser_name, namespaceId)?;
        }
    } else {
        /* search for it in search path */
        recomputeNamespacePath(mcx)?;
        for ns in active_search_path() {
            if ns == my_temp_namespace() {
                continue; /* do not look in temp namespace */
            }
            prsoid = syscache_seams::get_ts_parser_oid_cached::call(parser_name, ns)?;
            if OidIsValid(prsoid) {
                break;
            }
        }
    }

    if !OidIsValid(prsoid) && !missing_ok {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "text search parser \"{}\" does not exist",
                name_list_to_string(names)
            ))
            .finish(here("get_ts_parser_oid"))
            .map(|()| InvalidOid);
    }

    Ok(prsoid)
}

/// `TSParserIsVisible` — whether a TS parser is visible in the search path.
pub fn TSParserIsVisible(mcx: Mcx<'_>, prsId: Oid) -> PgResult<bool> {
    TSParserIsVisibleExt(mcx, prsId, None)
}

/// `TSParserIsVisibleExt`.
fn TSParserIsVisibleExt(mcx: Mcx<'_>, prsId: Oid, is_missing: Option<&mut bool>) -> PgResult<bool> {
    let visible: bool;

    let form = match syscache_seams::ts_parser_namespace_and_name::call(mcx, prsId)? {
        Some(f) => f,
        None => {
            if let Some(m) = is_missing {
                *m = true;
                return Ok(false);
            }
            return elog_error(format!("cache lookup failed for text search parser {prsId}"));
        }
    };

    recomputeNamespacePath(mcx)?;

    let namespace = form.namespace;
    let path = active_search_path();
    if namespace != PG_CATALOG_NAMESPACE && !list_member_oid(&path, namespace) {
        visible = false;
    } else {
        let name = &form.name;
        let mut found = false;
        for namespaceId in &path {
            let namespaceId = *namespaceId;
            if namespaceId == my_temp_namespace() {
                continue; /* do not look in temp namespace */
            }
            if namespaceId == namespace {
                found = true;
                break;
            }
            if syscache_seams::ts_parser_exists::call(name, namespaceId)? {
                break;
            }
        }
        visible = found;
    }

    Ok(visible)
}

/* ===========================================================================
 * get_ts_dict_oid / TSDictionaryIsVisible(Ext) (C 2864-3002)
 * ======================================================================== */

/// `get_ts_dict_oid` — find a TS dictionary by possibly qualified name.
pub fn get_ts_dict_oid(mcx: Mcx<'_>, names: NameList, missing_ok: bool) -> PgResult<Oid> {
    let namespaceId: Oid;
    let mut dictoid: Oid = InvalidOid;

    let (schemaname, dict_name) = DeconstructQualifiedName(mcx, names)?;

    if let Some(schemaname) = schemaname {
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok)?;
        if missing_ok && !OidIsValid(namespaceId) {
            dictoid = InvalidOid;
        } else {
            dictoid = syscache_seams::get_ts_dict_oid_cached::call(dict_name, namespaceId)?;
        }
    } else {
        recomputeNamespacePath(mcx)?;
        for ns in active_search_path() {
            if ns == my_temp_namespace() {
                continue; /* do not look in temp namespace */
            }
            dictoid = syscache_seams::get_ts_dict_oid_cached::call(dict_name, ns)?;
            if OidIsValid(dictoid) {
                break;
            }
        }
    }

    if !OidIsValid(dictoid) && !missing_ok {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "text search dictionary \"{}\" does not exist",
                name_list_to_string(names)
            ))
            .finish(here("get_ts_dict_oid"))
            .map(|()| InvalidOid);
    }

    Ok(dictoid)
}

/// `TSDictionaryIsVisible` — whether a TS dictionary is visible.
pub fn TSDictionaryIsVisible(mcx: Mcx<'_>, dictId: Oid) -> PgResult<bool> {
    TSDictionaryIsVisibleExt(mcx, dictId, None)
}

/// `TSDictionaryIsVisibleExt`.
fn TSDictionaryIsVisibleExt(mcx: Mcx<'_>, dictId: Oid, is_missing: Option<&mut bool>) -> PgResult<bool> {
    let visible: bool;

    let form = match syscache_seams::ts_dict_namespace_and_name::call(mcx, dictId)? {
        Some(f) => f,
        None => {
            if let Some(m) = is_missing {
                *m = true;
                return Ok(false);
            }
            return elog_error(format!(
                "cache lookup failed for text search dictionary {dictId}"
            ));
        }
    };

    recomputeNamespacePath(mcx)?;

    let namespace = form.namespace;
    let path = active_search_path();
    if namespace != PG_CATALOG_NAMESPACE && !list_member_oid(&path, namespace) {
        visible = false;
    } else {
        let name = &form.name;
        let mut found = false;
        for namespaceId in &path {
            let namespaceId = *namespaceId;
            if namespaceId == my_temp_namespace() {
                continue; /* do not look in temp namespace */
            }
            if namespaceId == namespace {
                found = true;
                break;
            }
            if syscache_seams::ts_dict_exists::call(name, namespaceId)? {
                break;
            }
        }
        visible = found;
    }

    Ok(visible)
}

/* ===========================================================================
 * get_ts_template_oid / TSTemplateIsVisible(Ext) (C 3010-3147)
 * ======================================================================== */

/// `get_ts_template_oid` — find a TS template by possibly qualified name.
pub fn get_ts_template_oid(mcx: Mcx<'_>, names: NameList, missing_ok: bool) -> PgResult<Oid> {
    let namespaceId: Oid;
    let mut tmploid: Oid = InvalidOid;

    let (schemaname, template_name) = DeconstructQualifiedName(mcx, names)?;

    if let Some(schemaname) = schemaname {
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok)?;
        if missing_ok && !OidIsValid(namespaceId) {
            tmploid = InvalidOid;
        } else {
            tmploid = syscache_seams::get_ts_template_oid_cached::call(template_name, namespaceId)?;
        }
    } else {
        recomputeNamespacePath(mcx)?;
        for ns in active_search_path() {
            if ns == my_temp_namespace() {
                continue; /* do not look in temp namespace */
            }
            tmploid = syscache_seams::get_ts_template_oid_cached::call(template_name, ns)?;
            if OidIsValid(tmploid) {
                break;
            }
        }
    }

    if !OidIsValid(tmploid) && !missing_ok {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "text search template \"{}\" does not exist",
                name_list_to_string(names)
            ))
            .finish(here("get_ts_template_oid"))
            .map(|()| InvalidOid);
    }

    Ok(tmploid)
}

/// `TSTemplateIsVisible` — whether a TS template is visible.
pub fn TSTemplateIsVisible(mcx: Mcx<'_>, tmplId: Oid) -> PgResult<bool> {
    TSTemplateIsVisibleExt(mcx, tmplId, None)
}

/// `TSTemplateIsVisibleExt`.
fn TSTemplateIsVisibleExt(mcx: Mcx<'_>, tmplId: Oid, is_missing: Option<&mut bool>) -> PgResult<bool> {
    let visible: bool;

    let form = match syscache_seams::ts_template_namespace_and_name::call(mcx, tmplId)? {
        Some(f) => f,
        None => {
            if let Some(m) = is_missing {
                *m = true;
                return Ok(false);
            }
            return elog_error(format!(
                "cache lookup failed for text search template {tmplId}"
            ));
        }
    };

    recomputeNamespacePath(mcx)?;

    let namespace = form.namespace;
    let path = active_search_path();
    if namespace != PG_CATALOG_NAMESPACE && !list_member_oid(&path, namespace) {
        visible = false;
    } else {
        let name = &form.name;
        let mut found = false;
        for namespaceId in &path {
            let namespaceId = *namespaceId;
            if namespaceId == my_temp_namespace() {
                continue; /* do not look in temp namespace */
            }
            if namespaceId == namespace {
                found = true;
                break;
            }
            if syscache_seams::ts_template_exists::call(name, namespaceId)? {
                break;
            }
        }
        visible = found;
    }

    Ok(visible)
}

/* ===========================================================================
 * get_ts_config_oid / TSConfigIsVisible(Ext) (C 3155-3293)
 * ======================================================================== */

/// `get_ts_config_oid` — find a TS config by possibly qualified name.
pub fn get_ts_config_oid(mcx: Mcx<'_>, names: NameList, missing_ok: bool) -> PgResult<Oid> {
    let namespaceId: Oid;
    let mut cfgoid: Oid = InvalidOid;

    let (schemaname, config_name) = DeconstructQualifiedName(mcx, names)?;

    if let Some(schemaname) = schemaname {
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok)?;
        if missing_ok && !OidIsValid(namespaceId) {
            cfgoid = InvalidOid;
        } else {
            cfgoid = syscache_seams::get_ts_config_oid_cached::call(config_name, namespaceId)?;
        }
    } else {
        recomputeNamespacePath(mcx)?;
        for ns in active_search_path() {
            if ns == my_temp_namespace() {
                continue; /* do not look in temp namespace */
            }
            cfgoid = syscache_seams::get_ts_config_oid_cached::call(config_name, ns)?;
            if OidIsValid(cfgoid) {
                break;
            }
        }
    }

    if !OidIsValid(cfgoid) && !missing_ok {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "text search configuration \"{}\" does not exist",
                name_list_to_string(names)
            ))
            .finish(here("get_ts_config_oid"))
            .map(|()| InvalidOid);
    }

    Ok(cfgoid)
}

/// `TSConfigIsVisible` — whether a TS configuration is visible.
pub fn TSConfigIsVisible(mcx: Mcx<'_>, cfgid: Oid) -> PgResult<bool> {
    TSConfigIsVisibleExt(mcx, cfgid, None)
}

/// `TSConfigIsVisibleExt`.
fn TSConfigIsVisibleExt(mcx: Mcx<'_>, cfgid: Oid, is_missing: Option<&mut bool>) -> PgResult<bool> {
    let visible: bool;

    let form = match syscache_seams::ts_config_namespace_and_name::call(mcx, cfgid)? {
        Some(f) => f,
        None => {
            if let Some(m) = is_missing {
                *m = true;
                return Ok(false);
            }
            return elog_error(format!(
                "cache lookup failed for text search configuration {cfgid}"
            ));
        }
    };

    recomputeNamespacePath(mcx)?;

    let namespace = form.namespace;
    let path = active_search_path();
    if namespace != PG_CATALOG_NAMESPACE && !list_member_oid(&path, namespace) {
        visible = false;
    } else {
        let name = &form.name;
        let mut found = false;
        for namespaceId in &path {
            let namespaceId = *namespaceId;
            if namespaceId == my_temp_namespace() {
                continue; /* do not look in temp namespace */
            }
            if namespaceId == namespace {
                found = true;
                break;
            }
            if syscache_seams::ts_config_exists::call(name, namespaceId)? {
                break;
            }
        }
        visible = found;
    }

    Ok(visible)
}

/* ===========================================================================
 * DeconstructQualifiedName (C lines 3304-3345)
 * ======================================================================== */

/// `DeconstructQualifiedName` — given a possibly-qualified name expressed as
/// a list of String nodes, extract the schema name and object name. Returns
/// `(schemaname, objname)`; `schemaname` is `None` if there is no explicit
/// schema name.
pub fn DeconstructQualifiedName<'a>(
    mcx: Mcx<'_>,
    names: NameList<'a>,
) -> PgResult<(Option<&'a str>, &'a str)> {
    let catalogname: &str;
    let schemaname: Option<&'a str>;
    let objname: &'a str;

    match names.len() {
        1 => {
            schemaname = None;
            objname = strVal(&names[0])?;
        }
        2 => {
            schemaname = Some(strVal(&names[0])?);
            objname = strVal(&names[1])?;
        }
        3 => {
            catalogname = strVal(&names[0])?;
            schemaname = Some(strVal(&names[1])?);
            objname = strVal(&names[2])?;

            /*
             * We check the catalog name and then ignore it.
             */
            if catalogname_differs_from_database(mcx, catalogname)? {
                return ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!(
                        "cross-database references are not implemented: {}",
                        name_list_to_string(names)
                    ))
                    .finish(here("DeconstructQualifiedName"))
                    .map(|()| (None, ""));
            }
        }
        _ => {
            return ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!(
                    "improper qualified name (too many dotted names): {}",
                    name_list_to_string(names)
                ))
                .finish(here("DeconstructQualifiedName"))
                .map(|()| (None, ""));
        }
    }

    Ok((schemaname, objname))
}

/// The C `strVal()` read of one name-list element (a non-String element is a
/// programming error in these callers).
fn strVal(e: &Option<String>) -> PgResult<&str> {
    match e {
        Some(s) => Ok(s.as_str()),
        None => elog_error("unexpected non-String node in name list".to_string()),
    }
}

/* ===========================================================================
 * LookupNamespaceNoError (C lines 3358-3378)
 * ======================================================================== */

/// `LookupNamespaceNoError` — look up a schema name; InvalidOid if not found.
/// No errors, and no permissions check (callers check that themselves).
pub fn LookupNamespaceNoError(nspname: &str) -> PgResult<Oid> {
    /* check for pg_temp alias */
    if nspname == "pg_temp" {
        if OidIsValid(my_temp_namespace()) {
            objectaccess_seams::invoke_namespace_search_hook::call(my_temp_namespace(), true)?;
            return Ok(my_temp_namespace());
        }

        /*
         * Since this is used only for looking up existing objects, there is
         * no point in trying to initialize the temp namespace here; and doing
         * so might create problems for some callers. Just report "not found".
         */
        return Ok(InvalidOid);
    }

    get_namespace_oid(nspname, true)
}

/* ===========================================================================
 * LookupExplicitNamespace (C lines 3388-3418)
 * ======================================================================== */

/// `LookupExplicitNamespace` — process an explicitly-specified schema name:
/// look up the schema and verify we have USAGE (lookup) rights in it.
pub fn LookupExplicitNamespace(nspname: &str, missing_ok: bool) -> PgResult<Oid> {
    let namespaceId: Oid;

    /* check for pg_temp alias */
    if nspname == "pg_temp" {
        if OidIsValid(my_temp_namespace()) {
            return Ok(my_temp_namespace());
        }

        /*
         * Since this is used only for looking up existing objects, there is
         * no point in trying to initialize the temp namespace here --- and
         * anyway, the namespace not existing means that the object presumably
         * doesn't exist either. Just fall through and let the error happen.
         */
    }

    namespaceId = get_namespace_oid(nspname, missing_ok)?;
    if missing_ok && !OidIsValid(namespaceId) {
        return Ok(InvalidOid);
    }

    let aclresult = namespace_aclcheck(namespaceId, miscinit_seams::get_user_id::call(), ACL_USAGE)?;
    if aclresult != AclResult::AclcheckOk {
        aclcheck_error_schema(aclresult, Some(nspname.to_string()))?;
    }
    /* Schema search hook for this lookup */
    objectaccess_seams::invoke_namespace_search_hook::call(namespaceId, true)?;

    Ok(namespaceId)
}

/* ===========================================================================
 * LookupCreationNamespace (C lines 3431-3452)
 * ======================================================================== */

/// `LookupCreationNamespace` — look up the schema and verify we have CREATE
/// rights on it.
pub fn LookupCreationNamespace(mcx: Mcx<'_>, nspname: &str) -> PgResult<Oid> {
    let namespaceId: Oid;

    /* check for pg_temp alias */
    if nspname == "pg_temp" {
        /* Initialize temp namespace */
        AccessTempTableNamespace(mcx, false)?;
        return Ok(my_temp_namespace());
    }

    namespaceId = get_namespace_oid(nspname, false)?;

    let aclresult = namespace_aclcheck(namespaceId, miscinit_seams::get_user_id::call(), ACL_CREATE)?;
    if aclresult != AclResult::AclcheckOk {
        aclcheck_error_schema(aclresult, Some(nspname.to_string()))?;
    }

    Ok(namespaceId)
}

/* ===========================================================================
 * CheckSetNamespace (C lines 3462-3475)
 * ======================================================================== */

/// `CheckSetNamespace` — common checks on switching namespaces.
pub fn CheckSetNamespace(mcx: Mcx<'_>, oldNspOid: Oid, nspOid: Oid) -> PgResult<()> {
    /* disallow renaming into or out of temp schemas */
    if isAnyTempNamespace(mcx, nspOid)? || isAnyTempNamespace(mcx, oldNspOid)? {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot move objects into or out of temporary schemas")
            .finish(here("CheckSetNamespace"));
    }

    /* same for TOAST schema */
    if nspOid == PG_TOAST_NAMESPACE || oldNspOid == PG_TOAST_NAMESPACE {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot move objects into or out of TOAST schema")
            .finish(here("CheckSetNamespace"));
    }
    Ok(())
}

/* ===========================================================================
 * QualifiedNameGetCreationNamespace (C lines 3490-3529)
 * ======================================================================== */

/// `QualifiedNameGetCreationNamespace` — given a possibly-qualified name for
/// an object (in List-of-Strings format), determine what namespace the object
/// should be created in. Also returns the object name. Note: this does not
/// apply any permissions check, nor lock the namespace.
pub fn QualifiedNameGetCreationNamespace<'a>(
    mcx: Mcx<'_>,
    names: NameList<'a>,
) -> PgResult<(Oid, &'a str)> {
    let namespaceId: Oid;

    /* deconstruct the name list */
    let (schemaname, objname) = DeconstructQualifiedName(mcx, names)?;

    if let Some(schemaname) = schemaname {
        /* check for pg_temp alias */
        if schemaname == "pg_temp" {
            /* Initialize temp namespace */
            AccessTempTableNamespace(mcx, false)?;
            return Ok((my_temp_namespace(), objname));
        }
        /* use exact schema given */
        namespaceId = get_namespace_oid(schemaname, false)?;
        /* we do not check for USAGE rights here! */
    } else {
        /* use the default creation namespace */
        recomputeNamespacePath(mcx)?;
        if STATE.with(|s| s.borrow().active_temp_creation_pending) {
            /* Need to initialize temp namespace */
            AccessTempTableNamespace(mcx, true)?;
            return Ok((my_temp_namespace(), objname));
        }
        namespaceId = STATE.with(|s| s.borrow().active_creation_namespace);
        if !OidIsValid(namespaceId) {
            return ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_SCHEMA)
                .errmsg("no schema has been selected to create in")
                .finish(here("QualifiedNameGetCreationNamespace"))
                .map(|()| (InvalidOid, ""));
        }
    }

    Ok((namespaceId, objname))
}

/* ===========================================================================
 * get_namespace_oid (C lines 3538-3550)
 * ======================================================================== */

/// `get_namespace_oid` — given a namespace name, look up the OID.
///
/// If missing_ok is false, throw an error if namespace name not found. If
/// true, just return InvalidOid.
pub fn get_namespace_oid(nspname: &str, missing_ok: bool) -> PgResult<Oid> {
    let oid = syscache_seams::get_namespace_oid_cached::call(nspname)?;
    if !OidIsValid(oid) && !missing_ok {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_SCHEMA)
            .errmsg(format!("schema \"{nspname}\" does not exist"))
            .finish(here("get_namespace_oid"))
            .map(|()| InvalidOid);
    }

    Ok(oid)
}

/* ===========================================================================
 * makeRangeVarFromNameList (C lines 3557-3584)
 * ======================================================================== */

/// `makeRangeVarFromNameList` — utility routine to convert a qualified-name
/// list into RangeVar form.
pub fn makeRangeVarFromNameList(names: NameList) -> PgResult<RangeVar> {
    // makeRangeVar(NULL, NULL, -1) — permanent, inheritance-enabled template.
    let mut rel = RangeVar {
        inh: true,
        relpersistence: ::types_tuple::access::RELPERSISTENCE_PERMANENT,
        location: -1,
        ..RangeVar::default()
    };

    /* The `RangeVar` node owns its strings (C shares the List's pointers);
     * the copies live in the node, an owned-model adaptation. */
    match names.len() {
        1 => {
            rel.relname = strVal(&names[0])?.to_string();
        }
        2 => {
            rel.schemaname = Some(strVal(&names[0])?.to_string());
            rel.relname = strVal(&names[1])?.to_string();
        }
        3 => {
            rel.catalogname = Some(strVal(&names[0])?.to_string());
            rel.schemaname = Some(strVal(&names[1])?.to_string());
            rel.relname = strVal(&names[2])?.to_string();
        }
        _ => {
            return ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!(
                    "improper relation name (too many dotted names): {}",
                    name_list_to_string(names)
                ))
                .finish(here("makeRangeVarFromNameList"))
                .map(|()| RangeVar::default());
        }
    }

    Ok(rel)
}

/* ===========================================================================
 * NameListToString / NameListToQuotedString (C lines 3597-3646)
 * ======================================================================== */

/// `NameListToString` — utility routine to convert a qualified-name list into
/// a standard string (the name parts are NOT double-quoted). Mostly used for
/// error messages.
///
/// In C the list elements may be either `String` or `A_Star`; the owned image
/// is `None` for `A_Star`.
pub fn NameListToString<'mcx>(mcx: Mcx<'mcx>, names: NameList) -> PgResult<PgString<'mcx>> {
    let mut string = PgString::new_in(mcx);

    for (i, name) in names.iter().enumerate() {
        if i != 0 {
            string.try_push('.')?;
        }
        match name {
            Some(s) => string.try_push_str(s)?,
            None => string.try_push('*')?,
        }
    }

    Ok(string)
}

/// [`NameListToString`] for error-message construction: the text goes into
/// the `PgError` carrier (C: the errmsg is evaluated into `ErrorContext`),
/// so it uses the error channel's own allocation, not a caller `Mcx`.
fn name_list_to_string(names: NameList) -> String {
    let mut string = String::new();
    for (i, name) in names.iter().enumerate() {
        if i != 0 {
            string.push('.');
        }
        match name {
            Some(s) => string.push_str(s),
            None => string.push('*'),
        }
    }
    string
}

/// `NameListToQuotedString` — like NameListToString, but the names are
/// double-quoted where necessary, so the string could be re-parsed.
pub fn NameListToQuotedString<'mcx>(
    mcx: Mcx<'mcx>,
    names: NameList,
) -> PgResult<PgString<'mcx>> {
    let mut string = PgString::new_in(mcx);

    for (i, name) in names.iter().enumerate() {
        if i != 0 {
            string.try_push('.')?;
        }
        let quoted = ruleutils_seams::quote_identifier::call(mcx, strVal(name)?)?;
        string.try_push_str(quoted.as_str())?;
    }

    Ok(string)
}

/* ===========================================================================
 * isTempNamespace family (C lines 3652-3719)
 * ======================================================================== */

/// `isTempNamespace` — is the given namespace my temporary-table namespace?
pub fn isTempNamespace(namespaceId: Oid) -> bool {
    let mtn = my_temp_namespace();
    OidIsValid(mtn) && mtn == namespaceId
}

/// `isTempToastNamespace` — is the given namespace my temporary-toast-table
/// namespace?
pub fn isTempToastNamespace(namespaceId: Oid) -> bool {
    let mttn = my_temp_toast_namespace();
    OidIsValid(mttn) && mttn == namespaceId
}

/// `isTempOrTempToastNamespace` — is the given namespace my temporary-table
/// or temporary-toast-table namespace?
pub fn isTempOrTempToastNamespace(namespaceId: Oid) -> PgResult<bool> {
    let mtn = my_temp_namespace();
    Ok(OidIsValid(mtn) && (mtn == namespaceId || my_temp_toast_namespace() == namespaceId))
}

/// `isAnyTempNamespace` — is the given namespace a temporary-table namespace
/// (either my own, or another backend's)? Temporary-toast-table namespaces
/// are included, too.
pub fn isAnyTempNamespace(mcx: Mcx<'_>, namespaceId: Oid) -> PgResult<bool> {
    /* True if the namespace name starts with "pg_temp_" or "pg_toast_temp_" */
    let nspname = match lsyscache_seams::get_namespace_name::call(mcx, namespaceId)? {
        Some(n) => n,
        None => return Ok(false), /* no such namespace? */
    };
    let nspname = nspname.as_str();
    let result = nspname.starts_with("pg_temp_") || nspname.starts_with("pg_toast_temp_");
    Ok(result)
}

/// `isOtherTempNamespace` — is the given namespace some other backend's
/// temporary-table namespace (including temporary-toast-table namespaces)?
pub fn isOtherTempNamespace(mcx: Mcx<'_>, namespaceId: Oid) -> PgResult<bool> {
    /* If it's my own temp namespace, say "false" */
    if isTempOrTempToastNamespace(namespaceId)? {
        return Ok(false);
    }
    /* Else, if it's any temp namespace, say "true" */
    isAnyTempNamespace(mcx, namespaceId)
}

/* ===========================================================================
 * checkTempNamespaceStatus (C lines 3732-3760)
 * ======================================================================== */

/// `checkTempNamespaceStatus` — is the given namespace owned and actively
/// used by a backend?
///
/// Note: this can be used while scanning relations in pg_class to detect
/// orphaned temporary tables or namespaces with a backend connected to a
/// given database.
pub fn checkTempNamespaceStatus(mcx: Mcx<'_>, namespaceId: Oid) -> PgResult<TempNamespaceStatus> {
    debug_assert!(OidIsValid(globals_seams::my_database_id::call()));

    let procNumber = GetTempNamespaceProcNumber(mcx, namespaceId)?;

    /* No such namespace, or its name shows it's not temp? */
    if procNumber == INVALID_PROC_NUMBER {
        return Ok(TEMP_NAMESPACE_NOT_TEMP);
    }

    /* Is the backend alive? */
    let (database_id, temp_namespace_id) = match procarray_seams::proc_status::call(procNumber) {
        Some(p) => p,
        None => return Ok(TEMP_NAMESPACE_IDLE),
    };

    /* Is the backend connected to the same database we are looking at? */
    if database_id != globals_seams::my_database_id::call() {
        return Ok(TEMP_NAMESPACE_IDLE);
    }

    /* Does the backend own the temporary namespace? */
    if temp_namespace_id != namespaceId {
        return Ok(TEMP_NAMESPACE_IDLE);
    }

    /* Yup, so namespace is busy */
    Ok(TEMP_NAMESPACE_IN_USE)
}

/* ===========================================================================
 * GetTempNamespaceProcNumber (C lines 3769-3786)
 * ======================================================================== */

/// `GetTempNamespaceProcNumber` — if the given namespace is a temporary-table
/// namespace (either my own, or another backend's), return the proc number
/// that owns it. Temporary-toast-table namespaces are included, too. If it
/// isn't a temp namespace, return INVALID_PROC_NUMBER.
pub fn GetTempNamespaceProcNumber(mcx: Mcx<'_>, namespaceId: Oid) -> PgResult<ProcNumber> {
    let result: ProcNumber;

    /* See if the namespace name starts with "pg_temp_" or "pg_toast_temp_" */
    let nspname = match lsyscache_seams::get_namespace_name::call(mcx, namespaceId)? {
        Some(n) => n,
        None => return Ok(INVALID_PROC_NUMBER), /* no such namespace? */
    };
    let nspname = nspname.as_str();
    if let Some(rest) = nspname.strip_prefix("pg_temp_") {
        result = atoi(rest);
    } else if let Some(rest) = nspname.strip_prefix("pg_toast_temp_") {
        result = atoi(rest);
    } else {
        result = INVALID_PROC_NUMBER;
    }
    Ok(result)
}

/// `GetTempNamespaceProcNumber(namespaceId)` for callers with no `Mcx` of their
/// own (relcache's `RelationBuildDesc`). The only allocation is the transient
/// namespace-name string, which C `pfree`s before returning a bare scalar; here
/// it lives in a short-lived child of `TopMemoryContext` that is dropped on
/// return, so nothing of the caller's lifetime is involved.
pub fn get_temp_namespace_proc_number_no_mcx(namespaceId: Oid) -> PgResult<ProcNumber> {
    let top = mcxt_seams::top_memory_context::call();
    let scratch = top.context().new_child("GetTempNamespaceProcNumber");
    GetTempNamespaceProcNumber(scratch.mcx(), namespaceId)
}

/// C `atoi(str)` — parse the leading decimal integer, defaulting to 0.
fn atoi(s: &str) -> i32 {
    let bytes = s.as_bytes();
    let mut i = 0;
    /* skip leading whitespace (atoi behavior) */
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let mut sign: i64 = 1;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        if bytes[i] == b'-' {
            sign = -1;
        }
        i += 1;
    }
    let mut acc: i64 = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        acc = acc.wrapping_mul(10).wrapping_add((bytes[i] - b'0') as i64);
        i += 1;
    }
    (sign * acc) as i32
}

/* ===========================================================================
 * GetTempToastNamespace / Get/SetTempNamespaceState (C lines 3794-3844)
 * ======================================================================== */

/// `GetTempToastNamespace` — get the OID of my temporary-toast-table
/// namespace, which must already be assigned.
pub fn GetTempToastNamespace() -> Oid {
    let mttn = my_temp_toast_namespace();
    debug_assert!(OidIsValid(mttn));
    mttn
}

/// `GetTempNamespaceState` — fetch status of session's temporary namespace
/// (for conveying state to a parallel worker). Returns
/// `(tempNamespaceId, tempToastNamespaceId)`, 0 if not created.
pub fn GetTempNamespaceState() -> (Oid, Oid) {
    STATE.with(|s| {
        let st = s.borrow();
        (st.my_temp_namespace, st.my_temp_toast_namespace)
    })
}

/// `SetTempNamespaceState` — set status of session's temporary namespace
/// (parallel-worker state transfer).
pub fn SetTempNamespaceState(tempNamespaceId: Oid, tempToastNamespaceId: Oid) {
    STATE.with(|s| {
        let mut st = s.borrow_mut();
        /* Worker should not have created its own namespaces ... */
        debug_assert_eq!(st.my_temp_namespace, InvalidOid);
        debug_assert_eq!(st.my_temp_toast_namespace, InvalidOid);
        debug_assert_eq!(st.my_temp_namespace_sub_id, InvalidSubTransactionId);

        /* Assign same namespace OIDs that leader has */
        st.my_temp_namespace = tempNamespaceId;
        st.my_temp_toast_namespace = tempToastNamespaceId;

        /*
         * It's fine to leave myTempNamespaceSubID == InvalidSubTransactionId.
         * Even if the namespace is new so far as the leader is concerned,
         * it's not new to the worker, and we certainly wouldn't want the
         * worker trying to destroy it.
         */

        st.base_search_path_valid = false; /* may need to rebuild list */
        st.search_path_cache_valid = false;
    });
}

/* ===========================================================================
 * GetSearchPathMatcher / CopySearchPathMatcher /
 * SearchPathMatchesCurrentEnvironment (C lines 3855-3965)
 * ======================================================================== */

/// `GetSearchPathMatcher(context)` — fetch current search path definition,
/// allocated in `mcx` (the C `context` argument).
pub fn GetSearchPathMatcher<'mcx>(mcx: Mcx<'mcx>) -> PgResult<SearchPathMatcher<'mcx>> {
    recomputeNamespacePath(mcx)?;

    let (active_path, active_creation, mtn, active_generation) = STATE.with(|s| {
        let st = s.borrow();
        (
            st.active_search_path.clone(),
            st.active_creation_namespace,
            st.my_temp_namespace,
            st.active_path_generation,
        )
    });

    /* list_copy(activeSearchPath) and consume the leading implicit entries */
    let mut schemas: PgVec<'mcx, Oid> = slice_in(mcx, &active_path)?;
    let mut add_temp = false;
    let mut add_catalog = false;
    while !schemas.is_empty() && schemas[0] != active_creation {
        if schemas[0] == mtn {
            add_temp = true;
        } else {
            debug_assert_eq!(schemas[0], PG_CATALOG_NAMESPACE);
            add_catalog = true;
        }
        schemas.remove(0);
    }

    Ok(SearchPathMatcher {
        schemas,
        addCatalog: add_catalog,
        addTemp: add_temp,
        generation: active_generation,
    })
}

/// `CopySearchPathMatcher` — copy the specified SearchPathMatcher into
/// `mcx` (C: palloc + list_copy in the current context).
pub fn CopySearchPathMatcher<'mcx>(
    mcx: Mcx<'mcx>,
    path: &SearchPathMatcher<'_>,
) -> PgResult<SearchPathMatcher<'mcx>> {
    Ok(SearchPathMatcher {
        schemas: slice_in(mcx, &path.schemas)?,
        addCatalog: path.addCatalog,
        addTemp: path.addTemp,
        generation: path.generation,
    })
}

/// `SearchPathMatchesCurrentEnvironment` — does path match the current
/// environment?
pub fn SearchPathMatchesCurrentEnvironment(
    mcx: Mcx<'_>,
    path: &mut SearchPathMatcher<'_>,
) -> PgResult<bool> {
    recomputeNamespacePath(mcx)?;

    let (active_path, active_creation, mtn, active_generation) = STATE.with(|s| {
        let st = s.borrow();
        (
            st.active_search_path.clone(),
            st.active_creation_namespace,
            st.my_temp_namespace,
            st.active_path_generation,
        )
    });

    /* Quick out if already known equal to active path. */
    if path.generation == active_generation {
        return Ok(true);
    }

    /* We scan down the activeSearchPath to see if it matches the input. */
    let mut lc = 0usize;

    /* If path->addTemp, first item should be my temp namespace. */
    if path.addTemp {
        if lc < active_path.len() && active_path[lc] == mtn {
            lc += 1;
        } else {
            return Ok(false);
        }
    }
    /* If path->addCatalog, next item should be pg_catalog. */
    if path.addCatalog {
        if lc < active_path.len() && active_path[lc] == PG_CATALOG_NAMESPACE {
            lc += 1;
        } else {
            return Ok(false);
        }
    }
    /* We should now be looking at the activeCreationNamespace. */
    let cur = if lc < active_path.len() {
        active_path[lc]
    } else {
        InvalidOid
    };
    if active_creation != cur {
        return Ok(false);
    }
    /* The remainder of activeSearchPath should match path->schemas. */
    for sch in path.schemas.iter() {
        if lc < active_path.len() && active_path[lc] == *sch {
            lc += 1;
        } else {
            return Ok(false);
        }
    }
    if lc < active_path.len() {
        return Ok(false);
    }

    /*
     * Update path->generation so that future tests will return quickly, so
     * long as the active search path doesn't change.
     */
    path.generation = active_generation;

    Ok(true)
}

/* ===========================================================================
 * get_collation_oid / get_conversion_oid / FindDefaultConversionProc
 * (C lines 3974-4104)
 * ======================================================================== */

/// `get_collation_oid` — find a collation by possibly qualified name.
///
/// Note that this will only find collations that work with the current
/// database's encoding.
pub fn get_collation_oid(mcx: Mcx<'_>, collname: NameList, missing_ok: bool) -> PgResult<Oid> {
    let dbencoding = mbutils_seams::get_database_encoding::call();
    let namespaceId: Oid;

    /* deconstruct the name list */
    let (schemaname, collation_name) = DeconstructQualifiedName(mcx, collname)?;

    if let Some(schemaname) = schemaname {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok)?;
        if missing_ok && !OidIsValid(namespaceId) {
            return Ok(InvalidOid);
        }

        let colloid = lookup_collation(collation_name, namespaceId, dbencoding)?;
        if OidIsValid(colloid) {
            return Ok(colloid);
        }
    } else {
        /* search for it in search path */
        recomputeNamespacePath(mcx)?;

        for ns in active_search_path() {
            if ns == my_temp_namespace() {
                continue; /* do not look in temp namespace */
            }

            let colloid = lookup_collation(collation_name, ns, dbencoding)?;
            if OidIsValid(colloid) {
                return Ok(colloid);
            }
        }
    }

    /* Not found in path */
    if !missing_ok {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "collation \"{}\" for encoding \"{}\" does not exist",
                name_list_to_string(collname),
                mbutils_seams::get_database_encoding_name::call()
            ))
            .finish(here("get_collation_oid"))
            .map(|()| InvalidOid);
    }
    Ok(InvalidOid)
}

/// `get_conversion_oid` — find a conversion by possibly qualified name.
pub fn get_conversion_oid(mcx: Mcx<'_>, conname: NameList, missing_ok: bool) -> PgResult<Oid> {
    let namespaceId: Oid;
    let mut conoid: Oid = InvalidOid;

    /* deconstruct the name list */
    let (schemaname, conversion_name) = DeconstructQualifiedName(mcx, conname)?;

    if let Some(schemaname) = schemaname {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok)?;
        if missing_ok && !OidIsValid(namespaceId) {
            conoid = InvalidOid;
        } else {
            conoid = syscache_seams::get_conversion_oid_cached::call(conversion_name, namespaceId)?;
        }
    } else {
        /* search for it in search path */
        recomputeNamespacePath(mcx)?;

        for ns in active_search_path() {
            if ns == my_temp_namespace() {
                continue; /* do not look in temp namespace */
            }

            conoid = syscache_seams::get_conversion_oid_cached::call(conversion_name, ns)?;
            if OidIsValid(conoid) {
                return Ok(conoid);
            }
        }
    }

    /* Not found in path */
    if !OidIsValid(conoid) && !missing_ok {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "conversion \"{}\" does not exist",
                name_list_to_string(conname)
            ))
            .finish(here("get_conversion_oid"))
            .map(|()| InvalidOid);
    }
    Ok(conoid)
}

/// `FindDefaultConversionProc` — find default encoding conversion proc.
pub fn FindDefaultConversionProc(mcx: Mcx<'_>, for_encoding: i32, to_encoding: i32) -> PgResult<Oid> {
    recomputeNamespacePath(mcx)?;

    for namespaceId in active_search_path() {
        if namespaceId == my_temp_namespace() {
            continue; /* do not look in temp namespace */
        }

        let proc =
            pg_conversion_seams::find_default_conversion::call(namespaceId, for_encoding, to_encoding)?;
        if OidIsValid(proc) {
            return Ok(proc);
        }
    }

    /* Not found in path */
    Ok(InvalidOid)
}

/* ===========================================================================
 * preprocessNamespacePath / finalNamespacePath / cachedNamespacePath
 * (C lines 4110-4297)
 * ======================================================================== */

/// `preprocessNamespacePath` — look up the OIDs and perform ACL checks for
/// the namespaces in the `searchPath` string. Returns
/// `(oidlist, temp_missing)`.
///
/// If any names are not recognizable or we don't have read access, just
/// leave them out of the list. (We can't raise an error, since the
/// search_path setting has already been accepted.) Don't make duplicate
/// entries, either.
fn preprocessNamespacePath(
    mcx: Mcx<'_>,
    searchPath: &str,
    roleid: Oid,
) -> PgResult<(Vec<Oid>, bool)> {
    /* Parse string into list of identifiers */
    let namelist = match varlena_seams::split_identifier_string::call(mcx, searchPath, ',')? {
        Some(l) => l,
        None => {
            /* syntax error in name list */
            /* this should not happen if GUC checked check_search_path */
            return elog_error("invalid list syntax".to_string());
        }
    };

    /*
     * Convert the list of names to a list of OIDs.
     */
    let mut oidlist: Vec<Oid> = Vec::new();
    let mut temp_missing = false;
    for curname in namelist.iter() {
        let curname = curname.as_str();
        if curname == "$user" {
            /* $user --- substitute namespace matching user name, if any */
            if let Some(rname) = syscache_seams::authid_rolname::call(mcx, roleid)? {
                let namespaceId = get_namespace_oid(rname.as_str(), true)?;
                if OidIsValid(namespaceId)
                    && namespace_aclcheck(namespaceId, roleid, ACL_USAGE)?
                        == AclResult::AclcheckOk
                {
                    oidlist.push(namespaceId);
                }
            }
        } else if curname == "pg_temp" {
            /* pg_temp --- substitute temp namespace, if any */
            if OidIsValid(my_temp_namespace()) {
                oidlist.push(my_temp_namespace());
            } else {
                /* If it ought to be the creation namespace, set flag */
                if oidlist.is_empty() {
                    temp_missing = true;
                }
            }
        } else {
            /* normal namespace reference */
            let namespaceId = get_namespace_oid(curname, true)?;
            if OidIsValid(namespaceId)
                && namespace_aclcheck(namespaceId, roleid, ACL_USAGE)? == AclResult::AclcheckOk
            {
                oidlist.push(namespaceId);
            }
        }
    }

    Ok((oidlist, temp_missing))
}

/// `finalNamespacePath` — remove duplicates, run namespace search hooks, and
/// prepend implicitly-searched namespaces. Returns `(finalPath, firstNS)`.
///
/// If an object_access_hook is present, this must always be recalculated. It
/// may seem that duplicate elimination is not dependent on the result of the
/// hook, but if a hook returns different results on different calls for the
/// same namespace ID, then it could affect the order in which that namespace
/// appears in the final list.
fn finalNamespacePath(oidlist: &[Oid]) -> PgResult<(Vec<Oid>, Oid)> {
    let mut finalPath: Vec<Oid> = Vec::new();

    for namespaceId in oidlist {
        let namespaceId = *namespaceId;
        if !list_member_oid(&finalPath, namespaceId) {
            if objectaccess_seams::invoke_namespace_search_hook::call(namespaceId, false)? {
                finalPath.push(namespaceId);
            }
        }
    }

    /*
     * Remember the first member of the explicit list. (Note: this is
     * nominally wrong if temp_missing, but we need it anyway to distinguish
     * explicit from implicit mention of pg_catalog.)
     */
    let firstNS = if finalPath.is_empty() {
        InvalidOid
    } else {
        finalPath[0]
    };

    /*
     * Add any implicitly-searched namespaces to the list. Note these go on
     * the front, not the back; also notice that we do not check USAGE
     * permissions for these.
     */
    if !list_member_oid(&finalPath, PG_CATALOG_NAMESPACE) {
        finalPath.insert(0, PG_CATALOG_NAMESPACE);
    }

    if OidIsValid(my_temp_namespace()) && !list_member_oid(&finalPath, my_temp_namespace()) {
        finalPath.insert(0, my_temp_namespace());
    }

    Ok((finalPath, firstNS))
}

/// A snapshot of a [`SearchPathCacheEntry`]'s consumer-visible contents. In C
/// the caller is handed a pointer into the cache (valid only until the next
/// call); the owned image hands back a copy of the fields
/// `recomputeNamespacePath` reads.
struct CachedPathSnapshot {
    final_path: Vec<Oid>,
    first_ns: Oid,
    temp_missing: bool,
}

/// `cachedNamespacePath` — retrieve search path information from the cache;
/// or if not there, fill it.
fn cachedNamespacePath(
    mcx: Mcx<'_>,
    searchPath: &str,
    roleid: Oid,
) -> PgResult<CachedPathSnapshot> {
    spcache_init();

    spcache_insert(searchPath, roleid);
    let key: SearchPathCacheKey = (searchPath.to_string(), roleid);

    /*
     * An OOM may have resulted in a cache entry with missing 'oidlist' or
     * 'finalPath', so just compute whatever is missing. (The C NIL test is an
     * is_empty() test here; a legitimately empty oidlist is recomputed each
     * time, exactly as in C.)
     *
     * The computations run outside the STATE borrow (they call back into
     * get_namespace_oid and the seams, which touch STATE).
     */
    let have_oidlist = STATE.with(|s| {
        let st = s.borrow();
        !st.search_path_cache.as_ref().unwrap()[&key].oidlist.is_empty()
    });

    let mut new_oidlist: Option<(Vec<Oid>, bool)> = None;
    if !have_oidlist {
        new_oidlist = Some(preprocessNamespacePath(mcx, searchPath, roleid)?);
    }
    if let Some((oidlist, temp_missing)) = &new_oidlist {
        STATE.with(|s| {
            let mut st = s.borrow_mut();
            let e = st.search_path_cache.as_mut().unwrap().get_mut(&key).unwrap();
            e.oidlist = oidlist.clone();
            e.temp_missing = *temp_missing;
        });
    }

    /*
     * If a hook is set, we must recompute finalPath from the oidlist each
     * time, because the hook may affect the result. This is still much
     * faster than recomputing from the string (and doing catalog lookups and
     * ACL checks).
     */
    let object_access_hook = objectaccess_seams::object_access_hook_present::call();
    let (have_final_path, force_recompute, oidlist_for_final) = STATE.with(|s| {
        let st = s.borrow();
        let e = &st.search_path_cache.as_ref().unwrap()[&key];
        (!e.final_path.is_empty(), e.force_recompute, e.oidlist.clone())
    });

    if !have_final_path || object_access_hook || force_recompute {
        /* list_free(entry->finalPath); entry->finalPath = NIL */
        STATE.with(|s| {
            let mut st = s.borrow_mut();
            st.search_path_cache
                .as_mut()
                .unwrap()
                .get_mut(&key)
                .unwrap()
                .final_path = Vec::new();
        });

        let (final_path, first_ns) = finalNamespacePath(&oidlist_for_final)?;

        STATE.with(|s| {
            let mut st = s.borrow_mut();
            let e = st.search_path_cache.as_mut().unwrap().get_mut(&key).unwrap();
            e.final_path = final_path;
            e.first_ns = first_ns;

            /*
             * If an object_access_hook is set when finalPath is calculated,
             * the result may be affected by the hook. Force recomputation of
             * finalPath the next time this cache entry is used, even if the
             * object_access_hook is not set at that time.
             */
            e.force_recompute = object_access_hook;
        });
    }

    STATE.with(|s| {
        let st = s.borrow();
        let e = &st.search_path_cache.as_ref().unwrap()[&key];
        Ok(CachedPathSnapshot {
            final_path: e.final_path.clone(),
            first_ns: e.first_ns,
            temp_missing: e.temp_missing,
        })
    })
}

/* ===========================================================================
 * recomputeNamespacePath (C lines 4302-4354)
 * ======================================================================== */

/// `recomputeNamespacePath` — recompute path derived variables if needed.
fn recomputeNamespacePath(mcx: Mcx<'_>) -> PgResult<()> {
    let roleid = miscinit_seams::get_user_id::call();

    /* Do nothing if path is already valid. */
    let already_valid = STATE.with(|s| {
        let st = s.borrow();
        st.base_search_path_valid && st.namespace_user == roleid
    });
    if already_valid {
        return Ok(());
    }

    let search_path = namespace_search_path();
    let entry = cachedNamespacePath(mcx, &search_path, roleid)?;

    let pathChanged = STATE.with(|s| {
        let st = s.borrow();
        !(st.base_creation_namespace == entry.first_ns
            && st.base_temp_creation_pending == entry.temp_missing
            && st.base_search_path == entry.final_path)
    });

    if pathChanged {
        STATE.with(|s| {
            let mut st = s.borrow_mut();
            /* Must save OID list in permanent storage (C: list_copy into
             * TopMemoryContext, list_free the old baseSearchPath; here the
             * owned Vec replacement frees the old one). */
            st.base_search_path = entry.final_path.clone();
            st.base_creation_namespace = entry.first_ns;
            st.base_temp_creation_pending = entry.temp_missing;
        });
    }

    STATE.with(|s| {
        let mut st = s.borrow_mut();
        /* Mark the path valid. */
        st.base_search_path_valid = true;
        st.namespace_user = roleid;

        /* And make it active. (C: activeSearchPath aliases baseSearchPath;
         * here it is a snapshot clone.) */
        st.active_search_path = st.base_search_path.clone();
        st.active_creation_namespace = st.base_creation_namespace;
        st.active_temp_creation_pending = st.base_temp_creation_pending;

        /*
         * Bump the generation only if something actually changed. (Notice
         * that what we compared to was the old state of the base path
         * variables.)
         */
        if pathChanged {
            st.active_path_generation += 1;
        }
    });

    Ok(())
}

/* ===========================================================================
 * AccessTempTableNamespace / InitTempTableNamespace (C lines 4365-4509)
 * ======================================================================== */

/// `AccessTempTableNamespace` — provide access to a temporary namespace,
/// potentially creating it if not present yet. This routine registers if the
/// namespace gets in use in this transaction. `force` can be set true to
/// enforce the creation of the temporary namespace for use in this backend,
/// which happens if its creation is pending.
fn AccessTempTableNamespace(mcx: Mcx<'_>, force: bool) -> PgResult<()> {
    /*
     * Make note that this temporary namespace has been accessed in this
     * transaction.
     */
    xact_seams::set_xact_accessed_temp_namespace::call();

    /*
     * If the caller attempting to access a temporary schema expects the
     * creation of the namespace to be pending and should be enforced, then go
     * through the creation.
     */
    if !force && OidIsValid(my_temp_namespace()) {
        return Ok(());
    }

    /*
     * The temporary tablespace does not exist yet and is wanted, so
     * initialize it.
     */
    InitTempTableNamespace(mcx)
}

/// `InitTempTableNamespace` — initialize temp table namespace on first use in
/// a particular backend.
fn InitTempTableNamespace(mcx: Mcx<'_>) -> PgResult<()> {
    debug_assert!(!OidIsValid(my_temp_namespace()));

    /*
     * First, do permission check to see if we are authorized to make temp
     * tables. We use a nonstandard error message here since "databasename:
     * permission denied" might be a tad cryptic.
     */
    if aclchk_seams::object_aclcheck::call(
        DATABASE_RELATION_ID,
        globals_seams::my_database_id::call(),
        miscinit_seams::get_user_id::call(),
        ACL_CREATE_TEMP,
    )? != AclResult::AclcheckOk
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied to create temporary tables in database \"{}\"",
                dbcommands_seams::get_database_name::call(
                    mcx,
                    globals_seams::my_database_id::call()
                )?
                .as_ref()
                .map(|s| s.as_str())
                .unwrap_or_default()
            ))
            .finish(here("InitTempTableNamespace"));
    }

    /*
     * Do not allow a Hot Standby session to make temp tables. Aside from
     * problems with modifying the system catalogs, there is a naming
     * conflict: pg_temp_N belongs to the session with proc number N on the
     * primary, not to a hot standby session with the same proc number.
     */
    if xlog_seams::recovery_in_progress::call() {
        return ereport(ERROR)
            .errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION)
            .errmsg("cannot create temporary tables during recovery")
            .finish(here("InitTempTableNamespace"));
    }

    /* Parallel workers can't create temporary tables, either. */
    if parallel_seams::is_parallel_worker() {
        return ereport(ERROR)
            .errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION)
            .errmsg("cannot create temporary tables during a parallel operation")
            .finish(here("InitTempTableNamespace"));
    }

    let my_proc_number = globals_seams::my_proc_number::call();
    let namespaceName = format!("pg_temp_{my_proc_number}");

    let mut namespaceId = get_namespace_oid(&namespaceName, true)?;
    if !OidIsValid(namespaceId) {
        /*
         * First use of this temp namespace in this database; create it. The
         * temp namespaces are always owned by the superuser. We leave their
         * permissions at default --- i.e., no access except to superuser ---
         * to ensure that unprivileged users can't peek at other backends'
         * temp tables. This works because the places that access the temp
         * namespace for my own backend skip permissions checks on it.
         */
        namespaceId =
            pg_namespace_seams::namespace_create::call(&namespaceName, BOOTSTRAP_SUPERUSERID, true)?;
        /* Advance command counter to make namespace visible */
        xact_seams::command_counter_increment::call()?;
    } else {
        /*
         * If the namespace already exists, clean it out (in case the former
         * owner crashed without doing so).
         */
        RemoveTempRelations(namespaceId)?;
    }

    /*
     * If the corresponding toast-table namespace doesn't exist yet, create
     * it. (We assume there is no need to clean it out if it does exist, since
     * dropping a parent table should make its toast table go away.)
     */
    let toastNamespaceName = format!("pg_toast_temp_{my_proc_number}");

    let mut toastspaceId = get_namespace_oid(&toastNamespaceName, true)?;
    if !OidIsValid(toastspaceId) {
        toastspaceId = pg_namespace_seams::namespace_create::call(
            &toastNamespaceName,
            BOOTSTRAP_SUPERUSERID,
            true,
        )?;
        /* Advance command counter to make namespace visible */
        xact_seams::command_counter_increment::call()?;
    }

    /*
     * Okay, we've prepared the temp namespace ... but it's not committed yet,
     * so all our work could be undone by transaction rollback. Set flag for
     * AtEOXact_Namespace to know what to do.
     */
    let sub_id = xact_seams::get_current_sub_transaction_id::call();
    STATE.with(|s| {
        let mut st = s.borrow_mut();
        st.my_temp_namespace = namespaceId;
        st.my_temp_toast_namespace = toastspaceId;
        /* It should not be done already. */
        debug_assert_eq!(st.my_temp_namespace_sub_id, InvalidSubTransactionId);
        st.my_temp_namespace_sub_id = sub_id;
        st.base_search_path_valid = false; /* need to rebuild list */
        st.search_path_cache_valid = false;
    });

    /*
     * Mark MyProc as owning this namespace which other processes can use to
     * decide if a temporary namespace is in use or not. We assume that
     * assignment of namespaceId is an atomic operation.
     */
    proc_seams::set_my_proc_temp_namespace_id::call(namespaceId);

    Ok(())
}

/* ===========================================================================
 * AtEOXact_Namespace / AtEOSubXact_Namespace (C lines 4515-4590)
 * ======================================================================== */

/// `AtEOXact_Namespace` — end-of-transaction cleanup for namespaces.
pub fn AtEOXact_Namespace(isCommit: bool, parallel: bool) -> PgResult<()> {
    /*
     * If we abort the transaction in which a temp namespace was selected,
     * we'll have to do any creation or cleanout work over again. So, just
     * forget the namespace entirely until next time. On the other hand, if
     * we commit then register an exit callback to clean out the temp tables
     * at backend shutdown. (We only want to register the callback once per
     * session, so this is a good place to do it.)
     */
    let should_handle =
        STATE.with(|s| s.borrow().my_temp_namespace_sub_id != InvalidSubTransactionId && !parallel);

    if should_handle {
        if isCommit {
            ipc_seams::before_shmem_exit::call(
                RemoveTempRelationsCallback,
                ::types_tuple::Datum::null(),
            )?;
        } else {
            STATE.with(|s| {
                let mut st = s.borrow_mut();
                st.my_temp_namespace = InvalidOid;
                st.my_temp_toast_namespace = InvalidOid;
                st.base_search_path_valid = false; /* need to rebuild list */
                st.search_path_cache_valid = false;
            });

            /*
             * Reset the temporary namespace flag in MyProc. We assume that
             * this operation is atomic.
             */
            proc_seams::set_my_proc_temp_namespace_id::call(InvalidOid);
        }
        STATE.with(|s| {
            s.borrow_mut().my_temp_namespace_sub_id = InvalidSubTransactionId;
        });
    }

    Ok(())
}

/// `AtEOSubXact_Namespace` — at subtransaction commit, propagate the
/// temp-namespace-creation flag to the parent subtransaction; at
/// subtransaction abort, forget the flag and reset state.
pub fn AtEOSubXact_Namespace(
    isCommit: bool,
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
) -> PgResult<()> {
    let matches = STATE.with(|s| s.borrow().my_temp_namespace_sub_id == mySubid);
    if matches {
        if isCommit {
            STATE.with(|s| s.borrow_mut().my_temp_namespace_sub_id = parentSubid);
        } else {
            STATE.with(|s| {
                let mut st = s.borrow_mut();
                st.my_temp_namespace_sub_id = InvalidSubTransactionId;
                /* TEMP namespace creation failed, so reset state */
                st.my_temp_namespace = InvalidOid;
                st.my_temp_toast_namespace = InvalidOid;
                st.base_search_path_valid = false; /* need to rebuild list */
                st.search_path_cache_valid = false;
            });

            /*
             * Reset the temporary namespace flag in MyProc. We assume that
             * this operation is atomic.
             */
            proc_seams::set_my_proc_temp_namespace_id::call(InvalidOid);
        }
    }

    Ok(())
}

/* ===========================================================================
 * RemoveTempRelations / RemoveTempRelationsCallback / ResetTempTableNamespace
 * (C lines 4601-4651)
 * ======================================================================== */

/// `RemoveTempRelations` — remove all relations in the specified temp
/// namespace.
///
/// This is called at backend shutdown (if we made any temp relations). It is
/// also called when we begin using a pre-existing temp namespace, in order to
/// clean out any relations that might have been created by a crashed backend.
fn RemoveTempRelations(tempNamespaceId: Oid) -> PgResult<()> {
    /*
     * We want to get rid of everything in the target namespace, but not the
     * namespace itself (deleting it only to recreate it later would be a
     * waste of cycles). Hence, specify SKIP_ORIGINAL. It's also an INTERNAL
     * deletion, and we want to not drop any extensions that might happen to
     * own temp objects.
     */
    dependency_seams::perform_deletion::call(
        NAMESPACE_RELATION_ID,
        tempNamespaceId,
        0,
        ::nodes::parsenodes::DROP_CASCADE,
        PERFORM_DELETION_INTERNAL
            | PERFORM_DELETION_QUIETLY
            | PERFORM_DELETION_SKIP_ORIGINAL
            | PERFORM_DELETION_SKIP_EXTENSIONS,
    )
}

/// `RemoveTempRelationsCallback` — remove temp relations at backend exit
/// (registered as a `before_shmem_exit` callback; the C `(code, arg)`
/// parameters are unused).
pub fn RemoveTempRelationsCallback(_code: i32, _arg: ::types_tuple::Datum<'static>) -> PgResult<()> {
    if OidIsValid(my_temp_namespace()) {
        /* should always be true */
        /* Need to ensure we have a usable transaction. */
        xact_seams::abort_out_of_any_transaction::call()?;
        xact_seams::start_transaction_command::call()?;

        /* C: PushActiveSnapshot(GetTransactionSnapshot()) / PopActiveSnapshot()
         * around the deletion — a snapmgr-owned scope here, per
         * docs/query-lifecycle-raii.md (no ambient snapshot stack). */
        snapmgr_seams::with_transaction_snapshot::call(&mut || {
            RemoveTempRelations(my_temp_namespace())
        })?;

        xact_seams::commit_transaction_command::call()?;
    }
    Ok(())
}

/// `ResetTempTableNamespace` — remove all temp tables from the temporary
/// namespace.
pub fn ResetTempTableNamespace() -> PgResult<()> {
    if OidIsValid(my_temp_namespace()) {
        RemoveTempRelations(my_temp_namespace())?;
    }
    Ok(())
}

/* ===========================================================================
 * GUC hooks: check_search_path / assign_search_path / InitializeSearchPath /
 * InvalidationCallback (C lines 4660-4808)
 * ======================================================================== */

/// `check_search_path` — check_hook: validate new search_path value.
///
/// The only requirement is syntactic validity of the identifier list (there
/// are many valid use-cases for schemas that don't exist, and we often are
/// not inside a transaction here).
pub fn check_search_path(mcx: Mcx<'_>, newval: &str) -> PgResult<bool> {
    let mut roleid: Oid = InvalidOid;
    let searchPath = newval;
    let use_cache = STATE.with(|s| s.borrow().search_path_cache_context_created);

    /*
     * Checking only the syntactic validity also allows us to use the search
     * path cache (if available) to avoid calling SplitIdentifierString() on
     * the same string repeatedly.
     */
    if use_cache {
        spcache_init();

        roleid = miscinit_seams::get_user_id::call();

        if spcache_lookup(searchPath, roleid) {
            return Ok(true);
        }
    }

    /*
     * Ensure validity check succeeds before creating cache entry.
     */
    /* Parse string into list of identifiers */
    if varlena_seams::split_identifier_string::call(mcx, searchPath, ',')?.is_none() {
        /* syntax error in name list */
        guc_seams::guc_check_errdetail::call("List syntax is invalid.".to_string());
        return Ok(false);
    }

    /* OK to create empty cache entry */
    if use_cache {
        spcache_insert(searchPath, roleid);
    }

    Ok(true)
}

/// `assign_search_path` — assign_hook: do extra actions as needed.
pub fn assign_search_path(_newval: &str) {
    /* don't access search_path during bootstrap */
    debug_assert!(!miscinit_seams::is_bootstrap_processing_mode::call());

    /*
     * We mark the path as needing recomputation, but don't do anything until
     * it's needed. This avoids trying to do database access during GUC
     * initialization, or outside a transaction.
     *
     * This does not invalidate the search path cache, so if this value had
     * been previously set and no syscache invalidations happened,
     * recomputation may not be necessary.
     */
    STATE.with(|s| s.borrow_mut().base_search_path_valid = false);
}

/// Slot-shaped wrapper installing `check_search_path` as `search_path`'s
/// GUC `check_hook` (the hook signature the GUC machinery invokes). Supplies a
/// scratch context for the identifier-list split / cache.
fn check_search_path_hook(
    newval: &mut Option<String>,
    _extra: &mut Option<guc_tables::GucHookExtra>,
    _source: types_guc::GucSource,
) -> PgResult<bool> {
    let scratch = MemoryContext::new("check_search_path");
    let s = newval.as_deref().unwrap_or("");
    check_search_path(scratch.mcx(), s)
}

/// Slot-shaped wrapper installing `assign_search_path` as `search_path`'s
/// GUC `assign_hook`.
fn assign_search_path_hook(
    newval: Option<&str>,
    _extra: Option<&guc_tables::GucHookExtra>,
) {
    assign_search_path(newval.unwrap_or(""));
}

/// `InitializeSearchPath` — initialize module during InitPostgres (called
/// after we are up enough to be able to do catalog lookups).
pub fn InitializeSearchPath() -> PgResult<()> {
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        /*
         * In bootstrap mode, the search path must be 'pg_catalog' so that
         * tables are created in the proper namespace; ignore the GUC setting.
         */
        let user = miscinit_seams::get_user_id::call();
        STATE.with(|s| {
            let mut st = s.borrow_mut();
            /* baseSearchPath = list_make1_oid(PG_CATALOG_NAMESPACE), in
             * TopMemoryContext (here: the owned Vec replacement). */
            st.base_search_path = vec![PG_CATALOG_NAMESPACE];
            st.base_creation_namespace = PG_CATALOG_NAMESPACE;
            st.base_temp_creation_pending = false;
            st.base_search_path_valid = true;
            st.namespace_user = user;
            st.active_search_path = st.base_search_path.clone();
            st.active_creation_namespace = st.base_creation_namespace;
            st.active_temp_creation_pending = st.base_temp_creation_pending;
            st.active_path_generation += 1; /* pro forma */
        });
    } else {
        /*
         * In normal mode, arrange for a callback on any syscache invalidation
         * that will affect the search_path cache.
         */

        /* namespace name or ACLs may have changed */
        inval_seams::cache_register_syscache_callback::call(
            NAMESPACEOID,
            |_, _, _| InvalidationCallback(),
            Datum::null(),
        )?;

        /* role name may affect the meaning of "$user" */
        inval_seams::cache_register_syscache_callback::call(
            AUTHOID,
            |_, _, _| InvalidationCallback(),
            Datum::null(),
        )?;

        /* role membership may affect ACLs */
        inval_seams::cache_register_syscache_callback::call(
            AUTHMEMROLEMEM,
            |_, _, _| InvalidationCallback(),
            Datum::null(),
        )?;

        /* database owner may affect ACLs */
        inval_seams::cache_register_syscache_callback::call(
            DATABASEOID,
            |_, _, _| InvalidationCallback(),
            Datum::null(),
        )?;

        /* Force search path to be recomputed on next use */
        STATE.with(|s| {
            let mut st = s.borrow_mut();
            st.base_search_path_valid = false;
            st.search_path_cache_valid = false;
        });
    }

    Ok(())
}

/// `InvalidationCallback` — syscache inval callback function.
pub fn InvalidationCallback() {
    /*
     * Force search path to be recomputed on next use, also invalidating the
     * search path cache.
     */
    STATE.with(|s| {
        let mut st = s.borrow_mut();
        st.base_search_path_valid = false;
        st.search_path_cache_valid = false;
    });
}

/* ===========================================================================
 * fetch_search_path / fetch_search_path_array (C lines 4822-4882)
 * ======================================================================== */

/// `fetch_search_path` — fetch the active search path, expressed as a List of
/// OIDs.
///
/// The returned list includes the implicitly-prepended namespaces only if
/// `includeImplicit` is true.
pub fn fetch_search_path<'mcx>(
    mcx: Mcx<'mcx>,
    includeImplicit: bool,
) -> PgResult<PgVec<'mcx, Oid>> {
    recomputeNamespacePath(mcx)?;

    /*
     * If the temp namespace should be first, force it to exist. This is so
     * that callers can trust the result to reflect the actual default
     * creation namespace. It's a bit bogus to do this here, since
     * current_schema() is supposedly a stable function without side-effects,
     * but the alternatives seem worse.
     */
    if STATE.with(|s| s.borrow().active_temp_creation_pending) {
        AccessTempTableNamespace(mcx, true)?;
        recomputeNamespacePath(mcx)?;
    }

    /* list_copy(activeSearchPath) into the caller's context */
    let (mut result, active_creation) = STATE.with(|s| -> PgResult<_> {
        let st = s.borrow();
        Ok((
            slice_in(mcx, &st.active_search_path)?,
            st.active_creation_namespace,
        ))
    })?;
    if !includeImplicit {
        while !result.is_empty() && result[0] != active_creation {
            result.remove(0);
        }
    }

    Ok(result)
}

/// `fetch_search_path_array` — fetch the active search path into a
/// caller-allocated array, returning the number of path entries. (If this is
/// more than the array length, the extra entries were not stored.)
pub fn fetch_search_path_array(mcx: Mcx<'_>, sarray: &mut [Oid]) -> PgResult<i32> {
    let mut count = 0;

    recomputeNamespacePath(mcx)?;

    for namespaceId in active_search_path() {
        if namespaceId == my_temp_namespace() {
            continue; /* do not include temp namespace */
        }

        if (count as usize) < sarray.len() {
            sarray[count as usize] = namespaceId;
        }
        count += 1;
    }

    Ok(count)
}

/* ===========================================================================
 * SQL-callable FooIsVisible wrappers (C lines 4897-5089)
 *
 * Each is a `PG_FUNCTION_ARGS` entry point: run the `*IsVisibleExt`
 * predicate, return NULL (`None`) if the object was missing.
 * ======================================================================== */

type IsVisibleFn = fn(Mcx<'_>, Oid, Option<&mut bool>) -> PgResult<bool>;

fn pg_is_visible_body(mcx: Mcx<'_>, oid: Oid, f: IsVisibleFn) -> PgResult<Option<bool>> {
    let mut is_missing = false;
    let result = f(mcx, oid, Some(&mut is_missing))?;
    if is_missing {
        Ok(None)
    } else {
        Ok(Some(result))
    }
}

/// `pg_table_is_visible(oid) -> bool` (NULL if missing).
pub fn pg_table_is_visible(mcx: Mcx<'_>, oid: Oid) -> PgResult<Option<bool>> {
    pg_is_visible_body(mcx, oid, RelationIsVisibleExt)
}

/// `pg_type_is_visible(oid) -> bool`.
pub fn pg_type_is_visible(mcx: Mcx<'_>, oid: Oid) -> PgResult<Option<bool>> {
    pg_is_visible_body(mcx, oid, TypeIsVisibleExt)
}

/// `pg_function_is_visible(oid) -> bool`.
pub fn pg_function_is_visible(mcx: Mcx<'_>, oid: Oid) -> PgResult<Option<bool>> {
    pg_is_visible_body(mcx, oid, FunctionIsVisibleExt)
}

/// `pg_operator_is_visible(oid) -> bool`.
pub fn pg_operator_is_visible(mcx: Mcx<'_>, oid: Oid) -> PgResult<Option<bool>> {
    pg_is_visible_body(mcx, oid, OperatorIsVisibleExt)
}

/// `pg_opclass_is_visible(oid) -> bool`.
pub fn pg_opclass_is_visible(mcx: Mcx<'_>, oid: Oid) -> PgResult<Option<bool>> {
    pg_is_visible_body(mcx, oid, OpclassIsVisibleExt)
}

/// `pg_opfamily_is_visible(oid) -> bool`.
pub fn pg_opfamily_is_visible(mcx: Mcx<'_>, oid: Oid) -> PgResult<Option<bool>> {
    pg_is_visible_body(mcx, oid, OpfamilyIsVisibleExt)
}

/// `pg_collation_is_visible(oid) -> bool`.
pub fn pg_collation_is_visible(mcx: Mcx<'_>, oid: Oid) -> PgResult<Option<bool>> {
    pg_is_visible_body(mcx, oid, CollationIsVisibleExt)
}

/// `pg_conversion_is_visible(oid) -> bool`.
pub fn pg_conversion_is_visible(mcx: Mcx<'_>, oid: Oid) -> PgResult<Option<bool>> {
    pg_is_visible_body(mcx, oid, ConversionIsVisibleExt)
}

/// `pg_statistics_obj_is_visible(oid) -> bool`.
pub fn pg_statistics_obj_is_visible(mcx: Mcx<'_>, oid: Oid) -> PgResult<Option<bool>> {
    pg_is_visible_body(mcx, oid, StatisticsObjIsVisibleExt)
}

/// `pg_ts_parser_is_visible(oid) -> bool`.
pub fn pg_ts_parser_is_visible(mcx: Mcx<'_>, oid: Oid) -> PgResult<Option<bool>> {
    pg_is_visible_body(mcx, oid, TSParserIsVisibleExt)
}

/// `pg_ts_dict_is_visible(oid) -> bool`.
pub fn pg_ts_dict_is_visible(mcx: Mcx<'_>, oid: Oid) -> PgResult<Option<bool>> {
    pg_is_visible_body(mcx, oid, TSDictionaryIsVisibleExt)
}

/// `pg_ts_template_is_visible(oid) -> bool`.
pub fn pg_ts_template_is_visible(mcx: Mcx<'_>, oid: Oid) -> PgResult<Option<bool>> {
    pg_is_visible_body(mcx, oid, TSTemplateIsVisibleExt)
}

/// `pg_ts_config_is_visible(oid) -> bool`.
pub fn pg_ts_config_is_visible(mcx: Mcx<'_>, oid: Oid) -> PgResult<Option<bool>> {
    pg_is_visible_body(mcx, oid, TSConfigIsVisibleExt)
}

/// `pg_my_temp_schema() -> oid` (zero if none).
pub fn pg_my_temp_schema() -> Oid {
    my_temp_namespace()
}

/// `pg_is_other_temp_schema(oid) -> bool`.
pub fn pg_is_other_temp_schema(mcx: Mcx<'_>, oid: Oid) -> PgResult<bool> {
    isOtherTempNamespace(mcx, oid)
}
