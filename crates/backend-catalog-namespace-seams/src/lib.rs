//! Seam declarations for the `backend-catalog-namespace` unit
//! (`catalog/namespace.c`), search-path-aware object name resolution.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_core::SubTransactionId;
use types_error::PgResult;
use types_namespace::FuncCandidateList;
use types_storage::lock::LOCKMODE;
use types_tuple::access::RangeVar;

seam_core::seam!(
    /// `TypeIsVisible(typid)` (namespace.c): whether the type is visible in
    /// the current search path (so it can be referenced unqualified). Reads
    /// the syscache; `Err` carries the cache-lookup `elog(ERROR)` and OOM.
    pub fn type_is_visible(mcx: Mcx<'_>, typid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `isTempNamespace(namespaceId)` (namespace.c): whether the given
    /// namespace OID is this backend's temporary-schema namespace
    /// (`namespaceId == myTempNamespace && OidIsValid(myTempNamespace)`). No
    /// catalog access, so infallible; `Err` is reserved for the per-owner
    /// error channel only. Consumed by lsyscache.c's
    /// `get_namespace_name_or_temp`.
    pub fn is_temp_namespace(namespace_id: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `get_ts_config_oid(names, missing_ok)` (namespace.c): the OID of a
    /// text-search configuration given its possibly-qualified name list.
    /// With `missing_ok = false` a missing configuration raises
    /// `ERRCODE_UNDEFINED_OBJECT` (`Err`); with `missing_ok = true` it is
    /// `Ok(InvalidOid)`.
    pub fn get_ts_config_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_namespace_oid(nspname, missing_ok)` (namespace.c): the
    /// namespace's OID; with `missing_ok = false` a missing schema raises
    /// `ERRCODE_UNDEFINED_SCHEMA`, carried on `Err`.
    pub fn get_namespace_oid(nspname: &str, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `makeRangeVarFromNameList(names)` (namespace.c): build a `RangeVar`
    /// from a 1-to-3-element qualified name list (relname / schema.relname /
    /// catalog.schema.relname). More than three dotted names raises
    /// `ERRCODE_SYNTAX_ERROR` (`Err`). The `RangeVar` strings are owned
    /// `String`s (the type's own representation).
    pub fn make_range_var_from_name_list(names: &[&str]) -> PgResult<RangeVar>
);

seam_core::seam!(
    /// `RangeVarGetRelid(relation, lockmode, missing_ok)` (namespace.h macro
    /// over `RangeVarGetRelidExtended` with no callback and `RVR_MISSING_OK`
    /// per `missing_ok`). `mcx` is the C current context the lookup's
    /// transient catalog copies are made in.
    pub fn range_var_get_relid(
        mcx: Mcx<'_>,
        relation: &RangeVar,
        lockmode: LOCKMODE,
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `LookupExplicitNamespace(nspname, missing_ok)` (namespace.c): resolve
    /// an explicit schema name and verify USAGE rights.
    pub fn lookup_explicit_namespace(nspname: &str, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `RangeVarGetRelid(makeRangeVarFromNameList(textToQualifiedNameList(name)),
    /// lockmode, missing_ok)` — the SQL-function idiom for resolving a
    /// possibly-qualified relation name text to its OID without holding a lock
    /// (callers that lack privileges to lock it). With `missing_ok = false` a
    /// missing relation raises `ERRCODE_UNDEFINED_TABLE`, carried on `Err`.
    pub fn range_var_get_relid_from_text(
        mcx: Mcx<'_>,
        name: &str,
        lockmode: LOCKMODE,
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `AtEOXact_Namespace(isCommit, parallel)` — end-of-xact temp-namespace
    /// and search-path cleanup.
    pub fn at_eoxact_namespace(is_commit: bool, parallel: bool)
);

seam_core::seam!(
    /// `AtEOSubXact_Namespace(isCommit, mySubid, parentSubid)`.
    pub fn at_eosubxact_namespace(
        is_commit: bool,
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
    )
);

seam_core::seam!(
    /// `FuncnameGetCandidates(names, nargs, argnames, expand_variadic,
    /// expand_defaults, include_out_arguments, missing_ok)` (namespace.c):
    /// search-path-aware lookup of all function candidates with the given
    /// (possibly schema-qualified, dotted-component) `names`. `nargs = -1`
    /// disables the arity filter (the `regprocin` "name only" probe).
    pub fn funcname_get_candidates<'mcx>(
        mcx: Mcx<'mcx>,
        names: &[&str],
        nargs: i32,
        argnames: &[&str],
        expand_variadic: bool,
        expand_defaults: bool,
        include_out_arguments: bool,
        missing_ok: bool,
    ) -> PgResult<FuncCandidateList<'mcx>>
);

seam_core::seam!(
    /// `OpernameGetCandidates(names, oprkind, missing_schema_ok)`
    /// (namespace.c): search-path-aware lookup of operator candidates by
    /// name. `oprkind` is the C `char` (`'\0'` = any kind).
    pub fn opername_get_candidates<'mcx>(
        mcx: Mcx<'mcx>,
        names: &[&str],
        oprkind: u8,
        missing_schema_ok: bool,
    ) -> PgResult<FuncCandidateList<'mcx>>
);

seam_core::seam!(
    /// `OpernameGetOprid(names, oprleft, oprright)` (namespace.c): the OID of
    /// the operator with the given name and left/right argument type OIDs
    /// (`InvalidOid` for a missing argument), or `InvalidOid` if none.
    pub fn opername_get_oprid(
        mcx: Mcx<'_>,
        names: &[&str],
        oprleft: Oid,
        oprright: Oid,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_collation_oid(collname, missing_ok)` (namespace.c): the OID of a
    /// collation usable for the current database encoding, found by the
    /// possibly-qualified `collname` along the search path. With
    /// `missing_ok = false` a miss raises `ERRCODE_UNDEFINED_OBJECT`.
    pub fn get_collation_oid(
        mcx: Mcx<'_>,
        collname: &[&str],
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_ts_dict_oid(names, missing_ok)` (namespace.c): the OID of a text
    /// search dictionary given its possibly-qualified name list. With
    /// `missing_ok = false` a miss raises `ERRCODE_UNDEFINED_OBJECT`.
    pub fn get_ts_dict_oid(
        mcx: Mcx<'_>,
        names: &[&str],
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_conversion_oid(conname, missing_ok)` (namespace.c): the OID of a
    /// conversion given its possibly-qualified name list. With
    /// `missing_ok = false` a miss raises `ERRCODE_UNDEFINED_OBJECT`.
    pub fn get_conversion_oid(
        mcx: Mcx<'_>,
        names: &[&str],
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_ts_parser_oid(names, missing_ok)` (namespace.c): the OID of a text
    /// search parser given its possibly-qualified name list. With
    /// `missing_ok = false` a miss raises `ERRCODE_UNDEFINED_OBJECT`.
    pub fn get_ts_parser_oid(
        mcx: Mcx<'_>,
        names: &[&str],
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_ts_template_oid(names, missing_ok)` (namespace.c): the OID of a
    /// text search template given its possibly-qualified name list. With
    /// `missing_ok = false` a miss raises `ERRCODE_UNDEFINED_OBJECT`.
    pub fn get_ts_template_oid(
        mcx: Mcx<'_>,
        names: &[&str],
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_statistics_object_oid(names, missing_ok)` (namespace.c): the OID of
    /// an extended statistics object given its possibly-qualified name list.
    /// With `missing_ok = false` a miss raises `ERRCODE_UNDEFINED_OBJECT`.
    pub fn get_statistics_object_oid(
        mcx: Mcx<'_>,
        names: &[&str],
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `RelationIsVisible(relid)` (namespace.c): whether `relid` is visible
    /// in the current search path (would be found unqualified).
    pub fn relation_is_visible(mcx: Mcx<'_>, relid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `FunctionIsVisible(funcid)` (namespace.c).
    pub fn function_is_visible(mcx: Mcx<'_>, funcid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `OperatorIsVisible(oprid)` (namespace.c).
    pub fn operator_is_visible(mcx: Mcx<'_>, oprid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `CollationIsVisible(collid)` (namespace.c).
    pub fn collation_is_visible(mcx: Mcx<'_>, collid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `TSConfigIsVisible(cfgid)` (namespace.c).
    pub fn ts_config_is_visible(mcx: Mcx<'_>, cfgid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `TSDictionaryIsVisible(dictId)` (namespace.c).
    pub fn ts_dictionary_is_visible(mcx: Mcx<'_>, dict_id: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `ConversionIsVisible(conid)` (namespace.c).
    pub fn conversion_is_visible(mcx: Mcx<'_>, conid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `OpclassIsVisible(opcid)` (namespace.c).
    pub fn opclass_is_visible(mcx: Mcx<'_>, opcid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `OpfamilyIsVisible(opfid)` (namespace.c).
    pub fn opfamily_is_visible(mcx: Mcx<'_>, opfid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `StatisticsObjIsVisible(stxid)` (namespace.c).
    pub fn statistics_obj_is_visible(mcx: Mcx<'_>, stxid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `TSParserIsVisible(prsId)` (namespace.c).
    pub fn ts_parser_is_visible(mcx: Mcx<'_>, prs_id: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `TSTemplateIsVisible(tmplId)` (namespace.c).
    pub fn ts_template_is_visible(mcx: Mcx<'_>, tmpl_id: Oid) -> PgResult<bool>
);

/* ---- CLUSTER target resolution (used by backend-commands-cluster) -------- */

seam_core::seam!(
    /// `RangeVarGetRelidExtended(relation, AccessExclusiveLock, 0,
    /// RangeVarCallbackMaintainsTable, NULL)` (namespace.c): resolve+lock the
    /// CLUSTER target, running the maintains-table permission callback.
    pub fn range_var_get_relid_maintains_table<'mcx>(
        mcx: Mcx<'mcx>,
        relation: &RangeVar,
        lockmode: LOCKMODE,
    ) -> PgResult<Oid>
);
seam_core::seam!(
    /// `LookupCreationNamespace(nspname)` (namespace.c): OID of the namespace
    /// to create in (`pg_temp` for temp); `Err` on ACL/lookup failure.
    pub fn lookup_creation_namespace(nspname: &str) -> PgResult<Oid>
);
seam_core::seam!(
    /// `fetch_search_path(includeImplicit)` (namespace.c): the active search
    /// path as a list of namespace OIDs, copied into `mcx` (C: `list_copy`).
    /// The implicitly-prepended namespaces are included only when
    /// `include_implicit` is true. An empty path is an empty list (C: `NIL`).
    /// `Err` carries the `recomputeNamespacePath` / temp-namespace-creation
    /// `ereport(ERROR)` surface and OOM.
    pub fn fetch_search_path<'mcx>(
        mcx: Mcx<'mcx>,
        include_implicit: bool,
    ) -> PgResult<PgVec<'mcx, Oid>>
);
// (RestrictSearchPath re-homed to backend-utils-misc-guc-seams — it is guc.c's
// function (guc.c:2246), not namespace.c's — and installed by the merged guc
// owner. Consumers call it there.)



seam_core::seam!(
    /// `RangeVarGetAndCheckCreationNamespace(relation, NoLock, &existing_relid)`
    /// (namespace.c) — sequence.c `DefineSequence` if_not_exists pre-check.
    /// Takes the K1 owned-tree `RangeVar` node (the node model CreateSeqStmt
    /// carries). Returns any pre-existing relation OID (`InvalidOid` if none).
    /// NOTE: the namespace owner is still on the legacy
    /// `types_tuple::access::RangeVar` model and has not migrated to the K1
    /// owned-tree node, so it cannot yet install this; this is the contract the
    /// migrated owner will install. `Err` carries ACL/lookup ereports.
    pub fn range_var_get_and_check_creation_namespace(
        relation: &types_nodes::rawnodes::RangeVar<'_>,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `RangeVarGetRelidExtended(relation, ShareRowExclusiveLock,
    /// missing_ok ? RVR_MISSING_OK : 0, RangeVarCallbackOwnsRelation, NULL)`
    /// (namespace.c) — sequence.c `AlterSequence` open-and-own-check. Takes the
    /// K1 owned-tree `RangeVar`. Returns `InvalidOid` when missing_ok and the
    /// relation is absent. Same K1-node migration note as
    /// `range_var_get_and_check_creation_namespace`.
    pub fn range_var_get_relid_owns_seq(
        relation: &types_nodes::rawnodes::RangeVar<'_>,
        missing_ok: bool,
    ) -> PgResult<Oid>
);
