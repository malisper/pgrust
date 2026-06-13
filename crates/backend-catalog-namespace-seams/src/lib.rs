//! Seam declarations for the `backend-catalog-namespace` unit
//! (`catalog/namespace.c`), search-path-aware object name resolution.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::Mcx;
use types_core::Oid;
use types_core::SubTransactionId;
use types_error::PgResult;
use types_namespace::FuncCandidateList;
use types_storage::lock::LOCKMODE;
use types_tuple::access::RangeVar;

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
    /// `makeRangeVarFromNameList(names)` (namespace.c): build a `RangeVar`
    /// from a one-, two-, or three-element qualified-name list; a longer or
    /// empty list raises `ERRCODE_SYNTAX_ERROR`.
    pub fn make_range_var_from_name_list(
        names: &[&str],
    ) -> PgResult<RangeVar>
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
