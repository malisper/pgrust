//! Seam declarations for the `backend-utils-cache-syscache` unit
//! (`utils/cache/syscache.c` `SearchSysCache*` reads), expressed as
//! caller-shaped projected catalog rows.
//!
//! The owning unit (`backend-utils-cache-syscache`) installs these from its
//! `init_seams()` (catcache lookup + attribute extraction + field projection
//! — thin marshal only). A cache miss is `Ok(None)` / an empty list — the
//! caller raises its own `cache lookup failed` error, as in C.
//!
//! The projected rows are copies out of the catcache (the cache entries live
//! in `CacheMemoryContext`), so each lookup takes the caller's `Mcx` and the
//! allocated outputs carry its lifetime; `Err` includes OOM from the copy.

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_hash::backend_access_hash_hashvalidate::{AmopRow, AmprocRow, OpclassForm};
use mcx::PgString;
use types_namespace::{CatalogObjectName, OperRow, ProcRow};
use types_cache::AuthIdRow;
use types_partition::PartrelTupleData;

seam_core::seam!(
    /// `SearchSysCache2(ATTNAME, ObjectIdGetDatum(relid),
    /// CStringGetDatum(colname))` projected to the attribute's
    /// `(attnum, attisdropped)` (`Form_pg_attribute`). `Ok(None)` on a cache
    /// miss (`!HeapTupleIsValid`), distinguishing "no such pg_attribute row"
    /// from a present-but-dropped column (`Some((_, true))`); the installer
    /// owns the `ReleaseSysCache`. acl.c's `convert_column_name` consumes this
    /// to treat dropped columns differently from missing ones.
    pub fn search_attname_attnum(
        relid: Oid,
        colname: &str,
    ) -> PgResult<Option<(types_core::AttrNumber, bool)>>
);

seam_core::seam!(
    /// `SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid))` projected to the
    /// `pg_authid` fields role-identity callers read. `Ok(None)` on cache miss.
    pub fn lookup_authid_by_oid<'mcx>(
        mcx: Mcx<'mcx>,
        roleid: Oid,
    ) -> PgResult<Option<AuthIdRow<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(AUTHNAME, PointerGetDatum(rolename))` projected to the
    /// `pg_authid` fields role-identity callers read. `Ok(None)` on cache miss.
    pub fn lookup_authid_by_name<'mcx>(
        mcx: Mcx<'mcx>,
        rolename: &str,
    ) -> PgResult<Option<AuthIdRow<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(PARTRELID, ObjectIdGetDatum(relid))` +
    /// `GETSTRUCT(Form_pg_partitioned_table)` +
    /// `SysCacheGetAttrNotNull(partclass/partcollation)` +
    /// `SysCacheGetAttr(partexprs)`, with the `partexprs` `pg_node_tree`
    /// de-stringized (`stringToNode`), const-simplified
    /// (`eval_const_expressions`), opfuncid-fixed (`fix_opfuncids`), then
    /// `copyObject` (partcache.c:94-166). The `int2vector`/`oidvector` columns
    /// are decoded to value slices, all allocated in `mcx`. Returns `Ok(None)`
    /// when `!HeapTupleIsValid(tuple)` so the caller raises the exact
    /// `elog(ERROR, "cache lookup failed for partition key of relation %u")`.
    /// The `ReleaseSysCache` is subsumed by returning the data by value.
    pub fn open_partrel_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
    ) -> PgResult<Option<PartrelTupleData<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` projected to the
    /// `Form_pg_class.relam` field (the relation's access method). `Ok(None)`
    /// on a cache miss (`!HeapTupleIsValid`); the installer owns the
    /// `ReleaseSysCache`.
    pub fn search_relation_relam(relid: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `SearchSysCacheExists1(AUTHOID, ObjectIdGetDatum(roleid))`
    /// (utils/cache/syscache.c): does a pg_authid row for this role OID exist?
    /// Used to confirm a role wasn't concurrently dropped. `Err` carries the
    /// catcache lookup's own error surface.
    pub fn auth_oid_exists(roleid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid))` projected to the
    /// `Form_pg_opclass` fields the hash validator reads, copied into `mcx`.
    /// `Ok(None)` on a cache miss (`!HeapTupleIsValid`).
    pub fn search_opclass<'mcx>(
        mcx: Mcx<'mcx>,
        opclassoid: Oid,
    ) -> PgResult<Option<OpclassForm<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid))`
    /// member rows, projected and copied into `mcx`.
    pub fn search_amop_list<'mcx>(
        mcx: Mcx<'mcx>,
        opfamilyoid: Oid,
    ) -> PgResult<PgVec<'mcx, AmopRow>>
);

seam_core::seam!(
    /// `SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid))`
    /// member rows, projected and copied into `mcx`.
    pub fn search_amproc_list<'mcx>(
        mcx: Mcx<'mcx>,
        opfamilyoid: Oid,
    ) -> PgResult<PgVec<'mcx, AmprocRow>>
);

seam_core::seam!(
    /// `SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid))` projected to
    /// `NameStr(Form_pg_type->typname)`, copied into `mcx`. `Ok(None)` on a
    /// cache miss (`!HeapTupleIsValid`); `Err` includes OOM from the copy.
    pub fn search_type_name<'mcx>(
        mcx: Mcx<'mcx>,
        typoid: Oid,
    ) -> PgResult<Option<mcx::PgString<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nspid))` projected to
    /// `NameStr(Form_pg_namespace->nspname)`, copied into `mcx` (C: `pstrdup`).
    /// `Ok(None)` on a cache miss (`!HeapTupleIsValid`); the installer owns the
    /// `ReleaseSysCache`. `Err` includes OOM from the copy. Consumed by
    /// lsyscache.c's `get_namespace_name`.
    pub fn search_namespace_name<'mcx>(
        mcx: Mcx<'mcx>,
        nspid: Oid,
    ) -> PgResult<Option<mcx::PgString<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(AMOID, ObjectIdGetDatum(amOid))` projected to
    /// `NameStr(Form_pg_am->amname)`, copied into `mcx` (C: `pstrdup`).
    /// `Ok(None)` on a cache miss (`!HeapTupleIsValid`); the installer owns the
    /// `ReleaseSysCache`. `Err` includes OOM from the copy. Consumed by
    /// lsyscache.c's `get_am_name`.
    pub fn search_am_name<'mcx>(
        mcx: Mcx<'mcx>,
        am_oid: Oid,
    ) -> PgResult<Option<mcx::PgString<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(TYPEOID, ObjectIdGetDatum(oidtypeid))` +
    /// `GETSTRUCT(Form_pg_type)` projected to the type-dependent attribute
    /// fields `TupleDescInitEntry` (`access/common/tupdesc.c`) stamps onto a
    /// `Form_pg_attribute` (`typlen`/`typbyval`/`typalign`/`typstorage`/
    /// `typcollation`). `Ok(None)` on a cache miss (`!HeapTupleIsValid`), so the
    /// caller raises the exact `elog(ERROR, "cache lookup failed for type %u")`.
    /// The fields are `Copy`, so no `mcx` is needed; the installer owns the
    /// `ReleaseSysCache`.
    pub fn search_type_attr_info(
        oidtypeid: Oid,
    ) -> PgResult<Option<types_tuple::backend_access_common_tupdesc::PgTypeInfo>>
);

seam_core::seam!(
    /// `GetSysCacheOid1(NAMESPACENAME, Anum_pg_namespace_oid, nspname)`.
    pub fn get_namespace_oid_cached(nspname: &str) -> PgResult<Oid>
);

seam_core::seam!(
    /// `GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid, typname, nsp)`.
    pub fn get_type_oid(typname: &str, namespace_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `SearchSysCacheExists2(TYPENAMENSP, typname, nsp)`.
    pub fn type_exists(typname: &str, namespace_id: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `GetSysCacheOid3(CLAAMNAMENSP, Anum_pg_opclass_oid, amid, opcname, nsp)`.
    pub fn get_opclass_oid(amid: Oid, opcname: &str, namespace_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `GetSysCacheOid3(OPFAMILYAMNAMENSP, Anum_pg_opfamily_oid, amid, opfname, nsp)`.
    pub fn get_opfamily_oid(amid: Oid, opfname: &str, namespace_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `SearchSysCacheExists3(OPFAMILYAMNAMENSP, amoid, opfname, nsp)`.
    pub fn opfamily_exists(amoid: Oid, opfname: &str, namespace_id: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `SearchSysCacheExists3(CLAAMNAMENSP, amoid, opcname, nsp)`.
    pub fn opclass_exists(amoid: Oid, opcname: &str, namespace_id: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `GetSysCacheOid4(AMOPSTRATEGY, Anum_pg_amop_oid, opfamilyoid, lefttype,
    /// righttype, strategy)` — the `pg_amop` row's OID, or `InvalidOid`.
    pub fn amop_oid(
        opfamilyoid: Oid,
        lefttype: Oid,
        righttype: Oid,
        strategy: i16,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `GetSysCacheOid4(AMPROCNUM, Anum_pg_amproc_oid, opfamilyoid, lefttype,
    /// righttype, procnum)` — the `pg_amproc` row's OID, or `InvalidOid`.
    pub fn amproc_oid(
        opfamilyoid: Oid,
        lefttype: Oid,
        righttype: Oid,
        procnum: i16,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `GetSysCacheOid2(CONNAMENSP, Anum_pg_conversion_oid, conname, nsp)`.
    pub fn get_conversion_oid_cached(conname: &str, namespace_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `GetSysCacheOid2(STATEXTNAMENSP, Anum_pg_statistic_ext_oid, name, nsp)`.
    pub fn get_statext_oid(stats_name: &str, namespace_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `SearchSysCacheExists2(STATEXTNAMENSP, name, nsp)`.
    pub fn statext_exists(stats_name: &str, namespace_id: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `GetSysCacheOid2(TSPARSERNAMENSP, Anum_pg_ts_parser_oid, name, nsp)`.
    pub fn get_ts_parser_oid_cached(name: &str, namespace_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `SearchSysCacheExists2(TSPARSERNAMENSP, name, nsp)`.
    pub fn ts_parser_exists(name: &str, namespace_id: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `GetSysCacheOid2(TSDICTNAMENSP, Anum_pg_ts_dict_oid, name, nsp)`.
    pub fn get_ts_dict_oid_cached(name: &str, namespace_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `SearchSysCacheExists2(TSDICTNAMENSP, name, nsp)`.
    pub fn ts_dict_exists(name: &str, namespace_id: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `GetSysCacheOid2(TSTEMPLATENAMENSP, Anum_pg_ts_template_oid, name, nsp)`.
    pub fn get_ts_template_oid_cached(name: &str, namespace_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `SearchSysCacheExists2(TSTEMPLATENAMENSP, name, nsp)`.
    pub fn ts_template_exists(name: &str, namespace_id: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `GetSysCacheOid2(TSCONFIGNAMENSP, Anum_pg_ts_config_oid, name, nsp)`.
    pub fn get_ts_config_oid_cached(name: &str, namespace_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `SearchSysCacheExists2(TSCONFIGNAMENSP, name, nsp)`.
    pub fn ts_config_exists(name: &str, namespace_id: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `GetSysCacheOid3(COLLNAMEENCNSP, Anum_pg_collation_oid, collname,
    /// encoding, nsp)` — the encoding-specific (or, with `encoding = -1`,
    /// any-encoding) collation probe of `lookup_collation`.
    pub fn get_collation_oid_by_name_enc_nsp(
        collname: &str,
        encoding: i32,
        namespace_id: Oid,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `SearchSysCache3(COLLNAMEENCNSP, collname, -1, nsp)` projected to
    /// `(collform->oid, collform->collprovider)`; `Ok(None)` on cache miss.
    /// The any-encoding row read of `lookup_collation`.
    pub fn collation_any_encoding_row(
        collname: &str,
        namespace_id: Oid,
    ) -> PgResult<Option<(Oid, u8)>>
);

seam_core::seam!(
    /// `SearchSysCache1(AUTHOID, roleid)` projected to `rolname`, copied
    /// into `mcx`; `Ok(None)` on cache miss.
    pub fn authid_rolname<'mcx>(mcx: Mcx<'mcx>, roleid: Oid) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache4(OPERNAMENSP, opername, oprleft, oprright, nsp)`
    /// projected to the operator OID; `InvalidOid` on cache miss
    /// (`OpernameGetOprid`'s exact-schema probe).
    pub fn oper_exact(
        opername: &str,
        oprleft: Oid,
        oprright: Oid,
        namespace_id: Oid,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `SearchSysCache1(RELOID, relid)` projected to
    /// `(relnamespace, relname)`; `Ok(None)` on cache miss.
    pub fn relation_namespace_and_name<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<Option<CatalogObjectName<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(TYPEOID, typid)` projected to
    /// `(typnamespace, typname)`; `Ok(None)` on cache miss.
    pub fn type_namespace_and_name<'mcx>(mcx: Mcx<'mcx>, typid: Oid) -> PgResult<Option<CatalogObjectName<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(TYPEOID, typid)` projected to the `Form_pg_type`
    /// fields `format_type_extended` reads (`typelem`, `typsubscript`,
    /// `typstorage`, `typmodout`, `typnamespace`, `typname`); `Ok(None)` on a
    /// cache miss (the caller raises its own `cache lookup failed`, as in C).
    pub fn type_form<'mcx>(mcx: Mcx<'mcx>, typid: Oid) -> PgResult<Option<types_format_type::TypeFormInfo<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(COLLOID, collid)` projected to
    /// `(collnamespace, collname)`; `Ok(None)` on cache miss.
    pub fn collation_namespace_and_name<'mcx>(mcx: Mcx<'mcx>, collid: Oid) -> PgResult<Option<CatalogObjectName<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(CONVOID, conid)` projected to
    /// `(connamespace, conname)`; `Ok(None)` on cache miss.
    pub fn conversion_namespace_and_name<'mcx>(mcx: Mcx<'mcx>, conid: Oid) -> PgResult<Option<CatalogObjectName<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(STATEXTOID, stxid)` projected to
    /// `(stxnamespace, stxname)`; `Ok(None)` on cache miss.
    pub fn statext_namespace_and_name<'mcx>(mcx: Mcx<'mcx>, stxid: Oid) -> PgResult<Option<CatalogObjectName<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(TSPARSEROID, prsid)` projected to
    /// `(prsnamespace, prsname)`; `Ok(None)` on cache miss.
    pub fn ts_parser_namespace_and_name<'mcx>(mcx: Mcx<'mcx>, prsid: Oid) -> PgResult<Option<CatalogObjectName<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(TSDICTOID, dictid)` projected to
    /// `(dictnamespace, dictname)`; `Ok(None)` on cache miss.
    pub fn ts_dict_namespace_and_name<'mcx>(mcx: Mcx<'mcx>, dictid: Oid) -> PgResult<Option<CatalogObjectName<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(TSTEMPLATEOID, tmplid)` projected to
    /// `(tmplnamespace, tmplname)`; `Ok(None)` on cache miss.
    pub fn ts_template_namespace_and_name<'mcx>(mcx: Mcx<'mcx>, tmplid: Oid) -> PgResult<Option<CatalogObjectName<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(TSCONFIGOID, cfgid)` projected to
    /// `(cfgnamespace, cfgname)`; `Ok(None)` on cache miss.
    pub fn ts_config_namespace_and_name<'mcx>(mcx: Mcx<'mcx>, cfgid: Oid) -> PgResult<Option<CatalogObjectName<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(CLAOID, opcid)` projected to
    /// `(opcnamespace, opcmethod, opcname)`; `Ok(None)` on cache miss.
    pub fn opclass_namespace_method_name<'mcx>(
        mcx: Mcx<'mcx>,
        opcid: Oid,
    ) -> PgResult<Option<(Oid, Oid, PgString<'mcx>)>>
);

seam_core::seam!(
    /// `SearchSysCache1(OPFAMILYOID, opfid)` projected to
    /// `(opfnamespace, opfmethod, opfname)`; `Ok(None)` on cache miss.
    pub fn opfamily_namespace_method_name<'mcx>(
        mcx: Mcx<'mcx>,
        opfid: Oid,
    ) -> PgResult<Option<(Oid, Oid, PgString<'mcx>)>>
);

seam_core::seam!(
    /// `SearchSysCache1(PROCOID, funcid)` projected to the [`ProcRow`]
    /// fields; `Ok(None)` on cache miss (`FunctionIsVisibleExt`).
    pub fn proc_row_by_oid<'mcx>(mcx: Mcx<'mcx>, funcid: Oid) -> PgResult<Option<ProcRow<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(OPEROID, oprid)` projected to the [`OperRow`]
    /// fields; `Ok(None)` on cache miss (`OperatorIsVisibleExt`).
    pub fn oper_row_by_oid<'mcx>(mcx: Mcx<'mcx>, oprid: Oid) -> PgResult<Option<OperRow<'mcx>>>
);

seam_core::seam!(
    /// `SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_proargnames, &isnull)`
    /// null test for the pg_proc row of `funcid` (`MatchNamedCall`'s probe;
    /// the C caller holds the tuple, the owned marshal re-fetches it by
    /// OID). `Err` carries catcache-path `ereport(ERROR)`s (including a
    /// missing row, impossible for a tuple the caller just read).
    pub fn proc_proargnames_isnull(funcid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `SearchSysCacheList1(PROCNAMEARGSNSP, funcname)` projected member
    /// rows in catlist order, plus `catlist->ordered`.
    pub fn proc_catlist<'mcx>(
        mcx: Mcx<'mcx>,
        funcname: &str,
    ) -> PgResult<(PgVec<'mcx, ProcRow<'mcx>>, bool)>
);

seam_core::seam!(
    /// `SearchSysCacheList3(OPERNAMENSP, opername, oprleft, oprright)`
    /// projected member rows in catlist order, plus `catlist->ordered`.
    pub fn oper_catlist3<'mcx>(
        mcx: Mcx<'mcx>,
        opername: &str,
        oprleft: Oid,
        oprright: Oid,
    ) -> PgResult<(PgVec<'mcx, OperRow<'mcx>>, bool)>
);

seam_core::seam!(
    /// `SearchSysCacheList1(OPERNAMENSP, opername)` projected member rows in
    /// catlist order, plus `catlist->ordered`.
    pub fn oper_catlist1<'mcx>(
        mcx: Mcx<'mcx>,
        opername: &str,
    ) -> PgResult<(PgVec<'mcx, OperRow<'mcx>>, bool)>
);

/* ------------------------------------------------------------------------
 *  Catalog-tuple field projections for inval.c (`GETSTRUCT` reads).
 *
 *  inval.c deforms the on-disk `Form_*` struct of a catalog tuple to decide
 *  which relcache/snapshot invalidation a heap-tuple change implies. The
 *  deform lives behind syscache (it owns the tupdescs), so each projection
 *  takes a borrowed `HeapTupleData` and returns just the field(s) inval.c
 *  reads. Pure deform reads; infallible.
 * ------------------------------------------------------------------------ */

seam_core::seam!(
    /// `((Form_pg_class) GETSTRUCT(tuple))` projected to `{ oid, relisshared }`
    /// — the `pg_class` fields inval.c reads to route a relcache invalidation.
    pub fn pg_class_shape(
        tuple: &types_tuple::HeapTupleData<'_>,
    ) -> types_storage::PgClassShape
);

seam_core::seam!(
    /// `((Form_pg_attribute) GETSTRUCT(tuple))->attrelid` — the table a
    /// `pg_attribute` tuple belongs to.
    pub fn pg_attribute_attrelid(tuple: &types_tuple::HeapTupleData<'_>) -> Oid
);

seam_core::seam!(
    /// `((Form_pg_index) GETSTRUCT(tuple))->indexrelid` — the index OID of a
    /// `pg_index` tuple.
    pub fn pg_index_indexrelid(tuple: &types_tuple::HeapTupleData<'_>) -> Oid
);

seam_core::seam!(
    /// The FK target table of a `pg_constraint` tuple: for a foreign-key
    /// constraint (`contype == CONSTRAINT_FOREIGN`), `Form_pg_constraint.confrelid`;
    /// `None` for any other constraint type (inval.c skips those).
    pub fn pg_constraint_fk_target(tuple: &types_tuple::HeapTupleData<'_>) -> Option<Oid>
);

seam_core::seam!(
    /// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` projected to
    /// `{ oid, relisshared }` (the `CacheInvalidateRelcacheByRelid` lookup);
    /// `Ok(None)` on a cache miss (`!HeapTupleIsValid`). The installer owns the
    /// `ReleaseSysCache`. `Err` carries the underlying catalog-scan errors.
    pub fn lookup_pg_class_by_relid(relid: Oid) -> PgResult<Option<types_storage::PgClassShape>>
);

seam_core::seam!(
    /// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` projected to the
    /// `Form_pg_class.relrowsecurity`/`relforcerowsecurity` flags
    /// (`utils/misc/rls.c`). `Ok(None)` on a cache miss (`!HeapTupleIsValid`);
    /// the installer owns the `ReleaseSysCache`.
    pub fn search_relation_rls_flags(relid: Oid) -> PgResult<Option<(bool, bool)>>
);

seam_core::seam!(
    /// `SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid))` projected to the
    /// `Form_pg_authid.rolsuper` flag (`utils/misc/superuser.c`). `Ok(None)`
    /// on a cache miss (`!HeapTupleIsValid`), where `superuser_arg` treats the
    /// role as a non-superuser; the installer owns the `ReleaseSysCache`.
    pub fn search_authid_rolsuper(roleid: Oid) -> PgResult<Option<bool>>
);

seam_core::seam!(
    /// `SearchSysCache1(PROCOID, ObjectIdGetDatum(functionId))` projected to the
    /// catalog facts the function manager reads (`fmgr_info_cxt_security` /
    /// `fmgr_symbol` / `fmgr_security_definer` / `CheckFunctionValidatorAccess`):
    /// `pronargs`/`proisstrict`/`proretset`/`prolang`/`prosrc`/`probin`/
    /// `prosecdef`/`proowner`/`proname` and the `TransformGUCArray`'d `proconfig`
    /// names+values, plus the folded `prosecdef || proconfig-not-null` predicate.
    /// `Ok(None)` on a cache miss (`!HeapTupleIsValid`) — the caller raises
    /// `cache lookup failed for function %u`, as in C. The projected strings are
    /// copied into the caller's `Mcx`; `Err` includes OOM from the copy.
    pub fn lookup_proc<'mcx>(
        mcx: Mcx<'mcx>,
        function_id: Oid,
    ) -> PgResult<Option<types_fmgr::ProcInfo<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(LANGOID, ObjectIdGetDatum(language))` projected to
    /// `lanplcallfoid`/`lanvalidator`/`NameStr(lanname)`
    /// (`fmgr_info_other_lang` / `CheckFunctionValidatorAccess`). `Ok(None)` on a
    /// cache miss — the caller raises `cache lookup failed for language %u`. The
    /// `lanname` copy is charged to the caller's `Mcx`.
    pub fn lookup_language<'mcx>(
        mcx: Mcx<'mcx>,
        language_id: Oid,
    ) -> PgResult<Option<types_fmgr::LangInfo<'mcx>>>
);

/* ---- CLUSTER pg_class / pg_index writable copies (backend-commands-cluster) */

seam_core::seam!(
    /// `SearchSysCacheExists1(RELOID, indexOid)` (syscache.c).
    pub fn search_syscache_exists_reloid(reloid: Oid) -> PgResult<bool>
);
seam_core::seam!(
    /// `SearchSysCacheCopy1(RELOID, relid)` + `GETSTRUCT` (syscache.c): the
    /// writable pg_class row and its `t_self`; `None` on a cache miss.
    pub fn search_syscache_copy_pg_class<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
    ) -> PgResult<Option<(types_tuple::heaptuple::ItemPointerData, types_cluster::PgClassForm)>>
);
seam_core::seam!(
    /// `SearchSysCacheCopy1(INDEXRELID, indexOid)` + `GETSTRUCT` (syscache.c):
    /// the writable pg_index row and its `t_self`; `None` on a cache miss.
    pub fn search_syscache_copy_pg_index<'mcx>(
        mcx: Mcx<'mcx>,
        index_oid: Oid,
    ) -> PgResult<Option<(types_tuple::heaptuple::ItemPointerData, types_cluster::PgIndexForm)>>
);
seam_core::seam!(
    /// `SearchSysCacheAttName(relid, attname)` + `GETSTRUCT` (syscache.c):
    /// look up a column of `relid` by name, returning its
    /// `(attnum, atttypid)`; `None` when no such (non-dropped) column exists
    /// (the C `!HeapTupleIsValid(atttuple)`).
    pub fn search_syscache_attname(
        relid: Oid,
        attname: &str,
    ) -> PgResult<Option<(types_core::primitive::AttrNumber, Oid)>>
);
seam_core::seam!(
    /// `SearchSysCache1 + SysCacheGetAttr(Anum_pg_class_reloptions) +
    /// ReleaseSysCache` (the make_new_heap reloptions fetch): the pg_class
    /// reloptions token (NULL when unset). `Err` "cache lookup failed for
    /// relation %u" when the tuple is missing.
    pub fn fetch_class_reloptions<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
    ) -> PgResult<types_cluster::RelOptionsToken>
);

seam_core::seam!(
    /// The collation's schema-qualified name for `ri_GenerateQualCollation`:
    /// `SearchSysCache1(COLLOID)` then `(get_namespace_name(collnamespace),
    /// NameStr(collname))`, both copied into `mcx` as raw name bytes.
    /// `Ok(None)` on a cache miss (the C `elog(ERROR)`s); `Err` carries OOM.
    pub fn collation_qualified_name<'mcx>(
        mcx: Mcx<'mcx>,
        collation: Oid,
    ) -> PgResult<Option<(PgVec<'mcx, u8>, PgVec<'mcx, u8>)>>
);

seam_core::seam!(
    /// `GetSysCacheHashValue1(DATABASEOID, ObjectIdGetDatum(dbid))`
    /// (`utils/adt/acl.c` `initialize_acl`): the syscache hash value of the
    /// `pg_database` row keyed by `dbid`, cached to filter `DATABASEOID`
    /// invalidations for other databases. `Err` carries lookup failure.
    pub fn database_syscache_hash_value(dbid: Oid) -> PgResult<u32>
);

seam_core::seam!(
    /// `SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbid))` +
    /// `GETSTRUCT(Form_pg_database)->datdba` + `ReleaseSysCache`
    /// (`roles_is_member_of`, acl.c): the owning role of database `dbid`.
    /// `Ok(None)` on a cache miss so the caller raises the exact
    /// `elog(ERROR, "cache lookup failed for database %u")`.
    pub fn database_datdba(dbid: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `SearchSysCacheList1(AUTHMEMMEMROLE, ObjectIdGetDatum(memberid))`
    /// (`roles_is_member_of`, acl.c): the `pg_auth_members` rows where
    /// `member == memberid`, each projected to the
    /// `roleid`/`admin_option`/`inherit_option`/`set_option` fields the caller
    /// reads off `GETSTRUCT(Form_pg_auth_members)`. The catlist is copied into
    /// `mcx` and `ReleaseSysCacheList` is subsumed by returning the rows by
    /// value (the rows are plain scalars, so an owned `Vec` carries them with
    /// no lifetime). `Err` carries OOM from the copy.
    pub fn auth_members_of_member(
        memberid: Oid,
    ) -> PgResult<Vec<types_cache::AuthMembersRow>>
);
