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
use types_namespace::{CatalogObjectName, FuncProcAttrs, OperRow, ProcRow};
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
    /// `SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relid))`: whether a
    /// `pg_class` row exists for `relid`.
    pub fn reloid_exists(relid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `SearchSysCacheExists1(TABLESPACEOID, ObjectIdGetDatum(tblspc))`:
    /// whether a `pg_tablespace` row exists for `tblspc`.
    pub fn tablespace_exists(tblspc: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `SearchSysCacheExists1(AUTHOID, ObjectIdGetDatum(roleid))`
    /// (utils/cache/syscache.c): does a pg_authid row for this role OID exist?
    /// Used to confirm a role wasn't concurrently dropped. `Err` carries the
    /// catcache lookup's own error surface.
    pub fn auth_oid_exists(roleid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `SearchSysCacheExists1(NAMESPACENAME, PointerGetDatum(nspName))`
    /// (utils/cache/syscache.c): does a `pg_namespace` row with this name
    /// exist? pg_namespace.c's `NamespaceCreate` uses it for the duplicate-name
    /// check. `Err` carries the catcache lookup's own error surface.
    pub fn namespace_name_exists(nsp_name: &str) -> PgResult<bool>
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
    /// `SearchSysCache1(PROCOID, funcid)` projected to the [`FuncProcAttrs`]
    /// (`Form_pg_proc` `GETSTRUCT` scalars + the `SysCacheGetAttr` array
    /// columns `proallargtypes` / `proargmodes` / `proargnames` /
    /// `protrftypes`, each detoasted and deconstructed). `Ok(None)` on a cache
    /// miss (`!HeapTupleIsValid`); the funcapi caller raises its own
    /// `cache lookup failed for function %u` `elog(ERROR)`, as in C. The
    /// shape-validity checks (and the `elog`s for malformed arrays) stay on
    /// the funcapi consumer; the seam only projects.
    pub fn proc_arg_attrs<'mcx>(
        mcx: Mcx<'mcx>,
        funcid: Oid,
    ) -> PgResult<Option<FuncProcAttrs<'mcx>>>
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
    /// `SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid))` projected to the
    /// `pg_proc` facts `internal_get_result_type` (funcapi.c) reads to classify
    /// a function's result type: `prorettype`/`proretset`/`pronargs`/
    /// `proargtypes` (the declared input-type `oidvector`) and `NameStr(proname)`
    /// for the error message. `Ok(None)` on a cache miss (`!HeapTupleIsValid`) —
    /// the caller raises `cache lookup failed for function %u`, as in C. The
    /// `proargtypes` vector and `proname` are copied into the caller's `Mcx`;
    /// `Err` includes OOM from the copy.
    pub fn lookup_proc_result_info<'mcx>(
        mcx: Mcx<'mcx>,
        funcid: Oid,
    ) -> PgResult<Option<types_fmgr::ProcResultInfo<'mcx>>>
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
    /// `SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexOid))` +
    /// `heap_copytuple` projected to the `pg_index` row the relcache's
    /// `RelationInitIndexAccessInfo` consumes: the fixed `Form_pg_index`
    /// scalars plus the variable-length `indkey`/`indcollation`/`indclass`/
    /// `indoption` arrays (which the C reads off `rd_indextuple` with
    /// `fastgetattr`). `Ok(None)` on a cache miss (`!HeapTupleIsValid`); the
    /// caller raises `cache lookup failed for index %u`.
    pub fn search_pg_index_info<'mcx>(
        mcx: Mcx<'mcx>,
        index_oid: Oid,
    ) -> PgResult<Option<types_cache::PgIndexInfo<'mcx>>>
);
seam_core::seam!(
    /// `SearchSysCache1(AMOID, ObjectIdGetDatum(amoid))` +
    /// `GETSTRUCT(Form_pg_am)->amhandler` (syscache.c): the access method's
    /// handler-function OID. `Ok(None)` on a cache miss (`!HeapTupleIsValid`);
    /// the caller raises `cache lookup failed for access method %u`.
    pub fn search_am_handler(amoid: Oid) -> PgResult<Option<Oid>>
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
    /// `SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum))`
    /// + `NameStr(GETSTRUCT(Form_pg_attribute)->attname)` + `ReleaseSysCache`
    /// (the raw `ATTNUM` cache read behind `get_attname`, lsyscache.c). Returns
    /// the attribute's name copied into `mcx` (C: the `pstrdup` is the caller's,
    /// so the installer only copies the `NameData` bytes out of the cache
    /// entry). `Ok(None)` on a cache miss (`!HeapTupleIsValid`); unlike the
    /// `*AttNum` syscache helper this raw read does NOT filter dropped columns,
    /// matching `get_attname`. `Err` carries OOM from the copy.
    pub fn search_attnum_attname<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        attnum: types_core::AttrNumber,
    ) -> PgResult<Option<PgString<'mcx>>>
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

/* ------------------------------------------------------------------------
 *  pg_operator / pg_amop / pg_proc reads driven by lsyscache.c's
 *  operator-and-opfamily helpers (backend-utils-cache-lsyscache's
 *  `opfamily_operator` family). All are `SearchSysCache*(OPEROID / AMOPOPID /
 *  AMOPSTRATEGY / PROCOID)` probes projected to the few fields the caller
 *  reads off the `Form_pg_operator` / `Form_pg_amop` / `Form_pg_proc` struct.
 * ------------------------------------------------------------------------ */

/// `((Form_pg_amop) GETSTRUCT(tp))` projected to the fields lsyscache.c's
/// operator helpers read (`get_op_opfamily_properties`,
/// `get_ordering_op_properties`, `get_op_hash_functions`,
/// `get_opfamily_member`). A copy out of the catcache, so it carries no
/// lifetime.
#[derive(Clone, Copy, Debug, Default)]
pub struct AmopOpidRow {
    /// `amopmethod` — the index access method OID.
    pub amopmethod: Oid,
    /// `amopfamily` — the operator family OID.
    pub amopfamily: Oid,
    /// `amopstrategy` — the strategy number.
    pub amopstrategy: i16,
    /// `amoplefttype` — the operator's left input type.
    pub amoplefttype: Oid,
    /// `amoprighttype` — the operator's right input type.
    pub amoprighttype: Oid,
    /// `amopopr` — the operator's OID.
    pub amopopr: Oid,
}

seam_core::seam!(
    /// `SearchSysCache1(OPEROID, ObjectIdGetDatum(opno))` +
    /// `GETSTRUCT(Form_pg_operator)->oprcode` (`get_opcode`, lsyscache.c).
    /// `Ok(None)` on a cache miss (`!HeapTupleIsValid`); the caller returns
    /// `InvalidOid`. The installer owns the `ReleaseSysCache`.
    pub fn oper_oprcode(opno: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `SearchSysCache1(OPEROID, ObjectIdGetDatum(opno))` +
    /// `GETSTRUCT(Form_pg_operator)->oprcom` (`get_commutator`, lsyscache.c).
    /// `Ok(None)` on a cache miss (`!HeapTupleIsValid`); the caller returns
    /// `InvalidOid`. The installer owns the `ReleaseSysCache`.
    pub fn oper_oprcom(opno: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `SearchSysCache1(OPEROID, ObjectIdGetDatum(opno))` +
    /// `GETSTRUCT(Form_pg_operator)->(oprleft, oprright)` (`op_input_types`,
    /// lsyscache.c). `Ok(None)` on a cache miss (`!HeapTupleIsValid`), so the
    /// caller raises `cache lookup failed for operator %u`. The installer owns
    /// the `ReleaseSysCache`.
    pub fn oper_input_types(opno: Oid) -> PgResult<Option<(Oid, Oid)>>
);

seam_core::seam!(
    /// `SearchSysCache3(AMOPOPID, ObjectIdGetDatum(opno),
    /// CharGetDatum(purpose), ObjectIdGetDatum(opfamily))` projected to
    /// [`AmopOpidRow`] (`get_op_opfamily_properties` / `get_op_opfamily_sortfamily`,
    /// lsyscache.c). `purpose` is `AMOP_SEARCH` (`'s'`) or `AMOP_ORDER`
    /// (`'o'`). `Ok(None)` on a cache miss (`!HeapTupleIsValid`); the installer
    /// owns the `ReleaseSysCache`.
    pub fn amop_by_opr_purpose_family(
        opno: Oid,
        purpose: i8,
        opfamily: Oid,
    ) -> PgResult<Option<AmopOpidRow>>
);

seam_core::seam!(
    /// `SearchSysCache4(AMOPSTRATEGY, ObjectIdGetDatum(opfamily),
    /// ObjectIdGetDatum(lefttype), ObjectIdGetDatum(righttype),
    /// Int16GetDatum(strategy))` projected to [`AmopOpidRow`]
    /// (`get_opfamily_member`, lsyscache.c). `Ok(None)` on a cache miss
    /// (`!HeapTupleIsValid`); the caller returns `InvalidOid`. The installer
    /// owns the `ReleaseSysCache`.
    pub fn amop_by_strategy_full(
        opfamily: Oid,
        lefttype: Oid,
        righttype: Oid,
        strategy: i16,
    ) -> PgResult<Option<AmopOpidRow>>
);

seam_core::seam!(
    /// `SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno))` member rows,
    /// each projected to [`AmopOpidRow`] in catlist order
    /// (`get_ordering_op_properties` / `get_op_hash_functions`, lsyscache.c).
    /// The catlist is copied into `mcx`; `ReleaseSysCacheList` is subsumed by
    /// returning the rows by value. `Err` carries OOM from the copy.
    pub fn amop_list_by_opr<'mcx>(
        mcx: Mcx<'mcx>,
        opno: Oid,
    ) -> PgResult<PgVec<'mcx, AmopOpidRow>>
);

seam_core::seam!(
    /// `SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid))` +
    /// `GETSTRUCT(Form_pg_proc)->proisstrict` (`func_strict`, lsyscache.c).
    /// `Ok(None)` on a cache miss (`!HeapTupleIsValid`), so the caller raises
    /// `cache lookup failed for function %u`. The installer owns the
    /// `ReleaseSysCache`.
    pub fn proc_isstrict(funcid: Oid) -> PgResult<Option<bool>>
);

/* ---- lsyscache.c collation / constraint / language / cast / transform reads */

seam_core::seam!(
    /// `SearchSysCache1(COLLOID, ObjectIdGetDatum(colloid))` +
    /// `GETSTRUCT(Form_pg_collation)->collisdeterministic` + `ReleaseSysCache`
    /// (`get_collation_isdeterministic`, lsyscache.c). `Ok(None)` on a cache
    /// miss so the caller raises the exact `elog(ERROR, "cache lookup failed
    /// for collation %u")`.
    pub fn collation_isdeterministic(colloid: Oid) -> PgResult<Option<bool>>
);

seam_core::seam!(
    /// `SearchSysCache1(COLLOID, ObjectIdGetDatum(colloid))` +
    /// `NameStr(Form_pg_collation->collname)` copied into `mcx` (C: `pstrdup`)
    /// + `ReleaseSysCache` (`get_collation_name`, lsyscache.c). `Ok(None)` on a
    /// cache miss (the C NULL return). `Err` carries OOM from the copy.
    pub fn collation_name<'mcx>(
        mcx: Mcx<'mcx>,
        colloid: Oid,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(CONSTROID, ObjectIdGetDatum(conoid))` +
    /// `NameStr(Form_pg_constraint->conname)` copied into `mcx` (C: `pstrdup`)
    /// + `ReleaseSysCache` (`get_constraint_name`, lsyscache.c). `Ok(None)` on
    /// a cache miss (the C NULL return). `Err` carries OOM from the copy.
    pub fn constraint_name<'mcx>(
        mcx: Mcx<'mcx>,
        conoid: Oid,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(CONSTROID, ObjectIdGetDatum(conoid))` +
    /// `ReleaseSysCache` projected to `Form_pg_constraint`'s
    /// `(contype, conindid)` (`get_constraint_index` / `get_constraint_type`,
    /// lsyscache.c). `contype` is the raw `pg_constraint.contype` char; the
    /// caller applies the CONSTRAINT_{UNIQUE,PRIMARY,EXCLUSION} test and its
    /// own `cache lookup failed for constraint %u` error. `Ok(None)` on a cache
    /// miss.
    pub fn constraint_type_index(conoid: Oid) -> PgResult<Option<(u8, Oid)>>
);

seam_core::seam!(
    /// `SearchSysCache1(LANGOID, ObjectIdGetDatum(langoid))` +
    /// `NameStr(Form_pg_language->lanname)` copied into `mcx` (C: `pstrdup`) +
    /// `ReleaseSysCache` (`get_language_name`, lsyscache.c). `Ok(None)` on a
    /// cache miss; the caller raises `cache lookup failed for language %u` only
    /// when `!missing_ok`. `Err` carries OOM from the copy.
    pub fn language_name<'mcx>(
        mcx: Mcx<'mcx>,
        langoid: Oid,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `GetSysCacheOid2(CASTSOURCETARGET, Anum_pg_cast_oid,
    /// ObjectIdGetDatum(sourcetypeid), ObjectIdGetDatum(targettypeid))`
    /// (`get_cast_oid`, lsyscache.c): the `pg_cast.oid` of the cast between the
    /// two types, or `InvalidOid` (0) when there is none. `Err` carries the
    /// catcache-path `ereport(ERROR)`s.
    pub fn cast_oid(sourcetypeid: Oid, targettypeid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `SearchSysCache2(TRFTYPELANG, ObjectIdGetDatum(typid),
    /// ObjectIdGetDatum(langid))` + `ReleaseSysCache` projected to
    /// `Form_pg_transform`'s `(trffromsql, trftosql)`
    /// (`get_transform_fromsql` / `get_transform_tosql`, lsyscache.c).
    /// `Ok(None)` on a cache miss (the C `InvalidOid` return).
    pub fn transform_funcs(typid: Oid, langid: Oid) -> PgResult<Option<(Oid, Oid)>>
);

/* ------------------------------------------------------------------------
 *  lsyscache.c `pg_class` / `pg_index` reads (backend-utils-cache-lsyscache,
 *  `relation` family). Each is a `SearchSysCache1` + `GETSTRUCT` field read +
 *  `ReleaseSysCache`; a cache miss is `Ok(None)` so the caller applies the
 *  exact C "not found" return (NULL / '\0' / false / elog).
 * ------------------------------------------------------------------------ */

seam_core::seam!(
    /// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` projected to
    /// `pstrdup(NameStr(Form_pg_class.relname))` (`get_rel_name`), copied into
    /// `mcx`. `Ok(None)` on a cache miss (`!HeapTupleIsValid`); the installer
    /// owns the `ReleaseSysCache`. `Err` includes OOM from the copy.
    pub fn rel_name<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` projected to
    /// `Form_pg_class.relkind` (`get_rel_relkind`). `Ok(None)` on a cache miss
    /// (`!HeapTupleIsValid`); the installer owns the `ReleaseSysCache`.
    pub fn rel_relkind(relid: Oid) -> PgResult<Option<u8>>
);

seam_core::seam!(
    /// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` projected to
    /// `Form_pg_class.relispartition` (`get_rel_relispartition`). `Ok(None)`
    /// on a cache miss (`!HeapTupleIsValid`); the installer owns the
    /// `ReleaseSysCache`.
    pub fn rel_relispartition(relid: Oid) -> PgResult<Option<bool>>
);

seam_core::seam!(
    /// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` projected to
    /// `Form_pg_class.relnamespace` (`get_rel_namespace`). `Ok(None)` on a
    /// cache miss (`!HeapTupleIsValid`); the installer owns the
    /// `ReleaseSysCache`.
    pub fn rel_namespace(relid: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `GetSysCacheOid2(RELNAMENSP, Anum_pg_class_oid,
    /// PointerGetDatum(relname), ObjectIdGetDatum(relnamespace))`
    /// (`get_relname_relid`): the relation's OID or `InvalidOid`. `Err`
    /// carries the catcache-path `ereport(ERROR)`s.
    pub fn relname_relid(relname: &str, relnamespace: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid))` projected to
    /// `Form_pg_index.indisclustered` (`get_index_isclustered`). `Ok(None)` on
    /// a cache miss (`!HeapTupleIsValid`) so the caller raises the exact
    /// `elog(ERROR, "cache lookup failed for index %u")`; the installer owns
    /// the `ReleaseSysCache`.
    pub fn index_isclustered(index_oid: Oid) -> PgResult<Option<bool>>
);

seam_core::seam!(
    /// `SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid))` +
    /// `GETSTRUCT(Form_pg_type)` (`utils/cache/lsyscache.c` reads). Returns the
    /// fixed-length `pg_type` columns by value (every field through
    /// `typcollation`), or `Ok(None)` on a cache miss (`!HeapTupleIsValid`) so
    /// the caller raises its own `cache lookup failed for type %u`
    /// `elog(ERROR)`. The struct is `Copy`, so no `mcx` is needed; the
    /// installer owns the `ReleaseSysCache`.
    pub fn pg_type_form(
        typid: Oid,
    ) -> PgResult<Option<types_tuple::pg_type::FormData_pg_type>>
);

seam_core::seam!(
    /// `SearchSysCache1(RANGETYPE, ObjectIdGetDatum(rngtypid))` +
    /// `GETSTRUCT(Form_pg_range)`, projected to the `Form_pg_range` fields
    /// `load_rangetype_info` reads (`rngsubtype` / `rngcollation` / `rngsubopc`
    /// / `rngcanonical` / `rngsubdiff`). `Ok(None)` on a cache miss
    /// (`!HeapTupleIsValid`); the installer owns the `ReleaseSysCache`.
    pub fn pg_range_form(
        rngtypid: Oid,
    ) -> PgResult<Option<types_cache::typcache::PgRangeRow>>
);

seam_core::seam!(
    /// `SearchSysCache1(RANGEMULTIRANGE, ObjectIdGetDatum(mltrngtypid))`
    /// projected to `Form_pg_range.rngtypid` (the multirange's element range
    /// type). `Ok(None)` on a cache miss (`!HeapTupleIsValid`); the installer
    /// owns the `ReleaseSysCache`.
    pub fn pg_range_rngtypid_of_multirange(
        mltrngtypid: Oid,
    ) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `GetSysCacheHashValue1(TYPEOID, ObjectIdGetDatum(type_id))`
    /// (`utils/cache/syscache.c`) — the catcache hash value of the
    /// `pg_type` row, as stored in `TypeCacheEntry.type_id_hash`. `Err`
    /// carries the catcache machinery's `ereport(ERROR)`s.
    pub fn get_syscache_hash_value_typeoid(type_id: Oid) -> PgResult<u32>
);

/// `STATISTIC_NUM_SLOTS` (pg_statistic.h) — the number of statistics slots in
/// a `pg_statistic` row. C: `#define STATISTIC_NUM_SLOTS 5`.
pub const STATISTIC_NUM_SLOTS: usize = 5;

/// The fixed-width slot metadata of a `pg_statistic` `GETSTRUCT`
/// (`Form_pg_statistic`), namely the `stakindN` / `staopN` / `stacollN`
/// arrays that `get_attstatsslot` scans to find a matching slot. These are the
/// non-`CATALOG_VARLEN` slot fields, so they read straight off the struct with
/// no detoast.
#[derive(Clone, Copy, Debug, Default)]
pub struct PgStatisticSlotMeta {
    /// `(&stats->stakind1)[i]` for `i in 0..STATISTIC_NUM_SLOTS`.
    pub stakind: [i16; STATISTIC_NUM_SLOTS],
    /// `(&stats->staop1)[i]`.
    pub staop: [Oid; STATISTIC_NUM_SLOTS],
    /// `(&stats->stacoll1)[i]`.
    pub stacoll: [Oid; STATISTIC_NUM_SLOTS],
}

seam_core::seam!(
    /// `((Form_pg_statistic) GETSTRUCT(statstuple))` projected to its
    /// fixed-width slot metadata (`stakindN` / `staopN` / `stacollN` arrays),
    /// for `get_attstatsslot`'s slot-matching loop. A pure struct read of the
    /// caller-supplied `pg_statistic` tuple (no syscache lookup, no detoast),
    /// so it cannot miss; `Err` is reserved for the (unreachable) marshalling
    /// surface.
    pub fn pg_statistic_slot_meta(
        stats_tuple: types_selfuncs::StatsTuple,
    ) -> PgResult<PgStatisticSlotMeta>
);

seam_core::seam!(
    /// `SysCacheGetAttrNotNull(STATRELATTINH, statstuple, attnum)`
    /// (utils/cache/lsyscache.c, via syscache.c): fetch the (guaranteed
    /// non-null) attribute `attnum` of the supplied `pg_statistic` tuple as a
    /// raw `Datum` — used by `get_attstatsslot` to pull the `stavaluesN` /
    /// `stanumbersN` array Datums. The Datum still points into the cached
    /// tuple, so the caller must detoast/copy before the tuple is released.
    /// `Err` carries the `elog(ERROR, "...returned NULL")` from the NotNull
    /// assertion.
    pub fn syscache_get_attr_not_null_statistic(
        stats_tuple: types_selfuncs::StatsTuple,
        attnum: types_core::AttrNumber,
    ) -> PgResult<types_datum::Datum>
);

seam_core::seam!(
    /// `SearchSysCache3(STATRELATTINH, ObjectIdGetDatum(relid),
    /// Int16GetDatum(attnum), BoolGetDatum(inherit))` (the
    /// `ExecHashBuildSkewHash` probe): look up the `pg_statistic` row for a
    /// column. `Ok(None)` on a cache miss (`!HeapTupleIsValid`). The returned
    /// handle is a pinned syscache tuple; the caller must pair it with
    /// [`release_stats_tuple`] (C `ReleaseSysCache`). `Err` carries the
    /// catcache machinery's `ereport(ERROR)`s.
    pub fn search_statrelattinh<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        attnum: types_core::AttrNumber,
        inherit: bool,
    ) -> PgResult<Option<types_selfuncs::StatsTuple>>
);

seam_core::seam!(
    /// `ReleaseSysCache(tuple)` for a `pg_statistic` tuple obtained from
    /// [`search_statrelattinh`]: drops the syscache pin. Infallible.
    pub fn release_stats_tuple(stats_tuple: types_selfuncs::StatsTuple)
);

// ===========================================================================
// Additional lsyscache.c projected-row reads (PG 18.3). Each is a
// `SearchSysCache*` + `GETSTRUCT` field projection owned by syscache.c; a
// cache miss is `Ok(None)` so the lsyscache caller raises its own
// `cache lookup failed` / returns the C "not found" sentinel. These panic
// loudly until the catcache/syscache owner installs them.
// ===========================================================================

/// The fixed-width `Form_pg_operator` columns lsyscache.c reads off
/// `SearchSysCache1(OPEROID, ...)`. `oprname` rides as an owned `String`
/// (error-message / `get_opname` use only).
#[derive(Clone, Debug)]
pub struct PgOperatorForm {
    /// `oprname` — the operator's name.
    pub oprname: String,
    /// `oprcanmerge` — `pg_operator.oprcanmerge`.
    pub oprcanmerge: bool,
    /// `oprcanhash` — `pg_operator.oprcanhash`.
    pub oprcanhash: bool,
    /// `oprleft` — left input type.
    pub oprleft: Oid,
    /// `oprright` — right input type.
    pub oprright: Oid,
    /// `oprresult` — result type.
    pub oprresult: Oid,
    /// `oprcom` — commutator.
    pub oprcom: Oid,
    /// `oprnegate` — negator.
    pub oprnegate: Oid,
    /// `oprcode` — the implementing function OID.
    pub oprcode: Oid,
    /// `oprrest` — restriction selectivity estimator.
    pub oprrest: Oid,
    /// `oprjoin` — join selectivity estimator.
    pub oprjoin: Oid,
}

seam_core::seam!(
    /// `SearchSysCache1(OPEROID, ObjectIdGetDatum(opno))` + `GETSTRUCT` of the
    /// fixed-width `Form_pg_operator` columns the lsyscache operator helpers
    /// read. `Ok(None)` on a cache miss; the installer owns the
    /// `ReleaseSysCache`.
    pub fn pg_operator_form<'mcx>(
        mcx: Mcx<'mcx>,
        opno: Oid,
    ) -> PgResult<Option<PgOperatorForm>>
);

/// The fixed-width `Form_pg_proc` columns lsyscache.c reads off
/// `SearchSysCache1(PROCOID, ...)` for the scalar `get_func_*` / `func_*`
/// helpers. `proname` rides as an owned `String`.
#[derive(Clone, Debug)]
pub struct PgProcForm {
    /// `proname` — the function's name.
    pub proname: String,
    /// `pronamespace` — the function's schema OID.
    pub pronamespace: Oid,
    /// `provariadic` — the variadic array element type OID.
    pub provariadic: Oid,
    /// `prosupport` — the planner support function OID.
    pub prosupport: Oid,
    /// `prokind` — `f`/`p`/`a`/`w`.
    pub prokind: i8,
    /// `proleakproof` — leakproof flag.
    pub proleakproof: bool,
    /// `proisstrict` — strict flag.
    pub proisstrict: bool,
    /// `proretset` — returns-set flag.
    pub proretset: bool,
    /// `provolatile` — `i`/`s`/`v`.
    pub provolatile: i8,
    /// `proparallel` — `s`/`r`/`u`.
    pub proparallel: i8,
    /// `pronargs` — number of input arguments.
    pub pronargs: i16,
    /// `prorettype` — the result type OID.
    pub prorettype: Oid,
}

seam_core::seam!(
    /// `SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid))` + `GETSTRUCT` of the
    /// fixed-width `Form_pg_proc` columns the scalar `get_func_*` / `func_*`
    /// lsyscache helpers read. `Ok(None)` on a cache miss; the installer owns
    /// the `ReleaseSysCache`.
    pub fn pg_proc_form<'mcx>(
        mcx: Mcx<'mcx>,
        funcid: Oid,
    ) -> PgResult<Option<PgProcForm>>
);

seam_core::seam!(
    /// `SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum))`
    /// + `GETSTRUCT(Form_pg_attribute)` projected to the fixed-width row
    /// (`get_attgenerated` / `get_atttype` / `get_atttypetypmodcoll`).
    /// `Ok(None)` on a cache miss; the installer owns the `ReleaseSysCache`.
    pub fn pg_attribute_form(
        relid: Oid,
        attnum: types_core::AttrNumber,
    ) -> PgResult<Option<types_tuple::heaptuple::FormData_pg_attribute>>
);

seam_core::seam!(
    /// `SearchSysCache2(ATTNUM, ...)` + `SysCacheGetAttr(ATTNAME, tuple,
    /// Anum_pg_attribute_attoptions, &isnull)` + `datumCopy` (`get_attoptions`):
    /// the attribute's `attoptions` `text[]` Datum copied into `mcx`, or
    /// `Ok(None)` for SQL null. An outer `Ok(None)` (the cache miss) lets the
    /// caller raise its own `cache lookup failed for attribute` error. `Err`
    /// carries OOM from the copy.
    pub fn pg_attribute_attoptions<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        attnum: i16,
    ) -> PgResult<Option<Option<types_datum::Datum>>>
);

/// The fixed-width `Form_pg_class` columns lsyscache.c reads off
/// `SearchSysCache1(RELOID, ...)` not already covered by the single-field
/// relation seams.
#[derive(Clone, Copy, Debug, Default)]
pub struct PgClassExtra {
    /// `relnatts` — number of user attributes.
    pub relnatts: i16,
    /// `reltype` — the relation's composite type OID.
    pub reltype: Oid,
    /// `reltablespace` — the relation's tablespace OID.
    pub reltablespace: Oid,
    /// `relpersistence` — `p`/`u`/`t`.
    pub relpersistence: u8,
    /// `relam` — the table/index access method OID.
    pub relam: Oid,
}

seam_core::seam!(
    /// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` + `GETSTRUCT` of the
    /// extra fixed-width `Form_pg_class` columns the remaining lsyscache
    /// relation helpers read. `Ok(None)` on a cache miss; the installer owns the
    /// `ReleaseSysCache`.
    pub fn pg_class_extra(relid: Oid) -> PgResult<Option<PgClassExtra>>
);

/// The complete fixed-width `Form_pg_class` columns
/// (`CLASS_TUPLE_SIZE` worth) — the `memcpy(rd_rel, GETSTRUCT(htup), ...)`
/// payload relcache's `RelationCacheInitializePhase3` copies back into a
/// faked-up nailed entry. Field-for-field with `pg_class.h`'s
/// `FormData_pg_class`.
#[derive(Debug)]
pub struct PgClassFullForm<'mcx> {
    /// `NameData relname` (mcx-allocated copy of the catcache entry's name).
    pub relname: PgString<'mcx>,
    pub relnamespace: Oid,
    pub reltype: Oid,
    pub reloftype: Oid,
    pub relowner: Oid,
    pub relam: Oid,
    pub relfilenode: Oid,
    pub reltablespace: Oid,
    pub relpages: i32,
    pub reltuples: f32,
    pub relallvisible: i32,
    pub reltoastrelid: Oid,
    pub relhasindex: bool,
    pub relisshared: bool,
    pub relpersistence: i8,
    pub relkind: i8,
    pub relnatts: i16,
    pub relchecks: i16,
    pub relhasrules: bool,
    pub relhastriggers: bool,
    pub relhassubclass: bool,
    pub relrowsecurity: bool,
    pub relforcerowsecurity: bool,
    pub relispopulated: bool,
    pub relreplident: i8,
    pub relispartition: bool,
    pub relrewrite: Oid,
    pub relfrozenxid: u32,
    pub relminmxid: u32,
}

seam_core::seam!(
    /// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` + `GETSTRUCT` of the
    /// full `Form_pg_class` tuple (`relcache.c` Phase3 nailed-entry refill:
    /// `memcpy(relation->rd_rel, relp, CLASS_TUPLE_SIZE)`). `Ok(None)` on a
    /// cache miss (`!HeapTupleIsValid`) so the caller raises its own
    /// `cache lookup failed for relation %u` `ereport(FATAL)`; the installer
    /// owns the `ReleaseSysCache`.
    pub fn search_pg_class_full_form<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
    ) -> PgResult<Option<PgClassFullForm<'mcx>>>
);

seam_core::seam!(
    /// `RelationSupportsSysCache(relid)` (syscache.c): whether any system cache
    /// is keyed on the given catalog relation OID
    /// (`RelationHasSysCache`/`RelationSupportsSysCache`). Pure lookup over the
    /// static cache-info table, so infallible.
    pub fn relation_supports_syscache(relid: Oid) -> bool
);

seam_core::seam!(
    /// `InitCatalogCachePhase2()` (syscache.c): complete second-phase init of
    /// all system caches (`InitCatCachePhase2(SysCache[cacheId], true)` for
    /// every `cacheId`), loading each cache's index relcache entry. Done while
    /// building the relcache init file. Can `ereport(ERROR)` (relcache open),
    /// carried on `Err`.
    pub fn init_catalog_cache_phase2() -> PgResult<()>
);

seam_core::seam!(
    /// `SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass))` projected to
    /// `(opcfamily, opcintype, opcmethod)` (`get_opclass_opfamily_and_input_type`
    /// / `get_opclass_method`). `Ok(None)` on a cache miss; the installer owns
    /// the `ReleaseSysCache`.
    pub fn pg_opclass_form(opclass: Oid) -> PgResult<Option<(Oid, Oid, Oid)>>
);

/// The `Form_pg_range` columns `get_range_*` read off `SearchSysCache1(
/// RANGETYPE, ...)`.
#[derive(Clone, Copy, Debug, Default)]
pub struct PgRangeFields {
    /// `rngsubtype` — the range's element (subtype) OID.
    pub rngsubtype: Oid,
    /// `rngcollation` — the range's collation OID.
    pub rngcollation: Oid,
    /// `rngmultitypid` — the corresponding multirange type OID.
    pub rngmultitypid: Oid,
}

seam_core::seam!(
    /// `SearchSysCache1(RANGETYPE, ObjectIdGetDatum(rangeOid))` projected to the
    /// `Form_pg_range` fields `get_range_subtype` / `get_range_collation` /
    /// `get_range_multirange` read. `Ok(None)` on a cache miss; the installer
    /// owns the `ReleaseSysCache`.
    pub fn pg_range_fields(range_oid: Oid) -> PgResult<Option<PgRangeFields>>
);

/// The `Form_pg_index` boolean flags `get_index_*` read off `SearchSysCache1(
/// INDEXRELID, ...)`.
#[derive(Clone, Copy, Debug, Default)]
pub struct PgIndexFlags {
    /// `indisreplident` — the replica-identity flag.
    pub indisreplident: bool,
    /// `indisvalid` — the index-is-valid flag.
    pub indisvalid: bool,
    /// `indisclustered` — the index-is-clustered flag.
    pub indisclustered: bool,
}

seam_core::seam!(
    /// `SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid))` projected to
    /// the boolean `Form_pg_index` flags `get_index_isreplident` /
    /// `get_index_isvalid`. `Ok(None)` on a cache miss; the installer owns the
    /// `ReleaseSysCache`.
    pub fn pg_index_flags(index_oid: Oid) -> PgResult<Option<PgIndexFlags>>
);

seam_core::seam!(
    /// `SearchSysCache1(INDEXRELID, ...)` + `Form_pg_index.indnatts` /
    /// `indnkeyatts` + `SysCacheGetAttrNotNull(INDEXRELID, tuple,
    /// Anum_pg_index_indclass)` projected to the `oidvector` of per-column
    /// opclass OIDs (`get_index_column_opclass`). Returns `(indnatts,
    /// indnkeyatts, indclass)` with `indclass` copied into `mcx`. `Ok(None)` on
    /// a cache miss (the C `return InvalidOid`). `Err` carries OOM and the
    /// `SysCacheGetAttrNotNull` `elog`.
    pub fn pg_index_indclass<'mcx>(
        mcx: Mcx<'mcx>,
        index_oid: Oid,
    ) -> PgResult<Option<(i16, i16, PgVec<'mcx, Oid>)>>
);

seam_core::seam!(
    /// `GetSysCacheOid1(PUBLICATIONNAME, Anum_pg_publication_oid,
    /// CStringGetDatum(pubname))` (`get_publication_oid`). `InvalidOid` (0) when
    /// not found; the caller turns that into the "publication does not exist"
    /// `ereport(ERROR)` when `!missing_ok`. `Err` carries the catcache surface.
    pub fn get_publication_oid_syscache(pubname: &str) -> PgResult<Oid>
);

seam_core::seam!(
    /// `SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(pubid))` +
    /// `pstrdup(NameStr(pubname))` (`get_publication_name`): the publication's
    /// name copied into `mcx`, or `Ok(None)` on a cache miss. `Err` includes
    /// OOM from the copy.
    pub fn get_publication_name_syscache<'mcx>(
        mcx: Mcx<'mcx>,
        pubid: Oid,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `GetSysCacheOid2(SUBSCRIPTIONNAME, Anum_pg_subscription_oid,
    /// MyDatabaseId, CStringGetDatum(subname))` (`get_subscription_oid`). The
    /// `MyDatabaseId` key is supplied by the syscache installer (it owns the
    /// per-backend global). `InvalidOid` (0) when not found. `Err` carries the
    /// catcache surface.
    pub fn get_subscription_oid_syscache(subname: &str) -> PgResult<Oid>
);

seam_core::seam!(
    /// `SearchSysCache1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid))` +
    /// `pstrdup(NameStr(subname))` (`get_subscription_name`): the subscription's
    /// name copied into `mcx`, or `Ok(None)` on a cache miss. `Err` includes
    /// OOM from the copy.
    pub fn get_subscription_name_syscache<'mcx>(
        mcx: Mcx<'mcx>,
        subid: Oid,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCacheExists3(AMOPOPID, ObjectIdGetDatum(opno),
    /// CharGetDatum(AMOP_SEARCH), ObjectIdGetDatum(opfamily))`
    /// (`op_in_opfamily`). `Err` carries the catcache surface.
    pub fn amop_search_exists(opno: Oid, opfamily: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `SearchSysCache3(AMOPOPID, ObjectIdGetDatum(opno), CharGetDatum(purpose),
    /// ObjectIdGetDatum(opfamily))` projected to `(amopstrategy, amopsortfamily)`
    /// (`get_op_opfamily_strategy` with `purpose = AMOP_SEARCH`,
    /// `get_op_opfamily_sortfamily` with `purpose = AMOP_ORDER`). `purpose` is
    /// the raw `pg_amop.amoppurpose` char. `Ok(None)` on a cache miss. `Err`
    /// carries the catcache surface.
    pub fn amop_by_opr_purpose(
        opno: Oid,
        purpose: u8,
        opfamily: Oid,
    ) -> PgResult<Option<(i16, Oid)>>
);

seam_core::seam!(
    /// `SearchSysCache3(STATRELATTINH, ObjectIdGetDatum(relid),
    /// Int16GetDatum(attnum), BoolGetDatum(false))` +
    /// `((Form_pg_statistic) GETSTRUCT(tp))->stawidth` (`get_attavgwidth`): the
    /// non-inherited average stored width of the column. `Ok(None)` on a cache
    /// miss (`!HeapTupleIsValid`); the installer owns the `ReleaseSysCache`.
    pub fn pg_statistic_stawidth(
        relid: Oid,
        attnum: types_core::AttrNumber,
    ) -> PgResult<Option<i32>>
);

/// The `pg_type` row fields `get_typdefault` reads off `SearchSysCache1(
/// TYPEOID, ...)`: the two potentially-null default columns
/// (`SysCacheGetAttr` + `text_to_cstring`, folded into owned `String`s) plus
/// the `Form_pg_type` fields `makeConst` needs.
#[derive(Clone, Debug, Default)]
pub struct PgTypeDefault {
    /// `SysCacheGetAttr(Anum_pg_type_typdefaultbin)` rendered via
    /// `text_to_cstring`, or `None` for SQL null. When present it is a
    /// `pg_node_tree` string the caller feeds to `stringToNode`.
    pub typdefaultbin: Option<String>,
    /// `SysCacheGetAttr(Anum_pg_type_typdefault)` via `text_to_cstring`, or
    /// `None` for SQL null. The plain literal default text.
    pub typdefault: Option<String>,
    /// `type->typinput` — the type's input function OID.
    pub typinput: Oid,
    /// `getTypeIOParam(typeTuple)` — the I/O parameter OID `makeConst`'s value
    /// conversion uses.
    pub typioparam: Oid,
    /// `type->typcollation`.
    pub typcollation: Oid,
    /// `type->typlen`.
    pub typlen: i16,
    /// `type->typbyval`.
    pub typbyval: bool,
}

seam_core::seam!(
    /// `SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid))` +
    /// `SysCacheGetAttr(Anum_pg_type_typdefaultbin / typdefault)` +
    /// `text_to_cstring`, projected for `get_typdefault`. `Ok(None)` on a cache
    /// miss (the caller raises `cache lookup failed for type %u`). The default
    /// text columns ride as owned `String`s in `mcx`; `Err` carries OOM and the
    /// catcache surface.
    pub fn pg_type_default<'mcx>(
        mcx: Mcx<'mcx>,
        typid: Oid,
    ) -> PgResult<Option<PgTypeDefault>>
);
