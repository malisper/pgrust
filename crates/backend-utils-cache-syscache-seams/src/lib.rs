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
use types_namespace::{CatalogObjectName, FastpathProcRow, FuncProcAttrs, OperRow, ProcRow};
use types_cache::AuthIdRow;
use types_cache::syscache::{
    ClassOwnerAcl, ForeignDataWrapperFormRow, ForeignServerFormRow, NamespaceOwnerAcl,
    ObjectOwnerAcl, RolePasswordLookup, TypeOwnerAcl,
};
use types_acl::AclItem;
use types_catalog::pg_aggregate::{AggFormData, AggRow};
use types_nodes::nodes::NodePtr;
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
    /// `SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum))`
    /// projected to the attribute's `attisdropped` flag (`Form_pg_attribute`).
    /// `Ok(None)` on a cache miss (`!HeapTupleIsValid`); the installer owns the
    /// `ReleaseSysCache`. Consumed by `get_rte_attribute_is_dropped`
    /// (parser/parse_relation.c).
    pub fn search_attnum_attisdropped(
        relid: Oid,
        attnum: types_core::AttrNumber,
    ) -> PgResult<Option<bool>>
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
    /// `SearchSysCache1(AUTHNAME, PointerGetDatum(role))` for
    /// `get_role_password` (`libpq/crypt.c`): projects `rolpassword`
    /// (`TextDatumGetCString`) and `rolvaliduntil` (`DatumGetTimestampTz`),
    /// distinguishing no-such-role / no-password / found via
    /// [`RolePasswordLookup`]. The `ReleaseSysCache` is subsumed by returning
    /// the data by value.
    pub fn fetch_role_password(role: &str) -> PgResult<RolePasswordLookup>
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
    /// `SearchSysCache1(ENUMOID, ObjectIdGetDatum(enumval))` projected to an
    /// [`EnumTupleData`] (the `Form_pg_enum` columns `enum.c` reads plus the
    /// header `xmin`/`xmin_committed` `check_safe_enum_use` needs). `Ok(None)`
    /// on a cache miss (`!HeapTupleIsValid`); the installer owns the
    /// `ReleaseSysCache`. Consumed by `enum_out`/`enum_send`/`enum_cmp_internal`
    /// (utils/adt/enum.c).
    pub fn lookup_enum_by_oid(
        enumval: Oid,
    ) -> PgResult<Option<types_catalog::pg_enum::EnumTupleData>>
);

seam_core::seam!(
    /// `SearchSysCache2(ENUMTYPOIDNAME, ObjectIdGetDatum(enumtypoid),
    /// CStringGetDatum(name))` projected to an [`EnumTupleData`]. `name` is the
    /// label text (the caller has already truncated at the first NUL, as
    /// C `CStringGetDatum` does). `Ok(None)` on a cache miss; the installer
    /// owns the `ReleaseSysCache`. Consumed by `enum_in`/`enum_recv`
    /// (utils/adt/enum.c).
    pub fn lookup_enum_by_typoid_name(
        enumtypoid: Oid,
        name: &str,
    ) -> PgResult<Option<types_catalog::pg_enum::EnumTupleData>>
);

seam_core::seam!(
    /// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` projected to the
    /// `Form_pg_class.relam` field (the relation's access method). `Ok(None)`
    /// on a cache miss (`!HeapTupleIsValid`); the installer owns the
    /// `ReleaseSysCache`.
    pub fn search_relation_relam(relid: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` projected to the
    /// `Form_pg_class.reloftype` field (the OF-type of a typed table, else
    /// `InvalidOid`). `Ok(None)` on a cache miss; the installer owns the
    /// `ReleaseSysCache`.
    pub fn search_relation_reloftype(relid: Oid) -> PgResult<Option<Oid>>
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
    /// `SearchSysCacheList2(AMPROCNUM, ObjectIdGetDatum(opfamilyoid),
    /// ObjectIdGetDatum(lefttype))` member rows, projected and copied into
    /// `mcx` (the partial-key list keyed by opfamily + amproclefttype).
    pub fn search_amproc_list2<'mcx>(
        mcx: Mcx<'mcx>,
        opfamilyoid: Oid,
        lefttype: Oid,
    ) -> PgResult<PgVec<'mcx, AmprocRow>>
);

seam_core::seam!(
    /// `SearchSysCacheList1(CLAAMNAMENSP, ObjectIdGetDatum(amoid))` member rows
    /// projected to `(oid, opcfamily, opcintype)` (`Form_pg_opclass`), copied
    /// into `mcx`. amvalidate.c's `opclass_for_family_datatype` scans this list
    /// for the opclass of an opfamily accepting a given input type.
    pub fn search_opclass_list_by_am<'mcx>(
        mcx: Mcx<'mcx>,
        amoid: Oid,
    ) -> PgResult<PgVec<'mcx, (Oid, Oid, Oid)>>
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
    /// `SearchSysCache1(NAMESPACENAME, CStringGetDatum(name))` +
    /// `GETSTRUCT(Form_pg_namespace)` projected to `(oid, nspowner, nspname)`,
    /// the fields `schemacmds.c`'s `AlterSchemaOwner` reads. `Ok(None)` on a
    /// cache miss (`!HeapTupleIsValid`), so the caller raises the schema's own
    /// "does not exist" error. The installer owns the `ReleaseSysCache`; `Err`
    /// carries OOM from the name copy.
    pub fn namespace_owner_row_by_name<'mcx>(
        mcx: Mcx<'mcx>,
        name: &str,
    ) -> PgResult<Option<(Oid, Oid, mcx::PgString<'mcx>)>>
);

seam_core::seam!(
    /// `SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(schemaoid))` +
    /// `GETSTRUCT(Form_pg_namespace)` projected to `(oid, nspowner, nspname)`,
    /// the fields `schemacmds.c`'s `AlterSchemaOwner_oid` reads. `Ok(None)` on a
    /// cache miss, so the caller raises the `elog(ERROR, "cache lookup failed
    /// for schema %u")`. The installer owns the `ReleaseSysCache`.
    pub fn namespace_owner_row_by_oid<'mcx>(
        mcx: Mcx<'mcx>,
        schemaoid: Oid,
    ) -> PgResult<Option<(Oid, Oid, mcx::PgString<'mcx>)>>
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
    /// `func_get_detail`'s default-argument extraction (`parse_func.c`):
    ///
    /// ```c
    /// proargdefaults = SysCacheGetAttrNotNull(PROCOID, ftup,
    ///                                         Anum_pg_proc_proargdefaults);
    /// str = TextDatumGetCString(proargdefaults);
    /// defaults = castNode(List, stringToNode(str));
    /// ```
    ///
    /// Projects the `pg_proc.proargdefaults` `pg_node_tree` column of `funcid`
    /// to its deserialized default-expression list (the elements of the
    /// `castNode(List, ...)`), each node allocated in `mcx`. The caller only
    /// reaches this path when `best_candidate->ndargs > 0`, where the column is
    /// guaranteed non-null (`SysCacheGetAttrNotNull`); a SQL-null column or a
    /// cache miss is therefore an `Err` (catcache `ereport(ERROR)`), as in C.
    pub fn proc_argdefaults<'mcx>(
        mcx: Mcx<'mcx>,
        funcid: Oid,
    ) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(OPEROID, oprid)` projected to the [`OperRow`]
    /// fields; `Ok(None)` on cache miss (`OperatorIsVisibleExt`).
    pub fn oper_row_by_oid<'mcx>(mcx: Mcx<'mcx>, oprid: Oid) -> PgResult<Option<OperRow<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(AGGFNOID, ObjectIdGetDatum(funcid))` projected to the
    /// [`AggRow`] fields (`Form_pg_aggregate` `aggkind` / `aggnumdirectargs`);
    /// read by `func_get_detail` (`parse_func.c`) for an aggregate function.
    /// `Ok(None)` on a cache miss (`!HeapTupleIsValid`); the caller raises its
    /// own `cache lookup failed for aggregate %u` `elog(ERROR)`, as in C.
    pub fn agg_row_by_oid<'mcx>(mcx: Mcx<'mcx>, funcid: Oid) -> PgResult<Option<AggRow>>
);

seam_core::seam!(
    /// `aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfnoid));
    /// aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple)` plus the two
    /// `CATALOG_VARLEN` `agginitval` / `aggminitval` `SysCacheGetAttr` text
    /// columns (nodeAgg.c `ExecInitAgg`'s `fetch_agg_form`). Projects the full
    /// [`AggFormData`] (every aggregate support-function Oid + the transition
    /// type/space columns + the `aggfinalextra`/`aggmfinalextra` flags +
    /// `aggfinalmodify`/`aggmfinalmodify` + `aggkind` + the nullable initial-value
    /// texts) so the executor can read it all from the one pinned tuple as the C
    /// does. `Ok(None)` on a cache miss (`!HeapTupleIsValid`); the caller raises
    /// its own `cache lookup failed for aggregate %u` `elog(ERROR)`, as in C.
    pub fn agg_form_by_oid<'mcx>(
        mcx: Mcx<'mcx>,
        aggfnoid: Oid,
    ) -> PgResult<Option<AggFormData>>
);

seam_core::seam!(
    /// `SearchSysCache1(AGGFNOID, ObjectIdGetDatum(funcid))` returning the held
    /// `pg_aggregate` tuple (`AggregateCreate`'s REPLACE path,
    /// pg_aggregate.c:690). Returns the owned [`FormedTuple`] copy (supplying the
    /// not-replaced columns + the `t_self` update target for
    /// `heap_modify_tuple`/`CatalogTupleUpdate`) together with the projected
    /// [`AggRow`] (`oldagg->aggkind` / `oldagg->aggnumdirectargs`, which
    /// `AggregateCreate` validates before the update). `Ok(None)` on a cache miss
    /// (`!HeapTupleIsValid` ⇒ C falls through to the fresh-insert branch).
    pub fn aggregate_tuple_by_fnoid<'mcx>(
        mcx: Mcx<'mcx>,
        funcid: Oid,
    ) -> PgResult<
        Option<(
            types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
            AggRow,
        )>,
    >
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
    /// `data` is the tuple's user-data area (`(char *) t_data + t_hoff`),
    /// threaded alongside the header-only [`HeapTupleData`] so `GETSTRUCT` can
    /// read the fixed-width `Form_pg_class` columns.
    pub fn pg_class_shape(
        tuple: &types_tuple::HeapTupleData<'_>,
        data: &[u8],
    ) -> types_storage::PgClassShape
);

seam_core::seam!(
    /// `((Form_pg_attribute) GETSTRUCT(tuple))->attrelid` — the table a
    /// `pg_attribute` tuple belongs to. `data` is the tuple's user-data area.
    pub fn pg_attribute_attrelid(
        tuple: &types_tuple::HeapTupleData<'_>,
        data: &[u8],
    ) -> Oid
);

seam_core::seam!(
    /// `((Form_pg_index) GETSTRUCT(tuple))->indexrelid` — the index OID of a
    /// `pg_index` tuple. `data` is the tuple's user-data area.
    pub fn pg_index_indexrelid(
        tuple: &types_tuple::HeapTupleData<'_>,
        data: &[u8],
    ) -> Oid
);

seam_core::seam!(
    /// The FK target table of a `pg_constraint` tuple: for a foreign-key
    /// constraint (`contype == CONSTRAINT_FOREIGN`), `Form_pg_constraint.confrelid`;
    /// `None` for any other constraint type (inval.c skips those). `data` is the
    /// tuple's user-data area.
    pub fn pg_constraint_fk_target(
        tuple: &types_tuple::HeapTupleData<'_>,
        data: &[u8],
    ) -> Option<Oid>
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

seam_core::seam!(
    /// `GetSysCacheOid1(LANGNAME, Anum_pg_language_oid, CStringGetDatum(langname))`
    /// (`get_language_oid`, proclang.c): the language's OID by name, or
    /// `InvalidOid` on a cache miss. The missing-language error is the caller's
    /// (`get_language_oid` raises `ERRCODE_UNDEFINED_OBJECT` when `!missing_ok`).
    pub fn language_oid_by_name(langname: &str) -> PgResult<Oid>
);

seam_core::seam!(
    /// `SearchSysCache1(LANGNAME, PointerGetDatum(languageName))` (proclang.c
    /// pre-existing-definition check): the writable `pg_language` tuple by name
    /// plus its deformed `oid`/`lanowner`, or `None` if no such language exists.
    /// `CreateProceduralLanguage`'s replace branch needs the whole tuple
    /// (`heap_modify_tuple` keeps `oid`/`lanowner`/`lanacl` from it) and the
    /// `oldform->oid`.
    pub fn language_tuple_by_name<'mcx>(
        mcx: Mcx<'mcx>,
        langname: &str,
    ) -> PgResult<
        Option<(
            types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
            types_catalog::pg_language::FormData_pg_language,
        )>,
    >
);

/* ---- CLUSTER pg_class / pg_index writable copies (backend-commands-cluster) */

seam_core::seam!(
    /// `SearchSysCacheExists1(RELOID, indexOid)` (syscache.c).
    pub fn search_syscache_exists_reloid(reloid: Oid) -> PgResult<bool>
);
seam_core::seam!(
    /// `SearchSysCache1(SEQRELID, ObjectIdGetDatum(seqid))` + `GETSTRUCT`
    /// projected to the `pg_sequence` row (sequence.c
    /// `SearchSysCache1`/`SearchSysCacheCopy1`). `Ok(None)` on a cache miss
    /// (`!HeapTupleIsValid`); the caller raises `cache lookup failed for
    /// sequence %u`. The projected row is a by-value copy out of the catcache,
    /// so a caller that mutates it (`AlterSequence`) treats it as its own.
    pub fn search_seqrelid(
        seqid: Oid,
    ) -> PgResult<Option<types_catalog::pg_sequence::FormData_pg_sequence>>
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
    /// `SearchSysCache2(RULERELNAME, ObjectIdGetDatum(relid),
    /// PointerGetDatum(rulename))` + `GETSTRUCT(Form_pg_rewrite)` projected to
    /// `(oid, ev_class)` (rewriteSupport.c `get_rewrite_oid`). `Ok(None)` on a
    /// cache miss (`!HeapTupleIsValid`); the caller decides between
    /// `InvalidOid` (missing_ok) and `ERRCODE_UNDEFINED_OBJECT`. `ev_class` is
    /// returned so the caller can keep the C `Assert(relid == ev_class)`.
    pub fn search_rewrite_oid(
        relid: Oid,
        rulename: &str,
    ) -> PgResult<Option<(Oid, Oid)>>
);

seam_core::seam!(
    /// `GetSysCacheOid1(AMNAME, Anum_pg_am_oid, CStringGetDatum(amname))`
    /// (syscache.c): the OID of the `pg_am` row named `amname`, or
    /// `InvalidOid` when no such access method exists. amcmds.c's
    /// `CreateAccessMethod` uses this for its "already exists" duplicate-name
    /// check.
    pub fn get_am_oid_by_name(amname: &str) -> PgResult<Oid>
);

seam_core::seam!(
    /// `SearchSysCache1(AMNAME, CStringGetDatum(amname))` +
    /// `GETSTRUCT(Form_pg_am)` (syscache.c) projected to the
    /// `(oid, amtype, amname)` fields amcmds.c's `get_am_type_oid` reads.
    /// `Ok(None)` on a cache miss (`!HeapTupleIsValid`); the caller raises its
    /// own "access method does not exist" error. The name copy lands in the
    /// caller's `Mcx`; the installer owns the `ReleaseSysCache`.
    pub fn search_am_by_name<'mcx>(
        mcx: Mcx<'mcx>,
        amname: &str,
    ) -> PgResult<Option<types_namespace::backend_catalog_namespace::PgAmInfo<'mcx>>>
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

seam_core::seam!(
    /// `SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid))` +
    /// `GETSTRUCT(Form_pg_proc)->(procost, prorows, proretset, prosupport)`
    /// (the `Form_pg_proc` fields `add_function_cost` / `get_function_rows`
    /// read in plancat.c). `Err` carries the C `elog(ERROR, "cache lookup
    /// failed for function %u")`. The installer owns the `ReleaseSysCache`.
    pub fn proc_cost_rows(funcid: Oid) -> PgResult<ProcCostRows>
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
    /// `SearchSysCache1(PARTRELID, ObjectIdGetDatum(parentId))` projected to
    /// `Form_pg_partitioned_table.partdefid` (catalog/partition.c
    /// `get_default_partition_oid`): the OID of the default partition of the
    /// given partitioned table, or `Ok(None)` when no `pg_partitioned_table`
    /// row exists for `parentId` (`!HeapTupleIsValid`), in which case the
    /// caller leaves the result `InvalidOid`. The installer owns the
    /// `ReleaseSysCache`.
    pub fn search_partrelid_partdefid(parentId: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `update_default_partition_oid(parentId, defaultPartId)`
    /// (catalog/partition.c): set `pg_partitioned_table.partdefid` of the
    /// partitioned table `parentId` to `defaultPartId`. The whole
    /// `table_open(PartitionedRelationId, RowExclusiveLock)` /
    /// `SearchSysCacheCopy1(PARTRELID, ...)` / in-place `partdefid` write /
    /// `CatalogTupleUpdate` / `heap_freetuple` / `table_close` sequence rides
    /// on the `pg_partitioned_table` syscache tuple-copy + the heap-form value
    /// layer (unported here); the installer raises `elog(ERROR, "cache lookup
    /// failed for partition key of relation %u")` on a cache miss. Loud-panics
    /// until the `pg_partitioned_table` write owner lands.
    pub fn update_default_partition_oid(parentId: Oid, defaultPartId: Oid) -> PgResult<()>
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
    /// `SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid))` projected to
    /// `Form_pg_index.indrelid` (`catalog/index.c` `IndexGetRelation`). `Ok(None)`
    /// on a cache miss (`!HeapTupleIsValid`) so the caller decides between
    /// `InvalidOid` (`missing_ok`) and the `elog(ERROR, "cache lookup failed for
    /// index %u")`. The scalar `indrelid` is returned by value (no allocation),
    /// mirroring the C that returns a bare `Oid`; the installer owns the
    /// `ReleaseSysCache`. The C `Assert(index->indexrelid == indexId)` lives in
    /// the installer.
    pub fn index_get_relid(index_oid: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid))` projected to
    /// `Form_pg_index.indisprimary` (`catalog/index.c` `relationHasPrimaryKey`).
    /// `Ok(None)` on a cache miss (`!HeapTupleIsValid`) so the caller raises the
    /// exact `elog(ERROR, "cache lookup failed for index %u")`; the installer
    /// owns the `ReleaseSysCache`.
    pub fn index_get_indisprimary(index_oid: Oid) -> PgResult<Option<bool>>
);

seam_core::seam!(
    /// `SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum))`
    /// projected to `Form_pg_attribute.attnotnull` (`catalog/index.c`
    /// `index_check_primary_key`). Returns `Ok(None)` on a cache miss
    /// (`!HeapTupleIsValid`) so the caller raises its own `elog(ERROR, "cache
    /// lookup failed for attribute %d of relation %u")`; the installer owns the
    /// `ReleaseSysCache`. The C also reads `NameStr(attname)` for the error
    /// message, returned alongside the flag.
    pub fn att_get_attnotnull<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relid: Oid,
        attnum: i16,
    ) -> PgResult<Option<(bool, mcx::PgString<'mcx>)>>
);

seam_core::seam!(
    /// `SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass))` +
    /// `GETSTRUCT(Form_pg_opclass)` projected to the fields
    /// `catalog/index.c` `ConstructTupleDescriptor` reads: `(opckeytype,
    /// opcintype, opcname)`. `Ok(None)` on a cache miss (`!HeapTupleIsValid`)
    /// so the caller raises `elog(ERROR, "cache lookup failed for opclass %u")`
    /// or the nondeterministic-collation `errmsg`; the installer owns the
    /// `ReleaseSysCache`.
    pub fn pg_opclass_keytype<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        opclass: Oid,
    ) -> PgResult<Option<(Oid, Oid, mcx::PgString<'mcx>)>>
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

seam_core::seam!(
    /// `GetSysCacheHashValue1(cache_id, ObjectIdGetDatum(oid))`
    /// (`utils/cache/syscache.c`) — the catcache hash value for an arbitrary
    /// single-OID syscache key. Used by `record_plan_function_dependency`'s
    /// VALUE form (setrefs's `extract_query_dependencies`) to build a
    /// `PlanInvalItem.hashValue` directly (with `cache_id = PROCOID`), since
    /// plancache consumes the computed `(cacheId, hashValue)` pair. `Err`
    /// carries the catcache machinery's `ereport(ERROR)`s.
    pub fn get_syscache_hash_value_oid(cache_id: i32, oid: Oid) -> PgResult<u32>
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
    ///
    /// The result is the canonical unified `Datum` (Datum-unification). It is
    /// the raw machine word `SysCacheGetAttrNotNull` returns, still pointing
    /// into the cached `pg_statistic` tuple; its lifetime is unconstrained at
    /// this seam boundary (the caller detoasts/copies before the tuple is
    /// released), so it is pinned to `'static` per the bare-word datum.c
    /// contract.
    pub fn syscache_get_attr_not_null_statistic(
        stats_tuple: types_selfuncs::StatsTuple,
        attnum: types_core::AttrNumber,
    ) -> PgResult<types_tuple::Datum<'static>>
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

/// The `Form_pg_proc` cost/rows columns `add_function_cost` /
/// `get_function_rows` (plancat.c) read off `SearchSysCache1(PROCOID, ...)`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcCostRows {
    /// `procost` — per-call execution cost estimate (cpu_operator_cost units).
    pub procost: f32,
    /// `prorows` — estimated result rows for a set-returning function.
    pub prorows: f32,
    /// `proretset` — does the function return a set?
    pub proretset: bool,
    /// `prosupport` — planner support function OID (`InvalidOid` if none).
    pub prosupport: Oid,
}

/// The fixed-width `Form_pg_operator` columns lsyscache.c reads off
/// `SearchSysCache1(OPEROID, ...)`. `oprname` rides as an owned `String`
/// (error-message / `get_opname` use only).
#[derive(Clone, Debug)]
pub struct PgOperatorForm {
    /// `oprname` — the operator's name.
    pub oprname: String,
    /// `oprkind` — `b` (binary/infix), `l` (prefix); the raw C `char`.
    pub oprkind: i8,
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
    ///
    /// The inner value is the canonical unified `Datum` (Datum-unification):
    /// the `text[]` varlena image copied into `mcx` (C: `datumCopy`), so it
    /// carries the caller's `'mcx` lifetime (the `ByRef` bytes live in `mcx`).
    pub fn pg_attribute_attoptions<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        attnum: i16,
    ) -> PgResult<Option<Option<types_tuple::Datum<'mcx>>>>
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
    /// `!heap_attisnull(rd_indextuple, Anum_pg_index_indpred, NULL)` —
    /// `SearchSysCache1(INDEXRELID, index_oid)` then whether the `indpred`
    /// attribute is non-null, i.e. the index has a partial-index predicate
    /// (`RelationGetIndexPredicate(index) != NIL` without materializing the node
    /// tree). `Ok(None)` on a cache miss. `Err` carries the catcache error
    /// surface.
    pub fn pg_index_has_predicate(index_oid: Oid) -> PgResult<Option<bool>>
);

seam_core::seam!(
    /// `SearchSysCache1(INDEXRELID, index_oid)` then
    /// `heap_getattr(rd_indextuple, Anum_pg_index_indexprs, GetPgIndexDescriptor,
    /// &isnull)` + `TextDatumGetCString` — the raw `pg_index.indexprs`
    /// `pg_node_tree` text the relcache index-expression transform
    /// (`RelationGetIndexExpressions` / `RelationGetIndexAttrBitmap`) feeds to
    /// `stringToNode`. `Ok(None)` when the index has no expression columns
    /// (`heap_attisnull`) or on a cache miss; `Ok(Some(text))` otherwise. `Err`
    /// carries the catcache error surface.
    pub fn pg_index_exprs_text(index_oid: Oid) -> PgResult<Option<String>>
);

seam_core::seam!(
    /// `SearchSysCache1(INDEXRELID, index_oid)` then
    /// `heap_getattr(rd_indextuple, Anum_pg_index_indpred, GetPgIndexDescriptor,
    /// &isnull)` + `TextDatumGetCString` — the raw `pg_index.indpred`
    /// `pg_node_tree` text the relcache index-predicate transform
    /// (`RelationGetIndexPredicate` / `RelationGetIndexAttrBitmap`) feeds to
    /// `stringToNode`. `Ok(None)` when the index is not partial
    /// (`heap_attisnull`) or on a cache miss; `Ok(Some(text))` otherwise. `Err`
    /// carries the catcache error surface.
    pub fn pg_index_pred_text(index_oid: Oid) -> PgResult<Option<String>>
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

seam_core::seam!(
    /// `((Form_pg_statistic) GETSTRUCT(statsTuple))->stanullfrac` (pg_statistic.h):
    /// the fraction of NULLs in the column, read off a `pg_statistic` tuple the
    /// selectivity code holds pinned as a [`StatsTuple`]
    /// ([`search_statrelattinh`]). A pure fixed-area struct read (no syscache
    /// lookup, no detoast) of the caller-supplied tuple, so it cannot miss; the
    /// tuple stays pinned (the caller releases it via [`release_stats_tuple`]).
    pub fn pg_statistic_stanullfrac(
        stats_tuple: types_selfuncs::StatsTuple,
    ) -> f32
);

seam_core::seam!(
    /// `((Form_pg_statistic) GETSTRUCT(statsTuple))->stadistinct` (pg_statistic.h):
    /// the number-of-distinct-values estimate for the column (positive = an
    /// absolute count, negative = a fraction of the row count), read off a
    /// `pg_statistic` tuple the selectivity code holds pinned as a
    /// [`StatsTuple`] ([`search_statrelattinh`]). A pure fixed-area struct read,
    /// so it cannot miss; the caller releases the tuple via
    /// [`release_stats_tuple`].
    pub fn pg_statistic_stadistinct(
        stats_tuple: types_selfuncs::StatsTuple,
    ) -> f32
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

/* ---------------------------------------------------------------------------
 * pg_foreign_* catalog reads (foreign/foreign.c accessors). A cache miss is
 * `Ok(None)`; the caller (`foreign.c`) raises its own `cache lookup failed`
 * / `does not exist` error, exactly as the C `!HeapTupleIsValid` branches do.
 * ------------------------------------------------------------------------- */

seam_core::seam!(
    /// `SearchSysCache1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(fdwid))`
    /// projected to `Form_pg_foreign_data_wrapper`'s
    /// `(fdwname, fdwowner, fdwhandler, fdwvalidator)`. `Ok(None)` on a cache
    /// miss. The name is copied into `mcx`; `Err` carries OOM/catcache errors.
    pub fn foreign_data_wrapper_form<'mcx>(
        mcx: Mcx<'mcx>,
        fdwid: Oid,
    ) -> PgResult<Option<ForeignDataWrapperFormRow<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(serverid))`
    /// projected to `Form_pg_foreign_server`'s `(srvname, srvowner, srvfdw)`.
    /// `Ok(None)` on a cache miss.
    pub fn foreign_server_form<'mcx>(
        mcx: Mcx<'mcx>,
        serverid: Oid,
    ) -> PgResult<Option<ForeignServerFormRow<'mcx>>>
);

seam_core::seam!(
    /// `GetSysCacheOid1(FOREIGNDATAWRAPPERNAME,
    /// Anum_pg_foreign_data_wrapper_oid, CStringGetDatum(fdwname))`: the FDW's
    /// OID, or `InvalidOid` when no row matches.
    pub fn foreign_data_wrapper_oid_by_name(fdwname: &str) -> PgResult<Oid>
);

seam_core::seam!(
    /// `GetSysCacheOid1(FOREIGNSERVERNAME, Anum_pg_foreign_server_oid,
    /// CStringGetDatum(servername))`: the server's OID, or `InvalidOid` when no
    /// row matches.
    pub fn foreign_server_oid_by_name(servername: &str) -> PgResult<Oid>
);

seam_core::seam!(
    /// `SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid))` projected to
    /// `Form_pg_foreign_table`'s `ftserver`. `Ok(None)` on a cache miss (the
    /// caller raises `cache lookup failed for foreign table %u`).
    pub fn foreign_table_server_by_relid(relid: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid))` projected to
    /// `Form_pg_foreign_table`'s `ftserver` plus the raw `ftoptions` `text[]`
    /// (`SysCacheGetAttr(Anum_pg_foreign_table_ftoptions)`): `(ftserver,
    /// Some(bytes))` with the detoasted option array, or `(ftserver, None)`
    /// when `ftoptions` is SQL NULL. `Ok(None)` on a cache miss (the caller
    /// raises `cache lookup failed for foreign table %u`). The caller runs
    /// `untransformRelOptions` on the bytes.
    pub fn foreign_table_form<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
    ) -> PgResult<Option<(Oid, Option<PgVec<'mcx, u8>>)>>
);

seam_core::seam!(
    /// `SearchSysCache1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(fdwid))` then
    /// `SysCacheGetAttr(Anum_pg_foreign_data_wrapper_fdwoptions)`: the raw
    /// `fdwoptions` `text[]` (`Some(bytes)`), or `None` when SQL NULL.
    /// `Ok(None)` on a cache miss. The caller (`AlterForeignDataWrapper`) runs
    /// `untransformRelOptions` on the bytes.
    pub fn foreign_data_wrapper_options<'mcx>(
        mcx: Mcx<'mcx>,
        fdwid: Oid,
    ) -> PgResult<Option<Option<PgVec<'mcx, u8>>>>
);

seam_core::seam!(
    /// `SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(serverid))` then
    /// `SysCacheGetAttr(Anum_pg_foreign_server_srvoptions)`: the raw
    /// `srvoptions` `text[]` (`Some(bytes)`), or `None` when SQL NULL.
    /// `Ok(None)` on a cache miss. The caller (`AlterForeignServer`) runs
    /// `untransformRelOptions` on the bytes.
    pub fn foreign_server_options<'mcx>(
        mcx: Mcx<'mcx>,
        serverid: Oid,
    ) -> PgResult<Option<Option<PgVec<'mcx, u8>>>>
);

seam_core::seam!(
    /// `SearchSysCache1(USERMAPPINGOID, ObjectIdGetDatum(umid))` then
    /// `SysCacheGetAttr(Anum_pg_user_mapping_umoptions)`: the raw `umoptions`
    /// `text[]` (`Some(bytes)`), or `None` when SQL NULL. `Ok(None)` on a cache
    /// miss. The caller (`AlterUserMapping`) runs `untransformRelOptions` on the
    /// bytes.
    pub fn user_mapping_options_by_oid<'mcx>(
        mcx: Mcx<'mcx>,
        umid: Oid,
    ) -> PgResult<Option<Option<PgVec<'mcx, u8>>>>
);

seam_core::seam!(
    /// `SearchSysCache2(USERMAPPINGUSERSERVER, ObjectIdGetDatum(userid),
    /// ObjectIdGetDatum(serverid))` projected to the mapping OID
    /// (`Form_pg_user_mapping.oid`) plus the raw `umoptions` `text[]`
    /// (`SysCacheGetAttr(Anum_pg_user_mapping_umoptions)`): `(umid,
    /// Some(bytes))` or `(umid, None)` when `umoptions` is SQL NULL. `Ok(None)`
    /// on a cache miss — the caller (`GetUserMapping`) retries with
    /// `userid = InvalidOid` (PUBLIC) and, if still absent, raises
    /// `user mapping not found ...`. The caller runs `untransformRelOptions`.
    pub fn user_mapping_form<'mcx>(
        mcx: Mcx<'mcx>,
        userid: Oid,
        serverid: Oid,
    ) -> PgResult<Option<(Oid, Option<PgVec<'mcx, u8>>)>>
);

seam_core::seam!(
    /// `SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum))`
    /// then `SysCacheGetAttr(Anum_pg_attribute_attfdwoptions)`: the raw
    /// `attfdwoptions` `text[]` (`Some(bytes)`), or `None` when SQL NULL.
    /// `Ok(None)` on a cache miss (the caller raises
    /// `cache lookup failed for attribute %d of relation %u`). The caller runs
    /// `untransformRelOptions`.
    pub fn attribute_fdwoptions<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        attnum: i16,
    ) -> PgResult<Option<Option<PgVec<'mcx, u8>>>>
);

seam_core::seam!(
    /// `SearchSysCache1(PROCOID, ObjectIdGetDatum(func_id))` + `GETSTRUCT`
    /// projected to the `pg_proc` fields `fetch_fp_info` (`tcop/fastpath.c`)
    /// loads into its `struct fp_info`. `Ok(None)` is the C
    /// `!HeapTupleIsValid(func_htp)` cache miss (the caller raises
    /// `"function with OID %u does not exist"`); the installer owns the
    /// `ReleaseSysCache`. The projected row is copied into `mcx`.
    pub fn search_pg_proc_fastpath<'mcx>(
        mcx: Mcx<'mcx>,
        func_id: Oid,
    ) -> PgResult<Option<FastpathProcRow<'mcx>>>
);

// ===========================================================================
// objectaddress.c per-class catalog-row projections (getObjectDescription F1
// + getObjectIdentityParts F3). Each is the `table_open + systable_beginscan(
// <oid index>) + GETSTRUCT` (no-syscache catalogs) or `SearchSysCache1 +
// GETSTRUCT` (cached catalogs) of objectaddress.c, projected to the fixed-width
// fields the description/identity arm interpolates. `Ok(None)` is the C
// `!HeapTupleIsValid(tup)` "row vanished" (the caller raises its own error when
// `!missing_ok`); the installer owns the scan/`ReleaseSysCache` teardown. They
// panic loudly until the syscache/catalog owner installs them.
// ===========================================================================

seam_core::seam!(
    /// `get_catalog_object_by_oid(pg_cast, Anum_pg_cast_oid, castid)` +
    /// `GETSTRUCT` projected to `(castsource, casttarget)`
    /// (`Form_pg_cast`). `Ok(None)` on a scan miss.
    pub fn cast_source_target(castid: Oid) -> PgResult<Option<(Oid, Oid)>>
);

/// `((Form_pg_cast) GETSTRUCT(tup))` fields the parser coercion-pathway logic
/// reads (parse_coerce.c `find_coercion_pathway` / `IsBinaryCoercibleWithCast` /
/// `find_typmod_coercion_function`).
#[derive(Clone, Copy, Debug, Default)]
pub struct CastRow {
    /// `oid` — the pg_cast row's own OID.
    pub oid: Oid,
    /// `castfunc` — coercion function OID (`InvalidOid` if none).
    pub castfunc: Oid,
    /// `castcontext` — one of `COERCION_CODE_{IMPLICIT,ASSIGNMENT,EXPLICIT}`
    /// (`'i'`/`'a'`/`'e'`).
    pub castcontext: i8,
    /// `castmethod` — one of `COERCION_METHOD_{FUNCTION,BINARY,INOUT}`
    /// (`'f'`/`'b'`/`'i'`).
    pub castmethod: i8,
}

seam_core::seam!(
    /// `SearchSysCache2(CASTSOURCETARGET, srctype, targettype)` +
    /// `GETSTRUCT` projected to the [`CastRow`] fields (`Form_pg_cast`).
    /// `Ok(None)` on a scan miss (no pg_cast entry).
    pub fn cast_by_source_target(
        sourcetypeid: Oid,
        targettypeid: Oid,
    ) -> PgResult<Option<CastRow>>
);

seam_core::seam!(
    /// `table_open(AccessMethodOperatorRelationId) +
    /// systable_beginscan(AccessMethodOperatorOidIndexId, oid = amopid) +
    /// GETSTRUCT(Form_pg_amop)` projected to
    /// `(amopfamily, amopstrategy, amoplefttype, amoprighttype, amopopr)`.
    /// `Ok(None)` on a vanished row.
    pub fn amop_description_row(amopid: Oid) -> PgResult<Option<AmopDescriptionRow>>
);

seam_core::seam!(
    /// `table_open(AccessMethodProcedureRelationId) +
    /// systable_beginscan(AccessMethodProcedureOidIndexId, oid = amprocid) +
    /// GETSTRUCT(Form_pg_amproc)` projected to
    /// `(amprocfamily, amprocnum, amproclefttype, amprocrighttype, amproc)`.
    /// `Ok(None)` on a vanished row.
    pub fn amproc_description_row(amprocid: Oid) -> PgResult<Option<AmprocDescriptionRow>>
);

seam_core::seam!(
    /// `table_open(RewriteRelationId) + systable_beginscan(RewriteOidIndexId,
    /// oid = ruleid) + GETSTRUCT(Form_pg_rewrite)` projected to
    /// `(ev_class, NameStr(rulename))`. `Ok(None)` on a vanished row; the name
    /// is copied into `mcx`.
    pub fn rewrite_class_name<'mcx>(
        mcx: Mcx<'mcx>,
        ruleid: Oid,
    ) -> PgResult<Option<(Oid, PgString<'mcx>)>>
);

seam_core::seam!(
    /// `table_open(TriggerRelationId) + systable_beginscan(TriggerOidIndexId,
    /// oid = trigid) + GETSTRUCT(Form_pg_trigger)` projected to
    /// `(tgrelid, NameStr(tgname))`. `Ok(None)` on a vanished row; the name is
    /// copied into `mcx`.
    pub fn trigger_relid_name<'mcx>(
        mcx: Mcx<'mcx>,
        trigid: Oid,
    ) -> PgResult<Option<(Oid, PgString<'mcx>)>>
);

seam_core::seam!(
    /// `table_open(AuthMemRelationId) + systable_beginscan(AuthMemOidIndexId,
    /// oid = authmemid) + GETSTRUCT(Form_pg_auth_members)` projected to
    /// `(member, roleid)`. `Ok(None)` on a vanished row.
    pub fn auth_member_member_role(authmemid: Oid) -> PgResult<Option<(Oid, Oid)>>
);

seam_core::seam!(
    /// `table_open(DefaultAclRelationId) +
    /// systable_beginscan(DefaultAclOidIndexId, oid = defaclid) +
    /// GETSTRUCT(Form_pg_default_acl)` projected to
    /// `(defaclrole, defaclnamespace, defaclobjtype)`. `Ok(None)` on a vanished
    /// row.
    pub fn default_acl_row(defaclid: Oid) -> PgResult<Option<DefaultAclDescRow>>
);

seam_core::seam!(
    /// `table_open(PolicyRelationId) + systable_beginscan(PolicyOidIndexId,
    /// oid = polid) + GETSTRUCT(Form_pg_policy)` projected to
    /// `(polrelid, NameStr(polname))`. `Ok(None)` on a vanished row; the name
    /// is copied into `mcx`.
    pub fn policy_relid_name<'mcx>(
        mcx: Mcx<'mcx>,
        polid: Oid,
    ) -> PgResult<Option<(Oid, PgString<'mcx>)>>
);

seam_core::seam!(
    /// `SearchSysCache1(STATEXTOID, statextid)` +
    /// `GETSTRUCT(Form_pg_statistic_ext)` projected to `stxnamespace` for the
    /// statistics-object description (the name comes via
    /// [`statext_namespace_and_name`]). Unused by F1 directly; kept for parity.
    /// `Ok(None)` on a cache miss.
    pub fn statext_namespace(statextid: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `SearchSysCache1(USERMAPPINGOID, umid)` +
    /// `GETSTRUCT(Form_pg_user_mapping)` projected to `(umuser, umserver)`
    /// (the user-mapping description arm). `Ok(None)` on a cache miss.
    pub fn user_mapping_user_server(umid: Oid) -> PgResult<Option<(Oid, Oid)>>
);

seam_core::seam!(
    /// `SearchSysCache1(STATEXTOID, statsOid)` + `GETSTRUCT(Form_pg_statistic_ext)`
    /// projected to `stxrelid` (statscmds.c `RemoveStatisticsById` /
    /// `StatisticsGetRelation`). `Ok(None)` on a cache miss (the caller then
    /// reports `cache lookup failed for statistics object %u`). `Err` carries
    /// the syscache lookup `ereport(ERROR)`s.
    pub fn statext_get_relid(stats_oid: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `SearchSysCache1(STATEXTOID, statsOid)` returned as the owned
    /// `FormedTuple` copy (statscmds.c `AlterStatistics` / `RemoveStatisticsById`
    /// need the held tuple for `heap_modify_tuple` / `CatalogTupleDelete` of its
    /// `t_self`). `Ok(None)` on a cache miss. `Err` carries the syscache lookup
    /// `ereport(ERROR)`s.
    pub fn statext_search_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        stats_oid: Oid,
    ) -> PgResult<Option<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache2(STATEXTDATASTXOID, statsOid, inh)` returned as the owned
    /// `FormedTuple` copy, for `RemoveStatisticsDataById`'s
    /// `CatalogTupleDelete(&tup->t_self)`. `Ok(None)` when no such data row
    /// exists (the C "We don't know if the data row for inh value exists.").
    /// `Err` carries the syscache lookup `ereport(ERROR)`s.
    pub fn statext_data_search_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        stats_oid: Oid,
        inh: bool,
    ) -> PgResult<Option<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(PARAMETERACLOID, paramaclid)` +
    /// `SysCacheGetAttrNotNull(Anum_pg_parameter_acl_parname)` +
    /// `TextDatumGetCString` (the parameter-ACL description arm): the GUC
    /// parameter name copied into `mcx`. `Ok(None)` on a cache miss.
    pub fn parameter_acl_name<'mcx>(
        mcx: Mcx<'mcx>,
        paramaclid: Oid,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `systable_beginscan(pg_amop, AccessMethodOperatorOidIndexId, oid=amopid)`
    /// + `GETSTRUCT` projected to `(amopfamily, amoplefttype, amoprighttype,
    /// amopstrategy)` (`Form_pg_amop`). `Ok(None)` on a scan miss.
    pub fn amop_identity(amopid: Oid) -> PgResult<Option<(Oid, Oid, Oid, i16)>>
);

seam_core::seam!(
    /// `systable_beginscan(pg_amproc, AccessMethodProcedureOidIndexId,
    /// oid=amprocid)` + `GETSTRUCT` projected to `(amprocfamily,
    /// amproclefttype, amprocrighttype, amprocnum)` (`Form_pg_amproc`).
    /// `Ok(None)` on a scan miss.
    pub fn amproc_identity(amprocid: Oid) -> PgResult<Option<(Oid, Oid, Oid, i16)>>
);

seam_core::seam!(
    /// `get_catalog_object_by_oid(pg_rewrite, Anum_pg_rewrite_oid, ruleid)` +
    /// `GETSTRUCT` projected to `(rulename, ev_class)` (`Form_pg_rewrite`).
    /// `Ok(None)` on a scan miss.
    pub fn rewrite_name_evclass<'mcx>(
        mcx: Mcx<'mcx>,
        ruleid: Oid,
    ) -> PgResult<Option<(PgString<'mcx>, Oid)>>
);

seam_core::seam!(
    /// `get_catalog_object_by_oid(pg_trigger, Anum_pg_trigger_oid, trigid)` +
    /// `GETSTRUCT` projected to `(tgname, tgrelid)` (`Form_pg_trigger`).
    /// `Ok(None)` on a scan miss.
    pub fn trigger_name_relid<'mcx>(
        mcx: Mcx<'mcx>,
        trigid: Oid,
    ) -> PgResult<Option<(PgString<'mcx>, Oid)>>
);

seam_core::seam!(
    /// `systable_beginscan(pg_default_acl, DefaultAclOidIndexId, oid=daclid)` +
    /// `GETSTRUCT` projected to `(defaclrole, defaclnamespace, defaclobjtype)`
    /// (`Form_pg_default_acl`; `defaclobjtype` is the raw `DEFACLOBJ_*` char).
    /// `Ok(None)` on a scan miss.
    pub fn default_acl_identity(daclid: Oid) -> PgResult<Option<(Oid, Oid, i8)>>
);

seam_core::seam!(
    /// `SearchSysCache1(TRFOID, transformid)` +
    /// `GETSTRUCT(Form_pg_transform)` projected to `(trftype, trflang)` (the
    /// transform description arm). `Ok(None)` on a cache miss.
    pub fn transform_type_lang(transformid: Oid) -> PgResult<Option<(Oid, Oid)>>
);

seam_core::seam!(
    /// `get_catalog_object_by_oid(pg_policy, Anum_pg_policy_oid, polid)` +
    /// `GETSTRUCT` projected to `(polname, polrelid)` (`Form_pg_policy`).
    /// `Ok(None)` on a scan miss.
    pub fn policy_name_relid<'mcx>(
        mcx: Mcx<'mcx>,
        polid: Oid,
    ) -> PgResult<Option<(PgString<'mcx>, Oid)>>
);

seam_core::seam!(
    /// `SearchSysCache1(EVENTTRIGGEROID, evtid)` +
    /// `NameStr(GETSTRUCT(Form_pg_event_trigger)->evtname)` copied into `mcx`
    /// (the event-trigger description arm). `Ok(None)` on a cache miss.
    pub fn event_trigger_name<'mcx>(
        mcx: Mcx<'mcx>,
        evtid: Oid,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(PUBLICATIONREL, pubrelid)` +
    /// `GETSTRUCT(Form_pg_publication_rel)` projected to `(prpubid, prrelid)`
    /// (the publication-table description arm). `Ok(None)` on a cache miss.
    pub fn publication_rel_pub_rel(pubrelid: Oid) -> PgResult<Option<(Oid, Oid)>>
);

seam_core::seam!(
    /// `SearchSysCache1(AMOID, amid)` +
    /// `NameStr(GETSTRUCT(Form_pg_am)->amname)` copied into `mcx` (the
    /// access-method description arm and the opclass arm's access-method name).
    /// `Ok(None)` on a cache miss; the caller raises `cache lookup failed for
    /// access method %u` when `!missing_ok`.
    pub fn am_name<'mcx>(mcx: Mcx<'mcx>, amid: Oid) -> PgResult<Option<PgString<'mcx>>>
);

/// `((Form_pg_amop) GETSTRUCT(tup))` fields the `getObjectDescription` amop arm
/// interpolates (objectaddress.c 3229-3292).
#[derive(Clone, Copy, Debug, Default)]
pub struct AmopDescriptionRow {
    /// `amopfamily`.
    pub amopfamily: Oid,
    /// `amopstrategy`.
    pub amopstrategy: i16,
    /// `amoplefttype`.
    pub amoplefttype: Oid,
    /// `amoprighttype`.
    pub amoprighttype: Oid,
    /// `amopopr`.
    pub amopopr: Oid,
}

/// `((Form_pg_amproc) GETSTRUCT(tup))` fields the `getObjectDescription` amproc
/// arm interpolates (objectaddress.c 3294-3357).
#[derive(Clone, Copy, Debug, Default)]
pub struct AmprocDescriptionRow {
    /// `amprocfamily`.
    pub amprocfamily: Oid,
    /// `amprocnum`.
    pub amprocnum: i16,
    /// `amproclefttype`.
    pub amproclefttype: Oid,
    /// `amprocrighttype`.
    pub amprocrighttype: Oid,
    /// `amproc`.
    pub amproc: Oid,
}

/// `((Form_pg_default_acl) GETSTRUCT(tup))` fields the `getObjectDescription`
/// default-ACL arm interpolates (objectaddress.c 3761-3873).
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultAclDescRow {
    /// `defaclrole`.
    pub defaclrole: Oid,
    /// `defaclnamespace` (`InvalidOid` for a non-schema-scoped default ACL).
    pub defaclnamespace: Oid,
    /// `defaclobjtype` — the `DEFACLOBJ_*` discriminant char.
    pub defaclobjtype: i8,
}

seam_core::seam!(
    /// `SearchSysCache1(CONSTROID, conoid)` +
    /// `GETSTRUCT(Form_pg_constraint)->conrelid` (the `getObjectDescription`
    /// constraint arm: a constraint with an owning relation prints
    /// "constraint %s on %s"). `Ok(None)` on a cache miss; `InvalidOid` for a
    /// non-relation-scoped constraint.
    pub fn constraint_relid(conoid: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `SearchSysCache1(PUBLICATIONNAMESPACE, pubschemaid)` +
    /// `GETSTRUCT(Form_pg_publication_namespace)` projected to
    /// `(pnpubid, pnnspid)` (objectaddress.c `getPublicationSchemaInfo`).
    /// `Ok(None)` on a cache miss.
    pub fn publication_namespace_pub_nsp(pubschemaid: Oid) -> PgResult<Option<(Oid, Oid)>>
);

seam_core::seam!(
    /// `GetAttrDefaultColumnAddress(attrdefoid)` (pg_attrdef.c):
    /// `table_open(AttrDefaultRelationId) +
    /// systable_beginscan(AttrDefaultOidIndexId, oid = attrdefoid) +
    /// GETSTRUCT(Form_pg_attrdef)` projected to the owning column's
    /// `(adrelid, adnum)`. `Ok(None)` is the C `InvalidObjectAddress` return
    /// (no such pg_attrdef entry).
    pub fn attr_default_column(attrdefoid: Oid) -> PgResult<Option<(Oid, i16)>>
);

seam_core::seam!(
    /// `getObjectIdentityParts` `ConstraintRelationId` arm:
    /// `SearchSysCache1(CONSTROID, conid)` + `GETSTRUCT` projected to
    /// `(conname, conrelid, contypid)` (`Form_pg_constraint`). `Ok(None)` on a
    /// cache miss.
    pub fn constraint_identity<'mcx>(
        mcx: Mcx<'mcx>,
        conid: Oid,
    ) -> PgResult<Option<(PgString<'mcx>, Oid, Oid)>>
);

seam_core::seam!(
    /// `SearchSysCache1(PUBLICATIONNAMESPACE, pubnspid)` + `GETSTRUCT`
    /// projected to `(pnpubid, pnnspid)` (`Form_pg_publication_namespace`),
    /// feeding objectaddress.c's `getPublicationSchemaInfo`. `Ok(None)` on a
    /// cache miss.
    pub fn publication_namespace_ids(pubnspid: Oid) -> PgResult<Option<(Oid, Oid)>>
);

seam_core::seam!(
    /// `SearchSysCache1(PUBLICATIONREL, pubrelid)` + `GETSTRUCT` projected to
    /// `(prpubid, prrelid)` (`Form_pg_publication_rel`). `Ok(None)` on a cache
    /// miss.
    pub fn publication_rel_ids(pubrelid: Oid) -> PgResult<Option<(Oid, Oid)>>
);

seam_core::seam!(
    /// `GetAttrDefaultColumnAddress(attrdefoid)` (pg_attrdef.c): scan
    /// `pg_attrdef` by OID and project `GETSTRUCT`'s `(adrelid, adnum)`
    /// (`Form_pg_attrdef`), from which the caller rebuilds the column
    /// `ObjectAddress` (`RelationRelationId`, `adrelid`, `adnum`). `Ok(None)`
    /// when no such attrdef row exists (the C `InvalidObjectAddress`).
    pub fn attrdef_column(attrdefoid: Oid) -> PgResult<Option<(Oid, i16)>>
);

/* ------------------------------------------------------------------------
 *  pg_constraint reads/writes for backend-catalog-pg-constraint
 *  (SearchSysCache1(CONSTROID) + pg_class relchecks read/decrement, plus the
 *  conkey/FK array-column detoast reads). Owned by backend-utils-cache-syscache
 *  (and heaptuple for the array reads); panic until they land.
 * ------------------------------------------------------------------------ */

seam_core::seam!(
    /// `GetSysCacheHashValue1(CONSTROID, ObjectIdGetDatum(oid))` (syscache.c) —
    /// the catcache hash value for a `pg_constraint` row, used by
    /// `ri_LoadConstraintInfo`'s `oid_hash_value` / hash-of-root. `Err` carries
    /// the catcache-path `ereport(ERROR)`s.
    pub fn get_syscache_hash_value_constroid(oid: Oid) -> PgResult<u32>
);

seam_core::seam!(
    /// `SearchSysCache1(CONSTROID, ObjectIdGetDatum(conoid))` + `GETSTRUCT`
    /// projected to the scalar `Form_pg_constraint` columns plus the `conkey`
    /// array column, copied into `mcx`, then `ReleaseSysCache`. `Ok(None)` on a
    /// cache miss (`!HeapTupleIsValid`); the caller raises `cache lookup failed
    /// for constraint %u`. `Err` carries OOM / catcache `ereport(ERROR)`s.
    pub fn search_constraint_form_by_oid(
        conoid: Oid,
    ) -> PgResult<Option<types_catalog::pg_constraint::ConstraintFormCopy>>
);

seam_core::seam!(
    /// `SearchSysCache1(CONSTROID, ObjectIdGetDatum(conoid))` + `heap_copytuple`
    /// — the full `pg_constraint` tuple (for `DeconstructFkConstraintRow`),
    /// copied into `mcx`, then `ReleaseSysCache`. `Ok(None)` on a cache miss.
    /// `Err` carries OOM / catcache `ereport(ERROR)`s.
    pub fn search_constraint_tuple_by_oid<'mcx>(
        mcx: Mcx<'mcx>,
        conoid: Oid,
    ) -> PgResult<Option<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid))` +
    /// `GETSTRUCT(Form_pg_class)->relchecks` (RemoveConstraintById): the
    /// relation's check-constraint count. `Ok(None)` on a cache miss; the
    /// caller raises `cache lookup failed for relation %u`.
    pub fn fetch_relchecks(relid: Oid) -> PgResult<Option<i16>>
);

seam_core::seam!(
    /// `SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid))` returned as the
    /// owned writable `FormedTuple` copy (RemoveConstraintById's relchecks
    /// update needs the held tuple for `heap_modify_tuple` over its `t_self`,
    /// preserving all non-`relchecks` columns). `Ok(None)` on a cache miss
    /// (`!HeapTupleIsValid`); the caller raises `cache lookup failed for
    /// relation %u`. `Err` carries the syscache lookup `ereport(ERROR)`s.
    pub fn search_syscache_copy_pg_class_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
    ) -> PgResult<Option<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>>>
);

seam_core::seam!(
    /// RemoveConstraintById's relchecks update: `table_open(RelationRelationId,
    /// RowExclusiveLock)` + `SearchSysCacheCopy1(RELOID)` + `classForm->relchecks--`
    /// + `CatalogTupleUpdate` + `heap_freetuple` + `table_close`. The
    /// `relchecks == 0` guard is the caller's (it has already read the value via
    /// [`fetch_relchecks`]); this seam performs the decrement-and-store. `Err`
    /// carries the heap-mutation `ereport(ERROR)`s.
    pub fn decrement_relchecks(relid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `StoreAttrDefault`'s pg_attribute update (pg_attrdef.c):
    /// `table_open(AttributeRelationId, RowExclusiveLock)` +
    /// `SearchSysCacheCopy2(ATTNUM, relid, attnum)` (error
    /// `cache lookup failed for attribute %d of relation %u` via `Ok(None)`),
    /// read `attStruct->attgenerated`, then `heap_modify_tuple` setting
    /// `atthasdef = true` + `CatalogTupleUpdate` + `table_close`. Returns the
    /// pre-existing `attgenerated` so the caller can choose the dependency type
    /// (the C `attgenerated ? DEPENDENCY_INTERNAL : DEPENDENCY_AUTO`).
    /// `Ok(None)` on the cache miss; `Err` carries the heap-mutation
    /// `ereport(ERROR)`s.
    pub fn set_attribute_has_default(
        relid: Oid,
        attnum: types_core::AttrNumber,
    ) -> PgResult<Option<i8>>
);

seam_core::seam!(
    /// `RemoveAttrDefaultById`'s pg_attribute reset (pg_attrdef.c):
    /// `table_open(AttributeRelationId, RowExclusiveLock)` +
    /// `SearchSysCacheCopy2(ATTNUM, myrelid, myattnum)` (error
    /// `cache lookup failed for attribute %d of relation %u` via `Ok(false)`),
    /// then `((Form_pg_attribute) GETSTRUCT(tuple))->atthasdef = false` +
    /// `CatalogTupleUpdate` + `table_close`. Returns `false` on the cache miss
    /// (the C "shouldn't happen" elog); `Err` carries the heap-mutation
    /// `ereport(ERROR)`s.
    pub fn clear_attribute_has_default(
        relid: Oid,
        attnum: types_core::AttrNumber,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `SysCacheGetAttrNotNull(CONSTROID, constrTup, Anum_pg_constraint_conkey)`
    /// + `DatumGetArrayTypeP` (heaptuple/array detoast): the `conkey` 1-D
    /// smallint array of a not-null constraint tuple, with the validated
    /// `ARR_*` fields. The caller performs the 1-D/elemtype/hasnull/dim
    /// validation + error message. `Err` carries the detoast `ereport(ERROR)`s.
    pub fn get_conkey_array(
        tuple: &types_tuple::backend_access_common_heaptuple::FormedTuple<'_>,
    ) -> PgResult<types_catalog::pg_constraint::ConKeyArray>
);

seam_core::seam!(
    /// `DeconstructFkConstraintRow`'s `SysCacheGetAttrNotNull` /
    /// `SysCacheGetAttr` reads of `conkey` / `confkey` / `conpfeqop` /
    /// `conppeqop` / `conffeqop` / `confdelsetcols` + `DatumGetArrayTypeP`
    /// (heaptuple/array detoast). Returns all six array columns; the caller
    /// performs every dimension/elemtype/null validation + the FK error
    /// messages. `confdelsetcols` is `None` when the column is SQL NULL.
    pub fn deconstruct_fk_arrays(
        tuple: &types_tuple::backend_access_common_heaptuple::FormedTuple<'_>,
    ) -> PgResult<types_catalog::pg_constraint::FkArrayProjection>
);

seam_core::seam!(
    /// `heap_getattr(tuple, Anum_pg_constraint_conkey,
    /// RelationGetDescr(pg_constraint), &isNull)` + `DatumGetArrayTypeP`
    /// (get_primary_key_attnos): the `conkey` array column of a scanned
    /// `pg_constraint` tuple, against the open relation's descriptor.
    /// `Ok(None)` when the column is SQL NULL (the C `isNull` branch → the
    /// caller's `null conkey for constraint %u`). `Err` carries the detoast
    /// `ereport(ERROR)`s.
    pub fn heap_get_conkey(
        rel: &types_rel::RelationData<'_>,
        tuple: &types_tuple::backend_access_common_heaptuple::FormedTuple<'_>,
    ) -> PgResult<Option<types_catalog::pg_constraint::ConKeyArray>>
);

seam_core::seam!(
    /// `(Form_pg_constraint) GETSTRUCT(tup)` of a held `pg_constraint` tuple
    /// (AdjustNotNullInheritance reads the form off the tuple returned by
    /// `findNotNullConstraintAttnum`). Projects the fixed-width scalar columns.
    /// Owned by the heaptuple/syscache layer (GETSTRUCT decode). `Err` carries
    /// any decode `ereport(ERROR)`.
    pub fn read_constraint_form(
        tuple: &types_tuple::backend_access_common_heaptuple::FormedTuple<'_>,
    ) -> PgResult<types_catalog::pg_constraint::FormData_pg_constraint>
);

/* ------------------------------------------------------------------------
 *  OID-keyed existence probes (catalog/dependency.c's per-class
 *  `SearchSysCacheExists1` "did this object get concurrently dropped?"
 *  checks, modeled after `reloid_exists`). Each is a single catcache probe;
 *  `Err` carries the catcache lookup's own error surface.
 * ------------------------------------------------------------------------ */

seam_core::seam!(
    /// `SearchSysCacheExists1(PROCOID, ObjectIdGetDatum(proc_oid))`: whether a
    /// `pg_proc` row exists for `proc_oid`.
    pub fn procoid_exists(proc_oid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `SearchSysCacheExists1(OPEROID, ObjectIdGetDatum(oper_oid))`: whether a
    /// `pg_operator` row exists for `oper_oid`.
    pub fn operoid_exists(oper_oid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `SearchSysCacheExists1(TYPEOID, ObjectIdGetDatum(type_oid))`: whether a
    /// `pg_type` row exists for `type_oid`.
    pub fn typeoid_exists(type_oid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `SearchSysCacheExists1(COLLOID, ObjectIdGetDatum(coll_oid))`: whether a
    /// `pg_collation` row exists for `coll_oid`.
    pub fn colloid_exists(coll_oid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `SearchSysCacheExists1(TSCONFIGOID, ObjectIdGetDatum(cfg_oid))`: whether
    /// a `pg_ts_config` row exists for `cfg_oid`.
    pub fn tsconfigoid_exists(cfg_oid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `SearchSysCacheExists1(TSDICTOID, ObjectIdGetDatum(dict_oid))`: whether a
    /// `pg_ts_dict` row exists for `dict_oid`.
    pub fn tsdictoid_exists(dict_oid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `SearchSysCacheExists1(NAMESPACEOID, ObjectIdGetDatum(nsp_oid))`: whether
    /// a `pg_namespace` row exists for `nsp_oid`.
    pub fn namespaceoid_exists(nsp_oid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `SearchSysCache1(cacheId, ObjectIdGetDatum(key))` for a *dynamic*
    /// `cacheId` (dependency.c `DropObjectById`): looks the tuple up by OID and
    /// returns its heap TID (`tup->t_self`) for a subsequent `CatalogTupleDelete`,
    /// or `None` on a cache miss (the caller raises its own "cache lookup failed"
    /// error). `ReleaseSysCache` is folded in (only the TID escapes). NOTE: not
    /// installed — the syscache owner models caches statically and this generic
    /// dynamic-`cacheId` primitive is not yet ported; tracked in
    /// `CONTRACT_RECONCILE_PENDING`.
    pub fn search_syscache1_tid(
        cache_id: i32,
        key: Oid,
    ) -> PgResult<Option<types_tuple::heaptuple::ItemPointerData>>
);

/* ---------------------------------------------------------------------------
 * ACL/owner catalog-row projections (the aclmask/aclcheck family in
 * catalog/aclchk.c — the F0 keystone for the aclchk check-half). Each reads
 * the object's owner + `aclitem[]` ACL off SearchSysCache1/2 + GETSTRUCT +
 * SysCacheGetAttr, returning the owner OID plus the decoded `&[AclItem]`
 * (`None` = SQL-null column, where aclchk builds the hardwired `acldefault`).
 * `Ok(None)` on a cache miss (`!HeapTupleIsValid`) — the caller decides
 * between its is_missing fast path and its `ereport`/`elog`, as in C.
 * ------------------------------------------------------------------------- */

seam_core::seam!(
    /// `pg_class_aclmask_ext`'s catalog read (aclchk.c):
    /// `SearchSysCache1(RELOID, ObjectIdGetDatum(table_oid))` then `GETSTRUCT`
    /// (`relowner`/`relkind`/`relnamespace`) + `SysCacheGetAttr(RELOID, tuple,
    /// Anum_pg_class_relacl)`. The `relacl` column is detoasted
    /// (`DatumGetAclP`) and decoded to its `aclitem[]` items. `Ok(None)` on a
    /// cache miss. `relnamespace` is surfaced so the caller can compute
    /// `IsSystemClass` (catalog.c). Can `ereport(ERROR)` (OOM on the copy),
    /// carried on `Err`.
    pub fn pg_class_owner_acl<'mcx>(
        mcx: Mcx<'mcx>,
        table_oid: Oid,
    ) -> PgResult<Option<ClassOwnerAcl<'mcx>>>
);

seam_core::seam!(
    /// `pg_attribute_aclmask_ext`'s catalog read (aclchk.c):
    /// `SearchSysCache2(ATTNUM, ObjectIdGetDatum(table_oid),
    /// Int16GetDatum(attnum))` then `SysCacheGetAttr(ATTNUM, attTuple,
    /// Anum_pg_attribute_attacl)`. Returns the decoded column ACL items, the
    /// `attisdropped` flag, and `Ok(None)` on a cache miss (no such
    /// pg_attribute row). The inner `Option<PgVec>` is `None` for a SQL-null
    /// `attacl` (the very common case — aclchk hard-wires "no privileges"
    /// there). The relation owner is fetched separately by the caller via
    /// [`pg_class_owner_acl`], matching the C two-lookup structure. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn pg_attribute_owner_acl<'mcx>(
        mcx: Mcx<'mcx>,
        table_oid: Oid,
        attnum: i16,
    ) -> PgResult<Option<(bool, Option<PgVec<'mcx, AclItem>>)>>
);

seam_core::seam!(
    /// `pg_namespace_aclmask_ext`'s catalog read (aclchk.c):
    /// `SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nsp_oid))` then
    /// `GETSTRUCT(nspowner)` + `SysCacheGetAttr(NAMESPACEOID, tuple,
    /// Anum_pg_namespace_nspacl)`. `Ok(None)` on a cache miss. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn pg_namespace_owner_acl<'mcx>(
        mcx: Mcx<'mcx>,
        nsp_oid: Oid,
    ) -> PgResult<Option<NamespaceOwnerAcl<'mcx>>>
);

seam_core::seam!(
    /// `pg_type_aclmask_ext`'s catalog read (aclchk.c):
    /// `SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid))` then `GETSTRUCT`
    /// (`typowner`/`typacl`), resolving the "true array type -> consult element
    /// type" (`IsTrueArrayType`, via `typelem`/`typsubscript`) and "multirange
    /// -> consult range type" (`typtype == TYPTYPE_MULTIRANGE`, via
    /// `get_multirange_range`) redirects inside the projection so the caller
    /// gets the *effective* type's `(owner, acl)`. A cache miss at any step is
    /// `Ok(None)` (the caller distinguishes its is_missing path from `elog`).
    /// Can `ereport(ERROR)`, carried on `Err`.
    pub fn pg_type_owner_acl<'mcx>(
        mcx: Mcx<'mcx>,
        type_oid: Oid,
    ) -> PgResult<Option<TypeOwnerAcl<'mcx>>>
);

seam_core::seam!(
    /// `object_aclmask_ext`'s generic catalog read (aclchk.c):
    /// `SearchSysCache1(cacheid, ObjectIdGetDatum(objectid))` then
    /// `SysCacheGetAttrNotNull(cacheid, tuple, owner_attnum)` (the owner) +
    /// `SysCacheGetAttr(cacheid, tuple, acl_attnum)` (the `aclitem[]` ACL,
    /// decoded). The caller resolves `cacheid` (`get_object_catcache_oid`),
    /// `owner_attnum` (`get_object_attnum_owner`) and `acl_attnum`
    /// (`get_object_attnum_acl`) for its `classid` and passes them in, keeping
    /// this a class-agnostic projection. `Ok(None)` on a cache miss. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn object_owner_acl<'mcx>(
        mcx: Mcx<'mcx>,
        cacheid: i32,
        objectid: Oid,
        owner_attnum: i16,
        acl_attnum: i16,
    ) -> PgResult<Option<ObjectOwnerAcl<'mcx>>>
);

seam_core::seam!(
    /// `pg_parameter_aclmask`'s catalog read (aclchk.c):
    /// `SearchSysCache1(PARAMETERACLNAME, PointerGetDatum(partext))` then
    /// `SysCacheGetAttr(PARAMETERACLNAME, tuple, Anum_pg_parameter_acl_paracl)`.
    /// `parname` is the already-canonicalized parameter name (the caller runs
    /// `convert_GUC_name_for_parameter_acl` + `cstring_to_text`). Returns the
    /// decoded `paracl` items, with the outer `Option` distinguishing a cache
    /// miss (`None` -> the C `ACL_NO_RIGHTS` no-entry case) from a present row
    /// whose `paracl` may itself be SQL-null (`Some(None)` -> build default).
    /// Can `ereport(ERROR)`, carried on `Err`.
    pub fn parameter_acl_by_name<'mcx>(
        mcx: Mcx<'mcx>,
        parname: &str,
    ) -> PgResult<Option<Option<PgVec<'mcx, AclItem>>>>
);

seam_core::seam!(
    /// `pg_parameter_acl_aclmask`'s catalog read (aclchk.c):
    /// `SearchSysCache1(PARAMETERACLOID, ObjectIdGetDatum(acl_oid))` then
    /// `SysCacheGetAttr(PARAMETERACLOID, tuple, Anum_pg_parameter_acl_paracl)`.
    /// `Ok(None)` on a cache miss (the caller raises "parameter ACL with OID %u
    /// does not exist"); `Some(None)` for a present row with a SQL-null
    /// `paracl` (build default). Can `ereport(ERROR)`, carried on `Err`.
    pub fn parameter_acl_by_oid<'mcx>(
        mcx: Mcx<'mcx>,
        acl_oid: Oid,
    ) -> PgResult<Option<Option<PgVec<'mcx, AclItem>>>>
);

seam_core::seam!(
    /// `SearchSysCache2(RULERELNAME, ObjectIdGetDatum(ev_class),
    /// PointerGetDatum(rulename))` + `GETSTRUCT` (rewriteDefine.c). The
    /// writable `pg_rewrite` tuple for `(ev_class, rulename)` plus its deformed
    /// fixed columns (`oid`/`ev_class`/`ev_type`/`ev_enabled`/`is_instead`),
    /// or `None` if no such rule exists (`!HeapTupleIsValid`). The whole tuple
    /// crosses for the InsertRule replace branch's `heap_modify_tuple` and for
    /// EnableDisableRule / RenameRewriteRule's in-place column update at
    /// `ruletup->t_self`; the form serves the `ruleform->ev_class` /
    /// `ev_enabled` / `ev_type` / `oid` reads. Can `ereport(ERROR)` (OOM on the
    /// copy), carried on `Err`.
    pub fn rule_tuple_by_relname<'mcx>(
        mcx: Mcx<'mcx>,
        ev_class: Oid,
        rulename: &str,
    ) -> PgResult<
        Option<(
            types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
            types_catalog::pg_rewrite::FormData_pg_rewrite,
        )>,
    >
);

seam_core::seam!(
    /// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` + `GETSTRUCT`
    /// (`relkind`, `relnamespace`) for `RangeVarCallbackForRenameRule`
    /// (rewriteDefine.c:762-765). `Ok(None)` on a cache miss (the C
    /// "concurrently dropped" fast return). Can `ereport(ERROR)`, carried on
    /// `Err`.
    pub fn class_relkind_namespace(relid: Oid) -> PgResult<Option<(u8, Oid)>>
);
