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

seam_core::seam!(
    /// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` projected to the
    /// `Form_pg_class.relam` field (the relation's access method). `Ok(None)`
    /// on a cache miss (`!HeapTupleIsValid`); the installer owns the
    /// `ReleaseSysCache`.
    pub fn search_relation_relam(relid: Oid) -> PgResult<Option<Oid>>
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
