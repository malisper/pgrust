// DefineIndex (partitioned recursion included) + ComputeIndexAttrs +
// CheckPredicate + ChooseIndex*Name* + IndexSetParentIndex (indexcmds.c).
// Loud: CONCURRENTLY, named opclasses, WITH options,
// exclusion/WITHOUT OVERLAPS, index detach.
use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttr, SysCacheKey, INDEXRELID};
use catalog_index::{
    IndexCreateExtra, BTREE_AM_OID,
    INDEX_CREATE_ADD_CONSTRAINT, INDEX_CREATE_IS_PRIMARY,
};
use datum::Datum;
use execindexing::IndexInfo;
use mcx::{Mcx, PgString, PgVec};
use types_core::{
    AttrNumber, InvalidOid, Oid, RegProcedure, INDEX_MAX_KEYS, NAMEDATALEN,
    RELATION_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_INDETERMINATE_COLLATION,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_TOO_MANY_COLUMNS,
    ERRCODE_UNDEFINED_COLUMN, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::rawnodes::{IndexElem, IndexStmt, SortByDir, SortByNulls};
use types_rel::{InplaceUpdateTupleLock, Relation, ShareLock, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

use crate::GetDefaultOpClass;

const NamespaceRelationId: Oid = 2615;
const RELPERSISTENCE_TEMP: i8 = b't' as i8;
const ClassNameNspIndexId: Oid = 2663;
const Anum_pg_class_relname: AttrNumber = 2;
const Anum_pg_class_relnamespace: AttrNumber = 3;
const F_NAMEEQ: RegProcedure = 62;
const F_OIDEQ: RegProcedure = 184;
const ACL_CREATE: u64 = 1 << 9;
const INDOPTION_DESC: i16 = 1 << 0;
const INDOPTION_NULLS_FIRST: i16 = 1 << 1;
const ATTRIBUTE_GENERATED_VIRTUAL: i8 = b'v' as i8;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: indexcmds {what}")
}

#[track_caller]
#[cold]
#[inline(never)]
fn virtual_generated_err(primary: bool, isconstraint: bool) -> Box<PgError> {
    err(
        if primary {
            "primary keys on virtual generated columns are not supported"
        } else if isconstraint {
            "unique constraints on virtual generated columns are not supported"
        } else {
            "indexes on virtual generated columns are not supported"
        }
        .into(),
        types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

pub(crate) fn define_index_for_alter<'mcx>(
    mcx: Mcx<'mcx>,
    table_id: Oid,
    stmt_node: types_nodes::Node<'mcx>,
    is_rebuild: bool,
    skip_build: bool,
) -> PgResult<Oid> {
    let stmt = stmt_node.as_variant::<IndexStmt>().expect("IndexStmt");
    let skip_build = skip_build || stmt.oldNumber != types_core::InvalidRelFileNumber;
    let index_oid = DefineIndex(
        mcx,
        table_id,
        stmt,
        InvalidOid,
        InvalidOid,
        InvalidOid,
        true,
        !is_rebuild,
        false,
        skip_build,
        is_rebuild,
    )?;
    // C ATExecAddIndex tail: a TryReuseIndex relfilenumber means the new
    // index adopted the dropped edition's storage — cancel its pending unlink.
    if stmt.oldNumber != types_core::InvalidRelFileNumber {
        let irel = indexam::index_open(mcx, index_oid, types_rel::NoLock)?;
        irel.rd_createSubid.set(stmt.oldCreateSubid);
        irel.rd_firstRelfilelocatorSubid.set(stmt.oldFirstRelfilelocatorSubid);
        catalog_storage::RelationPreserveStorage(irel.rd_locator.get(), true);
        indexam::index_close(irel, types_rel::NoLock)?;
    }
    Ok(index_oid)
}

// SAFETY contract: d points at a detoasted, not-null oidvector.
unsafe fn oidvector_values<'a>(d: Datum) -> &'a [Oid] {
    let p = d.as_usize() as *const array::oidvector;
    core::slice::from_raw_parts(p.add(1) as *const Oid, (*p).dim1 as usize)
}

const Anum_pg_index_indnkeyatts: i32 = 4;
const Anum_pg_index_indisvalid: i32 = 11;
const Anum_pg_index_indcollation: i32 = 17;
const Anum_pg_index_indclass: i32 = 18;
const Anum_pg_index_indexprs: i32 = 20;
const Anum_pg_index_indpred: i32 = 21;

// CheckIndexCompatible (indexcmds.c:180-354). Expressions, predicates and
// invalid indexes are assumed incompatible, as in C.
pub fn CheckIndexCompatible<'mcx>(
    mcx: Mcx<'mcx>,
    old_id: Oid,
    access_method_name: &str,
    attribute_list: &types_nodes::NodeList<'mcx>,
    exclusion_op_names: &types_nodes::NodeList<'mcx>,
    is_without_overlaps: bool,
) -> PgResult<bool> {
    if !exclusion_op_names.is_nil() || is_without_overlaps {
        unported("CheckIndexCompatible: exclusion / WITHOUT OVERLAPS constraints");
    }
    let relationId = catalog_index::IndexGetRelation(mcx, old_id, false)?;
    let am = resolve_index_am(Some(access_method_name))?;
    let (accessMethodId, amname, amcanorder) = (am.oid, am.name.as_str(), am.amcanorder);

    let numberOfAttributes = attribute_list.len();
    debug_assert!(numberOfAttributes > 0 && numberOfAttributes <= INDEX_MAX_KEYS as usize);

    let mut indexInfo = IndexInfo {
        ii_NumIndexAttrs: numberOfAttributes as i32,
        ii_AmCache: None,
        ii_NumIndexKeyAttrs: numberOfAttributes as i32,
        ii_IndexAttrNumbers: [0; INDEX_MAX_KEYS as usize],
        ii_Expressions: types_nodes::NodeList::nil(),
        ii_ExpressionsState: PgVec::new_in(mcx),
        ii_Predicate: types_nodes::NodeList::nil(),
        ii_PredicateState: None,
        ii_Unique: false,
        ii_NullsNotDistinct: false,
        ii_ReadyForInserts: false,
        ii_Summarizing: false,
        ii_Concurrent: false,
        ii_BrokenHotChain: false,
        ii_UniqueOps: [0; INDEX_MAX_KEYS as usize],
        ii_UniqueProcs: [0; INDEX_MAX_KEYS as usize],
        ii_UniqueStrats: [0; INDEX_MAX_KEYS as usize],
        ii_HasExclusion: false,
        ii_ExclusionOps: [0; INDEX_MAX_KEYS as usize],
        ii_ExclusionProcs: [0; INDEX_MAX_KEYS as usize],
        ii_ExclusionStrats: [0; INDEX_MAX_KEYS as usize],
        ii_WithoutOverlaps: false,
    };
    let mut typeIds = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut collationIds = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut opclassIds = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut opclassOptions = [Datum::null(); INDEX_MAX_KEYS as usize];
    let mut coloptions = [0i16; INDEX_MAX_KEYS as usize];
    let rel = table::table_open(mcx, relationId, types_rel::AccessShareLock)?;
    ComputeIndexAttrs(
        mcx,
        &rel,
        &mut indexInfo,
        &mut typeIds,
        &mut collationIds,
        &mut opclassIds,
        &mut opclassOptions,
        &mut coloptions,
        attribute_list,
        exclusion_op_names,
        false,
        is_without_overlaps,
        accessMethodId,
        amname,
        amcanorder,
        None,
    )?;
    rel.close(types_rel::AccessShareLock)?;

    let Some(tup) = SearchSysCache1(INDEXRELID, SysCacheKey::Value(Datum::from_oid(old_id)))?
    else {
        panic!("cache lookup failed for index {old_id}");
    };
    let notnull = |anum: i32| -> PgResult<Datum> {
        let (d, isnull) = SysCacheGetAttr(INDEXRELID, &tup, anum)?;
        assert!(!isnull, "unexpected NULL pg_index column {anum}");
        Ok(d)
    };
    let (_, pred_null) = SysCacheGetAttr(INDEXRELID, &tup, Anum_pg_index_indpred)?;
    let (_, exprs_null) = SysCacheGetAttr(INDEXRELID, &tup, Anum_pg_index_indexprs)?;
    let indisvalid = notnull(Anum_pg_index_indisvalid)?.as_bool();
    if !(pred_null && exprs_null && indisvalid) {
        ReleaseSysCache(tup);
        return Ok(false);
    }
    let old_natts = notnull(Anum_pg_index_indnkeyatts)?.as_i16() as usize;
    debug_assert_eq!(old_natts, numberOfAttributes);
    // SAFETY (both): not-null oidvector columns of the held syscache tuple.
    let same = unsafe { oidvector_values(notnull(Anum_pg_index_indclass)?) }[..old_natts]
        == opclassIds[..old_natts]
        && unsafe { oidvector_values(notnull(Anum_pg_index_indcollation)?) }[..old_natts]
            == collationIds[..old_natts];
    ReleaseSysCache(tup);
    if !same {
        return Ok(false);
    }

    let irel = indexam::index_open(mcx, old_id, types_rel::AccessShareLock)?;
    let mut ret = true;
    for i in 0..old_natts {
        if coerce::IsPolymorphicType(lsyscache::get_opclass_input_type(opclassIds[i])?)
            && irel.rd_att.attr(i).atttypid != typeIds[i]
        {
            ret = false;
            break;
        }
    }
    // CompareOpclassOptions (indexcmds.c:307-318, 341-396). Both sides are
    // transformRelOptions-built text[] images, so the byte comparison of the
    // 4-byte-header normalized images coincides with C's array_eq under C
    // collation.
    if ret {
        for i in 0..old_natts {
            let old = lsyscache::get_attoptions(mcx, old_id, (i + 1) as i16)?;
            let new = opclassOptions[i];
            match (old == Datum::null(), new == Datum::null()) {
                (true, true) => {}
                (false, false) => {
                    if reloptions::text_array_image(mcx, old)?
                        != reloptions::text_array_image(mcx, new)?
                    {
                        ret = false;
                        break;
                    }
                }
                _ => {
                    ret = false;
                    break;
                }
            }
        }
    }
    // ii_ExclusionOps comparison: exclusion indexes are loud upstream.
    indexam::index_close(irel, types_rel::NoLock)?;
    Ok(ret)
}

struct IndexAmInfo {
    oid: Oid,
    name: String,
    kind: types_relscan::IndexAmKind,
    amcanorder: bool,
    amcanunique: bool,
    amcanmulticol: bool,
    amcaninclude: bool,
}

// DefineIndex's AM lookup (indexcmds.c:840-901): pg_am AMNAME probe with the
// rtree->gist substitution hack; capability flags per each builtin handler's
// IndexAmRoutine (nbtree.c:122, hash.c:65, ginutil.c:45, gist.c:66,
// spgutils.c:51, brin.c:257).
fn resolve_index_am(name: Option<&str>) -> PgResult<IndexAmInfo> {
    let Some(mut name) = name else {
        unported("DefineIndex: access method None (AMNAME lookup)");
    };
    let mut probe = index_am_probe(name)?;
    if probe.is_none() && name == "rtree" {
        elog_seams::ereport_msg::call(
            types_error::NOTICE,
            "substituting access method \"gist\" for obsolete method \"rtree\"".to_string(),
            None,
        )?;
        name = "gist";
        probe = index_am_probe(name)?;
    }
    let Some((oid, amhandler)) = probe else {
        return Err(err(
            format!("access method \"{name}\" does not exist"),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    };
    let kind = amapi::GetIndexAmRoutine(amhandler);
    let (amcanorder, amcanunique, amcanmulticol, amcaninclude) = index_am_flags(kind);
    Ok(IndexAmInfo {
        oid,
        name: name.to_string(),
        kind,
        amcanorder,
        amcanunique,
        amcanmulticol,
        amcaninclude,
    })
}

// (amcanorder, amcanunique, amcanmulticol, amcaninclude) per builtin handler.
fn index_am_flags(kind: types_relscan::IndexAmKind) -> (bool, bool, bool, bool) {
    use types_relscan::IndexAmKind::*;
    match kind {
        Btree => (true, true, true, true),
        Hash => (false, false, false, false),
        Gin => (false, false, true, false),
        Gist => (false, false, true, true),
        Spgist => (false, false, false, true),
        Brin => (false, false, true, false),
        // contrib/bloom blhandler: multicolumn only.
        Bloom => (false, false, true, false),
        #[allow(unreachable_patterns)]
        _ => (false, false, false, false),
    }
}

// pg_am AMNAME probe: (oid, amhandler); None if no such access method.
fn index_am_probe(amname: &str) -> PgResult<Option<(Oid, Oid)>> {
    const Anum_pg_am_oid: i32 = 1;
    const Anum_pg_am_amhandler: i32 = 3;
    let Some(tup) = SearchSysCache1(cache_syscache::cacheinfo::AMNAME, SysCacheKey::Str(amname))?
    else {
        return Ok(None);
    };
    let notnull = |anum: i32| -> PgResult<Datum> {
        cache_syscache::SysCacheGetAttrNotNull(cache_syscache::cacheinfo::AMNAME, &tup, anum)
    };
    let oid = notnull(Anum_pg_am_oid)?.as_oid();
    let amhandler = notnull(Anum_pg_am_amhandler)?.as_oid();
    ReleaseSysCache(tup);
    Ok(Some((oid, amhandler)))
}

// get_am_name (amcmds.c) for error details.
fn get_am_name(amid: Oid) -> String {
    const Anum_pg_am_amname: i32 = 2;
    let Ok(Some(tup)) =
        SearchSysCache1(cache_syscache::cacheinfo::AMOID, SysCacheKey::Value(Datum::from_oid(amid)))
    else {
        return "???".to_string();
    };
    let name = cache_syscache::SysCacheGetAttrNotNull(cache_syscache::cacheinfo::AMOID, &tup, Anum_pg_am_amname)
        .map(|d| {
            // SAFETY: amname is the row's inline NameData column.
            let nd = unsafe { *(d.as_usize() as *const types_tuple::NameData) };
            core::str::from_utf8(nd.name_str()).unwrap_or("???").to_string()
        })
        .unwrap_or_else(|_| "???".to_string());
    ReleaseSysCache(tup);
    name
}

// GetOperatorFromCompareType (indexcmds.c): equality/overlaps/contained-by
// operator lookup for temporal constraints via the index AM's stratnum
// translation. rhstype = InvalidOid means the opclass input type.
pub fn GetOperatorFromCompareType<'mcx>(
    mcx: Mcx<'mcx>,
    opclass: Oid,
    rhstype: Oid,
    cmptype: lsyscache::CompareType,
) -> PgResult<(Oid, u16)> {
    debug_assert!(matches!(
        cmptype,
        lsyscache::COMPARE_EQ | lsyscache::COMPARE_OVERLAP | lsyscache::COMPARE_CONTAINED_BY
    ));
    let amid = lsyscache::get_opclass_method(opclass)?;
    let mut opid = InvalidOid;
    let mut strat: u16 = 0;
    let cannot_identify = |opcintype: Oid,
                           detail: String|
     -> PgResult<Box<types_error::PgError>> {
        let msg = match cmptype {
            lsyscache::COMPARE_EQ => format!(
                "could not identify an equality operator for type {}",
                format_type::format_type_be(opcintype)?
            ),
            lsyscache::COMPARE_OVERLAP => format!(
                "could not identify an overlaps operator for type {}",
                format_type::format_type_be(opcintype)?
            ),
            _ => format!(
                "could not identify a contained-by operator for type {}",
                format_type::format_type_be(opcintype)?
            ),
        };
        Ok(Box::new((*err(msg, ERRCODE_UNDEFINED_OBJECT)).with_detail(detail)))
    };
    let mut opcintype = InvalidOid;
    if let Some((opfamily, intype)) = lsyscache::get_opclass_opfamily_and_input_type(opclass)? {
        opcintype = intype;
        strat = amapi::IndexAmTranslateCompareType(cmptype, amid, opfamily, true)?;
        if strat == 0 {
            let famname = lsyscache::get_opfamily_name(mcx, opfamily, false)?
                .expect("opfamily name");
            return Err(cannot_identify(
                opcintype,
                format!(
                    "Could not translate compare type {cmptype} for operator family \"{}\" of access method \"{}\".",
                    famname.as_str(),
                    get_am_name(amid)
                ),
            )?);
        }
        // rhstype parameterized so FKs can ask for a <@ whose rhs matches the
        // aggregate (range_agg returns anymultirange).
        let rhstype = if rhstype == InvalidOid { opcintype } else { rhstype };
        opid = lsyscache::get_opfamily_member(opfamily, opcintype, rhstype, strat as i16)?;
    }
    if opid == InvalidOid {
        let famname = match lsyscache::get_opclass_opfamily_and_input_type(opclass)? {
            Some((opfamily, _)) => lsyscache::get_opfamily_name(mcx, opfamily, false)?
                .map(|n| n.as_str().to_string())
                .unwrap_or_default(),
            None => String::new(),
        };
        return Err(cannot_identify(
            opcintype,
            format!(
                "There is no suitable operator in operator family \"{famname}\" for access method \"{}\".",
                get_am_name(amid)
            ),
        )?);
    }
    Ok((opid, strat))
}

#[allow(clippy::too_many_arguments)]
pub fn DefineIndex<'mcx>(
    mcx: Mcx<'mcx>,
    tableId: Oid,
    stmt: &IndexStmt<'mcx>,
    indexRelationId: Oid,
    parentIndexId: Oid,
    parentConstraintId: Oid,
    is_alter_table: bool,
    check_rights: bool,
    check_not_in_use: bool,
    skip_build: bool,
    quiet: bool,
) -> PgResult<Oid> {
    let concurrent = stmt.concurrent
        && lsyscache::get_rel_persistence(tableId)? != RELPERSISTENCE_TEMP;
    if stmt.reset_default_tblspc {
        guc::set_config_option(
            "default_tablespace",
            Some(""),
            types_guc::PGC_USERSET,
            types_guc::PGC_S_SESSION,
            guc::GUC_ACTION_SAVE,
            true,
            types_error::ErrorLevel(0),
            false,
        )?;
    }
    let exclusion = !stmt.excludeOpNames.is_nil() || stmt.iswithoutoverlaps;
    let am = resolve_index_am(stmt.accessMethod)?;
    let (accessMethodId, amname, amcanorder, amcanunique, amcanmulticol, amcaninclude) = (
        am.oid,
        am.name.as_str(),
        am.amcanorder,
        am.amcanunique,
        am.amcanmulticol,
        am.amcaninclude,
    );
    if stmt.unique && !stmt.iswithoutoverlaps && !amcanunique {
        return Err(err(
            format!("access method \"{amname}\" does not support unique indexes"),
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }
    if !stmt.indexIncludingParams.is_nil() {
        if !amcaninclude {
            return Err(err(
                format!("access method \"{amname}\" does not support included columns"),
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
    }
    // C: exclusion requires amRoutine->amgettuple (gin and brin lack it).
    if exclusion
        && matches!(
            am.kind,
            types_relscan::IndexAmKind::Gin
                | types_relscan::IndexAmKind::Brin
                | types_relscan::IndexAmKind::Bloom
        )
    {
        return Err(err(
            format!("access method \"{amname}\" does not support exclusion constraints"),
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }
    if stmt.iswithoutoverlaps && amname != "gist" {
        return Err(err(
            format!("access method \"{amname}\" does not support WITHOUT OVERLAPS constraints"),
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }

    let reloptions =
        reloptions::transformRelOptions(mcx, None, &stmt.options, None, &[], false, false)?;
    reloptions::index_reloptions(mcx, accessMethodId, reloptions.as_deref(), true)?;

    let mut root_save_nestlevel = guc::NewGUCNestLevel();
    guc::RestrictSearchPath()?;

    let numberOfKeyAttributes = stmt.indexParams.len();
    // C: allIndexParams = list_concat_copy(indexParams, indexIncludingParams);
    // key columns are list positions < numberOfKeyAttributes (indexcmds.c:652).
    let mut allIndexParams = stmt.indexParams.clone_in(mcx)?;
    allIndexParams.concat(mcx, &stmt.indexIncludingParams)?;
    let numberOfAttributes = allIndexParams.len();
    if numberOfKeyAttributes > 1 && !amcanmulticol {
        return Err(err(
            format!("access method \"{amname}\" does not support multicolumn indexes"),
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }
    if numberOfKeyAttributes == 0 {
        return Err(err("must specify at least one column".into(), ERRCODE_INVALID_OBJECT_DEFINITION));
    }
    if numberOfAttributes > INDEX_MAX_KEYS as usize {
        return Err(err(
            format!("cannot use more than {INDEX_MAX_KEYS} columns in an index"),
            ERRCODE_TOO_MANY_COLUMNS,
        ));
    }

    let lockmode = if concurrent { types_rel::ShareUpdateExclusiveLock } else { ShareLock };
    let rel = table::table_open(mcx, tableId, lockmode)?;
    let (root_save_userid, root_save_sec_context) = miscinit::GetUserIdAndSecContext();
    let guard = miscinit::SecContextGuard::security_restricted(rel.rd_rel.relowner);

    let namespaceId = rel.rd_rel.relnamespace;

    match rel.rd_rel.relkind {
        RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_PARTITIONED_TABLE => {}
        other => {
            return Err(Box::new(
                (*err(
                    format!("cannot create index on relation \"{}\"", rel.name()),
                    ERRCODE_WRONG_OBJECT_TYPE,
                ))
                .with_detail(pg_class_seams::errdetail_relkind_not_supported::call(other)?),
            ))
        }
    }
    let partitioned = rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE;
    if partitioned && stmt.concurrent {
        return Err(err(
            format!(
                "cannot create index on partitioned table \"{}\" concurrently",
                rel.name()
            ),
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }

    if check_not_in_use {
        catalog_heap::CheckTableNotInUse(&rel, "CREATE INDEX")?;
    }

    if check_rights && !miscinit_seams::is_bootstrap_processing_mode::call() {
        let aclresult = aclchk_seams::object_aclcheck::call(
            NamespaceRelationId,
            namespaceId,
            root_save_userid,
            ACL_CREATE,
        )?;
        if aclresult != 0 {
            let nspname = lsyscache::get_namespace_name(mcx, namespaceId)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return Err(err(
                format!("permission denied for schema {nspname}"),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
    }

    if rel.rd_rel.relisshared {
        unported("DefineIndex: shared relations");
    }
    let tablespaceId = match stmt.tableSpace {
        Some(name) => {
            let oid = commands_tablespace::get_tablespace_oid(mcx, name, false)?;
            if partitioned && oid == init_small::globals::MyDatabaseTableSpace() {
                return Err(err(
                    "cannot specify default tablespace for partitioned relations".to_string(),
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                ));
            }
            oid
        }
        None => commands_tablespace::GetDefaultTablespace(
            mcx,
            rel.rd_rel.relpersistence,
            partitioned,
        )?,
    };
    if check_rights
        && tablespaceId != InvalidOid
        && tablespaceId != init_small::globals::MyDatabaseTableSpace()
    {
        let aclresult = aclchk_seams::object_aclcheck::call(
            commands_tablespace::TableSpaceRelationId,
            tablespaceId,
            root_save_userid,
            ACL_CREATE,
        )?;
        if aclresult != 0 {
            let ctx = mcx::MemoryContext::new("DefineIndex");
            let name = commands_tablespace::get_tablespace_name(ctx.mcx(), tablespaceId)?;
            aclchk::aclcheck_error(
                aclresult,
                types_nodes::parsenodes::ObjectType::OBJECT_TABLESPACE,
                name.as_ref().map(|n| std::str::from_utf8(n.name_str()).unwrap_or("")).unwrap_or(""),
            )?;
        }
    }
    if tablespaceId == commands_tablespace::GLOBALTABLESPACE_OID {
        return Err(err(
            "only shared relations can be placed in pg_global tablespace".to_string(),
            types_error::ERRCODE_INVALID_PARAMETER_VALUE,
        ));
    }

    let indexColNames = ChooseIndexColumnNames(mcx, &allIndexParams)?;
    let name_storage;
    let indexRelationName: &str = match stmt.idxname {
        Some(n) => n,
        None => {
            name_storage = if stmt.primary {
                ChooseRelationName(mcx, rel.name(), None, "pkey", namespaceId, true)?
            } else if stmt.isconstraint {
                let addition = ChooseIndexNameAddition(mcx, &indexColNames)?;
                let suffix = if exclusion { "excl" } else { "key" };
                ChooseRelationName(mcx, rel.name(), Some(addition.as_str()), suffix, namespaceId, true)?
            } else {
                ChooseIndexName(mcx, rel.name(), namespaceId, &indexColNames)?
            };
            name_storage.as_str()
        }
    };

    // Must run as the table owner (indexcmds.c:906, after the :689-690
    // switch): contain_mutable_functions_after_planning pre-evaluates
    // constant immutable calls.
    if let Some(wc) = stmt.whereClause {
        CheckPredicate(mcx, wc)?;
    }

    let mut indexInfo = IndexInfo {
        ii_NumIndexAttrs: numberOfAttributes as i32,
        ii_AmCache: None,
        ii_NumIndexKeyAttrs: numberOfKeyAttributes as i32,
        ii_IndexAttrNumbers: [0; INDEX_MAX_KEYS as usize],
        ii_Expressions: types_nodes::NodeList::nil(),
        ii_ExpressionsState: PgVec::new_in(mcx),
        ii_Predicate: clauses::make_ands_implicit(mcx, stmt.whereClause)?,
        ii_PredicateState: None,
        ii_Unique: stmt.unique,
        ii_NullsNotDistinct: stmt.nulls_not_distinct,
        ii_ReadyForInserts: !concurrent,
        ii_Summarizing: false,
        ii_Concurrent: concurrent,
        ii_BrokenHotChain: false,
        ii_UniqueOps: [0; INDEX_MAX_KEYS as usize],
        ii_UniqueProcs: [0; INDEX_MAX_KEYS as usize],
        ii_UniqueStrats: [0; INDEX_MAX_KEYS as usize],
        ii_HasExclusion: exclusion,
        ii_ExclusionOps: [0; INDEX_MAX_KEYS as usize],
        ii_ExclusionProcs: [0; INDEX_MAX_KEYS as usize],
        ii_ExclusionStrats: [0; INDEX_MAX_KEYS as usize],
        ii_WithoutOverlaps: stmt.iswithoutoverlaps,
    };

    let mut typeIds = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut collationIds = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut opclassIds = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut opclassOptions = [Datum::null(); INDEX_MAX_KEYS as usize];
    let mut coloptions = [0i16; INDEX_MAX_KEYS as usize];
    ComputeIndexAttrs(
        mcx,
        &rel,
        &mut indexInfo,
        &mut typeIds,
        &mut collationIds,
        &mut opclassIds,
        &mut opclassOptions,
        &mut coloptions,
        &allIndexParams,
        &stmt.excludeOpNames,
        stmt.isconstraint,
        stmt.iswithoutoverlaps,
        accessMethodId,
        amname,
        amcanorder,
        Some(&mut root_save_nestlevel),
    )?;

    if stmt.primary {
        catalog_index::index_check_primary_key(mcx, &rel, &indexInfo, is_alter_table)?;
    }

    // A unique index on a partitioned table must cover the partition key
    // with the same notion of equality; global uniqueness has no other proof.
    if partitioned && (stmt.unique || exclusion) {
        let key = partcache::RelationGetPartitionKey(&rel)?;
        let constraint_type = if stmt.primary {
            "PRIMARY KEY"
        } else if stmt.unique {
            "UNIQUE"
        } else {
            "EXCLUDE"
        };
        for i in 0..key.partnatts as usize {
            // List/range partkeys use btree opclasses, hash partkeys hash
            // ones (indexcmds.c:997-1001, in sync with ComputePartitionAttrs).
            let eq_strategy: i16 = if key.strategy == partcache::PARTITION_STRATEGY_HASH {
                lsyscache::HTEqualStrategyNumber
            } else {
                BTEqualStrategyNumber as i16
            };
            let ptkey_eqop = lsyscache::get_opfamily_member(
                key.partopfamily[i],
                key.partopcintype[i],
                key.partopcintype[i],
                eq_strategy,
            )?;
            if ptkey_eqop == InvalidOid {
                panic!(
                    "missing operator {}({},{}) in partition opfamily {}",
                    eq_strategy, key.partopcintype[i], key.partopcintype[i],
                    key.partopfamily[i]
                );
            }
            if key.partattrs[i] == 0 {
                return Err(Box::new(
                    (*err(
                        format!(
                            "unsupported {constraint_type} constraint with partition key \
                             definition"
                        ),
                        types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                    ))
                    .with_detail(format!(
                        "{constraint_type} constraints cannot be used when partition keys \
                         include expressions."
                    )),
                ));
            }
            let mut found = false;
            for j in 0..indexInfo.ii_NumIndexKeyAttrs as usize {
                if key.partattrs[i] != indexInfo.ii_IndexAttrNumbers[j] {
                    continue;
                }
                if key.partcollation[i] != collationIds[j] {
                    continue;
                }
                let idx_eqop = if exclusion {
                    indexInfo.ii_ExclusionOps[j]
                } else if let Some((idx_opfamily, idx_opcintype)) =
                    lsyscache::get_opclass_opfamily_and_input_type(opclassIds[j])?
                {
                    let op = lsyscache::get_opfamily_member_for_cmptype(
                        idx_opfamily,
                        idx_opcintype,
                        idx_opcintype,
                        lsyscache::COMPARE_EQ,
                    )?;
                    if op == InvalidOid {
                        unported("DefineIndex: no-equality-operator report (opfamily name)");
                    }
                    op
                } else {
                    InvalidOid
                };
                if idx_eqop != InvalidOid {
                    if ptkey_eqop == idx_eqop {
                        found = true;
                        break;
                    } else if exclusion {
                        // C prints get_opname here (indexcmds.c:1079), not format_operator.
                        let opname_pg = lsyscache::get_opname(mcx, idx_eqop)?.expect("opname");
                        let opname = opname_pg.as_str();
                        let att = rel.rd_att.attr(key.partattrs[i] as usize - 1);
                        let attname = core::str::from_utf8(att.attname.name_str())
                            .expect("attname");
                        return Err(err(
                            format!(
                                "cannot match partition key to index on column \"{attname}\" \
                                 using non-equal operator \"{opname}\""
                            ),
                            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                        ));
                    }
                }
            }
            if !found {
                let att = rel.rd_att.attr(key.partattrs[i] as usize - 1);
                let attname = core::str::from_utf8(att.attname.name_str())
                    .expect("attname")
                    .to_string();
                return Err(Box::new(
                    (*err(
                        "unique constraint on partitioned table must include all \
                         partitioning columns"
                            .into(),
                        types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                    ))
                    .with_detail(format!(
                        "{constraint_type} constraint on table \"{}\" lacks column \
                         \"{attname}\" which is part of the partition key.",
                        rel.name()
                    )),
                ));
            }
        }
    }

    for i in 0..numberOfAttributes {
        let attno = indexInfo.ii_IndexAttrNumbers[i];
        if attno < 0 {
            return Err(err(
                "index creation on system columns is not supported".into(),
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
        // C divergence: expression columns (attno == 0) skipped — C reads
        // attrs[-1]; the expression pass below screens them.
        if attno > 0
            && rel.rd_att.attr(attno as usize - 1).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL
        {
            return Err(virtual_generated_err(stmt.primary, stmt.isconstraint));
        }
    }
    if !indexInfo.ii_Expressions.is_nil() || !indexInfo.ii_Predicate.is_nil() {
        let mut check = |list: &types_nodes::NodeList<'mcx>| -> PgResult<()> {
            for e in list.iter() {
                for v in vars::pull_var_clause(mcx, e, 0)?.iter() {
                    if v.as_var().expect("pull_var_clause").varattno < 0 {
                        return Err(err(
                            "index creation on system columns is not supported".into(),
                            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                        ));
                    }
                }
            }
            Ok(())
        };
        check(&indexInfo.ii_Expressions)?;
        check(&indexInfo.ii_Predicate)?;

        let mut indexattrs = types_nodes::Bitmapset::empty();
        for e in indexInfo.ii_Expressions.iter() {
            vars::pull_varattnos(mcx, e, 1, &mut indexattrs)?;
        }
        for e in indexInfo.ii_Predicate.iter() {
            vars::pull_varattnos(mcx, e, 1, &mut indexattrs)?;
        }
        let mut j = -1;
        loop {
            j = indexattrs.next_member(j);
            if j < 0 {
                break;
            }
            let attno = j + types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
            if attno > 0
                && rel.rd_att.attr(attno as usize - 1).attgenerated
                    == ATTRIBUTE_GENERATED_VIRTUAL
            {
                return Err(virtual_generated_err(false, stmt.isconstraint));
            }
        }
    }

    let mut colname_refs: PgVec<'_, &str> = PgVec::new_in(mcx);
    for n in indexColNames.iter() {
        colname_refs.push(n.as_str());
    }

    let safe_index =
        indexInfo.ii_Expressions.is_nil() && indexInfo.ii_Predicate.is_nil();

    // C indexcmds.c:1177-1198: report index creation if appropriate (delayed
    // till after most of the error checks). errmsg_internal: not translated.
    if stmt.isconstraint && !quiet {
        let constraint_type = if stmt.primary {
            "PRIMARY KEY"
        } else if stmt.unique {
            "UNIQUE"
        } else if !stmt.excludeOpNames.is_nil() {
            "EXCLUDE"
        } else {
            return Err(Box::new(PgError::new(ERROR, "unknown constraint type")));
        };
        elog_seams::ereport::call(
            PgError::new(
                types_error::DEBUG1,
                format!(
                    "{} {} will create implicit index \"{}\" for table \"{}\"",
                    if is_alter_table { "ALTER TABLE / ADD" } else { "CREATE TABLE /" },
                    constraint_type,
                    indexRelationName,
                    rel.name()
                ),
            )
            .with_location("indexcmds.c", 1195, "DefineIndex"),
        )?;
    }

    let mut flags = (if stmt.primary { INDEX_CREATE_IS_PRIMARY } else { 0 })
        | (if stmt.isconstraint { INDEX_CREATE_ADD_CONSTRAINT } else { 0 });
    if stmt.if_not_exists {
        flags |= catalog_index::INDEX_CREATE_IF_NOT_EXISTS;
    }
    if skip_build || concurrent {
        flags |= catalog_index::INDEX_CREATE_SKIP_BUILD;
    }
    if concurrent {
        flags |= catalog_index::INDEX_CREATE_CONCURRENT;
    }
    if partitioned {
        flags |= catalog_index::INDEX_CREATE_SKIP_BUILD | catalog_index::INDEX_CREATE_PARTITIONED;
        // ONLY with existing partitions: catalog rows only, invalid until
        // every partition gains an attached index.
        if let Some(rv) = stmt.relation {
            if !rv.inh {
                let pd = partdesc::RelationGetPartitionDesc(&rel, true)?;
                if pd.nparts != 0 {
                    flags |= catalog_index::INDEX_CREATE_INVALID;
                }
            }
        }
    }

    let (indexRelationId, createdConstraintId) = catalog_index::index_create(
        mcx,
        &rel,
        indexRelationName,
        indexRelationId,
        &mut indexInfo,
        &colname_refs,
        accessMethodId,
        tablespaceId,
        &collationIds[..numberOfAttributes],
        &opclassIds[..numberOfAttributes],
        &coloptions[..numberOfAttributes],
        &IndexCreateExtra {
            flags,
            constr_flags: (if stmt.deferrable {
            catalog_index::INDEX_CONSTR_CREATE_DEFERRABLE
        } else {
            0
        }) | (if stmt.initdeferred {
            catalog_index::INDEX_CONSTR_CREATE_INIT_DEFERRED
        } else {
            0
        }) | (if stmt.iswithoutoverlaps {
            catalog_index::INDEX_CONSTR_CREATE_WITHOUT_OVERLAPS
        } else {
            0
        }),
            allow_system_table_mods: false,
            is_internal: !check_rights,
            parent_index_relid: parentIndexId,
            parent_constraint_id: parentConstraintId,
            reloptions: reloptions.as_deref(),
            opclass_options: Some(&opclassOptions[..numberOfAttributes]),
            stattargets: None,
            old_number: stmt.oldNumber,
        },
    )?;

    if indexRelationId == InvalidOid {
        // IF NOT EXISTS found a duplicate; index_create already NOTICE'd.
        guc::AtEOXact_GUC(false, root_save_nestlevel);
        guard.restore();
        rel.close(types_rel::NoLock)?;
        return Ok(indexRelationId);
    }

    guc::AtEOXact_GUC(false, root_save_nestlevel);
    let root_save_nestlevel = guc::NewGUCNestLevel();
    guc::RestrictSearchPath()?;

    if let Some(comment) = stmt.idxcomment {
        commands_comment::CreateComments(
            mcx,
            indexRelationId,
            RELATION_RELATION_ID,
            0,
            Some(comment),
        )?;
    }

    if partitioned {
        let recurse = stmt.relation.map(|rv| rv.inh).unwrap_or(true);
        let partdesc = partdesc::RelationGetPartitionDesc(&rel, true)?;
        if recurse && partdesc.nparts > 0 {
            let nparts = partdesc.nparts;
            let mut part_oids: PgVec<'_, Oid> = mcx::vec_with_capacity_in(mcx, nparts)?;
            for i in 0..nparts {
                part_oids.push(partdesc.oids[i]);
            }
            let mut invalidate_parent = false;
            let parentIndex = indexam::index_open(mcx, indexRelationId, lockmode)?;
            // The IndexInfo built above hasn't been through expression
            // preprocessing; child comparison wants the BuildIndexInfo form.
            let parentInfo = execindexing::BuildIndexInfo(mcx, &parentIndex)?;

            for i in 0..nparts {
                let childRelid = part_oids[i];
                let childrel = table::table_open(mcx, childRelid, lockmode)?;
                let (child_save_userid, child_save_sec_context) =
                    miscinit::GetUserIdAndSecContext();
                let child_guard =
                    miscinit::SecContextGuard::security_restricted(childrel.rd_rel.relowner);
                let child_save_nestlevel = guc::NewGUCNestLevel();
                guc::RestrictSearchPath()?;

                // Foreign-table partitions get no index: skip for a plain
                // index, fail for a constraint index (indexcmds.c:1390-1409).
                if childrel.rd_rel.relkind == types_rel::RELKIND_FOREIGN_TABLE {
                    if stmt.unique || stmt.primary {
                        return Err(Box::new(
                            (*err(
                                format!(
                                    "cannot create unique index on partitioned table \"{}\"",
                                    rel.name()
                                ),
                                ERRCODE_WRONG_OBJECT_TYPE,
                            ))
                            .with_detail(format!(
                                "Table \"{}\" contains partitions that are foreign tables.",
                                rel.name()
                            )),
                        ));
                    }
                    guc::AtEOXact_GUC(false, child_save_nestlevel);
                    child_guard.restore();
                    childrel.close(lockmode)?;
                    continue;
                }

                let childidxs = relcache::RelationGetIndexList(mcx, childRelid)?;
                let attmap = tupdesc::build_attrmap_by_name(mcx, childrel.descr(), rel.descr())?;

                let mut found = false;
                for &cldidxid in childidxs.iter() {
                    if pg_inherits::has_superclass(mcx, cldidxid)? {
                        continue;
                    }
                    let cldidx = indexam::index_open(mcx, cldidxid, lockmode)?;
                    let cldIdxInfo = execindexing::BuildIndexInfo(mcx, &cldidx)?;
                    if catalog_index::CompareIndexInfo(
                        mcx,
                        &cldIdxInfo,
                        &parentInfo,
                        &cldidx,
                        &parentIndex,
                        &attmap,
                    )? {
                        let mut cldConstrOid = InvalidOid;
                        if createdConstraintId != InvalidOid {
                            cldConstrOid = pg_constraint::get_relation_idx_constraint_oid(
                                mcx, childRelid, cldidxid,
                            )?;
                            if cldConstrOid == InvalidOid {
                                indexam::index_close(cldidx, lockmode)?;
                                continue;
                            }
                        }
                        IndexSetParentIndex(mcx, &cldidx, indexRelationId)?;
                        if createdConstraintId != InvalidOid {
                            pg_constraint::ConstraintSetParentConstraint(
                                mcx,
                                cldConstrOid,
                                createdConstraintId,
                                childRelid,
                            )?;
                        }
                        if !cldidx.rd_index.as_ref().expect("rd_index").indisvalid {
                            invalidate_parent = true;
                        }
                        found = true;
                        indexam::index_close(cldidx, types_rel::NoLock)?;
                        break;
                    }
                    indexam::index_close(cldidx, lockmode)?;
                }

                guc::AtEOXact_GUC(false, child_save_nestlevel);
                child_guard.restore();
                childrel.close(types_rel::NoLock)?;

                if !found {
                    let childStmt = parse_utilcmd::generateClonedIndexStmt(
                        mcx,
                        None,
                        &parentIndex,
                        &attmap,
                    )?
                    .0;
                    // Recurse as the starting user ID; callee re-restricts.
                    let _ = (child_save_userid, child_save_sec_context);
                    let recurse_guard =
                        miscinit::SecContextGuard::set(root_save_userid, root_save_sec_context);
                    let childAddr = DefineIndex(
                        mcx,
                        childRelid,
                        &childStmt,
                        InvalidOid,
                        indexRelationId,
                        createdConstraintId,
                        is_alter_table,
                        check_rights,
                        check_not_in_use,
                        skip_build,
                        quiet,
                    )?;
                    recurse_guard.restore();
                    if !lsyscache::get_index_isvalid(childAddr)? {
                        invalidate_parent = true;
                    }
                }
            }

            indexam::index_close(parentIndex, lockmode)?;

            if invalidate_parent {
                set_pg_index_invalid(mcx, indexRelationId)?;
                xact::CommandCounterIncrement()?;
            }
        }

        guc::AtEOXact_GUC(false, root_save_nestlevel);
        guard.restore();
        rel.close(types_rel::NoLock)?;
        return Ok(indexRelationId);
    }

    guc::AtEOXact_GUC(false, root_save_nestlevel);
    guard.restore();

    if !concurrent {
        rel.close(types_rel::NoLock)?;
        return Ok(indexRelationId);
    }

    // Concurrent build: phases per validate_index()'s protocol; session lock
    // on the table pins it across the mid-command commits.
    let heaprelid = rel.rd_lockInfo.lockRelId;
    let heaplocktag = [types_storage::lock::LOCKTAG::relation(heaprelid.dbId, heaprelid.relId)];
    rel.close(types_rel::NoLock)?;

    lmgr::LockRelationIdForSession(&heaprelid, types_rel::ShareUpdateExclusiveLock)?;

    snapmgr::PopActiveSnapshot()?;
    xact::CommitTransactionCommand()?;
    xact::StartTransactionCommand()?;
    if safe_index {
        procarray::SetIndexsafeProcflags()?;
    }

    lmgr::WaitForLockersMultiple(mcx, &heaplocktag, ShareLock)?;

    let snap = snapmgr::GetTransactionSnapshot()?;
    snapmgr::PushActiveSnapshot(&snap)?;
    catalog_index::index_concurrently_build(mcx, tableId, indexRelationId)?;
    snapmgr::PopActiveSnapshot()?;

    xact::CommitTransactionCommand()?;
    xact::StartTransactionCommand()?;
    if safe_index {
        procarray::SetIndexsafeProcflags()?;
    }

    lmgr::WaitForLockersMultiple(mcx, &heaplocktag, ShareLock)?;

    let snap = snapmgr::GetTransactionSnapshot()?;
    let snapshot = snapmgr::RegisterSnapshot(Some(&snap))?.expect("registered snapshot");
    snapmgr::PushActiveSnapshot(&snapshot)?;

    catalog_index::validate_index(mcx, tableId, indexRelationId, &snapshot)?;

    let limit_xmin = snapshot.xmin;
    snapmgr::PopActiveSnapshot()?;
    snapmgr::UnregisterSnapshot(Some(&snapshot));

    xact::CommitTransactionCommand()?;
    xact::StartTransactionCommand()?;
    if safe_index {
        procarray::SetIndexsafeProcflags()?;
    }

    crate::WaitForOlderSnapshots(limit_xmin)?;

    let snap = snapmgr::GetTransactionSnapshot()?;
    snapmgr::PushActiveSnapshot(&snap)?;
    catalog_index::index_set_state_flags(
        mcx,
        indexRelationId,
        catalog_index::IndexStateFlagsAction::CreateSetValid,
    )?;
    snapmgr::PopActiveSnapshot()?;

    inval::invalidate::CacheInvalidateRelcacheByRelid(heaprelid.relId)?;

    lmgr::UnlockRelationIdForSession(&heaprelid, types_rel::ShareUpdateExclusiveLock)?;

    Ok(indexRelationId)
}

// ResolveOpClass (indexcmds.c), named-opclass arm; the NIL arm stays inline
// in ComputeIndexAttrs.
pub(crate) fn ResolveOpClass(
    opclass: &types_nodes::NodeList<'_>,
    attrType: Oid,
    accessMethodName: &str,
    accessMethodId: Oid,
) -> PgResult<Oid> {
    // C DeconstructQualifiedName's default arm raises the improper-qualified-name
    // error itself for 0 or >3 parts; collect every part so it can.
    let names: Vec<&str> =
        opclass.iter().map(|n| n.as_string().expect("opclass holds Strings").sval).collect();
    let (schemaname, opcname) = catalog_namespace::DeconstructQualifiedName(&names)?;

    let opClassId = if let Some(schemaname) = schemaname {
        let namespaceId = catalog_namespace::LookupExplicitNamespace(schemaname, false)?;
        syscache_seams::lookup_pg_opclass_oid_by_name::call(
            accessMethodId,
            opcname,
            namespaceId,
        )?
    } else {
        catalog_namespace::OpclassnameGetOpcid(accessMethodId, opcname)?
    };
    if opClassId == InvalidOid {
        return Err(err(
            format!(
                "operator class \"{}\" does not exist for access method \"{}\"",
                if schemaname.is_some() { names.join(".") } else { opcname.to_string() },
                accessMethodName
            ),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    }

    let Some(shape) = syscache_seams::lookup_pg_opclass_shape::call(opClassId)? else {
        return Err(err(
            format!(
                "operator class \"{}\" does not exist for access method \"{}\"",
                names.join("."),
                accessMethodName
            ),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    };
    if !coerce::IsBinaryCoercible(attrType, shape.opcintype)? {
        return Err(err(
            format!(
                "operator class \"{}\" does not accept data type {}",
                names.join("."),
                format_type::format_type_be(attrType)?
            ),
            ERRCODE_DATATYPE_MISMATCH,
        ));
    }
    Ok(opClassId)
}

// IndexSetParentIndex (indexcmds.c).
pub fn IndexSetParentIndex<'mcx>(
    mcx: Mcx<'mcx>,
    partitionIdx: &types_rel::Relation<'mcx>,
    parentOid: Oid,
) -> PgResult<()> {
    let partRelid = partitionIdx.rd_id;

    const InheritsRelationId: Oid = 2611;
    const InheritsRelidSeqnoIndexId: Oid = 2680;
    const F_INT4EQ: RegProcedure = 65;
    let pg_inherits_rel = table::table_open(mcx, InheritsRelationId, types_rel::RowExclusiveLock)?;
    let keys = [
        eq_key(1, F_OIDEQ, Datum::from_oid(partRelid)),
        eq_key(3, F_INT4EQ, Datum::from_i32(1)),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &pg_inherits_rel,
        InheritsRelidSeqnoIndexId,
        true,
        None,
        &keys,
    )?;
    let fix_dependencies = match genam::systable_getnext(mcx, &mut scan)? {
        None => parentOid != InvalidOid,
        Some(tup) if parentOid == InvalidOid => {
            let tid = tup.t_self;
            catalog_indexing::CatalogTupleDelete(&pg_inherits_rel, &tid)?;
            true
        }
        Some(tup) => {
            let mut isnull = false;
            // SAFETY: inhparent (2) is a fixed NOT NULL pg_inherits column.
            let inhparent = unsafe {
                types_tuple::heap_getattr(tup, 2, pg_inherits_rel.descr(), &mut isnull)
            }
            .as_oid();
            if inhparent != parentOid {
                panic!("bogus pg_inherit row: inhrelid {partRelid} inhparent {inhparent}");
            }
            false
        }
    };
    genam::systable_endscan(mcx, scan)?;
    pg_inherits_rel.close(types_rel::RowExclusiveLock)?;

    if fix_dependencies && parentOid != InvalidOid {
        pg_inherits::StoreSingleInheritance(mcx, partRelid, parentOid, 1)?;
    }

    if parentOid != InvalidOid {
        lmgr::LockRelationOid(parentOid, types_rel::ShareUpdateExclusiveLock)?;
        tablecmds::SetRelationHasSubclass(mcx, parentOid, true)?;
    }

    update_relispartition(mcx, partRelid, parentOid != InvalidOid)?;

    if fix_dependencies {
        if parentOid != InvalidOid {
            let partIdx = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, partRelid);
            let parentIdx = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, parentOid);
            let partitionTbl = pg_depend::ObjectAddress::set(
                RELATION_RELATION_ID,
                partitionIdx.rd_index.as_ref().expect("rd_index").indrelid,
            );
            pg_depend::recordDependencyOn(
                mcx,
                &partIdx,
                &parentIdx,
                pg_depend::DependencyType::PartitionPri,
            )?;
            pg_depend::recordDependencyOn(
                mcx,
                &partIdx,
                &partitionTbl,
                pg_depend::DependencyType::PartitionSec,
            )?;
        } else {
            pg_depend::deleteDependencyRecordsForClass(
                mcx,
                RELATION_RELATION_ID,
                partRelid,
                RELATION_RELATION_ID,
                pg_depend::DependencyType::PartitionPri,
            )?;
            pg_depend::deleteDependencyRecordsForClass(
                mcx,
                RELATION_RELATION_ID,
                partRelid,
                RELATION_RELATION_ID,
                pg_depend::DependencyType::PartitionSec,
            )?;
        }
        xact::CommandCounterIncrement()?;
    }
    Ok(())
}

fn update_relispartition<'mcx>(mcx: Mcx<'mcx>, relationId: Oid, newval: bool) -> PgResult<()> {
    const Anum_pg_class_relispartition: usize = 28;
    const ClassOidIndexId: Oid = 2662;
    let class_rel = table::table_open(mcx, RELATION_RELATION_ID, types_rel::RowExclusiveLock)?;
    let keys = [eq_key(1, F_OIDEQ, Datum::from_oid(relationId))];
    let mut scan = genam::systable_beginscan(
        mcx,
        &class_rel,
        ClassOidIndexId,
        true,
        None,
        &keys,
    )?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {relationId}"));
    // C: SearchSysCacheLockedCopy1 (indexcmds.c:4582) / UnlockTuple (:4589).
    // Ahead of the relispartition read below, which is C's Assert input.
    let otid = tup.t_self;
    lmgr::LockTuple(&class_rel, &otid, InplaceUpdateTupleLock)?;
    {
        let mut isnull = false;
        // SAFETY: relispartition is a fixed NOT NULL pg_class column.
        let cur = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_class_relispartition as i32, class_rel.descr(), &mut isnull)
        }
        .as_bool();
        assert!(cur != newval, "update_relispartition: no-op write for relation {relationId}");
    }
    let desc = class_rel.descr();
    let natts = desc.natts as usize;
    let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut replace: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, Datum::null());
    isnull.resize(natts, false);
    replace.resize(natts, false);
    values[Anum_pg_class_relispartition - 1] = Datum::from_bool(newval);
    replace[Anum_pg_class_relispartition - 1] = true;
    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &isnull, &replace)?;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &class_rel, &otid, &mut newtup)?;
    lmgr::UnlockTuple(&class_rel, &otid, InplaceUpdateTupleLock)?;
    class_rel.close(types_rel::RowExclusiveLock)
}

// DefineIndex's invalidate_parent arm: flip pg_index.indisvalid off in place.
fn set_pg_index_invalid<'mcx>(mcx: Mcx<'mcx>, indexRelationId: Oid) -> PgResult<()> {
    const IndexRelationId: Oid = 2610;
    const IndexRelidIndexId: Oid = 2679;
    const Anum_pg_index_indisvalid: usize = 11;
    let pg_index = table::table_open(mcx, IndexRelationId, types_rel::RowExclusiveLock)?;
    let keys = [eq_key(1, F_OIDEQ, Datum::from_oid(indexRelationId))];
    let mut scan =
        genam::systable_beginscan(mcx, &pg_index, IndexRelidIndexId, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for index {indexRelationId}"));
    let desc = pg_index.descr();
    let natts = desc.natts as usize;
    let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut replace: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, Datum::null());
    isnull.resize(natts, false);
    replace.resize(natts, false);
    values[Anum_pg_index_indisvalid - 1] = Datum::from_bool(false);
    replace[Anum_pg_index_indisvalid - 1] = true;
    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &isnull, &replace)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pg_index, &otid, &mut newtup)?;
    pg_index.close(types_rel::RowExclusiveLock)
}

#[allow(clippy::too_many_arguments)]
fn ComputeIndexAttrs<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    indexInfo: &mut IndexInfo<'mcx>,
    typeIds: &mut [Oid],
    collationIds: &mut [Oid],
    opclassIds: &mut [Oid],
    opclassOptions: &mut [Datum],
    coloptions: &mut [i16],
    attList: &types_nodes::NodeList<'mcx>,
    exclusionOpNames: &types_nodes::NodeList<'mcx>,
    isconstraint: bool,
    iswithoutoverlaps: bool,
    accessMethodId: Oid,
    amname: &str,
    amcanorder: bool,
    mut ddl_save_nestlevel: Option<&mut i32>,
) -> PgResult<()> {
    let nkeycols = indexInfo.ii_NumIndexKeyAttrs as usize;
    debug_assert!(exclusionOpNames.is_nil() || exclusionOpNames.len() == nkeycols);
    let mut excl_iter = exclusionOpNames.iter();
    for (attn, node) in attList.iter().enumerate() {
        let attribute = node
            .as_variant::<IndexElem>()
            .unwrap_or_else(|| panic!("IndexElem expected in indexParams"));
        let (atttype, attcollation) = if let Some(name) = attribute.name {
            let desc = rel.descr();
            let mut found = None;
            for i in 0..desc.natts as usize {
                let att = desc.attr(i);
                if !att.attisdropped && att.attname.name_str() == name.as_bytes() {
                    found = Some(*att);
                    break;
                }
            }
            // C SearchSysCacheAttName resolves system columns to negative
            // attnums; DefineIndex then rejects them with 0A000.
            if found.is_none() {
                if let Some(sysatt) = catalog_heap::SystemAttributeByName(name) {
                    found = Some(*sysatt);
                }
            }
            let Some(attform) = found else {
                let msg = if isconstraint {
                    format!("column \"{name}\" named in key does not exist")
                } else {
                    format!("column \"{name}\" does not exist")
                };
                return Err(err(msg, ERRCODE_UNDEFINED_COLUMN));
            };
            indexInfo.ii_IndexAttrNumbers[attn] = attform.attnum;
            (attform.atttypid, attform.attcollation)
        } else {
            // Expression column.
            if attn >= nkeycols {
                return Err(err(
                    "expressions are not supported in included columns".into(),
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                ));
            }
            let mut expr = attribute.expr.expect("IndexElem without name or expr");
            let atttype = nodes_core::expr_type(expr);
            let attcollation = nodes_core::expr_collation(expr);
            // Strip any top-level COLLATE clause, so "x COLLATE y" and
            // "(x COLLATE y)" are treated alike (indexcmds.c:1985).
            while let Some(c) = expr.as_collate_expr() {
                expr = c.arg;
            }
            if let Some(var) = expr.as_var() {
                if var.varattno != 0 {
                    indexInfo.ii_IndexAttrNumbers[attn] = var.varattno;
                } else {
                    push_index_expression(mcx, indexInfo, attn, expr)?;
                }
            } else {
                push_index_expression(mcx, indexInfo, attn, expr)?;
            }
            (atttype, attcollation)
        };
        typeIds[attn] = atttype;
        // Included columns have no collation, no opclass and no ordering
        // options (indexcmds.c:2029-2058).
        if attn >= nkeycols {
            let unsupported = if !attribute.collation.is_nil() {
                Some("a collation")
            } else if !attribute.opclass.is_nil() {
                Some("an operator class")
            } else if attribute.ordering != SortByDir::SORTBY_DEFAULT {
                Some("ASC/DESC options")
            } else if attribute.nulls_ordering != SortByNulls::SORTBY_NULLS_DEFAULT {
                Some("NULLS FIRST/LAST options")
            } else {
                None
            };
            if let Some(what) = unsupported {
                return Err(err(
                    format!("including column does not support {what}"),
                    ERRCODE_INVALID_OBJECT_DEFINITION,
                ));
            }
            opclassIds[attn] = InvalidOid;
            coloptions[attn] = 0;
            collationIds[attn] = InvalidOid;
            continue;
        }
        let mut attcollation = attcollation;
        // COLLATE clause overrides either leg's collation (indexcmds.c:2050-2062,
        // resolved before the collatable check).
        if !attribute.collation.is_nil() {
            if let Some(lvl) = ddl_save_nestlevel.as_deref_mut() {
                guc::AtEOXact_GUC(false, *lvl);
            }
            let resolved = catalog_namespace::get_collation_oid_list(&attribute.collation, false);
            if let Some(lvl) = ddl_save_nestlevel.as_deref_mut() {
                *lvl = guc::NewGUCNestLevel();
                guc::RestrictSearchPath()?;
            }
            attcollation = resolved?;
        }

        if lsyscache::type_is_collatable(atttype)? {
            if attcollation == InvalidOid {
                return Err(Box::new(
                    (*err(
                        "could not determine which collation to use for index expression".into(),
                        ERRCODE_INDETERMINATE_COLLATION,
                    ))
                    .with_hint("Use the COLLATE clause to set the collation explicitly."),
                ));
            }
        } else if attcollation != InvalidOid {
            return Err(err(
                format!(
                    "collations are not supported by type {}",
                    format_type::format_type_be(atttype)?
                ),
                ERRCODE_DATATYPE_MISMATCH,
            ));
        }
        collationIds[attn] = attcollation;

        // Opclass (and collation above) resolve under the DDL owner's original
        // search path: the RestrictSearchPath nest level pops around the
        // lookup (indexcmds.c ComputeIndexAttrs, ddl_save_nestlevel dance).
        if let Some(lvl) = ddl_save_nestlevel.as_deref_mut() {
            guc::AtEOXact_GUC(false, *lvl);
        }
        let resolved = if !attribute.opclass.is_nil() {
            ResolveOpClass(&attribute.opclass, atttype, amname, accessMethodId)
        } else {
            GetDefaultOpClass(atttype, accessMethodId)
        };
        if let Some(lvl) = ddl_save_nestlevel.as_deref_mut() {
            *lvl = guc::NewGUCNestLevel();
            guc::RestrictSearchPath()?;
        }
        opclassIds[attn] = resolved?;
        if attribute.opclass.is_nil() {
            if opclassIds[attn] == InvalidOid {
                return Err(Box::new(
                    (*err(
                        format!(
                            "data type {} has no default operator class for access method \"{amname}\"",
                            format_type::format_type_be(atttype)?
                        ),
                        ERRCODE_UNDEFINED_OBJECT,
                    ))
                    .with_hint(
                        "You must specify an operator class for the index or define a \
                         default operator class for the data type.",
                    ),
                ));
            }
        }

        if let Some(opnode) = excl_iter.next() {
            let opname = opnode.as_list().expect("exclusion op name list");
            let pstate = parser_small1::make_parsestate(mcx, None);
            let opid = parse_oper::compatible_oper_opid(&pstate, opname, atttype, atttype, false)?;
            if lsyscache::get_commutator(opid)? != opid {
                return Err(Box::new(
                    (*err(
                        format!(
                            "operator {} is not commutative",
                            regproc::format_operator(mcx, opid)?
                        ),
                        ERRCODE_WRONG_OBJECT_TYPE,
                    ))
                    .with_detail(
                        "Only commutative operators can be used in exclusion constraints."
                            .to_string(),
                    ),
                ));
            }
            let opfamily = lsyscache::get_opclass_family(opclassIds[attn])?;
            let strat = lsyscache::get_op_opfamily_strategy(opid, opfamily)?;
            if strat == 0 {
                let famname = lsyscache::get_opfamily_name(mcx, opfamily, false)?
                    .expect("opfamily name");
                return Err(Box::new(
                    (*err(
                        format!(
                            "operator {} is not a member of operator family \"{}\"",
                            regproc::format_operator(mcx, opid)?,
                            famname.as_str()
                        ),
                        ERRCODE_WRONG_OBJECT_TYPE,
                    ))
                    .with_detail(
                        "The exclusion operator must be related to the index operator class \
                         for the constraint."
                            .to_string(),
                    ),
                ));
            }
            indexInfo.ii_ExclusionOps[attn] = opid;
            indexInfo.ii_ExclusionProcs[attn] = lsyscache::get_opcode(opid)?;
            indexInfo.ii_ExclusionStrats[attn] = strat as u16;
        } else if iswithoutoverlaps {
            // Last key column takes the overlaps operator; the rest equality.
            let cmptype = if attn == indexInfo.ii_NumIndexKeyAttrs as usize - 1 {
                lsyscache::COMPARE_OVERLAP
            } else {
                lsyscache::COMPARE_EQ
            };
            let (opid, strat) =
                GetOperatorFromCompareType(mcx, opclassIds[attn], InvalidOid, cmptype)?;
            indexInfo.ii_ExclusionOps[attn] = opid;
            indexInfo.ii_ExclusionProcs[attn] = lsyscache::get_opcode(opid)?;
            indexInfo.ii_ExclusionStrats[attn] = strat;
        }

        coloptions[attn] = 0;
        if amcanorder {
            if attribute.ordering == SortByDir::SORTBY_DESC {
                coloptions[attn] |= INDOPTION_DESC;
            }
            match attribute.nulls_ordering {
                SortByNulls::SORTBY_NULLS_DEFAULT => {
                    if attribute.ordering == SortByDir::SORTBY_DESC {
                        coloptions[attn] |= INDOPTION_NULLS_FIRST;
                    }
                }
                SortByNulls::SORTBY_NULLS_FIRST => coloptions[attn] |= INDOPTION_NULLS_FIRST,
                SortByNulls::SORTBY_NULLS_LAST => {}
            }
        } else {
            if attribute.ordering != SortByDir::SORTBY_DEFAULT {
                return Err(err(
                    format!("access method \"{amname}\" does not support ASC/DESC options"),
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                ));
            }
            if attribute.nulls_ordering != SortByNulls::SORTBY_NULLS_DEFAULT {
                return Err(err(
                    format!(
                        "access method \"{amname}\" does not support NULLS FIRST/LAST options"
                    ),
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                ));
            }
        }

        // Per-column opclass options, attoptions field (indexcmds.c:2237-2247).
        opclassOptions[attn] = if !attribute.opclassopts.is_nil() {
            let opts = reloptions::transformRelOptions(
                mcx,
                None,
                &attribute.opclassopts,
                None,
                &[],
                false,
                false,
            )?
            .expect("transformRelOptions: non-nil opclassopts");
            Datum::from_usize(opts.leak().as_ptr() as usize)
        } else {
            Datum::null()
        };
    }
    Ok(())
}

fn push_index_expression<'mcx>(
    mcx: Mcx<'mcx>,
    indexInfo: &mut IndexInfo<'mcx>,
    attn: usize,
    expr: types_nodes::Node<'mcx>,
) -> PgResult<()> {
    indexInfo.ii_IndexAttrNumbers[attn] = 0;
    indexInfo.ii_Expressions.lappend(mcx, expr)?;
    if clauses::contain_mutable_functions_after_planning(mcx, expr)? {
        return Err(err(
            "functions in index expression must be marked IMMUTABLE".into(),
            ERRCODE_INVALID_OBJECT_DEFINITION,
        ));
    }
    Ok(())
}

// CheckPredicate (indexcmds.c).
fn CheckPredicate<'mcx>(mcx: Mcx<'mcx>, predicate: types_nodes::Node<'mcx>) -> PgResult<()> {
    if clauses::contain_mutable_functions_after_planning(mcx, predicate)? {
        return Err(err(
            "functions in index predicate must be marked IMMUTABLE".into(),
            ERRCODE_INVALID_OBJECT_DEFINITION,
        ));
    }
    Ok(())
}

// ChooseIndexName, non-constraint arm (pkey/key/excl labels ride the
// constraint lane).
fn ChooseIndexName<'mcx>(
    mcx: Mcx<'mcx>,
    tabname: &str,
    namespaceId: Oid,
    colnames: &[PgString<'mcx>],
) -> PgResult<PgString<'mcx>> {
    let addition = ChooseIndexNameAddition(mcx, colnames)?;
    ChooseRelationName(mcx, tabname, Some(addition.as_str()), "idx", namespaceId, false)
}

fn ChooseIndexNameAddition<'mcx>(
    mcx: Mcx<'mcx>,
    colnames: &[PgString<'mcx>],
) -> PgResult<PgString<'mcx>> {
    let mut buf = PgString::new_in(mcx);
    for name in colnames {
        if !buf.is_empty() {
            buf.try_push_str("_")?;
        }
        buf.try_push_str(name.as_str())?;
        if buf.len() >= NAMEDATALEN as usize {
            unported("ChooseIndexNameAddition: name truncation");
        }
    }
    Ok(buf)
}

fn ChooseIndexColumnNames<'mcx>(
    mcx: Mcx<'mcx>,
    indexElems: &types_nodes::NodeList<'mcx>,
) -> PgResult<PgVec<'mcx, PgString<'mcx>>> {
    let mut result: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
    for node in indexElems.iter() {
        let ielem = node.as_variant::<IndexElem>().expect("IndexElem");
        let origname = ielem
            .indexcolname
            .or(ielem.name)
            .unwrap_or("expr");
        let mut curname = PgString::from_str_in(origname, mcx)?;
        let mut i = 1;
        while result.iter().any(|n| n.as_str() == curname.as_str()) {
            if origname.len() + 10 >= NAMEDATALEN as usize {
                unported("ChooseIndexColumnNames: mbcliplen truncation");
            }
            curname = PgString::from_str_in(origname, mcx)?;
            use core::fmt::Write;
            write!(curname, "{i}").expect("suffix");
            i += 1;
        }
        result.push(curname);
    }
    Ok(result)
}

// ChooseRelationName (indexcmds.c:2578). The pg_class probe runs under a DIRTY
// snapshot (indexcmds.c:2613/2617/2643), deliberately, so it can see another
// backend's *uncommitted* claim on the generated name and skip to the next
// suffix. Under an MVCC snapshot the collision is invisible and the loser of the
// race aborts on pg_class_relname_nsp_index instead of taking `foo_i_idx1`.
// C's header comment (indexcmds.c:2593-2601) is explicit that this only narrows
// the window rather than closing it, and that a command choosing several names
// must CommandCounterIncrement between them.
// Note ConstraintNameExists stays MVCC: C passes a NULL snapshot there too
// (pg_constraint.c), so that half is already correct.
pub(crate) fn ChooseRelationName<'mcx>(
    mcx: Mcx<'mcx>,
    name1: &str,
    name2: Option<&str>,
    label: &str,
    namespaceid: Oid,
    isconstraint: bool,
) -> PgResult<PgString<'mcx>> {
    let pgclassrel = table::table_open(mcx, RELATION_RELATION_ID, types_rel::AccessShareLock)?;
    // indexcmds.c:2617 InitDirtySnapshot, held across the whole retry loop as
    // C's stack SnapshotDirty is.
    let dirty = std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
        mcx,
        ::types_snapshot::SnapshotType::SNAPSHOT_DIRTY,
    ));
    let mut pass = 0;
    let mut modlabel = PgString::from_str_in(label, mcx)?;
    let relname = loop {
        let relname = make_object_name(mcx, name1, name2, modlabel.as_str())?;
        let cname = name_arg(mcx, relname.as_str())?;
        let keys = [
            eq_key(Anum_pg_class_relname, F_NAMEEQ, Datum::from_usize(cname.as_ptr() as usize)),
            eq_key(Anum_pg_class_relnamespace, F_OIDEQ, Datum::from_oid(namespaceid)),
        ];
        let mut scan = genam::systable_beginscan(
            mcx,
            &pgclassrel,
            ClassNameNspIndexId,
            true,
            Some(dirty.clone()),
            &keys,
        )?;
        let mut collides = genam::systable_getnext(mcx, &mut scan)?.is_some();
        genam::systable_endscan(mcx, scan)?;
        if !collides && isconstraint {
            collides = constraint_name_exists(mcx, relname.as_str(), namespaceid)?;
        }
        if !collides {
            break relname;
        }
        pass += 1;
        modlabel = PgString::from_str_in(label, mcx)?;
        use core::fmt::Write;
        write!(modlabel, "{pass}").expect("label suffix");
    };
    pgclassrel.close(types_rel::AccessShareLock)?;
    Ok(relname)
}

// ConstraintNameExists (pg_constraint.c).
fn constraint_name_exists(mcx: Mcx<'_>, name: &str, namespaceid: Oid) -> PgResult<bool> {
    let conrel = table::table_open(
        mcx,
        types_core::CONSTRAINT_RELATION_ID,
        types_rel::AccessShareLock,
    )?;
    let cname = name_arg(mcx, name)?;
    let keys = [
        eq_key(2, F_NAMEEQ, Datum::from_usize(cname.as_ptr() as usize)),
        eq_key(3, F_OIDEQ, Datum::from_oid(namespaceid)),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &conrel,
        types_core::CONSTRAINT_NAME_NSP_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let found = genam::systable_getnext(mcx, &mut scan)?.is_some();
    genam::systable_endscan(mcx, scan)?;
    conrel.close(types_rel::AccessShareLock)?;
    Ok(found)
}

// makeObjectName (indexcmds.c:2518-2577): truncate the longer of name1/name2
// (multibyte-aware) until "name1[_name2]_label" fits in NAMEDATALEN-1 bytes.
fn make_object_name<'mcx>(
    mcx: Mcx<'mcx>,
    name1: &str,
    name2: Option<&str>,
    label: &str,
) -> PgResult<PgString<'mcx>> {
    let mut overhead = label.len() + 1;
    if name2.is_some() {
        overhead += 1;
    }
    assert!(NAMEDATALEN as usize - 1 > overhead, "makeObjectName label too long ({label:?})");
    let availchars = NAMEDATALEN as usize - 1 - overhead;
    let mut name1chars = name1.len();
    let mut name2chars = name2.map_or(0, str::len);
    while name1chars + name2chars > availchars {
        if name1chars > name2chars {
            name1chars -= 1;
        } else {
            name2chars -= 1;
        }
    }
    name1chars = mbutils_seams::pg_mbcliplen::call(
        name1.as_bytes(),
        name1chars as i32,
        name1chars as i32,
    ) as usize;
    let mut s = PgString::from_str_in(&name1[..name1chars], mcx)?;
    if let Some(n2) = name2 {
        name2chars = mbutils_seams::pg_mbcliplen::call(
            n2.as_bytes(),
            name2chars as i32,
            name2chars as i32,
        ) as usize;
        s.try_push_str("_")?;
        s.try_push_str(&n2[..name2chars])?;
    }
    s.try_push_str("_")?;
    s.try_push_str(label)?;
    Ok(s)
}

fn name_arg<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<PgVec<'mcx, u8>> {
    let n = NAMEDATALEN as usize;
    assert!(name.len() < n, "makeObjectName truncation unported: {name:?}");
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, n)?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..n - name.len()])?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use types_relscan::IndexAmKind::*;

    // Re-homed from parse_utilcmd, which carried a second, weaker copy of
    // makeObjectName (a char-boundary clip closure rather than pg_mbcliplen).
    // C has one makeObjectName (indexcmds.c:2517); so do we now, and this is it.
    #[test]
    fn make_object_name_matches_c() {
        // pg_mbcliplen is a seam and unit tests install no seams; the stub is
        // the same UTF-8 boundary clip pg_constraint's truncation_tests uses.
        static SETUP: std::sync::Once = std::sync::Once::new();
        SETUP.call_once(|| {
            ::mbutils_seams::pg_mbcliplen::set(|s, len, limit| {
                let mut l = (limit as usize).min(len as usize);
                while l > 0 && l < s.len() && s[l] & 0xC0 == 0x80 {
                    l -= 1;
                }
                l as i32
            });
        });
        let cx = Box::leak(Box::new(::mcx::MemoryContext::new("indexcmds-objname-test")));
        let mcx = cx.mcx();
        assert_eq!(
            super::make_object_name(mcx, "st", Some("id"), "seq").unwrap().as_str(),
            "st_id_seq"
        );
        let long_a = "a".repeat(60);
        let long_b = "b".repeat(60);
        let n = super::make_object_name(mcx, &long_a, Some(&long_b), "seq").unwrap();
        assert_eq!(n.len(), ::types_core::NAMEDATALEN as usize - 1);
        assert_eq!(n.as_str(), format!("{}_{}_seq", "a".repeat(29), "b".repeat(29)));
    }

    // Each AM handler's IndexAmRoutine flags (PG18): amcaninclude true for
    // btree/gist/spgist; amcanmulticol false for hash/spgist.
    #[test]
    fn index_am_flags_match_handlers() {
        for (kind, caninclude) in [
            (Btree, true),
            (Hash, false),
            (Gin, false),
            (Gist, true),
            (Spgist, true),
            (Brin, false),
        ] {
            assert_eq!(super::index_am_flags(kind).3, caninclude, "{kind:?}");
        }
        for (kind, canmulticol) in [
            (Btree, true),
            (Hash, false),
            (Gin, true),
            (Gist, true),
            (Spgist, false),
            (Brin, true),
        ] {
            assert_eq!(super::index_am_flags(kind).2, canmulticol, "{kind:?}");
        }
    }
}

fn eq_key(attno: AttrNumber, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}
