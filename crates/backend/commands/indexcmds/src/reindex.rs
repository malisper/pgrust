// ExecReindex/ReindexIndex/ReindexTable + ReindexMultipleTables/Partitions/
// MultipleInternal + ReindexRelationConcurrently (indexcmds.c).
use catalog_index::{
    reindex_index, reindex_relation, ReindexParams, REINDEXOPT_CONCURRENTLY,
    REINDEXOPT_MISSING_OK, REINDEXOPT_REPORT_PROGRESS, REINDEXOPT_VERBOSE,
    REINDEX_REL_CHECK_CONSTRAINTS, REINDEX_REL_PROCESS_TOAST,
};
use mcx::Mcx;
use pg_depend::ObjectAddress;
use types_core::{InvalidOid, Oid, RELATION_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_SYNTAX_ERROR, ERRCODE_WRONG_OBJECT_TYPE, ERROR, WARNING,
};

// Every path in this file is reached only from ExecReindex, i.e. an actual
// REINDEX statement is always driving the call (unlike catalog_index's
// reindex_index/reindex_relation, which CLUSTER/VACUUM FULL/TRUNCATE also
// call with no statement). C's guard is `if (stmt)`; here it is unconditional.
fn collect_reindex_cb() -> impl FnMut(Oid) {
    let tag = cmdtag::GetCommandTagEnum(b"REINDEX");
    move |index_id: Oid| {
        event_trigger::EventTriggerCollectSimpleCommand(
            ObjectAddress::set(RELATION_RELATION_ID, index_id),
            ObjectAddress::set(InvalidOid, InvalidOid),
            tag,
        );
    }
}
use types_nodes::parsenodes::{DropBehavior, ReindexObjectType, ReindexStmt};
use types_rel::{
    AccessExclusiveLock, LockRelId, ShareLock, ShareUpdateExclusiveLock, RELKIND_INDEX,
    RELKIND_MATVIEW, RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELKIND_TOASTVALUE,
};
use types_storage::lock::LOCKTAG;

const RELPERSISTENCE_TEMP: i8 = b't' as i8;
const GLOBALTABLESPACE_OID: Oid = 1664;
const NamespaceRelationId: Oid = 2615;
const DatabaseRelationId: Oid = 1262;
const TableSpaceRelationId: Oid = 1213;
const ROLE_PG_MAINTAIN: Oid = 6337;

fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

pub fn ExecReindex<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ReindexStmt<'mcx>,
    is_top_level: bool,
) -> PgResult<()> {
    let mut concurrently = false;
    let mut verbose = false;
    let mut tablespacename: Option<&str> = None;
    for opt_node in stmt.params.iter() {
        let opt = opt_node.as_def_elem().expect("ReindexStmt option is DefElem");
        match opt.defname.unwrap_or("") {
            "verbose" => verbose = explain::defGetBoolean(opt)?,
            "concurrently" => concurrently = explain::defGetBoolean(opt)?,
            "tablespace" => tablespacename = Some(explain::defGetString(mcx, opt)?),
            name => {
                return Err(err(
                    format!("unrecognized REINDEX option \"{name}\""),
                    ERRCODE_SYNTAX_ERROR,
                ))
            }
        }
    }

    if concurrently {
        xact::PreventInTransactionBlock(is_top_level, "REINDEX CONCURRENTLY")?;
    }

    let mut params = ReindexParams {
        options: (if verbose { REINDEXOPT_VERBOSE } else { 0 })
            | (if concurrently { REINDEXOPT_CONCURRENTLY } else { 0 }),
        tablespace_oid: InvalidOid,
    };
    if let Some(name) = tablespacename {
        params.tablespace_oid = commands_tablespace::get_tablespace_oid(mcx, name, false)?;
        check_tablespace_create_acl(params.tablespace_oid, name)?;
    }

    match stmt.kind {
        ReindexObjectType::REINDEX_OBJECT_INDEX => ReindexIndex(mcx, stmt, &mut params, is_top_level),
        ReindexObjectType::REINDEX_OBJECT_TABLE => ReindexTable(mcx, stmt, &mut params, is_top_level),
        ReindexObjectType::REINDEX_OBJECT_SCHEMA
        | ReindexObjectType::REINDEX_OBJECT_SYSTEM
        | ReindexObjectType::REINDEX_OBJECT_DATABASE => {
            xact::PreventInTransactionBlock(
                is_top_level,
                match stmt.kind {
                    ReindexObjectType::REINDEX_OBJECT_SCHEMA => "REINDEX SCHEMA",
                    ReindexObjectType::REINDEX_OBJECT_SYSTEM => "REINDEX SYSTEM",
                    _ => "REINDEX DATABASE",
                },
            )?;
            ReindexMultipleTables(mcx, stmt, &params)
        }
    }
}

fn check_tablespace_create_acl(tablespace_oid: Oid, name: &str) -> PgResult<()> {
    if tablespace_oid != InvalidOid
        && tablespace_oid != init_small::globals::MyDatabaseTableSpace()
    {
        let aclresult = aclchk::object_aclcheck(
            TableSpaceRelationId,
            tablespace_oid,
            miscinit::GetUserId(),
            adt_acl::ACL_CREATE,
        )?;
        if aclresult != aclchk::ACLCHECK_OK {
            aclchk_seams::aclcheck_error::call(
                aclresult,
                types_nodes::parsenodes::ObjectType::OBJECT_TABLESPACE as i32,
                name,
            )?;
        }
    }
    Ok(())
}

fn stmt_range_var<'a, 'mcx>(stmt: &'a ReindexStmt<'mcx>) -> rel_vocab::RangeVar<'mcx> {
    let rv = stmt
        .relation
        .and_then(|n| n.as_range_var())
        .expect("ReindexStmt.relation is RangeVar");
    rel_vocab::RangeVar {
        catalogname: rv.catalogname,
        schemaname: rv.schemaname,
        relname: rv.relname.expect("RangeVar.relname"),
        inh: rv.inh,
        relpersistence: rv.relpersistence,
        location: rv.location,
    }
}

fn ReindexIndex<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ReindexStmt<'mcx>,
    params: &mut ReindexParams,
    is_top_level: bool,
) -> PgResult<()> {
    let rv = stmt_range_var(stmt);
    let concurrent = params.options & REINDEXOPT_CONCURRENTLY != 0;

    let mut locked_table_oid = InvalidOid;
    let mut cb = |rv2: &rel_vocab::RangeVar<'_>, rel_id: Oid, old_rel_id: Oid| -> PgResult<()> {
        RangeVarCallbackForReindexIndex(mcx, rv2, rel_id, old_rel_id, concurrent, &mut locked_table_oid)
    };
    let ind_oid = catalog_namespace::RangeVarGetRelidExtended(
        &rv,
        if concurrent { ShareUpdateExclusiveLock } else { AccessExclusiveLock },
        0,
        Some(&mut cb),
    )?;

    let persistence = lsyscache::get_rel_persistence(ind_oid)? as u8;
    let relkind = lsyscache::get_rel_relkind(ind_oid)? as u8;

    if relkind == RELKIND_PARTITIONED_INDEX {
        ReindexPartitions(mcx, ind_oid, params, is_top_level)
    } else if concurrent && persistence != RELPERSISTENCE_TEMP as u8 {
        ReindexRelationConcurrently(mcx, ind_oid, params)?;
        Ok(())
    } else {
        let mut newparams = *params;
        newparams.options |= REINDEXOPT_REPORT_PROGRESS;
        let mut collect = collect_reindex_cb();
        reindex_index(mcx, ind_oid, false, persistence, &newparams, Some(&mut collect))
    }
}

fn RangeVarCallbackForReindexIndex(
    _mcx: Mcx<'_>,
    relation: &rel_vocab::RangeVar<'_>,
    relId: Oid,
    oldRelId: Oid,
    concurrent: bool,
    locked_table_oid: &mut Oid,
) -> PgResult<()> {
    // Table lock level must match reindex_index / index_concurrently_*.
    let table_lockmode = if concurrent { ShareUpdateExclusiveLock } else { ShareLock };

    if relId != oldRelId && oldRelId != InvalidOid {
        lmgr::UnlockRelationOid(*locked_table_oid, table_lockmode)?;
        *locked_table_oid = InvalidOid;
    }
    if relId == InvalidOid {
        return Ok(());
    }
    let relkind = lsyscache::get_rel_relkind(relId)? as u8;
    if relkind == 0 {
        return Ok(());
    }
    if relkind != RELKIND_INDEX && relkind != RELKIND_PARTITIONED_INDEX {
        return Err(err(
            format!("\"{}\" is not an index", relation.relname),
            ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }

    let table_oid = catalog_index::IndexGetRelation(_mcx, relId, true)?;
    if table_oid != InvalidOid {
        let aclresult =
            aclchk::pg_class_aclcheck(table_oid, miscinit::GetUserId(), adt_acl::ACL_MAINTAIN)?;
        if aclresult != aclchk::ACLCHECK_OK {
            aclchk_seams::aclcheck_error::call(
                aclresult,
                types_nodes::parsenodes::ObjectType::OBJECT_INDEX as i32,
                relation.relname,
            )?;
        }
    }

    if relId != oldRelId && table_oid != InvalidOid {
        lmgr::LockRelationOid(table_oid, table_lockmode)?;
        *locked_table_oid = table_oid;
    }
    Ok(())
}

fn ReindexTable<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ReindexStmt<'mcx>,
    params: &mut ReindexParams,
    is_top_level: bool,
) -> PgResult<()> {
    let rv = stmt_range_var(stmt);
    let concurrent = params.options & REINDEXOPT_CONCURRENTLY != 0;

    let mut cb = |rv2: &rel_vocab::RangeVar<'_>, rel_id: Oid, old_rel_id: Oid| -> PgResult<()> {
        tablecmds::RangeVarCallbackMaintainsTable(rv2, rel_id, old_rel_id)
    };
    let heap_oid = catalog_namespace::RangeVarGetRelidExtended(
        &rv,
        if concurrent { ShareUpdateExclusiveLock } else { ShareLock },
        0,
        Some(&mut cb),
    )?;

    if lsyscache::get_rel_relkind(heap_oid)? as u8 == RELKIND_PARTITIONED_TABLE {
        return ReindexPartitions(mcx, heap_oid, params, is_top_level);
    }
    if concurrent && lsyscache::get_rel_persistence(heap_oid)? != RELPERSISTENCE_TEMP {
        let result = ReindexRelationConcurrently(mcx, heap_oid, params)?;
        if !result {
            elog::ereport(types_error::NOTICE)
                .errmsg(format!(
                    "table \"{}\" has no indexes that can be reindexed concurrently",
                    rv.relname
                ))
                .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "ReindexTable"))?;
        }
        return Ok(());
    }
    let mut newparams = *params;
    newparams.options |= REINDEXOPT_REPORT_PROGRESS;
    let mut collect = collect_reindex_cb();
    let result = reindex_relation(
        mcx,
        heap_oid,
        REINDEX_REL_PROCESS_TOAST | REINDEX_REL_CHECK_CONSTRAINTS,
        &newparams,
        &mut collect,
    )?;
    if !result {
        elog::ereport(types_error::NOTICE)
            .errmsg(format!("table \"{}\" has no indexes to reindex", rv.relname))
            .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "ReindexTable"))?;
    }
    Ok(())
}

const Anum_pg_class_oid: usize = 1;
const Anum_pg_class_relnamespace: usize = 3;
const Anum_pg_class_relfilenode: usize = 8;
const Anum_pg_class_relisshared: usize = 16;
const Anum_pg_class_relpersistence: usize = 17;
const Anum_pg_class_relkind: usize = 18;

// ReindexMultipleTables (indexcmds.c:3108).
fn ReindexMultipleTables<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ReindexStmt<'mcx>,
    params: &ReindexParams,
) -> PgResult<()> {
    let object_name = stmt.name;
    let object_kind = stmt.kind;
    let concurrent = params.options & REINDEXOPT_CONCURRENTLY != 0;

    if object_kind == ReindexObjectType::REINDEX_OBJECT_SYSTEM && concurrent {
        return Err(err(
            "cannot reindex system catalogs concurrently".to_string(),
            ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }

    let object_oid;
    if object_kind == ReindexObjectType::REINDEX_OBJECT_SCHEMA {
        let name = object_name.expect("REINDEX SCHEMA requires a name");
        object_oid = catalog_namespace::get_namespace_oid(name, false)?;
        if !aclchk::object_ownercheck(NamespaceRelationId, object_oid, miscinit::GetUserId())?
            && !adt_acl::has_privs_of_role(miscinit::GetUserId(), ROLE_PG_MAINTAIN)?
        {
            aclchk_seams::aclcheck_error::call(
                aclchk::ACLCHECK_NOT_OWNER,
                types_nodes::parsenodes::ObjectType::OBJECT_SCHEMA as i32,
                name,
            )?;
        }
    } else {
        object_oid = init_small::globals::MyDatabaseId();
        let dbname = dbcommands_seams::get_database_name::call(object_oid)?
            .unwrap_or_default();
        if let Some(name) = object_name {
            if name != dbname {
                return Err(err(
                    "can only reindex the currently open database".to_string(),
                    ERRCODE_FEATURE_NOT_SUPPORTED,
                ));
            }
        }
        if !aclchk::object_ownercheck(DatabaseRelationId, object_oid, miscinit::GetUserId())?
            && !adt_acl::has_privs_of_role(miscinit::GetUserId(), ROLE_PG_MAINTAIN)?
        {
            aclchk_seams::aclcheck_error::call(
                aclchk::ACLCHECK_NOT_OWNER,
                types_nodes::parsenodes::ObjectType::OBJECT_DATABASE as i32,
                &dbname,
            )?;
        }
    }

    let mut concurrent_warning = false;
    let mut tablespace_warning = false;
    let mut relids: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);

    {
        let pg_class = table::table_open(mcx, types_core::RELATION_RELATION_ID, types_rel::AccessShareLock)?;
        let key;
        let keys: &[types_scan::scankey::ScanKeyData] =
            if object_kind == ReindexObjectType::REINDEX_OBJECT_SCHEMA {
                key = [oid_key_at(Anum_pg_class_relnamespace, object_oid)];
                &key
            } else {
                &[]
            };
        let mut scan = genam::systable_beginscan(mcx, &pg_class, InvalidOid, false, None, keys)?;
        let desc = pg_class.descr();
        while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
            let get = |anum: usize| {
                let mut isnull = false;
                // SAFETY: fixed NOT NULL pg_class columns under its descriptor.
                unsafe { types_tuple::heap_getattr(tup, anum as i32, desc, &mut isnull) }
            };
            let relid = get(Anum_pg_class_oid).as_oid();
            let relkind = get(Anum_pg_class_relkind).as_i8() as u8;
            let relpersistence = get(Anum_pg_class_relpersistence).as_i8();
            let relisshared = get(Anum_pg_class_relisshared).as_bool();
            let relnamespace = get(Anum_pg_class_relnamespace).as_oid();

            if relkind != RELKIND_RELATION && relkind != RELKIND_MATVIEW {
                continue;
            }
            if relpersistence == RELPERSISTENCE_TEMP
                && !catalog_namespace::isTempNamespace(relnamespace)
            {
                continue;
            }
            if object_kind == ReindexObjectType::REINDEX_OBJECT_SYSTEM
                && !catalog::IsCatalogRelationOid(relid)
            {
                continue;
            }
            if object_kind == ReindexObjectType::REINDEX_OBJECT_DATABASE
                && catalog::IsCatalogRelationOid(relid)
            {
                continue;
            }
            if relisshared
                && aclchk::pg_class_aclcheck(relid, miscinit::GetUserId(), adt_acl::ACL_MAINTAIN)?
                    != aclchk::ACLCHECK_OK
            {
                continue;
            }
            if concurrent && catalog::IsCatalogRelationOid(relid) {
                if !concurrent_warning {
                    elog_seams::ereport::call(
                        PgError::new(
                            WARNING,
                            "cannot reindex system catalogs concurrently, skipping all"
                                .to_string(),
                        )
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                    )?;
                }
                concurrent_warning = true;
                continue;
            }
            if params.tablespace_oid != InvalidOid {
                let relfilenode = get(Anum_pg_class_relfilenode).as_oid();
                let mapped = types_rel::RELKIND_HAS_STORAGE(relkind) && relfilenode == InvalidOid;
                let system = catalog::IsCatalogRelationOid(relid)
                    || catalog::IsToastNamespace(relnamespace);
                if mapped || system {
                    if !tablespace_warning {
                        elog_seams::ereport::call(
                            PgError::new(
                                WARNING,
                                "cannot move system relations, skipping all".to_string(),
                            )
                            .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
                        )?;
                    }
                    tablespace_warning = true;
                    continue;
                }
            }

            relids.push(relid);
        }
        genam::systable_endscan(mcx, scan)?;
        pg_class.close(types_rel::AccessShareLock)?;
    }

    // pg_class first so its own indexes are sane before anything else.
    if let Some(pos) = relids.iter().position(|&r| r == types_core::RELATION_RELATION_ID) {
        relids[..=pos].rotate_right(1);
    }

    ReindexMultipleInternal(mcx, &relids, params)
}

fn oid_key_at(attno: usize, oid: Oid) -> types_scan::scankey::ScanKeyData {
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = attno as types_core::AttrNumber;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = datum::Datum::from_oid(oid);
    key
}

// ReindexPartitions (indexcmds.c:3348); C's error-context callback covers only
// PreventInTransactionBlock (pushed then popped around it), so the CONTEXT
// line is attached to that error alone.
fn ReindexPartitions<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    params: &ReindexParams,
    is_top_level: bool,
) -> PgResult<()> {
    let relkind = lsyscache::get_rel_relkind(relid)? as u8;

    xact::PreventInTransactionBlock(
        is_top_level,
        if relkind == RELKIND_PARTITIONED_TABLE { "REINDEX TABLE" } else { "REINDEX INDEX" },
    )
    .map_err(|mut e| -> Box<types_error::PgError> {
        let ns = lsyscache::get_namespace_name(mcx, lsyscache::get_rel_namespace(relid).unwrap_or(InvalidOid))
            .ok()
            .flatten()
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        let name = lsyscache::get_rel_name(mcx, relid)
            .ok()
            .flatten()
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        e.add_context_line(format!(
            "while reindexing partitioned {} \"{ns}.{name}\"",
            if relkind == RELKIND_PARTITIONED_TABLE { "table" } else { "index" }
        ));
        e
    })?;

    let inhoids = pg_inherits::find_all_inheritors(mcx, relid, ShareLock)?;
    let mut partitions: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    for &partoid in inhoids.iter() {
        let partkind = lsyscache::get_rel_relkind(partoid)? as u8;
        if !types_rel::RELKIND_HAS_STORAGE(partkind) {
            continue;
        }
        debug_assert!(partkind == RELKIND_INDEX || partkind == RELKIND_RELATION);
        partitions.push(partoid);
    }

    ReindexMultipleInternal(mcx, &partitions, params)
}

// ReindexMultipleInternal (indexcmds.c:3442): one transaction per relation.
fn ReindexMultipleInternal<'mcx>(
    mcx: Mcx<'mcx>,
    relids: &[Oid],
    params: &ReindexParams,
) -> PgResult<()> {
    if snapmgr::ActiveSnapshotSet() {
        snapmgr::PopActiveSnapshot()?;
    }
    xact::CommitTransactionCommand()?;

    for &relid in relids {
        xact::StartTransactionCommand()?;
        let snap = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snap)?;

        if lsyscache::get_rel_relkind(relid).map(|k| k as u8).unwrap_or(0) == 0 {
            snapmgr::PopActiveSnapshot()?;
            xact::CommitTransactionCommand()?;
            continue;
        }

        if params.tablespace_oid != InvalidOid
            && params.tablespace_oid != init_small::globals::MyDatabaseTableSpace()
        {
            let aclresult = aclchk::object_aclcheck(
                TableSpaceRelationId,
                params.tablespace_oid,
                miscinit::GetUserId(),
                adt_acl::ACL_CREATE,
            )?;
            if aclresult != aclchk::ACLCHECK_OK {
                let name = commands_tablespace::get_tablespace_name(mcx, params.tablespace_oid)?;
                aclchk_seams::aclcheck_error::call(
                    aclresult,
                    types_nodes::parsenodes::ObjectType::OBJECT_TABLESPACE as i32,
                    name.as_ref()
                        .map(|n| std::str::from_utf8(n.name_str()).unwrap_or(""))
                        .unwrap_or(""),
                )?;
            }
        }

        let relkind = lsyscache::get_rel_relkind(relid)? as u8;
        let relpersistence = lsyscache::get_rel_persistence(relid)?;
        debug_assert!(relkind != RELKIND_PARTITIONED_INDEX && relkind != RELKIND_PARTITIONED_TABLE);

        if params.options & REINDEXOPT_CONCURRENTLY != 0
            && relpersistence != RELPERSISTENCE_TEMP
        {
            let mut newparams = *params;
            newparams.options |= REINDEXOPT_MISSING_OK;
            ReindexRelationConcurrently(mcx, relid, &newparams)?;
            if snapmgr::ActiveSnapshotSet() {
                snapmgr::PopActiveSnapshot()?;
            }
        } else if relkind == RELKIND_INDEX {
            let mut newparams = *params;
            newparams.options |= REINDEXOPT_REPORT_PROGRESS | REINDEXOPT_MISSING_OK;
            let mut collect = collect_reindex_cb();
            reindex_index(mcx, relid, false, relpersistence as u8, &newparams, Some(&mut collect))?;
            snapmgr::PopActiveSnapshot()?;
        } else {
            let mut newparams = *params;
            newparams.options |= REINDEXOPT_REPORT_PROGRESS | REINDEXOPT_MISSING_OK;
            let mut collect = collect_reindex_cb();
            let result = reindex_relation(
                mcx,
                relid,
                REINDEX_REL_PROCESS_TOAST | REINDEX_REL_CHECK_CONSTRAINTS,
                &newparams,
                &mut collect,
            )?;
            if result && params.options & REINDEXOPT_VERBOSE != 0 {
                elog::ereport(types_error::INFO)
                    .errmsg(format!(
                        "table \"{}.{}\" was reindexed",
                        lsyscache::get_namespace_name(mcx, lsyscache::get_rel_namespace(relid)?)?
                            .map(|s| s.as_str().to_string())
                            .unwrap_or_default(),
                        lsyscache::get_rel_name(mcx, relid)?
                            .map(|s| s.as_str().to_string())
                            .unwrap_or_default()
                    ))
                    .finish(types_error::ErrorLocation::new(
                        "indexcmds.c",
                        0,
                        "ReindexMultipleInternal",
                    ))?;
            }
            snapmgr::PopActiveSnapshot()?;
        }

        xact::CommitTransactionCommand()?;
    }

    xact::StartTransactionCommand()
}

struct ReindexIndexInfo {
    index_id: Oid,
    table_id: Oid,
    safe: bool,
}

// ReindexRelationConcurrently (indexcmds.c:3568). Progress reporting is
// unported; the six-phase protocol is C-exact.
fn ReindexRelationConcurrently<'mcx>(
    mcx: Mcx<'mcx>,
    relationOid: Oid,
    params: &ReindexParams,
) -> PgResult<bool> {
    let verbose = params.options & REINDEXOPT_VERBOSE != 0;
    let (ru0, relation_name, relation_namespace) = if verbose {
        (
            pg_rusage::pg_rusage_init(),
            lsyscache::get_rel_name(mcx, relationOid)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default(),
            lsyscache::get_namespace_name(mcx, lsyscache::get_rel_namespace(relationOid)?)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default(),
        )
    } else {
        (pg_rusage::PgRUsage::default(), String::new(), String::new())
    };
    let missing_ok = params.options & REINDEXOPT_MISSING_OK != 0;

    let mut heap_relation_ids: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    let mut index_ids: mcx::PgVec<'mcx, ReindexIndexInfo> = mcx::PgVec::new_in(mcx);

    let relkind = lsyscache::get_rel_relkind(relationOid)? as u8;
    match relkind {
        RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_TOASTVALUE => 'arm: {
            heap_relation_ids.push(relationOid);

            if catalog::IsCatalogRelationOid(relationOid) {
                return Err(err(
                    "cannot reindex system catalogs concurrently".to_string(),
                    ERRCODE_FEATURE_NOT_SUPPORTED,
                ));
            }

            let heapRelation = if missing_ok {
                match table::try_table_open(mcx, relationOid, ShareUpdateExclusiveLock)? {
                    Some(rel) => rel,
                    None => break 'arm,
                }
            } else {
                table::table_open(mcx, relationOid, ShareUpdateExclusiveLock)?
            };

            if params.tablespace_oid != InvalidOid && catalog::IsSystemRelation(&heapRelation) {
                return Err(err(
                    format!("cannot move system relation \"{}\"", heapRelation.name()),
                    ERRCODE_FEATURE_NOT_SUPPORTED,
                ));
            }

            let index_list = relcache::indexlist::RelationGetIndexList(mcx, relationOid)?;
            for &cell_oid in index_list.iter() {
                collect_index_for_concurrent_reindex(mcx, cell_oid, &mut index_ids)?;
            }

            let toast_oid = heapRelation.rd_rel.reltoastrelid;
            if toast_oid != InvalidOid {
                let toastRelation = table::table_open(mcx, toast_oid, ShareUpdateExclusiveLock)?;
                heap_relation_ids.push(toast_oid);
                let toast_indexes = relcache::indexlist::RelationGetIndexList(mcx, toast_oid)?;
                for &cell_oid in toast_indexes.iter() {
                    collect_index_for_concurrent_reindex(mcx, cell_oid, &mut index_ids)?;
                }
                toastRelation.close(types_rel::NoLock)?;
            }

            heapRelation.close(types_rel::NoLock)?;
        }
        RELKIND_INDEX => 'arm: {
            let heap_id = catalog_index::IndexGetRelation(mcx, relationOid, missing_ok)?;
            if heap_id == InvalidOid {
                break 'arm;
            }
            if catalog::IsCatalogRelationOid(heap_id) {
                return Err(err(
                    "cannot reindex system catalogs concurrently".to_string(),
                    ERRCODE_FEATURE_NOT_SUPPORTED,
                ));
            }
            if catalog::IsToastNamespace(lsyscache::get_rel_namespace(relationOid)?)
                && !lsyscache::get_index_isvalid(relationOid)?
            {
                return Err(err(
                    "cannot reindex invalid index on TOAST table".to_string(),
                    ERRCODE_FEATURE_NOT_SUPPORTED,
                ));
            }
            let heapRelation = if missing_ok {
                match table::try_table_open(mcx, heap_id, ShareUpdateExclusiveLock)? {
                    Some(rel) => rel,
                    None => break 'arm,
                }
            } else {
                table::table_open(mcx, heap_id, ShareUpdateExclusiveLock)?
            };
            if params.tablespace_oid != InvalidOid && catalog::IsSystemRelation(&heapRelation) {
                return Err(err(
                    format!(
                        "cannot move system relation \"{}\"",
                        lsyscache::get_rel_name(mcx, relationOid)?
                            .map(|s| s.as_str().to_string())
                            .unwrap_or_default()
                    ),
                    ERRCODE_FEATURE_NOT_SUPPORTED,
                ));
            }
            heapRelation.close(types_rel::NoLock)?;

            heap_relation_ids.push(heap_id);
            // Invalid indexes are allowed here.
            index_ids.push(ReindexIndexInfo { index_id: relationOid, table_id: heap_id, safe: false });
        }
        _ => {
            return Err(err(
                "cannot reindex this type of relation concurrently".to_string(),
                ERRCODE_WRONG_OBJECT_TYPE,
            ));
        }
    }

    if index_ids.is_empty() {
        return Ok(false);
    }

    if params.tablespace_oid == GLOBALTABLESPACE_OID {
        let name = commands_tablespace::get_tablespace_name(mcx, params.tablespace_oid)?;
        let name = name
            .as_ref()
            .map(|n| std::str::from_utf8(n.name_str()).unwrap_or(""))
            .unwrap_or("");
        return Err(err(
            format!("cannot move non-shared relation to tablespace \"{name}\""),
            ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }

    // Phase 1: create the new indexes in the catalog and take session locks.
    let mut new_index_ids: mcx::PgVec<'mcx, ReindexIndexInfo> = mcx::PgVec::new_in(mcx);
    let mut relation_locks: mcx::PgVec<'mcx, LockRelId> = mcx::PgVec::new_in(mcx);
    let mut lock_tags: mcx::PgVec<'mcx, LOCKTAG> = mcx::PgVec::new_in(mcx);

    for i in 0..index_ids.len() {
        let idx_index_id = index_ids[i].index_id;
        let indexRel = indexam::index_open(mcx, idx_index_id, ShareUpdateExclusiveLock)?;
        let heap_id = indexRel.rd_index.as_ref().expect("index relation").indrelid;
        let heapRel = table::table_open(mcx, heap_id, ShareUpdateExclusiveLock)?;

        let guard = miscinit::SecContextGuard::security_restricted(heapRel.rd_rel.relowner);
        let save_nestlevel = guc::NewGUCNestLevel();
        guc::RestrictSearchPath()?;

        let safe = execindexing::RelationGetIndexExpressions(mcx, &indexRel)?.is_nil()
            && execindexing::RelationGetIndexPredicate(mcx, &indexRel)?.is_nil();
        index_ids[i].safe = safe;
        index_ids[i].table_id = heapRel.rd_id;

        if indexRel.rd_rel.relpersistence == RELPERSISTENCE_TEMP as u8 {
            panic!("cannot reindex a temporary table concurrently");
        }

        let concurrent_name = crate::define::ChooseRelationName(
            mcx,
            lsyscache::get_rel_name(mcx, idx_index_id)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default()
                .as_str(),
            None,
            "ccnew",
            lsyscache::get_rel_namespace(heap_id)?,
            false,
        )?;

        let tablespaceid = if params.tablespace_oid != InvalidOid
            && heapRel.rd_rel.relkind != RELKIND_TOASTVALUE
        {
            params.tablespace_oid
        } else {
            indexRel.rd_rel.reltablespace
        };

        let new_index_id = catalog_index::index_concurrently_create_copy(
            mcx,
            &heapRel,
            idx_index_id,
            tablespaceid,
            concurrent_name.as_str(),
        )?;

        let newIndexRel = indexam::index_open(mcx, new_index_id, ShareUpdateExclusiveLock)?;

        new_index_ids.push(ReindexIndexInfo {
            index_id: new_index_id,
            table_id: index_ids[i].table_id,
            safe,
        });
        relation_locks.push(indexRel.rd_lockInfo.lockRelId);
        relation_locks.push(newIndexRel.rd_lockInfo.lockRelId);

        indexam::index_close(indexRel, types_rel::NoLock)?;
        indexam::index_close(newIndexRel, types_rel::NoLock)?;

        guc::AtEOXact_GUC(false, save_nestlevel);
        guard.restore();

        heapRel.close(types_rel::NoLock)?;

        // index.c: EventTriggerCollectSimpleCommand(RelationRelationId,
        // newIndexId, stmt) — unconditional here (see collect_reindex_cb).
        event_trigger::EventTriggerCollectSimpleCommand(
            ObjectAddress::set(RELATION_RELATION_ID, new_index_id),
            ObjectAddress::set(InvalidOid, InvalidOid),
            cmdtag::GetCommandTagEnum(b"REINDEX"),
        );
    }

    for &heap_oid in heap_relation_ids.iter() {
        let heapRelation = table::table_open(mcx, heap_oid, ShareUpdateExclusiveLock)?;
        let lockrelid = heapRelation.rd_lockInfo.lockRelId;
        relation_locks.push(lockrelid);
        lock_tags.push(LOCKTAG::relation(lockrelid.dbId, lockrelid.relId));
        heapRelation.close(types_rel::NoLock)?;
    }

    for lockrelid in relation_locks.iter() {
        lmgr::LockRelationIdForSession(lockrelid, ShareUpdateExclusiveLock)?;
    }

    snapmgr::PopActiveSnapshot()?;
    xact::CommitTransactionCommand()?;
    xact::StartTransactionCommand()?;

    // Phase 2: build the new indexes, one transaction each.
    lmgr::WaitForLockersMultiple(mcx, &lock_tags, ShareLock)?;
    xact::CommitTransactionCommand()?;

    for newidx in new_index_ids.iter() {
        xact::StartTransactionCommand()?;
        postgres_seams::check_for_interrupts::call()?;
        if newidx.safe {
            procarray::SetIndexsafeProcflags()?;
        }
        let snap = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snap)?;

        catalog_index::index_concurrently_build(mcx, newidx.table_id, newidx.index_id)?;

        snapmgr::PopActiveSnapshot()?;
        xact::CommitTransactionCommand()?;
    }
    xact::StartTransactionCommand()?;

    // Phase 3: let the new indexes catch up, then validate, one per xact.
    lmgr::WaitForLockersMultiple(mcx, &lock_tags, ShareLock)?;
    xact::CommitTransactionCommand()?;

    for newidx in new_index_ids.iter() {
        xact::StartTransactionCommand()?;
        postgres_seams::check_for_interrupts::call()?;
        if newidx.safe {
            procarray::SetIndexsafeProcflags()?;
        }

        let snap = snapmgr::GetTransactionSnapshot()?;
        let snapshot = snapmgr::RegisterSnapshot(Some(&snap))?.expect("registered snapshot");
        snapmgr::PushActiveSnapshot(&snapshot)?;

        catalog_index::validate_index(mcx, newidx.table_id, newidx.index_id, &snapshot)?;

        let limit_xmin = snapshot.xmin;
        snapmgr::PopActiveSnapshot()?;
        snapmgr::UnregisterSnapshot(Some(&snapshot));

        xact::CommitTransactionCommand()?;
        xact::StartTransactionCommand()?;

        crate::WaitForOlderSnapshots(limit_xmin)?;

        xact::CommitTransactionCommand()?;
    }

    // Phase 4: swap the indexes.
    xact::StartTransactionCommand()?;
    procarray::SetIndexsafeProcflags()?;

    for (oldidx, newidx) in index_ids.iter().zip(new_index_ids.iter()) {
        postgres_seams::check_for_interrupts::call()?;

        let old_name = crate::define::ChooseRelationName(
            mcx,
            lsyscache::get_rel_name(mcx, oldidx.index_id)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default()
                .as_str(),
            None,
            "ccold",
            lsyscache::get_rel_namespace(oldidx.table_id)?,
            false,
        )?;

        let snap = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snap)?;

        catalog_index::index_concurrently_swap(mcx, newidx.index_id, oldidx.index_id, old_name.as_str())?;

        snapmgr::PopActiveSnapshot()?;

        inval::invalidate::CacheInvalidateRelcacheByRelid(oldidx.table_id)?;

        xact::CommandCounterIncrement()?;
    }

    xact::CommitTransactionCommand()?;
    xact::StartTransactionCommand()?;

    // Phase 5: mark the old indexes dead.
    lmgr::WaitForLockersMultiple(mcx, &lock_tags, AccessExclusiveLock)?;

    for oldidx in index_ids.iter() {
        postgres_seams::check_for_interrupts::call()?;
        let snap = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snap)?;
        catalog_index::index_concurrently_set_dead(mcx, oldidx.table_id, oldidx.index_id)?;
        snapmgr::PopActiveSnapshot()?;
    }

    xact::CommitTransactionCommand()?;
    xact::StartTransactionCommand()?;

    // Phase 6: drop the old indexes.
    lmgr::WaitForLockersMultiple(mcx, &lock_tags, AccessExclusiveLock)?;

    let snap = snapmgr::GetTransactionSnapshot()?;
    snapmgr::PushActiveSnapshot(&snap)?;
    {
        let mut objects = catalog_dependency::ObjectAddresses::new();
        for idx in index_ids.iter() {
            objects.add_exact_object_address(pg_depend::ObjectAddress {
                classId: types_core::RELATION_RELATION_ID,
                objectId: idx.index_id,
                objectSubId: 0,
            });
        }
        catalog_dependency::performMultipleDeletions(
            mcx,
            &objects,
            DropBehavior::DROP_RESTRICT,
            catalog_dependency::PERFORM_DELETION_CONCURRENT_LOCK
                | catalog_dependency::PERFORM_DELETION_INTERNAL,
        )?;
    }
    snapmgr::PopActiveSnapshot()?;
    xact::CommitTransactionCommand()?;

    for lockrelid in relation_locks.iter() {
        lmgr::UnlockRelationIdForSession(lockrelid, ShareUpdateExclusiveLock)?;
    }

    xact::StartTransactionCommand()?;

    if verbose {
        if relkind == RELKIND_INDEX {
            elog::ereport(types_error::INFO)
                .errmsg(format!(
                    "index \"{relation_namespace}.{relation_name}\" was reindexed"
                ))
                .errdetail(format!("{}.", pg_rusage::pg_rusage_show(&ru0).as_str()))
                .finish(types_error::ErrorLocation::new(
                    "indexcmds.c",
                    0,
                    "ReindexRelationConcurrently",
                ))?;
        } else {
            for idx in new_index_ids.iter() {
                elog::ereport(types_error::INFO)
                    .errmsg(format!(
                        "index \"{}.{}\" was reindexed",
                        lsyscache::get_namespace_name(
                            mcx,
                            lsyscache::get_rel_namespace(idx.index_id)?
                        )?
                        .map(|s| s.as_str().to_string())
                        .unwrap_or_default(),
                        lsyscache::get_rel_name(mcx, idx.index_id)?
                            .map(|s| s.as_str().to_string())
                            .unwrap_or_default()
                    ))
                    .finish(types_error::ErrorLocation::new(
                        "indexcmds.c",
                        0,
                        "ReindexRelationConcurrently",
                    ))?;
            }
            elog::ereport(types_error::INFO)
                .errmsg(format!(
                    "table \"{relation_namespace}.{relation_name}\" was reindexed"
                ))
                .errdetail(format!("{}.", pg_rusage::pg_rusage_show(&ru0).as_str()))
                .finish(types_error::ErrorLocation::new(
                    "indexcmds.c",
                    0,
                    "ReindexRelationConcurrently",
                ))?;
        }
    }

    Ok(true)
}

fn collect_index_for_concurrent_reindex<'mcx>(
    mcx: Mcx<'mcx>,
    cell_oid: Oid,
    index_ids: &mut mcx::PgVec<'mcx, ReindexIndexInfo>,
) -> PgResult<()> {
    let index_relation = indexam::index_open(mcx, cell_oid, ShareUpdateExclusiveLock)?;
    let form = index_relation.rd_index.as_ref().expect("index relation");
    if !form.indisvalid {
        elog_seams::ereport::call(
            PgError::new(
                WARNING,
                format!(
                    "skipping reindex of invalid index \"{}.{}\"",
                    lsyscache::get_namespace_name(mcx, lsyscache::get_rel_namespace(cell_oid)?)?
                        .map(|s| s.as_str().to_string())
                        .unwrap_or_default(),
                    lsyscache::get_rel_name(mcx, cell_oid)?
                        .map(|s| s.as_str().to_string())
                        .unwrap_or_default()
                ),
            )
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint("Use DROP INDEX or REINDEX INDEX.".to_string()),
        )?;
    } else if form.indisexclusion {
        elog_seams::ereport::call(
            PgError::new(
                WARNING,
                format!(
                    "cannot reindex exclusion constraint index \"{}.{}\" concurrently, skipping",
                    lsyscache::get_namespace_name(mcx, lsyscache::get_rel_namespace(cell_oid)?)?
                        .map(|s| s.as_str().to_string())
                        .unwrap_or_default(),
                    lsyscache::get_rel_name(mcx, cell_oid)?
                        .map(|s| s.as_str().to_string())
                        .unwrap_or_default()
                ),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        )?;
    } else {
        index_ids.push(ReindexIndexInfo { index_id: cell_oid, table_id: InvalidOid, safe: false });
    }
    indexam::index_close(index_relation, types_rel::NoLock)
}
