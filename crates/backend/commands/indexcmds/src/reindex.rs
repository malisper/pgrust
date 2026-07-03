// ExecReindex/ReindexIndex/ReindexTable, non-concurrent lane.
#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: indexcmds {what}")
}

use catalog_index::{
    reindex_index, reindex_relation, ReindexParams, REINDEXOPT_CONCURRENTLY,
    REINDEXOPT_REPORT_PROGRESS, REINDEXOPT_VERBOSE, REINDEX_REL_CHECK_CONSTRAINTS,
    REINDEX_REL_PROCESS_TOAST,
};
use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR, ERRCODE_WRONG_OBJECT_TYPE, ERROR};
use types_nodes::parsenodes::{ReindexObjectType, ReindexStmt};
use types_rel::{
    AccessExclusiveLock, ShareLock, RELKIND_INDEX, RELKIND_PARTITIONED_INDEX,
    RELKIND_PARTITIONED_TABLE,
};

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
            "tablespace" => tablespacename = Some(explain::defGetString(opt)?),
            name => {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!("unrecognized REINDEX option \"{name}\""),
                    )
                    .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                ))
            }
        }
    }

    if concurrently {
        xact::PreventInTransactionBlock(is_top_level, "REINDEX CONCURRENTLY")?;
        unported("ExecReindex: CONCURRENTLY (ReindexRelationConcurrently)");
    }

    let mut params = ReindexParams {
        options: if verbose { REINDEXOPT_VERBOSE } else { 0 },
        tablespace_oid: InvalidOid,
    };
    if tablespacename.is_some() {
        unported("ExecReindex: TABLESPACE option (get_tablespace_oid move lane)");
    }

    match stmt.kind {
        ReindexObjectType::REINDEX_OBJECT_INDEX => ReindexIndex(mcx, stmt, &mut params),
        ReindexObjectType::REINDEX_OBJECT_TABLE => ReindexTable(mcx, stmt, &mut params),
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
            unported("ExecReindex: ReindexMultipleTables (SCHEMA/SYSTEM/DATABASE)");
        }
    }
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
) -> PgResult<()> {
    let rv = stmt_range_var(stmt);
    let concurrent = params.options & REINDEXOPT_CONCURRENTLY != 0;
    debug_assert!(!concurrent);

    let mut locked_table_oid = InvalidOid;
    let mut cb = |rv2: &rel_vocab::RangeVar<'_>, rel_id: Oid, old_rel_id: Oid| -> PgResult<()> {
        RangeVarCallbackForReindexIndex(mcx, rv2, rel_id, old_rel_id, &mut locked_table_oid)
    };
    let ind_oid = catalog_namespace::RangeVarGetRelidExtended(
        &rv,
        AccessExclusiveLock,
        0,
        Some(&mut cb),
    )?;

    let persistence = lsyscache::get_rel_persistence(ind_oid)? as u8;
    let relkind = lsyscache::get_rel_relkind(ind_oid)? as u8;

    if relkind == RELKIND_PARTITIONED_INDEX {
        unported("ReindexIndex: ReindexPartitions");
    }
    let mut newparams = *params;
    newparams.options |= REINDEXOPT_REPORT_PROGRESS;
    reindex_index(mcx, ind_oid, false, persistence, &newparams)
}

fn RangeVarCallbackForReindexIndex(
    _mcx: Mcx<'_>,
    relation: &rel_vocab::RangeVar<'_>,
    relId: Oid,
    oldRelId: Oid,
    locked_table_oid: &mut Oid,
) -> PgResult<()> {
    // Non-concurrent lane: table lock ShareLock (matches reindex_index).
    let table_lockmode = ShareLock;

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
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("\"{}\" is not an index", relation.relname),
            )
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
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
) -> PgResult<()> {
    let rv = stmt_range_var(stmt);
    debug_assert!(params.options & REINDEXOPT_CONCURRENTLY == 0);

    let mut cb = |rv2: &rel_vocab::RangeVar<'_>, rel_id: Oid, old_rel_id: Oid| -> PgResult<()> {
        tablecmds::RangeVarCallbackMaintainsTable(rv2, rel_id, old_rel_id)
    };
    let heap_oid =
        catalog_namespace::RangeVarGetRelidExtended(&rv, ShareLock, 0, Some(&mut cb))?;

    if lsyscache::get_rel_relkind(heap_oid)? as u8 == RELKIND_PARTITIONED_TABLE {
        unported("ReindexTable: ReindexPartitions");
    }
    let mut newparams = *params;
    newparams.options |= REINDEXOPT_REPORT_PROGRESS;
    let result = reindex_relation(
        mcx,
        heap_oid,
        REINDEX_REL_PROCESS_TOAST | REINDEX_REL_CHECK_CONSTRAINTS,
        &newparams,
    )?;
    if !result {
        elog::ereport(types_error::NOTICE)
            .errmsg(format!("table \"{}\" has no indexes to reindex", rv.relname))
            .finish(elog::loc("ReindexTable"))?;
    }
    Ok(())
}
