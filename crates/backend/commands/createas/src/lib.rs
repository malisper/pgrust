// createas.c — CREATE TABLE AS / SELECT INTO / CREATE MATERIALIZED VIEW
// (PG 18.3). The DR_intorel marshal shape lives in createas_seams (tcop_dest
// sits below the executor stack).
#![allow(non_snake_case)]

use createas_seams::IntoRelState;
use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::{
    PgResult, ERRCODE_DUPLICATE_TABLE, ERRCODE_INDETERMINATE_COLLATION, ERRCODE_SYNTAX_ERROR,
    ERROR, NOTICE,
};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::Query;
use types_nodes::rawnodes::{ColumnDef, CreateStmt, CreateTableAsStmt, IntoClause, TypeName};
use types_nodes::{Node, NodeList};
use types_portal::{
    ParamListHandle, QueryCompletion, QueryEnvHandle, CMDTAG_SELECT, CURSOR_OPT_PARALLEL_OK,
};
use types_slot::{SlotData, EXEC_FLAG_WITH_NO_DATA};
use types_tuple::TupleDescData;

pub fn init_seams() {
    createas_seams::intorel_startup::set(intorel_startup);
    createas_seams::intorel_receive::set(intorel_receive);
    createas_seams::intorel_shutdown::set(intorel_shutdown);
    createas_seams::get_into_rel_eflags::set(into_rel_eflags);
    createas_seams::create_table_as_rel_exists::set(CreateTableAsRelExists);
}

fn into_rel_eflags(skip_data: bool) -> i32 {
    if skip_data {
        EXEC_FLAG_WITH_NO_DATA
    } else {
        0
    }
}

struct QueryDescOwner(types_portal::QueryDescHandle);

impl QueryDescOwner {
    fn disarm(&mut self) {
        self.0 = types_portal::QueryDescHandle::NULL;
    }
}

impl Drop for QueryDescOwner {
    fn drop(&mut self) {
        if !self.0.is_null() {
            execmain_seams::release_query_desc::call(self.0);
        }
    }
}

pub fn ExecCreateTableAs<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateTableAsStmt<'mcx>,
    source_text: &str,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
    mut qc: Option<&mut QueryCompletion>,
) -> PgResult<Oid> {
    let into_node = stmt.into.expect("CreateTableAsStmt.into");
    let into = into_node.as_variant::<IntoClause>().expect("IntoClause");
    let is_matview = into.viewQuery.is_some();

    if CreateTableAsRelExists(mcx, stmt)? {
        return Ok(InvalidOid);
    }

    let query_node = stmt.query.expect("CreateTableAsStmt.query");
    // C copyObject(query) before QueryRewrite (createas.c): the statement
    // tree lives in the caller's arena; rewrite/planning grow its lists
    // under this mcx (see ExplainQuery).
    let query_node = copyfuncs::copy_object(mcx, query_node)?;
    // SAFETY: fresh copy; this call holds its only live access.
    let mut query: Query<'mcx> = unsafe { query_node.with_mut::<Query, _>(core::mem::take) }
        .expect("CreateTableAsStmt.query is an analyzed Query");

    if parser_analyze::tap_post_parse_analyze::is_installed() && queryjumble::IsQueryIdEnabled()
    {
        let js = queryjumble::JumbleQuery(mcx, &mut query)?;
        parser_analyze::tap_post_parse_analyze::call_if(|f| f(&mut query, &js, source_text));
    } else if queryjumble::IsQueryIdEnabled() {
        queryjumble::JumbleQueryDiscard(mcx, &mut query)?;
    }

    if query.commandType == CmdType::CMD_UTILITY {
        debug_assert!(!is_matview);
        let estmt = query
            .utilityStmt
            .and_then(|u| u.as_execute_stmt())
            .expect("CTAS utility query is EXECUTE (excluded by syntax)");
        let mut dest = tcop_dest::DestReceiver::IntoRel(IntoRelState::new(mcx, into_node));
        let r = prepare::ExecuteQuery(mcx, estmt, source_text, params, Some(into), &mut dest, qc);
        let relid = match &dest {
            tcop_dest::DestReceiver::IntoRel(st) => st.reladdr,
            _ => unreachable!(),
        };
        dest.destroy();
        r?;
        return Ok(relid);
    }
    debug_assert!(query.commandType == CmdType::CMD_SELECT);

    if is_matview {
        // C forces the no-data create then fills via RefreshMatViewByOid;
        // viewQuery aliases stmt.query, so the taken Query is C's copyObject.
        let do_refresh = !into.skipData;
        let mut query = query;
        let relid = create_ctas_nodata(mcx, &mut query, into_node, true)?;
        if do_refresh {
            matview_seams::refresh_mat_view_by_oid::call(
                mcx,
                relid,
                true,
                false,
                false,
                source_text,
                qc.as_deref_mut(),
            )?;
        }
        return Ok(relid);
    }

    if into.skipData {
        // WITH NO DATA skips rewriter/planner/executor entirely; the portal's
        // parse-time tag (CREATE TABLE AS / SELECT INTO) reaches the client.
        let mut query = query;
        return create_ctas_nodata(mcx, &mut query, into_node, false);
    }

    let mut dest = tcop_dest::DestReceiver::IntoRel(IntoRelState::new(mcx, into_node));

    let rewritten = rewrite_handler_seams::query_rewrite::call(mcx, query)?;
    assert_eq!(rewritten.len(), 1, "unexpected rewrite result for CREATE TABLE AS SELECT");
    let query = rewritten.into_iter().next().expect("checked above");
    debug_assert!(query.commandType == CmdType::CMD_SELECT);

    let plan = postgres::simple_query::pg_plan_query(
        mcx,
        mcx::leak_in(mcx::alloc_in(mcx, query)?),
        source_text,
        CURSOR_OPT_PARALLEL_OK,
        params,
    )?
    .expect("planner handles CMD_SELECT");

    snapmgr::PushCopiedSnapshot(&snapmgr::GetActiveSnapshot())?;
    snapmgr::UpdateActiveSnapshotCommandId()?;

    let qd = execmain_seams::create_query_desc::call(
        &plan,
        source_text,
        Some(snapmgr::GetActiveSnapshot()),
        None,
        types_dest::CommandDest::IntoRel,
        params,
        query_env,
        0,
    )?;
    let mut qd_owner = QueryDescOwner(qd);

    execmain_seams::executor_start::call(qd, GetIntoRelEFlags(into))?;
    execmain_seams::executor_run::call(
        qd,
        types_scan::sdir::ScanDirection::ForwardScanDirection,
        0,
        &mut dest,
    )?;

    if let Some(qc) = qc.as_deref_mut() {
        qc.commandTag = CMDTAG_SELECT;
        qc.nprocessed = execmain_seams::query_desc_es_processed::call(qd);
    }

    execmain_seams::executor_finish::call(qd)?;
    execmain_seams::executor_end::call(qd)?;
    qd_owner.disarm();
    execmain_seams::free_query_desc::call(qd);
    snapmgr::PopActiveSnapshot()?;
    let relid = match &dest {
        tcop_dest::DestReceiver::IntoRel(st) => st.reladdr,
        _ => unreachable!(),
    };
    dest.destroy();

    Ok(relid)
}

pub fn GetIntoRelEFlags(into: &IntoClause<'_>) -> i32 {
    into_rel_eflags(into.skipData)
}

pub fn CreateTableAsRelExists<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateTableAsStmt<'mcx>,
) -> PgResult<bool> {
    let into = stmt
        .into
        .expect("CreateTableAsStmt.into")
        .as_variant::<IntoClause>()
        .expect("IntoClause");
    let rv_node = into
        .rel
        .expect("IntoClause.rel")
        .as_range_var()
        .expect("IntoClause.rel is RangeVar");
    let relname = rv_node.relname.expect("RangeVar.relname");
    let rv = rel_vocab::RangeVar {
        catalogname: rv_node.catalogname,
        schemaname: rv_node.schemaname,
        relname,
        inh: rv_node.inh,
        relpersistence: rv_node.relpersistence,
        location: rv_node.location,
    };
    let nspid = catalog_namespace::RangeVarGetCreationNamespace(mcx, &rv)?;
    let oldrelid = lsyscache::get_relname_relid(relname, nspid)?;
    if oldrelid != InvalidOid {
        if !stmt.if_not_exists {
            return Err(elog::ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_TABLE)
                .errmsg(format!("relation \"{relname}\" already exists"))
                .into_error()
                .into());
        }
        // checkMembershipInCurrentExtension: creating_extension is always
        // false (no extension lane), so the C check is a no-op.
        elog_seams::ereport::call(
            types_error::PgError::new(
                NOTICE,
                format!("relation \"{relname}\" already exists, skipping"),
            )
            .with_sqlstate(ERRCODE_DUPLICATE_TABLE),
        )?;
        return Ok(true);
    }
    Ok(false)
}

fn create_ctas_internal<'mcx>(
    mcx: Mcx<'mcx>,
    attr_list: NodeList<'mcx>,
    into_node: Node<'mcx>,
    mut view_query: Option<&mut Query<'mcx>>,
) -> PgResult<Oid> {
    let into = into_node.as_variant::<IntoClause>().expect("IntoClause");
    let is_matview = view_query.is_some();
    debug_assert!(is_matview == into.viewQuery.is_some());
    let relkind =
        if is_matview { types_rel::RELKIND_MATVIEW } else { types_rel::RELKIND_RELATION };

    let create = CreateStmt {
        relation: into
            .rel
            .expect("IntoClause.rel")
            .as_range_var(),
        tableElts: attr_list,
        options: into.options.clone_in(mcx)?,
        oncommit: into.onCommit,
        tablespacename: into.tableSpaceName,
        if_not_exists: false,
        accessMethod: into.accessMethod,
        ..CreateStmt::default()
    };

    let relid = tablecmds::DefineRelation(mcx, &create, relkind, InvalidOid, "")?;
    xact::CommandCounterIncrement()?;
    // toast reloptions: WITH (...) is loud in DefineRelation, so the list is
    // nil here and transformRelOptions would yield (Datum) 0.
    catalog_toasting::NewRelationCreateToastTable(mcx, relid, None)?;
    if let Some(query) = view_query.take() {
        commands_view::StoreViewQuery(mcx, relid, query, false)?;
        xact::CommandCounterIncrement()?;
    }
    Ok(relid)
}

fn create_ctas_nodata<'mcx>(
    mcx: Mcx<'mcx>,
    query: &mut Query<'mcx>,
    into_node: Node<'mcx>,
    is_matview: bool,
) -> PgResult<Oid> {
    let into = into_node.as_variant::<IntoClause>().expect("IntoClause");
    let mut attr_list = NodeList::nil();
    let mut colnames = into.colNames.iter();
    for t in query.targetList.iter() {
        let tle = t.as_target_entry().expect("targetlist entry is a TargetEntry");
        if tle.resjunk {
            continue;
        }
        let colname = match colnames.next() {
            Some(n) => n.as_string().expect("colNames are String nodes").sval,
            None => tle.resname.expect("non-junk TLE has a resname"),
        };
        let expr = tle.expr;
        let col = make_column_def(
            mcx,
            colname,
            parse_expr::expr_type(expr),
            parse_expr::expr_typmod(expr),
            parse_expr::expr_collation(expr),
        )?;
        attr_list.lappend(mcx, col)?;
    }
    if colnames.next().is_some() {
        return Err(too_many_column_names());
    }
    create_ctas_internal(mcx, attr_list, into_node, is_matview.then_some(query))
}

// makeColumnDef (makefuncs.c) + the collatable double-check both intorel
// callers share (DefineRelation would adopt the type default silently).
fn make_column_def<'mcx>(
    mcx: Mcx<'mcx>,
    colname: &'mcx str,
    typid: Oid,
    typmod: i32,
    coll_oid: Oid,
) -> PgResult<Node<'mcx>> {
    if coll_oid == InvalidOid && lsyscache::type_is_collatable(typid)? {
        return Err(elog::ereport(ERROR)
            .errcode(ERRCODE_INDETERMINATE_COLLATION)
            .errmsg(format!(
                "no collation was derived for column \"{colname}\" with collatable type {}",
                format_type::format_type_be(typid)?
            ))
            .errhint("Use the COLLATE clause to set the collation explicitly.")
            .into_error()
            .into());
    }
    let mut tn = Node::build::<TypeName>(mcx)?;
    tn.typeOid = typid;
    tn.typemod = typmod;
    tn.location = -1;
    let tn = tn.seal();
    let mut col = Node::build::<ColumnDef>(mcx)?;
    col.colname = Some(colname);
    col.typeName = Some(tn);
    col.is_local = true;
    col.collOid = coll_oid;
    col.location = -1;
    Ok(col.seal())
}

#[cold]
fn too_many_column_names() -> Box<types_error::PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("too many column names were specified")
            .into_error(),
    )
}



fn intorel_startup<'mcx>(
    state: &mut IntoRelState<'mcx>,
    _operation: i32,
    typeinfo: &TupleDescData<'_>,
) -> PgResult<()> {
    let mcx = state.mcx;
    let into_node = state.into;
    let into = into_node.as_variant::<IntoClause>().expect("IntoClause");
    // ExecCreateTableAs handles matviews via no-data create + refresh; this
    // path fires only for EXPLAIN ANALYZE CREATE MATERIALIZED VIEW.
    let is_matview = into.viewQuery.is_some();

    let mut attr_list = NodeList::nil();
    let mut colnames = into.colNames.iter();
    for i in 0..typeinfo.natts as usize {
        let att = typeinfo.attr(i);
        let colname: &'mcx str = match colnames.next() {
            Some(n) => n.as_string().expect("colNames are String nodes").sval,
            None => {
                let bytes = mcx::slice_in(mcx, att.attname.name_str())?.leak();
                core::str::from_utf8(bytes).expect("attname UTF-8")
            }
        };
        let col = make_column_def(mcx, colname, att.atttypid, att.atttypmod, att.attcollation)?;
        attr_list.lappend(mcx, col)?;
    }
    if colnames.next().is_some() {
        return Err(too_many_column_names());
    }

    let relid = match into.viewQuery {
        Some(vq_node) => {
            // C: StoreViewQuery scribbles on the tree, so copyObject first.
            let copy = copyfuncs::copy_object(mcx, vq_node)?;
            // SAFETY: fresh copy; this call holds its only live access.
            unsafe {
                copy.with_mut::<Query, _>(|q| {
                    create_ctas_internal(mcx, attr_list, into_node, Some(q))
                })
            }
            .expect("viewQuery is an analyzed Query")?
        }
        None => create_ctas_internal(mcx, attr_list, into_node, None)?,
    };
    let rel = table::table_open(mcx, relid, types_rel::AccessExclusiveLock)?;

    if rel.rd_rel.relrowsecurity {
        panic!("intorel_startup (createas.c): check_enable_rls unported (rls lane)");
    }

    if is_matview && !into.skipData {
        matview_seams::set_mat_view_populated_state::call(mcx, &rel, true)?;
    }

    state.reladdr = relid;
    state.output_cid = xact::GetCurrentCommandId(true)?;
    state.ti_options = tableam_vocab::TABLE_INSERT_SKIP_FSM;
    state.bistate = if !into.skipData { Some(heapam::GetBulkInsertState()) } else { None };
    // W1 multi-insert buffering (PGRUST_CTAS_MULTIINSERT, default OFF).
    state.mibuf = if !into.skipData { tableam::write_buffer::write_buffer_begin(&rel) } else { None };
    state.rel = Some(rel);
    Ok(())
}

fn intorel_receive<'mcx>(
    state: &mut IntoRelState<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // WITH NO DATA never starts the executor for plain tables, so C's
    // skipData test here has no live arm; a newly created relation has no
    // indexes to insert into.
    let rel = state.rel.as_ref().expect("intorel_startup ran");
    if let Some(buf) = state.mibuf.as_mut() {
        tableam::write_buffer::write_buffer_receive(
            state.mcx,
            rel,
            buf,
            slot,
            state.output_cid,
            state.ti_options,
            state.bistate.as_mut(),
        )?;
        return Ok(true);
    }
    tableam::table_tuple_insert(
        state.mcx,
        rel,
        slot,
        state.output_cid,
        state.ti_options,
        state.bistate.as_mut(),
    )?;
    Ok(true)
}

fn intorel_shutdown<'mcx>(state: &mut IntoRelState<'mcx>) -> PgResult<()> {
    let skip_data = state
        .into
        .as_variant::<IntoClause>()
        .expect("IntoClause")
        .skipData;
    if !skip_data {
        // Tail flush (success path only — an erroring statement drops the
        // buffered copies unflushed, and the aborted xact kills the rest).
        if let Some(mut buf) = state.mibuf.take() {
            if let Some(rel) = state.rel.as_ref() {
                tableam::write_buffer::write_buffer_flush(
                    state.mcx,
                    rel,
                    &mut buf,
                    state.output_cid,
                    state.ti_options,
                    state.bistate.as_mut(),
                )?;
            }
        }
        // FreeBulkInsertState: the pin/strategy guards release on drop.
        drop(state.bistate.take());
        if let Some(rel) = state.rel.as_ref() {
            tableam::table_finish_bulk_insert(rel, state.ti_options)?;
        }
    }
    if let Some(rel) = state.rel.take() {
        table::table_close(rel, types_rel::NoLock)?;
    }
    Ok(())
}
