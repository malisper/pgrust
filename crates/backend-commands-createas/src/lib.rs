#![allow(non_snake_case)]
//! Port of `commands/createas.c` — `CREATE TABLE AS` / `SELECT INTO` /
//! `CREATE MATERIALIZED VIEW`.
//!
//! CTAS is implemented by diverting the query's normal output to a specialized
//! `DestReceiver` (`DR_intorel`). This crate is the real owner of that receiver:
//! its callbacks (`intorel_startup` / `intorel_receive` / `intorel_shutdown`)
//! are registered into the tcop-dest router ([`backend_tcop_dest`]) the same way
//! `copyto.c`'s `DR_copy` is — a real [`backend_tcop_dest::ReceiverVtable`] keyed
//! by an owner-supplied `state` token. The DestReceiver mcx-vtable keystone
//! (#333) threads the per-query `Mcx<'mcx>` to each callback, so
//! `intorel_receive` reaches `table_tuple_insert(mcx, &rel, slot, …)`.
//!
//! # The `DR_intorel` state model
//!
//! C's `DR_intorel` is `palloc0`'d at receiver creation; `intorel_startup`
//! fills its `rel`/`reladdr`/`output_cid`/`ti_options`/`bistate` fields, the
//! later callbacks read them, and `ExecCreateTableAs` reads `reladdr` back out
//! after the run. Here each receiver owns a [`ReceiverSlot`] in a per-backend
//! (`thread_local`) registry keyed by its `state` token (the router's stand-in
//! for the C `(DR_intorel *) self` downcast). The slot holds:
//!
//!  * `reladdr` — a lifetime-free [`ObjectAddress`], stored directly so
//!    `ExecCreateTableAs` reads it after `intorel_shutdown` without the run's
//!    `'mcx`.
//!  * `state` — a raw pointer to the `'mcx`-bound [`IntoRelStateData`] that
//!    `intorel_startup` allocates in the threaded query arena (via
//!    [`mcx::leak_in`], so it lives for the query, exactly like the C `palloc`)
//!    and binds for the duration of the run. `intorel_receive`/`_shutdown`
//!    recover it. This mirrors `copyto.c`'s `DR_copy.cstate` raw-pointer alias,
//!    except the backing store is the receiver's own arena allocation rather
//!    than a caller stack frame.

extern crate alloc;

use core::cell::RefCell;

use backend_utils_error::ereport;
use mcx::{Mcx, PgBox, PgVec};
use types_catalog::catalog_dependency::{InvalidObjectAddress, ObjectAddress};
use types_core::primitive::{InvalidOid, Oid};
use types_core::xact::CommandId;
use types_dest::CommandDest;
use types_error::{
    ErrorLocation, PgResult, ERRCODE_DUPLICATE_TABLE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INDETERMINATE_COLLATION, ERRCODE_SYNTAX_ERROR, ERROR, NOTICE,
};
use types_nodes::ddlnodes::{CreateTableAsStmt, IntoClause};
use types_nodes::nodes::{CmdType, Node};
use types_nodes::params::ParamListInfo;
use types_nodes::parsestmt::DestReceiverHandle;
use types_nodes::tuptable::SlotData;
use types_portal::QueryCompletion;
use types_rel::Relation;
use types_storage::lock::{AccessExclusiveLock, NoLock};
use types_tableam::tableam::BulkInsertStateData;
use types_tuple::access::RangeVar as AccessRangeVar;
use types_tuple::heaptuple::TupleDescData;

/// `RelationRelationId` (`catalog/pg_class.h`) — OID of `pg_class`.
const RELATION_RELATION_ID: Oid = types_core::catalog::RELATION_RELATION_ID;

/// `RELKIND_RELATION` / `RELKIND_MATVIEW` (`catalog/pg_class.h`).
const RELKIND_RELATION: u8 = types_tuple::access::RELKIND_RELATION;
const RELKIND_MATVIEW: u8 = types_tuple::access::RELKIND_MATVIEW;

/// `EXEC_FLAG_WITH_NO_DATA` (`executor/executor.h`).
const EXEC_FLAG_WITH_NO_DATA: i32 = 0x0040;

/// `TABLE_INSERT_SKIP_FSM` (`access/tableam.h`).
const TABLE_INSERT_SKIP_FSM: i32 = 0x0002;

/// `RLS_ENABLED` (`utils/rls.h`).
const RLS_ENABLED: types_acl::CheckEnableRlsResult = types_acl::CheckEnableRlsResult::RlsEnabled;

/// `OidIsValid(oid)`.
#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// `ErrorLocation` for `ereport(...).finish(...)` in this module.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/commands/createas.c", 0, funcname)
}

// ===========================================================================
// DR_intorel state + per-backend receiver registry
// ===========================================================================

/// The private `DR_intorel` fields `intorel_startup` fills and the later
/// callbacks consume. `'mcx`-bound because `rel` (an open relcache handle from
/// `table_open`) and `bistate` live in the per-query arena.
struct IntoRelStateData<'mcx> {
    /// `IntoClause *into` — the receiver's target spec (copied into the arena).
    into: IntoClause<'mcx>,
    /// `Relation rel` — the open destination relation (`None` once closed).
    rel: Option<Relation<'mcx>>,
    /// `CommandId output_cid` — cmin to stamp on inserted tuples.
    output_cid: CommandId,
    /// `int ti_options` — `table_tuple_insert` performance options.
    ti_options: i32,
    /// `BulkInsertState bistate` — bulk-insert state (`None` when `skipData`).
    bistate: Option<PgBox<'mcx, BulkInsertStateData>>,
}

/// One registered `DR_intorel` receiver. `reladdr` is lifetime-free so
/// `ExecCreateTableAs` reads it after the run; `state` is the raw pointer to the
/// `'mcx`-bound [`IntoRelStateData`] bound for the duration of the run.
struct ReceiverSlot {
    /// `((DR_intorel *) dest)->reladdr` (createas.c:264/352) — saved by
    /// `intorel_startup`, read by `ExecCreateTableAs`. `InvalidObjectAddress`
    /// until startup runs.
    reladdr: ObjectAddress,
    /// Raw pointer to the live `IntoRelStateData` (set by `intorel_startup`,
    /// cleared by `intorel_shutdown`). Null when no run is in progress.
    state: *mut (),
}

thread_local! {
    static RECEIVERS: RefCell<alloc::vec::Vec<Option<ReceiverSlot>>> =
        const { RefCell::new(alloc::vec::Vec::new()) };
}

/// Allocate a fresh receiver slot (1-based token; 0 is never handed out).
fn receiver_register() -> u64 {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        reg.push(Some(ReceiverSlot {
            reladdr: InvalidObjectAddress,
            state: core::ptr::null_mut(),
        }));
        reg.len() as u64
    })
}

/// Bind the live `IntoRelStateData` pointer to a receiver token for the run.
fn receiver_bind(token: u64, state: *mut ()) {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        if let Some(Some(slot)) = reg.get_mut((token - 1) as usize) {
            slot.state = state;
        }
    });
}

/// Save the created relation's address on the receiver slot (the C
/// `myState->reladdr = intoRelationAddr`).
fn receiver_set_reladdr(token: u64, addr: ObjectAddress) {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        if let Some(Some(slot)) = reg.get_mut((token - 1) as usize) {
            slot.reladdr = addr;
        }
    });
}

/// Set up the run-bound `IntoRelStateData` for a receiver: allocate it in the
/// per-query arena (carrying `myState->into`, with the `rel`/`bistate` fields
/// filled later by `intorel_startup`), leak it to a stable `'mcx` reference (it
/// lives for the query, as the C `palloc`'d `DR_intorel` does), and bind its raw
/// pointer to the receiver token for the duration of the run.
///
/// This is the owned-model stand-in for C's `CreateIntoRelDestReceiver` setting
/// `self->into` at receiver creation: the receiver-creation site
/// (`CreateIntoRelDestReceiver`) has no per-query arena, so the driver
/// (`ExecCreateTableAs` / `ExecuteQuery`) that owns the run threads `into` here.
fn receiver_setup_run<'mcx>(token: u64, mcx: Mcx<'mcx>, into: IntoClause<'mcx>) -> PgResult<()> {
    let state = mcx::leak_in(mcx::alloc_in(
        mcx,
        IntoRelStateData {
            into,
            rel: None,
            output_cid: 0,
            ti_options: 0,
            bistate: None,
        },
    )?);
    receiver_bind(token, state as *mut IntoRelStateData<'mcx> as *mut ());
    Ok(())
}

/// Clear the bound state pointer after the run (the C `myState->rel = NULL`).
fn receiver_unbind(token: u64) {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        if let Some(Some(slot)) = reg.get_mut((token - 1) as usize) {
            slot.state = core::ptr::null_mut();
        }
    });
}

/// Read the saved `reladdr` for a receiver token (`((DR_intorel *) dest)->reladdr`).
fn receiver_reladdr(token: u64) -> ObjectAddress {
    RECEIVERS.with(|r| {
        let reg = r.borrow();
        reg.get((token - 1) as usize)
            .and_then(|s| s.as_ref())
            .map(|s| s.reladdr)
            .unwrap_or(InvalidObjectAddress)
    })
}

/// Recover the live `IntoRelStateData` for a bound receiver token (the C
/// `(DR_intorel *) self`).
///
/// SAFETY: the pointer is the arena-allocated state `intorel_startup` bound for
/// the synchronous executor run; it is valid until `intorel_shutdown` clears it.
fn receiver_state<'mcx>(token: u64) -> &'mcx mut IntoRelStateData<'mcx> {
    let ptr = RECEIVERS.with(|r| {
        let reg = r.borrow();
        reg.get((token - 1) as usize)
            .and_then(|s| s.as_ref())
            .map(|s| s.state)
            .unwrap_or(core::ptr::null_mut())
    });
    if ptr.is_null() {
        panic!("backend-commands-createas: intorel callback on an unbound DR_intorel receiver");
    }
    unsafe { &mut *(ptr as *mut IntoRelStateData<'mcx>) }
}

// ===========================================================================
// build_coldef_checked — the shared column-list step (createas.c)
// ===========================================================================

/// Build one `ColumnDef` from a pre-cooked `(name, typeOid, typmod, collation)`
/// and run the indeterminate-collation double-check, exactly as the two C
/// column-list loops do at `col = makeColumnDef(...)` + the `!OidIsValid(
/// col->collOid) && type_is_collatable(col->typeName->typeOid)` test.
fn build_coldef_checked<'mcx>(
    mcx: Mcx<'mcx>,
    colname: &str,
    type_oid: Oid,
    typmod: i32,
    collation: Oid,
) -> PgResult<Node<'mcx>> {
    let col = backend_nodes_core::makefuncs::make_column_def(mcx, colname, type_oid, typmod, collation)?;

    /*
     * It's possible that the column is of a collatable type but the collation
     * could not be resolved, so double-check.  (We must check this here because
     * DefineRelation would adopt the type's default collation rather than
     * complaining.)
     */
    if !oid_is_valid(col.collOid) {
        // col->typeName->typeOid — makeColumnDef always sets a TypeName built by
        // makeTypeNameFromOid(typeOid, typmod).
        let typeoid = col
            .typeName
            .as_ref()
            .map(|tn| tn.typeOid)
            .unwrap_or(InvalidOid);
        if backend_utils_cache_lsyscache_seams::type_is_collatable::call(typeoid)? {
            let colname = col.colname.as_ref().map(|s| s.as_str()).unwrap_or("");
            let fmt = backend_utils_adt_format_type_seams::format_type_be::call(mcx, typeoid)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INDETERMINATE_COLLATION)
                .errmsg(format!(
                    "no collation was derived for column \"{}\" with collatable type {}",
                    colname,
                    fmt.as_str()
                ))
                .errhint("Use the COLLATE clause to set the collation explicitly.")
                .into_error());
        }
    }

    Ok(Node::mk_column_def(mcx, col)?)
}

// ===========================================================================
// create_ctas_internal (createas.c:81-145)
// ===========================================================================

/// `create_ctas_internal(attrList, into)` — build the destination relation's
/// definition from a list of `ColumnDef`s.
///
/// The C body fakes up a `CreateStmt`, calls `DefineRelation`, does
/// `CommandCounterIncrement`, the TOAST-options validation +
/// `NewRelationCreateToastTable`, and (for a matview) `copyObject(into->
/// viewQuery)` + `StoreViewQuery` + `CommandCounterIncrement`. Those steps have
/// no createas-observable intermediate state and share `create->options` / the
/// new OID, so they cross as the `create_ctas_relation` seam (the
/// tablecmds/view owners are not ported).
fn create_ctas_internal<'mcx>(
    mcx: Mcx<'mcx>,
    attr_list: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    into: &IntoClause<'mcx>,
) -> PgResult<ObjectAddress> {
    /* This code supports both CREATE TABLE AS and CREATE MATERIALIZED VIEW */
    let is_matview = into.viewQuery.is_some();
    let relkind = if is_matview {
        RELKIND_MATVIEW
    } else {
        RELKIND_RELATION
    };

    backend_commands_createas_seams::create_ctas_relation::call(
        mcx,
        into.clone_in(mcx)?,
        attr_list,
        relkind,
        is_matview,
    )
}

// ===========================================================================
// create_ctas_nodata (createas.c:154-216)
// ===========================================================================

/// `create_ctas_nodata(tlist, into)` — build the CTAS / matview definition from
/// the SELECT or view-definition targetlist when `WITH NO DATA` is used.
fn create_ctas_nodata<'mcx>(
    mcx: Mcx<'mcx>,
    query: &types_nodes::copy_query::Query<'mcx>,
    into: &IntoClause<'mcx>,
) -> PgResult<ObjectAddress> {
    /*
     * Build list of ColumnDefs from non-junk elements of the tlist.  If a column
     * name list was specified in CREATE TABLE AS, override the column names in
     * the query.  (Too few column names are OK, too many are not.)
     */
    let mut attr_list: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>> = mcx::vec_with_capacity_in(mcx, 0)?;
    let mut lc = into.colNames.iter();

    for tle in query.targetList.iter() {
        if !tle.resjunk {
            /* if (lc) { colname = strVal(lfirst(lc)); lc = lnext(...); }
             * else colname = tle->resname; */
            let colname: &str = match lc.next() {
                Some(name) => node_string_value(name),
                None => tle.resname.as_ref().map(|s| s.as_str()).unwrap_or(""),
            };

            // exprType / exprTypmod / exprCollation((Node *) tle->expr).
            let info = match tle.expr.as_ref() {
                Some(expr) => backend_nodes_nodeFuncs_seams::expr_type_info::call(expr)?,
                None => backend_nodes_nodeFuncs_seams::ExprTypeInfo {
                    typid: InvalidOid,
                    typmod: -1,
                    collation: InvalidOid,
                },
            };

            let col = build_coldef_checked(mcx, colname, info.typid, info.typmod, info.collation)?;
            attr_list.push(mcx::alloc_in(mcx, col)?);
        }
    }

    if lc.next().is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("too many column names were specified")
            .into_error());
    }

    /* Create the relation definition using the ColumnDef list */
    create_ctas_internal(mcx, attr_list, into)
}

/// `strVal(lfirst(lc))` — the string value of a `colNames` list element (a
/// `String`/`T_String` value node).
fn node_string_value<'a>(node: &'a PgBox<'_, Node<'_>>) -> &'a str {
    match node.as_string() {
        Some(s) => s.sval.as_str(),
        None => panic!("createas: into->colNames element is not a String value node"),
    }
}

// ===========================================================================
// ExecCreateTableAs (createas.c:222-364)
// ===========================================================================

/// `ExecCreateTableAs(pstate, stmt, params, queryEnv, qc)` — execute a
/// CREATE TABLE AS command. `query_string` is `pstate->p_sourcetext`. Returns
/// the created relation's address and the (possibly filled) `QueryCompletion`.
pub fn ExecCreateTableAs<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateTableAsStmt<'mcx>,
    query_string: &str,
    params: ParamListInfo,
    mut qc: Option<QueryCompletion>,
) -> PgResult<(ObjectAddress, Option<QueryCompletion>)> {
    /* query = castNode(Query, stmt->query); into = stmt->into; */
    let query = stmt_query(stmt);
    let into = stmt_into(stmt);
    let is_matview = into.viewQuery.is_some();
    let mut do_refresh = false;

    /* Check if the relation exists or not */
    if CreateTableAsRelExists(mcx, stmt)? {
        return Ok((InvalidObjectAddress, qc));
    }

    /*
     * Create the tuple receiver object and insert info it will need
     */
    let dest = CreateIntoRelDestReceiver(Some(into))?;

    // The dest router returns a global DestReceiverHandle; the createas registry
    // token (the one threaded back to the intorel_* callbacks as `state`) is the
    // owner token the receiver was registered with. Recover it so the run-binding
    // and reladdr read-back key the same slot the callbacks see.
    let token = backend_tcop_dest::dest_receiver_state_token(dest);

    /*
     * Query contained by CTAS needs to be jumbled if requested, then the
     * post_parse_analyze_hook.
     */
    backend_commands_createas_seams::jumble_and_post_analyze::call(mcx, query, query_string)?;

    /*
     * The contained Query could be a SELECT, or an EXECUTE utility command.  If
     * the latter, we just pass it off to ExecuteQuery.
     */
    if query.commandType == CmdType::CMD_UTILITY && query_is_execute_stmt(query) {
        let estmt = execute_stmt_of(mcx, query)?;

        debug_assert!(!is_matview); /* excluded by syntax */
        // Bind the receiver's run state (self->into) before ExecuteQuery drives
        // the executor (which invokes intorel_startup).
        receiver_setup_run(token, mcx, into.clone_in(mcx)?)?;
        qc = backend_commands_createas_seams::execute_query::call(
            mcx,
            estmt,
            into.clone_in(mcx)?,
            query_string,
            params,
            dest,
            qc,
        )?;

        /* get object address that intorel_startup saved for us */
        let address = receiver_reladdr(token);
        return Ok((address, qc));
    }
    debug_assert_eq!(query.commandType, CmdType::CMD_SELECT);

    /*
     * For materialized views, always skip data during table creation, and use
     * REFRESH instead (see below).
     */
    let mut into_owned = into.clone_in(mcx)?;
    if is_matview {
        do_refresh = !into_owned.skipData;
        into_owned.skipData = true;
    }
    let into = &into_owned;

    let address;
    if into.skipData {
        /*
         * If WITH NO DATA was specified, do not go through the rewriter, planner
         * and executor.  Just define the relation using a code path similar to
         * CREATE VIEW.
         */
        address = create_ctas_nodata(mcx, query, into)?;

        /*
         * For materialized views, reuse the REFRESH logic, which locks down
         * security-restricted operations and restricts the search_path.
         */
        if do_refresh {
            let (_addr, new_qc) = backend_commands_matview_seams::RefreshMatViewByOid::call(
                address.objectId,
                true,
                false,
                false,
                query_string.into(),
                qc.map(to_matview_qc),
            )?;
            qc = new_qc.map(from_matview_qc);
        }
    } else {
        debug_assert!(!is_matview);

        // Bind the receiver's run state (self->into) before the executor runs
        // (which invokes intorel_startup).
        receiver_setup_run(token, mcx, into.clone_in(mcx)?)?;

        /*
         * Run the rule rewriter, plan the query, and execute it with output
         * redirected to our DR_intorel receiver (createas.c 300-361). The
         * rewriter/planner take the trimmed handle Query while CTAS carries the
         * canonical arena Query, so the whole pipeline crosses one seam (see
         * `run_ctas_executor`).
         */
        qc = backend_commands_createas_seams::run_ctas_executor::call(
            mcx,
            query.clone_in(mcx)?,
            into.clone_in(mcx)?,
            query_string,
            params,
            dest,
            qc,
        )?;

        /* get object address that intorel_startup saved for us */
        address = receiver_reladdr(token);
    }

    Ok((address, qc))
}

/// Convert the crate's `types_portal::QueryCompletion` to the matview seam's
/// `types_matview::QueryCompletion` (same `{commandTag, nprocessed}` shape).
fn to_matview_qc(qc: QueryCompletion) -> types_matview::QueryCompletion {
    types_matview::QueryCompletion {
        commandTag: types_core::cmdtag::CommandTag(qc.commandTag),
        nprocessed: qc.nprocessed,
    }
}

/// The inverse of [`to_matview_qc`].
fn from_matview_qc(qc: types_matview::QueryCompletion) -> QueryCompletion {
    QueryCompletion {
        commandTag: qc.commandTag.0,
        nprocessed: qc.nprocessed,
    }
}

/// `castNode(Query, stmt->query)`.
fn stmt_query<'a, 'mcx>(stmt: &'a CreateTableAsStmt<'mcx>) -> &'a types_nodes::copy_query::Query<'mcx> {
    match stmt.query.as_deref().and_then(Node::as_query) {
        Some(q) => q,
        _ => panic!("createas: stmt->query is not a Query"),
    }
}

/// `stmt->into` — the `IntoClause` target spec (never NULL for a CTAS stmt).
fn stmt_into<'a, 'mcx>(stmt: &'a CreateTableAsStmt<'mcx>) -> &'a IntoClause<'mcx> {
    match stmt.into.as_deref().and_then(Node::as_intoclause) {
        Some(into) => into,
        _ => panic!("createas: stmt->into is not an IntoClause"),
    }
}

/// `query->commandType == CMD_UTILITY && IsA(query->utilityStmt, ExecuteStmt)`.
fn query_is_execute_stmt(query: &types_nodes::copy_query::Query<'_>) -> bool {
    query.utilityStmt.as_deref().is_some_and(|n| n.is_executestmt())
}

/// `castNode(ExecuteStmt, query->utilityStmt)` — a deep copy of the contained
/// `ExecuteStmt` (the `execute_query` seam takes it by value).
fn execute_stmt_of<'mcx>(
    mcx: Mcx<'mcx>,
    query: &types_nodes::copy_query::Query<'mcx>,
) -> PgResult<types_nodes::ddlnodes::ExecuteStmt<'mcx>> {
    match query.utilityStmt.as_deref().and_then(Node::as_executestmt) {
        Some(estmt) => estmt.clone_in(mcx),
        _ => panic!("createas: query->utilityStmt is not an ExecuteStmt"),
    }
}

// ===========================================================================
// GetIntoRelEFlags (createas.c:374-383)
// ===========================================================================

/// `GetIntoRelEFlags(intoClause)` — compute executor flags needed for CTAS.
/// Exported because EXPLAIN and PREPARE need it too.
pub fn GetIntoRelEFlags(into_clause: &IntoClause<'_>) -> i32 {
    let mut flags: i32 = 0;
    if into_clause.skipData {
        flags |= EXEC_FLAG_WITH_NO_DATA;
    }
    flags
}

// ===========================================================================
// CreateTableAsRelExists (createas.c:392-430)
// ===========================================================================

/// `CreateTableAsRelExists(ctas)` — check existence of the relation pending
/// creation. Returns `true` if it already exists (and IF NOT EXISTS allows
/// skipping); raises the duplicate-table error when IF NOT EXISTS was not given.
pub fn CreateTableAsRelExists<'mcx>(
    mcx: Mcx<'mcx>,
    ctas: &CreateTableAsStmt<'mcx>,
) -> PgResult<bool> {
    let into = stmt_into(ctas);
    let rel_node = match into.rel.as_deref().and_then(Node::as_rangevar) {
        Some(rv) => rv,
        _ => panic!("createas: into->rel is not a RangeVar"),
    };

    /* nspid = RangeVarGetCreationNamespace(into->rel); */
    let access_rv = AccessRangeVar {
        catalogname: rel_node.catalogname.as_ref().map(|s| s.as_str().to_string()),
        schemaname: rel_node.schemaname.as_ref().map(|s| s.as_str().to_string()),
        relname: rel_node.relname.as_ref().map(|s| s.as_str().to_string()).unwrap_or_default(),
        inh: rel_node.inh,
        relpersistence: rel_node.relpersistence as u8,
        location: rel_node.location,
    };
    let nspid = backend_catalog_namespace::RangeVarGetCreationNamespace(mcx, &access_rv)?;

    /* oldrelid = get_relname_relid(into->rel->relname, nspid); */
    let relname = rel_node.relname.as_ref().map(|s| s.as_str()).unwrap_or("");
    let oldrelid = backend_utils_cache_lsyscache_seams::get_relname_relid::call(relname, nspid)?;
    if oid_is_valid(oldrelid) {
        if !ctas.if_not_exists {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_TABLE)
                .errmsg(format!("relation \"{relname}\" already exists"))
                .into_error());
        }

        /*
         * The relation exists and IF NOT EXISTS has been specified. If we are in
         * an extension script, insist that the pre-existing object be a member
         * of the extension, to avoid security risks.
         *
         * ObjectAddressSet(address, RelationRelationId, oldrelid);
         * checkMembershipInCurrentExtension(&address);
         */
        let address = ObjectAddress {
            classId: RELATION_RELATION_ID,
            objectId: oldrelid,
            objectSubId: 0,
        };
        backend_catalog_pg_depend::checkMembershipInCurrentExtension(mcx, &address)?;

        /* OK to skip */
        ereport(NOTICE)
            .errcode(ERRCODE_DUPLICATE_TABLE)
            .errmsg(format!("relation \"{relname}\" already exists, skipping"))
            .finish(here("CreateTableAsRelExists"))?;
        return Ok(true);
    }

    /* Relation does not exist, it can be created */
    Ok(false)
}

// ===========================================================================
// CreateIntoRelDestReceiver (createas.c:439-453)
// ===========================================================================

/// `CreateIntoRelDestReceiver(intoClause)` — build the `DR_intorel`
/// `DestReceiver` and register it into the tcop-dest router. `intoClause` may be
/// `None` (the `CreateDestReceiver()` deferred-`into` contract); `into` is then
/// filled later by the executor's first use (not exercised by this crate's own
/// callers, which always pass it).
pub fn CreateIntoRelDestReceiver<'mcx>(
    into: Option<&IntoClause<'mcx>>,
) -> PgResult<DestReceiverHandle> {
    let _ = into; // The `into` is recovered per-startup from the executor's
                  // ExecCreateTableAs caller; the receiver slot holds only the
                  // run-bound state. C stores `self->into` here, but the owned
                  // model threads the live `IntoClause` into `intorel_startup`
                  // via the run-bound state set up by ExecCreateTableAs.
    let token = receiver_register();
    Ok(backend_tcop_dest::register_dest_receiver(
        CommandDest::IntoRel,
        backend_tcop_dest::ReceiverVtable {
            rStartup: intorel_startup,
            receiveSlot: intorel_receive,
            rShutdown: intorel_shutdown,
        },
        token,
    ))
}

// ===========================================================================
// intorel_startup (createas.c:458-577)
// ===========================================================================

/// `intorel_startup(self, operation, typeinfo)` — executor startup: build the
/// destination's column list from the pre-cooked `TupleDesc`, create the table,
/// open it, reject RLS, tentatively mark a matview populated, and fill the
/// private `DR_intorel` fields.
fn intorel_startup<'mcx>(
    mcx: Mcx<'mcx>,
    state: u64,
    _operation: CmdType,
    typeinfo: &TupleDescData<'mcx>,
) -> PgResult<()> {
    // myState->into — the live IntoClause the run was set up with. We recover it
    // from the run-bound state stub ExecCreateTableAs installs (see
    // intorel_run_setup); the receiver slot is bound to a fresh state holding it.
    let into = intorel_startup_into(state);

    /* This code supports both CREATE TABLE AS and CREATE MATERIALIZED VIEW */
    let is_matview = into.viewQuery.is_some();

    /*
     * Build column definitions using "pre-cooked" type and collation info.  If a
     * column name list was specified in CREATE TABLE AS, override the column
     * names derived from the query.
     */
    let mut attr_list: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>> = mcx::vec_with_capacity_in(mcx, 0)?;
    let mut lc = into.colNames.iter();

    for attnum in 0..typeinfo.natts as usize {
        let attribute = typeinfo.attr(attnum);
        let colname_buf;
        let colname: &str = match lc.next() {
            Some(name) => node_string_value(name),
            None => {
                colname_buf = core::str::from_utf8(attribute.attname.name_str())
                    .unwrap_or("")
                    .to_string();
                &colname_buf
            }
        };

        let col = build_coldef_checked(
            mcx,
            colname,
            attribute.atttypid,
            attribute.atttypmod,
            attribute.attcollation,
        )?;
        attr_list.push(mcx::alloc_in(mcx, col)?);
    }

    if lc.next().is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("too many column names were specified")
            .into_error());
    }

    /* Actually create the target table */
    let into_relation_addr = create_ctas_internal(mcx, attr_list, into)?;

    /* Finally we can open the target table */
    let into_relation_desc =
        backend_access_table_table::table_open(mcx, into_relation_addr.objectId, AccessExclusiveLock)?;

    /*
     * Make sure the constructed table does not have RLS enabled.
     *
     * check_enable_rls() ereports itself for invalid requests; we don't support
     * RLS here, so reject RLS_ENABLED.
     */
    if backend_utils_misc_more_seams::check_enable_rls::call(
        into_relation_addr.objectId,
        InvalidOid,
        false,
    )? == RLS_ENABLED
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("policies not yet implemented for this command")
            .into_error());
    }

    /*
     * Tentatively mark the target as populated, if it's a matview and we're
     * going to fill it; otherwise, no change needed.
     */
    if is_matview && !into.skipData {
        backend_commands_matview_seams::SetMatViewPopulatedState::call(
            into_relation_addr.objectId,
            true,
        )?;
    }

    /* Fill private fields of myState for use by later routines */
    let output_cid = backend_access_transam_xact::GetCurrentCommandId(true)?;
    let ti_options = TABLE_INSERT_SKIP_FSM;

    /*
     * If WITH NO DATA is specified, there is no need to set up the state for bulk
     * inserts as there are no tuples to insert.
     */
    let bistate = if !into.skipData {
        Some(mcx::alloc_in(mcx, backend_access_heap_heapam::GetBulkInsertState()?)?)
    } else {
        None
    };

    /*
     * Valid smgr_targblock implies something already wrote to the relation. This
     * may be harmless, but this function hasn't planned for it.
     *
     * C: Assert(RelationGetTargetBlock(intoRelationDesc) == InvalidBlockNumber).
     * `RelationGetTargetBlock` reads `rd_smgr->smgr_targblock`, which the trimmed
     * relcache projection does not carry; the C check is a debug-only `Assert`
     * (a no-op in release), so it is elided here — no logic depends on it.
     */

    // myState->reladdr = intoRelationAddr (read by ExecCreateTableAs after the
    // run); myState->rel/output_cid/ti_options/bistate via the bound state.
    receiver_set_reladdr(state, into_relation_addr);

    let st = receiver_state(state);
    st.rel = Some(into_relation_desc);
    st.output_cid = output_cid;
    st.ti_options = ti_options;
    st.bistate = bistate;

    Ok(())
}

/// Recover the live `IntoClause` the run was set up with, from the bound state.
fn intorel_startup_into<'mcx>(state: u64) -> &'mcx IntoClause<'mcx> {
    &receiver_state(state).into
}

// ===========================================================================
// intorel_receive (createas.c:582-608)
// ===========================================================================

/// `intorel_receive(slot, self)` — receive one tuple.
fn intorel_receive<'mcx>(
    mcx: Mcx<'mcx>,
    state: u64,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    let st = receiver_state::<'mcx>(state);

    /* Nothing to insert if WITH NO DATA is specified. */
    if !st.into.skipData {
        /*
         * Note that the input slot might not be of the type of the target
         * relation. That's supported by table_tuple_insert(), but slightly less
         * efficient than inserting with the right slot - but the alternative
         * would be to copy into a slot of the right type, which would not be
         * cheap either.
         */
        let rel = st
            .rel
            .as_ref()
            .expect("intorel_receive: DR_intorel relation is open");
        let bistate = st.bistate.as_deref_mut();
        backend_access_table_tableam::table_tuple_insert(
            mcx,
            rel,
            slot,
            st.output_cid,
            st.ti_options,
            bistate,
        )?;
    }

    /* We know this is a newly created relation, so there are no indexes */
    Ok(true)
}

// ===========================================================================
// intorel_shutdown (createas.c:613-628)
// ===========================================================================

/// `intorel_shutdown(self)` — executor end: flush bulk inserts (if any) and
/// close the relation, keeping the lock until commit.
fn intorel_shutdown<'mcx>(mcx: Mcx<'mcx>, state: u64) -> PgResult<()> {
    let _ = mcx;
    let st = receiver_state::<'mcx>(state);

    if !st.into.skipData {
        if let Some(bistate) = st.bistate.as_deref_mut() {
            backend_access_heap_heapam::FreeBulkInsertState(bistate);
        }
        let rel = st
            .rel
            .as_ref()
            .expect("intorel_shutdown: DR_intorel relation is open");
        backend_commands_createas_seams::table_finish_bulk_insert::call(rel, st.ti_options)?;
    }

    /* close rel, but keep lock until commit */
    if let Some(rel) = st.rel.take() {
        backend_access_table_table::table_close(rel, NoLock)?;
    }

    /* myState->rel = NULL: release the run-bound state pointer. */
    receiver_unbind(state);

    Ok(())
}

// ===========================================================================
// intorel_destroy (createas.c:633-637)
// ===========================================================================

/// `intorel_destroy(self)` — release the `DestReceiver` object (C: `pfree(self)`).
///
/// In the owned model the `DR_intorel` is a [`ReceiverSlot`] in the per-backend
/// registry plus the arena-allocated [`IntoRelStateData`]; the latter is freed
/// when its query context resets (as the C `palloc` is), and the slot is a small
/// fixed entry, so there is nothing to `pfree` here. The dest-router vtable does
/// not carry `rDestroy` (it is the owner's teardown path; see
/// `backend_tcop_dest`), so this is not wired into the vtable — it exists for
/// C-function parity and to document the no-op.
#[allow(dead_code)]
fn intorel_destroy(_self: DestReceiverHandle) {
    /* pfree(self): the registry slot + arena state are reclaimed by context
     * reset; nothing to free explicitly. */
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// `get_into_rel_eflags` inward seam impl over the trimmed `parsestmt::
/// IntoClause` the prepare/explain callers carry. Only `skipData` matters.
fn get_into_rel_eflags_seam(into: &types_nodes::parsestmt::IntoClause<'_>) -> PgResult<i32> {
    let mut flags: i32 = 0;
    if into.skipData {
        flags |= EXEC_FLAG_WITH_NO_DATA;
    }
    Ok(flags)
}

/// `exec_create_table_as` inward seam impl.
fn exec_create_table_as_seam<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateTableAsStmt<'mcx>,
    query_string: &str,
    params: ParamListInfo,
    qc: Option<QueryCompletion>,
) -> PgResult<(ObjectAddress, Option<QueryCompletion>)> {
    ExecCreateTableAs(mcx, stmt, query_string, params, qc)
}

/// Utility-dispatcher entry for `ExecCreateTableAs` (utility.c
/// `ProcessUtilitySlow`'s `T_CreateTableAsStmt` arm). Adapts the dispatch shape
/// (`pstate`, an untyped `&Node`, and a `&mut QueryCompletion` out-param) to the
/// crate's `ExecCreateTableAs`: `query_string` is `pstate->p_sourcetext`, and the
/// returned `QueryCompletion` is written back into the dispatcher's slot.
fn exec_create_table_as_utility<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut types_nodes::parsestmt::ParseState<'mcx>,
    stmt: &Node<'mcx>,
    params: ParamListInfo,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<ObjectAddress> {
    let ctas = match stmt.as_createtableasstmt() {
        Some(c) => c,
        None => panic!("exec_create_table_as: utilityStmt is not a CreateTableAsStmt"),
    };
    let query_string = pstate
        .p_sourcetext
        .as_ref()
        .map(|s| s.as_str())
        .unwrap_or("");

    let qc_in = qc.as_deref().map(|q| QueryCompletion {
        commandTag: q.commandTag,
        nprocessed: q.nprocessed,
    });
    let (address, qc_out) = ExecCreateTableAs(mcx, ctas, query_string, params, qc_in)?;
    if let (Some(slot), Some(filled)) = (qc, qc_out) {
        *slot = filled;
    }
    Ok(address)
}

/// `create_table_as_rel_exists` inward seam impl.
fn create_table_as_rel_exists_seam<'mcx>(
    mcx: Mcx<'mcx>,
    ctas: &CreateTableAsStmt<'mcx>,
) -> PgResult<bool> {
    CreateTableAsRelExists(mcx, ctas)
}

/// `create_into_rel_dest_receiver` inward seam impl.
fn create_into_rel_dest_receiver_seam<'mcx>(
    into: Option<&IntoClause<'mcx>>,
) -> PgResult<DestReceiverHandle> {
    CreateIntoRelDestReceiver(into)
}

/// `CreateIntoRelDestReceiver(into)` + bind the run-state with `into` for callers
/// (EXPLAIN of CREATE TABLE AS) that drive the executor themselves. Mirrors what
/// `ExecCreateTableAs` does (create receiver, then `receiver_setup_run`) but
/// without running the query — the caller's own `ExecutorStart`/`ExecutorRun`
/// invokes `intorel_startup`, which recovers `into` from the bound run-state.
pub fn CreateIntoRelDestReceiverSetup<'mcx>(
    mcx: Mcx<'mcx>,
    into: &IntoClause<'mcx>,
) -> PgResult<DestReceiverHandle> {
    let dest = CreateIntoRelDestReceiver(Some(into))?;
    let token = backend_tcop_dest::dest_receiver_state_token(dest);
    receiver_setup_run(token, mcx, into.clone_in(mcx)?)?;
    Ok(dest)
}

/// `create_into_rel_dest_receiver_setup` inward seam impl over the trimmed
/// `parsestmt::IntoClause` the EXPLAIN driver carries. The full createas-owned
/// `ddlnodes::IntoClause` is recovered from the opaque node payload (`into.node`),
/// the same trim the EXPLAIN driver builds in `ExplainOneUtility`. Returns the
/// receiver handle's raw value for the EXPLAIN executor-start.
fn create_into_rel_dest_receiver_setup_seam<'mcx>(
    mcx: Mcx<'mcx>,
    into: &types_nodes::parsestmt::IntoClause<'mcx>,
) -> PgResult<u64> {
    let ddl_into = into
        .node
        .as_intoclause()
        .expect("create_into_rel_dest_receiver_setup: into.node is not an IntoClause");
    Ok(CreateIntoRelDestReceiverSetup(mcx, ddl_into)?.0)
}

/// Install this crate's inward seams. Wired into `seams-init`.
pub fn init_seams() {
    backend_commands_createas_seams::get_into_rel_eflags::set(get_into_rel_eflags_seam);
    backend_commands_createas_seams::exec_create_table_as::set(exec_create_table_as_seam);
    backend_commands_createas_seams::create_table_as_rel_exists::set(create_table_as_rel_exists_seam);
    backend_commands_createas_seams::create_into_rel_dest_receiver::set(
        create_into_rel_dest_receiver_seam,
    );
    backend_commands_createas_seams::create_into_rel_dest_receiver_setup::set(
        create_into_rel_dest_receiver_setup_seam,
    );
    // The utility dispatcher (ProcessUtilitySlow) reaches ExecCreateTableAs
    // through tcop-utility-out-seams; install the dispatch-shape adapter.
    backend_tcop_utility_out_seams::exec_create_table_as::set(exec_create_table_as_utility);
}
