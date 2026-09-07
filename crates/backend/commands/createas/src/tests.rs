// Witnesses for createas.c error arms that have no SQL-level reach.

use std::sync::Once;

use mcx::{Mcx, MemoryContext, PgVec};
use types_core::{InvalidOid, RELPERSISTENCE_PERMANENT};
use types_error::{PgResult, ERRCODE_INTERNAL_ERROR, ERROR};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{ObjectType, Query};
use types_nodes::primnodes::RangeVar;
use types_nodes::rawnodes::{CreateTableAsStmt, IntoClause};
use types_nodes::Node;
use types_portal::{ParamListHandle, QueryEnvHandle};

static INSTALL: Once = Once::new();

// The rewriter never returns anything but one SELECT for a CTAS query
// (rules on SELECT are restricted to single INSTEAD SELECT actions), so the
// createas.c:318 arm is only reachable through a rewrite defect; the stub
// plays that defect by handing the query back twice.
fn rewrite_twice<'mcx>(mcx: Mcx<'mcx>, query: Query<'mcx>) -> PgResult<PgVec<'mcx, Query<'mcx>>> {
    let mut v = PgVec::new_in(mcx);
    v.push(Query { commandType: CmdType::CMD_SELECT, canSetTag: true, ..Default::default() });
    v.push(query);
    Ok(v)
}

fn install() {
    INSTALL.call_once(|| {
        // CreateTableAsRelExists: explicit schema resolves, target absent.
        syscache_seams::lookup_pg_namespace_oid_by_name::set(|_| Ok(2200));
        syscache_seams::lookup_pg_class_relid_by_name::set(|_, _| Ok(InvalidOid));
        rewrite_handler_seams::query_rewrite::set(rewrite_twice);
    });
}

fn ctas_stmt<'mcx>(mcx: Mcx<'mcx>) -> CreateTableAsStmt<'mcx> {
    let rel = Node::mk(
        mcx,
        RangeVar {
            schemaname: Some("public"),
            relname: Some("ctas_rewrite_witness"),
            inh: true,
            relpersistence: RELPERSISTENCE_PERMANENT,
            ..Default::default()
        },
    )
    .unwrap();
    let into = Node::mk(mcx, IntoClause { rel: Some(rel), ..Default::default() }).unwrap();
    let query = Node::mk(
        mcx,
        Query { commandType: CmdType::CMD_SELECT, canSetTag: true, ..Default::default() },
    )
    .unwrap();
    CreateTableAsStmt {
        query: Some(query),
        into: Some(into),
        objtype: ObjectType::OBJECT_TABLE,
        ..Default::default()
    }
}

// createas.c:317-318: `if (list_length(rewritten) != 1) elog(ERROR,
// "unexpected rewrite result for CREATE TABLE AS SELECT");` — a structured
// XX000 error, never a process abort.
#[test]
fn ctas_rewrite_result_not_single_query_is_internal_error() {
    install();
    let ctx = MemoryContext::new("createas-rewrite-witness");
    let mcx = ctx.mcx();
    let stmt = ctas_stmt(mcx);
    let err = crate::ExecCreateTableAs(
        mcx,
        &stmt,
        "CREATE TABLE public.ctas_rewrite_witness AS SELECT 1",
        ParamListHandle::NULL,
        QueryEnvHandle::NULL,
        None,
    )
    .err()
    .expect("createas.c:318 raises elog(ERROR) on a non-single rewrite result");
    assert_eq!(err.level(), ERROR);
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);
    assert_eq!(err.message(), "unexpected rewrite result for CREATE TABLE AS SELECT");
}
