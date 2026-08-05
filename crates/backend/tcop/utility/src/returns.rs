use std::cell::Cell;
use std::rc::Rc;

use mcx::{Mcx, MemoryContext};
use types_error::PgResult;
use types_nodes::node_tree::Node;
use types_nodes::NodeTag;
use types_tuple::TupleDescData;

// Divergence from C (execmain::desc_mcx precedent): portal-held descriptors
// outlive the statement, so they live in a backend-lifetime aset, not
// CurrentMemoryContext.
fn desc_mcx() -> Mcx<'static> {
    thread_local! {
        static CTX: Cell<Option<&'static MemoryContext>> = const { Cell::new(None) };
    }
    CTX.with(|c| match c.get() {
        Some(m) => m.mcx(),
        None => {
            let m: &'static MemoryContext =
                ::mcx::session_root("UtilityTupleDescs");
            c.set(Some(m));
            m.mcx()
        }
    })
}

pub fn UtilityReturnsTuples(parsetree: Node<'_>) -> bool {
    use NodeTag::*;
    match parsetree.node_tag() {
        T_CallStmt => {
            let stmt = parsetree.as_call_stmt().unwrap();
            stmt.funcexpr
                .expect("CALL: analyzed CallStmt holds a FuncExpr")
                .funcresulttype
                == types_core::RECORDOID
        }
        T_FetchStmt => {
            let stmt = parsetree.as_fetch_stmt().unwrap();
            if stmt.ismove {
                return false;
            }
            match portalmem::GetPortalByName(stmt.portalname) {
                Some(portal) => portal.borrow().tupDesc.is_some(),
                None => false,
            }
        }
        T_ExecuteStmt => {
            let stmt = parsetree.as_execute_stmt().unwrap();
            let entry = prepare::FetchPreparedStatement(
                stmt.name.expect("EXECUTE has a name"),
                false,
            )
            .expect("throwError=false cannot fail");
            match entry {
                Some(e) => prepare::FetchPreparedStatementResultDesc(&e).is_some(),
                None => false,
            }
        }
        T_ExplainStmt => true,
        T_VariableShowStmt => true,
        _ => false,
    }
}

pub fn UtilityTupleDescriptor(
    parsetree: Node<'_>,
) -> PgResult<Option<Rc<TupleDescData<'static>>>> {
    use NodeTag::*;
    match parsetree.node_tag() {
        T_CallStmt => {
            let stmt = parsetree.as_call_stmt().unwrap();
            Ok(functioncmds::CallStmtResultDesc(desc_mcx(), stmt)?.map(Rc::new))
        }
        T_FetchStmt => {
            let stmt = parsetree.as_fetch_stmt().unwrap();
            if stmt.ismove {
                return Ok(None);
            }
            // C CreateTupleDescCopy; the Rc clone is the caller-owned copy.
            Ok(portalmem::GetPortalByName(stmt.portalname)
                .and_then(|portal| portal.borrow().tupDesc.clone()))
        }
        T_ExecuteStmt => {
            let stmt = parsetree.as_execute_stmt().unwrap();
            let entry = prepare::FetchPreparedStatement(
                stmt.name.expect("EXECUTE has a name"),
                false,
            )
            .expect("throwError=false cannot fail");
            Ok(entry.and_then(|e| prepare::FetchPreparedStatementResultDesc(&e)))
        }
        T_ExplainStmt => {
            let stmt = parsetree.as_explain_stmt().unwrap();
            Ok(Some(Rc::new(explain::ExplainResultDesc(desc_mcx(), stmt)?)))
        }
        T_VariableShowStmt => {
            let n = parsetree.as_variable_show_stmt().unwrap();
            Ok(Some(Rc::new(guc_funcs::GetPGVariableResultDesc(
                desc_mcx(),
                n.name.unwrap_or(""),
            )?)))
        }
        _ => Ok(None),
    }
}

// UtilityContainsQuery (utility.c).
pub fn UtilityContainsQuery<'mcx>(parsetree: Node<'mcx>) -> Option<Node<'mcx>> {
    let qry_node = match parsetree.node_tag() {
        NodeTag::T_DeclareCursorStmt => parsetree
            .as_declare_cursor_stmt()
            .expect("tag checked")
            .query
            .expect("analyzed DECLARE holds a Query"),
        NodeTag::T_ExplainStmt => parsetree
            .as_explain_stmt()
            .expect("tag checked")
            .query
            .expect("analyzed EXPLAIN holds a Query"),
        NodeTag::T_CreateTableAsStmt => parsetree
            .as_variant::<types_nodes::rawnodes::CreateTableAsStmt>()
            .expect("tag checked")
            .query
            .expect("analyzed CTAS holds a Query"),
        _ => return None,
    };
    let qry = qry_node.as_query().expect("analyzed statement holds a Query");
    if qry.commandType == types_nodes::nodes_enums::CmdType::CMD_UTILITY {
        return UtilityContainsQuery(qry.utilityStmt.expect("utility Query holds its stmt"));
    }
    Some(qry_node)
}
