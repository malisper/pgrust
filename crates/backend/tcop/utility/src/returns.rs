use std::cell::Cell;
use std::rc::Rc;

use mcx::{Mcx, MemoryContext};
use types_error::PgResult;
use types_nodes::node_tree::Node;
use types_nodes::NodeTag;
use types_tuple::TupleDescData;

use crate::payload_gap;

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
                Box::leak(Box::new(MemoryContext::new("UtilityTupleDescs")));
            c.set(Some(m));
            m.mcx()
        }
    })
}

pub fn UtilityReturnsTuples(parsetree: Node<'_>) -> bool {
    use NodeTag::*;
    match parsetree.node_tag() {
        // C probes funcexpr->funcresulttype / the named portal / the prepared
        // statement; those land with functioncmds/portalcmds/prepare.c.
        T_CallStmt => payload_gap("UtilityReturnsTuples", "CallStmt"),
        T_FetchStmt => payload_gap("UtilityReturnsTuples", "FetchStmt"),
        T_ExecuteStmt => payload_gap("UtilityReturnsTuples", "ExecuteStmt"),
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
        T_CallStmt => payload_gap("UtilityTupleDescriptor", "CallStmt"),
        T_FetchStmt => payload_gap("UtilityTupleDescriptor", "FetchStmt"),
        T_ExecuteStmt => payload_gap("UtilityTupleDescriptor", "ExecuteStmt"),
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
