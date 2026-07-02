use std::rc::Rc;

use types_error::PgResult;
use types_nodes::node_tree::Node;
use types_nodes::NodeTag;
use types_tuple::TupleDescData;

use crate::payload_gap;

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
        // ExplainResultDesc / GetPGVariableResultDesc live with explain.c and
        // guc.c's SHOW surface.
        T_ExplainStmt => panic!(
            "UtilityTupleDescriptor (utility.c:2115): ExplainResultDesc not ported (explain lane)"
        ),
        T_VariableShowStmt => panic!(
            "UtilityTupleDescriptor (utility.c:2118): GetPGVariableResultDesc not ported (guc SHOW lane)"
        ),
        _ => Ok(None),
    }
}
