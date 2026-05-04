use std::collections::HashMap;

use pgrust_nodes::{SystemVarBinding, Value};

#[derive(Debug, Clone, Default)]
pub struct ExprEvalBindings {
    pub exec_params: HashMap<usize, Value>,
    pub initplan_values: HashMap<usize, Value>,
    pub external_params: HashMap<usize, Value>,
    pub outer_tuple: Option<Vec<Value>>,
    pub outer_system_bindings: Vec<SystemVarBinding>,
    pub grouping_ref_stack: Vec<Vec<usize>>,
    pub inner_tuple: Option<Vec<Value>>,
    pub inner_system_bindings: Vec<SystemVarBinding>,
    pub index_tuple: Option<Vec<Value>>,
    pub index_system_bindings: Vec<SystemVarBinding>,
    pub rule_old_tuple: Option<Vec<Value>>,
    pub rule_new_tuple: Option<Vec<Value>>,
}

pub fn merge_system_bindings(
    left: &[SystemVarBinding],
    right: &[SystemVarBinding],
) -> Vec<SystemVarBinding> {
    let mut merged = left.to_vec();
    for binding in right {
        if !merged
            .iter()
            .any(|existing| existing.varno == binding.varno)
        {
            merged.push(*binding);
        }
    }
    merged
}

pub fn set_outer_expr_bindings(
    bindings: &mut ExprEvalBindings,
    values: Vec<Value>,
    system_bindings: &[SystemVarBinding],
) {
    bindings.outer_tuple = Some(values);
    bindings.outer_system_bindings = system_bindings.to_vec();
}

pub fn set_inner_expr_bindings(
    bindings: &mut ExprEvalBindings,
    values: Vec<Value>,
    system_bindings: &[SystemVarBinding],
) {
    bindings.inner_tuple = Some(values);
    bindings.inner_system_bindings = system_bindings.to_vec();
}
