use types_nodes::Node;

seam_core::seam!(
    // contain_var_clause (var.c): clauses.c predicates consume it; the direct
    // edge would cycle (vars depends on clauses for the walker engine).
    pub fn contain_var_clause<'mcx>(node: Node<'mcx>) -> bool
);
