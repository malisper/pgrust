use types_nodes::Node;

seam_core::seam!(
    pub fn contain_var_clause<'mcx>(node: Node<'mcx>) -> bool
);
