use ::mcx::MemoryContext;
use ::types_core::primitive::Oid;

#[test]
fn accepts_null_node_rendering() {
    read_seams::string_to_node_opt::set(nodes_core::read::string_to_node_opt);

    let ctx = MemoryContext::new("pg_get_expr_null_node");
    let result = ruleutils::pg_get_expr_worker(ctx.mcx(), "<>", Oid::default(), 0)
        .unwrap()
        .unwrap();

    assert_eq!(result.as_str(), "");

    common_relation_seams::try_relation_open::set(|_mcx, _relid, _lockmode| Ok(None));
    let missing_relation =
        ruleutils::pg_get_expr_worker(ctx.mcx(), "<>", Oid::from(999_999_u32), 0).unwrap();

    assert!(missing_relation.is_none());
}
