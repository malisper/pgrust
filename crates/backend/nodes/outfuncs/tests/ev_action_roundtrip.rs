//! Byte-exactness gate: live-PG-18.3-captured pg_rewrite.ev_action strings
//! must survive stringToNode -> nodeToString unchanged.

use mcx::MemoryContext;

const EV_ACTION_V1_JOIN_WHERE: &str = include_str!("captures/ev_action_v1.txt");
const EV_ACTION_V2_NESTED: &str = include_str!("captures/ev_action_v2.txt");
const EV_ACTION_VAGG_GROUP: &str = include_str!("captures/ev_action_vagg.txt");
const EV_ACTION_VCTE: &str = include_str!("captures/ev_action_vcte.txt");
const EV_ACTION_VSUB: &str = include_str!("captures/ev_action_vsub.txt");

fn roundtrip(capture: &str) {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let node = readfuncs::stringToNode(mcx, capture).unwrap();
    let written = outfuncs::nodeToString(mcx, node).unwrap();
    assert_eq!(written.as_str(), capture);
}

#[test]
fn join_where_view_rule_roundtrips() {
    roundtrip(EV_ACTION_V1_JOIN_WHERE);
}

#[test]
fn nested_view_rule_roundtrips() {
    roundtrip(EV_ACTION_V2_NESTED);
}

#[test]
fn group_by_aggregate_view_rule_roundtrips() {
    roundtrip(EV_ACTION_VAGG_GROUP);
}

#[test]
fn cte_view_rule_roundtrips() {
    roundtrip(EV_ACTION_VCTE);
}

#[test]
fn sublink_view_rule_roundtrips() {
    roundtrip(EV_ACTION_VSUB);
}
