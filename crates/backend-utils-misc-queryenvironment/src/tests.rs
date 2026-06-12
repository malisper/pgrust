//! Unit tests for the query environment port.

extern crate std;

use super::*;
use alloc::string::ToString;
use alloc::vec::Vec;
use types_tuple::access::ENR_NAMED_TUPLESTORE;
use types_tuple::heaptuple::TupleDescData;

fn make_enr(name: &str) -> EphemeralNamedRelationData {
    EphemeralNamedRelationData {
        md: EphemeralNamedRelationMetadataData {
            name: Some(name.to_string()),
            reliddesc: InvalidOid,
            tupdesc: None,
            enrtype: ENR_NAMED_TUPLESTORE,
            enrtuples: 0.0,
        },
        reldata: None,
    }
}

#[test]
fn create_is_empty() {
    let ctx = mcx::MemoryContext::new("test");
    let env = create_queryEnv(ctx.mcx());
    assert!(env.namedRelList.is_empty());
    assert_eq!(ctx.used(), 0, "empty environment allocates nothing");
}

#[test]
fn register_then_get() {
    let ctx = mcx::MemoryContext::new("test");
    let mut env = create_queryEnv(ctx.mcx());
    register_ENR(&mut env, make_enr("delta")).unwrap();
    assert!(ctx.used() > 0, "registered ENR is charged to the context");

    let found = get_ENR(&env, "delta").expect("registered ENR must be found");
    assert_eq!(found.md.name.as_deref(), Some("delta"));

    // Quietly returns None for an unknown name.
    assert!(get_ENR(&env, "missing").is_none());
}

#[test]
fn get_visible_metadata_clones_md() {
    let ctx = mcx::MemoryContext::new("test");
    let mut env = create_queryEnv(ctx.mcx());
    register_ENR(&mut env, make_enr("trans")).unwrap();

    let md = get_visible_ENR_metadata(Some(&env), "trans").expect("must find metadata");
    assert_eq!(md.name.as_deref(), Some("trans"));

    // NULL queryEnv -> None.
    assert!(get_visible_ENR_metadata(None, "trans").is_none());
    // Unknown name -> None.
    assert!(get_visible_ENR_metadata(Some(&env), "nope").is_none());
}

#[test]
fn unregister_removes_match() {
    let ctx = mcx::MemoryContext::new("test");
    let mut env = create_queryEnv(ctx.mcx());
    register_ENR(&mut env, make_enr("a")).unwrap();
    register_ENR(&mut env, make_enr("b")).unwrap();

    unregister_ENR(&mut env, "a");
    assert!(get_ENR(&env, "a").is_none());
    assert!(get_ENR(&env, "b").is_some());

    // Unregistering an absent name is a no-op.
    unregister_ENR(&mut env, "ghost");
    assert_eq!(env.namedRelList.len(), 1);
}

#[test]
fn get_enr_walk_order_preserved() {
    let ctx = mcx::MemoryContext::new("test");
    let mut env = create_queryEnv(ctx.mcx());
    let names = ["x", "y", "z"];
    for n in names {
        register_ENR(&mut env, make_enr(n)).unwrap();
    }
    let got: Vec<_> = env
        .namedRelList
        .iter()
        .map(|e| e.md.name.clone().unwrap())
        .collect();
    assert_eq!(got, names.iter().map(|s| s.to_string()).collect::<Vec<_>>());
}

#[test]
fn tupdesc_branch_uses_inline_descriptor() {
    let mut md = EphemeralNamedRelationMetadataData {
        name: Some("d".to_string()),
        reliddesc: InvalidOid,
        tupdesc: None,
        enrtype: ENR_NAMED_TUPLESTORE,
        enrtuples: 0.0,
    };
    let desc: TupleDesc = Some(Box::new(TupleDescData {
        natts: 0,
        tdtypeid: InvalidOid,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: Vec::new(),
        attrs: Vec::new(),
    }));
    md.tupdesc = desc.clone();

    let out = ENRMetadataGetTupDesc(&md).unwrap();
    assert!(out.is_some());
}

#[test]
fn environment_bytes_return_on_drop() {
    let ctx = mcx::MemoryContext::new("per-query");
    {
        let mut env = create_queryEnv(ctx.mcx());
        register_ENR(&mut env, make_enr("delta")).unwrap();
        assert_eq!(
            ctx.used(),
            env.namedRelList.capacity() * core::mem::size_of::<EphemeralNamedRelationData>()
        );
    }
    assert_eq!(ctx.used(), 0, "dropping the environment returns every byte");
}
