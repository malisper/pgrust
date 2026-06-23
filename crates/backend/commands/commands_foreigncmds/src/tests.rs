//! Unit tests for `backend-commands-foreigncmds`.
//!
//! These exercise the in-crate logic that does *not* depend on an installed
//! runtime: the `transformGenericOptions` SET/ADD/DROP merge and its
//! `"="`-in-name rejection (reached only when `fdwvalidator == InvalidOid`, so
//! no seam fires), and the pure helper predicates. The seam-bearing command
//! drivers panic (loud, not silent) without an installed runtime; they are
//! exercised by the integration harness once a runtime is wired.

use super::*;
use ::mcx::{MemoryContext, PgString, PgVec};
use ::types_foreigncmds::{DEFELEM_ADD, DEFELEM_DROP, DEFELEM_SET, DEFELEM_UNSPEC};

fn ctx() -> MemoryContext {
    MemoryContext::new("foreigncmds-test")
}

/// A `DefElem` carrying `name` with the given action and no arg (the merge
/// never reads `arg`).
fn defelem<'mcx>(mcx: Mcx<'mcx>, name: &str, action: DefElemAction) -> DefElem<'mcx> {
    DefElem {
        defname: PgString::from_str_in(name, mcx).unwrap(),
        arg: None,
        defaction: action,
    }
}

fn pgvec_of<'mcx>(mcx: Mcx<'mcx>, names: &[&str]) -> PgVec<'mcx, DefElem<'mcx>> {
    let mut v = PgVec::new_in(mcx);
    for n in names {
        v.push(defelem(mcx, n, DEFELEM_UNSPEC));
    }
    v
}

#[test]
fn oid_is_valid_matches_c_predicate() {
    assert!(!OidIsValid(InvalidOid));
    assert!(OidIsValid(1234));
}

#[test]
fn object_address_set_shape() {
    let a = object_address_set(ForeignServerRelationId, 42);
    assert_eq!(a.classId, ForeignServerRelationId);
    assert_eq!(a.objectId, 42);
    assert_eq!(a.objectSubId, 0);
}

#[test]
fn options_for_store_maps_empty_to_null() {
    let c = ctx();
    let mcx = c.mcx();
    let empty: PgVec<DefElem> = PgVec::new_in(mcx);
    assert!(options_for_store(&empty).is_none());
    let one = pgvec_of(mcx, &["a"]);
    assert!(options_for_store(&one).is_some());
}

#[test]
fn transform_add_appends_new_option() {
    let c = ctx();
    let mcx = c.mcx();
    let opts = vec![defelem(mcx, "host", DEFELEM_ADD)];
    let merged =
        transformGenericOptions(mcx, ForeignServerRelationId, PgVec::new_in(mcx), &opts, InvalidOid)
            .unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].defname.as_str(), "host");
}

#[test]
fn transform_unspec_is_treated_as_add() {
    let c = ctx();
    let mcx = c.mcx();
    let opts = vec![defelem(mcx, "host", DEFELEM_UNSPEC)];
    let merged =
        transformGenericOptions(mcx, ForeignServerRelationId, PgVec::new_in(mcx), &opts, InvalidOid)
            .unwrap();
    assert_eq!(merged.len(), 1);
}

#[test]
fn transform_add_duplicate_errors() {
    let c = ctx();
    let mcx = c.mcx();
    let old = pgvec_of(mcx, &["host"]);
    let opts = vec![defelem(mcx, "host", DEFELEM_ADD)];
    let err =
        transformGenericOptions(mcx, ForeignServerRelationId, old, &opts, InvalidOid).unwrap_err();
    assert!(err.message().contains("provided more than once"));
}

#[test]
fn transform_set_replaces_existing() {
    let c = ctx();
    let mcx = c.mcx();
    let old = pgvec_of(mcx, &["host", "port"]);
    let opts = vec![defelem(mcx, "port", DEFELEM_SET)];
    let merged =
        transformGenericOptions(mcx, ForeignServerRelationId, old, &opts, InvalidOid).unwrap();
    assert_eq!(merged.len(), 2);
    let port = merged.iter().find(|d| d.defname.as_str() == "port").unwrap();
    assert_eq!(port.defaction, DEFELEM_SET);
}

#[test]
fn transform_set_missing_errors() {
    let c = ctx();
    let mcx = c.mcx();
    let opts = vec![defelem(mcx, "nope", DEFELEM_SET)];
    let err =
        transformGenericOptions(mcx, ForeignServerRelationId, PgVec::new_in(mcx), &opts, InvalidOid)
            .unwrap_err();
    assert!(err.message().contains("not found"));
}

#[test]
fn transform_drop_removes_existing() {
    let c = ctx();
    let mcx = c.mcx();
    let old = pgvec_of(mcx, &["host", "port"]);
    let opts = vec![defelem(mcx, "host", DEFELEM_DROP)];
    let merged =
        transformGenericOptions(mcx, ForeignServerRelationId, old, &opts, InvalidOid).unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].defname.as_str(), "port");
}

#[test]
fn transform_drop_missing_errors() {
    let c = ctx();
    let mcx = c.mcx();
    let opts = vec![defelem(mcx, "nope", DEFELEM_DROP)];
    let err =
        transformGenericOptions(mcx, ForeignServerRelationId, PgVec::new_in(mcx), &opts, InvalidOid)
            .unwrap_err();
    assert!(err.message().contains("not found"));
}

#[test]
fn transform_rejects_equals_in_name() {
    let c = ctx();
    let mcx = c.mcx();
    let opts = vec![defelem(mcx, "a=b", DEFELEM_ADD)];
    let err =
        transformGenericOptions(mcx, ForeignServerRelationId, PgVec::new_in(mcx), &opts, InvalidOid)
            .unwrap_err();
    assert!(err.message().contains("must not contain"));
}

#[test]
fn transform_empty_yields_empty_list() {
    let c = ctx();
    let mcx = c.mcx();
    let merged =
        transformGenericOptions(mcx, ForeignServerRelationId, PgVec::new_in(mcx), &[], InvalidOid)
            .unwrap();
    assert!(merged.is_empty());
}

#[test]
fn rolespec_public_detection() {
    let c = ctx();
    let mcx = c.mcx();
    let public = RoleSpec {
        roletype: ROLESPEC_PUBLIC,
        rolename: None,
    };
    assert!(rolespec_is_public(&public));
    let named = RoleSpec {
        roletype: nodes::parsenodes::ROLESPEC_CSTRING,
        rolename: Some(PgString::from_str_in("alice", mcx).unwrap()),
    };
    assert!(!rolespec_is_public(&named));
}
