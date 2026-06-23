//! Unit tests for the in-crate, side-effect-free logic of alter.c: the
//! `report_name_conflict` `classId` → message switch (alter.c:75-108) and the
//! dispatch helpers that don't require an installed catalog substrate.

use super::*;
use ::types_catalog::catalog::{
    EVENT_TRIGGER_RELATION_ID, FOREIGN_DATA_WRAPPER_RELATION_ID, FOREIGN_SERVER_RELATION_ID,
    LANGUAGE_RELATION_ID, PUBLICATION_RELATION_ID, SUBSCRIPTION_RELATION_ID, TYPE_RELATION_ID,
};

#[test]
fn report_name_conflict_messages() {
    let cases = [
        (EVENT_TRIGGER_RELATION_ID, "event trigger \"x\" already exists"),
        (
            FOREIGN_DATA_WRAPPER_RELATION_ID,
            "foreign-data wrapper \"x\" already exists",
        ),
        (FOREIGN_SERVER_RELATION_ID, "server \"x\" already exists"),
        (LANGUAGE_RELATION_ID, "language \"x\" already exists"),
        (PUBLICATION_RELATION_ID, "publication \"x\" already exists"),
        (SUBSCRIPTION_RELATION_ID, "subscription \"x\" already exists"),
    ];
    for (class_id, expected) in cases {
        let err = report_name_conflict(class_id, "x").unwrap_err();
        assert!(
            err.message().contains(expected),
            "class {class_id}: got {:?}, want contains {expected}",
            err.message()
        );
    }
}

#[test]
fn report_name_conflict_unsupported_class() {
    // A class with no message arm raises the internal "unsupported object class".
    let err = report_name_conflict(TYPE_RELATION_ID, "x").unwrap_err();
    assert!(
        err.message().contains("unsupported object class"),
        "got {:?}",
        err.message()
    );
}
