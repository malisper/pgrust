//! Pure-helper tests for comment.c's in-crate control flow.
//!
//! The catalog read/write path now runs over real `pg_description` /
//! `pg_shdescription` relations (`table_open` + `systable` scans +
//! `CatalogTuple*`), which require a live backend, so the upsert/delete decision
//! is exercised end-to-end in the boot smoke test rather than mocked here. These
//! unit tests cover the two pure decisions that have no backend dependency: the
//! empty-string -> NULL reduction and the `stmt.comment` borrow.

use super::*;
use ::nodes::parsenodes::OBJECT_TABLE;
use ::nodes::parsenodes::ObjectType;
use ::parsenodes::{Node, StringNode};

/// A `String` value node naming the object (`strVal(stmt->object)`).
fn string_node(name: &str) -> Box<Node> {
    Box::new(Node::String(StringNode {
        sval: Some(name.to_string()),
    }))
}

fn comment_stmt(objtype: ObjectType, comment: Option<&str>) -> CommentStmt {
    CommentStmt {
        objtype,
        object: Some(string_node("the_object")),
        comment: comment.map(|c| c.to_string()),
    }
}

// --- reduce_empty / comment_str pure helpers -----------------------------

#[test]
fn reduce_empty_folds_empty_string_to_none() {
    assert_eq!(reduce_empty(None), None);
    assert_eq!(reduce_empty(Some("")), None);
    assert_eq!(reduce_empty(Some("hi")), Some("hi"));
}

#[test]
fn comment_str_maps_absent_to_none() {
    assert_eq!(comment_str(&comment_stmt(OBJECT_TABLE, None)), None);
    assert_eq!(comment_str(&comment_stmt(OBJECT_TABLE, Some("note"))), Some("note"));
}
