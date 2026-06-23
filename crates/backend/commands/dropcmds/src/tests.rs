//! Unit tests for the pure (seam-free) helpers of dropcmds.c.

use super::*;
use ::parsenodes::StringNode;

fn s(v: &str) -> Node {
    Node::String(StringNode {
        sval: Some(v.to_string()),
    })
}

#[test]
fn render_format_substitutes_in_order() {
    assert_eq!(
        render_format("function %s(%s) does not exist, skipping", &["f", "int"]),
        "function f(int) does not exist, skipping"
    );
    assert_eq!(
        render_format("schema \"%s\" does not exist, skipping", &["s"]),
        "schema \"s\" does not exist, skipping"
    );
    // `%%` renders a literal percent; a bare trailing `%` is preserved.
    assert_eq!(render_format("100%% %s", &["done"]), "100% done");
    assert_eq!(render_format("a%", &[]), "a%");
}

#[test]
fn list_copy_head_and_tail() {
    let list = vec![s("a"), s("b"), s("c")];
    assert_eq!(list_length(&list), 3);

    let head = list_copy_head(&list, 2);
    assert_eq!(head.len(), 2);
    assert_eq!(node_str_val(&head[0]), "a");
    assert_eq!(node_str_val(&head[1]), "b");

    let tail = list_copy_tail(&list, 1);
    assert_eq!(tail.len(), 2);
    assert_eq!(node_str_val(&tail[0]), "b");

    assert_eq!(node_str_val(linitial(&list)), "a");
    assert_eq!(node_str_val(lsecond(&list)), "b");
    assert_eq!(node_str_val(llast(&list)), "c");
}

#[test]
fn list_copy_head_clamps() {
    let list = vec![s("only")];
    // list_length(list) - 1 == 0 for a single-element list.
    assert!(list_copy_head(&list, 0).is_empty());
}

#[test]
fn namelist_projection() {
    let list = vec![s("public"), s("widget")];
    let nl = namelist_of_nodes(&list).unwrap();
    assert_eq!(nl, vec![Some("public".to_string()), Some("widget".to_string())]);
}
