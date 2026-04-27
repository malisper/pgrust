use std::cmp::Ordering;
use std::collections::BTreeSet;

use crate::include::nodes::tsearch::{TsQuery, TsQueryNode, TsQueryOperand};

pub(crate) fn compare_tsquery(left: &TsQuery, right: &TsQuery) -> Ordering {
    query_item_count(&left.root)
        .cmp(&query_item_count(&right.root))
        .then_with(|| query_operand_bytes(&left.root).cmp(&query_operand_bytes(&right.root)))
        .then_with(|| compare_tsquery_node(&left.root, &right.root))
}

pub(crate) fn tsquery_and(left: TsQuery, right: TsQuery) -> TsQuery {
    TsQuery {
        root: TsQueryNode::And(Box::new(left.root), Box::new(right.root)),
    }
}

pub(crate) fn tsquery_or(left: TsQuery, right: TsQuery) -> TsQuery {
    TsQuery {
        root: TsQueryNode::Or(Box::new(left.root), Box::new(right.root)),
    }
}

pub(crate) fn tsquery_not(query: TsQuery) -> TsQuery {
    TsQuery {
        root: TsQueryNode::Not(Box::new(query.root)),
    }
}

pub(crate) fn tsquery_phrase(left: TsQuery, right: TsQuery, distance: u16) -> TsQuery {
    TsQuery {
        root: TsQueryNode::Phrase {
            left: Box::new(left.root),
            right: Box::new(right.root),
            distance,
        },
    }
}

pub(crate) fn tsquery_rewrite(query: TsQuery, target: TsQuery, substitute: TsQuery) -> TsQuery {
    let substitute = (!tsquery_node_is_empty(&substitute.root)).then_some(substitute.root);
    rewrite_tsquery_node(query.root, &target.root, substitute.as_ref())
        .unwrap_or_else(empty_tsquery_node)
        .into()
}

pub(crate) fn tsquery_contains(left: &TsQuery, right: &TsQuery) -> bool {
    let left_values = query_lexemes(&left.root);
    let right_values = query_lexemes(&right.root);
    right_values.is_subset(&left_values)
}

impl From<TsQueryNode> for TsQuery {
    fn from(root: TsQueryNode) -> Self {
        Self { root }
    }
}

fn empty_tsquery_node() -> TsQueryNode {
    TsQueryNode::Operand(TsQueryOperand::new(""))
}

fn tsquery_node_is_empty(node: &TsQueryNode) -> bool {
    matches!(
        node,
        TsQueryNode::Operand(operand)
            if operand.lexeme.as_str().is_empty() && !operand.prefix && operand.weights.is_empty()
    )
}

fn rewrite_tsquery_node(
    node: TsQueryNode,
    target: &TsQueryNode,
    substitute: Option<&TsQueryNode>,
) -> Option<TsQueryNode> {
    if node == *target {
        return substitute.cloned();
    }
    if let Some(rewritten) = rewrite_bool_subset(&node, target, substitute) {
        return rewritten;
    }
    match node {
        TsQueryNode::Not(inner) => rewrite_tsquery_node(*inner, target, substitute)
            .map(|inner| TsQueryNode::Not(Box::new(inner))),
        TsQueryNode::And(left, right) => rebuild_bool_node(
            BoolOp::And,
            [*left, *right]
                .into_iter()
                .filter_map(|child| rewrite_tsquery_node(child, target, substitute))
                .collect(),
        ),
        TsQueryNode::Or(left, right) => rebuild_bool_node(
            BoolOp::Or,
            [*left, *right]
                .into_iter()
                .filter_map(|child| rewrite_tsquery_node(child, target, substitute))
                .collect(),
        ),
        TsQueryNode::Phrase {
            left,
            right,
            distance,
        } => match (
            rewrite_tsquery_node(*left, target, substitute),
            rewrite_tsquery_node(*right, target, substitute),
        ) {
            (Some(left), Some(right)) => Some(TsQueryNode::Phrase {
                left: Box::new(left),
                right: Box::new(right),
                distance,
            }),
            (Some(node), None) | (None, Some(node)) => Some(node),
            (None, None) => None,
        },
        TsQueryNode::Operand(_) => Some(node),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoolOp {
    And,
    Or,
}

fn rewrite_bool_subset(
    node: &TsQueryNode,
    target: &TsQueryNode,
    substitute: Option<&TsQueryNode>,
) -> Option<Option<TsQueryNode>> {
    let op = bool_op(node)?;
    if bool_op(target)? != op {
        return None;
    }
    let mut node_children = Vec::new();
    flatten_bool_node(node.clone(), op, &mut node_children);
    let mut target_children = Vec::new();
    flatten_bool_node(target.clone(), op, &mut target_children);
    if target_children.is_empty() || node_children.len() <= target_children.len() {
        return None;
    }
    let mut matched = vec![false; node_children.len()];
    for target_child in &target_children {
        let Some(index) = node_children
            .iter()
            .enumerate()
            .find_map(|(index, child)| (!matched[index] && child == target_child).then_some(index))
        else {
            return None;
        };
        matched[index] = true;
    }
    let mut remaining = node_children
        .into_iter()
        .enumerate()
        .filter_map(|(index, child)| (!matched[index]).then_some(child))
        .collect::<Vec<_>>();
    if let Some(substitute) = substitute {
        remaining.push(substitute.clone());
    }
    Some(rebuild_bool_node(op, remaining))
}

fn bool_op(node: &TsQueryNode) -> Option<BoolOp> {
    match node {
        TsQueryNode::And(_, _) => Some(BoolOp::And),
        TsQueryNode::Or(_, _) => Some(BoolOp::Or),
        _ => None,
    }
}

fn flatten_bool_node(node: TsQueryNode, op: BoolOp, out: &mut Vec<TsQueryNode>) {
    match (op, node) {
        (BoolOp::And, TsQueryNode::And(left, right))
        | (BoolOp::Or, TsQueryNode::Or(left, right)) => {
            flatten_bool_node(*left, op, out);
            flatten_bool_node(*right, op, out);
        }
        (_, node) => out.push(node),
    }
}

fn rebuild_bool_node(op: BoolOp, nodes: Vec<TsQueryNode>) -> Option<TsQueryNode> {
    let mut nodes = nodes.into_iter();
    let mut root = nodes.next()?;
    for node in nodes {
        root = match op {
            BoolOp::And => TsQueryNode::And(Box::new(root), Box::new(node)),
            BoolOp::Or => TsQueryNode::Or(Box::new(root), Box::new(node)),
        };
    }
    Some(root)
}

pub(crate) fn tsquery_contained_by(left: &TsQuery, right: &TsQuery) -> bool {
    tsquery_contains(right, left)
}

pub(crate) fn numnode(query: &TsQuery) -> i32 {
    fn count(node: &TsQueryNode) -> i32 {
        match node {
            TsQueryNode::Operand(_) => 1,
            TsQueryNode::Not(inner) => 1 + count(inner),
            TsQueryNode::And(left, right) | TsQueryNode::Or(left, right) => {
                1 + count(left) + count(right)
            }
            TsQueryNode::Phrase { left, right, .. } => 1 + count(left) + count(right),
        }
    }
    count(&query.root)
}

pub(crate) fn tsquery_operands(query: &TsQuery) -> Vec<String> {
    query_lexemes(&query.root)
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn query_lexemes(node: &TsQueryNode) -> BTreeSet<&str> {
    let mut values = BTreeSet::new();
    collect_query_lexemes(node, &mut values);
    values
}

fn collect_query_lexemes<'a>(node: &'a TsQueryNode, values: &mut BTreeSet<&'a str>) {
    match node {
        TsQueryNode::Operand(operand) => {
            values.insert(operand.lexeme.as_str());
        }
        TsQueryNode::Not(inner) => collect_query_lexemes(inner, values),
        TsQueryNode::And(left, right) | TsQueryNode::Or(left, right) => {
            collect_query_lexemes(left, values);
            collect_query_lexemes(right, values);
        }
        TsQueryNode::Phrase { left, right, .. } => {
            collect_query_lexemes(left, values);
            collect_query_lexemes(right, values);
        }
    }
}

fn query_item_count(node: &TsQueryNode) -> usize {
    match node {
        TsQueryNode::Operand(_) => 1,
        TsQueryNode::Not(inner) => 1 + query_item_count(inner),
        TsQueryNode::And(left, right) | TsQueryNode::Or(left, right) => {
            1 + query_item_count(left) + query_item_count(right)
        }
        TsQueryNode::Phrase { left, right, .. } => {
            1 + query_item_count(left) + query_item_count(right)
        }
    }
}

fn query_operand_bytes(node: &TsQueryNode) -> usize {
    match node {
        TsQueryNode::Operand(operand) => operand.lexeme.as_str().len(),
        TsQueryNode::Not(inner) => query_operand_bytes(inner),
        TsQueryNode::And(left, right) | TsQueryNode::Or(left, right) => {
            query_operand_bytes(left) + query_operand_bytes(right)
        }
        TsQueryNode::Phrase { left, right, .. } => {
            query_operand_bytes(left) + query_operand_bytes(right)
        }
    }
}

fn compare_tsquery_node(left: &TsQueryNode, right: &TsQueryNode) -> Ordering {
    query_item_type(right)
        .cmp(&query_item_type(left))
        .then_with(|| match (left, right) {
            (TsQueryNode::Operand(left), TsQueryNode::Operand(right)) => left
                .lexeme
                .cmp(&right.lexeme)
                .then_with(|| left.prefix.cmp(&right.prefix))
                .then_with(|| left.weights.cmp(&right.weights)),
            (TsQueryNode::Not(left), TsQueryNode::Not(right)) => {
                compare_tsquery_node(left, right)
            }
            (
                TsQueryNode::And(left_left, left_right),
                TsQueryNode::And(right_left, right_right),
            )
            | (
                TsQueryNode::Or(left_left, left_right),
                TsQueryNode::Or(right_left, right_right),
            ) => compare_tsquery_node(left_right, right_right)
                .then_with(|| compare_tsquery_node(left_left, right_left)),
            (
                TsQueryNode::Phrase {
                    left: left_left,
                    right: left_right,
                    distance: left_distance,
                },
                TsQueryNode::Phrase {
                    left: right_left,
                    right: right_right,
                    distance: right_distance,
                },
            ) => compare_tsquery_node(left_right, right_right)
                .then_with(|| compare_tsquery_node(left_left, right_left))
                .then_with(|| right_distance.cmp(left_distance)),
            _ => query_operator_code(right).cmp(&query_operator_code(left)),
        })
}

fn query_item_type(node: &TsQueryNode) -> u8 {
    match node {
        TsQueryNode::Operand(_) => 1,
        TsQueryNode::And(_, _)
        | TsQueryNode::Or(_, _)
        | TsQueryNode::Not(_)
        | TsQueryNode::Phrase { .. } => 2,
    }
}

fn query_operator_code(node: &TsQueryNode) -> u8 {
    match node {
        TsQueryNode::Not(_) => 1,
        TsQueryNode::And(_, _) => 2,
        TsQueryNode::Or(_, _) => 3,
        TsQueryNode::Phrase { .. } => 4,
        TsQueryNode::Operand(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_tsquery_uses_postgres_size_ordering() {
        let rhs = TsQuery::parse("b & c").unwrap();

        assert_eq!(
            compare_tsquery(&TsQuery::parse("a").unwrap(), &rhs),
            Ordering::Less
        );
        assert_eq!(
            compare_tsquery(&TsQuery::parse("a | f").unwrap(), &rhs),
            Ordering::Less
        );
        assert_eq!(
            compare_tsquery(&TsQuery::parse("a | ff").unwrap(), &rhs),
            Ordering::Greater
        );
        assert_eq!(
            compare_tsquery(&TsQuery::parse("a | f | g").unwrap(), &rhs),
            Ordering::Greater
        );
    }

    #[test]
    fn tsquery_containment_uses_query_lexeme_sets() {
        assert!(tsquery_contains(
            &TsQuery::parse("new <-> york").unwrap(),
            &TsQuery::parse("new").unwrap()
        ));
        assert!(tsquery_contained_by(
            &TsQuery::parse("new").unwrap(),
            &TsQuery::parse("new <-> york").unwrap()
        ));
        assert!(!tsquery_contains(
            &TsQuery::parse("new").unwrap(),
            &TsQuery::parse("new & york").unwrap()
        ));
        assert!(tsquery_contains(
            &TsQuery::parse("new | !york").unwrap(),
            &TsQuery::parse("york").unwrap()
        ));
    }
}
