use std::cmp::Ordering;

use crate::include::nodes::tsearch::{TsQuery, TsQueryNode};

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
}
