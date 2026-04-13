use std::collections::BTreeSet;

use crate::include::nodes::tsearch::{TsQuery, TsQueryNode, TsVector};

pub(crate) fn eval_tsvector_matches_tsquery(vector: &TsVector, query: &TsQuery) -> bool {
    eval_match(vector, &query.root).matched
}

pub(crate) fn eval_tsquery_matches_tsvector(query: &TsQuery, vector: &TsVector) -> bool {
    eval_tsvector_matches_tsquery(vector, query)
}

#[derive(Default)]
struct MatchResult {
    matched: bool,
    positions: BTreeSet<u16>,
}

fn eval_match(vector: &TsVector, node: &TsQueryNode) -> MatchResult {
    match node {
        TsQueryNode::Operand(operand) => {
            let positions = vector
                .positions_for_operand(operand)
                .collect::<BTreeSet<_>>();
            MatchResult {
                matched: !positions.is_empty() || vector.contains_term(operand),
                positions,
            }
        }
        TsQueryNode::And(left, right) => {
            let left = eval_match(vector, left);
            let right = eval_match(vector, right);
            let mut positions = left.positions;
            positions.extend(right.positions);
            MatchResult {
                matched: left.matched && right.matched,
                positions,
            }
        }
        TsQueryNode::Or(left, right) => {
            let left = eval_match(vector, left);
            let right = eval_match(vector, right);
            let mut positions = left.positions;
            positions.extend(right.positions);
            MatchResult {
                matched: left.matched || right.matched,
                positions,
            }
        }
        TsQueryNode::Not(inner) => MatchResult {
            matched: !eval_match(vector, inner).matched,
            positions: BTreeSet::new(),
        },
        TsQueryNode::Phrase {
            left,
            right,
            distance,
        } => {
            let left = eval_match(vector, left);
            let right = eval_match(vector, right);
            let mut positions = BTreeSet::new();
            for left_pos in &left.positions {
                let target = left_pos.saturating_add(*distance);
                if right.positions.contains(&target) {
                    positions.insert(target);
                }
            }
            MatchResult {
                matched: !positions.is_empty(),
                positions,
            }
        }
    }
}
