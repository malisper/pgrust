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
    extents: Vec<Extent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Extent {
    start: u16,
    end: u16,
}

fn eval_match(vector: &TsVector, node: &TsQueryNode) -> MatchResult {
    let max_pos = vector
        .lexemes
        .iter()
        .flat_map(|lexeme| lexeme.positions.iter().map(|position| position.position))
        .max()
        .unwrap_or(0);
    eval_match_with_max(vector, node, max_pos)
}

fn eval_match_with_max(vector: &TsVector, node: &TsQueryNode, max_pos: u16) -> MatchResult {
    match node {
        TsQueryNode::Operand(operand) => {
            let positions = vector
                .positions_for_operand(operand)
                .collect::<BTreeSet<_>>();
            let extents = positions
                .iter()
                .map(|position| Extent {
                    start: *position,
                    end: *position,
                })
                .collect::<Vec<_>>();
            MatchResult {
                matched: !positions.is_empty() || vector.contains_term(operand),
                positions,
                extents,
            }
        }
        TsQueryNode::And(left, right) => {
            let left = eval_match_with_max(vector, left, max_pos);
            let right = eval_match_with_max(vector, right, max_pos);
            let mut positions = left.positions;
            let right_positions = right.positions;
            let mut extents = left
                .extents
                .iter()
                .copied()
                .filter(|extent| right_positions.contains(&extent.start))
                .collect::<Vec<_>>();
            if extents.is_empty() {
                extents = right
                    .extents
                    .iter()
                    .copied()
                    .filter(|extent| positions.contains(&extent.start))
                    .collect();
            }
            positions.extend(right_positions);
            MatchResult {
                matched: left.matched && right.matched,
                positions,
                extents,
            }
        }
        TsQueryNode::Or(left, right) => {
            let left = eval_match_with_max(vector, left, max_pos);
            let right = eval_match_with_max(vector, right, max_pos);
            let mut positions = left.positions;
            positions.extend(right.positions);
            let mut extents = left.extents;
            extents.extend(right.extents);
            extents.sort();
            extents.dedup();
            MatchResult {
                matched: left.matched || right.matched,
                positions,
                extents,
            }
        }
        TsQueryNode::Not(inner) => {
            let inner = eval_match_with_max(vector, inner, max_pos);
            let positions = (1..=max_pos)
                .filter(|position| !inner.positions.contains(position))
                .collect::<BTreeSet<_>>();
            let extents = positions
                .iter()
                .map(|position| Extent {
                    start: *position,
                    end: *position,
                })
                .collect::<Vec<_>>();
            MatchResult {
                matched: !inner.matched,
                positions,
                extents,
            }
        }
        TsQueryNode::Phrase {
            left,
            right,
            distance,
        } => {
            let right_negative = right_is_negative(right.as_ref());
            let left = eval_match_with_max(vector, left, max_pos);
            let right = eval_match_with_max(vector, right, max_pos);
            let mut positions = BTreeSet::new();
            let mut extents = Vec::new();
            for left_extent in &left.extents {
                let target = left_extent.end.saturating_add(*distance);
                for right_extent in right.extents.iter().filter(|extent| extent.start == target) {
                    positions.insert(right_extent.end);
                    let end = if right_negative {
                        left_extent.end
                    } else {
                        right_extent.end
                    };
                    extents.push(Extent {
                        start: left_extent.start,
                        end,
                    });
                }
            }
            extents.sort();
            extents.dedup();
            MatchResult {
                matched: !positions.is_empty(),
                positions,
                extents,
            }
        }
    }
}

fn right_is_negative(node: &TsQueryNode) -> bool {
    matches!(node, TsQueryNode::Not(_))
}
