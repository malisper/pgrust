use std::collections::BTreeSet;

use crate::compat::include::nodes::tsearch::{TsQuery, TsQueryNode, TsVector};

pub fn eval_tsvector_matches_tsquery(vector: &TsVector, query: &TsQuery) -> bool {
    eval_match(vector, &query.root).matched
}

pub fn eval_tsquery_matches_tsvector(query: &TsQuery, vector: &TsVector) -> bool {
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
            if let (Some(left_inner), Some(right_inner)) =
                (not_inner(left.as_ref()), not_inner(right.as_ref()))
            {
                let left = eval_match_with_max(vector, left_inner, max_pos);
                let right = eval_match_with_max(vector, right_inner, max_pos);
                let mut positions = left
                    .extents
                    .iter()
                    .filter_map(|extent| extent.end.checked_add(*distance))
                    .chain(right.extents.iter().map(|extent| extent.end))
                    .collect::<BTreeSet<_>>();
                positions.retain(|position| *position > 0);
                let extents = positions
                    .iter()
                    .map(|position| Extent {
                        start: *position,
                        end: *position,
                    })
                    .collect::<Vec<_>>();
                return MatchResult {
                    matched: !(left.matched || right.matched) || !positions.is_empty(),
                    positions,
                    extents,
                };
            }
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

fn not_inner(node: &TsQueryNode) -> Option<&TsQueryNode> {
    match node {
        TsQueryNode::Not(inner) => Some(inner),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::include::nodes::tsearch::{TsQuery, TsVector};

    fn matches(vector: &str, query: &str) -> bool {
        let vector = TsVector::parse(vector).unwrap();
        let query = TsQuery::parse(query).unwrap();
        eval_tsvector_matches_tsquery(&vector, &query)
    }

    #[test]
    fn phrase_with_two_negative_operands_follows_postgres_position_semantics() {
        assert!(matches("'pl':1 'xx':2", "!pl <-> !yh"));
        assert!(matches("'xx':1 'yh':2", "!pl <-> !yh"));
        assert!(matches("'xx':1", "!pl <-> !yh"));
        assert!(matches("", "!pl <-> !yh"));
        assert!(!matches("'pl' 'xx'", "!pl <-> !yh"));
    }
}
