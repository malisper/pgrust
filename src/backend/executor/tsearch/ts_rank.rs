use crate::include::nodes::tsearch::{
    TsPosition, TsQuery, TsQueryNode, TsQueryOperand, TsVector, TsWeight,
};

const DEFAULT_WEIGHTS: [f64; 4] = [0.1, 0.2, 0.4, 1.0];

pub(crate) fn ts_rank(
    vector: &TsVector,
    query: &TsQuery,
    weights: Option<[f64; 4]>,
    normalization: i32,
) -> f64 {
    let operands = query_operands(query);
    if vector.lexemes.is_empty() || operands.is_empty() {
        return 0.0;
    }
    let weights = weights.unwrap_or(DEFAULT_WEIGHTS);
    let mut result = if matches!(
        query.root,
        TsQueryNode::And(_, _) | TsQueryNode::Phrase { .. }
    ) {
        rank_and(vector, &operands, &weights)
    } else {
        rank_or(vector, &operands, &weights)
    };
    if result < 0.0 {
        result = 1e-20;
    }
    normalize_rank(result, vector, normalization)
}

pub(crate) fn ts_rank_cd(
    vector: &TsVector,
    query: &TsQuery,
    weights: Option<[f64; 4]>,
    normalization: i32,
) -> f64 {
    if vector.lexemes.is_empty() {
        return 0.0;
    }
    let weights = weights.unwrap_or(DEFAULT_WEIGHTS);
    let result = match &query.root {
        TsQueryNode::Or(_, _) => rank_cd_or(vector, &query_operands(query), &weights),
        TsQueryNode::And(_, _) => rank_cd_cover(vector, &query_operands(query), None, &weights),
        TsQueryNode::Phrase { .. } => rank_cd_phrase(vector, &query.root, &weights),
        TsQueryNode::Operand(operand) => rank_cd_or(vector, &[operand.clone()], &weights),
        TsQueryNode::Not(_) => 0.0,
    };
    normalize_rank(result, vector, normalization)
}

fn query_operands(query: &TsQuery) -> Vec<TsQueryOperand> {
    fn walk(node: &TsQueryNode, out: &mut Vec<TsQueryOperand>) {
        match node {
            TsQueryNode::Operand(operand) => {
                if !out.contains(operand) {
                    out.push(operand.clone());
                }
            }
            TsQueryNode::Not(inner) => walk(inner, out),
            TsQueryNode::And(left, right) | TsQueryNode::Or(left, right) => {
                walk(left, out);
                walk(right, out);
            }
            TsQueryNode::Phrase { left, right, .. } => {
                walk(left, out);
                walk(right, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(&query.root, &mut out);
    out
}

fn matching_positions(vector: &TsVector, operand: &TsQueryOperand) -> Vec<TsPosition> {
    let mut out = Vec::new();
    for lexeme in &vector.lexemes {
        let text_matches = if operand.prefix {
            lexeme.text.as_str().starts_with(operand.lexeme.as_str())
        } else {
            lexeme.text == operand.lexeme
        };
        if !text_matches {
            continue;
        }
        if lexeme.positions.is_empty() {
            out.push(TsPosition {
                position: u16::MAX,
                weight: None,
            });
            continue;
        }
        out.extend(lexeme.positions.iter().copied().filter(|position| {
            operand.weights.is_empty()
                || operand
                    .weights
                    .iter()
                    .any(|weight| Some(*weight) == position.weight)
        }));
    }
    out.sort();
    out
}

fn rank_or(vector: &TsVector, operands: &[TsQueryOperand], weights: &[f64; 4]) -> f64 {
    let mut result = 0.0;
    for operand in operands {
        let positions = matching_positions(vector, operand);
        if positions.is_empty() {
            continue;
        }
        let mut resj = 0.0;
        let mut max_weight = -1.0;
        let mut max_index = 0usize;
        for (idx, position) in positions.iter().enumerate() {
            let weight = weight_value(position.weight, weights);
            let ord = (idx + 1) as f64;
            resj += weight / (ord * ord);
            if weight > max_weight {
                max_weight = weight;
                max_index = idx;
            }
        }
        let ord = (max_index + 1) as f64;
        result += (max_weight + resj - max_weight / (ord * ord)) / 1.64493406685;
    }
    result / operands.len() as f64
}

fn rank_and(vector: &TsVector, operands: &[TsQueryOperand], weights: &[f64; 4]) -> f64 {
    if operands.len() < 2 {
        return rank_or(vector, operands, weights);
    }
    let positions = operands
        .iter()
        .map(|operand| matching_positions(vector, operand))
        .collect::<Vec<_>>();
    let mut result = -1.0;
    for i in 0..positions.len() {
        for k in 0..i {
            for left in &positions[i] {
                for right in &positions[k] {
                    let mut distance = left.position.abs_diff(right.position) as i32;
                    if distance == 0 && (left.position == u16::MAX || right.position == u16::MAX) {
                        distance = i32::from(u16::MAX);
                    }
                    if distance == 0 {
                        continue;
                    }
                    let current = (weight_value(left.weight, weights)
                        * weight_value(right.weight, weights)
                        * word_distance(distance))
                    .sqrt();
                    result = if result < 0.0 {
                        current
                    } else {
                        1.0 - (1.0 - result) * (1.0 - current)
                    };
                }
            }
        }
    }
    result
}

fn rank_cd_or(vector: &TsVector, operands: &[TsQueryOperand], weights: &[f64; 4]) -> f64 {
    operands
        .iter()
        .flat_map(|operand| matching_positions(vector, operand))
        .filter(|position| position.position != u16::MAX)
        .map(|position| weight_value(position.weight, weights))
        .sum()
}

fn rank_cd_cover(
    vector: &TsVector,
    operands: &[TsQueryOperand],
    required_distance: Option<u16>,
    weights: &[f64; 4],
) -> f64 {
    let matched = operands
        .iter()
        .map(|operand| matching_positions(vector, operand))
        .collect::<Vec<_>>();
    if matched.iter().any(Vec::is_empty) {
        return 0.0;
    }
    let mut best = 0.0;
    for first in &matched[0] {
        let mut chosen = vec![*first];
        let mut last = *first;
        let mut ok = true;
        for positions in matched.iter().skip(1) {
            let next = if let Some(distance) = required_distance {
                positions
                    .iter()
                    .find(|position| position.position == last.position.saturating_add(distance))
            } else {
                positions
                    .iter()
                    .find(|position| position.position >= last.position)
            };
            if let Some(next) = next {
                chosen.push(*next);
                last = *next;
            } else {
                ok = false;
                break;
            }
        }
        if ok {
            best = f64::max(best, cover_density(&chosen, weights));
        }
    }
    best
}

fn rank_cd_phrase(vector: &TsVector, node: &TsQueryNode, weights: &[f64; 4]) -> f64 {
    let mut operands = Vec::new();
    let mut distance = None;
    if flatten_simple_phrase(node, &mut operands, &mut distance) {
        return rank_cd_cover(vector, &operands, distance, weights);
    }
    0.0
}

fn flatten_simple_phrase(
    node: &TsQueryNode,
    operands: &mut Vec<TsQueryOperand>,
    distance: &mut Option<u16>,
) -> bool {
    match node {
        TsQueryNode::Operand(operand) => {
            operands.push(operand.clone());
            true
        }
        TsQueryNode::Phrase {
            left,
            right,
            distance: node_distance,
        } => {
            if distance.is_some_and(|distance| distance != *node_distance) {
                return false;
            }
            *distance = Some(*node_distance);
            flatten_simple_phrase(left, operands, distance)
                && flatten_simple_phrase(right, operands, distance)
        }
        _ => false,
    }
}

fn cover_density(positions: &[TsPosition], weights: &[f64; 4]) -> f64 {
    let Some(first) = positions.first() else {
        return 0.0;
    };
    let Some(last) = positions.last() else {
        return 0.0;
    };
    let inv_sum = positions
        .iter()
        .map(|position| 1.0 / weight_value(position.weight, weights))
        .sum::<f64>();
    let cpos = positions.len() as f64 / inv_sum;
    let noise =
        i32::from(last.position.saturating_sub(first.position)) - (positions.len() as i32 - 1);
    cpos / f64::from(1 + noise.max(0))
}

fn normalize_rank(mut result: f64, vector: &TsVector, method: i32) -> f64 {
    let len = vector_length(vector) as f64;
    if method & 0x01 != 0 && len > 0.0 {
        result /= (len + 1.0).log2();
    }
    if method & 0x02 != 0 && len > 0.0 {
        result /= len;
    }
    if method & 0x08 != 0 && !vector.lexemes.is_empty() {
        result /= vector.lexemes.len() as f64;
    }
    if method & 0x10 != 0 && !vector.lexemes.is_empty() {
        result /= (vector.lexemes.len() as f64 + 1.0).log2();
    }
    if method & 0x20 != 0 {
        result /= result + 1.0;
    }
    result
}

fn vector_length(vector: &TsVector) -> usize {
    vector
        .lexemes
        .iter()
        .map(|lexeme| lexeme.positions.len().max(1))
        .sum()
}

fn weight_value(weight: Option<TsWeight>, weights: &[f64; 4]) -> f64 {
    match weight.unwrap_or(TsWeight::D) {
        TsWeight::D => weights[0],
        TsWeight::C => weights[1],
        TsWeight::B => weights[2],
        TsWeight::A => weights[3],
    }
}

fn word_distance(distance: i32) -> f64 {
    if distance > 100 {
        return 1e-30;
    }
    1.0 / (1.005 + 0.05 * ((distance as f64) / 1.5 - 2.0).exp())
}
