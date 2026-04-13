use std::cmp::Ordering;

use crate::include::nodes::tsearch::{TsLexeme, TsPosition, TsVector};

pub(crate) fn compare_tsvector(left: &TsVector, right: &TsVector) -> Ordering {
    left.render().cmp(&right.render())
}

pub(crate) fn concat_tsvector(left: &TsVector, right: &TsVector) -> TsVector {
    let max_left_pos = left
        .lexemes
        .iter()
        .flat_map(|lexeme| lexeme.positions.iter().map(|position| position.position))
        .max()
        .unwrap_or(0);
    let mut merged = left.lexemes.clone();
    merged.extend(right.lexemes.iter().map(|lexeme| {
        TsLexeme {
            text: lexeme.text.clone(),
            positions: lexeme
                .positions
                .iter()
                .map(|position| TsPosition {
                    position: position.position.saturating_add(max_left_pos),
                    weight: position.weight,
                })
                .collect(),
        }
    }));
    TsVector::new(merged)
}
