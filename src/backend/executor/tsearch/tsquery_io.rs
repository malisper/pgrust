use super::super::ExecError;
use super::tsvector_io::{Cursor, decode_weight, weight_code};
use crate::include::nodes::tsearch::{TsQuery, TsQueryNode, TsQueryOperand};
use crate::pgrust::compact_string::CompactString;

const NODE_OPERAND: u8 = 1;
const NODE_AND: u8 = 2;
const NODE_OR: u8 = 3;
const NODE_NOT: u8 = 4;
const NODE_PHRASE: u8 = 5;

pub(crate) fn parse_tsquery_text(text: &str) -> Result<TsQuery, ExecError> {
    TsQuery::parse(text).map_err(|message| tsquery_input_parse_error(text, message))
}

pub(crate) fn render_tsquery_text(query: &TsQuery) -> String {
    query.render()
}

pub(crate) fn encode_tsquery_bytes(query: &TsQuery) -> Vec<u8> {
    let mut out = Vec::new();
    encode_node(&query.root, &mut out);
    out
}

pub(crate) fn decode_tsquery_bytes(bytes: &[u8]) -> Result<TsQuery, ExecError> {
    let mut cursor = Cursor::new(bytes);
    let root = decode_node(&mut cursor)?;
    cursor.ensure_finished()?;
    Ok(TsQuery { root })
}

pub(crate) fn tsquery_input_error(message: String) -> ExecError {
    ExecError::InvalidStorageValue {
        column: "<tsquery>".into(),
        details: message,
    }
}

fn tsquery_input_parse_error(text: &str, message: String) -> ExecError {
    if message == "invalid phrase distance" {
        return ExecError::DetailedError {
            message:
                "distance in phrase operator must be an integer value between zero and 16384 inclusive"
                    .into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        };
    }
    ExecError::DetailedError {
        message: format!("syntax error in tsquery: \"{text}\""),
        detail: None,
        hint: None,
        sqlstate: "42601",
    }
}

fn encode_node(node: &TsQueryNode, out: &mut Vec<u8>) {
    match node {
        TsQueryNode::Operand(operand) => {
            out.push(NODE_OPERAND);
            let text = operand.lexeme.as_bytes();
            out.extend_from_slice(&(text.len() as u32).to_le_bytes());
            out.extend_from_slice(text);
            out.push(u8::from(operand.prefix));
            out.push(operand.weights.len() as u8);
            for weight in &operand.weights {
                out.push(weight_code(Some(*weight)));
            }
        }
        TsQueryNode::And(left, right) => {
            out.push(NODE_AND);
            encode_node(left, out);
            encode_node(right, out);
        }
        TsQueryNode::Or(left, right) => {
            out.push(NODE_OR);
            encode_node(left, out);
            encode_node(right, out);
        }
        TsQueryNode::Not(inner) => {
            out.push(NODE_NOT);
            encode_node(inner, out);
        }
        TsQueryNode::Phrase {
            left,
            right,
            distance,
        } => {
            out.push(NODE_PHRASE);
            out.extend_from_slice(&distance.to_le_bytes());
            encode_node(left, out);
            encode_node(right, out);
        }
    }
}

fn decode_node(cursor: &mut Cursor<'_>) -> Result<TsQueryNode, ExecError> {
    match cursor.read_u8()? {
        NODE_OPERAND => {
            let text_len = cursor.read_u32()? as usize;
            let lexeme = cursor.read_str(text_len)?;
            let prefix = cursor.read_u8()? != 0;
            let weight_count = cursor.read_u8()? as usize;
            let mut weights = Vec::with_capacity(weight_count);
            for _ in 0..weight_count {
                let Some(weight) = decode_weight(cursor.read_u8()?)? else {
                    return Err(ExecError::InvalidStorageValue {
                        column: "<tsquery>".into(),
                        details: "tsquery operand weight cannot be unspecified".into(),
                    });
                };
                weights.push(weight);
            }
            Ok(TsQueryNode::Operand(TsQueryOperand {
                lexeme: CompactString::from_owned(lexeme),
                prefix,
                weights,
            }))
        }
        NODE_AND => Ok(TsQueryNode::And(
            Box::new(decode_node(cursor)?),
            Box::new(decode_node(cursor)?),
        )),
        NODE_OR => Ok(TsQueryNode::Or(
            Box::new(decode_node(cursor)?),
            Box::new(decode_node(cursor)?),
        )),
        NODE_NOT => Ok(TsQueryNode::Not(Box::new(decode_node(cursor)?))),
        NODE_PHRASE => Ok(TsQueryNode::Phrase {
            distance: cursor.read_u16()?,
            left: Box::new(decode_node(cursor)?),
            right: Box::new(decode_node(cursor)?),
        }),
        tag => Err(ExecError::InvalidStorageValue {
            column: "<tsquery>".into(),
            details: format!("invalid tsquery node tag {tag}"),
        }),
    }
}
