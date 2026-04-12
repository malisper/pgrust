use crate::backend::executor::{ExecError, Value, parse_text_array_literal_with_op};
use crate::backend::parser::SqlType;

pub fn parse_text_array_literal(raw: &str, element_type: SqlType) -> Result<Value, ExecError> {
    parse_text_array_literal_with_op(raw, element_type, "copy assignment")
}
