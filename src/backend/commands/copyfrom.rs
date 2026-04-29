use crate::backend::executor::{
    ExecError, Value, parse_text_array_literal_with_catalog_op_and_explicit,
};
use crate::backend::parser::{CatalogLookup, SqlType};

pub fn parse_text_array_literal(raw: &str, element_type: SqlType) -> Result<Value, ExecError> {
    parse_text_array_literal_with_catalog(raw, element_type, None)
}

pub fn parse_text_array_literal_with_catalog(
    raw: &str,
    element_type: SqlType,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    parse_text_array_literal_with_catalog_op_and_explicit(
        raw,
        element_type,
        "copy assignment",
        false,
        catalog,
    )
}
