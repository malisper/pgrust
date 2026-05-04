use pgrust_expr::{ExprCatalogLookup, ExprError};
use pgrust_nodes::{SqlType, Value};

pub fn parse_text_array_literal(raw: &str, element_type: SqlType) -> Result<Value, ExprError> {
    parse_text_array_literal_with_catalog(raw, element_type, None)
}

pub fn parse_text_array_literal_with_catalog(
    raw: &str,
    element_type: SqlType,
    catalog: Option<&dyn ExprCatalogLookup>,
) -> Result<Value, ExprError> {
    pgrust_expr::parse_text_array_literal_with_catalog_op_and_explicit(
        raw,
        element_type,
        "copy assignment",
        false,
        catalog,
    )
}
