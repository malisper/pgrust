use crate::backend::parser::SqlType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgAttributeRow {
    pub attrelid: u32,
    pub attname: String,
    pub atttypid: u32,
    pub attnum: i16,
    pub attnotnull: bool,
    pub atttypmod: i32,
    pub sql_type: SqlType,
}
