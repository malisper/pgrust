use crate::backend::parser::SqlType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgTypeRow {
    pub oid: u32,
    pub typname: String,
    pub typnamespace: u32,
    pub typrelid: u32,
    pub sql_type: SqlType,
}
