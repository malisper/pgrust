#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgNamespaceRow {
    pub oid: u32,
    pub nspname: String,
}
