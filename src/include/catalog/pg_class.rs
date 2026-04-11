#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgClassRow {
    pub oid: u32,
    pub relname: String,
    pub relnamespace: u32,
    pub reltype: u32,
    pub relfilenode: u32,
    pub relkind: char,
}
