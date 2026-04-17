use crate::include::catalog::RangeCanonicalization;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgRangeRow {
    pub rngtypid: u32,
    pub rngsubtype: u32,
    pub rngcollation: u32,
    pub rngcanonical: Option<String>,
    pub rngsubdiff: Option<String>,
    pub canonicalization: RangeCanonicalization,
}
