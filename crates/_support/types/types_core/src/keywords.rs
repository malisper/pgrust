// Values match C's UNRESERVED_KEYWORD..RESERVED_KEYWORD defines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeywordCategory {
    Unreserved = 0,
    ColumnName = 1,
    TypeOrFunctionName = 2,
    Reserved = 3,
}
