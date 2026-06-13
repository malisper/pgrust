//! Keyword category vocabulary (`src/include/common/keywords.h`).

/// Grammatical category of a SQL keyword. Mirrors the C `#define`d values
/// `UNRESERVED_KEYWORD`..`RESERVED_KEYWORD` in `src/include/common/keywords.h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeywordCategory {
    Unreserved = 0,
    ColumnName = 1,
    TypeOrFunctionName = 2,
    Reserved = 3,
}
