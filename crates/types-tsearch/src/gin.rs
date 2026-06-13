//! GIN ternary / search-mode vocabulary (`access/gin.h`), trimmed to the items
//! the `tsvector_ops` GIN support functions consume.

/// `GinTernaryValue` — a `char`-sized tri-state (access/gin.h).
pub type GinTernaryValue = i8;

/// `GIN_FALSE`: item is not present / does not match.
pub const GIN_FALSE: GinTernaryValue = 0;
/// `GIN_TRUE`: item is present / matches.
pub const GIN_TRUE: GinTernaryValue = 1;
/// `GIN_MAYBE`: don't know if item is present / matches.
pub const GIN_MAYBE: GinTernaryValue = 2;

/// `GIN_SEARCH_MODE_DEFAULT` (access/gin.h).
pub const GIN_SEARCH_MODE_DEFAULT: i32 = 0;
/// `GIN_SEARCH_MODE_INCLUDE_EMPTY` (access/gin.h).
pub const GIN_SEARCH_MODE_INCLUDE_EMPTY: i32 = 1;
/// `GIN_SEARCH_MODE_ALL` (access/gin.h).
pub const GIN_SEARCH_MODE_ALL: i32 = 2;
/// `GIN_SEARCH_MODE_EVERYTHING` (access/gin.h).
pub const GIN_SEARCH_MODE_EVERYTHING: i32 = 3;
