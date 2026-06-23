//! GIN ternary / search-mode vocabulary (`access/gin.h`).
//!
//! The whole GIN vocabulary is owned by the `types-gin` crate (the GIN index
//! AM's home); `types-tsearch`'s `tsvector_ops` GIN support functions consume
//! the ternary / search-mode items from there. Re-exported here so the existing
//! `tsearch::gin::*` paths keep resolving.

pub use ::gin::{
    GinTernaryValue, GIN_FALSE, GIN_MAYBE, GIN_SEARCH_MODE_ALL, GIN_SEARCH_MODE_DEFAULT,
    GIN_SEARCH_MODE_EVERYTHING, GIN_SEARCH_MODE_INCLUDE_EMPTY, GIN_TRUE,
};
