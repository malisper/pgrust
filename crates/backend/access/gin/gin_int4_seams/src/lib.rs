//! Seams for the gin__int_ops opclass (contrib/intarray/_int_gin.c),
//! installed by the intarray crate. `image` / `query_image` are detoasted
//! full 4B-header varlena images: an int4[] array for the RT array
//! strategies, a QUERYTYPE for BooleanSearchStrategy (20). check values are
//! GinTernaryValue (i8: 0 false, 1 true, 2 maybe).

use types_error::PgResult;

seam_core::seam!(
    /// ginint4_queryextract core: (entry keys, searchMode). Empty keys with
    /// searchMode GIN_SEARCH_MODE_DEFAULT is C's NULL-return void result.
    pub fn int4_extract_query(image: &[u8], strategy: u16) -> PgResult<(Vec<i32>, i32)>
);

seam_core::seam!(
    /// ginint4_consistent core: (result, recheck).
    pub fn int4_consistent(
        check: &[i8],
        strategy: u16,
        nkeys: usize,
        query_image: &[u8]
    ) -> PgResult<(bool, bool)>
);
