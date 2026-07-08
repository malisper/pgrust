//! Seams for the gin_hstore_ops opclass (contrib/hstore/hstore_gin.c),
//! installed by the hstore crate. Keys are header-ful `text` varlena images
//! (flag byte 'K'/'V'/'N' + bytes, hstore_gin.c makeitem); check values are
//! GinTernaryValue (i8: 0 false, 1 true, 2 maybe).

use types_error::PgResult;

seam_core::seam!(
    /// gin_extract_hstore core: one 'K' item per key + one 'V'/'N' item per
    /// value, in pair order. `hs_image` is the full header-ful hstore varlena.
    pub fn hstore_extract_value(hs_image: &[u8]) -> PgResult<Vec<Vec<u8>>>
);

seam_core::seam!(
    /// gin_extract_hstore_query core: (key items, searchMode). The query
    /// image shape depends on strategy (hstore / text / text[]).
    pub fn hstore_extract_query(query_image: &[u8], strategy: u16) -> PgResult<(Vec<Vec<u8>>, i32)>
);

seam_core::seam!(
    /// gin_consistent_hstore core: (result, recheck). hstore has no C
    /// triconsistent; the GIN core shims ternary over this (ginlogic.c
    /// shimTriConsistentFn).
    pub fn hstore_consistent(check: &[i8], strategy: u16, nkeys: usize) -> PgResult<(bool, bool)>
);
