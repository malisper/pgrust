//! Seams for the gin_trgm_ops opclass (contrib/pg_trgm/trgm_gin.c),
//! installed by the pg_trgm crate. check/tri values are GinTernaryValue
//! (i8: 0 false, 1 true, 2 maybe); keys are trgm2int int4 key values.

use types_error::PgResult;

seam_core::seam!(
    /// gin_extract_value_trgm core: sorted unique trgm2int keys of the text
    /// payload.
    pub fn trgm_extract_value(payload: &[u8]) -> PgResult<Vec<i32>>
);

seam_core::seam!(
    /// gin_extract_query_trgm core: (keys, searchMode). Regexp strategies
    /// error loudly inside (trgm_regexp.c unported).
    pub fn trgm_extract_query(payload: &[u8], strategy: u16) -> PgResult<(Vec<i32>, i32)>
);

seam_core::seam!(
    /// gin_trgm_consistent core: (result, recheck).
    pub fn trgm_consistent(check: &[i8], strategy: u16, nkeys: usize) -> PgResult<(bool, bool)>
);

seam_core::seam!(
    /// gin_trgm_triconsistent core.
    pub fn trgm_triconsistent(check: &[i8], strategy: u16, nkeys: usize) -> PgResult<i8>
);
