//! Seam declarations for the `backend-tsearch-ts-utils` unit
//! (`tsearch/ts_utils.c`): tsearch config-file path resolution and stop-word
//! list management.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgVec};
use ::types_error::PgResult;
use ::tsearch::StopList;

seam_core::seam!(
    /// `get_tsearch_config_filename(basename, extension)` (ts_utils.c): build
    /// the absolute `$SHAREDIR/tsearch_data/<basename>.<extension>` path,
    /// validating that `basename` contains no path separators (else
    /// `ereport(ERROR, ERRCODE_INVALID_PARAMETER_VALUE)`). The path is
    /// palloc'd in C; the seam allocates the returned bytes (no trailing NUL)
    /// in `mcx`.
    pub fn get_tsearch_config_filename<'mcx>(
        mcx: Mcx<'mcx>,
        basename: &[u8],
        extension: &[u8],
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `readstoplist(fname, s, wordop)` (ts_utils.c): read the
    /// `<fname>.stop` config file, apply `wordop` to each word, sort the list,
    /// and store it in a freshly built [`StopList`] allocated in `mcx`. The
    /// ispell/simple dictionaries always pass `str_tolower` as `wordop`, so
    /// `lowercase` selects that lowercasing (vs the C `NULL` wordop, verbatim).
    /// File-read / encoding errors and OOM surface on `Err`.
    pub fn readstoplist<'mcx>(
        mcx: Mcx<'mcx>,
        fname: &[u8],
        lowercase: bool,
    ) -> PgResult<StopList<'mcx>>
);

seam_core::seam!(
    /// `searchstoplist(s, key)` (ts_utils.c): true iff `key` is in the sorted
    /// stop list `s` (binary search). Pure.
    pub fn searchstoplist(s: &StopList<'_>, key: &[u8]) -> bool
);
