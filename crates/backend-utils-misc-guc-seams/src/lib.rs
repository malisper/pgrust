//! Seam declarations for the GUC subsystem (`utils/misc/guc.c`) calls
//! `ri_triggers.c` makes to bump `work_mem`/`hash_mem_multiplier` for its bulk
//! validation queries and to unwind them at transaction end.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;
use types_guc::guc::{GucContext, GucSource};

seam_core::seam!(
    /// `NewGUCNestLevel()` (guc.c): begin a new GUC nesting level for
    /// transactional/function SET, returning the save-nestlevel to pass to
    /// `AtEOXact_GUC`.
    pub fn new_guc_nest_level() -> i32
);

seam_core::seam!(
    /// `set_config_option(name, value, context, source, GUC_ACTION_SAVE,
    /// changeVal=true, elevel=0, is_reload=false)` (guc.c): apply a temporary
    /// SET. Returns the C `int` result (`>0` ok, `0` no-op, `<0` error at
    /// `elevel < ERROR`); `Err` carries an `ereport(ERROR)`.
    pub fn set_config_option(
        name: &str,
        value: &str,
        context: GucContext,
        source: GucSource,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `AtEOXact_GUC(isCommit, nestLevel)` (guc.c): roll back the GUC settings
    /// made above `nestLevel`. Can `ereport(ERROR)` on a bad assign hook,
    /// carried on `Err`.
    pub fn at_eoxact_guc(is_commit: bool, nest_level: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `maintenance_work_mem` (guc.c global, KB): the value `ri_triggers.c`
    /// installs as `work_mem` for its validation query.
    pub fn maintenance_work_mem() -> i32
);
