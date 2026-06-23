//! Command-tag vocabulary (`tcop/cmdtag.h`).
//!
//! `CommandTag` is the statement's command-tag enumerator, carried as its
//! integer value (the generated `tcop/cmdtaglist.h` order). The full tag table
//! is owned by `tcop/cmdtag.c`; here it is the canonical scalar identity shared
//! by the unported parser/plancache/matview layers (which never inspect the
//! tag, only thread it by value). Homing it in `types-core` lets crates that do
//! not depend on `types-nodes` (e.g. `types-matview`, `types-plancache`) share
//! the one definition.

/// `typedef enum CommandTag` (`tcop/cmdtag.h`, generated from
/// `tcop/cmdtaglist.h`), as a value-checked newtype over the enumerator index.
///
/// The values are the positional indices in `cmdtaglist.h` (verified against
/// PostgreSQL 18.3 via the c2rust rendering: `CMDTAG_UNKNOWN` = 0,
/// `CMDTAG_REFRESH_MATERIALIZED_VIEW` = 169, `CMDTAG_SELECT` = 179). Extend the
/// associated constants as more commands are ported.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[repr(transparent)]
pub struct CommandTag(pub i32);

impl CommandTag {
    /// `CMDTAG_UNKNOWN` (cmdtaglist.h line 27).
    pub const UNKNOWN: CommandTag = CommandTag(0);
    /// `CMDTAG_REFRESH_MATERIALIZED_VIEW` (cmdtaglist.h line 196; enum index 169).
    pub const REFRESH_MATERIALIZED_VIEW: CommandTag = CommandTag(169);
    /// `CMDTAG_SELECT` (cmdtaglist.h line 206; enum index 179).
    pub const SELECT: CommandTag = CommandTag(179);
}
