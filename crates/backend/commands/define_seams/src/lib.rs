//! Seam declarations for the `DefElem` value extractors
//! (`commands/define.c`): `defGetString` / `defGetBoolean`.
//!
//! `transformRelOptions` (reloptions.c) flattens each `DefElem` into a
//! `name=value` text element, reading the value with `defGetString(def)` and
//! filtering `oids` with `defGetBoolean(def)`. Both inspect `def->arg`'s node
//! tag; both can `ereport(ERROR, ERRCODE_SYNTAX_ERROR)` ("requires a
//! parameter" / "requires a Boolean value"), so the seams return
//! `PgResult<_>`. `defGetString` palloc's its result string, so it takes the
//! target `Mcx<'mcx>`.
//!
//! `DefElem.arg` (the value node) belongs to the parser node tree, which is
//! not yet ported; [`DefElemArg`] is the projection of the value-node variants
//! `defGetString`/`defGetBoolean` actually read. The DDL caller fills it from
//! its real `def->arg`; the owning unit (`backend-commands-define`) runs the
//! nodeTag switch on it.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use ::mcx::{Mcx, PgString};
use ::types_error::PgResult;

/// Projection of a `DefElem`'s `arg` value node (`nodes/value.h`) — the
/// variants `defGetString`/`defGetBoolean`/etc. switch on. `None` for the
/// `def->arg == NULL` case is carried by the `Option<DefElemArg>` parameter.
#[derive(Clone, Debug, PartialEq)]
pub enum DefElemArg {
    /// `T_Integer` (`intVal`).
    Integer(i64),
    /// `T_Float` (`Float->fval`, kept as its source text).
    Float(String),
    /// `T_Boolean` (`boolVal`).
    Boolean(bool),
    /// `T_String` (`strVal`).
    String(String),
    /// `T_TypeName` rendered via `TypeNameToString`.
    TypeName(String),
    /// `T_List` rendered via `NameListToString`.
    List(String),
    /// `T_A_Star` (renders to `"*"`).
    AStar,
}

seam_core::seam!(
    /// `defGetString(def)` (define.c): render the `DefElem`'s value as a
    /// string. `Err(ERRCODE_SYNTAX_ERROR)` when `arg` is `None`
    /// ("%s requires a parameter", with `defname`).
    pub fn def_get_string<'mcx>(
        mcx: Mcx<'mcx>,
        defname: String,
        arg: Option<DefElemArg>,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `defGetBoolean(def)` (define.c): interpret the `DefElem`'s value as a
    /// boolean (0/1, true/false, on/off). `None` arg assumes `true`.
    /// `Err(ERRCODE_SYNTAX_ERROR)` ("%s requires a Boolean value").
    pub fn def_get_boolean(defname: String, arg: Option<DefElemArg>) -> PgResult<bool>
);
